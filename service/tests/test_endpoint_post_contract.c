/*
 * Endpoint post() NULL-session contract during connection establishment.
 *
 * endpoint.h §5.4 (normative, exactly-once): an accepted post() task is invoked
 * EXACTLY ONCE -- either normally with a live session, or during stop()/terminal
 * drain with session == NULL as the closed marker. It must NEVER run with a NULL
 * session during a normal (pre-establishment) pump cycle: a managed WebTransport
 * facade pumps before its session is published (the CONNECT still handshaking),
 * and running a task there would break the contract and, for a client that reads
 * NULL as "session gone", tear the still-connecting session down.
 *
 * Pure, no network: the bare-endpoint test seam drives ep_pump_cycle directly
 * with a controlled session (NULL, then a real IDLE core session).
 */
#include "test_support.h"

#include <moq/endpoint.h>
#include <moq/session.h>
#include <moq/types.h>

#include <string.h>

/* Bare-endpoint + pump test seam (compiled only under MOQ_MEDIA_*_TESTING; see
 * service/src/endpoint.c). Forward-declared here like the other seam users. */
moq_endpoint_t *moq_endpoint_test_make_bare(void);
void   moq_endpoint_test_free_bare(moq_endpoint_t *ep);
size_t moq_endpoint_test_task_count(moq_endpoint_t *ep);
void   moq_endpoint_test_pump_cycle(moq_endpoint_t *ep, moq_session_t *session,
                                    uint64_t now_us);
void   moq_endpoint_test_drain_terminal(moq_endpoint_t *ep);

static int failures = 0;

typedef struct {
    int total_runs;
    int null_runs;
    int live_runs;
    int order[16];
    int order_n;
} recorder_t;

typedef struct {
    recorder_t *rec;
    int id;
} task_ctx_t;

static moq_result_t rec_task(moq_endpoint_t *ep, moq_session_t *sess,
                             uint64_t now, void *ctx)
{
    (void)ep; (void)now;
    task_ctx_t *t = (task_ctx_t *)ctx;
    t->rec->total_runs++;
    if (sess) t->rec->live_runs++;
    else      t->rec->null_runs++;
    if (t->rec->order_n < (int)(sizeof(t->rec->order) / sizeof(t->rec->order[0])))
        t->rec->order[t->rec->order_n++] = t->id;
    return MOQ_OK;
}

int main(void)
{
    /* A real client session for the live-cycle cases: created IDLE (never
     * started), so moq_session_state() reads a non-ESTABLISHED state -- the
     * pump's state-refresh runs without touching a (bare, NULL) facade vtable. */
    const moq_alloc_t *alloc = moq_alloc_default();
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), alloc, MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *live = NULL;
    MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &live) == MOQ_OK);
    MOQ_TEST_CHECK(live != NULL);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(live), (int)MOQ_SESS_IDLE);

    uint64_t now = 1000;

    /* == Case 1: a task accepted while CONNECTING stays queued across NULL
     *            (pre-establishment) pump cycles. ============================ */
    {
        recorder_t rec; memset(&rec, 0, sizeof(rec));
        task_ctx_t t = { &rec, 1 };
        moq_endpoint_t *ep = moq_endpoint_test_make_bare();
        MOQ_TEST_CHECK(ep != NULL);
        MOQ_TEST_CHECK(moq_endpoint_post(ep, rec_task, &t) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_test_task_count(ep), 1);

        for (int i = 0; i < 3; i++) {
            now += 100;
            moq_endpoint_test_pump_cycle(ep, NULL, now); /* still connecting */
        }
        MOQ_TEST_CHECK_EQ_INT(rec.total_runs, 0);        /* never ran on NULL */
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_test_task_count(ep), 1);

        /* == Case 2: once a live session is supplied, it runs EXACTLY once with
         *            the live session, and not again. ======================== */
        now += 100;
        moq_endpoint_test_pump_cycle(ep, live, now);
        MOQ_TEST_CHECK_EQ_INT(rec.total_runs, 1);
        MOQ_TEST_CHECK_EQ_INT(rec.live_runs, 1);
        MOQ_TEST_CHECK_EQ_INT(rec.null_runs, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_test_task_count(ep), 0);

        now += 100;
        moq_endpoint_test_pump_cycle(ep, live, now);     /* nothing left */
        MOQ_TEST_CHECK_EQ_INT(rec.total_runs, 1);        /* exactly once */

        moq_endpoint_test_free_bare(ep);
    }

    /* == Case 3: stop before establishment -> the task runs EXACTLY once during
     *            the terminal drain, with the NULL closed marker. ============ */
    {
        recorder_t rec; memset(&rec, 0, sizeof(rec));
        task_ctx_t t = { &rec, 2 };
        moq_endpoint_t *ep = moq_endpoint_test_make_bare();
        MOQ_TEST_CHECK(ep != NULL);
        MOQ_TEST_CHECK(moq_endpoint_post(ep, rec_task, &t) == MOQ_OK);

        now += 100;
        moq_endpoint_test_pump_cycle(ep, NULL, now);     /* connecting: queued */
        MOQ_TEST_CHECK_EQ_INT(rec.total_runs, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_test_task_count(ep), 1);

        moq_endpoint_test_drain_terminal(ep);            /* stop/terminal drain */
        MOQ_TEST_CHECK_EQ_INT(rec.total_runs, 1);
        MOQ_TEST_CHECK_EQ_INT(rec.null_runs, 1);         /* NULL closed marker */
        MOQ_TEST_CHECK_EQ_INT(rec.live_runs, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_test_task_count(ep), 0);

        moq_endpoint_test_free_bare(ep);
    }

    /* == Case 4: FIFO order preserved across deferred tasks. A run of tasks
     *            posted before establishment all defer, then run in submission
     *            order on the first live cycle. =============================== */
    {
        recorder_t rec; memset(&rec, 0, sizeof(rec));
        task_ctx_t a = { &rec, 10 }, b = { &rec, 11 }, c = { &rec, 12 };
        moq_endpoint_t *ep = moq_endpoint_test_make_bare();
        MOQ_TEST_CHECK(ep != NULL);
        MOQ_TEST_CHECK(moq_endpoint_post(ep, rec_task, &a) == MOQ_OK);
        MOQ_TEST_CHECK(moq_endpoint_post(ep, rec_task, &b) == MOQ_OK);
        MOQ_TEST_CHECK(moq_endpoint_post(ep, rec_task, &c) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_test_task_count(ep), 3);

        now += 100;
        moq_endpoint_test_pump_cycle(ep, NULL, now);     /* all deferred */
        MOQ_TEST_CHECK_EQ_INT(rec.order_n, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_test_task_count(ep), 3);

        now += 100;
        moq_endpoint_test_pump_cycle(ep, live, now);     /* all run, in order */
        MOQ_TEST_CHECK_EQ_INT(rec.order_n, 3);
        MOQ_TEST_CHECK_EQ_INT(rec.order[0], 10);
        MOQ_TEST_CHECK_EQ_INT(rec.order[1], 11);
        MOQ_TEST_CHECK_EQ_INT(rec.order[2], 12);
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_test_task_count(ep), 0);

        moq_endpoint_test_free_bare(ep);
    }

    moq_session_destroy(live);

    if (failures) {
        fprintf(stderr, "FAILED: %d check(s)\n", failures);
        return 1;
    }
    printf("PASS: endpoint_post_contract\n");
    return 0;
}
