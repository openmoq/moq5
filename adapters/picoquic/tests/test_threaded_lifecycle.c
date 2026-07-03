/*
 * White-box lifecycle test for the managed threaded SERVER's terminal-conn
 * observation contract:
 *
 *   A connection that becomes terminal must remain visible in
 *   moq_pq_threaded_next_conn() for one app on_pump before the adapter
 *   prunes/destroys it.
 *
 * The hard-to-reach window is a death arising in the POST-on_pump service
 * pass (a session/bridge failure there; peer closes land at packet
 * ingestion and are observable by the ordinary pump). The
 * MOQ_PQ_THREADED_TESTING seam models exactly that: armed from on_pump,
 * the NEXT service pass -- the post-pump one -- marks the conn fatal.
 * Without the observation window, the same loop iteration prunes it and no
 * pump ever sees the dead conn (the relay "namespace stays occupied after
 * disconnect" leak); with it, one extra sequential pump observes the conn
 * before the prune.
 *
 * Links the test-internals build of the adapter (never shipped).
 */

#include <moq/picoquic_threaded.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* The MOQ_PQ_THREADED_TESTING seam (deliberately NOT in the public header). */
extern void moq_pq_threaded_test_arm_service_fatal(moq_pq_threaded_conn_t *c);

static int failures = 0;

#define CHECK(expr)                                                     \
    do {                                                                \
        if (!(expr)) {                                                  \
            fprintf(stderr, "FAIL: %s:%d: %s\n",                       \
                    __FILE__, __LINE__, #expr);                         \
            failures++;                                                 \
        }                                                               \
    } while (0)

#ifdef MOQ_TEST_CERT_PATH

typedef struct {
    moq_pq_threaded_conn_t *conn;      /* network-thread only */
    int accepted;                      /* atomics: read from the test thread */
    int armed;
    int observed_after_death;          /* polled SESSION_CLOSED post-kill:
                                          THE contract bit */
    int in_pump;
    int reentered;
} window_ctx_t;

static int window_pump(moq_pq_threaded_t *t, uint64_t now, void *vctx)
{
    (void)now;
    window_ctx_t *w = (window_ctx_t *)vctx;
    if (__atomic_exchange_n(&w->in_pump, 1, __ATOMIC_ACQ_REL))
        __atomic_store_n(&w->reentered, 1, __ATOMIC_RELEASE);

    int was_armed = __atomic_load_n(&w->armed, __ATOMIC_ACQUIRE);
    int present = 0;
    moq_pq_threaded_conn_t *c = NULL;
    while ((c = moq_pq_threaded_next_conn(t, c)) != NULL) {
        if (!w->conn) {
            w->conn = c;
            __atomic_store_n(&w->accepted, 1, __ATOMIC_RELEASE);
        }
        if (c == w->conn)
            present = 1;
    }
    if (present && was_armed) {
        /* The seam closed the conn's transport in the service pass right
         * after the arming pump returned, queuing the REAL
         * MOQ_EVENT_SESSION_CLOSED. This pump must be able to poll that
         * exact artifact -- visibility alone is not the contract; the
         * relay detaches its binding on this event. */
        moq_session_t *sess = moq_pq_threaded_conn_session(w->conn);
        if (sess) {
            moq_event_t ev[8];
            size_t ne = 0;
            moq_session_poll_events_ex(sess, ev, 8, sizeof(moq_event_t), &ne);
            for (size_t i = 0; i < ne; i++) {
                if (ev[i].kind == MOQ_EVENT_SESSION_CLOSED)
                    __atomic_store_n(&w->observed_after_death, 1,
                                     __ATOMIC_RELEASE);
            }
        }
    } else if (present && !was_armed) {
        moq_pq_threaded_test_arm_service_fatal(w->conn);
        __atomic_store_n(&w->armed, 1, __ATOMIC_RELEASE);
    }
    __atomic_store_n(&w->in_pump, 0, __ATOMIC_RELEASE);
    return 0;
}

static int dummy_pump(moq_pq_threaded_t *t, uint64_t now, void *ctx)
{
    (void)t; (void)now; (void)ctx;
    return 0;
}

int main(void)
{
    window_ctx_t w;
    memset(&w, 0, sizeof(w));
    srand((unsigned)getpid());
    int port = 15200 + (rand() % 400);

    moq_pq_threaded_cfg_t srv_cfg;
    moq_pq_threaded_cfg_init_sized(&srv_cfg, sizeof(srv_cfg));
    srv_cfg.alloc = moq_alloc_default();
    srv_cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    srv_cfg.cert_path = MOQ_TEST_CERT_PATH;
    srv_cfg.key_path = MOQ_TEST_KEY_PATH;
    srv_cfg.port = port;
    srv_cfg.on_pump = window_pump;
    srv_cfg.on_pump_ctx = &w;
    moq_pq_threaded_t *srv = NULL;
    CHECK(moq_pq_threaded_create(&srv_cfg, &srv) == MOQ_OK);

    if (srv) {
        moq_pq_threaded_cfg_t cli_cfg;
        moq_pq_threaded_cfg_init_sized(&cli_cfg, sizeof(cli_cfg));
        cli_cfg.alloc = moq_alloc_default();
        cli_cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cli_cfg.host = "localhost";
        cli_cfg.port = port;
        cli_cfg.insecure_skip_verify = true;
        cli_cfg.on_pump = dummy_pump;
        moq_pq_threaded_t *cli = NULL;
        CHECK(moq_pq_threaded_create(&cli_cfg, &cli) == MOQ_OK);

        if (cli) {
            for (int tries = 0; tries < 400 &&
                 !__atomic_load_n(&w.armed, __ATOMIC_ACQUIRE);
                 tries++)
                moq_pq_threaded_wait(srv, 25000);
            CHECK(__atomic_load_n(&w.accepted, __ATOMIC_ACQUIRE) == 1);
            CHECK(__atomic_load_n(&w.armed, __ATOMIC_ACQUIRE) == 1);

            /* The armed kill fires in the post-pump service pass; wait for
             * the prune, observable via the cross-thread count. */
            for (int tries = 0; tries < 400 &&
                 moq_pq_threaded_conn_count(srv) > 0;
                 tries++)
                moq_pq_threaded_wait(srv, 25000);
            CHECK(moq_pq_threaded_conn_count(srv) == 0);

            /* THE contract: some app pump saw the conn AFTER its death and
             * before the prune -- never destroyed unobserved. */
            CHECK(__atomic_load_n(&w.observed_after_death,
                                  __ATOMIC_ACQUIRE) == 1);
            CHECK(__atomic_load_n(&w.reentered, __ATOMIC_ACQUIRE) == 0);

            moq_pq_threaded_stop(cli);
            moq_pq_threaded_destroy(cli);
        }
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    if (failures == 0)
        printf("PASS: threaded_lifecycle (terminal conn observed before prune)\n");
    return failures ? 1 : 0;
}

#else  /* !MOQ_TEST_CERT_PATH */

int main(void)
{
    printf("SKIP: threaded_lifecycle (no MOQ_TEST_CERT_PATH)\n");
    return 0;
}

#endif
