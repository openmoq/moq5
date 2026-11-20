/* The subscriber facade refuses the requests a peer opens that nothing above
 * it serves (a relay's PUBLISH_NAMESPACE, for one), so none stays unanswered
 * until the session ends. Each gets REQUEST_ERROR: DOES_NOT_EXIST for a
 * SUBSCRIBE or TRACK_STATUS, NOT_SUPPORTED otherwise, as the publisher facade
 * answers them. A refusal the session cannot take yet is held and sent on a
 * later tick. */
#include <moq/subscriber.h>
#include <moq/sim.h>
#include "test_session_support.h"
#include <stdio.h>
#include <string.h>

static int failures = 0;

enum { K_NAMESPACE, K_SUBSCRIBE, K_TRACK_STATUS, K_NS_SUB, K_SUBSCRIBE_TRACKS, K_PUBLISH, K_FETCH,
       K_COUNT };
static const char *const k_names[K_COUNT] = {
    "PUBLISH_NAMESPACE", "SUBSCRIBE", "TRACK_STATUS", "SUBSCRIBE_NAMESPACE",
    "SUBSCRIBE_TRACKS", "PUBLISH", "FETCH"
};

typedef struct {
    int count[K_COUNT];
    moq_request_error_t code[K_COUNT];
    int other_failures;  /* a request answered with an unexpected kind */
} answers_t;

static void record(answers_t *a, int k, moq_request_error_t code)
{
    a->count[k]++;
    a->code[k] = code;
}

/* The relay side's answers: one error event per refused request. */
static void collect(moq_session_t *relay, answers_t *a)
{
    moq_event_t ev;
    while (moq_session_poll_events(relay, &ev, 1) == 1) {
        switch (ev.kind) {
        case MOQ_EVENT_NAMESPACE_REJECTED:
            record(a, K_NAMESPACE, ev.u.namespace_rejected.error_code); break;
        case MOQ_EVENT_SUBSCRIBE_ERROR:
            record(a, K_SUBSCRIBE, ev.u.subscribe_error.error_code); break;
        case MOQ_EVENT_TRACK_STATUS_ERROR:
            record(a, K_TRACK_STATUS, ev.u.track_status_error.error_code); break;
        case MOQ_EVENT_NS_SUB_ERROR:
            record(a, K_NS_SUB, ev.u.ns_sub_error.error_code); break;
        case MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR:
            record(a, K_SUBSCRIBE_TRACKS, ev.u.subscribe_tracks_error.error_code); break;
        case MOQ_EVENT_PUBLISH_ERROR:
            record(a, K_PUBLISH, ev.u.publish_error.error_code); break;
        case MOQ_EVENT_FETCH_ERROR:
            record(a, K_FETCH, ev.u.fetch_error.error_code); break;
        case MOQ_EVENT_NAMESPACE_ACCEPTED:
        case MOQ_EVENT_SUBSCRIBE_OK:
        case MOQ_EVENT_TRACK_STATUS_OK:
        case MOQ_EVENT_NS_SUB_OK:
        case MOQ_EVENT_SUBSCRIBE_TRACKS_OK:
        case MOQ_EVENT_PUBLISH_OK:
        case MOQ_EVENT_FETCH_OK:
            a->other_failures++; break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
}

static int answered(const answers_t *a)
{
    int n = 0;
    for (int k = 0; k < K_COUNT; k++) n += a->count[k] > 0;
    return n;
}

static void drain(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1) moq_event_cleanup(&ev);
}

/* The relay (server) opens one request of each kind towards the subscriber
 * (client). With step set, the pair runs after each request, so a session
 * limited to a few actions can still open them all. */
static void open_requests(moq_session_t *relay, uint64_t now, moq_simpair_t *step)
{
#define STEP() do { if (step) moq_simpair_run_until_quiescent(step, 16, NULL); } while (0)
    moq_bytes_t part = MOQ_BYTES_LITERAL("relay");
    moq_namespace_t ns = { &part, 1 };

    moq_publish_namespace_cfg_t nc; moq_publish_namespace_cfg_init(&nc);
    nc.track_namespace = ns;
    moq_announcement_t ann;
    MOQ_TEST_CHECK(moq_session_publish_namespace(relay, &nc, now, &ann) == MOQ_OK);
    STEP();

    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = ns;
    sc.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub;
    MOQ_TEST_CHECK(moq_session_subscribe(relay, &sc, now, &sub) == MOQ_OK);
    STEP();

    moq_track_status_cfg_t tc; moq_track_status_cfg_init(&tc);
    tc.track_namespace = ns;
    tc.track_name = MOQ_BYTES_LITERAL("video");
    moq_track_status_handle_t ts;
    MOQ_TEST_CHECK(moq_session_track_status(relay, &tc, now, &ts) == MOQ_OK);
    STEP();

    moq_subscribe_namespace_cfg_t nsc; moq_subscribe_namespace_cfg_init(&nsc);
    nsc.track_namespace_prefix = ns;
    nsc.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nsh;
    MOQ_TEST_CHECK(moq_session_subscribe_namespace(relay, &nsc, now, &nsh) == MOQ_OK);
    STEP();

    moq_subscribe_tracks_cfg_t stc; moq_subscribe_tracks_cfg_init(&stc);
    stc.track_namespace_prefix = ns;
    moq_track_sub_handle_t sth;
    MOQ_TEST_CHECK(moq_session_subscribe_tracks(relay, &stc, now, &sth) == MOQ_OK);
    STEP();

    moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
    pc.track_namespace = ns;
    pc.track_name = MOQ_BYTES_LITERAL("pushed");
    moq_publication_t pub;
    MOQ_TEST_CHECK(moq_session_publish(relay, &pc, now, &pub) == MOQ_OK);
    STEP();

    moq_fetch_cfg_t fc; moq_fetch_cfg_init(&fc);
    fc.track_namespace = ns;
    fc.track_name = MOQ_BYTES_LITERAL("past");
    fc.end_group = 0;
    fc.end_object = 9;
    moq_fetch_t fetch;
    MOQ_TEST_CHECK(moq_session_fetch(relay, &fc, now, &fetch) == MOQ_OK);
    STEP();
#undef STEP
}

static void make_pair(test_alloc_state_t *as, moq_alloc_t *alloc, moq_version_t version,
                      uint32_t max_actions, moq_simpair_t **sp, moq_subscriber_t **sub)
{
    *as = (test_alloc_state_t){0};
    *alloc = test_allocator(as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc; cfg.seed = 7; cfg.initial_now_us = 1000;
    cfg.version = version;
    cfg.max_actions = max_actions;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 32;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, sp) == MOQ_OK);
    moq_simpair_start(*sp);
    moq_simpair_run_until_quiescent(*sp, 8, NULL);
    drain(moq_simpair_client(*sp));
    drain(moq_simpair_server(*sp));
    moq_sub_cfg_t scfg; moq_sub_cfg_init(&scfg);
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(*sp), alloc, &scfg, sub) == MOQ_OK);
}

static void destroy_pair(test_alloc_state_t *as, moq_simpair_t *sp, moq_subscriber_t *sub)
{
    moq_sub_destroy(sub);
    drain(moq_simpair_server(sp));
    drain(moq_simpair_client(sp));
    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as->balance == 0);
}

static void check_codes(const answers_t *a, const char *label)
{
    for (int k = 0; k < K_COUNT; k++) {
        moq_request_error_t want = (k == K_SUBSCRIBE || k == K_TRACK_STATUS)
            ? MOQ_REQUEST_ERROR_DOES_NOT_EXIST : MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        if (a->count[k] != 1 || a->code[k] != want) {
            fprintf(stderr, "  %s: %s answered %d time(s), code %u (want 1, code %u)\n", label,
                    k_names[k], a->count[k], (unsigned)a->code[k], (unsigned)want);
            failures++;
        }
    }
    MOQ_TEST_CHECK(a->other_failures == 0);
}

/* Every kind of peer request gets exactly one refusal, on drafts 18 and 22. */
static void test_refuses_every_unserved_request(moq_version_t version, const char *label)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp; moq_subscriber_t *sub;
    make_pair(&as, &alloc, version, 0, &sp, &sub);
    moq_session_t *relay = moq_simpair_server(sp);

    open_requests(relay, moq_simpair_now_us(sp), NULL);
    answers_t a = {0};
    for (int round = 0; round < 8 && answered(&a) < K_COUNT; round++) {
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        MOQ_TEST_CHECK(moq_sub_tick(sub, moq_simpair_now_us(sp)) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        collect(relay, &a);
    }
    check_codes(&a, label);

    destroy_pair(&as, sp, sub);
    MOQ_TEST_PASS(label);
}

/* With a session that can take only a few actions at a time, some refusals
 * wait in the subscriber: they go out only on a later tick. After each tick
 * the pair runs until quiet with no further tick, so whatever the session
 * accepted has gone out by then; an answer that appears only after the next
 * tick was held by the subscriber. Each request is still answered once. */
static void test_held_refusals_are_retried(void)
{
    test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp; moq_subscriber_t *sub;
    make_pair(&as, &alloc, MOQ_VERSION_DRAFT_18, 4, &sp, &sub);
    moq_session_t *relay = moq_simpair_server(sp);

    open_requests(relay, moq_simpair_now_us(sp), sp);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    answers_t a = {0};
    int ticks = 0, held_ticks = 0;
    while (ticks < 32 && answered(&a) < K_COUNT) {
        moq_result_t rc = moq_sub_tick(sub, moq_simpair_now_us(sp));
        MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        ticks++;
        for (int i = 0; i < 8; i++) {
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            collect(relay, &a);
        }
        /* Settled with no further tick, and still not all answered: the rest
         * waits in the subscriber. */
        if (answered(&a) < K_COUNT) held_ticks++;
    }
    fprintf(stderr, "  held_refusals_are_retried: %d tick(s), %d with refusals still held\n",
            ticks, held_ticks);
    check_codes(&a, "held_refusals_are_retried");
    if (held_ticks == 0) {
        fprintf(stderr, "  no refusal was held: the session took every refusal at once\n");
        failures++;
    }

    destroy_pair(&as, sp, sub);
    MOQ_TEST_PASS("held_refusals_are_retried");
}

int main(void)
{
    test_refuses_every_unserved_request(MOQ_VERSION_DRAFT_18, "refuses_every_unserved_request_d18");
    test_refuses_every_unserved_request(MOQ_VERSION_DRAFT_22, "refuses_every_unserved_request_d22");
    test_held_refusals_are_retried();
    fprintf(stderr, "subscriber_unserved: %d failure(s)\n", failures);
    return failures ? 1 : 0;
}
