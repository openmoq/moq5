/*
 * Draft-16: a subscriber that finds a Track malformed cancels it locally,
 * and the publisher's PUBLISH_DONE that was already in flight crosses that
 * cancel. draft-ietf-moq-transport-16 2.4.2 makes condition 6 (one Subgroup
 * on two streams terminated by FIN with different final Objects) a Malformed
 * Track the subscriber MUST UNSUBSCRIBE; 5.1.1 keeps subscription state only
 * until UNSUBSCRIBE is sent; 9.15 lets the publisher end the subscription
 * with PUBLISH_DONE for any reason. The crossing DONE therefore names a
 * request the subscriber has ended, and must not close the session.
 *
 * Wire order is driven by hand: the publisher's data streams reach the
 * subscriber first, the subscriber's UNSUBSCRIBE is held back, and only then
 * is the publisher's PUBLISH_DONE delivered. Both origins of the
 * subscription are covered -- established by the subscriber's SUBSCRIBE and
 * by the publisher's PUBLISH. In each the track ends locally exactly once,
 * the session stays ESTABLISHED, and the ended request does not come back.
 */
#include <moq/moq.h>
#include "test_raw_pair_support.h"
#include "test_support.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

static moq_bytes_t g_ns_part[] = { { (const uint8_t *)"live", 4 } };

/* Deliver every queued action of `from` to `to`, control messages last, so
 * the data streams written before a PUBLISH_DONE land first. */
static void deliver_data_then_control(moq_session_t *from, moq_session_t *to,
                                      bool from_server, uint64_t now)
{
    moq_action_t held[16];
    size_t nheld = 0;
    moq_action_t a;
    while (moq_session_poll_actions(from, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_SEND_CONTROL && nheld < 16) {
            held[nheld++] = a;
            continue;
        }
        deliver(to, &a, from_server, now);
        moq_action_cleanup(&a);
    }
    for (size_t i = 0; i < nheld; i++) {
        deliver(to, &held[i], from_server, now);
        moq_action_cleanup(&held[i]);
    }
}

/* Every request terminal the application can see, tallied in one drain of
 * the event queue so that no count is read from a queue another count
 * already emptied. */
typedef struct {
    unsigned unsubscribed, subscribe_done, publish_finished, publish_error;
    unsigned subscribe_error, closed;
    uint64_t unsubscribed_cause, publish_finished_cause;
} terminals_t;

static terminals_t tally_terminals(moq_session_t *s)
{
    terminals_t t;
    memset(&t, 0, sizeof(t));
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_UNSUBSCRIBED:
            t.unsubscribed++;
            t.unsubscribed_cause = ev.u.unsubscribed.cause;
            break;
        case MOQ_EVENT_SUBSCRIBE_DONE: t.subscribe_done++; break;
        case MOQ_EVENT_SUBSCRIBE_ERROR: t.subscribe_error++; break;
        case MOQ_EVENT_PUBLISH_FINISHED:
            t.publish_finished++;
            t.publish_finished_cause = ev.u.publish_finished.cause;
            break;
        case MOQ_EVENT_PUBLISH_ERROR: t.publish_error++; break;
        case MOQ_EVENT_SESSION_CLOSED:
            t.closed++;
            fprintf(stderr, "  session closed: code 0x%llx \"%.*s\"\n",
                    (unsigned long long)ev.u.closed.code, (int)ev.u.closed.reason.len,
                    ev.u.closed.reason.data ? (const char *)ev.u.closed.reason.data : "");
            break;
        default: break;
        }
        moq_event_cleanup(&ev);
    }
    return t;
}

static moq_rcbuf_t *payload(void)
{
    static const uint8_t bytes[] = { 1, 2, 3 };
    moq_rcbuf_t *p = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes), &p) == MOQ_OK);
    return p;
}

/* One subgroup sent twice: final Object 1 on the first stream, then Object 2
 * on a second stream for the same (Group, Subgroup) -- condition 6. Each
 * stream reaches the subscriber before the next is opened; anything the
 * subscriber sends back stays queued. */
static void write_contradicting_subgroups(moq_session_t *pub, moq_session_t *peer,
                                          bool from_server, moq_subscription_t sub,
                                          moq_publication_t pubh, bool publish_origin)
{
    for (int i = 0; i < 2; i++) {
        moq_subgroup_cfg_t sc;
        moq_subgroup_cfg_init(&sc);
        sc.group_id = 0;
        sc.subgroup_id = 0;
        sc.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        moq_result_t rc = publish_origin
            ? moq_session_open_pub_subgroup(pub, pubh, &sc, 2000, &sg)
            : moq_session_open_subgroup(pub, sub, &sc, 2000, &sg);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        if (rc != MOQ_OK) return;
        moq_rcbuf_t *p = payload();
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(pub, sg, (uint64_t)i + 1, p, 2000),
                              (int)MOQ_OK);
        moq_rcbuf_decref(p);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(pub, sg, 2000), (int)MOQ_OK);
        deliver_data_then_control(pub, peer, from_server, 2000);
    }
}

/* The subscriber is the client; the server publishes on its SUBSCRIBE. */
static void test_subscribe_origin(void)
{
    int before = failures;
    moq_session_t *c = NULL, *sv = NULL;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    if (!c || !sv) return;

    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = (moq_namespace_t){ g_ns_part, 1 };
    sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t csub;
    MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 1000, &csub) == MOQ_OK);
    pump(c, sv, false, 1000, 0);
    moq_event_t ev;
    bool got = take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev);
    MOQ_TEST_CHECK(got);
    if (!got) { destroy(c, sv); return; }
    moq_subscription_t ssub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, ssub, &ac, 1000) == MOQ_OK);
    run(c, sv, 1000);
    drain(c); drain(sv);

    write_contradicting_subgroups(sv, c, true, ssub, MOQ_PUBLICATION_INVALID, false);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(sv, ssub, &dc, 2000), (int)MOQ_OK);

    /* The data streams failed the track; now the crossing PUBLISH_DONE. The
     * client's UNSUBSCRIBE stays queued: the server has not seen it. */
    deliver_data_then_control(sv, c, true, 2000);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
    /* The local malformed-track terminal is the only terminal: the crossing
     * DONE adds none, and the ended request is not revived. */
    terminals_t t = tally_terminals(c);
    MOQ_TEST_CHECK_EQ_INT((int)t.unsubscribed, 1);
    MOQ_TEST_CHECK_EQ_U64(t.unsubscribed_cause, MOQ_REQUEST_TERMINAL_MALFORMED_TRACK);
    MOQ_TEST_CHECK_EQ_INT((int)t.subscribe_done, 0);
    MOQ_TEST_CHECK_EQ_INT((int)t.subscribe_error, 0);
    MOQ_TEST_CHECK_EQ_INT((int)t.publish_finished, 0);
    MOQ_TEST_CHECK_EQ_INT((int)t.publish_error, 0);
    MOQ_TEST_CHECK_EQ_INT((int)t.closed, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(c, csub, 3000),
                          (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
    destroy(c, sv);
    if (failures == before) printf("PASS: d16 malformed track, crossing PUBLISH_DONE (SUBSCRIBE)\n");
}

/* The subscriber is the server, on the client's PUBLISH. */
static void test_publish_origin(void)
{
    int before = failures;
    moq_session_t *c = NULL, *sv = NULL;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    if (!c || !sv) return;

    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ g_ns_part, 1 };
    pc.track_name = MOQ_BYTES_LITERAL("video");
    moq_publication_t cpub;
    MOQ_TEST_CHECK(moq_session_publish(c, &pc, 1000, &cpub) == MOQ_OK);
    pump(c, sv, false, 1000, 0);
    moq_event_t ev;
    bool got = take(sv, MOQ_EVENT_PUBLISH_REQUEST, &ev);
    MOQ_TEST_CHECK(got);
    if (!got) { destroy(c, sv); return; }
    moq_publication_t spub = ev.u.publish_request.pub;
    moq_event_cleanup(&ev);
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init(&ac);
    MOQ_TEST_CHECK(moq_session_accept_publish(sv, spub, &ac, 1000) == MOQ_OK);
    run(c, sv, 1000);
    drain(c); drain(sv);

    write_contradicting_subgroups(c, sv, false, MOQ_SUBSCRIPTION_INVALID, cpub, true);
    moq_finish_publish_cfg_t fc;
    moq_finish_publish_cfg_init(&fc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(c, cpub, &fc, 2000), (int)MOQ_OK);

    deliver_data_then_control(c, sv, false, 2000);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);
    /* Exactly one PUBLISH_FINISHED, the malformed-track one, and nothing
     * from the crossing DONE. */
    terminals_t t = tally_terminals(sv);
    MOQ_TEST_CHECK_EQ_INT((int)t.publish_finished, 1);
    MOQ_TEST_CHECK_EQ_U64(t.publish_finished_cause, MOQ_REQUEST_TERMINAL_MALFORMED_TRACK);
    MOQ_TEST_CHECK_EQ_INT((int)t.publish_error, 0);
    MOQ_TEST_CHECK_EQ_INT((int)t.unsubscribed, 0);
    MOQ_TEST_CHECK_EQ_INT((int)t.subscribe_done, 0);
    MOQ_TEST_CHECK_EQ_INT((int)t.subscribe_error, 0);
    MOQ_TEST_CHECK_EQ_INT((int)t.closed, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_cancel_publication(sv, spub, 0, 3000),
                          (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);
    destroy(c, sv);
    if (failures == before) printf("PASS: d16 malformed track, crossing PUBLISH_DONE (PUBLISH)\n");
}

int main(void)
{
    test_subscribe_origin();
    test_publish_origin();
    if (failures) {
        fprintf(stderr, "FAIL: test_d16_cancel_crossing_done (%d)\n", failures);
        return 1;
    }
    printf("PASS: test_d16_cancel_crossing_done\n");
    return 0;
}
