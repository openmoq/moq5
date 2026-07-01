/*
 * Same-shard fan-out cookbook (compiled, CI-run).
 *
 * THIS IS A TEACHING ARTIFACT, NOT A LIBMOQ API. Same-shard fan-out is a
 * HOST pattern: libmoq provides no fan-out helper, no recipient-group
 * object, and no cross-session membership state. The host owns the
 * recipient list (a plain local array here) and loops over it, calling
 * the ordinary per-session publish API once per recipient.
 *
 * It demonstrates two invariants:
 *
 *  1. SHARED PAYLOAD, NO COPY (same shard). One moq_rcbuf_t payload is
 *     written to every same-shard recipient. moq_pub_write_object retains
 *     it via a non-atomic incref (cheap), so the buffer is shared, never
 *     cloned. We prove this by watching moq_rcbuf_refcount rise by exactly
 *     one per accepted write. (Crossing a shard boundary would instead
 *     require moq_rcbuf_clone(dst_alloc, src, out) /
 *     moq::buffer::clone_for_shard(dst_alloc) — not used here, because all
 *     recipients live on this one thread / executor-affinity domain.)
 *
 *  2. INDEPENDENT PER-RECIPIENT BACKPRESSURE. Recipients are handled
 *     independently: if one recipient's session action queue is full,
 *     moq_pub_write_object returns MOQ_ERR_WOULD_BLOCK for THAT recipient
 *     only. The host records it for retry and KEEPS GOING — the block must
 *     not abort the fan-out or drop the other recipients' data. Here
 *     recipient B models a slow/backed-up consumer (its action queue is
 *     pre-filled), while A and C accept normally; B is retried after its
 *     queue drains.
 *
 * "Blocked recipient" here means: that recipient's session action queue is
 * full, so the publisher cannot enqueue the SEND_DATA action and returns
 * MOQ_ERR_WOULD_BLOCK. The host drains (pumps) that session and retries.
 *
 * No cache / namespace / registry / routing policy appears anywhere: the
 * recipient list is a local array, nothing in libmoq owns the membership.
 */

#include <moq/publisher.h>
#include <moq/sim.h>
#include "test_session_support.h"
#include <stdio.h>
#include <string.h>

static int failures = 0;

/* Shared track identity - each recipient is an independent session pair.
 * Plain aggregate init (not MOQ_BYTES_LITERAL): a compound literal is not
 * a constant initializer element at file scope under ISO C (-Wpedantic). */
static moq_bytes_t g_ns_parts[] = {
    { .data = (const uint8_t *)"live", .len = sizeof("live") - 1 },
};

/* One host-owned recipient: a same-shard subscriber session reached via a
 * publisher + track with an accepted subscription. Pure local state — not
 * a libmoq object. */
typedef struct {
    moq_simpair_t   *sp;
    moq_publisher_t *pub;
    moq_pub_track_t *track;
    bool             delivered;  /* host-tracked: write accepted */
    bool             blocked;    /* host-tracked: WOULD_BLOCK, needs retry */
} recipient_t;

/* Build one recipient: publisher on the server side, accepted subscription
 * from the client. Mirrors test_publisher.c's write_single_sub setup. */
static void recipient_setup(const moq_alloc_t *alloc, uint64_t seed,
                            uint32_t max_actions, recipient_t *r)
{
    memset(r, 0, sizeof(*r));

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = seed;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.max_actions = max_actions;  /* 0 = default */
    moq_simpair_create(&cfg, &r->sp);
    moq_simpair_start(r->sp);
    moq_simpair_run_until_quiescent(r->sp, 8, NULL);

    moq_event_t ev;
    if (moq_session_poll_events(moq_simpair_client(r->sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    if (moq_session_poll_events(moq_simpair_server(r->sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);

    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_pub_create(moq_simpair_server(r->sp), alloc, &pcfg, &r->pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = g_ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_pub_add_track(r->pub, &tcfg, moq_simpair_now_us(r->sp), &r->track);

    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = g_ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t sub_h;
    moq_session_subscribe(moq_simpair_client(r->sp), &scfg,
                          moq_simpair_now_us(r->sp), &sub_h);
    moq_simpair_run_until_quiescent(r->sp, 8, NULL);

    /* Server accepts the subscribe. */
    if (moq_session_poll_events(moq_simpair_server(r->sp), &ev, 1) == 1) {
        moq_pub_event_result_t res;
        moq_pub_handle_event(r->pub, &ev, moq_simpair_now_us(r->sp), &res);
        moq_event_cleanup(&ev);
    }
    moq_simpair_run_until_quiescent(r->sp, 8, NULL);
    if (moq_session_poll_events(moq_simpair_client(r->sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
}

/* Pump a recipient's pair to quiescence: the sim consumes each session's
 * queued actions (freeing the action queue — the "downstream caught up"
 * step) and delivers them, producing events on the peer. Does NOT poll
 * events, so a following recipient_received() can still observe them. */
static void recipient_pump(recipient_t *r)
{
    moq_simpair_run_until_quiescent(r->sp, 32, NULL);
}

/* True if recipient's client subscriber received an object with (g, o).
 * Drains the client event queue (so it must be called after pumping; it
 * consumes the events it inspects). */
static bool recipient_received(recipient_t *r, uint64_t g, uint64_t o)
{
    bool found = false;
    moq_event_t evts[16]; size_t ne;
    while ((ne = moq_session_poll_events(moq_simpair_client(r->sp), evts, 16)) > 0) {
        for (size_t i = 0; i < ne; i++) {
            if (evts[i].kind == MOQ_EVENT_OBJECT_RECEIVED &&
                evts[i].u.object_received.group_id == g &&
                evts[i].u.object_received.object_id == o)
                found = true;
            moq_event_cleanup(&evts[i]);
        }
    }
    return found;
}

static void recipient_teardown(recipient_t *r)
{
    moq_pub_remove_track(r->pub, r->track, moq_simpair_now_us(r->sp));
    moq_pub_destroy(r->pub);
    /* Final drain: empty both ends so all queued action refs are released. */
    recipient_pump(r);
    moq_event_t evts[16]; size_t ne;
    while ((ne = moq_session_poll_events(moq_simpair_client(r->sp), evts, 16)) > 0)
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
    while ((ne = moq_session_poll_events(moq_simpair_server(r->sp), evts, 16)) > 0)
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
    moq_action_t acts[16]; size_t na;
    while ((na = moq_session_poll_actions(moq_simpair_client(r->sp), acts, 16)) > 0)
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    while ((na = moq_session_poll_actions(moq_simpair_server(r->sp), acts, 16)) > 0)
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    moq_simpair_destroy(r->sp);
}

int main(void)
{
    /* One shard = this thread; one allocator backs every recipient and the
     * shared payload, so a single balance check covers the whole shard. */
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);

    /* Three host-owned recipients on the same shard. B has a shallow action
     * queue so we can model it as a backed-up/slow consumer. */
    recipient_t rcpts[3];
    recipient_setup(&alloc, 42, 0,  &rcpts[0]);  /* A: default queue */
    recipient_setup(&alloc, 43, 8,  &rcpts[1]);  /* B: shallow queue  */
    recipient_setup(&alloc, 44, 0,  &rcpts[2]);  /* C: default queue */

    /* --- Make recipient B "blocked": fill its action queue with a
     *     separate filler stream (kept off the shared-payload refcount so
     *     the shared-reuse math below stays exact). --- */
    {
        uint8_t fill[] = { 0x01 };
        moq_rcbuf_t *filler = NULL;
        moq_rcbuf_create(&alloc, fill, sizeof(fill), &filler);
        moq_result_t frc = MOQ_OK;
        for (uint64_t oid = 0; oid < 1000 && frc == MOQ_OK; oid++)
            frc = moq_pub_write_object(rcpts[1].pub, rcpts[1].track,
                                       0, oid, filler,
                                       moq_simpair_now_us(rcpts[1].sp));
        MOQ_TEST_CHECK(frc == MOQ_ERR_WOULD_BLOCK);  /* B's queue is now full */
        moq_rcbuf_decref(filler);
    }

    /* --- The shared payload and the host fan-out loop --- */
    uint8_t data[] = { 0xDE, 0xAD, 0xBE, 0xEF };
    moq_rcbuf_t *payload = NULL;
    moq_rcbuf_create(&alloc, data, sizeof(data), &payload);
    MOQ_TEST_CHECK(moq_rcbuf_refcount(payload) == 1);

    const uint64_t FAN_G = 99, FAN_O = 7;  /* distinctive fan-out object id */

    /* The cookbook loop: one shared payload, N recipients, per-recipient
     * result handled independently; a block never aborts the loop. */
    int visited = 0, accepted = 0, blocked = 0;
    for (size_t i = 0; i < 3; i++) {
        visited++;
        moq_result_t rc = moq_pub_write_object(rcpts[i].pub, rcpts[i].track,
                                               FAN_G, FAN_O, payload,
                                               moq_simpair_now_us(rcpts[i].sp));
        if (rc == MOQ_OK) {
            rcpts[i].delivered = true;
            accepted++;
        } else if (rc == MOQ_ERR_WOULD_BLOCK) {
            rcpts[i].blocked = true;   /* record for retry, do NOT abort */
            blocked++;
        } else {
            MOQ_TEST_CHECK(0);         /* unexpected error */
        }
    }

    /* The loop processed every recipient despite B blocking. */
    MOQ_TEST_CHECK(visited == 3);
    MOQ_TEST_CHECK(rcpts[0].delivered && !rcpts[0].blocked);  /* A accepted */
    MOQ_TEST_CHECK(!rcpts[1].delivered && rcpts[1].blocked);  /* B blocked  */
    MOQ_TEST_CHECK(rcpts[2].delivered && !rcpts[2].blocked);  /* C accepted */
    MOQ_TEST_CHECK(accepted == 2 && blocked == 1);

    /* SHARED, NOT CLONED: each accepted write incref'd the SAME payload.
     * refcount == 1 (ours) + 2 (A, C). B's blocked write took no ref. */
    MOQ_TEST_CHECK(moq_rcbuf_refcount(payload) == 3);

    /* A and C delivered to their subscribers, independent of B's block. */
    recipient_pump(&rcpts[0]);
    recipient_pump(&rcpts[2]);
    MOQ_TEST_CHECK(recipient_received(&rcpts[0], FAN_G, FAN_O));
    MOQ_TEST_CHECK(recipient_received(&rcpts[2], FAN_G, FAN_O));

    /* --- Retry the blocked recipient(s) after their queues drain. The
     *     blocked recipient's data is NOT dropped — it is retried. --- */
    for (size_t i = 0; i < 3; i++) {
        if (!rcpts[i].blocked) continue;
        recipient_pump(&rcpts[i]);    /* downstream catches up; queue frees */
        moq_result_t rc = moq_pub_write_object(rcpts[i].pub, rcpts[i].track,
                                               FAN_G, FAN_O, payload,
                                               moq_simpair_now_us(rcpts[i].sp));
        MOQ_TEST_CHECK(rc == MOQ_OK);
        rcpts[i].blocked = false;
        rcpts[i].delivered = true;
    }
    MOQ_TEST_CHECK(rcpts[1].delivered && !rcpts[1].blocked);

    /* B's retried object reaches its subscriber too. */
    recipient_pump(&rcpts[1]);
    MOQ_TEST_CHECK(recipient_received(&rcpts[1], FAN_G, FAN_O));

    /* Drop our reference; recipients still hold theirs until torn down. */
    moq_rcbuf_decref(payload);

    for (size_t i = 0; i < 3; i++)
        recipient_teardown(&rcpts[i]);

    /* Whole shard accounted for: payload + all session/publisher storage. */
    MOQ_TEST_CHECK(as.balance == 0);

    MOQ_TEST_PASS("test_shard_fanout_cookbook");
    return failures;
}
