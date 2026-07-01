/*
 * Receiver teardown RESUMES under transient session-queue backpressure.
 *
 * p2-medium__12 follow-up (Codex review): moq_session_unsubscribe() can return
 * MOQ_ERR_WOULD_BLOCK (action queue full) or MOQ_ERR_BUFFER (control send
 * scratch momentarily full) before it frees the session entry. The teardown task
 * must treat both as transient -- preserve its cursor + the subscriber and ask
 * the pump to retry the SAME task node on a later cycle -- never advance past a
 * still-live subscription and destroy the facade (which would recreate the
 * stale-subscription bug under backpressure), and never depend on a second task
 * allocation.
 *
 * This drives the real receiver_hook to ACTIVE catalog + video subscriptions
 * over a simpair client whose action queue is capped at ONE entry, posts the
 * PRODUCTION teardown task to a (bare) test endpoint, and drives the REAL pump
 * drain (ep_drain_posted, with requeue-on-WOULD_BLOCK) across cycles:
 *
 *   cycle 1: cancel catalog -> queues 1 UNSUBSCRIBE (queue now full); cancel
 *            video -> WOULD_BLOCK -> task returns WOULD_BLOCK -> REQUEUED (not
 *            freed, subscriber intact). Exactly one UNSUBSCRIBE emitted so far.
 *   drain the client action queue (frees the slot).
 *   cycle 2: resume at the video handle -> queues the 2nd UNSUBSCRIBE -> task
 *            completes, node freed, facade destroyed.
 *
 * Asserts: the task is requeued (not dropped) after cycle 1, exactly one
 * UNSUBSCRIBE after cycle 1, and two total after cycle 2. Run under ASan this
 * also proves the requeue path leaks neither the task node nor the subscriber.
 *
 * The teardown must requeue across the block so both UNSUBSCRIBEs are emitted;
 * an advance-and-destroy-on-block teardown would emit only 1 and destroy the
 * facade (cycle 2 then has no task -> total stuck at 1).
 */
#include <moq/media_receiver.h>
#include <moq/endpoint.h>
#include <moq/msf.h>
#include <moq/sim.h>
#include <moq/session.h>
#include <moq/control.h>
#include <moq/codec.h>
#include "test_support.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>

static int failures = 0;

/* A public-post task that always asks to retry. The public path must ignore the
 * return value and run it exactly once (counter increments once, node freed). */
static moq_result_t public_wouldblock_task(moq_endpoint_t *ep,
                                           moq_session_t *session,
                                           uint64_t now_us, void *ctx)
{
    (void)ep; (void)session; (void)now_us;
    (*(int *)ctx)++;
    return MOQ_ERR_WOULD_BLOCK;
}

/* -- Test seams -------------------------------------------------------- */
moq_media_receiver_t *moq_media_receiver_test_new_cfg(
    const moq_media_receiver_cfg_t *cfg);
void moq_media_receiver_test_pump(moq_media_receiver_t *r,
                                  moq_session_t *session, uint64_t now_us);
moq_result_t moq_media_receiver_test_post_teardown(moq_media_receiver_t *r,
                                                   moq_endpoint_t *ep);
moq_endpoint_t *moq_endpoint_test_make_bare(void);
void moq_endpoint_test_free_bare(moq_endpoint_t *ep);
void moq_endpoint_test_drain_posted(moq_endpoint_t *ep, moq_session_t *session,
                                    uint64_t now_us);
size_t moq_endpoint_test_task_count(moq_endpoint_t *ep);

/* -- Scripted-peer byte builders --------------------------------------- */

static size_t put_varint(uint8_t *buf, uint64_t v)
{
    if (v < 0x40) { buf[0] = (uint8_t)v; return 1; }
    if (v < 0x4000) {
        buf[0] = (uint8_t)(0x40 | (v >> 8));
        buf[1] = (uint8_t)(v & 0xff);
        return 2;
    }
    if (v < 0x40000000ull) {
        buf[0] = (uint8_t)(0x80 | (v >> 24));
        buf[1] = (uint8_t)((v >> 16) & 0xff);
        buf[2] = (uint8_t)((v >> 8) & 0xff);
        buf[3] = (uint8_t)(v & 0xff);
        return 4;
    }
    buf[0] = (uint8_t)(0xc0 | (v >> 56));
    for (int i = 1; i < 8; i++)
        buf[i] = (uint8_t)((v >> (8 * (7 - i))) & 0xff);
    return 8;
}

static size_t build_subscribe_ok(uint8_t *out, size_t out_cap,
                                 uint64_t request_id, uint64_t track_alias)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, out, out_cap);
    if (moq_d16_encode_subscribe_ok(&w, request_id, track_alias,
                                    NULL, 0, NULL, 0) < 0)
        return 0;
    return moq_buf_writer_offset(&w);
}

typedef struct {
    bool     have_catalog_rid;
    uint64_t catalog_rid;
    bool     have_video_rid;
    uint64_t video_rid;
    int      subscribe_seen;
    int      unsubscribe_seen;
} learned_t;

static void drain_actions(moq_session_t *client, learned_t *l)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(client, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind != MOQ_ACTION_SEND_CONTROL) continue;
            moq_control_envelope_t env;
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, acts[i].u.send_control.data,
                                acts[i].u.send_control.len);
            if (moq_control_decode_envelope(&r, &env) < 0) continue;
            if (env.msg_type == MOQ_D16_SUBSCRIBE) {
                moq_bytes_t ns_parts[8];
                moq_kvp_entry_t params[16];
                moq_d16_subscribe_t s;
                memset(&s, 0, sizeof(s));
                s.params = params;
                s.params_cap = 16;
                if (moq_d16_decode_subscribe(env.payload, env.payload_len,
                                             ns_parts, 8, &s) < 0)
                    continue;
                l->subscribe_seen++;
                if (!l->have_catalog_rid) {
                    l->have_catalog_rid = true;
                    l->catalog_rid = s.request_id;
                } else if (!l->have_video_rid) {
                    l->have_video_rid = true;
                    l->video_rid = s.request_id;
                }
            } else if (env.msg_type == MOQ_D16_UNSUBSCRIBE) {
                l->unsubscribe_seen++;
            }
        }
    }
}

static const char CATALOG_JSON[] =
    "{\"version\":1,\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"avc1.42e01e\"}]}";

int main(void)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.seed = 42;
    cfg.initial_now_us = 1000;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.max_actions = 1;   /* one queued action -> the 2nd cancel WOULD_BLOCK */

    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 64, NULL);

    moq_session_t *client = moq_simpair_client(sp);
    moq_session_t *server = moq_simpair_server(sp);
    MOQ_TEST_CHECK(moq_session_state(client) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(server) == MOQ_SESS_ESTABLISHED);
    { moq_event_t ev;
      while (moq_session_poll_events(client, &ev, 1) == 1) moq_event_cleanup(&ev);
      while (moq_session_poll_events(server, &ev, 1) == 1) moq_event_cleanup(&ev); }

    moq_bytes_t ns_parts[2] = {
        MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_live(&rcfg);
    rcfg.namespace_.parts = ns_parts;
    rcfg.namespace_.count = 2;
    rcfg.auto_subscribe = true;
    rcfg.time_mode = MOQ_MEDIA_TIME_RAW;
    rcfg.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    rcfg.overflow.max_objects = 64;
    rcfg.overflow.max_bytes = 1u << 20;

    moq_media_receiver_t *r = moq_media_receiver_test_new_cfg(&rcfg);
    MOQ_TEST_CHECK(r != NULL);

    uint64_t now = moq_simpair_now_us(sp);
    learned_t learned;
    memset(&learned, 0, sizeof(learned));

    uint8_t ctrl[256];
    moq_stream_ref_t rx_cat = moq_stream_ref_from_u64(101);
    bool cat_ok_sent = false, cat_data_sent = false, video_ok_sent = false;
    bool catalog_ready = false;

    for (int cycle = 0; cycle < 80; cycle++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_actions(client, &learned);

        if (learned.have_catalog_rid && !cat_ok_sent) {
            size_t n = build_subscribe_ok(ctrl, sizeof(ctrl),
                                          learned.catalog_rid, 0);
            MOQ_TEST_CHECK(n > 0);
            MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n, now) >= 0);
            cat_ok_sent = true;
        }
        if (cat_ok_sent && !cat_data_sent) {
            uint8_t buf[512];
            size_t o = 0;
            buf[o++] = 0x30;
            o += put_varint(buf + o, 0);
            o += put_varint(buf + o, 0);
            o += put_varint(buf + o, 0);
            size_t jlen = sizeof(CATALOG_JSON) - 1;
            o += put_varint(buf + o, jlen);
            memcpy(buf + o, CATALOG_JSON, jlen);
            o += jlen;
            MOQ_TEST_CHECK(moq_session_on_data_bytes(
                client, rx_cat, buf, o, /*fin=*/true, now) >= 0);
            cat_data_sent = true;
        }
        if (catalog_ready && learned.have_video_rid && !video_ok_sent) {
            size_t n = build_subscribe_ok(ctrl, sizeof(ctrl),
                                          learned.video_rid, 2);
            MOQ_TEST_CHECK(n > 0);
            MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n, now) >= 0);
            video_ok_sent = true;
        }

        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(r, &te, sizeof(te)) == MOQ_OK) {
            if (te.kind == MOQ_MEDIA_CATALOG_READY) catalog_ready = true;
        }
        (void)moq_session_process_pending(client, now);
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));
        if (video_ok_sent) break;
    }

    /* Settle both SUBSCRIBE_OK events into ACTIVE facade tracks. */
    for (int cycle = 0; cycle < 8; cycle++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_actions(client, &learned);
        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(r, &te, sizeof(te)) == MOQ_OK) { }
        (void)moq_session_process_pending(client, now);
    }

    MOQ_TEST_CHECK(catalog_ready);
    MOQ_TEST_CHECK_EQ_INT(learned.subscribe_seen, 2);
    MOQ_TEST_CHECK(video_ok_sent);

    /* Clean the action queue so it starts empty (cap 1) for teardown. */
    learned.unsubscribe_seen = 0;
    drain_actions(client, &learned);
    MOQ_TEST_CHECK_EQ_INT(learned.unsubscribe_seen, 0);

    /* Post the production teardown task to a bare test endpoint. */
    moq_endpoint_t *ep = moq_endpoint_test_make_bare();
    MOQ_TEST_CHECK(ep != NULL);
    now += 1000;
    MOQ_TEST_CHECK(moq_media_receiver_test_post_teardown(r, ep) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_test_task_count(ep), 1);

    /* Cycle 1: catalog cancel fits (queue now full), video cancel WOULD_BLOCKs.
     * The task must be REQUEUED, not dropped, and exactly one UNSUBSCRIBE out. */
    now += 1000;
    moq_endpoint_test_drain_posted(ep, client, now);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_test_task_count(ep), 1); /* requeued */
    drain_actions(client, &learned);
    fprintf(stderr, "after cycle1: unsubscribes=%d tasks=%d\n",
            learned.unsubscribe_seen, (int)moq_endpoint_test_task_count(ep));
    MOQ_TEST_CHECK_EQ_INT(learned.unsubscribe_seen, 1);

    /* Cycle 2: queue drained, resume at the video handle -> 2nd UNSUBSCRIBE,
     * task completes and is freed. */
    now += 1000;
    moq_endpoint_test_drain_posted(ep, client, now);
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_test_task_count(ep), 0); /* done */
    drain_actions(client, &learned);
    fprintf(stderr, "after cycle2: unsubscribes=%d tasks=%d\n",
            learned.unsubscribe_seen, (int)moq_endpoint_test_task_count(ep));
    MOQ_TEST_CHECK_EQ_INT(learned.unsubscribe_seen, 2);

    moq_endpoint_test_free_bare(ep);

    /* Contract guard: the PUBLIC post() path stays exactly-once. A public task
     * that returns MOQ_ERR_WOULD_BLOCK must still run once and be FREED (its
     * return value is ignored) -- only the internal retryable path requeues.
     * (Without scoping, this task would be re-invoked with the same ctx.) */
    moq_endpoint_t *ep2 = moq_endpoint_test_make_bare();
    MOQ_TEST_CHECK(ep2 != NULL);
    int runs = 0;
    MOQ_TEST_CHECK(moq_endpoint_post(ep2, public_wouldblock_task, &runs) == MOQ_OK);
    moq_endpoint_test_drain_posted(ep2, client, now);
    MOQ_TEST_CHECK_EQ_INT(runs, 1);                                   /* ran once */
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_test_task_count(ep2), 0); /* freed */
    moq_endpoint_test_free_bare(ep2);

    moq_simpair_destroy(sp);

    if (failures == 0)
        MOQ_TEST_PASS("media_receiver_teardown_retry");
    return failures ? 1 : 0;
}
