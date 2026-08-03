/*
 * Media-receiver update-acknowledgment attribution (scripted peer, no
 * network) -- the attribution state machine end to end on a REAL
 * moq_media_receiver_t driven through its real pump hook.
 *
 * The scripted draft-16 peer (same shape as test_media_receiver_scripted.c)
 * establishes the catalog + one auto-subscribed LOC video track, then this
 * test drives app pause/resume toggles and flow-control pressure, learns
 * every outbound REQUEST_UPDATE (request id + FORWARD value), and injects
 * REQUEST_OK acknowledgments at controlled instants. Assertions:
 *
 *   A  app pause ack  -> exactly one MOQ_MEDIA_TRACK_UPDATE_OK, stable
 *      handle; resume ack with a LARGEST param -> event carries the scalars.
 *   B  toggle-while-outstanding -> two wire updates -> two ordered events;
 *      no extra REQUEST_UPDATE goes out while one is pending (WRONG_STATE
 *      retry path never duplicates).
 *   C  idempotent repeat toggle -> no wire update, no event.
 *   D  THE FLOW-RESUME DISCRIMINATOR: flow pause sends
 *      Forward=0 (ack SILENT) -> app pause+resume while still flow-paused
 *      (wire-neutral, reconciled silently) -> flow resume sends Forward=1
 *      CARRYING the newer app generations -> that ack stays SILENT.
 *   E  queue-time scalar copies: two attributed acks queued before any
 *      poll -> each polled event carries ITS OWN Largest, not the latest.
 *
 *   F  deterministic interleaving discriminators on the production
 *      attribution classifier (test seam; snapshot semantics).
 *   G  same-track live->VOD rearm: terminating with an app-attributed
 *      update OUTSTANDING clears the pending attribution (seam-proven),
 *      and the replacement subscription raises no phantom event.
 */
#include <moq/media_receiver.h>
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

moq_media_receiver_t *moq_media_receiver_test_new_cfg(
    const moq_media_receiver_cfg_t *cfg);
void moq_media_receiver_test_free(moq_media_receiver_t *r);
void moq_media_receiver_test_pump(moq_media_receiver_t *r,
                                  moq_session_t *session, uint64_t now_us);
void moq_media_receiver_test_classify(uint64_t gen_snap, uint64_t reconciled,
                                      bool *out_notify,
                                      uint64_t *out_reconciled);
void moq_media_receiver_test_track_attrib(moq_media_receiver_t *r,
                                          moq_media_track_t *t,
                                          bool *out_notify,
                                          uint64_t *out_sent_gen,
                                          uint64_t *out_app_gen,
                                          uint64_t *out_reconciled);
void moq_media_receiver_test_set_race_hook(void (*fn)(void *), void *ctx);

/* One-shot race hook: re-enables the video track at the reconciler's racy
 * instant (between the snapshot and the wire send). */
typedef struct { moq_media_receiver_t *r; moq_media_track_t *track;
                 int fired; } race_ctx_t;
static void race_resubscribe_hook(void *ctx)
{
    race_ctx_t *rc = (race_ctx_t *)ctx;
    if (rc->fired) return;
    rc->fired = 1;
    (void)moq_media_receiver_subscribe_track(rc->r, rc->track, NULL);
}

static size_t put_varint(uint8_t *buf, uint64_t v)
{
    if (v < 0x40) { buf[0] = (uint8_t)v; return 1; }
    if (v < 0x4000) {
        buf[0] = (uint8_t)(0x40 | (v >> 8));
        buf[1] = (uint8_t)(v & 0xff);
        return 2;
    }
    buf[0] = (uint8_t)(0x80 | (v >> 24));
    buf[1] = (uint8_t)((v >> 16) & 0xff);
    buf[2] = (uint8_t)((v >> 8) & 0xff);
    buf[3] = (uint8_t)(v & 0xff);
    return 4;
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

/* REQUEST_OK, optionally carrying a LARGEST_OBJECT param {group, object}. */
static size_t build_request_ok(uint8_t *out, size_t out_cap,
                               uint64_t request_id,
                               bool with_largest, uint64_t lg, uint64_t lo)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, out, out_cap);
    if (with_largest) {
        uint8_t loc[16];
        size_t ll = put_varint(loc, lg);
        ll += put_varint(loc + ll, lo);
        moq_kvp_entry_t p = {
            .type = MOQ_MSG_PARAM_LARGEST_OBJECT,
            .value = loc, .value_len = ll, .is_varint = false };
        if (moq_d16_encode_request_ok(&w, request_id, &p, 1) < 0) return 0;
    } else {
        if (moq_d16_encode_request_ok(&w, request_id, NULL, 0) < 0) return 0;
    }
    return moq_buf_writer_offset(&w);
}

/* Framed draft-16 PUBLISH_DONE for a subscription's request id. */
static size_t build_publish_done(uint8_t *out, size_t out_cap,
                                 uint64_t request_id, uint64_t status)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, out, out_cap);
    if (moq_d16_encode_publish_done(&w, request_id, status, 0, NULL, 0) < 0)
        return 0;
    return moq_buf_writer_offset(&w);
}

/* Learned outbound state: SUBSCRIBE request ids + the latest pending
 * REQUEST_UPDATE (id + Forward value), plus a total update counter. */
typedef struct {
    bool     have_catalog_rid; uint64_t catalog_rid;
    bool     have_video_rid;   uint64_t video_rid;
    bool     have_audio_rid;   uint64_t audio_rid;
    bool     have_upd;         uint64_t upd_rid;      /* latest */
    bool     upd_forward;                              /* latest */
    /* All not-yet-acked updates, in arrival order: {rid, existing SUBSCRIBE
     * rid (correlates the track), forward}. */
    struct { uint64_t rid, existing; bool forward; } pending[8];
    int      pending_n;
    int      updates_seen;
    uint64_t last_sub_rid;
} learned_t;

static void drain_and_learn(moq_session_t *client, learned_t *l)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(client, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind != MOQ_ACTION_SEND_CONTROL) {
                moq_action_cleanup(&acts[i]);
                continue;
            }
            moq_control_envelope_t env;
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, acts[i].u.send_control.data,
                                acts[i].u.send_control.len);
            if (moq_control_decode_envelope(&r, &env) == MOQ_OK) {
                if (env.msg_type == MOQ_D16_SUBSCRIBE) {
                    moq_bytes_t ns_parts[8];
                    moq_kvp_entry_t params[16];
                    moq_d16_subscribe_t sb;
                    memset(&sb, 0, sizeof(sb));
                    sb.params = params; sb.params_cap = 16;
                    if (moq_d16_decode_subscribe(env.payload,
                            env.payload_len, ns_parts, 8, &sb) == MOQ_OK) {
                        if (!l->have_catalog_rid) {
                            l->have_catalog_rid = true;
                            l->catalog_rid = sb.request_id;
                        } else if (!l->have_video_rid) {
                            l->have_video_rid = true;
                            l->video_rid = sb.request_id;
                        } else if (!l->have_audio_rid) {
                            l->have_audio_rid = true;
                            l->audio_rid = sb.request_id;
                        }
                        l->last_sub_rid = sb.request_id;
                    }
                } else if (env.msg_type == MOQ_D16_REQUEST_UPDATE) {
                    moq_kvp_entry_t params[8];
                    moq_d16_request_update_t upd = {
                        .params = params, .params_cap = 8 };
                    if (moq_d16_decode_request_update(env.payload,
                            env.payload_len, &upd) == MOQ_OK) {
                        l->have_upd = true;
                        l->upd_rid = upd.request_id;
                        l->updates_seen++;
                        bool fwd = false;
                        for (size_t pi = 0; pi < upd.params_count; pi++) {
                            if (upd.params[pi].type != MOQ_MSG_PARAM_FORWARD)
                                continue;
                            uint64_t v = 0;
                            (void)moq_quic_varint_decode(
                                upd.params[pi].value,
                                upd.params[pi].value_len, &v);
                            fwd = (v != 0);
                        }
                        l->upd_forward = fwd;
                        if (l->pending_n < 8) {
                            l->pending[l->pending_n].rid = upd.request_id;
                            l->pending[l->pending_n].existing =
                                upd.existing_request_id;
                            l->pending[l->pending_n].forward = fwd;
                            l->pending_n++;
                        }
                    }
                }
            }
            moq_action_cleanup(&acts[i]);
        }
    }
}

typedef struct {
    int      n;
    moq_media_track_t *track;
    bool     has_largest;
    uint64_t lg[8], lo[8];   /* per-event copies, in poll order */
    bool     haslg[8];
} ackrec_t;

/* Drain track events; record UPDATE_OK occurrences in order. */
static moq_media_track_t *g_added_track;
static moq_media_track_t *g_added_audio;

static void drain_track_events(moq_media_receiver_t *r, ackrec_t *a,
                               bool *added, bool *ready)
{
    moq_media_track_event_t te;
    while (moq_media_receiver_poll_track(r, &te, sizeof(te)) == MOQ_OK) {
        if (te.kind == MOQ_MEDIA_TRACK_ADDED) {
            if (added) *added = true;
            if (te.desc && te.desc->name.len == 5 &&
                memcmp(te.desc->name.data, "video", 5) == 0)
                g_added_track = te.track;
            if (te.desc && te.desc->name.len == 5 &&
                memcmp(te.desc->name.data, "audio", 5) == 0)
                g_added_audio = te.track;
        }
        else if (te.kind == MOQ_MEDIA_CATALOG_READY) {
            if (ready) *ready = true;
        } else if (te.kind == MOQ_MEDIA_TRACK_UPDATE_OK) {
            if (a->n < 8) {
                a->haslg[a->n] = te.has_largest;
                a->lg[a->n] = te.largest_group;
                a->lo[a->n] = te.largest_object;
            }
            a->n++;
            a->track = te.track;
            a->has_largest = te.has_largest;
        }
    }
}

static const char CATALOG_JSON[] =
    "{\"version\":1,\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"avc1.42e01e\"},"
    "{\"name\":\"audio\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"audio\",\"codec\":\"opus\"}]}";

int main(void)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.seed = 42;
    cfg.initial_now_us = 1000;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 64;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 64;

    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    moq_session_t *client = moq_simpair_client(sp);
    { moq_event_t ev;
      while (moq_session_poll_events(client, &ev, 1) == 1)
          moq_event_cleanup(&ev);
      while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    moq_bytes_t ns_parts[2] = {
        MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_live(&rcfg);
    rcfg.namespace_.parts = ns_parts;
    rcfg.namespace_.count = 2;
    rcfg.auto_subscribe = true;
    rcfg.time_mode = MOQ_MEDIA_TIME_RAW;
    /* FLOW_CONTROL with a tiny queue so scripted objects can force the
     * flow-pause reconciliation deterministically. */
    rcfg.overflow.policy = MOQ_MEDIA_OVERFLOW_FLOW_CONTROL;
    rcfg.overflow.max_objects = 4;
    rcfg.overflow.max_bytes = 1u << 20;

    moq_media_receiver_t *r = moq_media_receiver_test_new_cfg(&rcfg);
    MOQ_TEST_CHECK(r != NULL);

    uint64_t now = moq_simpair_now_us(sp);
    learned_t learned; memset(&learned, 0, sizeof(learned));
    ackrec_t acks; memset(&acks, 0, sizeof(acks));
    uint8_t ctrl[256];
    moq_stream_ref_t rx_cat = moq_stream_ref_from_u64(101);
    uint64_t next_video_rx = 202;
    bool cat_ok_sent = false, cat_data_sent = false;
    bool video_ok_sent = false, audio_ok_sent = false;
    bool track_added = false, catalog_ready = false;

    /* -- Establish catalog + auto-subscribed video track ---------------- */
    for (int cycle = 0; cycle < 40 && !(video_ok_sent && audio_ok_sent);
         cycle++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
        if (learned.have_catalog_rid && !cat_ok_sent) {
            size_t n = build_subscribe_ok(ctrl, sizeof(ctrl),
                                          learned.catalog_rid, 0);
            MOQ_TEST_CHECK(n > 0);
            MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n,
                                                        now) >= 0);
            cat_ok_sent = true;
        }
        if (cat_ok_sent && !cat_data_sent) {
            uint8_t buf[512]; size_t o = 0;
            buf[o++] = 0x30;
            o += put_varint(buf + o, 0);
            o += put_varint(buf + o, 0);
            o += put_varint(buf + o, 0);
            size_t jlen = sizeof(CATALOG_JSON) - 1;
            o += put_varint(buf + o, jlen);
            memcpy(buf + o, CATALOG_JSON, jlen);
            o += jlen;
            MOQ_TEST_CHECK(moq_session_on_data_bytes(client, rx_cat, buf, o,
                                                     true, now) >= 0);
            cat_data_sent = true;
        }
        if (catalog_ready && learned.have_video_rid && !video_ok_sent) {
            size_t n = build_subscribe_ok(ctrl, sizeof(ctrl),
                                          learned.video_rid, 2);
            MOQ_TEST_CHECK(n > 0);
            MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n,
                                                        now) >= 0);
            video_ok_sent = true;
        }
        if (catalog_ready && learned.have_audio_rid && !audio_ok_sent) {
            size_t n = build_subscribe_ok(ctrl, sizeof(ctrl),
                                          learned.audio_rid, 3);
            MOQ_TEST_CHECK(n > 0);
            MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n,
                                                        now) >= 0);
            audio_ok_sent = true;
        }
        drain_track_events(r, &acks, &track_added, &catalog_ready);
        (void)moq_session_process_pending(client, now);
        MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));
    }
    MOQ_TEST_CHECK(track_added && catalog_ready && video_ok_sent &&
                   audio_ok_sent);
    MOQ_TEST_CHECK_EQ_INT(acks.n, 0);

    /* The stable video handle was captured from its TRACK_ADDED event. */
    moq_media_track_t *video = g_added_track;
    moq_media_track_t *audio = g_added_audio;
    MOQ_TEST_CHECK(video != NULL);
    MOQ_TEST_CHECK(audio != NULL);

    int updates_before;

    /* == A: app pause ack emits exactly once; resume ack carries Largest = */
    MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(r, video) == MOQ_OK);
    updates_before = learned.updates_seen;
    for (int i = 0; i < 4 && learned.updates_seen == updates_before; i++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
    }
    MOQ_TEST_CHECK_EQ_INT(learned.updates_seen, updates_before + 1);
    MOQ_TEST_CHECK(!learned.upd_forward);          /* pause = Forward 0 */
    {
        size_t n = build_request_ok(ctrl, sizeof(ctrl), learned.upd_rid,
                                    false, 0, 0);
        MOQ_TEST_CHECK(n > 0);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n,
                                                    now) >= 0);
    }
    now += 1000;
    moq_media_receiver_test_pump(r, client, now);
    drain_and_learn(client, &learned);
    drain_track_events(r, &acks, NULL, NULL);
    MOQ_TEST_CHECK_EQ_INT(acks.n, 1);              /* exactly one event */
    MOQ_TEST_CHECK(acks.track == video);           /* stable handle */
    MOQ_TEST_CHECK(!acks.haslg[0]);

    /* Resume: the ack carries LARGEST {7,3}; the event copies it. */
    MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(r, video,
                                                      NULL) == MOQ_OK);
    updates_before = learned.updates_seen;
    for (int i = 0; i < 4 && learned.updates_seen == updates_before; i++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
    }
    MOQ_TEST_CHECK_EQ_INT(learned.updates_seen, updates_before + 1);
    MOQ_TEST_CHECK(learned.upd_forward);           /* resume = Forward 1 */
    {
        size_t n = build_request_ok(ctrl, sizeof(ctrl), learned.upd_rid,
                                    true, 7, 3);
        MOQ_TEST_CHECK(n > 0);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n,
                                                    now) >= 0);
    }
    now += 1000;
    moq_media_receiver_test_pump(r, client, now);
    drain_and_learn(client, &learned);
    drain_track_events(r, &acks, NULL, NULL);
    MOQ_TEST_CHECK_EQ_INT(acks.n, 2);
    MOQ_TEST_CHECK(acks.haslg[1]);
    MOQ_TEST_CHECK_EQ_U64(acks.lg[1], 7);
    MOQ_TEST_CHECK_EQ_U64(acks.lo[1], 3);

    /* == B: toggle while outstanding -> two wire updates, two events;
     *       nothing extra goes out while one is pending. ================ */
    memset(&acks, 0, sizeof(acks));
    MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(r, video) == MOQ_OK);
    updates_before = learned.updates_seen;
    for (int i = 0; i < 4 && learned.updates_seen == updates_before; i++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
    }
    MOQ_TEST_CHECK_EQ_INT(learned.updates_seen, updates_before + 1);
    uint64_t pause_rid = learned.upd_rid;
    /* Race the resume in BEFORE the ack; pump: WRONG_STATE retry path must
     * NOT emit a second update while the first is outstanding. */
    MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(r, video,
                                                      NULL) == MOQ_OK);
    for (int i = 0; i < 3; i++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
    }
    MOQ_TEST_CHECK_EQ_INT(learned.updates_seen, updates_before + 1);
    {   /* ack #1 (the pause) */
        size_t n = build_request_ok(ctrl, sizeof(ctrl), pause_rid,
                                    false, 0, 0);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n,
                                                    now) >= 0);
    }
    for (int i = 0; i < 4 && learned.updates_seen == updates_before + 1;
         i++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
    }
    MOQ_TEST_CHECK_EQ_INT(learned.updates_seen, updates_before + 2);
    {   /* ack #2 (the racing resume) */
        size_t n = build_request_ok(ctrl, sizeof(ctrl), learned.upd_rid,
                                    false, 0, 0);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n,
                                                    now) >= 0);
    }
    now += 1000;
    moq_media_receiver_test_pump(r, client, now);
    drain_and_learn(client, &learned);
    drain_track_events(r, &acks, NULL, NULL);
    MOQ_TEST_CHECK_EQ_INT(acks.n, 2);   /* two attributed wire acks */

    /* == C: idempotent repeat toggle -> no wire, no event =============== */
    memset(&acks, 0, sizeof(acks));
    updates_before = learned.updates_seen;
    MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(r, video,
                                                      NULL) == MOQ_OK);
    for (int i = 0; i < 3; i++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
        drain_track_events(r, &acks, NULL, NULL);
    }
    MOQ_TEST_CHECK_EQ_INT(learned.updates_seen, updates_before);
    MOQ_TEST_CHECK_EQ_INT(acks.n, 0);

    /* == D: THE FLOW-RESUME DISCRIMINATOR =============================== *
     * Overfill the tiny object queue with VIDEO objects so the receiver
     * FLOW-pauses (Forward=0 to EVERY subscribed track, no app intent --
     * those acks must be silent). Then toggle the AUDIO track off+on while
     * still flow-paused: audio has no queued objects, so the toggle purges
     * nothing, the flow pause holds, and the toggle is WIRE-NEUTRAL
     * (reconciled silently). Then drain the video queue so flow RESUMES
     * (Forward=1 to both tracks); the audio resume CARRIES the coalesced
     * newer app generations -- the classifier keeps that ack SILENT (an
     * earlier rule
     * would emit here). */
    memset(&acks, 0, sizeof(acks));
    learned.pending_n = 0;
    updates_before = learned.updates_seen;
    {
        uint8_t buf[512]; size_t o = 0;
        buf[o++] = 0x30;
        o += put_varint(buf + o, 2);          /* video alias */
        o += put_varint(buf + o, 4);          /* group 4 */
        for (int k = 0; k < 6; k++) {         /* 6 > max_objects(4) */
            char frame[16];
            int fl = snprintf(frame, sizeof(frame), "flow:g4:o%d", k);
            o += put_varint(buf + o, 0);
            o += put_varint(buf + o, (uint64_t)fl);
            memcpy(buf + o, frame, (size_t)fl);
            o += (size_t)fl;
        }
        moq_stream_ref_t rx = moq_stream_ref_from_u64(next_video_rx++);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(client, rx, buf, o,
                                                 true, now) >= 0);
    }
    /* Flow pause: Forward=0 updates to BOTH tracks (one per track). */
    for (int i = 0; i < 8 && learned.updates_seen < updates_before + 2;
         i++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
    }
    MOQ_TEST_CHECK_EQ_INT(learned.updates_seen, updates_before + 2);
    for (int i = 0; i < learned.pending_n; i++) {
        MOQ_TEST_CHECK(!learned.pending[i].forward);
        size_t n = build_request_ok(ctrl, sizeof(ctrl),
                                    learned.pending[i].rid, false, 0, 0);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n,
                                                    now) >= 0);
    }
    learned.pending_n = 0;
    now += 1000;
    moq_media_receiver_test_pump(r, client, now);
    drain_and_learn(client, &learned);
    drain_track_events(r, &acks, NULL, NULL);
    MOQ_TEST_CHECK_EQ_INT(acks.n, 0);         /* flow acks: SILENT */

    /* Audio off+on while flow-paused: wire-neutral, reconciled silently
     * (audio has nothing queued, so nothing is purged and the flow pause
     * holds). */
    MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(r, audio) == MOQ_OK);
    MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(r, audio,
                                                      NULL) == MOQ_OK);
    updates_before = learned.updates_seen;
    for (int i = 0; i < 3; i++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
    }
    MOQ_TEST_CHECK_EQ_INT(learned.updates_seen, updates_before);

    /* Drain the video queue below low-water so flow RESUMES: Forward=1 to
     * both tracks. The audio one carries the coalesced app generations. */
    {
        moq_media_object_t obj;
        while (moq_media_receiver_poll_object(r, &obj, sizeof(obj))
               == MOQ_OK)
            moq_media_object_cleanup(&obj);
    }
    learned.pending_n = 0;
    for (int i = 0; i < 8 && learned.updates_seen < updates_before + 2;
         i++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
    }
    MOQ_TEST_CHECK_EQ_INT(learned.updates_seen, updates_before + 2);
    for (int i = 0; i < learned.pending_n; i++) {
        MOQ_TEST_CHECK(learned.pending[i].forward);
        size_t n = build_request_ok(ctrl, sizeof(ctrl),
                                    learned.pending[i].rid, true, 9, 9);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n,
                                                    now) >= 0);
    }
    learned.pending_n = 0;
    now += 1000;
    moq_media_receiver_test_pump(r, client, now);
    drain_and_learn(client, &learned);
    drain_track_events(r, &acks, NULL, NULL);
    /* Rev-2 (app_gen > last_emitted) would emit for the audio resume; the
     * attribution stays SILENT for both. */
    MOQ_TEST_CHECK_EQ_INT(acks.n, 0);

    /* == E: queue-time scalar copies -- two attributed acks queued before
     *       any poll each keep their OWN Largest. ======================= */
    memset(&acks, 0, sizeof(acks));
    MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(r, video) == MOQ_OK);
    updates_before = learned.updates_seen;
    for (int i = 0; i < 4 && learned.updates_seen == updates_before; i++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
    }
    MOQ_TEST_CHECK_EQ_INT(learned.updates_seen, updates_before + 1);
    {   /* pause ack with LARGEST {11,1} -- queued, NOT polled yet */
        size_t n = build_request_ok(ctrl, sizeof(ctrl), learned.upd_rid,
                                    true, 11, 1);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n,
                                                    now) >= 0);
    }
    now += 1000;
    moq_media_receiver_test_pump(r, client, now);   /* event queued */
    drain_and_learn(client, &learned);
    MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(r, video,
                                                      NULL) == MOQ_OK);
    updates_before = learned.updates_seen;
    for (int i = 0; i < 4 && learned.updates_seen == updates_before; i++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
    }
    MOQ_TEST_CHECK_EQ_INT(learned.updates_seen, updates_before + 1);
    {   /* resume ack with LARGEST {12,2} -- also queued before polling */
        size_t n = build_request_ok(ctrl, sizeof(ctrl), learned.upd_rid,
                                    true, 12, 2);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n,
                                                    now) >= 0);
    }
    now += 1000;
    moq_media_receiver_test_pump(r, client, now);
    drain_and_learn(client, &learned);
    /* NOW poll: each event carries its own scalars, in order. */
    drain_track_events(r, &acks, NULL, NULL);
    MOQ_TEST_CHECK_EQ_INT(acks.n, 2);
    MOQ_TEST_CHECK(acks.haslg[0]);
    MOQ_TEST_CHECK_EQ_U64(acks.lg[0], 11);
    MOQ_TEST_CHECK_EQ_U64(acks.lo[0], 1);
    MOQ_TEST_CHECK(acks.haslg[1]);
    MOQ_TEST_CHECK_EQ_U64(acks.lg[1], 12);
    MOQ_TEST_CHECK_EQ_U64(acks.lo[1], 2);

    /* == The PRODUCTION race, deterministically -- the app
     *       re-toggles at the exact instant between the reconciler's
     *       snapshot and its wire send. The pause update must (a) still
     *       send Forward=0 (snapshot target), (b) attribute to the FIRST
     *       toggle only, and (c) leave the racy toggle unreconciled so its
     *       OWN Forward=1 update sends and attributes next pass: exactly
     *       TWO attributed acks. The regressed rule (live app_gen at send)
     *       reconciles the racy toggle into the first send and silences
     *       the second ack. ============================================ */
    {
        memset(&acks, 0, sizeof(acks));
        race_ctx_t rctx = { r, video, 0 };
        moq_media_receiver_test_set_race_hook(race_resubscribe_hook, &rctx);
        MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(r, video)
                       == MOQ_OK);
        updates_before = learned.updates_seen;
        for (int i = 0; i < 4 && learned.updates_seen == updates_before;
             i++) {
            now += 1000;
            moq_media_receiver_test_pump(r, client, now);
            drain_and_learn(client, &learned);
        }
        moq_media_receiver_test_set_race_hook(NULL, NULL);
        MOQ_TEST_CHECK(rctx.fired == 1);
        MOQ_TEST_CHECK_EQ_INT(learned.updates_seen, updates_before + 1);
        MOQ_TEST_CHECK(!learned.upd_forward);   /* snapshot target held */
        {   /* ack the pause: attributed to the FIRST toggle */
            size_t n = build_request_ok(ctrl, sizeof(ctrl), learned.upd_rid,
                                        false, 0, 0);
            MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n,
                                                        now) >= 0);
        }
        /* The racy resume stayed unreconciled: its own update sends... */
        for (int i = 0; i < 4 && learned.updates_seen == updates_before + 1;
             i++) {
            now += 1000;
            moq_media_receiver_test_pump(r, client, now);
            drain_and_learn(client, &learned);
        }
        MOQ_TEST_CHECK_EQ_INT(learned.updates_seen, updates_before + 2);
        MOQ_TEST_CHECK(learned.upd_forward);
        {   /* ...and its ack attributes too. */
            size_t n = build_request_ok(ctrl, sizeof(ctrl), learned.upd_rid,
                                        false, 0, 0);
            MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n,
                                                        now) >= 0);
        }
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
        drain_track_events(r, &acks, NULL, NULL);
        MOQ_TEST_CHECK_EQ_INT(acks.n, 2);   /* one per toggle, never one */
    }

    /* == F: deterministic interleaving discriminators on the PRODUCTION
     *       classifier: the reconciled generation never
     *       advances past the SNAPSHOT, so a toggle landing between the
     *       snapshot and the send stays unreconciled. ================== */
    {
        bool notify; uint64_t rec;
        moq_media_receiver_test_classify(1, 0, &notify, &rec);
        MOQ_TEST_CHECK(notify);                 /* snapshot covers a toggle */
        MOQ_TEST_CHECK_EQ_U64(rec, 1);
        /* Live app_gen may already be 2 (toggle after the snapshot): the
         * classifier sees only the snapshot -- reconciled stays 1, so the
         * later toggle's own wire update still attributes next pass. */
        moq_media_receiver_test_classify(2, 1, &notify, &rec);
        MOQ_TEST_CHECK(notify);
        MOQ_TEST_CHECK_EQ_U64(rec, 2);
        /* Flow-only send: snapshot == reconciled -> no attribution. */
        moq_media_receiver_test_classify(2, 2, &notify, &rec);
        MOQ_TEST_CHECK(!notify);
        MOQ_TEST_CHECK_EQ_U64(rec, 2);
        /* Stale reconciled never regresses. */
        moq_media_receiver_test_classify(1, 3, &notify, &rec);
        MOQ_TEST_CHECK(!notify);
        MOQ_TEST_CHECK_EQ_U64(rec, 3);
    }

    /* == G: SAME-TRACK VOD-rearm attribution cleanup: the
     *       live subscription terminates (status 0x2) WHILE an
     *       app-attributed update is outstanding; the receiver re-arms the
     *       SAME stable track for VOD, and the pending attribution must
     *       NOT survive into the replacement subscription. ============= */
    {
        MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(r, video)
                       == MOQ_OK);
        updates_before = learned.updates_seen;
        for (int i = 0; i < 4 && learned.updates_seen == updates_before;
             i++) {
            now += 1000;
            moq_media_receiver_test_pump(r, client, now);
            drain_and_learn(client, &learned);
        }
        MOQ_TEST_CHECK_EQ_INT(learned.updates_seen, updates_before + 1);
        bool notify = false;
        moq_media_receiver_test_track_attrib(r, video, &notify, NULL,
                                             NULL, NULL);
        MOQ_TEST_CHECK(notify);   /* app-attributed update OUTSTANDING */

        /* Terminate the live subscription with Track Ended (0x2, count 0)
         * INSTEAD of acking the update: the receiver releases the facade
         * slot and re-arms the same track for VOD. */
        {
            size_t n = build_publish_done(ctrl, sizeof(ctrl),
                                          learned.video_rid, 0x2);
            MOQ_TEST_CHECK(n > 0);
            MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n,
                                                        now) >= 0);
        }
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
        /* CAUSAL cleanup proof: the pending attribution died with the live
         * subscription -- the replacement's first ack cannot inherit it. */
        uint64_t sent_gen = 99;
        moq_media_receiver_test_track_attrib(r, video, &notify, &sent_gen,
                                             NULL, NULL);
        MOQ_TEST_CHECK(!notify);
        MOQ_TEST_CHECK_EQ_U64(sent_gen, 0);

        /* The app re-enables; the receiver issues a REPLACEMENT SUBSCRIBE
         * for the same stable handle; established at alias 4. */
        MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(r, video,
                                                          NULL) == MOQ_OK);
        uint64_t old_rid = learned.last_sub_rid;   /* audio's, from setup */
        for (int i = 0; i < 6 && learned.last_sub_rid == old_rid; i++) {
            now += 1000;
            moq_media_receiver_test_pump(r, client, now);
            drain_and_learn(client, &learned);
        }
        MOQ_TEST_CHECK(learned.last_sub_rid != old_rid);
        {
            size_t n = build_subscribe_ok(ctrl, sizeof(ctrl),
                                          learned.last_sub_rid, 4);
            MOQ_TEST_CHECK(n > 0);
            MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n,
                                                        now) >= 0);
        }
        memset(&acks, 0, sizeof(acks));
        for (int i = 0; i < 3; i++) {
            now += 1000;
            moq_media_receiver_test_pump(r, client, now);
            drain_and_learn(client, &learned);
            drain_track_events(r, &acks, NULL, NULL);
        }
        /* No phantom event from the pre-rearm attribution. */
        MOQ_TEST_CHECK_EQ_INT(acks.n, 0);
    }

    MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));
    moq_media_receiver_test_free(r);
    moq_simpair_destroy(sp);

    if (failures == 0)
        MOQ_TEST_PASS("media_receiver_update_ack");
    return failures;
}
