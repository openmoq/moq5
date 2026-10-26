/*
 * Media_receiver facade intake test (scripted peer, no network).
 *
 * Pairs a REAL moq_media_receiver_t (driven through its real endpoint pump hook,
 * receiver_hook) with a SCRIPTED moqx-behavior peer -- no network, no moqx. The
 * scripted peer injects crafted draft-16 control + data bytes that mirror what
 * the moqx relay sends on the wire (captured), in the captured order:
 *
 *   SERVER_SETUP (via simpair handshake)
 *   SUBSCRIBE_OK(request_id=<catalog>, track_alias=0)   [catalog]
 *   catalog data object (track_alias 0)
 *   SUBSCRIBE_OK(request_id=<video>,  track_alias=2)    [video]
 *   video subgroup data (track_alias 2)
 *
 * moqx's subgroup header (captured): type byte 0x30 (no extensions, subgroup-id
 * mode ZERO, default priority) -> header is { type, track_alias varint,
 * group_id varint }. Each object: { objid_delta varint, payload_len varint,
 * payload } with object ids delta-decoded.
 *
 * The receiver is driven entirely through receiver_hook (the real facade
 * orchestration): catalog SUBSCRIBE, catalog object delivery + ingest,
 * TRACK_ADDED/CATALOG_READY, auto-subscribe of the video track, and media
 * object routing + parse. The request ids the receiver allocates are learned by
 * decoding its own outbound SUBSCRIBE control actions, so the injected
 * SUBSCRIBE_OKs echo the exact ids (no hard-coded guesses). The catalog is
 * delivered here via the live SUBSCRIBE object; the receiver may also issue a
 * catalog joining FETCH, which this harness does not answer (fetch count is
 * informational, not asserted).
 *
 * Invariant: LOC video objects delivered with no capture-timestamp extension
 * (as a moqx-style relay sends) are surfaced with has_capture_time=false, not
 * dropped. After TRACK_ADDED + CATALOG_READY, this test asserts all four
 * video objects surface. Completion then distinguishes a track declared VOD
 * from one created live: only the latter re-arms once, whether its conversion
 * catalog arrives before or after the done. A never-live track cannot become
 * live through a later catalog. Every outbound SUBSCRIBE is counted; the peer
 * replays the same group on any replacement so an erroneous re-arm also
 * exposes duplicate delivery. All inputs and pumps are synchronous.
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

/* -- Test seam (media_receiver.c, MOQ_MEDIA_RECEIVER_TESTING) ----------- */
moq_media_receiver_t *moq_media_receiver_test_new_cfg(
    const moq_media_receiver_cfg_t *cfg);
/* The SESSION-AWARE teardown seam: the pumped receiver_hook creates the
 * subscriber facade, so only this seam runs the production teardown (live-handle
 * snapshot, per-handle UNSUBSCRIBE on the supplied session, facade destroy)
 * before freeing the receiver. The raw shell free cannot retire a subscriber the
 * hook created. */
void moq_media_receiver_test_destroy_with_session(moq_media_receiver_t *r,
                                                  moq_session_t *session,
                                                  uint64_t now_us);
void moq_media_receiver_test_pump(moq_media_receiver_t *r,
                                  moq_session_t *session, uint64_t now_us);

/* -- Scripted-peer byte builders ---------------------------------------- */

/* Append a draft-16 varint (QUIC varint) to buf. */
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

/* Build a framed SUBSCRIBE_OK control message (request_id, track_alias, 0
 * params). moq_d16_encode_subscribe_ok already emits the full draft-16 frame
 * (type byte + uint16 length + payload via moq_control_write_header), so its
 * output is fed to moq_session_on_control_bytes directly -- exactly like
 * test_session_support's feed_subscribe_ok. Returns length written. */
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

/* Each video subscription has exactly one finished subgroup stream. */
static void feed_track_ended(moq_session_t *client, uint64_t request_id,
                             uint64_t now)
{
    uint8_t ctrl[64];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, ctrl, sizeof(ctrl));
    MOQ_TEST_CHECK(moq_d16_encode_publish_done(&w, request_id, 0x2, 1,
                                               NULL, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl,
                           moq_buf_writer_offset(&w), now) >= 0);
}

static void feed_catalog(moq_session_t *client, uint64_t stream, uint64_t group,
                         const char *json, uint64_t now)
{
    uint8_t buf[512];
    size_t o = 0, len = strlen(json);
    buf[o++] = 0x30;
    o += put_varint(buf + o, 0);       /* catalog alias */
    o += put_varint(buf + o, group);
    o += put_varint(buf + o, 0);       /* object 0 */
    o += put_varint(buf + o, len);
    MOQ_TEST_CHECK(o + len <= sizeof(buf));
    if (o + len > sizeof(buf)) return;
    memcpy(buf + o, json, len);
    o += len;
    MOQ_TEST_CHECK(moq_session_on_data_bytes(client, moq_stream_ref_from_u64(stream),
                                             buf, o, true, now) >= 0);
}

/* The same four Objects, for the original subscription and any replacement. */
static void feed_video_group(moq_session_t *client, uint64_t stream,
                             uint64_t alias, uint64_t now)
{
    uint8_t buf[512];
    size_t o = 0;
    buf[o++] = 0x30;
    o += put_varint(buf + o, alias);
    o += put_varint(buf + o, 2);
    for (int k = 0; k < 4; k++) {
        char frame[16];
        int fl = snprintf(frame, sizeof(frame), "frame:g2:o%d", k);
        o += put_varint(buf + o, 0);
        o += put_varint(buf + o, (uint64_t)fl);
        memcpy(buf + o, frame, (size_t)fl);
        o += (size_t)fl;
    }
    MOQ_TEST_CHECK(moq_session_on_data_bytes(client, moq_stream_ref_from_u64(stream),
                                             buf, o, true, now) >= 0);
}

/* -- request-id learning: decode the receiver's outbound SUBSCRIBE actions -- */

typedef struct {
    bool     have_catalog_rid;
    uint64_t catalog_rid;
    bool     have_video_rid;
    uint64_t video_rid;
    uint64_t last_sub_rid;
    int      subscribe_seen;
    int      fetch_seen;
} learned_t;

/* Drain the client session's pending actions; decode each SUBSCRIBE to learn its
 * request id (first SUBSCRIBE = catalog, second = video) and count FETCHes.
 * Control actions are otherwise discarded (the scripted peer replies by direct
 * byte injection, not via the simpair server). */
static void drain_and_learn(moq_session_t *client, learned_t *l)
{
    moq_action_t acts[16];
    size_t n;
    for (;;) {
        moq_result_t rc = moq_session_poll_actions_ex(client, acts, 16,
                                                       sizeof(acts[0]), &n);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        if (rc != MOQ_OK || n == 0) break;
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind != MOQ_ACTION_SEND_CONTROL) {
                moq_action_cleanup(&acts[i]);
                continue;
            }
            moq_control_envelope_t env;
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, acts[i].u.send_control.data,
                                acts[i].u.send_control.len);
            rc = moq_control_decode_envelope(&r, &env);
            MOQ_TEST_CHECK(rc >= 0);
            if (rc < 0) {
                moq_action_cleanup(&acts[i]);
                continue;
            }
            if (env.msg_type == MOQ_D16_SUBSCRIBE) {
                moq_bytes_t ns_parts[8];
                moq_kvp_entry_t params[16];
                moq_d16_subscribe_t s;
                memset(&s, 0, sizeof(s));
                s.params = params;
                s.params_cap = 16;
                moq_result_t drc = moq_d16_decode_subscribe(
                    env.payload, env.payload_len, ns_parts, 8, &s);
                MOQ_TEST_CHECK(drc >= 0);
                if (drc < 0) {
                    moq_action_cleanup(&acts[i]);
                    continue;
                }
                l->subscribe_seen++;
                l->last_sub_rid = s.request_id;
                if (!l->have_catalog_rid) {
                    l->have_catalog_rid = true;
                    l->catalog_rid = s.request_id;
                } else if (!l->have_video_rid) {
                    l->have_video_rid = true;
                    l->video_rid = s.request_id;
                }
            } else if (env.msg_type == MOQ_D16_FETCH) {
                l->fetch_seen++;
            }
            moq_action_cleanup(&acts[i]);
        }
    }
}

/* Minimal MSF catalog declaring one live LOC video track. */
static const char CATALOG_JSON[] =
    "{\"version\":1,\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"avc1.42e01e\"}]}";

/* No duration is needed to declare a never-live VOD track. This also lets the
 * rejected update change only isLive, isolating the false-to-true violation. */
static const char VOD_CATALOG_JSON[] =
    "{\"version\":1,\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":false,"
    "\"role\":\"video\",\"codec\":\"avc1.42e01e\"}]}";
static const char CONVERSION_JSON[] =
    "{\"version\":1,\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":false,"
    "\"role\":\"video\",\"codec\":\"avc1.42e01e\",\"trackDuration\":5000}]}";

typedef struct {
    int updated;
    int ended;
} completion_t;

/* Pump every synchronous receiver stage and drain its outbound actions. There
 * is no transport timer or peer thread that can hide a replacement SUBSCRIBE. */
static void pump_completion(moq_media_receiver_t *r, moq_session_t *client,
                            learned_t *learned, moq_media_track_t *video,
                            completion_t *events, uint64_t *now)
{
    for (int i = 0; i < 8; i++) {
        *now += 1000;
        MOQ_TEST_CHECK(moq_session_process_pending(client, *now) >= 0);
        moq_media_receiver_test_pump(r, client, *now);
        drain_and_learn(client, learned);
        moq_media_track_event_t ev;
        while (moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK) {
            MOQ_TEST_CHECK(ev.track == video);
            if (ev.kind == MOQ_MEDIA_TRACK_UPDATED) {
                events->updated++;
            } else if (ev.kind == MOQ_MEDIA_TRACK_ENDED) {
                events->ended++;
                MOQ_TEST_CHECK(ev.has_done_status);
                MOQ_TEST_CHECK_EQ_U64(ev.done_status, 0x2);
            } else {
                MOQ_TEST_CHECK(false); /* no new handle, removal or catalog-ready */
            }
        }
    }
    MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));
    MOQ_TEST_CHECK(moq_session_state(client) == MOQ_SESS_ESTABLISHED);
}

static void run_scenario(const char *name, bool initially_live,
                         bool catalog_before_done, bool invalid_live_update)
{
    int failures_before = failures;
    fprintf(stderr, "SCENARIO: %s\n", name);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = MOQ_VERSION_DRAFT_16;
    cfg.seed = 42;
    cfg.initial_now_us = 1000;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;

    moq_simpair_t *sp = NULL;
    moq_result_t setup_rc = moq_simpair_create(&cfg, &sp);
    MOQ_TEST_CHECK(setup_rc == MOQ_OK);
    if (setup_rc != MOQ_OK) return;
    setup_rc = moq_simpair_start(sp);
    MOQ_TEST_CHECK(setup_rc == MOQ_OK);
    if (setup_rc != MOQ_OK) {
        moq_simpair_destroy(sp);
        return;
    }
    setup_rc = moq_simpair_run_until_quiescent(sp, 16, NULL);
    MOQ_TEST_CHECK(setup_rc >= 0);
    if (setup_rc < 0) {
        moq_simpair_destroy(sp);
        return;
    }

    moq_session_t *client = moq_simpair_client(sp);
    moq_session_t *server = moq_simpair_server(sp);
    MOQ_TEST_CHECK(moq_session_state(client) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(server) == MOQ_SESS_ESTABLISHED);
    if (moq_session_state(client) != MOQ_SESS_ESTABLISHED ||
        moq_session_state(server) != MOQ_SESS_ESTABLISHED) {
        moq_simpair_destroy(sp);
        return;
    }
    /* Drain handshake events both sides. */
    { moq_event_t ev;
      while (moq_session_poll_events(client, &ev, 1) == 1) moq_event_cleanup(&ev);
      while (moq_session_poll_events(server, &ev, 1) == 1) moq_event_cleanup(&ev); }

    /* Build the real receiver (no endpoint) on the namespace {"svc","demo"}. */
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
    if (!r) {
        moq_simpair_destroy(sp);
        return;
    }

    uint64_t now = moq_simpair_now_us(sp);
    learned_t learned;
    memset(&learned, 0, sizeof(learned));

    uint8_t ctrl[256];

    bool cat_ok_sent = false, cat_data_sent = false;
    bool video_ok_sent = false, video_data_sent = false;
    bool track_added = false, catalog_ready = false;
    moq_media_track_t *video = NULL;

    /* Drive the flow deterministically. Each cycle: pump the real hook (which
     * issues the catalog SUBSCRIBE, then -- after CATALOG_READY -- the video
     * SUBSCRIBE), learn the request ids from the client's actions, then feed the
     * scripted moqx responses at the right step. */
    for (int cycle = 0; cycle < 40; cycle++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);

        /* 1) Once the catalog SUBSCRIBE is observed: SUBSCRIBE_OK(alias 0). */
        if (learned.have_catalog_rid && !cat_ok_sent) {
            size_t n = build_subscribe_ok(ctrl, sizeof(ctrl),
                                          learned.catalog_rid, 0);
            MOQ_TEST_CHECK(n > 0);
            moq_result_t rc =
                moq_session_on_control_bytes(client, ctrl, n, now);
            if (rc < 0) {
                fprintf(stderr,
                    "catalog SUBSCRIBE_OK rejected: rc=%d rid=%llu\n",
                    rc, (unsigned long long)learned.catalog_rid);
            }
            MOQ_TEST_CHECK(rc >= 0);
            cat_ok_sent = true;
        }

        /* 2) After the catalog OK, feed the catalog subgroup (alias 0):
         *    0x30, track_alias(0), group_id(0), objid_delta(0), len, JSON */
        if (cat_ok_sent && !cat_data_sent) {
            feed_catalog(client, 101, 0,
                         initially_live ? CATALOG_JSON : VOD_CATALOG_JSON, now);
            cat_data_sent = true;
        }

        /* 3) After CATALOG_READY + the video SUBSCRIBE is observed:
         *    SUBSCRIBE_OK(alias 2). */
        if (catalog_ready && learned.have_video_rid && !video_ok_sent) {
            size_t n = build_subscribe_ok(ctrl, sizeof(ctrl),
                                          learned.video_rid, 2);
            MOQ_TEST_CHECK(n > 0);
            moq_result_t rc =
                moq_session_on_control_bytes(client, ctrl, n, now);
            if (rc < 0)
                fprintf(stderr,
                    "video SUBSCRIBE_OK rejected: rc=%d rid=%llu\n",
                    rc, (unsigned long long)learned.video_rid);
            MOQ_TEST_CHECK(rc >= 0);
            video_ok_sent = true;
        }

        /* 4) After the video OK, feed the video subgroup (alias 2, group 2):
         *    0x30, alias(2), group(2), then 4 objects, each
         *    { objid_delta(0), len(0x0b), "frame:g2:oN" }. */
        if (video_ok_sent && !video_data_sent) {
            feed_video_group(client, 202, 2, now);
            video_data_sent = true;
        }

        /* Drain track events emitted this cycle. */
        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(r, &te, sizeof(te)) == MOQ_OK) {
            if (te.kind == MOQ_MEDIA_TRACK_ADDED) {
                track_added = true;
                video = te.track;
                if (te.desc)
                    fprintf(stderr, "TRACK_ADDED: %.*s\n",
                            (int)te.desc->name.len,
                            (const char *)te.desc->name.data);
            } else if (te.kind == MOQ_MEDIA_CATALOG_READY) {
                catalog_ready = true;
                fprintf(stderr, "CATALOG_READY\n");
            }
        }

        /* Let the session settle (process buffered control input). */
        (void)moq_session_process_pending(client, now);

        if (moq_media_receiver_is_fatal(r)) {
            fprintf(stderr, "receiver went FATAL: code=0x%llx\n",
                (unsigned long long)moq_media_receiver_fatal_code(r));
            break;
        }
    }

    /* A few more pump cycles so any queued objects route through the hook. */
    for (int cycle = 0; cycle < 8; cycle++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(r, &te, sizeof(te)) == MOQ_OK) {
            if (te.kind == MOQ_MEDIA_TRACK_ADDED) track_added = true;
            else if (te.kind == MOQ_MEDIA_CATALOG_READY) catalog_ready = true;
        }
    }

    /* Drain media objects. Each must be the exact moqx-style frame payload, and
     * -- per the receiver's contract -- carry NO capture timestamp
     * (has_capture_time == false), since the scripted objects have no LOC
     * extension. */
    int objects = 0;
    moq_media_object_t obj;
    while (moq_media_receiver_poll_object(r, &obj, sizeof(obj)) == MOQ_OK) {
        char want[16];
        int wl = snprintf(want, sizeof(want), "frame:g2:o%d", objects);
        fprintf(stderr, "OBJECT %d: %.*s has_capture_time=%d\n", objects,
                (int)obj.payload.len, (const char *)obj.payload.data,
                (int)obj.has_capture_time);
        MOQ_TEST_CHECK(obj.payload.len == (size_t)wl &&
                       memcmp(obj.payload.data, want, (size_t)wl) == 0);
        MOQ_TEST_CHECK(obj.has_capture_time == false);
        MOQ_TEST_CHECK(obj.track == video);
        MOQ_TEST_CHECK_EQ_U64(obj.group_id, 2);
        MOQ_TEST_CHECK_EQ_U64(obj.object_id, (uint64_t)objects);
        objects++;
        moq_media_object_cleanup(&obj);
    }

    moq_media_receiver_stats_t st;
    memset(&st, 0, sizeof(st));
    moq_media_receiver_get_stats(r, &st, sizeof(st));

    fprintf(stderr,
        "RESULT: track_added=%d catalog_ready=%d subscribes=%d fetches=%d "
        "objects_received=%llu objects_dropped=%llu parse_drops=%llu "
        "objects_surfaced=%d catalog_rid=%llu video_rid=%llu\n",
        track_added, catalog_ready, learned.subscribe_seen,
        learned.fetch_seen,
        (unsigned long long)st.objects_received,
        (unsigned long long)st.objects_dropped,
        (unsigned long long)st.parse_drops,
        objects,
        (unsigned long long)learned.catalog_rid,
        (unsigned long long)learned.video_rid);

    bool fatal = moq_media_receiver_is_fatal(r);

    /* Catalog discovery + the video subscribe/data path were actually
     * exercised (guards against a routing regression masquerading as the
     * surfacing assertion below). */
    MOQ_TEST_CHECK(track_added);                 /* video track discovered */
    MOQ_TEST_CHECK(catalog_ready);               /* CATALOG_READY emitted */
    MOQ_TEST_CHECK(learned.subscribe_seen == 2); /* catalog + video SUBSCRIBE */
    MOQ_TEST_CHECK(video_ok_sent);               /* scripted video SUBSCRIBE_OK */
    MOQ_TEST_CHECK(video_data_sent);             /* scripted video subgroup */
    MOQ_TEST_CHECK(!fatal);                      /* session/receiver stayed live */

    /* A LOC object without a capture timestamp is surfaced with
     * has_capture_time=false, NOT dropped. All four moqx-style (type-0x30,
     * no-extension) video objects must surface; none parse-dropped. */
    MOQ_TEST_CHECK_EQ_INT(objects, 4);
    MOQ_TEST_CHECK_EQ_U64(st.parse_drops, 0);
    MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 0);

    /* The first completion is terminal only for a track created non-live. */
    completion_t events = { 0 };
    const moq_media_track_desc_t *desc = moq_media_track_desc_get(video);
    MOQ_TEST_CHECK(desc && desc->is_live == initially_live);
    if (!video || !desc) goto teardown;
    if (invalid_live_update) {
        feed_catalog(client, 303, 1, CATALOG_JSON, now);
        pump_completion(r, client, &learned, video, &events, &now);
        MOQ_TEST_CHECK(!desc->is_live);
        MOQ_TEST_CHECK_EQ_INT(events.updated, 0);
        MOQ_TEST_CHECK(moq_media_receiver_get_stats(r, &st, sizeof(st)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(st.catalog_drops, 1);
    }
    if (initially_live && catalog_before_done) {
        feed_catalog(client, 303, 1, CONVERSION_JSON, now);
        pump_completion(r, client, &learned, video, &events, &now);
        MOQ_TEST_CHECK_EQ_INT(events.updated, 1);
        MOQ_TEST_CHECK(!desc->is_live && desc->has_track_duration);
    }
    MOQ_TEST_CHECK_EQ_INT(events.ended, 0);
    MOQ_TEST_CHECK_EQ_INT(learned.subscribe_seen, 2);

    feed_track_ended(client, learned.video_rid, now);
    pump_completion(r, client, &learned, video, &events, &now);
    MOQ_TEST_CHECK_EQ_INT(events.ended, initially_live ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(learned.subscribe_seen,
                         initially_live && catalog_before_done ? 3 : 2);

    if (initially_live && !catalog_before_done) {
        /* The live subscription completed while the catalog still said live;
         * the handle stays PENDING without a replacement wire request. */
        MOQ_TEST_CHECK(desc->is_live);
        moq_media_track_state_t waiting = MOQ_MEDIA_TRACK_STATE_DISCOVERED;
        MOQ_TEST_CHECK(moq_media_receiver_track_state(r, video, &waiting) == MOQ_OK);
        MOQ_TEST_CHECK(waiting == MOQ_MEDIA_TRACK_STATE_PENDING);
        pump_completion(r, client, &learned, video, &events, &now);
        MOQ_TEST_CHECK_EQ_INT(learned.subscribe_seen, 2);
        feed_catalog(client, 303, 1, CONVERSION_JSON, now);
        pump_completion(r, client, &learned, video, &events, &now);
    }
    MOQ_TEST_CHECK_EQ_INT(learned.subscribe_seen, initially_live ? 3 : 2);
    MOQ_TEST_CHECK_EQ_INT(events.updated, initially_live ? 1 : 0);
    MOQ_TEST_CHECK(moq_media_track_desc_get(video) == desc);
    MOQ_TEST_CHECK(!desc->is_live);
    if (initially_live) {
        MOQ_TEST_CHECK(desc->has_track_duration);
        MOQ_TEST_CHECK_EQ_U64(desc->track_duration_ms, 5000);
    }

    /* Answer even an erroneous replacement and replay the SAME group. This
     * makes the never-live duplicate-delivery assertion causal as well as
     * checking the number of outbound subscriptions. */
    bool replacement = learned.subscribe_seen > 2;
    if (replacement) {
        size_t n = build_subscribe_ok(ctrl, sizeof(ctrl), learned.last_sub_rid, 4);
        MOQ_TEST_CHECK(n > 0);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n, now) >= 0);
        feed_video_group(client, 404, 4, now);
    }
    pump_completion(r, client, &learned, video, &events, &now);
    int replayed = 0;
    while (moq_media_receiver_poll_object(r, &obj, sizeof(obj)) == MOQ_OK) {
        MOQ_TEST_CHECK(obj.track == video);
        MOQ_TEST_CHECK_EQ_U64(obj.group_id, 2);
        MOQ_TEST_CHECK_EQ_U64(obj.object_id, (uint64_t)replayed);
        replayed++;
        moq_media_object_cleanup(&obj);
    }
    MOQ_TEST_CHECK_EQ_INT(replayed, initially_live ? 4 : 0);
    MOQ_TEST_CHECK_EQ_INT(events.ended, initially_live ? 0 : 1);
    moq_media_track_state_t state = MOQ_MEDIA_TRACK_STATE_DISCOVERED;
    MOQ_TEST_CHECK(moq_media_receiver_track_state(r, video, &state) == MOQ_OK);
    MOQ_TEST_CHECK(state == (initially_live ? MOQ_MEDIA_TRACK_STATE_ACTIVE
                                          : MOQ_MEDIA_TRACK_STATE_ENDED));

    if (replacement) {
        feed_track_ended(client, learned.last_sub_rid, now);
        pump_completion(r, client, &learned, video, &events, &now);
    }
    MOQ_TEST_CHECK_EQ_INT(events.ended, 1);
    MOQ_TEST_CHECK(moq_media_receiver_track_state(r, video, &state) == MOQ_OK);
    MOQ_TEST_CHECK(state == MOQ_MEDIA_TRACK_STATE_ENDED);
    MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(r, video, NULL) ==
                   MOQ_ERR_WRONG_STATE);
    pump_completion(r, client, &learned, video, &events, &now);
    MOQ_TEST_CHECK_EQ_INT(events.ended, 1);
    MOQ_TEST_CHECK_EQ_INT(learned.subscribe_seen, initially_live ? 3 : 2);
    moq_result_t rc = moq_media_receiver_poll_object(r, &obj, sizeof(obj));
    /* This offline seam has no endpoint, so an empty queue reports CLOSED. */
    MOQ_TEST_CHECK(rc == MOQ_ERR_CLOSED);
    if (rc == MOQ_OK) moq_media_object_cleanup(&obj);

teardown:
    /* Retire through the production teardown path while the client session is
     * live and the pair is quiescent (so each UNSUBSCRIBE has action capacity),
     * then deliver that teardown traffic before the pair goes away. */
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    now += 1000;
    moq_media_receiver_test_destroy_with_session(r, client, now);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(client, &ev, 1) == 1) moq_event_cleanup(&ev);
      while (moq_session_poll_events(server, &ev, 1) == 1) moq_event_cleanup(&ev); }
    moq_simpair_destroy(sp);

    if (failures == failures_before)
        MOQ_TEST_PASS(name);
}

int main(void)
{
    run_scenario("never_live_track_ended", false, false, false);
    run_scenario("never_live_rejects_live_catalog", false, false, true);
    run_scenario("live_conversion_catalog_before_done", true, true, false);
    run_scenario("live_conversion_catalog_after_done", true, false, false);
    return failures ? 1 : 0;
}
