/*
* A roleless MSF catalog (role OPTIONAL per MSF-01 §5.2.6)
 * with a classifiable audio/video codec (§5.2.18) is received as media.
 *
 * Pairs a REAL moq_media_receiver_t (driven through receiver_hook) with a
 * scripted moqx-behavior peer (no network), like test_media_receiver_scripted,
 * but the catalog declares the video track WITHOUT a `role` -- only
 * packaging:"loc", isLive:true, codec:"avc1.42e01e". The receiver must classify
 * media_type=VIDEO from the codec and surface its objects. Without codec
 * classification media_type stays 0 (role-only), and although the track is
 * still auto-subscribed (auto_subscribe gates on timeline/namespace flags, not
 * media_type), object parse would reject every object as media_type=0 -> parse
 * drops, so no media surfaced.
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
} learned_t;

static void drain_and_learn(moq_session_t *client, learned_t *l)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(client, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                moq_control_envelope_t env;
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, acts[i].u.send_control.data,
                                    acts[i].u.send_control.len);
                if (moq_control_decode_envelope(&r, &env) >= 0 &&
                    env.msg_type == MOQ_D16_SUBSCRIBE) {
                    moq_bytes_t ns_parts[8];
                    moq_kvp_entry_t params[16];
                    moq_d16_subscribe_t s;
                    memset(&s, 0, sizeof(s));
                    s.params = params; s.params_cap = 16;
                    if (moq_d16_decode_subscribe(env.payload, env.payload_len,
                                                 ns_parts, 8, &s) >= 0) {
                        l->subscribe_seen++;
                        if (!l->have_catalog_rid) {
                            l->have_catalog_rid = true;
                            l->catalog_rid = s.request_id;
                        } else if (!l->have_video_rid) {
                            l->have_video_rid = true;
                            l->video_rid = s.request_id;
                        }
                    }
                }
            }
            moq_action_cleanup(&acts[i]);
        }
    }
}

/* Roleless MSF catalog: one live LOC video track classified by codec alone. */
static const char CATALOG_JSON[] =
    "{\"version\":\"1\",\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"codec\":\"avc1.42e01e\"}]}";

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

    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    moq_session_t *client = moq_simpair_client(sp);
    MOQ_TEST_CHECK(moq_session_state(client) == MOQ_SESS_ESTABLISHED);
    { moq_event_t ev;
      while (moq_session_poll_events(client, &ev, 1) == 1) moq_event_cleanup(&ev);
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
    rcfg.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    rcfg.overflow.max_objects = 64;
    rcfg.overflow.max_bytes = 1u << 20;

    moq_media_receiver_t *r = moq_media_receiver_test_new_cfg(&rcfg);
    MOQ_TEST_CHECK(r != NULL);

    uint64_t now = moq_simpair_now_us(sp);
    learned_t learned;
    memset(&learned, 0, sizeof(learned));

    uint8_t ctrl[256];
    moq_stream_ref_t rx_cat   = moq_stream_ref_from_u64(101);
    moq_stream_ref_t rx_video = moq_stream_ref_from_u64(202);

    bool cat_ok_sent = false, cat_data_sent = false;
    bool video_ok_sent = false, video_data_sent = false;
    bool track_added = false, catalog_ready = false;
    int  video_media_type = 0;

    for (int cycle = 0; cycle < 40; cycle++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);

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
            buf[o++] = 0x30;                 /* subgroup header type */
            o += put_varint(buf + o, 0);     /* track_alias = 0 */
            o += put_varint(buf + o, 0);     /* group_id = 0 */
            o += put_varint(buf + o, 0);     /* objid_delta = 0 */
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

        if (video_ok_sent && !video_data_sent) {
            uint8_t buf[512];
            size_t o = 0;
            buf[o++] = 0x30;
            o += put_varint(buf + o, 2);     /* track_alias = 2 */
            o += put_varint(buf + o, 2);     /* group_id = 2 */
            for (int k = 0; k < 4; k++) {
                char frame[16];
                int fl = snprintf(frame, sizeof(frame), "frame:g2:o%d", k);
                o += put_varint(buf + o, 0);
                o += put_varint(buf + o, (uint64_t)fl);
                memcpy(buf + o, frame, (size_t)fl);
                o += (size_t)fl;
            }
            MOQ_TEST_CHECK(moq_session_on_data_bytes(
                client, rx_video, buf, o, /*fin=*/true, now) >= 0);
            video_data_sent = true;
        }

        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(r, &te, sizeof(te)) == MOQ_OK) {
            if (te.kind == MOQ_MEDIA_TRACK_ADDED) {
                track_added = true;
                if (te.desc) video_media_type = (int)te.desc->info.media_type;
            } else if (te.kind == MOQ_MEDIA_CATALOG_READY) {
                catalog_ready = true;
            }
        }

        (void)moq_session_process_pending(client, now);
        if (moq_media_receiver_is_fatal(r)) break;
    }

    for (int cycle = 0; cycle < 8; cycle++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(r, &te, sizeof(te)) == MOQ_OK) {
            if (te.kind == MOQ_MEDIA_TRACK_ADDED) {
                track_added = true;
                if (te.desc) video_media_type = (int)te.desc->info.media_type;
            } else if (te.kind == MOQ_MEDIA_CATALOG_READY) catalog_ready = true;
        }
    }

    int objects = 0;
    moq_media_object_t obj;
    while (moq_media_receiver_poll_object(r, &obj, sizeof(obj)) == MOQ_OK) {
        objects++;
        moq_media_object_cleanup(&obj);
    }

    moq_media_receiver_stats_t st;
    memset(&st, 0, sizeof(st));
    moq_media_receiver_get_stats(r, &st, sizeof(st));

    fprintf(stderr,
        "RESULT: track_added=%d media_type=%d catalog_ready=%d subscribes=%d "
        "video_ok=%d video_data=%d objects=%d parse_drops=%llu\n",
        track_added, video_media_type, catalog_ready, learned.subscribe_seen,
        video_ok_sent, video_data_sent, objects,
        (unsigned long long)st.parse_drops);

    /* The roleless track is classified VIDEO by codec, auto-subscribed, and its
     * objects surface. */
    MOQ_TEST_CHECK(track_added);
    MOQ_TEST_CHECK_EQ_INT(video_media_type, MOQ_MEDIA_TYPE_VIDEO);
    MOQ_TEST_CHECK(catalog_ready);
    MOQ_TEST_CHECK(learned.subscribe_seen == 2);  /* catalog + video SUBSCRIBE */
    MOQ_TEST_CHECK(video_ok_sent);
    MOQ_TEST_CHECK(video_data_sent);
    MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));
    MOQ_TEST_CHECK_EQ_INT(objects, 4);
    MOQ_TEST_CHECK_EQ_U64(st.parse_drops, 0);

    moq_media_receiver_test_free(r);
    moq_simpair_destroy(sp);

    if (failures == 0)
        MOQ_TEST_PASS("media_receiver_roleless");
    return failures ? 1 : 0;
}
