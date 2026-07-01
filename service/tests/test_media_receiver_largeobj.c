/*
 * Large-object media_receiver intake guard.
 *
 * Invariant: the receive path (session reassembly -> subscriber facade ->
 * media_receiver intake -> poll_object) surfaces a large media object at its
 * full byte length, byte-exact, including an extension-enabled LOC object.
 *
 * Pairs a REAL moq_media_receiver_t with a scripted peer (no network, no
 * transport adapter) and feeds ONE large LOC video object on the video
 * subgroup to the session in realistic ~1400-byte wire chunks, then asserts
 * poll_object surfaces it at full length with a position-dependent pattern.
 * Feeding bytes directly into the session keeps this a pure intake test,
 * independent of any transport-level flow control.
 *
 * Two object shapes are covered:
 *   - no-extension subgroup (type-0x30), surfaced with
 *     has_capture_time == false; and
 *   - extension-enabled LOC (type-0x31) carrying a Capture Timestamp and a
 *     keyframe Video Frame Marking, surfaced with has_capture_time == true
 *     and keyframe == true.
 *
 * Shares the scripted-peer scaffolding of test_media_receiver_scripted.c
 * (catalog SUBSCRIBE/OK, catalog object, auto-subscribe of the video track,
 * video SUBSCRIBE/OK).
 */
#include <moq/media_receiver.h>
#include <moq/msf.h>
#include <moq/loc.h>
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
#include <stdlib.h>

static int failures = 0;

/* -- Test seam (media_receiver.c, MOQ_MEDIA_RECEIVER_TESTING) ----------- */
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
    int      fetch_seen;
} learned_t;

static void drain_and_learn(moq_session_t *client, learned_t *l)
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
            } else if (env.msg_type == MOQ_D16_FETCH) {
                l->fetch_seen++;
            }
        }
    }
}

static const char CATALOG_JSON[] =
    "{\"version\":1,\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"avc1.42e01e\"}]}";

static uint8_t pat_byte(size_t i) { return (uint8_t)((i * 131u + 7u) & 0xffu); }

/*
 * Build the LOC extension (object property) bytes for the extension-enabled
 * variant: a Capture Timestamp plus a keyframe Video Frame Marking. Returns
 * the encoded length, or 0 on failure; the bytes are copied into `out`
 * (caller-owned).
 */
static size_t build_loc_ext(uint8_t *out, size_t out_cap)
{
    moq_loc_headers_t h;
    moq_loc_headers_init(&h);
    h.has_timestamp = true;
    h.timestamp = 1700000000000000ull;  /* arbitrary capture time (us) */
    h.has_video_frame_marking = true;
    h.video_frame_marking.start_of_frame = true;
    h.video_frame_marking.end_of_frame = true;
    h.video_frame_marking.independent = true;   /* -> keyframe */

    moq_rcbuf_t *ext = NULL;
    if (moq_loc_encode(moq_alloc_default(), MOQ_LOC_PROFILE_01, &h, &ext) < 0
        || !ext)
        return 0;
    size_t n = moq_rcbuf_len(ext);
    if (n == 0 || n > out_cap) { moq_rcbuf_decref(ext); return 0; }
    memcpy(out, moq_rcbuf_data(ext), n);
    moq_rcbuf_decref(ext);
    return n;
}

/*
 * Drive one object through the real media_receiver. `loc_ext` selects the
 * extension-enabled LOC shape (type-0x31 with Capture Timestamp + Video Frame
 * Marking) vs the plain no-extension shape (type-0x30). Returns the number of
 * objects surfaced; *out_len/_has_ts/_keyframe receive the first object's
 * length and LOC-derived metadata.
 */
static int run_one_size(size_t payload_size, bool loc_ext, size_t *out_len,
                        bool *out_has_ts, bool *out_keyframe)
{
    *out_len = 0; *out_has_ts = false; *out_keyframe = false;
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
      moq_session_t *server = moq_simpair_server(sp);
      while (moq_session_poll_events(server, &ev, 1) == 1) moq_event_cleanup(&ev); }

    static const moq_bytes_t ns_parts[] = {
        { (const uint8_t *)"media", 5 }, { (const uint8_t *)"cat", 3 },
    };
    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_live(&rcfg);
    rcfg.namespace_.parts = ns_parts;
    rcfg.namespace_.count = 2;
    rcfg.auto_subscribe = true;
    rcfg.time_mode = MOQ_MEDIA_TIME_RAW;
    rcfg.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    rcfg.overflow.max_objects = 64;
    rcfg.overflow.max_bytes = 8u << 20;   /* 8 MiB: comfortably > 256 KiB */

    moq_media_receiver_t *r = moq_media_receiver_test_new_cfg(&rcfg);
    MOQ_TEST_CHECK(r != NULL);

    uint8_t *big = (uint8_t *)malloc(payload_size);
    MOQ_TEST_CHECK(big != NULL);
    for (size_t i = 0; i < payload_size; i++) big[i] = pat_byte(i);

    uint8_t ext[64];
    size_t ext_len = 0;
    if (loc_ext) {
        ext_len = build_loc_ext(ext, sizeof(ext));
        MOQ_TEST_CHECK(ext_len > 0);
    }

    learned_t learned;
    memset(&learned, 0, sizeof(learned));
    uint8_t ctrl[256];
    moq_stream_ref_t rx_cat   = moq_stream_ref_from_u64(101);
    moq_stream_ref_t rx_video = moq_stream_ref_from_u64(202);
    bool cat_ok_sent = false, cat_data_sent = false;
    bool video_ok_sent = false, video_data_sent = false;
    bool catalog_ready = false;
    uint64_t now = 1000;

    for (int cycle = 0; cycle < 64; cycle++) {
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
            buf[o++] = 0x30;
            o += put_varint(buf + o, 0);
            o += put_varint(buf + o, 0);
            o += put_varint(buf + o, 0);
            size_t jlen = sizeof(CATALOG_JSON) - 1;
            o += put_varint(buf + o, jlen);
            memcpy(buf + o, CATALOG_JSON, jlen);
            o += jlen;
            MOQ_TEST_CHECK(moq_session_on_data_bytes(
                client, rx_cat, buf, o, true, now) >= 0);
            cat_data_sent = true;
        }

        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(r, &te, sizeof(te)) == MOQ_OK) {
            if (te.kind == MOQ_MEDIA_CATALOG_READY) catalog_ready = true;
        }

        if (catalog_ready && learned.have_video_rid && !video_ok_sent) {
            size_t n = build_subscribe_ok(ctrl, sizeof(ctrl),
                                          learned.video_rid, 2);
            MOQ_TEST_CHECK(n > 0);
            MOQ_TEST_CHECK(moq_session_on_control_bytes(client, ctrl, n, now) >= 0);
            video_ok_sent = true;
        }

        /* After the video OK: one large object on the video subgroup
         * (alias 2, group 2), fed in ~1400-byte wire chunks with FIN on the
         * last — mirroring real QUIC stream delivery. The subgroup header
         * type is 0x31 (extensions present) for the LOC-extension variant,
         * else 0x30; the object framing is
         *   objid_delta [, ext_len, ext_bytes] , payload_len, payload. */
        if (video_ok_sent && !video_data_sent) {
            size_t hdr = 0;
            uint8_t header[64];
            header[hdr++] = loc_ext ? 0x31 : 0x30;   /* 0x31 = 0x30|EXTENSIONS */
            hdr += put_varint(header + hdr, 2);      /* track_alias = 2 */
            hdr += put_varint(header + hdr, 2);      /* group_id = 2 */
            hdr += put_varint(header + hdr, 0);      /* objid_delta = 0 */
            if (loc_ext) {
                hdr += put_varint(header + hdr, ext_len);
                memcpy(header + hdr, ext, ext_len);
                hdr += ext_len;
            }
            hdr += put_varint(header + hdr, payload_size); /* payload_len */

            size_t total = hdr + payload_size;
            uint8_t *stream = (uint8_t *)malloc(total);
            MOQ_TEST_CHECK(stream != NULL);
            memcpy(stream, header, hdr);
            memcpy(stream + hdr, big, payload_size);

            size_t off = 0;
            while (off < total) {
                size_t chunk = total - off < 1400 ? total - off : 1400;
                bool fin = (off + chunk >= total);
                MOQ_TEST_CHECK(moq_session_on_data_bytes(
                    client, rx_video, stream + off, chunk, fin, now) >= 0);
                off += chunk;
            }
            free(stream);
            video_data_sent = true;
        }

        (void)moq_session_process_pending(client, now);
        if (moq_media_receiver_is_fatal(r)) break;
    }

    for (int cycle = 0; cycle < 16; cycle++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(r, &te, sizeof(te)) == MOQ_OK)
            ;
    }

    int objects = 0;
    moq_media_object_t obj;
    while (moq_media_receiver_poll_object(r, &obj, sizeof(obj)) == MOQ_OK) {
        if (objects == 0) {
            *out_len = obj.payload.len;
            *out_has_ts = obj.has_capture_time;
            *out_keyframe = obj.keyframe;
            bool pat_ok = (obj.payload.len == payload_size);
            for (size_t i = 0; pat_ok && i < obj.payload.len; i++)
                if (obj.payload.data[i] != pat_byte(i)) pat_ok = 0;
            MOQ_TEST_CHECK(pat_ok);
        }
        objects++;
        moq_media_object_cleanup(&obj);
    }

    moq_media_receiver_stats_t st;
    memset(&st, 0, sizeof(st));
    moq_media_receiver_get_stats(r, &st, sizeof(st));
    fprintf(stderr,
        "SIZE %zu loc_ext=%d: surfaced=%d first_len=%zu has_ts=%d keyframe=%d "
        "received=%llu dropped=%llu parse_drops=%llu overflow=%llu\n",
        payload_size, loc_ext, objects, *out_len, *out_has_ts, *out_keyframe,
        (unsigned long long)st.objects_received,
        (unsigned long long)st.objects_dropped,
        (unsigned long long)st.parse_drops,
        (unsigned long long)st.overflow_events);

    free(big);
    moq_media_receiver_test_free(r);
    moq_simpair_destroy(sp);
    return objects;
}

int main(void)
{
    size_t len = 0; bool has_ts = false, keyframe = false;

    /* No-extension (moqx type-0x30) shape across the size range. */
    MOQ_TEST_CHECK_EQ_INT(run_one_size(64u * 1024, false, &len, &has_ts, &keyframe), 1);
    MOQ_TEST_CHECK(len == 64u * 1024 && has_ts == false);
    MOQ_TEST_CHECK_EQ_INT(run_one_size(128u * 1024, false, &len, &has_ts, &keyframe), 1);
    MOQ_TEST_CHECK(len == 128u * 1024 && has_ts == false);
    MOQ_TEST_CHECK_EQ_INT(run_one_size(256u * 1024, false, &len, &has_ts, &keyframe), 1);
    MOQ_TEST_CHECK(len == 256u * 1024 && has_ts == false);

    /* Extension-enabled LOC (type-0x31): a large object carrying a Capture
     * Timestamp + keyframe Video Frame Marking must surface byte-exact with
     * the LOC metadata decoded. */
    MOQ_TEST_CHECK_EQ_INT(run_one_size(128u * 1024, true, &len, &has_ts, &keyframe), 1);
    MOQ_TEST_CHECK(len == 128u * 1024);
    MOQ_TEST_CHECK(has_ts == true);     /* capture timestamp decoded */
    MOQ_TEST_CHECK(keyframe == true);   /* independent frame marking */

    if (failures == 0)
        printf("test_media_receiver_largeobj: PASS\n");
    else
        fprintf(stderr, "test_media_receiver_largeobj: %d failure(s)\n", failures);
    return failures ? 1 : 0;
}
