/*
* Regression: receiver-side FETCH-ingest of a catalog, at the
 * media_receiver facade level.
 *
 * Pairs a REAL moq_media_receiver_t (driven through receiver_hook) with a
 * SCRIPTED moqx-behavior peer -- no network, no moqx. Unlike
 * test_media_receiver_scripted (which delivers the catalog as a live SUBGROUP),
 * this delivers the catalog ONLY via a FETCH object stream, exactly the captured
 * moqx shape for a catalog-driven subscriber behind a relay:
 *
 *   SERVER_SETUP (via simpair handshake)
 *   SUBSCRIBE_OK(request_id=<catalog>, track_alias=0, Largest=[0,0])
 *   -- receiver then issues a Joining FETCH(offset 0) for the catalog --
 *   FETCH stream: FETCH_HEADER(<fetch>) + fetch object { group 0, object 0,
 *                 priority 0, payload = catalog JSON }, then FIN
 *   FETCH_OK(request_id=<fetch>, End Location=[0,1])
 *
 * Note the captured ORDER: the FETCH object stream arrives BEFORE the FETCH_OK
 * control. Draft-16 (FETCH §9.16 / FETCH_OK §9.17) permits FETCH_OK at any time
 * relative to object delivery, and a relay (moqx) FINs the object stream ahead
 * of its FETCH_OK. The receiver must still ingest the FETCH-delivered catalog
 * and reach TRACK_ADDED + CATALOG_READY (MSF-01 §5 retrieves the catalog with
 * SUBSCRIBE + Joining FETCH(offset 0), so a FETCH-delivered catalog is the
 * normal path). A FETCH object that arrives while the request is still PENDING
 * (before FETCH_OK marks it ACTIVE) must be ingested, so the catalog reaches
 * CATALOG_READY.
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
void moq_media_receiver_test_free(moq_media_receiver_t *r);
void moq_media_receiver_test_pump(moq_media_receiver_t *r,
                                  moq_session_t *session, uint64_t now_us);

/* -- Scripted-peer byte builders ---------------------------------------- */

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

/* SUBSCRIBE_OK carrying a LARGEST_OBJECT (0x09) param = {group, object}, so the
 * receiver's catalog subscription has a stored Largest and can issue a valid
 * Joining FETCH(offset 0). The captured wire form is `09 02 <group> <object>`
 * (odd param type -> length-prefixed value). */
static size_t build_subscribe_ok_largest(uint8_t *out, size_t out_cap,
                                         uint64_t request_id,
                                         uint64_t track_alias,
                                         uint64_t lg_group, uint64_t lg_object)
{
    uint8_t val[16];
    size_t vlen = 0;
    vlen += put_varint(val + vlen, lg_group);
    vlen += put_varint(val + vlen, lg_object);

    moq_kvp_entry_t largest;
    memset(&largest, 0, sizeof(largest));
    largest.type = MOQ_MSG_PARAM_LARGEST_OBJECT;   /* 0x09, length-prefixed */
    largest.value = val;
    largest.value_len = vlen;
    largest.is_varint = false;

    moq_buf_writer_t w;
    moq_buf_writer_init(&w, out, out_cap);
    if (moq_d16_encode_subscribe_ok(&w, request_id, track_alias,
                                    &largest, 1, NULL, 0) < 0)
        return 0;
    return moq_buf_writer_offset(&w);
}

/* Framed FETCH_OK(request_id, End Location = end_group/end_object). */
static size_t build_fetch_ok(uint8_t *out, size_t out_cap, uint64_t request_id,
                             uint64_t end_group, uint64_t end_object)
{
    moq_d16_fetch_ok_t ok;
    memset(&ok, 0, sizeof(ok));
    ok.request_id = request_id;
    ok.end_of_track = 0;
    ok.end_group = end_group;
    ok.end_object = end_object;
    ok.params = NULL;
    ok.params_count = 0;

    moq_buf_writer_t w;
    moq_buf_writer_init(&w, out, out_cap);
    if (moq_d16_encode_fetch_ok(&w, &ok) < 0)
        return 0;
    return moq_buf_writer_offset(&w);
}

/* FETCH object stream: FETCH_HEADER(request_id) + one fetch object carrying the
 * catalog at (group, object). Mirrors the captured moqx framing
 * `05 <rid> 1c 00 00 00 <len> <json>` (flags 0x1c = group+object+priority). */
static size_t build_fetch_stream(uint8_t *out, size_t out_cap,
                                 uint64_t request_id,
                                 uint64_t group, uint64_t object,
                                 const uint8_t *payload, size_t payload_len)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, out, out_cap);
    if (moq_d16_encode_fetch_header(&w, request_id) < 0)
        return 0;

    moq_d16_fetch_object_t obj;
    memset(&obj, 0, sizeof(obj));
    obj.is_end_of_range = false;
    obj.group_id = group;
    obj.subgroup_id = 0;
    obj.object_id = object;
    obj.publisher_priority = 0;
    obj.extensions = NULL;
    obj.extensions_len = 0;
    obj.payload_len = payload_len;
    obj.payload = payload;
    if (moq_d16_encode_fetch_object(&w, &obj, NULL) < 0)
        return 0;
    return moq_buf_writer_offset(&w);
}

/* -- request-id learning: decode the receiver's outbound control actions -- */

typedef struct {
    bool     have_catalog_rid;
    uint64_t catalog_rid;
    bool     have_fetch_rid;
    uint64_t fetch_rid;
    int      subscribe_seen;
    int      fetch_seen;
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
                if (moq_control_decode_envelope(&r, &env) >= 0) {
                    if (env.msg_type == MOQ_D16_SUBSCRIBE) {
                        moq_bytes_t ns_parts[8];
                        moq_kvp_entry_t params[16];
                        moq_d16_subscribe_t s;
                        memset(&s, 0, sizeof(s));
                        s.params = params;
                        s.params_cap = 16;
                        if (moq_d16_decode_subscribe(env.payload,
                                env.payload_len, ns_parts, 8, &s) >= 0) {
                            l->subscribe_seen++;
                            if (!l->have_catalog_rid) {
                                l->have_catalog_rid = true;
                                l->catalog_rid = s.request_id;
                            }
                        }
                    } else if (env.msg_type == MOQ_D16_FETCH) {
                        moq_bytes_t ns_parts[8];
                        moq_kvp_entry_t params[16];
                        moq_d16_fetch_t f;
                        memset(&f, 0, sizeof(f));
                        f.params = params;
                        f.params_cap = 16;
                        if (moq_d16_decode_fetch(env.payload,
                                env.payload_len, ns_parts, 8, &f) >= 0) {
                            l->fetch_seen++;
                            if (!l->have_fetch_rid) {
                                l->have_fetch_rid = true;
                                l->fetch_rid = f.request_id;
                            }
                        }
                    }
                }
            }
            /* API contract: every polled action must be cleaned up, regardless
             * of kind or whether we decoded it. */
            moq_action_cleanup(&acts[i]);
        }
    }
}

/* Minimal MSF catalog declaring one live LOC video track. */
static const char CATALOG_JSON[] =
    "{\"version\":\"1\",\"tracks\":["
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

    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 16, NULL);

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
    moq_stream_ref_t rx_fetch = moq_stream_ref_from_u64(303);

    bool cat_ok_sent = false, fetch_ok_sent = false, fetch_data_sent = false;
    bool track_added = false, catalog_ready = false;

    for (int cycle = 0; cycle < 60; cycle++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);

        /* 1) Catalog SUBSCRIBE observed -> SUBSCRIBE_OK(alias 0, Largest=[0,0]),
         *    so the receiver can issue a Joining FETCH(offset 0). */
        if (learned.have_catalog_rid && !cat_ok_sent) {
            size_t n = build_subscribe_ok_largest(ctrl, sizeof(ctrl),
                                                  learned.catalog_rid, 0, 0, 0);
            MOQ_TEST_CHECK(n > 0);
            moq_result_t rc = moq_session_on_control_bytes(client, ctrl, n, now);
            if (rc < 0)
                fprintf(stderr, "catalog SUBSCRIBE_OK rejected: rc=%d\n", rc);
            MOQ_TEST_CHECK(rc >= 0);
            cat_ok_sent = true;
        }

        /* 2) Deliver the catalog on the FETCH object stream (group 0, object 0)
         *    and FIN it -- BEFORE FETCH_OK, exactly the captured moqx order (the
         *    object stream arrives ahead of the FETCH_OK control). */
        if (learned.have_fetch_rid && !fetch_data_sent) {
            uint8_t buf[512];
            size_t jlen = sizeof(CATALOG_JSON) - 1;
            size_t o = build_fetch_stream(buf, sizeof(buf), learned.fetch_rid,
                                          0, 0, (const uint8_t *)CATALOG_JSON,
                                          jlen);
            MOQ_TEST_CHECK(o > 0);
            moq_result_t rc = moq_session_on_data_bytes(
                client, rx_fetch, buf, o, /*fin=*/true, now);
            if (rc < 0)
                fprintf(stderr, "FETCH data rejected: rc=%d\n", rc);
            MOQ_TEST_CHECK(rc >= 0);
            fetch_data_sent = true;
        }

        /* 3) FETCH_OK(End=[0,1]) AFTER the object stream. */
        if (fetch_data_sent && !fetch_ok_sent) {
            size_t n = build_fetch_ok(ctrl, sizeof(ctrl),
                                      learned.fetch_rid, 0, 1);
            MOQ_TEST_CHECK(n > 0);
            moq_result_t rc = moq_session_on_control_bytes(client, ctrl, n, now);
            if (rc < 0)
                fprintf(stderr, "FETCH_OK rejected: rc=%d\n", rc);
            MOQ_TEST_CHECK(rc >= 0);
            fetch_ok_sent = true;
        }

        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(r, &te, sizeof(te)) == MOQ_OK) {
            if (te.kind == MOQ_MEDIA_TRACK_ADDED) track_added = true;
            else if (te.kind == MOQ_MEDIA_CATALOG_READY) catalog_ready = true;
        }

        (void)moq_session_process_pending(client, now);

        if (moq_media_receiver_is_fatal(r)) {
            fprintf(stderr, "receiver FATAL: code=0x%llx\n",
                (unsigned long long)moq_media_receiver_fatal_code(r));
            break;
        }
    }

    moq_media_receiver_stats_t st;
    memset(&st, 0, sizeof(st));
    moq_media_receiver_get_stats(r, &st, sizeof(st));

    fprintf(stderr,
        "RESULT: track_added=%d catalog_ready=%d subscribes=%d fetches=%d "
        "cat_ok=%d fetch_ok=%d fetch_data=%d fetch_rid=%llu\n",
        track_added, catalog_ready, learned.subscribe_seen, learned.fetch_seen,
        cat_ok_sent, fetch_ok_sent, fetch_data_sent,
        (unsigned long long)learned.fetch_rid);

    bool fatal = moq_media_receiver_is_fatal(r);

    /* The flow was actually exercised. */
    MOQ_TEST_CHECK(cat_ok_sent);          /* catalog SUBSCRIBE_OK injected */
    MOQ_TEST_CHECK(learned.fetch_seen >= 1); /* receiver issued a Joining FETCH */
    MOQ_TEST_CHECK(fetch_ok_sent);        /* FETCH_OK injected */
    MOQ_TEST_CHECK(fetch_data_sent);      /* catalog delivered on FETCH stream */
    MOQ_TEST_CHECK(!fatal);

    /* The required behavior: a FETCH-delivered catalog is ingested -> the video
     * track is discovered and CATALOG_READY fires, including a catalog object
     * that arrives before FETCH_OK. */
    MOQ_TEST_CHECK(track_added);
    MOQ_TEST_CHECK(catalog_ready);

    moq_media_receiver_test_free(r);
    moq_simpair_destroy(sp);

    if (failures == 0)
        MOQ_TEST_PASS("media_receiver_fetch_catalog");
    return failures ? 1 : 0;
}
