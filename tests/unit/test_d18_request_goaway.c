/*
 * Draft-18 per-request-stream GOAWAY (§10.4, receive): a peer sends a GOAWAY on an
 * established request bidi to migrate that single request. The library surfaces a
 * dedicated MOQ_EVENT_REQUEST_GOAWAY (all 7 request families), frees the request,
 * closes our half (if still open), and puts the ref into a GOAWAY-strict drain so a
 * duplicate GOAWAY / late bytes close the session while FIN/RESET retire it. Data
 * streams are left intact (graceful migration). Covers the codec, per-family
 * surfacing, close-half gating, strict duplicate, validation, and preflight
 * WOULD_BLOCK-before-mutation.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

static const moq_bytes_t k_live[1] = { { (const uint8_t *)"live", 4 } };

static moq_session_t *make_session_caps(moq_perspective_t persp,
                                        uint32_t max_events, uint32_t max_actions)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (max_events) cfg.max_events = max_events;
    if (max_actions) cfg.max_actions = max_actions;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (moq_session_start(s, 0) < 0) { moq_session_destroy(s); return NULL; }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    moq_d18_encode_setup(&w);
    moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0);
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
    moq_action_t a2;
    while (moq_session_poll_actions(s, &a2, 1) > 0) moq_action_cleanup(&a2);
    return s;
}

static moq_session_t *make_session(moq_perspective_t persp)
{
    return make_session_caps(persp, 0, 0);
}

static moq_stream_ref_t take_open_bidi(moq_session_t *s)
{
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
            ref = act.u.open_bidi_stream.stream_ref;
        moq_action_cleanup(&act);
    }
    return ref;
}

static moq_stream_ref_t open_subscribe(moq_session_t *s)
{
    moq_subscribe_cfg_t cfg; moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    moq_subscription_t h; moq_session_subscribe(s, &cfg, 1, &h);
    return take_open_bidi(s);
}
static moq_stream_ref_t open_fetch(moq_session_t *s)
{
    moq_fetch_cfg_t cfg; moq_fetch_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v"); cfg.end_group = 1;
    moq_fetch_t h; moq_session_fetch(s, &cfg, 1, &h);
    return take_open_bidi(s);
}
static moq_stream_ref_t open_track_status(moq_session_t *s)
{
    moq_track_status_cfg_t cfg; moq_track_status_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    moq_track_status_handle_t h; moq_session_track_status(s, &cfg, 1, &h);
    return take_open_bidi(s);
}
static moq_stream_ref_t open_publish_namespace(moq_session_t *s)
{
    moq_publish_namespace_cfg_t cfg; moq_publish_namespace_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
    moq_announcement_t h; moq_session_publish_namespace(s, &cfg, 1, &h);
    return take_open_bidi(s);
}
static moq_stream_ref_t open_subscribe_namespace(moq_session_t *s)
{
    moq_subscribe_namespace_cfg_t cfg; moq_subscribe_namespace_cfg_init(&cfg);
    cfg.track_namespace_prefix = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
    cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t h; moq_session_subscribe_namespace(s, &cfg, 1, &h);
    return take_open_bidi(s);
}
static moq_stream_ref_t open_publish(moq_session_t *s)
{
    moq_publish_cfg_t cfg; moq_publish_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    moq_publication_t h; moq_session_publish(s, &cfg, 1, &h);
    return take_open_bidi(s);
}
static moq_stream_ref_t open_subscribe_tracks(moq_session_t *s)
{
    moq_subscribe_tracks_cfg_t cfg; moq_subscribe_tracks_cfg_init(&cfg);
    cfg.track_namespace_prefix = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
    moq_track_sub_handle_t h; moq_session_subscribe_tracks(s, &cfg, 1, &h);
    return take_open_bidi(s);
}

/* Build a framed request-stream GOAWAY (URI + timeout, no Request ID). */
static size_t encode_req_goaway(uint8_t *buf, size_t cap, const char *uri,
                                uint64_t timeout_ms)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_d18_encode_goaway_request(&w, (const uint8_t *)uri,
                                  uri ? strlen(uri) : 0, timeout_ms);
    return moq_buf_writer_offset(&w);
}

/* Count CLOSE_BIDI_STREAM actions queued for `ref`, draining all actions. */
static int count_close_bidi(moq_session_t *s, moq_stream_ref_t ref)
{
    int n = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_CLOSE_BIDI_STREAM &&
            a.u.close_bidi_stream.stream_ref._v == ref._v)
            n++;
        moq_action_cleanup(&a);
    }
    return n;
}

int main(void)
{
    int failures = 0;

    /* == Codec: request-stream GOAWAY round-trip + strict reject ===== */
    {
        uint8_t buf[64];
        size_t n = encode_req_goaway(buf, sizeof(buf), "https://r2", 9000);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, n);
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_GOAWAY);
        moq_d18_goaway_t ga;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_goaway_request(env.payload, env.payload_len, &ga),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(ga.uri.len, 10);
        MOQ_TEST_CHECK_EQ_U64(ga.timeout_ms, 9000);
        MOQ_TEST_CHECK_EQ_U64(ga.request_id, 0);   /* omitted */
        /* A trailing Request ID (control-stream shape) is rejected here. */
        moq_buf_writer_t w2; uint8_t buf2[64];
        moq_buf_writer_init(&w2, buf2, sizeof(buf2));
        moq_d18_encode_goaway(&w2, NULL, 0, 0, 4);   /* has Request ID */
        moq_buf_reader_t r2;
        moq_buf_reader_init(&r2, buf2, moq_buf_writer_offset(&w2));
        moq_control_envelope_t env2;
        moq_d18_decode_envelope(&r2, &env2);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_goaway_request(env2.payload, env2.payload_len, &ga),
            (int)MOQ_ERR_PROTO);
    }

    /* == Per-family inbound GOAWAY surfaces the migration event ======= */
    struct { moq_stream_ref_t (*open)(moq_session_t *); moq_request_family_t fam;
             bool closes_half; } cases[] = {
        { open_subscribe,           MOQ_REQUEST_FAMILY_SUBSCRIBE,        true  },
        { open_fetch,               MOQ_REQUEST_FAMILY_FETCH,           true  },
        { open_track_status,        MOQ_REQUEST_FAMILY_TRACK_STATUS,    false },
        { open_publish_namespace,   MOQ_REQUEST_FAMILY_ANNOUNCEMENT,    true  },
        { open_subscribe_namespace, MOQ_REQUEST_FAMILY_NS_SUB,          true  },
        { open_publish,             MOQ_REQUEST_FAMILY_PUBLISH,         true  },
        { open_subscribe_tracks,    MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS, true },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_stream_ref_t ref = cases[i].open(s);
        uint8_t msg[64];
        size_t n = encode_req_goaway(msg, sizeof(msg), "https://r2", 9000);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_REQUEST_GOAWAY) {
                got = true;
                MOQ_TEST_CHECK_EQ_INT((int)ev.u.request_goaway.family,
                                      (int)cases[i].fam);
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.request_goaway.new_session_uri.len, 10);
                MOQ_TEST_CHECK_EQ_U64(ev.u.request_goaway.timeout_ms, 9000);
                MOQ_TEST_CHECK(ev.u.request_goaway.handle.raw != 0);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        /* Close-half gating: every family except TRACK_STATUS (opened w/ FIN). */
        MOQ_TEST_CHECK_EQ_INT(count_close_bidi(s, ref),
                              cases[i].closes_half ? 1 : 0);
        /* The request entry is freed (its stream-ref key is gone). */
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_NONE);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == FETCH late data after GOAWAY is absorbed, never session-fatal == *
     *  FETCH data unis correlate by FETCH_HEADER.request_id, so freeing the entry
     *  on GOAWAY would make a *later* data uni (header not yet seen) hit
     *  "FETCH_HEADER unknown request ID" -> session close. The migration tombstone
     *  keeps the by-id key so the late header is absorbed, without STOP_SENDING. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_stream_ref_t ref = open_fetch(s);   /* outbound fetch, request id 0 */
        uint8_t msg[64];
        size_t n = encode_req_goaway(msg, sizeof(msg), NULL, 0);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        moq_event_t ev; while (moq_session_poll_events(s, &ev, 1) > 0)
            moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* The migration must not STOP the (not-yet-started) data uni. */
        moq_action_t a; int stops = 0;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_STOP_DATA) stops++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(stops, 0);
        /* A FETCH data uni for the old request id arrives *after* the GOAWAY. */
        uint8_t data[32];
        moq_buf_writer_t dw;
        moq_buf_writer_init(&dw, data, sizeof(data));
        moq_d18_encode_fetch_header(&dw, 0);   /* request id 0 */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_data_bytes(s, moq_stream_ref_from_u64(0xFE01),
                                           data, moq_buf_writer_offset(&dw),
                                           true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* No spurious FETCH_COMPLETE for the abandoned request. */
        bool spurious = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_COMPLETE) spurious = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!spurious);
        moq_session_destroy(s);
    }

    /* == FETCH GOAWAY mid-stream (data already started) frees safely ==== *
     *  When the data uni already presented its FETCH_HEADER, the cached rx handle
     *  absorbs the remainder, so the entry can be released fully on GOAWAY. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_stream_ref_t ref = open_fetch(s);
        /* Start the data uni (header only) so data_stream_started becomes true. */
        uint8_t data[32];
        moq_buf_writer_t dw;
        moq_buf_writer_init(&dw, data, sizeof(data));
        moq_d18_encode_fetch_header(&dw, 0);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_data_bytes(s, moq_stream_ref_from_u64(0xFE02),
                                           data, moq_buf_writer_offset(&dw),
                                           false, 1),
            (int)MOQ_OK);
        moq_event_t ev0; while (moq_session_poll_events(s, &ev0, 1) > 0)
            moq_event_cleanup(&ev0);
        uint8_t msg[64];
        size_t n = encode_req_goaway(msg, sizeof(msg), NULL, 0);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        moq_event_t ev; while (moq_session_poll_events(s, &ev, 1) > 0)
            moq_event_cleanup(&ev);
        /* Remaining objects + FIN on the already-started uni: absorbed, no close. */
        moq_buf_writer_t dw2; uint8_t data2[16]; uint8_t prio = 0;
        moq_buf_writer_init(&dw2, data2, sizeof(data2));
        moq_buf_write_vi64(&dw2, 0x1F);   /* group+object+priority, subgroup pres */
        moq_buf_write_vi64(&dw2, 0);
        moq_buf_write_vi64(&dw2, 0);
        moq_buf_write_vi64(&dw2, 0);
        moq_buf_write_raw(&dw2, &prio, 1);
        moq_buf_write_vi64(&dw2, 0);       /* payload len */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_data_bytes(s, moq_stream_ref_from_u64(0xFE02),
                                           data2, moq_buf_writer_offset(&dw2),
                                           true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Strict duplicate: second GOAWAY (bytes) closes 0x3 ========== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_stream_ref_t ref = open_subscribe(s);
        uint8_t msg[64];
        size_t n = encode_req_goaway(msg, sizeof(msg), NULL, 0);
        moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
        moq_event_t ev; while (moq_session_poll_events(s, &ev, 1) > 0)
            moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* A duplicate GOAWAY (non-empty bytes) on the strict-drained ref closes. */
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Strict drain: a clean FIN after GOAWAY retires (no close) ==== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_stream_ref_t ref = open_subscribe(s);
        uint8_t msg[64];
        size_t n = encode_req_goaway(msg, sizeof(msg), NULL, 0);
        moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
        moq_event_t ev; while (moq_session_poll_events(s, &ev, 1) > 0)
            moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Same-chunk trailing bytes after GOAWAY closes 0x3 =========== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_stream_ref_t ref = open_subscribe(s);
        uint8_t msg[64];
        size_t n = encode_req_goaway(msg, sizeof(msg), NULL, 0);
        msg[n] = 0xAB;   /* trailing garbage in the same chunk */
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg, n + 1, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == First-message GOAWAY on a fresh staging stream closes 0x3 ==== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        uint8_t msg[64];
        size_t n = encode_req_goaway(msg, sizeof(msg), NULL, 0);
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Server receiving a non-zero New Session URI closes 0x3 ====== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = open_subscribe(s);   /* a server may subscribe */
        uint8_t msg[64];
        size_t n = encode_req_goaway(msg, sizeof(msg), "https://nope", 0);
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Preflight WOULD_BLOCK before mutation, then retry succeeds === *
     *  With a 1-slot event queue already holding one request's GOAWAY event, a
     *  second request's GOAWAY can't reserve its event slot: it returns
     *  WOULD_BLOCK with nothing mutated (entry intact, bytes retained), then
     *  succeeds once the queue drains. */
    {
        moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_CLIENT, 1, 0);
        /* Two distinct tracks (same track name would be a duplicate-subscribe). */
        moq_stream_ref_t ref1, ref2;
        {
            moq_subscribe_cfg_t c1; moq_subscribe_cfg_init(&c1);
            c1.track_namespace = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
            c1.track_name = MOQ_BYTES_LITERAL("v1");
            moq_subscription_t h1; moq_session_subscribe(s, &c1, 1, &h1);
            ref1 = take_open_bidi(s);
            moq_subscribe_cfg_t c2; moq_subscribe_cfg_init(&c2);
            c2.track_namespace = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
            c2.track_name = MOQ_BYTES_LITERAL("v2");
            moq_subscription_t h2; moq_session_subscribe(s, &c2, 1, &h2);
            ref2 = take_open_bidi(s);
        }
        MOQ_TEST_CHECK(ref1._v != 0 && ref2._v != 0 && ref1._v != ref2._v);
        uint8_t msg[64];
        size_t n = encode_req_goaway(msg, sizeof(msg), NULL, 0);
        /* First GOAWAY fills the 1-slot event queue (left undrained). */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref1, msg, n, false, 1),
            (int)MOQ_OK);
        /* Second GOAWAY: event queue full -> WOULD_BLOCK, nothing mutated. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref2, msg, n, false, 1),
            (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref2).kind ==
                       MOQ_REQ_SUBSCRIPTION);   /* entry intact */
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* Drain events, then re-feed empty -> the buffered GOAWAY retries + frees. */
        moq_event_t ev; int goaways = 0;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_REQUEST_GOAWAY) goaways++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(goaways, 1);   /* only ref1 so far */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref2, NULL, 0, false, 1),
            (int)MOQ_OK);
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_REQUEST_GOAWAY) goaways++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(goaways, 2);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref2).kind ==
                       MOQ_REQ_NONE);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    MOQ_TEST_PASS("d18_request_goaway");
    return failures != 0;
}
