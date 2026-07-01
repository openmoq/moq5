/*
 * Draft-18 Track-Property UNSUPPORTED_EXTENSION (0x33) control flow (§2.5.1).
 * An unknown Mandatory Track Property (type 0x4000-0x7FFF) in a control response
 * is NOT a session-fatal PROTOCOL_VIOLATION: the receiver issues a request-level
 * UNSUPPORTED_EXTENSION instead, routed on the request bidi.
 *   - SUBSCRIBE_OK -> surface SUBSCRIBE_ERROR(0x33) + cancel (STOP+RESET).
 *   - FETCH_OK     -> surface FETCH_ERROR(0x33) + cancel (STOP+RESET).
 *   - PUBLISH      -> auto REQUEST_ERROR(0x33) on the bidi, no PUBLISH_REQUEST.
 * The cancel uses the internal request-bidi sequence (not the public unsubscribe /
 * fetch-cancel entrypoints). The session stays ESTABLISHED throughout.
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

/* vi64 even-type KVP carrying a Mandatory Track Property (0x4000). */
static size_t mandatory_kvp(uint8_t *buf, size_t cap)
{
    moq_buf_writer_t w; moq_buf_writer_init(&w, buf, cap);
    moq_buf_write_vi64(&w, 0x4000);
    moq_buf_write_vi64(&w, 0);
    return moq_buf_writer_offset(&w);
}

static size_t framed(uint8_t *buf, size_t cap, uint64_t type,
                     const uint8_t *payload, size_t plen)
{
    moq_buf_writer_t w; moq_buf_writer_init(&w, buf, cap);
    moq_buf_write_vi64(&w, type);
    moq_buf_write_uint16(&w, (uint16_t)plen);
    moq_buf_write_raw(&w, payload, plen);
    return moq_buf_writer_offset(&w);
}

/* Count STOP_BIDI_STREAM + RESET_BIDI_STREAM for `ref`, draining all actions. */
static void count_cancel_actions(moq_session_t *s, moq_stream_ref_t ref,
                                 int *stops, int *resets)
{
    *stops = 0; *resets = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_STOP_BIDI_STREAM &&
            a.u.stop_bidi_stream.stream_ref._v == ref._v) (*stops)++;
        if (a.kind == MOQ_ACTION_RESET_BIDI_STREAM &&
            a.u.reset_bidi_stream.stream_ref._v == ref._v) (*resets)++;
        moq_action_cleanup(&a);
    }
}

int main(void)
{
    int failures = 0;

    /* == SUBSCRIBE_OK with unknown mandatory track property -> 0x33 ==== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_stream_ref_t ref = open_subscribe(s);
        uint8_t props[8];
        size_t plen = mandatory_kvp(props, sizeof(props));
        uint8_t pl[64];
        moq_buf_writer_t pw; moq_buf_writer_init(&pw, pl, sizeof(pl));
        moq_buf_write_vi64(&pw, 9);   /* track_alias */
        moq_buf_write_vi64(&pw, 0);   /* param count */
        moq_buf_write_raw(&pw, props, plen);
        uint8_t msg[80];
        size_t n = framed(msg, sizeof(msg), MOQ_D18_SUBSCRIBE_OK, pl,
                          moq_buf_writer_offset(&pw));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR) {
                got = true;
                MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_error.error_code,
                                      MOQ_REQUEST_ERROR_UNSUPPORTED_EXTENSION);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        int stops, resets;
        count_cancel_actions(s, ref, &stops, &resets);
        MOQ_TEST_CHECK_EQ_INT(stops, 1);
        MOQ_TEST_CHECK_EQ_INT(resets, 1);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_NONE);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == FETCH_OK with unknown mandatory track property -> 0x33 ======== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_stream_ref_t ref = open_fetch(s);
        uint8_t props[8];
        size_t plen = mandatory_kvp(props, sizeof(props));
        uint8_t pl[64];
        moq_buf_writer_t pw; moq_buf_writer_init(&pw, pl, sizeof(pl));
        uint8_t eot = 0;
        moq_buf_write_raw(&pw, &eot, 1);   /* end_of_track = 0 */
        moq_buf_write_vi64(&pw, 1);        /* end location: group */
        moq_buf_write_vi64(&pw, 0);        /* end location: object */
        moq_buf_write_vi64(&pw, 0);        /* param count */
        moq_buf_write_raw(&pw, props, plen);
        uint8_t msg[80];
        size_t n = framed(msg, sizeof(msg), MOQ_D18_FETCH_OK, pl,
                          moq_buf_writer_offset(&pw));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_ERROR) {
                got = true;
                MOQ_TEST_CHECK_EQ_U64(ev.u.fetch_error.error_code,
                                      MOQ_REQUEST_ERROR_UNSUPPORTED_EXTENSION);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        int stops, resets;
        count_cancel_actions(s, ref, &stops, &resets);
        MOQ_TEST_CHECK_EQ_INT(stops, 1);
        MOQ_TEST_CHECK_EQ_INT(resets, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Inbound PUBLISH with unknown mandatory -> auto REQUEST_ERROR === */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        /* Encode a PUBLISH with empty Track Properties, then append a mandatory
         * property to the tail (the encoder rejects mandatory directly). */
        uint8_t base[160];
        moq_buf_writer_t bw; moq_buf_writer_init(&bw, base, sizeof(base));
        moq_d18_publish_t p = {0};
        p.request_id = 0;
        p.track_namespace = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
        p.track_name = MOQ_BYTES_LITERAL("v");
        p.track_alias = 7;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_publish(&bw, &p), (int)MOQ_OK);
        /* PUBLISH type 0x1D is a 1-byte vi64; payload starts after type+u16 len. */
        size_t hdr = 1 + 2;
        const uint8_t *base_pl = base + hdr;
        size_t base_pl_len = moq_buf_writer_offset(&bw) - hdr;
        uint8_t props[8];
        size_t plen = mandatory_kvp(props, sizeof(props));
        uint8_t pl[200];
        memcpy(pl, base_pl, base_pl_len);
        memcpy(pl + base_pl_len, props, plen);
        uint8_t msg[208];
        size_t n = framed(msg, sizeof(msg), MOQ_D18_PUBLISH, pl,
                          base_pl_len + plen);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        /* No PUBLISH_REQUEST surfaces; a terminal REQUEST_ERROR(0x33) is queued. */
        bool publish_req = false, err_ok = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) publish_req = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!publish_req);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a.u.send_bidi_stream.stream_ref._v == ref._v &&
                a.u.send_bidi_stream.fin) {
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, a.u.send_bidi_stream.data,
                                    a.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                if (moq_d18_decode_envelope(&r, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D18_REQUEST_ERROR) {
                    moq_d18_request_error_t re;
                    if (moq_d18_decode_request_error(env.payload,
                            env.payload_len, &re) == MOQ_OK &&
                        re.error_code == MOQ_REQUEST_ERROR_UNSUPPORTED_EXTENSION)
                        err_ok = true;
                }
            }
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK(err_ok);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Non-mandatory track property is accepted + surfaced (regress) == */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_stream_ref_t ref = open_subscribe(s);
        uint8_t pl[64];
        moq_buf_writer_t pw; moq_buf_writer_init(&pw, pl, sizeof(pl));
        moq_buf_write_vi64(&pw, 9);   /* track_alias */
        moq_buf_write_vi64(&pw, 0);   /* param count */
        moq_buf_write_vi64(&pw, 2);   /* non-mandatory even type */
        moq_buf_write_vi64(&pw, 0x41);
        uint8_t msg[80];
        size_t n = framed(msg, sizeof(msg), MOQ_D18_SUBSCRIBE_OK, pl,
                          moq_buf_writer_offset(&pw));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool ok = false, err = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
                ok = true;
                MOQ_TEST_CHECK(ev.u.subscribe_ok.track_properties.len > 0);
            }
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR) err = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(ok);
        MOQ_TEST_CHECK(!err);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Preflight: 0x33 cancel needs 2 actions (STOP+RESET); with only one ==
     *  free action slot it returns WOULD_BLOCK with nothing surfaced and the
     *  request intact, then commits once a slot is freed. */
    {
        moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_CLIENT, 0, 2);
        moq_stream_ref_t ref = open_subscribe(s);   /* drains its OPEN_BIDI */
        /* A second subscribe (distinct track) leaves its OPEN_BIDI queued,
         * occupying one of the two action slots. */
        moq_subscribe_cfg_t c2; moq_subscribe_cfg_init(&c2);
        c2.track_namespace = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
        c2.track_name = MOQ_BYTES_LITERAL("v2");
        moq_subscription_t h2; moq_session_subscribe(s, &c2, 1, &h2);

        uint8_t props[8];
        size_t plen = mandatory_kvp(props, sizeof(props));
        uint8_t pl[64];
        moq_buf_writer_t pw; moq_buf_writer_init(&pw, pl, sizeof(pl));
        moq_buf_write_vi64(&pw, 9);
        moq_buf_write_vi64(&pw, 0);
        moq_buf_write_raw(&pw, props, plen);
        uint8_t msg[80];
        size_t n = framed(msg, sizeof(msg), MOQ_D18_SUBSCRIBE_OK, pl,
                          moq_buf_writer_offset(&pw));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_ERR_WOULD_BLOCK);
        /* Nothing surfaced, request intact. */
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(s, &ev, 1), 0);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_SUBSCRIPTION);
        /* Free the slot held by the second OPEN_BIDI, then re-feed empty. */
        moq_action_t a;
        MOQ_TEST_CHECK(moq_session_poll_actions(s, &a, 1) > 0);
        moq_action_cleanup(&a);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        bool got = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR) got = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    MOQ_TEST_PASS("d18_unsupported_ext");
    return failures != 0;
}
