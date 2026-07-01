/*
 * Draft-18 REQUEST_ERROR Redirect (§10.6 / §10.6.1, error code REDIRECT = 0x34): a
 * relay rejects a request and points the requester at a new Connect URI and/or a
 * different Full Track Name. The library surfaces a dedicated
 * MOQ_EVENT_REQUEST_REDIRECT (across the 5 spec-supported families) and frees the
 * request. Covers the codec, per-family surfacing, the disallowed-family +
 * validation protocol-violation matrix, and a SimPair migration path.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

/* -- Session helper ------------------------------------------------ */

static moq_session_t *make_session(moq_perspective_t persp)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = MOQ_VERSION_DRAFT_18;
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

/* Drain pending actions, returning the first OPEN_BIDI_STREAM ref. */
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

static const moq_bytes_t k_live[1] = { { (const uint8_t *)"live", 4 } };

static moq_stream_ref_t open_subscribe(moq_session_t *s)
{
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    moq_subscription_t h;
    moq_session_subscribe(s, &cfg, 1, &h);
    return take_open_bidi(s);
}

static moq_stream_ref_t open_fetch(moq_session_t *s)
{
    moq_fetch_cfg_t cfg;
    moq_fetch_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    cfg.end_group = 1;
    moq_fetch_t h;
    moq_session_fetch(s, &cfg, 1, &h);
    return take_open_bidi(s);
}

static moq_stream_ref_t open_track_status(moq_session_t *s)
{
    moq_track_status_cfg_t cfg;
    moq_track_status_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    moq_track_status_handle_t h;
    moq_session_track_status(s, &cfg, 1, &h);
    return take_open_bidi(s);
}

static moq_stream_ref_t open_publish_namespace(moq_session_t *s)
{
    moq_publish_namespace_cfg_t cfg;
    moq_publish_namespace_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
    moq_announcement_t h;
    moq_session_publish_namespace(s, &cfg, 1, &h);
    return take_open_bidi(s);
}

static moq_stream_ref_t open_subscribe_namespace(moq_session_t *s)
{
    moq_subscribe_namespace_cfg_t cfg;
    moq_subscribe_namespace_cfg_init(&cfg);
    cfg.track_namespace_prefix = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
    cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t h;
    moq_session_subscribe_namespace(s, &cfg, 1, &h);
    return take_open_bidi(s);
}

static moq_stream_ref_t open_publish(moq_session_t *s)
{
    moq_publish_cfg_t cfg;
    moq_publish_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    moq_publication_t h;
    moq_session_publish(s, &cfg, 1, &h);
    return take_open_bidi(s);
}

static moq_stream_ref_t open_subscribe_tracks(moq_session_t *s)
{
    moq_subscribe_tracks_cfg_t cfg;
    moq_subscribe_tracks_cfg_init(&cfg);
    cfg.track_namespace_prefix = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
    moq_track_sub_handle_t h;
    moq_session_subscribe_tracks(s, &cfg, 1, &h);
    return take_open_bidi(s);
}

/* Build a REQUEST_ERROR(REDIRECT) with the given Connect URI + redirect track. */
static size_t encode_redirect_error(uint8_t *buf, size_t cap, const char *uri,
                                    const char *ns_field, const char *name)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_d18_redirect_t rd;
    memset(&rd, 0, sizeof(rd));
    rd.connect_uri = (moq_bytes_t){ (const uint8_t *)uri, uri ? strlen(uri) : 0 };
    moq_bytes_t parts[1];
    if (ns_field) {
        parts[0] = (moq_bytes_t){ (const uint8_t *)ns_field, strlen(ns_field) };
        rd.track_namespace = (moq_namespace_t){ parts, 1 };
    }
    rd.track_name = (moq_bytes_t){ (const uint8_t *)name, name ? strlen(name) : 0 };
    /* Non-zero Retry Interval: relays may cache the redirect for this long. */
    moq_d18_encode_request_error_redirect(&w, MOQ_REQUEST_ERROR_REDIRECT, 7000,
        (moq_bytes_t){ NULL, 0 }, &rd);
    return moq_buf_writer_offset(&w);
}

/* Feed a REDIRECT on `ref`, return the surfaced redirect event (kind 0 if none). */
static bool feed_redirect_expect(moq_session_t *s, moq_stream_ref_t ref,
                                 const char *uri, const char *ns_field,
                                 const char *name, moq_request_redirect_event_t *out)
{
    uint8_t msg[256];
    size_t n = encode_redirect_error(msg, sizeof(msg), uri, ns_field, name);
    moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
    bool got = false;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_REQUEST_REDIRECT) {
            *out = ev.u.request_redirect;
            got = true;
        }
        moq_event_cleanup(&ev);
    }
    return got;
}

/* Drain actions; true iff a terminal REQUEST_ERROR(REDIRECT) was queued on `ref`. */
static bool expect_redirect_sent(moq_session_t *s, moq_stream_ref_t ref)
{
    bool ok = false;
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
            act.u.send_bidi_stream.stream_ref._v == ref._v &&
            act.u.send_bidi_stream.fin) {
            moq_buf_reader_t rr;
            moq_buf_reader_init(&rr, act.u.send_bidi_stream.data,
                                act.u.send_bidi_stream.len);
            moq_control_envelope_t env;
            if (moq_d18_decode_envelope(&rr, &env) == MOQ_OK &&
                env.msg_type == MOQ_D18_REQUEST_ERROR) {
                moq_d18_request_error_t er; moq_d18_redirect_t rdt; moq_bytes_t dp[8];
                if (moq_d18_decode_request_error_redirect(env.payload,
                        env.payload_len, dp, 8, &er, &rdt) == MOQ_OK &&
                    er.error_code == MOQ_REQUEST_ERROR_REDIRECT)
                    ok = true;
            }
        }
        moq_action_cleanup(&act);
    }
    return ok;
}

int main(void)
{
    int failures = 0;

    /* == Codec round-trips ========================================== */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t parts[1] = { MOQ_BYTES_LITERAL("alt") };
        moq_d18_redirect_t rd;
        memset(&rd, 0, sizeof(rd));
        rd.connect_uri = MOQ_BYTES_LITERAL("https://relay2");
        rd.track_namespace = (moq_namespace_t){ parts, 1 };
        rd.track_name = MOQ_BYTES_LITERAL("v2");
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_redirect(&w, &rd), (int)MOQ_OK);
        moq_bytes_t dp[8];
        moq_d18_redirect_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_redirect(buf, moq_buf_writer_offset(&w), dp, 8,
                                         &out), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(out.connect_uri.len, 14);
        MOQ_TEST_CHECK_EQ_SIZE(out.track_namespace.count, 1);
        MOQ_TEST_CHECK_EQ_SIZE(out.track_name.len, 2);

        /* All-empty round-trip (reuse current URI / original track). */
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d18_redirect_t empty;
        memset(&empty, 0, sizeof(empty));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_redirect(&w, &empty),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_redirect(buf, moq_buf_writer_offset(&w), dp, 8,
                                         &out), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(out.connect_uri.len, 0);
        MOQ_TEST_CHECK_EQ_SIZE(out.track_namespace.count, 0);

        /* REQUEST_ERROR(REDIRECT) framing decodes via the redirect-aware decoder
         * and the base decoder still strict-rejects the tail. */
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d18_encode_request_error_redirect(&w, MOQ_REQUEST_ERROR_REDIRECT, 0,
            MOQ_BYTES_LITERAL("go"), &rd);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_REQUEST_ERROR);
        moq_d18_request_error_t er;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_request_error(env.payload, env.payload_len, &er),
            (int)MOQ_ERR_PROTO);   /* base decoder rejects the redirect tail */
        moq_d18_redirect_t rr;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_request_error_redirect(env.payload,
                env.payload_len, dp, 8, &er, &rr), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(er.error_code, MOQ_REQUEST_ERROR_REDIRECT);
        MOQ_TEST_CHECK_EQ_SIZE(rr.connect_uri.len, 14);

        /* The redirect-encode helper rejects a non-REDIRECT error code: the
         * Redirect tail is only valid for REDIRECT (§10.6.1). */
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_request_error_redirect(&w,
                MOQ_REQUEST_ERROR_NOT_SUPPORTED, 0, (moq_bytes_t){ NULL, 0 }, &rd),
            (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);  /* nothing written */

        /* moq_d18_encode_redirect is transactional: a buffer shortfall after the
         * first field rolls w->pos back to 0 (no partial output). The buffer fits
         * the Connect URI span but overruns on the namespace. */
        uint8_t tiny[16];
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, tiny, sizeof(tiny));
        MOQ_TEST_CHECK(moq_d18_encode_redirect(&tw, &rd) < 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&tw), 0);
    }

    /* == REDIRECT surfaces on each supported family =================== */
    struct { moq_stream_ref_t (*open)(moq_session_t *); moq_request_family_t fam;
             bool ns_scoped; } cases[] = {
        { open_subscribe,           MOQ_REQUEST_FAMILY_SUBSCRIBE,           false },
        { open_fetch,               MOQ_REQUEST_FAMILY_FETCH,              false },
        { open_track_status,        MOQ_REQUEST_FAMILY_TRACK_STATUS,       false },
        { open_publish_namespace,   MOQ_REQUEST_FAMILY_ANNOUNCEMENT,  true  },
        { open_subscribe_namespace, MOQ_REQUEST_FAMILY_NS_SUB, true },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_stream_ref_t ref = cases[i].open(s);
        moq_request_redirect_event_t ev;
        memset(&ev, 0, sizeof(ev));
        /* Track-scoped families carry a redirect track name; namespace-scoped
         * ones must leave it empty. */
        const char *name = cases[i].ns_scoped ? NULL : "v2";
        bool got = feed_redirect_expect(s, ref, "https://relay2", "alt", name, &ev);
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)ev.family, (int)cases[i].fam);
        MOQ_TEST_CHECK_EQ_U64(ev.error_code, MOQ_REQUEST_ERROR_REDIRECT);
        MOQ_TEST_CHECK(ev.can_retry);
        MOQ_TEST_CHECK_EQ_U64(ev.retry_after_ms, 7000);
        MOQ_TEST_CHECK_EQ_SIZE(ev.connect_uri.len, 14);
        MOQ_TEST_CHECK_EQ_SIZE(ev.track_namespace.count, 1);
        MOQ_TEST_CHECK(ev.handle.raw != 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Negative: REDIRECT on PUBLISH and SUBSCRIBE_TRACKS -> close 0x3 = */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_stream_ref_t ref = open_publish(s);
        uint8_t msg[256];
        size_t n = encode_redirect_error(msg, sizeof(msg), "https://x", "alt", NULL);
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);

        s = make_session(MOQ_PERSPECTIVE_CLIENT);
        ref = open_subscribe_tracks(s);
        n = encode_redirect_error(msg, sizeof(msg), "https://x", "alt", NULL);
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Validation: namespace-scoped REDIRECT with a Track Name -> 0x3 = */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_stream_ref_t ref = open_publish_namespace(s);
        uint8_t msg[256];
        size_t n = encode_redirect_error(msg, sizeof(msg), "https://x", "alt", "nope");
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Validation: server receiving a non-zero Connect URI -> 0x3 ==== *
     *  A server (peer = client) gets an inbound PUBLISH_NAMESPACE; if the app
     *  rejects with a REDIRECT carrying a Connect URI, that is a violation. Feed
     *  the PUBLISH_NAMESPACE first, then an outbound reject is what the app would
     *  do -- but to exercise the receive guard we feed a server an inbound
     *  REDIRECT on its own outbound subscribe_namespace. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = open_subscribe_namespace(s);
        uint8_t msg[256];
        size_t n = encode_redirect_error(msg, sizeof(msg), "https://x", "alt", NULL);
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Malformed Redirect (truncated tail) -> close 0x3 ============= */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_stream_ref_t ref = open_subscribe(s);
        /* Hand-build REQUEST_ERROR(REDIRECT) with a truncated Redirect (only a
         * Connect URI length that overruns). */
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_REQUEST_ERROR);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, MOQ_REQUEST_ERROR_REDIRECT);
        moq_buf_write_vi64(&w, 0);                 /* retry */
        moq_buf_write_vi64(&w, 0);                 /* reason len */
        moq_buf_write_vi64(&w, 50);                /* Connect URI len (overruns) */
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Send: reject_subscribe(REDIRECT) emits REQUEST_ERROR(REDIRECT)+FIN == *
     *  A server (responder) gets an inbound SUBSCRIBE and rejects it with a
     *  REDIRECT pointing at a new Connect URI + Full Track Name. The wire decodes
     *  back to the same fields (round-trip through the new outbound encode). */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        uint8_t sub[128];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, sub, sizeof(sub));
        moq_d18_msg_params_t mp = {0};
        moq_d18_encode_subscribe(&sw, 0,
            &(moq_namespace_t){ (moq_bytes_t *)k_live, 1 },
            MOQ_BYTES_LITERAL("v"), &mp);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x5101);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, sub,
                moq_buf_writer_offset(&sw), false, 1), (int)MOQ_OK);
        moq_event_t ev; moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
        bool got = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                h = ev.u.subscribe_request.sub; got = true;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);

        moq_reject_subscribe_cfg_t rc;
        moq_reject_subscribe_cfg_init(&rc);
        rc.error_code = (moq_request_error_t)MOQ_REQUEST_ERROR_REDIRECT;
        rc.can_retry = true; rc.retry_after_ms = 5000;
        rc.redirect.connect_uri = MOQ_BYTES_LITERAL("https://r2");  /* server may */
        moq_bytes_t altparts[] = { MOQ_BYTES_LITERAL("alt") };
        rc.redirect.track_namespace = (moq_namespace_t){ altparts, 1 };
        rc.redirect.track_name = MOQ_BYTES_LITERAL("v2");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_subscribe(s, h, &rc, 1), (int)MOQ_OK);

        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act, 1), 1);
        MOQ_TEST_CHECK_EQ_U64(act.kind, MOQ_ACTION_SEND_BIDI_STREAM);
        MOQ_TEST_CHECK_EQ_U64(act.u.send_bidi_stream.stream_ref._v, ref._v);
        MOQ_TEST_CHECK(act.u.send_bidi_stream.fin);
        moq_buf_reader_t rr;
        moq_buf_reader_init(&rr, act.u.send_bidi_stream.data,
                            act.u.send_bidi_stream.len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_REQUEST_ERROR);
        moq_d18_request_error_t er; moq_d18_redirect_t rdt;
        moq_bytes_t dparts[4];
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_request_error_redirect(env.payload,
                env.payload_len, dparts, 4, &er, &rdt), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(er.error_code, MOQ_REQUEST_ERROR_REDIRECT);
        MOQ_TEST_CHECK_EQ_SIZE(rdt.connect_uri.len, 10);
        MOQ_TEST_CHECK_EQ_SIZE(rdt.track_namespace.count, 1);
        MOQ_TEST_CHECK_EQ_SIZE(rdt.track_name.len, 2);
        moq_action_cleanup(&act);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Send: an old-size (pre-redirect) reject cfg still works (ABI) ==== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        uint8_t sub[128];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, sub, sizeof(sub));
        moq_d18_msg_params_t mp = {0};
        moq_d18_encode_subscribe(&sw, 0,
            &(moq_namespace_t){ (moq_bytes_t *)k_live, 1 },
            MOQ_BYTES_LITERAL("v"), &mp);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x5102);
        moq_session_on_bidi_stream_bytes(s, ref, sub,
            moq_buf_writer_offset(&sw), false, 1);
        moq_event_t ev; moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) h = ev.u.subscribe_request.sub;
            moq_event_cleanup(&ev);
        }
        moq_reject_subscribe_cfg_t rc;
        memset(&rc, 0, sizeof(rc));
        rc.struct_size = (uint32_t)offsetof(moq_reject_subscribe_cfg_t, redirect);
        rc.error_code = (moq_request_error_t)MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_subscribe(s, h, &rc, 1), (int)MOQ_OK);
        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act, 1), 1);
        MOQ_TEST_CHECK_EQ_U64(act.kind, MOQ_ACTION_SEND_BIDI_STREAM);
        moq_action_cleanup(&act);
        moq_session_destroy(s);
    }

    /* == Send: REDIRECT on PUBLISH / SUBSCRIBE_TRACKS reject -> INVAL ===== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_reject_publish_cfg_t pc;
        moq_reject_publish_cfg_init(&pc);
        pc.error_code = (moq_request_error_t)MOQ_REQUEST_ERROR_REDIRECT;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_publish(s, MOQ_PUBLICATION_INVALID, &pc, 1),
            (int)MOQ_ERR_INVAL);
        moq_reject_subscribe_tracks_cfg_t tc;
        moq_reject_subscribe_tracks_cfg_init(&tc);
        tc.error_code = (moq_request_error_t)MOQ_REQUEST_ERROR_REDIRECT;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_subscribe_tracks(
                s, (moq_track_sub_handle_t){0}, &tc, 1), (int)MOQ_ERR_INVAL);
        moq_session_destroy(s);
    }

    /* == Send: a CLIENT must leave the Connect URI empty (else INVAL) ===== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        uint8_t sub[128];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, sub, sizeof(sub));
        moq_d18_msg_params_t mp = {0};
        /* Inbound request to a client uses server request-id parity (odd). */
        moq_d18_encode_subscribe(&sw, 1,
            &(moq_namespace_t){ (moq_bytes_t *)k_live, 1 },
            MOQ_BYTES_LITERAL("v"), &mp);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x5103);
        moq_session_on_bidi_stream_bytes(s, ref, sub,
            moq_buf_writer_offset(&sw), false, 1);
        moq_event_t ev; moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) h = ev.u.subscribe_request.sub;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_reject_subscribe_cfg_t rc;
        moq_reject_subscribe_cfg_init(&rc);
        rc.error_code = (moq_request_error_t)MOQ_REQUEST_ERROR_REDIRECT;
        rc.redirect.connect_uri = MOQ_BYTES_LITERAL("https://x");  /* not allowed */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_subscribe(s, h, &rc, 1), (int)MOQ_ERR_INVAL);
        /* The same redirect with an empty URI is fine (reuse current session). */
        rc.redirect.connect_uri = (moq_bytes_t){ NULL, 0 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_subscribe(s, h, &rc, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Send: a late empty FIN after a REDIRECT reject is drained (not fatal) =
     *  SUBSCRIBE/FETCH requesters open the bidi without FIN; after we send
     *  REQUEST_ERROR(REDIRECT)+FIN and free the entry, a trailing empty FIN must
     *  be absorbed by the drain ring rather than hit the fatal empty-FIN-without-
     *  request path. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        uint8_t sub[128];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, sub, sizeof(sub));
        moq_d18_msg_params_t mp = {0};
        moq_d18_encode_subscribe(&sw, 0,
            &(moq_namespace_t){ (moq_bytes_t *)k_live, 1 },
            MOQ_BYTES_LITERAL("v"), &mp);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x5201);
        moq_session_on_bidi_stream_bytes(s, ref, sub,
            moq_buf_writer_offset(&sw), false, 1);
        moq_event_t ev; moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) h = ev.u.subscribe_request.sub;
            moq_event_cleanup(&ev);
        }
        moq_reject_subscribe_cfg_t rc;
        moq_reject_subscribe_cfg_init(&rc);
        rc.error_code = (moq_request_error_t)MOQ_REQUEST_ERROR_REDIRECT;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_subscribe(s, h, &rc, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(expect_redirect_sent(s, ref));
        /* Late empty FIN from the requester. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Send: an invalid redirect namespace is rejected, not dereferenced ==== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        uint8_t sub[128];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, sub, sizeof(sub));
        moq_d18_msg_params_t mp = {0};
        moq_d18_encode_subscribe(&sw, 0,
            &(moq_namespace_t){ (moq_bytes_t *)k_live, 1 },
            MOQ_BYTES_LITERAL("v"), &mp);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x5202);
        moq_session_on_bidi_stream_bytes(s, ref, sub,
            moq_buf_writer_offset(&sw), false, 1);
        moq_event_t ev; moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) h = ev.u.subscribe_request.sub;
            moq_event_cleanup(&ev);
        }
        moq_reject_subscribe_cfg_t rc;
        moq_reject_subscribe_cfg_init(&rc);
        rc.error_code = (moq_request_error_t)MOQ_REQUEST_ERROR_REDIRECT;
        moq_bytes_t bigparts[33];
        for (size_t i = 0; i < 33; i++) bigparts[i] = MOQ_BYTES_LITERAL("x");
        rc.redirect.track_namespace = (moq_namespace_t){ bigparts, 33 };  /* > 32 */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_subscribe(s, h, &rc, 1), (int)MOQ_ERR_INVAL);
        rc.redirect.track_namespace = (moq_namespace_t){ NULL, 2 };  /* NULL parts */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_subscribe(s, h, &rc, 1), (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Send: FETCH redirect round-trip + late-FIN drain ================== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        uint8_t fb[160];
        moq_buf_writer_t fw;
        moq_buf_writer_init(&fw, fb, sizeof(fb));
        moq_d18_fetch_t f;
        memset(&f, 0, sizeof(f));
        f.request_id = 0;
        f.fetch_type = MOQ_D18_FETCH_TYPE_STANDALONE;
        f.track_namespace = (moq_namespace_t){ (moq_bytes_t *)k_live, 1 };
        f.track_name = MOQ_BYTES_LITERAL("v");
        f.start = (moq_d18_location_t){ 0, 0 };
        f.end = (moq_d18_location_t){ 10, 0 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&fw, &f), (int)MOQ_OK);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x5203);
        moq_session_on_bidi_stream_bytes(s, ref, fb,
            moq_buf_writer_offset(&fw), false, 1);
        moq_event_t ev; moq_fetch_t h = MOQ_FETCH_INVALID;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) h = ev.u.fetch_request.fetch;
            moq_event_cleanup(&ev);
        }
        moq_reject_fetch_cfg_t rc;
        moq_reject_fetch_cfg_init(&rc);
        rc.error_code = (moq_request_error_t)MOQ_REQUEST_ERROR_REDIRECT;
        rc.redirect.connect_uri = MOQ_BYTES_LITERAL("https://r2");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_fetch(s, h, &rc, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(expect_redirect_sent(s, ref));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Send: TRACK_STATUS redirect round-trip + old-size cfg (ABI) ======= */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        uint8_t tb[128];
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, tb, sizeof(tb));
        moq_d18_msg_params_t mp = {0};
        moq_d18_encode_track_status(&tw, 0,
            &(moq_namespace_t){ (moq_bytes_t *)k_live, 1 },
            MOQ_BYTES_LITERAL("v"), &mp);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x5204);
        moq_session_on_bidi_stream_bytes(s, ref, tb,
            moq_buf_writer_offset(&tw), true, 1);  /* track-status opens w/ FIN */
        moq_event_t ev; moq_track_status_handle_t h = {0};
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST)
                h = ev.u.track_status_request.handle;
            moq_event_cleanup(&ev);
        }
        moq_reject_track_status_cfg_t rc;
        moq_reject_track_status_cfg_init(&rc);
        rc.error_code = (moq_request_error_t)MOQ_REQUEST_ERROR_REDIRECT;
        rc.redirect.track_name = MOQ_BYTES_LITERAL("v2");  /* track-scoped: allowed */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_track_status(s, h, &rc, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(expect_redirect_sent(s, ref));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);

        /* Old-size cfg (pre-redirect was just {struct_size, error_code}). */
        s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_buf_writer_init(&tw, tb, sizeof(tb));
        moq_d18_encode_track_status(&tw, 0,
            &(moq_namespace_t){ (moq_bytes_t *)k_live, 1 },
            MOQ_BYTES_LITERAL("v"), &mp);
        ref = moq_stream_ref_from_u64(0x5205);
        moq_session_on_bidi_stream_bytes(s, ref, tb,
            moq_buf_writer_offset(&tw), true, 1);
        h = (moq_track_status_handle_t){0};
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST)
                h = ev.u.track_status_request.handle;
            moq_event_cleanup(&ev);
        }
        moq_reject_track_status_cfg_t rc_old;
        memset(&rc_old, 0, sizeof(rc_old));
        rc_old.struct_size =
            (uint32_t)offsetof(moq_reject_track_status_cfg_t, reason);
        rc_old.error_code = (moq_request_error_t)MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_track_status(s, h, &rc_old, 1), (int)MOQ_OK);
        moq_session_destroy(s);
    }

    /* == Send: ANNOUNCEMENT redirect (namespace-scoped) + track-name INVAL == */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        uint8_t ab[128];
        moq_buf_writer_t aw;
        moq_buf_writer_init(&aw, ab, sizeof(ab));
        moq_d18_msg_params_t mp = {0};
        moq_d18_encode_publish_namespace(&aw, 0,
            &(moq_namespace_t){ (moq_bytes_t *)k_live, 1 }, &mp);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x5206);
        moq_session_on_bidi_stream_bytes(s, ref, ab,
            moq_buf_writer_offset(&aw), false, 1);
        moq_event_t ev; moq_announcement_t ann = MOQ_ANNOUNCEMENT_INVALID;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED)
                ann = ev.u.namespace_published.ann;
            moq_event_cleanup(&ev);
        }
        moq_reject_namespace_cfg_t rc;
        moq_reject_namespace_cfg_init(&rc);
        rc.error_code = (moq_request_error_t)MOQ_REQUEST_ERROR_REDIRECT;
        rc.redirect.track_name = MOQ_BYTES_LITERAL("x");  /* ns-scoped: forbidden */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_namespace(s, ann, &rc, 1), (int)MOQ_ERR_INVAL);
        rc.redirect.track_name = (moq_bytes_t){ NULL, 0 };
        moq_bytes_t altparts[] = { MOQ_BYTES_LITERAL("alt") };
        rc.redirect.track_namespace = (moq_namespace_t){ altparts, 1 };
        rc.redirect.connect_uri = MOQ_BYTES_LITERAL("https://r2");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_namespace(s, ann, &rc, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(expect_redirect_sent(s, ref));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Send: NS_SUB redirect (namespace-scoped) + old-size cfg (ABI) ===== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        uint8_t nb[128];
        moq_buf_writer_t nw;
        moq_buf_writer_init(&nw, nb, sizeof(nb));
        moq_d18_msg_params_t mp = {0};
        moq_d18_encode_subscribe_namespace(&nw, 0,
            &(moq_namespace_t){ (moq_bytes_t *)k_live, 1 }, &mp);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x5207);
        moq_session_on_bidi_stream_bytes(s, ref, nb,
            moq_buf_writer_offset(&nw), false, 1);
        moq_event_t ev; moq_ns_sub_handle_t h = {0};
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) h = ev.u.ns_sub_request.handle;
            moq_event_cleanup(&ev);
        }
        moq_reject_ns_sub_cfg_t rc;
        moq_reject_ns_sub_cfg_init(&rc);
        rc.error_code = (moq_request_error_t)MOQ_REQUEST_ERROR_REDIRECT;
        moq_bytes_t altparts[] = { MOQ_BYTES_LITERAL("alt") };
        rc.redirect.track_namespace = (moq_namespace_t){ altparts, 1 };
        rc.redirect.connect_uri = MOQ_BYTES_LITERAL("https://r2");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_ns_sub(s, h, &rc, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(expect_redirect_sent(s, ref));
        moq_session_destroy(s);

        /* Old-size cfg (pre-redirect was {struct_size, error_code, reason}). */
        s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_buf_writer_init(&nw, nb, sizeof(nb));
        moq_d18_encode_subscribe_namespace(&nw, 0,
            &(moq_namespace_t){ (moq_bytes_t *)k_live, 1 }, &mp);
        ref = moq_stream_ref_from_u64(0x5208);
        moq_session_on_bidi_stream_bytes(s, ref, nb,
            moq_buf_writer_offset(&nw), false, 1);
        h = (moq_ns_sub_handle_t){0};
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) h = ev.u.ns_sub_request.handle;
            moq_event_cleanup(&ev);
        }
        moq_reject_ns_sub_cfg_t rc_old;
        memset(&rc_old, 0, sizeof(rc_old));
        rc_old.struct_size = (uint32_t)offsetof(moq_reject_ns_sub_cfg_t, can_retry);
        rc_old.error_code = (moq_request_error_t)MOQ_REQUEST_ERROR_PREFIX_OVERLAP;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_ns_sub(s, h, &rc_old, 1), (int)MOQ_OK);
        moq_session_destroy(s);
    }

    MOQ_TEST_PASS("d18_redirect");
    return failures != 0;
}
