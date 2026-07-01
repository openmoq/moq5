/*
 * Draft-18 SUBSCRIBE_TRACKS (§10.19) + PUBLISH_BLOCKED (§10.20): track discovery
 * on its own request bidi via the generic staging + handoff (a separate
 * track-sub pool, independent of the namespace-sub overlap space). After the
 * accept the bidi stays established and carries PUBLISH_BLOCKED. Covers inbound
 * surfacing (single-read + fragmented), the outbound open-a-bidi path (no FIN),
 * accept + send-publish-blocked, reject + split-FIN drain, auth reject, the
 * independent vs same-family overlap rule, the response-side OK / ERROR /
 * PUBLISH_BLOCKED handling and protocol-violation matrix, the draft-16 rejection,
 * peer teardown, and SimPair end-to-end.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include <moq/sim.h>
#include "test_support.h"
#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"

/* -- Session helpers ----------------------------------------------- */

static moq_session_t *make_session_caps(moq_perspective_t persp,
                                        uint32_t max_events,
                                        uint32_t send_buffer_size,
                                        uint32_t max_track_subs)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (max_events) cfg.max_events = max_events;
    if (send_buffer_size) cfg.send_buffer_size = send_buffer_size;
    if (max_track_subs) cfg.max_track_subscriptions = max_track_subs;
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
    return make_session_caps(persp, 0, 0, 0);
}

static int count_busy_subs(moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state != MOQ_SUB_FREE) n++;
    return n;
}

static size_t encode_st(uint8_t *buf, size_t cap, uint64_t request_id,
                        const char *ns_field, const moq_d18_msg_params_t *p)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { { (const uint8_t *)ns_field, strlen(ns_field) } };
    moq_namespace_t pfx = { parts, 1 };
    moq_d18_encode_subscribe_tracks(&w, request_id, &pfx, p);
    return moq_buf_writer_offset(&w);
}

static size_t encode_sn(uint8_t *buf, size_t cap, uint64_t request_id,
                        const char *ns_field)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { { (const uint8_t *)ns_field, strlen(ns_field) } };
    moq_namespace_t pfx = { parts, 1 };
    moq_d18_encode_subscribe_namespace(&w, request_id, &pfx,
                                       &(moq_d18_msg_params_t){0});
    return moq_buf_writer_offset(&w);
}

/* Feed an inbound SUBSCRIBE_TRACKS (first message, no FIN -- the bidi stays
 * open). Returns the request event handle (zero if none surfaced). */
static moq_track_sub_handle_t feed_st(moq_session_t *s, moq_stream_ref_t ref,
                                      uint64_t request_id, const char *field,
                                      const moq_d18_msg_params_t *p)
{
    uint8_t msg[160];
    size_t n = encode_st(msg, sizeof(msg), request_id, field, p);
    moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
    moq_track_sub_handle_t h = { 0 };
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST)
            h = ev.u.subscribe_tracks_request.handle;
        moq_event_cleanup(&ev);
    }
    return h;
}

static bool action_has_msg(moq_session_t *s, moq_stream_ref_t ref,
                           uint64_t msg_type, bool *out_fin)
{
    bool seen = false;
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
            act.u.send_bidi_stream.stream_ref._v == ref._v) {
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, act.u.send_bidi_stream.data,
                                act.u.send_bidi_stream.len);
            moq_control_envelope_t env;
            if (moq_d18_decode_envelope(&r, &env) == MOQ_OK &&
                env.msg_type == msg_type) {
                seen = true;
                if (out_fin) *out_fin = act.u.send_bidi_stream.fin;
            }
        }
        moq_action_cleanup(&act);
    }
    return seen;
}

/* Find a REQUEST_ERROR on `ref`, returning its error code + fin. */
static bool action_request_error(moq_session_t *s, moq_stream_ref_t ref,
                                 uint64_t *out_code, bool *out_fin)
{
    bool seen = false;
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
            act.u.send_bidi_stream.stream_ref._v == ref._v) {
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, act.u.send_bidi_stream.data,
                                act.u.send_bidi_stream.len);
            moq_control_envelope_t env;
            if (moq_d18_decode_envelope(&r, &env) == MOQ_OK &&
                env.msg_type == MOQ_D18_REQUEST_ERROR) {
                moq_d18_request_error_t er;
                if (moq_d18_decode_request_error(env.payload, env.payload_len,
                                                 &er) == MOQ_OK) {
                    seen = true;
                    if (out_code) *out_code = er.error_code;
                    if (out_fin) *out_fin = act.u.send_bidi_stream.fin;
                }
            }
        }
        moq_action_cleanup(&act);
    }
    return seen;
}

/* Open an outbound subscribe_tracks, returning the bidi ref (FIN must be off). */
static moq_stream_ref_t open_st(moq_session_t *s, moq_track_sub_handle_t *h,
                                bool *out_open_fin)
{
    moq_subscribe_tracks_cfg_t cfg;
    moq_subscribe_tracks_cfg_init(&cfg);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
    moq_session_subscribe_tracks(s, &cfg, 1, h);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    if (out_open_fin) *out_open_fin = true;
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
            ref = act.u.open_bidi_stream.stream_ref;
            if (out_open_fin) *out_open_fin = act.u.open_bidi_stream.fin;
        }
        moq_action_cleanup(&act);
    }
    return ref;
}

static size_t encode_st_ok(uint8_t *buf, size_t cap)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_d18_encode_request_ok(&w);
    return moq_buf_writer_offset(&w);
}

static size_t encode_pb(uint8_t *buf, size_t cap, const char *suffix_field,
                        const char *name)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { { (const uint8_t *)suffix_field,
                              strlen(suffix_field) } };
    moq_namespace_t sfx = { parts, 1 };
    moq_bytes_t tn = { (const uint8_t *)name, strlen(name) };
    moq_d18_encode_publish_blocked(&w, &sfx, tn);
    return moq_buf_writer_offset(&w);
}

int main(void)
{
    int failures = 0;

    /* == Inbound SUBSCRIBE_TRACKS surfaces prefix + forward + token ==== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        MOQ_TEST_CHECK(s != NULL);
        moq_d18_msg_params_t p = { 0 };
        p.has_forward = true; p.forward = 0;
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[0].token_type = 4;
        p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("pk");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        uint8_t msg[160];
        size_t n = encode_st(msg, sizeof(msg), 0, "live", &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.subscribe_tracks_request.track_namespace_prefix.count, 1);
                MOQ_TEST_CHECK(!ev.u.subscribe_tracks_request.forward);
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.subscribe_tracks_request.token_count, 1);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_SUBSCRIBE_TRACKS);
        MOQ_TEST_CHECK_EQ_INT(count_busy_subs(s), 0);   /* staging slot freed */
        moq_session_destroy(s);
    }

    /* == Inbound fragmented byte-by-byte -> one request event ========= */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        uint8_t msg[160];
        size_t n = encode_st(msg, sizeof(msg), 0, "live", &p);
        for (size_t i = 0; i < n; i++) {
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, ref, &msg[i], 1, false, 1),
                (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        }
        int reqs = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST) reqs++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(reqs, 1);
        moq_session_destroy(s);
    }

    /* == Outbound subscribe_tracks opens a request bidi (no FIN) ======= */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_track_sub_handle_t h;
        bool open_fin = true;
        moq_subscribe_tracks_cfg_t cfg;
        moq_subscribe_tracks_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe_tracks(s, &cfg, 1, &h), (int)MOQ_OK);
        bool opened = false;
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, act.u.open_bidi_stream.data,
                                    act.u.open_bidi_stream.len);
                moq_control_envelope_t env;
                if (moq_d18_decode_envelope(&r, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D18_SUBSCRIBE_TRACKS)
                    opened = true;
                open_fin = act.u.open_bidi_stream.fin;
            }
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(opened);
        MOQ_TEST_CHECK(!open_fin);   /* bidi stays open for the response stream */
        moq_session_destroy(s);
    }

    /* == Accept -> REQUEST_OK (no FIN); then send PUBLISH_BLOCKED ====== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_track_sub_handle_t h = feed_st(s, ref, 0, "live", &p);
        MOQ_TEST_CHECK(h._opaque != 0);
        moq_accept_subscribe_tracks_cfg_t ac;
        moq_accept_subscribe_tracks_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_subscribe_tracks(s, h, &ac, 1), (int)MOQ_OK);
        bool fin = true;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_OK, &fin));
        MOQ_TEST_CHECK(!fin);   /* stays open */
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);

        /* PUBLISH_BLOCKED on the established bidi (no FIN). */
        moq_bytes_t sparts[] = { MOQ_BYTES_LITERAL("room") };
        moq_namespace_t sfx = { sparts, 1 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_send_publish_blocked(s, h, &sfx,
                MOQ_BYTES_LITERAL("audio"), 1), (int)MOQ_OK);
        bool pbfin = true;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_PUBLISH_BLOCKED, &pbfin));
        MOQ_TEST_CHECK(!pbfin);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Reject -> REQUEST_ERROR + FIN; trailing FIN drained ========== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_track_sub_handle_t h = feed_st(s, ref, 0, "live", &p);
        moq_reject_subscribe_tracks_cfg_t rc;
        moq_reject_subscribe_tracks_cfg_init(&rc);
        rc.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_subscribe_tracks(s, h, &rc, 1), (int)MOQ_OK);
        uint64_t code = 0; bool fin = false;
        MOQ_TEST_CHECK(action_request_error(s, ref, &code, &fin));
        MOQ_TEST_CHECK(fin);
        MOQ_TEST_CHECK_EQ_U64(code, MOQ_REQUEST_ERROR_UNAUTHORIZED);
        /* The request arrived without its FIN, so the bidi is retired via the
         * drain ring until the subscriber closes its half. */
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 1);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);   /* trailing FIN drained */
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Auth reject: unknown USE_ALIAS -> REQUEST_ERROR + FIN, no event */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
        p.auth_tokens[0].alias = 77;
        uint8_t msg[160];
        size_t n = encode_st(msg, sizeof(msg), 0, "live", &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool any = false, fin = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST) any = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!any);
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_ERROR, &fin));
        MOQ_TEST_CHECK(fin);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Same-family PREFIX_OVERLAP -> REQUEST_ERROR(0x30), no event === */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_d18_msg_params_t p = { 0 };
        moq_stream_ref_t r1 = moq_stream_ref_from_u64(1);
        moq_track_sub_handle_t h1 = feed_st(s, r1, 0, "live", &p);
        MOQ_TEST_CHECK(h1._opaque != 0);
        moq_accept_subscribe_tracks_cfg_t ac;
        moq_accept_subscribe_tracks_cfg_init(&ac);
        moq_session_accept_subscribe_tracks(s, h1, &ac, 1);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);

        /* A second, overlapping SUBSCRIBE_TRACKS is rejected. */
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(3);
        uint8_t msg[160];
        size_t n = encode_st(msg, sizeof(msg), 2, "live", &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, msg, n, false, 1),
            (int)MOQ_OK);
        bool any = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST) any = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!any);
        uint64_t code = 0; bool fin = false;
        MOQ_TEST_CHECK(action_request_error(s, r2, &code, &fin));
        MOQ_TEST_CHECK(fin);
        MOQ_TEST_CHECK_EQ_U64(code, MOQ_REQUEST_ERROR_PREFIX_OVERLAP);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Independent overlap: SUBSCRIBE_NAMESPACE prefix does NOT clash = */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        /* An established namespace-subscription with prefix "live". */
        moq_stream_ref_t r1 = moq_stream_ref_from_u64(1);
        uint8_t sn[160];
        size_t sn_n = encode_sn(sn, sizeof(sn), 0, "live");
        moq_session_on_bidi_stream_bytes(s, r1, sn, sn_n, false, 1);
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);

        /* A SUBSCRIBE_TRACKS with the same prefix is accepted (independent
         * overlap space, §10.18/§10.19). */
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(3);
        moq_d18_msg_params_t p = { 0 };
        moq_track_sub_handle_t h = feed_st(s, r2, 2, "live", &p);
        MOQ_TEST_CHECK(h._opaque != 0);   /* request surfaced, not rejected */
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Subscriber: REQUEST_OK -> OK event + established; PUBLISH_BLOCKED */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_track_sub_handle_t h;
        bool of;
        moq_stream_ref_t ref = open_st(s, &h, &of);
        uint8_t ok[32];
        size_t okn = encode_st_ok(ok, sizeof(ok));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, ok, okn, false, 1),
            (int)MOQ_OK);
        bool got_ok = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_OK) got_ok = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got_ok);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_SUBSCRIBE_TRACKS);   /* still established */

        /* A PUBLISH_BLOCKED then surfaces with its suffix + name. */
        uint8_t pb[64];
        size_t pbn = encode_pb(pb, sizeof(pb), "room", "audio");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, pb, pbn, false, 1),
            (int)MOQ_OK);
        bool got_pb = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_BLOCKED) {
                got_pb = true;
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.publish_blocked.track_namespace_suffix.count, 1);
                MOQ_TEST_CHECK(ev.u.publish_blocked.track_name.len == 5);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got_pb);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Subscriber: REQUEST_ERROR -> ERROR event, drained on FIN ===== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_track_sub_handle_t h;
        bool of;
        moq_stream_ref_t ref = open_st(s, &h, &of);
        uint8_t er[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, er, sizeof(er));
        moq_d18_encode_request_error(&w, MOQ_REQUEST_ERROR_UNAUTHORIZED, 0,
                                     (moq_bytes_t){0});
        size_t ern = moq_buf_writer_offset(&w);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, er, ern, false, 1),
            (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR) got = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        /* On the terminal error the requester closes its send half (so the
         * publisher can retire its bidi). */
        bool closed = false;
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_CLOSE_BIDI_STREAM &&
                act.u.close_bidi_stream.stream_ref._v == ref._v)
                closed = true;
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(closed);
        /* The trailing FIN frees the entry (drainable until then). */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_NONE);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Subscriber: PUBLISH_BLOCKED before REQUEST_OK closes 0x3 ====== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_track_sub_handle_t h;
        bool of;
        moq_stream_ref_t ref = open_st(s, &h, &of);
        uint8_t pb[64];
        size_t pbn = encode_pb(pb, sizeof(pb), "room", "audio");
        (void)moq_session_on_bidi_stream_bytes(s, ref, pb, pbn, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Subscriber: a second REQUEST_OK after established closes 0x3 == */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_track_sub_handle_t h;
        bool of;
        moq_stream_ref_t ref = open_st(s, &h, &of);
        uint8_t ok[32];
        size_t okn = encode_st_ok(ok, sizeof(ok));
        moq_session_on_bidi_stream_bytes(s, ref, ok, okn, false, 1);
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
        (void)moq_session_on_bidi_stream_bytes(s, ref, ok, okn, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Malformed SUBSCRIBE_TRACKS (over-32 prefix) -> close 0x3 ====== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE_TRACKS);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);      /* request id */
        moq_buf_write_vi64(&w, 33);     /* 33 prefix fields -> violation */
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == draft-16 session rejects subscribe_tracks (no wire form) ===== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);
        moq_subscribe_tracks_cfg_t cfg;
        moq_subscribe_tracks_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        moq_track_sub_handle_t h;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe_tracks(c, &cfg, 1, &h), (int)MOQ_ERR_INVAL);
        moq_session_destroy(c);
        moq_session_destroy(sv);
    }

    /* == Publisher: peer RESET -> CANCELLED event, session up ========= */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_track_sub_handle_t h = feed_st(s, ref, 0, "live", &p);
        moq_accept_subscribe_tracks_cfg_t ac;
        moq_accept_subscribe_tracks_cfg_init(&ac);
        moq_session_accept_subscribe_tracks(s, h, &ac, 1);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_reset(s, ref, 0x1, 1), (int)MOQ_OK);
        bool cancelled = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED) cancelled = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(cancelled);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_NONE);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == send_publish_blocked in the wrong state is rejected =========== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_track_sub_handle_t h = feed_st(s, ref, 0, "live", &p);
        /* Still PENDING_PUBLISHER (not accepted): publish-blocked is invalid. */
        moq_bytes_t sparts[] = { MOQ_BYTES_LITERAL("room") };
        moq_namespace_t sfx = { sparts, 1 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_send_publish_blocked(s, h, &sfx,
                MOQ_BYTES_LITERAL("audio"), 1), (int)MOQ_ERR_WRONG_STATE);
        moq_session_destroy(s);
    }

    /* == REQUEST_UPDATE on a pending (un-accepted) ST bidi closes 0x3 == */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_track_sub_handle_t h = feed_st(s, ref, 0, "live", &p);
        MOQ_TEST_CHECK(h._opaque != 0);   /* request surfaced, still PENDING */
        /* A REQUEST_UPDATE before the app accepts is a protocol violation. */
        uint8_t up[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, up, sizeof(up));
        moq_d18_encode_request_update(&w, 2, &(moq_d18_msg_params_t){0});
        (void)moq_session_on_bidi_stream_bytes(s, ref, up,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == End-to-end accept + PUBLISH_BLOCKED over SimPair ============== */
    {
        moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
        scfg.alloc = moq_alloc_default();
        scfg.version = MOQ_VERSION_DRAFT_18;
        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&scfg, &sp), (int)MOQ_OK);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_subscribe_tracks_cfg_t cfg;
        moq_subscribe_tracks_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        moq_track_sub_handle_t ch;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe_tracks(client, &cfg, 1, &ch), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_track_sub_handle_t sh = { 0 };
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST)
                sh = ev.u.subscribe_tracks_request.handle;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(sh._opaque != 0);
        moq_accept_subscribe_tracks_cfg_t ac;
        moq_accept_subscribe_tracks_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_subscribe_tracks(server, sh, &ac,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool ok = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_OK) ok = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(ok);
        /* A conforming accept leaves the bidi open with no drain ref pending. */
        MOQ_TEST_CHECK_EQ_SIZE(server->drain_ref_count, 0);

        /* The publisher signals PUBLISH_BLOCKED; the subscriber surfaces it. */
        moq_bytes_t sparts[] = { MOQ_BYTES_LITERAL("room") };
        moq_namespace_t sfx = { sparts, 1 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_send_publish_blocked(server, sh, &sfx,
                MOQ_BYTES_LITERAL("audio"), moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        bool blocked = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_BLOCKED) blocked = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(blocked);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == End-to-end reject over SimPair =============================== */
    {
        moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
        scfg.alloc = moq_alloc_default();
        scfg.version = MOQ_VERSION_DRAFT_18;
        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&scfg, &sp), (int)MOQ_OK);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_subscribe_tracks_cfg_t cfg;
        moq_subscribe_tracks_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        moq_track_sub_handle_t ch;
        moq_session_subscribe_tracks(client, &cfg, 1, &ch);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_track_sub_handle_t sh = { 0 };
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST)
                sh = ev.u.subscribe_tracks_request.handle;
            moq_event_cleanup(&ev);
        }
        moq_reject_subscribe_tracks_cfg_t rc;
        moq_reject_subscribe_tracks_cfg_init(&rc);
        rc.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_subscribe_tracks(server, sh, &rc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool rejected = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR) rejected = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(rejected);
        /* On the terminal error the subscriber closes its send half; the SimPair
         * both-halves model delivers that FIN, so the publisher retires the
         * request bidi's drain ref end-to-end. */
        MOQ_TEST_CHECK_EQ_SIZE(server->drain_ref_count, 0);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    MOQ_TEST_PASS("d18_subscribe_tracks");
    return failures != 0;
}
