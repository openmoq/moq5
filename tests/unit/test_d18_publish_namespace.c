/*
 * Draft-18 PUBLISH_NAMESPACE (§10.15) on its own request bidi: codec round-trip
 * and malformed rejection, inbound surfacing of the advertised namespace and
 * resolved auth tokens, the outbound open-a-bidi path, accept/reject responses
 * on the bidi, and the role-keyed idempotent teardown (withdraw / revoke via
 * RESET+STOP), the REQUEST_UPDATE-on-announce close-on-fail, and an end-to-end
 * exchange over SimPair in both directions.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

/* -- Codec round-trips and malformed rejection --------------------- */

static int test_codec(void)
{
    int failures = 0;
    uint8_t buf[256];
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("example.com"),
                            MOQ_BYTES_LITERAL("meeting=1") };
    moq_namespace_t ns = { parts, 2 };

    /* Round-trip with an auth token. */
    {
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[0].token_type = 7;
        p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("tok");
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_publish_namespace(&w, 4, &ns, &p), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_PUBLISH_NAMESPACE);
        moq_bytes_t dp[8];
        moq_d18_publish_namespace_t pn;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_publish_namespace(env.payload, env.payload_len,
                                                  dp, 8, &pn), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(pn.request_id, 4);
        MOQ_TEST_CHECK_EQ_SIZE(pn.track_namespace.count, 2);
        MOQ_TEST_CHECK(memcmp(pn.track_namespace.parts[0].data,
                              "example.com", 11) == 0);
        MOQ_TEST_CHECK_EQ_SIZE(pn.params.auth_token_count, 1);
        MOQ_TEST_CHECK_EQ_U64(pn.params.auth_tokens[0].token_type, 7);
    }

    /* Zero-field namespace round-trips (draft-18 allows 0..32). */
    {
        moq_namespace_t empty = { NULL, 0 };
        moq_d18_msg_params_t p = { 0 };
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_publish_namespace(&w, 0, &empty, &p), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_publish_namespace_t pn;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_publish_namespace(env.payload, env.payload_len,
                                                  dp, 8, &pn), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(pn.track_namespace.count, 0);
    }

    /* A non-AUTHORIZATION_TOKEN parameter (FORWARD) is rejected. */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_PUBLISH_NAMESPACE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);                       /* request id */
        moq_buf_write_vi64(&w, 1);                       /* ns count */
        moq_buf_write_vi64(&w, 3);
        moq_buf_write_raw(&w, (const uint8_t *)"abc", 3);
        moq_buf_write_vi64(&w, 1);                       /* 1 parameter */
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_FORWARD);
        uint8_t one = 1; moq_buf_write_raw(&w, &one, 1);
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_publish_namespace_t pn;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_publish_namespace(env.payload, env.payload_len,
                                                  dp, 8, &pn), (int)MOQ_ERR_PROTO);
    }

    /* A malformed auth Token structure -> MOQ_D18_ERR_KVP_FORMAT (0x6 close). */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_PUBLISH_NAMESPACE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, 3);
        moq_buf_write_raw(&w, (const uint8_t *)"abc", 3);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_AUTHORIZATION_TOKEN);
        moq_buf_write_vi64(&w, 1);                       /* span len */
        moq_buf_write_vi64(&w, 9);                       /* invalid alias type */
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_publish_namespace_t pn;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_publish_namespace(env.payload, env.payload_len,
                                                  dp, 8, &pn),
            (int)MOQ_D18_ERR_KVP_FORMAT);
    }

    return failures;
}

/* -- Session helpers ----------------------------------------------- */

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
    return s;
}

static size_t encode_pns(uint8_t *buf, size_t cap, uint64_t request_id,
                         const moq_d18_msg_params_t *p)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_encode_publish_namespace(&w, request_id, &ns, p);
    return moq_buf_writer_offset(&w);
}

/* Feed an inbound PUBLISH_NAMESPACE on `ref`, return the surfaced handle. */
static moq_announcement_t feed_pns(moq_session_t *s, moq_stream_ref_t ref,
                                   uint64_t request_id,
                                   const moq_d18_msg_params_t *p)
{
    uint8_t msg[128];
    size_t n = encode_pns(msg, sizeof(msg), request_id, p);
    moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
    moq_announcement_t h = { 0 };
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED)
            h = ev.u.namespace_published.ann;
        moq_event_cleanup(&ev);
    }
    return h;
}

/* Scan queued actions for a REQUEST_OK / REQUEST_ERROR on `ref`. */
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

int main(void)
{
    int failures = 0;
    failures += test_codec();

    /* == Inbound PUBLISH_NAMESPACE surfaces the namespace + token ===== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        MOQ_TEST_CHECK(s != NULL);
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[0].token_type = 9;
        p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("pk");
        uint8_t msg[128];
        size_t n = encode_pns(msg, sizeof(msg), 0, &p);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.namespace_published.track_namespace.count, 1);
                MOQ_TEST_CHECK(memcmp(
                    ev.u.namespace_published.track_namespace.parts[0].data,
                    "live", 4) == 0);
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.namespace_published.token_count, 1);
                MOQ_TEST_CHECK_EQ_U64(
                    ev.u.namespace_published.tokens[0].token_type, 9);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Outbound publish_namespace opens a request bidi ============== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace = (moq_namespace_t){ parts, 1 };
        moq_announcement_t h;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_publish_namespace(s, &cfg, 1, &h), (int)MOQ_OK);
        bool opened = false;
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, act.u.open_bidi_stream.data,
                                    act.u.open_bidi_stream.len);
                moq_control_envelope_t env;
                if (moq_d18_decode_envelope(&r, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D18_PUBLISH_NAMESPACE)
                    opened = true;
            }
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(opened);
        moq_session_destroy(s);
    }

    /* == Inbound accept -> REQUEST_OK on the bidi (fin=false) ========= */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_announcement_t h = feed_pns(s, ref, 0, &p);
        moq_accept_namespace_cfg_t ac;
        moq_accept_namespace_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_namespace(s, h, &ac, 1), (int)MOQ_OK);
        bool fin = true;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_OK, &fin));
        MOQ_TEST_CHECK(!fin);    /* announce bidi stays open while established */
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Inbound reject -> REQUEST_ERROR + FIN on the bidi ============ */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_announcement_t h = feed_pns(s, ref, 0, &p);
        moq_reject_namespace_cfg_t rc;
        moq_reject_namespace_cfg_init(&rc);
        rc.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        rc.reason = MOQ_BYTES_LITERAL("nope");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_namespace(s, h, &rc, 1), (int)MOQ_OK);
        bool fin = false;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_ERROR, &fin));
        MOQ_TEST_CHECK(fin);
        /* The peer's request half FINs after seeing our REQUEST_ERROR; the bidi
         * is retained in the drain ring so that trailing FIN is absorbed. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Receiver teardown: peer RESET / STOP -> NAMESPACE_DONE ======= *
     *  The local role is RECEIVER (it got PUBLISH_NAMESPACE), so any peer
     *  teardown means the announcer withdrew. Cover reset, stop, and both
     *  reordered: the first fires DONE, the second is a no-op. */
    {
        const int variants = 3;   /* 0=reset, 1=stop, 2=both */
        for (int v = 0; v < variants; v++) {
            moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
            moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
            moq_d18_msg_params_t p = { 0 };
            moq_announcement_t h = feed_pns(s, ref, 0, &p);
            moq_accept_namespace_cfg_t ac;
            moq_accept_namespace_cfg_init(&ac);
            moq_session_accept_namespace(s, h, &ac, 1);
            moq_action_t a;
            while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);

            if (v == 0) {
                moq_session_on_bidi_stream_reset(s, ref, 0x1, 1);
            } else if (v == 1) {
                moq_session_on_bidi_stream_stop(s, ref, 0x1, 1);
            } else {
                moq_session_on_bidi_stream_reset(s, ref, 0x1, 1);
                moq_session_on_bidi_stream_stop(s, ref, 0x1, 1);
            }
            int done = 0;
            moq_event_t ev;
            while (moq_session_poll_events(s, &ev, 1) > 0) {
                if (ev.kind == MOQ_EVENT_NAMESPACE_DONE) done++;
                moq_event_cleanup(&ev);
            }
            MOQ_TEST_CHECK_EQ_INT(done, 1);   /* exactly one, second is no-op */
            MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
            moq_session_destroy(s);
        }
    }

    /* == REQUEST_UPDATE on an announce bidi -> REQUEST_ERROR+FIN, gone = */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_announcement_t h = feed_pns(s, ref, 0, &p);
        moq_accept_namespace_cfg_t ac;
        moq_accept_namespace_cfg_init(&ac);
        moq_session_accept_namespace(s, h, &ac, 1);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);

        /* Feed a REQUEST_UPDATE (request id 2 = next peer/client id). */
        moq_d18_msg_params_t up = { 0 };
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_request_update(&w, 2, &up);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool fin = false;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_ERROR, &fin));
        MOQ_TEST_CHECK(fin);
        /* The bidi is retained in the drain ring: a late request the peer sent
         * before seeing our FIN is discarded, not mistaken for a fresh request. */
        uint8_t late[128];
        moq_buf_writer_t lw;
        moq_buf_writer_init(&lw, late, sizeof(late));
        moq_bytes_t lp[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t lns = { lp, 1 };
        moq_d18_msg_params_t lmp = { 0 };
        moq_d18_encode_subscribe(&lw, 4, &lns, MOQ_BYTES_LITERAL("v"), &lmp);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, late,
                moq_buf_writer_offset(&lw), false, 1), (int)MOQ_OK);
        bool late_ev = false;
        moq_event_t lev;
        while (moq_session_poll_events(s, &lev, 1) > 0) {
            if (lev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) late_ev = true;
            moq_event_cleanup(&lev);
        }
        MOQ_TEST_CHECK(!late_ev);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* The drain ref retires on FIN; a teardown now produces no event. */
        moq_session_on_bidi_stream_reset(s, ref, 0x1, 1);
        bool any = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) { any = true; moq_event_cleanup(&ev); }
        MOQ_TEST_CHECK(!any);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == cancel_namespace with a reason is rejected (D18) ============= */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_announcement_t h = feed_pns(s, ref, 0, &p);
        moq_accept_namespace_cfg_t ac;
        moq_accept_namespace_cfg_init(&ac);
        moq_session_accept_namespace(s, h, &ac, 1);
        moq_cancel_namespace_cfg_t cc;
        moq_cancel_namespace_cfg_init(&cc);
        cc.reason = MOQ_BYTES_LITERAL("x");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_cancel_namespace(s, h, &cc, 1), (int)MOQ_ERR_INVAL);
        /* Empty reason succeeds (RESET+STOP). */
        moq_cancel_namespace_cfg_init(&cc);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_cancel_namespace(s, h, &cc, 1), (int)MOQ_OK);
        moq_session_destroy(s);
    }

    /* == Auth reject: unknown USE_ALIAS -> REQUEST_ERROR(0x17)+FIN ===== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
        p.auth_tokens[0].alias = 99;
        uint8_t msg[128];
        size_t n = encode_pns(msg, sizeof(msg), 0, &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool any = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) any = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!any);
        bool fin = false;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_ERROR, &fin));
        MOQ_TEST_CHECK(fin);
        /* The rejected (uncommitted) request bidi is drained: a trailing peer FIN
         * is absorbed, not mistaken for a fresh request. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Fragmented inbound REQUEST_UPDATE on an announce bidi ========= */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_announcement_t h = feed_pns(s, ref, 0, &p);
        moq_accept_namespace_cfg_t ac;
        moq_accept_namespace_cfg_init(&ac);
        moq_session_accept_namespace(s, h, &ac, 1);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        moq_d18_msg_params_t up = { 0 };
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_request_update(&w, 2, &up);
        size_t n = moq_buf_writer_offset(&w);
        for (size_t i = 0; i < n; i++) {
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, ref, &msg[i], 1,
                    false, 1), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        }
        bool fin = false;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_ERROR, &fin));
        MOQ_TEST_CHECK(fin);
        moq_session_destroy(s);
    }

    /* == End-to-end: accept flow over SimPair ========================= */
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

        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace = (moq_namespace_t){ parts, 1 };
        moq_announcement_t ch;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_publish_namespace(client, &cfg, 1, &ch), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_announcement_t sh = { 0 };
        bool got_req = false;
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                got_req = true; sh = ev.u.namespace_published.ann;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got_req);
        moq_accept_namespace_cfg_t ac;
        moq_accept_namespace_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_namespace(server, sh, &ac,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool accepted = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_ACCEPTED) accepted = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(accepted);

        /* Withdraw: announcer cancels the request; receiver sees NAMESPACE_DONE. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_publish_namespace_done(client, ch,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        bool done = false;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_DONE) done = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(done);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == End-to-end: reject flow over SimPair ========================= */
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

        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace = (moq_namespace_t){ parts, 1 };
        moq_announcement_t ch;
        moq_session_publish_namespace(client, &cfg, 1, &ch);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_announcement_t sh = { 0 };
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED)
                sh = ev.u.namespace_published.ann;
            moq_event_cleanup(&ev);
        }
        moq_reject_namespace_cfg_t rc;
        moq_reject_namespace_cfg_init(&rc);
        rc.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        rc.reason = MOQ_BYTES_LITERAL("no");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_namespace(server, sh, &rc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool rejected = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_REJECTED) rejected = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(rejected);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == Announcer: a second response after establishment is fatal ==== */
    {
        /* Establish an announcer (open the bidi, accept it with REQUEST_OK). */
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace = (moq_namespace_t){ parts, 1 };
        moq_announcement_t h;
        moq_session_publish_namespace(s, &cfg, 1, &h);
        moq_action_t act;
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        if (moq_session_poll_actions(s, &act, 1) == 1) {
            ref = act.u.open_bidi_stream.stream_ref;
            moq_action_cleanup(&act);
        }
        uint8_t ok[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        moq_d18_encode_request_ok(&w);
        size_t oklen = moq_buf_writer_offset(&w);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, ok, oklen, false, 1),
            (int)MOQ_OK);
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
        /* A second REQUEST_OK on the now-established announce bidi is fatal. */
        (void)moq_session_on_bidi_stream_bytes(s, ref, ok, oklen, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Announcer: REQUEST_ERROR after establishment is fatal ======== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace = (moq_namespace_t){ parts, 1 };
        moq_announcement_t h;
        moq_session_publish_namespace(s, &cfg, 1, &h);
        moq_action_t act;
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        if (moq_session_poll_actions(s, &act, 1) == 1) {
            ref = act.u.open_bidi_stream.stream_ref;
            moq_action_cleanup(&act);
        }
        uint8_t ok[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        moq_d18_encode_request_ok(&w);
        moq_session_on_bidi_stream_bytes(s, ref, ok, moq_buf_writer_offset(&w),
                                         false, 1);
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
        uint8_t er[32];
        moq_buf_writer_init(&w, er, sizeof(er));
        moq_d18_encode_request_error(&w, MOQ_REQUEST_ERROR_NOT_SUPPORTED, 0,
                                     (moq_bytes_t){ NULL, 0 });
        (void)moq_session_on_bidi_stream_bytes(s, ref, er,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == REQUEST_UPDATE before the receiver accepts is fatal ========== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        (void)feed_pns(s, ref, 0, &p);   /* PENDING_RECEIVER: not yet accepted */
        moq_d18_msg_params_t up = { 0 };
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_request_update(&w, 2, &up);
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Announcer REQUEST_ERROR with a split FIN does not close ======= *
     *  The peer's REQUEST_ERROR bytes and the FIN arrive in separate callbacks;
     *  the terminal response frees the entry but retains the bidi in the drain
     *  ring, so the trailing empty FIN is absorbed, not mistaken for a request. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace = (moq_namespace_t){ parts, 1 };
        moq_announcement_t h;
        moq_session_publish_namespace(s, &cfg, 1, &h);
        moq_action_t act;
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        if (moq_session_poll_actions(s, &act, 1) == 1) {
            ref = act.u.open_bidi_stream.stream_ref;
            moq_action_cleanup(&act);
        }
        uint8_t er[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, er, sizeof(er));
        moq_d18_encode_request_error(&w, MOQ_REQUEST_ERROR_NOT_SUPPORTED, 0,
                                     (moq_bytes_t){ NULL, 0 });
        /* REQUEST_ERROR bytes (no FIN) -> NAMESPACE_REJECTED. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, er,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool rejected = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_REJECTED) rejected = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(rejected);
        /* The trailing empty FIN is drained, not a "request without request". */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == A request message on an established announce bidi is fatal ==== *
     *  Guards against an announce slot being passed as a subscription/fetch slot
     *  (cross-pool corruption). After accept, a SUBSCRIBE on that bidi closes. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_announcement_t h = feed_pns(s, ref, 0, &p);
        moq_accept_namespace_cfg_t ac;
        moq_accept_namespace_cfg_init(&ac);
        moq_session_accept_namespace(s, h, &ac, 1);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        /* A SUBSCRIBE (request id 2) on the established announce bidi: rejected. */
        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_bytes_t sp[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t sns = { sp, 1 };
        moq_d18_msg_params_t mp = { 0 };
        moq_d18_encode_subscribe(&w, 2, &sns, MOQ_BYTES_LITERAL("v"), &mp);
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Drain ring is sized for announcements ======================== *
     *  Small sub/fetch pools but a larger announcement pool: cancelling several
     *  established announcements must not exhaust drain capacity (each
     *  cancellation reserves a drain ref). */
    {
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
        cfg.version = MOQ_VERSION_DRAFT_18;
        cfg.max_subscriptions = 1;
        cfg.max_fetches = 1;
        cfg.max_announcements = 8;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
        moq_session_start(s, 0);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        uint8_t setup[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, setup, sizeof(setup));
        moq_d18_encode_setup(&w);
        moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0);
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);

        /* Establish 3 announcements (distinct namespaces, request ids 0/2/4) and
         * cancel each: with the old drain cap (sub+fetch=2) the 3rd would
         * WOULD_BLOCK. */
        moq_announcement_t handles[3];
        for (int i = 0; i < 3; i++) {
            moq_stream_ref_t ref = moq_stream_ref_from_u64(0x900 + i);
            uint8_t msg[128];
            moq_buf_writer_t mw;
            moq_buf_writer_init(&mw, msg, sizeof(msg));
            moq_bytes_t np[] = { { (const uint8_t *)(i == 0 ? "a" : i == 1 ? "b" : "c"), 1 } };
            moq_namespace_t ns = { np, 1 };
            moq_d18_msg_params_t mp = { 0 };
            moq_d18_encode_publish_namespace(&mw, (uint64_t)(i * 2), &ns, &mp);
            moq_session_on_bidi_stream_bytes(s, ref, msg,
                moq_buf_writer_offset(&mw), false, 1);
            moq_event_t ev;
            while (moq_session_poll_events(s, &ev, 1) > 0) {
                if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED)
                    handles[i] = ev.u.namespace_published.ann;
                moq_event_cleanup(&ev);
            }
            moq_accept_namespace_cfg_t ac;
            moq_accept_namespace_cfg_init(&ac);
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_accept_namespace(s, handles[i], &ac, 1),
                (int)MOQ_OK);
        }
        moq_cancel_namespace_cfg_t cc;
        moq_cancel_namespace_cfg_init(&cc);
        for (int i = 0; i < 3; i++)
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_cancel_namespace(s, handles[i], &cc, 1),
                (int)MOQ_OK);
        moq_session_destroy(s);
    }

    MOQ_TEST_PASS("d18_publish_namespace");
    return failures != 0;
}
