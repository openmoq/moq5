/*
 * Draft-18 GOAWAY (§10.4) on the control stream + session drain. D18 GOAWAY is
 * type 0x10 like D16 but additionally carries a Timeout (drain hint, ms) and a
 * control-stream Request ID (smallest unprocessed peer Request ID). The
 * draft-neutral drain core (DRAINING transition, drain-deadline GOAWAY_TIMEOUT
 * close, new-request guard, MOQ_EVENT_GOAWAY) is reused. Covers the wire codec,
 * outbound emission, inbound surfacing, the post-GOAWAY request guard, the drain
 * timeout, the protocol-violation matrix, and SimPair end-to-end.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

/* -- Session helpers ----------------------------------------------- */

static moq_session_t *make_session_to(moq_perspective_t persp,
                                      uint64_t goaway_timeout_us, bool feed_setup)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = MOQ_VERSION_DRAFT_18;
    cfg.goaway_timeout_us = goaway_timeout_us;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (moq_session_start(s, 0) < 0) { moq_session_destroy(s); return NULL; }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    if (feed_setup) {
        uint8_t setup[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, setup, sizeof(setup));
        moq_d18_encode_setup(&w);
        moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0);
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
        moq_action_t a2;
        while (moq_session_poll_actions(s, &a2, 1) > 0) moq_action_cleanup(&a2);
    }
    return s;
}

static moq_session_t *make_session(moq_perspective_t persp)
{
    return make_session_to(persp, 0, true);
}

/* Feed an inbound D18 GOAWAY (full framed control message). */
static moq_result_t feed_goaway(moq_session_t *s, const char *uri,
                                uint64_t timeout_ms, uint64_t request_id)
{
    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    size_t uri_len = uri ? strlen(uri) : 0;
    moq_d18_encode_goaway(&w, (const uint8_t *)uri, uri_len, timeout_ms, request_id);
    return moq_session_on_control_bytes(s, buf, moq_buf_writer_offset(&w), 1);
}

/* Pull the close-session error code from the action queue (0 if none). */
static uint64_t poll_close_code(moq_session_t *s)
{
    uint64_t code = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_CLOSE_SESSION)
            code = a.u.close_session.code;
        moq_action_cleanup(&a);
    }
    return code;
}

int main(void)
{
    int failures = 0;

    /* == Codec round-trip + strict trailing reject =================== */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_goaway(&w, (const uint8_t *)"https://m", 9,
                                       5000, 7), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_GOAWAY);
        MOQ_TEST_CHECK_EQ_U64(MOQ_D18_GOAWAY, 0x10);
        moq_d18_goaway_t ga;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_goaway(env.payload, env.payload_len, &ga),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(ga.uri.len, 9);
        MOQ_TEST_CHECK(memcmp(ga.uri.data, "https://m", 9) == 0);
        MOQ_TEST_CHECK_EQ_U64(ga.timeout_ms, 5000);
        MOQ_TEST_CHECK_EQ_U64(ga.request_id, 7);

        /* Empty URI round-trip. */
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d18_encode_goaway(&w, NULL, 0, 0, 2);
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_goaway(env.payload, env.payload_len, &ga),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(ga.uri.len, 0);

        /* Trailing byte after Request ID is a violation. */
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_GOAWAY);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);   /* uri len */
        moq_buf_write_vi64(&w, 0);   /* timeout */
        moq_buf_write_vi64(&w, 0);   /* request id */
        uint8_t junk = 0xFF; moq_buf_write_raw(&w, &junk, 1);
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_goaway(env.payload, env.payload_len, &ga),
            (int)MOQ_ERR_PROTO);

        /* URI over 8192 is rejected by the encoder. */
        static uint8_t big[8193];
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_goaway(&w, big, sizeof(big), 0, 0),
            (int)MOQ_ERR_INVAL);
    }

    /* == Outbound: D18 session emits a D18-shaped GOAWAY ============== */
    {
        moq_session_t *s = make_session_to(MOQ_PERSPECTIVE_CLIENT, 5000, true);
        MOQ_TEST_CHECK(s != NULL);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_goaway(s, NULL, 0, 1), (int)MOQ_OK);
        bool seen = false;
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_SEND_CONTROL) {
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, act.u.send_control.data,
                                    act.u.send_control.len);
                moq_control_envelope_t env;
                if (moq_d18_decode_envelope(&r, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D18_GOAWAY) {
                    moq_d18_goaway_t ga;
                    /* Decodes as a D18 GOAWAY: carries Timeout + Request ID. */
                    MOQ_TEST_CHECK_EQ_INT(
                        (int)moq_d18_decode_goaway(env.payload, env.payload_len,
                                                   &ga), (int)MOQ_OK);
                    MOQ_TEST_CHECK_EQ_U64(ga.timeout_ms, 5);   /* 5000us -> 5ms */
                    MOQ_TEST_CHECK_EQ_U64(ga.request_id, 1);   /* peer (server) parity */
                    MOQ_TEST_CHECK_EQ_SIZE(ga.uri.len, 0);     /* client: zero URI */
                    seen = true;
                }
            }
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(seen);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_DRAINING);
        moq_session_destroy(s);
    }

    /* == Inbound GOAWAY surfaces the event + enters DRAINING ========== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        /* Server->client GOAWAY MAY carry a URI; client's outbound parity is even. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)feed_goaway(s, "https://relay.example", 0, 0), (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_GOAWAY) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.goaway.new_session_uri.len, 21);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK(s->goaway_received);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_DRAINING);
        moq_session_destroy(s);
    }

    /* == After GOAWAY, a new outbound request is refused ============== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        feed_goaway(s, NULL, 0, 0);
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_subscribe_cfg_t sub;
        moq_subscribe_cfg_init(&sub);
        sub.track_namespace = (moq_namespace_t){ parts, 1 };
        sub.track_name = MOQ_BYTES_LITERAL("v");
        moq_subscription_t sh;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe(s, &sub, 1, &sh), (int)MOQ_ERR_GOAWAY);
        /* TRACK_STATUS is also a request the receiver must not initiate (§10.4). */
        moq_track_status_cfg_t ts;
        moq_track_status_cfg_init(&ts);
        ts.track_namespace = (moq_namespace_t){ parts, 1 };
        ts.track_name = MOQ_BYTES_LITERAL("v");
        moq_track_status_handle_t th;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_track_status(s, &ts, 1, &th), (int)MOQ_ERR_GOAWAY);
        moq_session_destroy(s);
    }

    /* == Drain timeout closes with GOAWAY_TIMEOUT (0x10) ============== */
    {
        moq_session_t *s = make_session_to(MOQ_PERSPECTIVE_CLIENT, 1000, true);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_goaway(s, NULL, 0, 1), (int)MOQ_OK);
        { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
              moq_action_cleanup(&a); }
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_DRAINING);
        /* A tick before the deadline (now 1 + timeout 1000 = 1001) does not close. */
        moq_session_tick(s, 500);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_DRAINING);
        /* A tick past the deadline fires GOAWAY_TIMEOUT. */
        moq_session_tick(s, 5000);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), MOQ_CLOSE_GOAWAY_TIMEOUT);
        moq_session_destroy(s);
    }

    /* == Duplicate GOAWAY -> close 0x3 =============================== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        feed_goaway(s, NULL, 0, 0);
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
        feed_goaway(s, NULL, 0, 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x3);
        moq_session_destroy(s);
    }

    /* == GOAWAY before ESTABLISHED -> close 0x3 ====================== */
    {
        moq_session_t *s = make_session_to(MOQ_PERSPECTIVE_CLIENT, 0, false);
        MOQ_TEST_CHECK(s != NULL);
        feed_goaway(s, NULL, 0, 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Server receiving a non-empty URI -> close 0x3 =============== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        /* Server's outbound parity is odd -> request_id must be odd. */
        feed_goaway(s, "https://nope", 0, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x3);
        moq_session_destroy(s);
    }

    /* == Request ID parity mismatch -> close 0x4 ===================== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        /* Client's outbound parity is even; an odd request_id is invalid. */
        feed_goaway(s, NULL, 0, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x4);
        moq_session_destroy(s);
    }

    /* == SimPair end-to-end: server GOAWAY -> client MOQ_EVENT_GOAWAY = */
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

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_goaway(server, NULL, 0, moq_simpair_now_us(sp)),
            (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_GOAWAY) got = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK(client->goaway_received);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_DRAINING);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_DRAINING);
        moq_simpair_destroy(sp);
    }

    MOQ_TEST_PASS("d18_goaway");
    return failures != 0;
}
