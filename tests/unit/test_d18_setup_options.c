/*
 * Draft-18 conformance closeout: SETUP Options (§10.3.1), defined-but-unmodeled
 * message parameters (§10.2.5/6/13/14), and the descending-FETCH capability guard.
 *
 * Setup Options are vi64 Key-Value-Pairs — NOT the draft-16 QUIC-varint KVP form.
 * The two integer encodings diverge in the value range 64..127 (vi64: one byte;
 * QUIC varint: two), so the boundary tests below pin the vi64 form explicitly.
 */
#include <moq/moq.h>
#include <moq/sim.h>
#include <moq/control_d18.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

static int failures = 0;

/* -- Session harness ------------------------------------------------ */

static moq_session_t *make_started(moq_perspective_t persp, uint64_t cache_size)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (cache_size > 0) {
        cfg.send_auth_token_cache_size = true;
        cfg.auth_token_cache_size = cache_size;
    }
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (moq_session_start(s, 0) < 0) { moq_session_destroy(s); return NULL; }
    return s;
}

/* Feed a hand-built SETUP control message carrying `payload`. */
static moq_result_t feed_setup(moq_session_t *s, const uint8_t *payload,
                               size_t plen)
{
    uint8_t msg[160];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_buf_write_vi64(&w, 0x2F00);                  /* SETUP type */
    moq_buf_write_uint16(&w, (uint16_t)plen);
    if (plen > 0) moq_buf_write_raw(&w, payload, plen);
    return moq_session_on_control_bytes(s, msg, moq_buf_writer_offset(&w), 0);
}

/* -- Param-block builders -------------------------------------------- */

static void put_vi64(moq_buf_writer_t *w, uint64_t v) { moq_buf_write_vi64(w, v); }

static void put_ns1(moq_buf_writer_t *w, const char *part)
{
    put_vi64(w, 1);
    put_vi64(w, strlen(part));
    moq_buf_write_raw(w, (const uint8_t *)part, strlen(part));
}

static void put_span(moq_buf_writer_t *w, const char *b)
{
    put_vi64(w, strlen(b));
    moq_buf_write_raw(w, (const uint8_t *)b, strlen(b));
}

int main(void)
{
    /* == 1. SETUP option codec: vi64 KVP form ========================== */

    /* 1a. Encode: MAX_AUTH_TOKEN_CACHE_SIZE 100 emits the vi64 single-byte
     * value (0x64) at the divergence boundary — a QUIC-varint encoder would
     * emit two bytes (0x40 0x64). */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_d18_setup_opts_t o;
        memset(&o, 0, sizeof(o));
        o.has_max_auth_token_cache_size = true;
        o.max_auth_token_cache_size = 100;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup_opts(&w, &o), (int)MOQ_OK);
        size_t n = moq_buf_writer_offset(&w);
        MOQ_TEST_CHECK(n >= 4);
        /* 16-bit Length is the 2 bytes before the payload; payload is exactly
         * {delta 0x04, vi64 value 0x64}. */
        MOQ_TEST_CHECK_EQ_HEX(buf[n - 2], 0x04);
        MOQ_TEST_CHECK_EQ_HEX(buf[n - 1], 0x64);
        MOQ_TEST_CHECK_EQ_HEX(buf[n - 3], 0x02);   /* Length low byte == 2 */
        MOQ_TEST_CHECK_EQ_HEX(buf[n - 4], 0x00);   /* Length high byte */
    }

    /* 1b. Decode the vi64 form back: {0x04, 0x64} -> 100. */
    {
        uint8_t p[] = { 0x04, 0x64 };
        moq_d18_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_setup_opts(p, sizeof(p), &o),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(o.has_max_auth_token_cache_size);
        MOQ_TEST_CHECK_EQ_U64(o.max_auth_token_cache_size, 100);
    }

    /* 1c. QUIC-varint-shaped bytes for 100 ({0x40,0x64}) must NOT decode as
     * 100 under the vi64 walker (it reads 0x40 as the one-byte value 64). */
    {
        uint8_t p[] = { 0x04, 0x40, 0x64 };
        moq_d18_setup_opts_t o;
        moq_result_t rc = moq_d18_decode_setup_opts(p, sizeof(p), &o);
        MOQ_TEST_CHECK(rc < 0 ||
                       !(o.has_max_auth_token_cache_size &&
                         o.max_auth_token_cache_size == 100));
    }

    /* 1d. Full walk: PATH + MAX + AUTHORITY + unknown even + unknown odd
     * (duplicated) are all consumed; unknowns ignored, flags recorded. */
    {
        uint8_t p[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0x01); put_vi64(&w, 2);            /* PATH "ab" */
        moq_buf_write_raw(&w, (const uint8_t *)"ab", 2);
        put_vi64(&w, 0x03); put_vi64(&w, 100);          /* delta 3 -> 0x04 MAX */
        put_vi64(&w, 0x01); put_vi64(&w, 1);            /* delta 1 -> 0x05 AUTHORITY */
        moq_buf_write_raw(&w, (const uint8_t *)"x", 1);
        put_vi64(&w, 0x01); put_vi64(&w, 5);            /* delta 1 -> 0x06 unknown even */
        put_vi64(&w, 0x01); put_vi64(&w, 3);            /* delta 1 -> 0x07 unknown odd */
        moq_buf_write_raw(&w, (const uint8_t *)"zzz", 3);
        put_vi64(&w, 0x00); put_vi64(&w, 1);            /* delta 0 -> dup unknown odd */
        moq_buf_write_raw(&w, (const uint8_t *)"q", 1);
        moq_d18_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_setup_opts(p, moq_buf_writer_offset(&w), &o),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(o.has_path && o.has_authority);
        MOQ_TEST_CHECK_EQ_U64(o.max_auth_token_cache_size, 100);
    }

    /* 1e. Duplicate known non-repeatable option closes. */
    {
        uint8_t p[] = { 0x04, 0x01, 0x00, 0x02 };   /* MAX=1, dup MAX=2 */
        moq_d18_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_setup_opts(p, sizeof(p), &o),
                              (int)MOQ_ERR_PROTO);
    }

    /* 1f. Over-cap odd value length (> 2^16-1) closes. */
    {
        uint8_t p[8];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0x01);                 /* PATH */
        put_vi64(&w, 0x10000);              /* length 65536: over cap */
        moq_d18_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_setup_opts(p, moq_buf_writer_offset(&w), &o),
            (int)MOQ_ERR_PROTO);
    }

    /* 1g. Malformed AUTHORIZATION_TOKEN structure -> KVP_FORMAT (0x6 path). */
    {
        uint8_t p[] = { 0x03, 0x01, 0x09 };   /* token option, len 1, alias_type 9 */
        moq_d18_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_setup_opts(p, sizeof(p), &o),
                              (int)MOQ_D18_ERR_KVP_FORMAT);
    }

    /* 1h. A REGISTER token option decodes its fields. */
    {
        uint8_t p[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0x03);
        put_vi64(&w, 1 + 1 + 1 + 5);        /* alias_type + alias + type + "alpha" */
        put_vi64(&w, MOQ_AUTH_TOKEN_REGISTER);
        put_vi64(&w, 1);                    /* alias */
        put_vi64(&w, 4);                    /* token type */
        moq_buf_write_raw(&w, (const uint8_t *)"alpha", 5);
        moq_d18_setup_opts_t o;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_setup_opts(p, moq_buf_writer_offset(&w), &o),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(o.auth_token_count, 1);
        MOQ_TEST_CHECK_EQ_U64(o.auth_tokens[0].alias, 1);
        MOQ_TEST_CHECK_EQ_SIZE(o.auth_tokens[0].token_value.len, 5);
    }

    /* == 2. Session inbound SETUP ====================================== */

    /* 2a. SETUP with a REGISTER token + unknown option: establishes, surfaces
     * the token on SETUP_COMPLETE, and the alias is usable by a later request. */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 1024);
        MOQ_TEST_CHECK(s != NULL);
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        uint8_t p[48];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0x03);                              /* AUTH token option */
        put_vi64(&w, 1 + 1 + 1 + 5);
        put_vi64(&w, MOQ_AUTH_TOKEN_REGISTER);
        put_vi64(&w, 1); put_vi64(&w, 4);
        moq_buf_write_raw(&w, (const uint8_t *)"alpha", 5);
        put_vi64(&w, 0x04);                              /* delta -> 0x07 unknown odd */
        put_vi64(&w, 2);
        moq_buf_write_raw(&w, (const uint8_t *)"xy", 2);
        MOQ_TEST_CHECK_EQ_INT(
            (int)feed_setup(s, p, moq_buf_writer_offset(&w)), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        bool tok_ok = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE &&
                ev.u.setup_complete.token_count == 1 &&
                ev.u.setup_complete.tokens &&
                ev.u.setup_complete.tokens[0].token_value.len == 5 &&
                memcmp(ev.u.setup_complete.tokens[0].token_value.data,
                       "alpha", 5) == 0)
                tok_ok = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(tok_ok);
        /* The registered alias resolves in a later SUBSCRIBE (USE_ALIAS 1). */
        uint8_t sub[96];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, sub, sizeof(sub));
        put_vi64(&sw, 0x03);                /* SUBSCRIBE type */
        size_t len_off = moq_buf_writer_offset(&sw);
        moq_buf_write_uint16(&sw, 0);
        size_t body0 = moq_buf_writer_offset(&sw);
        put_vi64(&sw, 0);                   /* request id */
        put_ns1(&sw, "ns");
        put_span(&sw, "t");
        put_vi64(&sw, 1);                   /* one param */
        put_vi64(&sw, 0x03);                /* AUTH_TOKEN */
        put_vi64(&sw, 2);                   /* len: alias_type + alias */
        put_vi64(&sw, MOQ_AUTH_TOKEN_USE_ALIAS);
        put_vi64(&sw, 1);
        moq_buf_patch_uint16(&sw, len_off,
                             (uint16_t)(moq_buf_writer_offset(&sw) - body0));
        moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(0x51),
            sub, moq_buf_writer_offset(&sw), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        bool sub_tok = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST &&
                ev.u.subscribe_request.token_count == 1 &&
                ev.u.subscribe_request.tokens[0].token_value.len == 5 &&
                memcmp(ev.u.subscribe_request.tokens[0].token_value.data,
                       "alpha", 5) == 0)
                sub_tok = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(sub_tok);
        moq_session_destroy(s);
    }

    /* 2b. PATH direction rules: a client receiving PATH closes (INVALID_PATH);
     * a server receiving PATH records it and proceeds. */
    {
        uint8_t p[8];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0x01); put_vi64(&w, 1);
        moq_buf_write_raw(&w, (const uint8_t *)"/", 1);
        size_t plen = moq_buf_writer_offset(&w);

        moq_session_t *c = make_started(MOQ_PERSPECTIVE_CLIENT, 0);
        feed_setup(c, p, plen);
        MOQ_TEST_CHECK_EQ_INT((int)c->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(c);

        moq_session_t *sv = make_started(MOQ_PERSPECTIVE_SERVER, 0);
        feed_setup(sv, p, plen);
        MOQ_TEST_CHECK_EQ_INT((int)sv->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(sv->peer_setup.has_path);
        moq_session_destroy(sv);
    }

    /* 2c. DELETE token in SETUP closes; a malformed token closes. */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 1024);
        uint8_t p[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0x03); put_vi64(&w, 2);
        put_vi64(&w, MOQ_AUTH_TOKEN_DELETE); put_vi64(&w, 1);
        feed_setup(s, p, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);

        moq_session_t *s2 = make_started(MOQ_PERSPECTIVE_SERVER, 1024);
        uint8_t bad[] = { 0x03, 0x01, 0x09 };
        feed_setup(s2, bad, sizeof(bad));
        MOQ_TEST_CHECK_EQ_INT((int)s2->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s2);
    }

    /* 2c2. Semantically malformed token in SETUP closes with the
     * MALFORMED_AUTH_TOKEN session error (0x16) -- there is no request to
     * reject at SETUP time. Distinct from the structural 0x6 close in 2c:
     * the Token structure here is well-formed; the RESOLVED value
     * (zero-length) fails semantic validation. */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 1024);
        uint8_t p[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0x03); put_vi64(&w, 2);
        put_vi64(&w, MOQ_AUTH_TOKEN_USE_VALUE);
        put_vi64(&w, 7);                       /* token type; empty value */
        feed_setup(s, p, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        bool saw_close = false;
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_CLOSE_SESSION) {
                saw_close = true;
                MOQ_TEST_CHECK_EQ_U64(act.u.close_session.code, 0x16);
            }
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(saw_close);
        moq_session_destroy(s);
    }

    /* 2e. Duplicate REGISTER alias in SETUP closes -- and the close STICKS
     * (regression: close_with_error inside token resolution returns MOQ_OK; the
     * handler must not fall through and establish a closed session). */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 1024);
        uint8_t p[48];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        for (int i = 0; i < 2; i++) {       /* two REGISTERs, same alias 1 */
            put_vi64(&w, i == 0 ? 0x03 : 0x00);   /* delta: 0x03, then dup type */
            put_vi64(&w, 1 + 1 + 1 + 1);
            put_vi64(&w, MOQ_AUTH_TOKEN_REGISTER);
            put_vi64(&w, 1); put_vi64(&w, 4);
            moq_buf_write_raw(&w, (const uint8_t *)"v", 1);
        }
        feed_setup(s, p, moq_buf_writer_offset(&w));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        bool complete = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) complete = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!complete);
        moq_session_destroy(s);
    }

    /* 2f. §10.3.1.4: a SETUP REGISTER exceeding the advertised cache size
     * (here the default 0) MUST NOT fail the session -- it is treated as
     * USE_VALUE: established, token surfaced, alias NOT registered. */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 0);
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        uint8_t p[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0x03);
        put_vi64(&w, 1 + 1 + 1 + 5);
        put_vi64(&w, MOQ_AUTH_TOKEN_REGISTER);
        put_vi64(&w, 1); put_vi64(&w, 4);
        moq_buf_write_raw(&w, (const uint8_t *)"alpha", 5);
        MOQ_TEST_CHECK_EQ_INT(
            (int)feed_setup(s, p, moq_buf_writer_offset(&w)), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        bool tok = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE &&
                ev.u.setup_complete.token_count == 1 &&
                ev.u.setup_complete.tokens[0].token_value.len == 5)
                tok = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(tok);
        /* The alias was NOT registered: a later USE_ALIAS is rejected (the
         * request fails; the session stays alive). */
        uint8_t sub[96];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, sub, sizeof(sub));
        put_vi64(&sw, 0x03);
        size_t len_off = moq_buf_writer_offset(&sw);
        moq_buf_write_uint16(&sw, 0);
        size_t body0 = moq_buf_writer_offset(&sw);
        put_vi64(&sw, 0);
        put_ns1(&sw, "ns"); put_span(&sw, "t");
        put_vi64(&sw, 1);
        put_vi64(&sw, 0x03);
        put_vi64(&sw, 2);
        put_vi64(&sw, MOQ_AUTH_TOKEN_USE_ALIAS);
        put_vi64(&sw, 1);
        moq_buf_patch_uint16(&sw, len_off,
                             (uint16_t)(moq_buf_writer_offset(&sw) - body0));
        moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(0x52),
            sub, moq_buf_writer_offset(&sw), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        bool surfaced = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) surfaced = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!surfaced);   /* unknown alias: request auto-rejected */
        moq_session_destroy(s);
    }

    /* 2d. The peer's advertised cache size is surfaced. */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 0);
        uint8_t p[] = { 0x04, 0x64 };
        feed_setup(s, p, sizeof(p));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_U64(moq_session_peer_auth_token_cache_size(s), 100);
        moq_session_destroy(s);
    }

    /* == 3. Outbound SETUP option emission ============================= */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_CLIENT, 100);
        bool found = false;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_OPEN_UNI_CONTROL) {
                const uint8_t *d = a.u.open_uni_control.data;
                size_t n = a.u.open_uni_control.len;
                /* SETUP payload follows the vi64 type + 16-bit length; assert
                 * the vi64 single-byte value form at the boundary (100). */
                MOQ_TEST_CHECK(n >= 4);
                MOQ_TEST_CHECK_EQ_HEX(d[n - 2], 0x04);
                MOQ_TEST_CHECK_EQ_HEX(d[n - 1], 0x64);
                moq_d18_setup_opts_t o;
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_setup_opts(d + (n - 2), 2, &o),
                    (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(o.max_auth_token_cache_size, 100);
                found = true;
            }
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK(found);
        moq_session_destroy(s);

        /* Without the cfg, the SETUP payload stays empty. */
        moq_session_t *s2 = make_started(MOQ_PERSPECTIVE_CLIENT, 0);
        bool empty_ok = false;
        while (moq_session_poll_actions(s2, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_OPEN_UNI_CONTROL) {
                size_t n = a.u.open_uni_control.len;
                const uint8_t *d = a.u.open_uni_control.data;
                /* last two bytes are the 16-bit zero Length */
                empty_ok = (d[n - 1] == 0x00 && d[n - 2] == 0x00);
            }
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK(empty_ok);
        moq_session_destroy(s2);
    }

    /* == 4. Defined-but-unmodeled message parameters =================== */

    /* 4a. SUBSCRIBE with RENDEZVOUS_TIMEOUT (0x04) + NEW_GROUP_REQUEST (0x32):
     * accepted (decoded + dropped). */
    {
        uint8_t p[96];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0);                    /* request id */
        put_ns1(&w, "ns"); put_span(&w, "t");
        put_vi64(&w, 2);
        put_vi64(&w, 0x04); put_vi64(&w, 1000);          /* RENDEZVOUS */
        put_vi64(&w, 0x32 - 0x04); put_vi64(&w, 7);      /* NGR */
        moq_bytes_t parts[4];
        moq_d18_subscribe_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe(p, moq_buf_writer_offset(&w),
                                          parts, 4, &out), (int)MOQ_OK);
    }

    /* 4b. FILL_TIMEOUT in SUBSCRIBE is out of scope -> PROTO. */
    {
        uint8_t p[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0);
        put_ns1(&w, "ns"); put_span(&w, "t");
        put_vi64(&w, 1);
        put_vi64(&w, 0x0A); put_vi64(&w, 5);
        moq_bytes_t parts[4];
        moq_d18_subscribe_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe(p, moq_buf_writer_offset(&w),
                                          parts, 4, &out), (int)MOQ_ERR_PROTO);
    }

    /* 4c. FETCH with FILL_TIMEOUT (0x0A): accepted. */
    {
        uint8_t p[96];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 2);                    /* request id */
        put_vi64(&w, 1);                    /* standalone */
        put_ns1(&w, "ns"); put_span(&w, "t");
        put_vi64(&w, 0); put_vi64(&w, 0);   /* start */
        put_vi64(&w, 1); put_vi64(&w, 0);   /* end */
        put_vi64(&w, 1);
        put_vi64(&w, 0x0A); put_vi64(&w, 5);
        moq_bytes_t parts[4];
        moq_d18_fetch_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_fetch(p, moq_buf_writer_offset(&w),
                                      parts, 4, &out), (int)MOQ_OK);
    }

    /* 4d. REQUEST_UPDATE with NEW_GROUP_REQUEST + TRACK_NAMESPACE_PREFIX:
     * accepted. A zero-field (root) prefix is legal (§10.2.14: 0..32 fields);
     * more than 32 fields rejects. */
    {
        uint8_t p[160];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0);                    /* request id */
        put_vi64(&w, 2);
        put_vi64(&w, 0x32); put_vi64(&w, 3);             /* NGR */
        put_vi64(&w, 0x34 - 0x32);                       /* TNP */
        put_vi64(&w, 1);                                 /* 1 part */
        put_vi64(&w, 2);
        moq_buf_write_raw(&w, (const uint8_t *)"ab", 2);
        moq_d18_request_update_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_request_update(p, moq_buf_writer_offset(&w),
                                               &out), (int)MOQ_OK);

        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0);
        put_vi64(&w, 1);
        put_vi64(&w, 0x34);
        put_vi64(&w, 0);                                 /* 0 parts: root prefix */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_request_update(p, moq_buf_writer_offset(&w),
                                               &out), (int)MOQ_OK);

        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0);
        put_vi64(&w, 1);
        put_vi64(&w, 0x34);
        put_vi64(&w, 33);                                /* over the 32 cap */
        for (int i = 0; i < 33; i++) {
            put_vi64(&w, 1);
            moq_buf_write_raw(&w, (const uint8_t *)"a", 1);
        }
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_request_update(p, moq_buf_writer_offset(&w),
                                               &out), (int)MOQ_ERR_PROTO);
    }

    /* 4e. PUBLISH_OK with NEW_GROUP_REQUEST: accepted. */
    {
        uint8_t p[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 1);
        put_vi64(&w, 0x32); put_vi64(&w, 9);
        moq_d18_publish_ok_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_publish_ok(p, moq_buf_writer_offset(&w), &out),
            (int)MOQ_OK);
    }

    /* 4f. A truly unknown parameter type still closes. */
    {
        uint8_t p[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        put_vi64(&w, 0);
        put_ns1(&w, "ns"); put_span(&w, "t");
        put_vi64(&w, 1);
        put_vi64(&w, 0x66); put_vi64(&w, 1);
        moq_bytes_t parts[4];
        moq_d18_subscribe_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe(p, moq_buf_writer_offset(&w),
                                          parts, 4, &out), (int)MOQ_ERR_PROTO);
    }

    /* 4g. End-to-end: a server receiving a SUBSCRIBE that carries the accepted
     * params surfaces SUBSCRIBE_REQUEST and stays alive. */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 0);
        feed_setup(s, NULL, 0);
        { moq_event_t e; while (moq_session_poll_events(s,&e,1)>0) moq_event_cleanup(&e); }
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        uint8_t m[96];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, m, sizeof(m));
        put_vi64(&w, 0x03);                 /* SUBSCRIBE */
        size_t len_off = moq_buf_writer_offset(&w);
        moq_buf_write_uint16(&w, 0);
        size_t body0 = moq_buf_writer_offset(&w);
        put_vi64(&w, 0);
        put_ns1(&w, "ns"); put_span(&w, "t");
        put_vi64(&w, 2);
        put_vi64(&w, 0x04); put_vi64(&w, 1000);
        put_vi64(&w, 0x32 - 0x04); put_vi64(&w, 7);
        moq_buf_patch_uint16(&w, len_off,
                             (uint16_t)(moq_buf_writer_offset(&w) - body0));
        moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(0x61),
            m, moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) got = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        moq_session_destroy(s);
    }

    /* == 5. Descending FETCH guard ===================================== */

    /* 5a. Draft-18: DESCENDING is refused (ascending-only data plane). */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_CLIENT, 0);
        feed_setup(s, NULL, 0);
        { moq_event_t e; while (moq_session_poll_events(s,&e,1)>0) moq_event_cleanup(&e); }
        { moq_action_t a; while (moq_session_poll_actions(s,&a,1)>0) moq_action_cleanup(&a); }
        moq_fetch_cfg_t fc;
        moq_fetch_cfg_init(&fc);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("ns") };
        fc.track_namespace = (moq_namespace_t){ parts, 1 };
        fc.track_name = MOQ_BYTES_LITERAL("t");
        fc.end_group = 1;
        fc.group_order = MOQ_GROUP_ORDER_DESCENDING;
        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(s, &fc, 1, &h),
                              (int)MOQ_ERR_INVAL);
        fc.group_order = MOQ_GROUP_ORDER_ASCENDING;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(s, &fc, 1, &h),
                              (int)MOQ_OK);
        moq_session_destroy(s);
    }

    /* 5b. Draft-16 still accepts DESCENDING (absolute group ids on the wire). */
    {
        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = moq_alloc_default();
        cfg.version = MOQ_VERSION_DRAFT_16;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 10;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 10;
        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&cfg, &sp), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        moq_fetch_cfg_t fc;
        moq_fetch_cfg_init(&fc);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("ns") };
        fc.track_namespace = (moq_namespace_t){ parts, 1 };
        fc.track_name = MOQ_BYTES_LITERAL("t");
        fc.end_group = 1;
        fc.group_order = MOQ_GROUP_ORDER_DESCENDING;
        moq_fetch_t h;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_fetch(moq_simpair_client(sp), &fc,
                                   moq_simpair_now_us(sp), &h), (int)MOQ_OK);
        moq_simpair_destroy(sp);
    }

    MOQ_TEST_PASS("d18_setup_options");
    return failures != 0;
}
