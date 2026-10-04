/*
 * Draft-21 session bring-up through the d21 profile (draft-ietf-moq-transport-21):
 * SETUP options on the wire in both directions, the peer limits the session
 * records, the setup and GOAWAY violation matrix, and the profile capabilities
 * that differ from draft 18.
 *
 * Inputs are built with the draft-21 codec (control_d21), whose own byte vectors
 * are pinned in test_control_d21; this file checks what the SESSION does with them.
 * The d21 profile is not wire ready (it is refused by the endpoint), but sessions
 * can be created for it, which is what the simulator and these tests use.
 */
#include <moq/moq.h>
#include <moq/control_d21.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"
#include "../../core/src/session/profile.h"

static int failures = 0;

/* -- helpers ------------------------------------------------------- */

static moq_session_t *make_started(moq_perspective_t persp, uint64_t cache_size,
                                   uint64_t goaway_timeout_us)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = MOQ_VERSION_DRAFT_21;
    cfg.goaway_timeout_us = goaway_timeout_us;
    if (cache_size > 0) {
        cfg.send_auth_token_cache_size = true;
        cfg.auth_token_cache_size = cache_size;
    }
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (moq_session_start(s, 0) < 0) { moq_session_destroy(s); return NULL; }
    return s;
}

static void drain_actions(moq_session_t *s)
{
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
}

static void drain_events(moq_session_t *s)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
}

/* Feed a SETUP whose body is `payload` (a Setup Options block). */
static moq_result_t feed_setup(moq_session_t *s, const uint8_t *payload, size_t plen)
{
    uint8_t msg[200];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_buf_write_vi64(&w, MOQ_D21_STREAM_SETUP);
    moq_buf_write_uint16(&w, (uint16_t)plen);
    if (plen > 0) moq_buf_write_raw(&w, payload, plen);
    return moq_session_on_control_bytes(s, msg, moq_buf_writer_offset(&w), 0);
}

/* A session that has completed setup with the peer sending nothing optional. */
static moq_session_t *make_established(moq_perspective_t persp, uint64_t goaway_timeout_us)
{
    moq_session_t *s = make_started(persp, 0, goaway_timeout_us);
    if (!s) return NULL;
    drain_actions(s);
    feed_setup(s, NULL, 0);
    drain_events(s);
    drain_actions(s);
    return s;
}

static uint64_t poll_close_code(moq_session_t *s)
{
    uint64_t code = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_CLOSE_SESSION) code = a.u.close_session.code;
        moq_action_cleanup(&a);
    }
    return code;
}

/* Frame one control message: type (vi64) + 16-bit length + body. */
static size_t frame(uint8_t *out, size_t cap, uint64_t type, const uint8_t *body, size_t n)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, out, cap);
    moq_buf_write_vi64(&w, type);
    moq_buf_write_uint16(&w, (uint16_t)n);
    if (n) moq_buf_write_raw(&w, body, n);
    return moq_buf_writer_offset(&w);
}

/* Capture the SETUP message the session opened its control stream with. */
static bool capture_setup_options(moq_session_t *s, moq_d21_setup_opts_t *out)
{
    bool found = false;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_OPEN_UNI_CONTROL) {
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, a.u.open_uni_control.data, a.u.open_uni_control.len);
            moq_control_envelope_t env;
            if (moq_d21_decode_envelope(&r, &env) == MOQ_OK &&
                env.msg_type == MOQ_D21_STREAM_SETUP &&
                moq_d21_decode_setup_opts(env.payload, env.payload_len, out) == MOQ_OK)
                found = true;
        }
        moq_action_cleanup(&a);
    }
    return found;
}

/* == A. SETUP the session sends =========================================== */
static void t_setup_emitted(void)
{
    static const char impl[] = "libmoq/" MOQ_VERSION_STRING;

    /* Both perspectives identify the library (9.1.5) and advertise neither
     * MAX_FILTER_RANGES (its default 0 forbids Range Filters, which are never
     * applied, 9.1.6) nor MAX_REQUEST_UPDATES (updates are processed as they
     * arrive, so the default is accurate, 9.1.7). */
    moq_perspective_t persp[] = { MOQ_PERSPECTIVE_CLIENT, MOQ_PERSPECTIVE_SERVER };
    for (size_t i = 0; i < 2; i++) {
        moq_session_t *s = make_started(persp[i], 0, 0);
        MOQ_TEST_CHECK(s != NULL);
        moq_d21_setup_opts_t o;
        MOQ_TEST_CHECK(capture_setup_options(s, &o));
        MOQ_TEST_CHECK(o.has_implementation);
        MOQ_TEST_CHECK(o.implementation.len == sizeof(impl) - 1 &&
                       memcmp(o.implementation.data, impl, sizeof(impl) - 1) == 0);
        MOQ_TEST_CHECK(!o.has_max_filter_ranges);
        MOQ_TEST_CHECK(!o.has_max_request_updates);
        MOQ_TEST_CHECK(!o.has_max_auth_token_cache_size);
        MOQ_TEST_CHECK(!o.has_path && !o.has_authority);
        moq_session_destroy(s);
    }

    /* The cache size still goes out when configured, with the vi64 single-byte form
     * at the boundary (100 = 0x64), alongside the implementation string. */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_CLIENT, 100, 0);
        moq_d21_setup_opts_t o;
        MOQ_TEST_CHECK(capture_setup_options(s, &o));
        MOQ_TEST_CHECK(o.has_max_auth_token_cache_size);
        MOQ_TEST_CHECK_EQ_U64(o.max_auth_token_cache_size, 100);
        MOQ_TEST_CHECK(o.has_implementation);
        moq_session_destroy(s);
    }
}

/* A native-QUIC client names its URI: PATH (0x01) first, AUTHORITY (0x05) after the cache
 * size, each a length-prefixed byte string (9.1.1, 9.1.2). A server never sends them. */
static void t_setup_authority_path(void)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    cfg.version = MOQ_VERSION_DRAFT_21;
    cfg.setup_authority = (moq_bytes_t){ (const uint8_t *)"h:4443", 6 };
    cfg.setup_path = (moq_bytes_t){ (const uint8_t *)"/moq?x=1", 8 };
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) >= 0 && s);
    MOQ_TEST_CHECK(moq_session_start(s, 0) >= 0);
    /* The first option bytes on the wire: Type 1, Length 8, "/moq?x=1"; then
     * AUTHORITY as delta 4 from it, Length 6, "h:4443". */
    static const uint8_t want_path[] = { 0x01, 0x08, '/', 'm', 'o', 'q', '?', 'x', '=', '1',
                                         0x04, 0x06, 'h', ':', '4', '4', '4', '3' };
    bool found = false;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_OPEN_UNI_CONTROL && a.u.open_uni_control.len > sizeof(want_path)) {
            const uint8_t *d = a.u.open_uni_control.data;
            for (size_t i = 0; i + sizeof(want_path) <= a.u.open_uni_control.len; i++)
                if (memcmp(d + i, want_path, sizeof(want_path)) == 0) found = true;
        }
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK(found);
    moq_session_destroy(s);

    /* An empty path-abempty is still sent: PATH with length 0 (non-NULL, empty). */
    {
        moq_session_cfg_t c2 = cfg;
        c2.setup_path = (moq_bytes_t){ (const uint8_t *)"", 0 };
        moq_session_t *s2 = NULL;
        MOQ_TEST_CHECK(moq_session_create(&c2, 0, &s2) >= 0 && s2);
        MOQ_TEST_CHECK(moq_session_start(s2, 0) >= 0);
        moq_d21_setup_opts_t o2;
        MOQ_TEST_CHECK(capture_setup_options(s2, &o2));
        MOQ_TEST_CHECK(o2.has_path && o2.has_authority);
        moq_session_destroy(s2);
    }

    /* A server given the same fields sends neither. */
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    s = NULL;
    MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) >= 0 && s);
    MOQ_TEST_CHECK(moq_session_start(s, 0) >= 0);
    moq_d21_setup_opts_t o;
    MOQ_TEST_CHECK(capture_setup_options(s, &o));
    MOQ_TEST_CHECK(!o.has_path && !o.has_authority);
    moq_session_destroy(s);

    /* Over-long values are refused at create. */
    uint8_t big[MOQ_SETUP_PATH_MAX + 1];
    memset(big, 'a', sizeof(big));
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.setup_path = (moq_bytes_t){ big, sizeof(big) };
    s = NULL;
    MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) < 0);
}

/* == B. Peer limits the session records =================================== */
static void t_setup_peer_options(void)
{
    /* MAX_FILTER_RANGES = 2 (type 0x06), MAX_REQUEST_UPDATES = 3 (+2 -> 0x08),
     * MOQT_IMPLEMENTATION "x" (0x07 lies between them: 0x06, +1, +1). */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 0, 0);
        drain_actions(s);
        static const uint8_t p[] = { 0x06, 0x02, 0x01, 0x01, 'x', 0x01, 0x03 };
        MOQ_TEST_CHECK_EQ_INT((int)feed_setup(s, p, sizeof(p)), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(s->peer_setup.has_max_filter_ranges);
        MOQ_TEST_CHECK_EQ_U64(s->peer_setup.max_filter_ranges, 2);
        MOQ_TEST_CHECK(s->peer_setup.has_max_request_updates);
        MOQ_TEST_CHECK_EQ_U64(s->peer_setup.max_request_updates, 3);
        MOQ_TEST_CHECK(s->peer_setup.has_implementation);
        moq_session_destroy(s);
    }
    /* Nothing sent: the draft defaults, which are both zero (9.1.6, 9.1.7). */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_SERVER, 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(!s->peer_setup.has_max_filter_ranges);
        MOQ_TEST_CHECK_EQ_U64(s->peer_setup.max_filter_ranges, 0);
        MOQ_TEST_CHECK(!s->peer_setup.has_max_request_updates);
        MOQ_TEST_CHECK_EQ_U64(s->peer_setup.max_request_updates, 0);
        MOQ_TEST_CHECK(!s->peer_setup.has_implementation);
        moq_session_destroy(s);
    }
    /* The peer's cache size is still surfaced (unchanged from draft 18). */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 0, 0);
        drain_actions(s);
        static const uint8_t p[] = { 0x04, 0x64 };
        feed_setup(s, p, sizeof(p));
        MOQ_TEST_CHECK_EQ_U64(moq_session_peer_auth_token_cache_size(s), 100);
        moq_session_destroy(s);
    }
    /* Unknown options, GREASE included, are ignored whatever their parity, and a
     * duplicate unknown option is allowed (9.1, 13): 0x9D is odd, 0x11C is even. */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 0, 0);
        drain_actions(s);
        uint8_t p[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, p, sizeof(p));
        moq_buf_write_vi64(&w, 0x06); moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, 0x9D - 0x06); moq_buf_write_vi64(&w, 2);
        moq_buf_write_raw(&w, (const uint8_t *)"zz", 2);
        moq_buf_write_vi64(&w, 0); moq_buf_write_vi64(&w, 1);          /* duplicate */
        moq_buf_write_raw(&w, (const uint8_t *)"y", 1);
        moq_buf_write_vi64(&w, 0x11C - 0x9D); moq_buf_write_vi64(&w, 5);
        MOQ_TEST_CHECK_EQ_INT((int)feed_setup(s, p, moq_buf_writer_offset(&w)), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_U64(s->peer_setup.max_filter_ranges, 1);
        moq_session_destroy(s);
    }
}

/* == C. Setup violations ================================================== */
static void expect_setup_close(const char *what, moq_perspective_t persp,
                               const uint8_t *p, size_t n, uint64_t want_code)
{
    moq_session_t *s = make_started(persp, 0, 0);
    drain_actions(s);
    (void)feed_setup(s, p, n);
    uint64_t code = poll_close_code(s);
    if (s->state != MOQ_SESS_CLOSED || code != want_code) {
        fprintf(stderr, "FAIL: %s: state=%d code=0x%llx want 0x%llx\n", what,
                (int)s->state, (unsigned long long)code, (unsigned long long)want_code);
        failures++;
    }
    moq_session_destroy(s);
}

static void t_setup_violations(void)
{
    static const uint8_t dup_filter[]  = { 0x06, 0x01, 0x00, 0x02 };
    static const uint8_t dup_updates[] = { 0x08, 0x01, 0x00, 0x02 };
    static const uint8_t dup_impl[]    = { 0x07, 0x01, 'a', 0x00, 0x01, 'b' };
    static const uint8_t trunc_value[] = { 0x06 };
    static const uint8_t over_cap[]    = { 0x07, 0xC1, 0x00, 0x00 };      /* length 65536 */
    static const uint8_t short_len[]   = { 0x07, 0x05, 'a', 'b' };
    expect_setup_close("duplicate MAX_FILTER_RANGES", MOQ_PERSPECTIVE_SERVER, dup_filter, sizeof(dup_filter), 0x3);
    expect_setup_close("duplicate MAX_REQUEST_UPDATES", MOQ_PERSPECTIVE_SERVER, dup_updates, sizeof(dup_updates), 0x3);
    expect_setup_close("duplicate MOQT_IMPLEMENTATION", MOQ_PERSPECTIVE_SERVER, dup_impl, sizeof(dup_impl), 0x3);
    expect_setup_close("truncated option value", MOQ_PERSPECTIVE_SERVER, trunc_value, sizeof(trunc_value), 0x3);
    expect_setup_close("value length over 2^16-1", MOQ_PERSPECTIVE_SERVER, over_cap, sizeof(over_cap), 0x3);
    expect_setup_close("value length past the message", MOQ_PERSPECTIVE_SERVER, short_len, sizeof(short_len), 0x3);

    /* PATH and AUTHORITY are client-to-server only (9.1.1, 9.1.2): a client that
     * receives either from the server closes with INVALID_PATH (0x8) /
     * INVALID_AUTHORITY (0x19). */
    static const uint8_t path[]      = { 0x01, 0x01, 'p' };
    static const uint8_t authority[] = { 0x05, 0x01, 'a' };
    expect_setup_close("PATH from a server", MOQ_PERSPECTIVE_CLIENT, path, sizeof(path), 0x8);
    expect_setup_close("AUTHORITY from a server", MOQ_PERSPECTIVE_CLIENT, authority, sizeof(authority), 0x19);
    /* ... and a server receiving them records and accepts (the application owns
     * the URI policy). */
    {
        moq_session_t *s = make_started(MOQ_PERSPECTIVE_SERVER, 0, 0);
        drain_actions(s);
        feed_setup(s, path, sizeof(path));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(s->peer_setup.has_path);
        moq_session_destroy(s);
    }
    /* A SETUP token may not DELETE or USE_ALIAS: nothing is registered yet (9.1.4). */
    static const uint8_t del_token[] = { 0x03, 0x02, 0x00, 0x01 };    /* DELETE alias 1 */
    expect_setup_close("DELETE token in SETUP", MOQ_PERSPECTIVE_SERVER, del_token, sizeof(del_token), 0x3);
    /* A second SETUP is a violation. */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_SERVER, 0);
        feed_setup(s, NULL, 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x3);
        moq_session_destroy(s);
    }
}

/* == D. GOAWAY ============================================================= */
static moq_result_t feed_goaway(moq_session_t *s, const char *uri, uint64_t timeout_ms)
{
    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    size_t n = uri ? strlen(uri) : 0;
    moq_d21_encode_goaway(&w, (const uint8_t *)uri, n, timeout_ms);
    return moq_session_on_control_bytes(s, buf, moq_buf_writer_offset(&w), 1);
}

static void t_goaway(void)
{
    /* Emitted: no Request ID, Timeout from the session's drain timeout (9.2). */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_CLIENT, 5000);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_goaway(s, NULL, 0, 1), (int)MOQ_OK);
        bool seen = false;
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_SEND_CONTROL) {
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, act.u.send_control.data, act.u.send_control.len);
                moq_control_envelope_t env;
                if (moq_d21_decode_envelope(&r, &env) == MOQ_OK && env.msg_type == MOQ_D21_GOAWAY) {
                    moq_d21_goaway_t ga;
                    /* The strict decoder accepts it only because nothing follows the
                     * Timeout: a draft-18 style Request ID would be rejected. */
                    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_goaway(env.payload, env.payload_len, &ga), (int)MOQ_OK);
                    MOQ_TEST_CHECK_EQ_U64(ga.timeout_ms, 5);        /* 5000 us -> 5 ms */
                    MOQ_TEST_CHECK_EQ_SIZE(ga.uri.len, 0);          /* a client sends none */
                    seen = true;
                }
            }
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(seen);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_DRAINING);
        moq_session_destroy(s);
    }
    /* A client may receive a New Session URI, surfaced with the event. */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_CLIENT, 0);
        MOQ_TEST_CHECK_EQ_INT((int)feed_goaway(s, "https://relay.example", 0), (int)MOQ_OK);
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
    /* A server that receives a New Session URI closes with PROTOCOL_VIOLATION
     * (9.2: only a server can say where to go), and an empty URI is fine. */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_SERVER, 0);
        feed_goaway(s, "https://nope", 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x3);
        moq_session_destroy(s);
        s = make_established(MOQ_PERSPECTIVE_SERVER, 0);
        MOQ_TEST_CHECK_EQ_INT((int)feed_goaway(s, NULL, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_DRAINING);
        moq_session_destroy(s);
    }
    /* Draft 18's control-stream GOAWAY ended with a Request ID. There is no such
     * field now, so it is trailing data and the session closes instead of treating
     * the number as a request. */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_CLIENT, 0);
        static const uint8_t d18[] = { 0x00, 0x05, 0x01 };           /* uri "", timeout 5, id 1 */
        uint8_t m[16];
        size_t n = frame(m, sizeof(m), MOQ_D21_GOAWAY, d18, sizeof(d18));
        moq_session_on_control_bytes(s, m, n, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x3);
        moq_session_destroy(s);
    }
    /* A second GOAWAY, and one before ESTABLISHED, close with 0x3 (9.2, 6.3). */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_CLIENT, 0);
        feed_goaway(s, NULL, 0);
        drain_events(s);
        feed_goaway(s, NULL, 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x3);
        moq_session_destroy(s);
        s = make_started(MOQ_PERSPECTIVE_CLIENT, 0, 0);
        drain_actions(s);
        feed_goaway(s, NULL, 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }
}

/* == E. Unknown control-stream messages =================================== */
static void t_control_messages(void)
{
    /* Only SETUP and GOAWAY travel on the control stream (6.3, 9). Request
     * messages belong on request streams, and 0x1E (draft 18's PUBLISH_OK) is
     * reserved: each is an unknown control message and closes the session. */
    static const uint64_t types[] = {
        MOQ_D21_SUBSCRIBE, MOQ_D21_REQUEST_OK, MOQ_D21_REQUEST_ERROR, MOQ_D21_PUBLISH,
        MOQ_D21_FETCH, MOQ_D21_PUBLISH_STATE_NOTIFY, MOQ_D21_PUBLISH_SKIPPED, 0x1E, 0x7F };
    for (size_t i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_CLIENT, 0);
        uint8_t m[8];
        size_t n = frame(m, sizeof(m), types[i], NULL, 0);
        moq_session_on_control_bytes(s, m, n, 1);
        if (s->state != MOQ_SESS_CLOSED || poll_close_code(s) != 0x3) {
            fprintf(stderr, "FAIL: control message type 0x%llx did not close with 0x3\n",
                    (unsigned long long)types[i]);
            failures++;
        }
        moq_session_destroy(s);
    }
}

/* == F. What differs from draft 18 in the profile ========================= */
static void t_capabilities(void)
{
    const moq_profile_ops_t *d16 = moq_profile_lookup(MOQ_VERSION_DRAFT_16);
    const moq_profile_ops_t *d18 = moq_profile_lookup(MOQ_VERSION_DRAFT_18);
    const moq_profile_ops_t *d21 = moq_profile_lookup(MOQ_VERSION_DRAFT_21);
    MOQ_TEST_CHECK(d16 && d18 && d21);
    if (!(d16 && d18 && d21)) return;

    /* The answer to a PUBLISH no longer carries the subscriber's choices, and the
     * Joining FETCH is gone (draft 21 moved the first to REQUEST_UPDATE and replaced
     * the second with fill streams): the core asks these instead of a version. */
    MOQ_TEST_CHECK(d16->publish_ok_carries_params && d18->publish_ok_carries_params);
    MOQ_TEST_CHECK(!d21->publish_ok_carries_params);
    MOQ_TEST_CHECK(d16->supports_joining_fetch && d18->supports_joining_fetch);
    MOQ_TEST_CHECK(!d21->supports_joining_fetch);

    /* Unchanged from draft 18: the uni control pair, request streams, full-range
     * vi64 locations and error codes, root namespaces, ascending-only fetch. */
    MOQ_TEST_CHECK(d21->uses_uni_control_channel && d21->uses_request_streams);
    MOQ_TEST_CHECK_EQ_INT((int)d21->min_track_namespace_fields, 0);
    MOQ_TEST_CHECK_EQ_U64(d21->location_varint_max, UINT64_MAX);
    MOQ_TEST_CHECK_EQ_U64(d21->request_error_wire_max, UINT64_MAX);
    MOQ_TEST_CHECK_EQ_U64(d21->object_payload_len_max, UINT64_MAX);
    MOQ_TEST_CHECK(!d21->fetch_descending_supported);
    MOQ_TEST_CHECK(d21->version == MOQ_VERSION_DRAFT_21);
}

int main(void)
{
    t_setup_emitted();
    t_setup_authority_path();
    t_setup_peer_options();
    t_setup_violations();
    t_goaway();
    t_control_messages();
    t_capabilities();
    if (failures) {
        fprintf(stderr, "test_d21_setup: %d failures\n", failures);
        return 1;
    }
    MOQ_TEST_PASS("test_d21_setup");
}
