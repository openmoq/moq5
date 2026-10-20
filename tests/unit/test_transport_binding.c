/*
 * Transport binding: the public bind contract (sizes, facts, roles, URI
 * syntax, refusals that leave the session untouched), the input refusal of a
 * session whose profile requires a binding it has not received, the SETUP
 * target options a native-QUIC client sends and a WebTransport binding
 * withholds, and a raw server's URI policy with the INVALID_* / MALFORMED_*
 * closes the draft assigns. Draft-16 and draft-18 require no binding: never
 * bound they start as before, and bound they follow the same target rules in
 * their own framing.
 */
#include "test_session_support.h"
#include "test_oom_support.h"
#include "../../core/src/wire/control_d22_internal.h"
#include <moq/control.h>
#include <moq/control_d18.h>
#include <moq/vi64.h>

_Static_assert(MOQ_TRANSPORT_BINDING_V0_SIZE ==
    offsetof(moq_transport_binding_t, accept_uri_ctx) + sizeof(void *), "v0 end");
_Static_assert(MOQ_CONNECTION_TARGET_V0_SIZE ==
    offsetof(moq_connection_target_t, accept_uri_ctx) + sizeof(void *), "target v0 end");
#if UINTPTR_MAX == UINT64_MAX
_Static_assert(MOQ_TRANSPORT_BINDING_V0_SIZE == 72, "LP64 binding floor");
_Static_assert(MOQ_CONNECTION_TARGET_V0_SIZE == 56, "LP64 target floor");
#elif UINTPTR_MAX == UINT32_MAX
_Static_assert(MOQ_TRANSPORT_BINDING_V0_SIZE == 44, "ILP32 binding floor");
_Static_assert(MOQ_CONNECTION_TARGET_V0_SIZE == 32, "ILP32 target floor");
#endif

static int failures;
#define CHECK(name, condition) do { if (!(condition)) { \
    fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, name); ++failures; \
} } while (0)

typedef struct fixture {
    oom_alloc_state_t as;
    moq_alloc_t alloc;
    moq_session_t *s;
    uint32_t decision, present;
    size_t policy_calls;
} fixture_t;

static moq_bytes_t bytes(const char *s)
{ return (moq_bytes_t){(const uint8_t *)s, strlen(s)}; }

static uint32_t accept_uri(void *ctx, uint32_t present,
                           moq_bytes_t authority, moq_bytes_t path)
{
    fixture_t *f = ctx;
    ++f->policy_calls;
    f->present = present;
    CHECK("policy borrowed authority", !authority.len || authority.data != NULL);
    CHECK("policy borrowed path", !path.len || path.data != NULL);
    return f->decision;
}

static bool create(fixture_t *f, moq_perspective_t role, moq_version_t version)
{
    memset(f, 0, sizeof(*f));
    f->alloc = oom_allocator(&f->as);
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), &f->alloc, role);
    cfg.version = version ? version : MOQ_VERSION_DRAFT_22;
    cfg.max_actions = cfg.max_events = 2;
    cfg.send_buffer_size = 65539;
    cfg.send_auth_token_cache_size = true;
    cfg.auth_token_cache_size = 64;
    return moq_session_create(&cfg, 0, &f->s) == MOQ_OK;
}

static void destroy(fixture_t *f)
{
    moq_session_destroy(f->s);
    CHECK("destruction balance", f->as.balance == 0);
}

static moq_transport_binding_t binding(fixture_t *f, uint32_t kind)
{
    moq_transport_binding_t b;
    moq_transport_binding_init(&b);
    b.kind = kind;
    b.facts_known = b.facts_supported = MOQ_BINDING_FACTS_ALL;
    if (kind == MOQ_BINDING_RAW_QUIC &&
        moq_session_perspective(f->s) == MOQ_PERSPECTIVE_SERVER) {
        b.accept_uri = accept_uri;
        b.accept_uri_ctx = f;
    } else {
        b.target_present = MOQ_BINDING_TARGET_COMPLETE;
        b.authority = bytes("uri.example:8443");
        b.path_query = bytes("/a%2Fb?query=?/%23");
    }
    return b;
}

static void refused(fixture_t *f, const moq_transport_binding_t *b,
                    size_t size, moq_result_t expected, const char *name)
{
    moq_session_t before;
    memcpy(&before, f->s, sizeof(before));
    int64_t balance = f->as.balance;
    CHECK(name, moq_session_bind_transport(f->s, b, size) == expected);
    CHECK("bind refusal transaction", memcmp(&before, f->s, sizeof(before)) == 0);
    CHECK("bind refusal allocation balance", balance == f->as.balance);
}

static void test_sizes_facts(void)
{
    fixture_t f;
    if (!create(&f, MOQ_PERSPECTIVE_CLIENT, 0)) { ++failures; return; }
    CHECK("required metadata", moq_session_binding_requirements(f.s) == MOQ_BINDING_FACTS_ALL);
    CHECK("required by version", moq_version_binding_requirements(MOQ_VERSION_DRAFT_22) == MOQ_BINDING_FACTS_ALL);
    CHECK("null requirements", moq_session_binding_requirements(NULL) == 0);
    CHECK("unknown version requirements", moq_version_binding_requirements((moq_version_t)99) == 0);
    moq_transport_binding_t b = binding(&f, MOQ_BINDING_RAW_QUIC);
    refused(&f, NULL, 0, MOQ_ERR_INVAL, "null binding");
    for (size_t n = 0; n < MOQ_TRANSPORT_BINDING_V0_SIZE; ++n) {
        /* A genuinely short allocation catches reads before the size gate. */
        uint8_t *short_input = malloc(n ? n : 1);
        memset(short_input, 0xa5, n ? n : 1);
        refused(&f, (const moq_transport_binding_t *)short_input, n,
                MOQ_ERR_INVAL, "short explicit size");
        free(short_input);
        moq_transport_binding_t small = b;
        small.struct_size = (uint32_t)n;
        refused(&f, &small, sizeof(small), MOQ_ERR_INVAL, "short stamped size");
    }
    b.struct_size = (uint32_t)sizeof(b) + 1;
    refused(&f, &b, sizeof(b), MOQ_ERR_INVAL, "stamp exceeds buffer");
    b = binding(&f, MOQ_BINDING_RAW_QUIC);
    for (uint32_t bit = 1; bit <= MOQ_BINDING_DATAGRAM_RECEIVE; bit <<= 1) {
        b.facts_known = b.facts_supported = MOQ_BINDING_FACTS_ALL & ~bit;
        refused(&f, &b, sizeof(b), MOQ_ERR_WOULD_BLOCK, "omitted fact");
        b.facts_known = MOQ_BINDING_FACTS_ALL;
        refused(&f, &b, sizeof(b), MOQ_ERR_UNSUPPORTED, "known absent fact");
        b.facts_known &= ~bit;
        b.facts_supported |= bit;
        refused(&f, &b, sizeof(b), MOQ_ERR_INVAL, "optimistic fact");
    }
    b = binding(&f, MOQ_BINDING_RAW_QUIC);
    b.facts_known |= UINT32_C(0x80000000);
    refused(&f, &b, sizeof(b), MOQ_ERR_INVAL, "unknown fact bit");
    b = binding(&f, MOQ_BINDING_WEBTRANSPORT_H2);
    refused(&f, &b, sizeof(b), MOQ_ERR_UNSUPPORTED, "H2 explicitly held");
    b.kind = 99;
    refused(&f, &b, sizeof(b), MOQ_ERR_INVAL, "unknown binding kind");
    b.kind = 0;
    refused(&f, &b, sizeof(b), MOQ_ERR_INVAL, "omitted binding kind");
    b = binding(&f, MOQ_BINDING_RAW_QUIC);
    b.target_present = 4;
    refused(&f, &b, sizeof(b), MOQ_ERR_INVAL, "unknown target bit");
    for (uint32_t present = 0; present < MOQ_BINDING_TARGET_COMPLETE; ++present) {
        b.target_present = present;
        refused(&f, &b, sizeof(b), MOQ_ERR_INVAL, "complete client target required");
    }
    b = binding(&f, MOQ_BINDING_RAW_QUIC);
    b.authority = bytes("bad%");
    refused(&f, &b, sizeof(b), MOQ_ERR_INVAL, "bind authority syntax");
    b = binding(&f, MOQ_BINDING_RAW_QUIC);
    b.path_query = bytes("/path#fragment");
    refused(&f, &b, sizeof(b), MOQ_ERR_INVAL, "bind fragment refusal");
    b.path_query = (moq_bytes_t){NULL, 1};
    refused(&f, &b, sizeof(b), MOQ_ERR_INVAL, "bind invalid span");
    b.authority.len = SIZE_MAX;
    refused(&f, &b, sizeof(b), MOQ_ERR_INVAL, "bind length overflow");
    b = binding(&f, MOQ_BINDING_RAW_QUIC);
    b.accept_uri = accept_uri; b.accept_uri_ctx = &f;
    refused(&f, &b, sizeof(b), MOQ_ERR_INVAL, "client cannot carry server policy");
    b = binding(&f, MOQ_BINDING_RAW_QUIC);
    f.as.fail_at = f.as.alloc_count + 1;
    refused(&f, &b, sizeof(b), MOQ_ERR_NOMEM, "target allocation refusal");
    f.as.fail_at = 0;
    struct { moq_transport_binding_t value; uint8_t tail[32]; } future;
    memset(&future, 0xa5, sizeof(future));
    future.value = b;
    future.value.struct_size = (uint32_t)sizeof(future);
    CHECK("future input prefix", moq_session_bind_transport(f.s, &future.value,
        sizeof(future)) == MOQ_OK);
    refused(&f, &b, sizeof(b), MOQ_ERR_WRONG_STATE, "duplicate binding");
    destroy(&f);

    for (size_t n = 0; n <= sizeof(b) + 8; ++n) {
        uint8_t *p = malloc(n + 8);
        memset(p, 0xa5, n + 8);
        moq_transport_binding_init_sized((moq_transport_binding_t *)p, n);
        size_t touched = n < sizeof(b) ? n : sizeof(b);
        for (size_t i = touched; i < n + 8; ++i)
            CHECK("initializer canary", p[i] == 0xa5);
        if (n >= sizeof(uint32_t)) {
            uint32_t stamped; memcpy(&stamped, p, sizeof(stamped));
            CHECK("initializer stamp", stamped == touched);
        }
        free(p);
    }
}

static void test_uri_syntax(void)
{
    static const char *const good_authority[] = {
        "example", "EXAMPLE.com:443", "user:password@host:", "@host", "host:0",
        "host:99999999999999999999", "a%2Fb!$&'()*+,;=", "192.0.2.1",
        "[::]", "[::1]:443", "[2001:db8::1]", "[1:2:3:4:5:6:7:8]",
        "[::ffff:192.0.2.1]", "[1:2:3:4:5:6:192.0.2.1]", "[vF.a:b!$]"
    };
    static const char *const bad_authority[] = {
        "", ":443", "user@", "a@b@c", "host/path", "host?x", "host#f", "a b",
        "%", "%0", "%GG", "host:x", "a\nb", "[::1", "[]", "[:::]",
        "[1:2:3:4:5:6:7]", "[1:2:3:4:5:6:7:8:9]", "[1::2::3]",
        "[::ffff:192.00.2.1]", "[::ffff:256.0.0.1]", "[::1%25eth0]",
        "[v.a]", "[v1.]", "[v1.%20]", "[::1]junk", "::1", "a\x80"
    };
    static const char *const good_path[] = {
        "", "?", "?q=x", "/", "//a", "/a%2fb?x=?/%23", "/a:b@c!$&'()*+,;="
    };
    static const char *const bad_path[] = {"a", "a?b", "/#x", "?#x", "/%", "/%0g", "/ ", "/\x80"};
    for (size_t i = 0; i < sizeof(good_authority)/sizeof(good_authority[0]); ++i)
        CHECK(good_authority[i], moq_uri_authority_valid(bytes(good_authority[i])));
    for (size_t i = 0; i < sizeof(bad_authority)/sizeof(bad_authority[0]); ++i)
        CHECK(bad_authority[i], !moq_uri_authority_valid(bytes(bad_authority[i])));
    for (size_t i = 0; i < sizeof(good_path)/sizeof(good_path[0]); ++i)
        CHECK(good_path[i], moq_uri_path_query_valid(bytes(good_path[i])));
    for (size_t i = 0; i < sizeof(bad_path)/sizeof(bad_path[0]); ++i)
        CHECK(bad_path[i], !moq_uri_path_query_valid(bytes(bad_path[i])));
}

static void test_unbound_routes(void)
{
    for (unsigned route = 0; route < 16; ++route) {
        fixture_t f;
        if (!create(&f, MOQ_PERSPECTIVE_CLIENT, 0)) { ++failures; return; }
        moq_session_t before; memcpy(&before, f.s, sizeof(before));
        moq_stream_ref_t ref = moq_stream_ref_from_u64(40);
        const uint8_t wire[] = {0xaf,0,0,0};
        moq_result_t rc = MOQ_ERR_INTERNAL;
        size_t consumed = 88;
        switch (route) {
        case 0: rc = moq_session_start(f.s, 99); break;
        case 1: rc = moq_session_on_control_bytes(f.s, wire, sizeof(wire), 99); break;
        case 2: rc = moq_session_on_datagram(f.s, wire, sizeof(wire), 99); break;
        case 3: rc = moq_session_on_data_bytes(f.s, ref, NULL, 0, false, 99); break;
        case 4: rc = moq_session_on_data_rcbuf(f.s, ref, NULL, false, 99); break;
        case 5: rc = moq_session_on_data_reset(f.s, ref, 0, 99); break;
        case 6: rc = moq_session_on_data_stop(f.s, ref, 0, 99); break;
        case 7: rc = moq_session_on_bidi_stream_bytes(f.s, ref, NULL, 0, false, 99); break;
        case 8: rc = moq_session_on_bidi_stream_reset(f.s, ref, 0, 99); break;
        case 9: rc = moq_session_on_bidi_stream_stop(f.s, ref, 0, 99); break;
        case 10: rc = f.s->profile->process_control_data(f.s, wire, sizeof(wire), &consumed); break;
        case 11: rc = f.s->profile->start(f.s); break;
        case 12: rc = moq_session_process_pending(f.s, 99); break;
        case 13: {
            moq_request_endpoint_t ep;
            memset(&ep, 0xa5, sizeof(ep));
            moq_request_endpoint_t saved = ep;
            rc = moq_session_validate_inbound_request_stream(f.s, ref, 3, 1, &ep);
            CHECK("unbound request output", memcmp(&ep, &saved, sizeof(ep)) == 0);
            break;
        }
        case 14: rc = moq_session_tick(f.s, 99); break;
        case 15: rc = moq_session_on_data_committed(f.s, ref, 99); break;
        }
        if (rc != MOQ_ERR_WRONG_STATE) fprintf(stderr, "unbound route=%u rc=%d\n", route, rc);
        CHECK("unbound input refusal", rc == MOQ_ERR_WRONG_STATE);
        CHECK("unbound no advance/effects", memcmp(&before, f.s, sizeof(before)) == 0);
        CHECK("transport close before bind", moq_session_on_transport_close(f.s, 0, 100) == MOQ_OK);
        CHECK("transport close state", f.s->state == MOQ_SESS_CLOSED);
        destroy(&f);
    }
}

static void drain(moq_session_t *s)
{
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1)) moq_action_cleanup(&a);
}

static uint64_t close_code(moq_session_t *s)
{
    moq_action_t a;
    return moq_session_poll_actions(s, &a, 1) && a.kind == MOQ_ACTION_CLOSE_SESSION
        ? a.u.close_session.code : UINT64_MAX;
}

static void test_roles_and_copy(void)
{
    for (unsigned role = 0; role < 2; ++role) {
        for (uint32_t kind = MOQ_BINDING_RAW_QUIC; kind <= MOQ_BINDING_WEBTRANSPORT_H3; ++kind) {
            fixture_t f;
            if (!create(&f, role ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT, 0)) { ++failures; return; }
            moq_transport_binding_t b = binding(&f, kind);
            if (kind == MOQ_BINDING_RAW_QUIC && role) {
                b.accept_uri = NULL;
                refused(&f, &b, sizeof(b), MOQ_ERR_INVAL, "raw server requires policy");
                b = binding(&f, kind);
                b.target_present = MOQ_BINDING_TARGET_COMPLETE;
                b.authority = bytes("server.test");
                refused(&f, &b, sizeof(b), MOQ_ERR_INVAL, "raw server cannot supply outbound URI");
                b = binding(&f, kind);
            }
            CHECK("role binding", moq_session_bind_transport(f.s, &b, sizeof(b)) == MOQ_OK);
            CHECK("bound start", moq_session_start(f.s, 0) == MOQ_OK);
            moq_action_t a;
            size_t n = moq_session_poll_actions(f.s, &a, 1);
            CHECK("setup action", n == 1 && a.kind == MOQ_ACTION_OPEN_UNI_CONTROL);
            if (n && a.kind == MOQ_ACTION_OPEN_UNI_CONTROL) {
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, a.u.open_uni_control.data, a.u.open_uni_control.len);
                moq_control_envelope_t env;
                moq_d22_setup_options_t opts;
                moq_result_t rc = moq_d22_decode_envelope(&r, &env);
                CHECK("setup envelope", rc == MOQ_OK);
                if (rc == MOQ_OK) {
                    rc = moq_d22_decode_setup_options(env.payload, env.payload_len, &opts);
                    CHECK("setup options", rc == MOQ_OK);
                    if (rc == MOQ_OK) {
                        bool uri = !role && kind == MOQ_BINDING_RAW_QUIC;
                        CHECK("binding controls URI emission", opts.has_authority == uri && opts.has_path == uri);
                        if (uri) {
                            CHECK("exact authority not SNI", opts.authority.len == b.authority.len &&
                                memcmp(opts.authority.data, b.authority.data, b.authority.len) == 0);
                            CHECK("exact path and query", opts.path.len == b.path_query.len &&
                                memcmp(opts.path.data, b.path_query.data, b.path_query.len) == 0);
                        }
                    }
                }
            }
            destroy(&f);
        }
    }
    fixture_t f;
    if (!create(&f, MOQ_PERSPECTIVE_CLIENT, 0)) { ++failures; return; }
    f.s->send_auth_token_cache_size = false;
    moq_transport_binding_t b = binding(&f, MOQ_BINDING_RAW_QUIC);
    uint8_t authority[] = {'x'}, path[] = {'?'};
    b.authority = (moq_bytes_t){authority, 1}; b.path_query = (moq_bytes_t){path, 1};
    CHECK("copy bind", moq_session_bind_transport(f.s, &b, sizeof(b)) == MOQ_OK);
    authority[0] = 'y'; path[0] = '#';
    test_session_fill_action_queue(f.s);
    CHECK("action refusal", moq_session_start(f.s, 0) == MOQ_ERR_WOULD_BLOCK);
    CHECK("action refusal state", f.s->state == MOQ_SESS_IDLE && f.s->send_len == 0);
    drain(f.s);
    moq_action_t held;
    memset(&held, 0, sizeof(held));
    held.kind = MOQ_ACTION_SEND_CONTROL;
    held.detail_size = (uint32_t)sizeof(moq_send_control_action_t);
    CHECK("retain send arena", push_action(f.s, &held) == MOQ_OK);
    f.s->send_len = f.s->send_cap - 1;
    CHECK("temporary send space", moq_session_start(f.s, 0) == MOQ_ERR_WOULD_BLOCK);
    CHECK("temporary space retains cursor", f.s->send_len == f.s->send_cap - 1 && f.s->state == MOQ_SESS_IDLE);
    drain(f.s);
    f.s->send_len = 0;
    CHECK("copy start", moq_session_start(f.s, 0) == MOQ_OK);
    moq_action_t a;
    /* PATH "?" (type 1), AUTHORITY "x" (type 5, delta 4), MAX_FILTER_RANGES 0
     * (type 6), MAX_REQUEST_UPDATES 1 (type 8, delta 2): the copied bytes,
     * not the caller's since-overwritten ones. The optional implementation
     * identifier is omitted. */
    static const uint8_t expected[] = {0xaf,0,0,10, 1,1,'?',4,1,'x',1,0,2,1};
    CHECK("copied target literal", moq_session_poll_actions(f.s, &a, 1) == 1 &&
        a.u.open_uni_control.len == sizeof(expected) &&
        memcmp(a.u.open_uni_control.data, expected, sizeof(expected)) == 0);
    destroy(&f);
}

static void test_peer_policy(void)
{
    static const struct {
        const char *authority, *path;
        uint32_t decision, kind;
        uint64_t error;
        size_t calls;
    } rows[] = {
        {"host", "/ok?x", MOQ_URI_ACCEPT, MOQ_BINDING_RAW_QUIC, 0, 1},
        {NULL, NULL, MOQ_URI_ACCEPT, MOQ_BINDING_RAW_QUIC, 0, 1},
        {"host", "", MOQ_URI_INVALID_AUTHORITY, MOQ_BINDING_RAW_QUIC, 0x19, 1},
        {"host", "", MOQ_URI_INVALID_PATH, MOQ_BINDING_RAW_QUIC, 8, 1},
        {"host", "", 99, MOQ_BINDING_RAW_QUIC, 1, 1},
        {"bad%", "/ok", MOQ_URI_ACCEPT, MOQ_BINDING_RAW_QUIC, 0x1a, 0},
        {"host", "/bad%", MOQ_URI_ACCEPT, MOQ_BINDING_RAW_QUIC, 9, 0},
        {"host", "fragment#x", MOQ_URI_ACCEPT, MOQ_BINDING_RAW_QUIC, 9, 0},
        {"host", NULL, MOQ_URI_ACCEPT, MOQ_BINDING_WEBTRANSPORT_H3, 0x19, 0},
        {NULL, "/ok", MOQ_URI_ACCEPT, MOQ_BINDING_WEBTRANSPORT_H3, 8, 0}
    };
    for (size_t i = 0; i < sizeof(rows)/sizeof(rows[0]); ++i) {
        fixture_t f;
        if (!create(&f, MOQ_PERSPECTIVE_SERVER, 0)) { ++failures; return; }
        f.decision = rows[i].decision;
        moq_transport_binding_t b = binding(&f, rows[i].kind);
        CHECK("policy bind", moq_session_bind_transport(f.s, &b, sizeof(b)) == MOQ_OK);
        CHECK("policy start", moq_session_start(f.s, 0) == MOQ_OK); drain(f.s);
        uint8_t wire[256], token[] = {1,9,0,'x'};
        moq_d22_kvp_t opts[3]; size_t n = 0;
        if (rows[i].path) opts[n++] = (moq_d22_kvp_t){.type=1,.bytes=bytes(rows[i].path)};
        opts[n++] = (moq_d22_kvp_t){.type=3,.bytes={token,sizeof(token)}};
        if (rows[i].authority) opts[n++] = (moq_d22_kvp_t){.type=5,.bytes=bytes(rows[i].authority)};
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        CHECK("peer fixture encode", moq_d22_encode_setup(&w, opts, n) == MOQ_OK);
        CHECK("peer feed", moq_session_on_control_bytes(f.s, wire, moq_buf_writer_offset(&w), 1) == MOQ_OK);
        CHECK("policy exact calls", f.policy_calls == rows[i].calls);
        if (rows[i].error) {
            CHECK("policy error identity", f.s->state == MOQ_SESS_CLOSED && close_code(f.s) == rows[i].error);
            CHECK("refused policy no auth commit", f.s->peer_token_cache.used_bytes == 0);
        } else {
            CHECK("accepted policy auth commit", f.s->state == MOQ_SESS_ESTABLISHED && f.s->peer_token_cache.used_bytes == 17);
            CHECK("policy presence", f.present == ((rows[i].authority ? MOQ_BINDING_AUTHORITY_PRESENT : 0) |
                (rows[i].path ? MOQ_BINDING_PATH_PRESENT : 0)));
        }
        destroy(&f);
    }
    /* A client closes on either option from its server, over any binding. */
    for (uint32_t kind = MOQ_BINDING_RAW_QUIC; kind <= MOQ_BINDING_WEBTRANSPORT_H3; ++kind) {
        for (unsigned which = 0; which < 2; ++which) {
            fixture_t f;
            if (!create(&f, MOQ_PERSPECTIVE_CLIENT, 0)) { ++failures; return; }
            moq_transport_binding_t b = binding(&f, kind);
            CHECK("client bind", moq_session_bind_transport(f.s, &b, sizeof(b)) == MOQ_OK);
            CHECK("client start", moq_session_start(f.s, 0) == MOQ_OK); drain(f.s);
            uint8_t wire[64];
            moq_d22_kvp_t opt = which ? (moq_d22_kvp_t){.type=5,.bytes=bytes("host")}
                                      : (moq_d22_kvp_t){.type=1,.bytes=bytes("/p")};
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, wire, sizeof(wire));
            CHECK("server fixture encode", moq_d22_encode_setup(&w, &opt, 1) == MOQ_OK);
            CHECK("server feed", moq_session_on_control_bytes(f.s, wire, moq_buf_writer_offset(&w), 1) == MOQ_OK);
            CHECK("option from server closes", f.s->state == MOQ_SESS_CLOSED &&
                close_code(f.s) == (which ? 0x19 : 8));
            destroy(&f);
        }
    }
}

/* -- Draft-16 and draft-18: an optional binding ---------------------- */

typedef struct seen_target {
    size_t calls;
    uint32_t present, verdict;
    uint8_t authority[64], path[64];
    size_t authority_len, path_len;
} seen_target_t;

static uint32_t record_uri(void *ctx, uint32_t present,
                           moq_bytes_t authority, moq_bytes_t path)
{
    seen_target_t *seen = ctx;
    ++seen->calls;
    seen->present = present;
    seen->authority_len = authority.len < sizeof(seen->authority)
        ? authority.len : sizeof(seen->authority);
    seen->path_len = path.len < sizeof(seen->path) ? path.len : sizeof(seen->path);
    if (seen->authority_len) memcpy(seen->authority, authority.data, seen->authority_len);
    if (seen->path_len) memcpy(seen->path, path.data, seen->path_len);
    return seen->verdict;
}

static bool same(moq_bytes_t b, const char *s)
{ return b.len == strlen(s) && (!b.len || memcmp(b.data, s, b.len) == 0); }

typedef struct setup_target {
    bool has_path, has_authority;
    moq_bytes_t path, authority;
} setup_target_t;

/* The target options a client's setup carries, read back with each
 * profile's own decoder: draft-16 counted parameters on the bidirectional
 * channel, draft-18 options on the unidirectional one. */
static bool decode_client_target(moq_version_t v, const moq_action_t *a,
                                 setup_target_t *out)
{
    memset(out, 0, sizeof(*out));
    moq_buf_reader_t r;
    moq_control_envelope_t env;
    if (v == MOQ_VERSION_DRAFT_16) {
        if (a->kind != MOQ_ACTION_SEND_CONTROL) return false;
        moq_buf_reader_init(&r, a->u.send_control.data, a->u.send_control.len);
        if (moq_control_decode_envelope(&r, &env) != MOQ_OK) return false;
        moq_kvp_entry_t params[8];
        moq_d16_setup_t setup = { .params = params, .params_count = 0,
                                  .params_cap = 8 };
        if (moq_d16_decode_client_setup(env.payload, env.payload_len, &setup) != MOQ_OK)
            return false;
        for (size_t i = 0; i < setup.params_count; i++) {
            moq_bytes_t value = { params[i].value, params[i].value_len };
            if (params[i].type == MOQ_SETUP_PARAM_PATH) {
                out->has_path = true; out->path = value;
            } else if (params[i].type == MOQ_SETUP_PARAM_AUTHORITY) {
                out->has_authority = true; out->authority = value;
            }
        }
        return true;
    }
    if (a->kind != MOQ_ACTION_OPEN_UNI_CONTROL) return false;
    moq_buf_reader_init(&r, a->u.open_uni_control.data, a->u.open_uni_control.len);
    moq_d18_setup_opts_t opts;
    if (moq_d18_decode_envelope(&r, &env) != MOQ_OK ||
        moq_d18_decode_setup_opts(env.payload, env.payload_len, &opts, sizeof(opts)) != MOQ_OK)
        return false;
    out->has_path = opts.has_path; out->path = opts.path;
    out->has_authority = opts.has_authority; out->authority = opts.authority;
    return true;
}

/* A client setup naming the given target (NULL: option absent), as the
 * profile frames it. */
static size_t encode_client_target(moq_version_t v, const char *path,
                                   const char *authority, uint8_t *buf, size_t cap)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    if (v == MOQ_VERSION_DRAFT_16) {
        moq_kvp_entry_t params[2];
        size_t n = 0;
        if (path)
            params[n++] = (moq_kvp_entry_t){ .type = MOQ_SETUP_PARAM_PATH,
                .value = (const uint8_t *)path, .value_len = strlen(path) };
        if (authority)
            params[n++] = (moq_kvp_entry_t){ .type = MOQ_SETUP_PARAM_AUTHORITY,
                .value = (const uint8_t *)authority, .value_len = strlen(authority) };
        if (moq_d16_encode_client_setup(&w, params, n) != MOQ_OK) return 0;
    } else {
        moq_d18_setup_opts_t opts;
        moq_d18_setup_opts_init(&opts);
        if (path) { opts.has_path = true; opts.path = bytes(path); }
        if (authority) { opts.has_authority = true; opts.authority = bytes(authority); }
        if (moq_d18_encode_setup_opts(&w, &opts, sizeof(opts)) != MOQ_OK) return 0;
    }
    return moq_buf_writer_offset(&w);
}

static void optional_client(moq_version_t v)
{
    /* Never bound: the session starts as it always has and names no target. */
    fixture_t f;
    if (!create(&f, MOQ_PERSPECTIVE_CLIENT, v)) { ++failures; return; }
    CHECK("zero requirements", moq_session_binding_requirements(f.s) == 0);
    CHECK("zero requirements by version", moq_version_binding_requirements(v) == 0);
    CHECK("unbound start", moq_session_start(f.s, 0) == MOQ_OK);
    moq_action_t a;
    setup_target_t t;
    CHECK("unbound setup", moq_session_poll_actions(f.s, &a, 1) == 1 &&
        decode_client_target(v, &a, &t));
    CHECK("unbound setup names no target", !t.has_path && !t.has_authority);
    destroy(&f);

    /* Bound natively over QUIC: PATH is the moqt URI's path-abempty with "?"
     * and the query, AUTHORITY its authority, both exactly as given. The
     * profile requires no facts, so none need be known. */
    if (!create(&f, MOQ_PERSPECTIVE_CLIENT, v)) { ++failures; return; }
    f.s->send_auth_token_cache_size = false;
    moq_transport_binding_t b;
    moq_transport_binding_init(&b);
    b.kind = MOQ_BINDING_RAW_QUIC;
    b.target_present = MOQ_BINDING_TARGET_COMPLETE;
    b.authority = bytes("127.0.0.1:31000");
    b.path_query = bytes("/moq?interop=1");
    CHECK("optional bind without facts", moq_session_bind_transport(f.s, &b, sizeof(b)) == MOQ_OK);
    refused(&f, &b, sizeof(b), MOQ_ERR_WRONG_STATE, "optional bind is single-use");
    CHECK("bound start", moq_session_start(f.s, 0) == MOQ_OK);
    CHECK("bound setup", moq_session_poll_actions(f.s, &a, 1) == 1 &&
        decode_client_target(v, &a, &t));
    CHECK("bound setup PATH", t.has_path && same(t.path, "/moq?interop=1"));
    CHECK("bound setup AUTHORITY", t.has_authority && same(t.authority, "127.0.0.1:31000"));
    /* The whole message, against the drafts' framing: draft-16 CLIENT_SETUP
     * 0x20, 16-bit Length 34, two parameters, PATH (delta 1) then AUTHORITY
     * (delta 4); draft-18 SETUP 0x2F00 (vi64 af 00), Length 33, the same two
     * options without a count. */
    static const uint8_t d16_wire[] = {
        0x20, 0x00, 0x22, 0x02,
        0x01, 0x0e, '/','m','o','q','?','i','n','t','e','r','o','p','=','1',
        0x04, 0x0f, '1','2','7','.','0','.','0','.','1',':','3','1','0','0','0',
    };
    static const uint8_t d18_wire[] = {
        0xaf, 0x00, 0x00, 0x21,
        0x01, 0x0e, '/','m','o','q','?','i','n','t','e','r','o','p','=','1',
        0x04, 0x0f, '1','2','7','.','0','.','0','.','1',':','3','1','0','0','0',
    };
    const uint8_t *want = v == MOQ_VERSION_DRAFT_16 ? d16_wire : d18_wire;
    size_t want_len = v == MOQ_VERSION_DRAFT_16 ? sizeof(d16_wire) : sizeof(d18_wire);
    const uint8_t *got = v == MOQ_VERSION_DRAFT_16 ? a.u.send_control.data
                                                   : a.u.open_uni_control.data;
    size_t got_len = v == MOQ_VERSION_DRAFT_16 ? a.u.send_control.len
                                               : a.u.open_uni_control.len;
    CHECK("bound setup wire", got_len == want_len && memcmp(got, want, want_len) == 0);
    destroy(&f);

    /* Inside WebTransport the client never names the target. */
    if (!create(&f, MOQ_PERSPECTIVE_CLIENT, v)) { ++failures; return; }
    b.kind = MOQ_BINDING_WEBTRANSPORT_H3;
    CHECK("webtransport bind", moq_session_bind_transport(f.s, &b, sizeof(b)) == MOQ_OK);
    CHECK("webtransport start", moq_session_start(f.s, 0) == MOQ_OK);
    CHECK("webtransport setup", moq_session_poll_actions(f.s, &a, 1) == 1 &&
        decode_client_target(v, &a, &t));
    CHECK("webtransport setup names no target", !t.has_path && !t.has_authority);
    destroy(&f);

    /* The bind contract is the same as a requiring profile's. */
    if (!create(&f, MOQ_PERSPECTIVE_CLIENT, v)) { ++failures; return; }
    b.kind = MOQ_BINDING_WEBTRANSPORT_H2;
    refused(&f, &b, sizeof(b), MOQ_ERR_UNSUPPORTED, "optional h2 refused");
    b.kind = MOQ_BINDING_RAW_QUIC;
    b.target_present = MOQ_BINDING_AUTHORITY_PRESENT;
    refused(&f, &b, sizeof(b), MOQ_ERR_INVAL, "optional incomplete target");
    b.target_present = MOQ_BINDING_TARGET_COMPLETE;
    b.path_query = bytes("/a#frag");
    refused(&f, &b, sizeof(b), MOQ_ERR_INVAL, "optional target syntax");
    destroy(&f);
}

/* A server's setup handling of a client's target, judged after the client's
 * setup is fed: 0 means established. */
static uint64_t serve_target(moq_version_t v, const moq_transport_binding_t *b,
                             const char *path, const char *authority)
{
    fixture_t f;
    if (!create(&f, MOQ_PERSPECTIVE_SERVER, v)) { ++failures; return UINT64_MAX; }
    if (b) CHECK("server bind", moq_session_bind_transport(f.s, b, sizeof(*b)) == MOQ_OK);
    if (v != MOQ_VERSION_DRAFT_16) {
        CHECK("server start", moq_session_start(f.s, 0) == MOQ_OK);
        drain(f.s);
    }
    uint8_t wire[256];
    size_t n = encode_client_target(v, path, authority, wire, sizeof(wire));
    CHECK("client setup fixture", n > 0);
    CHECK("client setup feed", moq_session_on_control_bytes(f.s, wire, n, 1) == MOQ_OK);
    uint64_t code = f.s->state == MOQ_SESS_ESTABLISHED ? 0 : close_code(f.s);
    destroy(&f);
    return code;
}

static void optional_server(moq_version_t v)
{
    /* A client's own bound setup reaches a raw-QUIC server's URI policy
     * with the exact target. */
    seen_target_t seen;
    memset(&seen, 0, sizeof(seen));
    fixture_t client, server;
    if (!create(&client, MOQ_PERSPECTIVE_CLIENT, v)) { ++failures; return; }
    if (!create(&server, MOQ_PERSPECTIVE_SERVER, v)) { ++failures; destroy(&client); return; }
    moq_transport_binding_t cb;
    moq_transport_binding_init(&cb);
    cb.kind = MOQ_BINDING_RAW_QUIC;
    cb.target_present = MOQ_BINDING_TARGET_COMPLETE;
    cb.authority = bytes("[::1]:4443");
    cb.path_query = bytes("/relay/live?room=a%20b");
    moq_transport_binding_t sb;
    moq_transport_binding_init(&sb);
    sb.kind = MOQ_BINDING_RAW_QUIC;
    sb.accept_uri = record_uri;
    sb.accept_uri_ctx = &seen;
    CHECK("pair client bind", moq_session_bind_transport(client.s, &cb, sizeof(cb)) == MOQ_OK);
    CHECK("pair server bind", moq_session_bind_transport(server.s, &sb, sizeof(sb)) == MOQ_OK);
    CHECK("pair client start", moq_session_start(client.s, 0) == MOQ_OK);
    if (v != MOQ_VERSION_DRAFT_16) {
        CHECK("pair server start", moq_session_start(server.s, 0) == MOQ_OK);
        drain(server.s);
    }
    moq_action_t a;
    CHECK("pair client setup", moq_session_poll_actions(client.s, &a, 1) == 1);
    const uint8_t *data = v == MOQ_VERSION_DRAFT_16 ? a.u.send_control.data
                                                    : a.u.open_uni_control.data;
    size_t len = v == MOQ_VERSION_DRAFT_16 ? a.u.send_control.len
                                           : a.u.open_uni_control.len;
    CHECK("pair server feed", moq_session_on_control_bytes(server.s, data, len, 1) == MOQ_OK);
    CHECK("pair established", server.s->state == MOQ_SESS_ESTABLISHED);
    CHECK("pair policy asked once", seen.calls == 1);
    CHECK("pair policy presence", seen.present == MOQ_BINDING_TARGET_COMPLETE);
    CHECK("pair policy authority", seen.authority_len == 10 &&
        memcmp(seen.authority, "[::1]:4443", 10) == 0);
    CHECK("pair policy path", seen.path_len == 22 &&
        memcmp(seen.path, "/relay/live?room=a%20b", 22) == 0);
    destroy(&client);
    destroy(&server);

    moq_transport_binding_t raw;
    moq_transport_binding_init(&raw);
    raw.kind = MOQ_BINDING_RAW_QUIC;
    raw.accept_uri = record_uri;
    raw.accept_uri_ctx = &seen;
    seen.verdict = MOQ_URI_INVALID_AUTHORITY;
    CHECK("policy refuses authority", serve_target(v, &raw, "/p", "h") == 0x19);
    seen.verdict = MOQ_URI_INVALID_PATH;
    CHECK("policy refuses path", serve_target(v, &raw, "/p", "h") == 0x8);
    seen.verdict = MOQ_URI_ACCEPT;
    seen.calls = 0;
    CHECK("policy asked without a target", serve_target(v, &raw, NULL, NULL) == 0 &&
        seen.calls == 1 && seen.present == 0);

    /* Inside WebTransport either option is INVALID_*. */
    moq_transport_binding_t wt;
    moq_transport_binding_init(&wt);
    wt.kind = MOQ_BINDING_WEBTRANSPORT_H3;
    wt.target_present = MOQ_BINDING_TARGET_COMPLETE;
    wt.authority = bytes("h");
    wt.path_query = bytes("/moq");
    CHECK("webtransport PATH", serve_target(v, &wt, "/moq", NULL) == 0x8);
    CHECK("webtransport AUTHORITY", serve_target(v, &wt, NULL, "h") == 0x19);
    CHECK("webtransport no target", serve_target(v, &wt, NULL, NULL) == 0);

    /* Never bound: syntax still decides, and a well-formed target is
     * accepted as before. */
    CHECK("unbound malformed PATH", serve_target(v, NULL, "/bad%", "h") == 0x9);
    CHECK("unbound fragment PATH", serve_target(v, NULL, "/a#b", "h") == 0x9);
    CHECK("unbound malformed AUTHORITY", serve_target(v, NULL, "/p", "bad%") == 0x1a);
    CHECK("unbound empty host AUTHORITY", serve_target(v, NULL, NULL, ":80") == 0x1a);
    CHECK("unbound well-formed target", serve_target(v, NULL, "/p?q", "h:1") == 0);
    CHECK("unbound empty PATH", serve_target(v, NULL, "", "h") == 0);
}

/* The target options a server's own setup carries: draft-16 SERVER_SETUP
 * on the bidirectional channel, draft-18 SETUP on its unidirectional one. */
static bool decode_server_target(moq_version_t v, const moq_action_t *a,
                                 setup_target_t *out)
{
    memset(out, 0, sizeof(*out));
    moq_buf_reader_t r;
    moq_control_envelope_t env;
    if (v == MOQ_VERSION_DRAFT_16) {
        if (a->kind != MOQ_ACTION_SEND_CONTROL) return false;
        moq_buf_reader_init(&r, a->u.send_control.data, a->u.send_control.len);
        moq_kvp_entry_t params[8];
        moq_d16_setup_t setup = { .params = params, .params_count = 0,
                                  .params_cap = 8 };
        if (moq_control_decode_envelope(&r, &env) != MOQ_OK ||
            env.msg_type != MOQ_D16_SERVER_SETUP ||
            moq_d16_decode_server_setup(env.payload, env.payload_len, &setup) != MOQ_OK)
            return false;
        for (size_t i = 0; i < setup.params_count; i++) {
            out->has_path |= params[i].type == MOQ_SETUP_PARAM_PATH;
            out->has_authority |= params[i].type == MOQ_SETUP_PARAM_AUTHORITY;
        }
        return true;
    }
    return decode_client_target(v, a, out);
}

/* A server never names a target in its own setup, however it is bound. */
static void optional_server_output(moq_version_t v)
{
    for (unsigned mode = 0; mode < 3; ++mode) {
        fixture_t f;
        if (!create(&f, MOQ_PERSPECTIVE_SERVER, v)) { ++failures; return; }
        moq_transport_binding_t b;
        moq_transport_binding_init(&b);
        if (mode == 1) {
            b.kind = MOQ_BINDING_RAW_QUIC;
            b.accept_uri = accept_uri;
            b.accept_uri_ctx = &f;
        } else if (mode == 2) {
            b.kind = MOQ_BINDING_WEBTRANSPORT_H3;
            b.target_present = MOQ_BINDING_TARGET_COMPLETE;
            b.authority = bytes("h");
            b.path_query = bytes("/moq");
        }
        if (mode)
            CHECK("server output bind", moq_session_bind_transport(f.s, &b, sizeof(b)) == MOQ_OK);
        moq_action_t a;
        if (v == MOQ_VERSION_DRAFT_16) {
            /* Draft-16 answers the client's setup. */
            uint8_t wire[64];
            size_t n = encode_client_target(v, mode == 2 ? NULL : "/p",
                                            mode == 2 ? NULL : "h", wire, sizeof(wire));
            CHECK("server output feed", n > 0 &&
                moq_session_on_control_bytes(f.s, wire, n, 1) == MOQ_OK);
        } else {
            CHECK("server output start", moq_session_start(f.s, 0) == MOQ_OK);
        }
        setup_target_t t = {0};
        CHECK("server output setup", moq_session_poll_actions(f.s, &a, 1) == 1 &&
            decode_server_target(v, &a, &t));
        CHECK("server setup names no target", !t.has_path && !t.has_authority);
        destroy(&f);
    }
}

/* A client closes on either option from its server, exactly INVALID_PATH
 * or INVALID_AUTHORITY, however it is bound. */
static void optional_client_refuses_server_target(moq_version_t v)
{
    for (unsigned mode = 0; mode < 3; ++mode) {
        for (unsigned which = 0; which < 2; ++which) {
            fixture_t f;
            if (!create(&f, MOQ_PERSPECTIVE_CLIENT, v)) { ++failures; return; }
            if (mode) {
                moq_transport_binding_t b = binding(&f, mode == 1 ? MOQ_BINDING_RAW_QUIC
                                                                 : MOQ_BINDING_WEBTRANSPORT_H3);
                CHECK("client bind", moq_session_bind_transport(f.s, &b, sizeof(b)) == MOQ_OK);
            }
            CHECK("client start", moq_session_start(f.s, 0) == MOQ_OK);
            drain(f.s);
            uint8_t wire[64];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, wire, sizeof(wire));
            moq_result_t rc;
            if (v == MOQ_VERSION_DRAFT_16) {
                moq_kvp_entry_t param = {
                    .type = which ? MOQ_SETUP_PARAM_AUTHORITY : MOQ_SETUP_PARAM_PATH,
                    .value = (const uint8_t *)(which ? "host" : "/p"),
                    .value_len = which ? 4 : 2 };
                rc = moq_d16_encode_server_setup(&w, &param, 1);
            } else {
                moq_d18_setup_opts_t opts;
                moq_d18_setup_opts_init(&opts);
                if (which) { opts.has_authority = true; opts.authority = bytes("host"); }
                else { opts.has_path = true; opts.path = bytes("/p"); }
                rc = moq_d18_encode_setup_opts(&w, &opts, sizeof(opts));
            }
            CHECK("server target fixture", rc == MOQ_OK);
            CHECK("server target feed", moq_session_on_control_bytes(f.s, wire,
                moq_buf_writer_offset(&w), 1) == MOQ_OK);
            CHECK("server target closes exactly", f.s->state == MOQ_SESS_CLOSED &&
                close_code(f.s) == (which ? 0x19 : 0x8));
            destroy(&f);
        }
    }
}

static void test_optional_binding(void)
{
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18};
    for (size_t i = 0; i < 2; ++i) {
        optional_client(versions[i]);
        optional_server(versions[i]);
        optional_server_output(versions[i]);
        optional_client_refuses_server_target(versions[i]);
    }
}

static void test_policy_full_body_and_retry(void)
{
    /* Valid URI and REGISTER followed by a delta without its value. */
    const uint8_t token_setup[] = {0xaf,0,0,9, 1,0,2,4,1,9,0,'x',0};
    fixture_t f;
    if (!create(&f, MOQ_PERSPECTIVE_SERVER, 0)) { ++failures; return; }
    moq_transport_binding_t b = binding(&f, MOQ_BINDING_RAW_QUIC);
    CHECK("full-body bind", moq_session_bind_transport(f.s, &b, sizeof(b)) == MOQ_OK);
    CHECK("full-body start", moq_session_start(f.s, 0) == MOQ_OK); drain(f.s);
    uint64_t allocations = f.as.alloc_count;
    CHECK("malformed tail feed", moq_session_on_control_bytes(f.s,
        token_setup, sizeof(token_setup), 1) == MOQ_OK);
    CHECK("full body before URI policy", f.policy_calls == 0);
    CHECK("full body before token allocation", f.as.alloc_count == allocations);
    CHECK("malformed tail no auth", f.s->peer_token_cache.used_bytes == 0 &&
        f.s->state == MOQ_SESS_CLOSED && close_code(f.s) == 3);
    destroy(&f);

    if (!create(&f, MOQ_PERSPECTIVE_SERVER, 0)) { ++failures; return; }
    b = binding(&f, MOQ_BINDING_RAW_QUIC);
    CHECK("retry bind", moq_session_bind_transport(f.s, &b, sizeof(b)) == MOQ_OK);
    CHECK("retry start", moq_session_start(f.s, 0) == MOQ_OK); drain(f.s);
    const uint8_t valid[] = {0xaf,0,0,8, 1,0,2,4,1,9,0,'x'};
    moq_event_t occupied;
    memset(&occupied, 0, sizeof(occupied));
    occupied.kind = MOQ_EVENT_SETUP_COMPLETE;
    while (push_event(f.s, &occupied) == MOQ_OK) { }
    size_t consumed = 90;
    allocations = f.as.alloc_count;
    CHECK("policy event reservation", f.s->profile->process_control_data(f.s,
        valid, sizeof(valid), &consumed) == MOQ_ERR_WOULD_BLOCK);
    CHECK("no policy or allocation without event", consumed == 0 &&
        f.policy_calls == 0 && f.as.alloc_count == allocations);
    moq_event_t event;
    while (moq_session_poll_events(f.s, &event, 1)) moq_event_cleanup(&event);
    size_t scratch = f.s->event_scratch_len;
    f.s->event_scratch_len = f.s->event_scratch_cap;
    int64_t balance = f.as.balance;
    CHECK("policy scratch retry", f.s->profile->process_control_data(f.s,
        valid, sizeof(valid), &consumed) == MOQ_ERR_WOULD_BLOCK);
    CHECK("retry aborts cache staging", consumed == 0 && f.policy_calls == 1 &&
        f.s->state != MOQ_SESS_ESTABLISHED && f.s->peer_token_cache.used_bytes == 0 &&
        f.as.balance == balance);
    f.s->event_scratch_len = scratch;
    CHECK("policy successful retry", f.s->profile->process_control_data(f.s,
        valid, sizeof(valid), &consumed) == MOQ_OK);
    CHECK("policy retried before commit", consumed == sizeof(valid) &&
        f.policy_calls == 2 && f.present == MOQ_BINDING_PATH_PRESENT &&
        f.s->state == MOQ_SESS_ESTABLISHED && f.s->peer_token_cache.used_bytes == 17);
    destroy(&f);
}

/* The SETUP body is bounded by its 16-bit Length: the largest target that
 * fits is accepted and one byte more is a permanent refusal. The fixed part
 * of the body is measured from an empty-path SETUP rather than assumed. */
static void test_output_bounds(void)
{
    size_t fixed = 0;
    {
        fixture_t f;
        if (!create(&f, MOQ_PERSPECTIVE_CLIENT, 0)) { ++failures; return; }
        moq_transport_binding_t b = binding(&f, MOQ_BINDING_RAW_QUIC);
        b.authority = bytes("x"); b.path_query = (moq_bytes_t){NULL, 0};
        CHECK("probe bind", moq_session_bind_transport(f.s, &b, sizeof(b)) == MOQ_OK);
        CHECK("probe start", moq_session_start(f.s, 0) == MOQ_OK);
        moq_action_t a;
        if (moq_session_poll_actions(f.s, &a, 1) == 1 && a.kind == MOQ_ACTION_OPEN_UNI_CONTROL)
            fixed = a.u.open_uni_control.len - 4 - 1;   /* body minus the empty PATH length byte */
        CHECK("probe measured", fixed > 0);
        destroy(&f);
    }
    size_t path_len = 0;
    while (fixed + moq_vi64_len(path_len + 1) + path_len + 1 <= UINT16_MAX) ++path_len;
    for (unsigned overflow = 0; overflow < 2; ++overflow) {
        fixture_t f;
        if (!create(&f, MOQ_PERSPECTIVE_CLIENT, 0)) { ++failures; return; }
        moq_transport_binding_t b = binding(&f, MOQ_BINDING_RAW_QUIC);
        size_t n = path_len + overflow;
        uint8_t *path = malloc(n);
        if (!path) { ++failures; destroy(&f); return; }
        memset(path, 'a', n); path[0] = '/';
        b.authority = bytes("x"); b.path_query = (moq_bytes_t){path, n};
        CHECK("bounded large target", moq_session_bind_transport(f.s, &b, sizeof(b)) == MOQ_OK);
        free(path);
        uint64_t allocations = f.as.alloc_count;
        moq_result_t rc = moq_session_start(f.s, 1);
        CHECK("SETUP start uses no allocation", f.as.alloc_count == allocations);
        if (overflow) {
            CHECK("body length overflow", rc == MOQ_ERR_BUFFER && f.s->state == MOQ_SESS_IDLE &&
                f.s->send_len == 0 && f.s->action_head == f.s->action_tail);
        } else {
            moq_action_t a;
            CHECK("maximum SETUP start", rc == MOQ_OK);
            size_t got = moq_session_poll_actions(f.s, &a, 1);
            CHECK("maximum SETUP action", got == 1 && a.kind == MOQ_ACTION_OPEN_UNI_CONTROL);
            if (got && a.kind == MOQ_ACTION_OPEN_UNI_CONTROL) {
                const uint8_t header[] = {0xaf,0,0xff,0xff};
                CHECK("maximum SETUP wire length", a.u.open_uni_control.len == 65539 &&
                    memcmp(a.u.open_uni_control.data, header, sizeof(header)) == 0);
            }
        }
        destroy(&f);
    }
    fixture_t f;
    if (!create(&f, MOQ_PERSPECTIVE_CLIENT, 0)) { ++failures; return; }
    moq_transport_binding_t b = binding(&f, MOQ_BINDING_RAW_QUIC);
    b.path_query = (moq_bytes_t){NULL, 0};
    CHECK("explicit empty path bind", moq_session_bind_transport(f.s, &b, sizeof(b)) == MOQ_OK);
    size_t cap = f.s->send_cap;
    f.s->send_cap = 1;
    CHECK("permanent send buffer refusal", moq_session_start(f.s, 1) == MOQ_ERR_BUFFER);
    CHECK("permanent refusal transaction", f.s->state == MOQ_SESS_IDLE && f.s->send_len == 0);
    f.s->send_cap = cap;
    CHECK("empty path retry start", moq_session_start(f.s, 2) == MOQ_OK);
    moq_action_t a;
    CHECK("empty path option exists", moq_session_poll_actions(f.s, &a, 1) == 1 &&
        a.u.open_uni_control.data[4] == 1 && a.u.open_uni_control.data[5] == 0);
    destroy(&f);
}

int main(void)
{
    test_sizes_facts(); test_uri_syntax(); test_unbound_routes();
    test_roles_and_copy(); test_peer_policy(); test_optional_binding();
    test_policy_full_body_and_retry(); test_output_bounds();
    if (failures) { fprintf(stderr, "transport binding: %d failures\n", failures); return 1; }
    printf("PASS: transport_binding\n");
    return 0;
}
