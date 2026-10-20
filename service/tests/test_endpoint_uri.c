/*
 * Endpoint URI handling: the MOQT URI grammar and its managed-client subset,
 * the config tail layout the URI fields append to, the connection target the
 * endpoint derives in both URI modes, the preferred-version knob on AUTO
 * offers, the URI-policy shape, owned target clones, and the size bound.
 */
#include "endpoint_internal.h"
#include "endpoint_offer_expect.h"
#include "../../core/src/base/uri_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* The complete layout that precedes the URI fields, not a prefix inferred
 * from the current header. */
typedef struct endpoint_pre_uri {
    uint32_t struct_size;
    moq_bytes_t url;
    moq_transport_protocol_t protocol;
    moq_transport_backend_t backend;
    moq_version_offer_t versions;
    moq_bytes_t sni;
    moq_bytes_t ca_file;
    bool insecure_skip_verify;
    moq_bytes_t wt_path;
    moq_perspective_t perspective;
    const moq_alloc_t *alloc;
    uint32_t wt_profile;
    uint64_t handshake_timeout_us;
    const moq_auth_token_t *setup_auth_tokens;
    size_t setup_auth_token_count;
    moq_tls_certificate_verify_fn verify_certificate;
    void *verify_certificate_ctx;
} endpoint_pre_uri_t;

#define OFFSET_SAME(f) _Static_assert(offsetof(endpoint_pre_uri_t, f) == \
    offsetof(moq_endpoint_cfg_t, f), "old offset " #f)
OFFSET_SAME(struct_size); OFFSET_SAME(url); OFFSET_SAME(protocol);
OFFSET_SAME(backend); OFFSET_SAME(versions); OFFSET_SAME(sni);
OFFSET_SAME(ca_file); OFFSET_SAME(insecure_skip_verify); OFFSET_SAME(wt_path);
OFFSET_SAME(perspective); OFFSET_SAME(alloc); OFFSET_SAME(wt_profile);
OFFSET_SAME(handshake_timeout_us); OFFSET_SAME(setup_auth_tokens);
OFFSET_SAME(setup_auth_token_count); OFFSET_SAME(verify_certificate);
OFFSET_SAME(verify_certificate_ctx);
_Static_assert(offsetof(moq_endpoint_cfg_t, uri_mode) >= sizeof(endpoint_pre_uri_t),
    "uri mode cannot occupy old padding");
_Static_assert(sizeof(((moq_endpoint_cfg_t *)0)->uri_mode) == 4, "fixed width mode");
_Static_assert(offsetof(moq_endpoint_cfg_t, preferred_version) >
    offsetof(moq_endpoint_cfg_t, uri_mode), "preferred version follows mode");
_Static_assert(offsetof(moq_endpoint_cfg_t, accept_uri) >
    offsetof(moq_endpoint_cfg_t, preferred_version), "policy follows preference");
_Static_assert(offsetof(moq_endpoint_cfg_t, accept_uri_ctx) >
    offsetof(moq_endpoint_cfg_t, accept_uri), "policy context follows policy");
_Static_assert(MOQ_ENDPOINT_CFG_V0_SIZE == offsetof(endpoint_pre_uri_t, wt_profile),
    "unchanged V0");
#if UINTPTR_MAX == UINT64_MAX
_Static_assert(sizeof(endpoint_pre_uri_t) == 176, "LP64 old full size");
_Static_assert(offsetof(moq_endpoint_cfg_t, uri_mode) == 176, "LP64 mode");
_Static_assert(offsetof(moq_endpoint_cfg_t, preferred_version) == 180, "LP64 preference");
_Static_assert(offsetof(moq_endpoint_cfg_t, accept_uri) == 184, "LP64 policy");
_Static_assert(offsetof(moq_endpoint_cfg_t, accept_uri_ctx) == 192, "LP64 policy ctx");
_Static_assert(offsetof(moq_endpoint_cfg_t, send_queue_cap_bytes) == 200,
               "LP64 URI-era full size");
_Static_assert(sizeof(moq_endpoint_cfg_t) == 208, "LP64 new full size");
#endif

static unsigned failures;
#define CHECK(row, expr) do { if (!(expr)) { \
    fprintf(stderr, "FAIL[%s] line %d: %s\n", row, __LINE__, #expr); \
    ++failures; } } while (0)

static moq_bytes_t B(const char *s)
{ return (moq_bytes_t){(const uint8_t *)s, strlen(s)}; }
static bool equal(moq_bytes_t b, const char *s)
{ return b.len == strlen(s) && (!b.len || (b.data && !memcmp(b.data, s, b.len))); }
static moq_endpoint_cfg_t config(const char *s)
{
    moq_endpoint_cfg_t c;
    moq_endpoint_cfg_init_sized(&c, sizeof(c));
    c.url = B(s);
    c.uri_mode = MOQ_ENDPOINT_URI_STANDARD;
    return c;
}

/* An absent or partial URI mode retains the original caller's defaults.
 * Standards URI handling requires an explicit, complete field. */
static void layout(void)
{
    moq_endpoint_cfg_t c = config("moqt://host");
    c.uri_mode = MOQ_ENDPOINT_URI_STANDARD;
    moq_endpoint_resolved_t r;
    size_t end = offsetof(moq_endpoint_cfg_t, uri_mode) + sizeof(c.uri_mode);
    c.struct_size = (uint32_t)(end - 1);
    CHECK("partial.full.buffer", moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK && r.url.port == 4433);
    c.struct_size = sizeof(endpoint_pre_uri_t);
    CHECK("old.full.buffer", moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK && r.url.port == 4433);
    c.struct_size = sizeof(c);
    const size_t sizes[] = { MOQ_ENDPOINT_CFG_V0_SIZE,
        sizeof(endpoint_pre_uri_t), end - 3, end - 2, end - 1 };
    for (size_t i = 0; i < sizeof(sizes)/sizeof(sizes[0]); ++i) {
        unsigned char *p = malloc(sizes[i]);
        CHECK("short.alloc", p != NULL);
        if (!p) continue;
        memcpy(p, &c, sizes[i]);
        uint32_t stamp = (uint32_t)sizes[i];
        memcpy(p, &stamp, sizeof(stamp));
        /* Existing padding is poisoned independently of the field bytes. */
        size_t old_end = offsetof(endpoint_pre_uri_t, verify_certificate_ctx) +
                         sizeof(void *);
        if (sizes[i] >= sizeof(endpoint_pre_uri_t) && old_end < sizeof(endpoint_pre_uri_t))
            memset(p + old_end, 0xff, sizeof(endpoint_pre_uri_t) - old_end);
        CHECK("short.legacy", moq_endpoint_resolve_cfg((const void *)p, &r) == MOQ_OK);
        CHECK("short.port", r.url.port == 4433);
        CHECK("short.mode", r.uri_mode == MOQ_ENDPOINT_URI_LEGACY);
        free(p);
    }
    c.struct_size = (uint32_t)end;
    c.uri_mode = MOQ_ENDPOINT_URI_LEGACY;
    CHECK("complete.mode", moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK);
    CHECK("complete.legacy", r.uri_mode == MOQ_ENDPOINT_URI_LEGACY);
    CHECK("complete.port", r.url.port == 4433);
    c.uri_mode = MOQ_ENDPOINT_URI_STANDARD;
    CHECK("complete.standard", moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK && r.url.port == 443 &&
        r.uri_mode == MOQ_ENDPOINT_URI_STANDARD);
    c.uri_mode = UINT32_MAX;
    CHECK("unknown.mode", moq_endpoint_resolve_cfg(&c, &r) == MOQ_ERR_INVAL);
    c.uri_mode = 2;
    CHECK("unknown.mode.next", moq_endpoint_resolve_cfg(&c, &r) == MOQ_ERR_INVAL);

    struct { moq_endpoint_cfg_t cfg; unsigned char future[17]; } f;
    memset(&f, 0xa5, sizeof(f));
    moq_endpoint_cfg_init_sized(&f.cfg, sizeof(f));
    CHECK("init.stamp", f.cfg.struct_size == sizeof(f.cfg));
    CHECK("init.legacy", f.cfg.uri_mode == MOQ_ENDPOINT_URI_LEGACY);
    CHECK("init.preference", f.cfg.preferred_version == 0);
    CHECK("init.policy", !f.cfg.accept_uri && !f.cfg.accept_uri_ctx);
    f.cfg.url = B("https://host");
    CHECK("init.default.wt.resolve", moq_endpoint_resolve_cfg(&f.cfg, &r) == MOQ_OK);
    CHECK("init.default.wt.path", equal(r.wt_path, "/moq"));
    for (size_t i = 0; i < sizeof(f.future); ++i)
        CHECK("init.future.canary", f.future[i] == 0xa5);
    f.cfg = config("moqt://host");
    f.cfg.struct_size = sizeof(f);
    CHECK("future.resolve", moq_endpoint_resolve_cfg(&f.cfg, &r) == MOQ_OK);
    CHECK("future.port", r.url.port == 443);
    memset(&c, 0xa5, sizeof(c));
    moq_endpoint_cfg_init(&c);
    CHECK("init.pointer.floor", c.struct_size == MOQ_ENDPOINT_CFG_V0_SIZE);
    for (size_t i = MOQ_ENDPOINT_CFG_V0_SIZE; i < sizeof(c); ++i)
        CHECK("init.pointer.tail", ((unsigned char *)&c)[i] == 0xa5);
    for (size_t n = 0; n <= sizeof(c); ++n) {
        memset(&c, 0xa5, sizeof(c));
        moq_endpoint_cfg_init_sized(&c, n);
        for (size_t i = n; i < sizeof(c); ++i)
            CHECK("init.sized.canary", ((unsigned char *)&c)[i] == 0xa5);
    }
}

static void identities(void)
{
    const struct {
        const char *input, *authority, *host, *target, *http;
        uint16_t port;
        bool query;
    } rows[] = {
        {"moqt://Example.com", "Example.com", "Example.com", "", "/", 443, false},
        {"moqt://Example.com?", "Example.com", "Example.com", "?", "/?", 443, true},
        {"moqt://Example.com?x=%2f", "Example.com", "Example.com", "?x=%2f", "/?x=%2f", 443, true},
        {"MoQt://Example.com.:00443/a/../%2Fb?q=%23?", "Example.com.:00443", "Example.com.", "/a/../%2Fb?q=%23?", "/a/../%2Fb?q=%23?", 443, true},
        {"https://[2001:db8::1]:8443/a%2Fb?q=%23", "[2001:db8::1]:8443", "2001:db8::1", "/a%2Fb?q=%23", "/a%2Fb?q=%23", 8443, true},
        {"HTTPS://127.0.0.1:1/", "127.0.0.1:1", "127.0.0.1", "/", "/", 1, false},
        {"moqt://[::ffff:192.0.2.1]:65535/!$&'()*+,;=:@", "[::ffff:192.0.2.1]:65535", "::ffff:192.0.2.1", "/!$&'()*+,;=:@", "/!$&'()*+,;=:@", 65535, false},
        {"moqt://h:00000000000000000000000000000000000443", "h:00000000000000000000000000000000000443", "h", "", "/", 443, false},
    };
    for (size_t i = 0; i < sizeof(rows)/sizeof(rows[0]); ++i) {
        moq_endpoint_uri_t p;
        moq_result_t rc = moq_endpoint_uri_parse(B(rows[i].input), &p);
        CHECK(rows[i].input, rc == MOQ_OK);
        if (rc != MOQ_OK) continue;
        CHECK("authority.original", equal(p.authority, rows[i].authority));
        CHECK("host.only", equal(p.url.host, rows[i].host));
        CHECK("raw.target", equal(p.path_query, rows[i].target));
        CHECK("numeric.port", p.url.port == rows[i].port);
        CHECK("query.presence", p.query_present == rows[i].query);
        moq_endpoint_cfg_t c = config(rows[i].input);
        c.sni = B("tls.example");
        c.protocol = MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT;
        moq_endpoint_resolved_t r;
        CHECK("resolve.standard", moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK);
        CHECK("resolve.authority", equal(r.uri.authority, rows[i].authority));
        CHECK("resolve.sni", equal(r.sni, "tls.example"));
        CHECK("resolve.target.present", r.target.target_present == MOQ_BINDING_TARGET_COMPLETE);
        CHECK("resolve.target.authority", equal(r.target.authority, rows[i].authority));
        CHECK("resolve.target.path", equal(r.target.path_query, rows[i].target));
        CHECK("resolve.target.no.policy", !r.target.accept_uri && !r.target.accept_uri_ctx);
        CHECK("resolve.wt.raw.path", equal(r.wt_path, rows[i].target));
        moq_endpoint_uri_owned_t owned = {0};
        rc = moq_endpoint_uri_clone(moq_alloc_default(), &p, true, &owned);
        CHECK("clone.http", rc == MOQ_OK);
        if (rc == MOQ_OK) {
            CHECK("http.target", owned.wt_path && !strcmp(owned.wt_path, rows[i].http));
            CHECK("clone.authority", equal(owned.authority, rows[i].authority));
            CHECK("clone.raw", equal(owned.path_query, rows[i].target));
        }
        moq_endpoint_uri_cleanup(moq_alloc_default(), &owned);
        c.protocol = MOQ_TRANSPORT_PROTOCOL_RAW_QUIC;
        CHECK("override.raw", moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK);
        CHECK("override.protocol", r.protocol == MOQ_TRANSPORT_PROTOCOL_RAW_QUIC);
        CHECK("override.raw.target", equal(r.uri.path_query, rows[i].target));
        CHECK("override.raw.binding.target", equal(r.target.path_query, rows[i].target));
    }
    moq_endpoint_cfg_t c = config("HTTPS://host");
    moq_endpoint_resolved_t r;
    CHECK("auto.https", moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK &&
        r.protocol == MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT);
    c = config("MoQt://host");
    CHECK("auto.moqt", moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK &&
        r.protocol == MOQ_TRANSPORT_PROTOCOL_RAW_QUIC);
}

static void refusals(void)
{
    const struct { const char *input; moq_result_t want; } rows[] = {
        {"moq://host", MOQ_ERR_UNSUPPORTED}, {"ftp://host/a", MOQ_ERR_INVAL},
        {"ws://host/a", MOQ_ERR_INVAL}, {"mailto:a@host", MOQ_ERR_INVAL},
        {"moq:a@host", MOQ_ERR_UNSUPPORTED}, {"moq:%zz", MOQ_ERR_INVAL},
        {"moqt://user:p@host/a", MOQ_ERR_UNSUPPORTED},
        {"moqt://@host/", MOQ_ERR_UNSUPPORTED},
        {"moqt://[v1.future]/", MOQ_ERR_UNSUPPORTED},
        {"moqt://%65xample.com/", MOQ_ERR_UNSUPPORTED},
        {"moqt://host:/", MOQ_ERR_UNSUPPORTED},
        {"moqt://host:0/", MOQ_ERR_UNSUPPORTED},
        {"moqt://host:65536/", MOQ_ERR_UNSUPPORTED},
        {"moqt://host:18446744073709552059/", MOQ_ERR_UNSUPPORTED},
        {"moqt://host#future:x", MOQ_ERR_UNSUPPORTED},
        {"moqt://host/path?x#future-1:", MOQ_ERR_UNSUPPORTED},
        {"moqt://host#future:/?a:b%23", MOQ_ERR_UNSUPPORTED},
        {"", MOQ_ERR_INVAL}, {"1moqt://host", MOQ_ERR_INVAL},
        {"moqt:/host", MOQ_ERR_INVAL}, {"moqt://", MOQ_ERR_INVAL},
        {"moqt://:443", MOQ_ERR_INVAL}, {"moqt://a@b@host", MOQ_ERR_INVAL},
        {"moqt://[:::1]", MOQ_ERR_INVAL}, {"moqt://[::1]tail", MOQ_ERR_INVAL},
        {"moqt://[]", MOQ_ERR_INVAL}, {"moqt://[1:2:3:4:5:6:7]", MOQ_ERR_INVAL},
        {"moqt://host:abc", MOQ_ERR_INVAL}, {"moqt://host/%", MOQ_ERR_INVAL},
        {"moqt://host/%xy", MOQ_ERR_INVAL}, {"moqt://ho st/", MOQ_ERR_INVAL},
        {"moqt://host/\r\n", MOQ_ERR_INVAL}, {"moqt://host/[bad]", MOQ_ERR_INVAL},
        {"ftp://host/%zz", MOQ_ERR_INVAL},
        {"moqt://user@host/%zz", MOQ_ERR_INVAL},
        {"moqt://host:65536/%zz", MOQ_ERR_INVAL},
        {"moqt://host#", MOQ_ERR_INVAL}, {"moqt://host#:x", MOQ_ERR_INVAL},
        {"moqt://host#Future:x", MOQ_ERR_INVAL}, {"moqt://host#future", MOQ_ERR_INVAL},
        {"moqt://host#fu%74ure:x", MOQ_ERR_INVAL}, {"moqt://host#future:%zz", MOQ_ERR_INVAL},
        {"moqt://host#future:x#y", MOQ_ERR_INVAL},
    };
    for (size_t i = 0; i < sizeof(rows)/sizeof(rows[0]); ++i) {
        moq_endpoint_uri_t p, before;
        memset(&p, 0xa5, sizeof(p)); before = p;
        CHECK(rows[i].input, moq_endpoint_uri_parse(B(rows[i].input), &p) == rows[i].want);
        CHECK("parse.refusal.unchanged", !memcmp(&p, &before, sizeof(p)));
        moq_endpoint_cfg_t c = config(rows[i].input);
        moq_endpoint_resolved_t r;
        CHECK(rows[i].input, moq_endpoint_resolve_cfg(&c, &r) == rows[i].want);
    }
    moq_endpoint_uri_t p;
    CHECK("parse.null", moq_endpoint_uri_parse((moq_bytes_t){NULL, 1}, &p) == MOQ_ERR_INVAL);
    CHECK("parse.null.out", moq_endpoint_uri_parse(B("moqt://host"), NULL) == MOQ_ERR_INVAL);
    static const uint8_t nul[] = "moqt://host/\0suffix";
    CHECK("parse.nul", moq_endpoint_uri_parse((moq_bytes_t){nul, sizeof(nul)-1}, &p) == MOQ_ERR_INVAL);
    moq_endpoint_cfg_t c = config("moqt://host/path");
    c.wt_path = B("/other");
    moq_endpoint_resolved_t r;
    CHECK("override.refused", moq_endpoint_resolve_cfg(&c, &r) == MOQ_ERR_INVAL);
    c.wt_path = (moq_bytes_t){0};
    c.sni = (moq_bytes_t){(const uint8_t *)"a\0b", 3};
    CHECK("sni.nul", moq_endpoint_resolve_cfg(&c, &r) == MOQ_ERR_INVAL);
}

static void authority_policy(void)
{
    const struct { const char *input; moq_result_t want; } rows[] = {
        {"moq://:80/a", MOQ_ERR_UNSUPPORTED},
        {"moq://u@/a", MOQ_ERR_UNSUPPORTED},
        {"moq://u@:80/a", MOQ_ERR_UNSUPPORTED},
        {"moq:///a", MOQ_ERR_UNSUPPORTED},
        {"moq://@host/a", MOQ_ERR_UNSUPPORTED},
        {"moq://:bad/a", MOQ_ERR_INVAL},
        {"moq://u@:bad/a", MOQ_ERR_INVAL},
        {"moq://u%zz@/a", MOQ_ERR_INVAL},
        {"moq://u@/a%zz", MOQ_ERR_INVAL},
        {"moq://u@@/a", MOQ_ERR_INVAL},
        {"moq://@@host/a", MOQ_ERR_INVAL},
        {"x://:80/a", MOQ_ERR_INVAL},
        {"x://host/a", MOQ_ERR_INVAL},
        {"moqt:///a", MOQ_ERR_INVAL},
        {"moqt://:80/a", MOQ_ERR_INVAL},
        {"moqt://u@/a", MOQ_ERR_INVAL},
        {"moqt://u@:80/a", MOQ_ERR_INVAL},
        {"moqt://@/a", MOQ_ERR_INVAL},
        {"moqt://@host/a", MOQ_ERR_UNSUPPORTED},
        {"https:///a", MOQ_ERR_INVAL},
        {"https://:80/a", MOQ_ERR_INVAL},
        {"https://u@/a", MOQ_ERR_INVAL},
        {"https://u@:80/a", MOQ_ERR_INVAL},
        {"https://@/a", MOQ_ERR_INVAL},
        {"https://@host/a", MOQ_ERR_UNSUPPORTED},
    };
    for (size_t i = 0; i < sizeof(rows)/sizeof(rows[0]); ++i) {
        moq_endpoint_uri_t parsed, before;
        memset(&parsed, 0xa5, sizeof(parsed)); before = parsed;
        moq_result_t rc = moq_endpoint_uri_parse(B(rows[i].input), &parsed);
        moq_endpoint_cfg_t cfg = config(rows[i].input);
        moq_endpoint_resolved_t resolved;
        moq_result_t resolved_rc = moq_endpoint_resolve_cfg(&cfg, &resolved);
        CHECK(rows[i].input, rc == rows[i].want && resolved_rc == rows[i].want);
        CHECK("policy.output.unchanged", !memcmp(&parsed, &before, sizeof(parsed)));
    }
    const char *const empty_hosts[] = {"", ":80", "u@", "u@:80", "@", "@:80"};
    for (size_t i = 0; i < sizeof(empty_hosts)/sizeof(empty_hosts[0]); ++i) {
        moq_uri_authority_parts_t parts, before;
        memset(&parts, 0xa5, sizeof(parts)); before = parts;
        CHECK("binding.empty.host", !moq_uri_authority_parse(B(empty_hosts[i]), &parts) &&
            !moq_uri_authority_valid(B(empty_hosts[i])));
        CHECK("binding.refusal.unchanged", !memcmp(&parts, &before, sizeof(parts)));
    }
    CHECK("binding.null.empty", !moq_uri_authority_valid((moq_bytes_t){NULL, 0}));
    CHECK("binding.null.nonempty", !moq_uri_authority_valid((moq_bytes_t){NULL, 1}));
    moq_uri_authority_parts_t parts, before;
    memset(&parts, 0xa5, sizeof(parts));
    CHECK("generic.null.empty", moq_uri_authority_parse_generic((moq_bytes_t){NULL, 0}, &parts));
    CHECK("generic.null.empty.output", !parts.host.data && !parts.host.len &&
        !parts.userinfo_present && !parts.port_present && !parts.ip_literal && !parts.ipvfuture);
    memset(&parts, 0xa5, sizeof(parts)); before = parts;
    CHECK("generic.null.nonempty", !moq_uri_authority_parse_generic((moq_bytes_t){NULL, 1}, &parts));
    CHECK("generic.refusal.unchanged", !memcmp(&parts, &before, sizeof(parts)));
    CHECK("generic.empty.userinfo", moq_uri_authority_parse_generic(B("@:80"), &parts));
    CHECK("generic.empty.userinfo.parts", parts.userinfo_present && !parts.userinfo.len &&
        !parts.host.len && parts.port_present && equal(parts.port, "80"));
    /* One-past-the-allocation is a valid empty slice, not a readable byte. */
    uint8_t *p = malloc(1);
    CHECK("empty.slice.alloc", p != NULL);
    if (p) {
        moq_bytes_t empty = {p + 1, 0};
        moq_endpoint_uri_t parsed;
        CHECK("binding.empty.slice", !moq_uri_authority_valid(empty));
        CHECK("uri.empty.slice", moq_endpoint_uri_parse(empty, &parsed) == MOQ_ERR_INVAL);
        CHECK("generic.empty.slice", moq_uri_authority_parse_generic(empty, &parts));
        CHECK("generic.empty.slice.output", parts.host.data == empty.data && !parts.host.len);
        free(p);
    }
}

/* LEGACY, selected explicitly, keeps its parser and defaults and still
 * derives the connection target from the URL's own bytes; a legacy URL whose
 * authority or path the grammar does not admit yields no target, and the
 * offer then excludes any version whose profile must be told one. */
static moq_endpoint_cfg_t legacy_config(const char *s)
{
    moq_endpoint_cfg_t c = config(s);
    c.uri_mode = MOQ_ENDPOINT_URI_LEGACY;
    return c;
}

static void legacy_targets(void)
{
    const struct { const char *input, *authority, *target; } rows[] = {
        {"moqt://relay.example:4433/live?x=1", "relay.example:4433", "/live?x=1"},
        {"moqt://relay.example", "relay.example", ""},
        {"moq://relay.example/a", "relay.example", "/a"},
        {"https://relay.example/moq?q", "relay.example", "/moq?q"},
        {"https://[::1]:8443", "[::1]:8443", ""},
    };
    for (size_t i = 0; i < sizeof(rows)/sizeof(rows[0]); ++i) {
        moq_endpoint_cfg_t c = legacy_config(rows[i].input);
        moq_endpoint_resolved_t r;
        CHECK(rows[i].input, moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK);
        CHECK("legacy.mode", r.uri_mode == MOQ_ENDPOINT_URI_LEGACY);
        CHECK("legacy.target.present", r.target.target_present == MOQ_BINDING_TARGET_COMPLETE);
        CHECK("legacy.target.authority", equal(r.target.authority, rows[i].authority));
        CHECK("legacy.target.path", equal(r.target.path_query, rows[i].target));
        CHECK("legacy.target.size", r.target.struct_size == sizeof(r.target));
    }
    /* LEGACY WebTransport dereferences an explicit wt_path, so a target is
     * kept only when that resource is admitted too. */
    {
        moq_endpoint_cfg_t w = legacy_config("https://relay.example/moq");
        w.wt_path = B("/tenant%2Fone?token=a%26b");
        moq_endpoint_resolved_t wr;
        CHECK("legacy.wt.override", moq_endpoint_resolve_cfg(&w, &wr) == MOQ_OK);
        CHECK("legacy.wt.override.target",
              wr.target.target_present == MOQ_BINDING_TARGET_COMPLETE);
        w.wt_path = B("/a b");
        CHECK("legacy.wt.bad", moq_endpoint_resolve_cfg(&w, &wr) == MOQ_OK);
        CHECK("legacy.wt.bad.no.target", wr.target.target_present == 0);
    }
    /* The legacy parser admits a space the grammar refuses. */
    moq_endpoint_cfg_t c = legacy_config("moqt://relay.example/a b");
    moq_endpoint_resolved_t r;
    moq_result_t rc = moq_endpoint_resolve_cfg(&c, &r);
    if (rc == MOQ_OK) {
        CHECK("legacy.no.target", r.target.target_present == 0);
        CHECK("legacy.no.target.spans", !r.target.authority.len && !r.target.path_query.len);
        for (size_t i = 0; i < r.version_count; ++i)
            CHECK("legacy.no.target.offer", moq_version_binding_requirements(r.versions[i]) == 0);
        CHECK("legacy.no.target.nonempty", r.version_count > 0);
        /* An explicit offer of a version that must be told its target is
         * refused rather than offered without one. */
        moq_version_t all[MOQ_ENDPOINT_MAX_VERSIONS];
        size_t n = moq_versions_supported(all, MOQ_ENDPOINT_MAX_VERSIONS);
        if (n > MOQ_ENDPOINT_MAX_VERSIONS) n = MOQ_ENDPOINT_MAX_VERSIONS;
        for (size_t i = 0; i < n; ++i) {
            if (moq_version_binding_requirements(all[i]) == 0) continue;
            c.versions.struct_size = sizeof(c.versions);
            c.versions.policy = MOQ_VERSION_POLICY_EXACT;
            c.versions.versions = &all[i];
            c.versions.version_count = 1;
            CHECK("legacy.no.target.exact", moq_endpoint_resolve_cfg(&c, &r) == MOQ_ERR_UNSUPPORTED);
        }
    } else {
        CHECK("legacy.space.refused", rc == MOQ_ERR_INVAL);
    }
}

/* The default offer is every supported draft the selected backend can
 * carry, newest first; a named preference moves to the front of that set. A
 * preference for a draft the backend cannot carry only orders the rest. An
 * explicit offer naming such a draft is refused; one naming only carried
 * drafts keeps its order. Draft-22 stays a supported core profile either
 * way. */
static void preference(void)
{
    const bool d22 = TEST_DEFAULT_BACKEND_HOSTS_D22 != 0;
    moq_version_t all[MOQ_ENDPOINT_MAX_VERSIONS];
    size_t n = moq_versions_supported(all, MOQ_ENDPOINT_MAX_VERSIONS);
    if (n > MOQ_ENDPOINT_MAX_VERSIONS) n = MOQ_ENDPOINT_MAX_VERSIONS;
    /* The drafts the default backend carries, in the registry's order. */
    moq_version_t carried[MOQ_ENDPOINT_MAX_VERSIONS];
    size_t nc = 0;
    for (size_t i = 0; i < n; ++i)
        if (d22 || all[i] != MOQ_VERSION_DRAFT_22) carried[nc++] = all[i];

    moq_endpoint_cfg_t c = config("moqt://host");
    moq_endpoint_resolved_t r;
    CHECK("default.resolve", moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK);
    CHECK("registry.d22", moq_endpoint_version_supported(MOQ_VERSION_DRAFT_22));
    if (d22) {
        CHECK("default.count", r.version_count == 3);
        CHECK("default.preference", r.preferred_version == MOQ_VERSION_DRAFT_22);
        CHECK("default.order", r.version_count == 3 &&
            r.versions[0] == MOQ_VERSION_DRAFT_22 && r.versions[1] == MOQ_VERSION_DRAFT_18 &&
            r.versions[2] == MOQ_VERSION_DRAFT_16);
    } else {
        CHECK("default.count.no_d22", r.version_count == 2);
        CHECK("default.preference.no_d22", r.preferred_version == MOQ_VERSION_DRAFT_18);
        CHECK("default.order.no_d22", r.version_count == 2 &&
            r.versions[0] == MOQ_VERSION_DRAFT_18 && r.versions[1] == MOQ_VERSION_DRAFT_16);
    }
    for (size_t i = 0; i < n; ++i) {
        c.preferred_version = all[i];
        CHECK("explicit.resolve", moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK);
        if (!d22 && all[i] == MOQ_VERSION_DRAFT_22) {
            /* Filtered preference: it orders nothing it cannot offer. */
            CHECK("filtered.first", r.versions[0] == MOQ_VERSION_DRAFT_18 &&
                r.preferred_version == MOQ_VERSION_DRAFT_18);
            CHECK("filtered.count", r.version_count == 2u);
        } else {
            CHECK("explicit.first", r.versions[0] == all[i] && r.preferred_version == all[i]);
            CHECK("explicit.count", r.version_count == (d22 ? 3u : 2u));
        }
        for (size_t a = 0; a < n; ++a) {
            size_t hits = 0;
            for (size_t b = 0; b < r.version_count; ++b) if (r.versions[b] == all[a]) ++hits;
            bool want = d22 || all[a] != MOQ_VERSION_DRAFT_22;
            CHECK("explicit.membership", hits == (want ? 1u : 0u));
        }
        /* The rest keep newest-first order behind the leading version. */
        moq_version_t prev = (moq_version_t)UINT32_MAX;
        for (size_t b = 1; b < r.version_count; ++b) {
            CHECK("explicit.rest.newest_first", r.versions[b] < prev);
            prev = r.versions[b];
        }
    }
    c.preferred_version = (moq_version_t)99;
    CHECK("unknown.preference", moq_endpoint_resolve_cfg(&c, &r) == MOQ_ERR_UNSUPPORTED);
    c.preferred_version = MOQ_VERSION_DRAFT_16;
    c.versions.struct_size = sizeof(c.versions);
    c.versions.policy = MOQ_VERSION_POLICY_LIST;
    c.versions.versions = all;
    c.versions.version_count = n;
    CHECK("list.preference.contradiction", moq_endpoint_resolve_cfg(&c, &r) == MOQ_ERR_INVAL);
    c.preferred_version = 0;
    /* A list of every supported draft: verbatim where the backend carries
     * them all, refused where it names one the backend cannot carry. */
    if (d22)
        CHECK("list.verbatim", moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK &&
            r.preferred_version == 0 && r.version_count == n &&
            !memcmp(r.versions, all, n * sizeof(all[0])));
    else
        CHECK("list.refused.no_d22", moq_endpoint_resolve_cfg(&c, &r) == MOQ_ERR_UNSUPPORTED);
    /* A list of carried drafts keeps its order, oldest first as given. */
    moq_version_t reversed[MOQ_ENDPOINT_MAX_VERSIONS];
    for (size_t i = 0; i < nc; ++i) reversed[i] = carried[nc - 1 - i];
    c.versions.versions = reversed;
    c.versions.version_count = nc;
    CHECK("list.carried.verbatim", moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK &&
        r.version_count == nc && !memcmp(r.versions, reversed, nc * sizeof(reversed[0])));
    moq_version_t d22v = MOQ_VERSION_DRAFT_22;
    c.versions.policy = MOQ_VERSION_POLICY_EXACT;
    c.versions.versions = &d22v;
    c.versions.version_count = 1;
    if (d22)
        CHECK("exact.d22", moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK &&
            r.version_count == 1 && r.versions[0] == d22v);
    else
        CHECK("exact.d22.refused", moq_endpoint_resolve_cfg(&c, &r) == MOQ_ERR_UNSUPPORTED);
    /* A v0-sized cfg has no preference field and gets the default. */
    moq_endpoint_cfg_t v0;
    moq_endpoint_cfg_init(&v0);
    v0.url = B("moqt://host");
    if (d22)
        CHECK("v0.default", moq_endpoint_resolve_cfg(&v0, &r) == MOQ_OK &&
            r.version_count == 3 && r.versions[0] == MOQ_VERSION_DRAFT_22 &&
            r.versions[1] == MOQ_VERSION_DRAFT_18 && r.versions[2] == MOQ_VERSION_DRAFT_16);
    else
        CHECK("v0.default.no_d22", moq_endpoint_resolve_cfg(&v0, &r) == MOQ_OK &&
            r.version_count == 2 && r.versions[0] == MOQ_VERSION_DRAFT_18 &&
            r.versions[1] == MOQ_VERSION_DRAFT_16);
    v0.url = B("https://host");
    CHECK("v0.wt.resolve", moq_endpoint_resolve_cfg(&v0, &r) == MOQ_OK);
    CHECK("v0.wt.path", equal(r.wt_path, "/moq"));
}

static uint32_t never_called(void *ctx, uint32_t present, moq_bytes_t a, moq_bytes_t p)
{ (void)ctx; (void)present; (void)a; (void)p; return MOQ_URI_ACCEPT; }

static void policy_shape(void)
{
    moq_endpoint_cfg_t c = config("moqt://host");
    moq_endpoint_resolved_t r;
    c.accept_uri = never_called;
    CHECK("client.policy", moq_endpoint_resolve_cfg(&c, &r) == MOQ_ERR_INVAL);
    c.accept_uri = NULL;
    c.accept_uri_ctx = &c;
    CHECK("client.policy.ctx", moq_endpoint_resolve_cfg(&c, &r) == MOQ_ERR_INVAL);
    c.accept_uri = never_called;
    c.perspective = MOQ_PERSPECTIVE_SERVER;
    CHECK("server.reserved", moq_endpoint_resolve_cfg(&c, &r) == MOQ_ERR_UNSUPPORTED);
    /* A cfg too short to carry the policy fields is read as carrying none. */
    c.perspective = MOQ_PERSPECTIVE_CLIENT;
    c.struct_size = (uint32_t)offsetof(moq_endpoint_cfg_t, accept_uri);
    CHECK("short.policy.ignored", moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK &&
        !r.target.accept_uri && !r.target.accept_uri_ctx);
}

typedef struct ledger { unsigned calls, live, frees; bool fail; } ledger_t;
static void *allocate(size_t n, void *ctx)
{
    ledger_t *l = ctx; ++l->calls;
    if (l->fail) return NULL;
    size_t *p = malloc(sizeof(size_t) + n);
    if (!p) return NULL;
    *p = n; ++l->live; return p + 1;
}
static void release(void *p, size_t n, void *ctx)
{
    ledger_t *l = ctx;
    CHECK("free.pointer", p != NULL);
    if (!p) return;
    size_t *header = (size_t *)p - 1;
    CHECK("free.exact.size", *header == n);
    CHECK("free.owned", l->live > 0);
    --l->live; ++l->frees; free(header);
}
static void *resize(void *p, size_t old, size_t n, void *ctx)
{
    (void)p; (void)old; (void)n; (void)ctx;
    CHECK("unexpected.realloc", false); return NULL;
}

static void ownership(void)
{
    /* Literal input also exercises cloning independently of the parser. */
    moq_endpoint_uri_t literal = {0};
    char literal_authority[] = "Literal:00443";
    char literal_path[] = "?q=%2F";
    literal.authority = B(literal_authority); literal.path_query = B(literal_path);
    moq_endpoint_uri_owned_t copy = {0};
    moq_result_t copied = moq_endpoint_uri_clone(moq_alloc_default(), &literal, true, &copy);
    CHECK("literal.clone", copied == MOQ_OK);
    memset(literal_authority, 'x', sizeof(literal_authority) - 1);
    memset(literal_path, 'x', sizeof(literal_path) - 1);
    if (copied == MOQ_OK) {
        CHECK("literal.http", copy.wt_path && !strcmp(copy.wt_path, "/?q=%2F"));
        CHECK("literal.authority", equal(copy.authority, "Literal:00443"));
        CHECK("literal.raw", equal(copy.path_query, "?q=%2F"));
    }
    moq_endpoint_uri_cleanup(moq_alloc_default(), &copy);
    const char *inputs[] = {"moqt://Host:00443/a%2fb?q=", "moqt://Host:00443?", "moqt://Host:00443"};
    const char *raw[] = {"/a%2fb?q=", "?", ""};
    const char *http[] = {"/a%2fb?q=", "/?", "/"};
    for (size_t row = 0; row < 3; ++row) for (unsigned wt = 0; wt < 2; ++wt) {
        char *input = malloc(strlen(inputs[row]) + 1);
        CHECK("caller.alloc", input != NULL);
        if (!input) continue;
        strcpy(input, inputs[row]);
        moq_endpoint_uri_t uri;
        moq_result_t rc = moq_endpoint_uri_parse(B(input), &uri);
        CHECK("owner.parse", rc == MOQ_OK);
        if (rc != MOQ_OK) { free(input); continue; }
        ledger_t l = {0};
        moq_alloc_t a = {.ctx = &l, .alloc = allocate, .realloc = resize, .free = release};
        moq_endpoint_uri_owned_t owned = {0}, before = owned;
        l.fail = true;
        CHECK("clone.fail", moq_endpoint_uri_clone(&a, &uri, wt != 0, &owned) == MOQ_ERR_NOMEM);
        CHECK("clone.rollback", !memcmp(&owned, &before, sizeof(owned)) && l.calls == 1 && !l.live);
        l.fail = false;
        rc = moq_endpoint_uri_clone(&a, &uri, wt != 0, &owned);
        CHECK("clone.retry", rc == MOQ_OK);
        if (rc != MOQ_OK) { free(input); continue; }
        CHECK("clone.single.allocation", l.calls == 2 && l.live == 1);
        before = owned;
        CHECK("clone.occupied", moq_endpoint_uri_clone(&a, &uri, wt != 0, &owned) == MOQ_ERR_WRONG_STATE);
        CHECK("clone.occupied.unchanged", !memcmp(&owned, &before, sizeof(owned)) && l.calls == 2);
        memset(input, 0xa5, strlen(inputs[row])); free(input);
        memset(&uri, 0, sizeof(uri));
        CHECK("owned.authority", equal(owned.authority, "Host:00443"));
        CHECK("owned.target", equal(owned.path_query, raw[row]));
        CHECK("owned.nuls", owned.authority.data[owned.authority.len] == 0 &&
            owned.path_query.data[owned.path_query.len] == 0);
        CHECK("owned.http", wt ? (owned.wt_path && !strcmp(owned.wt_path, http[row])) : !owned.wt_path);
        size_t need = strlen("Host:00443") + 1 + strlen(raw[row]) + 1;
        if (wt) need += strlen(http[row]) + 1;
        CHECK("owned.exact.allocation", owned.storage_size == need);
        moq_endpoint_uri_cleanup(&a, &owned);
        CHECK("owned.settled", !l.live && l.frees == 1 && !owned.storage && !owned.wt_path &&
            !owned.authority.data && !owned.authority.len && !owned.path_query.data && !owned.path_query.len);
        moq_endpoint_uri_cleanup(&a, &owned);
        CHECK("cleanup.idempotent", l.frees == 1);
    }
}

static void limits_and_legacy(void)
{
    size_t n = 7 + UINT16_MAX + 1;
    char *input = malloc(n + 1);
    CHECK("limit.alloc", input != NULL);
    if (input) {
        memcpy(input, "moqt://h/", 9);
        memset(input + 9, 'a', n - 9); input[n] = 0;
        moq_endpoint_uri_t p;
        CHECK("limit.exact", moq_endpoint_uri_parse((moq_bytes_t){(const uint8_t *)input, n-1}, &p) == MOQ_OK);
        CHECK("limit.over", moq_endpoint_uri_parse(B(input), &p) == MOQ_ERR_INVAL);
        /* The same bound applies to a LEGACY target: one byte over yields none. */
        moq_endpoint_cfg_t c;
        moq_endpoint_cfg_init(&c);
        c.url = (moq_bytes_t){(const uint8_t *)input, n-1};
        moq_endpoint_resolved_t r;
        if (moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK)
            CHECK("limit.legacy.exact", r.target.target_present == MOQ_BINDING_TARGET_COMPLETE);
        c.url = B(input);
        if (moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK)
            CHECK("limit.legacy.over", r.target.target_present == 0);
        free(input);
    }
    ledger_t bad_l = {0};
    moq_alloc_t bad_a = {.ctx=&bad_l, .alloc=allocate, .realloc=resize, .free=release};
    moq_endpoint_uri_t forged = {0};
    moq_endpoint_uri_owned_t owned = {0};
    forged.authority = B("h");
    forged.path_query = (moq_bytes_t){(const uint8_t *)"?", SIZE_MAX};
    CHECK("clone.overflow", moq_endpoint_uri_clone(&bad_a, &forged, true, &owned) == MOQ_ERR_INVAL);
    CHECK("clone.overflow.before.read.alloc", !bad_l.calls);
    forged.path_query = (moq_bytes_t){NULL, 1};
    CHECK("clone.null.span", moq_endpoint_uri_clone(&bad_a, &forged, true, &owned) == MOQ_ERR_INVAL);
    CHECK("clone.null.alloc", moq_endpoint_uri_clone(NULL, &forged, true, &owned) == MOQ_ERR_INVAL);
    moq_endpoint_cfg_t c = config("moqt://host/path?");
    c.uri_mode = UINT32_MAX;
    moq_endpoint_t *ep = (moq_endpoint_t *)(uintptr_t)1;
    ledger_t l = {.fail = true};
    moq_alloc_t a = {.ctx=&l, .alloc=allocate, .realloc=resize, .free=release};
    c.alloc = &a;
    CHECK("connect.unknown", moq_endpoint_connect(&c, &ep) == MOQ_ERR_INVAL);
    CHECK("connect.unknown.no.effects", !ep && !l.calls);
    c = config("moq://host"); c.uri_mode = MOQ_ENDPOINT_URI_LEGACY;
    moq_endpoint_resolved_t r;
    CHECK("legacy.alias", moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK && r.url.port == 4433);
    c = config("https://host?lost-in-legacy"); c.uri_mode = MOQ_ENDPOINT_URI_LEGACY;
    CHECK("legacy.wt", moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK && equal(r.wt_path, "/moq"));
    CHECK("legacy.wt.target", equal(r.target.path_query, "?lost-in-legacy"));
    c.wt_path = B("/override");
    CHECK("legacy.override", moq_endpoint_resolve_cfg(&c, &r) == MOQ_OK && equal(r.wt_path, "/override"));
}

int main(void)
{
    layout(); identities(); refusals(); authority_policy(); legacy_targets();
    preference(); policy_shape(); ownership(); limits_and_legacy();
    printf("endpoint_uri: %u failures\n", failures);
    return failures ? 1 : 0;
}
