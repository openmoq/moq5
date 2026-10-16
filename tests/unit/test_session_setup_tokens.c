/*
 * The AUTHORIZATION TOKEN in SETUP (draft-16 §9.3.1.5 setup parameter,
 * draft-18 §10.3.1.4 Setup Option): moq_session_cfg_t.setup_auth_tokens.
 *
 * Rows, on draft-16 and draft-18, through two real transport bridges:
 *   - configured tokens reach the peer's SETUP_COMPLETE by value, from either
 *     side, at one, two and sixteen tokens, and a value just inside the
 *     default send arena arrives whole (session to session, since the fake
 *     endpoint records at most 256 bytes per write);
 *   - the session owns its copy: changing the caller's storage after create
 *     does not change what is sent;
 *   - refusals at create (count, NULL storage, token type, value length, a
 *     SETUP the 16-bit length cannot carry) leave nothing behind;
 *   - a caller compiled before the fields existed (smaller struct_size) sends
 *     none, whatever its stack holds past its struct;
 *   - the credential does not outlive its owner: no block the session frees
 *     still holds the token value;
 *   - the draft-18 Setup Option codec emits USE_VALUE and REGISTER tokens and
 *     refuses the alias forms that cannot appear before SETUP.
 *
 * Token values are opaque bytes; row_opaque_values pins empty and zero-byte
 * values in SETUP and in a request.
 */

#include <stdbool.h>

static int failures = 0;

#include "../support/bridge_wire_pair.h"
#include <moq/control_d18.h>

#include <stddef.h>

/* -- ABI: the appended fields lie past the layout that preceded them ------- */

/* The layout ended at max_namespace_suffixes_per_subscription; its sizeof
 * rounds that end up to the struct alignment. A field read under a struct_size
 * gate must start at or past that old sizeof, or an old caller's padding would
 * be read as input. */
#define OLD_SESSION_CFG_END \
    (offsetof(moq_session_cfg_t, max_namespace_suffixes_per_subscription) + \
     sizeof(uint32_t))
#define ROUND_UP(x, a) ((((x) + (a) - 1) / (a)) * (a))
_Static_assert(offsetof(moq_session_cfg_t, setup_auth_tokens) >=
                   ROUND_UP(OLD_SESSION_CFG_END, _Alignof(moq_session_cfg_t)),
               "setup_auth_tokens must start past the previous sizeof");
_Static_assert(offsetof(moq_session_cfg_t, setup_auth_token_count) >
                   offsetof(moq_session_cfg_t, setup_auth_tokens),
               "setup_auth_token_count follows setup_auth_tokens");

/* -- tuning ----------------------------------------------------------------- */

typedef struct token_side {
    const moq_auth_token_t *tokens;
    size_t                  count;
    bool                    on_server;
    uint64_t                token_cache;
    size_t                  struct_size;   /* 0 = full */
} token_side_t;

static void tune_tokens(moq_session_cfg_t *cfg, bool client, void *ctx)
{
    const token_side_t *t = (const token_side_t *)ctx;
    if (t->token_cache) {
        cfg->send_auth_token_cache_size = true;
        cfg->auth_token_cache_size = t->token_cache;
    }
    if (client == !t->on_server) {
        cfg->setup_auth_tokens = t->tokens;
        cfg->setup_auth_token_count = t->count;
        if (t->struct_size) cfg->struct_size = (uint32_t)t->struct_size;
    }
}

/* What the receiver's SETUP_COMPLETE carried. */
typedef struct got_tokens {
    size_t   count;
    uint64_t type[16];
    size_t   len[16];
    uint8_t  first[16];
    uint8_t  last[16];
} got_tokens_t;


static bool open_and_collect(rl_pair_t *p, moq_version_t v, token_side_t *t,
                             got_tokens_t *got)
{
    memset(got, 0, sizeof(*got));
    rl_opts_t o = { .tune = tune_tokens, .tune_ctx = t };
    /* As pair_open, but the receiver's SETUP_COMPLETE is read for its
     * borrowed tokens before anything else is polled. */
    memset(p, 0, sizeof(*p));
    p->ver = v;
    p->now = 1000;
    p->client_control_sid = UINT64_MAX;
    p->server_control_uni = UINT64_MAX;
    p->calloc = test_allocator(&p->cas);
    p->salloc = test_allocator(&p->sas);
    moq_session_cfg_t cc, sc;
    moq_session_cfg_init_sized(&cc, sizeof(cc), &p->calloc, MOQ_PERSPECTIVE_CLIENT);
    moq_session_cfg_init_sized(&sc, sizeof(sc), &p->salloc, MOQ_PERSPECTIVE_SERVER);
    cc.version = sc.version = v;
    cc.send_request_capacity = sc.send_request_capacity = true;
    cc.initial_request_capacity = sc.initial_request_capacity = 8;
    o.tune(&cc, true, o.tune_ctx);
    o.tune(&sc, false, o.tune_ctx);
    if (moq_session_create(&cc, p->now, &p->c) < 0) return false;
    if (moq_session_create(&sc, p->now, &p->s) < 0) return false;
    fake_endpoint_init(&p->cep, 1000, 2000);
    fake_endpoint_init(&p->sep, 3000, 4000);
    p->cep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    p->sep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    moq_transport_bridge_cfg_t bc;
    moq_transport_bridge_cfg_init(&bc, moq_alloc_default());
    if (moq_transport_bridge_create(&bc, p->c, &p->cep.vtable, &p->cep, &p->cb) < 0)
        return false;
    if (moq_transport_bridge_create(&bc, p->s, &p->sep.vtable, &p->sep, &p->sb) < 0)
        return false;
    if (moq_session_start(p->c, p->now) < 0) return false;
    if (v == MOQ_VERSION_DRAFT_18 && moq_session_start(p->s, p->now) < 0)
        return false;
    shuttle(p);
    moq_session_t *receiver = t->on_server ? p->c : p->s;
    moq_event_t ev;
    while (moq_session_poll_events(receiver, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) {
            const moq_setup_complete_event_t *sc_ev = &ev.u.setup_complete;
            got->count = sc_ev->token_count;
            for (size_t i = 0; i < sc_ev->token_count && i < 16; i++) {
                const moq_resolved_token_t *rt = &sc_ev->tokens[i];
                got->type[i] = rt->token_type;
                got->len[i] = rt->token_value.len;
                if (rt->token_value.len) {
                    got->first[i] = rt->token_value.data[0];
                    got->last[i] = rt->token_value.data[rt->token_value.len - 1];
                }
            }
        }
        moq_event_cleanup(&ev);
    }
    obs_t ob;
    memset(&ob, 0, sizeof(ob));
    drain(t->on_server ? p->s : p->c, &ob);
    return moq_session_state(p->c) == MOQ_SESS_ESTABLISHED &&
           moq_session_state(p->s) == MOQ_SESS_ESTABLISHED &&
           !moq_transport_bridge_is_fatal(p->cb) &&
           !moq_transport_bridge_is_fatal(p->sb);
}

static const char *vname(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_18 ? "draft-18" : "draft-16";
}

/* -- rows ------------------------------------------------------------------- */

/* Tokens configured on one side reach the other's SETUP_COMPLETE by value. */
static int row_tokens_reach_peer(moq_version_t v)
{
    failures = 0;
    const size_t counts[] = { 1, 2, 16 };
    for (int on_server = 0; on_server < 2; on_server++) {
        for (size_t ci = 0; ci < 3; ci++) {
            uint8_t values[16][3];
            moq_auth_token_t toks[16];
            for (size_t i = 0; i < 16; i++) {
                values[i][0] = (uint8_t)(0xA0 + i);
                values[i][1] = 0x55;
                values[i][2] = (uint8_t)(i + 1);
                toks[i] = (moq_auth_token_t){ 7 + i, { values[i], 3 } };
            }
            token_side_t t = { .tokens = toks, .count = counts[ci],
                               .on_server = on_server != 0, .token_cache = 64 };
            rl_pair_t p;
            got_tokens_t got;
            MOQ_TEST_CHECK(open_and_collect(&p, v, &t, &got));
            MOQ_TEST_CHECK_EQ_SIZE(got.count, counts[ci]);
            for (size_t i = 0; i < got.count && i < counts[ci]; i++) {
                MOQ_TEST_CHECK_EQ_U64(got.type[i], 7 + i);
                MOQ_TEST_CHECK_EQ_SIZE(got.len[i], 3);
                MOQ_TEST_CHECK_EQ_INT(got.first[i], 0xA0 + (int)i);
                MOQ_TEST_CHECK_EQ_INT(got.last[i], (int)i + 1);
            }
            pair_close(&p);
        }
    }
    /* No tokens configured: none arrive. */
    token_side_t none = { 0 };
    rl_pair_t p;
    got_tokens_t got;
    MOQ_TEST_CHECK(open_and_collect(&p, v, &none, &got));
    MOQ_TEST_CHECK_EQ_SIZE(got.count, 0);
    pair_close(&p);
    if (failures) fprintf(stderr, "  in row tokens_reach_peer (%s)\n", vname(v));
    MOQ_TEST_PASS("setup tokens reach the peer by value");
    return failures;
}

/* Control output of `from` delivered straight to `to` (no transport): the
 * fake endpoint records at most 256 bytes per write, which a SETUP carrying a
 * large token exceeds. */
static void direct_control(moq_session_t *from, moq_session_t *to)
{
    moq_action_t a;
    while (moq_session_poll_actions(from, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_SEND_CONTROL)
            (void)moq_session_on_control_bytes(to, a.u.send_control.data,
                                               a.u.send_control.len, 0);
        else if (a.kind == MOQ_ACTION_OPEN_UNI_CONTROL)
            (void)moq_session_on_control_bytes(to, a.u.open_uni_control.data,
                                               a.u.open_uni_control.len, 0);
        moq_action_cleanup(&a);
    }
}

/* A value just inside the default 4096-byte arenas leaves and arrives whole,
 * in both directions. */
static int row_large_token(moq_version_t v)
{
    failures = 0;
    static uint8_t big[3800];
    for (size_t i = 0; i < sizeof(big); i++) big[i] = (uint8_t)((i * 37u + 5u) % 255u + 1u);
    for (int on_server = 0; on_server < 2; on_server++) {
        moq_auth_token_t tok = { 12, { big, sizeof(big) } };
        moq_session_cfg_t cc, sc;
        moq_session_cfg_init_sized(&cc, sizeof(cc), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
        moq_session_cfg_init_sized(&sc, sizeof(sc), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
        cc.version = sc.version = v;
        moq_session_cfg_t *src = on_server ? &sc : &cc;
        src->setup_auth_tokens = &tok;
        src->setup_auth_token_count = 1;
        moq_session_t *c = NULL, *sv = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cc, 0, &c), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&sc, 0, &sv), MOQ_OK);
        if (!c || !sv) { moq_session_destroy(c); moq_session_destroy(sv); continue; }
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(c, 0), MOQ_OK);
        if (v == MOQ_VERSION_DRAFT_18)
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(sv, 0), MOQ_OK);
        for (int i = 0; i < 4; i++) {
            direct_control(c, sv);
            direct_control(sv, c);
        }
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        moq_session_t *receiver = on_server ? c : sv;
        bool seen = false;
        moq_event_t ev;
        while (moq_session_poll_events(receiver, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) {
                seen = true;
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.setup_complete.token_count, 1);
                if (ev.u.setup_complete.token_count == 1) {
                    const moq_resolved_token_t *t = &ev.u.setup_complete.tokens[0];
                    MOQ_TEST_CHECK_EQ_U64(t->token_type, 12);
                    MOQ_TEST_CHECK(t->token_value.len == sizeof(big) &&
                                   memcmp(t->token_value.data, big, sizeof(big)) == 0);
                }
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(seen);
        moq_session_destroy(c);
        moq_session_destroy(sv);
    }
    if (failures) fprintf(stderr, "  in row large_token (%s)\n", vname(v));
    MOQ_TEST_PASS("a large setup token arrives whole");
    return failures;
}

/* The session copies the tokens at create: the caller's storage may change or
 * go away before start without changing what is sent. */
static int row_owned_copy(moq_version_t v)
{
    failures = 0;
    uint8_t value[2] = { 0x11, 0x22 };
    moq_auth_token_t tok = { 3, { value, sizeof(value) } };
    moq_session_cfg_t cfg;
    moq_alloc_t a;
    test_alloc_state_t as;
    memset(&as, 0, sizeof(as));
    a = test_allocator(&as);
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), &a, MOQ_PERSPECTIVE_CLIENT);
    cfg.version = v;
    cfg.setup_auth_tokens = &tok;
    cfg.setup_auth_token_count = 1;
    moq_session_t *c = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &c), MOQ_OK);
    value[0] = 0x99;
    tok.token_type = 9;
    tok.token_value.len = 1;
    /* Re-run the copy through the pair: the session under test is the
     * client; open_and_collect builds its own, so drive this one directly. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(c, 0), MOQ_OK);
    moq_action_t act;
    size_t n = moq_session_poll_actions(c, &act, 1);
    MOQ_TEST_CHECK_EQ_SIZE(n, 1);
    const uint8_t *bytes = NULL;
    size_t len = 0;
    if (n == 1 && act.kind == MOQ_ACTION_SEND_CONTROL) {
        bytes = act.u.send_control.data;
        len = act.u.send_control.len;
    } else if (n == 1 && act.kind == MOQ_ACTION_OPEN_UNI_CONTROL) {
        bytes = act.u.open_uni_control.data;
        len = act.u.open_uni_control.len;
    }
    MOQ_TEST_CHECK(bytes != NULL);
    /* The original value (11 22) is on the wire; the later edit (99) is not. */
    bool has_orig = false, has_edit = false;
    for (size_t i = 0; bytes && i + 1 < len; i++) {
        if (bytes[i] == 0x11 && bytes[i + 1] == 0x22) has_orig = true;
        if (bytes[i] == 0x99) has_edit = true;
    }
    MOQ_TEST_CHECK(has_orig);
    MOQ_TEST_CHECK(!has_edit);
    if (n == 1) moq_action_cleanup(&act);
    moq_session_destroy(c);
    MOQ_TEST_CHECK_EQ_INT(as.balance, 0);
    if (failures) fprintf(stderr, "  in row owned_copy (%s)\n", vname(v));
    MOQ_TEST_PASS("the session sends its own copy of the tokens");
    return failures;
}

/* Every token set the SETUP cannot carry is refused at create, with nothing
 * allocated left behind. */
static int row_refusals(moq_version_t v)
{
    failures = 0;
    test_alloc_state_t as;
    memset(&as, 0, sizeof(as));
    moq_alloc_t a = test_allocator(&as);
    moq_session_cfg_t cfg;
    moq_session_t *s = NULL;
    moq_auth_token_t tokens[17];
    memset(tokens, 0, sizeof(tokens));
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), &a, MOQ_PERSPECTIVE_CLIENT);
    cfg.version = v;

    cfg.setup_auth_tokens = tokens;
    cfg.setup_auth_token_count = 17;                       /* more than 16 */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), MOQ_ERR_INVAL);
    cfg.setup_auth_tokens = NULL;
    cfg.setup_auth_token_count = 1;                        /* NULL array */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), MOQ_ERR_INVAL);
    cfg.setup_auth_tokens = tokens;
    tokens[0].token_value = (moq_bytes_t){ NULL, 3 };      /* NULL value */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), MOQ_ERR_INVAL);
    tokens[0].token_value = (moq_bytes_t){ NULL, 0 };
    tokens[0].token_type = UINT64_C(1) << 62;              /* type past 2^62-1 */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), MOQ_ERR_INVAL);
    static uint8_t huge[65536];
    tokens[0].token_type = 1;
    tokens[0].token_value = (moq_bytes_t){ huge, sizeof(huge) };   /* > 65535 */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), MOQ_ERR_INVAL);
    /* Each value fits, but together they overflow the 16-bit SETUP length. */
    tokens[0].token_value = (moq_bytes_t){ huge, 40000 };
    tokens[1] = (moq_auth_token_t){ 2, { huge, 40000 } };
    cfg.setup_auth_token_count = 2;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(s == NULL);
    MOQ_TEST_CHECK_EQ_INT(as.balance, 0);

    /* Zero tokens with a NULL array is simply "none". */
    cfg.setup_auth_tokens = NULL;
    cfg.setup_auth_token_count = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), MOQ_OK);
    moq_session_destroy(s);
    MOQ_TEST_CHECK_EQ_INT(as.balance, 0);
    if (failures) fprintf(stderr, "  in row refusals (%s)\n", vname(v));
    MOQ_TEST_PASS("unsendable setup tokens are refused at create");
    return failures;
}

/* A caller whose struct ends before the fields sends none, whatever lies in
 * its memory past that struct. */
static int row_old_struct_size(moq_version_t v)
{
    failures = 0;
    moq_auth_token_t tok = { 5, { (const uint8_t *)"zz", 2 } };
    token_side_t t = { .tokens = &tok, .count = 1, .on_server = false,
                       .struct_size = offsetof(moq_session_cfg_t, setup_auth_tokens) };
    rl_pair_t p;
    got_tokens_t got;
    MOQ_TEST_CHECK(open_and_collect(&p, v, &t, &got));
    MOQ_TEST_CHECK_EQ_SIZE(got.count, 0);
    pair_close(&p);
    /* Covering the array but not the count is still "none". */
    t.struct_size = offsetof(moq_session_cfg_t, setup_auth_token_count);
    MOQ_TEST_CHECK(open_and_collect(&p, v, &t, &got));
    MOQ_TEST_CHECK_EQ_SIZE(got.count, 0);
    pair_close(&p);
    if (failures) fprintf(stderr, "  in row old_struct_size (%s)\n", vname(v));
    MOQ_TEST_PASS("an older cfg layout sends no setup tokens");
    return failures;
}

/* -- credential lifetime ---------------------------------------------------- */

/* An allocator that, on every free, reports whether the block still holds the
 * secret pattern. */
static const uint8_t k_secret[] = { 0x5e, 0xc2, 0xe7, 0x7a, 0x0b, 0x3c, 0x91, 0x4d,
                                    0x26, 0xf8, 0x13, 0x6a, 0xd5, 0x40, 0x8e, 0x77 };
typedef struct scan_alloc {
    int64_t balance;
    int     leaked_frees;
} scan_alloc_t;

static void *scan_alloc_fn(size_t n, void *ctx)
{
    scan_alloc_t *s = (scan_alloc_t *)ctx;
    void *p = malloc(n ? n : 1);
    if (p) s->balance += (int64_t)n;
    return p;
}

static void scan_free_fn(void *p, size_t n, void *ctx)
{
    scan_alloc_t *s = (scan_alloc_t *)ctx;
    if (!p) return;
    const uint8_t *b = (const uint8_t *)p;
    for (size_t i = 0; i + sizeof(k_secret) <= n; i++)
        if (memcmp(b + i, k_secret, sizeof(k_secret)) == 0) { s->leaked_frees++; break; }
    s->balance -= (int64_t)n;
    free(p);
}

static void *scan_realloc_fn(void *p, size_t old_n, size_t new_n, void *ctx)
{
    void *q = scan_alloc_fn(new_n, ctx);
    if (q && p) memcpy(q, p, old_n < new_n ? old_n : new_n);
    if (p) scan_free_fn(p, old_n, ctx);
    return q;
}

/* The session's copy of a credential, and the SETUP encoded from it, are
 * wiped before their memory is returned: no block the session frees still
 * holds the token value, whether the session ran or was refused. */
static int row_credential_wiped(moq_version_t v)
{
    failures = 0;
    for (int run = 0; run < 2; run++) {
        scan_alloc_t sa = { 0, 0 };
        moq_alloc_t a = { .alloc = scan_alloc_fn, .free = scan_free_fn,
                          .realloc = scan_realloc_fn, .ctx = &sa };
        static uint8_t value[600];
        for (size_t i = 0; i < sizeof(value); i += sizeof(k_secret))
            memcpy(value + i, k_secret,
                   sizeof(value) - i < sizeof(k_secret) ? sizeof(value) - i
                                                        : sizeof(k_secret));
        moq_auth_token_t tok = { 4, { value, sizeof(value) } };
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), &a, MOQ_PERSPECTIVE_CLIENT);
        cfg.version = v;
        cfg.setup_auth_tokens = &tok;
        cfg.setup_auth_token_count = 1;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), MOQ_OK);
        if (run == 1 && s) {
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(s, 0), MOQ_OK);
            moq_action_t act;
            while (moq_session_poll_actions(s, &act, 1) > 0)
                moq_action_cleanup(&act);
        }
        moq_session_destroy(s);
        MOQ_TEST_CHECK_EQ_INT(sa.leaked_frees, 0);
        MOQ_TEST_CHECK_EQ_INT((int)sa.balance, 0);
    }
    if (failures) fprintf(stderr, "  in row credential_wiped (%s)\n", vname(v));
    MOQ_TEST_PASS("no freed session memory still holds the setup token");
    return failures;
}


/* Every allocation create makes after the session block (the peer token
 * cache, the track history) can fail. With setup tokens configured the
 * refusal still returns through the caller's own free callback, wipes the
 * owned credential first, leaves no output and no live allocation. A no-token
 * create is the control. */
static int g_fail_at, g_alloc_calls;
static scan_alloc_t g_fa_scan;
static void *late_alloc_fn(size_t n, void *ctx)
{
    (void)ctx;
    if (++g_alloc_calls == g_fail_at) return NULL;
    return scan_alloc_fn(n, &g_fa_scan);
}
static void late_free_fn(void *p, size_t n, void *ctx)
{
    (void)ctx;
    scan_free_fn(p, n, &g_fa_scan);
}
static void *late_realloc_fn(void *p, size_t o, size_t n, void *ctx)
{
    void *q = late_alloc_fn(n, ctx);
    if (q && p) memcpy(q, p, o < n ? o : n);
    if (q && p) late_free_fn(p, o, ctx);
    return q;
}

static int row_late_create_failure(moq_version_t v)
{
    failures = 0;
    static uint8_t value[64];
    for (size_t i = 0; i < sizeof(value); i++) value[i] = k_secret[i % sizeof(k_secret)];
    moq_auth_token_t tok = { 4, { value, sizeof(value) } };
    for (int with_tokens = 0; with_tokens < 2; with_tokens++) {
        int refused_late = 0;
        for (int k = 2; k < 16; k++) {
            g_fail_at = k;
            g_alloc_calls = 0;
            memset(&g_fa_scan, 0, sizeof(g_fa_scan));
            moq_alloc_t a = { .ctx = NULL, .alloc = late_alloc_fn, .free = late_free_fn,
                              .realloc = late_realloc_fn };
            moq_session_cfg_t cfg;
            moq_session_cfg_init_sized(&cfg, sizeof(cfg), &a, MOQ_PERSPECTIVE_CLIENT);
            cfg.version = v;
            cfg.send_auth_token_cache_size = true;
            cfg.auth_token_cache_size = 256;
            if (with_tokens) {
                cfg.setup_auth_tokens = &tok;
                cfg.setup_auth_token_count = 1;
            }
            moq_session_t *s = (moq_session_t *)(uintptr_t)1;
            moq_result_t rc = moq_session_create(&cfg, 0, &s);
            if (rc == MOQ_OK) {
                moq_session_destroy(s);
                MOQ_TEST_CHECK_EQ_INT((int)g_fa_scan.balance, 0);
                break;   /* past the last allocation create makes */
            }
            refused_late++;
            MOQ_TEST_CHECK_EQ_INT((int)rc, MOQ_ERR_NOMEM);
            MOQ_TEST_CHECK(s == NULL);
            MOQ_TEST_CHECK_EQ_INT((int)g_fa_scan.balance, 0);
            MOQ_TEST_CHECK_EQ_INT(g_fa_scan.leaked_frees, 0);
        }
        /* The cache and the history: at least two late refusal points. */
        MOQ_TEST_CHECK(refused_late >= 2);
    }
    if (failures) fprintf(stderr, "  in row late_create_failure (%s)\n", vname(v));
    MOQ_TEST_PASS("late create failures release through the caller's allocator");
    return failures;
}


/* -- draft-16 receive: more tokens than this side holds ---------------------- */

/* A directly encoded CLIENT_SETUP / SERVER_SETUP carrying `n` AUTHORIZATION
 * TOKEN parameters (USE_VALUE or REGISTER with distinct aliases). */
static size_t d16_setup_with_tokens(uint8_t *buf, size_t cap, bool client, size_t n,
                                    bool reg, uint8_t vals[][2])
{
    moq_kvp_entry_t params[40];
    uint8_t enc[40][32];
    for (size_t i = 0; i < n; i++) {
        moq_d16_auth_token_t t;
        memset(&t, 0, sizeof(t));
        t.alias_type = reg ? MOQ_AUTH_TOKEN_REGISTER : MOQ_AUTH_TOKEN_USE_VALUE;
        t.alias = 100 + i;
        t.token_type = 5;
        vals[i][0] = (uint8_t)(0x40 + i);
        vals[i][1] = 0x41;
        t.token_value = vals[i];
        t.token_value_len = 2;
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, enc[i], sizeof(enc[i]));
        if (moq_d16_auth_token_encode(&tw, &t) != MOQ_OK) return 0;
        params[i] = (moq_kvp_entry_t){ .type = MOQ_SETUP_PARAM_AUTHORIZATION_TOKEN,
                                       .value = enc[i],
                                       .value_len = moq_buf_writer_offset(&tw) };
    }
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_result_t rc = client ? moq_d16_encode_client_setup(&w, params, n)
                             : moq_d16_encode_server_setup(&w, params, n);
    return rc == MOQ_OK ? moq_buf_writer_offset(&w) : 0;
}

/* Sixteen tokens -- the most this side holds -- arrive whole; a seventeenth
 * is refused before anything is published or cached: the session ends with a
 * local-capacity close, never establishes, and surfaces no truncated token
 * list. Both roles, USE_VALUE and REGISTER (with cache room for all). */
static int row_d16_token_capacity(void)
{
    failures = 0;
    for (int client_sends = 0; client_sends < 2; client_sends++) {
        for (int reg = 0; reg < 2; reg++) {
            for (size_t n = 16; n <= 17; n++) {
                moq_session_cfg_t cfg;
                moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                    client_sends ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT);
                cfg.version = MOQ_VERSION_DRAFT_16;
                cfg.send_auth_token_cache_size = true;
                cfg.auth_token_cache_size = 4096;
                moq_session_t *r = NULL;
                MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &r), MOQ_OK);
                if (!r) continue;
                if (!client_sends) {
                    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(r, 0), MOQ_OK);
                    moq_action_t a;
                    while (moq_session_poll_actions(r, &a, 1) > 0) moq_action_cleanup(&a);
                }
                uint8_t vals[40][2];
                uint8_t buf[2048];
                size_t len = d16_setup_with_tokens(buf, sizeof(buf), client_sends != 0,
                                                   n, reg != 0, vals);
                MOQ_TEST_CHECK(len > 0);
                (void)moq_session_on_control_bytes(r, buf, len, 1);
                size_t seen = SIZE_MAX;
                int closed = 0;
                uint64_t close_code = UINT64_MAX;
                moq_event_t ev;
                while (moq_session_poll_events(r, &ev, 1) > 0) {
                    if (ev.kind == MOQ_EVENT_SETUP_COMPLETE)
                        seen = ev.u.setup_complete.token_count;
                    if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                        closed++;
                        close_code = ev.u.closed.code;
                    }
                    moq_event_cleanup(&ev);
                }
                if (n == 16) {
                    MOQ_TEST_CHECK_EQ_SIZE(seen, 16);
                    MOQ_TEST_CHECK(moq_session_state(r) == MOQ_SESS_ESTABLISHED);
                } else {
                    MOQ_TEST_CHECK_EQ_SIZE(seen, SIZE_MAX);
                    MOQ_TEST_CHECK(moq_session_state(r) == MOQ_SESS_CLOSED);
                    MOQ_TEST_CHECK_EQ_INT(closed, 1);
                    MOQ_TEST_CHECK_EQ_U64(close_code, 0x1);
                    MOQ_TEST_CHECK_EQ_SIZE(r->peer_token_cache.used_bytes, 0);
                }
                moq_session_destroy(r);
            }
        }
    }
    MOQ_TEST_PASS("draft-16 SETUP token capacity is refused, not truncated");
    return failures;
}


/* -- create-time admission counts the whole SETUP ----------------------------- */

/* With every optional field the profile emits at its largest encoding (a
 * request capacity and a token-cache size of 2^30, a token type of 2^30) and
 * a send arena far larger than any SETUP, every token value create accepts
 * yields a SETUP the session can encode and queue at start, and the first
 * value it refuses is exactly the one whose SETUP would no longer fit the
 * 16-bit message length. */
static int row_setup_wire_size(moq_version_t v)
{
    failures = 0;
    static uint8_t big[65535];
    memset(big, 0x5a, sizeof(big));
    size_t largest_ok = 0;
    for (size_t len = 65535; len >= 65300; len--) {
        moq_auth_token_t tok = { UINT64_C(1) << 30, { big, len } };
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
        cfg.version = v;
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = UINT64_C(1) << 30;
        cfg.send_auth_token_cache_size = true;
        cfg.auth_token_cache_size = UINT64_C(1) << 30;
        cfg.send_buffer_size = 1u << 18;
        cfg.setup_auth_tokens = &tok;
        cfg.setup_auth_token_count = 1;
        moq_session_t *c = NULL;
        moq_result_t rc = moq_session_create(&cfg, 0, &c);
        if (rc != MOQ_OK) {
            MOQ_TEST_CHECK_EQ_INT((int)rc, MOQ_ERR_INVAL);
            continue;
        }
        moq_result_t src = moq_session_start(c, 0);
        if (src != MOQ_OK)
            fprintf(stderr, "  value %zu accepted at create, start rc=%d\n", len, (int)src);
        MOQ_TEST_CHECK_EQ_INT((int)src, MOQ_OK);
        moq_session_destroy(c);
        largest_ok = len;
        break;
    }
    MOQ_TEST_CHECK(largest_ok > 65300);
    if (failures) fprintf(stderr, "  in row setup_wire_size (%s)\n", vname(v));
    MOQ_TEST_PASS("create admits exactly the SETUPs that encode");
    return failures;
}


/* -- opaque token values ------------------------------------------------------ */

/* Token values are opaque bytes: an empty value and one holding zero bytes
 * reach the peer exactly, in SETUP from either side and in a request, on both
 * drafts. They are handed to the application's authorization policy, not
 * refused by the session. */
static int row_opaque_values(moq_version_t v)
{
    failures = 0;
    static const uint8_t zeros[] = { 0x00, 0x01, 0x00 };
    for (int on_server = 0; on_server < 2; on_server++) {
        moq_auth_token_t toks[2] = { { 9, { NULL, 0 } }, { 10, { zeros, sizeof(zeros) } } };
        token_side_t t = { .tokens = toks, .count = 2, .on_server = on_server != 0 };
        rl_pair_t p;
        got_tokens_t got;
        MOQ_TEST_CHECK(open_and_collect(&p, v, &t, &got));
        MOQ_TEST_CHECK_EQ_SIZE(got.count, 2);
        MOQ_TEST_CHECK_EQ_U64(got.type[0], 9);
        MOQ_TEST_CHECK_EQ_SIZE(got.len[0], 0);
        MOQ_TEST_CHECK_EQ_U64(got.type[1], 10);
        MOQ_TEST_CHECK_EQ_SIZE(got.len[1], 3);
        MOQ_TEST_CHECK_EQ_INT(got.first[1], 0x00);
        MOQ_TEST_CHECK_EQ_INT(got.last[1], 0x00);

        /* The same values on a request (SUBSCRIBE) the other side receives. */
        if (moq_session_state(p.c) == MOQ_SESS_ESTABLISHED) {
            moq_subscribe_cfg_t sc;
            moq_subscribe_cfg_init(&sc);
            moq_bytes_t ns[1] = { { (const uint8_t *)"op", 2 } };
            sc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 1 };
            sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
            sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
            sc.auth_tokens = toks;
            sc.auth_token_count = 2;
            moq_subscription_t sub;
            moq_session_t *req = on_server ? p.s : p.c;
            moq_session_t *rsp = on_server ? p.c : p.s;
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(req, &sc, p.now, &sub), MOQ_OK);
            shuttle(&p);
            int seen = 0;
            moq_event_t ev;
            while (moq_session_poll_events(rsp, &ev, 1) > 0) {
                if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                    seen++;
                    const moq_subscribe_request_event_t *r = &ev.u.subscribe_request;
                    MOQ_TEST_CHECK_EQ_SIZE(r->token_count, 2);
                    if (r->token_count == 2) {
                        MOQ_TEST_CHECK_EQ_SIZE(r->tokens[0].token_value.len, 0);
                        MOQ_TEST_CHECK(r->tokens[1].token_value.len == 3 &&
                                       memcmp(r->tokens[1].token_value.data, zeros, 3) == 0);
                    }
                }
                moq_event_cleanup(&ev);
            }
            MOQ_TEST_CHECK_EQ_INT(seen, 1);
            MOQ_TEST_CHECK(moq_session_state(rsp) == MOQ_SESS_ESTABLISHED);
        }
        pair_close(&p);
    }
    if (failures) fprintf(stderr, "  in row opaque_values (%s)\n", vname(v));
    MOQ_TEST_PASS("empty and zero-byte token values are opaque");
    return failures;
}

/* -- draft-18 Setup Option codec -------------------------------------------- */

static int row_d18_codec(void)
{
    failures = 0;
    uint8_t buf[256];
    moq_buf_writer_t w;
    moq_d18_setup_opts_t o, d;
    static const uint8_t v1[] = { 1, 2, 3 };

    memset(&o, 0, sizeof(o));
    o.auth_token_count = 2;
    o.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    o.auth_tokens[0].token_type = 7;
    o.auth_tokens[0].token_value = (moq_bytes_t){ v1, sizeof(v1) };
    o.auth_tokens[1].alias_type = MOQ_AUTH_TOKEN_REGISTER;
    o.auth_tokens[1].alias = 4;
    o.auth_tokens[1].token_type = 8;
    o.auth_tokens[1].token_value = (moq_bytes_t){ v1, 1 };
    o.has_max_auth_token_cache_size = true;
    o.max_auth_token_cache_size = 100;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup_opts(&w, &o), MOQ_OK);
    size_t n = moq_buf_writer_offset(&w);
    /* SETUP type 0x2F00 (two vi64 bytes) and the 16-bit length precede the
     * options. */
    MOQ_TEST_CHECK(n > 4);
    memset(&d, 0, sizeof(d));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_setup_opts(buf + 4, n - 4, &d), MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(d.auth_token_count, 2);
    MOQ_TEST_CHECK_EQ_U64(d.auth_tokens[0].token_type, 7);
    MOQ_TEST_CHECK(d.auth_tokens[0].alias_type == MOQ_AUTH_TOKEN_USE_VALUE);
    MOQ_TEST_CHECK_EQ_SIZE(d.auth_tokens[0].token_value.len, 3);
    MOQ_TEST_CHECK(d.auth_tokens[1].alias_type == MOQ_AUTH_TOKEN_REGISTER);
    MOQ_TEST_CHECK_EQ_U64(d.auth_tokens[1].alias, 4);
    MOQ_TEST_CHECK(d.has_max_auth_token_cache_size);
    MOQ_TEST_CHECK_EQ_U64(d.max_auth_token_cache_size, 100);

    /* Nothing is registered before SETUP: an alias reference or a deletion
     * cannot be sent there, nor more than the decoder's 16 tokens. */
    o.auth_tokens[1].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup_opts(&w, &o), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
    o.auth_tokens[1].alias_type = MOQ_AUTH_TOKEN_DELETE;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup_opts(&w, &o), MOQ_ERR_INVAL);
    o.auth_tokens[1].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    o.auth_token_count = MOQ_D18_MAX_AUTH_TOKENS + 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup_opts(&w, &o), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);
    MOQ_TEST_PASS("draft-18 setup option codec carries tokens");
    return failures;
}

int main(void)
{
    int total = 0;
    const moq_version_t vers[2] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18 };
    for (int i = 0; i < 2; i++) {
        total += row_tokens_reach_peer(vers[i]);
        total += row_large_token(vers[i]);
        total += row_owned_copy(vers[i]);
        total += row_refusals(vers[i]);
        total += row_old_struct_size(vers[i]);
        total += row_credential_wiped(vers[i]);
        total += row_late_create_failure(vers[i]);
        total += row_setup_wire_size(vers[i]);
        total += row_opaque_values(vers[i]);
    }
    total += row_d18_codec();
    total += row_d16_token_capacity();
    if (total) fprintf(stderr, "test_session_setup_tokens: %d failure(s)\n", total);
    return total ? 1 : 0;
}
