/* Production sender hook over SimPair: no replacement session or sockets. */
#include <moq/media_sender.h>
#include <moq/media_auth.h>
#include <moq/sim.h>
#include <moq/session.h>
#include <moq/publisher.h>
#include <moq/rcbuf.h>
#include <moq/msf.h>
#include "test_support.h"
#include <stddef.h>
#include <string.h>
#include <time.h>

static int failures;
moq_media_sender_t *moq_media_sender_test_new_cfg(const moq_media_sender_cfg_t *cfg);
void moq_media_sender_test_pump(moq_media_sender_t *s, moq_session_t *session, uint64_t now);
void moq_media_sender_test_free(moq_media_sender_t *s);
bool moq_media_sender_test_auth_unlocked(moq_media_sender_t *s);
bool moq_media_sender_test_auth_failed(moq_media_sender_t *s, moq_media_track_t *t);
void moq_media_sender_test_auth_after_select(
    void (*fn)(moq_media_sender_t *, moq_media_track_t *, void *), void *ctx);
void moq_media_sender_test_set_alloc(const moq_alloc_t *alloc);
moq_rcbuf_t *moq_media_sender_test_auth_catalog(moq_media_sender_t *s);
bool moq_media_sender_test_auth_named_failed(moq_media_sender_t *s, moq_bytes_t name);
size_t moq_media_sender_test_auth_pending(moq_media_sender_t *s);
bool moq_media_sender_test_publish_admission(moq_media_sender_t *s,
    moq_media_track_t *track, uint64_t *attempts, moq_result_t *result);
uint64_t moq_media_sender_test_catalog_group(const moq_media_sender_t *s);
bool moq_media_sender_test_catalog_dirty(const moq_media_sender_t *s);
unsigned moq_media_sender_test_retained_installs(const moq_media_sender_t *s);
moq_media_companion_profile_t moq_media_sender_test_companion(const moq_media_sender_t *s);
void moq_media_sender_test_set_clock_hook(int (*fn)(struct timespec *, void *), void *ctx);
bool moq_media_sender_test_queue_entry(moq_media_sender_t *s, size_t i,
    const moq_media_track_t **track, const moq_rcbuf_t **payload,
    uint64_t *group, bool *starts_group);
bool moq_media_sender_test_has_pending_output(moq_media_sender_t *s);
uint64_t moq_media_sender_test_next_deadline_us(moq_media_sender_t *s);

typedef struct fixture {
    moq_simpair_t *pair;
    moq_media_sender_t *sender;
    moq_media_track_t *track;
    moq_media_track_t *replacement;
    moq_media_track_t *healthy;
    moq_rcbuf_t *queued_failed, *queued_healthy;
    moq_media_track_cfg_t track_cfg;
    moq_bytes_t fail_name;
    unsigned selections, outcomes, namespaces, publications;
    unsigned ready, closed, track_closed;
    unsigned media_publications, generated_publications, healthy_publications, cancelled, live_allocations;
    moq_media_auth_action_t fail_action;
    moq_result_t selector_result;
    bool deny, empty, anonymous, pull, static_source;
    bool required, generated, copy_fail, fail_next_alloc;
    bool native_buffer;
    bool metadata_nomem;
    unsigned metadata_refusals;
    unsigned fail_alloc_after, alloc_refusals;
    size_t refused_alloc_size;
    uint8_t bytes[4];
} fixture_t;

static const uint8_t expected[] = {0x80, 0, 0xff, 0x42};
/* Within the helper's 16 KiB/value budget, beyond the native default 4096-byte
 * control arena. This exercises the real encoder, not a mocked return code. */
static const uint8_t oversized_message_token[5000] = {0x80};
static const moq_bytes_t ns[] = {{(const uint8_t *)"auth", 4}};

static void on_ready(void *ctx, moq_media_sender_t *s)
{
    fixture_t *f = ctx;
    MOQ_TEST_CHECK(s == f->sender && moq_media_sender_test_auth_unlocked(s));
    f->ready++;
}

static void on_closed(void *ctx, moq_media_sender_t *s, bool fatal, uint64_t code)
{
    fixture_t *f = ctx;
    MOQ_TEST_CHECK(s == f->sender && fatal && code != 0);
    MOQ_TEST_CHECK(moq_media_sender_test_auth_unlocked(s));
    f->closed++;
}

static void on_track_closed(void *ctx, moq_media_sender_t *s, moq_media_track_t *t)
{
    (void)s; (void)t;
    ((fixture_t *)ctx)->track_closed++;
}

static bool bytes_equal(moq_bytes_t a, moq_bytes_t b)
{
    return a.len == b.len && (!a.len || memcmp(a.data, b.data, a.len) == 0);
}

static void *fixture_alloc(size_t n, void *ctx)
{
    fixture_t *f = ctx;
    if (f->fail_next_alloc) { f->fail_next_alloc = false; return NULL; }
    if (f->fail_alloc_after && --f->fail_alloc_after == 0) {
        f->alloc_refusals++;
        f->refused_alloc_size = n;
        return NULL;
    }
    const moq_alloc_t *a = moq_alloc_default();
    void *p = a->alloc(n, a->ctx);
    if (p) f->live_allocations++;
    return p;
}

static void fixture_free(void *p, size_t n, void *ctx)
{
    fixture_t *f = ctx;
    if (p) {
        MOQ_TEST_CHECK(f->live_allocations > 0);
        f->live_allocations--;
    }
    const moq_alloc_t *a = moq_alloc_default();
    a->free(p, n, a->ctx);
}

/* Only service allocations use this ledger; session/SimPair keep theirs. */
static void *fixture_realloc(void *p, size_t old, size_t n, void *ctx)
{
    void *q = fixture_alloc(n, ctx);
    if (!q) return NULL;
    if (p) memcpy(q, p, old < n ? old : n);
    fixture_free(p, old, ctx);
    return q;
}

static void replace_after_selection(moq_media_sender_t *s, moq_media_track_t *t, void *ctx)
{
    fixture_t *f = ctx;
    if (t != f->track || f->cancelled) return;
    f->cancelled++;
    MOQ_TEST_CHECK(moq_media_sender_remove_track(s, t) == MOQ_OK);
    MOQ_TEST_CHECK(moq_media_sender_add_track(s, &f->track_cfg, &f->replacement) == MOQ_OK);
    /* Only the old intent was denied. The replacement is independent. */
    f->fail_action = MOQ_MEDIA_AUTH_ACTION_NONE;
}

static void refuse_metadata_after_selection(moq_media_sender_t *s, moq_media_track_t *t, void *ctx)
{
    (void)s;
    fixture_t *f = ctx;
    if (t != f->track || f->metadata_refusals) return;
    f->metadata_refusals++;
    /* Selection is already copied. Refuse the actual facade registration
     * allocation, which must not be reported as a credential copy failure. */
    f->fail_next_alloc = true;
}

static void replace_after_catalog_snapshot(moq_media_sender_t *s, moq_media_track_t *t, void *ctx)
{
    fixture_t *f = ctx;
    if (t == f->track || f->cancelled) return;
    moq_rcbuf_t *catalog = moq_media_sender_test_auth_catalog(s);
    if (!catalog) return;
    moq_rcbuf_decref(catalog);
    replace_after_selection(s, f->track, f);
}

static moq_result_t select_tokens(void *ctx, const moq_media_auth_request_t *req,
    moq_auth_token_t *tokens, size_t capacity, size_t *count,
    moq_media_auth_decision_t *decision)
{
    fixture_t *f = ctx;
    MOQ_TEST_CHECK(moq_media_sender_test_auth_unlocked(f->sender));
    MOQ_TEST_CHECK(req->track_namespace.count == 1 && capacity == 16);
    if (req->track_namespace.count == 1)
        MOQ_TEST_CHECK(bytes_equal(req->track_namespace.parts[0], ns[0]));
    f->selections++;
    bool fail = req->action == f->fail_action &&
        (req->action == MOQ_MEDIA_AUTH_ACTION_PUBLISH_NAMESPACE ||
         bytes_equal(req->track_name, f->fail_name));
    if (fail && f->selector_result != MOQ_OK) return f->selector_result;
    if (fail && f->deny) return MOQ_OK;
    *decision = MOQ_MEDIA_AUTH_ALLOW;
    if (fail && f->empty) return MOQ_OK;
    memcpy(f->bytes, expected, sizeof(expected));
    tokens[0] = (moq_auth_token_t){7, {f->bytes, sizeof(f->bytes)}};
    tokens[1] = (moq_auth_token_t){8, {NULL, 0}};
    *count = 2;
    if (fail && f->native_buffer) {
        tokens[0].token_value = (moq_bytes_t){oversized_message_token, sizeof(oversized_message_token)};
        *count = 1;
    }
    if (fail && f->copy_fail) f->fail_next_alloc = true;
    return MOQ_OK;
}

static void outcome(void *ctx, const moq_media_auth_outcome_info_t *info)
{
    fixture_t *f = ctx;
    f->outcomes++;
    MOQ_TEST_CHECK(moq_media_sender_test_auth_unlocked(f->sender));
    MOQ_TEST_CHECK(info->request->action == f->fail_action);
    bool service = f->required;
    MOQ_TEST_CHECK(info->scope == (service ? MOQ_MEDIA_AUTH_SCOPE_SERVICE : MOQ_MEDIA_AUTH_SCOPE_TRACK));
    MOQ_TEST_CHECK(info->track == (service || f->generated ? NULL : f->track));
    MOQ_TEST_CHECK(bytes_equal(info->request->track_name, f->fail_name));
    MOQ_TEST_CHECK(service ? moq_media_sender_test_auth_failed(f->sender, NULL)
        : moq_media_sender_test_auth_named_failed(f->sender, f->fail_name));
    moq_media_auth_outcome_t expected_outcome = f->deny ? MOQ_MEDIA_AUTH_OUTCOME_POLICY_DENIED
        : (f->empty || f->selector_result == MOQ_ERR_WOULD_BLOCK)
            ? MOQ_MEDIA_AUTH_OUTCOME_INVALID_SELECTION : MOQ_MEDIA_AUTH_OUTCOME_SELECTOR_ERROR;
    moq_result_t result = f->deny ? MOQ_OK : (f->empty || f->selector_result == MOQ_ERR_WOULD_BLOCK)
        ? MOQ_ERR_INVAL : f->selector_result;
    if (f->copy_fail) {
        expected_outcome = MOQ_MEDIA_AUTH_OUTCOME_TOKEN_COPY_FAILED;
        result = MOQ_ERR_NOMEM;
    }
    if (f->native_buffer) {
        expected_outcome = MOQ_MEDIA_AUTH_OUTCOME_TOKEN_LIMIT;
        result = MOQ_ERR_BUFFER;
    }
    MOQ_TEST_CHECK(info->outcome == expected_outcome && info->result == result);
    if (f->queued_failed) MOQ_TEST_CHECK(moq_rcbuf_refcount(f->queued_failed) == 1);
    if (f->queued_healthy) MOQ_TEST_CHECK(moq_rcbuf_refcount(f->queued_healthy) == 2);
}

static void check_tokens(fixture_t *f, const moq_resolved_token_t *tokens, size_t n)
{
    MOQ_TEST_CHECK(n == (f->anonymous ? 0u : 2u));
    if (n != 2) return;
    MOQ_TEST_CHECK(tokens[0].token_type == 7 && tokens[0].token_value.len == sizeof(expected));
    if (tokens[0].token_value.len == sizeof(expected))
        MOQ_TEST_CHECK(memcmp(tokens[0].token_value.data, expected, sizeof(expected)) == 0);
    MOQ_TEST_CHECK(tokens[1].token_type == 8 && tokens[1].token_value.len == 0);
}

static void drive(fixture_t *f)
{
    uint64_t now = moq_simpair_now_us(f->pair);
    moq_media_sender_test_pump(f->sender, moq_simpair_client(f->pair), now);
    memset(f->bytes, 0xee, sizeof(f->bytes));
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(f->pair, 64, NULL) == MOQ_OK);
    moq_session_t *peer = moq_simpair_server(f->pair);
    moq_event_t ev;
    while (moq_session_poll_events(peer, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            f->namespaces++;
            check_tokens(f, ev.u.namespace_published.tokens, ev.u.namespace_published.token_count);
            moq_accept_namespace_cfg_t ac;
            moq_accept_namespace_cfg_init(&ac);
            MOQ_TEST_CHECK(moq_session_accept_namespace(peer, ev.u.namespace_published.ann, &ac, now) == MOQ_OK);
        } else if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            f->publications++;
            moq_bytes_t name = ev.u.publish_request.track_name;
            if (bytes_equal(name, MOQ_BYTES_LITERAL("v"))) f->media_publications++;
            else if (bytes_equal(name, MOQ_BYTES_LITERAL("healthy"))) f->healthy_publications++;
            else if (!bytes_equal(name, MOQ_BYTES_LITERAL("catalog"))) f->generated_publications++;
            if (f->fail_action == MOQ_MEDIA_AUTH_ACTION_PUBLISH)
                MOQ_TEST_CHECK(!bytes_equal(name, f->fail_name));
            check_tokens(f, ev.u.publish_request.tokens, ev.u.publish_request.token_count);
            moq_accept_publish_cfg_t ac;
            moq_accept_publish_cfg_init(&ac);
            MOQ_TEST_CHECK(moq_session_accept_publish(peer, ev.u.publish_request.pub, &ac, now) == MOQ_OK);
        }
        moq_event_cleanup(&ev);
    }
}

static void check_write(fixture_t *f, moq_media_track_t *track, moq_result_t expected_rc)
{
    moq_rcbuf_t *payload = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"data", 4, &payload) == MOQ_OK);
    if (!payload) return;
    /* Keep an independent observer ref even if a broken write takes ownership. */
    moq_media_send_object_t obj = {0};
    obj.struct_size = sizeof(obj);
    obj.payload = moq_rcbuf_incref(payload);
    obj.is_sync = obj.starts_group = obj.ends_group = true;
    obj.has_sap_type = true;
    obj.sap_type = MOQ_SAP_TYPE_1;
    moq_result_t rc = moq_media_sender_write(f->sender, track, &obj);
    MOQ_TEST_CHECK(rc == expected_rc);
    if (rc != MOQ_OK) {
        MOQ_TEST_CHECK(moq_rcbuf_refcount(payload) == 2);
        moq_rcbuf_decref(obj.payload);
    }
    moq_rcbuf_decref(payload);
}

static void check_catalog(fixture_t *f, size_t count)
{
    moq_rcbuf_t *json = moq_media_sender_test_auth_catalog(f->sender);
    MOQ_TEST_CHECK(json != NULL);
    if (!json) return;
    moq_msf_catalog_t cat = {0};
    moq_result_t rc = moq_msf_catalog_parse(moq_alloc_default(),
        (moq_bytes_t){moq_rcbuf_data(json), moq_rcbuf_len(json)}, &cat);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc == MOQ_OK) {
        MOQ_TEST_CHECK(cat.track_count == count);
        for (size_t i = 0; i < cat.track_count; ++i)
            if (f->fail_action == MOQ_MEDIA_AUTH_ACTION_PUBLISH)
                MOQ_TEST_CHECK(!bytes_equal(cat.tracks[i].name, f->fail_name));
        if (f->replacement && cat.track_count == 1)
            MOQ_TEST_CHECK(cat.tracks[0].bitrate == f->track_cfg.bitrate);
        moq_msf_catalog_cleanup(moq_alloc_default(), &cat);
    }
    moq_rcbuf_decref(json);
}

static moq_rcbuf_t *enqueue_observed(fixture_t *f, moq_media_track_t *track)
{
    moq_rcbuf_t *payload = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"queued", 6, &payload) == MOQ_OK);
    if (!payload) return NULL;
    moq_media_send_object_t obj = {0};
    obj.struct_size = sizeof(obj);
    obj.payload = moq_rcbuf_incref(payload);
    obj.is_sync = obj.starts_group = obj.ends_group = true;
    moq_result_t rc = moq_media_sender_write(f->sender, track, &obj);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) moq_rcbuf_decref(obj.payload);
    return payload;
}

static void scenario(moq_version_t version, unsigned mode)
{
    int before = failures;
    fixture_t f = {0};
    f.anonymous = mode == 0;
    f.static_source = mode == 1;
    f.pull = mode == 2;
    f.required = mode == 3 || mode == 9 || mode == 17;
    f.generated = mode == 10 || mode == 11 || mode == 21;
    f.copy_fail = mode == 14;
    f.native_buffer = mode == 16 || mode == 17;
    f.metadata_nomem = mode == 18;
    f.fail_name = mode == 3 || mode == 17 ? (moq_bytes_t){NULL, 0} : mode == 9 ? MOQ_BYTES_LITERAL("catalog")
        : mode == 10 || mode == 21 ? MOQ_BYTES_LITERAL("v.sap") : mode == 11 ? MOQ_BYTES_LITERAL("v.timeline")
        : MOQ_BYTES_LITERAL("v");
    f.fail_action = mode == 3 || mode == 17 ? MOQ_MEDIA_AUTH_ACTION_PUBLISH_NAMESPACE : MOQ_MEDIA_AUTH_ACTION_PUBLISH;
    f.deny = mode == 3 || mode == 4 || (mode >= 9 && mode <= 11) || mode == 13 || mode >= 20;
    f.empty = mode == 5;
    f.selector_result = mode == 6 ? MOQ_ERR_WOULD_BLOCK : mode == 7 ? MOQ_ERR_NOMEM : MOQ_OK;
    if (mode < 3 || mode == 8 || mode == 12 || mode == 15 || mode == 18 || mode == 19)
        f.fail_action = MOQ_MEDIA_AUTH_ACTION_NONE;
    moq_simpair_cfg_t pc = MOQ_SIMPAIR_CFG_INIT;
    pc.alloc = moq_alloc_default();
    pc.version = version;
    pc.seed = 42;
    pc.initial_now_us = 1000;
    pc.client_send_request_capacity = pc.server_send_request_capacity = true;
    pc.client_initial_request_capacity = pc.server_initial_request_capacity = 32;
    pc.max_actions = mode == 8 || mode == 15 ? 1 : 0;
    MOQ_TEST_CHECK(moq_simpair_create(&pc, &f.pair) == MOQ_OK);
    if (!f.pair) return;
    MOQ_TEST_CHECK(moq_simpair_start(f.pair) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(f.pair, 64, NULL) == MOQ_OK);
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(f.pair), &ev, 1) == 1) moq_event_cleanup(&ev);
    while (moq_session_poll_events(moq_simpair_server(f.pair), &ev, 1) == 1) moq_event_cleanup(&ev);
    moq_media_auth_cfg_t auth;
    moq_media_auth_cfg_init(&auth);
    auth.ctx = &f;
    auth.select = select_tokens;
    auth.on_outcome = outcome;
    memcpy(f.bytes, expected, sizeof(expected));
    moq_auth_token_t tokens[] = {{7, {f.bytes, sizeof(f.bytes)}}, {8, {NULL, 0}}};
    if (f.static_source) { auth.select = NULL; auth.tokens = tokens; auth.token_count = 2; }
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ns, 1};
    cfg.publish_tracks = !f.pull;
    cfg.resource_auth = f.anonymous ? NULL : &auth;
    moq_media_sender_callbacks_init_sized(&cfg.callbacks, sizeof(cfg.callbacks));
    cfg.callbacks.ctx = &f;
    cfg.callbacks.on_ready = on_ready;
    cfg.callbacks.on_closed = on_closed;
    cfg.callbacks.on_track_closed = on_track_closed;
    moq_alloc_t alloc = {.alloc = fixture_alloc, .free = fixture_free,
        .realloc = fixture_realloc, .ctx = &f};
    moq_media_sender_test_set_alloc(&alloc);
    f.sender = moq_media_sender_test_new_cfg(&cfg);
    moq_media_sender_test_set_alloc(NULL);
    MOQ_TEST_CHECK(f.sender != NULL);
    if (!f.sender) { moq_simpair_destroy(f.pair); return; }
    memset(f.bytes, 0xee, sizeof(f.bytes));
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init_sized(&tc, sizeof(tc));
    tc.name = MOQ_BYTES_LITERAL("v");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = MOQ_BYTES_LITERAL("av01");
    tc.bitrate = 1000000;
    tc.is_live = true;
    tc.emit_sap_timeline = tc.emit_media_timeline = f.generated;
    f.track_cfg = tc;
    if (mode == 19) f.track_cfg.bitrate = 2000000;
    MOQ_TEST_CHECK(moq_media_sender_add_track(f.sender, &tc, &f.track) == MOQ_OK);
    if (mode == 4 || mode == 14) {
        tc.name = MOQ_BYTES_LITERAL("healthy");
        MOQ_TEST_CHECK(moq_media_sender_add_track(f.sender, &tc, &f.healthy) == MOQ_OK);
        f.queued_failed = enqueue_observed(&f, f.track);
        f.queued_healthy = enqueue_observed(&f, f.healthy);
    }
    if (mode == 12 || mode == 13)
        moq_media_sender_test_auth_after_select(replace_after_selection, &f);
    if (f.metadata_nomem)
        moq_media_sender_test_auth_after_select(refuse_metadata_after_selection, &f);
    if (mode == 19)
        moq_media_sender_test_auth_after_select(replace_after_catalog_snapshot, &f);
    if (mode == 8 || mode == 15) {
        moq_session_t *client = moq_simpair_client(f.pair);
        uint64_t now = moq_simpair_now_us(f.pair);
        moq_media_sender_test_pump(f.sender, client, now);
        unsigned selected = f.selections;
        MOQ_TEST_CHECK(selected == 3);
        MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(f.sender) == 2);
        memset(f.bytes, 0xee, sizeof(f.bytes));
        moq_media_sender_test_pump(f.sender, client, now);
        MOQ_TEST_CHECK(f.selections == selected);
        if (mode == 15) replace_after_selection(f.sender, f.track, &f);
    }
    for (unsigned i = 0; i < 24; ++i) drive(&f);
    moq_media_sender_test_auth_after_select(NULL, NULL);
    bool failed = (mode >= 3 && mode <= 7) || (mode >= 9 && mode <= 11) || mode == 14 || f.native_buffer || mode >= 20;
    bool cancelled = mode == 12 || mode == 13 || mode == 15 || mode == 19;
    MOQ_TEST_CHECK(f.outcomes == (failed ? 1u : 0u));
    MOQ_TEST_CHECK(f.namespaces == (f.required ? 0u : 1u));
    unsigned extra = f.healthy ? 1u : 0u;
    MOQ_TEST_CHECK(f.publications == extra + (mode == 19 ? 1u : 0u) +
        (f.required || f.pull || f.metadata_nomem ? 0u : f.generated ? 3u : failed ? 1u : 2u));
    MOQ_TEST_CHECK(f.selections == (f.anonymous || f.static_source ? 0u : mode == 3 || f.pull ? 1u
        : mode == 9 || mode == 17 ? 2u : f.generated ? 5u : cancelled ? 4u : 3u) + extra);
    MOQ_TEST_CHECK(f.media_publications == (f.required || f.pull || f.metadata_nomem || (failed && !f.generated)
        ? 0u : mode == 19 ? 2u : 1u));
    MOQ_TEST_CHECK(f.generated_publications == (f.generated ? 1u : 0u));
    MOQ_TEST_CHECK(f.healthy_publications == extra);
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(f.sender) == 0);
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(f.pair)) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_media_sender_is_ready(f.sender) == !(f.required || f.metadata_nomem));
    MOQ_TEST_CHECK(f.closed == (f.required || f.metadata_nomem ? 1u : 0u));
    MOQ_TEST_CHECK(f.ready == (f.required || f.metadata_nomem ? 0u : 1u));
    MOQ_TEST_CHECK(f.track_closed == 0);
    if (f.metadata_nomem) {
        MOQ_TEST_CHECK(f.metadata_refusals == 1);
        MOQ_TEST_CHECK(moq_media_sender_is_fatal(f.sender));
    }
    if (mode >= 20) {
        /* A refused optional resource must not be ended or keep a healthy
         * broadcast's completion pending, including a generated timeline. */
        unsigned selected = f.selections;
        MOQ_TEST_CHECK(moq_media_sender_complete(f.sender) == MOQ_OK);
        for (unsigned i = 0; i < 24; ++i) drive(&f);
        moq_rcbuf_t *json = moq_media_sender_test_auth_catalog(f.sender);
        MOQ_TEST_CHECK(json != NULL);
        if (json) {
            moq_msf_catalog_t cat = {0};
            moq_result_t rc = moq_msf_catalog_parse(moq_alloc_default(),
                (moq_bytes_t){moq_rcbuf_data(json), moq_rcbuf_len(json)}, &cat);
            MOQ_TEST_CHECK(rc == MOQ_OK);
            if (rc == MOQ_OK) {
                MOQ_TEST_CHECK(cat.is_complete && cat.track_count == 0);
                moq_msf_catalog_cleanup(moq_alloc_default(), &cat);
            }
            moq_rcbuf_decref(json);
        }
        moq_media_sender_stats_t stats = {0};
        MOQ_TEST_CHECK(moq_media_sender_get_stats(f.sender, &stats, sizeof(stats)) == MOQ_OK);
        MOQ_TEST_CHECK(stats.completion_error == MOQ_OK && !moq_media_sender_is_fatal(f.sender));
        MOQ_TEST_CHECK(f.selections == selected && f.outcomes == 1);
        check_write(&f, f.track, MOQ_ERR_WRONG_STATE);
    } else if (!f.required && !f.metadata_nomem) {
        check_catalog(&f, extra + (f.generated ? 2u : failed ? 0u : 1u));
        check_write(&f, f.track, cancelled || (failed && !f.generated) ? MOQ_ERR_WRONG_STATE : MOQ_OK);
        if (cancelled) {
            MOQ_TEST_CHECK(f.cancelled == 1 && f.replacement != NULL && f.replacement != f.track);
            check_write(&f, f.replacement, MOQ_OK);
        }
        if (failed && !f.generated) {
            MOQ_TEST_CHECK(moq_media_sender_end_track(f.sender, f.track) == MOQ_ERR_WRONG_STATE);
            MOQ_TEST_CHECK(moq_media_sender_remove_track(f.sender, f.track) == MOQ_OK);
        }
        if (f.healthy) check_write(&f, f.healthy, MOQ_OK);
    }
    moq_media_sender_test_free(f.sender);
    moq_simpair_destroy(f.pair);
    MOQ_TEST_CHECK(f.live_allocations == 0 && !f.fail_next_alloc);
    moq_rcbuf_decref(f.queued_failed);
    moq_rcbuf_decref(f.queued_healthy);
    if (failures != before)
        fprintf(stderr, "media_sender_auth: mode=%u version=%u failed\n", mode, (unsigned)version);
}

enum { AUTH_NAMESPACE, AUTH_CATALOG, AUTH_OLD_MEDIA, AUTH_NEW_MEDIA, AUTH_RESOURCES };
enum { CATALOG_WIRE_CAP = 4 };
static const uint64_t d22_group_base = UINT64_C(1700000000123);
static const uint8_t d22_init[] = {1, 2, 3};
static const uint8_t d22_properties[] = {0x78, 0x01};
static const uint8_t d22_dynamic_properties[] = {0x30, 0x01, 0x48, 0x01};

static int admission_clock(struct timespec *out, void *ctx)
{
    (void)ctx;
    out->tv_sec = (time_t)(d22_group_base / 1000u);
    out->tv_nsec = (long)(d22_group_base % 1000u) * 1000000L;
    return 0;
}

typedef struct admission_fixture {
    fixture_t f;
    moq_version_t version;
    uint32_t max_actions;
    uint64_t group_base;
    unsigned selected[AUTH_RESOURCES];
    uint8_t token_bytes[AUTH_RESOURCES][4];
    moq_announcement_t announcement;
    moq_publication_t catalog_pub, old_media_pub;
    unsigned catalog_requests, media_ends, media_subgroups, media_finished, wire_count;
    unsigned catalog_accept_refusals;
    moq_rcbuf_t *wire[CATALOG_WIRE_CAP];
    uint64_t wire_group[CATALOG_WIRE_CAP], wire_object[CATALOG_WIRE_CAP];
    bool capture_publish_bidis;
    unsigned publish_bidis;
    moq_stream_ref_t publish_bidi[2];
} admission_fixture_t;

static void admission_trace(void *ctx, const moq_sim_trace_record_t *record)
{
    admission_fixture_t *a = ctx;
    if (!a->capture_publish_bidis || record->kind != MOQ_SIM_TRACE_ACTION ||
        record->from != MOQ_PERSPECTIVE_CLIENT ||
        record->action_kind != MOQ_ACTION_OPEN_BIDI_STREAM) return;
    bool has_ref = record->struct_size >= offsetof(moq_sim_trace_record_t, stream_ref) +
        sizeof(record->stream_ref);
    MOQ_TEST_CHECK(has_ref && a->publish_bidis < 2);
    if (has_ref && a->publish_bidis < 2)
        a->publish_bidi[a->publish_bidis] = record->stream_ref;
    a->publish_bidis++;
}

static const uint8_t admission_tokens[AUTH_RESOURCES][4] = {
    {0x80, 0, 0xff, 0x10}, {0x80, 0, 0xff, 0x20},
    {0x80, 0, 0xff, 0x30}, {0x80, 0, 0xff, 0x40}
};

static moq_result_t admission_select(void *ctx, const moq_media_auth_request_t *req,
    moq_auth_token_t *tokens, size_t capacity, size_t *count,
    moq_media_auth_decision_t *decision)
{
    admission_fixture_t *a = ctx;
    MOQ_TEST_CHECK(moq_media_sender_test_auth_unlocked(a->f.sender));
    MOQ_TEST_CHECK(req->struct_size >= MOQ_MEDIA_AUTH_REQUEST_V0_SIZE);
    MOQ_TEST_CHECK(capacity == 16 && req->track_namespace.count == 1);
    if (req->track_namespace.count == 1)
        MOQ_TEST_CHECK(bytes_equal(req->track_namespace.parts[0], ns[0]));
    unsigned resource;
    if (req->action == MOQ_MEDIA_AUTH_ACTION_PUBLISH_NAMESPACE) {
        MOQ_TEST_CHECK(req->track_name.len == 0);
        resource = AUTH_NAMESPACE;
    } else if (req->action == MOQ_MEDIA_AUTH_ACTION_PUBLISH &&
               bytes_equal(req->track_name, MOQ_BYTES_LITERAL("catalog"))) {
        resource = AUTH_CATALOG;
    } else if (req->action == MOQ_MEDIA_AUTH_ACTION_PUBLISH &&
               bytes_equal(req->track_name, MOQ_BYTES_LITERAL("v"))) {
        resource = a->f.cancelled ? AUTH_NEW_MEDIA : AUTH_OLD_MEDIA;
    } else if (req->action == MOQ_MEDIA_AUTH_ACTION_PUBLISH &&
               bytes_equal(req->track_name, MOQ_BYTES_LITERAL("late"))) {
        resource = AUTH_NEW_MEDIA;
    } else {
        MOQ_TEST_CHECK(false);
        return MOQ_ERR_INVAL;
    }
    a->selected[resource]++;
    a->f.selections++;
    memcpy(a->token_bytes[resource], admission_tokens[resource], 4);
    tokens[0] = (moq_auth_token_t){
        a->version == MOQ_VERSION_DRAFT_22 ? UINT64_MAX : 7,
        {a->token_bytes[resource], 4}};
    tokens[1] = (moq_auth_token_t){8, {NULL, 0}};
    *count = 2;
    *decision = MOQ_MEDIA_AUTH_ALLOW;
    return MOQ_OK;
}

static void admission_outcome(void *ctx, const moq_media_auth_outcome_info_t *info)
{
    admission_fixture_t *a = ctx;
    (void)info;
    a->f.outcomes++;
    MOQ_TEST_CHECK(moq_media_sender_test_auth_unlocked(a->f.sender));
    MOQ_TEST_CHECK(false);  /* Capacity refusals must never become auth outcomes. */
}

static void admission_check_tokens(const moq_resolved_token_t *tokens, size_t n,
                                    unsigned resource, moq_version_t version)
{
    MOQ_TEST_CHECK(n == 2);
    if (n != 2) return;
    MOQ_TEST_CHECK(tokens[0].token_type == (version == MOQ_VERSION_DRAFT_22 ? UINT64_MAX : 7) &&
        bytes_equal(tokens[0].token_value, (moq_bytes_t){admission_tokens[resource], 4}));
    MOQ_TEST_CHECK(tokens[1].token_type == 8 && tokens[1].token_value.len == 0);
}

/* Flush only transport and peer responses. Startup acceptance is held until
 * the admitted catalog's unchanged snapshot has been inspected. */
static void admission_flush(admission_fixture_t *a)
{
    fixture_t *f = &a->f;
    moq_session_t *peer = moq_simpair_server(f->pair);
    for (unsigned round = 0; round < 4; ++round) {
        MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(f->pair, 64, NULL) == MOQ_OK);
        unsigned drained;
        for (drained = 0; drained < 64; ++drained) {
            moq_event_t ev;
            size_t n = 0;
            MOQ_TEST_CHECK(moq_session_poll_events_ex(peer, &ev, 1, sizeof(ev), &n) == MOQ_OK);
            if (!n) break;
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                MOQ_TEST_CHECK(f->namespaces == 0);
                f->namespaces++;
                a->announcement = ev.u.namespace_published.ann;
                admission_check_tokens(ev.u.namespace_published.tokens,
                    ev.u.namespace_published.token_count, AUTH_NAMESPACE, a->version);
            } else if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                f->publications++;
                if (bytes_equal(ev.u.publish_request.track_name, MOQ_BYTES_LITERAL("catalog"))) {
                    MOQ_TEST_CHECK(a->catalog_requests == 0);
                    a->catalog_requests++;
                    a->catalog_pub = ev.u.publish_request.pub;
                    admission_check_tokens(ev.u.publish_request.tokens,
                        ev.u.publish_request.token_count, AUTH_CATALOG, a->version);
                } else {
                    MOQ_TEST_CHECK(bytes_equal(ev.u.publish_request.track_name, MOQ_BYTES_LITERAL("v")));
                    MOQ_TEST_CHECK(f->media_publications < (f->cancelled ? 2u : 1u));
                    unsigned resource = f->media_publications ? AUTH_NEW_MEDIA : AUTH_OLD_MEDIA;
                    if (!f->media_publications) a->old_media_pub = ev.u.publish_request.pub;
                    f->media_publications++;
                    if (a->version == MOQ_VERSION_DRAFT_22) {
                        MOQ_TEST_CHECK(ev.u.publish_request.dynamic_groups);
                        MOQ_TEST_CHECK(bytes_equal(ev.u.publish_request.track_properties,
                            (moq_bytes_t){d22_dynamic_properties, sizeof(d22_dynamic_properties)}));
                    }
                    admission_check_tokens(ev.u.publish_request.tokens,
                        ev.u.publish_request.token_count, resource, a->version);
                    moq_accept_publish_cfg_t ac;
                    moq_accept_publish_cfg_init(&ac);
                    MOQ_TEST_CHECK(moq_session_accept_publish(peer, ev.u.publish_request.pub,
                        &ac, moq_simpair_now_us(f->pair)) == MOQ_OK);
                }
            } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                const moq_object_received_event_t *o = &ev.u.object_received;
                if (moq_publication_is_valid(a->catalog_pub) &&
                    moq_publication_eq(o->pub, a->catalog_pub)) {
                    MOQ_TEST_CHECK(o->status == MOQ_OBJECT_NORMAL && o->payload != NULL);
                    if (o->status == MOQ_OBJECT_NORMAL && o->payload) {
                        unsigned i = a->wire_count++;
                        MOQ_TEST_CHECK(i < CATALOG_WIRE_CAP);
                        if (i < CATALOG_WIRE_CAP) {
                            a->wire[i] = moq_rcbuf_incref(o->payload);
                            a->wire_group[i] = o->group_id;
                            a->wire_object[i] = o->object_id;
                        }
                    }
                } else if (f->cancelled && moq_publication_is_valid(a->old_media_pub) &&
                           moq_publication_eq(o->pub, a->old_media_pub)) {
                    MOQ_TEST_CHECK(o->status == MOQ_OBJECT_END_OF_TRACK && o->payload == NULL);
                    MOQ_TEST_CHECK(o->group_id == 0 && o->object_id == 0);
                    MOQ_TEST_CHECK(a->media_ends++ == 0);
                    MOQ_TEST_CHECK(a->media_subgroups == 0 && a->media_finished == 0);
                } else MOQ_TEST_CHECK(false);
            } else if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
                const moq_subgroup_finished_event_t *sg = &ev.u.subgroup_finished;
                if (f->cancelled && moq_publication_is_valid(a->old_media_pub) &&
                    moq_publication_eq(sg->pub, a->old_media_pub)) {
                    MOQ_TEST_CHECK(sg->group_id == 0 && sg->subgroup_id == 0);
                    MOQ_TEST_CHECK(ev.detail_size >= offsetof(moq_subgroup_finished_event_t,
                        last_object_id) + sizeof(sg->last_object_id));
                    MOQ_TEST_CHECK(sg->has_last_object && sg->last_object_id == 0);
                    MOQ_TEST_CHECK(a->media_ends == 1 && a->media_finished == 0);
                    MOQ_TEST_CHECK(a->media_subgroups++ == 0);
                } else {
                    MOQ_TEST_CHECK(moq_publication_is_valid(a->catalog_pub) &&
                        moq_publication_eq(sg->pub, a->catalog_pub));
                }
            } else if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) {
                MOQ_TEST_CHECK(f->cancelled && moq_publication_is_valid(a->old_media_pub) &&
                    moq_publication_eq(ev.u.publish_finished.pub, a->old_media_pub));
                MOQ_TEST_CHECK(ev.u.publish_finished.status_code == MOQ_PUB_DONE_TRACK_ENDED &&
                    ev.u.publish_finished.stream_count == 1 &&
                    ev.u.publish_finished.cause == MOQ_REQUEST_TERMINAL_NONE);
                MOQ_TEST_CHECK(a->media_ends == 1 && a->media_subgroups == 1);
                MOQ_TEST_CHECK(a->media_finished++ == 0);
            } else {
                MOQ_TEST_CHECK(false);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(drained < 64);
    }
}

static void admission_pump(admission_fixture_t *a)
{
    moq_media_sender_test_pump(a->f.sender, moq_simpair_client(a->f.pair),
                              moq_simpair_now_us(a->f.pair));
    memset(a->token_bytes, 0xee, sizeof(a->token_bytes));
}

static bool admission_setup_options(admission_fixture_t *a, moq_version_t version,
    uint64_t peer_capacity, uint32_t max_actions, bool anonymous, bool pull)
{
    memset(a, 0, sizeof(*a));
    a->version = version;
    a->max_actions = max_actions;
    a->group_base = version == MOQ_VERSION_DRAFT_22 ? d22_group_base : 0;
    fixture_t *f = &a->f;
    f->anonymous = anonymous;
    f->pull = pull;
    moq_simpair_cfg_t pc = MOQ_SIMPAIR_CFG_INIT;
    pc.alloc = moq_alloc_default();
    pc.version = version;
    pc.seed = 42;
    pc.initial_now_us = 1000;
    pc.client_send_request_capacity = pc.server_send_request_capacity = true;
    pc.client_initial_request_capacity = 32;
    pc.server_initial_request_capacity = peer_capacity;
    pc.max_actions = max_actions;
    pc.trace_fn = admission_trace;
    pc.trace_ctx = a;
    MOQ_TEST_CHECK(moq_simpair_create(&pc, &f->pair) == MOQ_OK);
    if (!f->pair) return false;
    MOQ_TEST_CHECK(moq_simpair_start(f->pair) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(f->pair, 64, NULL) == MOQ_OK);
    moq_session_t *sessions[] = {moq_simpair_client(f->pair), moq_simpair_server(f->pair)};
    for (size_t i = 0; i < 2; ++i) {
        unsigned drained;
        for (drained = 0; drained < 64; ++drained) {
            moq_event_t ev;
            size_t n = 0;
            MOQ_TEST_CHECK(moq_session_poll_events_ex(sessions[i], &ev, 1, sizeof(ev), &n) == MOQ_OK);
            if (!n) break;
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SETUP_COMPLETE);
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(drained < 64);
    }
    moq_media_auth_cfg_t auth;
    moq_media_auth_cfg_init(&auth);
    auth.ctx = a;
    auth.select = admission_select;
    auth.on_outcome = admission_outcome;
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){ns, 1};
    cfg.publish_tracks = !pull;
    cfg.resource_auth = anonymous ? NULL : &auth;
    moq_media_sender_callbacks_init_sized(&cfg.callbacks, sizeof(cfg.callbacks));
    cfg.callbacks.ctx = f;
    cfg.callbacks.on_ready = on_ready;
    cfg.callbacks.on_closed = on_closed;
    cfg.callbacks.on_track_closed = on_track_closed;
    moq_alloc_t alloc = {.alloc = fixture_alloc, .free = fixture_free,
        .realloc = fixture_realloc, .ctx = f};
    moq_media_sender_test_set_alloc(&alloc);
    moq_media_sender_test_set_clock_hook(admission_clock, NULL);
    f->sender = moq_media_sender_test_new_cfg(&cfg);
    moq_media_sender_test_set_clock_hook(NULL, NULL);
    moq_media_sender_test_set_alloc(NULL);
    MOQ_TEST_CHECK(f->sender != NULL);
    if (!f->sender) return false;
    MOQ_TEST_CHECK(moq_media_sender_test_companion(f->sender) == MOQ_MEDIA_COMPANION_AUTO);
    moq_media_track_cfg_init_sized(&f->track_cfg, sizeof(f->track_cfg));
    f->track_cfg.name = MOQ_BYTES_LITERAL("v");
    f->track_cfg.media_type = MOQ_MEDIA_TYPE_VIDEO;
    f->track_cfg.packaging = MOQ_MEDIA_PACKAGING_RAW;
    f->track_cfg.codec = MOQ_BYTES_LITERAL("av01");
    f->track_cfg.bitrate = 1000000;
    f->track_cfg.is_live = true;
    if (version == MOQ_VERSION_DRAFT_22) {
        f->track_cfg.init_data = (moq_bytes_t){d22_init, sizeof(d22_init)};
        f->track_cfg.has_label = true;
        f->track_cfg.label = MOQ_BYTES_LITERAL("Camera 31");
        f->track_cfg.has_render_group = true;
        f->track_cfg.render_group = 31;
        f->track_cfg.track_properties = (moq_bytes_t){d22_properties, sizeof(d22_properties)};
        f->track_cfg.dynamic_groups = true;
    }
    moq_result_t rc = moq_media_sender_add_track(f->sender, &f->track_cfg, &f->track);
    MOQ_TEST_CHECK(rc == MOQ_OK && f->track != NULL);
    return rc == MOQ_OK && f->track != NULL;
}

static bool admission_setup(admission_fixture_t *a, moq_version_t version,
                            uint64_t peer_capacity, uint32_t max_actions)
{
    return admission_setup_options(a, version, peer_capacity, max_actions, false, false);
}

static void admission_cleanup(admission_fixture_t *a)
{
    moq_media_sender_test_free(a->f.sender);
    moq_simpair_destroy(a->f.pair);
    for (unsigned i = 0; i < CATALOG_WIRE_CAP; ++i) moq_rcbuf_decref(a->wire[i]);
    MOQ_TEST_CHECK(a->f.live_allocations == 0 && !a->f.fail_next_alloc);
}

static void admission_expect(admission_fixture_t *a, moq_media_track_t *track,
                              uint64_t attempts, moq_result_t result)
{
    uint64_t observed = 0;
    moq_result_t rc = MOQ_ERR_INTERNAL;
    MOQ_TEST_CHECK(moq_media_sender_test_publish_admission(a->f.sender, track, &observed, &rc));
    MOQ_TEST_CHECK_EQ_U64(observed, attempts);
    if (attempts) MOQ_TEST_CHECK_EQ_INT(rc, result);
}

static void admission_check_selection(admission_fixture_t *a, bool replaced)
{
    MOQ_TEST_CHECK(a->selected[AUTH_NAMESPACE] == 1);
    MOQ_TEST_CHECK(a->selected[AUTH_CATALOG] == 1);
    MOQ_TEST_CHECK(a->selected[AUTH_OLD_MEDIA] == 1);
    MOQ_TEST_CHECK(a->selected[AUTH_NEW_MEDIA] == (replaced ? 1u : 0u));
    MOQ_TEST_CHECK(a->f.selections == (replaced ? 4u : 3u));
    MOQ_TEST_CHECK(a->f.outcomes == 0 && a->f.closed == 0);
}

static void admission_grant_d16(admission_fixture_t *a, uint64_t capacity)
{
    moq_session_t *peer = moq_simpair_server(a->f.pair);
    MOQ_TEST_CHECK(moq_session_version(peer) == MOQ_VERSION_DRAFT_16);
    MOQ_TEST_CHECK(moq_session_grant_request_capacity(peer, capacity,
        moq_simpair_now_us(a->f.pair)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_local_request_capacity(peer) == capacity);
    admission_flush(a);
    MOQ_TEST_CHECK(moq_session_peer_request_capacity(moq_simpair_client(a->f.pair)) == capacity);
}

static moq_bytes_t admission_bytes(moq_rcbuf_t *buf)
{
    return (moq_bytes_t){moq_rcbuf_data(buf), moq_rcbuf_len(buf)};
}

static moq_rcbuf_t *admission_snapshot(admission_fixture_t *a)
{
    moq_rcbuf_t *current = moq_media_sender_test_auth_catalog(a->f.sender);
    moq_rcbuf_t *copy = NULL;
    MOQ_TEST_CHECK(current != NULL);
    if (current) {
        MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), moq_rcbuf_data(current),
            moq_rcbuf_len(current), &copy) == MOQ_OK);
        moq_rcbuf_decref(current);
    }
    return copy;
}

static void admission_check_track(const moq_msf_catalog_t *cat, uint64_t bitrate)
{
    MOQ_TEST_CHECK(cat->delta_update == NULL && cat->track_count == 1);
    if (cat->track_count == 1) {
        MOQ_TEST_CHECK(bytes_equal(cat->tracks[0].name, MOQ_BYTES_LITERAL("v")));
        MOQ_TEST_CHECK(cat->tracks[0].bitrate == bitrate);
    }
}

static void admission_check_catalog(moq_rcbuf_t *buf, uint64_t bitrate, bool d22)
{
    MOQ_TEST_CHECK(buf != NULL);
    if (!buf) return;
    moq_msf_catalog_t cat = {0};
    moq_result_t rc = moq_msf_catalog_parse(moq_alloc_default(), admission_bytes(buf), &cat);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc == MOQ_OK) {
        admission_check_track(&cat, bitrate);
        if (d22) {
            MOQ_TEST_CHECK(cat.version_form == MOQ_MSF_VERSION_FORM_DRAFT_NAME);
            MOQ_TEST_CHECK(cat.init_data_count == 1);
            if (cat.init_data_count == 1) {
                MOQ_TEST_CHECK(bytes_equal(cat.init_data_list[0].id, MOQ_BYTES_LITERAL("v")));
                MOQ_TEST_CHECK(bytes_equal(cat.init_data_list[0].type, MOQ_BYTES_LITERAL("inline")));
                MOQ_TEST_CHECK(bytes_equal(cat.init_data_list[0].data, MOQ_BYTES_LITERAL("AQID")));
            }
            if (cat.track_count == 1) {
                const moq_msf_track_t *t = &cat.tracks[0];
                MOQ_TEST_CHECK(t->has_init_ref && bytes_equal(t->init_ref, MOQ_BYTES_LITERAL("v")));
                MOQ_TEST_CHECK(!t->has_init_data);
                MOQ_TEST_CHECK(t->has_label && bytes_equal(t->label, MOQ_BYTES_LITERAL("Camera 31")));
                MOQ_TEST_CHECK(t->has_render_group && t->render_group == 31);
            }
        }
    }
    moq_msf_catalog_cleanup(moq_alloc_default(), &cat);
}

static void admission_check_snapshot(admission_fixture_t *a, moq_rcbuf_t *snapshot, bool dirty)
{
    moq_rcbuf_t *current = moq_media_sender_test_auth_catalog(a->f.sender);
    MOQ_TEST_CHECK(snapshot != NULL && current != NULL);
    if (snapshot && current)
        MOQ_TEST_CHECK(bytes_equal(admission_bytes(snapshot), admission_bytes(current)));
    moq_rcbuf_decref(current);
    MOQ_TEST_CHECK(moq_media_sender_test_catalog_group(a->f.sender) == a->group_base);
    MOQ_TEST_CHECK(moq_media_sender_test_retained_installs(a->f.sender) == 1);
    MOQ_TEST_CHECK(moq_media_sender_test_catalog_dirty(a->f.sender) == dirty);
    MOQ_TEST_CHECK(!moq_media_sender_is_ready(a->f.sender) && a->f.ready == 0);
}

static bool admission_accept_startup(admission_fixture_t *a)
{
    int before = failures;
    MOQ_TEST_CHECK(a->f.namespaces == 1 && a->catalog_requests == 1 &&
        moq_announcement_is_valid(a->announcement) && moq_publication_is_valid(a->catalog_pub));
    if (failures != before) return false;
    moq_session_t *peer = moq_simpair_server(a->f.pair);
    uint64_t now = moq_simpair_now_us(a->f.pair);
    moq_accept_namespace_cfg_t nc;
    moq_accept_namespace_cfg_init(&nc);
    moq_result_t accepted = moq_session_accept_namespace(peer, a->announcement, &nc, now);
    MOQ_TEST_CHECK_EQ_INT(accepted, MOQ_OK);
    if (accepted != MOQ_OK) return false;
    moq_accept_publish_cfg_t pc;
    moq_accept_publish_cfg_init(&pc);
    accepted = moq_session_accept_publish(peer, a->catalog_pub, &pc, now);
    if (a->max_actions == 1) {
        /* The namespace response occupies the peer's sole action slot. Its
         * drain must precede the catalog response, without pumping the sender. */
        MOQ_TEST_CHECK_EQ_INT(accepted, MOQ_ERR_WOULD_BLOCK);
        if (failures != before) return false;
        if (accepted == MOQ_ERR_WOULD_BLOCK) {
            a->catalog_accept_refusals++;
            admission_flush(a);
            if (failures != before) return false;
            accepted = moq_session_accept_publish(peer, a->catalog_pub, &pc,
                moq_simpair_now_us(a->f.pair));
        }
    }
    MOQ_TEST_CHECK_EQ_INT(accepted, MOQ_OK);
    if (accepted != MOQ_OK) return false;
    admission_flush(a);
    if (failures != before) return false;
    for (unsigned i = 0; i < 24; ++i) {
        admission_pump(a);
        admission_flush(a);
        if (failures != before) return false;
    }
    MOQ_TEST_CHECK(a->f.ready == 1 && moq_media_sender_is_ready(a->f.sender));
    MOQ_TEST_CHECK(a->f.outcomes == 0 && a->f.closed == 0 && a->f.track_closed == 0);
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(a->f.sender) == 0);
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(a->f.pair)) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(peer) == MOQ_SESS_ESTABLISHED);
    return failures == before;
}

static void test_d16_media_credit_retention(void)
{
    int before = failures;
    admission_fixture_t a;
    if (!admission_setup(&a, MOQ_VERSION_DRAFT_16, 2, 0)) goto done;
    MOQ_TEST_CHECK(moq_session_peer_request_capacity(moq_simpair_client(a.f.pair)) == 2);
    admission_pump(&a);
    admission_expect(&a, a.f.track, 1, MOQ_ERR_REQUEST_BLOCKED);
    admission_expect(&a, NULL, 0, MOQ_OK);
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(a.f.sender) == 2);
    admission_pump(&a);
    admission_expect(&a, a.f.track, 2, MOQ_ERR_REQUEST_BLOCKED);
    admission_check_selection(&a, false);
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(a.f.sender) == 2);
    admission_grant_d16(&a, 6);
    admission_pump(&a);
    admission_expect(&a, a.f.track, 3, MOQ_OK);
    admission_expect(&a, NULL, 1, MOQ_OK);
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(a.f.sender) == 0);
    admission_flush(&a);
    MOQ_TEST_CHECK(a.f.namespaces == 1 && a.f.media_publications == 1 &&
                   a.catalog_requests == 1 && a.f.publications == 2);
    if (failures != before || !admission_accept_startup(&a)) goto done;
    admission_check_selection(&a, false);
    admission_expect(&a, a.f.track, 3, MOQ_OK);
    admission_expect(&a, NULL, 1, MOQ_OK);
    MOQ_TEST_CHECK(a.wire_count == 1 && a.wire_group[0] == 0 && a.wire_object[0] == 0);
    if (a.wire_count == 1) admission_check_catalog(a.wire[0], 1000000, false);
    check_write(&a.f, a.f.track, MOQ_OK);
done:
    admission_cleanup(&a);
    if (failures != before) fprintf(stderr, "media_sender_auth: d16_media_credit_retention failed\n");
}

static void admission_check_correction(admission_fixture_t *a, moq_rcbuf_t *snapshot)
{
    if (a->version == MOQ_VERSION_DRAFT_22) {
        /* An init-bearing add stays a full independent MSF-01 generation. */
        MOQ_TEST_CHECK(a->wire_count == 2);
        if (a->wire_count != 2 || !snapshot) return;
        MOQ_TEST_CHECK(a->wire_group[0] == a->group_base && a->wire_object[0] == 0);
        MOQ_TEST_CHECK(a->wire_group[1] == a->group_base + 1 && a->wire_object[1] == 0);
        MOQ_TEST_CHECK(bytes_equal(admission_bytes(a->wire[0]), admission_bytes(snapshot)));
        admission_check_catalog(a->wire[1], 2000000, true);
        return;
    }
    MOQ_TEST_CHECK(a->wire_count == CATALOG_WIRE_CAP);
    if (a->wire_count != CATALOG_WIRE_CAP || !snapshot) return;
    for (unsigned i = 0; i < CATALOG_WIRE_CAP; ++i) {
        MOQ_TEST_CHECK(a->wire_group[i] == (i ? 1u : 0u));
        MOQ_TEST_CHECK(a->wire_object[i] == (i ? i - 1u : 0u));
        MOQ_TEST_CHECK(a->wire[i] != NULL);
        if (!a->wire[i]) return;
    }
    MOQ_TEST_CHECK(bytes_equal(admission_bytes(a->wire[0]), admission_bytes(snapshot)));
    MOQ_TEST_CHECK(bytes_equal(admission_bytes(a->wire[1]), admission_bytes(snapshot)));
    moq_msf_catalog_t effective = {0};
    moq_result_t rc = moq_msf_catalog_parse(moq_alloc_default(), admission_bytes(a->wire[1]), &effective);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return;
    admission_check_track(&effective, 1000000);
    for (unsigned i = 2; i < CATALOG_WIRE_CAP; ++i) {
        moq_msf_catalog_t delta = {0}, next = {0};
        rc = moq_msf_catalog_parse(moq_alloc_default(), admission_bytes(a->wire[i]), &delta);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        if (rc == MOQ_OK) {
            MOQ_TEST_CHECK(delta.delta_update != NULL && delta.delta_update_count == 1);
            if (delta.delta_update && delta.delta_update_count == 1) {
                const moq_msf_delta_op_t *op = &delta.delta_update[0];
                MOQ_TEST_CHECK(op->op == (i == 2 ? MOQ_MSF_DELTA_OP_REMOVE : MOQ_MSF_DELTA_OP_ADD));
                MOQ_TEST_CHECK(op->track_count == 1);
                if (op->track_count == 1)
                    MOQ_TEST_CHECK(bytes_equal(op->tracks[0].name, MOQ_BYTES_LITERAL("v")));
            }
            rc = moq_msf_catalog_apply_delta(moq_alloc_default(), &effective, &delta, &next);
            MOQ_TEST_CHECK(rc == MOQ_OK);
        }
        moq_msf_catalog_cleanup(moq_alloc_default(), &delta);
        moq_msf_catalog_cleanup(moq_alloc_default(), &effective);
        effective = next;
        if (rc != MOQ_OK) break;
        if (i == 2) MOQ_TEST_CHECK(effective.track_count == 0);
    }
    if (rc == MOQ_OK) admission_check_track(&effective, 2000000);
    moq_msf_catalog_cleanup(moq_alloc_default(), &effective);
}

static void test_catalog_admission_snapshot(moq_version_t version, const char *name)
{
    int before = failures;
    bool credit = version == MOQ_VERSION_DRAFT_16;
    moq_result_t refusal = credit ? MOQ_ERR_REQUEST_BLOCKED : MOQ_ERR_WOULD_BLOCK;
    admission_fixture_t a;
    moq_rcbuf_t *snapshot = NULL;
    if (!admission_setup(&a, version, credit ? 4 : 32, credit ? 0 : 2)) goto done;
    admission_pump(&a);
    admission_expect(&a, a.f.track, 1, MOQ_OK);
    admission_expect(&a, NULL, 1, refusal);
    snapshot = admission_snapshot(&a);
    admission_check_catalog(snapshot, 1000000, version == MOQ_VERSION_DRAFT_22);
    admission_check_snapshot(&a, snapshot, false);
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(a.f.sender) == 1);
    admission_pump(&a);
    admission_expect(&a, NULL, 2, refusal);
    admission_check_selection(&a, false);
    admission_check_snapshot(&a, snapshot, false);
    admission_flush(&a);
    MOQ_TEST_CHECK(a.f.namespaces == 1 && a.f.media_publications == 1 && a.catalog_requests == 0);
    if (failures != before) goto done;

    MOQ_TEST_CHECK(moq_media_sender_remove_track(a.f.sender, a.f.track) == MOQ_OK);
    a.f.cancelled = 1;
    a.f.track_cfg.bitrate = 2000000;
    MOQ_TEST_CHECK(moq_media_sender_add_track(a.f.sender, &a.f.track_cfg, &a.f.replacement) == MOQ_OK);
    MOQ_TEST_CHECK(a.f.replacement != NULL && a.f.replacement != a.f.track);
    check_write(&a.f, a.f.track, MOQ_ERR_WRONG_STATE);
    if (failures != before) goto done;
    if (credit) {
        /* Retiring the old publication first opens its END_OF_TRACK stream.
         * Resolve that admission through the real peer before expecting the
         * replacement's first attempt. Keep its request credit blocked and
         * stop as soon as that attempt occurs, so no retry is hidden. */
        for (unsigned i = 0; i < 24; ++i) {
            admission_pump(&a);
            admission_flush(&a);
            uint64_t attempts = 0;
            moq_result_t rc = MOQ_ERR_INTERNAL;
            MOQ_TEST_CHECK(moq_media_sender_test_publish_admission(
                a.f.sender, a.f.replacement, &attempts, &rc));
            if (failures != before) goto done;
            if (attempts) break;
        }
        MOQ_TEST_CHECK(a.media_ends == 1 && a.media_subgroups == 1 && a.media_finished == 1);
        admission_expect(&a, a.f.replacement, 1, MOQ_ERR_REQUEST_BLOCKED);
        admission_expect(&a, NULL, 2, refusal);
        admission_check_snapshot(&a, snapshot, true);
        if (failures != before) goto done;
        /* Admit replacement ID 4, but keep catalog ID 6 credit-blocked. */
        admission_grant_d16(&a, 6);
        admission_pump(&a);
        admission_expect(&a, a.f.track, 1, MOQ_OK);
        admission_expect(&a, a.f.replacement, 2, MOQ_OK);
        admission_expect(&a, NULL, 3, refusal);
        admission_check_selection(&a, true);
        admission_check_snapshot(&a, snapshot, true);
        admission_pump(&a);
        admission_expect(&a, NULL, 4, refusal);
        admission_expect(&a, a.f.track, 1, MOQ_OK);
        admission_expect(&a, a.f.replacement, 2, MOQ_OK);
        admission_check_selection(&a, true);
        admission_check_snapshot(&a, snapshot, true);
        MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(a.f.sender) == 1);
        if (failures != before) goto done;
        admission_grant_d16(&a, 8);
    } else {
        /* Two slots admit the old publication's terminal header + status.
         * Without a transport drain, FIN cannot advance or start replacement. */
        for (unsigned retry = 0; retry < 2; ++retry) {
            admission_pump(&a);
            admission_expect(&a, a.f.track, 1, MOQ_OK);
            admission_expect(&a, a.f.replacement, 0, MOQ_OK);
            admission_expect(&a, NULL, 2, refusal);
            admission_check_selection(&a, false);
            admission_check_snapshot(&a, snapshot, true);
            MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(a.f.sender) == 1);
            MOQ_TEST_CHECK(a.media_ends == 0 && a.media_subgroups == 0 && a.media_finished == 0);
            if (failures != before) goto done;
        }
        admission_flush(&a);
        MOQ_TEST_CHECK(a.media_ends == 1 && a.media_subgroups == 0 && a.media_finished == 0);
        MOQ_TEST_CHECK(a.wire_count == 0 && a.catalog_requests == 0 && a.f.media_publications == 1);
        admission_check_snapshot(&a, snapshot, true);
        if (failures != before) goto done;

        /* FIN + done consume both slots next. Old retirement completes, but
         * the newly selected replacement's actual PUBLISH is refused. */
        admission_pump(&a);
        admission_expect(&a, a.f.track, 1, MOQ_OK);
        admission_expect(&a, a.f.replacement, 1, refusal);
        admission_expect(&a, NULL, 2, refusal);
        admission_check_selection(&a, true);
        admission_check_snapshot(&a, snapshot, true);
        MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(a.f.sender) == 2);
        MOQ_TEST_CHECK(a.media_ends == 1 && a.media_subgroups == 0 && a.media_finished == 0);
        if (failures != before) goto done;
        admission_flush(&a);
        MOQ_TEST_CHECK(a.media_ends == 1 && a.media_subgroups == 1 && a.media_finished == 1);
        MOQ_TEST_CHECK(a.wire_count == 0 && a.catalog_requests == 0 && a.f.media_publications == 1);
        admission_check_snapshot(&a, snapshot, true);
        if (failures != before) goto done;
    }

    uint64_t catalog_attempts = credit ? 5u : 3u;
    admission_pump(&a);
    admission_expect(&a, a.f.track, 1, MOQ_OK);
    admission_expect(&a, a.f.replacement, 2, MOQ_OK);
    admission_expect(&a, NULL, catalog_attempts, MOQ_OK);
    admission_check_selection(&a, true);
    admission_check_snapshot(&a, snapshot, true);
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(a.f.sender) == 0);
    if (failures != before) goto done;
    admission_flush(&a);
    MOQ_TEST_CHECK(a.f.namespaces == 1 && a.f.media_publications == 2 &&
                   a.catalog_requests == 1 && a.f.publications == 3);
    if (failures != before || !admission_accept_startup(&a)) goto done;
    admission_check_selection(&a, true);
    admission_expect(&a, a.f.track, 1, MOQ_OK);
    admission_expect(&a, a.f.replacement, 2, MOQ_OK);
    admission_expect(&a, NULL, catalog_attempts, MOQ_OK);
    MOQ_TEST_CHECK(a.media_ends == 1 && a.media_subgroups == 1 && a.media_finished == 1);
    MOQ_TEST_CHECK(moq_media_sender_test_catalog_group(a.f.sender) == a.group_base + 1);
    MOQ_TEST_CHECK(moq_media_sender_test_retained_installs(a.f.sender) == 2);
    MOQ_TEST_CHECK(!moq_media_sender_test_catalog_dirty(a.f.sender));
    moq_rcbuf_t *corrected = moq_media_sender_test_auth_catalog(a.f.sender);
    admission_check_catalog(corrected, 2000000, version == MOQ_VERSION_DRAFT_22);
    moq_rcbuf_decref(corrected);
    admission_check_correction(&a, snapshot);
    check_write(&a.f, a.f.track, MOQ_ERR_WRONG_STATE);
    check_write(&a.f, a.f.replacement, MOQ_OK);
done:
    moq_rcbuf_decref(snapshot);
    admission_cleanup(&a);
    if (failures != before) fprintf(stderr, "media_sender_auth: %s failed\n", name);
}

static void test_d16_catalog_credit_snapshot(void)
{
    test_catalog_admission_snapshot(MOQ_VERSION_DRAFT_16, "d16_catalog_credit_snapshot");
}

static void test_d22_media_action_retention(void)
{
    int before = failures;
    admission_fixture_t a;
    if (!admission_setup(&a, MOQ_VERSION_DRAFT_22, 32, 1)) goto done;
    admission_pump(&a);
    MOQ_TEST_CHECK(moq_media_sender_test_companion(a.f.sender) ==
        MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC);
    admission_expect(&a, a.f.track, 1, MOQ_ERR_WOULD_BLOCK);
    admission_expect(&a, NULL, 0, MOQ_OK);
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(a.f.sender) == 2);
    admission_pump(&a);
    admission_expect(&a, a.f.track, 2, MOQ_ERR_WOULD_BLOCK);
    admission_check_selection(&a, false);
    admission_flush(&a);
    MOQ_TEST_CHECK(a.f.namespaces == 1 && a.f.publications == 0);
    admission_pump(&a);
    admission_expect(&a, a.f.track, 3, MOQ_OK);
    admission_expect(&a, NULL, 1, MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(a.f.sender) == 1);
    admission_pump(&a);
    admission_expect(&a, NULL, 2, MOQ_ERR_WOULD_BLOCK);
    admission_check_selection(&a, false);
    admission_flush(&a);
    MOQ_TEST_CHECK(a.f.media_publications == 1 && a.catalog_requests == 0);
    admission_pump(&a);
    admission_expect(&a, a.f.track, 3, MOQ_OK);
    admission_expect(&a, NULL, 3, MOQ_OK);
    admission_flush(&a);
    MOQ_TEST_CHECK(a.f.namespaces == 1 && a.f.publications == 2 && a.catalog_requests == 1);
    if (failures != before || !admission_accept_startup(&a)) goto done;
    admission_check_selection(&a, false);
    MOQ_TEST_CHECK(a.wire_count == 1 && a.wire_group[0] == a.group_base &&
        a.wire_object[0] == 0);
    MOQ_TEST_CHECK(a.catalog_accept_refusals == 1);
    if (a.wire_count == 1) admission_check_catalog(a.wire[0], 1000000, true);
done:
    admission_cleanup(&a);
    if (failures != before) fprintf(stderr, "media_sender_auth: d22_media_action_retention failed\n");
}

static void test_d18_catalog_action_admission_snapshot(void)
{
    test_catalog_admission_snapshot(MOQ_VERSION_DRAFT_18, "d18_catalog_action_admission_snapshot");
}

typedef struct pending_publication_fixture {
    admission_fixture_t a;
    moq_media_track_t *late;
    moq_publication_t late_pub;
    moq_subscription_t healthy_sub;
    unsigned late_requests, late_finished;
    unsigned healthy_objects, healthy_ends, healthy_finished, subscribe_oks;
    unsigned terminal_catalogs;
    bool hold_late_ack, hold_timeline_ack, timeline_enabled;
    moq_publication_t timeline_pub;
    moq_subscription_t late_sub;
    unsigned late_acks, late_ends, late_subgroups;
    unsigned late_sub_oks, late_sub_done;
    unsigned timeline_requests, timeline_acks, timeline_ends, timeline_subgroups, timeline_finished;
} pending_publication_fixture_t;

static bool pending_accept(pending_publication_fixture_t *p,
                            moq_publication_t pub, const moq_accept_publish_cfg_t *cfg)
{
    moq_accept_publish_cfg_t defaults;
    if (!cfg) {
        moq_accept_publish_cfg_init_sized(&defaults, sizeof(defaults));
        cfg = &defaults;
    }
    moq_result_t rc = moq_session_accept_publish(moq_simpair_server(p->a.f.pair),
        pub, cfg, moq_simpair_now_us(p->a.f.pair));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return false;
    if (moq_publication_eq(pub, p->late_pub)) p->late_acks++;
    if (moq_publication_eq(pub, p->timeline_pub)) p->timeline_acks++;
    return true;
}

static void pending_tokens(pending_publication_fixture_t *p,
    const moq_resolved_token_t *tokens, size_t count, unsigned resource)
{
    if (p->a.f.anonymous) MOQ_TEST_CHECK(count == 0);
    else admission_check_tokens(tokens, count, resource, p->a.version);
}

/* Unlike the snapshot rows, these peers accept startup immediately. Transport
 * and peer replies can advance without pumping the sender's retained intent. */
static void pending_flush(pending_publication_fixture_t *p)
{
    fixture_t *f = &p->a.f;
    moq_session_t *peer = moq_simpair_server(f->pair);
    for (unsigned round = 0; round < 4; ++round) {
        MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(f->pair, 64, NULL) == MOQ_OK);
        unsigned drained;
        for (drained = 0; drained < 64; ++drained) {
            moq_event_t ev;
            size_t n = 0;
            MOQ_TEST_CHECK(moq_session_poll_events_ex(peer, &ev, 1, sizeof(ev), &n) == MOQ_OK);
            if (!n) break;
            uint64_t now = moq_simpair_now_us(f->pair);
            if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                MOQ_TEST_CHECK(f->namespaces++ == 0);
                pending_tokens(p, ev.u.namespace_published.tokens,
                    ev.u.namespace_published.token_count, AUTH_NAMESPACE);
                moq_accept_namespace_cfg_t cfg;
                moq_accept_namespace_cfg_init(&cfg);
                MOQ_TEST_CHECK(moq_session_accept_namespace(peer,
                    ev.u.namespace_published.ann, &cfg, now) == MOQ_OK);
            } else if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                MOQ_TEST_CHECK(!f->pull);
                f->publications++;
                moq_bytes_t name = ev.u.publish_request.track_name;
                unsigned resource = AUTH_NEW_MEDIA;
                if (bytes_equal(name, MOQ_BYTES_LITERAL("catalog"))) {
                    resource = AUTH_CATALOG;
                    MOQ_TEST_CHECK(p->a.catalog_requests++ == 0);
                    p->a.catalog_pub = ev.u.publish_request.pub;
                } else if (bytes_equal(name, MOQ_BYTES_LITERAL("v"))) {
                    resource = AUTH_OLD_MEDIA;
                    MOQ_TEST_CHECK(f->media_publications++ == 0);
                    p->a.old_media_pub = ev.u.publish_request.pub;
                } else if (bytes_equal(name, MOQ_BYTES_LITERAL("late.sap"))) {
                    MOQ_TEST_CHECK(p->timeline_enabled && f->anonymous);
                    MOQ_TEST_CHECK(p->timeline_requests++ == 0);
                    p->timeline_pub = ev.u.publish_request.pub;
                } else {
                    MOQ_TEST_CHECK(bytes_equal(name, MOQ_BYTES_LITERAL("late")));
                    MOQ_TEST_CHECK(p->late_requests++ == 0);
                    p->late_pub = ev.u.publish_request.pub;
                }
                pending_tokens(p, ev.u.publish_request.tokens,
                    ev.u.publish_request.token_count, resource);
                bool hold = (moq_publication_eq(ev.u.publish_request.pub, p->late_pub) &&
                             p->hold_late_ack) ||
                            (moq_publication_eq(ev.u.publish_request.pub, p->timeline_pub) &&
                             p->hold_timeline_ack);
                if (!hold) (void)pending_accept(p, ev.u.publish_request.pub, NULL);
            } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                const moq_object_received_event_t *o = &ev.u.object_received;
                if (p->a.catalog_pub._opaque && moq_publication_eq(o->pub, p->a.catalog_pub)) {
                    MOQ_TEST_CHECK(o->status == MOQ_OBJECT_NORMAL && o->payload != NULL);
                    if (o->payload) {
                        moq_msf_catalog_t cat = {0};
                        moq_result_t rc = moq_msf_catalog_parse(moq_alloc_default(),
                            admission_bytes(o->payload), &cat);
                        MOQ_TEST_CHECK(rc == MOQ_OK);
                        if (rc == MOQ_OK && cat.is_complete) p->terminal_catalogs++;
                        moq_msf_catalog_cleanup(moq_alloc_default(), &cat);
                    }
                } else if ((f->pull && p->healthy_sub._opaque &&
                            moq_subscription_eq(o->sub, p->healthy_sub)) ||
                           (!f->pull && p->a.old_media_pub._opaque &&
                            moq_publication_eq(o->pub, p->a.old_media_pub))) {
                    if (o->status == MOQ_OBJECT_END_OF_TRACK) p->healthy_ends++;
                    else {
                        MOQ_TEST_CHECK(o->status == MOQ_OBJECT_NORMAL && o->payload != NULL);
                        if (o->payload)
                            MOQ_TEST_CHECK(bytes_equal(admission_bytes(o->payload), MOQ_BYTES_LITERAL("queued")));
                        p->healthy_objects++;
                    }
                } else if (p->timeline_pub._opaque &&
                           moq_publication_eq(o->pub, p->timeline_pub)) {
                    MOQ_TEST_CHECK(o->status == MOQ_OBJECT_END_OF_TRACK && !o->payload);
                    p->timeline_ends++;
                } else {
                    MOQ_TEST_CHECK(p->late_pub._opaque && moq_publication_eq(o->pub, p->late_pub));
                    MOQ_TEST_CHECK(o->status == MOQ_OBJECT_END_OF_TRACK && !o->payload);
                    p->late_ends++;
                }
            } else if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED) {
                MOQ_TEST_CHECK(ev.u.publish_finished.status_code == MOQ_PUB_DONE_TRACK_ENDED);
                MOQ_TEST_CHECK(ev.u.publish_finished.cause == MOQ_REQUEST_TERMINAL_NONE);
                if (p->a.old_media_pub._opaque &&
                    moq_publication_eq(ev.u.publish_finished.pub, p->a.old_media_pub))
                    p->healthy_finished++;
                else if (p->timeline_pub._opaque &&
                         moq_publication_eq(ev.u.publish_finished.pub, p->timeline_pub)) {
                    MOQ_TEST_CHECK(ev.u.publish_finished.stream_count == p->timeline_ends);
                    p->timeline_finished++;
                } else {
                    MOQ_TEST_CHECK(p->late_pub._opaque &&
                        moq_publication_eq(ev.u.publish_finished.pub, p->late_pub));
                    MOQ_TEST_CHECK(ev.u.publish_finished.stream_count == p->late_ends);
                    p->late_finished++;
                }
            } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
                if (p->late_sub._opaque && moq_subscription_eq(ev.u.subscribe_ok.sub, p->late_sub))
                    p->late_sub_oks++;
                else {
                    MOQ_TEST_CHECK(f->pull &&
                        moq_subscription_eq(ev.u.subscribe_ok.sub, p->healthy_sub));
                    p->subscribe_oks++;
                }
            } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
                if (p->late_sub._opaque && moq_subscription_eq(ev.u.subscribe_done.sub, p->late_sub)) {
                    MOQ_TEST_CHECK(ev.u.subscribe_done.status_code == MOQ_PUB_DONE_TRACK_ENDED);
                    MOQ_TEST_CHECK(ev.u.subscribe_done.stream_count == 0);
                    p->late_sub_done++;
                } else {
                    MOQ_TEST_CHECK(f->pull &&
                        moq_subscription_eq(ev.u.subscribe_done.sub, p->healthy_sub));
                    p->healthy_finished++;
                }
            } else if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
                if (p->late_pub._opaque &&
                    moq_publication_eq(ev.u.subgroup_finished.pub, p->late_pub))
                    p->late_subgroups++;
                else if (p->timeline_pub._opaque &&
                         moq_publication_eq(ev.u.subgroup_finished.pub, p->timeline_pub))
                    p->timeline_subgroups++;
                else MOQ_TEST_CHECK((p->a.catalog_pub._opaque &&
                        moq_publication_eq(ev.u.subgroup_finished.pub, p->a.catalog_pub)) ||
                    (p->a.old_media_pub._opaque &&
                        moq_publication_eq(ev.u.subgroup_finished.pub, p->a.old_media_pub)) ||
                    (f->pull && p->healthy_sub._opaque &&
                        moq_subscription_eq(ev.u.subgroup_finished.sub, p->healthy_sub)));
            } else if (ev.kind != MOQ_EVENT_PUBLICATION_UPDATE_OK) {
                MOQ_TEST_CHECK(false);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(drained < 64);
    }
}

static void pending_drive(pending_publication_fixture_t *p)
{
    admission_pump(&p->a);
    pending_flush(p);
}

static void pending_healthy_demand(pending_publication_fixture_t *p, bool forward)
{
    fixture_t *f = &p->a.f;
    moq_session_t *peer = moq_simpair_server(f->pair);
    uint64_t now = moq_simpair_now_us(f->pair);
    if (f->pull) {
        MOQ_TEST_CHECK(forward && !p->healthy_sub._opaque);
        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){ns, 1};
        cfg.track_name = MOQ_BYTES_LITERAL("v");
        cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        cfg.has_forward = cfg.forward = true;
        MOQ_TEST_CHECK(moq_session_subscribe(peer, &cfg, now, &p->healthy_sub) == MOQ_OK);
    } else {
        moq_publication_update_cfg_t cfg;
        moq_publication_update_cfg_init(&cfg);
        cfg.has_forward = true;
        cfg.forward = forward;
        MOQ_TEST_CHECK(moq_session_update_publication(peer, p->a.old_media_pub, &cfg, now) == MOQ_OK);
    }
    for (unsigned i = 0; i < 8; ++i) pending_drive(p);
    if (f->pull) MOQ_TEST_CHECK(p->subscribe_oks == 1);
}

static void pending_catalog(pending_publication_fixture_t *p, bool complete)
{
    moq_rcbuf_t *json = moq_media_sender_test_auth_catalog(p->a.f.sender);
    MOQ_TEST_CHECK(json != NULL);
    if (!json) return;
    moq_msf_catalog_t cat = {0};
    moq_result_t rc = moq_msf_catalog_parse(moq_alloc_default(), admission_bytes(json), &cat);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc == MOQ_OK) {
        MOQ_TEST_CHECK(cat.is_complete == complete);
        if (complete) MOQ_TEST_CHECK(cat.track_count == 0);
    }
    moq_msf_catalog_cleanup(moq_alloc_default(), &cat);
    moq_rcbuf_decref(json);
}

static void pending_live(pending_publication_fixture_t *p)
{
    fixture_t *f = &p->a.f;
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(f->sender));
    MOQ_TEST_CHECK(f->closed == 0 && f->outcomes == 0);
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(f->pair)) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(f->pair)) == MOQ_SESS_ESTABLISHED);
}

/* D16 request IDs 0/2/4 admit namespace, healthy media and catalog. ID 6 is
 * refused by real peer credit, independently of action-queue room. Modes:
 * selected credentials, anonymous push, and pull (no PUBLISH obligation). */
static void test_post_ready_pending_terminal(bool complete, unsigned mode)
{
    int before = failures;
    pending_publication_fixture_t p = {0};
    fixture_t *f = &p.a.f;
    moq_rcbuf_t *second = NULL;
    if (!admission_setup_options(&p.a, MOQ_VERSION_DRAFT_16, 6, 0,
                                  mode == 1, mode == 2)) goto done;
    for (unsigned i = 0; i < 24; ++i) pending_drive(&p);
    MOQ_TEST_CHECK(f->ready == 1 && moq_media_sender_is_ready(f->sender));
    MOQ_TEST_CHECK(f->namespaces == 1 && f->publications == (f->pull ? 0u : 2u));
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(f->sender) == 0);
    if (failures != before) goto done;  /* Stop before checking unestablished state. */

    if (complete && !f->pull) pending_healthy_demand(&p, false);
    if (!complete && f->pull) pending_healthy_demand(&p, true);
    f->queued_healthy = enqueue_observed(f, f->track);
    MOQ_TEST_CHECK(f->queued_healthy != NULL);
    if (!f->queued_healthy) goto done;
    MOQ_TEST_CHECK(moq_rcbuf_refcount(f->queued_healthy) == 2);
    moq_media_track_cfg_t tc = f->track_cfg;
    tc.name = MOQ_BYTES_LITERAL("late");
    MOQ_TEST_CHECK(moq_media_sender_add_track(f->sender, &tc, &p.late) == MOQ_OK);
    if (!p.late) goto done;

    /* Observe an actual refused PUBLISH BEFORE end/complete is requested. */
    pending_drive(&p);
    admission_expect(&p.a, p.late, f->pull ? 0 : 1, MOQ_ERR_REQUEST_BLOCKED);
    MOQ_TEST_CHECK(moq_session_peer_request_capacity(moq_simpair_client(f->pair)) == 6);
    MOQ_TEST_CHECK(p.late_requests == 0 && p.late_finished == 0);
    MOQ_TEST_CHECK(p.healthy_objects == (complete ? 0u : 1u));
    MOQ_TEST_CHECK(moq_rcbuf_refcount(f->queued_healthy) == (complete ? 2u : 1u));
    pending_live(&p);
    if (failures != before) goto done;

    MOQ_TEST_CHECK((complete ? moq_media_sender_complete(f->sender)
                            : moq_media_sender_end_track(f->sender, p.late)) == MOQ_OK);
    for (unsigned retry = 0; retry < 2; ++retry) {
        pending_drive(&p);
        admission_expect(&p.a, p.late, f->pull ? 0 : 2 + retry, MOQ_ERR_REQUEST_BLOCKED);
        /* The empty late track cannot end locally ahead of its admission.
         * Completion instead keeps the healthy payload ahead of finalization. */
        const moq_media_track_t *queued_track = NULL;
        const moq_rcbuf_t *queued_payload = NULL;
        bool queued = moq_media_sender_test_queue_entry(f->sender, 0,
            &queued_track, &queued_payload, NULL, NULL);
        if (complete) {
            MOQ_TEST_CHECK(queued && queued_track == f->track && queued_payload == f->queued_healthy);
            MOQ_TEST_CHECK(moq_rcbuf_refcount(f->queued_healthy) == 2);
        } else if (!f->pull) {
            MOQ_TEST_CHECK(queued && queued_track == p.late && queued_payload == NULL);
        } else MOQ_TEST_CHECK(!queued);
        pending_live(&p);
        pending_catalog(&p, false);
    }
    MOQ_TEST_CHECK(p.a.selected[AUTH_NEW_MEDIA] == (f->anonymous || f->pull ? 0u : 1u));
    MOQ_TEST_CHECK(f->selections == (f->anonymous ? 0u : f->pull ? 1u : 4u));
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(f->sender) == (f->pull ? 0u : 1u));
    MOQ_TEST_CHECK(p.terminal_catalogs == 0 && p.healthy_ends == 0 &&
        p.healthy_finished == 0 && p.late_requests == 0 && p.late_finished == 0);
    if (failures != before) goto done;  /* Preserve the refused-admission failure, not cleanup noise. */
    check_write(f, p.late, MOQ_ERR_WRONG_STATE);
    if (complete) check_write(f, f->track, MOQ_ERR_WRONG_STATE);

    if (!f->pull) {
        MOQ_TEST_CHECK(moq_session_grant_request_capacity(moq_simpair_server(f->pair), 8,
            moq_simpair_now_us(f->pair)) == MOQ_OK);
        pending_flush(&p);
        MOQ_TEST_CHECK(moq_session_peer_request_capacity(moq_simpair_client(f->pair)) == 8);
    }
    for (unsigned i = 0; i < 24; ++i) pending_drive(&p);
    admission_expect(&p.a, p.late, f->pull ? 0 : 4, MOQ_OK);
    MOQ_TEST_CHECK(p.late_requests == (f->pull ? 0u : 1u));
    MOQ_TEST_CHECK(p.late_finished == (f->pull ? 0u : 1u));
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(f->sender) == 0);
    pending_live(&p);
    pending_catalog(&p, false);
    MOQ_TEST_CHECK(p.terminal_catalogs == 0 && p.healthy_finished == 0);
    if (complete) {
        MOQ_TEST_CHECK(moq_rcbuf_refcount(f->queued_healthy) == 2 && p.healthy_objects == 0);
        pending_healthy_demand(&p, true);
    } else {
        second = enqueue_observed(f, f->track);
        MOQ_TEST_CHECK(second != NULL);
    }
    for (unsigned i = 0; i < 24; ++i) pending_drive(&p);
    pending_live(&p);
    pending_catalog(&p, complete);
    MOQ_TEST_CHECK(p.healthy_objects == (complete ? 1u : 2u));
    MOQ_TEST_CHECK(p.healthy_ends == (complete ? 1u : 0u));
    MOQ_TEST_CHECK(p.healthy_finished == (complete ? 1u : 0u));
    MOQ_TEST_CHECK(p.terminal_catalogs == (complete && !f->pull ? 1u : 0u));
    MOQ_TEST_CHECK(moq_rcbuf_refcount(f->queued_healthy) == 1);
    if (second) MOQ_TEST_CHECK(moq_rcbuf_refcount(second) == 1);
    moq_media_sender_stats_t stats = {0};
    MOQ_TEST_CHECK(moq_media_sender_get_stats(f->sender, &stats, sizeof(stats)) == MOQ_OK);
    MOQ_TEST_CHECK(stats.objects_written == (complete ? 1u : 2u));
    MOQ_TEST_CHECK(stats.objects_sent == stats.objects_written && stats.objects_queued == 0);
    MOQ_TEST_CHECK(stats.objects_dropped == 0 && stats.completion_error == MOQ_OK);
    MOQ_TEST_CHECK(f->selections == (f->anonymous ? 0u : f->pull ? 1u : 4u));
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(f->sender) == 0);
done:
    admission_cleanup(&p.a);
    if (f->queued_healthy) MOQ_TEST_CHECK(moq_rcbuf_refcount(f->queued_healthy) == 1);
    if (second) MOQ_TEST_CHECK(moq_rcbuf_refcount(second) == 1);
    moq_rcbuf_decref(f->queued_healthy);
    moq_rcbuf_decref(second);
    if (failures != before)
        fprintf(stderr, "media_sender_auth: post_ready_pending_%s mode=%u failed\n",
            complete ? "complete" : "end_track", mode);
}

static bool terminal_setup(pending_publication_fixture_t *p, moq_version_t version,
                            uint32_t max_actions, bool anonymous)
{
    int before = failures;
    if (!admission_setup_options(&p->a, version, 32, max_actions, anonymous, false))
        return false;
    for (unsigned i = 0; i < 24 && failures == before; ++i) pending_drive(p);
    if (failures != before) return false;
    MOQ_TEST_CHECK(p->a.f.ready == 1 && moq_media_sender_is_ready(p->a.f.sender));
    MOQ_TEST_CHECK(p->a.f.namespaces == 1 && p->a.f.publications == 2);
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(p->a.f.sender) == 0);
    MOQ_TEST_CHECK(!moq_media_sender_test_has_pending_output(p->a.f.sender));
    pending_live(p);
    return failures == before;
}

static bool terminal_add_late(pending_publication_fixture_t *p, bool timeline)
{
    moq_media_track_cfg_t tc = p->a.f.track_cfg;
    tc.name = MOQ_BYTES_LITERAL("late");
    tc.emit_sap_timeline = timeline;
    p->timeline_enabled = timeline;
    moq_result_t rc = moq_media_sender_add_track(p->a.f.sender, &tc, &p->late);
    MOQ_TEST_CHECK(rc == MOQ_OK && p->late != NULL);
    return rc == MOQ_OK && p->late != NULL;
}

static void terminal_marker(pending_publication_fixture_t *p, bool present)
{
    const moq_media_track_t *track = NULL;
    const moq_rcbuf_t *payload = NULL;
    bool queued = moq_media_sender_test_queue_entry(p->a.f.sender, 0,
        &track, &payload, NULL, NULL);
    MOQ_TEST_CHECK(queued == present);
    if (queued) MOQ_TEST_CHECK(track == p->late && payload == NULL);
    MOQ_TEST_CHECK(!moq_media_sender_test_queue_entry(p->a.f.sender, 1,
        NULL, NULL, NULL, NULL));
}

static void terminal_waiting(pending_publication_fixture_t *p)
{
    pending_live(p);
    pending_catalog(p, false);
    MOQ_TEST_CHECK(p->terminal_catalogs == 0);
    MOQ_TEST_CHECK(moq_media_sender_test_has_pending_output(p->a.f.sender));
    MOQ_TEST_CHECK(moq_media_sender_test_next_deadline_us(p->a.f.sender) == UINT64_MAX);
}

static void terminal_stats(pending_publication_fixture_t *p, uint64_t written)
{
    moq_media_sender_stats_t stats = {0};
    MOQ_TEST_CHECK(moq_media_sender_get_stats(p->a.f.sender, &stats, sizeof(stats)) == MOQ_OK);
    MOQ_TEST_CHECK(stats.objects_written == written && stats.objects_sent == written);
    MOQ_TEST_CHECK(stats.objects_queued == 0 && stats.objects_dropped == 0);
    MOQ_TEST_CHECK(stats.completion_error == MOQ_OK);
    if (p->a.f.queued_healthy)
        MOQ_TEST_CHECK(moq_rcbuf_refcount(p->a.f.queued_healthy) == 1);
}

static void terminal_cleanup(pending_publication_fixture_t *p, int before, const char *name)
{
    MOQ_TEST_CHECK(p->a.f.fail_alloc_after == 0);
    admission_cleanup(&p->a);
    if (p->a.f.queued_healthy)
        MOQ_TEST_CHECK(moq_rcbuf_refcount(p->a.f.queued_healthy) == 1);
    moq_rcbuf_decref(p->a.f.queued_healthy);
    if (failures != before) fprintf(stderr, "media_sender_auth: %s failed\n", name);
}

/* Native admission succeeds, but the real peer retains the request without an
 * ACK. The end must wait even though the credentials no longer need retaining.
 * Forward=0 on the eventual ACK resolves admission without creating demand. */
static void test_withheld_ack_terminal(bool complete)
{
    int before = failures;
    pending_publication_fixture_t p = {0};
    fixture_t *f = &p.a.f;
    if (!terminal_setup(&p, MOQ_VERSION_DRAFT_16, 0, false)) goto done;
    p.hold_late_ack = true;
    if (!terminal_add_late(&p, false)) goto done;
    pending_drive(&p);
    admission_expect(&p.a, p.late, 1, MOQ_OK);
    MOQ_TEST_CHECK(p.late_requests == 1 && p.late_acks == 0);
    MOQ_TEST_CHECK(moq_publication_is_valid(p.late_pub));
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(f->sender) == 0);
    if (failures != before) goto done;

    /* In the marker row, put healthy media BEHIND the blocked end. */
    if (!complete) MOQ_TEST_CHECK(moq_media_sender_end_track(f->sender, p.late) == MOQ_OK);
    f->queued_healthy = enqueue_observed(f, f->track);
    MOQ_TEST_CHECK(f->queued_healthy != NULL);
    if (complete) MOQ_TEST_CHECK(moq_media_sender_complete(f->sender) == MOQ_OK);
    if (failures != before) goto done;
    /* Healthy completion needs the hook after its first-header admission;
     * the late publication's ACK remains withheld throughout these drives. */
    for (unsigned i = 0; complete && i < 24 && p.healthy_finished == 0; ++i)
        pending_drive(&p);
    for (unsigned i = 0; i < 2; ++i) {
        pending_drive(&p);
        terminal_marker(&p, !complete);
        terminal_waiting(&p);
        admission_expect(&p.a, p.late, 1, MOQ_OK);
        MOQ_TEST_CHECK(p.late_acks == 0 && p.late_ends == 0 && p.late_finished == 0);
        MOQ_TEST_CHECK(p.healthy_objects == 1 && moq_rcbuf_refcount(f->queued_healthy) == 1);
        MOQ_TEST_CHECK(p.healthy_ends == (complete ? 1u : 0u));
        MOQ_TEST_CHECK(p.healthy_finished == (complete ? 1u : 0u));
    }
    MOQ_TEST_CHECK(f->selections == 4 && p.a.selected[AUTH_NEW_MEDIA] == 1);
    if (failures != before) goto done;
    check_write(f, p.late, MOQ_ERR_WRONG_STATE);
    if (complete) check_write(f, f->track, MOQ_ERR_WRONG_STATE);
    moq_accept_publish_cfg_t ack;
    moq_accept_publish_cfg_init_sized(&ack, sizeof(ack));
    ack.has_forward = true;
    ack.forward = false;
    if (!pending_accept(&p, p.late_pub, &ack)) goto done;
    pending_flush(&p);   /* Deliver the ACK; only the next hook may consume it. */
    for (unsigned i = 0; i < 24; ++i) pending_drive(&p);
    terminal_marker(&p, false);
    pending_live(&p);
    pending_catalog(&p, complete);
    admission_expect(&p.a, p.late, 1, MOQ_OK);
    MOQ_TEST_CHECK(p.late_requests == 1 && p.late_acks == 1);
    MOQ_TEST_CHECK(p.late_ends == 0 && p.late_finished == 1);
    MOQ_TEST_CHECK(p.terminal_catalogs == (complete ? 1u : 0u));
    MOQ_TEST_CHECK(f->selections == 4 && f->track_closed == 0);
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(f->sender) == 0);
    MOQ_TEST_CHECK(!moq_media_sender_test_has_pending_output(f->sender));
    terminal_stats(&p, 1);
done:
    terminal_cleanup(&p, before, complete ? "withheld_ack_complete" : "withheld_ack_end_track");
}

/* Empty publication plus a real Forward=0 pull subscription. With two action
 * slots: header/status, then FIN/PUBLISH_DONE, then SUBSCRIBE_DONE. The marker
 * must survive the middle phase even though local unpublish cleared publish_ok. */
static void test_partial_end_retry(void)
{
    int before = failures;
    pending_publication_fixture_t p = {0};
    fixture_t *f = &p.a.f;
    if (!terminal_setup(&p, MOQ_VERSION_DRAFT_16, 2, false)) goto done;
    if (!terminal_add_late(&p, false)) goto done;
    for (unsigned i = 0; i < 24; ++i) pending_drive(&p);
    admission_expect(&p.a, p.late, 1, MOQ_OK);
    MOQ_TEST_CHECK(p.late_requests == 1 && p.late_acks == 1);
    if (failures != before) goto done;

    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ns, 1};
    sc.track_name = MOQ_BYTES_LITERAL("late");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    sc.has_forward = true;
    sc.forward = false;
    MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_server(f->pair), &sc,
        moq_simpair_now_us(f->pair), &p.late_sub) == MOQ_OK);
    if (failures != before) goto done;
    for (unsigned i = 0; i < 8; ++i) pending_drive(&p);
    MOQ_TEST_CHECK(p.late_sub_oks == 1 && p.late_sub_done == 0);
    MOQ_TEST_CHECK(!moq_media_sender_test_has_pending_output(f->sender));
    MOQ_TEST_CHECK(moq_session_action_capacity(moq_simpair_client(f->pair)) == 2);
    if (failures != before) goto done;

    MOQ_TEST_CHECK(moq_media_sender_end_track(f->sender, p.late) == MOQ_OK);
    if (failures != before) goto done;
    admission_pump(&p.a);
    MOQ_TEST_CHECK(moq_session_action_capacity(moq_simpair_client(f->pair)) == 0);
    terminal_marker(&p, true);
    pending_flush(&p);
    MOQ_TEST_CHECK(p.late_ends == 1 && p.late_subgroups == 0 && p.late_finished == 0);
    MOQ_TEST_CHECK(p.late_sub_done == 0);
    terminal_waiting(&p);
    if (failures != before) goto done;

    admission_pump(&p.a);
    MOQ_TEST_CHECK(moq_session_action_capacity(moq_simpair_client(f->pair)) == 0);
    terminal_marker(&p, true);
    pending_flush(&p);
    MOQ_TEST_CHECK(p.late_ends == 1 && p.late_subgroups == 1 && p.late_finished == 1);
    MOQ_TEST_CHECK(p.late_sub_done == 0);
    terminal_waiting(&p);
    if (failures != before) goto done;  /* Require the post-unpublish partial state. */

    pending_drive(&p);
    terminal_marker(&p, false);
    MOQ_TEST_CHECK(p.late_sub_done == 1);
    f->queued_healthy = enqueue_observed(f, f->track);
    MOQ_TEST_CHECK(f->queued_healthy != NULL);
    for (unsigned i = 0; i < 24; ++i) pending_drive(&p);
    MOQ_TEST_CHECK(p.late_ends == 1 && p.late_subgroups == 1 && p.late_finished == 1);
    MOQ_TEST_CHECK(p.late_sub_done == 1 && p.healthy_objects == 1 && p.healthy_finished == 0);
    MOQ_TEST_CHECK(f->selections == 4 && f->track_closed == 0);
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(f->sender) == 0);
    MOQ_TEST_CHECK(!moq_media_sender_test_has_pending_output(f->sender));
    admission_expect(&p.a, p.late, 1, MOQ_OK);
    pending_live(&p);
    pending_catalog(&p, false);
    terminal_stats(&p, 1);
done:
    terminal_cleanup(&p, before, "partial_end_after_unpublish");
}

/* Generated tracks receive no app on_track_closed callback. Check their ACK
 * latch separately from an actual peer cancellation of both media and timeline
 * publications; cancellation must not cause republish or strand a local end. */
static void test_generated_resolution(bool peer_finished)
{
    int before = failures;
    pending_publication_fixture_t p = {0};
    fixture_t *f = &p.a.f;
    if (!terminal_setup(&p, peer_finished ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_22,
                         0, true)) goto done;
    p.hold_timeline_ack = !peer_finished;
    if (!terminal_add_late(&p, true)) goto done;
    for (unsigned i = 0; i < 24; ++i) pending_drive(&p);
    admission_expect(&p.a, p.late, 1, MOQ_OK);
    MOQ_TEST_CHECK(p.late_requests == 1 && p.late_acks == 1);
    MOQ_TEST_CHECK(p.timeline_requests == 1 && p.timeline_acks == (peer_finished ? 1u : 0u));
    MOQ_TEST_CHECK(f->publications == 4 && f->track_closed == 0);
    if (failures != before) goto done;

    if (peer_finished) {
        moq_session_t *peer = moq_simpair_server(f->pair);
        MOQ_TEST_CHECK(moq_session_cancel_publication(peer, p.late_pub, 0,
            moq_simpair_now_us(f->pair)) == MOQ_OK);
        if (failures != before) goto done;
        MOQ_TEST_CHECK(moq_session_cancel_publication(peer, p.timeline_pub, 0,
            moq_simpair_now_us(f->pair)) == MOQ_OK);
        if (failures != before) goto done;
        pending_flush(&p);
        for (unsigned i = 0; i < 4; ++i) pending_drive(&p);
        MOQ_TEST_CHECK(f->track_closed == 1);   /* Media only; no generated notification. */
        MOQ_TEST_CHECK(p.late_finished == 0 && p.timeline_finished == 0);
        pending_live(&p);
        if (failures != before) goto done;
        MOQ_TEST_CHECK(moq_media_sender_end_track(f->sender, p.late) == MOQ_OK);
    }
    f->queued_healthy = enqueue_observed(f, f->track);
    MOQ_TEST_CHECK(f->queued_healthy != NULL);
    MOQ_TEST_CHECK(moq_media_sender_complete(f->sender) == MOQ_OK);
    if (failures != before) goto done;
    for (unsigned i = 0; i < 4; ++i) pending_drive(&p);
    if (!peer_finished) {
        terminal_waiting(&p);
        MOQ_TEST_CHECK(p.healthy_finished == 1 && p.late_finished == 1);
        MOQ_TEST_CHECK(p.timeline_acks == 0 && p.timeline_ends == 0 && p.timeline_finished == 0);
        MOQ_TEST_CHECK(p.timeline_subgroups == 0);
        MOQ_TEST_CHECK(f->track_closed == 0);
        if (failures != before) goto done;
        /* D22 PUBLISH_OK cannot override the initial PUBLISH Forward=1. */
        if (!pending_accept(&p, p.timeline_pub, NULL)) goto done;
        pending_flush(&p);
    }
    for (unsigned i = 0; i < 24; ++i) pending_drive(&p);
    terminal_marker(&p, false);
    pending_live(&p);
    pending_catalog(&p, true);
    MOQ_TEST_CHECK(p.terminal_catalogs == 1 && p.healthy_objects == 1 && p.healthy_finished == 1);
    MOQ_TEST_CHECK(p.late_finished == (peer_finished ? 0u : 1u));
    MOQ_TEST_CHECK_EQ_U64(p.timeline_ends, peer_finished ? 0u : 1u);
    MOQ_TEST_CHECK_EQ_U64(p.timeline_subgroups, peer_finished ? 0u : 1u);
    MOQ_TEST_CHECK_EQ_U64(p.timeline_finished, peer_finished ? 0u : 1u);
    MOQ_TEST_CHECK(p.timeline_acks == 1 && f->track_closed == (peer_finished ? 1u : 0u));
    MOQ_TEST_CHECK(f->publications == 4 && f->selections == 0);
    MOQ_TEST_CHECK(!moq_media_sender_test_has_pending_output(f->sender));
    admission_expect(&p.a, p.late, 1, MOQ_OK);
    terminal_stats(&p, 1);
done:
    terminal_cleanup(&p, before, peer_finished ? "peer_finished_media_and_timeline"
                                             : "generated_timeline_withheld_ack");
}

/* add -> end -> complete before any registration pump. In this settled hook
 * the first allocation snapshots demand; the second snapshots maintenance.
 * Refuse just that second snapshot, twice, while the healthy track's first
 * terminal admission resolves and the following hook fully ends it. */
static void test_unregistered_end_completion(void)
{
    int before = failures;
    pending_publication_fixture_t p = {0};
    fixture_t *f = &p.a.f;
    if (!terminal_setup(&p, MOQ_VERSION_DRAFT_16, 0, false)) goto done;
    if (!terminal_add_late(&p, false)) goto done;
    MOQ_TEST_CHECK(moq_media_sender_end_track(f->sender, p.late) == MOQ_OK);
    MOQ_TEST_CHECK(moq_media_sender_complete(f->sender) == MOQ_OK);
    if (failures != before) goto done;
    for (unsigned i = 0; i < 2; ++i) {
        f->fail_alloc_after = 2;
        pending_drive(&p);
        MOQ_TEST_CHECK(f->fail_alloc_after == 0 && f->alloc_refusals == i + 1);
        MOQ_TEST_CHECK(f->refused_alloc_size == 2 * sizeof(moq_media_track_t *));
        admission_expect(&p.a, p.late, 0, MOQ_OK);
        MOQ_TEST_CHECK(f->selections == 3 && p.a.selected[AUTH_NEW_MEDIA] == 0);
        MOQ_TEST_CHECK(p.late_requests == 0 && p.late_finished == 0);
        /* The first hook emits the healthy terminal; its transport admission
         * is resolved by pending_flush, so only the second hook can finish. */
        MOQ_TEST_CHECK(p.healthy_ends == 1);
        MOQ_TEST_CHECK(p.healthy_finished == (i == 0 ? 0u : 1u));
        terminal_marker(&p, true);
        terminal_waiting(&p);
        if (failures != before) goto done;
    }
    check_write(f, p.late, MOQ_ERR_WRONG_STATE);
    for (unsigned i = 0; i < 24; ++i) pending_drive(&p);
    admission_expect(&p.a, p.late, 1, MOQ_OK);
    MOQ_TEST_CHECK(p.late_requests == 1 && p.late_acks == 1);
    MOQ_TEST_CHECK(p.late_ends == 1 && p.late_subgroups == 1 && p.late_finished == 1);
    MOQ_TEST_CHECK(p.healthy_ends == 1 && p.healthy_finished == 1 && p.terminal_catalogs == 1);
    MOQ_TEST_CHECK(f->selections == 4 && p.a.selected[AUTH_NEW_MEDIA] == 1);
    MOQ_TEST_CHECK(f->alloc_refusals == 2 && f->track_closed == 0);
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(f->sender) == 0);
    MOQ_TEST_CHECK(!moq_media_sender_test_has_pending_output(f->sender));
    terminal_marker(&p, false);
    pending_live(&p);
    pending_catalog(&p, true);
    terminal_stats(&p, 0);
done:
    terminal_cleanup(&p, before, "unregistered_end_then_complete");
}

/* Unlike the accepted-publication cancellation row, neither PUBLISH_OK can
 * set the latch here. Observe the actual two post-ready request bidis, then
 * deliver the peer's STOP through the real session transport entry point.
 * No facade callback or publication state is forged. */
static void test_peer_finished_before_ack(void)
{
    int before = failures;
    pending_publication_fixture_t p = {0};
    fixture_t *f = &p.a.f;
    if (!terminal_setup(&p, MOQ_VERSION_DRAFT_18, 0, true)) goto done;
    p.hold_late_ack = p.hold_timeline_ack = true;
    p.a.capture_publish_bidis = true;
    if (!terminal_add_late(&p, true)) goto done;
    pending_drive(&p);
    p.a.capture_publish_bidis = false;
    admission_expect(&p.a, p.late, 1, MOQ_OK);
    MOQ_TEST_CHECK(p.late_requests == 1 && p.timeline_requests == 1 && f->publications == 4);
    MOQ_TEST_CHECK(p.late_acks == 0 && p.timeline_acks == 0);
    MOQ_TEST_CHECK(p.a.publish_bidis == 2 && p.a.publish_bidi[0]._v != 0 &&
        p.a.publish_bidi[1]._v != 0 && p.a.publish_bidi[0]._v != p.a.publish_bidi[1]._v);
    if (failures != before) goto done;

    f->queued_healthy = enqueue_observed(f, f->track);
    MOQ_TEST_CHECK(f->queued_healthy != NULL);
    MOQ_TEST_CHECK(moq_media_sender_complete(f->sender) == MOQ_OK);
    if (failures != before) goto done;
    for (unsigned i = 0; i < 24 && p.healthy_finished == 0; ++i)
        pending_drive(&p);
    terminal_waiting(&p);
    MOQ_TEST_CHECK(p.healthy_objects == 1 && p.healthy_finished == 1 && f->track_closed == 0);
    if (failures != before) goto done;

    moq_session_t *client = moq_simpair_client(f->pair);
    for (unsigned i = 0; i < 2; ++i) {
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_stop(client, p.a.publish_bidi[i], 0,
            moq_simpair_now_us(f->pair)) == MOQ_OK);
        if (failures != before) goto done;
    }
    for (unsigned i = 0; i < 24; ++i) pending_drive(&p);
    MOQ_TEST_CHECK(f->track_closed == 1);   /* The generated sibling has no app callback. */
    MOQ_TEST_CHECK(p.late_acks == 0 && p.timeline_acks == 0);
    MOQ_TEST_CHECK(p.late_ends == 0 && p.late_finished == 0 &&
        p.timeline_ends == 0 && p.timeline_finished == 0);
    MOQ_TEST_CHECK(f->publications == 4 && p.terminal_catalogs == 1 && f->selections == 0);
    admission_expect(&p.a, p.late, 1, MOQ_OK);
    MOQ_TEST_CHECK(!moq_media_sender_test_has_pending_output(f->sender));
    terminal_marker(&p, false);
    pending_live(&p);
    pending_catalog(&p, true);
    terminal_stats(&p, 1);
done:
    terminal_cleanup(&p, before, "peer_finished_before_ack_media_and_timeline");
}

typedef struct native_inval_fixture {
    fixture_t f;
    moq_bytes_t parts[33];
    unsigned namespace_selections, catalog_selections, prepared, wire_actions;
    bool observe_wire;
    moq_media_auth_outcome_t observed_outcome;
    moq_result_t observed_result;
} native_inval_fixture_t;

static void native_inval_trace(void *ctx, const moq_sim_trace_record_t *record)
{
    native_inval_fixture_t *a = ctx;
    if (a->observe_wire && record->kind == MOQ_SIM_TRACE_ACTION) a->wire_actions++;
}

static moq_result_t native_inval_select(void *ctx, const moq_media_auth_request_t *req,
    moq_auth_token_t *tokens, size_t capacity, size_t *count,
    moq_media_auth_decision_t *decision)
{
    native_inval_fixture_t *a = ctx;
    MOQ_TEST_CHECK(moq_media_sender_test_auth_unlocked(a->f.sender));
    MOQ_TEST_CHECK(capacity == 16 && req->track_namespace.count == 33);
    if (req->track_namespace.count == 33)
        for (size_t i = 0; i < 33; ++i)
            MOQ_TEST_CHECK(bytes_equal(req->track_namespace.parts[i], a->parts[i]));
    if (req->action == MOQ_MEDIA_AUTH_ACTION_PUBLISH_NAMESPACE) {
        a->namespace_selections++;
        MOQ_TEST_CHECK(req->track_name.len == 0);
    } else {
        MOQ_TEST_CHECK(req->action == MOQ_MEDIA_AUTH_ACTION_PUBLISH);
        MOQ_TEST_CHECK(bytes_equal(req->track_name, MOQ_BYTES_LITERAL("catalog")));
        a->catalog_selections++;
    }
    a->f.selections++;
    tokens[0] = (moq_auth_token_t){7, {expected, sizeof(expected)}};
    tokens[1] = (moq_auth_token_t){8, {NULL, 0}};
    *count = 2;
    *decision = MOQ_MEDIA_AUTH_ALLOW;
    return MOQ_OK;
}

static void native_inval_after_select(moq_media_sender_t *s, moq_media_track_t *t, void *ctx)
{
    native_inval_fixture_t *a = ctx;
    (void)t;
    MOQ_TEST_CHECK(s == a->f.sender);
    a->prepared++;
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(s) == a->prepared);
}

static void native_inval_outcome(void *ctx, const moq_media_auth_outcome_info_t *info)
{
    native_inval_fixture_t *a = ctx;
    MOQ_TEST_CHECK(moq_media_sender_test_auth_unlocked(a->f.sender));
    a->f.outcomes++;
    a->observed_outcome = info->outcome;
    a->observed_result = info->result;
}

static void test_native_identity_inval(moq_version_t version)
{
    int before = failures;
    native_inval_fixture_t a = {0};
    for (size_t i = 0; i < 33; ++i) a.parts[i] = MOQ_BYTES_LITERAL("n");
    moq_simpair_cfg_t pc = MOQ_SIMPAIR_CFG_INIT;
    pc.alloc = moq_alloc_default();
    pc.version = version;
    pc.seed = 42;
    pc.initial_now_us = 1000;
    pc.client_send_request_capacity = pc.server_send_request_capacity = true;
    pc.client_initial_request_capacity = pc.server_initial_request_capacity = 32;
    pc.trace_fn = native_inval_trace;
    pc.trace_ctx = &a;
    MOQ_TEST_CHECK(moq_simpair_create(&pc, &a.f.pair) == MOQ_OK);
    if (!a.f.pair) goto done;
    MOQ_TEST_CHECK(moq_simpair_start(a.f.pair) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(a.f.pair, 64, NULL) == MOQ_OK);
    moq_session_t *sessions[] = {moq_simpair_client(a.f.pair), moq_simpair_server(a.f.pair)};
    for (size_t i = 0; i < 2; ++i) {
        unsigned drained;
        for (drained = 0; drained < 64; ++drained) {
            moq_event_t ev;
            size_t n = 0;
            MOQ_TEST_CHECK(moq_session_poll_events_ex(sessions[i], &ev, 1, sizeof(ev), &n) == MOQ_OK);
            if (!n) break;
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SETUP_COMPLETE);
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(drained < 64);
    }
    moq_media_auth_cfg_t auth;
    moq_media_auth_cfg_init(&auth);
    auth.ctx = &a;
    auth.select = native_inval_select;
    auth.on_outcome = native_inval_outcome;
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = (moq_namespace_t){a.parts, 33};
    cfg.publish_tracks = true;
    cfg.resource_auth = &auth;
    moq_media_sender_callbacks_init_sized(&cfg.callbacks, sizeof(cfg.callbacks));
    cfg.callbacks.ctx = &a.f;
    cfg.callbacks.on_ready = on_ready;
    cfg.callbacks.on_closed = on_closed;
    cfg.callbacks.on_track_closed = on_track_closed;
    moq_alloc_t alloc = {.alloc = fixture_alloc, .free = fixture_free,
        .realloc = fixture_realloc, .ctx = &a.f};
    moq_media_sender_test_set_alloc(&alloc);
    a.f.sender = moq_media_sender_test_new_cfg(&cfg);
    moq_media_sender_test_set_alloc(NULL);
    bool construction_reaches_native_identity_validation = a.f.sender != NULL;
    MOQ_TEST_CHECK(construction_reaches_native_identity_validation);
    if (!a.f.sender) goto done;

    /* A real facade call proves the native identity refusal independently of
     * the sender classifier. No return injection or action pressure. */
    a.observe_wire = true;
    moq_pub_cfg_t pubcfg;
    moq_pub_cfg_init_sized(&pubcfg, sizeof(pubcfg));
    moq_publisher_t *probe = NULL;
    MOQ_TEST_CHECK(moq_pub_create(sessions[0], moq_alloc_default(), &pubcfg, &probe) == MOQ_OK);
    if (probe) {
        moq_auth_token_t tokens[] = {{7, {expected, sizeof(expected)}}, {8, {NULL, 0}}};
        moq_pub_track_cfg_t tc;
        moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
        tc.track_namespace = cfg.namespace_;
        tc.track_name = MOQ_BYTES_LITERAL("catalog");
        tc.advertise_namespace = true;
        tc.namespace_auth_tokens = tokens;
        tc.namespace_auth_token_count = 2;
        moq_pub_track_t *track = NULL;
        moq_result_t native_identity_result = moq_pub_add_track(probe, &tc,
            moq_simpair_now_us(a.f.pair), &track);
        MOQ_TEST_CHECK_EQ_INT(native_identity_result, MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(track == NULL);
        moq_pub_destroy(probe);
    }
    moq_media_sender_test_auth_after_select(native_inval_after_select, &a);
    moq_media_sender_test_pump(a.f.sender, sessions[0], moq_simpair_now_us(a.f.pair));
    moq_media_sender_test_auth_after_select(NULL, NULL);
    MOQ_TEST_CHECK(a.namespace_selections == 1 && a.catalog_selections == 1 && a.prepared == 2);
    bool ordinary_setup_failure = a.f.closed == 1 && moq_media_sender_is_fatal(a.f.sender) &&
        moq_media_sender_fatal_code(a.f.sender) == MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED;
    MOQ_TEST_CHECK(ordinary_setup_failure);
    for (unsigned i = 0; i < 4; ++i) {
        moq_media_sender_test_pump(a.f.sender, sessions[0], moq_simpair_now_us(a.f.pair));
        MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(a.f.pair, 64, NULL) == MOQ_OK);
    }
    bool no_credential_outcome = a.f.outcomes == 0;
    MOQ_TEST_CHECK(no_credential_outcome);
    MOQ_TEST_CHECK(a.f.selections == 2 && a.f.closed == 1 && a.f.ready == 0 && a.f.track_closed == 0);
    MOQ_TEST_CHECK(!moq_media_sender_is_ready(a.f.sender));
    MOQ_TEST_CHECK(moq_media_sender_test_auth_pending(a.f.sender) == 0);
    uint64_t publish_attempts = 0;
    MOQ_TEST_CHECK(moq_media_sender_test_publish_admission(a.f.sender, NULL, &publish_attempts, NULL));
    MOQ_TEST_CHECK(publish_attempts == 0);
    MOQ_TEST_CHECK(a.wire_actions == 0);
    for (size_t i = 0; i < 2; ++i) {
        moq_event_t ev;
        size_t n = 0;
        MOQ_TEST_CHECK(moq_session_poll_events_ex(sessions[i], &ev, 1, sizeof(ev), &n) == MOQ_OK);
        MOQ_TEST_CHECK(n == 0);
        if (n) moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(moq_session_state(sessions[i]) == MOQ_SESS_ESTABLISHED);
    }
done:
    moq_media_sender_test_auth_after_select(NULL, NULL);
    moq_media_sender_test_free(a.f.sender);
    moq_simpair_destroy(a.f.pair);
    MOQ_TEST_CHECK(a.f.live_allocations == 0 && !a.f.fail_next_alloc);
    if (failures != before)
        fprintf(stderr, "media_sender_auth: native_identity_inval version=%u outcome=%u result=%d failed\n",
            (unsigned)version, (unsigned)a.observed_outcome, a.observed_result);
}

int main(void)
{
    for (unsigned mode = 0; mode < 22; ++mode) {
        scenario(MOQ_VERSION_DRAFT_16, mode);
        scenario(MOQ_VERSION_DRAFT_18, mode);
        scenario(MOQ_VERSION_DRAFT_22, mode);
    }
    test_d16_media_credit_retention();
    test_d16_catalog_credit_snapshot();
    test_d18_catalog_action_admission_snapshot();
    test_catalog_admission_snapshot(MOQ_VERSION_DRAFT_22, "d22_catalog_action_admission_snapshot");
    test_d22_media_action_retention();
    for (unsigned mode = 0; mode < 3; ++mode) {
        test_post_ready_pending_terminal(false, mode);
        test_post_ready_pending_terminal(true, mode);
    }
    test_withheld_ack_terminal(false);
    test_withheld_ack_terminal(true);
    test_partial_end_retry();
    test_generated_resolution(false);
    test_generated_resolution(true);
    test_peer_finished_before_ack();
    test_unregistered_end_completion();
    test_native_identity_inval(MOQ_VERSION_DRAFT_16);
    test_native_identity_inval(MOQ_VERSION_DRAFT_18);
    test_native_identity_inval(MOQ_VERSION_DRAFT_22);
    if (failures) return 1;
    MOQ_TEST_PASS("media_sender_auth");
    return 0;
}
