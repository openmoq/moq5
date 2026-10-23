/* Real receiver hook + SimPair peer: local credential policy is not a peer
 * rejection, and native admission retries must carry the original copied bytes.
 * The catalog ingest seam supplies catalogs only; every asserted request is
 * encoded by the production facade/session and decoded by the peer session. */
#include <moq/media_receiver.h>
#include <moq/media_auth.h>
#include <moq/sim.h>
#include "../../core/src/session/session_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

moq_result_t moq_media_receiver_test_construct(const moq_media_receiver_cfg_t *, moq_media_receiver_t **);
void moq_media_receiver_test_destroy_with_session(moq_media_receiver_t *, moq_session_t *, uint64_t);
void moq_media_receiver_test_pump(moq_media_receiver_t *, moq_session_t *, uint64_t);
void moq_media_receiver_test_ingest(moq_media_receiver_t *, uint64_t, uint64_t, const char *, size_t);
int moq_media_receiver_test_mu_held(const moq_media_receiver_t *);
bool moq_media_receiver_test_auth_latched(moq_media_receiver_t *, moq_media_track_t *);
void moq_media_receiver_test_set_alloc(const moq_alloc_t *);
void moq_media_receiver_test_set_auth_hook(
    void (*)(moq_media_receiver_t *, moq_media_track_t *, void *), void *);

static int failures;
#define REQUIRE(x) do { if (!(x)) { fprintf(stderr, "PREREQUISITE: %s:%d: %s\n", __FILE__, __LINE__, #x); exit(2); } } while (0)
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #x); failures++; } } while (0)
#define CASE_CHECK(name, row, x) do { if (!(x)) { fprintf(stderr, "FAIL[%s.%s]: %s:%d: %s\n", (name), (row), __FILE__, __LINE__, #x); failures++; } } while (0)

static const moq_bytes_t ns[] = {{
    .data = (const uint8_t *)"receiver-auth", .len = sizeof("receiver-auth") - 1
}};
static const uint8_t original[] = {0x80, 0, 0xff, 0x42};
enum { BASE_RESOURCES = 5, SLOT_TRACKS = 30, RESOURCE_COUNT = BASE_RESOURCES + SLOT_TRACKS };
static const char *const resource_names[RESOURCE_COUNT] = {
    "catalog", "media", "other", "sap", "timeline",
    "slot-00", "slot-01", "slot-02", "slot-03", "slot-04", "slot-05",
    "slot-06", "slot-07", "slot-08", "slot-09", "slot-10", "slot-11",
    "slot-12", "slot-13", "slot-14", "slot-15", "slot-16", "slot-17",
    "slot-18", "slot-19", "slot-20", "slot-21", "slot-22", "slot-23",
    "slot-24", "slot-25", "slot-26", "slot-27", "slot-28", "slot-29"
};
static const char catalog[] =
    "{\"version\":1,\"tracks\":["
    "{\"name\":\"media\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\",\"codec\":\"avc1.42e01e\"},"
    "{\"name\":\"other\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\",\"codec\":\"avc1.42e01e\"},"
    "{\"name\":\"sap\",\"packaging\":\"eventtimeline\",\"isLive\":true,\"eventType\":\"org.ietf.moq.cmsf.sap\",\"mimeType\":\"application/json\"},"
    "{\"name\":\"timeline\",\"packaging\":\"mediatimeline\",\"isLive\":true}]}";

enum policy { ALLOW, DENY, ERROR, BAD, ASYNC, COPY_FAIL, PARAM_LIMIT };
typedef struct fixture {
    moq_simpair_t *pair;
    moq_session_t *client, *server;
    moq_media_receiver_t *receiver;
    moq_media_track_t *media, *other, *sap, *timeline;
    moq_media_track_t *slot_tracks[SLOT_TRACKS];
    uint8_t value[sizeof(original)];
    uint64_t type;
    bool anonymous;
    moq_media_auth_action_t fail_action;
    const char *fail_name;
    enum policy policy;
    unsigned selects[6], outcomes, subscribes, fetches, fills, updates;
    unsigned selected_resources[6][RESOURCE_COUNT], wire_subscribes[RESOURCE_COUNT];
    unsigned wire_updates[RESOURCE_COUNT], wire_unsubscribes[RESOURCE_COUNT];
    moq_subscription_t peer_subs[RESOURCE_COUNT];
    moq_fill_t peer_fills[RESOURCE_COUNT];
    uint64_t peer_fill_ids[RESOURCE_COUNT];
    moq_media_auth_scope_t scope;
    moq_media_auth_outcome_t outcome;
    moq_result_t result;
    moq_media_track_t *failed_track;
    bool latched;
    bool fail_next_alloc;
    unsigned allocations, live_allocations, refused_allocations;
} fixture_t;

static void *fixture_alloc(size_t size, void *ctx)
{
    fixture_t *f = ctx;
    f->allocations++;
    if (f->fail_next_alloc) {
        f->fail_next_alloc = false;
        f->refused_allocations++;
        return NULL;
    }
    void *p = malloc(size);
    if (p) f->live_allocations++;
    return p;
}

static void fixture_free(void *ptr, size_t size, void *ctx)
{
    (void)size;
    fixture_t *f = ctx;
    if (ptr) { REQUIRE(f->live_allocations != 0); f->live_allocations--; free(ptr); }
}

static void *fixture_realloc(void *ptr, size_t old, size_t size, void *ctx)
{
    (void)old;
    fixture_t *f = ctx;
    bool was_null = ptr == NULL;
    void *p = realloc(ptr, size);
    if (p && was_null) f->live_allocations++;
    return p;
}

static bool named(moq_bytes_t b, const char *s)
{
    return b.len == strlen(s) && memcmp(b.data, s, b.len) == 0;
}

static moq_result_t select_tokens(void *ctx, const moq_media_auth_request_t *request,
    moq_auth_token_t *out, size_t cap, size_t *count, moq_media_auth_decision_t *decision)
{
    fixture_t *f = ctx;
    CHECK(moq_media_receiver_test_mu_held(f->receiver) == 0);
    CHECK(request->struct_size >= MOQ_MEDIA_AUTH_REQUEST_V0_SIZE);
    CHECK(request->track_namespace.count == 1);
    CHECK(request->track_namespace.count && named(request->track_namespace.parts[0], "receiver-auth"));
    REQUIRE(request->action < 6 && cap >= 1);
    f->selects[request->action]++;
    bool known = false;
    for (size_t i = 0; i < RESOURCE_COUNT; i++) {
        if (!named(request->track_name, resource_names[i])) continue;
        f->selected_resources[request->action][i]++;
        known = true;
    }
    CHECK(known);
    if (request->action == f->fail_action && named(request->track_name, f->fail_name)) {
        if (f->policy == DENY) { *decision = MOQ_MEDIA_AUTH_DENY; return MOQ_OK; }
        if (f->policy == ERROR) return MOQ_ERR_NOMEM;
        if (f->policy == ASYNC) return MOQ_ERR_WOULD_BLOCK;
        if (f->policy == BAD) { *decision = MOQ_MEDIA_AUTH_ALLOW; *count = 0; return MOQ_OK; }
        if (f->policy == COPY_FAIL) f->fail_next_alloc = true;
        if (f->policy == PARAM_LIMIT) {
            REQUIRE(cap >= 16);
            for (size_t i = 0; i < 16; i++)
                out[i] = (moq_auth_token_t){7 + i, {NULL, 0}};
            *decision = MOQ_MEDIA_AUTH_ALLOW;
            *count = 16;
            return MOQ_OK;
        }
    }
    *decision = MOQ_MEDIA_AUTH_ALLOW;
    *count = 1;
    out[0] = (moq_auth_token_t){f->type, {f->value, sizeof(f->value)}};
    return MOQ_OK;
}

static void outcome(void *ctx, const moq_media_auth_outcome_info_t *info)
{
    fixture_t *f = ctx;
    CHECK(moq_media_receiver_test_mu_held(f->receiver) == 0);
    CHECK(info->struct_size >= MOQ_MEDIA_AUTH_OUTCOME_INFO_V0_SIZE);
    CHECK(info->request && info->request->action == f->fail_action);
    CHECK(info->request && named(info->request->track_name, f->fail_name));
    f->outcomes++;
    f->scope = info->scope;
    f->outcome = info->outcome;
    f->result = info->result;
    f->failed_track = info->track;
    /* Test-only observation; applications must not reenter service queries. */
    f->latched = moq_media_receiver_test_auth_latched(f->receiver, info->track);
}

static void wire_pump(fixture_t *f)
{
    REQUIRE(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    REQUIRE(moq_session_state(moq_simpair_client(f->pair)) == MOQ_SESS_ESTABLISHED);
    REQUIRE(moq_session_state(moq_simpair_server(f->pair)) == MOQ_SESS_ESTABLISHED);
}

static void open_fixture_lanes(fixture_t *f, moq_version_t version,
    bool static_source, bool anonymous, bool flow, uint32_t terminals)
{
    memset(f, 0, sizeof(*f));
    f->anonymous = anonymous;
    f->type = version == MOQ_VERSION_DRAFT_16 ? 7 : UINT64_MAX;
    memcpy(f->value, original, sizeof(original));
    moq_simpair_cfg_t pc = MOQ_SIMPAIR_CFG_INIT;
    pc.alloc = moq_alloc_default(); pc.version = version; pc.seed = 42; pc.initial_now_us = 1000;
    pc.client_send_request_capacity = pc.server_send_request_capacity = true;
    pc.client_initial_request_capacity = pc.server_initial_request_capacity = 64;
    REQUIRE(moq_simpair_create(&pc, &f->pair) == MOQ_OK);
    REQUIRE(moq_simpair_start(f->pair) == MOQ_OK);
    wire_pump(f);
    f->client = moq_simpair_client(f->pair); f->server = moq_simpair_server(f->pair);
    REQUIRE(moq_session_state(f->client) == MOQ_SESS_ESTABLISHED);
    moq_event_t e;
    while (moq_session_poll_events(f->client, &e, 1)) moq_event_cleanup(&e);
    while (moq_session_poll_events(f->server, &e, 1)) moq_event_cleanup(&e);
    moq_media_auth_cfg_t auth;
    moq_media_auth_cfg_init(&auth);
    auth.ctx = f; auth.on_outcome = outcome;
    moq_auth_token_t token = {f->type, {f->value, sizeof(f->value)}};
    if (static_source) { auth.tokens = &token; auth.token_count = 1; }
    else auth.select = select_tokens;
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.surface_stream_terminals = terminals != 0;
    cfg.max_stream_terminals = terminals;
    if (flow) {
        cfg.overflow.policy = MOQ_MEDIA_OVERFLOW_FLOW_CONTROL;
        cfg.overflow.max_objects = 2;
    }
    cfg.namespace_ = (moq_namespace_t){ns, 1};
    cfg.auto_subscribe = false;
    cfg.companion_profile = version == MOQ_VERSION_DRAFT_22
        ? MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC
        : MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
    cfg.resource_auth = anonymous ? NULL : &auth;
    moq_alloc_t alloc = {.alloc = fixture_alloc, .free = fixture_free,
                        .realloc = fixture_realloc, .ctx = f};
    moq_media_receiver_test_set_alloc(&alloc);
    REQUIRE(moq_media_receiver_test_construct(&cfg, &f->receiver) == MOQ_OK);
    moq_media_receiver_test_set_alloc(NULL);
    CHECK(f->outcomes == 0 && f->selects[MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE] == 0);
    memset(&auth, 0xa5, sizeof(auth));
    memset(&token, 0xa5, sizeof(token));
    if (static_source) memset(f->value, 0x33, sizeof(f->value));
}

static void open_fixture_mode(fixture_t *f, moq_version_t version,
                              bool static_source, bool anonymous, bool flow)
{
    open_fixture_lanes(f, version, static_source, anonymous, flow, 0);
}

static void open_fixture(fixture_t *f, moq_version_t version, bool stat, bool anonymous)
{
    open_fixture_mode(f, version, stat, anonymous, false);
}

static void receiver_pump(fixture_t *f)
{
    moq_media_receiver_test_pump(f->receiver, f->client, moq_simpair_now_us(f->pair));
}

static void tokens_equal(fixture_t *f, const moq_resolved_token_t *t, size_t n)
{
    CHECK(n == (f->anonymous ? 0u : 1u));
    if (!f->anonymous && n == 1 && t) {
        CHECK(t[0].token_type == f->type);
        CHECK(t[0].token_value.len == sizeof(original));
        if (t[0].token_value.len == sizeof(original))
            CHECK(memcmp(t[0].token_value.data, original, sizeof(original)) == 0);
    }
}

/* The peer accepts real SUBSCRIBEs and observes actual decoded credentials.
 * FETCH is left pending: its request-local ownership is bounded by teardown. */
static void peer_requests(fixture_t *f)
{
    wire_pump(f);
    moq_event_t e;
    while (moq_session_poll_events(f->server, &e, 1)) {
        moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
        if (e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            tokens_equal(f, e.u.subscribe_request.tokens, e.u.subscribe_request.token_count);
            CHECK(e.u.subscribe_request.track_namespace.count == 1);
            if (e.u.subscribe_request.track_namespace.count == 1)
                CHECK(named(e.u.subscribe_request.track_namespace.parts[0], "receiver-auth"));
            bool known = false;
            for (size_t i = 0; i < RESOURCE_COUNT; i++) {
                if (!named(e.u.subscribe_request.track_name, resource_names[i])) continue;
                known = true;
                f->wire_subscribes[i]++;
                f->peer_subs[i] = e.u.subscribe_request.sub;
            }
            CHECK(known);
            f->subscribes++;
            sub = e.u.subscribe_request.sub;
        } else if (e.kind == MOQ_EVENT_FILL_REQUEST) {
            bool known = false;
            for (size_t i = 0; i < RESOURCE_COUNT; i++) {
                if (!moq_subscription_eq(e.u.fill_request.sub, f->peer_subs[i]))
                    continue;
                CHECK(moq_subscription_is_valid(f->peer_subs[i]));
                f->peer_fills[i] = e.u.fill_request.fill;
                f->peer_fill_ids[i] = e.u.fill_request.initiating_request_id;
                known = true;
            }
            CHECK(known);
            f->fills++;
        } else if (e.kind == MOQ_EVENT_FETCH_REQUEST) {
            tokens_equal(f, e.u.fetch_request.tokens, e.u.fetch_request.token_count);
            CHECK(e.u.fetch_request.joining_sub._opaque == f->peer_subs[0]._opaque);
            CHECK(f->peer_subs[0]._opaque != 0);
            f->fetches++;
        } else if (e.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
            tokens_equal(f, e.u.subscribe_updated.tokens, e.u.subscribe_updated.token_count);
            bool known = false;
            for (size_t i = 1; i < RESOURCE_COUNT; i++)
                if (e.u.subscribe_updated.sub._opaque == f->peer_subs[i]._opaque &&
                    f->peer_subs[i]._opaque) {
                    known = true;
                    f->wire_updates[i]++;
                }
            CHECK(known && e.u.subscribe_updated.has_forward);
            f->updates++;
        } else if (e.kind == MOQ_EVENT_UNSUBSCRIBED) {
            bool known = false;
            for (size_t i = 0; i < RESOURCE_COUNT; i++) {
                if (!f->peer_subs[i]._opaque ||
                    e.u.unsubscribed.sub._opaque != f->peer_subs[i]._opaque) continue;
                f->wire_unsubscribes[i]++;
                known = true;
            }
            CHECK(known);
        }
        moq_event_cleanup(&e);
        if (sub._opaque) {
            moq_accept_subscribe_cfg_t ac;
            moq_accept_subscribe_cfg_init(&ac);
            ac.has_largest = true; ac.largest_group = 0; ac.largest_object = 0;
            REQUIRE(moq_session_accept_subscribe(f->server, sub, &ac,
                        moq_simpair_now_us(f->pair)) == MOQ_OK);
        }
    }
    wire_pump(f);
}

static void collect_tracks(fixture_t *f)
{
    moq_media_track_event_t e;
    while (moq_media_receiver_poll_track(f->receiver, &e, sizeof(e)) == MOQ_OK) {
        if (e.kind != MOQ_MEDIA_TRACK_ADDED) continue;
        if (named(e.desc->name, "media")) f->media = e.track;
        if (named(e.desc->name, "other")) f->other = e.track;
        if (named(e.desc->name, "sap")) f->sap = e.track;
        if (named(e.desc->name, "timeline")) f->timeline = e.track;
    }
    REQUIRE(f->media && f->other && f->sap && f->timeline);
}

static void discover(fixture_t *f)
{
    receiver_pump(f); peer_requests(f); receiver_pump(f); peer_requests(f);
    moq_media_receiver_test_ingest(f->receiver, 0, 0, catalog, sizeof(catalog) - 1);
    collect_tracks(f);
}

static void discover_d22(fixture_t *f)
{
    receiver_pump(f); peer_requests(f); receiver_pump(f); peer_requests(f);
    REQUIRE(moq_fill_is_valid(f->peer_fills[0]));
    CHECK(f->subscribes == 1 && f->fills == 1 && f->fetches == 0);
    CHECK(f->selected_resources[MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE][0] ==
          (f->anonymous ? 0u : f->selects[MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE]));
    CHECK(f->selects[MOQ_MEDIA_AUTH_ACTION_FETCH] == 0);
    uint64_t now = moq_simpair_now_us(f->pair);
    REQUIRE(moq_session_open_fill(f->server, f->peer_fills[0], now) == MOQ_OK);
    moq_fetch_object_cfg_t cfg;
    moq_fetch_object_cfg_init(&cfg);
    REQUIRE(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)catalog,
                sizeof(catalog) - 1, &cfg.payload) == MOQ_OK);
    REQUIRE(moq_session_write_fill_object(f->server, f->peer_fills[0], &cfg, now) == MOQ_OK);
    moq_rcbuf_decref(cfg.payload);
    REQUIRE(moq_session_end_fill(f->server, f->peer_fills[0], now) == MOQ_OK);
    peer_requests(f); receiver_pump(f); peer_requests(f); receiver_pump(f);
    collect_tracks(f);
}

static void close_fixture(fixture_t *f)
{
    moq_media_receiver_test_destroy_with_session(f->receiver, f->client,
                                                moq_simpair_now_us(f->pair));
    moq_simpair_destroy(f->pair);
    CHECK(f->live_allocations == 0);
}

static void test_carriage(moq_version_t version, bool stat, bool anonymous)
{
    fixture_t f; open_fixture(&f, version, stat, anonymous); discover(&f);
    moq_media_track_t *tracks[] = {f.media, f.other, f.sap, f.timeline};
    for (size_t i = 0; i < 4; i++)
        REQUIRE(moq_media_receiver_subscribe_track(f.receiver, tracks[i], NULL) == MOQ_OK);
    receiver_pump(&f); peer_requests(&f); receiver_pump(&f);
    CHECK(f.subscribes == 5 && f.fetches == 1);
    REQUIRE(moq_media_receiver_unsubscribe_track(f.receiver, f.media) == MOQ_OK);
    receiver_pump(&f); peer_requests(&f);
    CHECK(f.updates == 1 && f.outcomes == 0);
    if (!stat && !anonymous) {
        CHECK(f.selects[MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE] == 5);
        CHECK(f.selects[MOQ_MEDIA_AUTH_ACTION_FETCH] == 1);
        CHECK(f.selects[MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE] == 1);
        for (size_t i = 0; i < 5; i++)
            CHECK(f.selected_resources[MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE][i] == 1 &&
                  f.wire_subscribes[i] == 1);
        CHECK(f.selected_resources[MOQ_MEDIA_AUTH_ACTION_FETCH][0] == 1);
        CHECK(f.selected_resources[MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE][1] == 1);
    } else {
        CHECK(f.selects[MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE] == 0);
    }
    CHECK(!moq_media_receiver_is_fatal(f.receiver));
    close_fixture(&f);
}

static void test_catalog_failure(moq_version_t v, enum policy p)
{
    fixture_t f; open_fixture(&f, v, false, false);
    f.fail_action = MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE; f.fail_name = "catalog"; f.policy = p;
    for (int i = 0; i < 3; i++) { receiver_pump(&f); peer_requests(&f); }
    CHECK(f.subscribes == 0 && f.fetches == 0);
    CHECK(f.outcomes == 1 && f.scope == MOQ_MEDIA_AUTH_SCOPE_SERVICE && f.latched);
    CHECK(f.failed_track == NULL && moq_media_receiver_is_fatal(f.receiver));
    CHECK(moq_session_state(f.client) == MOQ_SESS_ESTABLISHED);
    CHECK(f.selects[MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE] == 1);
    CHECK(f.outcome == (p == DENY ? MOQ_MEDIA_AUTH_OUTCOME_POLICY_DENIED :
          p == ERROR ? MOQ_MEDIA_AUTH_OUTCOME_SELECTOR_ERROR :
          p == COPY_FAIL ? MOQ_MEDIA_AUTH_OUTCOME_TOKEN_COPY_FAILED : MOQ_MEDIA_AUTH_OUTCOME_INVALID_SELECTION));
    CHECK(f.result == (p == DENY ? MOQ_OK : (p == ERROR || p == COPY_FAIL) ? MOQ_ERR_NOMEM : MOQ_ERR_INVAL));
    CHECK(f.refused_allocations == (p == COPY_FAIL ? 1u : 0u));
    close_fixture(&f);
}

static void test_request_and_track_failure(moq_version_t v, bool update, size_t resource)
{
    fixture_t f; open_fixture(&f, v, false, false);
    f.fail_action = MOQ_MEDIA_AUTH_ACTION_FETCH; f.fail_name = "catalog"; f.policy = DENY;
    discover(&f);
    CHECK(f.fetches == 0 && f.outcomes == 1 && f.scope == MOQ_MEDIA_AUTH_SCOPE_REQUEST && f.latched);
    CHECK(!moq_media_receiver_is_fatal(f.receiver));
    moq_media_track_t *track = resource == 1 ? f.media : resource == 3 ? f.sap : f.timeline;
    f.outcomes = 0; f.fail_name = resource_names[resource];
    f.fail_action = update ? MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE : MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE;
    REQUIRE(moq_media_receiver_subscribe_track(f.receiver, track, NULL) == MOQ_OK);
    REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.other, NULL) == MOQ_OK);
    receiver_pump(&f); peer_requests(&f); receiver_pump(&f);
    if (update) {
        REQUIRE(moq_media_receiver_unsubscribe_track(f.receiver, track) == MOQ_OK);
        receiver_pump(&f); peer_requests(&f);
    }
    for (int i = 0; i < 3; i++) { receiver_pump(&f); peer_requests(&f); }
    moq_media_track_state_t state;
    CHECK(moq_media_receiver_track_state(f.receiver, track, &state) == MOQ_OK);
    CHECK(state == MOQ_MEDIA_TRACK_STATE_FAILED);
    CHECK(f.outcomes == 1 && f.scope == MOQ_MEDIA_AUTH_SCOPE_TRACK && f.failed_track == track && f.latched);
    CHECK(f.updates == 0 && !moq_media_receiver_is_fatal(f.receiver));
    CHECK(moq_media_receiver_subscribe_track(f.receiver, track, NULL) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_media_receiver_unsubscribe_track(f.receiver, track) == MOQ_OK);
    CHECK(moq_media_receiver_track_state(f.receiver, f.other, &state) == MOQ_OK && state == MOQ_MEDIA_TRACK_STATE_ACTIVE);
    moq_media_track_event_t e;
    while (moq_media_receiver_poll_track(f.receiver, &e, sizeof(e)) == MOQ_OK)
        CHECK(e.kind != MOQ_MEDIA_TRACK_ENDED && e.kind != MOQ_MEDIA_TRACK_UPDATE_OK);
    close_fixture(&f);
}

static size_t occupy_actions(fixture_t *f)
{
    REQUIRE(f->client->action_head == f->client->action_tail);
    size_t cap = f->client->action_cap;
    f->client->action_cap = 1;
    moq_track_status_cfg_t sc;
    moq_track_status_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ns, 1}; sc.track_name = MOQ_BYTES_LITERAL("occupy");
    moq_track_status_handle_t h;
    REQUIRE(moq_session_track_status(f->client, &sc, moq_simpair_now_us(f->pair), &h) == MOQ_OK);
    return cap;
}

static void release_actions(fixture_t *f, size_t cap)
{
    /* The pressure request consumed an ID: deliver it, never discard it. */
    peer_requests(f);
    REQUIRE(f->client->action_head == f->client->action_tail);
    REQUIRE(moq_session_tick(f->client, moq_simpair_now_us(f->pair)) == MOQ_OK);
    REQUIRE(f->client->action_head == f->client->action_tail && f->client->send_len == 0);
    f->client->action_cap = cap;
}

static void test_retry_copy(moq_version_t v)
{
    fixture_t f; open_fixture(&f, v, false, false);
    size_t cap = occupy_actions(&f);
    receiver_pump(&f);
    CHECK(f.selects[MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE] == 1 && !moq_media_receiver_is_fatal(f.receiver));
    memset(f.value, 0x33, sizeof(f.value));
    release_actions(&f, cap);
    receiver_pump(&f); peer_requests(&f);
    CHECK(f.subscribes == 1 && f.selects[MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE] == 1);
    close_fixture(&f);
}

static void test_operation_retry(moq_version_t v, moq_media_auth_action_t action)
{
    fixture_t f; open_fixture(&f, v, false, false);
    if (action == MOQ_MEDIA_AUTH_ACTION_FETCH) {
        receiver_pump(&f); peer_requests(&f); /* catalog ACK awaits receiver tick */
    } else {
        discover(&f);
        REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.media, NULL) == MOQ_OK);
        if (action == MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE) {
            receiver_pump(&f); peer_requests(&f); receiver_pump(&f);
            REQUIRE(moq_media_receiver_unsubscribe_track(f.receiver, f.media) == MOQ_OK);
        }
    }
    unsigned before = f.selects[action];
    size_t cap = occupy_actions(&f);
    receiver_pump(&f);
    CHECK(f.selects[action] == before + 1 && f.outcomes == 0);
    memset(f.value, 0x33, sizeof(f.value));
    receiver_pump(&f); /* unchanged intent, still blocked */
    CHECK(f.selects[action] == before + 1);
    release_actions(&f, cap);
    receiver_pump(&f); peer_requests(&f);
    CHECK(f.selects[action] == before + 1 && f.outcomes == 0);
    if (action == MOQ_MEDIA_AUTH_ACTION_FETCH) CHECK(f.fetches == 1);
    else if (action == MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE) CHECK(f.wire_subscribes[1] == 1);
    else CHECK(f.updates == 1);
    close_fixture(&f);
}

typedef struct intent_change {
    moq_media_track_t *track;
    unsigned calls;
    bool remove;
} intent_change_t;

/* This test-only phase runs AFTER selection returns, simulating an app-thread
 * cancellation or a catalog replacement without callback API reentry. */
static void change_intent(moq_media_receiver_t *r, moq_media_track_t *t, void *ctx)
{
    intent_change_t *change = ctx;
    if (t != change->track || change->calls++) return;
    if (change->remove) {
        const char empty[] = "{\"version\":1,\"tracks\":[]}";
        moq_media_receiver_test_ingest(r, 1, 0, empty, sizeof(empty) - 1);
    } else {
        REQUIRE(moq_media_receiver_unsubscribe_track(r, t) == MOQ_OK);
    }
}

static void test_intent_change(moq_version_t v, bool remove)
{
    fixture_t f; open_fixture(&f, v, false, false); discover(&f);
    REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.media, NULL) == MOQ_OK);
    intent_change_t change = {.track = f.media, .remove = remove};
    moq_media_receiver_test_set_auth_hook(change_intent, &change);
    receiver_pump(&f);
    moq_media_receiver_test_set_auth_hook(NULL, NULL);
    peer_requests(&f);
    CHECK(change.calls == 1 && f.subscribes == 1 && f.outcomes == 0);
    CHECK(f.selects[MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE] == 2);
    if (!remove) {
        REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.media, NULL) == MOQ_OK);
        receiver_pump(&f); peer_requests(&f);
        CHECK(f.selects[MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE] == 3 && f.subscribes == 2);
    }
    close_fixture(&f);
}

static void test_capacity_failure(moq_version_t v)
{
    fixture_t f; open_fixture(&f, v, false, false);
    f.fail_action = MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE; f.fail_name = "catalog";
    size_t cap = f.client->send_cap;
    REQUIRE(f.client->send_len == 0);
    f.client->send_cap = 1; /* complete valid request cannot fit even when empty */
    receiver_pump(&f);
    f.client->send_cap = cap;
    peer_requests(&f);
    CHECK(f.outcomes == 1 && f.outcome == MOQ_MEDIA_AUTH_OUTCOME_TOKEN_LIMIT);
    CHECK(f.result == MOQ_ERR_BUFFER && f.latched && f.subscribes == 0);
    CHECK(f.scope == MOQ_MEDIA_AUTH_SCOPE_SERVICE && moq_media_receiver_is_fatal(f.receiver));
    CHECK(moq_session_state(f.client) == MOQ_SESS_ESTABLISHED);
    close_fixture(&f);
}

static void send_payload(fixture_t *f, size_t resource, uint64_t group,
                         const char *text)
{
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    cfg.group_id = group;
    moq_subgroup_handle_t subgroup;
    uint64_t now = moq_simpair_now_us(f->pair);
    REQUIRE(moq_session_open_subgroup(f->server, f->peer_subs[resource],
                                     &cfg, now, &subgroup) == MOQ_OK);
    moq_rcbuf_t *payload = NULL;
    REQUIRE(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)text,
                             strlen(text), &payload) == MOQ_OK);
    REQUIRE(moq_session_write_object(f->server, subgroup, 0, payload, now) == MOQ_OK);
    moq_rcbuf_decref(payload);
    REQUIRE(moq_session_close_subgroup(f->server, subgroup, now) == MOQ_OK);
    wire_pump(f);
}

static void test_flow_failure_ownership(moq_version_t v)
{
    fixture_t f; open_fixture_mode(&f, v, false, false, true); discover(&f);
    REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.media, NULL) == MOQ_OK);
    REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.other, NULL) == MOQ_OK);
    receiver_pump(&f); peer_requests(&f); receiver_pump(&f);
    send_payload(&f, 1, 1, "app-owned"); receiver_pump(&f);
    moq_media_object_t owned;
    REQUIRE(moq_media_receiver_poll_object(f.receiver, &owned, sizeof(owned)) == MOQ_OK);
    CHECK(owned.track == f.media && named(owned.payload, "app-owned"));
    f.fail_action = MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE;
    f.fail_name = "media"; f.policy = DENY;
    /* The third queued object crosses the real flow threshold. Only media's
     * denied generation is purged; other and the already-polled object survive. */
    send_payload(&f, 1, 2, "purged-one");
    send_payload(&f, 2, 1, "other-owned");
    send_payload(&f, 1, 3, "purged-two");
    receiver_pump(&f); peer_requests(&f);
    CHECK(f.outcomes == 1 && f.failed_track == f.media && f.latched);
    CHECK(f.selected_resources[MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE][1] == 1);
    CHECK(f.wire_updates[1] == 0 && !moq_media_receiver_is_fatal(f.receiver));
    moq_media_object_t other;
    REQUIRE(moq_media_receiver_poll_object(f.receiver, &other, sizeof(other)) == MOQ_OK);
    CHECK(other.track == f.other && named(other.payload, "other-owned"));
    moq_media_object_cleanup(&other);
    /* This private fixture has no endpoint. Empty polling reports CLOSED;
     * health is checked separately, not inferred from that sentinel. */
    moq_result_t empty = moq_media_receiver_poll_object(f.receiver, &other, sizeof(other));
    if (empty != MOQ_ERR_CLOSED) fprintf(stderr, "flow empty poll result: %d\n", (int)empty);
    CHECK(empty == MOQ_ERR_CLOSED);
    CHECK(!moq_media_receiver_is_fatal(f.receiver));
    CHECK(named(owned.payload, "app-owned"));
    moq_media_object_cleanup(&owned);
    for (int i = 0; i < 3; i++) { receiver_pump(&f); peer_requests(&f); }
    CHECK(f.outcomes == 1 && f.wire_updates[1] == 0);
    moq_media_track_event_t event;
    while (moq_media_receiver_poll_track(f.receiver, &event, sizeof(event)) == MOQ_OK)
        CHECK(event.kind != MOQ_MEDIA_TRACK_ENDED && event.kind != MOQ_MEDIA_TRACK_UPDATE_OK);
    close_fixture(&f);
}

/* Catalog + media + other + 29 fillers occupy all 32 receiver facade slots.
 * The final filler proves release by admission, without a private observer. */
static void fill_receiver_slots(fixture_t *f)
{
    /* D16 grants request IDs, advancing by two; the initial limit of 64
     * cannot admit 32 subscriptions plus the catalog FETCH and pressure work. */
    if (moq_session_version(f->client) == MOQ_VERSION_DRAFT_16) {
        REQUIRE(moq_session_grant_request_capacity(f->server, 256,
                    moq_simpair_now_us(f->pair)) == MOQ_OK);
        wire_pump(f);
        REQUIRE(moq_session_peer_request_capacity(f->client) == 256);
    }
    char json[8192];
    size_t used = sizeof(catalog) - 3; /* retain the original four entries */
    memcpy(json, catalog, used);
    for (size_t i = 0; i < SLOT_TRACKS; i++) {
        int n = snprintf(json + used, sizeof(json) - used,
            ",{\"name\":\"%s\",\"packaging\":\"loc\",\"isLive\":true,"
            "\"role\":\"video\",\"codec\":\"avc1.42e01e\"}",
            resource_names[BASE_RESOURCES + i]);
        REQUIRE(n > 0 && (size_t)n < sizeof(json) - used);
        used += (size_t)n;
    }
    REQUIRE(sizeof(json) - used >= 3);
    memcpy(json + used, "]}", 3);
    used += 2;
    moq_media_receiver_test_ingest(f->receiver, 1, 0, json, used);
    moq_media_track_event_t event;
    while (moq_media_receiver_poll_track(f->receiver, &event, sizeof(event)) == MOQ_OK) {
        if (event.kind != MOQ_MEDIA_TRACK_ADDED) continue;
        for (size_t i = 0; i < SLOT_TRACKS; i++)
            if (named(event.desc->name, resource_names[BASE_RESOURCES + i]))
                f->slot_tracks[i] = event.track;
    }
    for (size_t i = 0; i < SLOT_TRACKS; i++) REQUIRE(f->slot_tracks[i]);
    REQUIRE(moq_media_receiver_subscribe_track(f->receiver, f->media, NULL) == MOQ_OK);
    REQUIRE(moq_media_receiver_subscribe_track(f->receiver, f->other, NULL) == MOQ_OK);
    receiver_pump(f); peer_requests(f); receiver_pump(f);
    /* Service both native event queues between bounded request/ACK batches;
     * setup must not exercise unrelated default event-queue pressure. */
    for (size_t i = 0; i < SLOT_TRACKS - 1; i++) {
        REQUIRE(moq_media_receiver_subscribe_track(f->receiver, f->slot_tracks[i], NULL) == MOQ_OK);
        if ((i + 1) % 8 == 0 || i + 1 == SLOT_TRACKS - 1) {
            receiver_pump(f); peer_requests(f); receiver_pump(f);
        }
    }
    REQUIRE(f->subscribes == 32);
    for (size_t i = 0; i < SLOT_TRACKS - 1; i++)
        REQUIRE(f->wire_subscribes[BASE_RESOURCES + i] == 1);
}

static const moq_sub_entry_t *native_subscription(moq_session_t *session,
                                                moq_subscription_t handle)
{
    for (size_t i = 0; i < session->sub_cap; i++) {
        const moq_sub_entry_t *entry = &session->subs[i];
        if (entry->state != MOQ_SUB_FREE && entry->handle._opaque == handle._opaque)
            return entry;
    }
    return NULL;
}

static moq_subscription_t client_subscription(fixture_t *f, size_t resource)
{
    const moq_sub_entry_t *peer = native_subscription(f->server, f->peer_subs[resource]);
    REQUIRE(peer && peer->track_id_len > 0);
    for (size_t i = 0; i < f->client->sub_cap; i++) {
        const moq_sub_entry_t *entry = &f->client->subs[i];
        if (entry->state != MOQ_SUB_FREE && entry->role == MOQ_SUB_ROLE_SUBSCRIBER &&
            entry->track_id_len == peer->track_id_len &&
            memcmp(entry->track_id_buf, peer->track_id_buf, peer->track_id_len) == 0)
            return entry->handle;
    }
    REQUIRE(false);
    return MOQ_SUBSCRIPTION_INVALID;
}

enum cancel_control { CANCEL_SCRATCH, CANCEL_WOULD_BLOCK, CANCEL_PEER_DONE };

static void test_cancel_cleanup(moq_version_t version, enum cancel_control control)
{
    const char *name = control == CANCEL_SCRATCH ? "cancel.d16.occupied_scratch" :
        control == CANCEL_WOULD_BLOCK ?
            (version == MOQ_VERSION_DRAFT_16 ? "cancel.d16.would_block" : "cancel.d18.would_block") :
            (version == MOQ_VERSION_DRAFT_16 ? "cancel.d16.peer_done" : "cancel.d18.peer_done");
    fixture_t f; open_fixture(&f, version, false, false); discover(&f);
    fill_receiver_slots(&f);
    moq_subscription_t media = client_subscription(&f, 1);
    moq_subscription_t healthy = client_subscription(&f, 2);
    REQUIRE(native_subscription(f.client, media)->state == MOQ_SUB_ESTABLISHED);
    REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.slot_tracks[SLOT_TRACKS - 1], NULL) == MOQ_OK);
    receiver_pump(&f); peer_requests(&f);
    REQUIRE(f.wire_subscribes[RESOURCE_COUNT - 1] == 0 && !moq_media_receiver_is_fatal(f.receiver));
    REQUIRE(f.selected_resources[MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE][RESOURCE_COUNT - 1] == 1);
    send_payload(&f, 2, 1, "healthy-before"); receiver_pump(&f);
    f.fail_action = MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE;
    f.fail_name = "media"; f.policy = DENY;
    REQUIRE(moq_media_receiver_unsubscribe_track(f.receiver, f.media) == MOQ_OK);
    receiver_pump(&f); /* failure latches; cancellation starts on the next pass */
    CASE_CHECK(name, "one_latched_denial", f.outcomes == 1 && f.latched &&
        f.failed_track == f.media && f.outcome == MOQ_MEDIA_AUTH_OUTCOME_POLICY_DENIED &&
        f.result == MOQ_OK && f.scope == MOQ_MEDIA_AUTH_SCOPE_TRACK);
    size_t action_cap = f.client->action_cap, send_cap = f.client->send_cap;
    if (control == CANCEL_SCRATCH) {
        REQUIRE(version == MOQ_VERSION_DRAFT_16 && f.client->send_len == 0);
        moq_track_status_cfg_t status;
        moq_track_status_cfg_init(&status);
        status.track_namespace = (moq_namespace_t){ns, 1};
        status.track_name = MOQ_BYTES_LITERAL("real-scratch-pressure-with-space-for-an-empty-buffer-cancellation");
        moq_track_status_handle_t handle;
        REQUIRE(moq_session_track_status(f.client, &status,
                    moq_simpair_now_us(f.pair), &handle) == MOQ_OK);
        /* The message fits empty scratch, so current queue_send_cancel must
         * report WOULD_BLOCK while these real bytes remain outstanding. */
        REQUIRE(f.client->send_len >= 32 && action_queue_avail(f.client) > 0);
        f.client->send_cap = f.client->send_len;
    } else {
        action_cap = occupy_actions(&f);
    }
    size_t queued = f.client->action_tail - f.client->action_head;
    size_t occupied = f.client->send_len;
    moq_result_t pressure = moq_session_unsubscribe(f.client, media, moq_simpair_now_us(f.pair));
    CASE_CHECK(name, "actual_native_pressure_result", pressure == MOQ_ERR_WOULD_BLOCK);
    receiver_pump(&f);
    CASE_CHECK(name, "native_owner_retained", native_subscription(f.client, media) != NULL);
    CASE_CHECK(name, "no_cancel_action_admitted",
        f.client->action_tail - f.client->action_head == queued && f.client->send_len == occupied);
    CASE_CHECK(name, "facade_still_full", f.wire_subscribes[RESOURCE_COUNT - 1] == 0);
    CASE_CHECK(name, "no_repeated_outcome", f.outcomes == 1);
    peer_requests(&f); /* deliver the real pressure action, rather than discard it */
    REQUIRE(f.client->action_head == f.client->action_tail);
    /* Polling consumes actions; a subsequent advancing call reclaims scratch. */
    REQUIRE(moq_session_tick(f.client, moq_simpair_now_us(f.pair)) == MOQ_OK);
    REQUIRE(f.client->action_head == f.client->action_tail && f.client->send_len == 0);
    f.client->action_cap = action_cap;
    f.client->send_cap = send_cap;
    CASE_CHECK(name, "no_early_cancel", f.wire_unsubscribes[1] == 0);

    if (control == CANCEL_PEER_DONE) {
        moq_done_subscribe_cfg_t done;
        moq_done_subscribe_cfg_init(&done);
        REQUIRE(moq_session_done_subscribe(f.server, f.peer_subs[1], &done,
                    moq_simpair_now_us(f.pair)) == MOQ_OK);
        wire_pump(&f);
    }
    receiver_pump(&f); peer_requests(&f); receiver_pump(&f);
    CASE_CHECK(name, "native_owner_retired", native_subscription(f.client, media) == NULL);
    CASE_CHECK(name, "facade_slot_reused", f.wire_subscribes[RESOURCE_COUNT - 1] == 1);
    moq_media_track_state_t state;
    CASE_CHECK(name, "replacement_active",
        moq_media_receiver_track_state(f.receiver, f.slot_tracks[SLOT_TRACKS - 1], &state) == MOQ_OK &&
        state == MOQ_MEDIA_TRACK_STATE_ACTIVE);
    CASE_CHECK(name, "failed_generation_stays_failed",
        moq_media_receiver_track_state(f.receiver, f.media, &state) == MOQ_OK &&
        state == MOQ_MEDIA_TRACK_STATE_FAILED);
    send_payload(&f, 2, 2, "healthy-after"); receiver_pump(&f); peer_requests(&f);
    moq_media_object_t object;
    moq_result_t rc = moq_media_receiver_poll_object(f.receiver, &object, sizeof(object));
    CASE_CHECK(name, "healthy_queue_survives", rc == MOQ_OK);
    if (rc == MOQ_OK) {
        CASE_CHECK(name, "healthy_queued_payload", object.track == f.other && named(object.payload, "healthy-before"));
        moq_media_object_cleanup(&object);
    }
    rc = moq_media_receiver_poll_object(f.receiver, &object, sizeof(object));
    CASE_CHECK(name, "healthy_delivery_continues", rc == MOQ_OK);
    if (rc == MOQ_OK) {
        CASE_CHECK(name, "healthy_new_payload", object.track == f.other && named(object.payload, "healthy-after"));
        moq_media_object_cleanup(&object);
    }
    for (int i = 0; i < 3; i++) { receiver_pump(&f); peer_requests(&f); }
    CASE_CHECK(name, "exact_cancel_count", f.wire_unsubscribes[1] == (control == CANCEL_PEER_DONE ? 0u : 1u));
    CASE_CHECK(name, "one_selection_and_outcome",
        f.selected_resources[MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE][1] == 1 && f.outcomes == 1);
    CASE_CHECK(name, "no_failed_update_sent", f.wire_updates[1] == 0);
    CASE_CHECK(name, "replacement_selected_once",
        f.selected_resources[MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE][RESOURCE_COUNT - 1] == 1);
    CASE_CHECK(name, "healthy_native_owner_survives", native_subscription(f.client, healthy) != NULL);
    CASE_CHECK(name, "healthy_track_active",
        moq_media_receiver_track_state(f.receiver, f.other, &state) == MOQ_OK &&
        state == MOQ_MEDIA_TRACK_STATE_ACTIVE);
    CASE_CHECK(name, "service_and_session_survive",
        !moq_media_receiver_is_fatal(f.receiver) && moq_session_state(f.client) == MOQ_SESS_ESTABLISHED);
    moq_media_track_event_t event;
    while (moq_media_receiver_poll_track(f.receiver, &event, sizeof(event)) == MOQ_OK)
        CASE_CHECK(name, "no_synthetic_peer_event", event.kind != MOQ_MEDIA_TRACK_ENDED && event.kind != MOQ_MEDIA_TRACK_UPDATE_OK);
    close_fixture(&f);
}

static void test_native_duplicate_identity(moq_version_t version)
{
    const char *name = version == MOQ_VERSION_DRAFT_16 ?
        "native.d16.duplicate_identity" : "native.d18.duplicate_identity";
    fixture_t f; open_fixture(&f, version, false, false); discover(&f);
    /* Observe unexpected SUBSCRIBE/media outcomes without changing ALLOW. */
    f.fail_action = MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE;
    f.fail_name = "media"; f.policy = ALLOW;
    moq_auth_token_t token = {f.type, {f.value, sizeof(f.value)}};
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ns, 1};
    cfg.track_name = MOQ_BYTES_LITERAL("media");
    cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    cfg.auth_tokens = &token; cfg.auth_token_count = 1;
    moq_subscription_t existing;
    REQUIRE(moq_session_subscribe(f.client, &cfg, moq_simpair_now_us(f.pair), &existing) == MOQ_OK);
    peer_requests(&f); receiver_pump(&f);
    REQUIRE(native_subscription(f.client, existing) != NULL && f.wire_subscribes[1] == 1);
    moq_subscription_t duplicate;
    CASE_CHECK(name, "actual_native_inval",
        moq_session_subscribe(f.client, &cfg, moq_simpair_now_us(f.pair), &duplicate) == MOQ_ERR_INVAL);
    REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.media, NULL) == MOQ_OK);
    for (int i = 0; i < 3; i++) { receiver_pump(&f); peer_requests(&f); }
    CASE_CHECK(name, "valid_selection_once", f.selected_resources[MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE][1] == 1);
    CASE_CHECK(name, "no_credential_outcome", f.outcomes == 0);
    CASE_CHECK(name, "ordinary_setup_failure", moq_media_receiver_is_fatal(f.receiver) &&
        moq_media_receiver_fatal_code(f.receiver) == MOQ_MEDIA_RECEIVER_FATAL_SETUP_FAILED);
    moq_media_track_state_t state;
    CASE_CHECK(name, "ordinary_terminal_state",
        moq_media_receiver_track_state(f.receiver, f.media, &state) == MOQ_OK && state == MOQ_MEDIA_TRACK_STATE_ENDED);
    CASE_CHECK(name, "no_duplicate_wire_request", f.wire_subscribes[1] == 1);
    CASE_CHECK(name, "existing_owner_survives", native_subscription(f.client, existing) != NULL);
    CASE_CHECK(name, "session_survives", moq_session_state(f.client) == MOQ_SESS_ESTABLISHED);
    REQUIRE(moq_session_unsubscribe(f.client, existing, moq_simpair_now_us(f.pair)) == MOQ_OK);
    peer_requests(&f);
    close_fixture(&f);
}

typedef struct native_update_failure {
    fixture_t *fixture;
    moq_alloc_t original_alloc;
    size_t original_send_cap;
    size_t occupied_send_len;
    unsigned hooks, refusals;
    bool armed;
} native_update_failure_t;

static void *native_update_alloc(size_t size, void *ctx)
{
    native_update_failure_t *failure = ctx;
    if (failure->armed) {
        failure->armed = false;
        failure->refusals++;
        return NULL;
    }
    return failure->original_alloc.alloc(size, failure->original_alloc.ctx);
}

static void native_update_free(void *ptr, size_t size, void *ctx)
{
    native_update_failure_t *failure = ctx;
    failure->original_alloc.free(ptr, size, failure->original_alloc.ctx);
}

static void *native_update_realloc(void *ptr, size_t old, size_t size, void *ctx)
{
    native_update_failure_t *failure = ctx;
    return failure->original_alloc.realloc(ptr, old, size,
                                           failure->original_alloc.ctx);
}

/* After the service has copied valid credentials, queue a real request and
 * leave one byte of send capacity. The UPDATE's real capacity probe then
 * fails its native allocation, not selection or the service's token copy. */
static void fail_native_update_probe(moq_media_receiver_t *r,
                                      moq_media_track_t *track, void *ctx)
{
    (void)r;
    native_update_failure_t *failure = ctx;
    fixture_t *f = failure->fixture;
    if (track != f->media) return;
    REQUIRE(failure->hooks++ == 0);
    REQUIRE(!failure->armed && failure->refusals == 0);
    moq_track_status_cfg_t cfg;
    moq_track_status_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ns, 1};
    cfg.track_name = MOQ_BYTES_LITERAL("occupy");
    moq_track_status_handle_t status;
    REQUIRE(moq_session_track_status(f->client, &cfg,
                moq_simpair_now_us(f->pair), &status) == MOQ_OK);
    REQUIRE(f->client->send_len > 0 &&
            f->client->send_len < failure->original_send_cap);
    REQUIRE(f->client->action_cap -
            (f->client->action_tail - f->client->action_head) > 0);
    failure->occupied_send_len = f->client->send_len;
    f->client->send_cap = f->client->send_len + 1;
    failure->armed = true;
}

static void test_native_update_failure_no_reselection(moq_version_t version)
{
    const char *name = version == MOQ_VERSION_DRAFT_16 ?
        "native.d16.update_probe_failure" : "native.d18.update_probe_failure";
    fixture_t f; open_fixture(&f, version, false, false); discover(&f);
    f.fail_action = MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE;
    f.fail_name = "media"; f.policy = ALLOW;
    REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.media, NULL) == MOQ_OK);
    REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.other, NULL) == MOQ_OK);
    receiver_pump(&f); peer_requests(&f); receiver_pump(&f);
    REQUIRE(f.wire_subscribes[1] == 1 && f.wire_subscribes[2] == 1);
    REQUIRE(f.selected_resources[MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE][1] == 0);
    REQUIRE(moq_media_receiver_unsubscribe_track(f.receiver, f.media) == MOQ_OK);

    native_update_failure_t failure = {
        .fixture = &f, .original_alloc = f.client->alloc,
        .original_send_cap = f.client->send_cap
    };
    f.client->alloc = (moq_alloc_t){
        .alloc = native_update_alloc, .free = native_update_free,
        .realloc = native_update_realloc, .ctx = &failure
    };
    moq_media_receiver_test_set_auth_hook(fail_native_update_probe, &failure);
    receiver_pump(&f);
    moq_media_receiver_test_set_auth_hook(NULL, NULL);
    f.client->send_cap = failure.original_send_cap;
    f.client->alloc = failure.original_alloc;
    REQUIRE(failure.hooks == 1 && failure.refusals == 1 && !failure.armed);
    REQUIRE(f.client->send_len == failure.occupied_send_len);
    REQUIRE(f.selected_resources[MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE][1] == 1);
    peer_requests(&f);
    CASE_CHECK(name, "failed_admission_sent_nothing", f.wire_updates[1] == 0);

    /* The published local-admission event must not become an auth outcome. */
    for (int i = 0; i < 3; i++) { receiver_pump(&f); peer_requests(&f); }
    CASE_CHECK(name, "unchanged_intent_selected_once",
        f.selected_resources[MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE][1] == 1);
    CASE_CHECK(name, "no_unrequested_retry", f.wire_updates[1] == 0);
    CASE_CHECK(name, "not_a_credential_failure", f.outcomes == 0);
    unsigned local_events = 0;
    moq_media_track_event_t event;
    while (moq_media_receiver_poll_track(f.receiver, &event, sizeof(event)) == MOQ_OK) {
        if (event.kind == MOQ_MEDIA_TRACK_UPDATE_LOCAL_FAILURE) {
            local_events++;
            CASE_CHECK(name, "native_failure_identity", event.track == f.media);
            CASE_CHECK(name, "native_failure_result", event.local_result == MOQ_ERR_NOMEM);
        }
        CASE_CHECK(name, "not_peer_terminal", event.kind != MOQ_MEDIA_TRACK_ENDED);
    }
    CASE_CHECK(name, "one_local_admission_event", local_events == 1);
    moq_media_track_state_t state;
    CASE_CHECK(name, "failed_generation_stays_failed",
        moq_media_receiver_track_state(f.receiver, f.media, &state) == MOQ_OK &&
        state == MOQ_MEDIA_TRACK_STATE_FAILED);
    CASE_CHECK(name, "healthy_track_active",
        moq_media_receiver_track_state(f.receiver, f.other, &state) == MOQ_OK &&
        state == MOQ_MEDIA_TRACK_STATE_ACTIVE);
    CASE_CHECK(name, "service_and_sessions_survive",
        !moq_media_receiver_is_fatal(f.receiver) &&
        moq_session_state(f.client) == MOQ_SESS_ESTABLISHED &&
        moq_session_state(f.server) == MOQ_SESS_ESTABLISHED);
    close_fixture(&f);
}

static void test_d22_carriage(bool stat, bool anonymous, bool deny_update)
{
    fixture_t f; open_fixture(&f, MOQ_VERSION_DRAFT_22, stat, anonymous);
    discover_d22(&f);
    REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.media, NULL) == MOQ_OK);
    REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.other, NULL) == MOQ_OK);
    receiver_pump(&f); peer_requests(&f); receiver_pump(&f); peer_requests(&f);
    CHECK(f.subscribes == 3 && f.fills == 3 && f.fetches == 0);
    CHECK(moq_fill_is_valid(f.peer_fills[1]) && moq_fill_is_valid(f.peer_fills[2]));
    uint64_t now = moq_simpair_now_us(f.pair);
    REQUIRE(moq_session_open_fill(f.server, f.peer_fills[1], now) == MOQ_OK);
    REQUIRE(moq_session_open_fill(f.server, f.peer_fills[2], now) == MOQ_OK);
    peer_requests(&f); receiver_pump(&f);
    REQUIRE(f.server->fills.live == 2 && f.client->fills.live == 2);
    CHECK(f.selects[MOQ_MEDIA_AUTH_ACTION_FETCH] == 0);
    if (!stat && !anonymous) CHECK(f.selects[MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE] == 3);
    f.fail_action = MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE;
    f.fail_name = "media";
    f.policy = deny_update ? DENY : ALLOW;
    REQUIRE(moq_media_receiver_unsubscribe_track(f.receiver, f.media) == MOQ_OK);
    for (unsigned i = 0; i < 8; i++) { receiver_pump(&f); peer_requests(&f); }
    CHECK(f.fetches == 0 && f.selects[MOQ_MEDIA_AUTH_ACTION_FETCH] == 0);
    moq_media_track_state_t state;
    CHECK(moq_media_receiver_track_state(f.receiver, f.other, &state) == MOQ_OK &&
          state == MOQ_MEDIA_TRACK_STATE_ACTIVE);
    CHECK(!moq_media_receiver_is_fatal(f.receiver));
    if (deny_update) {
        CHECK(f.outcomes == 1 && f.scope == MOQ_MEDIA_AUTH_SCOPE_TRACK && f.latched);
        CHECK(f.failed_track == f.media && f.outcome == MOQ_MEDIA_AUTH_OUTCOME_POLICY_DENIED);
        CHECK(f.updates == 0);
        CHECK(moq_media_receiver_track_state(f.receiver, f.media, &state) == MOQ_OK &&
              state == MOQ_MEDIA_TRACK_STATE_FAILED);
        CHECK(moq_media_receiver_subscribe_track(f.receiver, f.media, NULL) == MOQ_ERR_WRONG_STATE);
        CHECK(f.wire_unsubscribes[1] == 1);
        CHECK(f.server->fills.live == 1 && f.client->fills.live == 1);
        REQUIRE(moq_session_end_fill(f.server, f.peer_fills[2], now) == MOQ_OK);
        peer_requests(&f); receiver_pump(&f);
        moq_media_track_event_t event;
        while (moq_media_receiver_poll_track(f.receiver, &event, sizeof(event)) == MOQ_OK)
            CHECK(event.kind != MOQ_MEDIA_TRACK_ENDED && event.kind != MOQ_MEDIA_TRACK_UPDATE_OK);
    } else {
        CHECK(f.outcomes == 0 && f.updates == 1);
    }
    close_fixture(&f);
}

static void test_d16_parameter_limit(bool update, bool occupied)
{
    fixture_t f; open_fixture(&f, MOQ_VERSION_DRAFT_16, false, false); discover(&f);
    f.fail_action = update ? MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE
                           : MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE;
    f.fail_name = "media"; f.policy = PARAM_LIMIT;
    REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.media, NULL) == MOQ_OK);
    REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.other, NULL) == MOQ_OK);
    if (update) {
        receiver_pump(&f); peer_requests(&f); receiver_pump(&f);
        REQUIRE(moq_media_receiver_unsubscribe_track(f.receiver, f.media) == MOQ_OK);
    }
    if (occupied) {
        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){ns, 1};
        cfg.track_name = MOQ_BYTES_LITERAL("occupy");
        moq_track_status_handle_t status;
        REQUIRE(moq_session_track_status(f.client, &cfg,
                    moq_simpair_now_us(f.pair), &status) == MOQ_OK);
        REQUIRE(f.client->send_len > 0 && action_queue_avail(f.client) > 0);
    }
    for (unsigned i = 0; i < 4; i++) { receiver_pump(&f); peer_requests(&f); }
    const char *name = update ? "d16.update.parameter_limit" : "d16.subscribe.parameter_limit";
    CASE_CHECK(name, "one_track_outcome", f.outcomes == 1 && f.latched &&
        f.scope == MOQ_MEDIA_AUTH_SCOPE_TRACK && f.failed_track == f.media);
    CASE_CHECK(name, "capacity_not_invalid_credentials",
        f.outcome == MOQ_MEDIA_AUTH_OUTCOME_TOKEN_LIMIT && f.result == MOQ_ERR_BUFFER);
    CASE_CHECK(name, "no_refused_request", update ? f.wire_updates[1] == 0
                                                   : f.wire_subscribes[1] == 0);
    CASE_CHECK(name, "selected_once", f.selected_resources[f.fail_action][1] == 1);
    moq_media_track_state_t state;
    CASE_CHECK(name, "failed_generation", moq_media_receiver_track_state(
        f.receiver, f.media, &state) == MOQ_OK && state == MOQ_MEDIA_TRACK_STATE_FAILED);
    CASE_CHECK(name, "healthy_sibling", moq_media_receiver_track_state(
        f.receiver, f.other, &state) == MOQ_OK && state == MOQ_MEDIA_TRACK_STATE_ACTIVE);
    CASE_CHECK(name, "service_survives", !moq_media_receiver_is_fatal(f.receiver) &&
        moq_session_state(f.client) == MOQ_SESS_ESTABLISHED &&
        moq_session_state(f.server) == MOQ_SESS_ESTABLISHED);
    close_fixture(&f);
}

/* Auth failure does not fabricate peer ENDED or make a healthy sibling lose
 * the completion/drain contract when a terminal catalog overtakes its done. */
static void test_completion_after_auth_failure(moq_version_t version)
{
    fixture_t f; open_fixture(&f, version, false, false); discover(&f);
    REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.media, NULL) == MOQ_OK);
    REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.other, NULL) == MOQ_OK);
    receiver_pump(&f); peer_requests(&f); receiver_pump(&f);
    f.fail_action = MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE;
    f.fail_name = "media";
    f.policy = DENY;
    REQUIRE(moq_media_receiver_unsubscribe_track(f.receiver, f.media) == MOQ_OK);
    receiver_pump(&f);
    CHECK(f.outcomes == 1 && f.failed_track == f.media && f.latched);
    static const char terminal[] = "{\"version\":1,\"isComplete\":true,\"tracks\":[]}";
    moq_media_receiver_test_ingest(f.receiver, 1, 0, terminal, sizeof(terminal) - 1);
    receiver_pump(&f); peer_requests(&f); receiver_pump(&f);
    unsigned failed_removed = 0, healthy_removed = 0, healthy_ended = 0;
    moq_media_track_event_t event;
    while (moq_media_receiver_poll_track(f.receiver, &event, sizeof(event)) == MOQ_OK) {
        CHECK(event.kind != MOQ_MEDIA_TRACK_ENDED);
        if (event.kind == MOQ_MEDIA_TRACK_REMOVED) {
            if (event.track == f.media) failed_removed++;
            if (event.track == f.other) healthy_removed++;
        }
    }
    CHECK(failed_removed == 1 && healthy_removed == 0);
    moq_media_track_state_t state;
    CHECK(moq_media_receiver_track_state(f.receiver, f.other, &state) == MOQ_OK &&
          state == MOQ_MEDIA_TRACK_STATE_ACTIVE);
    CHECK(f.wire_unsubscribes[2] == 0);
    moq_done_subscribe_cfg_t done;
    moq_done_subscribe_cfg_init(&done);
    done.status_code = 2; /* Track Ended: removed completion cannot rearm VOD. */
    done.stream_count = 0;
    REQUIRE(moq_session_done_subscribe(f.server, f.peer_subs[2], &done,
                moq_simpair_now_us(f.pair)) == MOQ_OK);
    peer_requests(&f); receiver_pump(&f); peer_requests(&f); receiver_pump(&f);
    while (moq_media_receiver_poll_track(f.receiver, &event, sizeof(event)) == MOQ_OK) {
        CHECK(event.track != f.media);
        if (event.track == f.other && event.kind == MOQ_MEDIA_TRACK_ENDED) {
            healthy_ended++;
            CHECK(event.has_done_status && event.done_status == 2);
        }
        if (event.track == f.other && event.kind == MOQ_MEDIA_TRACK_REMOVED) {
            CHECK(healthy_ended == 1);
            healthy_removed++;
        }
    }
    CHECK(healthy_ended == 1 && healthy_removed == 1 && f.outcomes == 1);
    CHECK(!moq_media_receiver_is_fatal(f.receiver) && f.wire_subscribes[2] == 1);
    close_fixture(&f);
}

typedef struct terminal_composition {
    const char *name;
    bool fill;
    unsigned removed[BASE_RESOURCES], healthy_ended, fill_ended[BASE_RESOURCES];
    unsigned fins[BASE_RESOURCES], healthy_objects, fill_objects;
} terminal_composition_t;

static size_t composition_identity(fixture_t *f, moq_media_track_t *track)
{
    size_t resource = track == f->media ? 1 : track == f->other ? 2 :
                      track == f->sap ? 3 : track == f->timeline ? 4 : 0;
    REQUIRE(resource != 0);
    moq_media_track_desc_t desc;
    REQUIRE(moq_media_receiver_track_desc_copy(f->receiver, track,
                                               &desc, sizeof(desc)) == MOQ_OK);
    CHECK(desc.struct_size == sizeof(desc) && named(desc.name, resource_names[resource]));
    return resource;
}

static void composition_events(fixture_t *f, terminal_composition_t *c)
{
    moq_media_track_event_t event;
    moq_result_t rc;
    unsigned polled = 0;
    while ((rc = moq_media_receiver_poll_track(f->receiver, &event, sizeof(event))) == MOQ_OK) {
        REQUIRE(++polled <= 16);
        size_t resource = composition_identity(f, event.track);
        CASE_CHECK(c->name, "event_identity", event.struct_size == sizeof(event) &&
            event.desc && named(event.desc->name, resource_names[resource]));
        CASE_CHECK(c->name, "no_local_failure", event.local_result == MOQ_OK);
        if (event.kind == MOQ_MEDIA_TRACK_REMOVED) {
            CASE_CHECK(c->name, "one_removal", c->removed[resource]++ == 0);
            if (resource == 2)
                CASE_CHECK(c->name, "end_before_remove", c->healthy_ended == 1);
        } else if (event.kind == MOQ_MEDIA_TRACK_ENDED) {
            CASE_CHECK(c->name, "only_real_healthy_done", resource == 2 &&
                event.has_done_status && event.done_status == 2 &&
                c->healthy_ended++ == 0);
        } else if (event.kind == MOQ_MEDIA_TRACK_FILL_TERMINAL) {
            REQUIRE(c->fill && (resource == 1 || resource == 2));
            CASE_CHECK(c->name, "one_fill_terminal", c->fill_ended[resource]++ == 0);
            CASE_CHECK(c->name, "fill_identity", event.initiating_request_id ==
                f->peer_fill_ids[resource]);
            CASE_CHECK(c->name, "actual_child_outcome", event.fill_reason ==
                (resource == 1 ? MOQ_FILL_PARENT_TERMINATED : MOQ_FILL_COMPLETE) &&
                event.fill_error_domain == MOQ_FILL_ERROR_NONE && event.fill_error_code == 0);
            if (resource == 2)
                CASE_CHECK(c->name, "fill_object_before_terminal", c->fill_objects == 1);
        } else {
            CASE_CHECK(c->name, "unexpected_track_event", false);
        }
    }
    /* This session-only fixture has no endpoint: an empty poll is CLOSED. */
    CASE_CHECK(c->name, "event_poll_end", rc == MOQ_ERR_CLOSED);
}

static void composition_fin(fixture_t *f, terminal_composition_t *c,
                             size_t resource, uint64_t group)
{
    moq_media_stream_terminal_t st;
    REQUIRE(moq_media_receiver_poll_stream_terminal(f->receiver, &st, sizeof(st)) == MOQ_OK);
    size_t actual = composition_identity(f, st.track);
    CASE_CHECK(c->name, "held_fin_identity", actual == resource &&
        st.struct_size == sizeof(st) && st.kind == MOQ_MEDIA_STREAM_FIN &&
        st.group_id == group && st.subgroup_id == 0 && st.has_last_object &&
        st.last_object_id == 0 && !st.has_cut_object && st.error_code == 0);
    CASE_CHECK(c->name, "fin_once_in_order", ++c->fins[resource] == group);
}

/* Two real FINs fill the public lane. Auth cancellation must retain its
 * native owner on refusal and its old stream identities through retirement.
 * The catalog seam models completion overtaking already-received media;
 * Objects, FINs, done and D22 fill outcomes all enter through real sessions. */
static void test_terminal_composition(moq_version_t version)
{
    int before = failures;
    terminal_composition_t c = {
        .name = version == MOQ_VERSION_DRAFT_16 ? "terminal_composition.d16" :
                version == MOQ_VERSION_DRAFT_18 ? "terminal_composition.d18" :
                                                 "terminal_composition.d22_open_fill",
        .fill = version == MOQ_VERSION_DRAFT_22
    };
    fixture_t f;
    open_fixture_lanes(&f, version, false, false, false, 2);
    if (c.fill) discover_d22(&f);
    else discover(&f);
    REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.media, NULL) == MOQ_OK);
    REQUIRE(moq_media_receiver_subscribe_track(f.receiver, f.other, NULL) == MOQ_OK);
    for (unsigned i = 0; i < 4; i++) { receiver_pump(&f); peer_requests(&f); }
    REQUIRE(f.wire_subscribes[1] == 1 && f.wire_subscribes[2] == 1);
    moq_subscription_t media = client_subscription(&f, 1);
    moq_subscription_t healthy = client_subscription(&f, 2);
    REQUIRE(native_subscription(f.client, media)->state == MOQ_SUB_ESTABLISHED &&
            native_subscription(f.client, healthy)->state == MOQ_SUB_ESTABLISHED);
    if (c.fill) {
        REQUIRE(f.fills == 3 && f.fetches == 0);
        REQUIRE(moq_fill_is_valid(f.peer_fills[1]) && moq_fill_is_valid(f.peer_fills[2]));
        REQUIRE(f.peer_fill_ids[1] != f.peer_fill_ids[2]);
        REQUIRE(moq_session_open_fill(f.server, f.peer_fills[1],
                    moq_simpair_now_us(f.pair)) == MOQ_OK);
        REQUIRE(moq_session_open_fill(f.server, f.peer_fills[2],
                    moq_simpair_now_us(f.pair)) == MOQ_OK);
        peer_requests(&f); receiver_pump(&f);
        REQUIRE(f.client->fills.live == 2 && f.server->fills.live == 2);
    }
    composition_events(&f, &c);
    REQUIRE(failures == before);

    send_payload(&f, 1, 1, "app-owned"); receiver_pump(&f);
    moq_media_object_t owned, healthy_owned;
    REQUIRE(moq_media_receiver_poll_object(f.receiver, &owned, sizeof(owned)) == MOQ_OK);
    REQUIRE(owned.track == f.media && named(owned.payload, "app-owned") && owned.payload_ref);
    moq_rcbuf_t *observer = owned.payload_ref;
    moq_rcbuf_incref(observer);
    send_payload(&f, 2, 1, "healthy-before"); receiver_pump(&f);
    REQUIRE(moq_media_receiver_poll_object(f.receiver, &healthy_owned, sizeof(healthy_owned)) == MOQ_OK);
    REQUIRE(healthy_owned.track == f.other && named(healthy_owned.payload, "healthy-before"));

    /* No terminal polls: the next Objects and FINs must stay upstream. */
    send_payload(&f, 1, 2, "must-be-purged");
    send_payload(&f, 2, 2, "healthy-after");
    for (unsigned i = 0; i < 4; i++) { receiver_pump(&f); peer_requests(&f); }
    moq_media_receiver_stats_t stats;
    REQUIRE(moq_media_receiver_get_stats(f.receiver, &stats, sizeof(stats)) == MOQ_OK);
    REQUIRE(stats.objects_received == 2 && stats.objects_queued == 0 && stats.objects_dropped == 0);
    f.fail_action = MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE;
    f.fail_name = "media"; f.policy = DENY;
    REQUIRE(moq_media_receiver_unsubscribe_track(f.receiver, f.media) == MOQ_OK);
    receiver_pump(&f);
    REQUIRE(f.outcomes == 1 && f.failed_track == f.media && f.latched &&
        f.scope == MOQ_MEDIA_AUTH_SCOPE_TRACK &&
        f.outcome == MOQ_MEDIA_AUTH_OUTCOME_POLICY_DENIED && f.result == MOQ_OK);
    moq_media_track_state_t state;
    REQUIRE(moq_media_receiver_track_state(f.receiver, f.media, &state) == MOQ_OK &&
            state == MOQ_MEDIA_TRACK_STATE_FAILED);

    size_t cap = occupy_actions(&f);
    REQUIRE(moq_session_unsubscribe(f.client, media, moq_simpair_now_us(f.pair)) == MOQ_ERR_WOULD_BLOCK);
    size_t queued = f.client->action_tail - f.client->action_head;
    size_t occupied = f.client->send_len;
    for (unsigned i = 0; i < 2; i++) receiver_pump(&f);
    CASE_CHECK(c.name, "cancel_refusal_keeps_owner", native_subscription(f.client, media) != NULL &&
        native_subscription(f.client, media)->state == MOQ_SUB_ESTABLISHED &&
        f.client->action_tail - f.client->action_head == queued &&
        f.client->send_len == occupied && f.wire_unsubscribes[1] == 0);
    CASE_CHECK(c.name, "healthy_owner_independent", native_subscription(f.client, healthy) != NULL &&
        moq_media_receiver_track_state(f.receiver, f.other, &state) == MOQ_OK &&
        state == MOQ_MEDIA_TRACK_STATE_ACTIVE && named(healthy_owned.payload, "healthy-before"));
    if (c.fill) CASE_CHECK(c.name, "refused_cancel_keeps_both_fills",
        f.client->fills.live == 2 && f.server->fills.live == 2);
    REQUIRE(failures == before);
    release_actions(&f, cap);
    for (unsigned i = 0; i < 4; i++) { receiver_pump(&f); peer_requests(&f); }
    REQUIRE(f.wire_unsubscribes[1] == 1 && native_subscription(f.client, media) == NULL);
    REQUIRE(native_subscription(f.client, healthy) != NULL && f.wire_unsubscribes[2] == 0);
    send_payload(&f, 2, 3, "healthy-tail");
    if (c.fill) {
        REQUIRE(f.client->fills.live == 1 && f.server->fills.live == 1);
        moq_fetch_object_cfg_t cfg;
        moq_fetch_object_cfg_init(&cfg);
        REQUIRE(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"healthy-fill",
                    sizeof("healthy-fill") - 1, &cfg.payload) == MOQ_OK);
        REQUIRE(moq_session_write_fill_object(f.server, f.peer_fills[2], &cfg,
                    moq_simpair_now_us(f.pair)) == MOQ_OK);
        moq_rcbuf_decref(cfg.payload);
        REQUIRE(moq_session_end_fill(f.server, f.peer_fills[2],
                    moq_simpair_now_us(f.pair)) == MOQ_OK);
    }
    moq_done_subscribe_cfg_t done;
    moq_done_subscribe_cfg_init(&done);
    done.status_code = 2;
    done.stream_count = 3; /* Exactly three LIVE subgroups; core adds the D22 fill. */
    REQUIRE(moq_session_done_subscribe(f.server, f.peer_subs[2], &done,
                moq_simpair_now_us(f.pair)) == MOQ_OK);
    peer_requests(&f);
    static const char terminal[] = "{\"version\":1,\"isComplete\":true,\"tracks\":[]}";
    moq_media_receiver_test_ingest(f.receiver, 1, 0, terminal, sizeof(terminal) - 1);
    for (unsigned i = 0; i < 4; i++) { receiver_pump(&f); peer_requests(&f); }
    composition_events(&f, &c);
    REQUIRE(c.removed[1] == 1 && c.healthy_ended == 0 && c.removed[2] == 0);
    REQUIRE(c.fill_ended[1] == 0 && c.fill_ended[2] == 0);
    REQUIRE(moq_media_receiver_get_stats(f.receiver, &stats, sizeof(stats)) == MOQ_OK);
    REQUIRE(stats.catalog_complete && stats.objects_received == 2 && stats.objects_queued == 0);
    REQUIRE(failures == before);

    /* These records predate auth refusal/removal, but still name those handles. */
    composition_fin(&f, &c, 1, 1);
    composition_fin(&f, &c, 2, 1);
    moq_media_stream_terminal_t st;
    REQUIRE(moq_media_receiver_poll_stream_terminal(f.receiver, &st, sizeof(st)) == MOQ_DONE);
    for (unsigned round = 0; round < 32; round++) {
        receiver_pump(&f); peer_requests(&f);
        composition_events(&f, &c);
        moq_media_object_t object;
        moq_result_t rc;
        unsigned polled = 0;
        while ((rc = moq_media_receiver_poll_object(f.receiver, &object, sizeof(object))) == MOQ_OK) {
            REQUIRE(++polled <= 8);
            CASE_CHECK(c.name, "only_healthy_media_survives", object.track == f.other);
            (void)composition_identity(&f, object.track);
            if (object.delivery_origin == MOQ_MEDIA_DELIVERY_FILL) {
                CASE_CHECK(c.name, "actual_healthy_fill", c.fill && c.fill_objects++ == 0 &&
                    object.initiating_request_id == f.peer_fill_ids[2] &&
                    object.group_id == 0 && object.object_id == 0 && named(object.payload, "healthy-fill"));
            } else {
                CASE_CHECK(c.name, "healthy_tail_once_in_order", object.delivery_origin == MOQ_MEDIA_DELIVERY_LIVE &&
                    object.group_id == c.healthy_objects + 2 && object.object_id == 0 &&
                    named(object.payload, c.healthy_objects == 0 ? "healthy-after" : "healthy-tail"));
                c.healthy_objects++;
            }
            moq_media_object_cleanup(&object);
        }
        /* The established private fixture has no endpoint: empty is CLOSED. */
        CASE_CHECK(c.name, "empty_media_poll", rc == MOQ_ERR_CLOSED);
        polled = 0;
        while ((rc = moq_media_receiver_poll_stream_terminal(f.receiver, &st, sizeof(st))) == MOQ_OK) {
            REQUIRE(++polled <= 8);
            size_t resource = composition_identity(&f, st.track);
            REQUIRE(resource == 1 || resource == 2);
            CASE_CHECK(c.name, "remaining_fin_identity",
                st.struct_size == sizeof(st) && st.kind == MOQ_MEDIA_STREAM_FIN &&
                st.group_id == c.fins[resource] + 1 &&
                st.group_id <= (resource == 1 ? 2u : 3u) &&
                st.subgroup_id == 0 && st.has_last_object && st.last_object_id == 0 &&
                !st.has_cut_object && st.error_code == 0);
            c.fins[resource]++;
        }
        CASE_CHECK(c.name, "terminal_poll_end", rc == MOQ_DONE);
    }
    composition_events(&f, &c);
    CASE_CHECK(c.name, "all_held_fins_retained", c.fins[1] == 2 && c.fins[2] == 3);
    CASE_CHECK(c.name, "healthy_progress_after_polling", c.healthy_objects == 2 &&
        c.fill_objects == (c.fill ? 1u : 0u) && c.healthy_ended == 1);
    for (size_t i = 1; i < BASE_RESOURCES; i++)
        CASE_CHECK(c.name, "all_removals_once", c.removed[i] == 1);
    CASE_CHECK(c.name, "child_terminals_once", c.fill_ended[1] == (c.fill ? 1u : 0u) &&
        c.fill_ended[2] == (c.fill ? 1u : 0u));
    CASE_CHECK(c.name, "no_auth_reselection_or_failed_update", f.outcomes == 1 &&
        f.selected_resources[MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE][1] == 1 && f.wire_updates[1] == 0);
    CASE_CHECK(c.name, "no_rearm_or_healthy_cancellation", f.wire_subscribes[1] == 1 &&
        f.wire_subscribes[2] == 1 && f.wire_unsubscribes[1] == 1 && f.wire_unsubscribes[2] == 0);
    CASE_CHECK(c.name, "native_owners_released", native_subscription(f.client, media) == NULL &&
        native_subscription(f.client, healthy) == NULL);
    CASE_CHECK(c.name, "removed_handles_inert",
        moq_media_receiver_subscribe_track(f.receiver, f.media, NULL) == MOQ_ERR_WRONG_STATE &&
        moq_media_receiver_subscribe_track(f.receiver, f.other, NULL) == MOQ_ERR_WRONG_STATE);
    if (c.fill) CASE_CHECK(c.name, "all_native_children_released",
        f.client->fills.live == 0 && f.server->fills.live == 0);
    REQUIRE(moq_media_receiver_get_stats(f.receiver, &stats, sizeof(stats)) == MOQ_OK);
    CASE_CHECK(c.name, "no_media_loss", stats.objects_received == (c.fill ? 5u : 4u) &&
        stats.objects_queued == 0 && stats.objects_dropped == 0);
    CASE_CHECK(c.name, "polled_objects_still_owned", named(owned.payload, "app-owned") &&
        named(healthy_owned.payload, "healthy-before") && moq_rcbuf_refcount(observer) >= 2);
    moq_media_object_cleanup(&owned);
    moq_media_object_cleanup(&healthy_owned);
    CASE_CHECK(c.name, "observer_only_ref", moq_rcbuf_refcount(observer) == 1);
    moq_rcbuf_decref(observer);
    CASE_CHECK(c.name, "service_survives", !moq_media_receiver_is_fatal(f.receiver));
    close_fixture(&f);
    if (failures == before) printf("PASS: %s\n", c.name);
}

int main(void)
{
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18};
    for (size_t i = 0; i < 2; i++) {
        for (int p = DENY; p <= COPY_FAIL; p++) test_catalog_failure(versions[i], (enum policy)p);
        test_carriage(versions[i], false, false);
        test_carriage(versions[i], true, false);
        test_carriage(versions[i], false, true);
        test_request_and_track_failure(versions[i], false, 1);
        test_request_and_track_failure(versions[i], true, 1);
        test_request_and_track_failure(versions[i], false, 3);
        test_request_and_track_failure(versions[i], false, 4);
        test_retry_copy(versions[i]);
        test_operation_retry(versions[i], MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE);
        test_operation_retry(versions[i], MOQ_MEDIA_AUTH_ACTION_FETCH);
        test_operation_retry(versions[i], MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE);
        test_intent_change(versions[i], false);
        test_intent_change(versions[i], true);
        test_capacity_failure(versions[i]);
        test_flow_failure_ownership(versions[i]);
        test_cancel_cleanup(versions[i], CANCEL_WOULD_BLOCK);
        test_cancel_cleanup(versions[i], CANCEL_PEER_DONE);
        test_native_duplicate_identity(versions[i]);
        test_native_update_failure_no_reselection(versions[i]);
        test_completion_after_auth_failure(versions[i]);
        test_terminal_composition(versions[i]);
    }
    test_cancel_cleanup(MOQ_VERSION_DRAFT_16, CANCEL_SCRATCH);
    for (int p = DENY; p <= COPY_FAIL; p++)
        test_catalog_failure(MOQ_VERSION_DRAFT_22, (enum policy)p);
    test_d22_carriage(false, false, false);
    test_d22_carriage(true, false, false);
    test_d22_carriage(false, true, false);
    test_d22_carriage(false, false, true);
    test_terminal_composition(MOQ_VERSION_DRAFT_22);
    test_retry_copy(MOQ_VERSION_DRAFT_22);
    for (unsigned occupied = 0; occupied < 2; occupied++) {
        test_d16_parameter_limit(false, occupied != 0);
        test_d16_parameter_limit(true, occupied != 0);
    }
    if (!failures) puts("PASS: media_receiver_auth");
    return failures ? 1 : 0;
}
