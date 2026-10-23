#include <moq/sim.h>
#include "../../core/src/session/session_internal.h"
#include <stdio.h>
#include <stdlib.h>

/* Prerequisite failures are distinct from the named behavioral predicates. */
#define REQUIRE(x) do { if (!(x)) { \
    fprintf(stderr, "PREREQUISITE: %s:%d: %s\n", __FILE__, __LINE__, #x); \
    exit(2); } } while (0)
#define CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #x); \
    failures++; } } while (0)

static int failures;
static const moq_bytes_t ns[] = {{(const uint8_t *)"auth", 4}};
static const uint8_t value[] = {0x80, 0, 0xff, 0x42};
enum operation { SUBSCRIBE, PUBLISH, NAMESPACE, UPDATE, FETCH,
                 TRACK_STATUS, NS_SUBSCRIBE, TRACKS_SUBSCRIBE, PUB_UPDATE,
                 SUBSCRIBE_FILL, UPDATE_FILL, OP_COUNT };
static const char *const names[] = {"subscribe", "publish", "namespace", "update", "fetch",
    "track_status", "namespace_subscribe", "tracks_subscribe", "publication_update",
    "subscribe_fill", "update_fill"};

typedef struct fixture {
    moq_simpair_t *pair;
    moq_session_t *c, *s;
    moq_subscription_t sub;
    moq_publication_t pub;
    moq_alloc_t original;
    bool fail_probe;
    size_t probe_size, probes, wiped;
    int64_t balance;
} fixture_t;

static void *probe_alloc(size_t n, void *ctx)
{
    fixture_t *f = ctx;
    if (n == f->probe_size) {
        f->probes++;
        if (f->fail_probe) return NULL;
    }
    void *p = f->original.alloc(n, f->original.ctx);
    if (p) {
        f->balance++;
        if (n == f->probe_size) memset(p, 0xbd, n);
    }
    return p;
}

static void *probe_realloc(void *p, size_t old, size_t n, void *ctx)
{
    fixture_t *f = ctx;
    return f->original.realloc(p, old, n, f->original.ctx);
}

static void probe_free(void *p, size_t n, void *ctx)
{
    fixture_t *f = ctx;
    if (n == f->probe_size) {
        const uint8_t *b = p;
        bool zero = true;
        for (size_t i = 0; i < n; i++) if (b[i]) zero = false;
        CHECK(zero);
        f->wiped++;
    }
    if (p) f->balance--;
    f->original.free(p, n, f->original.ctx);
}

static void drain_events(moq_session_t *s)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1)) moq_event_cleanup(&e);
}

static void pump(fixture_t *f)
{
    REQUIRE(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
}

static void open_pair(fixture_t *f, moq_version_t version, enum operation op)
{
    memset(f, 0, sizeof(*f));
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = version;
    cfg.seed = 42;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 32;
    REQUIRE(moq_simpair_create(&cfg, &f->pair) == MOQ_OK);
    REQUIRE(moq_simpair_start(f->pair) == MOQ_OK);
    pump(f);
    f->c = moq_simpair_client(f->pair);
    f->s = moq_simpair_server(f->pair);
    REQUIRE(moq_session_state(f->c) == MOQ_SESS_ESTABLISHED);
    REQUIRE(moq_session_state(f->s) == MOQ_SESS_ESTABLISHED);
    drain_events(f->c);
    drain_events(f->s);
    moq_namespace_t space = {ns, 1};
    REQUIRE(moq_session_note_object_published(f->s, &space,
                MOQ_BYTES_LITERAL("track"), 10, 5) == MOQ_OK);
    if (op == UPDATE || op == UPDATE_FILL) {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ns, 1};
        sc.track_name = MOQ_BYTES_LITERAL("track");
        REQUIRE(moq_session_subscribe(f->c, &sc, 1000, &f->sub) == MOQ_OK);
        pump(f);
        moq_event_t e;
        REQUIRE(moq_session_poll_events(f->s, &e, 1) == 1);
        REQUIRE(e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t peer = e.u.subscribe_request.sub;
        moq_event_cleanup(&e);
        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        REQUIRE(moq_session_accept_subscribe(f->s, peer, &ac, 1000) == MOQ_OK);
        pump(f);
        drain_events(f->c);
        drain_events(f->s);
    }
    if (op == PUB_UPDATE) {
        moq_publish_cfg_t pc;
        moq_publish_cfg_init_sized(&pc, sizeof(pc));
        pc.track_namespace = (moq_namespace_t){ns, 1};
        pc.track_name = MOQ_BYTES_LITERAL("track");
        moq_publication_t peer;
        REQUIRE(moq_session_publish(f->s, &pc, 1000, &peer) == MOQ_OK);
        pump(f);
        moq_event_t e;
        REQUIRE(moq_session_poll_events(f->c, &e, 1) == 1);
        REQUIRE(e.kind == MOQ_EVENT_PUBLISH_REQUEST);
        f->pub = e.u.publish_request.pub;
        moq_event_cleanup(&e);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        REQUIRE(moq_session_accept_publish(f->c, f->pub, &ac, 1000) == MOQ_OK);
        pump(f);
        drain_events(f->c);
        drain_events(f->s);
    }
    REQUIRE(f->c->action_head == f->c->action_tail);
    /* Reclaim the drained output before taking transactional snapshots. */
    REQUIRE(moq_session_tick(f->c, 1000) == MOQ_OK);
    REQUIRE(f->c->send_len == 0);
}

static moq_result_t issue(fixture_t *f, enum operation op,
                          const moq_auth_token_t *token)
{
    moq_namespace_t space = {ns, 1};
    moq_bytes_t track = MOQ_BYTES_LITERAL("track");
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.has_fill_timeout = true;
    fill.fill_timeout_ms = UINT64_MAX;
    moq_location_filter_cfg_t location;
    moq_location_filter_cfg_init(&location);
    location.kind = MOQ_LOCATION_FILTER_OBJECT_END;
    location.start_group = 0;
    location.end_group = 1;
    location.end_object = 1;
    switch (op) {
    case SUBSCRIBE:
    case SUBSCRIBE_FILL: {
        moq_subscribe_cfg_t c;
        moq_subscribe_cfg_init_sized(&c, sizeof(c));
        c.track_namespace = space; c.track_name = track;
        c.auth_tokens = token; c.auth_token_count = 1;
        if (f->c->profile->version == MOQ_VERSION_DRAFT_22) {
            c.location = &location;
            c.delivery_timeout_flags = MOQ_SUBSCRIBE_OBJECT_DELIVERY_TIMEOUT |
                                      MOQ_SUBSCRIBE_SUBGROUP_DELIVERY_TIMEOUT;
            c.object_delivery_timeout_ms = UINT64_MAX;
            c.subgroup_delivery_timeout_ms = 0;
            c.has_rendezvous_timeout = true;
            c.rendezvous_timeout_ms = UINT64_MAX;
        }
        if (op == SUBSCRIBE_FILL) c.fill = &fill;
        moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
        moq_result_t rc = moq_session_subscribe(f->c, &c, 1000, &h);
        CHECK(rc == MOQ_OK || h._opaque == 0);
        return rc;
    }
    case PUBLISH: {
        moq_publish_cfg_t c;
        moq_publish_cfg_init_sized(&c, sizeof(c));
        c.track_namespace = space; c.track_name = track;
        c.auth_tokens = token; c.auth_token_count = 1;
        /* Exercise an eight-byte D16 / nine-byte D18 alias, not a dummy ID. */
        c.has_track_alias = true; c.track_alias = MOQ_QUIC_VARINT_MAX;
        moq_publication_t h = MOQ_PUBLICATION_INVALID;
        moq_result_t rc = moq_session_publish(f->c, &c, 1000, &h);
        CHECK(rc == MOQ_OK || h._opaque == 0);
        return rc;
    }
    case NAMESPACE: {
        moq_publish_namespace_cfg_t c;
        moq_publish_namespace_cfg_init(&c);
        c.track_namespace = space;
        c.auth_tokens = token; c.auth_token_count = 1;
        moq_announcement_t h = MOQ_ANNOUNCEMENT_INVALID;
        moq_result_t rc = moq_session_publish_namespace(f->c, &c, 1000, &h);
        CHECK(rc == MOQ_OK || h._opaque == 0);
        return rc;
    }
    case UPDATE:
    case UPDATE_FILL: {
        moq_subscription_update_cfg_t c;
        moq_subscription_update_cfg_init_sized(&c, sizeof(c));
        c.has_subscriber_priority = true; c.subscriber_priority = 42;
        c.auth_tokens = token; c.auth_token_count = 1;
        if (f->c->profile->version == MOQ_VERSION_DRAFT_22) {
            c.location = &location;
            c.delivery_timeout_flags = MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT |
                                      MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT;
            c.object_delivery_timeout_ms = UINT64_MAX;
            c.subgroup_delivery_timeout_ms = 0;
        }
        if (op == UPDATE_FILL) c.fill = &fill;
        return moq_session_update_subscription(f->c, f->sub, &c, 1000);
    }
    case FETCH: {
        moq_fetch_cfg_t c;
        moq_fetch_cfg_init_sized(&c, sizeof(c));
        c.track_namespace = space; c.track_name = track;
        c.end_group = 1; c.end_object = 1;
        c.auth_tokens = token; c.auth_token_count = 1;
        if (f->c->profile->version == MOQ_VERSION_DRAFT_22) {
            c.location = &location;
            c.has_fill_timeout = true;
            c.fill_timeout_ms = UINT64_MAX;
        }
        moq_fetch_t h = MOQ_FETCH_INVALID;
        moq_result_t rc = moq_session_fetch(f->c, &c, 1000, &h);
        CHECK(rc == MOQ_OK || h._opaque == 0);
        return rc;
    }
    case TRACK_STATUS: {
        moq_track_status_cfg_t c;
        moq_track_status_cfg_init(&c);
        c.track_namespace = space; c.track_name = track;
        c.auth_tokens = token; c.auth_token_count = 1;
        moq_track_status_handle_t h;
        return moq_session_track_status(f->c, &c, 1000, &h);
    }
    case NS_SUBSCRIBE: {
        moq_subscribe_namespace_cfg_t c;
        moq_subscribe_namespace_cfg_init(&c);
        c.track_namespace_prefix = space;
        c.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        c.auth_tokens = token; c.auth_token_count = 1;
        moq_ns_sub_handle_t h;
        return moq_session_subscribe_namespace(f->c, &c, 1000, &h);
    }
    case TRACKS_SUBSCRIBE: {
        moq_subscribe_tracks_cfg_t c;
        moq_subscribe_tracks_cfg_init(&c);
        c.track_namespace_prefix = space;
        c.auth_tokens = token; c.auth_token_count = 1;
        moq_track_sub_handle_t h;
        return moq_session_subscribe_tracks(f->c, &c, 1000, &h);
    }
    case PUB_UPDATE: {
        moq_publication_update_cfg_t c;
        moq_publication_update_cfg_init(&c);
        c.has_subscriber_priority = true; c.subscriber_priority = 42;
        c.auth_tokens = token; c.auth_token_count = 1;
        return moq_session_update_publication(f->c, f->pub, &c, 1000);
    }
    default: return MOQ_ERR_INVAL;
    }
}

static void peer_tokens(fixture_t *f, enum operation op,
                        const moq_auth_token_t *want, size_t count, int extra)
{
    pump(f);
    moq_event_t e;
    size_t seen = 0;
    size_t fill_events = 0;
    while (moq_session_poll_events(f->s, &e, 1)) {
        const moq_resolved_token_t *t = NULL;
        size_t n = 0;
#define TOKENS(kind_, field_) case kind_: t = e.u.field_.tokens; n = e.u.field_.token_count; break
        switch (e.kind) {
        case MOQ_EVENT_FILL_REQUEST:
            CHECK(op == UPDATE_FILL);
            fill_events++;
            moq_event_cleanup(&e);
            continue;
        TOKENS(MOQ_EVENT_SUBSCRIBE_REQUEST, subscribe_request);
        TOKENS(MOQ_EVENT_PUBLISH_REQUEST, publish_request);
        TOKENS(MOQ_EVENT_NAMESPACE_PUBLISHED, namespace_published);
        TOKENS(MOQ_EVENT_SUBSCRIBE_UPDATED, subscribe_updated);
        TOKENS(MOQ_EVENT_FETCH_REQUEST, fetch_request);
        TOKENS(MOQ_EVENT_TRACK_STATUS_REQUEST, track_status_request);
        TOKENS(MOQ_EVENT_NS_SUB_REQUEST, ns_sub_request);
        TOKENS(MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST, subscribe_tracks_request);
        TOKENS(MOQ_EVENT_PUBLISH_UPDATED, publish_updated);
        default: CHECK(false); break;
        }
#undef TOKENS
        static const moq_event_kind_t kinds[] = {
            MOQ_EVENT_SUBSCRIBE_REQUEST, MOQ_EVENT_PUBLISH_REQUEST,
            MOQ_EVENT_NAMESPACE_PUBLISHED, MOQ_EVENT_SUBSCRIBE_UPDATED,
            MOQ_EVENT_FETCH_REQUEST, MOQ_EVENT_TRACK_STATUS_REQUEST,
            MOQ_EVENT_NS_SUB_REQUEST, MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST,
            MOQ_EVENT_PUBLISH_UPDATED, MOQ_EVENT_SUBSCRIBE_REQUEST,
            MOQ_EVENT_SUBSCRIBE_UPDATED};
        CHECK(e.kind == kinds[op]);
        if (f->c->profile->version == MOQ_VERSION_DRAFT_22 &&
            (op == SUBSCRIBE || op == SUBSCRIBE_FILL)) {
            const moq_subscription_admission_options_t *o = e.u.subscribe_request.options;
            CHECK(o != NULL);
            if (o) {
                CHECK(o->location.kind == MOQ_LOCATION_FILTER_OBJECT_END);
                CHECK(o->location.end_group == 1 && o->location.end_object == 1);
                CHECK(o->object_delivery_timeout_ms == UINT64_MAX);
                CHECK(o->subgroup_delivery_timeout_ms == 0);
                CHECK(o->rendezvous_timeout_ms == UINT64_MAX);
                CHECK(((o->flags & MOQ_SUBSCRIPTION_ADMISSION_FILL) != 0) ==
                      (op == SUBSCRIBE_FILL));
            }
        }
        if (op == UPDATE_FILL) CHECK(e.u.subscribe_updated.fill_requested);
        CHECK(n == count && t != NULL);
        if (n == count && t) {
            for (size_t i = 0; i < count; i++) {
                CHECK(t[i].token_type == want[i].token_type);
                CHECK(t[i].token_value.len == want[i].token_value.len);
                if (t[i].token_value.len == want[i].token_value.len &&
                    want[i].token_value.len) {
                    CHECK(t[i].token_value.data != NULL);
                    if (t[i].token_value.data)
                        CHECK(memcmp(t[i].token_value.data, want[i].token_value.data,
                                     want[i].token_value.len) == 0);
                }
            }
        }
        if (extra >= 0) {
            switch (op) {
            case SUBSCRIBE:
                CHECK(e.u.subscribe_request.subscriber_priority == (extra ? 42 : 128));
                break;
            case PUBLISH:
                CHECK(e.u.publish_request.forward == (extra == 0));
                CHECK(!e.u.publish_request.has_largest);
                break;
            case FETCH:
                CHECK(e.u.fetch_request.subscriber_priority == (extra ? 42 : 128));
                break;
            case UPDATE:
                CHECK(e.u.subscribe_updated.has_subscriber_priority == (extra != 0));
                if (extra) CHECK(e.u.subscribe_updated.subscriber_priority == 42);
                CHECK(!e.u.subscribe_updated.has_forward);
                CHECK(!e.u.subscribe_updated.has_delivery_timeout);
                CHECK(!e.u.subscribe_updated.has_filter);
                break;
            case NAMESPACE: CHECK(extra == 0); break;
            default: CHECK(false); break;
            }
        }
        seen++;
        moq_event_cleanup(&e);
    }
    CHECK(seen == 1);
    CHECK(fill_events == (op == UPDATE_FILL ? 1u : 0u));
    CHECK(f->s->peer_token_cache.used_bytes == 0);
    CHECK(f->c->peer_token_cache.used_bytes == 0);
    CHECK(moq_session_state(f->c) == MOQ_SESS_ESTABLISHED);
    CHECK(moq_session_state(f->s) == MOQ_SESS_ESTABLISHED);
}

static void peer_token(fixture_t *f, enum operation op, const moq_auth_token_t *want)
{
    peer_tokens(f, op, want, 1, -1);
}

static void width_row(moq_version_t version, enum operation op, uint64_t type)
{
    fixture_t f;
    open_pair(&f, version, op);
    moq_auth_token_t token = {type, {value, sizeof(value)}};
    uint8_t *state = malloc(f.c->profile->state_size);
    REQUIRE(state != NULL);
    memcpy(state, f.c->profile_state, f.c->profile->state_size);
    uint64_t ref = f.c->next_stream_ref;
    int before = failures;
    moq_result_t rc = issue(&f, op, &token);
    bool allowed = version != MOQ_VERSION_DRAFT_16 || type <= MOQ_QUIC_VARINT_MAX;
    if (version == MOQ_VERSION_DRAFT_16 && op == TRACKS_SUBSCRIBE) allowed = false;
    CHECK(rc == (allowed ? MOQ_OK : MOQ_ERR_INVAL));
    if (allowed && rc == MOQ_OK) peer_token(&f, op, &token);
    if (!allowed) {
        CHECK(f.c->action_head == f.c->action_tail && f.c->send_len == 0);
        CHECK(f.c->next_stream_ref == ref);
        CHECK(memcmp(state, f.c->profile_state, f.c->profile->state_size) == 0);
    }
    free(state);
    moq_simpair_destroy(f.pair);
    printf("%s: width %u %s %llu\n", failures == before ? "PASS" : "ROW_FAIL",
           (unsigned)version, names[op], (unsigned long long)type);
}

enum capacity_case { EMPTY, OCCUPIED, TOO_LARGE, SCRATCH, ALLOC_FAIL,
                     EMPTY_TOO_LARGE, EMPTY_SCRATCH };
static const char *const cases[] = {"empty", "occupied", "too_large", "scratch", "alloc_fail",
                                  "empty_too_large", "empty_scratch"};

/* Fill tables are lazy, unlike the outbound encoding arena. Materialize the
 * D22 table through a real refused request before measuring admission: pool
 * backing storage may persist after refusal, but no child or request may.
 * This also keeps the later allocation fault specific to the encode probe. */
static void prepare_fill_storage(fixture_t *f, enum operation op)
{
    if (op != SUBSCRIBE_FILL && op != UPDATE_FILL) return;
    REQUIRE(moq_session_supports_fill(f->c));
    REQUIRE(f->c->fills.capacity == 0 && f->c->fill_storage == NULL);
    REQUIRE(f->c->fill_side == NULL);
    size_t cap = f->c->send_cap;
    size_t actions = f->c->action_tail, events = f->c->event_tail;
    uint64_t ref = f->c->next_stream_ref;
    uint8_t *state = malloc(f->c->profile->state_size);
    REQUIRE(state != NULL);
    memcpy(state, f->c->profile_state, f->c->profile->state_size);
    moq_auth_token_t token = {7, {value, sizeof(value)}};
    f->c->send_cap = 0;
    REQUIRE(issue(f, op, &token) == MOQ_ERR_BUFFER);
    f->c->send_cap = cap;
    REQUIRE(f->c->fills.capacity == f->c->fill_cap);
    REQUIRE(f->c->fills.entries == f->c->fill_storage && f->c->fill_storage != NULL);
    REQUIRE(f->c->fill_side != NULL && f->c->fills.live == 0);
    for (size_t i = 0; i < f->c->fills.capacity; i++) {
        REQUIRE(!f->c->fill_storage[i].active && f->c->fill_storage[i].generation == 0);
        REQUIRE(f->c->fill_side[i].handle._opaque == 0);
    }
    REQUIRE(f->c->action_tail == actions && f->c->event_tail == events);
    REQUIRE(f->c->send_len == 0 && f->c->next_stream_ref == ref);
    REQUIRE(memcmp(state, f->c->profile_state, f->c->profile->state_size) == 0);
    free(state);
}

static void capacity_row(moq_version_t version, enum operation op, enum capacity_case mode)
{
    fixture_t f;
    open_pair(&f, version, op);
    int before = failures;
    prepare_fill_storage(&f, op);
    /* Keep the allocation intact; only constrain the usable send arena. */
    size_t cap = f.c->send_cap, scratch = f.c->output_scratch_cap;
    REQUIRE(f.c->send_buf != NULL && cap >= 256);
    f.c->send_cap = 256;
    uint8_t bytes[300];
    memset(bytes, 0xa7, sizeof(bytes));
    bool too_large = mode == TOO_LARGE || mode == EMPTY_TOO_LARGE;
    bool no_scratch = mode == SCRATCH || mode == EMPTY_SCRATCH;
    moq_auth_token_t token = {7, {bytes, too_large ? sizeof(bytes) : 80}};
    bool queued = mode != EMPTY && mode != EMPTY_TOO_LARGE && mode != EMPTY_SCRATCH;
    uint8_t filler[255];
    memset(filler, 0x5a, sizeof(filler));
    if (queued) REQUIRE(queue_send_control(f.c, filler, sizeof(filler)) == MOQ_OK);
    if (no_scratch) f.c->output_scratch_cap = 1;
    size_t tail = f.c->action_tail, len = f.c->send_len;
    size_t events = f.c->event_tail, fills = f.c->fills.live;
    size_t history_refs = 0;
    for (size_t i = 0; i < f.c->th_cap; i++) history_refs += f.c->track_hist[i].refs;
    /* Snapshot only materialized, request-local fill state, including the
     * parallel session metadata. Legacy/no-fill requests keep a NULL table
     * even though the session records a nonzero configured fill limit. */
    moq_fill_table_t fill_table = f.c->fills;
    moq_fill_side_t *fill_side = f.c->fill_side;
    size_t fill_bytes = fill_table.capacity * sizeof(moq_fill_entry_t);
    size_t side_bytes = fill_table.capacity * sizeof(moq_fill_side_t);
    moq_fill_entry_t *fill_before = NULL;
    moq_fill_side_t *side_before = NULL;
    if (fill_table.capacity) {
        REQUIRE(fill_table.entries == f.c->fill_storage && fill_table.entries != NULL);
        REQUIRE(fill_side != NULL);
        fill_before = malloc(fill_bytes);
        side_before = malloc(side_bytes);
        REQUIRE(fill_before != NULL && side_before != NULL);
        memcpy(fill_before, fill_table.entries, fill_bytes);
        memcpy(side_before, fill_side, side_bytes);
    } else {
        REQUIRE(fill_table.entries == NULL && f.c->fill_storage == NULL);
        REQUIRE(fill_side == NULL && fills == 0);
    }
    uint64_t ref = f.c->next_stream_ref;
    uint8_t *state = malloc(f.c->profile->state_size);
    REQUIRE(state != NULL);
    memcpy(state, f.c->profile_state, f.c->profile->state_size);
    f.original = f.c->alloc;
    f.probe_size = 256;
    f.fail_probe = mode == ALLOC_FAIL;
    f.c->alloc = (moq_alloc_t){&f, probe_alloc, probe_realloc, probe_free};
    moq_result_t rc = issue(&f, op, &token);
    moq_result_t expected = mode == EMPTY ? MOQ_OK :
        mode == ALLOC_FAIL ? MOQ_ERR_NOMEM :
        (too_large || no_scratch) ? MOQ_ERR_BUFFER : MOQ_ERR_WOULD_BLOCK;
    CHECK(rc == expected);
    if (rc == MOQ_OK) {
        CHECK(f.c->action_tail - f.c->action_head == 1);
        CHECK(f.c->send_len > len);
        CHECK(f.c->fills.live == fills + (op >= SUBSCRIBE_FILL ? 1u : 0u));
        CHECK(f.c->event_tail - events == (op >= SUBSCRIBE_FILL ? 1u : 0u));
    }
    if (rc < 0) {
        CHECK(f.c->event_tail == events && f.c->fills.live == fills);
        CHECK(f.c->fills.capacity == fill_table.capacity);
        CHECK(f.c->fills.entries == fill_table.entries &&
              f.c->fill_storage == fill_table.entries && f.c->fill_side == fill_side);
        if (fill_table.capacity) {
            REQUIRE(f.c->fill_storage != NULL && f.c->fill_side != NULL);
            CHECK(memcmp(fill_before, f.c->fill_storage, fill_bytes) == 0);
            CHECK(memcmp(side_before, f.c->fill_side, side_bytes) == 0);
        }
        size_t refs = 0;
        for (size_t i = 0; i < f.c->th_cap; i++) refs += f.c->track_hist[i].refs;
        CHECK(refs == history_refs);
        CHECK(f.balance == 0);
        CHECK(f.c->action_tail == tail && f.c->send_len == len);
        CHECK(f.c->next_stream_ref == ref);
        CHECK(memcmp(state, f.c->profile_state, f.c->profile->state_size) == 0);
    }
    if (queued) {
        CHECK(f.c->action_tail == tail && f.c->send_len == len);
        CHECK(memcmp(f.c->send_buf, filler, sizeof(filler)) == 0);
        CHECK(f.c->next_stream_ref == ref);
        CHECK(memcmp(state, f.c->profile_state, f.c->profile->state_size) == 0);
        CHECK(f.c->output_scratch_len == 0);
        /* Discard only the fixture's filler, never feed it to the peer. */
        moq_action_t a;
        REQUIRE(moq_session_poll_actions(f.c, &a, 1) == 1);
        REQUIRE(a.kind == MOQ_ACTION_SEND_CONTROL);
        CHECK(a.u.send_control.len == sizeof(filler));
        CHECK(memcmp(a.u.send_control.data, filler, sizeof(filler)) == 0);
        moq_action_cleanup(&a);
        REQUIRE(moq_session_poll_actions(f.c, &a, 1) == 0);
        if (mode == OCCUPIED || mode == ALLOC_FAIL) CHECK(f.probes == 1);
        if (mode != ALLOC_FAIL) CHECK(f.probes == 1 && f.wiped == 1);
    }
    if (!queued) CHECK(f.probes == 0 && f.wiped == 0);
    f.c->alloc = f.original;
    if (mode == EMPTY && rc == MOQ_OK) peer_token(&f, op, &token);
    if (queued) {
        moq_result_t retry = issue(&f, op, &token);
        if (too_large || no_scratch) CHECK(retry == MOQ_ERR_BUFFER);
        else {
            CHECK(retry == MOQ_OK);
            if (retry == MOQ_OK) peer_token(&f, op, &token);
        }
    }
    free(state);
    free(fill_before);
    free(side_before);
    f.c->send_cap = cap;
    f.c->output_scratch_cap = scratch;
    moq_simpair_destroy(f.pair);
    printf("%s: capacity %u %s %s\n", failures == before ? "PASS" : "ROW_FAIL",
           (unsigned)version, names[op], cases[mode]);
}

/* Only public request APIs select the token/option counts. The occupied arena
 * and rollback observations use the same private fixture machinery as above. */
static moq_result_t issue_count(fixture_t *f, enum operation op,
    const moq_auth_token_t *tokens, size_t count, bool extra)
{
    moq_namespace_t space = {ns, 1};
    moq_bytes_t track = MOQ_BYTES_LITERAL("track");
    switch (op) {
    case SUBSCRIBE: {
        moq_subscribe_cfg_t c;
        moq_subscribe_cfg_init_sized(&c, sizeof(c));
        c.track_namespace = space; c.track_name = track;
        c.auth_tokens = tokens; c.auth_token_count = count;
        c.has_subscriber_priority = extra; c.subscriber_priority = 42;
        moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
        moq_result_t rc = moq_session_subscribe(f->c, &c, 1000, &h);
        CHECK(rc == MOQ_OK || h._opaque == 0);
        return rc;
    }
    case PUBLISH: {
        moq_publish_cfg_t c;
        moq_publish_cfg_init_sized(&c, sizeof(c));
        c.track_namespace = space; c.track_name = track;
        c.auth_tokens = tokens; c.auth_token_count = count;
        c.has_forward = extra; c.forward = false;
        moq_publication_t h = MOQ_PUBLICATION_INVALID;
        moq_result_t rc = moq_session_publish(f->c, &c, 1000, &h);
        CHECK(rc == MOQ_OK || h._opaque == 0);
        return rc;
    }
    case FETCH: {
        moq_fetch_cfg_t c;
        moq_fetch_cfg_init_sized(&c, sizeof(c));
        c.track_namespace = space; c.track_name = track;
        c.end_group = 1; c.end_object = 1;
        c.auth_tokens = tokens; c.auth_token_count = count;
        c.has_subscriber_priority = extra; c.subscriber_priority = 42;
        moq_fetch_t h = MOQ_FETCH_INVALID;
        moq_result_t rc = moq_session_fetch(f->c, &c, 1000, &h);
        CHECK(rc == MOQ_OK || h._opaque == 0);
        return rc;
    }
    case UPDATE: {
        moq_subscription_update_cfg_t c;
        moq_subscription_update_cfg_init_sized(&c, sizeof(c));
        c.auth_tokens = tokens; c.auth_token_count = count;
        c.has_subscriber_priority = extra; c.subscriber_priority = 42;
        return moq_session_update_subscription(f->c, f->sub, &c, 1000);
    }
    case NAMESPACE: {
        REQUIRE(!extra);
        moq_publish_namespace_cfg_t c;
        moq_publish_namespace_cfg_init(&c);
        c.track_namespace = space;
        c.auth_tokens = tokens; c.auth_token_count = count;
        moq_announcement_t h = MOQ_ANNOUNCEMENT_INVALID;
        moq_result_t rc = moq_session_publish_namespace(f->c, &c, 1000, &h);
        CHECK(rc == MOQ_OK || h._opaque == 0);
        return rc;
    }
    default: REQUIRE(false); return MOQ_ERR_INVAL;
    }
}

static void count_row(enum operation op, size_t count, bool extra,
                      bool occupied, bool malformed)
{
    fixture_t f;
    open_pair(&f, MOQ_VERSION_DRAFT_16, op);
    int before = failures;
    moq_auth_token_t tokens[17];
    REQUIRE(count > 0 && count <= sizeof(tokens) / sizeof(tokens[0]));
    for (size_t i = 0; i < count; i++)
        tokens[i] = (moq_auth_token_t){i, {i % 2 ? value : NULL,
                                          i % 2 ? sizeof(value) : 0}};
    if (malformed) tokens[0].token_value = (moq_bytes_t){NULL, 1};
    moq_result_t final = malformed ? MOQ_ERR_INVAL :
        count + (extra ? 1u : 0u) > 16 ? MOQ_ERR_BUFFER : MOQ_OK;

    size_t cap = f.c->send_cap;
    REQUIRE(cap >= 256);
    f.c->send_cap = 256;
    uint8_t filler[255];
    memset(filler, 0x5a, sizeof(filler));
    if (occupied) REQUIRE(queue_send_control(f.c, filler, sizeof(filler)) == MOQ_OK);
    size_t head = f.c->action_head, tail = f.c->action_tail, len = f.c->send_len;
    size_t event_head = f.c->event_head, event_tail = f.c->event_tail;
    uint64_t ref = f.c->next_stream_ref;
    struct snapshot {
        const void *data;
        size_t size;
        uint8_t *bytes;
    } snapshots[] = {
        {f.c->profile_state, f.c->profile->state_size, NULL},
        {f.c->subs, f.c->sub_cap * sizeof(*f.c->subs), NULL},
        {f.c->publishes, f.c->pub_cap * sizeof(*f.c->publishes), NULL},
        {f.c->fetches, f.c->fetch_cap * sizeof(*f.c->fetches), NULL},
        {f.c->announcements, f.c->ann_cap * sizeof(*f.c->announcements), NULL},
        {f.c->track_hist, f.c->th_cap * sizeof(*f.c->track_hist), NULL},
    };
    for (size_t i = 0; i < sizeof(snapshots) / sizeof(snapshots[0]); i++) {
        REQUIRE(snapshots[i].data != NULL && snapshots[i].size > 0);
        snapshots[i].bytes = malloc(snapshots[i].size);
        REQUIRE(snapshots[i].bytes != NULL);
        memcpy(snapshots[i].bytes, snapshots[i].data, snapshots[i].size);
    }

    moq_result_t expected = occupied && final == MOQ_OK ? MOQ_ERR_WOULD_BLOCK : final;
    moq_result_t rc = issue_count(&f, op, tokens, count, extra);
    CHECK(rc == expected);
    if (expected != MOQ_OK) {
        CHECK(f.c->action_head == head && f.c->action_tail == tail);
        CHECK(f.c->send_len == len && f.c->next_stream_ref == ref);
        CHECK(f.c->event_head == event_head && f.c->event_tail == event_tail);
        CHECK(f.c->output_scratch_len == 0 && f.c->fills.live == 0);
        for (size_t i = 0; i < sizeof(snapshots) / sizeof(snapshots[0]); i++)
            CHECK(memcmp(snapshots[i].bytes, snapshots[i].data, snapshots[i].size) == 0);
    }
    if (occupied) {
        CHECK(memcmp(f.c->send_buf, filler, sizeof(filler)) == 0);
        moq_action_t a;
        REQUIRE(moq_session_poll_actions(f.c, &a, 1) == 1);
        REQUIRE(a.kind == MOQ_ACTION_SEND_CONTROL);
        CHECK(a.u.send_control.len == sizeof(filler));
        if (a.u.send_control.len == sizeof(filler))
            CHECK(memcmp(a.u.send_control.data, filler, sizeof(filler)) == 0);
        moq_action_cleanup(&a);
        REQUIRE(moq_session_poll_actions(f.c, &a, 1) == 0);
        rc = issue_count(&f, op, tokens, count, extra);
        CHECK(rc == final);
    }
    if (final == MOQ_OK && rc == MOQ_OK) {
        CHECK(f.c->action_tail - f.c->action_head == 1);
        peer_tokens(&f, op, tokens, count, extra ? 1 : 0);
    } else {
        CHECK(f.c->action_head == f.c->action_tail);
        /* Malformed FETCH/UPDATE spans are refused before advancing. Polling
         * consumed their earlier action but does not itself reclaim scratch. */
        size_t remaining = occupied && malformed && (op == FETCH || op == UPDATE) ? len : 0;
        CHECK(f.c->send_len == remaining);
        CHECK(f.c->next_stream_ref == ref);
        CHECK(f.c->event_head == event_head && f.c->event_tail == event_tail);
        for (size_t i = 0; i < sizeof(snapshots) / sizeof(snapshots[0]); i++)
            CHECK(memcmp(snapshots[i].bytes, snapshots[i].data, snapshots[i].size) == 0);
        pump(&f);
        moq_event_t e;
        while (moq_session_poll_events(f.s, &e, 1)) {
            CHECK(false);
            moq_event_cleanup(&e);
        }
        CHECK(moq_session_state(f.c) == MOQ_SESS_ESTABLISHED);
        CHECK(moq_session_state(f.s) == MOQ_SESS_ESTABLISHED);
    }
    for (size_t i = 0; i < sizeof(snapshots) / sizeof(snapshots[0]); i++)
        free(snapshots[i].bytes);
    f.c->send_cap = cap;
    moq_simpair_destroy(f.pair);
    printf("%s: count 16 %s %zu+%u %s%s\n",
           failures == before ? "PASS" : "ROW_FAIL", names[op], count,
           extra ? 1u : 0u, occupied ? "occupied" : "empty",
           malformed ? " malformed" : "");
}

static void count_rows(void)
{
    for (enum operation op = SUBSCRIBE; op <= FETCH; op++) {
        for (unsigned occupied = 0; occupied < 2; occupied++) {
            if (op != NAMESPACE) {
                count_row(op, 15, true, occupied != 0, false);
                count_row(op, 16, true, occupied != 0, false);
            }
            count_row(op, 16, false, occupied != 0, false);
            count_row(op, 17, false, occupied != 0, false);
            count_row(op, 1, op != NAMESPACE, occupied != 0, true);
            if (op == UPDATE) count_row(op, 12, true, occupied != 0, false);
        }
    }
}

int main(int argc, char **argv)
{
#ifdef MOQ_REQUEST_AUTH_WIDTH_ONLY
    bool widths = true;
    bool capacity = false;
    bool counts = false;
    (void)argc; (void)argv;
#else
    bool widths = argc == 1 || (argc == 2 && strcmp(argv[1], "width") == 0);
    bool capacity = argc == 1 || (argc == 2 && strcmp(argv[1], "capacity") == 0);
    bool counts = capacity || (argc == 2 && strcmp(argv[1], "count") == 0);
#endif
    REQUIRE(widths || capacity || counts);
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                    MOQ_VERSION_DRAFT_22};
    const uint64_t types[] = {MOQ_QUIC_VARINT_MAX, UINT64_C(1) << 62, UINT64_MAX};
    for (size_t v = 0; v < 3; v++) {
        for (enum operation op = SUBSCRIBE; op < OP_COUNT; op++) {
            if (op >= SUBSCRIBE_FILL && versions[v] != MOQ_VERSION_DRAFT_22) continue;
            if (widths) for (size_t t = 0; t < 3; t++) width_row(versions[v], op, types[t]);
            if (capacity && (op <= FETCH || op >= SUBSCRIBE_FILL)) {
                for (enum capacity_case c = EMPTY; c <= EMPTY_SCRATCH; c++) {
                    if ((c != SCRATCH && c != EMPTY_SCRATCH) ||
                        versions[v] == MOQ_VERSION_DRAFT_16)
                        capacity_row(versions[v], op, c);
                }
            }
        }
    }
    if (counts) count_rows();
    printf("request_auth_capacity: %d failure(s)\n", failures);
    return failures ? 1 : 0;
}
