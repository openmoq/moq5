/* Admission pressure cancels one receive subgroup, not its owner/siblings.
 * Sessions, bindings, objects, updates and pressure are established through
 * real session inputs. Private state is observed, never fabricated. The
 * retirement call represents the bridge's separately proven positive local
 * transport retirement; provider acceptance/retry gates belong to bridge tests.
 * Link against moq-core-test-internals; no simulator/provider is required. */
#include <moq/session.h>
#include "test_binding_support.h"
#include "../../core/src/session/session_internal.h"
#include "../../core/src/bridge/transport_bridge_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static unsigned failures, rows, checks;
static const char *label;
#define CHECK(x) do { ++checks; if (!(x)) { ++failures; \
    fprintf(stderr, "FAIL[%s] %s:%d: %s\n", label, __FILE__, __LINE__, #x); \
} } while (0)
#define REQUIRE(x) do { if (!(x)) { \
    fprintf(stderr, "SETUP[%s] %s:%d: %s\n", label, __FILE__, __LINE__, #x); \
    exit(2); \
} } while (0)

typedef struct balance { size_t blocks, bytes; } balance_t;
static void *count_alloc(size_t n, void *ctx)
{
    balance_t *b = ctx;
    void *p = malloc(n ? n : 1);
    if (p) { ++b->blocks; b->bytes += n; }
    return p;
}
static void count_free(void *p, size_t n, void *ctx)
{
    balance_t *b = ctx;
    if (!p) return;
    REQUIRE(b->blocks && b->bytes >= n);
    --b->blocks; b->bytes -= n; free(p);
}
static void *count_realloc(void *p, size_t old, size_t n, void *ctx)
{
    balance_t *b = ctx;
    if (!p) return count_alloc(n, ctx);
    if (!n) { count_free(p, old, ctx); return NULL; }
    REQUIRE(b->blocks && b->bytes >= old);
    void *q = realloc(p, n);
    if (q) b->bytes = b->bytes - old + n;
    return q;
}

typedef struct rig {
    moq_session_t *s;
    balance_t balance;
    const void *owner;
    moq_version_t version;
    uint64_t now;
} rig_t;
typedef struct owner {
    moq_subscription_t sub;
    moq_publication_t pub;
    moq_stream_ref_t request;
    uint64_t request_id;
    uint8_t alias;
} owner_t;

static const moq_sub_entry_t *sub_entry(const rig_t *r, moq_subscription_t sub)
{
    for (size_t i = 0; i < r->s->sub_cap; ++i)
        if (r->s->subs[i].state != MOQ_SUB_FREE &&
            moq_subscription_eq(r->s->subs[i].handle, sub)) return &r->s->subs[i];
    return NULL;
}
static const moq_pub_entry_t *pub_entry(const rig_t *r, moq_publication_t pub)
{
    for (size_t i = 0; i < r->s->pub_cap; ++i)
        if (r->s->publishes[i].state != MOQ_PUB_FREE &&
            moq_publication_eq(r->s->publishes[i].handle, pub)) return &r->s->publishes[i];
    return NULL;
}
static const moq_rx_stream_t *rx_entry(const rig_t *r, uint64_t ref)
{
    for (size_t i = 0; i < r->s->rx_cap; ++i)
        if (r->s->rx_streams[i].active && r->s->rx_streams[i].stream_ref._v == ref)
            return &r->s->rx_streams[i];
    return NULL;
}
static uint64_t pressure_code(const rig_t *r)
{
    return r->version == MOQ_VERSION_DRAFT_16 ? 1 : 9;
}
static void drain_actions(rig_t *r)
{
    moq_action_t a;
    while (moq_session_poll_actions(r->s, &a, 1)) {
        CHECK(a.kind != MOQ_ACTION_STOP_DATA && a.kind != MOQ_ACTION_CLOSE_SESSION);
        moq_action_cleanup(&a);
    }
}
static void no_events(rig_t *r)
{
    moq_event_t e;
    while (moq_session_poll_events(r->s, &e, 1)) {
        CHECK(false); moq_event_cleanup(&e);
    }
}
static void open_rig(rig_t *r, moq_version_t version, bool streaming,
                     uint32_t events, uint32_t rx_count)
{
    memset(r, 0, sizeof(*r));
    r->version = version; r->now = 1; r->owner = r;
    moq_alloc_t alloc = {.ctx = &r->balance, .alloc = count_alloc,
                        .realloc = count_realloc, .free = count_free};
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), &alloc, MOQ_PERSPECTIVE_CLIENT);
    cfg.version = version; cfg.streaming_objects = streaming;
    cfg.max_events = events; cfg.max_actions = 2;
    cfg.max_subscriptions = 8; cfg.max_publishes = 2;
    cfg.max_data_streams = rx_count;
    cfg.send_request_capacity = true; cfg.initial_request_capacity = 32;
    REQUIRE(moq_session_create(&cfg, 0, &r->s) == MOQ_OK);
    REQUIRE(test_bind_if_required(r->s) == MOQ_OK);
    REQUIRE(moq_session_register_transport_owner(r->s, r->owner) == MOQ_OK);
    REQUIRE(moq_session_start(r->s, r->now) == MOQ_OK);
    drain_actions(r);
    static const uint8_t modern[] = {0xaf, 0, 0, 0};
    static const uint8_t legacy[] = {0x21, 0, 1, 0, 0x15, 0, 1, 32};
    const uint8_t *wire = version == MOQ_VERSION_DRAFT_16 ? legacy : modern;
    size_t n = version == MOQ_VERSION_DRAFT_16 ? sizeof(legacy) : sizeof(modern);
    REQUIRE(moq_session_on_control_bytes(r->s, wire, n, ++r->now) == MOQ_OK);
    REQUIRE(moq_session_state(r->s) == MOQ_SESS_ESTABLISHED);
    moq_event_t e;
    REQUIRE(moq_session_poll_events(r->s, &e, 1) == 1);
    REQUIRE(e.kind == MOQ_EVENT_SETUP_COMPLETE);
    moq_event_cleanup(&e); no_events(r); drain_actions(r);
}
static void close_rig(rig_t *r)
{
    moq_session_unregister_transport_owner(r->s, r->owner);
    moq_session_destroy(r->s);
    CHECK(r->balance.blocks == 0 && r->balance.bytes == 0);
    ++rows;
}

static owner_t request_sub(rig_t *r, const char *name, uint8_t priority, bool forward)
{
    owner_t o = {0};
    moq_bytes_t part = MOQ_BYTES_LITERAL("pressure");
    moq_subscribe_cfg_t cfg; moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    cfg.track_name = (moq_bytes_t){(const uint8_t *)name, strlen(name)};
    cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    cfg.has_subscriber_priority = true; cfg.subscriber_priority = priority;
    cfg.has_forward = true; cfg.forward = forward;
    REQUIRE(moq_session_subscribe(r->s, &cfg, ++r->now, &o.sub) == MOQ_OK);
    const moq_sub_entry_t *e = sub_entry(r, o.sub);
    REQUIRE(e != NULL);
    o.request = e->rs.ref; o.request_id = e->request_id;
    return o;
}
static owner_t subscribe(rig_t *r, const char *name, uint8_t alias,
                         uint8_t priority, int default_priority, bool forward)
{
    owner_t o = request_sub(r, name, priority, forward); o.alias = alias;
    drain_actions(r);
    /* SUBSCRIBE_OK, no parameters; optional response-local default property. */
    uint8_t wire[16] = {4, 0, 0}; size_t n = 3;
    if (r->version == MOQ_VERSION_DRAFT_16) {
        REQUIRE(o.request_id < 64); wire[n++] = (uint8_t)o.request_id;
    }
    wire[n++] = alias; wire[n++] = 0;
    if (default_priority >= 0) {
        REQUIRE(r->version != MOQ_VERSION_DRAFT_16);
        wire[n++] = 0x0e;
        if (default_priority >= 128) wire[n++] = 0x80;
        wire[n++] = (uint8_t)default_priority;
    }
    wire[2] = (uint8_t)(n - 3);
    moq_result_t rc = r->version == MOQ_VERSION_DRAFT_16
        ? moq_session_on_control_bytes(r->s, wire, n, ++r->now)
        : moq_session_on_bidi_stream_bytes(r->s, o.request, wire, n, false, ++r->now);
    REQUIRE(rc == MOQ_OK);
    moq_event_t e;
    REQUIRE(moq_session_poll_events(r->s, &e, 1) == 1);
    REQUIRE(e.kind == MOQ_EVENT_SUBSCRIBE_OK && moq_subscription_eq(e.u.subscribe_ok.sub, o.sub));
    REQUIRE(e.u.subscribe_ok.track_alias == alias);
    moq_event_cleanup(&e); no_events(r); drain_actions(r);
    return o;
}
static owner_t publication(rig_t *r)
{
    /* D22 PUBLISH, id 1, namespace n/name p, alias 8: subscriber priority
     * 199 is uint8 (parameter 0x20); default priority 240 is vi64 (property 0x0e). */
    static const uint8_t wire[] = {0x1d, 0, 13, 1, 1, 1, 'n', 1, 'p', 8,
                                   1, 0x20, 199, 0x0e, 0x80, 240};
    owner_t o = {0}; o.alias = 8; o.request = moq_stream_ref_from_u64(0x4000);
    REQUIRE(moq_session_on_bidi_stream_bytes(r->s, o.request, wire, sizeof(wire),
                                             false, ++r->now) == MOQ_OK);
    moq_event_t e;
    REQUIRE(moq_session_poll_events(r->s, &e, 1) == 1);
    REQUIRE(e.kind == MOQ_EVENT_PUBLISH_REQUEST);
    o.pub = e.u.publish_request.pub; moq_event_cleanup(&e);
    /* D22 PUBLISH_OK permits only EXPIRES; keep the PUBLISH's priority. */
    moq_accept_publish_cfg_t cfg; moq_accept_publish_cfg_init(&cfg);
    REQUIRE(moq_session_accept_publish(r->s, o.pub, &cfg, ++r->now) == MOQ_OK);
    drain_actions(r); no_events(r);
    return o;
}

/* Explicit priority subgroup header, or omitted-priority type 0x30. The
 * small locations/lengths used here have identical encodings in all drafts. */
static void start_stream(rig_t *r, owner_t o, uint64_t ref, uint8_t group,
                          int priority, bool partial)
{
    uint8_t wire[12] = {0x30, o.alias, group}; size_t n = 3;
    if (priority >= 0) { wire[0] = 0x10; wire[n++] = (uint8_t)priority; }
    if (partial) { wire[n++] = 0; wire[n++] = 4; wire[n++] = 'A'; }
    REQUIRE(moq_session_on_data_bytes(r->s, moq_stream_ref_from_u64(ref),
                                      wire, n, false, ++r->now) == MOQ_OK);
    REQUIRE(rx_entry(r, ref) != NULL);
}
static void filler_event(rig_t *r, owner_t o)
{
    const uint8_t wire[] = {0, o.alias, 30, 0, 100, 'F'};
    REQUIRE(moq_session_on_datagram(r->s, wire, sizeof(wire), ++r->now) == MOQ_OK);
    REQUIRE(r->s->event_tail - r->s->event_head == r->s->event_cap);
}
static void expect_filler(rig_t *r, owner_t o)
{
    moq_event_t e = {0}; size_t n = moq_session_poll_events(r->s, &e, 1);
    CHECK(n == 1 && e.kind == MOQ_EVENT_OBJECT_RECEIVED);
    if (n && e.kind == MOQ_EVENT_OBJECT_RECEIVED) {
        CHECK(moq_subscription_eq(e.u.object_received.sub, o.sub));
        CHECK(e.u.object_received.datagram && e.u.object_received.group_id == 30);
        CHECK(e.u.object_received.payload && moq_rcbuf_len(e.u.object_received.payload) == 1);
        if (e.u.object_received.payload && moq_rcbuf_len(e.u.object_received.payload) == 1)
            CHECK(moq_rcbuf_data(e.u.object_received.payload)[0] == 'F');
    }
    moq_event_cleanup(&e);
}
static void expect_chunk(rig_t *r, owner_t o, uint64_t group, bool begin,
                         bool end, moq_object_terminal_t terminal, uint64_t code,
                         int payload)
{
    moq_event_t e = {0}; size_t n = moq_session_poll_events(r->s, &e, 1);
    CHECK(n == 1 && e.kind == MOQ_EVENT_OBJECT_CHUNK);
    if (n && e.kind == MOQ_EVENT_OBJECT_CHUNK) {
        const moq_object_chunk_event_t *c = &e.u.object_chunk;
        CHECK(moq_subscription_eq(c->sub, o.sub) && moq_publication_eq(c->pub, o.pub));
        CHECK(c->group_id == group && c->subgroup_id == 0 && c->object_id == 0);
        CHECK(c->begin == begin && c->end == end);
        CHECK(c->terminal == terminal && c->error_code == code);
        if (payload < 0) CHECK(c->chunk == NULL && c->properties == NULL);
        else {
            CHECK(c->chunk && moq_rcbuf_len(c->chunk) == 1);
            if (c->chunk && moq_rcbuf_len(c->chunk) == 1)
                CHECK(moq_rcbuf_data(c->chunk)[0] == (uint8_t)payload);
        }
        if (begin) CHECK(c->payload_length == (end ? 1u : 4u));
    }
    moq_event_cleanup(&e);
}
static void expect_stop(rig_t *r, uint64_t ref)
{
    moq_action_t a = {0}; size_t n = moq_session_poll_actions(r->s, &a, 1);
    CHECK(n == 1 && a.kind == MOQ_ACTION_STOP_DATA);
    if (n && a.kind == MOQ_ACTION_STOP_DATA) {
        CHECK(a.u.stop_data.stream_ref._v == ref);
        CHECK(a.u.stop_data.error_code == pressure_code(r));
    }
    moq_action_cleanup(&a);
    CHECK(moq_session_poll_actions(r->s, &a, 1) == 0); moq_action_cleanup(&a);
}
static void expect_candidate(rig_t *r, uint64_t ref, uint8_t sub, uint8_t pub)
{
    moq_admitted_data_candidate_t c;
    CHECK(moq_session_worst_admitted_data(r->s, r->owner, &c) == MOQ_OK);
    CHECK(c.stream_ref._v == ref && c.subscriber_priority == sub && c.publisher_priority == pub);
}

/* Read-only refusal snapshots include queue contents, ownership/accounting,
 * borrows/time, bindings and every RX entry; actual input/output assertions
 * below additionally prove that the same object can finish on retry. */
typedef struct snapshot {
    moq_session_t session;
    moq_rx_stream_t rx[4];
    moq_sub_entry_t subs[8];
    moq_pub_entry_t pubs[2];
    moq_event_t events[4];
    moq_action_t actions[2];
    balance_t balance;
} snapshot_t;
static void snapshot(rig_t *r, snapshot_t *out)
{
    REQUIRE(r->s->rx_cap <= 4 && r->s->sub_cap <= 8 && r->s->pub_cap <= 2 &&
            r->s->event_cap <= 4 && r->s->action_cap <= 2);
    memset(out, 0, sizeof(*out));
    memcpy(&out->session, r->s, sizeof(*r->s));
    memcpy(out->rx, r->s->rx_streams, r->s->rx_cap * sizeof(out->rx[0]));
    memcpy(out->subs, r->s->subs, r->s->sub_cap * sizeof(out->subs[0]));
    memcpy(out->pubs, r->s->publishes, r->s->pub_cap * sizeof(out->pubs[0]));
    memcpy(out->events, r->s->events, r->s->event_cap * sizeof(out->events[0]));
    memcpy(out->actions, r->s->actions, r->s->action_cap * sizeof(out->actions[0]));
    out->balance = r->balance;
}
static void refusal(rig_t *r, const void *owner, uint64_t ref, uint64_t code,
                     moq_result_t expected)
{
    snapshot_t before, after; snapshot(r, &before);
    CHECK(moq_session_cancel_admitted_data(r->s, owner, moq_stream_ref_from_u64(ref), code) == expected);
    snapshot(r, &after); CHECK(memcmp(&before, &after, sizeof(before)) == 0);
}
static void finish_sibling(rig_t *r, owner_t o, uint64_t ref, uint64_t group)
{
    static const uint8_t object[] = {0, 1, 'B'};
    REQUIRE(moq_session_on_data_bytes(r->s, moq_stream_ref_from_u64(ref),
                                      object, sizeof(object), true, ++r->now) == MOQ_OK);
    expect_chunk(r, o, group, true, true, MOQ_OBJECT_TERMINAL_NORMAL, 0, 'B');
    moq_event_t e = {0}; size_t n = moq_session_poll_events(r->s, &e, 1);
    CHECK(n == 1 && e.kind == MOQ_EVENT_SUBGROUP_FINISHED);
    if (n && e.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
        CHECK(moq_subscription_eq(e.u.subgroup_finished.sub, o.sub));
        CHECK(moq_publication_eq(e.u.subgroup_finished.pub, o.pub));
        CHECK(e.u.subgroup_finished.group_id == group);
    }
    moq_event_cleanup(&e); no_events(r); drain_actions(r);
}

static void streaming_atomic(moq_version_t version)
{
    char name[64]; (void)snprintf(name, sizeof(name), "d%u.action-pressure-retirement", (unsigned)version);
    label = name;
    rig_t r; open_rig(&r, version, true, 2, 2);
    owner_t o = subscribe(&r, "a", 7, 128, -1, true);
    start_stream(&r, o, 0x5002, 10, 100, true);
    start_stream(&r, o, 0x5003, 11, 100, false);
    CHECK(!moq_session_can_admit_data_stream(r.s));
    CHECK(bridge_session_on_data_retired(r.s, moq_stream_ref_from_u64(0x5002), ++r.now) == MOQ_ERR_WRONG_STATE);
    /* BEGIN remains queued, not yet polled. Fill action capacity through two
     * real requests for other tracks, then verify a completely inert refusal. */
    (void)request_sub(&r, "pending1", 128, true);
    (void)request_sub(&r, "pending2", 128, true);
    REQUIRE(r.s->action_tail - r.s->action_head == r.s->action_cap);
    expect_candidate(&r, 0x5002, 128, 100);
    refusal(&r, r.owner, 0x5002, pressure_code(&r), MOQ_ERR_WOULD_BLOCK);
    drain_actions(&r);
    CHECK(moq_session_cancel_admitted_data(r.s, r.owner,
          moq_stream_ref_from_u64(0x5002), pressure_code(&r)) == MOQ_OK);
    expect_chunk(&r, o, 10, true, false, MOQ_OBJECT_TERMINAL_NORMAL, 0, 'A');
    expect_chunk(&r, o, 10, false, true, MOQ_OBJECT_TERMINAL_STOP, 0, -1);
    no_events(&r); expect_stop(&r, 0x5002);
    CHECK(moq_session_rx_entry_active(r.s, r.owner, moq_stream_ref_from_u64(0x5002)));
    CHECK(!moq_session_can_admit_data_stream(r.s));
    CHECK(sub_entry(&r, o.sub) && sub_entry(&r, o.sub)->processed_stream_count == 0);
    refusal(&r, r.owner, 0x5002, pressure_code(&r), MOQ_ERR_INVAL);
    /* Positive retirement releases one slot; it claims no FIN or peer reset. */
    size_t fin_count = r.s->rx_fin_count;
    CHECK(bridge_session_on_data_retired(r.s, moq_stream_ref_from_u64(0x5002), ++r.now) == MOQ_OK);
    CHECK(!moq_session_rx_entry_active(r.s, r.owner, moq_stream_ref_from_u64(0x5002)));
    CHECK(moq_session_can_admit_data_stream(r.s) && r.s->rx_fin_count == fin_count);
    CHECK(sub_entry(&r, o.sub) && sub_entry(&r, o.sub)->processed_stream_count == 1);
    CHECK(bridge_session_on_data_retired(r.s, moq_stream_ref_from_u64(0x5002), ++r.now) == MOQ_ERR_STALE_HANDLE);
    no_events(&r); finish_sibling(&r, o, 0x5003, 11);
    CHECK(sub_entry(&r, o.sub) && sub_entry(&r, o.sub)->processed_stream_count == 2);
    CHECK(moq_session_state(r.s) == MOQ_SESS_ESTABLISHED);
    close_rig(&r);
}

static void event_pressure(void)
{
    label = "d22.event-pressure";
    rig_t r; open_rig(&r, MOQ_VERSION_DRAFT_22, true, 1, 2);
    owner_t o = subscribe(&r, "a", 7, 128, -1, true);
    start_stream(&r, o, 0x5000, 10, 100, true);
    expect_candidate(&r, 0x5000, 128, 100); /* capacity never changes ranking */
    refusal(&r, r.owner, 0x5000, pressure_code(&r), MOQ_ERR_WOULD_BLOCK);
    expect_chunk(&r, o, 10, true, false, MOQ_OBJECT_TERMINAL_NORMAL, 0, 'A');
    CHECK(moq_session_cancel_admitted_data(r.s, r.owner, moq_stream_ref_from_u64(0x5000), 9) == MOQ_OK);
    expect_chunk(&r, o, 10, false, true, MOQ_OBJECT_TERMINAL_STOP, 0, -1);
    expect_stop(&r, 0x5000); no_events(&r);
    /* Genuine peer RESET after local STOP only settles ownership. */
    CHECK(moq_session_on_data_reset(r.s, moq_stream_ref_from_u64(0x5000), 77, ++r.now) == MOQ_OK);
    CHECK(sub_entry(&r, o.sub) && sub_entry(&r, o.sub)->processed_stream_count == 1);
    no_events(&r); close_rig(&r);
}

static void invisible_object(bool streaming)
{
    label = streaming ? "d22.suppressed" : "d22.whole-object";
    rig_t r; open_rig(&r, MOQ_VERSION_DRAFT_22, streaming, 1, 2);
    owner_t o = subscribe(&r, "a", 7, 128, -1, !streaming);
    owner_t other = subscribe(&r, "b", 8, 128, -1, true);
    start_stream(&r, o, 0x5000, 10, 100, true); no_events(&r);
    filler_event(&r, other); /* A full event ring is irrelevant: no begin surfaced. */
    CHECK(moq_session_cancel_admitted_data(r.s, r.owner, moq_stream_ref_from_u64(0x5000), 9) == MOQ_OK);
    expect_stop(&r, 0x5000); expect_filler(&r, other); no_events(&r);
    CHECK(rx_entry(&r, 0x5000) && rx_entry(&r, 0x5000)->payload_rcbuf == NULL &&
          rx_entry(&r, 0x5000)->input_buf == NULL);
    CHECK(moq_session_on_data_bytes(r.s, moq_stream_ref_from_u64(0x5000), NULL, 0, true, ++r.now) == MOQ_OK);
    CHECK(sub_entry(&r, o.sub) && sub_entry(&r, o.sub)->processed_stream_count == 1);
    no_events(&r); close_rig(&r);
}

static void first_reset_zero(void)
{
    label = "d22.first-reset-zero";
    rig_t r; open_rig(&r, MOQ_VERSION_DRAFT_22, true, 1, 2);
    owner_t o = subscribe(&r, "a", 7, 128, -1, true);
    start_stream(&r, o, 0x5000, 10, 100, true);
    REQUIRE(moq_session_on_data_reset(r.s, moq_stream_ref_from_u64(0x5000), 0, ++r.now) == MOQ_ERR_WOULD_BLOCK);
    REQUIRE(rx_entry(&r, 0x5000) && rx_entry(&r, 0x5000)->reset_owed &&
            rx_entry(&r, 0x5000)->reset_error_code == 0);
    refusal(&r, NULL, 0x5000, 9, MOQ_ERR_INVAL);
    refusal(&r, &o, 0x5000, 9, MOQ_ERR_INVAL);
    refusal(&r, r.owner, 0x5000, 9, MOQ_ERR_WOULD_BLOCK);
    moq_admitted_data_candidate_t c;
    CHECK(moq_session_worst_admitted_data(r.s, r.owner, &c) == MOQ_ERR_WOULD_BLOCK);
    CHECK(c.stream_ref._v == 0);
    expect_chunk(&r, o, 10, true, false, MOQ_OBJECT_TERMINAL_NORMAL, 0, 'A');
    CHECK(moq_session_on_data_reset(r.s, moq_stream_ref_from_u64(0x5000), 99, ++r.now) == MOQ_OK);
    expect_chunk(&r, o, 10, false, true, MOQ_OBJECT_TERMINAL_RESET, 0, -1);
    no_events(&r); drain_actions(&r);
    CHECK(!moq_session_rx_entry_active(r.s, r.owner, moq_stream_ref_from_u64(0x5000)));
    close_rig(&r);
}

static void pending_begin(void)
{
    label = "d22.pending-begin";
    rig_t r; open_rig(&r, MOQ_VERSION_DRAFT_22, true, 1, 2);
    owner_t o = subscribe(&r, "a", 7, 128, -1, true);
    filler_event(&r, o);
    static const uint8_t partial[] = {0x10, 7, 10, 100, 0, 4, 'A'};
    REQUIRE(moq_session_on_data_bytes(r.s, moq_stream_ref_from_u64(0x5000),
            partial, sizeof(partial), false, ++r.now) == MOQ_ERR_WOULD_BLOCK);
    REQUIRE(rx_entry(&r, 0x5000) && rx_entry(&r, 0x5000)->pending_begin &&
            !rx_entry(&r, 0x5000)->reset_owed);
    refusal(&r, r.owner, 0x5000, 9, MOQ_ERR_WOULD_BLOCK);
    moq_admitted_data_candidate_t c;
    CHECK(moq_session_worst_admitted_data(r.s, r.owner, &c) == MOQ_ERR_WOULD_BLOCK);
    expect_filler(&r, o);
    REQUIRE(moq_session_on_data_bytes(r.s, moq_stream_ref_from_u64(0x5000),
                                      NULL, 0, false, ++r.now) == MOQ_OK);
    expect_chunk(&r, o, 10, true, false, MOQ_OBJECT_TERMINAL_NORMAL, 0, 'A');
    CHECK(moq_session_cancel_admitted_data(r.s, r.owner, moq_stream_ref_from_u64(0x5000), 9) == MOQ_OK);
    expect_chunk(&r, o, 10, false, true, MOQ_OBJECT_TERMINAL_STOP, 0, -1);
    expect_stop(&r, 0x5000); no_events(&r); close_rig(&r);
}

static void ack_update(rig_t *r, owner_t o)
{
    static const uint8_t ack[] = {7, 0, 1, 0}; /* D22 REQUEST_OK, no parameters */
    REQUIRE(moq_session_on_bidi_stream_bytes(r->s, o.request, ack, sizeof(ack), false, ++r->now) == MOQ_OK);
    moq_event_t e;
    REQUIRE(moq_session_poll_events(r->s, &e, 1) == 1);
    REQUIRE(e.kind == (moq_subscription_is_valid(o.sub)
        ? MOQ_EVENT_SUBSCRIPTION_UPDATE_OK : MOQ_EVENT_PUBLICATION_UPDATE_OK));
    moq_event_cleanup(&e); no_events(r); drain_actions(r);
}
static void ranking_and_publication(void)
{
    label = "d22.effective-priorities-update-publication";
    rig_t r; open_rig(&r, MOQ_VERSION_DRAFT_22, true, 4, 4);
    owner_t a = subscribe(&r, "a", 7, 200, 10, true);
    owner_t b = publication(&r);
    start_stream(&r, a, 0x5003, 10, -1, false);
    start_stream(&r, a, 0x5000, 11, 0, false);
    start_stream(&r, b, 0x5002, 12, 240, false);
    start_stream(&r, b, 0x5001, 13, -1, false); /* last admission, lowest tied ref */
    expect_candidate(&r, 0x5003, 200, 10); /* subscriber outranks publisher */
    moq_subscription_update_cfg_t u; moq_subscription_update_cfg_init(&u);
    u.has_subscriber_priority = true; u.subscriber_priority = 198;
    REQUIRE(moq_session_update_subscription(r.s, a.sub, &u, ++r.now) == MOQ_OK);
    drain_actions(&r); expect_candidate(&r, 0x5003, 200, 10);
    ack_update(&r, a);
    expect_candidate(&r, 0x5001, 199, 240); /* default resolved, stable full-ref tie */
    moq_publication_update_cfg_t pu; moq_publication_update_cfg_init(&pu);
    pu.has_subscriber_priority = true; pu.subscriber_priority = 197;
    REQUIRE(moq_session_update_publication(r.s, b.pub, &pu, ++r.now) == MOQ_OK);
    drain_actions(&r); expect_candidate(&r, 0x5001, 199, 240);
    ack_update(&r, b); expect_candidate(&r, 0x5003, 198, 10);
    /* Cancel an exact publication-bound stream independently of selection. */
    CHECK(moq_session_cancel_admitted_data(r.s, r.owner, moq_stream_ref_from_u64(0x5001), 9) == MOQ_OK);
    expect_stop(&r, 0x5001); no_events(&r);
    CHECK(bridge_session_on_data_retired(r.s, moq_stream_ref_from_u64(0x5001), ++r.now) == MOQ_OK);
    CHECK(pub_entry(&r, b.pub) && pub_entry(&r, b.pub)->processed_stream_count == 1);
    finish_sibling(&r, b, 0x5002, 12);
    CHECK(pub_entry(&r, b.pub) && pub_entry(&r, b.pub)->processed_stream_count == 2);
    CHECK(sub_entry(&r, a.sub) && sub_entry(&r, a.sub)->processed_stream_count == 0);
    close_rig(&r);
}

static void owner_and_generation(void)
{
    label = "d22.owner-generation-reuse";
    rig_t r; open_rig(&r, MOQ_VERSION_DRAFT_22, true, 2, 2);
    owner_t old = subscribe(&r, "a", 7, 128, -1, true);
    start_stream(&r, old, 0x5000, 10, 100, false);
    refusal(&r, &old, 0x5000, 9, MOQ_ERR_INVAL);
    refusal(&r, r.owner, 0x5000, 1, MOQ_ERR_INVAL);
    refusal(&r, r.owner, 0xdead, 9, MOQ_ERR_INVAL);
    CHECK(moq_session_cancel_admitted_data(r.s, r.owner, moq_stream_ref_from_u64(0x5000), 9) == MOQ_OK);
    expect_stop(&r, 0x5000);
    REQUIRE(moq_session_unsubscribe(r.s, old.sub, ++r.now) == MOQ_OK);
    drain_actions(&r); no_events(&r);
    CHECK(!moq_session_rx_entry_active(r.s, r.owner, moq_stream_ref_from_u64(0x5000)));
    CHECK(moq_session_has_transport_stream(r.s, moq_stream_ref_from_u64(0x5000)));
    owner_t fresh = subscribe(&r, "fresh", 8, 222, -1, true);
    CHECK(moq_handle_slot(old.sub._opaque) == moq_handle_slot(fresh.sub._opaque));
    CHECK(moq_handle_generation(old.sub._opaque) != moq_handle_generation(fresh.sub._opaque));
    start_stream(&r, fresh, 0x5001, 11, 90, false);
    refusal(&r, r.owner, 0x5000, 9, MOQ_ERR_INVAL);
    CHECK(moq_session_on_data_reset(r.s, moq_stream_ref_from_u64(0x5000), 8, ++r.now) == MOQ_OK);
    CHECK(sub_entry(&r, fresh.sub) && sub_entry(&r, fresh.sub)->processed_stream_count == 0);
    int replacement_owner = 0;
    moq_session_unregister_transport_owner(r.s, r.owner);
    CHECK(moq_session_register_transport_owner(r.s, &replacement_owner) == MOQ_OK);
    refusal(&r, r.owner, 0x5001, 9, MOQ_ERR_INVAL);
    CHECK(!moq_session_rx_entry_active(r.s, r.owner, moq_stream_ref_from_u64(0x5001)));
    moq_admitted_data_candidate_t c;
    CHECK(moq_session_worst_admitted_data(r.s, r.owner, &c) == MOQ_ERR_INVAL);
    CHECK(c.stream_ref._v == 0);
    r.owner = &replacement_owner;
    expect_candidate(&r, 0x5001, 222, 90);
    CHECK(moq_session_cancel_admitted_data(r.s, r.owner, moq_stream_ref_from_u64(0x5001), 9) == MOQ_OK);
    expect_stop(&r, 0x5001);
    CHECK(bridge_session_on_data_retired(r.s, moq_stream_ref_from_u64(0x5001), ++r.now) == MOQ_OK);
    CHECK(sub_entry(&r, fresh.sub) && sub_entry(&r, fresh.sub)->processed_stream_count == 1);
    no_events(&r); close_rig(&r);
}

static void unknown_and_shared(void)
{
    label = "d22.unknown-shared-exclusions";
    rig_t r; open_rig(&r, MOQ_VERSION_DRAFT_22, true, 2, 2);
    owner_t first = subscribe(&r, "same", 7, 128, -1, true);
    (void)subscribe(&r, "same", 7, 255, -1, true);
    start_stream(&r, first, 0x5000, 10, 255, false);
    REQUIRE(rx_entry(&r, 0x5000)->fan_shared);
    static const uint8_t incomplete[] = {0x10};
    REQUIRE(moq_session_on_data_bytes(r.s, moq_stream_ref_from_u64(0x5001), incomplete,
                                      sizeof(incomplete), false, ++r.now) == MOQ_OK);
    refusal(&r, r.owner, 0x5000, 9, MOQ_ERR_INVAL);
    refusal(&r, r.owner, 0x5001, 9, MOQ_ERR_INVAL);
    moq_admitted_data_candidate_t c;
    CHECK(moq_session_worst_admitted_data(r.s, r.owner, &c) == MOQ_DONE);
    CHECK(c.stream_ref._v == 0 && c.subscriber_priority == 0 && c.publisher_priority == 0);
    CHECK(!moq_session_can_admit_data_stream(r.s));
    no_events(&r); drain_actions(&r); close_rig(&r);
}

int main(void)
{
    streaming_atomic(MOQ_VERSION_DRAFT_16);
    streaming_atomic(MOQ_VERSION_DRAFT_18);
    streaming_atomic(MOQ_VERSION_DRAFT_22);
    event_pressure();
    invisible_object(false);
    invisible_object(true);
    first_reset_zero();
    pending_begin();
    ranking_and_publication();
    owner_and_generation();
    unknown_and_shared();
    printf("receive_pressure: %u rows, %u checks, %u failures\n", rows, checks, failures);
    return failures ? 1 : 0;
}
