/*
 * Transport retirement reported through the public calls:
 * moq_session_on_data_stream_retired() and
 * moq_session_on_bidi_stream_retired().
 *
 * A stream this side stopped is kept until the peer's FIN or RESET. A
 * transport that drops the receive side after the stop delivers neither, and
 * the report releases what the session holds: no fabricated peer terminal (no
 * FIN fact, no reset, no peer cause, no stream terminal event), counted once
 * where the stream is still bound, and a done waiting for that stream is
 * completed with its one event. Admission refusals come before anything
 * runs; an admitted refusal still advances.
 *
 * Sessions, bindings and streams are established through real inputs, either
 * on one session or through the public-only host (public_host.c). Private
 * state is observed to pin exact counters; one row also stops a stream
 * through the admission-pressure cancel, and one fabricates an identity-only
 * stopped record. Link against moq-core-test-internals.
 */
#include <moq/session.h>
#include "test_binding_support.h"
#include "../../core/src/session/session_internal.h"
#include "../../core/src/session/session_transport.h"
#include "../public_host/public_host.h"

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
    moq_stream_ref_t request;
    uint64_t request_id;
    uint8_t alias;
} owner_t;

static moq_stream_ref_t ref_of(uint64_t v) { return moq_stream_ref_from_u64(v); }

static const moq_sub_entry_t *sub_entry(const rig_t *r, moq_subscription_t sub)
{
    for (size_t i = 0; i < r->s->sub_cap; ++i)
        if (r->s->subs[i].state != MOQ_SUB_FREE &&
            moq_subscription_eq(r->s->subs[i].handle, sub)) return &r->s->subs[i];
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
    for (int guard = 0; guard < 64; guard++) {
        if (moq_session_poll_actions(r->s, &a, 1) == 0) break;
        moq_action_cleanup(&a);
    }
}
static void drain_events(rig_t *r)
{
    moq_event_t e;
    for (int guard = 0; guard < 64; guard++) {
        if (moq_session_poll_events(r->s, &e, 1) == 0) break;
        moq_event_cleanup(&e);
    }
}
static void no_events(rig_t *r)
{
    moq_event_t e;
    for (int guard = 0; guard < 64; guard++) {
        if (moq_session_poll_events(r->s, &e, 1) == 0) break;
        CHECK(false); moq_event_cleanup(&e);
    }
}

/* A client session, with its setup handed over and not yet answered. */
static void create_rig(rig_t *r, moq_version_t version, bool streaming)
{
    memset(r, 0, sizeof(*r));
    r->version = version; r->now = 1; r->owner = r;
    moq_alloc_t alloc = {.ctx = &r->balance, .alloc = count_alloc,
                        .realloc = count_realloc, .free = count_free};
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), &alloc, MOQ_PERSPECTIVE_CLIENT);
    cfg.version = version; cfg.streaming_objects = streaming;
    cfg.max_events = 8; cfg.max_actions = 8;
    cfg.max_subscriptions = 8; cfg.max_publishes = 2;
    cfg.max_data_streams = 2;
    cfg.send_request_capacity = true; cfg.initial_request_capacity = 32;
    REQUIRE(moq_session_create(&cfg, 0, &r->s) == MOQ_OK);
    REQUIRE(test_bind_if_required(r->s) == MOQ_OK);
    REQUIRE(moq_session_register_transport_owner(r->s, r->owner) == MOQ_OK);
    REQUIRE(moq_session_start(r->s, r->now) == MOQ_OK);
    drain_actions(r);
}
static void establish_rig(rig_t *r)
{
    static const uint8_t d18_wire[] = {0xaf, 0, 0, 0};
    static const uint8_t d16_wire[] = {0x21, 0, 1, 0, 0x15, 0, 1, 32};
    const uint8_t *wire = r->version == MOQ_VERSION_DRAFT_16 ? d16_wire : d18_wire;
    size_t n = r->version == MOQ_VERSION_DRAFT_16 ? sizeof(d16_wire) : sizeof(d18_wire);
    REQUIRE(moq_session_on_control_bytes(r->s, wire, n, ++r->now) == MOQ_OK);
    REQUIRE(moq_session_state(r->s) == MOQ_SESS_ESTABLISHED);
    drain_events(r); drain_actions(r);
}
static void open_rig(rig_t *r, moq_version_t version, bool streaming)
{
    create_rig(r, version, streaming);
    establish_rig(r);
}
static void close_rig(rig_t *r)
{
    moq_session_unregister_transport_owner(r->s, r->owner);
    moq_session_destroy(r->s);
    CHECK(r->balance.blocks == 0 && r->balance.bytes == 0);
    ++rows;
}

static owner_t subscribe(rig_t *r, const char *name, uint8_t alias)
{
    owner_t o = {0};
    moq_bytes_t part = MOQ_BYTES_LITERAL("retire");
    moq_subscribe_cfg_t cfg; moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    cfg.track_name = (moq_bytes_t){(const uint8_t *)name, strlen(name)};
    cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    cfg.has_subscriber_priority = true; cfg.subscriber_priority = 128;
    cfg.has_forward = true; cfg.forward = true;
    REQUIRE(moq_session_subscribe(r->s, &cfg, ++r->now, &o.sub) == MOQ_OK);
    const moq_sub_entry_t *e = sub_entry(r, o.sub);
    REQUIRE(e != NULL);
    o.request = e->rs.ref; o.request_id = e->request_id; o.alias = alias;
    drain_actions(r);
    /* SUBSCRIBE_OK, no parameters. */
    uint8_t wire[16] = {4, 0, 0}; size_t n = 3;
    if (r->version == MOQ_VERSION_DRAFT_16) {
        REQUIRE(o.request_id < 64); wire[n++] = (uint8_t)o.request_id;
    }
    wire[n++] = alias; wire[n++] = 0;
    wire[2] = (uint8_t)(n - 3);
    moq_result_t rc = r->version == MOQ_VERSION_DRAFT_16
        ? moq_session_on_control_bytes(r->s, wire, n, ++r->now)
        : moq_session_on_bidi_stream_bytes(r->s, o.request, wire, n, false, ++r->now);
    REQUIRE(rc == MOQ_OK);
    moq_event_t e2;
    REQUIRE(moq_session_poll_events(r->s, &e2, 1) == 1);
    REQUIRE(e2.kind == MOQ_EVENT_SUBSCRIBE_OK);
    moq_event_cleanup(&e2); no_events(r); drain_actions(r);
    return o;
}

/* Explicit-priority subgroup header, optionally with the start of an object. */
static void start_stream(rig_t *r, owner_t o, uint64_t ref, uint8_t group,
                         bool partial)
{
    uint8_t wire[12] = {0x10, o.alias, group, 100}; size_t n = 4;
    if (partial) { wire[n++] = 0; wire[n++] = 4; wire[n++] = 'A'; }
    REQUIRE(moq_session_on_data_bytes(r->s, ref_of(ref), wire, n, false,
                                      ++r->now) == MOQ_OK);
    REQUIRE(rx_entry(r, ref) != NULL);
}

static moq_result_t retire_data(rig_t *r, uint64_t ref)
{
    return moq_session_on_data_stream_retired(r->s, ref_of(ref), ++r->now);
}

/* -- Data streams ---------------------------------------------------- */

/* Absent, live, stopped-with-its-stop-still-queued, stopped, and retired
 * again: only the fourth releases, and it counts the stream once. */
static void data_stopped_entry(moq_version_t version)
{
    char name[48];
    (void)snprintf(name, sizeof(name), "d%u.data.stopped-entry", (unsigned)version);
    label = name;
    rig_t r; open_rig(&r, version, true);
    owner_t o = subscribe(&r, "a", 7);
    start_stream(&r, o, 0x5002, 10, true);

    CHECK(retire_data(&r, 0x7001) == MOQ_ERR_STALE_HANDLE);

    /* A live stream is not this call's to end. */
    CHECK(retire_data(&r, 0x5002) == MOQ_ERR_WRONG_STATE);
    CHECK(rx_entry(&r, 0x5002) != NULL);
    if (rx_entry(&r, 0x5002))
        CHECK(rx_entry(&r, 0x5002)->parse_state != MOQ_RX_STOPPED);

    /* Stop it while its binding lives. */
    REQUIRE(moq_session_cancel_admitted_data(r.s, r.owner, ref_of(0x5002),
                                             pressure_code(&r)) == MOQ_OK);
    REQUIRE(rx_entry(&r, 0x5002) != NULL);
    REQUIRE(rx_entry(&r, 0x5002)->parse_state == MOQ_RX_STOPPED);

    /* The stop has not left the session, so no transport has accepted it:
     * the report is refused and the entry stays. */
    CHECK(retire_data(&r, 0x5002) == MOQ_ERR_WRONG_STATE);
    CHECK(rx_entry(&r, 0x5002) != NULL);
    CHECK(sub_entry(&r, o.sub) && sub_entry(&r, o.sub)->processed_stream_count == 0);

    moq_action_t a = {0};
    size_t polled = moq_session_poll_actions(r.s, &a, 1);
    CHECK(polled == 1 && a.kind == MOQ_ACTION_STOP_DATA);
    if (polled && a.kind == MOQ_ACTION_STOP_DATA)
        CHECK(a.u.stop_data.stream_ref._v == 0x5002);
    moq_action_cleanup(&a);
    drain_actions(&r);
    drain_events(&r);            /* the object's begin and its stop */

    size_t fin_count = r.s->rx_fin_count;
    CHECK(moq_session_has_transport_stream(r.s, ref_of(0x5002)));
    CHECK(retire_data(&r, 0x5002) == MOQ_OK);
    CHECK(rx_entry(&r, 0x5002) == NULL);
    CHECK(!moq_session_has_transport_stream(r.s, ref_of(0x5002)));
    CHECK(moq_session_can_admit_data_stream(r.s));
    CHECK(r.s->rx_fin_count == fin_count);                 /* no FIN fact */
    CHECK(sub_entry(&r, o.sub) && sub_entry(&r, o.sub)->processed_stream_count == 1);
    no_events(&r);
    drain_actions(&r);

    /* A repeated report finds nothing and counts nothing. */
    CHECK(retire_data(&r, 0x5002) == MOQ_ERR_STALE_HANDLE);
    CHECK(sub_entry(&r, o.sub) && sub_entry(&r, o.sub)->processed_stream_count == 1);

    /* Bytes fed for the ref after it was retired start a new stream: that
     * is why the caller may not hold any when it reports. */
    start_stream(&r, o, 0x5002, 12, false);
    CHECK(rx_entry(&r, 0x5002) && rx_entry(&r, 0x5002)->parse_state != MOQ_RX_STOPPED);
    CHECK(sub_entry(&r, o.sub) && sub_entry(&r, o.sub)->processed_stream_count == 1);
    CHECK(moq_session_state(r.s) == MOQ_SESS_ESTABLISHED);
    close_rig(&r);
}

/* A stream whose binding ended is kept as an identity only. It releases the
 * same way and counts nothing. */
static void data_stopped_record(moq_version_t version)
{
    char name[48];
    (void)snprintf(name, sizeof(name), "d%u.data.stopped-record", (unsigned)version);
    label = name;
    rig_t r; open_rig(&r, version, false);
    owner_t o = subscribe(&r, "a", 7);
    REQUIRE(r.s->rx_stopped_count < r.s->rx_stopped_cap);
    size_t before = r.s->rx_stopped_count;
    r.s->rx_stopped_refs[r.s->rx_stopped_count++] = 0x7777;
    CHECK(moq_session_has_transport_stream(r.s, ref_of(0x7777)));

    CHECK(retire_data(&r, 0x7777) == MOQ_OK);
    CHECK(r.s->rx_stopped_count == before);
    CHECK(!moq_session_has_transport_stream(r.s, ref_of(0x7777)));
    CHECK(sub_entry(&r, o.sub) && sub_entry(&r, o.sub)->processed_stream_count == 0);
    no_events(&r);
    CHECK(retire_data(&r, 0x7777) == MOQ_ERR_STALE_HANDLE);
    close_rig(&r);
}

/* Outside an established session there is nothing to retire. */
static void data_inactive(moq_version_t version)
{
    char name[48];
    (void)snprintf(name, sizeof(name), "d%u.data.inactive", (unsigned)version);
    label = name;
    rig_t r; create_rig(&r, version, false);
    CHECK(moq_session_on_data_stream_retired(NULL, ref_of(1), 1) == MOQ_ERR_INVAL);
    CHECK(retire_data(&r, 0x5002) == MOQ_ERR_CLOSED);      /* setup not complete */
    establish_rig(&r);
    REQUIRE(moq_session_close(r.s, 0, NULL, ++r.now) == MOQ_OK);
    CHECK(retire_data(&r, 0x5002) == MOQ_ERR_CLOSED);
    close_rig(&r);
}

/* -- Request streams -------------------------------------------------- */

static moq_result_t retire_bidi(rig_t *r, moq_stream_ref_t ref)
{
    return moq_session_on_bidi_stream_retired(r->s, ref, ++r->now);
}

static bool action_names(const moq_action_t *a, moq_stream_ref_t ref)
{
    switch (a->kind) {
    case MOQ_ACTION_SEND_BIDI_STREAM:  return a->u.send_bidi_stream.stream_ref._v == ref._v;
    case MOQ_ACTION_CLOSE_BIDI_STREAM: return a->u.close_bidi_stream.stream_ref._v == ref._v;
    case MOQ_ACTION_RESET_BIDI_STREAM: return a->u.reset_bidi_stream.stream_ref._v == ref._v;
    case MOQ_ACTION_STOP_BIDI_STREAM:  return a->u.stop_bidi_stream.stream_ref._v == ref._v;
    case MOQ_ACTION_ABORT_BIDI_STREAM: return a->u.abort_bidi_stream.stream_ref._v == ref._v;
    default: return false;
    }
}

/* A request stream this side abandoned keeps its identity until the peer's
 * terminal. The report releases it: after the owner has ended and its
 * teardown has been polled, and not before. */
static void bidi_abandoned_request(moq_version_t version)
{
    char name[48];
    (void)snprintf(name, sizeof(name), "d%u.bidi.abandoned", (unsigned)version);
    label = name;
    rig_t r; open_rig(&r, version, false);
    owner_t o = subscribe(&r, "a", 7);
    REQUIRE(o.request._v != 0);

    CHECK(retire_bidi(&r, ref_of(0)) == MOQ_ERR_INVAL);
    CHECK(moq_session_on_bidi_stream_retired(NULL, o.request, 1) == MOQ_ERR_INVAL);
    CHECK(retire_bidi(&r, ref_of(0x9999)) == MOQ_ERR_STALE_HANDLE);

    /* A request still owns the stream. */
    CHECK(retire_bidi(&r, o.request) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_session_has_transport_stream(r.s, o.request));
    CHECK(sub_entry(&r, o.sub) != NULL);

    REQUIRE(moq_session_unsubscribe(r.s, o.sub, ++r.now) == MOQ_OK);
    /* Its teardown is still queued: the transport cannot have retired a
     * stream it was not yet told to abandon. */
    CHECK(retire_bidi(&r, o.request) == MOQ_ERR_WRONG_STATE);
    size_t named = 0;
    moq_action_t a;
    for (int guard = 0; guard < 16; guard++) {
        if (moq_session_poll_actions(r.s, &a, 1) == 0) break;
        if (action_names(&a, o.request)) named++;
        moq_action_cleanup(&a);
    }
    CHECK(named >= 1);
    drain_events(&r);
    REQUIRE(moq_session_has_transport_stream(r.s, o.request));   /* its identity */

    /* What the peer still sends is absorbed, not taken for a new request. */
    static const uint8_t late[] = {0x00, 0x00};
    CHECK(moq_session_on_bidi_stream_bytes(r.s, o.request, late, sizeof(late),
                                           false, ++r.now) == MOQ_OK);
    no_events(&r);
    CHECK(moq_session_has_transport_stream(r.s, o.request));

    CHECK(retire_bidi(&r, o.request) == MOQ_OK);
    CHECK(!moq_session_has_transport_stream(r.s, o.request));
    no_events(&r);
    drain_actions(&r);
    CHECK(retire_bidi(&r, o.request) == MOQ_ERR_STALE_HANDLE);
    CHECK(moq_session_state(r.s) == MOQ_SESS_ESTABLISHED);

    REQUIRE(moq_session_close(r.s, 0, NULL, ++r.now) == MOQ_OK);
    CHECK(retire_bidi(&r, o.request) == MOQ_ERR_CLOSED);
    close_rig(&r);
}

/* -- Admission and borrows ------------------------------------------ */

/* A profile that requires a transport binding refuses both reports on an
 * unbound session before anything runs, in IDLE and in CLOSED alike: the
 * refusal is the same for every ref and leaves borrows valid. A session
 * can be closed unbound, because the control-ended report needs no
 * binding. */
static void retire_unbound(void)
{
    label = "d22.unbound";
    balance_t balance = {0};
    moq_alloc_t alloc = {.ctx = &balance, .alloc = count_alloc,
                         .realloc = count_realloc, .free = count_free};
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), &alloc, MOQ_PERSPECTIVE_CLIENT);
    cfg.version = MOQ_VERSION_DRAFT_22;
    moq_session_t *s = NULL;
    REQUIRE(moq_session_create(&cfg, 0, &s) == MOQ_OK);
    REQUIRE(moq_session_binding_requirements(s) != 0);
    uint64_t now = 1;

    /* Argument checks come before the binding check. */
    CHECK(moq_session_on_data_stream_retired(NULL, ref_of(1), now) == MOQ_ERR_INVAL);
    CHECK(moq_session_on_bidi_stream_retired(s, ref_of(0), now) == MOQ_ERR_INVAL);
    CHECK(moq_session_on_data_stream_retired(s, ref_of(0x5002), ++now) ==
          MOQ_ERR_WRONG_STATE);
    CHECK(moq_session_on_bidi_stream_retired(s, ref_of(0x9999), ++now) ==
          MOQ_ERR_WRONG_STATE);
    CHECK(moq_session_state(s) == MOQ_SESS_IDLE);

    CHECK(moq_session_on_control_stream_ended(s, ++now) == MOQ_OK);
    REQUIRE(moq_session_state(s) == MOQ_SESS_CLOSED);
    moq_event_t ev;
    REQUIRE(moq_session_poll_events(s, &ev, 1) == 1);
    REQUIRE(ev.kind == MOQ_EVENT_SESSION_CLOSED);
    uint64_t held = ev.borrow_epoch;
    CHECK(moq_session_borrow_valid(s, held));
    /* Closed, still unbound: refused the same way, not as MOQ_ERR_CLOSED,
     * and nothing ran. */
    CHECK(moq_session_on_data_stream_retired(s, ref_of(0x5002), ++now) ==
          MOQ_ERR_WRONG_STATE);
    CHECK(moq_session_borrow_valid(s, held));
    CHECK(moq_session_on_bidi_stream_retired(s, ref_of(0x9999), ++now) ==
          MOQ_ERR_WRONG_STATE);
    CHECK(moq_session_borrow_valid(s, held));
    moq_event_cleanup(&ev);
    moq_action_t a;
    for (int guard = 0; guard < 8; guard++) {
        if (moq_session_poll_actions(s, &a, 1) == 0) break;
        moq_action_cleanup(&a);
    }
    moq_session_destroy(s);
    CHECK(balance.blocks == 0 && balance.bytes == 0);
    ++rows;
}

/* A borrow held from the newest polled record: a request queued and its
 * actions polled. */
static uint64_t fresh_borrow(rig_t *r, const char *name)
{
    moq_bytes_t part = MOQ_BYTES_LITERAL("borrow");
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    cfg.track_name = (moq_bytes_t){(const uint8_t *)name, strlen(name)};
    cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sub;
    REQUIRE(moq_session_subscribe(r->s, &cfg, ++r->now, &sub) == MOQ_OK);
    uint64_t epoch = 0;
    size_t polled = 0;
    moq_action_t a;
    for (int guard = 0; guard < 16; guard++) {
        if (moq_session_poll_actions(r->s, &a, 1) == 0) break;
        epoch = a.borrow_epoch;
        polled++;
        moq_action_cleanup(&a);
    }
    REQUIRE(polled > 0 && moq_session_borrow_valid(r->s, epoch));
    return epoch;
}

/* An admitted call is advancing whatever it answers: an absent ref, a live
 * stream and an owned request stream are refused, and each refusal ends
 * the borrows held before it. */
static void retire_borrows(moq_version_t version)
{
    char name[48];
    (void)snprintf(name, sizeof(name), "d%u.borrows", (unsigned)version);
    label = name;
    rig_t r; open_rig(&r, version, true);
    owner_t o = subscribe(&r, "a", 7);
    start_stream(&r, o, 0x5002, 10, true);
    drain_events(&r);

    uint64_t b = fresh_borrow(&r, "b1");
    CHECK(retire_data(&r, 0x7001) == MOQ_ERR_STALE_HANDLE);
    CHECK(!moq_session_borrow_valid(r.s, b));
    b = fresh_borrow(&r, "b2");
    CHECK(retire_data(&r, 0x5002) == MOQ_ERR_WRONG_STATE);
    CHECK(!moq_session_borrow_valid(r.s, b));
    b = fresh_borrow(&r, "b3");
    CHECK(retire_bidi(&r, ref_of(0x9999)) == MOQ_ERR_STALE_HANDLE);
    CHECK(!moq_session_borrow_valid(r.s, b));
    if (o.request._v != 0) {
        b = fresh_borrow(&r, "b4");
        CHECK(retire_bidi(&r, o.request) == MOQ_ERR_WRONG_STATE);
        CHECK(!moq_session_borrow_valid(r.s, b));
    }
    /* An argument refusal comes before admission and ends nothing. */
    b = fresh_borrow(&r, "b5");
    CHECK(retire_bidi(&r, ref_of(0)) == MOQ_ERR_INVAL);
    CHECK(moq_session_borrow_valid(r.s, b));
    CHECK(moq_session_state(r.s) == MOQ_SESS_ESTABLISHED);
    close_rig(&r);
}

/* -- Through a host on the public calls ------------------------------ */

#define CLIENT 0
#define SERVER 1

/* One polled event, reduced to what these rows assert on. */
typedef struct rec {
    moq_event_kind_t kind;
    moq_subscription_t sub;
    moq_publication_t pub;
    moq_fill_t fill;
    uint64_t status, count, code;
    uint32_t reason, cause, domain;
    char text[16];
    size_t text_len;
} rec_t;
typedef struct recs {
    rec_t e[48];
    size_t n;
} recs_t;

/* Poll everything the side has queued; returns how many events. With no
 * log the events are released unread. */
static size_t collect(ph_pair_t *p, int side, recs_t *out)
{
    size_t polled = 0;
    moq_event_t ev;
    for (int guard = 0; guard < 128; guard++) {
        if (moq_session_poll_events(ph_session(p, side), &ev, 1) == 0) break;
        polled++;
        if (out) CHECK(out->n < sizeof(out->e) / sizeof(out->e[0]));
        if (out && out->n < sizeof(out->e) / sizeof(out->e[0])) {
            rec_t *x = &out->e[out->n++];
            memset(x, 0, sizeof(*x));
            x->kind = ev.kind;
            moq_bytes_t text = {0};
            switch (ev.kind) {
            case MOQ_EVENT_SUBSCRIBE_REQUEST:
                x->sub = ev.u.subscribe_request.sub;
                text = ev.u.subscribe_request.track_name;
                break;
            case MOQ_EVENT_SUBSCRIBE_OK:
                x->sub = ev.u.subscribe_ok.sub;
                break;
            case MOQ_EVENT_SUBSCRIBE_DONE:
                x->sub = ev.u.subscribe_done.sub;
                x->status = ev.u.subscribe_done.status_code;
                x->count = ev.u.subscribe_done.stream_count;
                text = ev.u.subscribe_done.reason;
                break;
            case MOQ_EVENT_PUBLISH_REQUEST:
                x->pub = ev.u.publish_request.pub;
                break;
            case MOQ_EVENT_PUBLISH_FINISHED:
                x->pub = ev.u.publish_finished.pub;
                x->status = ev.u.publish_finished.status_code;
                x->count = ev.u.publish_finished.stream_count;
                text = ev.u.publish_finished.reason;
                if ((size_t)ev.detail_size >=
                    offsetof(moq_publish_finished_event_t, cause) +
                    sizeof(moq_request_terminal_cause_t))
                    x->cause = ev.u.publish_finished.cause;
                break;
            case MOQ_EVENT_FILL_CREATED:
                x->fill = ev.u.fill_created.fill;
                break;
            case MOQ_EVENT_FILL_REQUEST:
                x->fill = ev.u.fill_request.fill;
                break;
            case MOQ_EVENT_FILL_TERMINAL:
                x->fill = ev.u.fill_terminal.fill;
                x->sub = ev.u.fill_terminal.sub;
                x->reason = ev.u.fill_terminal.reason;
                x->domain = ev.u.fill_terminal.error_domain;
                x->code = ev.u.fill_terminal.error_code;
                break;
            default:
                break;
            }
            CHECK(text.len <= sizeof(x->text));
            x->text_len = text.len <= sizeof(x->text) ? text.len : sizeof(x->text);
            if (x->text_len) memcpy(x->text, text.data, x->text_len);
        }
        moq_event_cleanup(&ev);
    }
    return polled;
}

static size_t kinds(const recs_t *l, moq_event_kind_t kind)
{
    size_t n = 0;
    for (size_t i = 0; i < l->n; i++)
        if (l->e[i].kind == kind) n++;
    return n;
}

static int first_kind(const recs_t *l, moq_event_kind_t kind)
{
    for (size_t i = 0; i < l->n; i++)
        if (l->e[i].kind == kind) return (int)i;
    return -1;
}

/* Run the pair until it is quiet with both event queues drained. Quiet is
 * not done: the rows assert what completed. */
static void settle(ph_pair_t *p, recs_t *client, recs_t *server)
{
    for (int round = 0; round < 32; round++) {
        bool quiet = ph_run(p, 400);
        size_t polled = collect(p, CLIENT, client) + collect(p, SERVER, server);
        if (quiet && polled == 0) return;
    }
    CHECK(!"the pair did not settle in 32 rounds");
}

static moq_rcbuf_t *payload(ph_pair_t *p, const char *text)
{
    moq_rcbuf_t *b = NULL;
    REQUIRE(moq_rcbuf_create(ph_allocator(p), (const uint8_t *)text,
                             strlen(text), &b) == MOQ_OK);
    return b;
}

/* A cancelled fill still owes its stream's end. When the transport retires
 * that stream with no terminal, the report settles the fill and counts the
 * stream on the parent subscription once; the fill's one terminal is the
 * cancellation's. */
static void fill_child_reclaimed(moq_version_t version)
{
    char name[48];
    (void)snprintf(name, sizeof(name), "d%u.data.fill-child", (unsigned)version);
    label = name;
    ph_options_t opt = {0};
    opt.drop_after_stop = true;
    ph_pair_t *p = ph_pair_create(version, &opt);
    REQUIRE(p != NULL);
    settle(p, NULL, NULL);
    moq_session_t *c = ph_session(p, CLIENT), *sv = ph_session(p, SERVER);
    REQUIRE(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
    if (!moq_session_supports_fill(c)) {
        CHECK(ph_pair_destroy(p) == 0);
        return;                  /* this profile has no fills */
    }

    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    sc.track_name = MOQ_BYTES_LITERAL("f");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    sc.fill = &fill;
    moq_subscription_t csub;
    REQUIRE(moq_session_subscribe(c, &sc, ph_now(p), &csub) == MOQ_OK);
    recs_t cl = {0}, sl = {0};
    settle(p, &cl, &sl);
    int at = first_kind(&sl, MOQ_EVENT_SUBSCRIBE_REQUEST);
    REQUIRE(at >= 0);
    /* The subscriber's child exists from its request on, before any
     * stream: its FILL_CREATED comes with the subscribe. */
    int created = first_kind(&cl, MOQ_EVENT_FILL_CREATED);
    REQUIRE(created >= 0);
    moq_fill_t cfill = cl.e[created].fill;
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true; ac.largest_group = 2; ac.largest_object = 0;
    REQUIRE(moq_session_accept_subscribe(sv, sl.e[at].sub, &ac, ph_now(p)) == MOQ_OK);
    cl.n = sl.n = 0;
    settle(p, &cl, &sl);
    int requested = first_kind(&sl, MOQ_EVENT_FILL_REQUEST);
    REQUIRE(requested >= 0);
    moq_fill_t sfill = sl.e[requested].fill;

    REQUIRE(moq_session_open_fill(sv, sfill, ph_now(p)) == MOQ_OK);
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0; oc.object_id = 0; oc.publisher_priority = 5;
    oc.payload = payload(p, "a");
    REQUIRE(moq_session_write_fill_object(sv, sfill, &oc, ph_now(p)) == MOQ_OK);
    moq_rcbuf_decref(oc.payload);
    cl.n = 0;
    settle(p, &cl, NULL);

    int slot = sub_resolve_handle(c, csub);
    REQUIRE(slot >= 0);
    uint64_t processed = c->subs[slot].processed_stream_count;
    size_t fin_count = c->rx_fin_count;
    REQUIRE(c->fills.live == 1);
    moq_stream_ref_t data = {0};
    REQUIRE(ph_only_open_peer_data_stream(p, CLIENT, &data));

    REQUIRE(moq_session_cancel_fill(c, cfill, ph_now(p)) == MOQ_OK);
    CHECK(c->fills.live == 1);                  /* it still owes its stream's end */
    cl.n = 0;
    settle(p, &cl, NULL);

    CHECK(ph_counters(p, CLIENT)->data_retired == 1);
    CHECK(c->fills.live == 0);                  /* reclaimed at the release */
    CHECK(c->subs[slot].processed_stream_count == processed + 1);
    CHECK(c->rx_fin_count == fin_count);
    CHECK(!moq_session_has_transport_stream(c, data));
    CHECK(c->subs[slot].state == MOQ_SUB_ESTABLISHED);
    CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
    /* Exactly one terminal, the cancellation's, across cancel and release. */
    CHECK(kinds(&cl, MOQ_EVENT_FILL_TERMINAL) == 1);
    at = first_kind(&cl, MOQ_EVENT_FILL_TERMINAL);
    if (at >= 0) {
        CHECK(moq_fill_eq(cl.e[at].fill, cfill));
        CHECK(moq_subscription_eq(cl.e[at].sub, csub));
        CHECK(cl.e[at].reason == MOQ_FILL_LOCAL_CANCELLED);
        CHECK(cl.e[at].domain == MOQ_FILL_ERROR_NONE && cl.e[at].code == 0);
    }
    CHECK(moq_session_on_data_stream_retired(c, data, ph_now(p)) == MOQ_ERR_STALE_HANDLE);
    CHECK(c->fills.live == 0);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    CHECK(ph_pair_destroy(p) == 0);
    ++rows;
}

/* The done a subscription or publication received is waiting for exactly
 * one more stream, and that stream is the one the transport retires. The
 * release counts it once and completes the done: its one event, with the
 * done's own status, count and reason, and no terminal for the stream
 * itself. Under event pressure the event is not lost: the next advancing
 * call queues it. A repeated report finds nothing.
 *
 * The publisher's FINs are held back once its first stream has ended, so
 * the second stream's end never reaches the subscriber; the subscriber
 * stops that stream through the admission-pressure cancel while the done
 * waits, and its transport drops the stream. */
static void done_waits_for_retired(moq_version_t version, bool publication,
                                   bool pressure)
{
    char name[64];
    (void)snprintf(name, sizeof(name), "d%u.done-gate.%s%s", (unsigned)version,
                   publication ? "publication" : "subscription",
                   pressure ? ".pressure" : "");
    label = name;
    ph_options_t opt = {0};
    opt.drop_after_stop = true;
    if (pressure) opt.max_events[CLIENT] = 1;
    ph_pair_t *p = ph_pair_create(version, &opt);
    REQUIRE(p != NULL);
    settle(p, NULL, NULL);
    moq_session_t *c = ph_session(p, CLIENT), *sv = ph_session(p, SERVER);
    REQUIRE(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_subscription_t csub = MOQ_SUBSCRIPTION_INVALID, ssub = MOQ_SUBSCRIPTION_INVALID;
    moq_publication_t cpub = MOQ_PUBLICATION_INVALID, spub = MOQ_PUBLICATION_INVALID;
    recs_t cl = {0}, sl = {0};

    if (!publication) {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
        sc.track_name = MOQ_BYTES_LITERAL("t");
        sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        REQUIRE(moq_session_subscribe(c, &sc, ph_now(p), &csub) == MOQ_OK);
        settle(p, &cl, &sl);
        int at = first_kind(&sl, MOQ_EVENT_SUBSCRIBE_REQUEST);
        REQUIRE(at >= 0);
        ssub = sl.e[at].sub;
        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        REQUIRE(moq_session_accept_subscribe(sv, ssub, &ac, ph_now(p)) == MOQ_OK);
    } else {
        moq_publish_cfg_t pc;
        moq_publish_cfg_init(&pc);
        pc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
        pc.track_name = MOQ_BYTES_LITERAL("p");
        REQUIRE(moq_session_publish(sv, &pc, ph_now(p), &spub) == MOQ_OK);
        settle(p, &cl, &sl);
        int at = first_kind(&cl, MOQ_EVENT_PUBLISH_REQUEST);
        REQUIRE(at >= 0);
        cpub = cl.e[at].pub;
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        REQUIRE(moq_session_accept_publish(c, cpub, &ac, ph_now(p)) == MOQ_OK);
    }
    settle(p, NULL, NULL);

    moq_subgroup_handle_t sg[2];
    for (uint64_t g = 0; g < 2; g++) {
        moq_subgroup_cfg_t sc;
        moq_subgroup_cfg_init(&sc);
        sc.group_id = g;
        sc.first_object = true;
        REQUIRE((publication
                 ? moq_session_open_pub_subgroup(sv, spub, &sc, ph_now(p), &sg[g])
                 : moq_session_open_subgroup(sv, ssub, &sc, ph_now(p), &sg[g])) == MOQ_OK);
        moq_rcbuf_t *b = payload(p, g ? "one" : "zero");
        REQUIRE(moq_session_write_object(sv, sg[g], 0, b, ph_now(p)) == MOQ_OK);
        moq_rcbuf_decref(b);
    }
    REQUIRE(moq_session_close_subgroup(sv, sg[0], ph_now(p)) == MOQ_OK);
    settle(p, NULL, NULL);

    /* The owner's entry on the subscriber, and its first stream counted. */
    uint64_t *processed, *done_count;
    bool *done_pending;
    if (publication) {
        int slot = pub_resolve_handle(c, cpub);
        REQUIRE(slot >= 0);
        processed = &c->publishes[slot].processed_stream_count;
        done_count = &c->publishes[slot].done_stream_count;
        done_pending = &c->publishes[slot].done_pending;
    } else {
        int slot = sub_resolve_handle(c, csub);
        REQUIRE(slot >= 0);
        processed = &c->subs[slot].processed_stream_count;
        done_count = &c->subs[slot].done_stream_count;
        done_pending = &c->subs[slot].done_pending;
    }
    REQUIRE(*processed == 1);

    /* The publisher ends the second stream and the owner, Stream Count 2;
     * the second FIN is held back. */
    ph_set_fin_hold(p, SERVER, true);
    REQUIRE(moq_session_close_subgroup(sv, sg[1], ph_now(p)) == MOQ_OK);
    uint64_t status = 0;
    if (publication) {
        moq_finish_publish_cfg_t fc;
        moq_finish_publish_cfg_init(&fc);
        fc.stream_count = 2;
        fc.reason = MOQ_BYTES_LITERAL("done");
        status = fc.status_code;
        REQUIRE(moq_session_finish_publish(sv, spub, &fc, ph_now(p)) == MOQ_OK);
    } else {
        moq_done_subscribe_cfg_t dc;
        moq_done_subscribe_cfg_init(&dc);
        dc.stream_count = 2;
        dc.reason = MOQ_BYTES_LITERAL("done");
        status = dc.status_code;
        REQUIRE(moq_session_done_subscribe(sv, ssub, &dc, ph_now(p)) == MOQ_OK);
    }
    cl.n = 0;
    settle(p, &cl, NULL);
    /* The done waits for this one stream. */
    CHECK(kinds(&cl, MOQ_EVENT_SUBSCRIBE_DONE) + kinds(&cl, MOQ_EVENT_PUBLISH_FINISHED) == 0);
    REQUIRE(*done_pending && *done_count == 2 && *processed == 1);
    moq_stream_ref_t data = {0};
    REQUIRE(ph_only_open_peer_data_stream(p, CLIENT, &data));

    moq_subscription_t other = MOQ_SUBSCRIPTION_INVALID;
    if (pressure) {
        /* Fill the subscriber's one event slot with an unrelated answer. */
        moq_subscribe_cfg_t xc;
        moq_subscribe_cfg_init(&xc);
        xc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
        xc.track_name = MOQ_BYTES_LITERAL("x");
        xc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        REQUIRE(moq_session_subscribe(c, &xc, ph_now(p), &other) == MOQ_OK);
        sl.n = 0;
        (void)ph_run(p, 400);
        collect(p, SERVER, &sl);
        int at = first_kind(&sl, MOQ_EVENT_SUBSCRIBE_REQUEST);
        REQUIRE(at >= 0);
        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        REQUIRE(moq_session_accept_subscribe(sv, sl.e[at].sub, &ac, ph_now(p)) == MOQ_OK);
        (void)ph_run(p, 400);
    }

    /* The subscriber stops the stream; its transport drops it and reports
     * the retirement. */
    const char token = 0;
    size_t fin_count = c->rx_fin_count;
    REQUIRE(moq_session_register_transport_owner(c, &token) == MOQ_OK);
    moq_result_t crc = moq_session_cancel_admitted_data(c, &token, data,
                                                        c->profile->resource_limit_stream_error);
    moq_session_unregister_transport_owner(c, &token);
    REQUIRE(crc == MOQ_OK);
    (void)ph_run(p, 400);
    CHECK(ph_counters(p, CLIENT)->data_retired == 1);
    CHECK(!moq_session_has_transport_stream(c, data));
    CHECK(c->rx_fin_count == fin_count);          /* no FIN fact */

    cl.n = 0;
    if (pressure) {
        /* The release counted the stream; the event queue was full, so the
         * done is still owed and nothing else was queued. */
        collect(p, CLIENT, &cl);
        CHECK(cl.n == 1 && kinds(&cl, MOQ_EVENT_SUBSCRIBE_OK) == 1);
        if (cl.n == 1) CHECK(moq_subscription_eq(cl.e[0].sub, other));
        CHECK(*done_pending && *processed == 2);
        cl.n = 0;
        CHECK(moq_session_tick(c, ph_now(p)) == MOQ_OK);
    }
    collect(p, CLIENT, &cl);
    moq_event_kind_t done_kind = publication ? MOQ_EVENT_PUBLISH_FINISHED
                                             : MOQ_EVENT_SUBSCRIBE_DONE;
    CHECK(kinds(&cl, done_kind) == 1);
    CHECK(kinds(&cl, MOQ_EVENT_SUBSCRIBE_DONE) + kinds(&cl, MOQ_EVENT_PUBLISH_FINISHED) == 1);
    CHECK(kinds(&cl, MOQ_EVENT_SUBGROUP_FINISHED) == 0);
    CHECK(kinds(&cl, MOQ_EVENT_SUBGROUP_RESET) == 0);
    int at = first_kind(&cl, done_kind);
    if (at >= 0) {
        const rec_t *x = &cl.e[at];
        if (publication) {
            CHECK(moq_publication_eq(x->pub, cpub));
            CHECK(x->cause == MOQ_REQUEST_TERMINAL_NONE);
        } else {
            CHECK(moq_subscription_eq(x->sub, csub));
        }
        CHECK(x->status == status && x->count == 2);
        CHECK(x->text_len == 4 && memcmp(x->text, "done", 4) == 0);
    }

    /* A repeated report finds nothing, and queues nothing. */
    CHECK(moq_session_on_data_stream_retired(c, data, ph_now(p)) == MOQ_ERR_STALE_HANDLE);
    cl.n = 0;
    collect(p, CLIENT, &cl);
    CHECK(cl.n == 0);
    CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);

    ph_set_fin_hold(p, SERVER, false);
    settle(p, NULL, NULL);
    CHECK(ph_pair_destroy(p) == 0);
    ++rows;
}

/* Poll every event both sides hold, releasing each (bounded). */
static void pair_drain_events(ph_pair_t *p)
{
    for (int side = 0; side < 2; side++) {
        moq_event_t ev;
        for (int guard = 0; guard < 256; guard++) {
            if (moq_session_poll_events(ph_session(p, side), &ev, 1) == 0) break;
            moq_event_cleanup(&ev);
        }
    }
}

/* Local stream refs stop below the host's range: at the bound every local
 * open is refused with the permanent MOQ_ERR_LIMIT and changes nothing. */
static void local_ref_limit(moq_version_t version)
{
    label = "local_ref_limit";
    rows++;
    ph_pair_t *p = ph_pair_create(version, NULL);
    REQUIRE(p != NULL);
    REQUIRE(ph_run(p, 400));
    moq_session_t *c = ph_session(p, 0);
    const uint64_t saved = c->next_stream_ref;
    c->next_stream_ref = MOQ_STREAM_REF_LOCAL_END;
    const size_t queued = c->action_tail - c->action_head;
    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    sc.track_name = MOQ_BYTES_LITERAL("t");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    moq_result_t rc = moq_session_subscribe(c, &sc, ph_now(p), &sub);
    if (version != MOQ_VERSION_DRAFT_16) {
        CHECK(rc == MOQ_ERR_LIMIT);
        CHECK(!moq_subscription_is_valid(sub));
        CHECK(c->action_tail - c->action_head == queued);
        CHECK(c->next_stream_ref == MOQ_STREAM_REF_LOCAL_END);
        /* Permanent: a retry is refused the same way. */
        CHECK(moq_session_subscribe(c, &sc, ph_now(p), &sub) == MOQ_ERR_LIMIT);
    } else {
        /* Draft 16 opens no stream for a request. */
        CHECK(rc == MOQ_OK);
    }
    c->next_stream_ref = saved;
    CHECK(ph_run(p, 400));
    pair_drain_events(p);
    (void)ph_pair_destroy(p);
}

/* Tagged mode: a request slot whose generation would leave the handle's
 * field is never reused, so an output-owner token is never 0 and never
 * repeats; the last representable generation is still used. */
static void owner_generation_bound(moq_version_t version)
{
    label = "owner_generation_bound";
    rows++;
    ph_options_t o;
    memset(&o, 0, sizeof(o));
    o.tagged[0] = true;
    ph_pair_t *p = ph_pair_create(version, &o);
    REQUIRE(p != NULL);
    REQUIRE(ph_run(p, 400));
    moq_session_t *c = ph_session(p, 0);
    for (size_t i = 0; i < c->sub_cap; i++)
        if (c->subs[i].state == MOQ_SUB_FREE) c->subs[i].generation = 0x10000000u;
    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    sc.track_name = MOQ_BYTES_LITERAL("t");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    CHECK(moq_session_subscribe(c, &sc, ph_now(p), &sub) != MOQ_OK);
    CHECK(!moq_subscription_is_valid(sub));
    size_t last = c->sub_cap;
    for (size_t i = 0; i < c->sub_cap; i++)
        if (c->subs[i].state == MOQ_SUB_FREE) { last = i; break; }
    REQUIRE(last < c->sub_cap);
    c->subs[last].generation = 0xFFFFFFEu;   /* next live: 0xFFFFFFF */
    CHECK(moq_session_subscribe(c, &sc, ph_now(p), &sub) == MOQ_OK);
    CHECK(sub._opaque != 0);
    CHECK(moq_handle_generation(sub._opaque) == 0xFFFFFFFu);
    CHECK(ph_run(p, 400));
    pair_drain_events(p);
    (void)ph_pair_destroy(p);
}

/* The FETCH teardown under drain pressure (drafts 18 and 22): with no
 * drain slot the STOP is refused and nothing of the FETCH changes; after
 * relief the same STOP commits it exactly once. */
static void fetch_stop_drain_pressure(moq_version_t version)
{
    label = "fetch_stop_drain_pressure";
    rows++;
    ph_pair_t *p = ph_pair_create(version, NULL);
    REQUIRE(p != NULL);
    REQUIRE(ph_run(p, 400));
    pair_drain_events(p);
    moq_session_t *c = ph_session(p, 0), *sv = ph_session(p, 1);
    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    fc.track_name = MOQ_BYTES_LITERAL("past");
    fc.end_group = 0;
    fc.end_object = 9;
    moq_fetch_t cf, sf = {0};
    REQUIRE(moq_session_fetch(c, &fc, ph_now(p), &cf) == MOQ_OK);
    REQUIRE(ph_run(p, 400));
    moq_event_t ev;
    for (int guard = 0; guard < 64; guard++) {
        if (moq_session_poll_events(sv, &ev, 1) == 0) break;
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST) sf = ev.u.fetch_request.fetch;
        moq_event_cleanup(&ev);
    }
    REQUIRE(sf._opaque != 0);
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0;
    ac.end_object = 9;
    REQUIRE(moq_session_accept_fetch(sv, sf, &ac, ph_now(p)) == MOQ_OK);
    REQUIRE(ph_run(p, 400));
    pair_drain_events(p);
    moq_stream_ref_t ref;
    REQUIRE(ph_only_open_local_data(p, 1, &ref));
    /* The drain is needed only while the request's FIN is unseen. */
    const moq_fetch_entry_t *fe = NULL;
    for (size_t i = 0; i < sv->fetch_cap; i++)
        if (sv->fetches[i].state == MOQ_FETCH_ACCEPTED &&
            sv->fetches[i].data_stream_ref._v == ref._v) fe = &sv->fetches[i];
    REQUIRE(fe != NULL);
    REQUIRE(!fetch_peer_fin_observed(fe));

    const size_t saved = sv->drain_ref_count;
    sv->drain_ref_count = sv->drain_ref_cap;
    const size_t queued = sv->action_tail - sv->action_head;
    CHECK(moq_session_on_data_stop(sv, ref, 0x2A, ph_now(p)) == MOQ_ERR_WOULD_BLOCK);
    CHECK(bridge_session_is_outgoing_fetch(sv, ref));
    CHECK(sv->action_tail - sv->action_head == queued);
    CHECK(sv->drain_ref_count == sv->drain_ref_cap);
    sv->drain_ref_count = saved;
    CHECK(moq_session_on_data_stop(sv, ref, 0x2A, ph_now(p)) == MOQ_OK);
    CHECK(!bridge_session_is_outgoing_fetch(sv, ref));
    CHECK(sv->drain_ref_count == saved + 1);
    CHECK(moq_session_on_data_stop(sv, ref, 0x2B, ph_now(p)) == MOQ_OK);
    unsigned cancelled = 0;
    for (int guard = 0; guard < 64; guard++) {
        if (moq_session_poll_events(sv, &ev, 1) == 0) break;
        if (ev.kind == MOQ_EVENT_FETCH_CANCELLED) cancelled++;
        moq_event_cleanup(&ev);
    }
    CHECK(cancelled == 1);
    CHECK(ph_run(p, 400));
    pair_drain_events(p);
    (void)ph_pair_destroy(p);
}

/* MOQ_HOST_ROW=<row function>:<draft> runs that one row alone, in that
 * draft (a focused run of one case); unset, every row runs. */
static const char *only_row;
static bool row_matched;
static bool row_wanted(const char *fn, unsigned draft)
{
    if (!only_row) return true;
    char want[80];
    (void)snprintf(want, sizeof(want), "%s:%u", fn, draft);
    if (strcmp(want, only_row) != 0) return false;
    row_matched = true;
    return true;
}

int main(void)
{
    only_row = getenv("MOQ_HOST_ROW");
    static const moq_version_t all[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22,
    };
    /* The rows before the focused ones run only in a full run. */
    if (!only_row) {
        for (size_t i = 0; i < sizeof(all) / sizeof(all[0]); i++) {
            data_stopped_entry(all[i]);
            data_stopped_record(all[i]);
            data_inactive(all[i]);
            retire_borrows(all[i]);
            fill_child_reclaimed(all[i]);
            for (int owner = 0; owner < 2; owner++) {
                done_waits_for_retired(all[i], owner == 1, false);
                done_waits_for_retired(all[i], owner == 1, true);
            }
        }
        bidi_abandoned_request(MOQ_VERSION_DRAFT_18);
        bidi_abandoned_request(MOQ_VERSION_DRAFT_22);
        retire_unbound();
    }
    for (size_t i = 0; i < sizeof(all) / sizeof(all[0]); i++) {
        if (row_wanted("local_ref_limit", (unsigned)all[i])) local_ref_limit(all[i]);
        if (row_wanted("owner_generation_bound", (unsigned)all[i]))
            owner_generation_bound(all[i]);
        if (all[i] != MOQ_VERSION_DRAFT_16 &&
            row_wanted("fetch_stop_drain_pressure", (unsigned)all[i]))
            fetch_stop_drain_pressure(all[i]);
    }

    if (only_row && !row_matched) {
        fprintf(stderr, "SETUP[selector] MOQ_HOST_ROW=%s names no row\n", only_row);
        return 2;
    }
    if (failures) {
        fprintf(stderr, "%u of %u checks failed\n", failures, checks);
        return 1;
    }
    printf("PASS: host_retire (%u rows, %u checks)\n", rows, checks);
    return 0;
}
