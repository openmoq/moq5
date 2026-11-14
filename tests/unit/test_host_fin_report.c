/* The tagged host's FIN-acceptance report, moq_session_report_data_fin().
 *
 * The public-only reference host (public_host.h) reports every FIN of a
 * local data stream the transport accepted. While output of a closed
 * subgroup with a delivery deadline may still be held by the host, the
 * session keeps that subgroup, so a held expiry or a STOP still commits its
 * one MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED with the exact identity; the
 * accepted FIN ends that hold. Each row that holds output drives an advancing
 * call while it is held, so the subgroup is kept by that hold and not by an
 * absence of sweeps. The rows here:
 *  - the result order: INVAL (NULL, ref 0, a peer ref), WRONG_STATE outside
 *    the tagged mode, CLOSED, WRONG_STATE before the FIN of a live subgroup
 *    is handed out, OK otherwise (absent identity, duplicate or late report),
 *    with nothing advanced, released or queued;
 *  - a report that a reset already made obsolete -- queued or claimed before
 *    the FIN was handed out, a peer STOP's reset claimed, a recognized
 *    terminal still waiting for event capacity, before or after the FIN was
 *    handed out: OK, and nothing the session or the host can observe moves;
 *    the reset then settles exactly once;
 *  - a pool of one: a stale or duplicate report of a reclaimed stream cannot
 *    touch the subgroup that took its slot;
 *  - held object and FIN past the object's deadline, on a subscription and
 *    on a publication: one correlated DELIVERY_TIMEOUT terminal and reset
 *    through the reset claim, no payload or FIN after credit returns;
 *  - healthy hand-off (no terminal), a FIN on the last bytes, and a refused
 *    FIN-only write (no timer for the FIN, released once accepted);
 *  - a STOP before the expiry; while held, the owner's cancellation (an
 *    unsubscribe, a cancelled publication, a rejected subscription or
 *    publication update) and the session's close; a normal DONE that keeps
 *    the identity; and a negotiated subgroup timer that the report leaves
 *    running.
 * The private session state is read by this test only, never by the host.
 */
#include "../public_host/public_host.h"
#include "../../core/src/session/session_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static unsigned failures, checks;
static char label[96];
#define CHECK(x) do { ++checks; if (!(x)) { ++failures; \
    fprintf(stderr, "FAIL[%s] %s:%d: %s\n", label, __FILE__, __LINE__, #x); \
} } while (0)
#define REQUIRE(x) do { if (!(x)) { \
    fprintf(stderr, "SETUP[%s] %s:%d: %s\n", label, __FILE__, __LINE__, #x); \
    exit(2); \
} } while (0)

#define CLIENT 0
#define SERVER 1
#define PASSES 400
#define TIMEOUT_MS 50

static void name_case(const char *what, moq_version_t version)
{
    (void)snprintf(label, sizeof(label), "%s.d%u", what,
                   version == MOQ_VERSION_DRAFT_16 ? 16u
                   : version == MOQ_VERSION_DRAFT_18 ? 18u : 22u);
}

/* -- events --------------------------------------------------------------- */

typedef struct tally {
    unsigned terminals, objects, sg_resets, others;
    moq_outgoing_subgroup_terminated_event_t terminal;
    uint64_t sg_reset_code;
} tally_t;

static tally_t drain(ph_pair_t *p, int side)
{
    tally_t t;
    memset(&t, 0, sizeof(t));
    moq_event_t e;
    while (moq_session_poll_events(ph_session(p, side), &e, 1) == 1) {
        if (e.kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED) {
            ++t.terminals;
            t.terminal = e.u.outgoing_subgroup_terminated;
        } else if (e.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            ++t.objects;
        } else if (e.kind == MOQ_EVENT_SUBGROUP_RESET) {
            ++t.sg_resets;
            t.sg_reset_code = e.u.subgroup_reset.error_code;
        } else {
            ++t.others;
        }
        moq_event_cleanup(&e);
    }
    return t;
}

/* -- pair, subscription, publication ------------------------------------- */

static ph_pair_t *open_pair_with(moq_version_t v, const ph_options_t *o)
{
    ph_pair_t *p = ph_pair_create(v, o);
    REQUIRE(p != NULL);
    REQUIRE(ph_run(p, PASSES));
    REQUIRE(moq_session_state(ph_session(p, CLIENT)) == MOQ_SESS_ESTABLISHED);
    REQUIRE(moq_session_state(ph_session(p, SERVER)) == MOQ_SESS_ESTABLISHED);
    (void)drain(p, CLIENT);
    (void)drain(p, SERVER);
    return p;
}

static ph_pair_t *open_pair_limited(moq_version_t v, int tagged_side, size_t write_limit)
{
    ph_options_t o;
    memset(&o, 0, sizeof(o));
    o.tagged[tagged_side] = true;
    o.write_limit = write_limit;
    return open_pair_with(v, &o);
}

static ph_pair_t *open_pair(moq_version_t v, int tagged_side)
{
    return open_pair_limited(v, tagged_side, 0);
}

static void close_pair(ph_pair_t *p)
{
    if (moq_session_state(ph_session(p, CLIENT)) != MOQ_SESS_CLOSED)
        CHECK(moq_session_close(ph_session(p, CLIENT), 0, NULL, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    (void)drain(p, CLIENT);
    (void)drain(p, SERVER);
    CHECK(ph_fault(p, CLIENT) == PH_FAULT_NONE && ph_fault(p, SERVER) == PH_FAULT_NONE);
    CHECK(ph_pair_destroy(p) == 0);
}

typedef struct owner {
    int side;                     /* the side that publishes */
    moq_subscription_t ssub, csub;
    moq_publication_t pub, peer;
} owner_t;

/* The publisher's track properties with these timeouts (0 = none). */
static size_t timeouts(moq_session_t *s, uint64_t object_ms, uint64_t subgroup_ms,
                       uint8_t *out, size_t cap)
{
    if (!object_ms && !subgroup_ms) return 0;
    moq_track_delivery_timeouts_cfg_t t;
    moq_track_delivery_timeouts_cfg_init(&t);
    t.has_object_delivery_timeout = object_ms != 0;
    t.object_delivery_timeout_ms = object_ms;
    t.has_subgroup_delivery_timeout = subgroup_ms != 0;
    t.subgroup_delivery_timeout_ms = subgroup_ms;
    size_t len = 0;
    REQUIRE(moq_session_track_properties_add_delivery_timeouts(s, NULL, 0, &t, out, cap,
                                                               &len) == MOQ_OK);
    return len;
}

/* The server publishes to the client's subscription. */
static void subscribe(ph_pair_t *p, owner_t *o, uint64_t object_ms, uint64_t subgroup_ms)
{
    memset(o, 0, sizeof(*o));
    o->side = SERVER;
    moq_session_t *c = ph_session(p, CLIENT), *s = ph_session(p, SERVER);
    moq_bytes_t part = MOQ_BYTES_LITERAL("fin");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init_sized(&sc, sizeof(sc));
    sc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    sc.track_name = MOQ_BYTES_LITERAL("t");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    REQUIRE(moq_session_subscribe(c, &sc, ph_now(p), &o->csub) == MOQ_OK);
    REQUIRE(ph_run(p, PASSES));
    moq_event_t e;
    REQUIRE(moq_session_poll_events(s, &e, 1) == 1 && e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    o->ssub = e.u.subscribe_request.sub;
    moq_event_cleanup(&e);
    uint8_t props[32];
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.track_properties = (moq_bytes_t){props, timeouts(s, object_ms, subgroup_ms,
                                                        props, sizeof(props))};
    REQUIRE(moq_session_accept_subscribe(s, o->ssub, &ac, ph_now(p)) == MOQ_OK);
    REQUIRE(ph_run(p, PASSES));
    (void)drain(p, CLIENT);
    (void)drain(p, SERVER);
}

/* The client publishes; the server accepts. */
static void publish(ph_pair_t *p, owner_t *o, uint64_t object_ms)
{
    memset(o, 0, sizeof(*o));
    o->side = CLIENT;
    moq_session_t *c = ph_session(p, CLIENT), *s = ph_session(p, SERVER);
    moq_bytes_t part = MOQ_BYTES_LITERAL("fin");
    uint8_t props[32];
    moq_publish_cfg_t pc;
    moq_publish_cfg_init_sized(&pc, sizeof(pc));
    pc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    pc.track_name = MOQ_BYTES_LITERAL("p");
    pc.track_properties = (moq_bytes_t){props, timeouts(c, object_ms, 0, props, sizeof(props))};
    REQUIRE(moq_session_publish(c, &pc, ph_now(p), &o->pub) == MOQ_OK);
    REQUIRE(ph_run(p, PASSES));
    moq_event_t e;
    REQUIRE(moq_session_poll_events(s, &e, 1) == 1 && e.kind == MOQ_EVENT_PUBLISH_REQUEST);
    o->peer = e.u.publish_request.pub;
    moq_event_cleanup(&e);
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
    REQUIRE(moq_session_accept_publish(s, o->peer, &ac, ph_now(p)) == MOQ_OK);
    REQUIRE(ph_run(p, PASSES));
    (void)drain(p, CLIENT);
    (void)drain(p, SERVER);
}

static moq_session_t *pub_session(ph_pair_t *p, const owner_t *o)
{
    return ph_session(p, o->side);
}

static moq_subgroup_handle_t open_group(ph_pair_t *p, const owner_t *o, uint64_t group)
{
    moq_subgroup_cfg_t c;
    moq_subgroup_cfg_init(&c);
    c.group_id = group;
    moq_subgroup_handle_t sg;
    moq_result_t rc = o->side == CLIENT
        ? moq_session_open_pub_subgroup(pub_session(p, o), o->pub, &c, ph_now(p), &sg)
        : moq_session_open_subgroup(pub_session(p, o), o->ssub, &c, ph_now(p), &sg);
    REQUIRE(rc == MOQ_OK);
    return sg;
}

static void write_object(ph_pair_t *p, const owner_t *o, moq_subgroup_handle_t sg,
                         uint64_t object, const char *text)
{
    moq_rcbuf_t *b = NULL;
    REQUIRE(moq_rcbuf_create(ph_allocator(p), (const uint8_t *)text, strlen(text), &b) == MOQ_OK);
    REQUIRE(moq_session_write_object(pub_session(p, o), sg, object, b, ph_now(p)) == MOQ_OK);
    moq_rcbuf_decref(b);
}

/* The stream the publishing side opened last. */
static moq_stream_ref_t last_ref(ph_pair_t *p, const owner_t *o)
{
    moq_stream_ref_t refs[16];
    size_t n = ph_local_data_refs(p, o->side, refs, 16);
    REQUIRE(n > 0 && n <= 16);
    return refs[n - 1];
}

static size_t occupied(moq_session_t *s)
{
    size_t n = 0;
    for (size_t i = 0; i < s->sg_cap; ++i)
        if (s->subgroups[i].state != MOQ_SG_FREE) ++n;
    return n;
}

/* An advancing call with nothing due. Polling is not one, and with no input
 * and no timer due the host makes none while its output waits: without this
 * the pool would not be swept between the hand-off and the next event, and a
 * row could not tell a kept subgroup from an unswept one. */
static bool sweep(ph_pair_t *p, moq_session_t *s)
{
    return moq_session_tick(s, ph_now(p)) == MOQ_OK;
}

static void check_terminal(const tally_t *t, moq_subgroup_handle_t sg, const owner_t *o,
                           moq_stream_ref_t ref, uint64_t group, uint32_t cause, uint64_t code)
{
    CHECK(t->terminals == 1);
    if (t->terminals != 1) return;
    CHECK(t->terminal.subgroup._opaque == sg._opaque);
    if (o->side == SERVER) CHECK(moq_subscription_eq(t->terminal.sub, o->ssub));
    else CHECK(moq_publication_eq(t->terminal.pub, o->pub));
    CHECK(t->terminal.stream_ref._v == ref._v);
    CHECK(t->terminal.group_id == group);
    CHECK(t->terminal.cause == cause);
    CHECK(t->terminal.error_code == code);
}

/* -- the result order ----------------------------------------------------- */

/* The session state a report must leave alone. */
typedef struct snap {
    uint64_t epoch, now;
    size_t action_tail, event_tail, occupied;
    uint64_t deadline;
    moq_session_state_t state;
} snap_t;

static snap_t snap_of(moq_session_t *s)
{
    snap_t x = {.epoch = s->borrow_epoch, .now = s->last_now_us,
                .action_tail = s->action_tail, .event_tail = s->event_tail,
                .occupied = occupied(s), .deadline = moq_session_next_deadline_us(s),
                .state = moq_session_state(s)};
    return x;
}

static bool unchanged(moq_session_t *s, const snap_t *x)
{
    snap_t y = snap_of(s);
    return y.epoch == x->epoch && y.now == x->now && y.action_tail == x->action_tail &&
           y.event_tail == x->event_tail && y.occupied == x->occupied &&
           y.deadline == x->deadline && y.state == x->state;
}

/* Report and check that nothing else moved. */
static moq_result_t report(moq_session_t *s, moq_stream_ref_t ref)
{
    snap_t x = snap_of(s);
    moq_result_t rc = moq_session_report_data_fin(s, ref);
    CHECK(unchanged(s, &x));
    return rc;
}

/* One subgroup's bookkeeping: state, generation, first cause and code, the
 * owed reset's code, the claim, the STOP fact, custody, the FIN hand-out,
 * the reliable prefix and the owner; and the stream's queued reset (count,
 * code, reliable size, claim). */
typedef struct sg_snap {
    int slot;
    moq_sg_state_t state;
    uint32_t generation;
    uint64_t cause, code, owed_code;
    bool claimed, stopped, custody, fin;
    uint8_t header_len;
    moq_subscription_t sub;
    moq_publication_t pub;
    unsigned queued_resets;
    uint64_t q_code, q_reliable;
    bool q_claimed;
} sg_snap_t;

static sg_snap_t sg_snap_of(moq_session_t *s, moq_stream_ref_t ref)
{
    sg_snap_t x;
    memset(&x, 0, sizeof(x));
    x.slot = sg_find_by_stream_ref(s, ref);
    if (x.slot < 0) return x;
    const moq_sg_entry_t *e = &s->subgroups[x.slot];
    x.state = e->state;
    x.generation = e->generation;
    x.cause = e->terminal_cause;
    x.code = e->terminal_code;
    x.owed_code = e->owed_reset_code;
    x.claimed = e->reset_claimed;
    x.stopped = e->peer_stopped;
    x.custody = e->timed_custody;
    x.fin = e->fin_handed;
    x.header_len = e->header_len;
    x.sub = e->sub;
    x.pub = e->pub;
    for (size_t j = s->action_head; j < s->action_tail; ++j) {
        const moq_action_t *a = &s->actions[j % s->action_cap];
        if (a->kind != MOQ_ACTION_RESET_DATA || a->u.reset_data.stream_ref._v != ref._v)
            continue;
        if (x.queued_resets++ == 0) {
            x.q_code = a->u.reset_data.error_code;
            x.q_reliable = a->u.reset_data.reliable_size;
            x.q_claimed = s->action_meta[j % s->action_cap].reset_claimed;
        }
    }
    return x;
}

static bool sg_same(const sg_snap_t *a, const sg_snap_t *b)
{
    return a->slot == b->slot && a->state == b->state && a->generation == b->generation &&
           a->cause == b->cause && a->code == b->code && a->owed_code == b->owed_code &&
           a->claimed == b->claimed && a->stopped == b->stopped && a->custody == b->custody &&
           a->fin == b->fin && a->header_len == b->header_len &&
           a->sub._opaque == b->sub._opaque && a->pub._opaque == b->pub._opaque &&
           a->queued_resets == b->queued_resets && a->q_code == b->q_code &&
           a->q_reliable == b->q_reliable && a->q_claimed == b->q_claimed;
}

/* What the transport took on the side's data stream `ref`. */
static ph_stream_trace_t trace_of(ph_pair_t *p, int side, moq_stream_ref_t ref)
{
    ph_stream_trace_t tr;
    memset(&tr, 0, sizeof(tr));
    REQUIRE(ph_data_trace(p, side, ref, &tr));
    return tr;
}

/* What the host sees of the next reset (moq_session_offer_data_reset is an
 * observing call). */
typedef struct offer_obs {
    moq_result_t rc;
    size_t count;
    moq_data_reset_offer_t off;
} offer_obs_t;

static offer_obs_t offer_of(moq_session_t *s)
{
    offer_obs_t x;
    memset(&x, 0, sizeof(x));
    x.rc = moq_session_offer_data_reset(s, &x.off, sizeof(x.off), &x.count);
    return x;
}

static bool offer_same(const offer_obs_t *a, const offer_obs_t *b)
{
    if (a->rc != b->rc || a->count != b->count) return false;
    if (a->count == 0) return true;
    return a->off.state == b->off.state && a->off.stream_ref._v == b->off.stream_ref._v &&
           a->off.error_code == b->off.error_code &&
           a->off.reliable_size == b->off.reliable_size &&
           a->off.owner._v == b->off.owner._v && a->off.cause == b->off.cause;
}

/* A report a reset already made obsolete: OK, and nothing moves -- the
 * session (queues, epoch, time, pool, deadline), the subgroup's bookkeeping
 * and the reset the host would be offered. */
static void check_obsolete(ph_pair_t *p, int side, moq_stream_ref_t ref)
{
    moq_session_t *s = ph_session(p, side);
    const ph_counters_t *n = ph_counters(p, side);
    const unsigned claims = n->claims, commits = n->commits;
    const size_t head = s->action_head, ev_head = s->event_head;
    snap_t x = snap_of(s);
    sg_snap_t e = sg_snap_of(s, ref);
    offer_obs_t o = offer_of(s);
    CHECK(moq_session_report_data_fin(s, ref) == MOQ_OK);
    CHECK(unchanged(s, &x));
    CHECK(s->action_head == head && s->event_head == ev_head);
    sg_snap_t e2 = sg_snap_of(s, ref);
    CHECK(sg_same(&e, &e2));
    offer_obs_t o2 = offer_of(s);
    CHECK(offer_same(&o, &o2));
    CHECK(n->claims == claims && n->commits == commits);
}

/* A SUBSCRIBE the server leaves unanswered: its request event takes one
 * place in the server's event queue until polled. */
static void request_unpolled(ph_pair_t *p, const char *name)
{
    moq_bytes_t part = MOQ_BYTES_LITERAL("other");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init_sized(&sc, sizeof(sc));
    sc.track_namespace = (moq_namespace_t){.parts = &part, .count = 1};
    sc.track_name = (moq_bytes_t){(const uint8_t *)name, strlen(name)};
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sub;
    REQUIRE(moq_session_subscribe(ph_session(p, CLIENT), &sc, ph_now(p), &sub) == MOQ_OK);
}

static void result_order(moq_version_t v)
{
    name_case("fin_report.results", v);
    ph_pair_t *p = open_pair(v, SERVER);
    moq_session_t *srv = ph_session(p, SERVER), *cli = ph_session(p, CLIENT);
    const ph_counters_t *sn = ph_counters(p, SERVER);
    owner_t o;
    subscribe(p, &o, TIMEOUT_MS, 0);

    CHECK(moq_session_report_data_fin(NULL, moq_stream_ref_from_u64(1)) == MOQ_ERR_INVAL);
    CHECK(report(srv, moq_stream_ref_from_u64(0)) == MOQ_ERR_INVAL);
    CHECK(report(srv, moq_stream_ref_from_u64(MOQ_STREAM_REF_LOCAL_END)) == MOQ_ERR_INVAL);
    CHECK(report(srv, moq_stream_ref_from_u64(UINT64_MAX)) == MOQ_ERR_INVAL);
    CHECK(report(cli, moq_stream_ref_from_u64(1)) == MOQ_ERR_WRONG_STATE);   /* raw mode */

    moq_subgroup_handle_t sg = open_group(p, &o, 0);
    moq_rcbuf_t *held = NULL;
    REQUIRE(moq_rcbuf_create(ph_allocator(p), (const uint8_t *)"first", 5, &held) == MOQ_OK);
    REQUIRE(moq_session_write_object(srv, sg, 0, held, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t ref = last_ref(p, &o);
    /* Open: its FIN was never handed out. */
    CHECK(report(srv, ref) == MOQ_ERR_WRONG_STATE);
    REQUIRE(moq_session_close_subgroup(srv, sg, ph_now(p)) == MOQ_OK);
    /* Queued, not handed out: still refused; the caller's buffer untouched. */
    const uint32_t refs = moq_rcbuf_refcount(held);
    CHECK(report(srv, ref) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_rcbuf_refcount(held) == refs);
    moq_rcbuf_decref(held);
    const unsigned reported = sn->fins_reported;
    CHECK(ph_run(p, PASSES));
    CHECK(sn->fins_reported == reported + 1);   /* the host's own report */
    /* A duplicate or late report, and one for a ref the session no longer
     * holds, change nothing. */
    CHECK(report(srv, ref) == MOQ_OK);
    CHECK(report(srv, moq_stream_ref_from_u64(ref._v + 1000)) == MOQ_OK);
    tally_t s = drain(p, SERVER);
    CHECK(s.terminals == 0);
    /* Closed: refused after the mode check, input validation first. */
    REQUIRE(moq_session_close(srv, 0, NULL, ph_now(p)) == MOQ_OK);
    CHECK(report(srv, ref) == MOQ_ERR_CLOSED);
    CHECK(report(srv, moq_stream_ref_from_u64(0)) == MOQ_ERR_INVAL);
    CHECK(ph_run(p, PASSES));
    (void)drain(p, CLIENT);
    (void)drain(p, SERVER);
    CHECK(ph_pair_destroy(p) == 0);
}

/* The report is non-advancing even where it ends a hold: the subgroup's
 * slot is reclaimed only by the next advancing call. */
static void report_does_not_reclaim(moq_version_t v)
{
    name_case("fin_report.no_immediate_reclaim", v);
    ph_pair_t *p = open_pair(v, SERVER);
    moq_session_t *srv = ph_session(p, SERVER);
    owner_t o;
    subscribe(p, &o, TIMEOUT_MS, 0);
    moq_subgroup_handle_t sg = open_group(p, &o, 0);
    write_object(p, &o, sg, 0, "x");
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t ref = last_ref(p, &o);
    REQUIRE(ph_refuse_data_stream(p, SERVER, ref));
    write_object(p, &o, sg, 1, "held");
    REQUIRE(moq_session_close_subgroup(srv, sg, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    CHECK(sweep(p, srv));
    CHECK(occupied(srv) == 1);                  /* the held output keeps it */
    /* The test, standing in for an accepted FIN, ends the hold directly. */
    CHECK(report(srv, ref) == MOQ_OK);
    CHECK(occupied(srv) == 1);                  /* not reclaimed yet */
    CHECK(moq_session_tick(srv, ph_now(p)) == MOQ_OK);
    CHECK(occupied(srv) == 0);                  /* the advancing call reaps */
    ph_lift_data_refusal(p, SERVER);
    CHECK(ph_run(p, PASSES));
    close_pair(p);
}

/* -- held output past its deadline ----------------------------------------- */

static void held_expiry(moq_version_t v, bool publication)
{
    name_case(publication ? "fin_report.held_expiry.publication"
                          : "fin_report.held_expiry.subscription", v);
    ph_pair_t *p = open_pair(v, publication ? CLIENT : SERVER);
    owner_t o;
    if (publication) publish(p, &o, TIMEOUT_MS);
    else subscribe(p, &o, TIMEOUT_MS, 0);
    moq_session_t *s = pub_session(p, &o);
    const ph_counters_t *n = ph_counters(p, o.side);
    moq_subgroup_handle_t sg = open_group(p, &o, 3);
    write_object(p, &o, sg, 0, "in time");
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t ref = last_ref(p, &o);
    REQUIRE(ph_refuse_data_stream(p, o.side, ref));
    write_object(p, &o, sg, 1, "too late");
    REQUIRE(moq_session_close_subgroup(s, sg, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    CHECK(n->fins_reported == 0);
    uint64_t due = ph_next_held_deadline(p, o.side);
    REQUIRE(due != UINT64_MAX && due > ph_now(p));
    CHECK(sweep(p, s));
    CHECK(occupied(s) == 1);                   /* the held output keeps it */
    const unsigned resets0 = ph_transport_resets(p, o.side, NULL);
    CHECK(ph_wake(p, due - 1, PASSES));
    CHECK(n->held_expired == 0);
    CHECK(ph_wake(p, due, PASSES));
    CHECK(n->held_expired == 1);
    CHECK(n->host_resets == 0);                /* the session owns the reset */
    CHECK(n->claims == 1 && n->commits == 1);
    CHECK(ph_transport_resets(p, o.side, NULL) == resets0 + 1);
    CHECK(ph_last_reset_code(p, o.side) == MOQ_RESET_DELIVERY_TIMEOUT);
    tally_t t = drain(p, o.side);
    check_terminal(&t, sg, &o, ref, 3, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT,
                   MOQ_RESET_DELIVERY_TIMEOUT);
    ph_lift_data_refusal(p, o.side);
    CHECK(ph_run(p, PASSES));
    CHECK(n->fins_reported == 0);              /* the FIN never went out */
    tally_t peer = drain(p, 1 - o.side);
    CHECK(peer.objects == 1);                  /* only the object in time */
    CHECK(peer.sg_resets == 1 && peer.sg_reset_code == MOQ_RESET_DELIVERY_TIMEOUT);
    CHECK(moq_session_tick(s, ph_now(p)) == MOQ_OK);
    CHECK(occupied(s) == 0);
    close_pair(p);
}

/* -- hand-off: healthy, on the last bytes, refused FIN-only ------------------ */

static void handoff(moq_version_t v, bool refuse_fin)
{
    name_case(refuse_fin ? "fin_report.refused_fin_only" : "fin_report.handoff", v);
    ph_pair_t *p = open_pair(v, SERVER);
    owner_t o;
    subscribe(p, &o, TIMEOUT_MS, 0);
    moq_session_t *s = ph_session(p, SERVER);
    const ph_counters_t *n = ph_counters(p, SERVER);
    moq_subgroup_handle_t sg = open_group(p, &o, 0);
    write_object(p, &o, sg, 0, "a");
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t ref = last_ref(p, &o);
    if (refuse_fin) {
        /* Everything written so far plus the next object is taken; the FIN
         * after it is refused. */
        REQUIRE(ph_refuse_data_stream(p, SERVER, ref));
        ph_set_refusal_allowance(p, SERVER, UINT64_MAX);
    }
    write_object(p, &o, sg, 1, "b");
    REQUIRE(moq_session_close_subgroup(s, sg, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    if (refuse_fin) {
        CHECK(n->fins_reported == 0);
        CHECK(sweep(p, s));
        CHECK(occupied(s) == 1);               /* the conservative tail */
        /* No timer for the FIN: long past the object's deadline nothing
         * expires and nothing is reset. */
        CHECK(ph_wake(p, ph_now(p) + 10 * TIMEOUT_MS * 1000, PASSES));
        CHECK(n->held_expired == 0 && n->claims == 0 && n->host_resets == 0);
        CHECK(drain(p, SERVER).terminals == 0);
        CHECK(sweep(p, s));
        CHECK(occupied(s) == 1);
        ph_lift_data_refusal(p, SERVER);
        CHECK(ph_run(p, PASSES));
    }
    CHECK(n->fins_reported == 1);
    CHECK(drain(p, SERVER).terminals == 0);
    tally_t peer = drain(p, CLIENT);
    CHECK(peer.objects == 2 && peer.sg_resets == 0);
    CHECK(moq_session_tick(s, ph_now(p)) == MOQ_OK);
    CHECK(occupied(s) == 0);
    close_pair(p);
}

/* The transport takes the object in pieces: a partial write is not
 * acceptance, and the FIN report follows only the last byte and the FIN. */
static void partial_writes(moq_version_t v)
{
    name_case("fin_report.partial_writes", v);
    ph_pair_t *p = open_pair_limited(v, SERVER, 2);
    owner_t o;
    subscribe(p, &o, TIMEOUT_MS, 0);
    moq_session_t *s = ph_session(p, SERVER);
    const ph_counters_t *n = ph_counters(p, SERVER);
    moq_subgroup_handle_t sg = open_group(p, &o, 0);
    write_object(p, &o, sg, 0, "several bytes");
    REQUIRE(moq_session_close_subgroup(s, sg, ph_now(p)) == MOQ_OK);
    /* One pass hands the object and FIN out and writes only the first
     * bytes: the rest is still the host's. */
    (void)ph_run(p, 1);
    CHECK(n->partial_writes > 0);
    CHECK(n->fins_reported == 0);
    CHECK(sweep(p, s));
    CHECK(occupied(s) == 1);
    CHECK(ph_run(p, PASSES));
    CHECK(n->fins_reported == 1);
    CHECK(drain(p, SERVER).terminals == 0);
    tally_t peer = drain(p, CLIENT);
    CHECK(peer.objects == 1 && peer.sg_resets == 0);
    CHECK(moq_session_tick(s, ph_now(p)) == MOQ_OK);
    CHECK(occupied(s) == 0);
    close_pair(p);
}

/* -- STOP first; cancellation; close; subgroup timer ----------------------- */

static void stop_then_expiry(moq_version_t v)
{
    name_case("fin_report.stop_then_expiry", v);
    ph_pair_t *p = open_pair(v, SERVER);
    owner_t o;
    subscribe(p, &o, TIMEOUT_MS, 0);
    moq_session_t *s = ph_session(p, SERVER);
    const ph_counters_t *n = ph_counters(p, SERVER);
    moq_subgroup_handle_t sg = open_group(p, &o, 0);
    write_object(p, &o, sg, 0, "a");
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t ref = last_ref(p, &o);
    REQUIRE(ph_refuse_data_stream(p, SERVER, ref));
    write_object(p, &o, sg, 1, "held");
    REQUIRE(moq_session_close_subgroup(s, sg, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    REQUIRE(ph_peer_stops_data(p, SERVER, ref, 0x9));
    CHECK(ph_run(p, PASSES));
    tally_t t = drain(p, SERVER);
    check_terminal(&t, sg, &o, ref, 0, MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING, 0x9);
    /* A late FIN report and the expiry add nothing. */
    CHECK(moq_session_report_data_fin(s, ref) == MOQ_OK);
    CHECK(ph_wake(p, ph_now(p) + 10 * TIMEOUT_MS * 1000, PASSES));
    CHECK(drain(p, SERVER).terminals == 0);
    CHECK(n->held_expired == 0);
    ph_lift_data_refusal(p, SERVER);
    CHECK(ph_run(p, PASSES));
    CHECK(moq_session_tick(s, ph_now(p)) == MOQ_OK);
    CHECK(occupied(s) == 0);
    close_pair(p);
}

/* The newest update of `kind` whose answer `s` owes; other events are
 * dropped. UINT64_MAX when none is owed. */
static uint64_t owed_update(moq_session_t *s, moq_event_kind_t kind)
{
    uint64_t id = UINT64_MAX;
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) == 1) {
        if (e.kind == kind && kind == MOQ_EVENT_SUBSCRIBE_UPDATED &&
            e.u.subscribe_updated.answer_owed)
            id = e.u.subscribe_updated.update_request_id;
        if (e.kind == kind && kind == MOQ_EVENT_PUBLISH_UPDATED &&
            e.u.publish_updated.answer_owed)
            id = e.u.publish_updated.update_request_id;
        moq_event_cleanup(&e);
    }
    return id;
}

/* How the owner's request ends while its closed subgroup's output is held. */
typedef enum {
    END_UNSUBSCRIBED,        /* the subscriber unsubscribes */
    END_PUBLICATION_CANCELLED, /* the receiver of a PUBLISH cancels it */
    END_UPDATE_REJECTED,     /* the publisher rejects a subscription update */
    END_PUB_UPDATE_REJECTED  /* the publisher rejects its publication's update */
} end_kind_t;

/* The owner's cancellation settles the held subgroup: one CANCELLED reset
 * through the claim, no terminal, nothing held expires later, and the slot
 * is reclaimed. */
static void cancelled_while_held(moq_version_t v, end_kind_t k)
{
    static const char *const names[] = {
        "fin_report.cancelled_while_held", "fin_report.publication_cancelled_while_held",
        "fin_report.update_rejected_while_held",
        "fin_report.publication_update_rejected_while_held"};
    name_case(names[k], v);
    const bool publication = k == END_PUBLICATION_CANCELLED || k == END_PUB_UPDATE_REJECTED;
    ph_pair_t *p = open_pair(v, publication ? CLIENT : SERVER);
    owner_t o;
    if (publication) publish(p, &o, TIMEOUT_MS);
    else subscribe(p, &o, TIMEOUT_MS, 0);
    moq_session_t *s = pub_session(p, &o), *peer = ph_session(p, 1 - o.side);
    const ph_counters_t *n = ph_counters(p, o.side);
    if (k == END_UPDATE_REJECTED || k == END_PUB_UPDATE_REJECTED)
        REQUIRE(moq_session_set_defer_subscription_updates(s, true) == MOQ_OK);
    moq_subgroup_handle_t sg = open_group(p, &o, 0);
    write_object(p, &o, sg, 0, "a");
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t ref = last_ref(p, &o);
    REQUIRE(ph_refuse_data_stream(p, o.side, ref));
    write_object(p, &o, sg, 1, "held");
    REQUIRE(moq_session_close_subgroup(s, sg, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    CHECK(sweep(p, s));
    CHECK(occupied(s) == 1);
    moq_reject_subscription_update_cfg_t rj;
    moq_reject_subscription_update_cfg_init(&rj);
    rj.error_code = 0x5;
    /* The trace keeps a stream's first reset; a later one of an already
     * reset half is counted apart, for the whole side (its request stream
     * may legitimately reset once too, so the side's reset total is not
     * pinned). */
    unsigned repeats0 = 0, repeats = 0;
    (void)ph_transport_resets(p, o.side, &repeats0);
    switch (k) {
    case END_UNSUBSCRIBED:
        REQUIRE(moq_session_unsubscribe(peer, o.csub, ph_now(p)) == MOQ_OK);
        break;
    case END_PUBLICATION_CANCELLED:
        REQUIRE(moq_session_cancel_publication(peer, o.peer, 0, ph_now(p)) == MOQ_OK);
        break;
    case END_UPDATE_REJECTED: {
        moq_subscription_update_cfg_t up;
        moq_subscription_update_cfg_init_sized(&up, sizeof(up));
        up.has_subscriber_priority = true;
        up.subscriber_priority = 9;
        REQUIRE(moq_session_update_subscription(peer, o.csub, &up, ph_now(p)) == MOQ_OK);
        CHECK(ph_run(p, PASSES));
        rj.update_request_id = owed_update(s, MOQ_EVENT_SUBSCRIBE_UPDATED);
        REQUIRE(rj.update_request_id != UINT64_MAX);
        REQUIRE(moq_session_reject_subscription_update(s, o.ssub, &rj, ph_now(p)) == MOQ_OK);
        break;
    }
    case END_PUB_UPDATE_REJECTED: {
        moq_publication_update_cfg_t up;
        moq_publication_update_cfg_init(&up);
        up.has_subscriber_priority = true;
        up.subscriber_priority = 9;
        REQUIRE(moq_session_update_publication(peer, o.peer, &up, ph_now(p)) == MOQ_OK);
        CHECK(ph_run(p, PASSES));
        rj.update_request_id = owed_update(s, MOQ_EVENT_PUBLISH_UPDATED);
        REQUIRE(rj.update_request_id != UINT64_MAX);
        REQUIRE(moq_session_reject_publication_update(s, o.pub, &rj, ph_now(p)) == MOQ_OK);
        break;
    }
    }
    CHECK(ph_run(p, PASSES));
    ph_stream_trace_t tr = trace_of(p, o.side, ref);
    CHECK(tr.reset != 0 && tr.reset_code == 0x1);   /* CANCELLED, through the claim */
    (void)ph_transport_resets(p, o.side, &repeats);
    CHECK(repeats == repeats0);                      /* and only once */
    CHECK(ph_wake(p, ph_now(p) + 10 * TIMEOUT_MS * 1000, PASSES));
    (void)ph_transport_resets(p, o.side, &repeats);
    CHECK(repeats == repeats0);
    CHECK(drain(p, o.side).terminals == 0);
    CHECK(n->held_expired == 0 && n->fins_reported == 0);
    ph_lift_data_refusal(p, o.side);
    CHECK(ph_run(p, PASSES));
    tr = trace_of(p, o.side, ref);
    CHECK(tr.fin == 0 && tr.reset_code == 0x1);     /* nothing held was written */
    (void)ph_transport_resets(p, o.side, &repeats);
    CHECK(repeats == repeats0);
    CHECK(moq_session_tick(s, ph_now(p)) == MOQ_OK);
    CHECK(occupied(s) == 0);
    close_pair(p);
}

/* A normal DONE ends no custody: the held output still expires, and its
 * terminal still carries the subgroup's identity. */
static void done_keeps_identity(moq_version_t v, bool publication)
{
    name_case(publication ? "fin_report.publication_done_keeps_identity"
                          : "fin_report.done_keeps_identity", v);
    ph_pair_t *p = open_pair(v, publication ? CLIENT : SERVER);
    owner_t o;
    if (publication) publish(p, &o, TIMEOUT_MS);
    else subscribe(p, &o, TIMEOUT_MS, 0);
    moq_session_t *s = pub_session(p, &o);
    const ph_counters_t *n = ph_counters(p, o.side);
    moq_subgroup_handle_t sg = open_group(p, &o, 0);
    write_object(p, &o, sg, 0, "a");
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t ref = last_ref(p, &o);
    REQUIRE(ph_refuse_data_stream(p, o.side, ref));
    write_object(p, &o, sg, 1, "held");
    REQUIRE(moq_session_close_subgroup(s, sg, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    if (publication) {
        moq_finish_publish_cfg_t fc;
        moq_finish_publish_cfg_init(&fc);
        REQUIRE(moq_session_finish_publish(s, o.pub, &fc, ph_now(p)) == MOQ_OK);
    } else {
        moq_done_subscribe_cfg_t dc;
        moq_done_subscribe_cfg_init(&dc);
        dc.status_code = 0x2;
        dc.stream_count = 1;
        REQUIRE(moq_session_done_subscribe(s, o.ssub, &dc, ph_now(p)) == MOQ_OK);
    }
    CHECK(ph_run(p, PASSES));
    CHECK(sweep(p, s));
    CHECK(occupied(s) == 1);
    uint64_t due = ph_next_held_deadline(p, o.side);
    REQUIRE(due != UINT64_MAX && due > ph_now(p));
    CHECK(ph_wake(p, due, PASSES));
    CHECK(n->held_expired == 1);
    tally_t t = drain(p, o.side);
    check_terminal(&t, sg, &o, ref, 0, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT,
                   MOQ_RESET_DELIVERY_TIMEOUT);
    ph_stream_trace_t tr = trace_of(p, o.side, ref);
    CHECK(tr.reset != 0 && tr.reset_code == MOQ_RESET_DELIVERY_TIMEOUT && tr.fin == 0);
    ph_lift_data_refusal(p, o.side);
    CHECK(ph_run(p, PASSES));
    CHECK(n->fins_reported == 0);
    CHECK(moq_session_tick(s, ph_now(p)) == MOQ_OK);
    CHECK(occupied(s) == 0);
    close_pair(p);
}

/* -- reports a reset made obsolete ------------------------------------------ */

/* The owner resets the subgroup after its first object, before any FIN is
 * queued: queued, or claimed by the host while the transport refuses the
 * reset. A FIN report then is obsolete. The reset settles once. */
static void reset_before_fin(moq_version_t v, bool claimed)
{
    name_case(claimed ? "fin_report.claimed_reset_before_fin"
                      : "fin_report.queued_reset_before_fin", v);
    ph_pair_t *p = open_pair(v, SERVER);
    owner_t o;
    subscribe(p, &o, TIMEOUT_MS, 0);
    moq_session_t *s = ph_session(p, SERVER);
    const ph_counters_t *n = ph_counters(p, SERVER);
    moq_subgroup_handle_t sg = open_group(p, &o, 0);
    write_object(p, &o, sg, 0, "a");
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t ref = last_ref(p, &o);
    const unsigned resets0 = ph_transport_resets(p, SERVER, NULL);
    const unsigned claims0 = n->claims, commits0 = n->commits;
    REQUIRE(moq_session_reset_subgroup(s, sg, 0x7, ph_now(p)) == MOQ_OK);
    if (claimed) {
        ph_refuse_resets(p, SERVER, 1000);
        CHECK(ph_run(p, PASSES));
        CHECK(n->claims == claims0 + 1 && n->commits == commits0);
    }
    sg_snap_t e = sg_snap_of(s, ref);
    CHECK(e.slot >= 0 && e.state == MOQ_SG_RESETTING && !e.fin);
    offer_obs_t r = offer_of(s);
    CHECK(r.rc == MOQ_OK && r.count == 1);
    CHECK(r.off.state == (claimed ? MOQ_DATA_RESET_CLAIMED : MOQ_DATA_RESET_READY));
    CHECK(r.off.stream_ref._v == ref._v && r.off.error_code == 0x7);
    CHECK(r.off.reliable_size == e.header_len && e.header_len > 0);
    CHECK(r.off.cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE);
    check_obsolete(p, SERVER, ref);
    check_obsolete(p, SERVER, ref);              /* a duplicate, just the same */
    if (claimed) ph_refuse_resets(p, SERVER, 0);
    CHECK(ph_run(p, PASSES));
    unsigned repeats = 0;
    CHECK(ph_transport_resets(p, SERVER, &repeats) == resets0 + 1 && repeats == 0);
    ph_stream_trace_t tr = trace_of(p, SERVER, ref);
    CHECK(tr.reset != 0 && tr.reset_code == 0x7 && tr.fin == 0);
    CHECK(tr.reset_reliable == e.header_len);
    CHECK(n->claims == claims0 + 1 && n->commits == commits0 + 1);
    CHECK(drain(p, SERVER).terminals == 0);
    tally_t peer = drain(p, CLIENT);
    CHECK(peer.sg_resets == 1 && peer.sg_reset_code == 0x7);
    CHECK(n->fins_reported == 0);
    CHECK(report(s, ref) == MOQ_OK);             /* after the reset settled */
    CHECK(sweep(p, s));
    CHECK(occupied(s) == 0);
    CHECK(report(s, ref) == MOQ_OK);             /* and once the entry is gone */
    close_pair(p);
}

/* The peer stops the stream and its terminal is recognized: the reset is
 * claimed and refused by the transport, or the terminal waits for event
 * capacity -- with the FIN not yet queued, or handed out and held. A FIN
 * report then is obsolete: it neither clears the debt nor moves the claim.
 * The terminal commits once and its reset settles once. */
typedef enum { STOP_CLAIMED, STOP_PRESSURE, STOP_PRESSURE_FIN_HANDED } stop_kind_t;

static void stop_before_report(moq_version_t v, stop_kind_t k)
{
    static const char *const names[] = {"fin_report.stop_claimed_before_fin",
                                        "fin_report.stop_under_pressure_before_fin",
                                        "fin_report.stop_under_pressure_fin_handed"};
    name_case(names[k], v);
    ph_options_t opt;
    memset(&opt, 0, sizeof(opt));
    opt.tagged[SERVER] = true;
    if (k != STOP_CLAIMED) opt.max_events[SERVER] = 2;
    ph_pair_t *p = open_pair_with(v, &opt);
    owner_t o;
    subscribe(p, &o, TIMEOUT_MS, 0);
    moq_session_t *s = ph_session(p, SERVER);
    const ph_counters_t *n = ph_counters(p, SERVER);
    moq_subgroup_handle_t sg = open_group(p, &o, 0);
    write_object(p, &o, sg, 0, "a");
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t ref = last_ref(p, &o);
    if (k == STOP_PRESSURE_FIN_HANDED) {
        REQUIRE(ph_refuse_data_stream(p, SERVER, ref));
        write_object(p, &o, sg, 1, "held");
        REQUIRE(moq_session_close_subgroup(s, sg, ph_now(p)) == MOQ_OK);
        CHECK(ph_run(p, PASSES));
        CHECK(n->fins_reported == 0);
    }
    if (k != STOP_CLAIMED) {
        request_unpolled(p, "u");
        request_unpolled(p, "v");
        CHECK(ph_run(p, PASSES));
    }
    const unsigned resets0 = ph_transport_resets(p, SERVER, NULL);
    const unsigned claims0 = n->claims, commits0 = n->commits;
    if (k == STOP_CLAIMED) ph_refuse_resets(p, SERVER, 1000);
    REQUIRE(ph_peer_stops_data(p, SERVER, ref, 0x9));
    CHECK(ph_run(p, PASSES));
    sg_snap_t e = sg_snap_of(s, ref);
    CHECK(e.slot >= 0 && e.cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING &&
          e.code == 0x9);
    CHECK(e.fin == (k == STOP_PRESSURE_FIN_HANDED));
    offer_obs_t r = offer_of(s);
    tally_t acc;
    memset(&acc, 0, sizeof(acc));
    if (k == STOP_CLAIMED) {
        /* The claim moved onto the queued reset. */
        CHECK(e.state == MOQ_SG_RESETTING && e.queued_resets == 1 && e.q_claimed);
        CHECK(r.rc == MOQ_OK && r.count == 1 && r.off.state == MOQ_DATA_RESET_CLAIMED);
        CHECK(n->claims == claims0 + 1 && n->commits == commits0);
        acc = drain(p, SERVER);                 /* the terminal committed with the claim */
    } else {
        CHECK(e.state == MOQ_SG_TERMINAL_OWED);
        CHECK(r.rc == MOQ_ERR_WOULD_BLOCK && r.count == 0);   /* event capacity only */
        CHECK(n->claims == claims0);
    }
    check_obsolete(p, SERVER, ref);
    check_obsolete(p, SERVER, ref);
    if (k == STOP_CLAIMED) {
        ph_refuse_resets(p, SERVER, 0);
    } else {
        /* The capacity the application frees lets the terminal commit, in
         * that same poll or after the next pass. */
        tally_t first = drain(p, SERVER);
        acc.terminals += first.terminals;
        if (first.terminals) acc.terminal = first.terminal;
    }
    ph_lift_data_refusal(p, SERVER);
    CHECK(ph_run(p, PASSES));
    tally_t last = drain(p, SERVER);
    acc.terminals += last.terminals;
    if (last.terminals) acc.terminal = last.terminal;
    check_terminal(&acc, sg, &o, ref, 0, MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING, 0x9);
    ph_stream_trace_t tr = trace_of(p, SERVER, ref);
    CHECK(tr.reset != 0 && tr.fin == 0);
    unsigned repeats = 0;
    CHECK(ph_transport_resets(p, SERVER, &repeats) == resets0 + 1 && repeats == 0);
    CHECK(n->claims == claims0 + 1 && n->commits == commits0 + 1);
    CHECK(n->fins_reported == 0 && n->held_expired == 0);
    CHECK(report(s, ref) == MOQ_OK);
    CHECK(sweep(p, s));
    CHECK(occupied(s) == 0);
    close_pair(p);
}

/* A pool of one: once the first subgroup's FIN is reported and its slot
 * reclaimed, the second takes the slot with a new stream. A stale or
 * duplicate report of the first stream changes nothing, and the second
 * subgroup's held expiry still commits its own terminal. */
static void pool_of_one(moq_version_t v)
{
    name_case("fin_report.pool_of_one", v);
    ph_options_t opt;
    memset(&opt, 0, sizeof(opt));
    opt.tagged[SERVER] = true;
    opt.max_open_subgroups[SERVER] = 1;
    ph_pair_t *p = open_pair_with(v, &opt);
    owner_t o;
    subscribe(p, &o, TIMEOUT_MS, 0);
    moq_session_t *s = ph_session(p, SERVER);
    const ph_counters_t *n = ph_counters(p, SERVER);
    moq_subgroup_handle_t first = open_group(p, &o, 0);
    write_object(p, &o, first, 0, "a");
    REQUIRE(moq_session_close_subgroup(s, first, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t old_ref = last_ref(p, &o);
    CHECK(n->fins_reported == 1);
    CHECK(sweep(p, s));
    CHECK(occupied(s) == 0);
    if (occupied(s) != 0) {             /* failed above: no slot for a second subgroup */
        close_pair(p);
        return;
    }
    moq_subgroup_handle_t second = open_group(p, &o, 1);
    write_object(p, &o, second, 0, "b");
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t ref = last_ref(p, &o);
    CHECK(ref._v != old_ref._v);
    REQUIRE(ph_refuse_data_stream(p, SERVER, ref));
    write_object(p, &o, second, 1, "held");
    REQUIRE(moq_session_close_subgroup(s, second, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    CHECK(sweep(p, s));
    CHECK(occupied(s) == 1);
    snap_t x = snap_of(s);
    sg_snap_t e = sg_snap_of(s, ref);
    CHECK(e.custody && e.fin);
    CHECK(moq_session_report_data_fin(s, old_ref) == MOQ_OK);
    CHECK(moq_session_report_data_fin(s, old_ref) == MOQ_OK);
    CHECK(unchanged(s, &x));
    sg_snap_t e2 = sg_snap_of(s, ref);
    CHECK(sg_same(&e, &e2));
    CHECK(sweep(p, s));
    CHECK(occupied(s) == 1);
    uint64_t due = ph_next_held_deadline(p, SERVER);
    REQUIRE(due != UINT64_MAX && due > ph_now(p));
    CHECK(ph_wake(p, due, PASSES));
    CHECK(n->held_expired == 1);
    tally_t t = drain(p, SERVER);
    check_terminal(&t, second, &o, ref, 1, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT,
                   MOQ_RESET_DELIVERY_TIMEOUT);
    ph_lift_data_refusal(p, SERVER);
    CHECK(ph_run(p, PASSES));
    CHECK(n->fins_reported == 1);
    CHECK(moq_session_tick(s, ph_now(p)) == MOQ_OK);
    CHECK(occupied(s) == 0);
    close_pair(p);
}

static void closed_while_held(moq_version_t v)
{
    name_case("fin_report.closed_while_held", v);
    ph_pair_t *p = open_pair(v, SERVER);
    owner_t o;
    subscribe(p, &o, TIMEOUT_MS, 0);
    moq_session_t *s = ph_session(p, SERVER);
    moq_subgroup_handle_t sg = open_group(p, &o, 0);
    write_object(p, &o, sg, 0, "a");
    CHECK(ph_run(p, PASSES));
    moq_stream_ref_t ref = last_ref(p, &o);
    REQUIRE(ph_refuse_data_stream(p, SERVER, ref));
    write_object(p, &o, sg, 1, "held");
    REQUIRE(moq_session_close_subgroup(s, sg, ph_now(p)) == MOQ_OK);
    CHECK(ph_run(p, PASSES));
    CHECK(sweep(p, s));
    CHECK(occupied(s) == 1);
    REQUIRE(moq_session_close(s, 0, NULL, ph_now(p)) == MOQ_OK);
    CHECK(drain(p, SERVER).terminals == 0);
    CHECK(moq_session_report_data_fin(s, ref) == MOQ_ERR_CLOSED);
    ph_lift_data_refusal(p, SERVER);
    CHECK(ph_run(p, PASSES));
    (void)drain(p, CLIENT);
    (void)drain(p, SERVER);
    CHECK(ph_pair_destroy(p) == 0);
}

/* A negotiated SUBGROUP_DELIVERY_TIMEOUT keeps its own rule: the FIN report
 * leaves it running; only the commit report (or its expiry) resolves it. */
static void subgroup_timer(moq_version_t v)
{
    name_case("fin_report.subgroup_timer", v);
    ph_pair_t *p = open_pair(v, SERVER);
    owner_t o;
    subscribe(p, &o, TIMEOUT_MS, 4 * TIMEOUT_MS);
    moq_session_t *s = ph_session(p, SERVER);
    const ph_counters_t *n = ph_counters(p, SERVER);
    moq_subgroup_handle_t sg = open_group(p, &o, 0);
    write_object(p, &o, sg, 0, "a");
    /* The transport accepts the FIN but keeps it from the peer, so nothing
     * is committed yet. */
    ph_set_fin_hold(p, SERVER, true);
    REQUIRE(moq_session_close_subgroup(s, sg, ph_now(p)) == MOQ_OK);
    const uint64_t fin_at = ph_now(p);
    CHECK(ph_run(p, PASSES));
    CHECK(n->fins_reported == 1);
    CHECK(moq_session_next_deadline_us(s) == fin_at + 4 * TIMEOUT_MS * 1000);
    CHECK(moq_session_tick(s, ph_now(p)) == MOQ_OK);
    CHECK(occupied(s) == 1);                   /* the subgroup timer keeps it */
    /* Delivered and read: the host reports it committed, which resolves it. */
    ph_set_fin_hold(p, SERVER, false);
    CHECK(ph_run(p, PASSES));
    CHECK(moq_session_next_deadline_us(s) == UINT64_MAX);
    CHECK(moq_session_tick(s, ph_now(p)) == MOQ_OK);
    CHECK(occupied(s) == 0);
    CHECK(drain(p, SERVER).terminals == 0);
    close_pair(p);
}

int main(void)
{
    const moq_version_t all[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                 MOQ_VERSION_DRAFT_22};
    for (size_t i = 0; i < 3; ++i) {
        result_order(all[i]);
        report_does_not_reclaim(all[i]);
        held_expiry(all[i], false);
        held_expiry(all[i], true);
        handoff(all[i], false);
        handoff(all[i], true);
        partial_writes(all[i]);
        stop_then_expiry(all[i]);
        for (int k = END_UNSUBSCRIBED; k <= END_PUB_UPDATE_REJECTED; ++k)
            cancelled_while_held(all[i], (end_kind_t)k);
        done_keeps_identity(all[i], false);
        done_keeps_identity(all[i], true);
        reset_before_fin(all[i], false);
        reset_before_fin(all[i], true);
        for (int k = STOP_CLAIMED; k <= STOP_PRESSURE_FIN_HANDED; ++k)
            stop_before_report(all[i], (stop_kind_t)k);
        pool_of_one(all[i]);
        closed_while_held(all[i]);
        if (all[i] != MOQ_VERSION_DRAFT_16) subgroup_timer(all[i]);
    }
    if (failures) {
        fprintf(stderr, "host_fin_report: %u of %u checks failed\n", failures, checks);
        return 1;
    }
    printf("host_fin_report: %u checks passed\n", checks);
    return 0;
}
