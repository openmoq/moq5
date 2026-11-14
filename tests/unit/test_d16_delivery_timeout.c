/* Draft-16 DELIVERY TIMEOUT under fake time, through every carrier.
 *
 * Draft-16 9.2.2.2: the timeout "is the duration in milliseconds the relay
 * SHOULD continue to attempt forwarding Objects after they have been
 * received. The start time for the timeout is based on when the Object
 * Headers are received". When the subscriber's parameter and the Track's
 * DELIVERY_TIMEOUT extension (11.1) are both present, "the endpoints use the
 * min of the two values". The budget bounds each object; nothing in the
 * draft bounds how long a subgroup stream stays open.
 *
 * Carriers: SUBSCRIBE (either public selector, or both with one value), an
 * acknowledged REQUEST_UPDATE of a subscription (typed, or the generic
 * microsecond field) and of a publication, PUBLISH_OK, and the publisher's
 * Track Extension on SUBSCRIBE_OK and on PUBLISH, alone and together with
 * the subscriber's value. Every carrier yields the same 100 ms budget.
 *
 * Rows for every carrier:
 *  - healthy: a subgroup idles past the budget before its first object, then
 *    carries objects (the last one streamed in chunks) across several
 *    budgets, each forwarded within its own; the receiver gets every object
 *    and the FIN, and neither end sees a timer, a reset or a terminal event.
 *  - delayed: an object whose header is queued 80 ms after its stream opened
 *    expires exactly 100 ms after that header and not before; its payload is
 *    never handed out, and a higher-priority stream of the same owner keeps
 *    flowing before and after the expiry.
 * And for one subscription carrier and one publication carrier:
 *  - streamed: the chunks of a streamed object carry the deadline its header
 *    set, and expire with it.
 *  - after-fin: an object still queued ahead of its stream's FIN keeps its
 *    own deadline, and its expiry replaces the FIN with the reset; a FIN
 *    handed over starts no timer.
 *  - transport-backlog: through the transport bridge, a stream the transport
 *    will not take (flow control) holds an object for part of its budget and
 *    then delivers it, past the stream's own open + budget; a later object
 *    held for its whole budget expires 100 ms after its header, its bytes
 *    never reach the transport, and another stream flows throughout.
 *  - transport-backlog-fin: the same with the stream's FIN queued behind the
 *    held object. Within budget both are delivered; past it the object
 *    expires at its header deadline, one correlated reset keeps only the
 *    subgroup header, and neither the payload nor the FIN goes out once
 *    credit returns.
 * And once: zero, or two different values, are refused on SUBSCRIBE, on a
 * subscription update and in the Track Extension builder (9.2.2.2, 11.1).
 */
#include <moq/moq.h>
#include <moq/sim.h>
#include "../support/fake_endpoint.h"
#include "test_alloc_support.h"
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

static unsigned checks, failures, prerequisites, cases, failed_cases;
static const char *case_name;
static const uint64_t MS = 1000;
static const uint64_t BUDGET_MS = 100;

static bool require(bool ok, const char *expression, int line)
{
    ++checks;
    if (!ok) {
        ++failures;
        ++prerequisites;
        fprintf(stderr, "PREREQUISITE %s:%d: %s\n", case_name, line, expression);
    }
    return ok;
}
#define NEED(x) do { if (!require(!!(x), #x, __LINE__)) goto done; } while (0)
#define READY(x) do { if (!require(!!(x), #x, __LINE__)) return false; } while (0)

static void expect(const char *name, uint64_t actual, uint64_t wanted)
{
    ++checks;
    if (actual != wanted) {
        ++failures;
        fprintf(stderr, "FAIL %s/%s: got=%" PRIu64 " expected=%" PRIu64 "\n",
                case_name, name, actual, wanted);
    }
}

typedef enum update_kind {
    UPDATE_NONE,
    UPDATE_OBJECT,        /* typed, object selector */
    UPDATE_SUBGROUP,      /* typed, subgroup selector */
    UPDATE_MICROSECONDS,  /* the generic delivery_timeout_us field */
} update_kind_t;

typedef struct carrier {
    const char   *name;
    bool          publication;   /* the client PUBLISHes; else it SUBSCRIBEs */
    uint32_t      selectors;     /* 1 object, 2 subgroup, 3 both */
    uint64_t      subscriber_ms; /* SUBSCRIBE / PUBLISH_OK value, 0 = none */
    uint64_t      extension_ms;  /* publisher Track Extension, 0 = none */
    update_kind_t update;        /* then an acknowledged REQUEST_UPDATE to 100 */
} carrier_t;

static const carrier_t k_carriers[] = {
    {"subscribe",                     false, 1, 100, 0,   UPDATE_NONE},
    {"subscribe-subgroup-selector",   false, 2, 100, 0,   UPDATE_NONE},
    {"subscribe-both-selectors",      false, 3, 100, 0,   UPDATE_NONE},
    {"subscribe-update",              false, 1, 400, 0,   UPDATE_OBJECT},
    {"subscribe-update-subgroup",     false, 1, 400, 0,   UPDATE_SUBGROUP},
    {"subscribe-update-microseconds", false, 1, 400, 0,   UPDATE_MICROSECONDS},
    {"subscribe-ok-extension",        false, 0, 0,   100, UPDATE_NONE},
    {"subscribe-smaller-subscriber",  false, 1, 100, 300, UPDATE_NONE},
    {"subscribe-smaller-extension",   false, 1, 300, 100, UPDATE_NONE},
    {"publish-ok",                    true,  1, 100, 0,   UPDATE_NONE},
    {"publish-ok-subgroup-selector",  true,  2, 100, 0,   UPDATE_NONE},
    {"publication-update",            true,  1, 400, 0,   UPDATE_OBJECT},
    {"publication-update-microseconds", true, 1, 400, 0,  UPDATE_MICROSECONDS},
    {"publish-extension",             true,  0, 0,   100, UPDATE_NONE},
    {"publish-smaller-subscriber",    true,  1, 100, 300, UPDATE_NONE},
    {"publish-smaller-extension",     true,  1, 300, 100, UPDATE_NONE},
};
#define N_CARRIERS (sizeof(k_carriers) / sizeof(k_carriers[0]))

typedef struct fixture {
    test_alloc_state_t memory;
    moq_alloc_t alloc;
    moq_simpair_t *pair;
    moq_session_t *sender, *receiver;
    moq_subscription_t sub;        /* sender's handle (publisher role) */
    moq_subscription_t local_sub;  /* receiver's handle (subscriber role) */
    moq_publication_t pub;         /* sender's publication */
    moq_publication_t peer_pub;    /* receiver's handle for that publication */
    moq_rcbuf_t *payload, *other;
    const carrier_t *carrier;
    uint64_t base;
} fixture_t;

static size_t drain_events(moq_session_t *s)
{
    moq_event_t e;
    size_t count = 0;
    while (moq_session_poll_events(s, &e, 1)) {
        ++count;
        moq_event_cleanup(&e);
    }
    return count;
}

static bool take(moq_session_t *s, moq_event_kind_t kind, moq_event_t *out)
{
    READY(moq_session_poll_events(s, out, 1) == 1);
    if (out->kind != kind) {
        moq_event_cleanup(out);
        READY(out->kind == kind);
    }
    return true;
}

static bool pump(fixture_t *f)
{
    READY(moq_simpair_run_until_quiescent(f->pair, 64, NULL) == MOQ_OK);
    return true;
}

static void select_timeout(uint32_t selectors, uint64_t ms, uint32_t *flags,
                           uint64_t *object_ms, uint64_t *subgroup_ms)
{
    *flags = ms ? selectors : 0;
    *object_ms = (*flags & 1u) ? ms : 0;
    *subgroup_ms = (*flags & 2u) ? ms : 0;
}

/* The subscriber changes its value to 100 ms; the publisher accepts. */
static bool update_to_budget(fixture_t *f)
{
    const carrier_t *c = f->carrier;
    uint32_t flags = 0;
    uint64_t object_ms = 0, subgroup_ms = 0;
    bool generic = c->update == UPDATE_MICROSECONDS;
    if (!generic)
        select_timeout(c->update == UPDATE_OBJECT ? 1u : 2u, BUDGET_MS,
                       &flags, &object_ms, &subgroup_ms);
    uint64_t now = moq_simpair_now_us(f->pair);
    if (c->publication) {
        moq_publication_update_cfg_t u;
        moq_publication_update_cfg_init_sized(&u, sizeof(u));
        u.delivery_timeout_flags = flags;
        u.object_delivery_timeout_ms = object_ms;
        u.subgroup_delivery_timeout_ms = subgroup_ms;
        u.has_delivery_timeout = generic;
        u.delivery_timeout_us = generic ? BUDGET_MS * MS : 0;
        READY(moq_session_update_publication(f->receiver, f->peer_pub, &u, now) == MOQ_OK);
    } else {
        moq_subscription_update_cfg_t u;
        moq_subscription_update_cfg_init_sized(&u, sizeof(u));
        u.delivery_timeout_flags = flags;
        u.object_delivery_timeout_ms = object_ms;
        u.subgroup_delivery_timeout_ms = subgroup_ms;
        u.has_delivery_timeout = generic;
        u.delivery_timeout_us = generic ? BUDGET_MS * MS : 0;
        READY(moq_session_update_subscription(f->receiver, f->local_sub, &u, now) == MOQ_OK);
    }
    if (!pump(f)) return false;
    unsigned acks = 0, other = 0;
    moq_event_t e;
    while (moq_session_poll_events(f->receiver, &e, 1)) {
        if (e.kind == (c->publication ? MOQ_EVENT_PUBLICATION_UPDATE_OK
                                      : MOQ_EVENT_SUBSCRIPTION_UPDATE_OK)) ++acks;
        else ++other;
        moq_event_cleanup(&e);
    }
    READY(acks == 1 && other == 0);
    (void)drain_events(f->sender);   /* the publisher's update notice */
    return true;
}

static bool setup(fixture_t *f, const carrier_t *c)
{
    memset(f, 0, sizeof(*f));
    f->carrier = c;
    f->alloc = test_allocator(&f->memory);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &f->alloc;
    cfg.version = MOQ_VERSION_DRAFT_16;
    cfg.initial_now_us = 1000000;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 64;
    READY(moq_simpair_create(&cfg, &f->pair) == MOQ_OK);
    READY(moq_simpair_start(f->pair) == MOQ_OK);
    if (!pump(f)) return false;
    moq_session_t *client = moq_simpair_client(f->pair);
    moq_session_t *server = moq_simpair_server(f->pair);
    READY(moq_session_state(client) == MOQ_SESS_ESTABLISHED);
    READY(moq_session_state(server) == MOQ_SESS_ESTABLISHED);
    READY(drain_events(client) == 1 && drain_events(server) == 1);
    f->sender = c->publication ? client : server;
    f->receiver = c->publication ? server : client;

    /* Draft-16 11.1: the publisher's value is Track Extension 0x02. */
    uint8_t extension[16];
    size_t extension_len = 0;
    if (c->extension_ms) {
        moq_track_delivery_timeouts_cfg_t t;
        moq_track_delivery_timeouts_cfg_init(&t);
        t.has_object_delivery_timeout = true;
        t.object_delivery_timeout_ms = c->extension_ms;
        READY(moq_session_track_properties_add_delivery_timeouts(f->sender, NULL, 0, &t,
                  extension, sizeof(extension), &extension_len) == MOQ_OK);
        READY(extension_len == 3 && extension[0] == 0x02);
    }
    moq_bytes_t properties = {extension, extension_len};
    moq_bytes_t part = MOQ_BYTES_LITERAL("d16-timeout");
    uint64_t now = moq_simpair_now_us(f->pair);
    moq_event_t e;
    if (c->publication) {
        moq_publish_cfg_t pc;
        moq_publish_cfg_init_sized(&pc, sizeof(pc));
        pc.track_namespace = (moq_namespace_t){&part, 1};
        pc.track_name = MOQ_BYTES_LITERAL("v");
        pc.track_properties = properties;
        READY(moq_session_publish(client, &pc, now, &f->pub) == MOQ_OK);
        if (!pump(f) || !take(server, MOQ_EVENT_PUBLISH_REQUEST, &e)) return false;
        f->peer_pub = e.u.publish_request.pub;
        moq_event_cleanup(&e);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
        select_timeout(c->selectors, c->subscriber_ms, &ac.delivery_timeout_flags,
                       &ac.object_delivery_timeout_ms, &ac.subgroup_delivery_timeout_ms);
        READY(moq_session_accept_publish(server, f->peer_pub, &ac, now) == MOQ_OK);
        if (!pump(f) || !take(client, MOQ_EVENT_PUBLISH_OK, &e)) return false;
        bool same = e.u.publish_ok.pub._opaque == f->pub._opaque &&
            e.u.publish_ok.has_delivery_timeout == (c->subscriber_ms != 0) &&
            (c->subscriber_ms == 0 || e.u.publish_ok.delivery_timeout_ms == c->subscriber_ms);
        moq_event_cleanup(&e);
        READY(same);
    } else {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init_sized(&sc, sizeof(sc));
        sc.track_namespace = (moq_namespace_t){&part, 1};
        sc.track_name = MOQ_BYTES_LITERAL("v");
        sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        select_timeout(c->selectors, c->subscriber_ms, &sc.delivery_timeout_flags,
                       &sc.object_delivery_timeout_ms, &sc.subgroup_delivery_timeout_ms);
        READY(moq_session_subscribe(client, &sc, now, &f->local_sub) == MOQ_OK);
        if (!pump(f) || !take(server, MOQ_EVENT_SUBSCRIBE_REQUEST, &e)) return false;
        f->sub = e.u.subscribe_request.sub;
        moq_event_cleanup(&e);
        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        ac.track_properties = properties;
        READY(moq_session_accept_subscribe(server, f->sub, &ac, now) == MOQ_OK);
        if (!pump(f) || !take(client, MOQ_EVENT_SUBSCRIBE_OK, &e)) return false;
        bool same = e.u.subscribe_ok.sub._opaque == f->local_sub._opaque;
        moq_event_cleanup(&e);
        READY(same);
    }
    if (c->update != UPDATE_NONE && !update_to_budget(f)) return false;
    READY(drain_events(client) == 0 && drain_events(server) == 0);
    READY(moq_session_next_deadline_us(f->sender) == UINT64_MAX);
    READY(moq_rcbuf_create(&f->alloc, (const uint8_t *)"object", 6, &f->payload) == MOQ_OK);
    READY(moq_rcbuf_create(&f->alloc, (const uint8_t *)"other", 5, &f->other) == MOQ_OK);
    f->base = moq_simpair_now_us(f->pair);
    return true;
}

static uint64_t at(const fixture_t *f, uint64_t ms)
{
    return f->base + ms * MS;
}

static void cleanup(fixture_t *f, unsigned before)
{
    if (f->sender) {
        moq_action_t a;
        while (moq_session_poll_actions(f->sender, &a, 1)) moq_action_cleanup(&a);
    }
    if (f->payload) {
        expect("final-payload-ref", moq_rcbuf_refcount(f->payload), 1);
        moq_rcbuf_decref(f->payload);
    }
    if (f->other) moq_rcbuf_decref(f->other);
    if (f->pair) moq_simpair_destroy(f->pair);
    expect("allocation-balance", (uint64_t)f->memory.balance, 0);
    ++cases;
    if (failures != before) ++failed_cases;
    printf("%s %s\n", failures == before ? "PASS" : "FAIL", case_name);
}

static moq_result_t open_subgroup(fixture_t *f, uint64_t group, uint8_t priority,
                                  uint64_t ms, moq_subgroup_handle_t *sg)
{
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    cfg.group_id = group;
    cfg.publisher_priority = priority;
    return f->carrier->publication
        ? moq_session_open_pub_subgroup(f->sender, f->pub, &cfg, at(f, ms), sg)
        : moq_session_open_subgroup(f->sender, f->sub, &cfg, at(f, ms), sg);
}

/* -- healthy: the pair forwards everything ------------------------------- */

typedef struct received {
    uint64_t objects;      /* complete objects, in Object ID order */
    uint64_t finished, resets, other;
    bool in_order;
    bool finished_last;    /* the FIN reported the last object */
} received_t;

static void receive(fixture_t *f, received_t *rx, uint64_t group, uint64_t last)
{
    moq_event_t e;
    while (moq_session_poll_events(f->receiver, &e, 1)) {
        if (e.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            rx->in_order &= e.u.object_received.group_id == group &&
                            e.u.object_received.object_id == rx->objects;
            ++rx->objects;
        } else if (e.kind == MOQ_EVENT_OBJECT_CHUNK) {
            if (e.u.object_chunk.end) {
                rx->in_order &= e.u.object_chunk.group_id == group &&
                                e.u.object_chunk.object_id == rx->objects &&
                                e.u.object_chunk.terminal == MOQ_OBJECT_TERMINAL_NORMAL;
                ++rx->objects;
            }
        } else if (e.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
            ++rx->finished;
            rx->finished_last = e.u.subgroup_finished.group_id == group &&
                                e.u.subgroup_finished.has_last_object &&
                                e.u.subgroup_finished.last_object_id == last;
        } else if (e.kind == MOQ_EVENT_SUBGROUP_RESET) {
            ++rx->resets;
        } else {
            ++rx->other;
        }
        moq_event_cleanup(&e);
    }
}

/* Advance both sessions to `ms`, let the pair exchange everything queued,
 * and check that the sender saw no terminal and the receiver no reset. */
static bool settle(fixture_t *f, received_t *rx, uint64_t ms, const char *name)
{
    READY(moq_simpair_advance_to(f->pair, at(f, ms)) == MOQ_OK);
    if (!pump(f)) return false;
    receive(f, rx, 7, 8);
    expect(name, drain_events(f->sender), 0);
    expect("receiver-no-reset", rx->resets, 0);
    return true;
}

static void healthy_row(const carrier_t *c)
{
    char name[96];
    snprintf(name, sizeof(name), "d16/%s/healthy", c->name);
    case_name = name;
    unsigned before = failures;
    fixture_t f;
    received_t rx = {.in_order = true};
    moq_rcbuf_t *first = NULL, *second = NULL;
    if (!setup(&f, c)) goto done;
    NEED(moq_rcbuf_create(&f.alloc, (const uint8_t *)"obj", 3, &first) == MOQ_OK);
    NEED(moq_rcbuf_create(&f.alloc, (const uint8_t *)"ect", 3, &second) == MOQ_OK);

    moq_subgroup_handle_t sg;
    NEED(moq_simpair_advance_to(f.pair, at(&f, 10)) == MOQ_OK);
    NEED(open_subgroup(&f, 7, 128, 10, &sg) == MOQ_OK);
    NEED(pump(&f));
    expect("open-arms-no-timer", moq_session_next_deadline_us(f.sender), UINT64_MAX);
    /* An empty stream, idle past open + budget. */
    if (!settle(&f, &rx, 10 + 15 * BUDGET_MS / 10, "idle-open-stream-no-terminal")) goto done;
    expect("idle-open-stream-still-no-timer", moq_session_next_deadline_us(f.sender), UINT64_MAX);
    expect("nothing-received-while-idle", rx.objects, 0);

    /* Objects 0..7, one every 60 ms, each forwarded 40 ms after its header:
     * within its own budget, while the stream's age crosses five budgets. */
    uint64_t t = 160;
    for (uint64_t id = 0; id < 8; ++id, t += 60) {
        NEED(moq_simpair_advance_to(f.pair, at(&f, t)) == MOQ_OK);
        NEED(moq_session_write_object(f.sender, sg, id, f.payload, at(&f, t)) == MOQ_OK);
        expect("object-deadline-from-its-header", moq_session_next_deadline_us(f.sender),
               at(&f, t + BUDGET_MS));
        if (!settle(&f, &rx, t + 40, "forwarded-object-no-terminal")) goto done;
        expect("forwarded-object-received", rx.objects, id + 1);
        expect("forwarded-object-leaves-no-timer", moq_session_next_deadline_us(f.sender),
               UINT64_MAX);
    }
    /* Object 8 streamed in two chunks; both go out before header + budget. */
    NEED(moq_simpair_advance_to(f.pair, at(&f, 640)) == MOQ_OK);
    NEED(moq_session_begin_object(f.sender, sg, 8, 6, at(&f, 640)) == MOQ_OK);
    if (!settle(&f, &rx, 670, "streamed-header-no-terminal")) goto done;
    NEED(moq_session_write_object_data(f.sender, sg, first, at(&f, 670)) == MOQ_OK);
    if (!settle(&f, &rx, 700, "streamed-chunk-no-terminal")) goto done;
    NEED(moq_session_write_object_data(f.sender, sg, second, at(&f, 700)) == MOQ_OK);
    NEED(moq_session_end_object(f.sender, sg, at(&f, 700)) == MOQ_OK);
    if (!settle(&f, &rx, 730, "streamed-object-no-terminal")) goto done;
    expect("streamed-object-received", rx.objects, 9);

    NEED(moq_session_close_subgroup(f.sender, sg, at(&f, 760)) == MOQ_OK);
    if (!settle(&f, &rx, 760, "fin-no-terminal")) goto done;
    expect("fin-starts-no-timer", moq_session_next_deadline_us(f.sender), UINT64_MAX);
    if (!settle(&f, &rx, 760 + 3 * BUDGET_MS, "after-fin-no-terminal")) goto done;

    expect("every-object-received", rx.objects, 9);
    expect("objects-in-order", rx.in_order, true);
    expect("one-fin-received", rx.finished, 1);
    expect("fin-names-last-object", rx.finished_last, true);
    expect("no-other-receiver-events", rx.other, 0);
    expect("sender-live", moq_session_state(f.sender), MOQ_SESS_ESTABLISHED);
    expect("receiver-live", moq_session_state(f.receiver), MOQ_SESS_ESTABLISHED);
done:
    if (first) moq_rcbuf_decref(first);
    if (second) moq_rcbuf_decref(second);
    cleanup(&f, before);
}

/* -- sender-only rows: the test is the transport ------------------------- */

typedef struct tally {
    unsigned payloads, other_payloads, fins, resets, other;
    uint64_t reset_reliable;
} tally_t;

/* Poll `limit` actions (0: all) as a transport would take them. */
static tally_t poll_sender(fixture_t *f, moq_stream_ref_t ref, moq_stream_ref_t other_ref,
                           size_t limit)
{
    tally_t t = {0};
    moq_action_t a;
    size_t taken = 0;
    while ((limit == 0 || taken < limit) && moq_session_poll_actions(f->sender, &a, 1)) {
        ++taken;
        if (a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.stream_ref._v == ref._v) {
            if (a.u.send_data.payload) ++t.payloads;
            else if (a.u.send_data.fin) ++t.fins;
            else ++t.other;
        } else if (a.kind == MOQ_ACTION_SEND_DATA &&
                   a.u.send_data.stream_ref._v == other_ref._v) {
            if (a.u.send_data.payload) ++t.other_payloads;
            else if (!a.u.send_data.fin) ++t.other;
        } else if (a.kind == MOQ_ACTION_RESET_DATA &&
                   a.u.reset_data.stream_ref._v == ref._v &&
                   a.u.reset_data.error_code == MOQ_RESET_DELIVERY_TIMEOUT) {
            ++t.resets;
            t.reset_reliable = a.u.reset_data.reliable_size;
        } else {
            ++t.other;
        }
        moq_action_cleanup(&a);
    }
    return t;
}

/* Open a subgroup and take its header the way a transport does. */
static bool open_taken(fixture_t *f, uint64_t group, uint8_t priority, uint64_t ms,
                       moq_subgroup_handle_t *sg, moq_stream_ref_t *ref, uint8_t *header_len)
{
    READY(open_subgroup(f, group, priority, ms, sg) == MOQ_OK);
    moq_action_t a;
    READY(moq_session_poll_actions(f->sender, &a, 1) == 1);
    bool header = a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.header_len > 0 &&
        a.u.send_data.payload == NULL && !a.u.send_data.fin;
    if (header) {
        *ref = a.u.send_data.stream_ref;
        if (header_len) *header_len = a.u.send_data.header_len;
    }
    moq_action_cleanup(&a);
    READY(header && ref->_v != 0);
    size_t extra = moq_session_poll_actions(f->sender, &a, 1);
    if (extra) moq_action_cleanup(&a);
    READY(extra == 0);
    return true;
}

/* Exactly one DELIVERY_TIMEOUT terminal, for exactly this subgroup. */
static void expect_one_terminal(fixture_t *f, moq_subgroup_handle_t sg,
                                moq_stream_ref_t ref, uint64_t group)
{
    moq_event_t e;
    unsigned terminals = 0, other = 0;
    while (moq_session_poll_events(f->sender, &e, 1)) {
        if (e.kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED &&
            e.detail_size >= MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE) {
            const moq_outgoing_subgroup_terminated_event_t *d =
                &e.u.outgoing_subgroup_terminated;
            ++terminals;
            expect("terminal-subgroup", d->subgroup._opaque, sg._opaque);
            expect("terminal-subscription", d->sub._opaque, f->sub._opaque);
            expect("terminal-publication", d->pub._opaque, f->pub._opaque);
            expect("terminal-stream", d->stream_ref._v, ref._v);
            expect("terminal-group", d->group_id, group);
            expect("terminal-code", d->error_code, MOQ_RESET_DELIVERY_TIMEOUT);
            expect("terminal-cause", d->cause, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT);
        } else {
            ++other;
        }
        moq_event_cleanup(&e);
    }
    expect("exactly-one-timeout-terminal", terminals, 1);
    expect("no-other-sender-events", other, 0);
}

static void delayed_row(const carrier_t *c)
{
    char name[96];
    snprintf(name, sizeof(name), "d16/%s/delayed", c->name);
    case_name = name;
    unsigned before = failures;
    fixture_t f;
    if (!setup(&f, c)) goto done;
    moq_subgroup_handle_t a, b;
    moq_stream_ref_t ar = moq_stream_ref_from_u64(0), br = moq_stream_ref_from_u64(0);
    uint8_t header_len = 0;
    /* A carries the delayed object; B, higher priority, is unrelated work. */
    if (!open_taken(&f, 7, 200, 0, &a, &ar, &header_len) ||
        !open_taken(&f, 8, 10, 0, &b, &br, NULL)) goto done;
    expect("opens-arm-no-timer", moq_session_next_deadline_us(f.sender), UINT64_MAX);

    NEED(moq_session_write_object(f.sender, a, 0, f.payload, at(&f, 80)) == MOQ_OK);
    expect("queued-payload-ref", moq_rcbuf_refcount(f.payload), 2);
    expect("deadline-from-header", moq_session_next_deadline_us(f.sender), at(&f, 180));

    NEED(moq_session_tick(f.sender, at(&f, BUDGET_MS)) == MOQ_OK);
    expect("no-timeout-at-open-plus-budget", drain_events(f.sender), 0);
    expect("deadline-still-from-header", moq_session_next_deadline_us(f.sender), at(&f, 180));

    /* B flows past the waiting object. */
    NEED(moq_session_write_object(f.sender, b, 0, f.other, at(&f, BUDGET_MS)) == MOQ_OK);
    tally_t t = poll_sender(&f, ar, br, 1);
    expect("unrelated-object-handed-over", t.other_payloads, 1);
    expect("waiting-object-not-taken", t.payloads, 0);

    NEED(moq_session_tick(f.sender, at(&f, 180) - 1) == MOQ_OK);
    expect("no-timeout-one-microsecond-early", drain_events(f.sender), 0);
    expect("deadline-unchanged", moq_session_next_deadline_us(f.sender), at(&f, 180));

    NEED(moq_session_tick(f.sender, at(&f, 180)) == MOQ_OK);
    expect_one_terminal(&f, a, ar, 7);
    t = poll_sender(&f, ar, br, 0);
    expect("exactly-one-timeout-reset", t.resets, 1);
    expect("reset-keeps-header", t.reset_reliable, header_len);
    expect("expired-payload-never-handed-out", t.payloads, 0);
    expect("no-fin", t.fins, 0);
    expect("nothing-else", t.other + t.other_payloads, 0);
    expect("expired-payload-released", moq_rcbuf_refcount(f.payload), 1);
    expect("timer-cleared", moq_session_next_deadline_us(f.sender), UINT64_MAX);
    expect("expired-subgroup-retired",
           moq_session_write_object(f.sender, a, 1, f.payload, at(&f, 181)) < 0, true);

    /* B keeps working and finishes normally. */
    NEED(moq_session_write_object(f.sender, b, 1, f.other, at(&f, 181)) == MOQ_OK);
    t = poll_sender(&f, br, ar, 0);
    expect("unrelated-continues", t.payloads, 1);
    NEED(moq_session_close_subgroup(f.sender, b, at(&f, 190)) == MOQ_OK);
    t = poll_sender(&f, br, ar, 0);
    expect("unrelated-fin", t.fins, 1);
    expect("unrelated-no-reset", t.resets, 0);
    NEED(moq_session_tick(f.sender, at(&f, 1000)) == MOQ_OK);
    t = poll_sender(&f, br, ar, 0);
    expect("nothing-later", t.payloads + t.fins + t.resets + t.other, 0);
    expect("no-later-sender-events", drain_events(f.sender), 0);
    expect("sender-live", moq_session_state(f.sender), MOQ_SESS_ESTABLISHED);
    expect("receiver-live", moq_session_state(f.receiver), MOQ_SESS_ESTABLISHED);
done:
    cleanup(&f, before);
}

static void streamed_row(const carrier_t *c)
{
    char name[96];
    snprintf(name, sizeof(name), "d16/%s/streamed", c->name);
    case_name = name;
    unsigned before = failures;
    fixture_t f;
    moq_rcbuf_t *chunk = NULL;
    if (!setup(&f, c)) goto done;
    NEED(moq_rcbuf_create(&f.alloc, (const uint8_t *)"obj", 3, &chunk) == MOQ_OK);
    moq_subgroup_handle_t a;
    moq_stream_ref_t ar = moq_stream_ref_from_u64(0);
    uint8_t header_len = 0;
    if (!open_taken(&f, 7, 128, 0, &a, &ar, &header_len)) goto done;
    NEED(moq_session_begin_object(f.sender, a, 0, 6, at(&f, 80)) == MOQ_OK);
    expect("deadline-from-header", moq_session_next_deadline_us(f.sender), at(&f, 180));
    NEED(moq_session_tick(f.sender, at(&f, BUDGET_MS)) == MOQ_OK);
    expect("no-timeout-at-open-plus-budget", drain_events(f.sender), 0);
    NEED(moq_session_write_object_data(f.sender, a, chunk, at(&f, 150)) == MOQ_OK);
    expect("chunk-keeps-header-deadline", moq_session_next_deadline_us(f.sender), at(&f, 180));
    NEED(moq_session_tick(f.sender, at(&f, 180) - 1) == MOQ_OK);
    expect("no-timeout-one-microsecond-early", drain_events(f.sender), 0);
    NEED(moq_session_tick(f.sender, at(&f, 180)) == MOQ_OK);
    expect_one_terminal(&f, a, ar, 7);
    tally_t t = poll_sender(&f, ar, moq_stream_ref_from_u64(0), 0);
    expect("exactly-one-timeout-reset", t.resets, 1);
    expect("reset-keeps-header", t.reset_reliable, header_len);
    expect("expired-chunk-never-handed-out", t.payloads, 0);
    expect("expired-object-header-never-handed-out", t.other, 0);
    expect("expired-chunk-released", moq_rcbuf_refcount(chunk), 1);
    expect("sender-live", moq_session_state(f.sender), MOQ_SESS_ESTABLISHED);
done:
    if (chunk) moq_rcbuf_decref(chunk);
    cleanup(&f, before);
}

static void after_fin_row(const carrier_t *c)
{
    char name[96];
    snprintf(name, sizeof(name), "d16/%s/after-fin", c->name);
    case_name = name;
    unsigned before = failures;
    fixture_t f;
    if (!setup(&f, c)) goto done;
    moq_subgroup_handle_t a, b;
    moq_stream_ref_t ar = moq_stream_ref_from_u64(0), br = moq_stream_ref_from_u64(0);
    uint8_t header_len = 0;
    if (!open_taken(&f, 7, 200, 0, &a, &ar, &header_len) ||
        !open_taken(&f, 8, 10, 0, &b, &br, NULL)) goto done;
    expect("opens-arm-no-timer", moq_session_next_deadline_us(f.sender), UINT64_MAX);
    /* A's object and FIN wait; B's object and FIN are handed over. */
    NEED(moq_session_write_object(f.sender, a, 0, f.payload, at(&f, 80)) == MOQ_OK);
    NEED(moq_session_write_object(f.sender, b, 0, f.other, at(&f, 80)) == MOQ_OK);
    tally_t t = poll_sender(&f, br, ar, 1);
    expect("b-object-handed-over", t.payloads, 1);
    NEED(moq_session_close_subgroup(f.sender, a, at(&f, 90)) == MOQ_OK);
    NEED(moq_session_close_subgroup(f.sender, b, at(&f, 90)) == MOQ_OK);
    t = poll_sender(&f, br, ar, 1);
    expect("b-fin-handed-over", t.fins, 1);
    expect("only-a-object-deadline", moq_session_next_deadline_us(f.sender), at(&f, 180));
    NEED(moq_session_on_data_committed(f.sender, br, at(&f, 95)) == MOQ_OK);
    expect("commit-report-changes-nothing", moq_session_next_deadline_us(f.sender), at(&f, 180));

    NEED(moq_session_tick(f.sender, at(&f, 180) - 1) == MOQ_OK);
    expect("no-timeout-one-microsecond-early", drain_events(f.sender), 0);
    NEED(moq_session_tick(f.sender, at(&f, 180)) == MOQ_OK);
    expect_one_terminal(&f, a, ar, 7);
    t = poll_sender(&f, ar, br, 0);
    expect("exactly-one-timeout-reset", t.resets, 1);
    expect("reset-keeps-header", t.reset_reliable, header_len);
    expect("expired-payload-never-handed-out", t.payloads, 0);
    expect("reset-replaces-fin", t.fins, 0);
    expect("nothing-else", t.other + t.other_payloads, 0);
    expect("expired-payload-released", moq_rcbuf_refcount(f.payload), 1);
    NEED(moq_session_tick(f.sender, at(&f, 180 + 10 * BUDGET_MS)) == MOQ_OK);
    t = poll_sender(&f, br, ar, 0);
    expect("handed-over-fin-never-reset", t.resets + t.payloads + t.fins + t.other, 0);
    expect("no-later-sender-events", drain_events(f.sender), 0);
    expect("timer-cleared", moq_session_next_deadline_us(f.sender), UINT64_MAX);
    expect("sender-live", moq_session_state(f.sender), MOQ_SESS_ESTABLISHED);
done:
    cleanup(&f, before);
}

/* -- transport backlog: the bridge holds what the transport will not take -- */

typedef struct wire {
    fake_endpoint_t ep;
    moq_transport_bridge_t *bridge;
} wire_t;

/* How many writes the transport accepted on `sid` that contain `bytes`. */
static unsigned writes_with(const wire_t *w, uint64_t sid, const char *bytes)
{
    size_t n = strlen(bytes);
    unsigned count = 0;
    for (size_t i = 0; i < w->ep.count; ++i) {
        const fake_op_t *o = &w->ep.ops[i];
        if (o->kind != FAKE_OP_WRITE || o->stream_id != sid || o->data_len < n) continue;
        for (size_t j = 0; j + n <= o->data_len; ++j)
            if (memcmp(o->data + j, bytes, n) == 0) { ++count; break; }
    }
    return count;
}

static bool wrote(const wire_t *w, uint64_t sid, const char *bytes)
{
    return writes_with(w, sid, bytes) > 0;
}

static unsigned ops_on(const wire_t *w, uint64_t sid, fake_op_kind_t kind, bool fin)
{
    unsigned n = 0;
    for (size_t i = 0; i < w->ep.count; ++i) {
        const fake_op_t *o = &w->ep.ops[i];
        if (o->kind == kind && o->stream_id == sid && (kind != FAKE_OP_WRITE || o->fin == fin))
            ++n;
    }
    return n;
}

static unsigned timeout_resets(const wire_t *w, uint64_t sid)
{
    unsigned n = 0;
    for (size_t i = 0; i < w->ep.count; ++i)
        if ((w->ep.ops[i].kind == FAKE_OP_RESET || w->ep.ops[i].kind == FAKE_OP_RESET_AT) &&
            w->ep.ops[i].stream_id == sid &&
            w->ep.ops[i].error_code == MOQ_RESET_DELIVERY_TIMEOUT) ++n;
    return n;
}

/* The reliable size of the first DELIVERY_TIMEOUT reliable reset on `sid`;
 * UINT64_MAX when there is none. */
static uint64_t reset_reliable(const wire_t *w, uint64_t sid)
{
    for (size_t i = 0; i < w->ep.count; ++i)
        if (w->ep.ops[i].kind == FAKE_OP_RESET_AT && w->ep.ops[i].stream_id == sid &&
            w->ep.ops[i].error_code == MOQ_RESET_DELIVERY_TIMEOUT)
            return w->ep.ops[i].reliable_size;
    return UINT64_MAX;
}

/* A bridge over the fake transport; `reliable` lets it reset with a reliable
 * size (RESET_STREAM_AT). */
static bool attach_wire(fixture_t *f, wire_t *w, bool reliable)
{
    fake_endpoint_init(&w->ep, 1000, 2000);
    if (reliable) fake_endpoint_enable_reset_at(&w->ep);
    w->ep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    moq_transport_bridge_cfg_t bc;
    moq_transport_bridge_cfg_init(&bc, &f->alloc);
    READY(moq_transport_bridge_create(&bc, f->sender, &w->ep.vtable, &w->ep, &w->bridge) == MOQ_OK);
    return true;
}

static bool serve(fixture_t *f, wire_t *w, uint64_t ms)
{
    READY(moq_session_tick(f->sender, at(f, ms)) == MOQ_OK);
    READY(moq_transport_bridge_service(w->bridge, at(f, ms)) == MOQ_OK);
    READY(!moq_transport_bridge_is_fatal(w->bridge));
    return true;
}

/* Open a subgroup at `ms` and let the bridge carry its header; the transport
 * stream is the one the bridge opened for it, and its only write so far is
 * that header. */
static bool open_served(fixture_t *f, wire_t *w, uint64_t group, uint64_t ms,
                        moq_subgroup_handle_t *sg, uint64_t *sid, moq_stream_ref_t *ref,
                        uint64_t *header_len)
{
    size_t first = w->ep.count;
    READY(open_subgroup(f, group, 128, ms, sg) == MOQ_OK);
    if (!serve(f, w, ms)) return false;
    *sid = UINT64_MAX;
    for (size_t i = first; i < w->ep.count && *sid == UINT64_MAX; ++i)
        if (w->ep.ops[i].kind == FAKE_OP_OPEN_UNI) *sid = w->ep.ops[i].stream_id;
    READY(*sid != UINT64_MAX && ops_on(w, *sid, FAKE_OP_WRITE, false) == 1);
    for (size_t i = first; header_len && i < w->ep.count; ++i)
        if (w->ep.ops[i].kind == FAKE_OP_WRITE && w->ep.ops[i].stream_id == *sid)
            *header_len = w->ep.ops[i].data_len;
    *ref = moq_transport_bridge_find_ref(w->bridge, *sid);
    READY(ref->_v != 0);
    return true;
}

static void transport_backlog_row(const carrier_t *c)
{
    char name[96];
    snprintf(name, sizeof(name), "d16/%s/transport-backlog", c->name);
    case_name = name;
    unsigned before = failures;
    fixture_t f;
    wire_t w;
    memset(&w, 0, sizeof(w));
    moq_rcbuf_t *late = NULL;
    if (!setup(&f, c)) goto done;
    NEED(moq_rcbuf_create(&f.alloc, (const uint8_t *)"late!", 5, &late) == MOQ_OK);
    if (!attach_wire(&f, &w, false)) goto done;

    moq_subgroup_handle_t a, b;
    uint64_t asid = UINT64_MAX, bsid = UINT64_MAX;
    moq_stream_ref_t ar = moq_stream_ref_from_u64(0), br = moq_stream_ref_from_u64(0);
    if (!open_served(&f, &w, 7, 0, &a, &asid, &ar, NULL) ||
        !open_served(&f, &w, 8, 0, &b, &bsid, &br, NULL)) goto done;
    expect("opens-arm-no-timer", moq_session_next_deadline_us(f.sender), UINT64_MAX);

    /* The transport takes nothing more on A's stream for a while. */
    fake_block_stream(&w.ep, asid);
    NEED(moq_session_write_object(f.sender, a, 0, f.payload, at(&f, 80)) == MOQ_OK);
    if (!serve(&f, &w, 80)) goto done;
    expect("held-object-not-written", wrote(&w, asid, "object"), false);
    expect("held-object-wakes-at-its-deadline", moq_session_next_deadline_us(f.sender),
           at(&f, 180));
    if (!serve(&f, &w, BUDGET_MS)) goto done;
    expect("no-timeout-at-open-plus-budget", drain_events(f.sender), 0);
    expect("no-reset-at-open-plus-budget", timeout_resets(&w, asid) + timeout_resets(&w, bsid), 0);
    NEED(moq_session_write_object(f.sender, b, 0, f.other, at(&f, BUDGET_MS)) == MOQ_OK);
    if (!serve(&f, &w, BUDGET_MS)) goto done;
    expect("other-stream-flows", wrote(&w, bsid, "other"), true);

    /* Credit returns within the object's budget: it is delivered. */
    fake_unblock_stream(&w.ep);
    if (!serve(&f, &w, 150)) goto done;
    expect("held-object-delivered-in-budget", wrote(&w, asid, "object"), true);
    expect("delivered-object-leaves-no-timer", moq_session_next_deadline_us(f.sender),
           UINT64_MAX);
    expect("no-timeout-while-delivered", drain_events(f.sender), 0);

    /* Blocked again, the next object waits out its whole budget. */
    fake_block_stream(&w.ep, asid);
    NEED(moq_session_write_object(f.sender, a, 1, late, at(&f, 200)) == MOQ_OK);
    if (!serve(&f, &w, 200)) goto done;
    expect("late-object-wakes-at-its-deadline", moq_session_next_deadline_us(f.sender),
           at(&f, 300));
    if (!serve(&f, &w, 300 - 1)) goto done;
    expect("no-timeout-one-microsecond-early", drain_events(f.sender), 0);
    expect("no-reset-one-microsecond-early", timeout_resets(&w, asid), 0);
    if (!serve(&f, &w, 300)) goto done;
    expect_one_terminal(&f, a, ar, 7);
    expect("exactly-one-timeout-reset", timeout_resets(&w, asid), 1);
    fake_unblock_stream(&w.ep);
    if (!serve(&f, &w, 301)) goto done;
    expect("expired-object-never-written", wrote(&w, asid, "late!"), false);
    expect("no-fin-on-expired-stream", ops_on(&w, asid, FAKE_OP_WRITE, true), 0);
    expect("expired-payload-released", moq_rcbuf_refcount(late), 1);

    /* B keeps working and finishes normally. */
    NEED(moq_session_write_object(f.sender, b, 1, f.other, at(&f, 301)) == MOQ_OK);
    NEED(moq_session_close_subgroup(f.sender, b, at(&f, 310)) == MOQ_OK);
    if (!serve(&f, &w, 310)) goto done;
    expect("other-stream-fin", ops_on(&w, bsid, FAKE_OP_WRITE, true), 1);
    if (!serve(&f, &w, 1000)) goto done;
    expect("other-stream-never-reset", timeout_resets(&w, bsid), 0);
    expect("no-later-sender-events", drain_events(f.sender), 0);
    expect("recorder-lost-nothing", w.ep.overflowed || w.ep.truncated, false);
    expect("sender-live", moq_session_state(f.sender), MOQ_SESS_ESTABLISHED);
done:
    if (w.bridge) moq_transport_bridge_destroy(w.bridge);
    if (late) moq_rcbuf_decref(late);
    cleanup(&f, before);
}

/* The transport will not take an object nor the FIN queued behind it, so the
 * bridge owns both. Credit returning within the object's budget delivers the
 * object and then the FIN. Held past it, the object expires at its header
 * deadline and no earlier: one correlated DELIVERY_TIMEOUT terminal and one
 * reset that keeps only the subgroup header, and once credit returns neither
 * the expired payload nor the FIN reaches the transport. Each buffer the
 * bridge held is released once, and another stream, and a subgroup opened
 * after the expiry, keep working. */
static void transport_fin_backlog_row(const carrier_t *c)
{
    char name[96];
    snprintf(name, sizeof(name), "d16/%s/transport-backlog-fin", c->name);
    case_name = name;
    unsigned before = failures;
    fixture_t f;
    wire_t w;
    memset(&w, 0, sizeof(w));
    moq_rcbuf_t *held = NULL, *late = NULL;
    if (!setup(&f, c)) goto done;
    NEED(moq_rcbuf_create(&f.alloc, (const uint8_t *)"held!", 5, &held) == MOQ_OK);
    NEED(moq_rcbuf_create(&f.alloc, (const uint8_t *)"late!", 5, &late) == MOQ_OK);
    if (!attach_wire(&f, &w, true)) goto done;

    moq_subgroup_handle_t a, b, e, d;
    uint64_t asid = UINT64_MAX, bsid = UINT64_MAX, esid = UINT64_MAX, dsid = UINT64_MAX;
    uint64_t eheader = 0;
    moq_stream_ref_t ar = moq_stream_ref_from_u64(0), br = moq_stream_ref_from_u64(0);
    moq_stream_ref_t er = moq_stream_ref_from_u64(0), dr = moq_stream_ref_from_u64(0);
    if (!open_served(&f, &w, 7, 0, &a, &asid, &ar, NULL) ||
        !open_served(&f, &w, 8, 0, &b, &bsid, &br, NULL)) goto done;
    expect("opens-arm-no-timer", moq_session_next_deadline_us(f.sender), UINT64_MAX);

    /* Within budget: A's object and FIN are held, then delivered. */
    fake_block_stream(&w.ep, asid);
    NEED(moq_session_write_object(f.sender, a, 0, held, at(&f, 80)) == MOQ_OK);
    NEED(moq_session_close_subgroup(f.sender, a, at(&f, 90)) == MOQ_OK);
    if (!serve(&f, &w, 90)) goto done;
    expect("bridge-owns-object-and-fin", moq_session_has_pending_output(f.sender), false);
    expect("transport-holds-them", moq_transport_bridge_has_outbound_pending(w.bridge), true);
    expect("held-object-not-written", wrote(&w, asid, "held!"), false);
    expect("held-fin-not-written", ops_on(&w, asid, FAKE_OP_WRITE, true), 0);
    expect("held-object-wakes-at-its-deadline", moq_session_next_deadline_us(f.sender),
           at(&f, 180));
    if (!serve(&f, &w, BUDGET_MS)) goto done;
    expect("no-timeout-at-open-plus-budget", drain_events(f.sender), 0);
    expect("no-reset-at-open-plus-budget", timeout_resets(&w, asid) + timeout_resets(&w, bsid), 0);
    expect("unrelated-object-accepted",
           moq_session_write_object(f.sender, b, 0, f.other, at(&f, BUDGET_MS)) == MOQ_OK, true);
    if (!serve(&f, &w, BUDGET_MS)) goto done;
    expect("unrelated-stream-flows", writes_with(&w, bsid, "other"), 1);
    fake_unblock_stream(&w.ep);
    if (!serve(&f, &w, 150)) goto done;
    expect("held-object-delivered-in-budget", writes_with(&w, asid, "held!"), 1);
    expect("held-fin-delivered-after-it", ops_on(&w, asid, FAKE_OP_WRITE, true), 1);
    expect("delivered-stream-never-reset", timeout_resets(&w, asid), 0);
    expect("delivered-payload-released", moq_rcbuf_refcount(held), 1);
    expect("delivered-leaves-no-timer", moq_session_next_deadline_us(f.sender), UINT64_MAX);
    expect("no-terminal-while-delivered", drain_events(f.sender), 0);

    /* Past budget: E's object and FIN are held until the object expires. */
    if (!open_served(&f, &w, 9, 200, &e, &esid, &er, &eheader)) goto done;
    fake_block_stream(&w.ep, esid);
    NEED(moq_session_write_object(f.sender, e, 0, late, at(&f, 210)) == MOQ_OK);
    NEED(moq_session_close_subgroup(f.sender, e, at(&f, 220)) == MOQ_OK);
    if (!serve(&f, &w, 220)) goto done;
    expect("bridge-owns-expiring-object-and-fin", moq_session_has_pending_output(f.sender), false);
    expect("transport-holds-expiring-data", moq_transport_bridge_has_outbound_pending(w.bridge),
           true);
    expect("expiring-object-wakes-at-its-deadline", moq_session_next_deadline_us(f.sender),
           at(&f, 310));
    if (!serve(&f, &w, 200 + BUDGET_MS)) goto done;
    expect("no-timeout-at-its-open-plus-budget", drain_events(f.sender), 0);
    expect("no-reset-at-its-open-plus-budget", timeout_resets(&w, esid), 0);
    expect("unrelated-object-accepted-while-held",
           moq_session_write_object(f.sender, b, 1, f.other, at(&f, 300)) == MOQ_OK, true);
    if (!serve(&f, &w, 300)) goto done;
    expect("unrelated-stream-flows-while-held", writes_with(&w, bsid, "other"), 2);
    if (!serve(&f, &w, 310 - 1)) goto done;
    expect("no-timeout-one-microsecond-early", drain_events(f.sender), 0);
    expect("no-reset-one-microsecond-early", timeout_resets(&w, esid), 0);
    if (!serve(&f, &w, 310)) goto done;
    expect_one_terminal(&f, e, er, 9);
    expect("exactly-one-timeout-reset", timeout_resets(&w, esid), 1);
    expect("reset-keeps-only-the-header", reset_reliable(&w, esid), eheader);
    expect("expired-payload-released", moq_rcbuf_refcount(late), 1);
    expect("nothing-left-for-the-transport", moq_transport_bridge_has_outbound_pending(w.bridge),
           false);
    fake_unblock_stream(&w.ep);
    if (!serve(&f, &w, 311)) goto done;
    expect("expired-object-never-written", wrote(&w, esid, "late!"), false);
    expect("no-fin-after-credit-returns", ops_on(&w, esid, FAKE_OP_WRITE, true), 0);
    expect("still-one-reset", timeout_resets(&w, esid), 1);

    /* The unrelated stream and the session keep working. */
    expect("unrelated-continues",
           moq_session_write_object(f.sender, b, 2, f.other, at(&f, 320)) == MOQ_OK, true);
    expect("unrelated-closes", moq_session_close_subgroup(f.sender, b, at(&f, 330)) == MOQ_OK,
           true);
    if (!serve(&f, &w, 330)) goto done;
    expect("unrelated-all-objects", writes_with(&w, bsid, "other"), 3);
    expect("unrelated-fin", ops_on(&w, bsid, FAKE_OP_WRITE, true), 1);
    if (!open_served(&f, &w, 10, 340, &d, &dsid, &dr, NULL)) goto done;
    NEED(moq_session_write_object(f.sender, d, 0, f.payload, at(&f, 340)) == MOQ_OK);
    NEED(moq_session_close_subgroup(f.sender, d, at(&f, 350)) == MOQ_OK);
    if (!serve(&f, &w, 350)) goto done;
    expect("new-subgroup-after-expiry-delivers", writes_with(&w, dsid, "object"), 1);
    expect("new-subgroup-fin", ops_on(&w, dsid, FAKE_OP_WRITE, true), 1);
    if (!serve(&f, &w, 1000)) goto done;
    expect("no-later-resets", timeout_resets(&w, asid) + timeout_resets(&w, bsid) +
           timeout_resets(&w, dsid), 0);
    expect("no-later-sender-events", drain_events(f.sender), 0);
    expect("no-later-timer", moq_session_next_deadline_us(f.sender), UINT64_MAX);
    expect("bridge-drained", moq_transport_bridge_has_outbound_pending(w.bridge), false);
    expect("recorder-lost-nothing", w.ep.overflowed || w.ep.truncated, false);
    expect("sender-live", moq_session_state(f.sender), MOQ_SESS_ESTABLISHED);
    expect("receiver-live", moq_session_state(f.receiver), MOQ_SESS_ESTABLISHED);
done:
    if (w.bridge) moq_transport_bridge_destroy(w.bridge);
    if (held) moq_rcbuf_decref(held);
    if (late) moq_rcbuf_decref(late);
    cleanup(&f, before);
}

/* The single value stays positive (9.2.2.2, 11.1) and single on every local
 * selector, refused before anything is sent. */
static void zero_refused_row(void)
{
    case_name = "d16/zero-and-two-values-refused";
    unsigned before = failures;
    fixture_t f;
    if (!setup(&f, &k_carriers[0])) goto done;
    moq_session_t *client = f.receiver, *server = f.sender;
    moq_bytes_t part = MOQ_BYTES_LITERAL("d16-timeout");
    const struct { uint32_t flags; uint64_t object_ms, subgroup_ms; } bad[] = {
        {1, 0, 0}, {2, 0, 0}, {3, 0, 0}, {3, 50, 60},
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); ++i) {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init_sized(&sc, sizeof(sc));
        sc.track_namespace = (moq_namespace_t){&part, 1};
        sc.track_name = MOQ_BYTES_LITERAL("w");
        sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        sc.delivery_timeout_flags = bad[i].flags;
        sc.object_delivery_timeout_ms = bad[i].object_ms;
        sc.subgroup_delivery_timeout_ms = bad[i].subgroup_ms;
        moq_subscription_t s;
        expect("subscribe-refused", (uint64_t)(int64_t)moq_session_subscribe(client, &sc,
               moq_simpair_now_us(f.pair), &s), (uint64_t)(int64_t)MOQ_ERR_INVAL);
        moq_subscription_update_cfg_t u;
        moq_subscription_update_cfg_init_sized(&u, sizeof(u));
        u.delivery_timeout_flags = bad[i].flags;
        u.object_delivery_timeout_ms = bad[i].object_ms;
        u.subgroup_delivery_timeout_ms = bad[i].subgroup_ms;
        expect("update-refused", (uint64_t)(int64_t)moq_session_update_subscription(client,
               f.local_sub, &u, moq_simpair_now_us(f.pair)), (uint64_t)(int64_t)MOQ_ERR_INVAL);
        moq_track_delivery_timeouts_cfg_t t;
        moq_track_delivery_timeouts_cfg_init(&t);
        t.has_object_delivery_timeout = (bad[i].flags & 1u) != 0;
        t.object_delivery_timeout_ms = bad[i].object_ms;
        t.has_subgroup_delivery_timeout = (bad[i].flags & 2u) != 0;
        t.subgroup_delivery_timeout_ms = bad[i].subgroup_ms;
        uint8_t out[16];
        size_t len = 0;
        expect("extension-refused", (uint64_t)(int64_t)moq_session_track_properties_add_delivery_timeouts(
               server, NULL, 0, &t, out, sizeof(out), &len), (uint64_t)(int64_t)MOQ_ERR_INVAL);
    }
    moq_action_t a;
    expect("nothing-sent", moq_session_poll_actions(client, &a, 1), 0);
    expect("client-live", moq_session_state(client), MOQ_SESS_ESTABLISHED);
done:
    cleanup(&f, before);
}

int main(void)
{
    for (size_t i = 0; i < N_CARRIERS; ++i) {
        healthy_row(&k_carriers[i]);
        delayed_row(&k_carriers[i]);
    }
    /* One subscription carrier and one publication carrier. */
    streamed_row(&k_carriers[0]);
    streamed_row(&k_carriers[9]);
    after_fin_row(&k_carriers[0]);
    after_fin_row(&k_carriers[9]);
    transport_backlog_row(&k_carriers[0]);
    transport_backlog_row(&k_carriers[9]);
    transport_fin_backlog_row(&k_carriers[0]);
    transport_fin_backlog_row(&k_carriers[9]);
    zero_refused_row();
    printf("%u cases, %u checks, %u failed cases, %u behavioral failures, %u prerequisite failures\n",
           cases, checks, failed_cases, failures - prerequisites, prerequisites);
    return failures ? 1 : 0;
}
