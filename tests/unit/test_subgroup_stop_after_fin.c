/* STOP after an accepted FIN, before transport commit. SimPair establishes
 * real subscription owners; a separate bridge then owns all sender actions.
 * Private reads distinguish core-slot retirement from transport retirement. */
#include <moq/moq.h>
#include <moq/sim.h>
#include "../../core/src/session/session_internal.h"
#include "../../core/src/bridge/transport_bridge_internal.h"
#include "../support/fake_endpoint.h"
#include "test_alloc_support.h"
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

static unsigned checks, failures, prerequisites, cases, failed_cases;
static char case_name[128];

static bool require(bool ok, const char *expr, int line)
{
    ++checks;
    if (!ok) {
        ++failures;
        ++prerequisites;
        fprintf(stderr, "PREREQUISITE %s:%d: %s\n", case_name, line, expr);
    }
    return ok;
}
#define READY(x) do { if (!require(!!(x), #x, __LINE__)) return false; } while (0)
#define NEED(x) do { if (!require(!!(x), #x, __LINE__)) goto done; } while (0)

static void expect(const char *name, uint64_t got, uint64_t want)
{
    ++checks;
    if (got != want) {
        ++failures;
        fprintf(stderr, "FAIL %s/%s: got=%" PRIu64 " expected=%" PRIu64 "\n",
                case_name, name, got, want);
    }
}

typedef struct {
    fake_endpoint_t fake;
    bool fail_reset;
    uint64_t reset_attempts, reset_sid, reset_code, reset_prefix;
} endpoint_t;

static moq_transport_result_t reset_at(void *ctx, uint64_t sid,
                                      uint64_t code, uint64_t reliable)
{
    endpoint_t *ep = ctx;
    ++ep->reset_attempts;
    ep->reset_sid = sid;
    ep->reset_code = code;
    ep->reset_prefix = reliable;
    if (ep->fail_reset) return MOQ_TRANSPORT_ERROR;
    return fake_reset_at(&ep->fake, sid, code, reliable);
}

typedef struct {
    test_alloc_state_t memory;
    moq_alloc_t alloc;
    moq_simpair_t *pair;
    moq_session_t *sender, *receiver;
    moq_subscription_t sub;
    moq_rcbuf_t *payload;
    moq_transport_bridge_t *bridge;
    endpoint_t ep;
    uint64_t now;
} fixture_t;

typedef struct {
    moq_subgroup_handle_t handle;
    moq_stream_ref_t ref;
    uint64_t sid, header_len, group_id;
} group_t;

static void drain_events(moq_session_t *s)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1)) moq_event_cleanup(&e);
}

static bool pump(fixture_t *f)
{
    READY(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    return true;
}

static bool setup(fixture_t *f, moq_version_t version, bool client_sender,
                  bool timed, bool bridge)
{
    memset(f, 0, sizeof(*f));
    f->alloc = test_allocator(&f->memory);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &f->alloc;
    cfg.version = version;
    cfg.initial_now_us = 1000000;
    cfg.max_actions = 4;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 64;
    READY(moq_simpair_create(&cfg, &f->pair) == MOQ_OK);
    READY(moq_simpair_start(f->pair) == MOQ_OK);
    if (!pump(f)) return false;
    f->sender = client_sender ? moq_simpair_client(f->pair) : moq_simpair_server(f->pair);
    f->receiver = client_sender ? moq_simpair_server(f->pair) : moq_simpair_client(f->pair);
    READY(moq_session_state(f->sender) == MOQ_SESS_ESTABLISHED);
    READY(moq_session_state(f->receiver) == MOQ_SESS_ESTABLISHED);
    drain_events(f->sender);
    drain_events(f->receiver);
    f->now = moq_simpair_now_us(f->pair);

    moq_bytes_t part = MOQ_BYTES_LITERAL("stop-after-fin");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init_sized(&sc, sizeof(sc));
    sc.track_namespace = (moq_namespace_t){&part, 1};
    sc.track_name = MOQ_BYTES_LITERAL("v");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t local;
    READY(moq_session_subscribe(f->receiver, &sc, f->now, &local) == MOQ_OK);
    if (!pump(f)) return false;
    moq_event_t e;
    READY(moq_session_poll_events(f->sender, &e, 1) == 1);
    bool request = e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST;
    if (request) f->sub = e.u.subscribe_request.sub;
    moq_event_cleanup(&e);
    READY(request);
    static const uint8_t d16[] = {0x02, 0x40, 0x64};
    static const uint8_t d18[] = {0x06, 0x64};
    moq_accept_subscribe_cfg_t accept;
    moq_accept_subscribe_cfg_init(&accept);
    if (timed) accept.track_properties = version == MOQ_VERSION_DRAFT_16
        ? (moq_bytes_t){d16, sizeof(d16)} : (moq_bytes_t){d18, sizeof(d18)};
    READY(moq_session_accept_subscribe(f->sender, f->sub, &accept, f->now) == MOQ_OK);
    if (!pump(f)) return false;
    READY(moq_session_poll_events(f->receiver, &e, 1) == 1);
    bool accepted = e.kind == MOQ_EVENT_SUBSCRIBE_OK &&
        e.u.subscribe_ok.sub._opaque == local._opaque;
    moq_event_cleanup(&e);
    READY(accepted);
    drain_events(f->sender);
    drain_events(f->receiver);
    READY(moq_rcbuf_create(&f->alloc, (const uint8_t *)"object", 6, &f->payload) == MOQ_OK);
    if (bridge) {
        fake_endpoint_init(&f->ep.fake, 1000, 2000);
        fake_endpoint_enable_reset_at(&f->ep.fake);
        f->ep.fake.vtable.reset_stream_at = reset_at;
        f->ep.fake.vtable.capabilities |= MOQ_TRANSPORT_CAP_COMMIT_REPORT |
                                          MOQ_TRANSPORT_CAP_HOLD_INPUT;
        moq_transport_bridge_cfg_t bc;
        moq_transport_bridge_cfg_init(&bc, &f->alloc);
        READY(moq_transport_bridge_create(&bc, f->sender, &f->ep.fake.vtable,
                                          &f->ep, &f->bridge) == MOQ_OK);
    }
    return true;
}

static void cleanup(fixture_t *f, unsigned before)
{
    expect("recorder-no-overflow", f->ep.fake.overflowed, false);
    expect("recorder-no-truncation", f->ep.fake.truncated, false);
    moq_transport_bridge_destroy(f->bridge);
    moq_simpair_destroy(f->pair);
    if (f->payload) {
        expect("caller-payload-ref", moq_rcbuf_refcount(f->payload), 1);
        moq_rcbuf_decref(f->payload);
    }
    expect("allocator-balanced", (uint64_t)f->memory.balance, 0);
    ++cases;
    if (before != failures) ++failed_cases;
    printf("%s %s\n", before == failures ? "PASS" : "FAIL", case_name);
}

static bool service(fixture_t *f)
{
    READY(moq_transport_bridge_service(f->bridge, f->now) == MOQ_OK);
    return true;
}

static bool open_group(fixture_t *f, uint64_t group_id, group_t *g)
{
    memset(g, 0, sizeof(*g));
    g->group_id = group_id;
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    cfg.group_id = group_id;
    READY(moq_session_open_subgroup(f->sender, f->sub, &cfg, f->now, &g->handle) == MOQ_OK);
    if (f->bridge) {
        size_t before = f->ep.fake.count;
        if (!service(f)) return false;
        READY(f->ep.fake.count == before + 2);
        const fake_op_t *open = &f->ep.fake.ops[before];
        const fake_op_t *header = &f->ep.fake.ops[before + 1];
        READY(open->kind == FAKE_OP_OPEN_UNI && header->kind == FAKE_OP_WRITE);
        READY(open->stream_id == header->stream_id && !header->fin && header->data_len > 0);
        g->sid = open->stream_id;
        g->header_len = header->data_len;
        g->ref = moq_transport_bridge_find_ref(f->bridge, g->sid);
    } else {
        moq_action_t a;
        READY(moq_session_poll_actions(f->sender, &a, 1) == 1);
        bool header = a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.header_len > 0 &&
            !a.u.send_data.fin && !a.u.send_data.payload;
        if (header) {
            g->ref = a.u.send_data.stream_ref;
            g->header_len = a.u.send_data.header_len;
        }
        moq_action_cleanup(&a);
        READY(header);
    }
    READY(g->ref._v != 0);
    return true;
}

static bool close_group(fixture_t *f, const group_t *g)
{
    READY(moq_session_close_subgroup(f->sender, g->handle, f->now) == MOQ_OK);
    if (f->bridge) {
        if (!service(f)) return false;
        bridge_stream_entry_t *e = bridge_find_by_ref(f->bridge, g->ref);
        READY(e && e->awaiting_commit && e->local_send_closed);
    } else {
        moq_action_t a;
        READY(moq_session_poll_actions(f->sender, &a, 1) == 1);
        bool fin = a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.fin &&
            a.u.send_data.stream_ref._v == g->ref._v;
        moq_action_cleanup(&a);
        READY(fin);
    }
    return true;
}

static bool fill_actions(fixture_t *f, const group_t *sibling)
{
    size_t capacity = moq_session_action_capacity(f->sender);
    READY(capacity == 4);
    for (size_t i = 0; i < capacity; ++i)
        READY(moq_session_write_object(f->sender, sibling->handle, i, f->payload, f->now) == MOQ_OK);
    READY(moq_session_action_capacity(f->sender) == 0);
    return true;
}

static unsigned core_actions(fixture_t *f, const group_t *g, uint64_t code)
{
    unsigned resets = 0;
    moq_action_t a;
    while (moq_session_poll_actions(f->sender, &a, 1)) {
        if (a.kind == MOQ_ACTION_RESET_DATA) {
            ++resets;
            expect("core-reset-ref", a.u.reset_data.stream_ref._v, g->ref._v);
            expect("core-reset-code", a.u.reset_data.error_code, code);
            expect("core-header-prefix", a.u.reset_data.reliable_size, g->header_len);
        } else {
            expect("sibling-action-kind", a.kind, MOQ_ACTION_SEND_DATA);
            expect("sibling-not-target", a.u.send_data.stream_ref._v != g->ref._v, true);
        }
        moq_action_cleanup(&a);
    }
    return resets;
}

static unsigned endpoint_resets(fixture_t *f, const group_t *g, uint64_t code,
                                 bool plain)
{
    unsigned resets = 0;
    for (size_t i = 0; i < f->ep.fake.count; ++i) {
        const fake_op_t *op = &f->ep.fake.ops[i];
        if (op->kind == FAKE_OP_RESET || op->kind == FAKE_OP_RESET_AT) {
            ++resets;
            expect("endpoint-reset-ref", op->stream_id, g->sid);
            expect("endpoint-reset-code", op->error_code, code);
            expect("endpoint-reset-kind", op->kind, plain ? FAKE_OP_RESET : FAKE_OP_RESET_AT);
            if (!plain) expect("endpoint-header-prefix", op->reliable_size, g->header_len);
        }
        expect("no-connection-close", op->kind == FAKE_OP_CLOSE, false);
    }
    return resets;
}

static unsigned terminal_events(fixture_t *f, const group_t *g, uint64_t code,
                                  moq_outgoing_subgroup_terminal_cause_t cause)
{
    unsigned count = 0;
    moq_event_t e;
    while (moq_session_poll_events(f->sender, &e, 1)) {
        ++count;
        expect("sender-event-is-terminal", e.kind, MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED);
        expect("terminal-detail-size", e.detail_size,
               MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE);
        if (e.kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED &&
            e.detail_size >= MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE) {
            const moq_outgoing_subgroup_terminated_event_t *d =
                &e.u.outgoing_subgroup_terminated;
            expect("terminal-subgroup", d->subgroup._opaque, g->handle._opaque);
            expect("terminal-owner", d->sub._opaque, f->sub._opaque);
            expect("terminal-no-publication", d->pub._opaque, 0);
            expect("terminal-group", d->group_id, g->group_id);
            expect("terminal-subgroup-id", d->subgroup_id, 0);
            expect("terminal-ref", d->stream_ref._v, g->ref._v);
            expect("terminal-code", d->error_code, code);
            expect("terminal-cause", d->cause, cause);
        }
        moq_event_cleanup(&e);
    }
    return count;
}

/* Recognized STOP retains its cause through output pressure and a later ACK.
 * An ACK observed first still wins. A zero-timeout retired core ref is inert;
 * the bridge tests below exercise its separate retained transport obligation. */
static void core_case(moq_version_t version, bool client_sender, bool timed, unsigned mode)
{
    const char *modes[] = {"stop", "action-full", "commit-first", "timer-first", "blocked-then-commit"};
    snprintf(case_name, sizeof(case_name), "core/d%u/%s/%s/%s", (unsigned)version,
             client_sender ? "client" : "server", timed ? "timed" : "zero", modes[mode]);
    unsigned before = failures;
    fixture_t f;
    group_t g, sibling;
    NEED(setup(&f, version, client_sender, timed, false));
    NEED(open_group(&f, 10, &g));
    NEED(open_group(&f, 11, &sibling));
    NEED(close_group(&f, &g));
    uint64_t deadline = moq_session_next_deadline_us(f.sender);
    unsigned observed_resets = 0;
    if (timed) NEED(deadline != UINT64_MAX);
    if (mode == 1 || mode == 4) {
        NEED(fill_actions(&f, &sibling));
        expect("stop-retained-would-block",
               (uint64_t)moq_session_on_data_stop(f.sender, g.ref, 0x77, f.now),
               (uint64_t)MOQ_ERR_WOULD_BLOCK);
        uint64_t retry = moq_session_next_deadline_us(f.sender);
        expect("retained-stop-bounded-retry", retry > f.now && retry < deadline, true);
        int slot = sg_find_by_stream_ref(f.sender, g.ref);
        NEED(slot >= 0);
        expect("core-stop-retains-terminal", f.sender->subgroups[slot].state,
               MOQ_SG_TERMINAL_OWED);
        expect("core-stop-retains-first-cause", f.sender->subgroups[slot].terminal_cause,
               MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING);
        expect("core-stop-retains-first-code", f.sender->subgroups[slot].terminal_code, 0x77);
        if (mode == 4)
            NEED(moq_session_on_data_committed(f.sender, g.ref, f.now) == MOQ_OK);
        observed_resets += core_actions(&f, &g, 0x77);
        expect("reset-on-action-drain", observed_resets, 1);
    } else if (mode == 2) {
        NEED(moq_session_on_data_committed(f.sender, g.ref, f.now) == MOQ_OK);
    } else if (mode == 3) {
        f.now = deadline;
        NEED(moq_session_tick(f.sender, f.now) == MOQ_OK);
    }
    NEED(moq_session_on_data_stop(f.sender, g.ref, 0x77, f.now) == MOQ_OK);
    NEED(moq_session_on_data_stop(f.sender, g.ref, 0x99, f.now) == MOQ_OK);
    NEED(moq_session_on_data_committed(f.sender, g.ref, f.now) == MOQ_OK);
    unsigned wanted = timed && mode != 2 ? 1 : 0;
    observed_resets += core_actions(&f, &g,
           mode == 3 ? MOQ_RESET_DELIVERY_TIMEOUT : 0x77);
    expect("one-first-terminal-reset", observed_resets, wanted);
    expect("one-first-terminal-event", terminal_events(&f, &g,
           mode == 3 ? MOQ_RESET_DELIVERY_TIMEOUT : 0x77,
           mode == 3 ? MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT
                     : MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING), wanted);
    expect("timer-retired", moq_session_next_deadline_us(f.sender), UINT64_MAX);
    NEED(moq_session_write_object(f.sender, sibling.handle, 20, f.payload, f.now) == MOQ_OK);
    expect("sibling-still-writable", core_actions(&f, &g, 0x77), 0);
    expect("session-still-established", moq_session_state(f.sender), MOQ_SESS_ESTABLISHED);
done:
    cleanup(&f, before);
}

static void bridge_case(moq_version_t version, bool client_sender, bool timed, unsigned mode)
{
    const char *modes[] = {"stop", "action-full", "reset-blocked", "commit-first",
                           "reset-error", "plain-fallback", "timer-first"};
    snprintf(case_name, sizeof(case_name), "bridge/d%u/%s/%s/%s", (unsigned)version,
             client_sender ? "client" : "server", timed ? "timed" : "zero", modes[mode]);
    unsigned before = failures;
    fixture_t f;
    group_t g, sibling, replacement;
    NEED(setup(&f, version, client_sender, timed, true));
    NEED(open_group(&f, 10, &g));
    NEED(close_group(&f, &g));
    NEED(open_group(&f, 11, &sibling));
    if (!timed || version == MOQ_VERSION_DRAFT_16)
        expect("core-slot-already-retired", sg_find_by_stream_ref(f.sender, g.ref) < 0, true);
    fake_endpoint_clear_ops(&f.ep.fake);
    if (mode == 1) NEED(fill_actions(&f, &sibling));
    if (mode == 2) f.ep.fake.block_reset_at = true;
    if (mode == 4) f.ep.fail_reset = true;
    if (mode == 5) f.ep.fake.reset_at_unavailable = true;
    if (mode == 6) {
        f.now = moq_session_next_deadline_us(f.sender);
        NEED(f.now != UINT64_MAX);
        NEED(moq_session_tick(f.sender, f.now) == MOQ_OK);
    }
    if (mode == 3) {
        NEED(moq_transport_bridge_on_local_stream_committed(f.bridge, g.sid, f.now) == MOQ_OK);
        expect("commit-retired-map", moq_transport_bridge_find_ref(f.bridge, g.sid)._v, 0);
    }
    moq_result_t rc = moq_transport_bridge_on_peer_stop_sending(f.bridge, g.sid, 0x77, f.now);
    expect("stop-result", (uint64_t)rc, (uint64_t)(mode == 1 ? MOQ_ERR_WOULD_BLOCK : MOQ_OK));
    if (version != MOQ_VERSION_DRAFT_16 && mode != 1)
        expect("stop-retires-subgroup-timer", moq_session_next_deadline_us(f.sender), UINT64_MAX);
    rc = moq_transport_bridge_on_peer_stop_sending(f.bridge, g.sid, 0x99, f.now);
    expect("duplicate-stop-result", (uint64_t)rc, (uint64_t)(mode == 1 ? MOQ_ERR_WOULD_BLOCK : MOQ_OK));
    /* An ACK after retained STOP cannot erase the pending reset action/fact. */
    NEED(moq_transport_bridge_on_local_stream_committed(f.bridge, g.sid, f.now) == MOQ_OK);
    if (mode != 3)
        expect("stop-map-before-dispatch", moq_transport_bridge_find_ref(f.bridge, g.sid)._v, g.ref._v);
    if (mode == 4) {
        expect("native-error-not-success", (uint64_t)moq_transport_bridge_service(f.bridge, f.now),
               (uint64_t)MOQ_ERR_INTERNAL);
        expect("native-error-fatal", moq_transport_bridge_is_fatal(f.bridge), true);
        expect("native-error-no-accepted-reset", endpoint_resets(&f, &g, 0x77, false), 0);
        goto done;
    }
    NEED(service(&f));
    if (mode == 2) {
        expect("map-retained-until-reset-accepted", moq_transport_bridge_find_ref(f.bridge, g.sid)._v, g.ref._v);
        expect("blocked-reset-no-effect", endpoint_resets(&f, &g, 0x77, false), 0);
        NEED(moq_transport_bridge_on_local_stream_committed(f.bridge, g.sid, f.now) == MOQ_OK);
        NEED(moq_transport_bridge_on_peer_stop_sending(f.bridge, g.sid, 0x88, f.now) == MOQ_OK);
        NEED(moq_session_write_object(f.sender, sibling.handle, 20, f.payload, f.now) == MOQ_OK);
        NEED(service(&f));
        unsigned payloads = 0;
        for (size_t i = 0; i < f.ep.fake.count; ++i) {
            const fake_op_t *op = &f.ep.fake.ops[i];
            if (op->kind == FAKE_OP_WRITE && op->stream_id == sibling.sid &&
                op->data_len == 6 && memcmp(op->data, "object", 6) == 0) ++payloads;
        }
        expect("sibling-progress-while-reset-blocked", payloads, 1);
        f.ep.fake.block_reset_at = false;
        NEED(service(&f));
    }
    expect("first-terminal-reset-count", endpoint_resets(&f, &g,
           mode == 6 ? MOQ_RESET_DELIVERY_TIMEOUT : 0x77, mode == 5), mode == 3 ? 0 : 1);
    expect("terminal-map-retired", moq_transport_bridge_find_ref(f.bridge, g.sid)._v, 0);
    /* Reuse the bounded map/core pools, then deliver stale identities. */
    fake_endpoint_clear_ops(&f.ep.fake);
    NEED(open_group(&f, 12, &replacement));
    NEED(replacement.ref._v != g.ref._v && replacement.sid != g.sid);
    NEED(moq_transport_bridge_on_local_stream_committed(f.bridge, g.sid, f.now) == MOQ_OK);
    NEED(moq_transport_bridge_on_peer_stream_reset(f.bridge, g.sid, 0x66, f.now) == MOQ_OK);
    NEED(moq_transport_bridge_on_peer_stop_sending(f.bridge, g.sid, 0x55, f.now) == MOQ_OK);
    NEED(moq_session_on_data_stop(f.sender, g.ref, 0x44, f.now) == MOQ_OK);
    NEED(moq_session_write_object(f.sender, replacement.handle, 0, f.payload, f.now) == MOQ_OK);
    NEED(moq_session_write_object(f.sender, sibling.handle, 21, f.payload, f.now) == MOQ_OK);
    NEED(service(&f));
    expect("stale-input-no-reset", endpoint_resets(&f, &g, 0x77, false), 0);
    expect("replacement-map-unchanged", moq_transport_bridge_find_ref(f.bridge, replacement.sid)._v,
           replacement.ref._v);
    expect("session-still-established", moq_session_state(f.sender), MOQ_SESS_ESTABLISHED);
done:
    cleanup(&f, before);
}

static unsigned queued_resets(const fixture_t *f, const group_t *g)
{
    unsigned queued = 0;
    for (size_t i = f->sender->action_head; i < f->sender->action_tail; ++i) {
        const moq_action_t *a = &f->sender->actions[i % f->sender->action_cap];
        if (a->kind == MOQ_ACTION_RESET_DATA &&
            a->u.reset_data.stream_ref._v == g->ref._v) {
            ++queued;
            expect("owned-reset-code", a->u.reset_data.error_code, MOQ_RESET_DELIVERY_TIMEOUT);
            expect("owned-reset-prefix", a->u.reset_data.reliable_size, g->header_len);
        }
    }
    return queued;
}

static void order_case(moq_version_t version, bool client, unsigned mode)
{
    const char *names[] = {"ack-first", "timer-reset-first", "queued-timer-then-ack"};
    snprintf(case_name, sizeof(case_name), "order/d%u/%s/%s",
             (unsigned)version, client ? "client" : "server", names[mode]);
    unsigned before = failures;
    fixture_t f;
    group_t g, sibling;
    NEED(setup(&f, version, client, true, true));
    NEED(open_group(&f, 16384, &g));
    NEED(close_group(&f, &g));
    NEED(open_group(&f, 16385, &sibling));
    fake_endpoint_clear_ops(&f.ep.fake);
    uint64_t deadline = moq_session_next_deadline_us(f.sender);
    NEED(deadline != UINT64_MAX);
    if (mode == 0)
        NEED(moq_transport_bridge_on_local_stream_committed(f.bridge, g.sid, f.now) == MOQ_OK);
    f.now = deadline;
    NEED(moq_session_tick(f.sender, f.now) == MOQ_OK);
    expect("queued-reset-before-ack", queued_resets(&f, &g), mode == 0 ? 0 : 1);
    if (mode == 2)
        NEED(moq_transport_bridge_on_local_stream_committed(f.bridge, g.sid, f.now) == MOQ_OK);
    NEED(service(&f));
    expect("owned-reset-reaches-endpoint", endpoint_resets(&f, &g,
           MOQ_RESET_DELIVERY_TIMEOUT, false), mode == 0 ? 0 : 1);
    expect("owned-timeout-event-count", terminal_events(&f, &g,
           MOQ_RESET_DELIVERY_TIMEOUT, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT),
           mode == 0 ? 0 : 1);
    expect("mapping-retires", moq_transport_bridge_find_ref(f.bridge, g.sid)._v, 0);
    expect("timer-retires", moq_session_next_deadline_us(f.sender), UINT64_MAX);
    NEED(moq_session_write_object(f.sender, sibling.handle, 0, f.payload, f.now) == MOQ_OK);
    NEED(service(&f));
    expect("sibling-session-live", moq_session_state(f.sender), MOQ_SESS_ESTABLISHED);
done:
    cleanup(&f, before);
}

static unsigned retained_resets(const fixture_t *f, const group_t *g)
{
    unsigned retained = 0;
    for (uint32_t i = 0; i < f->bridge->pending_count; ++i) {
        const bridge_pending_item_t *p = &f->bridge->pending[i];
        if (p->kind != PENDING_RESET_STREAM || p->stream_ref._v != g->ref._v) continue;
        ++retained;
        expect("retained-reset-is-uni", p->uni_reset, true);
        expect("retained-reset-has-sid", p->has_stream_id, true);
        expect("retained-reset-sid", p->stream_id, g->sid);
        expect("retained-reset-code", p->error_code, MOQ_RESET_DELIVERY_TIMEOUT);
        expect("retained-reset-prefix", p->reliable_size, g->header_len);
    }
    return retained;
}

static unsigned sibling_payloads(const fixture_t *f, const group_t *sibling)
{
    unsigned payloads = 0;
    for (size_t i = 0; i < f->ep.fake.count; ++i) {
        const fake_op_t *op = &f->ep.fake.ops[i];
        if (op->kind == FAKE_OP_WRITE && op->stream_id == sibling->sid &&
            op->data_len == 6 && memcmp(op->data, "object", 6) == 0) ++payloads;
    }
    return payloads;
}

static void order_boundary_case(moq_version_t version, bool client, unsigned mode)
{
    enum { QUEUED_BLOCKED, RETAINED_BLOCKED, ADVANCED_QUEUED, RESET_ERROR,
           RETRY_ERROR, ACK_AT_DEADLINE, REFUSED_TIMER_THEN_ACK };
    const char *names[] = {"queued-reset-blocked", "retained-reset-blocked", "advanced-queued",
                          "reset-error", "retry-error", "ack-at-deadline",
                          "refused-timer-then-ack"};
    snprintf(case_name, sizeof(case_name), "order-boundary/d%u/%s/%s",
             (unsigned)version, client ? "client" : "server", names[mode]);
    unsigned before = failures;
    fixture_t f;
    group_t g, sibling;
    NEED(setup(&f, version, client, true, true));
    NEED(open_group(&f, 16384, &g));
    NEED(close_group(&f, &g));
    NEED(open_group(&f, 16385, &sibling));
    fake_endpoint_clear_ops(&f.ep.fake);
    uint64_t deadline = moq_session_next_deadline_us(f.sender);
    NEED(deadline != UINT64_MAX && f.now < deadline);
    expect("no-reset-before-deadline", queued_resets(&f, &g), 0);
    f.now = deadline;
    if (mode == ACK_AT_DEADLINE || mode == REFUSED_TIMER_THEN_ACK) {
        if (mode == REFUSED_TIMER_THEN_ACK) {
            NEED(fill_actions(&f, &sibling));
            expect("timer-retained-under-pressure", (uint64_t)moq_session_tick(f.sender, f.now),
                   (uint64_t)MOQ_ERR_WOULD_BLOCK);
        }
        expect("no-reset-before-commit-input", queued_resets(&f, &g), 0);
        NEED(moq_transport_bridge_on_local_stream_committed(f.bridge, g.sid, f.now) == MOQ_OK);
        expect("commit-input-advances-to-deadline", f.sender->last_now_us, deadline);
        expect("no-reset-enqueued-before-drain", queued_resets(&f, &g), 0);
        expect("first-cause-controls-mapping", moq_transport_bridge_find_ref(f.bridge, g.sid)._v,
               mode == REFUSED_TIMER_THEN_ACK ? g.ref._v : 0);
        NEED(service(&f));
        NEED(moq_session_tick(f.sender, f.now) == MOQ_OK);
        NEED(service(&f));
        expect("recognized-timeout-survives-ack", endpoint_resets(&f, &g,
               MOQ_RESET_DELIVERY_TIMEOUT, false), mode == REFUSED_TIMER_THEN_ACK ? 1 : 0);
        expect("exact-reset-attempts", f.ep.reset_attempts,
               mode == REFUSED_TIMER_THEN_ACK ? 1 : 0);
        expect("settled-map-retired", moq_transport_bridge_find_ref(f.bridge, g.sid)._v, 0);
        expect("settlement-retires-timer", moq_session_next_deadline_us(f.sender), UINT64_MAX);
        if (mode == REFUSED_TIMER_THEN_ACK)
            expect("action-pressure-sibling-progress", sibling_payloads(&f, &sibling), 4);
        goto progress;
    }

    NEED(moq_session_tick(f.sender, f.now) == MOQ_OK);
    expect("timer-owns-queued-reset", queued_resets(&f, &g), 1);
    expect("not-yet-bridge-retained", retained_resets(&f, &g), 0);
    expect("not-yet-endpoint-attempted", f.ep.reset_attempts, 0);
    if (mode == ADVANCED_QUEUED) {
        NEED(moq_session_write_object(f.sender, sibling.handle, 0, f.payload, f.now) == MOQ_OK);
        int slot = sg_find_by_stream_ref(f.sender, g.ref);
        expect("queued-action-defers-reap", slot >= 0 && f.sender->subgroups[slot].state == MOQ_SG_RESETTING, true);
        expect("reset-survives-advance", queued_resets(&f, &g), 1);
    }
    bool blocked = mode == QUEUED_BLOCKED || mode == RETAINED_BLOCKED || mode == RETRY_ERROR;
    f.ep.fake.block_reset_at = blocked;
    if (mode == RETAINED_BLOCKED) {
        NEED(service(&f));
        expect("reset-moved-out-of-session", queued_resets(&f, &g), 0);
        expect("one-retained-reset-before-ack", retained_resets(&f, &g), 1);
        expect("blocked-attempt-before-ack", f.ep.reset_attempts, 1);
    }
    for (unsigned ack = 0; ack < 2; ++ack)
        NEED(moq_transport_bridge_on_local_stream_committed(f.bridge, g.sid, f.now) == MOQ_OK);
    expect("late-ack-keeps-owned-map", moq_transport_bridge_find_ref(f.bridge, g.sid)._v, g.ref._v);
    expect("late-ack-no-native-success", endpoint_resets(&f, &g,
           MOQ_RESET_DELIVERY_TIMEOUT, false), 0);
    if (mode == RESET_ERROR) f.ep.fail_reset = true;
    if (mode != RESET_ERROR) NEED(service(&f));
    if (blocked) {
        expect("no-queued-reset-after-dispatch", queued_resets(&f, &g), 0);
        expect("one-bridge-retained-reset", retained_resets(&f, &g), 1);
        expect("blocked-reset-not-accepted", endpoint_resets(&f, &g,
               MOQ_RESET_DELIVERY_TIMEOUT, false), 0);
        expect("blocked-map-still-owned", moq_transport_bridge_find_ref(f.bridge, g.sid)._v, g.ref._v);
        bridge_stream_entry_t *entry = bridge_find_by_ref(f.bridge, g.ref);
        expect("blocked-map-reset-state", entry && entry->fin_reset_pending && !entry->awaiting_commit, true);
        uint64_t attempts = f.ep.reset_attempts;
        expect("blocked-reset-attempted", attempts > 0, true);
        for (unsigned ack = 0; ack < 2; ++ack)
            NEED(moq_transport_bridge_on_local_stream_committed(f.bridge, g.sid, f.now) == MOQ_OK);
        expect("late-acks-not-reset-attempts", f.ep.reset_attempts, attempts);
        NEED(moq_session_write_object(f.sender, sibling.handle, 1, f.payload, f.now) == MOQ_OK);
        expect("core-reaped-after-action-drain", sg_find_by_stream_ref(f.sender, g.ref) < 0, true);
        NEED(service(&f));
        expect("sibling-progress-during-blocked-reset", sibling_payloads(&f, &sibling), 1);
        expect("one-retained-reset-after-retry", retained_resets(&f, &g), 1);
        f.ep.fake.block_reset_at = false;
        f.ep.fail_reset = mode == RETRY_ERROR;
    }
    if (mode == RESET_ERROR || mode == RETRY_ERROR) {
        expect("native-refusal-not-success", (uint64_t)moq_transport_bridge_service(f.bridge, f.now),
               (uint64_t)MOQ_ERR_INTERNAL);
        expect("native-refusal-fatal", moq_transport_bridge_is_fatal(f.bridge), true);
        expect("native-refusal-was-attempted", f.ep.reset_attempts > 0, true);
        expect("refused-reset-sid", f.ep.reset_sid, g.sid);
        expect("refused-reset-code", f.ep.reset_code, MOQ_RESET_DELIVERY_TIMEOUT);
        expect("refused-reset-prefix", f.ep.reset_prefix, g.header_len);
        expect("native-refusal-no-accepted-reset", endpoint_resets(&f, &g,
               MOQ_RESET_DELIVERY_TIMEOUT, false), 0);
        expect("native-refusal-one-local-terminal", terminal_events(&f, &g,
               MOQ_RESET_DELIVERY_TIMEOUT, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT), 1);
        goto done;
    }
    NEED(service(&f));
    expect("one-accepted-reset", endpoint_resets(&f, &g, MOQ_RESET_DELIVERY_TIMEOUT, false), 1);
    expect("accepted-reset-retires-map", moq_transport_bridge_find_ref(f.bridge, g.sid)._v, 0);
    expect("accepted-reset-no-retained-item", retained_resets(&f, &g), 0);
    expect("accepted-reset-no-timer", moq_session_next_deadline_us(f.sender), UINT64_MAX);
    uint64_t attempts = f.ep.reset_attempts;
    for (unsigned ack = 0; ack < 2; ++ack)
        NEED(moq_transport_bridge_on_local_stream_committed(f.bridge, g.sid, f.now) == MOQ_OK);
    NEED(service(&f));
    expect("post-accept-acks-inert", f.ep.reset_attempts, attempts);
progress:
    expect("exact-local-timeout-event-count", terminal_events(&f, &g,
           MOQ_RESET_DELIVERY_TIMEOUT, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT),
           mode == ACK_AT_DEADLINE ? 0 : 1);
    fake_endpoint_clear_ops(&f.ep.fake);
    NEED(moq_session_write_object(f.sender, sibling.handle, 20, f.payload, f.now) == MOQ_OK);
    NEED(service(&f));
    expect("sibling-payload-delivered", sibling_payloads(&f, &sibling), 1);
    expect("sibling-map-live", moq_transport_bridge_find_ref(f.bridge, sibling.sid)._v, sibling.ref._v);
    expect("sibling-session-live", moq_session_state(f.sender), MOQ_SESS_ESTABLISHED);
done:
    cleanup(&f, before);
}

int main(void)
{
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); ++v) {
        for (unsigned client = 0; client < 2; ++client) {
            core_case(versions[v], client != 0, false, 0);
            if (v > 0) {
                for (unsigned mode = 0; mode < 5; ++mode)
                    core_case(versions[v], client != 0, true, mode);
            }
            for (unsigned timed = 0; timed < 2; ++timed) {
                for (unsigned mode = 0; mode < 6; ++mode)
                    bridge_case(versions[v], client != 0, timed != 0, mode);
            }
            if (v > 0) {
                bridge_case(versions[v], client != 0, true, 6);
                for (unsigned mode = 0; mode < 3; ++mode)
                    order_case(versions[v], client != 0, mode);
                for (unsigned mode = 0; mode < 7; ++mode)
                    order_boundary_case(versions[v], client != 0, mode);
            }
        }
    }
    printf("%u cases, %u failed cases, %u assertions, %u failures, %u prerequisite failures\n",
           cases, failed_cases, checks, failures, prerequisites);
    return failures ? 1 : 0;
}
