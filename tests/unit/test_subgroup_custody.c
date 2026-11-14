/* Outgoing subgroup identity while its timed output is in the transport
 * bridge's custody, under fake time.
 *
 * A subgroup whose FIN has left the session can still have objects with a
 * delivery deadline held by the bridge (the transport took nothing more on
 * the stream). Until the transport accepts that FIN, the session keeps the
 * subgroup's entry, so an expiry or STOP of that output commits the one
 * MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED it promises, with the exact
 * identity. Accepted FIN, a reset, the owner's cancellation and the bridge
 * dropping its storage (destroyed, or fatal while still registered) end that
 * custody; none of them invents a terminal or erases one already recognized,
 * and a negotiated subgroup timer keeps its own commit/timer rule.
 *
 * Every row drives a real bridge over the fake endpoint after SimPair
 * established the session. The object clock starts at the header on
 * draft-16 and draft-22 and at the first payload byte on draft-18.
 */
#include <moq/moq.h>
#include <moq/sim.h>
#include <moq/publisher.h>
#include "../../core/src/session/session_internal.h"
#include "../support/fake_endpoint.h"
#include "test_alloc_support.h"
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

static unsigned checks, failures, prerequisites, cases, failed_cases;
static const char *case_name;
static const uint64_t MS = 1000;

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

static unsigned draft(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_16 ? 16u : v == MOQ_VERSION_DRAFT_18 ? 18u : 22u;
}

typedef struct fx_cfg {
    moq_version_t version;
    bool          publication;   /* the client PUBLISHes; else it SUBSCRIBEs */
    uint64_t      object_ms;     /* publisher's object timeout, 0 = none */
    uint64_t      subgroup_ms;   /* publisher's subgroup timeout, 0 = none */
    uint32_t      pool;          /* sender's subgroup pool (subscription only) */
    uint32_t      max_events;
    bool          commit_report; /* the endpoint reports all data committed */
} fx_cfg_t;

typedef struct fx {
    fx_cfg_t cfg;
    test_alloc_state_t memory;
    moq_alloc_t alloc;
    moq_simpair_t *pair;
    moq_session_t *sender, *receiver;
    moq_subscription_t sub, local_sub;
    moq_publication_t pub, peer_pub;
    moq_rcbuf_t *payload, *other;
    fake_endpoint_t ep;
    moq_transport_bridge_t *bridge;
    uint64_t base;
} fx_t;

static size_t drain(moq_session_t *s)
{
    moq_event_t e;
    size_t n = 0;
    while (moq_session_poll_events(s, &e, 1)) { ++n; moq_event_cleanup(&e); }
    return n;
}

static bool take(moq_session_t *s, moq_event_kind_t kind, moq_event_t *out)
{
    READY(moq_session_poll_events(s, out, 1) == 1);
    if (out->kind != kind) { moq_event_cleanup(out); READY(out->kind == kind); }
    return true;
}

static bool pump(fx_t *f)
{
    READY(moq_simpair_run_until_quiescent(f->pair, 64, NULL) == MOQ_OK);
    return true;
}

static uint64_t at(const fx_t *f, uint64_t ms) { return f->base + ms * MS; }

static bool attach(fx_t *f)
{
    fake_endpoint_init(&f->ep, 1000, 2000);
    fake_endpoint_enable_reset_at(&f->ep);
    f->ep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    if (f->cfg.commit_report) f->ep.vtable.capabilities |= MOQ_TRANSPORT_CAP_COMMIT_REPORT;
    moq_transport_bridge_cfg_t bc;
    moq_transport_bridge_cfg_init(&bc, &f->alloc);
    READY(moq_transport_bridge_create(&bc, f->sender, &f->ep.vtable, &f->ep, &f->bridge) == MOQ_OK);
    return true;
}

static bool setup(fx_t *f, const fx_cfg_t *cfg)
{
    memset(f, 0, sizeof(*f));
    f->cfg = *cfg;
    f->alloc = test_allocator(&f->memory);
    moq_simpair_cfg_t sc = MOQ_SIMPAIR_CFG_INIT;
    sc.alloc = &f->alloc;
    sc.version = cfg->version;
    sc.initial_now_us = 1000000;
    sc.client_send_request_capacity = sc.server_send_request_capacity = true;
    sc.client_initial_request_capacity = sc.server_initial_request_capacity = 64;
    sc.server_max_open_subgroups = cfg->pool;
    sc.max_events = cfg->max_events;
    READY(moq_simpair_create(&sc, &f->pair) == MOQ_OK);
    READY(moq_simpair_start(f->pair) == MOQ_OK);
    if (!pump(f)) return false;
    moq_session_t *client = moq_simpair_client(f->pair), *server = moq_simpair_server(f->pair);
    READY(moq_session_state(client) == MOQ_SESS_ESTABLISHED &&
          moq_session_state(server) == MOQ_SESS_ESTABLISHED);
    (void)drain(client); (void)drain(server);
    f->sender = cfg->publication ? client : server;
    f->receiver = cfg->publication ? server : client;

    uint8_t props[32];
    size_t props_len = 0;
    if (cfg->object_ms || cfg->subgroup_ms) {
        moq_track_delivery_timeouts_cfg_t t;
        moq_track_delivery_timeouts_cfg_init(&t);
        t.has_object_delivery_timeout = cfg->object_ms != 0;
        t.object_delivery_timeout_ms = cfg->object_ms;
        t.has_subgroup_delivery_timeout = cfg->subgroup_ms != 0;
        t.subgroup_delivery_timeout_ms = cfg->subgroup_ms;
        READY(moq_session_track_properties_add_delivery_timeouts(f->sender, NULL, 0, &t,
                  props, sizeof(props), &props_len) == MOQ_OK);
    }
    moq_bytes_t properties = {props, props_len};
    moq_bytes_t part = MOQ_BYTES_LITERAL("custody");
    uint64_t now = moq_simpair_now_us(f->pair);
    moq_event_t e;
    if (cfg->publication) {
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
        READY(moq_session_accept_publish(server, f->peer_pub, &ac, now) == MOQ_OK);
        if (!pump(f) || !take(client, MOQ_EVENT_PUBLISH_OK, &e)) return false;
        moq_event_cleanup(&e);
    } else {
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init_sized(&scfg, sizeof(scfg));
        scfg.track_namespace = (moq_namespace_t){&part, 1};
        scfg.track_name = MOQ_BYTES_LITERAL("v");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        READY(moq_session_subscribe(client, &scfg, now, &f->local_sub) == MOQ_OK);
        if (!pump(f) || !take(server, MOQ_EVENT_SUBSCRIBE_REQUEST, &e)) return false;
        f->sub = e.u.subscribe_request.sub;
        moq_event_cleanup(&e);
        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        ac.track_properties = properties;
        READY(moq_session_accept_subscribe(server, f->sub, &ac, now) == MOQ_OK);
        if (!pump(f) || !take(client, MOQ_EVENT_SUBSCRIBE_OK, &e)) return false;
        moq_event_cleanup(&e);
    }
    READY(drain(client) == 0 && drain(server) == 0);
    READY(moq_rcbuf_create(&f->alloc, (const uint8_t *)"object", 6, &f->payload) == MOQ_OK);
    READY(moq_rcbuf_create(&f->alloc, (const uint8_t *)"other", 5, &f->other) == MOQ_OK);
    f->base = moq_simpair_now_us(f->pair);
    return attach(f);
}

static void cleanup(fx_t *f, unsigned before)
{
    if (f->bridge) moq_transport_bridge_destroy(f->bridge);
    if (f->sender) {
        moq_action_t a;
        while (moq_session_poll_actions(f->sender, &a, 1)) moq_action_cleanup(&a);
    }
    if (f->payload) {
        expect("final-payload-ref", moq_rcbuf_refcount(f->payload), 1);
        moq_rcbuf_decref(f->payload);
    }
    if (f->other) {
        expect("final-other-ref", moq_rcbuf_refcount(f->other), 1);
        moq_rcbuf_decref(f->other);
    }
    if (f->pair) moq_simpair_destroy(f->pair);
    expect("recorder-lost-nothing", f->ep.overflowed || f->ep.truncated, false);
    expect("allocation-balance", (uint64_t)f->memory.balance, 0);
    ++cases;
    if (failures != before) ++failed_cases;
    printf("%s %s\n", failures == before ? "PASS" : "FAIL", case_name);
}

static bool serve_abs(fx_t *f, uint64_t now_us)
{
    READY(moq_session_tick(f->sender, now_us) == MOQ_OK);
    READY(moq_transport_bridge_service(f->bridge, now_us) == MOQ_OK);
    READY(!moq_transport_bridge_is_fatal(f->bridge));
    return true;
}

static bool serve(fx_t *f, uint64_t ms)
{
    return serve_abs(f, at(f, ms));
}

static moq_result_t open_sg(fx_t *f, uint64_t group, uint64_t ms, moq_subgroup_handle_t *sg)
{
    moq_subgroup_cfg_t c;
    moq_subgroup_cfg_init(&c);
    c.group_id = group;
    c.publisher_priority = 128;
    return f->cfg.publication
        ? moq_session_open_pub_subgroup(f->sender, f->pub, &c, at(f, ms), sg)
        : moq_session_open_subgroup(f->sender, f->sub, &c, at(f, ms), sg);
}

/* Open a subgroup and let the bridge carry its header. */
static bool open_served(fx_t *f, uint64_t group, uint64_t ms, moq_subgroup_handle_t *sg,
                        uint64_t *sid, moq_stream_ref_t *ref, uint64_t *header)
{
    size_t first = f->ep.count;
    READY(open_sg(f, group, ms, sg) == MOQ_OK);
    if (!serve(f, ms)) return false;
    *sid = UINT64_MAX;
    for (size_t i = first; i < f->ep.count && *sid == UINT64_MAX; ++i)
        if (f->ep.ops[i].kind == FAKE_OP_OPEN_UNI) *sid = f->ep.ops[i].stream_id;
    READY(*sid != UINT64_MAX);
    for (size_t i = first; header && i < f->ep.count; ++i)
        if (f->ep.ops[i].kind == FAKE_OP_WRITE && f->ep.ops[i].stream_id == *sid)
            *header = f->ep.ops[i].data_len;
    *ref = moq_transport_bridge_find_ref(f->bridge, *sid);
    READY(ref->_v != 0);
    return true;
}

static unsigned writes_with(const fx_t *f, uint64_t sid, const char *bytes)
{
    size_t n = strlen(bytes);
    unsigned count = 0;
    for (size_t i = 0; i < f->ep.count; ++i) {
        const fake_op_t *o = &f->ep.ops[i];
        if (o->kind != FAKE_OP_WRITE || o->stream_id != sid || o->data_len < n) continue;
        for (size_t j = 0; j + n <= o->data_len; ++j)
            if (memcmp(o->data + j, bytes, n) == 0) { ++count; break; }
    }
    return count;
}

static unsigned fins_on(const fx_t *f, uint64_t sid)
{
    unsigned n = 0;
    for (size_t i = 0; i < f->ep.count; ++i)
        if (f->ep.ops[i].kind == FAKE_OP_WRITE && f->ep.ops[i].stream_id == sid &&
            f->ep.ops[i].fin) ++n;
    return n;
}

static unsigned resets_on(const fx_t *f, uint64_t sid, uint64_t code)
{
    unsigned n = 0;
    for (size_t i = 0; i < f->ep.count; ++i)
        if ((f->ep.ops[i].kind == FAKE_OP_RESET || f->ep.ops[i].kind == FAKE_OP_RESET_AT) &&
            f->ep.ops[i].stream_id == sid && f->ep.ops[i].error_code == code) ++n;
    return n;
}

static unsigned any_resets_on(const fx_t *f, uint64_t sid)
{
    unsigned n = 0;
    for (size_t i = 0; i < f->ep.count; ++i)
        if ((f->ep.ops[i].kind == FAKE_OP_RESET || f->ep.ops[i].kind == FAKE_OP_RESET_AT) &&
            f->ep.ops[i].stream_id == sid) ++n;
    return n;
}

static uint64_t reliable_on(const fx_t *f, uint64_t sid)
{
    for (size_t i = 0; i < f->ep.count; ++i)
        if (f->ep.ops[i].kind == FAKE_OP_RESET_AT && f->ep.ops[i].stream_id == sid)
            return f->ep.ops[i].reliable_size;
    return UINT64_MAX;
}

/* RESET_DATA actions the sender has queued for `ref`, not yet polled. */
static unsigned queued_resets(const fx_t *f, moq_stream_ref_t ref)
{
    unsigned n = 0;
    const moq_session_t *s = f->sender;
    for (size_t j = s->action_head; j < s->action_tail; ++j) {
        const moq_action_t *a = &s->actions[j % s->action_cap];
        if (a->kind == MOQ_ACTION_RESET_DATA && a->u.reset_data.stream_ref._v == ref._v) ++n;
    }
    return n;
}

/* Live entries in the sender's subgroup pool, as they stand now. */
static size_t occupied(const fx_t *f)
{
    size_t n = 0;
    for (size_t i = 0; i < f->sender->sg_cap; ++i)
        if (f->sender->subgroups[i].state != MOQ_SG_FREE) ++n;
    return n;
}

typedef struct terminals {
    unsigned count, other;
    moq_outgoing_subgroup_terminated_event_t last;
} terminals_t;

static terminals_t sender_events(fx_t *f)
{
    terminals_t t;
    memset(&t, 0, sizeof(t));
    moq_event_t e;
    while (moq_session_poll_events(f->sender, &e, 1)) {
        if (e.kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED &&
            e.detail_size >= MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE) {
            ++t.count;
            t.last = e.u.outgoing_subgroup_terminated;
        } else {
            ++t.other;
        }
        moq_event_cleanup(&e);
    }
    return t;
}

static void accumulate(terminals_t *acc, terminals_t t)
{
    acc->other += t.other;
    if (t.count) acc->last = t.last;
    acc->count += t.count;
}

/* Exactly one terminal across `acc`, for this subgroup and cause/code. */
static void check_terminal(fx_t *f, const terminals_t *acc, moq_subgroup_handle_t sg,
                           moq_stream_ref_t ref, uint64_t group, uint32_t cause,
                           uint64_t code)
{
    expect("exactly-one-terminal", acc->count, 1);
    if (acc->count != 1) return;
    expect("terminal-subgroup", acc->last.subgroup._opaque, sg._opaque);
    expect("terminal-subscription", acc->last.sub._opaque, f->sub._opaque);
    expect("terminal-publication", acc->last.pub._opaque, f->pub._opaque);
    expect("terminal-stream", acc->last.stream_ref._v, ref._v);
    expect("terminal-group", acc->last.group_id, group);
    expect("terminal-cause", acc->last.cause, cause);
    expect("terminal-code", acc->last.error_code, code);
}

/* Exactly one terminal, for exactly this subgroup, with this cause/code. */
static void expect_terminal_ex(fx_t *f, moq_subgroup_handle_t sg, moq_stream_ref_t ref,
                               uint64_t group, uint32_t cause, uint64_t code, bool others_ok)
{
    terminals_t t = sender_events(f);
    expect("exactly-one-terminal", t.count, 1);
    if (!others_ok) expect("no-other-sender-events", t.other, 0);
    if (t.count != 1) return;
    expect("terminal-subgroup", t.last.subgroup._opaque, sg._opaque);
    expect("terminal-subscription", t.last.sub._opaque, f->sub._opaque);
    expect("terminal-publication", t.last.pub._opaque, f->pub._opaque);
    expect("terminal-stream", t.last.stream_ref._v, ref._v);
    expect("terminal-group", t.last.group_id, group);
    expect("terminal-cause", t.last.cause, cause);
    expect("terminal-code", t.last.error_code, code);
}

static void expect_one_terminal(fx_t *f, moq_subgroup_handle_t sg, moq_stream_ref_t ref,
                                uint64_t group, uint32_t cause, uint64_t code)
{
    expect_terminal_ex(f, sg, ref, group, cause, code, false);
}

static void expect_no_terminal(fx_t *f, const char *name)
{
    terminals_t t = sender_events(f);
    expect(name, t.count, 0);
}

/* A streamed object whose clock origin differs by draft: header at `h`,
 * its payload at `p`, the FIN at `fin`, all held by the transport. Returns
 * the deadline the object must expire at. */
static uint64_t object_deadline(const fx_t *f, uint64_t h, uint64_t p)
{
    uint64_t origin = f->cfg.version == MOQ_VERSION_DRAFT_18 ? p : h;
    return origin + f->cfg.object_ms;
}

/* -- held output expires at its own deadline ------------------------- */

static void held_expiry_row(moq_version_t v, bool publication)
{
    char name[96];
    snprintf(name, sizeof(name), "d%u/%s/held-object-and-fin-expire", draft(v),
             publication ? "publish" : "subscribe");
    case_name = name;
    unsigned before = failures;
    fx_t f;
    moq_rcbuf_t *chunk = NULL;
    fx_cfg_t cfg = {.version = v, .publication = publication, .object_ms = 100};
    if (!setup(&f, &cfg)) goto done;
    NEED(moq_rcbuf_create(&f.alloc, (const uint8_t *)"late!!", 6, &chunk) == MOQ_OK);
    moq_subgroup_handle_t a;
    uint64_t sid = UINT64_MAX, header = 0;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    if (!open_served(&f, 7, 0, &a, &sid, &ref, &header)) goto done;
    fake_block_stream(&f.ep, sid);
    NEED(moq_session_begin_object(f.sender, a, 0, 6, at(&f, 50)) == MOQ_OK);
    if (!serve(&f, 50)) goto done;
    NEED(moq_session_write_object_data(f.sender, a, chunk, at(&f, 80)) == MOQ_OK);
    NEED(moq_session_end_object(f.sender, a, at(&f, 80)) == MOQ_OK);
    NEED(moq_session_close_subgroup(f.sender, a, at(&f, 90)) == MOQ_OK);
    if (!serve(&f, 90)) goto done;
    const uint64_t deadline = object_deadline(&f, 50, 80);
    expect("bridge-holds-object-and-fin", moq_transport_bridge_has_outbound_pending(f.bridge), true);
    expect("session-gave-up-the-fin", moq_session_has_pending_output(f.sender), false);
    expect("wakes-at-the-object-deadline", moq_session_next_deadline_us(f.sender), at(&f, deadline));
    if (!serve_abs(&f, at(&f, deadline) - 1)) goto done;
    expect_no_terminal(&f, "no-terminal-one-microsecond-early");
    expect("no-reset-one-microsecond-early", any_resets_on(&f, sid), 0);
    if (!serve(&f, deadline)) goto done;
    expect_one_terminal(&f, a, ref, 7, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT,
                        MOQ_RESET_DELIVERY_TIMEOUT);
    expect("one-timeout-reset", resets_on(&f, sid, MOQ_RESET_DELIVERY_TIMEOUT), 1);
    expect("reset-keeps-only-header", reliable_on(&f, sid), header);
    expect("expired-chunk-released", moq_rcbuf_refcount(chunk), 1);
    fake_unblock_stream(&f.ep);
    if (!serve(&f, deadline + 1)) goto done;
    expect("expired-bytes-never-written", writes_with(&f, sid, "late!!"), 0);
    expect("no-fin-after-credit", fins_on(&f, sid), 0);
    expect("bridge-drained", moq_transport_bridge_has_outbound_pending(f.bridge), false);
    if (!serve(&f, deadline + 1000)) goto done;
    expect("expired-entry-reaped", occupied(&f), 0);
    expect("sender-live", moq_session_state(f.sender), MOQ_SESS_ESTABLISHED);
done:
    if (chunk) moq_rcbuf_decref(chunk);
    cleanup(&f, before);
}

/* -- healthy hand-off and the refused FIN-only write ----------------- */

static void handoff_row(moq_version_t v, bool refuse_fin)
{
    char name[96];
    snprintf(name, sizeof(name), "d%u/subscribe/%s", draft(v),
             refuse_fin ? "refused-fin-only-write" : "healthy-handoff");
    case_name = name;
    unsigned before = failures;
    fx_t f;
    fx_cfg_t cfg = {.version = v, .object_ms = 100};
    if (!setup(&f, &cfg)) goto done;
    moq_subgroup_handle_t a;
    uint64_t sid = UINT64_MAX;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    if (!open_served(&f, 7, 0, &a, &sid, &ref, NULL)) goto done;
    if (!refuse_fin) fake_block_stream(&f.ep, sid);
    NEED(moq_session_write_object(f.sender, a, 0, f.payload, at(&f, 10)) == MOQ_OK);
    if (!serve(&f, 10)) goto done;
    if (refuse_fin) {
        expect("object-accepted", writes_with(&f, sid, "object"), 1);
        fake_block_stream(&f.ep, sid);   /* the FIN-only write is refused */
    }
    NEED(moq_session_close_subgroup(f.sender, a, at(&f, 20)) == MOQ_OK);
    if (!serve(&f, 20)) goto done;
    expect("fin-not-accepted-yet", fins_on(&f, sid), 0);
    if (refuse_fin) {
        /* No timer for the FIN: far past the object's deadline nothing
         * expires; the identity stays (the conservative tail). */
        if (!serve(&f, 1000)) goto done;
        expect("no-reset-for-held-fin", any_resets_on(&f, sid), 0);
        expect_no_terminal(&f, "no-terminal-for-held-fin");
        expect("held-fin-keeps-identity", occupied(&f), 1);
        expect("no-timer-for-held-fin", moq_session_next_deadline_us(f.sender), UINT64_MAX);
    }
    fake_unblock_stream(&f.ep);
    uint64_t t = refuse_fin ? 1001 : 60;
    if (!serve(&f, t)) goto done;
    expect("object-delivered-once", writes_with(&f, sid, "object"), 1);
    expect("fin-accepted", fins_on(&f, sid), 1);
    expect("no-reset", any_resets_on(&f, sid), 0);
    expect_no_terminal(&f, "no-terminal-for-handoff");
    expect("bridge-drained", moq_transport_bridge_has_outbound_pending(f.bridge), false);
    /* Accepted FIN releases custody; the next advancing call reaps. */
    if (!serve(&f, t + 1)) goto done;
    expect("reaped-after-accepted-fin", occupied(&f), 0);
    expect("sender-live", moq_session_state(f.sender), MOQ_SESS_ESTABLISHED);
done:
    cleanup(&f, before);
}

/* -- a small pool: held identity blocks reuse; accepted FIN frees it --- */

static void pool_row(moq_version_t v)
{
    char name[96];
    snprintf(name, sizeof(name), "d%u/subscribe/pool-of-one", draft(v));
    case_name = name;
    unsigned before = failures;
    fx_t f;
    fx_cfg_t cfg = {.version = v, .object_ms = 100, .pool = 1};
    if (!setup(&f, &cfg)) goto done;
    moq_subgroup_handle_t a, b, c;
    uint64_t asid = UINT64_MAX, bsid = UINT64_MAX;
    moq_stream_ref_t ar = moq_stream_ref_from_u64(0), br = moq_stream_ref_from_u64(0);
    if (!open_served(&f, 7, 0, &a, &asid, &ar, NULL)) goto done;
    fake_block_stream(&f.ep, asid);
    NEED(moq_session_write_object(f.sender, a, 0, f.payload, at(&f, 10)) == MOQ_OK);
    NEED(moq_session_close_subgroup(f.sender, a, at(&f, 20)) == MOQ_OK);
    if (!serve(&f, 20)) goto done;
    expect("held-identity-occupies-the-pool", occupied(&f), 1);
    expect("held-identity-blocks-reuse", open_sg(&f, 8, 30, &b), MOQ_ERR_WOULD_BLOCK);
    fake_unblock_stream(&f.ep);
    if (!serve(&f, 40)) goto done;
    expect("fin-accepted", fins_on(&f, asid), 1);
    /* Released, not reclaimed: the slot frees at the next advancing call. */
    expect("not-reclaimed-before-advancing", occupied(&f), 1);
    expect("reuse-after-accepted-fin", open_sg(&f, 8, 50, &b) == MOQ_OK, true);
    expect("old-handle-stale",
           moq_session_write_object(f.sender, a, 1, f.payload, at(&f, 50)) == MOQ_ERR_STALE_HANDLE, true);
    if (!serve(&f, 50)) goto done;
    for (size_t i = 0; i < f.ep.count; ++i)
        if (f.ep.ops[i].kind == FAKE_OP_OPEN_UNI && f.ep.ops[i].stream_id != asid)
            bsid = f.ep.ops[i].stream_id;
    NEED(bsid != UINT64_MAX);
    br = moq_transport_bridge_find_ref(f.bridge, bsid);
    /* A late STOP for the old, released stream finds no identity (the
     * documented late-STOP boundary) and cannot touch the new one. */
    (void)moq_transport_bridge_on_peer_stop_sending(f.bridge, asid, 9, at(&f, 55));
    if (!serve(&f, 55)) goto done;
    expect_no_terminal(&f, "late-stop-invents-no-terminal");
    fake_block_stream(&f.ep, bsid);
    NEED(moq_session_write_object(f.sender, b, 0, f.other, at(&f, 60)) == MOQ_OK);
    NEED(moq_session_close_subgroup(f.sender, b, at(&f, 60)) == MOQ_OK);
    if (!serve(&f, 60)) goto done;
    (void)moq_transport_bridge_on_peer_stop_sending(f.bridge, asid, 9, at(&f, 70));
    if (!serve(&f, 160)) goto done;
    expect_one_terminal(&f, b, br, 8, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT,
                        MOQ_RESET_DELIVERY_TIMEOUT);
    expect("replacement-reset-once", resets_on(&f, bsid, MOQ_RESET_DELIVERY_TIMEOUT), 1);
    expect("old-stream-never-timed-out", resets_on(&f, asid, MOQ_RESET_DELIVERY_TIMEOUT), 0);
    fake_unblock_stream(&f.ep);
    if (!serve(&f, 300)) goto done;
    expect("expired-replacement-reaped", occupied(&f), 0);
    expect("pool-usable-again", open_sg(&f, 9, 300, &c) == MOQ_OK, true);
done:
    cleanup(&f, before);
}

/* An untimed stream is not pinned: its slot frees while its FIN is held. */
static void untimed_row(moq_version_t v)
{
    char name[96];
    snprintf(name, sizeof(name), "d%u/subscribe/untimed-not-pinned", draft(v));
    case_name = name;
    unsigned before = failures;
    fx_t f;
    fx_cfg_t cfg = {.version = v, .pool = 1};
    if (!setup(&f, &cfg)) goto done;
    moq_subgroup_handle_t a, b;
    uint64_t asid = UINT64_MAX;
    moq_stream_ref_t ar = moq_stream_ref_from_u64(0);
    if (!open_served(&f, 7, 0, &a, &asid, &ar, NULL)) goto done;
    fake_block_stream(&f.ep, asid);
    NEED(moq_session_write_object(f.sender, a, 0, f.payload, at(&f, 10)) == MOQ_OK);
    NEED(moq_session_close_subgroup(f.sender, a, at(&f, 20)) == MOQ_OK);
    if (!serve(&f, 20)) goto done;
    expect("untimed-fin-held", fins_on(&f, asid), 0);
    if (!serve(&f, 21)) goto done;
    expect("untimed-entry-not-pinned", occupied(&f), 0);
    expect("pool-free-while-fin-held", open_sg(&f, 8, 30, &b) == MOQ_OK, true);
    fake_unblock_stream(&f.ep);
    if (!serve(&f, 40)) goto done;
    expect("untimed-fin-delivered", fins_on(&f, asid), 1);
    expect_no_terminal(&f, "no-terminal");
done:
    cleanup(&f, before);
}

/* -- event pressure, then recovery ------------------------------------ */

static bool fill_events(fx_t *f)
{
    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_SETUP_COMPLETE;
    e.detail_size = (uint32_t)sizeof(moq_setup_complete_event_t);
    while (!event_queue_full(f->sender))
        READY(push_event(f->sender, &e) == MOQ_OK);
    return true;
}

static void pressure_row(moq_version_t v)
{
    char name[96];
    snprintf(name, sizeof(name), "d%u/subscribe/event-pressure", draft(v));
    case_name = name;
    unsigned before = failures;
    fx_t f;
    fx_cfg_t cfg = {.version = v, .object_ms = 100, .max_events = 4};
    if (!setup(&f, &cfg)) goto done;
    moq_subgroup_handle_t a;
    uint64_t sid = UINT64_MAX, header = 0;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    if (!open_served(&f, 7, 0, &a, &sid, &ref, &header)) goto done;
    fake_block_stream(&f.ep, sid);
    NEED(moq_session_write_object(f.sender, a, 0, f.payload, at(&f, 10)) == MOQ_OK);
    NEED(moq_session_close_subgroup(f.sender, a, at(&f, 20)) == MOQ_OK);
    if (!serve(&f, 20)) goto done;
    if (!fill_events(&f)) goto done;
    if (!serve(&f, 110)) goto done;
    expect("no-reset-without-event-credit", any_resets_on(&f, sid), 0);
    expect("debt-keeps-identity", occupied(&f), 1);
    /* Draining the fillers returns credit; the owed terminal commits as
     * soon as it can, and its reset follows only then. */
    terminals_t acc;
    memset(&acc, 0, sizeof(acc));
    accumulate(&acc, sender_events(&f));
    if (!serve(&f, 111)) goto done;
    accumulate(&acc, sender_events(&f));
    check_terminal(&f, &acc, a, ref, 7, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT,
                   MOQ_RESET_DELIVERY_TIMEOUT);
    expect("one-reset-after-recovery", resets_on(&f, sid, MOQ_RESET_DELIVERY_TIMEOUT), 1);
    expect("reset-keeps-only-header", reliable_on(&f, sid), header);
    if (!serve(&f, 500)) goto done;
    expect("no-duplicate-reset", any_resets_on(&f, sid), 1);
    expect_no_terminal(&f, "no-duplicate-terminal");
    expect("reaped", occupied(&f), 0);
done:
    cleanup(&f, before);
}

/* -- first cause ------------------------------------------------------ */

typedef enum { FIRST_EXPIRY_THEN_STOP, FIRST_STOP_THEN_EXPIRY, FIRST_EXPIRY_THEN_CANCEL,
               FIRST_APP_RESET } first_kind_t;

static bool feed_control(fx_t *f, uint64_t ms);

static void first_cause_row(moq_version_t v, first_kind_t k)
{
    static const char *names[] = {"expiry-then-stop", "stop-then-expiry",
                                  "expiry-then-cancel", "application-reset-first"};
    char name[96];
    snprintf(name, sizeof(name), "d%u/subscribe/first-cause-%s", draft(v), names[k]);
    case_name = name;
    unsigned before = failures;
    fx_t f;
    fx_cfg_t cfg = {.version = v, .object_ms = 100, .max_events = 4};
    if (!setup(&f, &cfg)) goto done;
    moq_subgroup_handle_t a;
    uint64_t sid = UINT64_MAX;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    if (!open_served(&f, 7, 0, &a, &sid, &ref, NULL)) goto done;
    fake_block_stream(&f.ep, sid);
    NEED(moq_session_write_object(f.sender, a, 0, f.payload, at(&f, 10)) == MOQ_OK);
    if (k == FIRST_APP_RESET) {
        if (!serve(&f, 10)) goto done;
        NEED(moq_session_reset_subgroup(f.sender, a, 7, at(&f, 20)) == MOQ_OK);
        if (!serve(&f, 20)) goto done;
        if (!serve(&f, 500)) goto done;
        expect_no_terminal(&f, "application-reset-has-no-terminal");
        expect("application-code-only", any_resets_on(&f, sid), resets_on(&f, sid, 7));
        expect("no-timeout-reset", resets_on(&f, sid, MOQ_RESET_DELIVERY_TIMEOUT), 0);
        expect("reaped", occupied(&f), 0);
        goto done;
    }
    NEED(moq_session_close_subgroup(f.sender, a, at(&f, 20)) == MOQ_OK);
    if (!serve(&f, 20)) goto done;
    if (k == FIRST_STOP_THEN_EXPIRY) {
        NEED(moq_transport_bridge_on_peer_stop_sending(f.bridge, sid, 9, at(&f, 30)) == MOQ_OK);
        if (!serve(&f, 30)) goto done;
        expect_one_terminal(&f, a, ref, 7, MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING, 9);
        if (!serve(&f, 500)) goto done;
        expect_no_terminal(&f, "expiry-adds-no-terminal");
        expect("no-timeout-reset", resets_on(&f, sid, MOQ_RESET_DELIVERY_TIMEOUT), 0);
        expect("reaped", occupied(&f), 0);
        goto done;
    }
    /* The expiry is recognized while no event credit is free. */
    if (!fill_events(&f)) goto done;
    if (!serve(&f, 110)) goto done;
    expect("expiry-owed", any_resets_on(&f, sid), 0);
    if (k == FIRST_EXPIRY_THEN_STOP) {
        /* The STOP arrives while the expiry's event still waits. */
        (void)moq_transport_bridge_on_peer_stop_sending(f.bridge, sid, 9, at(&f, 120));
        expect("stop-adds-no-reset-yet", any_resets_on(&f, sid), 0);
        terminals_t acc;
        memset(&acc, 0, sizeof(acc));
        accumulate(&acc, sender_events(&f));
        if (!serve(&f, 121)) goto done;
        accumulate(&acc, sender_events(&f));
        check_terminal(&f, &acc, a, ref, 7, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT,
                       MOQ_RESET_DELIVERY_TIMEOUT);
    } else {
        /* The cancellation needs event credit itself; the owed terminal
         * takes the first credit returned, and the cancellation follows
         * while its reset is still owed to the bridge. */
        terminals_t acc;
        memset(&acc, 0, sizeof(acc));
        accumulate(&acc, sender_events(&f));
        expect("timeout-reset-still-owed", any_resets_on(&f, sid), 0);
        NEED(moq_session_unsubscribe(f.receiver, f.local_sub, at(&f, 120)) == MOQ_OK);
        if (!feed_control(&f, 120)) goto done;
        if (!serve(&f, 121)) goto done;
        accumulate(&acc, sender_events(&f));
        check_terminal(&f, &acc, a, ref, 7, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT,
                       MOQ_RESET_DELIVERY_TIMEOUT);
        expect("cancel-reset-not-added", resets_on(&f, sid, 0x1), 0);
    }
    if (!serve(&f, 600)) goto done;
    expect_no_terminal(&f, "no-second-terminal");
    expect("first-code-kept", resets_on(&f, sid, MOQ_RESET_DELIVERY_TIMEOUT) >= 1, true);
    expect("no-other-code", any_resets_on(&f, sid), resets_on(&f, sid, MOQ_RESET_DELIVERY_TIMEOUT));
    expect("reaped", occupied(&f), 0);
done:
    cleanup(&f, before);
}

/* -- owner cancellation and normal completion ------------------------- */

/* Carry the receiver's control output to the sender (draft-16 control
 * channel), as the transport would. */
static bool feed_control(fx_t *f, uint64_t ms)
{
    moq_action_t a;
    while (moq_session_poll_actions(f->receiver, &a, 1)) {
        if (a.kind == MOQ_ACTION_SEND_CONTROL) {
            moq_result_t rc = moq_session_on_control_bytes(f->sender, a.u.send_control.data,
                                                           a.u.send_control.len, at(f, ms));
            moq_action_cleanup(&a);
            READY(rc == MOQ_OK);
        } else {
            moq_action_cleanup(&a);
        }
    }
    return true;
}

static void cancel_row(bool publication, bool cancel)
{
    char name[96];
    snprintf(name, sizeof(name), "d16/%s/%s", publication ? "publish" : "subscribe",
             cancel ? "owner-cancelled-while-held" : "done-keeps-identity");
    case_name = name;
    unsigned before = failures;
    fx_t f;
    fx_cfg_t cfg = {.version = MOQ_VERSION_DRAFT_16, .publication = publication,
                    .object_ms = 100};
    if (!setup(&f, &cfg)) goto done;
    moq_subgroup_handle_t a;
    uint64_t sid = UINT64_MAX;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    if (!open_served(&f, 7, 0, &a, &sid, &ref, NULL)) goto done;
    fake_block_stream(&f.ep, sid);
    NEED(moq_session_write_object(f.sender, a, 0, f.payload, at(&f, 10)) == MOQ_OK);
    NEED(moq_session_close_subgroup(f.sender, a, at(&f, 20)) == MOQ_OK);
    if (!serve(&f, 20)) goto done;
    if (cancel) {
        if (publication)
            NEED(moq_session_cancel_publication(f.receiver, f.peer_pub, 0, at(&f, 30)) == MOQ_OK);
        else
            NEED(moq_session_unsubscribe(f.receiver, f.local_sub, at(&f, 30)) == MOQ_OK);
        if (!feed_control(&f, 30)) goto done;
        if (!serve(&f, 30)) goto done;
        (void)sender_events(&f);   /* the owner's own cancellation notice */
        fake_unblock_stream(&f.ep);
        if (!serve(&f, 31)) goto done;
        expect("cancelled-with-cancel-reset", resets_on(&f, sid, 0x1), 1);
        expect("no-fin-after-cancel", fins_on(&f, sid), 0);
        if (!serve(&f, 500)) goto done;
        expect_no_terminal(&f, "cancellation-adds-no-terminal");
        expect("no-timeout-reset", resets_on(&f, sid, MOQ_RESET_DELIVERY_TIMEOUT), 0);
        expect("cancelled-slot-reaped", occupied(&f), 0);
    } else {
        moq_done_subscribe_cfg_t dc;
        moq_done_subscribe_cfg_init(&dc);
        dc.status_code = 0x2;
        dc.stream_count = 1;
        NEED(publication ||
             moq_session_done_subscribe(f.sender, f.sub, &dc, at(&f, 30)) == MOQ_OK);
        if (publication) {
            moq_finish_publish_cfg_t fc;
            moq_finish_publish_cfg_init(&fc);
            NEED(moq_session_finish_publish(f.sender, f.pub, &fc, at(&f, 30)) == MOQ_OK);
        }
        if (!serve(&f, 30)) goto done;
        (void)sender_events(&f);
        if (!serve(&f, 110)) goto done;
        expect_one_terminal(&f, a, ref, 7, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT,
                            MOQ_RESET_DELIVERY_TIMEOUT);
        expect("timeout-reset-after-done", resets_on(&f, sid, MOQ_RESET_DELIVERY_TIMEOUT), 1);
    }
done:
    cleanup(&f, before);
}

/* -- the bridge goes away while the session stays --------------------- */

typedef enum { TEARDOWN_DESTROY, TEARDOWN_FATAL, TEARDOWN_SESSION_CLOSE } teardown_t;

static void teardown_row(moq_version_t v, teardown_t k)
{
    static const char *names[] = {"bridge-destroyed", "bridge-fatal", "session-closed"};
    char name[96];
    snprintf(name, sizeof(name), "d%u/subscribe/%s-while-held", draft(v), names[k]);
    case_name = name;
    unsigned before = failures;
    fx_t f;
    fx_cfg_t cfg = {.version = v, .object_ms = 100, .pool = 1};
    if (!setup(&f, &cfg)) goto done;
    moq_subgroup_handle_t a, b;
    uint64_t sid = UINT64_MAX;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    if (!open_served(&f, 7, 0, &a, &sid, &ref, NULL)) goto done;
    fake_block_stream(&f.ep, sid);
    NEED(moq_session_write_object(f.sender, a, 0, f.payload, at(&f, 10)) == MOQ_OK);
    NEED(moq_session_close_subgroup(f.sender, a, at(&f, 20)) == MOQ_OK);
    if (!serve(&f, 20)) goto done;
    expect("held", occupied(&f), 1);
    if (k == TEARDOWN_SESSION_CLOSE) {
        NEED(moq_session_close(f.sender, 0, NULL, at(&f, 30)) == MOQ_OK);
        (void)moq_transport_bridge_service(f.bridge, at(&f, 30));
        terminals_t t = sender_events(&f);
        expect("close-invents-no-terminal", t.count, 0);
        expect("closed", moq_session_state(f.sender), MOQ_SESS_CLOSED);
        goto done;
    }
    if (k == TEARDOWN_FATAL) {
        f.ep.fail_write = true;
        fake_unblock_stream(&f.ep);
        moq_session_tick(f.sender, at(&f, 30));
        (void)moq_transport_bridge_service(f.bridge, at(&f, 30));
        expect("bridge-fatal", moq_transport_bridge_is_fatal(f.bridge), true);
        f.ep.fail_write = false;
        /* The fatal bridge is still the session's owner: its own clear
         * alone must end the custody, before any unregister could. */
        expect("fatal-owner-still-registered",
               f.sender->transport_owner == (const void *)f.bridge, true);
        NEED(moq_session_tick(f.sender, at(&f, 35)) == MOQ_OK);
        expect_no_terminal(&f, "fatal-invents-no-terminal");
        expect("fatal-custody-slot-reclaimed", occupied(&f), 0);
        expect("fatal-queues-no-reset", queued_resets(&f, ref), 0);
        expect("fatal-writes-no-reset", any_resets_on(&f, sid), 0);
        expect("fatal-session-survives", moq_session_state(f.sender), MOQ_SESS_ESTABLISHED);
    }
    moq_transport_bridge_destroy(f.bridge);
    f.bridge = NULL;
    NEED(moq_session_tick(f.sender, at(&f, 40)) == MOQ_OK);
    expect_no_terminal(&f, "teardown-invents-no-terminal");
    expect("custody-slot-reclaimed", occupied(&f), 0);
    expect("session-survives", moq_session_state(f.sender), MOQ_SESS_ESTABLISHED);
    /* A new bridge delivers a new subgroup in the reclaimed slot. */
    if (!attach(&f)) goto done;
    uint64_t bsid = UINT64_MAX;
    moq_stream_ref_t br = moq_stream_ref_from_u64(0);
    if (!open_served(&f, 8, 50, &b, &bsid, &br, NULL)) goto done;
    NEED(moq_session_write_object(f.sender, b, 0, f.other, at(&f, 50)) == MOQ_OK);
    NEED(moq_session_close_subgroup(f.sender, b, at(&f, 50)) == MOQ_OK);
    if (!serve(&f, 50)) goto done;
    expect("reuse-delivers", writes_with(&f, bsid, "other"), 1);
    expect("reuse-fin", fins_on(&f, bsid), 1);
    if (!serve(&f, 500)) goto done;
    expect_no_terminal(&f, "no-late-terminal");
    expect("reuse-reaped", occupied(&f), 0);
done:
    cleanup(&f, before);
}

/* A terminal already recognized, still waiting for event capacity, when
 * the bridge goes away (destroyed, or fatal and still registered): ending
 * the custody does not erase that debt. The entry and its first cause stay;
 * once capacity returns the terminal commits once, with the exact identity. */
static void debt_teardown_row(moq_version_t v, teardown_t k)
{
    char name[96];
    snprintf(name, sizeof(name), "d%u/subscribe/terminal-debt-across-%s", draft(v),
             k == TEARDOWN_DESTROY ? "bridge-destroy" : "bridge-fatal");
    case_name = name;
    unsigned before = failures;
    fx_t f;
    fx_cfg_t cfg = {.version = v, .object_ms = 100, .max_events = 4};
    if (!setup(&f, &cfg)) goto done;
    moq_subgroup_handle_t a, b;
    uint64_t sid = UINT64_MAX, bsid = UINT64_MAX;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0), br = moq_stream_ref_from_u64(0);
    if (!open_served(&f, 7, 0, &a, &sid, &ref, NULL)) goto done;
    fake_block_stream(&f.ep, sid);
    NEED(moq_session_write_object(f.sender, a, 0, f.payload, at(&f, 10)) == MOQ_OK);
    NEED(moq_session_close_subgroup(f.sender, a, at(&f, 20)) == MOQ_OK);
    if (!serve(&f, 20)) goto done;
    if (k == TEARDOWN_FATAL && !open_served(&f, 8, 20, &b, &bsid, &br, NULL)) goto done;
    if (!fill_events(&f)) goto done;
    if (!serve(&f, 110)) goto done;
    int slot = sg_find_by_stream_ref(f.sender, ref);
    NEED(slot >= 0);
    expect("debt-recognized", f.sender->subgroups[slot].state, MOQ_SG_TERMINAL_OWED);
    expect("debt-no-reset-yet", any_resets_on(&f, sid), 0);
    if (k == TEARDOWN_FATAL) {
        /* An unrelated write fails: the bridge turns fatal and stays. */
        f.ep.fail_write = true;
        NEED(moq_session_write_object(f.sender, b, 0, f.other, at(&f, 111)) == MOQ_OK);
        moq_session_tick(f.sender, at(&f, 111));
        (void)moq_transport_bridge_service(f.bridge, at(&f, 111));
        expect("bridge-fatal", moq_transport_bridge_is_fatal(f.bridge), true);
        f.ep.fail_write = false;
    } else {
        moq_transport_bridge_destroy(f.bridge);
        f.bridge = NULL;
    }
    /* The debt still waits for event capacity, so this tick may report
     * that it could not finish. */
    moq_result_t trc = moq_session_tick(f.sender, at(&f, 112));
    NEED(trc == MOQ_OK || trc == MOQ_ERR_WOULD_BLOCK);
    slot = sg_find_by_stream_ref(f.sender, ref);
    expect("debt-survives-teardown", slot >= 0, true);
    if (slot >= 0) {
        expect("debt-state-kept", f.sender->subgroups[slot].state, MOQ_SG_TERMINAL_OWED);
        expect("debt-cause-kept", f.sender->subgroups[slot].terminal_cause,
               MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT);
        expect("debt-custody-ended", f.sender->subgroups[slot].timed_custody, false);
    }
    /* Capacity returns: the terminal commits once, as recognized. */
    terminals_t acc;
    memset(&acc, 0, sizeof(acc));
    accumulate(&acc, sender_events(&f));
    NEED(moq_session_tick(f.sender, at(&f, 113)) == MOQ_OK);
    accumulate(&acc, sender_events(&f));
    check_terminal(&f, &acc, a, ref, 7, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT,
                   MOQ_RESET_DELIVERY_TIMEOUT);
    NEED(moq_session_tick(f.sender, at(&f, 500)) == MOQ_OK);
    expect_no_terminal(&f, "no-duplicate-terminal");
done:
    cleanup(&f, before);
}

/* -- negotiated subgroup timers keep their own rule ------------------- */

static void subgroup_timer_row(moq_version_t v, bool commit)
{
    char name[96];
    snprintf(name, sizeof(name), "d%u/subscribe/subgroup-timer-%s", draft(v),
             commit ? "commit-clears" : "timer-fires");
    case_name = name;
    unsigned before = failures;
    fx_t f;
    fx_cfg_t cfg = {.version = v, .object_ms = 100, .subgroup_ms = 200,
                    .commit_report = true};
    if (!setup(&f, &cfg)) goto done;
    moq_subgroup_handle_t a;
    uint64_t sid = UINT64_MAX;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    if (!open_served(&f, 7, 0, &a, &sid, &ref, NULL)) goto done;
    NEED(moq_session_write_object(f.sender, a, 0, f.payload, at(&f, 10)) == MOQ_OK);
    NEED(moq_session_close_subgroup(f.sender, a, at(&f, 20)) == MOQ_OK);
    if (!serve(&f, 20)) goto done;
    expect("fin-accepted", fins_on(&f, sid), 1);
    expect("accepted-fin-leaves-subgroup-timer", moq_session_next_deadline_us(f.sender),
           at(&f, 220));
    if (!serve(&f, 21)) goto done;
    expect("timer-keeps-the-slot", occupied(&f), 1);
    if (commit) {
        NEED(moq_transport_bridge_on_local_stream_committed(f.bridge, sid, at(&f, 100)) == MOQ_OK);
        expect("commit-clears-timer", moq_session_next_deadline_us(f.sender), UINT64_MAX);
        if (!serve(&f, 300)) goto done;
        expect_no_terminal(&f, "no-terminal-after-commit");
        expect("reaped-after-commit", occupied(&f), 0);
    } else {
        if (!serve(&f, 220)) goto done;
        expect_one_terminal(&f, a, ref, 7, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT,
                            MOQ_RESET_DELIVERY_TIMEOUT);
        expect("timer-reset", resets_on(&f, sid, MOQ_RESET_DELIVERY_TIMEOUT), 1);
    }
done:
    cleanup(&f, before);
}

/* -- the publisher facade reconciles the correlated event ------------- */

static void facade_row(moq_version_t v)
{
    char name[96];
    snprintf(name, sizeof(name), "d%u/facade/held-group-timeout-reconciled", draft(v));
    case_name = name;
    unsigned before = failures;
    test_alloc_state_t memory = {0};
    moq_alloc_t alloc = test_allocator(&memory);
    moq_simpair_t *pair = NULL;
    moq_publisher_t *pub = NULL;
    moq_pub_track_t *track = NULL;
    moq_transport_bridge_t *bridge = NULL;
    fake_endpoint_t ep;
    moq_rcbuf_t *obj = NULL;
    memset(&ep, 0, sizeof(ep));
    moq_simpair_cfg_t sc = MOQ_SIMPAIR_CFG_INIT;
    sc.alloc = &alloc;
    sc.version = v;
    sc.initial_now_us = 1000000;
    sc.client_send_request_capacity = sc.server_send_request_capacity = true;
    sc.client_initial_request_capacity = sc.server_initial_request_capacity = 16;
    NEED(moq_simpair_create(&sc, &pair) == MOQ_OK);
    NEED(moq_simpair_start(pair) == MOQ_OK);
    NEED(moq_simpair_run_until_quiescent(pair, 64, NULL) == MOQ_OK);
    moq_session_t *client = moq_simpair_client(pair), *server = moq_simpair_server(pair);
    (void)drain(client); (void)drain(server);
    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    NEED(moq_pub_create(server, &alloc, &pcfg, &pub) == MOQ_OK);
    moq_bytes_t part = MOQ_BYTES_LITERAL("live");
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace = (moq_namespace_t){&part, 1};
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    uint64_t now = moq_simpair_now_us(pair);
    NEED(moq_pub_add_track(pub, &tcfg, now, &track) == MOQ_OK);
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.track_namespace = (moq_namespace_t){&part, 1};
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    scfg.delivery_timeout_flags = MOQ_SUBSCRIBE_OBJECT_DELIVERY_TIMEOUT;
    scfg.object_delivery_timeout_ms = 100;
    moq_subscription_t local;
    NEED(moq_session_subscribe(client, &scfg, now, &local) == MOQ_OK);
    for (int i = 0; i < 4; ++i) {
        NEED(moq_simpair_run_until_quiescent(pair, 64, NULL) == MOQ_OK);
        (void)moq_pub_tick(pub, now);
    }
    NEED(moq_simpair_run_until_quiescent(pair, 64, NULL) == MOQ_OK);
    NEED(moq_pub_active_subscriptions(pub, track) == 1);
    (void)drain(client);
    const uint64_t base = moq_simpair_now_us(pair);
    fake_endpoint_init(&ep, 1000, 2000);
    fake_endpoint_enable_reset_at(&ep);
    ep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    moq_transport_bridge_cfg_t bc;
    moq_transport_bridge_cfg_init(&bc, &alloc);
    NEED(moq_transport_bridge_create(&bc, server, &ep.vtable, &ep, &bridge) == MOQ_OK);
    NEED(moq_rcbuf_create(&alloc, (const uint8_t *)"g0o0", 4, &obj) == MOQ_OK);

    /* Group 0 opens its stream; then the transport takes nothing more on it. */
    NEED(moq_pub_write_object(pub, track, 0, 0, obj, base) == MOQ_OK);
    NEED(moq_transport_bridge_service(bridge, base) == MOQ_OK);
    uint64_t g0 = UINT64_MAX;
    for (size_t i = 0; i < ep.count; ++i)
        if (ep.ops[i].kind == FAKE_OP_OPEN_UNI) g0 = ep.ops[i].stream_id;
    NEED(g0 != UINT64_MAX);
    fake_block_stream(&ep, g0);
    NEED(moq_pub_write_object(pub, track, 0, 1, obj, base + 10 * MS) == MOQ_OK);
    /* Group 1 makes the facade FIN group 0 behind its held object. */
    NEED(moq_pub_write_object(pub, track, 1, 0, obj, base + 20 * MS) == MOQ_OK);
    NEED(moq_transport_bridge_service(bridge, base + 20 * MS) == MOQ_OK);
    NEED(moq_session_tick(server, base + 110 * MS) == MOQ_OK);
    NEED(moq_transport_bridge_service(bridge, base + 110 * MS) == MOQ_OK);
    unsigned terminals = 0, consumed = 0;
    moq_event_t e;
    while (moq_session_poll_events(server, &e, 1)) {
        if (e.kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED) {
            ++terminals;
            expect("facade-terminal-group", e.u.outgoing_subgroup_terminated.group_id, 0);
            expect("facade-terminal-cause", e.u.outgoing_subgroup_terminated.cause,
                   MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT);
            moq_pub_event_result_t r = MOQ_PUB_EVENT_IGNORED;
            expect("facade-takes-event",
                   moq_pub_handle_event(pub, &e, base + 110 * MS, &r), MOQ_OK);
            consumed += r == MOQ_PUB_EVENT_CONSUMED;
        } else {
            moq_pub_event_result_t r;
            (void)moq_pub_handle_event(pub, &e, base + 110 * MS, &r);
        }
        moq_event_cleanup(&e);
    }
    expect("one-correlated-terminal", terminals, 1);
    expect("facade-reconciles-its-slot", consumed, 1);
    unsigned g0_resets = 0;
    for (size_t i = 0; i < ep.count; ++i)
        if (ep.ops[i].kind == FAKE_OP_RESET_AT && ep.ops[i].stream_id == g0 &&
            ep.ops[i].error_code == MOQ_RESET_DELIVERY_TIMEOUT) ++g0_resets;
    expect("group0-reset-once", g0_resets, 1);
    expect("subscription-kept", moq_pub_active_subscriptions(pub, track), 1);
done:
    if (obj) moq_rcbuf_decref(obj);
    if (bridge) moq_transport_bridge_destroy(bridge);
    if (pub) { if (track) moq_pub_remove_track(pub, track, 0); moq_pub_destroy(pub); }
    if (pair) {
        moq_action_t a;
        (void)drain(moq_simpair_client(pair)); (void)drain(moq_simpair_server(pair));
        while (moq_session_poll_actions(moq_simpair_server(pair), &a, 1)) moq_action_cleanup(&a);
        while (moq_session_poll_actions(moq_simpair_client(pair), &a, 1)) moq_action_cleanup(&a);
        moq_simpair_destroy(pair);
    }
    expect("allocation-balance", (uint64_t)memory.balance, 0);
    ++cases;
    if (failures != before) ++failed_cases;
    printf("%s %s\n", failures == before ? "PASS" : "FAIL", case_name);
}

int main(void)
{
    const moq_version_t all[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (size_t i = 0; i < 3; ++i) {
        held_expiry_row(all[i], false);
        held_expiry_row(all[i], true);
        handoff_row(all[i], false);
        handoff_row(all[i], true);
        pool_row(all[i]);
        untimed_row(all[i]);
        pressure_row(all[i]);
        for (first_kind_t k = FIRST_EXPIRY_THEN_STOP; k <= FIRST_APP_RESET; ++k)
            if (k != FIRST_EXPIRY_THEN_CANCEL || all[i] == MOQ_VERSION_DRAFT_16)
                first_cause_row(all[i], k);
        for (teardown_t t = TEARDOWN_DESTROY; t <= TEARDOWN_SESSION_CLOSE; ++t)
            teardown_row(all[i], t);
        debt_teardown_row(all[i], TEARDOWN_DESTROY);
        debt_teardown_row(all[i], TEARDOWN_FATAL);
    }
    cancel_row(false, true);
    cancel_row(true, true);
    cancel_row(false, false);
    cancel_row(true, false);
    subgroup_timer_row(MOQ_VERSION_DRAFT_18, true);
    subgroup_timer_row(MOQ_VERSION_DRAFT_18, false);
    subgroup_timer_row(MOQ_VERSION_DRAFT_22, true);
    subgroup_timer_row(MOQ_VERSION_DRAFT_22, false);
    facade_row(MOQ_VERSION_DRAFT_16);
    facade_row(MOQ_VERSION_DRAFT_18);
    printf("%u cases, %u checks, %u failed cases, %u behavioral failures, %u prerequisite failures\n",
           cases, checks, failed_cases, failures - prerequisites, prerequisites);
    return failures ? 1 : 0;
}
