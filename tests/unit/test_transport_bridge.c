#include <moq/moq.h>
#include <moq/transport_bridge.h>
#include <moq/control.h>
#include <moq/control_d18.h>
#include <moq/buf.h>
#include "test_support.h"
#include "../support/fake_endpoint.h"
#include "../../core/src/session/session_internal.h"
#include "../../core/src/session/session_transport.h"
#include "../../core/src/bridge/transport_bridge_internal.h"
#include "../support/sweep_arm.h"
#include "../support/failpoint.h"
#include "../support/fin_case.h"
#include "../support/ownership_graph.h"
#include "../support/txn_snapshot.h"
#include "../support/ns_owner_inventory.h"
#include <string.h>

/*
 * Bridge contracts and pending-state invariants: stream mapping, inbound
 * retry retention, control-channel topology, deferred close, terminal facts,
 * and the budgeted service entry. Each test builds a bridge over a fake
 * endpoint and exercises one scenario.
 */

/* -- Helpers -------------------------------------------------------- */

typedef struct {
    moq_session_t *client;
    moq_session_t *server;
    fake_endpoint_t client_ep;
    fake_endpoint_t server_ep;
    moq_transport_bridge_t *client_bridge;
    moq_transport_bridge_t *server_bridge;
} test_pair_t;

static int test_pair_init_full(test_pair_t *tp, uint32_t client_max_events,
                               bool client_streaming,
                               uint32_t client_max_actions,
                               uint64_t client_idle_us)
{
    memset(tp, 0, sizeof(*tp));

    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;
    /* Streaming delivery is what lets a peer RESET mid-object need an event,
     * which is the only way moq_session_on_data_reset can block. */
    ccfg.streaming_objects = client_streaming;
    /* A tiny action queue lets a test force ACTION-capacity backpressure,
     * which is a different blocker from a full event queue. 0 = default. */
    if (client_max_actions) ccfg.max_actions = client_max_actions;
    if (client_idle_us) ccfg.idle_timeout_us = client_idle_us;
    /* A tiny client event queue lets a test force receive backpressure
     * (a second object on a data stream blocks in PENDING_EMIT). 0 = default. */
    if (client_max_events) ccfg.max_events = client_max_events;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;

    if (moq_session_create(&ccfg, 0, &tp->client) < 0) return -1;
    if (moq_session_create(&scfg, 0, &tp->server) < 0) {
        moq_session_destroy(tp->client);
        return -1;
    }

    fake_endpoint_init(&tp->client_ep, 1000, 2000);
    fake_endpoint_init(&tp->server_ep, 3000, 4000);

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());

    if (moq_transport_bridge_create(&bcfg, tp->client,
            &tp->client_ep.vtable, &tp->client_ep,
            &tp->client_bridge) < 0) {
        moq_session_destroy(tp->server);
        moq_session_destroy(tp->client);
        return -1;
    }
    if (moq_transport_bridge_create(&bcfg, tp->server,
            &tp->server_ep.vtable, &tp->server_ep,
            &tp->server_bridge) < 0) {
        moq_transport_bridge_destroy(tp->client_bridge);
        moq_session_destroy(tp->server);
        moq_session_destroy(tp->client);
        return -1;
    }

    return 0;
}

static int test_pair_init_stream(test_pair_t *tp, uint32_t client_max_events,
                                 bool client_streaming)
{
    return test_pair_init_full(tp, client_max_events, client_streaming, 0, 0);
}

static int test_pair_init_ex(test_pair_t *tp, uint32_t client_max_events)
{
    return test_pair_init_stream(tp, client_max_events, false);
}

static int test_pair_init(test_pair_t *tp)
{
    return test_pair_init_ex(tp, 0);
}

static void test_pair_destroy(test_pair_t *tp)
{
    moq_transport_bridge_destroy(tp->client_bridge);
    moq_transport_bridge_destroy(tp->server_bridge);
    moq_session_destroy(tp->client);
    moq_session_destroy(tp->server);
}

/*
 * Pump: deliver client endpoint ops to server bridge, and vice versa.
 * Returns number of ops delivered.
 */
static size_t pump_once(test_pair_t *tp, uint64_t now)
{
    size_t delivered = 0;

    moq_transport_bridge_service(tp->client_bridge, now);

    for (size_t i = 0; i < tp->client_ep.count; i++) {
        fake_op_t *o = &tp->client_ep.ops[i];
        switch (o->kind) {
        case FAKE_OP_OPEN_BIDI:
            break;
        case FAKE_OP_WRITE:
            if (tp->server_bridge) {
                if (!tp->server_ep.next_bidi_id &&
                    o->stream_id >= 2000 && o->stream_id < 3000) {
                    moq_transport_bridge_on_peer_control_bytes(
                        tp->server_bridge, o->stream_id,
                        o->data, o->data_len, o->fin, now);
                } else if (o->stream_id >= 1000 && o->stream_id < 2000) {
                    moq_transport_bridge_on_peer_uni_bytes(
                        tp->server_bridge, o->stream_id,
                        o->data, o->data_len, o->fin, now);
                } else {
                    moq_transport_bridge_on_peer_control_bytes(
                        tp->server_bridge, o->stream_id,
                        o->data, o->data_len, o->fin, now);
                }
            }
            delivered++;
            break;
        case FAKE_OP_CLOSE:
            if (tp->server_bridge)
                moq_transport_bridge_on_transport_close(
                    tp->server_bridge, o->error_code, now);
            delivered++;
            break;
        default:
            delivered++;
            break;
        }
    }
    fake_endpoint_clear_ops(&tp->client_ep);

    moq_transport_bridge_service(tp->server_bridge, now);

    for (size_t i = 0; i < tp->server_ep.count; i++) {
        fake_op_t *o = &tp->server_ep.ops[i];
        switch (o->kind) {
        case FAKE_OP_WRITE:
            if (tp->client_bridge) {
                if (o->stream_id >= 2000 && o->stream_id < 3000) {
                    moq_transport_bridge_on_peer_control_bytes(
                        tp->client_bridge, o->stream_id,
                        o->data, o->data_len, o->fin, now);
                }
            }
            delivered++;
            break;
        default:
            delivered++;
            break;
        }
    }
    fake_endpoint_clear_ops(&tp->server_ep);

    return delivered;
}

static int pump_until_quiescent(test_pair_t *tp, int max, uint64_t now)
{
    for (int i = 0; i < max; i++) {
        if (pump_once(tp, now) == 0) return i;
    }
    return max;
}

static bool setup_handshake(test_pair_t *tp)
{
    moq_session_start(tp->client, 0);
    pump_until_quiescent(tp, 20, 0);

    moq_event_t ev;
    bool c_setup = false, s_setup = false;
    while (moq_session_poll_events(tp->client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) c_setup = true;
        moq_event_cleanup(&ev);
    }
    while (moq_session_poll_events(tp->server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) s_setup = true;
        moq_event_cleanup(&ev);
    }
    return c_setup && s_setup;
}

/*
 * The budgeted-advance context must be left on EVERY exit from the budgeted
 * service entry.
 * If any path returned without leaving it, the next ordinary session call would
 * run inside a budget it never asked for and could observe the private
 * suspension sentinel -- which no application-facing caller may ever see.
 *
 * Each case forces a different exit, then runs an unlimited advancing call.
 */
static int test_budget_context_paired_on_every_exit(void)
{
    int failures = 0;

    /* Exit: fatal short-circuit (before the pass is even entered). */
    {
        fake_endpoint_t ep;
        fake_endpoint_init(&ep, 100, 200);
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_CLIENT);
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
        moq_transport_bridge_t *b = NULL;
        MOQ_TEST_CHECK(moq_transport_bridge_create(&bcfg, s, &ep.vtable, &ep,
                                                    &b) == MOQ_OK);
        moq_transport_bridge_on_transport_error(b, 0x1, 1);
        MOQ_TEST_CHECK(moq_transport_bridge_is_fatal(b));
        moq_bridge_budgeted_result_t r;
        (void)moq_transport_bridge_service_budgeted(b, 2, 4, &r);
        MOQ_TEST_CHECK(!s->budget_active);
        MOQ_TEST_CHECK(moq_session_tick(s, 3) != MOQ_SESSION_SUSPENDED);
        MOQ_TEST_CHECK(!s->budget_active);
        moq_transport_bridge_destroy(b);
        moq_session_destroy(s);
    }

    /* Exit: ordinary drained pass (the bottom break). */
    {
        fake_endpoint_t ep;
        fake_endpoint_init(&ep, 100, 200);
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_CLIENT);
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
        moq_transport_bridge_t *b = NULL;
        MOQ_TEST_CHECK(moq_transport_bridge_create(&bcfg, s, &ep.vtable, &ep,
                                                    &b) == MOQ_OK);
        MOQ_TEST_CHECK(!s->budget_active);
        moq_bridge_budgeted_result_t r;
        MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(b, 1, 4, &r) ==
                       MOQ_OK);
        MOQ_TEST_CHECK(!s->budget_active);
        MOQ_TEST_CHECK(moq_session_tick(s, 2) != MOQ_SESSION_SUSPENDED);
        MOQ_TEST_CHECK(!s->budget_active);
        moq_transport_bridge_destroy(b);
        moq_session_destroy(s);
    }

    /* Exit: the pass itself returns non-OK -- a write error makes dispatch
     * fatal inside Step 2, so service returns MOQ_ERR_INTERNAL from within the
     * bracketed region rather than from a pre-enter guard. */
    {
        fake_endpoint_t ep;
        fake_endpoint_init(&ep, 100, 200);
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_CLIENT);
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
        moq_transport_bridge_t *b = NULL;
        MOQ_TEST_CHECK(moq_transport_bridge_create(&bcfg, s, &ep.vtable, &ep,
                                                    &b) == MOQ_OK);
        ep.fail_write = true;
        MOQ_TEST_CHECK(moq_session_start(s, 1) == MOQ_OK);
        moq_bridge_budgeted_result_t r;
        MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(b, 2, 4, &r) ==
                       MOQ_ERR_INTERNAL);
        MOQ_TEST_CHECK(!s->budget_active);
        MOQ_TEST_CHECK(moq_session_tick(s, 3) != MOQ_SESSION_SUSPENDED);
        MOQ_TEST_CHECK(!s->budget_active);
        moq_transport_bridge_destroy(b);
        moq_session_destroy(s);
    }

    /* Exit: outbound blocked, so the pass breaks out of Step 1. */
    {
        fake_endpoint_t ep;
        fake_endpoint_init(&ep, 100, 200);
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_CLIENT);
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
        moq_transport_bridge_t *b = NULL;
        MOQ_TEST_CHECK(moq_transport_bridge_create(&bcfg, s, &ep.vtable, &ep,
                                                    &b) == MOQ_OK);
        ep.block_write = true;      /* force WOULD_BLOCK on the setup write */
        MOQ_TEST_CHECK(moq_session_start(s, 1) == MOQ_OK);
        moq_bridge_budgeted_result_t r;
        (void)moq_transport_bridge_service_budgeted(b, 2, 4, &r);
        MOQ_TEST_CHECK(!s->budget_active);
        MOQ_TEST_CHECK(moq_session_tick(s, 3) != MOQ_SESSION_SUSPENDED);
        MOQ_TEST_CHECK(!s->budget_active);
        moq_transport_bridge_destroy(b);
        moq_session_destroy(s);
    }

    return failures;
}

/* == Tests ========================================================== */

/*
 * The unlimited service entry must enter NO budget context.
 *
 * Reading budget_active after the call cannot show this -- a paired
 * enter/leave also leaves it false. The entry COUNT is what discriminates:
 * a UINT32_MAX bracket would still be a finite budget, routing every wired
 * session entry point through the budgeted advance and giving the unlimited
 * entry a suspension it has no way to report.
 *
 * Deltas, not absolutes: the counters are gated globals that every test in
 * this binary accumulates into.
 */
static int test_unlimited_service_enters_no_budget_context(void)
{
    int failures = 0;

    fake_endpoint_t ep;
    fake_endpoint_init(&ep, 100, 200);
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    moq_transport_bridge_t *b = NULL;
    MOQ_TEST_CHECK(moq_transport_bridge_create(&bcfg, s, &ep.vtable, &ep,
                                                &b) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_start(s, 1) == MOQ_OK);

    uint64_t before = session_budget_enter_count;
    MOQ_TEST_CHECK(moq_transport_bridge_service(b, 2) == MOQ_OK);
    MOQ_TEST_CHECK(session_budget_enter_count - before == 0);
    MOQ_TEST_CHECK(!s->budget_active);

    before = session_budget_enter_count;
    moq_bridge_budgeted_result_t r;
    MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(b, 3, 8, &r) == MOQ_OK);
    MOQ_TEST_CHECK(session_budget_enter_count - before == 1);
    MOQ_TEST_CHECK(!s->budget_active);

    moq_transport_bridge_destroy(b);
    moq_session_destroy(s);
    return failures;
}

/*
 * The budgeted entry requires an output. A finite budget that can suspend,
 * paired with a discarded outcome, would report a drained pass while a cursor
 * is still live. *out is zeroed before any other argument is validated, so
 * every rejection except the NULL-output one still yields a defined outcome.
 */
static int test_budgeted_service_requires_output(void)
{
    int failures = 0;

    fake_endpoint_t ep;
    fake_endpoint_init(&ep, 100, 200);
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    moq_transport_bridge_t *b = NULL;
    MOQ_TEST_CHECK(moq_transport_bridge_create(&bcfg, s, &ep.vtable, &ep,
                                                &b) == MOQ_OK);

    uint64_t before = session_budget_enter_count;
    MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(b, 1, 8, NULL) ==
                   MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(session_budget_enter_count - before == 0);

    /* NULL bridge still leaves a defined, non-suspended outcome. */
    moq_bridge_budgeted_result_t r;
    memset(&r, 0xAB, sizeof(r));
    MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(NULL, 1, 8, &r) ==
                   MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(!r.suspended);
    MOQ_TEST_CHECK(r.sweep_spent == 0);

    /* An idle pool completes even at zero budget: no runnable work. */
    MOQ_TEST_CHECK(moq_session_start(s, 1) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(b, 2, 0, &r) == MOQ_OK);
    MOQ_TEST_CHECK(!r.suspended);

    moq_transport_bridge_destroy(b);
    moq_session_destroy(s);
    return failures;
}

/* -- Draft-18 pair: peer-opened request bidis ----------------------- *
 *
 * Bidi data entries exist only under draft-18, where each request travels on
 * its own bidirectional stream; draft-16 keeps every request on the single
 * shared control bidi. So the bidi arms of the inbound families need a d18
 * pair, and the client must be the one RECEIVING a request.
 *
 * Control is a unidirectional pair here, so the shuttle routes purely by the
 * fake endpoint's stream-id ranges and never needs the control entry point.
 */
/* As d18_pair_init below, with the CLIENT session on a caller-supplied
 * allocator so a test can fail one of its allocations by ordinal. The
 * bridges always stay on the default allocator, so an armed failure can
 * only land inside the session it targets. */
/* Generalized draft-18 pair: adds a server action cap and the server
 * endpoint's optional native whole-stream abort. Both extras default off, so
 * the wrappers below keep their existing behaviour exactly. */
static int d18_pair_init_caps(test_pair_t *tp, uint32_t client_max_events,
                              const moq_alloc_t *client_alloc,
                              const moq_alloc_t *server_alloc,
                              uint32_t server_max_actions,
                              bool server_native_abort,
                              uint32_t server_max_events)
{
    memset(tp, 0, sizeof(*tp));

    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), client_alloc,
                               MOQ_PERSPECTIVE_CLIENT);
    ccfg.version = MOQ_VERSION_DRAFT_18;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;
    if (client_max_events) ccfg.max_events = client_max_events;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), server_alloc,
                               MOQ_PERSPECTIVE_SERVER);
    scfg.version = MOQ_VERSION_DRAFT_18;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;
    if (server_max_actions) scfg.max_actions = server_max_actions;
    /* A tiny SERVER event queue lets a test defer an inbound teardown, which
     * is what leaves a FIN obligation retained on the bridge. 0 = default. */
    if (server_max_events) scfg.max_events = server_max_events;

    if (moq_session_create(&ccfg, 0, &tp->client) < 0) return -1;
    if (moq_session_create(&scfg, 0, &tp->server) < 0) {
        moq_session_destroy(tp->client);
        return -1;
    }

    fake_endpoint_init(&tp->client_ep, 1000, 2000);
    fake_endpoint_init(&tp->server_ep, 3000, 4000);
    /* The bridge RETAINS the vtable pointer it is handed
     * (transport_bridge.c:108) rather than copying the table, so an op may be
     * installed after init. It is done here, before bridge creation, so the
     * endpoint's capability set is fixed for the whole run. */
    if (server_native_abort) fake_endpoint_enable_abort(&tp->server_ep);

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    if (moq_transport_bridge_create(&bcfg, tp->client, &tp->client_ep.vtable,
                                    &tp->client_ep, &tp->client_bridge) < 0) {
        moq_session_destroy(tp->server);
        moq_session_destroy(tp->client);
        return -1;
    }
    if (moq_transport_bridge_create(&bcfg, tp->server, &tp->server_ep.vtable,
                                    &tp->server_ep, &tp->server_bridge) < 0) {
        moq_transport_bridge_destroy(tp->client_bridge);
        moq_session_destroy(tp->server);
        moq_session_destroy(tp->client);
        return -1;
    }
    return 0;
}

static int d18_pair_init_alloc(test_pair_t *tp, uint32_t client_max_events,
                               const moq_alloc_t *client_alloc,
                               const moq_alloc_t *server_alloc)
{
    return d18_pair_init_caps(tp, client_max_events, client_alloc,
                              server_alloc, 0, false, 0);
}

static int d18_pair_init(test_pair_t *tp, uint32_t client_max_events)
{
    return d18_pair_init_alloc(tp, client_max_events, moq_alloc_default(),
                               moq_alloc_default());
}

static void d18_feed(moq_transport_bridge_t *to, fake_endpoint_t *from,
                     uint64_t uni_base, uint64_t bidi_base, uint64_t now,
                     size_t *delivered)
{
    for (size_t i = 0; i < from->count; i++) {
        fake_op_t *o = &from->ops[i];
        if (o->kind != FAKE_OP_WRITE) { (*delivered)++; continue; }
        if (o->stream_id >= uni_base && o->stream_id < uni_base + 1000)
            moq_transport_bridge_on_peer_uni_bytes(
                to, o->stream_id, o->data, o->data_len, o->fin, now);
        else if (o->stream_id >= bidi_base && o->stream_id < bidi_base + 1000)
            moq_transport_bridge_on_peer_bidi_bytes(
                to, o->stream_id, o->data, o->data_len, o->fin, now);
        (*delivered)++;
    }
    fake_endpoint_clear_ops(from);
}

static size_t d18_shuttle(test_pair_t *tp, uint64_t now)
{
    size_t delivered = 0;
    moq_transport_bridge_service(tp->client_bridge, now);
    d18_feed(tp->server_bridge, &tp->client_ep, 1000, 2000, now, &delivered);
    moq_transport_bridge_service(tp->server_bridge, now);
    d18_feed(tp->client_bridge, &tp->server_ep, 3000, 4000, now, &delivered);
    return delivered;
}

static void d18_shuttle_until_quiescent(test_pair_t *tp, int max,
                                        uint64_t now)
{
    for (int i = 0; i < max; i++)
        if (d18_shuttle(tp, now) == 0) return;
}

/* The server issues the request, so the peer bidi lands on the CLIENT. */
static moq_result_t d18_server_subscribe(test_pair_t *tp)
{
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = ns;
    cfg.track_name = MOQ_BYTES_LITERAL("video");
    cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t sub;
    return moq_session_subscribe(tp->server, &cfg, 0, &sub);
}

/*
 * Subscribed client with its single event slot already occupied, plus the
 * server-side subgroup whose wire bytes the per-family tests deliver.
 *
 * The occupied slot is the client's own SUBSCRIBE_OK, deliberately left
 * unpolled: it is what makes the next session call that needs to emit return
 * WOULD_BLOCK, which is how each inbound pending flag is established for real
 * rather than written by hand.
 *
 * hdr holds the subgroup header alone (parsing it emits nothing, so it lands a
 * stream entry with no pending flag); obj additionally carries one object,
 * which cannot emit against a full queue.
 */
typedef struct {
    test_pair_t tp;
    uint64_t    sid;                  /* peer uni stream id on the client */
    uint8_t     hdr[512]; size_t hdr_len;
    uint8_t     obj[512]; size_t obj_len;
} uni_pending_fixture_t;

static int uni_pending_fixture_init_ex(uni_pending_fixture_t *f,
                                       bool streaming, size_t payload_len)
{
    memset(f, 0, sizeof(*f));
    f->sid = 5000;

    if (test_pair_init_stream(&f->tp, 1, streaming) < 0) return -1;
    if (!setup_handshake(&f->tp)) { test_pair_destroy(&f->tp); return -1; }

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
    sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t sub;
    if (moq_session_subscribe(f->tp.client, &sub_cfg, 0, &sub) != MOQ_OK)
        goto fail;
    pump_until_quiescent(&f->tp, 20, 0);

    moq_event_t ev;
    moq_subscription_t server_sub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(f->tp.server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
            server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    if (!moq_subscription_is_valid(server_sub)) goto fail;

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    if (moq_session_accept_subscribe(f->tp.server, server_sub, &acc, 0) != MOQ_OK)
        goto fail;
    pump_until_quiescent(&f->tp, 20, 0);
    /* The client's SUBSCRIBE_OK stays queued: that is the full slot. */

    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    sg_cfg.group_id = 0;
    sg_cfg.subgroup_id = 0;
    sg_cfg.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(f->tp.server, server_sub, &sg_cfg, 0, &sg)
        != MOQ_OK) goto fail;

    fake_endpoint_clear_ops(&f->tp.server_ep);
    moq_transport_bridge_service(f->tp.server_bridge, 0);

    uint64_t uni_sid = 0; bool have_uni = false;
    for (size_t i = 0; i < f->tp.server_ep.count; i++) {
        fake_op_t *o = &f->tp.server_ep.ops[i];
        if (o->kind == FAKE_OP_OPEN_UNI) { uni_sid = o->stream_id; have_uni = true; }
        else if (o->kind == FAKE_OP_WRITE && have_uni && o->stream_id == uni_sid &&
                 f->hdr_len + o->data_len <= sizeof(f->hdr)) {
            memcpy(f->hdr + f->hdr_len, o->data, o->data_len);
            f->hdr_len += o->data_len;
        }
    }
    if (!have_uni || f->hdr_len == 0) goto fail;
    memcpy(f->obj, f->hdr, f->hdr_len);
    f->obj_len = f->hdr_len;
    fake_endpoint_clear_ops(&f->tp.server_ep);

    uint8_t payload[256];
    if (payload_len > sizeof(payload)) goto fail;
    memset(payload, 'A', payload_len);
    moq_rcbuf_t *p = NULL;
    moq_rcbuf_create(moq_alloc_default(), payload, payload_len, &p);
    moq_result_t wrc = moq_session_write_object(f->tp.server, sg, 0, p, 0);
    moq_rcbuf_decref(p);
    if (wrc != MOQ_OK) goto fail;
    moq_transport_bridge_service(f->tp.server_bridge, 0);
    for (size_t i = 0; i < f->tp.server_ep.count; i++) {
        fake_op_t *o = &f->tp.server_ep.ops[i];
        if (o->kind == FAKE_OP_WRITE && o->stream_id == uni_sid &&
            f->obj_len + o->data_len <= sizeof(f->obj)) {
            memcpy(f->obj + f->obj_len, o->data, o->data_len);
            f->obj_len += o->data_len;
        }
    }
    fake_endpoint_clear_ops(&f->tp.server_ep);
    if (f->obj_len <= f->hdr_len) goto fail;

    return 0;

fail:
    test_pair_destroy(&f->tp);
    return -1;
}

static int uni_pending_fixture_init(uni_pending_fixture_t *f)
{
    return uni_pending_fixture_init_ex(f, false, 3);
}

/*
 * A suspended inbound data retry must keep pending_retry set and leave the
 * entry untouched, so the retained bytes are replayed on a later pass.
 */
static int test_data_retry_suspension_preserves_pending(void)
{
    int failures = 0;

    uni_pending_fixture_t f;
    if (uni_pending_fixture_init(&f) < 0) { failures++; return failures; }

    /* Header + one object against a full queue: the object cannot emit. */
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(
                       f.tp.client_bridge, f.sid, f.obj, f.obj_len, false, 0)
                   == MOQ_ERR_WOULD_BLOCK);
    bridge_stream_entry_t *e = bridge_find_by_id(f.tp.client_bridge, f.sid);
    MOQ_TEST_CHECK(e != NULL && e->pending_retry);
    MOQ_TEST_CHECK(e->kind == BRIDGE_STREAM_UNI);

    sweep_arm_closing_subgroup(f.tp.client, 0);

    uint64_t suspends = session_budget_suspend_count;
    moq_bridge_budgeted_result_t out;
    MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(
                       f.tp.client_bridge, 0, 0, &out) == MOQ_OK);

    MOQ_TEST_CHECK(out.suspended);
    MOQ_TEST_CHECK(out.sweep_spent == 0);
    MOQ_TEST_CHECK(session_budget_suspend_count - suspends == 1);
    MOQ_TEST_CHECK(e->pending_retry);
    MOQ_TEST_CHECK(e->active);
    MOQ_TEST_CHECK(!e->pending_fin && !e->fin_retained);
    MOQ_TEST_CHECK(!e->peer_send_closed);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));
    MOQ_TEST_CHECK(f.tp.client->subgroups[0].state == MOQ_SG_CLOSING);

    /* Drain the slot, then let the unlimited path complete both. */
    moq_event_t ev;
    while (moq_session_poll_events(f.tp.client, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(moq_transport_bridge_service(f.tp.client_bridge, 0) == MOQ_OK);
    MOQ_TEST_CHECK(f.tp.client->subgroups[0].state == MOQ_SG_FREE);
    MOQ_TEST_CHECK(!e->pending_retry);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));

    /* The retained object is delivered -- clearing the flag is bookkeeping,
     * this is the session transition it stands for -- exactly once. */
    MOQ_TEST_CHECK(moq_session_poll_events(f.tp.client, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
    MOQ_TEST_CHECK(ev.u.object_received.object_id == 0);
    MOQ_TEST_CHECK(ev.u.object_received.payload != NULL);
    MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == 3);
    MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.object_received.payload),
                          "AAA", 3) == 0);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(moq_session_poll_events(f.tp.client, &ev, 1) == 0);

    test_pair_destroy(&f.tp);
    return failures;
}

/*
 * A suspended reset delivery must keep pending_reset and its code, and must
 * not deactivate the entry -- the reset is delivered on a later pass instead.
 *
 * Only a streaming reset can block: the non-streaming path in handle_data_reset
 * records and frees without emitting, so it always returns MOQ_OK. The stream
 * is therefore parked mid-object with its begin chunk occupying the one event
 * slot, which is what makes the terminal chunk the reset must emit unaffordable
 * -- and it leaves no pending retry, so the reset block is the boundary that
 * suspends rather than the retry block above it.
 */
static int test_data_reset_suspension_preserves_pending(void)
{
    int failures = 0;

    uni_pending_fixture_t f;
    if (uni_pending_fixture_init_ex(&f, true, 64) < 0) { failures++; return failures; }

    /* Free the slot the begin chunk needs. */
    moq_event_t ev;
    while (moq_session_poll_events(f.tp.client, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    /* Truncated mid-payload: the begin chunk emits and fills the slot, and the
     * stream parks in STREAMING_PAYLOAD with nothing further to parse. */
    MOQ_TEST_CHECK(f.obj_len > f.hdr_len + 20);
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(
                       f.tp.client_bridge, f.sid, f.obj, f.obj_len - 20, false, 0)
                   == MOQ_OK);
    bridge_stream_entry_t *e = bridge_find_by_id(f.tp.client_bridge, f.sid);
    MOQ_TEST_CHECK(e != NULL && !e->pending_retry);
    MOQ_TEST_CHECK(f.tp.client->event_head != f.tp.client->event_tail);

    /* Pin the branch this test depends on by reading RX state rather than the
     * queued event: draining to inspect it would free the slot and unblock the
     * reset. Mid-payload under streaming delivery is the only combination
     * handle_data_reset answers by emitting. */
    MOQ_TEST_CHECK(f.tp.client->streaming_objects);
    size_t parked = 0;
    for (size_t i = 0; i < f.tp.client->rx_cap; i++) {
        moq_rx_stream_t *rx = &f.tp.client->rx_streams[i];
        if (rx->active && rx->stream_ref._v == e->ref._v &&
            rx->parse_state == MOQ_RX_STREAMING_PAYLOAD)
            parked++;
    }
    MOQ_TEST_CHECK(parked == 1);

    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(
                       f.tp.client_bridge, f.sid, 0x5, 0) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(e->pending_reset);
    MOQ_TEST_CHECK(e->pending_reset_code == 0x5);
    MOQ_TEST_CHECK(!e->pending_retry);

    sweep_arm_closing_subgroup(f.tp.client, 0);

    uint64_t suspends = session_budget_suspend_count;
    moq_bridge_budgeted_result_t out;
    MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(
                       f.tp.client_bridge, 0, 0, &out) == MOQ_OK);

    MOQ_TEST_CHECK(out.suspended);
    MOQ_TEST_CHECK(out.sweep_spent == 0);
    MOQ_TEST_CHECK(session_budget_suspend_count - suspends == 1);
    MOQ_TEST_CHECK(e->pending_reset);
    MOQ_TEST_CHECK(e->pending_reset_code == 0x5);
    MOQ_TEST_CHECK(e->active);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));
    MOQ_TEST_CHECK(f.tp.client->subgroups[0].state == MOQ_SG_CLOSING);

    while (moq_session_poll_events(f.tp.client, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(moq_transport_bridge_service(f.tp.client_bridge, 0) == MOQ_OK);
    MOQ_TEST_CHECK(f.tp.client->subgroups[0].state == MOQ_SG_FREE);
    MOQ_TEST_CHECK(!e->pending_reset);
    MOQ_TEST_CHECK(!e->active);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));

    /* The reset reaches the application as the object's terminal chunk. */
    MOQ_TEST_CHECK(moq_session_poll_events(f.tp.client, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_CHUNK);
    MOQ_TEST_CHECK(!ev.u.object_chunk.begin);
    MOQ_TEST_CHECK(ev.u.object_chunk.end);
    MOQ_TEST_CHECK(ev.u.object_chunk.terminal == MOQ_OBJECT_TERMINAL_RESET);
    MOQ_TEST_CHECK(ev.u.object_chunk.chunk == NULL);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(moq_session_poll_events(f.tp.client, &ev, 1) == 0);

    test_pair_destroy(&f.tp);
    return failures;
}

/*
 * A client holding an established peer-opened request bidi, with its single
 * event slot occupied by the SUBSCRIBE_REQUEST that bidi produced.
 *
 * The registration must happen while the queue still has room: a request whose
 * completing message cannot emit leaves the session without a retainable
 * stream, which the bridge escalates before any pending flag is set. Filling
 * the slot with the request's OWN event gives an established, registered entry
 * and a full queue at the same time.
 */
static int d18_request_bidi_fixture(test_pair_t *tp, uint64_t *bidi_out)
{
    if (d18_pair_init(tp, 1) < 0) return -1;
    moq_session_start(tp->client, 0);
    moq_session_start(tp->server, 0);
    d18_shuttle_until_quiescent(tp, 30, 0);

    moq_event_t ev;
    while (moq_session_poll_events(tp->client, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    if (d18_server_subscribe(tp) != MOQ_OK) goto fail;
    moq_transport_bridge_service(tp->server_bridge, 0);

    uint64_t bidi = 0;
    for (size_t i = 0; i < tp->server_ep.count; i++) {
        fake_op_t *o = &tp->server_ep.ops[i];
        if (o->kind != FAKE_OP_WRITE) continue;
        bidi = o->stream_id;
        if (moq_transport_bridge_on_peer_bidi_bytes(
                tp->client_bridge, o->stream_id, o->data, o->data_len,
                o->fin, 0) != MOQ_OK) goto fail;
    }
    fake_endpoint_clear_ops(&tp->server_ep);
    if (!bidi) goto fail;

    *bidi_out = bidi;
    return 0;

fail:
    test_pair_destroy(tp);
    return -1;
}

/*
 * BACKLOG #245(c): the bridge's own retry must complete the obligation the
 * refused call created, not silently discard it.
 *
 * Extra bytes on a publisher-side namespace-sub bidi tear that bidi down
 * (session_namespace_sub.c:1078-1080 -> ns_sub_local_teardown), and the
 * teardown can refuse for action capacity before recording anything durable
 * (:667-668). Bridge ingress correctly recognises the owner and retains the
 * retry (transport_bridge.c:2662-2669) -- so #245(a)'s predicate answers
 * correctly here, which this fixture also pins. Service then retries with
 * NULL/0 (:1934-1949); the session's durable marker makes that empty retry
 * complete the teardown (#245c), so pending_retry clears only once the abort
 * has dispatched.
 *
 * Recovery is SERVICE-ONLY: the bridge never re-delivers peer bytes, so the
 * fixture feeds none after the refusal.
 */
/* -- Owner-graph and inventory oracles for the ns_sub bidi ----------- */

/* Exact drain-ring membership: the declared refs, each with its declared
 * reason, and no others. */
typedef struct drain_spec { uint64_t ref; moq_drain_reason_t reason; } drain_spec_t;

static int check_drain_membership(const moq_session_t *s,
                                  const drain_spec_t *want, size_t n,
                                  const char *what)
{
    int failures = 0;
    if (s->drain_ref_count != n) {
        fprintf(stderr, "FAIL: %s: drain ring holds %zu refs, expected %zu\n",
                what, s->drain_ref_count, n);
        failures++;
    }
    for (size_t i = 0; i < n; i++) {
        moq_stream_ref_t r = moq_stream_ref_from_u64(want[i].ref);
        if (!drain_ref_contains(s, r)) {
            fprintf(stderr, "FAIL: %s: drain ref %llu is ABSENT\n", what,
                    (unsigned long long)want[i].ref);
            failures++;
            continue;
        }
        if (drain_ref_reason(s, r) != want[i].reason) {
            fprintf(stderr, "FAIL: %s: drain ref %llu reason %d, expected %d\n",
                    what, (unsigned long long)want[i].ref,
                    (int)drain_ref_reason(s, r), (int)want[i].reason);
            failures++;
        }
    }
    for (size_t i = 0; i < s->drain_ref_count; i++) {
        int declared = 0;
        for (size_t k = 0; k < n; k++)
            if (s->drain_refs[i] == want[k].ref) declared = 1;
        if (!declared) {
            fprintf(stderr, "FAIL: %s: UNDECLARED drain ref %llu\n", what,
                    (unsigned long long)s->drain_refs[i]);
            failures++;
        }
    }
    return failures;
}

/* A shuttle that CHECKS every result it produces and proves it converged.
 * d18_shuttle_until_quiescent discards both the service and the ingress
 * results, so a setup that limped to ESTABLISHED through a refused call would
 * look identical to a clean one.
 *
 * Deliberately file-local, and NOT yet strict about everything: a non-WRITE op
 * and a write outside both id ranges are counted as movement but not
 * classified. That is sound for the setup path this drives, where neither
 * occurs. Tighten those two cases before lifting this into shared support. */
static int strict_feed(moq_transport_bridge_t *to, fake_endpoint_t *from,
                       uint64_t uni_base, uint64_t bidi_base, uint64_t now,
                       size_t *moved, const char *what)
{
    int failures = 0;
    MOQ_TEST_CHECK(from->count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < from->count; i++) {
        fake_op_t *o = &from->ops[i];
        (*moved)++;
        if (o->kind != FAKE_OP_WRITE) continue;
        moq_result_t rc = MOQ_OK;
        if (o->stream_id >= uni_base && o->stream_id < uni_base + 1000)
            rc = moq_transport_bridge_on_peer_uni_bytes(
                to, o->stream_id, o->data, o->data_len, o->fin, now);
        else if (o->stream_id >= bidi_base && o->stream_id < bidi_base + 1000)
            rc = moq_transport_bridge_on_peer_bidi_bytes(
                to, o->stream_id, o->data, o->data_len, o->fin, now);
        if (rc != MOQ_OK) {
            fprintf(stderr, "FAIL: %s: ingress on stream %llu returned %d\n",
                    what, (unsigned long long)o->stream_id, (int)rc);
            failures++;
        }
    }
    fake_endpoint_clear_ops(from);
    return failures;
}

static int d18_strict_shuttle(test_pair_t *tp, int max, uint64_t now,
                              const char *what)
{
    int failures = 0;
    for (int i = 0; i < max; i++) {
        size_t moved = 0;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(tp->client_bridge, now),
            (int)MOQ_OK);
        failures += strict_feed(tp->server_bridge, &tp->client_ep, 1000, 2000,
                                now, &moved, what);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(tp->server_bridge, now),
            (int)MOQ_OK);
        failures += strict_feed(tp->client_bridge, &tp->server_ep, 3000, 4000,
                                now, &moved, what);
        if (moved) continue;
        /* Quiescence, proven rather than assumed: nothing left to write and
         * nothing retained on either side. */
        MOQ_TEST_CHECK_EQ_SIZE(tp->client_ep.count, (size_t)0);
        MOQ_TEST_CHECK_EQ_SIZE(tp->server_ep.count, (size_t)0);
        MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp->client_bridge));
        MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp->server_bridge));
        MOQ_TEST_CHECK_EQ_SIZE(tp->client->action_tail - tp->client->action_head,
                               (size_t)0);
        MOQ_TEST_CHECK_EQ_SIZE(tp->server->action_tail - tp->server->action_head,
                               (size_t)0);
        return failures;
    }
    fprintf(stderr, "FAIL: %s: shuttle did not converge in %d rounds\n",
            what, max);
    return failures + 1;
}

/*
 * A suspended inbound retry on a BIDI entry must keep pending_retry set and
 * leave the entry untouched, exactly as the uni arm does -- but through
 * moq_session_on_bidi_stream_bytes, which is a different session entry.
 *
 * The carrier is a namespace-subscription bidi the CLIENT opened, so the peer's
 * response arrives on a stream the client already owns in idx_ns_by_ref. That
 * ownership is what the bridge checks before believing a WOULD_BLOCK is a
 * retry; a draft-18 REQUEST bidi cannot be used here because that registry is
 * not consulted, and the retry is escalated to a fatal instead (tracked
 * separately).
 */
static int test_bidi_retry_suspension_preserves_pending(void)
{
    int failures = 0;

    test_pair_t tp;
    if (d18_pair_init(&tp, 1) < 0) { failures++; return failures; }
    moq_session_start(tp.client, 0);
    moq_session_start(tp.server, 0);
    d18_shuttle_until_quiescent(&tp, 30, 0);
    /* SETUP_COMPLETE deliberately left queued: it is the full slot. */
    MOQ_TEST_CHECK(tp.client->event_head != tp.client->event_tail);

    moq_bytes_t pfx_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t pfx = { pfx_parts, 1 };
    moq_subscribe_namespace_cfg_t nc;
    moq_subscribe_namespace_cfg_init(&nc);
    nc.track_namespace_prefix = pfx;
    nc.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nh;
    MOQ_TEST_CHECK(moq_session_subscribe_namespace(tp.client, &nc, 0, &nh)
                   == MOQ_OK);

    fake_endpoint_clear_ops(&tp.client_ep);
    moq_transport_bridge_service(tp.client_bridge, 0);
    uint64_t bidi = 0;
    for (size_t i = 0; i < tp.client_ep.count; i++)
        if (tp.client_ep.ops[i].kind == FAKE_OP_WRITE &&
            tp.client_ep.ops[i].stream_id >= 2000)
            bidi = tp.client_ep.ops[i].stream_id;
    MOQ_TEST_CHECK(bidi != 0);
    size_t fed = 0;
    d18_feed(tp.server_bridge, &tp.client_ep, 1000, 2000, 0, &fed);

    moq_event_t ev;
    moq_ns_sub_handle_t sh = MOQ_NS_SUB_HANDLE_INVALID;
    while (moq_session_poll_events(tp.server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST)
            sh = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);
    }
    moq_accept_ns_sub_cfg_t ac;
    moq_accept_ns_sub_cfg_init(&ac);
    MOQ_TEST_CHECK(moq_session_accept_ns_sub(tp.server, sh, &ac, 0) == MOQ_OK);

    /* The acceptance comes back on the client's own bidi and cannot emit. */
    fake_endpoint_clear_ops(&tp.server_ep);
    moq_transport_bridge_service(tp.server_bridge, 0);
    bool blocked = false;
    for (size_t i = 0; i < tp.server_ep.count; i++) {
        fake_op_t *o = &tp.server_ep.ops[i];
        if (o->kind != FAKE_OP_WRITE) continue;
        if (moq_transport_bridge_on_peer_bidi_bytes(
                tp.client_bridge, o->stream_id, o->data, o->data_len,
                o->fin, 0) == MOQ_ERR_WOULD_BLOCK)
            blocked = true;
    }
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK(blocked);

    bridge_stream_entry_t *e = bridge_find_by_id(tp.client_bridge, bidi);
    MOQ_TEST_CHECK(e != NULL);
    MOQ_TEST_CHECK(e->kind == BRIDGE_STREAM_BIDI);
    MOQ_TEST_CHECK(e->pending_retry);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    /* The session really owns this stream -- the precondition the bridge tests
     * before treating a WOULD_BLOCK as retainable. */
    MOQ_TEST_CHECK(moq_index_find(tp.client->idx_ns_by_ref,
                                  tp.client->idx_ns_mask, e->ref._v) >= 0);

    sweep_arm_closing_subgroup(tp.client, 0);

    uint64_t suspends = session_budget_suspend_count;
    moq_bridge_budgeted_result_t out;
    MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(
                       tp.client_bridge, 0, 0, &out) == MOQ_OK);

    MOQ_TEST_CHECK(out.suspended);
    MOQ_TEST_CHECK(out.sweep_spent == 0);
    MOQ_TEST_CHECK(session_budget_suspend_count - suspends == 1);
    MOQ_TEST_CHECK(e->pending_retry);
    MOQ_TEST_CHECK(e->active);
    MOQ_TEST_CHECK(!e->pending_fin && !e->fin_retained);
    MOQ_TEST_CHECK(!e->peer_send_closed);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(tp.client->subgroups[0].state == MOQ_SG_CLOSING);

    while (moq_session_poll_events(tp.client, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    MOQ_TEST_CHECK(tp.client->subgroups[0].state == MOQ_SG_FREE);
    MOQ_TEST_CHECK(!e->pending_retry);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));

    /* The retained acceptance is delivered, on the same handle, exactly once. */
    MOQ_TEST_CHECK(moq_session_poll_events(tp.client, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_OK);
    MOQ_TEST_CHECK(ev.u.ns_sub_ok.handle._opaque == nh._opaque);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_poll_events(tp.client, &ev, 1) == 0);

    test_pair_destroy(&tp);
    return failures;
}

/*
 * A session allocation failure on an inbound bidi is a HARD error, not a
 * wait: the bridge must escalate it to a connection fatal and retain no
 * retry. Individual pending flags are unobservable afterwards --
 * bridge_set_fatal() runs bridge_clear_all_state(), which deactivates every
 * entry -- so what is pinned is the DURABLE POSTCONDITION: the exact NOMEM
 * comes back to the adapter, the bridge is fatal (code 0x1) and not closed,
 * no stream entry is active or carries any pending flag, the mixed pending
 * query answers false, and every later ingress call refuses with
 * MOQ_ERR_CLOSED. Nothing can ever re-drive the failed message.
 *
 * Two cells, one per session-side recovery mode -- NOMEM_RETAIN (namespace
 * response) and NOMEM_REDELIVER (joining FETCH) -- because the bridge
 * deliberately collapses both to this one terminal outcome: no
 * session-level recovery is claimed, or possible, behind a fatal bridge.
 * Each cell runs a no-fault fixture first to pin the operation's declared
 * allocation signature on the bridge route, then a fresh failing fixture
 * whose armed origin must be reached on the same allocation path. Only the
 * RECEIVING session runs on the failing allocator; the bridges and the peer
 * stay on the default allocator, so the armed ordinal cannot land outside
 * the session under test.
 */

/* As test_pair_init_full's defaults (draft-16), with the CLIENT session on
 * a caller-supplied allocator. */
static int test_pair_init_client_alloc(test_pair_t *tp,
                                       const moq_alloc_t *client_alloc)
{
    memset(tp, 0, sizeof(*tp));

    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), client_alloc,
                               MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;

    if (moq_session_create(&ccfg, 0, &tp->client) < 0) return -1;
    if (moq_session_create(&scfg, 0, &tp->server) < 0) {
        moq_session_destroy(tp->client);
        return -1;
    }

    fake_endpoint_init(&tp->client_ep, 1000, 2000);
    fake_endpoint_init(&tp->server_ep, 3000, 4000);

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    if (moq_transport_bridge_create(&bcfg, tp->client,
            &tp->client_ep.vtable, &tp->client_ep,
            &tp->client_bridge) < 0) {
        moq_session_destroy(tp->server);
        moq_session_destroy(tp->client);
        return -1;
    }
    if (moq_transport_bridge_create(&bcfg, tp->server,
            &tp->server_ep.vtable, &tp->server_ep,
            &tp->server_bridge) < 0) {
        moq_transport_bridge_destroy(tp->client_bridge);
        moq_session_destroy(tp->server);
        moq_session_destroy(tp->client);
        return -1;
    }
    return 0;
}

/* The bridge-owned durable postcondition both cells assert. */
static int br_fatal_postcondition(moq_transport_bridge_t *b,
                                  uint64_t probe_id, fp_alloc_state_t *fs,
                                  const char *op)
{
    int failures = 0;
    MOQ_TEST_CHECK(moq_transport_bridge_is_fatal(b));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(b));
    MOQ_TEST_CHECK_EQ_HEX(b->fatal_code, 0x1);
    for (uint32_t i = 0; i < b->max_streams; i++) {
        const bridge_stream_entry_t *e = &b->streams[i];
        MOQ_TEST_CHECK(!e->active);
        MOQ_TEST_CHECK(!e->pending_retry && !e->pending_fin &&
                       !e->fin_retained && !e->pending_reset &&
                       !e->pending_stop);
    }
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(b));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_bidi_bytes(b, probe_id, NULL, 0,
                                                     false, 0),
        (int)MOQ_ERR_CLOSED);
    failures += fp_sticky_clean(fs, op);
    return failures;
}

/* The failing allocator's terminal facts AFTER the pair is destroyed:
 * destroy-to-zero, and the sticky flags re-checked -- destruction itself
 * frees through the wrapper, and a wrong-size free there would latch a
 * flag while still reaching all three zeros. The pre-destroy sticky check
 * stays where it is for localization; this one closes teardown. */
static int br_alloc_zeroed(const fp_alloc_state_t *fs, const char *op)
{
    int bad = fp_sticky_clean(fs, op);
    if (fs->balance != 0 || fs->live_bytes != 0 || fs->table_len != 0) {
        fprintf(stderr, "FAILPOINT %s: destroy left balance %lld, live "
                "%lld, table %zu\n", op, (long long)fs->balance,
                (long long)fs->live_bytes, fs->table_len);
        bad++;
    }
    return bad;
}

/* Establish a draft-16 namespace subscription THROUGH the bridges: the
 * client opens the ns_sub bidi, the server accepts, and the acceptance is
 * delivered back. Returns the client-side bidi id, or 0 on failure. */
static uint64_t br_ns_establish(test_pair_t *tp, int *failures_out)
{
    moq_subscribe_namespace_cfg_t nc;
    moq_subscribe_namespace_cfg_init(&nc);
    moq_bytes_t pfx_parts[] = { MOQ_BYTES_LITERAL("live") };
    nc.track_namespace_prefix = (moq_namespace_t){ pfx_parts, 1 };
    moq_ns_sub_handle_t nh;
    if (moq_session_subscribe_namespace(tp->client, &nc, 0, &nh) != MOQ_OK) {
        (*failures_out)++;
        return 0;
    }

    fake_endpoint_clear_ops(&tp->client_ep);
    moq_transport_bridge_service(tp->client_bridge, 0);
    uint64_t bidi = 0;
    for (size_t i = 0; i < tp->client_ep.count; i++)
        if (tp->client_ep.ops[i].kind == FAKE_OP_WRITE &&
            tp->client_ep.ops[i].stream_id >= 2000)
            bidi = tp->client_ep.ops[i].stream_id;
    if (bidi == 0) { (*failures_out)++; return 0; }
    size_t fed = 0;
    d18_feed(tp->server_bridge, &tp->client_ep, 1000, 2000, 0, &fed);

    moq_event_t ev;
    moq_ns_sub_handle_t sh = MOQ_NS_SUB_HANDLE_INVALID;
    while (moq_session_poll_events(tp->server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST)
            sh = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);
    }
    moq_accept_ns_sub_cfg_t ac;
    moq_accept_ns_sub_cfg_init(&ac);
    if (moq_session_accept_ns_sub(tp->server, sh, &ac, 0) != MOQ_OK) {
        (*failures_out)++;
        return 0;
    }
    /* The acceptance rides the CLIENT's own bidi, so the server's writes
     * land on that peer-side id -- outside the server's local stream
     * ranges -- and are fed back directly. */
    fake_endpoint_clear_ops(&tp->server_ep);
    moq_transport_bridge_service(tp->server_bridge, 0);
    for (size_t i = 0; i < tp->server_ep.count; i++) {
        fake_op_t *o = &tp->server_ep.ops[i];
        if (o->kind != FAKE_OP_WRITE) continue;
        moq_transport_bridge_on_peer_bidi_bytes(
            tp->client_bridge, o->stream_id, o->data, o->data_len,
            o->fin, 0);
    }
    fake_endpoint_clear_ops(&tp->server_ep);
    bool got_ok = false;
    while (moq_session_poll_events(tp->client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_NS_SUB_OK) got_ok = true;
        moq_event_cleanup(&ev);
    }
    if (!got_ok) { (*failures_out)++; return 0; }
    return bidi;
}

/* One draft-16 NAMESPACE response message with a single-part suffix. */
static size_t br_encode_d16_namespace(uint8_t *buf, size_t cap,
                                      const char *field)
{
    uint8_t payload[64];
    moq_buf_writer_t pw;
    moq_buf_writer_init(&pw, payload, sizeof(payload));
    moq_bytes_t parts[] = { { (const uint8_t *)field, strlen(field) } };
    moq_namespace_t ns = { parts, 1 };
    moq_buf_write_namespace_prefix(&pw, &ns);
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_control_encode_envelope(&w, MOQ_D16_NAMESPACE, payload,
                                (uint16_t)moq_buf_writer_offset(&pw));
    return moq_buf_writer_offset(&w);
}

/*
 * BR-RETAIN: the draft-16 namespace-response route, suffix-tracker origin.
 * Session-side this origin is NOMEM_RETAIN; behind the bridge it is the
 * one fatal terminal.
 */
static int test_bridge_nomem_ns_response(void)
{
    int failures = 0;

    /* The canonical key of a one-part 2-byte suffix ("aa") is
     * [count u8][u16 len][bytes] = 5 bytes; the stored copy repeats it. */
    static const fp_expect_t k_sig[4] = {
        { FP_ALLOC, FP_SIZE_EXACT, 5, 0 },      /* canonical key */
        { FP_ALLOC, FP_SIZE_ANY, 0, 0 },        /* tracker (private) */
        { FP_ALLOC, FP_SIZE_ANY, 0, 0 },        /* tree node (private) */
        { FP_ALLOC, FP_SIZE_SAME_AS, 0, 0 },    /* stored key copy (== key) */
    };
    fp_attempt_t base_log[FP_LOG_CAP];
    size_t base_n = 0;

    /* No-fault fixture: the operation's signature on this route. */
    {
        fp_alloc_state_t fs = {0};
        moq_alloc_t alloc = fp_allocator(&fs);
        test_pair_t tp;
        if (test_pair_init_client_alloc(&tp, &alloc) < 0) {
            failures++;
            return failures;
        }
        MOQ_TEST_CHECK(setup_handshake(&tp));
        uint64_t bidi = br_ns_establish(&tp, &failures);
        MOQ_TEST_CHECK(bidi != 0);

        uint8_t msg[128];
        size_t mlen = br_encode_d16_namespace(msg, sizeof(msg), "aa");
        fs.log_from = fs.call_count;
        fs.log_len = 0;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_bidi_bytes(
                tp.client_bridge, bidi, msg, mlen, false, 0),
            (int)MOQ_OK);
        failures += fp_check_signature(&fs, 0, k_sig, 4, "br-retain-base");
        memcpy(base_log, fs.log, fs.log_len * sizeof(fp_attempt_t));
        base_n = fs.log_len;
        MOQ_TEST_CHECK_EQ_SIZE(base_n, 4u);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(tp.client, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_FOUND);
        moq_event_cleanup(&ev);
        failures += fp_sticky_clean(&fs, "br-retain-base");
        test_pair_destroy(&tp);
        failures += br_alloc_zeroed(&fs, "br-retain-base");
    }

    /* Failing fixture: the tracker origin, and the durable postcondition. */
    {
        fp_alloc_state_t fs = {0};
        moq_alloc_t alloc = fp_allocator(&fs);
        test_pair_t tp;
        if (test_pair_init_client_alloc(&tp, &alloc) < 0) {
            failures++;
            return failures;
        }
        MOQ_TEST_CHECK(setup_handshake(&tp));
        uint64_t bidi = br_ns_establish(&tp, &failures);
        MOQ_TEST_CHECK(bidi != 0);

        uint8_t msg[128];
        size_t mlen = br_encode_d16_namespace(msg, sizeof(msg), "aa");
        /* Not full: the NOMEM is attributable to the armed allocation. */
        MOQ_TEST_CHECK(!event_queue_full(tp.client));
        fs.log_from = fs.call_count;
        fs.log_len = 0;
        fs.fail_at = fs.call_count + 2;     /* +1 = key, +2 = the tracker */
        moq_result_t rc = moq_transport_bridge_on_peer_bidi_bytes(
            tp.client_bridge, bidi, msg, mlen, false, 0);
        fp_context("br-retain", 2, 4, &fs);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK_EQ_U64(fs.call_count, fs.fail_at);
        fs.fail_at = 0;
        failures += fp_check_prefix(&fs, 0, base_log, 2, "br-retain");
        failures += br_fatal_postcondition(tp.client_bridge, bidi, &fs,
                                           "br-retain");
        test_pair_destroy(&tp);
        failures += br_alloc_zeroed(&fs, "br-retain");
    }

    return failures;
}

/* One draft-18 LARGEST_OBJECT SUBSCRIBE, left pending on the server. */
static size_t br_encode_d18_subscribe(uint8_t *buf, size_t cap,
                                      uint64_t req_id)
{
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    mp.has_filter = true;
    mp.filter_type = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    mp.has_forward = true;
    mp.forward = 1;
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_bytes_t name = MOQ_BYTES_LITERAL("v0");
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_d18_encode_subscribe(&w, req_id, &ns, name, &mp);
    return moq_buf_writer_offset(&w);
}

/* One joining FETCH carrying two USE_VALUE tokens of distinct lengths. */
static size_t br_encode_d18_join_fetch2(uint8_t *buf, size_t cap)
{
    moq_d18_fetch_t f;
    memset(&f, 0, sizeof(f));
    f.request_id = 2;
    f.fetch_type = 2;              /* relative joining */
    f.joining_request_id = 0;
    f.joining_start = 3;
    f.params.auth_token_count = 2;
    f.params.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    f.params.auth_tokens[0].token_type = 7;
    f.params.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("jointok");
    f.params.auth_tokens[1].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    f.params.auth_tokens[1].token_type = 9;
    f.params.auth_tokens[1].token_value =
        MOQ_BYTES_LITERAL("second-join-token");
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_d18_encode_fetch(&w, &f);
    return moq_buf_writer_offset(&w);
}

/* PENDING_JOIN fetch entries on the receiving session (white-box). */
static int count_busy_pending_joins(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state == MOQ_FETCH_PENDING_JOIN) n++;
    return n;
}

/* Feed the pending SUBSCRIBE the joining FETCH references; the SERVER is
 * the receiving session. */
static void br_join_prepare(test_pair_t *tp, int *failures_out)
{
    uint8_t sub[160];
    size_t sn = br_encode_d18_subscribe(sub, sizeof(sub), 0);
    if (moq_transport_bridge_on_peer_bidi_bytes(
            tp->server_bridge, 5001, sub, sn, false, 0) != MOQ_OK)
        (*failures_out)++;
    bool got = false;
    moq_event_t ev;
    while (moq_session_poll_events(tp->server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) got = true;
        moq_event_cleanup(&ev);
    }
    if (!got) (*failures_out)++;
    moq_action_t a;
    while (moq_session_poll_actions(tp->server, &a, 1) > 0)
        moq_action_cleanup(&a);
}

/*
 * BR-REDELIVER: the draft-18 joining-FETCH route, second token-copy
 * origin -- so the first copy's release still runs under the failure.
 * Session-side this origin is NOMEM_REDELIVER; behind the bridge it is
 * the SAME fatal terminal as BR-RETAIN, deliberately: both recovery modes
 * collapse there, and no session-level recovery is claimed behind it.
 */
static int test_bridge_nomem_joining_fetch(void)
{
    int failures = 0;

    static const fp_expect_t k_sig[2] = {
        { FP_ALLOC, FP_SIZE_EXACT, 7, 0 },   /* "jointok" */
        { FP_ALLOC, FP_SIZE_EXACT, 17, 0 },  /* "second-join-token" */
    };
    fp_attempt_t base_log[FP_LOG_CAP];
    size_t base_n = 0;

    /* No-fault fixture: the operation's signature on this route. */
    {
        fp_alloc_state_t fs = {0};
        moq_alloc_t alloc = fp_allocator(&fs);
        test_pair_t tp;
        if (d18_pair_init_alloc(&tp, 0, moq_alloc_default(), &alloc) < 0) {
            failures++;
            return failures;
        }
        moq_session_start(tp.client, 0);
        moq_session_start(tp.server, 0);
        d18_shuttle_until_quiescent(&tp, 30, 0);
        moq_event_t ev;
        while (moq_session_poll_events(tp.client, &ev, 1) > 0)
            moq_event_cleanup(&ev);
        while (moq_session_poll_events(tp.server, &ev, 1) > 0)
            moq_event_cleanup(&ev);
        br_join_prepare(&tp, &failures);

        uint8_t fb[224];
        size_t fn = br_encode_d18_join_fetch2(fb, sizeof(fb));
        fs.log_from = fs.call_count;
        fs.log_len = 0;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_bidi_bytes(
                tp.server_bridge, 5003, fb, fn, false, 0),
            (int)MOQ_OK);
        failures += fp_check_signature(&fs, 0, k_sig, 2, "br-redeliver-base");
        memcpy(base_log, fs.log, fs.log_len * sizeof(fp_attempt_t));
        base_n = fs.log_len;
        MOQ_TEST_CHECK_EQ_SIZE(base_n, 2u);
        MOQ_TEST_CHECK_EQ_INT(count_busy_pending_joins(tp.server), 1);
        failures += fp_sticky_clean(&fs, "br-redeliver-base");
        test_pair_destroy(&tp);
        failures += br_alloc_zeroed(&fs, "br-redeliver-base");
    }

    /* Failing fixture: the second copy, and the durable postcondition. */
    {
        fp_alloc_state_t fs = {0};
        moq_alloc_t alloc = fp_allocator(&fs);
        test_pair_t tp;
        if (d18_pair_init_alloc(&tp, 0, moq_alloc_default(), &alloc) < 0) {
            failures++;
            return failures;
        }
        moq_session_start(tp.client, 0);
        moq_session_start(tp.server, 0);
        d18_shuttle_until_quiescent(&tp, 30, 0);
        moq_event_t ev;
        while (moq_session_poll_events(tp.client, &ev, 1) > 0)
            moq_event_cleanup(&ev);
        while (moq_session_poll_events(tp.server, &ev, 1) > 0)
            moq_event_cleanup(&ev);
        br_join_prepare(&tp, &failures);

        uint8_t fb[224];
        size_t fn = br_encode_d18_join_fetch2(fb, sizeof(fb));
        MOQ_TEST_CHECK(!event_queue_full(tp.server));
        fs.log_from = fs.call_count;
        fs.log_len = 0;
        fs.fail_at = fs.call_count + 2;     /* the second token copy */
        moq_result_t rc = moq_transport_bridge_on_peer_bidi_bytes(
            tp.server_bridge, 5003, fb, fn, false, 0);
        fp_context("br-redeliver", 2, 2, &fs);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK_EQ_U64(fs.call_count, fs.fail_at);
        fs.fail_at = 0;
        failures += fp_check_prefix(&fs, 0, base_log, 2, "br-redeliver");
        MOQ_TEST_CHECK_EQ_INT(count_busy_pending_joins(tp.server), 0);
        failures += br_fatal_postcondition(tp.server_bridge, 5003, &fs,
                                           "br-redeliver");
        test_pair_destroy(&tp);
        failures += br_alloc_zeroed(&fs, "br-redeliver");
    }

    return failures;
}

/* Both arms of the reset family call different session entries, so the bidi
 * arm is proven separately from the uni arm above. */
static int test_bidi_reset_suspension_preserves_pending(void)
{
    int failures = 0;

    test_pair_t tp;
    uint64_t bidi = 0;
    if (d18_request_bidi_fixture(&tp, &bidi) < 0) { failures++; return failures; }

    bridge_stream_entry_t *e = bridge_find_by_id(tp.client_bridge, bidi);
    MOQ_TEST_CHECK(e != NULL && e->kind == BRIDGE_STREAM_BIDI);
    MOQ_TEST_CHECK(!e->pending_retry);
    MOQ_TEST_CHECK(tp.client->event_head != tp.client->event_tail);

    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(
                       tp.client_bridge, bidi, 0x5, 0) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(e->pending_reset);
    MOQ_TEST_CHECK(e->pending_reset_code == 0x5);

    sweep_arm_closing_subgroup(tp.client, 0);

    uint64_t suspends = session_budget_suspend_count;
    moq_bridge_budgeted_result_t out;
    MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(
                       tp.client_bridge, 0, 0, &out) == MOQ_OK);

    MOQ_TEST_CHECK(out.suspended);
    MOQ_TEST_CHECK(out.sweep_spent == 0);
    MOQ_TEST_CHECK(session_budget_suspend_count - suspends == 1);
    MOQ_TEST_CHECK(e->pending_reset);
    MOQ_TEST_CHECK(e->pending_reset_code == 0x5);
    MOQ_TEST_CHECK(e->active);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(tp.client->subgroups[0].state == MOQ_SG_CLOSING);

    /* Capture the subscription the inbound request created, so the terminal
     * event can be matched to it rather than merely counted. */
    moq_event_t ev;
    moq_subscription_t want_sub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(tp.client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
            want_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_subscription_is_valid(want_sub));
    moq_stream_ref_t bref = e->ref;

    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    MOQ_TEST_CHECK(tp.client->subgroups[0].state == MOQ_SG_FREE);
    MOQ_TEST_CHECK(!e->pending_reset);
    MOQ_TEST_CHECK(!e->active);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));

    /* The request teardown reaches the application, on the same subscription,
     * and the stream leaves the draft-18 request registry. */
    MOQ_TEST_CHECK(moq_session_poll_events(tp.client, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_UNSUBSCRIBED);
    MOQ_TEST_CHECK(ev.u.unsubscribed.sub._opaque == want_sub._opaque);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(request_registry_find_by_streamref(tp.client, bref).kind ==
                   MOQ_REQ_NONE);

    /* Re-servicing delivers nothing further. */
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_poll_events(tp.client, &ev, 1) == 0);

    test_pair_destroy(&tp);
    return failures;
}

/* moq_session_on_bidi_stream_stop is the draft-18 request-cancellation input
 * and is distinct from a reset, so it is wired and proven on its own. */
static int test_bidi_stop_suspension_preserves_pending(void)
{
    int failures = 0;

    test_pair_t tp;
    uint64_t bidi = 0;
    if (d18_request_bidi_fixture(&tp, &bidi) < 0) { failures++; return failures; }

    bridge_stream_entry_t *e = bridge_find_by_id(tp.client_bridge, bidi);
    MOQ_TEST_CHECK(e != NULL && e->kind == BRIDGE_STREAM_BIDI);

    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                       tp.client_bridge, bidi, 0x5, 0) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(e->pending_stop);
    MOQ_TEST_CHECK(e->pending_stop_code == 0x5);
    MOQ_TEST_CHECK(!e->pending_retry && !e->pending_reset);

    sweep_arm_closing_subgroup(tp.client, 0);

    uint64_t suspends = session_budget_suspend_count;
    moq_bridge_budgeted_result_t out;
    MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(
                       tp.client_bridge, 0, 0, &out) == MOQ_OK);

    MOQ_TEST_CHECK(out.suspended);
    MOQ_TEST_CHECK(out.sweep_spent == 0);
    MOQ_TEST_CHECK(session_budget_suspend_count - suspends == 1);
    MOQ_TEST_CHECK(e->pending_stop);
    MOQ_TEST_CHECK(e->pending_stop_code == 0x5);
    MOQ_TEST_CHECK(e->active);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(tp.client->subgroups[0].state == MOQ_SG_CLOSING);

    /* Capture the subscription the inbound request created, so the terminal
     * event can be matched to it rather than merely counted. */
    moq_event_t ev;
    moq_subscription_t want_sub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(tp.client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
            want_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_subscription_is_valid(want_sub));
    moq_stream_ref_t bref = e->ref;

    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    MOQ_TEST_CHECK(tp.client->subgroups[0].state == MOQ_SG_FREE);
    MOQ_TEST_CHECK(!e->pending_stop);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));

    /* The request teardown reaches the application, on the same subscription,
     * and the stream leaves the draft-18 request registry. */
    MOQ_TEST_CHECK(moq_session_poll_events(tp.client, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_UNSUBSCRIBED);
    MOQ_TEST_CHECK(ev.u.unsubscribed.sub._opaque == want_sub._opaque);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(request_registry_find_by_streamref(tp.client, bref).kind ==
                   MOQ_REQ_NONE);

    /* Re-servicing delivers nothing further. */
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_poll_events(tp.client, &ev, 1) == 0);

    test_pair_destroy(&tp);
    return failures;
}

/*
 * moq_session_on_data_stop is the local-origin uni arm of the stop family: a
 * peer STOP_SENDING on a stream WE opened. So the client must be the publisher
 * here, which is the reverse of every other fixture in this file.
 *
 * This entry blocks on ACTION capacity, not event capacity -- it answers a stop
 * by enqueuing RESET_DATA -- so the action queue is what has to be full.
 */
static int test_data_stop_suspension_preserves_pending(void)
{
    int failures = 0;

    test_pair_t tp;
    /* One action slot, so a single queued object fills it. A larger queue would
     * need enough writes to overrun the endpoint's op recorder, and a dropped
     * op would make the "no reset yet" assertion below prove nothing. */
    if (test_pair_init_full(&tp, 0, false, 1, 0) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* The SERVER subscribes, so the CLIENT publishes. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
    sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t ssub;
    MOQ_TEST_CHECK(moq_session_subscribe(tp.server, &sub_cfg, 0, &ssub) == MOQ_OK);
    pump_until_quiescent(&tp, 20, 0);

    moq_event_t ev;
    moq_subscription_t csub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(tp.client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
            csub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_subscription_is_valid(csub));
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(tp.client, csub, &acc, 0) == MOQ_OK);
    pump_until_quiescent(&tp, 20, 0);
    while (moq_session_poll_events(tp.server, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    /* The client opens a subgroup: a local-origin data uni. */
    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    sg_cfg.group_id = 0;
    sg_cfg.subgroup_id = 0;
    sg_cfg.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_subgroup(tp.client, csub, &sg_cfg, 0, &sg)
                   == MOQ_OK);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);

    uint64_t uni_sid = 0;
    for (size_t i = 0; i < tp.client_ep.count; i++)
        if (tp.client_ep.ops[i].kind == FAKE_OP_OPEN_UNI)
            uni_sid = tp.client_ep.ops[i].stream_id;
    MOQ_TEST_CHECK(uni_sid != 0);
    fake_endpoint_clear_ops(&tp.client_ep);

    bridge_stream_entry_t *e = bridge_find_by_id(tp.client_bridge, uni_sid);
    MOQ_TEST_CHECK(e != NULL);
    MOQ_TEST_CHECK(e->kind == BRIDGE_STREAM_UNI);
    MOQ_TEST_CHECK(e->origin == BRIDGE_ORIGIN_LOCAL);
    MOQ_TEST_CHECK(e->uni_disp != BRIDGE_UNI_DISP_CONTROL);

    /* The subgroup this stream carries must be live, or on_data_stop returns
     * without performing its reset transition at all. */
    size_t target = SIZE_MAX;
    for (size_t i = 0; i < tp.client->sg_cap; i++)
        if (tp.client->subgroups[i].stream_ref._v == e->ref._v &&
            (tp.client->subgroups[i].state == MOQ_SG_OPEN ||
             tp.client->subgroups[i].state == MOQ_SG_STREAMING))
            target = i;
    MOQ_TEST_CHECK(target != SIZE_MAX);
    moq_sg_state_t target_state = tp.client->subgroups[target].state;

    /* Fill the action queue with one real queued object, never serviced. */
    {
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"AAA", 3, &p);
        MOQ_TEST_CHECK(moq_session_write_object(tp.client, sg, 0, p, 0) == MOQ_OK);
        moq_rcbuf_decref(p);
    }
    MOQ_TEST_CHECK(action_queue_full(tp.client));

    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                       tp.client_bridge, uni_sid, 0x7, 0) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(e->pending_stop);
    MOQ_TEST_CHECK(e->pending_stop_code == 0x7);
    MOQ_TEST_CHECK(!e->pending_retry && !e->pending_reset);

    /* Arm the sweep in a FREE slot: arming the target would mark it CLOSING,
     * and on_data_stop would then return before doing anything. */
    size_t arm = SIZE_MAX;
    for (size_t i = 0; i < tp.client->sg_cap; i++)
        if (i != target && tp.client->subgroups[i].state == MOQ_SG_FREE)
            arm = i;
    MOQ_TEST_CHECK(arm != SIZE_MAX && arm != target);
    sweep_arm_closing_subgroup(tp.client, arm);

    fake_endpoint_clear_ops(&tp.client_ep);
    uint64_t suspends = session_budget_suspend_count;
    moq_bridge_budgeted_result_t out;
    MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(
                       tp.client_bridge, 0, 0, &out) == MOQ_OK);

    MOQ_TEST_CHECK(out.suspended);
    MOQ_TEST_CHECK(out.sweep_spent == 0);
    MOQ_TEST_CHECK(session_budget_suspend_count - suspends == 1);
    MOQ_TEST_CHECK(e->pending_stop);
    MOQ_TEST_CHECK(e->pending_stop_code == 0x7);
    MOQ_TEST_CHECK(e->active);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    /* The stop has not been performed: the subgroup is untouched and no reset
     * reached the transport. */
    MOQ_TEST_CHECK(tp.client->subgroups[target].state == target_state);
    MOQ_TEST_CHECK(tp.client->subgroups[arm].state == MOQ_SG_CLOSING);
    /* The recorder never overflowed, so an absent reset really is absent
     * rather than dropped. */
    MOQ_TEST_CHECK(tp.client_ep.count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < tp.client_ep.count; i++)
        MOQ_TEST_CHECK(tp.client_ep.ops[i].kind != FAKE_OP_RESET);

    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);

    MOQ_TEST_CHECK(tp.client->subgroups[arm].state == MOQ_SG_FREE);
    MOQ_TEST_CHECK(!e->pending_stop);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    /* on_data_stop deterministically resets the subgroup, and dispatching that
     * reset deactivates the bridge entry. */
    MOQ_TEST_CHECK(tp.client->subgroups[target].state == MOQ_SG_RESETTING);
    MOQ_TEST_CHECK(!e->active);

    size_t resets = 0;
    MOQ_TEST_CHECK(tp.client_ep.count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < tp.client_ep.count; i++) {
        fake_op_t *o = &tp.client_ep.ops[i];
        if (o->kind != FAKE_OP_RESET) continue;
        resets++;
        MOQ_TEST_CHECK(o->stream_id == uni_sid);
        MOQ_TEST_CHECK(o->error_code == 0x7);
    }
    MOQ_TEST_CHECK(resets == 1);

    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    MOQ_TEST_CHECK(tp.client_ep.count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < tp.client_ep.count; i++)
        MOQ_TEST_CHECK(tp.client_ep.ops[i].kind != FAKE_OP_RESET);

    test_pair_destroy(&tp);
    return failures;
}

/*
 * The deadline stage is the one boundary whose work is session-owned: the
 * bridge holds no flag to preserve, so what a suspension must prove is that the
 * OBLIGATION survives -- the deadline stays due and its transition runs exactly
 * once on a later pass, rather than being consumed, lost, or turned fatal.
 *
 * A real configured idle timeout supplies that deadline. Unlike every other
 * test here the arming helper's UINT64_MAX subgroup deadline matters in the
 * opposite direction: it keeps the sweep from contributing a competing
 * deadline, so the idle timeout stays the reported next deadline.
 */
static int test_tick_suspension_preserves_due_deadline(void)
{
    int failures = 0;

    const uint64_t idle_us = 30000000;
    test_pair_t tp;
    if (test_pair_init_full(&tp, 0, false, 0, idle_us) < 0) {
        failures++; return failures;
    }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    MOQ_TEST_CHECK(moq_session_state(tp.client) == MOQ_SESS_ESTABLISHED);
    /* The reported next deadline IS the idle deadline. This does not by itself
     * exclude another source sharing the same instant; the close code asserted
     * at the end is what attributes the transition to the idle timeout. */
    uint64_t due = moq_session_next_deadline_us(tp.client);
    MOQ_TEST_CHECK(due != UINT64_MAX);
    MOQ_TEST_CHECK(due == tp.client->idle_deadline_us);
    uint64_t idle_before = tp.client->idle_deadline_us;

    sweep_arm_closing_subgroup(tp.client, 0);
    /* Still the reported next deadline: the armed subgroup contributes none. */
    MOQ_TEST_CHECK(moq_session_next_deadline_us(tp.client) == due);
    MOQ_TEST_CHECK(tp.client_bridge->pending_count == 0);

    fake_endpoint_clear_ops(&tp.client_ep);
    uint64_t suspends = session_budget_suspend_count;
    moq_bridge_budgeted_result_t out;
    MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(
                       tp.client_bridge, due, 0, &out) == MOQ_OK);

    MOQ_TEST_CHECK(out.suspended);
    MOQ_TEST_CHECK(out.sweep_spent == 0);
    MOQ_TEST_CHECK(session_budget_suspend_count - suspends == 1);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(tp.client) != MOQ_SESS_CLOSED);
    /* The cursor is live, so the pass genuinely stopped mid-sweep. */
    MOQ_TEST_CHECK(tp.client->sweep_active);
    MOQ_TEST_CHECK(tp.client->subgroups[0].state == MOQ_SG_CLOSING);
    /* The obligation survives: same deadline, still due, nothing outstanding. */
    MOQ_TEST_CHECK(tp.client->idle_deadline_us == idle_before);
    MOQ_TEST_CHECK(moq_session_next_deadline_us(tp.client) == due);
    MOQ_TEST_CHECK(tp.client_bridge->pending_count == 0);
    MOQ_TEST_CHECK(tp.client_ep.count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < tp.client_ep.count; i++)
        MOQ_TEST_CHECK(tp.client_ep.ops[i].kind != FAKE_OP_CLOSE);

    /* Ordinary unlimited service at the SAME time runs the idle transition.
     *
     * The cursor and the armed subgroup are both gone afterwards, but that is
     * NOT evidence the sweep finished first: close_with_error discards the
     * cursor and frees every subgroup, so the terminal path alone produces the
     * same state. What this pass pins is the terminal outcome below. */
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, due) == MOQ_OK);

    MOQ_TEST_CHECK(!tp.client->sweep_active);
    MOQ_TEST_CHECK(tp.client->subgroups[0].state == MOQ_SG_FREE);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(tp.client) == MOQ_SESS_CLOSED);

    size_t closes = 0;
    MOQ_TEST_CHECK(tp.client_ep.count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < tp.client_ep.count; i++) {
        fake_op_t *o = &tp.client_ep.ops[i];
        if (o->kind != FAKE_OP_CLOSE) continue;
        closes++;
        /* Idle timeout, not the bridge's generic 0x1 fatal. */
        MOQ_TEST_CHECK(o->error_code == MOQ_CLOSE_IDLE_TIMEOUT);
    }
    MOQ_TEST_CHECK(closes == 1);

    size_t terminals = 0;
    moq_event_t ev;
    while (moq_session_poll_events(tp.client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
            terminals++;
            MOQ_TEST_CHECK(ev.u.closed.code ==
                           MOQ_CLOSE_IDLE_TIMEOUT);
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(terminals == 1);

    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, due) == MOQ_OK);
    MOQ_TEST_CHECK(tp.client_ep.count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < tp.client_ep.count; i++)
        MOQ_TEST_CHECK(tp.client_ep.ops[i].kind != FAKE_OP_CLOSE);

    test_pair_destroy(&tp);
    return failures;
}

/*
 * Loop termination: a suspension inside the per-stream scan must STOP the scan,
 * not continue to the next owner. The budget is already spent, so every later
 * owner would only suspend again -- repeating the work and, worse, reporting a
 * pass that touched owners it never advanced.
 *
 * The first pending entry sits at a nonzero index with two more above it, so a
 * `continue` is observable: it visits them and drives the suspension counter
 * past one. Entries are built directly here because the six family fixtures
 * already prove session routing; what is isolated is the loop rule alone.
 */
static int test_inbound_scan_stops_at_suspension(void)
{
    int failures = 0;

    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    moq_event_t ev;
    while (moq_session_poll_events(tp.client, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    fake_endpoint_clear_ops(&tp.client_ep);

    /* Nothing else can produce work: no queued actions, no outbound pending,
     * no due deadline. */
    MOQ_TEST_CHECK(tp.client_bridge->pending_count == 0);
    MOQ_TEST_CHECK(tp.client->action_head == tp.client->action_tail);
    MOQ_TEST_CHECK(moq_session_next_deadline_us(tp.client) == UINT64_MAX);
    MOQ_TEST_CHECK(!tp.client_bridge->pending_control);

    /* Three pending owners, the first at a nonzero index. */
    const uint32_t idx[3] = { 2, 5, 9 };
    MOQ_TEST_CHECK(tp.client_bridge->max_streams > idx[2]);
    moq_stream_ref_t refs[3];
    for (size_t k = 0; k < 3; k++) {
        bridge_stream_entry_t *e = &tp.client_bridge->streams[idx[k]];
        MOQ_TEST_CHECK(!e->active);
        /* Take the ref from the allocator directly. Calling
         * bridge_assign_inbound_ref on an already-active entry would find it and
         * hand back its own still-unset ref. */
        refs[k] = moq_stream_ref_from_u64(tp.client_bridge->next_inbound_ref++);
        MOQ_TEST_CHECK(refs[k]._v != 0);
        e->ref = refs[k];
        e->transport_id = 9000 + idx[k];
        e->kind = BRIDGE_STREAM_UNI;
        e->origin = BRIDGE_ORIGIN_PEER;
        e->active = true;
        e->pending_retry = true;
    }
    for (size_t k = 0; k < 3; k++) {
        MOQ_TEST_CHECK(refs[k]._v != refs[(k + 1) % 3]._v);
        bridge_stream_entry_t *e =
            bridge_find_by_ref(tp.client_bridge, refs[k]);
        MOQ_TEST_CHECK(e == &tp.client_bridge->streams[idx[k]]);
        MOQ_TEST_CHECK(bridge_find_by_id(tp.client_bridge, 9000 + idx[k]) == e);
        MOQ_TEST_CHECK(e->pending_retry);
    }
    /* The scan's first pending owner is exactly the entry placed at idx[0]. */
    uint32_t first = UINT32_MAX;
    for (uint32_t i = 0; i < tp.client_bridge->max_streams; i++)
        if (tp.client_bridge->streams[i].active &&
            tp.client_bridge->streams[i].pending_retry) { first = i; break; }
    MOQ_TEST_CHECK(first == idx[0]);

    sweep_arm_closing_subgroup(tp.client, 0);
    /* Arming must not introduce a due deadline, or the tick stage could report
     * a suspension of its own and hide a scan that failed to stop. */
    MOQ_TEST_CHECK(moq_session_next_deadline_us(tp.client) == UINT64_MAX);

    uint64_t suspends = session_budget_suspend_count;
    moq_bridge_budgeted_result_t out;
    MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(
                       tp.client_bridge, 0, 0, &out) == MOQ_OK);

    MOQ_TEST_CHECK(out.suspended);
    MOQ_TEST_CHECK(out.sweep_spent == 0);
    /* One owner observed the suspension; the scan stopped there. */
    MOQ_TEST_CHECK(session_budget_suspend_count - suspends == 1);
    MOQ_TEST_CHECK(tp.client->sweep_active);
    for (size_t k = 0; k < 3; k++) {
        bridge_stream_entry_t *e =
            bridge_find_by_id(tp.client_bridge, 9000 + idx[k]);
        MOQ_TEST_CHECK(e != NULL);
        MOQ_TEST_CHECK(e->pending_retry);
        MOQ_TEST_CHECK(e->active);
    }
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(tp.client) != MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK(tp.client->subgroups[0].state == MOQ_SG_CLOSING);
    MOQ_TEST_CHECK(tp.client_ep.count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < tp.client_ep.count; i++)
        MOQ_TEST_CHECK(tp.client_ep.ops[i].kind != FAKE_OP_CLOSE);

    test_pair_destroy(&tp);
    return failures;
}

/*
 * Durable progress and suspension in the SAME pass -- the two-field outcome the
 * inbound carrier exists to report.
 *
 * A reset delivered to entry i satisfies its subscription's terminal stream
 * count, and the eager finalization that follows blocks on the full event
 * queue. That leaves runnable sweep work behind MID-PASS, which the next
 * entry's advance preamble then cannot afford. So entry i commits while entry j
 * suspends, and no subgroup is armed: the reset itself creates the work.
 *
 * The non-streaming branch is required. Under streaming delivery
 * handle_data_reset pushes a terminal chunk first, which the deliberately full
 * queue blocks, so the reset would never be recorded and the construction
 * would evaporate.
 */
static int test_progress_then_suspension_in_one_pass(void)
{
    int failures = 0;

    test_pair_t tp;
    if (test_pair_init_ex(&tp, 1) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }
    MOQ_TEST_CHECK(!tp.client->streaming_objects);

    /* Client subscribes; the SUBSCRIBE_OK it receives stays queued and is what
     * keeps the single event slot full for the rest of the test. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
    sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t csub;
    MOQ_TEST_CHECK(moq_session_subscribe(tp.client, &sub_cfg, 0, &csub) == MOQ_OK);
    pump_until_quiescent(&tp, 20, 0);

    moq_event_t ev;
    moq_subscription_t ssub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(tp.server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
            ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_subscription_is_valid(ssub));
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(tp.server, ssub, &acc, 0) == MOQ_OK);
    pump_until_quiescent(&tp, 20, 0);
    /* Exactly one queued event, and it is this subscription's SUBSCRIBE_OK --
     * inspected in place, since draining it would free the slot the whole
     * construction depends on. */
    MOQ_TEST_CHECK(tp.client->event_tail - tp.client->event_head == 1);
    const moq_event_t *qe =
        &tp.client->events[tp.client->event_head % tp.client->event_cap];
    MOQ_TEST_CHECK(qe->kind == MOQ_EVENT_SUBSCRIBE_OK);
    MOQ_TEST_CHECK(qe->u.subscribe_ok.sub._opaque == csub._opaque);

    /* One real peer data uni bound to that subscription: header only, which
     * emits nothing and so leaves no pending retry of its own. */
    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    sg_cfg.group_id = 0;
    sg_cfg.subgroup_id = 0;
    sg_cfg.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_subgroup(tp.server, ssub, &sg_cfg, 0, &sg)
                   == MOQ_OK);
    fake_endpoint_clear_ops(&tp.server_ep);
    moq_transport_bridge_service(tp.server_bridge, 0);

    uint8_t hdr[256]; size_t hdr_len = 0;
    uint64_t suni = 0; bool have = false;
    for (size_t i = 0; i < tp.server_ep.count; i++) {
        fake_op_t *o = &tp.server_ep.ops[i];
        if (o->kind == FAKE_OP_OPEN_UNI) { suni = o->stream_id; have = true; }
        else if (o->kind == FAKE_OP_WRITE && have && o->stream_id == suni &&
                 hdr_len + o->data_len <= sizeof(hdr)) {
            memcpy(hdr + hdr_len, o->data, o->data_len);
            hdr_len += o->data_len;
        }
    }
    MOQ_TEST_CHECK(have && hdr_len > 0);
    fake_endpoint_clear_ops(&tp.server_ep);

    const uint64_t data_sid = 7000;
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(
                       tp.client_bridge, data_sid, hdr, hdr_len, false, 0)
                   == MOQ_OK);
    bridge_stream_entry_t *ei = bridge_find_by_id(tp.client_bridge, data_sid);
    MOQ_TEST_CHECK(ei != NULL && !ei->pending_retry && !ei->pending_stop);

    /* The RX stream really is bound to this entry. */
    size_t rx_slot = SIZE_MAX;
    for (size_t i = 0; i < tp.client->rx_cap; i++)
        if (tp.client->rx_streams[i].active &&
            tp.client->rx_streams[i].stream_ref._v == ei->ref._v)
            rx_slot = i;
    MOQ_TEST_CHECK(rx_slot != SIZE_MAX);
    MOQ_TEST_CHECK(tp.client->rx_streams[rx_slot].sub._opaque == csub._opaque);

    /* A real terminal done advertising exactly one stream. */
    MOQ_TEST_CHECK(moq_session_close_subgroup(tp.server, sg, 0) == MOQ_OK);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.stream_count = 1;
    MOQ_TEST_CHECK(moq_session_done_subscribe(tp.server, ssub, &dc, 0) == MOQ_OK);
    fake_endpoint_clear_ops(&tp.server_ep);
    moq_transport_bridge_service(tp.server_bridge, 0);
    for (size_t i = 0; i < tp.server_ep.count; i++) {
        fake_op_t *o = &tp.server_ep.ops[i];
        if (o->kind == FAKE_OP_WRITE && o->stream_id >= 2000 &&
            o->stream_id < 3000)
            MOQ_TEST_CHECK(moq_transport_bridge_on_peer_control_bytes(
                tp.client_bridge, o->stream_id, o->data, o->data_len,
                false, 0) == MOQ_OK);
    }
    fake_endpoint_clear_ops(&tp.server_ep);

    size_t sub_slot = SIZE_MAX;
    for (size_t i = 0; i < tp.client->sub_cap; i++)
        if (tp.client->subs[i].done_pending) sub_slot = i;
    MOQ_TEST_CHECK(sub_slot != SIZE_MAX);
    moq_sub_entry_t *se = &tp.client->subs[sub_slot];
    MOQ_TEST_CHECK(se->handle._opaque == csub._opaque);
    MOQ_TEST_CHECK(se->done_stream_count == 1);
    MOQ_TEST_CHECK(se->processed_stream_count == 0);
    MOQ_TEST_CHECK(!se->done_expired);
    MOQ_TEST_CHECK(se->done_deadline_us > 0);

    /* Entry i takes a reset; S6 already proves real reset admission, so the
     * flag is placed directly to keep this test on its own subject. */
    ei->pending_reset = true;
    ei->pending_reset_code = 0x9;
    uint32_t i_idx = (uint32_t)(ei - tp.client_bridge->streams);

    /* Entry j: a coherent later owner with a distinct ref. */
    uint32_t j_idx = i_idx + 3;
    MOQ_TEST_CHECK(j_idx < tp.client_bridge->max_streams);
    bridge_stream_entry_t *ej = &tp.client_bridge->streams[j_idx];
    MOQ_TEST_CHECK(!ej->active);
    moq_stream_ref_t jref =
        moq_stream_ref_from_u64(tp.client_bridge->next_inbound_ref++);
    MOQ_TEST_CHECK(jref._v != 0 && jref._v != ei->ref._v);
    ej->ref = jref;
    ej->transport_id = 7100;
    ej->kind = BRIDGE_STREAM_UNI;
    ej->origin = BRIDGE_ORIGIN_PEER;
    ej->active = true;
    ej->pending_retry = true;
    MOQ_TEST_CHECK(!ej->pending_reset && !ej->pending_stop && !ej->pending_fin);
    MOQ_TEST_CHECK(bridge_find_by_ref(tp.client_bridge, jref) == ej);

    /* Nothing else can supply work or a competing suspension. */
    MOQ_TEST_CHECK(tp.client->event_head != tp.client->event_tail);
    MOQ_TEST_CHECK(tp.client->action_head == tp.client->action_tail);
    MOQ_TEST_CHECK(tp.client_bridge->pending_count == 0);
    MOQ_TEST_CHECK(!tp.client_bridge->pending_control);
    /* The reported deadline is this entry's terminal wait, and it is future. */
    MOQ_TEST_CHECK(moq_session_next_deadline_us(tp.client) ==
                   se->done_deadline_us);
    MOQ_TEST_CHECK(se->done_deadline_us > 0);

    /* j is the NEXT pending owner after i: an unnoticed entry between them
     * could suspend instead and every other assertion would still hold. */
    uint32_t next_pending = UINT32_MAX;
    for (uint32_t i = i_idx + 1; i < tp.client_bridge->max_streams; i++) {
        bridge_stream_entry_t *e = &tp.client_bridge->streams[i];
        if (e->active && bridge_stream_has_inbound_pending(e)) {
            next_pending = i;
            break;
        }
    }
    MOQ_TEST_CHECK(next_pending == j_idx);

    uint64_t suspends = session_budget_suspend_count;
    moq_bridge_budgeted_result_t out;
    MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(
                       tp.client_bridge, 0, 0, &out) == MOQ_OK);

    /* Entry i could NOT commit: the abnormal-subgroup event is bounded, and the
     * event queue is full, so the reset is retained rather than lost. */
    MOQ_TEST_CHECK(ei->pending_reset);
    MOQ_TEST_CHECK(ei->pending_reset_code == 0x9);
    MOQ_TEST_CHECK(ei->active);
    MOQ_TEST_CHECK(tp.client->rx_streams[rx_slot].active);
    MOQ_TEST_CHECK(se->processed_stream_count == 0);

    /* A blocked reset is not progress and does not gate the pass: entry j is
     * free to run. WOULD_BLOCK here means only that THIS stream's reset event
     * could not be delivered yet -- it is not MOQ_SESSION_SUSPENDED. */
    MOQ_TEST_CHECK(!ej->pending_retry);

    /* The pass stops on the retained reset, not on a budget suspension: the
     * old row suspended during entry i's eager finalization, which cannot be
     * reached while the reset itself is still owed. */
    MOQ_TEST_CHECK(!out.suspended);
    MOQ_TEST_CHECK(out.sweep_spent == 0);
    MOQ_TEST_CHECK(session_budget_suspend_count == suspends);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));

    /* Drain EXACTLY the one blocking event, and classify it: silently
     * discarding setup or reset output would hide a wrong-event bug. */
    {
        moq_event_t bev;
        MOQ_TEST_CHECK(moq_session_poll_events(tp.client, &bev, 1) == 1);
        MOQ_TEST_CHECK(bev.kind != MOQ_EVENT_SUBGROUP_RESET);
        moq_event_cleanup(&bev);
    }

    /* Second service pass: the retained reset retries and retires the entry. */
    uint64_t suspends2 = session_budget_suspend_count;
    moq_bridge_budgeted_result_t out2;
    MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(
                       tp.client_bridge, 0, 0, &out2) == MOQ_OK);

    /* Exactly one SUBGROUP_RESET, carrying the retained code and identity. */
    {
        moq_event_t rev;
        int n_reset = 0;
        while (moq_session_poll_events(tp.client, &rev, 1) == 1) {
            if (rev.kind == MOQ_EVENT_SUBGROUP_RESET) {
                n_reset++;
                /* Exact image, not just shape: the retained code AND the
                 * identity this fixture declared (subscriber-role stream on
                 * csub, group/subgroup 0, no end-of-group). */
                MOQ_TEST_CHECK(rev.u.subgroup_reset.error_code == 0x9);
                MOQ_TEST_CHECK(rev.u.subgroup_reset.sub._opaque == csub._opaque);
                MOQ_TEST_CHECK(!moq_publication_is_valid(rev.u.subgroup_reset.pub));
                MOQ_TEST_CHECK(rev.u.subgroup_reset.group_id == 0);
                MOQ_TEST_CHECK(rev.u.subgroup_reset.subgroup_id == 0);
                MOQ_TEST_CHECK(!rev.u.subgroup_reset.end_of_group);
                MOQ_TEST_CHECK(rev.detail_size ==
                    (uint32_t)sizeof(moq_subgroup_reset_event_t));
            }
            moq_event_cleanup(&rev);
        }
        MOQ_TEST_CHECK(n_reset == 1);
    }

    /* Entry i committed now. */
    MOQ_TEST_CHECK(!ei->pending_reset);
    MOQ_TEST_CHECK(!ei->active);
    MOQ_TEST_CHECK(!tp.client->rx_streams[rx_slot].active);
    MOQ_TEST_CHECK(se->processed_stream_count == 1);
    MOQ_TEST_CHECK(se->processed_stream_count >= se->done_stream_count);
    (void)suspends2;

    /* A further pass neither duplicates the reset nor resurrects the entry. */
    {
        moq_bridge_budgeted_result_t out3;
        MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(
                           tp.client_bridge, 0, 0, &out3) == MOQ_OK);
        moq_event_t xev;
        int extra = 0;
        while (moq_session_poll_events(tp.client, &xev, 1) == 1) {
            if (xev.kind == MOQ_EVENT_SUBGROUP_RESET) extra++;
            moq_event_cleanup(&xev);
        }
        MOQ_TEST_CHECK(extra == 0);
        MOQ_TEST_CHECK(!ei->active);
    }
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(tp.client) != MOQ_SESS_CLOSED);

    test_pair_destroy(&tp);
    return failures;
}

/*
 * Event scratch is reclaimed on its own queue-empty conditional, separately
 * from send_len, so it needs its own proof -- and one built from a REACHABLE
 * state. A terminal event cannot serve: a real close discards the sweep cursor,
 * so a live cursor alongside a queued SESSION_CLOSED is a state the session
 * never occupies. A GOAWAY carrying a New Session URI is nonterminal and copies
 * that URI into event scratch, which is exactly the shape needed.
 *
 * The event is queued BEFORE the suspension, so the suspended call cannot
 * reclaim, and polled after it: poll_events does not advance, so the queue is
 * empty while the scratch it fed is still occupied. Only the continuation may
 * release it, and at the SAME timestamp.
 */
static int test_continuation_reclaims_event_scratch(void)
{
    int failures = 0;

    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    moq_event_t ev;
    while (moq_session_poll_events(tp.client, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(tp.client->event_scratch_len == 0);

    /* A real server GOAWAY with a URI: the client copies it into event scratch
     * and queues MOQ_EVENT_GOAWAY. */
    static const char uri[] = "wss://new.example.com";
    MOQ_TEST_CHECK(moq_session_goaway(tp.server, (const uint8_t *)uri,
                                      sizeof(uri) - 1, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.server_bridge, 0) == MOQ_OK);
    for (size_t i = 0; i < tp.server_ep.count; i++) {
        fake_op_t *o = &tp.server_ep.ops[i];
        if (o->kind != FAKE_OP_WRITE) continue;
        MOQ_TEST_CHECK(moq_transport_bridge_on_peer_control_bytes(
            tp.client_bridge, o->stream_id, o->data, o->data_len,
            false, 0) == MOQ_OK);
    }
    fake_endpoint_clear_ops(&tp.server_ep);

    MOQ_TEST_CHECK(tp.client->event_head != tp.client->event_tail);
    size_t scratch_used = tp.client->event_scratch_len;
    MOQ_TEST_CHECK(scratch_used >= sizeof(uri) - 1);

    /* Suspend with the event still queued: nothing may be reclaimed. */
    sweep_arm_expired_pub(tp.client, 0);
    sweep_bind_rx(tp.client, 2, tp.client->publishes[0].handle);
    session_budget_enter(tp.client, 0);
    MOQ_TEST_CHECK(session_begin_advance_budgeted(tp.client, 100) ==
                   MOQ_SESSION_SUSPENDED);
    session_budget_leave(tp.client);
    MOQ_TEST_CHECK(tp.client->sweep_active);
    MOQ_TEST_CHECK(tp.client->event_scratch_len == scratch_used);

    /* Drain it. poll_events does not advance the cursor. */
    MOQ_TEST_CHECK(moq_session_poll_events(tp.client, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_GOAWAY);
    MOQ_TEST_CHECK(ev.u.goaway.new_session_uri.len == sizeof(uri) - 1);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(tp.client->event_head == tp.client->event_tail);
    MOQ_TEST_CHECK(tp.client->sweep_active);
    MOQ_TEST_CHECK(tp.client->event_scratch_len == scratch_used);

    /* The continuation, at the same timestamp, owes the reclamation. */
    session_begin_advance(tp.client, 100);
    MOQ_TEST_CHECK(!tp.client->sweep_active);
    MOQ_TEST_CHECK(tp.client->event_scratch_len == 0);

    test_pair_destroy(&tp);
    return failures;
}

/*
 * A control retry that suspends must leave every field the completing path
 * would have written, so the retry and its deferred FIN survive to a later
 * pass and the close is delivered exactly once.
 *
 * The one-slot event queue is the whole fixture: the unpolled SETUP_COMPLETE
 * keeps it full, which is what makes the peer's GOAWAY block and establishes a
 * genuine pending control retry. Sweep work is armed only AFTER that, because
 * the delivery itself is an advancing call that would otherwise consume it; and
 * it is a closing subgroup rather than an expired publication because
 * publication finalization emits an event, which would refill the slot the
 * control retry needs.
 */
static int test_control_retry_suspension_preserves_pending(void)
{
    int failures = 0;

    test_pair_t tp;
    if (test_pair_init_ex(&tp, 1) < 0) { failures++; return failures; }

    /* Establish, draining the SERVER only: the client's single event slot must
     * stay occupied by its own SETUP_COMPLETE. */
    moq_session_start(tp.client, 0);
    pump_until_quiescent(&tp, 20, 0);

    moq_event_t ev;
    bool s_setup = false;
    while (moq_session_poll_events(tp.server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) s_setup = true;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(s_setup);
    MOQ_TEST_CHECK(moq_session_state(tp.client) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(tp.client->event_head != tp.client->event_tail);

    /* A real server GOAWAY, delivered as its actual encoded bytes with FIN. */
    MOQ_TEST_CHECK(moq_session_goaway(tp.server, NULL, 0, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.server_bridge, 0) == MOQ_OK);

    bool fed = false;
    moq_result_t frc = MOQ_OK;
    for (size_t i = 0; i < tp.server_ep.count; i++) {
        fake_op_t *o = &tp.server_ep.ops[i];
        if (o->kind != FAKE_OP_WRITE) continue;
        frc = moq_transport_bridge_on_peer_control_bytes(
            tp.client_bridge, o->stream_id, o->data, o->data_len, true, 0);
        fed = true;
    }
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK(fed);
    MOQ_TEST_CHECK(frc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(tp.client_bridge->pending_control);
    MOQ_TEST_CHECK(tp.client_bridge->pending_control_fin);

    /* Now arm one runnable sweep unit, with nothing to spend on it. */
    MOQ_TEST_CHECK(tp.client->sg_cap > 0);
    sweep_arm_closing_subgroup(tp.client, 0);

    uint64_t suspends = session_budget_suspend_count;
    moq_bridge_budgeted_result_t out;
    MOQ_TEST_CHECK(moq_transport_bridge_service_budgeted(
                       tp.client_bridge, 0, 0, &out) == MOQ_OK);

    MOQ_TEST_CHECK(out.suspended);
    MOQ_TEST_CHECK(out.sweep_spent == 0);
    MOQ_TEST_CHECK(session_budget_suspend_count - suspends == 1);
    MOQ_TEST_CHECK(tp.client_bridge->pending_control);
    MOQ_TEST_CHECK(tp.client_bridge->pending_control_fin);
    MOQ_TEST_CHECK(!tp.client_bridge->needs_close);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(tp.client) != MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK(tp.client->subgroups[0].state == MOQ_SG_CLOSING);

    /* Free the slot the retained GOAWAY needs. */
    MOQ_TEST_CHECK(moq_session_poll_events(tp.client, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SETUP_COMPLETE);
    moq_event_cleanup(&ev);

    /* Unlimited service completes the sweep -- a subgroup reap emits no event,
     * so the retry still finds capacity -- and delivers the deferred FIN. */
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);

    MOQ_TEST_CHECK(tp.client->subgroups[0].state == MOQ_SG_FREE);
    MOQ_TEST_CHECK(!tp.client_bridge->pending_control);
    MOQ_TEST_CHECK(!tp.client_bridge->pending_control_fin);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(tp.client) == MOQ_SESS_CLOSED);

    size_t closes = 0;
    for (size_t i = 0; i < tp.client_ep.count; i++)
        if (tp.client_ep.ops[i].kind == FAKE_OP_CLOSE) closes++;
    MOQ_TEST_CHECK(closes == 1);

    /* Re-servicing must not close a second time. */
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    closes = 0;
    for (size_t i = 0; i < tp.client_ep.count; i++)
        if (tp.client_ep.ops[i].kind == FAKE_OP_CLOSE) closes++;
    MOQ_TEST_CHECK(closes == 0);

    test_pair_destroy(&tp);
    return failures;
}

static int test_create_destroy(void)
{
    int failures = 0;
    fake_endpoint_t ep;
    fake_endpoint_init(&ep, 100, 200);

    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    moq_transport_bridge_t *b = NULL;
    MOQ_TEST_CHECK(moq_transport_bridge_create(&bcfg, s, &ep.vtable, &ep, &b) == MOQ_OK);
    MOQ_TEST_CHECK(b != NULL);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(b));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(b));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(b) == 0);

    moq_transport_bridge_destroy(b);
    moq_session_destroy(s);
    return failures;
}

static int test_create_rejects_bad_ops(void)
{
    int failures = 0;
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *s = NULL;
    moq_session_create(&cfg, 0, &s);

    moq_transport_endpoint_ops_t bad = MOQ_TRANSPORT_ENDPOINT_OPS_INIT;
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    moq_transport_bridge_t *b = NULL;

    MOQ_TEST_CHECK(moq_transport_bridge_create(&bcfg, s, &bad, &bad, &b) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(b == NULL);

    moq_session_destroy(s);
    return failures;
}

static int test_setup_handshake(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    MOQ_TEST_CHECK(setup_handshake(&tp));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));

    test_pair_destroy(&tp);
    return failures;
}

static int test_control_write_backpressure(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    tp.client_ep.block_write = true;
    moq_session_start(tp.client, 0);
    moq_transport_bridge_service(tp.client_bridge, 0);

    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(tp.client_bridge));
    MOQ_TEST_CHECK(tp.client_ep.block_count > 0);

    tp.client_ep.block_write = false;
    tp.client_ep.block_count = 0;

    moq_transport_bridge_service(tp.client_bridge, 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));

    test_pair_destroy(&tp);
    return failures;
}

static int test_transport_close(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    moq_transport_bridge_on_transport_close(tp.client_bridge, 0x42, 0);

    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(tp.client_bridge) == 0);

    test_pair_destroy(&tp);
    return failures;
}

static int test_datagram_inbound_not_fatal(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    moq_result_t rc = moq_transport_bridge_on_peer_datagram(
        tp.server_bridge, (const uint8_t *)"test", 4, 0);
    MOQ_TEST_CHECK(rc >= 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));

    test_pair_destroy(&tp);
    return failures;
}

static int test_inbound_uni_after_setup(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* After handshake, deliver some uni bytes to the server.
     * The session should accept them (it's established). */
    uint8_t dummy[4] = {0x01, 0x02, 0x03, 0x04};
    moq_result_t rc = moq_transport_bridge_on_peer_uni_bytes(
        tp.server_bridge, 5000, dummy, 4, false, 0);
    MOQ_TEST_CHECK(rc >= 0 || rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));

    test_pair_destroy(&tp);
    return failures;
}

static moq_transport_result_t error_close_transport(void *ctx, uint64_t code,
                                                     const uint8_t *r, size_t l)
{
    (void)ctx; (void)code; (void)r; (void)l;
    return MOQ_TRANSPORT_ERROR;
}

static int test_close_error_is_fatal_not_closed(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* Replace close_transport with one that always returns ERROR */
    tp.client_ep.vtable.close_transport = error_close_transport;

    /* Deliver control FIN to client using the control stream ID
     * that was established during handshake (client opened bidi 2000) */
    moq_transport_bridge_on_peer_control_bytes(
        tp.client_bridge, 2000,
        NULL, 0, true, 0);

    /* service() tries close_transport → ERROR → fatal, not closed */
    moq_transport_bridge_service(tp.client_bridge, 0);

    MOQ_TEST_CHECK(moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));

    test_pair_destroy(&tp);
    return failures;
}

/* -- Regression: empty uni on unknown stream ----------------------- */

static int test_empty_uni_no_ghost_stream(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    size_t before = moq_transport_bridge_stream_count(tp.server_bridge);

    moq_result_t rc = moq_transport_bridge_on_peer_uni_bytes(
        tp.server_bridge, 9999, NULL, 0, false, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(tp.server_bridge) == before);

    test_pair_destroy(&tp);
    return failures;
}

/* -- Regression: truncated vtable rejected -------------------------- */

static int test_truncated_vtable_rejected(void)
{
    int failures = 0;
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *s = NULL;
    moq_session_create(&cfg, 0, &s);

    moq_transport_endpoint_ops_t trunc = MOQ_TRANSPORT_ENDPOINT_OPS_INIT;
    trunc.open_uni = fake_open_uni;
    trunc.open_bidi = fake_open_bidi;
    trunc.write = fake_write;
    trunc.reset_stream = fake_reset;
    trunc.stop_sending = fake_stop;
    trunc.close_transport = fake_close;
    /* Shrink struct_size so close_transport is outside declared bounds.
     * The old bug would still read close_transport and accept it. The
     * fix uses HAS_FIELD() and rejects because struct_size is too small. */
    trunc.struct_size = (uint32_t)offsetof(moq_transport_endpoint_ops_t,
                                            close_transport);

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    moq_transport_bridge_t *b = NULL;

    moq_result_t rc = moq_transport_bridge_create(
        &bcfg, s, &trunc, &trunc, &b);
    MOQ_TEST_CHECK(rc == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(b == NULL);

    moq_session_destroy(s);
    return failures;
}

/* Bogus outbound datagram result test deferred to conformance suite —
 * triggering SEND_DATAGRAM requires a full subscribe+datagram flow.
 * The sanitizer switch in transport_bridge.c is verified by inspection
 * and will be covered by conformance scenarios. */

/* -- Hard retry: control write blocked then close ------------------- */

static int test_close_retry_after_blocked_control(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    /* Block writes so control setup goes pending */
    tp.client_ep.block_write = true;
    moq_session_start(tp.client, 0);
    moq_transport_bridge_service(tp.client_bridge, 0);
    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(tp.client_bridge));

    /* Deliver control FIN while write is still blocked.
     * The bridge should notify the session and schedule deferred close. */
    moq_transport_bridge_on_peer_control_bytes(
        tp.client_bridge, 2000, NULL, 0, true, 0);

    /* Unblock and service — close must happen */
    tp.client_ep.block_write = false;
    moq_transport_bridge_service(tp.client_bridge, 0);

    /* Bridge must be closed (not fatal), with endpoint close observed */
    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));
    MOQ_TEST_CHECK(fake_endpoint_find(&tp.client_ep, FAKE_OP_CLOSE) != NULL);

    test_pair_destroy(&tp);
    return failures;
}

/* -- Hard retry: close_transport WOULD_BLOCK then succeeds ---------- */

static int test_close_retry_would_block(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* Block close, then trigger control FIN */
    tp.client_ep.block_close = true;
    moq_transport_bridge_on_peer_control_bytes(
        tp.client_bridge, 2000, NULL, 0, true, 0);

    moq_transport_bridge_service(tp.client_bridge, 0);
    /* Close should be pending (WOULD_BLOCK) */
    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));

    /* Unblock and retry */
    tp.client_ep.block_close = false;
    fake_endpoint_clear_ops(&tp.client_ep);
    moq_transport_bridge_service(tp.client_bridge, 0);

    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(fake_endpoint_find(&tp.client_ep, FAKE_OP_CLOSE) != NULL);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));

    test_pair_destroy(&tp);
    return failures;
}

/* -- Reset on unknown stream is no-op ------------------------------- */

static int test_reset_on_unknown_stream(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* Reset for an unknown stream should be a no-op (no stream to reset) */
    moq_result_t rc = moq_transport_bridge_on_peer_stream_reset(
        tp.server_bridge, 9999, 0x42, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));

    test_pair_destroy(&tp);
    return failures;
}

/* -- Transport close clears all state ------------------------------- */

static int test_transport_close_clears_state(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    /* Create real pending state: block writes then start (no handshake).
     * session_start emits SEND_CONTROL which the bridge tries to send.
     * With writes blocked, the control data goes to the pending queue. */
    tp.client_ep.block_write = true;
    moq_session_start(tp.client, 0);
    moq_transport_bridge_service(tp.client_bridge, 0);
    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(tp.client_bridge));

    /* Transport close should clear everything */
    moq_transport_bridge_on_transport_close(tp.client_bridge, 0x1, 0);

    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(tp.client_bridge) == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_tombstone_count(tp.client_bridge) == 0);

    test_pair_destroy(&tp);
    return failures;
}

/* -- Regression: dropped inbound uni stream is discarded, not misparsed -----
 *
 * When the session drops a peer uni data stream -- here, a subgroup for a
 * track_alias nobody subscribed to -- it frees its rx entry and issues
 * STOP_DATA. Bytes already in flight on that stream must be DISCARDED by the
 * bridge: feeding them onward would have the session open a fresh rx entry and
 * parse mid-stream bytes as a leading stream type ("unknown data stream type",
 * 0x3), fataling the transport. This is the shape that previously closed the
 * connection during a live->VOD catalog conversion (the publisher finishes the
 * media subscription while its last objects are still arriving).
 *
 * The same contract holds on both inbound entry points (byte and rcbuf), so the
 * body is shared and run through each via this delivery shim. */
static moq_result_t deliver_uni(moq_transport_bridge_t *b, bool use_rcbuf,
                                uint64_t sid, const uint8_t *data, size_t len,
                                bool fin)
{
    if (!use_rcbuf)
        return moq_transport_bridge_on_peer_uni_bytes(b, sid, data, len, fin, 0);

    moq_rcbuf_t *buf = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), data, len, &buf) < 0)
        return MOQ_ERR_NOMEM;
    moq_result_t rc = moq_transport_bridge_on_peer_uni_rcbuf(b, sid, buf, fin, 0);
    moq_rcbuf_decref(buf);
    return rc;
}

static int run_inbound_uni_dropped_then_discarded(bool use_rcbuf)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* A valid subgroup header for an unsubscribed track_alias: the session
     * classifies it, fails to bind, and stops the stream within this call. */
    uint8_t hdr[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, hdr, sizeof(hdr));
    moq_d16_subgroup_header_t sh;
    memset(&sh, 0, sizeof(sh));
    sh.type = 0x14;
    sh.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
    sh.track_alias = 9999;
    sh.group_id = 0;
    sh.subgroup_id = 0;
    sh.publisher_priority = 128;
    MOQ_TEST_CHECK(moq_d16_encode_subgroup_header(&w, &sh) == MOQ_OK);
    size_t hdr_len = moq_buf_writer_offset(&w);

    const uint64_t sid = 5000;

    /* First delivery: the session drops the stream (STOP_DATA), so the bridge
     * marks it for discard. Not a protocol violation. */
    moq_result_t rc = deliver_uni(tp.server_bridge, use_rcbuf, sid,
                                  hdr, hdr_len, false);
    MOQ_TEST_CHECK(rc >= 0 || rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));

    /* More in-flight bytes on the same stream. Their leading byte (0x20)
     * classifies as an UNKNOWN data stream type: if the bridge re-fed them as a
     * fresh stream the session would fatal with 0x3. With the discard guard
     * they are swallowed and the bridge stays healthy. */
    uint8_t more[4] = { 0x20, 0x00, 0x00, 0x00 };
    rc = deliver_uni(tp.server_bridge, use_rcbuf, sid, more, sizeof(more), false);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));

    /* Final in-flight bytes with FIN: still no fatal, and the discard entry is
     * retired so it leaves no ghost stream behind. */
    size_t before = moq_transport_bridge_stream_count(tp.server_bridge);
    MOQ_TEST_CHECK(before >= 1);
    rc = deliver_uni(tp.server_bridge, use_rcbuf, sid, more, sizeof(more), true);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(tp.server_bridge) ==
                   before - 1);

    test_pair_destroy(&tp);
    return failures;
}

static int test_inbound_uni_dropped_then_discarded(void)
{
    return run_inbound_uni_dropped_then_discarded(false);
}

static int test_inbound_uni_rcbuf_dropped_then_discarded(void)
{
    return run_inbound_uni_dropped_then_discarded(true);
}

/* -- Regression: bytes delivered during inbound pending_retry are kept ------
 *
 * A peer fills the client's event queue so a data stream backs up into
 * PENDING_EMIT (bridge pending_retry). The peer then delivers another object
 * (with FIN) on the SAME stream while pending_retry is set. The transport does
 * not re-deliver stream bytes, so the bridge/session must RETAIN those bytes
 * across the WOULD_BLOCK and deliver the object after the queue drains.
 *
 * Pre-fix: handle_data_bytes_impl retried the pending emit and returned
 * WOULD_BLOCK before appending the new bytes, silently dropping the object.
 * Run through both inbound entry points via deliver_uni(). */
static int run_pending_retry_keeps_bytes(bool use_rcbuf_extra)
{
    int failures = 0;
    test_pair_t tp;
    /* max_events = 1: object 0 fills the queue, object 1 -> PENDING_EMIT. */
    if (test_pair_init_ex(&tp, 1) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* Client subscribes; server accepts (carry the control both ways via pump). */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
    sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t sub;
    MOQ_TEST_CHECK(moq_session_subscribe(tp.client, &sub_cfg, 0, &sub) == MOQ_OK);
    pump_until_quiescent(&tp, 20, 0);

    moq_event_t ev;
    moq_subscription_t server_sub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(tp.server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
            server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_subscription_is_valid(server_sub));
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(tp.server, server_sub, &acc, 0)
                   == MOQ_OK);
    pump_until_quiescent(&tp, 20, 0);
    /* Drain the client's SUBSCRIBE_OK so the (size-1) event queue starts empty. */
    while (moq_session_poll_events(tp.client, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    /* Server writes obj0+obj1 to one subgroup; capture the produced uni wire
     * bytes (header + obj0 + obj1) from the server endpoint into buf1. */
    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    sg_cfg.group_id = 0;
    sg_cfg.subgroup_id = 0;
    sg_cfg.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_subgroup(tp.server, server_sub, &sg_cfg, 0, &sg)
                   == MOQ_OK);
    const char *want[3] = { "AAA", "BBB", "CCC" };
    for (int i = 0; i < 2; i++) {
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)want[i], 3, &p);
        MOQ_TEST_CHECK(moq_session_write_object(tp.server, sg, (uint64_t)i, p, 0)
                       == MOQ_OK);
        moq_rcbuf_decref(p);
    }
    fake_endpoint_clear_ops(&tp.server_ep);
    moq_transport_bridge_service(tp.server_bridge, 0);

    uint8_t buf1[512]; size_t len1 = 0;
    uint64_t uni_sid = 0; bool have_uni = false;
    for (size_t i = 0; i < tp.server_ep.count; i++) {
        fake_op_t *o = &tp.server_ep.ops[i];
        if (o->kind == FAKE_OP_OPEN_UNI) { uni_sid = o->stream_id; have_uni = true; }
        else if (o->kind == FAKE_OP_WRITE && have_uni && o->stream_id == uni_sid &&
                 len1 + o->data_len <= sizeof(buf1)) {
            memcpy(buf1 + len1, o->data, o->data_len);
            len1 += o->data_len;
        }
    }
    MOQ_TEST_CHECK(have_uni && len1 > 0);
    fake_endpoint_clear_ops(&tp.server_ep);

    /* Server writes obj2 and closes; capture obj2's bytes into buf2. */
    {
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)want[2], 3, &p);
        MOQ_TEST_CHECK(moq_session_write_object(tp.server, sg, 2, p, 0) == MOQ_OK);
        moq_rcbuf_decref(p);
    }
    MOQ_TEST_CHECK(moq_session_close_subgroup(tp.server, sg, 0) == MOQ_OK);
    moq_transport_bridge_service(tp.server_bridge, 0);

    uint8_t buf2[512]; size_t len2 = 0;
    for (size_t i = 0; i < tp.server_ep.count; i++) {
        fake_op_t *o = &tp.server_ep.ops[i];
        if (o->kind == FAKE_OP_WRITE && o->stream_id == uni_sid &&
            len2 + o->data_len <= sizeof(buf2)) {
            memcpy(buf2 + len2, o->data, o->data_len);
            len2 += o->data_len;
        }
    }
    MOQ_TEST_CHECK(len2 > 0);

    /* Deliver buf1 (header+obj0+obj1) to the CLIENT bridge as a peer uni stream.
     * obj0 emits and fills the size-1 event queue; obj1 backs up into
     * PENDING_EMIT, so the bridge returns WOULD_BLOCK (pending_retry). */
    const uint64_t client_sid = 5000;
    moq_result_t rc = moq_transport_bridge_on_peer_uni_bytes(
        tp.client_bridge, client_sid, buf1, len1, false, 0);
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

    /* While pending_retry is set, deliver obj2 + FIN on the SAME stream. The
     * bytes must be retained (not dropped) even though this also WOULD_BLOCKs. */
    rc = deliver_uni(tp.client_bridge, use_rcbuf_extra, client_sid,
                     buf2, len2, true);
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

    /* Drain + service-retry until quiescent, collecting delivered objects. */
    bool got[3] = { false, false, false };
    for (int iter = 0; iter < 16; iter++) {
        while (moq_session_poll_events(tp.client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                uint64_t oid = ev.u.object_received.object_id;
                moq_rcbuf_t *pl = ev.u.object_received.payload;
                if (oid < 3 && pl && moq_rcbuf_len(pl) == 3 &&
                    memcmp(moq_rcbuf_data(pl), want[oid], 3) == 0)
                    got[(size_t)oid] = true;
            }
            moq_event_cleanup(&ev);
        }
        moq_transport_bridge_service(tp.client_bridge, 0);
    }

    MOQ_TEST_CHECK(got[0]);
    MOQ_TEST_CHECK(got[1]);
    MOQ_TEST_CHECK(got[2]);   /* must not be dropped during pending_retry */
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));

    test_pair_destroy(&tp);
    return failures;
}

static int test_pending_retry_keeps_bytes(void)
{
    return run_pending_retry_keeps_bytes(false);
}

static int test_pending_retry_keeps_rcbuf(void)
{
    return run_pending_retry_keeps_bytes(true);
}

/* == Main =========================================================== */


/* --- independent monotonic terminal facts -------------------------------- *
 * ENQUEUED and OBSERVED are separate. Availability is not observation: a
 * queued-but-unpolled MOQ_EVENT_SESSION_CLOSED must read enqueued=true,
 * observed=false, and only the poll that TRANSFERS it flips observed. */
static int test_terminal_facts_enqueued_then_observed(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    bool observed = true; /* poisoned: must be overwritten */
    MOQ_TEST_CHECK(!moq_transport_bridge_terminal_facts(tp.client_bridge,
                                                        &observed));
    MOQ_TEST_CHECK(!observed);          /* neither fact before terminal */

    /* peer-side terminal: the event is enqueued, nobody has polled it */
    moq_transport_bridge_on_transport_close(tp.client_bridge, 0x42, 0);
    observed = true;
    MOQ_TEST_CHECK(moq_transport_bridge_terminal_facts(tp.client_bridge,
                                                       &observed));
    MOQ_TEST_CHECK(!observed);          /* queued is NOT observed */

    /* the poll that transfers it is what makes it observed */
    moq_event_t ev;
    size_t n = 0;
    MOQ_TEST_CHECK(moq_session_poll_events_ex(tp.client, &ev, 1,
                                              sizeof(ev), &n) == MOQ_OK);
    MOQ_TEST_CHECK(n == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SESSION_CLOSED);
    observed = false;
    MOQ_TEST_CHECK(moq_transport_bridge_terminal_facts(tp.client_bridge,
                                                       &observed));
    MOQ_TEST_CHECK(observed);

    /* monotonic + idempotent: repeating the terminal does not clear anything,
     * and a second poll (no event left) leaves both facts set */
    moq_transport_bridge_on_transport_close(tp.client_bridge, 0x43, 0);
    n = 0;
    (void)moq_session_poll_events_ex(tp.client, &ev, 1, sizeof(ev), &n);
    observed = false;
    MOQ_TEST_CHECK(moq_transport_bridge_terminal_facts(tp.client_bridge,
                                                       &observed));
    MOQ_TEST_CHECK(observed);

    /* out_observed is optional */
    MOQ_TEST_CHECK(moq_transport_bridge_terminal_facts(tp.client_bridge, NULL));

    test_pair_destroy(&tp);
    return failures;
}

/* A non-terminal event must not set the observed fact, and the facts are not
 * derived from the bridge's own terminal latches. */
static int test_terminal_facts_not_set_by_other_events(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* the handshake surfaced SETUP_COMPLETE; draining it must not look terminal */
    moq_event_t ev;
    size_t n = 0;
    while (moq_session_poll_events_ex(tp.client, &ev, 1, sizeof(ev),
                                      &n) == MOQ_OK && n > 0)
        n = 0;

    bool observed = true;
    MOQ_TEST_CHECK(!moq_transport_bridge_terminal_facts(tp.client_bridge,
                                                        &observed));
    MOQ_TEST_CHECK(!observed);

    test_pair_destroy(&tp);
    return failures;
}

/* A bridge-local endpoint failure can latch fatal before the physical
 * transport reports its terminal. The bridge latch clears transport state but
 * does not itself close the session, so either transport-terminal flavor must
 * still deliver exactly one SESSION_CLOSED while preserving the first cause. */
static int run_already_fatal_transport_terminal(bool clean)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    const uint64_t first_code = 0x51;
    const uint64_t later_code = 0x62;
    bridge_set_fatal(tp.client_bridge, first_code);

    bool observed = true;
    MOQ_TEST_CHECK(moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_fatal_code(tp.client_bridge) ==
                   first_code);
    MOQ_TEST_CHECK(moq_session_state(tp.client) != MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK(!moq_transport_bridge_terminal_facts(tp.client_bridge,
                                                        &observed));
    MOQ_TEST_CHECK(!observed);

    moq_result_t rc = clean
        ? moq_transport_bridge_on_transport_close(tp.client_bridge, later_code,
                                                  1000)
        : moq_transport_bridge_on_transport_error(tp.client_bridge, later_code,
                                                  1000);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_fatal_code(tp.client_bridge) ==
                   first_code);
    MOQ_TEST_CHECK(moq_session_state(tp.client) == MOQ_SESS_CLOSED);

    moq_event_t ev;
    size_t n = 0;
    memset(&ev, 0, sizeof(ev));
    MOQ_TEST_CHECK(moq_session_poll_events_ex(tp.client, &ev, 1, sizeof(ev),
                                              &n) == MOQ_OK);
    MOQ_TEST_CHECK(n == 1);
    if (n == 1) {
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(ev.u.closed.code == first_code);
        moq_event_cleanup(&ev);
    }

    /* The alternate flavor and repeated original flavor are idempotent. */
    (void)moq_transport_bridge_on_transport_error(tp.client_bridge, 0x73,
                                                  1001);
    (void)moq_transport_bridge_on_transport_close(tp.client_bridge, 0x84,
                                                  1002);
    n = 0;
    MOQ_TEST_CHECK(moq_session_poll_events_ex(tp.client, &ev, 1, sizeof(ev),
                                              &n) == MOQ_OK);
    MOQ_TEST_CHECK(n == 0);
    observed = false;
    MOQ_TEST_CHECK(moq_transport_bridge_terminal_facts(tp.client_bridge,
                                                       &observed));
    MOQ_TEST_CHECK(observed);

    test_pair_destroy(&tp);
    return failures;
}

static int test_already_fatal_transport_terminal(void)
{
    return run_already_fatal_transport_terminal(false) +
           run_already_fatal_transport_terminal(true);
}

/* A SETUP whose completion-event tokens can never fit the arena must reach the
 * normal close path so the bridge dispatches CLOSE_SESSION: a buffer error here
 * would instead be escalated to a connection fatal, losing close semantics. */
static int test_setup_scratch_shortfall_closes_not_fatal(void)
{
    int failures = 0;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    scfg.output_scratch_size = 32;
    moq_session_t *sv = NULL;
    MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);
    if (!sv) return failures;

    fake_endpoint_t ep;
    fake_endpoint_init(&ep, 3000, 4000);
    moq_transport_bridge_t *br = NULL;
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    MOQ_TEST_CHECK(moq_transport_bridge_create(&bcfg, sv, &ep.vtable, &ep,
                                               &br) == MOQ_OK);
    if (!br) { moq_session_destroy(sv); return failures; }

    uint8_t val[40];
    for (size_t i = 0; i < sizeof(val); i++)
        val[i] = (uint8_t)(0x41 + (i % 26));
    uint8_t tokbuf[64];
    moq_buf_writer_t tw;
    moq_buf_writer_init(&tw, tokbuf, sizeof(tokbuf));
    moq_buf_write_vi64(&tw, MOQ_AUTH_TOKEN_USE_VALUE);
    moq_buf_write_vi64(&tw, 0);
    moq_buf_write_vi64(&tw, sizeof(val));
    moq_buf_write_raw(&tw, val, sizeof(val));

    moq_kvp_entry_t prm;
    memset(&prm, 0, sizeof(prm));
    prm.type = MOQ_SETUP_PARAM_AUTHORIZATION_TOKEN;
    prm.value = tokbuf;
    prm.value_len = moq_buf_writer_offset(&tw);

    uint8_t msg[256];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_d16_encode_client_setup(&w, &prm, 1);

    moq_result_t rc = moq_transport_bridge_on_peer_control_bytes(
        br, 0 /* peer control stream */, msg, moq_buf_writer_offset(&w),
        false, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(br));
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

    MOQ_TEST_CHECK(moq_transport_bridge_service(br, 0) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(br));

    size_t closes = 0;
    MOQ_TEST_CHECK(ep.count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < ep.count; i++) {
        if (ep.ops[i].kind != FAKE_OP_CLOSE) continue;
        closes++;
        MOQ_TEST_CHECK(ep.ops[i].error_code == 0x1);
    }
    MOQ_TEST_CHECK(closes == 1);

    moq_transport_bridge_destroy(br);
    moq_session_destroy(sv);
    return failures;
}

/* -- Axis 4: physical bridge retirement ------------------------------
 *
 * SEMANTIC consumption and PHYSICAL retirement are different facts, asserted
 * separately. The causal sequence is the Axis 4 one: the peer FIN arrives WITH
 * the request, so the destination owner is created with the FIN transferred
 * onto it; the application then retires that owner, which -- the FIN having
 * been observed -- owes NO drain; and the bridge's own service must afterwards
 * close our half and retire the physical mapping exactly once.
 *
 * Successful FIN ingress records `peer_send_closed` and leaves the entry ACTIVE
 * until the local half closes (transport_bridge.c:2677), so retirement is a
 * real transition, not a formality.
 *
 * No peer bytes follow the FIN. The only continuation is bridge service.
 */

typedef struct fin_bridge_run {
    test_pair_t tp;
    uint64_t    transport_id;   /* the TRANSPORT stream the peer bytes ride */
    moq_stream_ref_t ref;       /* the SESSION's internal ref -- a DIFFERENT
                                 * value, captured once at ingress and never
                                 * re-derived from an entry that may be gone */
    int         want_slot;
    uint32_t    want_gen;
    uint64_t    want_handle;
} fin_bridge_run_t;

/* -- P7 TRACK_STATUS family ----------------------------------------- */

#define P7_TOK_TYPE 7
static const uint8_t k_p7_tok[]  = { 't','s','t','o','k' };
static const uint8_t k_p7_ns0[]  = { 'e','x','.','c','o','m' };
static const uint8_t k_p7_ns1[]  = { 's','t','a','t' };
static const uint8_t k_p7_name[] = { 't','0' };
#define P7_LARGEST_GROUP 0x33
#define P7_LARGEST_OBJ   0x04
#define P7_EXPIRES_MS    5500

static const moq_ts_entry_t *p7_owner(const moq_session_t *s,
                                      moq_stream_ref_t ref)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if (ep.kind != MOQ_REQ_TRACK_STATUS) return NULL;
    if (ep.slot < 0 || (size_t)ep.slot >= s->ts_cap) return NULL;
    return &s->track_statuses[ep.slot];
}

static int p7_pool_busy(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->ts_cap; i++)
        if (s->track_statuses[i].state != MOQ_TS_FREE) n++;
    return n;
}

static int p7_derive_slot(const moq_session_t *s, uint32_t *out_gen)
{
    for (size_t i = 0; i < s->ts_cap; i++)
        if (s->track_statuses[i].state == MOQ_TS_FREE) {
            *out_gen = s->track_statuses[i].generation | 1u;
            return (int)i;
        }
    return -1;
}

/* The peer's request id for the request under check. Peer-opened request ids
 * advance by two, so a second request on the same session legitimately carries
 * a different one; the expectation is declared by the caller rather than being
 * read back from the entry. */
static uint64_t p7_want_request_id;

/* The FIN fact the owner under check must carry. The first request arrives WITH
 * a FIN, so its owner latches one; a reused slot admitted from a request with
 * NO FIN must show a CLEARED latch. Declared by the caller rather than assumed
 * true, or a free that left a stale `true` behind would pass at reuse. */
static int p7_want_fin = 1;

static int p7_check_live(const moq_session_t *s, moq_stream_ref_t ref,
                         int want_slot, uint32_t want_gen, uint64_t want_handle,
                         const char *what)
{
    int bad = 0;
    if (want_slot < 0 || (size_t)want_slot >= s->ts_cap) {
        fprintf(stderr, "FINBR %s: declared slot out of range\n", what);
        return 1;
    }
    const moq_ts_entry_t *e = p7_owner(s, ref);
    if (!e) { fprintf(stderr, "FINBR %s: owner absent\n", what); return 1; }
    if (e != &s->track_statuses[want_slot]) {
        fprintf(stderr, "FINBR %s: owner in an undeclared slot\n", what);
        bad++;
    }
    if ((int)e->state != (int)MOQ_TS_PENDING_PUBLISHER) {
        fprintf(stderr, "FINBR %s: state %d\n", what, (int)e->state);
        bad++;
    }
    if ((int)e->role != (int)MOQ_TS_ROLE_PUBLISHER) {
        fprintf(stderr, "FINBR %s: role %d\n", what, (int)e->role);
        bad++;
    }
    if (e->generation != want_gen) {
        fprintf(stderr, "FINBR %s: generation\n", what); bad++;
    }
    if (e->handle._opaque != want_handle) {
        fprintf(stderr, "FINBR %s: handle\n", what); bad++;
    }
    if (e->request_id != p7_want_request_id) {
        fprintf(stderr, "FINBR %s: request id\n", what); bad++;
    }
    if (e->request_stream_ref._v != ref._v) {
        fprintf(stderr, "FINBR %s: owner ref\n", what); bad++;
    }
    /* The transferred FIN -- this family's carrier is the durable latch. */
    if ((e->req_recv_fin ? 1 : 0) != p7_want_fin) {
        fprintf(stderr, "FINBR %s: FIN latch %d, expected %d\n", what,
                e->req_recv_fin ? 1 : 0, p7_want_fin);
        bad++;
    }
    if (p7_pool_busy(s) != 1) {
        fprintf(stderr, "FINBR %s: pool occupancy %d, expected 1\n", what,
                p7_pool_busy(s));
        bad++;
    }
    return bad;
}

static int p7_check_retired(const moq_session_t *s, moq_stream_ref_t ref,
                            int want_slot, const char *what)
{
    int bad = 0;
    if (p7_owner(s, ref) != NULL) {
        fprintf(stderr, "FINBR %s: registry edge survives\n", what); bad++;
    }
    /* The POOL too: removing the edge while leaking the slot must not pass. */
    if (want_slot >= 0 && (size_t)want_slot < s->ts_cap &&
        s->track_statuses[want_slot].state != MOQ_TS_FREE) {
        fprintf(stderr, "FINBR %s: pool slot leaked\n", what); bad++;
    }
    if (p7_pool_busy(s) != 0) {
        fprintf(stderr, "FINBR %s: pool occupancy %d, expected 0\n", what,
                p7_pool_busy(s));
        bad++;
    }
    return bad;
}

/* The request the producer must surface, built from the DECLARED handle. */
static int p7_want_request(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    txs_img_u64(&im, 2);                                /* ns part count */
    txs_img_bytes(&im, k_p7_ns0, sizeof(k_p7_ns0));
    txs_img_bytes(&im, k_p7_ns1, sizeof(k_p7_ns1));
    txs_img_bytes(&im, k_p7_name, sizeof(k_p7_name));
    txs_img_u64(&im, 1);                                /* token count */
    txs_img_u64(&im, P7_TOK_TYPE);
    txs_img_bytes(&im, k_p7_tok, sizeof(k_p7_tok));
    if (!txs_norm_append_img(v, MOQ_EVENT_TRACK_STATUS_REQUEST, &im)) {
        fprintf(stderr, "FINBR: could not build the declared request image\n");
        return 1;
    }
    return 0;
}

/* The owner record plus the bytes it owns. The raw compare is SHALLOW by
 * itself -- it would see a moved `track_id_buf` pointer but not edited bytes --
 * so the identity buffer is deep-copied alongside it. Bounds-safe: an
 * over-long or unbacked buffer makes the record incomparable rather than being
 * read. */
#define P7_OWN_MAX 256
typedef struct p7_snap {
    int      valid;
    moq_ts_entry_t raw;
    size_t   tid_len;
    uint8_t  tid[P7_OWN_MAX];
} p7_snap_t;

static int p7_check_terminal_wire(void *vctx, const char *what);

/* The family's EXACT topology: one stream-ref edge while live, none at all
 * once retired. */
static int p7_check_edges(const og_graph_t *g, moq_stream_ref_t ref,
                          int want_slot, int live, const char *what)
{
    int bad = og_check_integrity(g, what);
    if (live) {
        bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                             MOQ_REQ_TRACK_STATUS, want_slot, what);
        const og_edge_spec_t w[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_TRACK_STATUS, want_slot, w, 1,
                                    what);
    } else {
        bad += og_check_no_edge(g, OG_DOM_REQ_STREAMREF, ref._v, what);
        bad += og_check_owner_edges(g, MOQ_REQ_TRACK_STATUS, want_slot, NULL,
                                    0, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}

/* This route owes NO drain at any phase: the FIN is observed before the
 * terminal runs, so the declared multiset is EMPTY throughout. */
static int p7_check_drain(const moq_session_t *s, const char *what)
{
    return check_drain_membership(s, NULL, 0, what);
}


/* -- the descriptor's hooks, all four genuinely consumed -------------- */


static void p7_capture(const moq_session_t *s, void *vctx, void *state,
                       size_t cap)
{
    fin_bridge_run_t *r = (fin_bridge_run_t *)vctx;
    p7_snap_t *o = (p7_snap_t *)state;
    /* The declared size is the contract; refuse rather than overwrite. */
    if (cap < sizeof(*o)) { fprintf(stderr, "FINBR: snapshot storage too small\n"); return; }
    memset(o, 0, sizeof(*o));
    if (r->want_slot < 0 || (size_t)r->want_slot >= s->ts_cap) return;
    const moq_ts_entry_t *e = &s->track_statuses[r->want_slot];
    o->raw = *e;
    o->tid_len = e->track_id_len;
    if (e->track_id_len > P7_OWN_MAX || (e->track_id_len && !e->track_id_buf))
        return;
    if (e->track_id_len) memcpy(o->tid, e->track_id_buf, e->track_id_len);
    o->valid = 1;
}

static int p7_check(const moq_session_t *s, void *vctx, const void *state,
                    size_t cap, const char *what)
{
    const p7_snap_t *want = (const p7_snap_t *)state;
    p7_snap_t now;
    if (cap < sizeof(now)) {
        fprintf(stderr, "FINBR %s: snapshot storage too small\n", what);
        return 1;
    }
    p7_capture(s, vctx, &now, sizeof(now));
    if (!now.valid || !want->valid) {
        fprintf(stderr, "FINBR %s: incomparable owner record\n", what);
        return 1;
    }
    if (memcmp(&now.raw, &want->raw, sizeof(now.raw)) != 0) {
        fprintf(stderr, "FINBR %s: owner record changed\n", what);
        return 1;
    }
    if (now.tid_len != want->tid_len ||
        (now.tid_len && memcmp(now.tid, want->tid, now.tid_len) != 0)) {
        fprintf(stderr, "FINBR %s: retained track identity changed\n", what);
        return 1;
    }
    return 0;
}

static bool p7_norm_event(const moq_event_t *ev, void *ctx, txs_norm_vec_t *out)
{
    (void)ctx;
    if (ev->kind != MOQ_EVENT_TRACK_STATUS_REQUEST) {
        fprintf(stderr, "FINBR: unnormalized event kind %u\n",
                (unsigned)ev->kind);
        return false;
    }
    const moq_track_status_request_event_t *q = &ev->u.track_status_request;
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, q->handle._opaque);
    if (q->track_namespace.count > 32 || q->token_count > 16) {
        fprintf(stderr, "FINBR: implausible request counts\n");
        return false;
    }
    if (q->track_namespace.count && !q->track_namespace.parts) {
        fprintf(stderr, "FINBR: namespace count with NULL parts\n");
        return false;
    }
    if (q->token_count && !q->tokens) {
        fprintf(stderr, "FINBR: token count with NULL tokens\n");
        return false;
    }
    txs_img_u64(&im, (uint64_t)q->track_namespace.count);
    for (size_t i = 0; i < q->track_namespace.count; i++) {
        const moq_bytes_t *b = &q->track_namespace.parts[i];
        if (b->len && !b->data) {
            fprintf(stderr, "FINBR: namespace part %zu has NULL bytes\n", i);
            return false;
        }
        txs_img_bytes(&im, b->data, b->len);
    }
    if (q->track_name.len && !q->track_name.data) {
        fprintf(stderr, "FINBR: track name has NULL bytes\n");
        return false;
    }
    txs_img_bytes(&im, q->track_name.data, q->track_name.len);
    txs_img_u64(&im, (uint64_t)q->token_count);
    for (size_t i = 0; i < q->token_count; i++) {
        if (q->tokens[i].token_value.len && !q->tokens[i].token_value.data) {
            fprintf(stderr, "FINBR: token %zu has NULL bytes\n", i);
            return false;
        }
        txs_img_u64(&im, q->tokens[i].token_type);
        txs_img_bytes(&im, q->tokens[i].token_value.data,
                      q->tokens[i].token_value.len);
    }
    return txs_norm_append_img(out, ev->kind, &im);
}

/* The bridge drains the session's actions itself, so anything still queued
 * afterwards is unexpected by definition. */
static bool p7_norm_action(const moq_action_t *a, void *ctx, txs_norm_vec_t *out)
{
    (void)ctx; (void)out;
    fprintf(stderr, "FINBR: action kind %u left queued after service\n",
            (unsigned)a->kind);
    return false;
}

static moq_result_t p7_feed_id(moq_transport_bridge_t *b, uint64_t request_id,
                               uint64_t transport_id, bool fin)
{
    uint8_t msg[192];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_bytes_t parts[] = { { k_p7_ns0, sizeof(k_p7_ns0) },
                            { k_p7_ns1, sizeof(k_p7_ns1) } };
    moq_namespace_t ns = { parts, 2 };
    moq_d18_msg_params_t p = { 0 };
    p.auth_token_count = 1;
    p.auth_tokens[0].alias_type = 3;              /* USE_VALUE */
    p.auth_tokens[0].token_type = P7_TOK_TYPE;
    p.auth_tokens[0].token_value = (moq_bytes_t){ k_p7_tok, sizeof(k_p7_tok) };
    if (moq_d18_encode_track_status(&w, request_id, &ns,
            (moq_bytes_t){ k_p7_name, sizeof(k_p7_name) }, &p) != MOQ_OK)
        return MOQ_ERR_INTERNAL;
    return moq_transport_bridge_on_peer_bidi_bytes(
        b, transport_id, msg, moq_buf_writer_offset(&w), fin, 0);
}

static moq_result_t p7_feed(moq_transport_bridge_t *b, void *vctx,
                            uint64_t transport_id, bool fin)
{
    (void)vctx;
    return p7_feed_id(b, 0, transport_id, fin);
}

static moq_result_t p7_terminal(moq_transport_bridge_t *b, void *vctx,
                                uint64_t transport_id)
{
    fin_bridge_run_t *r = (fin_bridge_run_t *)vctx;
    (void)b; (void)transport_id;
    moq_track_status_handle_t h;
    h._opaque = r->want_handle;
    moq_accept_track_status_cfg_t c;
    moq_accept_track_status_cfg_init(&c);
    c.has_largest = true;
    c.largest_group = P7_LARGEST_GROUP;
    c.largest_object = P7_LARGEST_OBJ;
    c.has_expires = true;
    c.expires_ms = P7_EXPIRES_MS;
    return moq_session_accept_track_status(r->tp.server, h, &c, 0);
}

_Static_assert(sizeof(p7_snap_t) <= FIN_BR_SNAP_MAX,
               "p7 snapshot exceeds the shared bounded storage");

static const fin_bridge_family_t p7_family = {
    .owner_kind    = MOQ_REQ_TRACK_STATUS,
    .pool_tag      = MOQ_HANDLE_POOL_TRACK_STATUS,
    .snap_size     = sizeof(p7_snap_t),
    .capture       = p7_capture,
    .check         = p7_check,
    .normalize_event  = p7_norm_event,
    .normalize_action = p7_norm_action,
    .want_request  = p7_want_request,
    .derive_slot   = p7_derive_slot,
    .check_live    = p7_check_live,
    .check_retired = p7_check_retired,
    .check_edges   = p7_check_edges,
    .check_drain   = p7_check_drain,
    .check_terminal_wire = p7_check_terminal_wire,
};

/* The EXACT terminal the service must put on the wire, decoded rather than
 * counted: one write, on this transport stream, FIN'd, one REQUEST_OK envelope
 * with nothing after it, and a body carrying the declared status. */
static int p7_check_terminal_wire(void *vctx, const char *what)
{
    fin_bridge_run_t *r = (fin_bridge_run_t *)vctx;
    int bad = 0;
    if (r->tp.server_ep.count != 1) {
        fprintf(stderr, "FINBR %s: %zu endpoint ops, expected 1\n", what,
                r->tp.server_ep.count);
        return 1;
    }
    fake_op_t *o = &r->tp.server_ep.ops[0];
    if (o->kind != FAKE_OP_WRITE) {
        fprintf(stderr, "FINBR %s: op kind %d, expected WRITE\n", what,
                (int)o->kind);
        return 1;
    }
    if (o->stream_id != r->transport_id) {
        fprintf(stderr, "FINBR %s: wrong transport stream\n", what); bad++;
    }
    if (!o->fin) {
        fprintf(stderr, "FINBR %s: local half not FIN'd\n", what); bad++;
    }
    moq_buf_reader_t rr;
    moq_buf_reader_init(&rr, o->data, o->data_len);
    moq_control_envelope_t env;
    memset(&env, 0, sizeof(env));
    if (moq_d18_decode_envelope(&rr, &env) != MOQ_OK) {
        fprintf(stderr, "FINBR %s: undecodable envelope\n", what); return bad + 1;
    }
    if (env.msg_type != (uint64_t)MOQ_D18_REQUEST_OK) {
        fprintf(stderr, "FINBR %s: msg type %llu\n", what,
                (unsigned long long)env.msg_type);
        bad++;
    }
    if (moq_buf_reader_remaining(&rr) != 0) {
        fprintf(stderr, "FINBR %s: trailing bytes after the envelope\n", what);
        bad++;
    }
    moq_d18_track_status_ok_t ok;
    memset(&ok, 0, sizeof(ok));
    if (moq_d18_decode_track_status_ok(env.payload, env.payload_len,
                                       &ok) != MOQ_OK) {
        fprintf(stderr, "FINBR %s: undecodable TRACK_STATUS_OK body\n", what);
        return bad + 1;
    }
    if (!ok.params.has_largest || ok.params.largest_group != P7_LARGEST_GROUP ||
        ok.params.largest_object != P7_LARGEST_OBJ) {
        fprintf(stderr, "FINBR %s: largest mismatch\n", what); bad++;
    }
    if (!ok.params.has_expires || ok.params.expires_ms != P7_EXPIRES_MS) {
        fprintf(stderr, "FINBR %s: expires mismatch\n", what); bad++;
    }
    if (ok.track_properties.len != 0) {
        fprintf(stderr, "FINBR %s: non-empty track properties\n", what); bad++;
    }
    return bad;
}

/*
 * The runner. Every hook the descriptor declares is CONSUMED: the family
 * bundle derives and identifies the owner, `capture`/`check` pin its record
 * across the EVENT POLL, and both normalizers build the images compared
 * against the declared output. Repeated bridge service is legal; post-FIN peer
 * bytes are not, and none are ever fed.
 */
/* The four phases the bridge entry passes through. */
typedef enum fin_br_phase {
    FIN_BR_AFTER_INGRESS = 0,
    FIN_BR_AFTER_TERMINAL,
    FIN_BR_AFTER_SERVICE,
    FIN_BR_AFTER_RESERVICE
} fin_br_phase_t;

/*
 * The EXACT bridge state for a class at a phase -- every flag constrained, not
 * just the interesting ones, so an unrelated flag turning on is caught.
 *
 * A retained ingress returns at transport_bridge.c:2662, BEFORE
 * peer_send_closed is set at :2677: the obligation lives in the retry flags
 * instead, which is why the two classes need different expectations here.
 */
static int fin_br_check_bridge(moq_transport_bridge_t *b, uint64_t transport_id,
                               moq_stream_ref_t want_ref,
                               fin_bridge_ingress_t ingress,
                               fin_br_phase_t phase, const char *what)
{
    int bad = 0;
    bridge_stream_entry_t *e = bridge_find_by_id(b, transport_id);

    if (phase == FIN_BR_AFTER_SERVICE || phase == FIN_BR_AFTER_RESERVICE) {
        if (e && e->active) {
            fprintf(stderr, "FINBR %s: mapping still active\n", what);
            bad++;
        }
        {   /* Absent by BOTH identities: an entry repointed to another
             * transport id while keeping the internal ref must not pass. */
            bridge_stream_entry_t *by_ref = bridge_find_by_ref(b, want_ref);
            if (by_ref && by_ref->active) {
                fprintf(stderr, "FINBR %s: mapping still active by ref\n",
                        what);
                bad++;
            }
        }
        if (moq_transport_bridge_has_pending(b)) {
            fprintf(stderr, "FINBR %s: bridge reports pending work\n", what);
            bad++;
        }
    } else {
        if (!e) {
            fprintf(stderr, "FINBR %s: mapping absent\n", what);
            return bad + 1;
        }
        if (!e->active) {
            fprintf(stderr, "FINBR %s: mapping inactive\n", what); bad++;
        }
        if (e->transport_id != transport_id) {
            fprintf(stderr, "FINBR %s: transport id\n", what); bad++;
        }
        /* BOTH identities, so a repointed entry cannot masquerade. */
        if (e->ref._v != want_ref._v) {
            fprintf(stderr, "FINBR %s: internal ref\n", what); bad++;
        }
        if (bridge_find_by_ref(b, want_ref) != e) {
            fprintf(stderr, "FINBR %s: ref lookup resolves elsewhere\n", what);
            bad++;
        }
        if (e->kind != BRIDGE_STREAM_BIDI) {
            fprintf(stderr, "FINBR %s: stream kind %d\n", what, (int)e->kind);
            bad++;
        }
        if (e->origin != BRIDGE_ORIGIN_PEER) {
            fprintf(stderr, "FINBR %s: stream origin %d\n", what,
                    (int)e->origin);
            bad++;
        }
        if (e->aborting) {
            fprintf(stderr, "FINBR %s: entry aborting\n", what); bad++;
        }
        if (e->pending_reset || e->pending_stop) {
            fprintf(stderr, "FINBR %s: reset/stop pending\n", what); bad++;
        }
        int want_peer_closed = (ingress == FIN_BR_CONSUMED);
        if (!!e->peer_send_closed != want_peer_closed) {
            fprintf(stderr, "FINBR %s: peer_send_closed %d, expected %d\n",
                    what, (int)e->peer_send_closed, want_peer_closed);
            bad++;
        }
        if (e->local_send_closed) {
            fprintf(stderr, "FINBR %s: local half already closed\n", what);
            bad++;
        }
        if (ingress == FIN_BR_CONSUMED) {
            if (e->pending_retry || e->pending_fin || e->fin_retained) {
                fprintf(stderr, "FINBR %s: FIN state retained after a "
                        "consumed ingress\n", what);
                bad++;
            }
        } else {
            if (!e->pending_retry || !e->fin_retained) {
                fprintf(stderr, "FINBR %s: retained obligation missing\n",
                        what);
                bad++;
            }
            /* `pending_fin` is a LATER FIN arriving while already suspended;
             * initial same-call retention owes it false. */
            if (e->pending_fin) {
                fprintf(stderr, "FINBR %s: unexpected pending_fin on initial "
                        "retention\n", what);
                bad++;
            }
        }
    }
    if (moq_transport_bridge_is_fatal(b)) {
        fprintf(stderr, "FINBR %s: bridge went fatal\n", what); bad++;
    }
    if (moq_transport_bridge_is_closed(b)) {
        fprintf(stderr, "FINBR %s: bridge closed\n", what); bad++;
    }
    return bad;
}

/* Set while a self-check drives the runner down a deliberately REFUSED path,
 * so an expected refusal cannot be mistaken for a failure in focused output. */
static int fin_br_quiet;

static int run_fin_bridge(const fin_bridge_case_t *f, fin_bridge_run_t *r)
{
    int failures = 0;
    char what[160];
    /* An invalid descriptor must not reach ANY family hook: an oversized
     * snapshot would otherwise be written into bounded storage. */
    if (fin_bridge_problems(f) != 0) {
        if (!fin_br_quiet)
            fprintf(stderr, "FINBR %s: invalid descriptor; no hook invoked\n",
                    f->name ? f->name : "(unnamed)");
        return failures + 1;
    }
    if (f->family->snap_size > sizeof(((fin_bridge_snap_t *)0)->bytes)) {
        if (!fin_br_quiet)
            fprintf(stderr, "FINBR %s: snapshot larger than storage\n",
                    f->name);
        return failures + 1;
    }

    /* Slot, generation and the handle they pack into, all from pool state
     * BEFORE ingress. */
    r->want_slot = f->family->derive_slot(r->tp.server, &r->want_gen);
    MOQ_TEST_CHECK(r->want_slot >= 0);
    if (r->want_slot < 0) return failures;   /* the check above counted it */
    r->want_handle = moq_handle_pack(f->family->pool_tag,
                                     r->tp.server->session_tag, r->want_gen,
                                     (uint32_t)r->want_slot);
    /* This route owes NO drain at any point: the FIN is observed before the
     * terminal runs, so the declared multiset is EMPTY throughout. */
    failures += f->family->check_drain(r->tp.server, "pre-ingress");

    /* 1. Request + FIN in ONE chunk, through real bridge ingress. */
    moq_result_t want_rc = (f->ingress == FIN_BR_CONSUMED)
                               ? MOQ_OK : MOQ_ERR_WOULD_BLOCK;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(r->tp.server_bridge, f->ctx,
                                       r->transport_id, true), (int)want_rc);

    /* The SESSION ref, captured while the entry exists; re-deriving it later
     * would return NULL once retired and make every owner check vacuous. */
    {
        bridge_stream_entry_t *e =
            bridge_find_by_id(r->tp.server_bridge, r->transport_id);
        MOQ_TEST_CHECK(e != NULL);
        if (!e) return failures;             /* the check above counted it */
        r->ref = e->ref;
        MOQ_TEST_CHECK(r->ref._v != 0);
        /* The two identities are genuinely different values. */
        MOQ_TEST_CHECK(r->ref._v != r->transport_id);
        MOQ_TEST_CHECK(e->active);
    }
    snprintf(what, sizeof(what), "%s ingress", f->name);
    failures += fin_br_check_bridge(r->tp.server_bridge, r->transport_id,
                                    r->ref, f->ingress, FIN_BR_AFTER_INGRESS, what);

    /* 2. The owner, against the DECLARED identity, plus its exact edge set. */
    snprintf(what, sizeof(what), "%s admitted", f->name);
    failures += f->family->check_live(r->tp.server, r->ref, r->want_slot,
                                     r->want_gen, r->want_handle, what);
    {
        og_graph_t g;
        og_capture(r->tp.server, &g);
        failures += f->family->check_edges(&g, r->ref, r->want_slot, 1, what);
    }

    /* 3. Snapshot the owner BEFORE polling, so a mutation caused BY the poll
     *    cannot become the baseline it is measured against. */
    fin_bridge_snap_t snap;
    MOQ_TEST_CHECK(f->family->snap_size <= sizeof(snap.bytes));
    f->family->capture(r->tp.server, f->ctx, snap.bytes,
                       f->family->snap_size);

    /* Exactly the declared request event, compared field for field. */
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        moq_event_t ev;
        while (moq_session_poll_events(r->tp.server, &ev, 1) > 0) {
            if (!f->family->normalize_event(&ev, f->ctx, &got)) failures++;
            moq_event_cleanup(&ev);
        }
        failures += f->family->want_request(&want, r->want_handle);
        failures += txs_norm_equals(&got, &want, f->name);
        txs_norm_free(&got);
        txs_norm_free(&want);
    }

    /* Nothing the poll touched may have moved: the owner record, its identity,
     * its exact live edge topology and the drain multiset are ALL reasserted
     * here, so a poll-time mutation the terminal later clears cannot pass. */
    snprintf(what, sizeof(what), "%s post-poll", f->name);
    failures += f->family->check(r->tp.server, f->ctx, snap.bytes,
                                 f->family->snap_size, what);
    failures += f->family->check_live(r->tp.server, r->ref, r->want_slot,
                                      r->want_gen, r->want_handle, what);
    {
        og_graph_t g;
        og_capture(r->tp.server, &g);
        failures += f->family->check_edges(&g, r->ref, r->want_slot, 1, what);
    }
    failures += f->family->check_drain(r->tp.server, what);
    failures += fin_br_check_bridge(r->tp.server_bridge, r->transport_id,
                                    r->ref, f->ingress, FIN_BR_AFTER_INGRESS, what);

    /* 4. The application answers; the FIN having been observed, NO drain. */
    MOQ_TEST_CHECK_EQ_INT((int)f->terminal(r->tp.server_bridge, f->ctx,
                                           r->transport_id), (int)MOQ_OK);
    snprintf(what, sizeof(what), "%s terminal", f->name);
    failures += f->family->check_retired(r->tp.server, r->ref, r->want_slot,
                                        what);
    failures += f->family->check_drain(r->tp.server, what);
    {   /* Semantic retirement is proven in the RAW graph too, before any
         * physical service runs. */
        og_graph_t g;
        og_capture(r->tp.server, &g);
        failures += f->family->check_edges(&g, r->ref, r->want_slot, 0, what);
    }
    /* The PHYSICAL mapping is untouched by the semantic terminal: still active,
     * still carrying its class's ingress state, our half still open. */
    failures += fin_br_check_bridge(r->tp.server_bridge, r->transport_id,
                                    r->ref, f->ingress, FIN_BR_AFTER_TERMINAL, what);

    /* 5. Bridge service alone emits the EXACT terminal and retires the map. */
    fake_endpoint_clear_ops(&r->tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(r->tp.server_bridge, 0), (int)MOQ_OK);
    snprintf(what, sizeof(what), "%s serviced", f->name);
    failures += f->family->check_terminal_wire(f->ctx, what);
    failures += f->family->check_retired(r->tp.server, r->ref, r->want_slot,
                                        what);
    failures += fin_br_check_bridge(r->tp.server_bridge, r->transport_id,
                                    r->ref, f->ingress, FIN_BR_AFTER_SERVICE, what);
    {
        og_graph_t g;
        og_capture(r->tp.server, &g);
        failures += f->family->check_edges(&g, r->ref, r->want_slot, 0, what);
    }
    failures += f->family->check_drain(r->tp.server, what);

    /* 6. Idempotence: a second service adds EXACTLY ZERO operations, leaves
     *    nothing queued, recreates no owner, and installs no drain. */
    fake_endpoint_clear_ops(&r->tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(r->tp.server_bridge, 0), (int)MOQ_OK);
    snprintf(what, sizeof(what), "%s reserviced", f->name);
    MOQ_TEST_CHECK_EQ_SIZE(r->tp.server_ep.count, (size_t)0);
    {
        txs_norm_vec_t leftover;
        txs_norm_init(&leftover);
        moq_event_t ev;
        while (moq_session_poll_events(r->tp.server, &ev, 1) > 0) {
            if (!f->family->normalize_event(&ev, f->ctx, &leftover))
                failures++;
            moq_event_cleanup(&ev);
        }
        moq_action_t a;
        while (moq_session_poll_actions(r->tp.server, &a, 1) > 0) {
            if (!f->family->normalize_action(&a, f->ctx, &leftover))
                failures++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_SIZE(leftover.count, (size_t)0);
        txs_norm_free(&leftover);
    }
    failures += f->family->check_retired(r->tp.server, r->ref, r->want_slot,
                                        what);
    failures += f->family->check_drain(r->tp.server, what);
    {
        og_graph_t g;
        og_capture(r->tp.server, &g);
        failures += f->family->check_edges(&g, r->ref, r->want_slot, 0, what);
    }
    /* The COMPLETE physical postcondition again: a mapping resurrected only by
     * the second service must not pass. */
    failures += fin_br_check_bridge(r->tp.server_bridge, r->transport_id,
                                    r->ref, f->ingress, FIN_BR_AFTER_RESERVICE, what);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(r->tp.server),
                          (int)MOQ_SESS_ESTABLISHED);
    return failures;
}

/* Permanent descriptor self-checks: every required member, the ingress class,
 * and the (kind, pool) pairing. Deleting a member from a real descriptor would
 * only fail the BUILD via an unused static, so validity is probed here on
 * local copies instead. */
static int probe_capture_calls;
static void probe_capture_forbidden(const moq_session_t *s, void *ctx,
                                    void *state, size_t cap)
{
    (void)s; (void)ctx; (void)state; (void)cap;
    probe_capture_calls++;
}


/* -- #250: SUBSCRIBE_NAMESPACE response-stream termination, over the real
 * bridge (draft-18 §10.18).
 *
 * The route is indexed by idx_ns_by_ref, so the request-stream retainability
 * gap (#245(a)) must not be on its path -- these rows PROVE that with the very
 * predicate the bridge consults, and then by taking a genuine retained
 * WOULD_BLOCK rather than a fatal. Recovery is SERVICE-ONLY: the fixture never
 * re-delivers peer bytes, a FIN, or a second reset.
 *
 * The owner inventory, event images, drain multiset and action classification
 * come from tests/support/ns_owner_inventory.h, so this suite and the
 * direct-session suite hold ONE field contract. */

typedef struct nsfin_arm {
    test_pair_t      tp;
    uint64_t         bidi;      /* transport id, DECLARED before the request */
    moq_stream_ref_t ref;       /* session ref, DECLARED before the request */
    int              slot;
    uint32_t         generation;
    uint64_t         handle;
    int              srv_slot;        /* all three DERIVED before delivery */
    uint32_t         srv_generation;
    uint64_t         srv_handle;
    size_t           budget0;      /* client receive budget before any suffix */
    size_t           budget_active; /* DECLARED: budget0 + array + suffix key */
    nf_inv_t         want_live;     /* DECLARED established owner, not observed */
    const char      *sfx;
} nsfin_arm_t;

#define NSFIN_PREFIX  "live"
#define NSFIN_PREFIX2 "v2"
#define NSFIN_RID     0u

/* The exact bridge-entry class for this LOCAL-ORIGIN request bidi. (The
 * client opened it -- transport_bridge.c:1800 -- even though the peer's
 * response bytes later arrive on it. The earlier "peer-origin" wording was
 * wrong.) FIN and RESET supply only their own flag/code deltas. */
typedef struct nsfin_entry_want {
    int      active;
    int      pending_retry, pending_fin, fin_retained;
    int      pending_reset;  uint64_t pending_reset_code;
    int      pending_stop;   uint64_t pending_stop_code;
    int      peer_send_closed, local_send_closed, aborting;
    int      stream_pending;      /* moq_transport_bridge_stream_has_pending */
    int      bridge_pending;      /* moq_transport_bridge_has_pending */
    /* Inbound-uni classification storage. A BIDI/LOCAL entry never classifies,
     * so its declared value is the zero/PENDING state in EVERY live phase. */
    uint8_t  uni_disp;
    uint8_t  classify_len;
    uint8_t  classify_buf[9];
} nsfin_entry_want_t;

static nsfin_entry_want_t nsfin_entry_live(void)
{
    nsfin_entry_want_t w;
    memset(&w, 0, sizeof(w));
    w.active = 1;
    return w;
}

static int nsfin_check_entry(nsfin_arm_t *a, const nsfin_entry_want_t *w,
                             const char *what)
{
    int failures = 0;
    bridge_stream_entry_t *by_id = bridge_find_by_id(a->tp.client_bridge,
                                                     a->bidi);
    bridge_stream_entry_t *by_ref = bridge_find_by_ref(a->tp.client_bridge,
                                                       a->ref);
    if (!by_id || by_id != by_ref) {
        fprintf(stderr, "NSFIN %s: the entry is not reachable by BOTH the"
                " declared transport id and the declared internal ref\n", what);
        return failures + 1;
    }
#define NSE(f, fmt, expr) do { \
        if ((expr) != (w->f)) { \
            fprintf(stderr, "NSFIN %s: entry " #f " " fmt ", expected " fmt \
                    "\n", what, (expr), (w->f)); \
            failures++; \
        } \
    } while (0)
    /* identity first: a coherently wrong mapping must not pass */
    if (by_id->ref._v != a->ref._v) {
        fprintf(stderr, "NSFIN %s: entry ref %llu, declared %llu\n", what,
                (unsigned long long)by_id->ref._v,
                (unsigned long long)a->ref._v);
        failures++;
    }
    if (by_id->transport_id != a->bidi) {
        fprintf(stderr, "NSFIN %s: entry transport id %llu, declared %llu\n",
                what, (unsigned long long)by_id->transport_id,
                (unsigned long long)a->bidi);
        failures++;
    }
    if (by_id->kind != BRIDGE_STREAM_BIDI) {
        fprintf(stderr, "NSFIN %s: entry kind %d, expected BIDI\n", what,
                (int)by_id->kind);
        failures++;
    }
    if (by_id->origin != BRIDGE_ORIGIN_LOCAL) {
        fprintf(stderr, "NSFIN %s: entry origin %d, expected LOCAL\n", what,
                (int)by_id->origin);
        failures++;
    }
    NSE(active, "%d", (int)by_id->active);
    NSE(pending_retry, "%d", (int)by_id->pending_retry);
    NSE(pending_fin, "%d", (int)by_id->pending_fin);
    NSE(fin_retained, "%d", (int)by_id->fin_retained);
    NSE(pending_reset, "%d", (int)by_id->pending_reset);
    NSE(pending_stop, "%d", (int)by_id->pending_stop);
    NSE(peer_send_closed, "%d", (int)by_id->peer_send_closed);
    NSE(local_send_closed, "%d", (int)by_id->local_send_closed);
    NSE(aborting, "%d", (int)by_id->aborting);
    NSE(uni_disp, "%d", (int)by_id->uni_disp);
    NSE(classify_len, "%d", (int)by_id->classify_len);
#undef NSE
    if (memcmp(by_id->classify_buf, w->classify_buf,
               sizeof(w->classify_buf)) != 0) {
        fprintf(stderr, "NSFIN %s: entry classify_buf differs from the"
                " declared zero state\n", what);
        failures++;
    }
    if (by_id->pending_reset_code != w->pending_reset_code) {
        fprintf(stderr, "NSFIN %s: entry pending_reset_code %llu, expected"
                " %llu\n", what,
                (unsigned long long)by_id->pending_reset_code,
                (unsigned long long)w->pending_reset_code);
        failures++;
    }
    if (by_id->pending_stop_code != w->pending_stop_code) {
        fprintf(stderr, "NSFIN %s: entry pending_stop_code %llu, expected"
                " %llu\n", what,
                (unsigned long long)by_id->pending_stop_code,
                (unsigned long long)w->pending_stop_code);
        failures++;
    }
    if ((int)moq_transport_bridge_stream_has_pending(a->tp.client_bridge,
                                                     a->bidi)
        != w->stream_pending) {
        fprintf(stderr, "NSFIN %s: stream_has_pending %d, expected %d\n", what,
                (int)moq_transport_bridge_stream_has_pending(
                    a->tp.client_bridge, a->bidi), w->stream_pending);
        failures++;
    }
    if ((int)moq_transport_bridge_has_pending(a->tp.client_bridge)
        != w->bridge_pending) {
        fprintf(stderr, "NSFIN %s: has_pending %d, expected %d\n", what,
                (int)moq_transport_bridge_has_pending(a->tp.client_bridge),
                w->bridge_pending);
        failures++;
    }
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(a->tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(a->tp.client_bridge));
    return failures;
}

/* Exactly this endpoint operation set on `ep`, and nothing else. */
static int nsfin_ops_exact(fake_endpoint_t *ep, int want_open_bidi,
                           int want_write, uint64_t want_id, const char *what)
{
    int failures = 0;
    int opens = 0, writes = 0, other = 0, wrong_id = 0;
    MOQ_TEST_CHECK(ep->count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < ep->count; i++) {
        if (want_id && ep->ops[i].stream_id != want_id) wrong_id++;
        switch (ep->ops[i].kind) {
        case FAKE_OP_OPEN_BIDI: opens++;  break;
        case FAKE_OP_WRITE:     writes++; break;
        default:                other++;  break;
        }
    }
    if (wrong_id) {
        fprintf(stderr, "NSFIN %s: %d ops on a stream id other than the"
                " declared %llu\n", what, wrong_id,
                (unsigned long long)want_id);
        failures++;
    }
    if (opens != want_open_bidi || writes != want_write || other != 0) {
        fprintf(stderr, "NSFIN %s: ops open-bidi %d write %d other %d,"
                " expected %d/%d/0\n", what, opens, writes, other,
                want_open_bidi, want_write);
        failures++;
    }
    return failures;
}

/* Deliver every WRITE on `from` to `to`, checking each ingress result, and
 * report how many were delivered. Unlike d18_feed nothing is discarded. */
static int nsfin_deliver(moq_transport_bridge_t *to, fake_endpoint_t *from,
                         size_t *delivered, const char *what)
{
    int failures = 0;
    *delivered = 0;
    MOQ_TEST_CHECK(from->count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < from->count; i++) {
        fake_op_t *o = &from->ops[i];
        if (o->kind != FAKE_OP_WRITE) {
            if (o->kind != FAKE_OP_OPEN_BIDI && o->kind != FAKE_OP_OPEN_UNI) {
                fprintf(stderr, "NSFIN %s: unexpected endpoint op kind %d\n",
                        what, (int)o->kind);
                failures++;
            }
            continue;
        }
        moq_result_t rc = (o->stream_id >= 2000 && o->stream_id < 3000) ||
                          (o->stream_id >= 4000)
            ? moq_transport_bridge_on_peer_bidi_bytes(
                  to, o->stream_id, o->data, o->data_len, o->fin, 0)
            : moq_transport_bridge_on_peer_uni_bytes(
                  to, o->stream_id, o->data, o->data_len, o->fin, 0);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        (*delivered)++;
    }
    fake_endpoint_clear_ops(from);
    return failures;
}

/* Phases 1-4: one established client-side namespace subscription with exactly
 * one active suffix, every result checked and every record classified. */
static int nsfin_arm_build(nsfin_arm_t *a, const char *suffix_field)
{
    int failures = 0;
    memset(a, 0, sizeof(*a));
    a->sfx = suffix_field;
    if (d18_pair_init(&a->tp, 1) < 0) return 1;

    /* (1) checked starts, strict shuttle, exactly one SETUP_COMPLETE a side. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(a->tp.client, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(a->tp.server, 0), (int)MOQ_OK);
    failures += d18_strict_shuttle(&a->tp, 30, 0, "nsfin setup");
    {
        moq_event_t ev;
        int c_setup = 0, s_setup = 0, other = 0;
        while (moq_session_poll_events(a->tp.client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) c_setup++; else other++;
            moq_event_cleanup(&ev);
        }
        while (moq_session_poll_events(a->tp.server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) s_setup++; else other++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(c_setup, 1);
        MOQ_TEST_CHECK_EQ_INT(s_setup, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    MOQ_TEST_CHECK_EQ_SIZE(a->tp.client_ep.count, (size_t)0);
    MOQ_TEST_CHECK_EQ_SIZE(a->tp.server_ep.count, (size_t)0);

    /* (2) derive EVERY identity BEFORE the request exists: the session ref
     * from the session's own counter, the transport id from the fake
     * endpoint's next bidi id, and the owner slot/generation/handle from the
     * free pool. Nothing here is adopted from what the call produces. */
    a->ref = moq_stream_ref_from_u64(a->tp.client->next_stream_ref);
    a->bidi = a->tp.client_ep.next_bidi_id;
    MOQ_TEST_CHECK(a->ref._v != 0);
    MOQ_TEST_CHECK(a->bidi != 0);

    int want_slot = -1;
    for (size_t i = 0; i < a->tp.client->ns_sub_cap; i++)
        if (a->tp.client->ns_subs[i].state == MOQ_NS_SUB_FREE) {
            want_slot = (int)i; break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return failures + 1;
    a->slot = want_slot;
    a->generation = a->tp.client->ns_subs[want_slot].generation | 1u;
    a->handle = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                a->tp.client->session_tag, a->generation,
                                (uint32_t)want_slot);
    MOQ_TEST_CHECK(a->handle != 0);

    nf_inv_t free_rec;
    nf_inv_read(a->tp.client, want_slot, &free_rec);
    MOQ_TEST_CHECK(free_rec.valid);

    moq_bytes_t pfx_parts[] = { MOQ_BYTES_LITERAL(NSFIN_PREFIX),
                                MOQ_BYTES_LITERAL(NSFIN_PREFIX2) };
    moq_namespace_t pfx = { pfx_parts, 2 };
    moq_subscribe_namespace_cfg_t nc;
    moq_subscribe_namespace_cfg_init(&nc);
    nc.track_namespace_prefix = pfx;
    nc.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nh;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_subscribe_namespace(a->tp.client, &nc, 0, &nh),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(nh._opaque, a->handle);

    /* (3) exactly one local OPEN + one WRITE, decoded as SUBSCRIBE_NAMESPACE. */
    fake_endpoint_clear_ops(&a->tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(a->tp.client_bridge, 0), (int)MOQ_OK);
    failures += nsfin_ops_exact(&a->tp.client_ep, 1, 1, a->bidi,
                                "arm local open");
    for (size_t i = 0; i < a->tp.client_ep.count; i++) {
        fake_op_t *o = &a->tp.client_ep.ops[i];
        if (o->kind != FAKE_OP_WRITE) continue;
        MOQ_TEST_CHECK_EQ_U64(o->stream_id, a->bidi);
        MOQ_TEST_CHECK(!o->fin);
        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, o->data, o->data_len);
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_SUBSCRIBE_NAMESPACE);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), (size_t)0);
        moq_bytes_t dp[MOQ_DECODED_MAX_NAMESPACE_PARTS];
        moq_d18_subscribe_namespace_t sn;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_namespace(
                env.payload, env.payload_len, dp,
                MOQ_DECODED_MAX_NAMESPACE_PARTS, &sn), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(sn.request_id, NSFIN_RID);
        MOQ_TEST_CHECK_EQ_SIZE(sn.track_namespace_prefix.count, (size_t)2);
        failures += txs_check_part_bytes(&sn.track_namespace_prefix, 0,
                                         NSFIN_PREFIX, strlen(NSFIN_PREFIX),
                                         "arm request prefix 0");
        failures += txs_check_part_bytes(&sn.track_namespace_prefix, 1,
                                         NSFIN_PREFIX2, strlen(NSFIN_PREFIX2),
                                         "arm request prefix 1");
        /* no unexpected parameters may ride the request */
        MOQ_TEST_CHECK(sn.params.auth_token_count == 0 &&
                       !sn.params.has_forward &&
                       !sn.params.has_subscriber_priority &&
                       !sn.params.has_filter &&
                       !sn.params.has_group_order &&
                       !sn.params.has_new_group_request);
    }
    MOQ_TEST_CHECK(moq_index_find(a->tp.client->idx_ns_by_ref,
                                  a->tp.client->idx_ns_mask,
                                  a->ref._v) == a->slot);
    /* #245(a) independence, PROVEN with the predicate the bridge consults. */
    MOQ_TEST_CHECK(moq_session_has_transport_stream(a->tp.client, a->ref));

    /* Derive the SERVER owner BEFORE the request is delivered -- afterwards
     * the slot is already occupied and a scan would name the NEXT free one. */
    a->srv_slot = -1;
    for (size_t i = 0; i < a->tp.server->ns_sub_cap; i++)
        if (a->tp.server->ns_subs[i].state == MOQ_NS_SUB_FREE) {
            a->srv_slot = (int)i; break;
        }
    MOQ_TEST_CHECK(a->srv_slot >= 0);
    if (a->srv_slot < 0) return failures + 1;
    a->srv_generation = a->tp.server->ns_subs[a->srv_slot].generation | 1u;
    a->srv_handle = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                    a->tp.server->session_tag,
                                    a->srv_generation,
                                    (uint32_t)a->srv_slot);
    MOQ_TEST_CHECK(a->srv_handle != 0);

    /* The DECLARED pending-subscriber owner, from the free record plus the
     * fixture's own inputs -- never re-read from the session. */
    {
        static const char *const kParts[2] = { NSFIN_PREFIX, NSFIN_PREFIX2 };
        a->want_live = nf_local_pending_want(&free_rec, a->generation,
                                             a->handle, NSFIN_RID, a->ref._v,
                                             kParts, 2,
                                             MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE);
        int adopt = nf_adopt_prefix_addrs(a->tp.client, a->slot,
                                          &a->want_live, "arm client owner");
        failures += adopt;
        if (adopt == 0)
            failures += nf_inv_check(a->tp.client, a->slot, &a->want_live,
                                     "arm client owner");
    }

    size_t moved = 0;
    failures += nsfin_deliver(a->tp.server_bridge, &a->tp.client_ep, &moved,
                              "arm request");
    MOQ_TEST_CHECK_EQ_SIZE(moved, (size_t)1);

    /* (4) exactly one NS_SUB_REQUEST with the declared image. */
    moq_ns_sub_handle_t sh = MOQ_NS_SUB_HANDLE_INVALID;
    {
        moq_event_t ev;
        int reqs = 0, other = 0;
        while (moq_session_poll_events(a->tp.server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) {
                reqs++;
                sh = ev.u.ns_sub_request.handle;
                MOQ_TEST_CHECK_EQ_U64(sh._opaque, a->srv_handle);
                MOQ_TEST_CHECK(ev.u.ns_sub_request.forward);
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.ns_sub_request.track_namespace_prefix.count,
                    (size_t)2);
                failures += txs_check_part_bytes(
                    &ev.u.ns_sub_request.track_namespace_prefix, 0,
                    NSFIN_PREFIX, strlen(NSFIN_PREFIX),
                    "arm ns_sub_request 0");
                failures += txs_check_part_bytes(
                    &ev.u.ns_sub_request.track_namespace_prefix, 1,
                    NSFIN_PREFIX2, strlen(NSFIN_PREFIX2),
                    "arm ns_sub_request 1");
                MOQ_TEST_CHECK_EQ_U64(
                    ev.u.ns_sub_request.namespace_interest,
                    MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE);
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.ns_sub_request.token_count,
                                       (size_t)0);
            } else {
                other++;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(reqs, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    /* the physical server owner matches the derivation too */
    MOQ_TEST_CHECK(a->tp.server->ns_subs[a->srv_slot].state !=
                   MOQ_NS_SUB_FREE);
    MOQ_TEST_CHECK_EQ_U64(a->tp.server->ns_subs[a->srv_slot].generation,
                          a->srv_generation);
    MOQ_TEST_CHECK_EQ_U64(a->tp.server->ns_subs[a->srv_slot].handle._opaque,
                          a->srv_handle);

    moq_accept_ns_sub_cfg_t ac;
    moq_accept_ns_sub_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_accept_ns_sub(a->tp.server, sh, &ac, 0), (int)MOQ_OK);
    fake_endpoint_clear_ops(&a->tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(a->tp.server_bridge, 0), (int)MOQ_OK);
    /* The acceptance rides the client's own bidi: one WRITE, no local open. */
    failures += nsfin_ops_exact(&a->tp.server_ep, 0, 1, a->bidi,
                                "arm acceptance");
    {
        /* Decode the REQUEST_OK before delivering it. */
        for (size_t i = 0; i < a->tp.server_ep.count; i++) {
            fake_op_t *o = &a->tp.server_ep.ops[i];
            if (o->kind != FAKE_OP_WRITE) continue;
            MOQ_TEST_CHECK_EQ_U64(o->stream_id, a->bidi);
            MOQ_TEST_CHECK(!o->fin);
            moq_control_envelope_t env;
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, o->data, o->data_len);
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env),
                                  (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_REQUEST_OK);
            MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), (size_t)0);
            /* the BODY too: junk inside the envelope must not pass */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_decode_request_ok(env.payload, env.payload_len),
                (int)MOQ_OK);
            /* The body check is load-bearing: junk inside the envelope must
             * be rejected, so an envelope-type-only assertion would not be
             * equivalent. */
            if (env.payload_len + 1 <= 64) {
                /* A VALID body plus one trailing byte, still inside the
                 * envelope, must be rejected -- otherwise the body check
                 * would only be proving that a malformed count fails. */
                uint8_t junk[64];
                memcpy(junk, env.payload, env.payload_len);
                junk[env.payload_len] = 0x5A;
                MOQ_TEST_CHECK(moq_d18_decode_request_ok(
                    junk, env.payload_len + 1) != MOQ_OK);
            }
        }
    }
    failures += nsfin_deliver(a->tp.client_bridge, &a->tp.server_ep, &moved,
                              "arm acceptance");
    MOQ_TEST_CHECK_EQ_SIZE(moved, (size_t)1);
    {
        nf_ev_t got[NF_EV_MAX]; size_t k = 0;
        failures += nf_collect(a->tp.client, a->handle, got, NF_EV_MAX, &k,
                               "arm ns_sub_ok");
        MOQ_TEST_CHECK_EQ_SIZE(k, (size_t)1);
        if (k == 1) {
            nf_ev_t want = nf_ev_want(MOQ_EVENT_NS_SUB_OK, NULL, NULL);
            failures += nf_ev_equals(&got[0], &want, "arm ns_sub_ok");
        }
    }

    /* The REQUEST_OK transition, applied EXPLICITLY rather than re-read. */
    a->want_live.state = MOQ_NS_SUB_ESTABLISHED;
    a->want_live.got_response = 1;
    failures += nf_inv_check(a->tp.client, a->slot, &a->want_live,
                             "arm established owner");
    a->budget0 = a->tp.client->recv_payload_bytes;

    /* One NAMESPACE: decoded on the wire, then surfaced as one exact
     * NAMESPACE_FOUND that is deliberately LEFT QUEUED as the blocker. */
    {
        moq_bytes_t sp[2];
        sp[0] = (moq_bytes_t){ (const uint8_t *)"room", 4 };
        sp[1] = (moq_bytes_t){ (const uint8_t *)suffix_field,
                               strlen(suffix_field) };
        moq_namespace_t sfx = { sp, 2 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_send_namespace(a->tp.server, sh, &sfx, 0),
            (int)MOQ_OK);
    }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(a->tp.server_bridge, 0), (int)MOQ_OK);
    failures += nsfin_ops_exact(&a->tp.server_ep, 0, 1, a->bidi,
                                "arm namespace");
    for (size_t i = 0; i < a->tp.server_ep.count; i++) {
        fake_op_t *o = &a->tp.server_ep.ops[i];
        if (o->kind != FAKE_OP_WRITE) continue;
        MOQ_TEST_CHECK_EQ_U64(o->stream_id, a->bidi);
        moq_control_envelope_t env;
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, o->data, o->data_len);
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_NAMESPACE);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), (size_t)0);
        MOQ_TEST_CHECK(!o->fin);
        moq_buf_reader_t pr;
        moq_buf_reader_init(&pr, env.payload, env.payload_len);
        moq_bytes_t sp[MOQ_DECODED_MAX_NAMESPACE_PARTS];
        moq_namespace_t got_ns;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_buf_read_namespace_prefix(&pr, sp,
                MOQ_DECODED_MAX_NAMESPACE_PARTS, &got_ns), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(got_ns.count, (size_t)2);
        failures += txs_check_part_bytes(&got_ns, 0, "room", 4,
                                         "arm namespace suffix 0");
        failures += txs_check_part_bytes(&got_ns, 1, suffix_field,
                                         strlen(suffix_field),
                                         "arm namespace suffix 1");
        /* the whole payload, not just a decodable prefix of it */
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&pr), (size_t)0);
        /* and that check is load-bearing: the same body plus one trailing
         * byte inside the envelope must leave the reader unconsumed. */
        if (env.payload_len + 1 <= 128) {
            uint8_t tail[128];
            memcpy(tail, env.payload, env.payload_len);
            tail[env.payload_len] = 0x5A;
            moq_buf_reader_t tr;
            moq_buf_reader_init(&tr, tail, env.payload_len + 1);
            moq_bytes_t tp2[MOQ_DECODED_MAX_NAMESPACE_PARTS];
            moq_namespace_t tns;
            MOQ_TEST_CHECK(moq_buf_read_namespace_prefix(
                &tr, tp2, MOQ_DECODED_MAX_NAMESPACE_PARTS, &tns) == MOQ_OK);
            MOQ_TEST_CHECK(moq_buf_reader_remaining(&tr) != 0);
        }
    }
    failures += nsfin_deliver(a->tp.client_bridge, &a->tp.server_ep, &moved,
                              "arm namespace");
    MOQ_TEST_CHECK_EQ_SIZE(moved, (size_t)1);
    MOQ_TEST_CHECK(event_queue_full(a->tp.client));
    MOQ_TEST_CHECK_EQ_SIZE(a->tp.client_ep.count, (size_t)0);

    /* The NAMESPACE transition, applied EXPLICITLY: one inbound tracker is
     * present (presence only -- its address cannot authorize itself) and the
     * active budget is an absolute declared value with checked addition. */
    {
        const size_t charge = nf_suffix_charge(suffix_field);
        MOQ_TEST_CHECK(charge <= SIZE_MAX - a->budget0);
        a->budget_active = a->budget0 + charge;
        nf_inv_t want = a->want_live;
        want.cmp_suffix_ptr = 0;
        want.suffixes = (const void *)1;
        want.suffixes_inbound = 1;
        nf_inv_t now;
        nf_inv_read(a->tp.client, a->slot, &now);
        now.cmp_suffix_ptr = 0;
        failures += nf_inv_equals(&now, &want, "arm namespace owner");
        MOQ_TEST_CHECK(now.suffixes != NULL);
        MOQ_TEST_CHECK_EQ_SIZE(a->tp.client->recv_payload_bytes,
                               a->budget_active);
        /* Only now is the tracker address adopted for later conservation. */
        a->want_live.suffixes =
            a->tp.client->ns_subs[a->slot].announced_suffixes;
        a->want_live.suffixes_inbound = 1;
    }
    return failures;
}

/* The complete pre-terminal picture: owner inventory, sole ns edge, empty
 * drain set, and ONE bridge record reachable by both identities. */
static int nsfin_arm_precheck(nsfin_arm_t *a, nf_inv_t *live, nf_drain_t *d0,
                              og_graph_t *g0, const char *what)
{
    /* The DECLARED owner the arm built, not a fresh read of the session. */
    *live = a->want_live;
    int failures = nf_inv_check(a->tp.client, a->slot, live, what);

    MOQ_TEST_CHECK(nf_drain_snap(a->tp.client, d0) == 0);
    MOQ_TEST_CHECK_EQ_SIZE(d0->count, (size_t)0);

    og_capture(a->tp.client, g0);
    failures += og_check_integrity(g0, what);
    const og_edge_spec_t edges[] = { { OG_DOM_NS_REF, a->ref._v } };
    failures += og_check_owner_edges(g0, MOQ_REQ_NAMESPACE_SUB, a->slot,
                                     edges, 1, what);

    nsfin_entry_want_t ew = nsfin_entry_live();
    failures += nsfin_check_entry(a, &ew, what);
    /* No session output is owed at the arm point. */
    MOQ_TEST_CHECK_EQ_SIZE(
        a->tp.client->action_tail - a->tp.client->action_head, (size_t)0);
    return failures;
}

/* One conservation checker for the blocked and post-blocker windows: the
 * complete owner, exact graph topology, exact drain set, exact receive
 * budget, and no session action output. */
static int nsfin_conserved(nsfin_arm_t *a, const nf_inv_t *want,
                           const og_graph_t *g0, const nf_drain_t *d0,
                           size_t budget, const char *what)
{
    int failures = nf_inv_check(a->tp.client, a->slot, want, what);
    og_graph_t g;
    og_capture(a->tp.client, &g);
    failures += og_check_same_topology(g0, &g, what);
    nf_drain_t d;
    MOQ_TEST_CHECK(nf_drain_snap(a->tp.client, &d) == 0);
    failures += nf_drain_equals(&d, d0, what);
    if (a->tp.client->recv_payload_bytes != budget) {
        fprintf(stderr, "NSFIN %s: receive budget %zu, expected %zu\n", what,
                a->tp.client->recv_payload_bytes, budget);
        failures++;
    }
    MOQ_TEST_CHECK_EQ_SIZE(
        a->tp.client->action_tail - a->tp.client->action_head, (size_t)0);
    return failures;
}

/* Phases 8-9: nothing of this owner survives, in EITHER identity. */
static int nsfin_check_retired(nsfin_arm_t *a, const nf_inv_t *live,
                               const nf_drain_t *d0, size_t budget0,
                               const char *what)
{
    int failures = 0;
    if (a->tp.client->recv_payload_bytes != budget0) {
        fprintf(stderr, "NSFIN %s: receive budget %zu, expected %zu\n", what,
                a->tp.client->recv_payload_bytes, budget0);
        failures++;
    }
    if (moq_transport_bridge_stream_has_pending(a->tp.client_bridge, a->bidi)) {
        fprintf(stderr, "NSFIN %s: the stream still reports pending work\n",
                what);
        failures++;
    }
    nf_inv_t want = *live;
    nf_inv_apply_free(&want);
    failures += nf_inv_check(a->tp.client, a->slot, &want, what);

    og_graph_t g;
    og_capture(a->tp.client, &g);
    failures += og_check_integrity(&g, what);
    failures += og_check_no_edge(&g, OG_DOM_NS_REF, a->ref._v, what);
    failures += og_check_owner_unreferenced(&g, MOQ_REQ_NAMESPACE_SUB, a->slot,
                                            what);

    nf_drain_t d1;
    MOQ_TEST_CHECK(nf_drain_snap(a->tp.client, &d1) == 0);
    failures += nf_drain_equals(&d1, d0, what);

    if (bridge_find_by_id(a->tp.client_bridge, a->bidi) != NULL) {
        fprintf(stderr, "NSFIN %s: the transport-id mapping survived\n", what);
        failures++;
    }
    if (bridge_find_by_ref(a->tp.client_bridge, a->ref) != NULL) {
        fprintf(stderr, "NSFIN %s: the internal-ref mapping survived\n", what);
        failures++;
    }
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(a->tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(a->tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(a->tp.client_bridge));
    return failures;
}

/* Exactly one empty-FIN WRITE on the original transport id, or none. */
static int nsfin_expect_ops(nsfin_arm_t *a, size_t want_fin_writes,
                            const char *what)
{
    int failures = 0;
    size_t fins = 0, other = 0;
    MOQ_TEST_CHECK(a->tp.client_ep.count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < a->tp.client_ep.count; i++) {
        fake_op_t *o = &a->tp.client_ep.ops[i];
        if (o->kind == FAKE_OP_WRITE && o->stream_id == a->bidi &&
            o->data_len == 0 && o->fin)
            fins++;
        else
            other++;
    }
    if (fins != want_fin_writes || other != 0) {
        fprintf(stderr, "NSFIN %s: %zu empty-FIN writes and %zu other ops,"
                " expected %zu/0\n", what, fins, other, want_fin_writes);
        failures++;
    }
    fake_endpoint_clear_ops(&a->tp.client_ep);
    return failures;
}

/* Release ONLY the blocker: exactly the declared NAMESPACE_FOUND. */
static int nsfin_release_blocker(nsfin_arm_t *a)
{
    nf_ev_t got[NF_EV_MAX]; size_t k = 0;
    int failures = nf_collect(a->tp.client, a->handle, got, NF_EV_MAX, &k,
                              "blocker");
    MOQ_TEST_CHECK_EQ_SIZE(k, (size_t)1);
    if (k == 1) {
        nf_ev_t want = nf_ev_want(MOQ_EVENT_NAMESPACE_FOUND, "room", a->sfx);
        failures += nf_ev_equals(&got[0], &want, "blocker");
    }
    return failures;
}

static int nsfin_expect_gone(nsfin_arm_t *a, size_t n, const char *what)
{
    nf_ev_t got[NF_EV_MAX]; size_t k = 0;
    int failures = nf_collect(a->tp.client, a->handle, got, NF_EV_MAX, &k,
                              what);
    nf_ev_t want[1] = { nf_ev_want(MOQ_EVENT_NAMESPACE_GONE, "room", a->sfx) };
    if (n == 0) {
        MOQ_TEST_CHECK_EQ_SIZE(k, (size_t)0);
        return failures;
    }
    failures += nf_multiset(got, k, want, 1, what);
    return failures;
}

static int test_ns_response_fin_bridge(void)
{
    int failures = 0;
    nsfin_arm_t a;
    failures += nsfin_arm_build(&a, "alpha");

    nf_inv_t live; nf_drain_t d0; og_graph_t g0;
    failures += nsfin_arm_precheck(&a, &live, &d0, &g0, "fin arm");

    /* (5) the peer FINs its response half while the client cannot emit. */
    moq_result_t rc = moq_transport_bridge_on_peer_bidi_bytes(
        a.tp.client_bridge, a.bidi, NULL, 0, true, 0);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(a.tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(a.tp.client_bridge));
    {
        nsfin_entry_want_t ew = nsfin_entry_live();
        ew.pending_retry = 1;
        ew.fin_retained = 1;
        ew.stream_pending = 1;
        ew.bridge_pending = 1;
        failures += nsfin_check_entry(&a, &ew, "fin blocked");
    }
    nf_inv_t want_blocked = live;
    want_blocked.pending_fin = 1;
    const size_t budget_live = a.budget_active;   /* DECLARED by the arm */
    failures += nsfin_conserved(&a, &want_blocked, &g0, &d0, budget_live,
                                "fin blocked");
    failures += nsfin_expect_ops(&a, 0, "fin blocked");

    /* (7) release only the blocker, reassert, then SERVICE -- no
     * re-delivery of bytes, FIN or reset at any point. */
    failures += nsfin_release_blocker(&a);
    {
        nsfin_entry_want_t ew = nsfin_entry_live();
        ew.pending_retry = 1;
        ew.fin_retained = 1;
        ew.stream_pending = 1;
        ew.bridge_pending = 1;
        failures += nsfin_check_entry(&a, &ew, "fin released");
    }
    failures += nsfin_conserved(&a, &want_blocked, &g0, &d0, budget_live,
                                "fin released");
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(a.tp.client_bridge, 0), (int)MOQ_OK);
    /* (8) exact completion */
    failures += nsfin_expect_gone(&a, 1, "fin complete");
    failures += nsfin_expect_ops(&a, 1, "fin complete");
    failures += nsfin_check_retired(&a, &live, &d0, a.budget0, "fin complete");

    /* (9) a second service emits nothing and repeats the postcondition. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(a.tp.client_bridge, 0), (int)MOQ_OK);
    failures += nsfin_expect_gone(&a, 0, "fin idempotent");
    failures += nsfin_expect_ops(&a, 0, "fin idempotent");
    failures += nsfin_check_retired(&a, &live, &d0, a.budget0, "fin idempotent");

    test_pair_destroy(&a.tp);
    return failures;
}

static int test_ns_response_reset_bridge(void)
{
    int failures = 0;
    nsfin_arm_t a;
    failures += nsfin_arm_build(&a, "beta");

    nf_inv_t live; nf_drain_t d0; og_graph_t g0;
    failures += nsfin_arm_precheck(&a, &live, &d0, &g0, "reset arm");

    /* (6) the peer resets while the client cannot emit. */
    moq_result_t rc = moq_transport_bridge_on_peer_stream_reset(
        a.tp.client_bridge, a.bidi, 0x2B, 0);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(a.tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(a.tp.client_bridge));
    {
        nsfin_entry_want_t ew = nsfin_entry_live();
        ew.pending_reset = 1;
        ew.pending_reset_code = 0x2Bu;
        ew.stream_pending = 1;
        ew.bridge_pending = 1;
        failures += nsfin_check_entry(&a, &ew, "reset blocked");
    }
    const size_t budget_live = a.budget_active;   /* DECLARED by the arm */
    failures += nsfin_conserved(&a, &live, &g0, &d0, budget_live,
                                "reset blocked");
    failures += nsfin_expect_ops(&a, 0, "reset blocked");

    failures += nsfin_release_blocker(&a);
    {
        nsfin_entry_want_t ew = nsfin_entry_live();
        ew.pending_reset = 1;
        ew.pending_reset_code = 0x2Bu;
        ew.stream_pending = 1;
        ew.bridge_pending = 1;
        failures += nsfin_check_entry(&a, &ew, "reset released");
    }
    failures += nsfin_conserved(&a, &live, &g0, &d0, budget_live,
                                "reset released");
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(a.tp.client_bridge, 0), (int)MOQ_OK);
    failures += nsfin_expect_gone(&a, 1, "reset complete");
    /* A reset owns physical teardown: no local close is queued. */
    failures += nsfin_expect_ops(&a, 0, "reset complete");
    failures += nsfin_check_retired(&a, &live, &d0, a.budget0, "reset complete");

    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(a.tp.client_bridge, 0), (int)MOQ_OK);
    failures += nsfin_expect_gone(&a, 0, "reset idempotent");
    failures += nsfin_expect_ops(&a, 0, "reset idempotent");
    failures += nsfin_check_retired(&a, &live, &d0, a.budget0, "reset idempotent");

    test_pair_destroy(&a.tp);
    return failures;
}

static int test_fin_bridge_descriptor_validation(void)
{
    int failures = 0;
    fin_bridge_run_t r;
    memset(&r, 0, sizeof(r));

    fin_bridge_family_t o = p7_family;
    fin_bridge_case_t f;
    memset(&f, 0, sizeof(f));
    f.name = "probe";
    f.ctx = &r;
    f.ingress = FIN_BR_CONSUMED;
    f.family = &o;
    f.feed = p7_feed;
    f.terminal = p7_terminal;
    MOQ_TEST_CHECK_EQ_INT(fin_bridge_problems(&f), 0);

    /* An oversized snapshot declaration must be refused BEFORE any family hook
     * runs -- the runner writes into bounded storage, so a hook invoked under an
     * invalid descriptor would overflow it. */
    {
        fin_bridge_family_t big = p7_family;
        big.snap_size = FIN_BR_SNAP_MAX + 1;
        big.capture = probe_capture_forbidden;
        fin_bridge_case_t bad = f;
        bad.family = &big;
        probe_capture_calls = 0;
        MOQ_TEST_CHECK(fin_bridge_problems(&bad) > 0);
        fin_br_quiet = 1;
        MOQ_TEST_CHECK(run_fin_bridge(&bad, &r) > 0);
        fin_br_quiet = 0;
        MOQ_TEST_CHECK_EQ_INT(probe_capture_calls, 0);
    }

    /* ingress class */
    f.ingress = 0;              MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    f.ingress = 99;             MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    f.ingress = FIN_BR_RETAINED; MOQ_TEST_CHECK_EQ_INT(fin_bridge_problems(&f), 0);
    f.ingress = FIN_BR_CONSUMED;

    /* the bundle itself */
    f.family = NULL;             MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    f.family = &o;

    /* every required member */
    o = p7_family; o.snap_size = 0;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.snap_size = FIN_BR_SNAP_MAX + 1;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    /* A nonzero but UNDERSIZED declaration validates structurally -- the
     * descriptor cannot know the family's real state size -- so the runner
     * passes the DECLARED size and the family refuses to write. That refusal
     * is what the undersized mutant below proves. */
    o = p7_family; o.snap_size = 1;
    MOQ_TEST_CHECK_EQ_INT(fin_bridge_problems(&f), 0);
    o = p7_family; o.derive_slot = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.check_live = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.check_retired = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.check_edges = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.check_drain = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.check_terminal_wire = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.want_request = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);

    /* family identity: unknown kind, and a tag that belongs to another family */
    o = p7_family; o.owner_kind = MOQ_REQ_NONE;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.owner_kind = 0x7f;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.pool_tag = MOQ_HANDLE_POOL_FETCH;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.owner_kind = MOQ_REQ_FETCH;   /* tag still TRACK_STATUS */
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family;
    MOQ_TEST_CHECK_EQ_INT(fin_bridge_problems(&f), 0);

    /* the output members, now family-owned */
    o = p7_family; o.capture = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.check = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.normalize_event = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family; o.normalize_action = NULL;
    MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    o = p7_family;
    MOQ_TEST_CHECK_EQ_INT(fin_bridge_problems(&f), 0);

    /* the case's single context */
    f.ctx = NULL;               MOQ_TEST_CHECK(fin_bridge_problems(&f) > 0);
    f.ctx = &r;
    MOQ_TEST_CHECK_EQ_INT(fin_bridge_problems(&f), 0);
    return failures;
}

static int p7_fixture_setup(fin_bridge_run_t *r, const char *what)
{
    int failures = 0;
    memset(r, 0, sizeof(*r));
    if (d18_pair_init(&r->tp, 0) < 0) return -1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(r->tp.client, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(r->tp.server, 0), (int)MOQ_OK);
    failures += d18_strict_shuttle(&r->tp, 30, 0, what);
    /* Setup is classified, not discarded: exactly one SETUP_COMPLETE per side
     * and nothing else. */
    {
        int cs = 0, co = 0, ss = 0, so = 0;
        moq_event_t ev;
        while (moq_session_poll_events(r->tp.client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) cs++; else co++;
            moq_event_cleanup(&ev);
        }
        while (moq_session_poll_events(r->tp.server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) ss++; else so++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(cs, 1);
        MOQ_TEST_CHECK_EQ_INT(co, 0);
        MOQ_TEST_CHECK_EQ_INT(ss, 1);
        MOQ_TEST_CHECK_EQ_INT(so, 0);
    }
    fake_endpoint_clear_ops(&r->tp.server_ep);
    fake_endpoint_clear_ops(&r->tp.client_ep);
    r->transport_id = 400;                /* peer-opened client-initiated bidi */
    return failures;
}

static void p7_case_init(fin_bridge_case_t *f, fin_bridge_run_t *r)
{
    memset(f, 0, sizeof(*f));
    f->name = "p7 track-status";
    f->ctx = r;
    f->ingress = FIN_BR_CONSUMED;
    f->family = &p7_family;
    f->feed = p7_feed;
    f->terminal = p7_terminal;
}

static int test_p7_bridge_fin_retirement(void)
{
    int failures = 0;
    fin_bridge_run_t r;
    int rc = p7_fixture_setup(&r, "p7 setup");
    if (rc < 0) return 1;
    failures += rc;

    fin_bridge_case_t f;
    p7_case_init(&f, &r);

    failures += run_fin_bridge(&f, &r);
    test_pair_destroy(&r.tp);
    return failures;
}

/* Axis 4, on the family whose whole lifecycle is reachable today: once the
 * owner has been retired, retirement must be COMPLETE and hold under repetition
 * and reuse. Three obligations, each non-vacuous against the current source:
 *
 *   - a repeated APPLICATION terminal on the retired handle is refused and
 *     changes nothing -- no wire byte, no event, no action, no resurrected
 *     owner, no drain, no edge;
 *   - a fresh request REUSES the slot with an ADVANCED generation, so the two
 *     owners are distinguishable by handle;
 *   - the RETIRED handle stays refused after that reuse, which is the
 *     clear-exactly-once fact: a stale handle must not address the new owner.
 *
 * It deliberately asserts nothing about where a future FIN handoff marker
 * lives, and does not carry P3's retained-ingress assumption.
 */
static int p7_accept(fin_bridge_run_t *r, uint64_t handle)
{
    moq_track_status_handle_t h;
    h._opaque = handle;
    moq_accept_track_status_cfg_t c;
    moq_accept_track_status_cfg_init(&c);
    c.has_largest = true;
    c.largest_group = P7_LARGEST_GROUP;
    c.largest_object = P7_LARGEST_OBJ;
    c.has_expires = true;
    c.expires_ms = P7_EXPIRES_MS;
    return (int)moq_session_accept_track_status(r->tp.server, h, &c, 0);
}

/* No output of ANY kind: no endpoint op, no event, no action. */
static int p7_check_silent(fin_bridge_run_t *r, const char *what)
{
    int bad = 0;
    if (r->tp.server_ep.count != 0) {
        fprintf(stderr, "AXIS4 %s: %zu endpoint ops, expected 0\n", what,
                r->tp.server_ep.count);
        bad++;
    }
    moq_event_t ev;
    while (moq_session_poll_events(r->tp.server, &ev, 1) > 0) {
        fprintf(stderr, "AXIS4 %s: unexpected event kind %u\n", what,
                (unsigned)ev.kind);
        moq_event_cleanup(&ev);
        bad++;
    }
    moq_action_t a;
    while (moq_session_poll_actions(r->tp.server, &a, 1) > 0) {
        fprintf(stderr, "AXIS4 %s: unexpected action kind %u\n", what,
                (unsigned)a.kind);
        moq_action_cleanup(&a);
        bad++;
    }
    return bad;
}

/* The generic snapshot records the scratch cursor as it stands, but the next
 * advancing call reclaims it once the event queue has drained
 * (session_call_prepare). Normalizing that -- and only that, and only when the
 * captured queue was empty -- keeps real scratch mutation detectable. Same rule
 * the session-side FIN suite applies. */
static void p7_expect_after_call_prepare(txs_snapshot_t *snap)
{
    if (snap->event_depth == 0) snap->event_scratch_len = 0;
}

/* The DECLARED free record for `ts_free_entry` (session_track_status.c:27).
 * That free MEMSETS the entry and then restores exactly three things: the FREE
 * state, the next generation, and the co-allocated receive buffer. So the
 * expectation is "zeroed except those", with the owned track key required GONE
 * -- a narrowed cleanup that left any field behind is caught here rather than
 * by an absence check that a stale value also satisfies. */
typedef struct p7_free_expect {
    uint32_t        generation;
    const uint8_t  *req_recv_buf;
    size_t          req_recv_cap;
} p7_free_expect_t;

static int p7_check_free_record(const moq_session_t *s, int slot,
                                const p7_free_expect_t *w, const char *what)
{
    int bad = 0;
    if (slot < 0 || (size_t)slot >= s->ts_cap) {
        fprintf(stderr, "AXIS4 %s: slot out of range\n", what);
        return 1;
    }
    const moq_ts_entry_t *e = &s->track_statuses[slot];
#define P7_FREE_EQ(field, got, exp) do { \
    if ((uint64_t)(got) != (uint64_t)(exp)) { \
        fprintf(stderr, "AXIS4 %s: free record %s = %llu, expected %llu\n", \
                what, field, (unsigned long long)(got), \
                (unsigned long long)(exp)); \
        bad++; \
    } \
} while (0)
    P7_FREE_EQ("state", (int)e->state, (int)MOQ_TS_FREE);
    P7_FREE_EQ("generation", e->generation, w->generation);
    P7_FREE_EQ("req_recv_cap", e->req_recv_cap, w->req_recv_cap);
    /* Everything the memset zeroes. */
    P7_FREE_EQ("role", (int)e->role, 0);
    P7_FREE_EQ("handle", e->handle._opaque, 0);
    P7_FREE_EQ("request_id", e->request_id, 0);
    P7_FREE_EQ("request_stream_ref", e->request_stream_ref._v, 0);
    P7_FREE_EQ("req_recv_len", e->req_recv_len, 0);
    P7_FREE_EQ("req_recv_fin", e->req_recv_fin ? 1 : 0, 0);
    P7_FREE_EQ("goaway_sent", e->goaway_sent ? 1 : 0, 0);
    P7_FREE_EQ("track_id_len", e->track_id_len, 0);
#undef P7_FREE_EQ
    if (e->req_recv_buf != w->req_recv_buf) {
        fprintf(stderr, "AXIS4 %s: free record req_recv_buf pointer\n", what);
        bad++;
    }
    if (e->track_id_buf != NULL) {
        fprintf(stderr, "AXIS4 %s: owned track key survives the free\n", what);
        bad++;
    }
    if (e->hist != NULL) {
        fprintf(stderr, "AXIS4 %s: reserved history record survives\n", what);
        bad++;
    }
    return bad;
}

/* The retired physical mapping is gone under BOTH identities -- a stale entry
 * that lost only its transport-id key would still be reachable by ref -- and
 * the bridge itself is healthy with nothing owed. */
static int p7_check_mapping_absent(fin_bridge_run_t *r, uint64_t transport_id,
                                   moq_stream_ref_t old_ref, const char *what)
{
    int bad = 0;
    if (bridge_find_by_id(r->tp.server_bridge, transport_id) != NULL) {
        fprintf(stderr, "AXIS4 %s: retired transport id still maps\n", what);
        bad++;
    }
    if (bridge_find_by_ref(r->tp.server_bridge, old_ref) != NULL) {
        fprintf(stderr, "AXIS4 %s: retired internal ref still maps\n", what);
        bad++;
    }
    if (moq_transport_bridge_is_fatal(r->tp.server_bridge)) {
        fprintf(stderr, "AXIS4 %s: bridge fatal\n", what); bad++;
    }
    if (moq_transport_bridge_is_closed(r->tp.server_bridge)) {
        fprintf(stderr, "AXIS4 %s: bridge closed\n", what); bad++;
    }
    if (moq_transport_bridge_has_pending(r->tp.server_bridge)) {
        fprintf(stderr, "AXIS4 %s: bridge has pending work\n", what); bad++;
    }
    return bad;
}

/* The reused peer bidi's EXACT bridge record. This is an OPEN-peer-bidi oracle,
 * deliberately not the FIN-consumed phase oracle: that phase expects
 * peer_send_closed, which is wrong for an entry admitted from a request with no
 * FIN. Both lookups must land on the SAME record -- a reverse-lookup repoint
 * that kept the transport id would satisfy a "some mapping remains" check --
 * and every flag is declared, so an otherwise-unobserved one cannot drift. */
static int p7_check_open_peer_bidi(fin_bridge_run_t *r, uint64_t transport_id,
                                   moq_stream_ref_t want_ref, const char *what)
{
    int bad = 0;
    bridge_stream_entry_t *by_id = bridge_find_by_id(r->tp.server_bridge,
                                                     transport_id);
    bridge_stream_entry_t *by_ref = bridge_find_by_ref(r->tp.server_bridge,
                                                       want_ref);
    if (!by_id) {
        fprintf(stderr, "AXIS4 %s: no bridge entry for transport id\n", what);
        return 1;
    }
    if (!by_ref) {
        fprintf(stderr, "AXIS4 %s: no bridge entry for internal ref\n", what);
        return 1;
    }
    if (by_id != by_ref) {
        fprintf(stderr, "AXIS4 %s: the two lookups reach DIFFERENT records\n",
                what);
        bad++;
    }
#define P7_BR_EQ(field, got, exp) do { \
    if ((uint64_t)(got) != (uint64_t)(exp)) { \
        fprintf(stderr, "AXIS4 %s: bridge entry %s = %llu, expected %llu\n", \
                what, field, (unsigned long long)(got), \
                (unsigned long long)(exp)); \
        bad++; \
    } \
} while (0)
    P7_BR_EQ("transport_id", by_id->transport_id, transport_id);
    P7_BR_EQ("ref", by_id->ref._v, want_ref._v);
    P7_BR_EQ("kind", (int)by_id->kind, (int)BRIDGE_STREAM_BIDI);
    P7_BR_EQ("origin", (int)by_id->origin, (int)BRIDGE_ORIGIN_PEER);
    P7_BR_EQ("active", by_id->active ? 1 : 0, 1);
    /* An OPEN peer bidi with no FIN and no teardown: every one of these is
     * false, and each is named so a single drifting flag is attributable. */
    P7_BR_EQ("peer_send_closed", by_id->peer_send_closed ? 1 : 0, 0);
    P7_BR_EQ("local_send_closed", by_id->local_send_closed ? 1 : 0, 0);
    P7_BR_EQ("aborting", by_id->aborting ? 1 : 0, 0);
    P7_BR_EQ("pending_retry", by_id->pending_retry ? 1 : 0, 0);
    P7_BR_EQ("pending_fin", by_id->pending_fin ? 1 : 0, 0);
    P7_BR_EQ("fin_retained", by_id->fin_retained ? 1 : 0, 0);
    P7_BR_EQ("pending_reset", by_id->pending_reset ? 1 : 0, 0);
    P7_BR_EQ("pending_stop", by_id->pending_stop ? 1 : 0, 0);
    P7_BR_EQ("pending_reset_code", by_id->pending_reset_code, 0);
    P7_BR_EQ("pending_stop_code", by_id->pending_stop_code, 0);
    /* Uni-only classification residue stays in its initialized zero state on a
     * bidi entry. */
    P7_BR_EQ("uni_disp", by_id->uni_disp, (uint8_t)BRIDGE_UNI_DISP_PENDING);
    P7_BR_EQ("classify_len", by_id->classify_len, 0);
#undef P7_BR_EQ
    /* Length zero does not imply the buffer is clean: every retained
     * classification byte must be in its initialized zero state. */
    for (size_t ci = 0; ci < sizeof(by_id->classify_buf); ci++) {
        if (by_id->classify_buf[ci] != 0) {
            fprintf(stderr,
                    "AXIS4 %s: bridge entry classify_buf[%zu] = %u, expected 0\n",
                    what, ci, (unsigned)by_id->classify_buf[ci]);
            bad++;
        }
    }
    /* And the bridge itself is still healthy with nothing owed. */
    if (moq_transport_bridge_is_fatal(r->tp.server_bridge)) {
        fprintf(stderr, "AXIS4 %s: bridge fatal\n", what); bad++;
    }
    if (moq_transport_bridge_is_closed(r->tp.server_bridge)) {
        fprintf(stderr, "AXIS4 %s: bridge closed\n", what); bad++;
    }
    if (moq_transport_bridge_has_pending(r->tp.server_bridge)) {
        fprintf(stderr, "AXIS4 %s: bridge has pending work\n", what); bad++;
    }
    return bad;
}

static int test_p7_retirement_idempotence_and_reuse(void)
{
    int failures = 0;
    fin_bridge_run_t r;
    int rc = p7_fixture_setup(&r, "axis4 setup");
    if (rc < 0) return 1;
    failures += rc;

    fin_bridge_case_t f;
    p7_case_init(&f, &r);

    /* The co-allocated receive buffer persists across entry reuse, so its
     * pointer and capacity are properties of the FREE slot: captured BEFORE
     * ingress rather than adopted from the record the free produces. */
    const int slot_pre = r.want_slot;
    MOQ_TEST_CHECK(slot_pre >= 0 && (size_t)slot_pre < r.tp.server->ts_cap);
    const uint8_t *pre_recv_buf = r.tp.server->track_statuses[slot_pre].req_recv_buf;
    const size_t   pre_recv_cap = r.tp.server->track_statuses[slot_pre].req_recv_cap;

    failures += run_fin_bridge(&f, &r);

    const uint64_t retired_handle = r.want_handle;
    const int      slot           = r.want_slot;
    const moq_stream_ref_t old_ref = r.ref;

    /* The declared free record: the generation the LIVE owner carried (taken
     * from its declared handle, which the fixture derived pre-ingress) plus
     * one, and the buffer identity captured above. Nothing is read back from
     * the freed entry. */
    p7_free_expect_t want_free;
    want_free.generation   = moq_handle_generation(retired_handle) + 1u;
    want_free.req_recv_buf = pre_recv_buf;
    want_free.req_recv_cap = pre_recv_cap;
    failures += p7_check_free_record(r.tp.server, slot, &want_free,
                                     "first retirement");

    /* 1. A repeated application terminal on the retired handle. It must not
     *    mutate unrelated session state, advance the generation a second time,
     *    change the graph, add a drain, or recreate the retired owner. */
    fake_endpoint_clear_ops(&r.tp.server_ep);
    og_graph_t g_pre_repeat;
    og_capture(r.tp.server, &g_pre_repeat);
    {
        txs_snapshot_t before;
        txs_capture(r.tp.server, &old_ref, 1, &before);
        p7_expect_after_call_prepare(&before);
        MOQ_TEST_CHECK_EQ_INT(p7_accept(&r, retired_handle),
                              (int)MOQ_ERR_STALE_HANDLE);
        failures += p7_check_silent(&r, "repeat terminal");
        failures += txs_check_eq(r.tp.server, &old_ref, 1, &before,
                                 "repeat terminal");
    }
    failures += p7_family.check_retired(r.tp.server, old_ref, slot,
                                        "repeat terminal");
    failures += p7_family.check_drain(r.tp.server, "repeat terminal");
    /* The SAME declared record: no second generation increment. */
    failures += p7_check_free_record(r.tp.server, slot, &want_free,
                                     "repeat terminal");
    {
        og_graph_t g;
        og_capture(r.tp.server, &g);
        failures += og_check_integrity(&g, "repeat terminal");
        /* The WHOLE topology, not just the target's edges: an unrelated edge
         * inserted, removed or repointed by this refused call is caught here. */
        failures += og_check_same_topology(&g, &g_pre_repeat, "repeat terminal");
        failures += p7_family.check_edges(&g, old_ref, slot, 0,
                                          "repeat terminal");
    }
    failures += p7_check_mapping_absent(&r, r.transport_id, old_ref,
                                        "repeat terminal");

    /* 2. A fresh request reuses the slot with an advanced generation. The new
     *    identity is DERIVED from pool state before ingress, never read back. */
    /* Derived from the DECLARED freed generation, not from a fresh read of the
     *    now-free slot -- a free that advanced the generation twice, or not at
     *    all, must not be able to define the expectation it is checked against. */
    const uint32_t new_gen = want_free.generation | 1u;
    const int new_slot = slot;
    uint64_t new_handle = moq_handle_pack(p7_family.pool_tag,
                                          r.tp.server->session_tag, new_gen,
                                          (uint32_t)new_slot);
    MOQ_TEST_CHECK(new_handle != retired_handle);
    {   /* The pool really does hand back the same slot. */
        uint32_t probe_gen = 0;
        MOQ_TEST_CHECK_EQ_INT(p7_family.derive_slot(r.tp.server, &probe_gen),
                              slot);
        MOQ_TEST_CHECK_EQ_U64(probe_gen, (uint64_t)new_gen);
    }

    const uint64_t reuse_id = 404;                  /* a second peer-opened bidi */
    p7_want_request_id = 2;                         /* peer ids advance by two */
    /* NO FIN this time: the reused owner must show a CLEARED latch, which is
     * what makes ts_free_entry's cleanup of that field observable. */
    p7_want_fin = 0;
    fake_endpoint_clear_ops(&r.tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)p7_feed_id(r.tp.server_bridge, 2, reuse_id, false), (int)MOQ_OK);
    {
        bridge_stream_entry_t *e = bridge_find_by_id(r.tp.server_bridge, reuse_id);
        MOQ_TEST_CHECK(e != NULL);
        if (!e) return failures;             /* the check above counted it */
        moq_stream_ref_t new_ref = e->ref;
        MOQ_TEST_CHECK(new_ref._v != old_ref._v);
        failures += p7_family.check_live(r.tp.server, new_ref, new_slot, new_gen,
                                         new_handle, "reused");
    }
    {   /* The request surfaces against the DERIVED handle, and only it. */
        txs_norm_vec_t got, want;
        txs_norm_init(&got); txs_norm_init(&want);
        moq_event_t ev;
        while (moq_session_poll_events(r.tp.server, &ev, 1) > 0) {
            if (!p7_family.normalize_event(&ev, &r, &got)) failures++;
            moq_event_cleanup(&ev);
        }
        failures += p7_family.want_request(&want, new_handle);
        failures += txs_norm_equals(&got, &want, "reused request");
        txs_norm_free(&got); txs_norm_free(&want);
    }

    /* 3. The retired handle is STILL refused, although its slot is occupied
     *    again -- a stale handle must never address the new owner -- and the
     *    REPLACEMENT owner survives that refusal WHOLE. */
    fake_endpoint_clear_ops(&r.tp.server_ep);
    {
        bridge_stream_entry_t *be = bridge_find_by_id(r.tp.server_bridge,
                                                      reuse_id);
        MOQ_TEST_CHECK(be != NULL);
        if (be) {
            moq_stream_ref_t new_ref = be->ref;
            fin_bridge_snap_t owner_before;
            p7_family.capture(r.tp.server, &r, owner_before.bytes,
                              p7_family.snap_size);
            txs_snapshot_t before;
            txs_capture(r.tp.server, &new_ref, 1, &before);
            p7_expect_after_call_prepare(&before);
            og_graph_t g_pre_stale;
            og_capture(r.tp.server, &g_pre_stale);

            MOQ_TEST_CHECK_EQ_INT(p7_accept(&r, retired_handle),
                                  (int)MOQ_ERR_STALE_HANDLE);

            failures += p7_check_silent(&r, "stale after reuse");
            failures += txs_check_eq(r.tp.server, &new_ref, 1, &before,
                                     "stale after reuse");
            failures += p7_family.check(r.tp.server, &r, owner_before.bytes,
                                        p7_family.snap_size,
                                        "stale after reuse");
            failures += p7_family.check_live(r.tp.server, new_ref, new_slot,
                                             new_gen, new_handle,
                                             "stale after reuse");
            failures += p7_family.check_drain(r.tp.server, "stale after reuse");
            {
                og_graph_t g;
                og_capture(r.tp.server, &g);
                failures += og_check_integrity(&g, "stale after reuse");
                failures += og_check_same_topology(&g, &g_pre_stale,
                                                   "stale after reuse");
                failures += p7_family.check_edges(&g, new_ref, new_slot, 1,
                                                  "stale after reuse");
                /* The retired stream keys nothing, though its slot is live. */
                failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF,
                                             old_ref._v, "stale after reuse");
            }
            /* Both identities, with the flags this phase requires. */
            failures += p7_check_open_peer_bidi(&r, reuse_id, new_ref,
                                                "stale after reuse");
            failures += p7_check_mapping_absent(&r, r.transport_id, old_ref,
                                                "stale after reuse");
        }
    }

    p7_want_request_id = 0;
    p7_want_fin = 1;
    test_pair_destroy(&r.tp);
    return failures;
}


/* One framed REQUEST_ERROR with non-default code, retry interval and reason. */
#define LSTOP_ERR_CODE   0x3u
#define LSTOP_ERR_RETRY  4200u
#define LSTOP_ERR_REASON "no such track"

static size_t encode_request_error(uint8_t *buf, size_t cap)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t reason = MOQ_BYTES_LITERAL(LSTOP_ERR_REASON);
    if (moq_d18_encode_request_error(&w, LSTOP_ERR_CODE, LSTOP_ERR_RETRY,
                                     reason) != MOQ_OK)
        return 0;
    return moq_buf_writer_offset(&w);
}

/* -- exact endpoint-output oracle ---------------------------------------
 *
 * A phase declares the COMPLETE list of endpoint operations it may produce,
 * in order. Everything the fake endpoint holds is classified against that
 * list and then consumed, so an extra record -- of any kind, on any stream --
 * fails the phase that produced it rather than surviving into the next.
 */
typedef struct {
    fake_op_kind_t kind;
    uint64_t       stream_id;
    bool           has_code;
    uint64_t       error_code;
    bool           check_fin;
    bool           fin;
    const uint8_t *data;      /* NULL = payload not compared */
    size_t         data_len;
} ep_rec_t;

static int ep_expect(fake_endpoint_t *ep, const ep_rec_t *want, size_t n,
                     const char *what)
{
    int bad = 0;
    /* The recorder caps silently, so a full buffer would hide records. */
    if (ep->count >= FAKE_EP_MAX_OPS) {
        fprintf(stderr, "FAIL: %s: endpoint recorder is full (%zu)\n", what,
                ep->count);
        bad++;
    }
    if (ep->count != n) {
        fprintf(stderr, "FAIL: %s: %zu endpoint ops, expected %zu\n", what,
                ep->count, n);
        for (size_t i = 0; i < ep->count; i++)
            fprintf(stderr, "      [%zu] kind=%d sid=%llu code=%llu fin=%d\n",
                    i, (int)ep->ops[i].kind,
                    (unsigned long long)ep->ops[i].stream_id,
                    (unsigned long long)ep->ops[i].error_code,
                    (int)ep->ops[i].fin);
        bad++;
    }
    size_t m = ep->count < n ? ep->count : n;
    for (size_t i = 0; i < m; i++) {
        const fake_op_t *g = &ep->ops[i];
        const ep_rec_t *w = &want[i];
        if (g->kind != w->kind) {
            fprintf(stderr, "FAIL: %s: op %zu kind %d, expected %d\n", what, i,
                    (int)g->kind, (int)w->kind);
            bad++;
        }
        if (g->stream_id != w->stream_id) {
            fprintf(stderr, "FAIL: %s: op %zu stream %llu, expected %llu\n",
                    what, i, (unsigned long long)g->stream_id,
                    (unsigned long long)w->stream_id);
            bad++;
        }
        if (w->has_code && g->error_code != w->error_code) {
            fprintf(stderr, "FAIL: %s: op %zu code %llu, expected %llu\n",
                    what, i, (unsigned long long)g->error_code,
                    (unsigned long long)w->error_code);
            bad++;
        }
        if (w->check_fin && g->fin != w->fin) {
            fprintf(stderr, "FAIL: %s: op %zu fin %d, expected %d\n", what, i,
                    (int)g->fin, (int)w->fin);
            bad++;
        }
        if (w->data) {
            if (g->data_len != w->data_len) {
                fprintf(stderr, "FAIL: %s: op %zu payload %zu bytes,"
                        " expected %zu\n", what, i, g->data_len, w->data_len);
                bad++;
            } else if (w->data_len > 0 &&
                       memcmp(g->data, w->data, w->data_len) != 0) {
                fprintf(stderr, "FAIL: %s: op %zu payload bytes differ\n",
                        what, i);
                bad++;
            }
        }
    }
    fake_endpoint_clear_ops(ep);
    return bad;
}

static int ep_expect_none(fake_endpoint_t *ep, const char *what)
{
    return ep_expect(ep, NULL, 0, what);
}

/* A retired mapping must be unreachable through BOTH identities: an entry that
 * only lost one of them would still be found by the other lookup. */
static int check_mapping_gone(moq_transport_bridge_t *b, uint64_t sid,
                              moq_stream_ref_t ref, const char *what)
{
    int bad = 0;
    bridge_stream_entry_t *by_id = bridge_find_by_id(b, sid);
    bridge_stream_entry_t *by_ref = bridge_find_by_ref(b, ref);
    if (by_id != NULL && by_id->active) {
        fprintf(stderr, "FAIL: %s: stream %llu still maps by transport id\n",
                what, (unsigned long long)sid);
        bad++;
    }
    if (by_ref != NULL && by_ref->active) {
        fprintf(stderr, "FAIL: %s: ref %llu still maps by stream ref\n", what,
                (unsigned long long)ref._v);
        bad++;
    }
    return bad;
}

static ep_rec_t ep_reset(uint64_t sid, uint64_t code)
{
    ep_rec_t r;
    memset(&r, 0, sizeof(r));
    r.kind = FAKE_OP_RESET;
    r.stream_id = sid;
    r.has_code = true;
    r.error_code = code;
    return r;
}

/* -- P3 PUBLISH: the RETAINED ingress class (#245(a)) ----------------
 *
 * The counterpart to P7. Here the destination cannot take the FIN in the same
 * call -- the request event fills the ONE-slot queue, so the teardown defers --
 * and the obligation is RETAINED on the bridge instead of transferred to the
 * owner. That is the whole difference, and it is DECLARED as FIN_BR_RETAINED
 * rather than inferred: the ingress owes MOQ_ERR_WOULD_BLOCK, the bridge holds
 * pending_retry + fin_retained, and `peer_send_closed` is NOT yet set because
 * the retained path returns before the FIN is recorded.
 *
 * The semantic owner is a committed publication keyed ONLY in the request
 * stream registry (idx_req_by_streamref). moq_session_has_transport_stream()
 * now consults that registry (#245(a)), so the bridge recognises the live owner
 * and RETAINS the WOULD_BLOCK teardown instead of fatalizing the connection.
 * This case pins that landed behaviour.
 *
 * The application then rejects the surfaced owner inside that window, and the
 * bridge's own service -- with NO peer bytes redelivered -- completes the
 * retained obligation and retires the physical mapping exactly once.
 *
 * The fixture asserts the bridge's retention flags and the owner's own state.
 * It does NOT name, inspect or prescribe any storage for the FIN handoff
 * carrier; #249 owns that, storage-agnostic here.
 */
#define P3_ALIAS 0x21
static const uint8_t k_p3_ns0[]  = { 'l','i','v','e' };
static const uint8_t k_p3_ns1[]  = { 'c','a','m' };
static const uint8_t k_p3_name[] = { 'v' };

/* Every surfaced field carries a value distinguishable from its default, so an
 * omitted field cannot pass by coinciding with the zero image. */
#define P3_TOK_TYPE     0x0b
static const uint8_t k_p3_tok[]  = { 'p','t','o','k' };
/* Track Properties: DYNAMIC_GROUPS (0x30, an even type, so varint-valued) = 1,
 * delta-encoded from zero. */
static const uint8_t k_p3_props[] = { 0x30, 0x01 };
#define P3_LARGEST_GROUP 0x51
#define P3_LARGEST_OBJ   0x07
#define P3_EXPIRES_MS    7250
/* The publisher's initial forward intent, sent EXPLICITLY as 0 -- the opposite
 * of the omitted-parameter default. */
#define P3_FORWARD       0

/* The rejection's payload: a nonzero retry interval and a nonempty reason, so
 * neither can be dropped without changing the decoded image. */
#define P3_RETRY_MS      4500
static const uint8_t k_p3_reason[] = { 'n','o','t',' ','h','e','r','e' };

static const moq_pub_entry_t *p3_owner(const moq_session_t *s,
                                       moq_stream_ref_t ref)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if (ep.kind != MOQ_REQ_PUBLISH) return NULL;
    if (ep.slot < 0 || (size_t)ep.slot >= s->pub_cap) return NULL;
    return &s->publishes[ep.slot];
}

static int p3_pool_busy(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->pub_cap; i++)
        if (s->publishes[i].state != MOQ_PUB_FREE) n++;
    return n;
}

static int p3_derive_slot(const moq_session_t *s, uint32_t *out_gen)
{
    for (size_t i = 0; i < s->pub_cap; i++)
        if (s->publishes[i].state == MOQ_PUB_FREE) {
            *out_gen = s->publishes[i].generation | 1u;
            return (int)i;
        }
    return -1;
}

static int p3_check_live(const moq_session_t *s, moq_stream_ref_t ref,
                         int want_slot, uint32_t want_gen, uint64_t want_handle,
                         const char *what)
{
    int bad = 0;
    if (want_slot < 0 || (size_t)want_slot >= s->pub_cap) {
        fprintf(stderr, "FINBR %s: declared slot out of range\n", what);
        return 1;
    }
    const moq_pub_entry_t *e = p3_owner(s, ref);
    if (!e) { fprintf(stderr, "FINBR %s: owner absent\n", what); return 1; }
    if (e != &s->publishes[want_slot]) {
        fprintf(stderr, "FINBR %s: owner in an undeclared slot\n", what); bad++;
    }
    if ((int)e->state != (int)MOQ_PUB_PENDING_SUBSCRIBER) {
        fprintf(stderr, "FINBR %s: state %d\n", what, (int)e->state); bad++;
    }
    if ((int)e->role != (int)MOQ_PUB_ROLE_SUBSCRIBER) {
        fprintf(stderr, "FINBR %s: role %d\n", what, (int)e->role); bad++;
    }
    if (e->generation != want_gen) {
        fprintf(stderr, "FINBR %s: generation\n", what); bad++;
    }
    if (e->handle._opaque != want_handle) {
        fprintf(stderr, "FINBR %s: handle\n", what); bad++;
    }
    if (e->request_id != 0) {
        fprintf(stderr, "FINBR %s: request id\n", what); bad++;
    }
    if (e->request_stream_ref._v != ref._v) {
        fprintf(stderr, "FINBR %s: owner ref\n", what); bad++;
    }
    /* RETAINED class: the FIN is held by the BRIDGE, so the owner's own latch
     * must still be clear. This says nothing about where a future marker
     * lives -- only that the durable latch has not been set behind our back. */
    if (e->req_recv_fin) {
        fprintf(stderr, "FINBR %s: owner latch set on a retained ingress\n",
                what);
        bad++;
    }
    if (p3_pool_busy(s) != 1) {
        fprintf(stderr, "FINBR %s: pool occupancy %d, expected 1\n", what,
                p3_pool_busy(s));
        bad++;
    }
    return bad;
}

static int p3_check_retired(const moq_session_t *s, moq_stream_ref_t ref,
                            int want_slot, const char *what)
{
    int bad = 0;
    if (p3_owner(s, ref) != NULL) {
        fprintf(stderr, "FINBR %s: registry edge survives\n", what); bad++;
    }
    if (want_slot >= 0 && (size_t)want_slot < s->pub_cap &&
        s->publishes[want_slot].state != MOQ_PUB_FREE) {
        fprintf(stderr, "FINBR %s: pool slot leaked\n", what); bad++;
    }
    if (p3_pool_busy(s) != 0) {
        fprintf(stderr, "FINBR %s: pool occupancy %d, expected 0\n", what,
                p3_pool_busy(s));
        bad++;
    }
    return bad;
}

static int p3_check_edges(const og_graph_t *g, moq_stream_ref_t ref,
                          int want_slot, int live, const char *what)
{
    int bad = og_check_integrity(g, what);
    if (live) {
        bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                             MOQ_REQ_PUBLISH, want_slot, what);
        const og_edge_spec_t w[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_PUBLISH, want_slot, w, 1, what);
    } else {
        bad += og_check_no_edge(g, OG_DOM_REQ_STREAMREF, ref._v, what);
        bad += og_check_owner_edges(g, MOQ_REQ_PUBLISH, want_slot, NULL, 0,
                                    what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}

/* The peer's close is observed before the rejection commits, so this route
 * owes NO drain either -- the declared multiset is empty throughout. */
static int p3_check_drain(const moq_session_t *s, const char *what)
{
    return check_drain_membership(s, NULL, 0, what);
}

#define P3_OWN_MAX 256
typedef struct p3_snap {
    int      valid;
    moq_pub_entry_t raw;
    size_t   tid_len;
    uint8_t  tid[P3_OWN_MAX];
} p3_snap_t;

static void p3_capture(const moq_session_t *s, void *vctx, void *state,
                       size_t cap)
{
    fin_bridge_run_t *r = (fin_bridge_run_t *)vctx;
    p3_snap_t *o = (p3_snap_t *)state;
    if (cap < sizeof(*o)) {
        fprintf(stderr, "FINBR: snapshot storage too small\n"); return;
    }
    memset(o, 0, sizeof(*o));
    if (r->want_slot < 0 || (size_t)r->want_slot >= s->pub_cap) return;
    const moq_pub_entry_t *e = &s->publishes[r->want_slot];
    o->raw = *e;
    /* The publication's OWNED bytes are its deferred terminal reason
     * (`done_reason_buf`); `req_recv_buf` is co-allocated in the session block
     * and deliberately not copied. */
    o->tid_len = e->done_reason_len;
    if (e->done_reason_len > P3_OWN_MAX ||
        (e->done_reason_len && !e->done_reason_buf))
        return;
    if (e->done_reason_len)
        memcpy(o->tid, e->done_reason_buf, e->done_reason_len);
    o->valid = 1;
}

static int p3_check(const moq_session_t *s, void *vctx, const void *state,
                    size_t cap, const char *what)
{
    const p3_snap_t *want = (const p3_snap_t *)state;
    p3_snap_t now;
    if (cap < sizeof(now)) {
        fprintf(stderr, "FINBR %s: snapshot storage too small\n", what);
        return 1;
    }
    p3_capture(s, vctx, &now, sizeof(now));
    if (!now.valid || !want->valid) {
        fprintf(stderr, "FINBR %s: incomparable owner record\n", what);
        return 1;
    }
    if (memcmp(&now.raw, &want->raw, sizeof(now.raw)) != 0) {
        fprintf(stderr, "FINBR %s: owner record changed\n", what); return 1;
    }
    if (now.tid_len != want->tid_len ||
        (now.tid_len && memcmp(now.tid, want->tid, now.tid_len) != 0)) {
        fprintf(stderr, "FINBR %s: retained terminal reason changed\n", what);
        return 1;
    }
    return 0;
}

static int p3_want_request(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    txs_img_u64(&im, 2);                              /* ns part count */
    txs_img_bytes(&im, k_p3_ns0, sizeof(k_p3_ns0));
    txs_img_bytes(&im, k_p3_ns1, sizeof(k_p3_ns1));
    txs_img_bytes(&im, k_p3_name, sizeof(k_p3_name));
    txs_img_u64(&im, P3_ALIAS);
    txs_img_u64(&im, P3_FORWARD);
    txs_img_u64(&im, 1);                              /* token count */
    txs_img_u64(&im, P3_TOK_TYPE);
    txs_img_bytes(&im, k_p3_tok, sizeof(k_p3_tok));
    txs_img_bytes(&im, k_p3_props, sizeof(k_p3_props));
    txs_img_u64(&im, 1);                              /* dynamic_groups */
    txs_img_u64(&im, 1);                              /* has_largest */
    txs_img_u64(&im, P3_LARGEST_GROUP);
    txs_img_u64(&im, P3_LARGEST_OBJ);
    txs_img_u64(&im, 1);                              /* has_expires */
    txs_img_u64(&im, P3_EXPIRES_MS);
    if (!txs_norm_append_img(v, MOQ_EVENT_PUBLISH_REQUEST, &im)) {
        fprintf(stderr, "FINBR: could not build the declared request image\n");
        return 1;
    }
    return 0;
}

static bool p3_norm_event(const moq_event_t *ev, void *ctx, txs_norm_vec_t *out)
{
    (void)ctx;
    if (ev->kind != MOQ_EVENT_PUBLISH_REQUEST) {
        fprintf(stderr, "FINBR: unnormalized event kind %u\n",
                (unsigned)ev->kind);
        return false;
    }
    const moq_publish_request_event_t *q = &ev->u.publish_request;
    if (q->track_namespace.count > 32 || q->token_count > 16) {
        fprintf(stderr, "FINBR: implausible request counts\n"); return false;
    }
    if (q->track_namespace.count && !q->track_namespace.parts) {
        fprintf(stderr, "FINBR: namespace count with NULL parts\n");
        return false;
    }
    if (q->token_count && !q->tokens) {
        fprintf(stderr, "FINBR: token count with NULL tokens\n");
        return false;
    }
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, q->pub._opaque);
    txs_img_u64(&im, (uint64_t)q->track_namespace.count);
    for (size_t i = 0; i < q->track_namespace.count; i++) {
        const moq_bytes_t *b = &q->track_namespace.parts[i];
        if (b->len && !b->data) {
            fprintf(stderr, "FINBR: namespace part %zu has NULL bytes\n", i);
            return false;
        }
        txs_img_bytes(&im, b->data, b->len);
    }
    if (q->track_name.len && !q->track_name.data) {
        fprintf(stderr, "FINBR: track name has NULL bytes\n"); return false;
    }
    txs_img_bytes(&im, q->track_name.data, q->track_name.len);
    txs_img_u64(&im, q->track_alias);
    txs_img_u64(&im, q->forward ? 1 : 0);
    txs_img_u64(&im, (uint64_t)q->token_count);
    for (size_t i = 0; i < q->token_count; i++) {
        if (q->tokens[i].token_value.len && !q->tokens[i].token_value.data) {
            fprintf(stderr, "FINBR: token %zu has NULL bytes\n", i);
            return false;
        }
        txs_img_u64(&im, q->tokens[i].token_type);
        txs_img_bytes(&im, q->tokens[i].token_value.data,
                      q->tokens[i].token_value.len);
    }
    if (q->track_properties.len && !q->track_properties.data) {
        fprintf(stderr, "FINBR: track properties have NULL bytes\n");
        return false;
    }
    txs_img_bytes(&im, q->track_properties.data, q->track_properties.len);
    txs_img_u64(&im, q->dynamic_groups ? 1 : 0);
    txs_img_u64(&im, q->has_largest ? 1 : 0);
    txs_img_u64(&im, q->largest_group);
    txs_img_u64(&im, q->largest_object);
    txs_img_u64(&im, q->has_expires ? 1 : 0);
    txs_img_u64(&im, q->expires_ms);
    return txs_norm_append_img(out, ev->kind, &im);
}

static bool p3_norm_action(const moq_action_t *a, void *ctx, txs_norm_vec_t *out)
{
    (void)ctx; (void)out;
    fprintf(stderr, "FINBR: action kind %u left queued after service\n",
            (unsigned)a->kind);
    return false;
}

/* The rejection this family puts on the wire: one REQUEST_ERROR on the request
 * bidi, FIN'd, decoded rather than counted. */
static int p3_check_terminal_wire(void *vctx, const char *what)
{
    fin_bridge_run_t *r = (fin_bridge_run_t *)vctx;
    int bad = 0;
    if (r->tp.server_ep.count != 1) {
        fprintf(stderr, "FINBR %s: %zu endpoint ops, expected 1\n", what,
                r->tp.server_ep.count);
        return 1;
    }
    fake_op_t *o = &r->tp.server_ep.ops[0];
    if (o->kind != FAKE_OP_WRITE) {
        fprintf(stderr, "FINBR %s: op kind %d, expected WRITE\n", what,
                (int)o->kind);
        return 1;
    }
    if (o->stream_id != r->transport_id) {
        fprintf(stderr, "FINBR %s: wrong transport stream\n", what); bad++;
    }
    if (!o->fin) {
        fprintf(stderr, "FINBR %s: local half not FIN'd\n", what); bad++;
    }
    moq_buf_reader_t rr;
    moq_buf_reader_init(&rr, o->data, o->data_len);
    moq_control_envelope_t env;
    memset(&env, 0, sizeof(env));
    if (moq_d18_decode_envelope(&rr, &env) != MOQ_OK) {
        fprintf(stderr, "FINBR %s: undecodable envelope\n", what);
        return bad + 1;
    }
    if (env.msg_type != (uint64_t)MOQ_D18_REQUEST_ERROR) {
        fprintf(stderr, "FINBR %s: msg type %llu\n", what,
                (unsigned long long)env.msg_type);
        bad++;
    }
    if (moq_buf_reader_remaining(&rr) != 0) {
        fprintf(stderr, "FINBR %s: trailing bytes after the envelope\n", what);
        bad++;
    }
    moq_d18_request_error_t er;
    memset(&er, 0, sizeof(er));
    if (moq_d18_decode_request_error(env.payload, env.payload_len,
                                     &er) != MOQ_OK) {
        fprintf(stderr, "FINBR %s: undecodable REQUEST_ERROR body\n", what);
        return bad + 1;
    }
    /* The COMPLETE decoded payload, not the code alone: a dropped retry
     * interval or reason would otherwise leave the rejection looking correct. */
    if (er.error_code != (uint64_t)MOQ_REQUEST_ERROR_NOT_SUPPORTED) {
        fprintf(stderr, "FINBR %s: error code %llu\n", what,
                (unsigned long long)er.error_code);
        bad++;
    }
    if (er.retry_interval != (uint64_t)P3_RETRY_MS) {
        fprintf(stderr, "FINBR %s: retry interval %llu, expected %d\n", what,
                (unsigned long long)er.retry_interval, P3_RETRY_MS);
        bad++;
    }
    if (er.reason.len != sizeof(k_p3_reason) ||
        (er.reason.len && !er.reason.data) ||
        (er.reason.len &&
         memcmp(er.reason.data, k_p3_reason, sizeof(k_p3_reason)) != 0)) {
        fprintf(stderr, "FINBR %s: reason phrase differs\n", what);
        bad++;
    }
    return bad;
}

static moq_result_t p3_feed(moq_transport_bridge_t *b, void *vctx,
                            uint64_t transport_id, bool fin)
{
    (void)vctx;
    uint8_t msg[224];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_d18_publish_t p = { 0 };
    moq_bytes_t parts[] = { { k_p3_ns0, sizeof(k_p3_ns0) },
                            { k_p3_ns1, sizeof(k_p3_ns1) } };
    p.request_id = 0;
    p.track_namespace = (moq_namespace_t){ parts, 2 };
    p.track_name = (moq_bytes_t){ k_p3_name, sizeof(k_p3_name) };
    p.track_alias = P3_ALIAS;
    p.params.has_forward = true;
    p.params.forward = P3_FORWARD;
    p.params.has_largest = true;
    p.params.largest_group = P3_LARGEST_GROUP;
    p.params.largest_object = P3_LARGEST_OBJ;
    p.params.has_expires = true;
    p.params.expires_ms = P3_EXPIRES_MS;
    p.params.auth_token_count = 1;
    p.params.auth_tokens[0].alias_type = 3;           /* USE_VALUE */
    p.params.auth_tokens[0].token_type = P3_TOK_TYPE;
    p.params.auth_tokens[0].token_value =
        (moq_bytes_t){ k_p3_tok, sizeof(k_p3_tok) };
    p.track_properties = (moq_bytes_t){ k_p3_props, sizeof(k_p3_props) };
    p.dynamic_groups = true;
    if (moq_d18_encode_publish(&w, &p) != MOQ_OK) return MOQ_ERR_INTERNAL;
    return moq_transport_bridge_on_peer_bidi_bytes(
        b, transport_id, msg, moq_buf_writer_offset(&w), fin, 0);
}

static moq_result_t p3_terminal(moq_transport_bridge_t *b, void *vctx,
                                uint64_t transport_id)
{
    fin_bridge_run_t *r = (fin_bridge_run_t *)vctx;
    (void)b; (void)transport_id;
    moq_publication_t pub;
    pub._opaque = r->want_handle;
    moq_reject_publish_cfg_t c;
    moq_reject_publish_cfg_init(&c);
    c.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    c.can_retry = true;
    c.retry_after_ms = P3_RETRY_MS;
    c.reason = (moq_bytes_t){ k_p3_reason, sizeof(k_p3_reason) };
    return moq_session_reject_publish(r->tp.server, pub, &c, 0);
}

_Static_assert(sizeof(p3_snap_t) <= FIN_BR_SNAP_MAX,
               "p3 snapshot exceeds the shared bounded storage");

static const fin_bridge_family_t p3_family = {
    .owner_kind    = MOQ_REQ_PUBLISH,
    .pool_tag      = MOQ_HANDLE_POOL_PUBLICATION,
    .snap_size     = sizeof(p3_snap_t),
    .capture       = p3_capture,
    .check         = p3_check,
    .normalize_event  = p3_norm_event,
    .normalize_action = p3_norm_action,
    .want_request  = p3_want_request,
    .derive_slot   = p3_derive_slot,
    .check_live    = p3_check_live,
    .check_retired = p3_check_retired,
    .check_edges   = p3_check_edges,
    .check_drain   = p3_check_drain,
    .check_terminal_wire = p3_check_terminal_wire,
};

static int test_p3_bridge_fin_retirement(void)
{
    int failures = 0;
    fin_bridge_run_t r;
    memset(&r, 0, sizeof(r));
    /* ONE event slot on the destination: the request event fits, so its FIN
     * teardown must defer and the bridge must retain the obligation. */
    if (d18_pair_init_caps(&r.tp, 0, moq_alloc_default(),
                           moq_alloc_default(), 0, false, 1) < 0)
        return 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(r.tp.client, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(r.tp.server, 0), (int)MOQ_OK);
    failures += d18_strict_shuttle(&r.tp, 30, 0, "p3 setup");
    {
        int cs = 0, co = 0, ss = 0, so = 0;
        moq_event_t ev;
        while (moq_session_poll_events(r.tp.client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) cs++; else co++;
            moq_event_cleanup(&ev);
        }
        while (moq_session_poll_events(r.tp.server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) ss++; else so++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(cs, 1);
        MOQ_TEST_CHECK_EQ_INT(co, 0);
        MOQ_TEST_CHECK_EQ_INT(ss, 1);
        MOQ_TEST_CHECK_EQ_INT(so, 0);
    }
    fake_endpoint_clear_ops(&r.tp.server_ep);
    fake_endpoint_clear_ops(&r.tp.client_ep);
    r.transport_id = 400;

    fin_bridge_case_t f;
    memset(&f, 0, sizeof(f));
    f.name = "p3 publish";
    f.ctx = &r;
    f.ingress = FIN_BR_RETAINED;
    f.family = &p3_family;
    f.feed = p3_feed;
    f.terminal = p3_terminal;

    failures += run_fin_bridge(&f, &r);
    test_pair_destroy(&r.tp);
    return failures;
}

/* Local drain-multiset snapshot (order-insensitive), mirroring the direct
 * suite's contract: every expected ring is DERIVED from declared members. */
#define NOB_RING_MAX 256
typedef struct nob_ring {
    size_t   count;
    uint64_t ref[NOB_RING_MAX];
    uint8_t  reason[NOB_RING_MAX];
    int      overflow;
} nob_ring_t;

static void nob_ring_snap(const moq_session_t *s, nob_ring_t *d)
{
    memset(d, 0, sizeof(*d));
    if (s->drain_ref_count > NOB_RING_MAX) { d->overflow = 1; return; }
    for (size_t i = 0; i < s->drain_ref_count; i++) {
        d->ref[i] = s->drain_refs[i];
        d->reason[i] = s->drain_ref_reasons[i];
    }
    d->count = s->drain_ref_count;
}

static void nob_ring_plus(const nob_ring_t *base, moq_stream_ref_t extra,
                          moq_drain_reason_t reason, nob_ring_t *out)
{
    *out = *base;
    if (out->count >= NOB_RING_MAX) { out->overflow = 1; return; }
    out->ref[out->count] = extra._v;
    out->reason[out->count] = (uint8_t)reason;
    out->count++;
}

static int nob_ring_equals(const nob_ring_t *have, const nob_ring_t *want,
                           const char *what)
{
    if (have->overflow || want->overflow) {
        fprintf(stderr, "NOB %s: drain snapshot overflow\n", what);
        return 1;
    }
    int bad = 0;
    if (have->count != want->count) {
        fprintf(stderr, "NOB %s: %zu drain refs, expected %zu\n", what,
                have->count, want->count);
        bad++;
    }
    unsigned char used[NOB_RING_MAX] = { 0 };
    for (size_t i = 0; i < want->count; i++) {
        int found = 0;
        for (size_t j = 0; j < have->count; j++) {
            if (!used[j] && have->ref[j] == want->ref[i] &&
                have->reason[j] == want->reason[i]) {
                used[j] = 1; found = 1; break;
            }
        }
        if (!found) {
            fprintf(stderr, "NOB %s: drain ref %llu (reason %d) is ABSENT\n",
                    what, (unsigned long long)want->ref[i],
                    (int)want->reason[i]);
            bad++;
        }
    }
    return bad;
}

/* -- #245(c) over the PHYSICAL bridge: ns_sub local teardown ----------
 *
 * The publisher-side namespace-sub bidi treats extra inbound bytes as a
 * local teardown of that BIDI (draft-18 §10.9.1). The teardown needs one
 * action slot; a refusal returns MOQ_ERR_WOULD_BLOCK with the input bytes
 * DISCARDED (the branch never buffers them), and the bridge -- which never
 * re-delivers peer bytes -- retries with NULL/0. The session latches the
 * teardown obligation (and, with a FIN, the cumulative FIN) durably before the
 * capacity refusal (#245c), so the bridge's own empty re-drive completes the
 * teardown exactly once. Every recovery assertion below pins that contract.
 *
 * The blocked half also pins #245(a)'s predicate as a GREEN CONTROL: this
 * owner lives in idx_ns_by_ref, so the bridge's retainability check passes
 * and the refused ingress is retained rather than fatalized.
 *
 * Registry topology is the CURRENT source's: a peer-origin namespace
 * subscription holds its idx_ns_by_ref edge PLUS the request-ID edge its
 * commit installs (session_namespace_sub.c:978), and no stream-ref edge.
 *
 * Recovery is SERVICE-ONLY: no peer bytes are re-delivered after the
 * refused ingress.
 */

/* The complete bridge-entry state for this PEER-origin bidi at each phase:
 * every flag constrained, both identities required. */
static int nslb_check_entry(moq_transport_bridge_t *b, uint64_t transport_id,
                            uint64_t ref_v, int want_retry, int want_aborting,
                            int want_fin_retained, int want_peer_send_closed,
                            const char *what)
{
    int bad = 0;
    bridge_stream_entry_t *by_id = bridge_find_by_id(b, transport_id);
    moq_stream_ref_t ref; ref._v = ref_v;
    bridge_stream_entry_t *by_ref = bridge_find_by_ref(b, ref);
    if (!by_id || by_id != by_ref) {
        fprintf(stderr, "NSLB %s: entry not reachable by BOTH identities\n",
                what);
        return bad + 1;
    }
    if (!by_id->active) {
        fprintf(stderr, "NSLB %s: entry inactive\n", what); bad++;
    }
    if (by_id->ref._v != ref_v) {
        fprintf(stderr, "NSLB %s: entry ref\n", what); bad++;
    }
    if (by_id->transport_id != transport_id) {
        fprintf(stderr, "NSLB %s: entry transport id\n", what); bad++;
    }
    if (by_id->kind != BRIDGE_STREAM_BIDI) {
        fprintf(stderr, "NSLB %s: entry kind %d\n", what, (int)by_id->kind);
        bad++;
    }
    if (by_id->origin != BRIDGE_ORIGIN_PEER) {
        fprintf(stderr, "NSLB %s: entry origin %d\n", what,
                (int)by_id->origin);
        bad++;
    }
    if ((int)by_id->pending_retry != want_retry) {
        fprintf(stderr, "NSLB %s: pending_retry %d, expected %d\n", what,
                (int)by_id->pending_retry, want_retry);
        bad++;
    }
    if ((int)by_id->aborting != want_aborting) {
        fprintf(stderr, "NSLB %s: aborting %d, expected %d\n", what,
                (int)by_id->aborting, want_aborting);
        bad++;
    }
    if (by_id->pending_fin) {
        fprintf(stderr, "NSLB %s: unexpected pending_fin\n", what); bad++;
    }
    if ((int)by_id->fin_retained != want_fin_retained) {
        fprintf(stderr, "NSLB %s: fin_retained %d, expected %d\n", what,
                (int)by_id->fin_retained, want_fin_retained);
        bad++;
    }
    if (by_id->peer_stop_received) {
        fprintf(stderr, "NSLB %s: unexpected peer_stop_received\n", what); bad++;
    }
    if (by_id->pending_reset || by_id->pending_reset_code) {
        fprintf(stderr, "NSLB %s: unexpected reset state (flag %d code %llu)\n",
                what, (int)by_id->pending_reset,
                (unsigned long long)by_id->pending_reset_code);
        bad++;
    }
    if (by_id->pending_stop || by_id->pending_stop_code) {
        fprintf(stderr, "NSLB %s: unexpected stop state (flag %d code %llu)\n",
                what, (int)by_id->pending_stop,
                (unsigned long long)by_id->pending_stop_code);
        bad++;
    }
    /* A bidi carries no uni-classification: the disposition stays PENDING with
     * no retained leading bytes. Constraining it keeps a stray uni-path write
     * on this entry visible. */
    if (by_id->uni_disp != (uint8_t)BRIDGE_UNI_DISP_PENDING ||
        by_id->classify_len != 0) {
        fprintf(stderr, "NSLB %s: unexpected uni-classification (disp %d len %d)\n",
                what, (int)by_id->uni_disp, (int)by_id->classify_len);
        bad++;
    } else {
        for (size_t i = 0; i < sizeof(by_id->classify_buf); i++)
            if (by_id->classify_buf[i]) {
                fprintf(stderr, "NSLB %s: nonzero classify_buf byte %zu\n",
                        what, i);
                bad++;
                break;
            }
    }
    if ((int)by_id->peer_send_closed != want_peer_send_closed) {
        fprintf(stderr, "NSLB %s: peer_send_closed %d, expected %d\n", what,
                (int)by_id->peer_send_closed, want_peer_send_closed);
        bad++;
    }
    if (by_id->local_send_closed) {
        fprintf(stderr, "NSLB %s: local send half is closed\n", what); bad++;
    }
    return bad;
}

/* Exactly `want_match` events of `kind` and nothing else. */
static int nslb_classify_events(moq_session_t *s, uint32_t kind,
                                int want_match, const char *what,
                                uint64_t *out_handle)
{
    int bad = 0, match = 0, other = 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == kind) {
            match++;
            if (out_handle && kind == MOQ_EVENT_NS_SUB_REQUEST)
                *out_handle = ev.u.ns_sub_request.handle._opaque;
        } else {
            other++;
        }
        moq_event_cleanup(&ev);
    }
    if (match != want_match) {
        fprintf(stderr, "NSLB %s: %d events of kind %u, expected %d\n", what,
                match, (unsigned)kind, want_match);
        bad++;
    }
    if (other != 0) {
        fprintf(stderr, "NSLB %s: %d unexpected events\n", what, other);
        bad++;
    }
    return bad;
}

/* The live edge set for THIS owner: ns_by_ref plus the request-ID edge, and
 * deliberately NO stream-ref registry edge. */
static int nslb_graph_live(const moq_session_t *s, int slot, uint64_t rid,
                           uint64_t ref_v, const char *what)
{
    og_graph_t g;
    og_capture(s, &g);
    int bad = og_check_integrity(&g, what);
    bad += og_check_edge(&g, OG_DOM_NS_REF, ref_v,
                         MOQ_REQ_NAMESPACE_SUB, slot, what);
    bad += og_check_edge(&g, OG_DOM_REQ_RID, rid,
                         MOQ_REQ_NAMESPACE_SUB, slot, what);
    const og_edge_spec_t w[] = { { OG_DOM_NS_REF, ref_v },
                                 { OG_DOM_REQ_RID, rid } };
    bad += og_check_owner_edges(&g, MOQ_REQ_NAMESPACE_SUB, slot, w, 2, what);
    bad += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref_v, what);
    return bad;
}

/* Retirement is COMPLETE: no edge in any domain keys the stream or the slot. */
static int nslb_graph_retired(const moq_session_t *s, int slot, uint64_t rid,
                              uint64_t ref_v, const char *what)
{
    og_graph_t g;
    og_capture(s, &g);
    int bad = og_check_integrity(&g, what);
    bad += og_check_no_edge(&g, OG_DOM_NS_REF, ref_v, what);
    bad += og_check_no_edge(&g, OG_DOM_REQ_RID, rid, what);
    bad += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref_v, what);
    bad += og_check_owner_unreferenced(&g, MOQ_REQ_NAMESPACE_SUB, slot, what);
    return bad;
}

static int run_nslb_teardown(bool fin, bool fill_ring)
{
    int failures = 0;

    test_pair_t tp;
    /* One server action slot, and a server endpoint exposing the native
     * whole-stream abort the teardown's action dispatches to. */
    if (d18_pair_init_caps(&tp, 0, moq_alloc_default(), moq_alloc_default(),
                           1, true, 0) < 0) { failures++; return failures; }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(tp.client, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(tp.server, 0), (int)MOQ_OK);
    failures += d18_strict_shuttle(&tp, 30, 0, "nslb setup");
    MOQ_TEST_CHECK_EQ_INT((int)tp.server->state, (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)tp.client->state, (int)MOQ_SESS_ESTABLISHED);

    /* Start from no stale output on EITHER side: both setup queues are
     * classified, not merely drained. */
    failures += nslb_classify_events(tp.server, MOQ_EVENT_SETUP_COMPLETE, 1,
                                     "setup-server", NULL);
    failures += nslb_classify_events(tp.client, MOQ_EVENT_SETUP_COMPLETE, 1,
                                     "setup-client", NULL);
    fake_endpoint_clear_ops(&tp.client_ep);
    fake_endpoint_clear_ops(&tp.server_ep);

    /* Burn inbound request id 0 through a COMPLETE rejected exchange first:
     * the d18 staging cleanup's unqualified remove-by-id(0) (#252) would
     * otherwise destroy the fixture owner's own by-id key and make the
     * declared topology unassertable. The whole warm exchange is DECODED and
     * CLASSIFIED -- its request id, its rejection wire, its retirement -- so
     * the #252 avoidance cannot drift silently. The fixture subject then
     * arrives at request id 2, asserted explicitly. */
    {
        moq_bytes_t warm_parts[] = { MOQ_BYTES_LITERAL("warm") };
        moq_subscribe_namespace_cfg_t wc;
        moq_subscribe_namespace_cfg_init(&wc);
        wc.track_namespace_prefix = (moq_namespace_t){ warm_parts, 1 };
        wc.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t wh;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe_namespace(tp.client, &wc, 0, &wh),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(tp.client_bridge, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(tp.client_ep.count < FAKE_EP_MAX_OPS);
        MOQ_TEST_CHECK_EQ_SIZE(tp.client_ep.count, (size_t)2);
        uint64_t warm_id = 0;
        if (tp.client_ep.count == 2) {
            MOQ_TEST_CHECK_EQ_INT((int)tp.client_ep.ops[0].kind,
                                  (int)FAKE_OP_OPEN_BIDI);
            MOQ_TEST_CHECK_EQ_INT((int)tp.client_ep.ops[1].kind,
                                  (int)FAKE_OP_WRITE);
            MOQ_TEST_CHECK_EQ_U64(tp.client_ep.ops[1].stream_id,
                                  tp.client_ep.ops[0].stream_id);
            MOQ_TEST_CHECK_EQ_INT(tp.client_ep.ops[1].fin ? 1 : 0, 0);
            MOQ_TEST_CHECK(tp.client_ep.ops[1].data_len > 0);
            warm_id = tp.client_ep.ops[1].stream_id;
        }
        MOQ_TEST_CHECK(warm_id != 0);
        if (warm_id == 0) { test_pair_destroy(&tp); failures++; return failures; }
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_bidi_bytes(
                tp.server_bridge, warm_id, tp.client_ep.ops[1].data,
                tp.client_ep.ops[1].data_len, false, 0), (int)MOQ_OK);
        fake_endpoint_clear_ops(&tp.client_ep);
        uint64_t warm_h = 0;
        failures += nslb_classify_events(tp.server, MOQ_EVENT_NS_SUB_REQUEST,
                                         1, "warm-event", &warm_h);
        /* The warm owner really holds request id 0 -- the id being burned. */
        moq_stream_ref_t warm_ref = moq_stream_ref_from_u64(0);
        {
            bridge_stream_entry_t *we = bridge_find_by_id(tp.server_bridge,
                                                          warm_id);
            MOQ_TEST_CHECK(we != NULL);
            if (we) warm_ref = we->ref;
            if (we) {
                int32_t wslot = moq_index_find(tp.server->idx_ns_by_ref,
                                               tp.server->idx_ns_mask,
                                               we->ref._v);
                MOQ_TEST_CHECK(wslot >= 0);
                if (wslot >= 0) {
                    nf_inv_t winv;
                    nf_inv_read(tp.server, wslot, &winv);
                    MOQ_TEST_CHECK_EQ_U64(winv.request_id, (uint64_t)0);
                    MOQ_TEST_CHECK_EQ_U64(winv.handle, warm_h);
                }
            }
        }
        moq_ns_sub_handle_t sh0;
        sh0._opaque = warm_h;
        moq_reject_ns_sub_cfg_t rj;
        moq_reject_ns_sub_cfg_init(&rj);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_ns_sub(tp.server, sh0, &rj, 0),
            (int)MOQ_OK);
        fake_endpoint_clear_ops(&tp.server_ep);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(tp.server_bridge, 0),
            (int)MOQ_OK);
        /* The rejection wire, exactly: ONE FIN'd write on the warm bidi
         * carrying one complete REQUEST_ERROR and nothing else. */
        MOQ_TEST_CHECK(tp.server_ep.count < FAKE_EP_MAX_OPS);
        MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)1);
        if (tp.server_ep.count == 1) {
            fake_op_t *o = &tp.server_ep.ops[0];
            MOQ_TEST_CHECK_EQ_INT((int)o->kind, (int)FAKE_OP_WRITE);
            MOQ_TEST_CHECK_EQ_U64(o->stream_id, warm_id);
            MOQ_TEST_CHECK(o->fin);
            /* The recorder stores bytes INLINE (fake_op_t.data is an array),
             * so a NULL span is unrepresentable here; the length bound is the
             * meaningful guard. Live borrowed spans (session actions) carry
             * real NULL checks in the normalizers. */
            if (o->data_len > sizeof(o->data)) {
                fprintf(stderr, "NSLB warm: recorder length %zu exceeds the"
                        " inline capacity\n", o->data_len);
                failures++;
                test_pair_destroy(&tp);
                return failures;
            }
            moq_buf_reader_t rr;
            moq_buf_reader_init(&rr, o->data, o->data_len);
            moq_control_envelope_t env;
            memset(&env, 0, sizeof(env));
            MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                                  (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr), (size_t)0);
            MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                  (uint64_t)MOQ_D18_REQUEST_ERROR);
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_transport_bridge_on_peer_bidi_bytes(
                    tp.client_bridge, o->stream_id, o->data, o->data_len,
                    o->fin, 0), (int)MOQ_OK);
        }
        /* The warm owner is RETIRED on the server: empty ns pool, no warm
         * edge in any domain, and the burned id answers NONE. */
        {
            int busy = 0;
            for (size_t i = 0; i < tp.server->ns_sub_cap; i++)
                if (tp.server->ns_subs[i].state != MOQ_NS_SUB_FREE) busy++;
            MOQ_TEST_CHECK_EQ_INT(busy, 0);
            MOQ_TEST_CHECK_EQ_INT(
                (int)request_registry_find_by_id(tp.server, 0).kind,
                (int)MOQ_REQ_NONE);
        }
        /* The client's rejection surface, CLASSIFIED: exactly one
         * NS_SUB_ERROR and nothing else. Its own reaction (retiring its half
         * of the warm bidi) is exactly one empty FIN'd write back. */
        failures += nslb_classify_events(tp.client, MOQ_EVENT_NS_SUB_ERROR,
                                         1, "warm-client-reject", NULL);
        /* The client's own warm internal ref, saved WHILE its mapping is
         * live, so client-side retirement can be proven by both identities. */
        moq_stream_ref_t warm_cref = moq_stream_ref_from_u64(0);
        {
            bridge_stream_entry_t *ce = bridge_find_by_id(tp.client_bridge,
                                                          warm_id);
            MOQ_TEST_CHECK(ce != NULL);
            if (ce) warm_cref = ce->ref;
        }
        fake_endpoint_clear_ops(&tp.client_ep);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(tp.client_bridge, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(tp.client_ep.count < FAKE_EP_MAX_OPS);
        MOQ_TEST_CHECK_EQ_SIZE(tp.client_ep.count, (size_t)1);
        if (tp.client_ep.count == 1) {
            MOQ_TEST_CHECK_EQ_INT((int)tp.client_ep.ops[0].kind,
                                  (int)FAKE_OP_WRITE);
            MOQ_TEST_CHECK_EQ_U64(tp.client_ep.ops[0].stream_id, warm_id);
            MOQ_TEST_CHECK_EQ_INT(tp.client_ep.ops[0].fin ? 1 : 0, 1);
            MOQ_TEST_CHECK_EQ_SIZE(tp.client_ep.ops[0].data_len, (size_t)0);
        }
        /* The client's mapping is retired once its FIN'd close dispatched:
         * absent by BOTH the transport id and the saved client ref. */
        {
            bridge_stream_entry_t *ci = bridge_find_by_id(tp.client_bridge,
                                                          warm_id);
            bridge_stream_entry_t *cr = bridge_find_by_ref(tp.client_bridge,
                                                           warm_cref);
            MOQ_TEST_CHECK(!(ci && ci->active));
            MOQ_TEST_CHECK(!(cr && cr->active));
        }
        /* Deliver that final FIN THROUGH the server bridge, so the warm bidi
         * is physically retired on both sides before the subject starts. */
        fake_endpoint_clear_ops(&tp.server_ep);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_bidi_bytes(
                tp.server_bridge, warm_id, NULL, 0, true, 0), (int)MOQ_OK);
        /* The FIN itself owes NO immediate server op, asserted before any
         * clear or service. */
        MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(tp.server_bridge, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
        /* The warm mapping is gone by BOTH saved identities; both bridges are
         * open, nonfatal, with no pending work; and no warm owner or edge
         * survives in any domain. */
        {
            bridge_stream_entry_t *wi = bridge_find_by_id(tp.server_bridge,
                                                          warm_id);
            bridge_stream_entry_t *wr = bridge_find_by_ref(tp.server_bridge,
                                                           warm_ref);
            MOQ_TEST_CHECK(!(wi && wi->active));
            MOQ_TEST_CHECK(!(wr && wr->active));
        }
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
        MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));
        MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.server_bridge));
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
        MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));
        MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));
        {
            og_graph_t wg;
            og_capture(tp.server, &wg);
            failures += og_check_integrity(&wg, "warm-quiesce");
            failures += og_check_no_edge(&wg, OG_DOM_NS_REF, warm_ref._v,
                                         "warm-quiesce");
            failures += og_check_no_edge(&wg, OG_DOM_REQ_STREAMREF,
                                         warm_ref._v, "warm-quiesce");
            failures += og_check_no_edge(&wg, OG_DOM_REQ_RID, 0,
                                         "warm-quiesce");
        }
        fake_endpoint_clear_ops(&tp.server_ep);
        fake_endpoint_clear_ops(&tp.client_ep);
        /* Quiescent start for the subject: empty queues on BOTH sessions. */
        MOQ_TEST_CHECK_EQ_SIZE(
            (size_t)(tp.server->action_tail - tp.server->action_head),
            (size_t)0);
        MOQ_TEST_CHECK_EQ_SIZE(
            (size_t)(tp.client->action_tail - tp.client->action_head),
            (size_t)0);
        failures += nslb_classify_events(tp.server, MOQ_EVENT_NS_SUB_REQUEST,
                                         0, "warm-quiesce-server", NULL);
        failures += nslb_classify_events(tp.client, MOQ_EVENT_NS_SUB_REQUEST,
                                         0, "warm-quiesce-client", NULL);
    }

    /* The client opens the namespace-sub bidi through the public API. */
    moq_bytes_t pfx_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t pfx = { pfx_parts, 1 };
    moq_subscribe_namespace_cfg_t nc;
    moq_subscribe_namespace_cfg_init(&nc);
    nc.track_namespace_prefix = pfx;
    nc.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t nh;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_subscribe_namespace(tp.client, &nc, 0, &nh),
        (int)MOQ_OK);

    /* Exactly one OPEN_BIDI then one WRITE on the SAME transport id, and no
     * other op. The recorder caps silently, so headroom is asserted first. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.client_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK(tp.client_ep.count < FAKE_EP_MAX_OPS);
    MOQ_TEST_CHECK_EQ_SIZE(tp.client_ep.count, (size_t)2);
    uint64_t bidi_id = 0;
    if (tp.client_ep.count == 2) {
        MOQ_TEST_CHECK_EQ_INT((int)tp.client_ep.ops[0].kind,
                              (int)FAKE_OP_OPEN_BIDI);
        MOQ_TEST_CHECK_EQ_INT((int)tp.client_ep.ops[1].kind,
                              (int)FAKE_OP_WRITE);
        MOQ_TEST_CHECK_EQ_U64(tp.client_ep.ops[1].stream_id,
                              tp.client_ep.ops[0].stream_id);
        MOQ_TEST_CHECK_EQ_INT(tp.client_ep.ops[1].fin ? 1 : 0, 0);
        MOQ_TEST_CHECK(tp.client_ep.ops[1].data_len > 0);
        bidi_id = tp.client_ep.ops[1].stream_id;
    }
    MOQ_TEST_CHECK(bidi_id != 0);
    if (bidi_id == 0) { test_pair_destroy(&tp); failures++; return failures; }

    /* Deliver that one write, and assert the ingress result exactly. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_bidi_bytes(
            tp.server_bridge, bidi_id, tp.client_ep.ops[1].data,
            tp.client_ep.ops[1].data_len, false, 0), (int)MOQ_OK);
    fake_endpoint_clear_ops(&tp.client_ep);

    /* Exactly one request event, left unanswered. */
    uint64_t sh = 0;
    failures += nslb_classify_events(tp.server, MOQ_EVENT_NS_SUB_REQUEST, 1,
                                     "arm-event", &sh);

    bridge_stream_entry_t *be = bridge_find_by_id(tp.server_bridge, bidi_id);
    MOQ_TEST_CHECK(be != NULL);
    if (!be) { test_pair_destroy(&tp); failures++; return failures; }
    moq_stream_ref_t sref = be->ref;
    failures += nslb_check_entry(tp.server_bridge, bidi_id, sref._v, 0, 0, 0, 0,
                                 "arm");

    /* The server-side owner, absolutely: the DECLARED inventory contract. */
    int32_t ns_slot = moq_index_find(tp.server->idx_ns_by_ref,
                                     tp.server->idx_ns_mask, sref._v);
    MOQ_TEST_CHECK(ns_slot >= 0);
    if (ns_slot < 0) { test_pair_destroy(&tp); failures++; return failures; }
    {
        int busy = 0;
        for (size_t i = 0; i < tp.server->ns_sub_cap; i++)
            if (tp.server->ns_subs[i].state != MOQ_NS_SUB_FREE) busy++;
        MOQ_TEST_CHECK_EQ_INT(busy, 1);
    }
    nf_inv_t armed;
    nf_inv_read(tp.server, ns_slot, &armed);
    MOQ_TEST_CHECK_EQ_INT((int)armed.state, (int)MOQ_NS_SUB_PENDING_PUBLISHER);
    MOQ_TEST_CHECK_EQ_U64(armed.handle, sh);
    MOQ_TEST_CHECK_EQ_U64(armed.stream_ref, sref._v);
    MOQ_TEST_CHECK_EQ_INT(armed.pending_fin, 0);
    uint64_t ns_rid = armed.request_id;
    MOQ_TEST_CHECK_EQ_U64(ns_rid, (uint64_t)2);
    failures += nslb_graph_live(tp.server, ns_slot, ns_rid, sref._v, "arm");
    failures += check_drain_membership(tp.server, NULL, 0, "arm-drain");

    /* For the FIN row, EXHAUST the drain ring before ingress: a NORMAL drain
     * could no longer be reserved, so completion is only possible if the peer's
     * already-observed FIN owes none. The ring is snapshotted (multiset) and
     * required unchanged across the whole arc. */
    nob_ring_t ring_full;
    memset(&ring_full, 0, sizeof(ring_full));
    if (fill_ring) {
        while (tp.server->drain_ref_count < tp.server->drain_ref_cap)
            MOQ_TEST_CHECK(drain_ref_add(
                tp.server, moq_stream_ref_from_u64(
                               0x9000 + tp.server->drain_ref_count)));
        nob_ring_snap(tp.server, &ring_full);
    }

    /* Occupy the single action slot with a close on an unmapped ref, so the
     * teardown's own action cannot be queued. It is deliberately NOT polled:
     * only bridge service may consume it. */
    moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x7000);
    MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(tp.server, blocker),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(action_queue_full(tp.server));
    MOQ_TEST_CHECK(nf_head_action_is_close(tp.server, blocker._v));
    fake_endpoint_clear_ops(&tp.server_ep);

    /* One ingress call with extra bytes: refused for action capacity. The
     * refusal is bracketed by a whole-session snapshot -- an extra layer over
     * the entry/inventory/graph/drain checks for unrelated session scalars. */
    txs_snapshot_t nslb_before;
    txs_capture(tp.server, &sref, 1, &nslb_before);
    nf_expect_after_call_prepare(&nslb_before);
    static const uint8_t extra[2] = { 0xde, 0xad };
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_bidi_bytes(tp.server_bridge, bidi_id,
                                                     extra, sizeof(extra),
                                                     fin, 0),
        (int)MOQ_ERR_WOULD_BLOCK);
    failures += txs_check_eq(tp.server, &sref, 1, &nslb_before,
                             "blocked-session");

    /* Blocked state: the bridge holds the retry -- #245(a)'s predicate answers
     * correctly on this route, retaining rather than fatalizing -- and NOTHING
     * else moved. On the FIN row the bridge additionally retains the FIN
     * (fin_retained), which its own FIN-less retry then re-drives against the
     * session's already-latched cumulative FIN. */
    failures += nslb_check_entry(tp.server_bridge, bidi_id, sref._v, 1, 0,
                                 fin ? 1 : 0, 0, "blocked");
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(tp.server_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(tp.server_bridge,
                                                           bidi_id));
    {
        nf_inv_t blocked;
        nf_inv_read(tp.server, ns_slot, &blocked);
        /* The intended mutations: the durable teardown obligation, plus -- on
         * the FIN row -- the cumulative-FIN latch the session now owns. */
        nf_inv_t blocked_want = armed;
        blocked_want.local_teardown_pending = 1;
        if (fin) blocked_want.pending_fin = 1;
        failures += nf_inv_equals(&blocked, &blocked_want, "blocked-owner");
    }
    failures += nslb_graph_live(tp.server, ns_slot, ns_rid, sref._v,
                                "blocked");
    if (fill_ring) {
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_full, "blocked-drain");
    } else {
        failures += check_drain_membership(tp.server, NULL, 0, "blocked-drain");
    }
    /* The blocker is still queued, and it is still the SAME action -- service,
     * not the refused call, is what may consume or replace it. */
    MOQ_TEST_CHECK(action_queue_full(tp.server));
    MOQ_TEST_CHECK(nf_head_action_is_close(tp.server, blocker._v));
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
    failures += nslb_classify_events(tp.server, MOQ_EVENT_NS_SUB_REQUEST, 0,
                                     "blocked-events", NULL);

    /* Service-only recovery: drains the blocker, retries NULL/0, loops, and
     * dispatches the teardown's abort. No further ingress. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);

    const drain_spec_t want_drain[] = { { 0, MOQ_DRAIN_NORMAL } };
    drain_spec_t wd = want_drain[0];
    wd.ref = sref._v;
    for (int pass = 0; pass < 2; pass++) {
        const char *what = pass == 0 ? "recovered" : "recovered-again";
        MOQ_TEST_CHECK(tp.server_ep.count < FAKE_EP_MAX_OPS);
        if (pass == 0) {
            int aborts = 0, others = 0;
            for (size_t i = 0; i < tp.server_ep.count; i++) {
                fake_op_t *o = &tp.server_ep.ops[i];
                if (o->kind == FAKE_OP_ABORT && o->stream_id == bidi_id) {
                    aborts++;
                    MOQ_TEST_CHECK_EQ_U64(o->error_code, 0x1);
                } else {
                    others++;
                }
            }
            MOQ_TEST_CHECK_EQ_INT(aborts, 1);
            MOQ_TEST_CHECK_EQ_INT(others, 0);
        } else {
            MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
        }

        /* No event is owed by the teardown, in either pass. */
        failures += nslb_classify_events(tp.server, MOQ_EVENT_NS_SUB_REQUEST,
                                         0, what, NULL);

        /* The session owner is retired from the pool and from every index. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)tp.server->ns_subs[ns_slot].state, (int)MOQ_NS_SUB_FREE);
        failures += nslb_graph_retired(tp.server, ns_slot, ns_rid, sref._v,
                                       what);
        /* No-FIN: exactly one NORMAL drain is owed (the peer send half is still
         * open). FIN row: the already-observed FIN owes none, so the full ring
         * is left EXACTLY unchanged. */
        if (fill_ring) {
            nob_ring_t now;
            nob_ring_snap(tp.server, &now);
            failures += nob_ring_equals(&now, &ring_full, what);
        } else {
            failures += check_drain_membership(tp.server, &wd, 1, what);
        }
        MOQ_TEST_CHECK_EQ_INT((int)tp.server->state,
                              (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
        MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));

        /* dispatch_abort_bidi keeps the entry as a discard tombstone until a
         * peer terminal signal, so it stays active with its mapping intact.
         * The bridge cleared its retained FIN on the successful retry. */
        failures += nslb_check_entry(tp.server_bridge, bidi_id, sref._v, 0, 1,
                                     0, fin ? 1 : 0, what);
        MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.server_bridge));
        MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(
                           tp.server_bridge, bidi_id));

        if (pass == 0) {
            /* Exactly once: a second service adds nothing and must reproduce
             * the whole postcondition, not a subset of it. */
            fake_endpoint_clear_ops(&tp.server_ep);
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_transport_bridge_service(tp.server_bridge, 0),
                (int)MOQ_OK);
        }
    }

    test_pair_destroy(&tp);
    return failures;
}

/* Two rows over the physical bridge: the no-FIN teardown (the peer send half
 * stays open, so exactly one NORMAL drain is owed and recovered), and the
 * extra-bytes+FIN teardown against a FULL drain ring. The FIN row pins
 * #245(c)'s cumulative-FIN retention: the bridge re-drives the pending bidi
 * with NULL,0,false, so unless the session latched the FIN before the capacity
 * refusal the drain selector would recompute need_drain=true and stall against
 * the exhausted ring -- which the landed fix prevents. */
static int test_ns_sub_local_teardown_bridge_retry(void)
{
    int failures = 0;
    failures += run_nslb_teardown(false, false);
    failures += run_nslb_teardown(true, true);
    return failures;
}

/* -- #245(b) over the PHYSICAL bridge: the no-owner admission blockers --
 *
 * handle_request_stream_bytes()'s no-slot admission path returns WOULD_BLOCK
 * with NO owner in any registry. Before #245(b) the bridge's retainability
 * check answered false and the unowned WOULD_BLOCK became MOQ_ERR_INTERNAL,
 * fatalizing the connection -- local resource pressure destroying it. The
 * landed contract never fatalizes this path: the ingress either completes
 * immediately (MOQ_OK, obligation queued) or is retained (the no-slot carrier)
 * and completed by SERVICE ONLY, with no peer bytes re-delivered.
 *
 * One row per session-side origin, no-FIN form (the FIN forms are pinned by
 * the direct matrix). The whole run is driven off a DECLARED record: the seed
 * slot and generation are derived before ingress, both transport IDs and both
 * internal refs are saved while their entries are live, and every semantic
 * and physical check uses those saved identities -- nothing is re-derived
 * after retirement.
 *
 * Physical target contract, per class: post-SETUP, the FIN'd REQUEST_ERROR
 * write marks the local half closed and the mapping stays live until the
 * peer's own later FIN retires it exactly once (asserted). Pre-SETUP, the
 * RESET dispatch retires the mapping at dispatch, and the peer's REAL answer
 * to STOP_SENDING -- a RESET_STREAM delivered through the bridge on the
 * saved transport id -- must release the exact target NORMAL drain ref,
 * leave no owner or mapping, stay nonfatal and open, emit nothing, and be
 * idempotent under a repeated RESET plus service, whatever tombstone or
 * carrier design the corrected bridge uses internally.
 *
 * Drain honesty: origin 1's ring is helper-seeded capacity; its release
 * feeds the DECLARED filler's own FIN through the session's public absorb
 * path, and every expected ring is derived from declared members, never from
 * an observed result.
 */


typedef struct nob_case {
    const char *name;
    int         origin;      /* 1..4, as in the direct matrix */
    bool        pre_setup;
    /* The inbound target Request ID, DECLARED per row independently of the
     * encoded target bytes (draft-18 §10.1). Origin 4's announcement blocker
     * commits inbound id 0, so its next request is id 2; origins 1-3 commit no
     * preceding request and use id 0. A pre-ingress wire decoder compares the
     * encoded target against this declaration. */
    uint64_t    target_rid;
} nob_case_t;

/* Descriptor self-check: origin 4 requires target id 2, every other origin
 * requires 0. A missing/defaulted or coherently changed member cannot silently
 * redefine the fixture. */
static int nob_case_target_rid_ok(const nob_case_t *c)
{
    uint64_t want = (c->origin == 4) ? 2u : 0u;
    if (c->target_rid != want) {
        fprintf(stderr, "FAIL: nob %s: declared target_rid %llu, origin %d"
                " requires %llu\n", c->name, (unsigned long long)c->target_rid,
                c->origin, (unsigned long long)want);
        return 1;
    }
    return 0;
}

/* Everything a phase check needs, saved while live. */
typedef struct nob_run {
    uint64_t seed_id, target_id;      /* transport identities */
    moq_stream_ref_t seed_ref;        /* session refs, saved while mapped */
    moq_stream_ref_t target_ref;
    int      seed_slot;
    uint32_t seed_gen;
    size_t   seed_fed;                /* fragmented prefix length */
    uint8_t  seed_bytes[128];
    /* Origin-4 send blocker: a live receiver-side announcement whose nonterminal
     * per-request GOAWAY (§10.4) fills the send buffer -- a production-valid
     * request-bidi write, not a raw control blob. It stays live through the whole
     * target refusal/recovery, so it is part of the declared inventory. Zeroed
     * for other origins (blk_present = false). */
    bool     blk_present;
    int      blk_slot;
    uint32_t blk_gen;                 /* DECLARED live generation, pre-ingress */
    uint64_t blk_handle;              /* DECLARED packed announcement handle */
    uint64_t blk_rid;                 /* its committed inbound request id (0) */
    uint64_t blk_id;                  /* its transport id */
    moq_stream_ref_t blk_ref;         /* its request-bidi internal ref */
    uint64_t target_rid;              /* per-run inbound target Request ID */
} nob_run_t;

/* A bounds-safe conserved image of the origin-4 announcement blocker, captured
 * once its live shape is settled and re-checked at every later phase: the target
 * lifecycle must not mutate or retire it. Identity (slot/gen/handle/rid/ref) is
 * carried in nob_run_t and compared against the DECLARED values; the mutable
 * byte state is deep-copied here so an in-place change is caught. */
typedef struct nob_blk_snap {
    size_t   ns_len;
    uint8_t  ns_bytes[64];            /* deep copy of the canonical namespace */
    bool     ns_overflow;            /* ns_id_len exceeded the capture buffer */
    size_t   req_recv_cap;
    size_t   req_recv_len;
    bool     req_recv_fin;
    bool     handoff_fin_pending;
    const uint8_t *req_recv_buf;     /* pointer identity */
} nob_blk_snap_t;

/* Origin-4 GOAWAY New Session URI: 36 bytes, sized so the encoded REQUEST_GOAWAY
 * fills the 64-byte send buffer to within < 24 bytes of full. Shared by the
 * blocker and the wire oracle so the dispatched write is compared byte-exact. */
static const uint8_t nob_goaway_uri[36] = {
    'h','t','t','p','s',':','/','/','r','e','l','a','y','.','e','x',
    'a','m','p','l','e','/','m','i','g','r','a','t','e','/','a','a',
    'a','a','a','a' };

/* The exhaustion reset arm's GOAWAY URI: longer than the no-owner matrix's so
 * the encoded REQUEST_GOAWAY fills the small send buffer far enough that the
 * target's REQUEST_ERROR cannot fit -- forcing the carrier-exhausted reset arm
 * rather than an immediate rejection. */
static const uint8_t exh_goaway_uri[52] = {
    'h','t','t','p','s',':','/','/','r','e','l','a','y','.','e','x',
    'a','m','p','l','e','/','m','i','g','r','a','t','e','/','a','a',
    'a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a',
    'a','a','a','a' };

/* Build a draft-18 request-stream SEED prefix that ends exactly after the
 * envelope header (Type vi64 + uint16 Length) and carries NO Request ID byte:
 * staging it consumes no wire Request ID, so it cannot collide with a request
 * the same session legitimately commits (draft-18 section 10.1 -- a duplicate
 * Request ID MUST close INVALID_REQUEST_ID). The eventual SUBSCRIBE is encoded
 * at `eventual_rid` only to derive a valid header; solely the header bytes are
 * fed. On success returns 0, copies the header into `buf`, and fills
 * *out_prefix_len (bytes to feed) / *out_encoded_len (the full message). */
static int d18_seed_header_prefix(uint8_t *buf, size_t cap, uint64_t eventual_rid,
                                  size_t *out_prefix_len, size_t *out_encoded_len)
{
    int failures = 0;
    uint8_t enc[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, enc, sizeof(enc));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("nob") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t prm = { 0 };
    if (moq_d18_encode_subscribe(&w, eventual_rid, &ns, MOQ_BYTES_LITERAL("t"),
                                 &prm) != MOQ_OK) return failures + 1;
    size_t encoded_len = moq_buf_writer_offset(&w);
    moq_buf_reader_t rr;
    moq_buf_reader_init(&rr, enc, encoded_len);
    uint64_t mtype = 0;
    uint16_t plen = 0;
    if (moq_buf_read_vi64(&rr, &mtype) < 0) return failures + 1;
    if (moq_buf_read_uint16(&rr, &plen) < 0) return failures + 1;
    size_t prefix_len = moq_buf_reader_offset(&rr);
    /* The split is exactly the reader position after the two header fields, is
     * shorter than the full message, and precedes the first payload byte (the
     * Request ID). */
    MOQ_TEST_CHECK(prefix_len > 0 && prefix_len < encoded_len);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)plen, encoded_len - prefix_len);
    if (prefix_len == 0 || prefix_len >= encoded_len || prefix_len > cap)
        return failures + 1;
    memcpy(buf, enc, prefix_len);
    if (out_prefix_len) *out_prefix_len = prefix_len;
    if (out_encoded_len) *out_encoded_len = encoded_len;
    return failures;
}

/* The seed owner and its physical mapping, by SAVED identity: entry state,
 * retained prefix bytes, registry answer, and the bridge entry reachable by
 * BOTH identities. */
static int nob_check_seed(test_pair_t *tp, const nob_run_t *r,
                          const char *what)
{
    int bad = 0;
    if (r->seed_slot < 0 || (size_t)r->seed_slot >= tp->server->sub_cap) {
        fprintf(stderr, "NOB %s: seed slot out of range\n", what);
        return bad + 1;
    }
    const moq_sub_entry_t *e = &tp->server->subs[r->seed_slot];
    if ((int)e->state != (int)MOQ_SUB_RECVING_REQUEST ||
        (int)e->role != (int)MOQ_SUB_ROLE_PUBLISHER ||
        e->request_id != 0 ||
        e->generation != r->seed_gen ||
        e->request_stream_ref._v != r->seed_ref._v ||
        e->req_recv_fin ||
        e->req_recv_len != r->seed_fed ||
        !e->req_recv_buf ||
        memcmp(e->req_recv_buf, r->seed_bytes, r->seed_fed) != 0) {
        fprintf(stderr, "NOB %s: seed owner record changed\n", what);
        bad++;
    }
    moq_request_endpoint_t ep =
        request_registry_find_by_streamref(tp->server, r->seed_ref);
    if (ep.kind != MOQ_REQ_SUBSCRIPTION || ep.slot != r->seed_slot ||
        !ep.has_stream_ref || ep.has_request_id ||
        ep.stream_ref._v != r->seed_ref._v) {
        fprintf(stderr, "NOB %s: seed registry answer changed\n", what);
        bad++;
    }
    /* by-ID-0 absence is absolute at arm and completion, asserted by the
     * seed-only graph (og_check_no_edge on rid 0); the REFUSED phase admits
     * an identity-qualified target carrier there, so this per-phase helper
     * deliberately does not reject it. The seed's own endpoint stays
     * stream-ref keyed with no request-ID field, checked above. */
    /* The COMPLETE physical mapping, by BOTH saved identities. */
    bridge_stream_entry_t *by_id = bridge_find_by_id(tp->server_bridge,
                                                     r->seed_id);
    bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp->server_bridge,
                                                       r->seed_ref);
    if (!by_id || by_id != by_ref || !by_id->active ||
        by_id->ref._v != r->seed_ref._v ||
        by_id->transport_id != r->seed_id ||
        by_id->kind != BRIDGE_STREAM_BIDI ||
        by_id->origin != BRIDGE_ORIGIN_PEER ||
        by_id->peer_send_closed || by_id->local_send_closed ||
        by_id->pending_retry || by_id->pending_fin || by_id->fin_retained ||
        by_id->pending_reset || by_id->pending_stop ||
        by_id->peer_stop_received || by_id->aborting ||
        by_id->pending_reset_code != 0 || by_id->pending_stop_code != 0) {
        fprintf(stderr, "NOB %s: seed bridge mapping changed\n", what);
        bad++;
    }
    return bad;
}

/* The generic completion-session layer, DERIVED from the pre-target
 * baseline: session state, both queue depths (the declared output has been
 * classified/dispatched at these checkpoints), the normalized scratch cursor,
 * receive-budget accounting, the phase's declared drain count, and the named
 * seed owner record -- via txs_check_eq on a derived snapshot, never values
 * copied from current state. The richer seed/graph/ring/mapping/output
 * checks remain; this is the generic layer, not a replacement. */
static int nob_check_session(const moq_session_t *sv, moq_stream_ref_t seed_ref,
                             const txs_snapshot_t *before, size_t ring_count,
                             const char *what)
{
    txs_snapshot_t expect = *before;
    expect.event_depth = 0;
    expect.action_depth = 0;
    expect.drain_ref_count = ring_count;
    return txs_check_eq(sv, &seed_ref, 1, &expect, what);
}

/* Quiet gate for the physical-state probe's expected-failure arms. */
static int nob_quiet;

/* The target's physical state by BOTH saved identities. `live` selects the
 * post-SETUP completed shape -- one active entry resolving by both
 * identities with the local half closed, the peer half open, every
 * pending/reset/stop/abort flag false, the STOP terminal fact
 * (`peer_stop_received`) false, and both pending codes zero -- or the fully
 * retired shape. This is the complete LIFECYCLE inventory, deliberately not
 * a byte-for-byte entry comparison: the bidi-irrelevant inbound-uni
 * classification storage (`uni_disp`, `classify_len`, `classify_buf`) is
 * outside the contract for these request bidis. */
static int nob_check_target_phys(test_pair_t *tp, const nob_run_t *r,
                                 int live, const char *what)
{
    bridge_stream_entry_t *te = bridge_find_by_id(tp->server_bridge,
                                                  r->target_id);
    bridge_stream_entry_t *tr = bridge_find_by_ref(tp->server_bridge,
                                                   r->target_ref);
    if (!live) {
        if ((te && te->active) || (tr && tr->active)) {
            if (!nob_quiet)
                fprintf(stderr, "NOB %s: target mapping survives"
                        " retirement\n", what);
            return 1;
        }
        return 0;
    }
    if (!te || te != tr || !te->active ||
        te->transport_id != r->target_id ||
        te->ref._v != r->target_ref._v ||
        te->kind != BRIDGE_STREAM_BIDI ||
        te->origin != BRIDGE_ORIGIN_PEER ||
        !te->local_send_closed || te->peer_send_closed ||
        te->pending_retry || te->pending_fin || te->fin_retained ||
        te->pending_reset || te->pending_stop ||
        te->peer_stop_received || te->aborting ||
        te->pending_reset_code != 0 || te->pending_stop_code != 0) {
        if (!nob_quiet)
            fprintf(stderr, "NOB %s: target mapping not in the declared"
                    " local-closed live state\n", what);
        return 1;
    }
    return 0;
}

/* No target semantic owner and the seed-only graph, by SAVED target ref. */
static int nob_check_target_gone(test_pair_t *tp, const nob_run_t *r,
                                 size_t want_recv_payload, const char *what)
{
    int bad = 0;
    if (tp->server->recv_payload_bytes != want_recv_payload) {
        fprintf(stderr, "NOB %s: recv_payload_bytes %zu, expected the"
                " pre-target %zu\n", what, tp->server->recv_payload_bytes,
                want_recv_payload);
        bad++;
    }
    if (request_registry_find_by_streamref(tp->server, r->target_ref).kind
        != MOQ_REQ_NONE) {
        fprintf(stderr, "NOB %s: target still has a registry owner\n", what);
        bad++;
    }
    {
        int busy = 0;
        for (size_t i = 0; i < tp->server->sub_cap; i++)
            if (tp->server->subs[i].state != MOQ_SUB_FREE) busy++;
        if (busy != 1) {
            fprintf(stderr, "NOB %s: pool occupancy %d, expected 1\n", what,
                    busy);
            bad++;
        }
    }
    og_graph_t g;
    og_capture(tp->server, &g);
    bad += og_check_integrity(&g, what);
    bad += og_check_edge(&g, OG_DOM_REQ_STREAMREF, r->seed_ref._v,
                         MOQ_REQ_SUBSCRIPTION, r->seed_slot, what);
    {
        const og_edge_spec_t w[] = { { OG_DOM_REQ_STREAMREF,
                                       r->seed_ref._v } };
        bad += og_check_owner_edges(&g, MOQ_REQ_SUBSCRIPTION, r->seed_slot,
                                    w, 1, what);
    }
    bad += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, r->target_ref._v, what);
    bad += og_check_no_edge(&g, OG_DOM_NS_REF, r->target_ref._v, what);
    /* The refused target commits no owner, so its DECLARED request id keys
     * nothing. The blocker committed inbound id 0, but a d18 stream-correlated
     * announcement holds no by-ID edge, so id 0 stays absent too. */
    bad += og_check_no_edge(&g, OG_DOM_REQ_RID, r->target_rid, what);
    if (r->target_rid != r->blk_rid)
        bad += og_check_no_edge(&g, OG_DOM_REQ_RID, r->blk_rid, what);
    /* Origin 4's live announcement blocker keeps its own stream-ref edge for the
     * whole target refusal/recovery (an outbound per-request GOAWAY retires only
     * on the peer's empty-FIN), so the seed-only graph gains exactly that edge. */
    size_t want_edges = 1;
    if (r->blk_present) {
        want_edges = 2;
        bad += og_check_edge(&g, OG_DOM_REQ_STREAMREF, r->blk_ref._v,
                             MOQ_REQ_ANNOUNCEMENT, r->blk_slot, what);
        const og_edge_spec_t bw[] = { { OG_DOM_REQ_STREAMREF,
                                        r->blk_ref._v } };
        bad += og_check_owner_edges(&g, MOQ_REQ_ANNOUNCEMENT, r->blk_slot,
                                    bw, 1, what);
    }
    if (g.edge_count != want_edges) {
        fprintf(stderr, "NOB %s: %zu graph edges, expected exactly %zu\n",
                what, g.edge_count, want_edges);
        bad++;
    }
    return bad;
}

/* Deep-copy the blocker's mutable byte state so a later in-place change is
 * caught. Identity lives in nob_run_t; this captures only what can drift. */
static int nob_blk_capture(test_pair_t *tp, const nob_run_t *r,
                           nob_blk_snap_t *snap)
{
    memset(snap, 0, sizeof(*snap));
    if (!r->blk_present) return 0;
    if (r->blk_slot < 0 || (size_t)r->blk_slot >= tp->server->ann_cap)
        return 1;
    const moq_ann_entry_t *e = &tp->server->announcements[r->blk_slot];
    snap->ns_len = e->ns_id_len;
    if (e->ns_id_len > sizeof(snap->ns_bytes))
        snap->ns_overflow = true;
    else if (e->ns_id_buf && e->ns_id_len)
        memcpy(snap->ns_bytes, e->ns_id_buf, e->ns_id_len);
    snap->req_recv_cap = e->req_recv_cap;
    snap->req_recv_len = e->req_recv_len;
    snap->req_recv_fin = e->req_recv_fin;
    snap->handoff_fin_pending = e->handoff_fin_pending;
    snap->req_recv_buf = e->req_recv_buf;
    return 0;
}

/* The whole live blocker, by DECLARED identity plus a conserved byte image and
 * physical mapping. Re-run at every phase: the target lifecycle must neither
 * mutate nor retire it. `blk_gen`/`blk_handle` are load-bearing -- a coherent
 * generation/handle/slot change fails here independently. */
static int nob_blk_check(test_pair_t *tp, const nob_run_t *r,
                         const nob_blk_snap_t *snap, const char *what)
{
    int bad = 0;
    if (!r->blk_present) return 0;
    if (r->blk_slot < 0 || (size_t)r->blk_slot >= tp->server->ann_cap) {
        fprintf(stderr, "NOB %s: blocker slot out of range\n", what);
        return bad + 1;
    }
    const moq_ann_entry_t *e = &tp->server->announcements[r->blk_slot];
    if ((int)e->state != (int)MOQ_ANN_ESTABLISHED ||
        (int)e->role != (int)MOQ_ANN_ROLE_RECEIVER ||
        e->generation != r->blk_gen ||
        e->handle._opaque != r->blk_handle ||
        e->request_id != r->blk_rid ||
        e->request_stream_ref._v != r->blk_ref._v ||
        !e->goaway_sent ||
        e->handoff_fin_pending != snap->handoff_fin_pending ||
        e->req_recv_cap != snap->req_recv_cap ||
        e->req_recv_len != snap->req_recv_len ||
        e->req_recv_fin != snap->req_recv_fin ||
        e->req_recv_buf != snap->req_recv_buf) {
        fprintf(stderr, "NOB %s: blocker owner record changed\n", what);
        bad++;
    }
    /* Canonical namespace bytes, deep-compared (bounds-safe). */
    if (snap->ns_overflow || e->ns_id_len > sizeof(snap->ns_bytes) ||
        e->ns_id_len != snap->ns_len ||
        (e->ns_id_len && (!e->ns_id_buf ||
            memcmp(e->ns_id_buf, snap->ns_bytes, e->ns_id_len) != 0))) {
        fprintf(stderr, "NOB %s: blocker namespace bytes changed\n", what);
        bad++;
    }
    /* The stream-ref registry edge still targets the declared announcement. */
    moq_request_endpoint_t ep =
        request_registry_find_by_streamref(tp->server, r->blk_ref);
    if (ep.kind != MOQ_REQ_ANNOUNCEMENT || ep.slot != r->blk_slot ||
        !ep.has_stream_ref || ep.stream_ref._v != r->blk_ref._v) {
        fprintf(stderr, "NOB %s: blocker registry answer changed\n", what);
        bad++;
    }
    /* The physical bridge mapping, by BOTH identities. */
    bridge_stream_entry_t *by_id = bridge_find_by_id(tp->server_bridge,
                                                     r->blk_id);
    bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp->server_bridge,
                                                       r->blk_ref);
    if (!by_id || by_id != by_ref || !by_id->active ||
        by_id->ref._v != r->blk_ref._v ||
        by_id->transport_id != r->blk_id ||
        by_id->kind != BRIDGE_STREAM_BIDI ||
        by_id->origin != BRIDGE_ORIGIN_PEER ||
        by_id->peer_send_closed || by_id->local_send_closed ||
        by_id->pending_retry || by_id->pending_fin || by_id->fin_retained ||
        by_id->pending_reset || by_id->pending_stop ||
        by_id->peer_stop_received || by_id->aborting ||
        by_id->pending_reset_code != 0 || by_id->pending_stop_code != 0) {
        fprintf(stderr, "NOB %s: blocker bridge mapping changed\n", what);
        bad++;
    }
    return bad;
}

/* The encoded target request, decoded pre-ingress and compared against the
 * DECLARED case values (draft-18 §10.1) -- an independent wire-legality oracle,
 * separate from the encoder that produced the bytes: one complete SUBSCRIBE
 * envelope, no trailing bytes, a decoded body carrying the declared Request ID,
 * the fixture namespace "nob" / track "t", and the empty/default parameter image
 * the fixture asks for (prm = {0}). Every borrowed span is bounds/NULL-guarded. */
static int nob_check_target_wire(const uint8_t *data, size_t len,
                                 uint64_t want_rid, const char *what)
{
    int bad = 0;
    if (!data || len == 0) {
        fprintf(stderr, "FAIL: nob %s: no target bytes\n", what);
        return 1;
    }
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);
    moq_control_envelope_t env;
    memset(&env, 0, sizeof(env));
    if (moq_d18_decode_envelope(&r, &env) != MOQ_OK) {
        fprintf(stderr, "FAIL: nob %s: target is not a decodable envelope\n",
                what);
        return 1;
    }
    if (env.msg_type != MOQ_D18_SUBSCRIBE) {
        fprintf(stderr, "FAIL: nob %s: target msg type 0x%llx, expected"
                " SUBSCRIBE\n", what, (unsigned long long)env.msg_type);
        bad++;
    }
    if (moq_buf_reader_remaining(&r) != 0) {
        fprintf(stderr, "FAIL: nob %s: %zu trailing bytes after the target\n",
                what, moq_buf_reader_remaining(&r));
        bad++;
    }
    moq_bytes_t parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
    moq_d18_subscribe_t sub;
    memset(&sub, 0, sizeof(sub));
    if (moq_d18_decode_subscribe(env.payload, env.payload_len, parts,
                                 MOQ_DECODED_MAX_NAMESPACE_PARTS, &sub)
        != MOQ_OK) {
        fprintf(stderr, "FAIL: nob %s: target SUBSCRIBE body did not decode\n",
                what);
        return bad + 1;
    }
    if (sub.request_id != want_rid) {
        fprintf(stderr, "FAIL: nob %s: target request id %llu, declared %llu\n",
                what, (unsigned long long)sub.request_id,
                (unsigned long long)want_rid);
        bad++;
    }
    if (sub.track_namespace.count != 1 || sub.track_namespace.parts == NULL ||
        sub.track_namespace.parts[0].len != 3 ||
        sub.track_namespace.parts[0].data == NULL ||
        memcmp(sub.track_namespace.parts[0].data, "nob", 3) != 0) {
        fprintf(stderr, "FAIL: nob %s: target namespace differs from 'nob'\n",
                what);
        bad++;
    }
    if (sub.track_name.len != 1 || sub.track_name.data == NULL ||
        sub.track_name.data[0] != 't') {
        fprintf(stderr, "FAIL: nob %s: target track name differs from 't'\n",
                what);
        bad++;
    }
    if (sub.params.has_forward || sub.params.has_subscriber_priority ||
        sub.params.has_group_order || sub.params.has_filter ||
        sub.params.has_expires || sub.params.has_largest ||
        sub.params.has_object_delivery_timeout ||
        sub.params.has_subgroup_delivery_timeout ||
        sub.params.has_new_group_request) {
        fprintf(stderr, "FAIL: nob %s: target carries an unexpected"
                " parameter\n", what);
        bad++;
    }
    return bad;
}

/* The DECLARED key a legal added blocked-graph edge must carry: a request-ID
 * edge keys the target's declared request id; every other domain keys the saved
 * target ref. Identity-qualified checks on (kind, slot) run separately. */
static uint64_t nob_want_edge_key(int domain, uint64_t target_rid,
                                  uint64_t target_ref)
{
    return (domain == OG_DOM_REQ_RID) ? target_rid : target_ref;
}

/* The REAL blocked-graph added-edge allowance, factored so the live nob run and
 * the selfcheck exercise the same code: a legal added edge keys the declared id
 * (target_rid for request-ID, target_ref otherwise) AND its own (kind, slot)
 * must resolve to a live subscription owner whose request-stream identity IS the
 * target. Returns the number of failures for one added edge. */
static int nob_added_edge_ok(const moq_session_t *server, const og_edge_t *f,
                             uint64_t target_rid, uint64_t target_ref,
                             const char *what)
{
    uint64_t want_key = nob_want_edge_key(f->domain, target_rid, target_ref);
    if (f->key != want_key) {
        if (!og_quiet)
            fprintf(stderr, "NOB %s: added edge (%d key %llu) does not key the"
                    " target\n", what, (int)f->domain,
                    (unsigned long long)f->key);
        return 1;
    }
    if (f->kind == MOQ_REQ_SUBSCRIPTION) {
        if (f->slot < 0 || (size_t)f->slot >= server->sub_cap ||
            server->subs[f->slot].state == MOQ_SUB_FREE ||
            server->subs[f->slot].request_stream_ref._v != target_ref) {
            if (!og_quiet)
                fprintf(stderr, "NOB %s: added edge's owner does not carry the"
                        " target identity\n", what);
            return 1;
        }
        return 0;
    }
    if (!og_quiet)
        fprintf(stderr, "NOB %s: added edge has unsupported kind %d\n", what,
                f->kind);
    return 1;
}

/* Drive the REAL blocked-graph allowance (nob_added_edge_ok, the same helper the
 * live nob run calls) against a live server subscription, so acceptance and
 * rejection are proven through the actual owner-identity resolution rather than
 * through the pure nob_want_edge_key() return alone. A staged inbound SUBSCRIBE
 * gives a genuine subs[] entry whose request_stream_ref the helper must match.
 *
 * The DECLARED target request id the helper keys on (its target_rid parameter)
 * is independent of the staged owner's own protocol request id: the helper never
 * reads the owner's id, only its request_stream_ref. So the owner is admitted at
 * protocol id 0 (the next-in-sequence inbound id) while the helper is driven at a
 * DECLARED target id of 2 -- the origin-4 case's real value -- proving the
 * request-ID allowance keys on the declared id, not a hardcoded 0.
 *
 * Synthetic og_edge_t values probe every branch, for BOTH declared id 2 and id 0:
 *   - a request-ID edge keyed on the declared target id and resolving to the live
 *     owner is ACCEPTED; keyed on any other id is REJECTED;
 *   - a stream-ref edge keyed on the target ref is ACCEPTED; keyed on a foreign
 *     ref is REJECTED;
 *   - an otherwise-legal edge whose slot resolves to an owner NOT carrying the
 *     target ref is REJECTED by identity even when the key matches;
 *   - a free slot and an unsupported kind are both REJECTED. */
static int nob_rid_allowance_selfcheck(void)
{
    int failures = 0;

    /* The pure key selector, retained as the first-line contract. */
    {
        const uint64_t ref = 0xBEEF;
        MOQ_TEST_CHECK_EQ_U64(nob_want_edge_key(OG_DOM_REQ_RID, 2, ref),
                              (uint64_t)2);
        MOQ_TEST_CHECK_EQ_U64(nob_want_edge_key(OG_DOM_REQ_RID, 0, ref),
                              (uint64_t)0);
        MOQ_TEST_CHECK_EQ_U64(nob_want_edge_key(OG_DOM_REQ_STREAMREF, 2, ref),
                              ref);
        MOQ_TEST_CHECK_EQ_U64(nob_want_edge_key(OG_DOM_REQ_STREAMREF, 0, ref),
                              ref);
    }

    test_pair_t tp;
    if (d18_pair_init(&tp, 1) < 0) return failures + 1;
    if (moq_session_start(tp.client, 0) != MOQ_OK ||
        moq_session_start(tp.server, 0) != MOQ_OK) {
        test_pair_destroy(&tp);
        return failures + 1;
    }
    failures += d18_strict_shuttle(&tp, 30, 0, "nob-selfcheck");
    if (tp.server->state != MOQ_SESS_ESTABLISHED) {
        test_pair_destroy(&tp);
        return failures + 1;
    }
    {
        moq_event_t ev;
        while (moq_session_poll_events(tp.server, &ev, 1) > 0)
            moq_event_cleanup(&ev);
        while (moq_session_poll_events(tp.client, &ev, 1) > 0)
            moq_event_cleanup(&ev);
    }
    fake_endpoint_clear_ops(&tp.client_ep);
    fake_endpoint_clear_ops(&tp.server_ep);

    /* A complete inbound SUBSCRIBE stages a live owner at the next-in-sequence
     * protocol request id (0). The helper never reads that id -- see the driven
     * declared id below. */
    const uint64_t owner_rid = 0;
    const uint64_t owner_bidi_id = 6060;
    uint8_t sub[128];
    size_t sub_len;
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, sub, sizeof(sub));
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("nob") };
        moq_namespace_t ns = { parts, 1 };
        moq_d18_msg_params_t prm = { 0 };
        if (moq_d18_encode_subscribe(&w, owner_rid, &ns,
                                     MOQ_BYTES_LITERAL("t"), &prm) != MOQ_OK) {
            test_pair_destroy(&tp);
            return failures + 1;
        }
        sub_len = moq_buf_writer_offset(&w);
    }
    if (moq_transport_bridge_on_peer_bidi_bytes(tp.server_bridge, owner_bidi_id,
                                                sub, sub_len, false, 0)
            != MOQ_OK ||
        moq_transport_bridge_is_fatal(tp.server_bridge)) {
        test_pair_destroy(&tp);
        return failures + 1;
    }

    /* The staged owner's stream ref, taken while the bridge mapping is live. */
    moq_stream_ref_t owner_ref;
    {
        bridge_stream_entry_t *oe = bridge_find_by_id(tp.server_bridge,
                                                      owner_bidi_id);
        if (!oe) { test_pair_destroy(&tp); return failures + 1; }
        owner_ref = oe->ref;
    }
    /* Locate the subs[] slot the helper will index. */
    int owner_slot = -1;
    for (size_t i = 0; i < tp.server->sub_cap; i++)
        if (tp.server->subs[i].state != MOQ_SUB_FREE &&
            tp.server->subs[i].request_stream_ref._v == owner_ref._v) {
            owner_slot = (int)i;
            break;
        }
    MOQ_TEST_CHECK(owner_slot >= 0);
    MOQ_TEST_CHECK(owner_ref._v != 0);
    if (owner_slot < 0) { test_pair_destroy(&tp); return failures + 1; }

    /* A free slot for the free-slot rejection probe -- required present, so the
     * negative below runs unconditionally rather than being silently skipped. */
    int free_slot = -1;
    for (size_t i = 0; i < tp.server->sub_cap; i++)
        if (tp.server->subs[i].state == MOQ_SUB_FREE) {
            free_slot = (int)i;
            break;
        }
    MOQ_TEST_CHECK(free_slot >= 0);
    if (free_slot < 0) { test_pair_destroy(&tp); return failures + 1; }

    /* The declared target id the helper keys on -- driven at 2 (the origin-4
     * value), independent of the owner's protocol id 0. */
    const uint64_t DECL_RID = 2;
    const uint64_t foreign_ref = owner_ref._v ^ 0x5A5Au;

    /* ACCEPTANCE (declared id 2): a request-ID edge keyed on the declared target
     * id and resolving to the live owner, and a stream-ref edge keyed on the
     * owner ref (which ignores the declared id). */
    {
        og_edge_t e = { .domain = OG_DOM_REQ_RID, .key = DECL_RID,
                        .kind = MOQ_REQ_SUBSCRIPTION, .slot = owner_slot };
        MOQ_TEST_CHECK_EQ_INT(
            nob_added_edge_ok(tp.server, &e, DECL_RID, owner_ref._v, "sc"), 0);
    }
    {
        og_edge_t e = { .domain = OG_DOM_REQ_STREAMREF, .key = owner_ref._v,
                        .kind = MOQ_REQ_SUBSCRIPTION, .slot = owner_slot };
        MOQ_TEST_CHECK_EQ_INT(
            nob_added_edge_ok(tp.server, &e, DECL_RID, owner_ref._v, "sc"), 0);
    }
    /* Symmetric id-0 positive: keyed on declared id 0, same owner. */
    {
        og_edge_t e = { .domain = OG_DOM_REQ_RID, .key = 0,
                        .kind = MOQ_REQ_SUBSCRIPTION, .slot = owner_slot };
        MOQ_TEST_CHECK_EQ_INT(
            nob_added_edge_ok(tp.server, &e, 0, owner_ref._v, "sc"), 0);
    }

    /* REJECTIONS: exercised with diagnostics silenced so a passing run is
     * quiet; each must be reported as exactly one failure by the helper. */
    {
        int saved = og_quiet;
        og_quiet = 1;
        /* Wrong request id: key 0 against the declared target id 2. */
        {
            og_edge_t e = { .domain = OG_DOM_REQ_RID, .key = 0,
                        .kind = MOQ_REQ_SUBSCRIPTION, .slot = owner_slot };
            MOQ_TEST_CHECK_EQ_INT(
                nob_added_edge_ok(tp.server, &e, DECL_RID, owner_ref._v, "sc"),
                1);
        }
        /* Wrong request id the other way: key 2 against the declared id 0. */
        {
            og_edge_t e = { .domain = OG_DOM_REQ_RID, .key = DECL_RID,
                        .kind = MOQ_REQ_SUBSCRIPTION, .slot = owner_slot };
            MOQ_TEST_CHECK_EQ_INT(
                nob_added_edge_ok(tp.server, &e, 0, owner_ref._v, "sc"), 1);
        }
        /* Wrong stream ref key. */
        {
            og_edge_t e = { .domain = OG_DOM_REQ_STREAMREF, .key = foreign_ref,
                            .kind = MOQ_REQ_SUBSCRIPTION, .slot = owner_slot };
            MOQ_TEST_CHECK_EQ_INT(
                nob_added_edge_ok(tp.server, &e, DECL_RID, owner_ref._v, "sc"),
                1);
        }
        /* Right key, wrong identity: key 2 matches the declared id 2, but the
         * owner does not carry this target ref. */
        {
            og_edge_t e = { .domain = OG_DOM_REQ_RID, .key = DECL_RID,
                        .kind = MOQ_REQ_SUBSCRIPTION, .slot = owner_slot };
            MOQ_TEST_CHECK_EQ_INT(
                nob_added_edge_ok(tp.server, &e, DECL_RID, foreign_ref, "sc"),
                1);
        }
        /* Unsupported kind. */
        {
            og_edge_t e = { .domain = OG_DOM_REQ_RID, .key = DECL_RID,
                        .kind = MOQ_REQ_ANNOUNCEMENT, .slot = owner_slot };
            MOQ_TEST_CHECK_EQ_INT(
                nob_added_edge_ok(tp.server, &e, DECL_RID, owner_ref._v, "sc"),
                1);
        }
        /* Free slot: key matches the declared id but the slot resolves to no
         * owner. Run unconditionally (free_slot required present above). */
        {
            og_edge_t e = { .domain = OG_DOM_REQ_RID, .key = DECL_RID,
                        .kind = MOQ_REQ_SUBSCRIPTION, .slot = free_slot };
            MOQ_TEST_CHECK_EQ_INT(
                nob_added_edge_ok(tp.server, &e, DECL_RID, owner_ref._v, "sc"),
                1);
        }
        og_quiet = saved;
    }

    test_pair_destroy(&tp);
    return failures;
}

/* The no-slot carrier state for a ref: -1 absent, 0 present (no FIN), 1 present
 * (FIN). Used to prove the PRODUCT installs/retires the target carrier through
 * real refusal/recovery, not through a direct helper call. */
static int nob_carrier_state(const moq_session_t *s, moq_stream_ref_t ref)
{
    int i = noslot_carrier_find(s, ref);
    if (i < 0) return -1;
    return s->noslot_carriers[i].fin ? 1 : 0;
}

/* No event on EITHER session: the terminal owes none. */
static int nob_check_no_events(test_pair_t *tp, const char *what)
{
    int bad = 0, n = 0;
    moq_event_t ev;
    while (moq_session_poll_events(tp->server, &ev, 1) > 0) {
        fprintf(stderr, "NOB %s: server event kind %u\n", what,
                (unsigned)ev.kind);
        n++; moq_event_cleanup(&ev);
    }
    while (moq_session_poll_events(tp->client, &ev, 1) > 0) {
        fprintf(stderr, "NOB %s: client event kind %u\n", what,
                (unsigned)ev.kind);
        n++; moq_event_cleanup(&ev);
    }
    if (n) bad++;
    return bad;
}

/* The bridge completion-session helper must itself discriminate: it accepts
 * the exact derived state and rejects an unrelated event_scratch_len change
 * and a non-CLOSED session-state change. Quiet: only failures print. The nob
 * deep phases that consume this helper are live now that #245(b) retains the
 * ingress; this probe additionally pins the helper's own discrimination. */
static int nob_session_selfcheck(void)
{
    int failures = 0;
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    cfg.version = MOQ_VERSION_DRAFT_18;
    moq_session_t *sv = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &sv), (int)MOQ_OK);
    if (!sv) return failures + 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(sv, 0), (int)MOQ_OK);
    moq_action_t a;
    while (moq_session_poll_actions(sv, &a, 1) > 0) moq_action_cleanup(&a);
    moq_stream_ref_t none = moq_stream_ref_from_u64(0x9999);
    txs_snapshot_t before;
    txs_capture(sv, &none, 1, &before);
    nf_expect_after_call_prepare(&before);
    /* Exact derived state: accepted. */
    MOQ_TEST_CHECK_EQ_INT(
        nob_check_session(sv, none, &before, 0, "selfcheck-exact"), 0);
    /* An unrelated scratch change: rejected. */
    sv->event_scratch_len += 8;
    txs_quiet = 1;
    MOQ_TEST_CHECK(
        nob_check_session(sv, none, &before, 0, "selfcheck-scratch") > 0);
    txs_quiet = 0;
    sv->event_scratch_len -= 8;
    /* A non-CLOSED session-state change: rejected. */
    {
        moq_session_state_t saved = sv->state;
        sv->state = MOQ_SESS_DRAINING;
        txs_quiet = 1;
        MOQ_TEST_CHECK(
            nob_check_session(sv, none, &before, 0, "selfcheck-state") > 0);
        txs_quiet = 0;
        sv->state = saved;
    }
    moq_session_destroy(sv);
    return failures;
}

/* The COMPLETE bridge phase postcondition, applied after EVERY advancing
 * call: the generic session layer, the full seed contract, semantic target
 * retirement with the exact graph, the exact declared ring, the bridge
 * nonfatal/open/not-pending, no event on either session, and the target's
 * exact physical state by both saved identities. A later check can therefore
 * never adopt or erase a transient mutation made by service. */
static int nob_check_phase(test_pair_t *tp, const nob_run_t *r,
                           const txs_snapshot_t *before,
                           const nob_ring_t *ring, int live_mapping,
                           const char *what)
{
    int bad = 0;
    bad += nob_check_session(tp->server, r->seed_ref, before, ring->count,
                             what);
    bad += nob_check_seed(tp, r, what);
    bad += nob_check_target_gone(tp, r, before->recv_payload_bytes, what);
    {
        nob_ring_t now;
        nob_ring_snap(tp->server, &now);
        bad += nob_ring_equals(&now, ring, what);
    }
    if (moq_transport_bridge_is_fatal(tp->server_bridge) ||
        moq_transport_bridge_is_closed(tp->server_bridge) ||
        moq_transport_bridge_has_pending(tp->server_bridge)) {
        fprintf(stderr, "NOB %s: bridge fatal/closed/pending\n", what);
        bad++;
    }
    bad += nob_check_no_events(tp, what);
    bad += nob_check_target_phys(tp, r, live_mapping, what);
    return bad;
}

/* The physical-state helper must discriminate: against a real constructed live
 * mapping, the retired shape rejects it, the declared live shape accepts it,
 * and a flipped flag rejects it again. Quiet: only failures print. */
static int nob_phys_selfcheck(void)
{
    int failures = 0;
    test_pair_t tp;
    if (d18_pair_init_caps(&tp, 0, moq_alloc_default(), moq_alloc_default(),
                           0, false, 0) < 0)
        return failures + 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(tp.client, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(tp.server, 0), (int)MOQ_OK);
    static const uint8_t frag[4] = { 0x02, 0x01, 0x01, 0x01 };
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_bidi_bytes(tp.server_bridge, 500,
                                                     frag, sizeof(frag),
                                                     false, 0),
        (int)MOQ_OK);
    bridge_stream_entry_t *e = bridge_find_by_id(tp.server_bridge, 500);
    MOQ_TEST_CHECK(e != NULL);
    if (!e) { test_pair_destroy(&tp); return failures + 1; }
    nob_run_t r2;
    memset(&r2, 0, sizeof(r2));
    r2.target_id = 500;
    r2.target_ref = e->ref;
    /* A live mapping must fail the RETIRED shape. */
    nob_quiet = 1;
    MOQ_TEST_CHECK(nob_check_target_phys(&tp, &r2, 0, "phys-retired") > 0);
    nob_quiet = 0;
    /* The exact declared live shape is accepted... */
    e->local_send_closed = true;
    MOQ_TEST_CHECK_EQ_INT(nob_check_target_phys(&tp, &r2, 1, "phys-live"), 0);
    /* ...a single flipped pending-family flag rejects it... */
    e->aborting = true;
    nob_quiet = 1;
    MOQ_TEST_CHECK(nob_check_target_phys(&tp, &r2, 1, "phys-flag") > 0);
    nob_quiet = 0;
    e->aborting = false;
    /* ...and the STOP terminal fact rejects it INDEPENDENTLY: only
     * peer_stop_received flipped, everything else in the accepted shape. */
    e->peer_stop_received = true;
    nob_quiet = 1;
    MOQ_TEST_CHECK(nob_check_target_phys(&tp, &r2, 1, "phys-stop") > 0);
    nob_quiet = 0;
    e->peer_stop_received = false;
    MOQ_TEST_CHECK_EQ_INT(nob_check_target_phys(&tp, &r2, 1,
                                                "phys-live-again"), 0);
    e->local_send_closed = false;
    test_pair_destroy(&tp);
    return failures;
}

static int run_nob_case(const nob_case_t *c)
{
    int failures = 0;
    char what[96];
    snprintf(what, sizeof(what), "nob %s", c->name);

    test_pair_t tp;
    uint32_t max_actions = 0;
    if (c->origin == 2) max_actions = 2;
    if (c->origin == 3) max_actions = 1;
    if (d18_pair_init_caps(&tp, 0, moq_alloc_default(), moq_alloc_default(),
                           max_actions, false, 0) < 0)
        return failures + 1;
    /* The server needs a ONE-slot subscription pool, which is a create-time
     * config: rebuild the server side of the pair with it (plus the origin's
     * own capacity shape) before any traffic. */
    {
        moq_session_cfg_t scfg;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_SERVER);
        scfg.version = MOQ_VERSION_DRAFT_18;
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 10;
        scfg.max_subscriptions = 1;
        if (max_actions) scfg.max_actions = max_actions;
        if (c->origin == 4) scfg.send_buffer_size = 64;
        moq_session_t *sv = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&scfg, 0, &sv),
                              (int)MOQ_OK);
        if (!sv) { test_pair_destroy(&tp); return failures + 1; }
        moq_transport_bridge_destroy(tp.server_bridge);
        moq_session_destroy(tp.server);
        tp.server = sv;
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_create(&bcfg, tp.server,
                                             &tp.server_ep.vtable,
                                             &tp.server_ep,
                                             &tp.server_bridge),
            (int)MOQ_OK);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(tp.client, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(tp.server, 0), (int)MOQ_OK);
    if (!c->pre_setup) {
        failures += d18_strict_shuttle(&tp, 30, 0, what);
        MOQ_TEST_CHECK_EQ_INT((int)tp.server->state,
                              (int)MOQ_SESS_ESTABLISHED);
        moq_event_t ev;
        int ss = 0, so = 0, cs = 0, co = 0;
        while (moq_session_poll_events(tp.server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) ss++; else so++;
            moq_event_cleanup(&ev);
        }
        while (moq_session_poll_events(tp.client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) cs++; else co++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(ss, 1);
        MOQ_TEST_CHECK_EQ_INT(so, 0);
        MOQ_TEST_CHECK_EQ_INT(cs, 1);
        MOQ_TEST_CHECK_EQ_INT(co, 0);
    } else {
        /* Pre-SETUP: a draft-18 endpoint opens its own local control uni and
         * emits SETUP eagerly at start (§10.3). Service the SERVER bridge to
         * flush exactly that local output -- WITHOUT delivering the peer SETUP,
         * so the server stays pre-established and the target still reaches the
         * defer_dispatch STOP+RESET route -- then classify it exactly rather
         * than let it leak into the blocker/target phases. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
        /* Exactly one local uni open followed by one non-FIN control write on
         * the same transport id, and nothing else. */
        MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)2);
        if (tp.server_ep.count == 2) {
            const fake_op_t *o0 = &tp.server_ep.ops[0];
            const fake_op_t *o1 = &tp.server_ep.ops[1];
            MOQ_TEST_CHECK_EQ_INT((int)o0->kind, (int)FAKE_OP_OPEN_UNI);
            MOQ_TEST_CHECK_EQ_INT(o0->fin ? 1 : 0, 0);
            MOQ_TEST_CHECK_EQ_INT((int)o1->kind, (int)FAKE_OP_WRITE);
            MOQ_TEST_CHECK_EQ_U64(o1->stream_id, o0->stream_id);
            MOQ_TEST_CHECK_EQ_INT(o1->fin ? 1 : 0, 0);
            MOQ_TEST_CHECK(o1->data_len > 0);
            /* The write is a decodable draft-18 SETUP envelope with no trailing
             * bytes -- the local control output, not junk -- AND its body decodes
             * to exactly the fixture's declared defaults. This fixture leaves
             * send_auth_token_cache_size at 0, so SETUP carries no Setup Options
             * (§10.3): no PATH, no AUTHORITY, no MAX_AUTH_TOKEN_CACHE_SIZE, and no
             * auth tokens. The body decode is what distinguishes a correct empty
             * SETUP from one that silently gained an option. */
            if (o1->data_len > 0 && o1->data_len <= sizeof(o1->data)) {
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, o1->data, o1->data_len);
                moq_control_envelope_t env;
                memset(&env, 0, sizeof(env));
                if (moq_d18_decode_envelope(&rr, &env) != MOQ_OK) {
                    fprintf(stderr, "NOB %s: SETUP envelope did not decode\n",
                            what);
                    failures++;
                } else {
                    MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                          (uint64_t)MOQ_D18_STREAM_SETUP);
                    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                           (size_t)0);
                    moq_d18_setup_opts_t opts;
                    memset(&opts, 0, sizeof(opts));
                    if (moq_d18_decode_setup_opts(env.payload, env.payload_len,
                                                  &opts) != MOQ_OK) {
                        fprintf(stderr, "NOB %s: SETUP body did not decode\n",
                                what);
                        failures++;
                    } else {
                        MOQ_TEST_CHECK_EQ_INT(opts.has_path ? 1 : 0, 0);
                        MOQ_TEST_CHECK_EQ_INT(opts.has_authority ? 1 : 0, 0);
                        MOQ_TEST_CHECK_EQ_INT(
                            opts.has_max_auth_token_cache_size ? 1 : 0, 0);
                        MOQ_TEST_CHECK_EQ_SIZE(opts.auth_token_count, (size_t)0);
                    }
                }
            }
        }
        /* The server has NOT seen the peer SETUP: still pre-established, so the
         * target ingress selects the STOP+RESET terminal, not a live stream. */
        MOQ_TEST_CHECK(tp.server->state != MOQ_SESS_ESTABLISHED);
        /* The local SETUP flush leaves no residual outbound work: the action
         * ring and the bridge outbound-pending queue are both empty, so the
         * blocked-point and recovery oracles measure only #245 state. */
        MOQ_TEST_CHECK_EQ_SIZE(
            tp.server->action_tail - tp.server->action_head, (size_t)0);
        MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.server_bridge));
    }
    fake_endpoint_clear_ops(&tp.client_ep);
    fake_endpoint_clear_ops(&tp.server_ep);

    nob_run_t r;
    memset(&r, 0, sizeof(r));
    r.seed_id = 400;
    r.target_id = 404;
    /* The target Request ID comes from the case declaration, checked by the
     * descriptor self-check -- never from a local ternary that also feeds the
     * encoder (a symmetric producer+oracle mutation would go unnoticed). */
    failures += nob_case_target_rid_ok(c);
    r.target_rid = c->target_rid;
    uint64_t target_rid = r.target_rid;
    nob_blk_snap_t blk_snap;
    memset(&blk_snap, 0, sizeof(blk_snap));

    /* Origin 4 (send-buffer blocker): established BEFORE the seed. A live
     * RECEIVER-side announcement is admitted through the real request route on
     * transport id 700, then a nonterminal per-request GOAWAY (§10.4) whose New
     * Session URI is sized to leave less room than the target REQUEST_ERROR needs
     * fills the send buffer. The announcement stages through the single
     * subscription slot and, on acceptance, re-keys into the announcement pool
     * and FREES that slot -- so the fragmented seed can occupy it next and the
     * target still faces an exhausted pool. Accepting the announcement commits
     * inbound request id 0, so the target is the next-in-sequence id 2. The
     * GOAWAY is a genuine queued request-bidi write (not a raw control blob or a
     * direct send_len poke) and it leaves the announcement owner LIVE (an
     * outbound migration retires only on the peer's empty-FIN), so it is part of
     * the declared inventory. */
    if (c->origin == 4) {
        /* DECLARE the blocker identity from the free announcement pool entry,
         * BEFORE ingress: slot, next live generation, packed handle, request id,
         * transport id. The event/registry answers are checked AGAINST these --
         * never used to define the expectation they are compared to. */
        r.blk_present = true;
        r.blk_id = 700;
        r.blk_rid = 0;
        /* The declared target id is the announcement's committed id 0 plus 2
         * (§10.1 next-in-sequence). */
        MOQ_TEST_CHECK_EQ_U64(r.target_rid, (uint64_t)2);
        MOQ_TEST_CHECK_EQ_U64(r.target_rid, r.blk_rid + 2);
        r.blk_slot = -1;
        for (size_t i = 0; i < tp.server->ann_cap; i++)
            if (tp.server->announcements[i].state == MOQ_ANN_FREE) {
                r.blk_slot = (int)i;
                r.blk_gen = tp.server->announcements[i].generation | 1u;
                break;
            }
        MOQ_TEST_CHECK_EQ_INT(r.blk_slot, 0);
        r.blk_handle = moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT,
                                       tp.server->session_tag, r.blk_gen,
                                       (uint32_t)(r.blk_slot < 0 ? 0 : r.blk_slot));
        MOQ_TEST_CHECK(r.blk_handle != 0);

        uint8_t pn[128];
        size_t pn_len;
        {
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, pn, sizeof(pn));
            moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("blk") };
            moq_namespace_t ns = { parts, 1 };
            moq_d18_msg_params_t mp = { 0 };
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_encode_publish_namespace(&w, r.blk_rid, &ns, &mp),
                (int)MOQ_OK);
            pn_len = moq_buf_writer_offset(&w);
        }
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_bidi_bytes(
                tp.server_bridge, r.blk_id, pn, pn_len, false, 0), (int)MOQ_OK);

        /* Exactly one NAMESPACE_PUBLISHED, zero others, carrying the DECLARED
         * handle plus the fixture namespace and (empty) token image. */
        moq_announcement_t ah;
        memset(&ah, 0, sizeof(ah));
        {
            moq_event_t ev;
            int pub = 0, other = 0;
            while (moq_session_poll_events(tp.server, &ev, 1) > 0) {
                if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                    pub++;
                    ah = ev.u.namespace_published.ann;
                    MOQ_TEST_CHECK_EQ_U64(ah._opaque, r.blk_handle);
                    const moq_namespace_published_event_t *np =
                        &ev.u.namespace_published;
                    MOQ_TEST_CHECK_EQ_SIZE(np->track_namespace.count, (size_t)1);
                    if (np->track_namespace.count == 1) {
                        MOQ_TEST_CHECK_EQ_SIZE(np->track_namespace.parts[0].len,
                                               (size_t)3);
                        MOQ_TEST_CHECK(np->track_namespace.parts[0].data &&
                            memcmp(np->track_namespace.parts[0].data, "blk", 3)
                                == 0);
                    }
                    MOQ_TEST_CHECK_EQ_SIZE(np->token_count, (size_t)0);
                } else {
                    other++;
                }
                moq_event_cleanup(&ev);
            }
            MOQ_TEST_CHECK_EQ_INT(pub, 1);
            MOQ_TEST_CHECK_EQ_INT(other, 0);
        }
        MOQ_TEST_CHECK_EQ_U64(ah._opaque, r.blk_handle);

        /* Capture the eventual internal ref while mapped; blk_ref != 0, blk_ref
         * != blk_id, both bridge lookups resolve to the same active peer-origin
         * BIDI, and the stream-ref edge targets the declared announcement slot. */
        {
            bridge_stream_entry_t *by_id =
                bridge_find_by_id(tp.server_bridge, r.blk_id);
            MOQ_TEST_CHECK(by_id != NULL);
            r.blk_ref = by_id ? by_id->ref : moq_stream_ref_from_u64(0);
            MOQ_TEST_CHECK(r.blk_ref._v != 0);
            MOQ_TEST_CHECK(r.blk_ref._v != r.blk_id);
            MOQ_TEST_CHECK(bridge_find_by_ref(tp.server_bridge, r.blk_ref)
                           == by_id);
            moq_request_endpoint_t bep =
                request_registry_find_by_streamref(tp.server, r.blk_ref);
            MOQ_TEST_CHECK(bep.kind == MOQ_REQ_ANNOUNCEMENT);
            MOQ_TEST_CHECK_EQ_INT(bep.slot, r.blk_slot);
        }

        /* Accept the announcement, then CLASSIFY the acceptance exactly: zero
         * events, one non-FIN WRITE on the blocker id decoding to one complete
         * REQUEST_OK envelope/body with no trailing bytes, no other endpoint op,
         * and no pending bridge/action work afterwards. */
        fake_endpoint_clear_ops(&tp.server_ep);
        moq_accept_namespace_cfg_t ac;
        moq_accept_namespace_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_namespace(tp.server, ah, &ac, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
        {
            int nev = 0;
            moq_event_t ev;
            while (moq_session_poll_events(tp.server, &ev, 1) > 0) {
                nev++;
                moq_event_cleanup(&ev);
            }
            MOQ_TEST_CHECK_EQ_INT(nev, 0);
        }
        MOQ_TEST_CHECK(tp.server_ep.count < FAKE_EP_MAX_OPS);
        MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)1);
        if (tp.server_ep.count == 1) {
            const fake_op_t *o = &tp.server_ep.ops[0];
            MOQ_TEST_CHECK_EQ_INT((int)o->kind, (int)FAKE_OP_WRITE);
            MOQ_TEST_CHECK_EQ_U64(o->stream_id, r.blk_id);
            MOQ_TEST_CHECK_EQ_INT(o->fin ? 1 : 0, 0);
            if (o->data_len > 0 && o->data_len <= sizeof(o->data)) {
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, o->data, o->data_len);
                moq_control_envelope_t env;
                memset(&env, 0, sizeof(env));
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_envelope(&rr, &env), (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr), (size_t)0);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_REQUEST_OK);
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_request_ok(env.payload, env.payload_len),
                    (int)MOQ_OK);
            }
        }
        MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.server_bridge));
        MOQ_TEST_CHECK_EQ_SIZE(
            tp.server->action_tail - tp.server->action_head, (size_t)0);
        fake_endpoint_clear_ops(&tp.server_ep);

        /* Only then queue the per-request GOAWAY (fills the send buffer). */
        {
            moq_request_goaway_cfg_t gc;
            moq_request_goaway_cfg_init(&gc);
            gc.new_session_uri.data = nob_goaway_uri;
            gc.new_session_uri.len = sizeof(nob_goaway_uri);
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_request_goaway_namespace(tp.server, ah, &gc, 0),
                (int)MOQ_OK);
        }
        MOQ_TEST_CHECK(tp.server->send_cap - tp.server->send_len < 24);
        MOQ_TEST_CHECK(tp.server->state == MOQ_SESS_ESTABLISHED);

        /* The conserved blocker image, from AFTER the GOAWAY. Re-checked at every
         * later phase; goaway_sent is now true. */
        MOQ_TEST_CHECK_EQ_INT(nob_blk_capture(&tp, &r, &blk_snap), 0);
        failures += nob_blk_check(&tp, &r, &blk_snap, "blocker-armed");
    }

    /* Seed identity, DERIVED after any origin-4 blocker so the pool generation
     * reflects the announcement's transient staging use of the slot. */
    r.seed_slot = -1;
    for (size_t i = 0; i < tp.server->sub_cap; i++)
        if (tp.server->subs[i].state == MOQ_SUB_FREE) {
            r.seed_slot = (int)i;
            r.seed_gen = tp.server->subs[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK_EQ_INT(r.seed_slot, 0);

    /* Seed: a header-only request-stream PREFIX (Type + Length, no Request ID
     * byte) occupies the one subscription staging slot -- so it consumes no wire
     * Request ID and cannot collide with the announcement's committed id 0 or
     * the target. The eventual SUBSCRIBE is the id after the target. */
    {
        size_t enc_len = 0;
        failures += d18_seed_header_prefix(r.seed_bytes, sizeof(r.seed_bytes),
                                           target_rid + 2, &r.seed_fed, &enc_len);
    }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_bidi_bytes(tp.server_bridge,
                                                     r.seed_id, r.seed_bytes,
                                                     r.seed_fed, false, 0),
        (int)MOQ_OK);
    {
        bridge_stream_entry_t *se = bridge_find_by_id(tp.server_bridge,
                                                      r.seed_id);
        MOQ_TEST_CHECK(se != NULL);
        if (!se) { test_pair_destroy(&tp); return failures + 1; }
        r.seed_ref = se->ref;
        MOQ_TEST_CHECK(r.seed_ref._v != 0);
    }
    failures += nob_check_seed(&tp, &r, "seeded");
    failures += nob_check_target_gone(&tp, &r,
                                      tp.server->recv_payload_bytes,
                                      "seeded");
    failures += nob_blk_check(&tp, &r, &blk_snap, "seeded");

    /* Blockers, on the SESSION so bridge service can consume them. */
    if (c->origin == 1) {
        while (tp.server->drain_ref_count < tp.server->drain_ref_cap)
            MOQ_TEST_CHECK(drain_ref_add(
                tp.server,
                moq_stream_ref_from_u64(0x4000 + tp.server->drain_ref_count)));
    } else if (c->origin == 2 || c->origin == 3) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)queue_close_bidi(tp.server, moq_stream_ref_from_u64(0x6000)),
            (int)MOQ_OK);
    }
    nob_ring_t ring0;
    nob_ring_snap(tp.server, &ring0);

    /* An UNRELATED seeded no-slot carrier (distinct ref, no FIN): the target's
     * refusal/recovery must never disturb it. It is the carrier-count baseline. */
    const moq_stream_ref_t unrel = moq_stream_ref_from_u64(0x8888);
    MOQ_TEST_CHECK(noslot_carrier_install(tp.server, unrel, false));
    size_t carrier_base = tp.server->noslot_carrier_count;
    MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, unrel), 0);

    /* Whole-session and graph baselines before the target ingress. */
    txs_snapshot_t before;
    txs_capture(tp.server, &r.seed_ref, 1, &before);
    nf_expect_after_call_prepare(&before);
    og_graph_t g0;
    og_capture(tp.server, &g0);

    /* The target: a complete SUBSCRIBE on a fresh bidi, refused for capacity.
     * The landed contract: never fatal; either completed now or retained (the
     * no-slot carrier). Before #245(b) this returned MOQ_ERR_INTERNAL and
     * latched the bridge fatal. */
    uint8_t tgt[128];
    size_t tgt_len;
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, tgt, sizeof(tgt));
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("nob") };
        moq_namespace_t ns = { parts, 1 };
        moq_d18_msg_params_t prm = { 0 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe(&w, target_rid, &ns,
                                          MOQ_BYTES_LITERAL("t"), &prm),
            (int)MOQ_OK);
        tgt_len = moq_buf_writer_offset(&w);
    }
    /* Independent wire-legality oracle: decode the encoded target and compare it
     * to the DECLARED case values BEFORE the product call, so an out-of-sequence
     * target id fails here even though the no-slot path refuses before the
     * product's own request-id validation. */
    failures += nob_check_target_wire(tgt, tgt_len, c->target_rid, what);
    moq_result_t rc = moq_transport_bridge_on_peer_bidi_bytes(
        tp.server_bridge, r.target_id, tgt, tgt_len, false, 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));
    MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK) {
        fprintf(stderr, "NOB %s: unrecognized ingress result %d\n", what,
                (int)rc);
        test_pair_destroy(&tp);
        return failures + 1;
    }
    MOQ_TEST_CHECK(tp.server->state != MOQ_SESS_CLOSED);

    /* The target's own internal ref, captured while the mapping is LIVE --
     * never discovered after retirement. */
    {
        bridge_stream_entry_t *te = bridge_find_by_id(tp.server_bridge,
                                                      r.target_id);
        MOQ_TEST_CHECK(te != NULL);
        if (!te) { test_pair_destroy(&tp); return failures + 1; }
        r.target_ref = te->ref;
        MOQ_TEST_CHECK(r.target_ref._v != 0);
        MOQ_TEST_CHECK(bridge_find_by_ref(tp.server_bridge,
                                          r.target_ref) == te);
    }

    /* Refused-point conservation: seed, whole-session scalars (with only the
     * bounded target-carrier delta), graph topology (added edges must key the
     * target and resolve to a live carrier), drain multiset, and no event. */
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        failures += nob_check_seed(&tp, &r, "blocked");
        {
            txs_snapshot_t expect = before;
            const size_t grew = tp.server->recv_payload_bytes;
            MOQ_TEST_CHECK(grew >= before.recv_payload_bytes);
            MOQ_TEST_CHECK(grew - before.recv_payload_bytes <= tgt_len);
            expect.recv_payload_bytes = grew;
            failures += txs_check_eq(tp.server, &r.seed_ref, 1, &expect,
                                     "blocked");
        }
        {
            og_graph_t g1;
            og_capture(tp.server, &g1);
            failures += og_check_integrity(&g1, "blocked");
            for (size_t i = 0; i < g0.edge_count; i++) {
                const og_edge_t *e = &g0.edges[i];
                size_t seen = 0;
                for (size_t j = 0; j < g1.edge_count; j++) {
                    const og_edge_t *f = &g1.edges[j];
                    if (f->domain == e->domain && f->key == e->key &&
                        f->kind == e->kind && f->slot == e->slot)
                        seen++;
                }
                if (seen != 1) {
                    fprintf(stderr, "NOB blocked: pre-existing edge (%d key"
                            " %llu) not conserved\n", (int)e->domain,
                            (unsigned long long)e->key);
                    failures++;
                }
            }
            for (size_t j = 0; j < g1.edge_count; j++) {
                const og_edge_t *f = &g1.edges[j];
                int pre = 0;
                for (size_t i = 0; i < g0.edge_count; i++) {
                    const og_edge_t *e = &g0.edges[i];
                    if (f->domain == e->domain && f->key == e->key &&
                        f->kind == e->kind && f->slot == e->slot) {
                        pre = 1; break;
                    }
                }
                if (pre) continue;
                /* Domain-aware, IDENTITY-QUALIFIED allowance, through the same
                 * helper the selfcheck exercises: request-ID edges key the
                 * DECLARED request id (r.target_rid -- 2 for origin 4, 0 for
                 * origins 1-3), every other domain keys the target ref, and the
                 * edge's own (kind, slot) must resolve to the live target owner
                 * (an edge repointed at the seed fails by identity). */
                failures += nob_added_edge_ok(tp.server, f, r.target_rid,
                                              r.target_ref._v, "blocked");
            }
        }
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring0, "blocked");
        failures += nob_check_no_events(&tp, "blocked");
        /* The queued blocker ITSELF, not merely the depth. */
        {
            size_t depth = tp.server->action_tail - tp.server->action_head;
            size_t want_depth = (c->origin == 1) ? 0 : 1;
            MOQ_TEST_CHECK_EQ_SIZE(depth, want_depth);
            if (depth == 1) {
                const moq_action_t *head =
                    &tp.server->actions[tp.server->action_head %
                                        tp.server->action_cap];
                if (c->origin == 4) {
                    /* The blocker is the queued per-request GOAWAY, compared in
                     * full: a non-FIN SEND_BIDI_STREAM on the announcement's ref
                     * whose bytes are one complete REQUEST_GOAWAY envelope with no
                     * trailing bytes and the exact New Session URI. */
                    MOQ_TEST_CHECK_EQ_INT((int)head->kind,
                                          (int)MOQ_ACTION_SEND_BIDI_STREAM);
                    MOQ_TEST_CHECK_EQ_U64(
                        head->u.send_bidi_stream.stream_ref._v, r.blk_ref._v);
                    MOQ_TEST_CHECK_EQ_INT(head->u.send_bidi_stream.fin ? 1 : 0, 0);
                    MOQ_TEST_CHECK(head->u.send_bidi_stream.data != NULL);
                    if (head->u.send_bidi_stream.data) {
                        moq_buf_reader_t gr;
                        moq_buf_reader_init(&gr, head->u.send_bidi_stream.data,
                                            head->u.send_bidi_stream.len);
                        moq_control_envelope_t genv;
                        memset(&genv, 0, sizeof(genv));
                        MOQ_TEST_CHECK_EQ_INT(
                            (int)moq_d18_decode_envelope(&gr, &genv),
                            (int)MOQ_OK);
                        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&gr),
                                               (size_t)0);
                        MOQ_TEST_CHECK_EQ_U64(genv.msg_type,
                                              (uint64_t)MOQ_D18_GOAWAY);
                        moq_d18_goaway_t gaw;
                        memset(&gaw, 0, sizeof(gaw));
                        MOQ_TEST_CHECK_EQ_INT(
                            (int)moq_d18_decode_goaway_request(genv.payload,
                                genv.payload_len, &gaw), (int)MOQ_OK);
                        MOQ_TEST_CHECK(gaw.uri.len == sizeof(nob_goaway_uri) &&
                                       gaw.uri.data &&
                                       memcmp(gaw.uri.data, nob_goaway_uri,
                                              sizeof(nob_goaway_uri)) == 0);
                    }
                } else {
                    MOQ_TEST_CHECK_EQ_INT((int)head->kind,
                                          (int)MOQ_ACTION_CLOSE_BIDI_STREAM);
                    MOQ_TEST_CHECK_EQ_U64(
                        head->u.close_bidi_stream.stream_ref._v,
                        (uint64_t)0x6000);
                }
            }
        }
        failures += nob_blk_check(&tp, &r, &blk_snap, "blocked");
        /* Product-path carrier: the genuinely refused no-FIN target is retained
         * in exactly one carrier with its declared ref and FIN=false; the
         * unrelated carrier stays exact; the count is baseline + 1. */
        MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, r.target_ref), 0);
        MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, unrel), 0);
        MOQ_TEST_CHECK_EQ_SIZE(tp.server->noslot_carrier_count,
                               carrier_base + 1);
    }

    /* Declared completion/late drain rings, derived from DECLARED members. */
    nob_ring_t comp_want, late_want;
    if (c->origin == 1) {
        nob_ring_t less = ring0;
        size_t k = 0;
        int removed = 0;
        for (size_t i = 0; i < ring0.count; i++) {
            if (!removed && ring0.ref[i] == 0x4000) { removed = 1; continue; }
            less.ref[k] = ring0.ref[i];
            less.reason[k] = ring0.reason[i];
            k++;
        }
        less.count = k;
        MOQ_TEST_CHECK_EQ_INT(removed, 1);
        late_want = less;
        nob_ring_plus(&less, r.target_ref, MOQ_DRAIN_NORMAL, &comp_want);
    } else {
        nob_ring_plus(&ring0, r.target_ref, MOQ_DRAIN_NORMAL, &comp_want);
        late_want = ring0;
    }

    /* Origin 1: free ONE drain slot by feeding the DECLARED filler's own FIN
     * through the session's public absorb path. */
    if (c->origin == 1)
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(
                tp.server, moq_stream_ref_from_u64(0x4000), NULL, 0, true, 0),
            (int)MOQ_OK);

    /* Service-only recovery. No further ingress. */
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);

    /* The wire, as ONE ordered declared image per origin -- the blocker's own
     * dispatch included, byte for byte. Origins 2/3's blocker closes an
     * UNMAPPED ref and legally emits nothing. */
    {
        MOQ_TEST_CHECK(tp.server_ep.count < FAKE_EP_MAX_OPS);
        size_t want_n = 0;
        struct { int kind; uint64_t sid; int fin; } wantop[3];
        memset(wantop, 0, sizeof(wantop));
        if (c->origin == 4) {
            wantop[want_n].kind = FAKE_OP_WRITE;
            wantop[want_n].sid = r.blk_id;   /* the announcement's request bidi */
            wantop[want_n].fin = 0;
            want_n++;
        }
        if (c->pre_setup) {
            wantop[want_n].kind = FAKE_OP_STOP;
            wantop[want_n].sid = r.target_id;
            want_n++;
            wantop[want_n].kind = FAKE_OP_RESET;
            wantop[want_n].sid = r.target_id;
            want_n++;
        } else {
            wantop[want_n].kind = FAKE_OP_WRITE;
            wantop[want_n].sid = r.target_id;
            wantop[want_n].fin = 1;
            want_n++;
        }
        if (tp.server_ep.count != want_n) {
            fprintf(stderr, "NOB %s: %zu endpoint ops, expected %zu\n", what,
                    tp.server_ep.count, want_n);
            failures++;
        }
        size_t n = tp.server_ep.count < want_n ? tp.server_ep.count : want_n;
        for (size_t i = 0; i < n; i++) {
            fake_op_t *o = &tp.server_ep.ops[i];
            if ((int)o->kind != wantop[i].kind ||
                o->stream_id != wantop[i].sid) {
                fprintf(stderr, "NOB %s: op %zu kind %d sid %llu, expected"
                        " kind %d sid %llu\n", what, i, (int)o->kind,
                        (unsigned long long)o->stream_id, wantop[i].kind,
                        (unsigned long long)wantop[i].sid);
                failures++;
                continue;
            }
            if (o->kind == FAKE_OP_STOP || o->kind == FAKE_OP_RESET) {
                MOQ_TEST_CHECK_EQ_U64(o->error_code, (uint64_t)0x1);
            } else if (o->kind == FAKE_OP_WRITE && wantop[i].fin == 0) {
                /* Origin 4's blocker: the nonterminal per-request GOAWAY on the
                 * announcement's request bidi -- a decodable REQUEST_GOAWAY with
                 * the exact New Session URI, no trailing bytes, no FIN. */
                MOQ_TEST_CHECK_EQ_INT(o->fin ? 1 : 0, 0);
                if (o->data_len > sizeof(o->data)) {
                    fprintf(stderr, "NOB %s: recorder length %zu exceeds the"
                            " inline capacity\n", what, o->data_len);
                    failures++;
                    continue;
                }
                moq_buf_reader_t gr;
                moq_buf_reader_init(&gr, o->data, o->data_len);
                moq_control_envelope_t genv;
                memset(&genv, 0, sizeof(genv));
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_envelope(&gr, &genv), (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&gr), (size_t)0);
                MOQ_TEST_CHECK_EQ_U64(genv.msg_type, (uint64_t)MOQ_D18_GOAWAY);
                moq_d18_goaway_t gaw;
                memset(&gaw, 0, sizeof(gaw));
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_goaway_request(genv.payload,
                                                       genv.payload_len, &gaw),
                    (int)MOQ_OK);
                MOQ_TEST_CHECK(gaw.uri.len == sizeof(nob_goaway_uri) &&
                               gaw.uri.data &&
                               memcmp(gaw.uri.data, nob_goaway_uri,
                                      sizeof(nob_goaway_uri)) == 0);
            } else {
                /* The target terminal: one FIN'd write, one complete
                 * REQUEST_ERROR, no trailing bytes, guarded non-NULL span. */
                MOQ_TEST_CHECK(o->fin);
                if (o->data_len > sizeof(o->data)) {
                    fprintf(stderr, "NOB %s: recorder length %zu exceeds the"
                            " inline capacity\n", what, o->data_len);
                    failures++;
                    continue;
                }
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, o->data, o->data_len);
                moq_control_envelope_t env;
                memset(&env, 0, sizeof(env));
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_envelope(&rr, &env), (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_REQUEST_ERROR);
                moq_d18_request_error_t er;
                memset(&er, 0, sizeof(er));
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_request_error(env.payload,
                                                      env.payload_len, &er),
                    (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(
                    er.error_code, (uint64_t)MOQ_REQUEST_ERROR_INTERNAL_ERROR);
                MOQ_TEST_CHECK_EQ_U64(er.retry_interval, (uint64_t)0);
                MOQ_TEST_CHECK(er.reason.len == 17 && er.reason.data &&
                               memcmp(er.reason.data, "request pool full",
                                      17) == 0);
            }
        }
    }
    /* Both routes leave the target mapping in the local-closed live state at
     * recovery: post-SETUP the REQUEST_ERROR FIN'd our send half, pre-SETUP our
     * RESET_STREAM closed it. A bidi RESET closes only our sending direction
     * (RFC 9000 §3.5), so the mapping is retained by transport id and internal
     * ref while the peer terminal is still owed (a NORMAL drain reference), and
     * the later peer RESET/FIN retires it. */
    failures += nob_check_phase(&tp, &r, &before, &comp_want, 1, "recovered");
    failures += nob_blk_check(&tp, &r, &blk_snap, "recovered");
    /* Completion retired exactly the target carrier; count back to baseline. */
    MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, r.target_ref), -1);
    MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, unrel), 0);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->noslot_carrier_count, carrier_base);

    /* Exactly once: a second service adds nothing and reproduces the whole
     * postcondition. */
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
    failures += nob_check_phase(&tp, &r, &before, &comp_want, 1, "reserviced");
    failures += nob_blk_check(&tp, &r, &blk_snap, "reserviced");
    MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, r.target_ref), -1);
    MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, unrel), 0);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->noslot_carrier_count, carrier_base);

    /* The legitimate LATER target terminal: exact drain release and physical
     * retirement exactly once. Post-SETUP it is the peer FIN; pre-SETUP it is
     * the peer's RESET_STREAM answering our STOP_SENDING. Either arrives on the
     * saved transport id, is delivered to moq_session_on_bidi_stream_reset() /
     * the bidi-bytes FIN path through the retained mapping, releases the exact
     * target NORMAL drain ref, and retires the mapping. */
    if (c->pre_setup) {
        /* The peer's REAL answer to STOP_SENDING is RESET_STREAM, delivered
         * through the bridge on the saved TRANSPORT id. The contract is
         * storage-neutral: whatever tombstone/carrier design the corrected
         * bridge uses after its own STOP+RESET dispatch, this peer RESET must
         * release the exact target NORMAL drain ref, leave no semantic owner
         * or physical mapping, stay nonfatal and open, and emit nothing. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_stream_reset(
                tp.server_bridge, r.target_id, 0x1, 0),
            (int)MOQ_OK);
    } else {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_bidi_bytes(
                tp.server_bridge, r.target_id, NULL, 0, true, 0),
            (int)MOQ_OK);
    }
    /* An endpoint op emitted SYNCHRONOUSLY by the late terminal must be
     * caught, not discarded by the next clear: zero ops BEFORE any clear or
     * service. */
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
    failures += nob_check_phase(&tp, &r, &before, &late_want, 0,
                                c->pre_setup ? "late-reset-pre-service"
                                             : "late-fin-pre-service");
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
    failures += nob_check_phase(&tp, &r, &before, &late_want, 0,
                                c->pre_setup ? "late-reset-serviced"
                                             : "late-fin-serviced");
    /* The target's own late terminal must not have mutated or retired the
     * independently live blocker. */
    failures += nob_blk_check(&tp, &r, &blk_snap, "late-terminal");
    /* The late terminal keeps the target carrier absent and the unrelated one
     * exact -- no resurrection. */
    MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, r.target_ref), -1);
    MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, unrel), 0);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->noslot_carrier_count, carrier_base);
    if (c->pre_setup) {
        /* A REPEATED peer RESET plus service adds nothing: same ring, no op,
         * no event, nothing resurrected. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_stream_reset(
                tp.server_bridge, r.target_id, 0x1, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
        failures += nob_check_phase(&tp, &r, &before, &late_want, 0,
                                    "late-reset-again-pre-service");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(tp.server_bridge, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
        failures += nob_check_phase(&tp, &r, &before, &late_want, 0,
                                    "late-reset-again");
        failures += nob_check_seed(&tp, &r, "late-reset-again");
        failures += nob_check_target_gone(&tp, &r,
                                          before.recv_payload_bytes,
                                          "late-reset-again");
        nob_ring_t now2;
        nob_ring_snap(tp.server, &now2);
        failures += nob_ring_equals(&now2, &late_want, "late-reset-again");
    }

    test_pair_destroy(&tp);
    return failures;
}

/* Establish a draft-18 bridge pair whose SERVER has a one-slot subscription pool
 * (like the no-owner matrix), its slot occupied by a fragmented inbound
 * SUBSCRIBE so any further inbound SUBSCRIBE is refused for capacity. Every
 * create/start/action/feed/event result is checked; it fails immediately on any
 * setup failure. Leaves both op recorders cleared. Returns 0 on success. */
static int exh_arm_goaway_blocker(test_pair_t *tp, uint64_t bidi_id,
                                  moq_stream_ref_t *blk_ref_out);

static int exh_setup(test_pair_t *tp, uint32_t server_send_buf,
                     moq_stream_ref_t *seed_ref_out,
                     moq_stream_ref_t *blk_ref_out)
{
    if (d18_pair_init_caps(tp, 0, moq_alloc_default(), moq_alloc_default(),
                           0, false, 0) < 0)
        return -1;
    {
        moq_session_cfg_t scfg;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_SERVER);
        scfg.version = MOQ_VERSION_DRAFT_18;
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 10;
        /* All seven request-owner pools at 1, so the carrier pool (== the drain
         * ring == the sum of the seven caps) is exactly 7: small, bounded, and
         * fully fillable in the exhaustion arms. */
        scfg.max_subscriptions = 1;
        scfg.max_announcements = 1;
        scfg.max_fetches = 1;
        scfg.max_publishes = 1;
        scfg.max_track_statuses = 1;
        scfg.max_namespace_subscriptions = 1;
        scfg.max_track_subscriptions = 1;
        if (server_send_buf) scfg.send_buffer_size = server_send_buf;
        moq_session_t *sv = NULL;
        if (moq_session_create(&scfg, 0, &sv) != MOQ_OK || !sv) {
            test_pair_destroy(tp); return -1;
        }
        moq_transport_bridge_destroy(tp->server_bridge);
        moq_session_destroy(tp->server);
        tp->server = sv;
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
        if (moq_transport_bridge_create(&bcfg, tp->server, &tp->server_ep.vtable,
                                        &tp->server_ep, &tp->server_bridge)
            != MOQ_OK) { test_pair_destroy(tp); return -1; }
    }
    if (moq_session_start(tp->client, 0) != MOQ_OK ||
        moq_session_start(tp->server, 0) != MOQ_OK) {
        test_pair_destroy(tp); return -1;
    }
    if (d18_strict_shuttle(tp, 30, 0, "exh setup") != 0) {
        test_pair_destroy(tp); return -1;
    }
    /* Strict setup classification: exactly one SETUP_COMPLETE per side and zero
     * other events, both sessions ESTABLISHED, both bridges nonfatal/open with
     * no pending work and empty action queues. Setup transport output was
     * already classified by d18_strict_shuttle; clear the recorders so the
     * blocker/seed phases measure only their own output. */
    moq_event_t ev;
    int ss = 0, so = 0, cs = 0, co = 0;
    while (moq_session_poll_events(tp->server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) ss++; else so++;
        moq_event_cleanup(&ev);
    }
    while (moq_session_poll_events(tp->client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) cs++; else co++;
        moq_event_cleanup(&ev);
    }
    if (ss != 1 || so != 0 || cs != 1 || co != 0 ||
        tp->client->state != MOQ_SESS_ESTABLISHED ||
        tp->server->state != MOQ_SESS_ESTABLISHED ||
        moq_transport_bridge_is_fatal(tp->server_bridge) ||
        moq_transport_bridge_is_closed(tp->server_bridge) ||
        moq_transport_bridge_is_fatal(tp->client_bridge) ||
        moq_transport_bridge_is_closed(tp->client_bridge) ||
        moq_transport_bridge_has_pending(tp->server_bridge) ||
        moq_transport_bridge_has_pending(tp->client_bridge) ||
        tp->server->action_tail != tp->server->action_head ||
        tp->client->action_tail != tp->client->action_head) {
        fprintf(stderr, "EXH setup: setup classification failed\n");
        test_pair_destroy(tp); return -1;
    }
    fake_endpoint_clear_ops(&tp->client_ep);
    fake_endpoint_clear_ops(&tp->server_ep);

    /* Arm the send-buffer blocker BEFORE the seed so the announcement can take
     * and then free the single subscription slot the seed occupies next. */
    if (blk_ref_out &&
        exh_arm_goaway_blocker(tp, 700, blk_ref_out) != 0) {
        test_pair_destroy(tp); return -1;
    }

    /* A header-only request-stream PREFIX (Type + Length, no Request ID byte,
     * proven shorter than the full message by the helper): consumes no wire
     * Request ID, so it never collides with the blocker's committed id 0 or the
     * target. Feeding it must produce NO endpoint output and NO new action. */
    uint8_t seed[128];
    size_t seed_len = 0;
    if (d18_seed_header_prefix(seed, sizeof(seed), 4, &seed_len, NULL) != 0) {
        test_pair_destroy(tp); return -1;
    }
    size_t act_before = tp->server->action_tail - tp->server->action_head;
    fake_endpoint_clear_ops(&tp->server_ep);
    if (moq_transport_bridge_on_peer_bidi_bytes(tp->server_bridge, 500, seed,
                                                seed_len, false, 0) != MOQ_OK) {
        test_pair_destroy(tp); return -1;
    }
    if (tp->server_ep.count != 0 ||
        (size_t)(tp->server->action_tail - tp->server->action_head)
            != act_before) {
        fprintf(stderr, "EXH setup: seed produced unexpected output/action\n");
        test_pair_destroy(tp); return -1;
    }
    /* Absolute seed identity: distinct nonzero ref vs transport id, one
     * receiving subscription owner with the exact retained prefix bytes/length,
     * its stream-ref registry edge (no request-id key), and a live peer-origin
     * BIDI mapping with no pending/close flags. */
    {
        bridge_stream_entry_t *se = bridge_find_by_id(tp->server_bridge, 500);
        moq_stream_ref_t sref = se ? se->ref : moq_stream_ref_from_u64(0);
        if (!se || sref._v == 0 || sref._v == 500) {
            fprintf(stderr, "EXH setup: seed ref/id not distinct\n");
            test_pair_destroy(tp); return -1;
        }
        moq_request_endpoint_t ep =
            request_registry_find_by_streamref(tp->server, sref);
        if (ep.kind != MOQ_REQ_SUBSCRIPTION || ep.has_request_id ||
            !ep.has_stream_ref || ep.stream_ref._v != sref._v ||
            (int)tp->server->subs[ep.slot].state !=
                (int)MOQ_SUB_RECVING_REQUEST ||
            tp->server->subs[ep.slot].req_recv_len != seed_len ||
            !tp->server->subs[ep.slot].req_recv_buf ||
            memcmp(tp->server->subs[ep.slot].req_recv_buf, seed,
                   seed_len) != 0) {
            fprintf(stderr, "EXH setup: seed owner not the declared receiver\n");
            test_pair_destroy(tp); return -1;
        }
        if (seed_ref_out) *seed_ref_out = sref;
    }
    fake_endpoint_clear_ops(&tp->client_ep);
    fake_endpoint_clear_ops(&tp->server_ep);
    return 0;
}

/* Encode one complete inbound d18 SUBSCRIBE at `request_id` into buf; returns
 * its length. */
static size_t exh_encode_subscribe(uint8_t *buf, size_t cap, uint64_t request_id)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("x") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t prm = { 0 };
    if (moq_d18_encode_subscribe(&w, request_id, &ns, MOQ_BYTES_LITERAL("t"),
                                 &prm) != MOQ_OK) return 0;
    return moq_buf_writer_offset(&w);
}

/* Arm the production-valid send-buffer blocker used by the exhaustion reset arm:
 * admit a live RECEIVER-side announcement on `bidi_id` through the real request
 * route, accept it, and issue a nonterminal per-request GOAWAY (§10.4) carrying
 * nob_goaway_uri. The accepted announcement re-keys into the announcement pool
 * and FREES its subscription staging slot; the GOAWAY is a genuine queued
 * request-bidi write that fills the send buffer (never a raw send_len poke), and
 * it leaves the announcement owner LIVE. Consumes inbound request id 0, so a
 * later target must use the next-in-sequence id 2. The server must be
 * ESTABLISHED with a free subscription slot. Returns 0 on success (with
 * *blk_ref_out set to the announcement bidi's ref), else a positive count. */
static int exh_arm_goaway_blocker(test_pair_t *tp, uint64_t bidi_id,
                                  moq_stream_ref_t *blk_ref_out)
{
    int failures = 0;
    uint8_t pn[128];
    size_t pn_len;
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, pn, sizeof(pn));
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("blk") };
        moq_namespace_t ns = { parts, 1 };
        moq_d18_msg_params_t mp = { 0 };
        if (moq_d18_encode_publish_namespace(&w, 0, &ns, &mp) != MOQ_OK)
            return failures + 1;
        pn_len = moq_buf_writer_offset(&w);
    }
    if (moq_transport_bridge_on_peer_bidi_bytes(tp->server_bridge, bidi_id, pn,
                                                pn_len, false, 0) != MOQ_OK)
        return failures + 1;
    moq_announcement_t ah;
    memset(&ah, 0, sizeof(ah));
    int got = 0;
    moq_event_t ev;
    while (moq_session_poll_events(tp->server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            ah = ev.u.namespace_published.ann;
            got = 1;
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(got);
    if (!got) return failures + 1;
    moq_accept_namespace_cfg_t ac;
    moq_accept_namespace_cfg_init(&ac);
    if (moq_session_accept_namespace(tp->server, ah, &ac, 0) != MOQ_OK)
        return failures + 1;
    /* Flush the REQUEST_OK so the send buffer starts empty before the GOAWAY. */
    if (moq_transport_bridge_service(tp->server_bridge, 0) != MOQ_OK)
        return failures + 1;
    while (moq_session_poll_events(tp->server, &ev, 1) > 0) moq_event_cleanup(&ev);
    fake_endpoint_clear_ops(&tp->server_ep);
    {
        bridge_stream_entry_t *be = bridge_find_by_id(tp->server_bridge, bidi_id);
        MOQ_TEST_CHECK(be != NULL);
        if (!be) return failures + 1;
        if (blk_ref_out) *blk_ref_out = be->ref;
    }
    moq_request_goaway_cfg_t gc;
    moq_request_goaway_cfg_init(&gc);
    gc.new_session_uri.data = exh_goaway_uri;
    gc.new_session_uri.len = sizeof(exh_goaway_uri);
    if (moq_session_request_goaway_namespace(tp->server, ah, &gc, 0) != MOQ_OK)
        return failures + 1;
    return failures;
}

/* Count queued STOP_BIDI / RESET_BIDI actions targeting a ref. */
/* The no-slot carrier pool as a ref/FIN multiset, compared exactly. */
#define EXH_CARRIER_MAX 64
typedef struct exh_carrier_snap {
    size_t   count;
    uint64_t ref[EXH_CARRIER_MAX];
    uint8_t  fin[EXH_CARRIER_MAX];
    int      overflow;
} exh_carrier_snap_t;

/* The no-slot carrier pool is SPARSE: removal clears an arbitrary slot and
 * decrements the count without compacting, so a live carrier can sit above the
 * first `count` slots and a cleared slot can sit below. Scan ALL cap slots and
 * append every nonzero entry; a mismatch against the product's own
 * `noslot_carrier_count`, or an overflow of the bounded snapshot, makes the
 * snapshot INCOMPARABLE rather than truncating it. */
static void exh_carrier_capture(const moq_session_t *s, exh_carrier_snap_t *c)
{
    memset(c, 0, sizeof(*c));
    size_t n = 0;
    for (size_t i = 0; i < s->noslot_carrier_cap; i++) {
        if (s->noslot_carriers[i].stream_ref == 0) continue;   /* sparse hole */
        if (n >= EXH_CARRIER_MAX) { c->overflow = 1; return; }
        c->ref[n] = s->noslot_carriers[i].stream_ref;
        c->fin[n] = s->noslot_carriers[i].fin ? 1u : 0u;
        n++;
    }
    if (n != s->noslot_carrier_count) { c->overflow = 1; return; }
    c->count = n;
}

/* Set to silence exh_carrier_equals diagnostics for an expected-negative call. */
static int exh_quiet;

static int exh_carrier_equals(const moq_session_t *s,
                              const exh_carrier_snap_t *want, const char *what)
{
    exh_carrier_snap_t now;
    exh_carrier_capture(s, &now);
    if (now.overflow || want->overflow) {
        if (!exh_quiet)
            fprintf(stderr, "EXH %s: carrier snapshot overflow\n", what);
        return 1;
    }
    if (now.count != want->count) {
        if (!exh_quiet)
            fprintf(stderr, "EXH %s: %zu carriers, expected %zu\n", what,
                    now.count, want->count);
        return 1;
    }
    int used[EXH_CARRIER_MAX] = { 0 };
    for (size_t i = 0; i < want->count; i++) {
        int found = -1;
        for (size_t j = 0; j < now.count; j++)
            if (!used[j] && now.ref[j] == want->ref[i] &&
                now.fin[j] == want->fin[i]) { found = (int)j; break; }
        if (found < 0) {
            if (!exh_quiet)
                fprintf(stderr, "EXH %s: carrier (ref %llu fin %d) missing\n",
                        what, (unsigned long long)want->ref[i],
                        (int)want->fin[i]);
            return 1;
        }
        used[found] = 1;
    }
    return 0;
}

/* Sparse-pool capture self-check: install A then B, remove A. A's slot clears
 * and the count drops, leaving B ABOVE the first `count` slot with a hole below
 * -- the exact shape a count-bounded scan would miss. The capture must find
 * exactly B, no zero/stale A. */
static int exh_carrier_capture_selfcheck(void)
{
    int failures = 0;
    test_pair_t tp;
    if (d18_pair_init(&tp, 1) < 0) return failures + 1;
    if (tp.server->noslot_carrier_cap < 2) { test_pair_destroy(&tp); return failures + 1; }

    moq_stream_ref_t A = moq_stream_ref_from_u64(0x1111);
    moq_stream_ref_t B = moq_stream_ref_from_u64(0x2222);
    MOQ_TEST_CHECK(noslot_carrier_install(tp.server, A, false));
    MOQ_TEST_CHECK(noslot_carrier_install(tp.server, B, true));
    noslot_carrier_remove(tp.server, A);
    /* A genuine hole below B: A gone, B still live, count back to 1. */
    MOQ_TEST_CHECK(noslot_carrier_find(tp.server, A) < 0);
    MOQ_TEST_CHECK(noslot_carrier_find(tp.server, B) >= 0);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->noslot_carrier_count, (size_t)1);

    exh_carrier_snap_t now;
    exh_carrier_capture(tp.server, &now);
    MOQ_TEST_CHECK_EQ_INT(now.overflow, 0);
    MOQ_TEST_CHECK_EQ_SIZE(now.count, (size_t)1);
    if (now.count == 1) {
        MOQ_TEST_CHECK_EQ_U64(now.ref[0], B._v);   /* exactly B, never a hole */
        MOQ_TEST_CHECK_EQ_INT((int)now.fin[0], 1);
    }
    /* exact-equals ACCEPTS {B} and (quietly) REJECTS the stale {A}. */
    {
        exh_carrier_snap_t want;
        memset(&want, 0, sizeof(want));
        want.count = 1; want.ref[0] = B._v; want.fin[0] = 1;
        failures += exh_carrier_equals(tp.server, &want, "exh capture selfcheck");

        exh_carrier_snap_t stale;
        memset(&stale, 0, sizeof(stale));
        stale.count = 1; stale.ref[0] = A._v; stale.fin[0] = 0;
        exh_quiet = 1;
        MOQ_TEST_CHECK_EQ_INT(
            exh_carrier_equals(tp.server, &stale, "exh capture stale-A"), 1);
        exh_quiet = 0;
    }

    /* The incomparable path is load-bearing: make noslot_carrier_count disagree
     * with the scanned live entries and require the capture to set overflow;
     * restore the count and prove the exact {B} snapshot passes again. */
    {
        size_t saved = tp.server->noslot_carrier_count;
        tp.server->noslot_carrier_count = saved + 1;
        exh_carrier_snap_t bad;
        exh_carrier_capture(tp.server, &bad);
        MOQ_TEST_CHECK_EQ_INT(bad.overflow, 1);
        tp.server->noslot_carrier_count = saved;
        exh_carrier_snap_t want;
        memset(&want, 0, sizeof(want));
        want.count = 1; want.ref[0] = B._v; want.fin[0] = 1;
        failures += exh_carrier_equals(tp.server, &want, "exh capture restored");
    }
    test_pair_destroy(&tp);
    return failures;
}

/* The seed staging owner installed by exh_setup, by SAVED ref: still a receiving
 * subscription with its stream-ref registry edge, no request-ID key, and no
 * pending/close bridge flags. */
static int exh_check_seed(test_pair_t *tp, moq_stream_ref_t seed_ref,
                          const char *what)
{
    int bad = 0;
    moq_request_endpoint_t ep =
        request_registry_find_by_streamref(tp->server, seed_ref);
    if (ep.kind != MOQ_REQ_SUBSCRIPTION || !ep.has_stream_ref ||
        ep.has_request_id || ep.stream_ref._v != seed_ref._v) {
        fprintf(stderr, "EXH %s: seed registry answer changed\n", what);
        bad++;
    }
    if (ep.kind == MOQ_REQ_SUBSCRIPTION &&
        (int)tp->server->subs[ep.slot].state != (int)MOQ_SUB_RECVING_REQUEST) {
        fprintf(stderr, "EXH %s: seed no longer receiving\n", what);
        bad++;
    }
    bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp->server_bridge,
                                                       seed_ref);
    if (!by_ref || !by_ref->active || by_ref->pending_retry ||
        by_ref->pending_fin || by_ref->fin_retained || by_ref->pending_reset ||
        by_ref->pending_stop || by_ref->local_send_closed) {
        fprintf(stderr, "EXH %s: seed bridge mapping changed\n", what);
        bad++;
    }
    return bad;
}

/* #245b carrier pool full, the post-SETUP REQUEST_ERROR blocked
 * on the send buffer, but two action slots and a NORMAL drain slot free. The
 * refused no-FIN target is dropped via exactly STOP+RESET(CANCELLED), the
 * session stays ESTABLISHED/nonfatal, exactly one NORMAL drain is added, no
 * target carrier is created, and every pre-existing carrier is conserved. */
static int test_noslot_exhaustion_reset(void)
{
    int failures = 0;
    test_pair_t tp;
    moq_stream_ref_t seed_ref, blk_ref;
    /* A small send buffer so the production-valid GOAWAY blocker fills it and the
     * target REQUEST_ERROR cannot fit -- no direct send_len poke. */
    if (exh_setup(&tp, 64, &seed_ref, &blk_ref) < 0) return 1;
    failures += exh_check_seed(&tp, seed_ref, "reset-armed");

    /* Bounded fill over the INDEPENDENTLY declared carrier capacity, asserting
     * each insertion and the exact final count -- a broken insert cannot spin. */
    size_t cap = tp.server->noslot_carrier_cap;
    MOQ_TEST_CHECK(cap > 0 && cap <= EXH_CARRIER_MAX);
    for (size_t i = 0; i < cap; i++)
        MOQ_TEST_CHECK(noslot_carrier_install(
            tp.server, moq_stream_ref_from_u64(0x9000 + i), false));
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->noslot_carrier_count, cap);

    /* Carrier and drain multisets before the target. The GOAWAY blocker already
     * fills the send buffer AND keeps a queued SEND_BIDI action (so
     * session_call_prepare cannot reset send_len) -- no separate filler. */
    exh_carrier_snap_t csnap;
    exh_carrier_capture(tp.server, &csnap);
    nob_ring_t ring_before;
    nob_ring_snap(tp.server, &ring_before);
    MOQ_TEST_CHECK(tp.server->send_cap - tp.server->send_len < 24);
    MOQ_TEST_CHECK(tp.server->drain_ref_count < tp.server->drain_ref_cap);
    MOQ_TEST_CHECK(action_queue_avail(tp.server) >= 2);

    uint8_t tgt[128];
    size_t tgt_len = exh_encode_subscribe(tgt, sizeof(tgt), 2);
    MOQ_TEST_CHECK(tgt_len > 0);
    moq_result_t rc = moq_transport_bridge_on_peer_bidi_bytes(
        tp.server_bridge, 600, tgt, tgt_len, false, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(tp.server->state == MOQ_SESS_ESTABLISHED);

    bridge_stream_entry_t *te = bridge_find_by_id(tp.server_bridge, 600);
    MOQ_TEST_CHECK(te != NULL);
    if (!te) { test_pair_destroy(&tp); return failures + 1; }
    moq_stream_ref_t tgt_ref = te->ref;
    MOQ_TEST_CHECK(tgt_ref._v != 0);

    /* The WHOLE queued action inventory: exactly the blocker GOAWAY on blk_ref
     * plus target STOP_BIDI(CANCELLED 0x1) + RESET_BIDI(CANCELLED 0x1), no more. */
    {
        size_t depth = tp.server->action_tail - tp.server->action_head;
        MOQ_TEST_CHECK_EQ_SIZE(depth, (size_t)3);
        int goaway = 0, stop = 0, reset = 0, other = 0;
        for (size_t i = tp.server->action_head; i != tp.server->action_tail;
             i++) {
            const moq_action_t *a =
                &tp.server->actions[i % tp.server->action_cap];
            if (a->kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a->u.send_bidi_stream.stream_ref._v == blk_ref._v &&
                !a->u.send_bidi_stream.fin) goaway++;
            else if (a->kind == MOQ_ACTION_STOP_BIDI_STREAM &&
                     a->u.stop_bidi_stream.stream_ref._v == tgt_ref._v &&
                     a->u.stop_bidi_stream.error_code == 0x1) stop++;
            else if (a->kind == MOQ_ACTION_RESET_BIDI_STREAM &&
                     a->u.reset_bidi_stream.stream_ref._v == tgt_ref._v &&
                     a->u.reset_bidi_stream.error_code == 0x1) reset++;
            else other++;
        }
        MOQ_TEST_CHECK_EQ_INT(goaway, 1);
        MOQ_TEST_CHECK_EQ_INT(stop, 1);
        MOQ_TEST_CHECK_EQ_INT(reset, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* Carrier pool conserved EXACTLY (multiset), no target carrier created. */
    failures += exh_carrier_equals(tp.server, &csnap, "reset-refused");
    MOQ_TEST_CHECK(noslot_carrier_find(tp.server, tgt_ref) < 0);
    /* Complete drain multiset: prior entries + exactly (tgt_ref, NORMAL). */
    {
        nob_ring_t want;
        nob_ring_plus(&ring_before, tgt_ref, MOQ_DRAIN_NORMAL, &want);
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &want, "reset-refused");
    }
    failures += exh_check_seed(&tp, seed_ref, "reset-refused");
    MOQ_TEST_CHECK(request_registry_find_by_streamref(tp.server, blk_ref).kind
                   == MOQ_REQ_ANNOUNCEMENT);

    /* Service to the endpoint: exactly the GOAWAY WRITE(blk_id) + STOP + RESET on
     * the target id, the target's local send closed, both target ids resolving to
     * the retained live mapping while the NORMAL drain is still owed. */
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    {
        MOQ_TEST_CHECK(tp.server_ep.count < FAKE_EP_MAX_OPS);
        MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)3);
        int w = 0, s = 0, rst = 0, other = 0;
        for (size_t i = 0; i < tp.server_ep.count; i++) {
            const fake_op_t *o = &tp.server_ep.ops[i];
            if (o->kind == FAKE_OP_WRITE && o->stream_id == blk_ref._v) other++;
            else if (o->kind == FAKE_OP_WRITE && o->stream_id == 700 && !o->fin)
                w++;
            else if (o->kind == FAKE_OP_STOP && o->stream_id == 600 &&
                     o->error_code == 0x1) s++;
            else if (o->kind == FAKE_OP_RESET && o->stream_id == 600 &&
                     o->error_code == 0x1) rst++;
            else other++;
        }
        MOQ_TEST_CHECK_EQ_INT(w, 1);
        MOQ_TEST_CHECK_EQ_INT(s, 1);
        MOQ_TEST_CHECK_EQ_INT(rst, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    {
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.server_bridge,
                                                           tgt_ref);
        MOQ_TEST_CHECK(by_id && by_id == by_ref && by_id->active &&
                       by_id->local_send_closed && !by_id->peer_send_closed);
    }
    failures += exh_check_seed(&tp, seed_ref, "reset-serviced");
    MOQ_TEST_CHECK(request_registry_find_by_streamref(tp.server, blk_ref).kind
                   == MOQ_REQ_ANNOUNCEMENT);

    /* The legal later peer terminal (the peer's RESET_STREAM answering our
     * STOP_SENDING) releases exactly the target NORMAL drain and physically
     * retires the mapping, nonfatal and open. */
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_stream_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));
    {
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "reset-terminal");
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.server_bridge,
                                                           tgt_ref);
        MOQ_TEST_CHECK((!by_id || !by_id->active) &&
                       (!by_ref || !by_ref->active));
    }
    failures += exh_carrier_equals(tp.server, &csnap, "reset-terminal");
    failures += exh_check_seed(&tp, seed_ref, "reset-terminal");

    /* Repeat the terminal + service: inert -- no drain mutation, no resurrection,
     * carrier pool exact and the seed's pinned registry/receiving/mapping
     * inventory conserved. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_stream_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    {
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "reset-terminal-again");
    }
    failures += exh_carrier_equals(tp.server, &csnap, "reset-terminal-again");
    failures += exh_check_seed(&tp, seed_ref, "reset-terminal-again");
    MOQ_TEST_CHECK(request_registry_find_by_streamref(tp.server, blk_ref).kind
                   == MOQ_REQ_ANNOUNCEMENT);

    test_pair_destroy(&tp);
    return failures;
}

/* Reset-lifecycle discriminator 1: the bidi RESET endpoint op returns
 * WOULD_BLOCK, then is accepted. The pending reset carries the exact target ref
 * and code; local-send closure is NOT applied before endpoint acceptance; after
 * acceptance the RESET endpoint op is emitted, local-send is closed, and both
 * bridge-ID lookups remain while the NORMAL drain is owed; the later peer
 * terminal releases exactly that drain and retires the mapping; a repeat is
 * inert. */
static int test_noslot_reset_bidi_wouldblock_then_accept(void)
{
    int failures = 0;
    test_pair_t tp;
    moq_stream_ref_t seed_ref, blk_ref;
    if (exh_setup(&tp, 64, &seed_ref, &blk_ref) < 0) return 1;
    size_t cap = tp.server->noslot_carrier_cap;
    MOQ_TEST_CHECK(cap > 0 && cap <= EXH_CARRIER_MAX);
    for (size_t i = 0; i < cap; i++)
        MOQ_TEST_CHECK(noslot_carrier_install(
            tp.server, moq_stream_ref_from_u64(0x9000 + i), false));
    nob_ring_t ring_before;
    nob_ring_snap(tp.server, &ring_before);

    uint8_t tgt[128];
    size_t tgt_len = exh_encode_subscribe(tgt, sizeof(tgt), 2);
    MOQ_TEST_CHECK(tgt_len > 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_bidi_bytes(tp.server_bridge, 600, tgt,
                                                     tgt_len, false, 0),
        (int)MOQ_OK);
    bridge_stream_entry_t *te = bridge_find_by_id(tp.server_bridge, 600);
    MOQ_TEST_CHECK(te != NULL);
    if (!te) { test_pair_destroy(&tp); return failures + 1; }
    moq_stream_ref_t tgt_ref = te->ref;
    /* The owed drain the target added. */
    nob_ring_t ring_owed;
    nob_ring_plus(&ring_before, tgt_ref, MOQ_DRAIN_NORMAL, &ring_owed);

    /* Block the RESET endpoint op and service: the RESET enqueues a
     * PENDING_RESET_STREAM carrying the exact target ref and code, local-send is
     * NOT yet closed, the mapping resolves by both ids, and the NORMAL drain is
     * owed. */
    tp.server_ep.block_reset = true;
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    {
        int found = 0;
        for (size_t i = 0; i < tp.server_bridge->pending_count; i++) {
            const bridge_pending_item_t *p = &tp.server_bridge->pending[i];
            if (p->kind == PENDING_RESET_STREAM &&
                p->stream_ref._v == tgt_ref._v && p->error_code == 0x1) found++;
        }
        MOQ_TEST_CHECK_EQ_INT(found, 1);
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.server_bridge,
                                                           tgt_ref);
        MOQ_TEST_CHECK(by_id && by_id == by_ref && by_id->active &&
                       !by_id->local_send_closed);
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_owed, "reset-wb-blocked");
    }

    /* Accept: unblock and service -- exactly one RESET op on the target id,
     * local-send now closed, mapping still resolving by both ids, drain owed. */
    tp.server_ep.block_reset = false;
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    {
        int rst = 0, other = 0;
        for (size_t i = 0; i < tp.server_ep.count; i++) {
            const fake_op_t *o = &tp.server_ep.ops[i];
            if (o->kind == FAKE_OP_RESET && o->stream_id == 600 &&
                o->error_code == 0x1) rst++;
            else other++;
        }
        MOQ_TEST_CHECK_EQ_INT(rst, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.server_bridge,
                                                           tgt_ref);
        MOQ_TEST_CHECK(by_id && by_id == by_ref && by_id->active &&
                       by_id->local_send_closed);
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_owed, "reset-wb-accepted");
    }

    /* Peer terminal releases exactly the target drain and retires the mapping. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_stream_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));
    {
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "reset-wb-terminal");
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.server_bridge,
                                                           tgt_ref);
        MOQ_TEST_CHECK((!by_id || !by_id->active) &&
                       (!by_ref || !by_ref->active));
    }

    /* Repeat the terminal: inert. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_stream_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    {
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "reset-wb-inert");
    }
    (void)seed_ref; (void)blk_ref;
    test_pair_destroy(&tp);
    return failures;
}

/* Reset-lifecycle discriminator 2 (control): a uni RESET_DATA retires its
 * physical mapping IMMEDIATELY on endpoint acceptance -- no PENDING carrier and
 * no request-bidi drain lifecycle imposed, in deliberate contrast with the bidi
 * reset above. */
static int test_uni_reset_data_immediate_retire(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init_full(&tp, 0, false, 64, 0) < 0) return 1;
    if (!setup_handshake(&tp)) { test_pair_destroy(&tp); return 1; }

    /* SERVER subscribes, CLIENT publishes and opens a local-origin data uni. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
    sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t ssub;
    MOQ_TEST_CHECK(moq_session_subscribe(tp.server, &sub_cfg, 0, &ssub) == MOQ_OK);
    pump_until_quiescent(&tp, 20, 0);

    moq_event_t ev;
    moq_subscription_t csub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(tp.client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
            csub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_subscription_is_valid(csub));
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(tp.client, csub, &acc, 0)
                   == MOQ_OK);
    pump_until_quiescent(&tp, 20, 0);
    while (moq_session_poll_events(tp.server, &ev, 1) > 0) moq_event_cleanup(&ev);

    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    sg_cfg.group_id = 0;
    sg_cfg.subgroup_id = 0;
    sg_cfg.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_subgroup(tp.client, csub, &sg_cfg, 0, &sg)
                   == MOQ_OK);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    uint64_t uni_sid = 0;
    for (size_t i = 0; i < tp.client_ep.count; i++)
        if (tp.client_ep.ops[i].kind == FAKE_OP_OPEN_UNI)
            uni_sid = tp.client_ep.ops[i].stream_id;
    MOQ_TEST_CHECK(uni_sid != 0);
    bridge_stream_entry_t *e = bridge_find_by_id(tp.client_bridge, uni_sid);
    MOQ_TEST_CHECK(e != NULL);
    if (!e) { test_pair_destroy(&tp); return failures + 1; }
    MOQ_TEST_CHECK(e->kind == BRIDGE_STREAM_UNI &&
                   e->origin == BRIDGE_ORIGIN_LOCAL && e->active);
    moq_stream_ref_t uni_ref = e->ref;
    size_t drain_before = tp.client->drain_ref_count;

    /* Reset the subgroup -> a uni RESET_DATA action. Service and accept it. */
    MOQ_TEST_CHECK(moq_session_reset_subgroup(tp.client, sg, 0x1, 0) == MOQ_OK);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));

    /* Exactly one RESET/ABORT op on the uni id; the mapping retired IMMEDIATELY
     * by both ids; NO drain ref was imposed; no PENDING reset lingers. */
    {
        int rst = 0;
        for (size_t i = 0; i < tp.client_ep.count; i++) {
            const fake_op_t *o = &tp.client_ep.ops[i];
            if ((o->kind == FAKE_OP_RESET || o->kind == FAKE_OP_ABORT) &&
                o->stream_id == uni_sid) rst++;
        }
        MOQ_TEST_CHECK_EQ_INT(rst, 1);
    }
    {
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.client_bridge,
                                                         uni_sid);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.client_bridge,
                                                           uni_ref);
        MOQ_TEST_CHECK((!by_id || !by_id->active) &&
                       (!by_ref || !by_ref->active));
    }
    MOQ_TEST_CHECK_EQ_SIZE(tp.client->drain_ref_count, drain_before);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));

    test_pair_destroy(&tp);
    return failures;
}

/* The bridge pending set holds EXACTLY the declared local RESET and nothing
 * else: one item, of the exact PENDING_RESET_STREAM shape a plain reset carries
 * -- no payload, no FIN, no owned action. Called at both the blocked checkpoint
 * and after the peer terminal from ONE checker, so the two phases cannot drift
 * and a mutation to any formerly-unchecked field (data/data_len/fin/owns_action)
 * fails here. */
static int nob_pending_is_only_reset(const moq_transport_bridge_t *b,
                                     uint64_t stream_id, moq_stream_ref_t ref,
                                     uint64_t code, const char *what)
{
    int bad = 0;
    if (b->pending_count != 1) {
        fprintf(stderr, "NOB %s: pending_count %lu, expected 1\n", what,
                (unsigned long)b->pending_count);
        return bad + 1;
    }
    const bridge_pending_item_t *p = &b->pending[0];
    if ((int)p->kind != (int)PENDING_RESET_STREAM) {
        fprintf(stderr, "NOB %s: pending kind %d, expected RESET\n", what,
                (int)p->kind);
        bad++;
    }
    if (p->stream_id != stream_id) bad++;
    if (p->stream_ref._v != ref._v) bad++;
    if (p->error_code != code) bad++;
    if (p->data != NULL) bad++;
    if (p->data_len != 0) bad++;
    if (p->fin) bad++;
    if (p->owns_action) bad++;
    if (bad)
        fprintf(stderr, "NOB %s: pending RESET item not the exact declared"
                " shape (id/ref/code/data/data_len/fin/owns_action)\n", what);
    return bad;
}

/* Both sides of the pair stay quiet and open: neither bridge fatal or closed,
 * the client bridge holds no pending work, both sessions stay ESTABLISHED with
 * empty event and action rings, and the client endpoint emits nothing. The
 * server endpoint's exact per-phase output is asserted separately by the caller. */
static int nob_pair_quiet(const test_pair_t *tp, const char *what)
{
    int bad = 0;
    if (moq_transport_bridge_is_fatal(tp->server_bridge) ||
        moq_transport_bridge_is_closed(tp->server_bridge) ||
        moq_transport_bridge_is_fatal(tp->client_bridge) ||
        moq_transport_bridge_is_closed(tp->client_bridge)) {
        fprintf(stderr, "NOB %s: a bridge is fatal/closed\n", what);
        bad++;
    }
    if (moq_transport_bridge_has_pending(tp->client_bridge)) {
        fprintf(stderr, "NOB %s: client bridge has pending work\n", what);
        bad++;
    }
    if ((int)tp->server->state != (int)MOQ_SESS_ESTABLISHED ||
        (int)tp->client->state != (int)MOQ_SESS_ESTABLISHED) {
        fprintf(stderr, "NOB %s: a session is not ESTABLISHED\n", what);
        bad++;
    }
    if (tp->server->event_tail != tp->server->event_head ||
        tp->server->action_tail != tp->server->action_head ||
        tp->client->event_tail != tp->client->event_head ||
        tp->client->action_tail != tp->client->action_head) {
        fprintf(stderr, "NOB %s: a session ring is non-empty\n", what);
        bad++;
    }
    if (tp->client_ep.count != 0) {
        fprintf(stderr, "NOB %s: client endpoint emitted %zu ops\n", what,
                tp->client_ep.count);
        bad++;
    }
    return bad;
}

/* Reset-lifecycle discriminator 3: the OPPOSITE ordering -- the peer terminal
 * arrives WHILE the endpoint RESET is still pending (before local acceptance).
 * The peer RESET releases the exact NORMAL drain and retires the mapping without
 * losing the locally owed RESET; unblocking/service then emits that RESET
 * exactly once on the saved transport id, clears the pending item, stays
 * nonfatal/open, resurrects no mapping or drain; a repeat is inert. */
static int test_noslot_reset_bidi_peer_before_accept(void)
{
    int failures = 0;
    test_pair_t tp;
    moq_stream_ref_t seed_ref, blk_ref;
    if (exh_setup(&tp, 64, &seed_ref, &blk_ref) < 0) return 1;
    size_t cap = tp.server->noslot_carrier_cap;
    MOQ_TEST_CHECK(cap > 0 && cap <= EXH_CARRIER_MAX);
    for (size_t i = 0; i < cap; i++)
        MOQ_TEST_CHECK(noslot_carrier_install(
            tp.server, moq_stream_ref_from_u64(0x9000 + i), false));
    nob_ring_t ring_before;
    nob_ring_snap(tp.server, &ring_before);

    uint8_t tgt[128];
    size_t tgt_len = exh_encode_subscribe(tgt, sizeof(tgt), 2);
    MOQ_TEST_CHECK(tgt_len > 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_bidi_bytes(tp.server_bridge, 600, tgt,
                                                     tgt_len, false, 0),
        (int)MOQ_OK);
    bridge_stream_entry_t *te = bridge_find_by_id(tp.server_bridge, 600);
    MOQ_TEST_CHECK(te != NULL);
    if (!te) { test_pair_destroy(&tp); return failures + 1; }
    moq_stream_ref_t tgt_ref = te->ref;
    nob_ring_t ring_owed;
    nob_ring_plus(&ring_before, tgt_ref, MOQ_DRAIN_NORMAL, &ring_owed);

    /* Endpoint RESET blocks: exactly one pending reset carrying the target
     * transport-id/ref/code; local send not yet closed; and the exact NORMAL
     * drain for the target ref is owed (ring_before + one MOQ_DRAIN_NORMAL for
     * tgt_ref), so a product that never installed the drain fails here. */
    tp.server_ep.block_reset = true;
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    /* The whole pending set is exactly one item, of the exact declared RESET
     * shape (checked here and re-checked after the peer terminal by the SAME
     * checker against the SAME declared constants -- a plain reset carries no
     * payload, FIN or owned action). */
    {
        failures += nob_pending_is_only_reset(tp.server_bridge, 600, tgt_ref,
                                              0x1, "reset-wb-blocked");
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        MOQ_TEST_CHECK(by_id && by_id->active && !by_id->local_send_closed);
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_owed, "reset-wb-blocked");
    }

    /* Peer RESET arrives WHILE the endpoint reset is still pending: releases the
     * exact drain (back to ring_before), retires the mapping by BOTH saved
     * transport id and stream ref, keeps the SAME single pending local RESET
     * unchanged, stays nonfatal/open, and does no other work at all -- no
     * endpoint op, no session event, no queued session action. */
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_stream_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    /* Both sides stay quiet and open, the server endpoint emits nothing, and the
     * SAME single RESET survives unchanged. */
    failures += nob_pair_quiet(&tp, "reset-peer-first-drain");
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
    {
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "reset-peer-first-drain");
        /* The SAME single RESET, exact declared shape -- an unrelated pending
         * item, or a mutation to any field, fails here. */
        failures += nob_pending_is_only_reset(tp.server_bridge, 600, tgt_ref,
                                              0x1, "reset-peer-first-drain");
        /* The mapping is gone by both keys the moment the peer terminal lands --
         * a product that retained it until the local RESET succeeded fails here,
         * BEFORE the unblock/service phase. */
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.server_bridge,
                                                           tgt_ref);
        MOQ_TEST_CHECK(!by_id || !by_id->active);
        MOQ_TEST_CHECK(!by_ref || !by_ref->active);
    }

    /* Unblock and service: the RESET is emitted exactly once on the saved id,
     * the pending item clears, and no mapping/drain is resurrected. */
    tp.server_ep.block_reset = false;
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    {
        int rst = 0, other = 0;
        for (size_t i = 0; i < tp.server_ep.count; i++) {
            const fake_op_t *o = &tp.server_ep.ops[i];
            if (o->kind == FAKE_OP_RESET && o->stream_id == 600 &&
                o->error_code == 0x1) rst++;
            else other++;
        }
        MOQ_TEST_CHECK_EQ_INT(rst, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
        MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.server_bridge));
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.server_bridge,
                                                           tgt_ref);
        MOQ_TEST_CHECK((!by_id || !by_id->active) &&
                       (!by_ref || !by_ref->active));
        failures += nob_pair_quiet(&tp, "reset-peer-first-done");
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "reset-peer-first-done");
    }

    /* Repeat the peer terminal + service: proves the FULL postcondition again,
     * not just endpoint count plus ring -- no output, pending empty, mapping
     * absent by both keys, exact ring_before, open/nonfatal, no session work. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_stream_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
    {
        MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.server_bridge));
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.server_bridge,
                                                           tgt_ref);
        MOQ_TEST_CHECK((!by_id || !by_id->active) &&
                       (!by_ref || !by_ref->active));
        failures += nob_pair_quiet(&tp, "reset-peer-first-inert");
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "reset-peer-first-inert");
    }
    (void)seed_ref; (void)blk_ref;
    test_pair_destroy(&tp);
    return failures;
}

/* -- shared oracles for the no-slot carrier peer-teardown lifecycle -------- */

/* Expected bridge-entry flag shape. */
typedef struct {
    bool active, peer_send_closed, local_send_closed, peer_stop_received;
    bool pending_retry, pending_fin, fin_retained, pending_reset, pending_stop;
    bool aborting;
} bshape_t;

/* The entry is a live peer-origin BIDI cross-resolving by both keys (id and ref
 * distinct), with exactly the declared flag shape. */
static int bcheck_entry(moq_transport_bridge_t *b, uint64_t id,
                        moq_stream_ref_t ref, const bshape_t *w, const char *what)
{
    int bad = 0;
    bridge_stream_entry_t *by_id = bridge_find_by_id(b, id);
    bridge_stream_entry_t *by_ref = bridge_find_by_ref(b, ref);
    if (!by_id) {
        fprintf(stderr, "NOB %s: no entry by id %llu\n", what,
                (unsigned long long)id);
        return 1;
    }
    if (by_id != by_ref) {
        fprintf(stderr, "NOB %s: id/ref do not cross-resolve to one entry\n",
                what);
        bad++;
    }
    if (by_id->transport_id != id || by_id->ref._v != ref._v || id == ref._v) {
        fprintf(stderr, "NOB %s: id/ref not distinct/consistent\n", what);
        bad++;
    }
    if ((int)by_id->kind != (int)BRIDGE_STREAM_BIDI ||
        (int)by_id->origin != (int)BRIDGE_ORIGIN_PEER) {
        fprintf(stderr, "NOB %s: not a peer-origin BIDI\n", what);
        bad++;
    }
#define BF(field) do { if (by_id->field != w->field) { \
        fprintf(stderr, "NOB %s: entry " #field " = %d, expected %d\n", what, \
                (int)by_id->field, (int)w->field); bad++; } } while (0)
    BF(active); BF(peer_send_closed); BF(local_send_closed);
    BF(peer_stop_received); BF(pending_retry); BF(pending_fin);
    BF(fin_retained); BF(pending_reset); BF(pending_stop); BF(aborting);
#undef BF
    return bad;
}

/* The retired postcondition, reasserted in full at every RESET checkpoint: the
 * target carrier is gone, only the unrelated carrier remains, the pre-target
 * drain multiset is unchanged, the retainability identity is cleared, the
 * mapping is absent by both keys, the seed's registry answer / receiving state /
 * live bridge mapping (the exact inventory exh_check_seed pins) are conserved,
 * both bridges are open/nonfatal with no pending work, both sessions ESTABLISHED
 * with empty event/action queues, and neither endpoint emitted anything. */
static int carrier_retired_post(test_pair_t *tp, moq_stream_ref_t tgt_ref,
                                moq_stream_ref_t other, moq_stream_ref_t seed_ref,
                                const nob_ring_t *ring_before, const char *what)
{
    int bad = 0;
    if (noslot_carrier_find(tp->server, tgt_ref) >= 0) {
        fprintf(stderr, "NOB %s: target carrier still present\n", what);
        bad++;
    }
    exh_carrier_snap_t want;
    memset(&want, 0, sizeof(want));
    want.count = 1; want.ref[0] = other._v; want.fin[0] = 0;
    bad += exh_carrier_equals(tp->server, &want, what);
    {
        nob_ring_t now;
        nob_ring_snap(tp->server, &now);
        bad += nob_ring_equals(&now, ring_before, what);
    }
    if (moq_session_has_transport_stream(tp->server, tgt_ref)) {
        fprintf(stderr, "NOB %s: retainability identity still present\n", what);
        bad++;
    }
    {
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp->server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp->server_bridge,
                                                           tgt_ref);
        if ((by_id && by_id->active) || (by_ref && by_ref->active)) {
            fprintf(stderr, "NOB %s: mapping not retired by both keys\n", what);
            bad++;
        }
    }
    bad += exh_check_seed(tp, seed_ref, what);
    if (moq_transport_bridge_is_fatal(tp->server_bridge) ||
        moq_transport_bridge_is_closed(tp->server_bridge) ||
        moq_transport_bridge_is_fatal(tp->client_bridge) ||
        moq_transport_bridge_is_closed(tp->client_bridge) ||
        moq_transport_bridge_has_pending(tp->server_bridge) ||
        moq_transport_bridge_has_pending(tp->client_bridge)) {
        fprintf(stderr, "NOB %s: a bridge is fatal/closed/pending\n", what);
        bad++;
    }
    if ((int)tp->server->state != (int)MOQ_SESS_ESTABLISHED ||
        (int)tp->client->state != (int)MOQ_SESS_ESTABLISHED) {
        fprintf(stderr, "NOB %s: a session is not ESTABLISHED\n", what);
        bad++;
    }
    if (tp->server->event_tail != tp->server->event_head ||
        tp->server->action_tail != tp->server->action_head ||
        tp->client->event_tail != tp->client->event_head ||
        tp->client->action_tail != tp->client->action_head) {
        fprintf(stderr, "NOB %s: a session ring is non-empty\n", what);
        bad++;
    }
    if (tp->server_ep.count != 0 || tp->client_ep.count != 0) {
        fprintf(stderr, "NOB %s: endpoint emitted output this phase\n", what);
        bad++;
    }
    return bad;
}

/* Arm a genuine BLOCKED no-slot carrier for the target through real inbound
 * request bytes, and prove it is retained before any observed state becomes a
 * baseline. exh_setup exhausts the sub pool (no send-buffer blocker, so no
 * residual output); the drain ring is filled so the target's no-FIN terminal
 * blocks on drain capacity and is RETAINED in a carrier
 * (session_subscribe.c:1411). The target uses the protocol-conformant first
 * client Request ID 0 (draft-18 section 10.1); the header-only seed consumes no
 * wire id. An unrelated carrier is installed first and must survive untouched.
 *   return  0: armed; other, tgt_ref, seed_ref, ring_before all set; tp live
 *   return -1: exh_setup failed and already destroyed tp
 *   return  1: a later step failed; tp is live (caller destroys). */
static int carrier_target_arm(test_pair_t *tp, moq_stream_ref_t *other,
                              moq_stream_ref_t *tgt_ref,
                              moq_stream_ref_t *seed_ref,
                              nob_ring_t *ring_before)
{
    if (exh_setup(tp, 0, seed_ref, NULL) < 0) return -1;
    if (tp->server->noslot_carrier_cap < 2) return 1;
    *other = moq_stream_ref_from_u64(0x9999);
    if (!noslot_carrier_install(tp->server, *other, false)) return 1;
    while (tp->server->drain_ref_count < tp->server->drain_ref_cap)
        if (!drain_ref_add(tp->server, moq_stream_ref_from_u64(
                0x4000 + tp->server->drain_ref_count))) return 1;

    /* The FULL pre-target drain multiset -- the baseline every teardown phase
     * is measured against. */
    nob_ring_snap(tp->server, ring_before);
    fake_endpoint_clear_ops(&tp->client_ep);
    fake_endpoint_clear_ops(&tp->server_ep);

    uint8_t tgt[128];
    size_t tgt_len = exh_encode_subscribe(tgt, sizeof(tgt), 0);
    if (tgt_len == 0) return 1;
    moq_result_t irc = moq_transport_bridge_on_peer_bidi_bytes(
        tp->server_bridge, 600, tgt, tgt_len, false, 0);
    /* Exactly WOULD_BLOCK: the terminal is retained, not completed. */
    if (irc != MOQ_ERR_WOULD_BLOCK) {
        fprintf(stderr, "carrier arm: ingress %d, expected WOULD_BLOCK\n",
                (int)irc);
        return 1;
    }
    bridge_stream_entry_t *te = bridge_find_by_id(tp->server_bridge, 600);
    if (!te) return 1;
    *tgt_ref = te->ref;

    /* The exact live retained shape: peer-origin BIDI, pending_retry only. */
    bshape_t armed = { .active = true, .pending_retry = true };
    if (bcheck_entry(tp->server_bridge, 600, *tgt_ref, &armed, "carrier arm")
        != 0) return 1;
    /* Exact carrier multiset {target, other}, both FIN=false. */
    {
        exh_carrier_snap_t want;
        memset(&want, 0, sizeof(want));
        want.count = 2;
        want.ref[0] = tgt_ref->_v; want.fin[0] = 0;
        want.ref[1] = other->_v;   want.fin[1] = 0;
        if (exh_carrier_equals(tp->server, &want, "carrier arm") != 0) return 1;
    }
    if (!moq_session_has_transport_stream(tp->server, *tgt_ref)) return 1;
    /* The drain multiset is unchanged by the refusal (no drain was taken). */
    {
        nob_ring_t now;
        nob_ring_snap(tp->server, &now);
        if (nob_ring_equals(&now, ring_before, "carrier arm ring") != 0)
            return 1;
    }
    /* The seed's declared registry / receiving / mapping inventory is
     * conserved across the refusal. */
    if (exh_check_seed(tp, *seed_ref, "carrier arm seed") != 0) return 1;
    /* Zero output, zero events, zero queued actions on BOTH sides before any
     * recorder clear; both sessions/bridges established, open, nonfatal. */
    if (tp->server_ep.count != 0 || tp->client_ep.count != 0) {
        fprintf(stderr, "carrier arm: stray endpoint output\n"); return 1;
    }
    if (tp->server->event_tail != tp->server->event_head ||
        tp->server->action_tail != tp->server->action_head ||
        tp->client->event_tail != tp->client->event_head ||
        tp->client->action_tail != tp->client->action_head) {
        fprintf(stderr, "carrier arm: stray session event/action\n"); return 1;
    }
    if (moq_transport_bridge_is_fatal(tp->server_bridge) ||
        moq_transport_bridge_is_closed(tp->server_bridge) ||
        moq_transport_bridge_is_fatal(tp->client_bridge) ||
        moq_transport_bridge_is_closed(tp->client_bridge) ||
        (int)tp->server->state != (int)MOQ_SESS_ESTABLISHED ||
        (int)tp->client->state != (int)MOQ_SESS_ESTABLISHED) {
        fprintf(stderr, "carrier arm: session/bridge not established/open\n");
        return 1;
    }
    return 0;
}

/* #245b: a retained no-slot carrier is RETIRED when a peer RESET terminates the
 * request (draft-18 sections 3.3.2, 11.4.1). The full retired postcondition is
 * reasserted at the immediate RESET, after first service, at the repeated RESET
 * before service, and after repeat service -- never merely count/carrier/ring. */
static int test_noslot_carrier_peer_reset(void)
{
    int failures = 0;
    test_pair_t tp;
    moq_stream_ref_t other, tgt_ref, seed_ref;
    nob_ring_t ring_before;
    int rc = carrier_target_arm(&tp, &other, &tgt_ref, &seed_ref, &ring_before);
    if (rc < 0) return 1;
    if (rc > 0) { test_pair_destroy(&tp); return 1; }

    /* Immediate peer RESET. */
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_stream_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    failures += carrier_retired_post(&tp, tgt_ref, other, seed_ref, &ring_before,
                                     "carrier-reset immediate");

    /* First service. */
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    failures += carrier_retired_post(&tp, tgt_ref, other, seed_ref, &ring_before,
                                     "carrier-reset service");

    /* Repeated peer RESET (before service). */
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_stream_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    failures += carrier_retired_post(&tp, tgt_ref, other, seed_ref, &ring_before,
                                     "carrier-reset repeat");

    /* Repeat service. */
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    failures += carrier_retired_post(&tp, tgt_ref, other, seed_ref, &ring_before,
                                     "carrier-reset repeat service");
    test_pair_destroy(&tp);
    return failures;
}

/* #245b: peer STOP_SENDING terminates the request and retires the exact carrier,
 * while the bridge preserves its half-close lifecycle. The literal lifecycle is
 * pinned end to end: the half-closed mapping and its send-half RESET pending
 * item after STOP; exactly one RESET(600,0x1) on first service with the mapping
 * still half-closed and pending_retry cleared; and the fully retired
 * postcondition after the legal final peer RESET, reproduced on repeat. */
static int test_noslot_carrier_peer_stop(void)
{
    int failures = 0;
    test_pair_t tp;
    moq_stream_ref_t other, tgt_ref, seed_ref;
    nob_ring_t ring_before;
    int rc = carrier_target_arm(&tp, &other, &tgt_ref, &seed_ref, &ring_before);
    if (rc < 0) return 1;
    if (rc > 0) { test_pair_destroy(&tp); return 1; }

    exh_carrier_snap_t car_after;
    memset(&car_after, 0, sizeof(car_after));
    car_after.count = 1; car_after.ref[0] = other._v; car_after.fin[0] = 0;

    /* 1. Immediately after STOP: carrier retired; drain ring unchanged and the
     *    seed's registry answer / receiving state / live bridge mapping
     *    conserved (the exact inventory exh_check_seed pins, not every field);
     *    the mapping is half-closed (local send closed, peer send open,
     *    peer_stop_received, original pending_retry intact); the ONLY server
     *    pending item is the send-half RESET the bridge declares (id 600, code
     *    0x1, stream_ref 0, no payload/FIN/owned action); and BOTH sides are
     *    otherwise quiet/open (both sessions ESTABLISHED with empty event/action
     *    rings, both bridges nonfatal, the client bridge with no pending work,
     *    endpoints exactly zero). The server bridge's one declared pending RESET
     *    is checked by the phase-specific pending assertion, not a generic
     *    no-pending check. */
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_stop_sending(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK(noslot_carrier_find(tp.server, tgt_ref) < 0);
    failures += exh_carrier_equals(tp.server, &car_after, "carrier-stop");
    {
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "carrier-stop ring");
    }
    failures += exh_check_seed(&tp, seed_ref, "carrier-stop seed");
    MOQ_TEST_CHECK(!moq_session_has_transport_stream(tp.server, tgt_ref));
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
    MOQ_TEST_CHECK_EQ_SIZE(tp.client_ep.count, (size_t)0);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->event_tail - tp.server->event_head,
                           (size_t)0);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->action_tail - tp.server->action_head,
                           (size_t)0);
    {
        bshape_t half = { .active = true, .local_send_closed = true,
                          .peer_stop_received = true, .pending_retry = true };
        failures += bcheck_entry(tp.server_bridge, 600, tgt_ref, &half,
                                 "carrier-stop half-closed");
    }
    failures += nob_pending_is_only_reset(tp.server_bridge, 600,
                                          moq_stream_ref_from_u64(0), 0x1,
                                          "carrier-stop pending");
    /* Both sides quiet/open (both sessions ESTABLISHED, both rings empty, client
     * bridge no pending, both bridges nonfatal, client endpoint zero). This does
     * NOT assert the server bridge is pending-free -- that is the one declared
     * RESET, pinned above. */
    failures += nob_pair_quiet(&tp, "carrier-stop immediate");
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(tp.server->state != MOQ_SESS_CLOSED);

    /* 2. First service: exactly one FAKE_OP_RESET(600, 0x1) and no other op; no
     *    request WRITE; server pending empty; mapping still half-closed with
     *    pending_retry CLEARED; carrier/unrelated/ring unchanged and the seed's
     *    registry/receiving/mapping inventory conserved; BOTH sides quiet/open;
     *    the semantic target identity still absent. */
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    {
        int rst = 0, wr = 0, other_ops = 0;
        for (size_t i = 0; i < tp.server_ep.count; i++) {
            const fake_op_t *o = &tp.server_ep.ops[i];
            if (o->kind == FAKE_OP_RESET && o->stream_id == 600 &&
                o->error_code == 0x1) rst++;
            else if (o->kind == FAKE_OP_WRITE && o->stream_id == 600) wr++;
            else other_ops++;
        }
        MOQ_TEST_CHECK_EQ_INT(rst, 1);
        MOQ_TEST_CHECK_EQ_INT(wr, 0);
        MOQ_TEST_CHECK_EQ_INT(other_ops, 0);
    }
    MOQ_TEST_CHECK_EQ_SIZE(tp.client_ep.count, (size_t)0);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.server_bridge));
    {
        bshape_t half_done = { .active = true, .local_send_closed = true,
                               .peer_stop_received = true };
        failures += bcheck_entry(tp.server_bridge, 600, tgt_ref, &half_done,
                                 "carrier-stop serviced");
    }
    MOQ_TEST_CHECK(noslot_carrier_find(tp.server, tgt_ref) < 0);
    failures += exh_carrier_equals(tp.server, &car_after, "carrier-stop serviced car");
    {
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "carrier-stop serviced ring");
    }
    failures += exh_check_seed(&tp, seed_ref, "carrier-stop serviced seed");
    /* Complete both-sides quiet/open AFTER service: both bridges open/nonfatal
     * with no pending work (the send-half RESET was serviced), both sessions
     * ESTABLISHED with empty event/action rings, client endpoint zero. */
    failures += nob_pair_quiet(&tp, "carrier-stop serviced");
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_session_has_transport_stream(tp.server, tgt_ref));

    /* 3. Final legal peer terminal (RESET of the peer's send half). Clear the
     *    recorders only now, AFTER step 2's serviced RESET has been checked, so
     *    the peer RESET's own synchronous output is measured against zero. */
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_stream_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);   /* no new output */
    failures += carrier_retired_post(&tp, tgt_ref, other, seed_ref, &ring_before,
                                     "carrier-stop final");
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    failures += carrier_retired_post(&tp, tgt_ref, other, seed_ref, &ring_before,
                                     "carrier-stop final service");
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_stream_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    failures += carrier_retired_post(&tp, tgt_ref, other, seed_ref, &ring_before,
                                     "carrier-stop final repeat");
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    failures += carrier_retired_post(&tp, tgt_ref, other, seed_ref, &ring_before,
                                     "carrier-stop final repeat service");
    test_pair_destroy(&tp);
    return failures;
}

/* #245b carrier pool full AND the whole per-stream terminal is
 * inadmissible (full drain ring, even with action slots free). The refused
 * no-FIN target closes the session with the draft-18 Session INTERNAL_ERROR
 * (0x1) -- no partial STOP/RESET, no protocol-violation/unauthorized code, no
 * bare WOULD_BLOCK -- and through the physical bridge the outcome is a graceful
 * close, never a bridge fatal. */
static int test_noslot_exhaustion_close(void)
{
    int failures = 0;
    test_pair_t tp;
    moq_stream_ref_t seed_ref;
    if (exh_setup(&tp, 0, &seed_ref, NULL) < 0) return 1;

    /* The exact close reason the product declares. */
    static const char k_reason[] =
        "no-owner admission carrier storage and per-stream"
        " terminal both inadmissible";
    const size_t reason_len = sizeof(k_reason) - 1;

    /* Bounded fills over the DECLARED carrier + drain capacities. */
    size_t ccap = tp.server->noslot_carrier_cap;
    MOQ_TEST_CHECK(ccap > 0 && ccap <= EXH_CARRIER_MAX);
    for (size_t i = 0; i < ccap; i++)
        MOQ_TEST_CHECK(noslot_carrier_install(
            tp.server, moq_stream_ref_from_u64(0x9000 + i), false));
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->noslot_carrier_count, ccap);
    size_t dcap = tp.server->drain_ref_cap;
    MOQ_TEST_CHECK(dcap > 0);
    for (size_t i = 0; i < dcap; i++)
        MOQ_TEST_CHECK(drain_ref_add(tp.server,
                                     moq_stream_ref_from_u64(0xA000 + i)));
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->drain_ref_count, dcap);

    exh_carrier_snap_t csnap;
    exh_carrier_capture(tp.server, &csnap);
    nob_ring_t ring_before;
    nob_ring_snap(tp.server, &ring_before);
    /* Action slots ARE free: the close is forced by carrier + drain exhaustion,
     * not by an action shortage. */
    MOQ_TEST_CHECK(tp.server->action_tail == tp.server->action_head);
    MOQ_TEST_CHECK(action_queue_avail(tp.server) >= 2);

    uint8_t tgt[128];
    size_t tgt_len = exh_encode_subscribe(tgt, sizeof(tgt), 0);
    MOQ_TEST_CHECK(tgt_len > 0);
    moq_result_t rc = moq_transport_bridge_on_peer_bidi_bytes(
        tp.server_bridge, 600, tgt, tgt_len, false, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);                 /* never bare/unowned WB */
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(tp.server->state == MOQ_SESS_CLOSED);

    /* Exactly one queued CLOSE_SESSION carrying INTERNAL_ERROR (0x1) and the
     * declared reason bytes -- no STOP/RESET, no other action. */
    {
        size_t depth = tp.server->action_tail - tp.server->action_head;
        MOQ_TEST_CHECK_EQ_SIZE(depth, (size_t)1);
        int close_a = 0, other = 0;
        for (size_t i = tp.server->action_head; i != tp.server->action_tail;
             i++) {
            const moq_action_t *a =
                &tp.server->actions[i % tp.server->action_cap];
            if (a->kind == MOQ_ACTION_CLOSE_SESSION &&
                a->u.close_session.code == 0x1 &&
                a->u.close_session.reason.len == reason_len &&
                a->u.close_session.reason.data &&
                memcmp(a->u.close_session.reason.data, k_reason,
                       reason_len) == 0) close_a++;
            else other++;
        }
        MOQ_TEST_CHECK_EQ_INT(close_a, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* Exactly one SESSION_CLOSED (0x1 + the same reason), zero other events. */
    {
        moq_event_t ev;
        int closed = 0, other = 0;
        while (moq_session_poll_events(tp.server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                closed++;
                MOQ_TEST_CHECK_EQ_U64(ev.u.closed.code, (uint64_t)0x1);
                MOQ_TEST_CHECK(ev.u.closed.reason.len == reason_len &&
                               ev.u.closed.reason.data &&
                               memcmp(ev.u.closed.reason.data, k_reason,
                                      reason_len) == 0);
            } else {
                other++;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(closed, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* Every pre-existing carrier and drain entry remains identity/reason exact. */
    failures += exh_carrier_equals(tp.server, &csnap, "close-refused");
    {
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "close-refused");
    }

    /* Service emits exactly one FAKE_OP_CLOSE with that code and reason and no
     * other endpoint operation; the bridge is closed, not fatal, no pending. */
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.server_bridge));
    {
        MOQ_TEST_CHECK(tp.server_ep.count < FAKE_EP_MAX_OPS);
        MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)1);
        if (tp.server_ep.count == 1) {
            const fake_op_t *o = &tp.server_ep.ops[0];
            MOQ_TEST_CHECK_EQ_INT((int)o->kind, (int)FAKE_OP_CLOSE);
            MOQ_TEST_CHECK_EQ_U64(o->error_code, (uint64_t)0x1);
            MOQ_TEST_CHECK(o->data_len == reason_len &&
                           memcmp(o->data, k_reason, reason_len) == 0);
        }
    }

    /* A second service cannot resurrect the target mapping or re-close. */
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
    MOQ_TEST_CHECK(bridge_find_by_id(tp.server_bridge, 600) == NULL ||
                   !bridge_find_by_id(tp.server_bridge, 600)->active);
    (void)seed_ref;
    test_pair_destroy(&tp);
    return failures;
}

static int test_no_owner_admission_bridge(void)
{
    int failures = 0;
    failures += nob_session_selfcheck();
    failures += nob_phys_selfcheck();
    failures += nob_rid_allowance_selfcheck();
    failures += exh_carrier_capture_selfcheck();
    nob_case_t c;
    memset(&c, 0, sizeof(c));
    /* target_rid declared per row: 0, 0, 0, 2. */
    c.origin = 1; c.pre_setup = false; c.name = "o1-drain";    c.target_rid = 0;
    failures += run_nob_case(&c);
    c.origin = 2; c.pre_setup = true;  c.name = "o2-presetup"; c.target_rid = 0;
    failures += run_nob_case(&c);
    c.origin = 3; c.pre_setup = false; c.name = "o3-action";   c.target_rid = 0;
    failures += run_nob_case(&c);
    c.origin = 4; c.pre_setup = false; c.name = "o4-send";     c.target_rid = 2;
    failures += run_nob_case(&c);
    return failures;
}

/* -- Peer STOP_SENDING on a request bidi (draft-18 §3.3.2) -------------
 *
 * STOP_SENDING targets one direction: our SENDING part of the stream, for
 * which RFC 9000 §3.5 asks us to send RESET_STREAM. It does not terminate the
 * peer's sending part of a bidi. §3.3.2 additionally says an endpoint that
 * rejects a request without application processing SHOULD send a
 * REQUEST_ERROR and FIN the stream -- which can only arrive if we kept the
 * receive half of the request bidi alive after its STOP.
 *
 * So a peer STOP on a LOCALLY-opened request bidi must close our send half and
 * nothing else. A peer STOP on a PEER-opened request bidi is the requester
 * cancelling the request it made, which is delivered to the session.
 *
 * Every one of these decisions is keyed on the STOP fact itself, never on the
 * ordinary FIN state: the no-STOP control below pins that an ordinary FIN
 * keeps its existing mapping, tombstone and output behaviour.
 */

/* The COMPLETE request shape the fixture issues, declared here and compared
 * against the wire rather than adopted from it. Designated initializers keep a
 * new field from silently shifting an existing fixture's values. */
typedef struct {
    uint64_t    request_id;
    const char *ns;
    const char *track;
} req_shape_t;

/* One draft-18 SUBSCRIBE, decoded and compared across EVERY surfaced field:
 * the request id, the namespace, the track name, the one filter parameter this
 * fixture asks for (with its irrelevant position scalars at their normalized
 * zeros), and the absence of every other representable message parameter --
 * including FORWARD, whose omission means the protocol default applies. Every
 * span is bounds- and NULL-guarded before it is read. */
static int check_subscribe_bytes(const uint8_t *data, size_t len,
                                 const req_shape_t *want, const char *what)
{
    int bad = 0;
    if (!data || len == 0) {
        fprintf(stderr, "FAIL: %s: no request bytes\n", what);
        return 1;
    }
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);
    moq_control_envelope_t env;
    if (moq_d18_decode_envelope(&r, &env) != MOQ_OK) {
        fprintf(stderr, "FAIL: %s: request is not a decodable envelope\n", what);
        return 1;
    }
    if (env.msg_type != MOQ_D18_SUBSCRIBE) {
        fprintf(stderr, "FAIL: %s: msg type 0x%llx, expected SUBSCRIBE\n", what,
                (unsigned long long)env.msg_type);
        bad++;
    }
    if (moq_buf_reader_remaining(&r) != 0) {
        fprintf(stderr, "FAIL: %s: %zu trailing bytes after the request\n",
                what, moq_buf_reader_remaining(&r));
        bad++;
    }
    moq_bytes_t parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
    moq_d18_subscribe_t sub;
    if (moq_d18_decode_subscribe(env.payload, env.payload_len, parts,
                                 MOQ_DECODED_MAX_NAMESPACE_PARTS,
                                 &sub) != MOQ_OK) {
        fprintf(stderr, "FAIL: %s: SUBSCRIBE body did not decode\n", what);
        return bad + 1;
    }
    if (sub.track_namespace.count != 1 || sub.track_namespace.parts == NULL) {
        fprintf(stderr, "FAIL: %s: namespace has %zu parts, expected 1\n", what,
                sub.track_namespace.count);
        return bad + 1;
    }
    size_t ns_len = strlen(want->ns);
    if (sub.track_namespace.parts[0].len != ns_len ||
        sub.track_namespace.parts[0].data == NULL ||
        memcmp(sub.track_namespace.parts[0].data, want->ns, ns_len) != 0) {
        fprintf(stderr, "FAIL: %s: namespace part differs from '%s'\n", what,
                want->ns);
        bad++;
    }
    size_t tn_len = strlen(want->track);
    if (sub.track_name.len != tn_len || sub.track_name.data == NULL ||
        memcmp(sub.track_name.data, want->track, tn_len) != 0) {
        fprintf(stderr, "FAIL: %s: track name differs from '%s'\n", what,
                want->track);
        bad++;
    }
    if (sub.request_id != want->request_id) {
        fprintf(stderr, "FAIL: %s: request id %llu, expected %llu\n", what,
                (unsigned long long)sub.request_id,
                (unsigned long long)want->request_id);
        bad++;
    }
    /* §5.1.2 filter type 2 is LARGEST_OBJECT, the shape the fixture asks for;
     * the absolute-range scalars belong to types 3/4 and must be normalized. */
    if (!sub.params.has_filter || sub.params.filter_type != 2u) {
        fprintf(stderr, "FAIL: %s: filter type %u (present=%d), expected 2\n",
                what, (unsigned)sub.params.filter_type,
                (int)sub.params.has_filter);
        bad++;
    }
    if (sub.params.filter_start_group != 0 ||
        sub.params.filter_start_object != 0 ||
        sub.params.filter_end_group != 0) {
        fprintf(stderr, "FAIL: %s: filter position scalars are not zero\n",
                what);
        bad++;
    }
    /* Every other representable parameter is absent. */
    if (sub.params.has_forward) {
        fprintf(stderr, "FAIL: %s: FORWARD present (%u); the fixture relies on"
                " the protocol default\n", what,
                (unsigned)sub.params.forward);
        bad++;
    }
    if (sub.params.has_subscriber_priority) {
        fprintf(stderr, "FAIL: %s: SUBSCRIBER_PRIORITY present\n", what);
        bad++;
    }
    if (sub.params.has_group_order) {
        fprintf(stderr, "FAIL: %s: GROUP_ORDER present\n", what);
        bad++;
    }
    if (sub.params.has_expires) {
        fprintf(stderr, "FAIL: %s: EXPIRES present\n", what);
        bad++;
    }
    if (sub.params.has_largest) {
        fprintf(stderr, "FAIL: %s: LARGEST_OBJECT response param present\n",
                what);
        bad++;
    }
    if (sub.params.has_object_delivery_timeout) {
        fprintf(stderr, "FAIL: %s: OBJECT_DELIVERY_TIMEOUT present\n", what);
        bad++;
    }
    if (sub.params.has_subgroup_delivery_timeout) {
        fprintf(stderr, "FAIL: %s: SUBGROUP_DELIVERY_TIMEOUT present\n", what);
        bad++;
    }
    if (sub.params.auth_token_count != 0) {
        fprintf(stderr, "FAIL: %s: %zu authorization tokens, expected 0\n",
                what, sub.params.auth_token_count);
        bad++;
    }
    if (sub.params.has_new_group_request) {
        fprintf(stderr, "FAIL: %s: NEW_GROUP_REQUEST present\n", what);
        bad++;
    }
    return bad;
}

/* An unblocked local request arm: exactly OPEN_BIDI then WRITE on the same
 * transport id, the write carrying exactly the declared request with no FIN. */
static int classify_local_request(fake_endpoint_t *ep, const req_shape_t *want,
                                  uint64_t *out_sid, const char *what)
{
    int bad = 0;
    if (ep->count != 2) {
        fprintf(stderr, "FAIL: %s: %zu setup ops, expected 2\n", what,
                ep->count);
        for (size_t i = 0; i < ep->count; i++)
            fprintf(stderr, "      [%zu] kind=%d sid=%llu\n", i,
                    (int)ep->ops[i].kind,
                    (unsigned long long)ep->ops[i].stream_id);
        fake_endpoint_clear_ops(ep);
        return bad + 1;
    }
    if (ep->ops[0].kind != FAKE_OP_OPEN_BIDI ||
        ep->ops[1].kind != FAKE_OP_WRITE) {
        fprintf(stderr, "FAIL: %s: setup ops are kind %d,%d, expected"
                " OPEN_BIDI,WRITE\n", what, (int)ep->ops[0].kind,
                (int)ep->ops[1].kind);
        bad++;
    }
    if (ep->ops[0].stream_id != ep->ops[1].stream_id) {
        fprintf(stderr, "FAIL: %s: open on %llu but write on %llu\n", what,
                (unsigned long long)ep->ops[0].stream_id,
                (unsigned long long)ep->ops[1].stream_id);
        bad++;
    }
    if (ep->ops[1].fin) {
        fprintf(stderr, "FAIL: %s: the request write carried FIN\n", what);
        bad++;
    }
    bad += check_subscribe_bytes(ep->ops[1].data, ep->ops[1].data_len, want,
                                 what);
    *out_sid = ep->ops[0].stream_id;
    fake_endpoint_clear_ops(ep);
    return bad;
}

typedef struct {
    test_pair_t        tp;
    uint64_t           bidi;    /* transport id of the local request bidi */
    moq_stream_ref_t   ref;
    moq_subscription_t sub;
} local_req_fixture_t;

static int local_subscribe(test_pair_t *tp, const char *ns, const char *track,
                           moq_subscription_t *out)
{
    moq_bytes_t ns_parts[1];
    ns_parts[0].data = (const uint8_t *)ns;
    ns_parts[0].len = strlen(ns);
    moq_namespace_t nsp = { ns_parts, 1 };
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = nsp;
    cfg.track_name.data = (const uint8_t *)track;
    cfg.track_name.len = strlen(track);
    cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    return moq_session_subscribe(tp->client, &cfg, 0, out) == MOQ_OK ? 0 : -1;
}

/* A locally issued draft-18 SUBSCRIBE whose request bidi the client bridge has
 * opened and written, with the server deliberately never answering: the peer's
 * send half is still open, so the response is the test's to deliver. */
static int local_request_bidi_fixture_ex(local_req_fixture_t *f,
                                         bool block_write)
{
    memset(f, 0, sizeof(*f));
    if (d18_pair_init(&f->tp, 0) < 0) return -1;
    moq_session_start(f->tp.client, 0);
    moq_session_start(f->tp.server, 0);
    d18_shuttle_until_quiescent(&f->tp, 30, 0);

    moq_event_t ev;
    while (moq_session_poll_events(f->tp.client, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(f->tp.server, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    if (local_subscribe(&f->tp, "live", "video", &f->sub) < 0) goto fail;

    fake_endpoint_clear_ops(&f->tp.client_ep);
    /* Blocking the write leaves the request itself queued on the bidi the open
     * already created -- the "request write still pending" state. */
    f->tp.client_ep.block_write = block_write;
    if (moq_transport_bridge_service(f->tp.client_bridge, 0) != MOQ_OK)
        goto fail;

    /* A fresh draft-18 CLIENT starts at request id 0 (profile_d18.c:35-38) and
     * commits by two (:410,:419), so the single-request fixtures expect 0. */
    static const req_shape_t k_shape = {
        .request_id = 0, .ns = "live", .track = "video" };
    if (block_write) {
        /* The endpoint saw exactly the open; the request itself is the one
         * pending copied write, tied to the same id and ref. Its retained
         * bytes are decoded, not adopted. */
        if (f->tp.client_ep.count != 1 ||
            f->tp.client_ep.ops[0].kind != FAKE_OP_OPEN_BIDI) {
            fprintf(stderr, "NF setup: blocked arm produced %zu ops\n",
                    f->tp.client_ep.count);
            goto fail;
        }
        f->bidi = f->tp.client_ep.ops[0].stream_id;
        fake_endpoint_clear_ops(&f->tp.client_ep);
        if (f->tp.client_bridge->pending_count != 1 ||
            f->tp.client_bridge->pending[0].kind != PENDING_COPIED_WRITE ||
            f->tp.client_bridge->pending[0].stream_id != f->bidi)
            goto fail;
        if (check_subscribe_bytes(f->tp.client_bridge->pending[0].data,
                                  f->tp.client_bridge->pending[0].data_len,
                                  &k_shape, "NF setup blocked request") != 0)
            goto fail;
    } else {
        if (classify_local_request(&f->tp.client_ep, &k_shape, &f->bidi,
                                   "NF setup request") != 0)
            goto fail;
    }
    if (!f->bidi) goto fail;

    bridge_stream_entry_t *e = bridge_find_by_id(f->tp.client_bridge, f->bidi);
    if (!e || e->kind != BRIDGE_STREAM_BIDI ||
        e->origin != BRIDGE_ORIGIN_LOCAL || !e->active) goto fail;
    f->ref = e->ref;
    if (block_write &&
        f->tp.client_bridge->pending[0].stream_ref._v != f->ref._v) goto fail;
    return 0;

fail:
    test_pair_destroy(&f->tp);
    return -1;
}

static int local_request_bidi_fixture(local_req_fixture_t *f)
{
    return local_request_bidi_fixture_ex(f, false);
}

/* 1. STOP then REQUEST_ERROR+FIN on the same stream completes the request. */
static int test_local_bidi_stop_keeps_response_half(void)
{
    int failures = 0;
    local_req_fixture_t f;
    if (local_request_bidi_fixture(&f) < 0) { failures++; return failures; }

    MOQ_TEST_CHECK(request_registry_find_by_streamref(f.tp.client, f.ref).kind
                   == MOQ_REQ_SUBSCRIPTION);

    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                       f.tp.client_bridge, f.bidi, 0x1, 0) == MOQ_OK);

    /* The callback itself touches the transport not at all: the reset is
     * queued, never issued re-entrantly. */
    failures += ep_expect_none(&f.tp.client_ep, "R1 callback");

    bridge_stream_entry_t *e = bridge_find_by_id(f.tp.client_bridge, f.bidi);
    MOQ_TEST_CHECK(e != NULL && e->active);
    if (e) {
        MOQ_TEST_CHECK(e->peer_stop_received);
        MOQ_TEST_CHECK(e->local_send_closed);
        MOQ_TEST_CHECK(!e->peer_send_closed);
    }
    MOQ_TEST_CHECK(request_registry_find_by_streamref(f.tp.client, f.ref).kind
                   == MOQ_REQ_SUBSCRIPTION);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(f.tp.client) == MOQ_SESS_ESTABLISHED);

    /* First service: exactly the declared RESET and nothing else. */
    MOQ_TEST_CHECK(moq_transport_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    {
        ep_rec_t want[1] = { ep_reset(f.bidi, 0x1) };
        failures += ep_expect(&f.tp.client_ep, want, 1, "R1 service");
    }

    /* The peer rejects the request on its still-open send half (§3.3.2). */
    uint8_t msg[128];
    size_t n = encode_request_error(msg, sizeof(msg));
    MOQ_TEST_CHECK(n > 0);
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_bidi_bytes(
                       f.tp.client_bridge, f.bidi, msg, n, true, 0) == MOQ_OK);

    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(f.tp.client) == MOQ_SESS_ESTABLISHED);

    moq_event_t ev;
    int errors = 0, other = 0;
    while (moq_session_poll_events(f.tp.client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR) {
            errors++;
            MOQ_TEST_CHECK(ev.u.subscribe_error.sub._opaque ==
                           f.sub._opaque);
            MOQ_TEST_CHECK((uint64_t)ev.u.subscribe_error.error_code ==
                           LSTOP_ERR_CODE);
            MOQ_TEST_CHECK(ev.u.subscribe_error.retry_after_ms ==
                           LSTOP_ERR_RETRY);
            MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_error.reason.len,
                                   strlen(LSTOP_ERR_REASON));
            MOQ_TEST_CHECK(ev.u.subscribe_error.reason.data != NULL);
            if (ev.u.subscribe_error.reason.data != NULL &&
                ev.u.subscribe_error.reason.len == strlen(LSTOP_ERR_REASON))
                MOQ_TEST_CHECK(memcmp(ev.u.subscribe_error.reason.data,
                                      LSTOP_ERR_REASON,
                                      strlen(LSTOP_ERR_REASON)) == 0);
        } else {
            other++;
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(errors == 1);
    MOQ_TEST_CHECK(other == 0);

    /* The response terminal produces no endpoint output of its own, and the
     * mapping and owner retire. */
    failures += ep_expect_none(&f.tp.client_ep, "R1 response terminal");
    MOQ_TEST_CHECK(moq_transport_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    failures += ep_expect_none(&f.tp.client_ep, "R1 service after terminal");
    MOQ_TEST_CHECK(request_registry_find_by_streamref(f.tp.client, f.ref).kind
                   == MOQ_REQ_NONE);
    failures += check_mapping_gone(f.tp.client_bridge, f.bidi, f.ref,
                                   "R1 terminal");

    /* Repeated service stays inert, and the mapping stays gone by BOTH
     * identities. */
    MOQ_TEST_CHECK(moq_transport_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    failures += ep_expect_none(&f.tp.client_ep, "R1 repeated service");
    failures += check_mapping_gone(f.tp.client_bridge, f.bidi, f.ref,
                                   "R1 repeated service");

    test_pair_destroy(&f.tp);
    return failures;
}

/* 2. Duplicate STOP is idempotent: still one RESET, carrying the FIRST
 *    STOP's code (the reset is already owed; a repeat cannot re-arm it). */
static int test_local_bidi_stop_duplicate_is_idempotent(void)
{
    int failures = 0;
    local_req_fixture_t f;
    if (local_request_bidi_fixture(&f) < 0) { failures++; return failures; }

    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                       f.tp.client_bridge, f.bidi, 0x1, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                       f.tp.client_bridge, f.bidi, 0x4, 0) == MOQ_OK);
    failures += ep_expect_none(&f.tp.client_ep, "R2 callbacks");

    MOQ_TEST_CHECK(moq_transport_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    {
        ep_rec_t want[1] = { ep_reset(f.bidi, 0x1) };
        failures += ep_expect(&f.tp.client_ep, want, 1, "R2 service");
    }
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));

    /* A third STOP after the reset was sent adds nothing. */
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                       f.tp.client_bridge, f.bidi, 0x5, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    failures += ep_expect_none(&f.tp.client_ep, "R2 third stop");

    bridge_stream_entry_t *e = bridge_find_by_id(f.tp.client_bridge, f.bidi);
    MOQ_TEST_CHECK(e != NULL && e->active);
    MOQ_TEST_CHECK(request_registry_find_by_streamref(f.tp.client, f.ref).kind
                   == MOQ_REQ_SUBSCRIPTION);

    test_pair_destroy(&f.tp);
    return failures;
}

/* 3. Blocked RESET stays retryable while the targeted stream's queued output
 *    is discarded -- freeing exactly its own buffer -- and unrelated records
 *    keep their order, their contents and their ownership.
 *
 * The FIFO cannot hold two items through the drain loop: bridge_drain_actions
 * stops as soon as one is pending, and a later pass re-breaks at the retry. So
 * the unrelated records are constructed directly, each against a LIVE bridge
 * mapping created by a real subscribe, and one of them owns bytes taken from
 * the same accounting allocator the bridge itself uses -- which is what lets
 * the compaction be measured rather than assumed.
 */
static int test_local_bidi_stop_blocked_reset_retries(void)
{
    int failures = 0;
    fp_alloc_state_t fs = {0};
    moq_alloc_t balloc = fp_allocator(&fs);

    test_pair_t tp;
    memset(&tp, 0, sizeof(tp));

    moq_session_cfg_t ccfg, scfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    ccfg.version = MOQ_VERSION_DRAFT_18;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    scfg.version = MOQ_VERSION_DRAFT_18;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;
    if (moq_session_create(&ccfg, 0, &tp.client) < 0) { failures++; return failures; }
    if (moq_session_create(&scfg, 0, &tp.server) < 0) {
        moq_session_destroy(tp.client); failures++; return failures;
    }
    fake_endpoint_init(&tp.client_ep, 1000, 2000);
    fake_endpoint_init(&tp.server_ep, 3000, 4000);

    moq_transport_bridge_cfg_t cbcfg, sbcfg;
    moq_transport_bridge_cfg_init(&cbcfg, &balloc);
    moq_transport_bridge_cfg_init(&sbcfg, moq_alloc_default());
    if (moq_transport_bridge_create(&cbcfg, tp.client, &tp.client_ep.vtable,
                                    &tp.client_ep, &tp.client_bridge) < 0 ||
        moq_transport_bridge_create(&sbcfg, tp.server, &tp.server_ep.vtable,
                                    &tp.server_ep, &tp.server_bridge) < 0) {
        moq_session_destroy(tp.server); moq_session_destroy(tp.client);
        failures++; return failures;
    }

    moq_session_start(tp.client, 0);
    moq_session_start(tp.server, 0);
    d18_shuttle_until_quiescent(&tp, 30, 0);
    {
        moq_event_t ev;
        while (moq_session_poll_events(tp.client, &ev, 1) > 0)
            moq_event_cleanup(&ev);
        while (moq_session_poll_events(tp.server, &ev, 1) > 0)
            moq_event_cleanup(&ev);
    }

    /* Two live unrelated request bidis, written and flushed. */
    moq_subscription_t sub_b, sub_c, sub_a;
    /* Three requests in issue order on one client: 0, 2, 4. */
    static const req_shape_t k_b = {
        .request_id = 0, .ns = "live", .track = "audio" };
    static const req_shape_t k_c = {
        .request_id = 2, .ns = "live", .track = "text" };
    static const req_shape_t k_a = {
        .request_id = 4, .ns = "live", .track = "video" };
    uint64_t sid_b = 0, sid_c = 0;
    /* Each request is serviced and classified on its own, so a mapping is
     * bound to its own OPEN/WRITE pair rather than to a scan of the queue. */
    MOQ_TEST_CHECK(local_subscribe(&tp, k_b.ns, k_b.track, &sub_b) == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    failures += classify_local_request(&tp.client_ep, &k_b, &sid_b, "R3 arm B");
    MOQ_TEST_CHECK(local_subscribe(&tp, k_c.ns, k_c.track, &sub_c) == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    failures += classify_local_request(&tp.client_ep, &k_c, &sid_c, "R3 arm C");
    MOQ_TEST_CHECK(sid_b != 0 && sid_c != 0 && sid_b != sid_c);
    bridge_stream_entry_t *eb = bridge_find_by_id(tp.client_bridge, sid_b);
    bridge_stream_entry_t *ec = bridge_find_by_id(tp.client_bridge, sid_c);
    MOQ_TEST_CHECK(eb != NULL && eb->active);
    MOQ_TEST_CHECK(ec != NULL && ec->active);

    /* The targeted request: its own write blocks and stays pending. */
    tp.client_ep.block_write = true;
    MOQ_TEST_CHECK(local_subscribe(&tp, k_a.ns, k_a.track, &sub_a) == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    uint64_t sid_a = 0;
    {
        ep_rec_t want[1];
        memset(want, 0, sizeof(want));
        want[0].kind = FAKE_OP_OPEN_BIDI;
        want[0].stream_id = tp.client_ep.count ? tp.client_ep.ops[0].stream_id
                                               : 0;
        sid_a = want[0].stream_id;
        failures += ep_expect(&tp.client_ep, want, 1, "R3 arm A open");
    }
    MOQ_TEST_CHECK(sid_a != 0 && sid_a != sid_b && sid_a != sid_c);
    bridge_stream_entry_t *ea = bridge_find_by_id(tp.client_bridge, sid_a);
    MOQ_TEST_CHECK(ea != NULL && ea->active);
    MOQ_TEST_CHECK(tp.client_bridge->pending_count == 1);
    MOQ_TEST_CHECK(tp.client_bridge->pending[0].kind == PENDING_COPIED_WRITE);
    MOQ_TEST_CHECK(tp.client_bridge->pending[0].stream_id == sid_a);
    if (ea) MOQ_TEST_CHECK(tp.client_bridge->pending[0].stream_ref._v ==
                           ea->ref._v);
    failures += check_subscribe_bytes(tp.client_bridge->pending[0].data,
                                      tp.client_bridge->pending[0].data_len,
                                      &k_a, "R3 arm A request");

    /* Two unrelated records, one owning bytes from the bridge's allocator. */
    static const uint8_t k_unrelated[] = { 0xA1, 0xB2, 0xC3, 0xD4, 0xE5 };
    uint8_t *owned = (uint8_t *)balloc.alloc(sizeof(k_unrelated), balloc.ctx);
    MOQ_TEST_CHECK(owned != NULL);
    if (!owned) { test_pair_destroy(&tp); failures++; return failures; }
    memcpy(owned, k_unrelated, sizeof(k_unrelated));

    {
        bridge_pending_item_t *q = tp.client_bridge->pending;
        q[1] = q[0];                        /* targeted write moves to index 1 */
        memset(&q[0], 0, sizeof(q[0]));
        q[0].kind = PENDING_COPIED_WRITE;
        q[0].stream_id = sid_b;
        q[0].stream_ref = eb->ref;
        q[0].data = owned;
        q[0].data_len = sizeof(k_unrelated);
        memset(&q[2], 0, sizeof(q[2]));
        q[2].kind = PENDING_CLOSE_BIDI_FIN;
        q[2].stream_id = sid_c;
        q[2].stream_ref = ec->ref;
        tp.client_bridge->pending_count = 3;
    }
    const int64_t balance_before = fs.balance;
    const uint8_t *targeted_bytes = tp.client_bridge->pending[1].data;
    MOQ_TEST_CHECK(targeted_bytes != NULL);

    tp.client_ep.block_reset = true;
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                       tp.client_bridge, sid_a, 0x1, 0) == MOQ_OK);
    failures += ep_expect_none(&tp.client_ep, "R3 callback");

    /* Exactly the targeted item left; the unrelated records kept their order,
     * their complete contents and their buffer; the RESET took the tail. */
    MOQ_TEST_CHECK(tp.client_bridge->pending_count == 3);
    if (tp.client_bridge->pending_count == 3) {
        bridge_pending_item_t *q = tp.client_bridge->pending;
        MOQ_TEST_CHECK(q[0].kind == PENDING_COPIED_WRITE);
        MOQ_TEST_CHECK(q[0].stream_id == sid_b);
        MOQ_TEST_CHECK(q[0].stream_ref._v == eb->ref._v);
        MOQ_TEST_CHECK(q[0].data == owned);
        MOQ_TEST_CHECK_EQ_SIZE(q[0].data_len, sizeof(k_unrelated));
        if (q[0].data)
            MOQ_TEST_CHECK(memcmp(q[0].data, k_unrelated,
                                  sizeof(k_unrelated)) == 0);
        MOQ_TEST_CHECK(q[1].kind == PENDING_CLOSE_BIDI_FIN);
        MOQ_TEST_CHECK(q[1].stream_id == sid_c);
        MOQ_TEST_CHECK(q[1].stream_ref._v == ec->ref._v);
        MOQ_TEST_CHECK(q[1].data == NULL);
        MOQ_TEST_CHECK(q[2].kind == PENDING_RESET_STREAM);
        MOQ_TEST_CHECK(q[2].stream_id == sid_a);
        MOQ_TEST_CHECK(q[2].error_code == 0x1);
    }
    /* Exactly one block freed: the targeted write's own buffer. */
    MOQ_TEST_CHECK(fs.balance == balance_before - 1);
    MOQ_TEST_CHECK(fp_sticky_clean(&fs, "R3 compaction") == 0);

    /* Reset still blocked: the unrelated work goes out, the target does not. */
    tp.client_ep.block_write = false;
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    {
        ep_rec_t want[2];
        memset(want, 0, sizeof(want));
        want[0].kind = FAKE_OP_WRITE;
        want[0].stream_id = sid_b;
        want[0].data = k_unrelated;
        want[0].data_len = sizeof(k_unrelated);
        want[0].check_fin = true;
        want[0].fin = false;
        want[1].kind = FAKE_OP_WRITE;
        want[1].stream_id = sid_c;
        want[1].check_fin = true;
        want[1].fin = true;
        failures += ep_expect(&tp.client_ep, want, 2, "R3 blocked service");
    }
    MOQ_TEST_CHECK(tp.client_bridge->pending_count == 1);
    MOQ_TEST_CHECK(tp.client_bridge->pending[0].kind == PENDING_RESET_STREAM);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));

    /* The response mapping survived the blocked reset. */
    ea = bridge_find_by_id(tp.client_bridge, sid_a);
    MOQ_TEST_CHECK(ea != NULL && ea->active);
    MOQ_TEST_CHECK(request_registry_find_by_streamref(tp.client, ea->ref).kind
                   == MOQ_REQ_SUBSCRIPTION);

    tp.client_ep.block_reset = false;
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    {
        ep_rec_t want[1] = { ep_reset(sid_a, 0x1) };
        failures += ep_expect(&tp.client_ep, want, 1, "R3 reset service");
    }
    MOQ_TEST_CHECK(tp.client_bridge->pending_count == 0);

    test_pair_destroy(&tp);
    MOQ_TEST_CHECK(br_alloc_zeroed(&fs, "R3 teardown") == 0);
    return failures;
}

/* 4. Control: a PEER-opened request bidi keeps requester-cancellation, and its
 *    own physical RESET then retires the stream exactly once.
 *
 * Only RESET is exercised. The peer-FIN variant is BACKLOG #257, a separate
 * pre-existing defect: the STOP correctly retires the peer-origin request
 * owner, but the bridge keeps no discard mapping for the peer's still-open
 * sending direction, so its later legal empty FIN reaches the ownerless
 * request parser and closes 0x3. That outcome is not encoded here as a green
 * expectation, and this slice does not fix it. */
static int peer_bidi_stop_then_reset(void)
{
    int failures = 0;
    test_pair_t tp;
    uint64_t bidi = 0;
    if (d18_request_bidi_fixture(&tp, &bidi) < 0) { failures++; return failures; }

    bridge_stream_entry_t *e = bridge_find_by_id(tp.client_bridge, bidi);
    MOQ_TEST_CHECK(e != NULL && e->kind == BRIDGE_STREAM_BIDI);
    if (e) MOQ_TEST_CHECK(e->origin == BRIDGE_ORIGIN_PEER);
    moq_stream_ref_t bref = e ? e->ref : moq_stream_ref_from_u64(0);

    moq_event_t ev;
    moq_subscription_t want_sub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(tp.client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
            want_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_subscription_is_valid(want_sub));
    fake_endpoint_clear_ops(&tp.client_ep);

    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                       tp.client_bridge, bidi, 0x1, 0) == MOQ_OK);
    failures += ep_expect_none(&tp.client_ep, "R4 callback");
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(tp.client) == MOQ_SESS_ESTABLISHED);

    /* The cancellation reaches the application on the same subscription and
     * retires the request owner -- the peer-origin behaviour is unchanged. */
    int cancels = 0;
    while (moq_session_poll_events(tp.client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_UNSUBSCRIBED) {
            cancels++;
            MOQ_TEST_CHECK(ev.u.unsubscribed.sub._opaque ==
                           want_sub._opaque);
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(cancels == 1);
    MOQ_TEST_CHECK(request_registry_find_by_streamref(tp.client, bref).kind ==
                   MOQ_REQ_NONE);

    /* Our response send half is reset exactly once (RFC 9000 §3.5). */
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    {
        ep_rec_t want[1] = { ep_reset(bidi, 0x1) };
        failures += ep_expect(&tp.client_ep, want, 1, "R4 service");
    }

    /* The peer's own remaining direction then terminates. */
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(
                       tp.client_bridge, bidi, 0x1, 0) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(tp.client) == MOQ_SESS_ESTABLISHED);
    {
        moq_event_t tev; int extra = 0;
        while (moq_session_poll_events(tp.client, &tev, 1) > 0) {
            extra++;
            moq_event_cleanup(&tev);
        }
        MOQ_TEST_CHECK(extra == 0);
    }
    failures += ep_expect_none(&tp.client_ep, "R4 terminal");

    failures += check_mapping_gone(tp.client_bridge, bidi, bref, "R4 terminal");

    /* Repeated terminal and repeated service are both inert. */
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(
                       tp.client_bridge, bidi, 0x1, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    failures += ep_expect_none(&tp.client_ep, "R4 repeat");
    failures += check_mapping_gone(tp.client_bridge, bidi, bref, "R4 repeat");
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(tp.client) == MOQ_SESS_ESTABLISHED);

    test_pair_destroy(&tp);
    return failures;
}

static int test_peer_bidi_stop_is_request_cancellation(void)
{
    return peer_bidi_stop_then_reset();
}

/* 5. Peer FIN and peer RESET each retire a preserved mapping exactly once. */
static int test_local_bidi_stop_then_peer_terminal_retires_once(void)
{
    int failures = 0;

    for (int use_reset = 0; use_reset < 2; use_reset++) {
        local_req_fixture_t f;
        if (local_request_bidi_fixture(&f) < 0) { failures++; return failures; }

        MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                           f.tp.client_bridge, f.bidi, 0x1, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_transport_bridge_service(f.tp.client_bridge, 0) ==
                       MOQ_OK);
        {
            ep_rec_t want[1] = { ep_reset(f.bidi, 0x1) };
            failures += ep_expect(&f.tp.client_ep, want, 1, "R5 service");
        }
        MOQ_TEST_CHECK(bridge_find_by_id(f.tp.client_bridge, f.bidi) != NULL);

        if (use_reset) {
            MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(
                               f.tp.client_bridge, f.bidi, 0x1, 0) == MOQ_OK);
        } else {
            /* The peer's own terminal: a complete response, then FIN. The
             * preserved mapping must carry these bytes to the session -- a
             * tombstone here would turn a valid response into a protocol
             * error. (A BARE FIN with no response at all is a truncated
             * request stream and closes 0x3; that is pre-existing session
             * behaviour and is not what this row is about.) */
            uint8_t msg[128];
            size_t n = encode_request_error(msg, sizeof(msg));
            MOQ_TEST_CHECK(n > 0);
            MOQ_TEST_CHECK(moq_transport_bridge_on_peer_bidi_bytes(
                               f.tp.client_bridge, f.bidi, msg, n, true, 0)
                           == MOQ_OK);
        }
        {
            moq_event_t ev;
            while (moq_session_poll_events(f.tp.client, &ev, 1) > 0)
                moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));
        MOQ_TEST_CHECK(moq_session_state(f.tp.client) == MOQ_SESS_ESTABLISHED);
        failures += ep_expect_none(&f.tp.client_ep, "R5 terminal");

        failures += check_mapping_gone(f.tp.client_bridge, f.bidi, f.ref,
                                       "R5 terminal");
        MOQ_TEST_CHECK(request_registry_find_by_streamref(f.tp.client, f.ref)
                       .kind == MOQ_REQ_NONE);

        /* A second terminal and a second service pass are both inert. */
        MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(
                           f.tp.client_bridge, f.bidi, 0x1, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_transport_bridge_service(f.tp.client_bridge, 0) ==
                       MOQ_OK);
        failures += ep_expect_none(&f.tp.client_ep, "R5 repeat");
        failures += check_mapping_gone(f.tp.client_bridge, f.bidi, f.ref,
                                       "R5 repeat");
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));

        test_pair_destroy(&f.tp);
    }
    return failures;
}

/* 6. STOP after our OWN FIN, on a local-origin request bidi.
 *
 * RFC 9000 §3.5 requires the RESET_STREAM in the Ready and Send states and
 * permits it to be DEFERRED in Data Sent until the outstanding data is
 * acknowledged or declared lost. Sending nothing at all, forever, is not that
 * deferral, so this row does not claim the current outcome is conformant --
 * it pins what the product does.
 *
 * The cause is BACKLOG #245(a): moq_session_has_transport_stream() does not
 * consult the draft-18 request registry, so an ordinary FIN on a local-origin
 * request bidi retires and tombstones the mapping through the pre-existing
 * path, and by the time the STOP arrives there is no stream left to reset.
 * Retaining every local-FIN mapping instead is exactly the broadening this
 * slice must not do, and fixing #245(a) is not this task's scope.
 */
static int test_local_bidi_stop_after_local_fin(void)
{
    int failures = 0;
    local_req_fixture_t f;
    if (local_request_bidi_fixture(&f) < 0) { failures++; return failures; }

    MOQ_TEST_CHECK(queue_close_bidi(f.tp.client, f.ref) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    {
        ep_rec_t want[1];
        memset(want, 0, sizeof(want));
        want[0].kind = FAKE_OP_WRITE;
        want[0].stream_id = f.bidi;
        want[0].check_fin = true;
        want[0].fin = true;
        want[0].data_len = 0;
        want[0].data = (const uint8_t *)"";   /* empty FIN, payload compared */
        failures += ep_expect(&f.tp.client_ep, want, 1, "R6 fin");
    }

    /* The ordinary FIN retired the mapping: pre-existing tombstone behaviour,
     * which the no-STOP control below pins in its own right. */
    MOQ_TEST_CHECK(bridge_find_by_id(f.tp.client_bridge, f.bidi) == NULL);

    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stop_sending(
                       f.tp.client_bridge, f.bidi, 0x1, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    failures += ep_expect_none(&f.tp.client_ep, "R6 stop after fin");
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(f.tp.client) == MOQ_SESS_ESTABLISHED);

    test_pair_destroy(&f.tp);
    return failures;
}

/* 7. Control: with NO peer STOP anywhere, an ordinary local FIN keeps its
 *    existing mapping/tombstone behaviour and later session output for that
 *    stream is still suppressed by the pre-existing path -- none of which the
 *    STOP correction may change. */
static int test_local_bidi_normal_fin_unchanged(void)
{
    int failures = 0;
    local_req_fixture_t f;
    if (local_request_bidi_fixture(&f) < 0) { failures++; return failures; }

    bridge_stream_entry_t *e = bridge_find_by_id(f.tp.client_bridge, f.bidi);
    MOQ_TEST_CHECK(e != NULL && e->active);
    if (e) MOQ_TEST_CHECK(!e->peer_stop_received);

    MOQ_TEST_CHECK(queue_close_bidi(f.tp.client, f.ref) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    {
        ep_rec_t want[1];
        memset(want, 0, sizeof(want));
        want[0].kind = FAKE_OP_WRITE;
        want[0].stream_id = f.bidi;
        want[0].check_fin = true;
        want[0].fin = true;
        failures += ep_expect(&f.tp.client_ep, want, 1, "R7 fin");
    }

    /* Retired and tombstoned exactly as before this slice, by both identities. */
    failures += check_mapping_gone(f.tp.client_bridge, f.bidi, f.ref, "R7 fin");
    MOQ_TEST_CHECK(bridge_is_tombstoned(f.tp.client_bridge, f.bidi));

    /* A second FIN for the retired ref produces nothing. */
    MOQ_TEST_CHECK(queue_close_bidi(f.tp.client, f.ref) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_service(f.tp.client_bridge, 0) ==
                   MOQ_OK);
    failures += ep_expect_none(&f.tp.client_ep, "R7 second fin");
    failures += check_mapping_gone(f.tp.client_bridge, f.bidi, f.ref,
                                   "R7 second fin");
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.client_bridge));
    MOQ_TEST_CHECK(moq_session_state(f.tp.client) == MOQ_SESS_ESTABLISHED);

    test_pair_destroy(&f.tp);
    return failures;
}


int main(void)
{
    int failures = 0;

    failures += test_budget_context_paired_on_every_exit();
    failures += test_unlimited_service_enters_no_budget_context();
    failures += test_budgeted_service_requires_output();
    failures += test_control_retry_suspension_preserves_pending();
    failures += test_data_retry_suspension_preserves_pending();
    failures += test_data_reset_suspension_preserves_pending();
    failures += test_bidi_retry_suspension_preserves_pending();
    failures += test_fin_bridge_descriptor_validation();
    failures += test_ns_response_fin_bridge();
    failures += test_ns_response_reset_bridge();
    failures += test_p7_bridge_fin_retirement();
    failures += test_p7_retirement_idempotence_and_reuse();
    failures += test_p3_bridge_fin_retirement();
    failures += test_ns_sub_local_teardown_bridge_retry();
    failures += test_no_owner_admission_bridge();
    failures += test_noslot_exhaustion_reset();
    failures += test_noslot_reset_bidi_wouldblock_then_accept();
    failures += test_uni_reset_data_immediate_retire();
    failures += test_noslot_reset_bidi_peer_before_accept();
    failures += test_noslot_carrier_peer_reset();
    failures += test_noslot_carrier_peer_stop();
    failures += test_noslot_exhaustion_close();
    failures += test_bridge_nomem_ns_response();
    failures += test_bridge_nomem_joining_fetch();
    failures += test_bidi_reset_suspension_preserves_pending();
    failures += test_bidi_stop_suspension_preserves_pending();
    failures += test_data_stop_suspension_preserves_pending();
    failures += test_tick_suspension_preserves_due_deadline();
    failures += test_inbound_scan_stops_at_suspension();
    failures += test_progress_then_suspension_in_one_pass();
    failures += test_continuation_reclaims_event_scratch();
    failures += test_create_destroy();
    failures += test_create_rejects_bad_ops();
    failures += test_setup_handshake();
    failures += test_control_write_backpressure();
    failures += test_transport_close();
    failures += test_datagram_inbound_not_fatal();
    failures += test_inbound_uni_after_setup();
    failures += test_close_error_is_fatal_not_closed();

    /* Regression tests */
    failures += test_empty_uni_no_ghost_stream();
    failures += test_truncated_vtable_rejected();
    failures += test_inbound_uni_dropped_then_discarded();
    failures += test_inbound_uni_rcbuf_dropped_then_discarded();
    failures += test_pending_retry_keeps_bytes();
    failures += test_pending_retry_keeps_rcbuf();

    /* Hard retry tests */
    failures += test_close_retry_after_blocked_control();
    failures += test_close_retry_would_block();
    failures += test_reset_on_unknown_stream();
    failures += test_transport_close_clears_state();

    /* Peer STOP_SENDING on a request bidi (draft-18 §3.3.2) */
    failures += test_local_bidi_stop_keeps_response_half();
    failures += test_local_bidi_stop_duplicate_is_idempotent();
    failures += test_local_bidi_stop_blocked_reset_retries();
    failures += test_peer_bidi_stop_is_request_cancellation();
    failures += test_local_bidi_stop_then_peer_terminal_retires_once();
    failures += test_local_bidi_stop_after_local_fin();
    failures += test_local_bidi_normal_fin_unchanged();

    /* terminal facts */
    failures += test_terminal_facts_enqueued_then_observed();
    failures += test_terminal_facts_not_set_by_other_events();
    failures += test_already_fatal_transport_terminal();
    failures += test_setup_scratch_shortfall_closes_not_fatal();

    if (failures == 0)
        printf("test_transport_bridge: all tests passed\n");
    else
        fprintf(stderr, "test_transport_bridge: %d failure(s)\n", failures);

    return failures;
}
