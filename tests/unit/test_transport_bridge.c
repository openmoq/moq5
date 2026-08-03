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
static int d18_pair_init_alloc(test_pair_t *tp, uint32_t client_max_events,
                               const moq_alloc_t *client_alloc,
                               const moq_alloc_t *server_alloc)
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

    if (moq_session_create(&ccfg, 0, &tp->client) < 0) return -1;
    if (moq_session_create(&scfg, 0, &tp->server) < 0) {
        moq_session_destroy(tp->client);
        return -1;
    }

    fake_endpoint_init(&tp->client_ep, 1000, 2000);
    fake_endpoint_init(&tp->server_ep, 3000, 4000);

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
        { FP_ALLOC, FP_SIZE_SAME_AS, 0, 0 },    /* stored key copy */
        { FP_ALLOC, FP_SIZE_ANY, 0, 0 },        /* key array (private) */
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

    /* Entry i committed. */
    MOQ_TEST_CHECK(!ei->pending_reset);
    MOQ_TEST_CHECK(!ei->active);
    MOQ_TEST_CHECK(!tp.client->rx_streams[rx_slot].active);
    MOQ_TEST_CHECK(se->processed_stream_count == 1);
    /* ...and its eager finalization blocked, leaving runnable sweep work. */
    MOQ_TEST_CHECK(se->done_pending);
    MOQ_TEST_CHECK(se->processed_stream_count >= se->done_stream_count);

    /* Entry j did not run. */
    MOQ_TEST_CHECK(ej->active);
    MOQ_TEST_CHECK(ej->pending_retry);

    MOQ_TEST_CHECK(out.suspended);
    MOQ_TEST_CHECK(out.sweep_spent == 0);
    MOQ_TEST_CHECK(session_budget_suspend_count - suspends == 1);
    MOQ_TEST_CHECK(tp.client->sweep_active);
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

    /* terminal facts */
    failures += test_terminal_facts_enqueued_then_observed();
    failures += test_terminal_facts_not_set_by_other_events();
    failures += test_setup_scratch_shortfall_closes_not_fatal();

    if (failures == 0)
        printf("test_transport_bridge: all tests passed\n");
    else
        fprintf(stderr, "test_transport_bridge: %d failure(s)\n", failures);

    return failures;
}
