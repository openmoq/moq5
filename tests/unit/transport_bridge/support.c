#include "test_cases.h"

int og_quiet;
int txs_quiet;
int nf_quiet;
int fp_quiet;
held_bridge_driver_t held_bridge_drivers[16];

/* When set, the next pair gives the SERVER session this allocator, so a
 * fixture can account for memory the server side owns. */
const moq_alloc_t *g_server_alloc_override;

uint32_t g_client_rx_capacity;


int test_pair_init_full(test_pair_t *tp, uint32_t client_max_events,
                        bool client_streaming,
                        uint32_t client_max_actions,
                        uint64_t client_idle_us,
                        uint32_t server_max_actions)
{
    memset(tp, 0, sizeof(*tp));

    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;
    if (g_client_rx_capacity) ccfg.max_data_streams = g_client_rx_capacity;
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
    moq_session_cfg_init_sized(&scfg, sizeof(scfg),
                               g_server_alloc_override != NULL
                                   ? g_server_alloc_override
                                   : moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;
    /* The publisher's action queue. Raising it above the bridge's drain
     * budget is what lets a fixture reach the budget at all. 0 = default. */
    if (server_max_actions) scfg.max_actions = server_max_actions;

    if (moq_session_create(&ccfg, 0, &tp->client) < 0) return -1;
    if (moq_session_create(&scfg, 0, &tp->server) < 0) {
        moq_session_destroy(tp->client);
        return -1;
    }

    held_endpoint_init(&tp->client_ep, 1000, 2000);
    held_endpoint_init(&tp->server_ep, 3000, 4000);

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());

    if (held_bridge_create(&bcfg, tp->client,
            &tp->client_ep.vtable, &tp->client_ep,
            &tp->client_bridge) < 0) {
        moq_session_destroy(tp->server);
        moq_session_destroy(tp->client);
        return -1;
    }
    if (held_bridge_create(&bcfg, tp->server,
            &tp->server_ep.vtable, &tp->server_ep,
            &tp->server_bridge) < 0) {
        held_bridge_destroy(tp->client_bridge);
        moq_session_destroy(tp->server);
        moq_session_destroy(tp->client);
        return -1;
    }

    return 0;
}


int test_pair_init_stream(test_pair_t *tp, uint32_t client_max_events,
                          bool client_streaming)
{
    return test_pair_init_full(tp, client_max_events, client_streaming, 0, 0, 0);
}


/* Same pair, with the PUBLISHER's action queue raised. */
int test_pair_init_pub_cap(test_pair_t *tp, uint32_t server_max_actions)
{
    return test_pair_init_full(tp, 1, false, 0, 0, server_max_actions);
}


int test_pair_init_ex(test_pair_t *tp, uint32_t client_max_events)
{
    return test_pair_init_stream(tp, client_max_events, false);
}


int test_pair_init(test_pair_t *tp)
{
    return test_pair_init_ex(tp, 0);
}


void test_pair_destroy(test_pair_t *tp)
{
    held_bridge_destroy(tp->client_bridge);
    held_bridge_destroy(tp->server_bridge);
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

    held_bridge_service(tp->client_bridge, now);

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
                    held_bridge_uni_bytes(
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
                held_bridge_close(
                    tp->server_bridge, o->error_code, now);
            delivered++;
            break;
        default:
            delivered++;
            break;
        }
    }
    fake_endpoint_clear_ops(&tp->client_ep);

    held_bridge_service(tp->server_bridge, now);

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


int pump_until_quiescent(test_pair_t *tp, int max, uint64_t now)
{
    for (int i = 0; i < max; i++) {
        if (pump_once(tp, now) == 0) return i;
    }
    return max;
}


bool setup_handshake(test_pair_t *tp)
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
int d18_pair_init_caps(test_pair_t *tp, uint32_t client_max_events,
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

    held_endpoint_init(&tp->client_ep, 1000, 2000);
    held_endpoint_init(&tp->server_ep, 3000, 4000);
    /* The bridge RETAINS the vtable pointer it is handed
     * (transport_bridge.c:108) rather than copying the table, so an op may be
     * installed after init. It is done here, before bridge creation, so the
     * endpoint's capability set is fixed for the whole run. */
    if (server_native_abort) fake_endpoint_enable_abort(&tp->server_ep);

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    if (held_bridge_create(&bcfg, tp->client, &tp->client_ep.vtable,
                                    &tp->client_ep, &tp->client_bridge) < 0) {
        moq_session_destroy(tp->server);
        moq_session_destroy(tp->client);
        return -1;
    }
    if (held_bridge_create(&bcfg, tp->server, &tp->server_ep.vtable,
                                    &tp->server_ep, &tp->server_bridge) < 0) {
        held_bridge_destroy(tp->client_bridge);
        moq_session_destroy(tp->server);
        moq_session_destroy(tp->client);
        return -1;
    }
    return 0;
}


int d18_pair_init_alloc(test_pair_t *tp, uint32_t client_max_events,
                        const moq_alloc_t *client_alloc,
                        const moq_alloc_t *server_alloc)
{
    return d18_pair_init_caps(tp, client_max_events, client_alloc,
                              server_alloc, 0, false, 0);
}


int d18_pair_init(test_pair_t *tp, uint32_t client_max_events)
{
    return d18_pair_init_alloc(tp, client_max_events, moq_alloc_default(),
                               moq_alloc_default());
}


void d18_feed(moq_transport_bridge_t *to, fake_endpoint_t *from,
              uint64_t uni_base, uint64_t bidi_base, uint64_t now,
              size_t *delivered)
{
    for (size_t i = 0; i < from->count; i++) {
        fake_op_t *o = &from->ops[i];
        if (o->kind != FAKE_OP_WRITE) { (*delivered)++; continue; }
        if (o->stream_id >= uni_base && o->stream_id < uni_base + 1000)
            held_bridge_uni_bytes(
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
    held_bridge_service(tp->client_bridge, now);
    d18_feed(tp->server_bridge, &tp->client_ep, 1000, 2000, now, &delivered);
    held_bridge_service(tp->server_bridge, now);
    d18_feed(tp->client_bridge, &tp->server_ep, 3000, 4000, now, &delivered);
    return delivered;
}


void d18_shuttle_until_quiescent(test_pair_t *tp, int max,
                                 uint64_t now)
{
    for (int i = 0; i < max; i++)
        if (d18_shuttle(tp, now) == 0) return;
}


/* The server issues the request, so the peer bidi lands on the CLIENT. */
moq_result_t d18_server_subscribe(test_pair_t *tp)
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


int uni_pending_fixture_init_ex(uni_pending_fixture_t *f,
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
    held_bridge_service(f->tp.server_bridge, 0);

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
    held_bridge_service(f->tp.server_bridge, 0);
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


int uni_pending_fixture_init(uni_pending_fixture_t *f)
{
    return uni_pending_fixture_init_ex(f, false, 3);
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
int test_pair_init_client_alloc(test_pair_t *tp,
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

    held_endpoint_init(&tp->client_ep, 1000, 2000);
    held_endpoint_init(&tp->server_ep, 3000, 4000);

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    if (held_bridge_create(&bcfg, tp->client,
            &tp->client_ep.vtable, &tp->client_ep,
            &tp->client_bridge) < 0) {
        moq_session_destroy(tp->server);
        moq_session_destroy(tp->client);
        return -1;
    }
    if (held_bridge_create(&bcfg, tp->server,
            &tp->server_ep.vtable, &tp->server_ep,
            &tp->server_bridge) < 0) {
        held_bridge_destroy(tp->client_bridge);
        moq_session_destroy(tp->server);
        moq_session_destroy(tp->client);
        return -1;
    }
    return 0;
}


/* The bridge-owned durable postcondition both cells assert. */
int br_fatal_postcondition(moq_transport_bridge_t *b,
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
int br_alloc_zeroed(const fp_alloc_state_t *fs, const char *op)
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
uint64_t br_ns_establish(test_pair_t *tp, int *failures_out)
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
    held_bridge_service(tp->client_bridge, 0);
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
    held_bridge_service(tp->server_bridge, 0);
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
size_t br_encode_d16_namespace(uint8_t *buf, size_t cap,
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

size_t encode_request_error(uint8_t *buf, size_t cap)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t reason = MOQ_BYTES_LITERAL(LSTOP_ERR_REASON);
    /* Wire Retry Interval = milliseconds + 1 (d18 §10.6). */
    if (moq_d18_encode_request_error(&w, LSTOP_ERR_CODE, LSTOP_ERR_RETRY + 1u,
                                     reason) != MOQ_OK)
        return 0;
    return moq_buf_writer_offset(&w);
}


int ep_expect(fake_endpoint_t *ep, const ep_rec_t *want, size_t n,
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


int ep_expect_none(fake_endpoint_t *ep, const char *what)
{
    return ep_expect(ep, NULL, 0, what);
}


/* A retired mapping must be unreachable through BOTH identities: an entry that
 * only lost one of them would still be found by the other lookup. */
int check_mapping_gone(moq_transport_bridge_t *b, uint64_t sid,
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


ep_rec_t ep_reset(uint64_t sid, uint64_t code)
{
    ep_rec_t r;
    memset(&r, 0, sizeof(r));
    r.kind = FAKE_OP_RESET;
    r.stream_id = sid;
    r.has_code = true;
    r.error_code = code;
    return r;
}
