#include "test_cases.h"
#include "../test_binding_support.h"

static unsigned priority_calls;

static uint32_t priority_key;

static uint64_t priority_sid;

static bool priority_fail;

static moq_transport_result_t record_priority(void *ctx, uint64_t sid, uint32_t key)
{
    (void)ctx;
    ++priority_calls;
    priority_key = key;
    priority_sid = sid;
    return priority_fail ? MOQ_TRANSPORT_ERROR : MOQ_TRANSPORT_OK;
}


void priority_pump(test_pair_t *tp, bool d18, uint64_t now)
{
    if (!d18) {
        pump_until_quiescent(tp, 20, now);
        return;
    }
    /* Response bytes keep the request originator's bidi ID, so either
     * endpoint can emit writes in either of the two fake bidi ranges. */
    for (unsigned round = 0; round < 20; ++round) {
        size_t count = 0;
        for (unsigned side = 0; side < 2; ++side) {
            fake_endpoint_t *from = side ? &tp->server_ep : &tp->client_ep;
            moq_transport_bridge_t *source = side ? tp->server_bridge : tp->client_bridge;
            moq_transport_bridge_t *dest = side ? tp->client_bridge : tp->server_bridge;
            held_bridge_service(source, now);
            count += from->count;
            for (size_t i = 0; i < from->count; ++i) {
                const fake_op_t *o = &from->ops[i];
                if (o->kind != FAKE_OP_WRITE) continue;
                bool bidi = (o->stream_id >= 2000 && o->stream_id < 3000) ||
                            (o->stream_id >= 4000 && o->stream_id < 5000);
                if (bidi)
                    moq_transport_bridge_on_peer_bidi_bytes(dest, o->stream_id,
                        o->data, o->data_len, o->fin, now);
                else
                    held_bridge_uni_bytes(dest, o->stream_id,
                        o->data, o->data_len, o->fin, now);
            }
            fake_endpoint_clear_ops(from);
        }
        if (!count) return;
    }
}


/* The stream's priority survives the subgroup's retirement and a blocked
 * open. With `update`, a subscriber priority update made meanwhile applies to
 * that not yet scheduled data (MOQT 7.1: "a best effort SHOULD be made to
 * apply the change to all objects that have not been scheduled"). */
int test_priority_survives_closed_subgroup(bool fail, bool legacy, bool d18,
                                           bool update_priority)
{
    int failures = 0;
    test_pair_t tp;
    int init = d18 ? d18_pair_init(&tp, 0) : test_pair_init_ex(&tp, 0);
    MOQ_TEST_CHECK(init == 0);
    if (init != 0) return failures;
    if (d18) {
        MOQ_TEST_CHECK(moq_session_start(tp.client, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(tp.server, 0) == MOQ_OK);
        priority_pump(&tp, d18, 0);
        MOQ_TEST_CHECK(moq_session_state(tp.client) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_state(tp.server) == MOQ_SESS_ESTABLISHED);
    } else {
        MOQ_TEST_CHECK(setup_handshake(&tp));
    }
    tp.server_ep.vtable.set_stream_priority = record_priority;
    if (legacy)
        tp.server_ep.vtable.struct_size =
            (uint32_t)offsetof(moq_transport_endpoint_ops_t, set_stream_priority);
    moq_bytes_t part = MOQ_BYTES_LITERAL("priority");
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ &part, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("audio");
    cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    cfg.has_subscriber_priority = true;
    cfg.subscriber_priority = 37;
    moq_subscription_t client_sub;
    MOQ_TEST_CHECK(moq_session_subscribe(tp.client, &cfg, 0, &client_sub) == MOQ_OK);
    priority_pump(&tp, d18, 0);
    moq_event_t ev;
    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(tp.server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_subscription_is_valid(sub));
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(tp.server, sub, &acc, 0) == MOQ_OK);
    priority_pump(&tp, d18, 0);
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.publisher_priority = 19;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_subgroup(tp.server, sub, &sc, 0, &sg) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_close_subgroup(tp.server, sg, 0) == MOQ_OK);
    /* Advance retires the subgroup before the bridge consumes queued actions. */
    moq_session_tick(tp.server, 1);
    priority_calls = 0;
    priority_fail = fail;
    uint64_t expected_sid = tp.server_ep.next_uni_id;
    unsigned opens_before = tp.server_ep.open_uni_calls;
    tp.server_ep.block_open_uni = true;
    held_bridge_service(tp.server_bridge, 1);
    MOQ_TEST_CHECK(priority_calls == 0);
    if (update_priority) {
        moq_subscription_update_cfg_t update;
        moq_subscription_update_cfg_init(&update);
        update.has_subscriber_priority = true;
        update.subscriber_priority = 0;
        moq_result_t update_result = moq_session_update_subscription(
            tp.client, client_sub, &update, 1);
        if (update_result != MOQ_OK)
            fprintf(stderr, "priority retained update: d18=%d rc=%d\n", d18, update_result);
        MOQ_TEST_CHECK(update_result == MOQ_OK);
        priority_pump(&tp, d18, 1);
        bool updated = false;
        while (moq_session_poll_events(tp.server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
                updated = true;
                MOQ_TEST_CHECK(ev.u.subscribe_updated.has_subscriber_priority);
                MOQ_TEST_CHECK(ev.u.subscribe_updated.subscriber_priority == 0);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(updated);
        MOQ_TEST_CHECK(priority_calls == 0);
    }
    tp.server_ep.block_open_uni = false;
    held_bridge_service(tp.server_bridge, 2);
    MOQ_TEST_CHECK(tp.server_ep.open_uni_calls == opens_before + 1);
    if (legacy) {
        /* A non-null poisoned tail is outside the old table's size. */
        MOQ_TEST_CHECK(priority_calls == 0);
    } else {
        MOQ_TEST_CHECK(priority_calls > 0);
        /* Sub 37 (0x25), or 0 once updated; pub 19 (0x13); position 0 (+1). */
        MOQ_TEST_CHECK(priority_key == (update_priority
                                            ? UINT32_C(0x00130001)
                                            : UINT32_C(0x25130001)));
        MOQ_TEST_CHECK(priority_sid == expected_sid);
    }
    MOQ_TEST_CHECK(moq_transport_bridge_is_fatal(tp.server_bridge) == (fail && !legacy));
    priority_fail = false;
    test_pair_destroy(&tp);
    return failures;
}


/* A draft-22 pair: the subgroup's delivery timer runs from its FIN and awaits
 * the transport's commit report. */
int d22_pair_init(test_pair_t *tp)
{
    memset(tp, 0, sizeof(*tp));
    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    ccfg.version = MOQ_VERSION_DRAFT_22;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    scfg.version = MOQ_VERSION_DRAFT_22;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;
    if (moq_session_create(&ccfg, 0, &tp->client) < 0) return -1;
    if (moq_session_create(&scfg, 0, &tp->server) < 0) { moq_session_destroy(tp->client); return -1; }
    if (test_bind_if_required(tp->client) < 0 || test_bind_if_required(tp->server) < 0) {
        moq_session_destroy(tp->server); moq_session_destroy(tp->client); return -1;
    }
    held_endpoint_init(&tp->client_ep, 1000, 2000);
    held_endpoint_init(&tp->server_ep, 3000, 4000);
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    if (held_bridge_create(&bcfg, tp->client, &tp->client_ep.vtable, &tp->client_ep, &tp->client_bridge) < 0) {
        moq_session_destroy(tp->server); moq_session_destroy(tp->client); return -1;
    }
    if (held_bridge_create(&bcfg, tp->server, &tp->server_ep.vtable, &tp->server_ep, &tp->server_bridge) < 0) {
        held_bridge_destroy(tp->client_bridge);
        moq_session_destroy(tp->server); moq_session_destroy(tp->client); return -1;
    }
    return 0;
}
