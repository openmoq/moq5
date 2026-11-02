#include "test_cases.h"

/* Open a subgroup on `sub`, write one object and FIN it at `now`; service the
 * server bridge and return the transport stream id the fake endpoint opened. */
static uint64_t fin_one_subgroup(test_pair_t *tp, moq_subscription_t sub, uint64_t group, uint64_t now)
{
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = group;
    sc.publisher_priority = 19;
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(tp->server, sub, &sc, now, &sg) != MOQ_OK) return UINT64_MAX;
    moq_rcbuf_t *p = NULL;
    moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"o", 1, &p);
    moq_result_t wrc = moq_session_write_object(tp->server, sg, 0, p, now);
    moq_rcbuf_decref(p);
    if (wrc != MOQ_OK) return UINT64_MAX;
    if (moq_session_close_subgroup(tp->server, sg, now) != MOQ_OK) return UINT64_MAX;
    fake_endpoint_clear_ops(&tp->server_ep);
    held_bridge_service(tp->server_bridge, now);
    uint64_t sid = UINT64_MAX; bool fin = false;
    for (size_t i = 0; i < tp->server_ep.count; i++) {
        const fake_op_t *o = &tp->server_ep.ops[i];
        if (o->kind == FAKE_OP_OPEN_UNI) sid = o->stream_id;
        if (o->kind == FAKE_OP_WRITE && o->stream_id == sid && o->fin) fin = true;
    }
    return fin ? sid : UINT64_MAX;
}


/* With MOQ_TRANSPORT_CAP_COMMIT_REPORT a FIN'd local data stream stays
 * mapped until the adapter reports it committed, which stops the session's
 * FIN-started subgroup timer; when the timer fires first, its RESET reaches
 * the transport on that stream. Without the capability the stream is
 * retired as its FIN is written, and the report is a no-op. */
int test_local_uni_fin_awaits_commit(void)
{
    int failures = 0;
    test_pair_t tp;
    if (d22_pair_init(&tp) != 0) { MOQ_TEST_CHECK(false); return failures; }
    MOQ_TEST_CHECK(moq_session_start(tp.client, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_start(tp.server, 0) == MOQ_OK);
    priority_pump(&tp, true, 0);
    MOQ_TEST_CHECK(moq_session_state(tp.server) == MOQ_SESS_ESTABLISHED);
    tp.server_ep.vtable.capabilities |= MOQ_TRANSPORT_CAP_COMMIT_REPORT;

    moq_bytes_t part = MOQ_BYTES_LITERAL("timers");
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ &part, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t csub;
    MOQ_TEST_CHECK(moq_session_subscribe(tp.client, &cfg, 0, &csub) == MOQ_OK);
    priority_pump(&tp, true, 0);
    moq_event_t ev;
    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(tp.server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_subscription_is_valid(sub));
    /* SUBGROUP_DELIVERY_TIMEOUT 100 ms as a Track Property (type 0x06). */
    static const uint8_t props[] = { 0x06, 0x64 };
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    acc.track_properties = (moq_bytes_t){ props, sizeof(props) };
    MOQ_TEST_CHECK(moq_session_accept_subscribe(tp.server, sub, &acc, 0) == MOQ_OK);
    priority_pump(&tp, true, 0);
    fake_endpoint_clear_ops(&tp.server_ep);

    /* Committed before the timer: the mapping is released and no reset follows. */
    uint64_t sid = fin_one_subgroup(&tp, sub, 0, 0);
    MOQ_TEST_CHECK(sid != UINT64_MAX);
    bridge_stream_entry_t *e = bridge_find_by_id(tp.server_bridge, sid);
    MOQ_TEST_CHECK(e != NULL && e->local_send_closed && e->awaiting_commit);
    MOQ_TEST_CHECK(moq_session_next_deadline_us(tp.server) == 100000);
    MOQ_TEST_CHECK(moq_transport_bridge_on_local_stream_committed(tp.server_bridge, sid, 50000) == MOQ_OK);
    MOQ_TEST_CHECK(bridge_find_by_id(tp.server_bridge, sid) == NULL);
    MOQ_TEST_CHECK(moq_session_next_deadline_us(tp.server) == UINT64_MAX);
    MOQ_TEST_CHECK(moq_session_tick(tp.server, 200000) == MOQ_OK);
    fake_endpoint_clear_ops(&tp.server_ep);
    held_bridge_service(tp.server_bridge, 200000);
    MOQ_TEST_CHECK(fake_endpoint_count_kind(&tp.server_ep, FAKE_OP_RESET) == 0);
    /* A second report for the same stream changes nothing. */
    MOQ_TEST_CHECK(moq_transport_bridge_on_local_stream_committed(tp.server_bridge, sid, 200000) == MOQ_OK);

    /* The timer fires first: the stream is still mapped, so the RESET with
     * DELIVERY_TIMEOUT reaches the transport and retires it. */
    uint64_t sid2 = fin_one_subgroup(&tp, sub, 1, 300000);
    MOQ_TEST_CHECK(sid2 != UINT64_MAX && sid2 != sid);
    MOQ_TEST_CHECK(bridge_find_by_id(tp.server_bridge, sid2) != NULL);
    MOQ_TEST_CHECK(moq_session_tick(tp.server, 400000) == MOQ_OK);
    fake_endpoint_clear_ops(&tp.server_ep);
    held_bridge_service(tp.server_bridge, 400000);
    const fake_op_t *reset = fake_endpoint_find(&tp.server_ep, FAKE_OP_RESET);
    MOQ_TEST_CHECK(reset != NULL && reset->stream_id == sid2 &&
                   reset->error_code == MOQ_RESET_DELIVERY_TIMEOUT);
    MOQ_TEST_CHECK(bridge_find_by_id(tp.server_bridge, sid2) == NULL);

    /* Without the capability the FIN retires the stream at once. */
    tp.server_ep.vtable.capabilities &= ~(uint32_t)MOQ_TRANSPORT_CAP_COMMIT_REPORT;
    uint64_t sid3 = fin_one_subgroup(&tp, sub, 2, 500000);
    MOQ_TEST_CHECK(sid3 != UINT64_MAX);
    MOQ_TEST_CHECK(bridge_find_by_id(tp.server_bridge, sid3) == NULL);
    MOQ_TEST_CHECK(moq_transport_bridge_on_local_stream_committed(tp.server_bridge, sid3, 500000) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    test_pair_destroy(&tp);
    return failures;
}
