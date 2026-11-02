#include "test_cases.h"

/*
 * A suspended inbound data retry must keep pending_retry set and leave the
 * entry untouched, so the retained bytes are replayed on a later pass.
 */
int test_data_retry_suspension_preserves_pending(void)
{
    int failures = 0;

    uni_pending_fixture_t f;
    if (uni_pending_fixture_init(&f) < 0) { failures++; return failures; }

    /* Header + one object against a full queue: the object cannot emit. */
    MOQ_TEST_CHECK(held_bridge_uni_bytes(
                       f.tp.client_bridge, f.sid, f.obj, f.obj_len, false, 0)
                   == MOQ_ERR_WOULD_BLOCK);
    bridge_stream_entry_t *e = bridge_find_by_id(f.tp.client_bridge, f.sid);
    MOQ_TEST_CHECK(e != NULL && e->pending_retry);
    MOQ_TEST_CHECK(e->kind == BRIDGE_STREAM_UNI);

    sweep_arm_closing_subgroup(f.tp.client, 0);

    uint64_t suspends = session_budget_suspend_count;
    moq_bridge_budgeted_result_t out;
    MOQ_TEST_CHECK(held_bridge_service_budgeted(
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
    MOQ_TEST_CHECK(held_bridge_service(f.tp.client_bridge, 0) == MOQ_OK);
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
int test_data_reset_suspension_preserves_pending(void)
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
    MOQ_TEST_CHECK(held_bridge_uni_bytes(
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

    MOQ_TEST_CHECK(held_bridge_reset(
                       f.tp.client_bridge, f.sid, 0x5, 0) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(e->pending_reset);
    MOQ_TEST_CHECK(e->pending_reset_code == 0x5);
    MOQ_TEST_CHECK(!e->pending_retry);

    sweep_arm_closing_subgroup(f.tp.client, 0);

    uint64_t suspends = session_budget_suspend_count;
    moq_bridge_budgeted_result_t out;
    MOQ_TEST_CHECK(held_bridge_service_budgeted(
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
    MOQ_TEST_CHECK(held_bridge_service(f.tp.client_bridge, 0) == MOQ_OK);
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
int d18_request_bidi_fixture(test_pair_t *tp, uint64_t *bidi_out)
{
    if (d18_pair_init(tp, 1) < 0) return -1;
    moq_session_start(tp->client, 0);
    moq_session_start(tp->server, 0);
    d18_shuttle_until_quiescent(tp, 30, 0);

    moq_event_t ev;
    while (moq_session_poll_events(tp->client, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    if (d18_server_subscribe(tp) != MOQ_OK) goto fail;
    held_bridge_service(tp->server_bridge, 0);

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


int check_drain_membership(const moq_session_t *s,
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
            rc = held_bridge_uni_bytes(
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


int d18_strict_shuttle(test_pair_t *tp, int max, uint64_t now,
                       const char *what)
{
    int failures = 0;
    for (int i = 0; i < max; i++) {
        size_t moved = 0;
        MOQ_TEST_CHECK_EQ_INT(
            (int)held_bridge_service(tp->client_bridge, now),
            (int)MOQ_OK);
        failures += strict_feed(tp->server_bridge, &tp->client_ep, 1000, 2000,
                                now, &moved, what);
        MOQ_TEST_CHECK_EQ_INT(
            (int)held_bridge_service(tp->server_bridge, now),
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
int test_bidi_retry_suspension_preserves_pending(void)
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
    held_bridge_service(tp.client_bridge, 0);
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
    held_bridge_service(tp.server_bridge, 0);
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
    MOQ_TEST_CHECK(held_bridge_service_budgeted(
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
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    MOQ_TEST_CHECK(tp.client->subgroups[0].state == MOQ_SG_FREE);
    MOQ_TEST_CHECK(!e->pending_retry);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));

    /* The retained acceptance is delivered, on the same handle, exactly once. */
    MOQ_TEST_CHECK(moq_session_poll_events(tp.client, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NS_SUB_OK);
    MOQ_TEST_CHECK(ev.u.ns_sub_ok.handle._opaque == nh._opaque);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_poll_events(tp.client, &ev, 1) == 0);

    test_pair_destroy(&tp);
    return failures;
}


/* Both arms of the reset family call different session entries, so the bidi
 * arm is proven separately from the uni arm above. */
int test_bidi_reset_suspension_preserves_pending(void)
{
    int failures = 0;

    test_pair_t tp;
    uint64_t bidi = 0;
    if (d18_request_bidi_fixture(&tp, &bidi) < 0) { failures++; return failures; }

    bridge_stream_entry_t *e = bridge_find_by_id(tp.client_bridge, bidi);
    MOQ_TEST_CHECK(e != NULL && e->kind == BRIDGE_STREAM_BIDI);
    MOQ_TEST_CHECK(!e->pending_retry);
    MOQ_TEST_CHECK(tp.client->event_head != tp.client->event_tail);

    MOQ_TEST_CHECK(held_bridge_reset(
                       tp.client_bridge, bidi, 0x5, 0) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(e->pending_reset);
    MOQ_TEST_CHECK(e->pending_reset_code == 0x5);

    sweep_arm_closing_subgroup(tp.client, 0);

    uint64_t suspends = session_budget_suspend_count;
    moq_bridge_budgeted_result_t out;
    MOQ_TEST_CHECK(held_bridge_service_budgeted(
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

    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
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
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_poll_events(tp.client, &ev, 1) == 0);

    test_pair_destroy(&tp);
    return failures;
}


/* moq_session_on_bidi_stream_stop is the draft-18 request-cancellation input
 * and is distinct from a reset, so it is wired and proven on its own. */
int test_bidi_stop_suspension_preserves_pending(void)
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
    MOQ_TEST_CHECK(held_bridge_service_budgeted(
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

    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
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
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_poll_events(tp.client, &ev, 1) == 0);

    test_pair_destroy(&tp);
    return failures;
}


static int observe_data_stop_terminal(const moq_event_t *ev,
                                      moq_subscription_t owner,
                                      moq_subgroup_handle_t subgroup,
                                      moq_stream_ref_t ref, unsigned *count)
{
    int failures = 0;
    if (ev->kind == MOQ_EVENT_SUBGROUP_FINISHED) return 0; /* pressure cell */
    MOQ_TEST_CHECK(ev->kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED);
    MOQ_TEST_CHECK(ev->detail_size == MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE);
    if (ev->kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED &&
        ev->detail_size >= MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE) {
        const moq_outgoing_subgroup_terminated_event_t *d =
            &ev->u.outgoing_subgroup_terminated;
        ++*count;
        MOQ_TEST_CHECK(moq_subgroup_eq(d->subgroup, subgroup));
        MOQ_TEST_CHECK(moq_subscription_eq(d->sub, owner));
        MOQ_TEST_CHECK(!moq_publication_is_valid(d->pub));
        MOQ_TEST_CHECK(d->stream_ref._v == ref._v);
        MOQ_TEST_CHECK(d->group_id == 0 && d->subgroup_id == 0);
        MOQ_TEST_CHECK(d->error_code == 0x7);
        MOQ_TEST_CHECK(d->cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING);
    }
    return failures;
}


/*
 * moq_session_on_data_stop is the local-origin uni arm of the stop family: a
 * peer STOP_SENDING on a stream WE opened. So the client must be the publisher
 * here, which is the reverse of every other fixture in this file.
 *
 * Keep unrelated ACTION work queued so dropping obsolete same-stream output
 * cannot supply the reset's capacity. STOP recognition precedes retry sweeps.
 */
int test_data_stop_suspension_preserves_pending(void)
{
    int failures = 0;

    test_pair_t tp;
    /* One action slot, so a single queued object fills it. A larger queue would
     * need enough writes to overrun the endpoint's op recorder, and a dropped
     * op would make the "no reset yet" assertion below prove nothing. */
    if (test_pair_init_full(&tp, 0, false, 1, 0, 0) < 0) { failures++; return failures; }
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
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);

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
    moq_stream_ref_t stopped_ref = e->ref;
    unsigned terminal_notices = 0;

    /* The subgroup this stream carries must be live, or on_data_stop returns
     * without performing its reset transition at all. */
    size_t target = SIZE_MAX;
    for (size_t i = 0; i < tp.client->sg_cap; i++)
        if (tp.client->subgroups[i].stream_ref._v == e->ref._v &&
            (tp.client->subgroups[i].state == MOQ_SG_OPEN ||
             tp.client->subgroups[i].state == MOQ_SG_STREAMING))
            target = i;
    MOQ_TEST_CHECK(target != SIZE_MAX);
    /* An unrelated datagram keeps action capacity full through STOP. */
    {
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"AAA", 3, &p);
        MOQ_TEST_CHECK(moq_session_send_object_datagram(
                           tp.client, csub, 1, 0, 200, false, p, NULL, 0, 0) == MOQ_OK);
        moq_rcbuf_decref(p);
    }
    MOQ_TEST_CHECK(action_queue_full(tp.client));

    /* Otherwise the action poll can commit the reset directly, legitimately
     * settling STOP before its deferred-input retry reaches the sweep. */
    while (!event_queue_full(tp.client)) {
        moq_event_t pressure;
        memset(&pressure, 0, sizeof(pressure));
        pressure.kind = MOQ_EVENT_SUBGROUP_FINISHED;
        pressure.detail_size = sizeof(moq_subgroup_finished_event_t);
        MOQ_TEST_CHECK(push_event(tp.client, &pressure) == MOQ_OK);
    }

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
    MOQ_TEST_CHECK(held_bridge_service_budgeted(
                       tp.client_bridge, 0, 0, &out) == MOQ_OK);

    MOQ_TEST_CHECK(out.suspended);
    MOQ_TEST_CHECK(out.sweep_spent == 0);
    MOQ_TEST_CHECK(session_budget_suspend_count - suspends == 1);
    MOQ_TEST_CHECK(e->pending_stop);
    MOQ_TEST_CHECK(e->pending_stop_code == 0x7);
    MOQ_TEST_CHECK(e->active);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    /* Recognition survives suspension; no reset reached the transport. */
    MOQ_TEST_CHECK(tp.client->subgroups[target].state == MOQ_SG_TERMINAL_OWED);
    MOQ_TEST_CHECK(tp.client->subgroups[target].terminal_cause ==
                   MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING);
    MOQ_TEST_CHECK(tp.client->subgroups[target].terminal_code == 0x7);
    MOQ_TEST_CHECK(tp.client->subgroups[arm].state == MOQ_SG_CLOSING);
    /* The recorder never overflowed, so an absent reset really is absent
     * rather than dropped. */
    MOQ_TEST_CHECK(tp.client_ep.count < FAKE_EP_MAX_OPS);
    for (size_t i = 0; i < tp.client_ep.count; i++)
        MOQ_TEST_CHECK(tp.client_ep.ops[i].kind != FAKE_OP_RESET);

    fake_endpoint_clear_ops(&tp.client_ep);
    while (moq_session_poll_events(tp.client, &ev, 1) > 0) {
        failures += observe_data_stop_terminal(&ev, csub, sg, stopped_ref, &terminal_notices);
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);

    /* The accepted reset no longer needs an inbound retry. A subsequent
     * advancing call reaps the suspended sweep's terminal entries. */
    MOQ_TEST_CHECK(moq_session_tick(tp.client, 0) == MOQ_OK);

    MOQ_TEST_CHECK(tp.client->subgroups[arm].state == MOQ_SG_FREE);
    MOQ_TEST_CHECK(!e->pending_stop);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    /* on_data_stop deterministically resets the subgroup, and dispatching that
     * reset deactivates the bridge entry. */
    MOQ_TEST_CHECK(tp.client->subgroups[target].state == MOQ_SG_FREE);
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
    while (moq_session_poll_events(tp.client, &ev, 1) > 0) {
        failures += observe_data_stop_terminal(&ev, csub, sg, stopped_ref, &terminal_notices);
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(terminal_notices == 1);

    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
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
int test_tick_suspension_preserves_due_deadline(void)
{
    int failures = 0;

    const uint64_t idle_us = 30000000;
    test_pair_t tp;
    if (test_pair_init_full(&tp, 0, false, 0, idle_us, 0) < 0) {
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
    MOQ_TEST_CHECK(held_bridge_service_budgeted(
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
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, due) == MOQ_OK);

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
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, due) == MOQ_OK);
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
int test_inbound_scan_stops_at_suspension(void)
{
    int failures = 0;

    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    moq_event_t ev;
    while (moq_session_poll_events(tp.client, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
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
    /* Marked as every bridge setter marks it: the inbound walk is due. */
    tp.client_bridge->inbound_due = true;
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
    MOQ_TEST_CHECK(held_bridge_service_budgeted(
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
int test_progress_then_suspension_in_one_pass(void)
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
    held_bridge_service(tp.server_bridge, 0);

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
    MOQ_TEST_CHECK(held_bridge_uni_bytes(
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
    held_bridge_service(tp.server_bridge, 0);
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

    /* Entry i takes a reset; real reset admission has its own coverage, so the
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
    /* Marked as every bridge setter marks it: the inbound walk is due. */
    tp.client_bridge->inbound_due = true;
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
    MOQ_TEST_CHECK(held_bridge_service_budgeted(
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
    MOQ_TEST_CHECK(held_bridge_service_budgeted(
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
        MOQ_TEST_CHECK(held_bridge_service_budgeted(
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
int test_continuation_reclaims_event_scratch(void)
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
    MOQ_TEST_CHECK(held_bridge_service(tp.server_bridge, 0) == MOQ_OK);
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
int test_control_retry_suspension_preserves_pending(void)
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
    MOQ_TEST_CHECK(held_bridge_service(tp.server_bridge, 0) == MOQ_OK);

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
    MOQ_TEST_CHECK(held_bridge_service_budgeted(
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
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);

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
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    closes = 0;
    for (size_t i = 0; i < tp.client_ep.count; i++)
        if (tp.client_ep.ops[i].kind == FAKE_OP_CLOSE) closes++;
    MOQ_TEST_CHECK(closes == 0);

    test_pair_destroy(&tp);
    return failures;
}
