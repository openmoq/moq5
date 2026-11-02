#include "test_cases.h"

void nob_ring_snap(const moq_session_t *s, nob_ring_t *d)
{
    memset(d, 0, sizeof(*d));
    if (s->drain_ref_count > NOB_RING_MAX) { d->overflow = 1; return; }
    for (size_t i = 0; i < s->drain_ref_count; i++) {
        d->ref[i] = s->drain_refs[i];
        d->reason[i] = s->drain_ref_reasons[i];
    }
    d->count = s->drain_ref_count;
}


void nob_ring_plus(const nob_ring_t *base, moq_stream_ref_t extra,
                   moq_drain_reason_t reason, nob_ring_t *out)
{
    *out = *base;
    if (out->count >= NOB_RING_MAX) { out->overflow = 1; return; }
    out->ref[out->count] = extra._v;
    out->reason[out->count] = (uint8_t)reason;
    out->count++;
}


int nob_ring_equals(const nob_ring_t *have, const nob_ring_t *want,
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


/* -- durable teardown over the PHYSICAL bridge: ns_sub local teardown -
 *
 * The publisher-side namespace-sub bidi treats extra inbound bytes as a
 * local teardown of that BIDI (draft-18 §10.9.1). The teardown needs one
 * action slot; a refusal returns MOQ_ERR_WOULD_BLOCK with the input bytes
 * DISCARDED (the branch never buffers them), and the bridge -- which never
 * re-delivers peer bytes -- retries with NULL/0. The session latches the
 * teardown obligation (and, with a FIN, the cumulative FIN) durably before the
 * capacity refusal, so the bridge's own empty re-drive completes the
 * teardown exactly once. Every recovery assertion below pins that contract.
 *
 * The blocked half also pins the stream-ownership predicate as a passing
 * control: this
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
     * the d18 staging cleanup's unqualified remove-by-id(0) (it deletes by
     * request id without checking the owner, so an ordinal-zero key of
     * another owner goes with it) would
     * otherwise destroy the fixture owner's own by-id key and make the
     * declared topology unassertable. The whole warm exchange is DECODED and
     * CLASSIFIED -- its request id, its rejection wire, its retirement -- so
     * the ordinal-zero avoidance cannot drift silently. The fixture subject then
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
            (int)held_bridge_service(tp.client_bridge, 0),
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
            (int)held_bridge_service(tp.server_bridge, 0),
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
            (int)held_bridge_service(tp.client_bridge, 0),
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
            (int)held_bridge_service(tp.server_bridge, 0),
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
        (int)held_bridge_service(tp.client_bridge, 0), (int)MOQ_OK);
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

    /* Blocked state: the bridge holds the retry -- the stream-ownership
     * predicate answers
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
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);

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

        /* Without a peer terminal the discard mapping remains. A FIN already
         * retained before the abort is sufficient: after service settles the
         * abort, neither a mapping nor a second terminal is owed. */
        if (fin) {
            MOQ_TEST_CHECK(bridge_find_by_id(tp.server_bridge, bidi_id) == NULL);
            MOQ_TEST_CHECK(bridge_find_by_ref(tp.server_bridge, sref) == NULL);
        } else {
            failures += nslb_check_entry(tp.server_bridge, bidi_id, sref._v,
                                         0, 1, 0, 0, what);
        }
        MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.server_bridge));
        MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(
                           tp.server_bridge, bidi_id));

        if (pass == 0) {
            /* Exactly once: a second service adds nothing and must reproduce
             * the whole postcondition, not a subset of it. */
            fake_endpoint_clear_ops(&tp.server_ep);
            MOQ_TEST_CHECK_EQ_INT(
                (int)held_bridge_service(tp.server_bridge, 0),
                (int)MOQ_OK);
        }
    }

    test_pair_destroy(&tp);
    return failures;
}


/* Two rows over the physical bridge: the no-FIN teardown (the peer send half
 * stays open, so exactly one NORMAL drain is owed and recovered), and the
 * extra-bytes+FIN teardown against a FULL drain ring. The FIN row pins
 * cumulative-FIN retention: the bridge re-drives the pending bidi with
 * NULL,0,false, so the session must latch the FIN before the capacity refusal;
 * otherwise the drain selector would recompute need_drain=true and stall
 * against the exhausted ring. */
int test_ns_sub_local_teardown_bridge_retry(void)
{
    int failures = 0;
    failures += run_nslb_teardown(false, false);
    failures += run_nslb_teardown(true, true);
    return failures;
}
