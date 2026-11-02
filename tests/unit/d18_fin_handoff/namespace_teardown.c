#include "test_cases.h"

/* -- ns_sub local teardown (session_namespace_sub.c:665) -------------- */

/* Draft-18 defines no post-request message on a publisher-side namespace-sub
 * bidi, so extra bytes terminate that BIDI rather than the session (§10.9.1).
 * Two obligations meet here and are kept apart:
 *
 *   Durable teardown -- the refused call's obligation is RETAINED in owner
 *   state.
 *   The branch (`if (len > 0) return ns_sub_local_teardown(...)`) deliberately
 *   does NOT append those bytes to the entry; instead the refusal installs the
 *   `local_teardown_pending` marker, which carries the semantic retry, and the
 *   documented empty re-feed (len == 0) then COMPLETES the teardown through that
 *   marker. The oracle pins the marker, not raw bytes -- recv_len is not
 *   required to grow.
 *
 *   FIN-aware drain -- the drain selector is FIN-aware: the cumulative peer FIN is latched
 *   into `pending_fin` BEFORE the capacity refusal, so a same-call FIN owes no
 *   drain and a genuinely open peer half owes exactly one. */

bool nsl_norm_action(const moq_action_t *a, void *vctx,
                     txs_norm_vec_t *out)
{
    if (a->kind == MOQ_ACTION_ABORT_BIDI_STREAM) {
        txs_img_t im;
        txs_img_init(&im);
        txs_img_u64(&im, a->u.abort_bidi_stream.stream_ref._v);
        txs_img_u64(&im, a->u.abort_bidi_stream.error_code);
        return txs_norm_append_img(out, a->kind, &im);
    }
    return nss_norm_action(a, vctx, out);
}


static txs_op_hooks_t nsl_hooks = {
    NULL, NULL, NULL, NULL, no_event_expected, nsl_norm_action
};


/* One whole-stream abort carrying CANCELLED (§3.3.3), and no event. */
void want_nsl_output(txs_norm_vec_t *v, moq_stream_ref_t ref)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, ref._v);
    txs_img_u64(&im, 0x1);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_ABORT_BIDI_STREAM, &im));
}


static const uint8_t k_nsl_extra[2] = { 0xde, 0xad };


/* Exact graph conservation: the captured edge set survives unchanged (same
 * count, every prior edge present exactly once, no added edge). Used to bracket
 * the refused teardown so a stray edge created by the refused call is caught
 * before any later drain/fill can absorb it. */
static int nsl_graph_equal(const og_graph_t *g0, const moq_session_t *s,
                           const char *what)
{
    og_graph_t g1;
    og_capture(s, &g1);
    int bad = og_check_integrity(&g1, what);
    if (g1.edge_count != g0->edge_count) {
        fprintf(stderr, "NSL %s: %zu graph edges, expected %zu\n", what,
                g1.edge_count, g0->edge_count);
        bad++;
    }
    for (size_t i = 0; i < g0->edge_count; i++) {
        const og_edge_t *e = &g0->edges[i];
        size_t seen = 0;
        for (size_t j = 0; j < g1.edge_count; j++) {
            const og_edge_t *f = &g1.edges[j];
            if (f->domain == e->domain && f->key == e->key &&
                f->kind == e->kind && f->slot == e->slot)
                seen++;
        }
        if (seen != 1) {
            fprintf(stderr, "NSL %s: edge (%d key %llu) not conserved (%zu)\n",
                    what, (int)e->domain, (unsigned long long)e->key, seen);
            bad++;
        }
    }
    return bad;
}


/* An inbound SUBSCRIBE_NAMESPACE the application has not answered: a
 * publisher-side entry in PENDING_PUBLISHER, whose bidi treats further bytes as
 * a local teardown. Returns the entry's own buffered length so a later oracle
 * can require it UNCHANGED without assuming how the session stores its
 * obligation. */
static moq_stream_ref_t arm_pending_publisher_ns_sub_at(moq_session_t *s,
                                                        uint64_t ref_v,
                                                        uint64_t request_id,
                                                        size_t *out_recv_len)
{
    moq_stream_ref_t ref = moq_stream_ref_from_u64(ref_v);   /* peer-opened */
    nss_ctx_t c = nss_ctx;
    c.request_id = request_id;
    MOQ_TEST_CHECK_EQ_INT((int)nss_feed(s, &c, ref, false), (int)MOQ_OK);
    arm_output_exact(s, MOQ_EVENT_NS_SUB_REQUEST, 1, 0, "ns_sub request", NULL);
    MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), 1);
    const moq_ns_sub_entry_t *e = ns_by_ref(s, ref);
    MOQ_TEST_CHECK(e != NULL);
    if (out_recv_len) *out_recv_len = 0;
    if (e) {
        MOQ_TEST_CHECK_EQ_INT((int)e->state,
                              (int)MOQ_NS_SUB_PENDING_PUBLISHER);
        MOQ_TEST_CHECK_EQ_U64(e->stream_ref._v, ref._v);
        /* Absolute "false at arm" for BOTH carriers. */
        MOQ_TEST_CHECK_EQ_INT(e->pending_fin ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_INT(e->local_teardown_pending ? 1 : 0, 0);
        g_ns_sub_request_id = e->request_id;
        if (out_recv_len) *out_recv_len = e->recv_len;
    }
    return ref;
}


static moq_stream_ref_t arm_pending_publisher_ns_sub(moq_session_t *s,
                                                     size_t *out_recv_len)
{
    return arm_pending_publisher_ns_sub_at(s, 1, 0, out_recv_len);
}


/* Durable teardown -- the refused call's obligation is RETAINED, isolated with NO peer
 * FIN so the drain selector plays no part. The publisher-side branch
 * (`if (len > 0) return ns_sub_local_teardown(...)`) deliberately does NOT
 * append those bytes to the entry; the refusal installs the durable
 * `local_teardown_pending` marker, and the documented empty re-feed (len == 0)
 * COMPLETES the teardown through it. The marker carries the retry, so nothing
 * here requires recv_len to grow: only the bytes the entry ALREADY owned are
 * immutable. */
static void run_nsl_retry_obligation(void)
{
    const char *what = "ns_sub teardown retry";
    moq_session_t *s = make_session_full(MOQ_PERSPECTIVE_SERVER, 0, 1);
    if (!s) return;
    size_t armed_len = 0;
    moq_stream_ref_t ref = arm_pending_publisher_ns_sub(s, &armed_len);

    /* Fill the ONE action slot with a distinct blocker on an unrelated ref, so
     * the teardown's own abort cannot be queued. */
    moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x5000);
    MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker), (int)MOQ_OK);
    MOQ_TEST_CHECK(action_queue_full(s));

    seed_snap_t owner0;
    memset(&owner0, 0, sizeof(owner0));
    nsg_owner_capture(s, ref, &owner0);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);

    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, k_nsl_extra,
                                              sizeof(k_nsl_extra), false, 1),
        (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    /* Everything about the owner is required unchanged EXCEPT the retained
     * teardown itself, whose representation is the session's own. The bytes the
     * entry already held are compared byte for byte; only what lies beyond that
     * prefix is left free. */
    {
        seed_snap_t nowsnap;
        memset(&nowsnap, 0, sizeof(nowsnap));
        nsg_owner_capture(s, ref, &nowsnap);
        MOQ_TEST_CHECK_EQ_INT(nowsnap.req.buf_invalid, 0);
        MOQ_TEST_CHECK(nowsnap.req.recv_len >= armed_len);
        if (!nowsnap.req.buf_invalid && !owner0.req.buf_invalid &&
            nowsnap.req.recv_len >= armed_len && armed_len > 0)
            MOQ_TEST_CHECK(memcmp(nowsnap.req.buf, owner0.req.buf,
                                  armed_len) == 0);
        seed_snap_t expect = owner0;
        expect.req.recv_len = nowsnap.req.recv_len;   /* carrier: unconstrained */
        memcpy(expect.req.buf, nowsnap.req.buf, sizeof(expect.req.buf));
        /* The ONE intended mutation: the teardown obligation is now durable.
         * No FIN was fed, so the cumulative-FIN latch stays false. */
        expect.req.ns_local_teardown_pending = 1;
        expect.req.fin = 0;
        nsg_owner_check(s, ref, &expect, what);
        MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), 1);
    }
    failures += txs_check_eq(s, &ref, 1, &before, what);
    drain_snap_t now;
    drain_snap(s, &now);
    failures += drain_multiset_equals(&now, &ring_before, what);

    /* Only the blocker is queued; the teardown produced nothing yet. */
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &nsl_hooks, &got);
        MOQ_TEST_CHECK_EQ_SIZE(got.count, (size_t)0);
        collect_actions(s, &nsl_hooks, &got);
        want_close_bidi(&want, blocker);
        failures += txs_norm_equals(&got, &want, "ns_sub teardown blocked");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }

    /* The documented recovery: retry with no new bytes. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    check_nsg_retired(s, ref);
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &nsl_hooks, &got);
        collect_actions(s, &nsl_hooks, &got);
        want_nsl_output(&want, ref);
        failures += txs_norm_equals(&got, &want, "ns_sub teardown replayed");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    drain_snap_t after, want_ring;
    drain_snap(s, &after);
    drain_snap_plus(&ring_before, ref, MOQ_DRAIN_NORMAL, &want_ring);
    failures += drain_multiset_equals(&after, &want_ring,
                                      "ns_sub teardown replay granted");
    check_drain_ref_reason(s, ref, MOQ_DRAIN_NORMAL);

    /* And it happens exactly once: a second empty re-feed produces nothing. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    check_nsg_retired(s, ref);          /* no silent resurrection */
    check_no_output(s, &nsl_hooks);
    drain_snap_t after2;
    drain_snap(s, &after2);
    failures += drain_multiset_equals(&after2, &want_ring,
                                      "ns_sub teardown replay idempotent");
    moq_session_destroy(s);
}


/* Durable teardown + FIN-aware drain, cumulative-FIN retention: an extra-bytes+FIN teardown that
 * blocks on its action slot must retain the FIN durably in session-owned state,
 * not just the teardown obligation. The peer's send half is already closed, so
 * the eventual abort owes NO drain -- but the empty re-feed carries fin=false,
 * so unless the FIN was latched the selector recomputes need_drain=true and, on
 * a full ring, stalls the teardown forever waiting for a terminal that already
 * arrived. This pins the durable cumulative FIN fact and the drainless
 * completion against an exhausted ring. */
static void run_nsl_fin_retry_obligation(void)
{
    const char *what = "ns_sub teardown FIN retry";
    moq_session_t *s = make_session_full(MOQ_PERSPECTIVE_SERVER, 0, 1);
    if (!s) return;
    size_t armed_len = 0;
    moq_stream_ref_t ref = arm_pending_publisher_ns_sub(s, &armed_len);

    /* Fill the ONE action slot so the teardown's own abort cannot be queued. */
    moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x5000);
    MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker), (int)MOQ_OK);
    MOQ_TEST_CHECK(action_queue_full(s));

    /* Baselines captured BEFORE the refused call, so a wrong drain, a stray
     * graph edge, or a corrupted owned byte inserted BY that call is caught at
     * the refused point -- never absorbed into a later fill. */
    seed_snap_t owner0;
    memset(&owner0, 0, sizeof(owner0));
    nsg_owner_capture(s, ref, &owner0);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);
    og_graph_t g0;
    og_capture(s, &g0);

    /* Extra bytes AND a peer FIN, action-blocked. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, k_nsl_extra,
                                              sizeof(k_nsl_extra), true, 1),
        (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    /* PRE-FILL oracle: only the declared owner deltas moved, the previously
     * owned byte prefix is exact, the whole session / graph / drain multiset are
     * unchanged, and only the blocker action is queued with no event. */
    {
        seed_snap_t nowsnap;
        memset(&nowsnap, 0, sizeof(nowsnap));
        nsg_owner_capture(s, ref, &nowsnap);
        MOQ_TEST_CHECK_EQ_INT(nowsnap.req.buf_invalid, 0);
        MOQ_TEST_CHECK(nowsnap.req.recv_len >= armed_len);
        if (!nowsnap.req.buf_invalid && !owner0.req.buf_invalid &&
            nowsnap.req.recv_len >= armed_len && armed_len > 0)
            MOQ_TEST_CHECK(memcmp(nowsnap.req.buf, owner0.req.buf,
                                  armed_len) == 0);
        seed_snap_t expect = owner0;
        expect.req.recv_len = nowsnap.req.recv_len;   /* carrier: unconstrained */
        memcpy(expect.req.buf, nowsnap.req.buf, sizeof(expect.req.buf));
        expect.req.ns_local_teardown_pending = 1;
        expect.req.fin = 1;                      /* cumulative FIN, retained */
        nsg_owner_check(s, ref, &expect, "ns_sub FIN teardown blocked");
        MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), 1);
    }
    failures += txs_check_eq(s, &ref, 1, &before, "ns_sub FIN teardown blocked");
    failures += nsl_graph_equal(&g0, s, "ns_sub FIN teardown blocked");
    {
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before,
                                          "ns_sub FIN teardown blocked ring");
    }
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &nsl_hooks, &got);
        MOQ_TEST_CHECK_EQ_SIZE(got.count, (size_t)0);
        collect_actions(s, &nsl_hooks, &got);
        want_close_bidi(&want, blocker);
        failures += txs_norm_equals(&got, &want, "ns_sub FIN teardown blocked");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }

    /* Only now EXHAUST the drain ring: a NORMAL drain could no longer be
     * reserved, but the retained FIN means none is owed. (The blocker action was
     * consumed by the classification above.) */
    fill_drain_ring(s);
    drain_snap_t ring_full;
    drain_snap(s, &ring_full);

    /* The documented recovery: no new bytes. It completes with exactly one abort
     * and NO drain, even against the full ring. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    check_nsg_retired(s, ref);
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &nsl_hooks, &got);
        collect_actions(s, &nsl_hooks, &got);
        want_nsl_output(&want, ref);
        failures += txs_norm_equals(&got, &want, what);
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    drain_snap_t after;
    drain_snap(s, &after);
    failures += drain_multiset_equals(&after, &ring_full,
                                      "ns_sub FIN teardown no drain");

    /* Exactly once: a second empty re-feed changes nothing. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    check_nsg_retired(s, ref);
    check_no_output(s, &nsl_hooks);
    drain_snap_t after2;
    drain_snap(s, &after2);
    failures += drain_multiset_equals(&after2, &ring_full,
                                      "ns_sub FIN teardown idempotent");
    moq_session_destroy(s);
}


/* The drain selector pair, run with capacity available so nothing else can
 * confound it. */
static void run_nsl_drain_selector(bool fin_in_call)
{
    char what[96];
    moq_session_t *s = make_session_default();
    if (!s) return;
    moq_stream_ref_t ref = arm_pending_publisher_ns_sub(s, NULL);

    if (fin_in_call) fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);

    /* Same-call FIN needs no reference, so an exhausted ring cannot refuse it;
     * without the FIN exactly one normal-drain slot must be taken. */
    moq_result_t want_rc = MOQ_OK;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, k_nsl_extra,
                                              sizeof(k_nsl_extra),
                                              fin_in_call, 1),
        (int)want_rc);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    check_nsg_retired(s, ref);
    snprintf(what, sizeof(what), "ns_sub teardown selector fin=%d",
             fin_in_call ? 1 : 0);
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &nsl_hooks, &got);
        collect_actions(s, &nsl_hooks, &got);
        want_nsl_output(&want, ref);
        failures += txs_norm_equals(&got, &want, what);
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    drain_snap_t now, want_ring;
    drain_snap(s, &now);
    if (fin_in_call) want_ring = ring_before;
    else {
        drain_snap_plus(&ring_before, ref, MOQ_DRAIN_NORMAL, &want_ring);
        check_drain_ref_reason(s, ref, MOQ_DRAIN_NORMAL);
    }
    failures += drain_multiset_equals(&now, &want_ring, what);

    if (!fin_in_call) {
        /* The reference the terminal took is RELEASABLE: the peer's later FIN
         * restores the ring exactly, emits nothing, and leaves the owner gone. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        drain_snap_t after;
        drain_snap(s, &after);
        failures += drain_multiset_equals(&after, &ring_before,
                                          "ns_sub teardown released");
        check_nsg_retired(s, ref);
        check_no_output(s, &nsl_hooks);
    }
    moq_session_destroy(s);
}


/* Clear-exactly-once across slot reuse: owner A genuinely reaches the blocked
 * teardown state with BOTH local_teardown_pending=true and pending_fin=true,
 * then recovers and retires. The selective free must clear BOTH, and the two
 * facts are proven by DIFFERENT observations, stated honestly:
 *   - the freed slot is inspected DIRECTLY (before owner B arms), so both
 *     local_teardown_pending==false and pending_fin==false are proven by the
 *     free itself;
 *   - only local_teardown_pending drives the empty-refeed replay, so owner B's
 *     first empty feed being INERT (no replayed abort) is the independent proof
 *     that no stale teardown marker survived. A stale pending_fin alone would
 *     not tear owner B down; the direct field check above is what pins it. */
static void run_nsl_reuse_after_teardown(void)
{
    moq_session_t *s = make_session_full(MOQ_PERSPECTIVE_SERVER, 0, 1);
    if (!s) return;

    /* Owner A, armed then action-blocked on its FIN teardown so BOTH markers
     * become durable. */
    moq_stream_ref_t refA = arm_pending_publisher_ns_sub_at(s, 1, 0, NULL);
    int32_t slotA = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, refA._v);
    MOQ_TEST_CHECK(slotA >= 0);

    moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x5000);
    MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker), (int)MOQ_OK);
    MOQ_TEST_CHECK(action_queue_full(s));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, refA, k_nsl_extra,
                                              sizeof(k_nsl_extra), true, 1),
        (int)MOQ_ERR_WOULD_BLOCK);
    {
        const moq_ns_sub_entry_t *eA = ns_by_ref(s, refA);
        MOQ_TEST_CHECK(eA != NULL);
        if (eA) {
            MOQ_TEST_CHECK_EQ_INT(eA->local_teardown_pending ? 1 : 0, 1);
            MOQ_TEST_CHECK_EQ_INT(eA->pending_fin ? 1 : 0, 1);
        }
    }

    /* Recover: drain the blocker, empty re-feed completes the teardown and
     * retires owner A; drain its abort so the next arm starts clean. */
    {
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, refA, NULL, 0, false, 1),
        (int)MOQ_OK);
    check_nsg_retired(s, refA);

    /* The selective free itself cleared BOTH fields on the vacated slot, checked
     * directly BEFORE any fresh-bidi re-init could mask them: this is where the
     * free-entry marker-clear and pending-FIN-clear are each observable. */
    MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[slotA].state, (int)MOQ_NS_SUB_FREE);
    MOQ_TEST_CHECK_EQ_INT(s->ns_subs[slotA].local_teardown_pending ? 1 : 0, 0);
    MOQ_TEST_CHECK_EQ_INT(s->ns_subs[slotA].pending_fin ? 1 : 0, 0);

    {
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    }

    /* Owner B reuses the freed slot (the peer's next request id is 2). The arm
     * helper re-asserts both carriers false at arm; the slot identity is the one
     * A vacated. A stale field cleared by the selective free would break either
     * assertion or the inert-feed behaviour below. */
    moq_stream_ref_t refB = arm_pending_publisher_ns_sub_at(s, 5, 2, NULL);
    int32_t slotB = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, refB._v);
    MOQ_TEST_CHECK_EQ_INT(slotB, slotA);

    /* An empty feed on the fresh owner is inert: no teardown, no graceful close,
     * no output; the owner stays PENDING_PUBLISHER with both carriers clear. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, refB, NULL, 0, false, 1),
        (int)MOQ_OK);
    const moq_ns_sub_entry_t *eB = ns_by_ref(s, refB);
    MOQ_TEST_CHECK(eB != NULL);
    if (eB) {
        MOQ_TEST_CHECK_EQ_INT((int)eB->state,
                              (int)MOQ_NS_SUB_PENDING_PUBLISHER);
        MOQ_TEST_CHECK_EQ_INT(eB->local_teardown_pending ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_INT(eB->pending_fin ? 1 : 0, 0);
    }
    MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), 1);
    check_no_output(s, &nsl_hooks);
    moq_session_destroy(s);
}


void test_ns_sub_local_teardown(void)
{
    run_nsl_retry_obligation();
    run_nsl_fin_retry_obligation();
    run_nsl_drain_selector(false);
    run_nsl_drain_selector(true);
    run_nsl_reuse_after_teardown();
}
