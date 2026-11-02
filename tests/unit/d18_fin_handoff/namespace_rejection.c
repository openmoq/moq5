#include "test_cases.h"

/* -- ns_sub_send_request_error: both reject callers ------------------- */

/*
 * A SUBSCRIBE_NAMESPACE the responder refuses before the request is ever
 * app-visible: the auth walk rejects an unknown USE_ALIAS
 * (`session_namespace_sub.c:761`), or the prefix overlaps a live namespace
 * subscription (`:774`). Both reach `ns_sub_send_request_error` (`:365`),
 * which queues REQUEST_ERROR with FIN, drains the request bidi and frees the
 * transient entry.
 *
 * The drain is reserved FIN-aware: `ns_sub_send_request_error` takes a reference
 * only when `uses_request_streams && stream_ref && !peer_fin_observed`
 * (`session_namespace_sub.c:386`), where `peer_fin_observed` folds the current
 * FIN, the durable `pending_fin` latch and the `handoff_fin_pending` marker. So
 * a same-call FIN owes no drain and those rows are drainless under the unified
 * rule.
 *
 * Retention here is a ONE-owner story. The handoff transfers ownership of the
 * bidi to the `MOQ_NS_SUB_RECVING_PUBLISHER` entry and frees the generic
 * staging owner even on `MOQ_ERR_WOULD_BLOCK` (`session_subscribe.c:1310`),
 * because retries route through `idx_ns_by_ref` and never back through the
 * staging slot. The inline comment at `profile_d18.c:1590` is about
 * `ns_sub_on_new_bidi()` refusing before handoff on ns-sub pool exhaustion and
 * is accurate.
 */

static size_t enc_sns(uint8_t *buf, size_t cap, uint64_t rid, const char *field,
                      bool unknown_alias)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { { (const uint8_t *)field, strlen(field) } };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t p = { 0 };
    if (unknown_alias) {
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
        p.auth_tokens[0].alias = 99;
    }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe_namespace(&w, rid, &ns, &p), (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}


size_t enc_sns_auth(uint8_t *buf, size_t cap, uint64_t rid)
{
    return enc_sns(buf, cap, rid, "auth", true);
}


/* The same prefix the seeded subscription holds: an exact overlap. */
static size_t enc_sns_overlap(uint8_t *buf, size_t cap, uint64_t rid)
{
    return enc_sns(buf, cap, rid, "live", false);
}


/* Consume and retire inbound request id 0 through a complete, exactly checked
 * exchange, so no fixture in this family carries the ordinal-zero key whose
 * cross-owner deletion (the staging cleanup's unqualified remove-by-id(0)) would
 * otherwise mask the asserted topology. */
static void nsr_burn_request_id_0(moq_session_t *s)
{
    moq_stream_ref_t ref = moq_stream_ref_from_u64(9);
    uint8_t msg[192];
    size_t n = enc_sns(msg, sizeof(msg), 0, "burn", false);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
        (int)MOQ_OK);
    moq_ns_sub_handle_t h = MOQ_NS_SUB_HANDLE_INVALID;
    {
        int req = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_NS_SUB_REQUEST) {
                req++; h = e.u.ns_sub_request.handle;
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(req, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    moq_reject_ns_sub_cfg_t rc;
    moq_reject_ns_sub_cfg_init(&rc);
    rc.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_ns_sub(s, h, &rc, 1),
                          (int)MOQ_OK);
    /* Exactly one REQUEST_ERROR, decoded: our declared code, an empty reason,
     * FIN closing our send half, and nothing after the envelope. */
    {
        int sends = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a.u.send_bidi_stream.stream_ref._v == ref._v) {
                sends++;
                MOQ_TEST_CHECK_EQ_INT(a.u.send_bidi_stream.fin ? 1 : 0, 1);
                uint8_t want[128];
                size_t wn = enc_request_error(want, sizeof(want),
                    MOQ_REQUEST_ERROR_NOT_SUPPORTED, 0, NULL);
                failures += txs_check_span_bytes(a.u.send_bidi_stream.data,
                                                 a.u.send_bidi_stream.len,
                                                 want, wn,
                                                 "ns_sub rejection wire");
            } else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(sends, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    /* The peer FINs its half; the entry and its drain reference go with it. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), 0);
    MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 0);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, (size_t)0);
    check_no_output(s, &pub_hooks_for_actions);
    /* And no edge of the burned request survives anywhere: a stale key would
     * otherwise sit in the graph every later assertion is measured against. */
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "burn retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_RID, 0, "burn retired");
        failures += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v, "burn retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v,
                                     "burn retired");
    }
}



/* -- the retained overlap seed --------------------------------------- */

static moq_stream_ref_t g_nsr_seed_ref;

static uint64_t g_nsr_seed_rid;

static int      g_nsr_seed_slot;

static nso_snap_t g_nsr_seed_want;


/* The seeded owner -- whose expected record is DECLARED at arm time, not read
 * back -- and its exact edge closure must survive every call, replay and late
 * FIN in the overlap branch. */
static void nsr_seed_check(const moq_session_t *s, const char *what)
{
    if (g_nsr_seed_slot < 0) return;      /* branch has no seed */
    nso_snap_t now;
    nso_read(s, g_nsr_seed_slot, &now);
    failures += nso_diff(&now, &g_nsr_seed_want, what);

    og_graph_t g;
    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    {
        /* An ESTABLISHED namespace subscription holds its own index edge and
         * nothing else: the acceptance removes the pending-phase by-id key
         * (session_namespace_sub.c:1155) as it transitions. */
        const og_edge_spec_t want[] = {
            { OG_DOM_NS_REF, g_nsr_seed_ref._v },
        };
        failures += og_check_owner_edges(&g, MOQ_REQ_NAMESPACE_SUB,
                                         g_nsr_seed_slot, want, 1, what);
    }
    failures += og_check_no_edge(&g, OG_DOM_REQ_RID, g_nsr_seed_rid, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, g_nsr_seed_ref._v,
                                 what);
}


int nsr_problems(const nsr_case_t *c)
{
    int bad = 0;
    if (!c->name || !c->arm || !c->encode || !c->declare_transient) bad++;
    if (c->err_code == 0) bad++;
    return bad;
}


/* The single REQUEST_ERROR the reject owes, with FIN closing our send half. */
static void want_nsr_output(txs_norm_vec_t *v, moq_stream_ref_t ref,
                            uint64_t code)
{
    uint8_t buf[128];
    size_t n = enc_request_error(buf, sizeof(buf), code, 0, NULL);
    want_bidi_msg(v, ref, true, buf, n);
}


static void check_nsr_output(moq_session_t *s, moq_stream_ref_t ref,
                             uint64_t code, const char *what)
{
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &pub_hooks_for_actions, &got);
    collect_actions(s, &pub_hooks_for_actions, &got);
    want_nsr_output(&want, ref, code);
    failures += txs_norm_equals(&got, &want, what);
    txs_norm_free(&got);
    txs_norm_free(&want);
}


/* The one owner a refusal leaves behind, compared against a record built from
 * the PRE-CALL slot state plus this branch's declared mutations -- so nothing
 * the refused call did can define its own expectation. */
static void check_nsr_blocked_owner(const moq_session_t *s,
                                    moq_stream_ref_t ref, uint64_t rid,
                                    int want_slot, const nso_snap_t *want,
                                    const char *what)
{
    /* The generic staging owner is GONE, with its stream-ref edge. */
    MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)request_registry_find_by_streamref(s, ref).kind, (int)MOQ_REQ_NONE);

    int want_busy = (g_nsr_seed_slot >= 0) ? 2 : 1;
    MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), want_busy);
    int32_t slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
    /* The slot the handoff was DECLARED to take, not whichever one it took. */
    MOQ_TEST_CHECK_EQ_INT((int)slot, want_slot);
    if (slot < 0) return;

    nso_snap_t now;
    nso_read(s, slot, &now);
    failures += nso_diff(&now, want, what);

    /* Its edges: the namespace-ref edge it owns and NOTHING else -- the
     * request is not app-visible, so no by-id key is committed. */
    og_graph_t g;
    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    {
        const og_edge_spec_t want_e[] = { { OG_DOM_NS_REF, ref._v } };
        failures += og_check_owner_edges(&g, MOQ_REQ_NAMESPACE_SUB, slot,
                                         want_e, 1, what);
    }
    failures += og_check_no_edge(&g, OG_DOM_REQ_RID, rid, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
    nsr_seed_check(s, what);
}


static void check_nsr_retired(const moq_session_t *s, moq_stream_ref_t ref,
                              uint64_t rid, const char *what)
{
    int want_busy = (g_nsr_seed_slot >= 0) ? 1 : 0;
    MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), want_busy);
    MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 0);
    og_graph_t g;
    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    failures += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_RID, rid, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
    nsr_seed_check(s, what);
}


/* One branch under one (FIN, drain-ring) state. The blocked rows compare an
 * EMPTY re-feed against a snapshot taken once the transient owner exists,
 * since the owner is created by the very call under test. */
static void run_nsr(const nsr_case_t *c, bool fin_in_call, bool fill_ring,
                    size_t split)
{
    char what[128];
    moq_session_t *s = make_session_default();
    if (!s) return;
    MOQ_TEST_CHECK_EQ_INT(nsr_problems(c), 0);
    uint64_t rid = c->arm(s);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(21);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, (size_t)0);

    uint8_t m[192];
    size_t n = c->encode(m, sizeof(m), rid);
    MOQ_TEST_CHECK(n > 0 && split < n);
    if (split >= n) { moq_session_destroy(s); return; }

    if (fill_ring) fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    /* Captured BEFORE the terminal feed, so a refusal is measured against the
     * session as it was, not against whatever that same refused call left
     * behind. The destination slot and the generation it will carry are
     * DERIVED here too: ns_sub_on_new_bidi takes the first free slot and sets
     * `generation |= 1` (session_namespace_sub.c:942-950), so both are known
     * before the call and are never read back from the mutated owner. */
    txs_snapshot_t pre_call;
    txs_capture(s, &ref, 1, &pre_call);
    expect_after_call_prepare(&pre_call);
    int want_slot = -1;
    uint32_t want_gen = 0;
    uint64_t want_handle = 0;
    for (size_t i = 0; i < s->ns_sub_cap; i++) {
        if (s->ns_subs[i].state == MOQ_NS_SUB_FREE) {
            want_slot = (int)i;
            want_gen = s->ns_subs[i].generation | 1u;
            /* The handoff mints NO handle -- the entry is not app-visible
             * until it commits -- so whatever the slot carried must survive
             * unchanged. */
            want_handle = s->ns_subs[i].handle._opaque;
            break;
        }
    }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) { moq_session_destroy(s); return; }
    nso_snap_t pre_slot;
    nso_read(s, want_slot, &pre_slot);
    MOQ_TEST_CHECK_EQ_INT(pre_slot.invalid, 0);

    if (split > 0) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, m, split, false, 1),
            (int)MOQ_OK);
        check_no_output(s, &pub_hooks_for_actions);
    }

    /* The declared inventory a refusal must leave: the pre-call slot, plus
     * the universal handoff mutations, plus this branch's own. */
    nso_snap_t want_owner = pre_slot;
    want_owner.present = 1;
    want_owner.state = (int)MOQ_NS_SUB_RECVING_PUBLISHER;
    want_owner.stream_kind = (int)MOQ_STREAM_KIND_NAMESPACE_SUB;
    want_owner.generation = want_gen;
    want_owner.handle = want_handle;
    want_owner.stream_ref = ref._v;
    want_owner.request_id = rid;
    want_owner.ep_kind = (int)MOQ_REQ_NAMESPACE_SUB;
    want_owner.ep_slot = want_slot;
    want_owner.ep_has_request_id = 1;
    want_owner.ep_request_id = rid;
    want_owner.ep_has_stream_ref = 1;
    want_owner.ep_stream_ref = ref._v;
    want_owner.parse_complete = 0;
    want_owner.got_response = 0;
    want_owner.pending_fin = 0;
    want_owner.closing_remote_error = 0;
    want_owner.goaway_sent = 0;
    want_owner.auth_committed = 0;
    want_owner.prefix_valid = 0;
    want_owner.prefix_len = 0;
    want_owner.recv_len = n;
    MOQ_TEST_CHECK(n <= NSO_BUF_MAX);
    memcpy(want_owner.recv, m, n);
    c->declare_transient(&want_owner, &pre_slot);

    moq_result_t want_rc = (fill_ring && !fin_in_call) ? MOQ_ERR_WOULD_BLOCK
                                                       : MOQ_OK;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, m + split, n - split,
                                              fin_in_call, 1),
        (int)want_rc);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        snprintf(what, sizeof(what), "%s blocked", c->name);
        check_nsr_blocked_owner(s, ref, rid, want_slot, &want_owner, what);
        check_no_output(s, &pub_hooks_for_actions);
        {
            drain_snap_t now;
            drain_snap(s, &now);
            failures += drain_multiset_equals(&now, &ring_before, what);
        }
        /* The ONE declared change the refused call is allowed to make: the
         * bidi's owner moves from the generic staging subscription to the
         * namespace-sub entry. Every other session field must be as it was
         * before the feed. */
        {
            txs_snapshot_t expect = pre_call;
            if (expect.owner_count == 1) {
                expect.owners[0].kind = TXS_OWNER_NS;
                expect.owners[0].slot = want_slot;
                expect.owners[0].state = (int)MOQ_NS_SUB_RECVING_PUBLISHER;
                expect.owners[0].generation = want_gen;
                expect.owners[0].invalid = 0;
            }
            snprintf(what, sizeof(what), "%s handoff delta", c->name);
            failures += txs_check_eq(s, &ref, 1, &expect, what);
        }
        snprintf(what, sizeof(what), "%s blocked", c->name);

        /* The transient owner's inventory as the blocked state left it. Every
         * field is already declared absolutely above; this second, independent
         * comparison is what pins CONSERVATION across the re-feed, so a field
         * that drifts between the two calls is caught even though both were
         * measured against the same declared record. */
        nso_snap_t blocked0;
        {
            int32_t bslot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                           ref._v);
            nso_read(s, bslot, &blocked0);
            MOQ_TEST_CHECK_EQ_INT(blocked0.invalid, 0);
        }

        /* An empty re-feed against a snapshot taken with the owner already
         * present: nothing at all may move while the ring stays full. */
        txs_snapshot_t before;
        txs_capture(s, &ref, 1, &before);
        expect_after_call_prepare(&before);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_ERR_WOULD_BLOCK);
        snprintf(what, sizeof(what), "%s refeed blocked", c->name);
        check_nsr_blocked_owner(s, ref, rid, want_slot, &want_owner, what);
        {
            nso_snap_t now;
            int32_t bslot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                           ref._v);
            nso_read(s, bslot, &now);
            failures += nso_diff(&now, &blocked0, what);
        }
        failures += txs_check_eq(s, &ref, 1, &before, what);
        check_no_output(s, &pub_hooks_for_actions);
        {
            drain_snap_t now;
            drain_snap(s, &now);
            failures += drain_multiset_equals(&now, &ring_before, what);
        }

        /* Release one declared filler, then replay with no new bytes. */
        moq_stream_ref_t filler = moq_stream_ref_from_u64(0x4000);
        MOQ_TEST_CHECK(drain_ref_contains(s, filler));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, filler, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t ring_freed;
        drain_snap_minus(&ring_before, filler, MOQ_DRAIN_NORMAL, &ring_freed);

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        snprintf(what, sizeof(what), "%s replayed", c->name);
        check_nsr_output(s, ref, c->err_code, what);
        check_nsr_retired(s, ref, rid, what);
        drain_snap_t after, want_ring;
        drain_snap(s, &after);
        drain_snap_plus(&ring_freed, ref, MOQ_DRAIN_NORMAL, &want_ring);
        failures += drain_multiset_equals(&after, &want_ring, what);
        check_drain_ref_reason(s, ref, MOQ_DRAIN_NORMAL);

        /* The late FIN releases exactly that reference and owes nothing. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t released;
        drain_snap(s, &released);
        snprintf(what, sizeof(what), "%s replay released", c->name);
        failures += drain_multiset_equals(&released, &ring_freed, what);
        check_nsr_retired(s, ref, rid, what);
        check_no_output(s, &pub_hooks_for_actions);
        moq_session_destroy(s);
        return;
    }

    snprintf(what, sizeof(what), "%s output", c->name);
    check_nsr_output(s, ref, c->err_code, what);
    check_nsr_retired(s, ref, rid, what);

    drain_snap_t now, want_ring;
    drain_snap(s, &now);
    if (fin_in_call) {
        want_ring = ring_before;      /* the peer half is already closed */
    } else {
        drain_snap_plus(&ring_before, ref, MOQ_DRAIN_NORMAL, &want_ring);
        check_drain_ref_reason(s, ref, MOQ_DRAIN_NORMAL);
    }
    snprintf(what, sizeof(what), "%s granted", c->name);
    failures += drain_multiset_equals(&now, &want_ring, what);

    if (!fin_in_call) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t after;
        drain_snap(s, &after);
        snprintf(what, sizeof(what), "%s released", c->name);
        failures += drain_multiset_equals(&after, &ring_before, what);
        check_nsr_retired(s, ref, rid, what);
        check_no_output(s, &pub_hooks_for_actions);
    }
    moq_session_destroy(s);
}


static void run_nsr_matrix(const nsr_case_t *c, size_t split)
{
    run_nsr(c, false, false, 0);   /* no FIN: one releasable reference */
    run_nsr(c, true,  false, 0);   /* same-call FIN: none needed */
    run_nsr(c, true,  true,  0);   /* exhausted ring, FIN observed */
    run_nsr(c, false, true,  0);   /* exhausted ring, no FIN */
    run_nsr(c, false, true,  split);            /* fragmented */
}


/* -- the two branches ------------------------------------------------ */

/* The auth walk runs and its verdict is retained for the replay; the request
 * shape is never applied and the prefix stage is never reached, so interest,
 * forward and the prefix count all keep whatever the slot held before. */
void nsr_auth_declare(nso_snap_t *want, const nso_snap_t *pre)
{
    want->auth_processed = 1;
    /* ZERO resolved tokens: an unknown USE_ALIAS sets the reject code and
     * returns before the token is appended (session_auth.c:381), so the walk
     * leaves nothing staged (`:486`). */
    want->auth_reject_code = 0x17;
    want->token_count = 0;
    want->interest = pre->interest;
    want->forward = pre->forward;
    want->prefix_count = pre->prefix_count;
}


/* Auth passes, so the decoded request shape IS applied, and the conflicting
 * prefix is stored and then released when the reject cannot be queued
 * (`session_namespace_sub.c:776`) -- which also zeroes its count. */
static void nsr_overlap_declare(nso_snap_t *want, const nso_snap_t *pre)
{
    (void)pre;
    want->auth_processed = 1;
    want->auth_reject_code = 0;
    want->token_count = 0;
    want->interest = (int)MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    want->forward = 1;
    want->prefix_count = 0;
}


uint64_t nsr_arm_auth(moq_session_t *s)
{
    g_nsr_seed_slot = -1;
    g_nsr_seed_ref = moq_stream_ref_from_u64(0);
    nsr_burn_request_id_0(s);
    return 2;
}


/* An ACCEPTED namespace subscription on prefix "live" at request id 2, which
 * the overlapping request at id 4 must leave completely untouched. */
static uint64_t nsr_arm_overlap(moq_session_t *s)
{
    g_nsr_seed_slot = -1;
    nsr_burn_request_id_0(s);

    g_nsr_seed_ref = moq_stream_ref_from_u64(11);
    g_nsr_seed_rid = 2;
    /* The slot the handoff will take, the generation it will carry and the
     * handle the commit will therefore mint, ALL derived before ingress --
     * so a consistently wrong slot or handle cannot define its own
     * expectation. */
    int seed_slot = -1;
    uint32_t seed_gen = 0;
    for (size_t i = 0; i < s->ns_sub_cap; i++)
        if (s->ns_subs[i].state == MOQ_NS_SUB_FREE) {
            seed_slot = (int)i;
            seed_gen = s->ns_subs[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(seed_slot >= 0);
    if (seed_slot < 0) return 4;
    /* Storage is knowable BEFORE ingress: the receive buffer is carved out of
     * the session allocation at create, so a free slot already exposes it. */
    const void *seed_pre_buf = seed_slot >= 0
        ? (const void *)s->ns_subs[seed_slot].recv_buf : NULL;
    size_t seed_pre_cap = seed_slot >= 0 ? s->ns_subs[seed_slot].recv_cap : 0;
    uint64_t seed_handle = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                           s->session_tag, seed_gen,
                                           (uint32_t)seed_slot);
    uint8_t msg[192];
    size_t n = enc_sns(msg, sizeof(msg), g_nsr_seed_rid, "live", false);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, g_nsr_seed_ref, msg, n,
                                              false, 1), (int)MOQ_OK);
    moq_ns_sub_handle_t h = MOQ_NS_SUB_HANDLE_INVALID;
    {
        int req = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_NS_SUB_REQUEST) {
                req++; h = e.u.ns_sub_request.handle;
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(req, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
        MOQ_TEST_CHECK_EQ_U64(h._opaque, seed_handle);
    }
    moq_accept_ns_sub_cfg_t ac;
    moq_accept_ns_sub_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_ns_sub(s, h, &ac, 1),
                          (int)MOQ_OK);
    /* Exactly one REQUEST_OK, decoded, with our send half left OPEN so the
     * namespace stream can carry later NAMESPACE messages. */
    {
        int sends = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a.u.send_bidi_stream.stream_ref._v == g_nsr_seed_ref._v) {
                sends++;
                MOQ_TEST_CHECK_EQ_INT(a.u.send_bidi_stream.fin ? 1 : 0, 0);
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, a.u.send_bidi_stream.data,
                                    a.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                                      (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_REQUEST_OK);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_request_ok(env.payload,
                                                   env.payload_len),
                    (int)MOQ_OK);
            } else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(sends, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                            g_nsr_seed_ref._v), seed_slot);
    g_nsr_seed_slot = seed_slot;
    MOQ_TEST_CHECK_EQ_INT(ns_sub_pool_busy(s), 1);
    MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 0);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, (size_t)0);
    /* The seed's expected record, DECLARED field by field rather than read
     * back -- slot, generation and handle all come from the pool state
     * captured before the request was fed. */
    memset(&g_nsr_seed_want, 0, sizeof(g_nsr_seed_want));
    g_nsr_seed_want.present = 1;
    g_nsr_seed_want.state = (int)MOQ_NS_SUB_ESTABLISHED;
    g_nsr_seed_want.stream_kind = (int)MOQ_STREAM_KIND_NAMESPACE_SUB;
    g_nsr_seed_want.generation = seed_gen;
    g_nsr_seed_want.handle = seed_handle;
    g_nsr_seed_want.request_id = g_nsr_seed_rid;
    g_nsr_seed_want.stream_ref = g_nsr_seed_ref._v;
    g_nsr_seed_want.ep_kind = (int)MOQ_REQ_NAMESPACE_SUB;
    g_nsr_seed_want.ep_slot = g_nsr_seed_slot;
    g_nsr_seed_want.ep_has_request_id = 1;
    g_nsr_seed_want.ep_request_id = g_nsr_seed_rid;
    g_nsr_seed_want.ep_has_stream_ref = 1;
    g_nsr_seed_want.ep_stream_ref = g_nsr_seed_ref._v;
    g_nsr_seed_want.interest = (int)MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    g_nsr_seed_want.forward = 1;
    g_nsr_seed_want.parse_complete = 1;
    g_nsr_seed_want.auth_processed = 1;
    g_nsr_seed_want.auth_committed = 1;
    g_nsr_seed_want.prefix_valid = 1;
    g_nsr_seed_want.prefix_count = 1;
    /* Its stored extent is the declared length of "live"; its receive storage
     * is the slot's own, carved out at session create and captured from the
     * FREE slot before this request was fed. */
    g_nsr_seed_want.prefix_buf_len = 4;
    g_nsr_seed_want.recv_buf = seed_pre_buf;
    g_nsr_seed_want.recv_cap = seed_pre_cap;
    {
        static const uint8_t k_live[] = { 4, 'l', 'i', 'v', 'e' };
        g_nsr_seed_want.prefix_len = sizeof(k_live);
        memcpy(g_nsr_seed_want.prefix, k_live, sizeof(k_live));
    }
    g_nsr_seed_want.recv_len = n;
    memcpy(g_nsr_seed_want.recv, msg, n);
    /* It really is that record before it becomes the conservation baseline. */
    {
        nso_snap_t got;
        nso_read(s, g_nsr_seed_slot, &got);
        failures += nso_diff(&got, &g_nsr_seed_want, "seed declared");
    }
    return 4;
}


void test_ns_sub_request_error(void)
{
    static const nsr_case_t auth_reject = {
        .name = "ns_sub auth reject", .arm = nsr_arm_auth,
        .encode = enc_sns_auth, .err_code = 0x17,
        .declare_transient = nsr_auth_declare
    };
    static const nsr_case_t overlap_reject = {
        .name = "ns_sub prefix overlap", .arm = nsr_arm_overlap,
        .encode = enc_sns_overlap,
        .err_code = MOQ_REQUEST_ERROR_PREFIX_OVERLAP,
        .declare_transient = nsr_overlap_declare
    };
    run_nsr_matrix(&auth_reject, 2);
    run_nsr_matrix(&overlap_reject, 2);
}


int durable_problems(const durable_case_t *c)
{
    int bad = 0;
    if (!c->name || !c->make || !c->arm || !c->fire || !c->want_output) bad++;
    if (!c->hooks || !c->hooks->normalize_event ||
        !c->hooks->normalize_action) bad++;
    if (!c->owner || !c->owner->capture || !c->owner->check ||
        !c->owner->check_retired) bad++;
    if (!c->owner_edges) bad++;
    if (c->extra && (!c->extra->capture || !c->extra->check_blocked ||
                     !c->extra->check_committed)) bad++;
    return bad;
}


static void check_durable_output(moq_session_t *s, const durable_case_t *c,
                                 moq_stream_ref_t ref, uint64_t handle,
                                 const char *what)
{
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, c->hooks, &got);
    collect_actions(s, c->hooks, &got);
    c->want_output(&want, ref, handle);
    failures += txs_norm_equals(&got, &want, what);
    txs_norm_free(&got);
    txs_norm_free(&want);
}


/* One terminal under one (FIN observed, ring) state. */
static void run_durable(const durable_case_t *c, bool fin_observed,
                        bool fill_ring)
{
    char what[128];
    moq_session_t *s = c->make();
    if (!s) return;
    MOQ_TEST_CHECK_EQ_INT(durable_problems(c), 0);
    uint64_t handle = 0;
    moq_stream_ref_t ref = c->arm(s, fin_observed, &handle);
    MOQ_TEST_CHECK(ref._v != 0);
    if (ref._v == 0) { moq_session_destroy(s); return; }
    check_no_output(s, c->hooks);

    if (fill_ring) fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);

    seed_snap_t owner0;
    memset(&owner0, 0, sizeof(owner0));
    c->owner->capture(s, ref, &owner0);
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);
    if (c->extra) c->extra->capture(s);
    og_graph_t graph_before;
    og_capture(s, &graph_before);
    snprintf(what, sizeof(what), "%s armed", c->name);
    failures += og_check_integrity(&graph_before, what);
    failures += c->owner_edges(&graph_before, ref, what);

    bool refused = (fill_ring && !fin_observed);
    moq_result_t want_rc = (refused && !c->refusal_is_silent)
                               ? MOQ_ERR_WOULD_BLOCK : MOQ_OK;
    MOQ_TEST_CHECK_EQ_INT((int)c->fire(s, handle), (int)want_rc);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    if (refused) {
        /* An API terminal touches no input, so the WHOLE session must be as it
         * was -- owner, snapshot, graph, output and ring alike. */
        snprintf(what, sizeof(what), "%s blocked", c->name);
        c->owner->check(s, ref, &owner0, what);
        if (c->extra) failures += c->extra->check_blocked(s, what);
        failures += txs_check_eq(s, &ref, 1, &before, what);
        check_no_output(s, c->hooks);
        {
            drain_snap_t now;
            drain_snap(s, &now);
            failures += drain_multiset_equals(&now, &ring_before, what);
            /* Not merely a valid graph: the SAME graph. An edge quietly
             * removed or repointed is still internally consistent. */
            og_graph_t g;
            og_capture(s, &g);
            failures += og_check_integrity(&g, what);
            failures += og_check_same_topology(&g, &graph_before, what);
            failures += c->owner_edges(&g, ref, what);
        }

        /* Free one declared filler and retry through the SAME production
         * route -- no peer bytes are delivered at any point. */
        moq_stream_ref_t filler = moq_stream_ref_from_u64(0x4000);
        MOQ_TEST_CHECK(drain_ref_contains(s, filler));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, filler, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t ring_freed;
        drain_snap_minus(&ring_before, filler, MOQ_DRAIN_NORMAL, &ring_freed);
        {
            drain_snap_t now;
            drain_snap(s, &now);
            snprintf(what, sizeof(what), "%s filler released", c->name);
            failures += drain_multiset_equals(&now, &ring_freed, what);
            /* Releasing an unrelated filler must not have touched the target
             * owner or the graph the retry is about to consume. */
            c->owner->check(s, ref, &owner0, what);
            og_graph_t g;
            og_capture(s, &g);
            failures += og_check_integrity(&g, what);
            failures += og_check_same_topology(&g, &graph_before, what);
            failures += c->owner_edges(&g, ref, what);
        }

        MOQ_TEST_CHECK_EQ_INT((int)c->fire(s, handle), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        snprintf(what, sizeof(what), "%s retried", c->name);
        check_durable_output(s, c, ref, handle, what);
        c->owner->check_retired(s, ref);
        {
            og_graph_t g;
            og_capture(s, &g);
            failures += og_check_integrity(&g, what);
            failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
        }
        drain_snap_t after, want_ring;
        drain_snap(s, &after);
        drain_snap_plus(&ring_freed, ref, MOQ_DRAIN_NORMAL, &want_ring);
        failures += drain_multiset_equals(&after, &want_ring, what);
        check_drain_ref_reason(s, ref, MOQ_DRAIN_NORMAL);

        /* The reference the retry took is RELEASABLE: the late FIN removes
         * exactly it, owes nothing, and cannot resurrect the owner. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        snprintf(what, sizeof(what), "%s retry released", c->name);
        {
            drain_snap_t released;
            drain_snap(s, &released);
            failures += drain_multiset_equals(&released, &ring_freed, what);
        }
        c->owner->check_retired(s, ref);
        check_no_output(s, c->hooks);
        {
            og_graph_t g;
            og_capture(s, &g);
            failures += og_check_integrity(&g, what);
            failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
        }
        c->owner->check_retired(s, ref);   /* no resurrection */
        moq_session_destroy(s);
        return;
    }

    snprintf(what, sizeof(what), "%s fired", c->name);
    check_durable_output(s, c, ref, handle, what);
    c->owner->check_retired(s, ref);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, what);
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
    }

    drain_snap_t now, want_ring;
    drain_snap(s, &now);
    if (fin_observed) {
        /* The peer half is already closed: nothing to absorb. */
        want_ring = ring_before;
    } else {
        drain_snap_plus(&ring_before, ref, MOQ_DRAIN_NORMAL, &want_ring);
        check_drain_ref_reason(s, ref, MOQ_DRAIN_NORMAL);
    }
    snprintf(what, sizeof(what), "%s granted", c->name);
    failures += drain_multiset_equals(&now, &want_ring, what);

    if (!fin_observed) {
        /* The late FIN releases exactly that reference and owes nothing. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t released;
        drain_snap(s, &released);
        snprintf(what, sizeof(what), "%s released", c->name);
        failures += drain_multiset_equals(&released, &ring_before, what);
        c->owner->check_retired(s, ref);
        check_no_output(s, c->hooks);
        {
            og_graph_t g;
            og_capture(s, &g);
            failures += og_check_integrity(&g, what);
            failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v,
                                         what);
        }
        c->owner->check_retired(s, ref);   /* no resurrection */
    }
    moq_session_destroy(s);
}


void run_durable_matrix(const durable_case_t *c)
{
    run_durable(c, false, false);   /* FIN not observed: one releasable ref */
    run_durable(c, false, true);    /* exhausted ring, FIN not observed: refuse */
    run_durable(c, true,  false);   /* FIN observed: none needed */
    run_durable(c, true,  true);    /* exhausted ring, FIN observed */
}
