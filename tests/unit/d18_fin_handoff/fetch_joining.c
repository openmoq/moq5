#include "test_cases.h"

/* -- FETCH (P2 pending-join) ----------------------------------------- */

/* A Joining FETCH buffered against a still-pending subscription. Its FIN is
 * carried by a direct req_recv_fin copy, so these rows are REGRESSION guards,
 * not expected failures: they pin the behaviour any handoff-marker
 * representation must keep.
 * Its two obligations are exercised separately -- the internal rejection
 * itself, reached before the join is ever application-visible, and the
 * drain accounting in the reject-all preflight. */

static moq_subscription_t pj_feed_pending_subscribe(moq_session_t *s,
                                                    moq_stream_ref_t ref,
                                                    uint64_t req_id)
{
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    mp.has_filter = true;
    mp.filter_type = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    mp.has_forward = true; mp.forward = 1;
    uint8_t m[160];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, sizeof(m));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe(&w, req_id, &ns, MOQ_BYTES_LITERAL("v0"),
                                      &mp), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, m,
                                              moq_buf_writer_offset(&w),
                                              false, 1), (int)MOQ_OK);
    moq_subscription_t h = MOQ_SUBSCRIPTION_INVALID;
    moq_event_t ev;
    int n_sub = 0, n_other = 0;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            n_sub++;
            h = ev.u.subscribe_request.sub;
        } else {
            n_other++;
        }
        moq_event_cleanup(&ev);
    }
    /* Exactly the one request event, and nothing else: a duplicate or an
     * unrelated event would otherwise be absorbed by the fixture. */
    MOQ_TEST_CHECK_EQ_INT(n_sub, 1);
    MOQ_TEST_CHECK_EQ_INT(n_other, 0);
    moq_action_t a;
    int n_act = 0;
    while (moq_session_poll_actions(s, &a, 1) > 0) { n_act++; moq_action_cleanup(&a); }
    MOQ_TEST_CHECK_EQ_INT(n_act, 0);
    return h;
}


typedef struct pj_ctx {
    moq_stream_ref_t sref;
    moq_stream_ref_t jref;
    moq_subscription_t sub;   /* the subscription the join buffers against */
} pj_ctx_t;


static pj_ctx_t pj_ctx = { { 1 }, { 5 }, { 0 } };


static moq_result_t pj_feed_join(moq_session_t *s, moq_stream_ref_t ref,
                                 uint64_t req_id, uint64_t join_req_id,
                                 bool fin)
{
    moq_d18_fetch_t f;
    memset(&f, 0, sizeof(f));
    f.request_id = req_id;
    f.fetch_type = 2;
    f.joining_request_id = join_req_id;
    f.joining_start = 1;
    uint8_t m[160];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, sizeof(m));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&w, &f), (int)MOQ_OK);
    return moq_session_on_bidi_stream_bytes(s, ref, m,
                                            moq_buf_writer_offset(&w), fin, 1);
}


/* Descriptor plumbing: the join is fed on its own bidi, and its terminal is
 * the INTERNAL rejection -- reached by rejecting the subscription, never by
 * an application call naming the join. */
static moq_result_t pj_ops_feed(moq_session_t *s, void *ctx,
                                moq_stream_ref_t ref, bool fin)
{
    (void)ctx;
    return pj_feed_join(s, ref, 2, 0, fin);
}


static moq_result_t pj_ops_internal_reject(moq_session_t *s, void *ctx,
                                           moq_stream_ref_t ref)
{
    pj_ctx_t *c = (pj_ctx_t *)ctx;
    (void)ref;
    moq_reject_subscribe_cfg_t rj;
    moq_reject_subscribe_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    return moq_session_reject_subscribe(s, c->sub, &rj, 1);
}


static void pj_hook_capture(const moq_session_t *s, void *vctx, void *vst)
{
    fetch_ctx_t tmp = { 0, ((pj_ctx_t *)vctx)->jref, 0 };
    fetch_hook_capture(s, &tmp, vst);
}


static int pj_hook_check(const moq_session_t *s, void *vctx, const void *vst)
{
    fetch_ctx_t tmp = { 0, ((pj_ctx_t *)vctx)->jref, 0 };
    return fetch_hook_check(s, &tmp, vst);
}


static bool pj_norm_event(const moq_event_t *ev, void *vctx,
                          txs_norm_vec_t *out)
{
    fetch_ctx_t tmp = { 0, ((pj_ctx_t *)vctx)->jref, 0 };
    return fetch_norm_event(ev, &tmp, out);
}


static fin_owner_case_t pj_family(void)
{
    fin_owner_case_t f;
    memset(&f, 0, sizeof(f));
    f.name = "pending-join";
    f.ctx = &pj_ctx;
    /* The only family so far whose terminal is internal. */
    f.caps = FIN_TERM_INTERNAL_REJECT;
    f.feed = pj_ops_feed;
    f.owner_kind = MOQ_REQ_FETCH;
    f.owner_role = (int)MOQ_FETCH_ROLE_PUBLISHER;
    f.owner_state = (int)MOQ_FETCH_PENDING_JOIN;
    f.internal_reject = pj_ops_internal_reject;
    f.hooks.ctx = &pj_ctx;
    f.hooks.capture = pj_hook_capture;
    f.hooks.check = pj_hook_check;
    f.hooks.normalize_event = pj_norm_event;
    f.hooks.normalize_action = pub_norm_action;
    return f;
}


void drain_snap(const moq_session_t *s, drain_snap_t *d)
{
    memset(d, 0, sizeof(*d));
    if (s->drain_ref_count > DRAIN_SNAP_MAX) {
        /* Record the overflow and copy NOTHING: continuing would write past
         * the array, and the comparator refuses an overflowed snapshot. */
        d->overflow = 1;
        MOQ_TEST_CHECK(!d->overflow);
        return;
    }
    d->count = s->drain_ref_count;
    for (size_t i = 0; i < d->count; i++) {
        d->ref[i] = s->drain_refs[i];
        d->reason[i] = s->drain_ref_reasons[i];
    }
}


/* Multiset comparison: order is an implementation detail (removal swaps), so
 * each pair in `want` must be matched exactly once in `have`. */
int drain_multiset_equals(const drain_snap_t *have,
                          const drain_snap_t *want, const char *what)
{
    if (have->overflow || want->overflow) {
        TXS_DIAG("TXN %s: overflowed drain snapshot is incomparable\n", what);
        return 1;
    }
    if (have->count != want->count) {
        TXS_DIAG("TXN %s: %zu drain refs, expected %zu\n", what,
                 have->count, want->count);
        return 1;
    }
    bool used[DRAIN_SNAP_MAX] = { false };
    for (size_t i = 0; i < want->count; i++) {
        bool found = false;
        for (size_t j = 0; j < have->count; j++) {
            if (used[j]) continue;
            if (have->ref[j] == want->ref[i] &&
                have->reason[j] == want->reason[i]) {
                used[j] = true; found = true; break;
            }
        }
        if (!found) {
            TXS_DIAG("TXN %s: missing drain ref %llu reason %u\n", what,
                     (unsigned long long)want->ref[i],
                     (unsigned)want->reason[i]);
            return 1;
        }
    }
    return 0;
}


/* `base` plus exactly the one reference `extra` with the DECLARED reason. The
 * reason is never read back from the live ring: deriving the expectation from
 * the result would accept whatever the product chose, so a strict-versus-
 * normal substitution would pass. */
void drain_snap_plus(const drain_snap_t *base, moq_stream_ref_t extra,
                     uint8_t reason, drain_snap_t *out)
{
    *out = *base;
    if (out->count >= DRAIN_SNAP_MAX) {
        out->overflow = 1;
        MOQ_TEST_CHECK(out->count < DRAIN_SNAP_MAX);
        return;
    }
    out->ref[out->count] = extra._v;
    out->reason[out->count] = reason;
    out->count++;
}


/* `base` minus exactly the one (ref, reason) pair. The expectation is
 * DECLARED, so a release that removed the wrong reference cannot be adopted
 * as the new baseline. */
void drain_snap_minus(const drain_snap_t *base, moq_stream_ref_t gone,
                      uint8_t reason, drain_snap_t *out)
{
    memset(out, 0, sizeof(*out));
    out->overflow = base->overflow;
    bool removed = false;
    for (size_t i = 0; i < base->count; i++) {
        if (!removed && base->ref[i] == gone._v && base->reason[i] == reason) {
            removed = true;
            continue;
        }
        if (out->count >= DRAIN_SNAP_MAX) { out->overflow = 1; break; }
        out->ref[out->count] = base->ref[i];
        out->reason[out->count] = base->reason[i];
        out->count++;
    }
    if (!removed)
        TXS_DIAG("TXN: drain ref %llu reason %u absent from the base ring\n",
                 (unsigned long long)gone._v, (unsigned)reason);
    MOQ_TEST_CHECK(removed);
}


/* Present with the declared reason -- membership alone would accept a
 * reference added under the wrong drain reason. */
void check_drain_ref_reason(const moq_session_t *s,
                            moq_stream_ref_t ref, uint8_t reason)
{
    MOQ_TEST_CHECK(drain_ref_contains(s, ref));
    bool ok = false;
    for (size_t i = 0; i < s->drain_ref_count; i++)
        if (s->drain_refs[i] == ref._v && s->drain_ref_reasons[i] == reason)
            ok = true;
    if (!ok)
        TXS_DIAG("TXN: drain ref %llu not held under reason %u\n",
                 (unsigned long long)ref._v, (unsigned)reason);
    MOQ_TEST_CHECK(ok);
}


/* Exactly which references the ring holds, not merely how many: adding the
 * wrong stream's reference would keep the count right and the behaviour
 * wrong. */
static void check_drain_members(moq_session_t *s, const moq_stream_ref_t *want,
                                size_t n, size_t base, uint8_t reason)
{
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, base + n);
    for (size_t i = 0; i < n; i++)
        check_drain_ref_reason(s, want[i], reason);
}


/* Zero events and exactly the two ordered terminal responses the reject-all
 * path owes: the subscription's configured error on its own bidi, then the
 * joining rejection on the join's. */
static void check_pj_reject_output(moq_session_t *s, const txs_op_hooks_t *h,
                                   moq_stream_ref_t sref,
                                   moq_stream_ref_t jref)
{
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, h, &got);
    MOQ_TEST_CHECK_EQ_SIZE(got.count, 0);
    collect_actions(s, h, &got);
    want_reject_error_code(&want, sref, MOQ_REQUEST_ERROR_NOT_SUPPORTED);
    want_reject_error_code(&want, jref,
                           MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID);
    failures += txs_norm_equals(&got, &want, "pending-join reject");
    txs_norm_free(&got);
    txs_norm_free(&want);
}


int sub_busy_count(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state != MOQ_SUB_FREE) n++;
    return n;
}


void sub_owner_capture(const moq_session_t *s, moq_stream_ref_t ref,
                       sub_owner_state_t *st)
{
    memset(st, 0, sizeof(*st));
    st->busy = sub_busy_count(s);
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if (ep.kind != MOQ_REQ_SUBSCRIPTION) return;
    if (ep.slot < 0 || (size_t)ep.slot >= s->sub_cap) {
        TXS_DIAG("TXN: subscription slot %d out of range (cap %zu)\n",
                 ep.slot, s->sub_cap);
        return;
    }
    const moq_sub_entry_t *e = &s->subs[ep.slot];
    st->present = 1;
    st->slot = ep.slot;
    st->has_stream_ref = ep.has_stream_ref ? 1 : 0;
    st->registry_ref = ep.stream_ref._v;
    st->state = (int)e->state;
    st->role = (int)e->role;
    st->handle = e->handle._opaque;
    st->req_stream_ref = e->rs.ref._v;
    st->rs.recv_fin = e->rs.recv_fin;
}


static int sub_owner_check(const moq_session_t *s, moq_stream_ref_t ref,
                           const sub_owner_state_t *want)
{
    sub_owner_state_t now;
    sub_owner_capture(s, ref, &now);
    int bad = 0;
    if (now.present != want->present) bad++;
    if (now.state != want->state) bad++;
    if (now.role != want->role) bad++;
    if (now.handle != want->handle) bad++;
    if (now.req_stream_ref != want->req_stream_ref) bad++;
    if (now.rs.recv_fin != want->rs.recv_fin) bad++;
    if (now.slot != want->slot) bad++;
    if (now.has_stream_ref != want->has_stream_ref) bad++;
    if (now.registry_ref != want->registry_ref) bad++;
    if (now.busy != want->busy) bad++;
    if (bad)
        TXS_DIAG("TXN subscription owner: present %d state %d role %d "
                 "handle %llu ref %llu fin %d busy %d, expected %d/%d/%d/"
                 "%llu/%llu/%d/%d\n", now.present, now.state, now.role,
                 (unsigned long long)now.handle,
                 (unsigned long long)now.req_stream_ref, now.rs.recv_fin,
                 now.busy, want->present, want->state, want->role,
                 (unsigned long long)want->handle,
                 (unsigned long long)want->req_stream_ref, want->rs.recv_fin,
                 want->busy);
    return bad;
}


/* The two owners this fixture depends on, asserted before any snapshot so
 * equality cannot preserve an invalid baseline. The join is checked against
 * the DESCRIPTOR's declared kind, role and state. */
static void check_pj_owners(const fin_owner_case_t *f, moq_session_t *s,
                            moq_stream_ref_t sref, moq_stream_ref_t jref,
                            bool fin_in_request, const fetch_ident_t *id)
{
    /* The subscription's ABSOLUTE shape, so a captured baseline can never
     * preserve an invalid one: an inbound SUBSCRIBE makes us the publisher,
     * on its own request bidi, with a live handle and one pool slot. */
    sub_owner_state_t so;
    sub_owner_capture(s, sref, &so);
    MOQ_TEST_CHECK(so.present);
    MOQ_TEST_CHECK_EQ_INT(so.state, (int)MOQ_SUB_PENDING_PUBLISHER);
    MOQ_TEST_CHECK_EQ_INT(so.role, (int)MOQ_SUB_ROLE_PUBLISHER);
    MOQ_TEST_CHECK_EQ_INT(so.rs.recv_fin, 0);
    MOQ_TEST_CHECK(so.handle != 0);
    MOQ_TEST_CHECK_EQ_U64(so.req_stream_ref, sref._v);
    MOQ_TEST_CHECK(so.has_stream_ref);
    MOQ_TEST_CHECK_EQ_U64(so.registry_ref, sref._v);
    MOQ_TEST_CHECK_EQ_INT(so.busy, 1);

    failures += fetch_owner_present_problems(s, jref, f->owner_kind,
                                             f->owner_role, f->owner_state);
    /* The buffered join surfaces no request event, so its own entry handle is
     * what must equal the independently packed value; identity is declared
     * before ingress, never adopted from the committed entry. */
    failures += fetch_ident_problems(s, id, f->owner_role, f->owner_state,
                                     "pending join");
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "pending join");
        failures += fetch_ident_edges(&g, id, "pending join");
    }
    const moq_fetch_entry_t *e = fetch_by_ref(s, jref);
    MOQ_TEST_CHECK(e != NULL);
    if (e) {
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, id->handle);
        MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin, fin_in_request ? 1 : 0);
        /* The FIN reaches this owner through the generic destination handoff,
         * which installs it as transitional ownership and drives this family's
         * own FIN handling; that consumption is synchronous, so the marker is
         * already clear -- and with no FIN neither fact is set. */
        MOQ_TEST_CHECK_EQ_INT(e->handoff_fin_pending, 0);
    }
}


/* Both owners retired: both pools empty and both registry keys absent. */
static void check_pj_all_retired(moq_session_t *s, moq_stream_ref_t sref,
                                 moq_stream_ref_t jref)
{
    MOQ_TEST_CHECK_EQ_INT(fetch_busy_count(s), 0);
    MOQ_TEST_CHECK_EQ_INT(sub_busy_count(s), 0);
    check_unregistered(s, jref);
    check_unregistered(s, sref);
}


/* Leave exactly `free_slots` references available in the ring. */
static void fill_drain_ring_leaving(moq_session_t *s, size_t free_slots)
{
    uint64_t next = 0x5000;
    MOQ_TEST_CHECK(s->drain_ref_cap > free_slots);
    while (s->drain_ref_count + free_slots < s->drain_ref_cap)
        MOQ_TEST_CHECK(drain_ref_add(s, moq_stream_ref_from_u64(next++)));
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, s->drain_ref_cap - free_slots);
}


/* Obligation 1 -- the INTERNAL rejection, reached before the join was ever
 * application-visible: each join takes a drain reference only if its own
 * close has not been seen, and the reference taken is its own. */
static void run_pj_internal_reject(const fin_owner_case_t *f,
                                   bool fin_in_request)
{
    pj_ctx_t *c = (pj_ctx_t *)f->ctx;
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    c->sub = pj_feed_pending_subscribe(s, c->sref, 0);
    MOQ_TEST_CHECK(c->sub._opaque != MOQ_SUBSCRIPTION_INVALID._opaque);
    fetch_ident_t id = fetch_declare_ident(s, 2, c->jref);
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, c->jref, fin_in_request),
                          (int)MOQ_OK);
    check_pj_owners(f, s, c->sref, c->jref, fin_in_request, &id);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    /* Never application-visible: the buffered join surfaces nothing. */
    check_no_output(s, &f->hooks);

    MOQ_TEST_CHECK_EQ_INT((int)f->internal_reject(s, f->ctx, c->jref),
                          (int)MOQ_OK);

    /* Exactly which references are held: the subscription's own bidi always,
     * the join's only while its close is outstanding. */
    if (fin_in_request) {
        moq_stream_ref_t want[1] = { c->sref };
        check_drain_members(s, want, 1, 0, MOQ_DRAIN_NORMAL);
        MOQ_TEST_CHECK(!drain_ref_contains(s, c->jref));
    } else {
        moq_stream_ref_t want[2] = { c->sref, c->jref };
        check_drain_members(s, want, 2, 0, MOQ_DRAIN_NORMAL);
    }
    check_pj_all_retired(s, c->sref, c->jref);
    check_pj_reject_output(s, &f->hooks, c->sref, c->jref);

    /* Each outstanding FIN releases ITS OWN reference and no other. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, c->sref, NULL, 0, true, 1),
        (int)MOQ_OK);
    MOQ_TEST_CHECK(!drain_ref_contains(s, c->sref));
    if (!fin_in_request) {
        MOQ_TEST_CHECK(drain_ref_contains(s, c->jref));
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 1);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, c->jref, NULL, 0, true, 1),
            (int)MOQ_OK);
    }
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    check_no_output(s, &f->hooks);
    moq_session_destroy(s);
}


/* Obligation 2 -- the reject-all PREFLIGHT: its drain accounting decides
 * whether the whole rejection can proceed. */
static void run_pj_preflight(const fin_owner_case_t *f, bool fin_in_request)
{
    pj_ctx_t *c = (pj_ctx_t *)f->ctx;
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    c->sub = pj_feed_pending_subscribe(s, c->sref, 0);
    MOQ_TEST_CHECK(c->sub._opaque != MOQ_SUBSCRIPTION_INVALID._opaque);
    fetch_ident_t id = fetch_declare_ident(s, 2, c->jref);
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, c->jref, fin_in_request),
                          (int)MOQ_OK);
    check_pj_owners(f, s, c->sref, c->jref, fin_in_request, &id);
    check_no_output(s, &f->hooks);

    /* Exactly one reference left: enough for the subscription alone. */
    fill_drain_ring_leaving(s, 1);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);

    moq_stream_ref_t refs[2] = { c->sref, c->jref };
    txs_snapshot_t before;
    txs_capture(s, refs, 2, &before);
    expect_after_call_prepare(&before);
    fetch_hook_state_t owner_before;
    f->hooks.capture(s, f->ctx, &owner_before);
    sub_owner_state_t sub_before;
    sub_owner_capture(s, c->sref, &sub_before);

    moq_result_t want_rc = fin_in_request ? MOQ_OK : MOQ_ERR_WOULD_BLOCK;
    MOQ_TEST_CHECK_EQ_INT((int)f->internal_reject(s, f->ctx, c->jref),
                          (int)want_rc);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        failures += txs_check_eq(s, refs, 2, &before, f->name);
        failures += f->hooks.check(s, f->ctx, &owner_before);
        failures += sub_owner_check(s, c->sref, &sub_before);
        /* Exact conservation: the ring is the identical multiset. */
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before, "pj blocked");
        check_pj_owners(f, s, c->sref, c->jref, fin_in_request, &id);
        check_no_output(s, &f->hooks);
        moq_session_destroy(s);
        return;
    }

    /* Exactly the prior multiset plus the subscription's own reference. */
    drain_snap_t now, want_ring;
    drain_snap(s, &now);
    drain_snap_plus(&ring_before, c->sref, MOQ_DRAIN_NORMAL, &want_ring);
    failures += drain_multiset_equals(&now, &want_ring, "pj granted");
    MOQ_TEST_CHECK(!drain_ref_contains(s, c->jref));
    check_pj_all_retired(s, c->sref, c->jref);
    check_pj_reject_output(s, &f->hooks, c->sref, c->jref);

    /* Feeding that FIN restores the prior multiset exactly, with no output. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, c->sref, NULL, 0, true, 1),
        (int)MOQ_OK);
    drain_snap_t after;
    drain_snap(s, &after);
    failures += drain_multiset_equals(&after, &ring_before, "pj released");
    check_no_output(s, &f->hooks);
    moq_session_destroy(s);
}


void test_pending_join(void)
{
    fin_owner_case_t f = pj_family();
    MOQ_TEST_CHECK_EQ_INT(fin_owner_problems(&f), 0);
    run_pj_internal_reject(&f, false);
    run_pj_internal_reject(&f, true);
    run_pj_preflight(&f, true);
    run_pj_preflight(&f, false);
}
