#include "test_cases.h"

static size_t encode_fetch_msg(uint8_t *buf, size_t cap, uint64_t request_id)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live"),
                            MOQ_BYTES_LITERAL("eu") };
    moq_d18_fetch_t f;
    memset(&f, 0, sizeof(f));
    f.request_id = request_id;
    f.fetch_type = 1;
    f.track_namespace = (moq_namespace_t){ parts, 2 };
    f.track_name = MOQ_BYTES_LITERAL("vid");
    /* Deliberately non-default: a field the normalizer drops would otherwise
     * compare equal against a zero-filled expectation. */
    f.start = (moq_d18_location_t){ 3, 4 };
    f.end = (moq_d18_location_t){ 11, 6 };
    f.params.has_subscriber_priority = true;
    f.params.subscriber_priority = 9;
    f.params.has_group_order = true;
    f.params.group_order = MOQ_GROUP_ORDER_DESCENDING;
    f.params.auth_token_count = 1;
    f.params.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    f.params.auth_tokens[0].token_type = 5;
    f.params.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("tok");
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&w, &f), (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}


int fetch_busy_count(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state != MOQ_FETCH_FREE) n++;
    return n;
}


const moq_fetch_entry_t *fetch_by_ref(const moq_session_t *s,
                                      moq_stream_ref_t ref)
{
    for (size_t i = 0; i < s->fetch_cap; i++) {
        const moq_fetch_entry_t *e = &s->fetches[i];
        if (e->state != MOQ_FETCH_FREE && e->rs.ref._v == ref._v)
            return e;
    }
    return NULL;
}


fetch_ident_t fetch_declare_ident(const moq_session_t *s,
                                  uint64_t request_id,
                                  moq_stream_ref_t ref)
{
    fetch_ident_t d;
    memset(&d, 0, sizeof(d));
    d.slot = -1;
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state == MOQ_FETCH_FREE) {
            d.slot = (int)i;
            d.gen = s->fetches[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(d.slot >= 0);
    d.request_id = request_id;
    d.ref = ref;
    if (d.slot >= 0)
        d.handle = moq_handle_pack(MOQ_HANDLE_POOL_FETCH, s->session_tag,
                                   d.gen, (uint32_t)d.slot);
    return d;
}


/* The committed entry and its stream-ref endpoint must match the declaration
 * exactly -- physical slot, generation, handle, request id, role, state, and
 * the registry edge's own ref/slot. Returns a problem count. */
int fetch_ident_problems(const moq_session_t *s,
                         const fetch_ident_t *d,
                         int want_role, int want_state,
                         const char *what)
{
    int bad = 0;
    const moq_fetch_entry_t *e = fetch_by_ref(s, d->ref);
    if (!e) {
        TXS_DIAG("TXN fetch ident %s: no owner behind ref %llu\n", what,
                 (unsigned long long)d->ref._v);
        return 1;
    }
    if ((int)(e - s->fetches) != d->slot) bad++;
    if (e->generation != d->gen) bad++;
    if (e->handle._opaque != d->handle) bad++;
    if (e->request_id != d->request_id) bad++;
    if ((int)e->role != want_role) bad++;
    if ((int)e->state != want_state) bad++;
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, d->ref);
    if ((int)ep.kind != (int)MOQ_REQ_FETCH) bad++;
    else {
        if (ep.slot != d->slot) bad++;
        if (!ep.has_stream_ref) bad++;
        else if (ep.stream_ref._v != d->ref._v) bad++;
    }
    if (bad)
        TXS_DIAG("TXN fetch ident %s: slot %d gen %u handle %llu rid %llu "
                 "role %d state %d endpoint(kind %d slot %d), expected "
                 "slot %d gen %u handle %llu rid %llu role %d state %d "
                 "endpoint(kind %d slot %d)\n", what,
                 (int)(e - s->fetches), e->generation,
                 (unsigned long long)e->handle._opaque,
                 (unsigned long long)e->request_id, (int)e->role,
                 (int)e->state, (int)ep.kind, ep.slot,
                 d->slot, d->gen, (unsigned long long)d->handle,
                 (unsigned long long)d->request_id, want_role, want_state,
                 (int)MOQ_REQ_FETCH, d->slot);
    return bad;
}


/* The declared owner's EXACT graph edge set: one stream-ref edge from its
 * request bidi, and nothing else anywhere pointing at it. */
int fetch_ident_edges(const og_graph_t *g, const fetch_ident_t *d,
                      const char *what)
{
    og_edge_spec_t want[1];
    want[0].domain = OG_DOM_REQ_STREAMREF;
    want[0].key = d->ref._v;
    return og_check_owner_edges(g, (int)MOQ_REQ_FETCH, d->slot, want, 1, what);
}


moq_result_t fetch_feed(moq_session_t *s, void *ctx,
                        moq_stream_ref_t ref, bool fin)
{
    fetch_ctx_t *c = (fetch_ctx_t *)ctx;
    uint8_t msg[192];
    size_t n = encode_fetch_msg(msg, sizeof(msg), c->request_id);
    return moq_session_on_bidi_stream_bytes(s, ref, msg, n, fin, 1);
}


static moq_result_t fetch_app_reject(moq_session_t *s, void *ctx,
                                     uint64_t handle)
{
    (void)ctx;
    moq_fetch_t fh;
    fh._opaque = handle;
    moq_reject_fetch_cfg_t rj;
    moq_reject_fetch_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    return moq_session_reject_fetch(s, fh, &rj, 1);
}


void fetch_hook_capture(const moq_session_t *s, void *vctx, void *vst)
{
    fetch_ctx_t *c = (fetch_ctx_t *)vctx;
    fetch_hook_state_t *st = (fetch_hook_state_t *)vst;
    memset(st, 0, sizeof(*st));
    st->busy = fetch_busy_count(s);
    const moq_fetch_entry_t *e = fetch_by_ref(s, c->ref);
    if (!e) return;
    st->present = 1;
    st->handle = e->handle._opaque;
    st->req_stream_ref = e->rs.ref._v;
    st->rs.recv_fin = e->rs.recv_fin;
    st->handoff_fin = e->handoff_fin_pending;
    st->state = (int)e->state;
    st->role = (int)e->role;
}


int fetch_hook_check(const moq_session_t *s, void *vctx,
                     const void *vst)
{
    fetch_hook_state_t now;
    fetch_hook_capture(s, vctx, &now);
    const fetch_hook_state_t *want = (const fetch_hook_state_t *)vst;
    int bad = 0;
    if (now.present != want->present) bad++;
    if (now.handle != want->handle) bad++;
    if (now.req_stream_ref != want->req_stream_ref) bad++;
    if (now.rs.recv_fin != want->rs.recv_fin) bad++;
    if (now.handoff_fin != want->handoff_fin) bad++;
    if (now.state != want->state) bad++;
    if (now.role != want->role) bad++;
    if (now.busy != want->busy) bad++;
    if (bad)
        TXS_DIAG("TXN fetch hooks: present %d handle %llu ref %llu fin %d "
                 "state %d role %d busy %d, expected present %d handle %llu "
                 "ref %llu fin %d state %d role %d busy %d\n",
                 now.present, (unsigned long long)now.handle,
                 (unsigned long long)now.req_stream_ref, now.rs.recv_fin,
                 now.state, now.role, now.busy, want->present,
                 (unsigned long long)want->handle,
                 (unsigned long long)want->req_stream_ref, want->rs.recv_fin,
                 want->state, want->role, want->busy);
    return bad;
}


bool fetch_norm_event(const moq_event_t *ev, void *vctx,
                      txs_norm_vec_t *out)
{
    fetch_ctx_t *c = (fetch_ctx_t *)vctx;
    txs_img_t im;
    txs_img_init(&im);
    switch (ev->kind) {
    case MOQ_EVENT_FETCH_REQUEST: {
        /* The complete surfaced inventory, matching jf_norm_event: anything
         * omitted here could be wrong on the wire and still compare equal. */
        const moq_fetch_request_event_t *fr = &ev->u.fetch_request;
        c->last_fetch = fr->fetch._opaque;
        txs_img_u64(&im, fr->fetch._opaque);
        txs_img_u64(&im, fr->joining_sub._opaque);
        if (!txs_img_parts(&im, "fetch_request namespace",
                           fr->track_namespace.parts,
                           fr->track_namespace.count,
                           MOQ_DECODED_MAX_NAMESPACE_PARTS))
            return false;
        txs_img_bytes(&im, fr->track_name.data, fr->track_name.len);
        txs_img_u64(&im, fr->start_group);
        txs_img_u64(&im, fr->start_object);
        txs_img_u64(&im, fr->end_group);
        txs_img_u64(&im, fr->end_object);
        txs_img_u64(&im, fr->subscriber_priority);
        txs_img_u64(&im, (uint64_t)fr->group_order);
        if (!txs_img_tokens(&im, "fetch_request tokens", fr->tokens,
                            fr->token_count, MOQ_DECODED_MAX_TOKENS))
            return false;
        break;
    }
    case MOQ_EVENT_FETCH_CANCELLED:
        txs_img_u64(&im, ev->u.fetch_cancelled.fetch._opaque);
        break;
    default:
        TXS_DIAG("TXN fetch: unnormalized event kind %u\n",
                 (unsigned)ev->kind);
        return false;
    }
    return txs_norm_append_img(out, ev->kind, &im);
}


static bool fetch_norm_action(const moq_action_t *a, void *vctx,
                              txs_norm_vec_t *out)
{
    return pub_norm_action(a, vctx, out);
}


/* Built independently of the normalizer's own reading of the event, from the
 * values the fixture put on the wire. */
static void want_fetch_request(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    txs_img_u64(&im, 0);                    /* standalone: no joining sub */
    txs_img_u64(&im, 2);                    /* namespace parts */
    txs_img_bytes(&im, (const uint8_t *)"live", 4);
    txs_img_bytes(&im, (const uint8_t *)"eu", 2);
    txs_img_bytes(&im, (const uint8_t *)"vid", 3);
    txs_img_u64(&im, 3);                    /* start group */
    txs_img_u64(&im, 4);                    /* start object */
    txs_img_u64(&im, 11);                   /* end group */
    txs_img_u64(&im, 6);                    /* end object */
    txs_img_u64(&im, 9);                    /* subscriber priority */
    txs_img_u64(&im, (uint64_t)MOQ_GROUP_ORDER_DESCENDING);
    txs_img_u64(&im, 1);                    /* token count */
    txs_img_u64(&im, 5);                    /* token type */
    txs_img_bytes(&im, (const uint8_t *)"tok", 3);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_FETCH_REQUEST, &im));
}


fetch_ctx_t fetch_ctx = { 0, { 1 }, 0 };


static fin_owner_case_t fetch_family(void)
{
    fin_owner_case_t f;
    memset(&f, 0, sizeof(f));
    f.name = "fetch";
    f.ctx = &fetch_ctx;
    f.caps = FIN_TERM_APP_REJECT;
    f.feed = fetch_feed;
    f.owner_kind = MOQ_REQ_FETCH;
    f.owner_role = (int)MOQ_FETCH_ROLE_PUBLISHER;
    f.owner_state = (int)MOQ_FETCH_PENDING_PUBLISHER;
    f.app_reject = fetch_app_reject;
    f.hooks.ctx = &fetch_ctx;
    f.hooks.capture = fetch_hook_capture;
    f.hooks.check = fetch_hook_check;
    f.hooks.normalize_event = fetch_norm_event;
    f.hooks.normalize_action = fetch_norm_action;
    return f;
}


/* The declared owner, present with the declared role and state, and keyed in
 * the registry. Returns a problem count. */
int fetch_owner_present_problems(const moq_session_t *s,
                                 moq_stream_ref_t ref,
                                 moq_request_kind_t want_kind,
                                 int want_role, int want_state)
{
    int bad = 0;
    const moq_fetch_entry_t *e = fetch_by_ref(s, ref);
    if (!e) return 1;
    if ((int)e->role != want_role) bad++;
    if ((int)e->state != want_state) bad++;
    if (fetch_busy_count(s) != 1) bad++;
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if ((int)ep.kind != (int)want_kind) bad++;
    else if (ep.slot != (int)(e - s->fetches)) bad++;
    else if (!ep.has_stream_ref || ep.stream_ref._v != ref._v) bad++;
    if (bad)
        TXS_DIAG("TXN fetch owner: kind %d role %d state %d, expected "
                 "kind %d role %d state %d\n", (int)ep.kind, (int)e->role,
                 (int)e->state, (int)want_kind, want_role, want_state);
    return bad;
}


static void run_fetch_case(const fin_owner_case_t *f, bool fin_in_request,
                           bool fill_ring, bool reject_now)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    fetch_ctx_t *c = (fetch_ctx_t *)f->ctx;
    moq_stream_ref_t ref = c->ref;
    c->last_fetch = 0;

    /* The destination owner's identity, declared from pool state BEFORE the
     * request is fed, so the checks below are absolute rather than adopted. */
    fetch_ident_t id = fetch_declare_ident(s, c->request_id, ref);

    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, ref, fin_in_request),
                          (int)MOQ_OK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_fetch_request(&want, c->last_fetch);
    failures += txs_norm_equals(&got, &want, f->name);
    txs_norm_free(&got);
    txs_norm_free(&want);

    /* Latch and RETAIN: the owner survives the peer's close, and the close is
     * recorded on it. The owner still being present is the precondition here,
     * never the proof that the FIN was consumed. */
    failures += fetch_owner_present_problems(s, ref, f->owner_kind,
                                             f->owner_role, f->owner_state);
    failures += fetch_ident_problems(s, &id, f->owner_role, f->owner_state,
                                     f->name);
    /* The event surfaced the independently packed handle. */
    MOQ_TEST_CHECK_EQ_U64(c->last_fetch, id.handle);
    {   /* and the owner carries exactly its own request-bidi edge. */
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, f->name);
        failures += fetch_ident_edges(&g, &id, f->name);
    }
    fetch_hook_state_t owner;
    f->hooks.capture(s, f->ctx, &owner);
    MOQ_TEST_CHECK(owner.present);
    MOQ_TEST_CHECK_EQ_U64(owner.handle, c->last_fetch);
    MOQ_TEST_CHECK_EQ_INT(owner.rs.recv_fin, fin_in_request ? 1 : 0);
    /* Both publisher-role FETCH states consume a handed-over FIN synchronously
     * into the durable latch, so the transitional fact never survives the call
     * that installed it and the terminals below read the latch. */
    MOQ_TEST_CHECK_EQ_INT(owner.handoff_fin, 0);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    check_no_output(s, &f->hooks);

    size_t filled = 0;
    if (fill_ring) { fill_drain_ring(s); filled = s->drain_ref_count; }
    if (!reject_now) { moq_session_destroy(s); return; }

    bool expect_drain = !fin_in_request;
    moq_result_t want_rc = (expect_drain && fill_ring) ? MOQ_ERR_WOULD_BLOCK
                                                       : MOQ_OK;
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);
    fetch_hook_state_t owner_before;
    f->hooks.capture(s, f->ctx, &owner_before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, c->last_fetch),
                          (int)want_rc);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        failures += txs_check_eq(s, &ref, 1, &before, f->name);
        failures += f->hooks.check(s, f->ctx, &owner_before);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
        check_no_output(s, &f->hooks);
        moq_session_destroy(s);
        return;
    }

    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count,
                           expect_drain ? filled + 1 : filled);
    MOQ_TEST_CHECK_EQ_INT(fetch_busy_count(s), 0);
    check_unregistered(s, ref);
    check_reject_only_output(s, &f->hooks, ref, f->name);

    if (expect_drain) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
        /* Consuming the late FIN must not resurrect the retired owner. */
        MOQ_TEST_CHECK_EQ_INT(fetch_busy_count(s), 0);
        check_unregistered(s, ref);
        check_no_output(s, &f->hooks);
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(s);
}


/* Clear-on-reuse for the FETCH pool: a same-call FIN consumed by one fetch must
 * not follow its slot to the next one. The declared topology -- slot, both
 * generations and both packed handles -- is derived BEFORE any ingress, so the
 * evidence rests on the fixture rather than on whichever slot the allocator
 * happened to pick. */
static void run_fetch_slot_reuse_case(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    fetch_ctx_t *c = (fetch_ctx_t *)f->ctx;

    int slot = -1;
    uint32_t gen_first = 0;
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state == MOQ_FETCH_FREE) {
            slot = (int)i;
            gen_first = s->fetches[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { moq_session_destroy(s); return; }
    /* fetch_free_entry advances the generation once. */
    uint32_t gen_reuse = (gen_first + 1) | 1u;
    uint64_t handle_first = moq_handle_pack(MOQ_HANDLE_POOL_FETCH,
                                            s->session_tag, gen_first,
                                            (uint32_t)slot);
    uint64_t handle_reuse = moq_handle_pack(MOQ_HANDLE_POOL_FETCH,
                                            s->session_tag, gen_reuse,
                                            (uint32_t)slot);
    MOQ_TEST_CHECK(handle_first != handle_reuse);

    /* 1. A standalone FETCH carrying FIN lands in the declared slot and
     *    consumes the marker into the durable latch, retaining the owner. */
    moq_stream_ref_t first = c->ref;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, first, true), (int)MOQ_OK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_fetch_request(&want, c->last_fetch);
    failures += txs_norm_equals(&got, &want, "fetch reuse first");
    txs_norm_free(&got);
    txs_norm_free(&want);
    MOQ_TEST_CHECK_EQ_U64(c->last_fetch, handle_first);
    {
        const moq_fetch_entry_t *e = fetch_by_ref(s, first);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_INT((int)(e - s->fetches), slot);
            MOQ_TEST_CHECK_EQ_U64(e->generation, gen_first);
            MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin, 1);
            MOQ_TEST_CHECK_EQ_INT(e->handoff_fin_pending, 0);
        }
    }
    /* Its rejection is drainless -- the peer already closed -- and retires it. */
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_first),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    /* Its output is the family's EXACT rejection -- one REQUEST_ERROR carrying
     * NOT_SUPPORTED on this ref, and nothing else. Draining it unchecked would
     * let a wrong payload, an extra event or a duplicate action become the
     * baseline the reuse leg then measures against. */
    check_reject_only_output(s, &f->hooks, first, "fetch reuse first reject");
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    /* 2. That exact slot is free, its generation advanced, both FIN facts
     *    clear, and no edge anywhere still points at the retired owner. */
    MOQ_TEST_CHECK_EQ_INT((int)s->fetches[slot].state, (int)MOQ_FETCH_FREE);
    MOQ_TEST_CHECK_EQ_U64(s->fetches[slot].generation, gen_reuse - 1u);
    MOQ_TEST_CHECK_EQ_INT(s->fetches[slot].rs.recv_fin, 0);
    MOQ_TEST_CHECK_EQ_INT(s->fetches[slot].handoff_fin_pending, 0);
    check_unregistered(s, first);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "fetch reuse retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, first._v,
                                     "fetch reuse retired");
        failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_FETCH, slot,
                                                "fetch reuse retired");
    }

    /* 3. A distinct no-FIN FETCH reuses that physical slot. */
    moq_stream_ref_t second = moq_stream_ref_from_u64(c->ref._v + 4);
    moq_stream_ref_t saved_ref = c->ref;
    uint64_t saved_id = c->request_id;
    c->ref = second;
    c->request_id = saved_id + 2;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, second, false), (int)MOQ_OK);
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_fetch_request(&want, c->last_fetch);
    failures += txs_norm_equals(&got, &want, "fetch reuse second");
    txs_norm_free(&got);
    txs_norm_free(&want);
    failures += fetch_owner_present_problems(s, second, f->owner_kind,
                                             f->owner_role, f->owner_state);
    {
        const moq_fetch_entry_t *e = fetch_by_ref(s, second);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_INT((int)(e - s->fetches), slot);
            MOQ_TEST_CHECK_EQ_U64(e->generation, gen_reuse);
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, handle_reuse);
            MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin, 0);
            MOQ_TEST_CHECK_EQ_INT(e->handoff_fin_pending, 0);
        }
        MOQ_TEST_CHECK_EQ_U64(c->last_fetch, handle_reuse);
        moq_request_endpoint_t ep =
            request_registry_find_by_streamref(s, second);
        MOQ_TEST_CHECK_EQ_INT((int)ep.kind, (int)MOQ_REQ_FETCH);
        MOQ_TEST_CHECK_EQ_INT(ep.slot, slot);
    }

    /* 4. Its peer half is genuinely open, so the rejection owes a drain
     *    reference; an exhausted ring refuses it transactionally. The VALID
     *    second-owner topology is captured first: a snapshot of kind/slot/
     *    state/generation cannot see an undeclared or repointed edge, so the
     *    graph is what proves nothing was inserted during the refusal. */
    fill_drain_ring(s);
    size_t filled = s->drain_ref_count;
    fetch_ident_t id2;
    memset(&id2, 0, sizeof(id2));
    id2.slot = slot; id2.gen = gen_reuse; id2.handle = handle_reuse;
    id2.request_id = c->request_id; id2.ref = second;
    og_graph_t graph_before;
    og_capture(s, &graph_before);
    failures += og_check_integrity(&graph_before, "fetch reuse live");
    failures += fetch_ident_edges(&graph_before, &id2, "fetch reuse live");

    fetch_hook_state_t owner;
    f->hooks.capture(s, f->ctx, &owner);
    txs_snapshot_t before;
    txs_capture(s, &second, 1, &before);
    expect_after_call_prepare(&before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_reuse),
                          (int)MOQ_ERR_WOULD_BLOCK);
    failures += txs_check_eq(s, &second, 1, &before, "fetch reuse blocked");
    failures += f->hooks.check(s, f->ctx, &owner);
    failures += fetch_ident_problems(s, &id2, f->owner_role, f->owner_state,
                                     "fetch reuse blocked");
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "fetch reuse blocked");
        failures += fetch_ident_edges(&g, &id2, "fetch reuse blocked");
        failures += og_check_same_topology(&g, &graph_before,
                                           "fetch reuse blocked");
    }
    check_no_output(s, &f->hooks);

    /* 5. The retired first-generation handle stays stale after the reuse. Its
     *    own snapshot: a snapshot may not be carried across two advancing API
     *    calls merely because today's call preparation normalizes to the same
     *    values. */
    txs_snapshot_t before_stale;
    txs_capture(s, &second, 1, &before_stale);
    expect_after_call_prepare(&before_stale);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_first),
                          (int)MOQ_ERR_STALE_HANDLE);
    failures += txs_check_eq(s, &second, 1, &before_stale,
                             "fetch stale after reuse");
    failures += f->hooks.check(s, f->ctx, &owner);
    failures += fetch_ident_problems(s, &id2, f->owner_role, f->owner_state,
                                     "fetch stale after reuse");
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "fetch stale after reuse");
        failures += fetch_ident_edges(&g, &id2, "fetch stale after reuse");
        failures += og_check_same_topology(&g, &graph_before,
                                           "fetch stale after reuse");
    }
    check_no_output(s, &f->hooks);

    c->ref = saved_ref;
    c->request_id = saved_id;
    moq_session_destroy(s);
}


void test_fetch(void)
{
    fin_owner_case_t f = fetch_family();
    MOQ_TEST_CHECK_EQ_INT(fin_owner_problems(&f), 0);

    /* Control: no FIN -- the owner has no close recorded, and the rejection
     * reserves one drain reference that the peer's later FIN releases. */
    run_fetch_case(&f, false, false, true);
    /* Same-call FIN: latched on the retained owner; the rejection needs no
     * drain reference. */
    run_fetch_case(&f, true, false, true);
    /* Latch without any terminal at all. */
    run_fetch_case(&f, true, false, false);
    /* The selector against one and the same exhausted ring. */
    run_fetch_case(&f, true, true, true);
    run_fetch_case(&f, false, true, true);
    /* The consumed FIN does not follow the slot to its next owner. */
    run_fetch_slot_reuse_case(&f);
}



/* -- Route: fetch cancel (session_fetch.c:1485, via moq_session_fetch_cancel)
 *
 * The latch IS reachable, and the arm below drives it: a FETCH_OK is latched
 * FIN-first (session_subscribe.c:750), and accepting it sets
 * `control_response_seen`/`control_ok` WITHOUT leaving MOQ_FETCH_PENDING_FETCHER
 * (session_fetch.c:975) -- which is the only state the public cancel guards on
 * (:1534). So a valid FETCH_OK carrying FIN leaves a cancellable owner whose
 * durable latch the terminal consults: `fetch_request_bidi_cancel` reserves a
 * drain only when the peer half is still open, so a same-call FIN owes none. */
#define R5_END_OF_TRACK 1

/* Non-default, and INSIDE the requested range: draft-18 §10.13 permits the
 * response end to equal the requested end or shrink when published data ends
 * first, never to exceed it. */
#define R5_OK_END_GROUP 0x52

#define R5_OK_END_OBJ   0x08

#define R5_START_GROUP 0x31

#define R5_START_OBJ   0x02

#define R5_END_GROUP   0x60

#define R5_END_OBJ     0x0c

#define R5_PRIORITY    23

static const uint8_t k_r5_name[] = { 'f','0' };


static int g_r5_want_slot = -1;


int fetch_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                      const char *what)
{
    int bad = 0;
    if (g_r5_want_slot < 0) {
        OG_DIAG("OG %s: no declared fetch slot\n", what);
        return 1;
    }
    /* A locally-issued fetch registers BOTH keys by design: the response
     * correlates by the request bidi, while the response data uni carries the
     * Request ID (session_fetch.c:1360). */
    bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                         MOQ_REQ_FETCH, g_r5_want_slot, what);
    bad += og_check_edge(g, OG_DOM_REQ_RID, 0,
                         MOQ_REQ_FETCH, g_r5_want_slot, what);
    {
        const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v },
                                        { OG_DOM_REQ_RID, 0 } };
        bad += og_check_owner_edges(g, MOQ_REQ_FETCH, g_r5_want_slot,
                                    want, 2, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}


static void fetch_term_capture(const moq_session_t *s, moq_stream_ref_t ref,
                               seed_snap_t *o)
{
    req_seed_snap_t *r = &o->req;
    memset(r, 0, sizeof(*r));
    r->busy = fetch_busy_count(s);
    const moq_fetch_entry_t *e = fetch_by_ref(s, ref);
    r->found = e != NULL;
    if (e) {
        r->state      = (int)e->state;
        r->role       = (int)e->role;
        r->generation = e->generation;
        r->handle     = e->handle._opaque;
        r->request_id = e->request_id;
        r->stream_ref = e->rs.ref._v;
        r->fin        = e->rs.recv_fin ? 1 : 0;
        r->recv_len   = e->rs.recv_len;
        req_seed_copy_buf(r, e->rs.recv_buf, e->rs.recv_len);
    }
    req_seed_capture_registry(r, s, ref, r->request_id);
}


static void fetch_term_check(const moq_session_t *s, moq_stream_ref_t ref,
                             const seed_snap_t *want, const char *what)
{
    seed_snap_t now;
    fetch_term_capture(s, ref, &now);
    req_seed_diff(&now.req, &want->req, what);
}


static void check_fetch_terminal_retired(const moq_session_t *s,
                                         moq_stream_ref_t ref)
{
    MOQ_TEST_CHECK_EQ_INT(fetch_busy_count(s), 0);
    MOQ_TEST_CHECK(fetch_by_ref(s, ref) == NULL);
    check_unregistered(s, ref);
}


const owner_ops_t fetch_owner_ops = {
    fetch_term_capture, fetch_term_check, check_fetch_terminal_retired
};


/* The whole pending fetch, compared verbatim: a refusal owes zero mutation,
 * including the fields req_seed_snap_t does not model (the requested range,
 * priority and group order, the data-stream identity and its started/FIN
 * flags, and the response buffer's capacity). */
typedef struct r5_full {
    int      valid;
    moq_fetch_entry_t raw;
    size_t   req_len;
    uint8_t  req[256];
} r5_full_t;


static r5_full_t g_r5_full;


static void r5_full_read(const moq_session_t *s, r5_full_t *v)
{
    memset(v, 0, sizeof(*v));
    if (g_r5_want_slot < 0 || (size_t)g_r5_want_slot >= s->fetch_cap) return;
    const moq_fetch_entry_t *e = &s->fetches[g_r5_want_slot];
    v->raw = *e;
    v->req_len = e->rs.recv_len;
    if (e->rs.recv_len > sizeof(v->req) ||
        (e->rs.recv_len && !e->rs.recv_buf)) return;
    if (e->rs.recv_len) memcpy(v->req, e->rs.recv_buf, e->rs.recv_len);
    v->valid = 1;
}


static void r5_full_capture(const moq_session_t *s)
{
    r5_full_read(s, &g_r5_full);
    MOQ_TEST_CHECK_EQ_INT(g_r5_full.valid, 1);
}


static int r5_full_check_blocked(const moq_session_t *s, const char *what)
{
    r5_full_t now;
    r5_full_read(s, &now);
    int bad = 0;
    if (!now.valid || !g_r5_full.valid) {
        TXS_DIAG("TXN %s: incomparable fetch record\n", what);
        return 1;
    }
    if (memcmp(&now.raw, &g_r5_full.raw, sizeof(now.raw)) != 0) {
        TXS_DIAG("TXN %s: pending fetch entry changed\n", what);
        bad++;
    }
    if (now.req_len != g_r5_full.req_len ||
        (now.req_len && memcmp(now.req, g_r5_full.req, now.req_len) != 0)) {
        TXS_DIAG("TXN %s: retained response bytes changed\n", what);
        bad++;
    }
    return bad;
}


static int r5_full_check_committed(const moq_session_t *s, const char *what)
{
    if (g_r5_want_slot < 0 || (size_t)g_r5_want_slot >= s->fetch_cap) return 0;
    if (s->fetches[g_r5_want_slot].state != MOQ_FETCH_FREE) {
        TXS_DIAG("TXN %s: cancelled fetch slot is still live\n", what);
        return 1;
    }
    return 0;
}


const extra_ops_t r5_full_ops = {
    r5_full_capture, r5_full_check_blocked, r5_full_check_committed
};


moq_stream_ref_t arm_fetch_pending(moq_session_t *s, bool fin_observed,
                                   uint64_t *out_handle)
{
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state == MOQ_FETCH_FREE) {
            want_slot = (int)i;
            want_gen = s->fetches[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return moq_stream_ref_from_u64(0);
    g_r5_want_slot = want_slot;

    moq_bytes_t parts[] = { { k_ann_ns0, sizeof(k_ann_ns0) },
                            { k_ann_ns1, sizeof(k_ann_ns1) } };
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ parts, 2 };
    fc.track_name = MOQ_BYTES_LITERAL("f0");
    fc.start_group = R5_START_GROUP;
    fc.start_object = R5_START_OBJ;
    fc.end_group = R5_END_GROUP;
    fc.end_object = R5_END_OBJ;
    fc.group_order = MOQ_GROUP_ORDER_ASCENDING;
    fc.has_subscriber_priority = true;
    fc.subscriber_priority = R5_PRIORITY;
    moq_fetch_t fh = MOQ_FETCH_INVALID;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(s, &fc, 1, &fh), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(fh._opaque,
                          moq_handle_pack(MOQ_HANDLE_POOL_FETCH,
                                          s->session_tag, want_gen,
                                          (uint32_t)want_slot));

    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    {
        int opens = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                opens++;
                ref = a.u.open_bidi_stream.stream_ref;
                MOQ_TEST_CHECK_EQ_INT(a.u.open_bidi_stream.fin ? 1 : 0, 0);
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, a.u.open_bidi_stream.data,
                                    a.u.open_bidi_stream.len);
                moq_control_envelope_t env;
                memset(&env, 0, sizeof(env));
                MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                                      (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type, (uint64_t)MOQ_D18_FETCH);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                moq_bytes_t dp[8];
                moq_d18_fetch_t fb;
                memset(dp, 0, sizeof(dp));
                memset(&fb, 0, sizeof(fb));
                int dec_ok = (moq_d18_decode_fetch(env.payload, env.payload_len,
                                                   dp, 8, &fb) == MOQ_OK);
                MOQ_TEST_CHECK(dec_ok);
                if (dec_ok) {
                    MOQ_TEST_CHECK_EQ_U64(fb.request_id, 0);
                    MOQ_TEST_CHECK_EQ_U64(fb.fetch_type, 1);
                    MOQ_TEST_CHECK_EQ_SIZE(fb.track_namespace.count, (size_t)2);
                    if (fb.track_namespace.count == 2)
                        MOQ_TEST_CHECK(fb.track_namespace.parts != NULL);
                    if (fb.track_namespace.count == 2 &&
                        fb.track_namespace.parts) {
                        const moq_bytes_t *q0 = &fb.track_namespace.parts[0];
                        const moq_bytes_t *q1 = &fb.track_namespace.parts[1];
                        MOQ_TEST_CHECK_EQ_SIZE(q0->len, sizeof(k_ann_ns0));
                        MOQ_TEST_CHECK(q0->len == 0 || q0->data != NULL);
                        if (q0->len == sizeof(k_ann_ns0) && q0->data)
                            MOQ_TEST_CHECK(memcmp(q0->data, k_ann_ns0,
                                                  sizeof(k_ann_ns0)) == 0);
                        MOQ_TEST_CHECK_EQ_SIZE(q1->len, sizeof(k_ann_ns1));
                        MOQ_TEST_CHECK(q1->len == 0 || q1->data != NULL);
                        if (q1->len == sizeof(k_ann_ns1) && q1->data)
                            MOQ_TEST_CHECK(memcmp(q1->data, k_ann_ns1,
                                                  sizeof(k_ann_ns1)) == 0);
                    }
                    MOQ_TEST_CHECK_EQ_SIZE(fb.track_name.len,
                                           sizeof(k_r5_name));
                    MOQ_TEST_CHECK(fb.track_name.len == 0 ||
                                   fb.track_name.data != NULL);
                    if (fb.track_name.len == sizeof(k_r5_name) &&
                        fb.track_name.data)
                        MOQ_TEST_CHECK(memcmp(fb.track_name.data, k_r5_name,
                                              sizeof(k_r5_name)) == 0);
                    MOQ_TEST_CHECK_EQ_U64(fb.start.group, R5_START_GROUP);
                    MOQ_TEST_CHECK_EQ_U64(fb.start.object, R5_START_OBJ);
                    MOQ_TEST_CHECK_EQ_U64(fb.end.group, R5_END_GROUP);
                    MOQ_TEST_CHECK_EQ_U64(fb.end.object, R5_END_OBJ);
                    MOQ_TEST_CHECK_EQ_INT(fb.params.has_group_order ? 1 : 0, 1);
                    MOQ_TEST_CHECK_EQ_INT((int)fb.params.group_order, 1);
                    MOQ_TEST_CHECK_EQ_INT(fb.params.has_subscriber_priority
                                              ? 1 : 0, 1);
                    MOQ_TEST_CHECK_EQ_U64(fb.params.subscriber_priority,
                                          R5_PRIORITY);
                    MOQ_TEST_CHECK_EQ_SIZE(fb.params.auth_token_count,
                                           (size_t)0);
                }
            } else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(opens, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    MOQ_TEST_CHECK(ref._v != 0);
    if (ref._v == 0) return ref;

    /* A COMPLETE, valid, non-default FETCH_OK -- the two rows differ only in
     * whether this same call carries the FIN. */
    {
        uint8_t ok[128];
        moq_buf_writer_t ow;
        moq_buf_writer_init(&ow, ok, sizeof(ok));
        /* start <= response end <= request end, pinned permanently so a later
         * edit cannot drift the fixture off the protocol. */
        MOQ_TEST_CHECK(R5_START_GROUP < R5_OK_END_GROUP ||
                       (R5_START_GROUP == R5_OK_END_GROUP &&
                        R5_START_OBJ <= R5_OK_END_OBJ));
        MOQ_TEST_CHECK(R5_OK_END_GROUP < R5_END_GROUP ||
                       (R5_OK_END_GROUP == R5_END_GROUP &&
                        R5_OK_END_OBJ <= R5_END_OBJ));
        moq_d18_location_t end = { R5_OK_END_GROUP, R5_OK_END_OBJ };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_fetch_ok(&ow, R5_END_OF_TRACK != 0, end,
                                         (moq_bytes_t){0}), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                                                  moq_buf_writer_offset(&ow),
                                                  fin_observed, 1), (int)MOQ_OK);
    }
    {
        int oks = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_FETCH_OK) {
                oks++;
                const moq_fetch_ok_event_t *k = &e.u.fetch_ok;
                MOQ_TEST_CHECK_EQ_U64(k->fetch._opaque, fh._opaque);
                MOQ_TEST_CHECK_EQ_INT(k->end_of_track ? 1 : 0, R5_END_OF_TRACK);
                MOQ_TEST_CHECK_EQ_U64(k->end_group, R5_OK_END_GROUP);
                MOQ_TEST_CHECK_EQ_U64(k->end_object, R5_OK_END_OBJ);
                MOQ_TEST_CHECK_EQ_SIZE(k->track_properties.len, (size_t)0);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(oks, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* The acceptance leaves a still-CANCELLABLE owner: the state the public
     * cancel guards on is unchanged, and the latch now carries the row's FIN. */
    {
        const moq_fetch_entry_t *e = &s->fetches[want_slot];
        MOQ_TEST_CHECK_EQ_INT(fetch_busy_count(s), 1);
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_FETCH_PENDING_FETCHER);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_FETCH_ROLE_FETCHER);
        MOQ_TEST_CHECK_EQ_INT(e->control_response_seen ? 1 : 0, 1);
        MOQ_TEST_CHECK_EQ_INT(e->control_ok ? 1 : 0, 1);
        MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, fh._opaque);
        MOQ_TEST_CHECK_EQ_U64(e->request_id, 0);
        MOQ_TEST_CHECK_EQ_U64(e->rs.ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin ? 1 : 0, fin_observed ? 1 : 0);
        /* No data stream was ever opened, so the cancel owes no STOP_DATA. */
        MOQ_TEST_CHECK_EQ_INT(e->data_stream_started ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_INT(e->data_stream_fin ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_U64(e->data_stream_ref._v, 0);
        MOQ_TEST_CHECK_EQ_SIZE(e->rs.recv_len, (size_t)0);
        check_registered(s, ref, MOQ_REQ_FETCH, want_slot);
    }
    if (out_handle) *out_handle = fh._opaque;
    return ref;
}


moq_result_t fire_fetch_cancel(moq_session_t *s, uint64_t handle)
{
    moq_fetch_t f;
    f._opaque = handle;
    return moq_session_fetch_cancel(s, f, 1);
}


/* No data uni was ever opened, so the cancel owes exactly the request-bidi
 * abort -- no STOP_DATA. */
void want_fetch_cancel(txs_norm_vec_t *v, moq_stream_ref_t ref,
                       uint64_t handle)
{
    (void)handle;
    want_nsl_output(v, ref);
}
