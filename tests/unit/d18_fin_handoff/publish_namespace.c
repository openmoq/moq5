#include "test_cases.h"

typedef struct pn_hook_state {
    int      present;
    uint64_t handle;
    uint64_t req_stream_ref;
    struct {
        int recv_fin;
    } rs;
    /* The transitional FIN ownership a same-call handoff installs. This family
     * consumes it synchronously into req_recv_fin, so every observation point
     * must find it already clear; captured separately so a refusal cannot move
     * the peer's close from one fact to the other unnoticed. */
    int      handoff_fin;
    int      state;
    int      role;
    int      busy;
} pn_hook_state_t;


int ann_busy_count(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state != MOQ_ANN_FREE) n++;
    return n;
}


const moq_ann_entry_t *ann_by_ref(const moq_session_t *s,
                                  moq_stream_ref_t ref)
{
    for (size_t i = 0; i < s->ann_cap; i++) {
        const moq_ann_entry_t *e = &s->announcements[i];
        if (e->state != MOQ_ANN_FREE && e->rs.ref._v == ref._v)
            return e;
    }
    return NULL;
}


moq_result_t pn_feed(moq_session_t *s, void *ctx, moq_stream_ref_t ref,
                     bool fin)
{
    pn_ctx_t *c = (pn_ctx_t *)ctx;
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live"),
                            MOQ_BYTES_LITERAL("eu") };
    moq_namespace_t ns = { parts, 2 };
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    /* A non-default token, so a normalizer that drops the token fields cannot
     * compare equal against an empty expectation. */
    mp.auth_token_count = 1;
    mp.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    mp.auth_tokens[0].token_type = 5;
    mp.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("nstok");
    uint8_t m[192];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, sizeof(m));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_publish_namespace(&w, c->request_id, &ns, &mp),
        (int)MOQ_OK);
    return moq_session_on_bidi_stream_bytes(s, ref, m,
                                            moq_buf_writer_offset(&w), fin, 1);
}


static moq_result_t pn_app_reject(moq_session_t *s, void *ctx, uint64_t handle)
{
    (void)ctx;
    moq_announcement_t ah;
    ah._opaque = handle;
    moq_reject_namespace_cfg_t rj;
    moq_reject_namespace_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    return moq_session_reject_namespace(s, ah, &rj, 1);
}


static void pn_hook_capture(const moq_session_t *s, void *vctx, void *vst)
{
    pn_ctx_t *c = (pn_ctx_t *)vctx;
    pn_hook_state_t *st = (pn_hook_state_t *)vst;
    memset(st, 0, sizeof(*st));
    st->busy = ann_busy_count(s);
    const moq_ann_entry_t *e = ann_by_ref(s, c->ref);
    if (!e) return;
    st->present = 1;
    st->handle = e->handle._opaque;
    st->req_stream_ref = e->rs.ref._v;
    st->rs.recv_fin = e->rs.recv_fin;
    st->handoff_fin = e->handoff_fin_pending;
    st->state = (int)e->state;
    st->role = (int)e->role;
}


static int pn_hook_check(const moq_session_t *s, void *vctx, const void *vst)
{
    pn_hook_state_t now;
    pn_hook_capture(s, vctx, &now);
    const pn_hook_state_t *want = (const pn_hook_state_t *)vst;
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
        TXS_DIAG("TXN pn hooks: present %d handle %llu ref %llu fin %d "
                 "state %d role %d busy %d, expected %d/%llu/%llu/%d/%d/%d/%d\n",
                 now.present, (unsigned long long)now.handle,
                 (unsigned long long)now.req_stream_ref, now.rs.recv_fin,
                 now.state, now.role, now.busy, want->present,
                 (unsigned long long)want->handle,
                 (unsigned long long)want->req_stream_ref, want->rs.recv_fin,
                 want->state, want->role, want->busy);
    return bad;
}


static bool pn_norm_event(const moq_event_t *ev, void *vctx,
                          txs_norm_vec_t *out)
{
    pn_ctx_t *c = (pn_ctx_t *)vctx;
    txs_img_t im;
    txs_img_init(&im);
    switch (ev->kind) {
    case MOQ_EVENT_NAMESPACE_PUBLISHED: {
        const moq_namespace_published_event_t *np = &ev->u.namespace_published;
        c->last_ann = np->ann._opaque;
        txs_img_u64(&im, np->ann._opaque);
        if (!txs_img_parts(&im, "publish_namespace namespace", np->track_namespace.parts,
                           np->track_namespace.count,
                           MOQ_DECODED_MAX_NAMESPACE_PARTS))
            return false;
        if (!txs_img_tokens(&im, "publish_namespace tokens", np->tokens,
                            np->token_count, MOQ_DECODED_MAX_TOKENS))
            return false;
        break;
    }
    default:
        TXS_DIAG("TXN pn: unnormalized event kind %u\n", (unsigned)ev->kind);
        return false;
    }
    return txs_norm_append_img(out, ev->kind, &im);
}


/* Built from the values the fixture put on the wire, not from the event. */
static void want_pn_published(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    txs_img_u64(&im, 2);
    txs_img_bytes(&im, (const uint8_t *)"live", 4);
    txs_img_bytes(&im, (const uint8_t *)"eu", 2);
    txs_img_u64(&im, 1);                    /* token count */
    txs_img_u64(&im, 5);                    /* token type */
    txs_img_bytes(&im, (const uint8_t *)"nstok", 5);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_NAMESPACE_PUBLISHED, &im));
}


pn_ctx_t pn_ctx = { 0, { 1 }, 0 };


static fin_owner_case_t pn_family(void)
{
    fin_owner_case_t f;
    memset(&f, 0, sizeof(f));
    f.name = "publish-namespace";
    f.ctx = &pn_ctx;
    f.caps = FIN_TERM_APP_REJECT;
    f.feed = pn_feed;
    f.owner_kind = MOQ_REQ_ANNOUNCEMENT;
    f.owner_role = (int)MOQ_ANN_ROLE_RECEIVER;
    f.owner_state = (int)MOQ_ANN_PENDING_RECEIVER;
    f.app_reject = pn_app_reject;
    f.hooks.ctx = &pn_ctx;
    f.hooks.capture = pn_hook_capture;
    f.hooks.check = pn_hook_check;
    f.hooks.normalize_event = pn_norm_event;
    f.hooks.normalize_action = pub_norm_action;
    return f;
}


int pn_owner_present_problems(const moq_session_t *s,
                              moq_stream_ref_t ref,
                              moq_request_kind_t want_kind,
                              int want_role, int want_state)
{
    int bad = 0;
    const moq_ann_entry_t *e = ann_by_ref(s, ref);
    if (!e) return 1;
    if ((int)e->role != want_role) bad++;
    if ((int)e->state != want_state) bad++;
    if (ann_busy_count(s) != 1) bad++;
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if ((int)ep.kind != (int)want_kind) bad++;
    else if (ep.slot != (int)(e - s->announcements)) bad++;
    else if (!ep.has_stream_ref || ep.stream_ref._v != ref._v) bad++;
    if (bad)
        TXS_DIAG("TXN pn owner: kind %d role %d state %d, expected "
                 "kind %d role %d state %d\n", (int)ep.kind, (int)e->role,
                 (int)e->state, (int)want_kind, want_role, want_state);
    return bad;
}


static void run_pn_case(const fin_owner_case_t *f, bool fin_in_request,
                        bool fill_ring, bool reject_now)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    pn_ctx_t *c = (pn_ctx_t *)f->ctx;
    moq_stream_ref_t ref = c->ref;
    c->last_ann = 0;

    /* Slot, live generation and the handle they pack into, all from pool state
     * BEFORE ingress -- so the owner is checked against a declaration, never
     * against whatever the commit happened to produce. */
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state == MOQ_ANN_FREE) {
            want_slot = (int)i;
            want_gen = s->announcements[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) { moq_session_destroy(s); return; }
    uint64_t want_handle = moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT,
                                           s->session_tag, want_gen,
                                           (uint32_t)want_slot);

    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, ref, fin_in_request),
                          (int)MOQ_OK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_pn_published(&want, c->last_ann);
    failures += txs_norm_equals(&got, &want, f->name);
    txs_norm_free(&got);
    txs_norm_free(&want);

    /* Latch and RETAIN: the announcement survives and records the close. */
    failures += pn_owner_present_problems(s, ref, f->owner_kind, f->owner_role,
                                          f->owner_state);
    pn_hook_state_t owner;
    f->hooks.capture(s, f->ctx, &owner);
    MOQ_TEST_CHECK(owner.present);
    MOQ_TEST_CHECK_EQ_U64(owner.handle, c->last_ann);
    MOQ_TEST_CHECK_EQ_U64(owner.req_stream_ref, ref._v);
    /* The surfaced NAMESPACE_PUBLISHED handle and the physical owner both
     * match the pre-derived declaration. */
    MOQ_TEST_CHECK_EQ_U64(c->last_ann, want_handle);
    {
        const moq_ann_entry_t *e = ann_by_ref(s, ref);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_INT((int)(e - s->announcements), want_slot);
            MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, want_handle);
        }
        check_registered(s, ref, MOQ_REQ_ANNOUNCEMENT, want_slot);
    }
    MOQ_TEST_CHECK_EQ_INT(owner.rs.recv_fin, fin_in_request ? 1 : 0);
    /* Consumption is synchronous: a handed-over FIN lives in the durable latch
     * by now, never as transitional ownership. */
    MOQ_TEST_CHECK_EQ_INT(owner.handoff_fin, 0);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    check_no_output(s, &f->hooks);

    if (fill_ring) fill_drain_ring(s);   /* no slot left at all */
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    if (!reject_now) { moq_session_destroy(s); return; }

    /* With the peer's close observed the rejection needs no reference, so a
     * full ring cannot refuse it; without it, one is required. */
    bool expect_drain = !fin_in_request;
    moq_result_t want_rc = (expect_drain && fill_ring) ? MOQ_ERR_WOULD_BLOCK
                                                       : MOQ_OK;
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);
    pn_hook_state_t owner_before;
    f->hooks.capture(s, f->ctx, &owner_before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, c->last_ann),
                          (int)want_rc);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        failures += txs_check_eq(s, &ref, 1, &before, f->name);
        failures += f->hooks.check(s, f->ctx, &owner_before);
        /* The refusal must leave the full owner linkage intact, registry
         * kind, slot and stream-ref identity included. */
        failures += pn_owner_present_problems(s, ref, f->owner_kind,
                                              f->owner_role, f->owner_state);
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before, "pn blocked");
        check_no_output(s, &f->hooks);
        moq_session_destroy(s);
        return;
    }

    drain_snap_t now, want_ring;
    drain_snap(s, &now);
    if (expect_drain)
        drain_snap_plus(&ring_before, ref, MOQ_DRAIN_NORMAL, &want_ring);
    else
        want_ring = ring_before;
    failures += drain_multiset_equals(&now, &want_ring, "pn granted");
    MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 0);
    check_unregistered(s, ref);
    check_reject_only_output(s, &f->hooks, ref, f->name);

    if (expect_drain) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t after;
        drain_snap(s, &after);
        failures += drain_multiset_equals(&after, &ring_before, "pn released");
        MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 0);
        check_unregistered(s, ref);
        check_no_output(s, &f->hooks);
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(s);
}


/* Clear-on-reuse for the announcement pool. This family's free is SELECTIVE --
 * it resets named fields rather than the whole record -- so the marker's
 * clearing is an explicit obligation here, not a by-product of a memset. Both
 * generations and both packed handles are derived before ingress. */
static void run_pn_slot_reuse_case(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    pn_ctx_t *c = (pn_ctx_t *)f->ctx;

    int slot = -1;
    uint32_t gen_first = 0;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state == MOQ_ANN_FREE) {
            slot = (int)i;
            gen_first = s->announcements[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { moq_session_destroy(s); return; }
    uint32_t gen_reuse = (gen_first + 1) | 1u;    /* ann_free_entry advances once */
    uint64_t handle_first = moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT,
                                            s->session_tag, gen_first,
                                            (uint32_t)slot);
    uint64_t handle_reuse = moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT,
                                            s->session_tag, gen_reuse,
                                            (uint32_t)slot);
    MOQ_TEST_CHECK(handle_first != handle_reuse);

    /* 1. PUBLISH_NAMESPACE + FIN: the marker is consumed into the latch and the
     *    owner is retained for the application's answer. */
    moq_stream_ref_t first = c->ref;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, first, true), (int)MOQ_OK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_pn_published(&want, c->last_ann);
    failures += txs_norm_equals(&got, &want, "pn reuse first");
    txs_norm_free(&got);
    txs_norm_free(&want);
    MOQ_TEST_CHECK_EQ_U64(c->last_ann, handle_first);
    {
        const moq_ann_entry_t *e = ann_by_ref(s, first);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_INT((int)(e - s->announcements), slot);
            MOQ_TEST_CHECK_EQ_U64(e->generation, gen_first);
            MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin, 1);
            MOQ_TEST_CHECK_EQ_INT(e->handoff_fin_pending, 0);
        }
    }
    /* Its rejection is drainless -- the peer already closed -- and retires it. */
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_first),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    {   /* Drain the rejection's own output so the reuse leg measures only the
         * second request; its exact shape is pinned by the P4 matrix rows. */
        txs_norm_vec_t d;
        txs_norm_init(&d);
        collect_events(s, &f->hooks, &d);
        collect_actions(s, &f->hooks, &d);
        txs_norm_free(&d);
    }

    /* 2. The freed record: generation advanced, BOTH FIN facts clear, and no
     *    edge anywhere still points at the retired owner. */
    MOQ_TEST_CHECK_EQ_INT((int)s->announcements[slot].state, (int)MOQ_ANN_FREE);
    MOQ_TEST_CHECK_EQ_U64(s->announcements[slot].generation, gen_reuse - 1u);
    MOQ_TEST_CHECK_EQ_INT(s->announcements[slot].rs.recv_fin, 0);
    MOQ_TEST_CHECK_EQ_INT(s->announcements[slot].handoff_fin_pending, 0);
    check_unregistered(s, first);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "pn reuse retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, first._v,
                                     "pn reuse retired");
        failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_ANNOUNCEMENT,
                                                slot, "pn reuse retired");
    }

    /* 3. A distinct no-FIN PUBLISH_NAMESPACE reuses that physical slot. */
    moq_stream_ref_t second = moq_stream_ref_from_u64(c->ref._v + 4);
    moq_stream_ref_t saved_ref = c->ref;
    uint64_t saved_id = c->request_id;
    c->ref = second;
    c->request_id = saved_id + 2;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, second, false), (int)MOQ_OK);
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_pn_published(&want, c->last_ann);
    failures += txs_norm_equals(&got, &want, "pn reuse second");
    txs_norm_free(&got);
    txs_norm_free(&want);
    failures += pn_owner_present_problems(s, second, f->owner_kind,
                                          f->owner_role, f->owner_state);
    {
        const moq_ann_entry_t *e = ann_by_ref(s, second);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_INT((int)(e - s->announcements), slot);
            MOQ_TEST_CHECK_EQ_U64(e->generation, gen_reuse);
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, handle_reuse);
            /* The consumed FIN did not follow the slot to its next owner. */
            MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin, 0);
            MOQ_TEST_CHECK_EQ_INT(e->handoff_fin_pending, 0);
        }
        MOQ_TEST_CHECK_EQ_U64(c->last_ann, handle_reuse);
        check_registered(s, second, MOQ_REQ_ANNOUNCEMENT, slot);
    }

    /* 4. Its peer half is genuinely open, so its rejection owes a drain
     *    reference and an exhausted ring refuses it transactionally. */
    fill_drain_ring(s);
    size_t filled = s->drain_ref_count;
    og_graph_t graph_before;
    og_capture(s, &graph_before);
    failures += og_check_integrity(&graph_before, "pn reuse live");
    pn_hook_state_t owner;
    f->hooks.capture(s, f->ctx, &owner);
    txs_snapshot_t before;
    txs_capture(s, &second, 1, &before);
    expect_after_call_prepare(&before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_reuse),
                          (int)MOQ_ERR_WOULD_BLOCK);
    failures += txs_check_eq(s, &second, 1, &before, "pn reuse blocked");
    failures += f->hooks.check(s, f->ctx, &owner);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "pn reuse blocked");
        failures += og_check_same_topology(&g, &graph_before,
                                           "pn reuse blocked");
    }
    check_no_output(s, &f->hooks);

    /* 5. The retired first-generation handle stays stale after the reuse. */
    txs_snapshot_t before_stale;
    txs_capture(s, &second, 1, &before_stale);
    expect_after_call_prepare(&before_stale);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_first),
                          (int)MOQ_ERR_STALE_HANDLE);
    failures += txs_check_eq(s, &second, 1, &before_stale, "pn stale after reuse");
    failures += f->hooks.check(s, f->ctx, &owner);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    check_no_output(s, &f->hooks);

    c->ref = saved_ref;
    c->request_id = saved_id;
    moq_session_destroy(s);
}


void test_publish_namespace(void)
{
    fin_owner_case_t f = pn_family();
    MOQ_TEST_CHECK_EQ_INT(fin_owner_problems(&f), 0);
    run_pn_case(&f, false, false, true);   /* control: drain, then released */
    run_pn_case(&f, true, false, true);    /* same-call FIN: no drain */
    run_pn_case(&f, true, false, false);   /* latch without any terminal */
    run_pn_case(&f, true, true, true);     /* exhausted ring, FIN observed */
    run_pn_case(&f, false, true, true);    /* exhausted ring, no FIN */
    /* The consumed FIN does not follow the slot to its next owner. */
    run_pn_slot_reuse_case(&f);
}


/* -- announcer-side withdrawal (session_namespace.c:792 -> :85) ------- */

/* The slot the arm DECLARED, so the edge oracle never learns its expected
 * target from the graph it is auditing. */
int g_ann_want_slot = -1;


const uint8_t k_ann_ns0[] = { 'e','x','.','c','o','m' };

const uint8_t k_ann_ns1[] = { 'l','i','v','e' };

static const uint8_t k_ann_tok[] = { 'a','n','n','t','o','k' };

#define k_ann_tok_type 11


/* A locally-opened PUBLISH_NAMESPACE the peer accepted, optionally closing its
 * half in the same REQUEST_OK. The entry stays ANNOUNCER/ESTABLISHED either
 * way, so the latch is durable state the withdrawal can consult. */

/* The WHOLE announcement: its object representation plus the bytes it owns.
 * `ann_seed_capture` models the request-family fields common to every owner
 * kind and therefore omits this entry's own -- the canonical namespace key and
 * its length, the receive-buffer capacity, and `goaway_sent` -- so a refusal is
 * additionally compared verbatim here. Bounds-safe: an over-long or
 * unbacked buffer makes the record incomparable rather than being read. */
#define ANN_OWN_MAX 256

typedef struct ann_full {
    int      valid;
    moq_ann_entry_t raw;
    size_t   nsid_len;
    uint8_t  nsid[ANN_OWN_MAX];
    size_t   recv_len;
    uint8_t  recv[ANN_OWN_MAX];
} ann_full_t;


static int g_ann_full_slot = -1;

static ann_full_t g_ann_full;

/* Route 3's terminal is driven by peer BYTES, so its owner legitimately grows
 * its receive buffer by exactly the bytes the refused call delivered; a direct
 * API terminal (route 2) may append nothing. */
static size_t g_ann_full_allow_append;


static void ann_full_read(const moq_session_t *s, ann_full_t *v)
{
    memset(v, 0, sizeof(*v));
    if (g_ann_full_slot < 0 || (size_t)g_ann_full_slot >= s->ann_cap) return;
    const moq_ann_entry_t *e = &s->announcements[g_ann_full_slot];
    v->raw = *e;
    v->nsid_len = e->ns_id_len;
    if (e->ns_id_len > ANN_OWN_MAX || (e->ns_id_len && !e->ns_id_buf)) return;
    if (e->ns_id_len) memcpy(v->nsid, e->ns_id_buf, e->ns_id_len);
    v->recv_len = e->rs.recv_len;
    if (e->rs.recv_len > ANN_OWN_MAX || (e->rs.recv_len && !e->rs.recv_buf))
        return;
    if (e->rs.recv_len) memcpy(v->recv, e->rs.recv_buf, e->rs.recv_len);
    v->valid = 1;
}


static void ann_full_capture(const moq_session_t *s)
{
    ann_full_read(s, &g_ann_full);
    MOQ_TEST_CHECK_EQ_INT(g_ann_full.valid, 1);
}


static int ann_full_check_blocked(const moq_session_t *s, const char *what)
{
    ann_full_t now;
    ann_full_read(s, &now);
    int bad = 0;
    if (!now.valid || !g_ann_full.valid) {
        TXS_DIAG("TXN %s: incomparable announcement record\n", what);
        return 1;
    }
    /* `req_recv_len` and the bytes themselves are pinned EXACTLY by the owner
     * seed snapshot (its captured prefix plus this call's suffix, byte for
     * byte), so this record deliberately masks the length rather than
     * re-deriving the append -- and covers instead what that snapshot omits.
     * A route whose terminal takes no peer bytes declares an append of zero and
     * the mask is then a no-op. */
    moq_ann_entry_t want = g_ann_full.raw;
    want.rs.recv_len = now.raw.rs.recv_len;
    /* Route 3 has validated its peer error and owes reciprocal FIN. */
    if (g_ann_full_allow_append != 0)
        want.rs.retained_output = MOQ_RETAINED_OUTPUT_FIN;
    if (memcmp(&now.raw, &want, sizeof(want)) != 0) {
        TXS_DIAG("TXN %s: announcement entry changed\n", what);
        bad++;
    }
    if (g_ann_full_allow_append == 0 &&
        now.raw.rs.recv_len != g_ann_full.raw.rs.recv_len) {
        TXS_DIAG("TXN %s: retained request bytes grew under a terminal that"
                 " takes none\n", what);
        bad++;
    }
    /* Whatever the length, the prefix the owner already held is intact. */
    if (now.recv_len < g_ann_full.recv_len ||
        (g_ann_full.recv_len &&
         memcmp(now.recv, g_ann_full.recv, g_ann_full.recv_len) != 0)) {
        TXS_DIAG("TXN %s: retained request bytes changed\n", what);
        bad++;
    }
    if (now.nsid_len != g_ann_full.nsid_len ||
        (now.nsid_len &&
         memcmp(now.nsid, g_ann_full.nsid, now.nsid_len) != 0)) {
        TXS_DIAG("TXN %s: canonical namespace key changed\n", what);
        bad++;
    }
    return bad;
}


static int ann_full_check_committed(const moq_session_t *s, const char *what)
{
    if (g_ann_full_slot < 0 || (size_t)g_ann_full_slot >= s->ann_cap) return 0;
    if (s->announcements[g_ann_full_slot].state != MOQ_ANN_FREE) {
        TXS_DIAG("TXN %s: retired announcement slot is still live\n", what);
        return 1;
    }
    if (s->announcements[g_ann_full_slot].rs.retained_output != MOQ_RETAINED_OUTPUT_NONE) {
        TXS_DIAG("TXN %s: retired announcement kept output intent\n", what);
        return 1;
    }
    return 0;
}


static const extra_ops_t ann_full_ops = {
    ann_full_capture, ann_full_check_blocked, ann_full_check_committed
};


moq_stream_ref_t arm_announcer_established(moq_session_t *s,
                                           bool fin_observed,
                                           uint64_t *out_handle)
{
    moq_bytes_t parts[] = { { k_ann_ns0, sizeof(k_ann_ns0) },
                            { k_ann_ns1, sizeof(k_ann_ns1) } };
    /* The public token carries type and value only; the wire encoding is
     * USE_VALUE, which is what the decoded body must show. */
    moq_auth_token_t tok;
    memset(&tok, 0, sizeof(tok));
    tok.token_type = k_ann_tok_type;
    tok.token_value = (moq_bytes_t){ k_ann_tok, sizeof(k_ann_tok) };
    moq_publish_namespace_cfg_t pc;
    moq_publish_namespace_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ parts, 2 };
    pc.auth_tokens = &tok;
    pc.auth_token_count = 1;
    /* The slot the request will take and the generation it will carry, both
     * derived from pool state BEFORE the call (ann_find_free takes the first
     * free slot; the commit sets `generation |= 1`), so nothing downstream can
     * discover its own expectation. */
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state == MOQ_ANN_FREE) {
            want_slot = (int)i;
            want_gen = s->announcements[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return moq_stream_ref_from_u64(0);
    g_ann_want_slot = want_slot;
    g_ann_full_slot = want_slot;
    g_ann_full_allow_append = 0;   /* a direct API terminal appends nothing */

    moq_announcement_t ann = { 0 };
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_publish_namespace(s, &pc, 1, &ann), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(ann._opaque,
                          moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT,
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
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_PUBLISH_NAMESPACE);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                /* The body against the fixture's DECLARED request, not
                 * against anything read out of the message itself. */
                moq_bytes_t dp[8];
                moq_d18_publish_namespace_t pn;
                memset(dp, 0, sizeof(dp));
                memset(&pn, 0, sizeof(pn));
                int dec_ok = (moq_d18_decode_publish_namespace(env.payload,
                                  env.payload_len, dp, 8, &pn) == MOQ_OK);
                MOQ_TEST_CHECK(dec_ok);
                if (dec_ok) {
                MOQ_TEST_CHECK_EQ_U64(pn.request_id, 0);
                MOQ_TEST_CHECK_EQ_SIZE(pn.track_namespace.count, (size_t)2);
                if (pn.track_namespace.count == 2) {
                    /* A declared part count REQUIRES the array, and a
                     * non-empty part REQUIRES its bytes -- asserted, then
                     * still guarded, so a null never reaches memcmp. */
                    MOQ_TEST_CHECK(pn.track_namespace.parts != NULL);
                }
                if (pn.track_namespace.count == 2 &&
                    pn.track_namespace.parts != NULL) {
                    const moq_bytes_t *p0 = &pn.track_namespace.parts[0];
                    const moq_bytes_t *p1 = &pn.track_namespace.parts[1];
                    MOQ_TEST_CHECK_EQ_SIZE(p0->len, sizeof(k_ann_ns0));
                    MOQ_TEST_CHECK(p0->len == 0 || p0->data != NULL);
                    if (p0->len == sizeof(k_ann_ns0) && p0->data)
                        MOQ_TEST_CHECK(memcmp(p0->data, k_ann_ns0,
                                              sizeof(k_ann_ns0)) == 0);
                    MOQ_TEST_CHECK_EQ_SIZE(p1->len, sizeof(k_ann_ns1));
                    MOQ_TEST_CHECK(p1->len == 0 || p1->data != NULL);
                    if (p1->len == sizeof(k_ann_ns1) && p1->data)
                        MOQ_TEST_CHECK(memcmp(p1->data, k_ann_ns1,
                                              sizeof(k_ann_ns1)) == 0);
                }
                MOQ_TEST_CHECK_EQ_SIZE(pn.params.auth_token_count, (size_t)1);
                /* `auth_tokens` is a fixed array inside the decoded params,
                 * not a pointer, so there is nothing to null-check there; only
                 * each token's borrowed VALUE can be absent. */
                if (pn.params.auth_token_count == 1) {
                    MOQ_TEST_CHECK_EQ_INT(
                        (int)pn.params.auth_tokens[0].alias_type,
                        (int)MOQ_AUTH_TOKEN_USE_VALUE);
                    MOQ_TEST_CHECK_EQ_U64(pn.params.auth_tokens[0].token_type,
                                          k_ann_tok_type);
                    const moq_bytes_t *tv =
                        &pn.params.auth_tokens[0].token_value;
                    MOQ_TEST_CHECK_EQ_SIZE(tv->len, sizeof(k_ann_tok));
                    MOQ_TEST_CHECK(tv->len == 0 || tv->data != NULL);
                    if (tv->len == sizeof(k_ann_tok) && tv->data)
                        MOQ_TEST_CHECK(memcmp(tv->data, k_ann_tok,
                                              sizeof(k_ann_tok)) == 0);
                }
                }
            } else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(opens, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    MOQ_TEST_CHECK(ref._v != 0);
    if (ref._v == 0) return ref;

    uint8_t ok[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, ok, sizeof(ok));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_request_ok(&w), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                                              moq_buf_writer_offset(&w),
                                              fin_observed, 1), (int)MOQ_OK);
    {
        int accepted = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_NAMESPACE_ACCEPTED) {
                accepted++;
                MOQ_TEST_CHECK_EQ_U64(e.u.namespace_accepted.ann._opaque,
                                      ann._opaque);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(accepted, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* The ABSOLUTE owner shape the terminal will act on. */
    MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 1);
    {
        /* The owner physically occupies the DECLARED slot. */
        const moq_ann_entry_t *e = &s->announcements[want_slot];
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_ANN_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_ANN_ROLE_ANNOUNCER);
        MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, ann._opaque);
        MOQ_TEST_CHECK_EQ_U64(e->request_id, 0);
        MOQ_TEST_CHECK_EQ_U64(e->rs.ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin ? 1 : 0, fin_observed ? 1 : 0);
        /* And the registry edge targets that declared slot. */
        check_registered(s, ref, MOQ_REQ_ANNOUNCEMENT, want_slot);
    }
    if (out_handle) *out_handle = ann._opaque;
    return ref;
}


/* An announcement owns exactly its request-stream-ref edge: the family keys
 * its bidi there and nowhere else. */
int ann_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                    const char *what)
{
    int bad = 0;
    if (g_ann_want_slot < 0) {
        OG_DIAG("OG %s: no declared announcement slot\n", what);
        return 1;
    }
    bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                         MOQ_REQ_ANNOUNCEMENT, g_ann_want_slot, what);
    {
        const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_ANNOUNCEMENT, g_ann_want_slot,
                                    want, 1, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}


moq_result_t fire_publish_namespace_done(moq_session_t *s,
                                         uint64_t handle)
{
    moq_announcement_t ann;
    ann._opaque = handle;
    return moq_session_publish_namespace_done(s, ann, 1);
}


void want_announcer_teardown(txs_norm_vec_t *v, moq_stream_ref_t ref,
                             uint64_t handle)
{
    (void)handle;
    want_nsl_output(v, ref);   /* one ABORT_BIDI_STREAM(ref, CANCELLED) */
}


txs_op_hooks_t durable_abort_hooks = {
    NULL, NULL, NULL, NULL, no_event_expected, nsl_norm_action
};



/* -- Route: announcement error (session_namespace.c:582)
 *
 * The announcer's own PUBLISH_NAMESPACE is answered with a terminal
 * REQUEST_ERROR. The retirement reserves its drain FIN-aware
 * (`req_stream && !ann_peer_fin_observed`, :594), so a same-call FIN owes none
 * -- the ordinary Axis 3 shape on the announcer side. */
#define R3_ERROR_CODE  MOQ_REQUEST_ERROR_NOT_SUPPORTED

#define R3_RETRY_WIRE  4500   /* Retry Interval as sent: the time plus one */

static const char k_r3_reason[] = "namespace refused";


static uint64_t g_r3_handle;

static int g_r3_want_slot = -1;


static size_t enc_announcement_error(uint8_t *buf, size_t cap);


int r3_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                   const char *what)
{
    int bad = 0;
    if (g_r3_want_slot < 0) {
        OG_DIAG("OG %s: no declared announcement slot\n", what);
        return 1;
    }
    bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                         MOQ_REQ_ANNOUNCEMENT, g_r3_want_slot, what);
    {
        const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_ANNOUNCEMENT, g_r3_want_slot,
                                    want, 1, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}


/* A locally-issued PUBLISH_NAMESPACE left PENDING: the terminal REQUEST_ERROR
 * is legal only before the announcement is answered (the d18 dispatcher closes
 * on a non-PENDING_ANNOUNCER target), so this arm deliberately stops short of
 * the acceptance route 2 feeds. */
static moq_stream_ref_t arm_announcement_pending(moq_session_t *s)
{
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state == MOQ_ANN_FREE) {
            want_slot = (int)i;
            want_gen = s->announcements[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return moq_stream_ref_from_u64(0);
    g_r3_want_slot = want_slot;
    g_ann_full_slot = want_slot;
    /* Peer BYTES drive this terminal, so the refused call legitimately appends
     * onto this same owner; the owner seed snapshot pins that append exactly. */
    g_ann_full_allow_append = 1;

    moq_bytes_t parts[] = { { k_ann_ns0, sizeof(k_ann_ns0) },
                            { k_ann_ns1, sizeof(k_ann_ns1) } };
    moq_publish_namespace_cfg_t pc;
    moq_publish_namespace_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ parts, 2 };
    moq_announcement_t ann = MOQ_ANNOUNCEMENT_INVALID;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_publish_namespace(s, &pc, 1, &ann), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(ann._opaque,
                          moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT,
                                          s->session_tag, want_gen,
                                          (uint32_t)want_slot));
    g_r3_handle = ann._opaque;

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
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_PUBLISH_NAMESPACE);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                /* The body against the fixture's DECLARED request. */
                moq_bytes_t dp[8];
                moq_d18_publish_namespace_t pn;
                memset(dp, 0, sizeof(dp));
                memset(&pn, 0, sizeof(pn));
                int dec_ok = (moq_d18_decode_publish_namespace(
                                  env.payload, env.payload_len, dp, 8, &pn)
                              == MOQ_OK);
                MOQ_TEST_CHECK(dec_ok);
                if (dec_ok) {
                    MOQ_TEST_CHECK_EQ_U64(pn.request_id, 0);
                    MOQ_TEST_CHECK_EQ_SIZE(pn.track_namespace.count, (size_t)2);
                    if (pn.track_namespace.count == 2)
                        MOQ_TEST_CHECK(pn.track_namespace.parts != NULL);
                    if (pn.track_namespace.count == 2 &&
                        pn.track_namespace.parts) {
                        const moq_bytes_t *q0 = &pn.track_namespace.parts[0];
                        const moq_bytes_t *q1 = &pn.track_namespace.parts[1];
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
                    /* The default parameter set this arm configured. */
                    MOQ_TEST_CHECK_EQ_SIZE(pn.params.auth_token_count,
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

    {
        const moq_ann_entry_t *e = &s->announcements[want_slot];
        MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 1);
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_ANN_PENDING_ANNOUNCER);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_ANN_ROLE_ANNOUNCER);
        MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, ann._opaque);
        MOQ_TEST_CHECK_EQ_U64(e->request_id, 0);
        MOQ_TEST_CHECK_EQ_U64(e->rs.ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin ? 1 : 0, 0);
        check_registered(s, ref, MOQ_REQ_ANNOUNCEMENT, want_slot);
    }
    return ref;
}


/* With buf == NULL this answers only the encoded length, so the fixture can
 * DECLARE the append the refused call will make instead of measuring it. */
static size_t enc_announcement_error(uint8_t *buf, size_t cap)
{
    uint8_t scratch[128];
    if (!buf) { buf = scratch; cap = sizeof(scratch); }
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_request_error(
            &w, R3_ERROR_CODE, R3_RETRY_WIRE,
            (moq_bytes_t){ (const uint8_t *)k_r3_reason,
                           sizeof(k_r3_reason) - 1 }), (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}


/* The declared terminal: one NAMESPACE_REJECTED event carrying the announcement
 * the arm surfaced, then our send half closed. */
static void want_announcement_error(txs_norm_vec_t *v, moq_stream_ref_t ref)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, g_r3_handle);
    txs_img_u64(&im, (uint64_t)R3_ERROR_CODE);
    txs_img_u64(&im, 1);            /* can_retry, from the nonzero interval */
    txs_img_u64(&im, R3_RETRY_WIRE - 1);
    txs_img_bytes(&im, (const uint8_t *)k_r3_reason, sizeof(k_r3_reason) - 1);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_NAMESPACE_REJECTED, &im));

    txs_img_init(&im);
    txs_img_u64(&im, ref._v);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_CLOSE_BIDI_STREAM, &im));
}


static bool r3_norm_event(const moq_event_t *ev, void *ctx,
                          txs_norm_vec_t *out)
{
    (void)ctx;
    if (ev->kind != MOQ_EVENT_NAMESPACE_REJECTED) {
        TXS_DIAG("TXN announcement error: unnormalized event kind %u\n",
                 (unsigned)ev->kind);
        return false;
    }
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, ev->u.namespace_rejected.ann._opaque);
    txs_img_u64(&im, (uint64_t)ev->u.namespace_rejected.error_code);
    txs_img_u64(&im, ev->u.namespace_rejected.can_retry ? 1u : 0u);
    txs_img_u64(&im, ev->u.namespace_rejected.retry_after_ms);
    txs_img_bytes(&im, ev->u.namespace_rejected.reason.data,
                  ev->u.namespace_rejected.reason.len);
    return txs_norm_append_img(out, ev->kind, &im);
}


static bool r3_norm_action(const moq_action_t *a, void *ctx,
                           txs_norm_vec_t *out)
{
    (void)ctx;
    if (a->kind != MOQ_ACTION_CLOSE_BIDI_STREAM) {
        TXS_DIAG("TXN announcement error: unnormalized action kind %u\n",
                 (unsigned)a->kind);
        return false;
    }
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, a->u.close_bidi_stream.stream_ref._v);
    return txs_norm_append_img(out, a->kind, &im);
}


/* Designated: a positional form would shift the slot pointer into the family
 * if a member were dropped, turning a real deletion into a build error rather
 * than the MOQ_REQ_NONE the validator exists to catch. */
static const terminal_graph_ops_t r3_graph_ops = {
    .owner_edges = r3_owner_edges,
    .owner_kind  = MOQ_REQ_ANNOUNCEMENT,
    .owner_slot  = &g_r3_want_slot
};


static txs_op_hooks_t r3_hooks = {
    NULL, NULL, NULL, NULL, r3_norm_event, r3_norm_action
};




/* -- T5: ann_local_teardown via its RECEIVER-side caller
 *      (session_namespace.c:85, reached only through
 *       moq_session_cancel_namespace at :955-983)
 *
 * The P4 marker consumer, and a DIFFERENT route from the announcer-side caller
 * already signed off: same helper, opposite role, reached by revoking a
 * namespace WE accepted rather than withdrawing one we announced. Under the
 * unified FIN-aware rule the helper reserves a NORMAL drain only when the peer
 * half is genuinely open; a same-call FIN owes none.
 *
 * Two arms, and they differ in WHERE the peer FIN comes from -- which the
 * fixture asserts rather than assumes, because the two are not the same fact.
 * Both pass: whichever way the FIN arrives, it ends up in the DURABLE
 * latch, and the FIN rows are drainless under the unified rule.
 *
 *   T5-MARKER      the FIN rides the inbound PUBLISH_NAMESPACE that CREATES the
 *                  entry. The commit hands it to the announcement owner as
 *                  transitional ownership (the P4 handoff marker), and the
 *                  family's own handler consumes it into the durable latch -- so
 *                  a FIN-fed arm holds `req_recv_fin` TRUE and no marker.
 *   T5-DISCRIM     the FIN arrives as a legal empty FIN AFTER acceptance, which
 *                  `handle_announcement_stream_bytes` latches on the entry
 *                  (:121) -- a durable latch reached by a different route.
 *
 * Neither arm forges the latch from a same-call FIN by hand: the handoff
 * consumes the transitional marker into the latch, and both arms assert
 * `handoff_fin_pending` is cleared to prove that consumption is synchronous. */
#define T5_TOK_TYPE 11

static const uint8_t k_t5_tok[] = { 'r','c','v','t','o','k' };

static const uint8_t k_t5_ns0[] = { 'e','x','.','c','o','m' };

static const uint8_t k_t5_ns1[] = { 'r','e','c','v' };


static int g_t5_want_slot = -1;


static int t5_owner_edges(const og_graph_t *g, moq_stream_ref_t ref,
                          const char *what)
{
    int bad = 0;
    if (g_t5_want_slot < 0) {
        OG_DIAG("OG %s: no declared announcement slot\n", what);
        return 1;
    }
    bad += og_check_edge(g, OG_DOM_REQ_STREAMREF, ref._v,
                         MOQ_REQ_ANNOUNCEMENT, g_t5_want_slot, what);
    {
        const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
        bad += og_check_owner_edges(g, MOQ_REQ_ANNOUNCEMENT, g_t5_want_slot,
                                    want, 1, what);
    }
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}


/* The inbound PUBLISH_NAMESPACE, with a non-default two-part namespace and one
 * USE_VALUE token, so the surfaced event is discriminating. */
static size_t enc_t5_publish_namespace(uint8_t *buf, size_t cap, uint64_t rid)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { { k_t5_ns0, sizeof(k_t5_ns0) },
                            { k_t5_ns1, sizeof(k_t5_ns1) } };
    moq_namespace_t ns = { parts, 2 };
    moq_d18_msg_params_t p = { 0 };
    p.auth_token_count = 1;
    p.auth_tokens[0].alias_type = 3;            /* USE_VALUE */
    p.auth_tokens[0].token_type = T5_TOK_TYPE;
    p.auth_tokens[0].token_value =
        (moq_bytes_t){ k_t5_tok, sizeof(k_t5_tok) };
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_publish_namespace(&w, rid, &ns, &p), (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}


/* Admit + accept a peer PUBLISH_NAMESPACE, leaving an ESTABLISHED
 * RECEIVER-role announcement. `fin_on_request` selects whether the creating
 * call carries the FIN; `want_latched` is what the entry must then hold. */
/* The peer stream ref and the clock are DECLARED by the caller: two peer
 * requests in one session are distinct transport streams, and a fixture that
 * reused one identity (or let time regress between rows) would be asserting
 * against a topology the transport cannot produce. */
static moq_stream_ref_t t5_admit_and_accept_at(moq_session_t *s,
                                               uint64_t rid,
                                               uint64_t peer_ref_v,
                                               uint64_t now_us,
                                               bool fin_on_request,
                                               bool want_latched,
                                               uint64_t *out_handle)
{
    /* Slot, generation and the handle they pack into, all derived from pool
     * state BEFORE the request is fed. */
    int want_slot = -1;
    uint32_t want_gen = 0;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state == MOQ_ANN_FREE) {
            want_slot = (int)i;
            want_gen = s->announcements[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(want_slot >= 0);
    if (want_slot < 0) return moq_stream_ref_from_u64(0);
    g_t5_want_slot = want_slot;
    g_ann_full_slot = want_slot;
    g_ann_full_allow_append = 0;   /* a direct API terminal appends nothing */
    uint64_t want_handle = moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT,
                                           s->session_tag, want_gen,
                                           (uint32_t)want_slot);
    /* The co-allocated receive buffer persists across entry reuse, so its
     * pointer and capacity are properties of the FREE slot: captured BEFORE
     * ingress rather than adopted from whatever admission leaves behind. */
    const uint8_t *want_recv_buf = s->announcements[want_slot].rs.recv_buf;
    size_t want_recv_cap = s->announcements[want_slot].rs.recv_cap;

    moq_stream_ref_t ref = moq_stream_ref_from_u64(peer_ref_v); /* peer-opened */
    uint8_t m[256];
    size_t n = enc_t5_publish_namespace(m, sizeof(m), rid);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, m, n, fin_on_request,
                                              now_us),
        (int)MOQ_OK);

    moq_announcement_t ann = MOQ_ANNOUNCEMENT_INVALID;
    {
        int pub = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                pub++;
                const moq_namespace_published_event_t *ev =
                    &e.u.namespace_published;
                ann = ev->ann;
                /* Against the handle derived before ingress, and against the
                 * fixture's own declared namespace and token. */
                MOQ_TEST_CHECK_EQ_U64(ev->ann._opaque, want_handle);
                MOQ_TEST_CHECK_EQ_SIZE(ev->track_namespace.count, (size_t)2);
                MOQ_TEST_CHECK(ev->track_namespace.parts != NULL);
                if (ev->track_namespace.count == 2 &&
                    ev->track_namespace.parts) {
                    const moq_bytes_t *q0 = &ev->track_namespace.parts[0];
                    const moq_bytes_t *q1 = &ev->track_namespace.parts[1];
                    /* A non-empty field with NULL bytes is INVALID, not a
                     * comparison to skip. */
                    MOQ_TEST_CHECK_EQ_SIZE(q0->len, sizeof(k_t5_ns0));
                    MOQ_TEST_CHECK(q0->data != NULL);
                    if (q0->len == sizeof(k_t5_ns0) && q0->data)
                        MOQ_TEST_CHECK(memcmp(q0->data, k_t5_ns0,
                                              sizeof(k_t5_ns0)) == 0);
                    MOQ_TEST_CHECK_EQ_SIZE(q1->len, sizeof(k_t5_ns1));
                    MOQ_TEST_CHECK(q1->data != NULL);
                    if (q1->len == sizeof(k_t5_ns1) && q1->data)
                        MOQ_TEST_CHECK(memcmp(q1->data, k_t5_ns1,
                                              sizeof(k_t5_ns1)) == 0);
                } else MOQ_TEST_CHECK(0);
                MOQ_TEST_CHECK_EQ_SIZE(ev->token_count, (size_t)1);
                MOQ_TEST_CHECK(ev->tokens != NULL);
                if (ev->token_count == 1 && ev->tokens) {
                    const moq_bytes_t *tv = &ev->tokens[0].token_value;
                    MOQ_TEST_CHECK_EQ_U64(ev->tokens[0].token_type,
                                          T5_TOK_TYPE);
                    MOQ_TEST_CHECK_EQ_SIZE(tv->len, sizeof(k_t5_tok));
                    MOQ_TEST_CHECK(tv->data != NULL);
                    if (tv->len == sizeof(k_t5_tok) && tv->data)
                        MOQ_TEST_CHECK(memcmp(tv->data, k_t5_tok,
                                              sizeof(k_t5_tok)) == 0);
                } else MOQ_TEST_CHECK(0);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(pub, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    moq_accept_namespace_cfg_t ac;
    moq_accept_namespace_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_namespace(s, ann, &ac, now_us),
                          (int)MOQ_OK);
    /* The acceptance owes exactly one REQUEST_OK on the same ref, with our send
     * half left open and nothing after the envelope. */
    {
        int oks = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a.u.send_bidi_stream.stream_ref._v == ref._v) {
                oks++;
                MOQ_TEST_CHECK_EQ_INT(a.u.send_bidi_stream.fin ? 1 : 0, 0);
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, a.u.send_bidi_stream.data,
                                    a.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                memset(&env, 0, sizeof(env));
                MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                                      (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_REQUEST_OK);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                /* And the BODY. The envelope check sees only bytes AFTER the
                 * envelope; a payload carrying parameters or junk INSIDE it
                 * passes that and is caught only here. */
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_request_ok(env.payload,
                                                   env.payload_len),
                    (int)MOQ_OK);
            } else other++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(oks, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    {
        const moq_ann_entry_t *e = &s->announcements[want_slot];
        MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 1);
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_ANN_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_ANN_ROLE_RECEIVER);
        MOQ_TEST_CHECK_EQ_U64(e->generation, want_gen);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, want_handle);
        MOQ_TEST_CHECK_EQ_U64(e->request_id, rid);
        MOQ_TEST_CHECK_EQ_U64(e->rs.ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(e->goaway_sent ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_SIZE(e->rs.recv_len, (size_t)0);
        /* Both derived pre-ingress, so admission cannot define them. */
        MOQ_TEST_CHECK(e->rs.recv_buf == want_recv_buf);
        MOQ_TEST_CHECK_EQ_SIZE(e->rs.recv_cap, want_recv_cap);
        /* The canonical namespace key, built INDEPENDENTLY from the fixture's
         * own parts rather than read back from the entry. */
        {
            uint8_t want_nsid[64];
            size_t n_ns = 0;
            want_nsid[n_ns++] = 2;                      /* part count */
            want_nsid[n_ns++] = (uint8_t)(sizeof(k_t5_ns0) >> 8);
            want_nsid[n_ns++] = (uint8_t)(sizeof(k_t5_ns0) & 0xff);
            memcpy(want_nsid + n_ns, k_t5_ns0, sizeof(k_t5_ns0));
            n_ns += sizeof(k_t5_ns0);
            want_nsid[n_ns++] = (uint8_t)(sizeof(k_t5_ns1) >> 8);
            want_nsid[n_ns++] = (uint8_t)(sizeof(k_t5_ns1) & 0xff);
            memcpy(want_nsid + n_ns, k_t5_ns1, sizeof(k_t5_ns1));
            n_ns += sizeof(k_t5_ns1);
            MOQ_TEST_CHECK_EQ_SIZE(e->ns_id_len, n_ns);
            MOQ_TEST_CHECK(e->ns_id_buf != NULL);
            if (e->ns_id_len == n_ns && e->ns_id_buf)
                MOQ_TEST_CHECK(memcmp(e->ns_id_buf, want_nsid, n_ns) == 0);
        }
        /* The FIN FACT this arm claims, asserted rather than assumed -- and
         * that it lives in the DURABLE latch, never left as transitional
         * ownership: this family's consumption is synchronous. */
        MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin ? 1 : 0, want_latched ? 1 : 0);
        MOQ_TEST_CHECK_EQ_INT(e->handoff_fin_pending ? 1 : 0, 0);
        check_registered(s, ref, MOQ_REQ_ANNOUNCEMENT, want_slot);
    }
    if (out_handle) *out_handle = want_handle;
    return ref;
}


/* T5-MARKER: the FIN rides the creating request. The commit hands it to the
 * announcement owner as transitional ownership and the family's own handler
 * consumes it into the durable latch, so an arm fed with the FIN holds the
 * latch and no marker -- which is what makes the FIN rows drainless under the
 * unified rule. */
static moq_stream_ref_t t5_admit_and_accept(moq_session_t *s,
                                            bool fin_on_request,
                                            bool want_latched,
                                            uint64_t *out_handle)
{
    return t5_admit_and_accept_at(s, 0, /*peer_ref_v=*/1, /*now_us=*/1,
                                  fin_on_request, want_latched, out_handle);
}


static moq_stream_ref_t arm_t5_marker(moq_session_t *s, bool fin_observed,
                                      uint64_t *out_handle)
{
    /* The FIN rides the creating PUBLISH_NAMESPACE, so the accepted receiver
     * owner carries the latch exactly when this arm was fed one. */
    return t5_admit_and_accept(s, fin_observed, fin_observed, out_handle);
}


/* T5-DISCRIM: the FIN arrives as a legal empty FIN AFTER acceptance, which the
 * announce request-stream handler latches on the entry. No later peer bytes
 * follow, and the RECEIVER owner must stay established. */
static moq_stream_ref_t arm_t5_post_accept_fin(moq_session_t *s,
                                               bool fin_observed,
                                               uint64_t *out_handle)
{
    moq_stream_ref_t ref = t5_admit_and_accept(s, false, false, out_handle);
    if (ref._v == 0 || !fin_observed) return ref;

    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
        (int)MOQ_OK);
    check_no_output(s, &durable_abort_hooks);
    {
        const moq_ann_entry_t *e = &s->announcements[g_t5_want_slot];
        /* Latched, and the owner survives it whole. */
        MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin ? 1 : 0, 1);
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_ANN_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_ANN_ROLE_RECEIVER);
        MOQ_TEST_CHECK_EQ_SIZE(e->rs.recv_len, (size_t)0);
        check_registered(s, ref, MOQ_REQ_ANNOUNCEMENT, g_t5_want_slot);
    }
    return ref;
}


static moq_result_t fire_cancel_namespace(moq_session_t *s, uint64_t handle)
{
    moq_announcement_t ann;
    ann._opaque = handle;
    moq_cancel_namespace_cfg_t c;
    moq_cancel_namespace_cfg_init(&c);
    /* A RESET/STOP carries only a numeric code, so the reason must stay empty;
     * a non-empty one is rejected outright (session_namespace.c:975). */
    return moq_session_cancel_namespace(s, ann, &c, 1);
}


static void want_t5_cancel(txs_norm_vec_t *v, moq_stream_ref_t ref,
                           uint64_t handle)
{
    (void)handle;
    want_nsl_output(v, ref);   /* one ABORT_BIDI_STREAM(ref, CANCELLED) */
}



/* T5 is NOT an Axis 3 durable-latch row: one of its two arms carries a
 * marker-sourced FIN rather than a latch, so it runs in its own function
 * instead of inside the durable-latch tranche. */
void test_t5_receiver_teardown(void)
    {
        static const durable_case_t t5_marker = {
            .name = "cancel namespace (P4 marker)",
            .make = make_session_default,
            .arm = arm_t5_marker, .fire = fire_cancel_namespace,
            .want_output = want_t5_cancel,
            .hooks = &durable_abort_hooks, .owner = &ann_owner_ops,
            .owner_edges = t5_owner_edges, .extra = &ann_full_ops
        };
        run_durable_matrix(&t5_marker);

        static const durable_case_t t5_discrim = {
            .name = "cancel namespace (post-accept FIN)",
            .make = make_session_default,
            .arm = arm_t5_post_accept_fin, .fire = fire_cancel_namespace,
            .want_output = want_t5_cancel,
            .hooks = &durable_abort_hooks, .owner = &ann_owner_ops,
            .owner_edges = t5_owner_edges, .extra = &ann_full_ops
        };
        run_durable_matrix(&t5_discrim);
    }




/* A WHOLE live-announcement snapshot. `ann_seed_capture` deliberately models
 * only the fields common to every request family, so it sees neither the
 * canonical namespace key, nor `req_recv_cap`, nor `goaway_sent` -- an in-place
 * key edit or a lifecycle-flag flip would survive it. This one owns the entry's
 * bytes as well as its record, and is parameterized by ref (no single-slot
 * global). An oversized or unbacked record is INCOMPARABLE rather than read. */
#define ANN_WHOLE_MAX 256

typedef struct ann_whole_snap {
    int             valid;
    moq_ann_entry_t raw;
    size_t          ns_len;
    uint8_t         ns[ANN_WHOLE_MAX];
    size_t          rx_len;
    uint8_t         rx[ANN_WHOLE_MAX];
} ann_whole_snap_t;


static void ann_whole_capture(const moq_session_t *s, moq_stream_ref_t ref,
                              ann_whole_snap_t *o)
{
    memset(o, 0, sizeof(*o));
    const moq_ann_entry_t *e = ann_by_ref(s, ref);
    if (!e) return;
    if (e->ns_id_len > ANN_WHOLE_MAX || (e->ns_id_len && !e->ns_id_buf)) return;
    if (e->rs.recv_len > ANN_WHOLE_MAX || (e->rs.recv_len && !e->rs.recv_buf))
        return;
    o->raw = *e;
    o->ns_len = e->ns_id_len;
    if (o->ns_len) memcpy(o->ns, e->ns_id_buf, o->ns_len);
    o->rx_len = e->rs.recv_len;
    if (o->rx_len) memcpy(o->rx, e->rs.recv_buf, o->rx_len);
    o->valid = 1;
}


static int ann_whole_check(const moq_session_t *s, moq_stream_ref_t ref,
                           const ann_whole_snap_t *want, const char *what)
{
    int bad = 0;
    ann_whole_snap_t now;
    ann_whole_capture(s, ref, &now);
    if (!want->valid || !now.valid) {
        fprintf(stderr, "AXIS4 %s: whole-owner record incomparable\n", what);
        return 1;
    }
#define ANN_W_EQ(field, a, b) do { \
    if ((uint64_t)(a) != (uint64_t)(b)) { \
        fprintf(stderr, "AXIS4 %s: whole owner %s = %llu, expected %llu\n", \
                what, field, (unsigned long long)(a), (unsigned long long)(b)); \
        bad++; \
    } \
} while (0)
    ANN_W_EQ("state", (int)now.raw.state, (int)want->raw.state);
    ANN_W_EQ("role", (int)now.raw.role, (int)want->raw.role);
    ANN_W_EQ("generation", now.raw.generation, want->raw.generation);
    ANN_W_EQ("handle", now.raw.handle._opaque, want->raw.handle._opaque);
    ANN_W_EQ("request_id", now.raw.request_id, want->raw.request_id);
    ANN_W_EQ("request_stream_ref", now.raw.rs.ref._v,
             want->raw.rs.ref._v);
    ANN_W_EQ("ns_id_len", now.raw.ns_id_len, want->raw.ns_id_len);
    ANN_W_EQ("req_recv_cap", now.raw.rs.recv_cap, want->raw.rs.recv_cap);
    ANN_W_EQ("req_recv_len", now.raw.rs.recv_len, want->raw.rs.recv_len);
    ANN_W_EQ("req_recv_fin", now.raw.rs.recv_fin ? 1 : 0,
             want->raw.rs.recv_fin ? 1 : 0);
    ANN_W_EQ("goaway_sent", now.raw.goaway_sent ? 1 : 0,
             want->raw.goaway_sent ? 1 : 0);
#undef ANN_W_EQ
    if (now.raw.rs.recv_buf != want->raw.rs.recv_buf) {
        fprintf(stderr, "AXIS4 %s: whole owner req_recv_buf pointer\n", what);
        bad++;
    }
    /* Pointer identity as well as bytes: swapping the owned allocation for a
     * byte-identical one changes ownership and would otherwise pass. */
    if (now.raw.ns_id_buf != want->raw.ns_id_buf) {
        fprintf(stderr, "AXIS4 %s: whole owner ns_id_buf pointer\n", what);
        bad++;
    }
    /* The OWNED bytes, not just the pointer: an in-place edit moves nothing. */
    if (now.ns_len != want->ns_len ||
        (now.ns_len && memcmp(now.ns, want->ns, now.ns_len) != 0)) {
        fprintf(stderr, "AXIS4 %s: whole owner namespace key bytes\n", what);
        bad++;
    }
    if (now.rx_len != want->rx_len ||
        (now.rx_len && memcmp(now.rx, want->rx, now.rx_len) != 0)) {
        fprintf(stderr, "AXIS4 %s: whole owner retained request bytes\n", what);
        bad++;
    }
    return bad;
}


/* The topology a successful retirement must leave: the pre-call graph minus
 * EXACTLY the target's declared stream-ref edge. Absent or duplicated is a
 * failure, not a silent no-op, and an overflowed derivation is refused. */
static int ann_derive_retired_topology(const og_graph_t *base, uint64_t ref_v,
                                       int slot, og_graph_t *out,
                                       const char *what)
{
    *out = *base;
    out->edge_count = 0;
    size_t removed = 0;
    for (size_t i = 0; i < base->edge_count; i++) {
        const og_edge_t *e = &base->edges[i];
        if (e->domain == OG_DOM_REQ_STREAMREF && e->key == ref_v &&
            e->kind == (int)MOQ_REQ_ANNOUNCEMENT && e->slot == slot) {
            removed++;
            continue;
        }
        if (out->edge_count >= OG_MAX_EDGES) {
            fprintf(stderr, "AXIS4 %s: derived topology overflowed\n", what);
            out->overflow = 1;
            return 1;
        }
        out->edges[out->edge_count++] = *e;
    }
    if (removed != 1) {
        fprintf(stderr,
                "AXIS4 %s: target stream-ref edge appears %zu times, expected 1\n",
                what, removed);
        return 1;
    }
    return 0;
}


/* Axis 4 needs a declared clock; the signed durable-case callback keeps its
 * own fixed time, so this is a separate narrow entry rather than a change to
 * it. */
static moq_result_t fire_cancel_namespace_at(moq_session_t *s, uint64_t handle,
                                             uint64_t now_us)
{
    moq_announcement_t ann;
    ann._opaque = handle;
    moq_cancel_namespace_cfg_t c;
    moq_cancel_namespace_cfg_init(&c);
    /* A RESET/STOP carries only a numeric code, so the reason must stay empty;
     * a non-empty one is rejected outright (session_namespace.c:975). */
    return moq_session_cancel_namespace(s, ann, &c, now_us);
}


/* The declared retired record keeps only the co-allocated receive storage
 * and incremented generation. It is derived before retirement, never copied
 * from the post-free state under test. */
typedef struct ann_free_expect {
    int             state;
    int             role;
    uint32_t        generation;
    uint64_t        handle;
    uint64_t        request_id;
    struct {
        uint64_t ref;
        const uint8_t  * recv_buf;
        size_t recv_cap;
        size_t recv_len;
        int recv_fin;
    } rs;
    const uint8_t  *ns_id_buf;
    size_t          ns_id_len;
    int             goaway_sent;
} ann_free_expect_t;


/* Built from the LIVE entry BEFORE the terminal runs, so no expectation can be
 * derived from the record it is meant to check. */
static ann_free_expect_t ann_declare_free(const moq_ann_entry_t *live)
{
    ann_free_expect_t w;
    w.state              = (int)MOQ_ANN_FREE;
    w.role               = 0;
    w.generation         = live->generation + 1u;    /* uint32_t increment */
    w.handle             = 0;
    w.request_id         = 0;
    w.rs.ref = 0;                        /* cleared with its key */
    w.ns_id_buf          = NULL;                     /* owned key freed */
    w.ns_id_len          = 0;
    w.rs.recv_buf       = live->rs.recv_buf;       /* co-allocated, retained */
    w.rs.recv_cap       = live->rs.recv_cap;       /* retained */
    w.rs.recv_len       = 0;
    w.rs.recv_fin       = 0;
    w.goaway_sent        = 0;
    return w;
}


/* Every field of moq_ann_entry_t, named individually. */
static int ann_check_free_record(const moq_session_t *s, int slot,
                                 const ann_free_expect_t *w, const char *what)
{
    int bad = 0;
    if (slot < 0 || (size_t)slot >= s->ann_cap) {
        fprintf(stderr, "AXIS4 %s: slot out of range\n", what);
        return 1;
    }
    const moq_ann_entry_t *e = &s->announcements[slot];
#define ANN_FREE_EQ(field, got, exp) do { \
    if ((uint64_t)(got) != (uint64_t)(exp)) { \
        fprintf(stderr, "AXIS4 %s: free record %s = %llu, expected %llu\n", \
                what, field, (unsigned long long)(got), \
                (unsigned long long)(exp)); \
        bad++; \
    } \
} while (0)
    ANN_FREE_EQ("state", (int)e->state, w->state);
    ANN_FREE_EQ("role", (int)e->role, w->role);
    ANN_FREE_EQ("generation", e->generation, w->generation);
    ANN_FREE_EQ("handle", e->handle._opaque, w->handle);
    ANN_FREE_EQ("request_id", e->request_id, w->request_id);
    ANN_FREE_EQ("request_stream_ref", e->rs.ref._v,
                w->rs.ref);
    ANN_FREE_EQ("ns_id_len", e->ns_id_len, w->ns_id_len);
    ANN_FREE_EQ("req_recv_cap", e->rs.recv_cap, w->rs.recv_cap);
    ANN_FREE_EQ("req_recv_len", e->rs.recv_len, w->rs.recv_len);
    ANN_FREE_EQ("req_recv_fin", e->rs.recv_fin ? 1 : 0, w->rs.recv_fin);
    ANN_FREE_EQ("goaway_sent", e->goaway_sent ? 1 : 0, w->goaway_sent);
#define ANN_FREE_ZERO(field) ANN_FREE_EQ(#field, e->field, 0)
    ANN_FREE_ZERO(rs.lifetime.family);
    ANN_FREE_ZERO(rs.lifetime.local_requester);
    ANN_FREE_ZERO(rs.lifetime.response);
    ANN_FREE_ZERO(rs.lifetime.local_fin);
    ANN_FREE_ZERO(rs.lifetime.peer_fin);
    ANN_FREE_ZERO(rs.lifetime.local_reset);
    ANN_FREE_ZERO(rs.lifetime.peer_reset);
    ANN_FREE_ZERO(rs.lifetime.local_stop);
    ANN_FREE_ZERO(rs.lifetime.peer_stop);
    ANN_FREE_ZERO(rs.lifetime.cancelled);
    ANN_FREE_ZERO(rs.lifetime.peer_fin_failed);
    ANN_FREE_ZERO(rs.lifetime.publisher_done);
    ANN_FREE_ZERO(rs.lifetime.incoming_update);
    ANN_FREE_ZERO(rs.lifetime.outgoing_update);
    ANN_FREE_ZERO(rs.lifetime.update_failed);
    ANN_FREE_ZERO(handoff_fin_pending);
    ANN_FREE_ZERO(response_validated);
    ANN_FREE_ZERO(response_fin);
    ANN_FREE_ZERO(deferred_reset);
    ANN_FREE_ZERO(deferred_stop);
    ANN_FREE_ZERO(deferred_fin);
    ANN_FREE_ZERO(terminal_notified);
    ANN_FREE_ZERO(finish_fin);
    ANN_FREE_ZERO(cancel_pending);
    ANN_FREE_ZERO(migration_committed);
    ANN_FREE_ZERO(deferred_cause);
    ANN_FREE_ZERO(terminal_cause);
    ANN_FREE_ZERO(deferred_code);
    ANN_FREE_ZERO(terminal_code);
    ANN_FREE_ZERO(migration_scan.skip);
    ANN_FREE_ZERO(migration_scan.hdr_len);
    for (size_t i = 0; i < sizeof(e->migration_scan.hdr); i++)
        ANN_FREE_EQ("migration_scan.hdr", e->migration_scan.hdr[i], 0);
#undef ANN_FREE_ZERO
#undef ANN_FREE_EQ
    if (e->ns_id_buf != w->ns_id_buf) {
        fprintf(stderr, "AXIS4 %s: free record ns_id_buf pointer\n", what);
        bad++;
    }
    if (e->rs.recv_buf != w->rs.recv_buf) {
        fprintf(stderr, "AXIS4 %s: free record req_recv_buf pointer\n", what);
        bad++;
    }
    return bad;
}


/* Drain accounting as the exact declared MULTISET -- ref and reason, not just a
 * count: a replaced ref or reason that preserved the count would otherwise
 * pass. This route reserves none, so the ring must be exactly what it was. */
static int ann_check_drain_same(const moq_session_t *s,
                                const drain_snap_t *want, const char *what)
{
    drain_snap_t now;
    drain_snap(s, &now);
    return drain_multiset_equals(&now, want, what);
}


/* Zero event AND action output, counted rather than sampled. */
static int ann_check_silent(moq_session_t *s, const char *what)
{
    int bad = 0, evs = 0, acts = 0;
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) { evs++; moq_event_cleanup(&e); }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) { acts++; moq_action_cleanup(&a); }
    if (evs) { fprintf(stderr, "AXIS4 %s: %d event(s)\n", what, evs); bad++; }
    if (acts) { fprintf(stderr, "AXIS4 %s: %d action(s)\n", what, acts); bad++; }
    return bad;
}


/* Axis 4 -- the terminal-cleanup obligation of `session_core_on_announce_torn_down`
 * (session_namespace.c:187), reached through the announcement arm of the bidi
 * teardown (session_subscribe.c:1473) by a peer RESET on an ESTABLISHED
 * RECEIVER announcement. It reserves no drain, so what it owes is RETIREMENT:
 * exactly one terminal event, a completely freed owner, and a generation
 * advance that makes the retired handle unusable -- including after the slot is
 * reused. Nothing here names or assumes any future FIN handoff marker.
 *
 * The first owner takes a legal empty FIN while still established, so its
 * durable latch is TRUE at the terminal and `ann_free_entry`'s explicit
 * `req_recv_fin = false` is observable at reuse; the reused request carries no
 * FIN, so a free that left the latch set would surface there.
 */
void test_axis4_announce_torn_down_retirement(void)
{
    moq_session_t *s = make_session_default();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return;
    /* No local `failures`: the MOQ_TEST_CHECK macros and the helper returns
     * below must both land in the file-scope counter, or this row's
     * diagnostics would never reach the suite total. */

    /* now_us is monotonically nondecreasing across every phase below. */
    uint64_t h0 = 0;
    moq_stream_ref_t ref = t5_admit_and_accept_at(s, 0, /*peer_ref_v=*/1,
                                                  /*now_us=*/1, false, false,
                                                  &h0);
    const int slot = g_t5_want_slot;
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { moq_session_destroy(s); return; }
    const uint32_t gen0 = moq_handle_generation(h0);

    /* A legal EMPTY FIN on the established RECEIVER request bidi. It latches
     * the durable FIN fact (session_namespace.c:121) and leaves the entry
     * established -- the handler's own "incomplete or clean FIN; wait for
     * teardown" exit -- so the later RESET still reaches the teardown through
     * the intact stream-ref key. The latch is ASSERTED, never forged. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 2),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)s->announcements[slot].state,
                          (int)MOQ_ANN_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT(s->announcements[slot].rs.recv_fin ? 1 : 0, 1);
    failures += ann_check_silent(s, "post-accept FIN");

    /* Declared BEFORE the terminal: the free record and the exact generation. */
    const ann_free_expect_t want_free =
        ann_declare_free(&s->announcements[slot]);
    MOQ_TEST_CHECK_EQ_U64(want_free.generation, (uint64_t)(gen0 + 1u));
    /* A declared sentinel: the expected ring is built from the base plus the
     * (ref, REASON) pair we declare, then the production helper adds it and the
     * live ring is compared to that. Snapshotting AFTER the add would let a
     * wrong-reason insertion become its own baseline. */
    const moq_stream_ref_t sentinel = moq_stream_ref_from_u64(0x7001);
    drain_snap_t drain_base, drain0;
    drain_snap(s, &drain_base);
    drain_snap_plus(&drain_base, sentinel, MOQ_DRAIN_NORMAL, &drain0);
    MOQ_TEST_CHECK(drain_ref_add(s, sentinel));
    failures += ann_check_drain_same(s, &drain0, "drain sentinel seeded");
    og_graph_t g_pre_terminal;
    og_capture(s, &g_pre_terminal);

    /* The peer resets the announcement bidi: one terminal event, and only that. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_reset(s, ref, 0x1, 3), (int)MOQ_OK);
    {
        int done = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_NAMESPACE_DONE) {
                done++;
                MOQ_TEST_CHECK_EQ_U64(e.u.namespace_done.ann._opaque, h0);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(done, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    /* This route owes no output of its own. */
    {
        int acts = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) { acts++; moq_action_cleanup(&a); }
        MOQ_TEST_CHECK_EQ_INT(acts, 0);
    }

    /* The owner is retired COMPLETELY, against the record declared above. */
    failures += ann_check_free_record(s, slot, &want_free, "first retirement");
    failures += ann_check_drain_same(s, &drain0, "first retirement");
    {
        og_graph_t g;
        og_capture(s, &g);
        MOQ_TEST_CHECK_EQ_INT(og_check_integrity(&g, "axis4 torn-down"), 0);
        /* The WHOLE topology, derived from the pre-terminal graph by removing
         * exactly the target's stream-ref edge -- an unrelated edge inserted,
         * removed or repointed by this call is caught here, which a
         * target-owner-only check cannot do. */
        {
            og_graph_t want_topo;
            if (ann_derive_retired_topology(&g_pre_terminal, ref._v, slot,
                                            &want_topo, "first retirement") == 0)
                MOQ_TEST_CHECK_EQ_INT(
                    og_check_same_topology(&g, &want_topo, "first retirement"), 0);
            else failures++;
        }
        MOQ_TEST_CHECK_EQ_INT(
            og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, "axis4 torn-down"), 0);
        MOQ_TEST_CHECK_EQ_INT(
            og_check_no_edge(&g, OG_DOM_NS_REF, ref._v, "axis4 torn-down"), 0);
        MOQ_TEST_CHECK_EQ_INT(
            og_check_owner_unreferenced(&g, MOQ_REQ_ANNOUNCEMENT, slot,
                                   "axis4 torn-down"), 0);
    }

    /* Idempotence BEFORE reuse: a second teardown signal on the retired stream
     * misses the lookup entirely. It must not mutate unrelated session state,
     * advance the generation again, change the graph, add a drain, or
     * resurrect the owner. */
    {
        txs_snapshot_t before;
        txs_capture(s, &ref, 1, &before);
        expect_after_call_prepare(&before);
        og_graph_t g0;
        og_capture(s, &g0);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_reset(s, ref, 0x1, 4), (int)MOQ_OK);
        failures += ann_check_silent(s, "repeat terminal");
        failures += txs_check_eq(s, &ref, 1, &before, "repeat terminal");
        /* The SAME declared record: no second generation increment. */
        failures += ann_check_free_record(s, slot, &want_free, "repeat terminal");
        failures += ann_check_drain_same(s, &drain0, "repeat terminal");
        {
            og_graph_t g1;
            og_capture(s, &g1);
            MOQ_TEST_CHECK_EQ_INT(og_check_integrity(&g1, "repeat terminal"), 0);
            /* The captured baseline is COMPARED, not merely taken. */
            MOQ_TEST_CHECK_EQ_INT(
                og_check_same_topology(&g1, &g0, "repeat terminal"), 0);
            MOQ_TEST_CHECK_EQ_INT(
                og_check_owner_unreferenced(&g1, MOQ_REQ_ANNOUNCEMENT, slot,
                                            "repeat terminal"), 0);
        }
    }

    /* Slot reuse: a DISTINCT peer stream ref and request id, no FIN, at a
     * later time. The identity is derived from the DECLARED freed generation
     * rather than read back from the now-free slot. */
    const uint32_t want_reuse_gen = want_free.generation | 1u;
    const uint64_t want_reuse_handle =
        moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT, s->session_tag,
                        want_reuse_gen, (uint32_t)slot);
    uint64_t h1 = 0;
    moq_stream_ref_t ref1 = t5_admit_and_accept_at(s, 2, /*peer_ref_v=*/2,
                                                   /*now_us=*/5, false, false,
                                                   &h1);
    MOQ_TEST_CHECK_EQ_INT(g_t5_want_slot, slot);
    MOQ_TEST_CHECK(ref1._v != ref._v);
    MOQ_TEST_CHECK_EQ_U64(h1, want_reuse_handle);
    MOQ_TEST_CHECK(h1 != h0);
    /* The reused owner carries NO FIN: this is what makes ann_free_entry's
     * explicit latch clear load-bearing. */
    MOQ_TEST_CHECK_EQ_INT(s->announcements[slot].rs.recv_fin ? 1 : 0, 0);
    MOQ_TEST_CHECK_EQ_U64(s->announcements[slot].generation,
                          (uint64_t)want_reuse_gen);

    /* The retired handle is still refused, and the REPLACEMENT owner survives
     * that refusal WHOLE -- session snapshot, owner record with its owned
     * bytes, graph topology, drain multiset and output all conserved. */
    {
        seed_snap_t owner_before;
        ann_owner_ops.capture(s, ref1, &owner_before);
        ann_whole_snap_t whole_before;
        ann_whole_capture(s, ref1, &whole_before);
        MOQ_TEST_CHECK(whole_before.valid);
        txs_snapshot_t before;
        txs_capture(s, &ref1, 1, &before);
        expect_after_call_prepare(&before);
        drain_snap_t drain1;
        drain_snap(s, &drain1);
        og_graph_t g_before;
        og_capture(s, &g_before);

        /* At a LATER declared time than the reuse: the row's clock never
         * regresses (the signed durable-case helper keeps its own fixed one). */
        MOQ_TEST_CHECK_EQ_INT((int)fire_cancel_namespace_at(s, h0, 6),
                              (int)MOQ_ERR_STALE_HANDLE);

        failures += ann_check_silent(s, "stale after reuse");
        failures += txs_check_eq(s, &ref1, 1, &before, "stale after reuse");
        ann_owner_ops.check(s, ref1, &owner_before, "stale after reuse");
        failures += ann_whole_check(s, ref1, &whole_before, "stale after reuse");
        failures += ann_check_drain_same(s, &drain1, "stale after reuse");
        {
            og_graph_t g;
            og_capture(s, &g);
            MOQ_TEST_CHECK_EQ_INT(og_check_integrity(&g, "stale after reuse"), 0);
            MOQ_TEST_CHECK_EQ_INT(
                og_check_same_topology(&g, &g_before, "stale after reuse"), 0);
            MOQ_TEST_CHECK_EQ_INT(
                ann_owner_edges(&g, ref1, "stale after reuse"), 0);
            /* The retired stream keys nothing, even though its slot is live. */
            MOQ_TEST_CHECK_EQ_INT(
                og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v,
                                 "stale after reuse"), 0);
        }
        MOQ_TEST_CHECK_EQ_INT((int)s->announcements[slot].state,
                              (int)MOQ_ANN_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_U64(s->announcements[slot].generation,
                              (uint64_t)want_reuse_gen);
    }

    moq_session_destroy(s);
}


/* Axis 4 -- the same terminal under BACKPRESSURE.
 * `session_core_on_announce_torn_down` returns MOQ_ERR_WOULD_BLOCK when the
 * event queue is full (session_namespace.c:189), and `request_stream_teardown`
 * promises the caller can re-drive it with the owner untouched. The capacity
 * row above only ever exercises the path where the event fits, so a refusal
 * that lost or mutated the owner would pass there.
 *
 * Direct session-level retry, no bridge and no timing: the retry re-delivers
 * the SAME production teardown signal, never peer bytes.
 */
void test_axis4_announce_torn_down_blocked_retry(void)
{
    /* One event slot, so a single queued event is a full queue. */
    moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 1);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return;

    uint64_t h0 = 0;
    moq_stream_ref_t ref = t5_admit_and_accept_at(s, 0, /*peer_ref_v=*/1,
                                                  /*now_us=*/1, false, false,
                                                  &h0);
    const int slot = g_t5_want_slot;
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { moq_session_destroy(s); return; }

    /* The blocker: a SECOND peer request, admitted through the real ingress and
     * deliberately left unpolled, so the sole event slot is occupied by one
     * known event with a declared handle. */
    int blocker_slot = -1;
    uint32_t blocker_gen = 0;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state == MOQ_ANN_FREE) {
            blocker_slot = (int)i;
            blocker_gen = s->announcements[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK(blocker_slot >= 0);
    if (blocker_slot < 0) { moq_session_destroy(s); return; }
    const uint64_t blocker_handle =
        moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT, s->session_tag,
                        blocker_gen, (uint32_t)blocker_slot);
    {
        moq_stream_ref_t bref = moq_stream_ref_from_u64(2);
        uint8_t m[256];
        size_t n = enc_t5_publish_namespace(m, sizeof(m), 2);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, bref, m, n, false, 2),
            (int)MOQ_OK);
    }
    MOQ_TEST_CHECK_EQ_SIZE(s->event_tail - s->event_head, (size_t)1);

    /* Everything the refusal must conserve, declared before it runs. Both the
     * TARGET and the BLOCKER refs are in the session snapshot: one ref cannot
     * make the other owner's transition visible. */
    const moq_stream_ref_t bref = moq_stream_ref_from_u64(2);
    const moq_stream_ref_t both_refs[2] = { ref, bref };
    const ann_free_expect_t want_free =
        ann_declare_free(&s->announcements[slot]);
    seed_snap_t owner_before;
    ann_owner_ops.capture(s, ref, &owner_before);
    ann_whole_snap_t whole_before, blocker_before;
    ann_whole_capture(s, ref, &whole_before);
    ann_whole_capture(s, bref, &blocker_before);
    MOQ_TEST_CHECK(whole_before.valid);
    MOQ_TEST_CHECK(blocker_before.valid);
    /* A declared sentinel, built independently (base + the declared
     * (ref, REASON) pair) rather than read back after the insertion. */
    const moq_stream_ref_t sentinel = moq_stream_ref_from_u64(0x7002);
    drain_snap_t drain_base, drain0;
    drain_snap(s, &drain_base);
    drain_snap_plus(&drain_base, sentinel, MOQ_DRAIN_NORMAL, &drain0);
    MOQ_TEST_CHECK(drain_ref_add(s, sentinel));
    failures += ann_check_drain_same(s, &drain0, "drain sentinel seeded");
    txs_snapshot_t before;
    txs_capture(s, both_refs, 2, &before);
    expect_after_call_prepare(&before);
    og_graph_t g_before;
    og_capture(s, &g_before);

    /* 1. The teardown is REFUSED for capacity. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_reset(s, ref, 0x1, 3),
        (int)MOQ_ERR_WOULD_BLOCK);
    failures += txs_check_eq(s, both_refs, 2, &before, "ann teardown blocked");
    ann_owner_ops.check(s, ref, &owner_before, "ann teardown blocked");
    failures += ann_whole_check(s, ref, &whole_before, "ann teardown blocked");
    failures += ann_whole_check(s, bref, &blocker_before,
                                "ann blocker across blocked");
    failures += ann_check_drain_same(s, &drain0, "ann teardown blocked");
    /* The target owner is untouched: still established, still keyed. */
    MOQ_TEST_CHECK_EQ_INT((int)s->announcements[slot].state,
                          (int)MOQ_ANN_ESTABLISHED);
    {
        og_graph_t g;
        og_capture(s, &g);
        MOQ_TEST_CHECK_EQ_INT(og_check_integrity(&g, "ann teardown blocked"), 0);
        MOQ_TEST_CHECK_EQ_INT(
            og_check_same_topology(&g, &g_before, "ann teardown blocked"), 0);
        MOQ_TEST_CHECK_EQ_INT(
            ann_owner_edges(&g, ref, "ann teardown blocked"), 0);
    }
    /* Only the declared blocker is queued, and no action was produced. */
    MOQ_TEST_CHECK_EQ_SIZE(s->event_tail - s->event_head, (size_t)1);
    {
        int acts = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) { acts++; moq_action_cleanup(&a); }
        MOQ_TEST_CHECK_EQ_INT(acts, 0);
    }

    /* 2. Drain EXACTLY the blocker, identified by its declared handle. */
    {
        int pub = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                pub++;
                /* The COMPLETE fixture-declared image: handle, both namespace
                 * parts, token count/type/value bytes. Expected bytes come from
                 * the fixture's own constants, never from the event. */
                const moq_namespace_published_event_t *pe =
                    &e.u.namespace_published;
                MOQ_TEST_CHECK_EQ_U64(pe->ann._opaque, blocker_handle);
                MOQ_TEST_CHECK_EQ_SIZE(pe->track_namespace.count, (size_t)2);
                MOQ_TEST_CHECK(pe->track_namespace.parts != NULL);
                if (pe->track_namespace.count == 2 &&
                    pe->track_namespace.parts) {
                    const moq_bytes_t *q0 = &pe->track_namespace.parts[0];
                    const moq_bytes_t *q1 = &pe->track_namespace.parts[1];
                    MOQ_TEST_CHECK_EQ_SIZE(q0->len, sizeof(k_t5_ns0));
                    MOQ_TEST_CHECK(q0->data != NULL);
                    if (q0->len == sizeof(k_t5_ns0) && q0->data)
                        MOQ_TEST_CHECK(memcmp(q0->data, k_t5_ns0,
                                              sizeof(k_t5_ns0)) == 0);
                    MOQ_TEST_CHECK_EQ_SIZE(q1->len, sizeof(k_t5_ns1));
                    MOQ_TEST_CHECK(q1->data != NULL);
                    if (q1->len == sizeof(k_t5_ns1) && q1->data)
                        MOQ_TEST_CHECK(memcmp(q1->data, k_t5_ns1,
                                              sizeof(k_t5_ns1)) == 0);
                } else MOQ_TEST_CHECK(0);
                MOQ_TEST_CHECK_EQ_SIZE(pe->token_count, (size_t)1);
                MOQ_TEST_CHECK(pe->tokens != NULL);
                if (pe->token_count == 1 && pe->tokens) {
                    const moq_bytes_t *tv = &pe->tokens[0].token_value;
                    MOQ_TEST_CHECK_EQ_U64(pe->tokens[0].token_type, T5_TOK_TYPE);
                    MOQ_TEST_CHECK_EQ_SIZE(tv->len, sizeof(k_t5_tok));
                    MOQ_TEST_CHECK(tv->data != NULL);
                    if (tv->len == sizeof(k_t5_tok) && tv->data)
                        MOQ_TEST_CHECK(memcmp(tv->data, k_t5_tok,
                                              sizeof(k_t5_tok)) == 0);
                } else MOQ_TEST_CHECK(0);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(pub, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* 3. Retry the SAME teardown signal -- no peer bytes are re-delivered. */
    og_graph_t g_pre_retry;
    og_capture(s, &g_pre_retry);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_reset(s, ref, 0x1, 4), (int)MOQ_OK);
    {
        int done = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_NAMESPACE_DONE) {
                done++;
                MOQ_TEST_CHECK_EQ_U64(e.u.namespace_done.ann._opaque, h0);
            } else other++;
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(done, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    failures += ann_check_free_record(s, slot, &want_free, "ann teardown retry");
    failures += ann_check_drain_same(s, &drain0, "ann teardown retry");
    /* The UNRELATED blocker owner is whole across the target's retirement. */
    failures += ann_whole_check(s, bref, &blocker_before,
                                "ann blocker across retry");
    {
        int acts = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) { acts++; moq_action_cleanup(&a); }
        MOQ_TEST_CHECK_EQ_INT(acts, 0);
    }
    {
        og_graph_t g;
        og_capture(s, &g);
        MOQ_TEST_CHECK_EQ_INT(og_check_integrity(&g, "ann teardown retry"), 0);
        /* Derived from the PRE-RETRY graph by removing exactly the target's
         * stream-ref edge, so the blocker's edges must survive untouched. */
        {
            og_graph_t want_topo;
            if (ann_derive_retired_topology(&g_pre_retry, ref._v, slot,
                                            &want_topo, "ann teardown retry") == 0)
                MOQ_TEST_CHECK_EQ_INT(
                    og_check_same_topology(&g, &want_topo,
                                           "ann teardown retry"), 0);
            else failures++;
        }
        MOQ_TEST_CHECK_EQ_INT(
            og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v,
                             "ann teardown retry"), 0);
        MOQ_TEST_CHECK_EQ_INT(
            og_check_owner_unreferenced(&g, MOQ_REQ_ANNOUNCEMENT, slot,
                                        "ann teardown retry"), 0);
    }

    /* 4. Once more: silent and fully conserved. */
    {
        txs_snapshot_t after;
        txs_capture(s, both_refs, 2, &after);
        expect_after_call_prepare(&after);
        og_graph_t g_idem;
        og_capture(s, &g_idem);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_reset(s, ref, 0x1, 5), (int)MOQ_OK);
        failures += ann_check_silent(s, "ann teardown idempotent");
        failures += txs_check_eq(s, both_refs, 2, &after,
                                 "ann teardown idempotent");
        failures += ann_check_free_record(s, slot, &want_free,
                                          "ann teardown idempotent");
        failures += ann_check_drain_same(s, &drain0, "ann teardown idempotent");
        failures += ann_whole_check(s, bref, &blocker_before,
                                    "ann blocker across idempotent");
        {
            og_graph_t g;
            og_capture(s, &g);
            MOQ_TEST_CHECK_EQ_INT(
                og_check_integrity(&g, "ann teardown idempotent"), 0);
            MOQ_TEST_CHECK_EQ_INT(
                og_check_same_topology(&g, &g_idem, "ann teardown idempotent"), 0);
        }
    }

    moq_session_destroy(s);
}


void test_axis3_durable_terminals(void)
{
    /* session_namespace.c:792 -> :85 -- the withdrawal reserves its drain
     * FIN-aware, consulting the latch: none when the peer half already closed. */
    static const durable_case_t announcer_teardown = {
        .name = "announcer teardown", .make = make_session_client,
        .arm = arm_announcer_established,
        .fire = fire_publish_namespace_done,
        .want_output = want_announcer_teardown,
        .hooks = &durable_abort_hooks, .owner = &ann_owner_ops,
        .owner_edges = ann_owner_edges, .extra = &ann_full_ops
    };
    run_durable_matrix(&announcer_teardown);

    /* session_subscribe.c:3012 -- the cancel reserves its drain FIN-aware,
     * consulting the latch: none when the peer half already closed. */
    {
        static const durable_case_t sub_cancel = {
            .name = "subscriber unsubscribe", .make = make_session_client,
            .arm = arm_subscriber_established, .fire = fire_unsubscribe,
            .want_output = want_bidi_cancel,
            .hooks = &durable_abort_hooks, .owner = &sub_owner_ops,
            .owner_edges = sub_owner_edges, .extra = &r6_full_ops
        };
        run_durable_matrix(&sub_cancel);
    }

    /* session_fetch.c:1485 -- the cancel reserves its drain FIN-aware,
     * consulting the latch: none when the peer half already closed. */
    {
        static const durable_case_t fetch_cancel = {
            .name = "fetch cancel", .make = make_session_client,
            .arm = arm_fetch_pending, .fire = fire_fetch_cancel,
            .want_output = want_fetch_cancel,
            .hooks = &durable_abort_hooks, .owner = &fetch_owner_ops,
            .owner_edges = fetch_owner_edges, .extra = &r5_full_ops
        };
        run_durable_matrix(&fetch_cancel);
    }

    /* session_subscribe.c:3250 -- the terminal reserves its drain FIN-aware,
     * consulting the entry's own latch: none when the peer half already closed. */
    {
        static const durable_case_t done_sub = {
            .name = "done subscribe", .make = make_session_default,
            .arm = arm_accepted_subscription, .fire = fire_done_subscribe,
            .want_output = want_done_subscribe,
            .hooks = &r7_hooks, .owner = &sub_owner_ops,
            .owner_edges = r7_owner_edges, .extra = &r7_full_ops
        };
        run_durable_matrix(&done_sub);
    }

    /* session_publish.c:1847 -- the positive control: this terminal already
     * consults the latch, so all four rows complete. */
    {
        static const durable_case_t finish_pub = {
            .name = "finish publish", .make = make_session_client,
            .arm = arm_publisher_established, .fire = fire_finish_publish,
            .want_output = want_finish_publish,
            .hooks = &r7_hooks, .owner = &r4_owner_ops,
            .owner_edges = r4_owner_edges, .extra = &r4_full_ops
        };
        run_durable_matrix(&finish_pub);
    }

    /* session_namespace.c:582 -- the announcer-side terminal REQUEST_ERROR.
     * Its FIN arrives with the message, so it runs on the current-call
     * runner rather than the durable one. */
    {
        static const terminal_case_t ann_error = {
            .name = "announcement error", .make = make_session_client,
            .arm = arm_announcement_pending,
            .encode = enc_announcement_error,
            .drain_reason = MOQ_DRAIN_NORMAL,
            .want_output = want_announcement_error, .hooks = &r3_hooks,
            .owner = &ann_owner_ops, .extra = &ann_full_ops,
            .graph = &r3_graph_ops
        };
        /* Permanently pinned, all three members: this route's coverage is the
         * reason the capability exists, so dropping or repointing any of them
         * fails here rather than silently reducing what the runner checks. */
        MOQ_TEST_CHECK(ann_error.graph == &r3_graph_ops);
        MOQ_TEST_CHECK(r3_graph_ops.owner_edges == r3_owner_edges);
        MOQ_TEST_CHECK_EQ_INT(r3_graph_ops.owner_kind,
                              (int)MOQ_REQ_ANNOUNCEMENT);
        MOQ_TEST_CHECK(r3_graph_ops.owner_slot == &g_r3_want_slot);
        run_terminal_matrix(&ann_error, 2);
    }

    /* session_publish.c:144 -- the second positive control, reached through the
     * public deferred sweep. */
    {
        static const durable_case_t deferred_finalize = {
            .name = "deferred publish finalize", .make = make_session_default,
            .arm = arm_deferred_publish, .fire = fire_deferred_sweep,
            .want_output = want_deferred_finalize,
            .hooks = &r1_hooks, .owner = &r4_owner_ops,
            .owner_edges = r1_owner_edges, .extra = &r1_full_ops,
            .refusal_is_silent = 1
        };
        run_durable_matrix(&deferred_finalize);
    }

}
