#include "test_cases.h"

void nso_read(const moq_session_t *s, int slot, nso_snap_t *v)
{
    memset(v, 0, sizeof(*v));
    if (slot < 0 || (size_t)slot >= s->ns_sub_cap) { v->invalid = 1; return; }
    const moq_ns_sub_entry_t *e = &s->ns_subs[slot];
    v->present = (e->state != MOQ_NS_SUB_FREE);
    v->state = (int)e->state;
    v->stream_kind = (int)e->stream_kind;
    v->generation = e->generation;
    v->handle = e->handle._opaque;
    v->request_id = e->request_id;
    v->stream_ref = e->stream_ref._v;
    v->ep_kind = (int)e->request_ep.kind;
    v->ep_slot = e->request_ep.slot;
    v->ep_has_request_id = e->request_ep.has_request_id ? 1 : 0;
    v->ep_request_id = e->request_ep.request_id;
    v->ep_has_stream_ref = e->request_ep.has_stream_ref ? 1 : 0;
    v->ep_stream_ref = e->request_ep.stream_ref._v;
    v->interest = (int)e->namespace_interest;
    v->forward = e->forward ? 1 : 0;
    v->pending_fin = e->pending_fin ? 1 : 0;
    v->parse_complete = e->parse_complete ? 1 : 0;
    v->got_response = e->got_response ? 1 : 0;
    v->handoff_fin = e->handoff_fin_pending ? 1 : 0;
    v->closing_remote_error = e->closing_remote_error ? 1 : 0;
    v->goaway_sent = e->goaway_sent ? 1 : 0;
    v->auth_processed = e->auth_processed ? 1 : 0;
    v->auth_committed = e->auth_committed ? 1 : 0;
    v->auth_reject_code = e->auth_reject_code;
    v->token_count = e->token_count;
    v->prefix_valid = e->prefix_valid ? 1 : 0;
    v->prefix_count = e->prefix_count;
    /* Refuse BEFORE dereferencing: a count past the wire maximum, a count with
     * no array, or a non-empty part with no data would each be an out-of-range
     * read inside the harness meant to diagnose it. */
    if (e->prefix_count > NSO_MAX_PARTS) { v->invalid = 1; return; }
    if (e->prefix_count > 0 && !e->prefix_parts) { v->invalid = 1; return; }
    /* Allocation topology fails CLOSED, to the same discipline as the track-sub
     * inventory: for a stored prefix both allocations must exist, every part
     * must lie inside the owned buffer by SUBTRACTION-ONLY bounds proven BEFORE
     * `prefix_buf + off` is formed, its pointer must equal that declared offset,
     * and the offsets must sum to the owned extent. Bytes are copied only after
     * every check for that part passes, so a corrupt readable pointer is
     * refused rather than compared, and an unreadable one is never touched. */
    v->prefix_buf_len = e->prefix_buf_len;
    if (e->prefix_valid && e->prefix_count > 0 && !e->prefix_buf) {
        v->invalid = 1;
        return;
    }
    {
        size_t off = 0;
        for (size_t i = 0; i < e->prefix_count; i++) {
            size_t l = e->prefix_parts[i].len;
            if (l > 0 && !e->prefix_parts[i].data) { v->invalid = 1; return; }
            if (l > 255 || v->prefix_len + 1 + l > NSO_BUF_MAX) {
                v->invalid = 1;
                return;
            }
            if (!e->prefix_valid) continue;   /* counted, not encoded */
            if (off > e->prefix_buf_len || l > e->prefix_buf_len - off) {
                v->invalid = 1;
                return;
            }
            if (e->prefix_parts[i].data != (const uint8_t *)e->prefix_buf + off) {
                v->invalid = 1;
                return;
            }
            off += l;
            v->prefix[v->prefix_len++] = (uint8_t)l;
            if (l) memcpy(v->prefix + v->prefix_len, e->prefix_parts[i].data, l);
            v->prefix_len += l;
        }
        if (e->prefix_valid && off != e->prefix_buf_len) {
            v->invalid = 1;
            return;
        }
    }
    v->recv_buf = (const void *)e->recv_buf;
    v->recv_cap = e->recv_cap;
    v->recv_len = e->recv_len;
    if (e->recv_len > NSO_BUF_MAX || (e->recv_len && !e->recv_buf)) {
        v->invalid = 1;
        return;
    }
    if (e->recv_len) memcpy(v->recv, e->recv_buf, e->recv_len);
}


int nso_diff(const nso_snap_t *have, const nso_snap_t *want,
             const char *what)
{
    if (have->invalid || want->invalid) {
        TXS_DIAG("TXN %s: incomparable namespace-sub record\n", what);
        return 1;
    }
    int bad = 0;
#define NSO_EQ(field) do { \
        if ((uint64_t)have->field != (uint64_t)want->field) { \
            TXS_DIAG("TXN %s: ns owner " #field " %llu, expected %llu\n", what, \
                     (unsigned long long)have->field, \
                     (unsigned long long)want->field); \
            bad++; \
        } \
    } while (0)
    NSO_EQ(present); NSO_EQ(state); NSO_EQ(stream_kind); NSO_EQ(generation);
    NSO_EQ(handle); NSO_EQ(request_id); NSO_EQ(stream_ref);
    NSO_EQ(ep_kind); NSO_EQ(ep_slot); NSO_EQ(ep_has_request_id);
    NSO_EQ(ep_request_id); NSO_EQ(ep_has_stream_ref); NSO_EQ(ep_stream_ref);
    NSO_EQ(interest); NSO_EQ(forward); NSO_EQ(pending_fin);
    NSO_EQ(parse_complete); NSO_EQ(got_response); NSO_EQ(handoff_fin);
    NSO_EQ(closing_remote_error);
    NSO_EQ(goaway_sent); NSO_EQ(auth_processed); NSO_EQ(auth_committed);
    NSO_EQ(auth_reject_code); NSO_EQ(token_count); NSO_EQ(prefix_valid);
    NSO_EQ(prefix_count); NSO_EQ(prefix_len); NSO_EQ(recv_len);
    /* Storage facts are ALWAYS compared. Every namespace-sub receive buffer is
     * carved out of the session allocation at create (`session.c`, the
     * `ns_subs[i].recv_buf = mem + off_ns_recv + …` loop), so a free slot
     * exposes its buffer identity and capacity before any ingress -- there is
     * no slot for which they are unknowable. */
    NSO_EQ(prefix_buf_len); NSO_EQ(recv_cap);
    if (have->recv_buf != want->recv_buf) {
        TXS_DIAG("TXN %s: ns owner receive buffer identity changed\n", what);
        bad++;
    }
#undef NSO_EQ
    if (have->prefix_len == want->prefix_len && have->prefix_len &&
        memcmp(have->prefix, want->prefix, have->prefix_len) != 0) {
        TXS_DIAG("TXN %s: ns owner prefix bytes differ\n", what);
        bad++;
    }
    if (have->recv_len == want->recv_len && have->recv_len &&
        memcmp(have->recv, want->recv, have->recv_len) != 0) {
        TXS_DIAG("TXN %s: ns owner retained request bytes differ\n", what);
        bad++;
    }
    return bad;
}


/* -- SUBSCRIBE_NAMESPACE (P6) ---------------------------------------- */

/* This family's owner lives in the namespace index rather than the request
 * registry, and its durable FIN state is `pending_fin`. Its owner-present FIN
 * outcome is a reciprocal close: the peer's graceful cancel is answered by
 * closing our own send half, then the entry is reclaimed. That close needs an
 * action slot, so an action-blocked call must preserve both the owner and the
 * observed close for the retry. */

static txs_op_hooks_t nss_hooks_for_actions;   /* normalize_action only */


typedef struct nss_hook_state {
    int      present;
    uint64_t handle;
    uint64_t stream_ref;
    int      pending_fin;
    int      handoff_fin;
    int      state;
    uint32_t generation;
    int      busy;
} nss_hook_state_t;


static int ns_busy_count(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->ns_sub_cap; i++)
        if (s->ns_subs[i].state != MOQ_NS_SUB_FREE) n++;
    return n;
}


const moq_ns_sub_entry_t *ns_by_ref(const moq_session_t *s,
                                    moq_stream_ref_t ref)
{
    int32_t slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
    if (slot < 0 || (size_t)slot >= s->ns_sub_cap) return NULL;
    return &s->ns_subs[slot];
}


/* Allocation-identity CONSERVATION, and nothing more. A stored prefix's
 * addresses cannot be predicted before first ingress, so they are never used to
 * authorize the first call's extent, layout or bytes -- the declared inventory
 * does that. Once that absolute check has passed, these identities must not
 * change while the owner stays live, and this snapshot is what proves it. */
typedef struct nsc_snap {
    const void *prefix_buf;
    const void *prefix_parts;
    const void *part0_data;
    size_t      count;
    int         invalid;
} nsc_snap_t;


static void nsc_read(const moq_session_t *s, int slot, nsc_snap_t *v)
{
    memset(v, 0, sizeof(*v));
    if (slot < 0 || (size_t)slot >= s->ns_sub_cap) { v->invalid = 1; return; }
    const moq_ns_sub_entry_t *e = &s->ns_subs[slot];
    v->prefix_buf = (const void *)e->prefix_buf;
    v->prefix_parts = (const void *)e->prefix_parts;
    v->count = e->prefix_count;
    if (e->prefix_count > 0) {
        if (!e->prefix_parts) { v->invalid = 1; return; }
        v->part0_data = (const void *)e->prefix_parts[0].data;
    }
}


static int nsc_same(const nsc_snap_t *a, const nsc_snap_t *b, const char *what)
{
    if (a->invalid || b->invalid) {
        TXS_DIAG("TXN %s: incomparable allocation snapshot\n", what);
        return 1;
    }
    int bad = 0;
    if (a->prefix_buf != b->prefix_buf) bad++;
    if (a->prefix_parts != b->prefix_parts) bad++;
    if (a->part0_data != b->part0_data) bad++;
    if (a->count != b->count) bad++;
    if (bad)
        TXS_DIAG("TXN %s: prefix allocation identity changed\n", what);
    return bad;
}


static int nss_declare_free_slot(const moq_session_t *s)
{
    for (size_t i = 0; i < s->ns_sub_cap; i++)
        if (s->ns_subs[i].state == MOQ_NS_SUB_FREE) return (int)i;
    return -1;
}


/* A committed publisher-side namespace-sub owner in these rows holds its own
 * index edge AND its pending-phase request-id edge, and no request-stream-ref
 * edge -- the staging slot's key is reclaimed when that slot is freed. */
static int nss_check_owner_edges(const og_graph_t *g, int slot,
                                 moq_stream_ref_t ref, uint64_t rid,
                                 const char *what)
{
    const og_edge_spec_t want[] = {
        { OG_DOM_NS_REF,  ref._v },
        { OG_DOM_REQ_RID, rid    },
    };
    int bad = og_check_owner_edges(g, (int)MOQ_REQ_NAMESPACE_SUB, slot,
                                   want, 2, what);
    bad += og_check_no_edge(g, OG_DOM_REQ_STREAMREF, ref._v, what);
    return bad;
}


/* The record a committed publisher-side owner must hold, built from values the
 * fixture declared before ingress -- never read back from the call under test.
 * `recv` is the exact SUBSCRIBE_NAMESPACE the fixture encoded. */
static void nss_declare(nso_snap_t *v, int slot, uint32_t gen, uint64_t handle,
                        uint64_t rid, uint64_t ref, int handoff_fin,
                        const uint8_t *req, size_t req_len,
                        const void *recv_buf, size_t recv_cap)
{
    (void)slot;
    memset(v, 0, sizeof(*v));
    v->present = 1;
    v->state = (int)MOQ_NS_SUB_PENDING_PUBLISHER;
    v->stream_kind = (int)MOQ_STREAM_KIND_NAMESPACE_SUB;
    v->generation = gen;
    v->handle = handle;
    v->request_id = rid;
    v->stream_ref = ref;
    v->ep_kind = (int)MOQ_REQ_NAMESPACE_SUB;
    v->ep_slot = slot;
    v->ep_has_request_id = 1;
    v->ep_request_id = rid;
    v->ep_has_stream_ref = 1;
    v->ep_stream_ref = ref;
    v->interest = (int)MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    v->forward = 1;                   /* the wire default when unset */
    v->pending_fin = 0;
    v->parse_complete = 1;
    v->got_response = 0;
    v->handoff_fin = handoff_fin;
    v->closing_remote_error = 0;
    v->goaway_sent = 0;
    v->auth_processed = 1;
    v->auth_committed = 1;
    v->auth_reject_code = 0;
    v->token_count = 1;               /* nss_feed carries one USE_VALUE token */
    v->prefix_valid = 1;
    v->prefix_count = 2;              /* ("live", "eu") */
    v->prefix[v->prefix_len++] = 4;
    memcpy(v->prefix + v->prefix_len, "live", 4); v->prefix_len += 4;
    v->prefix[v->prefix_len++] = 2;
    memcpy(v->prefix + v->prefix_len, "eu", 2);   v->prefix_len += 2;
    /* The stored extent is independently known from the fixture's own prefix
     * ("live" + "eu"), not read back from the entry. */
    v->prefix_buf_len = 6;
    v->recv_buf = recv_buf;
    v->recv_cap = recv_cap;
    v->recv_len = req_len;
    if (req_len && req_len <= NSO_BUF_MAX) memcpy(v->recv, req, req_len);
}


moq_result_t nss_feed(moq_session_t *s, void *ctx, moq_stream_ref_t ref,
                      bool fin)
{
    nss_ctx_t *c = (nss_ctx_t *)ctx;
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live"),
                            MOQ_BYTES_LITERAL("eu") };
    moq_namespace_t ns = { parts, 2 };
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    mp.auth_token_count = 1;
    mp.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    mp.auth_tokens[0].token_type = 7;
    mp.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("nsstok");
    uint8_t m[192];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, sizeof(m));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe_namespace(&w, c->request_id, &ns, &mp),
        (int)MOQ_OK);
    /* Retain exactly what went on the wire so the declared record owns the
     * bytes the entry must hold, rather than reading them back from it. */
    c->last_req_len = moq_buf_writer_offset(&w);
    memcpy(c->last_req, m, c->last_req_len);
    return moq_session_on_bidi_stream_bytes(s, ref, m,
                                            c->last_req_len, fin, 1);
}


static moq_result_t nss_app_reject(moq_session_t *s, void *ctx, uint64_t handle)
{
    (void)ctx;
    moq_ns_sub_handle_t h;
    h._opaque = handle;
    moq_reject_ns_sub_cfg_t rj;
    moq_reject_ns_sub_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    return moq_session_reject_ns_sub(s, h, &rj, 1);
}


static void nss_hook_capture(const moq_session_t *s, void *vctx, void *vst)
{
    nss_ctx_t *c = (nss_ctx_t *)vctx;
    nss_hook_state_t *st = (nss_hook_state_t *)vst;
    memset(st, 0, sizeof(*st));
    st->busy = ns_busy_count(s);
    const moq_ns_sub_entry_t *e = ns_by_ref(s, c->ref);
    if (!e) return;
    st->present = 1;
    st->handle = e->handle._opaque;
    st->stream_ref = e->stream_ref._v;
    st->pending_fin = e->pending_fin;
    st->handoff_fin = e->handoff_fin_pending;
    st->state = (int)e->state;
    st->generation = e->generation;
}


static int nss_hook_check(const moq_session_t *s, void *vctx, const void *vst)
{
    nss_hook_state_t now;
    nss_hook_capture(s, vctx, &now);
    const nss_hook_state_t *want = (const nss_hook_state_t *)vst;
    int bad = 0;
    if (now.present != want->present) bad++;
    if (now.handle != want->handle) bad++;
    if (now.stream_ref != want->stream_ref) bad++;
    if (now.pending_fin != want->pending_fin) bad++;
    if (now.handoff_fin != want->handoff_fin) bad++;
    if (now.state != want->state) bad++;
    if (now.generation != want->generation) bad++;
    if (now.busy != want->busy) bad++;
    if (bad)
        TXS_DIAG("TXN nss hooks: present %d handle %llu ref %llu fin %d "
                 "handoff %d state %d gen %u busy %d, expected "
                 "%d/%llu/%llu/%d/%d/%d/%u/%d\n",
                 now.present, (unsigned long long)now.handle,
                 (unsigned long long)now.stream_ref, now.pending_fin,
                 now.handoff_fin, now.state, now.generation, now.busy,
                 want->present, (unsigned long long)want->handle,
                 (unsigned long long)want->stream_ref, want->pending_fin,
                 want->handoff_fin, want->state, want->generation, want->busy);
    return bad;
}


static bool nss_norm_event(const moq_event_t *ev, void *vctx,
                           txs_norm_vec_t *out)
{
    nss_ctx_t *c = (nss_ctx_t *)vctx;
    txs_img_t im;
    txs_img_init(&im);
    switch (ev->kind) {
    case MOQ_EVENT_NS_SUB_REQUEST: {
        const moq_ns_sub_request_event_t *rq = &ev->u.ns_sub_request;
        c->last_handle = rq->handle._opaque;
        txs_img_u64(&im, rq->handle._opaque);
        txs_img_u64(&im, (uint64_t)rq->namespace_interest);
        txs_img_u64(&im, rq->forward ? 1u : 0u);
        if (!txs_img_parts(&im, "ns_sub prefix", rq->track_namespace_prefix.parts,
                           rq->track_namespace_prefix.count,
                           MOQ_DECODED_MAX_NAMESPACE_PARTS))
            return false;
        if (!txs_img_tokens(&im, "ns_sub tokens", rq->tokens,
                            rq->token_count, MOQ_DECODED_MAX_TOKENS))
            return false;
        break;
    }
    default:
        TXS_DIAG("TXN nss: unnormalized event kind %u\n", (unsigned)ev->kind);
        return false;
    }
    return txs_norm_append_img(out, ev->kind, &im);
}


/* The reciprocal close is an action of its own kind, so the family's action
 * normalizer must describe it as well as the bidi sends. */
bool nss_norm_action(const moq_action_t *a, void *vctx,
                     txs_norm_vec_t *out)
{
    if (a->kind == MOQ_ACTION_CLOSE_BIDI_STREAM) {
        txs_img_t im;
        txs_img_init(&im);
        txs_img_u64(&im, a->u.close_bidi_stream.stream_ref._v);
        return txs_norm_append_img(out, a->kind, &im);
    }
    return pub_norm_action(a, vctx, out);
}


static void want_nss_request(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    /* Draft-18 supplies both as fixed values. */
    txs_img_u64(&im, (uint64_t)MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE);
    txs_img_u64(&im, 1);                    /* forward */
    txs_img_u64(&im, 2);
    txs_img_bytes(&im, (const uint8_t *)"live", 4);
    txs_img_bytes(&im, (const uint8_t *)"eu", 2);
    txs_img_u64(&im, 1);
    txs_img_u64(&im, 7);
    txs_img_bytes(&im, (const uint8_t *)"nsstok", 6);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_EVENT_NS_SUB_REQUEST, &im));
}


void want_close_bidi(txs_norm_vec_t *v, moq_stream_ref_t ref)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, ref._v);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_CLOSE_BIDI_STREAM, &im));
}


nss_ctx_t nss_ctx = { 0, { 1 }, 0, { 0 }, 0 };


static fin_owner_case_t nss_family(void)
{
    fin_owner_case_t f;
    memset(&f, 0, sizeof(f));
    f.name = "subscribe-namespace";
    f.ctx = &nss_ctx;
    f.caps = FIN_TERM_APP_REJECT;
    f.feed = nss_feed;
    /* The owner is namespace-indexed, which the shared capture reports as
     * TXS_OWNER_NS; the descriptor's kind names the request family it came
     * from. */
    f.owner_kind = MOQ_REQ_NAMESPACE_SUB;
    f.owner_role = FIN_OWNER_ROLE_NA;
    f.owner_state = (int)MOQ_NS_SUB_PENDING_PUBLISHER;
    f.app_reject = nss_app_reject;
    f.hooks.ctx = &nss_ctx;
    f.hooks.capture = nss_hook_capture;
    f.hooks.check = nss_hook_check;
    f.hooks.normalize_event = nss_norm_event;
    f.hooks.normalize_action = nss_norm_action;
    /* The id-0 burn compares its rejection as a full envelope; it needs only
     * the action normalizer, and this is the one place it is available. */
    nss_hooks_for_actions.ctx = f.ctx;
    nss_hooks_for_actions.normalize_action = nss_norm_action;
    return f;
}


/* The owner, in the namespace index rather than the request registry. */
static int nss_owner_present_problems(const moq_session_t *s,
                                      moq_stream_ref_t ref,
                                      moq_request_kind_t want_kind,
                                      int want_role, int want_state)
{
    int bad = 0;
    /* The family is role-less and namespace-indexed: both facts are declared
     * by the descriptor and checked here rather than assumed. */
    if (want_kind != MOQ_REQ_NAMESPACE_SUB) bad++;
    if (want_role != FIN_OWNER_ROLE_NA) bad++;
    const moq_ns_sub_entry_t *e = ns_by_ref(s, ref);
    if (!e) return bad + 1;
    if ((int)e->state != want_state) bad++;
    if (e->stream_ref._v != ref._v) bad++;
    if (ns_busy_count(s) != 1) bad++;
    txs_owner_t o = txs_owner_capture(s, ref);
    if (o.kind != TXS_OWNER_NS) bad++;
    else if (o.slot != (int)(e - s->ns_subs)) bad++;
    if (bad)
        TXS_DIAG("TXN nss owner: state %d ref %llu busy %d owner-kind %d, "
                 "expected state %d ref %llu busy 1 owner-kind %d\n",
                 (int)e->state, (unsigned long long)e->stream_ref._v,
                 ns_busy_count(s), o.kind, want_state,
                 (unsigned long long)ref._v, TXS_OWNER_NS);
    return bad;
}


static void check_nss_retired(moq_session_t *s, moq_stream_ref_t ref)
{
    MOQ_TEST_CHECK_EQ_INT(ns_busy_count(s), 0);
    MOQ_TEST_CHECK(ns_by_ref(s, ref) == NULL);
    txs_owner_t o = txs_owner_capture(s, ref);
    MOQ_TEST_CHECK_EQ_INT(o.kind, TXS_OWNER_NONE);
}


/* Retire inbound request id 0 through this family's own public sequence -- a
 * complete SUBSCRIBE_NAMESPACE, its application rejection and the peer's FIN --
 * so every measured P6 owner carries a NONZERO request id. Without it the
 * ordinal-zero key sits in the graph, where the staging cleanup's unqualified
 * remove-by-id(0) can delete another owner's edge, and masks the topology
 * these rows assert. It encodes its own message rather than borrowing the
 * family feed, so the shared fixture context is never mutated, and nothing
 * about the burned exchange is measured. */
/* `rid` is the inbound ordinal to retire and `busy_after` the number of owners
 * expected to remain (nonzero when an unrelated owner is deliberately live).
 * The slot it frees also RETAINS its packed handle -- the free advances the
 * generation but does not clear the handle -- and the event-blocked row reads
 * that residue to build an equivalent one on its own target slot. */
static void nss_burn_request(moq_session_t *s, moq_stream_ref_t burn_ref,
                             uint64_t rid, int busy_after)
{
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("burn") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    uint8_t m[192];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, sizeof(m));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe_namespace(&w, rid, &ns, &mp), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, burn_ref, m,
                                              moq_buf_writer_offset(&w),
                                              false, 1),
        (int)MOQ_OK);
    uint64_t burn_handle = 0;
    {   /* EXACTLY one NS_SUB_REQUEST and nothing else. */
        int reqs = 0, other = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) {
                reqs++;
                burn_handle = ev.u.ns_sub_request.handle._opaque;
            } else other++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(reqs, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    MOQ_TEST_CHECK(burn_handle != 0);
    {
        moq_ns_sub_handle_t h;
        h._opaque = burn_handle;
        moq_reject_ns_sub_cfg_t rj;
        moq_reject_ns_sub_cfg_init(&rj);
        rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_ns_sub(s, h, &rj, 1),
                              (int)MOQ_OK);
    }
    {   /* Zero events at the rejection point, counted explicitly rather than
         * through a normalizer that would have to classify an unexpected kind. */
        int evs = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            evs++;
            fprintf(stderr, "FAIL: %s:%d: unexpected burn event kind %u\n",
                    __FILE__, __LINE__, (unsigned)ev.kind);
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(evs, 0);
    }
    {   /* EXACTLY one REQUEST_ERROR(NOT_SUPPORTED, retry 0, empty reason) with
         * FIN on the burn ref, compared as a FULL ENVELOPE -- kind and ref
         * alone would accept any payload -- and nothing else queued. */
        txs_norm_vec_t acts, wacts;
        txs_norm_init(&acts);
        txs_norm_init(&wacts);
        collect_actions(s, &nss_hooks_for_actions, &acts);
        want_reject_error_code(&wacts, burn_ref,
                               (uint64_t)MOQ_REQUEST_ERROR_NOT_SUPPORTED);
        failures += txs_norm_equals(&acts, &wacts, "nss burn rejection");
        txs_norm_free(&acts);
        txs_norm_free(&wacts);
    }
    /* One drain reference is owed while the peer half is open. */
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, (size_t)1);
    /* The late FIN releases exactly that reference and produces no output. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, burn_ref, NULL, 0, true, 1),
        (int)MOQ_OK);
    {
        int any = 0;
        moq_event_t ev;
        moq_action_t a;
        while (moq_session_poll_events(s, &ev, 1) > 0) { any++; moq_event_cleanup(&ev); }
        while (moq_session_poll_actions(s, &a, 1) > 0) { any++; moq_action_cleanup(&a); }
        MOQ_TEST_CHECK_EQ_INT(any, 0);
    }
    MOQ_TEST_CHECK_EQ_INT(ns_busy_count(s), busy_after);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    /* No edge of the burned request survives to be measured against. */
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss burn retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_RID, rid, "nss burn retired");
        failures += og_check_no_edge(&g, OG_DOM_NS_REF, burn_ref._v,
                                     "nss burn retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, burn_ref._v,
                                     "nss burn retired");
    }
}


/* Permanent quiet self-checks for the namespace-sub inventory's refusal
 * branches. Each perturbs one field of a live owner, requires an INCOMPARABLE
 * capture AND an unequal comparison, and restores the field. Every perturbation
 * is itself safe -- nulled pointers, an offset that stays INSIDE the owned
 * buffer, and lengths that only shrink or grow the declared extent. */
static void nso_selfcheck_invalid(moq_session_t *s, int slot,
                                  const nso_snap_t *valid, const char *what)
{
    nso_snap_t got;
    nso_read(s, slot, &got);
    if (!got.invalid) {
        fprintf(stderr, "FAIL: %s:%d: nso_read accepted %s\n",
                __FILE__, __LINE__, what);
        failures++;
        return;
    }
    txs_quiet = 1;
    int diff = nso_diff(&got, valid, what);
    txs_quiet = 0;
    if (diff == 0) {
        fprintf(stderr, "FAIL: %s:%d: nso_diff compared equal on %s\n",
                __FILE__, __LINE__, what);
        failures++;
    }
}


static void nss_inventory_selfchecks(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    nss_ctx_t *c = (nss_ctx_t *)f->ctx;
    uint64_t saved_id = c->request_id;
    nss_burn_request(s, moq_stream_ref_from_u64(c->ref._v + 8), 0, 0);
    c->request_id = 2;
    c->last_handle = 0;
    int slot = nss_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { c->request_id = saved_id; moq_session_destroy(s); return; }
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, c->ref, false), (int)MOQ_OK);
    {
        txs_norm_vec_t d;
        txs_norm_init(&d);
        collect_events(s, &f->hooks, &d);
        txs_norm_free(&d);
    }
    nso_snap_t valid;
    nso_read(s, slot, &valid);
    MOQ_TEST_CHECK(valid.invalid == 0);
    moq_ns_sub_entry_t *e = &s->ns_subs[slot];

    /* Both out-of-range directions, through the comparator as well. */
    nso_selfcheck_invalid(s, (int)s->ns_sub_cap, &valid, "ns slot past the pool");
    nso_selfcheck_invalid(s, -1, &valid, "ns negative slot");

    size_t         save_pc  = e->prefix_count;
    moq_bytes_t   *save_pp  = e->prefix_parts;
    uint8_t       *save_pb  = e->prefix_buf;
    size_t         save_pbl = e->prefix_buf_len;
    const uint8_t *save_d0  = save_pp ? save_pp[0].data : NULL;
    const uint8_t *save_d1  = (save_pp && save_pc > 1) ? save_pp[1].data : NULL;
    size_t         save_rl  = e->recv_len;
    uint8_t       *save_rb  = e->recv_buf;

    e->prefix_count = NSO_MAX_PARTS + 1;
    nso_selfcheck_invalid(s, slot, &valid, "ns excessive prefix count");
    e->prefix_count = save_pc;

    e->prefix_parts = NULL;
    nso_selfcheck_invalid(s, slot, &valid, "ns NULL parts with nonzero count");
    e->prefix_parts = save_pp;

    if (save_pp && save_pc > 0) {
        e->prefix_parts[0].data = NULL;
        nso_selfcheck_invalid(s, slot, &valid, "ns NULL nonempty part data");
        e->prefix_parts[0].data = save_d0;
    }

    e->prefix_buf = NULL;
    nso_selfcheck_invalid(s, slot, &valid, "ns NULL prefix buffer");
    e->prefix_buf = save_pb;

    if (save_pp && save_pc > 1 && save_pb) {
        e->prefix_parts[1].data = save_pb;      /* offset 0, still in-bounds */
        nso_selfcheck_invalid(s, slot, &valid, "ns wrong in-bounds offset");
        e->prefix_parts[1].data = save_d1;
    }

    e->prefix_buf_len = 1;
    nso_selfcheck_invalid(s, slot, &valid, "ns prefix extent too short");
    e->prefix_buf_len = save_pbl + 1;
    nso_selfcheck_invalid(s, slot, &valid, "ns prefix extent too long");
    e->prefix_buf_len = save_pbl;

    e->recv_len = NSO_BUF_MAX + 1;
    nso_selfcheck_invalid(s, slot, &valid, "ns oversized retained length");
    e->recv_len = save_rl;

    e->recv_len = 4;
    e->recv_buf = NULL;
    nso_selfcheck_invalid(s, slot, &valid, "ns retained length, NULL buffer");
    e->recv_buf = save_rb;
    e->recv_len = save_rl;

    /* Every perturbation restored. */
    nso_snap_t back;
    nso_read(s, slot, &back);
    failures += nso_diff(&back, &valid, "ns inventory restored");
    c->request_id = saved_id;
    moq_session_destroy(s);
}


/* The complete retired postcondition for a P6 owner: production lookups, every
 * graph domain, no owner-targeting edge, and no output. Used after each
 * recovery branch AND after its second empty retry, so exactly-once is proven
 * rather than once-observed. */
static void nss_check_fully_retired(moq_session_t *s, const fin_owner_case_t *f,
                                    moq_stream_ref_t ref, int slot,
                                    uint64_t rid, const drain_snap_t *ring,
                                    const char *what)
{
    check_nss_retired(s, ref);
    og_graph_t g;
    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    failures += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_RID, rid, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
    failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_NAMESPACE_SUB,
                                            slot, what);
    drain_snap_t now;
    drain_snap(s, &now);
    failures += drain_multiset_equals(&now, ring, what);
    check_no_output(s, &f->hooks);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
}


/* PROFILE BOUNDARY. The marker is installed by the draft-18 request-stream
 * handoff ONLY. `ns_sub_process_recving_publisher()` is shared: the draft-16
 * bidi router reaches the same commit by creating the entry directly and
 * driving it through `handle_bidi_stream_bytes()`, which is NOT a handoff and
 * must never leave transitional FIN ownership behind.
 *
 * A full draft-16 session is not needed to discriminate that, and building one
 * here would test the setup rather than the boundary: driving the same shared
 * commit through the DIRECT creation route with a wire FIN observes the marker
 * at the one point it is visible. The action queue is filled first so the
 * reciprocal close refuses and the committed entry survives to be read. */
static void run_nss_direct_route_control(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session_full(MOQ_PERSPECTIVE_SERVER, 0, 1);
    nss_ctx_t *c = (nss_ctx_t *)f->ctx;
    uint64_t saved_id = c->request_id;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(c->ref._v + 24);
    moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x99);

    /* Encode the request, then create the owner through the DIRECT route --
     * never through the draft-18 request-stream handoff. */
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("direct") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    uint8_t m[192];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, sizeof(m));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe_namespace(&w, 0, &ns, &mp), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)ns_sub_on_new_bidi(s, ref, m, moq_buf_writer_offset(&w)),
        (int)MOQ_OK);
    int slot = (int)moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { c->request_id = saved_id; moq_session_destroy(s); return; }
    /* Nothing is owed yet, and nothing was installed by the creation. */
    MOQ_TEST_CHECK_EQ_INT(s->ns_subs[slot].handoff_fin_pending, 0);

    MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker), (int)MOQ_OK);
    MOQ_TEST_CHECK(action_queue_full(s));

    /* The wire FIN drives the shared commit through the direct route. The
     * reciprocal close refuses on action capacity, so the committed entry is
     * still readable -- and it must carry NO transitional FIN ownership: this
     * call's FIN is the wire's, not a handed-over one. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)handle_bidi_stream_bytes(s, ref, NULL, 0, true),
        (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[slot].state,
                          (int)MOQ_NS_SUB_PENDING_PUBLISHER);
    MOQ_TEST_CHECK_EQ_INT(s->ns_subs[slot].handoff_fin_pending, 0);
    MOQ_TEST_CHECK_EQ_INT(s->ns_subs[slot].pending_fin, 0);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    c->request_id = saved_id;
    moq_session_destroy(s);
}


static void run_nss_case(const fin_owner_case_t *f, bool fin_in_request,
                         bool fill_ring, bool reject_now)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    nss_ctx_t *c = (nss_ctx_t *)f->ctx;
    moq_stream_ref_t ref = c->ref;
    uint64_t saved_id = c->request_id;
    nss_burn_request(s, moq_stream_ref_from_u64(ref._v + 8), 0, 0);
    c->request_id = 2;               /* the next legal inbound ordinal */
    uint64_t rid = c->request_id;
    c->last_handle = 0;

    /* Identity declared from pool state BEFORE ingress. */
    int slot = nss_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { c->request_id = saved_id; moq_session_destroy(s); return; }
    uint32_t gen = s->ns_subs[slot].generation | 1u;
    uint64_t handle = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                      s->session_tag, gen, (uint32_t)slot);
    /* Co-allocated and preserved across reuse: declared from the FREE slot. */
    const void *slot_recv_buf = (const void *)s->ns_subs[slot].recv_buf;
    size_t slot_recv_cap = s->ns_subs[slot].recv_cap;
    txs_snapshot_t pre_feed;
    txs_capture(s, &ref, 1, &pre_feed);

    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, ref, fin_in_request),
                          (int)MOQ_OK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_nss_request(&want, c->last_handle);
    failures += txs_norm_equals(&got, &want, f->name);
    txs_norm_free(&got);
    txs_norm_free(&want);

    if (fin_in_request) {
        /* The owner is retired inside the feed, so the surfaced handle is
         * checked against the PRE-DERIVED one -- `c->last_handle` must not
         * authorize its own expectation. */
        MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle);
        /* The peer's graceful cancel is answered by closing our send half,
         * and the entry is reclaimed. */
        txs_norm_vec_t acts, wacts;
        txs_norm_init(&acts);
        txs_norm_init(&wacts);
        collect_actions(s, &f->hooks, &acts);
        want_close_bidi(&wacts, ref);
        failures += txs_norm_equals(&acts, &wacts, f->name);
        txs_norm_free(&acts);
        txs_norm_free(&wacts);
        check_nss_retired(s, ref);
        {   /* Nothing of the retired owner survives in any domain. */
            og_graph_t g;
            og_capture(s, &g);
            failures += og_check_integrity(&g, "nss immediate retired");
            failures += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v,
                                         "nss immediate retired");
            failures += og_check_no_edge(&g, OG_DOM_REQ_RID, rid,
                                         "nss immediate retired");
            failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v,
                                         "nss immediate retired");
            failures += og_check_owner_unreferenced(&g,
                (int)MOQ_REQ_NAMESPACE_SUB, slot, "nss immediate retired");
        }
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
        check_no_output(s, &f->hooks);
        c->request_id = saved_id;
        moq_session_destroy(s);
        return;
    }

    failures += nss_owner_present_problems(s, ref, f->owner_kind, f->owner_role, f->owner_state);
    nss_hook_state_t owner_before;
    f->hooks.capture(s, f->ctx, &owner_before);
    MOQ_TEST_CHECK_EQ_U64(owner_before.handle, handle);
    MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle);
    MOQ_TEST_CHECK_EQ_INT(owner_before.pending_fin, 0);
    MOQ_TEST_CHECK_EQ_INT(owner_before.handoff_fin, 0);
    check_no_output(s, &f->hooks);

    /* The whole declared owner inventory, the session delta of the handoff, and
     * the owner's ABSOLUTE edge set -- validated before any graph derived from
     * it becomes a conservation baseline. */
    nso_snap_t nso_want, nso_have;
    nss_declare(&nso_want, slot, gen, handle, rid, ref._v, 0,
                c->last_req, c->last_req_len, slot_recv_buf, slot_recv_cap);
    nso_read(s, slot, &nso_have);
    failures += nso_diff(&nso_have, &nso_want, "nss committed owner");
    {
        txs_snapshot_t observed, want_feed = pre_feed;
        txs_capture(s, &ref, 1, &observed);
        want_feed.event_scratch_len = observed.event_scratch_len;  /* adopted */
        want_feed.owners[0].kind = TXS_OWNER_NS;
        want_feed.owners[0].slot = slot;
        want_feed.owners[0].state = (int)MOQ_NS_SUB_PENDING_PUBLISHER;
        want_feed.owners[0].generation = gen;
        want_feed.owners[0].invalid = 0;
        failures += txs_check_eq(s, &ref, 1, &want_feed, "nss handoff delta");
    }
    og_graph_t live_graph;
    og_capture(s, &live_graph);
    failures += og_check_integrity(&live_graph, "nss live");
    failures += nss_check_owner_edges(&live_graph, slot, ref, rid, "nss live");

    if (fill_ring) fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    if (!reject_now) {
        c->request_id = saved_id;
        moq_session_destroy(s);
        return;
    }

    moq_result_t want_rc = fill_ring ? MOQ_ERR_WOULD_BLOCK : MOQ_OK;
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, c->last_handle),
                          (int)want_rc);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        failures += txs_check_eq(s, &ref, 1, &before, f->name);
        failures += f->hooks.check(s, f->ctx, &owner_before);
        failures += nss_owner_present_problems(
            s, ref, f->owner_kind, f->owner_role, f->owner_state);
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before, "nss blocked");
        nso_read(s, slot, &nso_have);
        failures += nso_diff(&nso_have, &nso_want, "nss blocked owner");
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss blocked");
        failures += nss_check_owner_edges(&g, slot, ref, rid, "nss blocked");
        failures += og_check_same_topology(&g, &live_graph, "nss blocked");
        check_no_output(s, &f->hooks);
        c->request_id = saved_id;
        moq_session_destroy(s);
        return;
    }

    drain_snap_t now, want_ring;
    drain_snap(s, &now);
    drain_snap_plus(&ring_before, ref, MOQ_DRAIN_NORMAL, &want_ring);
    failures += drain_multiset_equals(&now, &want_ring, "nss granted");
    check_nss_retired(s, ref);
    check_reject_only_output(s, &f->hooks, ref, f->name);

    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
        (int)MOQ_OK);
    check_no_output(s, &f->hooks);
    drain_snap_t after;
    drain_snap(s, &after);
    failures += drain_multiset_equals(&after, &ring_before, "nss released");
    check_nss_retired(s, ref);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    c->request_id = saved_id;
    moq_session_destroy(s);
}


/* Same-call FIN with no action slot for the reciprocal close. The pressure is
 * applied directly -- a benign filler close on an unrelated stream -- so the
 * fixture creates no second protocol owner and cannot terminate the session.
 * `then_reject` answers inside the blocked window instead of retrying. */
static void run_nss_action_blocked(const fin_owner_case_t *f, bool then_reject)
{
    moq_session_t *s = make_session_full(MOQ_PERSPECTIVE_SERVER, 0, 1);
    nss_ctx_t *c = (nss_ctx_t *)f->ctx;
    moq_stream_ref_t ref = c->ref;
    moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x77);
    uint64_t saved_id = c->request_id;

    /* The staging cleanup's unqualified remove-by-id(0) would destroy the
     * pending request-id edge at ordinal zero, so the
     * measured owner uses a nonzero id and its topology asserts absolutely. */
    nss_burn_request(s, moq_stream_ref_from_u64(ref._v + 8), 0, 0);
    c->request_id = 2;
    uint64_t rid = c->request_id;
    c->last_handle = 0;

    int slot = nss_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { c->request_id = saved_id; moq_session_destroy(s); return; }
    uint32_t gen = s->ns_subs[slot].generation | 1u;
    uint64_t handle = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                      s->session_tag, gen, (uint32_t)slot);
    const void *slot_recv_buf = (const void *)s->ns_subs[slot].recv_buf;
    size_t slot_recv_cap = s->ns_subs[slot].recv_cap;

    MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker), (int)MOQ_OK);
    MOQ_TEST_CHECK(action_queue_full(s));
    txs_snapshot_t pre_feed;
    txs_capture(s, &ref, 1, &pre_feed);

    /* The ring as it stands BEFORE the feed: a blocked call that wrongly took
     * a reference must not be able to install itself as the baseline. */
    drain_snap_t ring_pre_feed;
    drain_snap(s, &ring_pre_feed);

    /* No room for the reciprocal close. */
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, ref, true),
                          (int)MOQ_ERR_WOULD_BLOCK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_nss_request(&want, c->last_handle);
    failures += txs_norm_equals(&got, &want, f->name);
    txs_norm_free(&got);
    txs_norm_free(&want);

    /* Nothing was drained by the blocked call itself. */
    {
        drain_snap_t after_feed;
        drain_snap(s, &after_feed);
        failures += drain_multiset_equals(&after_feed, &ring_pre_feed,
                                          "nss blocked feed");
    }

    /* The owner survives, and the observed close is held by the new marker --
     * which is distinct from pending_fin, so that field stays clear. */
    failures += nss_owner_present_problems(s, ref, f->owner_kind, f->owner_role,
                                           f->owner_state);
    nss_hook_state_t owner_before;
    f->hooks.capture(s, f->ctx, &owner_before);
    MOQ_TEST_CHECK_EQ_U64(owner_before.handle, handle);
    MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle);
    MOQ_TEST_CHECK_EQ_INT(owner_before.pending_fin, 0);
    /* The observed close is transitional ownership, NOT the durable latch. */
    MOQ_TEST_CHECK_EQ_INT(owner_before.handoff_fin, 1);

    /* The declared owner inventory, the declared session delta of the refused
     * handoff, and the owner's ABSOLUTE edge set -- validated before any graph
     * derived from it becomes a conservation baseline. */
    nso_snap_t nso_want, nso_have;
    nss_declare(&nso_want, slot, gen, handle, rid, ref._v, 1,
                c->last_req, c->last_req_len, slot_recv_buf, slot_recv_cap);
    nso_read(s, slot, &nso_have);
    failures += nso_diff(&nso_have, &nso_want, "nss action-blocked owner");
    {
        txs_snapshot_t observed, want_feed = pre_feed;
        txs_capture(s, &ref, 1, &observed);
        want_feed.event_scratch_len = observed.event_scratch_len;  /* adopted */
        /* `pre_feed` was captured AFTER the blocker was queued, so the unchanged
         * action depth is already DECLARED by that snapshot -- adopting the
         * observed value would have made the one thing this row cares about
         * unfalsifiable. */
        want_feed.owners[0].kind = TXS_OWNER_NS;
        want_feed.owners[0].slot = slot;
        want_feed.owners[0].state = (int)MOQ_NS_SUB_PENDING_PUBLISHER;
        want_feed.owners[0].generation = gen;
        want_feed.owners[0].invalid = 0;
        failures += txs_check_eq(s, &ref, 1, &want_feed,
                                 "nss action-blocked delta");
    }
    nsc_snap_t ab_alloc;
    nsc_read(s, slot, &ab_alloc);           /* after the absolute check above */
    og_graph_t live_graph;
    og_capture(s, &live_graph);
    failures += og_check_integrity(&live_graph, "nss action-blocked live");
    failures += nss_check_owner_edges(&live_graph, slot, ref, rid,
                                      "nss action-blocked live");

    /* Exactly the filler, on the unrelated stream: polling it frees the slot,
     * and the ref proves which close was queued. */
    txs_norm_vec_t acts, wacts;
    txs_norm_init(&acts);
    txs_norm_init(&wacts);
    collect_actions(s, &f->hooks, &acts);
    want_close_bidi(&wacts, blocker);
    failures += txs_norm_equals(&acts, &wacts, f->name);
    txs_norm_free(&acts);
    txs_norm_free(&wacts);

    /* After the filler release, before the retry: the owner and its exact
     * topology are unchanged -- the release must not have disturbed them. */
    nso_read(s, slot, &nso_have);
    failures += nso_diff(&nso_have, &nso_want, "nss action-blocked released");
    {
        nsc_snap_t now_alloc;
        nsc_read(s, slot, &now_alloc);
        failures += nsc_same(&now_alloc, &ab_alloc,
                             "nss action-blocked alloc");
    }
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss action-blocked released");
        failures += nss_check_owner_edges(&g, slot, ref, rid,
                                          "nss action-blocked released");
        failures += og_check_same_topology(&g, &live_graph,
                                           "nss action-blocked released");
    }

    if (!then_reject) {
        /* An empty refeed completes the deferred close exactly once, for the
         * request's own stream. The expectation is the PRE-FEED ring. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        txs_norm_vec_t a2, w2;
        txs_norm_init(&a2);
        txs_norm_init(&w2);
        collect_actions(s, &f->hooks, &a2);
        want_close_bidi(&w2, ref);
        failures += txs_norm_equals(&a2, &w2, f->name);
        txs_norm_free(&a2);
        txs_norm_free(&w2);
        nss_check_fully_retired(s, f, ref, slot, rid, &ring_pre_feed,
                                "nss retry retired");
        /* A second empty retry resurrects nothing and queues no duplicate. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        nss_check_fully_retired(s, f, ref, slot, rid, &ring_pre_feed,
                                "nss retry idempotent");
        c->request_id = saved_id;
        moq_session_destroy(s);
        return;
    }

    /* The application answers inside the blocked window, against an exhausted
     * ring: the observed close means no drain reference is needed. */
    fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, c->last_handle),
                          (int)MOQ_OK);
    drain_snap_t now;
    drain_snap(s, &now);
    failures += drain_multiset_equals(&now, &ring_before, "nss window reject");
    check_reject_only_output(s, &f->hooks, ref, f->name);
    /* The exact rejection output is collected above; retirement is then proven
     * in every domain, and repeated after a second empty retry. */
    nss_check_fully_retired(s, f, ref, slot, rid, &ring_before,
                            "nss window reject retired");
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
        (int)MOQ_OK);
    nss_check_fully_retired(s, f, ref, slot, rid, &ring_before,
                            "nss window reject idempotent");
    c->request_id = saved_id;
    moq_session_destroy(s);
}


/* Same-call FIN against a full drain ring -- the paired half of the no-FIN
 * discriminator. The reciprocal close needs no drain reference, so an
 * exhausted ring cannot refuse it. */
static void run_nss_fin_full_ring(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    nss_ctx_t *c = (nss_ctx_t *)f->ctx;
    moq_stream_ref_t ref = c->ref;
    uint64_t saved_id = c->request_id;
    nss_burn_request(s, moq_stream_ref_from_u64(ref._v + 8), 0, 0);
    c->request_id = 2;
    uint64_t rid = c->request_id;
    c->last_handle = 0;
    int slot = nss_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { c->request_id = saved_id; moq_session_destroy(s); return; }
    uint32_t gen = s->ns_subs[slot].generation | 1u;
    uint64_t handle = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                      s->session_tag, gen, (uint32_t)slot);
    fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);

    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, ref, true), (int)MOQ_OK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_nss_request(&want, c->last_handle);
    failures += txs_norm_equals(&got, &want, f->name);
    txs_norm_free(&got);
    txs_norm_free(&want);

    txs_norm_vec_t acts, wacts;
    txs_norm_init(&acts);
    txs_norm_init(&wacts);
    collect_actions(s, &f->hooks, &acts);
    want_close_bidi(&wacts, ref);
    failures += txs_norm_equals(&acts, &wacts, f->name);
    txs_norm_free(&acts);
    txs_norm_free(&wacts);

    drain_snap_t now;
    drain_snap(s, &now);
    failures += drain_multiset_equals(&now, &ring_before, "nss fin full ring");
    /* The owner is retired inside the feed, so the surfaced handle is checked
     * against the PRE-DERIVED one, and nothing of it survives in any domain. */
    MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle);
    check_nss_retired(s, ref);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss full-ring retired");
        failures += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v,
                                     "nss full-ring retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_RID, rid,
                                     "nss full-ring retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v,
                                     "nss full-ring retired");
        failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_NAMESPACE_SUB,
                                                slot, "nss full-ring retired");
    }
    check_no_output(s, &f->hooks);
    c->request_id = saved_id;
    moq_session_destroy(s);
}


/* The PRE-COMMIT carrier: the event queue is full when the request arrives, so
 * `ns_sub_process_recving_publisher()` refuses BEFORE its commit -- yet the
 * draft-18 handoff still retires the generic staging subscription, because the
 * namespace-sub owner already owns the bidi and the request bytes. The FIN must
 * therefore reach that owner at the OWNERSHIP TRANSITION, not at the commit;
 * otherwise the empty re-feed commits with no FIN fact, queues no reciprocal
 * close and leaves the owner alive.
 *
 * The filler is a declared second SUBSCRIBE_NAMESPACE on its own ref: it is a
 * real, classified owner, so every count below names both. */
static void run_nss_event_blocked(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 1);
    nss_ctx_t *c = (nss_ctx_t *)f->ctx;
    uint64_t saved_id = c->request_id;
    moq_stream_ref_t saved_ref = c->ref;
    moq_stream_ref_t target = c->ref;
    moq_stream_ref_t filler = moq_stream_ref_from_u64(target._v + 16);

    /* The burn will take the first free slot. Everything the residue oracle
     * compares is derived from that slot's state BEFORE the burn, so the
     * product's own post-burn fields cannot define both sides of their
     * expectation -- in particular the claim that the free ADVANCED the
     * generation is only meaningful against a pre-derived value. */
    int burn_slot = nss_declare_free_slot(s);
    MOQ_TEST_CHECK(burn_slot >= 0);
    if (burn_slot < 0) { c->request_id = saved_id; moq_session_destroy(s);
                         return; }
    MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[burn_slot].state,
                          (int)MOQ_NS_SUB_FREE);
    uint32_t burn_pre_gen = s->ns_subs[burn_slot].generation;
    uint32_t burn_stale_gen = burn_pre_gen | 1u;        /* its live generation */
    uint32_t burn_free_gen = burn_stale_gen + 1u;       /* after the free */
    uint64_t burn_stale_handle =
        moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB, s->session_tag,
                        burn_stale_gen, (uint32_t)burn_slot);
    MOQ_TEST_CHECK(burn_stale_handle != 0);

    nss_burn_request(s, moq_stream_ref_from_u64(target._v + 8), 0, 0);

    /* The freed slot's residue, compared against those PRE-DERIVED values.
     * `ns_sub_free_entry()` advances the generation but does NOT clear
     * `handle`, so the slot is FREE while still carrying the packed handle of
     * the owner that has just gone. */
    MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[burn_slot].state,
                          (int)MOQ_NS_SUB_FREE);
    MOQ_TEST_CHECK_EQ_U64(s->ns_subs[burn_slot].generation, burn_free_gen);
    MOQ_TEST_CHECK_EQ_U64(s->ns_subs[burn_slot].handle._opaque,
                          burn_stale_handle);
    MOQ_TEST_CHECK_EQ_INT(ns_busy_count(s), 0);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss residue source");
        failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_NAMESPACE_SUB,
                                                burn_slot,
                                                "nss residue source");
    }

    /* The declared filler occupies the single event slot and is left unpolled,
     * so its handle is DERIVED before ingress rather than read from an event
     * this row must not drain. */
    int filler_slot = nss_declare_free_slot(s);
    MOQ_TEST_CHECK(filler_slot >= 0);
    if (filler_slot < 0) { c->request_id = saved_id; moq_session_destroy(s);
                           return; }
    uint32_t filler_gen = s->ns_subs[filler_slot].generation | 1u;
    uint64_t filler_handle = moq_handle_pack(
        MOQ_HANDLE_POOL_NAMESPACE_SUB, s->session_tag, filler_gen,
        (uint32_t)filler_slot);
    const void *filler_pre_buf = (const void *)s->ns_subs[filler_slot].recv_buf;
    size_t filler_pre_cap = s->ns_subs[filler_slot].recv_cap;
    uint8_t filler_req[192];
    size_t filler_req_len = 0;
    {   /* A DISJOINT prefix: the family feed's ("live","eu") would overlap the
         * target and be rejected under the independent-overlap rule instead of
         * blocking on event capacity, which is not what this row measures. */
        moq_bytes_t fp[] = { MOQ_BYTES_LITERAL("fill") };
        moq_namespace_t fns = { fp, 1 };
        moq_d18_msg_params_t fmp;
        memset(&fmp, 0, sizeof(fmp));
        uint8_t fm[192];
        moq_buf_writer_t fw;
        moq_buf_writer_init(&fw, fm, sizeof(fm));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe_namespace(&fw, 2, &fns, &fmp),
            (int)MOQ_OK);
        filler_req_len = moq_buf_writer_offset(&fw);
        memcpy(filler_req, fm, filler_req_len);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, filler, fm,
                                                  filler_req_len, false, 1),
            (int)MOQ_OK);
    }
    MOQ_TEST_CHECK(event_queue_full(s));

    /* The target lands on the OTHER, virgin slot -- first-free had to leave the
     * filler where it is. Reproduce on it exactly the residue the burn proved
     * a real free leaves behind: the same free generation, and the stale handle
     * of the previous generation repacked for this slot. Fixture setup only --
     * the relationship was established through the real public lifecycle above,
     * and nothing about the target's own ingress, refusal, retry, output or
     * retirement is bypassed. */
    int slot = nss_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { c->ref = saved_ref; c->request_id = saved_id;
                    moq_session_destroy(s); return; }
    /* The allocation assumptions this construction rests on, asserted rather
     * than described: the filler DID reuse the burned slot, and the target is a
     * different, untouched slot still at its initial generation with no
     * handle. */
    MOQ_TEST_CHECK_EQ_INT(filler_slot, burn_slot);
    MOQ_TEST_CHECK(slot != filler_slot);
    MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[slot].state, (int)MOQ_NS_SUB_FREE);
    MOQ_TEST_CHECK_EQ_U64(s->ns_subs[slot].generation, 0);
    MOQ_TEST_CHECK_EQ_U64(s->ns_subs[slot].handle._opaque, 0);
    {
        int busy_before = ns_busy_count(s);
        size_t ev_before = s->event_tail - s->event_head;
        og_graph_t before_seed;
        og_capture(s, &before_seed);
        failures += og_check_integrity(&before_seed, "nss residue pre-seed");

        /* Exactly two fields, both PRE-DERIVED above. */
        s->ns_subs[slot].generation = burn_free_gen;
        s->ns_subs[slot].handle._opaque =
            moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB, s->session_tag,
                            burn_stale_gen, (uint32_t)slot);

        /* The seeding itself is inert: still FREE, no owner, no queue movement,
         * the whole topology conserved, and literally no edge to the target's
         * slot, ref or id in ANY of the three domains. */
        MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[slot].state,
                              (int)MOQ_NS_SUB_FREE);
        MOQ_TEST_CHECK_EQ_INT(ns_busy_count(s), busy_before);
        MOQ_TEST_CHECK_EQ_SIZE(s->event_tail - s->event_head, ev_before);
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss residue seeded");
        failures += og_check_same_topology(&g, &before_seed,
                                           "nss residue seeded");
        failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_NAMESPACE_SUB,
                                                slot, "nss residue seeded");
        failures += og_check_no_edge(&g, OG_DOM_NS_REF, target._v,
                                     "nss residue seeded");
        failures += og_check_no_edge(&g, OG_DOM_REQ_RID, 4,
                                     "nss residue seeded");
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, target._v,
                                     "nss residue seeded");
        failures += nss_check_owner_edges(&g, filler_slot, filler, 2,
                                          "nss residue seeded");
    }

    /* Pre-commit: `ns_sub_on_new_bidi()` mints nothing, so the refused
     * RECVING_PUBLISHER owner must still carry the slot's own STALE handle. */
    uint64_t target_pre_handle = s->ns_subs[slot].handle._opaque;
    uint32_t gen = s->ns_subs[slot].generation | 1u;
    /* Commit: the handle the commit must mint and surface. */
    uint64_t handle = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                      s->session_tag, gen, (uint32_t)slot);
    MOQ_TEST_CHECK(target_pre_handle != 0);
    MOQ_TEST_CHECK_EQ_U64(target_pre_handle,
                          moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                          s->session_tag, burn_stale_gen,
                                          (uint32_t)slot));
    MOQ_TEST_CHECK(target_pre_handle != handle);
    /* The filler is a real committed owner, and its expected record is BUILT
     * from the fixture -- request id 2, its own ref, the one-part "fill" prefix
     * of extent 4, no tokens -- never read back from the setup that produced
     * it. */
    nso_snap_t filler_want, filler_have;
    memset(&filler_want, 0, sizeof(filler_want));
    filler_want.present = 1;
    filler_want.state = (int)MOQ_NS_SUB_PENDING_PUBLISHER;
    filler_want.stream_kind = (int)MOQ_STREAM_KIND_NAMESPACE_SUB;
    filler_want.generation = filler_gen;
    filler_want.handle = filler_handle;
    filler_want.request_id = 2;
    filler_want.stream_ref = filler._v;
    filler_want.ep_kind = (int)MOQ_REQ_NAMESPACE_SUB;
    filler_want.ep_slot = filler_slot;
    filler_want.ep_has_request_id = 1;
    filler_want.ep_request_id = 2;
    filler_want.ep_has_stream_ref = 1;
    filler_want.ep_stream_ref = filler._v;
    filler_want.interest = (int)MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    filler_want.forward = 1;
    filler_want.parse_complete = 1;
    filler_want.auth_processed = 1;
    filler_want.auth_committed = 1;
    filler_want.prefix_valid = 1;
    filler_want.prefix_count = 1;
    filler_want.prefix_buf_len = 4;
    filler_want.prefix[filler_want.prefix_len++] = 4;
    memcpy(filler_want.prefix + filler_want.prefix_len, "fill", 4);
    filler_want.prefix_len += 4;
    filler_want.recv_buf = filler_pre_buf;
    filler_want.recv_cap = filler_pre_cap;
    filler_want.recv_len = filler_req_len;
    memcpy(filler_want.recv, filler_req, filler_req_len);
    nso_read(s, filler_slot, &filler_have);
    failures += nso_diff(&filler_have, &filler_want, "nss filler committed");
    /* Allocation identity, conserved from here on. */
    nsc_snap_t filler_alloc, tgt_alloc, alloc_now;
    nsc_read(s, filler_slot, &filler_alloc);
    og_graph_t filler_graph;
    og_capture(s, &filler_graph);
    failures += og_check_integrity(&filler_graph, "nss filler committed");
    failures += nss_check_owner_edges(&filler_graph, filler_slot, filler, 2,
                                      "nss filler committed");

    c->ref = target;
    c->request_id = 4;
    c->last_handle = 0;
    const void *slot_recv_buf = (const void *)s->ns_subs[slot].recv_buf;
    size_t slot_recv_cap = s->ns_subs[slot].recv_cap;
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    moq_stream_ref_t both[2] = { target, filler };
    txs_snapshot_t pre_feed;
    txs_capture(s, both, 2, &pre_feed);

    /* Refused solely because the one event slot is taken. */
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, target, true),
                          (int)MOQ_ERR_WOULD_BLOCK);

    /* The staging subscription and its stream-ref key are gone; exactly one
     * RECVING_PUBLISHER owner holds the bytes, its real namespace-ref edge and
     * the transitional FIN -- with `pending_fin` still clear. */
    {   /* the staging subscription is retired: no live sub entry at all */
        int live_subs = 0;
        for (size_t i = 0; i < s->sub_cap; i++)
            if (s->subs[i].state != MOQ_SUB_FREE) live_subs++;
        MOQ_TEST_CHECK_EQ_INT(live_subs, 0);
    }
    MOQ_TEST_CHECK_EQ_INT(ns_busy_count(s), 2);   /* filler + target */
    /* The DECLARED pre-commit inventory. The request is buffered, parsed and
     * request-id-validated but NOT committed: the validation stores the request
     * id and endpoint on the entry for the retry, so those ARE present, while
     * the auth commitment waits for the commit. The handle is the NONZERO STALE
     * one declared on the free slot -- `ns_sub_on_new_bidi()` mints nothing, so
     * the refused owner still carries it; the commit later replaces it with the
     * independently packed live handle. Standing omissions are those of
     * `nso_snap_t` itself -- resolved-token storage beyond its count, and the
     * auth transaction's overlay. */
    nso_snap_t tgt_want, tgt_have;
    memset(&tgt_want, 0, sizeof(tgt_want));
    tgt_want.present = 1;
    tgt_want.state = (int)MOQ_NS_SUB_RECVING_PUBLISHER;
    tgt_want.stream_kind = (int)MOQ_STREAM_KIND_NAMESPACE_SUB;
    tgt_want.generation = gen;
    tgt_want.handle = target_pre_handle;   /* pre-commit: the slot's own value */
    tgt_want.stream_ref = target._v;
    /* The request-id validation runs BEFORE the event refusal and stores the
     * endpoint on the entry for the retry, so these ARE part of the pre-commit
     * record; only the handle and the auth commitment wait for the commit. */
    tgt_want.request_id = 4;
    tgt_want.ep_kind = (int)MOQ_REQ_NAMESPACE_SUB;
    tgt_want.ep_slot = slot;
    tgt_want.ep_has_request_id = 1;
    tgt_want.ep_request_id = 4;
    tgt_want.ep_has_stream_ref = 1;
    tgt_want.ep_stream_ref = target._v;
    tgt_want.interest = (int)MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    tgt_want.forward = 1;
    tgt_want.parse_complete = 1;
    tgt_want.auth_processed = 1;
    tgt_want.token_count = 1;
    tgt_want.prefix_valid = 1;
    tgt_want.prefix_count = 2;
    tgt_want.prefix_buf_len = 6;
    tgt_want.prefix[tgt_want.prefix_len++] = 4;
    memcpy(tgt_want.prefix + tgt_want.prefix_len, "live", 4);
    tgt_want.prefix_len += 4;
    tgt_want.prefix[tgt_want.prefix_len++] = 2;
    memcpy(tgt_want.prefix + tgt_want.prefix_len, "eu", 2);
    tgt_want.prefix_len += 2;
    tgt_want.handoff_fin = 1;
    tgt_want.recv_buf = slot_recv_buf;
    tgt_want.recv_cap = slot_recv_cap;
    tgt_want.recv_len = c->last_req_len;
    memcpy(tgt_want.recv, c->last_req, c->last_req_len);
    nso_read(s, slot, &tgt_have);
    failures += nso_diff(&tgt_have, &tgt_want, "nss event-blocked target");

    {   /* The target's EXACT pre-commit edge set: its namespace-ref edge only,
         * with no request-id key yet (the commit installs that) and no generic
         * stream-ref key (staging is already retired). */
        const og_edge_spec_t want_e[] = { { OG_DOM_NS_REF, target._v } };
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss event blocked");
        failures += og_check_owner_edges(&g, (int)MOQ_REQ_NAMESPACE_SUB, slot,
                                         want_e, 1, "nss event blocked");
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, target._v,
                                     "nss event blocked");
        failures += og_check_no_edge(&g, OG_DOM_REQ_RID, 4,
                                     "nss event blocked");
        /* The filler owner and its edges are untouched by the refusal. */
        failures += nss_check_owner_edges(&g, filler_slot, filler, 2,
                                          "nss event blocked filler edges");
    }
    nso_read(s, filler_slot, &filler_have);
    failures += nso_diff(&filler_have, &filler_want,
                         "nss event blocked filler owner");
    nsc_read(s, slot, &tgt_alloc);          /* after the absolute check above */
    nsc_read(s, filler_slot, &alloc_now);
    failures += nsc_same(&alloc_now, &filler_alloc,
                         "nss event blocked filler alloc");
    {   /* The declared whole-session delta over BOTH refs: the target's stream
         * acquires its namespace-sub owner and NOTHING else changes -- including
         * the event-scratch cursor. `event_queue_full()` returns before the
         * scratch-copy code, so a refused target cannot legitimately move it,
         * and the pre-feed value is compared unchanged. This is specific to the
         * event-capacity refusal; rows that successfully surface and poll an
         * event keep their documented scratch delta. */
        txs_snapshot_t want_feed = pre_feed;
        want_feed.owners[0].kind = TXS_OWNER_NS;
        want_feed.owners[0].slot = slot;
        want_feed.owners[0].state = (int)MOQ_NS_SUB_RECVING_PUBLISHER;
        want_feed.owners[0].generation = gen;
        want_feed.owners[0].invalid = 0;
        failures += txs_check_eq(s, both, 2, &want_feed,
                                 "nss event blocked delta");
    }
    {
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before,
                                          "nss event blocked");
    }
    /* Exactly the declared filler is queued -- the refused request emitted
     * nothing. */
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &f->hooks, &got);
        {   /* the filler's own declared image: one part, no tokens */
            txs_img_t im;
            txs_img_init(&im);
            txs_img_u64(&im, filler_handle);
            txs_img_u64(&im, (uint64_t)MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE);
            txs_img_u64(&im, 1);
            txs_img_u64(&im, 1);
            txs_img_bytes(&im, (const uint8_t *)"fill", 4);
            txs_img_u64(&im, 0);
            MOQ_TEST_CHECK(txs_norm_append_img(&want,
                MOQ_EVENT_NS_SUB_REQUEST, &im));
        }
        failures += txs_norm_equals(&got, &want, "nss event blocked filler");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }

    check_no_output(s, &f->hooks);

    /* Polling the filler is a legitimate queue transition; both owner records
     * and the exact topology must survive it unchanged. */
    nso_read(s, slot, &tgt_have);
    failures += nso_diff(&tgt_have, &tgt_want, "nss event blocked drained");
    nso_read(s, filler_slot, &filler_have);
    failures += nso_diff(&filler_have, &filler_want,
                         "nss event blocked filler drained");
    nsc_read(s, slot, &alloc_now);
    failures += nsc_same(&alloc_now, &tgt_alloc,
                         "nss event blocked target alloc");
    nsc_read(s, filler_slot, &alloc_now);
    failures += nsc_same(&alloc_now, &filler_alloc,
                         "nss event blocked filler alloc drained");
    {
        const og_edge_spec_t want_e[] = { { OG_DOM_NS_REF, target._v } };
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss event blocked drained");
        failures += og_check_owner_edges(&g, (int)MOQ_REQ_NAMESPACE_SUB, slot,
                                         want_e, 1, "nss event blocked drained");
        failures += nss_check_owner_edges(&g, filler_slot, filler, 2,
                                          "nss event blocked filler drained");
    }

    /* The empty re-feed commits the request AND closes reciprocally. */
    c->last_handle = 0;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, target, NULL, 0, false, 1),
        (int)MOQ_OK);
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &f->hooks, &got);
        want_nss_request(&want, handle);
        failures += txs_norm_equals(&got, &want, "nss event blocked retry");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle);
    {
        txs_norm_vec_t acts, wacts;
        txs_norm_init(&acts);
        txs_norm_init(&wacts);
        collect_actions(s, &f->hooks, &acts);
        want_close_bidi(&wacts, target);
        failures += txs_norm_equals(&acts, &wacts, "nss event blocked close");
        txs_norm_free(&acts);
        txs_norm_free(&wacts);
    }
    /* The target is retired in every domain; the filler is byte-for-byte and
     * edge-for-edge unchanged. Repeated after a second empty re-feed, so the
     * completion is exactly-once rather than once-observed. */
    for (int pass = 0; pass < 2; pass++) {
        if (pass == 1)
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, target, NULL, 0,
                                                      false, 1),
                (int)MOQ_OK);
        const char *what = pass == 0 ? "nss event blocked retired"
                                     : "nss event blocked idempotent";
        MOQ_TEST_CHECK(ns_by_ref(s, target) == NULL);
        MOQ_TEST_CHECK_EQ_INT(ns_busy_count(s), 1);
        MOQ_TEST_CHECK(ns_by_ref(s, filler) != NULL);
        nso_read(s, filler_slot, &filler_have);
        failures += nso_diff(&filler_have, &filler_want, what);
        nsc_read(s, filler_slot, &alloc_now);
        failures += nsc_same(&alloc_now, &filler_alloc, what);
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, what);
        failures += og_check_no_edge(&g, OG_DOM_NS_REF, target._v, what);
        failures += og_check_no_edge(&g, OG_DOM_REQ_RID, 4, what);
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, target._v, what);
        failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_NAMESPACE_SUB,
                                                slot, what);
        failures += nss_check_owner_edges(&g, filler_slot, filler, 2, what);
        /* The declared topology TRANSITION closes here: filler-only before the
         * target arrived, filler+target while it lived, and filler-only again
         * once it retired -- so `filler_graph` is the baseline, not a capture
         * taken for its own sake. */
        failures += og_check_same_topology(&g, &filler_graph, what);
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before, what);
        check_no_output(s, &f->hooks);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    }

    c->ref = saved_ref;
    c->request_id = saved_id;
    moq_session_destroy(s);
}


/* Clear-on-reuse for the namespace-sub pool. Its free is SELECTIVE, so the
 * marker's clearing is an explicit obligation rather than a memset by-product.
 * The blocked window makes the marker genuinely live at the boundary: the
 * reciprocal close is refused, the empty re-feed completes it, and the freed
 * record must carry neither FIN fact into its next owner. Both generations and
 * both packed handles are derived before ingress. */
static void run_nss_slot_reuse_case(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session_full(MOQ_PERSPECTIVE_SERVER, 0, 1);
    nss_ctx_t *c = (nss_ctx_t *)f->ctx;
    uint64_t saved_id = c->request_id;
    moq_stream_ref_t saved_ref = c->ref;
    moq_stream_ref_t first = c->ref;
    moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x77);

    nss_burn_request(s, moq_stream_ref_from_u64(first._v + 8), 0, 0);
    c->request_id = 2;
    c->last_handle = 0;

    int slot = nss_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { c->request_id = saved_id; moq_session_destroy(s); return; }
    uint32_t gen_first = s->ns_subs[slot].generation | 1u;
    uint32_t gen_reuse = (gen_first + 1u) | 1u;   /* the free advances once */
    uint64_t handle_first = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                            s->session_tag, gen_first,
                                            (uint32_t)slot);
    uint64_t handle_reuse = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                            s->session_tag, gen_reuse,
                                            (uint32_t)slot);
    MOQ_TEST_CHECK(handle_first != handle_reuse);
    const void *slot_recv_buf = (const void *)s->ns_subs[slot].recv_buf;
    size_t slot_recv_cap = s->ns_subs[slot].recv_cap;

    /* 1. SUBSCRIBE_NAMESPACE + FIN with no action slot: the reciprocal close is
     *    refused and the marker survives it. */
    MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker), (int)MOQ_OK);
    MOQ_TEST_CHECK(action_queue_full(s));
    txs_snapshot_t pre_first;
    txs_capture(s, &first, 1, &pre_first);
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, first, true),
                          (int)MOQ_ERR_WOULD_BLOCK);
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &f->hooks, &got);
        want_nss_request(&want, c->last_handle);
        failures += txs_norm_equals(&got, &want, "nss reuse first");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle_first);
    {   /* The retention feed's own declared session delta. `pre_first` was
         * captured for exactly this: leaving it uncompared would make an
         * unrelated mutation invisible in the proof built to catch one. */
        txs_snapshot_t observed, want_feed = pre_first;
        txs_capture(s, &first, 1, &observed);
        want_feed.event_scratch_len = observed.event_scratch_len;  /* adopted */
        want_feed.action_depth = observed.action_depth;            /* filler */
        want_feed.owners[0].kind = TXS_OWNER_NS;
        want_feed.owners[0].slot = slot;
        want_feed.owners[0].state = (int)MOQ_NS_SUB_PENDING_PUBLISHER;
        want_feed.owners[0].generation = gen_first;
        want_feed.owners[0].invalid = 0;
        failures += txs_check_eq(s, &first, 1, &want_feed,
                                 "nss reuse handoff delta");
    }
    {
        nso_snap_t w, h;
        nss_declare(&w, slot, gen_first, handle_first, 2, first._v, 1,
                    c->last_req, c->last_req_len, slot_recv_buf, slot_recv_cap);
        nso_read(s, slot, &h);
        failures += nso_diff(&h, &w, "nss reuse first owner");
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss reuse first");
        failures += nss_check_owner_edges(&g, slot, first, 2, "nss reuse first");
    }
    /* Drain the filler so the retry has a slot. */
    {
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    }

    /* 2. The empty re-feed completes the close exactly once and retires it. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, first, NULL, 0, false, 1),
        (int)MOQ_OK);
    {
        txs_norm_vec_t acts, wacts;
        txs_norm_init(&acts);
        txs_norm_init(&wacts);
        collect_actions(s, &f->hooks, &acts);
        want_close_bidi(&wacts, first);
        failures += txs_norm_equals(&acts, &wacts, "nss reuse retry");
        txs_norm_free(&acts);
        txs_norm_free(&wacts);
    }
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);

    /* 3. The freed record: generation advanced, BOTH FIN facts clear, and no
     *    edge anywhere still points at the retired owner. */
    MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[slot].state, (int)MOQ_NS_SUB_FREE);
    MOQ_TEST_CHECK_EQ_U64(s->ns_subs[slot].generation, gen_reuse - 1u);
    MOQ_TEST_CHECK_EQ_INT(s->ns_subs[slot].pending_fin, 0);
    MOQ_TEST_CHECK_EQ_INT(s->ns_subs[slot].handoff_fin_pending, 0);
    check_nss_retired(s, first);
    /* A second empty retry is silent, resurrects nothing and queues no
     * duplicate close -- exactly-once, not merely once-observed. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, first, NULL, 0, false, 1),
        (int)MOQ_OK);
    check_no_output(s, &f->hooks);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    MOQ_TEST_CHECK_EQ_INT((int)s->ns_subs[slot].state, (int)MOQ_NS_SUB_FREE);
    MOQ_TEST_CHECK_EQ_INT(s->ns_subs[slot].handoff_fin_pending, 0);
    check_nss_retired(s, first);
    {
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss reuse retired");
        failures += og_check_no_edge(&g, OG_DOM_NS_REF, first._v,
                                     "nss reuse retired");
        failures += og_check_no_edge(&g, OG_DOM_REQ_RID, 2,
                                     "nss reuse retired");
        failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_NAMESPACE_SUB,
                                                slot, "nss reuse retired");
    }

    /* 4. A distinct no-FIN request reuses that physical slot and holds neither
     *    FIN fact. */
    moq_stream_ref_t second = moq_stream_ref_from_u64(first._v + 4);
    c->ref = second;
    c->request_id = 4;
    c->last_handle = 0;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, second, false), (int)MOQ_OK);
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &f->hooks, &got);
        want_nss_request(&want, c->last_handle);
        failures += txs_norm_equals(&got, &want, "nss reuse second");
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle_reuse);
    og_graph_t graph_before;
    {
        nso_snap_t w, h;
        nss_declare(&w, slot, gen_reuse, handle_reuse, 4, second._v, 0,
                    c->last_req, c->last_req_len, slot_recv_buf, slot_recv_cap);
        nso_read(s, slot, &h);
        failures += nso_diff(&h, &w, "nss reuse second owner");
        og_capture(s, &graph_before);
        failures += og_check_integrity(&graph_before, "nss reuse live");
        failures += nss_check_owner_edges(&graph_before, slot, second, 4,
                                          "nss reuse live");
    }

    /* 5. Its peer half is genuinely open, so its rejection owes a drain
     *    reference and an exhausted ring refuses it transactionally. */
    fill_drain_ring(s);
    size_t filled = s->drain_ref_count;
    nss_hook_state_t owner;
    f->hooks.capture(s, f->ctx, &owner);
    txs_snapshot_t before;
    txs_capture(s, &second, 1, &before);
    expect_after_call_prepare(&before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_reuse),
                          (int)MOQ_ERR_WOULD_BLOCK);
    failures += txs_check_eq(s, &second, 1, &before, "nss reuse blocked");
    failures += f->hooks.check(s, f->ctx, &owner);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    {
        nso_snap_t w, h;
        nss_declare(&w, slot, gen_reuse, handle_reuse, 4, second._v, 0,
                    c->last_req, c->last_req_len, slot_recv_buf, slot_recv_cap);
        nso_read(s, slot, &h);
        failures += nso_diff(&h, &w, "nss reuse blocked owner");
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "nss reuse blocked");
        failures += nss_check_owner_edges(&g, slot, second, 4,
                                          "nss reuse blocked");
        failures += og_check_same_topology(&g, &graph_before,
                                           "nss reuse blocked");
    }
    check_no_output(s, &f->hooks);

    /* 6. The retired first-generation handle stays stale after the reuse. */
    txs_snapshot_t before_stale;
    txs_capture(s, &second, 1, &before_stale);
    expect_after_call_prepare(&before_stale);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_first),
                          (int)MOQ_ERR_STALE_HANDLE);
    failures += txs_check_eq(s, &second, 1, &before_stale,
                             "nss stale after reuse");
    failures += f->hooks.check(s, f->ctx, &owner);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    check_no_output(s, &f->hooks);

    c->ref = saved_ref;
    c->request_id = saved_id;
    moq_session_destroy(s);
}


void test_subscribe_namespace(void)
{
    fin_owner_case_t f = nss_family();
    MOQ_TEST_CHECK_EQ_INT(fin_owner_problems(&f), 0);
    run_nss_case(&f, false, false, true);   /* control: drain, then released */
    run_nss_case(&f, true,  false, false);  /* same-call FIN: reciprocal close */
    run_nss_case(&f, false, true,  true);   /* exhausted ring, no FIN */
    run_nss_fin_full_ring(&f);              /* its paired FIN half */
    run_nss_action_blocked(&f, false);      /* deferred close, then retry */
    run_nss_action_blocked(&f, true);       /* answered in the blocked window */
    nss_inventory_selfchecks(&f);
    run_nss_direct_route_control(&f);       /* the profile boundary */
    run_nss_event_blocked(&f);              /* the PRE-COMMIT carrier */
    /* The observed close does not follow the slot to its next owner. */
    run_nss_slot_reuse_case(&f);

    /* The role-less family must declare the sentinel, and only it. */
    {
        fin_owner_case_t bad = f;
        bad.owner_role = (int)MOQ_SUB_ROLE_PUBLISHER;
        MOQ_TEST_CHECK(fin_owner_problems(&bad) > 0);
        fin_owner_case_t bad2 = pub_family();
        bad2.owner_role = FIN_OWNER_ROLE_NA;
        MOQ_TEST_CHECK(fin_owner_problems(&bad2) > 0);
    }
}
