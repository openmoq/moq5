#include "test_cases.h"

typedef struct st_hook_state {
    int      present;
    uint64_t handle;
    uint64_t req_stream_ref;
    struct {
        int recv_fin;
    } rs;
    int      handoff_fin;
    int      state;
    int      role;
    int      busy;
} st_hook_state_t;


int st_busy_count(const moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->track_sub_cap; i++)
        if (s->track_subs[i].state != MOQ_TRACK_SUB_FREE) n++;
    return n;
}


/* The DECLARED track-sub owner inventory -- not the whole record. The shared
 * request-family snapshot models only fields common to every owner kind, so it
 * omits this entry's stored prefix, its request-bidi receive buffer, its auth
 * staging and both FIN facts -- exactly the fields a handoff touches, which is
 * what this inventory adds.
 *
 * Deliberately UNMODELLED, named rather than implied:
 *   - the resolved-token array's CONTENTS (`token_count` and the count of
 *     staged entries are modelled; the tokens themselves are not);
 *   - the auth transaction's interior (only `auth_committed` and
 *     `auth_reject_code` are modelled).
 * Prefix allocation topology IS modelled, relationally: see
 * `prefix_topology_ok`.
 *
 * Bounds and NULL inconsistencies make the record INCOMPARABLE before any
 * dereference, and two incomparable records never compare equal. That includes
 * a retained length beyond STO_BUF_MAX -- such a record is INVALID and its
 * comparison FAILS; it is not a silent exclusion of the bytes past the bound. */
#define STO_BUF_MAX 256

#define STO_MAX_PARTS 32            /* draft-18 caps a Track Namespace at 32 */


typedef struct sto_snap {
    int      present;
    int      state;
    int      role;
    uint32_t generation;
    uint64_t handle;
    uint64_t request_id;
    int      ep_kind;
    int      ep_slot;
    int      ep_has_request_id;
    uint64_t ep_request_id;
    int      ep_has_stream_ref;
    uint64_t ep_stream_ref;
    uint64_t stream_ref;
    int      forward;
    struct {
        int recv_fin;
    } rs;
    int      handoff_fin;
    int      goaway_sent;
    int      auth_committed;
    uint64_t auth_reject_code;
    size_t   token_count;
    size_t   prefix_count;
    size_t   prefix_buf_len;
    size_t   prefix_len;             /* encoded (u8 len, bytes) per part */
    uint8_t  prefix[STO_BUF_MAX];
    /* Relational, never an adopted address: for a non-empty prefix both
     * allocations exist and every part's data addresses its own running offset
     * inside the owned buffer. */
    int      prefix_topology_ok;
    size_t   token_staged_count;
    const void *recv_buf;            /* preserved across reuse: identity matters */
    size_t   recv_cap;
    size_t   recv_len;
    uint8_t  recv[STO_BUF_MAX];
    int      invalid;
} sto_snap_t;


static void sto_read(const moq_session_t *s, int slot, sto_snap_t *v)
{
    memset(v, 0, sizeof(*v));
    if (slot < 0 || (size_t)slot >= s->track_sub_cap) { v->invalid = 1; return; }
    const moq_track_sub_entry_t *e = &s->track_subs[slot];
    v->present = (e->state != MOQ_TRACK_SUB_FREE);
    v->state = (int)e->state;
    v->role = (int)e->role;
    v->generation = e->generation;
    v->handle = e->handle._opaque;
    v->request_id = e->request_id;
    v->ep_kind = (int)e->request_ep.kind;
    v->ep_slot = e->request_ep.slot;
    v->ep_has_request_id = e->request_ep.has_request_id ? 1 : 0;
    v->ep_request_id = e->request_ep.request_id;
    v->ep_has_stream_ref = e->request_ep.has_stream_ref ? 1 : 0;
    v->ep_stream_ref = e->request_ep.stream_ref._v;
    v->stream_ref = e->rs.ref._v;
    v->forward = e->forward ? 1 : 0;
    v->rs.recv_fin = e->rs.recv_fin ? 1 : 0;
    v->handoff_fin = e->handoff_fin_pending ? 1 : 0;
    v->goaway_sent = e->goaway_sent ? 1 : 0;
    v->auth_committed = e->auth_committed ? 1 : 0;
    v->auth_reject_code = e->auth_reject_code;
    v->token_count = e->token_count;
    v->prefix_count = e->prefix_count;
    v->prefix_buf_len = e->prefix_buf_len;
    /* Refuse BEFORE dereferencing: a count past the wire maximum, a count with
     * no array, or a non-empty part with no data would each be an out-of-range
     * read inside the harness meant to diagnose it. */
    if (e->prefix_count > STO_MAX_PARTS) { v->invalid = 1; return; }
    if (e->prefix_count > 0 && !e->prefix_parts) { v->invalid = 1; return; }
    if (e->token_count > MOQ_DECODED_MAX_TOKENS) { v->invalid = 1; return; }
    for (size_t i = 0; i < e->token_count; i++)
        if (e->token_staged[i]) v->token_staged_count++;
    /* Topology fails CLOSED: every check below returns an incomparable record
     * BEFORE forming a pointer outside the declared allocation and before any
     * byte is copied. A corrupted part pointer must make this harness refuse,
     * never dereference. `prefix_topology_ok` therefore reads 1 on every
     * comparable record -- the discrimination lives in the refusal, and the
     * field remains as a declared fact rather than the detector. */
    v->prefix_topology_ok = 1;
    if (e->prefix_count > 0 && (!e->prefix_buf || !e->prefix_parts)) {
        v->invalid = 1;
        return;
    }
    size_t off = 0;
    for (size_t i = 0; i < e->prefix_count; i++) {
        size_t l = e->prefix_parts[i].len;
        if (l > 0 && !e->prefix_parts[i].data) { v->invalid = 1; return; }
        if (l > 255 || v->prefix_len + 1 + l > STO_BUF_MAX) {
            v->invalid = 1;
            return;
        }
        /* Subtraction-safe: prove this part lies inside the owned buffer
         * BEFORE `prefix_buf + off` is formed. */
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
    if (off != e->prefix_buf_len) { v->invalid = 1; return; }
    v->recv_buf = (const void *)e->rs.recv_buf;
    v->recv_cap = e->rs.recv_cap;
    v->recv_len = e->rs.recv_len;
    if (e->rs.recv_len > STO_BUF_MAX ||
        (e->rs.recv_len && !e->rs.recv_buf)) {
        v->invalid = 1;
        return;
    }
    if (e->rs.recv_len) memcpy(v->recv, e->rs.recv_buf, e->rs.recv_len);
}


static int sto_diff(const sto_snap_t *have, const sto_snap_t *want,
                    const char *what)
{
    if (have->invalid || want->invalid) {
        TXS_DIAG("TXN %s: incomparable track-sub record\n", what);
        return 1;
    }
    int bad = 0;
#define STO_EQ(field) do { \
        if ((uint64_t)have->field != (uint64_t)want->field) { \
            TXS_DIAG("TXN %s: st owner " #field " %llu, expected %llu\n", what, \
                     (unsigned long long)have->field, \
                     (unsigned long long)want->field); \
            bad++; \
        } \
    } while (0)
    STO_EQ(present); STO_EQ(state); STO_EQ(role); STO_EQ(generation);
    STO_EQ(handle); STO_EQ(request_id);
    STO_EQ(ep_kind); STO_EQ(ep_slot); STO_EQ(ep_has_request_id);
    STO_EQ(ep_request_id); STO_EQ(ep_has_stream_ref); STO_EQ(ep_stream_ref);
    STO_EQ(stream_ref); STO_EQ(forward); STO_EQ(rs.recv_fin);
    STO_EQ(handoff_fin); STO_EQ(goaway_sent); STO_EQ(auth_committed);
    STO_EQ(auth_reject_code); STO_EQ(token_count); STO_EQ(prefix_count);
    STO_EQ(prefix_buf_len); STO_EQ(prefix_len); STO_EQ(recv_cap);
    STO_EQ(recv_len); STO_EQ(prefix_topology_ok); STO_EQ(token_staged_count);
#undef STO_EQ
    if (have->recv_buf != want->recv_buf) {
        TXS_DIAG("TXN %s: st owner receive buffer identity changed\n", what);
        bad++;
    }
    if (have->prefix_len == want->prefix_len && have->prefix_len &&
        memcmp(have->prefix, want->prefix, have->prefix_len) != 0) {
        TXS_DIAG("TXN %s: st owner prefix bytes differ\n", what);
        bad++;
    }
    if (have->recv_len == want->recv_len && have->recv_len &&
        memcmp(have->recv, want->recv, have->recv_len) != 0) {
        TXS_DIAG("TXN %s: st owner retained bytes differ\n", what);
        bad++;
    }
    return bad;
}


/* The record a committed publisher-role SUBSCRIBE_TRACKS owner must hold,
 * built from values the fixture declared before ingress plus the free slot's
 * preserved receive buffer -- never read back from the call under test. */
static void sto_declare(sto_snap_t *v, int slot, uint32_t gen, uint64_t handle,
                        uint64_t request_id, uint64_t stream_ref,
                        int handoff_fin, const void *recv_buf, size_t recv_cap)
{
    (void)slot;
    memset(v, 0, sizeof(*v));
    v->present = 1;
    v->state = (int)MOQ_TRACK_SUB_PENDING_PUBLISHER;
    v->role = (int)MOQ_TRACK_SUB_ROLE_PUBLISHER;
    v->generation = gen;
    v->handle = handle;
    v->request_id = request_id;
    /* The entry's own `request_ep` stays ZEROED on this family: the commit
     * builds the endpoint on the decoded request and installs it in the
     * stream-ref registry, and only the namespace-sub family mirrors one onto
     * its entry. Declared as unset so a future write to it is a diagnostic. */
    v->ep_kind = 0;
    v->ep_slot = 0;
    v->ep_has_request_id = 0;
    v->ep_request_id = 0;
    v->ep_has_stream_ref = 0;
    v->ep_stream_ref = 0;
    v->stream_ref = stream_ref;
    v->forward = 1;                      /* st_feed sets Forward = 1 */
    v->rs.recv_fin = 0;
    v->handoff_fin = handoff_fin;
    v->goaway_sent = 0;
    /* The inbound publisher-side commit never writes the entry's auth staging:
     * the transaction is committed on the DECODED request, and resolved tokens
     * belong to the locally-issued subscriber path. */
    v->auth_committed = 0;
    v->auth_reject_code = 0;
    v->token_count = 0;
    v->prefix_count = 2;
    /* st_feed's prefix is ("live", "eu"); the stored buffer holds the bytes. */
    v->prefix_buf_len = 6;
    v->prefix_len = 0;
    v->prefix[v->prefix_len++] = 4;
    memcpy(v->prefix + v->prefix_len, "live", 4); v->prefix_len += 4;
    v->prefix[v->prefix_len++] = 2;
    memcpy(v->prefix + v->prefix_len, "eu", 2);   v->prefix_len += 2;
    v->prefix_topology_ok = 1;
    v->token_staged_count = 0;
    v->recv_buf = recv_buf;
    v->recv_cap = recv_cap;
    v->recv_len = 0;                     /* the request rode the staging entry */
}


/* A live publisher-role track-sub owner in these rows holds exactly one
 * stream-ref edge and no by-id or namespace-index edge. */
static int st_check_owner_edges(const og_graph_t *g, int slot,
                                moq_stream_ref_t ref, const char *what)
{
    const og_edge_spec_t want[] = { { OG_DOM_REQ_STREAMREF, ref._v } };
    int bad = og_check_owner_edges(g, (int)MOQ_REQ_SUBSCRIBE_TRACKS, slot,
                                   want, 1, what);
    bad += og_check_no_edge(g, OG_DOM_NS_REF, ref._v, what);
    return bad;
}


/* The declared session delta a committed SUBSCRIBE_TRACKS handoff may produce,
 * measured against a capture taken BEFORE the feed. Exactly two changes are
 * permitted: the stream's owner becomes the declared track-sub entry, and the
 * event-scratch cursor is ADOPTED as an explicit exclusion -- the surfaced
 * request image borrowed it, and draining events does not reclaim the arena
 * (the next advancing call's preparation does), so it cannot be declared
 * independently. Everything else -- session state, both queue depths, payload
 * accounting and drain membership -- must be unchanged. */
static int st_check_handoff_delta(moq_session_t *s, moq_stream_ref_t ref,
                                  const txs_snapshot_t *pre, int slot,
                                  uint32_t gen, const char *what)
{
    txs_snapshot_t observed;
    txs_capture(s, &ref, 1, &observed);
    txs_snapshot_t want = *pre;
    want.event_scratch_len = observed.event_scratch_len;   /* adopted */
    want.owners[0].kind = (int)MOQ_REQ_SUBSCRIBE_TRACKS;
    want.owners[0].slot = slot;
    want.owners[0].state = (int)MOQ_TRACK_SUB_PENDING_PUBLISHER;
    want.owners[0].generation = gen;
    want.owners[0].invalid = 0;
    return txs_check_eq(s, &ref, 1, &want, what);
}


static int st_declare_free_slot(const moq_session_t *s)
{
    for (size_t i = 0; i < s->track_sub_cap; i++)
        if (s->track_subs[i].state == MOQ_TRACK_SUB_FREE) return (int)i;
    return -1;
}


const moq_track_sub_entry_t *st_by_ref(const moq_session_t *s,
                                       moq_stream_ref_t ref)
{
    for (size_t i = 0; i < s->track_sub_cap; i++) {
        const moq_track_sub_entry_t *e = &s->track_subs[i];
        if (e->state != MOQ_TRACK_SUB_FREE && e->rs.ref._v == ref._v)
            return e;
    }
    return NULL;
}


moq_result_t st_feed(moq_session_t *s, void *ctx, moq_stream_ref_t ref,
                     bool fin)
{
    st_ctx_t *c = (st_ctx_t *)ctx;
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live"),
                            MOQ_BYTES_LITERAL("eu") };
    moq_namespace_t pfx = { parts, 2 };
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    mp.has_forward = true; mp.forward = 1;
    mp.auth_token_count = 1;
    mp.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
    mp.auth_tokens[0].token_type = 6;
    mp.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("sttok");
    uint8_t m[192];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, sizeof(m));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe_tracks(&w, c->request_id, &pfx, &mp),
        (int)MOQ_OK);
    return moq_session_on_bidi_stream_bytes(s, ref, m,
                                            moq_buf_writer_offset(&w), fin, 1);
}


static moq_result_t st_app_reject(moq_session_t *s, void *ctx, uint64_t handle)
{
    (void)ctx;
    moq_track_sub_handle_t h;
    h._opaque = handle;
    moq_reject_subscribe_tracks_cfg_t rj;
    moq_reject_subscribe_tracks_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    return moq_session_reject_subscribe_tracks(s, h, &rj, 1);
}


static void st_hook_capture(const moq_session_t *s, void *vctx, void *vst)
{
    st_ctx_t *c = (st_ctx_t *)vctx;
    st_hook_state_t *st = (st_hook_state_t *)vst;
    memset(st, 0, sizeof(*st));
    st->busy = st_busy_count(s);
    const moq_track_sub_entry_t *e = st_by_ref(s, c->ref);
    if (!e) return;
    st->present = 1;
    st->handle = e->handle._opaque;
    st->req_stream_ref = e->rs.ref._v;
    st->rs.recv_fin = e->rs.recv_fin;
    st->handoff_fin = e->handoff_fin_pending;
    st->state = (int)e->state;
    st->role = (int)e->role;
}


static int st_hook_check(const moq_session_t *s, void *vctx, const void *vst)
{
    st_hook_state_t now;
    st_hook_capture(s, vctx, &now);
    const st_hook_state_t *want = (const st_hook_state_t *)vst;
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
        TXS_DIAG("TXN st hooks: present %d handle %llu ref %llu fin %d "
                 "handoff %d state %d role %d busy %d, expected "
                 "%d/%llu/%llu/%d/%d/%d/%d/%d\n",
                 now.present, (unsigned long long)now.handle,
                 (unsigned long long)now.req_stream_ref, now.rs.recv_fin,
                 now.handoff_fin, now.state, now.role, now.busy, want->present,
                 (unsigned long long)want->handle,
                 (unsigned long long)want->req_stream_ref, want->rs.recv_fin,
                 want->handoff_fin, want->state, want->role, want->busy);
    return bad;
}


static bool st_norm_event(const moq_event_t *ev, void *vctx,
                          txs_norm_vec_t *out)
{
    st_ctx_t *c = (st_ctx_t *)vctx;
    txs_img_t im;
    txs_img_init(&im);
    switch (ev->kind) {
    case MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST: {
        const moq_subscribe_tracks_request_event_t *rq =
            &ev->u.subscribe_tracks_request;
        c->last_handle = rq->handle._opaque;
        txs_img_u64(&im, rq->handle._opaque);
        txs_img_u64(&im, rq->forward ? 1u : 0u);
        if (!txs_img_parts(&im, "subscribe_tracks prefix", rq->track_namespace_prefix.parts,
                           rq->track_namespace_prefix.count,
                           MOQ_DECODED_MAX_NAMESPACE_PARTS))
            return false;
        if (!txs_img_tokens(&im, "subscribe_tracks tokens", rq->tokens,
                            rq->token_count, MOQ_DECODED_MAX_TOKENS))
            return false;
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED:
        txs_img_u64(&im, ev->u.subscribe_tracks_cancelled.handle._opaque);
        break;
    default:
        TXS_DIAG("TXN st: unnormalized event kind %u\n", (unsigned)ev->kind);
        return false;
    }
    return txs_norm_append_img(out, ev->kind, &im);
}


static void want_st_request(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    txs_img_u64(&im, 1);                    /* forward */
    txs_img_u64(&im, 2);                    /* prefix parts */
    txs_img_bytes(&im, (const uint8_t *)"live", 4);
    txs_img_bytes(&im, (const uint8_t *)"eu", 2);
    txs_img_u64(&im, 1);                    /* token count */
    txs_img_u64(&im, 6);                    /* token type */
    txs_img_bytes(&im, (const uint8_t *)"sttok", 5);
    MOQ_TEST_CHECK(
        txs_norm_append_img(v, MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST, &im));
}


static void want_st_cancelled(txs_norm_vec_t *v, uint64_t handle)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, handle);
    MOQ_TEST_CHECK(
        txs_norm_append_img(v, MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED, &im));
}


st_ctx_t st_ctx = { 0, { 1 }, 0 };


static fin_owner_case_t st_family(void)
{
    fin_owner_case_t f;
    memset(&f, 0, sizeof(f));
    f.name = "subscribe-tracks";
    f.ctx = &st_ctx;
    f.caps = FIN_TERM_APP_REJECT;
    f.feed = st_feed;
    f.owner_kind = MOQ_REQ_SUBSCRIBE_TRACKS;
    f.owner_role = (int)MOQ_TRACK_SUB_ROLE_PUBLISHER;
    f.owner_state = (int)MOQ_TRACK_SUB_PENDING_PUBLISHER;
    f.app_reject = st_app_reject;
    f.hooks.ctx = &st_ctx;
    f.hooks.capture = st_hook_capture;
    f.hooks.check = st_hook_check;
    f.hooks.normalize_event = st_norm_event;
    f.hooks.normalize_action = pub_norm_action;
    return f;
}


static int st_owner_present_problems(const moq_session_t *s,
                                     moq_stream_ref_t ref,
                                     moq_request_kind_t want_kind,
                                     int want_role, int want_state)
{
    int bad = 0;
    const moq_track_sub_entry_t *e = st_by_ref(s, ref);
    if (!e) return 1;
    if ((int)e->role != want_role) bad++;
    if ((int)e->state != want_state) bad++;
    if (st_busy_count(s) != 1) bad++;
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if ((int)ep.kind != (int)want_kind) bad++;
    else if (ep.slot != (int)(e - s->track_subs)) bad++;
    else if (!ep.has_stream_ref || ep.stream_ref._v != ref._v) bad++;
    if (bad)
        TXS_DIAG("TXN st owner: kind %d role %d state %d, expected "
                 "kind %d role %d state %d\n", (int)ep.kind, (int)e->role,
                 (int)e->state, (int)want_kind, want_role, want_state);
    return bad;
}


/* Retirement proven on the RAW graph as well as through the production lookup:
 * a hidden or duplicate edge pointing at the freed slot is invisible to every
 * real lookup while a table scan still sees it. The slot is the one DECLARED
 * before ingress -- never rediscovered through the index being audited. */
static void check_st_retired_slot(moq_session_t *s, moq_stream_ref_t ref,
                                  int slot, const char *what)
{
    MOQ_TEST_CHECK_EQ_INT(st_busy_count(s), 0);
    check_unregistered(s, ref);
    og_graph_t g;
    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
    failures += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v, what);
    failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_SUBSCRIBE_TRACKS,
                                            slot, what);
}


static void run_st_case(const fin_owner_case_t *f, uint32_t max_events,
                        bool fin_in_request, bool fill_ring, bool reject_now)
{
    moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, max_events);
    st_ctx_t *c = (st_ctx_t *)f->ctx;
    moq_stream_ref_t ref = c->ref;
    c->last_handle = 0;

    /* The identity the request must land on is derived from pool state before
     * ingress, so the surfaced handle is checked against a declaration rather
     * than read back from whatever admission produced. */
    int slot = st_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { moq_session_destroy(s); return; }
    uint32_t gen = s->track_subs[slot].generation | 1u;
    uint64_t handle = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIBE_TRACKS,
                                      s->session_tag, gen, (uint32_t)slot);
    /* The co-allocated receive buffer survives entry reuse, so its identity is
     * part of the declaration and is taken from the FREE slot. */
    const void *slot_recv_buf = (const void *)s->track_subs[slot].rs.recv_buf;
    size_t slot_recv_cap = s->track_subs[slot].rs.recv_cap;

    /* The refused handoff is measured against a capture taken BEFORE it, so the
     * unrelated session facts cannot be defined by the call under test. */
    txs_snapshot_t pre_feed;
    txs_capture(s, &ref, 1, &pre_feed);

    /* With the FIN in the request chunk and room for both events the
     * cancellation runs immediately; with room for only the request event it
     * defers and the feed reports backpressure. */
    bool defer = fin_in_request && max_events == 1;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, ref, fin_in_request),
                          (int)(defer ? MOQ_ERR_WOULD_BLOCK : MOQ_OK));

    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_st_request(&want, c->last_handle);
    if (fin_in_request && !defer)
        want_st_cancelled(&want, c->last_handle);
    failures += txs_norm_equals(&got, &want, f->name);
    txs_norm_free(&got);
    txs_norm_free(&want);
    MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle);

    if (fin_in_request && !defer) {
        check_st_retired_slot(s, ref, slot, "st same-call FIN retired");
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
        check_no_output(s, &f->hooks);
        moq_session_destroy(s);
        return;
    }

    failures += st_owner_present_problems(s, ref, f->owner_kind, f->owner_role,
                                          f->owner_state);
    st_hook_state_t owner_before;
    f->hooks.capture(s, f->ctx, &owner_before);
    MOQ_TEST_CHECK_EQ_U64(owner_before.handle, handle);
    MOQ_TEST_CHECK_EQ_U64(owner_before.req_stream_ref, ref._v);
    /* Resolve once and guard: a mutant that loses the owner must still reach a
     * normal exit with diagnostics, never a NULL dereference. */
    const moq_track_sub_entry_t *live = st_by_ref(s, ref);
    MOQ_TEST_CHECK(live != NULL);
    if (live) {
        MOQ_TEST_CHECK_EQ_INT((int)(live - s->track_subs), slot);
        MOQ_TEST_CHECK_EQ_U64(live->generation, gen);
    }
    /* The two FIN facts are distinct: a wire FIN whose cancellation was
     * refused on event capacity is held as transitional ownership, not as the
     * durable latch, and a request without FIN holds neither. */
    MOQ_TEST_CHECK_EQ_INT(owner_before.rs.recv_fin, 0);
    MOQ_TEST_CHECK_EQ_INT(owner_before.handoff_fin, defer ? 1 : 0);

    /* The declared track-sub owner inventory, built before ingress. */
    sto_snap_t sto_want, sto_have;
    sto_declare(&sto_want, slot, gen, handle, c->request_id, ref._v,
                defer ? 1 : 0, slot_recv_buf, slot_recv_cap);
    sto_read(s, slot, &sto_have);
    failures += sto_diff(&sto_have, &sto_want, "st committed owner");

    /* The handoff's declared session delta (see st_check_handoff_delta). */
    failures += st_check_handoff_delta(s, ref, &pre_feed, slot, gen,
                                       "st handoff delta");

    /* The live owner's ABSOLUTE edge set, validated BEFORE any graph derived
     * from it is used as a conservation baseline. */
    og_graph_t live_graph;
    og_capture(s, &live_graph);
    failures += og_check_integrity(&live_graph, "st live");
    failures += st_check_owner_edges(&live_graph, slot, ref, "st live");

    if (fill_ring) fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);

    if (!reject_now) {
        /* The deferred cancellation completes exactly once on retry, without
         * a drain reference and without wire output. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        txs_norm_vec_t got2, want2;
        txs_norm_init(&got2);
        txs_norm_init(&want2);
        collect_events(s, &f->hooks, &got2);
        want_st_cancelled(&want2, c->last_handle);
        failures += txs_norm_equals(&got2, &want2, f->name);
        txs_norm_free(&got2);
        txs_norm_free(&want2);
        check_st_retired_slot(s, ref, slot, "st retry retired");
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before, "st retry");
        check_no_output(s, &f->hooks);
        moq_session_destroy(s);
        return;
    }

    bool expect_drain = !fin_in_request;
    moq_result_t want_rc = (expect_drain && fill_ring) ? MOQ_ERR_WOULD_BLOCK
                                                       : MOQ_OK;
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, c->last_handle),
                          (int)want_rc);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        failures += txs_check_eq(s, &ref, 1, &before, f->name);
        failures += f->hooks.check(s, f->ctx, &owner_before);
        failures += st_owner_present_problems(s, ref, f->owner_kind,
                                              f->owner_role, f->owner_state);
        /* The refusal must preserve the DECLARED record and the owner's exact
         * edge set, and conserve the whole topology validated before it -- the
         * narrow hook alone cannot see a field it never modelled or an edge
         * inserted after `live_graph` was taken. */
        sto_read(s, slot, &sto_have);
        failures += sto_diff(&sto_have, &sto_want, "st blocked owner");
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "st blocked");
        failures += st_check_owner_edges(&g, slot, ref, "st blocked");
        failures += og_check_same_topology(&g, &live_graph, "st blocked");
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before, "st blocked");
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
    failures += drain_multiset_equals(&now, &want_ring, "st granted");
    check_st_retired_slot(s, ref, slot, "st granted retired");
    check_reject_only_output(s, &f->hooks, ref, f->name);

    if (expect_drain) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t after;
        drain_snap(s, &after);
        failures += drain_multiset_equals(&after, &ring_before, "st released");
        check_st_retired_slot(s, ref, slot, "st released retired");
        check_no_output(s, &f->hooks);
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    moq_session_destroy(s);
}


/* Clear-on-reuse for the subscribe-tracks pool. This family's consumer can
 * block, so the marker is genuinely live across a refusal: the case drives a
 * FIN-carrying request into a one-slot event queue, completes the deferred
 * cancellation from the empty re-feed, and then proves the observed close did
 * not follow the physical slot to its next owner. Both generations and both
 * packed handles are derived before ingress. */
static void run_st_slot_reuse_case(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 1);
    st_ctx_t *c = (st_ctx_t *)f->ctx;

    int slot = st_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { moq_session_destroy(s); return; }
    uint32_t gen_first = s->track_subs[slot].generation | 1u;
    uint32_t gen_reuse = (gen_first + 1u) | 1u;   /* the free advances once */
    uint64_t handle_first = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIBE_TRACKS,
                                            s->session_tag, gen_first,
                                            (uint32_t)slot);
    uint64_t handle_reuse = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIBE_TRACKS,
                                            s->session_tag, gen_reuse,
                                            (uint32_t)slot);
    MOQ_TEST_CHECK(handle_first != handle_reuse);
    const void *slot_recv_buf = (const void *)s->track_subs[slot].rs.recv_buf;
    size_t slot_recv_cap = s->track_subs[slot].rs.recv_cap;

    /* 1. SUBSCRIBE_TRACKS + FIN into a one-slot queue: the request event takes
     *    the slot, so the cancellation is refused and the marker survives it. */
    moq_stream_ref_t first = c->ref;
    c->last_handle = 0;
    txs_snapshot_t pre_first;
    txs_capture(s, &first, 1, &pre_first);
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, first, true),
                          (int)MOQ_ERR_WOULD_BLOCK);
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_st_request(&want, c->last_handle);
    failures += txs_norm_equals(&got, &want, "st reuse first");
    txs_norm_free(&got);
    txs_norm_free(&want);
    MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle_first);
    failures += st_owner_present_problems(s, first, f->owner_kind,
                                          f->owner_role, f->owner_state);
    {
        const moq_track_sub_entry_t *e = st_by_ref(s, first);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_INT((int)(e - s->track_subs), slot);
            MOQ_TEST_CHECK_EQ_U64(e->generation, gen_first);
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, handle_first);
            /* Held as transitional ownership, never as the durable latch. */
            MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin, 0);
            MOQ_TEST_CHECK_EQ_INT(e->handoff_fin_pending, 1);
        }
    }
    {   /* The declared owner inventory, and the owner's exact edge set validated
         * before any graph derived from it becomes a conservation baseline. */
        sto_snap_t w, h;
        sto_declare(&w, slot, gen_first, handle_first, c->request_id, first._v,
                    1, slot_recv_buf, slot_recv_cap);
        sto_read(s, slot, &h);
        failures += sto_diff(&h, &w, "st reuse first owner");
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "st reuse first");
        failures += st_check_owner_edges(&g, slot, first, "st reuse first");
    }
    /* The retention feed's own declared session delta: without it an unrelated
     * mutation is invisible in exactly the proof that exists to catch one. */
    failures += st_check_handoff_delta(s, first, &pre_first, slot, gen_first,
                                       "st reuse handoff delta");

    /* 2. The empty re-feed completes the cancellation exactly once, drainlessly,
     *    and retires the owner. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, first, NULL, 0, false, 1),
        (int)MOQ_OK);
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_st_cancelled(&want, handle_first);
    failures += txs_norm_equals(&got, &want, "st reuse retry");
    txs_norm_free(&got);
    txs_norm_free(&want);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
    check_no_output(s, &f->hooks);

    /* 3. The freed record: generation advanced, BOTH FIN facts clear, and no
     *    edge anywhere still points at the retired owner. */
    MOQ_TEST_CHECK_EQ_INT((int)s->track_subs[slot].state,
                          (int)MOQ_TRACK_SUB_FREE);
    MOQ_TEST_CHECK_EQ_U64(s->track_subs[slot].generation, gen_reuse - 1u);
    MOQ_TEST_CHECK_EQ_INT(s->track_subs[slot].rs.recv_fin, 0);
    MOQ_TEST_CHECK_EQ_INT(s->track_subs[slot].handoff_fin_pending, 0);
    check_st_retired_slot(s, first, slot, "st reuse retired");

    /* 4. A distinct no-FIN SUBSCRIBE_TRACKS reuses that physical slot and
     *    holds neither FIN fact. */
    moq_stream_ref_t second = moq_stream_ref_from_u64(c->ref._v + 4);
    moq_stream_ref_t saved_ref = c->ref;
    uint64_t saved_id = c->request_id;
    c->ref = second;
    c->request_id = saved_id + 2;
    c->last_handle = 0;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, second, false), (int)MOQ_OK);
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &f->hooks, &got);
    want_st_request(&want, c->last_handle);
    failures += txs_norm_equals(&got, &want, "st reuse second");
    txs_norm_free(&got);
    txs_norm_free(&want);
    failures += st_owner_present_problems(s, second, f->owner_kind,
                                          f->owner_role, f->owner_state);
    {
        const moq_track_sub_entry_t *e = st_by_ref(s, second);
        MOQ_TEST_CHECK(e != NULL);
        if (e) {
            MOQ_TEST_CHECK_EQ_INT((int)(e - s->track_subs), slot);
            MOQ_TEST_CHECK_EQ_U64(e->generation, gen_reuse);
            MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, handle_reuse);
            MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin, 0);
            MOQ_TEST_CHECK_EQ_INT(e->handoff_fin_pending, 0);
        }
        MOQ_TEST_CHECK_EQ_U64(c->last_handle, handle_reuse);
        check_registered(s, second, MOQ_REQ_SUBSCRIBE_TRACKS, slot);
        sto_snap_t w, h;
        sto_declare(&w, slot, gen_reuse, handle_reuse, c->request_id, second._v,
                    0, slot_recv_buf, slot_recv_cap);
        sto_read(s, slot, &h);
        failures += sto_diff(&h, &w, "st reuse second owner");
    }

    /* 5. Its peer half is genuinely open, so its rejection owes a drain
     *    reference and an exhausted ring refuses it transactionally. */
    fill_drain_ring(s);
    size_t filled = s->drain_ref_count;
    og_graph_t graph_before;
    og_capture(s, &graph_before);
    failures += og_check_integrity(&graph_before, "st reuse live");
    /* Validate the reused owner's ABSOLUTE edge set before this graph is used
     * as the conservation baseline -- an undeclared or duplicate edge must not
     * be able to become the thing conservation preserves. */
    failures += st_check_owner_edges(&graph_before, slot, second,
                                     "st reuse live");
    st_hook_state_t owner;
    f->hooks.capture(s, f->ctx, &owner);
    txs_snapshot_t before;
    txs_capture(s, &second, 1, &before);
    expect_after_call_prepare(&before);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_reuse),
                          (int)MOQ_ERR_WOULD_BLOCK);
    failures += txs_check_eq(s, &second, 1, &before, "st reuse blocked");
    failures += f->hooks.check(s, f->ctx, &owner);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    {
        sto_snap_t w, h;
        sto_declare(&w, slot, gen_reuse, handle_reuse, c->request_id, second._v,
                    0, slot_recv_buf, slot_recv_cap);
        sto_read(s, slot, &h);
        failures += sto_diff(&h, &w, "st reuse blocked owner");
        og_graph_t g;
        og_capture(s, &g);
        failures += og_check_integrity(&g, "st reuse blocked");
        failures += st_check_owner_edges(&g, slot, second, "st reuse blocked");
        failures += og_check_same_topology(&g, &graph_before,
                                           "st reuse blocked");
    }
    check_no_output(s, &f->hooks);

    /* 6. The retired first-generation handle stays stale after the reuse. */
    txs_snapshot_t before_stale;
    txs_capture(s, &second, 1, &before_stale);
    expect_after_call_prepare(&before_stale);
    MOQ_TEST_CHECK_EQ_INT((int)f->app_reject(s, f->ctx, handle_first),
                          (int)MOQ_ERR_STALE_HANDLE);
    failures += txs_check_eq(s, &second, 1, &before_stale,
                             "st stale after reuse");
    failures += f->hooks.check(s, f->ctx, &owner);
    MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, filled);
    check_no_output(s, &f->hooks);

    c->ref = saved_ref;
    c->request_id = saved_id;
    moq_session_destroy(s);
}


/* Permanent self-checks for the inventory's refusal paths. Each perturbs one
 * field of a live owner, requires the record to come back INCOMPARABLE (so the
 * comparison FAILS rather than reading out of range), and restores the field
 * before the fixture is destroyed. Quiet: a passing run prints nothing. */
static void st_selfcheck_invalid(moq_session_t *s, int slot,
                                 const sto_snap_t *want, const char *what)
{
    sto_snap_t got;
    sto_read(s, slot, &got);
    if (!got.invalid) {
        fprintf(stderr, "FAIL: %s:%d: sto_read accepted %s\n",
                __FILE__, __LINE__, what);
        failures++;
        return;
    }
    txs_quiet = 1;
    int diff = sto_diff(&got, want, what);
    txs_quiet = 0;
    if (diff == 0) {
        fprintf(stderr, "FAIL: %s:%d: sto_diff compared equal on %s\n",
                __FILE__, __LINE__, what);
        failures++;
    }
}


static void st_inventory_selfchecks(const fin_owner_case_t *f)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    st_ctx_t *c = (st_ctx_t *)f->ctx;
    int slot = st_declare_free_slot(s);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) { moq_session_destroy(s); return; }

    /* A live committed owner to perturb. */
    sto_snap_t valid;
    c->last_handle = 0;
    MOQ_TEST_CHECK_EQ_INT((int)f->feed(s, f->ctx, c->ref, false), (int)MOQ_OK);
    {   /* drain the request event */
        txs_norm_vec_t d;
        txs_norm_init(&d);
        collect_events(s, &f->hooks, &d);
        txs_norm_free(&d);
    }
    sto_read(s, slot, &valid);
    MOQ_TEST_CHECK(valid.invalid == 0);
    moq_track_sub_entry_t *e = &s->track_subs[slot];

    /* Out-of-range slots are refused without touching the pool, and their
     * records must also FAIL to compare -- the comparator end, not only the
     * capture end. */
    st_selfcheck_invalid(s, (int)s->track_sub_cap, &valid, "slot past the pool");
    st_selfcheck_invalid(s, -1, &valid, "negative slot");

    size_t       save_pc   = e->prefix_count;
    moq_bytes_t *save_pp   = e->prefix_parts;
    size_t       save_tc   = e->token_count;
    size_t       save_rl   = e->rs.recv_len;
    uint8_t     *save_rb   = e->rs.recv_buf;
    const uint8_t *save_d0 = e->prefix_parts ? e->prefix_parts[0].data : NULL;

    e->prefix_count = STO_MAX_PARTS + 1;
    st_selfcheck_invalid(s, slot, &valid, "excessive prefix count");
    e->prefix_count = save_pc;

    e->prefix_parts = NULL;
    st_selfcheck_invalid(s, slot, &valid, "NULL parts array with nonzero count");
    e->prefix_parts = save_pp;

    if (save_pp && save_pc > 0) {
        e->prefix_parts[0].data = NULL;      /* part 0 is non-empty ("live") */
        st_selfcheck_invalid(s, slot, &valid, "NULL nonempty part data");
        e->prefix_parts[0].data = save_d0;
    }

    e->token_count = MOQ_DECODED_MAX_TOKENS + 1;
    st_selfcheck_invalid(s, slot, &valid, "excessive token count");
    e->token_count = save_tc;

    e->rs.recv_len = STO_BUF_MAX + 1;
    st_selfcheck_invalid(s, slot, &valid, "oversized retained length");
    e->rs.recv_len = save_rl;

    e->rs.recv_len = 4;
    e->rs.recv_buf = NULL;
    st_selfcheck_invalid(s, slot, &valid, "retained length with NULL buffer");
    e->rs.recv_buf = save_rb;
    e->rs.recv_len = save_rl;

    /* Prefix allocation topology, all four branches. Every perturbation is
     * itself safe -- a nulled pointer, an offset that stays INSIDE the owned
     * buffer, and lengths that only shrink or grow the declared extent -- so a
     * refusal is proven without the probe ever risking an out-of-range read. */
    uint8_t     *save_pb   = e->prefix_buf;
    size_t       save_pbl  = e->prefix_buf_len;
    const uint8_t *save_d1 = (save_pp && save_pc > 1) ? save_pp[1].data : NULL;

    e->prefix_buf = NULL;
    st_selfcheck_invalid(s, slot, &valid, "NULL prefix buffer, nonempty prefix");
    e->prefix_buf = save_pb;

    if (save_pp && save_pc > 1 && save_pb) {
        e->prefix_parts[1].data = save_pb;      /* offset 0, still in-bounds */
        st_selfcheck_invalid(s, slot, &valid, "part addressing the wrong offset");
        e->prefix_parts[1].data = save_d1;
    }

    e->prefix_buf_len = 1;                      /* shorter than part 0 */
    st_selfcheck_invalid(s, slot, &valid, "prefix buffer length too short");
    e->prefix_buf_len = save_pbl;

    e->prefix_buf_len = save_pbl + 1;           /* parts fit, total disagrees */
    st_selfcheck_invalid(s, slot, &valid, "prefix extent length mismatch");
    e->prefix_buf_len = save_pbl;

    /* Every perturbation restored: the record compares equal again, so the
     * fixture is destroyed in exactly the state the session owns. */
    sto_snap_t back;
    sto_read(s, slot, &back);
    failures += sto_diff(&back, &valid, "st inventory restored");
    moq_session_destroy(s);
}


void test_subscribe_tracks(void)
{
    fin_owner_case_t f = st_family();
    MOQ_TEST_CHECK_EQ_INT(fin_owner_problems(&f), 0);
    st_inventory_selfchecks(&f);
    run_st_case(&f, 0, false, false, true);   /* control: drain, then released */
    run_st_case(&f, 0, true,  false, true);   /* same-call FIN: cancels at once */
    run_st_case(&f, 1, true,  false, false);  /* deferred, completed by retry */
    run_st_case(&f, 1, true,  false, true);   /* answered in the window */
    run_st_case(&f, 1, true,  true,  true);   /* exhausted ring, FIN observed */
    run_st_case(&f, 1, false, true,  true);   /* exhausted ring, no FIN */
    /* The observed close does not follow the slot to its next owner. */
    run_st_slot_reuse_case(&f);
}
