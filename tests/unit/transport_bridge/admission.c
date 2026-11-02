#include "test_cases.h"

/* -- no-slot admission over the PHYSICAL bridge: no-owner blockers ----
 *
 * handle_request_stream_bytes()'s no-slot admission path returns WOULD_BLOCK
 * with NO owner in any registry. The no-slot carrier makes the bridge's
 * retainability check answer true, so local resource pressure never turns the
 * unowned WOULD_BLOCK into MOQ_ERR_INTERNAL and a fatal connection. The
 * contract never fatalizes this path: the ingress either completes
 * immediately (MOQ_OK, obligation queued) or is retained (the no-slot carrier)
 * and completed by SERVICE ONLY, with no peer bytes re-delivered.
 *
 * One row per session-side origin, no-FIN form (the FIN forms are pinned by
 * the direct matrix). The whole run is driven off a DECLARED record: the seed
 * slot and generation are derived before ingress, both transport IDs and both
 * internal refs are saved while their entries are live, and every semantic
 * and physical check uses those saved identities -- nothing is re-derived
 * after retirement.
 *
 * Physical target contract, per class: post-SETUP, the FIN'd REQUEST_ERROR
 * write marks the local half closed and the mapping stays live until the
 * peer's own later FIN retires it exactly once (asserted). Pre-SETUP, the
 * RESET dispatch retires the mapping at dispatch, and the peer's REAL answer
 * to STOP_SENDING -- a RESET_STREAM delivered through the bridge on the
 * saved transport id -- must release the exact target NORMAL drain ref,
 * leave no owner or mapping, stay nonfatal and open, emit nothing, and be
 * idempotent under a repeated RESET plus service, whatever tombstone or
 * carrier design the corrected bridge uses internally.
 *
 * Drain honesty: origin 1's ring is helper-seeded capacity; its release
 * feeds the DECLARED filler's own FIN through the session's public absorb
 * path, and every expected ring is derived from declared members, never from
 * an observed result.
 */


typedef struct nob_case {
    const char *name;
    int         origin;      /* 1..4, as in the direct matrix */
    bool        pre_setup;
    /* The inbound target Request ID, DECLARED per row independently of the
     * encoded target bytes (draft-18 §10.1). Origin 4's announcement blocker
     * commits inbound id 0, so its next request is id 2; origins 1-3 commit no
     * preceding request and use id 0. A pre-ingress wire decoder compares the
     * encoded target against this declaration. */
    uint64_t    target_rid;
} nob_case_t;


/* Descriptor self-check: origin 4 requires target id 2, every other origin
 * requires 0. A missing/defaulted or coherently changed member cannot silently
 * redefine the fixture. */
static int nob_case_target_rid_ok(const nob_case_t *c)
{
    uint64_t want = (c->origin == 4) ? 2u : 0u;
    if (c->target_rid != want) {
        fprintf(stderr, "FAIL: nob %s: declared target_rid %llu, origin %d"
                " requires %llu\n", c->name, (unsigned long long)c->target_rid,
                c->origin, (unsigned long long)want);
        return 1;
    }
    return 0;
}


/* Everything a phase check needs, saved while live. */
typedef struct nob_run {
    uint64_t seed_id, target_id;      /* transport identities */
    moq_stream_ref_t seed_ref;        /* session refs, saved while mapped */
    moq_stream_ref_t target_ref;
    int      seed_slot;
    uint32_t seed_gen;
    size_t   seed_fed;                /* fragmented prefix length */
    uint8_t  seed_bytes[128];
    /* Origin-4 send blocker: a live receiver-side announcement whose nonterminal
     * per-request GOAWAY (§10.4) fills the send buffer -- a production-valid
     * request-bidi write, not a raw control blob. It stays live through the whole
     * target refusal/recovery, so it is part of the declared inventory. Zeroed
     * for other origins (blk_present = false). */
    bool     blk_present;
    int      blk_slot;
    uint32_t blk_gen;                 /* DECLARED live generation, pre-ingress */
    uint64_t blk_handle;              /* DECLARED packed announcement handle */
    uint64_t blk_rid;                 /* its committed inbound request id (0) */
    uint64_t blk_id;                  /* its transport id */
    moq_stream_ref_t blk_ref;         /* its request-bidi internal ref */
    uint64_t target_rid;              /* per-run inbound target Request ID */
} nob_run_t;


/* A bounds-safe conserved image of the origin-4 announcement blocker, captured
 * once its live shape is settled and re-checked at every later phase: the target
 * lifecycle must not mutate or retire it. Identity (slot/gen/handle/rid/ref) is
 * carried in nob_run_t and compared against the DECLARED values; the mutable
 * byte state is deep-copied here so an in-place change is caught. */
typedef struct nob_blk_snap {
    size_t   ns_len;
    uint8_t  ns_bytes[64];            /* deep copy of the canonical namespace */
    bool     ns_overflow;            /* ns_id_len exceeded the capture buffer */
    struct {
        size_t recv_cap;
        size_t recv_len;
        bool recv_fin;
        const uint8_t * recv_buf;
    } rs;
    bool     handoff_fin_pending;
} nob_blk_snap_t;


/* Origin-4 GOAWAY New Session URI: 36 bytes, sized so the encoded REQUEST_GOAWAY
 * fills the 64-byte send buffer to within < 24 bytes of full. Shared by the
 * blocker and the wire oracle so the dispatched write is compared byte-exact. */
static const uint8_t nob_goaway_uri[36] = {
    'h','t','t','p','s',':','/','/','r','e','l','a','y','.','e','x',
    'a','m','p','l','e','/','m','i','g','r','a','t','e','/','a','a',
    'a','a','a','a' };


/* The exhaustion reset arm's GOAWAY URI: longer than the no-owner matrix's so
 * the encoded REQUEST_GOAWAY fills the small send buffer far enough that the
 * target's REQUEST_ERROR cannot fit -- forcing the carrier-exhausted reset arm
 * rather than an immediate rejection. */
static const uint8_t exh_goaway_uri[52] = {
    'h','t','t','p','s',':','/','/','r','e','l','a','y','.','e','x',
    'a','m','p','l','e','/','m','i','g','r','a','t','e','/','a','a',
    'a','a','a','a','a','a','a','a','a','a','a','a','a','a','a','a',
    'a','a','a','a' };


/* Build a draft-18 request-stream SEED prefix that ends exactly after the
 * envelope header (Type vi64 + uint16 Length) and carries NO Request ID byte:
 * staging it consumes no wire Request ID, so it cannot collide with a request
 * the same session legitimately commits (draft-18 section 10.1 -- a duplicate
 * Request ID MUST close INVALID_REQUEST_ID). The eventual SUBSCRIBE is encoded
 * at `eventual_rid` only to derive a valid header; solely the header bytes are
 * fed. On success returns 0, copies the header into `buf`, and fills
 * *out_prefix_len (bytes to feed) / *out_encoded_len (the full message). */
static int d18_seed_header_prefix(uint8_t *buf, size_t cap, uint64_t eventual_rid,
                                  size_t *out_prefix_len, size_t *out_encoded_len)
{
    int failures = 0;
    uint8_t enc[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, enc, sizeof(enc));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("nob") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t prm = { 0 };
    if (moq_d18_encode_subscribe(&w, eventual_rid, &ns, MOQ_BYTES_LITERAL("t"),
                                 &prm) != MOQ_OK) return failures + 1;
    size_t encoded_len = moq_buf_writer_offset(&w);
    moq_buf_reader_t rr;
    moq_buf_reader_init(&rr, enc, encoded_len);
    uint64_t mtype = 0;
    uint16_t plen = 0;
    if (moq_buf_read_vi64(&rr, &mtype) < 0) return failures + 1;
    if (moq_buf_read_uint16(&rr, &plen) < 0) return failures + 1;
    size_t prefix_len = moq_buf_reader_offset(&rr);
    /* The split is exactly the reader position after the two header fields, is
     * shorter than the full message, and precedes the first payload byte (the
     * Request ID). */
    MOQ_TEST_CHECK(prefix_len > 0 && prefix_len < encoded_len);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)plen, encoded_len - prefix_len);
    if (prefix_len == 0 || prefix_len >= encoded_len || prefix_len > cap)
        return failures + 1;
    memcpy(buf, enc, prefix_len);
    if (out_prefix_len) *out_prefix_len = prefix_len;
    if (out_encoded_len) *out_encoded_len = encoded_len;
    return failures;
}


/* The seed owner and its physical mapping, by SAVED identity: entry state,
 * retained prefix bytes, registry answer, and the bridge entry reachable by
 * BOTH identities. */
static int nob_check_seed(test_pair_t *tp, const nob_run_t *r,
                          const char *what)
{
    int bad = 0;
    if (r->seed_slot < 0 || (size_t)r->seed_slot >= tp->server->sub_cap) {
        fprintf(stderr, "NOB %s: seed slot out of range\n", what);
        return bad + 1;
    }
    const moq_sub_entry_t *e = &tp->server->subs[r->seed_slot];
    if ((int)e->state != (int)MOQ_SUB_RECVING_REQUEST ||
        (int)e->role != (int)MOQ_SUB_ROLE_PUBLISHER ||
        e->request_id != 0 ||
        e->generation != r->seed_gen ||
        e->rs.ref._v != r->seed_ref._v ||
        e->rs.recv_fin ||
        e->rs.recv_len != r->seed_fed ||
        !e->rs.recv_buf ||
        memcmp(e->rs.recv_buf, r->seed_bytes, r->seed_fed) != 0) {
        fprintf(stderr, "NOB %s: seed owner record changed\n", what);
        bad++;
    }
    moq_request_endpoint_t ep =
        request_registry_find_by_streamref(tp->server, r->seed_ref);
    if (ep.kind != MOQ_REQ_SUBSCRIPTION || ep.slot != r->seed_slot ||
        !ep.has_stream_ref || ep.has_request_id ||
        ep.stream_ref._v != r->seed_ref._v) {
        fprintf(stderr, "NOB %s: seed registry answer changed\n", what);
        bad++;
    }
    /* by-ID-0 absence is absolute at arm and completion, asserted by the
     * seed-only graph (og_check_no_edge on rid 0); the REFUSED phase admits
     * an identity-qualified target carrier there, so this per-phase helper
     * deliberately does not reject it. The seed's own endpoint stays
     * stream-ref keyed with no request-ID field, checked above. */
    /* The COMPLETE physical mapping, by BOTH saved identities. */
    bridge_stream_entry_t *by_id = bridge_find_by_id(tp->server_bridge,
                                                     r->seed_id);
    bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp->server_bridge,
                                                       r->seed_ref);
    if (!by_id || by_id != by_ref || !by_id->active ||
        by_id->ref._v != r->seed_ref._v ||
        by_id->transport_id != r->seed_id ||
        by_id->kind != BRIDGE_STREAM_BIDI ||
        by_id->origin != BRIDGE_ORIGIN_PEER ||
        by_id->peer_send_closed || by_id->local_send_closed ||
        by_id->pending_retry || by_id->pending_fin || by_id->fin_retained ||
        by_id->pending_reset || by_id->pending_stop ||
        by_id->peer_stop_received || by_id->aborting ||
        by_id->pending_reset_code != 0 || by_id->pending_stop_code != 0) {
        fprintf(stderr, "NOB %s: seed bridge mapping changed\n", what);
        bad++;
    }
    return bad;
}


/* The generic completion-session layer, DERIVED from the pre-target
 * baseline: session state, both queue depths (the declared output has been
 * classified/dispatched at these checkpoints), the normalized scratch cursor,
 * receive-budget accounting, the phase's declared drain count, and the named
 * seed owner record -- via txs_check_eq on a derived snapshot, never values
 * copied from current state. The richer seed/graph/ring/mapping/output
 * checks remain; this is the generic layer, not a replacement. */
static int nob_check_session(const moq_session_t *sv, moq_stream_ref_t seed_ref,
                             const txs_snapshot_t *before, size_t ring_count,
                             const char *what)
{
    txs_snapshot_t expect = *before;
    expect.event_depth = 0;
    expect.action_depth = 0;
    expect.drain_ref_count = ring_count;
    return txs_check_eq(sv, &seed_ref, 1, &expect, what);
}


/* Quiet gate for the physical-state probe's expected-failure arms. */
static int nob_quiet;


/* The target's physical state by BOTH saved identities. `live` selects the
 * post-SETUP completed shape -- one active entry resolving by both
 * identities with the local half closed, the peer half open, every
 * pending/reset/stop/abort flag false, the STOP terminal fact
 * (`peer_stop_received`) false, and both pending codes zero -- or the fully
 * retired shape. This is the complete LIFECYCLE inventory, deliberately not
 * a byte-for-byte entry comparison: the bidi-irrelevant inbound-uni
 * classification storage (`uni_disp`, `classify_len`, `classify_buf`) is
 * outside the contract for these request bidis. */
static int nob_check_target_phys(test_pair_t *tp, const nob_run_t *r,
                                 int live, const char *what)
{
    bridge_stream_entry_t *te = bridge_find_by_id(tp->server_bridge,
                                                  r->target_id);
    bridge_stream_entry_t *tr = bridge_find_by_ref(tp->server_bridge,
                                                   r->target_ref);
    if (!live) {
        if ((te && te->active) || (tr && tr->active)) {
            if (!nob_quiet)
                fprintf(stderr, "NOB %s: target mapping survives"
                        " retirement\n", what);
            return 1;
        }
        return 0;
    }
    if (!te || te != tr || !te->active ||
        te->transport_id != r->target_id ||
        te->ref._v != r->target_ref._v ||
        te->kind != BRIDGE_STREAM_BIDI ||
        te->origin != BRIDGE_ORIGIN_PEER ||
        !te->local_send_closed || te->peer_send_closed ||
        te->pending_retry || te->pending_fin || te->fin_retained ||
        te->pending_reset || te->pending_stop ||
        te->peer_stop_received || te->aborting ||
        te->pending_reset_code != 0 || te->pending_stop_code != 0) {
        if (!nob_quiet)
            fprintf(stderr, "NOB %s: target mapping not in the declared"
                    " local-closed live state\n", what);
        return 1;
    }
    return 0;
}


/* No target semantic owner and the seed-only graph, by SAVED target ref. */
static int nob_check_target_gone(test_pair_t *tp, const nob_run_t *r,
                                 size_t want_recv_payload, const char *what)
{
    int bad = 0;
    if (tp->server->recv_payload_bytes != want_recv_payload) {
        fprintf(stderr, "NOB %s: recv_payload_bytes %zu, expected the"
                " pre-target %zu\n", what, tp->server->recv_payload_bytes,
                want_recv_payload);
        bad++;
    }
    if (request_registry_find_by_streamref(tp->server, r->target_ref).kind
        != MOQ_REQ_NONE) {
        fprintf(stderr, "NOB %s: target still has a registry owner\n", what);
        bad++;
    }
    {
        int busy = 0;
        for (size_t i = 0; i < tp->server->sub_cap; i++)
            if (tp->server->subs[i].state != MOQ_SUB_FREE) busy++;
        if (busy != 1) {
            fprintf(stderr, "NOB %s: pool occupancy %d, expected 1\n", what,
                    busy);
            bad++;
        }
    }
    og_graph_t g;
    og_capture(tp->server, &g);
    bad += og_check_integrity(&g, what);
    bad += og_check_edge(&g, OG_DOM_REQ_STREAMREF, r->seed_ref._v,
                         MOQ_REQ_SUBSCRIPTION, r->seed_slot, what);
    {
        const og_edge_spec_t w[] = { { OG_DOM_REQ_STREAMREF,
                                       r->seed_ref._v } };
        bad += og_check_owner_edges(&g, MOQ_REQ_SUBSCRIPTION, r->seed_slot,
                                    w, 1, what);
    }
    bad += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, r->target_ref._v, what);
    bad += og_check_no_edge(&g, OG_DOM_NS_REF, r->target_ref._v, what);
    /* The refused target commits no owner, so its DECLARED request id keys
     * nothing. The blocker committed inbound id 0, but a d18 stream-correlated
     * announcement holds no by-ID edge, so id 0 stays absent too. */
    bad += og_check_no_edge(&g, OG_DOM_REQ_RID, r->target_rid, what);
    if (r->target_rid != r->blk_rid)
        bad += og_check_no_edge(&g, OG_DOM_REQ_RID, r->blk_rid, what);
    /* Origin 4's live announcement blocker keeps its own stream-ref edge for the
     * whole target refusal/recovery (an outbound per-request GOAWAY retires only
     * on the peer's empty-FIN), so the seed-only graph gains exactly that edge. */
    size_t want_edges = 1;
    if (r->blk_present) {
        want_edges = 2;
        bad += og_check_edge(&g, OG_DOM_REQ_STREAMREF, r->blk_ref._v,
                             MOQ_REQ_ANNOUNCEMENT, r->blk_slot, what);
        const og_edge_spec_t bw[] = { { OG_DOM_REQ_STREAMREF,
                                        r->blk_ref._v } };
        bad += og_check_owner_edges(&g, MOQ_REQ_ANNOUNCEMENT, r->blk_slot,
                                    bw, 1, what);
    }
    if (g.edge_count != want_edges) {
        fprintf(stderr, "NOB %s: %zu graph edges, expected exactly %zu\n",
                what, g.edge_count, want_edges);
        bad++;
    }
    return bad;
}


/* Deep-copy the blocker's mutable byte state so a later in-place change is
 * caught. Identity lives in nob_run_t; this captures only what can drift. */
static int nob_blk_capture(test_pair_t *tp, const nob_run_t *r,
                           nob_blk_snap_t *snap)
{
    memset(snap, 0, sizeof(*snap));
    if (!r->blk_present) return 0;
    if (r->blk_slot < 0 || (size_t)r->blk_slot >= tp->server->ann_cap)
        return 1;
    const moq_ann_entry_t *e = &tp->server->announcements[r->blk_slot];
    snap->ns_len = e->ns_id_len;
    if (e->ns_id_len > sizeof(snap->ns_bytes))
        snap->ns_overflow = true;
    else if (e->ns_id_buf && e->ns_id_len)
        memcpy(snap->ns_bytes, e->ns_id_buf, e->ns_id_len);
    snap->rs.recv_cap = e->rs.recv_cap;
    snap->rs.recv_len = e->rs.recv_len;
    snap->rs.recv_fin = e->rs.recv_fin;
    snap->handoff_fin_pending = e->handoff_fin_pending;
    snap->rs.recv_buf = e->rs.recv_buf;
    return 0;
}


/* The whole live blocker, by DECLARED identity plus a conserved byte image and
 * physical mapping. Re-run at every phase: the target lifecycle must neither
 * mutate nor retire it. `blk_gen`/`blk_handle` are load-bearing -- a coherent
 * generation/handle/slot change fails here independently. */
static int nob_blk_check(test_pair_t *tp, const nob_run_t *r,
                         const nob_blk_snap_t *snap, const char *what)
{
    int bad = 0;
    if (!r->blk_present) return 0;
    if (r->blk_slot < 0 || (size_t)r->blk_slot >= tp->server->ann_cap) {
        fprintf(stderr, "NOB %s: blocker slot out of range\n", what);
        return bad + 1;
    }
    const moq_ann_entry_t *e = &tp->server->announcements[r->blk_slot];
    if ((int)e->state != (int)MOQ_ANN_ESTABLISHED ||
        (int)e->role != (int)MOQ_ANN_ROLE_RECEIVER ||
        e->generation != r->blk_gen ||
        e->handle._opaque != r->blk_handle ||
        e->request_id != r->blk_rid ||
        e->rs.ref._v != r->blk_ref._v ||
        !e->goaway_sent ||
        e->handoff_fin_pending != snap->handoff_fin_pending ||
        e->rs.recv_cap != snap->rs.recv_cap ||
        e->rs.recv_len != snap->rs.recv_len ||
        e->rs.recv_fin != snap->rs.recv_fin ||
        e->rs.recv_buf != snap->rs.recv_buf) {
        fprintf(stderr, "NOB %s: blocker owner record changed\n", what);
        bad++;
    }
    /* Canonical namespace bytes, deep-compared (bounds-safe). */
    if (snap->ns_overflow || e->ns_id_len > sizeof(snap->ns_bytes) ||
        e->ns_id_len != snap->ns_len ||
        (e->ns_id_len && (!e->ns_id_buf ||
            memcmp(e->ns_id_buf, snap->ns_bytes, e->ns_id_len) != 0))) {
        fprintf(stderr, "NOB %s: blocker namespace bytes changed\n", what);
        bad++;
    }
    /* The stream-ref registry edge still targets the declared announcement. */
    moq_request_endpoint_t ep =
        request_registry_find_by_streamref(tp->server, r->blk_ref);
    if (ep.kind != MOQ_REQ_ANNOUNCEMENT || ep.slot != r->blk_slot ||
        !ep.has_stream_ref || ep.stream_ref._v != r->blk_ref._v) {
        fprintf(stderr, "NOB %s: blocker registry answer changed\n", what);
        bad++;
    }
    /* The physical bridge mapping, by BOTH identities. */
    bridge_stream_entry_t *by_id = bridge_find_by_id(tp->server_bridge,
                                                     r->blk_id);
    bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp->server_bridge,
                                                       r->blk_ref);
    if (!by_id || by_id != by_ref || !by_id->active ||
        by_id->ref._v != r->blk_ref._v ||
        by_id->transport_id != r->blk_id ||
        by_id->kind != BRIDGE_STREAM_BIDI ||
        by_id->origin != BRIDGE_ORIGIN_PEER ||
        by_id->peer_send_closed || by_id->local_send_closed ||
        by_id->pending_retry || by_id->pending_fin || by_id->fin_retained ||
        by_id->pending_reset || by_id->pending_stop ||
        by_id->peer_stop_received || by_id->aborting ||
        by_id->pending_reset_code != 0 || by_id->pending_stop_code != 0) {
        fprintf(stderr, "NOB %s: blocker bridge mapping changed\n", what);
        bad++;
    }
    return bad;
}


/* The encoded target request, decoded pre-ingress and compared against the
 * DECLARED case values (draft-18 §10.1) -- an independent wire-legality oracle,
 * separate from the encoder that produced the bytes: one complete SUBSCRIBE
 * envelope, no trailing bytes, a decoded body carrying the declared Request ID,
 * the fixture namespace "nob" / track "t", and the empty/default parameter image
 * the fixture asks for (prm = {0}). Every borrowed span is bounds/NULL-guarded. */
static int nob_check_target_wire(const uint8_t *data, size_t len,
                                 uint64_t want_rid, const char *what)
{
    int bad = 0;
    if (!data || len == 0) {
        fprintf(stderr, "FAIL: nob %s: no target bytes\n", what);
        return 1;
    }
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);
    moq_control_envelope_t env;
    memset(&env, 0, sizeof(env));
    if (moq_d18_decode_envelope(&r, &env) != MOQ_OK) {
        fprintf(stderr, "FAIL: nob %s: target is not a decodable envelope\n",
                what);
        return 1;
    }
    if (env.msg_type != MOQ_D18_SUBSCRIBE) {
        fprintf(stderr, "FAIL: nob %s: target msg type 0x%llx, expected"
                " SUBSCRIBE\n", what, (unsigned long long)env.msg_type);
        bad++;
    }
    if (moq_buf_reader_remaining(&r) != 0) {
        fprintf(stderr, "FAIL: nob %s: %zu trailing bytes after the target\n",
                what, moq_buf_reader_remaining(&r));
        bad++;
    }
    moq_bytes_t parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
    moq_d18_subscribe_t sub;
    memset(&sub, 0, sizeof(sub));
    if (moq_d18_decode_subscribe(env.payload, env.payload_len, parts,
                                 MOQ_DECODED_MAX_NAMESPACE_PARTS, &sub)
        != MOQ_OK) {
        fprintf(stderr, "FAIL: nob %s: target SUBSCRIBE body did not decode\n",
                what);
        return bad + 1;
    }
    if (sub.request_id != want_rid) {
        fprintf(stderr, "FAIL: nob %s: target request id %llu, declared %llu\n",
                what, (unsigned long long)sub.request_id,
                (unsigned long long)want_rid);
        bad++;
    }
    if (sub.track_namespace.count != 1 || sub.track_namespace.parts == NULL ||
        sub.track_namespace.parts[0].len != 3 ||
        sub.track_namespace.parts[0].data == NULL ||
        memcmp(sub.track_namespace.parts[0].data, "nob", 3) != 0) {
        fprintf(stderr, "FAIL: nob %s: target namespace differs from 'nob'\n",
                what);
        bad++;
    }
    if (sub.track_name.len != 1 || sub.track_name.data == NULL ||
        sub.track_name.data[0] != 't') {
        fprintf(stderr, "FAIL: nob %s: target track name differs from 't'\n",
                what);
        bad++;
    }
    if (sub.params.has_forward || sub.params.has_subscriber_priority ||
        sub.params.has_group_order || sub.params.has_filter ||
        sub.params.has_expires || sub.params.has_largest ||
        sub.params.has_object_delivery_timeout ||
        sub.params.has_subgroup_delivery_timeout ||
        sub.params.has_new_group_request) {
        fprintf(stderr, "FAIL: nob %s: target carries an unexpected"
                " parameter\n", what);
        bad++;
    }
    return bad;
}


/* The DECLARED key a legal added blocked-graph edge must carry: a request-ID
 * edge keys the target's declared request id; every other domain keys the saved
 * target ref. Identity-qualified checks on (kind, slot) run separately. */
static uint64_t nob_want_edge_key(int domain, uint64_t target_rid,
                                  uint64_t target_ref)
{
    return (domain == OG_DOM_REQ_RID) ? target_rid : target_ref;
}


/* The REAL blocked-graph added-edge allowance, factored so the live nob run and
 * the selfcheck exercise the same code: a legal added edge keys the declared id
 * (target_rid for request-ID, target_ref otherwise) AND its own (kind, slot)
 * must resolve to a live subscription owner whose request-stream identity IS the
 * target. Returns the number of failures for one added edge. */
static int nob_added_edge_ok(const moq_session_t *server, const og_edge_t *f,
                             uint64_t target_rid, uint64_t target_ref,
                             const char *what)
{
    uint64_t want_key = nob_want_edge_key(f->domain, target_rid, target_ref);
    if (f->key != want_key) {
        if (!og_quiet)
            fprintf(stderr, "NOB %s: added edge (%d key %llu) does not key the"
                    " target\n", what, (int)f->domain,
                    (unsigned long long)f->key);
        return 1;
    }
    if (f->kind == MOQ_REQ_SUBSCRIPTION) {
        if (f->slot < 0 || (size_t)f->slot >= server->sub_cap ||
            server->subs[f->slot].state == MOQ_SUB_FREE ||
            server->subs[f->slot].rs.ref._v != target_ref) {
            if (!og_quiet)
                fprintf(stderr, "NOB %s: added edge's owner does not carry the"
                        " target identity\n", what);
            return 1;
        }
        return 0;
    }
    if (!og_quiet)
        fprintf(stderr, "NOB %s: added edge has unsupported kind %d\n", what,
                f->kind);
    return 1;
}


/* Drive the REAL blocked-graph allowance (nob_added_edge_ok, the same helper the
 * live nob run calls) against a live server subscription, so acceptance and
 * rejection are proven through the actual owner-identity resolution rather than
 * through the pure nob_want_edge_key() return alone. A staged inbound SUBSCRIBE
 * gives a genuine subs[] entry whose request_stream_ref the helper must match.
 *
 * The DECLARED target request id the helper keys on (its target_rid parameter)
 * is independent of the staged owner's own protocol request id: the helper never
 * reads the owner's id, only its request_stream_ref. So the owner is admitted at
 * protocol id 0 (the next-in-sequence inbound id) while the helper is driven at a
 * DECLARED target id of 2 -- the origin-4 case's real value -- proving the
 * request-ID allowance keys on the declared id, not a hardcoded 0.
 *
 * Synthetic og_edge_t values probe every branch, for BOTH declared id 2 and id 0:
 *   - a request-ID edge keyed on the declared target id and resolving to the live
 *     owner is ACCEPTED; keyed on any other id is REJECTED;
 *   - a stream-ref edge keyed on the target ref is ACCEPTED; keyed on a foreign
 *     ref is REJECTED;
 *   - an otherwise-legal edge whose slot resolves to an owner NOT carrying the
 *     target ref is REJECTED by identity even when the key matches;
 *   - a free slot and an unsupported kind are both REJECTED. */
static int nob_rid_allowance_selfcheck(void)
{
    int failures = 0;

    /* The pure key selector, retained as the first-line contract. */
    {
        const uint64_t ref = 0xBEEF;
        MOQ_TEST_CHECK_EQ_U64(nob_want_edge_key(OG_DOM_REQ_RID, 2, ref),
                              (uint64_t)2);
        MOQ_TEST_CHECK_EQ_U64(nob_want_edge_key(OG_DOM_REQ_RID, 0, ref),
                              (uint64_t)0);
        MOQ_TEST_CHECK_EQ_U64(nob_want_edge_key(OG_DOM_REQ_STREAMREF, 2, ref),
                              ref);
        MOQ_TEST_CHECK_EQ_U64(nob_want_edge_key(OG_DOM_REQ_STREAMREF, 0, ref),
                              ref);
    }

    test_pair_t tp;
    if (d18_pair_init(&tp, 1) < 0) return failures + 1;
    if (moq_session_start(tp.client, 0) != MOQ_OK ||
        moq_session_start(tp.server, 0) != MOQ_OK) {
        test_pair_destroy(&tp);
        return failures + 1;
    }
    failures += d18_strict_shuttle(&tp, 30, 0, "nob-selfcheck");
    if (tp.server->state != MOQ_SESS_ESTABLISHED) {
        test_pair_destroy(&tp);
        return failures + 1;
    }
    {
        moq_event_t ev;
        while (moq_session_poll_events(tp.server, &ev, 1) > 0)
            moq_event_cleanup(&ev);
        while (moq_session_poll_events(tp.client, &ev, 1) > 0)
            moq_event_cleanup(&ev);
    }
    fake_endpoint_clear_ops(&tp.client_ep);
    fake_endpoint_clear_ops(&tp.server_ep);

    /* A complete inbound SUBSCRIBE stages a live owner at the next-in-sequence
     * protocol request id (0). The helper never reads that id -- see the driven
     * declared id below. */
    const uint64_t owner_rid = 0;
    const uint64_t owner_bidi_id = 6060;
    uint8_t sub[128];
    size_t sub_len;
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, sub, sizeof(sub));
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("nob") };
        moq_namespace_t ns = { parts, 1 };
        moq_d18_msg_params_t prm = { 0 };
        if (moq_d18_encode_subscribe(&w, owner_rid, &ns,
                                     MOQ_BYTES_LITERAL("t"), &prm) != MOQ_OK) {
            test_pair_destroy(&tp);
            return failures + 1;
        }
        sub_len = moq_buf_writer_offset(&w);
    }
    if (moq_transport_bridge_on_peer_bidi_bytes(tp.server_bridge, owner_bidi_id,
                                                sub, sub_len, false, 0)
            != MOQ_OK ||
        moq_transport_bridge_is_fatal(tp.server_bridge)) {
        test_pair_destroy(&tp);
        return failures + 1;
    }

    /* The staged owner's stream ref, taken while the bridge mapping is live. */
    moq_stream_ref_t owner_ref;
    {
        bridge_stream_entry_t *oe = bridge_find_by_id(tp.server_bridge,
                                                      owner_bidi_id);
        if (!oe) { test_pair_destroy(&tp); return failures + 1; }
        owner_ref = oe->ref;
    }
    /* Locate the subs[] slot the helper will index. */
    int owner_slot = -1;
    for (size_t i = 0; i < tp.server->sub_cap; i++)
        if (tp.server->subs[i].state != MOQ_SUB_FREE &&
            tp.server->subs[i].rs.ref._v == owner_ref._v) {
            owner_slot = (int)i;
            break;
        }
    MOQ_TEST_CHECK(owner_slot >= 0);
    MOQ_TEST_CHECK(owner_ref._v != 0);
    if (owner_slot < 0) { test_pair_destroy(&tp); return failures + 1; }

    /* A free slot for the free-slot rejection probe -- required present, so the
     * negative below runs unconditionally rather than being silently skipped. */
    int free_slot = -1;
    for (size_t i = 0; i < tp.server->sub_cap; i++)
        if (tp.server->subs[i].state == MOQ_SUB_FREE) {
            free_slot = (int)i;
            break;
        }
    MOQ_TEST_CHECK(free_slot >= 0);
    if (free_slot < 0) { test_pair_destroy(&tp); return failures + 1; }

    /* The declared target id the helper keys on -- driven at 2 (the origin-4
     * value), independent of the owner's protocol id 0. */
    const uint64_t DECL_RID = 2;
    const uint64_t foreign_ref = owner_ref._v ^ 0x5A5Au;

    /* ACCEPTANCE (declared id 2): a request-ID edge keyed on the declared target
     * id and resolving to the live owner, and a stream-ref edge keyed on the
     * owner ref (which ignores the declared id). */
    {
        og_edge_t e = { .domain = OG_DOM_REQ_RID, .key = DECL_RID,
                        .kind = MOQ_REQ_SUBSCRIPTION, .slot = owner_slot };
        MOQ_TEST_CHECK_EQ_INT(
            nob_added_edge_ok(tp.server, &e, DECL_RID, owner_ref._v, "sc"), 0);
    }
    {
        og_edge_t e = { .domain = OG_DOM_REQ_STREAMREF, .key = owner_ref._v,
                        .kind = MOQ_REQ_SUBSCRIPTION, .slot = owner_slot };
        MOQ_TEST_CHECK_EQ_INT(
            nob_added_edge_ok(tp.server, &e, DECL_RID, owner_ref._v, "sc"), 0);
    }
    /* Symmetric id-0 positive: keyed on declared id 0, same owner. */
    {
        og_edge_t e = { .domain = OG_DOM_REQ_RID, .key = 0,
                        .kind = MOQ_REQ_SUBSCRIPTION, .slot = owner_slot };
        MOQ_TEST_CHECK_EQ_INT(
            nob_added_edge_ok(tp.server, &e, 0, owner_ref._v, "sc"), 0);
    }

    /* REJECTIONS: exercised with diagnostics silenced so a passing run is
     * quiet; each must be reported as exactly one failure by the helper. */
    {
        int saved = og_quiet;
        og_quiet = 1;
        /* Wrong request id: key 0 against the declared target id 2. */
        {
            og_edge_t e = { .domain = OG_DOM_REQ_RID, .key = 0,
                        .kind = MOQ_REQ_SUBSCRIPTION, .slot = owner_slot };
            MOQ_TEST_CHECK_EQ_INT(
                nob_added_edge_ok(tp.server, &e, DECL_RID, owner_ref._v, "sc"),
                1);
        }
        /* Wrong request id the other way: key 2 against the declared id 0. */
        {
            og_edge_t e = { .domain = OG_DOM_REQ_RID, .key = DECL_RID,
                        .kind = MOQ_REQ_SUBSCRIPTION, .slot = owner_slot };
            MOQ_TEST_CHECK_EQ_INT(
                nob_added_edge_ok(tp.server, &e, 0, owner_ref._v, "sc"), 1);
        }
        /* Wrong stream ref key. */
        {
            og_edge_t e = { .domain = OG_DOM_REQ_STREAMREF, .key = foreign_ref,
                            .kind = MOQ_REQ_SUBSCRIPTION, .slot = owner_slot };
            MOQ_TEST_CHECK_EQ_INT(
                nob_added_edge_ok(tp.server, &e, DECL_RID, owner_ref._v, "sc"),
                1);
        }
        /* Right key, wrong identity: key 2 matches the declared id 2, but the
         * owner does not carry this target ref. */
        {
            og_edge_t e = { .domain = OG_DOM_REQ_RID, .key = DECL_RID,
                        .kind = MOQ_REQ_SUBSCRIPTION, .slot = owner_slot };
            MOQ_TEST_CHECK_EQ_INT(
                nob_added_edge_ok(tp.server, &e, DECL_RID, foreign_ref, "sc"),
                1);
        }
        /* Unsupported kind. */
        {
            og_edge_t e = { .domain = OG_DOM_REQ_RID, .key = DECL_RID,
                        .kind = MOQ_REQ_ANNOUNCEMENT, .slot = owner_slot };
            MOQ_TEST_CHECK_EQ_INT(
                nob_added_edge_ok(tp.server, &e, DECL_RID, owner_ref._v, "sc"),
                1);
        }
        /* Free slot: key matches the declared id but the slot resolves to no
         * owner. Run unconditionally (free_slot required present above). */
        {
            og_edge_t e = { .domain = OG_DOM_REQ_RID, .key = DECL_RID,
                        .kind = MOQ_REQ_SUBSCRIPTION, .slot = free_slot };
            MOQ_TEST_CHECK_EQ_INT(
                nob_added_edge_ok(tp.server, &e, DECL_RID, owner_ref._v, "sc"),
                1);
        }
        og_quiet = saved;
    }

    test_pair_destroy(&tp);
    return failures;
}


/* The no-slot carrier state for a ref: -1 absent, 0 present (no FIN), 1 present
 * (FIN). Used to prove the PRODUCT installs/retires the target carrier through
 * real refusal/recovery, not through a direct helper call. */
static int nob_carrier_state(const moq_session_t *s, moq_stream_ref_t ref)
{
    int i = noslot_carrier_find(s, ref);
    if (i < 0) return -1;
    return s->noslot_carriers[i].fin ? 1 : 0;
}


/* No event on EITHER session: the terminal owes none. */
static int nob_check_no_events(test_pair_t *tp, const char *what)
{
    int bad = 0, n = 0;
    moq_event_t ev;
    while (moq_session_poll_events(tp->server, &ev, 1) > 0) {
        fprintf(stderr, "NOB %s: server event kind %u\n", what,
                (unsigned)ev.kind);
        n++; moq_event_cleanup(&ev);
    }
    while (moq_session_poll_events(tp->client, &ev, 1) > 0) {
        fprintf(stderr, "NOB %s: client event kind %u\n", what,
                (unsigned)ev.kind);
        n++; moq_event_cleanup(&ev);
    }
    if (n) bad++;
    return bad;
}


/* The bridge completion-session helper must itself discriminate: it accepts
 * the exact derived state and rejects an unrelated event_scratch_len change
 * and a non-CLOSED session-state change. Quiet: only failures print. The nob
 * deep phases that consume this helper rely on the no-slot carrier retaining
 * the ingress; this probe additionally pins the helper's own discrimination. */
static int nob_session_selfcheck(void)
{
    int failures = 0;
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    cfg.version = MOQ_VERSION_DRAFT_18;
    moq_session_t *sv = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &sv), (int)MOQ_OK);
    if (!sv) return failures + 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(sv, 0), (int)MOQ_OK);
    moq_action_t a;
    while (moq_session_poll_actions(sv, &a, 1) > 0) moq_action_cleanup(&a);
    moq_stream_ref_t none = moq_stream_ref_from_u64(0x9999);
    txs_snapshot_t before;
    txs_capture(sv, &none, 1, &before);
    nf_expect_after_call_prepare(&before);
    /* Exact derived state: accepted. */
    MOQ_TEST_CHECK_EQ_INT(
        nob_check_session(sv, none, &before, 0, "selfcheck-exact"), 0);
    /* An unrelated scratch change: rejected. */
    sv->event_scratch_len += 8;
    txs_quiet = 1;
    MOQ_TEST_CHECK(
        nob_check_session(sv, none, &before, 0, "selfcheck-scratch") > 0);
    txs_quiet = 0;
    sv->event_scratch_len -= 8;
    /* A non-CLOSED session-state change: rejected. */
    {
        moq_session_state_t saved = sv->state;
        sv->state = MOQ_SESS_DRAINING;
        txs_quiet = 1;
        MOQ_TEST_CHECK(
            nob_check_session(sv, none, &before, 0, "selfcheck-state") > 0);
        txs_quiet = 0;
        sv->state = saved;
    }
    moq_session_destroy(sv);
    return failures;
}


/* The COMPLETE bridge phase postcondition, applied after EVERY advancing
 * call: the generic session layer, the full seed contract, semantic target
 * retirement with the exact graph, the exact declared ring, the bridge
 * nonfatal/open/not-pending, no event on either session, and the target's
 * exact physical state by both saved identities. A later check can therefore
 * never adopt or erase a transient mutation made by service. */
static int nob_check_phase(test_pair_t *tp, const nob_run_t *r,
                           const txs_snapshot_t *before,
                           const nob_ring_t *ring, int live_mapping,
                           const char *what)
{
    int bad = 0;
    bad += nob_check_session(tp->server, r->seed_ref, before, ring->count,
                             what);
    bad += nob_check_seed(tp, r, what);
    bad += nob_check_target_gone(tp, r, before->recv_payload_bytes, what);
    {
        nob_ring_t now;
        nob_ring_snap(tp->server, &now);
        bad += nob_ring_equals(&now, ring, what);
    }
    if (moq_transport_bridge_is_fatal(tp->server_bridge) ||
        moq_transport_bridge_is_closed(tp->server_bridge) ||
        moq_transport_bridge_has_pending(tp->server_bridge)) {
        fprintf(stderr, "NOB %s: bridge fatal/closed/pending\n", what);
        bad++;
    }
    bad += nob_check_no_events(tp, what);
    bad += nob_check_target_phys(tp, r, live_mapping, what);
    return bad;
}


/* The physical-state helper must discriminate: against a real constructed live
 * mapping, the retired shape rejects it, the declared live shape accepts it,
 * and a flipped flag rejects it again. Quiet: only failures print. */
static int nob_phys_selfcheck(void)
{
    int failures = 0;
    test_pair_t tp;
    if (d18_pair_init_caps(&tp, 0, moq_alloc_default(), moq_alloc_default(),
                           0, false, 0) < 0)
        return failures + 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(tp.client, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(tp.server, 0), (int)MOQ_OK);
    static const uint8_t frag[4] = { 0x02, 0x01, 0x01, 0x01 };
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_bidi_bytes(tp.server_bridge, 500,
                                                     frag, sizeof(frag),
                                                     false, 0),
        (int)MOQ_OK);
    bridge_stream_entry_t *e = bridge_find_by_id(tp.server_bridge, 500);
    MOQ_TEST_CHECK(e != NULL);
    if (!e) { test_pair_destroy(&tp); return failures + 1; }
    nob_run_t r2;
    memset(&r2, 0, sizeof(r2));
    r2.target_id = 500;
    r2.target_ref = e->ref;
    /* A live mapping must fail the RETIRED shape. */
    nob_quiet = 1;
    MOQ_TEST_CHECK(nob_check_target_phys(&tp, &r2, 0, "phys-retired") > 0);
    nob_quiet = 0;
    /* The exact declared live shape is accepted... */
    e->local_send_closed = true;
    MOQ_TEST_CHECK_EQ_INT(nob_check_target_phys(&tp, &r2, 1, "phys-live"), 0);
    /* ...a single flipped pending-family flag rejects it... */
    e->aborting = true;
    nob_quiet = 1;
    MOQ_TEST_CHECK(nob_check_target_phys(&tp, &r2, 1, "phys-flag") > 0);
    nob_quiet = 0;
    e->aborting = false;
    /* ...and the STOP terminal fact rejects it INDEPENDENTLY: only
     * peer_stop_received flipped, everything else in the accepted shape. */
    e->peer_stop_received = true;
    nob_quiet = 1;
    MOQ_TEST_CHECK(nob_check_target_phys(&tp, &r2, 1, "phys-stop") > 0);
    nob_quiet = 0;
    e->peer_stop_received = false;
    MOQ_TEST_CHECK_EQ_INT(nob_check_target_phys(&tp, &r2, 1,
                                                "phys-live-again"), 0);
    e->local_send_closed = false;
    test_pair_destroy(&tp);
    return failures;
}


static int run_nob_case(const nob_case_t *c)
{
    int failures = 0;
    char what[96];
    snprintf(what, sizeof(what), "nob %s", c->name);

    test_pair_t tp;
    uint32_t max_actions = 0;
    if (c->origin == 2) max_actions = 2;
    if (c->origin == 3) max_actions = 1;
    if (d18_pair_init_caps(&tp, 0, moq_alloc_default(), moq_alloc_default(),
                           max_actions, false, 0) < 0)
        return failures + 1;
    /* The server needs a ONE-slot subscription pool, which is a create-time
     * config: rebuild the server side of the pair with it (plus the origin's
     * own capacity shape) before any traffic. */
    {
        moq_session_cfg_t scfg;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_SERVER);
        scfg.version = MOQ_VERSION_DRAFT_18;
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 10;
        scfg.max_subscriptions = 1;
        if (max_actions) scfg.max_actions = max_actions;
        if (c->origin == 4) scfg.send_buffer_size = 64;
        moq_session_t *sv = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&scfg, 0, &sv),
                              (int)MOQ_OK);
        if (!sv) { test_pair_destroy(&tp); return failures + 1; }
        held_bridge_destroy(tp.server_bridge);
        moq_session_destroy(tp.server);
        tp.server = sv;
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
        HELD_REQUIRE_EQ_INT(held_bridge_create(&bcfg, tp.server,
                                             &tp.server_ep.vtable,
                                             &tp.server_ep,
                                             &tp.server_bridge), MOQ_OK);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(tp.client, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(tp.server, 0), (int)MOQ_OK);
    if (!c->pre_setup) {
        failures += d18_strict_shuttle(&tp, 30, 0, what);
        MOQ_TEST_CHECK_EQ_INT((int)tp.server->state,
                              (int)MOQ_SESS_ESTABLISHED);
        moq_event_t ev;
        int ss = 0, so = 0, cs = 0, co = 0;
        while (moq_session_poll_events(tp.server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) ss++; else so++;
            moq_event_cleanup(&ev);
        }
        while (moq_session_poll_events(tp.client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) cs++; else co++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(ss, 1);
        MOQ_TEST_CHECK_EQ_INT(so, 0);
        MOQ_TEST_CHECK_EQ_INT(cs, 1);
        MOQ_TEST_CHECK_EQ_INT(co, 0);
    } else {
        /* Pre-SETUP: a draft-18 endpoint opens its own local control uni and
         * emits SETUP eagerly at start (§10.3). Service the SERVER bridge to
         * flush exactly that local output -- WITHOUT delivering the peer SETUP,
         * so the server stays pre-established and the target still reaches the
         * defer_dispatch STOP+RESET route -- then classify it exactly rather
         * than let it leak into the blocker/target phases. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
        /* Exactly one local uni open followed by one non-FIN control write on
         * the same transport id, and nothing else. */
        MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)2);
        if (tp.server_ep.count == 2) {
            const fake_op_t *o0 = &tp.server_ep.ops[0];
            const fake_op_t *o1 = &tp.server_ep.ops[1];
            MOQ_TEST_CHECK_EQ_INT((int)o0->kind, (int)FAKE_OP_OPEN_UNI);
            MOQ_TEST_CHECK_EQ_INT(o0->fin ? 1 : 0, 0);
            MOQ_TEST_CHECK_EQ_INT((int)o1->kind, (int)FAKE_OP_WRITE);
            MOQ_TEST_CHECK_EQ_U64(o1->stream_id, o0->stream_id);
            MOQ_TEST_CHECK_EQ_INT(o1->fin ? 1 : 0, 0);
            MOQ_TEST_CHECK(o1->data_len > 0);
            /* The write is a decodable draft-18 SETUP envelope with no trailing
             * bytes -- the local control output, not junk -- AND its body decodes
             * to exactly the fixture's declared defaults. This fixture leaves
             * send_auth_token_cache_size at 0, so SETUP carries no Setup Options
             * (§10.3): no PATH, no AUTHORITY, no MAX_AUTH_TOKEN_CACHE_SIZE, and no
             * auth tokens. The body decode is what distinguishes a correct empty
             * SETUP from one that silently gained an option. */
            if (o1->data_len > 0 && o1->data_len <= sizeof(o1->data)) {
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, o1->data, o1->data_len);
                moq_control_envelope_t env;
                memset(&env, 0, sizeof(env));
                if (moq_d18_decode_envelope(&rr, &env) != MOQ_OK) {
                    fprintf(stderr, "NOB %s: SETUP envelope did not decode\n",
                            what);
                    failures++;
                } else {
                    MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                          (uint64_t)MOQ_D18_STREAM_SETUP);
                    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                           (size_t)0);
                    moq_d18_setup_opts_t opts;
                    moq_d18_setup_opts_init(&opts);
                    if (moq_d18_decode_setup_opts(env.payload, env.payload_len,
                                                  &opts, sizeof(opts)) != MOQ_OK) {
                        fprintf(stderr, "NOB %s: SETUP body did not decode\n",
                                what);
                        failures++;
                    } else {
                        MOQ_TEST_CHECK_EQ_INT(opts.has_path ? 1 : 0, 0);
                        MOQ_TEST_CHECK_EQ_INT(opts.has_authority ? 1 : 0, 0);
                        MOQ_TEST_CHECK_EQ_INT(
                            opts.has_max_auth_token_cache_size ? 1 : 0, 0);
                        MOQ_TEST_CHECK_EQ_SIZE(opts.auth_token_count, (size_t)0);
                    }
                }
            }
        }
        /* The server has NOT seen the peer SETUP: still pre-established, so the
         * target ingress selects the STOP+RESET terminal, not a live stream. */
        MOQ_TEST_CHECK(tp.server->state != MOQ_SESS_ESTABLISHED);
        /* The local SETUP flush leaves no residual outbound work: the action
         * ring and the bridge outbound-pending queue are both empty, so the
         * blocked-point and recovery oracles measure only the retained
         * admission state. */
        MOQ_TEST_CHECK_EQ_SIZE(
            tp.server->action_tail - tp.server->action_head, (size_t)0);
        MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.server_bridge));
    }
    fake_endpoint_clear_ops(&tp.client_ep);
    fake_endpoint_clear_ops(&tp.server_ep);

    nob_run_t r;
    memset(&r, 0, sizeof(r));
    r.seed_id = 400;
    r.target_id = 404;
    /* The target Request ID comes from the case declaration, checked by the
     * descriptor self-check -- never from a local ternary that also feeds the
     * encoder (a symmetric producer+oracle mutation would go unnoticed). */
    failures += nob_case_target_rid_ok(c);
    r.target_rid = c->target_rid;
    uint64_t target_rid = r.target_rid;
    nob_blk_snap_t blk_snap;
    memset(&blk_snap, 0, sizeof(blk_snap));

    /* Origin 4 (send-buffer blocker): established BEFORE the seed. A live
     * RECEIVER-side announcement is admitted through the real request route on
     * transport id 700, then a nonterminal per-request GOAWAY (§10.4) whose New
     * Session URI is sized to leave less room than the target REQUEST_ERROR needs
     * fills the send buffer. The announcement stages through the single
     * subscription slot and, on acceptance, re-keys into the announcement pool
     * and FREES that slot -- so the fragmented seed can occupy it next and the
     * target still faces an exhausted pool. Accepting the announcement commits
     * inbound request id 0, so the target is the next-in-sequence id 2. The
     * GOAWAY is a genuine queued request-bidi write (not a raw control blob or a
     * direct send_len poke) and it leaves the announcement owner LIVE (an
     * outbound migration retires only on the peer's empty-FIN), so it is part of
     * the declared inventory. */
    if (c->origin == 4) {
        /* DECLARE the blocker identity from the free announcement pool entry,
         * BEFORE ingress: slot, next live generation, packed handle, request id,
         * transport id. The event/registry answers are checked AGAINST these --
         * never used to define the expectation they are compared to. */
        r.blk_present = true;
        r.blk_id = 700;
        r.blk_rid = 0;
        /* The declared target id is the announcement's committed id 0 plus 2
         * (§10.1 next-in-sequence). */
        MOQ_TEST_CHECK_EQ_U64(r.target_rid, (uint64_t)2);
        MOQ_TEST_CHECK_EQ_U64(r.target_rid, r.blk_rid + 2);
        r.blk_slot = -1;
        for (size_t i = 0; i < tp.server->ann_cap; i++)
            if (tp.server->announcements[i].state == MOQ_ANN_FREE) {
                r.blk_slot = (int)i;
                r.blk_gen = tp.server->announcements[i].generation | 1u;
                break;
            }
        MOQ_TEST_CHECK_EQ_INT(r.blk_slot, 0);
        r.blk_handle = moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT,
                                       tp.server->session_tag, r.blk_gen,
                                       (uint32_t)(r.blk_slot < 0 ? 0 : r.blk_slot));
        MOQ_TEST_CHECK(r.blk_handle != 0);

        uint8_t pn[128];
        size_t pn_len;
        {
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, pn, sizeof(pn));
            moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("blk") };
            moq_namespace_t ns = { parts, 1 };
            moq_d18_msg_params_t mp = { 0 };
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_encode_publish_namespace(&w, r.blk_rid, &ns, &mp),
                (int)MOQ_OK);
            pn_len = moq_buf_writer_offset(&w);
        }
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_bidi_bytes(
                tp.server_bridge, r.blk_id, pn, pn_len, false, 0), (int)MOQ_OK);

        /* Exactly one NAMESPACE_PUBLISHED, zero others, carrying the DECLARED
         * handle plus the fixture namespace and (empty) token image. */
        moq_announcement_t ah;
        memset(&ah, 0, sizeof(ah));
        {
            moq_event_t ev;
            int pub = 0, other = 0;
            while (moq_session_poll_events(tp.server, &ev, 1) > 0) {
                if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                    pub++;
                    ah = ev.u.namespace_published.ann;
                    MOQ_TEST_CHECK_EQ_U64(ah._opaque, r.blk_handle);
                    const moq_namespace_published_event_t *np =
                        &ev.u.namespace_published;
                    MOQ_TEST_CHECK_EQ_SIZE(np->track_namespace.count, (size_t)1);
                    if (np->track_namespace.count == 1) {
                        MOQ_TEST_CHECK_EQ_SIZE(np->track_namespace.parts[0].len,
                                               (size_t)3);
                        MOQ_TEST_CHECK(np->track_namespace.parts[0].data &&
                            memcmp(np->track_namespace.parts[0].data, "blk", 3)
                                == 0);
                    }
                    MOQ_TEST_CHECK_EQ_SIZE(np->token_count, (size_t)0);
                } else {
                    other++;
                }
                moq_event_cleanup(&ev);
            }
            MOQ_TEST_CHECK_EQ_INT(pub, 1);
            MOQ_TEST_CHECK_EQ_INT(other, 0);
        }
        MOQ_TEST_CHECK_EQ_U64(ah._opaque, r.blk_handle);

        /* Capture the eventual internal ref while mapped; blk_ref != 0, blk_ref
         * != blk_id, both bridge lookups resolve to the same active peer-origin
         * BIDI, and the stream-ref edge targets the declared announcement slot. */
        {
            bridge_stream_entry_t *by_id =
                bridge_find_by_id(tp.server_bridge, r.blk_id);
            MOQ_TEST_CHECK(by_id != NULL);
            r.blk_ref = by_id ? by_id->ref : moq_stream_ref_from_u64(0);
            MOQ_TEST_CHECK(r.blk_ref._v != 0);
            MOQ_TEST_CHECK(r.blk_ref._v != r.blk_id);
            MOQ_TEST_CHECK(bridge_find_by_ref(tp.server_bridge, r.blk_ref)
                           == by_id);
            moq_request_endpoint_t bep =
                request_registry_find_by_streamref(tp.server, r.blk_ref);
            MOQ_TEST_CHECK(bep.kind == MOQ_REQ_ANNOUNCEMENT);
            MOQ_TEST_CHECK_EQ_INT(bep.slot, r.blk_slot);
        }

        /* Accept the announcement, then CLASSIFY the acceptance exactly: zero
         * events, one non-FIN WRITE on the blocker id decoding to one complete
         * REQUEST_OK envelope/body with no trailing bytes, no other endpoint op,
         * and no pending bridge/action work afterwards. */
        fake_endpoint_clear_ops(&tp.server_ep);
        moq_accept_namespace_cfg_t ac;
        moq_accept_namespace_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_namespace(tp.server, ah, &ac, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
        {
            int nev = 0;
            moq_event_t ev;
            while (moq_session_poll_events(tp.server, &ev, 1) > 0) {
                nev++;
                moq_event_cleanup(&ev);
            }
            MOQ_TEST_CHECK_EQ_INT(nev, 0);
        }
        MOQ_TEST_CHECK(tp.server_ep.count < FAKE_EP_MAX_OPS);
        MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)1);
        if (tp.server_ep.count == 1) {
            const fake_op_t *o = &tp.server_ep.ops[0];
            MOQ_TEST_CHECK_EQ_INT((int)o->kind, (int)FAKE_OP_WRITE);
            MOQ_TEST_CHECK_EQ_U64(o->stream_id, r.blk_id);
            MOQ_TEST_CHECK_EQ_INT(o->fin ? 1 : 0, 0);
            if (o->data_len > 0 && o->data_len <= sizeof(o->data)) {
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, o->data, o->data_len);
                moq_control_envelope_t env;
                memset(&env, 0, sizeof(env));
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_envelope(&rr, &env), (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr), (size_t)0);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_REQUEST_OK);
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_request_ok(env.payload, env.payload_len),
                    (int)MOQ_OK);
            }
        }
        MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.server_bridge));
        MOQ_TEST_CHECK_EQ_SIZE(
            tp.server->action_tail - tp.server->action_head, (size_t)0);
        fake_endpoint_clear_ops(&tp.server_ep);

        /* Only then queue the per-request GOAWAY (fills the send buffer). */
        {
            moq_request_goaway_cfg_t gc;
            moq_request_goaway_cfg_init(&gc);
            gc.new_session_uri.data = nob_goaway_uri;
            gc.new_session_uri.len = sizeof(nob_goaway_uri);
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_request_goaway_namespace(tp.server, ah, &gc, 0),
                (int)MOQ_OK);
        }
        MOQ_TEST_CHECK(tp.server->send_cap - tp.server->send_len < 24);
        MOQ_TEST_CHECK(tp.server->state == MOQ_SESS_ESTABLISHED);

        /* The conserved blocker image, from AFTER the GOAWAY. Re-checked at every
         * later phase; goaway_sent is now true. */
        MOQ_TEST_CHECK_EQ_INT(nob_blk_capture(&tp, &r, &blk_snap), 0);
        failures += nob_blk_check(&tp, &r, &blk_snap, "blocker-armed");
    }

    /* Seed identity, DERIVED after any origin-4 blocker so the pool generation
     * reflects the announcement's transient staging use of the slot. */
    r.seed_slot = -1;
    for (size_t i = 0; i < tp.server->sub_cap; i++)
        if (tp.server->subs[i].state == MOQ_SUB_FREE) {
            r.seed_slot = (int)i;
            r.seed_gen = tp.server->subs[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK_EQ_INT(r.seed_slot, 0);

    /* Seed: a header-only request-stream PREFIX (Type + Length, no Request ID
     * byte) occupies the one subscription staging slot -- so it consumes no wire
     * Request ID and cannot collide with the announcement's committed id 0 or
     * the target. The eventual SUBSCRIBE is the id after the target. */
    {
        size_t enc_len = 0;
        failures += d18_seed_header_prefix(r.seed_bytes, sizeof(r.seed_bytes),
                                           target_rid + 2, &r.seed_fed, &enc_len);
    }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_bidi_bytes(tp.server_bridge,
                                                     r.seed_id, r.seed_bytes,
                                                     r.seed_fed, false, 0),
        (int)MOQ_OK);
    {
        bridge_stream_entry_t *se = bridge_find_by_id(tp.server_bridge,
                                                      r.seed_id);
        MOQ_TEST_CHECK(se != NULL);
        if (!se) { test_pair_destroy(&tp); return failures + 1; }
        r.seed_ref = se->ref;
        MOQ_TEST_CHECK(r.seed_ref._v != 0);
    }
    failures += nob_check_seed(&tp, &r, "seeded");
    failures += nob_check_target_gone(&tp, &r,
                                      tp.server->recv_payload_bytes,
                                      "seeded");
    failures += nob_blk_check(&tp, &r, &blk_snap, "seeded");

    /* Blockers, on the SESSION so bridge service can consume them. */
    if (c->origin == 1) {
        while (tp.server->drain_ref_count < tp.server->drain_ref_cap)
            MOQ_TEST_CHECK(drain_ref_add(
                tp.server,
                moq_stream_ref_from_u64(0x4000 + tp.server->drain_ref_count)));
    } else if (c->origin == 2 || c->origin == 3) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)queue_close_bidi(tp.server, moq_stream_ref_from_u64(0x6000)),
            (int)MOQ_OK);
    }
    nob_ring_t ring0;
    nob_ring_snap(tp.server, &ring0);

    /* An UNRELATED seeded no-slot carrier (distinct ref, no FIN): the target's
     * refusal/recovery must never disturb it. It is the carrier-count baseline. */
    const moq_stream_ref_t unrel = moq_stream_ref_from_u64(0x8888);
    MOQ_TEST_CHECK(noslot_carrier_install(tp.server, unrel, false));
    size_t carrier_base = tp.server->noslot_carrier_count;
    MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, unrel), 0);

    /* Whole-session and graph baselines before the target ingress. */
    txs_snapshot_t before;
    txs_capture(tp.server, &r.seed_ref, 1, &before);
    nf_expect_after_call_prepare(&before);
    og_graph_t g0;
    og_capture(tp.server, &g0);

    /* The target: a complete SUBSCRIBE on a fresh bidi, refused for capacity.
     * The contract: never fatal; either completed now or retained (the
     * no-slot carrier), never MOQ_ERR_INTERNAL with the bridge latched
     * fatal. */
    uint8_t tgt[128];
    size_t tgt_len;
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, tgt, sizeof(tgt));
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("nob") };
        moq_namespace_t ns = { parts, 1 };
        moq_d18_msg_params_t prm = { 0 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe(&w, target_rid, &ns,
                                          MOQ_BYTES_LITERAL("t"), &prm),
            (int)MOQ_OK);
        tgt_len = moq_buf_writer_offset(&w);
    }
    /* Independent wire-legality oracle: decode the encoded target and compare it
     * to the DECLARED case values BEFORE the product call, so an out-of-sequence
     * target id fails here even though the no-slot path refuses before the
     * product's own request-id validation. */
    failures += nob_check_target_wire(tgt, tgt_len, c->target_rid, what);
    moq_result_t rc = moq_transport_bridge_on_peer_bidi_bytes(
        tp.server_bridge, r.target_id, tgt, tgt_len, false, 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));
    MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK) {
        fprintf(stderr, "NOB %s: unrecognized ingress result %d\n", what,
                (int)rc);
        test_pair_destroy(&tp);
        return failures + 1;
    }
    MOQ_TEST_CHECK(tp.server->state != MOQ_SESS_CLOSED);

    /* The target's own internal ref, captured while the mapping is LIVE --
     * never discovered after retirement. */
    {
        bridge_stream_entry_t *te = bridge_find_by_id(tp.server_bridge,
                                                      r.target_id);
        MOQ_TEST_CHECK(te != NULL);
        if (!te) { test_pair_destroy(&tp); return failures + 1; }
        r.target_ref = te->ref;
        MOQ_TEST_CHECK(r.target_ref._v != 0);
        MOQ_TEST_CHECK(bridge_find_by_ref(tp.server_bridge,
                                          r.target_ref) == te);
    }

    /* Refused-point conservation: seed, whole-session scalars (with only the
     * bounded target-carrier delta), graph topology (added edges must key the
     * target and resolve to a live carrier), drain multiset, and no event. */
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        failures += nob_check_seed(&tp, &r, "blocked");
        {
            txs_snapshot_t expect = before;
            const size_t grew = tp.server->recv_payload_bytes;
            MOQ_TEST_CHECK(grew >= before.recv_payload_bytes);
            MOQ_TEST_CHECK(grew - before.recv_payload_bytes <= tgt_len);
            expect.recv_payload_bytes = grew;
            failures += txs_check_eq(tp.server, &r.seed_ref, 1, &expect,
                                     "blocked");
        }
        {
            og_graph_t g1;
            og_capture(tp.server, &g1);
            failures += og_check_integrity(&g1, "blocked");
            for (size_t i = 0; i < g0.edge_count; i++) {
                const og_edge_t *e = &g0.edges[i];
                size_t seen = 0;
                for (size_t j = 0; j < g1.edge_count; j++) {
                    const og_edge_t *f = &g1.edges[j];
                    if (f->domain == e->domain && f->key == e->key &&
                        f->kind == e->kind && f->slot == e->slot)
                        seen++;
                }
                if (seen != 1) {
                    fprintf(stderr, "NOB blocked: pre-existing edge (%d key"
                            " %llu) not conserved\n", (int)e->domain,
                            (unsigned long long)e->key);
                    failures++;
                }
            }
            for (size_t j = 0; j < g1.edge_count; j++) {
                const og_edge_t *f = &g1.edges[j];
                int pre = 0;
                for (size_t i = 0; i < g0.edge_count; i++) {
                    const og_edge_t *e = &g0.edges[i];
                    if (f->domain == e->domain && f->key == e->key &&
                        f->kind == e->kind && f->slot == e->slot) {
                        pre = 1; break;
                    }
                }
                if (pre) continue;
                /* Domain-aware, IDENTITY-QUALIFIED allowance, through the same
                 * helper the selfcheck exercises: request-ID edges key the
                 * DECLARED request id (r.target_rid -- 2 for origin 4, 0 for
                 * origins 1-3), every other domain keys the target ref, and the
                 * edge's own (kind, slot) must resolve to the live target owner
                 * (an edge repointed at the seed fails by identity). */
                failures += nob_added_edge_ok(tp.server, f, r.target_rid,
                                              r.target_ref._v, "blocked");
            }
        }
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring0, "blocked");
        failures += nob_check_no_events(&tp, "blocked");
        /* The queued blocker ITSELF, not merely the depth. */
        {
            size_t depth = tp.server->action_tail - tp.server->action_head;
            size_t want_depth = (c->origin == 1) ? 0 : 1;
            MOQ_TEST_CHECK_EQ_SIZE(depth, want_depth);
            if (depth == 1) {
                const moq_action_t *head =
                    &tp.server->actions[tp.server->action_head %
                                        tp.server->action_cap];
                if (c->origin == 4) {
                    /* The blocker is the queued per-request GOAWAY, compared in
                     * full: a non-FIN SEND_BIDI_STREAM on the announcement's ref
                     * whose bytes are one complete REQUEST_GOAWAY envelope with no
                     * trailing bytes and the exact New Session URI. */
                    MOQ_TEST_CHECK_EQ_INT((int)head->kind,
                                          (int)MOQ_ACTION_SEND_BIDI_STREAM);
                    MOQ_TEST_CHECK_EQ_U64(
                        head->u.send_bidi_stream.stream_ref._v, r.blk_ref._v);
                    MOQ_TEST_CHECK_EQ_INT(head->u.send_bidi_stream.fin ? 1 : 0, 0);
                    MOQ_TEST_CHECK(head->u.send_bidi_stream.data != NULL);
                    if (head->u.send_bidi_stream.data) {
                        moq_buf_reader_t gr;
                        moq_buf_reader_init(&gr, head->u.send_bidi_stream.data,
                                            head->u.send_bidi_stream.len);
                        moq_control_envelope_t genv;
                        memset(&genv, 0, sizeof(genv));
                        MOQ_TEST_CHECK_EQ_INT(
                            (int)moq_d18_decode_envelope(&gr, &genv),
                            (int)MOQ_OK);
                        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&gr),
                                               (size_t)0);
                        MOQ_TEST_CHECK_EQ_U64(genv.msg_type,
                                              (uint64_t)MOQ_D18_GOAWAY);
                        moq_d18_goaway_t gaw;
                        memset(&gaw, 0, sizeof(gaw));
                        MOQ_TEST_CHECK_EQ_INT(
                            (int)moq_d18_decode_goaway_request(genv.payload,
                                genv.payload_len, &gaw), (int)MOQ_OK);
                        MOQ_TEST_CHECK(gaw.uri.len == sizeof(nob_goaway_uri) &&
                                       gaw.uri.data &&
                                       memcmp(gaw.uri.data, nob_goaway_uri,
                                              sizeof(nob_goaway_uri)) == 0);
                    }
                } else {
                    MOQ_TEST_CHECK_EQ_INT((int)head->kind,
                                          (int)MOQ_ACTION_CLOSE_BIDI_STREAM);
                    MOQ_TEST_CHECK_EQ_U64(
                        head->u.close_bidi_stream.stream_ref._v,
                        (uint64_t)0x6000);
                }
            }
        }
        failures += nob_blk_check(&tp, &r, &blk_snap, "blocked");
        /* Product-path carrier: the genuinely refused no-FIN target is retained
         * in exactly one carrier with its declared ref and FIN=false; the
         * unrelated carrier stays exact; the count is baseline + 1. */
        MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, r.target_ref), 0);
        MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, unrel), 0);
        MOQ_TEST_CHECK_EQ_SIZE(tp.server->noslot_carrier_count,
                               carrier_base + 1);
    }

    /* Declared completion/late drain rings, derived from DECLARED members. */
    nob_ring_t comp_want, late_want;
    if (c->origin == 1) {
        nob_ring_t less = ring0;
        size_t k = 0;
        int removed = 0;
        for (size_t i = 0; i < ring0.count; i++) {
            if (!removed && ring0.ref[i] == 0x4000) { removed = 1; continue; }
            less.ref[k] = ring0.ref[i];
            less.reason[k] = ring0.reason[i];
            k++;
        }
        less.count = k;
        MOQ_TEST_CHECK_EQ_INT(removed, 1);
        late_want = less;
        nob_ring_plus(&less, r.target_ref, MOQ_DRAIN_NORMAL, &comp_want);
    } else {
        nob_ring_plus(&ring0, r.target_ref, MOQ_DRAIN_NORMAL, &comp_want);
        late_want = ring0;
    }

    /* Origin 1: free ONE drain slot by feeding the DECLARED filler's own FIN
     * through the session's public absorb path. */
    if (c->origin == 1)
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(
                tp.server, moq_stream_ref_from_u64(0x4000), NULL, 0, true, 0),
            (int)MOQ_OK);

    /* Service-only recovery. No further ingress. */
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);

    /* The wire, as ONE ordered declared image per origin -- the blocker's own
     * dispatch included, byte for byte. Origins 2/3's blocker closes an
     * UNMAPPED ref and legally emits nothing. */
    {
        MOQ_TEST_CHECK(tp.server_ep.count < FAKE_EP_MAX_OPS);
        size_t want_n = 0;
        struct { int kind; uint64_t sid; int fin; } wantop[3];
        memset(wantop, 0, sizeof(wantop));
        if (c->origin == 4) {
            wantop[want_n].kind = FAKE_OP_WRITE;
            wantop[want_n].sid = r.blk_id;   /* the announcement's request bidi */
            wantop[want_n].fin = 0;
            want_n++;
        }
        if (c->pre_setup) {
            wantop[want_n].kind = FAKE_OP_STOP;
            wantop[want_n].sid = r.target_id;
            want_n++;
            wantop[want_n].kind = FAKE_OP_RESET;
            wantop[want_n].sid = r.target_id;
            want_n++;
        } else {
            wantop[want_n].kind = FAKE_OP_WRITE;
            wantop[want_n].sid = r.target_id;
            wantop[want_n].fin = 1;
            want_n++;
        }
        if (tp.server_ep.count != want_n) {
            fprintf(stderr, "NOB %s: %zu endpoint ops, expected %zu\n", what,
                    tp.server_ep.count, want_n);
            failures++;
        }
        size_t n = tp.server_ep.count < want_n ? tp.server_ep.count : want_n;
        for (size_t i = 0; i < n; i++) {
            fake_op_t *o = &tp.server_ep.ops[i];
            if ((int)o->kind != wantop[i].kind ||
                o->stream_id != wantop[i].sid) {
                fprintf(stderr, "NOB %s: op %zu kind %d sid %llu, expected"
                        " kind %d sid %llu\n", what, i, (int)o->kind,
                        (unsigned long long)o->stream_id, wantop[i].kind,
                        (unsigned long long)wantop[i].sid);
                failures++;
                continue;
            }
            if (o->kind == FAKE_OP_STOP || o->kind == FAKE_OP_RESET) {
                MOQ_TEST_CHECK_EQ_U64(o->error_code, (uint64_t)0x1);
            } else if (o->kind == FAKE_OP_WRITE && wantop[i].fin == 0) {
                /* Origin 4's blocker: the nonterminal per-request GOAWAY on the
                 * announcement's request bidi -- a decodable REQUEST_GOAWAY with
                 * the exact New Session URI, no trailing bytes, no FIN. */
                MOQ_TEST_CHECK_EQ_INT(o->fin ? 1 : 0, 0);
                if (o->data_len > sizeof(o->data)) {
                    fprintf(stderr, "NOB %s: recorder length %zu exceeds the"
                            " inline capacity\n", what, o->data_len);
                    failures++;
                    continue;
                }
                moq_buf_reader_t gr;
                moq_buf_reader_init(&gr, o->data, o->data_len);
                moq_control_envelope_t genv;
                memset(&genv, 0, sizeof(genv));
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_envelope(&gr, &genv), (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&gr), (size_t)0);
                MOQ_TEST_CHECK_EQ_U64(genv.msg_type, (uint64_t)MOQ_D18_GOAWAY);
                moq_d18_goaway_t gaw;
                memset(&gaw, 0, sizeof(gaw));
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_goaway_request(genv.payload,
                                                       genv.payload_len, &gaw),
                    (int)MOQ_OK);
                MOQ_TEST_CHECK(gaw.uri.len == sizeof(nob_goaway_uri) &&
                               gaw.uri.data &&
                               memcmp(gaw.uri.data, nob_goaway_uri,
                                      sizeof(nob_goaway_uri)) == 0);
            } else {
                /* The target terminal: one FIN'd write, one complete
                 * REQUEST_ERROR, no trailing bytes, guarded non-NULL span. */
                MOQ_TEST_CHECK(o->fin);
                if (o->data_len > sizeof(o->data)) {
                    fprintf(stderr, "NOB %s: recorder length %zu exceeds the"
                            " inline capacity\n", what, o->data_len);
                    failures++;
                    continue;
                }
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, o->data, o->data_len);
                moq_control_envelope_t env;
                memset(&env, 0, sizeof(env));
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_envelope(&rr, &env), (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_REQUEST_ERROR);
                moq_d18_request_error_t er;
                memset(&er, 0, sizeof(er));
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_request_error(env.payload,
                                                      env.payload_len, &er),
                    (int)MOQ_OK);
                /* Local capacity: EXCESSIVE_LOAD, retryable at once
                 * (draft-18 §10.6). */
                MOQ_TEST_CHECK_EQ_U64(
                    er.error_code, (uint64_t)MOQ_REQUEST_ERROR_EXCESSIVE_LOAD);
                MOQ_TEST_CHECK_EQ_U64(er.retry_interval, (uint64_t)1);
                MOQ_TEST_CHECK(er.reason.len == 17 && er.reason.data &&
                               memcmp(er.reason.data, "request pool full",
                                      17) == 0);
            }
        }
    }
    /* Both routes leave the target mapping in the local-closed live state at
     * recovery: post-SETUP the REQUEST_ERROR FIN'd our send half, pre-SETUP our
     * RESET_STREAM closed it. A bidi RESET closes only our sending direction
     * (RFC 9000 §3.5), so the mapping is retained by transport id and internal
     * ref while the peer terminal is still owed (a NORMAL drain reference), and
     * the later peer RESET/FIN retires it. */
    failures += nob_check_phase(&tp, &r, &before, &comp_want, 1, "recovered");
    failures += nob_blk_check(&tp, &r, &blk_snap, "recovered");
    /* Completion retired exactly the target carrier; count back to baseline. */
    MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, r.target_ref), -1);
    MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, unrel), 0);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->noslot_carrier_count, carrier_base);

    /* Exactly once: a second service adds nothing and reproduces the whole
     * postcondition. */
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
    failures += nob_check_phase(&tp, &r, &before, &comp_want, 1, "reserviced");
    failures += nob_blk_check(&tp, &r, &blk_snap, "reserviced");
    MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, r.target_ref), -1);
    MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, unrel), 0);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->noslot_carrier_count, carrier_base);

    /* The legitimate LATER target terminal: exact drain release and physical
     * retirement exactly once. Post-SETUP it is the peer FIN; pre-SETUP it is
     * the peer's RESET_STREAM answering our STOP_SENDING. Either arrives on the
     * saved transport id, is delivered to moq_session_on_bidi_stream_reset() /
     * the bidi-bytes FIN path through the retained mapping, releases the exact
     * target NORMAL drain ref, and retires the mapping. */
    if (c->pre_setup) {
        /* The peer's REAL answer to STOP_SENDING is RESET_STREAM, delivered
         * through the bridge on the saved TRANSPORT id. The contract is
         * storage-neutral: whatever tombstone/carrier design the corrected
         * bridge uses after its own STOP+RESET dispatch, this peer RESET must
         * release the exact target NORMAL drain ref, leave no semantic owner
         * or physical mapping, stay nonfatal and open, and emit nothing. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)held_bridge_reset(
                tp.server_bridge, r.target_id, 0x1, 0),
            (int)MOQ_OK);
    } else {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_bidi_bytes(
                tp.server_bridge, r.target_id, NULL, 0, true, 0),
            (int)MOQ_OK);
    }
    /* An endpoint op emitted SYNCHRONOUSLY by the late terminal must be
     * caught, not discarded by the next clear: zero ops BEFORE any clear or
     * service. */
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
    failures += nob_check_phase(&tp, &r, &before, &late_want, 0,
                                c->pre_setup ? "late-reset-pre-service"
                                             : "late-fin-pre-service");
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
    failures += nob_check_phase(&tp, &r, &before, &late_want, 0,
                                c->pre_setup ? "late-reset-serviced"
                                             : "late-fin-serviced");
    /* The target's own late terminal must not have mutated or retired the
     * independently live blocker. */
    failures += nob_blk_check(&tp, &r, &blk_snap, "late-terminal");
    /* The late terminal keeps the target carrier absent and the unrelated one
     * exact -- no resurrection. */
    MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, r.target_ref), -1);
    MOQ_TEST_CHECK_EQ_INT(nob_carrier_state(tp.server, unrel), 0);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->noslot_carrier_count, carrier_base);
    if (c->pre_setup) {
        /* A REPEATED peer RESET plus service adds nothing: same ring, no op,
         * no event, nothing resurrected. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)held_bridge_reset(
                tp.server_bridge, r.target_id, 0x1, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
        failures += nob_check_phase(&tp, &r, &before, &late_want, 0,
                                    "late-reset-again-pre-service");
        MOQ_TEST_CHECK_EQ_INT(
            (int)held_bridge_service(tp.server_bridge, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
        failures += nob_check_phase(&tp, &r, &before, &late_want, 0,
                                    "late-reset-again");
        failures += nob_check_seed(&tp, &r, "late-reset-again");
        failures += nob_check_target_gone(&tp, &r,
                                          before.recv_payload_bytes,
                                          "late-reset-again");
        nob_ring_t now2;
        nob_ring_snap(tp.server, &now2);
        failures += nob_ring_equals(&now2, &late_want, "late-reset-again");
    }

    test_pair_destroy(&tp);
    return failures;
}


/* Establish a draft-18 bridge pair whose SERVER has a one-slot subscription pool
 * (like the no-owner matrix), its slot occupied by a fragmented inbound
 * SUBSCRIBE so any further inbound SUBSCRIBE is refused for capacity. Every
 * create/start/action/feed/event result is checked; it fails immediately on any
 * setup failure. Leaves both op recorders cleared. Returns 0 on success. */
static int exh_arm_goaway_blocker(test_pair_t *tp, uint64_t bidi_id,
                                  moq_stream_ref_t *blk_ref_out);


static int exh_setup(test_pair_t *tp, uint32_t server_send_buf,
                     moq_stream_ref_t *seed_ref_out,
                     moq_stream_ref_t *blk_ref_out)
{
    if (d18_pair_init_caps(tp, 0, moq_alloc_default(), moq_alloc_default(),
                           0, false, 0) < 0)
        return -1;
    {
        moq_session_cfg_t scfg;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_SERVER);
        scfg.version = MOQ_VERSION_DRAFT_18;
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 10;
        /* All seven request-owner pools at 1, so the carrier pool (== the drain
         * ring == the sum of the seven caps) is exactly 7: small, bounded, and
         * fully fillable in the exhaustion arms. */
        scfg.max_subscriptions = 1;
        scfg.max_announcements = 1;
        scfg.max_fetches = 1;
        scfg.max_publishes = 1;
        scfg.max_track_statuses = 1;
        scfg.max_namespace_subscriptions = 1;
        scfg.max_track_subscriptions = 1;
        if (server_send_buf) scfg.send_buffer_size = server_send_buf;
        moq_session_t *sv = NULL;
        if (moq_session_create(&scfg, 0, &sv) != MOQ_OK || !sv) {
            test_pair_destroy(tp); return -1;
        }
        held_bridge_destroy(tp->server_bridge);
        moq_session_destroy(tp->server);
        tp->server = sv;
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
        if (held_bridge_create(&bcfg, tp->server, &tp->server_ep.vtable,
                                        &tp->server_ep, &tp->server_bridge)
            != MOQ_OK) { test_pair_destroy(tp); return -1; }
    }
    if (moq_session_start(tp->client, 0) != MOQ_OK ||
        moq_session_start(tp->server, 0) != MOQ_OK) {
        test_pair_destroy(tp); return -1;
    }
    if (d18_strict_shuttle(tp, 30, 0, "exh setup") != 0) {
        test_pair_destroy(tp); return -1;
    }
    /* Strict setup classification: exactly one SETUP_COMPLETE per side and zero
     * other events, both sessions ESTABLISHED, both bridges nonfatal/open with
     * no pending work and empty action queues. Setup transport output was
     * already classified by d18_strict_shuttle; clear the recorders so the
     * blocker/seed phases measure only their own output. */
    moq_event_t ev;
    int ss = 0, so = 0, cs = 0, co = 0;
    while (moq_session_poll_events(tp->server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) ss++; else so++;
        moq_event_cleanup(&ev);
    }
    while (moq_session_poll_events(tp->client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) cs++; else co++;
        moq_event_cleanup(&ev);
    }
    if (ss != 1 || so != 0 || cs != 1 || co != 0 ||
        tp->client->state != MOQ_SESS_ESTABLISHED ||
        tp->server->state != MOQ_SESS_ESTABLISHED ||
        moq_transport_bridge_is_fatal(tp->server_bridge) ||
        moq_transport_bridge_is_closed(tp->server_bridge) ||
        moq_transport_bridge_is_fatal(tp->client_bridge) ||
        moq_transport_bridge_is_closed(tp->client_bridge) ||
        moq_transport_bridge_has_pending(tp->server_bridge) ||
        moq_transport_bridge_has_pending(tp->client_bridge) ||
        tp->server->action_tail != tp->server->action_head ||
        tp->client->action_tail != tp->client->action_head) {
        fprintf(stderr, "EXH setup: setup classification failed\n");
        test_pair_destroy(tp); return -1;
    }
    fake_endpoint_clear_ops(&tp->client_ep);
    fake_endpoint_clear_ops(&tp->server_ep);

    /* Arm the send-buffer blocker BEFORE the seed so the announcement can take
     * and then free the single subscription slot the seed occupies next. */
    if (blk_ref_out &&
        exh_arm_goaway_blocker(tp, 700, blk_ref_out) != 0) {
        test_pair_destroy(tp); return -1;
    }

    /* A header-only request-stream PREFIX (Type + Length, no Request ID byte,
     * proven shorter than the full message by the helper): consumes no wire
     * Request ID, so it never collides with the blocker's committed id 0 or the
     * target. Feeding it must produce NO endpoint output and NO new action. */
    uint8_t seed[128];
    size_t seed_len = 0;
    if (d18_seed_header_prefix(seed, sizeof(seed), 4, &seed_len, NULL) != 0) {
        test_pair_destroy(tp); return -1;
    }
    size_t act_before = tp->server->action_tail - tp->server->action_head;
    fake_endpoint_clear_ops(&tp->server_ep);
    if (moq_transport_bridge_on_peer_bidi_bytes(tp->server_bridge, 500, seed,
                                                seed_len, false, 0) != MOQ_OK) {
        test_pair_destroy(tp); return -1;
    }
    if (tp->server_ep.count != 0 ||
        (size_t)(tp->server->action_tail - tp->server->action_head)
            != act_before) {
        fprintf(stderr, "EXH setup: seed produced unexpected output/action\n");
        test_pair_destroy(tp); return -1;
    }
    /* Absolute seed identity: distinct nonzero ref vs transport id, one
     * receiving subscription owner with the exact retained prefix bytes/length,
     * its stream-ref registry edge (no request-id key), and a live peer-origin
     * BIDI mapping with no pending/close flags. */
    {
        bridge_stream_entry_t *se = bridge_find_by_id(tp->server_bridge, 500);
        moq_stream_ref_t sref = se ? se->ref : moq_stream_ref_from_u64(0);
        if (!se || sref._v == 0 || sref._v == 500) {
            fprintf(stderr, "EXH setup: seed ref/id not distinct\n");
            test_pair_destroy(tp); return -1;
        }
        moq_request_endpoint_t ep =
            request_registry_find_by_streamref(tp->server, sref);
        if (ep.kind != MOQ_REQ_SUBSCRIPTION || ep.has_request_id ||
            !ep.has_stream_ref || ep.stream_ref._v != sref._v ||
            (int)tp->server->subs[ep.slot].state !=
                (int)MOQ_SUB_RECVING_REQUEST ||
            tp->server->subs[ep.slot].rs.recv_len != seed_len ||
            !tp->server->subs[ep.slot].rs.recv_buf ||
            memcmp(tp->server->subs[ep.slot].rs.recv_buf, seed,
                   seed_len) != 0) {
            fprintf(stderr, "EXH setup: seed owner not the declared receiver\n");
            test_pair_destroy(tp); return -1;
        }
        if (seed_ref_out) *seed_ref_out = sref;
    }
    fake_endpoint_clear_ops(&tp->client_ep);
    fake_endpoint_clear_ops(&tp->server_ep);
    return 0;
}


/* Encode one complete inbound d18 SUBSCRIBE at `request_id` into buf; returns
 * its length. */
static size_t exh_encode_subscribe(uint8_t *buf, size_t cap, uint64_t request_id)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("x") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t prm = { 0 };
    if (moq_d18_encode_subscribe(&w, request_id, &ns, MOQ_BYTES_LITERAL("t"),
                                 &prm) != MOQ_OK) return 0;
    return moq_buf_writer_offset(&w);
}


/* Arm the production-valid send-buffer blocker used by the exhaustion reset arm:
 * admit a live RECEIVER-side announcement on `bidi_id` through the real request
 * route, accept it, and issue a nonterminal per-request GOAWAY (§10.4) carrying
 * nob_goaway_uri. The accepted announcement re-keys into the announcement pool
 * and FREES its subscription staging slot; the GOAWAY is a genuine queued
 * request-bidi write that fills the send buffer (never a raw send_len poke), and
 * it leaves the announcement owner LIVE. Consumes inbound request id 0, so a
 * later target must use the next-in-sequence id 2. The server must be
 * ESTABLISHED with a free subscription slot. Returns 0 on success (with
 * *blk_ref_out set to the announcement bidi's ref), else a positive count. */
static int exh_arm_goaway_blocker(test_pair_t *tp, uint64_t bidi_id,
                                  moq_stream_ref_t *blk_ref_out)
{
    int failures = 0;
    uint8_t pn[128];
    size_t pn_len;
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, pn, sizeof(pn));
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("blk") };
        moq_namespace_t ns = { parts, 1 };
        moq_d18_msg_params_t mp = { 0 };
        if (moq_d18_encode_publish_namespace(&w, 0, &ns, &mp) != MOQ_OK)
            return failures + 1;
        pn_len = moq_buf_writer_offset(&w);
    }
    if (moq_transport_bridge_on_peer_bidi_bytes(tp->server_bridge, bidi_id, pn,
                                                pn_len, false, 0) != MOQ_OK)
        return failures + 1;
    moq_announcement_t ah;
    memset(&ah, 0, sizeof(ah));
    int got = 0;
    moq_event_t ev;
    while (moq_session_poll_events(tp->server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            ah = ev.u.namespace_published.ann;
            got = 1;
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(got);
    if (!got) return failures + 1;
    moq_accept_namespace_cfg_t ac;
    moq_accept_namespace_cfg_init(&ac);
    if (moq_session_accept_namespace(tp->server, ah, &ac, 0) != MOQ_OK)
        return failures + 1;
    /* Flush the REQUEST_OK so the send buffer starts empty before the GOAWAY. */
    if (held_bridge_service(tp->server_bridge, 0) != MOQ_OK)
        return failures + 1;
    while (moq_session_poll_events(tp->server, &ev, 1) > 0) moq_event_cleanup(&ev);
    fake_endpoint_clear_ops(&tp->server_ep);
    {
        bridge_stream_entry_t *be = bridge_find_by_id(tp->server_bridge, bidi_id);
        MOQ_TEST_CHECK(be != NULL);
        if (!be) return failures + 1;
        if (blk_ref_out) *blk_ref_out = be->ref;
    }
    moq_request_goaway_cfg_t gc;
    moq_request_goaway_cfg_init(&gc);
    gc.new_session_uri.data = exh_goaway_uri;
    gc.new_session_uri.len = sizeof(exh_goaway_uri);
    if (moq_session_request_goaway_namespace(tp->server, ah, &gc, 0) != MOQ_OK)
        return failures + 1;
    return failures;
}


/* Count queued STOP_BIDI / RESET_BIDI actions targeting a ref. */
/* The no-slot carrier pool as a ref/FIN multiset, compared exactly. */
#define EXH_CARRIER_MAX 64

typedef struct exh_carrier_snap {
    size_t   count;
    uint64_t ref[EXH_CARRIER_MAX];
    uint8_t  fin[EXH_CARRIER_MAX];
    int      overflow;
} exh_carrier_snap_t;


/* The no-slot carrier pool is SPARSE: removal clears an arbitrary slot and
 * decrements the count without compacting, so a live carrier can sit above the
 * first `count` slots and a cleared slot can sit below. Scan ALL cap slots and
 * append every nonzero entry; a mismatch against the product's own
 * `noslot_carrier_count`, or an overflow of the bounded snapshot, makes the
 * snapshot INCOMPARABLE rather than truncating it. */
static void exh_carrier_capture(const moq_session_t *s, exh_carrier_snap_t *c)
{
    memset(c, 0, sizeof(*c));
    size_t n = 0;
    for (size_t i = 0; i < s->noslot_carrier_cap; i++) {
        if (s->noslot_carriers[i].stream_ref == 0) continue;   /* sparse hole */
        if (n >= EXH_CARRIER_MAX) { c->overflow = 1; return; }
        c->ref[n] = s->noslot_carriers[i].stream_ref;
        c->fin[n] = s->noslot_carriers[i].fin ? 1u : 0u;
        n++;
    }
    if (n != s->noslot_carrier_count) { c->overflow = 1; return; }
    c->count = n;
}


/* Set to silence exh_carrier_equals diagnostics for an expected-negative call. */
static int exh_quiet;


static int exh_carrier_equals(const moq_session_t *s,
                              const exh_carrier_snap_t *want, const char *what)
{
    exh_carrier_snap_t now;
    exh_carrier_capture(s, &now);
    if (now.overflow || want->overflow) {
        if (!exh_quiet)
            fprintf(stderr, "EXH %s: carrier snapshot overflow\n", what);
        return 1;
    }
    if (now.count != want->count) {
        if (!exh_quiet)
            fprintf(stderr, "EXH %s: %zu carriers, expected %zu\n", what,
                    now.count, want->count);
        return 1;
    }
    int used[EXH_CARRIER_MAX] = { 0 };
    for (size_t i = 0; i < want->count; i++) {
        int found = -1;
        for (size_t j = 0; j < now.count; j++)
            if (!used[j] && now.ref[j] == want->ref[i] &&
                now.fin[j] == want->fin[i]) { found = (int)j; break; }
        if (found < 0) {
            if (!exh_quiet)
                fprintf(stderr, "EXH %s: carrier (ref %llu fin %d) missing\n",
                        what, (unsigned long long)want->ref[i],
                        (int)want->fin[i]);
            return 1;
        }
        used[found] = 1;
    }
    return 0;
}


/* Sparse-pool capture self-check: install A then B, remove A. A's slot clears
 * and the count drops, leaving B ABOVE the first `count` slot with a hole below
 * -- the exact shape a count-bounded scan would miss. The capture must find
 * exactly B, no zero/stale A. */
static int exh_carrier_capture_selfcheck(void)
{
    int failures = 0;
    test_pair_t tp;
    if (d18_pair_init(&tp, 1) < 0) return failures + 1;
    if (tp.server->noslot_carrier_cap < 2) { test_pair_destroy(&tp); return failures + 1; }

    moq_stream_ref_t A = moq_stream_ref_from_u64(0x1111);
    moq_stream_ref_t B = moq_stream_ref_from_u64(0x2222);
    MOQ_TEST_CHECK(noslot_carrier_install(tp.server, A, false));
    MOQ_TEST_CHECK(noslot_carrier_install(tp.server, B, true));
    noslot_carrier_remove(tp.server, A);
    /* A genuine hole below B: A gone, B still live, count back to 1. */
    MOQ_TEST_CHECK(noslot_carrier_find(tp.server, A) < 0);
    MOQ_TEST_CHECK(noslot_carrier_find(tp.server, B) >= 0);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->noslot_carrier_count, (size_t)1);

    exh_carrier_snap_t now;
    exh_carrier_capture(tp.server, &now);
    MOQ_TEST_CHECK_EQ_INT(now.overflow, 0);
    MOQ_TEST_CHECK_EQ_SIZE(now.count, (size_t)1);
    if (now.count == 1) {
        MOQ_TEST_CHECK_EQ_U64(now.ref[0], B._v);   /* exactly B, never a hole */
        MOQ_TEST_CHECK_EQ_INT((int)now.fin[0], 1);
    }
    /* exact-equals ACCEPTS {B} and (quietly) REJECTS the stale {A}. */
    {
        exh_carrier_snap_t want;
        memset(&want, 0, sizeof(want));
        want.count = 1; want.ref[0] = B._v; want.fin[0] = 1;
        failures += exh_carrier_equals(tp.server, &want, "exh capture selfcheck");

        exh_carrier_snap_t stale;
        memset(&stale, 0, sizeof(stale));
        stale.count = 1; stale.ref[0] = A._v; stale.fin[0] = 0;
        exh_quiet = 1;
        MOQ_TEST_CHECK_EQ_INT(
            exh_carrier_equals(tp.server, &stale, "exh capture stale-A"), 1);
        exh_quiet = 0;
    }

    /* The incomparable path is load-bearing: make noslot_carrier_count disagree
     * with the scanned live entries and require the capture to set overflow;
     * restore the count and prove the exact {B} snapshot passes again. */
    {
        size_t saved = tp.server->noslot_carrier_count;
        tp.server->noslot_carrier_count = saved + 1;
        exh_carrier_snap_t bad;
        exh_carrier_capture(tp.server, &bad);
        MOQ_TEST_CHECK_EQ_INT(bad.overflow, 1);
        tp.server->noslot_carrier_count = saved;
        exh_carrier_snap_t want;
        memset(&want, 0, sizeof(want));
        want.count = 1; want.ref[0] = B._v; want.fin[0] = 1;
        failures += exh_carrier_equals(tp.server, &want, "exh capture restored");
    }
    test_pair_destroy(&tp);
    return failures;
}


/* The seed staging owner installed by exh_setup, by SAVED ref: still a receiving
 * subscription with its stream-ref registry edge, no request-ID key, and no
 * pending/close bridge flags. */
static int exh_check_seed(test_pair_t *tp, moq_stream_ref_t seed_ref,
                          const char *what)
{
    int bad = 0;
    moq_request_endpoint_t ep =
        request_registry_find_by_streamref(tp->server, seed_ref);
    if (ep.kind != MOQ_REQ_SUBSCRIPTION || !ep.has_stream_ref ||
        ep.has_request_id || ep.stream_ref._v != seed_ref._v) {
        fprintf(stderr, "EXH %s: seed registry answer changed\n", what);
        bad++;
    }
    if (ep.kind == MOQ_REQ_SUBSCRIPTION &&
        (int)tp->server->subs[ep.slot].state != (int)MOQ_SUB_RECVING_REQUEST) {
        fprintf(stderr, "EXH %s: seed no longer receiving\n", what);
        bad++;
    }
    bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp->server_bridge,
                                                       seed_ref);
    if (!by_ref || !by_ref->active || by_ref->pending_retry ||
        by_ref->pending_fin || by_ref->fin_retained || by_ref->pending_reset ||
        by_ref->pending_stop || by_ref->local_send_closed) {
        fprintf(stderr, "EXH %s: seed bridge mapping changed\n", what);
        bad++;
    }
    return bad;
}


/* No-slot carrier pool full, the post-SETUP REQUEST_ERROR blocked
 * on the send buffer, but two action slots and a NORMAL drain slot free. The
 * refused no-FIN target is dropped via exactly STOP+RESET(CANCELLED), the
 * session stays ESTABLISHED/nonfatal, exactly one NORMAL drain is added, no
 * target carrier is created, and every pre-existing carrier is conserved. */
int test_noslot_exhaustion_reset(void)
{
    int failures = 0;
    test_pair_t tp;
    moq_stream_ref_t seed_ref, blk_ref;
    /* A small send buffer so the production-valid GOAWAY blocker fills it and the
     * target REQUEST_ERROR cannot fit -- no direct send_len poke. */
    if (exh_setup(&tp, 64, &seed_ref, &blk_ref) < 0) return 1;
    failures += exh_check_seed(&tp, seed_ref, "reset-armed");

    /* Bounded fill over the INDEPENDENTLY declared carrier capacity, asserting
     * each insertion and the exact final count -- a broken insert cannot spin. */
    size_t cap = tp.server->noslot_carrier_cap;
    MOQ_TEST_CHECK(cap > 0 && cap <= EXH_CARRIER_MAX);
    for (size_t i = 0; i < cap; i++)
        MOQ_TEST_CHECK(noslot_carrier_install(
            tp.server, moq_stream_ref_from_u64(0x9000 + i), false));
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->noslot_carrier_count, cap);

    /* Carrier and drain multisets before the target. The GOAWAY blocker already
     * fills the send buffer AND keeps a queued SEND_BIDI action (so
     * session_call_prepare cannot reset send_len) -- no separate filler. */
    exh_carrier_snap_t csnap;
    exh_carrier_capture(tp.server, &csnap);
    nob_ring_t ring_before;
    nob_ring_snap(tp.server, &ring_before);
    MOQ_TEST_CHECK(tp.server->send_cap - tp.server->send_len < 24);
    MOQ_TEST_CHECK(tp.server->drain_ref_count < tp.server->drain_ref_cap);
    MOQ_TEST_CHECK(action_queue_avail(tp.server) >= 2);

    uint8_t tgt[128];
    size_t tgt_len = exh_encode_subscribe(tgt, sizeof(tgt), 2);
    MOQ_TEST_CHECK(tgt_len > 0);
    moq_result_t rc = moq_transport_bridge_on_peer_bidi_bytes(
        tp.server_bridge, 600, tgt, tgt_len, false, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(tp.server->state == MOQ_SESS_ESTABLISHED);

    bridge_stream_entry_t *te = bridge_find_by_id(tp.server_bridge, 600);
    MOQ_TEST_CHECK(te != NULL);
    if (!te) { test_pair_destroy(&tp); return failures + 1; }
    moq_stream_ref_t tgt_ref = te->ref;
    MOQ_TEST_CHECK(tgt_ref._v != 0);

    /* The WHOLE queued action inventory: exactly the blocker GOAWAY on blk_ref
     * plus target STOP_BIDI(CANCELLED 0x1) + RESET_BIDI(CANCELLED 0x1), no more. */
    {
        size_t depth = tp.server->action_tail - tp.server->action_head;
        MOQ_TEST_CHECK_EQ_SIZE(depth, (size_t)3);
        int goaway = 0, stop = 0, reset = 0, other = 0;
        for (size_t i = tp.server->action_head; i != tp.server->action_tail;
             i++) {
            const moq_action_t *a =
                &tp.server->actions[i % tp.server->action_cap];
            if (a->kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a->u.send_bidi_stream.stream_ref._v == blk_ref._v &&
                !a->u.send_bidi_stream.fin) goaway++;
            else if (a->kind == MOQ_ACTION_STOP_BIDI_STREAM &&
                     a->u.stop_bidi_stream.stream_ref._v == tgt_ref._v &&
                     a->u.stop_bidi_stream.error_code == 0x1) stop++;
            else if (a->kind == MOQ_ACTION_RESET_BIDI_STREAM &&
                     a->u.reset_bidi_stream.stream_ref._v == tgt_ref._v &&
                     a->u.reset_bidi_stream.error_code == 0x1) reset++;
            else other++;
        }
        MOQ_TEST_CHECK_EQ_INT(goaway, 1);
        MOQ_TEST_CHECK_EQ_INT(stop, 1);
        MOQ_TEST_CHECK_EQ_INT(reset, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* Carrier pool conserved EXACTLY (multiset), no target carrier created. */
    failures += exh_carrier_equals(tp.server, &csnap, "reset-refused");
    MOQ_TEST_CHECK(noslot_carrier_find(tp.server, tgt_ref) < 0);
    /* Complete drain multiset: prior entries + exactly (tgt_ref, NORMAL). */
    {
        nob_ring_t want;
        nob_ring_plus(&ring_before, tgt_ref, MOQ_DRAIN_NORMAL, &want);
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &want, "reset-refused");
    }
    failures += exh_check_seed(&tp, seed_ref, "reset-refused");
    MOQ_TEST_CHECK(request_registry_find_by_streamref(tp.server, blk_ref).kind
                   == MOQ_REQ_ANNOUNCEMENT);

    /* Service to the endpoint: exactly the GOAWAY WRITE(blk_id) + STOP + RESET on
     * the target id, the target's local send closed, both target ids resolving to
     * the retained live mapping while the NORMAL drain is still owed. */
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    {
        MOQ_TEST_CHECK(tp.server_ep.count < FAKE_EP_MAX_OPS);
        MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)3);
        int w = 0, s = 0, rst = 0, other = 0;
        for (size_t i = 0; i < tp.server_ep.count; i++) {
            const fake_op_t *o = &tp.server_ep.ops[i];
            if (o->kind == FAKE_OP_WRITE && o->stream_id == blk_ref._v) other++;
            else if (o->kind == FAKE_OP_WRITE && o->stream_id == 700 && !o->fin)
                w++;
            else if (o->kind == FAKE_OP_STOP && o->stream_id == 600 &&
                     o->error_code == 0x1) s++;
            else if (o->kind == FAKE_OP_RESET && o->stream_id == 600 &&
                     o->error_code == 0x1) rst++;
            else other++;
        }
        MOQ_TEST_CHECK_EQ_INT(w, 1);
        MOQ_TEST_CHECK_EQ_INT(s, 1);
        MOQ_TEST_CHECK_EQ_INT(rst, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }
    {
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.server_bridge,
                                                           tgt_ref);
        MOQ_TEST_CHECK(by_id && by_id == by_ref && by_id->active &&
                       by_id->local_send_closed && !by_id->peer_send_closed);
    }
    failures += exh_check_seed(&tp, seed_ref, "reset-serviced");
    MOQ_TEST_CHECK(request_registry_find_by_streamref(tp.server, blk_ref).kind
                   == MOQ_REQ_ANNOUNCEMENT);

    /* The legal later peer terminal (the peer's RESET_STREAM answering our
     * STOP_SENDING) releases exactly the target NORMAL drain and physically
     * retires the mapping, nonfatal and open. */
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));
    {
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "reset-terminal");
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.server_bridge,
                                                           tgt_ref);
        MOQ_TEST_CHECK((!by_id || !by_id->active) &&
                       (!by_ref || !by_ref->active));
    }
    failures += exh_carrier_equals(tp.server, &csnap, "reset-terminal");
    failures += exh_check_seed(&tp, seed_ref, "reset-terminal");

    /* Repeat the terminal + service: inert -- no drain mutation, no resurrection,
     * carrier pool exact and the seed's pinned registry/receiving/mapping
     * inventory conserved. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    {
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "reset-terminal-again");
    }
    failures += exh_carrier_equals(tp.server, &csnap, "reset-terminal-again");
    failures += exh_check_seed(&tp, seed_ref, "reset-terminal-again");
    MOQ_TEST_CHECK(request_registry_find_by_streamref(tp.server, blk_ref).kind
                   == MOQ_REQ_ANNOUNCEMENT);

    test_pair_destroy(&tp);
    return failures;
}


/* Reset-lifecycle discriminator 1: the bidi RESET endpoint op returns
 * WOULD_BLOCK, then is accepted. The pending reset carries the exact target ref
 * and code; local-send closure is NOT applied before endpoint acceptance; after
 * acceptance the RESET endpoint op is emitted, local-send is closed, and both
 * bridge-ID lookups remain while the NORMAL drain is owed; the later peer
 * terminal releases exactly that drain and retires the mapping; a repeat is
 * inert. */
int test_noslot_reset_bidi_wouldblock_then_accept(void)
{
    int failures = 0;
    test_pair_t tp;
    moq_stream_ref_t seed_ref, blk_ref;
    if (exh_setup(&tp, 64, &seed_ref, &blk_ref) < 0) return 1;
    size_t cap = tp.server->noslot_carrier_cap;
    MOQ_TEST_CHECK(cap > 0 && cap <= EXH_CARRIER_MAX);
    for (size_t i = 0; i < cap; i++)
        MOQ_TEST_CHECK(noslot_carrier_install(
            tp.server, moq_stream_ref_from_u64(0x9000 + i), false));
    nob_ring_t ring_before;
    nob_ring_snap(tp.server, &ring_before);

    uint8_t tgt[128];
    size_t tgt_len = exh_encode_subscribe(tgt, sizeof(tgt), 2);
    MOQ_TEST_CHECK(tgt_len > 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_bidi_bytes(tp.server_bridge, 600, tgt,
                                                     tgt_len, false, 0),
        (int)MOQ_OK);
    bridge_stream_entry_t *te = bridge_find_by_id(tp.server_bridge, 600);
    MOQ_TEST_CHECK(te != NULL);
    if (!te) { test_pair_destroy(&tp); return failures + 1; }
    moq_stream_ref_t tgt_ref = te->ref;
    /* The owed drain the target added. */
    nob_ring_t ring_owed;
    nob_ring_plus(&ring_before, tgt_ref, MOQ_DRAIN_NORMAL, &ring_owed);

    /* Block the RESET endpoint op and service: the RESET enqueues a
     * PENDING_RESET_STREAM carrying the exact target ref and code, local-send is
     * NOT yet closed, the mapping resolves by both ids, and the NORMAL drain is
     * owed. */
    tp.server_ep.block_reset = true;
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    {
        int found = 0;
        for (size_t i = 0; i < tp.server_bridge->pending_count; i++) {
            const bridge_pending_item_t *p = &tp.server_bridge->pending[i];
            if (p->kind == PENDING_RESET_STREAM &&
                p->stream_ref._v == tgt_ref._v && p->error_code == 0x1) found++;
        }
        MOQ_TEST_CHECK_EQ_INT(found, 1);
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.server_bridge,
                                                           tgt_ref);
        MOQ_TEST_CHECK(by_id && by_id == by_ref && by_id->active &&
                       !by_id->local_send_closed);
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_owed, "reset-wb-blocked");
    }

    /* Accept: unblock and service -- exactly one RESET op on the target id,
     * local-send now closed, mapping still resolving by both ids, drain owed. */
    tp.server_ep.block_reset = false;
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    {
        int rst = 0, other = 0;
        for (size_t i = 0; i < tp.server_ep.count; i++) {
            const fake_op_t *o = &tp.server_ep.ops[i];
            if (o->kind == FAKE_OP_RESET && o->stream_id == 600 &&
                o->error_code == 0x1) rst++;
            else other++;
        }
        MOQ_TEST_CHECK_EQ_INT(rst, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.server_bridge,
                                                           tgt_ref);
        MOQ_TEST_CHECK(by_id && by_id == by_ref && by_id->active &&
                       by_id->local_send_closed);
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_owed, "reset-wb-accepted");
    }

    /* Peer terminal releases exactly the target drain and retires the mapping. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));
    {
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "reset-wb-terminal");
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.server_bridge,
                                                           tgt_ref);
        MOQ_TEST_CHECK((!by_id || !by_id->active) &&
                       (!by_ref || !by_ref->active));
    }

    /* Repeat the terminal: inert. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    {
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "reset-wb-inert");
    }
    (void)seed_ref; (void)blk_ref;
    test_pair_destroy(&tp);
    return failures;
}


/* Reset-lifecycle discriminator 2 (control): a uni RESET_DATA retires its
 * physical mapping IMMEDIATELY on endpoint acceptance -- no PENDING carrier and
 * no request-bidi drain lifecycle imposed, in deliberate contrast with the bidi
 * reset above. */
int test_uni_reset_data_immediate_retire(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init_full(&tp, 0, false, 64, 0, 0) < 0) return 1;
    if (!setup_handshake(&tp)) { test_pair_destroy(&tp); return 1; }

    /* SERVER subscribes, CLIENT publishes and opens a local-origin data uni. */
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
    MOQ_TEST_CHECK(moq_session_accept_subscribe(tp.client, csub, &acc, 0)
                   == MOQ_OK);
    pump_until_quiescent(&tp, 20, 0);
    while (moq_session_poll_events(tp.server, &ev, 1) > 0) moq_event_cleanup(&ev);

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
    bridge_stream_entry_t *e = bridge_find_by_id(tp.client_bridge, uni_sid);
    MOQ_TEST_CHECK(e != NULL);
    if (!e) { test_pair_destroy(&tp); return failures + 1; }
    MOQ_TEST_CHECK(e->kind == BRIDGE_STREAM_UNI &&
                   e->origin == BRIDGE_ORIGIN_LOCAL && e->active);
    moq_stream_ref_t uni_ref = e->ref;
    size_t drain_before = tp.client->drain_ref_count;

    /* Reset the subgroup -> a uni RESET_DATA action. Service and accept it. */
    MOQ_TEST_CHECK(moq_session_reset_subgroup(tp.client, sg, 0x1, 0) == MOQ_OK);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK(held_bridge_service(tp.client_bridge, 0) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));

    /* Exactly one RESET/ABORT op on the uni id; the mapping retired IMMEDIATELY
     * by both ids; NO drain ref was imposed; no PENDING reset lingers. */
    {
        int rst = 0;
        for (size_t i = 0; i < tp.client_ep.count; i++) {
            const fake_op_t *o = &tp.client_ep.ops[i];
            if ((o->kind == FAKE_OP_RESET || o->kind == FAKE_OP_ABORT) &&
                o->stream_id == uni_sid) rst++;
        }
        MOQ_TEST_CHECK_EQ_INT(rst, 1);
    }
    {
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.client_bridge,
                                                         uni_sid);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.client_bridge,
                                                           uni_ref);
        MOQ_TEST_CHECK((!by_id || !by_id->active) &&
                       (!by_ref || !by_ref->active));
    }
    MOQ_TEST_CHECK_EQ_SIZE(tp.client->drain_ref_count, drain_before);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));

    test_pair_destroy(&tp);
    return failures;
}


/* The bridge pending set holds EXACTLY the declared local RESET and nothing
 * else: one item, of the exact PENDING_RESET_STREAM shape a plain reset carries
 * -- no payload, no FIN, no owned action. Called at both the blocked checkpoint
 * and after the peer terminal from ONE checker, so the two phases cannot drift
 * and a mutation to any formerly-unchecked field (data/data_len/fin/owns_action)
 * fails here. */
static int nob_pending_is_only_reset(const moq_transport_bridge_t *b,
                                     uint64_t stream_id, moq_stream_ref_t ref,
                                     uint64_t code, const char *what)
{
    int bad = 0;
    if (b->pending_count != 1) {
        fprintf(stderr, "NOB %s: pending_count %lu, expected 1\n", what,
                (unsigned long)b->pending_count);
        return bad + 1;
    }
    const bridge_pending_item_t *p = &b->pending[0];
    if ((int)p->kind != (int)PENDING_RESET_STREAM) {
        fprintf(stderr, "NOB %s: pending kind %d, expected RESET\n", what,
                (int)p->kind);
        bad++;
    }
    if (p->stream_id != stream_id) bad++;
    if (p->stream_ref._v != ref._v) bad++;
    if (p->error_code != code) bad++;
    if (p->data != NULL) bad++;
    if (p->data_len != 0) bad++;
    if (p->fin) bad++;
    if (p->owns_action) bad++;
    if (bad)
        fprintf(stderr, "NOB %s: pending RESET item not the exact declared"
                " shape (id/ref/code/data/data_len/fin/owns_action)\n", what);
    return bad;
}


/* Both sides of the pair stay quiet and open: neither bridge fatal or closed,
 * the client bridge holds no pending work, both sessions stay ESTABLISHED with
 * empty event and action rings, and the client endpoint emits nothing. The
 * server endpoint's exact per-phase output is asserted separately by the caller. */
static int nob_pair_quiet(const test_pair_t *tp, const char *what)
{
    int bad = 0;
    if (moq_transport_bridge_is_fatal(tp->server_bridge) ||
        moq_transport_bridge_is_closed(tp->server_bridge) ||
        moq_transport_bridge_is_fatal(tp->client_bridge) ||
        moq_transport_bridge_is_closed(tp->client_bridge)) {
        fprintf(stderr, "NOB %s: a bridge is fatal/closed\n", what);
        bad++;
    }
    if (moq_transport_bridge_has_pending(tp->client_bridge)) {
        fprintf(stderr, "NOB %s: client bridge has pending work\n", what);
        bad++;
    }
    if ((int)tp->server->state != (int)MOQ_SESS_ESTABLISHED ||
        (int)tp->client->state != (int)MOQ_SESS_ESTABLISHED) {
        fprintf(stderr, "NOB %s: a session is not ESTABLISHED\n", what);
        bad++;
    }
    if (tp->server->event_tail != tp->server->event_head ||
        tp->server->action_tail != tp->server->action_head ||
        tp->client->event_tail != tp->client->event_head ||
        tp->client->action_tail != tp->client->action_head) {
        fprintf(stderr, "NOB %s: a session ring is non-empty\n", what);
        bad++;
    }
    if (tp->client_ep.count != 0) {
        fprintf(stderr, "NOB %s: client endpoint emitted %zu ops\n", what,
                tp->client_ep.count);
        bad++;
    }
    return bad;
}


/* Reset-lifecycle discriminator 3: the OPPOSITE ordering -- the peer terminal
 * arrives WHILE the endpoint RESET is still pending (before local acceptance).
 * The peer RESET releases the exact NORMAL drain and retires the mapping without
 * losing the locally owed RESET; unblocking/service then emits that RESET
 * exactly once on the saved transport id, clears the pending item, stays
 * nonfatal/open, resurrects no mapping or drain; a repeat is inert. */
int test_noslot_reset_bidi_peer_before_accept(void)
{
    int failures = 0;
    test_pair_t tp;
    moq_stream_ref_t seed_ref, blk_ref;
    if (exh_setup(&tp, 64, &seed_ref, &blk_ref) < 0) return 1;
    size_t cap = tp.server->noslot_carrier_cap;
    MOQ_TEST_CHECK(cap > 0 && cap <= EXH_CARRIER_MAX);
    for (size_t i = 0; i < cap; i++)
        MOQ_TEST_CHECK(noslot_carrier_install(
            tp.server, moq_stream_ref_from_u64(0x9000 + i), false));
    nob_ring_t ring_before;
    nob_ring_snap(tp.server, &ring_before);

    uint8_t tgt[128];
    size_t tgt_len = exh_encode_subscribe(tgt, sizeof(tgt), 2);
    MOQ_TEST_CHECK(tgt_len > 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_bidi_bytes(tp.server_bridge, 600, tgt,
                                                     tgt_len, false, 0),
        (int)MOQ_OK);
    bridge_stream_entry_t *te = bridge_find_by_id(tp.server_bridge, 600);
    MOQ_TEST_CHECK(te != NULL);
    if (!te) { test_pair_destroy(&tp); return failures + 1; }
    moq_stream_ref_t tgt_ref = te->ref;
    nob_ring_t ring_owed;
    nob_ring_plus(&ring_before, tgt_ref, MOQ_DRAIN_NORMAL, &ring_owed);

    /* Endpoint RESET blocks: exactly one pending reset carrying the target
     * transport-id/ref/code; local send not yet closed; and the exact NORMAL
     * drain for the target ref is owed (ring_before + one MOQ_DRAIN_NORMAL for
     * tgt_ref), so a product that never installed the drain fails here. */
    tp.server_ep.block_reset = true;
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    /* The whole pending set is exactly one item, of the exact declared RESET
     * shape (checked here and re-checked after the peer terminal by the SAME
     * checker against the SAME declared constants -- a plain reset carries no
     * payload, FIN or owned action). */
    {
        failures += nob_pending_is_only_reset(tp.server_bridge, 600, tgt_ref,
                                              0x1, "reset-wb-blocked");
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        MOQ_TEST_CHECK(by_id && by_id->active && !by_id->local_send_closed);
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_owed, "reset-wb-blocked");
    }

    /* Peer RESET arrives WHILE the endpoint reset is still pending: releases the
     * exact drain (back to ring_before), retires the mapping by BOTH saved
     * transport id and stream ref, keeps the SAME single pending local RESET
     * unchanged, stays nonfatal/open, and does no other work at all -- no
     * endpoint op, no session event, no queued session action. */
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    /* Both sides stay quiet and open, the server endpoint emits nothing, and the
     * SAME single RESET survives unchanged. */
    failures += nob_pair_quiet(&tp, "reset-peer-first-drain");
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
    {
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "reset-peer-first-drain");
        /* The SAME single RESET, exact declared shape -- an unrelated pending
         * item, or a mutation to any field, fails here. */
        failures += nob_pending_is_only_reset(tp.server_bridge, 600, tgt_ref,
                                              0x1, "reset-peer-first-drain");
        /* The mapping is gone by both keys the moment the peer terminal lands --
         * a product that retained it until the local RESET succeeded fails here,
         * BEFORE the unblock/service phase. */
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.server_bridge,
                                                           tgt_ref);
        MOQ_TEST_CHECK(!by_id || !by_id->active);
        MOQ_TEST_CHECK(!by_ref || !by_ref->active);
    }

    /* Unblock and service: the RESET is emitted exactly once on the saved id,
     * the pending item clears, and no mapping/drain is resurrected. */
    tp.server_ep.block_reset = false;
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    {
        int rst = 0, other = 0;
        for (size_t i = 0; i < tp.server_ep.count; i++) {
            const fake_op_t *o = &tp.server_ep.ops[i];
            if (o->kind == FAKE_OP_RESET && o->stream_id == 600 &&
                o->error_code == 0x1) rst++;
            else other++;
        }
        MOQ_TEST_CHECK_EQ_INT(rst, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
        MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.server_bridge));
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.server_bridge,
                                                           tgt_ref);
        MOQ_TEST_CHECK((!by_id || !by_id->active) &&
                       (!by_ref || !by_ref->active));
        failures += nob_pair_quiet(&tp, "reset-peer-first-done");
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "reset-peer-first-done");
    }

    /* Repeat the peer terminal + service: proves the FULL postcondition again,
     * not just endpoint count plus ring -- no output, pending empty, mapping
     * absent by both keys, exact ring_before, open/nonfatal, no session work. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
    {
        MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.server_bridge));
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp.server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp.server_bridge,
                                                           tgt_ref);
        MOQ_TEST_CHECK((!by_id || !by_id->active) &&
                       (!by_ref || !by_ref->active));
        failures += nob_pair_quiet(&tp, "reset-peer-first-inert");
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "reset-peer-first-inert");
    }
    (void)seed_ref; (void)blk_ref;
    test_pair_destroy(&tp);
    return failures;
}


/* -- shared oracles for the no-slot carrier peer-teardown lifecycle -------- */

/* Expected bridge-entry flag shape. */
typedef struct {
    bool active, peer_send_closed, local_send_closed, peer_stop_received;
    bool pending_retry, pending_fin, fin_retained, pending_reset, pending_stop;
    bool aborting;
} bshape_t;


/* The entry is a live peer-origin BIDI cross-resolving by both keys (id and ref
 * distinct), with exactly the declared flag shape. */
static int bcheck_entry(moq_transport_bridge_t *b, uint64_t id,
                        moq_stream_ref_t ref, const bshape_t *w, const char *what)
{
    int bad = 0;
    bridge_stream_entry_t *by_id = bridge_find_by_id(b, id);
    bridge_stream_entry_t *by_ref = bridge_find_by_ref(b, ref);
    if (!by_id) {
        fprintf(stderr, "NOB %s: no entry by id %llu\n", what,
                (unsigned long long)id);
        return 1;
    }
    if (by_id != by_ref) {
        fprintf(stderr, "NOB %s: id/ref do not cross-resolve to one entry\n",
                what);
        bad++;
    }
    if (by_id->transport_id != id || by_id->ref._v != ref._v || id == ref._v) {
        fprintf(stderr, "NOB %s: id/ref not distinct/consistent\n", what);
        bad++;
    }
    if ((int)by_id->kind != (int)BRIDGE_STREAM_BIDI ||
        (int)by_id->origin != (int)BRIDGE_ORIGIN_PEER) {
        fprintf(stderr, "NOB %s: not a peer-origin BIDI\n", what);
        bad++;
    }
#define BF(field) do { if (by_id->field != w->field) { \
        fprintf(stderr, "NOB %s: entry " #field " = %d, expected %d\n", what, \
                (int)by_id->field, (int)w->field); bad++; } } while (0)
    BF(active); BF(peer_send_closed); BF(local_send_closed);
    BF(peer_stop_received); BF(pending_retry); BF(pending_fin);
    BF(fin_retained); BF(pending_reset); BF(pending_stop); BF(aborting);
#undef BF
    return bad;
}


/* The retired postcondition, reasserted in full at every RESET checkpoint: the
 * target carrier is gone, only the unrelated carrier remains, the pre-target
 * drain multiset is unchanged, the retainability identity is cleared, the
 * mapping is absent by both keys, the seed's registry answer / receiving state /
 * live bridge mapping (the exact inventory exh_check_seed pins) are conserved,
 * both bridges are open/nonfatal with no pending work, both sessions ESTABLISHED
 * with empty event/action queues, and neither endpoint emitted anything. */
static int carrier_retired_post(test_pair_t *tp, moq_stream_ref_t tgt_ref,
                                moq_stream_ref_t other, moq_stream_ref_t seed_ref,
                                const nob_ring_t *ring_before, const char *what)
{
    int bad = 0;
    if (noslot_carrier_find(tp->server, tgt_ref) >= 0) {
        fprintf(stderr, "NOB %s: target carrier still present\n", what);
        bad++;
    }
    exh_carrier_snap_t want;
    memset(&want, 0, sizeof(want));
    want.count = 1; want.ref[0] = other._v; want.fin[0] = 0;
    bad += exh_carrier_equals(tp->server, &want, what);
    {
        nob_ring_t now;
        nob_ring_snap(tp->server, &now);
        bad += nob_ring_equals(&now, ring_before, what);
    }
    if (moq_session_has_transport_stream(tp->server, tgt_ref)) {
        fprintf(stderr, "NOB %s: retainability identity still present\n", what);
        bad++;
    }
    {
        bridge_stream_entry_t *by_id = bridge_find_by_id(tp->server_bridge, 600);
        bridge_stream_entry_t *by_ref = bridge_find_by_ref(tp->server_bridge,
                                                           tgt_ref);
        if ((by_id && by_id->active) || (by_ref && by_ref->active)) {
            fprintf(stderr, "NOB %s: mapping not retired by both keys\n", what);
            bad++;
        }
    }
    bad += exh_check_seed(tp, seed_ref, what);
    if (moq_transport_bridge_is_fatal(tp->server_bridge) ||
        moq_transport_bridge_is_closed(tp->server_bridge) ||
        moq_transport_bridge_is_fatal(tp->client_bridge) ||
        moq_transport_bridge_is_closed(tp->client_bridge) ||
        moq_transport_bridge_has_pending(tp->server_bridge) ||
        moq_transport_bridge_has_pending(tp->client_bridge)) {
        fprintf(stderr, "NOB %s: a bridge is fatal/closed/pending\n", what);
        bad++;
    }
    if ((int)tp->server->state != (int)MOQ_SESS_ESTABLISHED ||
        (int)tp->client->state != (int)MOQ_SESS_ESTABLISHED) {
        fprintf(stderr, "NOB %s: a session is not ESTABLISHED\n", what);
        bad++;
    }
    if (tp->server->event_tail != tp->server->event_head ||
        tp->server->action_tail != tp->server->action_head ||
        tp->client->event_tail != tp->client->event_head ||
        tp->client->action_tail != tp->client->action_head) {
        fprintf(stderr, "NOB %s: a session ring is non-empty\n", what);
        bad++;
    }
    if (tp->server_ep.count != 0 || tp->client_ep.count != 0) {
        fprintf(stderr, "NOB %s: endpoint emitted output this phase\n", what);
        bad++;
    }
    return bad;
}


/* Arm a genuine BLOCKED no-slot carrier for the target through real inbound
 * request bytes, and prove it is retained before any observed state becomes a
 * baseline. exh_setup exhausts the sub pool (no send-buffer blocker, so no
 * residual output); the drain ring is filled so the target's no-FIN terminal
 * blocks on drain capacity and is RETAINED in a carrier
 * (session_subscribe.c:1411). The target uses the protocol-conformant first
 * client Request ID 0 (draft-18 section 10.1); the header-only seed consumes no
 * wire id. An unrelated carrier is installed first and must survive untouched.
 *   return  0: armed; other, tgt_ref, seed_ref, ring_before all set; tp live
 *   return -1: exh_setup failed and already destroyed tp
 *   return  1: a later step failed; tp is live (caller destroys). */
static int carrier_target_arm(test_pair_t *tp, moq_stream_ref_t *other,
                              moq_stream_ref_t *tgt_ref,
                              moq_stream_ref_t *seed_ref,
                              nob_ring_t *ring_before)
{
    if (exh_setup(tp, 0, seed_ref, NULL) < 0) return -1;
    if (tp->server->noslot_carrier_cap < 2) return 1;
    *other = moq_stream_ref_from_u64(0x9999);
    if (!noslot_carrier_install(tp->server, *other, false)) return 1;
    while (tp->server->drain_ref_count < tp->server->drain_ref_cap)
        if (!drain_ref_add(tp->server, moq_stream_ref_from_u64(
                0x4000 + tp->server->drain_ref_count))) return 1;

    /* The FULL pre-target drain multiset -- the baseline every teardown phase
     * is measured against. */
    nob_ring_snap(tp->server, ring_before);
    fake_endpoint_clear_ops(&tp->client_ep);
    fake_endpoint_clear_ops(&tp->server_ep);

    uint8_t tgt[128];
    size_t tgt_len = exh_encode_subscribe(tgt, sizeof(tgt), 0);
    if (tgt_len == 0) return 1;
    moq_result_t irc = moq_transport_bridge_on_peer_bidi_bytes(
        tp->server_bridge, 600, tgt, tgt_len, false, 0);
    /* Exactly WOULD_BLOCK: the terminal is retained, not completed. */
    if (irc != MOQ_ERR_WOULD_BLOCK) {
        fprintf(stderr, "carrier arm: ingress %d, expected WOULD_BLOCK\n",
                (int)irc);
        return 1;
    }
    bridge_stream_entry_t *te = bridge_find_by_id(tp->server_bridge, 600);
    if (!te) return 1;
    *tgt_ref = te->ref;

    /* The exact live retained shape: peer-origin BIDI, pending_retry only. */
    bshape_t armed = { .active = true, .pending_retry = true };
    if (bcheck_entry(tp->server_bridge, 600, *tgt_ref, &armed, "carrier arm")
        != 0) return 1;
    /* Exact carrier multiset {target, other}, both FIN=false. */
    {
        exh_carrier_snap_t want;
        memset(&want, 0, sizeof(want));
        want.count = 2;
        want.ref[0] = tgt_ref->_v; want.fin[0] = 0;
        want.ref[1] = other->_v;   want.fin[1] = 0;
        if (exh_carrier_equals(tp->server, &want, "carrier arm") != 0) return 1;
    }
    if (!moq_session_has_transport_stream(tp->server, *tgt_ref)) return 1;
    /* The drain multiset is unchanged by the refusal (no drain was taken). */
    {
        nob_ring_t now;
        nob_ring_snap(tp->server, &now);
        if (nob_ring_equals(&now, ring_before, "carrier arm ring") != 0)
            return 1;
    }
    /* The seed's declared registry / receiving / mapping inventory is
     * conserved across the refusal. */
    if (exh_check_seed(tp, *seed_ref, "carrier arm seed") != 0) return 1;
    /* Zero output, zero events, zero queued actions on BOTH sides before any
     * recorder clear; both sessions/bridges established, open, nonfatal. */
    if (tp->server_ep.count != 0 || tp->client_ep.count != 0) {
        fprintf(stderr, "carrier arm: stray endpoint output\n"); return 1;
    }
    if (tp->server->event_tail != tp->server->event_head ||
        tp->server->action_tail != tp->server->action_head ||
        tp->client->event_tail != tp->client->event_head ||
        tp->client->action_tail != tp->client->action_head) {
        fprintf(stderr, "carrier arm: stray session event/action\n"); return 1;
    }
    if (moq_transport_bridge_is_fatal(tp->server_bridge) ||
        moq_transport_bridge_is_closed(tp->server_bridge) ||
        moq_transport_bridge_is_fatal(tp->client_bridge) ||
        moq_transport_bridge_is_closed(tp->client_bridge) ||
        (int)tp->server->state != (int)MOQ_SESS_ESTABLISHED ||
        (int)tp->client->state != (int)MOQ_SESS_ESTABLISHED) {
        fprintf(stderr, "carrier arm: session/bridge not established/open\n");
        return 1;
    }
    return 0;
}


/* The immediate RESET retired the carrier but our half of the request bidi is
 * still open: its cancellation is owed to the service pass, not issued inside
 * the callback. */
static int carrier_cancel_owed(test_pair_t *tp, moq_stream_ref_t tgt_ref,
                               moq_stream_ref_t other, moq_stream_ref_t seed_ref,
                               const nob_ring_t *ring_before, const char *what)
{
    int bad = 0;
    if (noslot_carrier_find(tp->server, tgt_ref) >= 0) {
        fprintf(stderr, "NOB %s: target carrier still present\n", what);
        bad++;
    }
    exh_carrier_snap_t want;
    memset(&want, 0, sizeof(want));
    want.count = 1; want.ref[0] = other._v; want.fin[0] = 0;
    bad += exh_carrier_equals(tp->server, &want, what);
    {
        nob_ring_t now;
        nob_ring_snap(tp->server, &now);
        bad += nob_ring_equals(&now, ring_before, what);
    }
    if (moq_session_has_transport_stream(tp->server, tgt_ref)) {
        fprintf(stderr, "NOB %s: retainability identity still present\n", what);
        bad++;
    }
    bad += exh_check_seed(tp, seed_ref, what);
    if (moq_transport_bridge_is_fatal(tp->server_bridge) ||
        moq_transport_bridge_is_closed(tp->server_bridge) ||
        (int)tp->server->state != (int)MOQ_SESS_ESTABLISHED ||
        tp->server->event_tail != tp->server->event_head ||
        tp->server->action_tail != tp->server->action_head) {
        fprintf(stderr, "NOB %s: server not open/established/empty\n", what);
        bad++;
    }
    bridge_stream_entry_t *e = bridge_find_by_id(tp->server_bridge, 600);
    if (!e || !e->active || e->ref._v != tgt_ref._v || !e->peer_reset_accepted ||
        e->local_send_closed) {
        fprintf(stderr, "NOB %s: our open half is not kept for its cancel\n", what);
        bad++;
    }
    if (tp->server_ep.count != 0 || tp->client_ep.count != 0 ||
        tp->server_bridge->pending_count != 0) {
        fprintf(stderr, "NOB %s: output inside the callback\n", what);
        bad++;
    }
    return bad;
}


/* A retained no-slot carrier is RETIRED when a peer RESET terminates the
 * request (draft-18 sections 3.3.2, 11.4.1); the request is then over, and
 * the service pass cancels our still-open half once, with CANCELLED. The full
 * retired postcondition is reasserted after that service, at the repeated
 * RESET before service, and after repeat service -- never merely
 * count/carrier/ring. */
int test_noslot_carrier_peer_reset(void)
{
    int failures = 0;
    test_pair_t tp;
    moq_stream_ref_t other, tgt_ref, seed_ref;
    nob_ring_t ring_before;
    int rc = carrier_target_arm(&tp, &other, &tgt_ref, &seed_ref, &ring_before);
    if (rc < 0) return 1;
    if (rc > 0) { test_pair_destroy(&tp); return 1; }

    /* Immediate peer RESET. */
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    failures += carrier_cancel_owed(&tp, tgt_ref, other, seed_ref, &ring_before,
                                    "carrier-reset immediate");

    /* First service: exactly our half's CANCELLED reset, then retired. */
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)1);
    if (tp.server_ep.count == 1) {
        MOQ_TEST_CHECK_EQ_INT((int)tp.server_ep.ops[0].kind, (int)FAKE_OP_RESET);
        MOQ_TEST_CHECK(tp.server_ep.ops[0].stream_id == 600);
        MOQ_TEST_CHECK(tp.server_ep.ops[0].error_code == MOQ_STREAM_ERROR_CANCELLED);
    }
    fake_endpoint_clear_ops(&tp.server_ep);
    failures += carrier_retired_post(&tp, tgt_ref, other, seed_ref, &ring_before,
                                     "carrier-reset service");

    /* Repeated peer RESET (before service). */
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    failures += carrier_retired_post(&tp, tgt_ref, other, seed_ref, &ring_before,
                                     "carrier-reset repeat");

    /* Repeat service. */
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    failures += carrier_retired_post(&tp, tgt_ref, other, seed_ref, &ring_before,
                                     "carrier-reset repeat service");
    test_pair_destroy(&tp);
    return failures;
}


/* Peer STOP_SENDING terminates the request and retires the exact carrier,
 * while the bridge preserves its half-close lifecycle. The literal lifecycle is
 * pinned end to end: the half-closed mapping and its send-half RESET pending
 * item after STOP; exactly one RESET(600,0x1) on first service with the mapping
 * still half-closed and pending_retry cleared; and the fully retired
 * postcondition after the legal final peer RESET, reproduced on repeat. */
int test_noslot_carrier_peer_stop(void)
{
    int failures = 0;
    test_pair_t tp;
    moq_stream_ref_t other, tgt_ref, seed_ref;
    nob_ring_t ring_before;
    int rc = carrier_target_arm(&tp, &other, &tgt_ref, &seed_ref, &ring_before);
    if (rc < 0) return 1;
    if (rc > 0) { test_pair_destroy(&tp); return 1; }

    exh_carrier_snap_t car_after;
    memset(&car_after, 0, sizeof(car_after));
    car_after.count = 1; car_after.ref[0] = other._v; car_after.fin[0] = 0;

    /* 1. Immediately after STOP: carrier retired; drain ring unchanged and the
     *    seed's registry answer / receiving state / live bridge mapping
     *    conserved (the exact inventory exh_check_seed pins, not every field);
     *    the mapping is half-closed (local send closed, peer send open,
     *    peer_stop_received, original pending_retry intact); the ONLY server
     *    pending item is the send-half RESET the bridge declares (id 600, code
     *    0x1, stream_ref 0, no payload/FIN/owned action); and BOTH sides are
     *    otherwise quiet/open (both sessions ESTABLISHED with empty event/action
     *    rings, both bridges nonfatal, the client bridge with no pending work,
     *    endpoints exactly zero). The server bridge's one declared pending RESET
     *    is checked by the phase-specific pending assertion, not a generic
     *    no-pending check. */
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_transport_bridge_on_peer_stop_sending(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK(noslot_carrier_find(tp.server, tgt_ref) < 0);
    failures += exh_carrier_equals(tp.server, &car_after, "carrier-stop");
    {
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "carrier-stop ring");
    }
    failures += exh_check_seed(&tp, seed_ref, "carrier-stop seed");
    MOQ_TEST_CHECK(!moq_session_has_transport_stream(tp.server, tgt_ref));
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
    MOQ_TEST_CHECK_EQ_SIZE(tp.client_ep.count, (size_t)0);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->event_tail - tp.server->event_head,
                           (size_t)0);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->action_tail - tp.server->action_head,
                           (size_t)0);
    {
        bshape_t half = { .active = true, .local_send_closed = true,
                          .peer_stop_received = true, .pending_retry = true };
        failures += bcheck_entry(tp.server_bridge, 600, tgt_ref, &half,
                                 "carrier-stop half-closed");
    }
    failures += nob_pending_is_only_reset(tp.server_bridge, 600,
                                          moq_stream_ref_from_u64(0), 0x1,
                                          "carrier-stop pending");
    /* Both sides quiet/open (both sessions ESTABLISHED, both rings empty, client
     * bridge no pending, both bridges nonfatal, client endpoint zero). This does
     * NOT assert the server bridge is pending-free -- that is the one declared
     * RESET, pinned above. */
    failures += nob_pair_quiet(&tp, "carrier-stop immediate");
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(tp.server->state != MOQ_SESS_CLOSED);

    /* 2. First service: exactly one FAKE_OP_RESET(600, 0x1) and no other op; no
     *    request WRITE; server pending empty; mapping still half-closed with
     *    pending_retry CLEARED; carrier/unrelated/ring unchanged and the seed's
     *    registry/receiving/mapping inventory conserved; BOTH sides quiet/open;
     *    the semantic target identity still absent. */
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    {
        int rst = 0, wr = 0, other_ops = 0;
        for (size_t i = 0; i < tp.server_ep.count; i++) {
            const fake_op_t *o = &tp.server_ep.ops[i];
            if (o->kind == FAKE_OP_RESET && o->stream_id == 600 &&
                o->error_code == 0x1) rst++;
            else if (o->kind == FAKE_OP_WRITE && o->stream_id == 600) wr++;
            else other_ops++;
        }
        MOQ_TEST_CHECK_EQ_INT(rst, 1);
        MOQ_TEST_CHECK_EQ_INT(wr, 0);
        MOQ_TEST_CHECK_EQ_INT(other_ops, 0);
    }
    MOQ_TEST_CHECK_EQ_SIZE(tp.client_ep.count, (size_t)0);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.server_bridge));
    {
        bshape_t half_done = { .active = true, .local_send_closed = true,
                               .peer_stop_received = true };
        failures += bcheck_entry(tp.server_bridge, 600, tgt_ref, &half_done,
                                 "carrier-stop serviced");
    }
    MOQ_TEST_CHECK(noslot_carrier_find(tp.server, tgt_ref) < 0);
    failures += exh_carrier_equals(tp.server, &car_after, "carrier-stop serviced car");
    {
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "carrier-stop serviced ring");
    }
    failures += exh_check_seed(&tp, seed_ref, "carrier-stop serviced seed");
    /* Complete both-sides quiet/open AFTER service: both bridges open/nonfatal
     * with no pending work (the send-half RESET was serviced), both sessions
     * ESTABLISHED with empty event/action rings, client endpoint zero. */
    failures += nob_pair_quiet(&tp, "carrier-stop serviced");
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_session_has_transport_stream(tp.server, tgt_ref));

    /* 3. Final legal peer terminal (RESET of the peer's send half). Clear the
     *    recorders only now, AFTER step 2's serviced RESET has been checked, so
     *    the peer RESET's own synchronous output is measured against zero. */
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);   /* no new output */
    failures += carrier_retired_post(&tp, tgt_ref, other, seed_ref, &ring_before,
                                     "carrier-stop final");
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    failures += carrier_retired_post(&tp, tgt_ref, other, seed_ref, &ring_before,
                                     "carrier-stop final service");
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_reset(tp.server_bridge, 600,
                                                       0x1, 0), (int)MOQ_OK);
    failures += carrier_retired_post(&tp, tgt_ref, other, seed_ref, &ring_before,
                                     "carrier-stop final repeat");
    fake_endpoint_clear_ops(&tp.server_ep);
    fake_endpoint_clear_ops(&tp.client_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    failures += carrier_retired_post(&tp, tgt_ref, other, seed_ref, &ring_before,
                                     "carrier-stop final repeat service");
    test_pair_destroy(&tp);
    return failures;
}


/* No-slot carrier pool full AND the whole per-stream terminal is
 * inadmissible (full drain ring, even with action slots free). The refused
 * no-FIN target closes the session with the draft-18 Session INTERNAL_ERROR
 * (0x1) -- no partial STOP/RESET, no protocol-violation/unauthorized code, no
 * bare WOULD_BLOCK -- and through the physical bridge the outcome is a graceful
 * close, never a bridge fatal. */
int test_noslot_exhaustion_close(void)
{
    int failures = 0;
    test_pair_t tp;
    moq_stream_ref_t seed_ref;
    if (exh_setup(&tp, 0, &seed_ref, NULL) < 0) return 1;

    /* The exact close reason the product declares. */
    static const char k_reason[] =
        "no-owner admission carrier storage and per-stream"
        " terminal both inadmissible";
    const size_t reason_len = sizeof(k_reason) - 1;

    /* Bounded fills over the DECLARED carrier + drain capacities. */
    size_t ccap = tp.server->noslot_carrier_cap;
    MOQ_TEST_CHECK(ccap > 0 && ccap <= EXH_CARRIER_MAX);
    for (size_t i = 0; i < ccap; i++)
        MOQ_TEST_CHECK(noslot_carrier_install(
            tp.server, moq_stream_ref_from_u64(0x9000 + i), false));
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->noslot_carrier_count, ccap);
    size_t dcap = tp.server->drain_ref_cap;
    MOQ_TEST_CHECK(dcap > 0);
    for (size_t i = 0; i < dcap; i++)
        MOQ_TEST_CHECK(drain_ref_add(tp.server,
                                     moq_stream_ref_from_u64(0xA000 + i)));
    MOQ_TEST_CHECK_EQ_SIZE(tp.server->drain_ref_count, dcap);

    exh_carrier_snap_t csnap;
    exh_carrier_capture(tp.server, &csnap);
    nob_ring_t ring_before;
    nob_ring_snap(tp.server, &ring_before);
    /* Action slots ARE free: the close is forced by carrier + drain exhaustion,
     * not by an action shortage. */
    MOQ_TEST_CHECK(tp.server->action_tail == tp.server->action_head);
    MOQ_TEST_CHECK(action_queue_avail(tp.server) >= 2);

    uint8_t tgt[128];
    size_t tgt_len = exh_encode_subscribe(tgt, sizeof(tgt), 0);
    MOQ_TEST_CHECK(tgt_len > 0);
    moq_result_t rc = moq_transport_bridge_on_peer_bidi_bytes(
        tp.server_bridge, 600, tgt, tgt_len, false, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);                 /* never bare/unowned WB */
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(tp.server->state == MOQ_SESS_CLOSED);

    /* Exactly one queued CLOSE_SESSION carrying INTERNAL_ERROR (0x1) and the
     * declared reason bytes -- no STOP/RESET, no other action. */
    {
        size_t depth = tp.server->action_tail - tp.server->action_head;
        MOQ_TEST_CHECK_EQ_SIZE(depth, (size_t)1);
        int close_a = 0, other = 0;
        for (size_t i = tp.server->action_head; i != tp.server->action_tail;
             i++) {
            const moq_action_t *a =
                &tp.server->actions[i % tp.server->action_cap];
            if (a->kind == MOQ_ACTION_CLOSE_SESSION &&
                a->u.close_session.code == 0x1 &&
                a->u.close_session.reason.len == reason_len &&
                a->u.close_session.reason.data &&
                memcmp(a->u.close_session.reason.data, k_reason,
                       reason_len) == 0) close_a++;
            else other++;
        }
        MOQ_TEST_CHECK_EQ_INT(close_a, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* Exactly one SESSION_CLOSED (0x1 + the same reason), zero other events. */
    {
        moq_event_t ev;
        int closed = 0, other = 0;
        while (moq_session_poll_events(tp.server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                closed++;
                MOQ_TEST_CHECK_EQ_U64(ev.u.closed.code, (uint64_t)0x1);
                MOQ_TEST_CHECK(ev.u.closed.reason.len == reason_len &&
                               ev.u.closed.reason.data &&
                               memcmp(ev.u.closed.reason.data, k_reason,
                                      reason_len) == 0);
            } else {
                other++;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(closed, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* Every pre-existing carrier and drain entry remains identity/reason exact. */
    failures += exh_carrier_equals(tp.server, &csnap, "close-refused");
    {
        nob_ring_t now;
        nob_ring_snap(tp.server, &now);
        failures += nob_ring_equals(&now, &ring_before, "close-refused");
    }

    /* Service emits exactly one FAKE_OP_CLOSE with that code and reason and no
     * other endpoint operation; the bridge is closed, not fatal, no pending. */
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.server_bridge));
    {
        MOQ_TEST_CHECK(tp.server_ep.count < FAKE_EP_MAX_OPS);
        MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)1);
        if (tp.server_ep.count == 1) {
            const fake_op_t *o = &tp.server_ep.ops[0];
            MOQ_TEST_CHECK_EQ_INT((int)o->kind, (int)FAKE_OP_CLOSE);
            MOQ_TEST_CHECK_EQ_U64(o->error_code, (uint64_t)0x1);
            MOQ_TEST_CHECK(o->data_len == reason_len &&
                           memcmp(o->data, k_reason, reason_len) == 0);
        }
    }

    /* A second service cannot resurrect the target mapping or re-close. */
    fake_endpoint_clear_ops(&tp.server_ep);
    MOQ_TEST_CHECK_EQ_INT(
        (int)held_bridge_service(tp.server_bridge, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(tp.server_ep.count, (size_t)0);
    MOQ_TEST_CHECK(bridge_find_by_id(tp.server_bridge, 600) == NULL ||
                   !bridge_find_by_id(tp.server_bridge, 600)->active);
    (void)seed_ref;
    test_pair_destroy(&tp);
    return failures;
}


int test_no_owner_admission_bridge(void)
{
    int failures = 0;
    failures += nob_session_selfcheck();
    failures += nob_phys_selfcheck();
    failures += nob_rid_allowance_selfcheck();
    failures += exh_carrier_capture_selfcheck();
    nob_case_t c;
    memset(&c, 0, sizeof(c));
    /* target_rid declared per row: 0, 0, 0, 2. */
    c.origin = 1; c.pre_setup = false; c.name = "o1-drain";    c.target_rid = 0;
    failures += run_nob_case(&c);
    c.origin = 2; c.pre_setup = true;  c.name = "o2-presetup"; c.target_rid = 0;
    failures += run_nob_case(&c);
    c.origin = 3; c.pre_setup = false; c.name = "o3-action";   c.target_rid = 0;
    failures += run_nob_case(&c);
    c.origin = 4; c.pre_setup = false; c.name = "o4-send";     c.target_rid = 2;
    failures += run_nob_case(&c);
    return failures;
}
