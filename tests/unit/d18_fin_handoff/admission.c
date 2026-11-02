#include "test_cases.h"

/* -- No-slot admission: the no-owner admission blockers -------------
 *
 * handle_request_stream_bytes()'s no-slot path returns MOQ_ERR_WOULD_BLOCK on
 * capacity shortfalls BEFORE any staging entry or registry owner exists.
 * Verified against the current source, the path has exactly FOUR such
 * origins (session_subscribe.c:1258-1318):
 *
 *   1. !fin with the drain ring full, at the no-slot preflight (:1271);
 *   2. pre-SETUP no-slot rejection with action_queue_avail < 2 (:1280);
 *   3. post-SETUP no-slot REQUEST_ERROR with the action queue full
 *      (queue_send_bidi -> action_queue_full);
 *   4. post-SETUP no-slot REQUEST_ERROR with action room but an insufficient
 *      remaining send-buffer tail (queue_send_bidi's retryable tail check).
 *
 * No other WOULD_BLOCK origin exists on this path: push_action after the
 * `avail >= 2` preflight cannot block, the REQUEST_ERROR encode writes a
 * 64-byte stack buffer, drain_ref_add follows its own preflight, and
 * request_goaway_already_sent cannot hold for a fresh peer ref.
 *
 * The completion contract is STORAGE-NEUTRAL: each blocked case accepts
 * exactly two outcomes -- immediate MOQ_OK with the whole terminal obligation
 * durably queued, or WOULD_BLOCK with a durable carrier that completes on the
 * documented EMPTY re-feed -- through one shared completion checker, so both
 * arms require the same exact final output, cleanup and exactly-once
 * postconditions. Any other return fails the permanent outcome validator.
 * The WOULD_BLOCK arm completes on the documented empty re-feed through its
 * durable no-slot carrier, and its cumulative FIN is retained in the
 * carrier so the completion owes no drain the peer already made unnecessary.
 *
 * Terminal obligations, exact:
 *   pre-SETUP:  STOP_BIDI_STREAM + RESET_BIDI_STREAM, both CANCELLED (0x1),
 *               no event;
 *   post-SETUP: REQUEST_ERROR(EXCESSIVE_LOAD 0x9, Retry Interval 1,
 *               "request pool full") + FIN on the request bidi, no event --
 *               local capacity (draft-18 §10.6), retryable at once;
 *   no peer FIN: exactly one NORMAL drain ref, released by the later FIN;
 *   peer FIN:    no drain ref.
 *
 * The seed is a LEGALLY occupied one-slot subscription pool: a fragmented
 * inbound SUBSCRIBE on its own bidi, held in MOQ_SUB_RECVING_REQUEST. Its
 * slot and generation are DERIVED before ingress, its retained prefix is
 * compared byte-for-byte against the fixture's own input before it becomes a
 * baseline, and its whole declared record -- identity, role, registry
 * presence fields, forbidden by-ID/ns edges -- is conserved absolutely at
 * every stage. At the REFUSED point the target is NOT required to stay
 * ownerless (that would forbid the ownership-based repair); every allowed
 * delta is bounded instead, and complete target cleanup is required at
 * terminal completion.
 */

#define NOM_SEED_REF   1u     /* peer-opened bidi carrying the seed */

#define NOM_TARGET_REF 5u     /* peer-opened bidi carrying the target */

#define NOM_FILLER0    0x4000u /* fill_drain_ring's first declared filler */


/* Origin 4's live-announcement GOAWAY blocker identity, predeclared before it
 * becomes a baseline. `armed` is 0 for origins 1-3 (no blocker owner). */
typedef struct nom_blk {
    int      armed;
    int      slot;
    uint32_t gen;         /* live generation (| 1) after accept */
    uint64_t handle;
    uint64_t rid;         /* the committed inbound id (0) */
    uint64_t ref;         /* NOM_BLK_REF */
} nom_blk_t;


static const char *nom_reason = "request pool full";


/* A session with a ONE-slot subscription pool and per-origin capacities.
 * `establish` selects the pre-/post-SETUP arm. */
static moq_session_t *nom_make_session(bool establish, uint32_t max_actions,
                                       uint32_t send_buffer_size)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    cfg.version = MOQ_VERSION_DRAFT_18;
    cfg.max_subscriptions = 1;
    if (max_actions) cfg.max_actions = max_actions;
    if (send_buffer_size) cfg.send_buffer_size = send_buffer_size;
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
    if (!s) return NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(s, 0), (int)MOQ_OK);
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    if (establish) {
        uint8_t setup[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, setup, sizeof(setup));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup(&w), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_control_bytes(s, setup,
                                              moq_buf_writer_offset(&w), 0),
            (int)MOQ_OK);
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    }
    return s;
}


/* One complete, wire-valid d18 SUBSCRIBE at request id 0. */
static size_t nom_encode_subscribe_at(uint8_t *buf, size_t cap,
                                      uint64_t request_id)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("nom") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t p = { 0 };
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe(&w, request_id, &ns,
                                      MOQ_BYTES_LITERAL("t"), &p),
        (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}


static size_t nom_encode_subscribe(uint8_t *buf, size_t cap)
{
    return nom_encode_subscribe_at(buf, cap, 0);
}


/* The seed owner's DECLARED record. Identity (slot, generation) is derived
 * from the free pool BEFORE ingress; the retained prefix is compared against
 * the fixture's own input before this record becomes a baseline. An invalid
 * buffer (over-long, or non-empty with a NULL pointer) makes the record
 * INCOMPARABLE rather than equal-by-omission. */
typedef struct nom_seed {
    int      valid;
    int      slot;
    int      state, role;
    uint64_t request_id;
    uint32_t generation;
    uint64_t stream_ref;
    int      recv_fin;
    size_t   recv_len;
    uint8_t  recv[64];
    /* Registry presence, complete: the by-streamref endpoint's kind, slot,
     * presence flags and embedded linkage values, plus the by-ID-0 answer --
     * whose absence is an ARM/seed-only and completed-state rule; the
     * refused phase's global topology (which may hold an identity-qualified
     * target carrier at id 0) is owned by nom_graph_conserved. */
    int      ep_kind;
    int      ep_slot;
    int      ep_has_stream_ref;
    int      ep_has_request_id;
    uint64_t ep_stream_ref;       /* the endpoint's EMBEDDED linkage values */
    uint64_t ep_request_id;       /* declared zero while has_request_id==0 */
    int      byid0_kind;          /* find_by_id(0).kind: an ARM/seed-only
                                   * declaration. The REFUSED phase's global
                                   * topology is owned by nom_graph_conserved
                                   * (which admits an identity-qualified
                                   * target carrier at id 0); completion
                                   * restores absolute absence through
                                   * nom_graph_seed_only. */
} nom_seed_t;


static void nom_seed_capture(const moq_session_t *s, nom_seed_t *o)
{
    memset(o, 0, sizeof(*o));
    o->slot = -1;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state != MOQ_SUB_FREE) { o->slot = (int)i; break; }
    if (o->slot < 0) return;
    const moq_sub_entry_t *e = &s->subs[o->slot];
    o->state = (int)e->state;
    o->role = (int)e->role;
    o->request_id = e->request_id;
    o->generation = e->generation;
    o->stream_ref = e->rs.ref._v;
    o->recv_fin = e->rs.recv_fin ? 1 : 0;
    o->recv_len = e->rs.recv_len;
    if (o->recv_len > sizeof(o->recv) ||
        (o->recv_len && !e->rs.recv_buf))
        return;                              /* valid stays 0: incomparable */
    if (o->recv_len)
        memcpy(o->recv, e->rs.recv_buf, o->recv_len);
    moq_request_endpoint_t ep = request_registry_find_by_streamref(
        s, e->rs.ref);
    o->ep_kind = (int)ep.kind;
    o->ep_slot = ep.slot;
    o->ep_has_stream_ref = ep.has_stream_ref ? 1 : 0;
    o->ep_has_request_id = ep.has_request_id ? 1 : 0;
    o->ep_stream_ref = ep.stream_ref._v;
    o->ep_request_id = ep.request_id;
    o->byid0_kind = (int)request_registry_find_by_id(s, 0).kind;
    o->valid = 1;
}


static int nom_seed_equal(const moq_session_t *s, const nom_seed_t *want,
                          const char *what)
{
    nom_seed_t now;
    nom_seed_capture(s, &now);
    if (!now.valid || !want->valid) {
        fprintf(stderr, "NOM %s: seed record incomparable\n", what);
        return 1;
    }
    int bad = 0;
    if (now.slot != want->slot || now.state != want->state ||
        now.role != want->role || now.request_id != want->request_id ||
        now.generation != want->generation ||
        now.stream_ref != want->stream_ref ||
        now.recv_fin != want->recv_fin ||
        now.recv_len != want->recv_len ||
        (now.recv_len && memcmp(now.recv, want->recv, now.recv_len) != 0)) {
        fprintf(stderr, "NOM %s: seed owner record changed\n", what);
        bad++;
    }
    /* by-ID-0 absence is deliberately NOT compared here: it is absolute at
     * arm and at completion (the seed-only graph forbids the rid-0 edge
     * there), while the REFUSED phase admits an identity-qualified target
     * carrier at decoded id 0 -- the seed helper must not classify that
     * target-owned edge as seed mutation. */
    if (now.ep_kind != want->ep_kind || now.ep_slot != want->ep_slot ||
        now.ep_has_stream_ref != want->ep_has_stream_ref ||
        now.ep_has_request_id != want->ep_has_request_id ||
        now.ep_stream_ref != want->ep_stream_ref ||
        now.ep_request_id != want->ep_request_id) {
        fprintf(stderr, "NOM %s: seed registry answers changed\n", what);
        bad++;
    }
    return bad;
}


/* Every field the contract claims ABSOLUTELY, asserted before the captured
 * record may become a baseline -- shared by the matrix runner and the four
 * capacity controls so neither seed contract is weaker. */
static void nom_seed_arm_assert(const nom_seed_t *seed, int want_slot)
{
    MOQ_TEST_CHECK_EQ_INT(seed->valid, 1);
    MOQ_TEST_CHECK_EQ_INT(seed->slot, want_slot);
    MOQ_TEST_CHECK_EQ_INT(seed->state, (int)MOQ_SUB_RECVING_REQUEST);
    MOQ_TEST_CHECK_EQ_INT(seed->role, (int)MOQ_SUB_ROLE_PUBLISHER);
    MOQ_TEST_CHECK_EQ_U64(seed->request_id, (uint64_t)0);
    MOQ_TEST_CHECK_EQ_U64(seed->stream_ref, (uint64_t)NOM_SEED_REF);
    MOQ_TEST_CHECK_EQ_INT(seed->recv_fin, 0);
    MOQ_TEST_CHECK_EQ_INT(seed->ep_kind, (int)MOQ_REQ_SUBSCRIPTION);
    MOQ_TEST_CHECK_EQ_INT(seed->ep_slot, want_slot);
    MOQ_TEST_CHECK_EQ_INT(seed->ep_has_stream_ref, 1);
    MOQ_TEST_CHECK_EQ_INT(seed->ep_has_request_id, 0);
    MOQ_TEST_CHECK_EQ_U64(seed->ep_stream_ref, (uint64_t)NOM_SEED_REF);
    MOQ_TEST_CHECK_EQ_U64(seed->ep_request_id, (uint64_t)0);
    MOQ_TEST_CHECK_EQ_INT(seed->byid0_kind, (int)MOQ_REQ_NONE);
}


/* The seed-only graph: exactly ONE edge in the whole session -- the seed's
 * stream-ref registry edge -- and no edge of any kind for the target. */
static int nom_graph_seed_only(const moq_session_t *s, uint64_t seed_ref,
                               int seed_slot, uint64_t target_ref,
                               const nom_blk_t *blk, const char *what)
{
    og_graph_t g;
    og_capture(s, &g);
    int bad = og_check_integrity(&g, what);
    bad += og_check_edge(&g, OG_DOM_REQ_STREAMREF, seed_ref,
                         MOQ_REQ_SUBSCRIPTION, seed_slot, what);
    const og_edge_spec_t w[] = { { OG_DOM_REQ_STREAMREF, seed_ref } };
    bad += og_check_owner_edges(&g, MOQ_REQ_SUBSCRIPTION, seed_slot, w, 1,
                                what);
    bad += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, target_ref, what);
    bad += og_check_no_edge(&g, OG_DOM_NS_REF, seed_ref, what);
    bad += og_check_no_edge(&g, OG_DOM_NS_REF, target_ref, what);
    /* The seed is the only SUBSCRIPTION owner. Origin 4 additionally holds a
     * LIVE announcement blocker keyed by its own stream ref (a distinct
     * pool/domain-qualified edge); the by-RID index carries neither owner. */
    size_t want_edges = 1;
    if (blk && blk->armed) {
        bad += og_check_edge(&g, OG_DOM_REQ_STREAMREF, blk->ref,
                             MOQ_REQ_ANNOUNCEMENT, blk->slot, what);
        const og_edge_spec_t bw[] = { { OG_DOM_REQ_STREAMREF, blk->ref } };
        bad += og_check_owner_edges(&g, MOQ_REQ_ANNOUNCEMENT, blk->slot,
                                    bw, 1, what);
        bad += og_check_no_edge(&g, OG_DOM_REQ_RID, blk->rid, what);
        want_edges = 2;
    } else {
        bad += og_check_no_edge(&g, OG_DOM_REQ_RID, 0, what);
    }
    if (g.edge_count != want_edges) {
        fprintf(stderr, "NOM %s: %zu graph edges, expected exactly %zu\n",
                what, g.edge_count, want_edges);
        bad++;
    }
    return bad;
}


/* Conservation across the refused call: every prior edge survives exactly
 * once, and any ADDED edge is admitted only under a domain-aware,
 * IDENTITY-QUALIFIED allowance -- the bounded storage-neutral window for an
 * ownership repair:
 *
 *   - a new stream-ref/ns-ref edge must key the saved target ref AND its own
 *     (kind, slot) must resolve to a live owner whose declared stream
 *     identity IS that target;
 *   - a new request-ID edge may key the DECLARED target request id (0 for
 *     origins 1-3, 2 for origin 4) but must resolve to that same target owner,
 *     never the seed or an unrelated slot;
 *   - duplicates, unsupported kinds, wrong slots, unrelated keys, and an
 *     edge merely repointed at the seed all fail by identity.
 *
 * The resolver is local and bounds-safe: it models the two owner families a
 * request-bidi carrier could live in without broadening the shared ownership
 * policy. */
static int nom_edge_owns_target(const moq_session_t *s, const og_edge_t *f,
                                uint64_t target_ref, const char *what)
{
    if (f->kind == MOQ_REQ_SUBSCRIPTION) {
        if (f->slot < 0 || (size_t)f->slot >= s->sub_cap) {
            if (!og_quiet) fprintf(stderr, "NOM %s: added edge slot out of range\n", what);
            return 0;
        }
        const moq_sub_entry_t *e = &s->subs[f->slot];
        if (e->state == MOQ_SUB_FREE ||
            e->rs.ref._v != target_ref) {
            if (!og_quiet) fprintf(stderr, "NOM %s: added edge's owner does not carry the"
                    " target identity\n", what);
            return 0;
        }
        return 1;
    }
    /* The no-slot path is the GENERIC request-staging path, whose owner
     * family in source is MOQ_REQ_SUBSCRIPTION; no current route stages such
     * a request in another pool, so no other kind is blessed here. A future
     * explicit private carrier kind arrives with the product and its test
     * model. */
    if (!og_quiet) fprintf(stderr, "NOM %s: added edge has unsupported kind %d\n", what,
            f->kind);
    return 0;
}


static int nom_graph_conserved(const moq_session_t *s, const og_graph_t *g0,
                               uint64_t target_ref, uint64_t target_rid,
                               const char *what)
{
    og_graph_t g1;
    og_capture(s, &g1);
    int bad = og_check_integrity(&g1, what);
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
            if (!og_quiet) fprintf(stderr, "NOM %s: pre-existing edge (%d key %llu) not"
                    " conserved (%zu matches)\n", what, (int)e->domain,
                    (unsigned long long)e->key, seen);
            bad++;
        }
    }
    for (size_t j = 0; j < g1.edge_count; j++) {
        const og_edge_t *f = &g1.edges[j];
        int preexisting = 0;
        for (size_t i = 0; i < g0->edge_count; i++) {
            const og_edge_t *e = &g0->edges[i];
            if (f->domain == e->domain && f->key == e->key &&
                f->kind == e->kind && f->slot == e->slot) {
                preexisting = 1;
                break;
            }
        }
        if (preexisting) continue;
        /* Duplicate added edges on one (domain, key) fail. */
        size_t dup = 0;
        for (size_t k = 0; k < g1.edge_count; k++)
            if (g1.edges[k].domain == f->domain && g1.edges[k].key == f->key)
                dup++;
        if (dup != 1) {
            if (!og_quiet) fprintf(stderr, "NOM %s: added edge (%d key %llu) duplicated\n",
                    what, (int)f->domain, (unsigned long long)f->key);
            bad++;
            continue;
        }
        if (f->domain == OG_DOM_REQ_STREAMREF || f->domain == OG_DOM_NS_REF) {
            if (f->key != target_ref) {
                if (!og_quiet) fprintf(stderr, "NOM %s: added edge (%d key %llu) does not"
                        " key the target\n", what, (int)f->domain,
                        (unsigned long long)f->key);
                bad++;
                continue;
            }
            if (!nom_edge_owns_target(s, f, target_ref, what)) bad++;
        } else if (f->domain == OG_DOM_REQ_RID) {
            if (f->key != target_rid) {
                if (!og_quiet) fprintf(stderr, "NOM %s: added request-ID edge keys %llu,"
                        " not the target's declared id %llu\n", what,
                        (unsigned long long)f->key,
                        (unsigned long long)target_rid);
                bad++;
                continue;
            }
            if (!nom_edge_owns_target(s, f, target_ref, what)) bad++;
        } else {
            if (!og_quiet) fprintf(stderr, "NOM %s: added edge in unsupported domain %d\n",
                    what, (int)f->domain);
            bad++;
        }
    }
    return bad;
}


/* Normalize every action this matrix can legally see; anything else fails. */
static bool nom_norm_action(const moq_action_t *a, void *vctx,
                            txs_norm_vec_t *out)
{
    (void)vctx;
    txs_img_t im;
    txs_img_init(&im);
    switch (a->kind) {
    case MOQ_ACTION_CLOSE_BIDI_STREAM:
        txs_img_u64(&im, a->u.close_bidi_stream.stream_ref._v);
        return txs_norm_append_img(out, a->kind, &im);
    case MOQ_ACTION_STOP_BIDI_STREAM:
        txs_img_u64(&im, a->u.stop_bidi_stream.stream_ref._v);
        txs_img_u64(&im, a->u.stop_bidi_stream.error_code);
        return txs_norm_append_img(out, a->kind, &im);
    case MOQ_ACTION_RESET_BIDI_STREAM:
        txs_img_u64(&im, a->u.reset_bidi_stream.stream_ref._v);
        txs_img_u64(&im, a->u.reset_bidi_stream.error_code);
        return txs_norm_append_img(out, a->kind, &im);
    case MOQ_ACTION_SEND_BIDI_STREAM: {
        const moq_send_bidi_stream_action_t *sb = &a->u.send_bidi_stream;
        if (sb->len && !sb->data) {
            fprintf(stderr, "NOM: SEND_BIDI with NULL data\n");
            return false;
        }
        txs_img_u64(&im, sb->stream_ref._v);
        txs_img_u64(&im, sb->fin ? 1 : 0);
        /* Decode, never count: envelope, then the complete REQUEST_ERROR. */
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, sb->data, sb->len);
        moq_control_envelope_t env;
        memset(&env, 0, sizeof(env));
        if (moq_d18_decode_envelope(&r, &env) != MOQ_OK) {
            fprintf(stderr, "NOM: undecodable SEND_BIDI envelope\n");
            return false;
        }
        if (moq_buf_reader_remaining(&r) != 0) {
            fprintf(stderr, "NOM: trailing bytes after envelope\n");
            return false;
        }
        txs_img_u64(&im, env.msg_type);
        moq_d18_request_error_t er;
        memset(&er, 0, sizeof(er));
        if (env.msg_type == (uint64_t)MOQ_D18_REQUEST_ERROR) {
            if (moq_d18_decode_request_error(env.payload, env.payload_len,
                                             &er) != MOQ_OK) {
                fprintf(stderr, "NOM: undecodable REQUEST_ERROR body\n");
                return false;
            }
            if (er.reason.len && !er.reason.data) {
                fprintf(stderr, "NOM: REQUEST_ERROR reason NULL bytes\n");
                return false;
            }
            txs_img_u64(&im, er.error_code);
            txs_img_u64(&im, er.retry_interval);
            txs_img_bytes(&im, er.reason.data, er.reason.len);
        }
        return txs_norm_append_img(out, a->kind, &im);
    }
    default:
        fprintf(stderr, "NOM: unnormalized action kind %u\n",
                (unsigned)a->kind);
        return false;
    }
}


static txs_op_hooks_t nom_hooks = {
    NULL, NULL, NULL, NULL, no_event_expected, nom_norm_action
};


/* The DECLARED terminal output for one target completion. */
static void nom_want_terminal(txs_norm_vec_t *v, bool pre_setup,
                              uint64_t target_ref)
{
    txs_img_t im;
    if (pre_setup) {
        txs_img_init(&im);
        txs_img_u64(&im, target_ref);
        txs_img_u64(&im, 0x1);                        /* CANCELLED */
        MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_STOP_BIDI_STREAM,
                                           &im));
        txs_img_init(&im);
        txs_img_u64(&im, target_ref);
        txs_img_u64(&im, 0x1);
        MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_RESET_BIDI_STREAM,
                                           &im));
    } else {
        txs_img_init(&im);
        txs_img_u64(&im, target_ref);
        txs_img_u64(&im, 1);                          /* FIN */
        txs_img_u64(&im, (uint64_t)MOQ_D18_REQUEST_ERROR);
        txs_img_u64(&im, MOQ_REQUEST_ERROR_EXCESSIVE_LOAD);
        txs_img_u64(&im, 1);                          /* retry interval */
        txs_img_bytes(&im, (const uint8_t *)nom_reason, strlen(nom_reason));
        MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_SEND_BIDI_STREAM,
                                           &im));
    }
}


/* Permanent outcome validator: the ONLY recognized results are MOQ_OK and
 * MOQ_ERR_WOULD_BLOCK; everything else is an oracle failure. Quiet mode lets
 * the negative self-check below run without noise. */
static int nom_outcome_quiet;

static int nom_outcome_classify(moq_result_t rc, const char *what)
{
    if (rc == MOQ_OK) return 0;
    if (rc == MOQ_ERR_WOULD_BLOCK) return 1;
    if (!nom_outcome_quiet)
        fprintf(stderr, "NOM %s: unrecognized outcome %d\n", what, (int)rc);
    return -1;
}


static void nom_outcome_selfcheck(void)
{
    nom_outcome_quiet = 1;
    MOQ_TEST_CHECK_EQ_INT(nom_outcome_classify(MOQ_OK, "self"), 0);
    MOQ_TEST_CHECK_EQ_INT(nom_outcome_classify(MOQ_ERR_WOULD_BLOCK, "self"),
                          1);
    MOQ_TEST_CHECK_EQ_INT(nom_outcome_classify(MOQ_ERR_INTERNAL, "self"), -1);
    MOQ_TEST_CHECK_EQ_INT(nom_outcome_classify(MOQ_ERR_CLOSED, "self"), -1);
    nom_outcome_quiet = 0;
}


/* The complete retirement postcondition, shared by both outcome arms, the
 * second retry, and the post-late-FIN recheck: target carrier fully gone,
 * seed-only topology, exact pool occupancy, exact session state, exact drain
 * multiset. */
/* The blocker announcement's exact edge set: one stream-ref request-registry
 * edge keyed by NOM_BLK_REF resolving to its announcement slot. */
static int nom_blk_graph_edges(const moq_session_t *s, const nom_blk_t *b,
                               const char *what)
{
    if (!b || !b->armed) return 0;
    og_graph_t g;
    og_capture(s, &g);
    int bad = og_check_edge(&g, OG_DOM_REQ_STREAMREF, b->ref,
                            MOQ_REQ_ANNOUNCEMENT, b->slot, what);
    const og_edge_spec_t w[] = { { OG_DOM_REQ_STREAMREF, b->ref } };
    bad += og_check_owner_edges(&g, MOQ_REQ_ANNOUNCEMENT, b->slot, w, 1, what);
    return bad;
}


/* Conservation of the blocker owner across a phase: the announcement stays
 * ESTABLISHED/RECEIVER with its declared identity and goaway_sent, and its
 * stream-ref edge is intact. */
static int nom_blk_conserved(const moq_session_t *s, const nom_blk_t *b,
                             const char *what)
{
    if (!b || !b->armed) return 0;
    int bad = 0;
    if (b->slot < 0 || (size_t)b->slot >= s->ann_cap) {
        fprintf(stderr, "NOM %s: blocker slot out of range\n", what);
        return 1;
    }
    const moq_ann_entry_t *e = &s->announcements[b->slot];
    if ((int)e->state != (int)MOQ_ANN_ESTABLISHED ||
        (int)e->role != (int)MOQ_ANN_ROLE_RECEIVER ||
        e->generation != b->gen || e->handle._opaque != b->handle ||
        e->request_id != b->rid || e->rs.ref._v != b->ref ||
        !e->goaway_sent) {
        fprintf(stderr, "NOM %s: blocker owner record changed\n", what);
        bad++;
    }
    bad += nom_blk_graph_edges(s, b, what);
    return bad;
}


static int nom_check_complete(const moq_session_t *s, const nom_seed_t *seed,
                              uint64_t seed_ref, uint64_t target_ref,
                              int pre_state, const drain_snap_t *want_ring,
                              const txs_snapshot_t *base, const nom_blk_t *blk,
                              const char *what)
{
    int bad = 0;
    size_t want_recv_payload = base->recv_payload_bytes;
    /* The remaining generic scalars, DERIVED from the pre-target baseline
     * with the declared completion transitions: all output consumed (both
     * queues empty), the scratch arena back at its pre-target cursor, and
     * the drain count equal to the declared ring's. */
    if ((size_t)(s->event_tail - s->event_head) != 0 ||
        (size_t)(s->action_tail - s->action_head) != 0) {
        fprintf(stderr, "NOM %s: queues not drained at completion\n", what);
        bad++;
    }
    if (s->event_scratch_len != base->event_scratch_len) {
        fprintf(stderr, "NOM %s: event_scratch_len %zu, expected the"
                " pre-target %zu\n", what, s->event_scratch_len,
                base->event_scratch_len);
        bad++;
    }
    if (s->drain_ref_count != want_ring->count) {
        fprintf(stderr, "NOM %s: drain count %zu, expected the declared"
                " %zu\n", what, s->drain_ref_count, want_ring->count);
        bad++;
    }
    /* The target carrier's ACCOUNTING must be released, not only its edges:
     * a repair that retains target bytes forever would otherwise pass. */
    if (s->recv_payload_bytes != want_recv_payload) {
        fprintf(stderr, "NOM %s: recv_payload_bytes %zu, expected the"
                " pre-target %zu\n", what, s->recv_payload_bytes,
                want_recv_payload);
        bad++;
    }
    moq_stream_ref_t tr; tr._v = target_ref;
    if (request_registry_find_by_streamref(s, tr).kind != MOQ_REQ_NONE) {
        fprintf(stderr, "NOM %s: target still has a registry owner\n", what);
        bad++;
    }
    bad += nom_graph_seed_only(s, seed_ref, seed->slot, target_ref, blk, what);
    /* The live blocker owner survives the target's whole lifecycle. */
    bad += nom_blk_conserved(s, blk, what);
    bad += nom_seed_equal(s, seed, what);
    {
        int busy = 0;
        for (size_t i = 0; i < s->sub_cap; i++)
            if (s->subs[i].state != MOQ_SUB_FREE) busy++;
        if (busy != 1) {
            fprintf(stderr, "NOM %s: pool occupancy %d, expected 1\n", what,
                    busy);
            bad++;
        }
    }
    if ((int)s->state != pre_state) {
        fprintf(stderr, "NOM %s: session state %d, expected %d\n", what,
                (int)s->state, pre_state);
        bad++;
    }
    drain_snap_t now;
    drain_snap(s, &now);
    bad += drain_multiset_equals(&now, want_ring, what);
    return bad;
}


typedef struct nom_case {
    const char *name;
    int         origin;       /* 1..4 */
    bool        fin;          /* the target call carries a FIN */
    bool        pre_setup;
    /* The inbound target Request ID, DECLARED per row independently of the
     * encoded target bytes (draft-18 §10.1). Origin 4's live announcement
     * blocker commits inbound id 0, so its next request is id 2; origins 1-3
     * commit no preceding request and use id 0. The no-slot terminal refuses
     * the target BEFORE its id is validated, so this is a wire-legality
     * declaration the encoder honours, not a runtime-killable value. */
    uint64_t    target_rid;
} nom_case_t;


/* Descriptor self-check: origin 4 requires target id 2, every other origin
 * requires 0. A missing/defaulted or coherently changed member cannot silently
 * redefine the fixture. */
static int nom_case_target_rid_ok(const nom_case_t *c)
{
    uint64_t want = (c->origin == 4) ? 2u : 0u;
    if (c->target_rid != want) {
        fprintf(stderr, "FAIL: nom %s: declared target_rid %llu, origin %d"
                " requires %llu\n", c->name, (unsigned long long)c->target_rid,
                c->origin, (unsigned long long)want);
        return 1;
    }
    return 0;
}


/* Green-phase perturbation switches (test-local, default off): each forces a
 * deliberate divergence a load-bearing oracle must catch. Used only by the
 * temporary kill runs recorded in the task report; they are OFF in the
 * committed tree and asserted so by the matrix driver. */
static int nom_perturb;   /* 0=off, else the numbered perturbation */

/* -- Origin 4's LIVE draft-18 request blocker ------------------------
 *
 * Origin 4 fills the send buffer with a legal per-request GOAWAY on an
 * accepted inbound announcement. The GOAWAY is one non-FIN SEND_BIDI_STREAM
 * whose bytes occupy the send tail so the refused target's REQUEST_ERROR cannot
 * be encoded. The announcement owner is predeclared and required LIVE through
 * the whole arc. */
#define NOM_BLK_REF 3u

/* Sized so the encoded GOAWAY fills the 64-byte send buffer, leaving a tail too
 * small for the refused target's REQUEST_ERROR, so the target is retained. */
static const uint8_t nom_goaway_uri[44] = {
    'm','o','q',':','/','/','n','o','m','-','a','l','t','.','e','x','a',
    'm','p','l','e','/','g','o','a','w','a','y','/','n','o','m','/','0',
    '0','0','0','0','0','0','0','0','0','1'
};


/* Arm the blocker: feed an inbound PUBLISH_NAMESPACE (id 0), accept it, drain
 * the accept's REQUEST_OK so the GOAWAY starts from a clean send tail, then
 * issue the per-request GOAWAY. Must run BEFORE the seed occupies the sole
 * subscription staging slot -- the d18 request dispatch grabs one transiently. */
static void nom_arm_goaway_blocker(moq_session_t *s, nom_blk_t *out)
{
    memset(out, 0, sizeof(*out));
    int slot = -1;
    uint32_t gen = 0;
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state == MOQ_ANN_FREE) {
            slot = (int)i;
            gen = s->announcements[i].generation | 1u;   /* live generation */
            break;
        }
    MOQ_TEST_CHECK(slot >= 0);

    /* The packed announcement handle is MINTED from the pre-ingress
     * slot/generation/session-tag, never adopted from the entry afterwards, so
     * the surfaced event handle and the live owner handle are each REQUIRED to
     * equal this derivation. */
    uint16_t tag = s->session_tag;
    uint64_t want_handle =
        moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT, tag, gen, (uint32_t)slot);

    moq_stream_ref_t ref = moq_stream_ref_from_u64(NOM_BLK_REF);
    uint8_t m[192];
    size_t n = enc_pns_full(m, sizeof(m), 0, "blk", NULL);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, m, n, false, 1),
        (int)MOQ_OK);
    uint64_t h = 0;
    arm_output_exact(s, MOQ_EVENT_NAMESPACE_PUBLISHED, 1, 0, "nom blk", &h);
    MOQ_TEST_CHECK_EQ_U64(h, want_handle);

    moq_announcement_t ann;
    ann._opaque = want_handle;
    moq_accept_namespace_cfg_t ac;
    moq_accept_namespace_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_accept_namespace(s, ann, &ac, 1), (int)MOQ_OK);
    /* Drain the accept's REQUEST_OK so the empty action queue lets the next
     * advancing call reset the send tail before the GOAWAY encodes. */
    {
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    }

    moq_request_goaway_cfg_t gc;
    moq_request_goaway_cfg_init(&gc);
    gc.new_session_uri.data = nom_goaway_uri;
    gc.new_session_uri.len = sizeof(nom_goaway_uri);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_request_goaway_namespace(s, ann, &gc, 1), (int)MOQ_OK);

    const moq_ann_entry_t *e = ann_by_ref(s, ref);
    MOQ_TEST_CHECK(e != NULL);
    if (e) {
        out->armed  = 1;
        out->slot   = slot;               /* the pre-ingress derivation */
        out->gen    = gen;
        out->handle = want_handle;        /* MINTED, not read back */
        out->rid    = e->request_id;
        out->ref    = ref._v;
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_ANN_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_ANN_ROLE_RECEIVER);
        MOQ_TEST_CHECK(e->goaway_sent);
        MOQ_TEST_CHECK_EQ_U64(out->rid, (uint64_t)0);
        /* The live owner's own identity must match the minted derivation. */
        MOQ_TEST_CHECK_EQ_INT((int)(e - s->announcements), slot);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)e->generation, (uint64_t)gen);
        MOQ_TEST_CHECK_EQ_U64(e->handle._opaque, want_handle);
    }
}



/* The no-slot carrier state for a ref: -1 absent, 0 present (no FIN), 1 present
 * (FIN). Proves the PRODUCT installs/retires the refused target's carrier with
 * the row's exact FIN value, through real refusal/recovery. */
static int nom_carrier_state(const moq_session_t *s, uint64_t ref_v)
{
    moq_stream_ref_t ref;
    ref._v = ref_v;
    int i = noslot_carrier_find(s, ref);
    if (i < 0) return -1;
    return s->noslot_carriers[i].fin ? 1 : 0;
}


#define NOM_UNREL_CARRIER 0x8888u   /* an unrelated seeded carrier, conserved */


/* At every retired phase: the target carrier is gone, the count is back at the
 * declared baseline, and the unrelated seeded carrier is ref/FIN exact. */
static int nom_carrier_retired(const moq_session_t *s, size_t base,
                               const char *what)
{
    int bad = 0;
    if (nom_carrier_state(s, NOM_TARGET_REF) != -1) {
        fprintf(stderr, "NOM %s: target carrier still present\n", what);
        bad++;
    }
    if (nom_carrier_state(s, NOM_UNREL_CARRIER) != 0) {
        fprintf(stderr, "NOM %s: unrelated carrier changed\n", what);
        bad++;
    }
    if (s->noslot_carrier_count != base) {
        fprintf(stderr, "NOM %s: carrier count %zu, expected the baseline %zu\n",
                what, s->noslot_carrier_count, base);
        bad++;
    }
    return bad;
}


/* The shared matrix runner. Blockers by origin:
 *   1: full drain ring (no action blocker);
 *   2: pre-setup, max_actions=2, one queued close leaves avail == 1 < 2;
 *   3: max_actions=1, one queued close fills the ring;
 *   4: send_buffer_size=64, a per-request GOAWAY fills the send tail.
 * Recovery drains ONLY the blocker (poll_actions), then the documented empty
 * re-feed. No target bytes are ever re-delivered. */
static void run_nom_case(const nom_case_t *c)
{
    char what[96];
    snprintf(what, sizeof(what), "nom %s", c->name);
    uint32_t max_actions = 0, send_size = 0;
    if (c->origin == 2) max_actions = 2;
    if (c->origin == 3) max_actions = 1;
    if (c->origin == 4) send_size = 64;
    moq_session_t *s = nom_make_session(!c->pre_setup, max_actions, send_size);
    if (!s) return;

    failures += nom_case_target_rid_ok(c);

    /* Origin 4's blocker is a LIVE announcement + per-request GOAWAY. It must be
     * armed BEFORE the seed occupies the sole subscription staging slot, since
     * the d18 request dispatch grabs one transiently on the announcement
     * handoff. It commits inbound id 0, so the target is declared at id 2. */
    nom_blk_t blk;
    memset(&blk, 0, sizeof(blk));
    if (c->origin == 4)
        nom_arm_goaway_blocker(s, &blk);

    /* Seed identity, DERIVED from the free pool AFTER the blocker's transient
     * staging use (which advances the slot's generation). */
    int seed_slot = -1;
    uint32_t seed_gen = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state == MOQ_SUB_FREE) {
            seed_slot = (int)i;
            seed_gen = s->subs[i].generation | 1u;
            break;
        }
    MOQ_TEST_CHECK_EQ_INT(seed_slot, 0);

    /* Seed: a fragmented SUBSCRIBE occupies the ONE pool slot legally. */
    uint8_t sub[128];
    size_t sub_len = nom_encode_subscribe(sub, sizeof(sub));
    MOQ_TEST_CHECK(sub_len > 8);
    moq_stream_ref_t seed_ref = moq_stream_ref_from_u64(NOM_SEED_REF);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, seed_ref, sub, sub_len - 4,
                                              false, 1),
        (int)MOQ_OK);
    /* The retained prefix must equal the bytes actually fed BEFORE the
     * captured record becomes a baseline. */
    {
        const moq_sub_entry_t *e = &s->subs[seed_slot];
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_SUB_RECVING_REQUEST);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)e->generation, (uint64_t)seed_gen);
        MOQ_TEST_CHECK_EQ_SIZE(e->rs.recv_len, sub_len - 4);
        MOQ_TEST_CHECK(e->rs.recv_buf != NULL);
        if (e->rs.recv_buf && e->rs.recv_len == sub_len - 4)
            MOQ_TEST_CHECK(memcmp(e->rs.recv_buf, sub, sub_len - 4) == 0);
    }
    nom_seed_t seed;
    nom_seed_capture(s, &seed);
    nom_seed_arm_assert(&seed, seed_slot);
    failures += nom_graph_seed_only(s, NOM_SEED_REF, seed_slot,
                                    NOM_TARGET_REF, &blk, "seeded");

    /* Blockers. Origin 4's blocker is the live announcement GOAWAY armed before
     * the seed; its ref is NOM_BLK_REF, the others' is an unmapped close ref. */
    size_t blockers = 0;
    moq_stream_ref_t blocker_ref =
        moq_stream_ref_from_u64(c->origin == 4 ? NOM_BLK_REF : 0x6000);
    if (c->origin == 1) {
        fill_drain_ring(s);
    } else if (c->origin == 2 || c->origin == 3) {
        MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker_ref),
                              (int)MOQ_OK);
        blockers = 1;
        if (c->origin == 3) MOQ_TEST_CHECK(action_queue_full(s));
        else MOQ_TEST_CHECK_EQ_SIZE(action_queue_avail(s), (size_t)1);
    } else {
        /* The GOAWAY's SEND_BIDI_STREAM already occupies the action slot and
         * fills the send tail; nothing new is queued. Its bytes leave too small
         * a tail for the refused target's REQUEST_ERROR. */
        blockers = 1;
        MOQ_TEST_CHECK(s->send_cap - s->send_len <= 16);
        MOQ_TEST_CHECK(action_queue_avail(s) >= 2);
    }
    drain_snap_t ring0;
    drain_snap(s, &ring0);

    /* An unrelated no-slot carrier, seeded directly, that every phase must leave
     * ref/FIN exact. The baseline count is captured with it present. */
    MOQ_TEST_CHECK(noslot_carrier_install(
        s, moq_stream_ref_from_u64(NOM_UNREL_CARRIER), false));
    size_t carrier_base = s->noslot_carrier_count;
    MOQ_TEST_CHECK_EQ_INT(nom_carrier_state(s, NOM_UNREL_CARRIER), 0);
    MOQ_TEST_CHECK_EQ_INT(nom_carrier_state(s, NOM_TARGET_REF), -1);

    /* Whole-session and graph baselines, captured BEFORE the target call. */
    txs_snapshot_t before;
    txs_capture(s, &seed_ref, 1, &before);
    expect_after_call_prepare(&before);
    og_graph_t g0;
    og_capture(s, &g0);

    /* The target request, complete and FIN'd per the row, at the DECLARED id
     * (2 for origin 4's post-announcement sequence, 0 otherwise). */
    uint8_t tgt[128];
    size_t tgt_len = nom_encode_subscribe_at(tgt, sizeof(tgt), c->target_rid);
    moq_stream_ref_t target = moq_stream_ref_from_u64(NOM_TARGET_REF);
    int pre_state = (int)s->state;
    MOQ_TEST_CHECK(pre_state != (int)MOQ_SESS_CLOSED);
    if (!c->pre_setup)
        MOQ_TEST_CHECK_EQ_INT(pre_state, (int)MOQ_SESS_ESTABLISHED);
    moq_result_t rc = moq_session_on_bidi_stream_bytes(s, target, tgt, tgt_len,
                                                       c->fin, 1);
    if (nom_perturb == 6)
        s->event_scratch_len += 8;   /* an UNRELATED whole-session scalar */
    MOQ_TEST_CHECK_EQ_INT((int)s->state, pre_state);

    int arm = nom_outcome_classify(rc, what);
    MOQ_TEST_CHECK(arm >= 0);
    if (arm < 0) { moq_session_destroy(s); return; }

    /* The declared completion drain multiset. Origin 1's want is DERIVED by
     * removing exactly the DECLARED filler from the pre-call ring, never from
     * an observed result. */
    drain_snap_t comp_want, late_want;
    if (c->origin == 1) {
        drain_snap_t less = ring0;
        size_t k = 0;
        int removed = 0;
        for (size_t i = 0; i < ring0.count; i++) {
            if (!removed && ring0.ref[i] == NOM_FILLER0) { removed = 1; continue; }
            less.ref[k] = ring0.ref[i];
            less.reason[k] = ring0.reason[i];
            k++;
        }
        less.count = k;
        MOQ_TEST_CHECK_EQ_INT(removed, 1);
        late_want = less;
        drain_snap_plus(&less, target, MOQ_DRAIN_NORMAL, &comp_want);
    } else if (c->fin) {
        comp_want = ring0;
        late_want = ring0;
    } else {
        drain_snap_plus(&ring0, target, MOQ_DRAIN_NORMAL, &comp_want);
        late_want = ring0;
    }

    if (arm == 0) {
        /* Arm A: immediate completion -- the WHOLE obligation queued NOW.
         * Consume exactly the leading blocker actions so both arms meet the
         * same convergent output check. Structurally shared; no current or
         * candidate product reaches it yet. */
        txs_norm_vec_t lead;
        txs_norm_init(&lead);
        size_t taken = 0;
        moq_action_t a;
        while (taken < blockers && moq_session_poll_actions(s, &a, 1) > 0) {
            if (!nom_norm_action(&a, NULL, &lead)) failures++;
            moq_action_cleanup(&a);
            taken++;
        }
        MOQ_TEST_CHECK_EQ_SIZE(taken, blockers);
        txs_norm_free(&lead);
    } else {
        /* Arm B: conservation at the refused point. The target-specific
         * carrier is deliberately UNBOUNDED in identity (an ownership repair
         * may create one) but every allowed delta is bounded:
         *   - whole-session scalars equal, except recv_payload_bytes which
         *     may grow by at most the fed target length;
         *   - graph topology conserved; any added edge keys the target and
         *     resolves to a live carrier;
         *   - the seed record, drain multiset, blocker action (kind, ref,
         *     and for the GOAWAY blocker its exact bytes) unchanged;
         *   - no event, no new action. */
        failures += nom_seed_equal(s, &seed, what);
        {
            txs_snapshot_t expect = before;
            const size_t grew = s->recv_payload_bytes;
            MOQ_TEST_CHECK(grew >= before.recv_payload_bytes);
            MOQ_TEST_CHECK(grew - before.recv_payload_bytes <= tgt_len);
            expect.recv_payload_bytes = grew;   /* bounded target carrier */
            failures += txs_check_eq(s, &seed_ref, 1, &expect, what);
        }
        failures += nom_graph_conserved(s, &g0, NOM_TARGET_REF,
                                        c->target_rid, what);
        /* Product-path carrier: the refused target is retained in EXACTLY one
         * carrier with its declared ref and the ROW's FIN value; the count is
         * baseline + 1; the unrelated carrier stays ref/FIN exact. */
        MOQ_TEST_CHECK_EQ_INT(nom_carrier_state(s, NOM_TARGET_REF),
                              c->fin ? 1 : 0);
        MOQ_TEST_CHECK_EQ_INT(nom_carrier_state(s, NOM_UNREL_CARRIER), 0);
        MOQ_TEST_CHECK_EQ_SIZE(s->noslot_carrier_count, carrier_base + 1);
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring0, what);
        {
            int evs = 0;
            moq_event_t ev;
            while (moq_session_poll_events(s, &ev, 1) > 0) {
                evs++; moq_event_cleanup(&ev);
            }
            MOQ_TEST_CHECK_EQ_INT(evs, 0);
        }
        MOQ_TEST_CHECK_EQ_SIZE(
            (size_t)(s->action_tail - s->action_head), blockers);
        /* The blocker is still the SAME action, byte for byte. */
        if (blockers == 1) {
            const moq_action_t *head =
                &s->actions[s->action_head % s->action_cap];
            if (c->origin == 4) {
                /* The blocker is the queued per-request GOAWAY, compared in
                 * full: a non-FIN SEND_BIDI_STREAM on the announcement's ref
                 * whose bytes are one complete REQUEST_GOAWAY envelope with no
                 * trailing bytes and the exact New Session URI. */
                MOQ_TEST_CHECK_EQ_INT((int)head->kind,
                                      (int)MOQ_ACTION_SEND_BIDI_STREAM);
                MOQ_TEST_CHECK_EQ_U64(head->u.send_bidi_stream.stream_ref._v,
                                      blocker_ref._v);
                MOQ_TEST_CHECK_EQ_INT(head->u.send_bidi_stream.fin ? 1 : 0, 0);
                MOQ_TEST_CHECK(head->u.send_bidi_stream.data != NULL);
                if (head->u.send_bidi_stream.data) {
                    moq_buf_reader_t gr;
                    moq_buf_reader_init(&gr, head->u.send_bidi_stream.data,
                                        head->u.send_bidi_stream.len);
                    moq_control_envelope_t genv;
                    memset(&genv, 0, sizeof(genv));
                    MOQ_TEST_CHECK_EQ_INT(
                        (int)moq_d18_decode_envelope(&gr, &genv), (int)MOQ_OK);
                    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&gr),
                                           (size_t)0);
                    MOQ_TEST_CHECK_EQ_U64(genv.msg_type,
                                          (uint64_t)MOQ_D18_GOAWAY);
                    moq_d18_goaway_t gaw;
                    memset(&gaw, 0, sizeof(gaw));
                    MOQ_TEST_CHECK_EQ_INT(
                        (int)moq_d18_decode_goaway_request(genv.payload,
                            genv.payload_len, &gaw), (int)MOQ_OK);
                    MOQ_TEST_CHECK(gaw.uri.len == sizeof(nom_goaway_uri) &&
                                   gaw.uri.data &&
                                   memcmp(gaw.uri.data, nom_goaway_uri,
                                          sizeof(nom_goaway_uri)) == 0);
                }
            } else {
                MOQ_TEST_CHECK_EQ_INT((int)head->kind,
                                      (int)MOQ_ACTION_CLOSE_BIDI_STREAM);
                MOQ_TEST_CHECK_EQ_U64(head->u.close_bidi_stream.stream_ref._v,
                                      blocker_ref._v);
            }
        }

        /* Recovery: drain ONLY the blocker(s), then the documented empty
         * re-feed with the row's own FIN fact. */
        {
            txs_norm_vec_t got;
            txs_norm_init(&got);
            collect_actions(s, &nom_hooks, &got);
            MOQ_TEST_CHECK_EQ_SIZE(got.count, blockers);
            txs_norm_free(&got);
        }
        if (c->origin == 1) {
            /* Free ONE drain slot: the DECLARED filler's own legitimate FIN
             * releases it through the public drain-ring absorb path. */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(
                    s, moq_stream_ref_from_u64(
                        nom_perturb == 4 ? NOM_FILLER0 + 1 : NOM_FILLER0),
                    NULL, 0, true, 1),
                (int)MOQ_OK);
        }
        rc = moq_session_on_bidi_stream_bytes(s, target, NULL, 0, c->fin, 1);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    }

    /* Both arms converge: the EXACT terminal obligation, once, and the whole
     * retirement postcondition. */
    MOQ_TEST_CHECK_EQ_INT((int)s->state, pre_state);
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &nom_hooks, &got);
        MOQ_TEST_CHECK_EQ_SIZE(got.count, (size_t)0);
        collect_actions(s, &nom_hooks, &got);
        nom_want_terminal(&want, c->pre_setup, NOM_TARGET_REF);
        failures += txs_norm_equals(&got, &want, what);
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    if (!c->fin) check_drain_ref_reason(s, target, MOQ_DRAIN_NORMAL);
    failures += nom_check_complete(s, &seed, NOM_SEED_REF, NOM_TARGET_REF,
                                   pre_state, &comp_want, &before, &blk, what);
    failures += nom_carrier_retired(s, carrier_base, what);
    if (nom_perturb == 2) {
        /* Leave a phantom target owner behind: the completion oracle must
         * name it. */
        moq_request_endpoint_t pep;
        memset(&pep, 0, sizeof(pep));
        pep.kind = MOQ_REQ_SUBSCRIPTION;
        pep.slot = seed.slot;
        request_registry_insert_by_streamref(
            s, moq_stream_ref_from_u64(NOM_TARGET_REF), pep);
        failures += nom_check_complete(s, &seed, NOM_SEED_REF, NOM_TARGET_REF,
                                       pre_state, &comp_want, &before, &blk,
                                       "perturb-2");
        request_registry_remove_by_streamref(
            s, moq_stream_ref_from_u64(NOM_TARGET_REF));
    }

    /* Exactly once: a second empty re-feed emits nothing, resurrects
     * nothing, and reproduces the WHOLE postcondition. */
    rc = moq_session_on_bidi_stream_bytes(s, target, NULL, 0, false, 1);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    check_no_output(s, &nom_hooks);
    failures += nom_check_complete(s, &seed, NOM_SEED_REF, NOM_TARGET_REF,
                                   pre_state, &comp_want, &before, &blk, what);
    failures += nom_carrier_retired(s, carrier_base, "replay");
    /* The send buffer's accounting end state: the ring is empty and this
     * advancing call has run, so every queued byte -- blocker and terminal
     * alike -- must be reclaimed. */
    MOQ_TEST_CHECK_EQ_SIZE(s->send_len, (size_t)0);

    /* Without a peer FIN the drain ref is RELEASABLE: the later FIN restores
     * the DECLARED ring (never an observed one), emits nothing, and the whole
     * retirement + seed postcondition holds again. */
    if (!c->fin) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, target, NULL, 0, true, 1),
            (int)MOQ_OK);
        check_no_output(s, &nom_hooks);
        failures += nom_check_complete(s, &seed, NOM_SEED_REF, NOM_TARGET_REF,
                                       pre_state, &late_want, &before, &blk,
                                       what);
        failures += nom_carrier_retired(s, carrier_base, "late-terminal");
        MOQ_TEST_CHECK_EQ_SIZE(s->send_len, (size_t)0);
    }
    moq_session_destroy(s);
}


/* Capacity-available controls: the SAME seed and target, no blocker; each
 * must complete IMMEDIATELY through the exact terminal branch. These pin the
 * branch the blocked rows defer; a completed target emits no event. */
static void run_nom_control(bool pre_setup, bool fin)
{
    nom_case_t c;
    memset(&c, 0, sizeof(c));
    c.name = pre_setup ? (fin ? "control-pre-fin" : "control-pre")
                       : (fin ? "control-post-fin" : "control-post");
    c.origin = 0;
    c.fin = fin;
    c.pre_setup = pre_setup;
    char what[96];
    snprintf(what, sizeof(what), "nom %s", c.name);
    moq_session_t *s = nom_make_session(!pre_setup, 0, 0);
    if (!s) return;
    MOQ_TEST_CHECK(noslot_carrier_install(
        s, moq_stream_ref_from_u64(NOM_UNREL_CARRIER), false));
    size_t carrier_base = s->noslot_carrier_count;
    int seed_slot = -1;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state == MOQ_SUB_FREE) { seed_slot = (int)i; break; }
    uint8_t sub[128];
    size_t sub_len = nom_encode_subscribe(sub, sizeof(sub));
    moq_stream_ref_t seed_ref = moq_stream_ref_from_u64(NOM_SEED_REF);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, seed_ref, sub, sub_len - 4,
                                              false, 1),
        (int)MOQ_OK);
    nom_seed_t seed;
    nom_seed_capture(s, &seed);
    nom_seed_arm_assert(&seed, seed_slot);
    failures += nom_graph_seed_only(s, NOM_SEED_REF, seed_slot,
                                    NOM_TARGET_REF, NULL, "control-seeded");
    int pre_state = (int)s->state;
    txs_snapshot_t ctrl_base;
    txs_capture(s, &seed_ref, 1, &ctrl_base);
    expect_after_call_prepare(&ctrl_base);
    drain_snap_t none, wantr;
    memset(&none, 0, sizeof(none));
    uint8_t tgt[128];
    size_t tgt_len = nom_encode_subscribe(tgt, sizeof(tgt));
    moq_stream_ref_t target = moq_stream_ref_from_u64(NOM_TARGET_REF);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, target, tgt, tgt_len, fin, 1),
        (int)MOQ_OK);
    /* Immediate completion creates NO target carrier. */
    failures += nom_carrier_retired(s, carrier_base, "control-immediate");
    {
        txs_norm_vec_t got, want;
        txs_norm_init(&got);
        txs_norm_init(&want);
        collect_events(s, &nom_hooks, &got);
        MOQ_TEST_CHECK_EQ_SIZE(got.count, (size_t)0);
        collect_actions(s, &nom_hooks, &got);
        nom_want_terminal(&want, pre_setup, NOM_TARGET_REF);
        failures += txs_norm_equals(&got, &want, what);
        txs_norm_free(&got);
        txs_norm_free(&want);
    }
    if (fin) wantr = none;
    else drain_snap_plus(&none, target, MOQ_DRAIN_NORMAL, &wantr);
    (void)seed_slot;
    failures += nom_check_complete(s, &seed, NOM_SEED_REF, NOM_TARGET_REF,
                                   pre_state, &wantr, &ctrl_base, NULL, what);
    failures += nom_carrier_retired(s, carrier_base, "control-complete");
    moq_session_destroy(s);
}


/* One legal MONOTONIC product route on the carrier's FIN: refuse the complete
 * target with NO FIN (carrier false), then -- while the SAME blocker remains --
 * feed the target's legal empty FIN so the SAME carrier flips to true with the
 * count unchanged, then recover with an empty fin=false re-feed. Because the FIN
 * is already observed in the carrier, the completed terminal owes NO drain. */
static void run_nom_carrier_monotonic(void)
{
    const char *what = "nom carrier monotonic";
    moq_session_t *s = nom_make_session(true, 1, 0);   /* ONE action slot */
    if (!s) return;

    /* Seed the sole subscription slot with a fragment. */
    uint8_t sub[128];
    size_t sub_len = nom_encode_subscribe(sub, sizeof(sub));
    moq_stream_ref_t seed_ref = moq_stream_ref_from_u64(NOM_SEED_REF);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, seed_ref, sub, sub_len - 4,
                                              false, 1),
        (int)MOQ_OK);

    MOQ_TEST_CHECK(noslot_carrier_install(
        s, moq_stream_ref_from_u64(NOM_UNREL_CARRIER), false));
    size_t base = s->noslot_carrier_count;

    /* Action-block: fill the ONE slot so the no-slot terminal cannot send. */
    moq_stream_ref_t blocker = moq_stream_ref_from_u64(0x6000);
    MOQ_TEST_CHECK_EQ_INT((int)queue_close_bidi(s, blocker), (int)MOQ_OK);
    MOQ_TEST_CHECK(action_queue_full(s));

    uint8_t tgt[128];
    size_t tgt_len = nom_encode_subscribe(tgt, sizeof(tgt));
    moq_stream_ref_t target = moq_stream_ref_from_u64(NOM_TARGET_REF);

    /* 1. Complete target, NO FIN: refused, carrier installed with fin=false. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, target, tgt, tgt_len, false, 1),
        (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(nom_carrier_state(s, NOM_TARGET_REF), 0);
    MOQ_TEST_CHECK_EQ_INT(nom_carrier_state(s, NOM_UNREL_CARRIER), 0);
    MOQ_TEST_CHECK_EQ_SIZE(s->noslot_carrier_count, base + 1);

    /* 2. Legal empty FIN while the blocker remains: the SAME carrier flips to
     * true, count unchanged. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, target, NULL, 0, true, 1),
        (int)MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(nom_carrier_state(s, NOM_TARGET_REF), 1);
    MOQ_TEST_CHECK_EQ_SIZE(s->noslot_carrier_count, base + 1);

    /* 3. Recover: drain the blocker, then an empty fin=false re-feed completes
     * the refusal, taking NO drain (the FIN is already carried). */
    {
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    }
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, target, NULL, 0, false, 1),
        (int)MOQ_OK);
    failures += nom_carrier_retired(s, base, what);
    drain_snap_t ring_after;
    drain_snap(s, &ring_after);
    failures += drain_multiset_equals(&ring_after, &ring_before,
                                      "monotonic no drain");
    /* The completion drained its own terminal; a repeat is inert. */
    {
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, target, NULL, 0, false, 1),
        (int)MOQ_OK);
    failures += nom_carrier_retired(s, base, "monotonic-idempotent");
    moq_session_destroy(s);
}


/* Both sides of the identity-qualified RID allowance, load-bearing against the
 * REAL graph-conservation oracle at BOTH declared ids (0 for origins 1-3, 2 for
 * origin 4): a correctly linked target RID edge at the DECLARED id is ACCEPTED;
 * the same id repointed at the seed is REJECTED by identity; and a wrong-key
 * edge (id 0 while the row declares 2) is REJECTED by key. Quiet: only failures
 * print. */
static void nom_rid_allowance_selfcheck(void)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    cfg.version = MOQ_VERSION_DRAFT_18;
    cfg.max_subscriptions = 2;
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
    if (!s) return;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(s, 0), (int)MOQ_OK);
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup(&w), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w),
                                          0),
        (int)MOQ_OK);
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t sub[128];
    size_t sub_len = nom_encode_subscribe(sub, sizeof(sub));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(
            s, moq_stream_ref_from_u64(NOM_SEED_REF), sub, sub_len - 4,
            false, 1),
        (int)MOQ_OK);
    og_graph_t g0;
    og_capture(s, &g0);
    /* A second staging entry whose declared identity IS the target. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(
            s, moq_stream_ref_from_u64(NOM_TARGET_REF), sub, sub_len - 4,
            false, 1),
        (int)MOQ_OK);
    moq_request_endpoint_t tep = request_registry_find_by_streamref(
        s, moq_stream_ref_from_u64(NOM_TARGET_REF));
    MOQ_TEST_CHECK_EQ_INT((int)tep.kind, (int)MOQ_REQ_SUBSCRIPTION);
    moq_request_endpoint_t sep = request_registry_find_by_streamref(
        s, moq_stream_ref_from_u64(NOM_SEED_REF));
    MOQ_TEST_CHECK_EQ_INT((int)sep.kind, (int)MOQ_REQ_SUBSCRIPTION);

    /* id 0 (origins 1-3). POSITIVE: the identity-linked rid-0 edge is accepted;
     * NEGATIVE: the same id repointed at the SEED owner is rejected. */
    request_registry_insert_by_id(s, 0, tep);
    MOQ_TEST_CHECK_EQ_INT(
        nom_graph_conserved(s, &g0, NOM_TARGET_REF, 0, "rid0-positive"), 0);
    request_registry_remove_by_id(s, 0);
    request_registry_insert_by_id(s, 0, sep);
    og_quiet = 1;
    MOQ_TEST_CHECK(
        nom_graph_conserved(s, &g0, NOM_TARGET_REF, 0, "rid0-identity") > 0);
    og_quiet = 0;
    request_registry_remove_by_id(s, 0);

    /* id 2 (origin 4). POSITIVE: the identity-linked rid-2 edge is accepted at
     * the declared id 2. */
    request_registry_insert_by_id(s, 2, tep);
    MOQ_TEST_CHECK_EQ_INT(
        nom_graph_conserved(s, &g0, NOM_TARGET_REF, 2, "rid2-positive"), 0);
    /* NEGATIVE (wrong key): a rid-2 edge is rejected when the row declares id 0,
     * and symmetrically a rid-0 edge is rejected when the row declares id 2. */
    og_quiet = 1;
    MOQ_TEST_CHECK(
        nom_graph_conserved(s, &g0, NOM_TARGET_REF, 0, "rid2-wrongkey") > 0);
    og_quiet = 0;
    request_registry_remove_by_id(s, 2);
    request_registry_insert_by_id(s, 0, tep);
    og_quiet = 1;
    MOQ_TEST_CHECK(
        nom_graph_conserved(s, &g0, NOM_TARGET_REF, 2, "rid0-wrongkey") > 0);
    og_quiet = 0;
    request_registry_remove_by_id(s, 0);
    /* NEGATIVE (identity): a rid-2 edge repointed at the SEED is rejected. */
    request_registry_insert_by_id(s, 2, sep);
    og_quiet = 1;
    MOQ_TEST_CHECK(
        nom_graph_conserved(s, &g0, NOM_TARGET_REF, 2, "rid2-identity") > 0);
    og_quiet = 0;
    request_registry_remove_by_id(s, 2);
    moq_session_destroy(s);
}


void test_no_owner_admission_matrix(void)
{
    nom_outcome_selfcheck();
    nom_rid_allowance_selfcheck();
    MOQ_TEST_CHECK_EQ_INT(nom_perturb, 0);

    /* Controls first: they emit no event and pin the exact terminal branches. */
    run_nom_control(true, false);
    run_nom_control(true, true);
    run_nom_control(false, false);
    run_nom_control(false, true);

    /* Origin 1 is reachable only without a FIN (the preflight is !fin-gated). */
    nom_case_t c;
    memset(&c, 0, sizeof(c));
    c.origin = 1; c.pre_setup = false; c.fin = false; c.target_rid = 0;
    c.name = "o1-drain-nofin";       run_nom_case(&c);
    c.origin = 2; c.pre_setup = true;
    c.fin = false; c.name = "o2-presetup-nofin"; run_nom_case(&c);
    c.fin = true;  c.name = "o2-presetup-fin";   run_nom_case(&c);
    c.origin = 3; c.pre_setup = false;
    c.fin = false; c.name = "o3-action-nofin";   run_nom_case(&c);
    c.fin = true;  c.name = "o3-action-fin";     run_nom_case(&c);
    /* Origin 4's live announcement blocker commits inbound id 0, so its target
     * is the next id, 2. */
    c.origin = 4; c.pre_setup = false; c.target_rid = 2;
    c.fin = false; c.name = "o4-send-nofin";     run_nom_case(&c);
    c.fin = true;  c.name = "o4-send-fin";       run_nom_case(&c);

    run_nom_carrier_monotonic();
}
