#include <moq/relay/relay.h>

#include <moq/relay/capacity.h>

#include <moq/rcbuf.h>

#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

/*
 * Control-plane implementation. Sections: canonical keys · pools ·
 * intents · trie · tracks · subscriptions · bindings · ops · delivery.
 *
 * Determinism: every advancing call mutates under single-writer rules and
 * emits scalar trace records; intents are the only outward channel and
 * every op reserves ring space BEFORE mutating (WOULD_BLOCK = untouched).
 *
 * Intent view lifetimes: only UPSTREAM_SUBSCRIBE (track key storage) and
 * NS_FOUND/NS_GONE (trie node storage; nodes are never freed before core
 * destroy) carry byte views. Track key storage outlives its
 * UPSTREAM_SUBSCRIBE intent because a PENDING track is only freed by the
 * resolution that answers that intent. All other intents correlate by
 * cookie/handle and carry no views.
 */

#define R_DEF_BINDINGS   64u
#define R_DEF_TRACKS     64u
#define R_DEF_SUBS       256u
#define R_DEF_NS_NODES   256u
#define R_DEF_NS_SUBS    64u
#define R_DEF_INTENTS    64u
#define R_DEF_INTERN     (64u * 1024u)
#define R_DEF_PARKED       64u
#define R_DEF_PARKED_BYTES (64u * 1024u)
#define R_DEF_GRANTS       64u
#define R_DEF_GRANT_BYTES  (64u * 1024u)
#define R_DEF_FETCHES        64u
#define R_DEF_FETCH_PIN_BYTES (8u * 1024u * 1024u)   /* floored >= log max_bytes */

/* A terminal this relay chose is a meaning, not a draft's number: the
 * subscriber that reads it may speak either draft. */
static moqr_pd_desc_t
core_local_done(moqr_pd_status_t status)
{
    moqr_pd_desc_t d;

    if (moqr_pd_desc_local(status, &d) != MOQR_OK) {
        return moqr_pd_desc_none();
    }
    return d;
}

static uint64_t gpos_stride_for(uint64_t lists);
static uint64_t gpos_hash_cap_for(uint64_t groups);

/* REQUEST_ERROR codes (REJECT_SUB / reject_subscribe / cancel_namespace all
 * take this registry). */
#define R_ERR_DOES_NOT_EXIST 0x10u   /* mirrors MOQ_REQUEST_ERROR_DOES_NOT_EXIST */
#define R_ERR_INVALID_RANGE  0x11u   /* mirrors MOQ_REQUEST_ERROR_INVALID_RANGE */
#define R_ERR_UNAUTHORIZED   0x1u    /* mirrors MOQ_REQUEST_ERROR_UNAUTHORIZED  */
#define R_ERR_INTERNAL       0x0u

/* PUBLISH_DONE status codes (SUB_DONE / done_subscribe take this registry —
 * a DIFFERENT namespace from REQUEST_ERROR and from stream-reset codes). */
#define R_DONE_TRACK_ENDED   0x2u    /* the track is no longer being published */
#define R_DONE_SUB_ENDED     0x3u    /* end of the subscription's filter range */

/* The single-shard handle tag: a core with shard_index 0 packs this, keeping
 * the default build bit-identical. Per-core, the tag is shard_index + 1 (stored
 * on moqr_core_t) and the resolvers require the core's own tag, so a handle
 * repacked with a foreign shard tag is structurally refused. */
#define R_SHARD_TAG          1u

/* Borrowed-view validation: a non-zero length requires a data pointer,
 * and every namespace/name length is bounded by MOQ_FULL_TRACK_NAME_MAX
 * (per part, per name, and cumulatively) BEFORE anything narrows or
 * copies — the sizing math below runs on validated values only. */
static bool
bytes_view_ok(moq_bytes_t b)
{
    return (b.len == 0 || b.data != NULL) &&
           b.len <= MOQ_FULL_TRACK_NAME_MAX;
}

/* A PRESENT Track Namespace Field must contain at least one byte: draft-18 and
 * draft-16 Section 2.4.1 both require it. That is stricter than bytes_view_ok,
 * which exists for Track NAMES -- those may legitimately be empty in both
 * drafts. Keeping the two apart is what lets the core accept an empty Track
 * Name while rejecting an empty namespace field. */
static bool
ns_field_view_ok(moq_bytes_t b)
{
    return b.len > 0 && b.data != NULL && b.len <= MOQ_FULL_TRACK_NAME_MAX;
}

static bool
ns_view_len(moqr_ns_t ns, size_t min_count, size_t *out_total)
{
    if (ns.count < min_count || ns.count > 32) {
        return false;
    }
    if (ns.count > 0 && ns.parts == NULL) {
        return false;
    }
    size_t total = 0;
    for (size_t i = 0; i < ns.count; i++) {
        if (!ns_field_view_ok(ns.parts[i])) {
            return false;
        }
        total += ns.parts[i].len;
        if (total > MOQ_FULL_TRACK_NAME_MAX) {
            return false;
        }
    }
    *out_total = total;
    return true;
}

/* min_count is the protocol-neutral floor: draft-18 Section 2.4.1 allows a
 * Track Namespace of 0..32 fields (the ROOT namespace), draft-16 Section 2.4.1
 * allows 1..32, so the core represents both and the negotiated session profile
 * enforces its own minimum. */
static bool
ns_view_ok(moqr_ns_t ns, size_t min_count)
{
    size_t total = 0;
    return ns_view_len(ns, min_count, &total);
}

/* Full Track Name: a Track Namespace plus a Track Name, sharing the combined
 * cap. draft-18 Section 2.4.1 builds it on the same 0..32-field namespace, so a
 * ROOT Full Track Name -- zero fields plus a name -- is valid here; the Track
 * Name itself may be empty in both drafts. The core stays protocol-neutral and
 * the session profile rejects a draft-16 zero-field namespace before it. */
static bool
ftn_view_ok(moqr_ns_t ns, moq_bytes_t name)
{
    size_t total = 0;
    return ns_view_len(ns, 0, &total) && bytes_view_ok(name) &&
           total + name.len <= MOQ_FULL_TRACK_NAME_MAX;
}

/* -- canonical full-track-name key ---------------------------------------- *
 * Layout: u32 part_count P | u32 part_len[P] | u32 name_len | part bytes...
 * | name bytes. Self-contained, hashable, prefix-walkable. */

typedef struct r_key {
    uint8_t *buf;
    uint32_t len;
} r_key_t;

static uint32_t
key_rd32(const uint8_t *p)
{
    uint32_t v;
    memcpy(&v, p, sizeof(v));
    return v;
}

static void
key_wr32(uint8_t *p, uint32_t v)
{
    memcpy(p, &v, sizeof(v));
}

static uint32_t
key_part_count(const r_key_t *k)
{
    return key_rd32(k->buf);
}

static moq_bytes_t
key_part(const r_key_t *k, uint32_t i)
{
    uint32_t count = key_rd32(k->buf);
    uint32_t off = 4u * (2u + count);
    for (uint32_t p = 0; p < i; p++) {
        off += key_rd32(k->buf + 4u * (1u + p));
    }
    return (moq_bytes_t){ .data = k->buf + off,
                          .len = key_rd32(k->buf + 4u * (1u + i)) };
}

static moq_bytes_t
key_name(const r_key_t *k)
{
    uint32_t count = key_rd32(k->buf);
    uint32_t off = 4u * (2u + count);
    for (uint32_t p = 0; p < count; p++) {
        off += key_rd32(k->buf + 4u * (1u + p));
    }
    return (moq_bytes_t){ .data = k->buf + off,
                          .len = key_rd32(k->buf + 4u * (1u + count)) };
}

static uint64_t
key_hash(const r_key_t *k)
{
    uint64_t h = UINT64_C(0xCBF29CE484222325);
    for (uint32_t i = 0; i < k->len; i++) {
        h ^= k->buf[i];
        h *= UINT64_C(0x100000001B3);
    }
    return h;
}

static bool
key_eq(const r_key_t *a, const r_key_t *b)
{
    return a->len == b->len && memcmp(a->buf, b->buf, a->len) == 0;
}

/* -- pools ------------------------------------------------------------------ */

typedef struct r_trie_node {
    uint8_t  *part;        /* owned copy; NULL for root                    */
    uint32_t  part_len;
    uint32_t  parent;      /* UINT32_MAX for root                          */
    uint32_t *children;    /* grown array of node indices                  */
    uint32_t  child_count;
    uint32_t  child_cap;
    bool      has_announce;
    uint32_t  ann_binding;      /* binding slot                            */
    uint32_t  ann_binding_gen;
    uint64_t  session_cookie;   /* peer wire announce handle; 0 for mirrors */
} r_trie_node_t;

typedef struct r_ns_sub {
    bool      used;
    uint32_t  binding;
    uint32_t  binding_gen;
    uint64_t  cookie;
    uint8_t  *prefix;      /* canonical ns-only buffer (key layout, no name) */
    uint32_t  prefix_len;
} r_ns_sub_t;

typedef uint32_t r_track_state_t;
#define R_TRACK_PENDING 1u
#define R_TRACK_ACTIVE  2u
#define R_TRACK_WARM    3u

typedef uint8_t r_track_source_t;
#define R_TRACK_SOURCE_NONE 0u
#define R_TRACK_SOURCE_PULL 1u
#define R_TRACK_SOURCE_PUSH 2u

typedef struct r_track {
    uint32_t        gen;          /* odd = live                            */
    /* Head of the per-track subscriber index (R_SUB_NIL when empty). */
    uint32_t        subs_head;
    r_key_t         key;
    uint64_t        hash;
    moqr_log_t     *log;
    r_track_state_t state;
    uint64_t        track_gen;    /* upstream attempt identity guard       */
    r_track_source_t source_kind;
    bool            has_upstream_binding;
    uint32_t        up_binding;
    uint32_t        up_binding_gen;
    /* The announce that sourced this track, captured when the upstream was
     * chosen (subscribe-create / WARM rejoin / a self-owned publish_open).
     * UINT32_MAX = no announce source (an unannounced direct push). Force-withdraw
     * purges by this stored identity, never a re-derived longest-prefix. */
    uint32_t        src_ann_node;
    uint32_t        src_ann_binding;
    uint32_t        src_ann_binding_gen;
    uint64_t        upstream_cookie;
    bool            has_largest;
    uint64_t        largest_group;
    uint64_t        largest_object;
    uint64_t        linger_deadline_us;   /* 0 = unarmed                   */
} r_track_t;

typedef uint32_t r_sub_state_t;
#define R_SUB_PARKED 1u
#define R_SUB_ACTIVE 2u

/* Terminator for the per-binding subscription list (r_binding_t.subs_head /
 * r_sub_t.sub_next/prev). Slot 0 is a valid sub, so the empty sentinel is
 * UINT32_MAX, matching sub_slot_find's not-found value. */
#define R_SUB_NIL UINT32_MAX

/* Empty marker for the per-sub group_id -> gpos-slot index. */
#define R_GPOS_NIL UINT32_MAX

/* Per-(sub, group) scheduling positions: one read index per subgroup list
 * plus one for the datagram list (the last slot). Entries live in a
 * per-sub block of max_groups strides; group_id UINT64_MAX = free. */
typedef struct r_gpos {
    uint64_t group_id;
    /* Two parallel per-list arrays packed back-to-back: idx[0..gpos_lists-1]
     * (the next record index per subgroup list + datagram list), then
     * emitted[0..gpos_lists-1] (the live-edge chunk watermark — how many chunks
     * of the current OPEN head this sub has already scheduled downstream, so the
     * scheduler re-selects the head only when it grows past it or flips
     * COMPLETE/ABANDONED). emitted resets to 0 when idx advances to a new
     * record. Access emitted via gpos_emit(). */
    uint32_t idx[];
} r_gpos_t;

typedef struct r_sub {
    uint32_t      gen;            /* odd = live                            */
    uint32_t      binding;
    uint32_t      binding_gen;
    uint32_t      sub_next;       /* per-binding sub list (R_SUB_NIL = end) */
    uint32_t      sub_prev;
    uint32_t      track;
    uint32_t      track_gen_slot; /* track pool generation at bind         */
    /* Per-track subscriber index (intrusive, R_SUB_NIL = end): mirrors the
     * exact membership predicate of track_sub_count — linked when a live
     * sub binds a live track, unlinked at retire; a dying track clears its
     * whole list before its gen bump. Links of non-members are meaningless
     * and never read (traversal starts at the track's subs_head). */
    uint32_t      track_next;
    uint32_t      track_prev;
    uint8_t      *gpos;           /* positions block; see r_gpos_t         */
    uint32_t      gpos_groups;    /* entries                               */
    uint32_t      gpos_lists;     /* idx[] length = subgroup lists + 1     */
    /* O(1) group_id -> gpos-array-slot index: open-addressing,
     * R_GPOS_NIL = empty. gpos_gc_evicted gates the eviction-reclaim scan on
     * the log's monotonic evicted_groups_total. */
    uint32_t     *gpos_hash;
    uint32_t      gpos_hash_cap;  /* power of two                          */
    uint64_t      gpos_gc_evicted;
    uint32_t      begun_count;    /* # of (group,list) positions with a begun
                                   * live-edge object (emitted > 0). Gates the
                                   * evict-reset sweep: a begun-evicted group
                                   * requires emitted>0, so a sub with
                                   * begun_count==0 can never need a reset.     */
    bool          pending_skip;   /* eviction jumped positions; report on
                                   * the next delivery                     */
    bool          forward;        /* downstream Forward state (§10.2.12):
                                   * false = paused, never selected for
                                   * delivery until SUBSCRIBE_UPDATE flips
                                   * it back. Distinct from R_SUB_PARKED
                                   * (which is "awaiting upstream"). Fills
                                   * existing bool padding — no layout grow. */
    r_sub_state_t state;
    uint64_t      start_group;
    uint64_t      start_object;
    bool          has_end;
    uint64_t      end_group;
    uint8_t       subscriber_priority;
    moqr_group_order_t group_order;
    moqr_filter_type_t filter_type;
    uint64_t      cookie;
} r_sub_t;

/* Deep-copied borrowed request material (namespace / name / token bytes),
 * shared by parked-DEFER requests and revalidation grants so the security-
 * sensitive copy + validation + byte accounting lives in exactly one place
 * (r_copy_material / r_free_material). */
typedef struct r_material {
    moq_bytes_t       *ns_parts;  /* ns_count entries, .data into ns_bytes  */
    uint32_t           ns_count;
    uint8_t           *ns_bytes;
    size_t             ns_bytes_len;
    uint8_t           *name;      /* NULL when empty                        */
    size_t             name_len;
    moqr_auth_token_t *tokens;    /* token_count entries, values in tok_bytes */
    size_t             token_count;
    uint8_t           *tok_bytes;
    size_t             tok_bytes_len;
    size_t             bytes_charged;   /* total counted vs the byte budget  */
} r_material;

/* A DEFERred control request, held pending an async auth result. Everything
 * the binding needs to resume or reject is deep-copied here so it survives the
 * session scratch being reused before the result arrives. Keyed for lookup by
 * the hook's external ticket, which the verifier must keep relay-lifetime-
 * unique (see moqr_core_park): a finished/retired entry resolves stale because
 * its slot is freed, NOT by any external-ticket generation. Bounded by
 * max_parked / parked_bytes. */
typedef struct r_parked {
    uint32_t gen;                 /* odd = live                            */
    bool     pinned;              /* between begin_resolve and finish       */
    bool     retired;             /* binding closed while pinned: free on finish */
    uint64_t ext_ticket;          /* hook correlation id (nonzero)          */
    uint64_t binding_cookie;      /* owner connection                       */
    uint64_t session_cookie;      /* wire request handle _opaque            */
    moqr_auth_action_t action;
    r_material mat;               /* deep-copied ns / name / tokens         */
    /* SUBSCRIBE op params (unused for other actions) */
    uint32_t sub_filter_type;
    uint64_t sub_start_group, sub_start_object, sub_end_group_delta;
    uint8_t  sub_priority;
    uint32_t sub_group_order;
} r_parked;

/* An active auth grant for a long-lived operation (SUBSCRIBE / announce) whose
 * initial ALLOW carried a revalidation lease (CAT moqt-reval). The request
 * material is deep-copied so the periodic re-check survives the original
 * session event. The core schedules + decides rechecks off tick(now_us); the
 * binding executes any wire teardown. Bounded by max_grants / grant_bytes. */
typedef struct r_grant {
    uint32_t gen;                 /* odd = live (reserved or committed)     */
    bool     committed;           /* op succeeded: eligible for revalidation */
    bool     revoked;             /* reval decided revoke: teardown pending  */
    bool     unannounced;         /* announce revoke: NS cleared; awaiting the
                                   * binding's publisher-side cancel_namespace */
    /* Two DIFFERENT wire domains, deliberately. The announce grant's denial
     * rides PUBLISH_NAMESPACE_CANCEL / parked REQUEST_ERROR, so it is a
     * REQUEST_ERROR-domain number and must never be read as a status. The
     * subscribe grant's teardown is a PUBLISH_DONE terminal and is tagged.
     * ACTIVE downstream subscriptions purged by force_withdraw get their own
     * local PUBLISH_DONE TRACK_ENDED, independent of both. */
    uint64_t revoke_request_error; /* REQUEST_ERROR domain (namespace cancel) */
    moqr_pd_desc_t revoke_pd;      /* PUBLISH_DONE domain (subscribe teardown) */
    uint64_t binding_cookie;      /* owner connection                       */
    uint64_t session_cookie;      /* wire op handle _opaque (announce cancel) */
    uint64_t sub_raw;             /* core moqr_sub_t._opaque (SUBSCRIBE)     */
    moqr_auth_action_t action;    /* SUBSCRIBE or PUBLISH_NAMESPACE          */
    uint64_t lease_us;            /* revalidate_after_us of the last verdict */
    uint64_t next_recheck_us;     /* scheduled recheck (overflow-guarded)    */
    r_material mat;               /* deep-copied ns / name / tokens          */
} r_grant;

/* A force-withdraw publisher cancel awaiting the binding's wire cancel_namespace.
 * Grant-independent, so it lives in its own bounded queue and is merged into the
 * revoked-grant peek/ack surface (see moqr_core_peek_revoked_grants). */
typedef struct r_pending_cancel {
    bool     used;
    uint64_t binding_cookie;
    uint64_t session_cookie;      /* the announce wire handle to cancel      */
    uint64_t error_code;
} r_pending_cancel;

/* One pinned chunk of a chunked delivery: an incref'd zero-copy slice plus its
 * length. The pin keeps the object's bytes alive across same-track eviction so a
 * multi-pump chunked delivery cannot be truncated underneath the binding. */
typedef struct r_pin_chunk {
    moq_rcbuf_t *buf;
    uint64_t     len;
} r_pin_chunk_t;

typedef struct r_binding {
    uint32_t gen;                 /* odd = live                            */
    uint32_t subs_head;           /* head slot of this binding's live-sub
                                   * list (R_SUB_NIL = empty). The delivery
                                   * scheduler walks only these, not all
                                   * c->max_subs.                           */
    uint32_t begun_subs;          /* # of this binding's subs with begun_count>0.
                                   * The evict-reset sweep is skipped entirely
                                   * while this is 0: no begun object anywhere on
                                   * the binding => nothing to reset.           */
    uint64_t cookie;
    bool     out_active;          /* one outstanding delivery              */
    bool     out_chunked;         /* outstanding delivery is a chunked rec */
    bool     out_abandoned;       /* outstanding delivery is an abandoned
                                   * head begun downstream: the bind
                                   * resets, the core advances, no count   */
    bool     out_evict_reset;     /* the abandoned delivery is a whole group
                                   * evicted while begun downstream: the bind
                                   * resets EVERY begun subgroup of the group,
                                   * and the core reclaims the position slot */
    uint8_t  out_notice;          /* outstanding recordless notice
                                   * (MOQR_DELIVERY_NOTICE_*): DELIVERED
                                   * acknowledges it, WOULD_BLOCK re-peeks   */
    uint32_t out_sub;
    uint64_t out_group;           /* position to advance on DELIVERED      */
    uint32_t out_list;
    uint32_t out_chunk_base;      /* pinned batch's first chunk index (the
                                   * emitted watermark at select): the
                                   * accessor maps chunk idx -> idx-base    */
    moq_rcbuf_t *pin_payload;     /* pinned for the outstanding delivery:
                                   * the view stays valid even if the log
                                   * evicts the record underneath it       */
    moq_rcbuf_t *pin_properties;
    /* Chunked delivery (obj_state COMPLETE && chunk_count > 0): the record's
     * chunks are pinned here so eviction can't truncate a multi-pump delivery,
     * and out_view is replayed verbatim on an idempotent re-peek while held.
     * The array is grown lazily to the record's chunk_count and REUSED across
     * deliveries (freed only at binding destroy); its worst case is bounded by
     * the log chunk-node pool and reflected in moqr_core_capacity_describe. */
    r_pin_chunk_t  *pin_chunks;
    uint32_t        pin_chunks_cap;    /* allocated entries                 */
    uint32_t        pin_chunk_count;   /* live pinned entries this delivery */
    moqr_delivery_t out_view;          /* replayed on re-peek while held    */
} r_binding_t;

/* A retained-hit FETCH cursor (relay.h fetch API). Durable retry state is
 * SCALAR (cur_group, cur_object) plus at most one pinned payload/properties
 * pair; a peeked object is held pinned (incref) from peek to commit so it
 * survives log eviction across a blocked downstream write, and re-peek is
 * idempotent by reconstructing the view from the stored scalars + pins. */
typedef struct r_fetch {
    uint32_t gen;                 /* odd = live                            */
    uint32_t binding;
    uint32_t binding_gen;
    uint32_t track;
    uint32_t track_gen_slot;      /* track pool gen at open (identity guard) */
    uint64_t cookie;              /* caller correlation                    */
    /* Served range: [start .. eff_last] inclusive; eff_last capped to the
     * track largest at open. end_whole == "all of end_group". ASCENDING. */
    uint64_t start_group, start_object;
    uint64_t end_group, end_object;
    bool     end_whole;
    moqr_group_order_t group_order;
    uint8_t  subscriber_priority;
    /* Evicted-prefix lead marker: the fetch's Start fell below the retention
     * horizon but the range still intersects retained data, so one UNKNOWN
     * marker is emitted before the first retained group. Cleared on its commit
     * (the only state advance); the cursor already starts at that group. */
    bool     lead_marker;
    uint64_t lead_marker_group;
    /* Scalar cursor: next Location to emit is the smallest NORMAL object at
     * Location >= (cur_group, cur_object) within the range. */
    uint64_t cur_group, cur_object;
    /* Outstanding peeked head (held pinned; reconstructed on re-peek). */
    bool         out_active;
    uint64_t     out_group, out_subgroup, out_object;
    uint8_t      out_priority;
    bool         out_datagram;
    moq_rcbuf_t *pin_payload, *pin_properties;
    uint64_t     pin_bytes;       /* charged against core->fetch_pin_used   */
} r_fetch_t;

struct moqr_core {
    moq_alloc_t   alloc;
    moqr_trace_t *trace;

    /* Shard identity. shard_tag == shard_index + 1 is packed into every handle
     * this core mints, and the resolvers require it, so a handle from another
     * shard is structurally refused. Single-shard default: index 0, count 1,
     * tag 1 (bit-identical to the pre-shard build). */
    uint16_t shard_index;
    uint16_t shard_count;
    uint16_t shard_tag;

    uint32_t max_bindings, max_tracks, max_subs;
    uint32_t max_ns_nodes, max_ns_subs, max_intents;
    uint32_t intern_budget;
    uint32_t intern_used;

    moqr_log_budget_t log_budget;
    uint32_t log_max_subgroups, log_max_objects, log_max_cursors;
    uint32_t log_max_chunk_nodes;
    uint64_t linger_us;

    r_binding_t   *bindings;
    /* Binding-ready set: one bit per binding slot (ceil(max_bindings/64)
     * words) plus an exact running count. Produced by the core's readiness
     * transitions (ready_mark/ready_mark_track), consumed only through
     * moqr_core_drain_ready — nothing inside the core reads it. */
    uint64_t      *ready_words;
    uint32_t       ready_count;
    r_track_t     *tracks;
    r_sub_t       *subs;
    r_trie_node_t *nodes;
    uint32_t       node_count;
    r_ns_sub_t    *ns_subs;

    moqr_intent_t *intents;      /* ring                                   */
    uint32_t       intent_head;
    uint32_t       intent_count;

    /* Epoch triple for trace headers and route dumps. node/shard epochs are
     * control-plane generations (a cluster/shard-set change bumps them) — set
     * once at create and preserved across route mutations; route_epoch bumps on
     * every route mutation within this shard. */
    uint64_t node_epoch;
    uint64_t shard_epoch;
    uint64_t route_epoch;
    uint64_t ingested_total;
    uint64_t delivered_total;
    uint64_t evict_sweeps;       /* times the evict-reset sweep body ran (it is
                                  * gated off while no sub has a begun object) */
    uint64_t evicted_freed;      /* evictions from tracks already freed     */
    uint64_t note_emitted_total; /* begun-downstream checkpoints via
                                  * moqr_core_delivery_note_emitted           */
    uint32_t intent_hwm;         /* peak intent-ring occupancy              */
    uint64_t refusals[MOQR_REFUSE__COUNT];

    moqr_authorize_fn authorize;         /* NULL = allow-all                */
    void             *authorize_ctx;
    uint64_t auth_decisions[MOQR_AUTH_ACTION__COUNT][3];
    uint64_t auth_denials[MOQR_AUTH_REASON__COUNT];

    r_parked *parked;                    /* deferred-auth request storage   */
    uint32_t  max_parked;
    uint32_t  parked_bytes_cap;
    uint32_t  parked_bytes_used;

    r_grant  *grants;                    /* revalidation grant storage      */
    uint32_t  max_grants;
    uint32_t  grant_bytes_cap;
    uint32_t  grant_bytes_used;

    r_pending_cancel *pending_cancels;   /* force-withdraw publisher cancels */
    uint32_t  max_cancels;

    r_fetch_t *fetches;                  /* retained-hit fetch cursors      */
    uint32_t   max_fetches;
    uint64_t   fetch_pin_cap;            /* total pinnable payload bytes;
                                          * floored >= per-track log max_bytes */
    uint64_t   fetch_pin_used;
};

/* -- small helpers ------------------------------------------------------------- */

static void *
r_alloc(moqr_core_t *c, size_t n)
{
    return c->alloc.alloc(n, c->alloc.ctx);
}

static void
r_free(moqr_core_t *c, void *p, size_t n)
{
    if (p != NULL) {
        c->alloc.free(p, n, c->alloc.ctx);
    }
}

/* Validate the borrowed request view and deep-copy its namespace / name / token
 * bytes into *m (freshly allocated). Fails closed with no allocation and no
 * accounting change: MOQR_ERR_INVAL (ns.count > 32, a positive count/len with a
 * NULL pointer, or a length sum that overflows), MOQR_ERR_CAPACITY (would push
 * *used past cap), MOQR_ERR_NOMEM. On success *used += m->bytes_charged. Shared
 * by parked-DEFER storage and revalidation grants. */
static moqr_result_t
r_copy_material(moqr_core_t *c, const moqr_park_req_t *req, uint32_t *used,
                uint32_t cap, r_material *m)
{
    memset(m, 0, sizeof(*m));
    if (req->ns.count > 32) {
        return MOQR_ERR_INVAL; /* moqr_ns_t is 0..32 parts; ns_count is uint32_t */
    }
    if (req->token_count > MOQR_AUTH_MAX_TOKENS) {
        return MOQR_ERR_INVAL; /* bound before any tokens[i] walk */
    }
    if ((req->ns.count > 0 && req->ns.parts == NULL) ||
        (req->name.len > 0 && req->name.data == NULL) ||
        (req->token_count > 0 && req->tokens == NULL)) {
        return MOQR_ERR_INVAL;
    }
    size_t ns_bytes_len = 0;
    for (size_t i = 0; i < req->ns.count; i++) {
        size_t len = req->ns.parts[i].len;
        if (len > 0 && req->ns.parts[i].data == NULL) {
            return MOQR_ERR_INVAL;
        }
        if (ns_bytes_len > SIZE_MAX - len) {
            return MOQR_ERR_INVAL;
        }
        ns_bytes_len += len;
    }
    size_t tok_bytes_len = 0;
    for (size_t i = 0; i < req->token_count; i++) {
        size_t len = req->tokens[i].token_value.len;
        if (len > 0 && req->tokens[i].token_value.data == NULL) {
            return MOQR_ERR_INVAL;
        }
        if (tok_bytes_len > SIZE_MAX - len) {
            return MOQR_ERR_INVAL;
        }
        tok_bytes_len += len;
    }
    if ((req->ns.count != 0 &&
         req->ns.count > SIZE_MAX / sizeof(moq_bytes_t)) ||
        (req->token_count != 0 &&
         req->token_count > SIZE_MAX / sizeof(moqr_auth_token_t))) {
        return MOQR_ERR_INVAL;
    }
    const size_t terms[5] = {
        ns_bytes_len,
        req->name.len,
        tok_bytes_len,
        req->ns.count * sizeof(moq_bytes_t),
        req->token_count * sizeof(moqr_auth_token_t),
    };
    size_t charge = 0;
    for (size_t i = 0; i < 5; i++) {
        if (charge > SIZE_MAX - terms[i]) {
            return MOQR_ERR_INVAL;
        }
        charge += terms[i];
    }
    if ((uint64_t)*used + charge > cap) {
        return MOQR_ERR_CAPACITY;
    }
    moq_bytes_t       *ns_parts = NULL;
    uint8_t           *ns_bytes = NULL;
    uint8_t           *name = NULL;
    moqr_auth_token_t *tokens = NULL;
    uint8_t           *tok_bytes = NULL;
    if (req->ns.count > 0) {
        ns_parts = r_alloc(c, req->ns.count * sizeof(moq_bytes_t));
        ns_bytes = ns_bytes_len > 0 ? r_alloc(c, ns_bytes_len) : NULL;
        if (ns_parts == NULL || (ns_bytes_len > 0 && ns_bytes == NULL)) {
            goto nomem;
        }
        size_t off = 0;
        for (size_t i = 0; i < req->ns.count; i++) {
            size_t len = req->ns.parts[i].len;
            if (len > 0) {
                memcpy(ns_bytes + off, req->ns.parts[i].data, len);
                ns_parts[i].data = ns_bytes + off;
            } else {
                ns_parts[i].data = NULL; /* empty part: never NULL + off */
            }
            ns_parts[i].len = len;
            off += len;
        }
    }
    if (req->name.len > 0) {
        name = r_alloc(c, req->name.len);
        if (name == NULL) {
            goto nomem;
        }
        memcpy(name, req->name.data, req->name.len);
    }
    if (req->token_count > 0) {
        tokens = r_alloc(c, req->token_count * sizeof(moqr_auth_token_t));
        tok_bytes = tok_bytes_len > 0 ? r_alloc(c, tok_bytes_len) : NULL;
        if (tokens == NULL || (tok_bytes_len > 0 && tok_bytes == NULL)) {
            goto nomem;
        }
        size_t off = 0;
        for (size_t i = 0; i < req->token_count; i++) {
            size_t len = req->tokens[i].token_value.len;
            tokens[i].token_type = req->tokens[i].token_type;
            if (len > 0) {
                memcpy(tok_bytes + off, req->tokens[i].token_value.data, len);
                tokens[i].token_value.data = tok_bytes + off;
            } else {
                tokens[i].token_value.data = NULL;
            }
            tokens[i].token_value.len = len;
            off += len;
        }
    }
    m->ns_parts = ns_parts;
    m->ns_count = (uint32_t)req->ns.count;
    m->ns_bytes = ns_bytes;
    m->ns_bytes_len = ns_bytes_len;
    m->name = name;
    m->name_len = req->name.len;
    m->tokens = tokens;
    m->token_count = req->token_count;
    m->tok_bytes = tok_bytes;
    m->tok_bytes_len = tok_bytes_len;
    m->bytes_charged = charge;
    *used += (uint32_t)charge;
    return MOQR_OK;
nomem:
    r_free(c, ns_parts, req->ns.count * sizeof(moq_bytes_t));
    r_free(c, ns_bytes, ns_bytes_len);
    r_free(c, name, req->name.len);
    r_free(c, tokens, req->token_count * sizeof(moqr_auth_token_t));
    r_free(c, tok_bytes, tok_bytes_len);
    return MOQR_ERR_NOMEM;
}

static void
r_free_material(moqr_core_t *c, r_material *m, uint32_t *used)
{
    r_free(c, m->ns_parts, (size_t)m->ns_count * sizeof(*m->ns_parts));
    r_free(c, m->ns_bytes, m->ns_bytes_len);
    r_free(c, m->name, m->name_len);
    r_free(c, m->tokens, (size_t)m->token_count * sizeof(*m->tokens));
    r_free(c, m->tok_bytes, m->tok_bytes_len);
    if (*used >= m->bytes_charged) {
        *used -= (uint32_t)m->bytes_charged;
    } else {
        *used = 0;
    }
    memset(m, 0, sizeof(*m));
}

static void
r_trace(moqr_core_t *c, moqr_trace_kind_t kind, uint32_t detail,
        uint64_t e0, uint64_t e1, uint64_t e2, uint64_t e3)
{
    if (c->trace == NULL) {
        return;
    }
    moqr_trace_rec_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.kind = kind;
    rec.detail = detail;
    rec.e0 = e0;
    rec.e1 = e1;
    rec.e2 = e2;
    rec.e3 = e3;
    moqr_trace_emit(c->trace, rec);
}

/* Push the current epoch triple onto the trace ring so subsequent records name
 * their generation. node/shard epochs are preserved from the core (control-plane
 * generations); route_epoch is the core's per-mutation counter. */
static void
r_publish_epochs(moqr_core_t *c)
{
    if (c->trace == NULL) {
        return;
    }
    moqr_epochs_t e = MOQR_EPOCHS_INIT;
    e.node_epoch = c->node_epoch;
    e.shard_epoch = c->shard_epoch;
    e.route_epoch = c->route_epoch;
    moqr_trace_set_epochs(c->trace, e);
}

static void
r_bump_route_epoch(moqr_core_t *c)
{
    c->route_epoch++;
    r_publish_epochs(c);
}

void
moqr_core_set_epochs(moqr_core_t *c, uint64_t node_epoch, uint64_t shard_epoch)
{
    if (c == NULL) {
        return;
    }
    c->node_epoch = node_epoch;
    c->shard_epoch = shard_epoch;
    r_publish_epochs(c);
}

/* handle helpers */

static uint64_t
r_pack(const moqr_core_t *c, uint32_t pool, uint32_t gen, uint32_t slot)
{
    return moq_handle_pack(pool, c->shard_tag, gen, slot);
}

bool
moqr_sub_is_valid(moqr_sub_t h)
{
    return moq_handle_pool_tag(h._opaque) == MOQR_HANDLE_POOL_SUB &&
           moq_handle_session_tag(h._opaque) != 0 &&
           (moq_handle_generation(h._opaque) & 1u) != 0;
}

bool
moqr_sub_eq(moqr_sub_t a, moqr_sub_t b)
{
    return a._opaque == b._opaque;
}

static r_binding_t *
binding_resolve(moqr_core_t *c, moqr_binding_t h, uint32_t *out_slot)
{
    if (moq_handle_pool_tag(h._opaque) != MOQR_HANDLE_POOL_BINDING ||
        moq_handle_session_tag(h._opaque) != c->shard_tag ||
        (moq_handle_generation(h._opaque) & 1u) == 0) {
        return NULL;
    }
    uint32_t slot = moq_handle_slot(h._opaque);
    if (slot >= c->max_bindings ||
        c->bindings[slot].gen != moq_handle_generation(h._opaque)) {
        return NULL;
    }
    if (out_slot != NULL) {
        *out_slot = slot;
    }
    return &c->bindings[slot];
}

static r_track_t *
track_resolve(moqr_core_t *c, moqr_track_t h, uint32_t *out_slot)
{
    if (moq_handle_pool_tag(h._opaque) != MOQR_HANDLE_POOL_TRACK ||
        moq_handle_session_tag(h._opaque) != c->shard_tag ||
        (moq_handle_generation(h._opaque) & 1u) == 0) {
        return NULL;
    }
    uint32_t slot = moq_handle_slot(h._opaque);
    if (slot >= c->max_tracks ||
        c->tracks[slot].gen != moq_handle_generation(h._opaque)) {
        return NULL;
    }
    if (out_slot != NULL) {
        *out_slot = slot;
    }
    return &c->tracks[slot];
}

static r_sub_t *
sub_resolve(moqr_core_t *c, moqr_sub_t h, uint32_t *out_slot)
{
    if (moq_handle_pool_tag(h._opaque) != MOQR_HANDLE_POOL_SUB ||
        moq_handle_session_tag(h._opaque) != c->shard_tag ||
        (moq_handle_generation(h._opaque) & 1u) == 0) {
        return NULL;
    }
    uint32_t slot = moq_handle_slot(h._opaque);
    if (slot >= c->max_subs ||
        c->subs[slot].gen != moq_handle_generation(h._opaque)) {
        return NULL;
    }
    if (out_slot != NULL) {
        *out_slot = slot;
    }
    return &c->subs[slot];
}

/* -- intents ---------------------------------------------------------------------- */

static bool
intent_space(const moqr_core_t *c, uint32_t need)
{
    return c->intent_count + need <= c->max_intents;
}

static moqr_intent_t *
intent_push(moqr_core_t *c, moqr_intent_kind_t kind)
{
    moqr_intent_t *it =
        &c->intents[(c->intent_head + c->intent_count) % c->max_intents];
    c->intent_count++;
    if (c->intent_count > c->intent_hwm) {
        c->intent_hwm = c->intent_count;
    }
    memset(it, 0, sizeof(*it));
    it->kind = kind;
    return it;
}

/* Count an admission refusal against its resource and return the capacity
 * error. Centralizes the metric bump so every refusal path is observable. */
static moqr_result_t
r_refuse(moqr_core_t *c, moqr_refuse_reason_t reason)
{
    if (reason < MOQR_REFUSE__COUNT) {
        c->refusals[reason]++;
    }
    return MOQR_ERR_CAPACITY;
}

static void
intent_set_ns_from_key(moqr_intent_t *it, const r_key_t *key)
{
    uint32_t count = key_part_count(key);
    it->ns_count = count;
    for (uint32_t i = 0; i < count && i < 32; i++) {
        it->ns_parts[i] = key_part(key, i);
    }
    it->name = key_name(key);
}

size_t
moqr_core_poll_intents(moqr_core_t *c, moqr_intent_t *out, size_t cap)
{
    if (c == NULL || out == NULL || cap == 0) {
        return 0;
    }
    size_t n = c->intent_count < cap ? c->intent_count : cap;
    for (size_t i = 0; i < n; i++) {
        out[i] = c->intents[(c->intent_head + i) % c->max_intents];
    }
    c->intent_head = (uint32_t)((c->intent_head + n) % c->max_intents);
    c->intent_count -= (uint32_t)n;
    return n;
}

/* -- config / lifecycle -------------------------------------------------------------- */

#define RELAY_CFG_HAS(cfg, field)                        \
    (offsetof(moqr_core_relay_cfg_t, field) +            \
         sizeof(((moqr_core_relay_cfg_t *)0)->field) <= (cfg)->struct_size)

void
moqr_core_relay_cfg_init_sized(moqr_core_relay_cfg_t *cfg, size_t cfg_size,
                               const moq_alloc_t *alloc)
{
    if (cfg == NULL || cfg_size < sizeof(uint32_t)) {
        return;
    }
    size_t n = cfg_size < sizeof(moqr_core_relay_cfg_t)
                   ? cfg_size
                   : sizeof(moqr_core_relay_cfg_t);
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (offsetof(moqr_core_relay_cfg_t, alloc) +
            sizeof(((moqr_core_relay_cfg_t *)0)->alloc) <= n) {
        cfg->alloc = alloc;
    }
}

/* Whole 64-bit words for one ready bit per binding slot. Widened before the
 * +63 round-up: at max_bindings == UINT32_MAX a 32-bit add wraps and would
 * size the bitset at ZERO words while the capacity descriptor (64-bit
 * arithmetic) charges the true count. One helper feeds allocation,
 * destruction, draining, and the descriptor so they can never disagree. */
static uint32_t
ready_word_count(uint32_t max_bindings)
{
    return (uint32_t)(((uint64_t)max_bindings + 63u) >> 6);
}

moqr_result_t
moqr_core_create(const moqr_core_relay_cfg_t *cfg, moqr_core_t **out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    *out = NULL;
    if (cfg == NULL || cfg->struct_size < sizeof(uint32_t)) {
        return MOQR_ERR_INVAL;
    }
    const moq_alloc_t *a = RELAY_CFG_HAS(cfg, alloc) ? cfg->alloc : NULL;
    if (a == NULL || a->alloc == NULL || a->free == NULL) {
        return MOQR_ERR_INVAL;
    }

    /* Shard identity (single-shard default: index 0 / count 1). Validate before
     * allocating anything: index must be in range and the tag (index + 1)
     * representable in the 16-bit handle tag field. */
    uint32_t si = RELAY_CFG_HAS(cfg, shard_index) ? cfg->shard_index : 0;
    uint32_t sc = (RELAY_CFG_HAS(cfg, shard_count) && cfg->shard_count != 0)
                      ? cfg->shard_count
                      : 1;
    if (si >= sc || si + 1u > 0xFFFFu) {
        return MOQR_ERR_INVAL;
    }

    moqr_core_t *c = a->alloc(sizeof(*c), a->ctx);
    if (c == NULL) {
        return MOQR_ERR_NOMEM;
    }
    memset(c, 0, sizeof(*c));
    c->alloc = *a;
    c->trace = RELAY_CFG_HAS(cfg, trace) ? cfg->trace : NULL;
    c->shard_index = (uint16_t)si;
    c->shard_count = (uint16_t)sc;
    c->shard_tag = (uint16_t)(si + 1u);
    /* node/shard epochs stay 0 until a control input sets them (memset above). */

    /* Single-source resolution: create consumes the SAME pure resolver the
     * capacity describes use, so create and describe can never disagree. */
    moqr_core_limits_t lim;
    (void)moqr_core_limits_resolve(cfg, &lim);
    c->max_bindings = lim.max_bindings;
    c->max_tracks = lim.max_tracks;
    c->max_subs = lim.max_subs;
    c->max_ns_nodes = lim.max_ns_nodes;
    c->max_ns_subs = lim.max_ns_subs;
    c->max_intents = lim.max_intents;   /* atomicity clamp already folded */
    c->intern_budget = lim.name_intern_bytes;
    c->max_parked = lim.max_parked;
    c->parked_bytes_cap = lim.parked_bytes;
    c->max_grants = lim.max_grants;
    c->grant_bytes_cap = lim.grant_bytes;
    c->max_fetches = lim.max_fetches;
    c->max_cancels = lim.max_cancels;
    c->fetch_pin_cap = lim.fetch_pin_bytes;
    /* The intent-ring atomicity clamp is folded into
     * moqr_core_limits_resolve (single source with the capacity describes);
     * lim.max_intents above already carries it. */
    if (RELAY_CFG_HAS(cfg, log_budget)) {
        c->log_budget = cfg->log_budget;
    }
    c->log_max_subgroups =
        RELAY_CFG_HAS(cfg, log_max_subgroups) ? cfg->log_max_subgroups : 0;
    c->log_max_objects = RELAY_CFG_HAS(cfg, log_max_objects_per_group)
                             ? cfg->log_max_objects_per_group
                             : 0;
    c->log_max_cursors =
        RELAY_CFG_HAS(cfg, log_max_cursors) ? cfg->log_max_cursors : 0;
    c->log_max_chunk_nodes =
        RELAY_CFG_HAS(cfg, log_max_chunk_nodes) ? cfg->log_max_chunk_nodes : 0;
    /* gpos layout representability: lists = subgroups + the datagram list,
     * and the per-(sub, group) stride, must both fit the runtime's 32-bit
     * fields. Same formula the capacity describe checks. */
    {
        uint64_t lists =
            (uint64_t)(c->log_max_subgroups != 0 ? c->log_max_subgroups
                                                 : 16u) +
            1u;
        uint64_t groups = (uint64_t)(c->log_budget.max_groups != 0
                                         ? c->log_budget.max_groups
                                         : 8u);
        if (lists > UINT32_MAX || gpos_stride_for(lists) > UINT32_MAX ||
            gpos_hash_cap_for(groups) > UINT32_MAX) {
            moqr_core_destroy(c);
            return MOQR_ERR_INVAL;
        }
    }
    c->linger_us = RELAY_CFG_HAS(cfg, linger_us) ? cfg->linger_us : 0;
    c->authorize = RELAY_CFG_HAS(cfg, authorize) ? cfg->authorize : NULL;
    c->authorize_ctx =
        RELAY_CFG_HAS(cfg, authorize_ctx) ? cfg->authorize_ctx : NULL;

    /* Floor the fetch pin budget to the effective per-track log payload budget
     * (the largest a single retained record can be, log.c rejects bigger), so a
     * lone object always fits an idle budget: forward progress is guaranteed and
     * a fetch can never wait forever on the pin budget. */
    {
        moqr_log_cfg_t lc;
        moqr_log_cfg_init_sized(&lc, sizeof(lc), &c->alloc);
        lc.budget = c->log_budget;
        lc.max_subgroups_per_group = c->log_max_subgroups;
        lc.max_objects_per_group = c->log_max_objects;
        lc.max_cursors = c->log_max_cursors;
        lc.max_chunk_nodes = c->log_max_chunk_nodes;
        moqr_log_capacity_t logc;
        if (moqr_log_capacity_describe(&lc, &logc) != MOQR_OK) {
            /* Create/describe parity: a config whose per-track log model
             * wraps is refused HERE too — never a zero floor. */
            moqr_core_destroy(c);
            return MOQR_ERR_INVAL;
        }
        if (c->fetch_pin_cap < logc.payload_bytes) {
            c->fetch_pin_cap = logc.payload_bytes;
        }
    }

    /* Each pool is zeroed the moment it exists: a partial-failure destroy
     * must never walk uninitialized entries (pins, generations). */
#define R_POOL_ALLOC(field, count)                                        \
    do {                                                                  \
        c->field = r_alloc(c, (size_t)(count) * sizeof(*c->field));       \
        if (c->field != NULL) {                                           \
            memset(c->field, 0, (size_t)(count) * sizeof(*c->field));     \
        }                                                                 \
    } while (0)
    R_POOL_ALLOC(bindings, c->max_bindings);
    R_POOL_ALLOC(tracks, c->max_tracks);
    R_POOL_ALLOC(subs, c->max_subs);
    R_POOL_ALLOC(nodes, c->max_ns_nodes);
    R_POOL_ALLOC(ns_subs, c->max_ns_subs);
    R_POOL_ALLOC(intents, c->max_intents);
    R_POOL_ALLOC(parked, c->max_parked);
    R_POOL_ALLOC(grants, c->max_grants);
    R_POOL_ALLOC(fetches, c->max_fetches);
    R_POOL_ALLOC(pending_cancels, c->max_cancels);
#undef R_POOL_ALLOC
    /* Binding-ready bitset: ceil(max_bindings/64) words, zeroed (empty). */
    {
        size_t rw = (size_t)ready_word_count(c->max_bindings) *
                    sizeof(uint64_t);
        c->ready_words = r_alloc(c, rw);
        if (c->ready_words != NULL) {
            memset(c->ready_words, 0, rw);
        }
    }
    if (c->bindings == NULL || c->tracks == NULL || c->subs == NULL ||
        c->ready_words == NULL ||
        c->nodes == NULL || c->ns_subs == NULL || c->intents == NULL ||
        (c->max_parked != 0 && c->parked == NULL) ||
        (c->max_grants != 0 && c->grants == NULL) ||
        (c->max_cancels != 0 && c->pending_cancels == NULL) ||
        (c->max_fetches != 0 && c->fetches == NULL)) {
        moqr_core_destroy(c);
        return MOQR_ERR_NOMEM;
    }

    /* Root trie node. */
    c->nodes[0].parent = UINT32_MAX;
    c->node_count = 1;

    *out = c;
    return MOQR_OK;
}

static void track_free_slot(moqr_core_t *c, uint32_t slot);
static void binding_unpin(r_binding_t *b);
static void gpos_free(moqr_core_t *c, r_sub_t *s);
static void parked_free_slot(moqr_core_t *c, uint32_t slot);
static void grant_free_slot(moqr_core_t *c, uint32_t slot);
static void fetch_unpin(moqr_core_t *c, r_fetch_t *f);

void
moqr_core_destroy(moqr_core_t *c)
{
    if (c == NULL) {
        return;
    }
    moq_alloc_t a = c->alloc;
    if (c->tracks != NULL) {
        for (uint32_t i = 0; i < c->max_tracks; i++) {
            if ((c->tracks[i].gen & 1u) != 0) {
                track_free_slot(c, i);
            }
        }
        a.free(c->tracks, (size_t)c->max_tracks * sizeof(*c->tracks), a.ctx);
    }
    if (c->ready_words != NULL) {
        a.free(c->ready_words,
               (size_t)ready_word_count(c->max_bindings) * sizeof(uint64_t),
               a.ctx);
    }
    if (c->nodes != NULL) {
        for (uint32_t i = 0; i < c->node_count; i++) {
            r_free(c, c->nodes[i].part, c->nodes[i].part_len);
            r_free(c, c->nodes[i].children,
                   (size_t)c->nodes[i].child_cap * sizeof(uint32_t));
        }
        a.free(c->nodes, (size_t)c->max_ns_nodes * sizeof(*c->nodes), a.ctx);
    }
    if (c->ns_subs != NULL) {
        for (uint32_t i = 0; i < c->max_ns_subs; i++) {
            r_free(c, c->ns_subs[i].prefix, c->ns_subs[i].prefix_len);
        }
        a.free(c->ns_subs, (size_t)c->max_ns_subs * sizeof(*c->ns_subs),
               a.ctx);
    }
    if (c->bindings != NULL) {
        for (uint32_t i = 0; i < c->max_bindings; i++) {
            binding_unpin(&c->bindings[i]);
            r_free(c, c->bindings[i].pin_chunks,
                   (size_t)c->bindings[i].pin_chunks_cap *
                       sizeof(r_pin_chunk_t));
        }
        a.free(c->bindings, (size_t)c->max_bindings * sizeof(*c->bindings),
               a.ctx);
    }
    if (c->subs != NULL) {
        for (uint32_t i = 0; i < c->max_subs; i++) {
            if ((c->subs[i].gen & 1u) != 0) {
                gpos_free(c, &c->subs[i]);
            }
        }
        a.free(c->subs, (size_t)c->max_subs * sizeof(*c->subs), a.ctx);
    }
    if (c->intents != NULL) {
        a.free(c->intents, (size_t)c->max_intents * sizeof(*c->intents),
               a.ctx);
    }
    if (c->parked != NULL) {
        for (uint32_t i = 0; i < c->max_parked; i++) {
            if ((c->parked[i].gen & 1u) != 0) {
                parked_free_slot(c, i);
            }
        }
        a.free(c->parked, (size_t)c->max_parked * sizeof(*c->parked), a.ctx);
    }
    if (c->grants != NULL) {
        for (uint32_t i = 0; i < c->max_grants; i++) {
            if ((c->grants[i].gen & 1u) != 0) {
                grant_free_slot(c, i);
            }
        }
        a.free(c->grants, (size_t)c->max_grants * sizeof(*c->grants), a.ctx);
    }
    if (c->fetches != NULL) {
        for (uint32_t i = 0; i < c->max_fetches; i++) {
            if ((c->fetches[i].gen & 1u) != 0) {
                fetch_unpin(c, &c->fetches[i]);
            }
        }
        a.free(c->fetches, (size_t)c->max_fetches * sizeof(*c->fetches), a.ctx);
    }
    if (c->pending_cancels != NULL) {
        a.free(c->pending_cancels,
               (size_t)c->max_cancels * sizeof(*c->pending_cancels), a.ctx);
    }
    a.free(c, sizeof(*c), a.ctx);
}

/* -- key building -------------------------------------------------------------------- */

static moqr_result_t
key_build(moqr_core_t *c, moqr_ns_t ns, moq_bytes_t name, r_key_t *out)
{
    if (ns.count > 32) {
        return MOQR_ERR_INVAL;
    }
    uint32_t len = 4u * (2u + (uint32_t)ns.count);
    for (size_t i = 0; i < ns.count; i++) {
        if (ns.parts[i].len > MOQ_FULL_TRACK_NAME_MAX) {
            return MOQR_ERR_INVAL;
        }
        len += (uint32_t)ns.parts[i].len;
    }
    if (name.len > MOQ_FULL_TRACK_NAME_MAX) {
        return MOQR_ERR_INVAL;
    }
    len += (uint32_t)name.len;
    if (c->intern_used + len > c->intern_budget) {
        return r_refuse(c, MOQR_REFUSE_NAME_BYTES);
    }

    uint8_t *buf = r_alloc(c, len);
    if (buf == NULL) {
        return MOQR_ERR_NOMEM;
    }
    key_wr32(buf, (uint32_t)ns.count);
    uint32_t off = 4u * (2u + (uint32_t)ns.count);
    for (size_t i = 0; i < ns.count; i++) {
        key_wr32(buf + 4u * (1u + i), (uint32_t)ns.parts[i].len);
        if (ns.parts[i].len != 0) {
            memcpy(buf + off, ns.parts[i].data, ns.parts[i].len);
        }
        off += (uint32_t)ns.parts[i].len;
    }
    key_wr32(buf + 4u * (1u + ns.count), (uint32_t)name.len);
    if (name.len != 0) {
        memcpy(buf + off, name.data, name.len);
    }
    out->buf = buf;
    out->len = len;
    c->intern_used += len;
    return MOQR_OK;
}

static void
key_release(moqr_core_t *c, r_key_t *k)
{
    if (k->buf != NULL) {
        c->intern_used -= k->len;
        r_free(c, k->buf, k->len);
        k->buf = NULL;
        k->len = 0;
    }
}

/* -- trie ----------------------------------------------------------------------------- */

static uint32_t
trie_find_child(const moqr_core_t *c, uint32_t node, moq_bytes_t part)
{
    const r_trie_node_t *n = &c->nodes[node];
    for (uint32_t i = 0; i < n->child_count; i++) {
        const r_trie_node_t *ch = &c->nodes[n->children[i]];
        if (ch->part_len == part.len &&
            (part.len == 0 || memcmp(ch->part, part.data, part.len) == 0)) {
            return n->children[i];
        }
    }
    return UINT32_MAX;
}

static moqr_result_t
trie_child_get_or_create(moqr_core_t *c, uint32_t node, moq_bytes_t part,
                         uint32_t *out)
{
    uint32_t found = trie_find_child(c, node, part);
    if (found != UINT32_MAX) {
        *out = found;
        return MOQR_OK;
    }
    if (c->node_count == c->max_ns_nodes) {
        return r_refuse(c, MOQR_REFUSE_NS_NODES);
    }
    if (c->intern_used + part.len > c->intern_budget) {
        return r_refuse(c, MOQR_REFUSE_NAME_BYTES);
    }
    r_trie_node_t *parent = &c->nodes[node];
    if (parent->child_count == parent->child_cap) {
        uint32_t newcap = parent->child_cap == 0 ? 4 : parent->child_cap * 2;
        uint32_t *nc = r_alloc(c, (size_t)newcap * sizeof(uint32_t));
        if (nc == NULL) {
            return MOQR_ERR_NOMEM;
        }
        if (parent->child_count != 0) {
            memcpy(nc, parent->children,
                   (size_t)parent->child_count * sizeof(uint32_t));
        }
        r_free(c, parent->children,
               (size_t)parent->child_cap * sizeof(uint32_t));
        parent->children = nc;
        parent->child_cap = newcap;
    }
    uint8_t *copy = NULL;
    if (part.len != 0) {
        copy = r_alloc(c, part.len);
        if (copy == NULL) {
            return MOQR_ERR_NOMEM;
        }
        memcpy(copy, part.data, part.len);
    }
    uint32_t idx = c->node_count++;
    r_trie_node_t *nn = &c->nodes[idx];
    memset(nn, 0, sizeof(*nn));
    nn->part = copy;
    nn->part_len = (uint32_t)part.len;
    nn->parent = node;
    c->intern_used += (uint32_t)part.len;
    c->nodes[node].children[c->nodes[node].child_count++] = idx;
    *out = idx;
    return MOQR_OK;
}

/* Walk ns; return node index or UINT32_MAX (no create). */
static uint32_t
trie_walk(const moqr_core_t *c, moqr_ns_t ns)
{
    uint32_t node = 0;
    for (size_t i = 0; i < ns.count; i++) {
        node = trie_find_child(c, node, ns.parts[i]);
        if (node == UINT32_MAX) {
            return UINT32_MAX;
        }
    }
    return node;
}

/* Longest prefix of `ns` with a live announce; UINT32_MAX if none. */
static uint32_t
trie_longest_announce(const moqr_core_t *c, moqr_ns_t ns)
{
    uint32_t best = UINT32_MAX;
    uint32_t node = 0;
    if (c->nodes[0].has_announce) {
        best = 0;
    }
    for (size_t i = 0; i < ns.count; i++) {
        node = trie_find_child(c, node, ns.parts[i]);
        if (node == UINT32_MAX) {
            break;
        }
        if (c->nodes[node].has_announce) {
            best = node;
        }
    }
    return best;
}

/* Depth of a node (number of parts from root). */
static uint32_t
node_depth(const moqr_core_t *c, uint32_t node)
{
    uint32_t d = 0;
    while (c->nodes[node].parent != UINT32_MAX) {
        d++;
        node = c->nodes[node].parent;
    }
    return d;
}

/* Fill intent ns views from a trie node's root path. */
static void
intent_set_ns_from_node(const moqr_core_t *c, moqr_intent_t *it,
                        uint32_t node)
{
    uint32_t depth = node_depth(c, node);
    it->ns_count = depth;
    uint32_t n = node;
    for (uint32_t i = depth; i > 0; i--) {
        it->ns_parts[i - 1] = (moq_bytes_t){ .data = c->nodes[n].part,
                                             .len = c->nodes[n].part_len };
        n = c->nodes[n].parent;
    }
}

/* Does `node`'s path start with the ns-sub prefix? prefix buffer layout is
 * a key without name lens... stored as: u32 count | u32 lens | bytes. */
static bool
prefix_matches_node(const moqr_core_t *c, const r_ns_sub_t *nsub,
                    uint32_t node)
{
    uint32_t pcount = key_rd32(nsub->prefix);
    uint32_t depth = node_depth(c, node);
    if (depth < pcount) {
        return false;
    }
    /* Collect the first pcount parts of node's path. */
    uint32_t chain[32];
    uint32_t n = node, d = depth;
    while (d > 0) {
        chain[d - 1] = n;
        n = c->nodes[n].parent;
        d--;
    }
    uint32_t off = 4u * (1u + pcount);
    for (uint32_t i = 0; i < pcount; i++) {
        uint32_t plen = key_rd32(nsub->prefix + 4u * (1u + i));
        const r_trie_node_t *pn = &c->nodes[chain[i]];
        if (pn->part_len != plen ||
            (plen != 0 && memcmp(pn->part, nsub->prefix + off, plen) != 0)) {
            return false;
        }
        off += plen;
    }
    return true;
}

/* -- tracks ------------------------------------------------------------------------------ */

static uint32_t
track_find(const moqr_core_t *c, const r_key_t *key, uint64_t hash)
{
    for (uint32_t i = 0; i < c->max_tracks; i++) {
        const r_track_t *t = &c->tracks[i];
        if ((t->gen & 1u) != 0 && t->hash == hash && key_eq(&t->key, key)) {
            return i;
        }
    }
    return UINT32_MAX;
}

static void
track_free_slot(moqr_core_t *c, uint32_t slot)
{
    r_track_t *t = &c->tracks[slot];
    /* Fold this log's evictions into the monotonic accumulator before the
     * log (and its counters) vanish, so objects_evicted_total never drops. */
    moqr_log_stats_t ls;
    moqr_log_get_stats(t->log, &ls);
    c->evicted_freed += ls.evicted_records_total;
    moqr_log_destroy(t->log);
    t->log = NULL;
    key_release(c, &t->key);
    /* The gen bump below ends every remaining member's track membership at
     * once; clear their index links now so a later retire of a stale member
     * never unlinks through a dead track's list. */
    for (uint32_t i = t->subs_head; i != R_SUB_NIL;) {
        uint32_t next = c->subs[i].track_next;
        c->subs[i].track_next = R_SUB_NIL;
        c->subs[i].track_prev = R_SUB_NIL;
        i = next;
    }
    t->subs_head = R_SUB_NIL;
    t->gen++;   /* odd -> even */
    r_bump_route_epoch(c);
    r_trace(c, MOQR_TRACE_ROUTE_REMOVE, 1 /* track */, slot, 0, 0, 0);
}

static moqr_result_t
track_make_log(moqr_core_t *c, uint32_t slot, moqr_log_t **out)
{
    moqr_log_cfg_t lc;
    moqr_log_cfg_init_sized(&lc, sizeof(lc), &c->alloc);
    lc.budget = c->log_budget;
    lc.max_subgroups_per_group = c->log_max_subgroups;
    lc.max_objects_per_group = c->log_max_objects;
    lc.max_cursors = c->log_max_cursors;
    lc.max_chunk_nodes = c->log_max_chunk_nodes;
    lc.trace = c->trace;
    lc.trace_id = slot;
    return moqr_log_create(&lc, out);
}

static void
track_fetch_invalidate(moqr_core_t *c, uint32_t track_slot,
                       uint32_t track_gen)
{
    for (uint32_t fi = 0; fi < c->max_fetches; fi++) {
        r_fetch_t *f = &c->fetches[fi];
        if ((f->gen & 1u) != 0 && f->track == track_slot &&
            f->track_gen_slot == track_gen) {
            fetch_unpin(c, f);
            f->gen++;   /* odd -> even */
        }
    }
}

static void
track_replace_retained_generation(moqr_core_t *c, uint32_t slot,
                                  moqr_log_t *new_log)
{
    r_track_t *t = &c->tracks[slot];
    uint32_t old_gen = t->gen;

    track_fetch_invalidate(c, slot, old_gen);

    moqr_log_stats_t ls;
    moqr_log_get_stats(t->log, &ls);
    c->evicted_freed += ls.evicted_records_total;
    moqr_log_destroy(t->log);
    t->log = new_log;
    t->has_largest = false;
    t->largest_group = 0;
    t->largest_object = 0;
    t->linger_deadline_us = 0;
    t->gen += 2;   /* odd -> odd: keep slot, invalidate old readers/handles */
    r_bump_route_epoch(c);
}

static moqr_result_t
track_create(moqr_core_t *c, r_key_t *key /* adopted on OK */, uint64_t hash,
             uint32_t *out_slot)
{
    uint32_t slot = UINT32_MAX;
    for (uint32_t i = 0; i < c->max_tracks; i++) {
        if ((c->tracks[i].gen & 1u) == 0) {
            slot = i;
            break;
        }
    }
    if (slot == UINT32_MAX) {
        return r_refuse(c, MOQR_REFUSE_TRACKS);
    }
    r_track_t *t = &c->tracks[slot];

    moqr_log_t *log = NULL;
    moqr_result_t rc = track_make_log(c, slot, &log);
    if (rc != MOQR_OK) {
        return rc;
    }

    uint32_t gen = t->gen + 1;   /* even -> odd */
    memset(t, 0, sizeof(*t));
    t->gen = gen;
    t->subs_head = R_SUB_NIL;
    t->key = *key;
    key->buf = NULL;   /* adopted */
    t->hash = hash;
    t->log = log;
    t->track_gen = 0;
    t->src_ann_node = UINT32_MAX;   /* 0 is the valid root node, so set explicitly */
    *out_slot = slot;
    r_bump_route_epoch(c);
    r_trace(c, MOQR_TRACE_ROUTE_ADD, 1 /* track */, slot, hash, 0, 0);
    return MOQR_OK;
}

static moqr_track_t
track_handle(const moqr_core_t *c, uint32_t slot)
{
    return (moqr_track_t){ r_pack(c, MOQR_HANDLE_POOL_TRACK,
                                  c->tracks[slot].gen, slot) };
}

static moqr_sub_t
sub_handle(const moqr_core_t *c, uint32_t slot)
{
    return (moqr_sub_t){ r_pack(c, MOQR_HANDLE_POOL_SUB, c->subs[slot].gen,
                                slot) };
}

/* Seal-acknowledgement bit words per entry: one bit per position list. */
static uint32_t
gpos_acked_words(const r_sub_t *s)
{
    return (s->gpos_lists + 31u) / 32u;
}

/* Per-(sub, group) position-entry stride for `lists` position lists:
 * group_id + idx[] + emitted[] + seal-acked bit words, rounded up to
 * uint64_t alignment so entries beyond the first keep group_id naturally
 * aligned. Shared by the runtime and moqr_core_capacity_describe — the
 * advertised ceiling must never drift from the allocation. */
static uint64_t
gpos_stride_for(uint64_t lists)
{
    /* Computed WIDE; callers reject a stride the runtime's 32-bit layout
     * cannot represent (create and describe share this single formula). */
    uint64_t raw = (uint64_t)sizeof(uint64_t) + 8u * lists +
                   4u * ((lists + 31u) / 32u);
    return (raw + 7u) & ~(uint64_t)7u;   /* 64-bit mask: ~7u would truncate */
}

/* The per-sub gpos hash index: a power of two >= 2x the group entries (load
 * factor <= 0.5), computed WIDE. Shared by gpos_alloc, create's gate, and
 * the capacity describe; callers reject a result above UINT32_MAX. */
static uint64_t
gpos_hash_cap_for(uint64_t groups)
{
    uint64_t cap = 8u;
    while (cap < groups * 2u) {
        cap <<= 1u;
    }
    return cap;
}

static uint32_t
gpos_stride(const r_sub_t *s)
{
    /* Create rejected any layout whose wide stride exceeds UINT32_MAX. */
    return (uint32_t)gpos_stride_for(s->gpos_lists);
}

static r_gpos_t *
gpos_at(const r_sub_t *s, uint32_t i)
{
    return (r_gpos_t *)(s->gpos + (size_t)i * gpos_stride(s));
}

/* The live-edge emitted-chunk watermark array (gpos_lists entries) follows
 * idx[] in the same block. Each word packs a "begun downstream" flag in the top
 * bit with the exact emitted chunk count in the low bits: a begin_object that
 * shipped the object header but zero chunks (then blocked) is still recorded as
 * begun — its downstream object needs a RESET if the source abandons or the
 * group is evicted — while the count remains 0 so re-derivation re-sends from
 * chunk 0 (never skipping it). A position is "begun" iff the whole word != 0, so
 * the existing >0 / ==0 begun tests keep working; the count is masked out only
 * where the value is used as a chunk index (pin base, live-edge compare). */
#define GPOS_BEGUN_BIT 0x80000000u
#define GPOS_EMIT_MASK 0x7fffffffu
static uint32_t *
gpos_emit(const r_sub_t *s, r_gpos_t *e)
{
    return &e->idx[s->gpos_lists];
}

/* Seal-acknowledgement bit words follow emitted[] in the same block. A set
 * bit = this sub already acknowledged the (group, list) seal via its SEAL
 * notice (the only durable FIN path — records carry subgroup_end as
 * advisory metadata only), so it is never notified twice. */
static uint32_t *
gpos_acked(const r_sub_t *s, r_gpos_t *e)
{
    return &e->idx[2u * s->gpos_lists];
}

static bool
gpos_seal_acked(const r_sub_t *s, r_gpos_t *e, uint32_t list)
{
    return ((gpos_acked(s, e)[list / 32u] >> (list % 32u)) & 1u) != 0;
}

static void
gpos_seal_ack(const r_sub_t *s, r_gpos_t *e, uint32_t list)
{
    gpos_acked(s, e)[list / 32u] |= 1u << (list % 32u);
}

/* Zero an entry's per-list body — idx[], emitted[], and the seal-acked
 * words: a claimed or reused slot must never inherit stale positions or a
 * stale seal acknowledgement. */
static void
gpos_body_clear(const r_sub_t *s, r_gpos_t *e)
{
    memset(e->idx, 0, 8u * (size_t)s->gpos_lists +
                          4u * (size_t)gpos_acked_words(s));
}

/* -- Per-sub group_id -> gpos-slot index ---------------------------------------- *
 * The gpos entries stay at fixed array slots (pointer-stable — callers hold
 * r_gpos_t*), keyed by a stable group_id (log group ids are monotonic and never
 * reused). A small open-addressing index maps group_id -> slot for O(1) lookup,
 * replacing the former O(max_groups) linear scan in gpos_entry / delivery_done.
 * Eviction reclaim keeps the exact prior rule but is gated (gpos_gc) on the log's
 * monotonic evicted_groups_total, so it runs only when the log actually evicted;
 * a begun-evicted entry (emitted > 0) is KEPT until its evict-reset is confirmed
 * (the begun-evicted-reset guarantee). */

static uint32_t
gpos_hash0(uint64_t group_id, uint32_t cap)
{
    /* Fibonacci hash — the top bits of the product are well mixed. */
    return (uint32_t)((group_id * 0x9E3779B97F4A7C15ull) >> 48) & (cap - 1u);
}

/* Array slot for group_id, or R_GPOS_NIL. O(1) average. */
static uint32_t
gpos_hash_find(const r_sub_t *s, uint64_t group_id)
{
    if (s->gpos_hash == NULL) {
        return R_GPOS_NIL;
    }
    uint32_t cap = s->gpos_hash_cap;
    uint32_t i = gpos_hash0(group_id, cap);
    for (uint32_t probe = 0; probe < cap; probe++) {
        uint32_t slot = s->gpos_hash[i];
        if (slot == R_GPOS_NIL) {
            return R_GPOS_NIL;   /* empty run: not present */
        }
        if (gpos_at(s, slot)->group_id == group_id) {
            return slot;
        }
        i = (i + 1u) & (cap - 1u);
    }
    return R_GPOS_NIL;
}

/* Insert group_id -> slot; the key must not already be present. */
static void
gpos_hash_insert(r_sub_t *s, uint64_t group_id, uint32_t slot)
{
    uint32_t cap = s->gpos_hash_cap;
    uint32_t i = gpos_hash0(group_id, cap);
    while (s->gpos_hash[i] != R_GPOS_NIL) {
        i = (i + 1u) & (cap - 1u);
    }
    s->gpos_hash[i] = slot;
}

/* Rebuild the index from the live array entries (drops mappings left stale by a
 * reclaim). O(gpos_groups); only on reclaim, never on the delivery path. */
static void
gpos_hash_rebuild(r_sub_t *s)
{
    if (s->gpos_hash == NULL) {
        return;
    }
    for (uint32_t i = 0; i < s->gpos_hash_cap; i++) {
        s->gpos_hash[i] = R_GPOS_NIL;
    }
    for (uint32_t i = 0; i < s->gpos_groups; i++) {
        if (gpos_at(s, i)->group_id != UINT64_MAX) {
            gpos_hash_insert(s, gpos_at(s, i)->group_id, i);
        }
    }
}

/* -- Binding-ready set (produced here, drained by the binding layer) ----------
 *
 * ready_mark records that a binding MAY now have deliverable work; a set bit
 * re-marked is a no-op (dedup by construction) and the count tracks set bits
 * exactly. Pseudo-bindings (shard pump subs, cookie >= MOQR_SHARD_COOKIE_BASE)
 * are filtered at production: they never enter the set, never consume drain
 * capacity, and the shard data phase keeps its own polling. A spurious mark is
 * legal (it costs the consumer one empty probe); a MISSED readiness edge is
 * not — every core-owned transition that can make a binding deliverable must
 * mark. Nothing in the core consumes the set. */
static void
ready_mark(moqr_core_t *c, uint32_t bslot)
{
    const r_binding_t *b = &c->bindings[bslot];
    if ((b->gen & 1u) == 0 || b->cookie >= MOQR_SHARD_COOKIE_BASE) {
        return;
    }
    uint64_t *w = &c->ready_words[bslot >> 6];
    uint64_t bit = 1ull << (bslot & 63u);
    if ((*w & bit) == 0) {
        *w |= bit;
        c->ready_count++;
    }
}

static void
ready_clear(moqr_core_t *c, uint32_t bslot)
{
    uint64_t *w = &c->ready_words[bslot >> 6];
    uint64_t bit = 1ull << (bslot & 63u);
    if ((*w & bit) != 0) {
        *w &= ~bit;
        c->ready_count--;
    }
}

/* Track-wide readiness (ingest family, seal): mark every ACTIVE subscriber's
 * binding through the per-track index. PARKED subs are not deliverable and
 * are marked at activation instead. */
static void
ready_mark_track(moqr_core_t *c, uint32_t tslot)
{
    if ((c->tracks[tslot].gen & 1u) == 0) {
        return;
    }
    for (uint32_t i = c->tracks[tslot].subs_head; i != R_SUB_NIL;
         i = c->subs[i].track_next) {
        if (c->subs[i].state == R_SUB_ACTIVE) {
            ready_mark(c, c->subs[i].binding);
        }
    }
}

/* Gated eviction reclaim (was inline in the former gpos_entry scan). Runs only
 * when the log evicted since this sub last swept. Reclaims entries whose group
 * is gone AND that the sub never began (emitted == 0), marking the skip; a
 * begun-evicted entry is kept so next_delivery's sweep can surface its reset
 * before reclaim. Rebuilds the index iff anything was reclaimed. */
static void
gpos_gc(moqr_core_t *c, r_sub_t *s, const moqr_log_t *log)
{
    if (s->gpos == NULL) {
        return;
    }
    uint64_t evicted = moqr_log_evicted_groups_total(log);
    if (evicted <= s->gpos_gc_evicted) {
        return;   /* nothing evicted since last sweep */
    }
    s->gpos_gc_evicted = evicted;
    bool reclaimed = false;
    for (uint32_t i = 0; i < s->gpos_groups; i++) {
        r_gpos_t *e = gpos_at(s, i);
        if (e->group_id == UINT64_MAX) {
            continue;
        }
        if (moqr_log_group_list_count(log, e->group_id) != 0) {
            continue;   /* still retained */
        }
        moqr_record_view_t probe;
        if (moqr_log_read_rec(log, e->group_id, MOQR_LOG_LIST_DATAGRAM, 0,
                              &probe) != MOQR_ERR_TOO_OLD) {
            continue;   /* present-but-empty, not evicted */
        }
        bool begun = false;
        for (uint32_t li = 0; li < s->gpos_lists; li++) {
            if (gpos_emit(s, e)[li] > 0) {
                begun = true;
                break;
            }
        }
        if (!begun) {
            e->group_id = UINT64_MAX;   /* reclaim */
            s->pending_skip = true;
            ready_mark(c, s->binding);   /* the skip must be reported */
            reclaimed = true;
        }
        /* begun: keep the slot until its evict-reset is confirmed (belt-and-
         * suspenders — next_delivery's evict-reset sweep runs before this and
         * surfaces the reset first; preserved from the earlier reclaim-in-place
         * behavior). */
    }
    if (reclaimed) {
        gpos_hash_rebuild(s);
    }
}

static moqr_result_t
gpos_alloc(moqr_core_t *c, r_sub_t *s, const moqr_log_t *log)
{
    s->gpos_groups = moqr_log_max_groups(log);
    s->gpos_lists = moqr_log_max_subgroups(log) + 1u;   /* + datagram list */
    size_t bytes = (size_t)s->gpos_groups * gpos_stride(s);
    s->gpos = r_alloc(c, bytes);
    if (s->gpos == NULL) {
        return MOQR_ERR_NOMEM;
    }
    for (uint32_t i = 0; i < s->gpos_groups; i++) {
        gpos_at(s, i)->group_id = UINT64_MAX;
    }
    /* Wide shared resolver; create rejected any cap above UINT32_MAX. */
    uint32_t cap = (uint32_t)gpos_hash_cap_for(s->gpos_groups);
    s->gpos_hash_cap = cap;
    s->gpos_hash = r_alloc(c, (size_t)cap * sizeof(uint32_t));
    if (s->gpos_hash == NULL) {
        r_free(c, s->gpos, bytes);
        s->gpos = NULL;
        return MOQR_ERR_NOMEM;
    }
    for (uint32_t i = 0; i < cap; i++) {
        s->gpos_hash[i] = R_GPOS_NIL;
    }
    /* Start even with the log's eviction watermark: this sub has no entries yet,
     * so there is nothing to reclaim for past evictions. */
    s->gpos_gc_evicted = moqr_log_evicted_groups_total(log);
    return MOQR_OK;
}

static void
gpos_free(moqr_core_t *c, r_sub_t *s)
{
    if (s->gpos != NULL) {
        r_free(c, s->gpos, (size_t)s->gpos_groups * gpos_stride(s));
        s->gpos = NULL;
    }
    if (s->gpos_hash != NULL) {
        r_free(c, s->gpos_hash, (size_t)s->gpos_hash_cap * sizeof(uint32_t));
        s->gpos_hash = NULL;
    }
}

/* O(1) lookup of this sub's positions entry for a group, creating it on first
 * access. Eviction reclaim is handled by gpos_gc (run once per
 * sub_best_candidate), so this is pure find-or-allocate — no per-call scan. */
static r_gpos_t *
gpos_entry(moqr_core_t *c, r_sub_t *s, const moqr_log_t *log,
           uint64_t group_id)
{
    (void)c;
    (void)log;
    uint32_t slot = gpos_hash_find(s, group_id);
    if (slot != R_GPOS_NIL) {
        return gpos_at(s, slot);
    }
    /* First access to this group: claim a free array slot. gpos_gc has already
     * reclaimed evicted (non-begun) entries, and retained groups never exceed the
     * gpos capacity, so a free slot exists. */
    for (uint32_t i = 0; i < s->gpos_groups; i++) {
        r_gpos_t *e = gpos_at(s, i);
        if (e->group_id == UINT64_MAX) {
            e->group_id = group_id;
            gpos_body_clear(s, e);
            gpos_hash_insert(s, group_id, i);
            return e;
        }
    }
    return NULL;   /* cannot happen: slots >= retained group cap */
}

/* Count live subs bound to a track slot. */
static uint32_t
track_sub_count(const moqr_core_t *c, uint32_t track_slot)
{
    if ((c->tracks[track_slot].gen & 1u) == 0) {
        return 0;   /* dead track: no live sub can match an even gen */
    }
    uint32_t n = 0;
    for (uint32_t i = c->tracks[track_slot].subs_head; i != R_SUB_NIL;
         i = c->subs[i].track_next) {
        n++;
    }
    return n;
}

/* Live fetch cursors reading this track. A fetch is a track READER: while one is
 * open the track must not be age-evicted or freed (else its accepted, retained
 * range could vanish mid-fetch — a silent truncation). Cleanup resumes once the
 * fetch closes. */
static uint32_t
track_fetch_count(const moqr_core_t *c, uint32_t track_slot)
{
    uint32_t n = 0;
    for (uint32_t i = 0; i < c->max_fetches; i++) {
        if ((c->fetches[i].gen & 1u) != 0 &&
            c->fetches[i].track == track_slot &&
            c->fetches[i].track_gen_slot == c->tracks[track_slot].gen) {
            n++;
        }
    }
    return n;
}

/* Resolve a sub's filter start/end from a largest location. */
static void
sub_resolve_filter(r_sub_t *s, const moqr_filter_t *f, bool has_largest,
                   uint64_t lg, uint64_t lo)
{
    s->filter_type = f->type;
    s->has_end = false;
    switch (f->type) {
    case MOQR_FILTER_LARGEST_OBJECT:
        if (has_largest) {
            s->start_group = lg;
            s->start_object = lo + 1;
        } else {
            s->start_group = 0;
            s->start_object = 0;
        }
        break;
    case MOQR_FILTER_NEXT_GROUP_START:
        if (has_largest) {
            s->start_group = lg + 1;
            s->start_object = 0;
        } else {
            s->start_group = 0;
            s->start_object = 0;
        }
        break;
    case MOQR_FILTER_ABSOLUTE_RANGE:
        s->has_end = true;
        s->end_group = f->start_group + f->end_group_delta;
        /* fall through */
    case MOQR_FILTER_ABSOLUTE_START:
    default:
        s->start_group = f->start_group;
        s->start_object = f->start_object;
        break;
    }
}

/* -- ops: bindings -------------------------------------------------------------------------- */

moqr_result_t
moqr_core_binding_open(moqr_core_t *c, uint64_t session_cookie,
                       moqr_binding_t *out)
{
    if (c == NULL || out == NULL) {
        return MOQR_ERR_INVAL;
    }
    *out = MOQR_BINDING_INVALID;
    for (uint32_t i = 0; i < c->max_bindings; i++) {
        if ((c->bindings[i].gen & 1u) == 0) {
            r_binding_t *b = &c->bindings[i];
            uint32_t gen = b->gen + 1;
            memset(b, 0, sizeof(*b));
            b->gen = gen;
            b->cookie = session_cookie;
            b->subs_head = R_SUB_NIL;   /* empty (memset left it 0) */
            out->_opaque = r_pack(c, MOQR_HANDLE_POOL_BINDING, gen, i);
            r_trace(c, MOQR_TRACE_ROUTE_ADD, 0 /* binding */, i,
                    session_cookie, 0, 0);
            return MOQR_OK;
        }
    }
    return r_refuse(c, MOQR_REFUSE_BINDINGS);
}

static void
binding_unpin(r_binding_t *b)
{
    moq_rcbuf_decref(b->pin_payload);
    moq_rcbuf_decref(b->pin_properties);
    b->pin_payload = NULL;
    b->pin_properties = NULL;
    for (uint32_t i = 0; i < b->pin_chunk_count; i++) {
        moq_rcbuf_decref(b->pin_chunks[i].buf);
        b->pin_chunks[i].buf = NULL;
    }
    b->pin_chunk_count = 0;
    b->out_chunked = false;
    b->out_abandoned = false;
    b->out_evict_reset = false;
    b->out_chunk_base = 0;
}

/* Drop a HELD outstanding delivery without advancing any cursor: releases the
 * binding's one delivery slot + its pins and clears the notice, leaving the
 * sub's positions untouched so a later pass re-derives the same work. Used
 * when the held delivery's own subscription has just paused (Forward=0) — the
 * binding must not stay monopolized by a delivery that can no longer be
 * written, and its siblings must keep flowing; on resume the release re-peeks
 * from the unadvanced cursor, so exactly-once is preserved. */
static void
binding_drop_held(r_binding_t *b)
{
    b->out_active = false;
    b->out_notice = MOQR_DELIVERY_NOTICE_NONE;
    binding_unpin(b);
}

/* Pin every chunk of a chunked record for the outstanding delivery: incref the
 * zero-copy slices into the binding's reusable pin array so the object survives
 * same-track eviction across a multi-pump delivery. The array grows lazily to
 * chunk_count (a record can hold no more chunks than the log's chunk-node pool,
 * so the growth is bounded — see moqr_core_capacity_describe) and is reused.
 * Returns MOQR_ERR_CAPACITY if the array cannot be grown (OOM: the caller fails
 * the delivery closed and retries, never delivering a truncatable object). */
static moqr_result_t
binding_pin_chunks(moqr_core_t *c, r_binding_t *b, const moqr_log_t *log,
                   const moqr_record_view_t *v, uint32_t base)
{
    /* Pin only the undelivered batch [base, chunk_count): base is the sub's
     * emitted watermark, so already-scheduled chunks (0..base-1) are not
     * re-pinned. moqr_core_delivery_chunk maps an absolute index back through
     * out_chunk_base == base. */
    uint32_t n = v->chunk_count > base ? v->chunk_count - base : 0;
    if (n > b->pin_chunks_cap) {
        r_pin_chunk_t *na = r_alloc(c, (size_t)n * sizeof(r_pin_chunk_t));
        if (na == NULL) {
            return MOQR_ERR_CAPACITY;
        }
        r_free(c, b->pin_chunks,
               (size_t)b->pin_chunks_cap * sizeof(r_pin_chunk_t));
        b->pin_chunks = na;
        b->pin_chunks_cap = n;
    }
    for (uint32_t i = 0; i < n; i++) {
        const moq_rcbuf_t *cb = NULL;
        uint64_t clen = 0;
        if (moqr_log_view_chunk(log, v, base + i, &cb, &clen) != MOQR_OK) {
            for (uint32_t j = 0; j < i; j++) {
                moq_rcbuf_decref(b->pin_chunks[j].buf);
                b->pin_chunks[j].buf = NULL;
            }
            return MOQR_ERR_INTERNAL;   /* base+i < chunk_count: never expected */
        }
        b->pin_chunks[i].buf = moq_rcbuf_incref((moq_rcbuf_t *)(void *)cb);
        b->pin_chunks[i].len = clen;
    }
    b->pin_chunk_count = n;
    return MOQR_OK;
}

/* Per-binding subscription list: an intrusive doubly-linked list of
 * a binding's live subscriptions, threaded through r_sub_t.sub_next/sub_prev and
 * headed at r_binding_t.subs_head. Pushed when a subscription is fully created,
 * removed in sub_retire — so moqr_core_next_delivery walks a binding's own subs
 * directly instead of scanning all c->max_subs. */
static void
sub_list_push(moqr_core_t *c, uint32_t bslot, uint32_t sslot)
{
    r_binding_t *b = &c->bindings[bslot];
    r_sub_t *s = &c->subs[sslot];
    s->sub_prev = R_SUB_NIL;
    s->sub_next = b->subs_head;
    if (b->subs_head != R_SUB_NIL) {
        c->subs[b->subs_head].sub_prev = sslot;
    }
    b->subs_head = sslot;
}

static void
sub_list_remove(moqr_core_t *c, uint32_t bslot, uint32_t sslot)
{
    r_binding_t *b = &c->bindings[bslot];
    r_sub_t *s = &c->subs[sslot];
    if (s->sub_prev != R_SUB_NIL) {
        c->subs[s->sub_prev].sub_next = s->sub_next;
    } else if (b->subs_head == sslot) {
        b->subs_head = s->sub_next;   /* this sub was the head */
    }
    if (s->sub_next != R_SUB_NIL) {
        c->subs[s->sub_next].sub_prev = s->sub_prev;
    }
    s->sub_next = R_SUB_NIL;
    s->sub_prev = R_SUB_NIL;
}

/* Per-track subscriber index: an intrusive doubly-linked list over the sub
 * pool holding exactly the subs track_sub_count's membership predicate
 * accepts (live, bound to this track slot, generation-matched). Linked when
 * a subscription is created against a live track, unlinked at retire while
 * the track generation still matches; track_free_slot clears the whole list
 * before the track's gen bump. Link fields of non-members are meaningless
 * and never read (every walk starts at a live track's subs_head). */
static void
track_sub_link(moqr_core_t *c, uint32_t tslot, uint32_t sslot)
{
    r_track_t *t = &c->tracks[tslot];
    r_sub_t *s = &c->subs[sslot];
    s->track_prev = R_SUB_NIL;
    s->track_next = t->subs_head;
    if (t->subs_head != R_SUB_NIL) {
        c->subs[t->subs_head].track_prev = sslot;
    }
    t->subs_head = sslot;
}

static void
track_sub_unlink(moqr_core_t *c, uint32_t tslot, uint32_t sslot)
{
    r_track_t *t = &c->tracks[tslot];
    r_sub_t *s = &c->subs[sslot];
    if (s->track_prev != R_SUB_NIL) {
        c->subs[s->track_prev].track_next = s->track_next;
    } else if (t->subs_head == sslot) {
        t->subs_head = s->track_next;   /* this sub was the head */
    }
    if (s->track_next != R_SUB_NIL) {
        c->subs[s->track_next].track_prev = s->track_prev;
    }
    s->track_next = R_SUB_NIL;
    s->track_prev = R_SUB_NIL;
}

/* Begun live-edge accounting: a (group,list) position transitions to begun
 * (emitted 0 -> >0) or unbegun (>0 -> 0). Maintained so the evict-reset sweep
 * can skip a binding with no begun objects. The per-binding begun_subs tracks
 * how many of the binding's subs currently have any begun position, so the
 * sweep is an O(1) check when nothing is begun. */
static void
sub_begun_add(moqr_core_t *c, r_sub_t *s)
{
    if (s->begun_count++ == 0) {
        c->bindings[s->binding].begun_subs++;
    }
}

static void
sub_begun_sub(moqr_core_t *c, r_sub_t *s)
{
    if (s->begun_count > 0 && --s->begun_count == 0) {
        c->bindings[s->binding].begun_subs--;
    }
}

static void
sub_retire(moqr_core_t *c, uint32_t slot, uint64_t now_us)
{
    r_sub_t *s = &c->subs[slot];
    /* Drop any begun-live-edge contribution before unlinking: a retiring sub can
     * never need an evict-reset, and leaving begun_subs armed would keep the
     * sweep scanning a dead binding (and, after slot reuse, mis-gate). */
    if (s->begun_count > 0) {
        if (c->bindings[s->binding].begun_subs > 0) {
            c->bindings[s->binding].begun_subs--;
        }
        s->begun_count = 0;
    }
    sub_list_remove(c, s->binding, slot);   /* unlink from the binding's list */
    uint32_t track_slot = s->track;
    if ((c->tracks[track_slot].gen & 1u) != 0 &&
        c->tracks[track_slot].gen == s->track_gen_slot) {
        /* Last subscriber arms the linger deadline. */
        if (track_sub_count(c, track_slot) == 1 /* this one */ &&
            c->tracks[track_slot].state == R_TRACK_ACTIVE &&
            c->tracks[track_slot].source_kind == R_TRACK_SOURCE_PULL) {
            c->tracks[track_slot].linger_deadline_us =
                now_us + c->linger_us;
            if (c->tracks[track_slot].linger_deadline_us == 0) {
                c->tracks[track_slot].linger_deadline_us = 1; /* armed */
            }
        }
        track_sub_unlink(c, track_slot, slot);
    }
    /* Clear an outstanding delivery pinned to this sub. */
    r_binding_t *b = &c->bindings[s->binding];
    if ((b->gen & 1u) != 0 && b->gen == s->binding_gen && b->out_active &&
        b->out_sub == slot) {
        b->out_active = false;
        binding_unpin(b);
    }
    /* The retire changes what this binding delivers next (a pinned delivery
     * may have been released; a sibling sub's work surfaces). Spurious for
     * the last sub of a closing binding — the close clears the bit. */
    if ((b->gen & 1u) != 0 && b->gen == s->binding_gen) {
        ready_mark(c, s->binding);
    }
    gpos_free(c, s);
    s->gen++;   /* odd -> even */
}


/* Longest live announce for `ns` NOT owned by the excluded binding
 * generation (the one whose loss is being handled); UINT32_MAX if none. */
static uint32_t
trie_longest_announce_excluding(const moqr_core_t *c, moqr_ns_t ns,
                                uint32_t excl_bslot, uint32_t excl_bgen)
{
    uint32_t best = UINT32_MAX;
    uint32_t node = 0;
    if (c->nodes[0].has_announce &&
        !(c->nodes[0].ann_binding == excl_bslot &&
          c->nodes[0].ann_binding_gen == excl_bgen)) {
        best = 0;
    }
    for (size_t i = 0; i < ns.count; i++) {
        node = trie_find_child(c, node, ns.parts[i]);
        if (node == UINT32_MAX) {
            break;
        }
        if (c->nodes[node].has_announce &&
            !(c->nodes[node].ann_binding == excl_bslot &&
              c->nodes[node].ann_binding_gen == excl_bgen)) {
            best = node;
        }
    }
    return best;
}

/* The SOURCE of an ACTIVE track with live downstream subscribers just died
 * by transport terminal. Handle the standing demand VISIBLY and
 * deterministically: retarget the track to the longest live alternate
 * announce (one fresh upstream SUBSCRIBE; established subscribers stay
 * active and resolve silently — upstream acceptance touches only PARKED
 * subs — with retained delivery state untouched, and the generation bump
 * refusing any stale resolution from the dead source), or — with no
 * alternate — terminate every subscriber explicitly, exactly as a graceful
 * source end would. Silently leaving an established subscription alive
 * with no source is forbidden.
 *
 * Reserve-before-mutate: one intent for the retarget, or one per live sub
 * for the terminal fan-out; MOQR_ERR_WOULD_BLOCK leaves the track
 * untouched (the resumable binding-close re-finds it by its unchanged
 * upstream identity). *retargeted reports which arm ran. */
static moqr_result_t
track_source_failover(moqr_core_t *c, uint32_t tslot, uint32_t excl_bslot,
                      uint32_t excl_bgen, uint64_t now_us, bool *retargeted)
{
    r_track_t *t = &c->tracks[tslot];
    moq_bytes_t parts[32];
    moqr_ns_t ns = { .parts = parts, .count = 0 };
    uint32_t count = key_part_count(&t->key);

    *retargeted = false;
    for (uint32_t i = 0; i < count && i < 32; i++) {
        parts[i] = key_part(&t->key, i);
    }
    ns.count = count < 32 ? count : 32;

    uint32_t pub_node =
        trie_longest_announce_excluding(c, ns, excl_bslot, excl_bgen);

    if (pub_node != UINT32_MAX) {
        if (!intent_space(c, 1)) {
            return MOQR_ERR_WOULD_BLOCK;
        }
        t->state = R_TRACK_PENDING;
        t->source_kind = R_TRACK_SOURCE_PULL;
        t->track_gen++;
        t->has_upstream_binding = true;
        t->up_binding = c->nodes[pub_node].ann_binding;
        t->up_binding_gen = c->nodes[pub_node].ann_binding_gen;
        t->src_ann_node = pub_node;
        t->src_ann_binding = c->nodes[pub_node].ann_binding;
        t->src_ann_binding_gen = c->nodes[pub_node].ann_binding_gen;
        moqr_intent_t *up = intent_push(c, MOQR_INTENT_UPSTREAM_SUBSCRIBE);
        up->binding_cookie = c->bindings[t->up_binding].cookie;
        up->track = track_handle(c, tslot);
        up->track_gen = t->track_gen;
        intent_set_ns_from_key(up, &t->key);
        r_trace(c, MOQR_TRACE_UPSTREAM_SUBSCRIBE, 2 /* failover */, tslot,
                t->track_gen, t->up_binding, 0);
        *retargeted = true;
        return MOQR_OK;
    }

    uint32_t n = track_sub_count(c, tslot);
    if (n > 0 && !intent_space(c, n)) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    uint32_t tgen = t->gen;
    for (uint32_t i = 0; i < c->max_subs; i++) {
        r_sub_t *sub = &c->subs[i];
        if ((sub->gen & 1u) == 0 || sub->track != tslot ||
            sub->track_gen_slot != tgen) {
            continue;
        }
        uint64_t bcookie = 0;
        if (sub->binding < c->max_bindings &&
            c->bindings[sub->binding].gen == sub->binding_gen) {
            bcookie = c->bindings[sub->binding].cookie;
        }
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_SUB_DONE);
        it->binding_cookie = bcookie;
        it->cookie = sub->cookie;
        it->sub = sub_handle(c, i);
        it->error_code = R_DONE_TRACK_ENDED;
        it->pd = core_local_done(MOQR_PD_TRACK_ENDED);
        sub_retire(c, i, now_us);
    }
    return MOQR_OK;
}

moqr_result_t
moqr_core_binding_close(moqr_core_t *c, moqr_binding_t bh, uint64_t now_us)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }

    /* Resumable close. The atomic worst case (parked-sub rejects +
     * per-announce NS_GONE fan-out over possibly many announced namespaces)
     * can exceed the intent ring, so close makes bounded progress each call
     * and returns WOULD_BLOCK with a partial result; the caller drains and
     * retries. Each unit reserved here fits an empty ring under the create
     * clamp (rejects: 1 each; one announce's fan-out: <= max_ns_subs).
     * Retries re-scan and skip already-processed items (retired subs,
     * cleared announces), so no per-binding cursor state is needed. */

    /* Phase 1: reject parked subs on tracks this binding upstreams — with
     * the upstream gone they can never resolve — then transition each track
     * once its parked set is clear. */
    for (uint32_t i = 0; i < c->max_tracks; i++) {
        r_track_t *t = &c->tracks[i];
        if ((t->gen & 1u) == 0 || !t->has_upstream_binding ||
            t->up_binding != bslot || t->up_binding_gen != b->gen) {
            continue;
        }
        if (t->state == R_TRACK_PENDING) {
            for (uint32_t s = 0; s < c->max_subs; s++) {
                r_sub_t *sub = &c->subs[s];
                if ((sub->gen & 1u) == 0 || sub->track != i ||
                    sub->track_gen_slot != t->gen ||
                    sub->state != R_SUB_PARKED) {
                    continue;
                }
                if (!intent_space(c, 1)) {
                    return MOQR_ERR_WOULD_BLOCK;   /* partial; retry */
                }
                moqr_intent_t *it = intent_push(c, MOQR_INTENT_REJECT_SUB);
                it->cookie = sub->cookie;
                it->binding_cookie = c->bindings[sub->binding].cookie;
                it->error_code = R_ERR_DOES_NOT_EXIST;
                sub_retire(c, s, now_us);
            }
        }
        if (t->state == R_TRACK_ACTIVE && track_sub_count(c, i) > 0) {
            /* standing established demand: retarget or terminate — the
             * one thing this close may NOT do is strand it silently */
            bool retargeted = false;
            moqr_result_t frc =
                track_source_failover(c, i, bslot, b->gen, now_us,
                                      &retargeted);
            if (frc == MOQR_ERR_WOULD_BLOCK) {
                return MOQR_ERR_WOULD_BLOCK;   /* partial; retry */
            }
            if (retargeted) {
                continue;   /* the track now belongs to the alternate */
            }
        }
        t->has_upstream_binding = false;
        t->source_kind = R_TRACK_SOURCE_NONE;
        t->state = R_TRACK_WARM;
        t->track_gen++;   /* stale any in-flight resolution */
        moqr_log_stats_t ls;
        moqr_log_get_stats(t->log, &ls);
        if (ls.record_count == 0 && track_sub_count(c, i) == 0 &&
            track_fetch_count(c, i) == 0) {
            track_free_slot(c, i);
        }
    }

    /* Phase 2: announces owned by this binding — clear + NS_GONE fan-out,
     * one announce at a time (reserve its matching-watcher count first). */
    for (uint32_t n = 0; n < c->node_count; n++) {
        if (!c->nodes[n].has_announce || c->nodes[n].ann_binding != bslot ||
            c->nodes[n].ann_binding_gen != b->gen) {
            continue;
        }
        uint32_t matching = 0;
        for (uint32_t s = 0; s < c->max_ns_subs; s++) {
            if (c->ns_subs[s].used &&
                !(c->ns_subs[s].binding == bslot &&
                  c->ns_subs[s].binding_gen == b->gen) &&
                prefix_matches_node(c, &c->ns_subs[s], n)) {
                matching++;
            }
        }
        if (!intent_space(c, matching)) {
            return MOQR_ERR_WOULD_BLOCK;   /* this announce next retry */
        }
        c->nodes[n].has_announce = false;
        r_bump_route_epoch(c);
        r_trace(c, MOQR_TRACE_ROUTE_REMOVE, 0 /* announce */, n, bslot, 0,
                0);
        for (uint32_t s = 0; s < c->max_ns_subs; s++) {
            if (c->ns_subs[s].used &&
                !(c->ns_subs[s].binding == bslot &&
                  c->ns_subs[s].binding_gen == b->gen) &&
                prefix_matches_node(c, &c->ns_subs[s], n)) {
                moqr_intent_t *it = intent_push(c, MOQR_INTENT_NS_GONE);
                it->binding_cookie =
                    c->bindings[c->ns_subs[s].binding].cookie;
                it->cookie = c->ns_subs[s].cookie;
                intent_set_ns_from_node(c, it, n);
                it->value = key_rd32(c->ns_subs[s].prefix);
            }
        }
    }

    /* Phase 3: intent-free cleanup (only reached once phases 1-2 finish).
     * Subs owned by this binding retire; its namespace subscriptions free. */
    for (uint32_t s = 0; s < c->max_subs; s++) {
        if ((c->subs[s].gen & 1u) != 0 && c->subs[s].binding == bslot &&
            c->subs[s].binding_gen == b->gen) {
            sub_retire(c, s, now_us);
        }
    }
    for (uint32_t s = 0; s < c->max_ns_subs; s++) {
        if (c->ns_subs[s].used && c->ns_subs[s].binding == bslot &&
            c->ns_subs[s].binding_gen == b->gen) {
            uint32_t plen = c->ns_subs[s].prefix_len;
            c->intern_used -= plen;
            r_free(c, c->ns_subs[s].prefix, plen);
            memset(&c->ns_subs[s], 0, sizeof(c->ns_subs[s]));
        }
    }
    /* Fetch cursors owned by this binding: unpin (release pin-byte budget) and
     * invalidate. Intent-free — the session/adapter tears down the fetch streams
     * on session close, so no wire action is queued here. */
    for (uint32_t fi = 0; fi < c->max_fetches; fi++) {
        r_fetch_t *f = &c->fetches[fi];
        if ((f->gen & 1u) != 0 && f->binding == bslot &&
            f->binding_gen == b->gen) {
            fetch_unpin(c, f);
            f->gen++;   /* odd -> even */
        }
    }
    b->out_active = false;
    binding_unpin(b);
    /* The slot is reusable after the gen bump: a stale ready bit would
     * resolve to the REPLACEMENT binding's cookie on a later drain. */
    ready_clear(c, bslot);
    b->gen++;   /* odd -> even */
    r_trace(c, MOQR_TRACE_ROUTE_REMOVE, 2 /* binding */, bslot, 0, 0, 0);
    return MOQR_OK;
}

/* -- ops: announce / ns discovery -------------------------------------------------------------- */

moqr_result_t
moqr_core_announce(moqr_core_t *c, moqr_binding_t bh, moqr_ns_t ns)
{
    return moqr_core_announce_ex(c, bh, ns, 0);
}

moqr_result_t
moqr_core_announce_ex(moqr_core_t *c, moqr_binding_t bh, moqr_ns_t ns,
                      uint64_t session_cookie)
{
    if (c == NULL || !ns_view_ok(ns, 0)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    /* Count matching ns-subs for fan-out reservation; the trie walk for
     * counting must not create nodes, so match against parts directly. */
    uint32_t need = 0;
    for (uint32_t s = 0; s < c->max_ns_subs; s++) {
        if (!c->ns_subs[s].used) {
            continue;
        }
        uint32_t pcount = key_rd32(c->ns_subs[s].prefix);
        if (pcount > ns.count) {
            continue;
        }
        bool match = true;
        uint32_t off = 4u * (1u + pcount);
        for (uint32_t i = 0; i < pcount && match; i++) {
            uint32_t plen = key_rd32(c->ns_subs[s].prefix + 4u * (1u + i));
            if (ns.parts[i].len != plen ||
                (plen != 0 &&
                 memcmp(ns.parts[i].data, c->ns_subs[s].prefix + off,
                        plen) != 0)) {
                match = false;
            }
            off += plen;
        }
        if (match) {
            need++;
        }
    }
    if (!intent_space(c, need)) {
        return MOQR_ERR_WOULD_BLOCK;
    }

    uint32_t node = 0;
    for (size_t i = 0; i < ns.count; i++) {
        moqr_result_t rc = trie_child_get_or_create(c, node, ns.parts[i],
                                                    &node);
        if (rc != MOQR_OK) {
            return rc;
        }
    }
    if (c->nodes[node].has_announce) {
        return MOQR_ERR_WRONG_STATE;   /* occupied namespace */
    }
    c->nodes[node].has_announce = true;
    c->nodes[node].ann_binding = bslot;
    c->nodes[node].ann_binding_gen = b->gen;
    c->nodes[node].session_cookie = session_cookie;
    r_bump_route_epoch(c);
    r_trace(c, MOQR_TRACE_ROUTE_ADD, 0 /* announce */, node, bslot, 0, 0);

    for (uint32_t s = 0; s < c->max_ns_subs; s++) {
        if (c->ns_subs[s].used && prefix_matches_node(c, &c->ns_subs[s],
                                                      node)) {
            moqr_intent_t *it = intent_push(c, MOQR_INTENT_NS_FOUND);
            it->binding_cookie = c->bindings[c->ns_subs[s].binding].cookie;
            it->cookie = c->ns_subs[s].cookie;
            intent_set_ns_from_node(c, it, node);
            it->value = key_rd32(c->ns_subs[s].prefix);
        }
    }
    return MOQR_OK;
}

moqr_result_t
moqr_core_unannounce(moqr_core_t *c, moqr_binding_t bh, moqr_ns_t ns)
{
    if (c == NULL || !ns_view_ok(ns, 0)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    uint32_t node = trie_walk(c, ns);
    if (node == UINT32_MAX || !c->nodes[node].has_announce ||
        c->nodes[node].ann_binding != bslot ||
        c->nodes[node].ann_binding_gen != b->gen) {
        return MOQR_ERR_WRONG_STATE;
    }
    uint32_t need = 0;
    for (uint32_t s = 0; s < c->max_ns_subs; s++) {
        if (c->ns_subs[s].used &&
            prefix_matches_node(c, &c->ns_subs[s], node)) {
            need++;
        }
    }
    if (!intent_space(c, need)) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    uint64_t ann_session_cookie = c->nodes[node].session_cookie;
    c->nodes[node].has_announce = false;
    r_bump_route_epoch(c);
    r_trace(c, MOQR_TRACE_ROUTE_REMOVE, 0, node, bslot, 0, 0);
    for (uint32_t s = 0; s < c->max_ns_subs; s++) {
        if (c->ns_subs[s].used &&
            prefix_matches_node(c, &c->ns_subs[s], node)) {
            moqr_intent_t *it = intent_push(c, MOQR_INTENT_NS_GONE);
            it->binding_cookie = c->bindings[c->ns_subs[s].binding].cookie;
            it->cookie = c->ns_subs[s].cookie;
            intent_set_ns_from_node(c, it, node);
            it->value = key_rd32(c->ns_subs[s].prefix);
        }
    }
    /* A voluntary withdrawal retires the announce's OWN revalidation
     * grant: the authorization it re-checks no longer exists, and a grant
     * that outlives its announce could later revalidate to DENY and
     * withdraw a REPLACEMENT publisher's namespace. (A mid-teardown
     * cancel-vehicle grant — revoked + unannounced — is left to its own
     * peek/ack; its announce is already gone, so it cannot reach here.) */
    for (uint32_t gi = 0; gi < c->max_grants; gi++) {
        r_grant *g = &c->grants[gi];
        if ((g->gen & 1u) != 0 &&
            g->action == MOQR_AUTH_PUBLISH_NAMESPACE &&
            g->binding_cookie == c->bindings[bslot].cookie &&
            g->session_cookie == ann_session_cookie &&
            !(g->revoked && g->unannounced)) {
            grant_free_slot(c, gi);
        }
    }
    return MOQR_OK;
}

/* A wire cancel for {binding_cookie, session_cookie} is already peekable — from
 * either a revoked+unannounced grant or a queued pending cancel — so a fresh
 * force-withdraw must not enqueue a duplicate. */
static bool
cancel_already_pending(const moqr_core_t *c, uint64_t binding_cookie,
                       uint64_t session_cookie)
{
    for (uint32_t i = 0; i < c->max_grants; i++) {
        const r_grant *g = &c->grants[i];
        if ((g->gen & 1u) != 0 && g->revoked && g->unannounced &&
            g->binding_cookie == binding_cookie &&
            g->session_cookie == session_cookie) {
            return true;
        }
    }
    for (uint32_t i = 0; i < c->max_cancels; i++) {
        const r_pending_cancel *pc = &c->pending_cancels[i];
        if (pc->used && pc->binding_cookie == binding_cookie &&
            pc->session_cookie == session_cookie) {
            return true;
        }
    }
    return false;
}

static uint32_t
cancel_free_slot(const moqr_core_t *c)
{
    for (uint32_t i = 0; i < c->max_cancels; i++) {
        if (!c->pending_cancels[i].used) {
            return i;
        }
    }
    return UINT32_MAX;
}

/* A track is purged by force-withdraw iff its STORED source-announce identity is
 * the withdrawn node + owner (not a re-derived longest-prefix). */
static bool
track_sourced_by(const r_track_t *t, uint32_t node, uint32_t bslot, uint32_t bgen)
{
    return (t->gen & 1u) != 0 && t->src_ann_node == node &&
           t->src_ann_binding == bslot && t->src_ann_binding_gen == bgen;
}

/* Relay-topology force-withdraw of an announce: purge every track it sourced
 * (per-state terminals + fetch invalidation + slot free), clear the announce, fan
 * NS_GONE, and wire-cancel the publisher. Resumable, one track per reserved
 * unit: a short intent ring or a full cancel queue returns WOULD_BLOCK with the
 * tracks purged so far already committed, and the caller drains and retries to
 * completion. Only the FIRST unit is preflighted before any mutation, and the
 * final namespace effects wait for every unit. No-op OK on a missing /
 * no-announce / mirror-owned node.
 *
 * request_error is a REQUEST_ERROR-registry code and feeds exactly the two
 * wire messages that take that registry: the parked subscribers' REJECT_SUB
 * and the publisher-side cancel_namespace. Active subscribers' SUB_DONE is a
 * PUBLISH_DONE-registry message and always carries TRACK_ENDED — the track
 * stops being published, whatever prompted the withdrawal. */
moqr_result_t
moqr_core_force_withdraw(moqr_core_t *c, moqr_ns_t ns, uint64_t request_error,
                         uint64_t now_us)
{
    if (c == NULL || !ns_view_ok(ns, 0)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t node = trie_walk(c, ns);
    if (node == UINT32_MAX || !c->nodes[node].has_announce) {
        return MOQR_OK;   /* nothing announced here */
    }
    uint32_t ann_bslot = c->nodes[node].ann_binding;
    uint32_t ann_bgen = c->nodes[node].ann_binding_gen;
    uint64_t session_cookie = c->nodes[node].session_cookie;
    uint64_t owner_cookie = c->bindings[ann_bslot].cookie;
    if (owner_cookie >= MOQR_SHARD_COOKIE_BASE) {
        return MOQR_OK;   /* mirror-owned pseudo-binding: never force-withdraw */
    }

    /* -- Resumable purge: one track per reserved unit -- *
     * Each unit (a track's subscriber terminals + its upstream release)
     * fits an empty ring under the create clamp; ring exhaustion returns
     * WOULD_BLOCK with PARTIAL progress — already-purged tracks are freed
     * and no longer match, so the caller drains and retries to
     * completion, exactly like binding_close. The announce clear + its
     * NS_GONE fan-out is the FINAL unit, reserved together with the
     * publisher cancel slot, so the namespace state never clears while a
     * sourced track still stands. */
    uint32_t ns_gone = 0;
    for (uint32_t si = 0; si < c->max_ns_subs; si++) {
        if (c->ns_subs[si].used &&
            prefix_matches_node(c, &c->ns_subs[si], node)) {
            ns_gone++;
        }
    }
    for (uint32_t ti = 0; ti < c->max_tracks; ti++) {
        r_track_t *t = &c->tracks[ti];
        if (!track_sourced_by(t, node, ann_bslot, ann_bgen)) {
            continue;
        }
        uint32_t unit = 0;
        if (t->state == R_TRACK_PENDING) {
            for (uint32_t si = 0; si < c->max_subs; si++) {
                r_sub_t *s = &c->subs[si];
                if ((s->gen & 1u) != 0 && s->track == ti &&
                    s->track_gen_slot == t->gen && s->state == R_SUB_PARKED) {
                    unit++;
                }
            }
        } else if (t->state == R_TRACK_ACTIVE) {
            unit += track_sub_count(c, ti);
        }
        if (t->has_upstream_binding &&
            (c->bindings[t->up_binding].gen & 1u) != 0) {
            unit++;
        }
        if (!intent_space(c, unit)) {
            return MOQR_ERR_WOULD_BLOCK;   /* partial; drain and retry */
        }
        uint32_t tgen = t->gen;
        if (t->state == R_TRACK_PENDING) {
            for (uint32_t si = 0; si < c->max_subs; si++) {
                r_sub_t *s = &c->subs[si];
                if ((s->gen & 1u) == 0 || s->track != ti ||
                    s->track_gen_slot != tgen || s->state != R_SUB_PARKED) {
                    continue;
                }
                moqr_intent_t *it = intent_push(c, MOQR_INTENT_REJECT_SUB);
                it->binding_cookie = c->bindings[s->binding].cookie;
                it->cookie = s->cookie;
                it->error_code = request_error;
                sub_retire(c, si, now_us);
            }
        } else if (t->state == R_TRACK_ACTIVE) {
            for (uint32_t si = 0; si < c->max_subs; si++) {
                r_sub_t *s = &c->subs[si];
                if ((s->gen & 1u) == 0 || s->track != ti ||
                    s->track_gen_slot != tgen || s->state != R_SUB_ACTIVE) {
                    continue;
                }
                moqr_intent_t *it = intent_push(c, MOQR_INTENT_SUB_DONE);
                it->binding_cookie = c->bindings[s->binding].cookie;
                it->cookie = s->cookie;
                it->sub = sub_handle(c, si);
                it->error_code = R_DONE_TRACK_ENDED;
                it->pd = core_local_done(MOQR_PD_TRACK_ENDED);
                sub_retire(c, si, now_us);
            }
        }
        /* Fetches on this track: unpin + invalidate the handle so no held object
         * replays (next peek is STALE_HANDLE, before the track-gone guard). */
        for (uint32_t fi = 0; fi < c->max_fetches; fi++) {
            r_fetch_t *f = &c->fetches[fi];
            if ((f->gen & 1u) != 0 && f->track == ti &&
                f->track_gen_slot == tgen) {
                fetch_unpin(c, f);
                f->gen++;   /* odd -> even: invalidate the cursor handle */
            }
        }
        /* Release the upstream subscription: without this the binding's
         * upstream slot (and the publisher session's live subscription)
         * outlives the purged track, and the NEXT generation's fresh
         * demand dies as a duplicate — an accepted re-announcement that
         * can never carry data again. */
        if (t->has_upstream_binding &&
            (c->bindings[t->up_binding].gen & 1u) != 0) {
            moqr_intent_t *ui =
                intent_push(c, MOQR_INTENT_UPSTREAM_UNSUBSCRIBE);
            ui->track = track_handle(c, ti);
            ui->track_gen = t->track_gen;
            ui->cookie = t->upstream_cookie;
            ui->binding_cookie = c->bindings[t->up_binding].cookie;
        }
        track_free_slot(c, ti);   /* pool-gen bump orphans any stragglers */
    }

    /* Final unit: the announce clear + NS_GONE fan-out + publisher cancel. */
    if (!intent_space(c, ns_gone)) {
        return MOQR_ERR_WOULD_BLOCK;   /* tracks already purged stay purged */
    }
    bool need_cancel = !cancel_already_pending(c, owner_cookie, session_cookie);
    uint32_t cancel_slot = UINT32_MAX;
    if (need_cancel) {
        cancel_slot = cancel_free_slot(c);
        if (cancel_slot == UINT32_MAX) {
            return MOQR_ERR_WOULD_BLOCK;   /* transient: queue drains on ack */
        }
    }

    /* Clear the announce (bypass the owner check) + NS_GONE fan-out. */
    c->nodes[node].has_announce = false;
    r_bump_route_epoch(c);
    r_trace(c, MOQR_TRACE_ROUTE_REMOVE, 0 /* announce */, node, ann_bslot, 0, 0);
    for (uint32_t si = 0; si < c->max_ns_subs; si++) {
        if (!c->ns_subs[si].used ||
            !prefix_matches_node(c, &c->ns_subs[si], node)) {
            continue;
        }
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_NS_GONE);
        it->binding_cookie = c->bindings[c->ns_subs[si].binding].cookie;
        it->cookie = c->ns_subs[si].cookie;
        intent_set_ns_from_node(c, it, node);
        it->value = key_rd32(c->ns_subs[si].prefix);
    }

    /* Wire-cancel the publisher (unless already pending) + retire its live grant. */
    if (need_cancel) {
        r_pending_cancel *pc = &c->pending_cancels[cancel_slot];
        pc->used = true;
        pc->binding_cookie = owner_cookie;
        pc->session_cookie = session_cookie;
        pc->error_code = request_error;
        /* A live / mid-teardown PUBLISH_NAMESPACE grant would keep revalidating a
         * withdrawn namespace: retire exactly that one grant (an already
         * revoked+unannounced grant is the dedupe case and is left to its own ack;
         * never retire_grants(binding) — that would kill other namespaces). */
        for (uint32_t gi = 0; gi < c->max_grants; gi++) {
            r_grant *g = &c->grants[gi];
            if ((g->gen & 1u) != 0 &&
                g->action == MOQR_AUTH_PUBLISH_NAMESPACE &&
                g->binding_cookie == owner_cookie &&
                g->session_cookie == session_cookie &&
                !(g->revoked && g->unannounced)) {
                grant_free_slot(c, gi);
            }
        }
    }
    return MOQR_OK;
}

/* Collect announces under a subtree matching a new ns-sub (recursive). */
static void
nsub_fanout_existing(moqr_core_t *c, const r_ns_sub_t *nsub, uint32_t node,
                     bool counting, uint32_t *count)
{
    if (c->nodes[node].has_announce &&
        prefix_matches_node(c, nsub, node)) {
        if (counting) {
            (*count)++;
        } else {
            moqr_intent_t *it = intent_push(c, MOQR_INTENT_NS_FOUND);
            it->binding_cookie = c->bindings[nsub->binding].cookie;
            it->cookie = nsub->cookie;
            intent_set_ns_from_node(c, it, node);
            it->value = key_rd32(nsub->prefix);
        }
    }
    for (uint32_t i = 0; i < c->nodes[node].child_count; i++) {
        nsub_fanout_existing(c, nsub, c->nodes[node].children[i], counting,
                             count);
    }
}

moqr_result_t
moqr_core_ns_subscribe(moqr_core_t *c, moqr_binding_t bh, moqr_ns_t prefix,
                       uint64_t cookie)
{
    if (c == NULL || !ns_view_ok(prefix, 0)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    uint32_t slot = UINT32_MAX;
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        if (!c->ns_subs[i].used) {
            slot = i;
            break;
        }
    }
    if (slot == UINT32_MAX) {
        return r_refuse(c, MOQR_REFUSE_NS_SUBS);
    }

    /* Build the prefix buffer (key layout without name). */
    uint32_t plen = 4u * (1u + (uint32_t)prefix.count);
    for (size_t i = 0; i < prefix.count; i++) {
        plen += (uint32_t)prefix.parts[i].len;
    }
    if (c->intern_used + plen > c->intern_budget) {
        return r_refuse(c, MOQR_REFUSE_NAME_BYTES);
    }
    uint8_t *pbuf = r_alloc(c, plen);
    if (pbuf == NULL) {
        return MOQR_ERR_NOMEM;
    }
    key_wr32(pbuf, (uint32_t)prefix.count);
    uint32_t off = 4u * (1u + (uint32_t)prefix.count);
    for (size_t i = 0; i < prefix.count; i++) {
        key_wr32(pbuf + 4u * (1u + i), (uint32_t)prefix.parts[i].len);
        if (prefix.parts[i].len != 0) {
            memcpy(pbuf + off, prefix.parts[i].data, prefix.parts[i].len);
        }
        off += (uint32_t)prefix.parts[i].len;
    }

    r_ns_sub_t probe;
    memset(&probe, 0, sizeof(probe));
    probe.binding = bslot;
    probe.binding_gen = b->gen;
    probe.cookie = cookie;
    probe.prefix = pbuf;
    probe.prefix_len = plen;

    uint32_t need = 0;
    nsub_fanout_existing(c, &probe, 0, /*counting=*/true, &need);
    if (!intent_space(c, need)) {
        r_free(c, pbuf, plen);
        return MOQR_ERR_WOULD_BLOCK;
    }
    c->ns_subs[slot] = probe;
    c->ns_subs[slot].used = true;
    c->intern_used += plen;
    r_bump_route_epoch(c);
    r_trace(c, MOQR_TRACE_ROUTE_ADD, 2 /* ns_sub */, slot, bslot, cookie, 0);
    nsub_fanout_existing(c, &c->ns_subs[slot], 0, /*counting=*/false, NULL);
    return MOQR_OK;
}

moqr_result_t
moqr_core_ns_unsubscribe(moqr_core_t *c, moqr_binding_t bh, uint64_t cookie)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        r_ns_sub_t *s = &c->ns_subs[i];
        if (s->used && s->binding == bslot && s->binding_gen == b->gen &&
            s->cookie == cookie) {
            c->intern_used -= s->prefix_len;
            r_free(c, s->prefix, s->prefix_len);
            memset(s, 0, sizeof(*s));
            r_bump_route_epoch(c);
            r_trace(c, MOQR_TRACE_ROUTE_REMOVE, 2, i, bslot, cookie, 0);
            return MOQR_OK;
        }
    }
    return MOQR_ERR_STALE_HANDLE;
}

/* -- ops: subscribe ------------------------------------------------------------------------------- */

void
moqr_subscribe_req_init(moqr_subscribe_req_t *req)
{
    if (req == NULL) {
        return;
    }
    memset(req, 0, sizeof(*req));
    req->struct_size = sizeof(*req);
    req->filter.type = MOQR_FILTER_LARGEST_OBJECT;
    req->forward = true;   /* deliver unless the request explicitly pauses */
    req->subscriber_priority = 128;
}

static uint32_t
sub_slot_find(moqr_core_t *c)
{
    for (uint32_t i = 0; i < c->max_subs; i++) {
        if ((c->subs[i].gen & 1u) == 0) {
            return i;
        }
    }
    return UINT32_MAX;
}

moqr_result_t
moqr_core_subscribe(moqr_core_t *c, moqr_binding_t bh,
                    const moqr_subscribe_req_t *req, moqr_sub_t *out)
{
    /* Prefix-size (ABI) contract: `forward` is a trailing field, so a caller
     * that predates it (struct_size ending before `forward`) is still valid and
     * defaults to forward=true. The minimum is everything up to `forward`. */
    if (c == NULL || req == NULL || out == NULL ||
        req->struct_size < offsetof(moqr_subscribe_req_t, forward) ||
        req->filter.type < MOQR_FILTER_NEXT_GROUP_START ||
        req->filter.type > MOQR_FILTER_ABSOLUTE_RANGE ||
        !ftn_view_ok(req->ns, req->name)) {
        return MOQR_ERR_INVAL;
    }
    bool req_forward = true;   /* default when the caller omits the field */
    if (req->struct_size >=
        offsetof(moqr_subscribe_req_t, forward) + sizeof(req->forward)) {
        req_forward = req->forward;
    }
    if (req->filter.type == MOQR_FILTER_ABSOLUTE_RANGE &&
        req->filter.start_group + req->filter.end_group_delta <
            req->filter.start_group) {
        return MOQR_ERR_INVAL;   /* end-group overflow (spec: violation) */
    }
    *out = MOQR_SUB_INVALID;
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (!intent_space(c, 2)) {   /* worst case: ACCEPT + UPSTREAM_SUBSCRIBE */
        return MOQR_ERR_WOULD_BLOCK;
    }

    r_key_t key = { NULL, 0 };
    moqr_result_t rc = key_build(c, req->ns, req->name, &key);
    if (rc != MOQR_OK) {
        return rc;
    }
    uint64_t hash = key_hash(&key);
    uint32_t tslot = track_find(c, &key, hash);

    if (tslot == UINT32_MAX) {
        /* New track: needs an announced publisher. */
        uint32_t pub_node = trie_longest_announce(c, req->ns);
        if (pub_node == UINT32_MAX) {
            key_release(c, &key);
            moqr_intent_t *it = intent_push(c, MOQR_INTENT_REJECT_SUB);
            it->binding_cookie = b->cookie;
            it->cookie = req->cookie;
            it->error_code = R_ERR_DOES_NOT_EXIST;
            return MOQR_OK;
        }
        uint32_t sslot = sub_slot_find(c);
        if (sslot == UINT32_MAX) {
            key_release(c, &key);
            return r_refuse(c, MOQR_REFUSE_SUBS);
        }
        rc = track_create(c, &key, hash, &tslot);
        if (rc != MOQR_OK) {
            key_release(c, &key);
            return rc;
        }
        r_track_t *t = &c->tracks[tslot];
        t->state = R_TRACK_PENDING;
        t->source_kind = R_TRACK_SOURCE_PULL;
        t->track_gen++;
        t->has_upstream_binding = true;
        t->up_binding = c->nodes[pub_node].ann_binding;
        t->up_binding_gen = c->nodes[pub_node].ann_binding_gen;
        t->src_ann_node = pub_node;
        t->src_ann_binding = c->nodes[pub_node].ann_binding;
        t->src_ann_binding_gen = c->nodes[pub_node].ann_binding_gen;

        r_sub_t *s = &c->subs[sslot];
        uint32_t gen = s->gen + 1;
        memset(s, 0, sizeof(*s));
        s->gen = gen;
        s->binding = bslot;
        s->binding_gen = b->gen;
        s->track = tslot;
        s->track_gen_slot = t->gen;
        s->state = R_SUB_PARKED;
        s->forward = req_forward;   /* initial Forward, carried atomically */
        s->subscriber_priority = req->subscriber_priority;
        s->group_order = req->group_order;
        s->cookie = req->cookie;
        /* Filter resolves at upstream_ok; remember the request. */
        s->filter_type = req->filter.type;
        s->start_group = req->filter.start_group;
        s->start_object = req->filter.start_object;
        s->end_group = req->filter.end_group_delta;   /* delta until resolve */
        rc = gpos_alloc(c, s, t->log);
        if (rc != MOQR_OK) {
            s->gen++;
            track_free_slot(c, tslot);
            return rc;
        }
        sub_list_push(c, bslot, sslot);
        track_sub_link(c, tslot, sslot);
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_UPSTREAM_SUBSCRIBE);
        it->binding_cookie = c->bindings[t->up_binding].cookie;
        it->track = track_handle(c, tslot);
        it->track_gen = t->track_gen;
        intent_set_ns_from_key(it, &t->key);
        r_trace(c, MOQR_TRACE_UPSTREAM_SUBSCRIBE, 0, tslot, t->track_gen,
                bslot, 0);
        *out = sub_handle(c, sslot);
        return MOQR_OK;
    }

    /* Existing track. */
    key_release(c, &key);
    r_track_t *t = &c->tracks[tslot];
    uint32_t sslot = sub_slot_find(c);
    if (sslot == UINT32_MAX) {
        return r_refuse(c, MOQR_REFUSE_SUBS);
    }
    r_sub_t *s = &c->subs[sslot];
    uint32_t gen = s->gen + 1;
    memset(s, 0, sizeof(*s));
    s->gen = gen;
    s->binding = bslot;
    s->binding_gen = b->gen;
    s->track = tslot;
    s->track_gen_slot = t->gen;
    s->forward = req_forward;   /* initial Forward, carried atomically */
    s->subscriber_priority = req->subscriber_priority;
    s->group_order = req->group_order;
    s->cookie = req->cookie;
    moqr_result_t crc = gpos_alloc(c, s, t->log);
    if (crc != MOQR_OK) {
        s->gen++;
        return crc;
    }
    sub_list_push(c, bslot, sslot);
    track_sub_link(c, tslot, sslot);
    t->linger_deadline_us = 0;   /* subscriber present again */

    if (t->state == R_TRACK_PENDING) {
        s->state = R_SUB_PARKED;
        s->filter_type = req->filter.type;
        s->start_group = req->filter.start_group;
        s->start_object = req->filter.start_object;
        s->end_group = req->filter.end_group_delta;
        *out = sub_handle(c, sslot);
        return MOQR_OK;
    }

    /* ACTIVE or WARM: accept immediately from track state. */
    s->state = R_SUB_ACTIVE;
    ready_mark(c, bslot);   /* retained content is deliverable at once */
    sub_resolve_filter(s, &req->filter, t->has_largest, t->largest_group,
                       t->largest_object);
    moqr_intent_t *it = intent_push(c, MOQR_INTENT_ACCEPT_SUB);
    it->binding_cookie = b->cookie;
    it->cookie = req->cookie;
    it->sub = sub_handle(c, sslot);
    it->has_largest = t->has_largest;
    it->largest_group = t->largest_group;
    it->largest_object = t->largest_object;

    if (t->state == R_TRACK_WARM) {
        /* Re-establish upstream if a publisher is announced. */
        uint32_t pub_node = trie_longest_announce(c, req->ns);
        if (pub_node != UINT32_MAX) {
            t->state = R_TRACK_PENDING;
            t->source_kind = R_TRACK_SOURCE_PULL;
            t->track_gen++;
            t->has_upstream_binding = true;
            t->up_binding = c->nodes[pub_node].ann_binding;
            t->up_binding_gen = c->nodes[pub_node].ann_binding_gen;
            t->src_ann_node = pub_node;
            t->src_ann_binding = c->nodes[pub_node].ann_binding;
            t->src_ann_binding_gen = c->nodes[pub_node].ann_binding_gen;
            moqr_intent_t *up = intent_push(c,
                                            MOQR_INTENT_UPSTREAM_SUBSCRIBE);
            up->binding_cookie = c->bindings[t->up_binding].cookie;
            up->track = track_handle(c, tslot);
            up->track_gen = t->track_gen;
            intent_set_ns_from_key(up, &t->key);
            r_trace(c, MOQR_TRACE_UPSTREAM_SUBSCRIBE, 1 /* warm rejoin */,
                    tslot, t->track_gen, bslot, 0);
        }
    }
    *out = sub_handle(c, sslot);
    return MOQR_OK;
}

moqr_result_t
moqr_core_unsubscribe(moqr_core_t *c, moqr_sub_t sh, uint64_t now_us)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t slot = 0;
    r_sub_t *s = sub_resolve(c, sh, &slot);
    if (s == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    sub_retire(c, slot, now_us);
    return MOQR_OK;
}

moqr_result_t
moqr_core_sub_set_forward(moqr_core_t *c, moqr_sub_t sh, bool forward,
                          uint64_t now_us)
{
    (void)now_us;
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t slot = 0;
    r_sub_t *s = sub_resolve(c, sh, &slot);
    if (s == NULL) {
        return MOQR_ERR_STALE_HANDLE;   /* retired/unknown/foreign: fail closed */
    }
    if (s->forward == forward) {
        return MOQR_OK;   /* idempotent: no state change, no spurious re-arm */
    }
    s->forward = forward;
    r_binding_t *b = &c->bindings[s->binding];
    if (!forward) {
        /* Pausing: release this sub's own held ordinary-RECORD delivery NOW so
         * its pins free and the binding never sits monopolized while paused
         * (its siblings keep flowing). The cursor is left unadvanced — resume
         * re-derives. A held MAINTENANCE delivery (recordless notice or an
         * abandoned/evict reset) is NOT dropped: stream cleanup stays eligible
         * while paused so a downstream FIN/RESET is not deferred to resume. */
        if (b->out_active && b->out_sub == slot &&
            b->out_notice == MOQR_DELIVERY_NOTICE_NONE && !b->out_abandoned) {
            binding_drop_held(b);
        }
    } else {
        /* Resuming: re-arm the owning binding so the released/held work is
         * reconsidered on the next pump and delivered exactly once. */
        ready_mark(c, s->binding);
    }
    return MOQR_OK;
}

moqr_result_t
moqr_core_delivery_note_emitted(moqr_core_t *c, moqr_binding_t bh,
                                uint32_t emitted, uint64_t now_us)
{
    (void)now_us;
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    /* Only a live outstanding chunked delivery carries a begun watermark;
     * anything else (idle, whole-object, notice, abandoned-reset) has no
     * per-chunk progress to record. The CALL itself means "the object header is
     * begun downstream" — including emitted==0 (begin_object shipped, first
     * chunk not yet), which MUST still be recorded as begun so an abandon/evict
     * while paused surfaces the required RESET rather than being skipped as
     * never-begun. */
    if (!b->out_active || !b->out_chunked) {
        return MOQR_OK;
    }
    /* Contract: emitted is 0..chunk_count. An out-of-range count would become a
     * bogus pin base / re-selection watermark (re-derivation could pin zero
     * chunks and silently advance) — reject it and leave state unchanged. */
    if (emitted > b->out_view.rec.chunk_count) {
        return MOQR_ERR_INVAL;
    }
    r_sub_t *s = &c->subs[b->out_sub];
    if ((s->gen & 1u) == 0) {
        return MOQR_OK;   /* sub retired underneath the hold: nothing to mark */
    }
    r_track_t *t = &c->tracks[s->track];
    if ((t->gen & 1u) == 0 || t->gen != s->track_gen_slot || s->gpos == NULL) {
        return MOQR_OK;   /* group evicted/track gone: pins keep it whole */
    }
    uint32_t slot = gpos_hash_find(s, b->out_group);
    if (slot == R_GPOS_NIL) {
        return MOQR_OK;
    }
    r_gpos_t *pe = gpos_at(s, slot);
    /* Count every accepted checkpoint call (once per WOULD_BLOCK hold — this is
     * a call counter, NOT a per-begun-object transition). */
    c->note_emitted_total++;
    /* Mark this position begun (top bit) — decoupled from the chunk count so a
     * begin-with-zero-chunks is begun without faking a count of one (which would
     * skip chunk 0 on re-derivation). The begun TRANSITION is counted exactly
     * once (word 0 -> nonzero) into the begun accounting; the eventual
     * DELIVERED/ABANDONED balances it. Record the exact emitted count in the low
     * bits, monotonically. */
    uint32_t *w = &gpos_emit(s, pe)[b->out_list];
    if (*w == 0) {
        sub_begun_add(c, s);
    }
    uint32_t cnt = *w & GPOS_EMIT_MASK;
    if (emitted > cnt) {
        cnt = emitted;   /* monotonic: never rewind the count */
    }
    *w = GPOS_BEGUN_BIT | cnt;
    return MOQR_OK;
}

moqr_result_t
moqr_core_sub_track(moqr_core_t *c, moqr_sub_t sh, moqr_track_t *track,
                    uint64_t *track_gen)
{
    if (c == NULL || track == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t slot = 0;
    r_sub_t *s = sub_resolve(c, sh, &slot);
    if (s == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    *track = track_handle(c, s->track);
    if (track_gen != NULL) {
        *track_gen = c->tracks[s->track].track_gen;
    }
    return MOQR_OK;
}

moqr_result_t
moqr_core_sub_state(const moqr_core_t *c, moqr_sub_t sh,
                    moqr_sub_state_t *out)
{
    if (c == NULL || out == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* The same generation-guarded resolution as sub_resolve, on a const
     * view: pool tag, shard tag, live (odd) generation, slot bounds, and
     * the slot's current generation must all match — anything else is a
     * retired or foreign handle, reported STALE, never a stale value. */
    if (moq_handle_pool_tag(sh._opaque) != MOQR_HANDLE_POOL_SUB ||
        moq_handle_session_tag(sh._opaque) != c->shard_tag ||
        (moq_handle_generation(sh._opaque) & 1u) == 0) {
        return MOQR_ERR_STALE_HANDLE;
    }
    uint32_t slot = moq_handle_slot(sh._opaque);
    if (slot >= c->max_subs ||
        c->subs[slot].gen != moq_handle_generation(sh._opaque)) {
        return MOQR_ERR_STALE_HANDLE;
    }
    /* Exact mapping — a live sub is PARKED or ACTIVE by construction; any
     * other value is a corrupted slot and is refused, never guessed. */
    if (c->subs[slot].state == R_SUB_PARKED) {
        *out = MOQR_SUB_PARKED;
    } else if (c->subs[slot].state == R_SUB_ACTIVE) {
        *out = MOQR_SUB_ACTIVE;
    } else {
        return MOQR_ERR_INVAL;
    }
    return MOQR_OK;
}

moqr_result_t
moqr_core_revoke_sub(moqr_core_t *c, moqr_sub_t sh, moqr_pd_desc_t status,
                     uint64_t now_us)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* Reject a corrupt terminal before retiring anything: this subscription must stay live. */
    if (!moqr_pd_desc_valid(status)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t slot = 0;
    r_sub_t *s = sub_resolve(c, sh, &slot);
    if (s == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    /* Reserve intent space BEFORE mutating: a relay-initiated termination must
     * tell the subscriber (SUBSCRIBE_DONE), so the sub is never retired unless
     * the DONE is queued. Unlike moqr_core_unsubscribe (silent local retire),
     * this pushes MOQR_INTENT_SUB_DONE -> moq_session_done_subscribe. */
    if (!intent_space(c, 1)) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    uint64_t bcookie = 0;
    if (s->binding < c->max_bindings &&
        c->bindings[s->binding].gen == s->binding_gen) {
        bcookie = c->bindings[s->binding].cookie;
    }
    uint64_t scookie = s->cookie;
    moqr_intent_t *it = intent_push(c, MOQR_INTENT_SUB_DONE);
    it->binding_cookie = bcookie;
    it->cookie = scookie;
    it->sub = sh;
    it->pd = status;   /* already tagged by the caller; never re-derived */
    sub_retire(c, slot, now_us);
    return MOQR_OK;
}

/* -- ops: upstream resolution ------------------------------------------------------------------------ */

moqr_result_t
moqr_core_upstream_ok(moqr_core_t *c, moqr_track_t th, uint64_t track_gen,
                      uint64_t upstream_cookie, bool has_largest,
                      uint64_t lg, uint64_t lo)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL || t->track_gen != track_gen) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->state != R_TRACK_PENDING) {
        return MOQR_ERR_WRONG_STATE;
    }
    /* Reserve: one ACCEPT per parked sub. */
    uint32_t parked = 0;
    for (uint32_t i = 0; i < c->max_subs; i++) {
        if ((c->subs[i].gen & 1u) != 0 && c->subs[i].track == tslot &&
            c->subs[i].track_gen_slot == t->gen &&
            c->subs[i].state == R_SUB_PARKED) {
            parked++;
        }
    }
    if (!intent_space(c, parked)) {
        return MOQR_ERR_WOULD_BLOCK;
    }

    t->state = R_TRACK_ACTIVE;
    t->upstream_cookie = upstream_cookie;
    if (has_largest &&
        (!t->has_largest || lg > t->largest_group ||
         (lg == t->largest_group && lo > t->largest_object))) {
        t->has_largest = true;
        t->largest_group = lg;
        t->largest_object = lo;
    }
    r_trace(c, MOQR_TRACE_UPSTREAM_RESOLVE, 0 /* ok */, tslot, track_gen,
            parked, 0);

    for (uint32_t i = 0; i < c->max_subs; i++) {
        r_sub_t *s = &c->subs[i];
        if ((s->gen & 1u) == 0 || s->track != tslot ||
            s->track_gen_slot != t->gen || s->state != R_SUB_PARKED) {
            continue;
        }
        moqr_filter_t f = {
            .type = s->filter_type,
            .start_group = s->start_group,
            .start_object = s->start_object,
            .end_group_delta = s->end_group,
        };
        s->state = R_SUB_ACTIVE;
        ready_mark(c, s->binding);
        sub_resolve_filter(s, &f, t->has_largest, t->largest_group,
                           t->largest_object);
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_ACCEPT_SUB);
        it->binding_cookie = c->bindings[s->binding].cookie;
        it->cookie = s->cookie;
        it->sub = sub_handle(c, i);
        it->has_largest = t->has_largest;
        it->largest_group = t->largest_group;
        it->largest_object = t->largest_object;
    }
    return MOQR_OK;
}

moqr_result_t
moqr_core_upstream_error(moqr_core_t *c, moqr_track_t th, uint64_t track_gen,
                         uint64_t error_code, uint64_t now_us)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL || t->track_gen != track_gen) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->state != R_TRACK_PENDING) {
        return MOQR_ERR_WRONG_STATE;
    }
    uint32_t parked = 0;
    for (uint32_t i = 0; i < c->max_subs; i++) {
        if ((c->subs[i].gen & 1u) != 0 && c->subs[i].track == tslot &&
            c->subs[i].track_gen_slot == t->gen &&
            c->subs[i].state == R_SUB_PARKED) {
            parked++;
        }
    }
    if (!intent_space(c, parked)) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    r_trace(c, MOQR_TRACE_UPSTREAM_RESOLVE, 1 /* error */, tslot, track_gen,
            parked, error_code);
    for (uint32_t i = 0; i < c->max_subs; i++) {
        r_sub_t *s = &c->subs[i];
        if ((s->gen & 1u) == 0 || s->track != tslot ||
            s->track_gen_slot != t->gen || s->state != R_SUB_PARKED) {
            continue;
        }
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_REJECT_SUB);
        it->binding_cookie = c->bindings[s->binding].cookie;
        it->cookie = s->cookie;
        it->error_code = error_code;
        sub_retire(c, i, now_us);
    }
    t->has_upstream_binding = false;
    t->source_kind = R_TRACK_SOURCE_NONE;
    moqr_log_stats_t ls;
    moqr_log_get_stats(t->log, &ls);
    if (ls.record_count == 0 && track_sub_count(c, tslot) == 0 &&
        track_fetch_count(c, tslot) == 0) {
        track_free_slot(c, tslot);
    } else {
        t->state = R_TRACK_WARM;
    }
    return MOQR_OK;
}

moqr_result_t
moqr_core_upstream_cancel(moqr_core_t *c, moqr_track_t th, uint64_t track_gen,
                          uint64_t now_us)
{
    (void)now_us;
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL || t->track_gen != track_gen) {
        return MOQR_ERR_STALE_HANDLE;
    }
    /* Valid only on a PENDING track NOBODY subscribes to — in any sub state:
     * a WARM rejoin ACCEPTs its subscriber and only then re-enters PENDING,
     * so live subscribers can exist on a track with zero parked ones. */
    if (t->state != R_TRACK_PENDING || track_sub_count(c, tslot) != 0) {
        return MOQR_ERR_WRONG_STATE;
    }
    /* Preflight the single unsubscribe intent BEFORE any mutation. */
    if (!intent_space(c, 1)) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    r_trace(c, MOQR_TRACE_UPSTREAM_RESOLVE, 2 /* cancelled */, tslot,
            track_gen, 0, 0);
    moqr_intent_t *it = intent_push(c, MOQR_INTENT_UPSTREAM_UNSUBSCRIBE);
    it->track = th;
    it->track_gen = t->track_gen;
    it->cookie = t->upstream_cookie;
    if (t->has_upstream_binding && (c->bindings[t->up_binding].gen & 1u) != 0) {
        it->binding_cookie = c->bindings[t->up_binding].cookie;
    }
    /* Release: bump identity so a late upstream answer is STALE, then mirror
     * the source-release rules — free when nothing is retained, else WARM
     * (a cancelled rejoin must keep the retained log it was rejoining for). */
    t->has_upstream_binding = false;
    t->source_kind = R_TRACK_SOURCE_NONE;
    t->track_gen++;
    moqr_log_stats_t ls;
    moqr_log_get_stats(t->log, &ls);
    if (ls.record_count == 0 && track_fetch_count(c, tslot) == 0) {
        track_free_slot(c, tslot);
    } else {
        t->state = R_TRACK_WARM;
    }
    return MOQR_OK;
}

/* -- ops: publish push / ingest / status / capacity ---------------------------------------------------- */

/* Assign a pushed track's source-announce identity: the longest announce for the
 * pushed namespace, but ONLY when it is owned by the pushing binding itself. An
 * unannounced push — or one whose longest announce belongs to a different binding
 * — has no announce source (UINT32_MAX) and is not coupled to any announce
 * withdrawal, while a publisher that both announced and pushed is still purged. */
static void
track_set_push_source(moqr_core_t *c, r_track_t *t, moqr_ns_t ns, uint32_t bslot,
                      uint32_t bgen)
{
    uint32_t node = trie_longest_announce(c, ns);
    if (node != UINT32_MAX && c->nodes[node].ann_binding == bslot &&
        c->nodes[node].ann_binding_gen == bgen) {
        t->src_ann_node = node;
        t->src_ann_binding = bslot;
        t->src_ann_binding_gen = bgen;
    } else {
        t->src_ann_node = UINT32_MAX;
    }
}

moqr_result_t
moqr_core_publish_open(moqr_core_t *c, moqr_binding_t bh, moqr_ns_t ns,
                       moq_bytes_t name, uint64_t cookie, moqr_track_t *out)
{
    if (c == NULL || out == NULL || !ftn_view_ok(ns, name)) {
        return MOQR_ERR_INVAL;
    }
    *out = MOQR_TRACK_INVALID;
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (!intent_space(c, 1)) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    r_key_t key = { NULL, 0 };
    moqr_result_t rc = key_build(c, ns, name, &key);
    if (rc != MOQR_OK) {
        return rc;
    }
    uint64_t hash = key_hash(&key);
    uint32_t tslot = track_find(c, &key, hash);
    if (tslot != UINT32_MAX) {
        key_release(c, &key);
        r_track_t *t = &c->tracks[tslot];
        if (t->state != R_TRACK_WARM) {
            moqr_intent_t *it = intent_push(c, MOQR_INTENT_REJECT_PUBLISH);
            it->binding_cookie = b->cookie;
            it->cookie = cookie;
            it->error_code = R_ERR_INTERNAL;   /* duplicate active source */
            return MOQR_OK;
        }

        /* A warm pushed track can still carry retained content from a previous
         * source. Reacquiring the same full track name with PUBLISH creates a
         * new source generation: old readers are terminated/staled, the log is
         * replaced, and group 0 is compared against an empty generation. */
        uint32_t old_subs = track_sub_count(c, tslot);
        if (!intent_space(c, old_subs + 1u)) {
            return MOQR_ERR_WOULD_BLOCK;
        }
        moqr_log_t *new_log = NULL;
        rc = track_make_log(c, tslot, &new_log);
        if (rc != MOQR_OK) {
            return rc;
        }
        uint32_t old_track_gen = t->gen;
        for (uint32_t i = t->subs_head; i != R_SUB_NIL;) {
            uint32_t next = c->subs[i].track_next;
            r_sub_t *s = &c->subs[i];
            if ((s->gen & 1u) != 0 && s->track == tslot &&
                s->track_gen_slot == old_track_gen) {
                uint64_t bcookie = 0;
                if (s->binding < c->max_bindings &&
                    c->bindings[s->binding].gen == s->binding_gen) {
                    bcookie = c->bindings[s->binding].cookie;
                }
                moqr_intent_t *it = intent_push(c, MOQR_INTENT_SUB_DONE);
                it->binding_cookie = bcookie;
                it->cookie = s->cookie;
                it->sub = sub_handle(c, i);
                it->error_code = R_DONE_TRACK_ENDED;
                it->pd = core_local_done(MOQR_PD_TRACK_ENDED);
                sub_retire(c, i, 0);
            }
            i = next;
        }
        track_replace_retained_generation(c, tslot, new_log);
        t->state = R_TRACK_ACTIVE;
        t->source_kind = R_TRACK_SOURCE_PUSH;
        t->track_gen++;
        t->has_upstream_binding = true;
        t->up_binding = bslot;
        t->up_binding_gen = b->gen;
        track_set_push_source(c, t, ns, bslot, b->gen);
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_ACCEPT_PUBLISH);
        it->binding_cookie = b->cookie;
        it->cookie = cookie;
        it->track = track_handle(c, tslot);
        it->track_gen = t->track_gen;
        *out = it->track;
        return MOQR_OK;
    }
    rc = track_create(c, &key, hash, &tslot);
    if (rc != MOQR_OK) {
        key_release(c, &key);
        return rc;
    }
    r_track_t *t = &c->tracks[tslot];
    t->state = R_TRACK_ACTIVE;
    t->source_kind = R_TRACK_SOURCE_PUSH;
    t->track_gen = 1;
    t->has_upstream_binding = true;
    t->up_binding = bslot;
    t->up_binding_gen = b->gen;
    track_set_push_source(c, t, ns, bslot, b->gen);
    moqr_intent_t *it = intent_push(c, MOQR_INTENT_ACCEPT_PUBLISH);
    it->binding_cookie = b->cookie;
    it->cookie = cookie;
    it->track = track_handle(c, tslot);
    it->track_gen = t->track_gen;
    *out = it->track;
    return MOQR_OK;
}

moqr_result_t
moqr_core_ingest(moqr_core_t *c, moqr_track_t th,
                 const moqr_log_append_desc_t *desc)
{
    if (c == NULL || desc == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->state != R_TRACK_ACTIVE) {
        return MOQR_ERR_WRONG_STATE;
    }
    /* OPEN begins an incrementally-filled record (chunk-through): the object is
     * not yet complete, so it does NOT advance the track's largest -- that waits
     * for moqr_core_complete_record. Whole-object COMPLETE appends advance it as
     * before. */
    if (desc->obj_state == MOQR_OBJ_OPEN) {
        moqr_result_t orc = moqr_log_open_record(t->log, desc);
        if (orc == MOQR_OK) {
            ready_mark_track(c, tslot);   /* a live-edge head is selectable */
        }
        return orc;
    }
    moqr_result_t rc = moqr_log_append(t->log, desc);
    if (rc != MOQR_OK) {
        return rc;
    }
    if (!t->has_largest || desc->group_id > t->largest_group ||
        (desc->group_id == t->largest_group &&
         desc->object_id > t->largest_object)) {
        t->has_largest = true;
        t->largest_group = desc->group_id;
        t->largest_object = desc->object_id;
    }
    c->ingested_total++;
    ready_mark_track(c, tslot);
    return MOQR_OK;
}

/* Chunk-through ingest wrappers: resolve the track (must be ACTIVE) and forward
 * to the log's chunk-through APIs. Continuation chunks are routed by (group, subgroup,
 * object) -- the identity every OBJECT_CHUNK now carries. */
moqr_result_t
moqr_core_append_chunk(moqr_core_t *c, moqr_track_t th, uint64_t group_id,
                       uint64_t subgroup_id, uint64_t object_id,
                       moq_rcbuf_t *chunk)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->state != R_TRACK_ACTIVE) {
        return MOQR_ERR_WRONG_STATE;
    }
    moqr_result_t rc =
        moqr_log_append_chunk(t->log, group_id, subgroup_id, object_id, chunk);
    if (rc == MOQR_OK) {
        ready_mark_track(c, tslot);   /* growth resumes a stalled delivery */
    }
    return rc;
}

moqr_result_t
moqr_core_complete_record(moqr_core_t *c, moqr_track_t th, uint64_t group_id,
                          uint64_t subgroup_id, uint64_t object_id)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->state != R_TRACK_ACTIVE) {
        return MOQR_ERR_WRONG_STATE;
    }
    moqr_result_t rc =
        moqr_log_complete_record(t->log, group_id, subgroup_id, object_id);
    if (rc != MOQR_OK) {
        return rc;
    }
    /* Now a delivered/fetchable object: advance the track largest (deferred from
     * open). */
    if (!t->has_largest || group_id > t->largest_group ||
        (group_id == t->largest_group && object_id > t->largest_object)) {
        t->has_largest = true;
        t->largest_group = group_id;
        t->largest_object = object_id;
    }
    c->ingested_total++;
    ready_mark_track(c, tslot);
    return MOQR_OK;
}

moqr_result_t
moqr_core_abandon_record(moqr_core_t *c, moqr_track_t th, uint64_t group_id,
                         uint64_t subgroup_id, uint64_t object_id,
                         moqr_reset_desc_t reset)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* Reject a corrupt descriptor before anything is resolved or
     * mutated: the record must stay OPEN if we cannot state its terminal. */
    if (!moqr_reset_desc_valid(reset)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    /* ACTIVE or WARM: a pure log mutation. WARM matters — the idle
     * ACTIVE->WARM transition happens before the upstream unsubscribe is
     * even visible to the source, and a WARM log KEEPS its records, so the
     * source's teardown must still be able to scrub a partially ingested
     * OPEN record (a later rejoin must never meet stale partial state).
     * The handle's pool generation resolved above rejects a freed-and-reused
     * slot. Nothing more is enforced here: correlating the abandon with the
     * current source attempt (and serializing it against rejoins) is the
     * caller's job — the shard teardown runs it in the same single-writer
     * step that observed the WARM transition. */
    if (t->state != R_TRACK_ACTIVE && t->state != R_TRACK_WARM) {
        return MOQR_ERR_WRONG_STATE;
    }
    moqr_result_t rc = moqr_log_abandon_record(t->log, group_id, subgroup_id,
                                               object_id, reset);
    if (rc == MOQR_OK) {
        ready_mark_track(c, tslot);   /* the reset notice is deliverable */
    }
    return rc;
}

moqr_result_t
moqr_core_seal_subgroup(moqr_core_t *c, moqr_track_t th, uint64_t group_id,
                        uint64_t subgroup_id)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* The handle's pool generation rejects a freed-and-reused slot. Nothing
     * more is enforced here: correlating the seal with the current source
     * attempt (and serializing it against rejoins) is the caller's job. */
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    moqr_result_t rc = moqr_log_seal_subgroup(t->log, group_id, subgroup_id);
    if (rc == MOQR_OK) {
        ready_mark_track(c, tslot);   /* the seal notice is deliverable */
    }
    return rc;
}

moqr_result_t
moqr_core_reset_subgroup(moqr_core_t *c, moqr_track_t th, uint64_t group_id,
                         uint64_t subgroup_id, moqr_reset_desc_t reset)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* Reject a corrupt descriptor before anything is resolved or
     * mutated: the subgroup must keep its own terminal if we cannot state this one. */
    if (!moqr_reset_desc_valid(reset)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    moqr_result_t rc =
        moqr_log_reset_subgroup(t->log, group_id, subgroup_id, reset);
    if (rc == MOQR_OK) {
        ready_mark_track(c, tslot);   /* the reset notice is deliverable */
    }
    return rc;
}

moqr_result_t
moqr_core_note_track_evicted_below(moqr_core_t *c, moqr_track_t th,
                                   uint64_t oldest_retained)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    (void)moqr_log_note_evicted_below(t->log, oldest_retained);
    return MOQR_OK;
}

moqr_result_t
moqr_core_abandon_group_open(moqr_core_t *c, moqr_track_t th,
                             uint64_t group_id, moqr_reset_desc_t reset)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* Reject a corrupt descriptor before anything is resolved or
     * mutated: the group's open records must stay completable. */
    if (!moqr_reset_desc_valid(reset)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->state != R_TRACK_ACTIVE && t->state != R_TRACK_WARM) {
        return MOQR_ERR_WRONG_STATE;
    }
    moqr_result_t rc =
        moqr_log_abandon_group_open(t->log, group_id, reset, NULL);
    if (rc == MOQR_OK) {
        ready_mark_track(c, tslot);   /* any reset notice is deliverable */
    }
    return rc;
}

moqr_result_t
moqr_core_track_status(moqr_core_t *c, moqr_binding_t bh, moqr_ns_t ns,
                       moq_bytes_t name, uint64_t cookie)
{
    if (c == NULL || !ftn_view_ok(ns, name)) {
        return MOQR_ERR_INVAL;
    }
    r_binding_t *b = binding_resolve(c, bh, NULL);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (!intent_space(c, 1)) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    r_key_t key = { NULL, 0 };
    moqr_result_t rc = key_build(c, ns, name, &key);
    if (rc != MOQR_OK) {
        return rc;
    }
    uint32_t tslot = track_find(c, &key, key_hash(&key));
    key_release(c, &key);
    if (tslot == UINT32_MAX) {
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_TRACK_STATUS_ERROR);
        it->binding_cookie = b->cookie;
        it->cookie = cookie;
        it->error_code = R_ERR_DOES_NOT_EXIST;
        return MOQR_OK;
    }
    r_track_t *t = &c->tracks[tslot];
    if (t->state == R_TRACK_PENDING) {
        /* No established knowledge yet; answering OK would fabricate
         * status (forwarding the request upstream is future work). */
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_TRACK_STATUS_ERROR);
        it->binding_cookie = b->cookie;
        it->cookie = cookie;
        it->error_code = R_ERR_DOES_NOT_EXIST;
        return MOQR_OK;
    }
    moqr_intent_t *it = intent_push(c, MOQR_INTENT_TRACK_STATUS_OK);
    it->binding_cookie = b->cookie;
    it->cookie = cookie;
    it->has_largest = t->has_largest;
    it->largest_group = t->largest_group;
    it->largest_object = t->largest_object;
    return MOQR_OK;
}

/* -- grant revalidation (driven off tick) --------------------------------------------------------- */

/* Re-check request built from a grant's COPIED material (the original session
 * event is long gone), for the revalidation hook. */
static moqr_auth_request_t
grant_auth_req(const r_grant *g, uint64_t now_us)
{
    moqr_auth_request_t req;
    memset(&req, 0, sizeof(req));
    req.struct_size = (uint32_t)sizeof(req);
    req.action = g->action;
    req.ns.parts = g->mat.ns_parts;
    req.ns.count = g->mat.ns_count;
    req.name.data = g->mat.name;
    req.name.len = g->mat.name_len;
    req.tokens = g->mat.tokens;
    req.token_count = g->mat.token_count;
    req.binding_cookie = g->binding_cookie;
    req.now_us = now_us;
    return req;
}

/* Execute (or continue) the teardown of a revoked grant. Subscribe revocation
 * is fully core-driven (revoke_sub -> SUB_DONE intent); announce revocation
 * is a FULL withdrawal — a namespace whose authorization was revoked must not
 * keep sourcing tracks, so the teardown runs moqr_core_force_withdraw: every
 * sourced track is purged (subscriber terminals carry the revoke status,
 * which is what terminates a remote demand's pump-sub too), the announce
 * clears with NS_GONE fan-out, and the binding then sends the publisher-side
 * cancel_namespace (peek/ack_revoked_grant). The grant itself is the cancel
 * vehicle: it is marked unannounced BEFORE the withdrawal so force_withdraw's
 * pending-cancel dedupe leaves the cancel to this grant's own peek/ack.
 * Retryable: WOULD_BLOCK (intent ring / cancel queue) leaves the grant marked
 * for the next tick. The withdrawal itself is resumable, so some of the
 * grant's tracks may already be purged when that happens; re-arming the cancel
 * vehicle is safe because the retry resumes from the tracks that remain and
 * performs the final namespace effects exactly once, at the end. */
static void
grant_do_teardown(moqr_core_t *c, uint32_t slot, uint64_t now_us)
{
    r_grant *g = &c->grants[slot];
    if (g->action == MOQR_AUTH_SUBSCRIBE) {
        moqr_result_t rc = moqr_core_revoke_sub(c, (moqr_sub_t){ g->sub_raw },
                                                g->revoke_pd, now_us);
        if (rc != MOQR_ERR_WOULD_BLOCK) {
            grant_free_slot(c, slot); /* SUB_DONE queued, or the sub was gone */
        }
        return; /* WOULD_BLOCK: g->revoked stays set, retry next tick */
    }
    /* announce */
    if (!g->unannounced) {
        moqr_ns_t ns = { g->mat.ns_parts, g->mat.ns_count };
        /* Ownership guard: this grant may be STALE — its announce already
         * withdrawn and the key re-announced by a DIFFERENT publisher.
         * Only the exact announce this grant authorized ({binding_cookie,
         * session_cookie}) may be withdrawn; anything else retires the
         * stale grant and touches NOTHING — never a replacement
         * publisher's route, tracks, or cancellation state. */
        uint32_t node = trie_walk(c, ns);
        if (node == UINT32_MAX || !c->nodes[node].has_announce ||
            c->bindings[c->nodes[node].ann_binding].cookie !=
                g->binding_cookie ||
            c->nodes[node].session_cookie != g->session_cookie) {
            grant_free_slot(c, slot);
            return;
        }
        g->unannounced = true;   /* the cancel vehicle: dedupes the
                                  * withdrawal's own pending-cancel slot */
        moqr_result_t rc = moqr_core_force_withdraw(c, ns, g->revoke_request_error,
                                                    now_us);
        if (rc == MOQR_ERR_WOULD_BLOCK) {
            g->unannounced = false;   /* re-arm the vehicle; retry next tick */
            return;
        }
    }
    /* Now waiting for the binding's cancel_namespace via peek/ack_revoked_grant. */
}

static void
grant_tick(moqr_core_t *c, uint64_t now_us)
{
    for (uint32_t i = 0; i < c->max_grants; i++) {
        r_grant *g = &c->grants[i];
        if ((g->gen & 1u) == 0 || !g->committed) {
            continue; /* free, or reserved-but-not-committed */
        }
        if (g->revoked) {
            grant_do_teardown(c, i, now_us); /* retry pending teardown */
            continue;
        }
        if (g->next_recheck_us > now_us) {
            continue; /* not due */
        }
        moqr_auth_request_t req = grant_auth_req(g, now_us);
        moqr_auth_verdict_t v;
        moqr_core_authorize(c, &req, &v); /* counts + traces the reval decision */
        if (v.decision == MOQR_AUTH_ALLOW && v.revalidate_after_us > 0) {
            g->lease_us = v.revalidate_after_us; /* renew the lease */
            g->next_recheck_us = now_us > UINT64_MAX - g->lease_us
                                     ? UINT64_MAX
                                     : now_us + g->lease_us;
            continue;
        }
        if (v.decision == MOQR_AUTH_ALLOW) {
            grant_free_slot(c, i); /* lease 0: stop revalidating */
            continue;
        }
        /* DENY / DEFER / garbage -> revoke, fail closed. moqr_core_authorize
         * canonicalizes decision/reason but NOT error_code, so a DENY/DEFER
         * (or garbage) verdict may carry error_code == 0. A zero code would
         * ride out as a normal "range complete" / clean cancel on the wire, so
         * default it to UNAUTHORIZED while preserving any custom DENY code. */
        /* Build the terminal before anything is marked revoked: a denial we
         * cannot state truthfully must not leave a half-torn-down grant.
         *
         * The terminal comes from the verdict's TAGGED field, never from
         * `error_code`: that scalar is REQUEST_ERROR-domain, and reading it as
         * a PUBLISH_DONE status would be exactly the numeric coincidence the
         * tagged descriptors exist to prevent. NONE or an invalid descriptor
         * degrades to the truthful UNAUTHORIZED meaning. */
        moqr_pd_desc_t rpd = v.revoke_terminal;

        if (!moqr_pd_desc_valid(rpd) &&
            moqr_pd_desc_local(MOQR_PD_UNAUTHORIZED, &rpd) != MOQR_OK) {
            continue;   /* cannot state a terminal: leave the grant alone */
        }
        g->revoked = true;
        g->revoke_request_error =
            v.error_code != 0 ? v.error_code : R_ERR_UNAUTHORIZED;
        g->revoke_pd = rpd;
        grant_do_teardown(c, i, now_us);
    }
}

/* -- tick ------------------------------------------------------------------------------------------------ */

moqr_result_t
moqr_core_tick(moqr_core_t *c, uint64_t now_us)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    grant_tick(c, now_us);
    for (uint32_t i = 0; i < c->max_tracks; i++) {
        r_track_t *t = &c->tracks[i];
        if ((t->gen & 1u) == 0) {
            continue;
        }
        /* An open fetch pins the track's retained range against AGE eviction:
         * a fetch accepted a fully-retained range and must be able to drain it.
         * (Capacity eviction from new ingest is ingest-driven, not here, and
         * still surfaces as the cur_group < oldest TOO_OLD terminal in peek.) */
        if (track_fetch_count(c, i) == 0) {
            (void)moqr_log_tick(t->log, now_us);
        }
        if (t->state == R_TRACK_ACTIVE &&
            t->source_kind == R_TRACK_SOURCE_PULL &&
            t->linger_deadline_us != 0 &&
            now_us >= t->linger_deadline_us &&
            track_sub_count(c, i) == 0) {
            if (!intent_space(c, 1)) {
                continue;   /* retry next tick; deterministic given inputs */
            }
            moqr_intent_t *it = intent_push(c,
                                            MOQR_INTENT_UPSTREAM_UNSUBSCRIBE);
            it->track = track_handle(c, i);
            it->track_gen = t->track_gen;
            it->cookie = t->upstream_cookie;
            if (t->has_upstream_binding &&
                (c->bindings[t->up_binding].gen & 1u) != 0) {
                it->binding_cookie = c->bindings[t->up_binding].cookie;
            }
            t->linger_deadline_us = 0;
            t->has_upstream_binding = false;
            t->source_kind = R_TRACK_SOURCE_NONE;
            t->track_gen++;
            t->state = R_TRACK_WARM;
        }
        if (t->state == R_TRACK_WARM && track_sub_count(c, i) == 0 &&
            track_fetch_count(c, i) == 0) {
            moqr_log_stats_t ls;
            moqr_log_get_stats(t->log, &ls);
            if (ls.record_count == 0) {
                track_free_slot(c, i);
            }
        }
    }
    return MOQR_OK;
}

/* -- delivery --------------------------------------------------------------------------------------------- */

static bool
rec_passes(const r_sub_t *s, const moqr_record_view_t *v)
{
    /* rec_passes is the COMPLETE-record filter predicate: whole-object/status
     * records (chunk_count == 0, payload in the view) and COMPLETE-but-chunked
     * records (chunk_count > 0, payload NULL) pass; OPEN and ABANDONED do not.
     * OPEN live-edge delivery and ABANDONED-after-begin reset are handled by the
     * scheduling path AROUND this helper (sub_best_candidate selects an OPEN head
     * with new chunks and surfaces a begun ABANDONED head for reset, which
     * carries the terminal reset code forwarded downstream). */
    if (v->obj_state != MOQR_OBJ_COMPLETE) {
        return false;
    }
    if (v->group_id < s->start_group ||
        (v->group_id == s->start_group && v->object_id < s->start_object)) {
        return false;
    }
    return true;
}

/* Group comparison honoring the sub's order preference (spec rule 3). */
static int
group_cmp(const r_sub_t *s, uint64_t a, uint64_t b)
{
    if (a == b) {
        return 0;
    }
    bool asc = s->group_order != MOQR_GROUP_ORDER_DESCENDING;
    return (a < b) == asc ? -1 : 1;
}

typedef struct r_cand {
    bool               have;
    uint32_t           list;      /* idx[] slot to advance on DELIVERED   */
    uint32_t           emitted;   /* this sub's live-edge watermark for the
                                   * list: the batch (and pin) base        */
    moqr_record_view_t view;
} r_cand_t;

/* Spec §7.1.1 within one subscription: publisher priority, then group
 * order, then lowest subgroup, then object/arrival. Datagram-preference
 * candidates rank after subgroup candidates at equal priority (cross-
 * preference order is implementation-dependent, -18 :2399-2401); within
 * the datagram list, delivery follows arrival order (the rule-4 lowest-
 * object tie-break is approximated; datagrams carry no ordering
 * obligation). */
static bool
cand_better(const r_sub_t *s, const r_cand_t *cand, const r_cand_t *best)
{
    if (!best->have) {
        return true;
    }
    const moqr_record_view_t *a = &cand->view, *b = &best->view;
    if (a->publisher_priority != b->publisher_priority) {
        return a->publisher_priority < b->publisher_priority;
    }
    int gc = group_cmp(s, a->group_id, b->group_id);
    if (gc != 0) {
        return gc < 0;
    }
    uint64_t sga = a->datagram_pref ? UINT64_MAX : a->subgroup_id;
    uint64_t sgb = b->datagram_pref ? UINT64_MAX : b->subgroup_id;
    if (sga != sgb) {
        return sga < sgb;
    }
    return a->arrival_seq < b->arrival_seq;
}

/* A pending SEAL notice discovered during the candidate scan: the lowest
 * (group, list) whose subgroup is sealed, fully consumed by this sub, and
 * not yet acknowledged. */
typedef struct r_seal_note {
    bool     have;
    uint64_t group;
    uint32_t list;
    uint64_t subgroup_id;
    bool     reset;        /* the terminal was a RESET: the notice's close
                            * is abnormal, carrying reset_code             */
    moqr_reset_desc_t reset_desc;
} r_seal_note_t;

/* Best deliverable record for one subscription: scan the heads of every
 * retained (group, list) position, consuming records below the filter
 * start in place. Returns false when nothing is deliverable. `out_seal`
 * (optional) collects the lowest pending SEAL notice seen along the way —
 * the scan already reads every list head, so late-FIN detection costs one
 * acked-bit test on drained lists and a sealed lookup only when that bit is
 * clear. */
static bool
sub_best_candidate(moqr_core_t *c, r_sub_t *s, const moqr_log_t *log,
                   r_cand_t *out, bool *out_range_done,
                   r_seal_note_t *out_seal)
{
    uint32_t gn = moqr_log_group_count(log);
    *out_range_done = false;
    r_cand_t best = { .have = false };
    bool any_in_range = false;
    bool blocked_open = false;   /* an in-range OPEN head is pending */

    /* Reclaim this sub's entries for groups evicted since last time (gated on the
     * log's eviction watermark), so gpos_entry below is pure O(1) find-or-add. */
    gpos_gc(c, s, log);

    for (uint32_t gi = 0; gi < gn; gi++) {
        uint64_t g = moqr_log_group_id_at(log, gi);
        if (g < s->start_group) {
            continue;
        }
        if (s->has_end && g > s->end_group) {
            break;   /* ids ascend; nothing further can be in range */
        }
        any_in_range = true;
        r_gpos_t *e = gpos_entry(c, s, log, g);
        if (e == NULL) {
            continue;
        }
        uint32_t lists = moqr_log_group_list_count(log, g);
        for (uint32_t slot = 0; slot <= lists; slot++) {
            bool dg = slot == lists;   /* last position slot = datagrams */
            uint32_t pos_slot = dg ? s->gpos_lists - 1 : slot;
            uint32_t list_ref = dg ? MOQR_LOG_LIST_DATAGRAM : slot;
            for (;;) {
                moqr_record_view_t v;
                moqr_result_t rc = moqr_log_read_rec(log, g, list_ref,
                                                     e->idx[pos_slot], &v);
                if (rc != MOQR_OK) {
                    /* MOQR_DONE: this sub has consumed every retained record
                     * of the list. A SEALED list reached this way owes the
                     * sub a late-FIN notification — the seal landed after
                     * the final record was delivered (or the cursor skipped
                     * past it), so no record delivery will ever carry
                     * subgroup_end. Groups and slots iterate ascending, so
                     * the first hit is the lowest (group, subgroup). */
                    if (rc == MOQR_DONE && !dg && out_seal != NULL &&
                        (!out_seal->have || out_seal->group == g) &&
                        !gpos_seal_acked(s, e, pos_slot)) {
                        /* Groups iterate ascending, so the first group with
                         * an eligible seal wins; WITHIN it, list slots are
                         * insertion-ordered, so take the lowest WIRE
                         * subgroup id across the group's eligible lists. */
                        uint64_t sgid = 0;
                        if (moqr_log_subgroup_sealed(log, g, slot, &sgid) &&
                            (!out_seal->have ||
                             sgid < out_seal->subgroup_id)) {
                            out_seal->have = true;
                            out_seal->group = g;
                            out_seal->list = pos_slot;
                            out_seal->subgroup_id = sgid;
                            /* Both terminal flavors share eligibility and
                             * acknowledgement; the flavor only decides how
                             * the bind closes downstream. */
                            out_seal->reset_desc = moqr_reset_desc_none();
                            out_seal->reset = moqr_log_subgroup_reset(
                                log, g, slot, &out_seal->reset_desc);
                        }
                    }
                    break;
                }
                /* Below the sub's start filter: skip permanently (any state). */
                if (v.group_id < s->start_group ||
                    (v.group_id == s->start_group &&
                     v.object_id < s->start_object)) {
                    e->idx[pos_slot]++;
                    continue;
                }
                uint32_t emit_raw = gpos_emit(s, e)[pos_slot];
                uint32_t emitted = emit_raw & GPOS_EMIT_MASK;   /* chunk count */
                bool begun = emit_raw != 0;   /* header shipped downstream */
                /* Live edge: an OPEN head (the last record in this
                 * list) is selectable only while it has grown past what this sub
                 * already scheduled downstream. Once drained, HOLD the cursor (no
                 * consume) until more chunks arrive or it flips terminal — that
                 * hold is what keeps a publisher-stalled object from blocking the
                 * list, and consuming it would skip the object forever. */
                if (v.obj_state == MOQR_OBJ_OPEN && v.chunk_count <= emitted) {
                    /* In-range OPEN head, drained for now: it is still pending
                     * (more chunks, then complete or abandon), so it blocks range
                     * completion — a bounded sub must not retire while an in-range
                     * object is unfinished (possibly begun downstream). */
                    blocked_open = true;
                    break;
                }
                /* An ABANDONED head this sub NEVER began downstream is skipped
                 * permanently; if it was begun (header shipped — even with zero
                 * chunks emitted) it is selected so the bind can RESET the
                 * downstream subgroup. `begun` is tracked separately from the
                 * chunk count precisely so a begin-with-zero-chunks still resets. */
                if (v.obj_state == MOQR_OBJ_ABANDONED && !begun) {
                    e->idx[pos_slot]++;
                    continue;
                }
                /* Selectable: OPEN with new chunks, COMPLETE (deliver the
                 * remaining emitted..chunk_count then end), or ABANDONED-begun. */
                r_cand_t cand = { .have = true,
                                  .list = pos_slot,
                                  .emitted = emitted,
                                  .view = v };
                if (cand_better(s, &cand, &best)) {
                    best = cand;
                }
                break;
            }
        }
    }
    if (best.have) {
        *out = best;
        return true;
    }
    /* Range completion: the range end was passed by the live edge and no
     * in-range record remains deliverable — but NOT while an in-range OPEN head
     * is still pending (it must complete or be reset/abandoned first, else a
     * begun downstream object would be left without a terminal). */
    if (s->has_end && !blocked_open && gn > 0 &&
        moqr_log_group_id_at(log, gn - 1) > s->end_group) {
        *out_range_done = true;   /* live edge passed the range end */
    }
    (void)any_in_range;
    return false;
}

/* Find a group this sub BEGAN an OPEN object in (some emitted>0) that has since
 * been evicted from the log. The begun downstream object can never complete, so
 * it must be reset. Returns true + sets *out_group; the gpos slot is retained
 * (gpos_entry does not reclaim begun slots) until the reset is confirmed. */
static bool
sub_evicted_begun_group(r_sub_t *s, const moqr_log_t *log, uint64_t *out_group)
{
    if (s->gpos == NULL) {
        return false;
    }
    for (uint32_t i = 0; i < s->gpos_groups; i++) {
        r_gpos_t *e = gpos_at(s, i);
        if (e->group_id == UINT64_MAX) {
            continue;
        }
        bool begun = false;
        for (uint32_t li = 0; li < s->gpos_lists; li++) {
            if (gpos_emit(s, e)[li] > 0) {
                begun = true;
                break;
            }
        }
        if (!begun) {
            continue;
        }
        moqr_record_view_t probe;
        if (moqr_log_read_rec(log, e->group_id, MOQR_LOG_LIST_DATAGRAM, 0,
                              &probe) == MOQR_ERR_TOO_OLD) {
            *out_group = e->group_id;
            return true;
        }
    }
    return false;
}

/* True iff `s` is provably FINISHED on a track that itself ended: the log
 * carries a received END_OF_TRACK, and every in-range position from the
 * sub's effective start to the newest retained group is either delivered or
 * authoritatively closed. "Authoritatively closed" means below the log's
 * eviction floor (which an upstream watermark advances via
 * moqr_core_note_track_evicted_below) — an UNKNOWN gap (a missing group
 * above the floor, or an unsealed subgroup list) blocks retirement: unknown
 * is neither delivered nor nonexistent (draft-18 §2.1), so the subscription
 * must stay live. The caller has already established there is nothing
 * deliverable, no pending skip, and no pending seal notice for this sub.
 * Read-only: no cursor moves, no reclaim, no allocation. */
static bool
sub_eot_complete(moqr_core_t *c, r_sub_t *s, const moqr_log_t *log)
{
    (void)c;
    moqr_log_stats_t ls;
    moqr_log_get_stats(log, &ls);
    if (!ls.end_of_track || ls.group_count == 0) {
        return false;
    }
    uint64_t floor = moqr_log_evicted_floor(log);
    uint64_t expect = s->start_group > floor ? s->start_group : floor;
    uint32_t gn = moqr_log_group_count(log);
    for (uint32_t gi = 0; gi < gn; gi++) {
        uint64_t g = moqr_log_group_id_at(log, gi);
        if (g < expect) {
            continue;   /* below the start filter or the floor: closed */
        }
        if (s->has_end && g > s->end_group) {
            break;      /* beyond the subscribed range: irrelevant */
        }
        if (g != expect) {
            return false;   /* a missing group above the floor: UNKNOWN */
        }
        expect = g + 1;
        uint32_t slot = gpos_hash_find(s, g);
        if (slot == R_GPOS_NIL) {
            return false;   /* untouched retained group: not delivered */
        }
        r_gpos_t *e = gpos_at(s, slot);
        uint32_t lists = moqr_log_group_list_count(log, g);
        for (uint32_t li = 0; li <= lists; li++) {
            bool dg = li == lists;
            uint32_t pos_slot = dg ? s->gpos_lists - 1u : li;
            uint32_t list_ref = dg ? MOQR_LOG_LIST_DATAGRAM : li;
            moqr_record_view_t v;
            if (moqr_log_read_rec(log, g, list_ref, e->idx[pos_slot], &v) ==
                MOQR_OK) {
                return false;   /* undelivered record (defensive: the caller
                                 * already scanned and found nothing) */
            }
            if (!dg) {
                /* A subgroup list must be FINAL: sealed, or ended by its
                 * last record's end_of_group. Unsealed = could still have
                 * grown when the terminal raced the FIN = UNKNOWN. */
                uint64_t sgid = 0;
                if (!moqr_log_subgroup_sealed(log, g, li, &sgid) &&
                    !moqr_log_subgroup_is_final(
                        log, g, li,
                        e->idx[pos_slot] > 0 ? e->idx[pos_slot] - 1u : 0)) {
                    return false;
                }
            }
        }
    }
    return true;
}

moqr_result_t
moqr_core_next_delivery(moqr_core_t *c, moqr_binding_t bh, uint64_t now_us,
                        moqr_delivery_t *out)
{
    if (c == NULL || out == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (b->out_active) {
        /* If the held ordinary-RECORD delivery's own subscription has paused
         * (Forward=0), do NOT replay it — that write would draw
         * MOQ_ERR_WRONG_STATE and wrongly retire the sub, and holding it would
         * monopolize the binding against its siblings. Drop the hold (cursor
         * unadvanced) and fall through to select a sibling; resume re-derives
         * it. A held MAINTENANCE delivery (recordless notice or abandoned/evict
         * reset) is NOT dropped even while paused — stream cleanup must still
         * complete — so it re-peeks below. */
        if ((c->subs[b->out_sub].gen & 1u) != 0 &&
            !c->subs[b->out_sub].forward &&
            b->out_notice == MOQR_DELIVERY_NOTICE_NONE && !b->out_abandoned) {
            binding_drop_held(b);
        } else {
            /* A chunked, abandoned-reset, or notice delivery is held across a
             * downstream WOULD_BLOCK: re-peek is idempotent so the binding
             * resumes the same object's chunk cursor, retries the reset, or
             * re-attempts the notice verbatim. Whole-object deliveries never
             * re-peek (they commit in the same pump), so their outstanding
             * state is still a caller error. */
            if (b->out_chunked || b->out_abandoned ||
                b->out_notice != MOQR_DELIVERY_NOTICE_NONE) {
                *out = b->out_view;
                return MOQR_OK;
            }
            return MOQR_ERR_WRONG_STATE;
        }
    }

    /* Evict-reset sweep (before normal scheduling): if any sub BEGAN an OPEN
     * object downstream whose group has since been evicted, the peer is left with
     * a begun-but-unfinishable object — surface a synthetic ABANDONED delivery so
     * the bind resets every begun subgroup of that group. Higher priority than
     * ordinary delivery so a hang is cleared promptly. */
    /* Walk only this binding's subs (the per-binding sub list), and pick the
     * lowest-slot evicted-begun sub — the same one the former slot-ordered scan
     * returned first. Gate: a begun-evicted group requires a sub with an
     * emitted>0 position, so skip the whole sweep when no sub on the binding has
     * begun anything (begun_subs==0), and skip individual subs that have not
     * (begun_count==0). Conservative: begun_subs>0 with no eviction still enters
     * and finds nothing (a false positive, harmless); it is never a false
     * negative — every begun position is counted, so a reset is never missed.
     * begun_subs==0 is the steady state for whole-object delivery. */
    if (b->begun_subs > 0) {
        c->evict_sweeps++;
        uint32_t evict_sub = R_SUB_NIL;
        uint64_t evict_group = 0;
        for (uint32_t i = b->subs_head; i != R_SUB_NIL;
             i = c->subs[i].sub_next) {
            r_sub_t *s = &c->subs[i];
            if ((s->gen & 1u) == 0 || s->binding != bslot ||
                s->binding_gen != b->gen || s->state != R_SUB_ACTIVE) {
                continue;
            }
            /* NB: a PAUSED (Forward=0) sub is NOT skipped here — a begun-evicted
             * object still needs its downstream reset (stream maintenance is not
             * object transmission), so the evict-reset sweep stays eligible while
             * paused. Only ordinary record scheduling is suppressed (below). */
            if (s->begun_count == 0) {
                continue;   /* no begun position => no begun-evicted group */
            }
            r_track_t *t = &c->tracks[s->track];
            if ((t->gen & 1u) == 0 || t->gen != s->track_gen_slot) {
                continue;
            }
            uint64_t eg = 0;
            if (!sub_evicted_begun_group(s, t->log, &eg)) {
                continue;
            }
            if (evict_sub == R_SUB_NIL || i < evict_sub) {
                evict_sub = i;
                evict_group = eg;
            }
        }
        if (evict_sub != R_SUB_NIL) {
            uint32_t i = evict_sub;
            r_sub_t *s = &c->subs[i];
            b->out_sub = i;
            b->out_group = evict_group;
            b->out_list = 0;           /* reset is by group, not by list */
            b->out_abandoned = true;
            b->out_evict_reset = true;
            b->out_chunked = false;
            b->out_chunk_base = 0;
            b->out_notice = MOQR_DELIVERY_NOTICE_NONE;
            b->pin_payload = NULL;
            b->pin_properties = NULL;
            b->out_active = true;
            memset(out, 0, sizeof(*out));
            out->sub = sub_handle(c, i);
            out->sub_cookie = s->cookie;
            out->oldest_group = UINT64_MAX;
            out->rec.group_id = evict_group;
            out->rec.obj_state = MOQR_OBJ_ABANDONED;
            /* Eviction is this relay's own cause, never a draft's number:
             * build it as a local meaning before anything is staged. */
            if (moqr_reset_desc_internal(MOQR_RESET_CODE_EVICTED,
                                         &out->rec.reset) != MOQR_OK) {
                return MOQR_ERR_INTERNAL;
            }
            out->evicted_reset = true;
            b->out_view = *out;
            return MOQR_OK;
        }
    }

    bool have = false;
    uint32_t best_slot = 0;
    r_cand_t best;
    memset(&best, 0, sizeof(best));
    /* Pending recordless notice. A sub carries at most one notice per pass —
     * an unacknowledged eviction watermark (which also blocks that sub's
     * record work: its positions are not trustworthy until the downstream
     * closed what the jump orphaned), else its lowest pending seal — and the
     * notice IS that sub's candidate in the ordinary cross-subscription
     * schedule: subscriber priority first, then sub slot among notices. A
     * notice never blocks a SIBLING subscription's records; it only outranks
     * a record at equal subscriber priority (it is payload-free and unblocks
     * a downstream FIN/close). */
    uint32_t ntc_sub = R_SUB_NIL;
    uint8_t  ntc_kind = MOQR_DELIVERY_NOTICE_NONE;
    r_seal_note_t ntc_seal;
    memset(&ntc_seal, 0, sizeof(ntc_seal));
#define NTC_BETTER(c, i, ntc)                                              \
    ((ntc) == R_SUB_NIL ||                                                 \
     (c)->subs[(i)].subscriber_priority <                                  \
         (c)->subs[(ntc)].subscriber_priority ||                           \
     ((c)->subs[(i)].subscriber_priority ==                                \
          (c)->subs[(ntc)].subscriber_priority &&                          \
      (i) < (ntc)))

    for (uint32_t i = b->subs_head; i != R_SUB_NIL;) {
        uint32_t next = c->subs[i].sub_next;   /* save: sub_retire may unlink i */
        r_sub_t *s = &c->subs[i];
        if ((s->gen & 1u) == 0 || s->binding != bslot ||
            s->binding_gen != b->gen || s->state != R_SUB_ACTIVE) {
            i = next;
            continue;
        }
        r_track_t *t = &c->tracks[s->track];
        if ((t->gen & 1u) == 0 || t->gen != s->track_gen_slot) {
            i = next;
            continue;
        }
        if (s->pending_skip) {
            if (NTC_BETTER(c, i, ntc_sub)) {
                ntc_sub = i;
                ntc_kind = MOQR_DELIVERY_NOTICE_EVICT_WATERMARK;
            }
            i = next;
            continue;   /* records blocked until the watermark is acked */
        }
        r_cand_t cand;
        bool range_done = false;
        r_seal_note_t seal;
        memset(&seal, 0, sizeof(seal));
        if (!sub_best_candidate(c, s, t->log, &cand, &range_done, &seal)) {
            if (range_done && s->forward && intent_space(c, 1)) {
                /* Range-completion retirement is a delivery outcome, not stream
                 * maintenance — hold it while paused (Forward=0); resume re-runs
                 * the scan and retires then. The wire status is PUBLISH_DONE
                 * SUBSCRIPTION_ENDED: the filter range is exhausted. */
                moqr_intent_t *it = intent_push(c, MOQR_INTENT_SUB_DONE);
                it->binding_cookie = b->cookie;
                it->cookie = s->cookie;
                it->sub = sub_handle(c, i);
                it->error_code = R_DONE_SUB_ENDED;
                it->pd = core_local_done(MOQR_PD_SUBSCRIPTION_ENDED);
                sub_retire(c, i, now_us);
                i = next;
                continue;   /* retired: its SUB_DONE supersedes any notice */
            }
            if (s->pending_skip) {
                /* Fresh jump found by the scan's gpos_gc: EVICT_WATERMARK
                 * precedes any SEAL for the same sub. */
                if (NTC_BETTER(c, i, ntc_sub)) {
                    ntc_sub = i;
                    ntc_kind = MOQR_DELIVERY_NOTICE_EVICT_WATERMARK;
                }
            } else if (seal.have && NTC_BETTER(c, i, ntc_sub)) {
                ntc_sub = i;
                ntc_kind = MOQR_DELIVERY_NOTICE_SEAL;
                ntc_seal = seal;
            } else if (s->forward && sub_eot_complete(c, s, t->log) &&
                       intent_space(c, 1)) {
                /* The track ENDED and every in-range position is delivered
                 * or authoritatively closed: the subscription is finished.
                 * Same retirement as range completion — the existing
                 * SUB_DONE intent (wire PUBLISH_DONE), nothing new. Ordered
                 * after the skip/seal branches so every owed notice reaches
                 * the peer first. The wire status is PUBLISH_DONE
                 * TRACK_ENDED: the track itself is done, not the filter. */
                moqr_intent_t *it = intent_push(c, MOQR_INTENT_SUB_DONE);
                it->binding_cookie = b->cookie;
                it->cookie = s->cookie;
                it->sub = sub_handle(c, i);
                it->error_code = R_DONE_TRACK_ENDED;
                it->pd = core_local_done(MOQR_PD_TRACK_ENDED);
                sub_retire(c, i, now_us);
            }
            i = next;
            continue;
        }
        if (s->pending_skip) {
            /* The scan's own gpos_gc just detected the jump: the candidate
             * it returned is post-jump and must wait behind the watermark. */
            if (NTC_BETTER(c, i, ntc_sub)) {
                ntc_sub = i;
                ntc_kind = MOQR_DELIVERY_NOTICE_EVICT_WATERMARK;
            }
            i = next;
            continue;
        }
        if (seal.have) {
            /* Notice precedes record work for THIS sub: contribute the seal
             * as its candidate and hold its record for a later pass. */
            if (NTC_BETTER(c, i, ntc_sub)) {
                ntc_sub = i;
                ntc_kind = MOQR_DELIVERY_NOTICE_SEAL;
                ntc_seal = seal;
            }
            i = next;
            continue;
        }
        if (!s->forward && cand.view.obj_state != MOQR_OBJ_ABANDONED) {
            /* Forward=0: ordinary object transmission is suppressed for THIS
             * sub — its record is not selected and range-completion retirement
             * is held (above). Its maintenance notices (SEAL / EVICT_WATERMARK)
             * and the evict-reset sweep already ran unconditionally, so a
             * required downstream close/reset is never deferred to resume. An
             * ABANDONED candidate is a begun-downstream reset (sub_best_candidate
             * already dropped never-begun abandons), which is stream
             * maintenance too — it stays eligible while paused. Siblings are
             * unaffected; resume re-arms and re-derives the record from the
             * unadvanced cursor. */
            i = next;
            continue;
        }
        /* Cross-subscription ranking: subscriber priority first (spec
         * rule 1), then the same record ordering with ascending groups
         * (cross-request order past the priorities is implementation-
         * dependent; ties resolve by sub slot for determinism, so the
         * selected best is independent of iteration order). */
        bool better;
        if (!have) {
            better = true;
        } else if (s->subscriber_priority !=
                   c->subs[best_slot].subscriber_priority) {
            better = s->subscriber_priority <
                     c->subs[best_slot].subscriber_priority;
        } else if (cand.view.publisher_priority !=
                   best.view.publisher_priority) {
            better = cand.view.publisher_priority <
                     best.view.publisher_priority;
        } else if (cand.view.group_id != best.view.group_id) {
            better = cand.view.group_id < best.view.group_id;
        } else if (cand.view.arrival_seq != best.view.arrival_seq) {
            better = cand.view.arrival_seq < best.view.arrival_seq;
        } else {
            better = i < best_slot;
        }
        if (better) {
            have = true;
            best_slot = i;
            best = cand;
        }
        i = next;
    }
    /* Cross-subscription pick between the best notice and the best record:
     * subscriber priority first; a notice outranks a record at EQUAL
     * priority (payload-free, unblocks a FIN/close) but never preempts a
     * higher-priority sibling's record. Deterministic throughout: the notice
     * winner was chosen by (priority, sub slot) and a SEAL already names its
     * lowest (group, subgroup). */
    if (ntc_sub != R_SUB_NIL && have &&
        c->subs[best_slot].subscriber_priority <
            c->subs[ntc_sub].subscriber_priority) {
        ntc_sub = R_SUB_NIL;   /* the record wins; the notice waits its turn */
    }
    if (ntc_sub != R_SUB_NIL) {
        r_sub_t *ns = &c->subs[ntc_sub];
        b->out_sub = ntc_sub;
        b->out_group =
            (ntc_kind == MOQR_DELIVERY_NOTICE_SEAL) ? ntc_seal.group : 0;
        b->out_list =
            (ntc_kind == MOQR_DELIVERY_NOTICE_SEAL) ? ntc_seal.list : 0;
        b->out_chunked = false;
        b->out_abandoned = false;
        b->out_evict_reset = false;
        b->out_chunk_base = 0;
        b->out_notice = ntc_kind;
        b->pin_payload = NULL;
        b->pin_properties = NULL;
        b->out_active = true;
        memset(out, 0, sizeof(*out));
        out->sub = sub_handle(c, ntc_sub);
        out->sub_cookie = ns->cookie;
        out->notice = ntc_kind;
        out->oldest_group = UINT64_MAX;
        if (ntc_kind == MOQR_DELIVERY_NOTICE_SEAL) {
            out->rec.group_id = ntc_seal.group;
            out->rec.subgroup_id = ntc_seal.subgroup_id;
            /* subgroup_end is the CLEAN-finish advisory; a reset-flavored
             * terminal is abnormal and must never read as one. */
            out->subgroup_end = !ntc_seal.reset;
            out->seal_reset = ntc_seal.reset;
            out->seal_reset_desc = ntc_seal.reset_desc;
        } else {
            moqr_log_stats_t ls;
            moqr_log_get_stats(c->tracks[ns->track].log, &ls);
            out->oldest_group = ls.oldest_group_id;
            /* The eviction jump surfaces in the trace ring at notice time
             * (once per notice — a held re-peek replays out_view without
             * re-entering this path). Layout: detail=0 (gpos eviction skip),
             * e0=binding slot, e1=sub slot, e2=oldest retained group. */
            r_trace(c, MOQR_TRACE_CURSOR_SKIP, 0, bslot, ntc_sub,
                    ls.oldest_group_id, 0);
        }
        b->out_view = *out;
        return MOQR_OK;
    }
    if (!have) {
        return MOQR_DONE;
    }

    r_sub_t *ws = &c->subs[best_slot];
    b->out_sub = best_slot;
    b->out_group = best.view.group_id;
    b->out_list = best.list;
    b->out_chunked = false;
    b->out_abandoned = false;
    b->out_chunk_base = 0;
    /* Pin the outstanding delivery so its view stays valid across same-track
     * ingest/eviction until delivery_done (log views alone survive only until
     * the next mutating log call). Three shapes: an ABANDONED-begun head (no
     * chunks — the bind resets downstream); a chunked record (pin the
     * undelivered batch [emitted, chunk_count)); or a whole-object record (pin
     * the single payload). Properties are pinned for chunked/whole. */
    if (best.view.obj_state == MOQR_OBJ_ABANDONED) {
        b->out_abandoned = true;
        b->pin_payload = NULL;
        b->pin_properties = NULL;
    } else if (best.view.chunk_count > 0) {
        b->out_chunked = true;
        b->out_chunk_base = best.emitted;
        if (binding_pin_chunks(c, b, c->tracks[ws->track].log, &best.view,
                               best.emitted) != MOQR_OK) {
            /* Cannot pin (OOM growing the reusable array): fail this delivery
             * closed and retry next pump rather than hand out a truncatable
             * object. Nothing was mutated; the record stays the head. */
            b->out_chunked = false;
            return MOQR_ERR_CAPACITY;
        }
        b->pin_payload = NULL;
        b->pin_properties = moq_rcbuf_incref((moq_rcbuf_t *)(void *)
                                                 best.view.properties);
    } else {
        b->pin_payload = moq_rcbuf_incref((moq_rcbuf_t *)(void *)
                                              best.view.payload);
        b->pin_properties = moq_rcbuf_incref((moq_rcbuf_t *)(void *)
                                                 best.view.properties);
    }
    b->out_active = true;
    out->sub = sub_handle(c, best_slot);
    out->sub_cookie = ws->cookie;
    out->rec = best.view;
    out->evicted_reset = false;   /* only the evict-reset sweep sets this */
    out->skipped = false;         /* always: superseded by the notice     */
    out->notice = MOQR_DELIVERY_NOTICE_NONE;
    b->out_notice = MOQR_DELIVERY_NOTICE_NONE;
    /* Eviction jumps are no longer reported on record deliveries: an
     * unacknowledged watermark surfaces as an EVICT_WATERMARK notice and
     * BLOCKS this sub's record selection until acked, so a record reaching
     * here has no pending jump by construction. */
    out->oldest_group = UINT64_MAX;
    /* Flag the last record of a SEALED subgroup (datagrams have no subgroup:
     * best.list is a datagram pos-slot there, not a subgroup slot, so guard
     * on it). ADVISORY on records: the downstream close rides the SEAL
     * notice that becomes eligible once this record's DELIVERED ack advances
     * the cursor past it — one durable, retryable close path, so a close
     * that would WOULD_BLOCK after a delivered record can never be swallowed
     * with the delivery's acknowledgement. */
    out->subgroup_end =
        !best.view.datagram_pref &&
        moqr_log_subgroup_is_final(c->tracks[ws->track].log,
                                   best.view.group_id, best.list,
                                   best.view.object_id);
    if (b->out_chunked || b->out_abandoned) {
        /* Store the delivery so a re-peek after a downstream WOULD_BLOCK
         * replays it verbatim: chunk bytes come from the pinned array via
         * moqr_core_delivery_chunk (survives record eviction), and an abandoned
         * head replays so the bind can retry a WOULD_BLOCKed reset. */
        b->out_view = *out;
    }
    return MOQR_OK;
}

moqr_result_t
moqr_core_delivery_done(moqr_core_t *c, moqr_binding_t bh,
                        moqr_delivery_outcome_t outcome, uint64_t now_us)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (!b->out_active) {
        return MOQR_ERR_WRONG_STATE;
    }
    if (outcome != MOQR_DELIVERY_DELIVERED &&
        outcome != MOQR_DELIVERY_WOULD_BLOCK &&
        outcome != MOQR_DELIVERY_STREAM_ERROR &&
        outcome != MOQR_DELIVERY_STALLED &&
        outcome != MOQR_DELIVERY_ABANDONED &&
        outcome != MOQR_DELIVERY_REFUSED_UNBEGUN) {
        return MOQR_ERR_INVAL;
    }
    if (b->out_notice != MOQR_DELIVERY_NOTICE_NONE &&
        (outcome == MOQR_DELIVERY_STALLED ||
         outcome == MOQR_DELIVERY_ABANDONED ||
         outcome == MOQR_DELIVERY_REFUSED_UNBEGUN)) {
        return MOQR_ERR_INVAL;   /* a notice has no stall/abandon/pre-begin
                                  * refusal shape */
    }
    if (outcome == MOQR_DELIVERY_REFUSED_UNBEGUN && b->out_abandoned) {
        return MOQR_ERR_INVAL;   /* a reset has no pre-begin refusal site */
    }
    if (outcome == MOQR_DELIVERY_REFUSED_UNBEGUN && b->out_chunked &&
        b->out_chunk_base != 0) {
        /* The pinned batch resumes at a nonzero chunk index: the object
         * provably BEGAN downstream in an earlier batch, so the caller's
         * pre-begin claim is false — releasing would re-derive from the
         * emitted watermark and could duplicate. Fail closed. */
        return MOQR_ERR_INVAL;
    }
    /* Backpressure HOLDS the outstanding delivery (a chunked batch, an
     * abandoned-reset, or a recordless notice): keep out_active + pins so the
     * next next_delivery replays it verbatim and the bind resumes its chunk
     * cursor / retries the reset / re-attempts the notice. Whole-object
     * WOULD_BLOCK drops and re-derives (no sub-object position to preserve) —
     * and so does a REFUSED_UNBEGUN chunked record: nothing downstream began,
     * so there is no cursor to preserve, and holding it would starve the SEAL
     * notices whose closes free the slot it needs (the release lets the next
     * selection pick those notices first). Only while the sub is live; a
     * retired sub falls to cleanup. */
    if (outcome == MOQR_DELIVERY_WOULD_BLOCK &&
        (b->out_chunked || b->out_abandoned ||
         b->out_notice != MOQR_DELIVERY_NOTICE_NONE) &&
        (c->subs[b->out_sub].gen & 1u) != 0) {
        r_trace(c, MOQR_TRACE_BLOCKED, 0, bslot, b->out_sub, 0, 0);
        return MOQR_OK;
    }
    uint8_t notice = b->out_notice;
    b->out_active = false;
    b->out_notice = MOQR_DELIVERY_NOTICE_NONE;
    r_sub_t *s = &c->subs[b->out_sub];
    if ((s->gen & 1u) == 0) {
        binding_unpin(b);
        return MOQR_OK;   /* sub retired while outstanding */
    }
    /* This sub's positions entry for the delivered group (NULL if the group was
     * evicted meanwhile — already reclaimed; the skip reports on next delivery). */
    r_gpos_t *pe = NULL;
    {
        r_track_t *t = &c->tracks[s->track];
        if ((t->gen & 1u) != 0 && t->gen == s->track_gen_slot &&
            s->gpos != NULL) {
            uint32_t slot = gpos_hash_find(s, b->out_group);   /* O(1) */
            if (slot != R_GPOS_NIL) {
                pe = gpos_at(s, slot);
            }
        }
    }
    if (notice != MOQR_DELIVERY_NOTICE_NONE) {
        /* Recordless notice: WOULD_BLOCK was held above (live sub) and a
         * dead sub returned earlier, so only the acknowledgement and the
         * terminal reach here. The ack is what clears the pending core
         * state — never the peek. */
        if (outcome == MOQR_DELIVERY_DELIVERED) {
            if (notice == MOQR_DELIVERY_NOTICE_SEAL) {
                if (pe != NULL) {
                    gpos_seal_ack(s, pe, b->out_list);
                }
                r_trace(c, MOQR_TRACE_DELIVERY, 1, bslot, b->out_sub,
                        b->out_group, 0);
            } else {
                s->pending_skip = false;
                r_trace(c, MOQR_TRACE_DELIVERY, 2, bslot, b->out_sub, 0, 0);
            }
        } else {
            sub_retire(c, b->out_sub, now_us);   /* STREAM_ERROR */
        }
        binding_unpin(b);
        return MOQR_OK;
    }
    switch (outcome) {
    case MOQR_DELIVERY_DELIVERED:
        if (pe != NULL) {
            if (gpos_emit(s, pe)[b->out_list] > 0) {
                sub_begun_sub(c, s);               /* a begun head completed   */
            }
            pe->idx[b->out_list]++;                /* advance to next record  */
            gpos_emit(s, pe)[b->out_list] = 0;     /* reset live-edge cursor  */
        }
        c->delivered_total++;
        r_trace(c, MOQR_TRACE_DELIVERY, 0, bslot, b->out_sub, b->out_group, 0);
        break;
    case MOQR_DELIVERY_STALLED:
        /* Live edge drained but not COMPLETE: record how many chunks are now
         * scheduled (so the head is not re-selected until it grows past this),
         * but do NOT advance the record and do NOT count a delivery. STALLED
         * implies begin_object shipped the header, so mark this position begun
         * even when zero chunks are exposed yet (its object still needs a RESET
         * if abandoned/evicted). */
        if (pe != NULL) {
            if (gpos_emit(s, pe)[b->out_list] == 0) {
                sub_begun_add(c, s);   /* this position is now begun downstream */
            }
            gpos_emit(s, pe)[b->out_list] =
                GPOS_BEGUN_BIT | (uint32_t)b->out_view.rec.chunk_count;
        }
        r_trace(c, MOQR_TRACE_BLOCKED, 0, bslot, b->out_sub, 0, 0);
        break;
    case MOQR_DELIVERY_ABANDONED:
        /* Bind reset the downstream subgroup(s); advance past the abandoned state
         * without counting a delivery. An evict-reset covered the WHOLE evicted
         * group, so reclaim the entire position slot (a plain skip); a normal
         * abandon advanced just the one list past its tombstone. */
        if (pe != NULL) {
            if (b->out_evict_reset) {
                for (uint32_t li = 0; li < s->gpos_lists; li++) {
                    if (gpos_emit(s, pe)[li] > 0) {
                        sub_begun_sub(c, s);   /* clearing this begun position */
                    }
                }
                pe->group_id = UINT64_MAX;
                gpos_body_clear(s, pe);
                s->pending_skip = true;
                ready_mark(c, s->binding);   /* the skip must be reported */
                gpos_hash_rebuild(s);   /* drop the reclaimed entry from the index */
            } else {
                if (gpos_emit(s, pe)[b->out_list] > 0) {
                    sub_begun_sub(c, s);   /* a begun head was abandoned */
                }
                pe->idx[b->out_list]++;
                gpos_emit(s, pe)[b->out_list] = 0;
            }
        }
        r_trace(c, MOQR_TRACE_BLOCKED, 0, bslot, b->out_sub, 0, 0);
        break;
    case MOQR_DELIVERY_WOULD_BLOCK:
    case MOQR_DELIVERY_REFUSED_UNBEGUN:
        /* Released without advancing: re-derived by a later selection (for
         * REFUSED_UNBEGUN nothing downstream began, so re-derivation cannot
         * duplicate). */
        r_trace(c, MOQR_TRACE_BLOCKED, 0, bslot, b->out_sub, 0, 0);
        break;
    case MOQR_DELIVERY_STREAM_ERROR:
    default:
        sub_retire(c, b->out_sub, now_us);
        break;
    }
    binding_unpin(b);
    return MOQR_OK;
}

moqr_result_t
moqr_core_delivery_chunk(moqr_core_t *c, moqr_binding_t bh, uint32_t idx,
                         const moq_rcbuf_t **out_buf, uint64_t *out_len)
{
    if (c == NULL || out_buf == NULL || out_len == NULL) {
        return MOQR_ERR_INVAL;
    }
    r_binding_t *b = binding_resolve(c, bh, NULL);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (!b->out_active || !b->out_chunked) {
        return MOQR_ERR_WRONG_STATE;   /* no chunked delivery outstanding */
    }
    /* idx is ABSOLUTE (the bind's chunk cursor); the pinned array holds only the
     * current batch [out_chunk_base, out_chunk_base + pin_chunk_count). */
    if (idx < b->out_chunk_base ||
        idx - b->out_chunk_base >= b->pin_chunk_count) {
        return MOQR_DONE;              /* outside the pinned batch */
    }
    uint32_t rel = idx - b->out_chunk_base;
    *out_buf = b->pin_chunks[rel].buf;
    *out_len = b->pin_chunks[rel].len;
    return MOQR_OK;
}

/* -- Retained-hit FETCH (relay.h) --------------------------------------------------- */

static moqr_fetch_t
fetch_handle(const moqr_core_t *c, uint32_t slot)
{
    return (moqr_fetch_t){ r_pack(c, MOQR_HANDLE_POOL_FETCH,
                                  c->fetches[slot].gen, slot) };
}

static r_fetch_t *
fetch_resolve(moqr_core_t *c, moqr_fetch_t h, uint32_t *out_slot)
{
    if (moq_handle_pool_tag(h._opaque) != MOQR_HANDLE_POOL_FETCH ||
        moq_handle_session_tag(h._opaque) != c->shard_tag ||
        (moq_handle_generation(h._opaque) & 1u) == 0) {
        return NULL;
    }
    uint32_t slot = moq_handle_slot(h._opaque);
    if (slot >= c->max_fetches ||
        c->fetches[slot].gen != moq_handle_generation(h._opaque)) {
        return NULL;
    }
    if (out_slot != NULL) {
        *out_slot = slot;
    }
    return &c->fetches[slot];
}

bool
moqr_fetch_is_valid(moqr_fetch_t h)
{
    return moq_handle_pool_tag(h._opaque) == MOQR_HANDLE_POOL_FETCH &&
           moq_handle_session_tag(h._opaque) != 0 &&
           (moq_handle_generation(h._opaque) & 1u) != 0;
}

bool
moqr_fetch_eq(moqr_fetch_t a, moqr_fetch_t b)
{
    return a._opaque == b._opaque;
}

void
moqr_fetch_req_init(moqr_fetch_req_t *req)
{
    if (req == NULL) {
        return;
    }
    memset(req, 0, sizeof(*req));
    req->struct_size = (uint32_t)sizeof(*req);
}

static uint32_t
fetch_slot_find(moqr_core_t *c)
{
    for (uint32_t i = 0; i < c->max_fetches; i++) {
        if ((c->fetches[i].gen & 1u) == 0) {
            return i;
        }
    }
    return UINT32_MAX;
}

static void
fetch_unpin(moqr_core_t *c, r_fetch_t *f)
{
    if (!f->out_active) {
        return;
    }
    moq_rcbuf_decref(f->pin_payload);
    moq_rcbuf_decref(f->pin_properties);
    f->pin_payload = NULL;
    f->pin_properties = NULL;
    if (c->fetch_pin_used >= f->pin_bytes) {
        c->fetch_pin_used -= f->pin_bytes;
    } else {
        c->fetch_pin_used = 0;   /* defensive; accounting is symmetric */
    }
    f->pin_bytes = 0;
    f->out_active = false;
}

/* Find the smallest NORMAL object at Location >= (cur_group, cur_object) within
 * the fetch's range. Fills *out with a borrowed view and returns MOQR_OK;
 * MOQR_DONE when the range is exhausted. Read-only — every list of the current
 * group is scanned (subgroup lists are object-id ascending; the datagram list
 * carries no ordering, so it is scanned in full), yielding a k-way merge by
 * object_id; the first group (ascending) with an in-range NORMAL object wins. */
static moqr_result_t
fetch_scan_next(const moqr_log_t *log, const r_fetch_t *f,
                moqr_record_view_t *out)
{
    uint32_t gn = moqr_log_group_count(log);
    for (uint32_t gi = 0; gi < gn; gi++) {
        uint64_t g = moqr_log_group_id_at(log, gi);
        if (g < f->cur_group) {
            continue;
        }
        if (g > f->end_group) {
            break;   /* groups ascend; nothing further is in range */
        }
        uint64_t floor_obj = (g == f->cur_group) ? f->cur_object : 0;
        uint64_t ceil_obj =
            (g == f->end_group && !f->end_whole) ? f->end_object : UINT64_MAX;
        if (floor_obj > ceil_obj) {
            continue;   /* empty object window in this group */
        }
        bool have = false;
        moqr_record_view_t best;
        memset(&best, 0, sizeof(best));
        uint32_t lists = moqr_log_group_list_count(log, g);
        for (uint32_t slot = 0; slot <= lists; slot++) {
            uint32_t list_ref =
                (slot == lists) ? MOQR_LOG_LIST_DATAGRAM : slot;
            for (uint32_t idx = 0;; idx++) {
                moqr_record_view_t v;
                moqr_result_t rc =
                    moqr_log_read_rec(log, g, list_ref, idx, &v);
                if (rc != MOQR_OK) {
                    break;   /* list end (DONE) or evicted (TOO_OLD) */
                }
                if (v.status != MOQR_OBJ_NORMAL) {
                    continue;   /* status objects never become fetch items */
                }
                if (v.obj_state != MOQR_OBJ_COMPLETE) {
                    continue;   /* OPEN / ABANDONED are not fetchable (deferred).
                                 * COMPLETE records pass — whole-object (payload in
                                 * rec.payload) and chunked (chunk_count > 0, payload
                                 * coalesced in fetch_peek) alike. */
                }
                if (v.object_id < floor_obj || v.object_id > ceil_obj) {
                    continue;
                }
                if (!have || v.object_id < best.object_id) {
                    best = v;
                    have = true;
                }
            }
        }
        if (have) {
            *out = best;
            return MOQR_OK;
        }
        /* group had no in-range NORMAL object (holes / status-only): next group */
    }
    return MOQR_DONE;
}

moqr_result_t
moqr_core_fetch_open(moqr_core_t *c, moqr_binding_t bh,
                     const moqr_fetch_req_t *req, uint64_t now_us,
                     moqr_fetch_t *out, moqr_fetch_plan_t *out_plan)
{
    (void)now_us;
    if (c == NULL || out == NULL || out_plan == NULL || req == NULL ||
        req->struct_size < sizeof(moqr_fetch_req_t) ||
        !ftn_view_ok(req->ns, req->name)) {
        return MOQR_ERR_INVAL;
    }
    *out = MOQR_FETCH_INVALID;
    memset(out_plan, 0, sizeof(*out_plan));
    uint32_t bslot = 0;
    r_binding_t *b = binding_resolve(c, bh, &bslot);
    if (b == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    /* Standalone + ascending only (DEFAULT == ascending). The binding
     * rejects joining/descending before this call; the core guards anyway. */
    if (req->group_order == MOQR_GROUP_ORDER_DESCENDING) {
        return MOQR_ERR_UNSUPPORTED;
    }

    r_key_t key = { NULL, 0 };
    moqr_result_t rc = key_build(c, req->ns, req->name, &key);
    if (rc != MOQR_OK) {
        return rc;
    }
    uint64_t hash = key_hash(&key);
    uint32_t tslot = track_find(c, &key, hash);
    key_release(c, &key);

    if (tslot == UINT32_MAX) {
        out_plan->admit = MOQR_FETCH_REJECT;
        out_plan->error_code = R_ERR_DOES_NOT_EXIST;   /* fetch-on-miss seam */
        return MOQR_OK;
    }
    r_track_t *t = &c->tracks[tslot];
    if (!t->has_largest) {
        out_plan->admit = MOQR_FETCH_REJECT;
        out_plan->error_code = R_ERR_INVALID_RANGE;   /* empty track */
        return MOQR_OK;
    }
    /* Start past the largest object -> INVALID_RANGE (draft §FETCH). */
    if (req->start_group > t->largest_group ||
        (req->start_group == t->largest_group &&
         req->start_object > t->largest_object)) {
        out_plan->admit = MOQR_FETCH_REJECT;
        out_plan->error_code = R_ERR_INVALID_RANGE;
        return MOQR_OK;
    }
    moqr_log_stats_t ls;
    moqr_log_get_stats(t->log, &ls);
    /* Fully-evicted log: no oldest retained group to anchor a marker to, so a
     * Start below the horizon has no servable suffix -> reject. */
    if (ls.oldest_group_id == UINT64_MAX) {
        out_plan->admit = MOQR_FETCH_REJECT;
        out_plan->error_code = R_ERR_INVALID_RANGE;
        return MOQR_OK;
    }
    /* Evicted prefix: Start is below the retention horizon. The fetch serves the
     * retained suffix led by one UNKNOWN marker -- but only if the range still
     * reaches retained data (checked once the effective End is known, below).
     * Start >= oldest is the normal fully-retained path. */
    bool evicted_prefix = (req->start_group < ls.oldest_group_id);

    /* Decode the raw-wire End Location to an inclusive last object. */
    bool req_whole = (req->end_object == 0);
    uint64_t req_g = req->end_group;
    uint64_t req_o = req_whole ? 0 : req->end_object - 1;
    /* Effective inclusive last = min(requested, track largest). */
    bool req_le_largest;
    if (req_g != t->largest_group) {
        req_le_largest = req_g < t->largest_group;
    } else if (req_whole) {
        req_le_largest = false;   /* whole of largest_group covers largest_o */
    } else {
        req_le_largest = req_o <= t->largest_object;
    }
    bool eff_whole = req_le_largest ? req_whole : false;
    uint64_t eff_g = req_le_largest ? req_g : t->largest_group;
    uint64_t eff_o = req_le_largest ? req_o : t->largest_object;
    /* End MUST be >= Start (draft): reject an empty/inverted range. */
    bool eff_lt_start;
    if (eff_g != req->start_group) {
        eff_lt_start = eff_g < req->start_group;
    } else if (eff_whole) {
        eff_lt_start = false;
    } else {
        eff_lt_start = eff_o < req->start_object;
    }
    if (eff_lt_start) {
        out_plan->admit = MOQR_FETCH_REJECT;
        out_plan->error_code = R_ERR_INVALID_RANGE;
        return MOQR_OK;
    }
    /* An evicted-prefix fetch must reach retained data: if the effective End is
     * still below the oldest retained group, the whole range is evicted -> no
     * suffix to serve, so reject rather than emit a marker with no objects. */
    if (evicted_prefix && eff_g < ls.oldest_group_id) {
        out_plan->admit = MOQR_FETCH_REJECT;
        out_plan->error_code = R_ERR_INVALID_RANGE;
        return MOQR_OK;
    }

    uint32_t fslot = fetch_slot_find(c);
    if (fslot == UINT32_MAX) {
        return r_refuse(c, MOQR_REFUSE_FETCHES);
    }
    r_fetch_t *f = &c->fetches[fslot];
    uint32_t gen = f->gen + 1;   /* even -> odd */
    memset(f, 0, sizeof(*f));
    f->gen = gen;
    f->binding = bslot;
    f->binding_gen = b->gen;
    f->track = tslot;
    f->track_gen_slot = t->gen;
    f->cookie = req->cookie;
    f->start_group = req->start_group;
    f->start_object = req->start_object;
    f->end_group = eff_g;
    f->end_object = eff_o;
    f->end_whole = eff_whole;
    f->group_order = req->group_order;
    f->subscriber_priority = req->subscriber_priority;
    if (evicted_prefix) {
        /* Skip the evicted prefix: serve from the oldest retained group, led by
         * one UNKNOWN marker emitted immediately before it. */
        f->lead_marker = true;
        f->lead_marker_group = ls.oldest_group_id;
        f->cur_group = ls.oldest_group_id;
        f->cur_object = 0;
    } else {
        f->cur_group = req->start_group;
        f->cur_object = req->start_object;
    }

    out_plan->admit = MOQR_FETCH_ACCEPT;
    /* FETCH_OK End Location, re-encoded raw-wire ("last + 1"; 0 = whole group). */
    out_plan->end_group = eff_g;
    out_plan->end_object = eff_whole ? 0 : eff_o + 1;
    /* End Of Track only when the track has ended AND the served range reaches
     * the track's final (largest) object — a before-tail fetch reports 0. */
    out_plan->end_of_track =
        ls.end_of_track && eff_g == t->largest_group &&
        (eff_whole || eff_o == t->largest_object);

    *out = fetch_handle(c, fslot);
    return MOQR_OK;
}

/* Bounded coalesce of a chunked COMPLETE FETCH payload. Retained standalone FETCH has no
 * zero-copy chunk-write session seam yet (the future begin_fetch_object /
 * write_fetch_object_data / end_fetch_object API). A
 * chunked COMPLETE record (produced by production streaming ingest) is
 * therefore served by coalescing its retained slices, in log order, into ONE
 * OWNING rcbuf so the existing single-payload write_fetch_object path serves it
 * unchanged. Bounded: charged as the fetch pin (fetch_pin_bytes, floored to the
 * per-track log budget), so the closed-form memory model holds. The subscribe
 * delivery path stays zero-copy and untouched.
 *
 * Lifetime (important): the payload is an OWNING rcbuf built with
 * moq_rcbuf_create — it copies the bytes into storage it allocates and frees them
 * via the allocator VTABLE it copied (whose ctx is the caller's allocator, which
 * outlives the core). It is deliberately NOT a moq_rcbuf_wrap over a core-owned
 * buffer with a core-pointer release callback: the session layer increfs this
 * payload into a send action and may hold the final reference PAST core teardown
 * (after moqr_core_fetch_commit unpins the core's ref), so the free must not
 * dereference the core. The price is a second copy on this cold retained-FETCH
 * path (chunks -> temp -> owning rcbuf) — accepted for a core-independent
 * lifetime. Failure at any step is a TERMINAL error (never a partial buffer),
 * with the temp released. */
static moqr_result_t
fetch_coalesce_chunks(moqr_core_t *c, const moqr_log_t *log,
                      const moqr_record_view_t *v, moq_rcbuf_t **out)
{
    *out = NULL;
    uint64_t total = v->declared_len;
    uint8_t *tmp = NULL;
    if (total > 0) {
        tmp = r_alloc(c, (size_t)total);
        if (tmp == NULL) {
            return MOQR_ERR_NOMEM;   /* buffer alloc: fail closed */
        }
    }
    uint64_t off = 0;
    for (uint32_t i = 0; i < v->chunk_count; i++) {
        const moq_rcbuf_t *cb = NULL;
        uint64_t clen = 0;
        if (moqr_log_view_chunk(log, v, i, &cb, &clen) != MOQR_OK ||
            off + clen > total) {
            r_free(c, tmp, (size_t)total);
            return MOQR_ERR_INTERNAL;   /* corrupt chunk list: fail closed */
        }
        if (clen > 0) {
            memcpy(tmp + off, moq_rcbuf_data(cb), (size_t)clen);
        }
        off += clen;
    }
    if (off != total) {
        r_free(c, tmp, (size_t)total);   /* chunk bytes != declared: fail closed */
        return MOQR_ERR_INTERNAL;
    }
    /* Copy the coalesced bytes into an owning rcbuf, then release the temp
     * (freed whether create succeeds or fails). */
    moq_rcbuf_t *rc_out = NULL;
    moq_result_t crc = moq_rcbuf_create(&c->alloc, tmp, (size_t)total, &rc_out);
    r_free(c, tmp, (size_t)total);
    if (crc != MOQ_OK) {
        return MOQR_ERR_NOMEM;
    }
    *out = rc_out;
    return MOQR_OK;
}

moqr_result_t
moqr_core_fetch_peek(moqr_core_t *c, moqr_fetch_t fh, uint64_t now_us,
                     moqr_fetch_item_t *out)
{
    (void)now_us;
    if (c == NULL || out == NULL) {
        return MOQR_ERR_INVAL;
    }
    r_fetch_t *f = fetch_resolve(c, fh, NULL);
    if (f == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    memset(out, 0, sizeof(*out));

    /* Idempotent re-peek: reconstruct the held OBJECT from stored scalars +
     * pins, so eviction of the underlying record between pumps is harmless. */
    if (f->out_active) {
        out->kind = MOQR_FETCH_ITEM_OBJECT;
        out->rec.group_id = f->out_group;
        out->rec.subgroup_id = f->out_subgroup;
        out->rec.object_id = f->out_object;
        out->rec.publisher_priority = f->out_priority;
        out->rec.datagram_pref = f->out_datagram;
        out->rec.status = MOQR_OBJ_NORMAL;
        out->rec.payload = f->pin_payload;
        out->rec.properties = f->pin_properties;
        return MOQR_OK;
    }

    /* Track gone under a live fetch: the accepted retained range was lost (an
     * open fetch pins the track against age/free, so this is the defensive net —
     * e.g. capacity eviction on an ACTIVE track that also freed it). It is a
     * TERMINAL (TOO_OLD), never a clean END — a clean END would let a caller FIN
     * a truncated range as if it completed. */
    if ((c->tracks[f->track].gen & 1u) == 0 ||
        c->tracks[f->track].gen != f->track_gen_slot) {
        return MOQR_ERR_TOO_OLD;
    }
    const moqr_log_t *log = c->tracks[f->track].log;

    /* Mid-fetch eviction: our resume group fell below the retention horizon, so
     * the remaining range can no longer be served fully — terminate (the binding
     * resets the fetch stream) rather than fabricate a silent gap. */
    moqr_log_stats_t ls;
    moqr_log_get_stats(log, &ls);
    if (ls.oldest_group_id != UINT64_MAX && f->cur_group < ls.oldest_group_id) {
        return MOQR_ERR_TOO_OLD;
    }

    /* Evicted-prefix lead marker precedes all objects. It carries no payload
     * (no pin) and is idempotent while lead_marker holds -- cleared only on its
     * commit -- so a re-peek after a held/WOULD_BLOCK marker re-emits the same
     * marker. Placed after the eviction guards: if the anchored oldest group has
     * itself been evicted since open, that guard terminates (TOO_OLD) first
     * rather than emit a marker whose retained objects are already gone. */
    if (f->lead_marker) {
        out->kind = MOQR_FETCH_ITEM_MARKER;
        out->marker_group = f->lead_marker_group;
        return MOQR_OK;
    }

    moqr_record_view_t v;
    moqr_result_t rc = fetch_scan_next(log, f, &v);
    if (rc == MOQR_DONE) {
        out->kind = MOQR_FETCH_ITEM_END;
        return MOQR_OK;
    }
    if (rc != MOQR_OK) {
        return rc;   /* defensive terminal */
    }

    /* Pin budget: a single object is guaranteed to fit an idle budget (the
     * budget is floored to the log's max_bytes and a record can never exceed
     * that), so an object bigger than the whole budget is impossible — kept as a
     * terminal (CAPACITY) for defense. A momentarily-full budget (other fetches
     * hold pins) is WOULD_BLOCK: retry after they commit. Never an infinite wait. */
    uint64_t payload_len = v.chunk_count > 0
                               ? v.declared_len
                               : (uint64_t)moq_rcbuf_len(v.payload);
    uint64_t bytes = payload_len + (uint64_t)moq_rcbuf_len(v.properties);
    if (bytes > c->fetch_pin_cap) {
        return MOQR_ERR_CAPACITY;
    }
    if (c->fetch_pin_used + bytes > c->fetch_pin_cap) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    /* Acquire the payload pin AFTER the budget checks (so a rejected object never
     * allocates). Whole-object records pin the retained rcbuf directly (zero
     * copy); chunked COMPLETE records coalesce their slices into one owned
     * rcbuf. A coalesce failure is a terminal — the peer already has FETCH_OK
     * so the bind resets the fetch stream rather than send a short/empty object. */
    moq_rcbuf_t *payload_pin;
    if (v.chunk_count > 0) {
        moqr_result_t crc = fetch_coalesce_chunks(c, log, &v, &payload_pin);
        if (crc != MOQR_OK) {
            return crc;
        }
    } else {
        payload_pin = moq_rcbuf_incref((moq_rcbuf_t *)(void *)v.payload);
    }
    f->pin_payload = payload_pin;
    f->pin_properties = moq_rcbuf_incref((moq_rcbuf_t *)(void *)v.properties);
    f->pin_bytes = bytes;
    c->fetch_pin_used += bytes;
    f->out_active = true;
    f->out_group = v.group_id;
    f->out_subgroup = v.subgroup_id;
    f->out_object = v.object_id;
    f->out_priority = v.publisher_priority;
    f->out_datagram = v.datagram_pref;
    out->kind = MOQR_FETCH_ITEM_OBJECT;
    out->rec = v;
    /* Present the item as whole-object to the bind: payload is the pinned rcbuf
     * (coalesced for a chunked record), and chunk_count is cleared so the fetch
     * writer takes the single-payload path — matching the re-peek reconstruction
     * above. Whole-object records are unchanged (payload_pin == pinned v.payload,
     * chunk_count already 0). */
    out->rec.payload = f->pin_payload;
    out->rec.chunk_count = 0;
    return MOQR_OK;
}

moqr_result_t
moqr_core_fetch_commit(moqr_core_t *c, moqr_fetch_t fh)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    r_fetch_t *f = fetch_resolve(c, fh, NULL);
    if (f == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    /* Committing the lead marker: clear it (the marker's only state advance) and
     * return. There is no pin to release, and the cursor already starts at the
     * first retained group, so object scanning resumes there next peek. */
    if (f->lead_marker) {
        f->lead_marker = false;
        return MOQR_OK;
    }
    if (!f->out_active) {
        return MOQR_ERR_WRONG_STATE;   /* commit without a peeked object */
    }
    /* Advance strictly past the emitted object; the scan moves to the next
     * group once the current group's window is exhausted. object_id is a
     * varint < 2^62 (log rejects UINT64_MAX), so +1 never wraps. */
    f->cur_group = f->out_group;
    f->cur_object = f->out_object + 1;
    fetch_unpin(c, f);
    return MOQR_OK;
}

moqr_result_t
moqr_core_fetch_close(moqr_core_t *c, moqr_fetch_t fh)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    r_fetch_t *f = fetch_resolve(c, fh, NULL);
    if (f == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    fetch_unpin(c, f);
    f->gen++;   /* odd -> even: invalidate the handle */
    return MOQR_OK;
}

moqr_result_t
moqr_core_limits_resolve(const moqr_core_relay_cfg_t *cfg,
                         moqr_core_limits_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    if (cfg == NULL || cfg->struct_size < sizeof(uint32_t)) {
        return MOQR_ERR_INVAL;
    }
#define L_RESOLVE(dst, field, def)     out->dst = RELAY_CFG_HAS(cfg, field) && cfg->field != 0 ? cfg->field : (def)
    L_RESOLVE(max_bindings, max_bindings, R_DEF_BINDINGS);
    L_RESOLVE(max_tracks, max_tracks, R_DEF_TRACKS);
    L_RESOLVE(max_subs, max_subs, R_DEF_SUBS);
    L_RESOLVE(max_ns_nodes, max_ns_nodes, R_DEF_NS_NODES);
    L_RESOLVE(max_ns_subs, max_ns_subs, R_DEF_NS_SUBS);
    L_RESOLVE(max_intents, max_intents, R_DEF_INTENTS);
    L_RESOLVE(name_intern_bytes, name_intern_bytes, R_DEF_INTERN);
    L_RESOLVE(max_parked, max_parked, R_DEF_PARKED);
    L_RESOLVE(parked_bytes, parked_bytes, R_DEF_PARKED_BYTES);
    L_RESOLVE(max_grants, max_grants, R_DEF_GRANTS);
    L_RESOLVE(grant_bytes, grant_bytes, R_DEF_GRANT_BYTES);
    L_RESOLVE(max_fetches, max_fetches, R_DEF_FETCHES);
    L_RESOLVE(max_cancels, max_cancels, out->max_bindings);
    L_RESOLVE(fetch_pin_bytes, fetch_pin_bytes, R_DEF_FETCH_PIN_BYTES);
#undef L_RESOLVE
    /* Atomicity invariant: every single *atomic* reservation must fit an
     * empty intent ring, so a binding that fully drains can always retry to
     * success. The reservations, by call:
     *   subscribe               2 (ACCEPT + UPSTREAM_SUBSCRIBE)
     *   upstream_ok / _error    <= max_subs    (parked-sub fan-out)
     *   announce / unannounce   <= max_ns_subs (one namespace's watchers)
     *   ns_subscribe            <= max_ns_nodes (matching announce nodes)
     *   binding_close           chunked: 1 per reject, <= max_ns_subs per
     *                           announce (resumable). The ring need only
     *                           hold the largest single unit. */
    {
        uint64_t need = 2;
        if (out->max_subs > need) {
            need = out->max_subs;
        }
        if (out->max_ns_subs > need) {
            need = out->max_ns_subs;
        }
        if (out->max_ns_nodes > need) {
            need = out->max_ns_nodes;
        }
        /* force_withdraw purges one track per unit: that track's
         * subscriber terminals plus its one upstream release. */
        if ((uint64_t)out->max_subs + 1u > need) {
            need = (uint64_t)out->max_subs + 1u;
        }
        if (need > out->max_intents) {
            out->max_intents =
                need > UINT32_MAX ? UINT32_MAX : (uint32_t)need;
        }
    }
    return MOQR_OK;
}

void
moqr_core_get_limits(const moqr_core_t *c, moqr_core_limits_t *out)
{
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (c == NULL) {
        return;
    }
    out->max_bindings = c->max_bindings;
    out->max_tracks = c->max_tracks;
    out->max_subs = c->max_subs;
    out->max_ns_nodes = c->max_ns_nodes;
    out->max_ns_subs = c->max_ns_subs;
    out->max_intents = c->max_intents;
    out->max_parked = c->max_parked;
    out->max_grants = c->max_grants;
    out->max_cancels = c->max_cancels;
    out->name_intern_bytes = (uint32_t)c->intern_budget;
    out->parked_bytes = (uint32_t)c->parked_bytes_cap;
    out->grant_bytes = (uint32_t)c->grant_bytes_cap;
    out->fetch_pin_bytes = (uint32_t)c->fetch_pin_cap;
    out->max_fetches = c->max_fetches;
}

moqr_result_t
moqr_core_upstream_lost(moqr_core_t *c, moqr_track_t th, uint64_t track_gen)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL || t->track_gen != track_gen) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->state != R_TRACK_ACTIVE) {
        return MOQR_ERR_WRONG_STATE;
    }
    t->has_upstream_binding = false;
    t->source_kind = R_TRACK_SOURCE_NONE;
    t->track_gen++;
    moqr_log_stats_t ls;
    moqr_log_get_stats(t->log, &ls);
    if (ls.record_count == 0 && track_sub_count(c, tslot) == 0 &&
        track_fetch_count(c, tslot) == 0) {
        track_free_slot(c, tslot);
    } else {
        t->state = R_TRACK_WARM;
    }
    return MOQR_OK;
}

/* The track's SOURCE ended gracefully — a pull upstream that sent SUBSCRIBE_DONE
 * or a push publisher that finished (PUBLISH_FINISHED). Terminate every
 * downstream subscriber on the track with a wire SUBSCRIBE_DONE carrying
 * `status`, then release the source — mirroring moqr_core_upstream_lost (WARM if
 * retained content remains for a rejoin, else freed) — so no downstream hangs
 * behind a now-sourceless ACTIVE track. Generation-guarded like the other
 * upstream resolutions; a stale or non-ACTIVE track is refused. Reserve-before-
 * mutate: needs one intent per downstream sub, so a short ring returns
 * MOQR_ERR_WOULD_BLOCK with NO state change and the binding retries after a
 * drain. */
moqr_result_t
moqr_core_source_done(moqr_core_t *c, moqr_track_t th, uint64_t track_gen,
                      moqr_pd_desc_t status, uint64_t now_us)
{
    if (c == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* Reject a corrupt terminal before retiring anything: every downstream subscription must stay live. */
    if (!moqr_pd_desc_valid(status)) {
        return MOQR_ERR_INVAL;
    }
    uint32_t tslot = 0;
    r_track_t *t = track_resolve(c, th, &tslot);
    if (t == NULL || t->track_gen != track_gen) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (t->state != R_TRACK_ACTIVE) {
        return MOQR_ERR_WRONG_STATE;
    }
    /* One SUB_DONE per downstream sub, reserved up front: either every
     * subscriber is told or none is (no partial termination on a short ring). */
    uint32_t n = track_sub_count(c, tslot);
    if (n > 0 && !intent_space(c, n)) {
        return MOQR_ERR_WOULD_BLOCK;
    }
    uint32_t tgen = c->tracks[tslot].gen; /* pool gen; stable across the loop */
    for (uint32_t i = 0; i < c->max_subs; i++) {
        r_sub_t *s = &c->subs[i];
        if ((s->gen & 1u) == 0 || s->track != tslot ||
            s->track_gen_slot != tgen) {
            continue;
        }
        uint64_t bcookie = 0;
        if (s->binding < c->max_bindings &&
            c->bindings[s->binding].gen == s->binding_gen) {
            bcookie = c->bindings[s->binding].cookie;
        }
        moqr_intent_t *it = intent_push(c, MOQR_INTENT_SUB_DONE);
        it->binding_cookie = bcookie;
        it->cookie = s->cookie;
        it->sub = sub_handle(c, i);
        it->pd = status; /* the forwarded terminal, with its own origin */
        sub_retire(c, i, now_us);
    }
    /* Release the upstream, mirroring moqr_core_upstream_lost: bump the gen so a
     * late upstream resolution is refused; keep retained content WARM for a
     * rejoin, or free the track when nothing remains. */
    t->has_upstream_binding = false;
    t->source_kind = R_TRACK_SOURCE_NONE;
    t->track_gen++;
    moqr_log_stats_t ls;
    moqr_log_get_stats(t->log, &ls);
    if (ls.record_count == 0 && track_sub_count(c, tslot) == 0 &&
        track_fetch_count(c, tslot) == 0) {
        track_free_slot(c, tslot);
    } else {
        t->state = R_TRACK_WARM;
    }
    return MOQR_OK;
}

#ifdef MOQR_CORE_TESTING
/* Test-only: read BOTH halves of the begun ledger for one live subscription —
 * its own begun_count and the begun_subs tally on its owning binding. They are
 * two records of the same fact, and a leak in either is invisible from outside:
 * no public gauge reports them, and by the time an eviction watermark passes a
 * stale position its subscription's cursor is long past it, so no delivery
 * differs. Pure observation: no scheduling, lifetime or accounting effect.
 *
 * Fail-closed — an unknown core, a stale subscription generation or a dead
 * owning binding returns false with both outputs untouched. Never compiled
 * into a production core. */
bool
moqr_core_debug_begun_ledger(const moqr_core_t *c, moqr_sub_t sh,
                             uint32_t *out_sub_begun,
                             uint32_t *out_binding_begun_subs)
{
    if (c == NULL || out_sub_begun == NULL ||
        out_binding_begun_subs == NULL) {
        return false;
    }
    uint32_t slot = 0;
    const r_sub_t *s = sub_resolve((moqr_core_t *)(void *)c, sh, &slot);

    if (s == NULL || s->binding >= c->max_bindings) {
        return false;
    }
    const r_binding_t *b = &c->bindings[s->binding];

    if (b->gen != s->binding_gen) {
        return false;   /* the owning binding is gone or reused */
    }
    *out_sub_begun = s->begun_count;
    *out_binding_begun_subs = b->begun_subs;
    return true;
}
#endif /* MOQR_CORE_TESTING */

uint32_t
moqr_core_drain_ready(moqr_core_t *c, uint64_t *cookies, uint32_t cap)
{
    if (c == NULL || cookies == NULL) {
        return 0;
    }
    uint32_t words = ready_word_count(c->max_bindings);
    uint32_t out = 0;
    for (uint32_t wi = 0; wi < words && out < cap && c->ready_count > 0;
         wi++) {
        while (c->ready_words[wi] != 0 && out < cap) {
            /* Lowest set bit first: ascending slot order within and across
             * words. */
            uint64_t w = c->ready_words[wi];
            uint64_t low = w & (~w + 1u);
            uint32_t bi = 0;
            for (uint32_t sh = 32; sh != 0; sh >>= 1) {
                if ((low >> sh) != 0) {
                    bi += sh;
                    low >>= sh;
                }
            }
            cookies[out++] = c->bindings[((uint64_t)wi << 6) + bi].cookie;
            c->ready_words[wi] &= ~(1ull << bi);
            c->ready_count--;
        }
    }
    return out;
}

moqr_result_t
moqr_core_capacity_describe(const moqr_core_relay_cfg_t *cfg,
                            moqr_core_capacity_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    if (cfg == NULL || cfg->struct_size < sizeof(uint32_t)) {
        return MOQR_ERR_INVAL;
    }
    /* The SAME pure resolution create() consumes — defaults, the
     * cancels/bindings coupling, and the intent-ring atomicity clamp all
     * come from one place. */
    moqr_core_limits_t lim;
    if (moqr_core_limits_resolve(cfg, &lim) != MOQR_OK) {
        return MOQR_ERR_INVAL;
    }
    uint64_t bindings = lim.max_bindings;
    uint64_t tracks = lim.max_tracks;
    uint64_t subs = lim.max_subs;
    uint64_t nodes = lim.max_ns_nodes;
    uint64_t ns_subs = lim.max_ns_subs;
    uint64_t intents = lim.max_intents;
    uint64_t intern = lim.name_intern_bytes;
    uint64_t parked = lim.max_parked;
    uint64_t parked_bytes = lim.parked_bytes;
    uint64_t grants = lim.max_grants;
    uint64_t grant_bytes = lim.grant_bytes;
    uint64_t cancels = lim.max_cancels;
    uint64_t fetches = lim.max_fetches;
    uint64_t fetch_pin = lim.fetch_pin_bytes;

    uint64_t s = sizeof(moqr_core_t);
    s = moqr_cap_add(s, moqr_cap_mul(bindings, sizeof(r_binding_t)));
    /* Binding-ready bitset: one bit per binding slot, whole words. */
    s = moqr_cap_add(s, moqr_cap_mul(ready_word_count(lim.max_bindings),
                                     sizeof(uint64_t)));
    s = moqr_cap_add(s, moqr_cap_mul(tracks, sizeof(r_track_t)));
    s = moqr_cap_add(s, moqr_cap_mul(subs, sizeof(r_sub_t)));
    s = moqr_cap_add(s, moqr_cap_mul(nodes, sizeof(r_trie_node_t)));
    s = moqr_cap_add(s, moqr_cap_mul(ns_subs, sizeof(r_ns_sub_t)));
    s = moqr_cap_add(s, moqr_cap_mul(intents, sizeof(moqr_intent_t)));
    s = moqr_cap_add(s, moqr_cap_mul(parked, sizeof(r_parked)));
    s = moqr_cap_add(s, moqr_cap_mul(grants, sizeof(r_grant)));
    s = moqr_cap_add(s, moqr_cap_mul(cancels, sizeof(r_pending_cancel)));
    s = moqr_cap_add(s, moqr_cap_mul(fetches, sizeof(r_fetch_t)));
    s = moqr_cap_add(s, intern);   /* interned names + trie parts + ns-sub prefixes */
    /* Per-sub scheduling positions at full occupancy (stride ceiling:
     * max_groups entries of (u64 + u32 per list), 8-aligned). */
    moqr_log_cfg_t lc;
    moqr_log_cfg_init_sized(&lc, sizeof(lc), cfg->alloc);
    if (RELAY_CFG_HAS(cfg, log_budget)) {
        lc.budget = cfg->log_budget;
    }
    lc.max_subgroups_per_group =
        RELAY_CFG_HAS(cfg, log_max_subgroups) ? cfg->log_max_subgroups : 0;
    lc.max_objects_per_group = RELAY_CFG_HAS(cfg, log_max_objects_per_group)
                                   ? cfg->log_max_objects_per_group
                                   : 0;
    lc.max_cursors =
        RELAY_CFG_HAS(cfg, log_max_cursors) ? cfg->log_max_cursors : 0;
    lc.max_chunk_nodes =
        RELAY_CFG_HAS(cfg, log_max_chunk_nodes) ? cfg->log_max_chunk_nodes : 0;
    moqr_log_capacity_t logc;
    if (moqr_log_capacity_describe(&lc, &logc) != MOQR_OK) {
        return MOQR_ERR_INVAL;   /* the per-track log term wrapped */
    }
    uint64_t groups = lc.budget.max_groups != 0 ? lc.budget.max_groups : 8u;
    uint64_t lists =
        (lc.max_subgroups_per_group != 0 ? lc.max_subgroups_per_group
                                         : 16u) +
        1u;
    /* The per-(sub, group) position-entry stride — shared with the runtime
     * (gpos_stride) so the advertised ceiling can never drift from the
     * allocation. */
    uint64_t stride = gpos_stride_for(lists);
    if (lists > UINT32_MAX || stride > UINT32_MAX) {
        return MOQR_ERR_INVAL;   /* gpos layout unrepresentable */
    }
    s = moqr_cap_add(s, moqr_cap_mul(moqr_cap_mul(subs, groups), stride));
    /* Per-sub group_id -> gpos-slot index (shared wide resolver). */
    uint64_t gpos_hash_cap = gpos_hash_cap_for(groups);
    if (gpos_hash_cap > UINT32_MAX) {
        return MOQR_ERR_INVAL;   /* the runtime's u32 index cannot hold it */
    }
    s = moqr_cap_add(s, moqr_cap_mul(moqr_cap_mul(subs, gpos_hash_cap), sizeof(uint32_t)));

    /* Worst-case per-binding chunk-pin storage: a chunked delivery pins every
     * chunk of the in-flight record (ref + length) so eviction cannot truncate
     * a multi-pump delivery. A record can hold no more chunks than the log's
     * chunk-node pool, so each binding's reusable array is bounded by
     * max_chunk_nodes; the core-level log_max_chunk_nodes knob feeds the same
     * per-track log capacity path as live log creation. */
    s = moqr_cap_add(s, moqr_cap_mul(moqr_cap_mul(bindings, logc.max_chunk_nodes), sizeof(r_pin_chunk_t)));

    /* The fetch pin budget is floored to the per-track payload budget (create
     * applies the identical floor), and adds to the payload ceiling: a
     * backpressured fetch can hold that many payload bytes pinned past eviction,
     * on top of the logs' own retained payload. */
    if (fetch_pin < logc.payload_bytes) {
        fetch_pin = logc.payload_bytes;
    }
    /* rcbuf header overhead: the per-log retained-content headers (payload +
     * properties per record, one per chunk node — logc.header_bytes) times
     * tracks, plus one independently allocated coalesced buffer per live
     * fetch (possible at zero pinned payload bytes). H is the pinned
     * additive moq_rcbuf_allocation_size(0). */
    size_t rc_hdr = 0;
    (void)moq_rcbuf_allocation_size(0, &rc_hdr);
    out->structure_bytes = moqr_cap_add(
        s, moqr_cap_add(
               moqr_cap_mul(tracks, logc.structure_bytes),
               moqr_cap_add(moqr_cap_mul(tracks, logc.header_bytes),
                            moqr_cap_mul(fetches, rc_hdr))));
    out->payload_bytes = moqr_cap_add(
        moqr_cap_mul(tracks, logc.payload_bytes),
        moqr_cap_add(parked_bytes, moqr_cap_add(grant_bytes, fetch_pin)));
    out->total_bytes =
        moqr_cap_add(out->structure_bytes, out->payload_bytes);
    if (out->total_bytes == UINT64_MAX) {
        memset(out, 0, sizeof(*out));
        return MOQR_ERR_INVAL;   /* wrapped: refuse, never under-report */
    }
    return MOQR_OK;
}

/* -- stats ------------------------------------------------------------------------------------------------- */

void
moqr_core_get_stats(const moqr_core_t *c, moqr_core_stats_t *out)
{
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (c == NULL) {
        return;
    }
    for (uint32_t i = 0; i < c->max_bindings; i++) {
        if ((c->bindings[i].gen & 1u) != 0) {
            out->bindings++;
        }
    }
    for (uint32_t i = 0; i < c->max_subs; i++) {
        if ((c->subs[i].gen & 1u) != 0) {
            out->subs++;
            if (c->subs[i].state == R_SUB_PARKED) {
                out->subs_parked++;
            } else if (c->subs[i].state == R_SUB_ACTIVE) {
                out->subs_active++;
            }
        }
    }
    out->evicted_total = c->evicted_freed;
    for (uint32_t i = 0; i < c->max_tracks; i++) {
        if ((c->tracks[i].gen & 1u) != 0) {
            out->tracks++;
            moqr_log_stats_t ls;
            moqr_log_get_stats(c->tracks[i].log, &ls);
            out->retained_bytes += ls.retained_bytes;
            out->evicted_total += ls.evicted_records_total;
        }
    }
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        if (c->ns_subs[i].used) {
            out->ns_subs++;
        }
    }
    out->ns_nodes = c->node_count;
    out->ingested_total = c->ingested_total;
    out->delivered_total = c->delivered_total;
    out->evict_sweeps = c->evict_sweeps;
    out->note_emitted_total = c->note_emitted_total;
    out->intent_highwater = c->intent_hwm;
    out->route_epoch = c->route_epoch;
    for (uint32_t i = 0; i < MOQR_REFUSE__COUNT; i++) {
        out->refusals[i] = c->refusals[i];
    }
    for (uint32_t act = 0; act < MOQR_AUTH_ACTION__COUNT; act++) {
        for (uint32_t dec = 0; dec < 3; dec++) {
            out->auth_decisions[act][dec] = c->auth_decisions[act][dec];
        }
    }
    for (uint32_t rz = 0; rz < MOQR_AUTH_REASON__COUNT; rz++) {
        out->auth_denials[rz] = c->auth_denials[rz];
    }
}

const char *
moqr_refuse_reason_name(moqr_refuse_reason_t reason)
{
    switch (reason) {
    case MOQR_REFUSE_TRACKS:     return "tracks";
    case MOQR_REFUSE_SUBS:       return "subs";
    case MOQR_REFUSE_NS_NODES:   return "ns_nodes";
    case MOQR_REFUSE_NS_SUBS:    return "ns_subs";
    case MOQR_REFUSE_BINDINGS:   return "bindings";
    case MOQR_REFUSE_NAME_BYTES: return "name_bytes";
    case MOQR_REFUSE_FETCHES:    return "fetches";
    default:                     return "unknown";
    }
}

const char *
moqr_auth_action_name(moqr_auth_action_t action)
{
    switch (action) {
    case MOQR_AUTH_CLIENT_SETUP:        return "client_setup";
    case MOQR_AUTH_SERVER_SETUP:        return "server_setup";
    case MOQR_AUTH_PUBLISH_NAMESPACE:   return "publish_namespace";
    case MOQR_AUTH_SUBSCRIBE_NAMESPACE: return "subscribe_namespace";
    case MOQR_AUTH_SUBSCRIBE:           return "subscribe";
    case MOQR_AUTH_REQUEST_UPDATE:      return "request_update";
    case MOQR_AUTH_PUBLISH:             return "publish";
    case MOQR_AUTH_FETCH:               return "fetch";
    case MOQR_AUTH_TRACK_STATUS:        return "track_status";
    default:                           return "unknown";
    }
}

const char *
moqr_auth_reason_name(moqr_auth_reason_t reason)
{
    switch (reason) {
    case MOQR_AUTH_REASON_OK:            return "ok";
    case MOQR_AUTH_REASON_NO_TOKEN:      return "no_token";
    case MOQR_AUTH_REASON_UNSCOPED:      return "unscoped";
    case MOQR_AUTH_REASON_EXPIRED:       return "expired";
    case MOQR_AUTH_REASON_NOT_YET_VALID: return "not_yet_valid";
    case MOQR_AUTH_REASON_BAD_ISSUER:    return "bad_issuer";
    case MOQR_AUTH_REASON_BAD_AUDIENCE:  return "bad_audience";
    case MOQR_AUTH_REASON_BAD_SIGNATURE: return "bad_signature";
    case MOQR_AUTH_REASON_POLICY:        return "policy";
    default:                            return "unknown";
    }
}

void
moqr_core_authorize(moqr_core_t *c, const moqr_auth_request_t *req,
                    moqr_auth_verdict_t *out)
{
    if (out == NULL) {
        return;
    }
    /* Default is allow-all: one predictable branch, no hook call, so the
     * idle-auth path carries no measurable cost. */
    out->decision = MOQR_AUTH_ALLOW;
    out->revalidate_after_us = 0;
    out->error_code = 0;
    out->reason = MOQR_AUTH_REASON_OK;
    out->ticket = 0;
    /* NONE by default: only a hook that explicitly states a revocation
     * terminal gets one, and every other path sees "nothing said". */
    out->revoke_terminal = moqr_pd_desc_none();
    if (c == NULL || req == NULL) {
        return;
    }
    if (c->authorize != NULL) {
        c->authorize(c->authorize_ctx, req, out);
    }

    /* Canonicalize the verdict BEFORE any caller — or the trace/stats — sees
     * it: an out-of-range decision fails closed to DENY/POLICY, and an
     * out-of-range reason on a denial becomes POLICY. So a misbehaving hook
     * can never smuggle a garbage decision past the seam. */
    if (out->decision > MOQR_AUTH_DEFER) {
        out->decision = MOQR_AUTH_DENY;
        out->reason = MOQR_AUTH_REASON_POLICY;
    }
    if (out->decision == MOQR_AUTH_DENY &&
        out->reason >= MOQR_AUTH_REASON__COUNT) {
        out->reason = MOQR_AUTH_REASON_POLICY;
    }

    /* Count + trace with the canonical values (action clamped for the index;
     * the trace keeps the raw action as a diagnostic scalar). */
    uint32_t act = req->action < MOQR_AUTH_ACTION__COUNT
                       ? req->action
                       : MOQR_AUTH_ACTION__COUNT - 1u;
    c->auth_decisions[act][out->decision]++;
    if (out->decision == MOQR_AUTH_DENY) {
        c->auth_denials[out->reason]++;
    }
    r_trace(c, MOQR_TRACE_AUTH_DECISION, out->decision, req->action,
            req->binding_cookie, out->reason, 0);
}

/* -- deferred-auth parked storage ---------------------------------------------------------------- *
 * A DEFER verdict parks the request here (deep-copied) keyed by the hook's
 * external ticket. The binding resumes or rejects it later via begin/finish.
 * Per-entry buffers are fixed once copied, so a mutation during resume (e.g.
 * the resumed subscribe interning the pinned namespace) never moves the pinned
 * view. Bounded by max_parked (slots) and parked_bytes (copied bytes). */

static void
parked_free_slot(moqr_core_t *c, uint32_t slot)
{
    r_parked *p = &c->parked[slot];
    r_free_material(c, &p->mat, &c->parked_bytes_used);
    uint32_t gen = p->gen;
    memset(p, 0, sizeof(*p));
    p->gen = gen + 1u; /* even = free; internal slot liveness only — external
                        * ticket reuse is the verifier's contract (see relay.h) */
}

/* Live (odd-gen) parked slot holding external ticket `t`, or -1. */
static int
parked_find(const moqr_core_t *c, uint64_t t)
{
    if (t == 0) {
        return -1;
    }
    for (uint32_t i = 0; i < c->max_parked; i++) {
        if ((c->parked[i].gen & 1u) != 0 && c->parked[i].ext_ticket == t) {
            return (int)i;
        }
    }
    return -1;
}

moqr_result_t
moqr_core_park(moqr_core_t *c, const moqr_park_req_t *req, uint64_t ext_ticket,
               uint64_t now_us)
{
    (void)now_us;
    if (c == NULL || req == NULL) {
        return MOQR_ERR_INVAL;
    }
    if (ext_ticket == 0) {
        return MOQR_ERR_INVAL; /* a zero ticket can never be resolved */
    }
    if (parked_find(c, ext_ticket) >= 0) {
        return MOQR_ERR_STALE_HANDLE; /* duplicate live ticket: never merge */
    }
    int slot = -1;
    for (uint32_t i = 0; i < c->max_parked; i++) {
        if ((c->parked[i].gen & 1u) == 0) {
            slot = (int)i;
            break;
        }
    }
    if (slot < 0) {
        return MOQR_ERR_CAPACITY; /* max_parked exhausted */
    }
    r_material m;
    moqr_result_t rc = r_copy_material(c, req, &c->parked_bytes_used,
                                       c->parked_bytes_cap, &m);
    if (rc != MOQR_OK) {
        return rc; /* INVAL / CAPACITY / NOMEM: fail closed, no state change */
    }
    r_parked *p = &c->parked[slot];
    uint32_t  gen = p->gen; /* even (free) */
    memset(p, 0, sizeof(*p));
    p->gen = gen + 1u; /* odd = live */
    p->ext_ticket = ext_ticket;
    p->binding_cookie = req->binding_cookie;
    p->session_cookie = req->session_cookie;
    p->action = req->action;
    p->mat = m;
    p->sub_filter_type = req->sub_filter_type;
    p->sub_start_group = req->sub_start_group;
    p->sub_start_object = req->sub_start_object;
    p->sub_end_group_delta = req->sub_end_group_delta;
    p->sub_priority = req->sub_priority;
    p->sub_group_order = req->sub_group_order;
    return MOQR_OK;
}

moqr_result_t
moqr_core_auth_begin_resolve(moqr_core_t *c, uint64_t ext_ticket,
                             const moqr_auth_verdict_t *result, uint64_t now_us,
                             moqr_auth_verdict_t *verdict, moqr_park_req_t *view)
{
    (void)now_us;
    /* Fail-closed default: any early/error return leaves DENY, so a caller that
     * ignores the return code can never resume an unresolved request. */
    if (verdict != NULL) {
        memset(verdict, 0, sizeof(*verdict));
        verdict->decision = MOQR_AUTH_DENY;
        verdict->reason = MOQR_AUTH_REASON_POLICY;
    }
    if (c == NULL || view == NULL) {
        return MOQR_ERR_INVAL;
    }
    int slot = parked_find(c, ext_ticket);
    if (slot < 0 || c->parked[(uint32_t)slot].pinned) {
        return MOQR_ERR_STALE_HANDLE; /* unknown / retired / already resolving */
    }
    r_parked *p = &c->parked[(uint32_t)slot];
    /* A NULL result, or a DEFER/garbage decision, fails closed to DENY (no
     * recursive parking in v1). Otherwise the verifier's decision/reason/
     * error_code/lease flow through canonicalized. */
    moqr_auth_decision_t in_d = result != NULL ? result->decision
                                               : MOQR_AUTH_DENY;
    moqr_auth_reason_t   reason =
        result != NULL ? result->reason : MOQR_AUTH_REASON_POLICY;
    uint64_t error_code = result != NULL ? result->error_code : 0;
    uint64_t reval = result != NULL ? result->revalidate_after_us : 0;
    moqr_auth_decision_t d =
        in_d == MOQR_AUTH_ALLOW ? MOQR_AUTH_ALLOW : MOQR_AUTH_DENY;
    if (d == MOQR_AUTH_DENY && reason >= MOQR_AUTH_REASON__COUNT) {
        reason = MOQR_AUTH_REASON_POLICY;
    }
    uint32_t act = p->action < MOQR_AUTH_ACTION__COUNT
                       ? p->action
                       : MOQR_AUTH_ACTION__COUNT - 1u;
    c->auth_decisions[act][d]++;
    if (d == MOQR_AUTH_DENY) {
        c->auth_denials[reason]++;
    }
    r_trace(c, MOQR_TRACE_AUTH_DECISION, d, p->action, p->binding_cookie,
            reason, 0);
    if (verdict != NULL) {
        verdict->decision = d; /* CANONICAL: what the binding must branch on */
        verdict->reason = d == MOQR_AUTH_DENY ? reason : MOQR_AUTH_REASON_OK;
        verdict->error_code = d == MOQR_AUTH_DENY ? error_code : 0;
        verdict->revalidate_after_us = d == MOQR_AUTH_ALLOW ? reval : 0;
    }
    p->pinned = true;
    memset(view, 0, sizeof(*view));
    view->action = p->action;
    view->binding_cookie = p->binding_cookie;
    view->session_cookie = p->session_cookie;
    view->ns.parts = p->mat.ns_parts;
    view->ns.count = p->mat.ns_count;
    view->name.data = p->mat.name;
    view->name.len = p->mat.name_len;
    view->tokens = p->mat.tokens;
    view->token_count = p->mat.token_count;
    view->sub_filter_type = p->sub_filter_type;
    view->sub_start_group = p->sub_start_group;
    view->sub_start_object = p->sub_start_object;
    view->sub_end_group_delta = p->sub_end_group_delta;
    view->sub_priority = p->sub_priority;
    view->sub_group_order = p->sub_group_order;
    return MOQR_OK;
}

void
moqr_core_auth_finish_resolve(moqr_core_t *c, uint64_t ext_ticket)
{
    if (c == NULL) {
        return;
    }
    int slot = parked_find(c, ext_ticket);
    if (slot < 0 || !c->parked[(uint32_t)slot].pinned) {
        return; /* not pinned: nothing to finish */
    }
    parked_free_slot(c, (uint32_t)slot);
}

void
moqr_core_retire_parked(moqr_core_t *c, uint64_t binding_cookie)
{
    if (c == NULL) {
        return;
    }
    for (uint32_t i = 0; i < c->max_parked; i++) {
        if ((c->parked[i].gen & 1u) == 0 ||
            c->parked[i].binding_cookie != binding_cookie) {
            continue;
        }
        if (c->parked[i].pinned) {
            /* A resolve is mid-flight holding this view; the begin/finish
             * contract keeps it stable until finish_resolve, which frees a
             * retired entry. */
            c->parked[i].retired = true;
        } else {
            parked_free_slot(c, i);
        }
    }
}

/* -- revalidation grants ------------------------------------------------------------------------- */

static void
grant_free_slot(moqr_core_t *c, uint32_t slot)
{
    r_grant *g = &c->grants[slot];
    r_free_material(c, &g->mat, &c->grant_bytes_used);
    uint32_t gen = g->gen;
    memset(g, 0, sizeof(*g));
    g->gen = gen + 1u; /* even = free */
}

moqr_result_t
moqr_core_grant_reserve(moqr_core_t *c, const moqr_park_req_t *req,
                        uint64_t lease_us, uint64_t now_us,
                        moqr_grant_res_t *res_out)
{
    if (res_out != NULL) {
        res_out->slot = 0;
        res_out->gen = 0;
    }
    if (c == NULL || req == NULL || res_out == NULL) {
        return MOQR_ERR_INVAL;
    }
    if (lease_us == 0) {
        return MOQR_ERR_INVAL; /* no lease: the caller must not create a grant */
    }
    /* Only long-lived ops are grantable (subscribe + announce). */
    if (req->action != MOQR_AUTH_SUBSCRIBE &&
        req->action != MOQR_AUTH_PUBLISH_NAMESPACE) {
        return MOQR_ERR_UNSUPPORTED;
    }
    int slot = -1;
    for (uint32_t i = 0; i < c->max_grants; i++) {
        if ((c->grants[i].gen & 1u) == 0) {
            slot = (int)i;
            break;
        }
    }
    if (slot < 0) {
        return MOQR_ERR_CAPACITY; /* max_grants exhausted */
    }
    r_material m;
    moqr_result_t rc = r_copy_material(c, req, &c->grant_bytes_used,
                                       c->grant_bytes_cap, &m);
    if (rc != MOQR_OK) {
        return rc; /* INVAL / CAPACITY / NOMEM: fail closed, no state change */
    }
    r_grant *g = &c->grants[slot];
    uint32_t gen = g->gen; /* even (free) */
    memset(g, 0, sizeof(*g));
    g->gen = gen + 1u;      /* odd = live, but committed=false (reserved) */
    g->binding_cookie = req->binding_cookie;
    g->session_cookie = req->session_cookie;
    g->action = req->action;
    g->lease_us = lease_us;
    /* Schedule the first recheck; guard now_us + lease against wrap. */
    g->next_recheck_us =
        now_us > UINT64_MAX - lease_us ? UINT64_MAX : now_us + lease_us;
    g->mat = m;
    res_out->slot = (uint32_t)slot;
    res_out->gen = g->gen;
    return MOQR_OK;
}

moqr_result_t
moqr_core_grant_commit(moqr_core_t *c, moqr_grant_res_t res, uint64_t sub_raw)
{
    if (c == NULL || res.slot >= c->max_grants) {
        return MOQR_ERR_STALE_HANDLE;
    }
    r_grant *g = &c->grants[res.slot];
    if ((g->gen & 1u) == 0 || g->gen != res.gen || g->committed) {
        return MOQR_ERR_STALE_HANDLE; /* not a live, uncommitted reservation */
    }
    if (g->action == MOQR_AUTH_SUBSCRIBE && sub_raw == 0) {
        return MOQR_ERR_INVAL; /* a subscribe grant needs a real sub to revoke */
    }
    g->sub_raw = sub_raw;
    g->committed = true; /* now eligible for revalidation on tick */
    return MOQR_OK;      /* no-fail except stale/misuse */
}

void
moqr_core_grant_abort(moqr_core_t *c, moqr_grant_res_t res)
{
    if (c == NULL || res.slot >= c->max_grants) {
        return;
    }
    r_grant *g = &c->grants[res.slot];
    if ((g->gen & 1u) != 0 && g->gen == res.gen && !g->committed) {
        grant_free_slot(c, res.slot);
    }
}

void
moqr_core_retire_grants(moqr_core_t *c, uint64_t binding_cookie)
{
    if (c == NULL) {
        return;
    }
    for (uint32_t i = 0; i < c->max_grants; i++) {
        if ((c->grants[i].gen & 1u) != 0 &&
            c->grants[i].binding_cookie == binding_cookie) {
            grant_free_slot(c, i);
        }
    }
}

size_t
moqr_core_peek_revoked_grants(moqr_core_t *c, moqr_revoked_grant_t *out,
                              size_t max)
{
    if (c == NULL || out == NULL) {
        return 0;
    }
    /* Announce revocations whose NS_GONE fan-out is done (unannounced) but whose
     * publisher-side cancel_namespace the binding must still send. Subscribe
     * revocations are fully core-driven (revoke_sub) and never appear here.
     * NON-draining: the entry stays until moqr_core_ack_revoked_grant frees it,
     * so a cancel that WOULD_BLOCK is retried next pump. */
    size_t n = 0;
    for (uint32_t i = 0; i < c->max_grants && n < max; i++) {
        r_grant *g = &c->grants[i];
        if ((g->gen & 1u) != 0 && g->revoked && g->unannounced) {
            out[n].binding_cookie = g->binding_cookie;
            out[n].session_cookie = g->session_cookie;
            out[n].error_code = g->revoke_request_error; /* defaulted in grant_tick */
            n++;
        }
    }
    /* Force-withdraw publisher cancels, merged after the revoked grants. Same
     * non-draining contract: an entry stays until moqr_core_ack_revoked_grant. */
    for (uint32_t i = 0; i < c->max_cancels && n < max; i++) {
        r_pending_cancel *pc = &c->pending_cancels[i];
        if (pc->used) {
            out[n].binding_cookie = pc->binding_cookie;
            out[n].session_cookie = pc->session_cookie;
            out[n].error_code = pc->error_code;
            n++;
        }
    }
    return n;
}

void
moqr_core_ack_revoked_grant(moqr_core_t *c, uint64_t binding_cookie,
                            uint64_t session_cookie)
{
    if (c == NULL) {
        return;
    }
    for (uint32_t i = 0; i < c->max_grants; i++) {
        r_grant *g = &c->grants[i];
        if ((g->gen & 1u) != 0 && g->revoked && g->unannounced &&
            g->binding_cookie == binding_cookie &&
            g->session_cookie == session_cookie) {
            grant_free_slot(c, i);
            break;   /* at most one grant per {binding, session} */
        }
    }
    /* Retire every matching pending cancel too, across both sources. */
    for (uint32_t i = 0; i < c->max_cancels; i++) {
        r_pending_cancel *pc = &c->pending_cancels[i];
        if (pc->used && pc->binding_cookie == binding_cookie &&
            pc->session_cookie == session_cookie) {
            pc->used = false;
        }
    }
}

/* -- route dump (read-only introspection) -------------------------------------------------------
 *
 * A deterministic, entity-detailed snapshot of the routing state: the epoch
 * triple, announced namespaces, namespace watchers, and every live track
 * with its log watermarks and per-subscription cursor lag/state. Pure:
 * renders into a caller buffer, no mutation, no advancing calls, no
 * allocation. Ordering is pool-slot order, so the same state renders
 * identically every time. Same truncation contract as the metrics/trace
 * serializers (MOQR_OK only when content + NUL fit, else MOQR_ERR_CAPACITY
 * with the required length in *written). Unlike metrics, entity identity
 * (namespaces, names) IS included — this is an explicit operator dump, not
 * a bounded label space. */

typedef struct {
    char  *buf;
    size_t cap;
    size_t len;
} rd_writer_t;

static void
rd_addf(rd_writer_t *w, const char *fmt, ...)
{
    char  *dst = (w->len < w->cap) ? w->buf + w->len : NULL;
    size_t avail = (w->len < w->cap) ? w->cap - w->len : 0;
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(dst, avail, fmt, ap);
    va_end(ap);
    if (n > 0) {
        w->len += (size_t)n;
    }
}

/* Namespace/track-name bytes are arbitrary; render them as an all-ASCII,
 * always-valid JSON string (non-printables and non-ASCII as \u00XX) so a
 * binary name can never break the document or leak a raw byte. */
static void
rd_json_bytes(rd_writer_t *w, const uint8_t *data, uint32_t len)
{
    rd_addf(w, "\"");
    for (uint32_t i = 0; i < len; i++) {
        uint8_t ch = data[i];
        if (ch == '"' || ch == '\\') {
            rd_addf(w, "\\%c", ch);
        } else if (ch == '\n') {
            rd_addf(w, "\\n");
        } else if (ch == '\t') {
            rd_addf(w, "\\t");
        } else if (ch == '\r') {
            rd_addf(w, "\\r");
        } else if (ch >= 0x20 && ch < 0x7f) {
            rd_addf(w, "%c", ch);
        } else {
            rd_addf(w, "\\u%04x", ch);
        }
    }
    rd_addf(w, "\"");
}

/* Text form: printable ASCII passes; the path separator, backslash, quote,
 * and any non-printable render as \xXX so a part is never mistaken for a
 * boundary or blur the "name" quoting. */
static void
rd_text_bytes(rd_writer_t *w, const uint8_t *data, uint32_t len)
{
    for (uint32_t i = 0; i < len; i++) {
        uint8_t ch = data[i];
        if (ch >= 0x20 && ch < 0x7f && ch != '\\' && ch != '/' &&
            ch != '"') {
            rd_addf(w, "%c", ch);
        } else {
            rd_addf(w, "\\x%02x", ch);
        }
    }
}

/* Walk a trie node's namespace path (root-exclusive) into a bounded stack,
 * then render it leaf-last. json=true → ["a","b"]; json=false → a/b. */
static void
rd_node_ns(rd_writer_t *w, const moqr_core_t *c, uint32_t node, bool json)
{
    uint32_t stack[64];
    uint32_t depth = 0;
    for (uint32_t n = node; n != UINT32_MAX && depth < 64 &&
                            c->nodes[n].part != NULL;
         n = c->nodes[n].parent) {
        stack[depth++] = n;
    }
    if (json) {
        rd_addf(w, "[");
    }
    for (uint32_t i = depth; i > 0; i--) {
        const r_trie_node_t *nd = &c->nodes[stack[i - 1]];
        if (i != depth) {
            rd_addf(w, json ? "," : "/");
        }
        if (json) {
            rd_json_bytes(w, nd->part, nd->part_len);
        } else {
            rd_text_bytes(w, nd->part, nd->part_len);
        }
    }
    if (json) {
        rd_addf(w, "]");
    }
}

/* Render a track key's namespace parts (name excluded). */
static void
rd_key_ns(rd_writer_t *w, const r_key_t *k, bool json)
{
    uint32_t count = key_part_count(k);
    if (json) {
        rd_addf(w, "[");
    }
    for (uint32_t i = 0; i < count; i++) {
        moq_bytes_t p = key_part(k, i);
        if (i != 0) {
            rd_addf(w, json ? "," : "/");
        }
        if (json) {
            rd_json_bytes(w, p.data, (uint32_t)p.len);
        } else {
            rd_text_bytes(w, p.data, (uint32_t)p.len);
        }
    }
    if (json) {
        rd_addf(w, "]");
    }
}

/* Render an ns-sub prefix (key layout without the trailing name slot). */
static void
rd_prefix(rd_writer_t *w, const uint8_t *prefix, bool json)
{
    uint32_t count = key_rd32(prefix);
    uint32_t off = 4u * (1u + count);
    if (json) {
        rd_addf(w, "[");
    }
    for (uint32_t i = 0; i < count; i++) {
        uint32_t plen = key_rd32(prefix + 4u * (1u + i));
        if (i != 0) {
            rd_addf(w, json ? "," : "/");
        }
        if (json) {
            rd_json_bytes(w, prefix + off, plen);
        } else {
            rd_text_bytes(w, prefix + off, plen);
        }
        off += plen;
    }
    if (json) {
        rd_addf(w, "]");
    }
}

/*
 * Read-only cursor backlog: count the retained, in-range groups that still
 * owe this subscription a filter-passing record, and the oldest such group
 * (the delivery frontier). Mirrors sub_best_candidate's deliverability test
 * against the persisted cursor indices WITHOUT mutating them — a local copy
 * of the read position skips below-start records. `frontier` is UINT64_MAX
 * when nothing is owed (caught up, parked, or empty log).
 */
static void
sub_backlog(const moqr_core_t *c, const r_sub_t *s, const moqr_log_t *log,
            uint64_t *frontier, uint64_t *lag_groups)
{
    (void)c;
    *frontier = UINT64_MAX;
    *lag_groups = 0;
    uint32_t gn = moqr_log_group_count(log);
    for (uint32_t gi = 0; gi < gn; gi++) {
        uint64_t g = moqr_log_group_id_at(log, gi);
        if (g < s->start_group) {
            continue;
        }
        if (s->has_end && g > s->end_group) {
            break;   /* ids ascend */
        }
        const r_gpos_t *e = NULL;
        for (uint32_t i = 0; i < s->gpos_groups; i++) {
            const r_gpos_t *cand = gpos_at(s, i);
            if (cand->group_id == g) {
                e = cand;
                break;
            }
        }
        uint32_t lists = moqr_log_group_list_count(log, g);
        bool owed = false;
        for (uint32_t slot = 0; slot <= lists && !owed; slot++) {
            bool dg = slot == lists;
            uint32_t pos_slot = dg ? s->gpos_lists - 1 : slot;
            uint32_t list_ref = dg ? MOQR_LOG_LIST_DATAGRAM : slot;
            uint32_t pos = e != NULL ? e->idx[pos_slot] : 0;
            for (;;) {
                moqr_record_view_t v;
                if (moqr_log_read_rec(log, g, list_ref, pos, &v) != MOQR_OK) {
                    break;
                }
                if (rec_passes(s, &v)) {
                    owed = true;
                    break;
                }
                pos++;   /* local only: below filter start */
            }
        }
        if (owed) {
            (*lag_groups)++;
            if (*frontier == UINT64_MAX) {
                *frontier = g;
            }
        }
    }
}

static const char *
track_state_name(r_track_state_t st)
{
    switch (st) {
    case R_TRACK_PENDING: return "pending";
    case R_TRACK_ACTIVE:  return "active";
    case R_TRACK_WARM:    return "warm";
    default:              return "unknown";
    }
}

static const char *
sub_state_name(r_sub_state_t st)
{
    switch (st) {
    case R_SUB_PARKED: return "parked";
    case R_SUB_ACTIVE: return "active";
    default:           return "unknown";
    }
}

static void
rd_group_id(rd_writer_t *w, uint64_t id)
{
    if (id == UINT64_MAX) {
        rd_addf(w, "null");
    } else {
        rd_addf(w, "%llu", (unsigned long long)id);
    }
}

/* The bind-layer connection identity for a binding slot (the operator-
 * meaningful id, not the internal pool slot). Live announces/subs/ns-subs
 * imply a live binding, so the slot's current cookie is the right one. */
static uint64_t
rd_binding_cookie(const moqr_core_t *c, uint32_t slot)
{
    return c->bindings[slot].cookie;
}

moqr_result_t
moqr_core_route_dump_json(const moqr_core_t *c, char *buf, size_t cap,
                          size_t *written)
{
    if (written != NULL) {
        *written = 0;
    }
    if (c == NULL || buf == NULL) {
        return MOQR_ERR_INVAL;
    }
    rd_writer_t w = { buf, cap, 0 };
    rd_addf(&w, "{\"epochs\":{\"node\":%llu,\"shard\":%llu,\"route\":%llu}",
            (unsigned long long)c->node_epoch,
            (unsigned long long)c->shard_epoch,
            (unsigned long long)c->route_epoch);

    rd_addf(&w, ",\"announces\":[");
    bool first = true;
    for (uint32_t n = 0; n < c->node_count; n++) {
        if (!c->nodes[n].has_announce) {
            continue;
        }
        rd_addf(&w, first ? "{\"namespace\":" : ",{\"namespace\":");
        first = false;
        rd_node_ns(&w, c, n, true);
        rd_addf(&w, ",\"binding\":%llu}",
                (unsigned long long)rd_binding_cookie(c, c->nodes[n].ann_binding));
    }
    rd_addf(&w, "]");

    rd_addf(&w, ",\"namespace_subscriptions\":[");
    first = true;
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        if (!c->ns_subs[i].used) {
            continue;
        }
        rd_addf(&w, first ? "{\"prefix\":" : ",{\"prefix\":");
        first = false;
        rd_prefix(&w, c->ns_subs[i].prefix, true);
        rd_addf(&w, ",\"binding\":%llu}",
                (unsigned long long)rd_binding_cookie(c, c->ns_subs[i].binding));
    }
    rd_addf(&w, "]");

    rd_addf(&w, ",\"tracks\":[");
    first = true;
    for (uint32_t t = 0; t < c->max_tracks; t++) {
        const r_track_t *tr = &c->tracks[t];
        if ((tr->gen & 1u) == 0) {
            continue;
        }
        rd_addf(&w, first ? "{\"namespace\":" : ",{\"namespace\":");
        first = false;
        rd_key_ns(&w, &tr->key, true);
        rd_addf(&w, ",\"name\":");
        moq_bytes_t nm = key_name(&tr->key);
        rd_json_bytes(&w, nm.data, (uint32_t)nm.len);
        rd_addf(&w, ",\"state\":\"%s\",\"upstream_binding\":",
                track_state_name(tr->state));
        if (tr->has_upstream_binding) {
            rd_addf(&w, "%llu",
                    (unsigned long long)rd_binding_cookie(c, tr->up_binding));
        } else {
            rd_addf(&w, "null");
        }
        moqr_log_stats_t ls;
        moqr_log_get_stats(tr->log, &ls);
        rd_addf(&w, ",\"log\":{\"oldest_group\":");
        rd_group_id(&w, ls.oldest_group_id);
        rd_addf(&w, ",\"newest_group\":");
        rd_group_id(&w, ls.newest_group_id);
        rd_addf(&w,
                ",\"groups\":%u,\"records\":%llu,\"retained_bytes\":%llu,"
                "\"end_of_track\":%s}",
                ls.group_count, (unsigned long long)ls.record_count,
                (unsigned long long)ls.retained_bytes,
                ls.end_of_track ? "true" : "false");

        rd_addf(&w, ",\"subscriptions\":[");
        bool sfirst = true;
        for (uint32_t si = 0; si < c->max_subs; si++) {
            const r_sub_t *s = &c->subs[si];
            if ((s->gen & 1u) == 0 || s->track != t ||
                s->track_gen_slot != tr->gen) {
                continue;
            }
            rd_addf(&w, sfirst ? "{" : ",{");
            sfirst = false;
            rd_addf(&w,
                    "\"binding\":%llu,\"state\":\"%s\",\"start_group\":%llu,"
                    "\"start_object\":%llu,\"end_group\":",
                    (unsigned long long)rd_binding_cookie(c, s->binding),
                    sub_state_name(s->state),
                    (unsigned long long)s->start_group,
                    (unsigned long long)s->start_object);
            if (s->has_end) {
                rd_addf(&w, "%llu", (unsigned long long)s->end_group);
            } else {
                rd_addf(&w, "null");
            }
            uint64_t frontier = 0;
            uint64_t lag = 0;
            sub_backlog(c, s, tr->log, &frontier, &lag);
            bool before_ret = s->state == R_SUB_ACTIVE &&
                              ls.oldest_group_id != UINT64_MAX &&
                              s->start_group < ls.oldest_group_id;
            rd_addf(&w, ",\"lag_groups\":%llu,\"frontier_group\":",
                    (unsigned long long)lag);
            rd_group_id(&w, frontier);
            rd_addf(&w,
                    ",\"skip_pending\":%s,\"start_before_retention\":%s}",
                    s->pending_skip ? "true" : "false",
                    before_ret ? "true" : "false");
        }
        rd_addf(&w, "]}");
    }
    rd_addf(&w, "]}");

    if (cap > 0) {
        buf[w.len < cap ? w.len : cap - 1] = '\0';
    }
    if (written != NULL) {
        *written = w.len;
    }
    return w.len < cap ? MOQR_OK : MOQR_ERR_CAPACITY;
}

moqr_result_t
moqr_core_route_dump_text(const moqr_core_t *c, char *buf, size_t cap,
                          size_t *written)
{
    if (written != NULL) {
        *written = 0;
    }
    if (c == NULL || buf == NULL) {
        return MOQR_ERR_INVAL;
    }
    rd_writer_t w = { buf, cap, 0 };
    rd_addf(&w, "epochs: node=%llu shard=%llu route=%llu\n",
            (unsigned long long)c->node_epoch,
            (unsigned long long)c->shard_epoch,
            (unsigned long long)c->route_epoch);

    rd_addf(&w, "announces:\n");
    bool any = false;
    for (uint32_t n = 0; n < c->node_count; n++) {
        if (!c->nodes[n].has_announce) {
            continue;
        }
        any = true;
        rd_addf(&w, "  ");
        rd_node_ns(&w, c, n, false);
        rd_addf(&w, " -> binding %llu\n",
                (unsigned long long)rd_binding_cookie(c, c->nodes[n].ann_binding));
    }
    if (!any) {
        rd_addf(&w, "  (none)\n");
    }

    rd_addf(&w, "namespace_subscriptions:\n");
    any = false;
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        if (!c->ns_subs[i].used) {
            continue;
        }
        any = true;
        rd_addf(&w, "  ");
        rd_prefix(&w, c->ns_subs[i].prefix, false);
        rd_addf(&w, " -> binding %llu\n",
                (unsigned long long)rd_binding_cookie(c, c->ns_subs[i].binding));
    }
    if (!any) {
        rd_addf(&w, "  (none)\n");
    }

    rd_addf(&w, "tracks:\n");
    any = false;
    for (uint32_t t = 0; t < c->max_tracks; t++) {
        const r_track_t *tr = &c->tracks[t];
        if ((tr->gen & 1u) == 0) {
            continue;
        }
        any = true;
        rd_addf(&w, "  ");
        rd_key_ns(&w, &tr->key, false);
        rd_addf(&w, " \"");
        moq_bytes_t nm = key_name(&tr->key);
        rd_text_bytes(&w, nm.data, (uint32_t)nm.len);
        rd_addf(&w, "\" state=%s upstream_binding=", track_state_name(tr->state));
        if (tr->has_upstream_binding) {
            rd_addf(&w, "%llu\n",
                    (unsigned long long)rd_binding_cookie(c, tr->up_binding));
        } else {
            rd_addf(&w, "-\n");
        }
        moqr_log_stats_t ls;
        moqr_log_get_stats(tr->log, &ls);
        rd_addf(&w, "    log: groups=%u records=%llu bytes=%llu oldest=",
                ls.group_count, (unsigned long long)ls.record_count,
                (unsigned long long)ls.retained_bytes);
        rd_group_id(&w, ls.oldest_group_id);
        rd_addf(&w, " newest=");
        rd_group_id(&w, ls.newest_group_id);
        rd_addf(&w, " eot=%d\n", ls.end_of_track ? 1 : 0);

        rd_addf(&w, "    subs:\n");
        bool anysub = false;
        for (uint32_t si = 0; si < c->max_subs; si++) {
            const r_sub_t *s = &c->subs[si];
            if ((s->gen & 1u) == 0 || s->track != t ||
                s->track_gen_slot != tr->gen) {
                continue;
            }
            anysub = true;
            uint64_t frontier = 0;
            uint64_t lag = 0;
            sub_backlog(c, s, tr->log, &frontier, &lag);
            bool before_ret = s->state == R_SUB_ACTIVE &&
                              ls.oldest_group_id != UINT64_MAX &&
                              s->start_group < ls.oldest_group_id;
            rd_addf(&w,
                    "      binding=%llu state=%s start=%llu/%llu end=",
                    (unsigned long long)rd_binding_cookie(c, s->binding),
                    sub_state_name(s->state),
                    (unsigned long long)s->start_group,
                    (unsigned long long)s->start_object);
            if (s->has_end) {
                rd_addf(&w, "%llu", (unsigned long long)s->end_group);
            } else {
                rd_addf(&w, "-");
            }
            rd_addf(&w, " lag_groups=%llu frontier=", (unsigned long long)lag);
            rd_group_id(&w, frontier);
            rd_addf(&w, " skip_pending=%d start_before_retention=%d\n",
                    s->pending_skip ? 1 : 0, before_ret ? 1 : 0);
        }
        if (!anysub) {
            rd_addf(&w, "      (none)\n");
        }
    }
    if (!any) {
        rd_addf(&w, "  (none)\n");
    }

    if (cap > 0) {
        buf[w.len < cap ? w.len : cap - 1] = '\0';
    }
    if (written != NULL) {
        *written = w.len;
    }
    return w.len < cap ? MOQR_OK : MOQR_ERR_CAPACITY;
}
