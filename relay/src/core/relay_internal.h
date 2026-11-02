#ifndef MOQR_RELAY_INTERNAL_H
#define MOQR_RELAY_INTERNAL_H

#include <moq/relay/relay.h>

#include <moq/relay/capacity.h>

#include <moq/rcbuf.h>
#include "log_internal.h"
#include "retrieve_internal.h"

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


/* Reclaim reasons, carried in e1 of the track ROUTE_REMOVE trace record
 * (0 = an ordinary free). */
#define R_RECLAIM_CAPACITY 1u



/* REQUEST_ERROR codes (REJECT_SUB / reject_subscribe / cancel_namespace all
 * take this registry). */
#define R_ERR_DOES_NOT_EXIST 0x10u   /* mirrors MOQ_REQUEST_ERROR_DOES_NOT_EXIST */

#define R_ERR_INVALID_RANGE  0x11u   /* mirrors MOQ_REQUEST_ERROR_INVALID_RANGE */

#define R_ERR_INTERNAL       0x0u


/* PUBLISH_DONE status codes (SUB_DONE / done_subscribe take this registry —
 * a DIFFERENT namespace from REQUEST_ERROR and from stream-reset codes). */
#define R_DONE_TRACK_ENDED   0x2u    /* the track is no longer being published */

#define R_DONE_SUB_ENDED     0x3u    /* end of the subscription's filter range */



/* -- canonical full-track-name key ---------------------------------------- *
 * Layout: u32 part_count P | u32 part_len[P] | u32 name_len | part bytes...
 * | name bytes. Self-contained, hashable, prefix-walkable. */

typedef struct r_key {
    uint8_t *buf;
    uint32_t len;
} r_key_t;


/* -- pools ------------------------------------------------------------------ */

#define R_NODE_NIL UINT32_MAX


typedef struct r_trie_node {
    uint8_t  *part;        /* owned copy; NULL for root                    */
    uint32_t  part_len;
    uint32_t  parent;      /* UINT32_MAX for root                          */
    /* Children as an intrusive, ordered sibling list (R_NODE_NIL = none):
     * no per-node allocation, O(1) unlink, announce fan-out order kept. */
    uint32_t  first_child;
    uint32_t  last_child;
    uint32_t  next_sib;
    uint32_t  prev_sib;
    /* Live tracks whose src_ann_node names this node. Only a hint that lets
     * a free or move skip the track scan when nothing refers here. */
    uint32_t  src_refs;
    bool      has_announce;
    uint32_t  ann_binding;      /* binding slot                            */
    uint32_t  ann_binding_gen;
    uint32_t  cluster_route_index; /* exact projected route, including mirrors */
    uint64_t  session_cookie;   /* peer wire announce handle; 0 for mirrors */
} r_trie_node_t;


typedef struct r_cluster_route {
    bool used;
    bool mirrored;
    bool inherited_warm;
    bool warm_notified;
    uint32_t mirror_source_index;
    moqr_cluster_advertisement_id_t original;
    uint32_t node, binding, binding_gen, hop_count;
    uint64_t session_cookie, lifetime, arrival;
    uint64_t seed_cost, cost, source_peer_hop_id, source_correlation_id;
} r_cluster_route_t;


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

/* No source yet: every subscriber is a held rendezvous SUBSCRIBE (§9.20.6)
 * waiting for an announce or a PUBLISH to supply one. No upstream intent was
 * issued and no record can arrive. */
#define R_TRACK_RENDEZVOUS 4u


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
    bool            metadata_managed; /* sticky for this retained identity */
    uint32_t        up_binding;
    uint32_t        up_binding_gen;
    /* The announce that sourced this track, captured when the upstream was
     * chosen (subscribe-create / WARM rejoin / a self-owned publish_open).
     * UINT32_MAX = no announce source (an unannounced direct push). Force-withdraw
     * purges by this stored identity, never a re-derived longest-prefix. */
    uint32_t        src_ann_node;
    uint32_t        src_ann_binding;
    uint32_t        src_ann_binding_gen;
    uint64_t        cluster_route_lifetime;
    uint64_t        cluster_origin;
    uint64_t        cluster_requester_hop, cluster_requester_correlation;
    bool            cluster_group_floor_set;
    uint64_t        cluster_group_floor;
    uint64_t        upstream_cookie;
    bool            has_largest;
    uint32_t        fetch_count;      /* live fetch cursors reading it     */
    uint64_t        largest_group;
    uint64_t        largest_object;
    uint64_t        linger_deadline_us;   /* 0 = unarmed                   */
    /* Reclaim bookkeeping. interest_seq orders tracks by their most recent
     * interest change (create, subscribe, unsubscribe, fetch open/close,
     * source release): the reclaimable track with the smallest value is the
     * least recently used. idle_since_us is the tick time at which the track
     * was first seen reclaimable (0 = unarmed); any interest change disarms it. */
    uint64_t        interest_seq;
    uint64_t        idle_since_us;
    /* MAX_CACHE_DURATION (draft-22 §10.3) the source stated, in us: no
     * Object is started downstream once this long has passed since its
     * arrival. */
    bool            has_max_cache;
    uint64_t        max_cache_us;
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
 * per-sub block of max_groups strides; every uint64_t group_id is valid. */
typedef struct r_gpos {
    bool occupied;
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


_Static_assert(offsetof(r_gpos_t, idx) == sizeof(r_gpos_t),
               "gpos arrays must follow the complete entry header");


typedef struct r_sub {
    uint32_t      gen;            /* odd = live                            */
    uint32_t      binding;
    uint32_t      binding_gen;
    uint64_t      cluster_requester_hop, cluster_requester_correlation;
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
    bool          range_end_holds;/* an exhausted bounded range leaves the
                                   * sub established and idle instead of
                                   * retiring it with SUB_DONE (the draft-22
                                   * subscription contract). Also rides the
                                   * bool padding.                          */
    bool          rendezvous;     /* held for a publisher (R_TRACK_RENDEZVOUS)
                                   * until rendezvous_deadline_us; rides the
                                   * bool padding too                       */
    r_sub_state_t state;
    uint64_t      start_group;
    uint64_t      start_object;
    bool          has_end;
    /* The rendezvous deadline passed while the TIMEOUT could not be queued:
     * the subscription is over and owes that answer; no source resumes it.
     * Rides has_end's padding. */
    bool          rendezvous_expired;
    uint64_t      end_group;
    uint64_t      end_object;
    /* The track's source ended gracefully (moqr_core_source_finish) while
     * this sub still had content to deliver: it keeps delivering what was
     * admitted and retires with `src_done_pd` once nothing is left to send.
     * `src_abort` is the immediate counterpart (the source failed and the
     * terminal could not be queued at once): its first scan retires it with
     * `src_done_pd` before anything else is delivered. */
    bool          src_done;
    bool          src_abort;
    moqr_pd_desc_t src_done_pd;
    bool          has_location;
    bool          location_empty;
    moq_location_filter_cfg_t location; /* owned intent, including while parked */
    uint8_t      *excluded;       /* one bit per bounded log journal slot */
    uint32_t      excluded_cap;
    uint8_t       subscriber_priority;
    moqr_group_order_t group_order;
    moqr_filter_type_t filter_type;
    uint64_t      cookie;
    /* The absolute deadline of a held rendezvous, fixed by the binding at
     * the request's first receipt. */
    uint64_t      rendezvous_deadline_us;
} r_sub_t;


/* A track discovery subscription (SUBSCRIBE_TRACKS): the interest only. */
typedef struct r_tsub {
    bool      used;
    bool      forward;
    bool      include_properties;
    bool      held;        /* nothing offered until its acceptance is out */
    uint32_t  input_gen;   /* advances when its authorization tokens change */
    uint32_t  binding;
    uint32_t  binding_gen;
    moqr_group_order_t group_order;
    uint64_t  cookie;
    uint8_t  *prefix;      /* canonical ns-only buffer, as r_ns_sub_t */
    uint32_t  prefix_len;
} r_tsub_t;


/* What one track subscription was offered for one track source generation.
 * Valid only while track_slot_gen and track_gen still name the track's
 * current identity and source; otherwise nothing was offered. */
typedef struct r_offer {
    uint32_t track_slot_gen;
    uint8_t  state;        /* MOQR_DISCOVERY_* */
    uint64_t track_gen;
} r_offer_t;


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
    bool     sub_forward_off;
    bool     sub_rendezvous;
    uint64_t sub_rendezvous_deadline_us;
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


/* The authorization tokens a binding's long-lived request carries. */
typedef struct r_rtok {
    bool       used;
    uint32_t   binding;
    uint32_t   binding_gen;
    uint64_t   cookie;
    r_material mat;
} r_rtok_t;


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
    bool     cluster_negotiated;
    uint64_t peer_hop_id, relay_cost, correlation_id;
    bool     metadata_managed;
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
    /* The outstanding record's subgroup form and gap mark (see
     * moqr_core_internal_delivery_flags); kept with it across re-peeks. */
    bool            out_subgroup_properties;
    bool            out_follows_gap;
    moqr_reset_desc_t out_gap_reset;
    /* A track subscription of this binding may have an unoffered match: set
     * when one is added or changed or a track gains a source, cleared by the
     * discovery pull that finds nothing. */
    bool            discovery_dirty;
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
    uint64_t cluster_requester_hop, cluster_requester_correlation;
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
    /* Gap reporting (report_gaps): a peek that found the cursor ahead of the
     * next retained object (or the range end) reported the run's inclusive
     * end here; the next commit skips past it. Nothing is pinned for a gap
     * and a re-peek recomputes it. exhausted is set when a committed gap
     * reached the range end, so the next peek is END. report_unknown also
     * reports the runs the log has no evidence about, the same way. */
    bool     report_gaps;
    bool     report_unknown;
    /* report_pending: an Object still arriving at the cursor is reported as
     * a PENDING item (gap_* holds its Location) rather than WOULD_BLOCK. */
    bool     report_pending;
    bool     gap_active;
    uint64_t gap_group, gap_object;
    bool     exhausted;
    /* Outstanding peeked head (held pinned; reconstructed on re-peek). */
    bool         out_active;
    uint64_t     out_group, out_subgroup, out_object;
    uint8_t      out_priority;
    bool         out_datagram;
    moq_rcbuf_t *pin_payload, *pin_properties;
    uint64_t     pin_bytes;       /* charged against core->fetch_pin_used   */
    /* A retrieval owner (moqr_core_internal_retrieve_open): start_* and the
     * adm_end_* below are its admitted range, fixed for its life, while
     * cur_* and end_* are the current view inside it. adm_end_object is
     * UINT64_MAX for the whole end group. Its views report every Location
     * they do not serve and never fail on the retention horizon. */
    bool         retrieval_owner;
    uint64_t     adm_end_group, adm_end_object;
    uint64_t     retrieval_track_gen; /* the source generation admitted    */
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
    uint64_t warm_idle_us;       /* resolved; UINT64_MAX = never             */
    uint64_t interest_seq;       /* monotonic interest-change counter        */
    uint64_t tracks_evicted_capacity;
    uint64_t tracks_evicted_idle;
    uint64_t ns_nodes_reclaimed;
    /* Moves whenever admission room may have appeared (see r_room_changed). */
    uint64_t room_epoch;

    r_binding_t   *bindings;
    /* Binding-ready set: one bit per binding slot (ceil(max_bindings/64)
     * words) plus an exact running count. Produced by the core's readiness
     * transitions (ready_mark/ready_mark_track), consumed only through
     * moqr_core_drain_ready — nothing inside the core reads it. */
    uint64_t      *ready_words;
    uint32_t       ready_count;
    r_track_t     *tracks;
    r_sub_t       *subs;
    uint32_t       subs_reserved;   /* free slots held for later requests */
    uint32_t       intent_reserved; /* intent room held by prepared updates */
    r_trie_node_t *nodes;
    uint32_t       node_count;
    r_ns_sub_t    *ns_subs;
    r_tsub_t      *tsubs;        /* max_ns_subs track subscriptions        */
    r_offer_t     *offers;       /* max_ns_subs x max_tracks, by tsub row  */
    r_rtok_t      *rtoks;
    uint32_t       max_rtoks;

    bool cluster_enabled;
    uint64_t cluster_hop_id, cluster_sequence;
    uint32_t max_cluster_routes, max_cluster_hops;
    r_cluster_route_t *cluster_routes;
    uint64_t *cluster_hops;
    moqr_cluster_advertisement_id_t cluster_forced;

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

#ifdef MOQR_CORE_TESTING
    /* moqr_core_debug_hold_binding_close: the binding closes still to refuse
     * before any effect, how many were refused, and the cookie of the last. */
    uint32_t   dbg_close_hold;
    uint32_t   dbg_close_held;
    uint64_t   dbg_close_held_cookie;
#endif
};


/* -- small helpers ------------------------------------------------------------- */

/* Pool slot generations: odd = live, even = free. A live generation must fit
 * the 28-bit generation field of a packed handle (moq_handle_pack returns 0
 * for anything wider), so live generations run 1, 3, ... R_GEN_LAST_LIVE and
 * never wrap: a wrap would let a handle held across 2^27 reuses of one slot
 * alias the slot's new occupant. Releasing the last live generation leaves
 * the slot at R_GEN_RETIRED (even, wider than any handle), and a retired slot
 * is never handed out again, so a pool shrinks by one slot per 2^27 reuses of
 * a single slot. Every relay pool steps generations through these helpers. */
#define R_GEN_LAST_LIVE 0x0FFFFFFFu

#define R_GEN_RETIRED   (R_GEN_LAST_LIVE + 1u)



/* -- config / lifecycle -------------------------------------------------------------- */

#define RELAY_CFG_HAS(cfg, field)                        \
    (offsetof(moqr_core_relay_cfg_t, field) +            \
         sizeof(((moqr_core_relay_cfg_t *)0)->field) <= (cfg)->struct_size)



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


#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

moqr_pd_desc_t
core_local_done(moqr_pd_status_t status);

uint32_t
key_rd32(const uint8_t *p);

void
key_wr32(uint8_t *p, uint32_t v);

uint32_t
key_part_count(const r_key_t *k);

moq_bytes_t
key_part(const r_key_t *k, uint32_t i);

moq_bytes_t
key_name(const r_key_t *k);

bool
r_gen_openable(uint32_t gen);

uint32_t
r_gen_open(uint32_t gen);

uint32_t
r_gen_close(uint32_t gen);

bool
r_gen_renewable(uint32_t gen);

uint32_t
r_gen_renew(uint32_t gen);

bool
r_gen_reopens(uint32_t gen);

void *
r_alloc(moqr_core_t *c, size_t n);

void
r_free(moqr_core_t *c, void *p, size_t n);

void
r_trace(moqr_core_t *c, moqr_trace_kind_t kind, uint32_t detail,
        uint64_t e0, uint64_t e1, uint64_t e2, uint64_t e3);

void
r_bump_route_epoch(moqr_core_t *c);

void
r_room_changed(moqr_core_t *c);

uint64_t
r_pack(const moqr_core_t *c, uint32_t pool, uint32_t gen, uint32_t slot);

bool
rendezvous_timeout_owed(const moqr_core_t *c, uint32_t tslot);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#include "auth_internal.h"

#include "bindings_internal.h"

#include "delivery_internal.h"

#include "fetch_internal.h"

#include "intents_internal.h"

#include "namespace_internal.h"

#include "subscribe_internal.h"

#include "tracks_internal.h"

#endif /* MOQR_RELAY_INTERNAL_H */
