#ifndef MOQR_SHARDS_INTERNAL_H
#define MOQR_SHARDS_INTERNAL_H

#include <moq/relay/moqr_shards.h>
#if defined(MOQR_RELAY_INSPECT) || defined(MOQR_BIND_TESTING)
#include <moq/relay/moqr_shards_test.h>
#endif

#include <moq/relay/capacity.h>
#include "../bind/moqr_bind_metadata.h"
#include "../bind/moqr_bind_retrieval.h"
#include "../core/retrieve_internal.h"

#include <moq/rcbuf.h>

#include <pthread.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>


/* Forward decls: struct moqr_shards holds these by pointer. */
typedef struct r_mgr r_mgr;

typedef struct r_mailbox r_mailbox;

typedef struct r_demand_channel r_demand_channel_t;


/* One shard = one single-writer relay: a core, the session binding that drives
 * it, the core's borrowed trace ring, and (for K>1) the cross-shard control
 * manager. */
typedef struct r_shard {
    moqr_core_t  *core;
    moqr_bind_t  *bind;
    moqr_trace_t *trace;   /* borrowed by the core; freed after it */
    r_mgr        *mgr;      /* NULL at K == 1 (structurally inert) */
    /* Producer-credit accumulator: bit src set when this shard's stepper
     * durably popped a src->this demand-channel slot (capacity freed toward
     * src). Written only by this shard's own stepper (single writer), zeroed
     * at step entry, merged into the step's wake mask at exit. A redundant
     * wake is acceptable; a lost credit wake is not. */
    uint64_t      pop_wake;
} r_shard_t;


struct moqr_shards {
    moq_alloc_t            alloc;
    uint16_t               shard_count;
    r_shard_t             *shards;        /* [shard_count] */
    moqr_place_fn          placement;
    moqr_placement_state_t place_state;
    uint64_t               permute_seed;
    uint64_t               round;         /* barrier counter */
    /* Cross-shard control mailboxes (K>1 only): one directed coalescing mailbox per (src,dst),
     * indexed src*shard_count + dst; the diagonal is unused. */
    r_mailbox            *mbox;
    uint32_t              mbox_cap;        /* entries per mailbox */
    bool                  cluster_enabled;
    uint32_t              cluster_routes;
    uint32_t              cluster_hops;
    /* Directed FIFO demand channels (K>1 only), indexed src*shard_count+dst
     * like the mailboxes; the diagonal is unused. */
    r_demand_channel_t   *dch;
    uint32_t              dch_cap;         /* entries per channel */
    uint32_t              jrn_cap;         /* per-shard journal entries */
    uint32_t              pend_cap;        /* per-shard pending-demand entries */
    /* Visibility policy for inbound control state. false (default): the
     * deterministic round barrier — pushed in round r, visible at r+1 under
     * moqr_shards_step. true: pushes are visible as soon as the mailbox mutex
     * publishes them, for free-running per-shard steppers with no shared
     * round. Set only while quiesced (no steppers running). */
    bool                  live_visibility;
    /* Owner-side demand admission. false (default): every forwarded demand is
     * refused with NOT_SUPPORTED. true: the owner admits it as an ordinary
     * subscribe on the requester's pseudo-binding, round-trips the full
     * ACK/DONE lifecycle, and pumps admitted data — whole-object and
     * chunked/live-edge. Off in production until the data path is complete
     * end to end. */
    bool                  admit_remote;
    /* Per directed channel LOGICAL-BYTE cap (payload + properties + control
     * canon bytes); data pushes gate on it, control is counted but exempt. */
    uint64_t              dch_byte_cap;
    /* Data-pump turn budgets (see the cfg doc). */
    uint32_t              pump_turn_msgs;
    uint64_t              pump_turn_bytes;
    /* Per-demand subgroup-progress slots (see the cfg doc). */
    uint32_t              sg_slots;
    /* Fill-retrieval slots per shard and role (admission only): the core's
     * fetch-cursor pool size, the bound on fills a shard can serve at once
     * and on the cursors an owner can lend to retrievals. */
    uint32_t              fill_cap;
    /* Sticky per-directed-channel CTRL/DATA arbitration tokens, indexed like
     * the channels (src*K + dst). PRODUCER-owned: only the src shard's
     * stepper reads or writes row src, so no lock is needed. A token
     * transfers only on a successful preferred-class enqueue — never on
     * schedule position — which is what keeps an adversarially timed
     * consumer from letting fresh control starve data (or vice versa). */
    uint8_t              *arb;
};


#define R_ARB_CTRL 0u

#define R_ARB_DATA 1u

#define R_MIRROR_NONE        (-1)

#define R_OP_ANNOUNCE        1u

#define R_OP_UNANNOUNCE      2u

#define R_NS_MAX_PARTS       32u   /* the core bounds a namespace to 32 parts */


/* Canonical-key byte ceilings, tied to the core's limits by construction:
 * a namespace key is 4 (count) + 4 per part + the shared 4096-byte
 * namespace/full-track byte cap; a full-track key adds one 4-byte name
 * length (the name shares the same 4096 cap). Static-asserted so a drift in
 * either constant breaks the build, not the capacity model. */
#define R_CANON_NS_MAX (4u + 4u * R_NS_MAX_PARTS + MOQ_FULL_TRACK_NAME_MAX)

#define R_CANON_FT_MAX (R_CANON_NS_MAX + 4u)

_Static_assert(R_CANON_NS_MAX == 4228u, "namespace canon ceiling drifted");

_Static_assert(R_CANON_FT_MAX == 4232u, "full-track canon ceiling drifted");

/* Wire REQUEST_ERROR code refused cross-shard subscribes carry, mirroring
 * MOQ_REQUEST_ERROR_NOT_SUPPORTED (moq/session.h) the way the relay core mirrors
 * the other R_ERR_* codes — the shard layer never links the session headers. */
#define R_ERR_NOT_SUPPORTED  0x3u

/* Wire REQUEST_ERROR code carried by a topology force-withdrawal's terminals
 * (a split-brain loser's subscribers and its publisher cancel), mirroring
 * MOQ_REQUEST_ERROR_GOING_AWAY (moq/session.h): the publisher is withdrawn by
 * relay topology, not refused by auth or capability. */
/* REQUEST_ERROR registry: the relay (as the subscriber side of the loser's
 * PUBLISH_NAMESPACE) is no longer interested in this namespace — the remote
 * winner owns it. Valid in both drafts' REQUEST_ERROR tables, and the
 * cancel_namespace message takes exactly this registry. */
#define R_ERR_UNINTERESTED   0x20u

/* REQUEST_ERROR registry: the demand could not be completed on this route
 * because ownership moved mid-flight. Generic rather than TIMEOUT (0x2): no
 * implementation timeout elapsed, which is the only thing draft-18
 * Section 10.6 lets TIMEOUT mean. Assigned 0x0 in both registries. */
#define R_ERR_INTERNAL       0x0u

/* Wire REQUEST_ERROR code for an owner-side admission failure that has no
 * protocol meaning of its own (pump-sub table full, subscribe hard error),
 * mirroring MOQ_REQUEST_ERROR_INTERNAL (moq/session.h). */
#define R_ERR_INTERNAL       0x0u

/* Wire REQUEST_ERROR code for a Joining FETCH whose joined subscription's
 * track is no longer the one its name resolves to, mirroring
 * MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID (moq/session.h). */
#define R_ERR_INVALID_JOINING 0x32u



/* -- candidate-set journal (sorted by canonical key) ------------------------ */

/* One namespace's candidate set. The two echo counters are the self-echo
 * suppression, and they are allocation-free: the manager fans an NS_FOUND/NS_GONE
 * to its own wildcard watcher on every mirror announce/unannounce, so it bumps the
 * matching counter (only AFTER the core op succeeds — the echo is queued to the
 * watcher's intent ring and drained a round later, never delivered synchronously,
 * so the counter is always set before its echo can be observed). The router,
 * seeing an NS_FOUND/NS_GONE, consumes one pending echo of that op on this entry
 * and drops it; with no pending echo it is a real local publisher event. Matching
 * ignores the announcer origin (the intent lacks it), which is safe: a real local
 * publisher cannot produce a same-op event on a namespace whose mirror is
 * installed — the core rejects a local announce behind a mirror, and a local
 * unannounce needs a prior local announce. Living in the entry means there is no
 * token pool and no post-mutation allocation that could fail and then mis-classify
 * the next echo as a real local event. */
typedef struct r_jentry {
    bool     used;
    uint8_t *canon;        /* owned canonical key */
    uint32_t canon_len;
    uint64_t hash;         /* canon_hash, for HRW */
    uint64_t candidates;   /* bit i = shard i announces this ns */
    int32_t  mirror;       /* installed PB-mirror origin, or R_MIRROR_NONE */
    uint64_t sent;         /* bit d = the local bit value dest d was last told */
    uint16_t echo_ann;     /* pending self-echoes of our own mirror announces */
    uint16_t echo_unann;   /* pending self-echoes of our own mirror unannounces */
    bool     dirty;        /* candidates changed → reconcile in phase 3 */
    bool     holdout;      /* a local publisher that lost to a remote winner */
    bool     cap_blocked;  /* mirror install refused for lack of core room */
} r_jentry;


/* Owned snapshot of one original route slot. The exporter keeps the current
 * state until every destination accepted it. No borrowed core view crosses a
 * step or a mailbox lock. */
typedef struct r_cluster_snapshot {
    bool present;
    uint8_t *canon;
    uint32_t canon_len;
    uint64_t *hops;
    moqr_cluster_export_t record;
    uint64_t sent;
} r_cluster_snapshot;


typedef struct r_ctrl_msg {   /* one namespace's latest net state (coalesced) */
    bool     used;
    uint64_t round;        /* barrier stamp: visible at round + 1 */
    uint8_t  op;           /* R_OP_ANNOUNCE = present, R_OP_UNANNOUNCE = absent */
    uint16_t origin;       /* the announcing shard (constant per directed box) */
    uint8_t *canon;        /* owned */
    uint32_t canon_len;
    bool cluster;
    uint32_t source_index;
    uint64_t *hops;       /* owned; copied before publishing the slot */
    moqr_cluster_export_t record;
} r_ctrl_msg;


/* A directed control channel is a COALESCING set of per-namespace net states, not
 * an event queue: a namespace's announce and later withdraw collapse onto the same
 * slot, and each namespace's slot is applied independently, so a slot blocked on a
 * full receiver journal never heads-of-line the others (state-convergent; a full
 * mailbox is lag, never loss). */
struct r_mailbox {
    r_ctrl_msg *slots;
    uint32_t    cap, count;
    /* Cluster-only extra slot in the create-time allocation. Publication is
     * allocation-free, independent of refused admissions; never ANNOUNCE. */
    r_ctrl_msg *withdrawal;
    /* Leaf lock for this ONE directed mailbox: the producer (the source
     * shard's outbound phase) and the consumer (the destination shard's
     * inbound phase) may run on different threads; every slot/count access
     * happens under it. Strictly a leaf — critical sections call journal
     * helpers, the shared allocator, and (for cluster snapshots) the owning
     * shard's non-callback route import. They never enter another mailbox,
     * a shard phase, or anything that can call back in, so the lock order
     * "caller's shard domain -> one mailbox -> nothing" has no cycles. The
     * deterministic runner takes it uncontended (same code path). */
    pthread_mutex_t mu;
};


/* One recorded remote-owner demand. The router deep-copies the borrowed
 * UPSTREAM_SUBSCRIBE intent's ns+name into an owned canonical key at
 * observation time (a borrowed view can't be parked); scalars identify the
 * requesting track for the round-trip. States: RECORDED (not yet sent — the
 * demand phase probes local liveness, then forwards), SENT (awaiting the
 * owner's answer), ACKED (the owner accepted; the local track is ACTIVE with
 * upstream_cookie = demand_id, so a later UPSTREAM_UNSUBSCRIBE correlates
 * back here), UNDEMANDING (locally dead after send — the cancel notice still
 * owes the owner a message). */
#define D_ST_RECORDED    1u

#define D_ST_SENT        2u

#define D_ST_UNDEMANDING 3u

#define D_ST_ACKED       4u

#define D_ST_TERMINATING 5u   /* local capacity terminal committing: the

                               * source_done may WOULD_BLOCK, so the state
                               * holds until it lands, then UNDEMANDING */
typedef struct r_pdemand {
    moqr_track_t track;
    uint64_t     track_gen;
    uint64_t     demand_id;    /* per-shard monotonic; the correlation key */
    uint8_t     *canon;        /* owned ns+name key (canon2 layout)        */
    uint32_t     canon_len;
    uint16_t     origin;       /* owner shard the mirror pointed at        */
    moqr_cluster_advertisement_id_t     source_advertisement;
    uint64_t     requester_hop_id, requester_correlation_id;
    uint8_t      state;
    uint32_t     term_code;    /* TERMINATING: the wire code to commit     */
    bool         term_overrun; /* TERMINATING: count as table overrun, not
                                * ingest capacity                          */
    bool         term_quiet;   /* TERMINATING: an ordinary local stop (idle
                                * unsubscribe), not a loss — no metric     */
    uint32_t     popen_row;    /* owned progress row, UINT32_MAX = none    */
    /* ACKED: the owner track's Largest at acceptance, if it had one. This
     * shard's feed carries what follows it; a fill over Locations at or
     * below it retrieves them from the owner. */
    bool         ack_has_largest;
    uint64_t     ack_largest_group;
    uint64_t     ack_largest_object;
    /* The lowest group any record this demand's feed brought belongs to
     * (feed_seen once one has been ingested here). */
    bool         feed_seen;
    uint64_t     feed_min_group;
    /* The first ACK binds the remote full-track identity. Its applied
     * revision also makes a held ACK retry memory-only after copy refusal. */
    bool         metadata_bound;
    moqr_track_t metadata_track;
    uint64_t     metadata_track_gen;
    uint64_t     metadata_revision;
} r_pdemand;


/* Cross-shard demand messages: an ordered request/reply vocabulary, carried
 * by the FIFO demand channel below — NEVER by the coalescing announce
 * mailbox, whose latest-wins semantics would drop or reorder them. */
#define D_MSG_DEMAND    1u

#define D_MSG_UNDEMAND  2u

#define D_MSG_DONE      3u

#define D_MSG_ACK       4u

/* Data kinds (owner -> requester). OBJ carries a whole/status/datagram
 * record; OPEN/CHUNK/END stream a chunked or live-edge object; OBJ_RESET /
 * GRP_RESET abandon begun objects (per-object exact identity / whole
 * evicted group), GRP_EVICT crosses the acknowledged eviction watermark
 * (group_id = oldest retained group), and SG_SEAL crosses the durable
 * subgroup FIN. An unknown kind arriving is a fail-stop, never a silent
 * success. */
#define D_MSG_OBJ       5u

#define D_MSG_OBJ_OPEN  6u

#define D_MSG_OBJ_CHUNK 7u

#define D_MSG_OBJ_END   8u

#define D_MSG_OBJ_RESET 9u

#define D_MSG_GRP_RESET 10u

#define D_MSG_GRP_EVICT 11u

#define D_MSG_SG_SEAL   12u

/* Fill retrieval: a shard serving a fill over a range that precedes its own
 * feed of a remote-owned track asks the owner for exactly that range. The
 * requester's REQ / MORE / CANCEL are control; the owner's OPEN / ITEM / END
 * answer is data, ordered by the same FIFO. These kinds sit outside the
 * public per-kind vocabulary (moqr_shards_msg_kind_t), so they are
 * neither counted in enqueued[] nor, for the control kinds, in the pair
 * control_messages total; the answer kinds count as data like every other
 * data message.
 * - REQ: retrieve [group_id/object_id .. fill_end] for demand_id, with
 *   fill_credit items granted and the fill's deadline.
 * - MORE: grant fill_credit further items.
 * - CANCEL: the requester's fill ended before the answer did; the owner
 *   drops its side at once instead of at the deadline.
 * - OPEN: the owner accepted responsibility for answering the range.
 * - ITEM: a typed OBJECT, classified GAP or ERROR; each spends one credit.
 *   ERROR reason bytes use the owned props clone, never a borrowed pointer.
 * - END: proved coverage end, or an allocation-free child-local ERROR.
 * A standalone FETCH whose track this shard does not own rides the same kinds
 * with demand_id 0 and fetch_standalone set: REQ carries the request's own
 * key and range form (the owner resolves and admits it), and OPEN carries the
 * owner's admission -- REJECT ends the retrieval there, ACCEPT carries the
 * resolved range and FETCH_OK fields, and the answer follows as above. */
#define D_MSG_FILL_REQ    13u

#define D_MSG_FILL_MORE   14u

#define D_MSG_FILL_CANCEL 15u

#define D_MSG_FILL_OPEN   16u

#define D_MSG_FILL_ITEM   17u

#define D_MSG_FILL_END    18u

/* Latest Track Properties, including UNAVAILABLE, follow the initial ACK
 * in this same FIFO. Private control, outside the frozen public counters. */
#define D_MSG_METADATA    19u


/* Pointer-free retrieval result. OBJECT buffers and ERROR reason bytes travel
 * in the message/window's owned payload/props clones, charged to its channel. */
typedef struct r_fill_result {
    moqr_retrieve_kind_t kind;
    moqr_retrieve_location_t start, end;
    moq_fetch_range_kind_t gap_kind;
    moqr_retrieve_error_kind_t error_kind;
    moqr_result_t local_result;
    moq_result_t session_result;
    uint64_t error_code, retry_after_ms;
    moq_request_terminal_cause_t cause;
    bool can_retry, data_stream, stop_sending;
} r_fill_result_t;


/* Only values cross lanes. The bytes are an independent message-owned
 * props clone, never the owner's metadata reference or storage pointer. */
typedef struct r_metadata_stamp {
    moqr_track_t          track;
    uint64_t              track_gen;
    moqr_bind_meta_state_t state;
    moq_version_t         wire_version;
    moqr_binding_t        source_binding;
    uint64_t              source_handle;
    uint64_t              source_generation;
    uint64_t              revision;
} r_metadata_stamp_t;

/* The public per-kind counter index (moqr_shards_msg_kind_t) is D_MSG_* - 1
 * by construction; a drift in either vocabulary breaks the build here, not
 * the accounting. */
_Static_assert(D_MSG_DEMAND == MOQR_SHARDS_MSG_DEMAND + 1u &&
                   D_MSG_UNDEMAND == MOQR_SHARDS_MSG_UNDEMAND + 1u &&
                   D_MSG_DONE == MOQR_SHARDS_MSG_DONE + 1u &&
                   D_MSG_ACK == MOQR_SHARDS_MSG_ACK + 1u &&
                   D_MSG_OBJ == MOQR_SHARDS_MSG_OBJ + 1u &&
                   D_MSG_OBJ_OPEN == MOQR_SHARDS_MSG_OBJ_OPEN + 1u &&
                   D_MSG_OBJ_CHUNK == MOQR_SHARDS_MSG_OBJ_CHUNK + 1u &&
                   D_MSG_OBJ_END == MOQR_SHARDS_MSG_OBJ_END + 1u &&
                   D_MSG_OBJ_RESET == MOQR_SHARDS_MSG_OBJ_RESET + 1u &&
                   D_MSG_GRP_RESET == MOQR_SHARDS_MSG_GRP_RESET + 1u &&
                   D_MSG_GRP_EVICT == MOQR_SHARDS_MSG_GRP_EVICT + 1u &&
                   D_MSG_SG_SEAL == MOQR_SHARDS_MSG_SG_SEAL + 1u &&
                   D_MSG_SG_SEAL == MOQR_SHARDS_MSG__COUNT,
               "message-kind vocabularies drifted");

typedef struct r_demand_msg {
    uint8_t      kind;
    bool         pre_ack;      /* DONE: refusal before any accept          */
    uint64_t     error_code;   /* DONE                                     */
    uint64_t     demand_id;
    moqr_track_t track;        /* the REQUESTER's track handle             */
    uint64_t     track_gen;
    moqr_cluster_advertisement_id_t     source_advertisement;
    uint64_t     requester_hop_id, requester_correlation_id;
    uint8_t     *canon;        /* DEMAND: owned ns+name key (msg-owned)    */
    uint32_t     canon_len;
    bool         has_largest;  /* ACK: the owner track's largest location  */
    uint64_t     largest_group;
    uint64_t     largest_object;
    r_metadata_stamp_t metadata;   /* ACK / METADATA; props = Track Properties */
    /* Data (D_MSG_OBJ): one COMPLETE record. payload/props are CLONES made
     * with the shards allocator — the message owns the sole reference until
     * the requester's ingest takes it (transfer-on-OK) or the message is
     * consumed/destroyed (decref exactly once). adv_subgroup_end is ADVISORY
     * metadata only: the durable seal crosses as its own message kind. */
    uint64_t     group_id;
    uint64_t     subgroup_id;
    uint64_t     object_id;
    uint64_t     declared_len;  /* OBJ_OPEN: wire payload length up front */
    /* OBJ_RESET / GRP_RESET / reset-flavored SG_SEAL: the 62-bit protocol
     * reset code, bit-exact — never folded into the 32-bit DONE error
     * field. */
    moqr_reset_desc_t reset;   /* tagged terminal cause: the origin
                                * must survive the cross-shard hop */
    moqr_pd_desc_t    pd;      /* DONE: the tagged PUBLISH_DONE
                                * terminal, origin preserved      */
    bool         seal_reset;   /* SG_SEAL: the terminal was a RESET — apply
                                * as a reset-flavored seal carrying
                                * reset_code, never a clean FIN (the code
                                * alone cannot discriminate: 0 is a legal
                                * application reset code)                  */
    uint8_t      prio;
    uint8_t      status;
    bool         datagram_pref;
    bool         end_of_group;
    bool         first_object;
    bool         adv_subgroup_end;
    moq_rcbuf_t *payload;
    moq_rcbuf_t *props;
    /* OBJ / OBJ_OPEN / FILL_ITEM: the codec `props` were stated in at the
     * owner (MOQR_PROPS_CODEC_*), and whether the upstream subgroup header
     * set PROPERTIES; the requester's ingest states both as received. */
    uint8_t      props_codec;
    bool         subgroup_props;
    uint64_t     bytes_accounted;   /* gauge contribution fixed at push (an
                                     * ownership transfer nulls the refs
                                     * before pop, so pop cannot recount) */
    uint64_t     round;        /* visibility stamp (deterministic barrier) */
    /* Fill retrieval (D_MSG_FILL_*): the requester's retrieval id; REQ
     * carries the range start in group_id/object_id and its inclusive end
     * here, with the fill's order, priority (prio), deadline and the credit
     * granted; MORE carries credit; ITEM/END carry the pointer-free typed
     * result plus the owned record/reason bytes above. */
    uint64_t     fill_id;
    uint64_t     fill_end_group;
    uint64_t     fill_end_object;
    uint64_t     fill_deadline_us;
    uint32_t     fill_credit;
    uint8_t      fill_order;
    r_fill_result_t fill_result;
    /* Standalone FETCH retrieval (see D_MSG_FILL_*). REQ: the range form and
     * relative start of the request whose raw range is group_id/object_id ..
     * fill_end_group/fill_end_object. OPEN: the admission (error_code on
     * REJECT); on ACCEPT the inclusive range group_id/object_id ..
     * fill_end_group/fill_end_object, End Of Track, and the FETCH_OK End
     * Location's raw object field (its group is fill_end_group).
     * fetch_capacity: a REJECT because the owner had no room for it. */
    bool         fetch_standalone;
    bool         cluster_selected;
    bool         fetch_eot;
    bool         fetch_capacity;
    uint32_t     fetch_admit;
    uint32_t     fetch_range_form;
    uint64_t     fetch_relative_start;
    uint64_t     fetch_end_raw_object;
    /* REQ of a Joining FETCH: the demand whose feed the joined subscription
     * reads (0 for any other FETCH). */
    uint64_t     fetch_join_demand;
} r_demand_msg_t;


/* A directed FIFO demand channel: bounded ring, one producer (src shard) and
 * one consumer (dst shard), serialized by its own leaf mutex — the identical
 * lock discipline as the announce mailbox (caller's shard domain -> this one
 * mutex -> nothing). The consumer peeks the head and pops ONLY once the
 * message's outcome is durable elsewhere; a full ring backpressures the
 * producer's per-demand state machine. Latest metadata coalesces in that
 * state until pushed; queued revisions and DATA share this fixed entry cap.
 * Their order (ACK-before-latest/data, data-before-DONE) is the FIFO itself. */
struct r_demand_channel {
    r_demand_msg_t *slots;
    uint32_t        cap, head, count;
    uint64_t        bytes;   /* queued logical bytes (under mu)          */
    /* Fill-retrieval answer items the consumer has taken out of the ring
     * into a retrieval's window but not yet served: they stay charged to
     * this channel, entry and bytes, until the fill consumes them, so the
     * channel's caps (and the capacity ceiling built on them) bound them
     * exactly as if they were still queued. Under mu. */
    uint32_t        held;
    uint64_t        held_bytes;
    /* High-water marks for this ONE directed channel, updated by the
     * producer inside the successful-push critical section and read by the
     * consumer-side stats snapshot under the same leaf mutex. */
    uint32_t        count_hwm;
    uint64_t        bytes_hwm;
    /* Per-directed-pair cumulative accounting, same locking discipline as
     * the HWMs (producer writes inside the push critical section; the
     * refusal split counts THE predicate that fired, entries checked
     * first, one increment per refusal event). Data and control are
     * counted separately — they are compared against different oracles
     * (pump_messages/pump_bytes vs the per-kind enqueued[] vector). */
    uint64_t        pair_data_msgs;
    uint64_t        pair_data_bytes;
    uint64_t        pair_ctrl_msgs;
    uint64_t        refused_entries;
    uint64_t        refused_bytes;
    pthread_mutex_t mu;
};


/* One admitted remote demand on the OWNER: the pump-sub opened on the
 * requester's pseudo-binding plus the reply obligations the router records
 * against it. The router (phase 2) only marks flags here; the demand phase
 * (phase 5) pushes the replies and runs the staged teardown, so channel
 * pushes never happen inside the bind pump. {otrack, otrack_gen} are
 * captured at admission while the pump-sub is live — every generation-bump
 * path needs zero subscribers, so the pair stays current until the pump-sub
 * itself is retired, which is exactly when stage 2 consumes it. */
typedef struct r_psub {
    bool         used;
    bool         undemanding;      /* requester cancelled: staged teardown  */
    bool         terminating;      /* owner-side capacity terminal: the
                                    * outstanding delivery was released via
                                    * STREAM_ERROR (pump-sub retired), the
                                    * captured-track cancel then the DONE
                                    * stage each retry independently        */
    bool         term_cancelled;   /* the captured-track stage committed    */
    bool         pump_sub_retired; /* stage 1 ran (or the sub never lived)  */
    bool         has_otrack;       /* stage 2 has a track to cancel         */
    bool         ack_pending;      /* ACCEPT_SUB seen, ACK not yet pushed   */
    bool         ack_sent;
    bool         done_pending;     /* REJECT_SUB/SUB_DONE seen, DONE owed   */
    bool         done_pre_ack;
    uint64_t     done_code;
    moqr_pd_desc_t done_pd;   /* the tagged terminal to forward */
    bool         has_largest;      /* ACK payload, from ACCEPT_SUB          */
    uint64_t     largest_group;
    uint64_t     largest_object;
    uint16_t     src;              /* the requesting shard                  */
    uint64_t     demand_id;
    moqr_track_t rtrack;           /* the REQUESTER's track (reply echo)    */
    uint64_t     rtrack_gen;
    moqr_sub_t   sub;              /* the owner-side pump-sub               */
    moqr_track_t otrack;           /* the OWNER track it joined             */
    uint64_t     otrack_gen;
    moqr_bind_meta_ref_t ack_metadata;    /* exact executing reply, not latest */
    moqr_bind_meta_ref_t latest_metadata; /* one coalesced owner-lane pin      */
    bool         metadata_pending;
    uint64_t     metadata_sent_revision;
} r_psub;


/* One subgroup-progress slot: bounded per-demand state keyed by
 * (group, subgroup), mirroring the session binding's per-conn subgroup
 * lifetime — the slot survives across completed objects (only the OBJECT
 * fields reset at OBJ_END) and frees only on terminal/teardown. On the
 * OWNER it is the chunk-resume cursor (never duplicate an OBJ_OPEN or a
 * chunk across channel-full / budget / STALLED retries); on the REQUESTER
 * it is the open-object bookkeeping the staged terminals abandon. */
typedef struct r_psg {
    bool     used;
    bool     object_open;
    /* Requester only: the OPEN was rejected TOO_OLD (below the local
     * horizon), so the object has no log record — its chunk/END sequence is
     * consumed loss-visibly instead of appended. Cleared with the object
     * fields at OBJ_END. */
    bool     discarding;
    uint64_t group;
    uint64_t subgroup;
    uint64_t object_id;
    uint32_t next_chunk;
} r_psg_t;


/* -- fill retrieval state ---------------------------------------------------- *
 * A shard whose feed of a remote-owned track began at the owner's ACK serves
 * a fill over Locations at or below the ACK's Largest by retrieving them from
 * the owner (the binding's fill source, see shards_fill_open). The requester
 * keeps one r_fret per claimed range; the owner keeps one r_oret per
 * retrieval it is answering, using the same cache/origin coverage cursor as
 * a local fill. The owner bind owns upstream events and collector timers;
 * shards_fill_next_deadline adds cursor no-origin waits. Neither a credit
 * stall nor another cache hole renews the deadline. Flow control is a credit
 * window: the owner sends at most the
 * items granted, the requester grants a new window once the fill has
 * consumed the last. Window items stay charged to the channel that carried
 * them. */
#define R_FILL_WINDOW 4u


typedef struct r_fitem {
    r_fill_result_t result;
    uint64_t     group_id;
    uint64_t     subgroup_id;
    uint64_t     object_id;
    uint8_t      prio;
    uint8_t      status;
    bool         datagram_pref;
    bool         end_of_group;
    uint64_t     declared_len;
    moq_rcbuf_t *payload;       /* owned clone (OBJECT)                   */
    moq_rcbuf_t *props;         /* OBJECT properties / ERROR reason       */
    uint8_t      props_codec;   /* OBJECT properties' MOQR_PROPS_CODEC_*  */
    uint64_t     charged;       /* bytes still charged to the channel     */
} r_fitem_t;


typedef struct r_fret {
    bool      used;
    bool      req_owed;     /* REQ not yet pushed                         */
    bool      more_owed;    /* a credit window not yet pushed             */
    bool      cancel_owed;  /* CANCEL not yet pushed                      */
    bool      closed;       /* the binding ended its claim                */
    bool      begun;        /* the owner accepted the range (OPEN)        */
    bool      ended;        /* END or ERROR arrived                       */
    bool      abandoned;    /* deadline passed before the answer began    */
    bool      synth_peeked; /* the range-end gap below was peeked         */
    bool      exhausted;    /* ... and committed                          */
    uint16_t  origin;
    uint64_t  id;
    uint64_t  demand_id;
    uint64_t  start_group, start_object;
    uint64_t  next_group, next_object;
    uint64_t  end_group, end_object;   /* inclusive claimed end           */
    uint64_t  deadline_us;
    uint8_t   order;
    uint8_t   prio;
    uint32_t  granted;      /* credit the owner holds                     */
    uint32_t  head, count;  /* the window                                 */
    r_fitem_t win[R_FILL_WINDOW];
    r_fill_result_t terminal;   /* END or allocation-free local ERROR     */
    uint32_t  release_entries;   /* served window items whose channel   */
    uint64_t  release_bytes;     /* charge is still to be released      */
    /* A standalone FETCH asked of the owner shard: its owned key until the
     * REQ carries it, the request's raw range form, and the owner's
     * admission once OPEN arrives (begun). */
    bool      standalone;
    bool      cluster_selected;
    moqr_cluster_advertisement_id_t  source_advertisement;
    uint64_t  requester_hop_id, requester_correlation_id;
    uint8_t  *canon;
    uint32_t  canon_len;
    uint32_t  range_form;
    uint64_t  relative_start;
    uint64_t  join_demand_id;   /* a Joining FETCH's feeding demand, or 0 */
    moqr_retrieve_standalone_plan_t plan;
    moq_rcbuf_t *meta_props;    /* owns plan.meta_bytes while open        */
} r_fret_t;


typedef struct r_oret {
    bool         used;
    bool         open_sent;
    bool         failed;       /* durable child-local admission/copy error */
    moqr_result_t failure;
    uint16_t     src;
    uint64_t     id;            /* the requester's retrieval id           */
    uint64_t     demand_id;
    uint64_t     start_group, start_object;
    uint64_t     end_group, end_object;
    uint64_t     deadline_us;
    uint8_t      order;
    uint8_t      prio;
    uint32_t     credit;
    /* A standalone FETCH another shard asked for: its admission here (owner
     * holds this core's retrieval slot while the answer runs). */
    bool         standalone;
    moqr_fetch_t owner;
    moqr_core_retrieve_plan_t plan;
    /* Its Track Properties, from this binding's reply pin at admission, as
     * an owned clone (never the pin itself) until OPEN carries them. */
    moqr_bind_meta_state_t meta_state;
    moq_version_t meta_version;
    moq_rcbuf_t *meta_props;
    bool         capacity;         /* refused for room (the owner's pool) */
    moqr_retrieve_cursor_t cursor; /* fixed row: never copied while live */
} r_oret_t;


struct r_mgr {
    moqr_shards_t  *s;      /* back-ref for peer count + mailboxes */
    moq_alloc_t     alloc;
    uint16_t        shard;
    moqr_core_t    *core;
    /* Journal entries whose mirror install the core refused for room, and
     * the core room epoch at the start of the last reconciliation pass. */
    uint32_t        cap_blocked_count;
    uint64_t        cap_epoch;
    /* journal: sorted-by-canon array of candidate-set entries (self-echo
     * suppression lives per-entry — see r_jentry echo counters). */
    r_jentry       *jrn;
    uint32_t        jrn_cap;
    uint32_t        jrn_len;
    /* pseudo-bindings: pb[j] mirrors origin j; pb[shard] unused. */
    moqr_binding_t *pb;
    moqr_binding_t  mgr_bind;   /* wildcard namespace watcher */
    r_cluster_snapshot *cluster; /* indexed by originating core route slot */
    /* remote-owner demand recorded by the router, forwarded by the demand
     * phase, and resolved by the inbound drain (refused by default; admitted
     * with data pumped back when admission is enabled). */
    r_pdemand      *pend;
    uint32_t        pend_cap;
    uint32_t        pend_len;
    uint64_t        next_demand_id;         /* monotonic correlation counter */
    uint64_t        remote_demand_refused;  /* counted when the REQUESTER
                                             * resolves the owner's answer  */
    /* owner-side admitted demands (used-flag slots; populated only with
     * admission enabled). Bounded like pend: one admitted demand per slot. */
    r_psub         *psub;
    uint32_t        psub_cap;
    /* requester-side resolution stats: every owner DONE this shard resolved
     * into its core (refusal or post-accept terminal), with the last code
     * verbatim — the deterministic tests' code-propagation observable. */
    uint64_t        remote_demand_resolved;
    uint64_t        last_done_code;
    moqr_pd_desc_t  last_done_pd;   /* the tagged terminal, as resolved */
    bool            last_done_pre_ack;
    /* data-pump stats + state. data_pending_mask: bit d set while this shard
     * (as OWNER) has extraction work toward d it could not enqueue — the
     * signal control consults before taking a contended slot; recomputed by
     * every data phase, so it is at most one step stale. data_rr rotates the
     * extraction start across sources for fairness. */
    uint64_t        remote_data_rejected;
    uint64_t        remote_demand_term_capacity;
    uint64_t        remote_demand_term_overrun;
    /* subgroup-progress rows: the owner row for psub i is psg[i*sg_slots];
     * requester rows are claimed lazily (pend entries swap-move, so each
     * entry OWNS a row index) from popen + the popen_row_used map. */
    r_psg_t        *psg;
    r_psg_t        *popen;
    uint8_t        *popen_row_used;
    uint64_t        pump_turns;
    uint64_t        pump_messages;
    uint64_t        pump_bytes;
    uint64_t        data_pending_mask;
    uint16_t        data_rr;
    /* Successful durable demand-channel enqueues by this shard as the
     * PRODUCER, indexed by moqr_shards_msg_kind_t (kind - 1 from the wire
     * D_MSG_* value). Bumped only after dch_push accepted the message —
     * a refused attempt (full ring, byte gate, arbiter) never counts. */
    uint64_t        enq[MOQR_SHARDS_MSG__COUNT];
    /* Wake requests raised by this shard's step_shard calls, counted at
     * the MASK level (popcount of the pre-merge push mask and the credit
     * mask, once per step per cause). */
    uint64_t        wake_req_push;
    uint64_t        wake_req_credit;
    uint64_t        wake_req_local;
    /* Per-step scratch (cleared by the accounted step before the phases
     * run): an inbound DATA message applied into the LOCAL core this step —
     * whole/open/chunk/end ingest, a seal, or a reset/evict abandon — i.e.
     * an application that can mark local delivery readiness. One bind pump
     * pass may not exhaust everything a batch of applications made
     * deliverable (the pass can end with the bind's own ready latch set and
     * no later pump owed — the lost-continuation stall), so the step
     * requests exactly ONE coalesced local continuation; a step that
     * applies nothing (moot pops and rejected data included) requests none,
     * so the empty follow-up pump can never rearm itself into a spin. */
    bool            inbound_data_applied;
    /* Set when this step's reconcile left core intents owed a drain. The
     * reconcile's core calls fan NS_FOUND/NS_GONE to every matching local
     * watcher — including real downstream namespace subscribers — but they
     * run in phase 3, after the step's only bind pump, so what they queue has
     * no drain of its own. The step therefore requests exactly ONE coalesced
     * local continuation, which is the difference between advertising a
     * namespace that arrives after the subscription and stranding it until
     * some unrelated event happens to step this shard again.
     *
     * Exactly three cases set it, and a completed mutation is not the test —
     * owed work is:
     *   - a successful mirror install (moqr_core_announce);
     *   - a successful mirror teardown (moqr_core_unannounce);
     *   - a loser withdrawal (moqr_core_force_withdraw) that returned OK OR
     *     WOULD_BLOCK — the resumable case has already purged part of the
     *     loser and queued that much fan-out, so it owes the same drain.
     * A permanent error mutates nothing and queues nothing; it fail-stops the
     * step and must not set this. Cleared per step. */
    bool            reconcile_owed_local_drain;
    /* A cluster import refused intent credit before this step's pump.
     * Retry once after that pump has had its opportunity to return credit;
     * unchanged route CAPACITY does not arm this retry or a self-wake. */
    bool            cluster_inbound_retry;
    /* Turn-outcome classification: exactly one of the four turns_*
     * counters increments per data-phase turn, decided at the single
     * dp_turn_classify point with pre-registered precedence (message
     * budget > byte budget > blocked > drained) so a tie is never
     * data-dependent. turns_with_messages counts turns that pushed at
     * least one data message, independent of the outcome class.
     * arb_refusals counts sticky-arbiter class-gate refusals. All are
     * owning-stepper-only state, like every counter above. */
    uint64_t        turns_msg_budget;
    uint64_t        turns_byte_budget;
    uint64_t        turns_blocked;
    uint64_t        turns_drained;
    uint64_t        turns_with_messages;
    uint64_t        arb_refusals;
    /* Journal projection generation: bumped on every actual change to the
     * printed journal projection (candidate bit set/clear, mirror install/
     * teardown, holdout transition, entry reclaim); never on a retry,
     * no-op, or suppressed self-echo. */
    uint64_t        journal_epoch;
    /* stats / trace hooks. */
    uint64_t        holdouts;   /* current local-loser holdouts (gauge) */
    /* Fill retrieval (admission only): requester claims and owner answers,
     * fill_cap slots each. fill_starved: a claim was refused for want of a
     * slot since the last one freed; fill_wake_owed: a slot freed after
     * such a refusal, so this step owes the waiting fill one local
     * continuation (it retries on the shard's next pump). */
    r_fret_t       *fret;
    r_oret_t       *oret;
    uint32_t        fill_cap;
    uint64_t        next_fill_id;
    bool            fill_starved;
    bool            fill_wake_owed;
    bool            oom; /* dropped an unrecoverable borrowed observation
                          * (canon OOM / journal-full / pending-demand-full)
                          * — fail-stops the step */
    moqr_result_t cluster_error; /* sticky policy/invariant failure, never NOMEM */
#ifdef MOQR_BIND_TESTING
    /* Verify-only SG_SEAL ingest evidence: a bounded ring of the seals this
     * REQUESTER shard applied to its core, in ingest order (seq is the
     * lifetime count; the ring keeps the newest window). Written only by
     * the owning shard's stepper; snapshot from the owning lane. */
    struct {
        uint64_t seq;
        uint16_t src;
        uint64_t demand_id;    /* exact demand attribution: a background
                                * seal for the same (group, subgroup) from
                                * another track/demand cannot collide */
        uint64_t group_id;
        uint64_t subgroup_id;
    }               seal_log[32];
    uint64_t        seal_seq;
#endif
};


/* Per-turn data budgets. dp_admit answers whether one message of lb logical
 * bytes may push now; dp_spend records a successful push, noting when the
 * soft-first byte exception was spent (the caller must END the turn — even a
 * zero-byte follower may not ride behind it; no debt carries forward). */
typedef struct dp_budget {
    uint32_t msgs_left;
    uint64_t bytes_left;
    bool     pushed_any;
    bool     turn_over;
} dp_budget_t;

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

uint64_t
cap_add(uint64_t a, uint64_t b);

void *
sh_alloc(const moq_alloc_t *a, size_t n);

void
sh_free(const moq_alloc_t *a, void *p, size_t n);

void
shards_round_advance(moqr_shards_t *s);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#include "data_internal.h"

#include "demand_internal.h"

#include "fill_internal.h"

#include "journal_internal.h"

#include "mailbox_internal.h"

#include "placement_internal.h"

#endif /* MOQR_SHARDS_INTERNAL_H */
