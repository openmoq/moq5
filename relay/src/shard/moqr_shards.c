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

#define R_SHARDS_DEF_TRACE   512u
#define R_SHARDS_DEF_MAILBOX 256u
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

/* Checked capacity arithmetic: overflow poisons to UINT64_MAX, and the
 * describe rejects a poisoned total with INVAL — never a silent wrap. */
static uint64_t
cap_add(uint64_t a, uint64_t b)
{
    return (UINT64_MAX - a < b) ? UINT64_MAX : a + b;
}

static uint64_t
cap_mul(uint64_t a, uint64_t b)
{
    if (a == 0 || b == 0) {
        return 0;
    }
    return (a > UINT64_MAX / b) ? UINT64_MAX : a * b;
}
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
/* PUBLISH_DONE status registry (distinct from REQUEST_ERROR): the track is
 * no longer being published by the departing owner. */
#define R_DONE_TRACK_ENDED   0x2u
/* Data-stream reset registry (distinct again): the publisher ended the
 * subscription; PUBLISH_DONE carries the detailed status. */
#define R_RESET_CANCELLED    0x1u
/* Wire REQUEST_ERROR code for an owner-side admission failure that has no
 * protocol meaning of its own (pump-sub table full, subscribe hard error),
 * mirroring MOQ_REQUEST_ERROR_INTERNAL (moq/session.h). */
#define R_ERR_INTERNAL       0x0u
/* Wire REQUEST_ERROR code for a Joining FETCH whose joined subscription's
 * track is no longer the one its name resolves to, mirroring
 * MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID (moq/session.h). */
#define R_ERR_INVALID_JOINING 0x32u

#define SHARDS_CFG_HAS(cfg, field)                       \
    (offsetof(moqr_shards_cfg_t, field) +                \
         sizeof(((moqr_shards_cfg_t *)0)->field) <= (cfg)->struct_size)

/* -- small allocator helpers ------------------------------------------------ */

static void *
sh_alloc(const moq_alloc_t *a, size_t n)
{
    return a->alloc(n, a->ctx);
}

static void
sh_free(const moq_alloc_t *a, void *p, size_t n)
{
    if (p != NULL) {
        a->free(p, n, a->ctx);
    }
}

/* -- placement: rendezvous (highest-random-weight) hash --------------------- */

static uint64_t
r_splitmix64(uint64_t x)
{
    x += 0x9E3779B97F4A7C15ull;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
    return x ^ (x >> 31);
}

/* HRW score of a key hash for a candidate shard. Pure. */
static uint64_t
r_hrw_score(uint64_t hash, uint32_t shard)
{
    return r_splitmix64(hash ^ ((uint64_t)shard * 0x9E3779B97F4A7C15ull));
}

/* The default policy: score each shard and take the maximum (ties to the lowest
 * index). Pure in (key, state) — no clocks, counters, or globals — so the same
 * key and state always pick the same owner, which is what lets placement coexist
 * with seed-replay determinism. */
static moqr_owner_t
r_rendezvous_place(const moqr_place_key_t *key,
                   const moqr_placement_state_t *state)
{
    moqr_owner_t owner = { 0, 0 };
    uint32_t k = state->shard_count == 0 ? 1u : (uint32_t)state->shard_count;
    uint64_t best_score = 0;
    for (uint32_t i = 0; i < k; i++) {
        uint64_t score = r_hrw_score(key->hash, i);
        if (i == 0 || score > best_score) {
            best_score = score;
            owner.shard = i;
        }
    }
    return owner;
}

/* HRW winner over a live candidate set (bit i = shard i). -1 if empty. */
static int32_t
r_winner_of(uint64_t candidates, uint64_t hash)
{
    int32_t best = -1;
    uint64_t best_score = 0;
    for (uint32_t i = 0; i < 64u; i++) {
        if (((candidates >> i) & 1u) == 0) {
            continue;
        }
        uint64_t score = r_hrw_score(hash, i);
        if (best < 0 || score > best_score) {
            best_score = score;
            best = (int32_t)i;
        }
    }
    return best;
}

/* -- canonical namespace key ------------------------------------------------ */
/* Layout: [u32 count][u32 len_0..len_{c-1}][part bytes...], little-endian. The
 * journal keys and mailbox messages both use this single encoding, and it
 * reconstructs a moqr_ns_t for the mirror announce. */

static uint32_t
rd32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
           ((uint32_t)p[3] << 24);
}

static void
wr32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
}

static uint8_t *
canon_build(const moq_bytes_t *parts, uint32_t count, uint32_t *out_len,
            const moq_alloc_t *a)
{
    if (count > R_NS_MAX_PARTS) {
        return NULL;
    }
    uint32_t len = 4u + 4u * count;
    for (uint32_t i = 0; i < count; i++) {
        len += (uint32_t)parts[i].len;
    }
    uint8_t *buf = sh_alloc(a, len);
    if (buf == NULL) {
        return NULL;
    }
    wr32(buf, count);
    uint32_t off = 4u + 4u * count;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t l = (uint32_t)parts[i].len;
        wr32(buf + 4u + 4u * i, l);
        if (l != 0) {
            memcpy(buf + off, parts[i].data, l);
        }
        off += l;
    }
    *out_len = len;
    return buf;
}

static uint8_t *
canon_dup(const uint8_t *canon, uint32_t len, const moq_alloc_t *a)
{
    uint8_t *b = sh_alloc(a, len);
    if (b != NULL) {
        memcpy(b, canon, len);
    }
    return b;
}

static uint64_t
canon_hash(const uint8_t *canon, uint32_t len)
{
    uint64_t h = 0xCBF29CE484222325ull;
    for (uint32_t i = 0; i < len; i++) {
        h ^= canon[i];
        h *= 0x100000001B3ull;
    }
    return h;
}

/* Total order over canonical keys (lexicographic bytes, shorter first on a
 * shared prefix). The dirty-reconcile and outbound phases walk in this order. */
static int
canon_cmp(const uint8_t *a, uint32_t alen, const uint8_t *b, uint32_t blen)
{
    uint32_t n = alen < blen ? alen : blen;
    int c = memcmp(a, b, n);
    if (c != 0) {
        return c;
    }
    return alen < blen ? -1 : (alen > blen ? 1 : 0);
}

static moqr_ns_t
canon_to_ns(const uint8_t *canon, moq_bytes_t *parts)
{
    uint32_t count = rd32(canon);
    uint32_t off = 4u + 4u * count;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t l = rd32(canon + 4u + 4u * i);
        parts[i].data = (l != 0) ? canon + off : NULL;
        parts[i].len = l;
        off += l;
    }
    moqr_ns_t ns = { parts, count };
    return ns;
}

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
    uint64_t     source_binding_cookie, source_route_generation;
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
    uint64_t     source_binding_cookie, source_route_generation;
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

/* Whether a message kind is DATA (byte-gated, counted against the data
 * pump) rather than control. */
static bool
dmsg_is_data(uint8_t kind)
{
    return (kind >= D_MSG_OBJ && kind <= D_MSG_SG_SEAL) ||
           (kind >= D_MSG_FILL_OPEN && kind <= D_MSG_FILL_END);
}

/* Logical bytes a queued message contributes to its channel's byte gauge:
 * payload + properties + the control canonical key. */
static uint64_t
dmsg_logical_bytes(const r_demand_msg_t *m)
{
    return (uint64_t)moq_rcbuf_len(m->payload) +
           (uint64_t)moq_rcbuf_len(m->props) + m->canon_len;
}

/* Release everything a queued message still owns (clone refs + canon). */
static void
dmsg_release(const moq_alloc_t *a, r_demand_msg_t *m)
{
    if (m->canon != NULL) {
        a->free(m->canon, m->canon_len, a->ctx);
        m->canon = NULL;
    }
    moq_rcbuf_decref(m->payload);
    m->payload = NULL;
    moq_rcbuf_decref(m->props);
    m->props = NULL;
}

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

static r_psg_t *
psg_find(r_psg_t *row, uint32_t slots, uint64_t g, uint64_t sg)
{
    for (uint32_t i = 0; i < slots; i++) {
        if (row[i].used && row[i].group == g && row[i].subgroup == sg) {
            return &row[i];
        }
    }
    return NULL;
}

/* Find-or-claim the (g, sg) slot in a demand's progress row; NULL when the
 * row is exhausted (a demand terminal, never an untracked stream). */
static r_psg_t *
psg_claim(r_psg_t *row, uint32_t slots, uint64_t g, uint64_t sg)
{
    r_psg_t *e = psg_find(row, slots, g, sg);
    if (e != NULL) {
        return e;
    }
    for (uint32_t i = 0; i < slots; i++) {
        if (!row[i].used) {
            memset(&row[i], 0, sizeof(row[i]));
            row[i].used = true;
            row[i].group = g;
            row[i].subgroup = sg;
            return &row[i];
        }
    }
    return NULL;
}

static void
psg_row_clear(r_psg_t *row, uint32_t slots)
{
    memset(row, 0, (size_t)slots * sizeof(row[0]));
}

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
    uint64_t  source_binding_cookie, source_route_generation;
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
    bool            oom;        /* dropped an unrecoverable borrowed observation
                                 * (canon OOM / journal-full / pending-demand-full)
                                 * — fail-stops the step */
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

/* Find the sorted-array index of `canon` (binary search). Returns the entry
 * when present; else sets *ins to the insertion index and returns NULL. */
static r_jentry *
jrn_find(r_mgr *m, const uint8_t *canon, uint32_t len, uint32_t *ins)
{
    uint32_t lo = 0, hi = m->jrn_len;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2u;
        int c = canon_cmp(m->jrn[mid].canon, m->jrn[mid].canon_len, canon, len);
        if (c == 0) {
            if (ins != NULL) {
                *ins = mid;
            }
            return &m->jrn[mid];
        }
        if (c < 0) {
            lo = mid + 1u;
        } else {
            hi = mid;
        }
    }
    if (ins != NULL) {
        *ins = lo;
    }
    return NULL;
}

/* Get-or-insert an entry for `canon` (ADOPTS canon on insert; the caller must
 * not free it after). Returns NULL on capacity/OOM (caller frees canon). */
static r_jentry *
jrn_get(r_mgr *m, uint8_t *canon, uint32_t len)
{
    uint32_t ins = 0;
    r_jentry *e = jrn_find(m, canon, len, &ins);
    if (e != NULL) {
        return e;   /* existing; caller frees the dup canon */
    }
    if (m->jrn_len >= m->jrn_cap) {
        return NULL;
    }
    memmove(&m->jrn[ins + 1u], &m->jrn[ins],
            (size_t)(m->jrn_len - ins) * sizeof(m->jrn[0]));
    e = &m->jrn[ins];
    memset(e, 0, sizeof(*e));
    e->used = true;
    e->canon = canon;
    e->canon_len = len;
    e->hash = canon_hash(canon, len);
    e->mirror = R_MIRROR_NONE;
    m->jrn_len++;
    return e;
}

/* Remove a fully-quiesced entry (index i) and free its key. The entry
 * leaves the printed journal projection, so the projection generation
 * advances. */
static void
jrn_remove(r_mgr *m, uint32_t i)
{
    sh_free(&m->alloc, m->jrn[i].canon, m->jrn[i].canon_len);
    memmove(&m->jrn[i], &m->jrn[i + 1u],
            (size_t)(m->jrn_len - i - 1u) * sizeof(m->jrn[0]));
    m->jrn_len--;
    m->journal_epoch++;
}

/* Record an origin's candidate bit as present/absent, marking the entry
 * dirty for reconcile. The projection generation advances ONLY when the bit
 * actually changes — a redelivered/no-op observation is not a projection
 * change. */
static void
jrn_set_candidate(r_mgr *m, r_jentry *e, uint64_t bit, bool present)
{
    uint64_t next = present ? (e->candidates | bit) : (e->candidates & ~bit);
    if (next != e->candidates) {
        e->candidates = next;
        m->journal_epoch++;
    }
    e->dirty = true;
}

/* -- control mailboxes ------------------------------------------------------ */

/* Publish a namespace's latest net state to a directed mailbox. Coalesces onto an
 * existing slot for the same namespace — announce+withdraw collapse in place, so a
 * pending message is never duplicated and can never head-of-line another. Returns
 * false only when a NEW namespace finds every slot occupied (backpressure: the
 * sender leaves its sent bit clear and re-exports next round). */
static bool
mbox_push(moqr_shards_t *s, uint16_t src, uint16_t dst, uint8_t op,
          uint16_t origin, const uint8_t *canon, uint32_t len)
{
    r_mailbox *mb = &s->mbox[(uint32_t)src * s->shard_count + dst];
    bool pushed = false;
    pthread_mutex_lock(&mb->mu);
    int32_t free_slot = -1;
    for (uint32_t i = 0; i < mb->cap; i++) {
        if (!mb->slots[i].used) {
            if (free_slot < 0) {
                free_slot = (int32_t)i;
            }
            continue;
        }
        if (canon_cmp(mb->slots[i].canon, mb->slots[i].canon_len, canon, len) == 0) {
            mb->slots[i].op = op;          /* coalesce: latest state wins */
            mb->slots[i].round = s->round;
            pushed = true;
            goto out;
        }
    }
    if (free_slot < 0) {
        goto out;                          /* full of other namespaces */
    }
    {
        uint8_t *dup = canon_dup(canon, len, &s->alloc);
        if (dup == NULL) {
            goto out;
        }
        r_ctrl_msg *slot = &mb->slots[free_slot];
        slot->used = true;
        slot->round = s->round;
        slot->op = op;
        slot->origin = origin;
        slot->canon = dup;
        slot->canon_len = len;
        mb->count++;
        pushed = true;
    }
out:
    pthread_mutex_unlock(&mb->mu);
    return pushed;
}

/* Publish a whole original-source snapshot under one leaf lock. A reused
 * source slot replaces its predecessor, including while the consumer lags. */
static bool
mbox_push_cluster(moqr_shards_t *s, uint16_t src, uint16_t dst,
                  uint32_t index, const r_cluster_snapshot *snapshot,
                  bool *allocation_failed)
{
    r_mailbox *mb = &s->mbox[(uint32_t)src * s->shard_count + dst];
    uint8_t *canon = NULL;
    uint64_t *hops = NULL;
    size_t hop_bytes = snapshot->present
        ? snapshot->record.route.metadata.hop_count * sizeof(uint64_t) : 0;
    if (snapshot->present) {
        canon = canon_dup(snapshot->canon, snapshot->canon_len, &s->alloc);
        if (canon == NULL) { *allocation_failed = true; return false; }
        hops = sh_alloc(&s->alloc, hop_bytes);
        if (hops == NULL) {
            sh_free(&s->alloc, canon, snapshot->canon_len);
            *allocation_failed = true;
            return false;
        }
        memcpy(hops, snapshot->hops, hop_bytes);
    }
    bool pushed = false;
    pthread_mutex_lock(&mb->mu);
    r_ctrl_msg *slot = NULL;
    for (uint32_t i = 0; i < mb->cap; ++i) {
        if (mb->slots[i].used && mb->slots[i].cluster &&
            mb->slots[i].source_index == index) {
            slot = &mb->slots[i];
            break;
        }
        if (!mb->slots[i].used && slot == NULL) slot = &mb->slots[i];
    }
    if (slot != NULL) {
        if (slot->used) {
            sh_free(&s->alloc, slot->canon, slot->canon_len);
            sh_free(&s->alloc, slot->hops,
                    slot->record.route.metadata.hop_count * sizeof(uint64_t));
        } else {
            mb->count++;
        }
        memset(slot, 0, sizeof(*slot));
        slot->used = true;
        slot->cluster = true;
        slot->round = s->round;
        slot->origin = src;
        slot->source_index = index;
        slot->op = snapshot->present ? R_OP_ANNOUNCE : R_OP_UNANNOUNCE;
        slot->canon = canon;
        slot->canon_len = snapshot->present ? snapshot->canon_len : 0;
        slot->hops = hops;
        if (snapshot->present) slot->record = snapshot->record;
        slot->record.source_index = index;
        slot->record.route.metadata.hop_ids = hops;
        pushed = true;
    }
    pthread_mutex_unlock(&mb->mu);
    if (!pushed) {
        sh_free(&s->alloc, canon, snapshot->canon_len);
        sh_free(&s->alloc, hops, hop_bytes);
    }
    return pushed;
}

/* Capture after all local mutations. Comparisons include source identity and
 * charged/warm cost, so an alternative changing below the best path is still
 * replicated. Imports are excluded by the core exporter. */
static void
shard_cluster_outbound(moqr_shards_t *s, uint16_t shard, uint64_t *wake)
{
    r_mgr *m = s->shards[shard].mgr;
    for (uint32_t i = 0; i < s->cluster_routes; ++i) {
        moqr_cluster_export_t record;
        memset(&record, 0, sizeof(record));
        moqr_result_t rc = moqr_core_cluster_export(m->core, i, &record);
        if (rc != MOQR_OK && rc != MOQR_DONE) {
            m->oom = true;
            return;
        }
        bool present = rc == MOQR_OK;
        r_cluster_snapshot *snapshot = &m->cluster[i];
        if (present) {
            uint32_t canon_len = 0;
            uint8_t *canon = canon_build(record.ns.parts, record.ns.count,
                                        &canon_len, &m->alloc);
            if (canon == NULL) { m->oom = true; return; }
            const moqr_cluster_route_t *a = &snapshot->record.route;
            const moqr_cluster_route_t *b = &record.route;
            size_t hop_bytes = b->metadata.hop_count * sizeof(uint64_t);
            bool same = snapshot->present && snapshot->canon_len == canon_len &&
                memcmp(snapshot->canon, canon, canon_len) == 0 &&
                a->source_binding_cookie == b->source_binding_cookie &&
                a->source_session_cookie == b->source_session_cookie &&
                a->route_generation == b->route_generation &&
                snapshot->record.revision == record.revision &&
                a->accumulated_cost == b->accumulated_cost &&
                a->warm == b->warm &&
                a->metadata.has_route_cost == b->metadata.has_route_cost &&
                a->metadata.route_cost == b->metadata.route_cost &&
                a->metadata.hop_count == b->metadata.hop_count &&
                snapshot->record.source_peer_hop_id == record.source_peer_hop_id &&
                snapshot->record.source_correlation_id == record.source_correlation_id &&
                memcmp(snapshot->hops, b->metadata.hop_ids, hop_bytes) == 0;
            if (same) {
                sh_free(&m->alloc, canon, canon_len);
            } else {
                uint64_t *hops = sh_alloc(&m->alloc, hop_bytes);
                if (hops == NULL) {
                    sh_free(&m->alloc, canon, canon_len);
                    m->oom = true;
                    return;
                }
                memcpy(hops, b->metadata.hop_ids, hop_bytes);
                sh_free(&m->alloc, snapshot->canon, snapshot->canon_len);
                sh_free(&m->alloc, snapshot->hops,
                        snapshot->record.route.metadata.hop_count * sizeof(uint64_t));
                snapshot->canon = canon;
                snapshot->canon_len = canon_len;
                snapshot->hops = hops;
                snapshot->record = record;
                snapshot->record.ns.parts = NULL;
                snapshot->record.ns.count = 0;
                memset(snapshot->record.ns_parts, 0,
                       sizeof(snapshot->record.ns_parts));
                snapshot->record.route.metadata.hop_ids = hops;
                snapshot->present = true;
                snapshot->sent = 0;
            }
        } else if (snapshot->present) {
            snapshot->present = false;
            snapshot->sent = 0;
        }
        for (uint16_t d = 0; d < s->shard_count; ++d) {
            if (d == shard || ((snapshot->sent >> d) & 1u) != 0) continue;
            bool allocation_failed = false;
            if (mbox_push_cluster(s, shard, d, i, snapshot, &allocation_failed)) {
                snapshot->sent |= 1ull << d;
                if (wake != NULL) *wake |= 1ull << d;
            } else if (allocation_failed) {
                /* No queued message can return credit for an allocation
                 * refusal. Surface it instead of silently stranding state. */
                m->oom = true;
                return;
            }
        }
    }
}

/* -- the demand channel (FIFO; sibling to — never part of — the mailbox) ----- */

/* Canonical ns+name key: the mailbox canon layout with a trailing
 * [u32 name_len][name bytes] segment, so one owned buffer carries the full
 * track identity a DEMAND needs. */
static uint8_t *
canon2_build(const moq_bytes_t *parts, uint32_t count, moq_bytes_t name,
             uint32_t *out_len, const moq_alloc_t *a)
{
    if (count > R_NS_MAX_PARTS) {
        return NULL;
    }
    uint32_t len = 4u + 4u * count;
    for (uint32_t i = 0; i < count; i++) {
        len += (uint32_t)parts[i].len;
    }
    uint32_t ns_len = len;
    len += 4u + (uint32_t)name.len;
    uint8_t *buf = sh_alloc(a, len);
    if (buf == NULL) {
        return NULL;
    }
    wr32(buf, count);
    uint32_t off = 4u + 4u * count;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t l = (uint32_t)parts[i].len;
        wr32(buf + 4u + 4u * i, l);
        if (l != 0) {
            memcpy(buf + off, parts[i].data, l);
        }
        off += l;
    }
    wr32(buf + ns_len, (uint32_t)name.len);
    if (name.len != 0) {
        memcpy(buf + ns_len + 4u, name.data, name.len);
    }
    *out_len = len;
    return buf;
}

/* Length of the ns prefix inside a canon2 key — the exact bytes canon_build
 * would produce for the namespace alone, so the journal (keyed on that
 * layout) can be searched with a canon2 key's prefix. */
static uint32_t
canon2_ns_len(const uint8_t *canon)
{
    uint32_t count = rd32(canon);
    uint32_t len = 4u + 4u * count;
    for (uint32_t i = 0; i < count; i++) {
        len += rd32(canon + 4u + 4u * i);
    }
    return len;
}

/* Decode a canon2 key back into a borrowed {ns, name} view (valid while the
 * key lives). */
static moqr_ns_t
canon2_decode(const uint8_t *canon, moq_bytes_t *parts, moq_bytes_t *name)
{
    moqr_ns_t ns = canon_to_ns(canon, parts);
    uint32_t off = canon2_ns_len(canon);
    uint32_t nlen = rd32(canon + off);
    name->data = (nlen != 0) ? canon + off + 4u : NULL;
    name->len = nlen;
    return ns;
}

/* Append a message to a directed channel. The message's owned memory (canon
 * + payload/props clones) TRANSFERS to the channel on success (the consumer
 * releases it at pop); on a refused push the caller keeps ownership and
 * retries later. Every message's logical bytes count into the gauge; DATA
 * kinds are additionally GATED on the byte cap. Control keys and bounded
 * Track Properties are separately covered by the allocation ceiling and
 * must not starve behind that data cap. */
static bool
dch_push(moqr_shards_t *s, uint16_t src, uint16_t dst, const r_demand_msg_t *m)
{
    r_demand_channel_t *ch = &s->dch[(uint32_t)src * s->shard_count + dst];
    uint64_t lb = dmsg_logical_bytes(m);
    bool is_data = dmsg_is_data(m->kind);
    bool pushed = false;
    pthread_mutex_lock(&ch->mu);
    if (ch->count + ch->held < ch->cap &&
        (!is_data || ch->bytes + ch->held_bytes + lb <= s->dch_byte_cap)) {
        r_demand_msg_t *slot =
            &ch->slots[(ch->head + ch->count) % ch->cap];
        *slot = *m;
        slot->round = s->round;
        slot->bytes_accounted = lb;
        ch->count++;
        ch->bytes += lb;
        /* High-water marks: this channel's own maxima, maintained inside
         * the same critical section that made the push durable. */
        if (ch->count + ch->held > ch->count_hwm) {
            ch->count_hwm = ch->count + ch->held;
        }
        if (ch->bytes + ch->held_bytes > ch->bytes_hwm) {
            ch->bytes_hwm = ch->bytes + ch->held_bytes;
        }
        /* pair accounting: data uses the SAME logical-byte currency the
         * producer spends against pump_bytes, so the cross-pair sums close
         * exactly against the per-shard aggregates */
        if (is_data) {
            ch->pair_data_msgs++;
            ch->pair_data_bytes += lb;
        } else if (m->kind <= D_MSG_SG_SEAL) {
            ch->pair_ctrl_msgs++;
        }
        pushed = true;
    } else if (ch->count + ch->held >= ch->cap) {
        /* refusal split mirrors the admission predicate's own evaluation
         * order: entries first, one increment per refusal event */
        ch->refused_entries++;
    } else {
        ch->refused_bytes++;
    }
    pthread_mutex_unlock(&ch->mu);
    return pushed;
}

/* Class-arbitrated push. The sticky token is PRODUCER-owned (only the src
 * shard touches its row) and transfers ONLY on a successful preferred-class
 * enqueue: a refused/full push never moves it, and the non-preferred class
 * may enqueue only when the preferred class has no eligible work — in which
 * case the token still does not move. Eligibility is computed by the caller
 * OUTSIDE the channel lock. */
static bool
dch_push_arb(moqr_shards_t *s, uint16_t src, uint16_t dst,
             const r_demand_msg_t *m, bool is_data, bool other_eligible)
{
    uint8_t *tok = &s->arb[(uint32_t)src * s->shard_count + dst];
    uint8_t cls = is_data ? R_ARB_DATA : R_ARB_CTRL;
    if (*tok != cls && other_eligible) {
        r_mgr *pm = s->shards[src].mgr;
        if (pm != NULL) {
            pm->arb_refusals++;   /* producer-owned, like enq[] below */
        }
        return false;   /* the preferred class holds the claim on the slot */
    }
    if (!dch_push(s, src, dst, m)) {
        return false;   /* full: the token does not move on failure */
    }
    /* The push is durable: count it for the PRODUCER, by exact kind. Every
     * demand-channel enqueue funnels through here, and only success counts
     * — a refused attempt above never reaches this line. */
    {
        r_mgr *pm = s->shards[src].mgr;
        if (pm != NULL && m->kind >= D_MSG_DEMAND && m->kind <= D_MSG_SG_SEAL) {
            pm->enq[m->kind - 1u]++;
        }
    }

    if (*tok == cls) {
        *tok = (cls == R_ARB_DATA) ? R_ARB_CTRL : R_ARB_DATA;
    }
    return true;
}

/* The channel head, or NULL (empty, or the head is not yet visible under the
 * deterministic round barrier). The pointer stays valid until dch_pop —
 * there is exactly one consumer per channel and a published slot is never
 * mutated by the producer. */
static r_demand_msg_t *
dch_head(moqr_shards_t *s, uint16_t src, uint16_t dst)
{
    r_demand_channel_t *ch = &s->dch[(uint32_t)src * s->shard_count + dst];
    r_demand_msg_t *m = NULL;
    pthread_mutex_lock(&ch->mu);
    if (ch->count > 0) {
        r_demand_msg_t *head = &ch->slots[ch->head];
        if (s->live_visibility || head->round < s->round) {
            m = head;
        }
    }
    pthread_mutex_unlock(&ch->mu);
    return m;
}

/* Consume the head (call only after its outcome is durable). Releases
 * whatever the message still owns — a consumer that took ownership of a
 * clone (ingest transfer) must null the ref first. */
static void
dch_pop(moqr_shards_t *s, uint16_t src, uint16_t dst)
{
    r_demand_channel_t *ch = &s->dch[(uint32_t)src * s->shard_count + dst];
    pthread_mutex_lock(&ch->mu);
    if (ch->count > 0) {
        /* Credit the producer: this durable pop frees one src->dst slot, so
         * src may have work it was holding on channel-full. Only dst's own
         * stepper runs this (single writer on the accumulator); a held head
         * never pops and therefore never wakes anyone. */
        s->shards[dst].pop_wake |= 1ull << src;
        r_demand_msg_t *head = &ch->slots[ch->head];
        /* The gauge tracked the bytes as PUSHED; an ownership transfer since
         * then (nulled refs) does not change what the channel accounted. */
        ch->bytes -= head->bytes_accounted;
        dmsg_release(&s->alloc, head);
        ch->head = (ch->head + 1u) % ch->cap;
        ch->count--;
    }
    pthread_mutex_unlock(&ch->mu);
}

/* Take the head out of the ring while keeping its entry and bytes charged
 * to the channel (a fill-retrieval answer item moving into its window). The
 * caller has already taken every reference the head owned. No producer
 * credit: nothing was freed toward src yet. */
static uint64_t
dch_hold_head(moqr_shards_t *s, uint16_t src, uint16_t dst)
{
    r_demand_channel_t *ch = &s->dch[(uint32_t)src * s->shard_count + dst];
    uint64_t charged = 0;
    pthread_mutex_lock(&ch->mu);
    if (ch->count > 0) {
        r_demand_msg_t *head = &ch->slots[ch->head];
        charged = head->bytes_accounted;
        ch->bytes -= charged;
        ch->held++;
        ch->held_bytes += charged;
        dmsg_release(&s->alloc, head);   /* only the canon, if any */
        ch->head = (ch->head + 1u) % ch->cap;
        ch->count--;
    }
    pthread_mutex_unlock(&ch->mu);
    return charged;
}

/* Release held charge: `entries` items worth `bytes` have been served (or
 * dropped), so their room returns to the src->dst channel and src is
 * credited exactly as a durable pop credits it. dst's stepper only. */
static void
dch_release_held(moqr_shards_t *s, uint16_t src, uint16_t dst,
                 uint32_t entries, uint64_t bytes)
{
    if (entries == 0) {
        return;
    }
    r_demand_channel_t *ch = &s->dch[(uint32_t)src * s->shard_count + dst];
    pthread_mutex_lock(&ch->mu);
    ch->held -= entries;
    ch->held_bytes -= bytes;
    s->shards[dst].pop_wake |= 1ull << src;
    pthread_mutex_unlock(&ch->mu);
}

/* -- the intent router (phase-2 observation only) --------------------------- */

static void
psub_metadata_release(r_mgr *m, r_psub *p)
{
    moqr_bind_t *b = m->s->shards[m->shard].bind;
    moqr_bind_meta_release(b, &p->ack_metadata);
    moqr_bind_meta_release(b, &p->latest_metadata);
    p->metadata_pending = false;
}

/* A synchronous owner-lane observation only: retain/coalesce, without core,
 * session or channel calls. ACK A is a separate pin even if B arrives before
 * the channel can carry A. A failed retain invalidates, never fabricates EMPTY. */
static void
shards_metadata_changed(void *ctx, moqr_track_t track, uint64_t track_gen,
                         const moqr_bind_meta_ref_t *latest)
{
    r_mgr *m = ctx;
    moqr_bind_t *b = m->s->shards[m->shard].bind;
    for (uint32_t i = 0; i < m->psub_cap; i++) {
        r_psub *p = &m->psub[i];
        if (!p->used || !p->has_otrack || p->undemanding ||
            p->terminating || p->done_pending ||
            p->otrack._opaque != track._opaque || p->otrack_gen != track_gen ||
            (p->ack_sent && latest->revision <= p->metadata_sent_revision)) {
            continue;
        }
        moqr_bind_meta_release(b, &p->latest_metadata);
        if (moqr_bind_meta_retain(b, latest, &p->latest_metadata) != MOQR_OK) {
            p->latest_metadata = *latest;
            p->latest_metadata.state = MOQR_BIND_META_UNAVAILABLE;
            p->latest_metadata.storage = NULL;
            p->latest_metadata.bytes = (moq_bytes_t){ NULL, 0 };
        }
        p->metadata_pending = true;
    }
}

/* Called from inside moqr_bind_pump for the manager binding's reserved cookie.
 * An NS_FOUND/NS_GONE that is one of the manager's own mirror echoes consumes a
 * pending per-entry echo and is dropped; the rest update this shard's local
 * candidate bit and mark the namespace dirty. An UPSTREAM_SUBSCRIBE is downstream
 * demand for a namespace this shard only MIRRORS — the core emitted it to the
 * mirror's reserved pseudo-binding cookie, so only remote-owned demand reaches
 * here (a local publisher's demand targets a real cookie and never routes). It
 * is recorded for FORWARDING (the demand phase round-trips it to the owner,
 * who answers over the reply channel). It must NOT mutate the core trie (that
 * is phase 3) or reenter the core. */
static bool
shards_router(void *ctx, const moqr_intent_t *it, uint64_t now_us)
{
    (void)now_us;
    r_mgr *m = ctx;
    if (it->kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE) {
        /* Borrowed + non-deferrable: consume in-call, deep-copying the
         * ns+name into an owned key (the intent's spans die with this call)
         * plus the owned scalars. A full array or a failed copy can't park a
         * borrowed intent → fail-stop (never a silent drop). */
        if (m->pend_len >= m->pend_cap) {
            m->oom = true;
            return true;
        }
        uint64_t base = MOQR_SHARD_COOKIE_BASE;
        if (it->binding_cookie < base ||
            it->binding_cookie - base >= m->s->shard_count) {
            m->oom = true;   /* not a mirror pseudo-binding: unroutable */
            return true;
        }
        uint32_t clen = 0;
        uint8_t *canon = canon2_build(it->ns_parts, it->ns_count, it->name,
                                      &clen, &m->alloc);
        if (canon == NULL) {
            m->oom = true;
            return true;
        }
        r_pdemand *d = &m->pend[m->pend_len++];
        memset(d, 0, sizeof(*d));
        d->track = it->track;
        d->track_gen = it->track_gen;
        d->demand_id = ++m->next_demand_id;
        d->canon = canon;
        d->canon_len = clen;
        d->origin = (uint16_t)(it->binding_cookie - base);
        if (m->s->cluster_enabled) {
            moqr_cluster_route_t route;
            moqr_result_t rc = moqr_core_cluster_upstream_route(m->core,
                it->track, it->track_gen, &route, &d->requester_hop_id,
                &d->requester_correlation_id);
            if (rc != MOQR_OK) {
                /* The source can withdraw after this intent was queued and
                 * before the binding drains it. Its generation is then moot;
                 * never turn it into a fresh lookup on a replacement route. */
                sh_free(&m->alloc, d->canon, d->canon_len);
                m->pend_len--;
                if (rc != MOQR_DONE && rc != MOQR_ERR_STALE_HANDLE &&
                    rc != MOQR_ERR_WRONG_STATE) m->oom = true;
                return true;
            }
            d->source_binding_cookie = route.source_binding_cookie;
            d->source_route_generation = route.route_generation;
        }
        d->state = D_ST_RECORDED;
        d->term_overrun = false;
        d->term_quiet = false;
        d->popen_row = UINT32_MAX;
        d->ack_has_largest = false;
        d->ack_largest_group = 0;
        d->ack_largest_object = 0;
        d->feed_seen = false;
        d->feed_min_group = 0;
        return true;
    }
    if (it->kind == MOQR_INTENT_ACCEPT_SUB ||
        it->kind == MOQR_INTENT_REJECT_SUB ||
        it->kind == MOQR_INTENT_SUB_DONE) {
        /* Owner role: the pump-sub's lifecycle intents come back on the
         * requester's pseudo-binding with cookie = demand_id. A demand id is
         * only monotonic per REQUESTER, so the entry key is {src, demand_id}
         * — src recovered from the pseudo-binding cookie the intent rode in
         * on. Only reply OBLIGATIONS are recorded here (flags on the
         * admitted-demand entry); the demand phase pushes the actual channel
         * messages, so the bind pump never touches a channel mutex. An
         * intent with no matching entry is moot (the demand was torn down
         * first). */
        uint64_t base = MOQR_SHARD_COOKIE_BASE;
        if (it->binding_cookie < base ||
            it->binding_cookie - base >= m->s->shard_count) {
            return true;   /* not a per-origin pseudo-binding: not ours */
        }
        uint16_t src = (uint16_t)(it->binding_cookie - base);
        for (uint32_t i = 0; i < m->psub_cap; i++) {
            r_psub *p = &m->psub[i];
            if (!p->used || p->src != src || p->demand_id != it->cookie) {
                continue;
            }
            if (it->kind == MOQR_INTENT_ACCEPT_SUB) {
                /* A cancelled demand never ACKs — the requester is gone and
                 * the staged teardown will retire the pump-sub. */
                if (!p->undemanding && !p->ack_pending && !p->ack_sent &&
                    p->has_otrack && moqr_sub_eq(p->sub, it->sub)) {
                    /* No lookup here: the binding may already hold a newer
                     * directory set than this scalar reply was pinned to. */
                    (void)moqr_bind_meta_retain_executing(
                        m->s->shards[m->shard].bind, it, &p->ack_metadata);
                    p->ack_pending = true;
                    p->has_largest = it->has_largest;
                    p->largest_group = it->largest_group;
                    p->largest_object = it->largest_object;
                }
            } else {
                p->done_pending = true;
                p->done_pre_ack = (it->kind == MOQR_INTENT_REJECT_SUB);
                p->done_code = it->error_code;
                p->done_pd = it->pd;
                /* The terminal already retired the pump-sub in the core;
                 * a racing teardown must not re-run stage 1. */
                p->pump_sub_retired = true;
            }
            break;
        }
        return true;
    }
    if (it->kind == MOQR_INTENT_UPSTREAM_UNSUBSCRIBE) {
        /* Requester role: an acknowledged remote demand went idle — the
         * ACTIVE linger elapsed with no subscribers, and the core emitted the
         * unsubscribe toward the mirror pseudo-binding carrying
         * upstream_cookie = demand_id. Flip the entry to UNDEMANDING; the
         * demand phase owns pushing the UNDEMAND (durable retry on a full
         * channel). A probe-emitted unsubscribe carries cookie 0 and matches
         * nothing; ids start at 1. */
        for (uint32_t i = 0; i < m->pend_len; i++) {
            if (m->pend[i].demand_id == it->cookie &&
                m->pend[i].state == D_ST_ACKED) {
                /* Route through the staged terminal, quietly: the track is
                 * already WARM, but partially ingested OPEN records must be
                 * abandoned (STOPPED) before the demand and its progress row
                 * disappear — a WARM log keeps its records, and a later
                 * rejoin must never meet stale partial state. */
                m->pend[i].state = D_ST_TERMINATING;
                m->pend[i].term_code = 0;
                m->pend[i].term_overrun = false;
                m->pend[i].term_quiet = true;
                break;
            }
        }
        return true;
    }
    if (it->kind != MOQR_INTENT_NS_FOUND && it->kind != MOQR_INTENT_NS_GONE) {
        return true;
    }
    if (m->s->cluster_enabled) {
        return true; /* route snapshots observe all alternatives, not fan-out */
    }
    uint8_t op = (it->kind == MOQR_INTENT_NS_FOUND) ? R_OP_ANNOUNCE
                                                    : R_OP_UNANNOUNCE;
    uint32_t len = 0;
    uint8_t *canon = canon_build(it->ns_parts, it->ns_count, &len, &m->alloc);
    if (canon == NULL) {
        m->oom = true;
        return true;   /* borrowed view can't be parked; bounded + counted */
    }
    uint32_t ins = 0;
    r_jentry *e = jrn_find(m, canon, len, &ins);
    if (e != NULL) {
        uint16_t *echo = (op == R_OP_ANNOUNCE) ? &e->echo_ann : &e->echo_unann;
        if (*echo > 0) {
            (*echo)--;                    /* our own mirror echo — suppress */
            sh_free(&m->alloc, canon, len);
            return true;
        }
    }
    uint64_t bit = 1ull << m->shard;
    if (op == R_OP_ANNOUNCE) {
        if (e == NULL) {
            e = jrn_get(m, canon, len);   /* adopts canon on insert */
            if (e == NULL) {
                m->oom = true;
                sh_free(&m->alloc, canon, len);
                return true;
            }
        } else {
            sh_free(&m->alloc, canon, len);   /* existing entry; drop the dup */
        }
        jrn_set_candidate(m, e, bit, true);
    } else {
        sh_free(&m->alloc, canon, len);       /* find-only; never adopted */
        if (e != NULL) {
            jrn_set_candidate(m, e, bit, false);
        }
    }
    return true;
}

/* -- phase 3: dirty-reconcile ----------------------------------------------- */

static void
mgr_update_holdout(r_mgr *m, r_jentry *e, bool holdout)
{
    if (holdout && !e->holdout) {
        m->holdouts++;
        m->journal_epoch++;   /* holdout transition: a projection change */
    } else if (!holdout && e->holdout) {
        m->holdouts--;
        m->journal_epoch++;
    }
    e->holdout = holdout;
}

static void
mgr_reconcile_entry(r_mgr *m, r_jentry *e, uint64_t now_us)
{
    int32_t winner = r_winner_of(e->candidates, e->hash);
    uint64_t local_bit = (e->candidates >> m->shard) & 1u;

    moq_bytes_t parts[R_NS_MAX_PARTS];
    moqr_ns_t ns = canon_to_ns(e->canon, parts);

    /* A local publisher that lost to a remote winner is withdrawn before any
     * mirror work: force-withdraw purges its announce and every track it
     * sourced (SUB_DONE terminals carry PUBLISH_DONE TRACK_ENDED; the
     * publisher cancel and parked rejects carry UNINTERESTED) and queues
     * the publisher cancel.
     * On success the local candidate bit clears HERE, so the winner's mirror
     * installs in this same pass; the withdrawal's own NS_GONE reaches our
     * watcher next round as a REAL event (force-withdraw is not a mirror op,
     * so no self-echo is minted) and re-clears the already-clear bit — an
     * idempotent no-op. WOULD_BLOCK means the withdrawal is INCOMPLETE, not
     * that nothing happened: the purge is resumable, so some of the loser's
     * tracks may already be gone. What is guaranteed is that its announce is
     * still installed — the namespace effects are the withdrawal's final unit
     * — so the entry stays dirty, the loser stays counted as a holdout, and
     * the winner is NOT installed over an announce that still stands. Retried
     * next round, resuming from the tracks that remain. Any other result fail-stops the step (never
     * silently converge over a loser that could not be withdrawn). */
    if (local_bit != 0 && winner >= 0 && (uint16_t)winner != m->shard) {
        moqr_result_t rc =
            moqr_core_force_withdraw(m->core, ns, R_ERR_UNINTERESTED, now_us);
        if (rc == MOQR_ERR_WOULD_BLOCK) {
            /* Resumable, not inert: the purge got part way and queued that
             * much fan-out, so the drain is owed exactly as on success. */
            m->reconcile_owed_local_drain = true;
            mgr_update_holdout(m, e, true);
            return;   /* stay dirty; retry next round */
        }
        if (rc != MOQR_OK) {
            m->oom = true;   /* permanent: nothing mutated, nothing owed */
            return;
        }
        m->reconcile_owed_local_drain = true;
        e->candidates &= ~(1ull << m->shard);
        m->journal_epoch++;   /* candidate removal (the withdrawn loser) */
        local_bit = 0;
        winner = r_winner_of(e->candidates, e->hash);
    }

    /* Desired mirror: a remote winner with no local publisher here. */
    int32_t desired = R_MIRROR_NONE;
    if (winner >= 0 && (uint16_t)winner != m->shard && local_bit == 0) {
        desired = winner;
    }

    /* Holdout gauge: with enforcement above, a holdout persists only while its
     * withdrawal defers on WOULD_BLOCK (handled there); on every other path the
     * condition is re-derived from the possibly-just-cleared candidate set. */
    mgr_update_holdout(m, e, local_bit != 0 && winner >= 0 &&
                                  (uint16_t)winner != m->shard);

    if (desired == e->mirror) {
        e->dirty = false;   /* already converged */
        return;
    }

    /* Withdraw the stale mirror first. A transient failure (WOULD_BLOCK — the
     * intent ring is full) keeps the mirror installed and the entry dirty so we
     * retry next round; we never install a replacement over a mirror we failed to
     * remove. The echo counter is bumped only after the op succeeds — the echo is
     * queued to our watcher and observed a round later, never synchronously. */
    if (e->mirror != R_MIRROR_NONE) {
        if (moqr_core_unannounce(m->core, m->pb[e->mirror], ns) != MOQR_OK) {
            return;   /* stay dirty; retry next round */
        }
        m->reconcile_owed_local_drain = true;
        e->echo_unann++;
        e->mirror = R_MIRROR_NONE;
        m->journal_epoch++;   /* mirror teardown */
    }

    /* Install the new mirror. WRONG_STATE is permanent — a local publisher already
     * occupies the trie node (a late local announce, or this shard is the local
     * loser); stop retrying and hold on NONE (the local publisher is the holdout).
     * WOULD_BLOCK is transient: stay dirty and retry next round. */
    bool was_blocked = e->cap_blocked;
    if (e->cap_blocked) {
        e->cap_blocked = false;
        m->cap_blocked_count--;
    }
    if (desired != R_MIRROR_NONE && was_blocked &&
        moqr_core_announce_room(m->core, ns) == MOQR_ERR_CAPACITY) {
        /* Still no room: stay parked without another counted refusal. The
         * episode was counted once, by the install that first failed. */
        e->cap_blocked = true;
        m->cap_blocked_count++;
        e->dirty = false;
        return;
    }
    if (desired != R_MIRROR_NONE) {
        moqr_result_t rc = moqr_core_announce(m->core, m->pb[desired], ns);
        if (rc == MOQR_OK) {
            m->reconcile_owed_local_drain = true;
            e->echo_ann++;
            e->mirror = desired;
            m->journal_epoch++;   /* mirror install */
        } else if (rc == MOQR_ERR_WOULD_BLOCK) {
            return;   /* stay dirty; retry next round */
        } else if (rc == MOQR_ERR_CAPACITY) {
            /* The core holds only live namespaces right now. Room appears
             * only when capacity is released or becomes reclaimable (a
             * withdrawal, a freed track or reclaimed node), which moves the
             * core's room epoch, so park the entry until that epoch moves
             * past this refusal: bounded, no self-wake. Room
             * that appears is noticed at this shard's next step (any wake);
             * there is no timer for it. Retries ask moqr_core_announce_room
             * first, so one parked episode counts one refusal. */
            e->cap_blocked = true;
            m->cap_blocked_count++;
        }
        /* else WRONG_STATE / permanent: leave mirror NONE, clear dirty below. */
    }

    e->dirty = false;
}

/* -- create / destroy of the per-shard manager ------------------------------ */

static void
mgr_free(r_mgr *m)
{
    if (m == NULL) {
        return;
    }
    const moq_alloc_t *a = &m->alloc;
    if (m->cluster != NULL) {
        for (uint32_t i = 0; i < m->s->cluster_routes; ++i) {
            sh_free(a, m->cluster[i].canon, m->cluster[i].canon_len);
            sh_free(a, m->cluster[i].hops,
                    m->cluster[i].record.route.metadata.hop_count * sizeof(uint64_t));
        }
        sh_free(a, m->cluster,
                (size_t)m->s->cluster_routes * sizeof(*m->cluster));
    }
    if (m->jrn != NULL) {
        for (uint32_t i = 0; i < m->jrn_len; i++) {
            sh_free(a, m->jrn[i].canon, m->jrn[i].canon_len);
        }
        sh_free(a, m->jrn, (size_t)m->jrn_cap * sizeof(m->jrn[0]));
    }
    if (m->pend != NULL) {
        for (uint32_t i = 0; i < m->pend_len; i++) {
            sh_free(a, m->pend[i].canon, m->pend[i].canon_len);
        }
        sh_free(a, m->pend, (size_t)m->pend_cap * sizeof(m->pend[0]));
    }
    sh_free(a, m->psub, (size_t)m->psub_cap * sizeof(m->psub[0]));
    sh_free(a, m->psg,
            (size_t)m->psub_cap * m->s->sg_slots * sizeof(r_psg_t));
    sh_free(a, m->popen,
            (size_t)m->pend_cap * m->s->sg_slots * sizeof(r_psg_t));
    sh_free(a, m->popen_row_used, m->pend_cap);
    if (m->fret != NULL) {
        for (uint32_t i = 0; i < m->fill_cap; i++) {
            for (uint32_t k = 0; k < R_FILL_WINDOW; k++) {
                moq_rcbuf_decref(m->fret[i].win[k].payload);
                moq_rcbuf_decref(m->fret[i].win[k].props);
            }
            moq_rcbuf_decref(m->fret[i].meta_props);
            sh_free(a, m->fret[i].canon, m->fret[i].canon_len);
        }
        sh_free(a, m->fret, (size_t)m->fill_cap * sizeof(m->fret[0]));
    }
    if (m->oret != NULL) {
        for (uint32_t i = 0; i < m->fill_cap; i++) {
            /* bind_destroy already released collector/cached copies. */
            moqr_bind_retrieve_forget(&m->oret[i].cursor);
            moq_rcbuf_decref(m->oret[i].meta_props);
        }
        sh_free(a, m->oret, (size_t)m->fill_cap * sizeof(m->oret[0]));
    }
    if (m->pb != NULL) {
        sh_free(a, m->pb, (size_t)m->s->shard_count * sizeof(m->pb[0]));
    }
    sh_free(a, m, sizeof(*m));
}

static r_mgr *
mgr_create(moqr_shards_t *s, uint16_t shard, moqr_core_t *core)
{
    const moq_alloc_t *a = &s->alloc;
    r_mgr *m = sh_alloc(a, sizeof(*m));
    if (m == NULL) {
        return NULL;
    }
    memset(m, 0, sizeof(*m));
    m->s = s;
    m->alloc = s->alloc;
    m->shard = shard;
    m->core = core;
    m->jrn_cap = s->jrn_cap;
    m->pend_cap = s->pend_cap;
    m->psub_cap = s->pend_cap;
    if (s->cluster_enabled) {
        m->cluster = sh_alloc(a, (size_t)s->cluster_routes * sizeof(*m->cluster));
        if (m->cluster == NULL) {
            mgr_free(m);
            return NULL;
        }
        memset(m->cluster, 0, (size_t)s->cluster_routes * sizeof(*m->cluster));
        for (uint32_t i = 0; i < s->cluster_routes; ++i)
            m->cluster[i].sent = UINT64_MAX;
    }
    m->jrn = sh_alloc(a, (size_t)m->jrn_cap * sizeof(m->jrn[0]));
    m->pend = sh_alloc(a, (size_t)m->pend_cap * sizeof(m->pend[0]));
    m->psub = sh_alloc(a, (size_t)m->psub_cap * sizeof(m->psub[0]));
    if (s->admit_remote) {
        /* Subgroup-progress storage exists only where admitted data can
         * flow; a refusal-only runtime stays structurally inert — nothing
         * reserved, nothing to validate. */
        m->psg = sh_alloc(a,
                          (size_t)m->psub_cap * s->sg_slots * sizeof(r_psg_t));
        m->popen =
            sh_alloc(a, (size_t)m->pend_cap * s->sg_slots * sizeof(r_psg_t));
        m->popen_row_used = sh_alloc(a, m->pend_cap);
        m->fill_cap = s->fill_cap;
        m->fret = sh_alloc(a, (size_t)m->fill_cap * sizeof(m->fret[0]));
        m->oret = sh_alloc(a, (size_t)m->fill_cap * sizeof(m->oret[0]));
        if (m->fret != NULL) {
            memset(m->fret, 0, (size_t)m->fill_cap * sizeof(m->fret[0]));
        }
        if (m->oret != NULL) {
            memset(m->oret, 0, (size_t)m->fill_cap * sizeof(m->oret[0]));
        }
    }
    m->pb = sh_alloc(a, (size_t)s->shard_count * sizeof(m->pb[0]));
    if (m->jrn == NULL || m->pend == NULL || m->psub == NULL ||
        (s->admit_remote &&
         (m->psg == NULL || m->popen == NULL ||
          m->popen_row_used == NULL ||
          (m->fill_cap > 0 && (m->fret == NULL || m->oret == NULL)))) ||
        m->pb == NULL) {
        mgr_free(m);
        return NULL;
    }
    memset(m->psub, 0, (size_t)m->psub_cap * sizeof(m->psub[0]));
    if (s->admit_remote) {
        memset(m->psg, 0,
               (size_t)m->psub_cap * s->sg_slots * sizeof(r_psg_t));
        memset(m->popen, 0,
               (size_t)m->pend_cap * s->sg_slots * sizeof(r_psg_t));
        memset(m->popen_row_used, 0, m->pend_cap);
    }
    memset(m->pb, 0, (size_t)s->shard_count * sizeof(m->pb[0]));
    return m;
}

/* Open the pseudo-bindings + the wildcard watcher on the manager's core. */
static moqr_result_t
mgr_open_bindings(r_mgr *m)
{
    uint16_t k = m->s->shard_count;
    moqr_binding_options_t options;
    moqr_binding_options_init_sized(&options, sizeof(options));
    options.flags = MOQR_BINDING_TRACK_METADATA_MANAGED;
    for (uint16_t j = 0; j < k; j++) {
        if (j == m->shard) {
            continue;
        }
        moqr_result_t rc = moqr_core_binding_open_with_options(
            m->core, MOQR_SHARD_COOKIE_BASE + j, &options, &m->pb[j]);
        if (rc != MOQR_OK) {
            return rc;
        }
    }
    moqr_result_t rc = moqr_core_binding_open_with_options(
        m->core, MOQR_SHARD_COOKIE_BASE + k, &options, &m->mgr_bind);
    if (rc != MOQR_OK) {
        return rc;
    }
    if (m->s->cluster_enabled) {
        return MOQR_OK; /* snapshots enumerate all sources directly */
    }
    /* Zero-count wildcard prefix: matches every announce; opened on an empty
     * trie at create, so there is no replay backlog to overflow the intent
     * ring. */
    moqr_ns_t wildcard = { NULL, 0 };
    return moqr_core_ns_subscribe(m->core, m->mgr_bind, wildcard,
                                  MOQR_SHARD_COOKIE_BASE + k);
}

/* -- the six-phase stepper ---------------------------------------------------- */

/* Apply one ready mailbox slot to the journal and release it. Returns false and
 * leaves the slot in place ONLY when an announce cannot be recorded because the
 * journal is full — the slot is retried next round (cursor lag). All other cases
 * (announce recorded, any unannounce) consume the slot. */
static bool
shard_apply_slot(r_mgr *m, r_mailbox *mb, r_ctrl_msg *slot)
{
    if (slot->cluster) {
        moqr_cluster_export_t record = slot->record;
        moq_bytes_t parts[R_NS_MAX_PARTS];
        if (slot->op == R_OP_ANNOUNCE)
            record.ns = canon_to_ns(slot->canon, parts);
        record.route.metadata.hop_ids = slot->hops;
        moqr_result_t rc = moqr_core_cluster_import(m->core,
            m->pb[slot->origin], &record, slot->op == R_OP_UNANNOUNCE);
        if (rc == MOQR_ERR_WRONG_STATE && slot->op == R_OP_ANNOUNCE) {
            /* A source pool slot was reused for a different namespace. The
             * old lifetime withdraws before its replacement is admitted. */
            rc = moqr_core_cluster_import(m->core, m->pb[slot->origin],
                                          &record, true);
            if (rc == MOQR_OK || rc == MOQR_DONE) {
                m->reconcile_owed_local_drain = true;
                rc = moqr_core_cluster_import(m->core, m->pb[slot->origin],
                                              &record, false);
            }
        }
        if (rc == MOQR_ERR_WOULD_BLOCK || rc == MOQR_ERR_CAPACITY ||
            rc == MOQR_ERR_NOMEM) return false;
        if (rc != MOQR_OK && rc != MOQR_DONE) {
            m->oom = true;
            return false;
        }
        m->reconcile_owed_local_drain = true;
        sh_free(&m->alloc, slot->canon, slot->canon_len);
        sh_free(&m->alloc, slot->hops,
                slot->record.route.metadata.hop_count * sizeof(uint64_t));
        slot->used = false;
        slot->hops = NULL;
        mb->count--;
        /* A full control mailbox may have held the producer on an unsent
         * alternative. Return credit on this same durable pop; concurrent
         * steppers must not depend on an unrelated future session event. */
        m->s->shards[m->shard].pop_wake |= 1ull << slot->origin;
        return true;
    }
    uint64_t bit = 1ull << slot->origin;
    if (slot->op == R_OP_ANNOUNCE) {
        r_jentry *e = jrn_get(m, slot->canon, slot->canon_len);
        if (e == NULL) {
            return false;   /* journal full: leave the slot, retry next round */
        }
        /* jrn_get adopts the canon on insert; on an existing entry the mailbox's
         * copy is redundant and freed below. */
        bool adopted = (e->canon == slot->canon);
        jrn_set_candidate(m, e, bit, true);
        if (!adopted) {
            sh_free(&m->alloc, slot->canon, slot->canon_len);
        }
    } else {
        r_jentry *e = jrn_find(m, slot->canon, slot->canon_len, NULL);
        if (e != NULL) {
            jrn_set_candidate(m, e, bit, false);
        }
        sh_free(&m->alloc, slot->canon, slot->canon_len);
    }
    slot->used = false;
    mb->count--;
    return true;
}

/* Phase 1 — apply every prior-round inbound control slot into the candidate set,
 * marking namespaces dirty. No core-trie mutation here. Each namespace's slot is
 * independent (the mailbox coalesces per namespace), so a slot that cannot be
 * recorded because the journal is full is simply left for the next round while the
 * others — including unannounces that free journal slots — still apply. Capacity is
 * lag, never loss, and never head-of-line. */
static void
shard_inbound_batch(moqr_shards_t *s, uint16_t shard)
{
    r_mgr *m = s->shards[shard].mgr;
    if (m == NULL) {
        return;
    }
    for (uint16_t src = 0; src < s->shard_count; src++) {
        if (src == shard) {
            continue;
        }
        r_mailbox *mb = &s->mbox[(uint32_t)src * s->shard_count + shard];
        pthread_mutex_lock(&mb->mu);
        for (uint32_t i = 0; i < mb->cap; i++) {
            r_ctrl_msg *slot = &mb->slots[i];
            if (!slot->used) {
                continue;
            }
            if (!s->live_visibility && slot->round >= s->round) {
                continue;   /* pushed this round: the deterministic barrier */
            }
            (void)shard_apply_slot(m, mb, slot);
        }
        pthread_mutex_unlock(&mb->mu);
    }
}

/* Phase 3 — reconcile every dirty namespace in lexicographic key order (the
 * journal is kept sorted, so array order IS lexicographic). This phase alone
 * mutates the core trie (mirror ops and loser force-withdrawals). */
static void
shard_reconcile(moqr_shards_t *s, uint16_t shard, uint64_t now_us)
{
    r_mgr *m = s->shards[shard].mgr;
    if (m == NULL) {
        return;
    }
    if (s->cluster_enabled) {
        return; /* import applies atomic source-slot snapshots */
    }
    uint64_t room_epoch = moqr_core_room_epoch(m->core);
    if (m->cap_blocked_count != 0 && room_epoch != m->cap_epoch) {
        /* Room may have appeared in the core since a mirror install was
         * refused for room: retry those installs. */
        for (uint32_t i = 0; i < m->jrn_len; i++) {
            if (m->jrn[i].cap_blocked) {
                m->jrn[i].dirty = true;
            }
        }
    }
    /* A later withdrawal may free room for an entry already visited. Only
     * acknowledge this pass's starting epoch, so that entry retries next time. */
    m->cap_epoch = room_epoch;
    for (uint32_t i = 0; i < m->jrn_len; i++) {
        if (m->jrn[i].dirty) {
            mgr_reconcile_entry(m, &m->jrn[i], now_us);
        }
    }
}

/* Phase 4 — export ONLY this shard's own local-origin state to each destination
 * (never remote candidate bits), converging each mailbox to the current state
 * as space allows; collapse intermediate churn; reclaim fully-quiesced entries.
 * Walks in the same lexicographic order as reconcile. `pushed_dst_mask`
 * (optional) accumulates bit d for every destination whose mailbox accepted a
 * push (the demand/data phases fold their channel pushes into the same mask)
 * — the seam a runtime uses to wake exactly the shards with new inbound
 * state waiting. */
static void
shard_outbound(moqr_shards_t *s, uint16_t shard, uint64_t *pushed_dst_mask)
{
    r_mgr *m = s->shards[shard].mgr;
    if (m == NULL) {
        return;
    }
    if (s->cluster_enabled) {
        shard_cluster_outbound(s, shard, pushed_dst_mask);
        return;
    }
    uint32_t i = 0;
    while (i < m->jrn_len) {
        r_jentry *e = &m->jrn[i];
        uint64_t local = (e->candidates >> shard) & 1u;
        for (uint16_t d = 0; d < s->shard_count; d++) {
            if (d == shard) {
                continue;
            }
            uint64_t told = (e->sent >> d) & 1u;
            if (local == told) {
                continue;
            }
            uint8_t op = local ? R_OP_ANNOUNCE : R_OP_UNANNOUNCE;
            if (mbox_push(s, shard, d, op, shard, e->canon, e->canon_len)) {
                if (local) {
                    e->sent |= (1ull << d);
                } else {
                    e->sent &= ~(1ull << d);
                }
                if (pushed_dst_mask != NULL) {
                    *pushed_dst_mask |= 1ull << d;
                }
            }
            /* full: leave `sent` unchanged, retry next round (cursor lag). */
        }
        /* Reclaim a fully-quiesced entry: no live candidate, no mirror, every
         * destination already told "absent", and no self-echo still owed to it (a
         * pending mirror echo must find its entry, or it would be read as a real
         * local event). */
        if (e->candidates == 0 && e->mirror == R_MIRROR_NONE && e->sent == 0 &&
            !e->dirty && e->echo_ann == 0 && e->echo_unann == 0) {
            jrn_remove(m, i);
            continue;   /* i now indexes the next entry */
        }
        i++;
    }
}

/* -- fill retrieval: requester ----------------------------------------------- */

/* A record of `group` reached this shard's log through demand d's feed. */
static void
pend_note_feed(r_pdemand *d, uint64_t group)
{
    if (!d->feed_seen || group < d->feed_min_group) {
        d->feed_min_group = group;
    }
    d->feed_seen = true;
}

static int
r_loc_cmp(uint64_t ag, uint64_t ao, uint64_t bg, uint64_t bo)
{
    if (ag != bg) {
        return ag < bg ? -1 : 1;
    }
    if (ao != bo) {
        return ao < bo ? -1 : 1;
    }
    return 0;
}

static r_fret_t *
fret_find(r_mgr *m, uint64_t id)
{
    for (uint32_t i = 0; m->fret != NULL && i < m->fill_cap; i++) {
        if (m->fret[i].used && m->fret[i].id == id) {
            return &m->fret[i];
        }
    }
    return NULL;
}

/* Drop the window's items: their clones now, their channel charge in the
 * next demand phase (which owns the channel side). */
static void
fret_drop_window(r_fret_t *f)
{
    while (f->count > 0) {
        r_fitem_t *it = &f->win[f->head];
        moq_rcbuf_decref(it->payload);
        moq_rcbuf_decref(it->props);
        f->release_entries++;
        f->release_bytes += it->charged;
        memset(it, 0, sizeof(*it));
        f->head = (f->head + 1u) % R_FILL_WINDOW;
        f->count--;
    }
    f->head = 0;
}

/* The demand a retrieval rides is ending: the owner drops its side with the
 * demand, so nothing more will come and nothing is owed to it. */
static void
fret_demand_gone(r_mgr *m, uint64_t demand_id)
{
    for (uint32_t i = 0; m->fret != NULL && i < m->fill_cap; i++) {
        r_fret_t *f = &m->fret[i];
        if (f->used && f->demand_id == demand_id) {
            f->ended = true;
            memset(&f->terminal, 0, sizeof(f->terminal));
            f->terminal.kind = MOQR_RETRIEVE_ERROR;
            f->terminal.error_kind = MOQR_RETRIEVE_SOURCE_LOST;
            f->terminal.local_result = MOQR_ERR_STALE_HANDLE;
            f->req_owed = false;
            f->more_owed = false;
            f->cancel_owed = false;
        }
    }
}

/* The fill source's open (moqr_bind_fill_source_t): claim the part of the
 * range this shard's log cannot hold. The feed of an accepted demand carries
 * what follows the Largest the owner reported in its ACK, so the owner must
 * answer everything at or below it from cache plus upstream retrieval -- and,
 * until the feed has brought anything, the whole range. Once it has, the claim
 * runs through the groups before the
 * feed's first group, whole: the log takes over at a group it holds, so it
 * never reports the groups the feed never carried as an evicted prefix, and
 * an Object of those groups still on its way through the feed is answered
 * by the owner instead of being skipped. A track not fed by an accepted
 * demand, an ACK without a Largest (the feed carries everything), or a range
 * that starts after the claimable part is not the source's. */
static moqr_result_t
shards_fill_open(void *ctx, const moqr_bind_fill_query_t *q,
                 moqr_bind_fill_claim_t *claim)
{
    r_mgr *m = ctx;
    if (m == NULL || q == NULL || claim == NULL ||
        q->struct_size < offsetof(moqr_bind_fill_query_t, deadline_us) +
                             sizeof(q->deadline_us)) {
        return MOQR_ERR_INVAL;
    }
    const r_pdemand *d = NULL;
    for (uint32_t i = 0; i < m->pend_len; i++) {
        const r_pdemand *p = &m->pend[i];
        if (p->state == D_ST_ACKED && moqr_track_eq(p->track, q->track) &&
            p->track_gen == q->track_gen) {
            d = p;
            break;
        }
    }
    if (d == NULL || !d->ack_has_largest || m->fret == NULL) {
        return MOQR_DONE;
    }
    uint64_t eg = q->end_group, eo = q->end_object;
    if (d->feed_seen) {
        uint64_t cg = d->ack_largest_group, co = d->ack_largest_object;
        if (d->feed_min_group > d->ack_largest_group) {
            cg = d->feed_min_group - 1u;
            co = UINT64_MAX;
        }
        if (r_loc_cmp(q->start_group, q->start_object, cg, co) > 0) {
            return MOQR_DONE;
        }
        if (r_loc_cmp(eg, eo, cg, co) > 0) {
            eg = cg;
            eo = co;
        }
    }
    r_fret_t *f = NULL;
    for (uint32_t i = 0; i < m->fill_cap; i++) {
        if (!m->fret[i].used) {
            f = &m->fret[i];
            break;
        }
    }
    if (f == NULL) {
        m->fill_starved = true;
        return MOQR_ERR_WOULD_BLOCK;
    }
    memset(f, 0, sizeof(*f));
    f->used = true;
    f->req_owed = true;
    f->origin = d->origin;
    f->id = ++m->next_fill_id;
    f->demand_id = d->demand_id;
    f->start_group = q->start_group;
    f->start_object = q->start_object;
    f->next_group = q->start_group;
    f->next_object = q->start_object;
    f->end_group = eg;
    f->end_object = eo;
    f->deadline_us = q->deadline_us;
    f->order = (uint8_t)q->group_order;
    f->prio = q->subscriber_priority;
    claim->handle = f->id;
    claim->end_group = eg;
    claim->end_object = eo;
    return MOQR_OK;
}

/* Whether the owner's outstanding delivery toward `src` came from an
 * upstream subgroup whose header set PROPERTIES (false for a notice). */
static bool
shard_delivery_subgroup_props(r_mgr *m, uint16_t src)
{
    bool sgp = false, follows_gap = false;
    moqr_reset_desc_t gap_reset;
    if (moqr_core_internal_delivery_flags(m->core, m->pb[src], &sgp, &follows_gap,
                                          &gap_reset) != MOQR_OK) {
        return false;
    }
    return sgp;
}

/* The standalone FETCH source's open: ask the shard that owns the track
 * (named by its binding cookie here) to admit and answer the request. The
 * key is copied now; the REQ carries it. No free retrieval slot refuses this
 * request alone. A Joining FETCH names the accepted demand that feeds the
 * joined track here: the owner answers it only while that demand's
 * subscription there still reads the track the name resolves to. With no
 * such demand, the subscription it joins is no longer fed (STALE_HANDLE). */
static moqr_result_t
shards_sa_open(void *ctx, const moqr_retrieve_standalone_query_t *q,
               uint64_t *handle)
{
    r_mgr *m = ctx;
    if (m == NULL || q == NULL || handle == NULL || m->fret == NULL) {
        return MOQR_ERR_INVAL;
    }
    if (m->s->cluster_enabled && !q->cluster_selected) {
        return MOQR_ERR_WRONG_STATE; /* never dispatch an unfiltered mirror */
    }
    uint64_t base = MOQR_SHARD_COOKIE_BASE;
    if (q->remote_cookie < base || q->remote_cookie - base >= m->s->shard_count ||
        q->remote_cookie - base == m->shard) {
        return MOQR_ERR_INVAL;
    }
    r_fret_t *f = NULL;
    for (uint32_t i = 0; i < m->fill_cap; i++) {
        if (!m->fret[i].used) {
            f = &m->fret[i];
            break;
        }
    }
    if (f == NULL) {
        return MOQR_ERR_CAPACITY;
    }
    uint64_t join_demand = 0;
    if (q->join) {
        for (uint32_t i = 0; i < m->pend_len && join_demand == 0; i++) {
            const r_pdemand *d = &m->pend[i];
            if (d->state == D_ST_ACKED && moqr_track_eq(d->track, q->join_track) &&
                d->origin == (uint16_t)(q->remote_cookie - base)) {
                join_demand = d->demand_id;
            }
        }
        if (join_demand == 0) {
            return MOQR_ERR_STALE_HANDLE;
        }
    }
    uint32_t clen = 0;
    uint8_t *canon = canon2_build(q->ns.parts, (uint32_t)q->ns.count, q->name,
                                  &clen, &m->alloc);
    if (canon == NULL) {
        return MOQR_ERR_NOMEM;
    }
    memset(f, 0, sizeof(*f));
    f->used = true;
    f->standalone = true;
    f->cluster_selected = q->cluster_selected;
    f->source_binding_cookie = q->cluster_source_binding_cookie;
    f->source_route_generation = q->cluster_source_route_generation;
    f->requester_hop_id = q->cluster_requester_hop_id;
    f->requester_correlation_id = q->cluster_requester_correlation_id;
    f->req_owed = true;
    f->origin = (uint16_t)(q->remote_cookie - base);
    f->id = ++m->next_fill_id;
    f->canon = canon;
    f->canon_len = clen;
    f->start_group = q->start_group;
    f->start_object = q->start_object;
    f->next_group = q->start_group;
    f->next_object = q->start_object;
    f->end_group = q->end_group;
    f->end_object = q->end_object;
    f->range_form = q->range_form;
    f->relative_start = q->relative_start;
    f->join_demand_id = join_demand;
    f->deadline_us = q->deadline_us;
    f->order = (uint8_t)q->group_order;
    f->prio = q->subscriber_priority;
    *handle = f->id;
    return MOQR_OK;
}

/* The owner's admission, once its OPEN has crossed. An answer that ended
 * before admitting (no slot there, or the owner gone) is an error. */
static moqr_result_t
shards_sa_plan(void *ctx, uint64_t handle, moqr_retrieve_standalone_plan_t *out)
{
    r_mgr *m = ctx;
    r_fret_t *f = (m != NULL) ? fret_find(m, handle) : NULL;
    if (f == NULL || !f->standalone || f->closed || out == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (f->begun) {
        *out = f->plan;
        return MOQR_OK;
    }
    if (f->ended) {
        return f->terminal.local_result != MOQR_OK ? f->terminal.local_result
                                                   : MOQR_ERR_INTERNAL;
    }
    return MOQR_ERR_WOULD_BLOCK;
}

/* Stop expecting the answer: whatever has not begun by the deadline is the
 * fill's Timed-Out run, and an answer arriving later is dropped. A request
 * never pushed is simply not asked; otherwise retain one cancellation for
 * the ordinary demand phase, even when OPEN is still blocked at the owner. */
static void
fret_abandon(r_fret_t *f)
{
    f->abandoned = true;
    f->cancel_owed = !f->req_owed && !f->ended;
    f->req_owed = false;
    f->more_owed = false;
}

static void
fill_result_store(r_fill_result_t *out, const moqr_retrieve_item_t *in)
{
    *out = (r_fill_result_t){
        .kind = in->kind, .start = in->start, .end = in->end,
        .gap_kind = in->gap_kind, .error_kind = in->error_kind,
        .local_result = in->local_result, .session_result = in->session_result,
        .error_code = in->error_code, .retry_after_ms = in->retry_after_ms,
        .cause = in->cause, .can_retry = in->can_retry,
        .data_stream = in->data_stream, .stop_sending = in->stop_sending,
    };
}

static void
fill_result_load(moqr_retrieve_item_t *out, const r_fill_result_t *in)
{
    *out = (moqr_retrieve_item_t){
        .kind = in->kind, .start = in->start, .end = in->end,
        .gap_kind = in->gap_kind, .error_kind = in->error_kind,
        .local_result = in->local_result, .session_result = in->session_result,
        .error_code = in->error_code, .retry_after_ms = in->retry_after_ms,
        .cause = in->cause, .can_retry = in->can_retry,
        .data_stream = in->data_stream, .stop_sending = in->stop_sending,
    };
}

static moqr_result_t
shards_fill_retrieve_peek(void *ctx, uint64_t handle, uint64_t now_us,
                           moqr_retrieve_item_t *out)
{
    r_mgr *m = ctx;
    r_fret_t *f = (m != NULL) ? fret_find(m, handle) : NULL;
    if (f == NULL || f->closed || out == NULL) {
        return MOQR_ERR_STALE_HANDLE;
    }
    memset(out, 0, sizeof(*out));
    if (f->count > 0) {
        const r_fitem_t *it = &f->win[f->head];
        fill_result_load(out, &it->result);
        if (it->result.kind == MOQR_RETRIEVE_OBJECT) {
            out->object.group_id = it->group_id;
            out->object.subgroup_id = it->subgroup_id;
            out->object.object_id = it->object_id;
            out->object.publisher_priority = it->prio;
            out->object.datagram_pref = it->datagram_pref;
            out->object.status = (moqr_obj_status_t)it->status;
            out->object.end_of_group = it->end_of_group;
            out->object.declared_len = it->declared_len;
            out->object.obj_state = MOQR_OBJ_COMPLETE;
            out->object.payload = it->payload;
            out->object.properties = it->props;
            out->object.properties_codec = it->props_codec;
        } else if (it->result.kind == MOQR_RETRIEVE_ERROR && it->props != NULL) {
            out->reason = (moq_bytes_t){ moq_rcbuf_data(it->props),
                                         moq_rcbuf_len(it->props) };
        }
        return MOQR_OK;
    }
    if (f->ended) {
        fill_result_load(out, &f->terminal);
        return MOQR_OK;
    }
    if (f->exhausted) {
        out->kind = MOQR_RETRIEVE_END;
        out->start = (moqr_retrieve_location_t){ f->start_group, f->start_object };
        out->end = (moqr_retrieve_location_t){ f->end_group, f->end_object };
        return MOQR_OK;
    }
    if (!f->standalone && !f->abandoned && !f->begun &&
        now_us >= f->deadline_us) {
        fret_abandon(f);
    }
    if (f->abandoned) {
        out->kind = MOQR_RETRIEVE_GAP;
        out->start = (moqr_retrieve_location_t){ f->next_group, f->next_object };
        out->end = (moqr_retrieve_location_t){ f->end_group, f->end_object };
        out->gap_kind = MOQ_FETCH_RANGE_TIMED_OUT;
        f->synth_peeked = true;
        return MOQR_OK;
    }
    /* Once OPEN has crossed, only the owner's original deadline classifies
     * missing spans. Complete queued coverage is never discarded by time. */
    return MOQR_ERR_WOULD_BLOCK;
}

/* The public vtable still requires a cache reader. Typed gaps/errors cannot
 * be represented by it; the built-in source always installs the adjunct. */
static moqr_result_t
shards_fill_peek(void *ctx, uint64_t handle, uint64_t now_us,
                 moqr_fetch_item_t *out)
{
    if (out == NULL) return MOQR_ERR_INVAL;
    moqr_retrieve_item_t it;
    moqr_result_t rc = shards_fill_retrieve_peek(ctx, handle, now_us, &it);
    if (rc != MOQR_OK) return rc;
    memset(out, 0, sizeof(*out));
    if (it.kind == MOQR_RETRIEVE_OBJECT) {
        out->kind = MOQR_FETCH_ITEM_OBJECT;
        out->rec = it.object;
    } else if (it.kind == MOQR_RETRIEVE_END) {
        out->kind = MOQR_FETCH_ITEM_END;
    } else {
        return MOQR_ERR_UNSUPPORTED;
    }
    return MOQR_OK;
}

static moqr_result_t
shards_fill_commit(void *ctx, uint64_t handle)
{
    r_mgr *m = ctx;
    r_fret_t *f = (m != NULL) ? fret_find(m, handle) : NULL;
    if (f == NULL || f->closed) {
        return MOQR_ERR_STALE_HANDLE;
    }
    if (f->count > 0) {
        r_fitem_t *it = &f->win[f->head];
        if (it->result.kind == MOQR_RETRIEVE_ERROR) return MOQR_ERR_WRONG_STATE;
        if (r_loc_cmp(it->result.end.group, it->result.end.object,
                      f->end_group, f->end_object) < 0) {
            f->next_group = it->result.end.group +
                (it->result.end.object == UINT64_MAX ? 1u : 0u);
            f->next_object = it->result.end.object == UINT64_MAX
                                 ? 0 : it->result.end.object + 1u;
        }
        moq_rcbuf_decref(it->payload);
        moq_rcbuf_decref(it->props);
        f->release_entries++;
        f->release_bytes += it->charged;
        memset(it, 0, sizeof(*it));
        f->head = (f->head + 1u) % R_FILL_WINDOW;
        f->count--;
        if (f->count == 0 && f->granted == 0 && !f->ended &&
            !f->abandoned) {
            f->more_owed = true;
        }
        return MOQR_OK;
    }
    if (f->synth_peeked) {
        f->exhausted = true;
        return MOQR_OK;
    }
    return MOQR_ERR_WRONG_STATE;
}

static void
shards_fill_close(void *ctx, uint64_t handle)
{
    r_mgr *m = ctx;
    r_fret_t *f = (m != NULL) ? fret_find(m, handle) : NULL;
    if (f == NULL || f->closed) {
        return;
    }
    fret_drop_window(f);
    moq_rcbuf_decref(f->meta_props);
    f->meta_props = NULL;
    f->plan.meta_bytes = (moq_bytes_t){ NULL, 0 };
    if (f->req_owed) {
        f->req_owed = false;
    } else if (!f->ended && !f->abandoned) {
        f->cancel_owed = true;
    }
    f->more_owed = false;
    f->closed = true;
}

/* -- fill retrieval: owner ---------------------------------------------------- */

/* The requester times only unopened claims. Typed GAPs are already classified
 * and must not wait again. The owner contributes cursor no-origin waits;
 * admitted collector/cancellation timers remain in its bind at zero credit. */
static bool
fret_needs_deadline(const r_fret_t *f)
{
    /* A standalone FETCH waits for its admission, not for time: the owner
     * answers its REQ at once and times the retrieval itself. */
    return f->used && !f->standalone && !f->closed && !f->abandoned &&
           !f->exhausted && !f->ended && !f->begun && f->count == 0;
}

static uint64_t
shards_fill_next_deadline(void *ctx)
{
    const r_mgr *m = ctx;
    uint64_t next = UINT64_MAX;
    for (uint32_t i = 0; m != NULL && m->fret != NULL && i < m->fill_cap;
         i++) {
        const r_fret_t *f = &m->fret[i];
        if (fret_needs_deadline(f) && f->deadline_us < next) {
            next = f->deadline_us;
        }
    }
    for (uint32_t i = 0; m != NULL && m->oret != NULL && i < m->fill_cap;
         i++) {
        const r_oret_t *o = &m->oret[i];
        uint64_t d = moqr_bind_retrieve_next_deadline(&o->cursor);
        if (o->used && d < next) {
            next = d;
        }
    }
    return next;
}

static r_oret_t *
oret_find(r_mgr *m, uint16_t src, uint64_t id)
{
    for (uint32_t i = 0; m->oret != NULL && i < m->fill_cap; i++) {
        if (m->oret[i].used && m->oret[i].src == src && m->oret[i].id == id) {
            return &m->oret[i];
        }
    }
    return NULL;
}

static void
oret_free(r_mgr *m, r_oret_t *o)
{
    moqr_bind_retrieve_close(m->s->shards[m->shard].bind, &o->cursor);
    if (o->standalone && moqr_fetch_is_valid(o->owner)) {
        (void)moqr_core_fetch_close(m->core, o->owner);
    }
    moq_rcbuf_decref(o->meta_props);
    memset(o, 0, sizeof(*o));
}

/* An admitted demand is closing: its retrievals close with it (the
 * requester resolves them from the demand's own end). */
static void
oret_demand_gone(r_mgr *m, uint16_t src, uint64_t demand_id)
{
    for (uint32_t i = 0; m->oret != NULL && i < m->fill_cap; i++) {
        r_oret_t *o = &m->oret[i];
        if (o->used && o->src == src && o->demand_id == demand_id) {
            oret_free(m, o);
        }
    }
}

/* The admitted demand a retrieval answers for, while it still serves. */
static const r_psub *
owner_fill_psub(const r_mgr *m, uint16_t src, uint64_t demand_id)
{
    for (uint32_t i = 0; i < m->psub_cap; i++) {
        const r_psub *p = &m->psub[i];
        if (p->used && p->src == src && p->demand_id == demand_id &&
            p->ack_sent && p->has_otrack && !p->undemanding &&
            !p->terminating && !p->done_pending) {
            return p;
        }
    }
    return NULL;
}

/* Initialization sends nothing. Only the owner data phase calls this shared
 * cursor; its accepted pseudo-subscription is the authorization authority. */
static moqr_result_t
owner_fill_cursor(r_mgr *m, r_oret_t *o)
{
    if (o->standalone) {
        /* The request's own admission authorizes it, as for a local FETCH:
         * the requester authorized the FETCH before asking. */
        moqr_retrieve_request_t sa = {
            .binding = m->pb[o->src], .standalone = true, .owner = o->owner,
            .track = o->plan.track, .track_gen = o->plan.track_gen,
            .source = o->src, .child = o->id,
            .start = { o->plan.start_group, o->plan.start_object },
            .end = { o->plan.end_group, o->plan.end_object },
            .group_order = (moqr_group_order_t)o->order,
            .subscriber_priority = o->prio, .deadline_us = o->deadline_us,
        };
        return moqr_bind_retrieve_init(m->s->shards[m->shard].bind, &o->cursor,
                                       &sa);
    }
    const r_psub *p = owner_fill_psub(m, o->src, o->demand_id);
    if (p == NULL) return MOQR_ERR_STALE_HANDLE;
    moqr_retrieve_request_t request = {
        .binding = m->pb[o->src], .parent = p->sub,
        .track = p->otrack, .track_gen = p->otrack_gen,
        .source = o->src, .demand = o->demand_id, .child = o->id,
        .start = { o->start_group, o->start_object },
        .end = { o->end_group, o->end_object },
        .group_order = (moqr_group_order_t)o->order,
        .subscriber_priority = o->prio, .deadline_us = o->deadline_us,
    };
    return moqr_bind_retrieve_init(m->s->shards[m->shard].bind, &o->cursor,
                                   &request);
}

/* Detach upstream ownership before parking an allocation-free local terminal.
 * The bind retains any owed cancellation until its actual child settles. */
static void
owner_fill_fail(r_mgr *m, r_oret_t *o, moqr_result_t failure)
{
    moqr_bind_retrieve_close(m->s->shards[m->shard].bind, &o->cursor);
    o->failed = true;
    o->failure = failure;
}

static void
fill_local_error(r_fill_result_t *out, moqr_result_t failure)
{
    memset(out, 0, sizeof(*out));
    out->kind = MOQR_RETRIEVE_ERROR;
    out->error_kind = failure == MOQR_ERR_STALE_HANDLE
                          ? MOQR_RETRIEVE_SOURCE_LOST : MOQR_RETRIEVE_LOCAL;
    out->local_result = failure;
}

/* Drop a demand entry (frees its owned key) with keep-compaction handled by
 * the caller's loop. */
static void
pend_free_entry(r_mgr *m, r_pdemand *d)
{
    fret_demand_gone(m, d->demand_id);
    sh_free(&m->alloc, d->canon, d->canon_len);
    d->canon = NULL;
    if (d->popen_row != UINT32_MAX) {
        psg_row_clear(&m->popen[(size_t)d->popen_row * m->s->sg_slots],
                      m->s->sg_slots);
        m->popen_row_used[d->popen_row] = 0;
        d->popen_row = UINT32_MAX;
    }
}

/* The demand's progress row, claimed lazily on its first open object; NULL
 * when none is free (a demand terminal, never an untracked stream). */
static r_psg_t *
pend_popen_row(r_mgr *m, r_pdemand *d)
{
    if (d->popen_row != UINT32_MAX) {
        return &m->popen[(size_t)d->popen_row * m->s->sg_slots];
    }
    for (uint32_t i = 0; i < m->pend_cap; i++) {
        if (!m->popen_row_used[i]) {
            m->popen_row_used[i] = 1;
            d->popen_row = i;
            return &m->popen[(size_t)i * m->s->sg_slots];
        }
    }
    return NULL;
}

/* The retryable requester-side terminal: abandon every open object this
 * demand still has with reset_code (each commit clears its slot's object
 * fields, so a retry never repeats a completed abandon; discarding objects
 * have no record to abandon), then source_done(done_code). Every DONE-shaped
 * teardown of an ACKed demand — owner DONE, capacity terminal, re-target,
 * idle unsubscribe — funnels through here so no path can strand an orphan
 * OPEN record, WARM or ACTIVE. Returns false while a stage holds
 * (WOULD_BLOCK, or a hard error after latching fail-stop) — the caller
 * keeps the entry and its channel head durable. */
/* The shard's own terminals are meanings, not any draft's numbers: the
 * connection that reads them may speak either draft. */
static moqr_pd_desc_t
shard_local_done(moqr_pd_status_t status)
{
    moqr_pd_desc_t d;

    if (moqr_pd_desc_local(status, &d) != MOQR_OK) {
        return moqr_pd_desc_none();
    }
    return d;
}

static moqr_reset_desc_t
shard_local_reset(moqr_reset_cause_t cause)
{
    moqr_reset_desc_t d;

    if (moqr_reset_desc_local(cause, &d) != MOQR_OK) {
        return moqr_reset_desc_none();
    }
    return d;
}

/* A terminal the owner forwarded from its publisher (a wire status), as
 * opposed to one a relay chose (a local meaning: capacity, withdrawal,
 * re-target). Only the publisher's own end lets local subscribers first
 * receive what this shard already holds. */
static bool
shard_pd_forwarded(moqr_pd_desc_t pd)
{
    return pd.tag == MOQR_CODE_WIRE_D16 || pd.tag == MOQR_CODE_WIRE_D18 ||
           pd.tag == MOQR_CODE_WIRE_D22;
}

/* A track handle identifies retained storage; a source transition can keep
 * that handle and change only track_gen. Every queued response must match
 * the current source generation before touching that storage. */
static bool
pend_source_current(const r_mgr *m, const r_pdemand *d)
{
    uint64_t generation = 0;
    return moqr_core_track_current_generation(m->core, d->track, &generation)
               == MOQR_OK && generation == d->track_gen;
}

static bool
pend_terminal_step(moqr_shards_t *s, r_mgr *m, r_pdemand *d,
                   moqr_pd_desc_t done, moqr_reset_desc_t reset,
                   bool graceful, uint64_t now_us)
{
    if (!pend_source_current(m, d) &&
        !moqr_core_internal_source_idle(m->core, d->track, d->track_gen)) {
        return true; /* never abandon records belonging to a replacement */
    }
    /* The terminal this shard is resolving into its own core, recorded where
     * it is actually consumed rather than where it arrived. */
    m->last_done_pd = done;
    if (d->popen_row != UINT32_MAX) {
        r_psg_t *row = &m->popen[(size_t)d->popen_row * s->sg_slots];
        for (uint32_t sl = 0; sl < s->sg_slots; sl++) {
            if (!row[sl].used || !row[sl].object_open) {
                continue;
            }
            if (!row[sl].discarding) {
                moqr_result_t ar = moqr_core_abandon_record(
                    m->core, d->track, row[sl].group, row[sl].subgroup,
                    row[sl].object_id, reset);
                if (ar == MOQR_ERR_WOULD_BLOCK) {
                    return false;
                }
                if (ar != MOQR_OK && ar != MOQR_ERR_TOO_OLD &&
                    ar != MOQR_ERR_STALE_HANDLE && ar != MOQR_DONE) {
                    /* WRONG_STATE/INVAL: the record survives in a state
                     * this teardown cannot scrub — clearing only the
                     * bookkeeping would strand a partial OPEN record, so
                     * fail-stop and HOLD instead. */
                    m->oom = true;
                    return false;
                }
                /* OK, or moot (evicted/raced): the stage is done. */
            }
            row[sl].object_open = false;
            row[sl].discarding = false;
            row[sl].object_id = 0;
            row[sl].next_chunk = 0;
        }
    }
    /* The owner's own DONE lets each local subscriber receive what this
     * shard already holds first; a terminal this shard chooses ends them at
     * once. */
    moqr_result_t tr = graceful
        ? moqr_core_source_finish(m->core, d->track, d->track_gen, done, now_us)
        : moqr_core_source_done(m->core, d->track, d->track_gen, done, now_us);
    if (tr == MOQR_ERR_WOULD_BLOCK) {
        return false;
    }
    if (tr != MOQR_OK && tr != MOQR_ERR_STALE_HANDLE &&
        tr != MOQR_ERR_WRONG_STATE) {
        m->oom = true;
        return false;
    }
    return true;
}

/* The pending-demand entry for a demand id, or NULL. */
static r_pdemand *
pend_find(r_mgr *m, uint64_t demand_id)
{
    for (uint32_t i = 0; i < m->pend_len; i++) {
        if (m->pend[i].demand_id == demand_id) {
            return &m->pend[i];
        }
    }
    return NULL;
}

static bool
demand_reply_matches(const r_pdemand *d, uint16_t src,
                      const r_demand_msg_t *msg)
{
    return d != NULL && d->origin == src && d->demand_id == msg->demand_id &&
           moqr_track_eq(d->track, msg->track) && d->track_gen == msg->track_gen;
}

/* The message remains the owner of its clone. replace makes an independent
 * requester-budget copy (or publishes UNAVAILABLE on copy refusal) before
 * the ACK/fill behind it can be consumed. Record the REMOTE revision only
 * after that outcome is durable; a held initial ACK must not allocate twice. */
static moqr_result_t
demand_metadata_install(r_mgr *m, r_pdemand *d, const r_demand_msg_t *msg,
                         uint64_t now_us)
{
    moqr_bind_meta_ref_t input;
    memset(&input, 0, sizeof(input));
    input.state = msg->metadata.state;
    input.wire_version = msg->metadata.wire_version;
    input.source_binding = msg->metadata.source_binding;
    input.source_handle = msg->metadata.source_handle;
    input.source_generation = msg->metadata.source_generation;
    input.bytes = (moq_bytes_t){ moq_rcbuf_data(msg->props),
                                moq_rcbuf_len(msg->props) };
    uint64_t revision = 0;
    moqr_result_t rc = moqr_bind_meta_replace(
        m->s->shards[m->shard].bind, d->track, d->track_gen, &input,
        now_us, &revision);
    if (revision != 0) {
        d->metadata_bound = true;
        d->metadata_track = msg->metadata.track;
        d->metadata_track_gen = msg->metadata.track_gen;
        d->metadata_revision = msg->metadata.revision;
        return MOQR_OK;   /* includes the durable CAPACITY/NOMEM invalidation */
    }
    return rc;
}

/* Retire one pending-demand entry in place (swap-remove; order-free). */
static void
pend_retire(r_mgr *m, r_pdemand *d)
{
    pend_free_entry(m, d);
    *d = m->pend[m->pend_len - 1u];
    m->pend_len--;
}

/* Control work this manager has pending toward `dst` — the DATA class's
 * eligibility input for the sticky arbiter (a data push may take a contended
 * slot only when no control is waiting). Durable retry state only, computed
 * outside any channel lock: unsent demands and cancel notices (requester
 * role) and unpushed ACK/latest-metadata/DONE replies (owner role). An inbound
 * request awaiting its refusal is deliberately not counted — its reply retries
 * behind a durable head, so missing it costs control one round, never a
 * message. */
static bool
ctrl_work_pending(const r_mgr *m, uint16_t dst)
{
    for (uint32_t i = 0; i < m->pend_len; i++) {
        if (m->pend[i].origin == dst &&
            (m->pend[i].state == D_ST_RECORDED ||
             m->pend[i].state == D_ST_UNDEMANDING)) {
            return true;
        }
    }
    for (uint32_t i = 0; i < m->psub_cap; i++) {
        const r_psub *p = &m->psub[i];
        if (p->used && p->src == dst &&
            (p->ack_pending || p->done_pending ||
             (p->ack_sent && p->metadata_pending))) {
            return true;
        }
    }
    for (uint32_t i = 0; m->fret != NULL && i < m->fill_cap; i++) {
        const r_fret_t *f = &m->fret[i];
        if (f->used && f->origin == dst &&
            (f->req_owed || f->cancel_owed || f->more_owed)) {
            return true;
        }
    }
    return false;
}

/* A latest-state obligation is a causal prerequisite for data on this pair.
 * Hold the pair until phase 5 queues it: the old DATA eligibility bit must
 * not let a sticky arbiter wait for data that itself waits for metadata. */
static bool
owner_metadata_pending(const r_mgr *m, uint16_t dst)
{
    for (uint32_t i = 0; i < m->psub_cap; i++) {
        const r_psub *p = &m->psub[i];
        if (p->used && p->src == dst && !p->undemanding &&
            p->ack_sent && p->metadata_pending) {
            return true;
        }
    }
    return false;
}

/* Whether this manager's data phase reported unpushed extraction work toward
 * dst — the CTRL class's eligibility input (recomputed every data phase, so
 * at most one step stale). */
static bool
data_work_pending(const r_mgr *m, uint16_t dst)
{
    return ((m->data_pending_mask >> dst) & 1u) != 0 &&
           !owner_metadata_pending(m, dst);
}

/* One bounded clone is staged at a time. Failure becomes a durable scalar
 * UNAVAILABLE obligation even if the channel then refuses it; no allocation
 * retry can turn that failed copy into EMPTY or leave requester A available. */
static void
demand_metadata_message(r_mgr *m, r_psub *p, uint8_t kind,
                         moqr_bind_meta_ref_t *ref, r_demand_msg_t *msg)
{
    memset(msg, 0, sizeof(*msg));
    msg->kind = kind;
    msg->demand_id = p->demand_id;
    msg->track = p->rtrack;
    msg->track_gen = p->rtrack_gen;
    if (ref->state == MOQR_BIND_META_PRESENT &&
        (ref->bytes.len == 0 || ref->bytes.data == NULL ||
         ref->bytes.len > MOQR_BIND_META_SET_MAX ||
         moq_rcbuf_create(&m->alloc, ref->bytes.data, ref->bytes.len,
                          &msg->props) != MOQ_OK)) {
        moqr_bind_meta_ref_t unavailable = *ref;
        unavailable.state = MOQR_BIND_META_UNAVAILABLE;
        unavailable.storage = NULL;
        unavailable.bytes = (moq_bytes_t){ NULL, 0 };
        moqr_bind_meta_release(m->s->shards[m->shard].bind, ref);
        *ref = unavailable;
    }
    msg->metadata.track = p->otrack;
    msg->metadata.track_gen = p->otrack_gen;
    msg->metadata.state = ref->state;
    msg->metadata.wire_version = ref->wire_version;
    msg->metadata.source_binding = ref->source_binding;
    msg->metadata.source_handle = ref->source_handle;
    msg->metadata.source_generation = ref->source_generation;
    msg->metadata.revision = ref->revision;
}

/* Push a DONE reply answering `req` back to its requester. Returns false on
 * a refused push (full channel, or the arbiter holding the slot for data);
 * the caller leaves its durable record in place and retries. */
static bool
demand_reply_done(moqr_shards_t *s, uint16_t shard, uint16_t dst,
                  const r_demand_msg_t *req, bool pre_ack, uint64_t code,
                  uint64_t *pushed_dst_mask)
{
    r_mgr *m = s->shards[shard].mgr;
    r_demand_msg_t reply;
    memset(&reply, 0, sizeof(reply));
    reply.kind = D_MSG_DONE;
    reply.pre_ack = pre_ack;
    reply.error_code = code;
    reply.demand_id = req->demand_id;
    reply.track = req->track;
    reply.track_gen = req->track_gen;
    if (!dch_push_arb(s, shard, dst, &reply, false,
                      data_work_pending(m, dst))) {
        return false;
    }
    if (pushed_dst_mask != NULL) {
        *pushed_dst_mask |= 1ull << dst;
    }
    return true;
}

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

static bool
dp_admit(const dp_budget_t *b, uint64_t lb)
{
    if (b->msgs_left == 0) {
        return false;
    }
    if (b->pushed_any && lb > b->bytes_left) {
        return false;
    }
    return true;
}

static void
dp_spend(dp_budget_t *b, uint64_t lb)
{
    if (!b->pushed_any && lb > b->bytes_left) {
        b->turn_over = true;
    }
    b->pushed_any = true;
    b->msgs_left--;
    b->bytes_left = lb >= b->bytes_left ? 0 : b->bytes_left - lb;
}

/* Free an admitted-demand entry AND its subgroup-progress row (the row is
 * indexed by the psub slot, so a recycled slot must never inherit stale
 * cursors). */
static void
psub_free(r_mgr *m, r_psub *p)
{
    oret_demand_gone(m, p->src, p->demand_id);
    psub_metadata_release(m, p);
    uint32_t idx = (uint32_t)(p - m->psub);
    psg_row_clear(&m->psg[(size_t)idx * m->s->sg_slots], m->s->sg_slots);
    memset(p, 0, sizeof(*p));
}

/* The ONE turn-classification point: every turn-ending path funnels here,
 * so the four outcome counters partition pump_turns by construction.
 * Precedence is pre-registered (never data-dependent): message budget
 * beats byte budget beats blocked beats drained — a one-message budget
 * spent on an oversize-first delivery classifies as MSG_BUDGET even though
 * it also crossed the byte budget. "Blocked" reads data_pending_mask AFTER
 * this turn's updates: budget remained but at least one source still has
 * eligible data behind a channel/byte-cap/arbiter/un-ACKed refusal. */
static void
dp_turn_classify(r_mgr *m, const dp_budget_t *b)
{
    if (b->msgs_left == 0) {
        m->turns_msg_budget++;
    } else if (b->turn_over) {
        m->turns_byte_budget++;
    } else if (m->data_pending_mask != 0) {
        m->turns_blocked++;
    } else {
        m->turns_drained++;
    }
    if (b->pushed_any) {
        m->turns_with_messages++;
    }
}

/* End the data phase after the soft-first exception (or any mid-object turn
 * boundary): the source stays conservatively pending and the turn/rotation
 * bookkeeping happens exactly once. */
static void
dp_turn_end(r_mgr *m, uint16_t src, const dp_budget_t *b)
{
    m->data_pending_mask |= 1ull << src;
    dp_turn_classify(m, b);
    m->pump_turns++;
    m->data_rr = (uint16_t)((m->data_rr + 1u) % m->s->shard_count);
}

/* Confirm a delivery outcome on an owner pseudo-binding. A refused
 * confirmation means the core's outstanding-delivery state is no longer
 * what this phase believes — latch fail-stop rather than treat it as a
 * successful hold, release, or advance. */
static bool
data_done(r_mgr *m, uint16_t src, moqr_delivery_outcome_t oc, uint64_t now_us)
{
    if (moqr_core_delivery_done(m->core, m->pb[src], oc, now_us) != MOQR_OK) {
        m->oom = true;
        return false;
    }
    return true;
}

/* Requester side of the demand phase for fill retrievals: return the channel
 * charge of window items the fill has consumed, push whatever the retrieval
 * owes the owner -- REQ, then CANCEL or a new credit window -- and free a
 * claim the binding has closed once nothing more is owed. A refused push
 * keeps its flag: the flag is the durable record. */
static void
requester_fill_phase(moqr_shards_t *s, uint16_t shard,
                     uint64_t *pushed_dst_mask)
{
    r_mgr *m = s->shards[shard].mgr;
    for (uint32_t i = 0; m->fret != NULL && i < m->fill_cap; i++) {
        r_fret_t *f = &m->fret[i];
        if (!f->used) {
            continue;
        }
        if (f->release_entries > 0) {
            dch_release_held(s, f->origin, shard, f->release_entries,
                             f->release_bytes);
            f->release_entries = 0;
            f->release_bytes = 0;
        }
        r_demand_msg_t msg;
        memset(&msg, 0, sizeof(msg));
        msg.demand_id = f->demand_id;
        msg.fill_id = f->id;
        bool data_waiting = data_work_pending(m, f->origin);
        if (f->req_owed) {
            msg.kind = D_MSG_FILL_REQ;
            msg.group_id = f->start_group;
            msg.object_id = f->start_object;
            msg.fill_end_group = f->end_group;
            msg.fill_end_object = f->end_object;
            msg.fill_deadline_us = f->deadline_us;
            msg.fill_order = f->order;
            msg.prio = f->prio;
            msg.fill_credit = R_FILL_WINDOW;
            if (f->standalone) {
                /* The key moves to the channel with the REQ. */
                msg.fetch_standalone = true;
                msg.cluster_selected = f->cluster_selected;
                msg.source_binding_cookie = f->source_binding_cookie;
                msg.source_route_generation = f->source_route_generation;
                msg.requester_hop_id = f->requester_hop_id;
                msg.requester_correlation_id = f->requester_correlation_id;
                msg.fetch_range_form = f->range_form;
                msg.fetch_relative_start = f->relative_start;
                msg.fetch_join_demand = f->join_demand_id;
                msg.canon = f->canon;
                msg.canon_len = f->canon_len;
            }
            if (!dch_push_arb(s, shard, f->origin, &msg, false,
                              data_waiting)) {
                continue;
            }
            f->canon = NULL;
            f->canon_len = 0;
            f->req_owed = false;
            f->granted = R_FILL_WINDOW;
            if (pushed_dst_mask != NULL) {
                *pushed_dst_mask |= 1ull << f->origin;
            }
        }
        if (f->cancel_owed) {
            msg.kind = D_MSG_FILL_CANCEL;
            if (!dch_push_arb(s, shard, f->origin, &msg, false,
                              data_waiting)) {
                continue;
            }
            f->cancel_owed = false;
            if (pushed_dst_mask != NULL) {
                *pushed_dst_mask |= 1ull << f->origin;
            }
        } else if (f->more_owed) {
            msg.kind = D_MSG_FILL_MORE;
            msg.fill_credit = R_FILL_WINDOW;
            if (!dch_push_arb(s, shard, f->origin, &msg, false,
                              data_waiting)) {
                continue;
            }
            f->granted += msg.fill_credit;
            f->more_owed = false;
            if (pushed_dst_mask != NULL) {
                *pushed_dst_mask |= 1ull << f->origin;
            }
        }
        if (f->closed && !f->req_owed && !f->cancel_owed) {
            sh_free(&m->alloc, f->canon, f->canon_len);
            memset(f, 0, sizeof(*f));
            if (m->fill_starved) {
                m->fill_starved = false;
                m->fill_wake_owed = true;
            }
        }
    }
}

/* Push one owner answer message as DATA under this turn's budget. A refused
 * push releases what the message owned, marks src pending and leaves the
 * cursor where it was, so the same item is re-read next time. */
static bool
owner_fill_push(moqr_shards_t *s, uint16_t shard, uint16_t dst,
                r_demand_msg_t *msg, dp_budget_t *bud,
                uint64_t *pushed_dst_mask, uint64_t *pending)
{
    r_mgr *m = s->shards[shard].mgr;
    uint64_t lb = dmsg_logical_bytes(msg);
    if (!dp_admit(bud, lb) ||
        !dch_push_arb(s, shard, dst, msg, true, ctrl_work_pending(m, dst))) {
        dmsg_release(&s->alloc, msg);
        *pending |= 1ull << dst;
        return false;
    }
    dp_spend(bud, lb);
    m->pump_messages++;
    m->pump_bytes += lb;
    if (pushed_dst_mask != NULL) {
        *pushed_dst_mask |= 1ull << dst;
    }
    return true;
}

/* owner_fill_push with *props (an owned reference, or NULL) moved into the
 * message as its properties: on success the channel owns it and *props is
 * NULL; on a refused push it is handed back to *props, never released. */
static bool
owner_fill_push_props(moqr_shards_t *s, uint16_t shard, uint16_t dst,
                      r_demand_msg_t *msg, moq_rcbuf_t **props, dp_budget_t *bud,
                      uint64_t *pushed_dst_mask, uint64_t *pending)
{
    msg->props = *props;
    *props = NULL;
    r_mgr *m = s->shards[shard].mgr;
    uint64_t lb = dmsg_logical_bytes(msg);
    if (!dp_admit(bud, lb) ||
        !dch_push_arb(s, shard, dst, msg, true, ctrl_work_pending(m, dst))) {
        *props = msg->props;
        msg->props = NULL;
        dmsg_release(&s->alloc, msg);
        *pending |= 1ull << dst;
        return false;
    }
    dp_spend(bud, lb);
    m->pump_messages++;
    m->pump_bytes += lb;
    if (pushed_dst_mask != NULL) {
        *pushed_dst_mask |= 1ull << dst;
    }
    return true;
}

/* Shared coverage runs only in the owner data phase. Peeking at zero credit
 * still observes hard errors/deadlines; successful FIFO transfer is the sole
 * permission to commit. All cursor allocations remain bind-owned. */
static bool
owner_fill_pass(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                dp_budget_t *bud, uint64_t *pushed_dst_mask, bool *attempted,
                uint64_t *pending)
{
    r_mgr *m = s->shards[shard].mgr;
    moqr_bind_t *bind = s->shards[shard].bind;
    for (uint32_t i = 0; m->oret != NULL && i < m->fill_cap; i++) {
        r_oret_t *o = &m->oret[i];
        if (!o->used) continue;
        *attempted = true;
        uint16_t dst = o->src;
        if (owner_metadata_pending(m, dst)) continue;
        for (;;) {
            if (bud->turn_over) {
                *pending |= 1ull << dst;
                return false;
            }
            r_demand_msg_t msg;
            memset(&msg, 0, sizeof(msg));
            msg.demand_id = o->demand_id;
            msg.fill_id = o->id;
            if (o->standalone && o->plan.wire.admit != MOQR_FETCH_ACCEPT) {
                /* Refused here: the admission is the whole answer. */
                msg.kind = D_MSG_FILL_OPEN;
                msg.fetch_standalone = true;
                msg.fetch_admit = MOQR_FETCH_REJECT;
                msg.error_code = o->plan.wire.error_code;
                msg.fetch_capacity = o->capacity;
                if (owner_fill_push(s, shard, dst, &msg, bud,
                                    pushed_dst_mask, pending)) oret_free(m, o);
                break;
            }
            if (!o->cursor.live && !o->failed) {
                moqr_result_t rc = owner_fill_cursor(m, o);
                if (rc != MOQR_OK) owner_fill_fail(m, o, rc);
            }
            if (o->failed) {
                msg.kind = D_MSG_FILL_END;
                fill_local_error(&msg.fill_result, o->failure);
                if (owner_fill_push(s, shard, dst, &msg, bud,
                                     pushed_dst_mask, pending)) oret_free(m, o);
                break;
            }
            if (!o->open_sent) {
                msg.kind = D_MSG_FILL_OPEN;
                if (o->standalone) {
                    msg.fetch_standalone = true;
                    msg.fetch_admit = MOQR_FETCH_ACCEPT;
                    msg.fetch_eot = o->plan.wire.end_of_track;
                    msg.group_id = o->plan.start_group;
                    msg.object_id = o->plan.start_object;
                    msg.fill_end_group = o->plan.end_group;
                    msg.fill_end_object = o->plan.end_object;
                    msg.fetch_end_raw_object = o->plan.wire.end_object;
                    msg.metadata.state = o->meta_state;
                    msg.metadata.wire_version = o->meta_version;
                }
                /* The clone moves into the message whole: once pushed, this
                 * lane never touches it again; refused, it comes back. */
                if (!owner_fill_push_props(s, shard, dst, &msg, &o->meta_props, bud,
                                           pushed_dst_mask, pending)) break;
                o->open_sent = true;
                continue;
            }
            moqr_retrieve_item_t it;
            moqr_result_t rc = moqr_bind_retrieve_peek(bind, &o->cursor,
                                                       now_us, &it);
            if (rc == MOQR_ERR_WOULD_BLOCK) break;
            if (rc != MOQR_OK) {
                owner_fill_fail(m, o, rc);
                continue;
            }
            fill_result_store(&msg.fill_result, &it);
            if (it.kind == MOQR_RETRIEVE_END) {
                msg.kind = D_MSG_FILL_END;
                if (owner_fill_push(s, shard, dst, &msg, bud,
                                     pushed_dst_mask, pending)) oret_free(m, o);
                break;
            }
            if (it.kind != MOQR_RETRIEVE_OBJECT &&
                it.kind != MOQR_RETRIEVE_GAP && it.kind != MOQR_RETRIEVE_ERROR) {
                owner_fill_fail(m, o, MOQR_ERR_INVAL);
                continue;
            }
            msg.kind = D_MSG_FILL_ITEM;
            uint64_t bytes = it.kind == MOQR_RETRIEVE_OBJECT
                ? cap_add(moq_rcbuf_len(it.object.payload),
                           moq_rcbuf_len(it.object.properties))
                : it.kind == MOQR_RETRIEVE_ERROR ? it.reason.len : 0;
            if (bytes > s->dch_byte_cap) {
                owner_fill_fail(m, o, MOQR_ERR_CAPACITY);
                continue;
            }
            if (o->credit == 0) break;
            if (it.kind == MOQR_RETRIEVE_OBJECT) {
                msg.group_id = it.object.group_id;
                msg.subgroup_id = it.object.subgroup_id;
                msg.object_id = it.object.object_id;
                msg.prio = it.object.publisher_priority;
                msg.status = (uint8_t)it.object.status;
                msg.datagram_pref = it.object.datagram_pref;
                msg.end_of_group = it.object.end_of_group;
                msg.declared_len = it.object.declared_len;
                msg.props_codec = it.object.properties_codec;
                if ((it.object.payload != NULL &&
                     moq_rcbuf_clone(&s->alloc, it.object.payload,
                                     &msg.payload) != MOQ_OK) ||
                    (it.object.properties != NULL &&
                     moq_rcbuf_clone(&s->alloc, it.object.properties,
                                     &msg.props) != MOQ_OK)) {
                    dmsg_release(&s->alloc, &msg);
                    owner_fill_fail(m, o, MOQR_ERR_NOMEM);
                    continue;
                }
            } else if (it.kind == MOQR_RETRIEVE_ERROR && it.reason.len != 0) {
                if (it.reason.data == NULL) {
                    owner_fill_fail(m, o, MOQR_ERR_INVAL);
                    continue;
                }
                if (moq_rcbuf_create(&s->alloc, it.reason.data, it.reason.len,
                                     &msg.props) != MOQ_OK) {
                    owner_fill_fail(m, o, MOQR_ERR_NOMEM);
                    continue;
                }
            }
            if (!owner_fill_push(s, shard, dst, &msg, bud,
                                 pushed_dst_mask, pending)) break;
            o->credit--;
            if (it.kind == MOQR_RETRIEVE_ERROR) {
                oret_free(m, o);
                break;
            }
            rc = moqr_bind_retrieve_commit(bind, &o->cursor);
            if (rc != MOQR_OK) owner_fill_fail(m, o, rc);
        }
    }
    return true;
}

/* Phase 5 — the demand phase: probe local liveness, then forward. Per entry:
 * - The PROBE (moqr_core_upstream_cancel): a downstream subscriber closing
 *   while the track is PENDING fires no intent, so this is the only way to
 *   learn a demand died locally. WRONG_STATE = still wanted (or already
 *   active) — proceed; OK/STALE = locally dead — drop a RECORDED entry
 *   silently (nothing was sent) or turn a SENT one into UNDEMANDING;
 *   WOULD_BLOCK = hold the entry untouched this round.
 * - The RE-TARGET check: a SENT/ACKED demand whose namespace mirror now
 *   points at a DIFFERENT shard fails terminally (GOING_AWAY) rather than
 *   silently migrating — the subscriber's retry rides the new mirror. A
 *   mirror that is merely gone is NOT a re-target: the owner still holds the
 *   demand and its answer (or terminal) is guaranteed to arrive.
 * - RECORDED entries push DEMAND toward the recorded owner; a full channel
 *   keeps them RECORDED (the entry is the durable record). On push: SENT.
 * - UNDEMANDING entries push UNDEMAND, then drop; a full channel retries.
 * The owner's answer resolves the requester in the phase-1 inbound drain —
 * refusal is counted THERE, when the round-trip actually completes.
 * After the requester walk, the OWNER walk services the admitted-demand
 * table: pending ACK/DONE replies push here (FIFO order per demand is the
 * channel's), and a cancelled demand runs the staged teardown — stage 1
 * retires the pump-sub exactly once, stage 2 cancels the owner track's
 * upstream attempt if this was its last interest; a WOULD_BLOCK in stage 2
 * holds the entry WITHOUT re-running stage 1 (the retired flag is the
 * stage boundary). */
static void
shard_demand_phase(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                   uint64_t *pushed_dst_mask)
{
    r_mgr *m = s->shards[shard].mgr;
    if (m == NULL) {
        return;
    }
    uint32_t keep = 0;
    for (uint32_t i = 0; i < m->pend_len; i++) {
        r_pdemand *d = &m->pend[i];
        bool drop = false;
        if (!pend_source_current(m, d)) {
            if (d->state == D_ST_RECORDED) {
                drop = true;
            } else if ((d->state == D_ST_ACKED || d->state == D_ST_TERMINATING) &&
                       moqr_core_internal_source_idle(m->core, d->track,
                                                       d->track_gen)) {
                /* An idle release keeps this old log, so its unfinished
                 * records still need ordinary quiet cancellation cleanup. */
                if (d->state == D_ST_ACKED) {
                    d->state = D_ST_TERMINATING;
                    d->term_code = 0;
                    d->term_overrun = false;
                    d->term_quiet = true;
                }
            } else {
                d->state = D_ST_UNDEMANDING;
            }
        }
        if (d->state == D_ST_TERMINATING) {
            /* Commit the local terminal, one retryable stage at a time; the
             * triggering channel head stays durable (and keeps every later
             * message behind it) until this lands. */
            /* A quiet terminal is this relay stopping the stream; otherwise
             * the demand's terminal ends it. Both are relay-chosen causes
             * here — the PUBLISH_DONE status rides `term_code` separately and
             * no longer doubles as the reset number. */
            moqr_reset_desc_t rdesc =
                d->term_quiet ? shard_local_reset(MOQR_RESET_CANCELLED)
                              : shard_local_reset(MOQR_RESET_CANCELLED);
            if (!pend_terminal_step(s, m, d,
                                    shard_local_done(MOQR_PD_TRACK_ENDED),
                                    rdesc, false, now_us)) {
                m->pend[keep++] = *d;
                continue;
            }
            if (d->term_quiet) {
                /* An ordinary local stop, not a loss: no terminal metric. */
            } else if (d->term_overrun) {
                m->remote_demand_term_overrun++;
            } else {
                m->remote_demand_term_capacity++;
            }
            d->state = D_ST_UNDEMANDING;   /* the owner is owed the notice */
        }
        if (d->state == D_ST_RECORDED || d->state == D_ST_SENT) {
            moqr_result_t pr = moqr_core_upstream_cancel(m->core, d->track,
                                                         d->track_gen, now_us);
            if (pr == MOQR_ERR_WOULD_BLOCK) {
                m->pend[keep++] = *d;   /* hold untouched; retry next round */
                continue;
            }
            if (pr == MOQR_OK || pr == MOQR_ERR_STALE_HANDLE) {
                if (d->state == D_ST_RECORDED) {
                    drop = true;        /* never sent: nothing to undo */
                } else {
                    d->state = D_ST_UNDEMANDING;
                }
            } else if (pr != MOQR_ERR_WRONG_STATE) {
                m->oom = true;          /* unexpected; never silently converge */
                m->pend[keep++] = *d;
                continue;
            }
        }
        if (!drop && (d->state == D_ST_SENT || d->state == D_ST_ACKED)) {
            r_jentry *e = jrn_find(m, d->canon, canon2_ns_len(d->canon), NULL);
            if (e != NULL && e->mirror != R_MIRROR_NONE &&
                e->mirror != (int32_t)d->origin) {
                if (d->state == D_ST_ACKED) {
                    /* The full requester terminal: partially open records
                     * from the OLD owner are abandoned before the demand
                     * moves on — stale state must never meet the new
                     * owner's track generation. */
                    if (!pend_terminal_step(s, m, d,
                                            shard_local_done(
                                                MOQR_PD_TRACK_ENDED),
                                            shard_local_reset(
                                                MOQR_RESET_CANCELLED),
                                            false, now_us)) {
                        m->pend[keep++] = *d;   /* retry next round */
                        continue;
                    }
                } else {
                    moqr_result_t rc = moqr_core_upstream_error(
                        m->core, d->track, d->track_gen, R_ERR_INTERNAL,
                        now_us);
                    if (rc == MOQR_ERR_WOULD_BLOCK) {
                        m->pend[keep++] = *d;   /* retry next round */
                        continue;
                    }
                    if (rc != MOQR_OK && rc != MOQR_ERR_STALE_HANDLE &&
                        rc != MOQR_ERR_WRONG_STATE) {
                        m->oom = true;
                        m->pend[keep++] = *d;
                        continue;
                    }
                }
                /* Terminated locally (or already moot); the OLD owner is
                 * still owed the cancel notice. */
                d->state = D_ST_UNDEMANDING;
            }
        }
        if (!drop && d->state == D_ST_RECORDED) {
            r_demand_msg_t msg;
            memset(&msg, 0, sizeof(msg));
            msg.kind = D_MSG_DEMAND;
            msg.demand_id = d->demand_id;
            msg.track = d->track;
            msg.track_gen = d->track_gen;
            msg.source_binding_cookie = d->source_binding_cookie;
            msg.source_route_generation = d->source_route_generation;
            msg.requester_hop_id = d->requester_hop_id;
            msg.requester_correlation_id = d->requester_correlation_id;
            msg.canon = canon_dup(d->canon, d->canon_len, &m->alloc);
            msg.canon_len = d->canon_len;
            if (msg.canon != NULL &&
                dch_push_arb(s, shard, d->origin, &msg, false,
                             data_work_pending(m, d->origin))) {
                d->state = D_ST_SENT;
                if (pushed_dst_mask != NULL) {
                    *pushed_dst_mask |= 1ull << d->origin;
                }
            } else {
                sh_free(&m->alloc, msg.canon, msg.canon_len);
                /* full channel (or dup OOM): stay RECORDED, retry */
            }
        }
        if (!drop && d->state == D_ST_UNDEMANDING) {
            r_demand_msg_t msg;
            memset(&msg, 0, sizeof(msg));
            msg.kind = D_MSG_UNDEMAND;
            msg.demand_id = d->demand_id;
            msg.track = d->track;
            msg.track_gen = d->track_gen;
            if (dch_push_arb(s, shard, d->origin, &msg, false,
                             data_work_pending(m, d->origin))) {
                drop = true;            /* notice delivered to the channel */
                if (pushed_dst_mask != NULL) {
                    *pushed_dst_mask |= 1ull << d->origin;
                }
            }
        }
        if (drop) {
            pend_free_entry(m, d);
        } else {
            m->pend[keep++] = *d;
        }
    }
    m->pend_len = keep;

    requester_fill_phase(s, shard, pushed_dst_mask);

    /* Owner walk: service the admitted-demand table. */
    for (uint32_t i = 0; i < m->psub_cap; i++) {
        r_psub *p = &m->psub[i];
        if (!p->used) {
            continue;
        }
        if (p->undemanding) {
            if (!p->pump_sub_retired) {
                /* Stage 1: retire the pump-sub. OK and STALE both mean it no
                 * longer exists (a terminal may have retired it first);
                 * unsubscribe emits no intent, so it cannot block. */
                (void)moqr_core_unsubscribe(m->core, p->sub, now_us);
                p->pump_sub_retired = true;
            }
            if (p->has_otrack) {
                /* Stage 2: if the pump-sub was the owner track's last
                 * interest and its upstream attempt is still PENDING, cancel
                 * it — otherwise the track would sit in PENDING limbo with
                 * nobody left to resolve it. WRONG_STATE = the track is
                 * alive for other subscribers (or already active); STALE =
                 * its identity already moved on — a terminal resolved it.
                 * Both are moot here BECAUSE stage 1 has run: the captured
                 * generation was current until the pump-sub retired, so a
                 * mismatch can only mean another path resolved the track. */
                moqr_result_t rc = moqr_core_upstream_cancel(
                    m->core, p->otrack, p->otrack_gen, now_us);
                if (rc == MOQR_ERR_WOULD_BLOCK) {
                    continue;   /* hold at stage 2; stage 1 never re-runs */
                }
            }
            psub_free(m, p);
            continue;
        }
        if (p->terminating && !p->term_cancelled) {
            /* The captured-track cancellation stage of the owner-side
             * capacity terminal: WOULD_BLOCK holds HERE without re-running
             * the (already confirmed) STREAM_ERROR release, and the DONE
             * below waits for it. */
            if (p->has_otrack) {
                moqr_result_t rc = moqr_core_upstream_cancel(
                    m->core, p->otrack, p->otrack_gen, now_us);
                if (rc == MOQR_ERR_WOULD_BLOCK) {
                    continue;
                }
            }
            p->term_cancelled = true;
        }
        if (p->ack_pending) {
            r_demand_msg_t msg;
            demand_metadata_message(m, p, D_MSG_ACK, &p->ack_metadata, &msg);
            msg.has_largest = p->has_largest;
            msg.largest_group = p->largest_group;
            msg.largest_object = p->largest_object;
            if (!dch_push_arb(s, shard, p->src, &msg, false,
                              data_work_pending(m, p->src))) {
                dmsg_release(&s->alloc, &msg);
                continue;   /* refused: the flag is the durable record */
            }
            p->ack_pending = false;
            p->ack_sent = true;
            p->metadata_sent_revision = p->ack_metadata.revision;
            moqr_bind_meta_release(s->shards[shard].bind, &p->ack_metadata);
            if (pushed_dst_mask != NULL) {
                *pushed_dst_mask |= 1ull << p->src;
            }
        }
        if (p->ack_sent && p->metadata_pending) {
            if (p->latest_metadata.revision > p->metadata_sent_revision) {
                r_demand_msg_t msg;
                demand_metadata_message(m, p, D_MSG_METADATA,
                                         &p->latest_metadata, &msg);
                if (!dch_push_arb(s, shard, p->src, &msg, false,
                                  data_work_pending(m, p->src))) {
                    dmsg_release(&s->alloc, &msg);
                    continue;
                }
                p->metadata_sent_revision = p->latest_metadata.revision;
                if (pushed_dst_mask != NULL) {
                    *pushed_dst_mask |= 1ull << p->src;
                }
            }
            moqr_bind_meta_release(s->shards[shard].bind, &p->latest_metadata);
            p->metadata_pending = false;
        }
        if (p->done_pending) {
            /* Reached only with no ACK owed (a still-full ACK push moved to
             * the next entry above), and the FIFO channel preserves
             * ACK-before-DONE when both push in this same pass. */
            r_demand_msg_t msg;
            memset(&msg, 0, sizeof(msg));
            msg.kind = D_MSG_DONE;
            msg.pre_ack = p->done_pre_ack;
            msg.error_code = p->done_code;
            msg.pd = p->done_pd;
            msg.demand_id = p->demand_id;
            msg.track = p->rtrack;
            msg.track_gen = p->rtrack_gen;
            if (!dch_push_arb(s, shard, p->src, &msg, false,
                              data_work_pending(m, p->src))) {
                continue;   /* refused: the flag is the durable record */
            }
            if (pushed_dst_mask != NULL) {
                *pushed_dst_mask |= 1ull << p->src;
            }
            /* Terminal delivered: the demand is closed; its progress row is
             * freed only now, after the DONE is durably queued. */
            psub_free(m, p);
        }
    }
}

/* Phase 6 — the data phase: pull admitted deliveries from the per-origin
 * pseudo-bindings and pump each over the same ordered channel that carried
 * its ACK — a whole/status/datagram record as one D_MSG_OBJ, a chunked or
 * live-edge record as OBJ_OPEN / OBJ_CHUNK... / OBJ_END with the per-demand
 * subgroup slot as the resume cursor. Discipline:
 * - Extraction requires ack_sent: the ACK is already durably enqueued in the
 *   SAME FIFO, so an object can never overtake it.
 * - Correlation is {src, demand_id} via the delivery's sub_cookie; a
 *   delivery this table cannot name is a fail-stop, never a guess.
 * - Clone first (independent ownership — never a borrowed pointer or a
 *   shared rcbuf across shards), then enqueue, and only then acknowledge
 *   DELIVERED (or STALLED past a live edge, retaining progress). A refused
 *   push frees the clones and reports WOULD_BLOCK (the delivery re-derives
 *   and the slot cursor resumes without duplication); clone OOM releases
 *   the outstanding delivery with STREAM_ERROR — retiring the pump-sub so
 *   nothing stays pinned — and latches fail-stop.
 * - Shapes still without vocabulary (abandoned-after-begin resets, seal and
 *   eviction notices) are HELD via WOULD_BLOCK — replayed verbatim,
 *   acknowledged by nothing — so nothing is lost or faked; that source lags
 *   until those shapes can cross.
 * - Turn budgets bound each phase: pump_turn_msgs messages and
 *   pump_turn_bytes logical bytes, except that the FIRST message of an
 *   otherwise-empty turn may exceed the byte budget (a legal record is never
 *   permanently unsendable) — spending that exception ENDS the turn
 *   immediately, so nothing (not even a zero-byte status object) rides the
 *   same turn behind it; no debt carries forward. Extraction round-robins
 *   across sources between turns. */
static void
shard_data_phase(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                 uint64_t *pushed_dst_mask)
{
    r_mgr *m = s->shards[shard].mgr;
    if (m == NULL || !s->admit_remote) {
        return;
    }
    dp_budget_t bud = { s->pump_turn_msgs, s->pump_turn_bytes, false,
                        false };
    bool attempted = false;
    uint16_t k = s->shard_count;
    /* Fill-retrieval answers go first: each is bounded by the credit its
     * requester granted (one window per round trip), so they cannot starve
     * live data, while live data behind a busy channel could starve them
     * indefinitely. */
    uint64_t fill_pending = 0;
    bool fill_turn_open = owner_fill_pass(s, shard, now_us, &bud,
                                          pushed_dst_mask, &attempted,
                                          &fill_pending);
    m->data_pending_mask |= fill_pending;
    if (!fill_turn_open) {
        dp_turn_classify(m, &bud);
        m->pump_turns++;
        m->data_rr = (uint16_t)((m->data_rr + 1u) % k);
        return;
    }
    for (uint16_t step = 0; step < k; step++) {
        uint16_t src = (uint16_t)((m->data_rr + step) % k);
        if (src == shard) {
            continue;
        }
        /* Only pull a pseudo-binding that has an admitted, acknowledged
         * demand — pb[src] carries no deliverable work otherwise, and a
         * not-yet-acked demand must wait for its ACK to enqueue first. */
        if (owner_metadata_pending(m, src)) {
            m->data_pending_mask &= ~(1ull << src);
            continue;
        }
        bool live = false, unacked = false;
        for (uint32_t i = 0; i < m->psub_cap; i++) {
            const r_psub *p = &m->psub[i];
            if (!p->used || p->src != src || p->undemanding ||
                p->done_pending) {
                continue;
            }
            if (p->ack_sent) {
                live = true;
            } else {
                unacked = true;
            }
        }
        if (!live) {
            if (!unacked && ((fill_pending >> src) & 1u) == 0) {
                m->data_pending_mask &= ~(1ull << src);
            }
            continue;
        }
        attempted = true;
        bool ctrl_waiting = ctrl_work_pending(m, src);
        bool src_pending = false;
        for (;;) {
            if (bud.msgs_left == 0) {
                src_pending = true;   /* budget spent with work remaining */
                break;
            }
            moqr_delivery_t d;
            moqr_result_t rc =
                moqr_core_next_delivery(m->core, m->pb[src], now_us, &d);
            if (rc == MOQR_DONE) {
                break;   /* drained: nothing pending toward src */
            }
            if (rc != MOQR_OK) {
                m->oom = true;   /* unexpected; never silently converge */
                break;
            }
            r_psub *p = NULL;
            for (uint32_t i = 0; i < m->psub_cap; i++) {
                if (m->psub[i].used && m->psub[i].src == src &&
                    m->psub[i].demand_id == d.sub_cookie) {
                    p = &m->psub[i];
                    break;
                }
            }
            if (p == NULL) {
                /* A delivery this table cannot correlate: fail-stop. The
                 * WOULD_BLOCK keeps the delivery outstanding rather than
                 * asserting anything about it. */
                (void)data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK, now_us);
                m->oom = true;
                return;
            }
            if (!p->ack_sent) {
                if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK, now_us)) {
                    return;
                }
                src_pending = true;
                break;
            }
            if (d.notice != MOQR_DELIVERY_NOTICE_NONE || d.evicted_reset ||
                d.rec.obj_state == MOQR_OBJ_ABANDONED) {
                /* Terminal / notice shapes: one zero-byte message each,
                 * confirmed only after the push is durable — a notice is
                 * never acknowledged (and no owner slot is touched) while
                 * its channel message could still be lost. */
                r_psg_t *row =
                    m->psg != NULL
                        ? &m->psg[(size_t)(p - m->psub) * s->sg_slots]
                        : NULL;
                r_demand_msg_t tm;
                memset(&tm, 0, sizeof(tm));
                tm.demand_id = p->demand_id;
                tm.track = p->rtrack;
                tm.track_gen = p->rtrack_gen;
                moqr_delivery_outcome_t confirm = MOQR_DELIVERY_DELIVERED;
                if (d.notice == MOQR_DELIVERY_NOTICE_SEAL) {
                    tm.kind = D_MSG_SG_SEAL;
                    tm.group_id = d.rec.group_id;
                    tm.subgroup_id = d.rec.subgroup_id;
                    tm.seal_reset = d.seal_reset;
                    tm.reset = d.seal_reset_desc;
                } else if (d.notice == MOQR_DELIVERY_NOTICE_EVICT_WATERMARK) {
                    /* group_id carries the watermark: the oldest RETAINED
                     * group (everything below is gone). */
                    tm.kind = D_MSG_GRP_EVICT;
                    tm.group_id = d.oldest_group;
                } else if (d.evicted_reset) {
                    tm.kind = D_MSG_GRP_RESET;
                    tm.group_id = d.rec.group_id;
                    if (moqr_reset_desc_internal(
                            MOQR_RESET_CODE_EVICTED, &tm.reset) !=
                        MOQR_OK) {
                        tm.reset = moqr_reset_desc_none();
                    }
                    confirm = MOQR_DELIVERY_ABANDONED;
                } else {
                    /* Ordinary begun-abandoned object: exact progress
                     * identity required — this pump began it, so its slot
                     * must name it. */
                    r_psg_t *slot =
                        row != NULL ? psg_find(row, s->sg_slots,
                                               d.rec.group_id,
                                               d.rec.subgroup_id)
                                    : NULL;
                    if (slot == NULL || !slot->object_open ||
                        slot->object_id != d.rec.object_id) {
                        (void)data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                        now_us);
                        m->oom = true;   /* desync: never guess */
                        return;
                    }
                    tm.kind = D_MSG_OBJ_RESET;
                    tm.group_id = d.rec.group_id;
                    tm.subgroup_id = d.rec.subgroup_id;
                    tm.object_id = d.rec.object_id;
                    tm.reset = d.rec.reset;
                    confirm = MOQR_DELIVERY_ABANDONED;
                }
                if (!dp_admit(&bud, 0)) {
                    if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                   now_us)) {
                        return;
                    }
                    src_pending = true;
                    break;
                }
                if (!dch_push_arb(s, shard, src, &tm, true, ctrl_waiting)) {
                    if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                   now_us)) {
                        return;
                    }
                    src_pending = true;
                    break;
                }
                dp_spend(&bud, 0);
                m->pump_messages++;
                if (pushed_dst_mask != NULL) {
                    *pushed_dst_mask |= 1ull << src;
                }
                if (!data_done(m, src, confirm, now_us)) {
                    return;   /* the message is durable; the core faulted */
                }
                /* Progress-slot cleanup, only after the acknowledged
                 * confirmation above. */
                if (row != NULL) {
                    if (tm.kind == D_MSG_SG_SEAL) {
                        r_psg_t *slot = psg_find(row, s->sg_slots,
                                                 tm.group_id,
                                                 tm.subgroup_id);
                        if (slot != NULL) {
                            if (slot->object_open) {
                                /* A seal surfaces only after this sub
                                 * consumed the list — an open object here
                                 * is a core/pump desync. */
                                m->oom = true;
                                return;
                            }
                            memset(slot, 0, sizeof(*slot));
                        }
                        /* Absence is valid: whole-object subgroups never
                         * claim a slot. */
                    } else if (tm.kind == D_MSG_GRP_EVICT) {
                        for (uint32_t sl = 0; sl < s->sg_slots; sl++) {
                            if (row[sl].used &&
                                row[sl].group < tm.group_id) {
                                memset(&row[sl], 0, sizeof(row[sl]));
                            }
                        }
                    } else if (tm.kind == D_MSG_GRP_RESET) {
                        for (uint32_t sl = 0; sl < s->sg_slots; sl++) {
                            if (row[sl].used &&
                                row[sl].group == tm.group_id) {
                                memset(&row[sl], 0, sizeof(row[sl]));
                            }
                        }
                    } else {   /* OBJ_RESET: reset terminates the subgroup */
                        r_psg_t *slot = psg_find(row, s->sg_slots,
                                                 tm.group_id,
                                                 tm.subgroup_id);
                        if (slot != NULL) {
                            memset(slot, 0, sizeof(*slot));
                        }
                    }
                }
                if (bud.turn_over) {
                    dp_turn_end(m, src, &bud);
                    return;
                }
                ctrl_waiting = ctrl_work_pending(m, src);
                continue;
            }
            if (d.rec.chunk_count > 0 || d.rec.obj_state == MOQR_OBJ_OPEN) {
                /* Chunked / live-edge record: OBJ_OPEN exactly once, each
                 * exposed chunk in order, OBJ_END on completion — the
                 * subgroup-progress slot is the resume cursor across every
                 * hold (channel-full, budgets, STALLED regrowth), so nothing
                 * duplicates and nothing skips. */
                r_psg_t *row =
                    &m->psg[(size_t)(p - m->psub) * s->sg_slots];
                r_psg_t *slot = psg_claim(row, s->sg_slots, d.rec.group_id,
                                          d.rec.subgroup_id);
                if (slot == NULL) {
                    /* Distinct live subgroups beyond the progress table: a
                     * loss-visible owner-side demand terminal. Stage 1
                     * releases the outstanding delivery and retires the
                     * pump-sub in the one confirmed core step; the cancel
                     * and DONE stages retry from the reply walk. */
                    if (!data_done(m, src, MOQR_DELIVERY_STREAM_ERROR,
                                   now_us)) {
                        return;   /* nothing claimed released */
                    }
                    p->pump_sub_retired = true;
                    p->terminating = true;
                    p->done_pending = true;
                    p->done_pre_ack = false;
                    p->done_code = R_ERR_INTERNAL;
                    p->done_pd = shard_local_done(MOQR_PD_INTERNAL_ERROR);
                    m->remote_demand_term_capacity++;
                    src_pending = true;   /* the DONE reply is owed */
                    break;
                }
                if (slot->object_open &&
                    slot->object_id != d.rec.object_id) {
                    /* A new object before the open one ended: desync —
                     * never guess. */
                    (void)data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                    now_us);
                    m->oom = true;
                    return;
                }
                if (!slot->object_open) {
                    uint64_t plb =
                        (uint64_t)moq_rcbuf_len(d.rec.properties);
                    if (!dp_admit(&bud, plb)) {
                        if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                       now_us)) {
                            return;
                        }
                        src_pending = true;
                        break;
                    }
                    r_demand_msg_t om;
                    memset(&om, 0, sizeof(om));
                    om.kind = D_MSG_OBJ_OPEN;
                    om.demand_id = p->demand_id;
                    om.track = p->rtrack;
                    om.track_gen = p->rtrack_gen;
                    om.group_id = d.rec.group_id;
                    om.subgroup_id = d.rec.subgroup_id;
                    om.object_id = d.rec.object_id;
                    om.declared_len = d.rec.declared_len;
                    om.prio = d.rec.publisher_priority;
                    om.end_of_group = d.rec.end_of_group;
                    om.first_object = d.rec.first_object;
                    om.props_codec = d.rec.properties_codec;
                    om.subgroup_props = shard_delivery_subgroup_props(m, src);
                    if (d.rec.properties != NULL &&
                        moq_rcbuf_clone(&s->alloc, d.rec.properties,
                                        &om.props) != MOQ_OK) {
                        dmsg_release(&s->alloc, &om);
                        if (data_done(m, src, MOQR_DELIVERY_STREAM_ERROR,
                                      now_us)) {
                            psub_free(m, p);
                        }
                        m->oom = true;
                        return;
                    }
                    if (!dch_push_arb(s, shard, src, &om, true,
                                      ctrl_waiting)) {
                        dmsg_release(&s->alloc, &om);
                        if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                       now_us)) {
                            return;
                        }
                        src_pending = true;
                        break;
                    }
                    dp_spend(&bud, plb);
                    m->pump_messages++;
                    m->pump_bytes += plb;
                    if (pushed_dst_mask != NULL) {
                        *pushed_dst_mask |= 1ull << src;
                    }
                    slot->object_open = true;
                    slot->object_id = d.rec.object_id;
                    slot->next_chunk = 0;
                    if (bud.turn_over) {
                        if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                       now_us)) {
                            return;
                        }
                        dp_turn_end(m, src, &bud);
                        return;
                    }
                    ctrl_waiting = ctrl_work_pending(m, src);
                }
                bool held = false;
                while (slot->next_chunk < d.rec.chunk_count) {
                    const moq_rcbuf_t *cb = NULL;
                    uint64_t clen = 0;
                    if (moqr_core_delivery_chunk(m->core, m->pb[src],
                                                 slot->next_chunk, &cb,
                                                 &clen) != MOQR_OK) {
                        (void)data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                        now_us);
                        m->oom = true;   /* pinned chunk missing: desync */
                        return;
                    }
                    if (!dp_admit(&bud, clen)) {
                        if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                       now_us)) {
                            return;
                        }
                        src_pending = true;
                        held = true;
                        break;
                    }
                    r_demand_msg_t cm;
                    memset(&cm, 0, sizeof(cm));
                    cm.kind = D_MSG_OBJ_CHUNK;
                    cm.demand_id = p->demand_id;
                    cm.track = p->rtrack;
                    cm.track_gen = p->rtrack_gen;
                    cm.group_id = d.rec.group_id;
                    cm.subgroup_id = d.rec.subgroup_id;
                    cm.object_id = d.rec.object_id;
                    if (moq_rcbuf_clone(&s->alloc, cb, &cm.payload) !=
                        MOQ_OK) {
                        dmsg_release(&s->alloc, &cm);
                        if (data_done(m, src, MOQR_DELIVERY_STREAM_ERROR,
                                      now_us)) {
                            psub_free(m, p);
                        }
                        m->oom = true;
                        return;
                    }
                    if (!dch_push_arb(s, shard, src, &cm, true,
                                      ctrl_waiting)) {
                        dmsg_release(&s->alloc, &cm);
                        if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                       now_us)) {
                            return;
                        }
                        src_pending = true;
                        held = true;
                        break;
                    }
                    dp_spend(&bud, clen);
                    m->pump_messages++;
                    m->pump_bytes += clen;
                    if (pushed_dst_mask != NULL) {
                        *pushed_dst_mask |= 1ull << src;
                    }
                    slot->next_chunk++;
                    if (bud.turn_over) {
                        if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                       now_us)) {
                            return;
                        }
                        dp_turn_end(m, src, &bud);
                        return;
                    }
                    ctrl_waiting = ctrl_work_pending(m, src);
                }
                if (held) {
                    break;
                }
                if (d.rec.obj_state == MOQR_OBJ_COMPLETE) {
                    if (!dp_admit(&bud, 0)) {
                        if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                       now_us)) {
                            return;
                        }
                        src_pending = true;
                        break;
                    }
                    r_demand_msg_t em;
                    memset(&em, 0, sizeof(em));
                    em.kind = D_MSG_OBJ_END;
                    em.demand_id = p->demand_id;
                    em.track = p->rtrack;
                    em.track_gen = p->rtrack_gen;
                    em.group_id = d.rec.group_id;
                    em.subgroup_id = d.rec.subgroup_id;
                    em.object_id = d.rec.object_id;
                    em.end_of_group = d.rec.end_of_group;
                    em.adv_subgroup_end = d.subgroup_end;
                    if (!dch_push_arb(s, shard, src, &em, true,
                                      ctrl_waiting)) {
                        if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK,
                                       now_us)) {
                            return;
                        }
                        src_pending = true;
                        break;
                    }
                    dp_spend(&bud, 0);
                    m->pump_messages++;
                    if (pushed_dst_mask != NULL) {
                        *pushed_dst_mask |= 1ull << src;
                    }
                    if (!data_done(m, src, MOQR_DELIVERY_DELIVERED,
                                   now_us)) {
                        return;
                    }
                    /* Only the OBJECT fields reset: the subgroup slot lives
                     * on for the next object of this (group, subgroup). */
                    slot->object_open = false;
                    slot->object_id = 0;
                    slot->next_chunk = 0;
                    if (bud.turn_over) {
                        dp_turn_end(m, src, &bud);
                        return;
                    }
                } else {
                    /* Live edge drained: release fairly WITHOUT advancing —
                     * the progress slot resumes at next_chunk when the
                     * record grows or completes. Nothing is pushable toward
                     * this record until then, so no pending bit. */
                    if (!data_done(m, src, MOQR_DELIVERY_STALLED, now_us)) {
                        return;
                    }
                }
                ctrl_waiting = ctrl_work_pending(m, src);
                continue;
            }
            uint64_t lb = (uint64_t)moq_rcbuf_len(d.rec.payload) +
                          (uint64_t)moq_rcbuf_len(d.rec.properties);
            if (!dp_admit(&bud, lb)) {
                /* Budget spent; only an otherwise-empty turn's FIRST
                 * message may exceed the byte budget. */
                if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK, now_us)) {
                    return;
                }
                src_pending = true;
                break;
            }
            r_demand_msg_t msg;
            memset(&msg, 0, sizeof(msg));
            msg.kind = D_MSG_OBJ;
            msg.demand_id = p->demand_id;
            msg.track = p->rtrack;
            msg.track_gen = p->rtrack_gen;
            msg.group_id = d.rec.group_id;
            msg.subgroup_id = d.rec.subgroup_id;
            msg.object_id = d.rec.object_id;
            msg.prio = d.rec.publisher_priority;
            msg.status = (uint8_t)d.rec.status;
            msg.datagram_pref = d.rec.datagram_pref;
            msg.end_of_group = d.rec.end_of_group;
            msg.first_object = d.rec.first_object;
            msg.adv_subgroup_end = d.subgroup_end;
            msg.props_codec = d.rec.properties_codec;
            msg.subgroup_props = shard_delivery_subgroup_props(m, src);
            bool oom = false;
            if (d.rec.payload != NULL &&
                moq_rcbuf_clone(&s->alloc, d.rec.payload, &msg.payload) !=
                    MOQ_OK) {
                oom = true;
            }
            if (!oom && d.rec.properties != NULL &&
                moq_rcbuf_clone(&s->alloc, d.rec.properties, &msg.props) !=
                    MOQ_OK) {
                oom = true;
            }
            if (oom) {
                /* Allocation failure is NOT backpressure: release the
                 * outstanding delivery and its pins by retiring the pump-sub
                 * (the one core-sanctioned combined step), then fail-stop —
                 * the shard enters the failed state with nothing pinned. The
                 * entry is claimed released ONLY when the core confirmed it. */
                dmsg_release(&s->alloc, &msg);
                if (data_done(m, src, MOQR_DELIVERY_STREAM_ERROR, now_us)) {
                    psub_free(m, p);
                }
                m->oom = true;
                return;
            }
            if (!dch_push_arb(s, shard, src, &msg, true, ctrl_waiting)) {
                dmsg_release(&s->alloc, &msg);
                if (!data_done(m, src, MOQR_DELIVERY_WOULD_BLOCK, now_us)) {
                    return;
                }
                src_pending = true;
                break;
            }
            if (!data_done(m, src, MOQR_DELIVERY_DELIVERED, now_us)) {
                return;
            }
            dp_spend(&bud, lb);
            m->pump_messages++;
            m->pump_bytes += lb;
            if (pushed_dst_mask != NULL) {
                *pushed_dst_mask |= 1ull << src;
            }
            if (bud.turn_over) {
                /* The soft-first exception is spent: the turn ends NOW —
                 * nothing, not even a zero-byte status object, rides the
                 * same turn behind it. Work may remain here, so stay
                 * conservatively pending for the next turn. */
                dp_turn_end(m, src, &bud);
                return;
            }
            ctrl_waiting = ctrl_work_pending(m, src);
        }
        if (src_pending || ((fill_pending >> src) & 1u) != 0) {
            m->data_pending_mask |= 1ull << src;
        } else {
            m->data_pending_mask &= ~(1ull << src);
        }
        if (bud.msgs_left == 0) {
            break;
        }
    }
    if (attempted) {
        dp_turn_classify(m, &bud);
        m->pump_turns++;
        m->data_rr = (uint16_t)((m->data_rr + 1u) % k);
    }
}

/* Owner role: admit one DEMAND as an ordinary subscribe on the requester's
 * pseudo-binding, recording the pump-sub in the admitted-demand table so the
 * router can correlate its lifecycle intents back to the requester. Returns
 * false when the DEMAND must stay at the channel head (a full reply channel
 * or a transiently-full core); true once its outcome is durable (a pump-sub
 * entry exists, or a refusing DONE reached the reply channel). */
static bool
owner_admit_demand(moqr_shards_t *s, uint16_t shard, uint16_t src,
                   const r_demand_msg_t *msg, uint64_t *pushed_dst_mask)
{
    r_mgr *m = s->shards[shard].mgr;
    r_psub *p = NULL;
    for (uint32_t i = 0; i < m->psub_cap; i++) {
        if (!m->psub[i].used) {
            p = &m->psub[i];
            break;
        }
    }
    if (p == NULL) {
        /* Pump-sub table full: a capacity refusal, which the requester's
         * binding encodes per draft with a Retry Interval. The durable
         * outcome is the refusing DONE itself. */
        return demand_reply_done(s, shard, src, msg, true,
                                 MOQR_REQUEST_ERROR_CAPACITY, pushed_dst_mask);
    }
    moq_bytes_t parts[R_NS_MAX_PARTS];
    moq_bytes_t name;
    moqr_ns_t ns = canon2_decode(msg->canon, parts, &name);
    moqr_subscribe_req_t req;
    moqr_subscribe_req_init(&req);
    req.ns = ns;
    req.name = name;
    req.cookie = msg->demand_id;
    moqr_sub_t sub;
    memset(&sub, 0, sizeof(sub));
    moqr_result_t rc = s->cluster_enabled
        ? moqr_core_subscribe_cluster(m->core, m->pb[src], &req,
            msg->source_binding_cookie, msg->source_route_generation,
            msg->requester_hop_id, msg->requester_correlation_id, &sub)
        : moqr_core_subscribe(m->core, m->pb[src], &req, &sub);
    if (rc == MOQR_ERR_WOULD_BLOCK) {
        return false;   /* intent ring full: DEMAND stays durable */
    }
    if (rc != MOQR_OK) {
        return demand_reply_done(s, shard, src, msg, true,
                                 rc == MOQR_ERR_CAPACITY
                                     ? MOQR_REQUEST_ERROR_CAPACITY
                                     : R_ERR_INTERNAL,
                                 pushed_dst_mask);
    }
    memset(p, 0, sizeof(*p));
    p->used = true;
    p->src = src;
    p->demand_id = msg->demand_id;
    p->rtrack = msg->track;
    p->rtrack_gen = msg->track_gen;
    p->sub = sub;
    /* Capture the owner track NOW, while the pump-sub is live — the staged
     * teardown's stage 2 needs it after stage 1 has retired the sub. A
     * subscribe that rejected synchronously (no publisher) creates no sub:
     * the REJECT_SUB intent is already queued with cookie = demand_id and
     * the router turns it into the DONE reply. */
    p->has_otrack = (moqr_core_sub_track(m->core, sub, &p->otrack,
                                         &p->otrack_gen) == MOQR_OK);
    if (!p->has_otrack) {
        p->pump_sub_retired = true;   /* the sub never lived */
    } else {
        /* Admission may join an already ACTIVE/WARM track after its last
         * observer notification. This seeds only latest, never ACK A. */
        if (moqr_bind_meta_latest(s->shards[shard].bind, p->otrack,
                                  &p->latest_metadata) == MOQR_OK) {
            p->metadata_pending = p->latest_metadata.revision != 0;
        }
    }
    return true;
}

/* Owner role: take on one retrieval. Missing admission or a full owner table
 * produces a child-local ERROR, never successful END or a fabricated gap.
 * Returns false when that answer cannot be pushed yet (the REQ stays at the
 * head). */
/* Owner role: admit a standalone FETCH another shard routed here, exactly as
 * a FETCH this shard received would be admitted (the requester's pseudo-
 * binding stands for its connection). The answer, OPEN with the admission,
 * is pushed by the data phase. A Joining FETCH is answered only from the
 * track its feeding demand's pump-sub reads: once that demand no longer
 * serves, or the name resolves to a replacement, the join is refused, never
 * served the replacement's Objects. */
static bool
owner_sa_accept(moqr_shards_t *s, uint16_t shard, uint16_t src,
                const r_demand_msg_t *msg, r_oret_t *o, uint64_t now_us)
{
    r_mgr *m = s->shards[shard].mgr;
    moq_bytes_t parts[R_NS_MAX_PARTS], name;
    moqr_fetch_req_t req;
    moqr_fetch_req_init(&req);
    req.ns = canon2_decode(msg->canon, parts, &name);
    req.name = name;
    req.start_group = msg->group_id;
    req.start_object = msg->object_id;
    req.end_group = msg->fill_end_group;
    req.end_object = msg->fill_end_object;
    req.group_order = (moqr_group_order_t)msg->fill_order;
    req.subscriber_priority = msg->prio;
    req.cookie = msg->fill_id;
    req.report_gaps = true;
    req.report_unknown = true;
    req.report_pending = true;
    req.range_form = msg->fetch_range_form;
    req.relative_start = msg->fetch_relative_start;
    memset(o, 0, sizeof(*o));
    o->used = true;
    o->standalone = true;
    o->src = src;
    o->id = msg->fill_id;
    o->deadline_us = msg->fill_deadline_us;
    o->order = msg->fill_order;
    o->prio = msg->prio;
    o->credit = msg->fill_credit;
    const r_psub *joined = msg->fetch_join_demand != 0
                               ? owner_fill_psub(m, src, msg->fetch_join_demand)
                               : NULL;
    if (msg->fetch_join_demand != 0 && joined == NULL) {
        o->owner = MOQR_FETCH_INVALID;
        o->plan.wire.admit = MOQR_FETCH_REJECT;
        o->plan.wire.error_code = R_ERR_INVALID_JOINING;
        return true;
    }
    moqr_result_t rc = s->cluster_enabled && msg->cluster_selected
        ? moqr_core_internal_retrieve_open_cluster(m->core, m->pb[src], &req,
            msg->source_binding_cookie, msg->source_route_generation,
            msg->requester_hop_id, msg->requester_correlation_id,
            now_us, &o->owner, &o->plan)
        : moqr_core_internal_retrieve_open(m->core, m->pb[src], &req,
                                           now_us, &o->owner, &o->plan);
    if (rc == MOQR_OK && joined != NULL && o->plan.wire.admit == MOQR_FETCH_ACCEPT &&
        !moqr_track_eq(o->plan.track, joined->otrack)) {
        (void)moqr_core_fetch_close(m->core, o->owner);
        memset(&o->plan, 0, sizeof(o->plan));
        o->owner = MOQR_FETCH_INVALID;
        o->plan.wire.admit = MOQR_FETCH_REJECT;
        o->plan.wire.error_code = R_ERR_INVALID_JOINING;
    } else if (rc != MOQR_OK) {
        memset(&o->plan, 0, sizeof(o->plan));
        o->owner = MOQR_FETCH_INVALID;
        o->plan.wire.admit = MOQR_FETCH_REJECT;
        o->plan.wire.error_code = R_ERR_INTERNAL;
        o->capacity = rc == MOQR_ERR_CAPACITY;   /* the owner's FETCH pool */
    } else if (o->plan.wire.admit == MOQR_FETCH_ACCEPT) {
        /* The answer's Track Properties are fixed here, at admission. A copy
         * that cannot be made is UNAVAILABLE: the requester refuses rather
         * than answer without them. */
        moqr_bind_meta_ref_t pin;
        moqr_bind_t *bind = s->shards[shard].bind;
        (void)moqr_bind_meta_reply(bind, o->plan.track, &pin);
        o->meta_state = pin.state;
        o->meta_version = pin.wire_version;
        if (pin.state == MOQR_BIND_META_PRESENT &&
            (pin.bytes.len == 0 || pin.bytes.data == NULL ||
             pin.bytes.len > MOQR_BIND_META_SET_MAX ||
             moq_rcbuf_create(&m->alloc, pin.bytes.data, pin.bytes.len,
                              &o->meta_props) != MOQ_OK)) {
            o->meta_state = MOQR_BIND_META_UNAVAILABLE;
            o->meta_props = NULL;
        }
        moqr_bind_meta_release(bind, &pin);
        o->start_group = o->plan.start_group;
        o->start_object = o->plan.start_object;
        o->end_group = o->plan.end_group;
        o->end_object = o->plan.end_object;
    } else if (o->plan.remote_cookie != 0) {
        /* Not this shard's either: never routed further. */
        o->plan.wire.error_code = R_ERR_INTERNAL;
    }
    return true;
}

static bool
owner_fill_accept(moqr_shards_t *s, uint16_t shard, uint16_t src,
                  const r_demand_msg_t *msg, uint64_t *pushed_dst_mask,
                  uint64_t now_us)
{
    r_mgr *m = s->shards[shard].mgr;
    r_oret_t *o = NULL;
    if (m->oret != NULL && msg->fetch_standalone && msg->demand_id == 0 &&
        msg->canon != NULL) {
        for (uint32_t i = 0; i < m->fill_cap; i++) {
            if (!m->oret[i].used) {
                o = &m->oret[i];
                break;
            }
        }
        if (o != NULL) {
            return owner_sa_accept(s, shard, src, msg, o, now_us);
        }
    } else if (m->oret != NULL && !msg->fetch_standalone &&
        owner_fill_psub(m, src, msg->demand_id) != NULL) {
        for (uint32_t i = 0; i < m->fill_cap; i++) {
            if (!m->oret[i].used) {
                o = &m->oret[i];
                break;
            }
        }
    }
    if (o == NULL) {
        if (owner_metadata_pending(m, src)) {
            return false;   /* even a refusal follows latest metadata */
        }
        r_demand_msg_t reply;
        memset(&reply, 0, sizeof(reply));
        reply.kind = D_MSG_FILL_END;
        reply.demand_id = msg->demand_id;
        reply.fill_id = msg->fill_id;
        fill_local_error(&reply.fill_result,
            !msg->fetch_standalone &&
                    owner_fill_psub(m, src, msg->demand_id) == NULL
                ? MOQR_ERR_STALE_HANDLE : MOQR_ERR_CAPACITY);
        if (!dch_push_arb(s, shard, src, &reply, true,
                          ctrl_work_pending(m, src))) {
            return false;
        }
        m->pump_messages++;
        if (pushed_dst_mask != NULL) {
            *pushed_dst_mask |= 1ull << src;
        }
        return true;
    }
    memset(o, 0, sizeof(*o));
    o->used = true;
    o->src = src;
    o->id = msg->fill_id;
    o->demand_id = msg->demand_id;
    o->start_group = msg->group_id;
    o->start_object = msg->object_id;
    o->end_group = msg->fill_end_group;
    o->end_object = msg->fill_end_object;
    o->deadline_us = msg->fill_deadline_us;
    o->order = msg->fill_order;
    o->prio = msg->prio;
    o->credit = msg->fill_credit;
    return true;
}

/* Inbound demand drain (joins phase 1): each source channel's head is
 * processed until it can't be — pop-only-when-durable throughout.
 * - DEMAND (owner role): with admission off, answer DONE(NOT_SUPPORTED,
 *   pre_ack); with admission on, admit via owner_admit_demand. Either way
 *   the message pops only once the outcome is durable (the reply pushed, or
 *   the pump-sub entry recorded).
 * - UNDEMAND: mark the admitted demand for the staged teardown (the entry is
 *   the durable record); with no matching entry it is moot (never admitted,
 *   or already terminally resolved).
 * - ACK (requester role): first re-probe local liveness — an ACK for a
 *   demand whose last subscriber already left must not activate a zombie —
 *   then install its exact metadata and resolve via upstream_ok with
 *   upstream_cookie = demand_id (the correlation key the ACTIVE linger path
 *   later echoes back).
 * - METADATA: apply a newer, source-qualified complete set or UNAVAILABLE
 *   before the fill/data behind it, without generating another wire ACK.
 * - DONE (requester role): resolve by the entry's own state — pre-ACK
 *   through upstream_error, post-ACK through source_done — with the OWNER's
 *   code verbatim; OK records the resolution (and counts a pre-ACK refusal);
 *   WOULD_BLOCK leaves the DONE at the head; STALE/WRONG_STATE is moot (the
 *   probe, the linger path, or a re-target resolved the track first). */
static void
shard_demand_inbound(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                     uint64_t *pushed_dst_mask)
{
    r_mgr *m = s->shards[shard].mgr;
    if (m == NULL) {
        return;
    }
    for (uint16_t src = 0; src < s->shard_count; src++) {
        if (src == shard) {
            continue;
        }
        r_demand_msg_t *msg;
        while ((msg = dch_head(s, src, shard)) != NULL) {
            if ((msg->kind >= D_MSG_DONE && msg->kind <= D_MSG_SG_SEAL) ||
                msg->kind == D_MSG_METADATA) {
                r_pdemand *d = pend_find(m, msg->demand_id);
                if (!demand_reply_matches(d, src, msg)) {
                    dch_pop(s, src, shard);
                    continue; /* a reply for another retained/source identity */
                }
                if (!pend_source_current(m, d)) {
                    if ((d->state == D_ST_ACKED || d->state == D_ST_TERMINATING) &&
                        moqr_core_internal_source_idle(m->core, d->track,
                                                        d->track_gen)) {
                        if (d->state == D_ST_ACKED) {
                            d->state = D_ST_TERMINATING;
                            d->term_code = 0;
                            d->term_overrun = false;
                            d->term_quiet = true;
                        }
                        break; /* quiet old-log cleanup precedes later replies */
                    }
                    /* Source changes run before this FIFO drain. Old queued
                     * objects, chunks, resets, metadata and terminals must
                     * neither write nor scrub the replacement's log. Keep
                     * the old owner's cancel obligation, without a local
                     * source terminal that could end the replacement. */
                    d->state = D_ST_UNDEMANDING;
                    if (msg->kind == D_MSG_DONE) pend_retire(m, d);
                    dch_pop(s, src, shard);
                    continue;
                }
            }
            if (msg->kind == D_MSG_DEMAND) {
                bool durable =
                    s->admit_remote
                        ? owner_admit_demand(s, shard, src, msg,
                                             pushed_dst_mask)
                        : demand_reply_done(s, shard, src, msg, true,
                                            R_ERR_NOT_SUPPORTED,
                                            pushed_dst_mask);
                if (!durable) {
                    break;   /* DEMAND stays durable at the head */
                }
                dch_pop(s, src, shard);
            } else if (msg->kind == D_MSG_UNDEMAND) {
                /* Entry key is {src, demand_id}: ids from different
                 * requesters collide, the channel they arrive on does not. */
                for (uint32_t i = 0; i < m->psub_cap; i++) {
                    if (m->psub[i].used && m->psub[i].src == src &&
                        m->psub[i].demand_id == msg->demand_id &&
                        moqr_track_eq(m->psub[i].rtrack, msg->track) &&
                        m->psub[i].rtrack_gen == msg->track_gen) {
                        m->psub[i].undemanding = true;
                        break;
                    }
                }
                dch_pop(s, src, shard);   /* entry (or moot) is durable */
            } else if (msg->kind == D_MSG_ACK) {
                r_pdemand *d = pend_find(m, msg->demand_id);
                if (!demand_reply_matches(d, src, msg) ||
                    d->state != D_ST_SENT) {
                    dch_pop(s, src, shard);   /* cancel crossed in flight */
                    continue;
                }
                moqr_result_t pr = moqr_core_upstream_cancel(
                    m->core, d->track, d->track_gen, now_us);
                if (pr == MOQR_ERR_WOULD_BLOCK) {
                    break;   /* ACK stays durable at the head */
                }
                if (pr == MOQR_OK || pr == MOQR_ERR_STALE_HANDLE) {
                    /* Locally dead (or identity moved on): the owner now
                     * holds a live pump-sub that must be torn down. */
                    d->state = D_ST_UNDEMANDING;
                    dch_pop(s, src, shard);
                    continue;
                }
                if (pr != MOQR_ERR_WRONG_STATE) {
                    m->oom = true;   /* unexpected; never silently converge */
                    dch_pop(s, src, shard);
                    continue;
                }
                if (!d->metadata_bound) {
                    moqr_result_t mr = demand_metadata_install(m, d, msg,
                                                               now_us);
                    if (mr != MOQR_OK) {
                        /* A stale local attempt must not install on its
                         * replacement. Other non-publishing failures become
                         * a request-local refusal, never metadata fail-stop. */
                        if (mr != MOQR_ERR_STALE_HANDLE &&
                            mr != MOQR_ERR_WRONG_STATE &&
                            moqr_core_upstream_error(
                                m->core, d->track, d->track_gen,
                                R_ERR_INTERNAL, now_us) == MOQR_ERR_WOULD_BLOCK) {
                            break;
                        }
                        d->state = D_ST_UNDEMANDING;
                        dch_pop(s, src, shard);
                        continue;
                    }
                }
                moqr_result_t rc = moqr_core_upstream_ok(
                    m->core, d->track, d->track_gen, msg->demand_id,
                    msg->has_largest, msg->largest_group,
                    msg->largest_object);
                if (rc == MOQR_ERR_WOULD_BLOCK) {
                    break;   /* ACK stays durable at the head */
                }
                if (rc == MOQR_OK) {
                    d->state = D_ST_ACKED;
                    d->ack_has_largest = msg->has_largest;
                    d->ack_largest_group = msg->largest_group;
                    d->ack_largest_object = msg->largest_object;
                } else {
                    m->oom = true;
                    d->state = D_ST_UNDEMANDING;
                }
                dch_pop(s, src, shard);
            } else if (msg->kind == D_MSG_METADATA) {
                r_pdemand *d = pend_find(m, msg->demand_id);
                if (!demand_reply_matches(d, src, msg) ||
                    d->state != D_ST_ACKED || !d->metadata_bound ||
                    !moqr_track_eq(d->metadata_track, msg->metadata.track) ||
                    d->metadata_track_gen != msg->metadata.track_gen ||
                    msg->metadata.revision <= d->metadata_revision) {
                    dch_pop(s, src, shard);   /* old/moot demand or revision */
                    continue;
                }
                moqr_result_t mr = demand_metadata_install(m, d, msg, now_us);
                if (mr != MOQR_OK && d->state == D_ST_ACKED) {
                    /* A moved identity still retires partial OPEN records.
                     * replace's drain may already have staged a terminal;
                     * never overwrite that outcome. Copy refusal already
                     * published UNAVAILABLE and returns OK above. */
                    bool moot = mr == MOQR_ERR_STALE_HANDLE ||
                                mr == MOQR_ERR_WRONG_STATE;
                    d->state = D_ST_TERMINATING;
                    d->term_code = moot ? 0 : R_ERR_INTERNAL;
                    d->term_overrun = false;
                    d->term_quiet = moot;
                }
                dch_pop(s, src, shard);
                if (d->state == D_ST_TERMINATING) {
                    break;   /* phase 5 settles teardown before later results */
                }
            } else if (msg->kind == D_MSG_DONE) {
                r_pdemand *d = pend_find(m, msg->demand_id);
                if (!demand_reply_matches(d, src, msg)) {
                    dch_pop(s, src, shard);   /* cancel completed first */
                    continue;
                }
                if (d->state == D_ST_UNDEMANDING) {
                    /* The owner's terminal supersedes the pending cancel
                     * notice: its per-demand state is already gone, so the
                     * UNDEMAND is no longer owed and the local track was
                     * already resolved when the entry went UNDEMANDING. */
                    pend_retire(m, d);
                    dch_pop(s, src, shard);
                    continue;
                }
                if (d->state == D_ST_TERMINATING) {
                    /* A staged local terminal owns this demand's teardown;
                     * the DONE waits behind it (its trigger is the retained
                     * head, so this arm is normally unreachable — kept as a
                     * guard, never an erase). */
                    break;
                }
                if (d->state == D_ST_ACKED) {
                    /* Post-ACK DONE is a full requester terminal: abandon
                     * any still-open objects with the DONE's code before
                     * source_done, one retryable stage at a time. */
                    if (!pend_terminal_step(s, m, d, msg->pd,
                                            shard_local_reset(
                                                MOQR_RESET_CANCELLED),
                                            shard_pd_forwarded(msg->pd),
                                            now_us)) {
                        break;   /* stage held: the DONE stays durable */
                    }
                    m->remote_demand_resolved++;
                    m->last_done_code = msg->error_code;
                    m->last_done_pre_ack = false;
                    pend_retire(m, d);
                    dch_pop(s, src, shard);
                    continue;
                }
                moqr_result_t rc = moqr_core_upstream_error(
                    m->core, d->track, d->track_gen, msg->error_code,
                    now_us);
                if (rc == MOQR_ERR_WOULD_BLOCK) {
                    break;   /* intent ring full: DONE stays durable */
                }
                if (rc == MOQR_OK) {
                    m->remote_demand_refused++;
                    m->remote_demand_resolved++;
                    m->last_done_code = msg->error_code;
                    /* Pre-ACK refusal is a REQUEST_ERROR: no terminal at all. */
                    m->last_done_pd = moqr_pd_desc_none();
                    m->last_done_pre_ack = true;
                } else if (rc != MOQR_ERR_STALE_HANDLE &&
                           rc != MOQR_ERR_WRONG_STATE) {
                    m->oom = true;
                }
                pend_retire(m, d);
                dch_pop(s, src, shard);
            } else if (msg->kind == D_MSG_OBJ) {
                r_pdemand *d = pend_find(m, msg->demand_id);
                if (d == NULL || d->state == D_ST_UNDEMANDING) {
                    dch_pop(s, src, shard);   /* moot: pop releases clones */
                    continue;
                }
                if (d->state == D_ST_TERMINATING) {
                    /* The trigger (this head, or one before it) stays until
                     * the staged terminal commits — popping would let later
                     * messages jump the uncommitted teardown. */
                    break;
                }
                if (d->state != D_ST_ACKED || d->origin != src) {
                    /* Data for a demand that never ACKed (or from the wrong
                     * channel) is an invariant violation — the owner gates
                     * extraction on the ACK being in the same FIFO ahead of
                     * it. Consume safely (clones released at pop) and
                     * fail-stop. */
                    m->oom = true;
                    dch_pop(s, src, shard);
                    continue;
                }
                moqr_log_append_desc_t desc;
                moqr_log_append_desc_init(&desc);
                desc.group_id = msg->group_id;
                desc.subgroup_id = msg->subgroup_id;
                desc.object_id = msg->object_id;
                desc.publisher_priority = msg->prio;
                desc.status = (moqr_obj_status_t)msg->status;
                desc.datagram_pref = msg->datagram_pref;
                desc.end_of_group = msg->end_of_group;
                desc.first_object = msg->first_object;
                desc.payload = msg->payload;
                desc.properties = msg->props;
                desc.properties_codec = msg->props_codec;
                desc.subgroup_properties = msg->subgroup_props;
                desc.now_us = now_us;
                moqr_result_t rc = moqr_core_ingest(m->core, d->track, &desc);
                if (rc == MOQR_OK) {
                    /* Ownership transferred to the log: null the refs so the
                     * pop releases nothing twice. */
                    msg->payload = NULL;
                    msg->props = NULL;
                    pend_note_feed(d, msg->group_id);
                    m->inbound_data_applied = true;
                    dch_pop(s, src, shard);
                } else if (rc == MOQR_ERR_WOULD_BLOCK) {
                    break;   /* head durable, refs intact: retry next round */
                } else if (rc == MOQR_ERR_NOMEM) {
                    m->oom = true;   /* fail-stop; head stays durable */
                    break;
                } else if (rc == MOQR_ERR_CAPACITY) {
                    /* Local capacity edge: terminate THIS demand (INTERNAL
                     * 0x0) through the staged path — the head stays durable
                     * until the terminal commits, after which it pops as
                     * moot. Loss-visible, never a hidden drop. */
                    d->state = D_ST_TERMINATING;
                    d->term_code = R_ERR_INTERNAL;
                    break;
                } else if (rc == MOQR_ERR_TOO_OLD) {
                    /* Below the requester's OWN retention horizon — the same
                     * outcome local eviction produces; counted, never a
                     * hidden drop. */
                    m->remote_data_rejected++;
                    dch_pop(s, src, shard);
                } else {
                    /* Ordering INVAL / desync: consume safely, fail-stop. */
                    m->oom = true;
                    dch_pop(s, src, shard);
                }
            } else if (msg->kind == D_MSG_OBJ_OPEN ||
                       msg->kind == D_MSG_OBJ_CHUNK ||
                       msg->kind == D_MSG_OBJ_END) {
                r_pdemand *d = pend_find(m, msg->demand_id);
                if (d == NULL || d->state == D_ST_UNDEMANDING) {
                    dch_pop(s, src, shard);   /* moot: pop releases clones */
                    continue;
                }
                if (d->state == D_ST_TERMINATING) {
                    break;   /* hold everything behind the staged terminal */
                }
                if (d->state != D_ST_ACKED || d->origin != src) {
                    m->oom = true;   /* data before ACK: invariant broken */
                    dch_pop(s, src, shard);
                    continue;
                }
                r_psg_t *row = pend_popen_row(m, d);
                if (row == NULL) {
                    /* No progress row left: loss-visible demand terminal. */
                    d->state = D_ST_TERMINATING;
                    d->term_code = R_ERR_INTERNAL;
                    d->term_overrun = true;
                    break;   /* head durable until the terminal commits */
                }
                if (msg->kind == D_MSG_OBJ_OPEN) {
                    r_psg_t *slot = psg_claim(row, s->sg_slots,
                                              msg->group_id,
                                              msg->subgroup_id);
                    if (slot == NULL) {
                        d->state = D_ST_TERMINATING;
                        d->term_code = R_ERR_INTERNAL;
                        d->term_overrun = true;
                        break;
                    }
                    if (slot->object_open) {
                        m->oom = true;   /* re-open without END: desync */
                        dch_pop(s, src, shard);
                        continue;
                    }
                    moqr_log_append_desc_t desc;
                    moqr_log_append_desc_init(&desc);
                    desc.group_id = msg->group_id;
                    desc.subgroup_id = msg->subgroup_id;
                    desc.object_id = msg->object_id;
                    desc.publisher_priority = msg->prio;
                    desc.end_of_group = msg->end_of_group;
                    desc.first_object = msg->first_object;
                    desc.obj_state = MOQR_OBJ_OPEN;
                    desc.declared_len = msg->declared_len;
                    desc.properties = msg->props;
                    desc.properties_codec = msg->props_codec;
                    desc.subgroup_properties = msg->subgroup_props;
                    desc.now_us = now_us;
                    moqr_result_t rc =
                        moqr_core_ingest(m->core, d->track, &desc);
                    if (rc == MOQR_OK) {
                        msg->props = NULL;   /* transferred to the log */
                        pend_note_feed(d, msg->group_id);
                        slot->object_open = true;
                        slot->object_id = msg->object_id;
                        slot->next_chunk = 0;
                        m->inbound_data_applied = true;
                        dch_pop(s, src, shard);
                    } else if (rc == MOQR_ERR_WOULD_BLOCK) {
                        break;   /* head durable, refs intact */
                    } else if (rc == MOQR_ERR_NOMEM) {
                        m->oom = true;
                        break;   /* head durable; fail-stop */
                    } else if (rc == MOQR_ERR_CAPACITY) {
                        d->state = D_ST_TERMINATING;
                        d->term_code = R_ERR_INTERNAL;
                        d->term_overrun = false;
                        break;
                    } else if (rc == MOQR_ERR_TOO_OLD) {
                        /* Below the local horizon: no record, but the slot
                         * still tracks the object so its chunk/END sequence
                         * consumes as the same loss instead of desync. */
                        m->remote_data_rejected++;
                        slot->object_open = true;
                        slot->discarding = true;
                        slot->object_id = msg->object_id;
                        slot->next_chunk = 0;
                        dch_pop(s, src, shard);
                    } else {
                        m->oom = true;   /* ordering INVAL / desync */
                        dch_pop(s, src, shard);
                    }
                } else if (msg->kind == D_MSG_OBJ_CHUNK) {
                    r_psg_t *slot = psg_find(row, s->sg_slots,
                                             msg->group_id,
                                             msg->subgroup_id);
                    if (slot == NULL || !slot->object_open ||
                        slot->object_id != msg->object_id) {
                        m->oom = true;   /* chunk for no open object */
                        dch_pop(s, src, shard);
                        continue;
                    }
                    if (slot->discarding) {
                        m->remote_data_rejected++;
                        dch_pop(s, src, shard);
                        continue;
                    }
                    moqr_result_t rc = moqr_core_append_chunk(
                        m->core, d->track, msg->group_id, msg->subgroup_id,
                        msg->object_id, msg->payload);
                    if (rc == MOQR_OK) {
                        /* append_chunk increfs — the channel keeps its own
                         * clone ref and releases it at pop as usual. */
                        slot->next_chunk++;
                        m->inbound_data_applied = true;
                        dch_pop(s, src, shard);
                    } else if (rc == MOQR_ERR_WOULD_BLOCK) {
                        break;
                    } else if (rc == MOQR_ERR_NOMEM) {
                        m->oom = true;
                        break;
                    } else if (rc == MOQR_ERR_CAPACITY) {
                        d->state = D_ST_TERMINATING;
                        d->term_code = R_ERR_INTERNAL;
                        d->term_overrun = false;
                        break;
                    } else if (rc == MOQR_ERR_TOO_OLD) {
                        /* The record fell below the local horizon; every
                         * later message for it consumes the same way. */
                        m->remote_data_rejected++;
                        dch_pop(s, src, shard);
                    } else {
                        m->oom = true;
                        dch_pop(s, src, shard);
                    }
                } else {   /* D_MSG_OBJ_END */
                    r_psg_t *slot = psg_find(row, s->sg_slots,
                                             msg->group_id,
                                             msg->subgroup_id);
                    if (slot == NULL || !slot->object_open ||
                        slot->object_id != msg->object_id) {
                        m->oom = true;   /* end for no open object */
                        dch_pop(s, src, shard);
                        continue;
                    }
                    if (slot->discarding) {
                        m->remote_data_rejected++;
                        slot->object_open = false;
                        slot->discarding = false;
                        slot->object_id = 0;
                        slot->next_chunk = 0;
                        dch_pop(s, src, shard);
                        continue;
                    }
                    moqr_result_t rc = moqr_core_complete_record(
                        m->core, d->track, msg->group_id, msg->subgroup_id,
                        msg->object_id);
                    if (rc == MOQR_ERR_WOULD_BLOCK) {
                        break;
                    }
                    if (rc == MOQR_ERR_NOMEM) {
                        m->oom = true;
                        break;
                    }
                    if (rc == MOQR_ERR_CAPACITY) {
                        d->state = D_ST_TERMINATING;
                        d->term_code = R_ERR_INTERNAL;
                        d->term_overrun = false;
                        break;
                    }
                    if (rc == MOQR_ERR_TOO_OLD) {
                        m->remote_data_rejected++;
                    } else if (rc != MOQR_OK) {
                        m->oom = true;
                    } else {
                        m->inbound_data_applied = true;
                    }
                    /* Only the OBJECT fields reset — the subgroup slot
                     * lives on for this (group, subgroup)'s next object. */
                    slot->object_open = false;
                    slot->discarding = false;
                    slot->object_id = 0;
                    slot->next_chunk = 0;
                    dch_pop(s, src, shard);
                }
            } else if (msg->kind == D_MSG_OBJ_RESET ||
                       msg->kind == D_MSG_GRP_RESET ||
                       msg->kind == D_MSG_GRP_EVICT ||
                       msg->kind == D_MSG_SG_SEAL) {
                r_pdemand *d = pend_find(m, msg->demand_id);
                if (d == NULL || d->state == D_ST_UNDEMANDING) {
                    dch_pop(s, src, shard);   /* moot */
                    continue;
                }
                if (d->state == D_ST_TERMINATING) {
                    break;   /* hold everything behind the staged terminal */
                }
                if (d->state != D_ST_ACKED || d->origin != src) {
                    m->oom = true;   /* terminal before ACK: invariant broken */
                    dch_pop(s, src, shard);
                    continue;
                }
                r_psg_t *row =
                    (d->popen_row != UINT32_MAX)
                        ? &m->popen[(size_t)d->popen_row * s->sg_slots]
                        : NULL;
                if (msg->kind == D_MSG_SG_SEAL) {
                    /* The durable subgroup FIN. FIFO puts every OBJ_END
                     * before it, so an open (non-discarding) slot here is a
                     * desync, never a race. */
                    r_psg_t *slot =
                        row != NULL ? psg_find(row, s->sg_slots,
                                               msg->group_id,
                                               msg->subgroup_id)
                                    : NULL;
                    if (slot != NULL && slot->object_open &&
                        !slot->discarding) {
                        m->oom = true;
                        dch_pop(s, src, shard);
                        continue;
                    }
                    moqr_result_t rc =
                        msg->seal_reset
                            ? moqr_core_reset_subgroup(
                                  m->core, d->track, msg->group_id,
                                  msg->subgroup_id, msg->reset)
                            : moqr_core_seal_subgroup(
                                  m->core, d->track, msg->group_id,
                                  msg->subgroup_id);
                    if (rc != MOQR_OK && rc != MOQR_DONE &&
                        rc != MOQR_ERR_STALE_HANDLE) {
                        m->oom = true;   /* INVAL contradiction: fail closed */
                        dch_pop(s, src, shard);
                        continue;
                    }
                    if (rc == MOQR_OK) {
                        /* OK (not DONE/STALE) is the one outcome that marks
                         * local readiness — the seal notice is deliverable. */
                        m->inbound_data_applied = true;
                    }
#ifdef MOQR_BIND_TESTING
                    if (rc == MOQR_OK || rc == MOQR_DONE) {
                        uint32_t si = (uint32_t)(m->seal_seq %
                                                 (sizeof(m->seal_log) /
                                                  sizeof(m->seal_log[0])));
                        m->seal_log[si].seq = m->seal_seq;
                        m->seal_log[si].src = src;
                        m->seal_log[si].demand_id = msg->demand_id;
                        m->seal_log[si].group_id = msg->group_id;
                        m->seal_log[si].subgroup_id = msg->subgroup_id;
                        m->seal_seq++;
                    }
#endif
                    if (slot != NULL) {
                        memset(slot, 0, sizeof(*slot));
                    }
                    dch_pop(s, src, shard);
                } else if (msg->kind == D_MSG_OBJ_RESET) {
                    r_psg_t *slot =
                        row != NULL ? psg_find(row, s->sg_slots,
                                               msg->group_id,
                                               msg->subgroup_id)
                                    : NULL;
                    if (slot == NULL || !slot->object_open ||
                        slot->object_id != msg->object_id) {
                        m->oom = true;   /* reset for no open object */
                        dch_pop(s, src, shard);
                        continue;
                    }
                    if (!slot->discarding) {
                        moqr_result_t rc = moqr_core_abandon_record(
                            m->core, d->track, msg->group_id,
                            msg->subgroup_id, msg->object_id,
                            msg->reset);
                        if (rc != MOQR_OK && rc != MOQR_ERR_TOO_OLD &&
                            rc != MOQR_ERR_STALE_HANDLE) {
                            m->oom = true;
                            dch_pop(s, src, shard);
                            continue;
                        }
                        if (rc == MOQR_OK) {
                            m->inbound_data_applied = true;
                        }
                    }
                    /* Reset terminates the subgroup: free the whole slot. */
                    memset(slot, 0, sizeof(*slot));
                    dch_pop(s, src, shard);
                } else if (msg->kind == D_MSG_GRP_RESET) {
                    /* The group was evicted at the owner while begun. Like
                     * the local bind's evict-reset, this stage touches ONLY
                     * begun/open subgroups — completed-but-unsealed slots
                     * survive so the GRP_EVICT behind it can still find and
                     * seal those streams. Each cleared slot is the retry
                     * cursor — a fail-stop retry never redoes one. */
                    bool fault = false;
                    if (row != NULL) {
                        for (uint32_t sl = 0; sl < s->sg_slots; sl++) {
                            if (!row[sl].used ||
                                row[sl].group != msg->group_id ||
                                !row[sl].object_open) {
                                continue;
                            }
                            if (!row[sl].discarding) {
                                moqr_result_t rc = moqr_core_abandon_record(
                                    m->core, d->track, row[sl].group,
                                    row[sl].subgroup, row[sl].object_id,
                                    msg->reset);
                                if (rc != MOQR_OK &&
                                    rc != MOQR_ERR_TOO_OLD &&
                                    rc != MOQR_ERR_STALE_HANDLE) {
                                    fault = true;
                                    break;
                                }
                                if (rc == MOQR_OK) {
                                    m->inbound_data_applied = true;
                                }
                            }
                            memset(&row[sl], 0, sizeof(row[sl]));
                        }
                    }
                    /* The slot sweep covers every partial that was applied
                     * through a tracked slot — normally all of them. The
                     * group is dead at its source, so belt-and-suspenders:
                     * abandon any OPEN tail the walk could not see, so a
                     * dead group never retains an open record. */
                    {
                        moqr_result_t gr = moqr_core_abandon_group_open(
                            m->core, d->track, msg->group_id,
                            msg->reset);
                        if (gr != MOQR_OK && gr != MOQR_DONE &&
                            gr != MOQR_ERR_STALE_HANDLE) {
                            fault = true;
                        }
                    }
                    if (fault) {
                        m->oom = true;
                    }
                    dch_pop(s, src, shard);
                } else {   /* D_MSG_GRP_EVICT: group_id = the watermark */
                    /* Everything below the owner's oldest retained group is
                     * gone: abandon still-open objects, seal so completed-
                     * but-unsealed streams close, then free the slots. */
                    bool fault = false;
                    if (row != NULL) {
                        for (uint32_t sl = 0; sl < s->sg_slots; sl++) {
                            if (!row[sl].used ||
                                row[sl].group >= msg->group_id) {
                                continue;
                            }
                            moqr_reset_desc_t evict_desc;
                            if (moqr_reset_desc_internal(
                                    MOQR_RESET_CODE_EVICTED, &evict_desc) !=
                                MOQR_OK) {
                                evict_desc = moqr_reset_desc_none();
                            }
                            if (row[sl].object_open &&
                                !row[sl].discarding) {
                                moqr_result_t rc = moqr_core_abandon_record(
                                    m->core, d->track, row[sl].group,
                                    row[sl].subgroup, row[sl].object_id,
                                    evict_desc);
                                if (rc != MOQR_OK &&
                                    rc != MOQR_ERR_TOO_OLD &&
                                    rc != MOQR_ERR_STALE_HANDLE) {
                                    fault = true;
                                    break;
                                }
                                if (rc == MOQR_OK) {
                                    m->inbound_data_applied = true;
                                }
                            }
                            moqr_result_t sr = moqr_core_seal_subgroup(
                                m->core, d->track, row[sl].group,
                                row[sl].subgroup);
                            if (sr != MOQR_OK && sr != MOQR_DONE &&
                                sr != MOQR_ERR_STALE_HANDLE) {
                                fault = true;
                                break;
                            }
                            memset(&row[sl], 0, sizeof(row[sl]));
                        }
                    }
                    /* The slot sweep above can only reach subgroups this
                     * destination already OPENED; the watermark also closes
                     * everything it never received. Recording it on the local
                     * journal turns "not yet" into "never will" for the whole
                     * below-watermark range, and the EXISTING per-subscription
                     * eviction machinery (position reclaim, EVICT_WATERMARK
                     * notices, downstream subgroup closes) then retires the
                     * dangling downstream state on the following scans —
                     * without this, a subgroup whose seal died with the
                     * owner's eviction stays open downstream forever. */
                    {
                        moqr_result_t wr = moqr_core_note_track_evicted_below(
                            m->core, d->track, msg->group_id);
                        if (wr != MOQR_OK && wr != MOQR_ERR_STALE_HANDLE) {
                            fault = true;
                        }
                    }
                    if (fault) {
                        m->oom = true;
                    }
                    dch_pop(s, src, shard);
                }
            } else if (msg->kind == D_MSG_FILL_REQ) {
                if (!owner_fill_accept(s, shard, src, msg, pushed_dst_mask,
                                       now_us)) {
                    break;   /* the REQ stays durable at the head */
                }
                dch_pop(s, src, shard);
            } else if (msg->kind == D_MSG_FILL_MORE ||
                       msg->kind == D_MSG_FILL_CANCEL) {
                /* No entry: the retrieval already ended here (moot). The
                 * data phase re-reads every retrieval, so a grant takes
                 * effect in this same step. */
                r_oret_t *o = oret_find(m, src, msg->fill_id);
                if (o != NULL && o->demand_id != msg->demand_id) o = NULL;
                if (o != NULL && msg->kind == D_MSG_FILL_CANCEL) {
                    oret_free(m, o);
                } else if (o != NULL) {
                    o->credit += msg->fill_credit;
                }
                dch_pop(s, src, shard);
            } else if (msg->kind == D_MSG_FILL_OPEN ||
                       msg->kind == D_MSG_FILL_ITEM ||
                       msg->kind == D_MSG_FILL_END) {
                r_fret_t *f = fret_find(m, msg->fill_id);
                if (f == NULL || f->origin != src ||
                    f->demand_id != msg->demand_id || f->closed ||
                    f->abandoned || f->ended) {
                    /* Late or no longer wanted: ignored (pop releases). */
                    dch_pop(s, src, shard);
                    continue;
                }
                if (msg->kind == D_MSG_FILL_OPEN) {
                    if (f->standalone != msg->fetch_standalone) {
                        m->oom = true;   /* answers a different request kind */
                        dch_pop(s, src, shard);
                        continue;
                    }
                    if (f->standalone) {
                        memset(&f->plan, 0, sizeof(f->plan));
                        f->plan.wire.admit = msg->fetch_admit;
                        if (msg->fetch_admit == MOQR_FETCH_ACCEPT) {
                            f->plan.wire.end_of_track = msg->fetch_eot;
                            f->plan.wire.end_group = msg->fill_end_group;
                            f->plan.wire.end_object = msg->fetch_end_raw_object;
                            f->plan.start = (moqr_retrieve_location_t){
                                msg->group_id, msg->object_id };
                            f->plan.end = (moqr_retrieve_location_t){
                                msg->fill_end_group, msg->fill_end_object };
                            f->plan.meta_state = msg->metadata.state;
                            f->plan.meta_wire_version = msg->metadata.wire_version;
                            if (msg->props != NULL) {
                                /* Moved out of the message before its pop. */
                                f->meta_props = msg->props;
                                msg->props = NULL;
                                f->plan.meta_bytes = (moq_bytes_t){
                                    moq_rcbuf_data(f->meta_props),
                                    moq_rcbuf_len(f->meta_props) };
                            }
                            f->start_group = msg->group_id;
                            f->start_object = msg->object_id;
                            f->end_group = msg->fill_end_group;
                            f->end_object = msg->fill_end_object;
                        } else {
                            f->plan.wire.error_code = msg->error_code;
                            f->plan.capacity = msg->fetch_capacity;
                            f->ended = true;
                            f->more_owed = false;
                        }
                    }
                    f->begun = true;
                    dch_pop(s, src, shard);
                    continue;
                }
                if (msg->kind == D_MSG_FILL_END) {
                    f->ended = true;
                    f->terminal = msg->fill_result;
                    f->more_owed = false;
                    dch_pop(s, src, shard);
                    continue;
                }
                if (f->count >= R_FILL_WINDOW || f->granted == 0) {
                    m->oom = true;   /* beyond the granted window: desync */
                    dch_pop(s, src, shard);
                    continue;
                }
                r_fitem_t *it =
                    &f->win[(f->head + f->count) % R_FILL_WINDOW];
                memset(it, 0, sizeof(*it));
                it->result = msg->fill_result;
                it->group_id = msg->group_id;
                it->subgroup_id = msg->subgroup_id;
                it->object_id = msg->object_id;
                it->prio = msg->prio;
                it->status = msg->status;
                it->datagram_pref = msg->datagram_pref;
                it->end_of_group = msg->end_of_group;
                it->declared_len = msg->declared_len;
                it->payload = msg->payload;
                it->props = msg->props;
                it->props_codec = msg->props_codec;
                msg->payload = NULL;   /* moved into the window */
                msg->props = NULL;
                it->charged = dch_hold_head(s, src, shard);
                f->count++;
                f->granted--;
                if (it->result.kind == MOQR_RETRIEVE_ERROR) {
                    f->ended = true;
                    f->terminal = it->result;
                    f->more_owed = false;
                }
            } else {
                m->oom = true;   /* unknown kind: never silently converge */
                dch_pop(s, src, shard);
            }
        }
    }
}

static void
shard_step_order(const moqr_shards_t *s, uint16_t *order)
{
    uint16_t n = s->shard_count;
    for (uint16_t i = 0; i < n; i++) {
        order[i] = i;
    }
    if (s->permute_seed == 0 || n <= 1) {
        return;
    }
    uint64_t r = r_splitmix64(s->permute_seed ^ s->round);
    for (uint16_t i = n; i > 1; i--) {
        r = r_splitmix64(r);
        uint16_t j = (uint16_t)(r % i);
        uint16_t tmp = order[i - 1];
        order[i - 1] = order[j];
        order[j] = tmp;
    }
}

/* The per-shard step seam: run the six phases for exactly ONE shard. Touches
 * only this shard's {core, bind, mgr} plus its mailbox and channel endpoints
 * (inbound consumes src→shard rings; outbound pushes shard→dst rings).
 * Returns the bind pump's result verbatim; fail-stop aggregation (router
 * oom) and the round barrier stay with the runner. `pushed_dst_mask`
 * (optional) reports the step's WAKE SET: every destination that received an
 * outbound push (control mailbox, demand-channel control, or demand-channel
 * data) plus — merged by the step_shard wrapper — every producer whose
 * channel toward this shard regained capacity through a durable pop (the
 * producer credit). The seam a runtime uses to wake exactly the shards with
 * new inbound state or newly freed outbound room. */
static moqr_result_t
shards_step_one(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                uint64_t *pushed_dst_mask)
{
    shard_inbound_batch(s, shard);                              /* phase 1 */
    shard_demand_inbound(s, shard, now_us, pushed_dst_mask);    /*   ..    */
    moqr_result_t rc = moqr_bind_pump(s->shards[shard].bind, now_us); /* 2 */
    shard_reconcile(s, shard, now_us);                          /* phase 3 */
    shard_outbound(s, shard, pushed_dst_mask);                  /* phase 4 */
    shard_demand_phase(s, shard, now_us, pushed_dst_mask);      /* phase 5 */
    shard_data_phase(s, shard, now_us, pushed_dst_mask);        /* phase 6 */
    return rc;
}

/* Advance the round barrier exactly once, after EVERY shard has stepped:
 * pushes stamped this round become visible next round, which is what makes a
 * round's result invariant under the intra-round shard order. */
static void
shards_round_advance(moqr_shards_t *s)
{
    s->round++;
}

/* Population count of a wake mask (portable, branch-per-set-bit). */
static uint32_t
mask_popcount(uint64_t m)
{
    uint32_t n = 0;
    while (m != 0) {
        m &= m - 1u;
        n++;
    }
    return n;
}

/* One ACCOUNTED shard step — the single helper BOTH runners use, so the
 * wake-cause counters mean the same thing everywhere. The push and credit
 * masks stay separate until the accounting: each cause adds popcount(its
 * mask) once per step — several messages toward one destination in one
 * step are ONE wake request — and only then do they merge into the
 * caller's wake set (the deterministic runner discards the merged mask but
 * still records both causes). */
static moqr_result_t
shards_step_accounted(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                      uint64_t *pushed_dst_mask)
{
    s->shards[shard].pop_wake = 0;
    r_mgr *wm = s->shards[shard].mgr;
    if (wm != NULL) {
        wm->inbound_data_applied = false;
        wm->reconcile_owed_local_drain = false;
        wm->fill_wake_owed = false;
    }
    uint64_t push_mask = 0;
    moqr_result_t rc = shards_step_one(s, shard, now_us, &push_mask);
    uint64_t credit_mask = s->shards[shard].pop_wake;
    /* The LOCAL continuation, from four exact causes and nothing broader:
     * (a) this step applied inbound data into its own core AND its bind pump
     * (one pass, already run) left work a self-wake can carry forward —
     * REASON-AWARE per moqr_bind_pump_continuation_pending: a ready mark that
     * landed during the pass, or a SESSION_SG-parked conn (whose one-per-pump
     * re-attempt has no external edge). An ACTION_CAP-parked conn is NOT it: it
     * awaits a downstream transport-capacity edge (the managed adapter arms the
     * lane pump from a transport-event batch — a cause distinct from a shard
     * push/credit wake). (b) that pump pass ended ON THE DELIVERY-BUDGET GUARD with work
     * remaining. Both are the leftover-work outcome no future event re-signals,
     * which is how a batch bigger than one pass drains across a FINITE chain
     * (one self-wake per unfinished pass) instead of stranding one hop later. (c)
     * this step's reconcile left core intents owed a drain, its namespace
     * fan-out having landed in the intent ring AFTER the one pump pass that
     * could have taken it. (d) a fill-retrieval slot freed after a fill was
     * refused one: that fill retries its claim on this shard's next pump,
     * which nothing else is bound to bring. All four are the leftover-work
     * outcome no future event re-signals. A
     * distinct cause, never folded into push (no message crossed toward anyone)
     * or credit (no producer regained room). Applying inbound data whose pass
     * DRAINS EVERYTHING (or leaves only ACTION_CAP backpressure) requests
     * nothing, and a reconcile that converges without queueing anything
     * requests nothing, so steady traffic that keeps up earns no spurious
     * self-wakes. */
    bool continue_local =
        (wm != NULL && wm->inbound_data_applied &&
         moqr_bind_pump_continuation_pending(s->shards[shard].bind)) ||
        moqr_bind_pump_budget_pending(s->shards[shard].bind) ||
        (wm != NULL && wm->reconcile_owed_local_drain) ||
        (wm != NULL && wm->fill_wake_owed);
    uint64_t local_mask = continue_local ? (1ull << shard) : 0;
    if (wm != NULL) {
        wm->wake_req_push += mask_popcount(push_mask);
        wm->wake_req_credit += mask_popcount(credit_mask);
        wm->wake_req_local += mask_popcount(local_mask);
    }
    if (pushed_dst_mask != NULL) {
        *pushed_dst_mask = push_mask | credit_mask | local_mask;
    }
    return rc;
}

moqr_result_t
moqr_shards_step(moqr_shards_t *s, uint64_t now_us)
{
    if (s == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint16_t order[MOQR_SHARDS_MAX];
    shard_step_order(s, order);
    moqr_result_t result = MOQR_OK;
    for (uint16_t p = 0; p < s->shard_count; p++) {
        moqr_result_t rc = shards_step_accounted(s, order[p], now_us, NULL);
        if (rc != MOQR_OK && result == MOQR_OK) {
            result = rc;
        }
    }
    /* A local NS_FOUND/NS_GONE the router could not record (its canonical key
     * would not allocate, or the journal is full) is a BORROWED view that cannot
     * be parked or replayed — it is gone. Surface it so the runtime never reports
     * convergence over a candidate set that silently lost an observation. */
    for (uint16_t i = 0; i < s->shard_count && result == MOQR_OK; i++) {
        r_mgr *m = s->shards[i].mgr;
        if (m != NULL && m->oom) {
            result = MOQR_ERR_NOMEM;
        }
    }
    shards_round_advance(s);
    return result;
}

/* -- lifecycle -------------------------------------------------------------- */

void
moqr_shards_cfg_init_sized(moqr_shards_cfg_t *cfg, size_t cfg_size,
                           const moq_alloc_t *alloc)
{
    if (cfg == NULL || cfg_size < sizeof(uint32_t)) {
        return;
    }
    size_t n = cfg_size < sizeof(moqr_shards_cfg_t) ? cfg_size
                                                    : sizeof(moqr_shards_cfg_t);
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (offsetof(moqr_shards_cfg_t, alloc) +
            sizeof(((moqr_shards_cfg_t *)0)->alloc) <= n) {
        cfg->alloc = alloc;
    }
    if (offsetof(moqr_shards_cfg_t, core_cfg) +
            sizeof(((moqr_shards_cfg_t *)0)->core_cfg) <= n) {
        moqr_core_relay_cfg_init_sized(&cfg->core_cfg, sizeof(cfg->core_cfg),
                                       alloc);
    }
    if (offsetof(moqr_shards_cfg_t, bind_cfg) +
            sizeof(((moqr_shards_cfg_t *)0)->bind_cfg) <= n) {
        moqr_bind_cfg_init_sized(&cfg->bind_cfg, sizeof(cfg->bind_cfg), alloc);
    }
}

/* Per-shard core/bind templates: the caller's nested config only when the
 * outer prefix reaches it AND it is fully initialized; else a library
 * default. Shared by create, the resolver, and the capacity describe. */
/* Fill-retrieval slots per shard and role: the resolved core fetch-cursor
 * pool, which bounds the fills a shard can serve at once and the cursors an
 * owner can lend to retrievals. Zero without admission. */
static uint32_t
shards_fill_slots(const moqr_shards_limits_t *lim,
                  const moqr_core_limits_t *clim)
{
    return lim->admit ? clim->max_fetches : 0u;
}

static void
shards_templates(const moqr_shards_cfg_t *cfg, const moq_alloc_t *a,
                 moqr_core_relay_cfg_t *core_tmpl, moqr_bind_cfg_t *bind_tmpl)
{
    if (SHARDS_CFG_HAS(cfg, core_cfg) &&
        cfg->core_cfg.struct_size == sizeof(cfg->core_cfg)) {
        *core_tmpl = cfg->core_cfg;
    } else {
        moqr_core_relay_cfg_init_sized(core_tmpl, sizeof(*core_tmpl), a);
    }
    if (SHARDS_CFG_HAS(cfg, bind_cfg) &&
        cfg->bind_cfg.struct_size == sizeof(cfg->bind_cfg)) {
        *bind_tmpl = cfg->bind_cfg;
    } else {
        moqr_bind_cfg_init_sized(bind_tmpl, sizeof(*bind_tmpl), a);
    }
}

static moqr_cluster_cfg_t
shards_cluster_cfg(const moqr_shards_cfg_t *cfg)
{
    moqr_cluster_cfg_t out;
    moqr_cluster_cfg_init_sized(&out, sizeof(out));
    out.enabled = SHARDS_CFG_HAS(cfg, cluster_enabled) && cfg->cluster_enabled;
    out.local_hop_id = SHARDS_CFG_HAS(cfg, cluster_hop_id) ? cfg->cluster_hop_id : 0;
    out.max_routes = SHARDS_CFG_HAS(cfg, max_cluster_routes) ? cfg->max_cluster_routes : 0;
    out.max_hops = SHARDS_CFG_HAS(cfg, max_cluster_hops) ? cfg->max_cluster_hops : 0;
    return out;
}

moqr_result_t
moqr_shards_cfg_resolve(const moqr_shards_cfg_t *cfg,
                        moqr_shards_limits_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    if (cfg == NULL || cfg->struct_size < sizeof(uint32_t)) {
        return MOQR_ERR_INVAL;
    }
    const moq_alloc_t *a =
        (SHARDS_CFG_HAS(cfg, alloc) && cfg->alloc != NULL) ? cfg->alloc : NULL;
    uint32_t k = (SHARDS_CFG_HAS(cfg, shards) && cfg->shards != 0)
                     ? cfg->shards
                     : 1u;
    if (k > MOQR_SHARDS_MAX) {
        return MOQR_ERR_INVAL;
    }
    out->shards = (uint16_t)k;
    /* Admission is effective only at K>1: a single lane builds no manager, so
     * the progress tables are never reserved and a forced admit_remote_demand
     * is structurally inert. Normalize here so the resolved view, the capacity
     * descriptor, and s->admit_remote all agree on effective admission. */
    out->admit = k > 1u && SHARDS_CFG_HAS(cfg, admit_remote_demand) &&
                 cfg->admit_remote_demand;
    out->mbox_cap =
        (SHARDS_CFG_HAS(cfg, mailbox_entries) && cfg->mailbox_entries != 0)
            ? cfg->mailbox_entries
            : R_SHARDS_DEF_MAILBOX;
    out->trace_ring =
        (SHARDS_CFG_HAS(cfg, trace_ring_records) &&
         cfg->trace_ring_records != 0)
            ? cfg->trace_ring_records
            : R_SHARDS_DEF_TRACE;

    moqr_core_relay_cfg_t core_tmpl;
    moqr_bind_cfg_t bind_tmpl;
    shards_templates(cfg, a, &core_tmpl, &bind_tmpl);

    moqr_cluster_cfg_t cluster_cfg = shards_cluster_cfg(cfg);
    if (cluster_cfg.enabled) {
        moqr_core_capacity_t capacity;
        if (moqr_core_cluster_capacity_describe(&core_tmpl, &cluster_cfg,
                                               &capacity) != MOQR_OK)
            return MOQR_ERR_INVAL;
    }

    out->jrn_cap =
        (SHARDS_CFG_HAS(cfg, journal_entries) && cfg->journal_entries != 0)
            ? cfg->journal_entries
            : ((core_tmpl.max_ns_nodes != 0) ? core_tmpl.max_ns_nodes : 256u);
    out->pend_cap =
        (SHARDS_CFG_HAS(cfg, pending_demand_entries) &&
         cfg->pending_demand_entries != 0)
            ? cfg->pending_demand_entries
            : ((core_tmpl.max_tracks != 0) ? core_tmpl.max_tracks : 64u);
    out->dch_cap =
        (SHARDS_CFG_HAS(cfg, demand_channel_entries) &&
         cfg->demand_channel_entries != 0)
            ? cfg->demand_channel_entries
            : out->pend_cap;
    {
        moqr_log_cfg_t lc;
        moqr_log_cfg_init_sized(&lc, sizeof(lc), a);
        lc.budget = core_tmpl.log_budget;
        moqr_log_capacity_t logc;
        if (moqr_log_capacity_describe(&lc, &logc) != MOQR_OK) {
            return MOQR_ERR_INVAL;   /* a wrapped log model must never feed
                                      * a zero channel byte cap */
        }
        uint64_t resolved = logc.payload_bytes;
        uint64_t want = SHARDS_CFG_HAS(cfg, demand_channel_bytes)
                            ? cfg->demand_channel_bytes
                            : 0;
        if (want != 0 && want < resolved) {
            return MOQR_ERR_INVAL;   /* a legal record must always fit */
        }
        out->dch_byte_cap = want != 0 ? want : resolved;
    }
    out->pump_turn_msgs =
        (SHARDS_CFG_HAS(cfg, pump_turn_messages) &&
         cfg->pump_turn_messages != 0)
            ? cfg->pump_turn_messages
            : 64u;
    out->pump_turn_bytes =
        (SHARDS_CFG_HAS(cfg, pump_turn_bytes) && cfg->pump_turn_bytes != 0)
            ? cfg->pump_turn_bytes
            : out->dch_byte_cap / 4u;
    out->sg_slots =
        (SHARDS_CFG_HAS(cfg, pump_subgroup_slots) &&
         cfg->pump_subgroup_slots != 0)
            ? cfg->pump_subgroup_slots
            : (bind_tmpl.max_open_subgroups != 0
                   ? bind_tmpl.max_open_subgroups
                   : MOQR_BIND_DEF_OPEN_SUBGROUPS);
    if (k > 1u && out->admit) {
        uint64_t rows = (uint64_t)out->pend_cap * out->sg_slots;
        if (rows > UINT32_MAX || rows > SIZE_MAX / sizeof(r_psg_t)) {
            return MOQR_ERR_INVAL;
        }
    }
    /* Binding budget: at K>1 every shard's manager consumes K core binding
     * slots (K-1 origin pseudo-bindings + the manager binding), so the core
     * pool must leave at least one external slot. K=1 has no manager and
     * reserves nothing. usable = what a transport may actually admit. */
    moqr_core_limits_t clim;
    if (moqr_core_limits_resolve(&core_tmpl, &clim) != MOQR_OK) {
        return MOQR_ERR_INVAL;
    }
    if (k > 1u && clim.max_bindings <= k) {
        return MOQR_ERR_INVAL;
    }
    moqr_bind_limits_t blim;
    if (moqr_bind_cfg_resolve(&bind_tmpl, &clim, &blim) != MOQR_OK) {
        return MOQR_ERR_INVAL;
    }
    uint32_t external =
        (k > 1u) ? clim.max_bindings - k : clim.max_bindings;
    out->usable_bindings =
        blim.max_conns < external ? blim.max_conns : external;
    return MOQR_OK;
}

moqr_result_t
moqr_shards_capacity_describe(const moqr_shards_cfg_t *cfg,
                              moqr_shards_capacity_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    moqr_shards_limits_t lim;
    moqr_result_t rc = moqr_shards_cfg_resolve(cfg, &lim);
    if (rc != MOQR_OK) {
        return rc;
    }
    const moq_alloc_t *a =
        (SHARDS_CFG_HAS(cfg, alloc) && cfg->alloc != NULL) ? cfg->alloc : NULL;
    moqr_core_relay_cfg_t core_tmpl;
    moqr_bind_cfg_t bind_tmpl;
    shards_templates(cfg, a, &core_tmpl, &bind_tmpl);
    moqr_core_limits_t clim;
    (void)moqr_core_limits_resolve(&core_tmpl, &clim);

    uint64_t K = lim.shards;
    uint64_t K2 = cap_mul(K, K);

    moqr_core_capacity_t cc;
    moqr_cluster_cfg_t cluster_cfg = shards_cluster_cfg(cfg);
    if (moqr_core_cluster_capacity_describe(&core_tmpl, &cluster_cfg, &cc) != MOQR_OK) {
        return MOQR_ERR_INVAL;
    }
    moqr_bind_capacity_t bc;
    moqr_cluster_info_t cluster_info;
    if (moqr_cluster_cfg_resolve(&cluster_cfg, &cluster_info) != MOQR_OK)
        return MOQR_ERR_INVAL;
    if (moqr_bind_cluster_capacity_describe(&bind_tmpl, &clim, &cluster_info,
                                           &bc) != MOQR_OK) {
        return MOQR_ERR_INVAL;
    }
    out->core_structure_bytes = cc.structure_bytes;
    out->core_payload_bytes = cc.payload_bytes;
    out->bind_structure_bytes = bc.total_bytes;
    out->trace_bytes = moqr_trace_bytes(lim.trace_ring);
    out->per_shard_bytes =
        cap_add(cap_add(cc.total_bytes, bc.total_bytes), out->trace_bytes);

    /* Runtime container + the K>1 pools (§1 terms 1,2,6..18). */
    uint64_t st = cap_add(sizeof(moqr_shards_t),
                          cap_mul(K, sizeof(r_shard_t)));
    if (K > 1u) {
        st = cap_add(st, cap_mul(K2, cap_add(sizeof(r_mailbox),
                                             cap_mul(lim.mbox_cap,
                                                     sizeof(r_ctrl_msg)))));
        st = cap_add(st,
                     cap_mul(K2, cap_add(sizeof(r_demand_channel_t),
                                         cap_mul(lim.dch_cap,
                                                 sizeof(r_demand_msg_t)))));
        st = cap_add(st, K2);   /* the arbiter byte per directed pair */
        uint64_t mgr = cap_add(
            sizeof(r_mgr),
            cap_add(cap_mul(lim.jrn_cap, sizeof(r_jentry)),
                    cap_mul(lim.pend_cap,
                            sizeof(r_pdemand) + sizeof(r_psub))));
        st = cap_add(st, cap_mul(K, mgr));
        if (cluster_cfg.enabled) {
            uint64_t routes = cluster_cfg.max_routes != 0 ? cluster_cfg.max_routes : 256u;
            st = cap_add(st, cap_mul(K, cap_mul(routes, sizeof(r_cluster_snapshot))));
        }
        st = cap_add(st, cap_mul(K2, sizeof(moqr_binding_t)));
        if (lim.admit) {
            uint64_t rows = cap_mul(2u * (uint64_t)lim.pend_cap,
                                    lim.sg_slots);
            st = cap_add(st, cap_mul(K, cap_add(cap_mul(rows,
                                                        sizeof(r_psg_t)),
                                                lim.pend_cap)));
            /* Fill-retrieval tables. The window items' payload is charged
             * to the channel that carried it, inside channel_byte_ceiling. */
            st = cap_add(st, cap_mul(K, cap_mul(shards_fill_slots(&lim,
                                                                  &clim),
                                                sizeof(r_fret_t) +
                                                    sizeof(r_oret_t))));
        }
    }
    out->shards_structure_bytes = st;

    size_t hdr = 0;
    (void)moq_rcbuf_allocation_size(0, &hdr);
    if (K > 1u) {
        /* Channel ceiling: the byte gauge gates DATA only — control kinds
         * are counted but exempt, so every slot may additionally hold a
         * full-track canon key or a P-sized Track Properties clone. The
         * existing two clone headers per slot also cover metadata. */
        uint64_t per_ch = cap_add(
            lim.dch_byte_cap,
            cap_mul(lim.dch_cap,
                    cap_add(cap_add(R_CANON_FT_MAX, MOQR_BIND_META_SET_MAX),
                            cap_mul(2u, hdr))));
        out->channel_byte_ceiling = cap_mul(K2, per_ch);
        out->canon_byte_ceiling = cap_add(
            cap_mul(K2, cap_mul(lim.mbox_cap, R_CANON_NS_MAX)),
            cap_add(cap_mul(K, cap_mul(lim.jrn_cap, R_CANON_NS_MAX)),
                    cap_mul(K, cap_mul(lim.pend_cap, R_CANON_FT_MAX))));
        /* A requester retrieval slot routing a standalone FETCH to the
         * owning shard owns that request's full-track key until its REQ
         * carries it (admission only). */
        out->canon_byte_ceiling = cap_add(
            out->canon_byte_ceiling,
            cap_mul(K, cap_mul(shards_fill_slots(&lim, &clim),
                               R_CANON_FT_MAX)));
        /* Each standalone retrieval slot, owner and requester side, holds
         * at most one Track Properties copy of at most a full set: the
         * owner's from admission until OPEN carries it, the requester's from
         * OPEN until the request closes. In between it is the channel's,
         * inside channel_byte_ceiling, and never in two places at once. */
        if (lim.admit) {
            out->canon_byte_ceiling = cap_add(
                out->canon_byte_ceiling,
                cap_mul(K, cap_mul(cap_mul(2u, shards_fill_slots(&lim, &clim)),
                                   cap_add(MOQR_BIND_META_SET_MAX, hdr))));
        }
        /* Allocate-before-push staging: one message's owned material per
         * concurrently stepping source shard (a channel-bounded record/reason +
         * two clone headers, a full-track DEMAND key, or P-sized metadata
         * plus its clone header). Owner pins/requester directory copies
         * remain inside each binding's separately charged metadata budget. */
        {
            moqr_log_cfg_t lc;
            moqr_log_cfg_init_sized(&lc, sizeof(lc), a);
            lc.budget = core_tmpl.log_budget;
            moqr_log_capacity_t logc;
            if (moqr_log_capacity_describe(&lc, &logc) != MOQR_OK) {
                memset(out, 0, sizeof(*out));
                return MOQR_ERR_INVAL;
            }
            out->staging_byte_ceiling = cap_mul(
                K, cap_add(lim.dch_byte_cap,
                           cap_add(cap_add(cap_mul(2u, hdr), R_CANON_FT_MAX),
                                   cap_add(MOQR_BIND_META_SET_MAX, hdr))));
        }
    }

    if (K > 1u && cluster_cfg.enabled) {
        uint64_t routes = cluster_cfg.max_routes != 0 ? cluster_cfg.max_routes : 256u;
        uint64_t hops = cluster_cfg.max_hops != 0 ? cluster_cfg.max_hops : 32u;
        uint64_t hop_bytes = cap_mul(hops, sizeof(uint64_t));
        out->canon_byte_ceiling = cap_add(out->canon_byte_ceiling,
            cap_add(cap_mul(K, cap_mul(routes, cap_add(R_CANON_NS_MAX, hop_bytes))),
                    cap_mul(K2, cap_mul(lim.mbox_cap, hop_bytes))));
        /* Snapshot replacement and mailbox push allocate before releasing
         * their old bytes; at most one such operation per source shard. */
        out->staging_byte_ceiling = cap_add(out->staging_byte_ceiling,
            cap_mul(K, cap_add(R_CANON_NS_MAX, hop_bytes)));
    }

    uint64_t total = cap_mul(K, out->per_shard_bytes);
    total = cap_add(total, out->shards_structure_bytes);
    total = cap_add(total, out->channel_byte_ceiling);
    total = cap_add(total, out->canon_byte_ceiling);
    total = cap_add(total, out->staging_byte_ceiling);
    if (total == UINT64_MAX) {
        memset(out, 0, sizeof(*out));
        return MOQR_ERR_INVAL;   /* poisoned by overflow: refuse */
    }
    out->relay_alloc_ceiling = total;
    out->usable_bindings_per_shard = lim.usable_bindings;
    out->shards = lim.shards;
    out->admission = lim.admit;
    return MOQR_OK;
}

moqr_result_t
moqr_shards_create(const moqr_shards_cfg_t *cfg, moqr_shards_t **out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    *out = NULL;
    if (cfg == NULL || cfg->struct_size < sizeof(uint32_t)) {
        return MOQR_ERR_INVAL;
    }
    const moq_alloc_t *a = SHARDS_CFG_HAS(cfg, alloc) ? cfg->alloc : NULL;
    if (a == NULL || a->alloc == NULL || a->free == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* One pure resolution shared with moqr_shards_capacity_describe —
     * every default, coupling, and INVAL rule (byte cap, progress-table
     * product, K>1 binding budget) fires HERE, before any allocation. */
    moqr_shards_limits_t lim;
    {
        moqr_result_t rrc = moqr_shards_cfg_resolve(cfg, &lim);
        if (rrc != MOQR_OK) {
            return rrc;
        }
    }
    uint32_t k = lim.shards;

    moqr_shards_t *s = a->alloc(sizeof(*s), a->ctx);
    if (s == NULL) {
        return MOQR_ERR_NOMEM;
    }
    memset(s, 0, sizeof(*s));
    s->alloc = *a;
    s->shard_count = (uint16_t)k;
    s->placement = (SHARDS_CFG_HAS(cfg, placement) && cfg->placement != NULL)
                       ? cfg->placement
                       : r_rendezvous_place;
    s->permute_seed = SHARDS_CFG_HAS(cfg, permute_seed) ? cfg->permute_seed : 0;
    s->mbox_cap = lim.mbox_cap;
    s->place_state.struct_size = (uint32_t)sizeof(s->place_state);
    /* epochs stay generation 0 (memset); a control input bumps them later. */
    s->place_state.shard_count = (uint16_t)k;
    s->place_state.node_count = 1;

    s->shards = a->alloc((size_t)k * sizeof(*s->shards), a->ctx);
    if (s->shards == NULL) {
        a->free(s, sizeof(*s), a->ctx);
        return MOQR_ERR_NOMEM;
    }
    memset(s->shards, 0, (size_t)k * sizeof(*s->shards));

    uint32_t trace_ring = lim.trace_ring;

    moqr_core_relay_cfg_t core_tmpl;
    moqr_bind_cfg_t bind_tmpl;
    shards_templates(cfg, a, &core_tmpl, &bind_tmpl);

    /* Journal bound (per shard): one entry per tracked namespace. Explicit knob if
     * given, else sized off the core's namespace-node budget. Self-echo suppression
     * is per-entry, so it needs no separate pool. */
    s->jrn_cap = lim.jrn_cap;
    moqr_cluster_cfg_t cluster_cfg = shards_cluster_cfg(cfg);
    s->cluster_enabled = cluster_cfg.enabled;
    s->cluster_routes = cluster_cfg.max_routes != 0 ? cluster_cfg.max_routes : 256u;
    s->cluster_hops = cluster_cfg.max_hops != 0 ? cluster_cfg.max_hops : 32u;

    /* Pending remote-owner demand bound (per shard): at most one upstream attempt
     * per track, so the core's max_tracks is the natural default; explicit knob
     * lets a test exercise the fail-stop. */
    s->pend_cap = lim.pend_cap;

    /* Inbound visibility policy, fixed at create (before any stepper runs): a
     * concurrent per-shard runtime sets it true so a push is visible as soon
     * as the mailbox mutex publishes it (no shared round to advance); the
     * deterministic runner leaves it false and relies on the round barrier. */
    s->live_visibility =
        SHARDS_CFG_HAS(cfg, live_visibility) && cfg->live_visibility;

    /* Demand-channel ENTRY bound (per directed pair). Control keeps at most
     * one in-flight message per demand per direction, so the pending-demand
     * cap is the natural default; queued data is additionally governed by
     * the byte cap below. The explicit knob lets tests exercise channel
     * backpressure. */
    s->dch_cap = lim.dch_cap;

    /* Owner-side demand admission, fixed at create and effective only at K>1.
     * A bare config defaults OFF — the owner refuses every forwarded demand
     * (NOT_SUPPORTED); the production CLI turns it on for every multi-lane
     * serve, where a subscriber on one lane must reach a publisher on another.
     * A single lane builds no manager, so a forced flag is inert. */
    s->admit_remote = lim.admit;

    /* Channel byte cap: default = the RESOLVED per-track log byte budget
     * (moqr_log_capacity_describe applies the log's own defaulting), so any
     * single legal record — which the log bounds to that budget — always
     * eventually fits an empty channel. An explicit smaller cap could strand
     * a legal record forever: refuse it at create rather than wedge at
     * runtime. This is a per-channel bound, deliberately NOT a process-wide
     * ceiling — the capacity command reports the closed-form process ceiling
     * separately, at every lane count. */
    s->dch_byte_cap = lim.dch_byte_cap;
    s->pump_turn_msgs = lim.pump_turn_msgs;
    s->pump_turn_bytes = lim.pump_turn_bytes;
    /* Per-demand subgroup-progress capacity. Default follows the session
     * binding's resolution of max_open_subgroups: the template value when
     * set, else the shared MOQR_BIND_DEF_OPEN_SUBGROUPS default. */
    s->sg_slots = lim.sg_slots;
    {
        moqr_core_limits_t clim;
        if (moqr_core_limits_resolve(&core_tmpl, &clim) != MOQR_OK) {
            moqr_shards_destroy(s);
            return MOQR_ERR_INVAL;
        }
        s->fill_cap = shards_fill_slots(&lim, &clim);
    }

    /* Both progress tables are pend_cap x sg_slots rows per shard; reject
     * products the allocation and index arithmetic cannot represent BEFORE
     * any dependent allocation — an undersized table is never an option.
     * Only where the tables can exist: a K=1 or refusal-only runtime never
     * allocates them, so an impossible product is dead config there, not a
     * startup failure. */
    /* The progress-table product and K>1 binding-budget guards fired in
     * moqr_shards_cfg_resolve above, before anything was allocated. */

    /* Control mailboxes (K>1 only). */
    if (k > 1u) {
        size_t nring = (size_t)k * k;
        s->mbox = a->alloc(nring * sizeof(*s->mbox), a->ctx);
        if (s->mbox == NULL) {
            moqr_shards_destroy(s);
            return MOQR_ERR_NOMEM;
        }
        memset(s->mbox, 0, nring * sizeof(*s->mbox));
        /* Init every mailbox mutex first (cannot fail with default attrs on
         * the supported platforms; checked anyway), so a slot-allocation
         * failure below hands destroy() uniformly-initialized mutexes. */
        for (size_t r = 0; r < nring; r++) {
            if (pthread_mutex_init(&s->mbox[r].mu, NULL) != 0) {
                while (r > 0) {
                    r--;
                    (void)pthread_mutex_destroy(&s->mbox[r].mu);
                }
                s->alloc.free(s->mbox, nring * sizeof(*s->mbox), s->alloc.ctx);
                s->mbox = NULL;
                moqr_shards_destroy(s);
                return MOQR_ERR_NOMEM;
            }
        }
        for (size_t r = 0; r < nring; r++) {
            r_mailbox *mb = &s->mbox[r];
            mb->cap = s->mbox_cap;
            mb->slots = a->alloc((size_t)s->mbox_cap * sizeof(r_ctrl_msg), a->ctx);
            if (mb->slots == NULL) {
                moqr_shards_destroy(s);
                return MOQR_ERR_NOMEM;
            }
            memset(mb->slots, 0, (size_t)s->mbox_cap * sizeof(r_ctrl_msg));
        }
        /* Demand channels: same layout and the same mutex-first discipline. */
        s->dch = a->alloc(nring * sizeof(*s->dch), a->ctx);
        if (s->dch == NULL) {
            moqr_shards_destroy(s);
            return MOQR_ERR_NOMEM;
        }
        memset(s->dch, 0, nring * sizeof(*s->dch));
        for (size_t r = 0; r < nring; r++) {
            if (pthread_mutex_init(&s->dch[r].mu, NULL) != 0) {
                while (r > 0) {
                    r--;
                    (void)pthread_mutex_destroy(&s->dch[r].mu);
                }
                s->alloc.free(s->dch, nring * sizeof(*s->dch), s->alloc.ctx);
                s->dch = NULL;
                moqr_shards_destroy(s);
                return MOQR_ERR_NOMEM;
            }
        }
        for (size_t r = 0; r < nring; r++) {
            r_demand_channel_t *ch = &s->dch[r];
            ch->cap = s->dch_cap;
            ch->slots =
                a->alloc((size_t)s->dch_cap * sizeof(r_demand_msg_t), a->ctx);
            if (ch->slots == NULL) {
                moqr_shards_destroy(s);
                return MOQR_ERR_NOMEM;
            }
            memset(ch->slots, 0, (size_t)s->dch_cap * sizeof(r_demand_msg_t));
        }
        /* Sticky arbitration tokens, one per directed channel; zero =
         * R_ARB_CTRL, the ruled initial preference. */
        s->arb = a->alloc(nring, a->ctx);
        if (s->arb == NULL) {
            moqr_shards_destroy(s);
            return MOQR_ERR_NOMEM;
        }
        memset(s->arb, 0, nring);
    }

    for (uint16_t i = 0; i < (uint16_t)k; i++) {
        r_shard_t *sh = &s->shards[i];
        if (moqr_trace_create(&s->alloc, trace_ring, &sh->trace) != MOQR_OK) {
            moqr_shards_destroy(s);
            return MOQR_ERR_NOMEM;
        }
        moqr_core_relay_cfg_t ccfg = core_tmpl;
        ccfg.trace = sh->trace;
        ccfg.shard_index = i;
        ccfg.shard_count = (uint16_t)k;
        if (moqr_core_create_cluster(&ccfg, &cluster_cfg, &sh->core) != MOQR_OK) {
            moqr_shards_destroy(s);
            return MOQR_ERR_NOMEM;
        }
        /* Manager (K>1 only): allocated before the bind so the intent router can
         * point at it. K==1 is inert — no manager, no router. */
        moqr_bind_cfg_t bcfg = bind_tmpl;
        bcfg.core = sh->core;
        if (k > 1u) {
            sh->mgr = mgr_create(s, i, sh->core);
            if (sh->mgr == NULL) {
                moqr_shards_destroy(s);
                return MOQR_ERR_NOMEM;
            }
            bcfg.router = shards_router;
            bcfg.router_ctx = sh->mgr;
            bcfg.router_cookie_base = MOQR_SHARD_COOKIE_BASE;
        }
        if (moqr_bind_create(&bcfg, &sh->bind) != MOQR_OK) {
            moqr_shards_destroy(s);
            return MOQR_ERR_NOMEM;
        }
        if (sh->mgr != NULL &&
            moqr_bind_meta_set_observer(sh->bind, shards_metadata_changed,
                                        sh->mgr) != MOQR_OK) {
            moqr_shards_destroy(s);
            return MOQR_ERR_INVAL;
        }
        /* The fill wait: carried by this config's own appended fields, never
         * by the embedded bind template, whose size is fixed. */
        if (moqr_bind_set_fill_timeouts(
                sh->bind,
                SHARDS_CFG_HAS(cfg, fill_timeout_default_ms)
                    ? cfg->fill_timeout_default_ms : 0,
                SHARDS_CFG_HAS(cfg, fill_timeout_max_ms)
                    ? cfg->fill_timeout_max_ms : 0) != MOQR_OK) {
            moqr_shards_destroy(s);
            return MOQR_ERR_INVAL;
        }
        if (sh->mgr != NULL && s->fill_cap > 0) {
            /* Fills over a range that precedes this shard's feed of a
             * remote-owned track are retrieved from the owner. */
            moqr_bind_fill_source_t fsrc;
            memset(&fsrc, 0, sizeof(fsrc));
            fsrc.struct_size = (uint32_t)sizeof(fsrc);
            fsrc.ctx = sh->mgr;
            fsrc.open = shards_fill_open;
            fsrc.peek = shards_fill_peek;
            fsrc.commit = shards_fill_commit;
            fsrc.close = shards_fill_close;
            fsrc.next_deadline_us = shards_fill_next_deadline;
            if (moqr_bind_set_fill_source(sh->bind, &fsrc) != MOQR_OK) {
                moqr_shards_destroy(s);
                return MOQR_ERR_NOMEM;
            }
            if (moqr_bind_retrieve_source_set(sh->bind,
                    shards_fill_retrieve_peek) != MOQR_OK) {
                moqr_shards_destroy(s);
                return MOQR_ERR_INVAL;
            }
            /* A standalone FETCH for a track another shard owns is answered
             * by that shard through the same retrieval answers. */
            moqr_retrieve_standalone_ops_t sops = {
                .ctx = sh->mgr, .open = shards_sa_open, .plan = shards_sa_plan,
                .peek = shards_fill_retrieve_peek, .commit = shards_fill_commit,
                .close = shards_fill_close,
            };
            if (moqr_bind_retrieve_standalone_set(sh->bind, &sops) != MOQR_OK) {
                moqr_shards_destroy(s);
                return MOQR_ERR_INVAL;
            }
        }
        if (sh->mgr != NULL && mgr_open_bindings(sh->mgr) != MOQR_OK) {
            moqr_shards_destroy(s);
            return MOQR_ERR_NOMEM;
        }
    }

    *out = s;
    return MOQR_OK;
}

void
moqr_shards_destroy(moqr_shards_t *s)
{
    if (s == NULL) {
        return;
    }
    if (s->shards != NULL) {
        for (uint16_t i = 0; i < s->shard_count; i++) {
            r_shard_t *sh = &s->shards[i];
            if (sh->bind != NULL) {
                /* Memory-only, while the bind still owns its accounting and
                 * allocator. Partial creation without a bind has no pins. */
                if (sh->mgr != NULL && sh->mgr->psub != NULL) {
                    for (uint32_t j = 0; j < sh->mgr->psub_cap; j++) {
                        psub_metadata_release(sh->mgr, &sh->mgr->psub[j]);
                    }
                }
                moqr_bind_destroy(sh->bind);
            }
            mgr_free(sh->mgr);
            if (sh->core != NULL) {
                moqr_core_destroy(sh->core);
            }
            if (sh->trace != NULL) {
                moqr_trace_destroy(sh->trace);   /* after the core borrowing it */
            }
        }
        s->alloc.free(s->shards, (size_t)s->shard_count * sizeof(*s->shards),
                      s->alloc.ctx);
    }
    if (s->arb != NULL) {
        s->alloc.free(s->arb, (size_t)s->shard_count * s->shard_count,
                      s->alloc.ctx);
    }
    if (s->dch != NULL) {
        size_t nring = (size_t)s->shard_count * s->shard_count;
        for (size_t r = 0; r < nring; r++) {
            r_demand_channel_t *ch = &s->dch[r];
            for (uint32_t i = 0; ch->slots != NULL && i < ch->count; i++) {
                r_demand_msg_t *msg = &ch->slots[(ch->head + i) % ch->cap];
                dmsg_release(&s->alloc, msg);
            }
            sh_free(&s->alloc, ch->slots,
                    (size_t)ch->cap * sizeof(r_demand_msg_t));
            (void)pthread_mutex_destroy(&ch->mu);
        }
        s->alloc.free(s->dch, nring * sizeof(*s->dch), s->alloc.ctx);
    }
    if (s->mbox != NULL) {
        size_t nring = (size_t)s->shard_count * s->shard_count;
        for (size_t r = 0; r < nring; r++) {
            r_mailbox *mb = &s->mbox[r];
            for (uint32_t i = 0; mb->slots != NULL && i < mb->cap; i++) {
                if (mb->slots[i].used) {
                    sh_free(&s->alloc, mb->slots[i].canon, mb->slots[i].canon_len);
                    sh_free(&s->alloc, mb->slots[i].hops,
                            mb->slots[i].record.route.metadata.hop_count * sizeof(uint64_t));
                }
            }
            sh_free(&s->alloc, mb->slots, (size_t)mb->cap * sizeof(r_ctrl_msg));
            (void)pthread_mutex_destroy(&mb->mu);
        }
        s->alloc.free(s->mbox, nring * sizeof(*s->mbox), s->alloc.ctx);
    }
    s->alloc.free(s, sizeof(*s), s->alloc.ctx);
}

/* -- accessors -------------------------------------------------------------- */

uint16_t
moqr_shards_count(const moqr_shards_t *s)
{
    return s != NULL ? s->shard_count : 0;
}

moqr_core_t *
moqr_shards_core(moqr_shards_t *s, uint16_t shard)
{
    return (s != NULL && shard < s->shard_count) ? s->shards[shard].core : NULL;
}

moqr_bind_t *
moqr_shards_bind(moqr_shards_t *s, uint16_t shard)
{
    return (s != NULL && shard < s->shard_count) ? s->shards[shard].bind : NULL;
}

moqr_trace_t *
moqr_shards_trace(moqr_shards_t *s, uint16_t shard)
{
    return (s != NULL && shard < s->shard_count) ? s->shards[shard].trace : NULL;
}

moqr_owner_t
moqr_shards_place(const moqr_shards_t *s, const moqr_place_key_t *key)
{
    moqr_owner_t none = { 0, 0 };
    if (s == NULL || key == NULL) {
        return none;
    }
    return s->placement(key, &s->place_state);
}

/* -- test/debug introspection ----------------------------------------------- */
#if defined(MOQR_RELAY_INSPECT) || defined(MOQR_BIND_TESTING)

/* Hash a namespace directly from its parts, streaming the SAME bytes (count,
 * then all lengths, then all data) that canon_build lays out, so this matches
 * canon_hash(canon_build(parts)) without allocating. */
static uint64_t
ns_hash_parts(const moq_bytes_t *parts, uint32_t count)
{
    uint64_t h = 0xCBF29CE484222325ull;
    uint8_t cb[4];
    wr32(cb, count);
    for (int b = 0; b < 4; b++) {
        h ^= cb[b];
        h *= 0x100000001B3ull;
    }
    for (uint32_t i = 0; i < count; i++) {
        wr32(cb, (uint32_t)parts[i].len);
        for (int b = 0; b < 4; b++) {
            h ^= cb[b];
            h *= 0x100000001B3ull;
        }
    }
    for (uint32_t i = 0; i < count; i++) {
        for (uint32_t b = 0; b < parts[i].len; b++) {
            h ^= parts[i].data[b];
            h *= 0x100000001B3ull;
        }
    }
    return h;
}

void
moqr_shards_debug_journal(moqr_shards_t *s, uint16_t shard,
                          const moq_bytes_t *parts, uint32_t count,
                          moqr_shards_jinfo_t *out)
{
    memset(out, 0, sizeof(*out));
    out->winner = -1;
    out->mirror = -1;
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return;
    }
    r_mgr *m = s->shards[shard].mgr;
    uint32_t len = 0;
    uint8_t *canon = canon_build(parts, count, &len, &m->alloc);
    if (canon == NULL) {
        return;
    }
    uint32_t ins = 0;
    r_jentry *e = jrn_find(m, canon, len, &ins);
    sh_free(&m->alloc, canon, len);
    if (e == NULL) {
        return;
    }
    out->present = true;
    out->candidates = e->candidates;
    out->winner = r_winner_of(e->candidates, e->hash);
    out->mirror = e->mirror;
    out->holdout = e->holdout;
}

int32_t
moqr_shards_debug_hrw_winner(uint64_t candidates, const moq_bytes_t *parts,
                             uint32_t count)
{
    return r_winner_of(candidates, ns_hash_parts(parts, count));
}

uint32_t
moqr_shards_debug_pending_tokens(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    r_mgr *m = s->shards[shard].mgr;
    uint32_t pending = 0;
    for (uint32_t i = 0; i < m->jrn_len; i++) {
        pending += (uint32_t)m->jrn[i].echo_ann + (uint32_t)m->jrn[i].echo_unann;
    }
    return pending;
}

uint32_t
moqr_shards_debug_mailbox_pending(moqr_shards_t *s, uint16_t src, uint16_t dst)
{
    if (s == NULL || s->mbox == NULL || src >= s->shard_count ||
        dst >= s->shard_count) {
        return 0;
    }
    r_mailbox *mb = &s->mbox[(uint32_t)src * s->shard_count + dst];
    pthread_mutex_lock(&mb->mu);
    uint32_t n = mb->count;
    pthread_mutex_unlock(&mb->mu);
    return n;
}

uint32_t
moqr_shards_debug_demand_channel_pending(moqr_shards_t *s, uint16_t src,
                                         uint16_t dst)
{
    if (s == NULL || s->dch == NULL || src >= s->shard_count ||
        dst >= s->shard_count) {
        return 0;
    }
    r_demand_channel_t *ch = &s->dch[(uint32_t)src * s->shard_count + dst];
    pthread_mutex_lock(&ch->mu);
    uint32_t n = ch->count;
    pthread_mutex_unlock(&ch->mu);
    return n;
}

uint64_t
moqr_shards_debug_remote_demand_refused(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    return s->shards[shard].mgr->remote_demand_refused;
}

void
moqr_shards_debug_retrieval_meta(moqr_shards_t *s, uint16_t shard, uint32_t *owner_held,
                                 uint32_t *requester_held, uint32_t *max_refs)
{
    *owner_held = *requester_held = *max_refs = 0;
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) return;
    r_mgr *m = s->shards[shard].mgr;
    for (uint32_t i = 0; m->oret != NULL && i < m->fill_cap; i++) {
        if (m->oret[i].meta_props == NULL) continue;
        (*owner_held)++;
        uint32_t n = moq_rcbuf_refcount(m->oret[i].meta_props);
        if (n > *max_refs) *max_refs = n;
    }
    for (uint32_t i = 0; m->fret != NULL && i < m->fill_cap; i++) {
        if (m->fret[i].meta_props == NULL) continue;
        (*requester_held)++;
        uint32_t n = moq_rcbuf_refcount(m->fret[i].meta_props);
        if (n > *max_refs) *max_refs = n;
    }
}

uint32_t
moqr_shards_debug_pending_demand(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    return s->shards[shard].mgr->pend_len;
}

uint64_t
moqr_shards_debug_remote_demand_resolved(moqr_shards_t *s, uint16_t shard,
                                         uint64_t *last_code,
                                         bool *last_pre_ack)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    r_mgr *m = s->shards[shard].mgr;
    if (last_code != NULL) {
        *last_code = m->last_done_code;
    }
    if (last_pre_ack != NULL) {
        *last_pre_ack = m->last_done_pre_ack;
    }
    return m->remote_demand_resolved;
}

bool
moqr_shards_debug_remote_demand_last_pd(moqr_shards_t *s, uint16_t shard,
                                        moqr_pd_desc_t *out)
{
    if (s == NULL || out == NULL || shard >= s->shard_count ||
        s->shards[shard].mgr == NULL) {
        return false;
    }
    const r_mgr *m = s->shards[shard].mgr;

    if (m->remote_demand_resolved == 0) {
        return false;   /* nothing resolved yet: the field has no meaning */
    }
    *out = m->last_done_pd;
    return true;
}

/* The tagged terminal on the head message of a directed channel, without
 * consuming it: the same bytes a re-peek will read. */
bool
moqr_shards_debug_demand_channel_head_pd(moqr_shards_t *s, uint16_t src,
                                         uint16_t dst, moqr_pd_desc_t *out)
{
    if (s == NULL || out == NULL || src >= s->shard_count ||
        dst >= s->shard_count) {
        return false;
    }
    const r_demand_msg_t *msg = dch_head(s, src, dst);

    if (msg == NULL || msg->kind != D_MSG_DONE) {
        return false;
    }
    *out = msg->pd;
    return true;
}

uint32_t
moqr_shards_debug_owner_pump_subs(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    r_mgr *m = s->shards[shard].mgr;
    uint32_t n = 0;
    for (uint32_t i = 0; i < m->psub_cap; i++) {
        n += m->psub[i].used ? 1u : 0u;
    }
    return n;
}

moqr_track_t
moqr_shards_debug_pending_demand_track(moqr_shards_t *s, uint16_t shard,
                                       uint32_t idx, uint64_t *track_gen)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL ||
        idx >= s->shards[shard].mgr->pend_len) {
        return MOQR_TRACK_INVALID;
    }
    r_mgr *m = s->shards[shard].mgr;
    if (track_gen != NULL) {
        *track_gen = m->pend[idx].track_gen;
    }
    return m->pend[idx].track;
}

uint64_t
moqr_shards_debug_demand_channel_bytes(moqr_shards_t *s, uint16_t src,
                                       uint16_t dst)
{
    if (s == NULL || s->dch == NULL || src >= s->shard_count ||
        dst >= s->shard_count) {
        return 0;
    }
    r_demand_channel_t *ch = &s->dch[(uint32_t)src * s->shard_count + dst];
    pthread_mutex_lock(&ch->mu);
    uint64_t n = ch->bytes;
    pthread_mutex_unlock(&ch->mu);
    return n;
}

void
moqr_shards_debug_pump_counters(moqr_shards_t *s, uint16_t shard,
                                uint64_t *turns, uint64_t *messages,
                                uint64_t *bytes)
{
    uint64_t t = 0, m = 0, b = 0;
    if (s != NULL && shard < s->shard_count && s->shards[shard].mgr != NULL) {
        t = s->shards[shard].mgr->pump_turns;
        m = s->shards[shard].mgr->pump_messages;
        b = s->shards[shard].mgr->pump_bytes;
    }
    if (turns != NULL) {
        *turns = t;
    }
    if (messages != NULL) {
        *messages = m;
    }
    if (bytes != NULL) {
        *bytes = b;
    }
}

uint64_t
moqr_shards_debug_remote_data_rejected(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    return s->shards[shard].mgr->remote_data_rejected;
}

uint64_t
moqr_shards_debug_remote_demand_term_capacity(moqr_shards_t *s,
                                              uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    return s->shards[shard].mgr->remote_demand_term_capacity;
}

uint64_t
moqr_shards_debug_remote_demand_term_overrun(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    return s->shards[shard].mgr->remote_demand_term_overrun;
}

uint32_t
moqr_shards_debug_owner_progress_slots(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    r_mgr *m = s->shards[shard].mgr;
    if (m->psg == NULL) {
        return 0;
    }
    uint32_t n = 0;
    for (size_t i = 0; i < (size_t)m->psub_cap * s->sg_slots; i++) {
        n += m->psg[i].used ? 1u : 0u;
    }
    return n;
}

uint32_t
moqr_shards_debug_requester_open_objects(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    r_mgr *m = s->shards[shard].mgr;
    if (m->popen == NULL) {
        return 0;
    }
    uint32_t n = 0;
    for (size_t i = 0; i < (size_t)m->pend_cap * s->sg_slots; i++) {
        n += (m->popen[i].used && m->popen[i].object_open &&
              !m->popen[i].discarding)
                 ? 1u
                 : 0u;
    }
    return n;
}

#endif

moqr_result_t
moqr_shards_step_shard(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                       uint64_t *pushed_dst_mask)
{
    if (s == NULL || shard >= s->shard_count) {
        return MOQR_ERR_INVAL;
    }
    /* The accounted step: wake causes counted at the mask level, and the
     * returned wake set is push destinations MERGED with producer credits
     * (lanes whose src->this channel regained capacity through a durable
     * pop are woken alongside the push destinations). */
    moqr_result_t rc =
        shards_step_accounted(s, shard, now_us, pushed_dst_mask);
    if (rc != MOQR_OK) {
        return rc;   /* the bind pump's result takes precedence */
    }
    /* Surface this shard's manager fail-stop: a borrowed observation the
     * router/refuse path could not record latches mgr->oom (sticky, never
     * cleared), so once set every step of this shard reports it. Mirrors the
     * aggregate runner's per-shard oom scan (moqr_shards_step), scoped to the
     * one shard a threaded caller drives. */
    r_mgr *m = s->shards[shard].mgr;
    if (m != NULL && m->oom) {
        return MOQR_ERR_NOMEM;
    }
    return MOQR_OK;
}

#ifdef MOQR_BIND_TESTING
void moqr_shards_debug_fail_stop(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count) {
        return;
    }
    r_mgr *m = s->shards[shard].mgr;
    if (m != NULL) {
        m->oom = true;
    }
}
#endif

/* -- production observability ------------------------------------------------ */

const char *
moqr_shards_msg_kind_name(moqr_shards_msg_kind_t kind)
{
    switch (kind) {
    case MOQR_SHARDS_MSG_DEMAND:    return "demand";
    case MOQR_SHARDS_MSG_UNDEMAND:  return "undemand";
    case MOQR_SHARDS_MSG_DONE:      return "done";
    case MOQR_SHARDS_MSG_ACK:       return "ack";
    case MOQR_SHARDS_MSG_OBJ:       return "obj";
    case MOQR_SHARDS_MSG_OBJ_OPEN:  return "obj_open";
    case MOQR_SHARDS_MSG_OBJ_CHUNK: return "obj_chunk";
    case MOQR_SHARDS_MSG_OBJ_END:   return "obj_end";
    case MOQR_SHARDS_MSG_OBJ_RESET: return "obj_reset";
    case MOQR_SHARDS_MSG_GRP_RESET: return "grp_reset";
    case MOQR_SHARDS_MSG_GRP_EVICT: return "grp_evict";
    case MOQR_SHARDS_MSG_SG_SEAL:   return "sg_seal";
    default:                        return "unknown";
    }
}

moqr_result_t
moqr_shards_get_stats(moqr_shards_t *s, uint16_t shard,
                      moqr_shards_stats_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    if (s == NULL || shard >= s->shard_count) {
        return MOQR_ERR_INVAL;
    }
    r_mgr *m = s->shards[shard].mgr;
    if (m != NULL) {
        out->pump_turns = m->pump_turns;
        out->pump_messages = m->pump_messages;
        out->pump_bytes = m->pump_bytes;
        memcpy(out->enqueued, m->enq, sizeof(out->enqueued));
        out->remote_demand_refused = m->remote_demand_refused;
        out->remote_demand_resolved = m->remote_demand_resolved;
        out->remote_data_rejected = m->remote_data_rejected;
        out->term_capacity = m->remote_demand_term_capacity;
        out->term_overrun = m->remote_demand_term_overrun;
        out->wake_requests_push = m->wake_req_push;
        out->wake_requests_credit = m->wake_req_credit;
        out->wake_requests_local = m->wake_req_local;
        out->turns_msg_budget = m->turns_msg_budget;
        out->turns_byte_budget = m->turns_byte_budget;
        out->turns_blocked = m->turns_blocked;
        out->turns_drained = m->turns_drained;
        out->turns_with_messages = m->turns_with_messages;
        out->arb_class_refusals = m->arb_refusals;
        out->pending_demands = m->pend_len;
        out->journal_epoch = m->journal_epoch;
        /* Pump-subs by their EXACT core state, through the sub-state seam.
         * ONLY a stale/retired handle (a terminal already retired the sub,
         * or the entry never held one) is internal GONE — in neither
         * state. Any other failure is a LIVE sub in an impossible state:
         * the snapshot is poisoned and refused, never silently narrowed. */
        for (uint32_t i = 0; i < m->psub_cap; i++) {
            if (!m->psub[i].used) {
                continue;
            }
            moqr_sub_state_t st;
            moqr_result_t src =
                moqr_core_sub_state(m->core, m->psub[i].sub, &st);
            if (src == MOQR_ERR_STALE_HANDLE) {
                continue;
            }
            if (src != MOQR_OK) {
                memset(out, 0, sizeof(*out));
                return MOQR_ERR_INVAL;
            }
            if (st == MOQR_SUB_PARKED) {
                out->pump_subs_parked++;
            } else {
                out->pump_subs_active++;
            }
        }
        if (m->psg != NULL) {
            for (size_t i = 0; i < (size_t)m->psub_cap * s->sg_slots; i++) {
                out->owner_progress_slots += m->psg[i].used ? 1u : 0u;
            }
        }
        if (m->popen != NULL) {
            for (size_t i = 0; i < (size_t)m->pend_cap * s->sg_slots; i++) {
                out->requester_open_objects +=
                    (m->popen[i].used && m->popen[i].object_open &&
                     !m->popen[i].discarding)
                        ? 1u
                        : 0u;
            }
        }
        /* Manager-owned entities inside this shard core's own gauges: K
         * binding slots (K-1 origin pseudo-bindings + the watcher binding)
         * and the one wildcard namespace subscription. */
        out->internal_bindings = s->shard_count;
        out->internal_ns_subs = 1;
    }
    /* Inbound occupancy + high-water marks, each directed endpoint under
     * its OWN leaf mutex — never a global lock, never another shard's
     * core/bind/journal. */
    for (uint16_t src = 0; s->dch != NULL && src < s->shard_count; src++) {
        if (src == shard) {
            continue;
        }
        r_demand_channel_t *ch =
            &s->dch[(uint32_t)src * s->shard_count + shard];
        pthread_mutex_lock(&ch->mu);
        out->inbound_channel_entries += ch->count + ch->held;
        out->inbound_channel_bytes += ch->bytes + ch->held_bytes;
        if (ch->count_hwm > out->channel_entries_hwm) {
            out->channel_entries_hwm = ch->count_hwm;
        }
        if (ch->bytes_hwm > out->channel_bytes_hwm) {
            out->channel_bytes_hwm = ch->bytes_hwm;
        }
        pthread_mutex_unlock(&ch->mu);
    }
    for (uint16_t src = 0; s->mbox != NULL && src < s->shard_count; src++) {
        if (src == shard) {
            continue;
        }
        r_mailbox *mb = &s->mbox[(uint32_t)src * s->shard_count + shard];
        pthread_mutex_lock(&mb->mu);
        out->mailbox_pending += mb->count;
        pthread_mutex_unlock(&mb->mu);
    }
    return MOQR_OK;
}

moqr_result_t
moqr_shards_get_pair_stats(moqr_shards_t *s, uint16_t src, uint16_t dst,
                           moqr_shards_pair_stats_t *out, size_t out_size)
{
    if (s == NULL || out == NULL || src >= s->shard_count ||
        dst >= s->shard_count || src == dst ||
        out_size < MOQR_SHARDS_PAIR_STATS_V0_SIZE) {
        return MOQR_ERR_INVAL;
    }
    /* sized-output discipline: fill a full local view, copy the bounded
     * prefix — a smaller (older) caller struct gets its prefix, a larger
     * (newer) one gets the rest zeroed */
    moqr_shards_pair_stats_t full;
    memset(&full, 0, sizeof(full));
    r_demand_channel_t *ch = &s->dch[(uint32_t)src * s->shard_count + dst];
    pthread_mutex_lock(&ch->mu);
    full.data_messages = ch->pair_data_msgs;
    full.data_bytes = ch->pair_data_bytes;
    full.control_messages = ch->pair_ctrl_msgs;
    full.refused_entries = ch->refused_entries;
    full.refused_bytes = ch->refused_bytes;
    pthread_mutex_unlock(&ch->mu);
    memset(out, 0, out_size);
    memcpy(out, &full,
           out_size < sizeof(full) ? out_size : sizeof(full));
    return MOQR_OK;
}

/* Bounded journal-dump writer: snprintf-style, counts the full required
 * length past `cap` so the caller learns the size on truncation (the same
 * contract as the metrics/trace/route serializers). */
typedef struct jd_writer {
    char  *buf;
    size_t cap;
    size_t len;
} jd_writer_t;

static void
jd_addf(jd_writer_t *w, const char *fmt, ...)
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

/* One namespace part, LENGTH-DELIMITED and binary-safe: [<raw len>]"..."
 * with printable ASCII verbatim, backslash/quote escaped, and every other
 * byte as \xNN — lossless (the escape is invertible and the length prefix
 * is the raw byte count), never a C string, never a prefix. */
static void
jd_part(jd_writer_t *w, const uint8_t *p, uint32_t len)
{
    jd_addf(w, "[%u]\"", (unsigned)len);
    for (uint32_t i = 0; i < len; i++) {
        uint8_t b = p[i];
        if (b == (uint8_t)'\\' || b == (uint8_t)'"') {
            jd_addf(w, "\\%c", (char)b);
        } else if (b >= 0x20u && b <= 0x7Eu) {
            jd_addf(w, "%c", (char)b);
        } else {
            jd_addf(w, "\\x%02x", (unsigned)b);
        }
    }
    jd_addf(w, "\"");
}

moqr_result_t
moqr_shards_journal_dump_text(moqr_shards_t *s, uint16_t shard, char *buf,
                              size_t cap, size_t *written)
{
    if (written != NULL) {
        *written = 0;
    }
    if (s == NULL || shard >= s->shard_count || buf == NULL) {
        return MOQR_ERR_INVAL;
    }
    r_mgr *m = s->shards[shard].mgr;
    jd_writer_t w = { buf, cap, 0 };
    jd_addf(&w, "shard %u\n", (unsigned)shard);
    jd_addf(&w, "journal_epoch=%llu\n",
            (unsigned long long)(m != NULL ? m->journal_epoch : 0));
    uint32_t n = (m != NULL) ? m->jrn_len : 0;
    if (n == 0) {
        jd_addf(&w, "  (none)\n");
    }
    /* The journal array is kept sorted by canonical key, so array order IS
     * the ascending canonical order the dump promises. */
    for (uint32_t i = 0; i < n; i++) {
        const r_jentry *e = &m->jrn[i];
        uint32_t count = rd32(e->canon);
        uint32_t off = 4u + 4u * count;
        jd_addf(&w, "  ns %u:", (unsigned)count);
        for (uint32_t p = 0; p < count; p++) {
            uint32_t plen = rd32(e->canon + 4u + 4u * p);
            if (p != 0) {
                jd_addf(&w, "/");
            }
            jd_part(&w, e->canon + off, plen);
            off += plen;
        }
        int32_t winner = r_winner_of(e->candidates, e->hash);
        jd_addf(&w, " candidates=0x%llx",
                (unsigned long long)e->candidates);
        if (winner >= 0) {
            jd_addf(&w, " winner=%ld", (long)winner);
        } else {
            jd_addf(&w, " winner=-");
        }
        if (e->mirror != R_MIRROR_NONE) {
            jd_addf(&w, " mirror=%ld", (long)e->mirror);
        } else {
            jd_addf(&w, " mirror=-");
        }
        if (e->holdout) {
            jd_addf(&w, " holdout");
        }
        jd_addf(&w, "\n");
    }
    if (cap > 0) {
        buf[w.len < cap ? w.len : cap - 1] = '\0';
    }
    if (written != NULL) {
        *written = w.len;
    }
    return w.len < cap ? MOQR_OK : MOQR_ERR_CAPACITY;
}

#if defined(MOQR_RELAY_INSPECT) || defined(MOQR_BIND_TESTING)
moqr_result_t
moqr_shards_debug_step_shard(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                             uint64_t *pushed_dst_mask)
{
    return moqr_shards_step_shard(s, shard, now_us, pushed_dst_mask);
}

void
moqr_shards_debug_round_advance(moqr_shards_t *s)
{
    if (s != NULL) {
        shards_round_advance(s);
    }
}

void
moqr_shards_debug_set_live_visibility(moqr_shards_t *s, bool live)
{
    if (s != NULL) {
        s->live_visibility = live;
    }
}

#endif

#ifdef MOQR_BIND_TESTING
/* Verify-only: copy this shard's newest seal-ingest evidence, OLDEST first,
 * into out[0..cap). Returns the number copied (0 for K==1, a bad shard, or
 * an empty log). seq values are the lifetime ingest order. */
uint32_t
moqr_shards_debug_seal_log(const moqr_shards_t *s, uint16_t shard,
                           moqr_shards_seal_ev_t *out, uint32_t cap,
                           uint64_t *out_total)
{
    if (out_total != NULL) {
        *out_total = 0;
    }
    if (s == NULL || out == NULL || cap == 0 || shard >= s->shard_count ||
        s->shards[shard].mgr == NULL) {
        return 0;
    }
    const r_mgr *m = s->shards[shard].mgr;
    if (out_total != NULL) {
        *out_total = m->seal_seq;
    }
    uint32_t ring = (uint32_t)(sizeof(m->seal_log) / sizeof(m->seal_log[0]));
    uint64_t have = m->seal_seq < ring ? m->seal_seq : ring;
    if (have > cap) {
        have = cap;
    }
    uint64_t first = m->seal_seq - have;
    for (uint64_t i = 0; i < have; i++) {
        uint64_t seq = first + i;
        uint32_t si = (uint32_t)(seq % ring);
        out[i].seq = m->seal_log[si].seq;
        out[i].src = m->seal_log[si].src;
        out[i].demand_id = m->seal_log[si].demand_id;
        out[i].group_id = m->seal_log[si].group_id;
        out[i].subgroup_id = m->seal_log[si].subgroup_id;
    }
    return (uint32_t)have;
}

/* Verify-only: track (raw + gen) → demand id, by this shard's pending-demand
 * table (the table is keyed by track — the join key a BIND_SG refusal
 * captures). 0 = no live demand matches (bad shard, or a purely LOCAL
 * track) — and ALSO 0 when MORE THAN ONE entry matches: one-demand-per-
 * {track, gen} is a core invariant, and an exact fail-closed diagnostic
 * must refuse to choose if it ever breaks, never silently pick the first. */
uint64_t
moqr_shards_debug_track_demand(const moqr_shards_t *s, uint16_t shard,
                               uint64_t track_raw, uint64_t track_gen)
{
    if (s == NULL || shard >= s->shard_count ||
        s->shards[shard].mgr == NULL) {
        return 0;
    }
    const r_mgr *m = s->shards[shard].mgr;
    uint64_t found = 0;
    uint32_t hits = 0;
    for (uint32_t i = 0; i < m->pend_len; i++) {
        if (m->pend[i].track._opaque == track_raw &&
            m->pend[i].track_gen == track_gen) {
            found = m->pend[i].demand_id;
            hits++;
        }
    }
    return hits == 1 ? found : 0;
}

/* Verify-only invariant-breaker for the pin above: clone the pending-demand
 * entry for `demand_id` under a NEW demand id (own canon copy, so the
 * normal teardown frees both), then drop it again with _pend_drop. Never a
 * production path — it exists to prove the resolver fails closed on a
 * duplicate {track, gen} rather than silently choosing. */
bool
moqr_shards_debug_pend_dup(moqr_shards_t *s, uint16_t shard,
                           uint64_t demand_id, uint64_t new_demand_id)
{
    if (s == NULL || shard >= s->shard_count ||
        s->shards[shard].mgr == NULL) {
        return false;
    }
    r_mgr *m = s->shards[shard].mgr;
    r_pdemand *src = pend_find(m, demand_id);
    if (src == NULL || pend_find(m, new_demand_id) != NULL ||
        m->pend_len >= m->pend_cap) {
        return false;
    }
    uint8_t *canon = NULL;
    if (src->canon_len > 0) {
        canon = sh_alloc(&m->alloc, src->canon_len);
        if (canon == NULL) {
            return false;
        }
        memcpy(canon, src->canon, src->canon_len);
    }
    r_pdemand *d = &m->pend[m->pend_len++];
    *d = *src;
    d->demand_id = new_demand_id;
    d->canon = canon;
    d->popen_row = UINT32_MAX;   /* the row is OWNED by the source entry —
                                  * sharing it would free it twice on drop */
    return true;
}

bool
moqr_shards_debug_pend_drop(moqr_shards_t *s, uint16_t shard,
                            uint64_t demand_id)
{
    if (s == NULL || shard >= s->shard_count ||
        s->shards[shard].mgr == NULL) {
        return false;
    }
    r_mgr *m = s->shards[shard].mgr;
    r_pdemand *d = pend_find(m, demand_id);
    if (d == NULL) {
        return false;
    }
    pend_retire(m, d);
    return true;
}
#endif
