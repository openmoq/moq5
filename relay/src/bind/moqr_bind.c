#include <moq/relay/moqr_bind.h>
#ifdef MOQR_BIND_TESTING
#include <moq/relay/moqr_bind_test.h>
#endif

#include <moq/relay/capacity.h>

#include "moqr_bind_auth.h"
#include "moqr_bind_metadata.h"
#include "moqr_bind_retrieval.h"
#include "../core/retrieve_internal.h"
#include "../../../core/src/spi/core_spi.h"

#include <moq/rcbuf.h>

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* Fixed forward-latency bucket bounds (microseconds), 100us..1s. Shared by
 * the observer here and the exporter (via moqr_latency_bound_us). */
static const uint64_t moqr_lat_bounds[MOQR_LATENCY_BUCKETS] = {
    100u,   250u,   500u,    1000u,   2500u,   5000u,
    10000u, 25000u, 50000u,  100000u, 250000u, 1000000u,
};

uint64_t
moqr_latency_bound_us(uint32_t i)
{
    return i < MOQR_LATENCY_BUCKETS ? moqr_lat_bounds[i] : 0u;
}

void
moqr_latency_observe(moqr_latency_hist_t *h, uint64_t latency_us)
{
    uint32_t i = 0;
    while (i < MOQR_LATENCY_BUCKETS && latency_us > moqr_lat_bounds[i]) {
        i++;
    }
    h->bucket[i]++;   /* i == MOQR_LATENCY_BUCKETS => +Inf overflow */
    h->sum_us += latency_us;
    h->count++;
}

#define BIND_DEF_CONNS 64u
#define BIND_DEF_DSUBS 32u
#define BIND_DEF_SGS   MOQR_BIND_DEF_OPEN_SUBGROUPS
#define BIND_DEF_USUBS 8u
#define BIND_DEF_PUBS  8u
/* d16 MAX_REQUEST_ID grant step (request-id units). Each grant raises the
 * ceiling by this; the peer keeps ~half a window of headroom. */
#define BIND_DEF_GRANT_WINDOW 1024u
/* Fixed pump work batches. Storage is bind-owned, not automatic, so embedded
 * callers do not inherit a hidden multi-KiB moqr_bind_pump() frame. */
#define BIND_PUMP_EVENT_BATCH 16u
/* Events are polled ONE at a time: a polled event borrows namespace, name
 * and token bytes from the session's scratch, which the session reclaims at
 * the first advancing call made once its event queue is empty. Handling an
 * event with later events still queued keeps that scratch alive; taking a
 * whole batch first and then advancing while handling its first event would
 * reclaim the bytes of every later event in the batch. */
#define BIND_PUMP_EVENT_POLL  1u
#define BIND_PUMP_REVOKED_BATCH 16u
#define BIND_PUMP_INTENT_BATCH 32u

/* The answer a downstream subscription update is owed, decided but not yet
 * taken by the session (B_UPD_NONE once given). */
enum {
    B_UPD_NONE = 0,
    B_UPD_ACCEPT,
    B_UPD_REJECT_FILTER,        /* REQUEST_UPDATE_ERROR: a Location
                                 * Filter the relay does not serve      */
    B_UPD_REJECT_INTERNAL,      /* REQUEST_UPDATE_ERROR INTERNAL_ERROR  */
};

/* The refusal of a valid Location Filter the relay does not serve (see
 * bind_map_location_filter): the relay's own limit, so INTERNAL_ERROR ("an
 * implementation specific ... error", 12.3), never NOT_SUPPORTED, which says
 * the request type itself is not supported. */
#define BIND_FILTER_REFUSAL_CODE MOQ_REQUEST_ERROR_INTERNAL_ERROR
#define BIND_FILTER_REFUSAL_REASON "location filter not served"

/* What a parked SUBSCRIBE carries as its filter type when its Location
 * Filter was refused: a value no relay filter has (they start at
 * MOQR_FILTER_NEXT_GROUP_START), so the refusal is given only once the
 * authorization is decided (bind_resume). */
#define BIND_PARKED_FILTER_REFUSED 0u

typedef struct b_dsub {
    bool       used;
    /* A held update answer (B_UPD_*): the session could not take it yet
     * (WOULD_BLOCK); the pump retries it until it is given or the
     * subscription is gone. upd_has_forward / upd_forward is the Forward
     * change applied to the relay subscription once an acceptance is
     * given. These ride the bool padding. */
    uint8_t    upd_answer;
    bool       upd_has_forward;
    bool       upd_forward;
    /* sub_raw is a publication the relay opened for track discovery
     * (draft-22 SUBSCRIBE_TRACKS), not a peer's SUBSCRIBE. */
    bool       publication;
    uint64_t   sub_raw;      /* downstream session subscription handle    */
    moqr_sub_t rsub;
    uint64_t   upd_id;       /* the update the held answer names          */
    uint64_t   streams_opened; /* lifetime application data streams       */
} b_dsub_t;

/* A deferred discovery decision that came back ALLOW: the offer it
 * authorizes is made on a later pump, under the revalidation grant reserved
 * when the decision arrived (gres.gen 0: the ALLOW carried no lease). */
typedef struct b_dgrant {
    bool             used;
    uint32_t         conn;        /* connection slot                      */
    uint32_t         input_gen;   /* the track subscription's inputs then */
    uint64_t         tcookie;     /* the track subscription               */
    uint64_t         track;       /* moqr_track_t raw                     */
    uint64_t         track_gen;
    moqr_grant_res_t gres;
} b_dgrant_t;

/* Held deferred ALLOWs, across connections. One that finds no room is not
 * held: its offer asks the authorization hook again. */
#define BIND_DGRANTS 16u

typedef struct b_sg {
    bool                  used;
    uint64_t              sub_raw;
    uint64_t              group, subgroup;
    moq_subgroup_handle_t h;
    /* Chunked-delivery write progress on this downstream subgroup:
     * whether begin_object has shipped for the in-flight object, its id, and the
     * next chunk index to write. Held across a downstream WOULD_BLOCK so
     * write_object_data resumes without duplicating or skipping a chunk. */
    bool                  obj_begun;
    uint64_t              obj_id;
    uint32_t              next_chunk;
    /* The downstream stream was opened with PROPERTIES, so it can carry an
     * object with properties. */
    bool                  props;
    /* A reset with reset_wire is owed on this stream (its Track turned out
     * malformed while the session could not take the reset): every later
     * close of the stream is that reset, never a FIN or another code. */
    bool                  reset_owed;
    uint64_t              reset_wire;
} b_sg_t;

/* A downstream FETCH being served from a core fetch cursor. State machine:
 * NEEDS_OK -> accept_fetch (FETCH_OK + data stream) -> STREAMING -> peek/
 * write_fetch_object/commit (holds on WOULD_BLOCK) -> NEEDS_FIN -> end_fetch. */
typedef enum b_fetch_state {
    BF_NEEDS_OK = 0,
    BF_STREAMING,
    BF_NEEDS_FIN,
    BF_FILL_NEEDS_OPEN,    /* fill: open its stream with the FETCH_HEADER   */
    BF_FILL_NEEDS_ABORT,   /* fill: fail it on the wire (header then RESET) */
    BF_FETCH_NEEDS_PLAN,   /* standalone: the owning shard's admission owed */
    BF_FETCH_NEEDS_ABORT,  /* standalone: request-local RESET owed          */
} b_fetch_state_t;

typedef struct b_fetch {
    bool            used;
    /* A fill fetch stream owed to a downstream subscription rather than a
     * FETCH, written through open/write/end/abort_fill on sfill. Its shared
     * retrieval cursor draws on the core fetch and bind collector pools. */
    bool            fill;
    b_fetch_state_t state;
    moq_fetch_t     sfetch;      /* downstream session fetch handle          */
    moq_fill_t      sfill;       /* downstream session fill handle (fill)    */
    moqr_fetch_t    cfetch;      /* core fetch cursor                        */
    bool            eot;         /* plan.end_of_track -> accept_fetch cfg     */
    uint64_t        end_group;   /* plan End Location (raw wire) -> FETCH_OK  */
    uint64_t        end_object;
    /* fill: the instant after which a run of Locations the log does not hold
     * is reported as a Timed-Out gap instead of waited for (the request's
     * processing time plus its effective fill timeout). A FETCH has the same
     * deadline, from its own FILL_TIMEOUT: the most it waits for an Object
     * still arriving, and the instant from which the Locations it does not
     * serve are Timed-Out rather than Unknown where the draft has that
     * marker. */
    uint64_t        fill_deadline_us;
    /* FETCH: the draft's fetch stream has End of Timed-Out Range. */
    bool            fetch_timed_out_ok;
    /* FETCH: held at an Object still arriving, waiting until the deadline. */
    bool            fetch_held_pending;
    /* fill: the generation-pinned parent and remaining inclusive window.
     * Descending fills visit one group per window; source claims serve a
     * prefix within that window before local retrieval takes over. */
    moqr_sub_t      fill_rsub;
    moqr_track_t    fill_track;
    uint64_t        fill_track_gen;
    uint64_t        fill_start_group, fill_start_object;
    uint64_t        fill_end_group, fill_end_object;
    uint8_t         fill_priority;
    /* fill: the part of the range read through the binding's fill source
     * (fill_src_open) rather than the log, by its claim handle; the claimed
     * part ends at fill_src_end_*. fill_src_used: the source was consulted
     * and has finished its part, so the rest is the log's alone. */
    bool            fill_src_open;
    bool            fill_src_used;
    uint64_t        fill_src_handle;
    uint64_t        fill_src_end_group, fill_src_end_object;
    /* A standalone FETCH reads from its core retrieval owner (cfetch), or
     * from the owning shard (sa_remote, by sa_handle). */
    bool            standalone;
    bool            sa_remote;
    /* Shared FETCH/fill response state: immutable inclusive bounds and
     * delivery order, plus the next Location and completion flag. For a
     * fill, the frontier covers the current reader's part of its window.
     * Non-existent runs need an explicit end only before an unknown or
     * timed-out run; a following Object or final FIN implies them. */
    bool            response_done;
    bool            response_ne_pending;
    moqr_group_order_t response_order;
    uint64_t        sa_handle;
    uint64_t        sa_abort_code;
    moqr_retrieve_location_t response_start, response_end;
    moqr_retrieve_location_t response_next, response_ne_end;
    moqr_retrieve_cursor_t retrieval;
    /* fill: the reset code its abort sends; zero is this relay's
     * INTERNAL_ERROR (bind_fill_fail_code). */
    uint64_t        fill_abort_code;
    bool            fill_abort_code_set;
} b_fetch_t;

/* A downstream request terminal the session could not take yet
 * (WOULD_BLOCK on its moq_session_reject_* or, for a namespace or namespace
 * subscription the core has already admitted, its moq_session_accept_*):
 * the request is still pending in the session and its event has been
 * consumed, so the binding owns the terminal, with its error code and Retry
 * Interval, until it commits, the peer cancels, or the connection goes away.
 * A held acceptance whose request is gone before it commits undoes the core
 * state it answered for (unannounce, or namespace unsubscribe).
 *
 * Every terminal is produced while handling a polled request event or a
 * resolved authorization, and each owes exactly one terminal: an
 * acceptance, a refusal, or (B_ABORT_FILL) the reset of a fill the relay
 * fails on its stream alone. Forwarded-fetch terminals are not held here:
 * each forwarding transaction holds its own. One table per connection, sized so every held
 * terminal has an entry:
 *   - one poll of headroom: the pump polls a connection's next event only
 *     while a poll's worth of entries is free and unreserved, so every
 *     request it consumes can be owned whatever the session's own request
 *     capacity is;
 *   - one entry reserved per parked authorization of the connection (the
 *     core's max_parked bounds them), for the terminal its resolution owes.
 * A connection whose free entries are all held leaves its remaining events
 * queued in the session (connection-local backpressure) until a held
 * terminal commits. */
#define B_REFUSE_SUBSCRIBE_TRACKS MOQR_AUTH_ACTION__COUNT   /* no auth action */
#define B_ACCEPT_NAMESPACE        (MOQR_AUTH_ACTION__COUNT + 1u)
#define B_ACCEPT_NS_SUB           (MOQR_AUTH_ACTION__COUNT + 2u)
#define B_ABORT_FILL              (MOQR_AUTH_ACTION__COUNT + 3u)   /* handle: moq_fill_t,
                                                         * error_code: reset code */

typedef struct b_refusal {
    bool               used;
    bool               can_retry;
    bool               undo;         /* acceptance gone: core state to undo */
    moqr_auth_action_t kind;         /* MOQR_AUTH_*, or B_* above          */
    uint64_t           handle;       /* generation-qualified session handle */
    uint64_t           error_code;
    uint64_t           retry_ms;     /* Retry Interval when can_retry       */
} b_refusal_t;

typedef struct b_usub {
    bool               used;
    moqr_track_t       track;
    uint64_t           track_gen;
    moq_subscription_t ssub;    /* the relay's upstream subscription      */
    /* The Track was found malformed and the relay owes the upstream
     * cancellation (draft-22 §12.1); the session refused it so far. Retried
     * every pump until it commits or the subscription is gone; nothing more
     * is ingested from it meanwhile. */
    bool               cancel_owed;
    /* Owned copy of the track key this subscription was issued for (one
     * buffer: namespace parts then name), so a forwarded Joining FETCH can
     * name the same track upstream without consulting the core. has_key is
     * false when the copy could not be retained: live delivery is unaffected
     * and the subscription is simply not a forwarding source. */
    bool               has_key;
    uint8_t           *key;
    size_t             key_len;
    uint32_t           ns_count;
    moq_bytes_t        parts[32];
    moq_bytes_t        name;
} b_usub_t;

/* -- Forwarded Joining FETCH: bounded pre-acceptance collection ------------ *
 * A downstream Joining FETCH on a subscription this relay serves from an
 * upstream origin is answered by ONE standalone upstream FETCH carrying the
 * same already-normalized absolute range (the session computed it from the
 * joined subscription's saved Largest; it is never widened). The upstream
 * response is collected in full -- in arrival order, within fixed item counts
 * and the bind-wide byte pool -- and only after its COMPLETE is the downstream
 * fetch accepted and the collection replayed verbatim (FETCH_OK end fields and
 * Track Properties, each object's identity/priority/payload/properties, each
 * gap marker, then FIN). Every failure before that point -- upstream error,
 * reset, source loss or replacement, overflow, allocation refusal, downstream
 * cancellation -- is answered BEFORE FETCH_OK with a request-local rejection
 * (never a truncated success); after FETCH_OK a hard local write failure is
 * answered with the request-local abort, never a connection close. Forwarded
 * objects never enter the live log, and the joined subscription is consulted
 * exactly once (identity resolution): a later UNSUBSCRIBE does not touch the
 * fetch. Transactions live in ONE bind-wide pool sized by the core's fetch
 * pool, outside every connection's tables; both sides are addressed by
 * (cookie, binding, handle), so a reused connection slot or request slot can
 * never match an older transaction, and an event that still names a retired
 * handle is released untouched by the pump's event cleanup. */
#define BF_FWD_MAX_OBJECTS 64u
#define BF_FWD_MAX_GAPS    16u
#define BF_FWD_MAX_ITEMS   (BF_FWD_MAX_OBJECTS + BF_FWD_MAX_GAPS)
/* One owned track-key copy (namespace parts + name) can never exceed the
 * core's shared full-track-name cap. */
#define BF_FWD_KEY_MAX     4096u
/* Data-stream reset code for the request-local abort of an accepted forwarded
 * fetch whose remaining output cannot be encoded: INTERNAL_ERROR, 0x0 in both
 * drafts' stream-reset registries (draft-16 10.4.3, draft-18 3.3.3). */
#define BIND_RESET_INTERNAL 0x0u

/* Upstream (fetcher-role) side of a transaction. */
#define FW_U_UNSENT     0u   /* admitted; upstream FETCH not yet accepted by the session */
#define FW_U_PENDING    1u   /* sent; collecting */
#define FW_U_CANCELLING 2u   /* a cancel is owed upstream; retried until it commits */
#define FW_U_TERMINAL   3u   /* complete / error / reset / lost / cancelled */
/* Downstream (publisher-role) side. */
#define FW_D_PENDING    0u   /* request admitted, nothing sent */
#define FW_D_ACCEPTING  1u   /* FETCH_OK owed */
#define FW_D_STREAMING  2u   /* replaying the collection, then FIN */
#define FW_D_ABORTING   3u   /* request-local abort owed */
#define FW_D_TERMINAL   4u
/* Why the upstream side ended. */
#define FW_END_NONE      0u
#define FW_END_COMPLETE  1u
#define FW_END_ERROR     2u   /* err_* carry the copied REQUEST_ERROR */
#define FW_END_RESET     3u
#define FW_END_LOST      4u   /* upstream connection gone / replaced */
#define FW_END_CANCELLED 5u   /* cancelled by this relay (downstream gone) */
#define FW_END_OVERFLOW  6u   /* item count / byte pool / allocation refusal */
#define FW_END_MISMATCH  7u   /* upstream answered outside the pinned range */
#define FW_END_TIMEOUT   8u   /* private fill: preserve received complete items */

#define FW_CONSUMER_JOIN 0u
#define FW_CONSUMER_FILL 1u

typedef struct b_fwd_item {
    bool                   is_gap;
    bool                   datagram;
    uint8_t                publisher_priority;
    uint8_t                range_kind;   /* moq_fetch_range_kind_t, gaps only */
    uint64_t               group, subgroup, object;
    moq_rcbuf_t           *payload;      /* OWNED ref stolen from the event */
    moq_rcbuf_t           *properties;   /* OWNED ref or NULL               */
    uint64_t               charged;      /* bytes charged to the pool       */
} b_fwd_item_t;

typedef struct b_fwd {
    bool           used;
    uint8_t        u, d, end;
    bool           ufetch_live;   /* ufetch names a request the session holds */
    /* downstream (this relay is the publisher) */
    uint64_t       d_cookie;
    moqr_binding_t d_binding;
    moq_fetch_t    sfetch;
    /* upstream (this relay is the fetcher) */
    uint64_t       u_cookie;
    moqr_binding_t u_binding;
    moq_fetch_t    ufetch;
    moqr_track_t   track;
    uint64_t       track_gen;
    /* the request: normalized absolute range, forwarded verbatim */
    uint64_t       start_group, start_object, end_group, end_object;
    bool           has_prio;
    uint8_t        prio;
    /* owned track key copy (charged to the pool) */
    uint8_t       *key;
    size_t         key_len;
    uint32_t       ns_count;
    moq_bytes_t    parts[32];
    moq_bytes_t    name;
    /* the collected response */
    bool           ok_seen, ok_eot;
    uint64_t       ok_end_group, ok_end_object;
    uint8_t       *props;          /* owned copy of Track Properties */
    size_t         props_len;
    moq_request_error_t err_code;
    bool           err_can_retry;
    uint64_t       err_retry_after_ms;
    uint8_t       *reason;         /* owned copy of the error reason */
    size_t         reason_len;
    uint32_t       n_items, n_objects, n_gaps, next_write;
    uint64_t       charged;        /* every pool byte this transaction holds */
    /* Private retrieval consumer. Legacy Joining FETCH leaves this zero and
     * retains its collect-before-ACK policy. No cursor/table-row pointers. */
    uint8_t        consumer;
    uint64_t       generation;
    moqr_retrieve_request_t retrieval;
    moqr_retrieve_parent_t source_parent;
    moqr_retrieve_error_kind_t failure;
    moqr_result_t local_result;
    moq_result_t session_result;
    uint64_t       reset_code;
    moq_request_terminal_cause_t reset_cause;
    bool           reset_data_stream, reset_stop_sending;
    /* The Track turned out malformed (draft-22 §12.1): the downstream FETCH
     * is refused with MALFORMED_TRACK before FETCH_OK, or reset with
     * malformed_wire after it, whatever was collected; never FINed. */
    bool           malformed;
    uint64_t       malformed_wire;
    b_fwd_item_t   items[BF_FWD_MAX_ITEMS];
} b_fwd_t;

/* One pending cached object's exact-sized copies. Slots are bounded by the
 * core fetch limit, independent of upstream collectors, and remain bind-owned
 * when a replaced core cursor no longer pins its old retained generation. */
typedef struct b_retrieve_hold {
    uint64_t generation, charged;
    moq_rcbuf_t *payload, *properties;
} b_retrieve_hold_t;

/* A copy of a retained Object that arrives in chunks: equal metadata and
 * declared length are not equal bytes (draft-22 §7.1), so each chunk is
 * compared with the retained payload at its offset as it arrives, through the
 * copy's terminal. While the owner exists the copy's chunks never reach the
 * retained record. Bounded: a copy that finds no free owner is ignored
 * uncompared (§7.1: a relay "MAY ignore subsequently received Objects"); its
 * chunks then meet a record that is no longer OPEN and change nothing. */
#define BIND_DUP_OWNERS 8u
typedef struct b_dup_owner {
    bool         used;
    bool         differs;      /* a compared byte differed               */
    uint32_t     conn;         /* connection slot of the copy's source   */
    uint64_t     src;          /* its subscription or publication handle */
    moqr_track_t track;
    uint64_t     group, subgroup, object;
    uint64_t     offset;       /* payload bytes of the copy compared     */
} b_dup_owner_t;

typedef struct b_pub {
    bool         used;
    uint64_t     pub_raw;       /* peer-initiated publish handle          */
    moqr_track_t track;
    uint64_t     track_gen;
    /* As b_usub_t.cancel_owed, for a PUBLISH the relay accepted: the
     * cancellation with cancel_code is owed until it commits. */
    bool         cancel_owed;
    uint64_t     cancel_code;
} b_pub_t;

typedef struct b_ann {
    bool         used;
    uint64_t     ann_raw;       /* peer PUBLISH_NAMESPACE handle          */
    moqr_ns_t    ns;
    moq_bytes_t  parts[32];
    uint8_t     *bytes;
    size_t       bytes_len;
} b_ann_t;

/* A namespace update held for retry. draft-18 Section 6.2 makes the NAMESPACE
 * owed to a matching prefix subscriber a MUST, so a blocked control path may
 * delay it but must never discard it. The intent's ns_parts borrow
 * core-interned storage (relay.h:102), so the suffix is deep-copied here — the
 * same ownership shape ann_store() uses for an accepted announce. */
/* Payload sidecar for one pending-ring slot. Indexed 1:1 by ring slot, so the
 * ring stays the single ordering authority: a namespace update parks in the
 * SAME FIFO as every scalar terminal and cannot overtake or be overtaken.
 * Only slots whose parked intent is NS_FOUND / NS_GONE carry bytes. */
typedef struct b_nsu {
    bool     used;        /* occupancy is explicit: a valid suffix can carry
                           * zero bytes because it has NO fields at all --
                           * a present field always has at least one byte --
                           * so a NULL bytes pointer does NOT mean "free"  */
    uint8_t *bytes;       /* the parked suffix's bytes; parts ride in the
                           * intent's own inline ns_parts array            */
    size_t   bytes_len;
} b_nsu_t;

/* One held answer to a namespace update. */
typedef struct b_nsans {
    bool             used;
    bool             reject;
    bool             initial;     /* the request's own answer, not an
                                   * update's (a track subscription)      */
    uint8_t          family;      /* B_NSU_*                              */
    uint32_t         conn;        /* connection slot                      */
    uint64_t         handle;      /* the updated request                  */
    uint64_t         update_id;
    uint64_t         error_code;  /* reject only                          */
    moqr_grant_res_t gres;        /* an announcement's new grant, if any  */
} b_nsans_t;

/* The pool every held answer fits in at once: at most one update is owed per
 * request, and the requests that can be updated are bounded by the core's
 * namespace subscriptions, track subscriptions and announced namespaces;
 * each track subscription may also owe its own first answer. */
static uint32_t
bind_nsans_cap(const moqr_core_limits_t *lim)
{
    uint64_t n = 3u * (uint64_t)lim->max_ns_subs + (uint64_t)lim->max_ns_nodes;
    return n > UINT32_MAX ? UINT32_MAX : (uint32_t)n;
}

struct moqr_bind_meta_blob {
    moqr_bind_t *bind;
    size_t refs;
    size_t allocation;
    uint8_t data[];
};

typedef struct b_meta_entry {
    moqr_track_t track;
    uint64_t track_gen;
    moqr_bind_meta_ref_t latest;
} b_meta_entry_t;

enum { B_META_PUB_AUTH = 1, B_META_PUB_CORE };
typedef struct b_meta_publish {
    uint8_t state;
    moqr_binding_t binding;
    uint64_t cookie, pub, ticket;
    moqr_bind_meta_ref_t snapshot;
} b_meta_publish_t;

/* Delivery-park reasons: why this connection's last delivery pass
 * blocked, deciding its recovery gate. BIND_SG never parks — its recovery is
 * the explicit re-arm at every subgroup-slot release site. */
#define B_PARK_NONE       0u
#define B_PARK_ACTION_CAP 1u   /* session action queue full: capacity gate  */
#define B_PARK_SESSION_SG 2u   /* session subgroup pool: one try per pump   */

#define B_DRAFT_16 0u
#define B_DRAFT_18 1u
#define B_DRAFT_22 2u

typedef struct b_conn {
    bool           used;
    bool           closed;
    bool           detach_pending;   /* close blocked on intent space   */
    bool           bind_sg_waiting;  /* a delivery blocked on this conn's
                                      * subgroup-slot pool (B_DELIVER_BLOCKED_
                                      * BIND_SG): the ONLY state that makes a
                                      * later slot release re-arm the ready
                                      * bit. Cleared when the conn next drains
                                      * or on lifecycle reset, so ordinary
                                      * seal-close traffic (no delivery
                                      * waiting) re-arms nothing. Rides the
                                      * bool padding — no size cost.     */
    uint8_t        park_reason;      /* B_PARK_*                        */
    /* This connection's negotiated draft, held as a compact tag: the status
     * and reset registries disagree on several codepoints, so every terminal
     * is encoded for the peer that reads it. Rides the bool padding — no size
     * cost. */
    uint8_t        wire_draft;       /* B_DRAFT_*                       */
    size_t         park_cap;         /* ACTION_CAP: capacity floor when
                                      * parked; retry only above it     */
#ifdef MOQR_BIND_TESTING
    /* Test-only blockage attribution, PER CONNECTION (this conn's lane
     * thread is the only writer): how many delivery passes for THIS conn
     * ended in each park/refusal class (0 action-cap, 1 session-sg,
     * 2 bind-sg). Snapshot only from the owning lane or after the lane
     * threads are joined. */
    uint64_t       dbg_blocked[3];
    /* Test-only BIND_SG attribution: the TRACK whose delivery hit this
     * conn's subgroup-pool refusal. The track — not the delivery's
     * sub_cookie, which is the DOWNSTREAM subscription handle, a different
     * id space from the demand — is the join key the shard layer resolves
     * to a demand id (the demand table is keyed by track). The FIRST
     * refusal latches it; a later refusal for a DIFFERENT track (hence a
     * different demand — demands are per track) flips ambiguous, and an
     * unresolvable sub handle also flips ambiguous, so the SGDIAG→SEALLOG
     * join fails closed rather than attributing to a guess. Cleared on
     * slot claim/reuse alongside dbg_blocked. */
    uint64_t       dbg_bind_sg_track;      /* moqr_track_t raw            */
    uint64_t       dbg_bind_sg_track_gen;
    bool           dbg_bind_sg_track_set;
    bool           dbg_bind_sg_ambiguous;
#endif
    moq_session_t *session;
    moqr_binding_t binding;
    b_dsub_t      *dsubs;
    b_sg_t        *sgs;
    b_usub_t      *usubs;
    b_pub_t       *pubs;
    b_ann_t       *anns;
    b_fetch_t     *fetches;
    b_refusal_t   *refusals;   /* held refusals, n_refusals entries      */
    uint32_t       refusals_reserved;   /* entries promised to parked auths */
    uint64_t       requests_seen;    /* inbound peer requests observed (d16
                                      * request-id consumption proxy)     */
} b_conn_t;

struct moqr_bind {
    moq_alloc_t  alloc;
    b_dgrant_t   dgrants[BIND_DGRANTS];
    moqr_core_t *core;
    /* The request namespace / name of the event being handled, copied out of
     * session event memory (see bind_stash_ftn). */
    moq_bytes_t  stash_parts[32];
    uint8_t      stash_bytes[MOQ_FULL_TRACK_NAME_MAX];
    uint32_t     max_conns, n_dsubs, n_sgs, n_usubs, n_pubs, n_anns, n_fetches;
    uint32_t     n_refusals;             /* see b_refusal_t                 */
    uint32_t     request_grant_window;   /* d16 MAX_REQUEST_ID grant step   */
    uint64_t     fill_wait_default_us;   /* fill without FILL_TIMEOUT       */
    uint64_t     fill_wait_max_us;       /* cap on a requested FILL_TIMEOUT */
    /* Intent router for shard-manager pseudo-binding cookies (NULL = none). */
    uint64_t                    router_cookie_base;
    moqr_bind_intent_router_fn  router;
    void                       *router_ctx;
    /* Fill source (moqr_bind_set_fill_source); has_fill_src = installed. */
    moqr_bind_fill_source_t     fill_src;
    bool                        has_fill_src;
    moqr_retrieve_source_peek_fn retrieve_source_peek;
    /* The shard runtime's standalone FETCH source (has_sa_src = installed). */
    moqr_retrieve_standalone_ops_t sa_src;
    bool                        has_sa_src;
    /* Set during a pump when any connection's delivery pass expired the
     * 256-delivery guard with work remaining; cleared at the next pump's
     * start. Read through moqr_bind_pump_budget_pending. */
    bool         budget_pending;
    bool         in_pump;   /* non-reentrant: one owner thread pumps a bind */
    bool         intent_drain_active;
#ifdef MOQR_BIND_TESTING
    uint64_t     dbg_intent_retry_suppressed;
#endif
    b_conn_t    *conns;
    /* Phase F delivery scheduling (bind is the only writer of both):
     * dl_ready is filled from moqr_core_drain_ready and self-rearms
     * (BUDGET/RETRY); dl_parked holds connections whose last delivery pass
     * blocked downstream, gated per park_reason. One bit per conn slot,
     * ceil(max_conns/64) words each. */
    uint64_t    *dl_ready;
    uint64_t    *dl_parked;
    moq_event_t *pump_events;
    moqr_revoked_grant_t *pump_revoked;
    moqr_intent_t *pump_intents;
    uint64_t     events_translated;
    uint64_t     deliveries_written;
    uint64_t     ingest_refusals;
    uint64_t     ingest_duplicates;   /* identical duplicates dropped     */
    b_dup_owner_t dup_owners[BIND_DUP_OWNERS];
    uint64_t     malformed_tracks;    /* Tracks failed as malformed       */
    uint64_t     session_errors;
    /* Held downstream update answers (b_dsub_t.upd_answer) as of the last
     * count; the pump scans for them only while this is non-zero and
     * recounts as it goes. */
    uint32_t     upd_answers_held;
    moqr_latency_hist_t forward_latency;
    /* Deferred-output ring: output intents whose session write hit WOULD_BLOCK,
     * awaiting retry. FIFO order is preserved so an ACCEPT can never be
     * overtaken by the SUB_DONE or object delivery that follows it. Two kinds
     * of entry live here, and nothing else:
     *   - scalar-safe kinds (bind_intent_deferrable), stored as polled; and
     *   - NS_FOUND / NS_GONE, stored only AFTER their borrowed suffix has been
     *     deep-copied into this slot's payload sidecar, which is what makes
     *     them safe to hold across pumps.
     * Every other borrowed-view kind is consume-in-pump: an intent whose
     * ns_parts/name views BORROW core-interned storage (relay.h:135) and is not
     * deep-copied cannot survive to the next pump, so UPSTREAM_SUBSCRIBE is
     * executed inline the moment it is polled and never parked.
     * Sized from the core's admission limits (bind_create) to comfortably hold
     * every scalar terminal that can be simultaneously in flight; a would-be
     * overflow is counted, never a silent drop. */
    moqr_intent_t *pending;
    uint32_t       pending_cap;
    uint32_t       pending_head;
    uint32_t       pending_count;
    uint32_t       pending_high_water;      /* max depth seen (telemetry)     */
    uint32_t       pending_nonscalar_blocked; /* borrowed kind hit defer: BUG */
    /* Joining FETCH and private fill collectors: one bind-wide pool (n_fwd =
     * resolved core max_fetches) and their aggregate transient byte pool. */
    b_fwd_t       *fwd;
    uint32_t       n_fwd;
    uint64_t       fwd_bytes_cap;
    uint64_t       fwd_bytes_used;
    uint64_t       fwd_generation; /* never wraps: refuse instead of aliasing */
    b_retrieve_hold_t *cached_holds; /* n_fwd slots; memory-only teardown */
    uint64_t       cached_generation;
#ifdef MOQR_BIND_TESTING
    moqr_bind_fwd_debug_t dbg_fwd;
#endif
    b_nsu_t       *nsu;        /* payload sidecar, 1:1 with pending[]        */
    uint32_t       nsu_cap;    /* == pending_cap, by construction            */
    uint32_t       nsu_high_water;
    uint64_t       nsu_retained;
    uint64_t       nsu_failed_closed;
    uint64_t       ordered_failed_closed;
    uint32_t       nsu_live;   /* OCCUPIED sidecar slots; a valid zero-byte
                                * suffix is occupied while owning no bytes  */
    /* Held answers to draft-22 namespace updates (bind_nsu_pump). */
    b_nsans_t     *nsans;
    uint32_t       nsans_cap;
    uint32_t       nsans_live;
    /* Bumped every time a purge REMOVES ordered-ring entries. A retry of the
     * ring head can detach its own connection (hard send failure), and detach
     * purges + compacts the ring underneath the caller -- possibly moving a
     * DIFFERENT connection's entry into the head slot. The caller compares
     * this stamp across the retry so it never pops a slot it did not consume.
     * A stamp, not a count: purge can remove several entries at once. */
    uint32_t       pending_purge_epoch;
    b_meta_entry_t *metadata;
    uint32_t metadata_cap;
    moqr_bind_meta_ref_t *pending_metadata;
    b_meta_publish_t *metadata_publishes;
    uint32_t metadata_publish_cap;
    uint64_t metadata_bytes, metadata_revision;
    const moqr_intent_t *metadata_executing_intent;
    const moqr_bind_meta_ref_t *metadata_executing;
    moqr_bind_meta_changed_fn metadata_changed;
    void *metadata_changed_ctx;
    bool metadata_notifying;
};

static bool bind_execute_intents(moqr_bind_t *b, uint64_t now_us);

moqr_result_t
moqr_bind_meta_retain(moqr_bind_t *b, const moqr_bind_meta_ref_t *ref,
                      moqr_bind_meta_ref_t *out)
{
    if (b == NULL || ref == NULL || out == NULL || out == ref ||
        out->storage != NULL) {
        return MOQR_ERR_INVAL;
    }
    if (ref->storage != NULL) {
        if (ref->storage->bind != b || ref->storage->refs == SIZE_MAX) {
            return MOQR_ERR_INVAL;
        }
        ref->storage->refs++;
    }
    *out = *ref;
    return MOQR_OK;
}

void
moqr_bind_meta_release(moqr_bind_t *b, moqr_bind_meta_ref_t *ref)
{
    if (b == NULL || ref == NULL) return;
    struct moqr_bind_meta_blob *p = ref->storage;
    if (p != NULL && p->bind != b) return;
    if (p != NULL && --p->refs == 0) {
        size_t n = p->allocation;
        b->metadata_bytes -= n;
        b->alloc.free(p, n, b->alloc.ctx);
    }
    memset(ref, 0, sizeof(*ref));
}

moqr_result_t
moqr_bind_meta_retain_executing(moqr_bind_t *b, const moqr_intent_t *it,
                               moqr_bind_meta_ref_t *out)
{
    if (b == NULL || it == NULL || b->metadata_executing_intent != it ||
        b->metadata_executing == NULL || it->kind != MOQR_INTENT_ACCEPT_SUB) {
        return MOQR_ERR_WRONG_STATE;
    }
    return moqr_bind_meta_retain(b, b->metadata_executing, out);
}

moqr_result_t
moqr_bind_meta_latest(moqr_bind_t *b, moqr_track_t track,
                      moqr_bind_meta_ref_t *out)
{
    if (b == NULL || out == NULL || out->storage != NULL ||
        moq_handle_pool_tag(track._opaque) != MOQR_HANDLE_POOL_TRACK) {
        return MOQR_ERR_INVAL;
    }
    uint32_t slot = moq_handle_slot(track._opaque);
    if (slot >= b->metadata_cap) return MOQR_ERR_STALE_HANDLE;
    b_meta_entry_t *e = &b->metadata[slot];
    if (!moqr_track_eq(e->track, track)) {
        memset(out, 0, sizeof(*out));
        return MOQR_OK;
    }
    return moqr_bind_meta_retain(b, &e->latest, out);
}

moqr_result_t
moqr_bind_meta_set_observer(moqr_bind_t *b, moqr_bind_meta_changed_fn changed,
                            void *ctx)
{
    if (b == NULL) return MOQR_ERR_INVAL;
    if (b->in_pump || b->intent_drain_active || b->metadata_notifying ||
        b->metadata_changed != NULL) return MOQR_ERR_WRONG_STATE;
    b->metadata_changed = changed;
    b->metadata_changed_ctx = ctx;
    return MOQR_OK;
}

static void
bind_meta_sweep(moqr_bind_t *b)
{
    for (uint32_t i = 0; i < b->metadata_cap; i++) {
        b_meta_entry_t *e = &b->metadata[i];
        if (moqr_track_eq(e->track, MOQR_TRACK_INVALID)) continue;
        uint64_t generation;
        if (moqr_core_track_current_generation(b->core, e->track, &generation) !=
            MOQR_ERR_STALE_HANDLE) continue;
        /* Drop only the directory ref. Reply/manager pins keep their charge. */
        moqr_bind_meta_release(b, &e->latest);
        memset(e, 0, sizeof(*e));
    }
}

static moqr_result_t
bind_meta_copy(moqr_bind_t *b, const moqr_bind_meta_ref_t *input,
                moqr_bind_meta_ref_t *out)
{
    *out = *input;
    out->storage = NULL;
    out->revision = 0;
    out->bytes = (moq_bytes_t){ NULL, 0 };
    out->state = MOQR_BIND_META_UNAVAILABLE;
    if (input->state == MOQR_BIND_META_UNAVAILABLE) return MOQR_OK;
    if ((input->state != MOQR_BIND_META_EMPTY &&
         input->state != MOQR_BIND_META_PRESENT) ||
        (input->state == MOQR_BIND_META_EMPTY && input->bytes.len != 0) ||
        (input->state == MOQR_BIND_META_PRESENT &&
         (input->bytes.len == 0 || input->bytes.data == NULL))) {
        return MOQR_ERR_INVAL;
    }
    if (input->bytes.len > MOQR_BIND_META_SET_MAX) return MOQR_ERR_CAPACITY;
    bind_meta_sweep(b);
    size_t n = sizeof(struct moqr_bind_meta_blob) + input->bytes.len;
    if (n < input->bytes.len || n > MOQR_BIND_META_BYTES_MAX ||
        b->metadata_bytes > MOQR_BIND_META_BYTES_MAX - n) {
        return MOQR_ERR_CAPACITY;
    }
    b->metadata_bytes += n;
    struct moqr_bind_meta_blob *p = b->alloc.alloc(n, b->alloc.ctx);
    if (p == NULL) {
        b->metadata_bytes -= n;
        return MOQR_ERR_NOMEM;
    }
    p->bind = b;
    p->refs = 1;
    p->allocation = n;
    if (input->bytes.len != 0) memcpy(p->data, input->bytes.data, input->bytes.len);
    out->state = input->state;
    out->storage = p;
    out->bytes = (moq_bytes_t){ p->data, input->bytes.len };
    return MOQR_OK;
}

static moqr_result_t
bind_meta_track_current(moqr_bind_t *b, moqr_track_t track, uint64_t gen)
{
    bool has_largest;
    uint64_t g, o;
    return moqr_core_track_largest(b->core, track, gen, &has_largest, &g, &o);
}

/* Takes the allocation already reserved by ingress, without allocating while
 * dispatching an ACCEPT_PUBLISH or retaining a routed reply. */
/* The core enforces the source's MAX_CACHE_DURATION on every Object of the
 * track it later starts downstream. */
static void
bind_meta_apply_cache_duration(moqr_bind_t *b, moqr_track_t track,
                               const moqr_bind_meta_ref_t *meta)
{
    /* MAX_CACHE_DURATION, in the recorded set's own draft (draft-22 §10.3,
     * draft-18 §12.3, draft-16 §11.1.1). */
    bool has = false;
    uint64_t ms = 0;
    if (meta->state == MOQR_BIND_META_PRESENT && meta->bytes.len > 0) {
        has = moq_core_internal_track_max_cache_duration(
            meta->wire_version, meta->bytes.data, meta->bytes.len, &ms);
    }
    (void)moqr_core_internal_set_max_cache_duration(b->core, track, has, ms);
}

static void
bind_meta_commit(moqr_bind_t *b, moqr_track_t track, uint64_t gen,
                 moqr_bind_meta_ref_t *owned)
{
    uint32_t slot = moq_handle_slot(track._opaque);
    b_meta_entry_t *e = &b->metadata[slot];
    moqr_bind_meta_release(b, &e->latest);
    e->track = track;
    e->track_gen = gen;
    if (b->metadata_revision >= UINT64_MAX - 1u) {
        /* The final revision is permanently unavailable, never a present
         * set that a later invalidation could not supersede. */
        moqr_bind_meta_release(b, owned);
        b->metadata_revision = UINT64_MAX;
    } else {
        b->metadata_revision++;
    }
    owned->revision = b->metadata_revision;
    e->latest = *owned;
    memset(owned, 0, sizeof(*owned));
    bind_meta_apply_cache_duration(b, track, &e->latest);
    if (b->metadata_changed != NULL) {
        b->metadata_notifying = true;
        b->metadata_changed(b->metadata_changed_ctx, track, gen, &e->latest);
        b->metadata_notifying = false;
    }
}

moqr_result_t
moqr_bind_meta_replace(moqr_bind_t *b, moqr_track_t track, uint64_t gen,
                       const moqr_bind_meta_ref_t *input,
                       uint64_t now_us, uint64_t *out_revision)
{
    if (out_revision != NULL) *out_revision = 0;
    if (b == NULL || input == NULL) return MOQR_ERR_INVAL;
    if (b->intent_drain_active || b->metadata_notifying) return MOQR_ERR_WRONG_STATE;
    moqr_result_t rc = bind_meta_track_current(b, track, gen);
    if (rc != MOQR_OK) return rc;
    bool managed = false;
    rc = moqr_core_track_metadata_managed(b->core, track, &managed);
    if (rc != MOQR_OK) return rc;
    if (!managed) return MOQR_ERR_WRONG_STATE;
    moqr_bind_meta_ref_t owned;
    memset(&owned, 0, sizeof(owned));
    rc = bind_meta_copy(b, input, &owned);
    if (rc == MOQR_ERR_INVAL) return rc;
    /* Copy first: the drain can advance sessions and invalidate event scratch.
     * Replies already owed retain A before the directory switches to B. */
    (void)bind_execute_intents(b, now_us);
    moqr_result_t live = bind_meta_track_current(b, track, gen);
    if (live != MOQR_OK) {
        moqr_bind_meta_release(b, &owned);
        return live;
    }
    bind_meta_commit(b, track, gen, &owned);
    if (out_revision != NULL) {
        *out_revision = b->metadata[moq_handle_slot(track._opaque)].latest.revision;
    }
    return rc;
}

static bool
bind_meta_publish_count(const moqr_core_limits_t *lim,
                        const moqr_bind_limits_t *bl, uint32_t *out)
{
    uint64_t n = (uint64_t)lim->max_parked + bl->pending_cap + BIND_PUMP_EVENT_POLL;
    if (n > UINT32_MAX || n > SIZE_MAX / sizeof(b_meta_publish_t) ||
        (lim->max_tracks != 0 &&
         sizeof(b_meta_entry_t) > SIZE_MAX / lim->max_tracks) ||
        (bl->pending_cap != 0 &&
         sizeof(moqr_bind_meta_ref_t) > SIZE_MAX / bl->pending_cap)) {
        return false;
    }
    *out = (uint32_t)n;
    return true;
}

#define BIND_CFG_HAS(cfg, field)                     \
    (offsetof(moqr_bind_cfg_t, field) +              \
         sizeof(((moqr_bind_cfg_t *)0)->field) <= (cfg)->struct_size)

void
moqr_bind_cfg_init_sized(moqr_bind_cfg_t *cfg, size_t cfg_size,
                         const moq_alloc_t *alloc)
{
    if (cfg == NULL || cfg_size < sizeof(uint32_t)) {
        return;
    }
    size_t n = cfg_size < sizeof(moqr_bind_cfg_t) ? cfg_size
                                                  : sizeof(moqr_bind_cfg_t);
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (offsetof(moqr_bind_cfg_t, alloc) +
            sizeof(((moqr_bind_cfg_t *)0)->alloc) <= n) {
        cfg->alloc = alloc;
    }
}

/* Whole 64-bit words for one scheduling bit per conn slot — widened before
 * the +63 round-up so the uint32_t maximum cannot wrap (mirrors the core's
 * ready_word_count; the two layers size independent pools). */
static uint32_t
bind_dl_word_count(uint32_t max_conns)
{
    return (uint32_t)(((uint64_t)max_conns + 63u) >> 6);
}

static void
bind_dl_set(uint64_t *words, uint32_t slot)
{
    words[slot >> 6] |= 1ull << (slot & 63u);
}

static void
bind_dl_clear(uint64_t *words, uint32_t slot)
{
    words[slot >> 6] &= ~(1ull << (slot & 63u));
}

static bool
bind_dl_test(const uint64_t *words, uint32_t slot)
{
    return (words[slot >> 6] & (1ull << (slot & 63u))) != 0;
}

/* Re-arm a connection's delivery readiness from a bind-local recovery edge
 * (a subgroup-slot release on a still-used, still-open connection). Spurious
 * re-arms cost one empty probe and dedup in the bitset; teardown paths CLEAR
 * scheduling state instead — a dead or reused slot must never re-arm itself. */
static void
bind_dl_rearm(moqr_bind_t *b, const b_conn_t *cn)
{
    if (cn->used && !cn->closed) {
        bind_dl_set(b->dl_ready, (uint32_t)(cn - b->conns));
    }
}

/* Re-arm on a freed subgroup-pool slot ONLY when a delivery is actually waiting
 * on that pool (bind_sg_waiting, set by the last pass's B_DELIVER_BLOCKED_BIND_SG
 * and cleared by any other outcome). Ordinary seal-close / reset traffic, where
 * nothing is pool-blocked, re-arms nothing — so a fully-draining pass leaves the
 * ready set empty and earns no spurious self-wake. */
static void
bind_dl_rearm_on_slot_release(moqr_bind_t *b, const b_conn_t *cn)
{
    if (cn->bind_sg_waiting) {
        bind_dl_rearm(b, cn);
    }
}

static void
bind_dl_clear_conn(moqr_bind_t *b, b_conn_t *cn)
{
    uint32_t slot = (uint32_t)(cn - b->conns);
    bind_dl_clear(b->dl_ready, slot);
    bind_dl_clear(b->dl_parked, slot);
    cn->park_reason = B_PARK_NONE;
    cn->park_cap = 0;
    cn->bind_sg_waiting = false;
}

#ifdef MOQR_BIND_TESTING
#include <stdatomic.h>
/* Test-only probe gauge (compiled solely into the test-internals build):
 * one count per moqr_core_next_delivery call issued by the delivery half.
 * ATOMIC: multi-lane fixtures increment from every lane thread. */
static _Atomic uint64_t bind_dbg_probes;
uint64_t
moqr_bind_debug_delivery_probes(void)
{
    return atomic_load(&bind_dbg_probes);
}

/* Test-only fault injection: fail the Nth delivery confirmation from now
 * (1 = the very next one) as a consistency error. */
static _Atomic int bind_dbg_fail_confirms;
void
moqr_bind_debug_fail_confirm(int nth)
{
    atomic_store(&bind_dbg_fail_confirms, nth);
}

/* Test-only fault injection: fail the Nth SUB_DONE subgroup reset from now as
 * a hard (non-WOULD_BLOCK) error without calling the session, so the
 * production close -> detach -> purge path runs from inside an ordered-ring
 * retry. */
static _Atomic int bind_dbg_fail_sg_resets;
/* Test-only: the Location Filter kind the binding reads on the next
 * subscription update that carries a replacement filter, in place of the
 * session's. 0 = none pending. Stands in for a peer that sends a form this
 * library's own subscriber API cannot express on an update. */
static _Atomic uint32_t bind_dbg_update_filter_kind;
/* Test-only: the next N downstream update answers observe WOULD_BLOCK
 * without calling the session, so the hold-and-retry path runs against an
 * answer that was never given. */
static _Atomic int bind_dbg_block_update_answers;
static _Atomic int bind_dbg_fail_nsu_stores;
static _Atomic int bind_dbg_ring_full_n;
static _Atomic int bind_dbg_ring_full_nsu;
static moqr_bind_debug_detach_pending_fn bind_dbg_detach_pending_fn;
static void *bind_dbg_detach_pending_ctx;
void
moqr_bind_debug_on_detach_pending(moqr_bind_debug_detach_pending_fn fn,
                                  void *ctx)
{
    bind_dbg_detach_pending_fn = fn;
    bind_dbg_detach_pending_ctx = ctx;
}
void
moqr_bind_debug_ring_full(int nth, bool is_nsu)
{
    atomic_store(&bind_dbg_ring_full_nsu, is_nsu ? 1 : 0);
    atomic_store(&bind_dbg_ring_full_n, nth);
}
void
moqr_bind_debug_fail_nsu_store(int nth)
{
    atomic_store(&bind_dbg_fail_nsu_stores, nth);
}
void
moqr_bind_debug_fail_sg_reset(int nth)
{
    atomic_store(&bind_dbg_fail_sg_resets, nth);
}
void
moqr_bind_debug_update_filter_kind(uint32_t kind)
{
    atomic_store(&bind_dbg_update_filter_kind, kind);
}
void
moqr_bind_debug_block_update_answers(int n)
{
    atomic_store(&bind_dbg_block_update_answers, n);
}

/* Test-only: the next N upstream cancellations a malformed Track owes
 * observe WOULD_BLOCK without calling the session, so the owed-cancel retry
 * runs against a cancellation that was never sent. */
static _Atomic int bind_dbg_block_upstream_cancels;
void
moqr_bind_debug_block_upstream_cancels(int n)
{
    atomic_store(&bind_dbg_block_upstream_cancels, n);
}

/* Test-only fault injection: make the Nth next retry of a held acceptance
 * observe its request as already retired (without calling the session), so
 * the production undo arm runs. */
static _Atomic int bind_dbg_accept_gone;
void
moqr_bind_debug_accept_gone(int nth)
{
    atomic_store(&bind_dbg_accept_gone, nth);
}

/* Test-only: the next N upstream cancellations a malformed Track owes fail
 * with MOQ_ERR_BUFFER (a message that can never fit) without calling the
 * session, so the hard-failure arm runs against a cancellation that can
 * never be made. */
static _Atomic int bind_dbg_fail_upstream_cancels;
int
moqr_bind_debug_fail_upstream_cancels(int n)
{
    return atomic_exchange(&bind_dbg_fail_upstream_cancels, n);
}

/* Test-only fault injection: make the next N downstream SUB_DONE terminal
 * writes observe WOULD_BLOCK without calling the session, so the production
 * defer-and-retry arm runs against a terminal that was never sent. */
static _Atomic int bind_dbg_block_sub_dones;
void
moqr_bind_debug_block_sub_done(int n)
{
    atomic_store(&bind_dbg_block_sub_dones, n);
}
int
moqr_bind_debug_sub_done_blocks_left(void)
{
    return atomic_load(&bind_dbg_block_sub_dones);
}

/* Test-only fault injection: refuse the Nth SUB_DONE terminal write from now
 * with WRONG_STATE without calling the session -- a refusal that queued
 * nothing -- restricted to subscriptions (kind 1), publications (kind 2) or
 * either (kind 0). Proves the binding's handling of a hard refusal; it says
 * nothing about which real states produce one. */
static _Atomic int bind_dbg_fail_sub_dones;
static _Atomic int bind_dbg_fail_sub_done_kind;
void
moqr_bind_debug_fail_sub_done(int nth, int kind)
{
    atomic_store(&bind_dbg_fail_sub_done_kind, kind);
    atomic_store(&bind_dbg_fail_sub_dones, nth);
}

/* Test-only fault injection: make the next N subgroup resets a SUB_DONE owes
 * observe WOULD_BLOCK without calling the session, so the whole terminal is
 * deferred with that stream's obligation still recorded and the retry must
 * reset it before the terminal goes out. */
static _Atomic int bind_dbg_block_sg_resets;
void
moqr_bind_debug_block_sg_reset(int n)
{
    atomic_store(&bind_dbg_block_sg_resets, n);
}
int
moqr_bind_debug_sg_reset_blocks_left(void)
{
    return atomic_load(&bind_dbg_block_sg_resets);
}

/* Test-only probe-result injection: force the Nth next_delivery probe from
 * now to report `result` without calling the core — pins the CAPACITY-only
 * RETRY classification (every other error must fail closed). */
static _Atomic int bind_dbg_fail_probes;
static moqr_result_t bind_dbg_probe_result;
void
moqr_bind_debug_fail_probe(int nth, moqr_result_t result)
{
    bind_dbg_probe_result = result;   /* set before arming the countdown */
    atomic_store(&bind_dbg_fail_probes, nth);
}

/* Test-only write-failure injection for a forwarded fetch after its
 * downstream FETCH_OK: the Nth next forwarded object write from now is
 * reported MOQ_ERR_INVAL without reaching the session, so the request-local
 * abort path (never a connection close) is observable. */
static _Atomic int bind_dbg_fail_fwd_writes;
void
moqr_bind_debug_fail_fwd_write(int nth)
{
    atomic_store(&bind_dbg_fail_fwd_writes, nth);
}

/* Test-only reason-specific gauges: SESSION_SG re-attempts made by the
 * pump, and the current SESSION_SG-parked population (the exceptional
 * set the plan bounds). */
static _Atomic uint64_t bind_dbg_sg_attempts;
uint64_t
moqr_bind_debug_sg_attempts(void)
{
    return atomic_load(&bind_dbg_sg_attempts);
}

/* Saturating increment for the per-conn blockage counters: a counter that
 * has reached UINT64_MAX stays there rather than wrapping to 0, so the
 * LIVE aggregate below can never see a large history collapse to a small
 * value (the aggregate's checked-add would still catch a genuine overflow). */
static inline void
bind_dbg_blocked_inc(uint64_t *v)
{
    if (*v != UINT64_MAX) {
        (*v)++;
    }
}

/* Test-only seeding of one LIVE conn slot's blockage counters — drives the
 * aggregate overflow path and the lifecycle assertions without needing to
 * pump millions of refusals. Returns false for a NULL bind or a slot that is
 * out of range or not currently live (used && !closed). */
bool
moqr_bind_debug_seed_blocked(moqr_bind_t *b, uint32_t slot,
                             const uint64_t counts[3])
{
    if (b == NULL || counts == NULL || slot >= b->max_conns) {
        return false;
    }
    b_conn_t *cn = &b->conns[slot];
    if (!cn->used || cn->closed) {
        return false;
    }
    cn->dbg_blocked[0] = counts[0];
    cn->dbg_blocked[1] = counts[1];
    cn->dbg_blocked[2] = counts[2];
    return true;
}

/* Test-only blockage attribution for ONE conn slot of ONE bind. The
 * caller owns the synchronization: read from the owning lane thread, or
 * after the lane threads have been joined (post managed-stop). */
void
moqr_bind_debug_conn_blocked_counts(const moqr_bind_t *b, uint32_t slot,
                                    uint64_t out[3])
{
    out[0] = out[1] = out[2] = 0;
    if (slot < b->max_conns) {
        out[0] = b->conns[slot].dbg_blocked[0];
        out[1] = b->conns[slot].dbg_blocked[1];
        out[2] = b->conns[slot].dbg_blocked[2];
    }
}

/* Test-only BIND_SG track attribution for ONE conn slot: which TRACK's
 * delivery hit the subgroup-pool refusal (first latches; a different track
 * later — or an unresolvable handle — marks ambiguity). The track is the
 * join key: demands are keyed by track, so the shard layer resolves it to
 * the demand id (moqr_shards_debug_track_demand). Returns false (zeroed
 * outs) for NULL/out-of-range/never-refused; *ambiguous is meaningful even
 * then (an early unresolvable handle latches ambiguity without a track).
 * Same synchronization contract as moqr_bind_debug_conn_blocked_counts. */
bool
moqr_bind_debug_conn_bind_sg_track(const moqr_bind_t *b, uint32_t slot,
                                   uint64_t *track_raw, uint64_t *track_gen,
                                   bool *ambiguous)
{
    if (track_raw != NULL) {
        *track_raw = 0;
    }
    if (track_gen != NULL) {
        *track_gen = 0;
    }
    if (ambiguous != NULL) {
        *ambiguous = false;
    }
    if (b == NULL || slot >= b->max_conns) {
        return false;
    }
    const b_conn_t *cn = &b->conns[slot];
    if (ambiguous != NULL) {
        *ambiguous = cn->dbg_bind_sg_ambiguous;
    }
    if (!cn->dbg_bind_sg_track_set) {
        return false;
    }
    if (track_raw != NULL) {
        *track_raw = cn->dbg_bind_sg_track;
    }
    if (track_gen != NULL) {
        *track_gen = cn->dbg_bind_sg_track_gen;
    }
    return true;
}

uint32_t
moqr_bind_debug_sg_park_count(const moqr_bind_t *b)
{
    uint32_t n = 0;
    for (uint32_t i = 0; i < b->max_conns; i++) {
        if (bind_dl_test(b->dl_parked, i) &&
            b->conns[i].park_reason == B_PARK_SESSION_SG) {
            n++;
        }
    }
    return n;
}

/* Checked u64 add: MOQR_OK and *acc += v, or MOQR_ERR_INTERNAL on overflow
 * (leaving *acc unchanged). Load-bearing for the aggregate's three totals so
 * a counter sum that would wrap makes the lane emit a refusal row. */
static moqr_result_t
bind_ckadd_u64(uint64_t *acc, uint64_t v)
{
    if (*acc > UINT64_MAX - v) {
        return MOQR_ERR_INTERNAL;
    }
    *acc += v;
    return MOQR_OK;
}

moqr_result_t
moqr_bind_debug_blocked_aggregate(const moqr_bind_t *b,
                                  moqr_bind_blocked_agg_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));   /* zero output on every failure path too */
    if (b == NULL) {
        return MOQR_ERR_INVAL;
    }
    for (uint32_t i = 0; i < b->max_conns; i++) {
        const b_conn_t *cn = &b->conns[i];
        if (!cn->used || cn->closed) {
            continue;   /* LIVE only — a detached slot keeps stale counters */
        }
        out->live_conns++;
        if (cn->dbg_blocked[0] > 0) {
            out->conns_action_cap++;
        }
        if (cn->dbg_blocked[1] > 0) {
            out->conns_session_sg++;
        }
        if (cn->dbg_blocked[2] > 0) {
            out->conns_bind_sg++;
        }
        if (bind_ckadd_u64(&out->action_cap_total, cn->dbg_blocked[0]) !=
                MOQR_OK ||
            bind_ckadd_u64(&out->session_sg_total, cn->dbg_blocked[1]) !=
                MOQR_OK ||
            bind_ckadd_u64(&out->bind_sg_total, cn->dbg_blocked[2]) !=
                MOQR_OK) {
            memset(out, 0, sizeof(*out));   /* fail closed: no partial totals */
            return MOQR_ERR_INTERNAL;
        }
        if (bind_dl_test(b->dl_parked, i)) {
            if (cn->park_reason == B_PARK_ACTION_CAP) {
                out->parked_action_cap++;
            } else if (cn->park_reason == B_PARK_SESSION_SG) {
                out->parked_session_sg++;
            }
        }
    }
    return MOQR_OK;
}

/* Test-only downstream subgroup-pool occupancy for one conn slot. */
uint32_t
moqr_bind_debug_conn_open_sgs(const moqr_bind_t *b, uint32_t slot)
{
    uint32_t n = 0;
    if (slot < b->max_conns) {
        const b_conn_t *cn = &b->conns[slot];
        for (uint32_t i = 0; i < b->n_sgs; i++) {
            if (cn->sgs[i].used) {
                n++;
            }
        }
    }
    return n;
}

/* Test-only resolved conn-slot capacity. */
uint32_t
moqr_bind_debug_max_conns(const moqr_bind_t *b)
{
    return b != NULL ? b->max_conns : 0u;
}

uint64_t
moqr_bind_debug_intent_retry_suppressed(const moqr_bind_t *b)
{
    return b != NULL ? b->dbg_intent_retry_suppressed : 0u;
}

/* Test-only scheduling-state dump for one conn slot. Out-of-range slots
 * report a zeroed state (the SGDIAG sweep probes a fixed slot range). */
void
moqr_bind_debug_dl_state(const moqr_bind_t *b, uint32_t slot, bool *ready,
                         bool *parked, uint8_t *reason)
{
    if (slot >= b->max_conns) {
        *ready = false;
        *parked = false;
        *reason = 0;
        return;
    }
    *ready = bind_dl_test(b->dl_ready, slot);
    *parked = bind_dl_test(b->dl_parked, slot);
    *reason = b->conns[slot].park_reason;
}
#endif

/* The aggregate forwarding byte pool: the appended cfg field when the caller's
 * struct reaches it and it is nonzero, else the named default. Values the
 * accounting cannot carry on a 32-bit host are refused, never truncated. */
static moqr_result_t
bind_fwd_bytes_resolve(const moqr_bind_cfg_t *cfg, uint64_t *out)
{
    uint64_t v = BIND_CFG_HAS(cfg, fetch_forward_bytes)
                     ? cfg->fetch_forward_bytes : 0;
    if (v == 0) {
        v = MOQR_BIND_DEF_FETCH_FORWARD_BYTES;
    }
    if (v > UINT32_MAX) {
        *out = 0;
        return MOQR_ERR_INVAL;
    }
    *out = v;
    return MOQR_OK;
}

/* The fill wait pair, in microseconds: each value when nonzero, else its
 * named default; a default above the maximum is refused. Both are bounded so
 * the deadline arithmetic (processing time + wait) cannot wrap. */
#define BIND_FILL_WAIT_MAX_MS (UINT64_MAX / 1000000u)
static moqr_result_t
bind_fill_wait_resolve(uint64_t d, uint64_t m, uint64_t *def_us,
                       uint64_t *max_us)
{
    if (d == 0) {
        d = MOQR_BIND_DEF_FILL_TIMEOUT_MS;
    }
    if (m == 0) {
        m = MOQR_BIND_DEF_FILL_TIMEOUT_MAX_MS;
    }
    if (d > m || m > BIND_FILL_WAIT_MAX_MS) {
        *def_us = 0;
        *max_us = 0;
        return MOQR_ERR_INVAL;
    }
    *def_us = d * 1000u;
    *max_us = m * 1000u;
    return MOQR_OK;
}

moqr_result_t
moqr_bind_cfg_resolve(const moqr_bind_cfg_t *cfg,
                      const moqr_core_limits_t *lim, moqr_bind_limits_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    if (cfg == NULL || cfg->struct_size < sizeof(uint32_t) || lim == NULL) {
        return MOQR_ERR_INVAL;
    }
#define BL_RESOLVE(dst, field, def) \
    out->dst = BIND_CFG_HAS(cfg, field) && cfg->field != 0 ? cfg->field : (def)
    BL_RESOLVE(max_conns, max_conns, lim->max_bindings);
    BL_RESOLVE(n_dsubs, max_downstream_subs, lim->max_subs);
    BL_RESOLVE(n_sgs, max_open_subgroups, BIND_DEF_SGS);
    BL_RESOLVE(n_usubs, max_upstream_subs, lim->max_tracks);
    BL_RESOLVE(n_pubs, max_publishes, lim->max_tracks);
    BL_RESOLVE(n_anns, max_announces, lim->max_ns_nodes);
    BL_RESOLVE(request_grant_window, request_grant_window,
               BIND_DEF_GRANT_WINDOW);
#undef BL_RESOLVE
    out->n_fetches = lim->max_fetches;
    /* Held-terminal entries per connection (b_refusal_t): one poll of
     * headroom and one per parkable authorization. */
    uint64_t refusals = (uint64_t)BIND_PUMP_EVENT_POLL + lim->max_parked;
    if (refusals > UINT32_MAX) {
        memset(out, 0, sizeof(*out));
        return MOQR_ERR_INVAL;
    }
    out->n_refusals = (uint32_t)refusals;
    {
        uint64_t fwd_bytes;
        if (bind_fwd_bytes_resolve(cfg, &fwd_bytes) != MOQR_OK) {
            memset(out, 0, sizeof(*out));
            return MOQR_ERR_INVAL;
        }
    }
    /* Derived count, computed WIDE: a deferral ring the 32-bit field cannot
     * represent rejects the config — never a wrapped small ring. */
    uint64_t pend = 4ull * ((uint64_t)lim->max_subs + lim->max_tracks) +
                    4ull * lim->max_intents + 64u;
    if (pend > UINT32_MAX) {
        memset(out, 0, sizeof(*out));
        return MOQR_ERR_INVAL;
    }
    out->pending_cap = (uint32_t)pend;
    /* The namespace payload sidecar is 1:1 with the deferral ring, so it needs
     * no bound of its own. This is a finite POLICY bound, not a mathematical
     * ceiling on simultaneously owed updates: prefix-to-namespace matching is a
     * fan-out and blocked updates accumulate across pumps, so exhaustion is
     * possible by construction and is handled fail-closed at the push site. */
    out->nsu_cap = out->pending_cap;
    return MOQR_OK;
}

/* One announce's copied namespace bytes can never exceed the core's shared
 * full-track-name cap; a bind slot exists only for a core-ACCEPTED announce,
 * so the live-slot count is bounded by the core ns-node pool as well as the
 * bind tables. */
#define BIND_ANN_BYTES_MAX 4096u

moqr_result_t
moqr_bind_capacity_describe(const moqr_bind_cfg_t *cfg,
                            const moqr_core_limits_t *lim,
                            moqr_bind_capacity_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    moqr_bind_limits_t bl;
    moqr_result_t rc = moqr_bind_cfg_resolve(cfg, lim, &bl);
    if (rc != MOQR_OK) {
        return rc;
    }
    /* Mirrors moqr_bind_create's allocations exactly: the bind struct, the
     * deferral ring, the conn array, and every per-conn table (eager).
     * Checked composition: a wrap refuses INVAL, never under-reports. */
    uint64_t per_conn = moqr_cap_add(
        moqr_cap_mul(bl.n_dsubs, sizeof(b_dsub_t)),
        moqr_cap_add(
            moqr_cap_mul(bl.n_sgs, sizeof(b_sg_t)),
            moqr_cap_add(
                moqr_cap_mul(bl.n_usubs, sizeof(b_usub_t)),
                moqr_cap_add(
                    moqr_cap_mul(bl.n_pubs, sizeof(b_pub_t)),
                    moqr_cap_add(
                        moqr_cap_mul(bl.n_anns, sizeof(b_ann_t)),
                        moqr_cap_add(
                            moqr_cap_mul(bl.n_fetches, sizeof(b_fetch_t)),
                            moqr_cap_mul(bl.n_refusals,
                                         sizeof(b_refusal_t))))))));
    out->structure_bytes = moqr_cap_add(
        sizeof(moqr_bind_t),
        moqr_cap_add(moqr_cap_mul(bl.pending_cap, sizeof(moqr_intent_t)),
                     moqr_cap_mul(bl.max_conns,
                                  moqr_cap_add(sizeof(b_conn_t),
                                               per_conn))));
    /* Delivery scheduling bitsets: ready + parked, one bit per conn slot. */
    out->structure_bytes = moqr_cap_add(
        out->structure_bytes,
        moqr_cap_mul(2u * (uint64_t)bind_dl_word_count(bl.max_conns),
                     sizeof(uint64_t)));
    out->structure_bytes = moqr_cap_add(
        out->structure_bytes,
        moqr_cap_add(moqr_cap_mul(BIND_PUMP_EVENT_BATCH,
                                  sizeof(moq_event_t)),
                     moqr_cap_add(
                         moqr_cap_mul(BIND_PUMP_REVOKED_BATCH,
                                      sizeof(moqr_revoked_grant_t)),
                         moqr_cap_mul(BIND_PUMP_INTENT_BATCH,
                                      sizeof(moqr_intent_t)))));
    uint64_t ann_slots = moqr_cap_mul(bl.max_conns, bl.n_anns);
    if (ann_slots > lim->max_ns_nodes) {
        ann_slots = lim->max_ns_nodes;
    }
    out->announce_bytes = moqr_cap_mul(ann_slots, BIND_ANN_BYTES_MAX);
    /* The namespace payload sidecar: one entry per deferral-ring slot, plus
     * the suffix bytes every slot could own at once. One held suffix is
     * bounded by the same full-track-name cap an announce is. */
    out->structure_bytes = moqr_cap_add(
        out->structure_bytes, moqr_cap_mul(bl.nsu_cap, sizeof(b_nsu_t)));
    out->announce_bytes = moqr_cap_add(
        out->announce_bytes, moqr_cap_mul(bl.nsu_cap, BIND_ANN_BYTES_MAX));
    /* Forwarded-fetch transactions: the bind-wide pool of fixed headers (one
     * per core fetch slot) is an eager table like the rest; the owned track
     * keys every upstream-subscription slot may hold and the aggregate
     * fetch_forward_bytes pool are variable ceilings that this frozen output
     * carries in total_bytes only (moqr_bind.h documents the accounting). */
    out->structure_bytes = moqr_cap_add(
        out->structure_bytes, moqr_cap_mul(bl.n_fetches,
            moqr_cap_add(sizeof(b_fwd_t), sizeof(b_retrieve_hold_t))));
    uint64_t fwd_bytes = 0;
    if (bind_fwd_bytes_resolve(cfg, &fwd_bytes) != MOQR_OK) {
        memset(out, 0, sizeof(*out));
        return MOQR_ERR_INVAL;
    }
    uint64_t forward_bytes = moqr_cap_add(
        fwd_bytes,
        moqr_cap_mul(moqr_cap_mul(bl.max_conns, bl.n_usubs), BF_FWD_KEY_MAX));
    uint32_t meta_pubs;
    if (!bind_meta_publish_count(lim, &bl, &meta_pubs)) {
        memset(out, 0, sizeof(*out));
        return MOQR_ERR_INVAL;
    }
    /* Held namespace-update answers: one per request that can be updated. */
    out->structure_bytes = moqr_cap_add(out->structure_bytes,
        moqr_cap_mul(bind_nsans_cap(lim), sizeof(b_nsans_t)));
    out->structure_bytes = moqr_cap_add(out->structure_bytes,
        moqr_cap_add(moqr_cap_mul(lim->max_tracks, sizeof(b_meta_entry_t)),
            moqr_cap_add(moqr_cap_mul(bl.pending_cap, sizeof(moqr_bind_meta_ref_t)),
                         moqr_cap_mul(meta_pubs, sizeof(b_meta_publish_t)))));
    out->total_bytes = moqr_cap_add(
        moqr_cap_add(out->structure_bytes, out->announce_bytes),
        moqr_cap_add(forward_bytes, MOQR_BIND_META_BYTES_MAX));
    if (out->total_bytes == UINT64_MAX) {
        memset(out, 0, sizeof(*out));
        return MOQR_ERR_INVAL;   /* wrapped: refuse, never under-report */
    }
    return MOQR_OK;
}

moqr_result_t
moqr_bind_create(const moqr_bind_cfg_t *cfg, moqr_bind_t **out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    *out = NULL;
    if (cfg == NULL || cfg->struct_size < sizeof(uint32_t)) {
        return MOQR_ERR_INVAL;
    }
    const moq_alloc_t *a = BIND_CFG_HAS(cfg, alloc) ? cfg->alloc : NULL;
    moqr_core_t *core = BIND_CFG_HAS(cfg, core) ? cfg->core : NULL;
    if (a == NULL || a->alloc == NULL || a->free == NULL || core == NULL) {
        return MOQR_ERR_INVAL;
    }

    moqr_bind_t *b = a->alloc(sizeof(*b), a->ctx);
    if (b == NULL) {
        return MOQR_ERR_NOMEM;
    }
    memset(b, 0, sizeof(*b));
    b->alloc = *a;
    b->core = core;
    /* Default map sizes FOLLOW the core's admission limits, so the
     * binding can never be under-provisioned relative to what the core
     * will admit. Explicit cfg values still win (tests use tiny maps to
     * prove the refusal paths). */
    moqr_core_limits_t lim;
    moqr_core_get_limits(core, &lim);
    /* Same pure resolution the capacity describe uses; the live core's
     * limits equal moqr_core_limits_resolve(cfg) by construction. */
    moqr_bind_limits_t bl;
    if (moqr_bind_cfg_resolve(cfg, &lim, &bl) != MOQR_OK ||
        !bind_meta_publish_count(&lim, &bl, &b->metadata_publish_cap)) {
        b->alloc.free(b, sizeof(*b), b->alloc.ctx);
        return MOQR_ERR_INVAL;
    }
    b->max_conns = bl.max_conns;
    b->n_dsubs = bl.n_dsubs;
    b->n_sgs = bl.n_sgs;
    b->n_usubs = bl.n_usubs;
    b->n_pubs = bl.n_pubs;
    b->n_anns = bl.n_anns;
    b->request_grant_window = bl.request_grant_window;
    b->n_fetches = bl.n_fetches;
    b->n_refusals = bl.n_refusals;
    if (bind_fwd_bytes_resolve(cfg, &b->fwd_bytes_cap) != MOQR_OK ||
        bind_fill_wait_resolve(0, 0, &b->fill_wait_default_us,
                               &b->fill_wait_max_us) != MOQR_OK) {
        b->alloc.free(b, sizeof(*b), b->alloc.ctx);
        return MOQR_ERR_INVAL;
    }

    /* Intent router (multi-shard runtime; absent by default). */
    b->router_cookie_base =
        BIND_CFG_HAS(cfg, router_cookie_base) ? cfg->router_cookie_base : 0;
    b->router = BIND_CFG_HAS(cfg, router) ? cfg->router : NULL;
    b->router_ctx = BIND_CFG_HAS(cfg, router_ctx) ? cfg->router_ctx : NULL;
    /* A router with base 0 would route real connection cookies (1..max_conns)
     * too, starving every session of its intents. Treat 0 as "use the standard
     * reserved range" (the cfg's 0-means-default idiom) so the common case —
     * install a router, get MOQR_SHARD_COOKIE_BASE — is not a footgun. */
    if (b->router != NULL && b->router_cookie_base == 0) {
        b->router_cookie_base = MOQR_SHARD_COOKIE_BASE;
    }

    /* Deferred-output ring: large enough to hold every scalar terminal that can
     * be in flight at once (each live sub can owe an ACCEPT and a SUB_DONE, each
     * live pub an ACCEPT and a REJECT, plus upstream-failure REJECT_SUB fan-outs
     * and in-flight track-status), with generous headroom over the core intent
     * ring. Overflow is counted, never dropped silently; the lifecycle sweep
     * asserts the high-water stays well under this. */
    b->pending_cap = bl.pending_cap;
    b->pending = b->alloc.alloc((size_t)b->pending_cap * sizeof(*b->pending),
                                b->alloc.ctx);
    if (b->pending == NULL) {
        b->alloc.free(b, sizeof(*b), b->alloc.ctx);
        return MOQR_ERR_NOMEM;
    }
    memset(b->pending, 0, (size_t)b->pending_cap * sizeof(*b->pending));

    /* Retained namespace updates (draft-18 Section 6.2 owes each one durably).
     * Eagerly sized like the deferral ring so a blocked control path never has
     * to allocate the ring itself under pressure. */
    b->nsu_cap = bl.nsu_cap;
    b->nsu = b->alloc.alloc((size_t)b->nsu_cap * sizeof(*b->nsu),
                            b->alloc.ctx);
    if (b->nsu == NULL) {
        b->alloc.free(b->pending, (size_t)b->pending_cap * sizeof(*b->pending),
                      b->alloc.ctx);
        b->alloc.free(b, sizeof(*b), b->alloc.ctx);
        return MOQR_ERR_NOMEM;
    }
    memset(b->nsu, 0, (size_t)b->nsu_cap * sizeof(*b->nsu));

    b->conns = b->alloc.alloc((size_t)b->max_conns * sizeof(*b->conns),
                              b->alloc.ctx);
    if (b->conns == NULL) {
        b->alloc.free(b->nsu, (size_t)b->nsu_cap * sizeof(*b->nsu),
                      b->alloc.ctx);
        b->alloc.free(b->pending, (size_t)b->pending_cap * sizeof(*b->pending),
                      b->alloc.ctx);
        b->alloc.free(b, sizeof(*b), b->alloc.ctx);
        return MOQR_ERR_NOMEM;
    }
    memset(b->conns, 0, (size_t)b->max_conns * sizeof(*b->conns));
    b->metadata_cap = lim.max_tracks;
    b->metadata = b->alloc.alloc((size_t)b->metadata_cap * sizeof(*b->metadata),
                                 b->alloc.ctx);
    if (b->metadata == NULL) {
        moqr_bind_destroy(b);
        return MOQR_ERR_NOMEM;
    }
    memset(b->metadata, 0, (size_t)b->metadata_cap * sizeof(*b->metadata));
    b->pending_metadata = b->alloc.alloc(
        (size_t)b->pending_cap * sizeof(*b->pending_metadata), b->alloc.ctx);
    if (b->pending_metadata == NULL) {
        moqr_bind_destroy(b);
        return MOQR_ERR_NOMEM;
    }
    memset(b->pending_metadata, 0,
           (size_t)b->pending_cap * sizeof(*b->pending_metadata));
    b->metadata_publishes = b->alloc.alloc(
        (size_t)b->metadata_publish_cap * sizeof(*b->metadata_publishes), b->alloc.ctx);
    if (b->metadata_publishes == NULL) {
        moqr_bind_destroy(b);
        return MOQR_ERR_NOMEM;
    }
    memset(b->metadata_publishes, 0,
           (size_t)b->metadata_publish_cap * sizeof(*b->metadata_publishes));
    b->nsans_cap = bind_nsans_cap(&lim);
    if (b->nsans_cap > 0) {
        b->nsans = b->alloc.alloc((size_t)b->nsans_cap * sizeof(*b->nsans),
                                  b->alloc.ctx);
        if (b->nsans == NULL) {
            moqr_bind_destroy(b);
            return MOQR_ERR_NOMEM;
        }
        memset(b->nsans, 0, (size_t)b->nsans_cap * sizeof(*b->nsans));
    }
    /* Forwarded-fetch transaction pool: from here on every failure routes
     * through moqr_bind_destroy, which frees whatever was allocated. */
    b->n_fwd = bl.n_fetches;
    if (b->n_fwd > 0) {
        b->fwd = b->alloc.alloc((size_t)b->n_fwd * sizeof(*b->fwd),
                                b->alloc.ctx);
        if (b->fwd == NULL) {
            moqr_bind_destroy(b);
            return MOQR_ERR_NOMEM;
        }
        memset(b->fwd, 0, (size_t)b->n_fwd * sizeof(*b->fwd));
        if (sizeof(*b->cached_holds) > SIZE_MAX / b->n_fwd) {
            moqr_bind_destroy(b);
            return MOQR_ERR_INVAL;
        }
        b->cached_holds = b->alloc.alloc(
            (size_t)b->n_fwd * sizeof(*b->cached_holds), b->alloc.ctx);
        if (b->cached_holds == NULL) {
            moqr_bind_destroy(b);
            return MOQR_ERR_NOMEM;
        }
        memset(b->cached_holds, 0,
               (size_t)b->n_fwd * sizeof(*b->cached_holds));
    }
    {
        size_t dw = (size_t)bind_dl_word_count(b->max_conns) *
                    sizeof(uint64_t);
        b->dl_ready = b->alloc.alloc(dw, b->alloc.ctx);
        b->dl_parked = b->alloc.alloc(dw, b->alloc.ctx);
        if (b->dl_ready == NULL || b->dl_parked == NULL) {
            moqr_bind_destroy(b);
            return MOQR_ERR_NOMEM;
        }
        memset(b->dl_ready, 0, dw);
        memset(b->dl_parked, 0, dw);
    }
    b->pump_events =
        b->alloc.alloc((size_t)BIND_PUMP_EVENT_BATCH *
                           sizeof(*b->pump_events),
                       b->alloc.ctx);
    if (b->pump_events == NULL) {
        moqr_bind_destroy(b);
        return MOQR_ERR_NOMEM;
    }
    b->pump_revoked =
        b->alloc.alloc((size_t)BIND_PUMP_REVOKED_BATCH *
                           sizeof(*b->pump_revoked),
                       b->alloc.ctx);
    if (b->pump_revoked == NULL) {
        moqr_bind_destroy(b);
        return MOQR_ERR_NOMEM;
    }
    b->pump_intents =
        b->alloc.alloc((size_t)BIND_PUMP_INTENT_BATCH *
                           sizeof(*b->pump_intents),
                       b->alloc.ctx);
    if (b->pump_intents == NULL) {
        moqr_bind_destroy(b);
        return MOQR_ERR_NOMEM;
    }
    for (uint32_t i = 0; i < b->max_conns; i++) {
        b_conn_t *cn = &b->conns[i];
        /* Allocate each per-connection table and zero it IMMEDIATELY, before the
         * next allocation can fail. A partial-create moqr_bind_destroy scans the
         * `used` flags of every non-NULL table, so an allocated-but-uninitialized
         * table would be read as garbage; zeroing per table keeps the failed and
         * not-yet-reached tables NULL (skipped by destroy) and every allocated
         * table initialized. */
#define BIND_ALLOC_TABLE(field, count)                                        \
        do {                                                                  \
            cn->field =                                                       \
                b->alloc.alloc((size_t)(count) * sizeof(*cn->field),          \
                               b->alloc.ctx);                                 \
            if (cn->field == NULL) {                                          \
                moqr_bind_destroy(b);                                         \
                return MOQR_ERR_NOMEM;                                        \
            }                                                                 \
            memset(cn->field, 0, (size_t)(count) * sizeof(*cn->field));       \
        } while (0)
        BIND_ALLOC_TABLE(dsubs, b->n_dsubs);
        BIND_ALLOC_TABLE(sgs, b->n_sgs);
        BIND_ALLOC_TABLE(usubs, b->n_usubs);
        BIND_ALLOC_TABLE(pubs, b->n_pubs);
        BIND_ALLOC_TABLE(anns, b->n_anns);
        BIND_ALLOC_TABLE(fetches, b->n_fetches);
        BIND_ALLOC_TABLE(refusals, b->n_refusals);
#undef BIND_ALLOC_TABLE
    }
    *out = b;
    return MOQR_OK;
}

/* Scalar-safe intent kinds: payload is fully self-contained, so a copy can be
 * parked in the pending ring and retried across pumps. Kinds that read the
 * borrowed ns_parts/name views (UPSTREAM_SUBSCRIBE / NS_FOUND / NS_GONE) are
 * deliberately absent — those byte spans borrow core-interned storage and are
 * valid only until the next mutating core call (relay.h:102), so they can
 * never be parked by a shallow copy. NS_FOUND / NS_GONE reach the same ring
 * through bind_defer_push_nsu, which deep-copies the suffix into the slot's
 * payload sidecar first; UPSTREAM_SUBSCRIBE remains consume-in-pump. */
static bool
bind_intent_deferrable(moqr_intent_kind_t kind)
{
    switch (kind) {
    case MOQR_INTENT_ACCEPT_SUB:
    case MOQR_INTENT_REJECT_SUB:
    case MOQR_INTENT_SUB_DONE:
    case MOQR_INTENT_ACCEPT_PUBLISH:
    case MOQR_INTENT_REJECT_PUBLISH:
    case MOQR_INTENT_TRACK_STATUS_OK:
    case MOQR_INTENT_TRACK_STATUS_ERROR:
        return true;
    default:
        return false;
    }
}

/* Execute one output intent's session write(s).
 *   true  => consumed: delivered, moot (conn gone/closed), hard-failed, or
 *            (NS_FOUND/NS_GONE only) newly parked in the ring with its suffix
 *            deep-copied — *parked_out reports that case so the caller can set
 *            `blocked` before the next intent is attempted.
 *   false => the write hit WOULD_BLOCK and the intent must be retried from the
 *            ring next pump; *defer holds it (a copy of *it, or a synthesized
 *            refusal when downstream tracking was exhausted). Deferral leaves
 *            all binding/core state untouched so the retry re-runs cleanly.
 * Which kinds can return false: scalar-safe kinds (bind_intent_deferrable), and
 * a retained NS_FOUND/NS_GONE re-attempted as BIND_TRY_FROM_RING — its payload
 * is already bind-owned, so it stays in place rather than being re-parked. A
 * FRESH namespace intent instead parks and returns true. Consume-in-pump
 * borrowed kinds always return true and are never parked. */
/* How the executor is entering bind_try_intent for this intent. */
#define BIND_TRY_FRESH      0u   /* newly polled, nothing blocked          */
#define BIND_TRY_FROM_RING  1u   /* a retry of an already-parked slot      */
#define BIND_TRY_FORCE_PARK 2u   /* something is blocked: queue, never send */

static bool bind_try_intent(moqr_bind_t *b, const moqr_intent_t *it,
                            uint64_t now_us, uint8_t mode,
                            bool *parked_out, moqr_intent_t *defer);

/* Deep-copy one namespace update's suffix into the retained ring. The intent's
 * ns_parts borrow core-interned storage that dies at the next mutating core
 * call, so nothing borrowed may escape this call.
 * Returns true if the update is now owned by the bind and will be retried.
 * false means held state is exhausted — the caller must fail closed rather
 * than drop a MUST-required update. */
static bool
bind_nsu_store(moqr_bind_t *b, uint32_t slot, moqr_intent_t *parked)
{
#ifdef MOQR_BIND_TESTING
    if (atomic_load(&bind_dbg_fail_nsu_stores) > 0 &&
        atomic_fetch_sub(&bind_dbg_fail_nsu_stores, 1) == 1) {
        return false;   /* injected: byte allocation / slot exhaustion */
    }
#endif
    /* The sidecar owns the SUFFIX the subscriber is owed -- the same span the
     * healthy send passes (ns_parts + value). The prefix bytes are never sent
     * and must not be copied or left semantically live in the parked intent,
     * but the prefix/full CARDINALITY is preserved (see below). */
    if (parked->value > parked->ns_count) {
        return false;
    }
    uint32_t off = parked->value;
    uint32_t suffix_count = parked->ns_count - off;
    if (suffix_count > 32) {
        return false;
    }
    size_t total = 0;
    for (uint32_t i = 0; i < suffix_count; i++) {
        const moq_bytes_t *src = &parked->ns_parts[off + i];
        if (src->len > 0 && src->data == NULL) {
            return false;
        }
        if (SIZE_MAX - total < src->len) {
            return false;
        }
        total += src->len;
    }
    /* Defense in depth: never allocate more than the per-slot bound the
     * capacity description charges for. */
    if (total > BIND_ANN_BYTES_MAX) {
        return false;
    }
    uint8_t *bytes = NULL;
    if (total > 0) {
        bytes = b->alloc.alloc(total, b->alloc.ctx);
        if (bytes == NULL) {
            return false;
        }
    }
    b_nsu_t *u = &b->nsu[slot];
    if (u->used) {
        /* A live payload here means the ring handed out an occupied slot.
         * Overwriting it would silently drop an update already owed, so the
         * store is refused and the caller fails closed. The existing slot is
         * left exactly as it was. */
        if (bytes != NULL) {
            b->alloc.free(bytes, total, b->alloc.ctx);
        }
        return false;
    }
    u->used = true;   /* occupancy is explicit: a valid suffix may own 0 bytes */
    u->bytes = bytes;
    u->bytes_len = total;
    /* Only the SUFFIX is bind-owned, but ns_count and value keep their meaning:
     * ns_count is the FULL announced Track Namespace cardinality and value is
     * the subscribed prefix cardinality, exactly as the fresh intent carried
     * them. That distinction is load-bearing -- a true root announcement is
     * (ns_count 0, value 0), while a non-root namespace equal to its prefix is
     * (ns_count N, value N) and merely has an empty wire suffix -- so it must
     * survive retention or a replayed exact-prefix update would be
     * indistinguishable from a root one.
     *
     * The owned suffix descriptors therefore stay at their ORIGINAL indices
     * [value, ns_count); the prefix descriptors [0, value) are cleared so no
     * borrowed pointer survives the pump, and their bytes are never copied. */
    size_t o = 0;
    for (uint32_t i = 0; i < suffix_count; i++) {
        moq_bytes_t src = parked->ns_parts[off + i];
        parked->ns_parts[off + i].len = src.len;
        if (src.len == 0) {
            parked->ns_parts[off + i].data = NULL;   /* empty, not aliased */
            continue;
        }
        memcpy(bytes + o, src.data, src.len);
        parked->ns_parts[off + i].data = bytes + o;
        o += src.len;
    }
    for (uint32_t i = 0; i < off; i++) {
        parked->ns_parts[i].data = NULL;   /* no borrowed prefix left live */
        parked->ns_parts[i].len = 0;
    }
    b->nsu_live++;
    b->nsu_retained++;
    if (b->nsu_live > b->nsu_high_water) {
        b->nsu_high_water = b->nsu_live;
    }
    return true;
}

/* Release one sidecar slot's payload (delivered, purged, or torn down). */
static void
bind_nsu_release(moqr_bind_t *b, uint32_t slot)
{
    b_nsu_t *u = &b->nsu[slot];
    if (!u->used) {
        return;
    }
    if (u->bytes != NULL) {
        b->alloc.free(u->bytes, u->bytes_len, b->alloc.ctx);
    }
    b->nsu_live--;
    memset(u, 0, sizeof(*u));
}

/* THE ordered-ring room predicate. Both insertion kinds go through it, so an
 * injected "full" and a genuinely full ring take the identical branch. */
static bool
bind_ordered_room(moqr_bind_t *b, bool is_nsu)
{
#ifdef MOQR_BIND_TESTING
    if (atomic_load(&bind_dbg_ring_full_n) > 0 &&
        (atomic_load(&bind_dbg_ring_full_nsu) != 0) == is_nsu &&
        atomic_fetch_sub(&bind_dbg_ring_full_n, 1) == 1) {
        return false;
    }
#endif
    (void)is_nsu;
    return b->pending_count < b->pending_cap;
}

/* Append to the ordered ring. PRECONDITION: room was already admitted for this
 * insertion by exactly one bind_ordered_room() call. Admission is separate from
 * the append so that one insertion answers to one decision: a namespace
 * admitted as a namespace must never also be judged as a scalar. */
static void
bind_pending_append(moqr_bind_t *b, const moqr_intent_t *it)
{
    b->pending[(b->pending_head + b->pending_count) % b->pending_cap] = *it;
    b->pending_count++;
    if (b->pending_count > b->pending_high_water) {
        b->pending_high_water = b->pending_count;
    }
}

/* Scalar admission + append. Namespace insertions do NOT come through here:
 * they admit themselves as namespaces in bind_defer_push_nsu. */
static bool
bind_pending_push(moqr_bind_t *b, const moqr_intent_t *it)
{
    if (!bind_ordered_room(b, false)) {
        return false;
    }
    bind_pending_append(b, it);
    return true;
}

/* false => the intent could NOT join the ordered FIFO. Namespace updates now
 * share this ring, so "a scalar terminal always fits" is no longer a safe
 * assumption: a caller that ignores the failure would consume mandatory
 * ordered output while its target connection stays live. */
static bool
bind_defer_push(moqr_bind_t *b, const moqr_intent_t *it,
                 moqr_bind_meta_ref_t *pin)
{
    if (!bind_intent_deferrable(it->kind)) {
        b->pending_nonscalar_blocked++;
        return true;   /* contract violation, counted; not a ring overflow */
    }
    uint32_t slot = (b->pending_head + b->pending_count) % b->pending_cap;
    if (!bind_pending_push(b, it)) {
        b->session_errors++;
        return false;
    }
    if (pin != NULL && (it->kind == MOQR_INTENT_ACCEPT_SUB ||
                        it->kind == MOQR_INTENT_TRACK_STATUS_OK)) {
        b->pending_metadata[slot] = *pin;
        memset(pin, 0, sizeof(*pin));
    }
    return true;
}

#define BIND_SESSION_INTERNAL_ERROR 0x1u   /* MoQ session INTERNAL_ERROR      */

/* Wire registries are distinct namespaces; the bind never lets one value
 * serve two of them.
 * - Data-stream reset CANCELLED: every draft assigns 0x1 — the stream was
 *   cancelled because the publisher ended the subscription, and PUBLISH_DONE
 *   carries the more detailed status.
 * - PUBLISH_DONE GOING_AWAY: every draft assigns 0x4 — the publisher issued
 *   a GOAWAY.
 * - REQUEST_ERROR INTERNAL_ERROR: every draft assigns 0x0 — the relay's own
 *   upstream request was terminated by a signal it cannot act on. */
#define BIND_RESET_CANCELLED   0x1u
#define BIND_DONE_GOING_AWAY   0x4u
#define BIND_REQERR_INTERNAL   0x0u

static b_conn_t *conn_by_cookie(moqr_bind_t *b, uint64_t cookie);
static void conn_detach(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us);
static void bind_dup_owners_release(moqr_bind_t *b, const b_conn_t *cn);
static void fwd_conn_gone(moqr_bind_t *b, const b_conn_t *cn);
static void bind_nsans_conn_gone(moqr_bind_t *b, const b_conn_t *cn);
static void bind_dgrant_conn_gone(moqr_bind_t *b, const b_conn_t *cn);
static bool bind_cookie_is_track_sub(uint64_t cookie);
static bool bind_cookie_is_publication(uint64_t cookie);
static void bind_tracks_reject(moqr_bind_t *b, b_conn_t *cn, uint64_t handle,
                               uint64_t err, uint64_t now_us);
static void bind_tracks_answer_held(moqr_bind_t *b, b_conn_t *cn, b_nsans_t *e,
                                    uint64_t now_us);
static void bind_tracks_admit(moqr_bind_t *b, b_conn_t *cn, uint64_t handle,
                              moqr_ns_t prefix, bool forward,
                              bool include_properties, uint32_t group_order,
                              const moqr_auth_token_t *tokens,
                              size_t token_count, uint64_t now_us);
static void bind_retain_request_tokens(moqr_bind_t *b, b_conn_t *cn,
                                       uint64_t handle,
                                       const moqr_auth_token_t *tokens,
                                       size_t count);
static bool fwd_result_is_gone(moq_result_t rc);
static void bind_fetch_refuse(moqr_bind_t *b, b_conn_t *cn, moq_fetch_t fetch,
                              uint64_t error_code, uint64_t now_us);
static void bind_refuse(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t kind,
                        uint64_t handle, uint64_t error_code, bool can_retry,
                        uint64_t retry_ms, uint64_t now_us);
static void bind_accept(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t kind,
                        uint64_t handle, uint64_t now_us);
static void bind_terminal_hold(moqr_bind_t *b, b_conn_t *cn,
                               const b_refusal_t *want);
static bool bind_kind_is_accept(moqr_auth_action_t kind);
static bool bind_accept_undo(moqr_bind_t *b, b_conn_t *cn, const b_refusal_t *r,
                             uint64_t now_us);
static bool bind_ns_sub_accept_held(const moqr_bind_t *b, const b_conn_t *cn,
                                    uint64_t handle);
static moqr_result_t bind_ann_withdraw(moqr_bind_t *b, b_conn_t *cn,
                                       uint64_t raw, uint64_t now_us);
static void bind_refusal_pump(moqr_bind_t *b, uint64_t now_us);
static bool bind_refusal_drop(moqr_bind_t *b, b_conn_t *cn, uint64_t handle);
static bool bind_refusal_can_poll(const moqr_bind_t *b, const b_conn_t *cn);
static void bind_refusal_conn_gone(moqr_bind_t *b, b_conn_t *cn);
static void bind_fill_request(moqr_bind_t *b, b_conn_t *cn,
                              const moq_fill_request_event_t *fr,
                              uint64_t now_us);
static void bind_fill_terminal(moqr_bind_t *b, b_conn_t *cn, moq_fill_t fill);
static void bind_standalone_request(moqr_bind_t *b, b_conn_t *cn,
                                    const moq_fetch_request_event_t *fq,
                                    const moqr_fetch_req_t *freq,
                                    const moq_fetch_request_options_t *fo,
                                    uint64_t now_us);
static bool bind_standalone_cancelled(moqr_bind_t *b, b_conn_t *cn,
                                      moq_fetch_t fetch);
static uint64_t bind_wait_deadline(const moqr_bind_t *b, uint64_t processed_at_us,
                                   bool has_fill_timeout, uint64_t fill_timeout_ms);
static const moq_fetch_request_options_t *bind_fetch_options(const moq_event_t *ev);
static void bind_fetch_range(const moq_fetch_request_options_t *fo,
                             moqr_fetch_req_t *freq);
static void fwd_admit(moqr_bind_t *b, b_conn_t *cn,
                      const moq_fetch_request_event_t *fq, uint64_t now_us);
static bool fwd_on_up_event(moqr_bind_t *b, b_conn_t *cn, moq_event_t *ev,
                             uint64_t now_us);
static bool fwd_on_down_cancelled(moqr_bind_t *b, b_conn_t *cn, uint64_t handle);
static void bind_fwd_pump(moqr_bind_t *b, uint64_t now_us);
static void usub_store_key(moqr_bind_t *b, b_usub_t *us, const moq_bytes_t *parts,
                           uint32_t ns_count, moq_bytes_t name);
static void usub_release(moqr_bind_t *b, b_usub_t *us);
static void usub_release_all(moqr_bind_t *b, b_conn_t *cn);

static b_meta_publish_t *
bind_meta_pub_find(moqr_bind_t *b, const b_conn_t *cn, uint64_t pub)
{
    for (uint32_t i = 0; i < b->metadata_publish_cap; i++) {
        b_meta_publish_t *p = &b->metadata_publishes[i];
        if (p->state != 0 && moqr_binding_eq(p->binding, cn->binding) &&
            p->pub == pub) return p;
    }
    return NULL;
}

static b_meta_publish_t *
bind_meta_pub_reserve(moqr_bind_t *b, const b_conn_t *cn, uint64_t pub)
{
    for (uint32_t i = 0; i < b->metadata_publish_cap; i++) {
        b_meta_publish_t *p = &b->metadata_publishes[i];
        if (p->state == 0) {
            p->state = B_META_PUB_CORE;
            p->binding = cn->binding;
            p->cookie = (uint64_t)(cn - b->conns) + 1u;
            p->pub = pub;
            return p;
        }
    }
    return NULL;   /* event polling reserves headroom before consuming input */
}

static void
bind_meta_pub_release(moqr_bind_t *b, b_meta_publish_t *p)
{
    moqr_bind_meta_release(b, &p->snapshot);
    memset(p, 0, sizeof(*p));
}

/* Refuse the PUBLISH a metadata stage belongs to: the stage is released and
 * the refusal goes through the connection's held-terminal table like every
 * other, so one the session cannot take yet is held, never lost. */
static void
bind_meta_pub_refuse_ex(moqr_bind_t *b, b_conn_t *cn, b_meta_publish_t *p,
                        uint64_t code, bool can_retry, uint64_t retry_ms,
                        uint64_t now_us)
{
    uint64_t handle = p->pub;
    bind_meta_pub_release(b, p);
    bind_refuse(b, cn, MOQR_AUTH_PUBLISH, handle, code, can_retry, retry_ms,
                now_us);
}

static void
bind_meta_pub_refuse(moqr_bind_t *b, b_conn_t *cn, b_meta_publish_t *p,
                     uint64_t code, uint64_t now_us)
{
    bind_meta_pub_refuse_ex(b, cn, p, code, false, 0, now_us);
}

static void
bind_meta_pub_purge(moqr_bind_t *b, const b_conn_t *cn)
{
    for (uint32_t i = 0; i < b->metadata_publish_cap; i++) {
        b_meta_publish_t *p = &b->metadata_publishes[i];
        if (p->state != 0 && moqr_binding_eq(p->binding, cn->binding)) {
            bind_meta_pub_release(b, p);
        }
    }
}

static moqr_bind_meta_ref_t
bind_meta_input(const b_conn_t *cn, uint64_t handle, uint64_t gen,
                 moq_bytes_t bytes)
{
    moqr_bind_meta_ref_t input;
    memset(&input, 0, sizeof(input));
    input.state = bytes.len == 0 ? MOQR_BIND_META_EMPTY : MOQR_BIND_META_PRESENT;
    input.wire_version = moq_session_version(cn->session);
    input.source_binding = cn->binding;
    input.source_handle = handle;
    input.source_generation = gen;
    input.bytes = bytes;
    return input;
}

/* Validated Track Properties share the opaque vi64 representation in D18
 * and D22. Preserve the bytes, source profile and destination validation;
 * this is not a mandatory-property override or a LOC/media conversion. Empty
 * sets have no representation to convert. No nonempty D16 cross-profile
 * compatibility is implied. */
static bool
bind_meta_can_replay(const moqr_bind_meta_ref_t *pin, moq_version_t version)
{
    if (pin == NULL) return false;
    if (pin->state == MOQR_BIND_META_EMPTY) return true;
    if (pin->state != MOQR_BIND_META_PRESENT) return false;
    bool source_18_22 = pin->wire_version == MOQ_VERSION_DRAFT_18 ||
                        pin->wire_version == MOQ_VERSION_DRAFT_22;
    bool destination_18_22 = version == MOQ_VERSION_DRAFT_18 ||
                             version == MOQ_VERSION_DRAFT_22;
    return pin->wire_version == version || (source_18_22 && destination_18_22);
}

/* The Track Properties a reply this relay originates for an established
 * track states (TRACK_STATUS_OK, FETCH_OK; draft-22 §7.7): the track's
 * recorded set, or EMPTY when this binding does not manage the source's
 * metadata. A managed track with nothing usable recorded stays UNAVAILABLE,
 * which bind_meta_can_replay refuses, as it refuses an ACK. */
static void
bind_meta_reply_pin(moqr_bind_t *b, moqr_track_t track, moqr_bind_meta_ref_t *pin)
{
    if (moqr_bind_meta_latest(b, track, pin) != MOQR_OK) return;
    uint32_t slot = moq_handle_slot(track._opaque);
    if (slot < b->metadata_cap && moqr_track_eq(b->metadata[slot].track, track))
        return; /* A recorded obligation, including UNAVAILABLE, wins. */
    bool managed = true;
    if (moqr_core_track_metadata_managed(b->core, track, &managed) != MOQR_OK ||
        managed) return;
    pin->state = MOQR_BIND_META_EMPTY;
}

/* The properties of a FETCH_OK this relay originates from the core fetch
 * `cfetch`, retained in *pin for the call. False when the track's set cannot
 * be stated on this session; the fetch is refused rather than answered
 * without them. */
static bool
bind_fetch_ok_pin(moqr_bind_t *b, const b_conn_t *cn, moqr_fetch_t cfetch,
                  moqr_bind_meta_ref_t *pin)
{
    moqr_track_t track;
    uint64_t gen;
    if (moqr_core_internal_fetch_track(b->core, cfetch, &track, &gen) != MOQR_OK)
        return false;
    bind_meta_reply_pin(b, track, pin);
    return bind_meta_can_replay(pin, moq_session_version(cn->session));
}

/* Runs for every fresh scalar BEFORE the blocked fast path. PUBLISH input
 * was copied before parking/admission; its core ACCEPT supplies the exact
 * accepted track/source identity even when its wire response must wait. */
static void
bind_meta_prepare_reply(moqr_bind_t *b, moqr_intent_t *it,
                        moqr_bind_meta_ref_t *pin)
{
    if (it->kind == MOQR_INTENT_ACCEPT_PUBLISH ||
        it->kind == MOQR_INTENT_REJECT_PUBLISH) {
        b_conn_t *cn = conn_by_cookie(b, it->binding_cookie);
        if (cn == NULL || cn->closed) return;   /* core-only/unrouted source */
        b_meta_publish_t *p = bind_meta_pub_find(b, cn, it->cookie);
        if (it->kind == MOQR_INTENT_ACCEPT_PUBLISH) {
            if (p != NULL && p->state == B_META_PUB_CORE &&
                bind_meta_track_current(b, it->track, it->track_gen) == MOQR_OK) {
                p->snapshot.source_generation = it->track_gen;
                bind_meta_commit(b, it->track, it->track_gen, &p->snapshot);
            } else {
                (void)moqr_core_upstream_lost(b->core, it->track, it->track_gen);
                it->kind = MOQR_INTENT_REJECT_PUBLISH;
                it->error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
            }
        }
        if (p != NULL) bind_meta_pub_release(b, p);
    }
    if (it->kind == MOQR_INTENT_TRACK_STATUS_OK) {
        bind_meta_reply_pin(b, it->track, pin);
        return;
    }
    if (it->kind == MOQR_INTENT_ACCEPT_SUB) {
        moqr_track_t track;
        uint64_t generation;
        if (moqr_core_sub_track(b->core, it->sub, &track, &generation) != MOQR_OK ||
            moqr_bind_meta_latest(b, track, pin) != MOQR_OK) return;
        uint32_t slot = moq_handle_slot(track._opaque);
        if (slot >= b->metadata_cap || moqr_track_eq(b->metadata[slot].track, track))
            return; /* A recorded obligation, including UNAVAILABLE, wins. */
        bool managed = true;
        moqr_sub_state_t state;
        if (moqr_core_track_metadata_managed(b->core, track, &managed) != MOQR_OK ||
            managed || moqr_core_sub_state(b->core, it->sub, &state) != MOQR_OK ||
            state != MOQR_SUB_ACTIVE) return;
        /* Fresh PENDING demand cannot ACK. An ACTIVE sub proves admission,
         * including a retained direct identity's WARM rejoin. Do not put this
         * exception in generic latest lookup, which also serves pending demand. */
        pin->state = MOQR_BIND_META_EMPTY;
        pin->source_generation = generation;
    }
}

static bool
bind_try_pinned(moqr_bind_t *b, const moqr_intent_t *it, uint64_t now_us,
                 uint8_t mode, bool *parked, moqr_intent_t *defer,
                 const moqr_bind_meta_ref_t *pin)
{
    b->metadata_executing_intent = it;
    b->metadata_executing = pin;
    bool done = bind_try_intent(b, it, now_us, mode, parked, defer);
    b->metadata_executing_intent = NULL;
    b->metadata_executing = NULL;
    return done;
}

/* The ordered ring is full for this intent: the connection it targets can no
 * longer be given output in order, so close it rather than continue a live
 * session that has lost mandatory ordered output. Other connections' retained
 * entries are untouched. */
static void
bind_ordered_overflow_close(moqr_bind_t *b, const moqr_intent_t *it,
                            uint64_t now_us)
{
    b_conn_t *cn = conn_by_cookie(b, it->binding_cookie);
    if (cn == NULL || cn->closed) {
        return;
    }
    b->ordered_failed_closed++;
    (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                            "ordered output ring exhausted", now_us);
    conn_detach(b, cn, now_us);
}

/* Park a namespace update in the SAME ordered ring as every scalar terminal,
 * with its borrowed suffix deep-copied into the slot's payload sidecar. One
 * queue is what makes ordering exact: an update can neither overtake nor be
 * overtaken by control output queued around it.
 * false => the ring or the copy is exhausted; the caller must fail closed. */
static bool
bind_defer_push_nsu(moqr_bind_t *b, const moqr_intent_t *it)
{
    if (!bind_ordered_room(b, true)) {
        return false;
    }
    uint32_t slot = (b->pending_head + b->pending_count) % b->pending_cap;
    moqr_intent_t parked = *it;
    if (!bind_nsu_store(b, slot, &parked)) {
        return false;
    }
    bind_pending_append(b, &parked);   /* admitted above, as a namespace */
    return true;
}

/* Flush the core's outward intents to their sessions, durably. The ENTIRE core
 * ring is drained every pump: borrowed-payload views cannot survive to the next
 * pump (relay.h:102), so UPSTREAM_SUBSCRIBE is executed inline the moment it is
 * polled. A blocked NS_FOUND / NS_GONE instead parks in the SAME ordered ring
 * with its suffix deep-copied, so draft-18 Section 6.2's MUST survives without
 * letting it overtake or be overtaken. Once anything is blocked, every
 * subsequent parkable output queues behind it (preserving order). Returns true
 * if nothing is parked (caller may deliver), false otherwise (caller holds
 * delivery so no object overtakes a parked terminal). */
static bool
bind_execute_intents(moqr_bind_t *b, uint64_t now_us)
{
    if (b->intent_drain_active) {
#ifdef MOQR_BIND_TESTING
        b->dbg_intent_retry_suppressed++;
#endif
        return false;
    }
    b->intent_drain_active = true;

    /* 1. Retry parked scalars first, in FIFO order. Stop retrying at the first
     *    that still blocks (nothing behind it may overtake it), but fall
     *    through to step 2 so borrowed intents newly in the ring are still
     *    consumed this pump. */
    bool blocked = false;
    while (b->pending_count > 0) {
        moqr_intent_t defer;
        moqr_intent_t current = b->pending[b->pending_head];
        moqr_bind_meta_ref_t pin = {0};
        (void)moqr_bind_meta_retain(b, &b->pending_metadata[b->pending_head],
                                  &pin);
        uint32_t epoch = b->pending_purge_epoch;
        bool done = bind_try_pinned(b, &current, now_us, BIND_TRY_FROM_RING,
                                    NULL, &defer, &pin);
        moqr_bind_meta_release(b, &pin);
        if (b->pending_purge_epoch != epoch) {
            /* The retry detached its own connection: purge already released
             * this slot and compacted the ring, so the head is now somebody
             * else's entry. Popping here would silently discard it. */
            continue;
        }
        if (!done) {
            b->pending[b->pending_head] = defer;
            if (defer.kind != MOQR_INTENT_ACCEPT_SUB &&
                defer.kind != MOQR_INTENT_TRACK_STATUS_OK) {
                moqr_bind_meta_release(b, &b->pending_metadata[b->pending_head]);
            }
            blocked = true;
            break;
        }
        bind_nsu_release(b, b->pending_head);   /* no-op unless it held bytes */
        moqr_bind_meta_release(b, &b->pending_metadata[b->pending_head]);
        b->pending_head = (b->pending_head + 1u) % b->pending_cap;
        b->pending_count--;
    }
    blocked = blocked || b->pending_count > 0;
    /* 2. Drain the whole ring. Consume-in-pump borrowed intents
     *    (UPSTREAM_SUBSCRIBE) execute inline and are never parked; NS_FOUND /
     *    NS_GONE are borrowed too but DO park, after their suffix is
     *    deep-copied into the slot's sidecar. Both those and scalar intents
     *    park once anything is blocked (order), else execute and park only if
     *    they themselves block. */
    size_t n;
    while ((n = moqr_core_poll_intents(b->core, b->pump_intents,
                                       BIND_PUMP_INTENT_BATCH)) > 0) {
        for (size_t i = 0; i < n; i++) {
            moqr_intent_t *it = &b->pump_intents[i];
            bool is_nsu = it->kind == MOQR_INTENT_NS_FOUND ||
                          it->kind == MOQR_INTENT_NS_GONE;
            if (!bind_intent_deferrable(it->kind) && !is_nsu) {
                /* Router-only / consume-in-pump kinds (UPSTREAM_SUBSCRIBE):
                 * they own no per-session output order to preserve. */
                moqr_intent_t tw;
                (void)bind_try_intent(b, it, now_us, BIND_TRY_FRESH, NULL,
                                      &tw);
                continue;
            }
            moqr_bind_meta_ref_t pin = {0};
            bind_meta_prepare_reply(b, it, &pin);
            if (blocked) {
                /* Anything ordered joins the one FIFO with NO inline send:
                 * an attempt here could reach the wire ahead of what is
                 * already parked. */
                if (is_nsu) {
                    moqr_intent_t tw;
                    (void)bind_try_intent(b, it, now_us,
                                          BIND_TRY_FORCE_PARK, NULL, &tw);
                } else if (!bind_defer_push(b, it, &pin)) {
                    bind_ordered_overflow_close(b, it, now_us);
                }
                moqr_bind_meta_release(b, &pin);
                continue;
            }
            moqr_intent_t defer;
            bool parked = false;
            if (!bind_try_pinned(b, it, now_us, BIND_TRY_FRESH, &parked,
                                 &defer, &pin)) {
                if (!bind_defer_push(b, &defer, &pin)) {
                    bind_ordered_overflow_close(b, &defer, now_us);
                }
                blocked = true;
            } else if (parked) {
                /* A namespace update just joined the FIFO: everything polled
                 * after it must queue behind it, not race it. */
                blocked = true;
            }
            moqr_bind_meta_release(b, &pin);
        }
    }
    b->intent_drain_active = false;
    return !blocked;
}

/* Drop parked outputs that have become moot, keeping every other entry in
 * order: all of a connection's (ns_sub_only false), or only the NAMESPACE
 * updates for one of its namespace subscriptions (ns_sub_only, ns_cookie). */
static void
bind_pending_purge(moqr_bind_t *b, uint64_t binding_cookie, bool ns_sub_only,
                   uint64_t ns_cookie)
{
    uint32_t kept = 0;
    uint32_t scanned = b->pending_count;
    for (uint32_t i = 0; i < scanned; i++) {
        uint32_t from = (b->pending_head + i) % b->pending_cap;
        moqr_intent_t *src = &b->pending[from];
        bool moot = src->binding_cookie == binding_cookie &&
                    (!ns_sub_only ||
                     ((src->kind == MOQR_INTENT_NS_FOUND ||
                       src->kind == MOQR_INTENT_NS_GONE) &&
                      src->cookie == ns_cookie));
        if (moot) {
            bind_nsu_release(b, from);   /* frees any retained namespace bytes */
            moqr_bind_meta_release(b, &b->pending_metadata[from]);
            continue;
        }
        uint32_t to = (b->pending_head + kept) % b->pending_cap;
        if (to != from) {
            /* The payload sidecar is keyed by slot, so it moves with the
             * entry and the parked view is re-pointed at its new home. */
            /* The sidecar bookkeeping moves with the entry; the byte
             * allocation itself does not, so the parked part pointers stay
             * valid without fixup. */
            bind_nsu_release(b, to);
            b->nsu[to] = b->nsu[from];
            memset(&b->nsu[from], 0, sizeof(b->nsu[from]));
            moqr_bind_meta_release(b, &b->pending_metadata[to]);
            b->pending_metadata[to] = b->pending_metadata[from];
            memset(&b->pending_metadata[from], 0,
                   sizeof(b->pending_metadata[from]));
            b->pending[to] = *src;
        }
        kept++;
    }
    if (kept != scanned) {
        b->pending_purge_epoch++;   /* the ring was rewritten under any caller */
    }
    b->pending_count = kept;
}

/* Drop any parked outputs targeting a connection that is tearing down: its
 * session is about to go away, so the writes are moot. Called at close, before
 * the slot (and its binding cookie) can be reused by a new connection. */
static void
bind_pending_purge_conn(moqr_bind_t *b, uint64_t binding_cookie)
{
    bind_pending_purge(b, binding_cookie, false, 0);
}

static bool
bind_retry_may_drain_intents(moqr_bind_t *b)
{
    if (b->intent_drain_active) {
#ifdef MOQR_BIND_TESTING
        b->dbg_intent_retry_suppressed++;
#endif
        return false;
    }
    return true;
}

/* A core call that queues intents may return WOULD_BLOCK when the ring is
 * momentarily full (leftover intents from earlier in the same pump). The
 * core's atomicity invariant guarantees any single fan-out fits an EMPTY
 * ring, so executing the pending intents (a full drain) and retrying makes
 * progress outside an already-active drain. During an intent drain, preserve
 * WOULD_BLOCK for the caller-specific defer path instead of recursively
 * reusing b->pump_intents and overwriting the outer batch. Evaluate the
 * expression, draining+retrying up to a small bound; assign the final result
 * to `dst`. */
#define BIND_CALL_RETRY(dst, expr)                                        \
    do {                                                                  \
        (dst) = (expr);                                                   \
        for (int _r = 0; (dst) == MOQR_ERR_WOULD_BLOCK && _r < 8; _r++) { \
            if (!bind_retry_may_drain_intents(b)) {                       \
                break;                                                    \
            }                                                             \
            (void)bind_execute_intents(b, now_us);                        \
            (dst) = (expr);                                               \
        }                                                                 \
    } while (0)

static void
ann_clear_slot(moqr_bind_t *b, b_conn_t *cn, uint32_t slot)
{
    if (slot >= b->n_anns || cn->anns[slot].used == false) {
        return;
    }
    if (cn->anns[slot].bytes != NULL) {
        b->alloc.free(cn->anns[slot].bytes, cn->anns[slot].bytes_len,
                      b->alloc.ctx);
    }
    memset(&cn->anns[slot], 0, sizeof(cn->anns[slot]));
}

static void
ann_clear_all(moqr_bind_t *b, b_conn_t *cn)
{
    for (uint32_t i = 0; i < b->n_anns; i++) {
        ann_clear_slot(b, cn, i);
    }
}

static int
ann_find(const moqr_bind_t *b, const b_conn_t *cn, uint64_t raw)
{
    for (uint32_t i = 0; i < b->n_anns; i++) {
        if (cn->anns[i].used && cn->anns[i].ann_raw == raw) {
            return (int)i;
        }
    }
    return -1;
}

/* Withdraw this connection's announce `raw` from the core and drop its
 * binding copy. MOQR_OK also when nothing is left to withdraw; WOULD_BLOCK
 * when the withdrawal's fan-out must wait for intent room (nothing changed). */
static moqr_result_t
bind_ann_withdraw(moqr_bind_t *b, b_conn_t *cn, uint64_t raw, uint64_t now_us)
{
    int ann_slot = ann_find(b, cn, raw);
    if (ann_slot < 0) {
        return MOQR_OK;
    }
    moqr_result_t urc;
    BIND_CALL_RETRY(urc, moqr_core_unannounce(b->core, cn->binding,
                                              cn->anns[ann_slot].ns));
    if (urc == MOQR_ERR_WOULD_BLOCK) {
        return urc;
    }
    if (urc == MOQR_OK || urc == MOQR_ERR_STALE_HANDLE ||
        urc == MOQR_ERR_WRONG_STATE) {
        ann_clear_slot(b, cn, (uint32_t)ann_slot);
    } else {
        b->session_errors++;
    }
    return MOQR_OK;
}

static int
ann_store(moqr_bind_t *b, b_conn_t *cn, uint64_t raw, moqr_ns_t ns)
{
    /* draft-18 Section 2.4.1 admits 0..32 fields, so a ROOT namespace (count 0)
     * is a legitimate announce and must be storable; draft-16's 1..32 floor is
     * enforced by the session profile before the event reaches here. Only a
     * NON-EMPTY namespace needs a parts array. */
    if (raw == 0 || ns.count > 32 || (ns.count > 0 && ns.parts == NULL) ||
        ann_find(b, cn, raw) >= 0) {
        return -1;
    }
    int slot = -1;
    for (uint32_t i = 0; i < b->n_anns; i++) {
        if (!cn->anns[i].used) {
            slot = (int)i;
            break;
        }
    }
    if (slot < 0) {
        return -1;
    }
    size_t total = 0;
    for (size_t i = 0; i < ns.count; i++) {
        /* Every PRESENT Track Namespace Field must carry at least one byte
         * (draft-18 and draft-16 Section 2.4.1). Zero fields is a legal ROOT
         * namespace; a zero-LENGTH field is not, whether or not its pointer is
         * non-NULL. */
        if (ns.parts[i].len == 0 || ns.parts[i].data == NULL) {
            return -1;
        }
        if (SIZE_MAX - total < ns.parts[i].len) {
            return -1;
        }
        total += ns.parts[i].len;
    }
    uint8_t *bytes = NULL;
    if (total > 0) {
        bytes = b->alloc.alloc(total, b->alloc.ctx);
        if (bytes == NULL) {
            return -1;
        }
    }
    b_ann_t *a = &cn->anns[slot];
    memset(a, 0, sizeof(*a));
    a->used = true;
    a->ann_raw = raw;
    a->bytes = bytes;
    a->bytes_len = total;
    a->ns.parts = a->parts;
    a->ns.count = ns.count;
    size_t off = 0;
    for (size_t i = 0; i < ns.count; i++) {
        /* Validated above: every present field has len > 0 and non-NULL data. */
        a->parts[i].len = ns.parts[i].len;
        memcpy(bytes + off, ns.parts[i].data, ns.parts[i].len);
        a->parts[i].data = bytes + off;
        off += ns.parts[i].len;
    }
    return slot;
}

static bool
ns_equal(moqr_ns_t a, moqr_ns_t b)
{
    if (a.count != b.count) {
        return false;
    }
    for (size_t i = 0; i < a.count; i++) {
        if (a.parts[i].len != b.parts[i].len ||
            memcmp(a.parts[i].data, b.parts[i].data, a.parts[i].len) != 0) {
            return false;
        }
    }
    return true;
}

static bool
conn_has_other_announce(const moqr_bind_t *b, const b_conn_t *cn,
                        uint64_t raw, moqr_ns_t ns)
{
    for (uint32_t i = 0; i < b->n_anns; i++) {
        const b_ann_t *ann = &cn->anns[i];
        if (ann->used && ann->ann_raw != raw && ns_equal(ann->ns, ns)) {
            return true;
        }
    }
    return false;
}

/* The core has one source slot for an exact namespace. Drafts 16 Section 8.3
 * and 18 Section 9.3 permit establishment time to select a publisher when an
 * implementation cannot retain every source. This binding's deterministic
 * policy is newest-wins across connections: retire the incumbent generation,
 * including every track it sourced, then install the new one. A duplicate on
 * the same connection remains a request error and never displaces itself. */
static moqr_result_t
bind_announce(moqr_bind_t *b, b_conn_t *cn, moqr_ns_t ns, uint64_t raw,
              uint64_t now_us)
{
    moqr_result_t rc;
    BIND_CALL_RETRY(rc, moqr_core_announce_ex(b->core, cn->binding, ns, raw));
    if (rc != MOQR_ERR_WRONG_STATE ||
        conn_has_other_announce(b, cn, raw, ns)) {
        return rc;
    }

    BIND_CALL_RETRY(rc, moqr_core_force_withdraw(
                            b->core, ns, MOQ_REQUEST_ERROR_UNINTERESTED,
                            now_us));
    if (rc != MOQR_OK) {
        return rc;
    }
    BIND_CALL_RETRY(rc, moqr_core_announce_ex(b->core, cn->binding, ns, raw));
    return rc;
}

#ifdef MOQR_BIND_TESTING
/* Drive the PRODUCTION ann_store validator on one live conn slot.
 *
 * A malformed present Track Namespace Field cannot arrive through a real
 * session -- the parser rejects it first -- so this is the only way to exercise
 * bind's defense-in-depth arm without copying the predicate into the test.
 * Returns true when ann_store ACCEPTED the namespace; an accepted slot is
 * released again so the probe leaves no announcement state behind. */
bool
moqr_bind_debug_ann_store_probe(moqr_bind_t *b, uint32_t slot, uint64_t raw,
                                moqr_ns_t ns)
{
    if (b == NULL || slot >= b->max_conns || !b->conns[slot].used) {
        return false;
    }
    int r = ann_store(b, &b->conns[slot], raw, ns);
    if (r < 0) {
        return false;
    }
    ann_clear_slot(b, &b->conns[slot], (uint32_t)r);
    return true;
}
#endif

/* An upstream terminal code is that connection's draft, not ours. Capture the
 * origin with the code at ingest: the record may be replayed to a subscriber
 * on the other draft long after this connection is gone, and by then nothing
 * else can say which registry the number was written in. */
static moqr_reset_desc_t
bind_upstream_reset(const moqr_bind_t *b, const b_conn_t *cn, uint64_t wire)
{
    moqr_reset_desc_t d;

    if (moqr_reset_desc_wire(moqr_bind_conn_version(b, cn->session), wire,
                             &d) != MOQR_OK) {
        return moqr_reset_desc_none();
    }
    return d;
}

/* Encode a retained reset cause for the connection about to read it. This is
 * the single translation point: the descriptor carries its own origin, so a
 * draft-16 terminal replayed to a draft-18 subscriber is renumbered here and
 * nowhere else. A descriptor with nothing truthful to say fails the delivery
 * closed rather than emitting a plausible number. */
static bool
bind_reset_wire(const moqr_bind_t *b, const b_conn_t *cn,
                moqr_reset_desc_t desc, uint64_t *out)
{
    return moqr_reset_desc_emit(desc, moqr_bind_conn_version(b, cn->session),
                                out) == MOQR_OK;
}

/* An upstream PUBLISH_DONE status is that connection's draft. Capture the
 * origin with it: downstream subscribers may read the other draft, and the
 * terminal outlives this connection. */
static moqr_pd_desc_t
bind_upstream_pd(const moqr_bind_t *b, const b_conn_t *cn, uint64_t wire)
{
    moqr_pd_desc_t d;

    if (moqr_pd_desc_wire(moqr_bind_conn_version(b, cn->session), wire, &d) !=
        MOQR_OK) {
        return moqr_pd_desc_none();
    }
    return d;
}

/* A terminal this relay chose is a meaning, not a draft's number. */
static moqr_pd_desc_t
bind_local_pd(moqr_pd_status_t status)
{
    moqr_pd_desc_t d;

    if (moqr_pd_desc_local(status, &d) != MOQR_OK) {
        return moqr_pd_desc_none();
    }
    return d;
}

static void
conn_detach(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    if (!cn->used || cn->closed) {
        return;
    }
    cn->closed = true;   /* no new work routes to this conn */
    bind_dup_owners_release(b, cn);
    /* A fill reading through the fill source ends its claim now: the source
     * holds resources on its behalf until it is told, and this table is
     * cleared without another look when the slot is reused. */
    for (uint32_t fi = 0; fi < b->n_fetches; fi++) {
        b_fetch_t *bf = &cn->fetches[fi];
        if (bf->used && bf->fill_src_open) {
            b->fill_src.close(b->fill_src.ctx, bf->fill_src_handle);
            bf->fill_src_open = false;
        }
        if (bf->used && bf->sa_remote) {
            b->sa_src.close(b->sa_src.ctx, bf->sa_handle);
            bf->sa_remote = false;
        }
        if (bf->used && bf->retrieval.live) {
            moqr_bind_retrieve_close(b, &bf->retrieval);
        }
    }
    fwd_conn_gone(b, cn);
    bind_refusal_conn_gone(b, cn);
    bind_meta_pub_purge(b, cn);
    bind_nsans_conn_gone(b, cn);
    bind_dl_clear_conn(b, cn);
    /* Retire this binding's parked (DEFERred) requests and revalidation grants
     * before the core binding close, freeing their deep-copied bytes: a later
     * scheduled recheck can no longer touch a torn-down session. Drop any
     * parked output intents for this conn too — its session is going away, so
     * they are moot, and the cookie (= slot + 1) may be reused. */
    uint64_t bcookie = (uint64_t)(cn - b->conns) + 1u;
    bind_pending_purge_conn(b, bcookie);
    moqr_core_retire_parked(b->core, bcookie);
    moqr_core_retire_grants(b->core, bcookie);   /* held reservations too */
    bind_dgrant_conn_gone(b, cn);
    moqr_result_t rc;
    BIND_CALL_RETRY(rc, moqr_core_binding_close(b->core, cn->binding,
                                                now_us));
    if (rc == MOQR_ERR_WOULD_BLOCK) {
        cn->detach_pending = true;   /* retried after each pump's drain */
#ifdef MOQR_BIND_TESTING
        if (bind_dbg_detach_pending_fn != NULL) {
            bind_dbg_detach_pending_fn(b, cn->session,
                                       bind_dbg_detach_pending_ctx);
        }
#endif
        return;
    }
    if (rc != MOQR_OK) {
        b->session_errors++;
    }
    ann_clear_all(b, cn);
    usub_release_all(b, cn);
    cn->used = false;
    cn->detach_pending = false;
    cn->session = NULL;
}

void
moqr_bind_destroy(moqr_bind_t *b)
{
    if (b == NULL) {
        return;
    }
    moq_alloc_t a = b->alloc;
    /* Strictly memory-only: destroy calls NO session and NO core APIs.
     * It runs at process shutdown AFTER the transport (and its sessions)
     * are gone, so touching a session here would be use-after-free, and
     * executing intents would deliver to dead sessions. Per-connection
     * session teardown happens live on the network thread via conn_detach
     * (SESSION_CLOSED / moqr_bind_conn_close); the owning core is destroyed
     * separately by the caller, which frees any core bindings still open. */
    if (b->conns != NULL) {
        for (uint32_t i = 0; i < b->max_conns; i++) {
            b_conn_t *cn = &b->conns[i];
            if (cn->dsubs != NULL) {
                a.free(cn->dsubs, (size_t)b->n_dsubs * sizeof(*cn->dsubs),
                       a.ctx);
            }
            if (cn->sgs != NULL) {
                a.free(cn->sgs, (size_t)b->n_sgs * sizeof(*cn->sgs), a.ctx);
            }
            if (cn->usubs != NULL) {
                usub_release_all(b, cn);
                a.free(cn->usubs, (size_t)b->n_usubs * sizeof(*cn->usubs),
                       a.ctx);
            }
            if (cn->pubs != NULL) {
                a.free(cn->pubs, (size_t)b->n_pubs * sizeof(*cn->pubs),
                       a.ctx);
            }
            if (cn->anns != NULL) {
                ann_clear_all(b, cn);
                a.free(cn->anns, (size_t)b->n_anns * sizeof(*cn->anns),
                       a.ctx);
            }
            if (cn->fetches != NULL) {
                a.free(cn->fetches, (size_t)b->n_fetches * sizeof(*cn->fetches),
                       a.ctx);
            }
            if (cn->refusals != NULL) {
                a.free(cn->refusals,
                       (size_t)b->n_refusals * sizeof(*cn->refusals), a.ctx);
            }
        }
        a.free(b->conns, (size_t)b->max_conns * sizeof(*b->conns), a.ctx);
    }
    if (b->fwd != NULL) {
        /* Memory-only: owned references the transactions still hold are
         * released without any session call. */
        for (uint32_t i = 0; i < b->n_fwd; i++) {
            b_fwd_t *t = &b->fwd[i];
            if (!t->used) {
                continue;
            }
            for (uint32_t k = t->next_write; k < t->n_items; k++) {
                if (t->items[k].payload != NULL) {
                    moq_rcbuf_decref(t->items[k].payload);
                }
                if (t->items[k].properties != NULL) {
                    moq_rcbuf_decref(t->items[k].properties);
                }
            }
            if (t->props != NULL) a.free(t->props, t->props_len, a.ctx);
            if (t->reason != NULL) a.free(t->reason, t->reason_len, a.ctx);
            if (t->key != NULL) a.free(t->key, t->key_len, a.ctx);
        }
        a.free(b->fwd, (size_t)b->n_fwd * sizeof(*b->fwd), a.ctx);
    }
    if (b->cached_holds != NULL) {
        for (uint32_t i = 0; i < b->n_fwd; i++) {
            moq_rcbuf_decref(b->cached_holds[i].payload);
            moq_rcbuf_decref(b->cached_holds[i].properties);
        }
        a.free(b->cached_holds,
               (size_t)b->n_fwd * sizeof(*b->cached_holds), a.ctx);
    }
    {
        size_t dw = (size_t)bind_dl_word_count(b->max_conns) *
                    sizeof(uint64_t);
        if (b->dl_ready != NULL) {
            a.free(b->dl_ready, dw, a.ctx);
        }
        if (b->dl_parked != NULL) {
            a.free(b->dl_parked, dw, a.ctx);
        }
    }
    if (b->pump_intents != NULL) {
        a.free(b->pump_intents,
               (size_t)BIND_PUMP_INTENT_BATCH * sizeof(*b->pump_intents),
               a.ctx);
    }
    if (b->pump_revoked != NULL) {
        a.free(b->pump_revoked,
               (size_t)BIND_PUMP_REVOKED_BATCH * sizeof(*b->pump_revoked),
               a.ctx);
    }
    if (b->pump_events != NULL) {
        a.free(b->pump_events,
               (size_t)BIND_PUMP_EVENT_BATCH * sizeof(*b->pump_events),
               a.ctx);
    }
    if (b->nsu != NULL) {
        for (uint32_t i = 0; i < b->nsu_cap; i++) {
            if (b->nsu[i].used && b->nsu[i].bytes != NULL) {
                a.free(b->nsu[i].bytes, b->nsu[i].bytes_len, a.ctx);
            }
        }
        a.free(b->nsu, (size_t)b->nsu_cap * sizeof(*b->nsu), a.ctx);
    }
    if (b->nsans != NULL) {
        a.free(b->nsans, (size_t)b->nsans_cap * sizeof(*b->nsans), a.ctx);
    }
    if (b->pending != NULL) {
        a.free(b->pending, (size_t)b->pending_cap * sizeof(*b->pending), a.ctx);
    }
    if (b->pending_metadata != NULL) {
        for (uint32_t i = 0; i < b->pending_cap; i++) {
            moqr_bind_meta_release(b, &b->pending_metadata[i]);
        }
        a.free(b->pending_metadata,
               (size_t)b->pending_cap * sizeof(*b->pending_metadata), a.ctx);
    }
    if (b->metadata_publishes != NULL) {
        for (uint32_t i = 0; i < b->metadata_publish_cap; i++) {
            bind_meta_pub_release(b, &b->metadata_publishes[i]);
        }
        a.free(b->metadata_publishes,
               (size_t)b->metadata_publish_cap * sizeof(*b->metadata_publishes),
               a.ctx);
    }
    if (b->metadata != NULL) {
        for (uint32_t i = 0; i < b->metadata_cap; i++) {
            moqr_bind_meta_release(b, &b->metadata[i].latest);
        }
        a.free(b->metadata, (size_t)b->metadata_cap * sizeof(*b->metadata), a.ctx);
    }
    a.free(b, sizeof(*b), a.ctx);
}

static b_conn_t *
conn_by_session(moqr_bind_t *b, const moq_session_t *s)
{
    for (uint32_t i = 0; i < b->max_conns; i++) {
        if (b->conns[i].used && b->conns[i].session == s) {
            return &b->conns[i];
        }
    }
    return NULL;
}

static b_conn_t *
conn_by_cookie(moqr_bind_t *b, uint64_t cookie)
{
    if (cookie < 1 || cookie > b->max_conns) {
        return NULL;
    }
    b_conn_t *cn = &b->conns[cookie - 1];
    return cn->used ? cn : NULL;
}

moqr_result_t
moqr_bind_conn_open(moqr_bind_t *b, moq_session_t *session,
                    moq_version_t version)
{
    if (b == NULL || session == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* Refuse an unrecognized draft before anything is taken. Every terminal
     * this binding emits is encoded for the draft the peer reads, so a
     * connection whose draft is unknown has no safe encoding — and defaulting
     * to a plausible one is the silent wrong-numbering this parameter exists
     * to prevent. Rejecting here also keeps a refused attach free of cost: no
     * slot, no core binding. */
    uint8_t draft;
    switch (version) {
    case MOQ_VERSION_DRAFT_16: draft = B_DRAFT_16; break;
    case MOQ_VERSION_DRAFT_18: draft = B_DRAFT_18; break;
    case MOQ_VERSION_DRAFT_22: draft = B_DRAFT_22; break;
    default:                   return MOQR_ERR_INVAL;
    }
    if (conn_by_session(b, session) != NULL) {
        return MOQR_ERR_INVAL;   /* duplicate attach */
    }
    for (uint32_t i = 0; i < b->max_conns; i++) {
        b_conn_t *cn = &b->conns[i];
        if (!cn->used) {
            moqr_binding_t binding;
            moqr_binding_options_t options;
            moqr_binding_options_init_sized(&options, sizeof(options));
            options.flags = MOQR_BINDING_TRACK_METADATA_MANAGED;
            moqr_result_t rc = moqr_core_binding_open_with_options(
                b->core, (uint64_t)i + 1, &options, &binding);
            if (rc != MOQR_OK) {
                return rc;
            }
            cn->used = true;
            cn->closed = false;
            cn->session = session;
            cn->wire_draft = draft;
            cn->binding = binding;
            bind_dl_clear(b->dl_ready, i);
            bind_dl_clear(b->dl_parked, i);
            cn->park_reason = B_PARK_NONE;
            cn->park_cap = 0;
#ifdef MOQR_BIND_TESTING
            memset(cn->dbg_blocked, 0, sizeof(cn->dbg_blocked));
            cn->dbg_bind_sg_track = 0;
            cn->dbg_bind_sg_track_gen = 0;
            cn->dbg_bind_sg_track_set = false;
            cn->dbg_bind_sg_ambiguous = false;
#endif
            memset(cn->dsubs, 0, (size_t)b->n_dsubs * sizeof(*cn->dsubs));
            bind_dgrant_conn_gone(b, cn);
            memset(cn->sgs, 0, (size_t)b->n_sgs * sizeof(*cn->sgs));
            usub_release_all(b, cn);   /* owned keys, then the clean table */
            memset(cn->pubs, 0, (size_t)b->n_pubs * sizeof(*cn->pubs));
            memset(cn->fetches, 0, (size_t)b->n_fetches * sizeof(*cn->fetches));
            memset(cn->refusals, 0, (size_t)b->n_refusals * sizeof(*cn->refusals));
            cn->refusals_reserved = 0;
            /* The relay answers every downstream subscription update itself
             * (bind_update_answer), so nothing is acknowledged before the
             * relay has decided. */
            (void)moq_session_set_defer_subscription_updates(session, true);
            /* A FETCH range stated relative to the Largest Object is the
             * relay's to resolve, against its own log (bind_fetch_range). */
            (void)moq_session_set_defer_fetch_ranges(session, true);
            /* Draft-22 namespace updates are the relay's to authorize and
             * answer (bind_nsu_authorize); older drafts have none. */
            if (draft == B_DRAFT_22) {
                (void)moq_session_set_defer_namespace_updates(session, true);
            }
            return MOQR_OK;
        }
    }
    return MOQR_ERR_CAPACITY;
}

moq_version_t
moqr_bind_conn_version(const moqr_bind_t *b, const moq_session_t *session)
{
    const b_conn_t *cn = (b == NULL || session == NULL)
                             ? NULL
                             : conn_by_session((moqr_bind_t *)(void *)b,
                                               session);

    if (cn == NULL) {
        return (moq_version_t)0;   /* unattached names no draft */
    }
    switch (cn->wire_draft) {
    case B_DRAFT_22: return MOQ_VERSION_DRAFT_22;
    case B_DRAFT_18: return MOQ_VERSION_DRAFT_18;
    default:         return MOQ_VERSION_DRAFT_16;
    }
}

bool
moqr_bind_conn_is_open(const moqr_bind_t *b, const moq_session_t *session)
{
    return b != NULL && session != NULL &&
           conn_by_session((moqr_bind_t *)(void *)b, session) != NULL;
}

moqr_result_t
moqr_bind_conn_close(moqr_bind_t *b, moq_session_t *session)
{
    if (b == NULL || session == NULL) {
        return MOQR_ERR_INVAL;
    }
    b_conn_t *cn = conn_by_session(b, session);
    if (cn == NULL) {
        return MOQR_OK;   /* idempotent */
    }
    conn_detach(b, cn, 0);
    /* binding_close is resumable. A connection retained for the next pump
     * still owns its session pointer, so returning OK would falsely authorize
     * the caller to destroy storage the binding can still reference. */
    return conn_by_session(b, session) != NULL ? MOQR_ERR_WOULD_BLOCK
                                                : MOQR_OK;
}

/* -- event translation (the parity-proven table) --------------------------- */

static moqr_ns_t
ns_from_event(const moq_namespace_t *ns, moq_bytes_t *storage)
{
    size_t count = ns->count < 32 ? ns->count : 32;
    for (size_t i = 0; i < count; i++) {
        storage[i] = ns->parts[i];
    }
    return (moqr_ns_t){ .parts = storage, .count = count };
}

/* Copy a request's namespace (and optional name) out of session event memory
 * into the binding's own stash, repointing the views. A core call retried with
 * BIND_CALL_RETRY drains intents between attempts, and executing an intent on
 * the same session can reset that session's event scratch; a retry must still
 * read the request's own bytes. Views too large to stash are left as they are:
 * the core refuses them as INVAL on the first attempt, before any retry. One
 * stash serves the one event being handled. */
static void
bind_stash_ftn(moqr_bind_t *b, moqr_ns_t *ns, moq_bytes_t *name)
{
    size_t total = name != NULL ? name->len : 0;
    if (ns->count > 32) {
        return;
    }
    for (size_t i = 0; i < ns->count; i++) {
        total += ns->parts[i].len;
    }
    if (total > sizeof(b->stash_bytes)) {
        return;
    }
    size_t off = 0;
    for (size_t i = 0; i < ns->count; i++) {
        size_t len = ns->parts[i].len;
        if (len != 0) {
            memcpy(b->stash_bytes + off, ns->parts[i].data, len);
        }
        b->stash_parts[i].data = len != 0 ? b->stash_bytes + off : NULL;
        b->stash_parts[i].len = len;
        off += len;
    }
    ns->parts = b->stash_parts;
    if (name != NULL) {
        if (name->len != 0) {
            memcpy(b->stash_bytes + off, name->data, name->len);
            name->data = b->stash_bytes + off;
        }
    }
}

/* Session-level error code for an unauthorized connection (setup DENY). This
 * is the SESSION close code (0x2), distinct from the request-level
 * MOQ_REQUEST_ERROR_UNAUTHORIZED (0x1) used to reject individual requests. */
#define BIND_SESSION_UNAUTHORIZED 0x2u

/* The outcome of authorizing one control request. */
typedef enum {
    BIND_AUTH_ALLOW = 0,   /* proceed with the op                          */
    BIND_AUTH_DENY,        /* reject with *err                             */
    BIND_AUTH_DEFER,       /* park under *ticket; resolve later            */
    BIND_AUTH_FAIL_CLOSED, /* token mirror failed / DEFER without a ticket */
} bind_auth_result_t;

/* Authorize a control request against the core's auth hook via the shared,
 * unit-tested seam. On DENY/FAIL_CLOSED, *err gets the wire error code; on
 * DEFER, *ticket gets the hook's (nonzero) correlation ticket; on ALLOW, *lease
 * (when non-NULL) gets the verdict's revalidate_after_us — the caller passes a
 * pointer only for the grantable actions (SUBSCRIBE / PUBLISH_NAMESPACE) that
 * create a CAT revalidation grant, and NULL everywhere else. */
static bind_auth_result_t
bind_authorize(moqr_bind_t *b, const b_conn_t *cn, moqr_auth_action_t action,
               moqr_ns_t ns, moq_bytes_t name,
               const moq_resolved_token_t *tokens, size_t token_count,
               uint64_t now_us, uint64_t *err, uint64_t *ticket, uint64_t *lease)
{
    moqr_auth_verdict_t v;
    if (!moqr_bind_auth_eval(b->core, action, ns, name, tokens, token_count,
                             (uint64_t)(cn - b->conns) + 1u, now_us, &v)) {
        if (err != NULL) {
            *err = MOQ_REQUEST_ERROR_UNAUTHORIZED;
        }
        return BIND_AUTH_FAIL_CLOSED; /* token set couldn't be mirrored */
    }
    switch (v.decision) {
    case MOQR_AUTH_ALLOW:
        if (lease != NULL) {
            *lease = v.revalidate_after_us;
        }
        return BIND_AUTH_ALLOW;
    case MOQR_AUTH_DEFER:
        if (v.ticket == 0) {
            if (err != NULL) {
                *err = MOQ_REQUEST_ERROR_UNAUTHORIZED;
            }
            return BIND_AUTH_FAIL_CLOSED; /* DEFER needs a nonzero ticket */
        }
        if (ticket != NULL) {
            *ticket = v.ticket;
        }
        return BIND_AUTH_DEFER;
    default: /* canonicalized DENY */
        if (err != NULL) {
            *err = v.error_code != 0 ? v.error_code
                                     : MOQ_REQUEST_ERROR_UNAUTHORIZED;
        }
        return BIND_AUTH_DENY;
    }
}

/* Legacy profiles use the relay's monotonic filter model. Omission means
 * unfiltered, not the relay request initializer's Largest default.
 * D22 passes the semantic value directly to relay-core. */
static moqr_result_t
bind_map_location_filter(const moq_location_filter_cfg_t *loc,
                         moqr_filter_t *out)
{
    moqr_subscribe_req_t defaults;
    moqr_subscribe_req_init(&defaults);
    *out = defaults.filter;
    switch (loc->kind) {
    case MOQ_LOCATION_FILTER_NONE:
        out->type = MOQR_FILTER_ABSOLUTE_START;
        out->start_group = 0;
        out->start_object = 0;
        return MOQR_OK;
    case MOQ_LOCATION_FILTER_NEXT_OBJECT:
        out->type = MOQR_FILTER_LARGEST_OBJECT;
        return MOQR_OK;
    case MOQ_LOCATION_FILTER_RELATIVE_GROUP:
        if (loc->relative_group_offset == 0) {
            out->type = MOQR_FILTER_NEXT_GROUP_START;
            return MOQR_OK;
        }
        if (loc->relative_group_offset == 1) {
            out->type = MOQR_FILTER_CURRENT_GROUP_START;
            return MOQR_OK;
        }
        out->type = MOQR_FILTER_RELATIVE_START;
        out->start_group = loc->relative_group_offset;
        return MOQR_OK;
    case MOQ_LOCATION_FILTER_ABSOLUTE_START:
        out->type = MOQR_FILTER_ABSOLUTE_START;
        out->start_group = loc->start_group;
        out->start_object = loc->start_object;
        return MOQR_OK;
    case MOQ_LOCATION_FILTER_GROUP_END:
        if (loc->end_group < loc->start_group) {
            return MOQR_ERR_INVAL;
        }
        out->type = MOQR_FILTER_ABSOLUTE_RANGE;
        out->start_group = loc->start_group;
        out->start_object = loc->start_object;
        out->end_group_delta = loc->end_group - loc->start_group;
        return MOQR_OK;
    case MOQ_LOCATION_FILTER_OBJECT_END:
    default:
        return MOQR_ERR_UNSUPPORTED;
    }
}

static void
bind_location_for_test(moq_location_filter_cfg_t *loc)
{
#ifdef MOQR_BIND_TESTING
    uint32_t forced = atomic_exchange(&bind_dbg_update_filter_kind, 0);
    if (forced != 0) {
        loc->kind = forced;
        loc->relative_group_offset = 2;
        loc->end_group = loc->start_group;
        loc->end_object = 0;
    }
#else
    (void)loc;
#endif
}

/* Legacy profiles retain the old relay filter contract. */
static moqr_result_t
bind_map_update_filter(moq_location_filter_cfg_t *loc, moqr_filter_t *out)
{
    bind_location_for_test(loc);
    return bind_map_location_filter(loc, out);
}

/* The filter in effect on a downstream subscription as the session knows it
 * (the request's, or the replacement an accepted update carried), onto the
 * relay filter. */
static moqr_result_t
bind_read_sub_filter(b_conn_t *cn, moq_subscription_t sub, moqr_filter_t *out)
{
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    if (moq_session_subscription_filter(cn->session, sub, &loc) != MOQ_OK) {
        moqr_subscribe_req_t defaults;
        moqr_subscribe_req_init(&defaults);
        *out = defaults.filter;
        return MOQR_ERR_INVAL;
    }
    return bind_map_update_filter(&loc, out);
}

static moqr_result_t
bind_read_sub_location(b_conn_t *cn, moq_subscription_t sub,
                       moq_location_filter_cfg_t *out)
{
    moq_location_filter_cfg_init(out);
    if (moq_session_subscription_filter(cn->session, sub, out) != MOQ_OK) {
        return MOQR_ERR_INVAL;
    }
    bind_location_for_test(out);
    return MOQR_OK;
}

/* Whether this connection's draft keeps a subscription established once
 * Largest Object passes the end of its Location Filter. Draft-16/18 end it
 * with PUBLISH_DONE SUBSCRIPTION_ENDED; draft-22 removed that status and its
 * publisher does not end the subscription for that reason. */
static bool
bind_range_end_holds(const b_conn_t *cn)
{
    return cn->wire_draft == B_DRAFT_22;
}

/* Whether a subscription update on this connection's draft carries a
 * replacement Location Filter the relay applies. Draft-22's REQUEST_UPDATE
 * carries LOCATION_FILTER as a parameter; the draft-16/18 SUBSCRIBE_UPDATE
 * window is left to the session, as before. */
static bool
bind_update_filter_applies(const b_conn_t *cn)
{
    return cn->wire_draft == B_DRAFT_22;
}

/* Typed UPDATE selections, not the inactive legacy projections. An empty or
 * uncovered record selects no replacement; it never restores a legacy filter. */
static const moq_subscription_update_options_t *
bind_update_options(const moq_event_t *ev)
{
    const moq_subscribe_updated_event_t *up = &ev->u.subscribe_updated;
    if (ev->detail_size < offsetof(moq_subscribe_updated_event_t, options) +
                              sizeof(up->options) ||
        up->options == NULL ||
        up->options->struct_size <
            offsetof(moq_subscription_update_options_t, location) +
                sizeof(up->options->location)) {
        return NULL;
    }
    return up->options;
}

/* An automatic ACK no longer has an owed query. Its event must retain the
 * exact processing-time replacement, never re-resolve raw relative intent. */
static moqr_result_t
bind_update_resolved_location(const moq_subscription_update_options_t *options,
                              uint64_t id, moq_location_filter_cfg_t *out)
{
    if (options == NULL || options->struct_size <
            offsetof(moq_subscription_update_options_t, resolution) +
                sizeof(options->resolution)) {
        return MOQR_ERR_INVAL;
    }
    const moq_update_resolution_t *r = &options->resolution;
    if (r->struct_size < MOQ_UPDATE_RESOLUTION_V0_SIZE ||
        r->update_request_id != id ||
        !(r->flags & MOQ_UPDATE_RESOLUTION_LOCATION_REPLACEMENT) ||
        r->location.struct_size < MOQ_LOCATION_FILTER_CFG_V0_SIZE) {
        return MOQR_ERR_INVAL;
    }
    *out = r->location;
    bind_location_for_test(out);
    return MOQR_OK;
}

/* The End of Range kind for Locations a fill will not serve and does not
 * wait for. An unmarked gap in a fill fetch stream says the skipped Objects
 * do not exist, so a Location the relay cannot vouch for is always marked:
 * Timed-Out once the fill's wait budget is spent (9.20.5: with a budget of 0
 * "any unavailable Objects" are), Unknown before that, for Objects whose
 * state the relay cannot determine and has no source to wait on (3.2.2). */
static moq_fetch_range_kind_t
bind_fill_unserved_kind(bool budget_spent)
{
    return budget_spent ? MOQ_FETCH_RANGE_TIMED_OUT : MOQ_FETCH_RANGE_UNKNOWN;
}

/* -- Answering downstream subscription updates --------------------------------- *
 * Every attached session leaves the answer to a subscriber's update to the
 * binding (moq_session_set_defer_subscription_updates at attach), so the
 * relay decides before anything is acknowledged: the session sends
 * REQUEST_UPDATE_OK only once the relay has applied what it schedules on,
 * and a refusal is REQUEST_UPDATE_ERROR followed by the subscription's
 * PUBLISH_DONE UPDATE_FAILED, both written by the session. An answer the
 * session cannot take yet is held on the subscription's tracking slot and
 * retried every pump; it never holds up another subscription's work. */

static moq_bytes_t bind_subscribe_error_reason(moq_request_error_t code);

static bool
bind_update_answer_blocked_for_test(void)
{
#ifdef MOQR_BIND_TESTING
    if (atomic_load(&bind_dbg_block_update_answers) > 0) {
        (void)atomic_fetch_sub(&bind_dbg_block_update_answers, 1);
        return true;
    }
#endif
    return false;
}

static void
bind_dsub_release(moqr_bind_t *b, b_conn_t *cn, b_dsub_t *ds)
{
    (void)b;
    (void)cn;
    memset(ds, 0, sizeof(*ds));
}

/* The downstream request is over on the session's side: the session ended
 * it itself (a refusal's UPDATE_FAILED terminal, or a terminal it holds
 * behind owed resets), or the peer cancelled or refused it. Whatever its open
 * data streams are still owed -- their resets, a terminal -- is the session's
 * to send. The binding releases its subgroup slots for exactly this request
 * (its own handle; a later request in the same session slot is another
 * value), each with its write progress and any owed reset cleared and
 * capacity rearmed once, and retires the relay subscription. Nothing is
 * written. */
static void
bind_dsub_retire(moqr_bind_t *b, b_conn_t *cn, b_dsub_t *ds, uint64_t now_us)
{
    for (uint32_t i = 0; i < b->n_sgs; i++) {
        if (cn->sgs[i].used && cn->sgs[i].sub_raw == ds->sub_raw) {
            cn->sgs[i].used = false;
            cn->sgs[i].obj_begun = false;
            cn->sgs[i].obj_id = 0;
            cn->sgs[i].next_chunk = 0;
            cn->sgs[i].reset_owed = false;
            cn->sgs[i].reset_wire = 0;
            bind_dl_rearm_on_slot_release(b, cn);
        }
    }
    (void)moqr_core_unsubscribe(b->core, ds->rsub, now_us);
    bind_dsub_release(b, cn, ds);
}

/* Keep D22 downstream histories current even when Forward or a filter prevents
 * delivery. UPDATE snapshots Largest at message processing, so noting it only
 * when answering is too late. The note is non-advancing and monotonic; later
 * ingest cannot replace a snapshot already held for an owed answer. */
static void
bind_note_relay_largest(moqr_bind_t *b, moqr_track_t track)
{
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        b_conn_t *cn = &b->conns[ci];
        if (!cn->used || cn->closed || cn->session == NULL ||
            cn->wire_draft != B_DRAFT_22) {
            continue;
        }
        for (uint32_t i = 0; i < b->n_dsubs; i++) {
            b_dsub_t *ds = &cn->dsubs[i];
            moqr_track_t sub_track = MOQR_TRACK_INVALID;
            uint64_t track_gen = 0;
            if (!ds->used ||
                moqr_core_sub_track(b->core, ds->rsub, &sub_track, &track_gen) !=
                    MOQR_OK ||
                !moqr_track_eq(track, sub_track)) {
                continue;
            }
            bool has = false;
            uint64_t lg = 0, lo = 0;
            moq_bytes_t parts[32];
            moqr_ns_t ns;
            moq_bytes_t name;
            if (moqr_core_track_largest(b->core, track, track_gen, &has, &lg,
                                        &lo) == MOQR_OK && has &&
                moqr_core_track_key(b->core, track, track_gen, parts, 32, &ns,
                                    &name) == MOQR_OK) {
                const moq_namespace_t sns = { .parts = ns.parts, .count = ns.count };
                (void)moq_session_note_object_published(cn->session, &sns, name,
                                                        lg, lo);
            }
            break;   /* one history per track per session */
        }
    }
}

typedef enum b_upd_give {
    B_UPD_GIVEN = 0,    /* the answer is on its way (or nothing was owed)  */
    B_UPD_HELD,         /* WOULD_BLOCK: still owed, retry later            */
    B_UPD_ENDED,        /* a refusal ended the subscription                */
} b_upd_give_t;

/* Give the held answer on `ds`. An acceptance applies the held Forward
 * change to the relay subscription once the session has taken it; a
 * refusal releases the subscription's tracking. A subscription the session
 * no longer has, or that no longer owes this answer, owes nothing. */
static b_upd_give_t
bind_update_answer_give(moqr_bind_t *b, b_conn_t *cn, b_dsub_t *ds,
                        uint64_t now_us)
{
    if (ds->upd_answer == B_UPD_NONE) {
        return B_UPD_GIVEN;
    }
    moq_subscription_t sub = { ds->sub_raw };
    moq_result_t rc;
    bool accept = ds->upd_answer == B_UPD_ACCEPT;
    if (bind_update_answer_blocked_for_test()) {
        rc = MOQ_ERR_WOULD_BLOCK;
    } else if (accept) {
        moq_accept_subscription_update_cfg_t ac;
        moq_accept_subscription_update_cfg_init(&ac);
        ac.update_request_id = ds->upd_id;
        rc = ds->publication
                 ? moq_session_accept_publication_update(
                       cn->session,
                       (moq_publication_t){ ._opaque = ds->sub_raw }, &ac,
                       now_us)
                 : moq_session_accept_subscription_update(cn->session, sub,
                                                          &ac, now_us);
    } else {
        bool filter = ds->upd_answer == B_UPD_REJECT_FILTER;
        moq_request_error_t code = filter ? BIND_FILTER_REFUSAL_CODE
                                          : MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        moq_reject_subscription_update_cfg_t rj;
        moq_reject_subscription_update_cfg_init(&rj);
        rj.update_request_id = ds->upd_id;
        rj.error_code = code;
        rj.reason = filter ? MOQ_BYTES_LITERAL(BIND_FILTER_REFUSAL_REASON)
                           : bind_subscribe_error_reason(code);
        rc = ds->publication
                 ? moq_session_reject_publication_update(
                       cn->session,
                       (moq_publication_t){ ._opaque = ds->sub_raw }, &rj,
                       now_us)
                 : moq_session_reject_subscription_update(cn->session, sub,
                                                          &rj, now_us);
    }
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        return B_UPD_HELD;
    }
    bool has_forward = ds->upd_has_forward, forward = ds->upd_forward;
    ds->upd_answer = B_UPD_NONE;
    ds->upd_has_forward = false;
    ds->upd_forward = false;
    ds->upd_id = 0;
    if (rc != MOQ_OK) {
        /* STALE / WRONG_STATE / CLOSED: the subscription ended, or the
         * answer was already given, meanwhile. Anything else is a broken
         * invariant: count it. */
        if (!fwd_result_is_gone(rc)) {
            b->session_errors++;
        }
        return B_UPD_GIVEN;
    }
    if (!accept) {
        bind_dsub_retire(b, cn, ds, now_us);
        return B_UPD_ENDED;
    }
    if (has_forward) {
        moqr_result_t frc =
            moqr_core_sub_set_forward(b->core, ds->rsub, forward, now_us);
        if (frc != MOQR_OK && frc != MOQR_ERR_STALE_HANDLE) {
            b->session_errors++;
        }
    }
    return B_UPD_GIVEN;
}

/* Decide the answer to a downstream update the session left to the relay,
 * then give it (or hold it). The event carries the cumulative values of
 * every update the answer covers, so a later event for the same
 * subscription replaces a held decision.
 *
 * A selected semantic Location Filter is applied once to the relay
 * subscription before the acceptance is given, so the
 * acknowledgement follows the change. A Forward change is applied once the
 * acceptance is given -- the session's own forwarding state changes then.
 * Priority / delivery-timeout / new-group changes are not applied here. */
static void
bind_update_answer_for(moqr_bind_t *b, b_conn_t *cn, uint64_t raw, uint64_t id,
                       bool has_forward, bool forward, bool resolve_filter,
                       uint64_t now_us)
{
    /* A subscription the relay began with PUBLISH (track discovery) is
     * updated through the same answer as one a SUBSCRIBE began. */
    const bool pub = bind_cookie_is_publication(raw);
    b_dsub_t *ds = NULL;
    for (uint32_t i = 0; i < b->n_dsubs; i++) {
        if (cn->dsubs[i].used && cn->dsubs[i].sub_raw == raw) {
            ds = &cn->dsubs[i];
            break;
        }
    }
    if (ds == NULL) {
        /* No tracked subscription: the relay already released it (its
         * terminal crossed this update) or never tracked it. The session
         * may still owe the answer; refuse so the subscription ends rather
         * than wait forever. Gone means nothing is owed; a refusal that
         * lands is a broken invariant and is counted. */
        moq_reject_subscription_update_cfg_t rj;
        moq_reject_subscription_update_cfg_init(&rj);
        rj.update_request_id = id;
        rj.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        rj.reason = bind_subscribe_error_reason(rj.error_code);
        moq_result_t rc = pub
            ? moq_session_reject_publication_update(
                  cn->session, (moq_publication_t){ ._opaque = raw }, &rj, now_us)
            : moq_session_reject_subscription_update(
                  cn->session, (moq_subscription_t){ ._opaque = raw }, &rj, now_us);
        if (!fwd_result_is_gone(rc)) {
            b->session_errors++;
        }
        return;
    }
    bool was_held = ds->upd_answer != B_UPD_NONE;
    ds->upd_id = id;
    ds->upd_has_forward = has_forward;
    ds->upd_forward = forward;
    ds->upd_answer = B_UPD_ACCEPT;
    if (resolve_filter) {
        moq_update_resolution_t resolution;
        moqr_result_t mrc = MOQR_ERR_INVAL;
        moq_result_t qrc = pub
            ? moq_session_get_publication_update_resolution(cn->session,
                  (moq_publication_t){ ._opaque = raw }, id, &resolution,
                  sizeof(resolution))
            : moq_session_get_subscription_update_resolution(cn->session,
                  (moq_subscription_t){ ._opaque = raw }, id, &resolution,
                  sizeof(resolution));
        if (qrc == MOQ_OK) {
            mrc = MOQR_OK;
            if (resolution.flags & MOQ_UPDATE_RESOLUTION_LOCATION_REPLACEMENT) {
                bind_location_for_test(&resolution.location);
                mrc = moqr_core_sub_set_location_filter(b->core, ds->rsub,
                                                       &resolution.location, now_us);
            }
        }
        if (mrc != MOQR_OK && mrc != MOQR_ERR_STALE_HANDLE) {
            ds->upd_answer = mrc == MOQR_ERR_UNSUPPORTED || mrc == MOQR_ERR_INVAL
                                 ? B_UPD_REJECT_FILTER
                                 : B_UPD_REJECT_INTERNAL;
            if (mrc != MOQR_ERR_INVAL && mrc != MOQR_ERR_UNSUPPORTED &&
                mrc != MOQR_ERR_NOMEM) {
                b->session_errors++;
            }
        }
    }
    if (bind_update_answer_give(b, cn, ds, now_us) == B_UPD_HELD) {
        if (!was_held) {
            b->upd_answers_held++;
        }
    }
}

static void
bind_update_answer(moqr_bind_t *b, b_conn_t *cn,
                   const moq_subscribe_updated_event_t *up, bool resolve_filter,
                   uint64_t now_us)
{
    bind_update_answer_for(b, cn, up->sub._opaque, up->update_request_id,
                           up->has_forward, up->forward, resolve_filter,
                           now_us);
}

/* Retry every held update answer, once per pump. Recounts the held set, so
 * answers left on a connection that has since gone stop being counted. */
static void
bind_update_answer_pump(moqr_bind_t *b, uint64_t now_us)
{
    if (b->upd_answers_held == 0) {
        return;
    }
    uint32_t held = 0;
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        b_conn_t *cn = &b->conns[ci];
        if (!cn->used || cn->closed || cn->session == NULL) {
            continue;
        }
        for (uint32_t i = 0; i < b->n_dsubs; i++) {
            b_dsub_t *ds = &cn->dsubs[i];
            if (!ds->used || ds->upd_answer == B_UPD_NONE) {
                continue;
            }
            if (bind_update_answer_give(b, cn, ds, now_us) == B_UPD_HELD) {
                held++;
            }
        }
    }
    b->upd_answers_held = held;
}

/* Before the relay ends a downstream subscription itself: the session
 * refuses a terminal while an update answer is owed (§9.5 -- exactly one
 * answer per update), so that answer goes first. A held decision is given;
 * an owed answer whose event the binding has not polled yet is accepted, as
 * nothing it asks for will be served once the subscription ends. */
static b_upd_give_t
bind_update_answer_settle(moqr_bind_t *b, b_conn_t *cn, uint64_t sub_raw,
                          uint64_t now_us)
{
    for (uint32_t i = 0; i < b->n_dsubs; i++) {
        b_dsub_t *ds = &cn->dsubs[i];
        if (!ds->used || ds->sub_raw != sub_raw) {
            continue;
        }
        if (ds->upd_answer == B_UPD_NONE) {
            uint64_t id = 0;
            moq_result_t orc = ds->publication
                ? moq_session_publication_owed_update(
                      cn->session, (moq_publication_t){ ._opaque = sub_raw },
                      &id, NULL, NULL)
                : moq_session_subscription_owed_update(
                      cn->session, (moq_subscription_t){ sub_raw }, &id, NULL,
                      NULL);
            if (orc != MOQ_OK) {
                return B_UPD_GIVEN;
            }
            ds->upd_answer = B_UPD_ACCEPT;
            ds->upd_id = id;
            ds->upd_has_forward = false;
            b->upd_answers_held++;
        }
        return bind_update_answer_give(b, cn, ds, now_us);
    }
    return B_UPD_GIVEN;
}

static void
bind_park_init(moqr_park_req_t *pr, const b_conn_t *cn, const moqr_bind_t *b,
               moqr_auth_action_t action, moqr_ns_t ns, moq_bytes_t name,
               uint64_t session_cookie)
{
    memset(pr, 0, sizeof(*pr));
    pr->action = action;
    pr->binding_cookie = (uint64_t)(cn - b->conns) + 1u;
    pr->session_cookie = session_cookie;
    pr->ns = ns;
    pr->name = name;
}

/* Mirror the event's borrowed tokens (fail closed if the set can't be
 * faithfully mirrored) and hand the request to core parked storage, which
 * deep-copies it. */
static moqr_result_t
bind_park(moqr_bind_t *b, moqr_park_req_t *pr,
          const moq_resolved_token_t *tokens, size_t token_count,
          uint64_t ticket, uint64_t now_us)
{
    if (token_count > MOQR_BIND_AUTH_MAX_TOKENS ||
        (token_count > 0 && tokens == NULL)) {
        return MOQR_ERR_INVAL; /* fail closed, no truncation */
    }
    moqr_auth_token_t mt[MOQR_BIND_AUTH_MAX_TOKENS];
    for (size_t i = 0; i < token_count; i++) {
        mt[i].token_type = tokens[i].token_type;
        mt[i].token_value = tokens[i].token_value;
    }
    pr->tokens = token_count > 0 ? mt : NULL;
    pr->token_count = token_count;
    return moqr_core_park_sized(b->core, pr, sizeof(*pr), ticket, now_us);
}

/* Reserve a CAT revalidation grant for an ALLOWed SUBSCRIBE / PUBLISH_NAMESPACE
 * that carries a lease, BEFORE the core op mutates state. `pr` is already
 * populated by bind_park_init (action / cookies / ns / name). A lease of 0 means
 * no grant is wanted: *res is zeroed and MOQR_OK returned. Otherwise the event's
 * borrowed tokens are mirrored (fail closed if the set can't be represented) and
 * the reservation is taken; the core deep-copies the material. Any non-OK return
 * means the caller MUST fail the op closed with no wire accept. On success *res
 * identifies the reservation for a later commit (op OK) or abort (op failed). */
static moqr_result_t
bind_grant_reserve(moqr_bind_t *b, moqr_park_req_t *pr,
                   const moq_resolved_token_t *tokens, size_t token_count,
                   uint64_t lease_us, uint64_t now_us, moqr_grant_res_t *res)
{
    memset(res, 0, sizeof(*res));
    if (lease_us == 0) {
        return MOQR_OK; /* no revalidation grant for this ALLOW */
    }
    if (token_count > MOQR_BIND_AUTH_MAX_TOKENS ||
        (token_count > 0 && tokens == NULL)) {
        return MOQR_ERR_INVAL; /* fail closed, no truncation */
    }
    moqr_auth_token_t mt[MOQR_BIND_AUTH_MAX_TOKENS];
    for (size_t i = 0; i < token_count; i++) {
        mt[i].token_type = tokens[i].token_type;
        mt[i].token_value = tokens[i].token_value;
    }
    pr->tokens = token_count > 0 ? mt : NULL;
    pr->token_count = token_count;
    return moqr_core_grant_reserve(b->core, pr, lease_us, now_us, res);
}

/* Fixed diagnostic text only: never expose peer tokens or policy inputs. */
static moq_bytes_t
bind_subscribe_error_reason(moq_request_error_t code)
{
    switch (code) {
    case MOQ_REQUEST_ERROR_DOES_NOT_EXIST:
        return MOQ_BYTES_LITERAL("track does not exist");
    case MOQ_REQUEST_ERROR_UNAUTHORIZED:
        return MOQ_BYTES_LITERAL("unauthorized");
    case MOQ_REQUEST_ERROR_TIMEOUT:
        return MOQ_BYTES_LITERAL("timeout");
    case MOQ_REQUEST_ERROR_NOT_SUPPORTED:
        return MOQ_BYTES_LITERAL("not supported");
    case MOQ_REQUEST_ERROR_INVALID_RANGE:
        return MOQ_BYTES_LITERAL("invalid range");
    case MOQ_REQUEST_ERROR_MALFORMED_TRACK:
        return MOQ_BYTES_LITERAL("malformed track");
    case MOQ_REQUEST_ERROR_DUPLICATE_SUBSCRIPTION:
        return MOQ_BYTES_LITERAL("duplicate subscription");
    case MOQ_REQUEST_ERROR_UNINTERESTED:
        return MOQ_BYTES_LITERAL("uninterested");
    case MOQ_REQUEST_ERROR_EXCESSIVE_LOAD:
        return MOQ_BYTES_LITERAL("excessive load");
    case MOQ_REQUEST_ERROR_INTERNAL_ERROR:
        return MOQ_BYTES_LITERAL("internal error");
    default:
        return (moq_bytes_t){ NULL, 0 };
    }
}

static void
bind_reject_subscribe_cfg_init(moq_reject_subscribe_cfg_t *cfg,
                               moq_request_error_t code)
{
    moq_reject_subscribe_cfg_init(cfg);
    cfg->error_code = code;
    cfg->reason = bind_subscribe_error_reason(code);
}

/* Retry Interval offered with a capacity refusal: the condition is
 * transient (a live track, subscription or fetch ends and frees its slot).
 * Every client refused by the same full pool would otherwise retry in
 * lockstep, so each request gets its own interval in [MIN, MIN + SPREAD]:
 * "the sender can apply randomization to each retry interval so that retries
 * are spread out over time" (d16 Section 9.8, d18 Section 10.6). Derived
 * from the request handle, so it is deterministic per request. */
#define B_CAPACITY_RETRY_MIN_MS    750u
#define B_CAPACITY_RETRY_SPREAD_MS 500u

static uint64_t
bind_capacity_retry_ms(uint64_t request_cookie)
{
    uint64_t z = request_cookie + UINT64_C(0x9E3779B97F4A7C15);   /* SplitMix64 */
    z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
    z ^= z >> 31;
    return B_CAPACITY_RETRY_MIN_MS + z % (B_CAPACITY_RETRY_SPREAD_MS + 1u);
}

/* The REQUEST_ERROR code for a request this relay cannot admit because a
 * bounded pool holds only live entries (MOQR_ERR_CAPACITY). draft-18 has a
 * code for exactly this, EXCESSIVE_LOAD (0x9, Section 10.6: "The responder is
 * overloaded and cannot process the request at this time. The sender SHOULD
 * use the Retry Interval to indicate when the request can be retried"), and
 * draft-22 keeps it (0x9, Section 12.3). draft-16's registry (Section 13.4.2) assigns no load code, and 0x9 is
 * unassigned there, so INTERNAL_ERROR is its truthful answer. Either way the
 * refusal carries a nonzero Retry Interval. */
static moq_request_error_t
bind_capacity_error(const b_conn_t *cn)
{
    return cn->wire_draft == B_DRAFT_16 ? MOQ_REQUEST_ERROR_INTERNAL_ERROR
                                        : MOQ_REQUEST_ERROR_EXCESSIVE_LOAD;
}

static void
bind_reject_ex(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t action,
               uint64_t session_cookie, uint64_t err, bool can_retry,
               uint64_t retry_ms, uint64_t now_us);

/* Refuse a SUBSCRIBE waiting for a publisher that the relay has no room
 * to hold (EXCESSIVE_LOAD: "overloaded and cannot process the request at
 * this time"). Room frees as other subscriptions end, so the request may be
 * retried at once (a Retry Interval of 1). Held like every refusal. */
static void
bind_reject_load(moqr_bind_t *b, b_conn_t *cn, moq_subscription_t sub,
                 uint64_t now_us)
{
    bind_reject_ex(b, cn, MOQR_AUTH_SUBSCRIBE, sub._opaque,
                   bind_capacity_error(cn), true, 0, now_us);
}

/* Refuse an authorized SUBSCRIBE whose Location Filter the relay does not
 * serve. A refusal the session cannot take yet is held (without its reason
 * phrase) and retried like every other. */
static void
bind_reject_filter(moqr_bind_t *b, b_conn_t *cn, moq_subscription_t sub,
                   uint64_t now_us)
{
    moq_reject_subscribe_cfg_t rj;
    bind_reject_subscribe_cfg_init(&rj, BIND_FILTER_REFUSAL_CODE);
    rj.reason = MOQ_BYTES_LITERAL(BIND_FILTER_REFUSAL_REASON);
    moq_result_t rc = moq_session_reject_subscribe(cn->session, sub, &rj, now_us);
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        b_refusal_t want = {
            .used = true,
            .kind = MOQR_AUTH_SUBSCRIBE,
            .handle = sub._opaque,
            .error_code = BIND_FILTER_REFUSAL_CODE,
        };
        bind_terminal_hold(b, cn, &want);
    } else if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
        b->session_errors++;
    }
}

/* Reject the pending session request identified by (action, session_cookie)
 * with the wire error code. Shared by the inline refuse path and deferred
 * resolution, reconstructing the handle from the stored cookie. */
static void
bind_reject(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t action,
            uint64_t session_cookie, uint64_t err, uint64_t now_us)
{
    bind_reject_ex(b, cn, action, session_cookie, err, false, 0, now_us);
}

/* bind_reject for a core admission result: MOQR_ERR_CAPACITY is a capacity
 * refusal (bind_capacity_error, retryable); anything else INTERNAL_ERROR. */
static void
bind_reject_admission(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t action,
                      uint64_t session_cookie, moqr_result_t rc,
                      uint64_t now_us)
{
    if (rc == MOQR_ERR_CAPACITY) {
        bind_reject_ex(b, cn, action, session_cookie, bind_capacity_error(cn),
                       true, bind_capacity_retry_ms(session_cookie), now_us);
    } else {
        bind_reject(b, cn, action, session_cookie,
                    MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
    }
}

/* Every request family the auth hook gates is refused through bind_refuse,
 * so a refusal the session cannot take yet is held, never lost. */
static void
bind_reject_ex(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t action,
               uint64_t session_cookie, uint64_t err, bool can_retry,
               uint64_t retry_ms, uint64_t now_us)
{
    /* A draft-22 track subscription is refused as one, with its own held
     * answer; a PUBLISH with a metadata stage is refused through it. */
    if (action == MOQR_AUTH_SUBSCRIBE_NAMESPACE &&
        bind_cookie_is_track_sub(session_cookie)) {
        bind_tracks_reject(b, cn, session_cookie, err, now_us);
        return;
    }
    if (action == MOQR_AUTH_PUBLISH) {
        b_meta_publish_t *p = bind_meta_pub_find(b, cn, session_cookie);
        if (p != NULL) {
            bind_meta_pub_refuse_ex(b, cn, p, err, can_retry, retry_ms, now_us);
            return;
        }
    }
    switch (action) {
    case MOQR_AUTH_PUBLISH_NAMESPACE:
    case MOQR_AUTH_SUBSCRIBE:
    case MOQR_AUTH_SUBSCRIBE_NAMESPACE:
    case MOQR_AUTH_TRACK_STATUS:
    case MOQR_AUTH_PUBLISH:
    case MOQR_AUTH_FETCH:
        bind_refuse(b, cn, action, session_cookie, err, can_retry, retry_ms,
                    now_us);
        break;
    default:
        break;
    }
}

/* Commit a reserved subscribe grant after moqr_core_subscribe succeeded (no-op
 * when there was no grant). A commit failure is an invariant violation — the
 * reservation was just taken — so revoke the just-created sub: no untracked,
 * unrevalidatable subscription may stay live on the wire. */
static void
bind_grant_commit_sub(moqr_bind_t *b, moqr_grant_res_t gres, moqr_sub_t rsub,
                      uint64_t now_us)
{
    if (gres.gen == 0) {
        return; /* no grant (lease 0) */
    }
    if (moqr_core_grant_commit(b->core, gres, rsub._opaque) != MOQR_OK) {
        moqr_result_t vrc;
        /* The grant could not be committed: the truthful terminal is the
         * PUBLISH_DONE registry's INTERNAL_ERROR, not the same-numbered
         * REQUEST_ERROR constant this once borrowed. */
        BIND_CALL_RETRY(vrc, moqr_core_revoke_sub(
                                 b->core, rsub,
                                 bind_local_pd(MOQR_PD_INTERNAL_ERROR),
                                 now_us));
        (void)vrc;
        b->session_errors++;
    }
}

/* Commit a reserved announce grant after moqr_core_announce succeeded. Returns
 * true when the caller should send the wire accept. On the invariant commit
 * failure it undoes the announce and rejects (no untracked namespace stays
 * live) and returns false. A no-grant reservation (lease 0) returns true. */
static bool
bind_grant_commit_ann(moqr_bind_t *b, b_conn_t *cn, moqr_grant_res_t gres,
                      moqr_ns_t ns, uint64_t session_cookie, uint64_t now_us)
{
    if (gres.gen == 0) {
        return true; /* no grant (lease 0): proceed to accept */
    }
    if (moqr_core_grant_commit(b->core, gres, 0) != MOQR_OK) {
        moqr_result_t urc;
        BIND_CALL_RETRY(urc, moqr_core_unannounce(b->core, cn->binding, ns));
        (void)urc;
        bind_reject(b, cn, MOQR_AUTH_PUBLISH_NAMESPACE, session_cookie,
                    MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        b->session_errors++;
        return false;
    }
    return true;
}

/* Resume the original ALLOWed op from a parked view: re-run the same core call
 * + session accept the inline path would have. An ALLOW that carries a CAT
 * lease (`lease` > 0) reserves + commits a revalidation grant around the op,
 * exactly like the synchronous request path — an async ALLOW must be revalidat-
 * able too. On core failure (WOULD_BLOCK exhausted or error), fail closed by
 * rejecting the pending request. */
static void
bind_resume(moqr_bind_t *b, b_conn_t *cn, const moqr_park_req_t *v,
            uint64_t lease, uint64_t now_us)
{
    moqr_result_t rc = MOQR_OK;
    switch (v->action) {
    case MOQR_AUTH_PUBLISH_NAMESPACE: {
        /* The pinned view already holds deep-copied moqr_auth_token_t bytes, so
         * reserve straight from it (no session-token mirror). Reserve fails the
         * request closed with no announce; op failure aborts the reservation. */
        moqr_grant_res_t gres;
        memset(&gres, 0, sizeof(gres));
        if (lease > 0 &&
            moqr_core_grant_reserve(b->core, v, lease, now_us, &gres) !=
                MOQR_OK) {
            bind_reject(b, cn, v->action, v->session_cookie,
                        MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
            return;
        }
        int ann_slot = ann_store(b, cn, v->session_cookie, v->ns);
        if (ann_slot < 0) {
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            bind_reject_admission(b, cn, v->action, v->session_cookie,
                                  MOQR_ERR_CAPACITY, now_us);
            return;
        }
        /* The stored copy, not the parked material: retries drain intents. */
        moqr_ns_t ans = cn->anns[ann_slot].ns;
        /* Retained before the announce: its intent drain may end the
         * parked material this view borrows. */
        bind_retain_request_tokens(b, cn, v->session_cookie, v->tokens,
                                   v->token_count);
        rc = bind_announce(b, cn, ans, v->session_cookie, now_us);
        if (rc != MOQR_OK) {
            moqr_core_request_tokens_clear(b->core, cn->binding,
                                           v->session_cookie);
            ann_clear_slot(b, cn, (uint32_t)ann_slot);
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            break;
        }
        if (bind_grant_commit_ann(b, cn, gres, ans, v->session_cookie,
                                  now_us)) {
            bind_accept(b, cn, B_ACCEPT_NAMESPACE, v->session_cookie, now_us);
        } else {
            moqr_core_request_tokens_clear(b->core, cn->binding,
                                           v->session_cookie);
            ann_clear_slot(b, cn, (uint32_t)ann_slot);
        }
        return;
    }
    case MOQR_AUTH_SUBSCRIBE: {
        if (v->sub_filter_type == BIND_PARKED_FILTER_REFUSED) {
            /* Authorized, but its Location Filter is one the relay does
             * not serve: the refusal it was parked with. */
            bind_reject_filter(b, cn,
                               (moq_subscription_t){ ._opaque =
                                                         v->session_cookie },
                               now_us);
            return;
        }
        moqr_grant_res_t gres;
        memset(&gres, 0, sizeof(gres));
        if (lease > 0 &&
            moqr_core_grant_reserve(b->core, v, lease, now_us, &gres) !=
                MOQR_OK) {
            bind_reject(b, cn, v->action, v->session_cookie,
                        MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
            return;
        }
        moqr_subscribe_req_t rq;
        moqr_subscribe_req_init(&rq);
        rq.ns = v->ns;
        rq.name = v->name;
        rq.filter.type = (moqr_filter_type_t)v->sub_filter_type;
        rq.filter.start_group = v->sub_start_group;
        rq.filter.start_object = v->sub_start_object;
        rq.filter.end_group_delta = v->sub_end_group_delta;
        rq.subscriber_priority = v->sub_priority;
        rq.group_order = (moqr_group_order_t)v->sub_group_order;
        rq.cookie = v->session_cookie;
        /* The Forward State the SUBSCRIBE asked for: a paused one is not
         * scheduled until an update turns it on. */
        rq.forward = !v->sub_forward_off;
        rq.range_end_holds = bind_range_end_holds(cn);
        /* The deadline taken at first receipt: a deferred authorization
         * never renews it. */
        rq.rendezvous = v->sub_rendezvous;
        rq.rendezvous_deadline_us = v->sub_rendezvous_deadline_us;
        moq_location_filter_cfg_t location;
        if (cn->wire_draft == B_DRAFT_22) {
            if (bind_read_sub_location(cn,
                    (moq_subscription_t){ ._opaque = v->session_cookie },
                    &location) != MOQR_OK) {
                if (gres.gen != 0) moqr_core_grant_abort(b->core, gres);
                bind_reject_filter(b, cn,
                    (moq_subscription_t){ ._opaque = v->session_cookie }, now_us);
                return;
            }
            rq.location = &location;
        }
        moqr_sub_t rs;
        BIND_CALL_RETRY(rc, moqr_core_subscribe(b->core, cn->binding, &rq, &rs));
        if (rc != MOQR_OK) {
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            if (rc == MOQR_ERR_CAPACITY && v->sub_rendezvous) {
                bind_reject_load(b, cn,
                    (moq_subscription_t){ ._opaque = v->session_cookie }, now_us);
                return;
            }
            break;
        }
        bind_grant_commit_sub(b, gres, rs, now_us);
        return; /* ACCEPT_SUB / REJECT_SUB flows through intents */
    }
    case MOQR_AUTH_SUBSCRIBE_NAMESPACE:
        if (bind_cookie_is_track_sub(v->session_cookie)) {
            bind_tracks_admit(b, cn, v->session_cookie, v->ns,
                              !v->sub_forward_off, v->sub_start_object != 0,
                              v->sub_group_order, v->tokens, v->token_count,
                              now_us);
            return;
        }
        BIND_CALL_RETRY(rc, moqr_core_ns_subscribe(b->core, cn->binding, v->ns,
                                                   v->session_cookie));
        if (rc == MOQR_OK) {
            bind_retain_request_tokens(b, cn, v->session_cookie, v->tokens,
                                       v->token_count);
            bind_accept(b, cn, B_ACCEPT_NS_SUB, v->session_cookie, now_us);
            return;
        }
        break;
    case MOQR_AUTH_TRACK_STATUS:
        BIND_CALL_RETRY(rc, moqr_core_track_status(b->core, cn->binding, v->ns,
                                                   v->name, v->session_cookie));
        if (rc == MOQR_OK) {
            return;
        }
        break;
    case MOQR_AUTH_PUBLISH: {
        b_meta_publish_t *p = bind_meta_pub_find(b, cn, v->session_cookie);
        if (p == NULL || p->state != B_META_PUB_AUTH) {
            return;   /* cancelled/retired while authorization was outstanding */
        }
        p->state = B_META_PUB_CORE;
        moqr_track_t tr;
        BIND_CALL_RETRY(rc, moqr_core_publish_open(b->core, cn->binding, v->ns,
                                                   v->name, v->session_cookie,
                                                   &tr));
        if (rc != MOQR_OK) {
            p = bind_meta_pub_find(b, cn, v->session_cookie);
            if (p != NULL) bind_meta_pub_refuse(b, cn, p, MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        }
        return;
    }
    default:
        return;
    }
    /* Resume failed: reject the pending request rather than leave it hanging. */
    bind_reject_admission(b, cn, v->action, v->session_cookie, rc, now_us);
}

/* Handle a non-ALLOW result at a request site: DEFER parks (fail closed if the
 * park itself fails), and DENY/FAIL_CLOSED/park-failure reject. */
static void
bind_refuse_or_park(moqr_bind_t *b, b_conn_t *cn, bind_auth_result_t ar,
                    moqr_park_req_t *pr, const moq_resolved_token_t *tokens,
                    size_t token_count, uint64_t ticket, uint64_t err,
                    uint64_t now_us)
{
    if (ar == BIND_AUTH_DEFER &&
        bind_park(b, pr, tokens, token_count, ticket, now_us) == MOQR_OK) {
        /* Parked: leave the session request pending, and reserve the refusal
         * entry its resolution may need. The poll that delivered this request
         * guaranteed an unreserved free entry, and the core's max_parked
         * bounds the reservations the table is sized for. */
        cn->refusals_reserved++;
        return;
    }
    /* DENY / FAIL_CLOSED / park failure -> reject fail-closed. A valid DEFER
     * leaves err == 0 (bind_authorize only fills it for DENY/FAIL_CLOSED), so a
     * park failure (duplicate ticket / capacity / byte budget) must still carry
     * a definite wire code, never 0. */
    if (err == 0) {
        err = MOQ_REQUEST_ERROR_UNAUTHORIZED;
    }
    bind_reject(b, cn, pr->action, pr->session_cookie, err, now_us);
}

static b_dsub_t *dsub_find(const moqr_bind_t *b, b_conn_t *cn, uint64_t sub_raw);
static int dsub_free_slot(const moqr_bind_t *b, const b_conn_t *cn);

/* -- Draft-22 namespace updates and track discovery ------------------------ *
 *
 * The session leaves the answer to every update of a namespace announcement,
 * namespace subscription or track subscription to the relay
 * (moq_session_set_defer_namespace_updates). An update is authorized as the
 * request it changes would be -- REQUEST_UPDATE, then the original namespace
 * action (PUBLISH_NAMESPACE, or SUBSCRIBE_NAMESPACE for both kinds of
 * subscription) -- over the namespace or prefix it leaves in effect and the
 * tokens it carries, or else the tokens in effect (§9.5: an omitted parameter
 * keeps its value). Only then is it accepted: the relay state it changes is
 * checked first, the acceptance is given, and the change is applied right
 * after it, so nothing the change produces reaches the wire before the
 * acceptance. An acceptance the session cannot take yet is held and retried
 * every pump (bind_nsu_pump); a namespace subscription's acceptance also
 * waits for every notification already held for it, which names a suffix of
 * the old prefix. A refusal ends the request, and the relay retires what the
 * request held at once. */

/* A parked namespace update carries this in sub_filter_type (no SUBSCRIBE
 * filter type has it) and the update's Request ID in sub_start_group; the
 * session cookie is the updated request's handle. */
#define BIND_PARK_NS_UPDATE 0x4e5355u

/* A discovered track whose SUBSCRIBE authorization was deferred is parked
 * with this in sub_filter_type, the track subscription's handle as the
 * session cookie, and the track and source generation in sub_start_group /
 * sub_start_object. The offer is held (MOQR_DISCOVERY_PENDING) until the
 * verdict: ALLOW offers it, anything else declines it. */
#define BIND_PARK_DISCOVERY 0x445343u

/* A parked SUBSCRIBE_TRACKS admission carries its options in the SUBSCRIBE
 * fields of the parked request: sub_forward_off, sub_group_order, and
 * include_properties in sub_start_object. */

#define B_NSU_ANN    1u
#define B_NSU_NS     2u
#define B_NSU_TRACKS 3u

static uint8_t
bind_nsu_family(uint64_t handle)
{
    switch (moq_handle_pool_tag(handle)) {
    case MOQ_HANDLE_POOL_ANNOUNCEMENT:     return B_NSU_ANN;
    case MOQ_HANDLE_POOL_NAMESPACE_SUB:    return B_NSU_NS;
    case MOQ_HANDLE_POOL_SUBSCRIBE_TRACKS: return B_NSU_TRACKS;
    default:                               return 0;
    }
}

static bool
bind_cookie_is_publication(uint64_t cookie)
{
    return moq_handle_pool_tag(cookie) == MOQ_HANDLE_POOL_PUBLICATION;
}

static bool
bind_cookie_is_track_sub(uint64_t cookie)
{
    return moq_handle_pool_tag(cookie) == MOQ_HANDLE_POOL_SUBSCRIBE_TRACKS;
}

static moq_result_t
bind_nsu_owed(b_conn_t *cn, uint8_t family, uint64_t handle,
              moq_namespace_update_record_t *rec)
{
    memset(rec, 0, sizeof(*rec));
    switch (family) {
    case B_NSU_ANN:
        return moq_session_namespace_owed_update(
            cn->session, (moq_announcement_t){ ._opaque = handle }, rec,
            sizeof(*rec));
    case B_NSU_NS:
        return moq_session_ns_sub_owed_update(
            cn->session, (moq_ns_sub_handle_t){ ._opaque = handle }, rec,
            sizeof(*rec));
    case B_NSU_TRACKS:
        return moq_session_subscribe_tracks_owed_update(
            cn->session, (moq_track_sub_handle_t){ ._opaque = handle }, rec,
            sizeof(*rec));
    default:
        return MOQ_ERR_INVAL;
    }
}

static moq_result_t
bind_nsu_answer(b_conn_t *cn, uint8_t family, uint64_t handle, uint64_t id,
                bool reject, uint64_t error_code, uint64_t now_us)
{
    if (!reject) {
        moq_accept_subscription_update_cfg_t a;
        moq_accept_subscription_update_cfg_init(&a);
        a.update_request_id = id;
        switch (family) {
        case B_NSU_ANN:
            return moq_session_accept_namespace_update(
                cn->session, (moq_announcement_t){ ._opaque = handle }, &a,
                now_us);
        case B_NSU_NS:
            return moq_session_accept_ns_sub_update(
                cn->session, (moq_ns_sub_handle_t){ ._opaque = handle }, &a,
                now_us);
        case B_NSU_TRACKS:
            return moq_session_accept_subscribe_tracks_update(
                cn->session, (moq_track_sub_handle_t){ ._opaque = handle }, &a,
                now_us);
        default:
            return MOQ_ERR_INVAL;
        }
    }
    moq_reject_subscription_update_cfg_t r;
    moq_reject_subscription_update_cfg_init(&r);
    r.update_request_id = id;
    r.error_code = (moq_request_error_t)error_code;
    r.reason = bind_subscribe_error_reason((moq_request_error_t)error_code);
    switch (family) {
    case B_NSU_ANN:
        return moq_session_reject_namespace_update(
            cn->session, (moq_announcement_t){ ._opaque = handle }, &r, now_us);
    case B_NSU_NS:
        return moq_session_reject_ns_sub_update(
            cn->session, (moq_ns_sub_handle_t){ ._opaque = handle }, &r, now_us);
    case B_NSU_TRACKS:
        return moq_session_reject_subscribe_tracks_update(
            cn->session, (moq_track_sub_handle_t){ ._opaque = handle }, &r,
            now_us);
    default:
        return MOQ_ERR_INVAL;
    }
}

static void
bind_tokens_to_relay(const moq_resolved_token_t *in, size_t n,
                     moqr_auth_token_t *out)
{
    for (size_t i = 0; i < n; i++) {
        out[i].token_type = in[i].token_type;
        out[i].token_value = in[i].token_value;
    }
}

static void
bind_tokens_to_session(const moqr_auth_token_t *in, size_t n,
                       moq_resolved_token_t *out)
{
    for (size_t i = 0; i < n; i++) {
        out[i].token_type = in[i].token_type;
        out[i].token_value = in[i].token_value;
    }
}

/* What one update leaves in effect. The prefix and token views borrow the
 * session record (until the next advancing session call), the binding's
 * announcement copy, or core storage (until the next mutating core call). */
typedef struct b_nsu_view {
    moqr_ns_t            ns;
    moq_bytes_t          parts[32];
    moq_resolved_token_t tokens[MOQR_AUTH_MAX_TOKENS];
    size_t               token_count;
    bool                 prefix;     /* the update selects a new prefix   */
    bool                 forward_selected;
    bool                 forward;
    bool                 auth;       /* the update carries tokens         */
} b_nsu_view_t;

static moqr_result_t
bind_nsu_view(moqr_bind_t *b, b_conn_t *cn, uint8_t family, uint64_t handle,
              const moq_namespace_update_record_t *rec, b_nsu_view_t *v)
{
    memset(v, 0, sizeof(*v));
    v->prefix = (rec->flags & MOQ_NAMESPACE_UPDATE_PREFIX) != 0;
    v->forward_selected = (rec->flags & MOQ_NAMESPACE_UPDATE_FORWARD) != 0;
    v->forward = rec->forward;
    v->auth = (rec->flags & MOQ_NAMESPACE_UPDATE_AUTHORIZATION) != 0;
    if (v->prefix) {
        if (rec->track_namespace_prefix.count > 32) {
            return MOQR_ERR_INVAL;
        }
        for (size_t i = 0; i < rec->track_namespace_prefix.count; i++) {
            v->parts[i] = rec->track_namespace_prefix.parts[i];
        }
        v->ns = (moqr_ns_t){ .parts = v->parts,
                             .count = rec->track_namespace_prefix.count };
    } else if (family == B_NSU_ANN) {
        int slot = ann_find(b, cn, handle);
        if (slot < 0) {
            return MOQR_ERR_STALE_HANDLE;
        }
        v->ns = cn->anns[slot].ns;
    } else {
        moqr_result_t rc = moqr_core_subscription_prefix(
            b->core, cn->binding, handle, family == B_NSU_TRACKS, v->parts, 32,
            &v->ns);
        if (rc != MOQR_OK) {
            return rc;
        }
    }
    if (v->auth) {
        if (rec->token_count > MOQR_AUTH_MAX_TOKENS ||
            (rec->token_count > 0 && rec->tokens == NULL)) {
            return MOQR_ERR_INVAL;
        }
        for (size_t i = 0; i < rec->token_count; i++) {
            v->tokens[i] = rec->tokens[i];
        }
        v->token_count = rec->token_count;
    } else {
        const moqr_auth_token_t *t = NULL;
        size_t n = 0;
        if (moqr_core_request_tokens_get(b->core, cn->binding, handle, &t,
                                         &n) == MOQR_OK &&
            n <= MOQR_AUTH_MAX_TOKENS) {
            bind_tokens_to_session(t, n, v->tokens);
            v->token_count = n;
        }
    }
    return MOQR_OK;
}

static moqr_auth_action_t
bind_nsu_action(uint8_t family)
{
    return family == B_NSU_ANN ? MOQR_AUTH_PUBLISH_NAMESPACE
                               : MOQR_AUTH_SUBSCRIBE_NAMESPACE;
}

/* Retire what a namespace request held in the relay: its route or interest,
 * its announcement grant and its retained tokens. */
static void
bind_nsu_retire(moqr_bind_t *b, b_conn_t *cn, uint8_t family, uint64_t handle,
                uint64_t now_us)
{
    uint64_t bcookie = (uint64_t)(cn - b->conns) + 1u;
    moqr_result_t rc;
    switch (family) {
    case B_NSU_ANN: {
        int slot = ann_find(b, cn, handle);
        if (slot >= 0) {
            BIND_CALL_RETRY(rc, moqr_core_unannounce(b->core, cn->binding,
                                                     cn->anns[slot].ns));
            if (rc == MOQR_OK || rc == MOQR_ERR_STALE_HANDLE ||
                rc == MOQR_ERR_WRONG_STATE) {
                ann_clear_slot(b, cn, (uint32_t)slot);
            } else {
                b->session_errors++;
            }
        }
        moqr_core_grant_release(b->core, bcookie, handle);
        break;
    }
    case B_NSU_NS:
        (void)moqr_core_ns_unsubscribe(b->core, cn->binding, handle);
        break;
    case B_NSU_TRACKS:
        (void)moqr_core_tracks_unsubscribe(b->core, cn->binding, handle);
        break;
    default:
        return;
    }
    moqr_core_request_tokens_clear(b->core, cn->binding, handle);
}

static b_nsans_t *
bind_nsans_lookup(moqr_bind_t *b, const b_conn_t *cn, uint64_t handle,
                  bool initial)
{
    uint32_t ci = (uint32_t)(cn - b->conns);
    for (uint32_t i = 0; i < b->nsans_cap; i++) {
        if (b->nsans[i].used && b->nsans[i].conn == ci &&
            b->nsans[i].handle == handle && b->nsans[i].initial == initial) {
            return &b->nsans[i];
        }
    }
    return NULL;
}

/* The held answer to an update of this request. */
static b_nsans_t *
bind_nsans_find(moqr_bind_t *b, const b_conn_t *cn, uint64_t handle)
{
    return bind_nsans_lookup(b, cn, handle, false);
}

static void bind_nsans_drop(moqr_bind_t *b, b_nsans_t *e);

/* The request has ended: nothing held for it is owed any more. */
static void
bind_nsans_forget(moqr_bind_t *b, const b_conn_t *cn, uint64_t handle)
{
    for (int k = 0; k < 2; k++) {
        b_nsans_t *e = bind_nsans_lookup(b, cn, handle, k != 0);
        if (e != NULL) {
            bind_nsans_drop(b, e);
        }
    }
}

static void
bind_nsans_drop(moqr_bind_t *b, b_nsans_t *e)
{
    if (e->gres.gen != 0) {
        moqr_core_grant_abort(b->core, e->gres);
    }
    memset(e, 0, sizeof(*e));
    b->nsans_live--;
}

static void
bind_nsans_conn_gone(moqr_bind_t *b, const b_conn_t *cn)
{
    uint32_t ci = (uint32_t)(cn - b->conns);
    for (uint32_t i = 0; i < b->nsans_cap && b->nsans_live > 0; i++) {
        if (b->nsans[i].used && b->nsans[i].conn == ci) {
            bind_nsans_drop(b, &b->nsans[i]);
        }
    }
}

/* Hold an answer. A pool sized for every owed update cannot be full; if it
 * is, the connection is closed rather than leave an update unanswered. */
static b_nsans_t *
bind_nsans_hold(moqr_bind_t *b, b_conn_t *cn, uint8_t family, uint64_t handle,
                uint64_t id, uint64_t now_us)
{
    b_nsans_t *e = bind_nsans_find(b, cn, handle);
    if (e == NULL) {
        for (uint32_t i = 0; i < b->nsans_cap; i++) {
            if (!b->nsans[i].used) {
                e = &b->nsans[i];
                memset(e, 0, sizeof(*e));
                e->used = true;
                b->nsans_live++;
                break;
            }
        }
    }
    if (e == NULL) {
        b->session_errors++;
        (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                "namespace update answer", now_us);
        conn_detach(b, cn, now_us);
        return NULL;
    }
    e->family = family;
    e->conn = (uint32_t)(cn - b->conns);
    e->handle = handle;
    e->update_id = id;
    return e;
}

/* Hold a track subscription's first answer, sent on a later pump. */
static void
bind_tracks_hold_answer(moqr_bind_t *b, b_conn_t *cn, uint64_t handle,
                        bool reject, uint64_t err, uint64_t now_us)
{
    b_nsans_t *e = bind_nsans_lookup(b, cn, handle, true);
    if (e == NULL) {
        for (uint32_t i = 0; i < b->nsans_cap; i++) {
            if (!b->nsans[i].used) {
                e = &b->nsans[i];
                memset(e, 0, sizeof(*e));
                e->used = true;
                b->nsans_live++;
                break;
            }
        }
    }
    if (e == NULL) {
        b->session_errors++;
        (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                "track subscription answer", now_us);
        conn_detach(b, cn, now_us);
        return;
    }
    e->initial = true;
    e->family = B_NSU_TRACKS;
    e->conn = (uint32_t)(cn - b->conns);
    e->handle = handle;
    e->reject = reject;
    e->error_code = err;
}

/* Refuse an update: the request ends, and the relay stops serving it now. */
static void
bind_nsu_reject(moqr_bind_t *b, b_conn_t *cn, uint8_t family, uint64_t handle,
                uint64_t id, uint64_t err, uint64_t now_us)
{
    if (err == 0) {
        err = MOQ_REQUEST_ERROR_UNAUTHORIZED;
    }
    b_nsans_t *held = bind_nsans_find(b, cn, handle);
    if (held != NULL) {
        bind_nsans_drop(b, held);
    }
    bind_nsu_retire(b, cn, family, handle, now_us);
    moq_result_t rc = bind_nsu_answer(cn, family, handle, id, true, err, now_us);
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        b_nsans_t *e = bind_nsans_hold(b, cn, family, handle, id, now_us);
        if (e != NULL) {
            e->reject = true;
            e->error_code = err;
        }
        return;
    }
    if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
        b->session_errors++;
    }
}

/* True while the ordered ring still holds a namespace notification for this
 * namespace subscription: it names a suffix of the prefix in effect, so the
 * acceptance of a new prefix must not overtake it. */
static bool
bind_ring_holds_ns(const moqr_bind_t *b, uint64_t bcookie, uint64_t handle)
{
    for (uint32_t i = 0; i < b->pending_count; i++) {
        const moqr_intent_t *it =
            &b->pending[(b->pending_head + i) % b->pending_cap];
        if ((it->kind == MOQR_INTENT_NS_FOUND ||
             it->kind == MOQR_INTENT_NS_GONE) &&
            it->binding_cookie == bcookie && it->cookie == handle) {
            return true;
        }
    }
    return false;
}

/* One attempt to accept a held update. true when the entry is settled
 * (answered, refused, or moot); false while it must still be held. */
static bool
bind_nsu_commit(moqr_bind_t *b, b_conn_t *cn, b_nsans_t *e, uint64_t now_us)
{
    uint8_t family = e->family;
    uint64_t handle = e->handle;
    uint64_t id = e->update_id;
    uint64_t bcookie = (uint64_t)(cn - b->conns) + 1u;
    if (e->reject) {
        moq_result_t rc = bind_nsu_answer(cn, family, handle, id, true,
                                          e->error_code, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            return false;
        }
        if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
            b->session_errors++;
        }
        bind_nsans_drop(b, e);
        return true;
    }
    moq_namespace_update_record_t rec;
    b_nsu_view_t v;
    if (bind_nsu_owed(cn, family, handle, &rec) != MOQ_OK ||
        rec.update_request_id != id ||
        bind_nsu_view(b, cn, family, handle, &rec, &v) != MOQR_OK) {
        bind_nsans_drop(b, e);   /* nothing is owed any more */
        return true;
    }
    /* The new prefix outlives the acceptance, which ends the record's
     * borrow: keep a copy of its bytes. */
    uint8_t prefix_bytes[MOQ_FULL_TRACK_NAME_MAX];
    if (v.prefix) {
        size_t off = 0;
        for (size_t i = 0; i < v.ns.count; i++) {
            if (v.parts[i].len > sizeof(prefix_bytes) - off) {
                bind_nsans_drop(b, e);
                bind_nsu_reject(b, cn, family, handle, id,
                                MOQ_REQUEST_ERROR_EXCESSIVE_LOAD, now_us);
                return true;
            }
            if (v.parts[i].len > 0) {
                memcpy(prefix_bytes + off, v.parts[i].data, v.parts[i].len);
            }
            v.parts[i].data = prefix_bytes + off;
            off += v.parts[i].len;
        }
    }
    uint32_t tflags = 0;
    if (family == B_NSU_TRACKS) {
        tflags = (v.prefix ? MOQR_TRACKS_UPDATE_PREFIX : 0u) |
                 (v.forward_selected ? MOQR_TRACKS_UPDATE_FORWARD : 0u) |
                 (v.auth ? MOQR_TRACKS_UPDATE_TOKENS : 0u);
    }
    /* Stage the change before answering: everything it needs is held from
     * here, so the acceptance on the wire is always followed. */
    moqr_ns_update_prep_t nprep;
    moqr_tracks_update_prep_t tprep;
    memset(&nprep, 0, sizeof(nprep));
    memset(&tprep, 0, sizeof(tprep));
    moqr_result_t crc = MOQR_OK;
    if (family == B_NSU_NS && v.prefix) {
        BIND_CALL_RETRY(crc, moqr_core_ns_update_prepare(b->core, cn->binding,
                                                         handle, v.ns, &nprep));
    } else if (family == B_NSU_TRACKS && tflags != 0) {
        crc = moqr_core_tracks_update_prepare(b->core, cn->binding, handle,
                                              tflags, v.ns, v.forward, &tprep);
    }
    if (crc == MOQR_ERR_WOULD_BLOCK) {
        return false;
    }
    if (crc != MOQR_OK) {
        bind_nsans_drop(b, e);
        bind_nsu_reject(b, cn, family, handle, id,
                        crc == MOQR_ERR_CAPACITY || crc == MOQR_ERR_NOMEM
                            ? MOQ_REQUEST_ERROR_EXCESSIVE_LOAD
                            : MOQ_REQUEST_ERROR_INTERNAL_ERROR,
                        now_us);
        return true;
    }
    if (family == B_NSU_NS && v.prefix && bind_ring_holds_ns(b, bcookie, handle)) {
        moqr_core_ns_update_abort(b->core, &nprep);
        return false;   /* an old-prefix notification goes first */
    }
    moq_result_t arc = bind_nsu_answer(cn, family, handle, id, false, 0, now_us);
    if (arc != MOQ_OK) {
        moqr_core_ns_update_abort(b->core, &nprep);
        moqr_core_tracks_update_abort(b->core, &tprep);
        if (arc == MOQ_ERR_WOULD_BLOCK) {
            return false;
        }
        if (!fwd_result_is_gone(arc)) {
            b->session_errors++;
        }
        bind_nsans_drop(b, e);
        return true;
    }
    /* Accepted: apply the change it acknowledged. */
    moqr_grant_res_t gres = e->gres;
    e->gres.gen = 0;
    bind_nsans_drop(b, e);
    moqr_result_t prc = MOQR_OK;
    switch (family) {
    case B_NSU_ANN:
        if (gres.gen != 0) {
            moqr_core_grant_release(b->core, bcookie, handle);
            prc = moqr_core_grant_commit(b->core, gres, 0);
        }
        break;
    case B_NSU_NS:
        if (nprep.staged) {
            prc = moqr_core_ns_update_commit(b->core, &nprep);
        }
        break;
    case B_NSU_TRACKS:
        if (tprep.staged) {
            prc = moqr_core_tracks_update_commit(b->core, &tprep);
        }
        break;
    default:
        break;
    }
    if (prc != MOQR_OK) {
        /* The acceptance is on the wire and the relay could not follow it:
         * the connection cannot be told the truth, so it ends. */
        b->session_errors++;
        (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                "namespace update failed", now_us);
        conn_detach(b, cn, now_us);
    }
    return true;
}

/* The update is authorized (`lease` is an announcement's revalidation
 * lease): take the tokens it carries and any grant, then accept it. */
static void
bind_nsu_decide(moqr_bind_t *b, b_conn_t *cn, uint8_t family, uint64_t handle,
                uint64_t id, uint64_t lease, uint64_t now_us)
{
    moq_namespace_update_record_t rec;
    b_nsu_view_t v;
    if (bind_nsu_owed(cn, family, handle, &rec) != MOQ_OK ||
        rec.update_request_id != id) {
        return;   /* nothing is owed any more */
    }
    if (bind_nsu_view(b, cn, family, handle, &rec, &v) != MOQR_OK) {
        bind_nsu_reject(b, cn, family, handle, id,
                        MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        return;
    }
    moqr_grant_res_t gres;
    memset(&gres, 0, sizeof(gres));
    if (family == B_NSU_ANN && lease > 0) {
        moqr_park_req_t gpr;
        bind_park_init(&gpr, cn, b, MOQR_AUTH_PUBLISH_NAMESPACE, v.ns,
                       (moq_bytes_t){ NULL, 0 }, handle);
        if (bind_grant_reserve(b, &gpr, v.tokens, v.token_count, lease, now_us,
                               &gres) != MOQR_OK) {
            bind_nsu_reject(b, cn, family, handle, id,
                            MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
            return;
        }
    }
    if (v.auth) {
        moqr_auth_token_t mt[MOQR_AUTH_MAX_TOKENS];
        bind_tokens_to_relay(v.tokens, v.token_count, mt);
        if (moqr_core_request_tokens_set(b->core, cn->binding, handle,
                                         v.token_count > 0 ? mt : NULL,
                                         v.token_count) != MOQR_OK) {
            if (gres.gen != 0) moqr_core_grant_abort(b->core, gres);
            bind_nsu_reject(b, cn, family, handle, id,
                            MOQ_REQUEST_ERROR_EXCESSIVE_LOAD, now_us);
            return;
        }
    }
    b_nsans_t *e = bind_nsans_hold(b, cn, family, handle, id, now_us);
    if (e == NULL) {
        if (gres.gen != 0) moqr_core_grant_abort(b->core, gres);
        return;
    }
    e->reject = false;
    e->gres = gres;
    (void)bind_nsu_commit(b, cn, e, now_us);
}

/* Authorize from `stage` on: REQUEST_UPDATE first, then the original
 * namespace action. A DEFER parks the stage that deferred. */
static void
bind_nsu_authorize(moqr_bind_t *b, b_conn_t *cn, uint8_t family,
                   uint64_t handle, uint64_t id, moqr_auth_action_t stage,
                   uint64_t now_us)
{
    moq_namespace_update_record_t rec;
    b_nsu_view_t v;
    if (bind_nsu_owed(cn, family, handle, &rec) != MOQ_OK ||
        rec.update_request_id != id) {
        return;
    }
    if (bind_nsu_view(b, cn, family, handle, &rec, &v) != MOQR_OK) {
        bind_nsu_reject(b, cn, family, handle, id,
                        MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        return;
    }
    uint64_t lease = 0;
    for (;;) {
        uint64_t err = 0, ticket = 0;
        bind_auth_result_t ar = bind_authorize(
            b, cn, stage, v.ns, (moq_bytes_t){ NULL, 0 }, v.tokens,
            v.token_count, now_us, &err, &ticket,
            stage == MOQR_AUTH_PUBLISH_NAMESPACE ? &lease : NULL);
        if (ar == BIND_AUTH_DEFER) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, stage, v.ns, (moq_bytes_t){ NULL, 0 },
                           handle);
            pr.sub_filter_type = BIND_PARK_NS_UPDATE;
            pr.sub_start_group = id;
            if (bind_park(b, &pr, v.tokens, v.token_count, ticket, now_us) ==
                MOQR_OK) {
                return;   /* the update stays owed until the decision */
            }
            ar = BIND_AUTH_FAIL_CLOSED;
            err = MOQ_REQUEST_ERROR_UNAUTHORIZED;
        }
        if (ar != BIND_AUTH_ALLOW) {
            bind_nsu_reject(b, cn, family, handle, id, err, now_us);
            return;
        }
        if (stage == MOQR_AUTH_REQUEST_UPDATE) {
            stage = bind_nsu_action(family);
            continue;
        }
        break;
    }
    bind_nsu_decide(b, cn, family, handle, id, lease, now_us);
}

/* A deferred stage of an update's authorization was decided. */
static void
bind_nsu_resolved(moqr_bind_t *b, b_conn_t *cn, const moqr_park_req_t *v,
                  const moqr_auth_verdict_t *verdict, uint64_t now_us)
{
    uint8_t family = bind_nsu_family(v->session_cookie);
    uint64_t id = v->sub_start_group;
    if (family == 0) {
        return;
    }
    if (verdict->decision != MOQR_AUTH_ALLOW) {
        bind_nsu_reject(b, cn, family, v->session_cookie, id,
                        verdict->error_code != 0
                            ? verdict->error_code
                            : MOQ_REQUEST_ERROR_UNAUTHORIZED,
                        now_us);
        return;
    }
    if (v->action == MOQR_AUTH_REQUEST_UPDATE) {
        bind_nsu_authorize(b, cn, family, v->session_cookie, id,
                           bind_nsu_action(family), now_us);
        return;
    }
    bind_nsu_decide(b, cn, family, v->session_cookie, id,
                    verdict->revalidate_after_us, now_us);
}

/* Retry every held answer, once per pump. */
static void
bind_nsu_pump(moqr_bind_t *b, uint64_t now_us)
{
    for (uint32_t i = 0; i < b->nsans_cap && b->nsans_live > 0; i++) {
        b_nsans_t *e = &b->nsans[i];
        if (!e->used) {
            continue;
        }
        b_conn_t *cn = &b->conns[e->conn];
        if (!cn->used || cn->closed || cn->session == NULL) {
            bind_nsans_drop(b, e);
            continue;
        }
        if (e->initial) {
            bind_tracks_answer_held(b, cn, e, now_us);
            continue;
        }
        (void)bind_nsu_commit(b, cn, e, now_us);
    }
}

/* -- Track discovery: SUBSCRIBE_TRACKS admission ---------------------------- */

/* Refuse a track subscription. An answer the session cannot take yet is
 * held and sent on a later pump. */
static void
bind_tracks_reject(moqr_bind_t *b, b_conn_t *cn, uint64_t handle, uint64_t err,
                   uint64_t now_us)
{
    moq_reject_subscribe_tracks_cfg_t r;
    moq_reject_subscribe_tracks_cfg_init(&r);
    r.error_code = (moq_request_error_t)err;
    moq_result_t rc = moq_session_reject_subscribe_tracks(
        cn->session, (moq_track_sub_handle_t){ ._opaque = handle }, &r, now_us);
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        bind_tracks_hold_answer(b, cn, handle, true, err, now_us);
    } else if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
        b->session_errors++;
    }
}

/* Accept a registered track subscription. Its interest stays held (nothing
 * is offered for it) until the acceptance is out; one the session cannot
 * take yet is held and sent on a later pump. False when it was not
 * accepted and the interest is gone. */
static bool
bind_tracks_accept(moqr_bind_t *b, b_conn_t *cn, uint64_t handle,
                   uint64_t now_us)
{
    moq_accept_subscribe_tracks_cfg_t a;
    moq_accept_subscribe_tracks_cfg_init(&a);
    moq_result_t rc = moq_session_accept_subscribe_tracks(
        cn->session, (moq_track_sub_handle_t){ ._opaque = handle }, &a, now_us);
    if (rc == MOQ_OK) {
        (void)moqr_core_tracks_answered(b->core, cn->binding, handle);
        return true;
    }
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        bind_tracks_hold_answer(b, cn, handle, false, 0, now_us);
        return true;
    }
    /* Not accepted: no interest may outlive it. */
    (void)moqr_core_tracks_unsubscribe(b->core, cn->binding, handle);
    moqr_core_request_tokens_clear(b->core, cn->binding, handle);
    if (!fwd_result_is_gone(rc)) {
        b->session_errors++;
    }
    return false;
}

/* Retry a held first answer. */
static void
bind_tracks_answer_held(moqr_bind_t *b, b_conn_t *cn, b_nsans_t *e,
                        uint64_t now_us)
{
    uint64_t handle = e->handle;
    bool reject = e->reject;
    uint64_t err = e->error_code;
    bind_nsans_drop(b, e);   /* re-held below if still blocked */
    if (reject) {
        bind_tracks_reject(b, cn, handle, err, now_us);
    } else {
        (void)bind_tracks_accept(b, cn, handle, now_us);
    }
}

/* Admit an authorized track subscription: retain its tokens (each
 * discovered track is authorized with them), register its interest, then
 * accept it. Discovery offers matching tracks once the acceptance is out. */
static void
bind_tracks_admit(moqr_bind_t *b, b_conn_t *cn, uint64_t handle, moqr_ns_t prefix,
                  bool forward, bool include_properties, uint32_t group_order,
                  const moqr_auth_token_t *tokens, size_t token_count,
                  uint64_t now_us)
{
    if (moqr_core_request_tokens_set(b->core, cn->binding, handle, tokens,
                                     token_count) != MOQR_OK) {
        bind_tracks_reject(b, cn, handle, MOQ_REQUEST_ERROR_EXCESSIVE_LOAD,
                           now_us);
        return;
    }
    moqr_tracks_req_t rq;
    moqr_tracks_req_init_sized(&rq, sizeof(rq));
    rq.prefix = prefix;
    rq.cookie = handle;
    rq.forward = forward;
    rq.include_properties = include_properties;
    rq.group_order = (moqr_group_order_t)group_order;
    rq.held = true;
    moqr_result_t rc = moqr_core_tracks_subscribe(b->core, cn->binding, &rq);
    if (rc != MOQR_OK) {
        moqr_core_request_tokens_clear(b->core, cn->binding, handle);
        bind_tracks_reject(b, cn, handle,
                           rc == MOQR_ERR_CAPACITY
                               ? MOQ_REQUEST_ERROR_EXCESSIVE_LOAD
                               : MOQ_REQUEST_ERROR_INTERNAL_ERROR,
                           now_us);
        return;
    }
    (void)bind_tracks_accept(b, cn, handle, now_us);
}

/* -- Track discovery: offering matching tracks ------------------------------ */

/* Hold a deferred ALLOW for its offer. False when there is no room. */
static bool
bind_dgrant_put(moqr_bind_t *b, b_conn_t *cn, uint64_t tcookie, uint64_t track,
                uint64_t track_gen, uint32_t input_gen, moqr_grant_res_t gres)
{
    for (uint32_t i = 0; i < BIND_DGRANTS; i++) {
        b_dgrant_t *g = &b->dgrants[i];
        if (!g->used) {
            g->used = true;
            g->conn = (uint32_t)(cn - b->conns);
            g->tcookie = tcookie;
            g->track = track;
            g->track_gen = track_gen;
            g->input_gen = input_gen;
            g->gres = gres;
            return true;
        }
    }
    return false;
}

/* The connection is gone: its held ALLOWs with it (the core retires their
 * reservations with the binding). */
static void
bind_dgrant_conn_gone(moqr_bind_t *b, const b_conn_t *cn)
{
    uint32_t ci = (uint32_t)(cn - b->conns);
    for (uint32_t i = 0; i < BIND_DGRANTS; i++) {
        if (b->dgrants[i].used && b->dgrants[i].conn == ci) {
            memset(&b->dgrants[i], 0, sizeof(b->dgrants[i]));
        }
    }
}

/* Take the held ALLOW for this offer, if any. Held decisions made under the
 * track subscription's earlier inputs, or for an earlier instance of the
 * track, no longer authorize anything: their reservations are released. */
static bool
bind_dgrant_take(moqr_bind_t *b, b_conn_t *cn, const moqr_discovery_t *d,
                 moqr_grant_res_t *gres)
{
    bool found = false;
    uint32_t ci = (uint32_t)(cn - b->conns);
    for (uint32_t i = 0; i < BIND_DGRANTS; i++) {
        b_dgrant_t *g = &b->dgrants[i];
        if (!g->used || g->conn != ci || g->tcookie != d->cookie) {
            continue;
        }
        bool same_track = g->track == d->track._opaque;
        if (g->input_gen == d->input_gen && same_track &&
            g->track_gen == d->track_gen && !found) {
            *gres = g->gres;
            found = true;
        } else if (g->input_gen == d->input_gen && !same_track) {
            continue;   /* another track's decision, still current */
        } else if (g->gres.gen != 0) {
            moqr_core_grant_abort(b->core, g->gres);
        }
        memset(g, 0, sizeof(*g));
    }
    return found;
}

#define BIND_DISCOVERY_PER_PUMP 32u

/* Offer the binding's unoffered matches, each as a PUBLISH whose
 * subscription the relay then serves like any other: authorized for the
 * track with the track subscription's tokens (never copied into the
 * PUBLISH), carrying the subscription's FORWARD, GROUP_ORDER and, when asked
 * for, the track's properties. A track that cannot get a subscription is
 * reported with PUBLISH_SKIPPED and is not offered again for that source
 * (§3.6.3); one the subscriber is not authorized for is not offered until
 * its tokens change. */
static void
bind_discovery_pump(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    for (uint32_t n = 0; n < BIND_DISCOVERY_PER_PUMP; n++) {
        if (!cn->used || cn->closed) {
            return;
        }
        moqr_discovery_t d;
        if (moqr_core_next_discovery(b->core, cn->binding, &d, sizeof(d)) != MOQR_OK) {
            return;
        }
        moq_resolved_token_t toks[MOQR_AUTH_MAX_TOKENS];
        size_t ntok = 0;
        const moqr_auth_token_t *rt = NULL;
        if (moqr_core_request_tokens_get(b->core, cn->binding, d.cookie, &rt,
                                         &ntok) != MOQR_OK ||
            ntok > MOQR_AUTH_MAX_TOKENS) {
            ntok = 0;
        }
        bind_tokens_to_session(rt, ntok, toks);
        uint64_t err = 0, ticket = 0, lease = 0;
        /* A deferred ALLOW is offered under the grant reserved when it
         * arrived; any other offer is authorized now, with the track
         * subscription's current tokens. */
        moqr_grant_res_t gres;
        memset(&gres, 0, sizeof(gres));
        bool held = d.authorized && bind_dgrant_take(b, cn, &d, &gres);
        bind_auth_result_t ar = held
            ? BIND_AUTH_ALLOW
            : bind_authorize(b, cn, MOQR_AUTH_SUBSCRIBE, d.ns, d.name, toks,
                             ntok, now_us, &err, &ticket, &lease);
        if (ar == BIND_AUTH_DEFER) {
            /* Hold the offer until the verifier decides; its ticket is
             * resolved through moqr_bind_auth_resolve, or retired with the
             * connection. */
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_SUBSCRIBE, d.ns, d.name,
                           d.cookie);
            pr.sub_filter_type = BIND_PARK_DISCOVERY;
            pr.sub_start_group = d.track._opaque;
            pr.sub_start_object = d.track_gen;
            pr.sub_end_group_delta = d.input_gen;
            moqr_track_t track = d.track;
            uint64_t gen = d.track_gen;
            uint64_t tcookie = d.cookie;
            uint32_t igen = d.input_gen;
            bool parked = bind_park(b, &pr, toks, ntok, ticket, now_us) ==
                          MOQR_OK;
            (void)moqr_core_discovery_done(b->core, cn->binding, tcookie,
                                           track, gen, igen,
                                           parked ? MOQR_DISCOVERY_PENDING
                                                  : MOQR_DISCOVERY_DECLINED);
            continue;
        }
        if (ar == BIND_AUTH_ALLOW && !held && lease > 0) {
            /* A time-limited ALLOW is kept only as a revalidation grant,
             * reserved before the offer; without one nothing is offered,
             * as a SUBSCRIBE without one is refused. */
            moqr_park_req_t gpr;
            bind_park_init(&gpr, cn, b, MOQR_AUTH_SUBSCRIBE, d.ns, d.name,
                           d.cookie);
            if (bind_grant_reserve(b, &gpr, toks, ntok, lease, now_us,
                                   &gres) != MOQR_OK) {
                ar = BIND_AUTH_FAIL_CLOSED;
            }
        }
        if (ar != BIND_AUTH_ALLOW) {
            (void)moqr_core_discovery_done(b->core, cn->binding, d.cookie,
                                           d.track, d.track_gen, d.input_gen,
                                           MOQR_DISCOVERY_DECLINED);
            continue;
        }
        moq_result_t prc = MOQ_ERR_REQUEST_BLOCKED;
        moq_publication_t pub = { 0 };
        int slot = dsub_free_slot(b, cn);
        /* The relay's subscription is reserved before the offer goes on the
         * wire: a track the relay has no subscription for is SKIPPED, never
         * offered and then finished for an internal error (§3.6.3). */
        bool reserved = slot >= 0 &&
                        moqr_core_internal_sub_reserve(b->core) == MOQR_OK;
        if (reserved) {
            moqr_bind_meta_ref_t props;
            memset(&props, 0, sizeof(props));
            if (d.include_properties) {
                (void)moqr_bind_meta_latest(b, d.track, &props);
            }
            moq_publish_cfg_t pc;
            moq_publish_cfg_init_sized(&pc, sizeof(pc));
            pc.track_namespace = (moq_namespace_t){ .parts = d.ns.parts,
                                                    .count = d.ns.count };
            /* A PUBLISH names the track's Largest Object once objects exist:
             * the session takes it from this track history. */
            if (d.has_largest) {
                (void)moq_session_note_object_published(
                    cn->session, &pc.track_namespace, d.name, d.largest_group,
                    d.largest_object);
            }
            pc.track_name = d.name;
            pc.has_forward = true;
            pc.forward = d.forward;
            if (d.group_order != MOQR_GROUP_ORDER_DEFAULT) {
                pc.has_group_order = true;
                pc.group_order = (moq_group_order_t)d.group_order;
            }
            if (props.state == MOQR_BIND_META_PRESENT &&
                bind_meta_can_replay(&props, moq_session_version(cn->session))) {
                pc.track_properties = props.bytes;
            }
            prc = moq_session_publish(cn->session, &pc, now_us, &pub);
            moqr_bind_meta_release(b, &props);
        }
        if (reserved && prc != MOQ_OK) {
            moqr_core_internal_sub_release_reserve(b->core);
        }
        if (prc == MOQ_ERR_WOULD_BLOCK) {
            /* The same match is offered again next pump: a held ALLOW stays
             * held; a decision made now is made again then. */
            if (gres.gen != 0 &&
                !(held && bind_dgrant_put(b, cn, d.cookie, d.track._opaque,
                                          d.track_gen, d.input_gen, gres))) {
                moqr_core_grant_abort(b->core, gres);
            } else if (held && gres.gen == 0) {
                (void)bind_dgrant_put(b, cn, d.cookie, d.track._opaque,
                                      d.track_gen, d.input_gen, gres);
            }
            return;
        }
        if (prc != MOQ_OK && gres.gen != 0) {
            moqr_core_grant_abort(b->core, gres);
            memset(&gres, 0, sizeof(gres));
        }
        if (prc == MOQ_OK) {
            /* The relay's subscription starts paused: nothing is sent before
             * the subscriber's PUBLISH_OK, which sets its Forward. */
            moqr_subscribe_req_t rq;
            moqr_subscribe_req_init(&rq);
            rq.ns = d.ns;
            rq.name = d.name;
            rq.group_order = d.group_order;
            rq.cookie = pub._opaque;
            rq.forward = false;
            rq.range_end_holds = bind_range_end_holds(cn);
            rq.reserved = true;
            memset(&cn->dsubs[slot], 0, sizeof(cn->dsubs[slot]));
            cn->dsubs[slot].used = true;
            cn->dsubs[slot].publication = true;
            cn->dsubs[slot].sub_raw = pub._opaque;
            moqr_track_t track = d.track;
            uint64_t gen = d.track_gen;
            uint64_t tcookie = d.cookie;
            uint32_t igen = d.input_gen;
            moqr_sub_t rs;
            moqr_result_t src;
            BIND_CALL_RETRY(src, moqr_core_subscribe(b->core, cn->binding, &rq,
                                                     &rs));
            if (src != MOQR_OK) {
                moqr_core_internal_sub_release_reserve(b->core);
                if (gres.gen != 0) {
                    moqr_core_grant_abort(b->core, gres);
                }
            }
            if (!cn->used || cn->closed) {
                return;
            }
            if (src == MOQR_OK) {
                cn->dsubs[slot].rsub = rs;
                bind_grant_commit_sub(b, gres, rs, now_us);
            } else {
                memset(&cn->dsubs[slot], 0, sizeof(cn->dsubs[slot]));
                moqr_pd_desc_t pd = bind_local_pd(MOQR_PD_INTERNAL_ERROR);
                uint64_t wire = 0;
                moq_finish_publish_cfg_t fc;
                moq_finish_publish_cfg_init(&fc);
                if (moqr_pd_desc_emit(pd, moqr_bind_conn_version(b, cn->session),
                                      &wire) == MOQR_OK) {
                    fc.status_code = wire;
                }
                if (moq_session_finish_publish(cn->session, pub, &fc, now_us) !=
                    MOQ_OK) {
                    b->session_errors++;
                }
            }
            (void)moqr_core_discovery_done(b->core, cn->binding, tcookie, track,
                                           gen, igen, MOQR_DISCOVERY_OFFERED);
            continue;
        }
        /* No subscription for this track: say so on the track
         * subscription's stream, relative to its prefix. */
        size_t skip = d.prefix_count <= d.ns.count ? d.prefix_count : d.ns.count;
        moq_namespace_t suffix = { .parts = d.ns.parts + skip,
                                   .count = d.ns.count - skip };
        moq_result_t src = moq_session_send_publish_blocked(
            cn->session, (moq_track_sub_handle_t){ ._opaque = d.cookie },
            &suffix, d.name, now_us);
        if (src == MOQ_ERR_WOULD_BLOCK) {
            return;
        }
        if (src != MOQ_OK && !fwd_result_is_gone(src)) {
            b->session_errors++;
        }
        (void)moqr_core_discovery_done(b->core, cn->binding, d.cookie, d.track,
                                       d.track_gen, d.input_gen,
                                       MOQR_DISCOVERY_SKIPPED);
    }
}

/* A deferred discovery decision arrived. It counts only for the track
 * subscription's inputs it was asked under (a token update since then makes
 * the core refuse it as stale). An ALLOW is held for its offer with its
 * revalidation grant; a lease the relay cannot keep declines the track. */
static void
bind_discovery_resolved(moqr_bind_t *b, b_conn_t *cn, const moqr_park_req_t *v,
                        const moqr_auth_verdict_t *verdict, uint64_t now_us)
{
    moqr_track_t track = { ._opaque = v->sub_start_group };
    uint64_t track_gen = v->sub_start_object;
    uint32_t input_gen = (uint32_t)v->sub_end_group_delta;
    uint32_t outcome = MOQR_DISCOVERY_DECLINED;
    moqr_grant_res_t gres;
    memset(&gres, 0, sizeof(gres));
    if (verdict->decision == MOQR_AUTH_ALLOW) {
        outcome = MOQR_DISCOVERY_ALLOWED;
        if (verdict->revalidate_after_us > 0 &&
            moqr_core_grant_reserve(b->core, v, verdict->revalidate_after_us,
                                    now_us, &gres) != MOQR_OK) {
            outcome = MOQR_DISCOVERY_DECLINED;
        }
    }
    moqr_result_t rc = moqr_core_discovery_done(b->core, cn->binding,
                                                v->session_cookie, track,
                                                track_gen, input_gen, outcome);
    if (outcome != MOQR_DISCOVERY_ALLOWED) {
        return;
    }
    if (rc != MOQR_OK ||
        !bind_dgrant_put(b, cn, v->session_cookie, track._opaque, track_gen,
                         input_gen, gres)) {
        if (gres.gen != 0) {
            moqr_core_grant_abort(b->core, gres);
        }
    }
}

/* The subscriber answered a PUBLISH discovery sent: PUBLISH_OK starts the
 * relay subscription with the Forward the subscriber chose; PUBLISH_ERROR
 * retires it. */
static void
bind_publication_answered(moqr_bind_t *b, b_conn_t *cn, uint64_t pub_raw,
                          bool ok, bool forward, uint64_t now_us)
{
    b_dsub_t *ds = dsub_find(b, cn, pub_raw);
    if (ds == NULL || !ds->publication) {
        return;
    }
    if (!ok) {
        bind_dsub_retire(b, cn, ds, now_us);
        return;
    }
    moqr_result_t frc = moqr_core_sub_set_forward(b->core, ds->rsub, forward,
                                                  now_us);
    if (frc != MOQR_OK && frc != MOQR_ERR_STALE_HANDLE) {
        b->session_errors++;
    }
}

/* Which signal ended the relay's OWN upstream request. draft-18 gives these
 * two different meanings and they must not collapse into one cause:
 *
 *   GOAWAY on a request stream (Section 10.4) — the upstream is going away and
 *   the request should be re-issued elsewhere;
 *   REQUEST_ERROR REDIRECT (Section 10.6.1) — nobody is going away; this
 *   endpoint cannot serve the request and names where it could be served.
 *
 * draft-16 has neither: its GOAWAY (Section 9.4) is session-scoped only, and
 * its REQUEST_ERROR registry (Section 13.4.2) assigns no REDIRECT. */
typedef enum bind_upstream_end {
    BIND_UP_END_GOAWAY = 0,
    BIND_UP_END_REDIRECT,
} bind_upstream_end_t;

/* The PUBLISH_DONE status an ACTIVE upstream subscription ends with. The relay
 * neither re-issues a request nor follows a Redirect, so what it owes its
 * downstream subscribers is a truthful terminal for its OWN state:
 *
 *   GOAWAY   -> GOING_AWAY: the upstream really is departing (0x4 in both
 *               registries, d16 Section 13.4.3 / d18 Section 15.10.3);
 *   REDIRECT -> TRACK_ENDED: nobody is departing. This relay will not serve
 *               the track from the named location, so the track has ended as
 *               far as these subscribers are concerned (0x2 in both). Saying
 *               GOING_AWAY here would assert a departure that is not
 *               happening. */
static moqr_pd_status_t
bind_upstream_end_pd(bind_upstream_end_t how)
{
    switch (how) {
    case BIND_UP_END_GOAWAY:
        return MOQR_PD_GOING_AWAY;
    case BIND_UP_END_REDIRECT:
        return MOQR_PD_TRACK_ENDED;
    }
    return MOQR_PD_INTERNAL_ERROR;
}

/* The REQUEST_ERROR a PENDING upstream's parked subscribers are rejected with.
 *
 * TIMEOUT (0x2) is not available to either cause: no timeout elapsed, and
 * draft-18 Section 10.6 defines TIMEOUT as an implementation timeout the relay
 * did not hit. REDIRECT (0x34) is not available either — it is meaningful only
 * with the Redirect structure of Section 10.6.1, which this relay does not
 * carry, and draft-16 does not assign it at all. draft-18's GOING_AWAY (0x6)
 * would be precise for the GOAWAY cause, but draft-16 assigns no GOING_AWAY in
 * this registry, and REQUEST_ERROR codes travel to downstream subscribers as a
 * plain number with no per-target translation — so a d18-only code here would
 * reach a draft-16 subscriber as an unassigned number.
 *
 * The generic INTERNAL_ERROR (0x0) is assigned identically in both registries
 * and states nothing false. */
static uint32_t
bind_upstream_end_reqerr(bind_upstream_end_t how)
{
    switch (how) {
    case BIND_UP_END_GOAWAY:
    case BIND_UP_END_REDIRECT:
        return BIND_REQERR_INTERNAL;
    }
    return BIND_REQERR_INTERNAL;
}

/* A relay-originated request was terminated by the upstream. The relay does NOT
 * follow redirect URIs or migrate requests to a new session (a later routing
 * feature), so it treats the request as terminal and notifies downstreams
 * rather than hanging. The only request the relay originates is the upstream
 * SUBSCRIBE, so only that family maps to tracked state: an ACTIVE upstream
 * terminates its downstream subscribers with a wire SUBSCRIBE_DONE
 * (source_done); a PENDING upstream (terminated before its OK) rejects its
 * parked downstream subscribers (upstream_error). Any other family, or a
 * handle that matches no live upstream subscription, is a no-op — these are
 * terminal responses to the relay's OWN requests, not peer requests owed a
 * wire reply.
 *
 * The relay implements no reissue or retry schedule, so no answer it produces
 * here is retryable, whatever retry interval the upstream offered. */
static void
bind_upstream_terminated(moqr_bind_t *b, b_conn_t *cn,
                         moq_request_family_t family, uint64_t handle_raw,
                         bind_upstream_end_t how, uint64_t now_us)
{
    if (family != MOQ_REQUEST_FAMILY_SUBSCRIBE) {
        return; /* the relay originates no other upstream request family */
    }
    for (uint32_t i = 0; i < b->n_usubs; i++) {
        if (!cn->usubs[i].used || cn->usubs[i].ssub._opaque != handle_raw) {
            continue;
        }
        moqr_result_t rc;
        BIND_CALL_RETRY(rc, moqr_core_source_done(
            b->core, cn->usubs[i].track, cn->usubs[i].track_gen,
            bind_local_pd(bind_upstream_end_pd(how)), now_us));
        if (rc == MOQR_ERR_WRONG_STATE) {
            /* PENDING upstream (terminal before OK): reject the parked
             * downstream subscribers instead of terminating active ones. */
            BIND_CALL_RETRY(rc, moqr_core_upstream_error(
                b->core, cn->usubs[i].track, cn->usubs[i].track_gen,
                bind_upstream_end_reqerr(how), now_us));
        }
        if (rc != MOQR_OK && rc != MOQR_ERR_STALE_HANDLE) {
            b->session_errors++;
        }
        usub_release(b, &cn->usubs[i]);
    }
}

#ifdef MOQR_BIND_TESTING
/* Drive the production upstream-termination dispatch for one connection.
 * draft-18 makes two of the four cause/state combinations unreachable over the
 * wire — a REQUEST_ERROR cannot answer a request that already had its OK, and
 * a per-request GOAWAY is eligible only on an ESTABLISHED request — so those
 * two rows are pinned here, against the same function the event handler calls,
 * rather than left unpinned. */
/* The first live upstream subscription this bind holds, as {conn slot, the
 * handle on that connection's session} — the two coordinates the termination
 * dispatch matches on. */
void
moqr_bind_debug_fwd_stats(const moqr_bind_t *b, moqr_bind_fwd_debug_t *out)
{
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (b == NULL) {
        return;
    }
    *out = b->dbg_fwd;
    out->bytes_used = b->fwd_bytes_used;
    out->bytes_cap = b->fwd_bytes_cap;
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        const b_conn_t *cn = &b->conns[ci];
        if (!cn->used || cn->refusals == NULL) {
            continue;
        }
        for (uint32_t i = 0; i < b->n_refusals; i++) {
            if (cn->refusals[i].used &&
                bind_kind_is_accept(cn->refusals[i].kind)) {
                out->accepts_pending++;
            } else if (cn->refusals[i].used) {
                out->refusals_pending++;
            }
        }
    }
}

bool
moqr_bind_debug_sg_handle(const moqr_bind_t *b, const moq_session_t *session,
                          uint64_t group, uint64_t subgroup,
                          moq_subgroup_handle_t *out)
{
    if (b == NULL || session == NULL || out == NULL) {
        return false;
    }
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        const b_conn_t *cn = &b->conns[ci];
        if (!cn->used || cn->session != session) {
            continue;
        }
        for (uint32_t i = 0; i < b->n_sgs; i++) {
            if (cn->sgs[i].used && cn->sgs[i].group == group &&
                cn->sgs[i].subgroup == subgroup) {
                *out = cn->sgs[i].h;
                return true;
            }
        }
    }
    return false;
}

bool
moqr_bind_debug_first_usub(const moqr_bind_t *b, uint32_t *slot,
                           uint64_t *handle_raw)
{
    if (b == NULL || slot == NULL || handle_raw == NULL) {
        return false;
    }
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        if (!b->conns[ci].used) {
            continue;
        }
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (b->conns[ci].usubs[i].used) {
                *slot = ci;
                *handle_raw = b->conns[ci].usubs[i].ssub._opaque;
                return true;
            }
        }
    }
    return false;
}

void
moqr_bind_debug_upstream_terminated(moqr_bind_t *b, uint32_t slot,
                                    uint32_t family, uint64_t handle_raw,
                                    int redirect, uint64_t now_us)
{
    if (b == NULL || slot >= b->max_conns || !b->conns[slot].used) {
        return;
    }
    bind_upstream_terminated(b, &b->conns[slot], (moq_request_family_t)family,
                             handle_raw,
                             redirect ? BIND_UP_END_REDIRECT
                                      : BIND_UP_END_GOAWAY,
                             now_us);
}
#endif

/* Map an inbound data event's sub/pub handle to the relay track it feeds (the
 * relay's upstream subscription or a peer-initiated publish). MOQR_TRACK_INVALID
 * when it targets neither — i.e. not relay ingest. */
static moqr_track_t
bind_ingest_track(moqr_bind_t *b, b_conn_t *cn, moq_subscription_t sub,
                  moq_publication_t pub)
{
    /* A source whose cancellation is owed delivers nothing more. */
    if (moq_subscription_is_valid(sub)) {
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (cn->usubs[i].used && cn->usubs[i].ssub._opaque == sub._opaque) {
                return cn->usubs[i].cancel_owed ? MOQR_TRACK_INVALID
                                                : cn->usubs[i].track;
            }
        }
    } else if (moq_publication_is_valid(pub)) {
        for (uint32_t i = 0; i < b->n_pubs; i++) {
            if (cn->pubs[i].used && cn->pubs[i].pub_raw == pub._opaque) {
                return cn->pubs[i].cancel_owed ? MOQR_TRACK_INVALID
                                               : cn->pubs[i].track;
            }
        }
    }
    return MOQR_TRACK_INVALID;
}

static void bind_track_malformed(moqr_bind_t *b, b_conn_t *cn,
                                 moq_subscription_t sub, moq_publication_t pub,
                                 uint64_t now_us);
static void bind_fetches_malformed(moqr_bind_t *b, moqr_track_t track,
                                   uint64_t now_us);
static void bind_reset_track_streams(moqr_bind_t *b, moqr_track_t track,
                                     uint64_t now_us);
static bool bind_cancel_settled(moqr_bind_t *b, b_conn_t *cn, moq_result_t rc,
                                const char *what, uint64_t now_us);
static moq_result_t bind_cancel_upstream_sub(b_conn_t *cn, moq_subscription_t ssub,
                                             uint64_t now_us);
static moq_result_t bind_cancel_upstream_pub(b_conn_t *cn, moq_publication_t pub,
                                             uint64_t code, uint64_t now_us);

/* An object the core refused to ingest. A duplicate of a retained Object
 * that differs in Delivery Mode, Subgroup ID, Priority, status or Payload
 * makes the Track malformed (draft-22 §7.1, §12.1); an identical one is the
 * ordinary duplicate a relay drops (§7.3: "SHOULD attempt to deduplicate").
 * Any other refusal leaves a hole in the subgroup the object belonged to, so
 * the subgroup is not passed on as complete (§7.4, §11.3.2). */
static uint32_t
bind_ingest_refused(moqr_bind_t *b, b_conn_t *cn, moqr_track_t track,
                    const moqr_log_append_desc_t *d, moqr_result_t rc,
                    moq_subscription_t sub, moq_publication_t pub,
                    uint64_t now_us)
{
    /* Identity is the Location (Group, Object) across every subgroup and the
     * datagram list (§7.1: "identified by the Object's Full Track Name, Group
     * ID and Object ID"), and the comparison runs before any refusal is
     * treated as a plain drop, whatever the log refused it for. */
    (void)rc;
    uint32_t dup = moqr_core_internal_duplicate(b->core, track, d);
    switch (dup) {
    case MOQR_LOG_DUP_IDENTICAL:
    case MOQR_LOG_DUP_UNVERIFIED:
    case MOQR_LOG_DUP_UPDATE:
        /* A copy the relay drops (§7.3: "SHOULD attempt to deduplicate"), or
         * the permitted existing-to-missing update (§7.1 item 1), which the
         * relay MAY ignore: it keeps what it retains. */
        b->ingest_duplicates++;
        return dup;
    case MOQR_LOG_DUP_PENDING:
        return dup;          /* the caller compares the copy's chunks */
    case MOQR_LOG_DUP_DIFFERS:
        b->ingest_refusals++;
        bind_track_malformed(b, cn, sub, pub, now_us);
        return dup;
    default:
        break;
    }
    b->ingest_refusals++;
    if (!d->datagram_pref) {
        (void)moqr_core_internal_note_gap(b->core, track, d->group_id, d->subgroup_id);
    }
    return dup;
}

static uint32_t
bind_conn_slot(const moqr_bind_t *b, const b_conn_t *cn)
{
    return (uint32_t)(cn - b->conns);
}

/* The owner comparing the copy a chunk belongs to, or NULL. */
static b_dup_owner_t *
bind_dup_owner_find(moqr_bind_t *b, const b_conn_t *cn, uint64_t src,
                    const moq_object_chunk_event_t *oc)
{
    for (uint32_t i = 0; i < BIND_DUP_OWNERS; i++) {
        b_dup_owner_t *o = &b->dup_owners[i];
        if (o->used && o->conn == bind_conn_slot(b, cn) && o->src == src &&
            o->group == oc->group_id && o->subgroup == oc->subgroup_id &&
            o->object == oc->object_id) {
            return o;
        }
    }
    return NULL;
}

/* Compare one chunk of a copy. The first byte that differs from the retained
 * Object's is conclusive: whatever the rest of the copy holds, its payload
 * differs, so the Track is malformed at once (§7.1, §12.1) and the peer need
 * not finish the copy. The owner then stays only to consume the copy's later
 * chunks, which change nothing more. On the copy's terminal the owner is
 * released: every byte equal through a NORMAL end is an identical copy,
 * dropped; a copy that ended early (RESET or STOP) with no differing byte is
 * dropped too. The retained record is never touched. */
static void
bind_dup_owner_chunk(moqr_bind_t *b, b_conn_t *cn, b_dup_owner_t *o,
                     const moq_object_chunk_event_t *oc, uint64_t now_us)
{
    if (oc->chunk != NULL && moq_rcbuf_len(oc->chunk) > 0 && !o->differs) {
        size_t n = moq_rcbuf_len(oc->chunk);
        if (moqr_core_internal_duplicate_range(b->core, o->track, o->group,
                                               o->subgroup, o->object, o->offset,
                                               moq_rcbuf_data(oc->chunk), n) ==
            MOQR_LOG_RANGE_DIFFERS) {
            o->differs = true;
            b->ingest_refusals++;
            bind_track_malformed(b, cn, oc->sub, oc->pub, now_us);
        }
        o->offset += n;
    }
    if (!oc->end) {
        return;
    }
    o->used = false;
    if (!o->differs) {
        b->ingest_duplicates++;
    }
}

/* A connection or Track goes away: its copies' owners with it. */
static void
bind_dup_owners_release(moqr_bind_t *b, const b_conn_t *cn)
{
    for (uint32_t i = 0; i < BIND_DUP_OWNERS; i++) {
        if (b->dup_owners[i].used && b->dup_owners[i].conn == bind_conn_slot(b, cn)) {
            b->dup_owners[i].used = false;
        }
    }
}

/* The longest the relay holds a SUBSCRIBE waiting for a publisher
 * (§9.20.6 RENDEZVOUS_TIMEOUT), whatever the request asks: a private
 * resource ceiling, not a protocol value. */
#define BIND_RENDEZVOUS_MAX_MS UINT64_C(10000)

/* A draft-22 SUBSCRIBE with a positive RENDEZVOUS_TIMEOUT waits for a
 * publisher until one deadline, taken now, at the request's first receipt,
 * and never renewed. Absent or zero waits not at all. */
static bool
bind_sub_rendezvous(const b_conn_t *cn, const moq_event_t *ev, uint64_t now_us,
                    uint64_t *deadline_us)
{
    const moq_subscribe_request_event_t *sq = &ev->u.subscribe_request;
    *deadline_us = 0;
    if (cn->wire_draft != B_DRAFT_22 ||
        ev->detail_size < offsetof(moq_subscribe_request_event_t, options) +
                              sizeof(sq->options) ||
        sq->options == NULL) {
        return false;
    }
    const moq_subscription_admission_options_t *o = sq->options;
    if (o->struct_size <
            offsetof(moq_subscription_admission_options_t,
                     rendezvous_timeout_ms) +
                sizeof(o->rendezvous_timeout_ms) ||
        (o->flags & MOQ_SUBSCRIPTION_ADMISSION_RENDEZVOUS_TIMEOUT) == 0 ||
        o->rendezvous_timeout_ms == 0) {
        return false;
    }
    uint64_t ms = o->rendezvous_timeout_ms < BIND_RENDEZVOUS_MAX_MS
                      ? o->rendezvous_timeout_ms
                      : BIND_RENDEZVOUS_MAX_MS;
    uint64_t us = ms * 1000u;
    *deadline_us = now_us > UINT64_MAX - us ? UINT64_MAX : now_us + us;
    return true;
}

/* Retain a draft-22 namespace request's tokens, so an update that carries
 * none is authorized against them. A failure leaves none retained, which
 * can only narrow what a later update is authorized for. */
static void
bind_retain_request_tokens(moqr_bind_t *b, b_conn_t *cn, uint64_t handle,
                           const moqr_auth_token_t *tokens, size_t count)
{
    if (cn->wire_draft != B_DRAFT_22) {
        return;
    }
    (void)moqr_core_request_tokens_set(b->core, cn->binding, handle,
                                       count > 0 ? tokens : NULL, count);
}

static void
bind_retain_session_tokens(moqr_bind_t *b, b_conn_t *cn, uint64_t handle,
                           const moq_resolved_token_t *tokens, size_t count)
{
    if (count > MOQR_AUTH_MAX_TOKENS || (count > 0 && tokens == NULL)) {
        return;
    }
    moqr_auth_token_t mt[MOQR_AUTH_MAX_TOKENS];
    bind_tokens_to_relay(tokens, count, mt);
    bind_retain_request_tokens(b, cn, handle, mt, count);
}

/* The draft-22 SUBSCRIBE_TRACKS options, or NULL. */
static const moq_subscribe_tracks_options_t *
bind_tracks_options(const moq_event_t *ev)
{
    const moq_subscribe_tracks_request_event_t *sq =
        &ev->u.subscribe_tracks_request;
    if (ev->detail_size < offsetof(moq_subscribe_tracks_request_event_t,
                                   options) + sizeof(sq->options) ||
        sq->options == NULL ||
        sq->options->struct_size <
            offsetof(moq_subscribe_tracks_options_t, include_properties) +
                sizeof(sq->options->include_properties)) {
        return NULL;
    }
    return sq->options;
}

static void
bind_on_event(moqr_bind_t *b, b_conn_t *cn, moq_event_t *ev,
              uint64_t now_us)
{
    moq_bytes_t nsbuf[32];
    b->events_translated++;
    switch (ev->kind) {
    case MOQ_EVENT_NAMESPACE_PUBLISHED: {
        const moq_namespace_published_event_t *np =
            &ev->u.namespace_published;
        moqr_ns_t ns = ns_from_event(&np->track_namespace, nsbuf);
        uint64_t aerr = 0;
        uint64_t atkt = 0;
        uint64_t alease = 0;
        bind_auth_result_t aar =
            bind_authorize(b, cn, MOQR_AUTH_PUBLISH_NAMESPACE, ns,
                           (moq_bytes_t){ NULL, 0 }, np->tokens,
                           np->token_count, now_us, &aerr, &atkt, &alease);
        if (aar != BIND_AUTH_ALLOW) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_PUBLISH_NAMESPACE, ns,
                           (moq_bytes_t){ NULL, 0 }, np->ann._opaque);
            bind_refuse_or_park(b, cn, aar, &pr, np->tokens, np->token_count,
                                atkt, aerr, now_us);
            break;
        }
        /* ALLOW: if it carries a lease, reserve a revalidation grant BEFORE the
         * announce mutates routing state. A reserve failure fails the request
         * closed with no wire accept (CAT: a recipient that cannot track a
         * revalidatable grant rejects the request). */
        moqr_park_req_t gpr;
        bind_park_init(&gpr, cn, b, MOQR_AUTH_PUBLISH_NAMESPACE, ns,
                       (moq_bytes_t){ NULL, 0 }, np->ann._opaque);
        moqr_grant_res_t gres;
        if (bind_grant_reserve(b, &gpr, np->tokens, np->token_count, alease,
                               now_us, &gres) != MOQR_OK) {
            bind_refuse(b, cn, MOQR_AUTH_PUBLISH_NAMESPACE, np->ann._opaque,
                        MOQ_REQUEST_ERROR_UNAUTHORIZED, false, 0, now_us);
            break;
        }
        int ann_slot = ann_store(b, cn, np->ann._opaque, ns);
        if (ann_slot < 0) {
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            /* The announce table is a bounded pool: a capacity refusal. */
            bind_reject_admission(b, cn, MOQR_AUTH_PUBLISH_NAMESPACE,
                                  np->ann._opaque, MOQR_ERR_CAPACITY, now_us);
            break;
        }
        /* From here on the stored copy, never the event's views: the
         * announce may be retried after an intent drain, and executing an
         * intent on this session can reset its event memory. */
        moqr_ns_t ans = cn->anns[ann_slot].ns;
        /* The event's tokens, retained before the announce can drain
         * intents and reset the event memory they live in. */
        bind_retain_session_tokens(b, cn, np->ann._opaque, np->tokens,
                                   np->token_count);
        moqr_result_t arc;
        arc = bind_announce(b, cn, ans, np->ann._opaque, now_us);
        if (arc != MOQR_OK) {
            moqr_core_request_tokens_clear(b->core, cn->binding,
                                           np->ann._opaque);
            ann_clear_slot(b, cn, (uint32_t)ann_slot);
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            bind_reject_admission(b, cn, MOQR_AUTH_PUBLISH_NAMESPACE,
                                  np->ann._opaque, arc, now_us);
            break;
        }
        /* Announce succeeded. Commit the reservation (if any), undoing the
         * announce on the invariant commit failure. */
        if (bind_grant_commit_ann(b, cn, gres, ans, np->ann._opaque, now_us)) {
            bind_accept(b, cn, B_ACCEPT_NAMESPACE, np->ann._opaque, now_us);
        } else {
            moqr_core_request_tokens_clear(b->core, cn->binding,
                                           np->ann._opaque);
            ann_clear_slot(b, cn, (uint32_t)ann_slot);
        }
        break;
    }
    case MOQ_EVENT_NAMESPACE_DONE:
    case MOQ_EVENT_NAMESPACE_TERMINATED: {
        /* D22 retires the advertisement through a request-local outcome,
         * not the legacy DONE event. Only incoming advertisements own routes. */
        if (ev->kind == MOQ_EVENT_NAMESPACE_TERMINATED &&
            ev->u.namespace_terminated.local_role != MOQ_NAMESPACE_ROLE_RESPONDER) {
            break;
        }
        uint64_t raw = ev->kind == MOQ_EVENT_NAMESPACE_DONE
            ? ev->u.namespace_done.ann._opaque
            : ev->u.namespace_terminated.ann._opaque;
        moqr_core_request_tokens_clear(b->core, cn->binding, raw);
        if (bind_ann_withdraw(b, cn, raw, now_us) == MOQR_ERR_WOULD_BLOCK) {
            b->session_errors++;
        }
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_REQUEST: {
        const moq_subscribe_request_event_t *sq = &ev->u.subscribe_request;
        moqr_ns_t sns = ns_from_event(&sq->track_namespace, nsbuf);
        uint64_t rendezvous_deadline = 0;
        bool rendezvous = bind_sub_rendezvous(cn, ev, now_us,
                                              &rendezvous_deadline);
        uint64_t serr = 0;
        uint64_t stkt = 0;
        uint64_t slease = 0;
        bind_auth_result_t sar =
            bind_authorize(b, cn, MOQR_AUTH_SUBSCRIBE, sns, sq->track_name,
                           sq->tokens, sq->token_count, now_us, &serr, &stkt,
                           &slease);
        /* A failed filter observation is answered only after authorization;
         * a deferred request waits for its decision in bind_resume. */
        moqr_subscribe_req_t defaults;
        moqr_subscribe_req_init(&defaults);
        moqr_filter_t sfilter = defaults.filter;
        moq_location_filter_cfg_t location;
        bool semantic = cn->wire_draft == B_DRAFT_22;
        moqr_result_t frc = semantic
            ? bind_read_sub_location(cn, sq->sub, &location)
            : bind_read_sub_filter(cn, sq->sub, &sfilter);
        if (frc != MOQR_OK && sar == BIND_AUTH_ALLOW) {
            bind_reject_filter(b, cn, sq->sub, now_us);
            break;
        }
        if (sar != BIND_AUTH_ALLOW) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_SUBSCRIBE, sns,
                           sq->track_name, sq->sub._opaque);
            pr.sub_filter_type = frc == MOQR_OK ? sfilter.type
                                                : BIND_PARKED_FILTER_REFUSED;
            pr.sub_start_group = sfilter.start_group;
            pr.sub_start_object = sfilter.start_object;
            pr.sub_end_group_delta = sfilter.end_group_delta;
            pr.sub_priority = sq->subscriber_priority;
            pr.sub_group_order = sq->group_order;
            pr.sub_forward_off = !sq->forward;   /* resumed paused, as asked */
            pr.sub_rendezvous = rendezvous;
            pr.sub_rendezvous_deadline_us = rendezvous_deadline;
            bind_refuse_or_park(b, cn, sar, &pr, sq->tokens, sq->token_count,
                                stkt, serr, now_us);
            break;
        }
        /* ALLOW: reserve a revalidation grant BEFORE subscribing when the
         * verdict carries a lease. A reserve failure fails the request closed
         * with no wire OK (CAT: cannot track a revalidatable grant -> reject). */
        moqr_park_req_t gpr;
        bind_park_init(&gpr, cn, b, MOQR_AUTH_SUBSCRIBE, sns, sq->track_name,
                       sq->sub._opaque);
        moqr_grant_res_t gres;
        if (bind_grant_reserve(b, &gpr, sq->tokens, sq->token_count, slease,
                               now_us, &gres) != MOQR_OK) {
            bind_refuse(b, cn, MOQR_AUTH_SUBSCRIBE, sq->sub._opaque,
                        MOQ_REQUEST_ERROR_UNAUTHORIZED, false, 0, now_us);
            break;
        }
        moqr_subscribe_req_t req;
        moqr_subscribe_req_init(&req);
        moq_bytes_t sname = sq->track_name;
        bind_stash_ftn(b, &sns, &sname);
        req.ns = sns;
        req.name = sname;
        req.filter = sfilter;
        req.subscriber_priority = sq->subscriber_priority;
        req.group_order = (moqr_group_order_t)sq->group_order;
        req.cookie = sq->sub._opaque;
        req.forward = sq->forward;   /* carry initial Forward (a paused
                                      * subscription must not be scheduled) */
        req.range_end_holds = bind_range_end_holds(cn);
        req.rendezvous = rendezvous;
        req.rendezvous_deadline_us = rendezvous_deadline;
        if (semantic) req.location = &location;
        moqr_sub_t rsub;
        moqr_result_t src;
        BIND_CALL_RETRY(src, moqr_core_subscribe(b->core, cn->binding,
                                                 &req, &rsub));
        if (src == MOQR_ERR_CAPACITY && rendezvous) {
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            bind_reject_load(b, cn, sq->sub, now_us);
            break;
        }
        if (src != MOQR_OK) {
            if (gres.gen != 0) {
                moqr_core_grant_abort(b->core, gres);
            }
            bind_reject_admission(b, cn, MOQR_AUTH_SUBSCRIBE, sq->sub._opaque,
                                  src, now_us);
            break;
        }
        /* Subscribe succeeded; the ACCEPT_SUB flows through intents. Commit the
         * reservation with the new sub handle (revoking it on the invariant
         * commit failure). */
        bind_grant_commit_sub(b, gres, rsub, now_us);
        break;
    }
    case MOQ_EVENT_UNSUBSCRIBED: {
        uint64_t raw = ev->u.unsubscribed.sub._opaque;
        bool tracked = false;
        /* The relay's own upstream subscription, cancelled by its session
         * because the Track is malformed (§12.1): the downstream side ends
         * with MALFORMED_TRACK. */
        if (ev->detail_size >= offsetof(moq_unsubscribed_event_t, cause) +
                                   sizeof(ev->u.unsubscribed.cause) &&
            ev->u.unsubscribed.cause == MOQ_REQUEST_TERMINAL_MALFORMED_TRACK) {
            for (uint32_t i = 0; i < b->n_usubs; i++) {
                if (!cn->usubs[i].used || cn->usubs[i].ssub._opaque != raw) {
                    continue;
                }
                moqr_track_t track = cn->usubs[i].track;
                moqr_result_t mrc;
                b->malformed_tracks++;
                bind_reset_track_streams(b, track, now_us);
                BIND_CALL_RETRY(mrc, moqr_core_source_done(
                    b->core, track, cn->usubs[i].track_gen,
                    bind_local_pd(MOQR_PD_MALFORMED_TRACK), now_us));
                if (mrc != MOQR_OK && mrc != MOQR_ERR_STALE_HANDLE &&
                    mrc != MOQR_ERR_WRONG_STATE) {
                    b->session_errors++;
                }
                bind_fetches_malformed(b, track, now_us);
                usub_release(b, &cn->usubs[i]);
                tracked = true;
            }
        }
        /* The peer cancelled a request the relay serves: the session stops
         * its open streams itself, so the binding only retires its side. */
        for (uint32_t i = 0; i < b->n_dsubs; i++) {
            if (cn->dsubs[i].used && cn->dsubs[i].sub_raw == raw) {
                bind_dsub_retire(b, cn, &cn->dsubs[i], now_us);
                tracked = true;
            }
        }
        /* A draft-22 SUBSCRIBE withdrawn before its answer -- one held for
         * a publisher -- ends its wait too. */
        moqr_sub_t pending;
        if (!tracked && cn->wire_draft == B_DRAFT_22 &&
            moqr_core_sub_find(b->core, cn->binding, raw, &pending) ==
                MOQR_OK) {
            (void)moqr_core_unsubscribe(b->core, pending, now_us);
        }
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_OK: {
        uint64_t raw = ev->u.subscribe_ok.sub._opaque;
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (cn->usubs[i].used && cn->usubs[i].ssub._opaque == raw) {
                moqr_binding_t binding = cn->binding;
                moqr_track_t track = cn->usubs[i].track;
                uint64_t gen = cn->usubs[i].track_gen;
                moqr_bind_meta_ref_t input = bind_meta_input(
                    cn, raw, gen, ev->u.subscribe_ok.track_properties);
                moqr_result_t mrc = moqr_bind_meta_replace(
                    b, track, gen, &input, now_us, NULL);
                /* replace seals older intents and can detach a source. Byte
                 * exhaustion published UNAVAILABLE; this ACK may still make
                 * the core active, but each child will be refused locally. */
                if ((mrc != MOQR_OK && mrc != MOQR_ERR_CAPACITY &&
                     mrc != MOQR_ERR_NOMEM) || !cn->used || cn->closed ||
                    !moqr_binding_eq(cn->binding, binding) ||
                    !cn->usubs[i].used || cn->usubs[i].ssub._opaque != raw ||
                    !moqr_track_eq(cn->usubs[i].track, track) ||
                    cn->usubs[i].track_gen != gen) {
                    break;
                }
                moqr_result_t urc;
                BIND_CALL_RETRY(urc, moqr_core_upstream_ok(
                    b->core, track, gen,
                    raw, ev->u.subscribe_ok.has_largest,
                    ev->u.subscribe_ok.largest_group,
                    ev->u.subscribe_ok.largest_object));
                if (urc != MOQR_OK) {
                    b->session_errors++;   /* invariant makes this unreachable */
                }
            }
        }
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_ERROR: {
        uint64_t raw = ev->u.subscribe_error.sub._opaque;
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (cn->usubs[i].used && cn->usubs[i].ssub._opaque == raw) {
                moqr_result_t urc;
                BIND_CALL_RETRY(urc, moqr_core_upstream_error(
                    b->core, cn->usubs[i].track, cn->usubs[i].track_gen,
                    ev->u.subscribe_error.error_code, now_us));
                /* Only release the tracking slot once the core actually
                 * consumed the resolution (it retires the parked subs). */
                if (urc == MOQR_OK) {
                    usub_release(b, &cn->usubs[i]);
                } else {
                    b->session_errors++;
                }
            }
        }
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_UPDATED: {
        /* A downstream subscription update. Request-credit accounting already
         * counted it (bind_event_is_request). The relay answers it
         * (bind_update_answer) unless the session already did -- an update
         * it took before this binding switched it to deferral. Then what the
         * relay schedules on is applied after the fact: a replacement
         * Location Filter (copied from this event's resolved snapshot), then a
         * Forward change -- Forward=0 pauses object scheduling only (control
         * stays live), Forward=1 resumes. An update omitting a field leaves
         * that state unchanged. Priority / delivery-timeout / new-group
         * changes on this event are not applied here. */
        const moq_subscribe_updated_event_t *up = &ev->u.subscribe_updated;
        const moq_subscription_update_options_t *options =
            bind_update_options(ev);
        bool want_filter = bind_update_filter_applies(cn) && options != NULL &&
            (options->flags & MOQ_SUBSCRIPTION_UPDATE_OPTIONS_LOCATION) != 0;
        if (up->answer_owed) {
            bind_update_answer(b, cn, up, bind_update_filter_applies(cn), now_us);
            break;
        }
        if (!want_filter && !up->has_forward) {
            break;
        }
        uint64_t raw = up->sub._opaque;
        b_dsub_t *ds = NULL;
        for (uint32_t i = 0; i < b->n_dsubs; i++) {
            if (cn->dsubs[i].used && cn->dsubs[i].sub_raw == raw) {
                ds = &cn->dsubs[i];
                break;
            }
        }
        /* An update for a subscription the binding does not track is an
         * invariant break: fail closed (count it), mutate nothing. */
        if (ds == NULL) {
            b->session_errors++;
            break;
        }
        if (want_filter) {
            /* The session already acknowledged this selection. Failure to
             * apply it now requires PUBLISH_DONE UPDATE_FAILED; the SUB_DONE
             * intent releases its tracking slot. */
            moq_location_filter_cfg_t loc;
            moqr_result_t mrc = bind_update_resolved_location(options,
                up->update_request_id, &loc);
            if (mrc == MOQR_OK) {
                mrc = moqr_core_sub_set_location_filter(
                    b->core, ds->rsub, &loc, now_us);
            }
            moqr_result_t frc = mrc;
            if (mrc != MOQR_OK && mrc != MOQR_ERR_STALE_HANDLE) {
                moqr_pd_desc_t pd;
                if (moqr_pd_desc_local(MOQR_PD_UPDATE_FAILED, &pd) != MOQR_OK) {
                    b->session_errors++;
                    break;
                }
                BIND_CALL_RETRY(frc, moqr_core_revoke_sub(b->core, ds->rsub,
                                                          pd, now_us));
            }
            /* STALE = core already retired it (a terminal crossed): moot. Any
             * other non-OK is a broken invariant — fail closed via the counter,
             * never touch another slot. */
            if (frc != MOQR_OK && frc != MOQR_ERR_STALE_HANDLE) {
                b->session_errors++;
            }
            if (mrc != MOQR_OK) {
                break;   /* the subscription is ending: no Forward to apply */
            }
        }
        if (up->has_forward) {
            moqr_result_t frc = moqr_core_sub_set_forward(
                b->core, ds->rsub, up->forward, now_us);
            if (frc != MOQR_OK && frc != MOQR_ERR_STALE_HANDLE) {
                b->session_errors++;
            }
        }
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_DONE: {
        /* The origin finished the relay's upstream subscription. The session
         * surfaces this only once the done's Stream Count of data streams has
         * been processed (or its wait expired), so everything the origin sent
         * is ingested: each downstream subscriber receives what is still
         * owed to it and then its wire SUBSCRIBE_DONE, and the upstream is
         * released, so nothing hangs behind a sourceless ACTIVE track. The
         * finish queues no intent and cannot be refused for room, so the
         * tracking slot is always released (STALE/WRONG_STATE mean the core
         * already moved on). */
        uint64_t raw = ev->u.subscribe_done.sub._opaque;
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (!cn->usubs[i].used || cn->usubs[i].ssub._opaque != raw) {
                continue;
            }
            moqr_result_t drc = moqr_core_source_finish(
                b->core, cn->usubs[i].track, cn->usubs[i].track_gen,
                bind_upstream_pd(b, cn, ev->u.subscribe_done.status_code),
                now_us);
            if (drc != MOQR_OK && drc != MOQR_ERR_STALE_HANDLE &&
                drc != MOQR_ERR_WRONG_STATE) {
                b->session_errors++;
            }
            usub_release(b, &cn->usubs[i]);
        }
        break;
    }
    case MOQ_EVENT_REQUEST_REDIRECT: {
        /* The relay does not follow redirect URIs (a later routing feature);
         * the redirected upstream request is terminal. The Redirect's retry
         * interval is deliberately not read: there is no reissue schedule to
         * feed it to. */
        bind_upstream_terminated(b, cn, ev->u.request_redirect.family,
                                 ev->u.request_redirect.handle.raw,
                                 BIND_UP_END_REDIRECT, now_us);
        break;
    }
    case MOQ_EVENT_REQUEST_GOAWAY: {
        /* The relay does not migrate a request to a new session; treat the
         * per-request GOAWAY as terminal for the upstream subscription. */
        bind_upstream_terminated(b, cn, ev->u.request_goaway.family,
                                 ev->u.request_goaway.handle.raw,
                                 BIND_UP_END_GOAWAY, now_us);
        break;
    }
    case MOQ_EVENT_OBJECT_RECEIVED: {
        moq_object_received_event_t *ob = &ev->u.object_received;
        moqr_track_t track = bind_ingest_track(b, cn, ob->sub, ob->pub);
        if (!moqr_track_is_valid(track)) {
            break;   /* not relay ingest */
        }
        moqr_log_append_desc_t d;
        moqr_log_append_desc_init(&d);
        d.group_id = ob->group_id;
        d.subgroup_id = ob->subgroup_id;
        d.object_id = ob->object_id;
        d.publisher_priority = ob->publisher_priority;
        d.status = (moqr_obj_status_t)ob->status;
        d.datagram_pref = ob->datagram;
        d.end_of_group = ob->end_of_group;
        d.first_object = ob->first_object;
        d.subgroup_properties = ob->subgroup_properties;
        d.payload = ob->payload;        /* ownership transfers on OK */
        d.properties = ob->properties;
        d.now_us = now_us;
        moqr_result_t irc = moqr_core_ingest(b->core, track, &d);
        if (irc == MOQR_OK) {
            ob->payload = NULL;
            ob->properties = NULL;
            bind_note_relay_largest(b, track);
        } else {
            /* event cleanup releases the refs */
            bind_ingest_refused(b, cn, track, &d, irc, ob->sub, ob->pub, now_us);
        }
        break;
    }
    case MOQ_EVENT_OBJECT_CHUNK: {
        /* Streaming ingest: map a streamed NORMAL object onto the
         * OPEN-record machinery -- begin opens, payload slices append, end
         * completes (NORMAL) or abandons (RESET/STOP). Every chunk carries
         * (group, subgroup, object), so continuation chunks route correctly
         * even when concurrent subgroups interleave. Chunked COMPLETE records
         * deliver on the subscribe path, OPEN records at the live edge;
         * an abandon stores the terminal reset code (upstream RESET →
         * the publisher's code, else a relay-defined code) which the subscribe
         * path forwards downstream via reset_subgroup. Retained FETCH
         * still skips chunked records. */
        moq_object_chunk_event_t *oc = &ev->u.object_chunk;
        moqr_track_t track = bind_ingest_track(b, cn, oc->sub, oc->pub);
        if (!moqr_track_is_valid(track)) {
            break;   /* not relay ingest */
        }
        uint64_t src = moq_subscription_is_valid(oc->sub) ? oc->sub._opaque
                                                          : oc->pub._opaque;
        /* A chunk of a copy being compared: compared, never ingested. */
        b_dup_owner_t *dup_owner = bind_dup_owner_find(b, cn, src, oc);
        if (dup_owner != NULL) {
            bind_dup_owner_chunk(b, cn, dup_owner, oc, now_us);
            break;
        }
        /* Status objects (no payload) arrive as one begin+end chunk and are
         * whole-object records -- ingest via the COMPLETE path, which handles
         * EOG/EOT. Only NORMAL objects use the OPEN chunk machinery. */
        if (oc->begin && oc->status != MOQ_OBJECT_NORMAL) {
            moqr_log_append_desc_t d;
            moqr_log_append_desc_init(&d);
            d.group_id = oc->group_id;
            d.subgroup_id = oc->subgroup_id;
            d.object_id = oc->object_id;
            d.publisher_priority = oc->publisher_priority;
            d.status = (moqr_obj_status_t)oc->status;
            d.end_of_group = oc->end_of_group;
            d.first_object = oc->first_object;
            d.subgroup_properties = oc->subgroup_properties;
            d.payload = NULL;
            d.properties = oc->properties;   /* ownership transfers on OK */
            d.now_us = now_us;
            moqr_result_t irc = moqr_core_ingest(b->core, track, &d);
            if (irc == MOQR_OK) {
                oc->properties = NULL;
                bind_note_relay_largest(b, track);
            } else {
                bind_ingest_refused(b, cn, track, &d, irc, oc->sub, oc->pub,
                                    now_us);
            }
            break;
        }
        /* NORMAL object: begin opens the OPEN record, carrying the subgroup
         * header's EOG bit — subgroup metadata the log latches for the
         * deferred group-final at seal, and the record retains for the
         * downstream subgroup open. The subgroup itself still seals only via
         * SUBGROUP_FINISHED. */
        if (oc->begin) {
            moqr_log_append_desc_t d;
            moqr_log_append_desc_init(&d);
            d.group_id = oc->group_id;
            d.subgroup_id = oc->subgroup_id;
            d.object_id = oc->object_id;
            d.publisher_priority = oc->publisher_priority;
            d.status = MOQR_OBJ_NORMAL;
            d.end_of_group = oc->end_of_group;
            d.first_object = oc->first_object;
            d.subgroup_properties = oc->subgroup_properties;
            d.obj_state = MOQR_OBJ_OPEN;
            d.declared_len = oc->payload_length;
            d.properties = oc->properties;   /* ownership transfers on OK */
            d.now_us = now_us;
            moqr_result_t irc = moqr_core_ingest(b->core, track, &d);
            if (irc == MOQR_OK) {
                oc->properties = NULL;
                bind_note_relay_largest(b, track);
                /* The record's chunks are this source's only. */
                (void)moqr_core_internal_set_opener(b->core, track, oc->group_id,
                                                    oc->subgroup_id, oc->object_id,
                                                    bind_conn_slot(b, cn), src);
            } else {
                uint32_t dup = bind_ingest_refused(b, cn, track, &d, irc, oc->sub,
                                                   oc->pub, now_us);
                if (dup == MOQR_LOG_DUP_PENDING) {
                    /* The copy is compared chunk by chunk from here on. */
                    for (uint32_t i = 0; i < BIND_DUP_OWNERS; i++) {
                        b_dup_owner_t *o = &b->dup_owners[i];
                        if (o->used) continue;
                        *o = (b_dup_owner_t){ .used = true,
                                              .conn = bind_conn_slot(b, cn),
                                              .src = src, .track = track,
                                              .group = oc->group_id,
                                              .subgroup = oc->subgroup_id,
                                              .object = oc->object_id };
                        bind_dup_owner_chunk(b, cn, o, oc, now_us);
                        break;
                    }
                }
                break;   /* open failed: no record to abandon */
            }
        }
        /* A copy whose begin found no free comparison owner is ignored
         * uncompared (§7.1 lets a relay ignore a later copy); its chunks are
         * never appended to, nor complete or abandon, a record still arriving
         * from another source. */
        if (!oc->begin &&
            moqr_core_internal_open_by_other(b->core, track, oc->group_id,
                                             oc->subgroup_id, oc->object_id,
                                             bind_conn_slot(b, cn), src)) {
            if (oc->end) b->ingest_duplicates++;
            break;
        }
        /* Payload slice (begin/continuation/end may each carry one). The log
         * increfs the slice; the event keeps its ref (freed by event cleanup).
         * Fail-closed: an append failure abandons the record, no half-state. */
        if (oc->chunk != NULL) {
            if (moqr_core_append_chunk(b->core, track, oc->group_id,
                                       oc->subgroup_id, oc->object_id,
                                       oc->chunk) != MOQR_OK) {
                moqr_reset_desc_t stopd;
                if (moqr_reset_desc_internal(MOQR_RESET_CODE_STOPPED,
                                             &stopd) == MOQR_OK) {
                    (void)moqr_core_abandon_record(b->core, track,
                                                   oc->group_id,
                                                   oc->subgroup_id,
                                                   oc->object_id, stopd);
                }
                b->ingest_refusals++;
                break;
            }
        }
        /* End: complete (NORMAL) or abandon (RESET/STOP). A failed complete
         * (e.g. bytes != declared) also abandons rather than leave OPEN. The
         * abandon stores the terminal cause code: a genuine upstream RESET
         * forwards the publisher's oc->error_code; STOP / a relay-initiated
         * abandon uses a relay-defined code. Downstream delivery
         * propagates it. */
        if (oc->end) {
            moqr_result_t erc = MOQR_OK;
            if (oc->terminal == MOQ_OBJECT_TERMINAL_NORMAL) {
                erc = moqr_core_complete_record(b->core, track, oc->group_id,
                                                oc->subgroup_id, oc->object_id);
            }
            if (oc->terminal != MOQ_OBJECT_TERMINAL_NORMAL || erc != MOQR_OK) {
                moqr_reset_desc_t rdesc;
                bool have_rdesc;
                if (oc->terminal == MOQ_OBJECT_TERMINAL_RESET) {
                    rdesc = bind_upstream_reset(b, cn, oc->error_code);
                    have_rdesc = rdesc.tag != MOQR_CODE_NONE;
                } else {
                    have_rdesc = moqr_reset_desc_internal(
                                     MOQR_RESET_CODE_STOPPED, &rdesc) ==
                                 MOQR_OK;
                }
                if (have_rdesc) {
                    (void)moqr_core_abandon_record(b->core, track,
                                                   oc->group_id,
                                                   oc->subgroup_id,
                                                   oc->object_id, rdesc);
                }
                if (erc != MOQR_OK) {
                    b->ingest_refusals++;
                }
            }
        }
        break;
    }
#ifdef MOQ_EVENT_SUBGROUP_RESET
    case MOQ_EVENT_SUBGROUP_RESET: {
        /* The upstream subgroup stream RESET in whole-object receive mode:
         * the session dropped the partial object (no OBJECT_RECEIVED for
         * it), so this event is the only ingest-side evidence the stream
         * ended. Terminate the (group, subgroup) ledger abnormally with the
         * publisher's code — retained COMPLETE records stay servable and
         * each subscription's downstream subgroup is reset (never FIN'd)
         * once it has consumed them. Streaming receive mode surfaces resets
         * as terminal OBJECT_CHUNKs and never reaches here. */
        moq_subgroup_reset_event_t *sr = &ev->u.subgroup_reset;
        moqr_track_t track = bind_ingest_track(b, cn, sr->sub, sr->pub);
        if (moqr_track_is_valid(track)) {
            moqr_reset_desc_t srd = bind_upstream_reset(b, cn,
                                                        sr->error_code);
            if (srd.tag != MOQR_CODE_NONE) {
                (void)moqr_core_reset_subgroup(b->core, track, sr->group_id,
                                               sr->subgroup_id, srd);
            }
        }
        break;
    }
#endif
    case MOQ_EVENT_SUBGROUP_FINISHED: {
        /* The upstream subgroup stream FIN'd (plain FIN or EOG). Seal the log's
         * (group, subgroup): each subscription is then owed one acknowledged
         * SEAL notice (records only carry subgroup_end as advisory metadata),
         * and the notice's close — the ONLY durable FIN path — retires the
         * downstream stream and reclaims the slot. Not emitted for RESET/STOP,
         * so a reset is never mistaken for a graceful finish. Ordering is safe:
         * every OBJECT_RECEIVED for this subgroup was polled and ingested before
         * this event, so the sealed list will not grow. */
        moq_subgroup_finished_event_t *sf = &ev->u.subgroup_finished;
        moqr_track_t track = bind_ingest_track(b, cn, sf->sub, sf->pub);
        if (moqr_track_is_valid(track)) {
            moqr_result_t src = moqr_core_seal_subgroup(
                b->core, track, sf->group_id, sf->subgroup_id);
            if (src == MOQR_ERR_INVAL) {
                /* Malformed final evidence: this header-EOG subgroup's last
                 * object contradicts an already-retained higher object in
                 * the group. The cache must never declare a final below
                 * retained content — fail the publisher closed rather than
                 * discard the contradiction. */
                b->session_errors++;
                (void)moq_session_close(cn->session,
                                        BIND_SESSION_INTERNAL_ERROR,
                                        "eog final conflict", now_us);
                conn_detach(b, cn, now_us);
                break;
            }
        }
        break;
    }
    case MOQ_EVENT_PUBLISH_FINISHED: {
        /* A push publisher finished: same "source ended" contract as an upstream
         * SUBSCRIBE_DONE — terminate the track's downstream subscribers with a
         * wire SUBSCRIBE_DONE (forwarding the finish status) and release the
         * source, so no open-ended subscriber hangs behind a WARM track. */
        uint64_t raw = ev->u.publish_finished.pub._opaque;
        b_meta_publish_t *p = bind_meta_pub_find(b, cn, raw);
        if (p != NULL) bind_meta_pub_release(b, p);
        for (uint32_t i = 0; i < b->n_pubs; i++) {
            if (!cn->pubs[i].used || cn->pubs[i].pub_raw != raw) {
                continue;
            }
            /* Cancelled by the relay's session because the Track is
             * malformed (§12.1): downstream ends with MALFORMED_TRACK. */
            bool malformed =
                ev->detail_size >= offsetof(moq_publish_finished_event_t, cause) +
                                       sizeof(ev->u.publish_finished.cause) &&
                ev->u.publish_finished.cause == MOQ_REQUEST_TERMINAL_MALFORMED_TRACK;
            if (malformed) {
                b->malformed_tracks++;
                bind_reset_track_streams(b, cn->pubs[i].track, now_us);
                bind_fetches_malformed(b, cn->pubs[i].track, now_us);
            }
            /* A malformed track ends every subscriber at once; a publisher's
             * ordinary finish lets each one receive what is owed first (it
             * queues no intent and cannot be refused for room). */
            moqr_result_t lrc;
            if (malformed) {
                BIND_CALL_RETRY(lrc, moqr_core_source_done(
                    b->core, cn->pubs[i].track, cn->pubs[i].track_gen,
                    bind_local_pd(MOQR_PD_MALFORMED_TRACK), now_us));
                if (lrc == MOQR_ERR_WOULD_BLOCK) {
                    /* The ring could not take the terminals even after a
                     * drain: end the source now and let each subscriber's
                     * next delivery scan send its terminal, so the source
                     * this slot owns is never left ACTIVE without it. */
                    lrc = moqr_core_internal_source_abort(
                        b->core, cn->pubs[i].track, cn->pubs[i].track_gen,
                        bind_local_pd(MOQR_PD_MALFORMED_TRACK));
                }
            } else {
                lrc = moqr_core_source_finish(
                    b->core, cn->pubs[i].track, cn->pubs[i].track_gen,
                    bind_upstream_pd(b, cn,
                                     ev->u.publish_finished.status_code),
                    now_us);
            }
            if (lrc != MOQR_OK && lrc != MOQR_ERR_STALE_HANDLE &&
                lrc != MOQR_ERR_WRONG_STATE) {
                b->session_errors++;
            }
            cn->pubs[i].used = false;
        }
        break;
    }
    case MOQ_EVENT_NS_SUB_REQUEST: {
        const moq_ns_sub_request_event_t *nq = &ev->u.ns_sub_request;
        moqr_ns_t prefix =
            ns_from_event(&nq->track_namespace_prefix, nsbuf);
        uint64_t nerr = 0;
        uint64_t ntkt = 0;
        bind_auth_result_t nar =
            bind_authorize(b, cn, MOQR_AUTH_SUBSCRIBE_NAMESPACE, prefix,
                           (moq_bytes_t){ NULL, 0 }, nq->tokens,
                           nq->token_count, now_us, &nerr, &ntkt, NULL);
        if (nar != BIND_AUTH_ALLOW) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_SUBSCRIBE_NAMESPACE, prefix,
                           (moq_bytes_t){ NULL, 0 }, nq->handle._opaque);
            bind_refuse_or_park(b, cn, nar, &pr, nq->tokens, nq->token_count,
                                ntkt, nerr, now_us);
            break;
        }
        moqr_result_t nrc;
        bind_stash_ftn(b, &prefix, NULL);
        BIND_CALL_RETRY(nrc, moqr_core_ns_subscribe(
            b->core, cn->binding, prefix, nq->handle._opaque));
        if (nrc == MOQR_OK) {
            bind_retain_session_tokens(b, cn, nq->handle._opaque,
                                       nq->tokens, nq->token_count);
            bind_accept(b, cn, B_ACCEPT_NS_SUB, nq->handle._opaque, now_us);
        } else {
            bind_reject_admission(b, cn, MOQR_AUTH_SUBSCRIBE_NAMESPACE,
                                  nq->handle._opaque, nrc, now_us);
        }
        break;
    }
    case MOQ_EVENT_TRACK_STATUS_REQUEST: {
        const moq_track_status_request_event_t *tq =
            &ev->u.track_status_request;
        moqr_ns_t ns = ns_from_event(&tq->track_namespace, nsbuf);
        uint64_t terr = 0;
        uint64_t ttkt = 0;
        bind_auth_result_t tar =
            bind_authorize(b, cn, MOQR_AUTH_TRACK_STATUS, ns, tq->track_name,
                           tq->tokens, tq->token_count, now_us, &terr, &ttkt,
                           NULL);
        if (tar != BIND_AUTH_ALLOW) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_TRACK_STATUS, ns,
                           tq->track_name, tq->handle._opaque);
            bind_refuse_or_park(b, cn, tar, &pr, tq->tokens, tq->token_count,
                                ttkt, terr, now_us);
            break;
        }
        moqr_result_t trc;
        moq_bytes_t tname = tq->track_name;
        bind_stash_ftn(b, &ns, &tname);
        BIND_CALL_RETRY(trc, moqr_core_track_status(
            b->core, cn->binding, ns, tname, tq->handle._opaque));
        if (trc != MOQR_OK) {
            /* A ring-full status request would otherwise leave the peer
             * waiting forever; the invariant + drain makes trc==OK, but
             * if it ever weren't, reject on the wire rather than drop. */
            bind_refuse(b, cn, MOQR_AUTH_TRACK_STATUS, tq->handle._opaque,
                        MOQ_REQUEST_ERROR_INTERNAL_ERROR, false, 0, now_us);
        }
        break;
    }
    case MOQ_EVENT_PUBLISH_REQUEST: {
        const moq_publish_request_event_t *pq = &ev->u.publish_request;
        moqr_ns_t ns = ns_from_event(&pq->track_namespace, nsbuf);
        b_meta_publish_t *p = bind_meta_pub_reserve(b, cn, pq->pub._opaque);
        if (p == NULL) {
            b->session_errors++;   /* headroom invariant, no consumed orphan */
            break;
        }
        uint64_t perr = 0;
        uint64_t ptkt = 0;
        bind_auth_result_t par =
            bind_authorize(b, cn, MOQR_AUTH_PUBLISH, ns, pq->track_name,
                           pq->tokens, pq->token_count, now_us, &perr, &ptkt,
                           NULL);
        if (par != BIND_AUTH_ALLOW && par != BIND_AUTH_DEFER) {
            bind_meta_pub_refuse(b, cn, p, perr != 0 ? perr :
                                  MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
            break;
        }
        moqr_bind_meta_ref_t input = bind_meta_input(
            cn, pq->pub._opaque, 0, pq->track_properties);
        if (bind_meta_copy(b, &input, &p->snapshot) != MOQR_OK) {
            bind_meta_pub_refuse(b, cn, p, MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
            break;
        }
        if (par != BIND_AUTH_ALLOW) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_PUBLISH, ns, pq->track_name,
                           pq->pub._opaque);
            p->state = B_META_PUB_AUTH;
            p->ticket = ptkt;
            if (bind_park(b, &pr, pq->tokens, pq->token_count, ptkt, now_us) !=
                MOQR_OK) {
                bind_meta_pub_refuse(b, cn, p, MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
            }
            break;
        }
        moqr_track_t track;
        moqr_result_t prc;
        moq_bytes_t pname = pq->track_name;
        bind_stash_ftn(b, &ns, &pname);
        BIND_CALL_RETRY(prc, moqr_core_publish_open(
            b->core, cn->binding, ns, pname, pq->pub._opaque, &track));
        if (prc != MOQR_OK) {
            /* A drain can retire this source; do not resurrect its stage. */
            p = bind_meta_pub_find(b, cn, pq->pub._opaque);
            if (p != NULL) {
                if (prc == MOQR_ERR_CAPACITY) {
                    bind_meta_pub_refuse_ex(b, cn, p, bind_capacity_error(cn),
                                            true,
                                            bind_capacity_retry_ms(pq->pub._opaque),
                                            now_us);
                } else {
                    bind_meta_pub_refuse(b, cn, p, MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
                }
            }
        }
        break;
    }
    case MOQ_EVENT_FETCH_OK:
    case MOQ_EVENT_FETCH_OBJECT:
    case MOQ_EVENT_FETCH_GAP:
    case MOQ_EVENT_FETCH_COMPLETE:
    case MOQ_EVENT_FETCH_ERROR:
    case MOQ_EVENT_FETCH_RESET:
        /* This relay as the FETCHER of a forwarded response. An event that
         * names no live transaction is left to the pump's event cleanup. */
        (void)fwd_on_up_event(b, cn, ev, now_us);
        break;
    case MOQ_EVENT_FETCH_CANCELLED:
        if (!bind_refusal_drop(b, cn, ev->u.fetch_cancelled.fetch._opaque) &&
            !bind_standalone_cancelled(b, cn, ev->u.fetch_cancelled.fetch)) {
            (void)fwd_on_down_cancelled(b, cn, ev->u.fetch_cancelled.fetch._opaque);
        }
        break;
    case MOQ_EVENT_FETCH_REQUEST: {
        const moq_fetch_request_event_t *fq = &ev->u.fetch_request;
        moqr_ns_t fns = ns_from_event(&fq->track_namespace, nsbuf);
        /* Authorize FIRST so unsupported fetch shapes are never a pre-auth
         * support oracle. A DEFER also fails closed (no parked fetch yet). */
        uint64_t ferr = 0;
        uint64_t ftkt = 0;
        bind_auth_result_t far =
            bind_authorize(b, cn, MOQR_AUTH_FETCH, fns, fq->track_name,
                           fq->tokens, fq->token_count, now_us, &ferr, &ftkt,
                           NULL);
        if (far != BIND_AUTH_ALLOW) {
            bind_fetch_refuse(b, cn, fq->fetch,
                              ferr != 0 ? ferr : MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
            break;
        }
        /* Then reject unsupported shapes: a join of a PUBLISH-initiated
         * subscription, and descending order except for a draft-22
         * standalone FETCH. */
        if (fq->joining_pub._opaque != 0 ||
            (fq->group_order == MOQ_GROUP_ORDER_DESCENDING &&
             (cn->wire_draft != B_DRAFT_22 || fq->joining_sub._opaque != 0))) {
            bind_fetch_refuse(b, cn, fq->fetch, MOQ_REQUEST_ERROR_NOT_SUPPORTED, now_us);
            break;
        }
        /* An ascending join of a SUBSCRIBE-initiated subscription is forwarded
         * upstream as a bounded transaction (its identity is resolved and
         * authorized there; the empty-identity authorization above cannot
         * admit a join by itself). */
        if (fq->joining_sub._opaque != 0) {
            fwd_admit(b, cn, fq, now_us);
            break;
        }
        /* Plan against the core retained cursor. A range stated relative to
         * the Largest Object resolves against this relay's own Largest, in
         * the same call that plans it. */
        const moq_fetch_request_options_t *fo = bind_fetch_options(ev);
        moqr_fetch_req_t freq;
        moqr_fetch_req_init(&freq);
        freq.ns = fns;
        freq.name = fq->track_name;
        freq.start_group = fq->start_group;
        freq.start_object = fq->start_object;
        freq.end_group = fq->end_group;
        freq.end_object = fq->end_object;
        freq.group_order = (moqr_group_order_t)fq->group_order;
        freq.subscriber_priority = fq->subscriber_priority;
        freq.cookie = fq->fetch._opaque;
        /* Every Location of the response is served or marked: the log's
         * holes and the runs it has no evidence about come back as items
         * (bind_fetch_pump marks them). */
        freq.report_gaps = true;
        freq.report_unknown = true;
        /* An Object still arriving is waited for, up to the deadline. */
        freq.report_pending = true;
        bind_fetch_range(fo, &freq);
        if (cn->wire_draft == B_DRAFT_22) {
            /* Served from cache and, for what it does not hold, upstream
             * (draft-22 3.2.3). */
            bind_standalone_request(b, cn, fq, &freq, fo, now_us);
            break;
        }
        moqr_fetch_t cfetch;
        moqr_fetch_plan_t plan;
        moqr_result_t forc = moqr_core_fetch_open(b->core, cn->binding, &freq,
                                                  now_us, &cfetch, &plan);
        if (forc != MOQR_OK) {
            bind_reject_admission(b, cn, MOQR_AUTH_FETCH, fq->fetch._opaque,
                                  forc, now_us);
            break;
        }
        if (plan.admit == MOQR_FETCH_REJECT) {
            /* DOES_NOT_EXIST / INVALID_RANGE */
            bind_fetch_refuse(b, cn, fq->fetch, plan.error_code, now_us);
            break;
        }
        /* ACCEPT: park it in the conn fetch table; bind_fetch_pump drives
         * accept_fetch -> stream objects -> end_fetch on the network thread. */
        int fi = -1;
        for (uint32_t i = 0; i < b->n_fetches; i++) {
            if (!cn->fetches[i].used) {
                fi = (int)i;
                break;
            }
        }
        if (fi < 0) {   /* impossible: the bind table matches the core pool */
            (void)moqr_core_fetch_close(b->core, cfetch);
            bind_fetch_refuse(b, cn, fq->fetch,
                              MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
            break;
        }
        b_fetch_t *bf = &cn->fetches[fi];
        bf->used = true;
        bf->state = BF_NEEDS_OK;
        bf->sfetch = fq->fetch;
        bf->cfetch = cfetch;
        bf->eot = plan.end_of_track;
        bf->end_group = plan.end_group;
        bf->end_object = plan.end_object;
        bf->fill_deadline_us = bind_wait_deadline(
            b, now_us, fo != NULL && (fo->flags & MOQ_FETCH_REQUEST_HAS_FILL_TIMEOUT) != 0,
            fo != NULL ? fo->fill_timeout_ms : 0);
        bf->fetch_timed_out_ok = cn->wire_draft == B_DRAFT_22;
        bf->fetch_held_pending = false;
        break;
    }
    case MOQ_EVENT_FILL_REQUEST:
        bind_fill_request(b, cn, &ev->u.fill_request, now_us);
        break;
    case MOQ_EVENT_FILL_TERMINAL:
        bind_fill_terminal(b, cn, ev->u.fill_terminal.fill);
        break;
    case MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST: {
        const moq_subscribe_tracks_request_event_t *sq =
            &ev->u.subscribe_tracks_request;
        const moq_subscribe_tracks_options_t *to = bind_tracks_options(ev);
        if (cn->wire_draft != B_DRAFT_22 || to == NULL) {
            /* Track discovery is served on draft-22 only. */
            bind_refuse(b, cn, B_REFUSE_SUBSCRIBE_TRACKS, sq->handle._opaque,
                        MOQ_REQUEST_ERROR_NOT_SUPPORTED, false, 0, now_us);
            break;
        }
        /* §3.6: the subscriber must be authorized for this namespace
         * subscription; each track it discovers is authorized again. */
        moqr_ns_t prefix = ns_from_event(&sq->track_namespace_prefix, nsbuf);
        uint64_t terr = 0, ttkt = 0;
        bind_auth_result_t tar =
            bind_authorize(b, cn, MOQR_AUTH_SUBSCRIBE_NAMESPACE, prefix,
                           (moq_bytes_t){ NULL, 0 }, sq->tokens,
                           sq->token_count, now_us, &terr, &ttkt, NULL);
        if (tar != BIND_AUTH_ALLOW) {
            moqr_park_req_t pr;
            bind_park_init(&pr, cn, b, MOQR_AUTH_SUBSCRIBE_NAMESPACE, prefix,
                           (moq_bytes_t){ NULL, 0 }, sq->handle._opaque);
            pr.sub_forward_off = !sq->forward;
            pr.sub_group_order = (uint32_t)to->group_order;
            pr.sub_start_object = to->include_properties ? 1u : 0u;
            bind_refuse_or_park(b, cn, tar, &pr, sq->tokens, sq->token_count,
                                ttkt, terr, now_us);
            break;
        }
        if (sq->token_count > MOQR_AUTH_MAX_TOKENS) {
            bind_tracks_reject(b, cn, sq->handle._opaque,
                               MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
            break;
        }
        moqr_auth_token_t mt[MOQR_AUTH_MAX_TOKENS];
        bind_tokens_to_relay(sq->tokens, sq->token_count, mt);
        bind_tracks_admit(b, cn, sq->handle._opaque, prefix, sq->forward,
                          to->include_properties, (uint32_t)to->group_order,
                          sq->token_count > 0 ? mt : NULL, sq->token_count,
                          now_us);
        break;
    }
    case MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED: {
        /* Cancelling ends discovery only; subscriptions it created stay. */
        uint64_t h = ev->u.subscribe_tracks_cancelled.handle._opaque;
        bind_nsans_forget(b, cn, h);
        bind_nsu_retire(b, cn, B_NSU_TRACKS, h, now_us);
        break;
    }
    case MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED: {
        const moq_ns_subscription_terminated_event_t *nt =
            &ev->u.ns_subscription_terminated;
        if (cn->wire_draft != B_DRAFT_22 ||
            nt->local_role != MOQ_REQUEST_ROLE_RESPONDER) {
            break;
        }
        b_nsans_t *held = bind_nsans_find(b, cn, nt->handle._opaque);
        if (held != NULL) {
            bind_nsans_drop(b, held);
        }
        bind_nsu_retire(b, cn, B_NSU_NS, nt->handle._opaque, now_us);
        break;
    }
    case MOQ_EVENT_NAMESPACE_UPDATE_REQUEST:
        bind_nsu_authorize(b, cn, B_NSU_ANN,
                           ev->u.namespace_update_request.ann._opaque,
                           ev->u.namespace_update_request.update_request_id,
                           MOQR_AUTH_REQUEST_UPDATE, now_us);
        break;
    case MOQ_EVENT_NS_SUB_UPDATE_REQUEST:
        bind_nsu_authorize(b, cn, B_NSU_NS,
                           ev->u.ns_sub_update_request.handle._opaque,
                           ev->u.ns_sub_update_request.update_request_id,
                           MOQR_AUTH_REQUEST_UPDATE, now_us);
        break;
    case MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_REQUEST:
        bind_nsu_authorize(b, cn, B_NSU_TRACKS,
                           ev->u.subscribe_tracks_update_request.handle._opaque,
                           ev->u.subscribe_tracks_update_request.update_request_id,
                           MOQR_AUTH_REQUEST_UPDATE, now_us);
        break;
    case MOQ_EVENT_NAMESPACE_UPDATE_RESULT:
    case MOQ_EVENT_NS_SUB_UPDATE_RESULT:
    case MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_RESULT: {
        /* The session refused a peer update itself (a token failure,
         * PREFIX_OVERLAP or an unservable filter) and ended the request: the
         * relay stops serving it. The three result layouts are identical. */
        const moq_namespace_update_result_event_t *ur =
            &ev->u.namespace_update_result;
        if (ur->outcome != MOQ_NAMESPACE_UPDATE_OUTCOME_REFUSED) {
            break;
        }
        uint64_t h = ur->ann._opaque;
        bind_nsans_forget(b, cn, h);
        bind_nsu_retire(b, cn, bind_nsu_family(h), h, now_us);
        break;
    }
    case MOQ_EVENT_PUBLISH_OK:
        bind_publication_answered(b, cn, ev->u.publish_ok.pub._opaque, true,
                                  ev->u.publish_ok.send_allowed, now_us);
        break;
    case MOQ_EVENT_PUBLISH_ERROR:
        bind_publication_answered(b, cn, ev->u.publish_error.pub._opaque,
                                  false, false, now_us);
        break;
    case MOQ_EVENT_PUBLISH_UNSUBSCRIBED:
        bind_publication_answered(b, cn, ev->u.publish_unsubscribed.pub._opaque,
                                  false, false, now_us);
        break;
    case MOQ_EVENT_PUBLISH_UPDATED: {
        /* The subscriber's REQUEST_UPDATE following PUBLISH_OK on a
         * discovery publication: an ordinary subscription update (§3.6.2),
         * answered like one -- a selected Location Filter replaces the
         * relay subscription's, a fill it carries is admitted with the
         * acceptance and served like any fill, and Forward applies. */
        const moq_publish_updated_event_t *pu = &ev->u.publish_updated;
        bool owed = ev->detail_size >=
                        offsetof(moq_publish_updated_event_t, update_request_id) +
                            sizeof(pu->update_request_id) &&
                    pu->answer_owed;
        if (owed) {
            bind_update_answer_for(b, cn, pu->pub._opaque,
                                   pu->update_request_id, pu->has_forward,
                                   pu->forward, bind_update_filter_applies(cn),
                                   now_us);
            break;
        }
        b_dsub_t *ds = dsub_find(b, cn, pu->pub._opaque);
        if (ds == NULL || !ds->publication) {
            break;
        }
        const moq_subscription_update_options_t *options =
            ev->detail_size >= offsetof(moq_publish_updated_event_t, options) +
                                   sizeof(pu->options)
                ? pu->options : NULL;
        moq_location_filter_cfg_t loc;
        if (bind_update_filter_applies(cn) && options != NULL &&
            (options->flags & MOQ_SUBSCRIPTION_UPDATE_OPTIONS_LOCATION) &&
            bind_update_resolved_location(options, pu->update_request_id,
                                          &loc) == MOQR_OK) {
            moqr_result_t lrc = moqr_core_sub_set_location_filter(
                b->core, ds->rsub, &loc, now_us);
            if (lrc != MOQR_OK && lrc != MOQR_ERR_STALE_HANDLE) {
                b->session_errors++;
            }
        }
        if (pu->has_forward) {
            moqr_result_t frc = moqr_core_sub_set_forward(
                b->core, ds->rsub, pu->forward, now_us);
            if (frc != MOQR_OK && frc != MOQR_ERR_STALE_HANDLE) {
                b->session_errors++;
            }
        }
        break;
    }
    case MOQ_EVENT_SETUP_COMPLETE: {
        const moq_setup_complete_event_t *su = &ev->u.setup_complete;
        if (bind_authorize(b, cn, MOQR_AUTH_CLIENT_SETUP,
                           (moqr_ns_t){ NULL, 0 }, (moq_bytes_t){ NULL, 0 },
                           su->tokens, su->token_count, now_us, NULL,
                           NULL, NULL) != BIND_AUTH_ALLOW) {
            /* Setup DENY (and DEFER, which fails closed) hard-closes the
             * connection with the session-level UNAUTHORIZED code.
             * moq_session_close queues CLOSE_SESSION — the adapter drains it
             * to the peer — plus SESSION_CLOSED; the session stays
             * adapter-owned, so detaching the binding here is safe and the
             * close still reaches the wire. The reason is a static literal so
             * it outlives the queued action/event. */
            (void)moq_session_close(cn->session, BIND_SESSION_UNAUTHORIZED,
                                    "unauthorized", now_us);
            conn_detach(b, cn, now_us);
        }
        break;
    }
    case MOQ_EVENT_SESSION_CLOSED:
        conn_detach(b, cn, now_us);
        break;
    default:
        break;
    }
}

/* -- intent execution --------------------------------------------------------- */

static b_dsub_t *
dsub_find(const moqr_bind_t *b, b_conn_t *cn, uint64_t sub_raw)
{
    for (uint32_t i = 0; i < b->n_dsubs; i++) {
        if (cn->dsubs[i].used && cn->dsubs[i].sub_raw == sub_raw) {
            return &cn->dsubs[i];
        }
    }
    return NULL;
}

/* Whether the session has already ended this downstream request on its own
 * (an update it refused automatically ends it with UPDATE_FAILED and names
 * it in no event). Asked of the request itself -- the parent, by its exact
 * session handle -- through the non-advancing owed-update observer with no
 * outputs: STALE_HANDLE is the session's word that the request is over
 * (freed, or its terminal held behind resets it owes). OK (an answer owed)
 * and WRONG_STATE (none owed) are both a live request. A stream's stale
 * handle is never evidence: a single subgroup can end on its own (a delivery
 * timeout, the peer's STOP_SENDING) while its request lives on. */
static bool
bind_request_ended_by_session(const b_conn_t *cn, const b_dsub_t *ds)
{
    moq_result_t rc = ds->publication
        ? moq_session_publication_owed_update_sized(
              cn->session, (moq_publication_t){ ._opaque = ds->sub_raw },
              NULL, NULL, NULL, 0)
        : moq_session_subscription_owed_update_sized(
              cn->session, (moq_subscription_t){ ._opaque = ds->sub_raw },
              NULL, NULL, NULL, 0);
    return rc == MOQ_ERR_STALE_HANDLE;
}

static uint64_t
bind_stream_count_next(uint64_t count, uint8_t wire_draft)
{
    const uint64_t unknown = wire_draft == B_DRAFT_22
                                 ? UINT64_MAX : (UINT64_C(1) << 62) - 1;
    return count < unknown ? count + 1 : unknown;
}

static int
dsub_free_slot(const moqr_bind_t *b, const b_conn_t *cn)
{
    for (uint32_t i = 0; i < b->n_dsubs; i++) {
        if (!cn->dsubs[i].used) {
            return (int)i;
        }
    }
    return -1;
}

static int
usub_free_slot(const moqr_bind_t *b, const b_conn_t *cn)
{
    for (uint32_t i = 0; i < b->n_usubs; i++) {
        if (!cn->usubs[i].used) {
            return (int)i;
        }
    }
    return -1;
}

static int
pub_free_slot(const moqr_bind_t *b, const b_conn_t *cn)
{
    for (uint32_t i = 0; i < b->n_pubs; i++) {
        if (!cn->pubs[i].used) {
            return (int)i;
        }
    }
    return -1;
}

/* The ACK did not commit. Retire only its core subscription and preserve the
 * request-local refusal in the same FIFO if the session cannot take it yet.
 * Converting to REJECT_SUB also lets the caller release the old ACK pin. */
static bool
bind_refuse_unaccepted_sub(moqr_bind_t *b, b_conn_t *cn,
                           const moqr_intent_t *it, bool capacity,
                           uint64_t now_us, moqr_intent_t *defer)
{
    (void)moqr_core_unsubscribe(b->core, it->sub, now_us);
    moq_subscription_t ssub = { it->cookie };
    moq_reject_subscribe_cfg_t cfg;
    if (capacity) {
        /* The tracking table is a bounded pool: a capacity refusal. */
        bind_reject_subscribe_cfg_init(&cfg, bind_capacity_error(cn));
        cfg.can_retry = true;
        cfg.retry_after_ms = bind_capacity_retry_ms(it->cookie);
    } else {
        bind_reject_subscribe_cfg_init(&cfg, MOQ_REQUEST_ERROR_INTERNAL_ERROR);
    }
    moq_result_t rc = moq_session_reject_subscribe(cn->session, ssub, &cfg, now_us);
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        *defer = *it;
        defer->kind = MOQR_INTENT_REJECT_SUB;
        defer->error_code = capacity ? MOQR_REQUEST_ERROR_CAPACITY
                                     : MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        return false;
    }
    if (rc != MOQ_OK && !fwd_result_is_gone(rc)) b->session_errors++;
    return true;
}

static bool
bind_try_intent(moqr_bind_t *b, const moqr_intent_t *it, uint64_t now_us,
                uint8_t mode, bool *parked_out, moqr_intent_t *defer)
{
    /* Reserved-cookie intents belong to the shard manager's pseudo-bindings,
     * not a real session: hand them to the router. Without a router installed
     * (the single-shard default) this is skipped and the reserved cookie is
     * moot exactly as an unknown cookie is below. */
    if (b->router != NULL && it->binding_cookie >= b->router_cookie_base) {
        if (b->router(b->router_ctx, it, now_us)) {
            return true; /* the router consumed it */
        }
        if (bind_intent_deferrable(it->kind)) {
            *defer = *it; /* retry next pump, like a WOULD_BLOCKed write */
            return false;
        }
        return true; /* declined borrowed-view kind: moot, as an unrouted cookie */
    }
    b_conn_t *cn = conn_by_cookie(b, it->binding_cookie);
    if (cn == NULL || cn->closed) {
        return true; /* moot: the target session is gone or tearing down */
    }
    switch (it->kind) {
    case MOQR_INTENT_ACCEPT_SUB: {
        if (bind_cookie_is_publication(it->cookie)) {
            /* A discovery publication: the subscriber's PUBLISH_OK was the
             * acceptance, so nothing goes on the wire. */
            b_dsub_t *pds = dsub_find(b, cn, it->cookie);
            if (pds == NULL || !pds->publication) {
                (void)moqr_core_unsubscribe(b->core, it->sub, now_us);
            } else {
                pds->rsub = it->sub;
            }
            return true;
        }
        moq_subscription_t ssub = { it->cookie };
        const moqr_bind_meta_ref_t *pin = b->metadata_executing;
        /* Reserve the tracking slot BEFORE accepting on the wire: a sub
         * the binding cannot track must be refused, never orphaned. */
        int slot = dsub_free_slot(b, cn);
        if (slot < 0 || !bind_meta_can_replay(pin, moq_session_version(cn->session))) {
            /* An ownership/representation limit, not an unsupported request
             * type (D22 Section 12.3). */
            return bind_refuse_unaccepted_sub(b, cn, it, slot < 0, now_us,
                                              defer);
        }
        moq_accept_subscribe_cfg_t cfg;
        moq_accept_subscribe_cfg_init(&cfg);
        cfg.track_properties = pin->bytes;
        cfg.has_largest = it->has_largest;
        cfg.largest_group = it->largest_group;
        cfg.largest_object = it->largest_object;
        moq_result_t arc =
            moq_session_accept_subscribe(cn->session, ssub, &cfg, now_us);
        if (arc == MOQ_ERR_WOULD_BLOCK) {
            *defer = *it; /* slot NOT committed; the retry re-reserves */
            return false;
        }
        if (arc == MOQ_ERR_BUFFER) {
            /* Encoding/preflight could not fit the ACK, without committing
             * the session request. Do not abandon it, or classify unrelated
             * stale/closed/invalid failures as metadata capacity. */
            return bind_refuse_unaccepted_sub(b, cn, it, false, now_us, defer);
        }
        if (arc != MOQ_OK) {
            (void)moqr_core_unsubscribe(b->core, it->sub, now_us);
            b->session_errors++;
            return true;
        }
        memset(&cn->dsubs[slot], 0, sizeof(cn->dsubs[slot]));
        cn->dsubs[slot].used = true;
        cn->dsubs[slot].sub_raw = it->cookie;
        cn->dsubs[slot].rsub = it->sub;
        return true;
    }
    case MOQR_INTENT_REJECT_SUB: {
        if (bind_cookie_is_publication(it->cookie)) {
            /* The track a discovery publication offered has no source any
             * more: the publication ends as its track did. */
            moqr_intent_t done = *it;
            done.kind = MOQR_INTENT_SUB_DONE;
            done.pd = bind_local_pd(MOQR_PD_TRACK_ENDED);
            return bind_try_intent(b, &done, now_us, mode, parked_out, defer);
        }
        moq_subscription_t ssub = { it->cookie };
        moq_reject_subscribe_cfg_t cfg;
        if (it->error_code == MOQR_REQUEST_ERROR_CAPACITY) {
            bind_reject_subscribe_cfg_init(&cfg, bind_capacity_error(cn));
            cfg.can_retry = true;
            cfg.retry_after_ms = bind_capacity_retry_ms(it->cookie);
        } else {
            bind_reject_subscribe_cfg_init(&cfg, it->error_code);
        }
        if (moq_session_reject_subscribe(cn->session, ssub, &cfg, now_us) ==
            MOQ_ERR_WOULD_BLOCK) {
            *defer = *it;
            return false;
        }
        return true;
    }
    case MOQR_INTENT_SUB_DONE: {
        /* An update answer owed on the subscription goes first (the session
         * refuses the terminal while one is owed). A refusal given now is
         * itself the subscription's terminal (UPDATE_FAILED), which makes
         * this one moot. */
        b_upd_give_t settle = bind_update_answer_settle(b, cn, it->cookie,
                                                        now_us);
        if (settle == B_UPD_HELD) {
            *defer = *it;
            return false;
        }
        if (settle == B_UPD_ENDED) {
            return true;
        }
        b_dsub_t *ds = dsub_find(b, cn, it->cookie);
        if (ds == NULL) {
            /* Only a successful ACCEPT creates this owner. An unaccepted
             * or already-retired request has no binding-owned completion;
             * never send a guessed zero count for an ownerless intent. */
            return true;
        }
        if (bind_request_ended_by_session(cn, ds)) {
            /* The session already ended this request (and owes or sent its
             * own terminal): retire the binding's side only. */
            bind_dsub_retire(b, cn, ds, now_us);
            return true;
        }
        /* Close any open subgroup streams for this sub, THEN send the
         * terminal. A stream still open here never received its close: the
         * core delivers a subgroup's FIN (or upstream reset) as a seal notice
         * before the subscription's terminal, and that close frees the slot.
         * So nothing proves the subgroup was delivered whole -- its upstream
         * may never have been sealed (the done wait expired) or the
         * subscription ended early -- and it is reset, never FINed (draft-18
         * section 11.4.3, draft-22 section 11.3.2: a sender that closes the
         * stream before delivering all of its objects MUST reset it). Defer
         * the whole SUB_DONE if any write blocks: tracking is left intact so
         * the retry re-closes (already-closed slots are cleared, so it is
         * idempotent) and re-sends DONE. Downstream tracking is torn down
         * only once the terminal is actually on the wire. */
        for (uint32_t i = 0; i < b->n_sgs; i++) {
            if (!cn->sgs[i].used || cn->sgs[i].sub_raw != it->cookie) {
                continue;
            }
            moq_result_t crc;
#ifdef MOQR_BIND_TESTING
            if (atomic_load(&bind_dbg_block_sg_resets) > 0) {
                /* Injected WOULD_BLOCK before the session is called: the
                 * reset is genuinely unsent. */
                (void)atomic_fetch_sub(&bind_dbg_block_sg_resets, 1);
                *defer = *it;
                return false;
            }
#endif
            if (cn->sgs[i].reset_owed) {
                crc = moq_session_reset_subgroup(cn->session, cn->sgs[i].h,
                                                 cn->sgs[i].reset_wire, now_us);
            } else {
                /* Reset with the reset registry's CANCELLED (draft-18 section
                 * 3.3.3, draft-22 section 12.5: the stream was cancelled, and
                 * for a subscription PUBLISH_DONE carries the detailed status)
                 * -- the SUB_DONE's status code lives in the PUBLISH_DONE
                 * registry and never doubles as a reset code. This is also the
                 * only terminal a begun object can take: close_subgroup
                 * returns WRONG_STATE on a streaming subgroup. */
#ifdef MOQR_BIND_TESTING
                if (atomic_load(&bind_dbg_fail_sg_resets) > 0 &&
                    atomic_fetch_sub(&bind_dbg_fail_sg_resets, 1) == 1) {
                    /* Injected hard failure: the session is NOT called, so
                     * the production fail-closed path runs unchanged. */
                    crc = MOQ_ERR_WRONG_STATE;
                } else
#endif
                crc = moq_session_reset_subgroup(cn->session, cn->sgs[i].h,
                                                 BIND_RESET_CANCELLED,
                                                 now_us);
            }
            if (crc == MOQ_ERR_WOULD_BLOCK) {
                *defer = *it;
                return false;
            }
            /* Every one of these resets is owed: a refusal leaves the stream
             * without its terminal, so fail closed rather than strand it and
             * release the subscription. The one tolerated result is
             * STALE_HANDLE, which the session returns only when it already
             * owns this stream's termination: either the entry was freed
             * (its generation moved on: the stream ended and was reaped), or
             * the same generation is no longer OPEN or STREAMING -- CLOSING
             * (its FIN is queued), RESETTING (a reset is queued, e.g. the
             * session's delivery timeout or its answer to the peer's
             * STOP_SENDING) or RESET_OWED (a teardown's reset the session
             * itself issues once its action queue has room). The binding is
             * not told of those session-side terminations, so its slot can
             * outlive them; there is nothing left for it to send. */
            if (crc != MOQ_OK && crc != MOQ_ERR_STALE_HANDLE) {
                b->session_errors++;
                (void)moq_session_close(cn->session,
                                        BIND_SESSION_INTERNAL_ERROR,
                                        "sub_done reset", now_us);
                conn_detach(b, cn, now_us);
                return true;
            }
            cn->sgs[i].used = false;
            cn->sgs[i].obj_begun = false;
            cn->sgs[i].reset_owed = false;
            bind_dl_rearm_on_slot_release(b, cn);   /* freed slot: unblock a waiting delivery */
        }
        moq_subscription_t ssub = { it->cookie };
        moq_done_subscribe_cfg_t cfg;
        moq_done_subscribe_cfg_init(&cfg);
        cfg.stream_count = ds->streams_opened;
        /* The retained terminal is encoded for the draft this subscriber
         * reads — the single translation point for the PUBLISH_DONE domain. */
        uint64_t pd_wire = 0;
        if (moqr_pd_desc_emit(it->pd, moqr_bind_conn_version(b, cn->session),
                              &pd_wire) != MOQR_OK) {
            (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                    "untranslatable terminal", now_us);
            conn_detach(b, cn, now_us);
            return false;
        }
        cfg.status_code = pd_wire;   /* 64-bit: an extension may exceed 32 bits */
        moq_result_t drc;
#ifdef MOQR_BIND_TESTING
        int fail_kind = atomic_load(&bind_dbg_fail_sub_done_kind);
        bool fail_match = fail_kind == 0 ||
                          (fail_kind == 1 && !ds->publication) ||
                          (fail_kind == 2 && ds->publication);
        if (atomic_load(&bind_dbg_block_sub_dones) > 0) {
            /* Injected block: the session is NOT called, so the terminal is
             * genuinely unsent and the retry must produce it. */
            (void)atomic_fetch_sub(&bind_dbg_block_sub_dones, 1);
            drc = MOQ_ERR_WOULD_BLOCK;
        } else if (fail_match && atomic_load(&bind_dbg_fail_sub_dones) > 0 &&
                   atomic_fetch_sub(&bind_dbg_fail_sub_dones, 1) == 1) {
            /* Injected hard refusal: nothing queued, nothing sent. */
            drc = MOQ_ERR_WRONG_STATE;
        } else
#endif
        if (ds->publication) {
            moq_finish_publish_cfg_t fc;
            moq_finish_publish_cfg_init(&fc);
            fc.status_code = pd_wire;
            fc.stream_count = ds->streams_opened;
            drc = moq_session_finish_publish(
                cn->session, (moq_publication_t){ ._opaque = it->cookie }, &fc,
                now_us);
        } else {
            drc = moq_session_done_subscribe(cn->session, ssub, &cfg, now_us);
        }
        if (drc == MOQ_ERR_WOULD_BLOCK) {
            /* No room yet (action queue, encode space or drain slot): the
             * session queued nothing. Retry with tracking intact. */
            *defer = *it;
            return false;
        }
        if (drc == MOQ_ERR_STALE_HANDLE) {
            /* The session already ended this request itself, without telling
             * the binding (an update it refused on its own ends the request
             * with UPDATE_FAILED; no event names it). Its terminal -- and any
             * resets it still owes before that -- are the session's to send,
             * so the binding only retires its own owner, already validated
             * above. Stale is not proof of delivery. */
            bind_dsub_release(b, cn, ds);
            return true;
        }
        if (drc != MOQ_OK) {
            /* A hard refusal also queued nothing (WRONG_STATE, BUFFER: the
             * encoded terminal can never fit, INVAL, ...), and no retry can
             * deliver it. Releasing the owner here would strand the request
             * with no terminal, so end it the way every unrecoverable binding
             * fault does: close the session. */
            b->session_errors++;
            (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                    "terminal refused", now_us);
            conn_detach(b, cn, now_us);
            return true;
        }
        /* Accepted: the terminal is queued, so the owner is released. */
        bind_dsub_release(b, cn, ds);
        return true;
    }
    case MOQR_INTENT_UPSTREAM_SUBSCRIBE: {
        /* Borrowed-payload (ns_parts/name); NOT deferrable — a blocked
         * subscribe fails closed to upstream_error, whose downstream REJECT_SUB
         * IS durable, so the requester still gets a terminal. Making the
         * upstream subscribe itself retryable is a later refinement. */
        int slot = usub_free_slot(b, cn);
        if (slot < 0) {
            /* The upstream table is a bounded pool: the parked downstream
             * requests get a capacity refusal. */
            (void)moqr_core_upstream_error(
                b->core, it->track, it->track_gen,
                MOQR_REQUEST_ERROR_CAPACITY, now_us);
            return true;
        }
        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){ .parts = it->ns_parts,
                                                 .count = it->ns_count };
        cfg.track_name = it->name;
        cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t ssub;
        if (moq_session_subscribe(cn->session, &cfg, now_us, &ssub) ==
            MOQ_OK) {
            usub_release(b, &cn->usubs[slot]);   /* a clean slot */
            cn->usubs[slot].used = true;
            cn->usubs[slot].track = it->track;
            cn->usubs[slot].track_gen = it->track_gen;
            cn->usubs[slot].ssub = ssub;
            usub_store_key(b, &cn->usubs[slot], it->ns_parts, it->ns_count,
                           it->name);
        } else {
            (void)moqr_core_upstream_error(
                b->core, it->track, it->track_gen,
                MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        }
        return true;
    }
    case MOQR_INTENT_UPSTREAM_UNSUBSCRIBE:
        /* NOT deferrable: moq_session_unsubscribe WOULD_BLOCKs on UNSUBSCRIBE
         * tombstone-capacity exhaustion (session_subscribe.c), which persists
         * until late responses reclaim tombstones — not a transient full
         * queue. Parking it would deadlock the retry head. It is upstream
         * cleanup, not a downstream terminal: a dropped unsubscribe leaks an
         * unwanted upstream subscription (bounded, per-track), never hangs a
         * peer. Best-effort, as before. */
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (cn->usubs[i].used &&
                moqr_track_eq(cn->usubs[i].track, it->track)) {
                (void)moq_session_unsubscribe(cn->session, cn->usubs[i].ssub,
                                              now_us);
                usub_release(b, &cn->usubs[i]);
            }
        }
        return true;
    case MOQR_INTENT_ACCEPT_PUBLISH: {
        moq_publication_t pub = { it->cookie };
        /* Reserve tracking BEFORE accepting: an untracked publish would
         * silently drop every object it pushes. On refusal, unwind the
         * core's track (upstream lost) and reject on the wire (durable, as
         * with ACCEPT_SUB's no-slot path). */
        int slot = pub_free_slot(b, cn);
        if (slot < 0) {
            (void)moqr_core_upstream_lost(b->core, it->track, it->track_gen);
            moq_reject_publish_cfg_t rj;
            moq_reject_publish_cfg_init(&rj);
            rj.error_code = bind_capacity_error(cn);   /* bounded pool */
            rj.can_retry = true;
            rj.retry_after_ms = bind_capacity_retry_ms(it->cookie);
            if (moq_session_reject_publish(cn->session, pub, &rj, now_us) ==
                MOQ_ERR_WOULD_BLOCK) {
                *defer = *it;
                defer->kind = MOQR_INTENT_REJECT_PUBLISH;
                defer->error_code = MOQR_REQUEST_ERROR_CAPACITY;
                return false;
            }
            return true;
        }
        moq_accept_publish_cfg_t cfg;
        moq_accept_publish_cfg_init(&cfg);
        moq_result_t prc =
            moq_session_accept_publish(cn->session, pub, &cfg, now_us);
        if (prc == MOQ_ERR_WOULD_BLOCK) {
            *defer = *it;
            return false;
        }
        if (prc != MOQ_OK) {
            (void)moqr_core_upstream_lost(b->core, it->track, it->track_gen);
            b->session_errors++;
            return true;
        }
        cn->pubs[slot].used = true;
        cn->pubs[slot].pub_raw = it->cookie;
        cn->pubs[slot].track = it->track;
        cn->pubs[slot].track_gen = it->track_gen;
        return true;
    }
    case MOQR_INTENT_REJECT_PUBLISH: {
        moq_publication_t pub = { it->cookie };
        moq_reject_publish_cfg_t cfg;
        moq_reject_publish_cfg_init(&cfg);
        if (it->error_code == MOQR_REQUEST_ERROR_CAPACITY) {
            cfg.error_code = bind_capacity_error(cn);
            cfg.can_retry = true;
            cfg.retry_after_ms = bind_capacity_retry_ms(it->cookie);
        } else {
            cfg.error_code = it->error_code;
        }
        if (moq_session_reject_publish(cn->session, pub, &cfg, now_us) ==
            MOQ_ERR_WOULD_BLOCK) {
            *defer = *it;
            return false;
        }
        return true;
    }
    case MOQR_INTENT_NS_FOUND:
    case MOQR_INTENT_NS_GONE: {
        /* Borrowed-payload (ns_parts), so the intent itself can never be
         * parked. draft-18 Section 6.2 owes this NAMESPACE to the matching
         * prefix subscriber as a MUST, so a blocked write is retained by
         * deep copy (bind_nsu_retain) and replayed, never dropped. */
        moq_ns_sub_handle_t h = { it->cookie };
        bool is_gone = it->kind == MOQR_INTENT_NS_GONE;
        moqr_ns_t suffix_ns = {
            .parts = it->ns_parts + it->value,
            .count = it->ns_count - (size_t)it->value,
        };
        /* The ROOT namespace -- zero ANNOUNCED fields -- has no representation
         * on a peer whose negotiated draft requires at least one, so sending it
         * would put an invalid namespace on that wire. The test is the announced
         * cardinality, not an empty suffix: a non-root namespace equal to its
         * prefix also yields a zero-length suffix and is unaffected. Ask the
         * CAPABILITY, never a draft number, and drop the update before the send
         * and before any retain/owed accounting, so it is never queued,
         * retained or counted as owed. FOUND and GONE are suppressed
         * symmetrically, so the peer is never left holding half a pair; its own
         * namespace subscription, including a legal empty prefix, is
         * untouched. */
        if (it->ns_count == 0 &&
            !moq_session_supports_zero_field_track_namespace(cn->session)) {
            return true;   /* consumed: nothing sent, nothing owed */
        }
        /* No NAMESPACE before the subscription's REQUEST_OK: while its
         * acceptance is held, the update waits in order like a blocked one. */
        if (mode != BIND_TRY_FORCE_PARK &&
            !bind_ns_sub_accept_held(b, cn, it->cookie)) {
            moq_namespace_t suffix = { .parts = suffix_ns.parts,
                                       .count = suffix_ns.count };
            moq_result_t rc =
                is_gone
                    ? moq_session_send_namespace_done(cn->session, h, &suffix,
                                                      now_us)
                    : moq_session_send_namespace(cn->session, h, &suffix,
                                                 now_us);
            if (rc == MOQ_OK) {
                return true;   /* healthy: nothing allocated, nothing parked */
            }
            if (rc == MOQ_ERR_STALE_HANDLE) {
                /* The session has retired the subscription (the peer reset
                 * or stopped its stream; the session reports no event for
                 * that). Nothing is owed to it: drop it from the core so it
                 * stops taking updates and a namespace-subscription slot,
                 * and drop its other parked updates. A purge that removes
                 * this very entry from the ring is noticed by the ring
                 * retry through the purge epoch. */
                (void)moqr_core_ns_unsubscribe(b->core, cn->binding,
                                               it->cookie);
                bind_pending_purge(b, (uint64_t)(cn - b->conns) + 1u, true,
                                   it->cookie);
                return true;
            }
            if (rc != MOQ_ERR_WOULD_BLOCK) {
                /* A hard send failure loses an update the spec says MUST be
                 * delivered. Counting it is telemetry, not enforcement: the
                 * subscriber that can no longer be told the truth is closed
                 * on the same lifecycle-safe path capacity exhaustion uses. */
                b->session_errors++;
                b->nsu_failed_closed++;
                (void)moq_session_close(cn->session,
                                        BIND_SESSION_INTERNAL_ERROR,
                                        "namespace update failed", now_us);
                conn_detach(b, cn, now_us);
                return true;
            }
        }
        if (mode == BIND_TRY_FROM_RING) {
            /* Already parked, and its payload already bind-owned: keep the
             * slot exactly where it is so nothing overtakes it. */
            *defer = *it;
            return false;
        }
        if (!bind_defer_push_nsu(b, it)) {
            /* Held state exhausted. Failing closed on the subscriber that can
             * no longer be told the truth is the only honest option: silently
             * continuing would drop a MUST. Other connections' parked output
             * is untouched. */
            b->nsu_failed_closed++;
            (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                    "namespace update undeliverable", now_us);
            conn_detach(b, cn, now_us);
            return true;
        }
        /* Parked in the one FIFO: the executor must now treat the bind as
         * blocked so nothing polled after this can be attempted ahead of it. */
        if (parked_out != NULL) {
            *parked_out = true;
        }
        return true;
    }
    case MOQR_INTENT_TRACK_STATUS_OK: {
        moq_track_status_handle_t h = { it->cookie };
        const moqr_bind_meta_ref_t *pin = b->metadata_executing;
        /* Draft-16's answer (a REQUEST_OK) has no field for them at all. */
        const bool stated = moq_session_version(cn->session) != MOQ_VERSION_DRAFT_16;
        if (stated && !bind_meta_can_replay(pin, moq_session_version(cn->session))) {
            /* The track's properties cannot be stated on this session, so
             * the status is refused rather than answered without them, as an
             * ACK would be (§7.7). */
            moqr_intent_t refuse = *it;
            refuse.kind = MOQR_INTENT_TRACK_STATUS_ERROR;
            refuse.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
            return bind_try_intent(b, &refuse, now_us, mode, parked_out, defer);
        }
        moq_accept_track_status_cfg_t cfg;
        moq_accept_track_status_cfg_init(&cfg);
        cfg.has_largest = it->has_largest;
        cfg.largest_group = it->largest_group;
        cfg.largest_object = it->largest_object;
        if (stated) cfg.track_properties = pin->bytes;
        moq_result_t trc =
            moq_session_accept_track_status(cn->session, h, &cfg, now_us);
        if (trc == MOQ_ERR_WOULD_BLOCK) {
            *defer = *it;
            return false;
        }
        if (trc != MOQ_OK) {
            b->session_errors++;
        }
        return true;
    }
    case MOQR_INTENT_TRACK_STATUS_ERROR: {
        moq_track_status_handle_t h = { it->cookie };
        moq_reject_track_status_cfg_t cfg;
        moq_reject_track_status_cfg_init(&cfg);
        cfg.error_code = it->error_code;
        if (moq_session_reject_track_status(cn->session, h, &cfg, now_us) ==
            MOQ_ERR_WOULD_BLOCK) {
            *defer = *it;
            return false;
        }
        return true;
    }
    default:
        return true;
    }
}

/* -- delivery -------------------------------------------------------------------- */

/* An ACCEPTED fetch hit an unrecoverable session error (eviction TOO_OLD, or a
 * write/accept/FIN failure). The session has no per-fetch abort, and the peer
 * has been (or is about to be) admitted, so its liveness demands a terminal:
 * hard-close the session (a
 * visible transport close reaches the peer) and detach the conn. binding_close
 * inside conn_detach retires the conn's core fetch cursors. A per-fetch reset /
 * range markers is a later refinement. Returns after detaching — the caller must
 * stop touching this conn. */
/* -- Forwarded Joining FETCH implementation (see the b_fwd_t comment) -------- */

#ifdef MOQR_BIND_TESTING
#define FWD_DBG(b, field) ((b)->dbg_fwd.field++)
#else
#define FWD_DBG(b, field) ((void)0)
#endif

static uint64_t
conn_cookie(const moqr_bind_t *b, const b_conn_t *cn)
{
    return (uint64_t)(cn - b->conns) + 1u;
}

/* The connection a transaction side names, or NULL when that slot has been
 * closed or reused since (cookie + core binding must both still match). */
static b_conn_t *
fwd_conn(moqr_bind_t *b, uint64_t cookie, moqr_binding_t binding)
{
    b_conn_t *cn = conn_by_cookie(b, cookie);
    if (cn == NULL || !cn->used || cn->closed || cn->session == NULL ||
        !moqr_binding_eq(cn->binding, binding)) {
        return NULL;
    }
    return cn;
}

static bool
fwd_bytes_charge(moqr_bind_t *b, uint64_t *charged, uint64_t n)
{
    if (n > b->fwd_bytes_cap - b->fwd_bytes_used) {
        return false;
    }
    b->fwd_bytes_used += n;
    *charged += n;
#ifdef MOQR_BIND_TESTING
    if (b->fwd_bytes_used > b->dbg_fwd.bytes_high) {
        b->dbg_fwd.bytes_high = b->fwd_bytes_used;
    }
#endif
    return true;
}

static bool
fwd_charge(moqr_bind_t *b, b_fwd_t *t, uint64_t n)
{
    return fwd_bytes_charge(b, &t->charged, n);
}

static void
fwd_uncharge(moqr_bind_t *b, b_fwd_t *t, uint64_t n)
{
    b->fwd_bytes_used -= n;
    t->charged -= n;
}

/* Owned, charged copy of a borrowed byte span. Empty spans copy nothing. */
static bool
fwd_copy(moqr_bind_t *b, b_fwd_t *t, const uint8_t *src, size_t len,
         uint8_t **dst, size_t *dst_len)
{
    *dst = NULL;
    *dst_len = 0;
    if (len == 0) {
        return true;
    }
    if (src == NULL || !fwd_charge(b, t, len)) {
        return false;
    }
    uint8_t *p = b->alloc.alloc(len, b->alloc.ctx);
    if (p == NULL) {
        fwd_uncharge(b, t, len);
        return false;
    }
    memcpy(p, src, len);
    *dst = p;
    *dst_len = len;
    return true;
}

static void
fwd_item_release(moqr_bind_t *b, b_fwd_t *t, b_fwd_item_t *it)
{
    if (it->payload != NULL) {
        moq_rcbuf_decref(it->payload);
    }
    if (it->properties != NULL) {
        moq_rcbuf_decref(it->properties);
    }
    fwd_uncharge(b, t, it->charged);
    /* Append-only slots retain their Location until transaction retirement:
     * data can precede FETCH_OK, even after live coverage consumes this copy. */
    it->payload = NULL;
    it->properties = NULL;
    it->charged = 0;
}

/* Release every owned reference and copy and free the slot. Only called once
 * BOTH sides are terminal: nothing is owed on either session any more. */
static void
fwd_release(moqr_bind_t *b, b_fwd_t *t)
{
    for (uint32_t i = t->next_write; i < t->n_items; i++) {
        fwd_item_release(b, t, &t->items[i]);
    }
    if (t->props != NULL) {
        b->alloc.free(t->props, t->props_len, b->alloc.ctx);
        fwd_uncharge(b, t, t->props_len);
    }
    if (t->reason != NULL) {
        b->alloc.free(t->reason, t->reason_len, b->alloc.ctx);
        fwd_uncharge(b, t, t->reason_len);
    }
    if (t->key != NULL) {
        b->alloc.free(t->key, t->key_len, b->alloc.ctx);
        fwd_uncharge(b, t, t->key_len);
    }
    /* Invariant: every charge was matched by its release. */
    b->fwd_bytes_used -= t->charged;
    memset(t, 0, sizeof(*t));
#ifdef MOQR_BIND_TESTING
    b->dbg_fwd.live--;
#endif
}

static b_fwd_t *
fwd_find_up(moqr_bind_t *b, const b_conn_t *cn, uint64_t handle)
{
    uint64_t cookie = conn_cookie(b, cn);
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        b_fwd_t *t = &b->fwd[i];
        if (t->used && t->ufetch_live && t->u_cookie == cookie &&
            moqr_binding_eq(t->u_binding, cn->binding) &&
            t->ufetch._opaque == handle) {
            return t;
        }
    }
    return NULL;
}

static b_fwd_t *
fwd_find_down(moqr_bind_t *b, const b_conn_t *cn, uint64_t handle)
{
    uint64_t cookie = conn_cookie(b, cn);
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        b_fwd_t *t = &b->fwd[i];
        if (t->used && t->consumer == FW_CONSUMER_JOIN &&
            t->d != FW_D_TERMINAL && t->d_cookie == cookie &&
            moqr_binding_eq(t->d_binding, cn->binding) &&
            t->sfetch._opaque == handle) {
            return t;
        }
    }
    return NULL;
}

/* The upstream side ends for a reason other than its own COMPLETE/ERROR/RESET:
 * if a request is still live upstream it is cancelled (retried until the
 * session commits it); the recorded `end` is the first one, never overwritten. */
static void
fwd_end_up(b_fwd_t *t, uint8_t end)
{
    if (t->end == FW_END_NONE) {
        t->end = end;
    }
    if (t->u == FW_U_PENDING) {
        t->u = FW_U_CANCELLING;
    } else if (t->u == FW_U_UNSENT) {
        t->u = FW_U_TERMINAL;
    }
}

static int
retrieve_loc_cmp(moqr_retrieve_location_t a, moqr_retrieve_location_t z)
{
    if (a.group != z.group) return a.group < z.group ? -1 : 1;
    return a.object == z.object ? 0 : (a.object < z.object ? -1 : 1);
}

static moqr_retrieve_location_t
retrieve_loc(uint64_t group, uint64_t object)
{
    return (moqr_retrieve_location_t){ .group = group, .object = object };
}

/* Never carry the raw FETCH end into another group. Zero encodes the whole
 * end group, including its UINT64_MAX object. These conversions are lossless. */
static moqr_retrieve_location_t
retrieve_inclusive_end(uint64_t group, uint64_t raw_object)
{
    return retrieve_loc(group, raw_object == 0 ? UINT64_MAX : raw_object - 1u);
}

static uint64_t
retrieve_raw_end(moqr_retrieve_location_t end)
{
    return end.object == UINT64_MAX ? 0 : end.object + 1u;
}

static moqr_retrieve_location_t
retrieve_loc_min(moqr_retrieve_location_t a, moqr_retrieve_location_t z)
{
    return retrieve_loc_cmp(a, z) < 0 ? a : z;
}

/* Only called for a Location strictly above the current (nonnegative) next. */
static moqr_retrieve_location_t
retrieve_previous(moqr_retrieve_location_t at)
{
    return at.object == 0 ? retrieve_loc(at.group - 1u, UINT64_MAX)
                          : retrieve_loc(at.group, at.object - 1u);
}

/* A fill's parent subscription is still the live, accepted one for the
 * track generation; a standalone FETCH's track is still that generation. */
static bool
retrieve_parent_current(moqr_bind_t *b, const moqr_retrieve_request_t *r)
{
    if (r->standalone) {
        uint64_t gen = 0;
        return moqr_core_track_current_generation(b->core, r->track, &gen) ==
                   MOQR_OK && gen == r->track_gen;
    }
    moqr_track_t track = MOQR_TRACK_INVALID;
    uint64_t gen = 0;
    moqr_sub_state_t state;
    return moqr_core_sub_state(b->core, r->parent, &state) == MOQR_OK &&
           state == MOQR_SUB_ACTIVE &&
           moqr_core_sub_track(b->core, r->parent, &track, &gen) == MOQR_OK &&
           moqr_track_eq(track, r->track) && gen == r->track_gen;
}

typedef struct b_retrieve_source {
    b_conn_t *conn;
    moqr_retrieve_parent_t parent;
    const b_usub_t *subscription;
} b_retrieve_source_t;

static bool
retrieve_source(moqr_bind_t *b, const moqr_retrieve_request_t *r,
                 bool pinned, uint64_t cookie, moqr_binding_t binding,
                 moqr_retrieve_parent_t parent, b_retrieve_source_t *out)
{
    memset(out, 0, sizeof(*out));
    for (uint32_t i = 0; i < b->max_conns; i++) {
        b_conn_t *cn = &b->conns[i];
        if (!cn->used || cn->closed || cn->session == NULL ||
            (pinned && (conn_cookie(b, cn) != cookie ||
                        !moqr_binding_eq(cn->binding, binding)))) continue;
        for (uint32_t j = 0; j < b->n_usubs; j++) {
            b_usub_t *us = &cn->usubs[j];
            if (us->used && moqr_track_eq(us->track, r->track) &&
                us->track_gen == r->track_gen &&
                (!pinned || (!parent.publish &&
                             us->ssub._opaque == parent.handle.sub._opaque))) {
                out->conn = cn;
                out->parent.handle.sub = us->ssub;
                out->subscription = us;
                return true;
            }
        }
        for (uint32_t j = 0; j < b->n_pubs; j++) {
            const b_pub_t *pub = &cn->pubs[j];
            if (pub->used && moqr_track_eq(pub->track, r->track) &&
                pub->track_gen == r->track_gen &&
                (!pinned || (parent.publish &&
                             pub->pub_raw == parent.handle.pub._opaque))) {
                out->conn = cn;
                out->parent.publish = true;
                out->parent.handle.pub = (moq_publication_t){ ._opaque = pub->pub_raw };
                return true;
            }
        }
    }
    return false;
}

static bool
retrieve_fwd_current(moqr_bind_t *b, const b_fwd_t *t)
{
    b_retrieve_source_t source;
    return retrieve_parent_current(b, &t->retrieval) &&
           retrieve_source(b, &t->retrieval, true, t->u_cookie, t->u_binding,
                            t->source_parent, &source);
}

static void
retrieve_fwd_expire(b_fwd_t *t, uint64_t now_us)
{
    if (t->consumer == FW_CONSUMER_FILL && t->end == FW_END_NONE &&
        now_us >= t->retrieval.deadline_us) {
        fwd_end_up(t, FW_END_TIMEOUT);
    }
}

/* Clone visible bytes, not a possibly much larger rcbuf root/slice. Charge
 * the exact allocations (including both rcbuf headers) before retaining them.
 * Legacy Joining FETCH keeps its existing event-reference ownership policy. */
static moqr_result_t
retrieve_object_bytes(const moq_rcbuf_t *payload, const moq_rcbuf_t *properties,
                       uint64_t *out)
{
    size_t payload_bytes = 0, property_bytes = 0;
    if ((payload != NULL &&
         moq_rcbuf_allocation_size(moq_rcbuf_len(payload), &payload_bytes) !=
             MOQ_OK) ||
        (properties != NULL &&
         moq_rcbuf_allocation_size(moq_rcbuf_len(properties), &property_bytes) !=
             MOQ_OK)) {
        return MOQR_ERR_CAPACITY;
    }
    const uint64_t bytes = (uint64_t)payload_bytes + (uint64_t)property_bytes;
    if (bytes < (uint64_t)payload_bytes) {
        return MOQR_ERR_CAPACITY;
    }
    *out = bytes;
    return MOQR_OK;
}

static moqr_result_t
retrieve_copy_object(moqr_bind_t *b, b_fwd_t *t, b_fwd_item_t *it,
                      const moq_fetch_object_event_t *o)
{
    uint64_t bytes;
    moqr_result_t rc = retrieve_object_bytes(o->payload, o->properties, &bytes);
    if (rc != MOQR_OK) return rc;
    if (!fwd_charge(b, t, bytes)) return MOQR_ERR_CAPACITY;
    it->charged = bytes;
    if ((o->payload != NULL &&
         moq_rcbuf_clone(&b->alloc, o->payload, &it->payload) != MOQ_OK) ||
        (o->properties != NULL &&
         moq_rcbuf_clone(&b->alloc, o->properties, &it->properties) != MOQ_OK)) {
        fwd_item_release(b, t, it);
        return MOQR_ERR_NOMEM;
    }
    return MOQR_OK;
}

static b_fwd_t *
retrieve_collector(moqr_bind_t *b, const moqr_retrieve_cursor_t *c)
{
    if (c->collector_slot == 0 || c->collector_slot > b->n_fwd) return NULL;
    b_fwd_t *t = &b->fwd[c->collector_slot - 1u];
    return t->used && t->consumer == FW_CONSUMER_FILL &&
           t->generation == c->collector_generation ? t : NULL;
}

static void
retrieve_detach_collector(moqr_bind_t *b, moqr_retrieve_cursor_t *c)
{
    b_fwd_t *t = retrieve_collector(b, c);
    if (t != NULL) {
        t->d = FW_D_TERMINAL;
        fwd_end_up(t, FW_END_CANCELLED);
        if (t->u == FW_U_TERMINAL) {
            fwd_release(b, t);
        } else {
            b->budget_pending = true; /* one pump for newly owed cancellation */
        }
    }
    c->collector_slot = 0;
    c->collector_generation = 0;
}

/* No authorization call here: the still-live parent/grant authorizes this
 * child. The request is narrowed to this frozen hole, not a synthetic inbound
 * FETCH and not an unconditional pull of a track's history. */
static moqr_result_t
retrieve_admit(moqr_bind_t *b, moqr_retrieve_cursor_t *c)
{
    b_retrieve_source_t source;
    if (!retrieve_source(b, &c->request, c->source_bound,
                          c->source_cookie, c->source_binding,
                          c->source_parent, &source)) {
        return c->source_bound ? MOQR_ERR_STALE_HANDLE : MOQR_ERR_WOULD_BLOCK;
    }
    moq_bytes_t parts[32], name;
    moqr_ns_t ns;
    if (source.subscription != NULL) {
        const b_usub_t *us = source.subscription;
        if (!us->has_key) return MOQR_ERR_CAPACITY;
        ns = (moqr_ns_t){ .parts = us->parts, .count = us->ns_count };
        name = us->name;
    } else {
        /* Accepted PUBLISH tracking carries the full retained-track identity.
         * Copy its core-owned key now; never keep those borrowed views in a
         * transaction or infer source validity from a live connection alone. */
        moqr_result_t rc = moqr_core_track_key(b->core, c->request.track,
            c->request.track_gen, parts, 32, &ns, &name);
        if (rc != MOQR_OK) return rc;
    }
    size_t key_len = name.len;
    if (ns.count > 32 || key_len > BF_FWD_KEY_MAX ||
        (name.len != 0 && name.data == NULL)) return MOQR_ERR_CAPACITY;
    for (size_t i = 0; i < ns.count; i++) {
        if (ns.parts[i].len == 0 || ns.parts[i].data == NULL ||
            ns.parts[i].len > BF_FWD_KEY_MAX - key_len) return MOQR_ERR_CAPACITY;
        key_len += ns.parts[i].len;
    }
    b_fwd_t *t = NULL;
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        if (!b->fwd[i].used) { t = &b->fwd[i]; break; }
    }
    if (t == NULL || b->fwd_generation == UINT64_MAX) return MOQR_ERR_CAPACITY;
    if (key_len > b->fwd_bytes_cap - b->fwd_bytes_used) return MOQR_ERR_CAPACITY;
    memset(t, 0, sizeof(*t));
    t->used = true;
    t->consumer = FW_CONSUMER_FILL;
    t->generation = ++b->fwd_generation;
#ifdef MOQR_BIND_TESTING
    b->dbg_fwd.live++;
#endif
    if (!fwd_charge(b, t, key_len)) {
        fwd_release(b, t);
        return MOQR_ERR_CAPACITY;
    }
    if (key_len != 0) {
        t->key = b->alloc.alloc(key_len, b->alloc.ctx);
        if (t->key == NULL) {
            fwd_release(b, t);
            return MOQR_ERR_NOMEM;
        }
    }
    t->key_len = key_len;
    t->ns_count = (uint32_t)ns.count;
    size_t off = 0;
    for (uint32_t i = 0; i < t->ns_count; i++) {
        memcpy(t->key + off, ns.parts[i].data, ns.parts[i].len);
        t->parts[i] = (moq_bytes_t){ t->key + off, ns.parts[i].len };
        off += ns.parts[i].len;
    }
    if (name.len != 0) memcpy(t->key + off, name.data, name.len);
    t->name = (moq_bytes_t){ t->key != NULL ? t->key + off : NULL, name.len };
    t->retrieval = c->request;
    t->d_binding = c->request.binding;
    t->d_cookie = c->request.source;
    t->u_cookie = conn_cookie(b, source.conn);
    t->u_binding = source.conn->binding;
    t->source_parent = source.parent;
    t->track = c->request.track;
    t->track_gen = c->request.track_gen;
    t->start_group = c->next.group;
    t->start_object = c->next.object;
    t->end_group = c->span_end.group;
    t->end_object = retrieve_raw_end(c->span_end);
    t->has_prio = true;
    t->prio = c->request.subscriber_priority;
    t->u = FW_U_UNSENT;
    t->d = FW_D_PENDING;
    c->collector_slot = (uint32_t)(t - b->fwd) + 1u;
    c->collector_generation = t->generation;
    c->source_bound = true;
    c->source_cookie = t->u_cookie;
    c->source_binding = t->u_binding;
    c->source_parent = t->source_parent;
    /* Local fill replay runs after bind_fwd_pump. Signal its existing bounded
     * self-wake rather than leave an UNSENT request asleep until its deadline. */
    b->budget_pending = true;
    FWD_DBG(b, admitted);
    return MOQR_OK;
}

static void
retrieve_error(moqr_retrieve_cursor_t *c, moqr_retrieve_error_kind_t kind,
                moqr_result_t result)
{
    memset(&c->item, 0, sizeof(c->item));
    c->item.kind = MOQR_RETRIEVE_ERROR;
    c->item.error_kind = kind;
    c->item.local_result = result;
    c->item.start = c->next;
    c->item.end = c->request.end;
    c->pending = true;
}

static void
retrieve_fwd_error(moqr_retrieve_cursor_t *c, const b_fwd_t *t)
{
    moqr_retrieve_error_kind_t kind = t->failure;
    if (kind == 0) {
        switch (t->end) {
        case FW_END_ERROR: kind = MOQR_RETRIEVE_RESPONSE; break;
        case FW_END_RESET: kind = MOQR_RETRIEVE_RESET; break;
        case FW_END_LOST: kind = MOQR_RETRIEVE_SOURCE_LOST; break;
        case FW_END_MISMATCH: kind = MOQR_RETRIEVE_MALFORMED; break;
        default: kind = MOQR_RETRIEVE_LOCAL; break;
        }
    }
    retrieve_error(c, kind, t->local_result);
    c->item.session_result = t->session_result;
    c->item.error_code = t->end == FW_END_RESET ? t->reset_code : t->err_code;
    c->item.cause = t->reset_cause;
    c->item.data_stream = t->reset_data_stream;
    c->item.stop_sending = t->reset_stop_sending;
    c->item.can_retry = t->err_can_retry;
    c->item.retry_after_ms = t->err_retry_after_ms;
    c->item.reason = (moq_bytes_t){ t->reason, t->reason_len };
}

static void
retrieve_gap(moqr_retrieve_cursor_t *c, moqr_retrieve_location_t end,
              moq_fetch_range_kind_t kind)
{
    memset(&c->item, 0, sizeof(c->item));
    c->item.kind = MOQR_RETRIEVE_GAP;
    c->item.start = c->next;
    c->item.end = end;
    c->item.gap_kind = kind;
    c->pending = true;
}

static void
retrieve_object(moqr_retrieve_cursor_t *c, const moqr_record_view_t *object)
{
    memset(&c->item, 0, sizeof(c->item));
    c->item.kind = MOQR_RETRIEVE_OBJECT;
    c->item.start = c->next;
    c->item.end = retrieve_loc(object->group_id, object->object_id);
    c->item.object = *object;
    c->pending = true;
}

static void
retrieve_cached_release(moqr_bind_t *b, moqr_retrieve_cursor_t *c)
{
    if (c->cached_slot != 0 && c->cached_slot <= b->n_fwd) {
        b_retrieve_hold_t *hold = &b->cached_holds[c->cached_slot - 1u];
        if (hold->generation != 0 && hold->generation == c->cached_generation) {
            moq_rcbuf_decref(hold->payload);
            moq_rcbuf_decref(hold->properties);
            b->fwd_bytes_used -= hold->charged;
            memset(hold, 0, sizeof(*hold));
        }
    }
    c->cached_slot = 0;
    c->cached_generation = 0;
}

static moqr_result_t
retrieve_cached_copy(moqr_bind_t *b, moqr_retrieve_cursor_t *c,
                     moqr_record_view_t *object)
{
    if (c->cached_slot != 0) return MOQR_ERR_WRONG_STATE;
    uint64_t bytes;
    moqr_result_t rc = retrieve_object_bytes(object->payload, object->properties, &bytes);
    if (rc != MOQR_OK) return rc;
    b_retrieve_hold_t *hold = NULL;
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        if (b->cached_holds[i].generation == 0) {
            hold = &b->cached_holds[i];
            break;
        }
    }
    if (hold == NULL || b->cached_generation == UINT64_MAX ||
        !fwd_bytes_charge(b, &hold->charged, bytes)) return MOQR_ERR_CAPACITY;
    hold->generation = ++b->cached_generation;
    c->cached_slot = (uint32_t)(hold - b->cached_holds) + 1u;
    c->cached_generation = hold->generation;
    if ((object->payload != NULL &&
         moq_rcbuf_clone(&b->alloc, object->payload, &hold->payload) != MOQ_OK) ||
        (object->properties != NULL &&
         moq_rcbuf_clone(&b->alloc, object->properties, &hold->properties) != MOQ_OK)) {
        retrieve_cached_release(b, c);
        return MOQR_ERR_NOMEM;
    }
    object->payload = hold->payload;
    object->properties = hold->properties;
    return MOQR_OK;
}

static void
retrieve_cached_object(moqr_bind_t *b, moqr_retrieve_cursor_t *c)
{
    moqr_record_view_t object = c->cache_item.rec;
    if (object.payload != NULL || object.properties != NULL) {
        moqr_result_t rc = retrieve_cached_copy(b, c, &object);
        if (rc != MOQR_OK) {
            retrieve_error(c, MOQR_RETRIEVE_LOCAL, rc);
            return;
        }
    }
    retrieve_object(c, &object);
}

/* The part of the request the cursor's next Location belongs to: the whole
 * range in ascending order, the next Location's group in descending order. */
static moqr_retrieve_location_t
retrieve_window(const moqr_retrieve_cursor_t *c)
{
    return moqr_retrieve_window_end(c->request.end, c->request.group_order,
                                    c->next.group);
}

/* A fill's cache reader is its own core FETCH; a standalone FETCH's is its
 * admission, whose view is only reset (dropping a pin) and never closed. */
static void
retrieve_cache_close(moqr_bind_t *b, moqr_retrieve_cursor_t *c)
{
    if (c->request.standalone) {
        if (moqr_fetch_is_valid(c->cache)) {
            moqr_retrieve_location_t w = retrieve_window(c);
            (void)moqr_core_internal_retrieve_view(b->core, c->request.owner,
                                                   c->next.group, c->next.object,
                                                   w.group, w.object);
        }
    } else if (moqr_fetch_is_valid(c->cache)) {
        (void)moqr_core_fetch_close(b->core, c->cache);
    }
    c->cache = MOQR_FETCH_INVALID;
    c->cache_peeked = false;
    c->cache_absent = false;
    memset(&c->cache_item, 0, sizeof(c->cache_item));
}

static moqr_result_t
retrieve_cache_open(moqr_bind_t *b, moqr_retrieve_cursor_t *c, uint64_t now_us)
{
    if (c->request.standalone) {
        moqr_retrieve_location_t w = retrieve_window(c);
        moqr_result_t vrc = moqr_core_internal_retrieve_view(
            b->core, c->request.owner, c->next.group, c->next.object,
            w.group, w.object);
        if (vrc != MOQR_OK) return vrc;
        c->cache = c->request.owner;
        c->cache_end = w;
        return MOQR_OK;
    }
    moq_bytes_t parts[32], name;
    moqr_ns_t ns;
    moqr_result_t rc = moqr_core_track_key(b->core, c->request.track,
                                           c->request.track_gen, parts, 32,
                                           &ns, &name);
    if (rc != MOQR_OK) return rc;
    moqr_fetch_req_t req;
    moqr_fetch_req_init(&req);
    req.ns = ns;
    req.name = name;
    req.start_group = c->next.group;
    req.start_object = c->next.object;
    req.end_group = c->request.end.group;
    req.end_object = retrieve_raw_end(c->request.end);
    req.group_order = MOQR_GROUP_ORDER_ASCENDING;
    req.subscriber_priority = c->request.subscriber_priority;
    req.cookie = c->request.child;
    req.report_gaps = true;
    req.report_unknown = true;
    req.report_pending = true;
    moqr_fetch_plan_t plan;
    rc = moqr_core_fetch_open(b->core, c->request.binding, &req, now_us,
                              &c->cache, &plan);
    if (rc != MOQR_OK) return rc;
    if (plan.admit != MOQR_FETCH_ACCEPT) {
        c->cache = MOQR_FETCH_INVALID;
        if (plan.error_code != MOQ_REQUEST_ERROR_DOES_NOT_EXIST &&
            plan.error_code != MOQ_REQUEST_ERROR_INVALID_RANGE) {
            return MOQR_ERR_INTERNAL;
        }
        c->cache_absent = true;
        return MOQR_OK;
    }
    c->cache_end = retrieve_inclusive_end(plan.end_group, plan.end_object);
    if (retrieve_loc_cmp(c->cache_end, c->next) < 0 ||
        retrieve_loc_cmp(c->cache_end, c->request.end) > 0) {
        return MOQR_ERR_INTERNAL;
    }
    return MOQR_OK;
}

/* A cache frontier is either a pinned object, a known non-existent prefix,
 * or an exact unresolved run. A peeked core gap stays frozen until consumed;
 * re-peeking it while an origin is collecting would overwrite its commit end. */
typedef enum retrieve_cache_kind {
    RETRIEVE_CACHE_OBJECT, RETRIEVE_CACHE_MISSING, RETRIEVE_CACHE_ABSENT,
    RETRIEVE_CACHE_PENDING,
} retrieve_cache_kind_t;

static moqr_result_t
retrieve_cache_front(moqr_bind_t *b, moqr_retrieve_cursor_t *c, uint64_t now_us,
                      retrieve_cache_kind_t *kind, moqr_retrieve_location_t *end)
{
    if (!moqr_fetch_is_valid(c->cache) && !c->cache_absent) {
        moqr_result_t rc = retrieve_cache_open(b, c, now_us);
        if (rc != MOQR_OK) return rc;
    }
    if (c->cache_absent) {
        *kind = RETRIEVE_CACHE_MISSING;
        *end = retrieve_window(c);
        return MOQR_OK;
    }
    if (!c->cache_peeked) {
        moqr_result_t rc = moqr_core_fetch_peek(b->core, c->cache, now_us,
                                                &c->cache_item);
        if (rc != MOQR_OK) return rc;
        c->cache_peeked = true;
    }
    const moqr_fetch_item_t *it = &c->cache_item;
    *end = retrieve_loc(it->rec.group_id, it->rec.object_id);
    switch (it->kind) {
    case MOQR_FETCH_ITEM_OBJECT:
    case MOQR_FETCH_ITEM_PENDING:
        if (retrieve_loc_cmp(*end, c->next) > 0) {
            *end = retrieve_previous(*end);
            *kind = RETRIEVE_CACHE_ABSENT;
        } else if (it->kind == MOQR_FETCH_ITEM_OBJECT) {
            *kind = RETRIEVE_CACHE_OBJECT;
        } else {
            /* An Object still arriving here: a standalone FETCH waits for
             * it to complete (draft-22 3.2); a fill retrieves it. */
            *kind = c->request.standalone ? RETRIEVE_CACHE_PENDING
                                          : RETRIEVE_CACHE_MISSING;
        }
        break;
    case MOQR_FETCH_ITEM_MARKER:
        if (it->marker_group == 0) return MOQR_ERR_INTERNAL;
        *end = retrieve_loc(it->marker_group - 1u, UINT64_MAX);
        *kind = RETRIEVE_CACHE_MISSING;
        break;
    case MOQR_FETCH_ITEM_GAP:
    case MOQR_FETCH_ITEM_UNKNOWN:
        *kind = RETRIEVE_CACHE_MISSING;
        break;
    case MOQR_FETCH_ITEM_END:
        *end = c->cache_end;
        *kind = RETRIEVE_CACHE_ABSENT;
        break;
    default:
        return MOQR_ERR_INTERNAL;
    }
    if (retrieve_loc_cmp(*end, c->next) < 0 ||
        retrieve_loc_cmp(*end, retrieve_window(c)) > 0) return MOQR_ERR_INTERNAL;
    return MOQR_OK;
}

moqr_result_t
moqr_bind_retrieve_init(moqr_bind_t *b, moqr_retrieve_cursor_t *c,
                        const moqr_retrieve_request_t *r)
{
    if (b == NULL || c == NULL || r == NULL ||
        retrieve_loc_cmp(r->start, r->end) > 0) return MOQR_ERR_INVAL;
    if (c->live) return MOQR_ERR_WRONG_STATE;
    if (r->group_order != MOQR_GROUP_ORDER_ASCENDING &&
        !(r->standalone && r->group_order == MOQR_GROUP_ORDER_DESCENDING)) {
        return MOQR_ERR_UNSUPPORTED;
    }
    if (r->standalone && !moqr_fetch_is_valid(r->owner)) return MOQR_ERR_INVAL;
    if (!retrieve_parent_current(b, r)) return MOQR_ERR_STALE_HANDLE;
    memset(c, 0, sizeof(*c));
    c->request = *r;
    c->next = moqr_retrieve_first(r->start, r->end, r->group_order);
    c->cache = MOQR_FETCH_INVALID;
    c->live = true;
    return MOQR_OK;
}

/* Once origin work ends, convert its stream into explicit coverage. FIN
 * proves the trailing default gap ONLY through the accepted FETCH_OK end.
 * A shortened OK leaves an UNKNOWN tail, never invented non-existence. On
 * timeout, complete received items (even before OK) remain usable; only the
 * unproven tail is TIMED_OUT. No hard failure enters this path. */
static void
retrieve_collection_front(moqr_retrieve_cursor_t *c, const b_fwd_t *t)
{
    if (t != NULL && t->next_write < t->n_items) {
        const b_fwd_item_t *it = &t->items[t->next_write];
        moqr_retrieve_location_t end = retrieve_loc(it->group, it->object);
        if (it->is_gap) {
            retrieve_gap(c, end, (moq_fetch_range_kind_t)it->range_kind);
        } else if (retrieve_loc_cmp(end, c->next) > 0) {
            retrieve_gap(c, retrieve_previous(end), MOQ_FETCH_RANGE_NON_EXISTENT);
        } else {
            moqr_record_view_t object;
            memset(&object, 0, sizeof(object));
            object.group_id = it->group;
            object.subgroup_id = it->subgroup;
            object.object_id = it->object;
            object.publisher_priority = it->publisher_priority;
            object.datagram_pref = it->datagram;
            object.status = MOQR_OBJ_NORMAL;
            object.payload = it->payload;
            object.properties = it->properties;
            retrieve_object(c, &object);
        }
        return;
    }
    if (t != NULL && t->end == FW_END_COMPLETE) {
        moqr_retrieve_location_t end = retrieve_inclusive_end(t->ok_end_group,
                                                               t->ok_end_object);
        if (retrieve_loc_cmp(c->next, end) <= 0) {
            retrieve_gap(c, end, MOQ_FETCH_RANGE_NON_EXISTENT);
        } else {
            retrieve_gap(c, c->span_end, MOQ_FETCH_RANGE_UNKNOWN);
        }
    } else {
        retrieve_gap(c, c->span_end, MOQ_FETCH_RANGE_TIMED_OUT);
    }
}

moqr_result_t
moqr_bind_retrieve_peek(moqr_bind_t *b, moqr_retrieve_cursor_t *c,
                        uint64_t now_us, moqr_retrieve_item_t *out)
{
    if (b == NULL || c == NULL || out == NULL || !c->live) return MOQR_ERR_INVAL;
    if (c->pending && c->item.kind == MOQR_RETRIEVE_ERROR) {
        *out = c->item;
        return MOQR_OK;
    }
    b_fwd_t *t = retrieve_collector(b, c);
    if (t != NULL && t->end != FW_END_NONE && t->end != FW_END_COMPLETE &&
        t->end != FW_END_TIMEOUT) {
        /* Preserve the actual latched terminal, even if the parent/source
         * disappears later in the same pump. */
        retrieve_fwd_error(c, t);
        *out = c->item;
        return MOQR_OK;
    }
    if (!retrieve_parent_current(b, &c->request)) {
        retrieve_error(c, MOQR_RETRIEVE_SOURCE_LOST, MOQR_ERR_STALE_HANDLE);
    } else if (c->source_bound) {
        b_retrieve_source_t source;
        if (!retrieve_source(b, &c->request, true, c->source_cookie,
                              c->source_binding, c->source_parent, &source)) {
            retrieve_error(c, MOQR_RETRIEVE_SOURCE_LOST, MOQR_ERR_STALE_HANDLE);
        }
    }
    if ((!c->pending || c->item.kind != MOQR_RETRIEVE_ERROR) && t == NULL &&
        c->collector_slot != 0) {
        retrieve_error(c, MOQR_RETRIEVE_SOURCE_LOST, MOQR_ERR_STALE_HANDLE);
    }
    if (c->pending) {
        if (c->item.kind == MOQR_RETRIEVE_ERROR && t != NULL &&
            t->end == FW_END_NONE) {
            fwd_end_up(t, FW_END_LOST);
            b->budget_pending = true;
        }
        *out = c->item;
        return MOQR_OK;
    }
    if (c->done) {
        memset(&c->item, 0, sizeof(c->item));
        c->item.kind = MOQR_RETRIEVE_END;
        c->item.start = c->request.start;
        c->item.end = c->request.end;
        c->pending = true;
        *out = c->item;
        return MOQR_OK;
    }
    if (!c->span_active) {
        c->waiting_origin = false;
        retrieve_cache_kind_t kind;
        moqr_retrieve_location_t end;
        moqr_result_t rc = retrieve_cache_front(b, c, now_us, &kind, &end);
        if (rc == MOQR_ERR_WOULD_BLOCK) return rc;
        if (rc != MOQR_OK) {
            retrieve_error(c, MOQR_RETRIEVE_LOCAL, rc);
        } else if (kind == RETRIEVE_CACHE_OBJECT) {
            retrieve_cached_object(b, c);
        } else if (kind == RETRIEVE_CACHE_ABSENT) {
            retrieve_gap(c, end, MOQ_FETCH_RANGE_NON_EXISTENT);
        } else if (kind == RETRIEVE_CACHE_PENDING) {
            if (now_us < c->request.deadline_us) {
                /* Re-read on the next pump: it completes, or is abandoned. */
                retrieve_cache_close(b, c);
                c->waiting_origin = true;
                return MOQR_ERR_WOULD_BLOCK;
            }
            retrieve_gap(c, end, MOQ_FETCH_RANGE_TIMED_OUT);
        } else {
            c->span_active = true;
            c->span_end = end;
            c->span_refreshed = false;
        }
        if (c->pending) { *out = c->item; return MOQR_OK; }
    }
    t = retrieve_collector(b, c);
    if (t == NULL && c->collector_slot != 0) {
        retrieve_error(c, MOQR_RETRIEVE_SOURCE_LOST, MOQR_ERR_STALE_HANDLE);
    } else if (t == NULL && now_us < c->request.deadline_us) {
        moqr_result_t rc = retrieve_admit(b, c);
        if (rc == MOQR_ERR_WOULD_BLOCK && c->request.standalone) {
            /* No connection this relay could retrieve the run from: its
             * state cannot be determined (draft-22 3.2.2), which a FETCH
             * marks at once rather than wait for a source to appear. */
            retrieve_gap(c, c->span_end, MOQ_FETCH_RANGE_UNKNOWN);
            *out = c->item;
            return MOQR_OK;
        }
        if (rc == MOQR_ERR_WOULD_BLOCK) {
            /* No origin exists yet. No transaction has frozen this miss;
             * retire its cache peek so a newly complete local object can be
             * served on the next pump instead of waiting out the timer. */
            retrieve_cache_close(b, c);
            c->span_active = false;
            c->waiting_origin = true;
            return rc;
        }
        if (rc == MOQR_OK) return MOQR_ERR_WOULD_BLOCK;
        retrieve_error(c, rc == MOQR_ERR_STALE_HANDLE ? MOQR_RETRIEVE_SOURCE_LOST
                                                      : MOQR_RETRIEVE_LOCAL, rc);
    }
    if (c->pending) { *out = c->item; return MOQR_OK; }
    if (t != NULL) {
        /* Live coverage can commit before this FETCH delivers its copy. */
        while (t->next_write < t->n_items) {
            b_fwd_item_t *it = &t->items[t->next_write];
            if (retrieve_loc_cmp(retrieve_loc(it->group, it->object),
                                  c->next) >= 0) break;
            fwd_item_release(b, t, it);
            t->next_write++;
        }
        uint8_t before = t->u;
        retrieve_fwd_expire(t, now_us);
        if (before != FW_U_CANCELLING && t->u == FW_U_CANCELLING) {
            b->budget_pending = true;
        }
        if (t->end == FW_END_NONE) {
            /* No item is held here. Reopen the unresolved cache view at the
             * committed frontier without changing the admitted FETCH span. */
            retrieve_cache_close(b, c);
            retrieve_cache_kind_t kind;
            moqr_retrieve_location_t end;
            moqr_result_t rc = retrieve_cache_front(b, c, now_us, &kind, &end);
            if (rc == MOQR_ERR_WOULD_BLOCK) return rc;
            if (rc != MOQR_OK) {
                retrieve_error(c, MOQR_RETRIEVE_LOCAL, rc);
            } else if (kind == RETRIEVE_CACHE_OBJECT) {
                retrieve_cached_object(b, c);
            } else if (kind == RETRIEVE_CACHE_ABSENT) {
                retrieve_gap(c, retrieve_loc_min(end, c->span_end),
                              MOQ_FETCH_RANGE_NON_EXISTENT);
            } else {
                return MOQR_ERR_WOULD_BLOCK;
            }
            *out = c->item;
            return MOQR_OK;
        }
        if (t->end != FW_END_COMPLETE && t->end != FW_END_TIMEOUT) {
            retrieve_fwd_error(c, t);
            *out = c->item;
            return MOQR_OK;
        }
    }
    retrieve_collection_front(c, t);
    if (c->item.kind == MOQR_RETRIEVE_OBJECT) {
        /* The collector owns these bytes. Rebase the cache on the next peek,
         * without pinning a redundant cached object or retaining an old gap. */
        retrieve_cache_close(b, c);
    } else {
        /* Reconcile newly available cache data, without changing the admitted
         * span. Retire (not re-peek/commit) the old frozen core gap before
         * opening the same remaining coverage afresh. No extra core slot. */
        if (!c->span_refreshed) {
            retrieve_cache_close(b, c);
            c->span_refreshed = true;
        }
        retrieve_cache_kind_t kind;
        moqr_retrieve_location_t end;
        moqr_result_t rc = retrieve_cache_front(b, c, now_us, &kind, &end);
        if (rc == MOQR_ERR_WOULD_BLOCK) {
            c->pending = false;
            return rc;
        }
        if (rc != MOQR_OK) {
            retrieve_error(c, MOQR_RETRIEVE_LOCAL, rc);
        } else if (kind == RETRIEVE_CACHE_OBJECT &&
                   c->item.kind != MOQR_RETRIEVE_OBJECT) {
            retrieve_cached_object(b, c);
        } else if (c->item.kind == MOQR_RETRIEVE_GAP) {
            moqr_retrieve_location_t clipped = retrieve_loc_min(end, c->item.end);
            retrieve_gap(c, clipped, kind == RETRIEVE_CACHE_ABSENT
                                         ? MOQ_FETCH_RANGE_NON_EXISTENT
                                         : c->item.gap_kind);
        }
    }
    *out = c->item;
    return MOQR_OK;
}

/* Commit the frozen core item only after all of its coverage is accounted
 * for. Clipping a collector gap around a newly cached island must not skip
 * the rest of either item. END closes the capped cache plan, then reopens
 * the remaining original range on the next peek. */
static moqr_result_t
retrieve_cache_commit_through(moqr_bind_t *b, moqr_retrieve_cursor_t *c,
                              moqr_retrieve_location_t end)
{
    if (!c->cache_peeked) return MOQR_OK;
    moqr_retrieve_location_t held = retrieve_loc(c->cache_item.rec.group_id,
                                                 c->cache_item.rec.object_id);
    if (c->cache_item.kind == MOQR_FETCH_ITEM_MARKER) {
        held = retrieve_loc(c->cache_item.marker_group - 1u, UINT64_MAX);
    } else if (c->cache_item.kind == MOQR_FETCH_ITEM_END) {
        held = c->cache_end;
    }
    if (retrieve_loc_cmp(held, end) > 0) return MOQR_OK;
    if (c->cache_item.kind == MOQR_FETCH_ITEM_END) {
        retrieve_cache_close(b, c);
        return MOQR_OK;
    }
    moqr_result_t rc = moqr_core_fetch_commit(b->core, c->cache);
    if (rc == MOQR_OK) {
        c->cache_peeked = false;
        memset(&c->cache_item, 0, sizeof(c->cache_item));
        if (retrieve_loc_cmp(c->cache_end, end) <= 0) retrieve_cache_close(b, c);
    }
    return rc;
}

moqr_result_t
moqr_bind_retrieve_commit(moqr_bind_t *b, moqr_retrieve_cursor_t *c)
{
    if (b == NULL || c == NULL || !c->live || !c->pending) return MOQR_ERR_INVAL;
    if (c->item.kind == MOQR_RETRIEVE_ERROR) return MOQR_ERR_WRONG_STATE;
    if (c->item.kind == MOQR_RETRIEVE_END) return MOQR_OK;
    moqr_retrieve_location_t end = c->item.end;
    b_fwd_t *t = retrieve_collector(b, c);
    moqr_result_t rc = retrieve_cache_commit_through(b, c, end);
    if (rc != MOQR_OK) {
        retrieve_error(c, MOQR_RETRIEVE_LOCAL, rc);
        return rc;
    }
    if (t != NULL && t->next_write < t->n_items) {
        b_fwd_item_t *it = &t->items[t->next_write];
        if (retrieve_loc_cmp(retrieve_loc(it->group, it->object), end) <= 0) {
            fwd_item_release(b, t, it);
            t->next_write++;
        }
    }
    retrieve_cached_release(b, c);
    c->pending = false;
    if (c->span_active && retrieve_loc_cmp(c->span_end, end) <= 0) {
        retrieve_detach_collector(b, c);
        c->span_active = false;
        c->span_refreshed = false;
        c->cache_absent = false;
    }
    moqr_retrieve_location_t next;
    if (moqr_retrieve_after(c->request.start, c->request.end,
                            c->request.group_order, end, &next)) {
        c->next = next;
    } else {
        c->done = true;
    }
    memset(&c->item, 0, sizeof(c->item));
    return MOQR_OK;
}

void
moqr_bind_retrieve_close(moqr_bind_t *b, moqr_retrieve_cursor_t *c)
{
    if (b == NULL || c == NULL || !c->live) return;
    retrieve_detach_collector(b, c);
    retrieve_cache_close(b, c);
    retrieve_cached_release(b, c);
    memset(c, 0, sizeof(*c));
}

void
moqr_bind_retrieve_forget(moqr_retrieve_cursor_t *c)
{
    if (c != NULL) memset(c, 0, sizeof(*c));
}

uint64_t
moqr_bind_retrieve_next_deadline(const moqr_retrieve_cursor_t *c)
{
    /* An admitted collector's timer is bind-owned, including zero-credit
     * waits. Only the no-origin wait needs a cursor-owned timer here. */
    return c != NULL && c->live && !c->pending && !c->done && c->waiting_origin
               ? c->request.deadline_us : UINT64_MAX;
}

/* A connection is going away: every transaction naming it on either side
 * stops addressing its session. Idempotent (a deferred detach retries). */
static void
fwd_conn_gone(moqr_bind_t *b, const b_conn_t *cn)
{
    uint64_t cookie = conn_cookie(b, cn);
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        b_fwd_t *t = &b->fwd[i];
        if (!t->used) {
            continue;
        }
        if (t->u_cookie == cookie && moqr_binding_eq(t->u_binding, cn->binding) &&
            t->u != FW_U_TERMINAL) {
            if (t->end == FW_END_NONE) {
                t->end = FW_END_LOST;
            }
            t->u = FW_U_TERMINAL;      /* no cancel: the session is gone */
            t->ufetch_live = false;
            FWD_DBG(b, lost);
        }
        if (t->d_cookie == cookie && moqr_binding_eq(t->d_binding, cn->binding) &&
            t->d != FW_D_TERMINAL) {
            t->d = FW_D_TERMINAL;      /* nothing more may be written to it */
            fwd_end_up(t, FW_END_CANCELLED);
            FWD_DBG(b, down_lost);
        }
    }
}

/* Submit one refusal to the session: the reject call of its request family,
 * carrying the error code and, when set, the Retry Interval. */
static moq_result_t
bind_refusal_submit(b_conn_t *cn, const b_refusal_t *r, uint64_t now_us)
{
    switch (r->kind) {
    case MOQR_AUTH_FETCH: {
        moq_reject_fetch_cfg_t c;
        moq_reject_fetch_cfg_init(&c);
        c.error_code = r->error_code;
        c.can_retry = r->can_retry;
        c.retry_after_ms = r->retry_ms;
        return moq_session_reject_fetch(
            cn->session, (moq_fetch_t){ ._opaque = r->handle }, &c, now_us);
    }
    case MOQR_AUTH_PUBLISH_NAMESPACE: {
        moq_reject_namespace_cfg_t c;
        moq_reject_namespace_cfg_init(&c);
        c.error_code = r->error_code;
        c.can_retry = r->can_retry;
        c.retry_after_ms = r->retry_ms;
        return moq_session_reject_namespace(
            cn->session, (moq_announcement_t){ ._opaque = r->handle }, &c,
            now_us);
    }
    case MOQR_AUTH_SUBSCRIBE: {
        moq_reject_subscribe_cfg_t c;
        bind_reject_subscribe_cfg_init(&c, r->error_code);
        c.can_retry = r->can_retry;
        c.retry_after_ms = r->retry_ms;
        return moq_session_reject_subscribe(
            cn->session, (moq_subscription_t){ ._opaque = r->handle }, &c,
            now_us);
    }
    case MOQR_AUTH_SUBSCRIBE_NAMESPACE: {
        moq_reject_ns_sub_cfg_t c;
        moq_reject_ns_sub_cfg_init(&c);
        c.error_code = r->error_code;
        c.can_retry = r->can_retry;
        c.retry_after_ms = r->retry_ms;
        return moq_session_reject_ns_sub(
            cn->session, (moq_ns_sub_handle_t){ ._opaque = r->handle }, &c,
            now_us);
    }
    case MOQR_AUTH_TRACK_STATUS: {
        moq_reject_track_status_cfg_t c;
        moq_reject_track_status_cfg_init(&c);
        c.error_code = r->error_code;
        c.can_retry = r->can_retry;
        c.retry_after_ms = r->retry_ms;
        return moq_session_reject_track_status(
            cn->session, (moq_track_status_handle_t){ ._opaque = r->handle },
            &c, now_us);
    }
    case MOQR_AUTH_PUBLISH: {
        moq_reject_publish_cfg_t c;
        moq_reject_publish_cfg_init(&c);
        c.error_code = r->error_code;
        c.can_retry = r->can_retry;
        c.retry_after_ms = r->retry_ms;
        return moq_session_reject_publish(
            cn->session, (moq_publication_t){ ._opaque = r->handle }, &c,
            now_us);
    }
    case B_ACCEPT_NAMESPACE: {
        moq_accept_namespace_cfg_t c;
        moq_accept_namespace_cfg_init(&c);
        return moq_session_accept_namespace(
            cn->session, (moq_announcement_t){ ._opaque = r->handle }, &c,
            now_us);
    }
    case B_ACCEPT_NS_SUB: {
        moq_accept_ns_sub_cfg_t c;
        moq_accept_ns_sub_cfg_init(&c);
        return moq_session_accept_ns_sub(
            cn->session, (moq_ns_sub_handle_t){ ._opaque = r->handle }, &c,
            now_us);
    }
    case B_ABORT_FILL:
        return moq_session_abort_fill(
            cn->session, (moq_fill_t){ ._opaque = r->handle }, r->error_code,
            now_us);
    case B_REFUSE_SUBSCRIBE_TRACKS: {
        moq_reject_subscribe_tracks_cfg_t c;
        moq_reject_subscribe_tracks_cfg_init(&c);
        c.error_code = r->error_code;
        return moq_session_reject_subscribe_tracks(
            cn->session,
            (moq_track_sub_handle_t){ ._opaque = r->handle }, &c,
            now_us);
    }
    default:
        return MOQ_ERR_INVAL;
    }
}

/* Refuse a pending downstream request. A refusal the session cannot take yet
 * (WOULD_BLOCK) is held and retried every pump until it commits or the
 * request or connection goes away (b_refusal_t), so the request is always
 * answered. */
static void
bind_refuse(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t kind,
            uint64_t handle, uint64_t error_code, bool can_retry,
            uint64_t retry_ms, uint64_t now_us)
{
    b_refusal_t want = {
        .used = true,
        .can_retry = can_retry,
        .kind = kind,
        .handle = handle,
        .error_code = error_code,
        .retry_ms = can_retry ? retry_ms : 0,
    };
    moq_result_t rc = bind_refusal_submit(cn, &want, now_us);
    if (rc == MOQ_OK) {
        FWD_DBG(b, refusals_sent);
        return;
    }
    if (rc != MOQ_ERR_WOULD_BLOCK) {
        if (!fwd_result_is_gone(rc)) {
            b->session_errors++;
        }
        return;
    }
    FWD_DBG(b, refusals_blocked);
    bind_terminal_hold(b, cn, &want);
}

/* Hold a terminal the session could not take yet in a free entry. */
static void
bind_terminal_hold(moqr_bind_t *b, b_conn_t *cn, const b_refusal_t *want)
{
    for (uint32_t i = 0; i < b->n_refusals; i++) {
        b_refusal_t *r = &cn->refusals[i];
        if (!r->used) {
            *r = *want;
            FWD_DBG(b, refusals_parked);
            return;
        }
    }
    /* Unreachable: the table is sized for every terminal the binding can
     * owe (b_refusal_t). Counted so a regression is visible. */
    b->session_errors++;
    FWD_DBG(b, refusals_unparked);
}

static bool
bind_kind_is_accept(moqr_auth_action_t kind)
{
    return kind == B_ACCEPT_NAMESPACE || kind == B_ACCEPT_NS_SUB;
}

/* Undo the core state of an acceptance whose request is gone before the
 * acceptance committed: the announce (with its binding copy) or the
 * namespace subscription. True when done (or nothing was left to undo);
 * false when the unannounce must wait for intent room (retried next pump). */
static bool
bind_accept_undo(moqr_bind_t *b, b_conn_t *cn, const b_refusal_t *r,
                 uint64_t now_us)
{
    /* The acceptance never stood: no tokens are retained for it. */
    moqr_core_request_tokens_clear(b->core, cn->binding, r->handle);
    if (r->kind == B_ACCEPT_NS_SUB) {
        /* No new NAMESPACE for it, and none of those already parked behind
         * its acceptance may reach the session. */
        (void)moqr_core_ns_unsubscribe(b->core, cn->binding, r->handle);
        bind_pending_purge(b, (uint64_t)(cn - b->conns) + 1u, true,
                           r->handle);
        FWD_DBG(b, accepts_undone);
        return true;
    }
    moqr_result_t urc = bind_ann_withdraw(b, cn, r->handle, now_us);
    if (urc == MOQR_ERR_WOULD_BLOCK) {
        return false;
    }
    FWD_DBG(b, accepts_undone);
    return true;
}

/* Answer a namespace or namespace subscription the core has admitted. An
 * acceptance the session cannot take yet is held like a refusal; one whose
 * request is already gone undoes the admission. */
static void
bind_accept(moqr_bind_t *b, b_conn_t *cn, moqr_auth_action_t kind,
            uint64_t handle, uint64_t now_us)
{
    b_refusal_t want = { .used = true, .kind = kind, .handle = handle };
    moq_result_t rc = bind_refusal_submit(cn, &want, now_us);
    if (rc == MOQ_OK) {
        FWD_DBG(b, accepts_sent);
        return;
    }
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        FWD_DBG(b, accepts_blocked);
        bind_terminal_hold(b, cn, &want);
        return;
    }
    if (!fwd_result_is_gone(rc)) {
        b->session_errors++;
        return;
    }
    want.undo = true;
    if (!bind_accept_undo(b, cn, &want, now_us)) {
        bind_terminal_hold(b, cn, &want);
    }
}

/* Whether a namespace subscription's acceptance is still held: no NAMESPACE
 * may be sent on it before its REQUEST_OK. */
static bool
bind_ns_sub_accept_held(const moqr_bind_t *b, const b_conn_t *cn,
                        uint64_t handle)
{
    for (uint32_t i = 0; i < b->n_refusals; i++) {
        const b_refusal_t *r = &cn->refusals[i];
        if (r->used && !r->undo && r->kind == B_ACCEPT_NS_SUB &&
            r->handle == handle) {
            return true;
        }
    }
    return false;
}

/* Refuse a downstream FETCH request before any transaction exists. */
static void
bind_fetch_refuse(moqr_bind_t *b, b_conn_t *cn, moq_fetch_t fetch,
                  uint64_t error_code, uint64_t now_us)
{
    bind_refuse(b, cn, MOQR_AUTH_FETCH, fetch._opaque, error_code, false, 0,
                now_us);
}

/* Free entries not promised to a parked authorization. */
static uint32_t
bind_refusal_free(const moqr_bind_t *b, const b_conn_t *cn)
{
    uint32_t free_slots = 0;
    for (uint32_t i = 0; i < b->n_refusals; i++) {
        free_slots += cn->refusals[i].used ? 0u : 1u;
    }
    return free_slots > cn->refusals_reserved
               ? free_slots - cn->refusals_reserved
               : 0u;
}

/* Whether the pump may poll another event from this connection: a request
 * it polls must be ownable, so a poll's worth of unreserved refusal entries
 * has to be free first, and as many metadata publish stages (a consumed
 * PUBLISH owns one even when its first refusal blocks). */
static bool
bind_refusal_can_poll(const moqr_bind_t *b, const b_conn_t *cn)
{
    if (bind_refusal_free(b, cn) < BIND_PUMP_EVENT_POLL) {
        return false;
    }
    uint32_t free_slots = 0;
    for (uint32_t i = 0; i < b->metadata_publish_cap; i++) {
        if (b->metadata_publishes[i].state == 0 &&
            ++free_slots >= BIND_PUMP_EVENT_POLL) {
            return true;
        }
    }
    return false;
}

/* Retry every held refusal on every open connection (one attempt each per
 * pump); a request the session no longer holds is simply forgotten. */
static void
bind_refusal_pump(moqr_bind_t *b, uint64_t now_us)
{
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        b_conn_t *cn = &b->conns[ci];
        if (!cn->used || cn->closed || cn->session == NULL) {
            continue;
        }
        for (uint32_t i = 0; i < b->n_refusals; i++) {
            b_refusal_t *r = &cn->refusals[i];
            if (!r->used) {
                continue;
            }
            if (r->undo) {
                if (bind_accept_undo(b, cn, r, now_us)) {
                    memset(r, 0, sizeof(*r));
                }
                continue;
            }
            bool accept = bind_kind_is_accept(r->kind);
            moq_result_t rc;
#ifdef MOQR_BIND_TESTING
            if (accept && atomic_load(&bind_dbg_accept_gone) > 0 &&
                atomic_fetch_sub(&bind_dbg_accept_gone, 1) == 1) {
                rc = MOQ_ERR_STALE_HANDLE;
            } else
#endif
            {
                rc = bind_refusal_submit(cn, r, now_us);
            }
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                FWD_DBG(b, refusals_retried);   /* HOLD */
                continue;
            }
            if (rc == MOQ_OK) {
                if (accept) {
                    FWD_DBG(b, accepts_sent);
                } else {
                    FWD_DBG(b, refusals_sent);
                }
            } else if (fwd_result_is_gone(rc)) {
                FWD_DBG(b, refusals_gone);   /* retired by the peer meanwhile */
                if (accept) {
                    r->undo = true;
                    if (!bind_accept_undo(b, cn, r, now_us)) {
                        continue;   /* undo blocked: retried next pump */
                    }
                }
            } else {
                b->session_errors++;
            }
            memset(r, 0, sizeof(*r));
        }
    }
}

/* The peer cancelled a request whose refusal is still held: nothing is owed. */
static bool
bind_refusal_drop(moqr_bind_t *b, b_conn_t *cn, uint64_t handle)
{
    for (uint32_t i = 0; i < b->n_refusals; i++) {
        b_refusal_t *r = &cn->refusals[i];
        if (r->used && r->kind == MOQR_AUTH_FETCH && r->handle == handle) {
            memset(r, 0, sizeof(*r));
            FWD_DBG(b, refusals_dropped);
            return true;
        }
    }
    return false;
}

/* -- fill fetch streams --------------------------------------------------------- *
 * A subscription that asked for a fill is owed a fill fetch stream: the
 * FETCH response for the fill range, opened by the publisher. The relay
 * answers it from the same retrieval cursor that serves a FETCH over that
 * range, written through the session's fill family. The cursor serves cached
 * Objects and retrieves missing runs upstream, within the fill's deadline:
 * its processing time plus the FILL_TIMEOUT it carried (capped at the relay's
 * maximum) or, absent one, the relay's own wait. Past the deadline each run
 * is written as End of Timed-Out Range at its last Location and the fill
 * continues; a timeout of zero therefore serves what is held and marks the
 * rest at once. The stream ends normally after the range end. Descending
 * fills visit one group window at a time, newest first, without reversing
 * Object order inside a group.
 *
 * The same holds when the log of the shard serving the fill holds nothing of
 * the range when the request arrives: the Largest Object the range is bounded
 * by can be known (an upstream's or another shard's acceptance carries it)
 * before any Object of the range is retained here. The stream is opened with
 * its header at once and the cursor retrieves that missing range too. If no
 * source is available, it retries until the deadline; unserved windows are
 * then marked Timed-Out, followed by FIN.
 * Ending it without a marker would claim the range holds no Objects
 * (Section 3.2.2 of draft-ietf-moq-transport-22), and resetting it would
 * give up before the wait Section 9.20.5 allows.
 *
 * A multi-shard runtime may install a fill source (moqr_bind_set_fill_source)
 * for the part of a range this shard's log cannot hold because another shard
 * owns the track and this shard's feed of it started later. That part is
 * read through the source with the same item contract and the same deadline
 * handling, and whatever follows it is served from the log as above.
 *
 * An upstream failure leaves the Object state unknown and resets the fill
 * with UNKNOWN_OBJECT_STATUS; local admission or retrieval failures use
 * INTERNAL_ERROR. Neither failure touches the subscription. */

/* Both standalone FETCH and subscription fills use the same retrieval
 * outcomes: an upstream failure leaves Object status unknown; local failures
 * are this relay's own. */
static moqr_reset_cause_t
bind_retrieve_error_cause(const moqr_retrieve_item_t *it)
{
    switch (it->error_kind) {
    case MOQR_RETRIEVE_RESPONSE:
    case MOQR_RETRIEVE_RESET:
    case MOQR_RETRIEVE_SOURCE_LOST:
    case MOQR_RETRIEVE_MALFORMED:
        return MOQR_RESET_UNKNOWN_OBJECT_STATUS;
    default:
        return MOQR_RESET_INTERNAL_ERROR;
    }
}

static uint64_t
bind_retrieve_reset_code(const moqr_bind_t *b, const b_conn_t *cn,
                         moqr_reset_cause_t cause)
{
    uint64_t code = BIND_RESET_INTERNAL;
    (void)moqr_reset_encode(moqr_bind_conn_version(b, cn->session), cause, &code);
    return code;
}

static uint64_t
bind_fill_fail_code(const moqr_bind_t *b, const b_conn_t *cn)
{
    return bind_retrieve_reset_code(b, cn, MOQR_RESET_INTERNAL_ERROR);
}

/* Fail a fill on the wire. The reset is an obligation: a WOULD_BLOCK holds it
 * in the refusal table (B_ABORT_FILL; the poll that delivered the fill
 * request guaranteed a free entry) and the refusal pump retries it until it
 * commits or the child is gone. */
static void
bind_fill_fail(moqr_bind_t *b, b_conn_t *cn, moq_fill_t fill, uint64_t now_us)
{
    b_refusal_t want = {
        .used = true,
        .kind = B_ABORT_FILL,
        .handle = fill._opaque,
        .error_code = bind_fill_fail_code(b, cn),
    };
    moq_result_t rc = bind_refusal_submit(cn, &want, now_us);
    if (rc == MOQ_OK) {
        FWD_DBG(b, refusals_sent);
        return;
    }
    if (rc != MOQ_ERR_WOULD_BLOCK) {
        if (!fwd_result_is_gone(rc)) {
            b->session_errors++;
        }
        return;
    }
    FWD_DBG(b, refusals_blocked);
    bind_terminal_hold(b, cn, &want);
}

/* Release a tracked fill: retire its cursor and free the slot. */
static void
bind_fill_release(moqr_bind_t *b, b_fetch_t *bf)
{
    moqr_bind_retrieve_close(b, &bf->retrieval);
    if (moqr_fetch_is_valid(bf->cfetch)) {
        (void)moqr_core_fetch_close(b->core, bf->cfetch);
    }
    if (bf->fill_src_open) {
        b->fill_src.close(b->fill_src.ctx, bf->fill_src_handle);
    }
    if (bf->sa_remote) {
        b->sa_src.close(b->sa_src.ctx, bf->sa_handle);
    }
    memset(bf, 0, sizeof(*bf));
}

/* The instant a fill (or a FETCH) stops waiting for upstream: its processing
 * time plus the wait the request selects (a carried FILL_TIMEOUT shortened to
 * the relay's maximum, else the relay's default), saturating. */
static uint64_t
bind_wait_deadline(const moqr_bind_t *b, uint64_t processed_at_us,
                   bool has_fill_timeout, uint64_t fill_timeout_ms)
{
    uint64_t wait_us = b->fill_wait_default_us;
    if (has_fill_timeout) {
        wait_us = fill_timeout_ms > b->fill_wait_max_us / 1000u
                      ? b->fill_wait_max_us
                      : fill_timeout_ms * 1000u;
    }
    return processed_at_us > UINT64_MAX - wait_us ? UINT64_MAX
                                                  : processed_at_us + wait_us;
}

static uint64_t
bind_fill_deadline(const moqr_bind_t *b, const moq_fill_request_event_t *fr)
{
    return bind_wait_deadline(b, fr->processed_at_us, fr->has_fill_timeout,
                              fr->fill_timeout_ms);
}

/* Claim the external source or initialize the shared local retrieval cursor.
 * NOT_HELD is only the external source's transient open refusal. An empty
 * local cache is handled by the shared cursor, not a cache-only timeout. */
typedef enum bind_fill_open {
    BIND_FILL_OPENED,
    BIND_FILL_NOT_HELD,
    BIND_FILL_FAILED,
} bind_fill_open_t;

static bind_fill_open_t
bind_fill_cursor_open(moqr_bind_t *b, b_conn_t *cn, b_fetch_t *bf,
                      uint64_t now_us)
{
    moqr_track_t track = MOQR_TRACK_INVALID;
    uint64_t track_gen = 0;
    if (moqr_core_sub_track(b->core, bf->fill_rsub, &track, &track_gen) !=
            MOQR_OK ||
        !moqr_track_eq(track, bf->fill_track) || track_gen != bf->fill_track_gen) {
        return BIND_FILL_FAILED;
    }
    if (b->has_fill_src && !bf->fill_src_used) {
        /* The source claims the part of the range this shard's log cannot
         * hold, if any; the log serves whatever follows it. */
        moqr_bind_fill_query_t q;
        memset(&q, 0, sizeof(q));
        q.struct_size = (uint32_t)sizeof(q);
        q.track = track;
        q.track_gen = track_gen;
        q.start_group = bf->fill_start_group;
        q.start_object = bf->fill_start_object;
        q.end_group = bf->fill_end_group;
        q.end_object = bf->fill_end_object;
        /* Descending fills visit one group at a time. A source's historical
         * prefix must precede the local suffix WITHIN that group. */
        q.group_order = MOQR_GROUP_ORDER_ASCENDING;
        q.subscriber_priority = bf->fill_priority;
        q.deadline_us = bf->fill_deadline_us;
        moqr_bind_fill_claim_t claim;
        memset(&claim, 0, sizeof(claim));
        moqr_result_t sr = b->fill_src.open(b->fill_src.ctx, &q, &claim);
        if (sr == MOQR_OK) {
            moqr_retrieve_location_t end = retrieve_loc(claim.end_group,
                                                         claim.end_object);
            if (retrieve_loc_cmp(end, retrieve_loc(q.start_group, q.start_object)) < 0 ||
                retrieve_loc_cmp(end, retrieve_loc(q.end_group, q.end_object)) > 0) {
                b->fill_src.close(b->fill_src.ctx, claim.handle);
                return BIND_FILL_FAILED;
            }
            bf->fill_src_open = true;
            bf->fill_src_handle = claim.handle;
            bf->fill_src_end_group = claim.end_group;
            bf->fill_src_end_object = claim.end_object;
            return BIND_FILL_OPENED;
        }
        if (sr == MOQR_ERR_WOULD_BLOCK) {
            return BIND_FILL_NOT_HELD;
        }
        if (sr != MOQR_DONE) return BIND_FILL_FAILED;
        bf->fill_src_used = true;   /* not the source's: the log's alone */
    }
    moqr_retrieve_request_t req;
    memset(&req, 0, sizeof(req));
    req.binding = cn->binding;
    req.parent = bf->fill_rsub;
    req.track = bf->fill_track;
    req.track_gen = bf->fill_track_gen;
    req.source = conn_cookie(b, cn);
    req.demand = bf->fill_rsub._opaque;
    req.child = bf->sfill._opaque;
    req.start = retrieve_loc(bf->fill_start_group, bf->fill_start_object);
    req.end = retrieve_loc(bf->fill_end_group, bf->fill_end_object);
    req.group_order = MOQR_GROUP_ORDER_ASCENDING;
    req.subscriber_priority = bf->fill_priority;
    req.deadline_us = bf->fill_deadline_us;
    (void)now_us;
    return moqr_bind_retrieve_init(b, &bf->retrieval, &req) == MOQR_OK
               ? BIND_FILL_OPENED : BIND_FILL_FAILED;
}

/* Whether the fill has its shared local cursor or a fill-source claim. */
static bool
bind_fill_has_reader(const b_fetch_t *bf)
{
    return bf->fill_src_open || bf->retrieval.live;
}

static moqr_result_t
bind_fill_peek(moqr_bind_t *b, b_fetch_t *bf, uint64_t now_us,
               moqr_fetch_item_t *it)
{
    if (!bf->fill_src_open) return MOQR_ERR_WRONG_STATE;
    memset(it, 0, sizeof(*it));
    return b->fill_src.peek(b->fill_src.ctx, bf->fill_src_handle, now_us, it);
}

static moqr_result_t
bind_fill_commit(moqr_bind_t *b, b_fetch_t *bf)
{
    if (bf->fill_src_open) {
        return b->fill_src.commit(b->fill_src.ctx, bf->fill_src_handle);
    }
    return moqr_bind_retrieve_commit(b, &bf->retrieval);
}

/* The fill source has served its claimed part. The claim ends; a range that
 * continues past it is the log's from the Location after the claimed end.
 * Returns true when the whole current window is done. */
static bool
bind_fill_source_done(moqr_bind_t *b, b_fetch_t *bf)
{
    b->fill_src.close(b->fill_src.ctx, bf->fill_src_handle);
    bf->fill_src_open = false;
    bf->fill_src_used = true;
    bf->response_done = false;
    uint64_t eg = bf->fill_src_end_group;
    uint64_t eo = bf->fill_src_end_object;
    if (eg > bf->fill_end_group ||
        (eg == bf->fill_end_group &&
         (eo >= bf->fill_end_object || eo == UINT64_MAX))) {
        return true;
    }
    if (eo == UINT64_MAX) {
        bf->fill_start_group = eg + 1u;
        bf->fill_start_object = 0;
    } else {
        bf->fill_start_group = eg;
        bf->fill_start_object = eo + 1u;
    }
    bf->response_next = retrieve_loc(bf->fill_start_group, bf->fill_start_object);
    return false;
}

/* The current group window is covered. Descending orders groups only: each
 * reader and source claim within one window still runs in Object order. */
static void
bind_fill_window_done(moqr_bind_t *b, b_fetch_t *bf)
{
    moqr_bind_retrieve_close(b, &bf->retrieval);
    moqr_retrieve_location_t next;
    moqr_retrieve_location_t end = retrieve_loc(bf->fill_end_group,
                                                 bf->fill_end_object);
    if (moqr_retrieve_after(bf->response_start, bf->response_end, bf->response_order, end, &next)) {
        moqr_retrieve_location_t window = moqr_retrieve_window_end(
            bf->response_end, bf->response_order, next.group);
        bf->fill_start_group = next.group;
        bf->fill_start_object = next.object;
        bf->fill_end_group = window.group;
        bf->fill_end_object = window.object;
        bf->fill_src_used = false;
        bf->response_next = next;
        bf->response_done = false;
    } else {
        bf->response_ne_pending = false;
        bf->state = BF_NEEDS_FIN;
    }
}

/* Unmarked runs are non-existent by default. Retain their end only when a
 * subsequent marked run needs to distinguish the two, as for standalone
 * FETCH. A following Object or final FIN needs no explicit marker. */
static moq_result_t
bind_fill_write_range(b_conn_t *cn, b_fetch_t *bf,
                      moq_fetch_range_kind_t kind,
                      moqr_retrieve_location_t end, uint64_t now_us)
{
    if (kind == MOQ_FETCH_RANGE_NON_EXISTENT) return MOQ_OK;
    if (bf->response_ne_pending) {
        moq_result_t rc = moq_session_write_fill_range(cn->session, bf->sfill,
            MOQ_FETCH_RANGE_NON_EXISTENT, bf->response_ne_end.group,
            bf->response_ne_end.object, now_us);
        if (rc != MOQ_OK) return rc;
        bf->response_ne_pending = false;
    }
    return moq_session_write_fill_range(cn->session, bf->sfill, kind,
                                         end.group, end.object, now_us);
}

static void
bind_fill_request(moqr_bind_t *b, b_conn_t *cn,
                  const moq_fill_request_event_t *fr, uint64_t now_us)
{
    moqr_sub_t rsub = MOQR_SUB_INVALID;
    bool tracked = false;
    /* The fill's parent: a SUBSCRIBE's subscription, or a publication the
     * relay opened for track discovery. */
    uint64_t parent = fr->sub._opaque != 0 ? fr->sub._opaque : fr->pub._opaque;
    for (uint32_t i = 0; parent != 0 && i < b->n_dsubs; i++) {
        if (cn->dsubs[i].used && cn->dsubs[i].sub_raw == parent) {
            rsub = cn->dsubs[i].rsub;
            tracked = true;
            break;
        }
    }
    int fi = -1;
    for (uint32_t i = 0; tracked && i < b->n_fetches; i++) {
        if (!cn->fetches[i].used) {
            fi = (int)i;
            break;
        }
    }
    if (fi < 0) {
        bind_fill_fail(b, cn, fr->fill, now_us);
        return;
    }
    /* The fill range is inclusive and already bounded by Largest Object. */
    b_fetch_t *bf = &cn->fetches[fi];
    memset(bf, 0, sizeof(*bf));
    bf->fill = true;
    bf->state = BF_FILL_NEEDS_OPEN;
    bf->sfill = fr->fill;
    bf->cfetch = MOQR_FETCH_INVALID;
    bf->fill_deadline_us = bind_fill_deadline(b, fr);
    bf->fill_rsub = rsub;
    if (moqr_core_sub_track(b->core, rsub, &bf->fill_track,
                             &bf->fill_track_gen) != MOQR_OK) {
        memset(bf, 0, sizeof(*bf));
        bind_fill_fail(b, cn, fr->fill, now_us);
        return;
    }
    bf->fill_start_group = fr->start_group;
    bf->fill_start_object = fr->start_object;
    bf->fill_end_group = fr->end_group;
    bf->fill_end_object = fr->end_object;
    bf->fill_priority = fr->subscriber_priority;
    bf->response_start = retrieve_loc(fr->start_group, fr->start_object);
    bf->response_end = retrieve_loc(fr->end_group, fr->end_object);
    bf->response_order = (moqr_group_order_t)fr->group_order;
    bf->response_next = moqr_retrieve_first(bf->response_start, bf->response_end, bf->response_order);
    moqr_retrieve_location_t window = moqr_retrieve_window_end(
        bf->response_end, bf->response_order, bf->response_next.group);
    bf->fill_start_group = bf->response_next.group;
    bf->fill_start_object = bf->response_next.object;
    bf->fill_end_group = window.group;
    bf->fill_end_object = window.object;
    if (bind_fill_cursor_open(b, cn, bf, now_us) == BIND_FILL_FAILED) {
        memset(bf, 0, sizeof(*bf));
        bind_fill_fail(b, cn, fr->fill, now_us);
        return;
    }
    bf->used = true;
}

/* The child ended on the session side (its FIN or RESET committed, the peer
 * stopped it, or its subscription went away): nothing more is owed on it. */
static void
bind_fill_terminal(moqr_bind_t *b, b_conn_t *cn, moq_fill_t fill)
{
    for (uint32_t i = 0; i < b->n_fetches; i++) {
        b_fetch_t *bf = &cn->fetches[i];
        if (bf->used && bf->fill && bf->sfill._opaque == fill._opaque) {
            bind_fill_release(b, bf);
        }
    }
    for (uint32_t i = 0; i < b->n_refusals; i++) {
        b_refusal_t *r = &cn->refusals[i];
        if (r->used && r->kind == B_ABORT_FILL && r->handle == fill._opaque) {
            memset(r, 0, sizeof(*r));
            FWD_DBG(b, refusals_dropped);
        }
    }
}

/* The same writer consumes the local shared cursor and the built-in shard's
 * hidden typed adjunct. External public-only sources retain their documented
 * cache-item interpretation below. ERROR is always a child RESET, never END. */
static bool
bind_fill_retrieve_step(moqr_bind_t *b, b_conn_t *cn, b_fetch_t *bf,
                        uint64_t now_us)
{
    moqr_retrieve_item_t it;
    moqr_result_t prc = bf->fill_src_open
        ? b->retrieve_source_peek(b->fill_src.ctx, bf->fill_src_handle, now_us, &it)
        : moqr_bind_retrieve_peek(b, &bf->retrieval, now_us, &it);
    if (prc == MOQR_ERR_WOULD_BLOCK) return false;
    if (prc != MOQR_OK) {
        bf->state = BF_FILL_NEEDS_ABORT;
        return true;
    }
    if (it.kind == MOQR_RETRIEVE_ERROR) {
        bf->fill_abort_code = bind_retrieve_reset_code(b, cn,
            bind_retrieve_error_cause(&it));
        bf->fill_abort_code_set = true;
        bf->state = BF_FILL_NEEDS_ABORT;
        return true;
    }
    if (it.kind == MOQR_RETRIEVE_END) {
        moqr_retrieve_location_t end = bf->fill_src_open
            ? retrieve_loc(bf->fill_src_end_group, bf->fill_src_end_object)
            : retrieve_loc(bf->fill_end_group, bf->fill_end_object);
        if (!bf->response_done || retrieve_loc_cmp(it.end, end) != 0) {
            bf->state = BF_FILL_NEEDS_ABORT;
            return true;
        }
        if (bf->fill_src_open && !bind_fill_source_done(b, bf)) return true;
        bind_fill_window_done(b, bf);
        return true;
    }
    moqr_retrieve_location_t start = retrieve_loc(bf->fill_start_group,
                                                   bf->fill_start_object);
    moqr_retrieve_location_t end = bf->fill_src_open
        ? retrieve_loc(bf->fill_src_end_group, bf->fill_src_end_object)
        : retrieve_loc(bf->fill_end_group, bf->fill_end_object);
    if (bf->response_done || retrieve_loc_cmp(it.start, bf->response_next) != 0 ||
        retrieve_loc_cmp(it.start, start) != 0 ||
        retrieve_loc_cmp(it.end, start) < 0 || retrieve_loc_cmp(it.end, end) > 0) {
        bf->state = BF_FILL_NEEDS_ABORT;
        return true;
    }
    moq_result_t rc;
    if (it.kind == MOQR_RETRIEVE_GAP) {
        rc = bind_fill_write_range(cn, bf, it.gap_kind, it.end, now_us);
    } else if (it.kind == MOQR_RETRIEVE_OBJECT) {
        if (retrieve_loc_cmp(it.end, start) != 0 ||
            it.object.group_id != it.end.group ||
            it.object.object_id != it.end.object) {
            bf->state = BF_FILL_NEEDS_ABORT;
            return true;
        }
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.group_id = it.object.group_id;
        oc.subgroup_id = it.object.subgroup_id;
        oc.object_id = it.object.object_id;
        oc.publisher_priority = it.object.publisher_priority;
        oc.datagram = moq_session_supports_fetch_datagram(cn->session)
                          ? it.object.datagram_pref : false;
        oc.payload = (moq_rcbuf_t *)(void *)it.object.payload;
        oc.properties = (moq_rcbuf_t *)(void *)it.object.properties;
        rc = moq_session_write_fill_object(cn->session, bf->sfill, &oc, now_us);
    } else {
        bf->state = BF_FILL_NEEDS_ABORT;
        return true;
    }
    if (rc == MOQ_ERR_WOULD_BLOCK) return false;
    if (rc != MOQ_OK) {
        bf->state = BF_FILL_NEEDS_ABORT;
        return true;
    }
    if (it.kind == MOQR_RETRIEVE_OBJECT) {
        bf->response_ne_pending = false;
        b->deliveries_written++;
    } else if (it.gap_kind == MOQ_FETCH_RANGE_NON_EXISTENT) {
        bf->response_ne_pending = true;
        bf->response_ne_end = it.end;
    }
    if (bind_fill_commit(b, bf) != MOQR_OK) {
        bf->state = BF_FILL_NEEDS_ABORT;
        return true;
    }
    moqr_retrieve_location_t next;
    if (moqr_retrieve_after(start, end, MOQR_GROUP_ORDER_ASCENDING, it.end, &next)) {
        bf->response_next = next;
        bf->fill_start_group = next.group;
        bf->fill_start_object = next.object;
    } else {
        bf->response_done = true;
    }
    return true;
}

/* Drive one fill as far as the session allows; false means HOLD (retry next
 * pump, no state advanced). A fill never terminates the connection: anything
 * that goes wrong mid-stream is answered on its own stream with a RESET. */
static bool
bind_fill_step(moqr_bind_t *b, b_conn_t *cn, b_fetch_t *bf, uint64_t now_us)
{
    moq_result_t rc;
    switch (bf->state) {
    case BF_FILL_NEEDS_OPEN:
        rc = moq_session_open_fill(cn->session, bf->sfill, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            return false;
        }
        if (rc != MOQ_OK) {
            if (!fwd_result_is_gone(rc)) {
                b->session_errors++;
            }
            bind_fill_release(b, bf);
            return true;
        }
        bf->state = BF_STREAMING;
        return true;
    case BF_STREAMING: {
        bind_fill_open_t opened = bind_fill_has_reader(bf) ? BIND_FILL_OPENED
            : bind_fill_cursor_open(b, cn, bf, now_us);
        if (opened == BIND_FILL_FAILED) {
            bf->state = BF_FILL_NEEDS_ABORT;
            return true;
        }
        if (opened == BIND_FILL_NOT_HELD) {
            /* The source cannot claim this window yet. Wait only until
             * the original deadline, then classify this window and move
             * to the next group in the fill's delivery order. */
            if (now_us < bf->fill_deadline_us) {
                return false;
            }
            rc = bind_fill_write_range(cn, bf, MOQ_FETCH_RANGE_TIMED_OUT,
                retrieve_loc(bf->fill_end_group, bf->fill_end_object), now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                return false;
            }
            if (rc == MOQ_OK) bind_fill_window_done(b, bf);
            else bf->state = BF_FILL_NEEDS_ABORT;
            return true;
        }
        if (!bf->fill_src_open || b->retrieve_source_peek != NULL) {
            return bind_fill_retrieve_step(b, cn, bf, now_us);
        }
        moqr_fetch_item_t it;
        moqr_result_t prc = bind_fill_peek(b, bf, now_us, &it);
        if (prc == MOQR_ERR_WOULD_BLOCK) {
            /* pin budget momentarily full, or the source's part not here
             * yet */
            return false;
        }
        if (prc != MOQR_OK) {
            bf->state = BF_FILL_NEEDS_ABORT;   /* range lost mid-fill */
            return true;
        }
        if (it.kind == MOQR_FETCH_ITEM_END) {
            if (bf->fill_src_open && !bind_fill_source_done(b, bf)) {
                return true;   /* the rest of the range is the log's */
            }
            bind_fill_window_done(b, bf);
            return true;
        }
        if (it.kind == MOQR_FETCH_ITEM_GAP ||
            it.kind == MOQR_FETCH_ITEM_PENDING) {
            /* Locations the log does not hold, or an Object still arriving.
             * Upstream may still deliver them, so the cursor waits (the
             * re-peek recomputes the run as objects arrive, and serves the
             * Object once it is whole) until the fill's deadline; then the
             * run is one End of Timed-Out Range at its last Location. */
            if (now_us < bf->fill_deadline_us) {
                return false;
            }
            rc = bind_fill_write_range(cn, bf, MOQ_FETCH_RANGE_TIMED_OUT,
                retrieve_loc(it.rec.group_id, it.rec.object_id), now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                return false;
            }
            if (rc != MOQ_OK) {
                bf->state = BF_FILL_NEEDS_ABORT;
                return true;
            }
            if (bind_fill_commit(b, bf) != MOQR_OK) bf->state = BF_FILL_NEEDS_ABORT;
            return true;
        }
        if (it.kind == MOQR_FETCH_ITEM_UNKNOWN ||
            it.kind == MOQR_FETCH_ITEM_MARKER) {
            /* Locations the relay has no evidence about (a run, or the
             * evicted prefix: through the last object of the group before
             * the first retained one) and no source to wait on: marked at
             * once, Unknown while the fill's wait budget lasts and Timed-Out
             * once it is spent -- at once for a FILL_TIMEOUT of 0. The fill
             * profile's Location is a vi64, so UINT64_MAX is a group's last
             * object id. */
            uint64_t mg = it.rec.group_id, mo = it.rec.object_id;
            if (it.kind == MOQR_FETCH_ITEM_MARKER) {
                if (it.marker_group == 0) {
                    if (bind_fill_commit(b, bf) != MOQR_OK) {
                        bf->state = BF_FILL_NEEDS_ABORT;
                    }
                    return true;
                }
                mg = it.marker_group - 1u;
                mo = UINT64_MAX;
            }
            rc = bind_fill_write_range(cn, bf,
                bind_fill_unserved_kind(now_us >= bf->fill_deadline_us),
                retrieve_loc(mg, mo), now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                return false;
            }
            if (rc != MOQ_OK) {
                bf->state = BF_FILL_NEEDS_ABORT;
                return true;
            }
            if (bind_fill_commit(b, bf) != MOQR_OK) bf->state = BF_FILL_NEEDS_ABORT;
            return true;
        }
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.group_id = it.rec.group_id;
        oc.subgroup_id = it.rec.subgroup_id;
        oc.object_id = it.rec.object_id;
        oc.publisher_priority = it.rec.publisher_priority;
        oc.datagram = moq_session_supports_fetch_datagram(cn->session)
                          ? it.rec.datagram_pref
                          : false;
        oc.payload = (moq_rcbuf_t *)(void *)it.rec.payload;
        oc.properties = (moq_rcbuf_t *)(void *)it.rec.properties;
        rc = moq_session_write_fill_object(cn->session, bf->sfill, &oc, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            return false;   /* HOLD: not committed; the re-peek re-emits it */
        }
        if (rc != MOQ_OK) {
            bf->state = BF_FILL_NEEDS_ABORT;
            return true;
        }
        bf->response_ne_pending = false;
        if (bind_fill_commit(b, bf) != MOQR_OK) bf->state = BF_FILL_NEEDS_ABORT;
        b->deliveries_written++;
        return true;
    }
    case BF_NEEDS_FIN:
        rc = moq_session_end_fill(cn->session, bf->sfill, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            return false;
        }
        if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
            b->session_errors++;
        }
        bind_fill_release(b, bf);
        return true;
    case BF_FILL_NEEDS_ABORT:
        rc = moq_session_abort_fill(cn->session, bf->sfill,
                                    bf->fill_abort_code_set
                                        ? bf->fill_abort_code
                                        : bind_fill_fail_code(b, cn),
                                    now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            return false;
        }
        if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
            b->session_errors++;
        }
        bind_fill_release(b, bf);
        return true;
    default:
        b->session_errors++;   /* a FETCH state on a fill entry: a bug */
        bind_fill_release(b, bf);
        return true;
    }
}

/* -- draft-22 standalone FETCH ------------------------------------------------ *
 * A standalone FETCH is admitted once, by this core's retrieval owner or by
 * the shard that owns the track, and answered with FETCH_OK at once: its End
 * Location is known at admission. Its range is then covered in the requested
 * group order through the shared retrieval cursor: Objects this relay holds
 * are served from its cache, and every run it does not hold is retrieved with
 * an upstream FETCH for exactly that run (draft-22 3.2.3), all against the
 * request's one total wait budget (9.20.5). A run nothing can serve by then
 * is marked Timed-Out and the response continues; a FILL_TIMEOUT of 0
 * therefore serves what is held and marks the rest at once. An upstream
 * FETCH that fails resets this response with UNKNOWN_OBJECT_STATUS (3.2.2),
 * and any local failure resets it with INTERNAL_ERROR: both are
 * request-local, never a session close or a FIN over missing Objects. The
 * subscriber's cancellation retires the request and its upstream work. */

static void
bind_standalone_abort(moqr_bind_t *b, const b_conn_t *cn, b_fetch_t *bf,
                      moqr_reset_cause_t cause)
{
    bf->sa_abort_code = bind_retrieve_reset_code(b, cn, cause);
    bf->state = BF_FETCH_NEEDS_ABORT;
}

static b_fetch_t *
bind_fetch_free_slot(moqr_bind_t *b, b_conn_t *cn)
{
    for (uint32_t i = 0; i < b->n_fetches; i++) {
        if (!cn->fetches[i].used) return &cn->fetches[i];
    }
    return NULL;
}

/* Admission. The FETCH is already authorized; its range form is resolved by
 * whichever core owns the track. */
static void
bind_standalone_request(moqr_bind_t *b, b_conn_t *cn,
                        const moq_fetch_request_event_t *fq,
                        const moqr_fetch_req_t *freq,
                        const moq_fetch_request_options_t *fo, uint64_t now_us)
{
    b_fetch_t *bf = bind_fetch_free_slot(b, cn);
    if (bf == NULL) {
        bind_fetch_refuse(b, cn, fq->fetch, MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        return;
    }
    const uint64_t deadline = bind_wait_deadline(
        b, now_us, fo != NULL && (fo->flags & MOQ_FETCH_REQUEST_HAS_FILL_TIMEOUT) != 0,
        fo != NULL ? fo->fill_timeout_ms : 0);
    const moqr_group_order_t order =
        fq->group_order == MOQ_GROUP_ORDER_DESCENDING
            ? MOQR_GROUP_ORDER_DESCENDING : MOQR_GROUP_ORDER_ASCENDING;
    moqr_fetch_req_t req = *freq;
    req.group_order = order;
    moqr_fetch_t owner = MOQR_FETCH_INVALID;
    moqr_core_retrieve_plan_t plan;
    moqr_result_t rc = moqr_core_internal_retrieve_open(b->core, cn->binding, &req,
                                                        now_us, &owner, &plan);
    if (rc != MOQR_OK) {
        /* The FETCH pool is full (or the request cannot be planned): this
         * request alone is refused. */
        bind_fetch_refuse(b, cn, fq->fetch, MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        return;
    }
    if (plan.wire.admit != MOQR_FETCH_ACCEPT) {
        if (plan.remote_cookie != 0 && b->has_sa_src) {
            moqr_retrieve_standalone_query_t q;
            memset(&q, 0, sizeof(q));
            q.remote_cookie = plan.remote_cookie;
            q.ns = req.ns;
            q.name = req.name;
            q.start_group = req.start_group;
            q.start_object = req.start_object;
            q.end_group = req.end_group;
            q.end_object = req.end_object;
            q.range_form = req.range_form;
            q.relative_start = req.relative_start;
            q.group_order = order;
            q.subscriber_priority = req.subscriber_priority;
            q.deadline_us = deadline;
            uint64_t handle = 0;
            if (b->sa_src.open(b->sa_src.ctx, &q, &handle) == MOQR_OK) {
                memset(bf, 0, sizeof(*bf));
                bf->used = true;
                bf->standalone = true;
                bf->sa_remote = true;
                bf->sa_handle = handle;
                bf->response_order = order;
                bf->state = BF_FETCH_NEEDS_PLAN;
                bf->sfetch = fq->fetch;
                bf->fill_deadline_us = deadline;
                return;
            }
            plan.wire.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        }
        bind_fetch_refuse(b, cn, fq->fetch, plan.wire.error_code, now_us);
        return;
    }
    memset(bf, 0, sizeof(*bf));
    bf->standalone = true;
    bf->state = BF_NEEDS_OK;
    bf->sfetch = fq->fetch;
    bf->cfetch = owner;
    bf->eot = plan.wire.end_of_track;
    bf->end_group = plan.wire.end_group;
    bf->end_object = plan.wire.end_object;
    bf->fill_deadline_us = deadline;
    bf->response_order = order;
    bf->response_start = retrieve_loc(plan.start_group, plan.start_object);
    bf->response_end = retrieve_loc(plan.end_group, plan.end_object);
    bf->response_next = moqr_retrieve_first(bf->response_start, bf->response_end, order);
    moqr_retrieve_request_t r;
    memset(&r, 0, sizeof(r));
    r.binding = cn->binding;
    r.standalone = true;
    r.owner = owner;
    r.track = plan.track;
    r.track_gen = plan.track_gen;
    r.source = conn_cookie(b, cn);
    r.child = fq->fetch._opaque;
    r.start = bf->response_start;
    r.end = bf->response_end;
    r.group_order = order;
    r.subscriber_priority = req.subscriber_priority;
    r.deadline_us = deadline;
    if (moqr_bind_retrieve_init(b, &bf->retrieval, &r) != MOQR_OK) {
        (void)moqr_core_fetch_close(b->core, owner);
        memset(bf, 0, sizeof(*bf));
        bind_fetch_refuse(b, cn, fq->fetch, MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        return;
    }
    bf->used = true;
}

/* The subscriber cancelled (draft-22 3.2.4): nothing more is owed on the
 * request, and its cursor's release cancels whatever it asked upstream. */
static bool
bind_standalone_cancelled(moqr_bind_t *b, b_conn_t *cn, moq_fetch_t fetch)
{
    for (uint32_t i = 0; i < b->n_fetches; i++) {
        b_fetch_t *bf = &cn->fetches[i];
        if (bf->used && bf->standalone && bf->sfetch._opaque == fetch._opaque) {
            bind_fill_release(b, bf);
            return true;
        }
    }
    return false;
}

/* An End of Range marker on a FETCH stream. A run ending with its group goes
 * through the profile seam that knows the draft's last Object ID. */
static moq_result_t
bind_fetch_write_range(b_conn_t *cn, moq_fetch_t fetch,
                       moq_fetch_range_kind_t kind, moqr_retrieve_location_t end,
                       uint64_t now_us)
{
    if (end.object == UINT64_MAX && end.group != UINT64_MAX) {
        return moq_session_write_fetch_range_before_group(cn->session, fetch, kind,
                                                          end.group + 1u, now_us);
    }
    return moq_session_write_fetch_range(cn->session, fetch, kind, end.group,
                                         end.object, now_us);
}

static moqr_result_t
bind_standalone_peek(moqr_bind_t *b, b_fetch_t *bf, uint64_t now_us,
                     moqr_retrieve_item_t *it)
{
    if (bf->sa_remote) {
        return b->sa_src.peek(b->sa_src.ctx, bf->sa_handle, now_us, it);
    }
    return moqr_bind_retrieve_peek(b, &bf->retrieval, now_us, it);
}

static moqr_result_t
bind_standalone_commit(moqr_bind_t *b, b_fetch_t *bf)
{
    if (bf->sa_remote) return b->sa_src.commit(b->sa_src.ctx, bf->sa_handle);
    return moqr_bind_retrieve_commit(b, &bf->retrieval);
}

/* One item of an accepted standalone FETCH. Every item must start exactly
 * where the previous one ended in the request's order and stay inside its
 * group's part of the range; anything else resets the response rather than
 * write a stream that misstates what exists. WOULD_BLOCK consumes nothing. */
static bool
bind_standalone_item(moqr_bind_t *b, b_conn_t *cn, b_fetch_t *bf, uint64_t now_us)
{
    moqr_retrieve_item_t it;
    moqr_result_t prc = bind_standalone_peek(b, bf, now_us, &it);
    if (prc == MOQR_ERR_WOULD_BLOCK) return false;
    if (prc != MOQR_OK) {
        bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
        return true;
    }
    if (it.kind == MOQR_RETRIEVE_ERROR) {
        bind_standalone_abort(b, cn, bf, bind_retrieve_error_cause(&it));
        return true;
    }
    if (it.kind == MOQR_RETRIEVE_END) {
        if (!bf->response_done) {
            bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
            return true;
        }
        /* A run known not to exist at the end of the range is what FIN says. */
        bf->response_ne_pending = false;
        bf->state = BF_NEEDS_FIN;
        return true;
    }
    moqr_retrieve_location_t window = moqr_retrieve_window_end(
        bf->response_end, bf->response_order, bf->response_next.group);
    if (bf->response_done || retrieve_loc_cmp(it.start, bf->response_next) != 0 ||
        retrieve_loc_cmp(it.end, it.start) < 0 ||
        retrieve_loc_cmp(it.end, window) > 0) {
        bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
        return true;
    }
    moq_result_t rc;
    if (it.kind == MOQR_RETRIEVE_OBJECT) {
        if (retrieve_loc_cmp(it.start, it.end) != 0 ||
            it.object.group_id != it.end.group ||
            it.object.object_id != it.end.object) {
            bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
            return true;
        }
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.group_id = it.object.group_id;
        oc.subgroup_id = it.object.subgroup_id;
        oc.object_id = it.object.object_id;
        oc.publisher_priority = it.object.publisher_priority;
        oc.datagram = moq_session_supports_fetch_datagram(cn->session)
                          ? it.object.datagram_pref : false;
        oc.payload = (moq_rcbuf_t *)(void *)it.object.payload;
        oc.properties = (moq_rcbuf_t *)(void *)it.object.properties;
        rc = moq_session_write_fetch_object(cn->session, bf->sfetch, &oc, now_us);
    } else if (it.kind == MOQR_RETRIEVE_GAP) {
        if (it.gap_kind == MOQ_FETCH_RANGE_NON_EXISTENT) {
            /* Unmarked, a gap in a FETCH response says these Objects do not
             * exist (3.2.2). Held back in case a marked run follows. */
            rc = MOQ_OK;
        } else {
            if (bf->response_ne_pending) {
                /* Split the known non-existent run from the marked one. */
                rc = bind_fetch_write_range(cn, bf->sfetch,
                                            MOQ_FETCH_RANGE_NON_EXISTENT,
                                            bf->response_ne_end, now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK) return false;
                if (rc != MOQ_OK) {
                    if (fwd_result_is_gone(rc)) bind_fill_release(b, bf);
                    else bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
                    return true;
                }
                bf->response_ne_pending = false;
            }
            rc = bind_fetch_write_range(cn, bf->sfetch, it.gap_kind, it.end, now_us);
        }
    } else {
        bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
        return true;
    }
    if (rc == MOQ_ERR_WOULD_BLOCK) return false;
    if (rc != MOQ_OK) {
        if (fwd_result_is_gone(rc)) bind_fill_release(b, bf);
        else bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
        return true;
    }
    if (it.kind == MOQR_RETRIEVE_OBJECT) {
        bf->response_ne_pending = false;
        b->deliveries_written++;
    } else if (it.gap_kind == MOQ_FETCH_RANGE_NON_EXISTENT) {
        bf->response_ne_pending = true;
        bf->response_ne_end = it.end;
    }
    if (bind_standalone_commit(b, bf) != MOQR_OK) {
        bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
        return true;
    }
    moqr_retrieve_location_t next;
    if (moqr_retrieve_after(bf->response_start, bf->response_end, bf->response_order, it.end, &next)) {
        bf->response_next = next;
    } else {
        bf->response_done = true;
    }
    return true;
}

/* Drive one standalone FETCH as far as the session allows; false means HOLD
 * (retry next pump, nothing advanced). */
static bool
bind_standalone_step(moqr_bind_t *b, b_conn_t *cn, b_fetch_t *bf, uint64_t now_us)
{
    moq_result_t rc;
    switch (bf->state) {
    case BF_FETCH_NEEDS_PLAN: {
        moqr_retrieve_standalone_plan_t plan;
        memset(&plan, 0, sizeof(plan));
        moqr_result_t prc = b->sa_src.plan(b->sa_src.ctx, bf->sa_handle, &plan);
        if (prc == MOQR_ERR_WOULD_BLOCK) return false;
        if (prc != MOQR_OK || plan.wire.admit != MOQR_FETCH_ACCEPT ||
            retrieve_loc_cmp(plan.start, plan.end) > 0) {
            moq_fetch_t fetch = bf->sfetch;
            uint64_t code = prc == MOQR_OK && plan.wire.admit == MOQR_FETCH_REJECT
                                ? plan.wire.error_code
                                : MOQ_REQUEST_ERROR_INTERNAL_ERROR;
            bind_fill_release(b, bf);
            bind_fetch_refuse(b, cn, fetch, code, now_us);
            return true;
        }
        bf->eot = plan.wire.end_of_track;
        bf->end_group = plan.wire.end_group;
        bf->end_object = plan.wire.end_object;
        bf->response_start = plan.start;
        bf->response_end = plan.end;
        bf->response_next = moqr_retrieve_first(plan.start, plan.end, bf->response_order);
        bf->state = BF_NEEDS_OK;
        return true;
    }
    case BF_NEEDS_OK: {
        /* A local track's properties are stated (§7.7). One another shard
         * owns has its record there, and this answer carries none. */
        moqr_bind_meta_ref_t pin = {0};
        if (!bf->sa_remote && !bind_fetch_ok_pin(b, cn, bf->cfetch, &pin)) {
            moqr_bind_meta_release(b, &pin);
            moq_fetch_t fetch = bf->sfetch;
            bind_fill_release(b, bf);
            bind_fetch_refuse(b, cn, fetch, MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
            return true;
        }
        moq_accept_fetch_cfg_t ac;
        moq_accept_fetch_cfg_init(&ac);
        ac.end_of_track = bf->eot;
        ac.end_group = bf->end_group;
        ac.end_object = bf->end_object;
        ac.track_properties = pin.bytes;
        rc = moq_session_accept_fetch(cn->session, bf->sfetch, &ac, now_us);
        moqr_bind_meta_release(b, &pin);
        if (rc == MOQ_ERR_WOULD_BLOCK) return false;
        if (rc == MOQ_OK) {
            bf->state = BF_STREAMING;
            return true;
        }
        if (fwd_result_is_gone(rc)) {
            bind_fill_release(b, bf);
            return true;
        }
        /* Nothing reached the wire: the request is still pending, so it is
         * refused instead. */
        moq_fetch_t fetch = bf->sfetch;
        b->session_errors++;
        bind_fill_release(b, bf);
        bind_fetch_refuse(b, cn, fetch, MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        return true;
    }
    case BF_STREAMING:
        return bind_standalone_item(b, cn, bf, now_us);
    case BF_NEEDS_FIN:
        rc = moq_session_end_fetch(cn->session, bf->sfetch, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return false;
        if (rc == MOQ_OK || fwd_result_is_gone(rc)) {
            bind_fill_release(b, bf);
        } else {
            bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
        }
        return true;
    case BF_FETCH_NEEDS_ABORT:
        rc = moq_session_abort_fetch(cn->session, bf->sfetch, bf->sa_abort_code,
                                     now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK) return false;
        if (rc != MOQ_OK && !fwd_result_is_gone(rc)) b->session_errors++;
        bind_fill_release(b, bf);
        return true;
    default:
        b->session_errors++;   /* a fill state on a FETCH entry: a bug */
        bind_standalone_abort(b, cn, bf, MOQR_RESET_INTERNAL_ERROR);
        return true;
    }
}

/* The connection is going away: every held refusal on it is released, and
 * its parked authorizations (retired with the binding) need no entry. */
static void
bind_refusal_conn_gone(moqr_bind_t *b, b_conn_t *cn)
{
    cn->refusals_reserved = 0;
    for (uint32_t i = 0; i < b->n_refusals; i++) {
        b_refusal_t *r = &cn->refusals[i];
        if (r->used) {
            memset(r, 0, sizeof(*r));
            FWD_DBG(b, refusals_dropped);
        }
    }
}

/* Admit a downstream Joining FETCH as a forwarding transaction, or reject it
 * on the wire. The joined subscription is resolved ONCE to its track identity
 * and generation; the resolved namespace/name is what the fetch is authorized
 * against (fail-closed) and what the upstream request names. The upstream
 * source is the connection whose upstream subscription carries that exact
 * (track, generation): a replaced origin has a new generation and is never
 * addressed by an older transaction. */
static void
fwd_admit(moqr_bind_t *b, b_conn_t *cn, const moq_fetch_request_event_t *fq,
          uint64_t now_us)
{
    /* 1. Resolve the joined subscription. */
    moqr_sub_t rsub = MOQR_SUB_INVALID;
    for (uint32_t i = 0; i < b->n_dsubs; i++) {
        if (cn->dsubs[i].used &&
            cn->dsubs[i].sub_raw == fq->joining_sub._opaque) {
            rsub = cn->dsubs[i].rsub;
            break;
        }
    }
    moqr_track_t track = { 0 };
    uint64_t track_gen = 0;
    b_conn_t *ucn = NULL;
    const b_usub_t *us = NULL;
    bool resolved = moqr_sub_is_valid(rsub) &&
                    moqr_core_sub_track(b->core, rsub, &track, &track_gen) ==
                        MOQR_OK;
    if (resolved) {
        for (uint32_t ci = 0; ci < b->max_conns && us == NULL; ci++) {
            b_conn_t *c = &b->conns[ci];
            if (!c->used || c->closed || c->session == NULL) {
                continue;
            }
            for (uint32_t i = 0; i < b->n_usubs; i++) {
                if (c->usubs[i].used && c->usubs[i].has_key &&
                    moqr_track_eq(c->usubs[i].track, track) &&
                    c->usubs[i].track_gen == track_gen) {
                    ucn = c;
                    us = &c->usubs[i];
                    break;
                }
            }
        }
    }
    /* 2. Authorize on the RESOLVED identity (fail-closed). An unresolvable
     * join is authorized on the empty identity exactly as before, so the
     * request's existence is never a pre-auth oracle. */
    moqr_ns_t ans = us != NULL ? (moqr_ns_t){ us->parts, us->ns_count }
                               : (moqr_ns_t){ NULL, 0 };
    moq_bytes_t aname = us != NULL ? us->name : (moq_bytes_t){ NULL, 0 };
    uint64_t ferr = 0;
    uint64_t ftkt = 0;
    if (bind_authorize(b, cn, MOQR_AUTH_FETCH, ans, aname, fq->tokens,
                       fq->token_count, now_us, &ferr, &ftkt, NULL) !=
        BIND_AUTH_ALLOW) {
        bind_fetch_refuse(b, cn, fq->fetch,
                          ferr != 0 ? ferr : MOQ_REQUEST_ERROR_UNAUTHORIZED, now_us);
        return;
    }
    if (!resolved) {
        bind_fetch_refuse(b, cn, fq->fetch,
                          MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID, now_us);
        return;
    }
    if (us == NULL) {
        /* No upstream subscription carries this exact source generation
         * (origin gone, replaced, or its key could not be retained). */
        bind_fetch_refuse(b, cn, fq->fetch, MOQ_REQUEST_ERROR_DOES_NOT_EXIST, now_us);
        FWD_DBG(b, rejected_no_upstream);
        return;
    }
    /* 3. A transaction slot. */
    b_fwd_t *t = NULL;
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        if (!b->fwd[i].used) {
            t = &b->fwd[i];
            break;
        }
    }
    if (t == NULL) {
        /* The transaction table is a bounded pool: a capacity refusal. */
        bind_reject_admission(b, cn, MOQR_AUTH_FETCH, fq->fetch._opaque,
                              MOQR_ERR_CAPACITY, now_us);
        FWD_DBG(b, rejected_no_slot);
        return;
    }
    memset(t, 0, sizeof(*t));
    t->used = true;
#ifdef MOQR_BIND_TESTING
    b->dbg_fwd.live++;
#endif
    /* 4. Owned, charged copy of the track key (one buffer; parts + name). */
    if (!fwd_copy(b, t, us->key, us->key_len, &t->key, &t->key_len)) {
        fwd_release(b, t);
        bind_fetch_refuse(b, cn, fq->fetch, MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
        FWD_DBG(b, rejected_oom);
        return;
    }
    t->ns_count = us->ns_count;
    {
        size_t off = 0;
        for (uint32_t i = 0; i < us->ns_count; i++) {
            t->parts[i] = (moq_bytes_t){ t->key + off, us->parts[i].len };
            off += us->parts[i].len;
        }
        t->name = (moq_bytes_t){ t->key + off, us->name.len };
    }
    t->d_cookie = conn_cookie(b, cn);
    t->d_binding = cn->binding;
    t->sfetch = fq->fetch;
    t->u_cookie = conn_cookie(b, ucn);
    t->u_binding = ucn->binding;
    t->track = track;
    t->track_gen = track_gen;
    t->start_group = fq->start_group;
    t->start_object = fq->start_object;
    t->end_group = fq->end_group;
    t->end_object = fq->end_object;
    t->has_prio = true;
    t->prio = fq->subscriber_priority;
    t->u = FW_U_UNSENT;
    t->d = FW_D_PENDING;
    FWD_DBG(b, admitted);
}

/* An item refused by the counts or the pool: the collection is abandoned
 * before OK (overflow terminal) and the upstream request cancelled. */
static void
fwd_overflow(moqr_bind_t *b, b_fwd_t *t)
{
    (void)b;
    if (t->consumer == FW_CONSUMER_FILL) {
        t->failure = MOQR_RETRIEVE_LOCAL;
        t->local_result = MOQR_ERR_CAPACITY;
    }
    fwd_end_up(t, FW_END_OVERFLOW);
    FWD_DBG(b, overflow);
}

/* Whether (group, object) lies inside the transaction's normalized range
 * [start, end) -- end_object 0 meaning "through the end of end_group". */
static bool
fwd_in_range(const b_fwd_t *t, uint64_t group, uint64_t object)
{
    if (group < t->start_group ||
        (group == t->start_group && object < t->start_object)) {
        return false;
    }
    if (t->end_object == 0) {
        return group <= t->end_group;
    }
    return group < t->end_group ||
           (group == t->end_group && object < t->end_object);
}

/* Whether an upstream FETCH_OK's End Location stays within the request's
 * end (same end_object 0 convention on both sides). */
static bool
fwd_end_within(const b_fwd_t *t, uint64_t end_group, uint64_t end_object)
{
    if (t->end_object == 0) {
        return end_group <= t->end_group;
    }
    if (end_object == 0) {
        return end_group < t->end_group;
    }
    return end_group < t->end_group ||
           (end_group == t->end_group && end_object <= t->end_object);
}

/* The upstream answer does not fit the pinned range: abandoned before OK
 * (never relayed as a narrower or wider success) and cancelled upstream. */
static void
fwd_mismatch(moqr_bind_t *b, b_fwd_t *t)
{
    (void)b;
    if (t->consumer == FW_CONSUMER_FILL) {
        t->failure = MOQR_RETRIEVE_MALFORMED;
        t->local_result = MOQR_ERR_INVAL;
    }
    fwd_end_up(t, FW_END_MISMATCH);
    FWD_DBG(b, mismatch);
}

static bool
retrieve_response_position(const b_fwd_t *t, uint64_t group, uint64_t object)
{
    moqr_retrieve_location_t at = retrieve_loc(group, object);
    if (!fwd_in_range(t, group, object) ||
        (t->ok_seen && retrieve_loc_cmp(at, retrieve_inclusive_end(
             t->ok_end_group, t->ok_end_object)) > 0)) return false;
    if (t->n_items != 0) {
        const b_fwd_item_t *last = &t->items[t->n_items - 1u];
        if (retrieve_loc_cmp(at, retrieve_loc(last->group, last->object)) <= 0) {
            return false;
        }
    }
    return true;
}

static bool
retrieve_response_end(const b_fwd_t *t, uint64_t group, uint64_t object)
{
    moqr_retrieve_location_t end = retrieve_inclusive_end(group, object);
    if (!fwd_end_within(t, group, object) ||
        retrieve_loc_cmp(end, retrieve_loc(t->start_group, t->start_object)) < 0) {
        return false;
    }
    if (t->n_items != 0) {
        const b_fwd_item_t *last = &t->items[t->n_items - 1u];
        if (retrieve_loc_cmp(end, retrieve_loc(last->group, last->object)) < 0) {
            return false;
        }
    }
    return true;
}

/* Fetcher-role events on an upstream connection. Returns true when the event
 * belonged to a transaction (its owned references, if any, were taken). An
 * event naming no live transaction handle is left for moq_event_cleanup. */
static bool
fwd_on_up_event(moqr_bind_t *b, b_conn_t *cn, moq_event_t *ev, uint64_t now_us)
{
    uint64_t h;
    switch (ev->kind) {
    case MOQ_EVENT_FETCH_OK:       h = ev->u.fetch_ok.fetch._opaque; break;
    case MOQ_EVENT_FETCH_OBJECT:   h = ev->u.fetch_object.fetch._opaque; break;
    case MOQ_EVENT_FETCH_GAP:      h = ev->u.fetch_gap.fetch._opaque; break;
    case MOQ_EVENT_FETCH_COMPLETE: h = ev->u.fetch_complete.fetch._opaque; break;
    case MOQ_EVENT_FETCH_ERROR:    h = ev->u.fetch_error.fetch._opaque; break;
    case MOQ_EVENT_FETCH_RESET:    h = ev->u.fetch_reset.fetch._opaque; break;
    default:                       return false;
    }
    b_fwd_t *t = fwd_find_up(b, cn, h);
    if (t == NULL) {
        FWD_DBG(b, stale_events_ignored);
        return false;   /* retired handle: cleaned by the pump, never matched */
    }
    if (t->consumer == FW_CONSUMER_FILL) {
        if (t->end == FW_END_NONE && !retrieve_fwd_current(b, t)) {
            fwd_end_up(t, FW_END_LOST);
        }
        retrieve_fwd_expire(t, now_us);
    }
    switch (ev->kind) {
    case MOQ_EVENT_FETCH_OK: {
        const moq_fetch_ok_event_t *ok = &ev->u.fetch_ok;
        if (t->u != FW_U_PENDING || t->ok_seen) {
            if (t->consumer == FW_CONSUMER_FILL && t->u == FW_U_PENDING) {
                fwd_mismatch(b, t);
            }
            FWD_DBG(b, drained_events);
            return true;
        }
        if (!fwd_end_within(t, ok->end_group, ok->end_object) ||
            (t->consumer == FW_CONSUMER_FILL &&
             !retrieve_response_end(t, ok->end_group, ok->end_object))) {
            fwd_mismatch(b, t);
            return true;
        }
        moqr_bind_meta_ref_t input = bind_meta_input(cn, h, t->track_gen,
                                                     ok->track_properties);
        if (t->consumer == FW_CONSUMER_FILL ||
            fwd_copy(b, t, ok->track_properties.data,
                     ok->track_properties.len, &t->props, &t->props_len)) {
            t->ok_seen = true;
            t->ok_eot = ok->end_of_track;
            t->ok_end_group = ok->end_group;
            t->ok_end_object = ok->end_object;
        } else {
            fwd_overflow(b, t);
        }
        /* Fill has no second ACK and needs no transaction property copy;
         * the separate bounded directory owns its newest complete set.
         * A lawful new complete set replaces the directory even if this
         * separate Joining FETCH cannot retain its transaction. Its owned
         * reply remains unchanged. A failed directory copy publishes
         * UNAVAILABLE; a stale source generation cannot update a new source.
         * This callback precedes processing any following FETCH objects. */
        (void)moqr_bind_meta_replace(b, t->track, t->track_gen, &input,
                                     now_us, NULL);
        return true;
    }
    case MOQ_EVENT_FETCH_OBJECT: {
        moq_fetch_object_event_t *o = &ev->u.fetch_object;
        if (t->u != FW_U_PENDING) {
            FWD_DBG(b, drained_events);
            return true;   /* draining a cancelled/overflowed response */
        }
        if (!fwd_in_range(t, o->group_id, o->object_id) ||
            (t->consumer == FW_CONSUMER_FILL &&
             !retrieve_response_position(t, o->group_id, o->object_id))) {
            fwd_mismatch(b, t);
            return true;
        }
        if (t->n_items >= BF_FWD_MAX_ITEMS || t->n_objects >= BF_FWD_MAX_OBJECTS) {
            fwd_overflow(b, t);
            return true;
        }
        b_fwd_item_t *it = &t->items[t->n_items];
        memset(it, 0, sizeof(*it));
        if (t->consumer == FW_CONSUMER_FILL) {
            moqr_result_t rc = retrieve_copy_object(b, t, it, o);
            if (rc != MOQR_OK) {
                fwd_overflow(b, t);
                t->local_result = rc;
                return true;
            }
        } else {
            uint64_t bytes = (o->payload != NULL ? moq_rcbuf_len(o->payload) : 0) +
                             (o->properties != NULL ? moq_rcbuf_len(o->properties) : 0);
            if (!fwd_charge(b, t, bytes)) {
                fwd_overflow(b, t);
                return true;
            }
            it->payload = o->payload;
            it->properties = o->properties;
            it->charged = bytes;
            o->payload = NULL;
            o->properties = NULL;
        }
        it->group = o->group_id;
        it->subgroup = o->subgroup_id;
        it->object = o->object_id;
        it->publisher_priority = o->publisher_priority;
        it->datagram = o->datagram;
        t->n_items++;
        t->n_objects++;
        return true;
    }
    case MOQ_EVENT_FETCH_GAP: {
        const moq_fetch_gap_event_t *g = &ev->u.fetch_gap;
        if (t->u != FW_U_PENDING) {
            FWD_DBG(b, drained_events);
            return true;
        }
        if (t->consumer == FW_CONSUMER_FILL &&
            (!retrieve_response_position(t, g->group_id, g->object_id) ||
             (g->range_kind != MOQ_FETCH_RANGE_NON_EXISTENT &&
              g->range_kind != MOQ_FETCH_RANGE_UNKNOWN &&
              g->range_kind != MOQ_FETCH_RANGE_TIMED_OUT))) {
            fwd_mismatch(b, t);
            return true;
        }
        if (t->n_items >= BF_FWD_MAX_ITEMS || t->n_gaps >= BF_FWD_MAX_GAPS) {
            fwd_overflow(b, t);
            return true;
        }
        b_fwd_item_t *it = &t->items[t->n_items++];
        memset(it, 0, sizeof(*it));
        it->is_gap = true;
        it->range_kind = (uint8_t)g->range_kind;
        it->group = g->group_id;
        it->object = g->object_id;
        t->n_gaps++;
        return true;
    }
    case MOQ_EVENT_FETCH_COMPLETE:
        /* The session frees the fetcher entry with this terminal. */
        t->ufetch_live = false;
        if (t->u == FW_U_PENDING && t->end == FW_END_NONE) {
            t->end = t->ok_seen ? FW_END_COMPLETE : FW_END_ERROR;
            if (!t->ok_seen) {
                t->err_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
                if (t->consumer == FW_CONSUMER_FILL) {
                    t->failure = MOQR_RETRIEVE_MALFORMED;
                    t->local_result = MOQR_ERR_INVAL;
                }
            }
        }
        t->u = FW_U_TERMINAL;
        FWD_DBG(b, completed);
        return true;
    case MOQ_EVENT_FETCH_ERROR: {
        const moq_fetch_error_event_t *e = &ev->u.fetch_error;
        t->ufetch_live = false;
        if (t->end == FW_END_NONE) {
            t->end = FW_END_ERROR;
            if (fwd_copy(b, t, e->reason.data, e->reason.len, &t->reason,
                         &t->reason_len)) {
                t->err_code = e->error_code;
                t->err_can_retry = e->can_retry;
                t->err_retry_after_ms = e->retry_after_ms;
            } else {
                /* The complete upstream error cannot be retained: answer
                 * with the relay's own failure, never a trimmed copy. */
                t->err_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
                t->err_can_retry = false;
                t->err_retry_after_ms = 0;
                t->reason = NULL;
                t->reason_len = 0;
                if (t->consumer == FW_CONSUMER_FILL) {
                    t->failure = MOQR_RETRIEVE_LOCAL;
                    t->local_result = MOQR_ERR_CAPACITY;
                    t->err_code = e->error_code;
                }
                FWD_DBG(b, reason_unretained);
            }
        }
        t->u = FW_U_TERMINAL;
        FWD_DBG(b, errors);
        return true;
    }
    case MOQ_EVENT_FETCH_RESET:
        t->ufetch_live = false;
        if (t->end == FW_END_NONE) {
            t->end = FW_END_RESET;
            t->reset_code = ev->u.fetch_reset.error_code;
            t->reset_cause = ev->u.fetch_reset.cause;
            t->reset_data_stream = ev->u.fetch_reset.data_stream;
            t->reset_stop_sending = ev->u.fetch_reset.stop_sending;
        }
        t->u = FW_U_TERMINAL;
        FWD_DBG(b, resets);
        return true;
    default:
        return false;
    }
}

/* Publisher-role: the downstream subscriber cancelled its (forwarded) fetch.
 * The session already retired its entry and reset any open data stream. */
static bool
fwd_on_down_cancelled(moqr_bind_t *b, b_conn_t *cn, uint64_t handle)
{
    b_fwd_t *t = fwd_find_down(b, cn, handle);
    if (t == NULL) {
        return false;
    }
    t->d = FW_D_TERMINAL;
    fwd_end_up(t, FW_END_CANCELLED);
    FWD_DBG(b, cancelled_down);
    return true;
}

static bool
fwd_result_is_gone(moq_result_t rc)
{
    return rc == MOQ_ERR_STALE_HANDLE || rc == MOQ_ERR_WRONG_STATE ||
           rc == MOQ_ERR_CLOSED;
}

/* Drive every transaction as far as the sessions allow. Each step HOLDs on
 * WOULD_BLOCK with no state advance and is retried by the next pump; no step
 * loops on a refusal. */
static void
bind_fwd_pump(moqr_bind_t *b, uint64_t now_us)
{
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        b_fwd_t *t = &b->fwd[i];
        if (!t->used) {
            continue;
        }
        /* -- upstream side -- */
        if (t->consumer == FW_CONSUMER_FILL) {
            if (t->end == FW_END_NONE && !retrieve_fwd_current(b, t)) {
                fwd_end_up(t, FW_END_LOST);
            }
            retrieve_fwd_expire(t, now_us);
        }
        if (t->u == FW_U_UNSENT) {
            b_conn_t *ucn = fwd_conn(b, t->u_cookie, t->u_binding);
            if (ucn == NULL) {
                t->end = t->end == FW_END_NONE ? FW_END_LOST : t->end;
                t->u = FW_U_TERMINAL;
                FWD_DBG(b, lost);
            } else {
                moq_fetch_cfg_t fc;
                /* D22 section 9.20 parameters apply to this hop. Enforce the
                 * consumer's absolute deadline locally rather than forward
                 * FILL_TIMEOUT; expiry above prevents an unsent dispatch
                 * and retains cancellation of any request already sent. */
                moq_fetch_cfg_init(&fc);
                fc.track_namespace = (moq_namespace_t){ .parts = t->parts,
                                                        .count = t->ns_count };
                fc.track_name = t->name;
                fc.start_group = t->start_group;
                fc.start_object = t->start_object;
                fc.end_group = t->end_group;
                fc.end_object = t->end_object;
                /* A retrieval asks in its consumer's order; each span lies in
                 * one group whenever that order is descending. */
                fc.group_order = t->consumer == FW_CONSUMER_FILL &&
                        t->retrieval.group_order == MOQR_GROUP_ORDER_DESCENDING
                    ? MOQ_GROUP_ORDER_DESCENDING : MOQ_GROUP_ORDER_ASCENDING;
                fc.has_subscriber_priority = t->has_prio;
                fc.subscriber_priority = t->prio;
                moq_result_t rc = moq_session_fetch(ucn->session, &fc, now_us,
                                                    &t->ufetch);
                if (rc == MOQ_OK) {
                    t->ufetch_live = true;
                    t->u = FW_U_PENDING;
                } else if (rc == MOQ_ERR_WOULD_BLOCK ||
                           rc == MOQ_ERR_REQUEST_BLOCKED) {
                    FWD_DBG(b, retried_send);   /* HOLD */
                } else {
                    t->end = t->end == FW_END_NONE ? FW_END_ERROR : t->end;
                    t->err_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
                    if (t->consumer == FW_CONSUMER_FILL) {
                        t->failure = MOQR_RETRIEVE_SESSION;
                        t->session_result = rc;
                    }
                    t->u = FW_U_TERMINAL;
                    b->session_errors++;
                }
            }
        }
        if (t->u == FW_U_CANCELLING) {
            b_conn_t *ucn = fwd_conn(b, t->u_cookie, t->u_binding);
            if (ucn == NULL || !t->ufetch_live) {
                t->u = FW_U_TERMINAL;
                t->ufetch_live = false;
            } else {
                moq_result_t rc = moq_session_fetch_cancel(ucn->session,
                                                           t->ufetch, now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK) {
                    FWD_DBG(b, retried_cancel);   /* HOLD: ownership retained */
                } else if (t->consumer == FW_CONSUMER_FILL && rc != MOQ_OK &&
                           !fwd_result_is_gone(rc)) {
                    /* A hard local refusal is not proof the upstream request
                     * retired. Keep this bounded slot and retry cancellation;
                     * neither consumer teardown nor slot reuse may erase it. */
                    if (t->session_result == MOQ_OK) {
                        b->session_errors++;
                        t->session_result = rc;
                    }
                    if (t->end == FW_END_TIMEOUT) {
                        t->end = FW_END_ERROR;
                        t->failure = MOQR_RETRIEVE_SESSION;
                    }
                } else {
                    /* Committed, already gone, or a local failure that can
                     * never cancel it: the upstream session is then closed
                     * (bind_cancel_settled), so the FETCH cannot go on. */
                    (void)bind_cancel_settled(b, ucn, rc, "fetch cancellation failed",
                                              now_us);
                    t->u = FW_U_TERMINAL;
                    t->ufetch_live = false;
                    FWD_DBG(b, cancelled_up);
                }
            }
        }
        if (t->consumer == FW_CONSUMER_FILL) {
            if (t->u == FW_U_TERMINAL && t->d == FW_D_TERMINAL) {
                fwd_release(b, t);
            }
            continue; /* the private cursor owns replay, not a session FETCH */
        }
        /* -- downstream side -- */
        b_conn_t *dcn = t->d != FW_D_TERMINAL
                            ? fwd_conn(b, t->d_cookie, t->d_binding) : NULL;
        if (t->d != FW_D_TERMINAL && dcn == NULL) {
            t->d = FW_D_TERMINAL;   /* its session is gone: nothing to write */
            fwd_end_up(t, FW_END_CANCELLED);
            FWD_DBG(b, down_lost);
        }
        for (uint32_t guard = 0; guard < 256 && t->d != FW_D_TERMINAL; guard++) {
            if (t->d == FW_D_PENDING) {
                if (t->u != FW_U_TERMINAL) {
                    break;   /* still collecting */
                }
                if (t->end == FW_END_COMPLETE && t->ok_seen && !t->malformed) {
                    t->d = FW_D_ACCEPTING;
                    continue;
                }
                moq_reject_fetch_cfg_t rj;
                moq_reject_fetch_cfg_init(&rj);
                if (t->malformed) {
                    rj.error_code = MOQ_REQUEST_ERROR_MALFORMED_TRACK;
                } else if (t->end == FW_END_ERROR) {
                    rj.error_code = t->err_code;
                    rj.can_retry = t->err_can_retry;
                    rj.retry_after_ms = t->err_retry_after_ms;
                    rj.reason = (moq_bytes_t){ t->reason, t->reason_len };
                } else {
                    rj.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
                }
                moq_result_t rc = moq_session_reject_fetch(dcn->session,
                                                           t->sfetch, &rj,
                                                           now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK) {
                    FWD_DBG(b, retried_reject);
                    break;
                }
                if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
                    b->session_errors++;
                }
                t->d = FW_D_TERMINAL;
                FWD_DBG(b, rejected_down);
                break;
            }
            if (t->d == FW_D_ACCEPTING && t->malformed) {
                t->d = FW_D_PENDING;   /* not accepted yet: refused instead */
                continue;
            }
            if (t->d == FW_D_STREAMING && t->malformed) {
                t->d = FW_D_ABORTING;  /* accepted: reset, never FINed */
                continue;
            }
            if (t->d == FW_D_ACCEPTING) {
                moq_accept_fetch_cfg_t ac;
                moq_accept_fetch_cfg_init(&ac);
                ac.end_of_track = t->ok_eot;
                ac.end_group = t->ok_end_group;
                ac.end_object = t->ok_end_object;
                ac.empty = (t->n_items == 0);
                ac.track_properties = (moq_bytes_t){ t->props, t->props_len };
                moq_result_t rc = moq_session_accept_fetch(dcn->session,
                                                           t->sfetch, &ac,
                                                           now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK) {
                    FWD_DBG(b, retried_accept);
                    break;
                }
                if (fwd_result_is_gone(rc)) {
                    t->d = FW_D_TERMINAL;   /* peer cancelled / session closed */
                    FWD_DBG(b, down_gone);
                    break;
                }
                if (rc != MOQ_OK) {
                    /* Locally unencodable before anything reached the wire:
                     * the request is still open downstream, so it is answered
                     * with a rejection rather than left pending. */
                    b->session_errors++;
                    t->end = FW_END_ERROR;
                    t->err_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
                    t->err_can_retry = false;
                    t->err_retry_after_ms = 0;
                    t->d = FW_D_PENDING;
                    continue;
                }
                t->d = ac.empty ? FW_D_TERMINAL : FW_D_STREAMING;
                if (ac.empty) {
                    FWD_DBG(b, delivered);
                }
                continue;
            }
            if (t->d == FW_D_STREAMING) {
                if (t->next_write < t->n_items) {
                    b_fwd_item_t *it = &t->items[t->next_write];
                    moq_result_t rc;
                    if (it->is_gap) {
                        rc = moq_session_write_fetch_range(
                            dcn->session, t->sfetch,
                            (moq_fetch_range_kind_t)it->range_kind, it->group,
                            it->object, now_us);
                    } else {
                        moq_fetch_object_cfg_t oc;
                        moq_fetch_object_cfg_init(&oc);
                        oc.group_id = it->group;
                        oc.subgroup_id = it->subgroup;
                        oc.object_id = it->object;
                        oc.publisher_priority = it->publisher_priority;
                        oc.datagram = moq_session_supports_fetch_datagram(
                                          dcn->session) ? it->datagram : false;
                        oc.payload = it->payload;
                        oc.properties = it->properties;
#ifdef MOQR_BIND_TESTING
                        if (atomic_load(&bind_dbg_fail_fwd_writes) > 0 &&
                            atomic_fetch_sub(&bind_dbg_fail_fwd_writes, 1) == 1) {
                            rc = MOQ_ERR_INVAL;   /* injected: unencodable */
                        } else
#endif
                        {
                            rc = moq_session_write_fetch_object(
                                dcn->session, t->sfetch, &oc, now_us);
                        }
                    }
                    if (rc == MOQ_ERR_WOULD_BLOCK) {
                        FWD_DBG(b, retried_write);
                        break;   /* HOLD: the item is kept, retried as is */
                    }
                    if (rc == MOQ_OK) {
                        fwd_item_release(b, t, it);
                        t->next_write++;
                        b->deliveries_written++;
                        continue;
                    }
                    if (fwd_result_is_gone(rc)) {
                        t->d = FW_D_TERMINAL;
                        FWD_DBG(b, down_gone);
                        break;
                    }
                    t->d = FW_D_ABORTING;   /* locally unencodable after OK */
                    continue;
                }
                moq_result_t rc = moq_session_end_fetch(dcn->session, t->sfetch,
                                                        now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK) {
                    FWD_DBG(b, retried_fin);
                    break;
                }
                if (rc == MOQ_OK) {
                    t->d = FW_D_TERMINAL;
                    FWD_DBG(b, delivered);
                    break;
                }
                if (fwd_result_is_gone(rc)) {
                    t->d = FW_D_TERMINAL;
                    FWD_DBG(b, down_gone);
                    break;
                }
                t->d = FW_D_ABORTING;
                continue;
            }
            if (t->d == FW_D_ABORTING) {
                moq_result_t rc = moq_session_abort_fetch(dcn->session, t->sfetch,
                                                          t->malformed
                                                              ? t->malformed_wire
                                                              : BIND_RESET_INTERNAL,
                                                          now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK) {
                    FWD_DBG(b, retried_abort);
                    break;
                }
                if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
                    b->session_errors++;
                }
                t->d = FW_D_TERMINAL;
                FWD_DBG(b, aborted);
                break;
            }
        }
        if (t->u == FW_U_TERMINAL && t->d == FW_D_TERMINAL) {
            fwd_release(b, t);
        }
    }
}

/* Retain an owned copy of the track key an upstream subscription was issued
 * for, so a later forwarded fetch can name it without consulting the core.
 * Bounded by the full-track-name cap; a copy that cannot be made leaves the
 * subscription usable for live delivery but not as a forwarding source. */
static void
usub_store_key(moqr_bind_t *b, b_usub_t *us, const moq_bytes_t *parts,
               uint32_t ns_count, moq_bytes_t name)
{
    us->has_key = false;
    us->key = NULL;
    us->key_len = 0;
    if (ns_count > 32) {
        return;
    }
    size_t total = name.len;
    for (uint32_t i = 0; i < ns_count; i++) {
        if (parts[i].len == 0 || parts[i].data == NULL ||
            BF_FWD_KEY_MAX - total < parts[i].len) {
            return;
        }
        total += parts[i].len;
    }
    if (total > BF_FWD_KEY_MAX || (name.len > 0 && name.data == NULL)) {
        return;
    }
    uint8_t *bytes = NULL;
    if (total > 0) {
        bytes = b->alloc.alloc(total, b->alloc.ctx);
        if (bytes == NULL) {
            return;
        }
    }
    size_t off = 0;
    for (uint32_t i = 0; i < ns_count; i++) {
        memcpy(bytes + off, parts[i].data, parts[i].len);
        us->parts[i] = (moq_bytes_t){ bytes + off, parts[i].len };
        off += parts[i].len;
    }
    if (name.len > 0) {
        memcpy(bytes + off, name.data, name.len);
    }
    us->name = (moq_bytes_t){ bytes + off, name.len };
    us->ns_count = ns_count;
    us->key = bytes;
    us->key_len = total;
    us->has_key = true;
}

static void
usub_release(moqr_bind_t *b, b_usub_t *us)
{
    if (us->key != NULL) {
        b->alloc.free(us->key, us->key_len, b->alloc.ctx);
    }
    memset(us, 0, sizeof(*us));
}

static void
usub_release_all(moqr_bind_t *b, b_conn_t *cn)
{
    if (cn->usubs == NULL) {
        return;
    }
    for (uint32_t i = 0; i < b->n_usubs; i++) {
        if (cn->usubs[i].key != NULL || cn->usubs[i].used) {
            usub_release(b, &cn->usubs[i]);
        }
    }
}

/* The request's options (moq_fetch_request_options_t), when the event and
 * the record both reach the fields the binding reads; NULL otherwise (a
 * draft whose FETCH carries none). */
static const moq_fetch_request_options_t *
bind_fetch_options(const moq_event_t *ev)
{
    const moq_fetch_request_event_t *fq = &ev->u.fetch_request;
    if (ev->detail_size < offsetof(moq_fetch_request_event_t, options) +
                              sizeof(fq->options) ||
        fq->options == NULL ||
        fq->options->struct_size < MOQ_FETCH_REQUEST_OPTIONS_V0_SIZE) {
        return NULL;
    }
    return fq->options;
}

/* A FETCH the session surfaced with its range unresolved (every one with a
 * Location Filter, moq_session_set_defer_fetch_ranges at attach) onto the
 * core's range form: no filter starts at {0, 0}; RELATIVE_GROUP and
 * NEXT_OBJECT start relative to the Largest Object; those and ABSOLUTE_START
 * end at it. The bounded forms are explicit, and so is the event's range. */
static void
bind_fetch_range(const moq_fetch_request_options_t *fo, moqr_fetch_req_t *freq)
{
    if (fo == NULL || fo->struct_size <
            offsetof(moq_fetch_request_options_t, range_unresolved) +
                sizeof(fo->range_unresolved) || !fo->range_unresolved) {
        return;
    }
    const moq_location_filter_cfg_t *loc = &fo->location;
    switch (loc->kind) {
    case MOQ_LOCATION_FILTER_NONE:
        freq->range_form = MOQR_FETCH_END_AT_LARGEST;
        break;
    case MOQ_LOCATION_FILTER_RELATIVE_GROUP:
        freq->range_form =
            MOQR_FETCH_START_RELATIVE | MOQR_FETCH_END_AT_LARGEST;
        freq->relative_start = loc->relative_group_offset;
        break;
    case MOQ_LOCATION_FILTER_NEXT_OBJECT:
        freq->range_form =
            MOQR_FETCH_START_NEXT_OBJECT | MOQR_FETCH_END_AT_LARGEST;
        break;
    case MOQ_LOCATION_FILTER_ABSOLUTE_START:
        freq->range_form = MOQR_FETCH_END_AT_LARGEST;
        break;
    default:
        break;   /* GROUP_END / OBJECT_END: explicit */
    }
}

/* The End of Range kind for Locations a FETCH does not serve. A FETCH waits
 * only for an Object still arriving, so a Location the relay cannot vouch for
 * is marked at once: Unknown, a state the relay cannot determine (3.2.2),
 * while the FETCH's wait budget lasts, and Timed-Out once it is spent (3.2.3)
 * -- at once under a FILL_TIMEOUT of 0, which asks for any unavailable Object
 * to be reported Timed-Out (9.20.5). Only a draft whose fetch stream has End
 * of Timed-Out Range gets it; the others say Unknown throughout. */
static moq_fetch_range_kind_t
bind_fetch_unserved_kind(const b_fetch_t *bf, uint64_t now_us)
{
    return bf->fetch_timed_out_ok && now_us >= bf->fill_deadline_us
               ? MOQ_FETCH_RANGE_TIMED_OUT
               : MOQ_FETCH_RANGE_UNKNOWN;
}

/* Every downstream subgroup stream still open for a subscription to `track`
 * is reset with MALFORMED_TRACK, before the subscriptions end: the Track is
 * malformed, so nothing more is sent on them, and a reset stream counts
 * toward the PUBLISH_DONE stream count the subscriber waits for. A reset the
 * session cannot take now stays owed on the stream (b_sg_t.reset_owed): its
 * close is that reset whenever it happens, never a FIN. */
static void
bind_reset_track_streams(moqr_bind_t *b, moqr_track_t track, uint64_t now_us)
{
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        b_conn_t *cn = &b->conns[ci];
        if (!cn->used || cn->closed) continue;
        uint64_t wire = MOQ_STREAM_ERROR_MALFORMED_TRACK;
        (void)moqr_reset_encode(moqr_bind_conn_version(b, cn->session),
                                MOQR_RESET_MALFORMED_TRACK, &wire);
        for (uint32_t di = 0; di < b->n_dsubs; di++) {
            const b_dsub_t *ds = &cn->dsubs[di];
            moqr_track_t dt;
            uint64_t dg = 0;
            if (!ds->used ||
                moqr_core_sub_track(b->core, ds->rsub, &dt, &dg) != MOQR_OK ||
                !moqr_track_eq(dt, track)) {
                continue;
            }
            for (uint32_t i = 0; i < b->n_sgs; i++) {
                if (!cn->sgs[i].used || cn->sgs[i].sub_raw != ds->sub_raw) continue;
                moq_result_t rrc = moq_session_reset_subgroup(cn->session,
                                                              cn->sgs[i].h, wire,
                                                              now_us);
                if (rrc == MOQ_ERR_WOULD_BLOCK) {
                    /* Owed: the stream's next close is this reset. */
                    cn->sgs[i].reset_owed = true;
                    cn->sgs[i].reset_wire = wire;
                    continue;
                }
                cn->sgs[i].used = false;
                cn->sgs[i].obj_begun = false;
                cn->sgs[i].reset_owed = false;
                bind_dl_rearm_on_slot_release(b, cn);
            }
        }
    }
}

/* Every FETCH this relay serves for `track` from its log ends with
 * MALFORMED_TRACK (draft-22 §12.1: a relay "MUST ... reset any fetch streams
 * with Status Code MALFORMED_TRACK"): one already answered is reset, one not
 * yet answered is refused with the same code. */
static void
bind_fetches_malformed(moqr_bind_t *b, moqr_track_t track, uint64_t now_us)
{
    /* Forwarded Joining FETCHes of the Track, collected or still collecting:
     * the collection is not replayed; the upstream request, if live, is
     * cancelled; the downstream FETCH is refused or reset with
     * MALFORMED_TRACK by the forwarding pump, which retries under pressure.
     * A private fill collection stops collecting; its fill is ended below. */
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        b_fwd_t *t = &b->fwd[i];
        if (!t->used || !moqr_track_eq(t->track, track)) continue;
        if (t->consumer == FW_CONSUMER_JOIN && t->d != FW_D_TERMINAL &&
            !t->malformed) {
            b_conn_t *dcn = fwd_conn(b, t->d_cookie, t->d_binding);
            uint64_t wire = MOQ_STREAM_ERROR_MALFORMED_TRACK;
            if (dcn != NULL) {
                (void)moqr_reset_encode(moqr_bind_conn_version(b, dcn->session),
                                        MOQR_RESET_MALFORMED_TRACK, &wire);
            }
            t->malformed = true;
            t->malformed_wire = wire;
        }
        if (t->u == FW_U_PENDING || t->u == FW_U_UNSENT) {
            fwd_end_up(t, FW_END_ERROR);
            t->err_code = MOQ_REQUEST_ERROR_MALFORMED_TRACK;
        }
    }
    for (uint32_t ci = 0; ci < b->max_conns; ci++) {
        b_conn_t *cn = &b->conns[ci];
        if (!cn->used || cn->closed) continue;
        for (uint32_t fi = 0; fi < b->n_fetches; fi++) {
            b_fetch_t *bf = &cn->fetches[fi];
            if (!bf->used || bf->sa_remote) continue;
            if (bf->fill) {
                /* A fill of a subscription to the Track ends with
                 * MALFORMED_TRACK now, before the subscription's own
                 * terminal could end it as its child; a refusal keeps the
                 * abort owed with that code. */
                if (!moqr_track_eq(bf->fill_track, track)) continue;
                uint64_t code = MOQ_STREAM_ERROR_MALFORMED_TRACK;
                (void)moqr_reset_encode(moqr_bind_conn_version(b, cn->session),
                                        MOQR_RESET_MALFORMED_TRACK, &code);
                bf->fill_abort_code = code;
                bf->fill_abort_code_set = true;
                moq_result_t rc = moq_session_abort_fill(cn->session, bf->sfill,
                                                         code, now_us);
                if (rc == MOQ_ERR_WOULD_BLOCK) {
                    bf->state = BF_FILL_NEEDS_ABORT;
                } else {
                    if (rc != MOQ_OK && !fwd_result_is_gone(rc)) b->session_errors++;
                    bind_fill_release(b, bf);
                }
                continue;
            }
            moqr_track_t ft;
            uint64_t fgen = 0;
            if (moqr_core_internal_fetch_track(b->core, bf->cfetch, &ft, &fgen) !=
                    MOQR_OK ||
                !moqr_track_eq(ft, track)) {
                continue;
            }
            if (bf->state == BF_NEEDS_OK || bf->state == BF_FETCH_NEEDS_PLAN) {
                moq_fetch_t sf = bf->sfetch;
                if (bf->standalone) {
                    bind_fill_release(b, bf);
                } else {
                    (void)moqr_core_fetch_close(b->core, bf->cfetch);
                    memset(bf, 0, sizeof(*bf));
                }
                bind_fetch_refuse(b, cn, sf, MOQ_REQUEST_ERROR_MALFORMED_TRACK,
                                  now_us);
                continue;
            }
            /* Answered: the reset is owed and driven by the fetch pump. */
            bind_standalone_abort(b, cn, bf, MOQR_RESET_MALFORMED_TRACK);
        }
    }
}

/* The Track an upstream subscription or publication carries is malformed
 * (draft-22 §12.1): downstream subscriptions end with PUBLISH_DONE
 * MALFORMED_TRACK and served fetch streams are reset with it; upstream, the
 * relay as subscriber "MUST cancel any corresponding subscription or
 * fetches for that Track from that publisher". The triggering object was
 * never cached. */
static void
bind_track_malformed(moqr_bind_t *b, b_conn_t *cn, moq_subscription_t sub,
                     moq_publication_t pub, uint64_t now_us)
{
    moqr_track_t track = MOQR_TRACK_INVALID;
    uint64_t gen = 0;
    b_usub_t *us = NULL;
    b_pub_t *bp = NULL;
    if (moq_subscription_is_valid(sub)) {
        for (uint32_t i = 0; i < b->n_usubs; i++) {
            if (cn->usubs[i].used && cn->usubs[i].ssub._opaque == sub._opaque) {
                us = &cn->usubs[i];
                track = us->track;
                gen = us->track_gen;
                break;
            }
        }
    } else if (moq_publication_is_valid(pub)) {
        for (uint32_t i = 0; i < b->n_pubs; i++) {
            if (cn->pubs[i].used && cn->pubs[i].pub_raw == pub._opaque) {
                bp = &cn->pubs[i];
                track = bp->track;
                gen = bp->track_gen;
                break;
            }
        }
    }
    if (!moqr_track_is_valid(track)) {
        return;
    }
    b->malformed_tracks++;
    bind_reset_track_streams(b, track, now_us);
    moqr_result_t rc;
    BIND_CALL_RETRY(rc, moqr_core_source_done(
        b->core, track, gen, bind_local_pd(MOQR_PD_MALFORMED_TRACK), now_us));
    if (rc != MOQR_OK && rc != MOQR_ERR_STALE_HANDLE &&
        rc != MOQR_ERR_WRONG_STATE) {
        b->session_errors++;
    }
    bind_fetches_malformed(b, track, now_us);
    /* The relay as subscriber "MUST cancel any corresponding subscription"
     * (draft-22 §12.1): an obligation, not best effort. A cancellation the
     * session refuses for now stays owed on the source's entry and is
     * retried every pump (bind_retry_owed_cancels) until it commits or the
     * request is gone. */
    if (us != NULL) {
        if (bind_cancel_settled(b, cn, bind_cancel_upstream_sub(cn, us->ssub, now_us),
                                "malformed Track cancellation failed", now_us)) {
            usub_release(b, us);
        } else {
            us->cancel_owed = true;
        }
    } else if (bp != NULL) {
        uint64_t code = MOQ_STREAM_ERROR_MALFORMED_TRACK;
        (void)moqr_reset_encode(moqr_bind_conn_version(b, cn->session),
                                MOQR_RESET_MALFORMED_TRACK, &code);
        b_meta_publish_t *mp = bind_meta_pub_find(b, cn, pub._opaque);
        if (mp != NULL) bind_meta_pub_release(b, mp);
        if (bind_cancel_settled(b, cn, bind_cancel_upstream_pub(cn, pub, code, now_us),
                                "malformed Track cancellation failed", now_us)) {
            bp->used = false;
        } else {
            bp->cancel_owed = true;
            bp->cancel_code = code;
        }
    }
}

/* Whether an upstream cancellation attempt settled its obligation. The
 * session refuses it for now with WOULD_BLOCK -- a full action queue, or a
 * send buffer still holding earlier output, which the transport releases --
 * and the cancellation stays owed. It is settled when it committed or the
 * request is already gone (ended, cancelled, its session closed). Any other
 * result is a local failure that can never make the cancellation: the
 * upstream session is closed with INTERNAL_ERROR so the request cannot go on
 * with no owner, which settles it too. */
static bool
bind_cancel_settled(moqr_bind_t *b, b_conn_t *cn, moq_result_t rc, const char *what,
                    uint64_t now_us)
{
    if (rc == MOQ_ERR_WOULD_BLOCK) return false;
    if (rc != MOQ_OK && !fwd_result_is_gone(rc)) {
        b->session_errors++;
        (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR, what, now_us);
    }
    return true;
}

static moq_result_t
bind_cancel_upstream_sub(b_conn_t *cn, moq_subscription_t ssub, uint64_t now_us)
{
#ifdef MOQR_BIND_TESTING
    if (atomic_load(&bind_dbg_block_upstream_cancels) > 0) {
        (void)atomic_fetch_sub(&bind_dbg_block_upstream_cancels, 1);
        return MOQ_ERR_WOULD_BLOCK;   /* injected: the session is not called */
    }
    if (atomic_load(&bind_dbg_fail_upstream_cancels) > 0) {
        (void)atomic_fetch_sub(&bind_dbg_fail_upstream_cancels, 1);
        return MOQ_ERR_BUFFER;        /* injected: the session is not called */
    }
#endif
    return moq_session_unsubscribe(cn->session, ssub, now_us);
}

static moq_result_t
bind_cancel_upstream_pub(b_conn_t *cn, moq_publication_t pub, uint64_t code,
                         uint64_t now_us)
{
#ifdef MOQR_BIND_TESTING
    if (atomic_load(&bind_dbg_block_upstream_cancels) > 0) {
        (void)atomic_fetch_sub(&bind_dbg_block_upstream_cancels, 1);
        return MOQ_ERR_WOULD_BLOCK;   /* injected: the session is not called */
    }
#endif
    return moq_session_cancel_publication(cn->session, pub, code, now_us);
}

/* Retry the upstream cancellations owed by malformed Tracks, each settled
 * as bind_cancel_settled decides. */
static void
bind_retry_owed_cancels(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    for (uint32_t i = 0; i < b->n_usubs; i++) {
        b_usub_t *us = &cn->usubs[i];
        if (!us->used || !us->cancel_owed) continue;
        if (bind_cancel_settled(b, cn, bind_cancel_upstream_sub(cn, us->ssub, now_us),
                                "malformed Track cancellation failed", now_us)) {
            usub_release(b, us);
        }
    }
    for (uint32_t i = 0; i < b->n_pubs; i++) {
        b_pub_t *bp = &cn->pubs[i];
        if (!bp->used || !bp->cancel_owed) continue;
        moq_publication_t pub = { bp->pub_raw };
        if (bind_cancel_settled(b, cn,
                                bind_cancel_upstream_pub(cn, pub, bp->cancel_code, now_us),
                                "malformed Track cancellation failed", now_us)) {
            memset(bp, 0, sizeof(*bp));
        }
    }
}

static void
bind_fetch_terminate(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    b->session_errors++;
    (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                            "fetch error", now_us);
    conn_detach(b, cn, now_us);
}

static void
bind_fetch_pump(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    for (uint32_t fi = 0; fi < b->n_fetches; fi++) {
        b_fetch_t *bf = &cn->fetches[fi];
        if (!bf->used) {
            continue;
        }
        if (bf->fill) {
            uint32_t guard = 0;
            for (; guard < 512 && bf->used; guard++) {
                if (!bind_fill_step(b, cn, bf, now_us)) {
                    break;
                }
            }
            if (guard == 512 && bf->used) b->budget_pending = true;
            continue;
        }
        if (bf->standalone) {
            for (uint32_t guard = 0; guard < 512 && bf->used; guard++) {
                if (!bind_standalone_step(b, cn, bf, now_us)) {
                    break;
                }
            }
            continue;
        }
        /* Drive this fetch as far as the session queue allows; every step holds
         * (breaks, retried next pump) on WOULD_BLOCK with no state advance. */
        for (uint32_t guard = 0; guard < 512; guard++) {
            if (bf->state == BF_FETCH_NEEDS_ABORT) {
                moq_result_t arc = moq_session_abort_fetch(
                    cn->session, bf->sfetch, bf->sa_abort_code, now_us);
                if (arc == MOQ_ERR_WOULD_BLOCK) {
                    break;
                }
                if (arc != MOQ_OK && !fwd_result_is_gone(arc)) {
                    b->session_errors++;
                }
                (void)moqr_core_fetch_close(b->core, bf->cfetch);
                memset(bf, 0, sizeof(*bf));
                break;
            }
            if (bf->state == BF_NEEDS_OK) {
                /* The track's properties are stated (§7.7); a set this
                 * session cannot carry refuses the fetch instead. */
                moqr_bind_meta_ref_t pin = {0};
                if (!bind_fetch_ok_pin(b, cn, bf->cfetch, &pin)) {
                    moqr_bind_meta_release(b, &pin);
                    moq_fetch_t sf = bf->sfetch;
                    (void)moqr_core_fetch_close(b->core, bf->cfetch);
                    memset(bf, 0, sizeof(*bf));
                    bind_fetch_refuse(b, cn, sf,
                                      MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
                    break;
                }
                moq_accept_fetch_cfg_t ac;
                moq_accept_fetch_cfg_init(&ac);
                ac.end_of_track = bf->eot;
                ac.end_group = bf->end_group;
                ac.end_object = bf->end_object;
                ac.track_properties = pin.bytes;
                moq_result_t rc = moq_session_accept_fetch(cn->session,
                                                           bf->sfetch, &ac,
                                                           now_us);
                moqr_bind_meta_release(b, &pin);
                if (rc == MOQ_ERR_WOULD_BLOCK) {
                    break;
                }
                if (rc == MOQ_ERR_INVAL) {
                    /* The session holds the End against the request's form
                     * and refused it: the fetch is still pending, so it is
                     * answered with a refusal instead of a FETCH_OK. */
                    moq_fetch_t sf = bf->sfetch;
                    (void)moqr_core_fetch_close(b->core, bf->cfetch);
                    memset(bf, 0, sizeof(*bf));
                    b->session_errors++;
                    bind_fetch_refuse(b, cn, sf,
                                      MOQ_REQUEST_ERROR_INTERNAL_ERROR, now_us);
                    break;
                }
                if (rc != MOQ_OK) {
                    bind_fetch_terminate(b, cn, now_us);   /* FETCH_OK failed */
                    return;
                }
                bf->state = BF_STREAMING;
                continue;
            }
            if (bf->state == BF_STREAMING) {
                moqr_fetch_item_t it;
                moqr_result_t prc = moqr_core_fetch_peek(b->core, bf->cfetch,
                                                         now_us, &it);
                if (prc == MOQR_ERR_WOULD_BLOCK) {
                    break;   /* pin budget momentarily full; retry next pump */
                }
                if (prc != MOQR_OK) {
                    /* TOO_OLD / CAPACITY: the accepted range was lost mid-fetch
                     * (only reachable under ACTIVE-track capacity eviction; the
                     * lifetime guard pins WARM tracks). The peer already saw
                     * FETCH_OK, so a clean END would FIN a truncated range and a
                     * silent drop would hang it — give it a terminal via a hard
                     * close. */
                    bind_fetch_terminate(b, cn, now_us);
                    return;
                }
                bf->fetch_held_pending = false;
                if (it.kind == MOQR_FETCH_ITEM_END) {
                    bf->state = BF_NEEDS_FIN;
                    continue;
                }
                if (it.kind == MOQR_FETCH_ITEM_PENDING &&
                    now_us < bf->fill_deadline_us) {
                    /* An Object still arriving that the range includes: it is
                     * served in full once it completes (3.2), waited for
                     * until the FETCH's deadline (moqr_bind_next_deadline_us
                     * wakes the binding then). */
                    bf->fetch_held_pending = true;
                    break;
                }
                if (it.kind == MOQR_FETCH_ITEM_MARKER ||
                    it.kind == MOQR_FETCH_ITEM_GAP ||
                    it.kind == MOQR_FETCH_ITEM_UNKNOWN ||
                    it.kind == MOQR_FETCH_ITEM_PENDING) {
                    /* Locations the response will not serve: the evicted
                     * prefix (before the first retained group), a hole, a
                     * run the log has no evidence about, or an Object still
                     * arriving at the deadline. In every draft an unmarked
                     * gap in a FETCH response says the skipped Objects do
                     * not exist, so each is marked, at once (a FETCH does
                     * not wait for upstream), with bind_fetch_unserved_kind.
                     * A run ending with its group goes through the
                     * profile-owned seam that knows the draft's last object
                     * id. Same retry model as objects: HOLD on WOULD_BLOCK
                     * (re-peek re-emits it), terminal on any hard error, and
                     * commit (the cursor advance) ONLY after the session
                     * write succeeds. */
                    const moq_fetch_range_kind_t kind =
                        bind_fetch_unserved_kind(bf, now_us);
                    moq_result_t mrc;
                    if (it.kind == MOQR_FETCH_ITEM_MARKER) {
                        mrc = moq_session_write_fetch_range_before_group(
                            cn->session, bf->sfetch, kind, it.marker_group,
                            now_us);
                    } else if (it.rec.object_id == UINT64_MAX &&
                               it.rec.group_id != UINT64_MAX) {
                        mrc = moq_session_write_fetch_range_before_group(
                            cn->session, bf->sfetch, kind,
                            it.rec.group_id + 1u, now_us);
                    } else {
                        mrc = moq_session_write_fetch_range(
                            cn->session, bf->sfetch, kind, it.rec.group_id,
                            it.rec.object_id, now_us);
                    }
                    if (mrc == MOQ_ERR_WOULD_BLOCK) {
                        break;   /* HOLD: do not commit */
                    }
                    if (mrc != MOQ_OK) {
                        bind_fetch_terminate(b, cn, now_us);
                        return;
                    }
                    (void)moqr_core_fetch_commit(b->core, bf->cfetch);
                    continue;
                }
                moq_fetch_object_cfg_t oc;
                moq_fetch_object_cfg_init(&oc);
                oc.group_id = it.rec.group_id;
                oc.subgroup_id = it.rec.subgroup_id;
                oc.object_id = it.rec.object_id;
                oc.publisher_priority = it.rec.publisher_priority;
                /* Datagram preference is forwarding metadata, not object
                 * identity. Draft-18 fetch headers can encode it; draft-16
                 * cannot, so for a draft-16 fetcher clear the bit and serve the
                 * object normally (same group/object/payload) rather than fail
                 * the write. The capability query never leaks the version. */
                oc.datagram = moq_session_supports_fetch_datagram(cn->session)
                                  ? it.rec.datagram_pref
                                  : false;
                oc.payload = (moq_rcbuf_t *)(void *)it.rec.payload;
                oc.properties = (moq_rcbuf_t *)(void *)it.rec.properties;
                moq_result_t wrc = moq_session_write_fetch_object(
                    cn->session, bf->sfetch, &oc, now_us);
                if (wrc == MOQ_ERR_WOULD_BLOCK) {
                    break;   /* HOLD: do not commit; re-peek re-emits it */
                }
                if (wrc != MOQ_OK) {
                    /* The object cannot be written (defensive: the cursor only
                     * yields encodable NORMAL objects, and the datagram bit is
                     * already cleared for a draft-16 fetcher), so the fetch
                     * cannot complete: terminal. */
                    bind_fetch_terminate(b, cn, now_us);
                    return;
                }
                (void)moqr_core_fetch_commit(b->core, bf->cfetch);
                b->deliveries_written++;
                continue;
            }
            /* BF_NEEDS_FIN */
            moq_result_t rc = moq_session_end_fetch(cn->session, bf->sfetch,
                                                    now_us);
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                break;
            }
            if (rc != MOQ_OK) {
                bind_fetch_terminate(b, cn, now_us);   /* FIN failed: terminal */
                return;
            }
            (void)moqr_core_fetch_close(b->core, bf->cfetch);
            bf->used = false;
            break;
        }
    }
}

/* Stream one COMPLETE chunked record downstream: begin_object once, then
 * write_object_data for each retained chunk in order, then end_object. Progress
 * (obj_begun / obj_id / next_chunk) lives on the sg slot so a WOULD_BLOCK at any
 * step HOLDS and the next pump resumes the SAME chunk — no duplicate, no skip
 * (each write_object_data is retry-safe: capacity is gated before any incref).
 * Returns MOQ_OK only when the whole object shipped (begin + all chunks + end);
 * MOQ_ERR_WOULD_BLOCK to hold; another error to fail the delivery. Chunks are
 * read from the core's pinned array — never from the log directly. */
/* Map a single session write result to a delivery outcome (whole-object,
 * status, and datagram paths, which each emit exactly one action). */
static moqr_delivery_outcome_t
oc_from_result(moq_result_t wrc)
{
    return wrc == MOQ_OK              ? MOQR_DELIVERY_DELIVERED
           : wrc == MOQ_ERR_WOULD_BLOCK ? MOQR_DELIVERY_WOULD_BLOCK
                                        : MOQR_DELIVERY_STREAM_ERROR;
}

static moqr_delivery_outcome_t
bind_write_chunked(moqr_bind_t *b, b_conn_t *cn, const moqr_delivery_t *d,
                   b_sg_t *sg, uint64_t now_us)
{
    if (!sg->obj_begun || sg->obj_id != d->rec.object_id) {
        moq_begin_object_cfg_t bc;
        moq_begin_object_cfg_init(&bc);
        bc.object_id = d->rec.object_id;
        bc.payload_length = d->rec.declared_len;   /* known at open (OPEN too) */
        bc.properties = (moq_rcbuf_t *)(void *)d->rec.properties;
        moq_result_t brc =
            moq_session_begin_object_ex(cn->session, sg->h, &bc, now_us);
        if (brc == MOQ_ERR_WOULD_BLOCK) {
            return MOQR_DELIVERY_WOULD_BLOCK;   /* subgroup stays OPEN, retry */
        }
        if (brc != MOQ_OK) {
            return MOQR_DELIVERY_STREAM_ERROR;
        }
        sg->obj_begun = true;
        sg->obj_id = d->rec.object_id;
        sg->next_chunk = 0;
    }
    while (sg->next_chunk < d->rec.chunk_count) {
        const moq_rcbuf_t *cb = NULL;
        uint64_t clen = 0;
        if (moqr_core_delivery_chunk(b->core, cn->binding, sg->next_chunk, &cb,
                                     &clen) != MOQR_OK) {
            return MOQR_DELIVERY_STREAM_ERROR;   /* pinned chunk missing */
        }
        moq_result_t wrc = moq_session_write_object_data(
            cn->session, sg->h, (moq_rcbuf_t *)(void *)cb, now_us);
        if (wrc == MOQ_ERR_WOULD_BLOCK) {
            /* Hold: checkpoint that the object is begun downstream and how many
             * chunks have shipped (sg->next_chunk, which is <= chunk_count and
             * may be 0 when begin_object succeeded but this first chunk blocked).
             * This keeps the core's begun watermark accurate so that, if a
             * Forward pause releases this partial and the group is then
             * abandoned/evicted, the required RESET still surfaces. SAME chunk
             * retries next pump. next_chunk is in range, so INVAL here is a
             * consistency break, not a normal hold. */
            if (moqr_core_delivery_note_emitted(b->core, cn->binding,
                                                sg->next_chunk, now_us) !=
                MOQR_OK) {
                return MOQR_DELIVERY_STREAM_ERROR;
            }
            return MOQR_DELIVERY_WOULD_BLOCK;
        }
        if (wrc != MOQ_OK) {
            return MOQR_DELIVERY_STREAM_ERROR;
        }
        sg->next_chunk++;
    }
    if (d->rec.obj_state != MOQR_OBJ_COMPLETE) {
        /* OPEN live edge: every currently-exposed chunk shipped but the
         * object is not complete yet — HOLD here without ending it. The core
         * records the emitted watermark on STALLED and re-selects this head only
         * when it grows or flips terminal, so other subs are not HOL-blocked. */
        return MOQR_DELIVERY_STALLED;
    }
    moq_result_t erc = moq_session_end_object(cn->session, sg->h, now_us);
    if (erc != MOQ_OK) {
        return MOQR_DELIVERY_STREAM_ERROR;   /* end_object never WOULD_BLOCKs */
    }
    sg->obj_begun = false;   /* object complete; the slot can begin the next */
    return MOQR_DELIVERY_DELIVERED;
}

/* Confirm a delivery outcome to the core. delivery_done's error vocabulary
 * (INVAL / STALE_HANDLE / WRONG_STATE) is all consistency failures — never
 * transient — so any non-OK confirmation fails the session closed and
 * detaches; a binding whose confirmations misfire must not stay live with
 * an unacknowledged delivery. Returns false when the connection was
 * detached (the caller returns B_DELIVER_CLOSED). */
static bool
bind_confirm_delivery(moqr_bind_t *b, b_conn_t *cn,
                      moqr_delivery_outcome_t oc, uint64_t now_us)
{
    moqr_result_t crc;
#ifdef MOQR_BIND_TESTING
    if (atomic_load(&bind_dbg_fail_confirms) > 0 &&
        atomic_fetch_sub(&bind_dbg_fail_confirms, 1) == 1) {
        /* Injected consistency failure: the core is NOT called, so the
         * outstanding delivery stays live — teardown must cope with it. */
        crc = MOQR_ERR_WRONG_STATE;
    } else
#endif
    {
        crc = moqr_core_delivery_done(b->core, cn->binding, oc, now_us);
    }
    if (crc == MOQR_OK) {
        return true;
    }
    b->session_errors++;
    (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                            "delivery confirm failed", now_us);
    conn_detach(b, cn, now_us);
    return false;
}

/* One delivery pass over a connection, ending in exactly one outcome that
 * tells Phase F how to reschedule it. STALLED and ABANDONED remain in-loop
 * continuations (sibling subscriptions keep being served in the same pass);
 * every delivery_done error is a consistency failure — deterministic contract
 * violations are never retried. */
typedef enum b_deliver_outcome {
    B_DELIVER_DRAINED,            /* nothing deliverable now                */
    B_DELIVER_BLOCKED_ACTION_CAP, /* action-queue WB: capacity-floor gate   */
    B_DELIVER_BLOCKED_SESSION_SG, /* subgroup-open WB with queue room       */
    B_DELIVER_BLOCKED_BIND_SG,    /* downstream WB: bind subgroup slots     */
    B_DELIVER_BUDGET,             /* 256-delivery guard expired, work left  */
    B_DELIVER_RETRY,              /* transient CAPACITY / stream error:
                                   * sibling work needs a self-rearm        */
    B_DELIVER_CLOSED,             /* conn detached or consistency failure   */
} b_deliver_outcome_t;

static b_deliver_outcome_t
bind_deliver(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    for (uint32_t guard = 0; guard < 256; guard++) {
        moqr_delivery_t d;
        moqr_result_t drc;
#ifdef MOQR_BIND_TESTING
        atomic_fetch_add(&bind_dbg_probes, 1);
        if (atomic_load(&bind_dbg_fail_probes) > 0 &&
            atomic_fetch_sub(&bind_dbg_fail_probes, 1) == 1) {
            drc = bind_dbg_probe_result;   /* injected; d stays unused */
        } else
#endif
        {
            drc = moqr_core_next_delivery(b->core, cn->binding, now_us, &d);
        }
        if (drc != MOQR_OK) {
            if (drc == MOQR_DONE) {
                return B_DELIVER_DRAINED;   /* nothing NOW; a mark re-arms */
            }
            if (drc == MOQR_ERR_CAPACITY) {
                /* Transient scheduler capacity (pin array / table growth):
                 * nothing was consumed; self-rearm so sibling work is never
                 * stranded waiting for a mark that will not come. */
                return B_DELIVER_RETRY;
            }
            /* STALE_HANDLE (the core binding vanished under a live conn) or
             * any other error is a consistency failure: fail closed rather
             * than strand an unschedulable connection. */
            b->session_errors++;
            (void)moq_session_close(cn->session, BIND_SESSION_INTERNAL_ERROR,
                                    "delivery probe failed", now_us);
            conn_detach(b, cn, now_us);
            return B_DELIVER_CLOSED;
        }
        moq_subscription_t ssub = { d.sub_cookie };
        /* Before any notice, open or write for this request: if the session
         * already ended it, nothing more goes out for it. The outstanding
         * delivery is released as a stream error through the checked
         * confirmation (its pins dropped once, nothing counted as delivered;
         * the core retires the subscription) -- a confirmation that fails
         * has detached the connection, and nothing of it is touched again --
         * then the whole request is cleaned up -- its slots, its owner --
         * leaving the session's own resets and terminal to the session. */
        {
            const b_dsub_t *gds = dsub_find(b, cn, d.sub_cookie);
            if (gds != NULL && bind_request_ended_by_session(cn, gds)) {
                if (!bind_confirm_delivery(b, cn, MOQR_DELIVERY_STREAM_ERROR,
                                           now_us)) {
                    return B_DELIVER_CLOSED;
                }
                b_dsub_t *eds = dsub_find(b, cn, d.sub_cookie);
                if (eds != NULL) {
                    bind_dsub_retire(b, cn, eds, now_us);
                }
                continue;
            }
        }
        bool rec_props = false, rec_gap = false;
        moqr_reset_desc_t rec_gap_reset = moqr_reset_desc_none();
        if (d.notice == MOQR_DELIVERY_NOTICE_NONE) {
            (void)moqr_core_internal_delivery_flags(b->core, cn->binding,
                                                    &rec_props, &rec_gap,
                                                    &rec_gap_reset);
        }
        /* Recordless acknowledged notices precede record work. The close IS
         * the payload here, so the notice is acknowledged (DELIVERED) only
         * once every close it names is on the wire; a WOULD_BLOCK holds the
         * notice and the next pump retries exactly where it left off (closed
         * slots are already cleared). A hard close error fails closed — the
         * peer must not be left with a stream that will never FIN. */
        if (d.notice != MOQR_DELIVERY_NOTICE_NONE) {
            bool blocked = false, faulted = false;
            for (uint32_t i = 0; i < b->n_sgs; i++) {
                if (!cn->sgs[i].used || cn->sgs[i].sub_raw != d.sub_cookie) {
                    continue;
                }
                if (d.notice == MOQR_DELIVERY_NOTICE_SEAL
                        ? (cn->sgs[i].group != d.rec.group_id ||
                           cn->sgs[i].subgroup != d.rec.subgroup_id)
                        /* EVICT_WATERMARK: any open subgroup of a group BELOW
                         * the track's oldest RETAINED group is gone (its
                         * records are evicted; the group id is never
                         * re-admitted) and can never receive more objects.
                         * The watermark is the oldest retained group, NOT a
                         * delivered record's group: the schedule is not
                         * strictly ascending, so a lower group can still be
                         * live only at/above it. */
                        : cn->sgs[i].group >= d.oldest_group) {
                    continue;
                }
                /* A reset-flavored seal closes abnormally, forwarding the
                 * upstream RESET code — the subgroup's content is truncated
                 * at the source, and a clean FIN would misrepresent it as
                 * complete. Same retry discipline either way. */
                uint64_t seal_wire = 0;
                bool seal_is_reset = d.notice == MOQR_DELIVERY_NOTICE_SEAL &&
                                     d.seal_reset;
                /* An eviction watermark orphans the subgroup with objects the
                 * downstream never got (or may not have got): it is reset,
                 * never FINed, since a FIN says every object was delivered
                 * (§11.3.2; §7.4). */
                moqr_reset_desc_t seal_desc = d.seal_reset_desc;
                if (d.notice == MOQR_DELIVERY_NOTICE_EVICT_WATERMARK) {
                    seal_is_reset = moqr_reset_desc_internal(
                        MOQR_RESET_CODE_EVICTED, &seal_desc) == MOQR_OK;
                }
                if (cn->sgs[i].reset_owed) {
                    seal_is_reset = true;   /* the owed reset, as recorded */
                    seal_wire = cn->sgs[i].reset_wire;
                } else if (seal_is_reset &&
                    !bind_reset_wire(b, cn, seal_desc, &seal_wire)) {
                    (void)moq_session_close(cn->session,
                                            BIND_SESSION_INTERNAL_ERROR,
                                            "untranslatable reset", now_us);
                    conn_detach(b, cn, now_us);
                    return B_DELIVER_CLOSED;
                }
                moq_result_t crc =
                    seal_is_reset
                        ? moq_session_reset_subgroup(cn->session,
                                                     cn->sgs[i].h, seal_wire,
                                                     now_us)
                        : moq_session_close_subgroup(cn->session,
                                                     cn->sgs[i].h, now_us);
                if (crc == MOQ_ERR_WOULD_BLOCK) {
                    blocked = true;
                    break;
                }
                /* STALE_HANDLE on this exact stream (its full handle, so
                 * never a later stream that reused its pool slot): the
                 * session already ended it on its own -- the peer's
                 * STOP_SENDING, a delivery timeout, its own reset -- and owns
                 * that stream's terminal. The request is live (checked
                 * above), and a single subgroup's end does not touch the
                 * others or the subscription (draft-22 section 11.3.2,
                 * draft-18 section 11.4.3), so only this slot is released
                 * and the notice acknowledged. Nothing is sent in its place:
                 * a stale stream is not turned into a FIN, nor reset again,
                 * and is no evidence the peer received its objects. Any
                 * other refusal -- WRONG_STATE on a stream mid-object
                 * included -- is a hard error. */
                if (crc != MOQ_OK && crc != MOQ_ERR_STALE_HANDLE) {
                    faulted = true;
                    break;
                }
                cn->sgs[i].used = false;
                cn->sgs[i].obj_begun = false;
                cn->sgs[i].reset_owed = false;
                bind_dl_rearm_on_slot_release(b, cn);
                if (d.notice == MOQR_DELIVERY_NOTICE_SEAL) {
                    break;   /* a seal names exactly one subgroup */
                }
            }
            if (faulted) {
                b->session_errors++;
                (void)moq_session_close(cn->session,
                                        BIND_SESSION_INTERNAL_ERROR,
                                        "notice close failed", now_us);
                conn_detach(b, cn, now_us);
                return B_DELIVER_CLOSED;
            }
            if (!bind_confirm_delivery(b, cn,
                                       blocked ? MOQR_DELIVERY_WOULD_BLOCK
                                               : MOQR_DELIVERY_DELIVERED,
                                       now_us)) {
                return B_DELIVER_CLOSED;
            }
            if (blocked) {
                /* held: the close is an ordinary session write, so this is
                 * action-queue backpressure — resume above the floor */
                return B_DELIVER_BLOCKED_ACTION_CAP;
            }
            continue;
        }
        moqr_delivery_outcome_t oc;
        if (d.rec.datagram_pref) {
            moq_result_t wrc;
            if (d.rec.payload == NULL) {
                wrc = bind_cookie_is_publication(d.sub_cookie)
                    ? moq_session_send_pub_status_datagram(
                          cn->session,
                          (moq_publication_t){ ._opaque = d.sub_cookie },
                          d.rec.group_id, d.rec.object_id,
                          d.rec.publisher_priority,
                          (moq_object_status_t)d.rec.status, now_us)
                    : moq_session_send_status_datagram(
                          cn->session, ssub, d.rec.group_id, d.rec.object_id,
                          d.rec.publisher_priority,
                          (moq_object_status_t)d.rec.status, now_us);
            } else {
                const uint8_t *pb = NULL;
                size_t pl = 0;
                if (d.rec.properties != NULL) {
                    pb = moq_rcbuf_data(d.rec.properties);
                    pl = moq_rcbuf_len(d.rec.properties);
                }
                wrc = bind_cookie_is_publication(d.sub_cookie)
                    ? moq_session_send_pub_object_datagram(
                          cn->session,
                          (moq_publication_t){ ._opaque = d.sub_cookie },
                          d.rec.group_id, d.rec.object_id,
                          d.rec.publisher_priority, d.rec.end_of_group,
                          (moq_rcbuf_t *)(void *)d.rec.payload, pb, pl, now_us)
                    : moq_session_send_object_datagram(
                          cn->session, ssub, d.rec.group_id, d.rec.object_id,
                          d.rec.publisher_priority, d.rec.end_of_group,
                          (moq_rcbuf_t *)(void *)d.rec.payload, pb, pl, now_us);
            }
            oc = oc_from_result(wrc);
        } else {
            int sgi = -1, freei = -1;
            for (uint32_t i = 0; i < b->n_sgs; i++) {
                if (cn->sgs[i].used && cn->sgs[i].sub_raw == d.sub_cookie &&
                    cn->sgs[i].group == d.rec.group_id &&
                    cn->sgs[i].subgroup == d.rec.subgroup_id) {
                    sgi = (int)i;
                    break;
                }
                if (!cn->sgs[i].used && freei < 0) {
                    freei = (int)i;
                }
            }
            if (d.rec.obj_state == MOQR_OBJ_ABANDONED && d.evicted_reset) {
                /* The whole group was evicted while begun downstream: reset EVERY
                 * begun subgroup of the group so no peer stream is left with a
                 * begun object and no terminal. Retry-safe — a reset subgroup is
                 * cleared (used=false), so a WOULD_BLOCK mid-way holds the
                 * delivery and the next pump resumes with the remaining ones. A
                 * hard reset error fails closed (terminate the connection). */
                bool blocked = false, faulted = false;
                for (uint32_t i = 0; i < b->n_sgs; i++) {
                    if (!cn->sgs[i].used || cn->sgs[i].sub_raw != d.sub_cookie ||
                        cn->sgs[i].group != d.rec.group_id ||
                        !cn->sgs[i].obj_begun) {
                        continue;
                    }
                    uint64_t rec_wire = 0;
                    if (!bind_reset_wire(b, cn, d.rec.reset, &rec_wire)) {
                        (void)moq_session_close(cn->session,
                                                BIND_SESSION_INTERNAL_ERROR,
                                                "untranslatable reset", now_us);
                        conn_detach(b, cn, now_us);
                        return B_DELIVER_CLOSED;
                    }
                    moq_result_t rrc = moq_session_reset_subgroup(
                        cn->session, cn->sgs[i].h, rec_wire, now_us);
                    if (rrc == MOQ_ERR_WOULD_BLOCK) {
                        blocked = true;
                        break;
                    }
                    if (rrc != MOQ_OK) {
                        faulted = true;
                        break;
                    }
                    cn->sgs[i].used = false;
                    cn->sgs[i].obj_begun = false;
                    bind_dl_rearm_on_slot_release(b, cn);
                }
                if (faulted) {
                    b->session_errors++;
                    (void)moq_session_close(cn->session,
                                            BIND_SESSION_INTERNAL_ERROR,
                                            "evict reset failed", now_us);
                    conn_detach(b, cn, now_us);
                    return B_DELIVER_CLOSED;
                }
                if (blocked) {
                    if (!bind_confirm_delivery(b, cn,
                                               MOQR_DELIVERY_WOULD_BLOCK,
                                               now_us)) {
                        return B_DELIVER_CLOSED;
                    }
                    return B_DELIVER_BLOCKED_ACTION_CAP;
                }
                oc = MOQR_DELIVERY_ABANDONED;
            } else if (d.rec.obj_state == MOQR_OBJ_ABANDONED) {
                /* Abandoned head, selected only because this sub already began
                 * the object downstream: RESET the existing subgroup so
                 * the peer sees an abnormal end, not a stuck object — never open
                 * a fresh one. The reset forwards the record's stored terminal
                 * code (d.rec.reset_code): the publisher's RESET code, or a
                 * relay-defined code for STOP / eviction. The core reports
                 * ABANDONED only after the reset is on the wire, so a missing
                 * begun subgroup or a failed reset FAILS CLOSED (terminate the
                 * connection) rather than silently advancing. */
                if (sgi < 0 || !cn->sgs[sgi].obj_begun ||
                    cn->sgs[sgi].obj_id != d.rec.object_id) {
                    /* Desync: core scheduled chunks (emitted>0) but the bind has
                     * no matching begun subgroup. Do not advance without a wire
                     * terminal. */
                    b->session_errors++;
                    (void)moq_session_close(cn->session,
                                            BIND_SESSION_INTERNAL_ERROR,
                                            "abandon desync", now_us);
                    conn_detach(b, cn, now_us);
                    return B_DELIVER_CLOSED;
                }
                uint64_t rec_wire2 = 0;
                if (!bind_reset_wire(b, cn, d.rec.reset, &rec_wire2)) {
                    (void)moq_session_close(cn->session,
                                            BIND_SESSION_INTERNAL_ERROR,
                                            "untranslatable reset", now_us);
                    conn_detach(b, cn, now_us);
                    return B_DELIVER_CLOSED;
                }
                moq_result_t rrc = moq_session_reset_subgroup(
                    cn->session, cn->sgs[sgi].h, rec_wire2, now_us);
                if (rrc == MOQ_ERR_WOULD_BLOCK) {
                    /* Retry the reset next pump; the core holds the delivery. */
                    if (!bind_confirm_delivery(b, cn,
                                               MOQR_DELIVERY_WOULD_BLOCK,
                                               now_us)) {
                        return B_DELIVER_CLOSED;
                    }
                    return B_DELIVER_BLOCKED_ACTION_CAP;
                }
                if (rrc != MOQ_OK) {
                    /* Reset could not be put on the wire: fail closed — the peer
                     * must see a terminal for the begun object, so terminate the
                     * connection (its close IS the terminal). */
                    b->session_errors++;
                    (void)moq_session_close(cn->session,
                                            BIND_SESSION_INTERNAL_ERROR,
                                            "abandon reset failed", now_us);
                    conn_detach(b, cn, now_us);
                    return B_DELIVER_CLOSED;
                }
                cn->sgs[sgi].used = false;   /* reset shipped: stream finished */
                cn->sgs[sgi].obj_begun = false;
                bind_dl_rearm_on_slot_release(b, cn);
                oc = MOQR_DELIVERY_ABANDONED;
            } else {
                /* The open downstream stream cannot carry this record: an
                 * object of the subgroup before it was not forwarded
                 * (refused at ingest, or expired from cache), so it is not
                 * known to be the next object (§11.3.2: the relay "MUST
                 * reset the Subgroup stream and open a new one to forward
                 * it"); or it has properties the stream was opened without.
                 * A record already begun on the stream continues there. */
                if (sgi >= 0 &&
                    !(cn->sgs[sgi].obj_begun &&
                      cn->sgs[sgi].obj_id == d.rec.object_id) &&
                    (rec_gap ||
                     (d.rec.properties != NULL && !cn->sgs[sgi].props))) {
                    moqr_reset_desc_t rd = moqr_reset_desc_none();
                    uint64_t rwire = 0;
                    /* The stream the record does not continue ends as its
                     * upstream stream ended when that was a RESET (the
                     * upstream code), else as a truncated one. */
                    moqr_result_t drc = !rec_gap
                        ? moqr_reset_desc_local(MOQR_RESET_INTERNAL_ERROR, &rd)
                        : rec_gap_reset.tag != MOQR_CODE_NONE
                            ? (rd = rec_gap_reset, MOQR_OK)
                            : moqr_reset_desc_internal(MOQR_RESET_CODE_EVICTED, &rd);
                    if (drc != MOQR_OK || !bind_reset_wire(b, cn, rd, &rwire)) {
                        (void)moq_session_close(cn->session,
                                                BIND_SESSION_INTERNAL_ERROR,
                                                "untranslatable reset", now_us);
                        conn_detach(b, cn, now_us);
                        return B_DELIVER_CLOSED;
                    }
                    moq_result_t rrc = moq_session_reset_subgroup(
                        cn->session, cn->sgs[sgi].h, rwire, now_us);
                    if (rrc == MOQ_ERR_WOULD_BLOCK) {
                        if (!bind_confirm_delivery(b, cn,
                                                   MOQR_DELIVERY_REFUSED_UNBEGUN,
                                                   now_us)) {
                            return B_DELIVER_CLOSED;
                        }
                        return B_DELIVER_BLOCKED_ACTION_CAP;
                    }
                    if (rrc != MOQ_OK) {
                        b->session_errors++;
                        (void)moq_session_close(cn->session,
                                                BIND_SESSION_INTERNAL_ERROR,
                                                "gap reset failed", now_us);
                        conn_detach(b, cn, now_us);
                        return B_DELIVER_CLOSED;
                    }
                    cn->sgs[sgi].used = false;
                    cn->sgs[sgi].obj_begun = false;
                    bind_dl_rearm_on_slot_release(b, cn);
                    freei = sgi;
                    sgi = -1;
                }
                if (sgi < 0) {
                    moq_subgroup_cfg_t sc;
                    moq_subgroup_cfg_init(&sc);
                    sc.group_id = d.rec.group_id;
                    sc.subgroup_id = d.rec.subgroup_id;
                    sc.publisher_priority = d.rec.publisher_priority;
                    /* The upstream stream's form, per record: a record
                     * appended without it opens with properties exactly
                     * when it carries some. */
                    sc.object_properties = rec_props ||
                                           d.rec.properties != NULL;
                    /* Header-EOG evidence rides the subgroup header on every
                     * record of the upstream subgroup, so the first record
                     * (which opens the downstream stream) preserves the bit. */
                    sc.end_of_group = d.rec.end_of_group;
                    /* §2.2: a relay forwarding a subgroup that begins with
                     * the first object ever published in it MUST set
                     * FIRST_OBJECT; only that record carries the mark, so a
                     * stream opened at any later record leaves it clear. */
                    sc.first_object = d.rec.first_object;
                    moq_subgroup_handle_t h;
                    if (freei < 0) {
                        /* Bind subgroup slots exhausted — never reaches the
                         * session, and provably BEFORE any downstream write
                         * (a mid-object batch would have found its subgroup
                         * open, sgi >= 0). Confirmed as REFUSED_UNBEGUN so
                         * the core releases a chunked outstanding delivery
                         * instead of holding it — a held pre-begin record
                         * would starve the SEAL notices whose closes free
                         * the slot it needs. No parked state: every
                         * slot-release site on this still-open connection
                         * re-arms readiness. */
#ifdef MOQR_BIND_TESTING
                        /* Attribute the refusal to the OFFENDING track
                         * (first latches; a different track — a different
                         * demand — or an unresolvable handle marks the
                         * conn ambiguous: the join must fail closed, never
                         * pick between candidates). */
                        {
                            moqr_track_t dtr;
                            uint64_t dtg;
                            if (moqr_core_sub_track(b->core, d.sub, &dtr,
                                                    &dtg) != MOQR_OK) {
                                cn->dbg_bind_sg_ambiguous = true;
                            } else if (!cn->dbg_bind_sg_track_set) {
                                cn->dbg_bind_sg_track = dtr._opaque;
                                cn->dbg_bind_sg_track_gen = dtg;
                                cn->dbg_bind_sg_track_set = true;
                            } else if (cn->dbg_bind_sg_track !=
                                           dtr._opaque ||
                                       cn->dbg_bind_sg_track_gen != dtg) {
                                cn->dbg_bind_sg_ambiguous = true;
                            }
                        }
#endif
                        if (!bind_confirm_delivery(
                                b, cn, MOQR_DELIVERY_REFUSED_UNBEGUN,
                                now_us)) {
                            return B_DELIVER_CLOSED;
                        }
                        return B_DELIVER_BLOCKED_BIND_SG;
                    }
                    b_dsub_t *ds = dsub_find(b, cn, d.sub_cookie);
                    if (ds == NULL) {
                        /* Delivery cannot overtake a parked ACCEPT. If its
                         * request owner is gone, retire only this core sub;
                         * no unaccounted stream may be opened for it. */
                        b->session_errors++;
                        if (!bind_confirm_delivery(
                                b, cn, MOQR_DELIVERY_STREAM_ERROR, now_us)) {
                            return B_DELIVER_CLOSED;
                        }
                        return B_DELIVER_RETRY;
                    }
                    /* Snapshot BEFORE the call: open_subgroup returns one
                     * WOULD_BLOCK for two distinct causes. A full action
                     * queue (cap == 0) recovers on the capacity gate; a
                     * queue with room means the SUBGROUP POOL refused —
                     * its frees are observable only through the reap each
                     * open's own prologue performs, so that set gets one
                     * bounded re-attempt per pump. */
                    size_t precap =
                        moq_session_action_capacity(cn->session);
                    moq_result_t orc = bind_cookie_is_publication(d.sub_cookie)
                        ? moq_session_open_pub_subgroup(
                              cn->session,
                              (moq_publication_t){ ._opaque = d.sub_cookie },
                              &sc, now_us, &h)
                        : moq_session_open_subgroup(cn->session, ssub, &sc,
                                                    now_us, &h);
                    if (orc == MOQ_ERR_WOULD_BLOCK) {
                        /* The open itself failed, so the downstream object
                         * provably never began. The SESSION-pool refusal
                         * (queue had room) confirms REFUSED_UNBEGUN so a
                         * chunked outstanding delivery RELEASES instead of
                         * starving the SEAL notices whose closes free the
                         * pool; the zero-capacity ACTION_CAP branch keeps
                         * plain WOULD_BLOCK (its capacity-gated park and
                         * recovery are validated as-is, and a whole-object
                         * release there is already the behavior). */
                        if (!bind_confirm_delivery(
                                b, cn,
                                precap == 0
                                    ? MOQR_DELIVERY_WOULD_BLOCK
                                    : MOQR_DELIVERY_REFUSED_UNBEGUN,
                                now_us)) {
                            return B_DELIVER_CLOSED;
                        }
                        return precap == 0 ? B_DELIVER_BLOCKED_ACTION_CAP
                                           : B_DELIVER_BLOCKED_SESSION_SG;
                    }
                    if (orc != MOQ_OK) {
                        /* A deterministic contract violation can never
                         * succeed on retry: fail closed. */
                        b->session_errors++;
                        (void)moq_session_close(cn->session,
                                                BIND_SESSION_INTERNAL_ERROR,
                                                "subgroup open failed",
                                                now_us);
                        conn_detach(b, cn, now_us);
                        return B_DELIVER_CLOSED;
                    }
                    ds->streams_opened = bind_stream_count_next(
                        ds->streams_opened, cn->wire_draft);
                    sgi = freei;
                    cn->sgs[sgi].used = true;
                    cn->sgs[sgi].sub_raw = d.sub_cookie;
                    cn->sgs[sgi].group = d.rec.group_id;
                    cn->sgs[sgi].subgroup = d.rec.subgroup_id;
                    cn->sgs[sgi].h = h;
                    cn->sgs[sgi].obj_begun = false;   /* fresh write progress  */
                    cn->sgs[sgi].obj_id = 0;
                    cn->sgs[sgi].next_chunk = 0;
                    cn->sgs[sgi].props = sc.object_properties;
                }
                if (d.rec.chunk_count > 0) {
                    /* Chunked record: stream the batch. Returns DELIVERED after
                     * end_object (COMPLETE), STALLED at the live edge (OPEN),
                     * WOULD_BLOCK to hold, or STREAM_ERROR. */
                    oc = bind_write_chunked(b, cn, &d, &cn->sgs[sgi], now_us);
                } else if (d.rec.payload == NULL) {
                    oc = oc_from_result(moq_session_write_status_object(
                        cn->session, cn->sgs[sgi].h, d.rec.object_id,
                        (moq_object_status_t)d.rec.status, now_us));
                } else {
                    moq_object_cfg_t ocfg;
                    moq_object_cfg_init(&ocfg);
                    ocfg.object_id = d.rec.object_id;
                    ocfg.payload = (moq_rcbuf_t *)(void *)d.rec.payload;
                    ocfg.properties = (moq_rcbuf_t *)(void *)d.rec.properties;
                    oc = oc_from_result(moq_session_write_object_ex(
                        cn->session, cn->sgs[sgi].h, &ocfg, now_us));
                }
                /* NO inline graceful close here — every graceful subgroup
                 * closure rides the acknowledged SEAL notice, whose close
                 * retries under WOULD_BLOCK. An inline close swallowed on
                 * WOULD_BLOCK after a delivered record would clear the slot
                 * and lose the FIN forever (the later notice would find
                 * nothing to close). The seal itself comes only from the
                 * upstream's actual graceful terminal (SUBGROUP_FINISHED,
                 * emitted after the FIN and every buffered object) — the
                 * header EOG bit is subgroup metadata preserved on the
                 * downstream open, never a per-record terminal. */
            }
        }
        if (!bind_confirm_delivery(b, cn, oc, now_us)) {
            return B_DELIVER_CLOSED;
        }
        if (oc == MOQR_DELIVERY_WOULD_BLOCK) {
            /* Write-path backpressure: ordinary writes document only the
             * action queue, and a property-bearing write needs TWO slots —
             * so the observed remaining capacity is the honest floor (the
             * write retries only once the queue drains PAST it). The held
             * (chunked/abandoned) delivery resumes on the recovery probe. */
            return B_DELIVER_BLOCKED_ACTION_CAP;
        }
        if (oc == MOQR_DELIVERY_STREAM_ERROR) {
            /* Terminal write error retired ONE subscription; sibling subs on
             * this binding may hold deliverable work no future mark
             * re-signals — self-rearm rather than strand them. */
            return B_DELIVER_RETRY;
        }
        if (oc != MOQR_DELIVERY_DELIVERED) {
            /* STALLED (live edge) or ABANDONED (reset done): nothing to observe,
             * and the head is no longer selectable — keep serving OTHER subs on
             * this binding so one publisher-stalled object never HOL-blocks them. */
            continue;
        }
        /* Every successful downstream write records exactly one latency
         * observation, so the histogram count tracks deliveries_written. A
         * regressed clock (now_us < arrival) saturates to 0us rather than
         * skipping — the observation is never dropped. */
        uint64_t latency_us =
            now_us >= d.rec.arrival_us ? now_us - d.rec.arrival_us : 0;
        moqr_latency_observe(&b->forward_latency, latency_us);
        b->deliveries_written++;
    }
    return B_DELIVER_BUDGET;   /* guard expired with work remaining */
}

/* -- request-capacity auto-grant (draft-16 MAX_REQUEST_ID) ---------------------- */

/* A peer-originated request that consumes one request id (draft-16 §9.5:
 * SUBSCRIBE, PUBLISH_NAMESPACE, FETCH, SUBSCRIBE_NAMESPACE, REQUEST_UPDATE,
 * TRACK_STATUS). PUBLISH / SUBSCRIBE_TRACKS are counted too — a superset only
 * over-estimates consumption, which grants EARLY (never late), so the peer can
 * never stall from an undercount. */
static bool
bind_event_is_request(uint32_t kind)
{
    switch (kind) {
    case MOQ_EVENT_SUBSCRIBE_REQUEST:
    case MOQ_EVENT_NAMESPACE_PUBLISHED:
    case MOQ_EVENT_NS_SUB_REQUEST:
    case MOQ_EVENT_TRACK_STATUS_REQUEST:
    case MOQ_EVENT_FETCH_REQUEST:
    case MOQ_EVENT_PUBLISH_REQUEST:
    case MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST:
    case MOQ_EVENT_SUBSCRIBE_UPDATED:
    case MOQ_EVENT_PUBLISH_UPDATED:
    case MOQ_EVENT_NAMESPACE_UPDATE_REQUEST:
    case MOQ_EVENT_NS_SUB_UPDATE_REQUEST:
    case MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_REQUEST:
    case MOQ_EVENT_NAMESPACE_UPDATE_RESULT:
    case MOQ_EVENT_NS_SUB_UPDATE_RESULT:
    case MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_RESULT:
        return true;
    default:
        return false;
    }
}

/* Keep the peer's request-id credit ahead of its consumption. Draft-18 has no
 * MAX_REQUEST_ID (local_request_capacity() == 0), so this is inert there. */
static void
bind_maybe_grant_capacity(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us)
{
    uint64_t ceiling = moq_session_local_request_capacity(cn->session);
    if (ceiling == 0 || b->request_grant_window == 0) {
        return;   /* draft-18, or no local grant advertised */
    }
    /* Peer (client) request ids start at 0 and advance by 2 per request (the
     * relay owns the odd ids), so its position is 2 * requests_seen. Grant when
     * the remaining credit drops below half a window. */
    uint64_t consumed = cn->requests_seen * 2u;
    uint64_t headroom = ceiling > consumed ? ceiling - consumed : 0;
    if (headroom >= (uint64_t)b->request_grant_window / 2u) {
        return;
    }
    /* New ceiling is a full window above the peer's position: absolute and
     * strictly greater than the current ceiling (headroom < window/2 < window).
     * A WOULD_BLOCK does not advance the session's ceiling (grant contract), and
     * this runs every pump, so a backpressured grant is retried until it ships —
     * never lost, and the peer never waits forever. */
    uint64_t new_ceiling = consumed + b->request_grant_window;
    (void)moq_session_grant_request_capacity(cn->session, new_ceiling, now_us);
}

/* -- pump ----------------------------------------------------------------------- */

moqr_result_t
moqr_bind_pump(moqr_bind_t *b, uint64_t now_us)
{
    if (b == NULL) {
        return MOQR_ERR_INVAL;
    }
    if (b->in_pump || b->pump_events == NULL || b->pump_revoked == NULL ||
        b->pump_intents == NULL) {
        return MOQR_ERR_INVAL;
    }
    b->in_pump = true;
    /* Per-pump flag, not sticky: a pass below reports it when the
     * 256-delivery guard expired with work remaining — the ONE outcome
     * whose leftover work no future event re-signals AND whose retry needs
     * nothing but another pump. The runtime turns it into one further
     * coalesced self-wake (see moqr_bind_pump_budget_pending). */
    b->budget_pending = false;
    /* Held rendezvous SUBSCRIBEs past their deadline end before any event
     * of this pump -- a source among them included -- is consumed. */
    (void)moqr_core_internal_rendezvous_expire(b->core, now_us);
    for (uint32_t i = 0; i < b->max_conns; i++) {
        b_conn_t *cn = &b->conns[i];
        if (!cn->used || cn->closed) {
            continue;
        }
        size_t n;
        while (cn->used && !cn->closed) {
            if (!bind_refusal_can_poll(b, cn)) {
                /* Held refusals leave no room to own another request: this
                 * connection's events wait in its session until one commits. */
                FWD_DBG(b, polls_paused);
                break;
            }
            n = moq_session_poll_events(cn->session, b->pump_events,
                                        BIND_PUMP_EVENT_POLL);
            if (n == 0) {
                break;
            }
            for (size_t e = 0; e < n; e++) {
                moq_event_t *ev = &b->pump_events[e];
                if (bind_event_is_request(ev->kind)) {
                    cn->requests_seen++;
                }
                /* A fail-close must stop translation immediately — a DEFERRED
                 * detach (binding close held on intent space) leaves used=true
                 * with closed=true, and no further event of this conn may be
                 * ingested. */
                if (cn->used && !cn->closed) {
                    bind_on_event(b, cn, ev, now_us);
                }
                moq_event_cleanup(ev);
            }
        }
        /* Re-check the grant every pump (not only when events arrive): a peer
         * blocked at its ceiling sends nothing until the grant reaches it. */
        if (cn->used && !cn->closed && cn->session != NULL) {
            bind_maybe_grant_capacity(b, cn, now_us);
            bind_retry_owed_cancels(b, cn, now_us);
        }
    }
    (void)moqr_core_tick(b->core, now_us);
    bind_meta_sweep(b);
    /* Announce grants the tick revoked on revalidation: the core already
     * cleared its routing state and fanned NS_GONE to watchers, but the
     * publisher-side namespace withdrawal is a session write only the binding
     * can make. Peek them and send moq_session_cancel_namespace carrying the
     * denial code (custom DENY code or the UNAUTHORIZED default). Subscribe
     * revocations are fully core-driven (SUB_DONE via intents) and never appear
     * here. Ack (free the grant) once the cancel is on the wire; a WOULD_BLOCK
     * leaves the grant peekable and is retried next pump, after the transport
     * drains the session action queue — the core grant is the durable record,
     * so a backpressured cancel is never lost. A revoked grant on a gone/closed
     * conn is acked with no wire cancel (never act on a torn-down session).
     * Peek is non-draining, so this is a single bounded pass, not a while-loop. */
    size_t rn = moqr_core_peek_revoked_grants(b->core, b->pump_revoked,
                                              BIND_PUMP_REVOKED_BATCH);
    for (size_t k = 0; k < rn; k++) {
        moqr_revoked_grant_t *rev = &b->pump_revoked[k];
        if (rev->binding_cookie == 0 ||
            rev->binding_cookie - 1u >= b->max_conns) {
            moqr_core_ack_revoked_grant(b->core, rev->binding_cookie,
                                        rev->session_cookie);
            continue;
        }
        b_conn_t *rcn = &b->conns[rev->binding_cookie - 1u];
        if (!rcn->used || rcn->closed || rcn->session == NULL) {
            moqr_core_ack_revoked_grant(b->core, rev->binding_cookie,
                                        rev->session_cookie);
            continue;
        }
        moq_cancel_namespace_cfg_t ccfg;
        moq_cancel_namespace_cfg_init(&ccfg);
        ccfg.error_code = rev->error_code;
        moq_result_t crc = moq_session_cancel_namespace(
            rcn->session,
            (moq_announcement_t){ ._opaque = rev->session_cookie }, &ccfg,
            now_us);
        if (crc == MOQ_OK) {
            int ann_slot = ann_find(b, rcn, rev->session_cookie);
            if (ann_slot >= 0) {
                ann_clear_slot(b, rcn, (uint32_t)ann_slot);
            }
            moqr_core_ack_revoked_grant(b->core, rev->binding_cookie,
                                        rev->session_cookie);
        } else if (crc != MOQ_ERR_WOULD_BLOCK) {
            /* A hard error (e.g. wrong state) can never succeed on retry: drop
             * it rather than spin forever, retire the binding mirror whose
             * core route is already gone, and record the anomaly. */
            int ann_slot = ann_find(b, rcn, rev->session_cookie);
            if (ann_slot >= 0) {
                ann_clear_slot(b, rcn, (uint32_t)ann_slot);
            }
            b->session_errors++;
            moqr_core_ack_revoked_grant(b->core, rev->binding_cookie,
                                        rev->session_cookie);
        }
        /* WOULD_BLOCK: leave the grant peekable; retried on the next pump. */
    }
    (void)bind_execute_intents(b, now_us);
    /* Retry closes that blocked on intent space (the ring is drained). */
    bool detach_retried = false;
    for (uint32_t i = 0; i < b->max_conns; i++) {
        b_conn_t *cn = &b->conns[i];
        if (cn->used && cn->detach_pending) {
            cn->closed = false;
            cn->detach_pending = false;
            conn_detach(b, cn, now_us);
            detach_retried = true;
        }
    }
    if (detach_retried) {
        (void)bind_execute_intents(b, now_us);
    }
    /* Forwarded Joining FETCH transactions: upstream requests/cancels and
     * downstream responses, each step bounded and retry-safe. */
    bind_fwd_pump(b, now_us);
    bind_refusal_pump(b, now_us);
    bind_update_answer_pump(b, now_us);
    bind_nsu_pump(b, now_us);
    for (uint32_t i = 0; i < b->max_conns; i++) {
        b_conn_t *cn = &b->conns[i];
        if (cn->used && !cn->closed && cn->wire_draft == B_DRAFT_22) {
            bind_discovery_pump(b, cn, now_us);
        }
    }
    /* Hold delivery while an output intent is still parked: an object must
     * never overtake the ACCEPT/REJECT/DONE queued ahead of it. The backlog
     * drains first (next pump, after the transport frees queue room), then
     * objects flow. */
    if (b->pending_count == 0) {
        /* One complete bounded drain of the core-ready set into the bind
         * set (pseudo cookies are filtered at production; a real cookie is
         * slot + 1). Looping until a short batch means a per-call cap can
         * never defer a ready binding to a later pump. */
        uint64_t rdy[64];
        uint32_t got;
        do {
            got = moqr_core_drain_ready(b->core, rdy, 64);
            for (uint32_t k = 0; k < got; k++) {
                if (rdy[k] != 0 && rdy[k] - 1u < b->max_conns) {
                    bind_dl_set(b->dl_ready, (uint32_t)(rdy[k] - 1u));
                }
            }
        } while (got == 64);
        for (uint32_t i = 0; i < b->max_conns; i++) {
            b_conn_t *cn = &b->conns[i];
            if (cn->used && !cn->closed) {
                bool ready = bind_dl_test(b->dl_ready, i);
                bool parked = bind_dl_test(b->dl_parked, i);
                bool attempt;
                if (parked && cn->park_reason == B_PARK_ACTION_CAP) {
                    /* One O(1) capacity read per pump, zero probes below
                     * the floor. A ready mark that lands meanwhile stays
                     * SET (never cleared unattempted), so the post-recovery
                     * pass sees it — no lost wakeup, no early probe. */
                    attempt = moq_session_action_capacity(cn->session) >
                              cn->park_cap;
                } else if (parked) {
                    /* SESSION_SG: exactly one bounded re-attempt per pump —
                     * each attempt's own open prologue performs the reap. */
                    attempt = true;
#ifdef MOQR_BIND_TESTING
                    atomic_fetch_add(&bind_dbg_sg_attempts, 1);
#endif
                } else {
                    attempt = ready;
                }
                if (attempt) {
                    /* Clear BEFORE the pass: a mark landing during it (a
                     * delivery-triggered state change) re-sets the bit and
                     * the next pump serves it — same thread, in order. */
                    bind_dl_clear(b->dl_ready, i);
                    switch (bind_deliver(b, cn, now_us)) {
                    case B_DELIVER_DRAINED:
                        bind_dl_clear(b->dl_parked, i);
                        cn->park_reason = B_PARK_NONE;
                        cn->park_cap = 0;
                        cn->bind_sg_waiting = false;   /* no longer pool-blocked */
                        break;
                    case B_DELIVER_BLOCKED_ACTION_CAP:
                        /* Park on the OBSERVED capacity floor: a property-
                         * bearing write needs TWO slots, so it can refuse
                         * with capacity 1 — the retry gate is capacity
                         * EXCEEDING what the refused call saw, not merely
                         * nonzero. (The subgroup-open case observed zero.) */
                        bind_dl_set(b->dl_parked, i);
                        cn->park_reason = B_PARK_ACTION_CAP;
                        cn->park_cap =
                            moq_session_action_capacity(cn->session);
                        cn->bind_sg_waiting = false;   /* blocked elsewhere */
#ifdef MOQR_BIND_TESTING
                        bind_dbg_blocked_inc(&cn->dbg_blocked[0]);
#endif
                        break;
                    case B_DELIVER_BLOCKED_SESSION_SG:
                        bind_dl_set(b->dl_parked, i);
                        cn->park_reason = B_PARK_SESSION_SG;
                        cn->park_cap = 0;
                        cn->bind_sg_waiting = false;   /* blocked elsewhere */
#ifdef MOQR_BIND_TESTING
                        bind_dbg_blocked_inc(&cn->dbg_blocked[1]);
#endif
                        break;
                    case B_DELIVER_BLOCKED_BIND_SG:
                        /* Never parked and never polled: latch that a delivery
                         * is waiting on this conn's subgroup-slot pool, so ONLY
                         * a later slot release for THIS conn re-arms the ready
                         * bit (bind_dl_rearm_on_slot_release). Ordinary closes
                         * with nothing waiting re-arm nothing. */
                        bind_dl_clear(b->dl_parked, i);
                        cn->park_reason = B_PARK_NONE;
                        cn->park_cap = 0;
                        cn->bind_sg_waiting = true;
#ifdef MOQR_BIND_TESTING
                        bind_dbg_blocked_inc(&cn->dbg_blocked[2]);
#endif
                        break;
                    case B_DELIVER_BUDGET:
                        /* The guard expired mid-batch: the batch may be the
                         * only traffic there is, so surface it for the
                         * runtime's one further self-wake. RETRY below is
                         * deliberately NOT surfaced — its leftovers arise
                         * mid-anomaly during active traffic and ride the
                         * next pump any event earns (the ambient cadence). */
                        b->budget_pending = true;
                        /* fallthrough */
                    case B_DELIVER_RETRY:
                        /* Work remains that no future mark re-signals:
                         * self-rearm; parked state unchanged. Every other
                         * ready binding still got its own budget this
                         * pump, so the starvation bound is exactly the
                         * old one (guard x ready bindings). Not a pool block:
                         * the self-rearm above already schedules the retry. */
                        cn->bind_sg_waiting = false;
                        bind_dl_set(b->dl_ready, i);
                        break;
                    case B_DELIVER_CLOSED:
                        /* Every CLOSED path detached the connection, which
                         * cleared its scheduling state; the guarded clear
                         * below is pure defense against a future
                         * non-detaching CLOSED path. */
                        if (cn->used && !cn->closed) {
                            bind_dl_clear_conn(b, cn);
                        }
                        break;
                    }
                }
            }
            /* The fetch half is untouched: per open connection, every
             * pump, exactly as before. */
            if (cn->used && !cn->closed) {
                bind_fetch_pump(b, cn, now_us);
            }
        }
        /* A delivery pass can end subscriptions: a sub whose source finished
         * or whose track ended retires once nothing is left to send, queueing
         * its DONE behind the data it closes. Execute those terminals in this
         * pump; nothing else is bound to bring a later one, so they would
         * otherwise wait for an unrelated event. */
        (void)bind_execute_intents(b, now_us);
    }
    b->in_pump = false;
    return MOQR_OK;
}

moqr_result_t
moqr_bind_set_fill_source(moqr_bind_t *b,
                          const moqr_bind_fill_source_t *source)
{
    if (b == NULL) {
        return MOQR_ERR_INVAL;
    }
    const size_t need = offsetof(moqr_bind_fill_source_t, close) +
                        sizeof(source->close);
    if (source != NULL &&
        (source->struct_size < need || source->open == NULL ||
         source->peek == NULL || source->commit == NULL ||
         source->close == NULL)) {
        return MOQR_ERR_INVAL;
    }
    if (b->in_pump) {
        return MOQR_ERR_WRONG_STATE;
    }
    for (uint32_t i = 0; i < b->max_conns; i++) {
        const b_conn_t *cn = &b->conns[i];
        for (uint32_t fi = 0; cn->used && fi < b->n_fetches; fi++) {
            if (cn->fetches[fi].used && cn->fetches[fi].fill_src_open) {
                return MOQR_ERR_WRONG_STATE;
            }
        }
    }
    memset(&b->fill_src, 0, sizeof(b->fill_src));
    b->retrieve_source_peek = NULL;
    b->has_fill_src = source != NULL;
    if (source != NULL) {
        size_t n = source->struct_size < sizeof(b->fill_src)
                       ? source->struct_size
                       : sizeof(b->fill_src);
        memcpy(&b->fill_src, source, n);
        b->fill_src.struct_size = (uint32_t)sizeof(b->fill_src);
    }
    return MOQR_OK;
}

moqr_result_t
moqr_bind_retrieve_source_set(moqr_bind_t *b, moqr_retrieve_source_peek_fn peek)
{
    if (b == NULL || (peek != NULL && !b->has_fill_src)) return MOQR_ERR_INVAL;
    if (b->in_pump) return MOQR_ERR_WRONG_STATE;
    for (uint32_t i = 0; i < b->max_conns; i++) {
        const b_conn_t *cn = &b->conns[i];
        for (uint32_t j = 0; cn->used && j < b->n_fetches; j++) {
            if (cn->fetches[j].used && cn->fetches[j].fill) {
                return MOQR_ERR_WRONG_STATE;
            }
        }
    }
    b->retrieve_source_peek = peek;
    return MOQR_OK;
}

moqr_result_t
moqr_bind_retrieve_standalone_set(moqr_bind_t *b,
                                  const moqr_retrieve_standalone_ops_t *ops)
{
    if (b == NULL || (ops != NULL &&
                      (ops->open == NULL || ops->plan == NULL ||
                       ops->peek == NULL || ops->commit == NULL ||
                       ops->close == NULL))) {
        return MOQR_ERR_INVAL;
    }
    if (b->in_pump) return MOQR_ERR_WRONG_STATE;
    for (uint32_t i = 0; i < b->max_conns; i++) {
        const b_conn_t *cn = &b->conns[i];
        for (uint32_t j = 0; cn->used && j < b->n_fetches; j++) {
            if (cn->fetches[j].used && cn->fetches[j].sa_remote) {
                return MOQR_ERR_WRONG_STATE;
            }
        }
    }
    if (ops == NULL) {
        memset(&b->sa_src, 0, sizeof(b->sa_src));
        b->has_sa_src = false;
    } else {
        b->sa_src = *ops;
        b->has_sa_src = true;
    }
    return MOQR_OK;
}

moqr_result_t
moqr_bind_set_fill_timeouts(moqr_bind_t *b, uint64_t default_ms,
                            uint64_t max_ms)
{
    if (b == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint64_t def_us = 0;
    uint64_t max_us = 0;
    if (bind_fill_wait_resolve(default_ms, max_ms, &def_us, &max_us) !=
        MOQR_OK) {
        return MOQR_ERR_INVAL;
    }
    b->fill_wait_default_us = def_us;
    b->fill_wait_max_us = max_us;
    return MOQR_OK;
}

uint64_t
moqr_bind_next_deadline_us(const moqr_bind_t *b)
{
    uint64_t next = UINT64_MAX;
    if (b == NULL) {
        return next;
    }
    /* Includes owner-shard collectors even while their FIFO has no credit.
     * Cancellation is independent of whether the consumer is being peeked. */
    for (uint32_t i = 0; i < b->n_fwd; i++) {
        const b_fwd_t *t = &b->fwd[i];
        if (t->used && t->consumer == FW_CONSUMER_FILL &&
            t->end == FW_END_NONE && t->retrieval.deadline_us < next) {
            next = t->retrieval.deadline_us;
        }
    }
    for (uint32_t i = 0; i < b->max_conns; i++) {
        const b_conn_t *cn = &b->conns[i];
        if (!cn->used || cn->closed) {
            continue;
        }
        for (uint32_t fi = 0; fi < b->n_fetches; fi++) {
            const b_fetch_t *bf = &cn->fetches[fi];
            /* A fill reading through a fill source that reports its own
             * deadlines leaves its deadline to the source, which knows
             * whether the part it serves still waits on time. */
            bool by_source = bf->fill_src_open &&
                             b->fill_src.next_deadline_us != NULL;
            uint64_t fill_deadline = bf->retrieval.live
                ? moqr_bind_retrieve_next_deadline(&bf->retrieval)
                : bf->fill_deadline_us;
            if (bf->used && bf->fill && bf->state == BF_STREAMING &&
                !by_source && fill_deadline < next) {
                next = fill_deadline;
            }
            /* A standalone FETCH waiting for an origin to retrieve from
             * stops waiting at its deadline (an admitted upstream FETCH's
             * timer is its collector's, above). */
            if (bf->used && bf->standalone && bf->state == BF_STREAMING &&
                bf->retrieval.live) {
                uint64_t d = moqr_bind_retrieve_next_deadline(&bf->retrieval);
                if (d < next) next = d;
            }
            /* A FETCH waiting for an Object still arriving gives up on it
             * at its deadline. */
            if (bf->used && !bf->fill && bf->state == BF_STREAMING &&
                bf->fetch_held_pending && bf->fill_deadline_us < next) {
                next = bf->fill_deadline_us;
            }
        }
    }
    if (b->has_fill_src && b->fill_src.next_deadline_us != NULL) {
        uint64_t sd = b->fill_src.next_deadline_us(b->fill_src.ctx);
        if (sd < next) {
            next = sd;
        }
    }
    /* A SUBSCRIBE held waiting for a publisher ends at its deadline. */
    uint64_t cd = moqr_core_next_deadline_us(b->core);
    if (cd < next) {
        next = cd;
    }
    return next;
}

bool
moqr_bind_pump_budget_pending(const moqr_bind_t *b)
{
    return b != NULL && b->budget_pending;
}

bool
moqr_bind_pump_continuation_pending(const moqr_bind_t *b)
{
    /* Work left by the last pump that a self-continuation must carry forward,
     * REASON-AWARE (a self-wake only helps if a future pump would make progress
     * without any external edge):
     *   - any connection still READY (a mark that landed during a pass, which
     *     nothing else re-signals on this thread); or
     *   - a connection parked on SESSION_SG, whose bounded one-per-pump
     *     re-attempt (its open prologue performs the reap) needs a pump to
     *     happen and has no external edge to earn one.
     * A connection parked on ACTION_CAP is DELIBERATELY excluded: its retry
     * gate waits for downstream transport capacity to rise above the observed
     * floor — an external edge the managed adapter surfaces by arming the lane
     * pump from a transport-event batch (write completions etc.), a cause
     * distinct from a shard push/credit wake. An arbitrary self-continuation
     * would only spin while that capacity is unchanged. A fully-drained pass
     * leaves nothing here, so an applied batch that delivered everything earns
     * no self-wake. */
    if (b == NULL || b->dl_ready == NULL) {
        return false;
    }
    /* Scan only LIVE connections: a core-ready cookie for a slot with no live
     * bind connection (e.g. a direct-to-core binding the bind never opened a
     * connection for) aliases an unused dl_ready bit that the pump's delivery
     * loop never services and never clears. A raw word scan would treat that
     * sticky bit as residual and manufacture a self-continuation on every
     * inbound pop. Liveness is the guard. */
    for (uint32_t i = 0; i < b->max_conns; i++) {
        const b_conn_t *cn = &b->conns[i];
        if (!cn->used || cn->closed) {
            continue;
        }
        if (bind_dl_test(b->dl_ready, i)) {
            return true;   /* a live connection has residual ready work */
        }
        if (b->dl_parked != NULL && bind_dl_test(b->dl_parked, i) &&
            cn->park_reason == B_PARK_SESSION_SG) {
            return true;   /* SESSION_SG re-attempt needs a pump */
        }
    }
    return false;
}

moqr_result_t
moqr_bind_auth_resolve(moqr_bind_t *b, uint64_t ticket,
                       const moqr_auth_verdict_t *result, uint64_t now_us)
{
    if (b == NULL) {
        return MOQR_ERR_INVAL;
    }
    moqr_auth_verdict_t verdict;
    moqr_park_req_t     view;
    moqr_result_t rc = moqr_core_auth_begin_resolve_sized(
        b->core, ticket, result, now_us, &verdict, &view, sizeof(view));
    if (rc != MOQR_OK) {
        return rc; /* unknown / retired / stale -> no-op, nothing pinned */
    }
    /* Single cleanup path: every exit below finishes the resolve. Revalidate
     * the owning connection before touching any session — a closed/gone
     * binding means no-op (never act on a stale session). */
    uint64_t  slot = view.binding_cookie - 1u; /* cookie == slot + 1 */
    b_conn_t *cn = (view.binding_cookie != 0 && slot < b->max_conns)
                       ? &b->conns[slot]
                       : NULL;
    if (cn != NULL && cn->used && !cn->closed && cn->session != NULL) {
        /* The entry reserved at park time now backs whatever refusal the
         * resolution owes, so a blocked rejection is held, not lost. */
        bool reserved = view.action != MOQR_AUTH_PUBLISH &&
                        view.sub_filter_type != BIND_PARK_DISCOVERY &&
                        view.sub_filter_type != BIND_PARK_NS_UPDATE;
        if (reserved && cn->refusals_reserved != 0) {
            cn->refusals_reserved--;
        }
        b_meta_publish_t *p = view.action == MOQR_AUTH_PUBLISH
                                 ? bind_meta_pub_find(b, cn, view.session_cookie)
                                 : NULL;
        bool current = view.action != MOQR_AUTH_PUBLISH ||
                       (p != NULL && p->state == B_META_PUB_AUTH && p->ticket == ticket);
        /* A cancelled PUBLISH has no bind-owned snapshot to resume. */
        if (current && view.action == MOQR_AUTH_SUBSCRIBE &&
            view.sub_filter_type == BIND_PARK_DISCOVERY) {
            bind_discovery_resolved(b, cn, &view, &verdict, now_us);
        } else if (current && view.sub_filter_type == BIND_PARK_NS_UPDATE &&
                   bind_nsu_family(view.session_cookie) != 0) {
            bind_nsu_resolved(b, cn, &view, &verdict, now_us);
        } else if (current) {
            if (verdict.decision == MOQR_AUTH_ALLOW) {
                bind_resume(b, cn, &view, verdict.revalidate_after_us, now_us);
            } else {
                uint64_t err = verdict.error_code != 0
                                   ? verdict.error_code
                                   : MOQ_REQUEST_ERROR_UNAUTHORIZED;
                bind_reject(b, cn, view.action, view.session_cookie, err, now_us);
            }
        }
    }
    moqr_core_auth_finish_resolve(b->core, ticket);
    return MOQR_OK;
}

void
moqr_bind_get_stats(const moqr_bind_t *b, moqr_bind_stats_t *out)
{
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (b == NULL) {
        return;
    }
    for (uint32_t i = 0; i < b->max_conns; i++) {
        if (b->conns[i].used) {
            out->conns++;
        }
    }
    out->events_translated = b->events_translated;
    out->deliveries_written = b->deliveries_written;
    out->ingest_refusals = b->ingest_refusals;
    out->session_errors = b->session_errors;
    out->forward_latency = b->forward_latency;
    out->pending_high_water = b->pending_high_water;
    out->pending_nonscalar_blocked = b->pending_nonscalar_blocked;
    out->nsu_high_water = b->nsu_high_water;
    out->nsu_live = b->nsu_live;
    out->nsu_retained = b->nsu_retained;
    out->nsu_failed_closed = b->nsu_failed_closed;
    out->ordered_failed_closed = b->ordered_failed_closed;
}
