#ifndef MOQR_BIND_INTERNAL_H
#define MOQR_BIND_INTERNAL_H

#include <moq/relay/moqr_bind.h>
#ifdef MOQR_BIND_TESTING
#include <moq/relay/moqr_bind_test.h>
#endif

#include <moq/relay/capacity.h>

#include "moqr_bind_auth.h"
#include "moqr_bind_metadata.h"
#include "moqr_bind_retrieval.h"
#include "../core/retrieve_internal.h"
#include "../core/props_codec_internal.h"
#include "../../../core/src/spi/core_spi.h"

#include <moq/rcbuf.h>

#include <stddef.h>
#include <stdint.h>
#include <string.h>

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
    /* The track this subscription's SUBSCRIBE_OK answered for: its
     * generation-checked handle, renewed when the retained identity is
     * replaced. A Joining FETCH on the subscription is served only for that
     * retained identity. */
    bool         has_ident;
    moqr_track_t ident_track;
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
    /* Suppression belongs to this exact owner/key, not the reusable session
     * pool slot. Source seals do not retire numeric keys. */
    bool                  stopped;
    uint64_t              sub_raw;
    moqr_sub_t            rsub;
    uint64_t              group, subgroup;
    uint32_t              core_list;
    moq_subgroup_handle_t h;
    moq_stream_ref_t       stopped_ref;
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
    /* The stream was opened for the record of object open_obj and nothing
     * has been written on it since: that record, offered again after the
     * session refused its first write, starts this stream rather than
     * finding one it does not continue. */
    bool                  unwritten;
    uint64_t              open_obj;
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
    uint8_t                properties_codec; /* the upstream connection's */
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
    uint8_t        props_codec;    /* MOQR_PROPS_CODEC_* of the upstream's draft */
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
    bool         discovered;
    moq_ns_sub_handle_t discovery;
    moq_cluster_metadata_t cluster; /* latest received state, in conn path pool */
    bool cluster_pending;
    bool cluster_initial;
    moqr_grant_res_t cluster_grant;
} b_ann_t;


/* What one peer was actually told, independent of current best-route state. */
typedef struct b_cadv {
    b_ann_t ann;
    moqr_cluster_advertisement_id_t source_advertisement;
    uint64_t origin;
} b_cadv_t;


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
    bool             cluster_applied;
    uint64_t         error_code;  /* reject only                          */
    moqr_grant_res_t gres;        /* an announcement's new grant, if any  */
} b_nsans_t;


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


#define B_RETRY_REFUSAL (1u << 0)

#define B_RETRY_CANCEL  (1u << 1)


#define B_DRAFT_16 0u

#define B_DRAFT_22 2u


typedef struct b_conn {
    bool           used;
    bool           closed;
    bool           detach_pending;   /* close blocked on intent space   */
    bool           events_pending;   /* refusal pressure interrupted drain */
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
    /* B_RETRY_*: a held refusal or owed upstream cancellation. Each bit is
     * cleared only by the walk that finds none left. Sharing one byte keeps
     * both scan hints in the padding before park_cap. */
    uint8_t        retry_pending;
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
    b_cadv_t      *cluster_adverts;
    uint64_t      *cluster_paths;
    bool           cluster_peer_ready;
    bool           cluster_negotiated;
    bool           cluster_cost_override;
    bool           cluster_discovery_closed;
    bool           cluster_discovery_retiring;
    bool           cluster_discovery_cancel_owed;
    uint64_t       cluster_cost;
    uint64_t       cluster_correlation;
    uint64_t       cluster_peer_hop;
    moq_ns_sub_handle_t cluster_discovery;
    b_fetch_t     *fetches;
    b_refusal_t   *refusals;   /* held refusals, n_refusals entries      */
    uint32_t       refusals_reserved;   /* entries promised to parked auths */
    /* One past the highest fetch slot marked used since the table was last
     * cleared: every slot at or above it is unused, so the per-pump fetch
     * walks stop there instead of at n_fetches. Raised when a slot is
     * marked used, lowered only by bind_fetch_pump's trailing trim. Rides
     * the padding after refusals_reserved — no size cost. */
    uint32_t       fetch_hw;
    uint64_t       requests_seen;    /* inbound peer requests observed (d16
                                      * request-id consumption proxy)     */
} b_conn_t;


struct moqr_bind {
    moq_alloc_t  alloc;
    b_dgrant_t   dgrants[BIND_DGRANTS];
    moqr_core_t *core;
    moqr_cluster_info_t cluster_info;
    uint64_t *cluster_hops; /* max received path plus this relay's append */
    uint64_t cluster_next_discovery;
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

#ifdef MOQR_BIND_TESTING


#include <stdatomic.h>


#endif


/* One announce's copied namespace bytes can never exceed the core's shared
 * full-track-name cap; a bind slot exists only for a core-ACCEPTED announce,
 * so the live-slot count is bounded by the core ns-node pool as well as the
 * bind tables. */
#define BIND_ANN_BYTES_MAX 4096u



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



#define BIND_SESSION_INTERNAL_ERROR 0x1u   /* MoQ session INTERNAL_ERROR      */

#define BIND_REQERR_INTERNAL   0x0u


#ifdef MOQR_BIND_TESTING


#define FWD_DBG(b, field) ((b)->dbg_fwd.field++)


#else
#define FWD_DBG(b, field) ((void)0)


#endif


/* A converted Track Property set, owned until bind_track_props_release. */
typedef struct b_props_tmp {
    uint8_t *buf;
    size_t   len;
} b_props_tmp_t;


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


/* The outcome of authorizing one control request. */
typedef enum {
    BIND_AUTH_ALLOW = 0,   /* proceed with the op                          */
    BIND_AUTH_DENY,        /* reject with *err                             */
    BIND_AUTH_DEFER,       /* park under *ticket; resolve later            */
    BIND_AUTH_FAIL_CLOSED, /* token mirror failed / DEFER without a ticket */
} bind_auth_result_t;


typedef enum b_upd_give {
    B_UPD_GIVEN = 0,    /* the answer is on its way (or nothing was owed)  */
    B_UPD_HELD,         /* WOULD_BLOCK: still owed, retry later            */
    B_UPD_ENDED,        /* a refusal ended the subscription                */
} b_upd_give_t;


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


/* The longest the relay holds a SUBSCRIBE waiting for a publisher
 * (§9.20.6 RENDEZVOUS_TIMEOUT), whatever the request asks: a private
 * resource ceiling, not a protocol value. */
#define BIND_RENDEZVOUS_MAX_MS UINT64_C(10000)



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


typedef struct b_delivery_capacity {
    const moqr_bind_t *bind;
    const b_conn_t *conn;
} b_delivery_capacity_t;

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

moqr_reset_desc_t
bind_upstream_reset(const moqr_bind_t *b, const b_conn_t *cn, uint64_t wire);

bool
bind_reset_wire(const moqr_bind_t *b, const b_conn_t *cn,
                moqr_reset_desc_t desc, uint64_t *out);

moqr_pd_desc_t
bind_upstream_pd(const moqr_bind_t *b, const b_conn_t *cn, uint64_t wire);

moqr_pd_desc_t
bind_local_pd(moqr_pd_status_t status);

void
conn_detach(moqr_bind_t *b, b_conn_t *cn, uint64_t now_us);

b_conn_t *
conn_by_session(moqr_bind_t *b, const moq_session_t *s);

b_conn_t *
conn_by_cookie(moqr_bind_t *b, uint64_t cookie);

uint8_t
bind_props_codec(const b_conn_t *cn);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#include "intents_internal.h"

#endif /* MOQR_BIND_INTERNAL_H */
