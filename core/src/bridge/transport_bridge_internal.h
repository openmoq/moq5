#ifndef MOQ_TRANSPORT_BRIDGE_INTERNAL_H
#define MOQ_TRANSPORT_BRIDGE_INTERNAL_H

#include <moq/transport_bridge.h>
#include <moq/session.h>
#include "../session/session_transport.h"   /* moq_uni_class_t + capability */

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

/* -- Defaults ------------------------------------------------------- */

#define BRIDGE_DEFAULT_MAX_STREAMS    128
#define BRIDGE_DEFAULT_MAX_PENDING     64
/* Actions one service pass will drain. Replaces the bound that the old
 * "stop draining once anything is pending" rule provided implicitly, so a
 * pass is still hard-bounded now that per-stream ordering is enforced at
 * dispatch instead. */
#define BRIDGE_DRAIN_BUDGET           256
#define BRIDGE_DEFAULT_MAX_TOMBSTONES  64

#define BRIDGE_INBOUND_REF_BASE (1ULL << 63)

/* Local hold-age policy (MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT): a peer stream
 * refused at admission is cancelled once it has waited this long since its
 * first refusal. Not a wire timeout. */
#define BRIDGE_HOLD_AGE_US 5000000ULL

/* -- Stream entry --------------------------------------------------- */

typedef enum {
    BRIDGE_STREAM_UNI  = 0,
    BRIDGE_STREAM_BIDI = 1,
} bridge_stream_kind_t;

/*
 * Control-channel topology. Default is a single shared bidirectional channel
 * (draft-16). Uni-control-pair mode is used by profiles that open a local
 * unidirectional control channel and receive the peer's. Selected at bridge
 * creation from the session's profile capability.
 */
typedef enum {
    BRIDGE_CONTROL_BIDI     = 0,
    BRIDGE_CONTROL_UNI_PAIR = 1,
} bridge_control_mode_t;

/*
 * Routing decision for an inbound peer unidirectional stream once classified.
 */
typedef enum {
    BRIDGE_UNI_ROUTE_DATA      = 0,  /* hand to the existing data-stream path */
    BRIDGE_UNI_ROUTE_HANDLED   = 1,  /* consumed: control channel recorded, or padding discarded */
    BRIDGE_UNI_ROUTE_NEED_MORE = 2,  /* insufficient leading bytes to classify */
    BRIDGE_UNI_ROUTE_FATAL     = 3,  /* unknown type, or a second peer control channel */
} bridge_uni_route_t;

typedef enum {
    BRIDGE_ORIGIN_LOCAL = 0,
    BRIDGE_ORIGIN_PEER  = 1,
} bridge_stream_origin_t;

/*
 * Disposition of an inbound peer unidirectional stream in uni-control-pair
 * mode, decided from its leading stream type and then fixed for the stream's
 * lifetime. PENDING means not enough leading bytes have arrived to classify.
 */
typedef enum {
    BRIDGE_UNI_DISP_PENDING = 0,  /* not yet classified */
    BRIDGE_UNI_DISP_CONTROL = 1,  /* uni-control-pair control stream. A peer-origin
                                   * control stream feeds the control path; a
                                   * local-origin one we opened carries our control
                                   * output. Either way a peer RESET/STOP_SENDING of
                                   * it is a fatal control-channel teardown, never an
                                   * unknown-data-stream no-op. */
    BRIDGE_UNI_DISP_DATA    = 2,  /* object data: feed to data path */
    BRIDGE_UNI_DISP_DISCARD = 3,  /* drop remaining bytes until FIN: a padding
                                   * stream, or a data stream the session has
                                   * dropped/refused (replaying would misparse) */
} bridge_uni_disp_t;

typedef struct {
    moq_stream_ref_t       ref;
    uint64_t               transport_id;
    bridge_stream_kind_t   kind;
    bridge_stream_origin_t origin;
    bool                   active;
    bool                   peer_send_closed;
    bool                   local_send_closed;
    /* A peer STOP_SENDING has been answered for this stream's local send half.
     * Tracked separately from local_send_closed because a FIN may already have
     * closed that half before the STOP arrived: the RESET is owed once, and
     * only once, per stream. */
    bool                   peer_stop_received;
    /* Inbound pending state (bridge-owned) */
    bool                   pending_retry;
    bool                   pending_fin;
    bool                   fin_retained;
    bool                   pending_reset;
    uint64_t               pending_reset_code;
    bool                   pending_stop;
    uint64_t               pending_stop_code;
    /* A new peer uni data stream refused at admission (no receive entry).
     * held_input: the adapter still owes the refused chunk -- the session
     * never saw this stream; ended only by the chunk's acceptance, a peer
     * RESET, or bridge teardown. held_payload: that chunk carried bytes, so
     * an empty (FIN-only) delivery cannot stand in for it. pending_admission:
     * readiness -- set while the session cannot admit, cleared by a service
     * pass once it can (the adapter's cue to redeliver); it says nothing
     * about the obligation. Nothing is retained here and service() never
     * empty-feeds such a stream (unlike pending_retry). */
    bool                   held_input;
    bool                   held_payload;
    bool                   pending_admission;
    /* Whole-stream ABORT discard lifecycle: the entry is kept (its ref
     * mapping preserved so late peer bytes are DISCARDED, never given a
     * fresh ref), and retired on the peer's FIN / RESET / stream
     * terminal. Set by dispatch_abort_bidi. */
    bool                   aborting;
    /* A local unidirectional data stream whose FIN has been written, kept
     * mapped until the adapter reports it committed or the session resets it
     * (MOQ_TRANSPORT_CAP_COMMIT_REPORT). */
    bool                   awaiting_commit;
    /* First accepted stream header, classified by the session profile. Zero
     * for non-subgroup streams. Survives zero-timeout core-slot retirement. */
    uint8_t                subgroup_header_len;
    /* A retained subgroup FIN is being reset; keep its mapping until the
     * endpoint accepts the reset, and ignore later commit/STOP callbacks. */
    bool                   fin_reset_pending;
    /* Local unidirectional data stream: bytes the endpoint has accepted
     * (header and payload writes). Bounds a reset's reliable size. */
    uint64_t               bytes_sent;
    /* Inbound peer-uni classification (uni-control-pair mode only). */
    uint8_t                uni_disp;        /* bridge_uni_disp_t */
    uint8_t                classify_len;    /* retained leading bytes (< 9) */
    uint8_t                classify_buf[9];
} bridge_stream_entry_t;

/* -- Held peer streams (MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT) ------------ *
 *
 * A peer stream the session has not admitted, whose exact refused chunk the
 * adapter keeps. One record per such stream, in a fixed table sized from
 * max_pending and separate from the outbound pending queue. A full table
 * refuses a new arrival with MOQ_BRIDGE_INGRESS_REFUSE_CANCEL. A record
 * without a stream mapping (mapped false) holds a request stream, or a new
 * peer uni stream whose first chunk is a complete data type; the adapter's
 * redelivery of that exact chunk carries the stream type, so no prefix is
 * kept here.
 *
 *   RECORDED    waiting for admission. Offered to the adapter oldest first
 *               (offered: its pending state is clear, so it redelivers the
 *               held chunk); released by the acceptance of that chunk; turns
 *               CANCELLING when it reaches BRIDGE_HOLD_AGE_US or, for a
 *               request bidi, when the peer resets its sending half.
 *   CANCELLING  this side cancels the stream with the profile's code
 *               (EXCESSIVE_LOAD on drafts 18 and 22, CANCELLED on draft
 *               16) in this record's own slot: STOP_SENDING for the peer's half
 *               (stop_owed), RESET_STREAM for ours on a request bidi
 *               (reset_owed). Each half is settled by an accepted op or, for
 *               the receive half, an observed peer terminal. The record is
 *               freed once both are settled; until then the stream reports
 *               pending and nothing on it is fed to the session. */
typedef enum {
    BRIDGE_HOLD_FREE       = 0,
    BRIDGE_HOLD_RECORDED   = 1,
    BRIDGE_HOLD_CANCELLING = 2,
} bridge_hold_state_t;

typedef struct {
    uint8_t  state;          /* bridge_hold_state_t */
    uint8_t  kind;           /* bridge_stream_kind_t */
    bool     mapped;         /* a stream entry exists (uni admission hold) */
    bool     offered;
    bool     held_payload;   /* the refused chunk carried bytes, so a
                                FIN-only delivery cannot stand in for it */
    bool     stop_owed;
    bool     reset_owed;
    uint64_t transport_id;
    uint64_t first_refusal_us;
    uint64_t seq;            /* admission order, set once */
} bridge_hold_t;

/* -- Pending item --------------------------------------------------- */

typedef enum {
    PENDING_OPEN_UNI_DATA,
    PENDING_HEADER_PAYLOAD,
    PENDING_PAYLOAD_ONLY,
    PENDING_COPIED_WRITE,
    PENDING_COPIED_DATAGRAM,
    PENDING_OPEN_CONTROL,
    PENDING_OPEN_UNI_CONTROL,
    PENDING_OPEN_BIDI_DATA,
    PENDING_CLOSE_BIDI_FIN,
    PENDING_RESET_STREAM,
    PENDING_STOP_SENDING,
    PENDING_ABORT_STREAM,   /* whole-stream abort not yet applied (or,
                               in the sequential fallback, its RESET
                               half not yet accepted) */
    PENDING_CLOSE_TRANSPORT,
} bridge_pending_kind_t;

typedef struct {
    bridge_pending_kind_t kind;
    moq_action_t          act;
    moq_stream_ref_t      stream_ref;
    uint64_t              stream_id;
    uint8_t              *data;
    size_t                data_len;
    bool                  fin;
    bool                  owns_action;
    uint64_t              error_code;
    /* PENDING_RESET_STREAM of a unidirectional data stream (RESET_DATA):
     * reset through the reliable-reset path with reliable_size (already
     * bounded by the bytes the stream is handed before it). A uni reset that
     * names only stream_ref was retained before its stream was opened; the
     * retry resolves the stream and retires its mapping. */
    bool                  uni_reset;
    uint64_t              reliable_size;
    /* Ordering identity, normalised once at the enqueue boundary so the
     * conflict scan does only field comparisons. `dom` is the domain class
     * (bridge_domain_t); canon_ref / canon_sid are the resolved names, with
     * canon_have_sid false only while no mapping exists yet (a pre-open
     * item). Internal to the bridge: no public field, no ABI. */
    /* Whether stream_id names a real transport stream. Carried explicitly,
     * because transport stream id 0 is LEGAL -- inferring knowledge from
     * `stream_id != 0` would alias a legitimate id 0 with "unknown". */
    bool                  has_stream_id;
    uint8_t               dom;
    bool                  canon_have_sid;
    uint64_t              canon_ref;
    uint64_t              canon_sid;
} bridge_pending_item_t;

/* A local data stream's MOQT 7.2 scheduling state, from its SET_DATA_PRIORITY
 * key until its FIN or reset leaves the bridge. Its SEND_DATA actions wait in
 * `q` until the endpoint can take them, best key first. Slot links use
 * BRIDGE_INDEX_EMPTY for none. */
typedef struct {
    moq_data_priority_t key;
    moq_stream_ref_t    ref;
    uint64_t            sid;
    uint64_t            reset_code;
    uint64_t            reset_reliable;
    uint64_t            served;     /* serve_seq of its last hand-off */
    moq_action_t       *q;          /* ring of owned SEND_DATA actions */
    uint32_t            q_cap;
    uint32_t            q_head;
    uint32_t            q_len;
    uint32_t            pos;        /* place within (owner, publisher prio) */
    uint32_t            applied;    /* endpoint key last set */
    uint32_t            hkey;       /* endpoint key of its next object */
    uint32_t            retained;   /* its writes the bridge retains */
    uint32_t            heap_at;    /* place in dheap[heap_id] */
    uint32_t            owner_prev; /* streams of key.owner */
    uint32_t            owner_next;
    uint32_t            next_free;  /* free slot list */
    uint8_t             pub;        /* publisher priority of the next object */
    uint8_t             heap_id;    /* 0: open, 1: not yet open */
    bool                active;
    bool                has_sid;
    bool                keyed;      /* the endpoint holds `applied` */
    bool                fin_handed;
    /* A reset waiting behind the queued prefix its reliable size covers. */
    bool                has_reset;
} bridge_dstream_t;

/* Open-addressing entry mapping an ID to a dstream slot. */
typedef struct {
    uint64_t id;
    uint32_t slot;                  /* BRIDGE_INDEX_EMPTY when unused */
} bridge_index_entry_t;

#define BRIDGE_INDEX_EMPTY UINT32_MAX

/* -- Bridge --------------------------------------------------------- */

struct moq_transport_bridge {
    moq_alloc_t                         alloc;
    moq_session_t                      *session;
    const moq_transport_endpoint_ops_t *ops;
    void                               *endpoint_ctx;

    bool     fatal;
    uint64_t fatal_code;
    bool     closed;
    uint64_t close_code;
    bool     needs_close;       /* deferred close (e.g. control FIN) */
    uint64_t needs_close_code;
    bool     is_client;

    bool     control_open;
    uint64_t control_stream_id;
    bool     peer_control_open;
    uint64_t peer_control_stream_id;
    bool     pending_control;
    uint64_t pending_control_stream_id;
    bool     pending_control_fin;

    /* Control-channel topology. BIDI (default) uses the fields above.
     * UNI_PAIR uses the local/peer unidirectional channels below. */
    bridge_control_mode_t control_mode;
    /* Local unidirectional control channel (opened via OPEN_UNI_CONTROL). */
    bool             local_ctrl_uni_open;
    uint64_t         local_ctrl_uni_stream_id;
    moq_stream_ref_t local_ctrl_uni_ref;
    /* Peer unidirectional control channel, once a peer uni stream is accepted
     * as control. A second, distinct peer control channel is a violation. */
    bool             peer_ctrl_uni_open;
    uint64_t         peer_ctrl_uni_stream_id;

    uint64_t next_inbound_ref;

    bridge_stream_entry_t *streams;
    uint32_t               max_streams;
    uint32_t               streams_active;

    bridge_pending_item_t *pending;
    uint32_t               pending_count;
    uint32_t               max_pending;
    uint32_t               pending_opens;  /* OPEN_UNI_DATA */

    uint64_t              *tombstones;
    uint32_t               tombstone_count;
    uint32_t               max_tombstones;

    /* Held peer streams; allocated only when the endpoint advertises
     * MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT, on every profile (holds == NULL
     * otherwise). Request streams, and peer uni streams of a complete data
     * type that find no mapping free beyond the control reserve, are held
     * without a mapping only on profiles whose control channel is a
     * unidirectional pair; every peer uni stream may be held so on a
     * bidi-control profile. */
    bridge_hold_t         *holds;
    uint32_t               max_holds;
    uint64_t               hold_seq;
    bridge_dstream_t      *dstreams;       /* dstream_cap entries, grows */
    uint32_t               dstream_cap;
    uint32_t               dstream_free;   /* free list; dstream_cap if empty */
    bridge_index_entry_t  *dref_index;     /* ref -> slot */
    bridge_index_entry_t  *dsid_index;     /* stream ID -> slot */
    bridge_index_entry_t  *downer_index;   /* owner -> first of its slots */
    uint32_t               dindex_mask;    /* index size - 1 */
    /* Streams ready to hand data over, best key first: open ones, and ones
     * still to open (a blocked open holds back only those). */
    uint32_t              *dheap[2];
    uint32_t               dheap_len[2];
    uint32_t              *dskip;          /* streams refused in one pump */
    /* The pump's write: one the endpoint refuses before any byte went out
     * comes back to its stream instead of the pending FIFO. */
    bool                   pump_hand_back;
    bool                   pump_handed_back;
    uint64_t               dq_bytes;       /* bytes queued across dstreams */
    uint32_t               dq_items;       /* actions queued across dstreams */
    uint64_t               max_retained_bytes;
    uint64_t               serve_seq;
};

/* -- Internal helpers ----------------------------------------------- */

/* Capture this role while accepting output, before stream mappings retire.
 * Read-only and non-installed; no additional core symbol or execution path. */
static inline bool bridge_stream_is_session_control(
    const moq_transport_bridge_t *b, uint64_t stream_id)
{
    if (!b) return false;
    if (b->control_mode == BRIDGE_CONTROL_UNI_PAIR)
        return (b->local_ctrl_uni_open && b->local_ctrl_uni_stream_id == stream_id) ||
               (b->peer_ctrl_uni_open && b->peer_ctrl_uni_stream_id == stream_id);
    return (b->control_open && b->control_stream_id == stream_id) ||
           (b->peer_control_open && b->peer_control_stream_id == stream_id);
}

/* Session-side helpers for the bridge's retained subgroup FIN only. None
 * is public/exported. The profile owns header classification; the session
 * owns action-capacity admission, including when its subgroup slot was reaped. */
uint8_t bridge_session_subgroup_header_size(
    const moq_session_t *s, const moq_send_data_action_t *data);

moq_result_t bridge_session_on_subgroup_stop(
    moq_session_t *s, moq_stream_ref_t ref, uint64_t error_code,
    uint64_t now_us, uint8_t retained_header_len);

/* A reset action retains its stream identity even after its core slot reaps. */
bool bridge_session_has_queued_data_reset(
    const moq_session_t *s, moq_stream_ref_t ref);

bridge_stream_entry_t *bridge_alloc_stream(moq_transport_bridge_t *b);

bridge_stream_entry_t *bridge_find_by_ref(
    moq_transport_bridge_t *b, moq_stream_ref_t ref);

bridge_stream_entry_t *bridge_find_by_id(
    moq_transport_bridge_t *b, uint64_t transport_id);
const bridge_stream_entry_t *bridge_find_by_id_const(
    const moq_transport_bridge_t *b, uint64_t transport_id);

void bridge_deactivate_stream(moq_transport_bridge_t *b,
                              bridge_stream_entry_t *e);
/* Retire an aborting entry on any terminal peer signal (FIN/reset/
 * stream terminal). No-op if the entry is absent or not aborting. */
void bridge_retire_aborting(moq_transport_bridge_t *b, uint64_t transport_id);

moq_stream_ref_t bridge_assign_inbound_ref(
    moq_transport_bridge_t *b, uint64_t transport_id,
    bridge_stream_kind_t kind);

void bridge_mark_local_close(
    moq_transport_bridge_t *b, moq_stream_ref_t ref);

void bridge_mark_peer_close(
    moq_transport_bridge_t *b, moq_stream_ref_t ref);

void bridge_retire_or_tombstone(
    moq_transport_bridge_t *b, moq_stream_ref_t ref);

bool bridge_is_tombstoned(
    const moq_transport_bridge_t *b, uint64_t transport_id);

void bridge_remove_tombstone(
    moq_transport_bridge_t *b, uint64_t transport_id);

void bridge_set_fatal(moq_transport_bridge_t *b, uint64_t code);

void bridge_cleanup_pending_item(
    const moq_alloc_t *alloc, bridge_pending_item_t *item);

void bridge_cleanup_all_pending(moq_transport_bridge_t *b);

void bridge_pending_take_action(
    bridge_pending_item_t *item, moq_action_t *act);

bool bridge_stream_has_inbound_pending(
    const bridge_stream_entry_t *e);

/*
 * Decide how to route an inbound peer unidirectional stream given its class,
 * updating peer control-channel acceptance state:
 *   DATA      -> BRIDGE_UNI_ROUTE_DATA      (caller uses the data-stream path)
 *   PADDING   -> BRIDGE_UNI_ROUTE_HANDLED   (caller discards the bytes)
 *   NEED_MORE -> BRIDGE_UNI_ROUTE_NEED_MORE (caller retains until more bytes)
 *   UNKNOWN   -> BRIDGE_UNI_ROUTE_FATAL     (caller closes with a violation)
 *   CONTROL   -> first accepted channel (records peer_ctrl_uni_*) returns
 *                HANDLED; the same channel again returns HANDLED; a second,
 *                distinct channel returns FATAL.
 * Pure decision + state update: does not set fatal or touch the transport.
 */
bridge_uni_route_t bridge_route_peer_uni(
    moq_transport_bridge_t *b, moq_uni_class_t cls, uint64_t stream_id);

/*
 * Outcome of a budgeted service pass.
 *
 * suspended    a deferred-completion sweep could not finish within the budget.
 *              The session cursor holds the resume point; a later pass
 *              continues from there. The operation that OBSERVED the
 *              suspension retains its retry/deferred state, while earlier work
 *              in the same pass remains committed -- a pass may legitimately
 *              commit several owners and then suspend on the next.
 * sweep_spent  sweep steps charged by this call.
 */
typedef struct moq_bridge_budgeted_result {
    bool     suspended;
    uint32_t sweep_spent;
} moq_bridge_budgeted_result_t;

/*
 * Service the bridge with a bounded deferred-completion sweep.
 *
 * WHAT sweep_budget METERS, precisely: steps of the session's deferred
 * completion stage machine (REAP_SUBGROUPS / PUB / SUB). It does NOT
 * meter outbound pending items retried, actions dispatched, control messages
 * handled, or rx parse units -- so this is not an invocation-wide work bound,
 * and must not be described as one.
 *
 * A budget of 0 is legal and useful: a pool that is idle, or holds only
 * future/count-unsatisfied owners, has no runnable work and completes anyway.
 * Only runnable work that cannot be afforded suspends.
 *
 * out is REQUIRED. A finite budget that can suspend, paired with a discarded
 * outcome, would report a drained pass while a cursor is still live -- the
 * hidden stall this exists to prevent. Returns MOQ_ERR_INVAL when out is NULL;
 * *out is zeroed before any other argument is validated.
 *
 * moq_transport_bridge_service() is the unlimited path and does NOT route
 * through here: it enters no budget context at all, which is what makes it
 * structurally incapable of suspending.
 *
 * Not exported. Callable within moq-core and from white-box tests linking
 * moq-core-test-internals; promotion to MOQ_API belongs with the first
 * out-of-library caller.
 */
moq_result_t moq_transport_bridge_service_budgeted(
    moq_transport_bridge_t *b, uint64_t now_us, uint32_t sweep_budget,
    moq_bridge_budgeted_result_t *out);

#endif /* MOQ_TRANSPORT_BRIDGE_INTERNAL_H */
