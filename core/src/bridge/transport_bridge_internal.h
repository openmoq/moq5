#ifndef MOQ_TRANSPORT_BRIDGE_INTERNAL_H
#define MOQ_TRANSPORT_BRIDGE_INTERNAL_H

#include <moq/transport_bridge.h>
#include <moq/session.h>
#include "../session/session_transport.h"   /* moq_uni_class_t + capability */
#include "../session/action_meta.h"
#include "stream_order.h"

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
    /* A bidi whose peer RESET the session accepted: the peer's half is over,
     * and the entry is kept only for our own send half (closed, or reset in
     * answer to a STOP, before it retires). */
    bool                   peer_reset_accepted;
    /* After that RESET ended the request, our still-open send half was
     * cancelled by us (a reset with CANCELLED is owed or sent): one reset per
     * half, so a later STOP or session reset adds none. */
    bool                   local_cancel_queued;
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
     * fresh ref). A genuine FIN / RESET / stream terminal is latched;
     * service retires the drain identity and mapping after outbound
     * obligations settle. Set by dispatch_abort_bidi. */
    bool                   aborting;
    /* A local unidirectional data stream whose FIN has been written, kept
     * mapped until the adapter reports it committed or the session resets it
     * (MOQ_TRANSPORT_CAP_COMMIT_REPORT). */
    bool                   awaiting_commit;
    /* First accepted stream header, classified by the session profile. Zero
     * for non-subgroup streams. Survives zero-timeout core-slot retirement. */
    uint8_t                subgroup_header_len;
    /* A retained FIN or outgoing terminal is being reset; keep its mapping
     * until reset acceptance, ignoring later commit/STOP callbacks. */
    bool                   fin_reset_pending;
    /* A pressure-path reset remains in core storage through endpoint refusal,
     * even after discarding its data has relieved the byte pressure. */
    bool                   reset_source_held;
    /* A core terminal debt owns this outgoing ref, even before its RESET
     * reaches the bridge. Never retire its mapping on a late ACK. */
    bool                   data_terminal_received;
    /* Local unidirectional data stream: bytes the endpoint has accepted
     * (header and payload writes). Bounds a reset's reliable size. */
    uint64_t               bytes_sent;
    /* Local receive retirement of a peer data stream the session stopped.
     * local_stop_accepted: the endpoint accepted our STOP_SENDING for it
     * (STOP_DATA, on its first dispatch or on a pending retry -- never a
     * refusal or WOULD_BLOCK). transport_final: the adapter reported the
     * transport finished with the stream (moq_transport_bridge_on_peer_
     * stream_terminal) before the peer's FIN or RESET reached the session.
     * With both, and no inbound obligation still pending here, the next
     * service pass releases the mapping and the session's stopped owner
     * (bridge_session_on_data_retired). Neither is a FIN or a reset. */
    bool                   local_stop_accepted;
    /* Also latches a genuine FIN/RESET/full terminal for an aborting bidi;
     * it never substitutes a peer terminal input at the session. */
    bool                   transport_final;
    /* Inbound peer-uni classification (uni-control-pair mode only). */
    uint8_t                uni_disp;        /* bridge_uni_disp_t */
    uint8_t                classify_len;    /* retained leading bytes (< 9) */
    uint8_t                classify_buf[9];
    bool                   known_data_type; /* valid type, not topology alone */
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
    bool     known_data_type; /* strict profile type, before pressure policy */
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
    moq_action_meta_t     meta;
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
    /* PENDING_STOP_SENDING retained from a STOP_DATA (a peer data stream the
     * session stopped): its acceptance on retry is local_stop_accepted. */
    bool                  data_stop;
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
    moq_action_t action;
    moq_action_meta_t meta;
} bridge_data_item_t;

typedef struct {
    moq_data_priority_t key;
    moq_action_meta_t   meta;       /* current handed or retained class */
    moq_action_meta_t   order_meta; /* representative in the current graph */
    moq_stream_ref_t    ref;
    uint64_t            sid;
    uint64_t            reset_code;
    uint64_t            reset_reliable;
    uint64_t            served;     /* serve_seq of its last hand-off */
    uint64_t            reset_pass; /* pass_seq of its last reset attempt */
    bridge_data_item_t *q;          /* action and metadata share ownership */
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
    bool                prefix_only; /* only the FETCH preamble was handed over */
    bool                fin_handed;
    /* A reset waiting behind the queued prefix its reliable size covers. */
    bool                has_reset;
    bool                reset_terminal; /* typed terminal, even before open */
    bool                order_dirty;
    bool                order_active;
    bool                pump_skipped;
    bool                dispatching;
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
    /* A stopped peer data stream or locally-aborted bidi has a terminal
     * latched: the service pass scans for retirement work. */
    bool     local_retire_due;
    /* A bidi whose peer RESET the session accepted may still owe our own
     * request cancellation (its owner ended, our half open): the service pass
     * decides. */
    bool     bidi_cancel_due;
    /* Some stream entry may hold inbound work for the service pass (an
     * admission readiness mark, a retained retry, a deferred RESET or
     * STOP): set wherever one is marked, cleared only by the walk that
     * visits every entry, so a clear flag means the walk would find none. */
    bool     inbound_due;
    /* The endpoint's budget_spent op, copied at create when the endpoint
     * advertises MOQ_TRANSPORT_CAP_SHARED_ADMISSION and its struct_size
     * covers the op; NULL otherwise. */
    bool   (*budget_spent)(void *ctx);
    uint64_t needs_close_code;
    uint64_t now_us;            /* time supplied to the current service pass */
    bool     done_waiting;      /* a core terminal waited for bridge output */
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
     * otherwise). Request streams may be held without a mapping on every
     * profile. Peer uni streams require a complete data type before taking
     * such a hold on a uni-control profile; on a bidi-control profile every
     * peer uni stream is data and may be held before classification. */
    bridge_hold_t         *holds;
    uint32_t               max_holds;
    uint64_t               hold_seq;
    uint32_t               holds_live; /* records not BRIDGE_HOLD_FREE */
    /* One receive-entry cancellation per waiting DATA demand. A successful
     * STOP keeps its ref here until the session's RX allocation ends; STOP
     * acceptance alone never means capacity was recovered. */
    moq_stream_ref_t       *pressure_victims; /* max_holds entries */
    uint32_t               pressure_count;
    moq_stream_ref_t        pressure_choice; /* retained on ring refusal */
    /* A pending answer after the last service needs a new pass when an
     * outside-service release makes room. A paired adapter sweep owns every
     * release during service, including a suspended budgeted call. */
    bool                   may_wait;
    bool                   ready_due;
    bool                   in_service;
    uint64_t               ready_at;
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
    moq_stream_order_node_t *order_nodes;  /* scratch, bounded by dstream_cap */
    uint32_t              *order_slots;
    bool                   order_dirty;
    uint32_t               pump_budget;  /* shared by pumps in one service call */
    /* The pump's write: one the endpoint refuses before any byte went out
     * comes back to its stream instead of the pending FIFO. */
    bool                   pump_hand_back;
    bool                   pump_handed_back;
    /* At full pending capacity a mapped head keeps even its partially sent
     * action in its existing queue slot; header_len then counts unsent bytes. */
    bool                   pump_keep_head;
    /* The pump's write was refused (handed back, or retained after a part
     * of it went). */
    bool                   pump_refused;
    uint64_t               dq_bytes;       /* bytes queued across dstreams */
    uint32_t               dq_items;       /* actions queued across dstreams */
    uint32_t               dq_timed;       /* of those, with a delivery deadline */
    uint32_t               dopen_heads;    /* active dstreams with nothing
                                            * queued, no FIN handed and no
                                            * deferred reset */
    uint32_t               dreset_count;   /* active dstreams holding a deferred
                                            * reset (has_reset) */
    /* Service passes begun, from 1: a dstream whose reset_pass equals it
     * has had its reset attempt in this pass. */
    uint64_t               pass_seq;
    uint64_t               deadline_slots; /* dstream slots deadline walks read */
    uint64_t               max_retained_bytes;
    uint64_t               serve_seq;
};

#ifdef MOQ_SESSION_SWEEP_TESTING
/* Exact-work probe counters for the per-pass dstream-table walks, the same
 * kind as the session's (session_internal.h): each is a plain increment at a
 * loop-body entry, none takes part in any decision, and they are compiled
 * solely into moq-core-test-internals. A probe is one dstream slot (or one
 * queued item) a walk reads. Tests assert deltas around a call. */
extern uint64_t bridge_work_expiry_slots;  /* slots read, queued-expiry walk */
extern uint64_t bridge_work_expiry_items;  /* queued items read, same walk */
extern uint64_t bridge_work_reset_slots;   /* slots read, deferred-reset walk */
extern uint64_t bridge_work_query_slots;   /* slots read, has_pending's reset walk */
extern uint64_t bridge_work_terminal_slots; /* slots read, new-terminal prune */
extern uint64_t bridge_work_inbound_slots; /* stream entries read, inbound retry walk */
#endif

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

/* Session-side helpers for outgoing subgroup terminals/retained FIN. None
 * is public/exported. The profile owns header classification; the session
 * owns action-capacity admission, including when its subgroup slot was reaped. */
uint8_t bridge_session_subgroup_header_size(
    const moq_session_t *s, const moq_send_data_action_t *data);
/* Private first-open facts, unrelated to FIN acknowledgement/commit. */
bool bridge_session_resolve_data_open(moq_session_t *s, moq_stream_ref_t ref,
                                      uint64_t owner, uint8_t disposition);
bool bridge_session_suppress_data_open(moq_session_t *s, moq_stream_ref_t ref,
                                       uint64_t owner);
void bridge_session_abandon_data_opens(moq_session_t *s);
bool bridge_session_is_fetch_header(
    const moq_session_t *s, const moq_send_data_action_t *data);
/* Pure profile classification: a complete subgroup/FETCH type, without
 * allocating a receive entry or parsing the remaining header. */
bool bridge_session_has_data_stream_type(
    const moq_session_t *s, const uint8_t *data, size_t len);

moq_result_t bridge_session_on_subgroup_stop(
    moq_session_t *s, moq_stream_ref_t ref, uint64_t error_code,
    uint64_t now_us, uint8_t retained_header_len);

/* A reset action retains its stream identity even after its core slot reaps. */
bool bridge_session_has_queued_data_reset(
    const moq_session_t *s, moq_stream_ref_t ref);
bool bridge_session_has_data_terminal(const moq_session_t *s, moq_stream_ref_t ref);
/* Consume only the need to inspect newly recognized terminals. Their event
 * and reset obligations remain session-owned until ordinary settlement. */
bool bridge_session_take_terminal_changes(moq_session_t *s);
bool bridge_session_is_outgoing_subgroup(const moq_session_t *s, moq_stream_ref_t ref);
/* Service only due outgoing subgroup work, including behind blocked output. */
moq_result_t bridge_session_service_outgoing(moq_session_t *s, uint64_t now_us);

/* Cancellation under output pressure. The offer is a value copy of the first
 * reset after a data-only run, or an owed reset after a data-only ring. It
 * stays session-owned until commit. The bridge may discard that ref's queued
 * data only after proving its complete reliable prefix was handed over.
 * No public polling contract or action layout changes. */
bool bridge_session_offer_data_reset(moq_session_t *s,
                                     moq_reset_data_action_t *out);
/* Claim only after pressure/prefix checks: a terminal event and its compact
 * reset become locally owned together, before any provider reset attempt. */
bool bridge_session_claim_data_reset(moq_session_t *s,
                                     const moq_reset_data_action_t *reset);
void bridge_session_discard_reset_data(moq_session_t *s, moq_stream_ref_t ref);
bool bridge_session_commit_data_reset(moq_session_t *s,
                                      const moq_reset_data_action_t *reset);
/* After a refused offer, admit leading data without consuming its reset or
 * any intervening control barrier. The caller reserves ordinary capacity. */
bool bridge_session_poll_leading_data(moq_session_t *s, moq_action_t *out,
                                     moq_action_meta_t *meta);
/* Same settlement and ownership as public polling; copy the selected scalar
 * metadata before compaction or owner retirement can invalidate its source. */
bool bridge_session_poll_action(moq_session_t *s, moq_action_t *out,
                                moq_action_meta_t *meta);
/* The bridge can retain an owner's stream closure after the session has
 * queued DONE. Leave that terminal in core storage until the bridge accepts
 * its prerequisites; unrelated ordering domains remain eligible. */
typedef bool (*bridge_done_ready_fn)(void *ctx, uint64_t owner);
bool bridge_session_poll_ready_action(moq_session_t *s, moq_action_t *out,
    moq_action_meta_t *meta, bridge_done_ready_fn ready, void *ctx);
/* Full pending capacity: select only mapped DATA or a leading session close.
 * Ordinary data ordering remains intact; SKIP permits crossing a proven
 * map-blocked request, never a connection/control barrier. The gate's first
 * call only inspects. Its reserve call must secure ownership before take,
 * without advancing the session or calling the endpoint. False admission
 * leaves the action session-owned. No borrowed action escapes this call.
 * `data_open` false promises the gate admits no SEND_DATA; only a leading
 * session close can then be selected, so the scan stops at the head. */
typedef enum {
    BRIDGE_ACTION_BARRIER,
    BRIDGE_ACTION_SKIP,
    BRIDGE_ACTION_READY,
} bridge_action_gate_t;
typedef bridge_action_gate_t (*bridge_action_gate_fn)(
    void *ctx, const moq_action_t *action, const moq_action_meta_t *meta,
    bool reserve);
bool bridge_session_poll_mapped_action(moq_session_t *s, moq_action_t *out,
    moq_action_meta_t *meta, bool data_open, bridge_action_gate_fn gate,
    void *ctx);
/* Transfer an expired subgroup's reset debt into its existing bridge owner.
 * No action slot is needed. False retains a terminal waiting for event credit
 * in the session; do not dispatch its reset yet. The terminal flag is returned
 * on either outcome. An earlier reset keeps its cause and provenance.
 * Retry timing uses recognition time now_us; this does not advance the session. */
bool bridge_session_take_data_timeout(moq_session_t *s, moq_stream_ref_t ref,
                                      uint64_t now_us,
                                      moq_reset_data_action_t *reset,
                                      bool *outgoing_terminal);

/* Local receive retirement (session_receive.c): the transport finished with a
 * peer data stream after our accepted STOP_SENDING, and no FIN or RESET for it
 * will ever reach the session. Releases the session's stopped owner for the
 * ref -- a STOPPED receive entry or an identity-only stopped record -- with no
 * event and no FIN/reset claim; a still-bound entry's stream counts once
 * toward its binding's Stream Count, and a bound fill child settles locally.
 * MOQ_OK: released. MOQ_ERR_STALE_HANDLE: the session holds nothing for the
 * ref. MOQ_ERR_WRONG_STATE: the ref is a live (not stopped) owner, left as it
 * is. MOQ_ERR_CLOSED: the session is closed. */
moq_result_t bridge_session_on_data_retired(
    moq_session_t *s, moq_stream_ref_t ref, uint64_t now_us);

/* The request stream `ref` has no owner left at the session: no live request,
 * carrier, receive or drain owner for it, and no queued session action names
 * it (a queued response, close, reset or abort keeps its own order). */
bool bridge_session_bidi_owner_ended(const moq_session_t *s, moq_stream_ref_t ref);

/* Service-only settlement after a locally-aborted bidi's genuine terminal.
 * Advances normally, refuses a live owner or same-ref queued bidi action,
 * then removes only this drain ref, with no FIN/reset/event/cause claim.
 * OK: removed; STALE_HANDLE: already absent; WRONG_STATE: still owned;
 * suspension/backpressure leaves the identity untouched for retry. */
moq_result_t bridge_session_on_bidi_retired(
    moq_session_t *s, moq_stream_ref_t ref, uint64_t now_us);

bridge_stream_entry_t *bridge_alloc_stream(moq_transport_bridge_t *b);

bridge_stream_entry_t *bridge_find_by_ref(
    moq_transport_bridge_t *b, moq_stream_ref_t ref);

bridge_stream_entry_t *bridge_find_by_id(
    moq_transport_bridge_t *b, uint64_t transport_id);
const bridge_stream_entry_t *bridge_find_by_id_const(
    const moq_transport_bridge_t *b, uint64_t transport_id);

void bridge_deactivate_stream(moq_transport_bridge_t *b,
                              bridge_stream_entry_t *e);
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
