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
#define BRIDGE_DEFAULT_MAX_TOMBSTONES  64

#define BRIDGE_INBOUND_REF_BASE (1ULL << 63)

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
    /* Inbound pending state (bridge-owned) */
    bool                   pending_retry;
    bool                   pending_fin;
    bool                   fin_retained;
    bool                   pending_reset;
    uint64_t               pending_reset_code;
    bool                   pending_stop;
    uint64_t               pending_stop_code;
    /* Inbound peer-uni classification (uni-control-pair mode only). */
    uint8_t                uni_disp;        /* bridge_uni_disp_t */
    uint8_t                classify_len;    /* retained leading bytes (< 9) */
    uint8_t                classify_buf[9];
} bridge_stream_entry_t;

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
} bridge_pending_item_t;

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

    bridge_pending_item_t *pending;
    uint32_t               pending_count;
    uint32_t               max_pending;

    uint64_t              *tombstones;
    uint32_t               tombstone_count;
    uint32_t               max_tombstones;
};

/* -- Internal helpers ----------------------------------------------- */

bridge_stream_entry_t *bridge_alloc_stream(moq_transport_bridge_t *b);

bridge_stream_entry_t *bridge_find_by_ref(
    moq_transport_bridge_t *b, moq_stream_ref_t ref);

bridge_stream_entry_t *bridge_find_by_id(
    moq_transport_bridge_t *b, uint64_t transport_id);

void bridge_deactivate_stream(bridge_stream_entry_t *e);

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

#endif /* MOQ_TRANSPORT_BRIDGE_INTERNAL_H */
