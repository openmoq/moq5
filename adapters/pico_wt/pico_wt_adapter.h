#ifndef MOQ_PICO_WT_ADAPTER_H
#define MOQ_PICO_WT_ADAPTER_H

/*
 * Picoquic WebTransport adapter — internal header.
 *
 * Defines the private moq_pico_wt_conn struct layout. The public,
 * opaque API lives in <moq/pico_wt.h>; this header is for the adapter
 * implementation and white-box tests that need internal fields
 * (bridge, stream counters, classification state).
 *
 * Not installed. Experimental, build-tree only.
 */

#include <moq/pico_wt.h>
#include "pico_wt_endpoint.h"

/*
 * Routing kind for an inbound WT data stream, preserved across pause/replay so
 * retained bytes re-enter the bridge through the same handler they would have
 * used when they arrived (a paused draft-16 control bidi replays through the
 * control path, not the bidi path).
 */
typedef enum pw_rx_kind {
    PW_RX_UNI = 0,
    PW_RX_BIDI = 1,
    PW_RX_CONTROL = 2
} pw_rx_kind_t;

/*
 * Per-stream inbound receive state.
 *
 * The WT callback hands us borrowed bytes and will not redeliver them, and the
 * session can refuse them (WOULD_BLOCK) when its event queue is full. So the
 * adapter owns this stream's QUIC receive window (picoquic_set_app_flow_control)
 * and keeps it at most `budget` bytes ahead of what the session has taken:
 * while the stream has retained work the window is NOT advanced, so the peer
 * stalls once it exhausts the frozen window. Bytes already inside that window
 * are retained here and replayed in order.
 *
 * `delivered` tracks picoquic's consumed_offset, which counts bytes handed from
 * the transport INTO the callback -- it is incremented before the callback runs
 * (picoquic/picoquic/frames.c) -- so it is NOT a measure of application
 * progress. Anchoring credit on it alone would replenish the window from
 * transport-to-callback delivery and let blocked session input grow without
 * bound. The retained-work gate is what prevents that: because no grant issues
 * while the stream still owes the bridge work, an advance can only follow the
 * session draining that work, and therefore corresponds to session catch-up.
 */
typedef struct pw_rx_stream {
    uint64_t stream_id;
    uint8_t *buf;          /* bytes retained while paused, awaiting replay */
    size_t   buf_len;
    size_t   buf_cap;
    uint64_t delivered;    /* callback-delivered bytes == consumed_offset;
                            * NOT application progress (see above) */
    uint64_t blocked;      /* bytes of the chunk the session last refused */
    uint64_t budget;       /* window headroom kept ahead of session intake */
    uint64_t granted;      /* absolute window last advertised */
    uint8_t  kind;         /* pw_rx_kind_t */
    bool     app_fc;       /* this adapter owns the stream's window */
    bool     paused;       /* has retained/blocked work; must not feed bridge */
    bool     buf_fin;      /* a peer FIN follows the retained bytes */
    bool     fin_blocked;  /* the FIN rode the chunk that blocked */
    bool     active;
} pw_rx_stream_t;

struct moq_pico_wt_conn {
    moq_session_t              *session;
    picoquic_cnx_t             *cnx;
    moq_alloc_t                 alloc;

    moq_transport_bridge_t     *bridge;
    moq_transport_endpoint_ops_t endpoint_ops;
    pico_wt_endpoint_ctx_t       endpoint_ctx;

    h3zero_callback_ctx_t      *h3_ctx;
    h3zero_stream_ctx_t        *control_stream_ctx;
    uint64_t                    control_stream_id;

    /* Accumulator for inbound WT capsules on the control stream
     * (CLOSE_WEBTRANSPORT_SESSION). Released in destroy. */
    picowt_capsule_t            inbound_capsule;

    /* MoQ control stream classification (perspective-aware) */
    moq_perspective_t           perspective;
    bool                        first_peer_wt_bidi_seen;
    bool                        prefix_registered;
    uint64_t                    moq_control_stream_id;

    void                       *user_ctx;

    /* Stream tracking (internal, for conformance/test use). */
    size_t                      opened_bidi_count;
    uint64_t                    last_opened_bidi_id;
    size_t                      opened_uni_count;
    uint64_t                    last_opened_uni_id;

    /* Inbound stop_sending tracking (internal, test-visible). */
    size_t                      stop_sending_count;
    uint64_t                    last_stop_sending_stream_id;

    /* Inbound receive-flow-control state, one entry per tracked WT data
     * stream. Grown on demand; slots are reused after a stream retires. */
    pw_rx_stream_t             *rx;
    size_t                      rx_count;
    size_t                      rx_cap;
};

#endif /* MOQ_PICO_WT_ADAPTER_H */
