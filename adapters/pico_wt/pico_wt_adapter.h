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
};

#endif /* MOQ_PICO_WT_ADAPTER_H */
