#ifndef MOQ_PICO_WT_ENDPOINT_H
#define MOQ_PICO_WT_ENDPOINT_H

/*
 * Private endpoint context for picoquic WebTransport.
 *
 * Implements moq_transport_endpoint_ops_t by forwarding to
 * picoquic's h3zero/picohttp WebTransport API.
 *
 * Not installed. Internal to the pico_wt adapter.
 */

#include <moq/transport_bridge.h>
#include <pico_webtransport.h>
#include <h3zero_common.h>
#include "../common/moq_pq_send_queue.h"

typedef struct {
    picoquic_cnx_t *cnx;
    h3zero_callback_ctx_t *h3_ctx;
    h3zero_stream_ctx_t *control_stream_ctx;
    uint64_t control_stream_id;

    /* Adapter-owned outbound queue: WT MoQ data streams hold their bytes here
     * and copy them into picoquic's packet buffer from the h3zero provide_data
     * (pull) callback. */
    moq_pq_send_queue_t *queue;

    /* Datagram buffer (single-slot, lossy, inline — no heap) */
    size_t pending_dg_len;
    uint8_t pending_dg_buf[2048];

    /* Local stream opened callbacks (for bridge MoQ control
     * classification and test stream-ID tracking). */
    void (*on_bidi_opened)(void *ctx, uint64_t stream_id);
    void (*on_uni_opened)(void *ctx, uint64_t stream_id);
    void *cb_ctx;

    /* App callback set on locally-created WT data streams so h3zero
     * dispatches inbound response bytes through path_callback. */
    picohttp_post_data_cb_fn app_callback;
    void *app_callback_ctx;
} pico_wt_endpoint_ctx_t;

/* Returns 0 on success, -1 on allocation failure (queue create). */
int pico_wt_endpoint_init(moq_transport_endpoint_ops_t *ops,
                           pico_wt_endpoint_ctx_t *ctx,
                           picoquic_cnx_t *cnx,
                           h3zero_callback_ctx_t *h3_ctx,
                           h3zero_stream_ctx_t *control_stream_ctx,
                           const moq_alloc_t *alloc);

void pico_wt_endpoint_cleanup(pico_wt_endpoint_ctx_t *ctx);

/* Service an h3zero picohttp_callback_provide_data for `stream_id`: copy up to
 * `space` queued bytes into picoquic's buffer via `provide_ctx`, setting FIN
 * and still-active as the queue dictates. Reneges when nothing is queued.
 * Returns the value the WT callback should return (0). */
int pico_wt_endpoint_on_provide_data(pico_wt_endpoint_ctx_t *ctx,
                                     uint64_t stream_id,
                                     void *provide_ctx, size_t space);

/*
 * Called by the adapter when picohttp_callback_provide_datagram
 * fires. Flushes the pending datagram buffer.
 * Returns 0 on success, -1 if no datagram pending.
 */
int pico_wt_endpoint_provide_datagram(pico_wt_endpoint_ctx_t *ctx,
                                       uint8_t *context,
                                       size_t space);

#endif /* MOQ_PICO_WT_ENDPOINT_H */
