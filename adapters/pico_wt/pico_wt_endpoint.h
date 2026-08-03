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

/*
 * WebTransport stream-error mapping (draft-ietf-webtrans-http3 §4.4). A MoQ
 * application error code (e.g. the §3.3.3 request-stream abort code
 * CANCELLED=0x1) rides a WT stream as an HTTP/3 RESET_STREAM / STOP_SENDING
 * code. A raw MoQ code is NOT a valid WebTransport application error — it is
 * an arbitrary value in HTTP/3's error space — so it MUST be mapped into the
 * WebTransport application-error range. (This is a wire-conformance fix; it is
 * not what caused the observed draft-18 WT teardown — that came from picoquic
 * locally rejecting the non-negotiated reliable-size reset; see ep_reset.)
 *
 * The §4.4 mapping is NOT a plain additive base (that is picoquic's
 * simplified H3ZERO_WEBTRANSPORT_APPLICATION_ERROR, correct only for
 * n < 0x1e): it skips the reserved HTTP/3 GREASE points (0x1f*N + 0x21).
 *   encode(n) = FIRST + n + n/0x1e            (n <= UINT32_MAX)
 *   decode(h) = shifted - shifted/0x1f        (shifted = h - FIRST)
 * FIRST is picoquic's pinned base (H3ZERO_WEBTRANSPORT_APPLICATION_ERROR(0));
 * LAST = encode(UINT32_MAX) bounds the space. WebTransport codes are limited
 * to UINT32_MAX, so encode of a larger MoQ code fails. */
#define PICO_WT_WT_ERR_FIRST (H3ZERO_WEBTRANSPORT_APPLICATION_ERROR(0))
#define PICO_WT_WT_ERR_LAST                                                \
    (PICO_WT_WT_ERR_FIRST + (uint64_t)UINT32_MAX +                         \
     (uint64_t)UINT32_MAX / 0x1eull)

/* MoQ code -> HTTP/3-space WebTransport code. Returns false (nothing mapped)
 * for a code outside the WebTransport range [0, UINT32_MAX]; the caller must
 * fail the reset/stop without mutating stream state. */
static inline bool pico_wt_moq_err_to_wt(uint64_t moq_code, uint64_t *out)
{
    if (moq_code > (uint64_t)UINT32_MAX) return false;
    *out = PICO_WT_WT_ERR_FIRST + moq_code + moq_code / 0x1eull;
    return true;
}

/* Inbound HTTP/3-space code -> MoQ code. Only genuine, non-reserved
 * WebTransport codes in [FIRST, LAST] are decoded; anything else (a legacy
 * peer's raw MoQ code, a reserved GREASE point, or a non-WT error) is
 * preserved unchanged. */
static inline uint64_t pico_wt_wt_err_to_moq(uint64_t h3_code)
{
    if (h3_code < PICO_WT_WT_ERR_FIRST || h3_code > PICO_WT_WT_ERR_LAST)
        return h3_code;
    uint64_t shifted = h3_code - PICO_WT_WT_ERR_FIRST;
    if (shifted % 0x1full == 0x1eull) return h3_code;  /* reserved GREASE */
    return shifted - shifted / 0x1full;
}

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
    /* Private (adapter-owned) notification: a LOCAL stop_sending cancelled our
     * receive direction on this stream, so tracked receive state must be
     * dropped (retained bytes must never be replayed for a stream the
     * application just stopped). Invoked ONLY after picoquic_stop_sending
     * returns success; a failed stop leaves receive state untouched. */
    void (*on_local_stop_sending)(void *ctx, uint64_t stream_id);
    void *cb_ctx;

    /* App callback set on locally-created WT data streams so h3zero
     * dispatches inbound response bytes through path_callback. */
    picohttp_post_data_cb_fn app_callback;
    void *app_callback_ctx;

    /* Capability-aware stream reset: true when the peer's RESET_STREAM_AT
     * support was SYNTHESIZED (the managed client faked the transport
     * parameter to unblock the WT CONNECT against a relay that omits it —
     * see allow_peer_missing_reset_stream_at). Reliable-reset was not actually
     * negotiated for such a peer, so a reliable_size preamble would be refused
     * locally by picoquic; ep_reset must emit a plain RESET_STREAM. Default
     * false (attach path / genuine negotiation): the live peer TP is
     * authoritative. */
    bool reset_stream_at_synthesized;

    /* Internal test witness (not exported): the reliable_size ep_reset last
     * used for a stream abort -- >0 for the RESET_STREAM_AT preamble taken on
     * genuine reliable-reset negotiation, 0 for the plain RESET_STREAM
     * downgrade. Lets a white-box test pin the branch directly. */
    size_t last_reset_reliable_size;
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
