/*
 * picoquic_endpoint.c — Thin endpoint binding for picoquic.
 *
 * Implements moq_transport_endpoint_ops_t by forwarding to picoquic
 * API calls. Used by the bridge-based picoquic adapter.
 *
 * MoQ stream data uses picoquic's pull ("just in time") API: write() and
 * write_payload() copy/retain the bytes into the adapter-owned send queue and
 * mark the stream active; picoquic later asks for them via
 * picoquic_callback_prepare_to_send, serviced by pq_endpoint_on_prepare_to_send.
 * write returns WOULD_BLOCK when the queue's byte cap is reached, so the bridge
 * retains and retries. reset_stream and stop_sending return codes are checked —
 * non-zero maps to ERROR (the bridge treats this as fatal).
 */

#include "picoquic_endpoint.h"
#include <stddef.h>

/* PICOQUIC_ERROR_DATAGRAM_TOO_LONG is defined in picoquic.h */
#ifndef PICOQUIC_ERROR_DATAGRAM_TOO_LONG
#define PICOQUIC_ERROR_DATAGRAM_TOO_LONG 0x43b
#endif

static moq_transport_result_t pq_open_uni(void *ctx, uint64_t *out_id)
{
    pq_endpoint_ctx_t *ep = (pq_endpoint_ctx_t *)ctx;
    *out_id = picoquic_get_next_local_stream_id(ep->cnx, 1);
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t pq_open_bidi(void *ctx, uint64_t *out_id)
{
    pq_endpoint_ctx_t *ep = (pq_endpoint_ctx_t *)ctx;
    *out_id = picoquic_get_next_local_stream_id(ep->cnx, 0);
    return MOQ_TRANSPORT_OK;
}

/* Map a queue push result to the bridge write contract. */
static moq_transport_result_t pq_push_result(pq_endpoint_ctx_t *ep,
                                              uint64_t stream_id, int r)
{
    if (r < 0) return MOQ_TRANSPORT_ERROR;       /* allocation failure: fatal */
    if (r == 0) return MOQ_TRANSPORT_WOULD_BLOCK; /* queue cap: retain + retry */
    /* Bytes are queued; picoquic must poll the stream to pull them. A failure
     * to mark it active means they would never be sent -- treat as fatal. */
    if (picoquic_mark_active_stream(ep->cnx, stream_id, 1, NULL) != 0)
        return MOQ_TRANSPORT_ERROR;
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t pq_write(void *ctx, uint64_t stream_id,
                                        const uint8_t *data, size_t len,
                                        bool fin)
{
    pq_endpoint_ctx_t *ep = (pq_endpoint_ctx_t *)ctx;
    return pq_push_result(ep, stream_id,
        moq_pq_send_queue_push_copy(ep->queue, stream_id, data, len, fin));
}

static moq_transport_result_t pq_write_payload(void *ctx, uint64_t stream_id,
                                                moq_rcbuf_t *buf, bool fin)
{
    pq_endpoint_ctx_t *ep = (pq_endpoint_ctx_t *)ctx;
    return pq_push_result(ep, stream_id,
        moq_pq_send_queue_push_rcbuf(ep->queue, stream_id, buf, fin));
}

static moq_transport_result_t pq_reset(void *ctx, uint64_t stream_id,
                                        uint64_t error_code)
{
    pq_endpoint_ctx_t *ep = (pq_endpoint_ctx_t *)ctx;
    /* Abandon anything still queued for this stream and clear the active mark
     * before resetting, so no prepare_to_send lands for a reset stream. */
    moq_pq_send_queue_drop(ep->queue, stream_id);
    picoquic_mark_active_stream(ep->cnx, stream_id, 0, NULL);
    int rc = picoquic_reset_stream(ep->cnx, stream_id, error_code);
    return rc == 0 ? MOQ_TRANSPORT_OK : MOQ_TRANSPORT_ERROR;
}

void pq_endpoint_on_prepare_to_send(pq_endpoint_ctx_t *ep,
                                    uint64_t stream_id,
                                    void *provide_ctx, size_t max)
{
    ep->prepare_count++;
    size_t nb = 0; bool is_fin = false, still = false;
    if (!moq_pq_send_queue_plan(ep->queue, stream_id, max,
                                &nb, &is_fin, &still)) {
        /* Nothing queued: renege so picoquic can fill the packet otherwise. */
        (void)picoquic_provide_stream_data_buffer(provide_ctx, 0, 0, 0);
        return;
    }
    uint8_t *dst = picoquic_provide_stream_data_buffer(
        provide_ctx, nb, is_fin ? 1 : 0, still ? 1 : 0);
    if (dst) {
        moq_pq_send_queue_commit(ep->queue, stream_id, dst, nb);
        ep->provided_bytes += nb;
    }
}

void pq_endpoint_get_stats(const pq_endpoint_ctx_t *ep,
                           moq_pq_send_stats_t *out)
{
    if (!out) return;
    out->prepare_count = ep ? ep->prepare_count : 0;
    out->provided_bytes = ep ? ep->provided_bytes : 0;
    out->queue_high_water = ep ? moq_pq_send_queue_high_water(ep->queue) : 0;
    out->queue_would_block = ep ? moq_pq_send_queue_would_block_count(ep->queue) : 0;
}

static moq_transport_result_t pq_stop_sending(void *ctx, uint64_t stream_id,
                                               uint64_t error_code)
{
    pq_endpoint_ctx_t *ep = (pq_endpoint_ctx_t *)ctx;
    int rc = picoquic_stop_sending(ep->cnx, stream_id, error_code);
    return rc == 0 ? MOQ_TRANSPORT_OK : MOQ_TRANSPORT_ERROR;
}

/*
 * The negotiated max datagram frame size the PEER will accept, i.e. the
 * largest datagram we may send. Zero means the peer did not negotiate QUIC
 * DATAGRAM (no transport parameter), so this connection has no datagram
 * capability -- the honest per-connection answer regardless of the static
 * CAP_DATAGRAM advertisement.
 */
static size_t pq_max_datagram_size(void *ctx)
{
    pq_endpoint_ctx_t *ep = (pq_endpoint_ctx_t *)ctx;
    picoquic_tp_t const *remote =
        picoquic_get_transport_parameters(ep->cnx, 0 /* remote */);
    if (!remote) return 0;
    return (size_t)remote->max_datagram_frame_size;
}

static moq_transport_result_t pq_send_datagram(void *ctx,
                                                const uint8_t *data,
                                                size_t len)
{
    pq_endpoint_ctx_t *ep = (pq_endpoint_ctx_t *)ctx;

    /* Honesty gate: picoquic only enforces the negotiated limit for datagrams
     * above its "cautious length", so a small datagram queued on a connection
     * where the peer never negotiated DATAGRAM would sit in the queue and
     * never be sent. Refuse it deterministically instead of silently dropping
     * it later: report DROPPED when there is no negotiated capacity, TOO_LARGE
     * when the datagram exceeds the negotiated frame size. */
    picoquic_tp_t const *remote =
        picoquic_get_transport_parameters(ep->cnx, 0 /* remote */);
    if (!remote || remote->max_datagram_frame_size == 0)
        return MOQ_TRANSPORT_DROPPED;
    if (len > remote->max_datagram_frame_size)
        return MOQ_TRANSPORT_TOO_LARGE;

    int rc = picoquic_queue_datagram_frame(ep->cnx, len, data);
    if (rc == 0) return MOQ_TRANSPORT_OK;
    if (rc == PICOQUIC_ERROR_DATAGRAM_TOO_LONG)
        return MOQ_TRANSPORT_TOO_LARGE;
    return MOQ_TRANSPORT_DROPPED;
}

/*
 * picoquic_close_ex accepts a reason string. We forward the reason
 * if provided; picoquic limits the reason in the CONNECTION_CLOSE
 * frame to its internal buffer size.
 */
static moq_transport_result_t pq_close(void *ctx, uint64_t code,
                                        const uint8_t *reason,
                                        size_t reason_len)
{
    pq_endpoint_ctx_t *ep = (pq_endpoint_ctx_t *)ctx;
    int rc;
    if (reason && reason_len > 0) {
        char buf[256];
        size_t copy = reason_len < sizeof(buf) - 1 ? reason_len : sizeof(buf) - 1;
        for (size_t i = 0; i < copy; i++) buf[i] = (char)reason[i];
        buf[copy] = '\0';
        rc = picoquic_close_ex(ep->cnx, code, buf);
    } else {
        rc = picoquic_close(ep->cnx, code);
    }
    return rc == 0 ? MOQ_TRANSPORT_OK : MOQ_TRANSPORT_ERROR;
}

int pq_endpoint_init(moq_transport_endpoint_ops_t *ops,
                     pq_endpoint_ctx_t *ep_ctx,
                     picoquic_cnx_t *cnx,
                     const moq_alloc_t *alloc)
{
    ep_ctx->cnx = cnx;
    ep_ctx->queue = moq_pq_send_queue_create(alloc, 0);
    if (!ep_ctx->queue) return -1;

    *ops = (moq_transport_endpoint_ops_t){
        .struct_size     = sizeof(moq_transport_endpoint_ops_t),
        .capabilities    = MOQ_TRANSPORT_CAP_DATAGRAM |
                           MOQ_TRANSPORT_CAP_WRITE_PAYLOAD,
        .open_uni        = pq_open_uni,
        .open_bidi       = pq_open_bidi,
        .write           = pq_write,
        .write_payload   = pq_write_payload,
        .reset_stream    = pq_reset,
        .stop_sending    = pq_stop_sending,
        .send_datagram   = pq_send_datagram,
        .max_datagram_size = pq_max_datagram_size,
        .close_transport = pq_close,
    };
    return 0;
}

void pq_endpoint_cleanup(pq_endpoint_ctx_t *ep_ctx)
{
    if (!ep_ctx) return;
    moq_pq_send_queue_destroy(ep_ctx->queue);
    ep_ctx->queue = NULL;
}
