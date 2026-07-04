/*
 * pico_wt_endpoint.c — Picoquic WebTransport endpoint ops.
 *
 * Implements moq_transport_endpoint_ops_t by forwarding to
 * picoquic's h3zero/picohttp WebTransport API.
 *
 * Pull writes: write()/write_payload() hold MoQ data in the adapter-owned
 * send queue and mark the stream active with its h3zero stream context;
 * h3zero then routes prepare_to_send to picohttp_callback_provide_data, which
 * pico_wt_endpoint_on_provide_data services by copying queued bytes into
 * picoquic's packet buffer. write returns WOULD_BLOCK when the queue's byte
 * cap is reached, so the bridge retains and retries.
 *
 * Datagram bridge: push send_datagram() buffers one datagram;
 * the pull provide_datagram callback flushes it. Single-slot,
 * lossy — excess datagrams are DROPPED.
 */

#include "pico_wt_endpoint.h"
#include <string.h>
#include <stddef.h>

/* -- Endpoint ops --------------------------------------------------- */

static moq_transport_result_t ep_open_uni(void *ctx, uint64_t *out_id)
{
    pico_wt_endpoint_ctx_t *ep = (pico_wt_endpoint_ctx_t *)ctx;
    h3zero_stream_ctx_t *sc = picowt_create_local_stream(
        ep->cnx, 0, ep->h3_ctx, ep->control_stream_id);
    if (!sc) return MOQ_TRANSPORT_ERROR;
    sc->path_callback = ep->app_callback;
    sc->path_callback_ctx = ep->app_callback_ctx;
    *out_id = sc->stream_id;
    if (ep->on_uni_opened)
        ep->on_uni_opened(ep->cb_ctx, sc->stream_id);
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t ep_open_bidi(void *ctx, uint64_t *out_id)
{
    pico_wt_endpoint_ctx_t *ep = (pico_wt_endpoint_ctx_t *)ctx;
    h3zero_stream_ctx_t *sc = picowt_create_local_stream(
        ep->cnx, 1, ep->h3_ctx, ep->control_stream_id);
    if (!sc) return MOQ_TRANSPORT_ERROR;
    sc->path_callback = ep->app_callback;
    sc->path_callback_ctx = ep->app_callback_ctx;
    *out_id = sc->stream_id;
    if (ep->on_bidi_opened)
        ep->on_bidi_opened(ep->cb_ctx, sc->stream_id);
    return MOQ_TRANSPORT_OK;
}

/* Map a queue push result to the bridge write contract. On accept, mark the
 * stream active with its h3zero stream context so h3zero routes prepare_to_send
 * back to our provide_data path. */
static moq_transport_result_t wt_push_result(pico_wt_endpoint_ctx_t *ep,
                                             uint64_t stream_id, int r)
{
    if (r < 0) return MOQ_TRANSPORT_ERROR;
    if (r == 0) return MOQ_TRANSPORT_WOULD_BLOCK;
    h3zero_stream_ctx_t *sc = h3zero_find_stream(ep->h3_ctx, stream_id);
    if (picoquic_mark_active_stream(ep->cnx, stream_id, 1, sc) != 0)
        return MOQ_TRANSPORT_ERROR;
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t ep_write(void *ctx, uint64_t stream_id,
                                        const uint8_t *data, size_t len,
                                        bool fin)
{
    pico_wt_endpoint_ctx_t *ep = (pico_wt_endpoint_ctx_t *)ctx;
    return wt_push_result(ep, stream_id,
        moq_pq_send_queue_push_copy(ep->queue, stream_id, data, len, fin));
}

static moq_transport_result_t ep_write_payload(void *ctx, uint64_t stream_id,
                                                moq_rcbuf_t *buf, bool fin)
{
    pico_wt_endpoint_ctx_t *ep = (pico_wt_endpoint_ctx_t *)ctx;
    return wt_push_result(ep, stream_id,
        moq_pq_send_queue_push_rcbuf(ep->queue, stream_id, buf, fin));
}

static moq_transport_result_t ep_reset(void *ctx, uint64_t stream_id,
                                        uint64_t error_code)
{
    pico_wt_endpoint_ctx_t *ep = (pico_wt_endpoint_ctx_t *)ctx;
    /* Abandon anything still queued and clear the active mark before resetting,
     * so no provide_data lands for a reset stream. */
    moq_pq_send_queue_drop(ep->queue, stream_id);
    picoquic_mark_active_stream(ep->cnx, stream_id, 0, NULL);
    h3zero_stream_ctx_t *sc = h3zero_find_stream(ep->h3_ctx, stream_id);
    if (!sc) {
        /* Stream unknown to h3zero — try raw QUIC reset as fallback */
        int rc = picoquic_reset_stream(ep->cnx, stream_id, error_code);
        return rc == 0 ? MOQ_TRANSPORT_OK : MOQ_TRANSPORT_ERROR;
    }
    int rc = picowt_reset_stream(ep->cnx, sc, error_code);
    return rc == 0 ? MOQ_TRANSPORT_OK : MOQ_TRANSPORT_ERROR;
}

int pico_wt_endpoint_on_provide_data(pico_wt_endpoint_ctx_t *ep,
                                     uint64_t stream_id,
                                     void *provide_ctx, size_t space)
{
    size_t nb = 0; bool is_fin = false, still = false;
    if (!moq_pq_send_queue_plan(ep->queue, stream_id, space,
                                &nb, &is_fin, &still)) {
        (void)picoquic_provide_stream_data_buffer(provide_ctx, 0, 0, 0);
        return 0;
    }
    uint8_t *dst = picoquic_provide_stream_data_buffer(
        provide_ctx, nb, is_fin ? 1 : 0, still ? 1 : 0);
    if (dst)
        moq_pq_send_queue_commit(ep->queue, stream_id, dst, nb);
    return 0;
}

static moq_transport_result_t ep_stop_sending(void *ctx,
                                               uint64_t stream_id,
                                               uint64_t error_code)
{
    pico_wt_endpoint_ctx_t *ep = (pico_wt_endpoint_ctx_t *)ctx;
    int rc = picoquic_stop_sending(ep->cnx, stream_id, error_code);
    return rc == 0 ? MOQ_TRANSPORT_OK : MOQ_TRANSPORT_ERROR;
}

static moq_transport_result_t ep_send_datagram(void *ctx,
                                                const uint8_t *data,
                                                size_t len)
{
    pico_wt_endpoint_ctx_t *ep = (pico_wt_endpoint_ctx_t *)ctx;

    /* Single-slot buffer — drop if occupied */
    if (ep->pending_dg_len > 0)
        return MOQ_TRANSPORT_DROPPED;

    if (len > sizeof(ep->pending_dg_buf))
        return MOQ_TRANSPORT_TOO_LARGE;

    if (data && len > 0)
        memcpy(ep->pending_dg_buf, data, len);
    ep->pending_dg_len = len;

    int rc = h3zero_set_datagram_ready(ep->cnx, ep->control_stream_id);
    if (rc != 0) {
        ep->pending_dg_len = 0;
        return MOQ_TRANSPORT_DROPPED;
    }
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t ep_close(void *ctx, uint64_t code,
                                        const uint8_t *reason,
                                        size_t reason_len)
{
    pico_wt_endpoint_ctx_t *ep = (pico_wt_endpoint_ctx_t *)ctx;

    uint32_t code32 = (code > UINT32_MAX) ? UINT32_MAX
                                          : (uint32_t)code;

    char msg[256];
    size_t copy = reason_len < sizeof(msg) - 1
                  ? reason_len : sizeof(msg) - 1;
    if (reason && copy > 0) {
        memcpy(msg, reason, copy);
        msg[copy] = '\0';
    } else {
        msg[0] = '\0';
    }

    int rc = picowt_send_close_session_message(
        ep->cnx, ep->control_stream_ctx,
        code32, msg[0] ? msg : NULL);
    return rc == 0 ? MOQ_TRANSPORT_OK : MOQ_TRANSPORT_ERROR;
}

/* -- Init / cleanup ------------------------------------------------- */

int pico_wt_endpoint_init(moq_transport_endpoint_ops_t *ops,
                           pico_wt_endpoint_ctx_t *ctx,
                           picoquic_cnx_t *cnx,
                           h3zero_callback_ctx_t *h3_ctx,
                           h3zero_stream_ctx_t *control_stream_ctx,
                           const moq_alloc_t *alloc)
{
    memset(ctx, 0, sizeof(*ctx));
    ctx->cnx = cnx;
    ctx->h3_ctx = h3_ctx;
    ctx->control_stream_ctx = control_stream_ctx;
    ctx->control_stream_id = control_stream_ctx->stream_id;
    ctx->pending_dg_len = 0;

    ctx->queue = moq_pq_send_queue_create(alloc, 0);
    if (!ctx->queue) return -1;

    *ops = (moq_transport_endpoint_ops_t){
        .struct_size     = sizeof(moq_transport_endpoint_ops_t),
        .capabilities    = MOQ_TRANSPORT_CAP_DATAGRAM |
                           MOQ_TRANSPORT_CAP_WRITE_PAYLOAD,
        .open_uni        = ep_open_uni,
        .open_bidi       = ep_open_bidi,
        .write           = ep_write,
        .write_payload   = ep_write_payload,
        .reset_stream    = ep_reset,
        .stop_sending    = ep_stop_sending,
        .send_datagram   = ep_send_datagram,
        .max_datagram_size = NULL,
        .close_transport = ep_close,
    };
    return 0;
}

void pico_wt_endpoint_cleanup(pico_wt_endpoint_ctx_t *ctx)
{
    if (!ctx) return;
    ctx->pending_dg_len = 0;
    moq_pq_send_queue_destroy(ctx->queue);
    ctx->queue = NULL;
}

int pico_wt_endpoint_provide_datagram(pico_wt_endpoint_ctx_t *ctx,
                                       uint8_t *context,
                                       size_t space)
{
    if (ctx->pending_dg_len == 0)
        return -1;

    if (ctx->pending_dg_len > space) {
        ctx->pending_dg_len = 0;
        return -1;
    }

    uint8_t *buf = h3zero_provide_datagram_buffer(
        context, ctx->pending_dg_len, 0);
    if (!buf) {
        ctx->pending_dg_len = 0;
        return -1;
    }

    memcpy(buf, ctx->pending_dg_buf, ctx->pending_dg_len);
    ctx->pending_dg_len = 0;
    return 0;
}
