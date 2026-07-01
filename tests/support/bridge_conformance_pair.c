/*
 * Bridge conformance pair implementation.
 *
 * Two moq_transport_bridge_t instances with fake endpoints, connected
 * by routing endpoint operations across the pair. Implements the
 * moq_adapter_pair_ops_t vtable for conformance testing.
 */

#include "bridge_conformance_pair.h"
#include "fake_endpoint.h"
#include <moq/moq.h>
#include <moq/transport_bridge.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ------------------------------------------------------------------ */
/* Pair context                                                        */
/* ------------------------------------------------------------------ */

typedef struct {
    moq_session_t          *client_session;
    moq_session_t          *server_session;
    moq_transport_bridge_t *client_bridge;
    moq_transport_bridge_t *server_bridge;
    fake_endpoint_t         client_ep;
    fake_endpoint_t         server_ep;
    uint64_t                now;
    uint64_t                client_control_sid;
    uint64_t                server_control_sid;
    bool                    error;
    char                    last_error[256];
} bridge_pair_ctx_t;

/* ------------------------------------------------------------------ */
/* Deliver endpoint ops from one side to the other                     */
/* ------------------------------------------------------------------ */

static bool is_bidi_stream(uint64_t id)
{
    /* Client bidi IDs start at 2000, server bidi at 4000.
     * Uni IDs: client 1000-1999, server 3000-3999.
     * A write on any bidi ID (from either side) is bidi. */
    return (id >= 2000 && id < 3000) || (id >= 4000 && id < 5000);
}

static bool deliver_ops(bridge_pair_ctx_t *ctx,
                         fake_endpoint_t *from_ep,
                         moq_transport_bridge_t *to_bridge,
                         bool from_is_client)
{
    bool delivered = false;

    for (size_t i = 0; i < from_ep->count; i++) {
        fake_op_t *o = &from_ep->ops[i];

        switch (o->kind) {
        case FAKE_OP_OPEN_UNI:
        case FAKE_OP_OPEN_BIDI:
            delivered = true;
            break;

        case FAKE_OP_WRITE: {
            moq_result_t rc;
            if (is_bidi_stream(o->stream_id)) {
                if (from_is_client) {
                    if (ctx->client_control_sid == UINT64_MAX)
                        ctx->client_control_sid = o->stream_id;
                    if (o->stream_id == ctx->client_control_sid) {
                        rc = moq_transport_bridge_on_peer_control_bytes(
                            to_bridge, o->stream_id,
                            o->data, o->data_len, o->fin, ctx->now);
                    } else {
                        rc = moq_transport_bridge_on_peer_bidi_bytes(
                            to_bridge, o->stream_id,
                            o->data, o->data_len, o->fin, ctx->now);
                    }
                } else {
                    if (ctx->server_control_sid == UINT64_MAX)
                        ctx->server_control_sid = o->stream_id;
                    if (o->stream_id == ctx->server_control_sid ||
                        o->stream_id == ctx->client_control_sid) {
                        rc = moq_transport_bridge_on_peer_control_bytes(
                            to_bridge, o->stream_id,
                            o->data, o->data_len, o->fin, ctx->now);
                    } else {
                        rc = moq_transport_bridge_on_peer_bidi_bytes(
                            to_bridge, o->stream_id,
                            o->data, o->data_len, o->fin, ctx->now);
                    }
                }
            } else {
                /* Uni stream */
                rc = moq_transport_bridge_on_peer_uni_bytes(
                    to_bridge, o->stream_id,
                    o->data, o->data_len, o->fin, ctx->now);
            }
            if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK &&
                rc != MOQ_ERR_CLOSED) {
                ctx->error = true;
                snprintf(ctx->last_error, sizeof(ctx->last_error),
                         "deliver write failed: %d", (int)rc);
            }
            delivered = true;
            break;
        }

        case FAKE_OP_RESET:
            moq_transport_bridge_on_peer_stream_reset(
                to_bridge, o->stream_id, o->error_code, ctx->now);
            delivered = true;
            break;

        case FAKE_OP_STOP:
            moq_transport_bridge_on_peer_stop_sending(
                to_bridge, o->stream_id, o->error_code, ctx->now);
            delivered = true;
            break;

        case FAKE_OP_DATAGRAM:
            moq_transport_bridge_on_peer_datagram(
                to_bridge, o->data, o->data_len, ctx->now);
            delivered = true;
            break;

        case FAKE_OP_CLOSE:
            moq_transport_bridge_on_transport_close(
                to_bridge, o->error_code, ctx->now);
            delivered = true;
            break;
        }
    }

    fake_endpoint_clear_ops(from_ep);
    return delivered;
}

/* ------------------------------------------------------------------ */
/* Adapter pair ops implementation                                     */
/* ------------------------------------------------------------------ */

static moq_session_t *bp_client_session(void *ctx)
{
    return ((bridge_pair_ctx_t *)ctx)->client_session;
}

static moq_session_t *bp_server_session(void *ctx)
{
    return ((bridge_pair_ctx_t *)ctx)->server_session;
}

static uint64_t bp_now_us(void *ctx)
{
    return ((bridge_pair_ctx_t *)ctx)->now;
}

static void bp_advance_to(void *ctx, uint64_t now_us)
{
    ((bridge_pair_ctx_t *)ctx)->now = now_us;
}

static uint64_t bp_next_deadline_us(void *ctx)
{
    bridge_pair_ctx_t *bp = (bridge_pair_ctx_t *)ctx;
    uint64_t cd = moq_session_next_deadline_us(bp->client_session);
    uint64_t sd = moq_session_next_deadline_us(bp->server_session);
    return cd < sd ? cd : sd;
}

static moq_adapter_pair_pump_result_t bp_pump_once(void *ctx,
                                                    uint64_t now_us)
{
    bridge_pair_ctx_t *bp = (bridge_pair_ctx_t *)ctx;
    bp->now = now_us;
    bool progress = false;

    moq_transport_bridge_service(bp->client_bridge, now_us);
    progress |= deliver_ops(bp, &bp->client_ep, bp->server_bridge, true);

    moq_transport_bridge_service(bp->server_bridge, now_us);
    progress |= deliver_ops(bp, &bp->server_ep, bp->client_bridge, false);

    if (moq_transport_bridge_is_fatal(bp->client_bridge) ||
        moq_transport_bridge_is_fatal(bp->server_bridge)) {
        bp->error = true;
        return MOQ_ADAPTER_PAIR_ERROR;
    }

    return progress ? MOQ_ADAPTER_PAIR_PROGRESS
                    : MOQ_ADAPTER_PAIR_QUIESCENT;
}

static moq_adapter_pair_pump_result_t bp_pump_until_quiescent(void *ctx,
                                                               int max_steps)
{
    bridge_pair_ctx_t *bp = (bridge_pair_ctx_t *)ctx;
    for (int i = 0; i < max_steps; i++) {
        moq_adapter_pair_pump_result_t r = bp_pump_once(ctx, bp->now);
        if (r == MOQ_ADAPTER_PAIR_ERROR) return r;
        if (r == MOQ_ADAPTER_PAIR_QUIESCENT) return r;
    }
    return MOQ_ADAPTER_PAIR_MAX_STEPS;
}

static bool bp_has_error(void *ctx)
{
    bridge_pair_ctx_t *bp = (bridge_pair_ctx_t *)ctx;
    return bp->error ||
           moq_transport_bridge_is_fatal(bp->client_bridge) ||
           moq_transport_bridge_is_fatal(bp->server_bridge);
}

static bool bp_is_closed(void *ctx, moq_adapter_pair_side_t side)
{
    bridge_pair_ctx_t *bp = (bridge_pair_ctx_t *)ctx;
    return side == MOQ_ADAPTER_PAIR_CLIENT
        ? moq_transport_bridge_is_closed(bp->client_bridge)
        : moq_transport_bridge_is_closed(bp->server_bridge);
}

static uint64_t bp_close_code(void *ctx, moq_adapter_pair_side_t side)
{
    (void)ctx; (void)side;
    return 0;
}

static bool bp_has_fatal(void *ctx, moq_adapter_pair_side_t side)
{
    bridge_pair_ctx_t *bp = (bridge_pair_ctx_t *)ctx;
    return side == MOQ_ADAPTER_PAIR_CLIENT
        ? moq_transport_bridge_is_fatal(bp->client_bridge)
        : moq_transport_bridge_is_fatal(bp->server_bridge);
}

static uint64_t bp_fatal_code(void *ctx, moq_adapter_pair_side_t side)
{
    bridge_pair_ctx_t *bp = (bridge_pair_ctx_t *)ctx;
    return side == MOQ_ADAPTER_PAIR_CLIENT
        ? moq_transport_bridge_fatal_code(bp->client_bridge)
        : moq_transport_bridge_fatal_code(bp->server_bridge);
}

static const char *bp_last_error(void *ctx)
{
    bridge_pair_ctx_t *bp = (bridge_pair_ctx_t *)ctx;
    return bp->last_error[0] ? bp->last_error : NULL;
}

static void bp_block_writes(void *ctx, moq_adapter_pair_side_t side,
                             bool block)
{
    bridge_pair_ctx_t *bp = (bridge_pair_ctx_t *)ctx;
    if (side == MOQ_ADAPTER_PAIR_CLIENT)
        bp->client_ep.block_write = block;
    else
        bp->server_ep.block_write = block;
}

static void bp_drop_datagrams(void *ctx, moq_adapter_pair_side_t side,
                               bool drop)
{
    bridge_pair_ctx_t *bp = (bridge_pair_ctx_t *)ctx;
    if (side == MOQ_ADAPTER_PAIR_CLIENT)
        bp->client_ep.drop_datagram = drop;
    else
        bp->server_ep.drop_datagram = drop;
}

static size_t bp_stream_count(void *ctx, moq_adapter_pair_side_t side)
{
    bridge_pair_ctx_t *bp = (bridge_pair_ctx_t *)ctx;
    return side == MOQ_ADAPTER_PAIR_CLIENT
        ? moq_transport_bridge_stream_count(bp->client_bridge)
        : moq_transport_bridge_stream_count(bp->server_bridge);
}

static size_t bp_tombstone_count(void *ctx, moq_adapter_pair_side_t side)
{
    bridge_pair_ctx_t *bp = (bridge_pair_ctx_t *)ctx;
    return side == MOQ_ADAPTER_PAIR_CLIENT
        ? moq_transport_bridge_tombstone_count(bp->client_bridge)
        : moq_transport_bridge_tombstone_count(bp->server_bridge);
}

static size_t bp_opened_bidi_count(void *ctx, moq_adapter_pair_side_t side)
{
    bridge_pair_ctx_t *bp = (bridge_pair_ctx_t *)ctx;
    fake_endpoint_t *ep = (side == MOQ_ADAPTER_PAIR_CLIENT)
        ? &bp->client_ep : &bp->server_ep;
    /* Count = next_bidi_id - base */
    uint64_t base = (side == MOQ_ADAPTER_PAIR_CLIENT) ? 2000 : 4000;
    return (size_t)(ep->next_bidi_id - base);
}

static int bp_inject_bidi_fin(void *ctx, moq_adapter_pair_side_t from_side)
{
    bridge_pair_ctx_t *bp = (bridge_pair_ctx_t *)ctx;
    fake_endpoint_t *from_ep = (from_side == MOQ_ADAPTER_PAIR_CLIENT)
        ? &bp->client_ep : &bp->server_ep;
    moq_transport_bridge_t *to_bridge = (from_side == MOQ_ADAPTER_PAIR_CLIENT)
        ? bp->server_bridge : bp->client_bridge;

    /* Find the last opened bidi stream that isn't the control stream */
    uint64_t base = (from_side == MOQ_ADAPTER_PAIR_CLIENT) ? 2000 : 4000;
    if (from_ep->next_bidi_id <= base) return -1;
    uint64_t last_bidi = from_ep->next_bidi_id - 1;

    /* Skip the control stream */
    uint64_t ctrl = (from_side == MOQ_ADAPTER_PAIR_CLIENT)
        ? bp->client_control_sid : bp->server_control_sid;
    if (last_bidi == ctrl && last_bidi > base)
        last_bidi--;
    if (last_bidi < base || last_bidi == ctrl) return -1;

    moq_result_t rc = moq_transport_bridge_on_peer_bidi_bytes(
        to_bridge, last_bidi, NULL, 0, true, bp->now);
    return (rc >= 0 || rc == MOQ_ERR_WOULD_BLOCK) ? 0 : -1;
}

static void bp_destroy(void *ctx)
{
    bridge_pair_ctx_t *bp = (bridge_pair_ctx_t *)ctx;
    moq_transport_bridge_destroy(bp->client_bridge);
    moq_transport_bridge_destroy(bp->server_bridge);
    moq_session_destroy(bp->client_session);
    moq_session_destroy(bp->server_session);
    free(bp);
}

/* ------------------------------------------------------------------ */
/* Public create function                                              */
/* ------------------------------------------------------------------ */

static const moq_adapter_pair_ops_t bridge_pair_ops = {
    .client_session      = bp_client_session,
    .server_session      = bp_server_session,
    .now_us              = bp_now_us,
    .advance_to          = bp_advance_to,
    .next_deadline_us    = bp_next_deadline_us,
    .pump_once           = bp_pump_once,
    .pump_until_quiescent = bp_pump_until_quiescent,
    .has_error           = bp_has_error,
    .is_closed           = bp_is_closed,
    .close_code          = bp_close_code,
    .has_fatal           = bp_has_fatal,
    .fatal_code          = bp_fatal_code,
    .last_error          = bp_last_error,
    .block_writes        = bp_block_writes,
    .drop_datagrams      = bp_drop_datagrams,
    .stream_count        = bp_stream_count,
    .tombstone_count     = bp_tombstone_count,
    .opened_bidi_count   = bp_opened_bidi_count,
    .inject_bidi_fin     = bp_inject_bidi_fin,
    .destroy             = bp_destroy,
};

moq_adapter_pair_t bridge_conformance_create(void)
{
    moq_adapter_pair_t pair = {0};

    bridge_pair_ctx_t *bp = (bridge_pair_ctx_t *)calloc(1, sizeof(*bp));
    if (!bp) return pair;

    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 64;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 64;
    scfg.goaway_timeout_us = 1000;

    if (moq_session_create(&ccfg, 0, &bp->client_session) < 0) goto fail;
    if (moq_session_create(&scfg, 0, &bp->server_session) < 0) goto fail;

    bp->client_control_sid = UINT64_MAX;
    bp->server_control_sid = UINT64_MAX;

    fake_endpoint_init(&bp->client_ep, 1000, 2000);
    fake_endpoint_init(&bp->server_ep, 3000, 4000);

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());

    if (moq_transport_bridge_create(&bcfg, bp->client_session,
            &bp->client_ep.vtable, &bp->client_ep,
            &bp->client_bridge) < 0)
        goto fail;

    if (moq_transport_bridge_create(&bcfg, bp->server_session,
            &bp->server_ep.vtable, &bp->server_ep,
            &bp->server_bridge) < 0)
        goto fail;

    pair.ops = &bridge_pair_ops;
    pair.capabilities =
        MOQ_ADAPTER_PAIR_CAP_BLOCK_WRITES |
        MOQ_ADAPTER_PAIR_CAP_DROP_DATAGRAMS |
        MOQ_ADAPTER_PAIR_CAP_STREAM_COUNTS |
        MOQ_ADAPTER_PAIR_CAP_TOMBSTONES |
        MOQ_ADAPTER_PAIR_CAP_DATAGRAMS |
        MOQ_ADAPTER_PAIR_CAP_BIDI_STREAMS |
        MOQ_ADAPTER_PAIR_CAP_VIRTUAL_TIME |
        MOQ_ADAPTER_PAIR_CAP_OPENED_BIDI_COUNT |
        MOQ_ADAPTER_PAIR_CAP_INJECT_BIDI_FIN;
    pair.ctx = bp;
    return pair;

fail:
    if (bp->client_bridge) moq_transport_bridge_destroy(bp->client_bridge);
    if (bp->server_bridge) moq_transport_bridge_destroy(bp->server_bridge);
    if (bp->client_session) moq_session_destroy(bp->client_session);
    if (bp->server_session) moq_session_destroy(bp->server_session);
    free(bp);
    return pair;
}
