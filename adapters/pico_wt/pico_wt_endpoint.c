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
 * Reliable reset: reset_stream_at keeps the MoQ stream header with
 * RESET_STREAM_AT. The reliable size on the wire is the WebTransport stream
 * preamble plus the MoQ bytes, never the MoQ bytes alone. picoquic accepts a
 * reliable size only up to what it has sent, so a prefix still queued is kept
 * (the rest dropped) and the frame is issued once picoquic has pulled it.
 *
 * Datagram bridge: push send_datagram() buffers one datagram;
 * the pull provide_datagram callback flushes it. Single-slot,
 * lossy — excess datagrams are DROPPED.
 *
 * Every h3zero lookup goes through the endpoint's own h3zero context. A few
 * h3zero helpers instead recover it from the connection's callback context,
 * which only holds when the connection callback is h3zero's own; those
 * helpers are not used here (see ep_send_datagram, and pico_wt_h3_delete_prefix
 * in pico_wt_endpoint.h).
 */

#include "pico_wt_endpoint.h"
#include <picoquic_utils.h>   /* picoquic_frames_varint_encode_length */
#include <string.h>
#include <stddef.h>

#if !defined(PICOQUIC_TERMINAL_STREAM_ALREADY_CLOSED) || PICOQUIC_TERMINAL_STREAM_ALREADY_CLOSED != 1
#error picoquic_must_classify_terminal_stream_ops_apply_libmoq_picoquic_patch_0008
#endif

/* -- Endpoint ops --------------------------------------------------- */

static moq_transport_result_t ep_open_uni(void *ctx, uint64_t *out_id)
{
    pico_wt_endpoint_ctx_t *ep = (pico_wt_endpoint_ctx_t *)ctx;
    h3zero_stream_ctx_t *sc = picowt_create_local_stream(
        ep->cnx, 0, ep->h3_ctx, ep->control_stream_id);
    if (!sc) return MOQ_TRANSPORT_ERROR;
#if defined(PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION) && PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION >= 1
    if (picoquic_enable_stream_send_commit(ep->cnx, sc->stream_id) != 0) {
        h3zero_delete_stream(ep->cnx, ep->h3_ctx, sc);
        return MOQ_TRANSPORT_ERROR;
    }
#endif
    sc->path_callback = ep->app_callback;
    sc->path_callback_ctx = ep->app_callback_ctx;
    if (moq_pq_send_queue_opened(ep->queue, sc->stream_id) != 0) {
        h3zero_delete_stream(ep->cnx, ep->h3_ctx, sc);
        return MOQ_TRANSPORT_ERROR;
    }
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
    if (moq_pq_send_queue_opened(ep->queue, sc->stream_id) != 0) {
        h3zero_delete_stream(ep->cnx, ep->h3_ctx, sc);
        return MOQ_TRANSPORT_ERROR;
    }
    *out_id = sc->stream_id;
    if (ep->on_bidi_opened)
        ep->on_bidi_opened(ep->cb_ctx, sc->stream_id);
    return MOQ_TRANSPORT_OK;
}

static int wt_apply_priority(void *ctx, uint64_t sid, uint8_t priority)
{
    pico_wt_endpoint_ctx_t *ep = ctx;
    return ep->cnx ? picoquic_set_stream_priority(ep->cnx, sid, priority) : -1;
}

static moq_transport_result_t ep_priority(void *ctx, uint64_t sid, uint32_t key)
{
    pico_wt_endpoint_ctx_t *ep = ctx;
    return moq_pq_send_queue_set_key(ep->queue, sid, key) == 0
        ? MOQ_TRANSPORT_OK : MOQ_TRANSPORT_ERROR;
}

int pico_wt_endpoint_apply_priorities(pico_wt_endpoint_ctx_t *ctx)
{
    if (!ctx->cnx) return 0;
    return moq_pq_send_queue_apply_priorities(ctx->queue, wt_apply_priority,
                                              ctx);
}

/* Map a queue push result to the bridge write contract: a refusal is the
 * queue's one cap for the connection (MOQ_TRANSPORT_CAP_SHARED_ADMISSION). On
 * accept, mark the stream active with its h3zero stream context so h3zero
 * routes prepare_to_send back to our provide_data path. */
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

#if defined(MOQ_PICO_WT_TESTING)
int (*pw_test_reset_stream_at)(picoquic_cnx_t *, uint64_t, uint64_t,
                               uint64_t) = NULL;
#define PW_RESET_STREAM_AT(cnx, sid, err, rs)                               \
    (pw_test_reset_stream_at ? pw_test_reset_stream_at(cnx, sid, err, rs)  \
                             : picoquic_reset_stream_at(cnx, sid, err, rs))
#else
#define PW_RESET_STREAM_AT(cnx, sid, err, rs)                               \
    picoquic_reset_stream_at(cnx, sid, err, rs)
#endif

/* picoquic_stop_sending and an accepted reset clear the stream's QUIC
 * application context, and so does marking it inactive. That context is
 * h3zero's own record of the stream, and picoquic reports the stream's
 * retirement (which h3zero forwards as picohttp_callback_free and uses to
 * delete the record) only while it is set. Put h3zero's record back for a
 * stream h3zero still holds; nonzero when picoquic refuses it. */
static int wt_restore_h3_ctx(pico_wt_endpoint_ctx_t *ep, uint64_t sid)
{
    h3zero_stream_ctx_t *sc = h3zero_find_stream(ep->h3_ctx, sid);
    if (sc == NULL) return 0;
    return picoquic_set_app_stream_ctx(ep->cnx, sid, sc) == 0 ? 0 : -1;
}

static void wt_retire_local_uni(pico_wt_endpoint_ctx_t *ep, uint64_t sid, int rc)
{
#if defined(PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION) && PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION >= 1
    if (rc == 0 && (sid & 2u) &&
        ((sid ^ (unsigned)picoquic_is_client(ep->cnx)) & 1u)) {
        h3zero_stream_ctx_t *sc = h3zero_find_stream(ep->h3_ctx, sid);
        if (sc) h3zero_delete_stream(ep->cnx, ep->h3_ctx, sc);
    }
#else
    (void)ep; (void)sid; (void)rc;
#endif
}

/* After a reset picoquic accepted or deferred: abandon what is still queued
 * for the stream and clear its active mark, so no provide_data lands for it.
 * The stream exists at picoquic, so the mark creates nothing. */
static void wt_quiesce(pico_wt_endpoint_ctx_t *ep, uint64_t sid)
{
    moq_pq_send_queue_drop(ep->queue, sid);
    (void)picoquic_mark_active_stream(ep->cnx, sid, 0, NULL);
}

static moq_transport_result_t ep_reset(void *ctx, uint64_t stream_id,
                                        uint64_t error_code)
{
    pico_wt_endpoint_ctx_t *ep = (pico_wt_endpoint_ctx_t *)ctx;
    if (!ep->cnx || !ep->h3_ctx) return MOQ_TRANSPORT_ERROR;
    /* MoQ code -> WebTransport application-error space for the wire. Reject
     * an out-of-range code BEFORE mutating any stream state, so a rejected
     * reset leaves the queue/active-mark untouched. */
    uint64_t wt_code;
    if (!pico_wt_moq_err_to_wt(error_code, &wt_code))
        return MOQ_TRANSPORT_ERROR;
    /* picoquic classifies the stream before anything here touches it: an
     * active mark could create a stream that was never opened. Only once a
     * reset is accepted (or deferred) are the stream's queued bytes abandoned
     * and its active mark cleared, so no provide_data lands for it. A sending
     * part that has already ended (PICOQUIC_ERROR_STREAM_ALREADY_CLOSED) needs
     * no reset: the call is a no-op and nothing about the stream changes. */
    h3zero_stream_ctx_t *sc = h3zero_find_stream(ep->h3_ctx, stream_id);
    if (!sc) {
        /* Stream unknown to h3zero -- try raw QUIC reset as fallback */
        int rc = picoquic_reset_stream(ep->cnx, stream_id, wt_code);
        if (rc == PICOQUIC_ERROR_STREAM_ALREADY_CLOSED) return MOQ_TRANSPORT_OK;
        if (rc != 0) return MOQ_TRANSPORT_ERROR;
        ep->last_reset_reliable_size = 0;
        wt_quiesce(ep, stream_id);
        return MOQ_TRANSPORT_OK;
    }

    /* Capability-aware reset. picowt_reset_stream() resets a local stream with
     * a reliable_size preamble, i.e. picoquic_reset_stream_at() with
     * reliable_size > 0. picoquic only permits that when the reliable-reset
     * extension was genuinely NEGOTIATED: cnx->is_reset_stream_at_enabled is
     * latched once as (local && remote) while parsing the peer's transport
     * parameters, and picoquic_reset_stream_at() rejects a nonzero
     * reliable_size with PICOQUIC_ERROR_ILLEGAL_TRANSPORT_EXTENSION otherwise
     * -- LOCALLY, before any frame is queued. That local error surfaces as a
     * fatal bridge failure and closes the connection (the draft-18 announce
     * withdrawal did exactly this against a relay without the extension).
     *
     * The managed client SYNTHESIZES the peer's missing TP to unblock the WT
     * CONNECT, but that mutates remote_parameters AFTER the negotiated bit is
     * latched, so it never makes reliable-reset usable. A synthesized-
     * capability peer must therefore get a plain RESET_STREAM (reliable_size
     * 0). The live remote TP plus the synthesized flag reconstruct genuine
     * negotiation without reaching into picoquic's internal cnx state. */
    const picoquic_tp_t *rtp = picoquic_get_transport_parameters(ep->cnx, 0);
    bool genuine_at = rtp && rtp->is_reset_stream_at_enabled &&
                      !ep->reset_stream_at_synthesized;
    if (genuine_at) {
        /* picowt uses reliable_size = 2 + varint(control_stream_id) for a local
         * stream; mirror it as the test witness. */
        size_t preamble =
            2 + picoquic_frames_varint_encode_length(ep->control_stream_id);
        int fin_sent = sc->ps.stream_state.is_fin_sent;
        int rc = picowt_reset_stream(ep->cnx, sc, wt_code);
        if (rc == 0) {
            ep->last_reset_reliable_size = preamble;
            wt_quiesce(ep, stream_id);
            wt_retire_local_uni(ep, stream_id, rc);
            return wt_restore_h3_ctx(ep, stream_id) == 0 ? MOQ_TRANSPORT_OK
                                                          : MOQ_TRANSPORT_ERROR;
        }
        /* picowt_reset_stream marks the send side finished even when picoquic
         * refuses; a refusal changed nothing, so the flag is restored. */
        sc->ps.stream_state.is_fin_sent = fin_sent;
        if (rc == PICOQUIC_ERROR_STREAM_ALREADY_CLOSED)
            return MOQ_TRANSPORT_OK;
        /* picoquic takes a Reliable Size only up to the bytes it has sent,
         * and a stream reset right after it opened may still hold its
         * preamble in picoquic's queue. WebTransport requires RESET_STREAM_AT
         * to cover the preamble (draft-ietf-webtrans-http3 Section 4.4), so
         * the reset waits: the queued preamble goes out on its own, and
         * pico_wt_endpoint_flush_resets issues the reset once it has. The
         * refusal changed nothing in picoquic; the stream's queued MoQ bytes
         * are dropped and it is no longer active. (Keeping the send side
         * unfinished matters: a session teardown must not skip this stream's
         * WT_SESSION_GONE reset.) */
        if (rc != PICOQUIC_ERROR_OFFSET_TOO_BIG)
            return MOQ_TRANSPORT_ERROR;
        /* At the bound, the bridge keeps the reset in its own bounded pending
         * queue and retries it; by then the preamble may be out. Nothing was
         * accepted here, so leave its bytes, context and bookkeeping intact. */
        if (ep->deferred_count >= PICO_WT_DEFERRED_RESETS_MAX)
            return MOQ_TRANSPORT_WOULD_BLOCK;
        ep->deferred[ep->deferred_count++] = (pico_wt_deferred_reset_t){
            .sid = sc->stream_id, .wt_code = wt_code,
            .reliable_size = preamble };
        ep->last_reset_reliable_size = preamble;
        wt_quiesce(ep, stream_id);
        return wt_restore_h3_ctx(ep, stream_id) == 0 ? MOQ_TRANSPORT_OK
                                                     : MOQ_TRANSPORT_ERROR;
    }

    /* Plain RESET_STREAM, preserving picowt_reset_stream's bookkeeping: the
     * same local/bidirectional eligibility (a remote unidirectional stream has
     * no local send half to reset) and is_fin_sent marking, but reliable_size 0
     * so picoquic emits RESET_STREAM and never trips the guard above. */
    unsigned client_mode = (unsigned)(picoquic_is_client(ep->cnx) != 0);
    bool is_bidir = (sc->stream_id & 2u) == 0;
    bool is_local = ((sc->stream_id ^ client_mode) & 1u) != 0;
    if (!is_local && !is_bidir)
        return MOQ_TRANSPORT_ERROR;
    int rc = picoquic_reset_stream(ep->cnx, sc->stream_id, wt_code);
    if (rc == PICOQUIC_ERROR_STREAM_ALREADY_CLOSED) return MOQ_TRANSPORT_OK;
    if (rc != 0) return MOQ_TRANSPORT_ERROR;
    ep->last_reset_reliable_size = 0;
    sc->ps.stream_state.is_fin_sent = 1;
    wt_quiesce(ep, stream_id);
    wt_retire_local_uni(ep, stream_id, rc);
    return wt_restore_h3_ctx(ep, stream_id) == 0 ? MOQ_TRANSPORT_OK
                                                  : MOQ_TRANSPORT_ERROR;
}

/* The WebTransport preamble picowt writes on a local stream (stream type
 * plus session id): the part of every reset picowt itself keeps. */
static uint64_t wt_preamble_len(const pico_wt_endpoint_ctx_t *ep)
{
    return 2 + picoquic_frames_varint_encode_length(ep->control_stream_id);
}

/* RESET_STREAM_AT is usable: advertised locally, and genuinely by the peer
 * (a synthesized peer parameter does not make it negotiated, see ep_reset). */
static bool wt_reset_at_negotiated(const pico_wt_endpoint_ctx_t *ep)
{
    const picoquic_tp_t *ltp = picoquic_get_transport_parameters(ep->cnx, 1);
    const picoquic_tp_t *rtp = picoquic_get_transport_parameters(ep->cnx, 0);
    return ltp && rtp && ltp->is_reset_stream_at_enabled &&
           rtp->is_reset_stream_at_enabled && !ep->reset_stream_at_synthesized;
}

/* The prefix has been sent: reset keeping it. Should picoquic refuse, the
 * stream still ends, with a plain RESET_STREAM. */
static int wt_issue_reset_at(pico_wt_endpoint_ctx_t *ep, uint64_t sid,
                             uint64_t wt_code, uint64_t reliable_size)
{
    int rc = picoquic_reset_stream_at(ep->cnx, sid, wt_code, reliable_size);
    if (rc == 0) {
        ep->last_reset_reliable_size = (size_t)reliable_size;
        ep->reliable_resets++;
    } else if (rc != PICOQUIC_ERROR_STREAM_ALREADY_CLOSED) {
        rc = picoquic_reset_stream(ep->cnx, sid, wt_code);
        if (rc == 0) ep->last_reset_reliable_size = 0;
    }
    if (rc == PICOQUIC_ERROR_STREAM_ALREADY_CLOSED) return 0;   /* ended */
    if (rc != 0) return rc;
    h3zero_stream_ctx_t *sc = h3zero_find_stream(ep->h3_ctx, sid);
    if (sc) sc->ps.stream_state.is_fin_sent = 1;
    wt_quiesce(ep, sid);
    wt_retire_local_uni(ep, sid, 0);
    return wt_restore_h3_ctx(ep, sid);
}

static moq_transport_result_t ep_reset_at(void *ctx, uint64_t stream_id,
                                          uint64_t error_code,
                                          uint64_t reliable_size)
{
    pico_wt_endpoint_ctx_t *ep = (pico_wt_endpoint_ctx_t *)ctx;
    uint64_t wt_code;
    if (!pico_wt_moq_err_to_wt(error_code, &wt_code))
        return MOQ_TRANSPORT_ERROR;
    if (!ep->cnx || !wt_reset_at_negotiated(ep))
        return MOQ_TRANSPORT_DROPPED;
    /* Only a local WebTransport stream carries the preamble this size
     * extends; anything else takes the plain path. */
    h3zero_stream_ctx_t *sc = h3zero_find_stream(ep->h3_ctx, stream_id);
    unsigned client_mode = (unsigned)(picoquic_is_client(ep->cnx) != 0);
    if (!sc || ((stream_id ^ client_mode) & 1u) == 0)
        return MOQ_TRANSPORT_DROPPED;
    uint64_t total = wt_preamble_len(ep) + reliable_size;
    if (moq_pq_send_queue_truncate(ep->queue, stream_id, reliable_size) == 0)
        return wt_issue_reset_at(ep, stream_id, wt_code, total) == 0
            ? MOQ_TRANSPORT_OK : MOQ_TRANSPORT_ERROR;
    if (ep->deferred_at_count == ep->deferred_at_cap) {
        size_t ncap = ep->deferred_at_cap ? ep->deferred_at_cap * 2 : 4;
        pico_wt_deferred_reset_t *n = (pico_wt_deferred_reset_t *)ep->alloc.realloc(
            ep->deferred_at, ep->deferred_at_cap * sizeof(*n), ncap * sizeof(*n),
            ep->alloc.ctx);
        if (!n) return MOQ_TRANSPORT_ERROR;
        ep->deferred_at = n;
        ep->deferred_at_cap = ncap;
    }
    ep->deferred_at[ep->deferred_at_count++] = (pico_wt_deferred_reset_t){
        .sid = stream_id, .wt_code = wt_code, .reliable_size = total };
    return MOQ_TRANSPORT_OK;
}

int pico_wt_endpoint_flush_resets(pico_wt_endpoint_ctx_t *ep)
{
    if (!ep || !ep->cnx) return 0;

    /* MoQ reliable resets: issued once the stream's queued prefix has been
     * handed to picoquic. */
    int failed = 0;
    size_t w = 0;
    for (size_t r = 0; r < ep->deferred_at_count; r++) {
        pico_wt_deferred_reset_t d = ep->deferred_at[r];
        if (moq_pq_send_queue_has_data(ep->queue, d.sid)) {
            ep->deferred_at[w++] = d;
            continue;
        }
        /* Refused both as RESET_STREAM_AT and as a plain reset: the stream
         * cannot be ended. */
        if (wt_issue_reset_at(ep, d.sid, d.wt_code, d.reliable_size) != 0)
            failed = 1;
    }
    ep->deferred_at_count = w;

    /* Plain stream resets: issued once picoquic has sent the preamble. */
    w = 0;
    for (size_t r = 0; r < ep->deferred_count; r++) {
        pico_wt_deferred_reset_t d = ep->deferred[r];
        int rc = PW_RESET_STREAM_AT(ep->cnx, d.sid, d.wt_code, d.reliable_size);
        if (rc == PICOQUIC_ERROR_OFFSET_TOO_BIG) {
            ep->deferred[w++] = d;            /* preamble still unsent */
        } else if (rc == 0) {
            /* Issued: the send side is finished, as picowt_reset_stream
             * records it. */
            h3zero_stream_ctx_t *sc = h3zero_find_stream(ep->h3_ctx, d.sid);
            if (sc) sc->ps.stream_state.is_fin_sent = 1;
            wt_retire_local_uni(ep, d.sid, rc);
            if (wt_restore_h3_ctx(ep, d.sid) != 0) failed = 1;
        } else if (rc == PICOQUIC_ERROR_STREAM_ALREADY_CLOSED) {
            /* The sending part already ended, or the stream retired:
             * nothing is owed. */
        } else {
            /* picoquic may already have latched the reset before failing to
             * queue its frame, so a retry could not prove it was sent. */
            failed = 1;
        }
    }
    ep->deferred_count = w;
    return failed ? -1 : 0;
}

void pico_wt_endpoint_settle_deferred(pico_wt_endpoint_ctx_t *ep)
{
    if (!ep) return;
    ep->deferred_count = 0;
    ep->deferred_at_count = 0;
}

bool pico_wt_endpoint_on_provide_data(pico_wt_endpoint_ctx_t *ep,
                                      uint64_t stream_id,
                                      void *provide_ctx, size_t space)
{
    size_t nb = 0; bool is_fin = false, still = false;
    if (!moq_pq_send_queue_plan(ep->queue, stream_id, space,
                                &nb, &is_fin, &still)) {
        (void)picoquic_provide_stream_data_buffer(provide_ctx, 0, 0, 0);
        return false;
    }
    uint8_t *dst = picoquic_provide_stream_data_buffer(
        provide_ctx, nb, is_fin ? 1 : 0, still ? 1 : 0);
    if (!dst) return false;
    moq_pq_send_queue_commit(ep->queue, stream_id, dst, nb);
    return !still;
}

static moq_transport_result_t ep_stop_sending(void *ctx,
                                               uint64_t stream_id,
                                               uint64_t error_code)
{
    pico_wt_endpoint_ctx_t *ep = (pico_wt_endpoint_ctx_t *)ctx;
    if (!ep->cnx || !ep->h3_ctx) return MOQ_TRANSPORT_ERROR;
    /* MoQ code -> WebTransport application-error space for the wire; an
     * out-of-range code is rejected (nothing is sent). */
    uint64_t wt_code;
    if (!pico_wt_moq_err_to_wt(error_code, &wt_code))
        return MOQ_TRANSPORT_ERROR;
    /* The adapter's own record shows the receive half already terminal at
     * the transport (every byte and the FIN delivered): STOP_SENDING has
     * nothing left to stop, and picoquic may already have released the
     * stream, so its refusal would not be a failure of this cancellation.
     * The adapter discarded what it held; the stop is complete with no
     * frame. Only that positive fact skips the call: any refusal of a STOP
     * picoquic does receive stays an ERROR. */
    if (ep->rx_terminal_discard &&
        ep->rx_terminal_discard(ep->cb_ctx, stream_id))
        return MOQ_TRANSPORT_OK;
    /* Idempotent as a reset is: picoquic answers a receiving part already
     * terminal -- reset by the peer, read to its final size, or retired with
     * the stream -- before changing anything. No frame/context change is
     * due; notify only that receive fact to discard held adapter input.
     * Any other refusal stays an ERROR. */
    int rc = picoquic_stop_sending(ep->cnx, stream_id, wt_code);
    if (rc == PICOQUIC_ERROR_STREAM_ALREADY_CLOSED) {
        if (ep->rx_stop_terminal) ep->rx_stop_terminal(ep->cb_ctx, stream_id);
        return MOQ_TRANSPORT_OK;
    }
    if (rc != 0)
        return MOQ_TRANSPORT_ERROR;   /* nothing sent: receive state untouched */
    if (wt_restore_h3_ctx(ep, stream_id) != 0)
        return MOQ_TRANSPORT_ERROR;
    /* STOP_SENDING accepted: our receive direction on this stream is cancelled,
     * so the adapter must discard what it retained. Otherwise a later
     * sweep could replay retained bytes, or grant credit, for a stream the
     * application just stopped -- which is exactly what libmoq emits when it
     * cancels a peer-origin fetch data stream. */
    if (ep->on_local_stop_sending)
        ep->on_local_stop_sending(ep->cb_ctx, stream_id);
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t ep_send_datagram(void *ctx,
                                                const uint8_t *data,
                                                size_t len)
{
    pico_wt_endpoint_ctx_t *ep = (pico_wt_endpoint_ctx_t *)ctx;

    /* A detached endpoint no longer owns a connection: nothing to send on. */
    if (!ep->cnx || !ep->h3_ctx)
        return MOQ_TRANSPORT_DROPPED;

    /* Single-slot buffer — drop if occupied */
    if (ep->pending_dg_len > 0)
        return MOQ_TRANSPORT_DROPPED;

    if (len > sizeof(ep->pending_dg_buf))
        return MOQ_TRANSPORT_TOO_LARGE;

    /* Readiness is declared on the WT session's stream prefix, looked up in
     * the endpoint's own h3zero context. h3zero_set_datagram_ready() would
     * instead fetch the h3zero context from the connection's callback context,
     * which is not an h3zero context when the connection callback is wrapped
     * (the managed client owns it): that lookup walks unrelated memory. */
    h3zero_stream_prefix_t *prefix =
        h3zero_find_stream_prefix(ep->h3_ctx, ep->control_stream_id);
    if (!prefix)
        return MOQ_TRANSPORT_DROPPED;

    if (data && len > 0)
        memcpy(ep->pending_dg_buf, data, len);
    ep->pending_dg_len = len;

    prefix->ready_to_send_datagrams = 1;
    if (picoquic_mark_datagram_ready(ep->cnx, 1) != 0) {
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

    ctx->alloc = *alloc;
    ctx->queue = moq_pq_send_queue_create(alloc, 0);
    if (!ctx->queue) return -1;

    *ops = (moq_transport_endpoint_ops_t){
        .struct_size     = sizeof(moq_transport_endpoint_ops_t),
        .capabilities    = MOQ_TRANSPORT_CAP_DATAGRAM |
                           MOQ_TRANSPORT_CAP_WRITE_PAYLOAD |
                           MOQ_TRANSPORT_CAP_HOLD_INPUT |
                           MOQ_TRANSPORT_CAP_RELIABLE_RESET |
                           MOQ_TRANSPORT_CAP_STREAM_DRAINED |
                           MOQ_TRANSPORT_CAP_SHARED_ADMISSION,
        .open_uni        = ep_open_uni,
        .set_stream_priority = ep_priority,
        .open_bidi       = ep_open_bidi,
        .write           = ep_write,
        .write_payload   = ep_write_payload,
        .reset_stream    = ep_reset,
        .stop_sending    = ep_stop_sending,
        .send_datagram   = ep_send_datagram,
        .max_datagram_size = NULL,
        .close_transport = ep_close,
        .reset_stream_at = ep_reset_at,
    };
#if defined(PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION) && PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION >= 1
    ops->capabilities |= MOQ_TRANSPORT_CAP_COMMIT_REPORT;
#endif
    return 0;
}

void pico_wt_endpoint_cleanup(pico_wt_endpoint_ctx_t *ctx)
{
    if (!ctx) return;
    ctx->pending_dg_len = 0;
    moq_pq_send_queue_destroy(ctx->queue);
    ctx->queue = NULL;
    ctx->deferred_count = 0;
    if (ctx->deferred_at)
        ctx->alloc.free(ctx->deferred_at,
                        ctx->deferred_at_cap * sizeof(*ctx->deferred_at),
                        ctx->alloc.ctx);
    ctx->deferred_at = NULL;
    ctx->deferred_at_count = ctx->deferred_at_cap = 0;
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
