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
 * The §4.4 mapping is NOT a plain additive base: it skips the reserved
 * HTTP/3 GREASE points (0x1f*N + 0x21).
 *   encode(n) = FIRST + n + n/0x1e            (n <= UINT32_MAX)
 *   decode(h) = shifted - shifted/0x1f        (shifted = h - FIRST)
 * picoquic's H3ZERO_WEBTRANSPORT_APPLICATION_ERROR(n) encodes the same way
 * but has no range check or decoder, so the adapter keeps its own pair.
 * FIRST is picoquic's base (H3ZERO_WEBTRANSPORT_APPLICATION_ERROR(0));
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

/* Inbound HTTP/3-space code -> MoQ code. True, with *out set, only for a
 * genuine, non-reserved WebTransport application code in [FIRST, LAST].
 * Anything else -- an HTTP/3 or WebTransport protocol code such as
 * WT_SESSION_GONE, a reserved GREASE point, a value above the range, or a bare
 * small value a peer sent unmapped -- is still a reset, but carries no
 * application error code (draft-ietf-webtrans-http3-15 §4.4): false, and the
 * caller must not report the raw value as a MoQ code. */
/* The code a data-stream reset with no application error code is delivered
 * with. The bridge's stream-reset entry point has no "no code" form, so this
 * is a stand-in: INTERNAL_ERROR (0x0), the generic stream error in both
 * drafts' tables. The session and the application cannot tell it apart from
 * a peer that sent INTERNAL_ERROR itself; a peer resetting with raw, unmapped
 * MoQ codes no longer has them passed through. */
#define PICO_WT_RESET_NO_APP_CODE 0x0u

static inline bool pico_wt_wt_err_to_moq(uint64_t h3_code, uint64_t *out)
{
    if (h3_code < PICO_WT_WT_ERR_FIRST || h3_code > PICO_WT_WT_ERR_LAST)
        return false;
    uint64_t shifted = h3_code - PICO_WT_WT_ERR_FIRST;
    if (shifted % 0x1full == 0x1eull) return false;    /* reserved GREASE */
    *out = shifted - shifted / 0x1full;
    return true;
}

/* A reset accepted from the bridge whose WebTransport preamble picoquic has
 * not sent yet. */
#define PICO_WT_DEFERRED_RESETS_MAX 64u   /* the bridge's default max_pending */
typedef struct {
    uint64_t sid;
    uint64_t wt_code;
    uint64_t reliable_size;   /* the preamble */
} pico_wt_deferred_reset_t;

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

    /* Resets waiting for their preamble to be sent (see ep_reset). Bounded:
     * a reset that finds this full is refused with WOULD_BLOCK, so the
     * bridge keeps it in its own bounded pending queue and retries it. */
    pico_wt_deferred_reset_t deferred[PICO_WT_DEFERRED_RESETS_MAX];
    size_t deferred_count;
} pico_wt_endpoint_ctx_t;

#if defined(MOQ_PICO_WT_TESTING)
/* Test builds only: replaces picoquic_reset_stream_at for the deferred-reset
 * retry, so a test can make the provider fail deterministically. */
extern int (*pw_test_reset_stream_at)(picoquic_cnx_t *cnx, uint64_t stream_id,
                                      uint64_t error, uint64_t reliable_size);
#endif

/* Returns 0 on success, -1 on allocation failure (queue create). */
int pico_wt_endpoint_init(moq_transport_endpoint_ops_t *ops,
                           pico_wt_endpoint_ctx_t *ctx,
                           picoquic_cnx_t *cnx,
                           h3zero_callback_ctx_t *h3_ctx,
                           h3zero_stream_ctx_t *control_stream_ctx,
                           const moq_alloc_t *alloc);

void pico_wt_endpoint_cleanup(pico_wt_endpoint_ctx_t *ctx);

/* Issue every deferred reset whose preamble picoquic has now sent. Called
 * outside the provide_data callback: from moq_pico_wt_service and at the
 * start of the other WebTransport callbacks. Returns 0, or -1 when picoquic
 * failed to issue one of them; the caller then fails the transport, as it
 * would for a reset refused on the spot. */
int pico_wt_endpoint_flush_resets(pico_wt_endpoint_ctx_t *ctx);

/* Forget every deferred reset once the session is detached. The streams
 * themselves are not left open: their send side was never marked finished,
 * so deregistering the session resets them with WT_SESSION_GONE. */
void pico_wt_endpoint_settle_deferred(pico_wt_endpoint_ctx_t *ctx);

/* Service an h3zero picohttp_callback_provide_data for `stream_id`: copy up to
 * `space` queued bytes into picoquic's buffer via `provide_ctx`, setting FIN
 * and still-active as the queue dictates. Reneges when nothing is queued.
 * Returns true when this call emptied the stream's queue. */
bool pico_wt_endpoint_on_provide_data(pico_wt_endpoint_ctx_t *ctx,
                                      uint64_t stream_id,
                                      void *provide_ctx, size_t space);

/* Apply the stream priorities recorded since the last call; call before
 * picoquic sends. Returns 0, or -1 when picoquic refuses one (fatal). */
int pico_wt_endpoint_apply_priorities(pico_wt_endpoint_ctx_t *ctx);

/*
 * Remove the WT session's stream prefix from `h3`, delivering the same
 * picohttp_callback_deregister to its bound function that
 * h3zero_delete_stream_prefix() would. h3zero finds the control stream for
 * that callback through the connection's callback context, which is not an
 * h3zero context when the connection callback is wrapped (the managed
 * client); this does the lookup in `h3` instead. Inline so the attach adapter
 * and the managed facade share it without a cross-library private symbol.
 */
static inline void pico_wt_h3_delete_prefix(picoquic_cnx_t *cnx,
                                            h3zero_callback_ctx_t *h3,
                                            uint64_t prefix_id)
{
    h3zero_stream_prefix_t *prefix = h3zero_find_stream_prefix(h3, prefix_id);
    if (!prefix) {
        h3zero_delete_stream_prefix(cnx, h3, prefix_id);   /* logs the miss */
        return;
    }
    picohttp_post_data_cb_fn fn = prefix->function_call;
    void *fn_ctx = prefix->function_ctx;
    h3zero_stream_ctx_t *control = fn ? h3zero_find_stream(h3, prefix_id) : NULL;
    /* With no function bound, h3zero only unlinks and frees the prefix. */
    prefix->function_call = NULL;
    h3zero_delete_stream_prefix(cnx, h3, prefix_id);
    if (fn && control)
        (void)fn(cnx, NULL, 0, picohttp_callback_deregister, control, fn_ctx);
}

/*
 * h3zero_callback_delete_context() for a context whose connection callback
 * may be wrapped: every remaining prefix is removed through
 * pico_wt_h3_delete_prefix() first, then the context is deleted.
 */
static inline void pico_wt_h3_delete_context(picoquic_cnx_t *cnx,
                                             h3zero_callback_ctx_t *h3)
{
    while (h3->stream_prefixes.first != NULL)
        pico_wt_h3_delete_prefix(cnx, h3, h3->stream_prefixes.first->prefix);
    h3zero_callback_delete_context(cnx, h3);
}

/*
 * Called by the adapter when picohttp_callback_provide_datagram
 * fires. Flushes the pending datagram buffer.
 * Returns 0 on success, -1 if no datagram pending.
 */
int pico_wt_endpoint_provide_datagram(pico_wt_endpoint_ctx_t *ctx,
                                       uint8_t *context,
                                       size_t space);

#endif /* MOQ_PICO_WT_ENDPOINT_H */
