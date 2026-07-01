#ifndef MOQ_TRANSPORT_BRIDGE_H
#define MOQ_TRANSPORT_BRIDGE_H

/*
 * Shared transport bridge for adapter authors.
 *
 * Bridges moq_session_t actions to a transport endpoint via a thin
 * vtable (moq_transport_endpoint_ops_t). Owns stream mapping, outbound
 * and inbound backpressure retry, FIN/reset/stop lifecycle, tombstoning,
 * and close/drain semantics so that concrete QUIC/WebTransport adapters
 * need only implement the endpoint operations.
 *
 * The bridge does NOT own the session or the endpoint. The adapter
 * creates both and passes them to the bridge at creation time. The
 * bridge derives client/server perspective from the session via
 * moq_session_perspective().
 *
 * Stability: this is a PRIVATE ADAPTER SPI, not a stable third-party or
 * application ABI. The header is NOT included by moq/moq.h and is excluded
 * from install; its MOQ_API symbols are exported from the shared libmoq-core
 * only so that separately-built adapter DSOs can link them. They are
 * versioned lockstep with libmoq-core (an adapter builds against the matching
 * core) and MAY CHANGE before 1.0 and between minor releases. Application
 * code must use moq/session.h + an adapter, never these symbols directly.
 * The exact exported set is pinned by the bridge_symbol_policy test
 * (tests/cmake/check_bridge_symbols.cmake). Adapters include this header
 * directly from the source tree:  #include <moq/transport_bridge.h>
 *
 * -- Reentrancy --
 *
 * Non-reentrant. Single-threaded. All calls on the same thread.
 * Inbound handlers feed the session and update bridge state only;
 * endpoint ops are called exclusively from service(). Calling any
 * bridge function from inside an endpoint operation is undefined.
 *
 * -- Service ordering --
 *
 * Caller supplies now_us. Bridge owns the ordering:
 *   1. Retry outbound pending (FIFO). Old blocked actions first.
 *   2. Drain new session actions (only if pending queue is empty).
 *   3. Retry inbound pending (reset/stop/FIN/control). If progress,
 *      loop back to step 1.
 *   4. Tick deadlines. If tick produced actions, loop back to step 1.
 *
 * Invariant: new session actions are never polled while the outbound
 * pending queue is non-empty.
 *
 * -- Buffer ownership (outbound) --
 *
 * On OK: bridge calls moq_action_cleanup (rcbuf payload decrefs).
 *   If the endpoint needs the data longer (e.g. IOBuf wrap), its
 *   write_payload() must have incref'd before returning OK.
 * On WOULD_BLOCK: bridge retains the action in the pending queue.
 *   No cleanup until retry succeeds or the item is discarded.
 * Borrowed data (SEND_CONTROL, OPEN_BIDI initial data): bridge copies
 *   into a heap allocation before action cleanup. Copy freed on
 *   successful send or bridge destroy.
 *
 * -- Buffer ownership (inbound rcbuf) --
 *
 * on_peer_uni_rcbuf() borrows the caller's rcbuf reference for the
 * duration of the call. On OK: bridge/session has incref'd if it
 * needs to retain the rcbuf; caller may decref after return. On
 * WOULD_BLOCK: bridge does NOT retain the rcbuf (the session's
 * pending_retry mechanism replays via empty-data retry, not by
 * re-reading the original buffer). Caller may decref after return.
 * On error: same as WOULD_BLOCK. The bridge never holds a reference
 * to the caller's inbound rcbuf past the function return.
 *
 * -- Endpoint write contract --
 *
 * All-or-nothing. Endpoint write()/write_payload() must consume all
 * bytes or reject with WOULD_BLOCK. No partial consumption. If the
 * real transport does partial writes, the endpoint shim must buffer
 * internally and hide that from the bridge.
 *
 * -- Inbound WOULD_BLOCK contract --
 *
 * When an inbound handler returns MOQ_ERR_WOULD_BLOCK, the bridge has
 * retained internal retry state for that stream. The adapter:
 *   - SHOULD pause transport-level reads for that stream where it can,
 *     and SHOULD call service() promptly to allow retry. Pausing keeps
 *     the retry cheap (the session replays its own retained input via an
 *     empty-data retry rather than re-buffering).
 *   - MAY nonetheless deliver more bytes for the stream while it is still
 *     pending: the bridge appends them to the session's retained input
 *     when the session still owns the stream, or discards the remainder
 *     (until FIN) if the session has dropped or refused it. Delivering
 *     more bytes is therefore safe, just less efficient than pausing.
 *     Use moq_transport_bridge_stream_has_pending() to check per-stream
 *     status after service() returns.
 * Datagrams: WOULD_BLOCK means silently dropped, no retry.
 */

#include "export.h"
#include "types.h"

typedef struct moq_rcbuf moq_rcbuf_t;

#ifdef __cplusplus
extern "C" {
#endif

/* -- Endpoint result ------------------------------------------------ */

typedef enum moq_transport_result {
    MOQ_TRANSPORT_OK          = 0,
    MOQ_TRANSPORT_WOULD_BLOCK = 1,
    MOQ_TRANSPORT_DROPPED     = 2,  /* datagram only */
    MOQ_TRANSPORT_TOO_LARGE   = 3,  /* datagram only */
    MOQ_TRANSPORT_ERROR       = 4,  /* fatal */
} moq_transport_result_t;

/* -- Endpoint capabilities ------------------------------------------ */

typedef enum moq_transport_cap {
    MOQ_TRANSPORT_CAP_DATAGRAM      = 1u << 0,
    MOQ_TRANSPORT_CAP_WRITE_PAYLOAD = 1u << 1,
} moq_transport_cap_t;

/* -- Endpoint ops vtable -------------------------------------------- */
/*
 * Transport-specific operations. Each QUIC/WebTransport stack provides
 * one of these. Required ops (create rejects NULL):
 *   open_uni, open_bidi, write, reset_stream, stop_sending,
 *   close_transport.
 * Optional ops (gated by capabilities or struct_size):
 *   write_payload (CAP_WRITE_PAYLOAD), send_datagram (CAP_DATAGRAM),
 *   max_datagram_size (CAP_DATAGRAM).
 *
 * -- Write contract --
 *
 * write() and write_payload(): all-or-nothing. Consume all bytes or
 * reject with WOULD_BLOCK. No partial consumption.
 *
 * -- Reset/stop/close contract --
 *
 * reset_stream(), stop_sending(), and close_transport():
 * all-or-nothing, same as writes.
 *   OK: frame/operation is queued for transmission.
 *   WOULD_BLOCK: bridge retains and retries on next service().
 *   ERROR: fatal.
 * reset_stream and stop_sending are idempotent: calling after the
 * stream is already reset/stopped is OK (returns OK, no-op).
 *
 * -- Outbound datagram policy --
 *
 * send_datagram() results (all non-fatal, no retry):
 *   OK: datagram queued for transmission. Action cleaned up.
 *   WOULD_BLOCK: datagram silently dropped. Action cleaned up.
 *   DROPPED: datagram silently dropped (e.g. queue full). Cleaned up.
 *   TOO_LARGE: datagram silently dropped (oversized). Cleaned up.
 *   ERROR: fatal.
 * When CAP_DATAGRAM is not set, the bridge silently drops
 * SEND_DATAGRAM actions and cleans them up (not fatal).
 * Datagrams are never retried — they are lossy by nature.
 */

typedef struct moq_transport_endpoint_ops {
    uint32_t struct_size;
    uint32_t capabilities;

    moq_transport_result_t (*open_uni)(void *ctx, uint64_t *out_id);
    moq_transport_result_t (*open_bidi)(void *ctx, uint64_t *out_id);

    moq_transport_result_t (*write)(void *ctx, uint64_t stream_id,
                                    const uint8_t *data, size_t len,
                                    bool fin);

    moq_transport_result_t (*write_payload)(void *ctx, uint64_t stream_id,
                                            moq_rcbuf_t *buf, bool fin);

    moq_transport_result_t (*reset_stream)(void *ctx, uint64_t stream_id,
                                           uint64_t error_code);
    moq_transport_result_t (*stop_sending)(void *ctx, uint64_t stream_id,
                                           uint64_t error_code);

    moq_transport_result_t (*send_datagram)(void *ctx,
                                            const uint8_t *data,
                                            size_t len);

    size_t (*max_datagram_size)(void *ctx);

    moq_transport_result_t (*close_transport)(void *ctx, uint64_t code,
                                              const uint8_t *reason,
                                              size_t reason_len);

    void *reserved[4];
} moq_transport_endpoint_ops_t;

#ifdef __cplusplus
#define MOQ_TRANSPORT_ENDPOINT_OPS_INIT \
    ([]{moq_transport_endpoint_ops_t o{}; \
        o.struct_size=sizeof(o); return o;}())
#else
#define MOQ_TRANSPORT_ENDPOINT_OPS_INIT \
    ((moq_transport_endpoint_ops_t){ \
        .struct_size = sizeof(moq_transport_endpoint_ops_t) })
#endif

/* -- Bridge config -------------------------------------------------- */
/*
 * is_client is NOT in this struct. The bridge derives perspective
 * from moq_session_perspective(session) at create time. This avoids
 * drift between bridge and session assumptions.
 */

typedef struct moq_transport_bridge_cfg {
    uint32_t           struct_size;
    const moq_alloc_t *alloc;          /* required; copied into bridge */
    uint32_t           max_streams;    /* 0 = default 128 */
    uint32_t           max_pending;    /* 0 = default 64 */
    uint32_t           max_tombstones; /* 0 = default 64 */
} moq_transport_bridge_cfg_t;

#ifdef __cplusplus
#define MOQ_TRANSPORT_BRIDGE_CFG_INIT \
    ([]{moq_transport_bridge_cfg_t c{}; \
        c.struct_size=sizeof(c); return c;}())
#else
#define MOQ_TRANSPORT_BRIDGE_CFG_INIT \
    ((moq_transport_bridge_cfg_t){ \
        .struct_size = sizeof(moq_transport_bridge_cfg_t) })
#endif

MOQ_API void moq_transport_bridge_cfg_init(
    moq_transport_bridge_cfg_t *cfg,
    const moq_alloc_t *alloc);

/* -- Bridge lifecycle ----------------------------------------------- */

typedef struct moq_transport_bridge moq_transport_bridge_t;

/*
 * Create a bridge.
 *
 * session: NOT owned. Must outlive the bridge. Perspective (client/
 *          server) is derived from moq_session_perspective(session).
 * ops:     NOT owned. Must remain valid for the bridge's lifetime.
 *          Required ops must not be NULL (create returns ERR_INVAL).
 *          Zero-init is a valid starting point but NOT a complete
 *          vtable — missing required ops are rejected.
 * endpoint_ctx: passed as first arg to every ops call.
 *
 * Allocates: bridge state via cfg->alloc.
 * Ownership: caller owns *out until moq_transport_bridge_destroy().
 */
MOQ_API moq_result_t moq_transport_bridge_create(
    const moq_transport_bridge_cfg_t   *cfg,
    moq_session_t                      *session,
    const moq_transport_endpoint_ops_t *ops,
    void                               *endpoint_ctx,
    moq_transport_bridge_t            **out);

/*
 * Destroy a bridge and free all internal state.
 * Cleans up pending items (decrefs owned rcbufs, frees copied data).
 * destroy(NULL) is a no-op.
 */
MOQ_API void moq_transport_bridge_destroy(moq_transport_bridge_t *brg);

/*
 * Drive the bridge. See service ordering in the header comment.
 * Returns MOQ_OK or MOQ_ERR_INTERNAL (fatal — bridge is dead).
 */
MOQ_API moq_result_t moq_transport_bridge_service(
    moq_transport_bridge_t *brg,
    uint64_t                now_us);

/* -- Inbound handlers ----------------------------------------------- */
/*
 * Feed transport events into the session via the bridge.
 *
 * These do NOT call endpoint ops. All endpoint writes happen from
 * service(). Calling inbound handlers from inside an endpoint op
 * callback is undefined behavior.
 *
 * stream_id is the transport's native stream identifier.
 * The bridge maps it to/from moq_stream_ref_t internally.
 *
 * Return value: MOQ_OK on success. MOQ_ERR_WOULD_BLOCK means the
 * bridge has retained internal retry state; the adapter MUST NOT
 * deliver more bytes for that stream until service() clears it.
 * Negative values other than WOULD_BLOCK indicate fatal errors.
 */

/*
 * Control stream bytes. bool fin signals that the peer has FIN'd
 * the control stream (which the MoQ spec treats as a protocol
 * violation — the bridge routes it to the session which will close).
 */
MOQ_API moq_result_t moq_transport_bridge_on_peer_control_bytes(
    moq_transport_bridge_t *brg, uint64_t stream_id,
    const uint8_t *data, size_t len, bool fin, uint64_t now_us);

MOQ_API moq_result_t moq_transport_bridge_on_peer_uni_bytes(
    moq_transport_bridge_t *brg, uint64_t stream_id,
    const uint8_t *data, size_t len, bool fin, uint64_t now_us);

/*
 * Zero-copy variant. The bridge borrows the rcbuf for the duration
 * of the call. Bridge/session increfs if needed. Caller may decref
 * after return regardless of result (OK, WOULD_BLOCK, or error).
 */
MOQ_API moq_result_t moq_transport_bridge_on_peer_uni_rcbuf(
    moq_transport_bridge_t *brg, uint64_t stream_id,
    moq_rcbuf_t *data, bool fin, uint64_t now_us);

MOQ_API moq_result_t moq_transport_bridge_on_peer_bidi_bytes(
    moq_transport_bridge_t *brg, uint64_t stream_id,
    const uint8_t *data, size_t len, bool fin, uint64_t now_us);

MOQ_API moq_result_t moq_transport_bridge_on_peer_stream_reset(
    moq_transport_bridge_t *brg, uint64_t stream_id,
    uint64_t error_code, uint64_t now_us);

MOQ_API moq_result_t moq_transport_bridge_on_peer_stop_sending(
    moq_transport_bridge_t *brg, uint64_t stream_id,
    uint64_t error_code, uint64_t now_us);

/*
 * Datagram received from peer. On WOULD_BLOCK: silently dropped
 * (datagrams are lossy). No retry state is retained.
 */
MOQ_API moq_result_t moq_transport_bridge_on_peer_datagram(
    moq_transport_bridge_t *brg,
    const uint8_t *data, size_t len, uint64_t now_us);

/*
 * Transport-level close initiated by the peer (or detected by the
 * adapter, e.g. connection loss). Marks the bridge as closed (not
 * fatal), calls moq_session_on_transport_close() internally, and
 * cleans up pending state. After this, service() returns MOQ_OK and
 * is_closed() is true. Use for: peer close, application close,
 * connection end.
 */
MOQ_API moq_result_t moq_transport_bridge_on_transport_close(
    moq_transport_bridge_t *brg, uint64_t code, uint64_t now_us);

/*
 * Local transport/setup failure. Marks the bridge as fatal (not
 * closed), calls moq_session_on_transport_close() internally, and
 * cleans up pending state. After this, service() returns
 * MOQ_ERR_INTERNAL and is_fatal() is true. Use for: connection setup
 * error, cert/TLS failure, callback registration failure, adapter
 * invariant violation.
 */
MOQ_API moq_result_t moq_transport_bridge_on_transport_error(
    moq_transport_bridge_t *brg, uint64_t code, uint64_t now_us);

/* -- State queries -------------------------------------------------- */

MOQ_API bool     moq_transport_bridge_is_fatal(
    const moq_transport_bridge_t *brg);
MOQ_API uint64_t moq_transport_bridge_fatal_code(
    const moq_transport_bridge_t *brg);
MOQ_API bool     moq_transport_bridge_is_closed(
    const moq_transport_bridge_t *brg);
MOQ_API uint64_t moq_transport_bridge_close_code(
    const moq_transport_bridge_t *brg);
MOQ_API bool     moq_transport_bridge_is_terminal(
    const moq_transport_bridge_t *brg);
MOQ_API bool     moq_transport_bridge_has_pending(
    const moq_transport_bridge_t *brg);

/*
 * True when the attached session's profile carries control messages on a
 * unidirectional control-channel pair (each endpoint opens its own
 * unidirectional control stream). In this mode the bridge classifies peer
 * unidirectional streams itself (control vs data vs padding) and NO
 * bidirectional stream is the control channel -- adapters must route every
 * peer bidirectional stream to on_peer_bidi_bytes. False for profiles that
 * carry control on a shared bidirectional stream (the client-initiated
 * first bidi), where adapters route that stream to on_peer_control_bytes.
 * Fixed at bridge create; never changes over the bridge's lifetime.
 */
MOQ_API bool     moq_transport_bridge_uses_uni_control(
    const moq_transport_bridge_t *brg);

/*
 * Per-stream pending query. Returns true if the bridge has deferred
 * inbound state for this stream_id (pending_retry, pending_reset,
 * pending_stop, or pending_fin). Adapters use this to decide when
 * to resume transport-level reads for a specific stream after an
 * inbound handler returned MOQ_ERR_WOULD_BLOCK.
 * Returns false if stream_id is unknown or has no pending state.
 */
MOQ_API bool     moq_transport_bridge_stream_has_pending(
    const moq_transport_bridge_t *brg, uint64_t stream_id);

MOQ_API size_t   moq_transport_bridge_stream_count(
    const moq_transport_bridge_t *brg);
MOQ_API size_t   moq_transport_bridge_tombstone_count(
    const moq_transport_bridge_t *brg);

/*
 * Look up the moq_stream_ref_t assigned to a transport stream ID.
 * Returns a ref with ._v == 0 if the stream is not known.
 */
MOQ_API moq_stream_ref_t moq_transport_bridge_find_ref(
    const moq_transport_bridge_t *brg, uint64_t stream_id);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_TRANSPORT_BRIDGE_H */
