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
 *   2. Drain new session actions, up to a bounded number per pass.
 *   3. Retry inbound pending (reset/stop/FIN/control). If progress,
 *      loop back to step 1.
 *   4. Tick deadlines. If tick produced actions, loop back to step 1.
 *
 * Invariant: an action never passes older pending work for the SAME
 * transport stream. Ordering is per stream, not per connection: an
 * action for one stream is not held up by blocked work on another.
 *
 * Polling is DESTRUCTIVE -- poll_actions transfers ownership and there
 * is no peek and no put-back -- so the bridge cannot decline to take an
 * action it has looked at. A conflicting action is therefore RETAINED
 * after being polled, on the same pending path an endpoint WOULD_BLOCK
 * uses, and is retried in FIFO order like any other pending item.
 *
 * Connection-level work keeps connection-wide ordering: a retained
 * control-stream write orders ahead of later ordinary work, and a
 * pending transport close keeps top priority. Close and fatal intent is
 * polled and acted on even while a stream's work is parked, so a clean
 * shutdown never waits on stream credit that may not arrive.
 *
 * Retained work has a bounded capacity. When that storage is full the
 * bridge stops polling new session actions and applies connection-wide
 * backpressure until capacity returns, then resumes. Saturation is a
 * normal flow-control state: it never drops queued work and never
 * escalates to a connection error on its own.
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
 *   - MAY nonetheless deliver more bytes for a RETAINED-input stream while
 *     it is still pending: the bridge appends them to the session's retained
 *     input when the session still owns the stream, or discards the
 *     remainder (until FIN) if the session has dropped it. Delivering more
 *     bytes is therefore safe for that case, just less efficient than
 *     pausing. Use moq_transport_bridge_stream_has_pending() to check
 *     per-stream status after service() returns.
 * Datagrams: WOULD_BLOCK means silently dropped, no retry.
 *
 * -- Inbound admission contract (MOQ_TRANSPORT_CAP_HOLD_INPUT) --
 *
 * A NEW peer unidirectional data stream (one the session does not own yet)
 * may arrive while the session has no free receive entry. HOLD_INPUT is
 * required at bridge creation for every endpoint, including publishers.
 * Missing support returns MOQ_ERR_UNSUPPORTED before allocation or endpoint
 * calls (after argument/vtable validation). There is no send-only exemption.
 * This is a lifetime contract; changing the vtable cannot withdraw it.
 * on_peer_uni_bytes/_rcbuf may return MOQ_ERR_INPUT_NOT_CONSUMED:
 *   - ZERO bytes of that call's chunk were consumed, and its FIN was not
 *     taken. Any leading bytes accepted on PREVIOUS calls (the stream-type
 *     prefix the bridge classifies with) stay bridge-owned and are replayed
 *     by the bridge itself; the adapter never resends them.
 *   - The adapter keeps that exact chunk (same bytes, same FIN flag) and
 *     delivers nothing newer on the stream. The stream reports pending via
 *     moq_transport_bridge_stream_has_pending() until a service() pass
 *     observes that the session can admit a stream again; the adapter then
 *     redelivers the held chunk. Admission is not reserved: another stream
 *     may take the entry first, in which case the redelivery is refused the
 *     same way, losslessly. Readiness (pending cleared) does not end the
 *     obligation: until the held chunk is accepted, a FIN-only delivery on
 *     the stream is refused too -- the FIN belongs with the held chunk and
 *     cannot overtake its bytes (a refused chunk that itself carried zero
 *     bytes and the FIN is simply redelivered as such). The obligation ends
 *     with the chunk's acceptance, a peer RESET, or bridge teardown.
 *   - A peer RESET of a pending stream retires it; the adapter drops the
 *     held chunk. Bridge close/fatal/destroy retires every pending stream;
 *     held chunks are then dropped by the adapter.
 *   - On a profile whose control channel is a unidirectional pair (drafts 18
 *     and 22), while the session is still processing control input it could
 *     not take at once, a chunk of any OTHER peer uni stream -- data,
 *     padding, or a stream whose type is not yet known -- is refused the
 *     same way, with the same obligations; the stream reports pending until
 *     that control input is processed, and its redelivery is then handled as
 *     if it had just arrived.
 * MOQ_ERR_INPUT_NOT_CONSUMED is never returned for an identified control
 * stream or for bidi input, and never reinterprets
 * MOQ_ERR_WOULD_BLOCK (retained input), except as follows.
 *
 * -- Held peer streams (MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT) --
 *
 * With this capability, admission holding is bounded:
 *   - On a profile whose control channel is a unidirectional pair (drafts
 *     18 and 22), the streams below never take the last free mappings while
 *     a control stream still needs one: one for each control stream not yet
 *     mapped (ours until it is first opened, the peer's until its stream type
 *     identifies it). A NEW peer bidirectional (request)
 *     stream that finds no mapping free beyond that reserve is held too:
 *     MOQ_ERR_INPUT_NOT_CONSUMED, with the same zero-consumption and
 *     exact-redelivery obligations as above.
 *   - On the same profiles, a NEW peer uni stream whose first chunk carries
 *     a complete data stream type (a subgroup or fetch header) and finds no
 *     mapping free beyond the reserve is held the same way; a NEW peer uni
 *     stream whose first chunk carries a complete padding stream type is
 *     then refused with MOQ_BRIDGE_INGRESS_REFUSE_CANCEL and never held. A
 *     stream whose first chunk is the control type, an incomplete type or
 *     an unknown type is never held or refused this way: it takes a free
 *     mapping as without this capability, and a full stream map is fatal
 *     for it.
 *   - On a profile whose control channel is one bidirectional stream (draft
 *     16, where every peer unidirectional stream is data), a NEW peer uni
 *     stream that finds no free stream mapping is held the same way. The
 *     draft-16 control stream is never held.
 *   - Every held stream (uni or bidi) takes one hold record; there are
 *     max_pending of them. Held streams are offered for redelivery oldest
 *     first, and a new arrival waits behind them.
 *   - A stream that cannot be held because every record is in use is refused
 *     with MOQ_BRIDGE_INGRESS_REFUSE_CANCEL (above): the adapter cancels it.
 *   - A stream held for the bridge's local hold age (five seconds from its first
 *     refusal, checked against now_us on every service pass) is cancelled
 *     by the bridge through stop_sending (and
 *     abort_stream, or reset_stream, for a bidi). The adapter drops the held
 *     chunk when it sees that cancellation for a held id and delivers
 *     nothing more on it. An unexpected endpoint ERROR is fatal.
 *   - A peer FIN never discharges a held chunk; a peer RESET settles only the
 *     receive half, and our own half of a bidi is still reset.
 * Every cancellation carries EXCESSIVE_LOAD (0x9) on drafts 18 and 22, and
 * CANCELLED (0x1) on draft 16, which defines no EXCESSIVE_LOAD stream code;
 * an adapter selects it by moq_session_version.
 * The earliest hold age is reported through moq_session_next_deadline_us;
 * moq_session_tick does not consume it, so an adapter with this capability
 * drives time only through moq_transport_bridge_service.
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
    MOQ_TRANSPORT_DROPPED     = 2,  /* datagram; reset_stream_at: unavailable */
    MOQ_TRANSPORT_TOO_LARGE   = 3,  /* datagram only */
    MOQ_TRANSPORT_ERROR       = 4,  /* fatal */
} moq_transport_result_t;

/* -- Endpoint capabilities ------------------------------------------ */

typedef enum moq_transport_cap {
    MOQ_TRANSPORT_CAP_DATAGRAM      = 1u << 0,
    MOQ_TRANSPORT_CAP_WRITE_PAYLOAD = 1u << 1,
    /* The adapter can HOLD an inbound peer uni data chunk the bridge did not
     * consume (see "Inbound admission contract" below) and redeliver it. */
    MOQ_TRANSPORT_CAP_HOLD_INPUT    = 1u << 2,
    /* The adapter reports when a local data stream reaches "all data
     * committed" (every byte through its FIN acknowledged by the peer) via
     * moq_transport_bridge_on_local_stream_committed(). With this set the
     * bridge keeps a FIN'd local unidirectional data stream mapped until that
     * report or a RESET of the stream, so a session that times the subgroup
     * from its FIN can still reset it; without it the stream is retired as
     * soon as its FIN is written and the report is never expected. */
    MOQ_TRANSPORT_CAP_COMMIT_REPORT = 1u << 3,
    /* The adapter provides reset_stream_at(): it can reset a stream while
     * still delivering a prefix of it (RESET_STREAM_AT), where the connection
     * negotiated that extension. Without it every reset is a plain
     * RESET_STREAM. */
    MOQ_TRANSPORT_CAP_RELIABLE_RESET = 1u << 4,
    /* Bounded admission holding with cancellation at the limit (see "Held
     * peer streams" below). Set by an adapter only once it intercepts
     * MOQ_BRIDGE_INGRESS_REFUSE_CANCEL at every ingress site and performs the
     * cancellations it implies. */
    MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT = 1u << 5,
    /* The endpoint calls moq_transport_bridge_on_stream_drained(). */
    MOQ_TRANSPORT_CAP_STREAM_DRAINED = 1u << 6,
} moq_transport_cap_t;

/* Private ingress outcome of on_peer_uni_bytes / on_peer_uni_rcbuf /
 * on_peer_bidi_bytes, returned only to endpoints that advertise
 * MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT: the bridge took zero bytes and no FIN
 * (a borrowed rcbuf stays the caller's), holds no record of the stream, and
 * the adapter must cancel it itself before returning to its transport --
 * STOP_SENDING, plus RESET_STREAM of its own half on a bidirectional stream,
 * both with the profile's cancellation code (MOQ_STREAM_ERROR_EXCESSIVE_LOAD
 * on drafts 18 and 22, MOQ_STREAM_ERROR_CANCELLED on draft 16) -- and drop
 * the chunk. An
 * unexpected failure of the transport to take that cancellation is a fatal
 * transport error. Disjoint from every moq_result_t and from internal
 * sentinels; never an application-visible result. */
#define MOQ_BRIDGE_INGRESS_REFUSE_CANCEL (-1100)

/* -- Endpoint ops vtable -------------------------------------------- */
/*
 * Transport-specific operations. Each QUIC/WebTransport stack provides
 * one of these. Required ops (create rejects NULL):
 *   open_uni, open_bidi, write, reset_stream, stop_sending,
 *   close_transport.
 * Optional ops (gated by capabilities or struct_size):
 *   write_payload (CAP_WRITE_PAYLOAD), send_datagram (CAP_DATAGRAM),
 *   max_datagram_size (CAP_DATAGRAM), reset_stream_at (CAP_RELIABLE_RESET).
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

    /* Object-pointer reserve prefix: unchanged layout for existing
     * callers. New ops are appended AFTER it and gated by struct_size
     * (HAS_FIELD), never overlaying a reserve slot with a function
     * pointer (ISO C does not guarantee they share a representation). */
    void *reserved[4];

    /*
     * OPTIONAL whole-stream abort: RESET our send half AND STOP_SENDING
     * the peer's half, both with `error_code`, as one operation. When
     * absent (struct_size too small, or NULL) the bridge falls back to
     * reset_stream + stop_sending with explicit pending state (a
     * WOULD_BLOCK after one successful half retries only the remaining
     * half); a runtime failure after partial application is fatal, never
     * rolled back. Appended after `reserved`: old callers stay ABI-safe.
     */
    moq_transport_result_t (*abort_stream)(void *ctx, uint64_t stream_id,
                                           uint64_t error_code);
    /* Optional scheduling hint for a data stream: a lower key is served
     * first, equal keys round-robin, zero is control. The key is
     * subscriber_priority << 24 | publisher_priority << 16 | (position + 1),
     * position being the stream's MOQT 7.2 place within its request. Called
     * before writes and whenever the key changes. OK or ERROR only; ERROR is
     * fatal. Must not consume bytes. */
    moq_transport_result_t (*set_stream_priority)(void *ctx, uint64_t stream_id,
                                                uint32_t key);
    /*
     * OPTIONAL reliable reset (MOQ_TRANSPORT_CAP_RELIABLE_RESET): reset our
     * send half of a unidirectional data stream with RESET_STREAM_AT, keeping
     * its first `reliable_size` bytes. The bridge calls it only with
     * 0 < reliable_size <= the bytes it handed this stream through
     * write/write_payload, after every one of those writes was accepted. The
     * endpoint must see those bytes delivered before the stream ends; it may
     * hold the frame until its transport has taken them. Results:
     *   OK           the reliable reset is committed;
     *   WOULD_BLOCK  nothing done; the bridge retains and retries;
     *   DROPPED      this connection cannot reset reliably (the extension
     *                was not negotiated, or the stack cannot express this
     *                size) and nothing was done: the bridge sends a plain
     *                reset_stream instead, the fallback the protocol expects;
     *   ERROR        fatal.
     * Appended after set_stream_priority: old callers stay ABI-safe.
     */
    moq_transport_result_t (*reset_stream_at)(void *ctx, uint64_t stream_id,
                                              uint64_t error_code,
                                              uint64_t reliable_size);
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

/*
 * max_streams is the stream-map capacity (0 selects the default, 128). On a
 * profile whose control channel is a unidirectional pair (drafts 18 and 22:
 * each peer opens one control stream, draft-22 section 6.3, draft-18 section
 * 3.3, and neither may be closed during the session), both control streams
 * hold a map entry for the whole session, so create refuses an effective
 * capacity below 2 with MOQ_ERR_INVAL before allocating anything. This is a
 * constraint of this implementation's map, not a protocol minimum, and 2
 * leaves no room for request or data streams. Draft-16 (one bidirectional
 * control stream) accepts 1.
 */
typedef struct moq_transport_bridge_cfg {
    uint32_t           struct_size;
    const moq_alloc_t *alloc;          /* required; copied into bridge */
    uint32_t           max_streams;    /* 0 = default 128; see above */
    uint32_t           max_pending;    /* 0 = default 64 */
    uint32_t           max_tombstones; /* 0 = default 64 */
    /* Appended: the most data bytes the bridge holds back for the endpoint,
     * to hand over in priority order; at the limit the application sees
     * backpressure. 0 = MOQ_TRANSPORT_BRIDGE_RETAINED_DEFAULT. */
    uint64_t           max_retained_bytes;
} moq_transport_bridge_cfg_t;

/* With the adapters' default 128 KiB send queue: 1 MiB outbound in total. */
#define MOQ_TRANSPORT_BRIDGE_RETAINED_DEFAULT ((uint64_t)(896u * 1024u))

#ifdef __cplusplus
#define MOQ_TRANSPORT_BRIDGE_CFG_INIT \
    ([]{moq_transport_bridge_cfg_t c{}; \
        c.struct_size=sizeof(c); return c;}())
#else
#define MOQ_TRANSPORT_BRIDGE_CFG_INIT \
    ((moq_transport_bridge_cfg_t){ \
        .struct_size = sizeof(moq_transport_bridge_cfg_t) })
#endif

/* Frozen v0 prefix: through max_tombstones. Not offsetof(max_retained_bytes):
 * where uint64_t aligns to 8 on a 32-bit ABI that adds padding an older
 * caller's struct_size does not cover. */
#define MOQ_TRANSPORT_BRIDGE_CFG_V0_SIZE \
    (offsetof(moq_transport_bridge_cfg_t, max_tombstones) + sizeof(uint32_t))

/* Clears and stamps only the v0 prefix; appended fields need
 * moq_transport_bridge_cfg_init_sized(). */
MOQ_API void moq_transport_bridge_cfg_init(
    moq_transport_bridge_cfg_t *cfg,
    const moq_alloc_t *alloc);

/* Clears and stamps min(cfg_size, sizeof); pass sizeof(*cfg). */
MOQ_API void moq_transport_bridge_cfg_init_sized(
    moq_transport_bridge_cfg_t *cfg, size_t cfg_size,
    const moq_alloc_t *alloc);

/* -- Bridge lifecycle ----------------------------------------------- */

typedef struct moq_transport_bridge moq_transport_bridge_t;

/*
 * Create a bridge.
 *
 * session: NOT owned. Must outlive the bridge. Perspective (client/
 *          server) is derived from moq_session_perspective(session).
 *          With max_actions >= 4 the bridge turns on the session's
 *          stream keys (moq_session_set_data_priority_updates) and hands
 *          data over in MOQT 7.2 order; below that it keys each write
 *          alone.
 * ops:     NOT owned. Must remain valid for the bridge's lifetime.
 *          Required ops must not be NULL (create returns ERR_INVAL).
 *          Zero-init is a valid starting point but NOT a complete
 *          vtable — missing required ops are rejected.
 * endpoint_ctx: passed as first arg to every ops call.
 *
 * Allocates: bridge state via cfg->alloc.
 * Ownership: caller owns *out until moq_transport_bridge_destroy().
 *
 * One bridge per session: while a bridge created on `session` exists --
 * including one that has gone fatal or closed -- another create on the same
 * session returns MOQ_ERR_WRONG_STATE, freeing what it allocated and
 * changing nothing else.
 * Destroying the bridge releases the session for a new one.
 */
MOQ_API moq_result_t moq_transport_bridge_create(
    const moq_transport_bridge_cfg_t   *cfg,
    moq_session_t                      *session,
    const moq_transport_endpoint_ops_t *ops,
    void                               *endpoint_ctx,
    moq_transport_bridge_t            **out);

/*
 * Destroy a bridge and free all internal state.
 * Cleans up pending items (decrefs owned rcbufs, frees copied data) and
 * releases the session for another bridge, so the session must still be
 * alive: destroy the bridge before its session.
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
 * stream_id is an ADAPTER-CHOSEN OPAQUE KEY identifying this transport
 * stream: unique for the LIFETIME OF THE CONNECTION (never reused, even
 * after tombstone expiry) and never UINT64_MAX (reserved as
 * MOQ_TRANSPORT_STREAM_ID_NONE). Adapters whose transport assigns native
 * QUIC stream ids synchronously MAY use them as keys (they satisfy the
 * uniqueness rule and make logs match captures); the bridge compares keys
 * by equality only, derives no meaning from the value, and maps them
 * to/from moq_stream_ref_t internally.
 *
 * Return value: MOQ_OK on success. MOQ_ERR_WOULD_BLOCK means the
 * bridge has retained internal retry state; the adapter MUST NOT
 * deliver more bytes for that stream until service() clears it.
 * MOQ_ERR_INPUT_NOT_CONSUMED (peer uni data, MOQ_TRANSPORT_CAP_HOLD_INPUT
 * endpoints only) means the chunk was not taken: hold it and redeliver it
 * when the stream's pending state clears (see the admission contract
 * above). Negative values other than these two indicate fatal errors.
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

/*
 * The peer's transport stream has fully terminated (both directions
 * done, as the adapter's stream-closed signal reports). Backend-neutral:
 * every adapter that learns of stream termination should call this,
 * except while the connection itself is shutting down (that is the
 * connection's terminal, reported once through on_transport_close /
 * on_transport_error, never as per-stream completions).
 *
 * It says only that the transport is finished with the stream: it is not a
 * FIN, not a reset, and not evidence that any byte was delivered or
 * acknowledged. The callback only latches retirement work; service performs
 * settlement outside endpoint-op frames, once pending ownership permits it:
 *   - a bidi the bridge is discarding after a whole-stream abort: its drain
 *     identity and mapping are released after outbound obligations settle,
 *     with no live session owner or same-ref queued action remaining. Genuine
 *     peer FIN/RESET reports on an already-aborting bidi use this same
 *     deferred settlement; accepting our abort alone, or receiving a peer
 *     STOP_SENDING, is not a terminal. No peer FIN/reset is fabricated;
 *   - a peer data stream the session stopped, once the endpoint accepted
 *     that STOP_SENDING (after a local receive abort some transports never
 *     report the peer's FIN or RESET). The mapping and the session's stopped
 *     owner are released by an eligible service pass, with no event; a FIN or
 *     RESET the bridge still owes the session is delivered first. The report
 *     may arrive before or after the STOP is accepted.
 * A live stream, a control stream or a stream the session has not admitted
 * is never released by it, and an unknown stream allocates nothing; a
 * repeated report is harmless.
 */
MOQ_API moq_result_t moq_transport_bridge_on_peer_stream_terminal(
    moq_transport_bridge_t *b, uint64_t stream_id, uint64_t now_us);

/*
 * A local unidirectional data stream has reached "all data committed": the
 * peer has acknowledged every byte through its FIN. Adapters declaring
 * MOQ_TRANSPORT_CAP_COMMIT_REPORT call this once per such stream; the bridge
 * retires the mapping it kept for the stream and tells the session
 * (moq_session_on_data_committed), which stops the subgroup's FIN-started
 * delivery timer. A stream the bridge is not holding for this report is a
 * harmless no-op.
 */
MOQ_API moq_result_t moq_transport_bridge_on_local_stream_committed(
    moq_transport_bridge_t *b, uint64_t stream_id, uint64_t now_us);

/*
 * Report a peer STOP_SENDING. Do not also reset the stream yourself.
 *
 * For an ACTIVE BIDIRECTIONAL mapping that is NOT already undergoing a
 * whole-stream abort, the bridge owns the response: the first STOP report it
 * accepts closes that stream's local send half and queues one RESET_STREAM
 * obligation, which service() performs outside the transport's receive
 * callback. Acceptance can fail -- if the pending queue is full the call
 * returns an error and the bridge becomes fatal, and no obligation exists.
 *
 * Given that accepted first report, further STOP_SENDING indications for the
 * same stream queue NO additional obligation. service() may ATTEMPT the one
 * obligation on several passes -- a WOULD_BLOCK leaves it pending and it is
 * retried -- but at most one RESET is ever successfully emitted and accepted by
 * the endpoint. A bridge destroyed before service, or one whose endpoint fails
 * the operation, emits none.
 *
 * An active bidi already undergoing a whole-stream abort is left to that
 * stronger lifecycle: the abort already owns the reset/stop work and the
 * mapping must stay in its discard state until the peer's terminal, so no
 * additional reset is queued for it.
 *
 * Only the sending direction ends. On a bidi the peer's own send half stays
 * live until its own terminal (FIN, RESET_STREAM, or full stream
 * termination). For a bidi WE opened, the mapping and its receive half are
 * therefore preserved across the STOP so the response to our request still
 * reaches the session.
 *
 * Every other stream class keeps its existing handling and is NOT part of the
 * queued-reset path above: an unknown or already retired stream is a no-op; a
 * peer-origin unidirectional stream has no local send half and is a no-op; a
 * unidirectional control stream takes its own fatal teardown; and any other
 * local-origin unidirectional stream continues through the session's data-stop
 * path.
 */
MOQ_API moq_result_t moq_transport_bridge_on_peer_stop_sending(
    moq_transport_bridge_t *brg, uint64_t stream_id,
    uint64_t error_code, uint64_t now_us);

/*
 * The endpoint holds no more bytes of local data stream `stream_id` (its send
 * queue emptied). After the FIN the stream leaves the priority order;
 * otherwise its next write sets its key again. A queued FETCH object whose
 * publisher priority differs from the held objects becomes eligible now;
 * a draft-22 fill also waits when its group changes. Objects in the same
 * scheduling class may be handed over together. This is local send
 * queue exhaustion, not peer acknowledgment. Without
 * MOQ_TRANSPORT_CAP_STREAM_DRAINED the bridge keys every write and drops the
 * stream from the order at the FIN. Its peers move up, so it may call
 * set_stream_priority for them before it returns.
 */
MOQ_API void moq_transport_bridge_on_stream_drained(
    moq_transport_bridge_t *brg, uint64_t stream_id);

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
/*
 * Mixed/global pending query: true when a service() call could make progress,
 * covering BOTH blocked outbound writes AND deferred inbound retry (paused
 * reads, buffered control). Use for "is there work" / idle decisions -- NOT
 * for transport write-readiness registration (see has_outbound_pending).
 */
MOQ_API bool     moq_transport_bridge_has_pending(
    const moq_transport_bridge_t *brg);

/*
 * Outbound-only pending query for connection write-readiness. True only for
 * state that transport send capacity can unblock: the outbound action FIFO
 * (writes the transport could not accept) and a deferred transport close.
 * Deliberately EXCLUDES inbound retry state (paused-read pending_retry,
 * pending_control, etc.), which is unblocked by the app draining events, not
 * by write capacity. Adapters arm a write-ready notification on this predicate
 * so a WOULD_BLOCK write retries when the transport can send again; arming on
 * the mixed has_pending() instead would spin under receive backpressure, since
 * connections are almost always write-ready.
 */
MOQ_API bool     moq_transport_bridge_has_outbound_pending(
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
 * pending_stop, or pending_fin), or, for a stream whose chunk the bridge
 * refused (MOQ_ERR_INPUT_NOT_CONSUMED), while that chunk may not be
 * redelivered yet (see the admission contract). Adapters use this to
 * decide when to resume transport-level reads for a specific stream after
 * an inbound handler returned MOQ_ERR_WOULD_BLOCK, and when to redeliver a
 * refused chunk.
 *
 * It is a readiness answer for streams whose receive records the adapter
 * polls, not an identity or ownership answer: on a profile whose control
 * channel is a unidirectional pair, while the session is still processing
 * control input it could not take at once, it returns true for every
 * stream_id other than the control stream that the bridge cannot exclude --
 * a mapped peer uni stream, or an id it has no mapping for -- since any of
 * those may be a peer uni stream whose chunk was refused. Locally opened
 * streams and mapped request streams are excluded. Otherwise it returns false if
 * stream_id is unknown or has no pending state.
 */
MOQ_API bool     moq_transport_bridge_stream_has_pending(
    const moq_transport_bridge_t *brg, uint64_t stream_id);

/*
 * Opaque event-progress token for the attached session. Advances (by an
 * unspecified amount) on every event ENQUEUE and every event DEQUEUE, and is
 * stable across calls that move no event. A coalesced-doorbell adapter (whose
 * app pump runs on a pass SEPARATE from service()) snapshots it BEFORE the pump
 * and compares for EQUALITY after the post-pump service pass: inequality means
 * the whole cycle moved events — the bounded pump dequeued some (draining a
 * pre-existing backlog) and/or service enqueued some (refilling the queue). It
 * pairs this with has_events to owe another pump only when work moved AND events
 * remain. Bounded by construction: a non-draining app moves no events once its
 * queue is full, so the token never advances and no pump re-arms.
 *
 * Caller-serialized, opaque (equality-only; never subtract or order it — it
 * wraps). This is private, lockstep adapter SPI, NOT application API.
 */
MOQ_API uint64_t moq_transport_bridge_event_progress_token(
    const moq_transport_bridge_t *brg);

/*
 * True while the attached session still holds at least one undelivered event.
 * Paired with the progress token by a coalesced-doorbell adapter: after a
 * pump+service cycle, re-arm one pump only when the token advanced (the cycle
 * moved events -- enqueue or dequeue) AND events remain here to drain. That
 * closes both the small-queue case (service refilled the queue after the pump)
 * and the large-queue case (the bounded pump drained some pre-existing events
 * but left more), while a non-draining app -- which moves no events once its
 * queue is full -- never advances the token and so never re-arms. Private,
 * lockstep adapter SPI, NOT application API.
 */
MOQ_API bool     moq_transport_bridge_has_events(
    const moq_transport_bridge_t *brg);

/*
 * The attached session's independent monotonic terminal facts, read together
 * so an adapter cannot sample them at two different instants.
 *
 *   returns        ENQUEUED -- MOQ_EVENT_SESSION_CLOSED was actually placed in
 *                  the event queue.
 *   *out_observed  OBSERVED -- moq_session_poll_events_ex TRANSFERRED that
 *                  event to a caller. A queued-but-unpolled terminal reads
 *                  false: availability is not observation.
 *
 * OBSERVED implies ENQUEUED. Both are monotonic and idempotent, and their
 * ORDER RELATIVE TO TRANSPORT TERMINAL IS NOT FIXED -- a local close enqueues
 * before any transport shutdown, a peer close can complete natively first. An
 * adapter must therefore record its own transport facts separately and never
 * derive one from the other. Deliberately NOT expressed through
 * moq_transport_bridge_is_closed/is_fatal, which latch the BRIDGE's terminal on
 * paths that need not have enqueued a session event.
 *
 * out_observed may be NULL. Pure read under caller-provided serialization.
 * Private, lockstep adapter SPI, NOT application API.
 */
MOQ_API bool     moq_transport_bridge_terminal_facts(
    const moq_transport_bridge_t *brg, bool *out_observed);

/*
 * True when `stream_id` carries this bridge's MoQ control channel: the control
 * bidi of a bidi-control profile, or either stream of a uni-control pair (the
 * local one once opened, the peer's once classified). A transport adapter
 * that must decide how a transport-level terminal on a stream maps to the
 * session (e.g. a WebTransport reset that carries no application code) asks
 * this instead of inferring the topology. Pure read under caller-provided
 * serialization. Private, lockstep adapter SPI, NOT application API.
 *
 * It answers from what the bridge has seen, not from the transport's full
 * topology: a peer unidirectional stream is known as control only once its
 * stream type has been read, so one reset before its first byte answers
 * false.
 */
MOQ_API bool     moq_transport_bridge_stream_is_control(
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
