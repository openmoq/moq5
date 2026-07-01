#ifndef MOQ_PICOQUIC_H
#define MOQ_PICOQUIC_H

/*
 * Stability: pre-1.0, transport-specific adapter API. Mature enough for
 * integration pilots, but may change before 1.0.
 *
 * moq-adapter-picoquic: bridges moq-core sessions to a real QUIC
 * transport via picoquic.
 *
 * The adapter translates session actions (SEND_CONTROL, SEND_DATA,
 * SEND_DATAGRAM, RESET_DATA, STOP_DATA, CLOSE_SESSION) into picoquic
 * stream/datagram operations, and routes inbound stream bytes, resets,
 * stops, and datagrams into the corresponding session input functions.
 *
 * One moq_pq_conn_t per MoQ session/QUIC connection. The adapter does
 * not own the picoquic context or connection — those are created and
 * driven by the application's event loop.
 *
 * Thread safety: none. All calls must be serialized on the same thread
 * that drives picoquic (the callback thread).
 */

#include <moq/session.h>
#include <picoquic.h>

/* Draft-16 ALPN for picoquic connection setup. */
#define MOQ_PQ_ALPN_DRAFT16  "moqt-16"
#define MOQ_PQ_ALPN_DEFAULT  MOQ_PQ_ALPN_DRAFT16

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moq_pq_conn moq_pq_conn_t;

typedef struct moq_pq_conn_cfg {
    uint32_t         struct_size;
    moq_session_t   *session;
    picoquic_cnx_t  *cnx;
    const moq_alloc_t *alloc;
    void            *user_ctx;  /* opaque application context; default NULL */
    /*
     * Optional hook invoked after moq_pq_callback() and moq_pq_service()
     * complete. Receives the adapter connection and user_ctx. Intended
     * for signaling (e.g. waking a blocked thread) without requiring
     * an application callback wrapper around the picoquic callback.
     *
     * Non-reentrant: do not call moq_pq_*, moq_session_*, or
     * moq_pq_conn_destroy from inside the hook. The adapter reads
     * internal state after the hook returns. Signal-only.
     *
     * Not invoked from arbitrary internal helpers — only from the two
     * documented adapter entry points. Fires on all paths including
     * fatal. May be NULL (default).
     */
    void (*after_callback)(moq_pq_conn_t *conn, void *user_ctx);
} moq_pq_conn_cfg_t;

/*
 * Initialize a config to defaults.
 *
 * ABI note: this pointer-only form cannot know the caller's storage size, so
 * it clears and stamps ONLY the frozen v0 prefix (the fields up to but not
 * including user_ctx). That keeps it safe for a binary compiled against ANY
 * version of this header calling ANY version of the library -- it never writes
 * past the caller's object. Appended fields (user_ctx, after_callback) are left
 * untouched and are NOT enabled (struct_size stays at the v0 size, so
 * moq_pq_conn_create ignores them).
 *
 * To use the appended fields, initialize with moq_pq_conn_cfg_init_sized(),
 * passing sizeof(*cfg) from your compilation unit; that clears the full struct
 * you allocated and stamps the matching size.
 */
void moq_pq_conn_cfg_init(moq_pq_conn_cfg_t *cfg);

/*
 * Size-aware initializer. Clears min(cfg_size, sizeof(*cfg)) bytes and stamps
 * struct_size to that size, where cfg_size is sizeof(*cfg) in the CALLER's
 * compilation unit. This is the ABI-safe way for a current caller to enable
 * the appended fields, and the recommended form for new code:
 *
 *     moq_pq_conn_cfg_t cfg;
 *     moq_pq_conn_cfg_init_sized(&cfg, sizeof(cfg));
 *     cfg.user_ctx = ...;
 */
void moq_pq_conn_cfg_init_sized(moq_pq_conn_cfg_t *cfg, size_t cfg_size);

/*
 * Create an adapter connection binding a moq_session to a picoquic
 * connection. The session and cnx must already exist and be in a
 * usable state (ESTABLISHED or handshaking).
 *
 * The adapter registers itself as the picoquic stream/datagram
 * callback context. The application must call moq_pq_service()
 * after each picoquic event loop iteration to pump session actions
 * into the QUIC connection.
 *
 * QUIC DATAGRAM (attach mode): this adapter routes datagrams but does NOT
 * configure the caller-owned cnx. To send/receive datagrams the application
 * must enable negotiation on its own picoquic context BEFORE the handshake --
 * e.g. picoquic_set_default_tp_value(quic, picoquic_tp_max_datagram_frame_size,
 * size) on the quic, or set the per-cnx local transport parameter. Without
 * negotiation the peer's max is 0 and datagram sends are dropped non-fatally
 * (reported via the endpoint's max_datagram_size / send_datagram). The managed
 * moq_pq_threaded helper enables this by default; raw attach callers do not get
 * it for free.
 */
int moq_pq_conn_create(const moq_pq_conn_cfg_t *cfg,
                         moq_pq_conn_t **out);

void moq_pq_conn_destroy(moq_pq_conn_t *conn);

/*
 * Get the underlying moq_session for direct facade/API use.
 */
moq_session_t *moq_pq_conn_session(moq_pq_conn_t *conn);

/*
 * Opaque application context stored on the adapter connection.
 *
 * Intended pattern: the application wraps the picoquic callback to
 * add side effects, and recovers its own state from the adapter via
 * moq_pq_conn_user_ctx() without maintaining an external map from
 * picoquic connection to app state.
 *
 * Set at creation via moq_pq_conn_cfg_t.user_ctx, or at any time
 * via moq_pq_conn_set_user_ctx(). The adapter never reads or
 * modifies user_ctx itself.
 */
void *moq_pq_conn_user_ctx(const moq_pq_conn_t *conn);
void  moq_pq_conn_set_user_ctx(moq_pq_conn_t *conn, void *user_ctx);

/*
 * Query whether the adapter has entered a terminal fatal state.
 *
 * The adapter goes fatal when a non-recoverable error occurs during
 * moq_pq_callback() or moq_pq_service() — e.g. pre-retention
 * WOULD_BLOCK or a session protocol violation. After this point the
 * adapter will not process further events.
 *
 * App event loops use this to decide when to tear down the
 * connection and log the error. It is not a recovery mechanism:
 * once fatal, the adapter and its session are done.
 */
bool     moq_pq_conn_is_fatal(const moq_pq_conn_t *conn);
uint64_t moq_pq_conn_fatal_code(const moq_pq_conn_t *conn);

/*
 * Query whether the adapter reached terminal non-fatal close.
 *
 * is_closed() means the session or peer initiated a transport close.
 * close_code distinguishes the reason:
 *   - 0:      normal close (GOAWAY drain, clean shutdown)
 *   - nonzero: peer/session close with application error code
 *
 * After is_closed(), moq_pq_service() returns 0 and no further
 * processing occurs. This is distinct from is_fatal(), which
 * indicates an adapter or bridge internal failure.
 */
bool     moq_pq_conn_is_closed(const moq_pq_conn_t *conn);
uint64_t moq_pq_conn_close_code(const moq_pq_conn_t *conn);

/*
 * Pump session actions and retry pending inbound work.
 *
 * 1. Drains session actions into the picoquic connection.
 * 2. Retries any inbound operations that returned WOULD_BLOCK:
 *    - Control bytes: via moq_session_process_pending
 *    - Data/bidi bytes: empty retry (session retained input)
 *    - Deferred FIN: delivered after pending data retry succeeds
 *    - STOP_SENDING: retries with stored error code
 * 3. Drains any new actions produced by retries.
 *
 * Pre-retention WOULD_BLOCK (session did not create stream state)
 * is treated as fatal — the session has exhausted both stream
 * slots and action queue capacity.
 *
 * Datagrams that hit WOULD_BLOCK are silently dropped (lossy).
 * Returns 0 on success, negative on fatal error.
 */
int moq_pq_service(moq_pq_conn_t *conn, uint64_t now_us);

/*
 * Picoquic stream data callback. Install this as the connection's
 * callback function. The callback context must be the moq_pq_conn_t*.
 *
 * Usage:
 *   picoquic_set_callback(cnx, moq_pq_callback, adapter_conn);
 */
int moq_pq_callback(picoquic_cnx_t *cnx,
                      uint64_t stream_id,
                      uint8_t *bytes, size_t length,
                      picoquic_call_back_event_t event,
                      void *callback_ctx,
                      void *stream_ctx);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_PICOQUIC_H */
