#ifndef MOQ_PICO_WT_H
#define MOQ_PICO_WT_H

/*
 * moq-adapter-pico-wt: bridges moq-core sessions to WebTransport over
 * picoquic's HTTP/3 (h3zero) + WebTransport (picowt) stack.
 *
 * Attach mode: the caller owns the picoquic connection, the h3zero
 * callback context, and the WebTransport control stream. The adapter
 * is created AFTER the WT session is established (CONNECT accepted on
 * both sides). On creation it rebinds the h3zero stream prefix so all
 * WT data streams and datagrams route through this adapter's callback.
 *
 * One moq_pico_wt_conn_t per MoQ session / WT session. The adapter
 * does not own the session, picoquic connection, or h3zero context —
 * those are created and driven by the application's event loop.
 *
 * Certificate policy is NOT this adapter's concern: the QUIC/TLS
 * handshake (and any certificate verification / SNI) is completed by the
 * caller's connection before the WT session is established and the
 * adapter is attached. For a managed alternative that owns the handshake
 * and offers a verifier hook, see <moq/pico_wt_managed.h> and
 * <moq/picoquic_verify.h>.
 *
 * Depends on picoquic built with BUILD_HTTP=ON (provides
 * pico_webtransport.h / h3zero_common.h and the picohttp-core
 * library). No moxygen / proxygen dependency.
 *
 * Thread safety: none. All calls must be serialized on the same
 * thread that drives picoquic (the callback thread).
 *
 * EXPERIMENTAL: installed as component adapter-pico-wt. Consume via
 * CMake (find_package(libmoq COMPONENTS adapter-pico-wt)) or pkg-config
 * (libmoq-pico-wt). The connection handle is opaque; the struct layout
 * behind moq_pico_wt_conn_t lives in pico_wt_adapter.h, which is
 * internal and NOT installed.
 */

#include <moq/export.h>
#include <moq/session.h>
#include <pico_webtransport.h>
#include <h3zero_common.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque adapter connection. The full struct is private. */
typedef struct moq_pico_wt_conn moq_pico_wt_conn_t;

typedef struct moq_pico_wt_conn_cfg {
    uint32_t               struct_size;
    moq_session_t         *session;   /* NOT owned */
    picoquic_cnx_t        *cnx;       /* NOT owned */
    h3zero_callback_ctx_t *h3_ctx;    /* NOT owned */
    h3zero_stream_ctx_t   *ctrl_ctx;  /* NOT owned (WT control stream) */
    const moq_alloc_t     *alloc;     /* requires alloc + realloc + free */
    void                  *user_ctx;  /* opaque application context */
} moq_pico_wt_conn_cfg_t;

/*
 * Initialize a config to defaults.
 *
 * ABI note: this pointer-only form cannot know the caller's storage size, so
 * it clears and stamps ONLY the frozen v0 prefix (the fields up to but not
 * including user_ctx). That keeps it safe for a binary compiled against ANY
 * version of this header calling ANY version of the library -- it never writes
 * past the caller's object. The appended user_ctx field is left untouched and
 * is NOT enabled (struct_size stays at the v0 size, so moq_pico_wt_conn_create
 * ignores it).
 *
 * To use user_ctx, initialize with moq_pico_wt_conn_cfg_init_sized(), passing
 * sizeof(*cfg) from your compilation unit; that clears the full struct you
 * allocated and stamps the matching size.
 */
MOQ_API void moq_pico_wt_conn_cfg_init(moq_pico_wt_conn_cfg_t *cfg);

/*
 * Size-aware initializer. Clears min(cfg_size, sizeof(*cfg)) bytes and stamps
 * struct_size to that size, where cfg_size is sizeof(*cfg) in the CALLER's
 * compilation unit. The ABI-safe way for a current caller to enable user_ctx,
 * and the recommended form for new code:
 *
 *     moq_pico_wt_conn_cfg_t cfg;
 *     moq_pico_wt_conn_cfg_init_sized(&cfg, sizeof(cfg));
 *     cfg.user_ctx = ...;
 */
MOQ_API void moq_pico_wt_conn_cfg_init_sized(moq_pico_wt_conn_cfg_t *cfg,
                                             size_t cfg_size);

/*
 * Create an adapter connection binding a moq_session to an
 * established WebTransport session. Returns 0 on success, -1 on
 * failure. On success *out owns no caller resources; destroy it with
 * moq_pico_wt_conn_destroy.
 */
MOQ_API int moq_pico_wt_conn_create(const moq_pico_wt_conn_cfg_t *cfg,
                                     moq_pico_wt_conn_t **out);

MOQ_API void moq_pico_wt_conn_destroy(moq_pico_wt_conn_t *conn);

/* Underlying moq_session for direct facade/API use (not owned). */
MOQ_API moq_session_t *moq_pico_wt_conn_session(moq_pico_wt_conn_t *conn);

/* True if the session/transport hit an unrecoverable error. */
MOQ_API bool moq_pico_wt_conn_is_fatal(const moq_pico_wt_conn_t *conn);

/* True if the session closed cleanly (e.g. GOAWAY drain). */
MOQ_API bool moq_pico_wt_conn_is_closed(const moq_pico_wt_conn_t *conn);

/* Clean-close code (e.g. MOQ_CLOSE_GOAWAY_TIMEOUT); 0 if not closed
 * or conn is NULL. */
MOQ_API uint64_t moq_pico_wt_conn_close_code(const moq_pico_wt_conn_t *conn);

/* Surface a QUIC CONNECTION_CLOSE into the bridge as a clean close. */
MOQ_API void moq_pico_wt_conn_notify_transport_closed(
    moq_pico_wt_conn_t *conn, uint64_t code, uint64_t now_us);

/*
 * Pump session actions into the WT connection. Call after each
 * picoquic event-loop iteration. Returns 0 on success, -1 on fatal.
 */
MOQ_API int moq_pico_wt_service(moq_pico_wt_conn_t *conn, uint64_t now_us);

/*
 * picohttp_post_data_cb_fn callback for the WT session. The adapter
 * installs this on the WT control stream prefix at create time; it is
 * declared here so applications that wrap the picoquic callback can
 * forward to it.
 */
MOQ_API int moq_pico_wt_callback(picoquic_cnx_t *cnx,
                                 uint8_t *bytes, size_t length,
                                 picohttp_call_back_event_t event,
                                 h3zero_stream_ctx_t *stream_ctx,
                                 void *path_app_ctx);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_PICO_WT_H */
