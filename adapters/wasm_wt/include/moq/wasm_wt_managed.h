#ifndef MOQ_WASM_WT_MANAGED_H
#define MOQ_WASM_WT_MANAGED_H

/*
 * moq_wasm_wt_managed: a managed WebTransport facade for a libmoq built with
 * Emscripten and run in a browser. The browser's WebTransport API carries the
 * session; the shared transport bridge (<moq/transport_bridge.h>) does the
 * stream bookkeeping, as it does for every other adapter.
 *
 * Differences from the threaded facades (pico_wt, mvfst, MsQuic), all because a
 * browser page has one thread and cannot block:
 *
 *  - There is no network thread. WebTransport events arrive as JavaScript
 *    callbacks on the page's thread; each one feeds the session and then runs
 *    one pump cycle (the same on_pump callback, bridge service and timer
 *    arming a threaded facade runs per loop pass). Everything libmoq does for
 *    this endpoint therefore runs inside those callbacks, on that one thread.
 *  - _create() starts the WebTransport connection and returns at once. The
 *    session does not exist until the connection is ready and the relay has
 *    named the subprotocol, so session() is NULL until then and on_pump may
 *    run with no session (hooks already tolerate it).
 *  - wait() cannot block and returns MOQ_ERR_UNSUPPORTED. A consumer polls
 *    from its own JavaScript events instead; wake() schedules a pump cycle
 *    from the event loop.
 *  - TLS is the browser's: there is no insecure mode, no custom CA file and no
 *    SNI override. A certificate the browser rejects fails the connection.
 *
 * Experimental. Built only with Emscripten (MOQ_BUILD_WASM_WT_MANAGED=ON). The
 * browser side is wasm_wt_browser.js, linked with --js-library.
 */

#include <moq/session.h>
#include <moq/connection_target.h>
#include <moq/types.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moq_wasm_wt_managed moq_wasm_wt_managed_t;

typedef struct moq_wasm_wt_managed_cfg {
    uint32_t           struct_size;
    const moq_alloc_t *alloc;            /* required: alloc+realloc+free */

    const char        *host;             /* required */
    int                port;             /* 1..65535; required */
    const char        *path;             /* default "/moq" */

    /* WT-Available-Protocols offer, in preference order: a comma-separated
     * list of tokens, each optionally double-quoted ("moqt-22", "moqt-18").
     * Required: the relay's WT-Protocol answer picks the session's version,
     * and a relay that answers none ends the connection (a browser cannot
     * negotiate in-band). */
    const char        *wt_protocols;

    bool               send_request_capacity;
    uint64_t           initial_request_capacity;   /* default 64 */
    uint64_t           goaway_timeout_us;          /* 0 = disabled */

    /* The connection target the session names in SETUP (see
     * moq_adapter_bind_session). NULL derives it from host, port and path. */
    const moq_connection_target_t *target;

    const moq_auth_token_t *setup_auth_tokens;
    size_t                  setup_auth_token_count;

    /* App pump callback (required). Runs on the page's thread after every
     * WebTransport event and timer, with the session (NULL until the
     * connection is ready). Return 0 to continue; nonzero ends the pump
     * (the facade then reports closed). */
    int              (*on_pump)(moq_wasm_wt_managed_t *m, uint64_t now_us, void *ctx);
    void              *on_pump_ctx;

    /* Optional application service deadline: the app's earliest pending
     * time-based deadline in this facade's clock domain (microseconds, the
     * browser's monotonic clock), or UINT64_MAX for none. Folded into the
     * facade's timer after each pump. Pure and non-blocking. */
    uint64_t         (*app_deadline_us)(void *ctx);
    void              *app_deadline_ctx;
} moq_wasm_wt_managed_cfg_t;

MOQ_API void moq_wasm_wt_managed_cfg_init(moq_wasm_wt_managed_cfg_t *cfg);

/* Validates, copies what it needs, and starts the WebTransport connection
 * (asynchronous). MOQ_ERR_INVAL for a bad cfg, MOQ_ERR_UNSUPPORTED when the
 * browser has no WebTransport. */
MOQ_API moq_result_t moq_wasm_wt_managed_create(const moq_wasm_wt_managed_cfg_t *cfg,
                                                moq_wasm_wt_managed_t **out);

/* Closes the connection and marks the facade stopped. Idempotent. */
MOQ_API moq_result_t moq_wasm_wt_managed_stop(moq_wasm_wt_managed_t *m);
MOQ_API void         moq_wasm_wt_managed_destroy(moq_wasm_wt_managed_t *m);

/* NULL until the connection is ready and the session was created. Valid until
 * destroy. Use it only from on_pump or the callbacks libmoq runs from it. */
MOQ_API moq_session_t *moq_wasm_wt_managed_session(moq_wasm_wt_managed_t *m);

/* The version the relay selected (0 until then). */
MOQ_API moq_version_t moq_wasm_wt_managed_negotiated_version(const moq_wasm_wt_managed_t *m);

MOQ_API bool     moq_wasm_wt_managed_is_fatal(const moq_wasm_wt_managed_t *m);
MOQ_API uint64_t moq_wasm_wt_managed_fatal_code(const moq_wasm_wt_managed_t *m);
MOQ_API bool     moq_wasm_wt_managed_is_closed(const moq_wasm_wt_managed_t *m);
MOQ_API uint64_t moq_wasm_wt_managed_close_code(const moq_wasm_wt_managed_t *m);

/* Schedules one pump cycle from the event loop (coalesced). */
MOQ_API moq_result_t moq_wasm_wt_managed_wake(moq_wasm_wt_managed_t *m);

/* Always MOQ_ERR_UNSUPPORTED: a browser page cannot block. */
MOQ_API moq_result_t moq_wasm_wt_managed_wait(moq_wasm_wt_managed_t *m, uint64_t timeout_us);

/* Runs one pump cycle now (bridge service, on_pump, bridge service, timer).
 * For hosts that drive the facade from their own events; normally the
 * WebTransport callbacks do this. */
MOQ_API void moq_wasm_wt_managed_service(moq_wasm_wt_managed_t *m);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_WASM_WT_MANAGED_H */
