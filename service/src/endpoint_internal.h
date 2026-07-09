#ifndef MOQ_SERVICE_ENDPOINT_INTERNAL_H
#define MOQ_SERVICE_ENDPOINT_INTERNAL_H

/*
 * Service-tier internals. NOT installed, NOT public API; white-box tests
 * compile against this via the moq-service-test-internals target.
 */

#include <moq/endpoint.h>
#include <moq/url.h>

/* The largest version-offer set: every version this build supports. */
#define MOQ_ENDPOINT_MAX_VERSIONS 8

/*
 * The pure result of configuration resolution: everything connect needs,
 * derived with no I/O. Byte fields BORROW from the caller's cfg (url bytes)
 * or from static storage ("/moq"); the resolved struct is only valid while
 * the cfg it was resolved from is.
 */
typedef struct moq_endpoint_resolved {
    moq_transport_protocol_t protocol;   /* never AUTO after resolve */
    moq_transport_backend_t  backend;    /* never AUTO after resolve */
    moq_url_t   url;                     /* parsed; spans borrow cfg->url */
    moq_bytes_t sni;                     /* cfg->sni, or url host */
    moq_bytes_t wt_path;                 /* WEBTRANSPORT only (else empty):
                                            cfg->wt_path, or url path, or "/moq" */
    moq_version_policy_t policy;
    moq_version_t versions[MOQ_ENDPOINT_MAX_VERSIONS];
    size_t        version_count;
} moq_endpoint_resolved_t;

/*
 * Resolve + validate an endpoint configuration (pure; no I/O, no allocation).
 *
 *  - MOQ_ERR_INVAL: malformed cfg (NULL/undersized struct, empty or
 *    unparsable URL, out-of-range enum, malformed version offer).
 *  - MOQ_ERR_UNSUPPORTED: well-formed but not servable by this build
 *    (server perspective, mvfst/proxygen backend, a version whose profile
 *    is not compiled in).
 */
moq_result_t moq_endpoint_resolve_cfg(const moq_endpoint_cfg_t *cfg,
                                      moq_endpoint_resolved_t *out);

/* True if this build's core can create a session for `v`. */
bool moq_endpoint_version_supported(moq_version_t v);

/* -- Internal pump hooks (attachment seam, design SS5.5) ------------- *
 *
 * Attached media services register a pump hook; the endpoint runs every
 * registered hook on its network thread each pump cycle, after the post()
 * task drain. This is an INTERNAL interface -- there is no public endpoint
 * hook API.
 *
 * Contract:
 *  - Hooks run on the network thread WITH THE ENDPOINT MUTEX HELD (that is
 *    what makes detach race-free), so a hook MUST NOT call public
 *    moq_endpoint_* APIs -- it may use `session` directly (it is on the
 *    session's thread) and its own state.
 *  - `session` may be NULL while the connection has not produced one yet
 *    (deferred version negotiation); hooks must tolerate it.
 *  - Bounded work per cycle (SS5.5) is the hook author's obligation.
 *  - Attach fails MOQ_ERR_WRONG_STATE after stop, or when the kind's
 *    single slot is taken (v0: at most one receiver + one sender).
 *  - Detach is idempotent for a ctx that is not attached. Each successful
 *    attach increments the endpoint attachment count (gating stop());
 *    detach decrements it.
 */
typedef enum moq_endpoint_hook_kind {
    MOQ_ENDPOINT_HOOK_RECEIVER = 0,
    MOQ_ENDPOINT_HOOK_SENDER   = 1,
    MOQ_ENDPOINT_HOOK_KIND_COUNT
} moq_endpoint_hook_kind_t;

typedef void (*moq_endpoint_hook_fn)(moq_endpoint_t *ep,
                                     moq_session_t *session,
                                     uint64_t now_us, void *ctx);

moq_result_t moq_endpoint_attach_hook(moq_endpoint_t *ep,
                                      moq_endpoint_hook_kind_t kind,
                                      moq_endpoint_hook_fn fn, void *ctx);
void moq_endpoint_detach_hook(moq_endpoint_t *ep,
                              moq_endpoint_hook_kind_t kind, void *ctx);

/* The sticky interrupt latch, for attached services' non-blocking polls
 * (the design's poll contract returns MOQ_ERR_INTERRUPTED while latched). */
bool moq_endpoint_interrupted_internal(const moq_endpoint_t *ep);

/* "Endpoint closed" check that does NOT take ep->mu, for a thread that holds a
 * service mutex (the hook lock order is ep->mu -> service-mu, so a service-mu
 * holder MUST NOT take ep->mu via the public moq_endpoint_is_closed/state APIs).
 * Mirrors that determination without ep->mu (a facade accessor it consults may
 * take its own non-endpoint lock). Safe to call from any thread. */
bool moq_endpoint_is_closed_internal(const moq_endpoint_t *ep);

bool moq_endpoint_is_fatal_internal(const moq_endpoint_t *ep);
uint64_t moq_endpoint_fatal_code_internal(const moq_endpoint_t *ep);

/* Internal retryable post: like moq_endpoint_post(), but a task that returns
 * MOQ_ERR_WOULD_BLOCK is REQUEUED (the same node, no re-allocation) to run again
 * on a later pump cycle instead of being freed -- allocation-free retry for work
 * that can hit transient session-queue backpressure (media receiver subscriber
 * teardown). All other returns free the node after one run, and the terminal
 * drain still runs the task exactly once with session == NULL (never requeued),
 * so the task MUST complete and release its ctx on the closed marker. This is
 * NOT the public post() contract (which is one-shot, return value ignored); it
 * stays internal precisely so public callers keep exactly-once semantics. */
moq_result_t moq_endpoint_post_retryable(moq_endpoint_t *ep,
                                         moq_endpoint_task_fn fn, void *ctx);

/* moq_endpoint_wake() is PUBLIC (declared in <moq/endpoint.h>, included above):
 * the non-allocating pump nudge attached services use to schedule
 * reconciliation of already-recorded state (record state, then wake, so the
 * public command stays atomic with no fallible allocation). Semantics
 * unchanged by the export. */

#endif /* MOQ_SERVICE_ENDPOINT_INTERNAL_H */
