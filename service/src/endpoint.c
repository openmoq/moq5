/*
 * moq_endpoint_t — configuration resolution (pure) + managed lifecycle.
 *
 * The endpoint wraps the existing managed transport facades rather than
 * duplicating adapter logic. It selects a backend via an internal registry
 * (a table of {protocol, backend, create-fn} rows in preference order) and
 * dispatches lifecycle ops through a small backend vtable (ep->vt) over an
 * opaque facade handle (ep->facade) -- so adding a transport is "one shim +
 * one registry row," not a new field plus a branch in every accessor. The
 * rows are: RAW_QUIC -> moq_pq_threaded_t and WEBTRANSPORT ->
 * moq_pico_wt_managed_t (always, picoquic); plus, when compiled in and asked
 * for explicitly (AUTO stays picoquic/pico_wt first), RAW_QUIC ->
 * moq_mvfst_managed_t (MOQ_SERVICE_HAVE_MVFST_MANAGED) and WEBTRANSPORT ->
 * moq_proxygen_wt_managed_t (MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED). Each
 * facade owns the network thread, the QUIC context, and the MoQ session. The
 * endpoint adds
 * the service-tier contracts on top: the
 * sticky interrupt latch, the post() executor (exactly-once, FIFO,
 * NULL-session terminal drain), terminal-state classification, and the
 * attachment count the media services use.
 *
 * Threading: the facade network thread runs ep_pump_cycle() (drains post()
 * tasks, refreshes the cached endpoint state); the app thread calls the
 * public surface. Shared state lives under ep->mu; the interrupt latch is a
 * lone atomic so set_interrupted() never takes a lock the network thread
 * holds.
 *
 * Version negotiation is live for the picoquic facades: the resolved offer
 * becomes the RAW_QUIC ALPN offer list / the WebTransport
 * WT-Available-Protocols token list (both in PREFERENCE ORDER — the client's
 * order decides), the facade reads the negotiated ALPN / selected WT-Protocol
 * back and creates the MoQ session with the matching version. The proxygen
 * WebTransport facade is likewise live (it creates the session after the WT
 * CONNECT response). The mvfst facade is exact-version-first: it creates the
 * session before the ALPN handshake, so it accepts only a single offered
 * version (a multi-version / AUTO offer is MOQ_ERR_UNSUPPORTED at create).
 * Either way moq_endpoint_negotiated_version() surfaces the version once
 * established.
 */

#include "endpoint_internal.h"

#include "../../adapters/common/moq_alpn.h"

#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>   /* clock_gettime: drain deadline (service tier may use the wall clock) */
#include <string.h>

#ifdef MOQ_SERVICE_HAVE_PQ_THREADED
#include <moq/picoquic_threaded.h>
#include <moq/picoquic_verify.h>
#endif
#ifdef MOQ_SERVICE_HAVE_PICO_WT_MANAGED
#include <moq/pico_wt_managed.h>
#ifndef MOQ_SERVICE_HAVE_PQ_THREADED
#include <moq/picoquic_verify.h>
#endif
#endif
#ifdef MOQ_SERVICE_HAVE_MVFST_MANAGED
#include <moq/mvfst.h>
#endif
#ifdef MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED
#include <moq/proxygen_wt_managed.h>
#endif

/* -- Version support ------------------------------------------------ */

bool moq_endpoint_version_supported(moq_version_t v)
{
    /* Single point of truth for the build's supported set. Both profiles are
     * unconditionally compiled today (core/CMakeLists.txt lists profile_d16.c
     * and profile_d18.c in every configuration); if a profile ever becomes a
     * build option, thread its define through here -- the offer rules (§5.2)
     * depend on this predicate being exact. */
    return v == MOQ_VERSION_DRAFT_16 || v == MOQ_VERSION_DRAFT_18;
}

/* The full supported set, NEWEST FIRST: both TLS ALPN selection and the
 * WebTransport protocol field honor the CLIENT's offer order (verified
 * end to end by the WT protocol-token negotiation test), so AUTO prefers
 * the newest version the peer also speaks. */
static size_t supported_versions(moq_version_t *out, size_t cap)
{
    size_t n = 0;
    if (n < cap) out[n++] = MOQ_VERSION_DRAFT_18;
    if (n < cap) out[n++] = MOQ_VERSION_DRAFT_16;
    return n;
}

/* -- Resolution ------------------------------------------------------ */

static bool bytes_empty(moq_bytes_t b) { return b.len == 0; }

static moq_result_t resolve_versions(const moq_version_offer_t *offer,
                                     moq_endpoint_resolved_t *out)
{
    /* Zero-init (struct_size 0) means AUTO per the cfg comment; a non-zero
     * but undersized struct is a malformed offer. */
    if (offer->struct_size == 0) {
        out->policy = MOQ_VERSION_POLICY_AUTO;
        out->version_count =
            supported_versions(out->versions, MOQ_ENDPOINT_MAX_VERSIONS);
        return MOQ_OK;
    }
    if (offer->struct_size < sizeof(moq_version_offer_t)) return MOQ_ERR_INVAL;

    switch (offer->policy) {
    case MOQ_VERSION_POLICY_AUTO:
        out->policy = MOQ_VERSION_POLICY_AUTO;
        out->version_count =
            supported_versions(out->versions, MOQ_ENDPOINT_MAX_VERSIONS);
        return MOQ_OK;
    case MOQ_VERSION_POLICY_LIST:
    case MOQ_VERSION_POLICY_EXACT:
        break;
    default:
        return MOQ_ERR_INVAL;
    }

    if (!offer->versions || offer->version_count == 0) return MOQ_ERR_INVAL;
    if (offer->policy == MOQ_VERSION_POLICY_EXACT && offer->version_count != 1)
        return MOQ_ERR_INVAL;
    if (offer->version_count > MOQ_ENDPOINT_MAX_VERSIONS) return MOQ_ERR_INVAL;

    for (size_t i = 0; i < offer->version_count; i++) {
        /* §5.2 rule 4: a version whose profile is not compiled in is
         * MOQ_ERR_UNSUPPORTED at create -- never silently downgraded. */
        if (!moq_endpoint_version_supported(offer->versions[i]))
            return MOQ_ERR_UNSUPPORTED;
        out->versions[i] = offer->versions[i];
    }
    out->policy = offer->policy;
    out->version_count = offer->version_count;
    return MOQ_OK;
}

moq_result_t moq_endpoint_resolve_cfg(const moq_endpoint_cfg_t *cfg,
                                      moq_endpoint_resolved_t *out)
{
    if (!cfg || !out) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_endpoint_cfg_t)) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));

    /* Optional byte spans must be coherent: a non-zero length with a NULL
     * data pointer is malformed, and rejecting it here (validation is live)
     * is what lets the connect path dereference these fields unchecked. */
    if (cfg->sni.len > 0 && !cfg->sni.data) return MOQ_ERR_INVAL;
    if (cfg->ca_file.len > 0 && !cfg->ca_file.data) return MOQ_ERR_INVAL;
    if (cfg->wt_path.len > 0 && !cfg->wt_path.data) return MOQ_ERR_INVAL;

    /* URL: required; the parser owns scheme/host/port/path validation
     * (moqt/moq/https only, so an unknown scheme is MOQ_ERR_INVAL whether or
     * not the protocol field would have overridden it -- the URL itself must
     * be well-formed). */
    if (bytes_empty(cfg->url) || !cfg->url.data) return MOQ_ERR_INVAL;
    moq_result_t urc = moq_url_parse(cfg->url, &out->url);
    if (urc < 0) return urc;

    /* Protocol: an explicit non-AUTO field WINS over the URL scheme (§5.1 --
     * the field is the override knob, a scheme/field conflict is not an
     * error). AUTO derives from the scheme. */
    switch (cfg->protocol) {
    case MOQ_TRANSPORT_PROTOCOL_AUTO: {
        /* moq_url_parse admits exactly moqt/moq/https. */
        bool https = out->url.scheme.len == 5 &&
                     memcmp(out->url.scheme.data, "https", 5) == 0;
        out->protocol = https ? MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT
                              : MOQ_TRANSPORT_PROTOCOL_RAW_QUIC;
        break;
    }
    case MOQ_TRANSPORT_PROTOCOL_RAW_QUIC:
    case MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT:
        out->protocol = cfg->protocol;
        break;
    default:
        return MOQ_ERR_INVAL;
    }

    /* Backend matrix: picoquic serves both protocols (native QUIC and
     * pico_wt); mvfst serves RAW_QUIC only, when compiled into the service.
     * AUTO picks the first compiled-in backend supporting the resolved
     * protocol -- picoquic first -- so AUTO stays picoquic even with mvfst
     * built. An explicit backend that is not compiled in (or not valid for
     * the resolved protocol) is MOQ_ERR_UNSUPPORTED. mvfst is RAW_QUIC-only;
     * proxygen is WEBTRANSPORT-only (each rejects the other protocol). */
    switch (cfg->backend) {
    case MOQ_TRANSPORT_BACKEND_AUTO:
    case MOQ_TRANSPORT_BACKEND_PICOQUIC:
        out->backend = MOQ_TRANSPORT_BACKEND_PICOQUIC;
        break;
    case MOQ_TRANSPORT_BACKEND_MVFST:
#ifdef MOQ_SERVICE_HAVE_MVFST_MANAGED
        /* mvfst is native QUIC only -- it has no WebTransport facade. */
        if (out->protocol != MOQ_TRANSPORT_PROTOCOL_RAW_QUIC)
            return MOQ_ERR_UNSUPPORTED;
        out->backend = MOQ_TRANSPORT_BACKEND_MVFST;
        break;
#else
        return MOQ_ERR_UNSUPPORTED;
#endif
    case MOQ_TRANSPORT_BACKEND_PROXYGEN:
#ifdef MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED
        /* proxygen is WebTransport only -- it has no raw-QUIC facade. */
        if (out->protocol != MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT)
            return MOQ_ERR_UNSUPPORTED;
        out->backend = MOQ_TRANSPORT_BACKEND_PROXYGEN;
        break;
#else
        return MOQ_ERR_UNSUPPORTED;
#endif
    default:
        return MOQ_ERR_INVAL;
    }

    /* Perspective: client-only in v0; server is reserved. */
    switch (cfg->perspective) {
    case MOQ_PERSPECTIVE_CLIENT:
        break;
    case MOQ_PERSPECTIVE_SERVER:
        return MOQ_ERR_UNSUPPORTED;
    default:
        return MOQ_ERR_INVAL;
    }

    /* SNI: explicit value wins; the mandatory default is the URL host
     * (an SNI mismatch already cost a debug round -- §5.1). */
    out->sni = bytes_empty(cfg->sni) ? out->url.host : cfg->sni;

    /* WT path: WEBTRANSPORT only. Explicit value, else the URL path, else
     * "/moq". RAW_QUIC leaves it empty (the URL path is carried in the
     * PATH setup option there, which the endpoint does not emit yet). */
    if (out->protocol == MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT) {
        if (!bytes_empty(cfg->wt_path)) {
            out->wt_path = cfg->wt_path;
        } else if (out->url.path.len > 0) {
            out->wt_path = out->url.path;
        } else {
            static const uint8_t k_default_path[] = "/moq";
            out->wt_path.data = k_default_path;
            out->wt_path.len = sizeof(k_default_path) - 1;
        }
    }

    return resolve_versions(&cfg->versions, out);
}

/* -- Endpoint instance ----------------------------------------------- */

typedef struct ep_task {
    struct ep_task      *next;
    moq_endpoint_task_fn fn;
    void                *ctx;
    bool                 retryable;  /* internal post path only: a WOULD_BLOCK
                                      * return requeues this node (see
                                      * moq_endpoint_post_retryable). Public
                                      * post() tasks are exactly-once. */
} ep_task_t;

/*
 * Backend vtable: the per-facade lifecycle ops the endpoint dispatches over an
 * opaque managed-facade handle (ep->facade). Each managed transport (picoquic
 * raw, pico_wt, mvfst, proxygen) provides one constant vtable + thin
 * shims; the endpoint never branches on protocol in the accessors. A facade
 * with no clean-close concept supplies a clean_closed that returns false.
 */
typedef struct ep_backend_vtable {
    moq_result_t  (*wait)(void *facade, uint64_t timeout_us);
    void          (*wake)(void *facade);
    moq_version_t (*negotiated_version)(void *facade);
    bool          (*is_fatal)(void *facade);
    uint64_t      (*fatal_code)(void *facade);
    bool          (*clean_closed)(void *facade);
    /* Graceful-drain probe (for moq_endpoint_drain). Returns: 1 = transport
     * outbound is idle enough to stop without truncating queued reliable stream
     * data; 0 = still draining; -1 = the backend cannot prove drain (caller maps
     * to MOQ_ERR_UNSUPPORTED); -2 = called on the network thread (MOQ_ERR_WRONG
     * _STATE). Evaluated only by the facade's own network thread's view, exposed
     * thread-safely. */
    int           (*drain_state)(void *facade);
    moq_result_t  (*stop)(void *facade);
    void          (*destroy)(void *facade);
} ep_backend_vtable_t;

/* drain_state return codes. */
#define EP_DRAIN_UNSUPPORTED  (-1)
#define EP_DRAIN_WRONG_THREAD (-2)
#define EP_DRAIN_PENDING      (0)
#define EP_DRAIN_DONE         (1)

struct moq_endpoint {
    moq_alloc_t alloc;                 /* thread-safe (libc default) */
    moq_transport_protocol_t protocol;

    /* Owned NUL-terminated copies for the facade cfgs. */
    char *host;    size_t host_len;
    char *sni;     size_t sni_len;
    char *path;    size_t path_len;    /* WT path; NULL for RAW_QUIC */
    char *ca_file; size_t ca_file_len; /* NULL = system roots */
    bool  insecure;

    /* The version offer as transport tokens, preference order. The ALPN
     * entries point at the static strings in moq_alpn.h (no ownership);
     * the WT offer is one owned comma-joined string. */
    const char *alpn_offer[MOQ_ENDPOINT_MAX_VERSIONS];
    size_t      alpn_offer_count;
    char       *wt_offer; size_t wt_offer_len;

    /* Selected backend: an opaque managed-facade handle + its vtable. Set by
     * the backend create-fn in connect(); NULL until then / after destroy. */
    void                      *facade;
    const ep_backend_vtable_t *vt;

    /* Internal pump hooks (one slot per kind; see endpoint_internal.h).
     * Mutated under mu; invoked on the network thread under mu. */
    moq_endpoint_hook_fn hook_fn[MOQ_ENDPOINT_HOOK_KIND_COUNT];
    void                *hook_ctx[MOQ_ENDPOINT_HOOK_KIND_COUNT];

    /* Sticky interrupt latch: lone atomic, settable from any thread. */
    atomic_bool interrupted;

    /* Sticky closed latch: mirrors the mutex-guarded closed-ness (app stop() or
     * a CLOSED session pump state) into an atomic so a thread holding a service
     * mutex (e.g. the media sender's s->mu, reachable from a hook under ep->mu)
     * can observe "endpoint closed" WITHOUT taking ep->mu, so it does not invert
     * the hook lock order. Monotonic: set true, never reset. Read with the facade
     * terminal check via moq_endpoint_is_closed_internal(). */
    atomic_bool closed;

    /* Shared app<->network-thread state. */
    pthread_mutex_t mu;
    moq_endpoint_state_t pump_state;   /* refreshed by the pump cycle */
    moq_version_t        negotiated;   /* set when ESTABLISHED is observed */
    bool                 stopped;      /* stop() joined the thread */
    int                  attachments;  /* media services (none yet in v0) */
    ep_task_t *q_head, *q_tail;        /* post() FIFO */
};

/* Pop the head task, or NULL. */
static ep_task_t *ep_pop_task(moq_endpoint_t *ep)
{
    pthread_mutex_lock(&ep->mu);
    ep_task_t *t = ep->q_head;
    if (t) {
        ep->q_head = t->next;
        if (!ep->q_head) ep->q_tail = NULL;
    }
    pthread_mutex_unlock(&ep->mu);
    return t;
}

#if defined(MOQ_SERVICE_HAVE_PQ_THREADED) || \
    defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_MVFST_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED) || \
    defined(MOQ_MEDIA_SENDER_TESTING) || defined(MOQ_MEDIA_RECEIVER_TESTING)
/* Re-queue a task node at the tail without re-allocating it. */
static void ep_requeue_task(moq_endpoint_t *ep, ep_task_t *t)
{
    t->next = NULL;
    pthread_mutex_lock(&ep->mu);
    if (ep->q_tail) ep->q_tail->next = t;
    else            ep->q_head = t;
    ep->q_tail = t;
    pthread_mutex_unlock(&ep->mu);
}

/*
 * Drain the post() tasks queued when this cycle began -- captured tail, FIFO.
 * Calling moq_session_* from a task is legal here (network thread).
 *
 * A task posted via the internal retryable path (t->retryable) that returns
 * MOQ_ERR_WOULD_BLOCK is REQUEUED (the same node, no re-allocation) to retry on
 * a LATER cycle instead of being freed -- e.g. subscriber teardown waiting on a
 * transiently full session queue. Public moq_endpoint_post() tasks are NOT
 * retryable: their return value is ignored and they run exactly once, per the
 * normative one-shot contract in endpoint.h. Bounding the drain to the
 * cycle-start tail keeps a requeued (or concurrently posted) task from being
 * re-run within this same cycle, which would spin: the queues a blocked task
 * waits on do not advance mid-drain.
 */
static void ep_drain_posted(moq_endpoint_t *ep, moq_session_t *session,
                            uint64_t now_us)
{
    pthread_mutex_lock(&ep->mu);
    ep_task_t *stop_at = ep->q_tail;
    pthread_mutex_unlock(&ep->mu);
    while (stop_at) {
        ep_task_t *t = ep_pop_task(ep);
        if (!t) break;                 /* unreachable: stop_at is in the queue */
        bool last = (t == stop_at);
        moq_result_t rc = t->fn(ep, session, now_us, t->ctx);
        if (t->retryable && rc == MOQ_ERR_WOULD_BLOCK)
            ep_requeue_task(ep, t);    /* retry next cycle; node preserved */
        else
            ep->alloc.free(t, sizeof(*t), ep->alloc.ctx);
        if (last) break;
    }
}
#endif

#if defined(MOQ_SERVICE_HAVE_PQ_THREADED) || \
    defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_MVFST_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED)
static moq_version_t ep_facade_negotiated_version(const moq_endpoint_t *ep);
/*
 * One pump cycle, on the facade's network thread: drain accepted post()
 * tasks FIFO (calling moq_session_* is legal here), then refresh the cached
 * endpoint state from the session.
 */
static void ep_pump_cycle(moq_endpoint_t *ep, moq_session_t *session,
                          uint64_t now_us)
{
    ep_drain_posted(ep, session, now_us);

    moq_endpoint_state_t st = MOQ_ENDPOINT_CONNECTING;
    if (session) {
        switch (moq_session_state(session)) {
        case MOQ_SESS_ESTABLISHED: st = MOQ_ENDPOINT_ESTABLISHED; break;
        case MOQ_SESS_DRAINING:    st = MOQ_ENDPOINT_DRAINING; break;
        case MOQ_SESS_CLOSED:      st = MOQ_ENDPOINT_CLOSED; break;
        default:                   st = MOQ_ENDPOINT_CONNECTING; break;
        }
    }
    pthread_mutex_lock(&ep->mu);
    ep->pump_state = st;
    if (st == MOQ_ENDPOINT_CLOSED)
        atomic_store(&ep->closed, true);   /* mirror for ep->mu-free readers */
    if (st == MOQ_ENDPOINT_ESTABLISHED && ep->negotiated == 0)
        ep->negotiated = ep_facade_negotiated_version(ep);
    /* Attached-service hooks run under mu (detach is race-free); the hook
     * contract in endpoint_internal.h forbids re-entering public endpoint
     * APIs from inside. */
    for (int k = 0; k < MOQ_ENDPOINT_HOOK_KIND_COUNT; k++) {
        if (ep->hook_fn[k])
            ep->hook_fn[k](ep, session, now_us, ep->hook_ctx[k]);
    }
    pthread_mutex_unlock(&ep->mu);
}

#endif /* any facade */

/* Drain every queued task with the NULL-session closed marker (§5.4: each
 * accepted task runs exactly once; ctx cleanup is deterministic). Called
 * after the network thread is joined, so nothing races the queue. */
static void ep_drain_terminal(moq_endpoint_t *ep)
{
    for (;;) {
        ep_task_t *t = ep_pop_task(ep);
        if (!t) break;
        (void)t->fn(ep, NULL, 0, t->ctx);
        ep->alloc.free(t, sizeof(*t), ep->alloc.ctx);
    }
}

/* -- Facade dispatch -------------------------------------------------- */

#ifdef MOQ_SERVICE_HAVE_PQ_THREADED
static int ep_pq_pump(moq_pq_threaded_t *t, uint64_t now_us, void *ctx)
{
    moq_endpoint_t *ep = (moq_endpoint_t *)ctx;
    ep_pump_cycle(ep, moq_pq_threaded_session(t), now_us);
    return 0;
}
#endif

#ifdef MOQ_SERVICE_HAVE_PICO_WT_MANAGED
static int ep_wt_pump(moq_pico_wt_managed_t *m, uint64_t now_us, void *ctx)
{
    moq_endpoint_t *ep = (moq_endpoint_t *)ctx;
    ep_pump_cycle(ep, moq_pico_wt_managed_session(m), now_us);
    return 0;
}
#endif

#ifdef MOQ_SERVICE_HAVE_MVFST_MANAGED
static int ep_mvfst_pump(moq_mvfst_managed_t *m, uint64_t now_us, void *ctx)
{
    moq_endpoint_t *ep = (moq_endpoint_t *)ctx;
    ep_pump_cycle(ep, moq_mvfst_managed_session(m), now_us);
    return 0;
}
#endif

#ifdef MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED
static int ep_proxygen_pump(moq_proxygen_wt_managed_t *m, uint64_t now_us,
                            void *ctx)
{
    moq_endpoint_t *ep = (moq_endpoint_t *)ctx;
    ep_pump_cycle(ep, moq_proxygen_wt_managed_session(m), now_us);
    return 0;
}
#endif

#if defined(MOQ_SERVICE_HAVE_PQ_THREADED) || defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED)
/* Install real certificate verification unless explicitly skipped: chain +
 * server-name validation against ep->ca_file (NULL = system roots). The
 * facades' built-in default accepts the peer cert, which is NOT
 * production-safe -- making the safe path the default is this tier's job. */
static int ep_configure_quic(picoquic_quic_t *quic, void *ctx)
{
    moq_endpoint_t *ep = (moq_endpoint_t *)ctx;
    if (ep->insecure) return 0;
    return moq_picoquic_set_cert_verifier(quic, ep->ca_file);
}
#endif

/* -- Per-facade vtable shims ---------------------------------------- *
 * Cast the opaque facade handle to its concrete managed type and forward.
 * Compiled only when the facade is built; a facade with no clean-close
 * concept (raw QUIC) returns false. Adding a backend = one such block. */
#ifdef MOQ_SERVICE_HAVE_PQ_THREADED
static moq_result_t  pq_vt_wait(void *f, uint64_t t) { return moq_pq_threaded_wait((moq_pq_threaded_t *)f, t); }
static void          pq_vt_wake(void *f) { (void)moq_pq_threaded_wake((moq_pq_threaded_t *)f); }
static moq_version_t pq_vt_negver(void *f) { return moq_pq_threaded_negotiated_version((moq_pq_threaded_t *)f); }
static bool          pq_vt_is_fatal(void *f) { return moq_pq_threaded_is_fatal((moq_pq_threaded_t *)f); }
static uint64_t      pq_vt_fatal_code(void *f) { return moq_pq_threaded_fatal_code((moq_pq_threaded_t *)f); }
static bool          pq_vt_clean_closed(void *f) { (void)f; return false; } /* raw: no clean-close accessor */
static int           pq_vt_drain_state(void *f) { return moq_pq_threaded_drain_state((moq_pq_threaded_t *)f); }
static moq_result_t  pq_vt_stop(void *f) { return moq_pq_threaded_stop((moq_pq_threaded_t *)f); }
static void          pq_vt_destroy(void *f) { moq_pq_threaded_destroy((moq_pq_threaded_t *)f); }
static const ep_backend_vtable_t PQ_VT = {
    pq_vt_wait, pq_vt_wake, pq_vt_negver, pq_vt_is_fatal,
    pq_vt_fatal_code, pq_vt_clean_closed, pq_vt_drain_state, pq_vt_stop,
    pq_vt_destroy,
};
#endif

#ifdef MOQ_SERVICE_HAVE_PICO_WT_MANAGED
static moq_result_t  wt_vt_wait(void *f, uint64_t t) { return moq_pico_wt_managed_wait((moq_pico_wt_managed_t *)f, t); }
static void          wt_vt_wake(void *f) { (void)moq_pico_wt_managed_wake((moq_pico_wt_managed_t *)f); }
static moq_version_t wt_vt_negver(void *f) { return moq_pico_wt_managed_negotiated_version((moq_pico_wt_managed_t *)f); }
static bool          wt_vt_is_fatal(void *f) { return moq_pico_wt_managed_is_fatal((moq_pico_wt_managed_t *)f); }
static uint64_t      wt_vt_fatal_code(void *f) { return moq_pico_wt_managed_fatal_code((moq_pico_wt_managed_t *)f); }
static bool          wt_vt_clean_closed(void *f) { return moq_pico_wt_managed_is_closed((moq_pico_wt_managed_t *)f); }
static int           wt_vt_drain_state(void *f) { return moq_pico_wt_managed_drain_state((moq_pico_wt_managed_t *)f); }
static moq_result_t  wt_vt_stop(void *f) { return moq_pico_wt_managed_stop((moq_pico_wt_managed_t *)f); }
static void          wt_vt_destroy(void *f) { moq_pico_wt_managed_destroy((moq_pico_wt_managed_t *)f); }
static const ep_backend_vtable_t WT_VT = {
    wt_vt_wait, wt_vt_wake, wt_vt_negver, wt_vt_is_fatal,
    wt_vt_fatal_code, wt_vt_clean_closed, wt_vt_drain_state, wt_vt_stop,
    wt_vt_destroy,
};
#endif

#ifdef MOQ_SERVICE_HAVE_MVFST_MANAGED
static moq_result_t  mvfst_vt_wait(void *f, uint64_t t) { return moq_mvfst_managed_wait((moq_mvfst_managed_t *)f, t); }
static void          mvfst_vt_wake(void *f) { (void)moq_mvfst_managed_wake((moq_mvfst_managed_t *)f); }
static moq_version_t mvfst_vt_negver(void *f) { return moq_mvfst_managed_negotiated_version((moq_mvfst_managed_t *)f); }
static bool          mvfst_vt_is_fatal(void *f) { return moq_mvfst_managed_is_fatal((moq_mvfst_managed_t *)f); }
static uint64_t      mvfst_vt_fatal_code(void *f) { return moq_mvfst_managed_fatal_code((moq_mvfst_managed_t *)f); }
static bool          mvfst_vt_clean_closed(void *f) { (void)f; return false; } /* raw: no clean-close accessor */
/* mvfst exposes no public outbound-flush probe yet -> drain unsupported (the
 * caller maps this to MOQ_ERR_UNSUPPORTED rather than silently claiming drain). */
static int           mvfst_vt_drain_state(void *f) { (void)f; return EP_DRAIN_UNSUPPORTED; }
static moq_result_t  mvfst_vt_stop(void *f) { return moq_mvfst_managed_stop((moq_mvfst_managed_t *)f); }
static void          mvfst_vt_destroy(void *f) { moq_mvfst_managed_destroy((moq_mvfst_managed_t *)f); }
static const ep_backend_vtable_t MVFST_VT = {
    mvfst_vt_wait, mvfst_vt_wake, mvfst_vt_negver, mvfst_vt_is_fatal,
    mvfst_vt_fatal_code, mvfst_vt_clean_closed, mvfst_vt_drain_state,
    mvfst_vt_stop, mvfst_vt_destroy,
};
#endif

#ifdef MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED
static moq_result_t  proxygen_vt_wait(void *f, uint64_t t) { return moq_proxygen_wt_managed_wait((moq_proxygen_wt_managed_t *)f, t); }
static void          proxygen_vt_wake(void *f) { (void)moq_proxygen_wt_managed_wake((moq_proxygen_wt_managed_t *)f); }
static moq_version_t proxygen_vt_negver(void *f) { return moq_proxygen_wt_managed_negotiated_version((moq_proxygen_wt_managed_t *)f); }
static bool          proxygen_vt_is_fatal(void *f) { return moq_proxygen_wt_managed_is_fatal((moq_proxygen_wt_managed_t *)f); }
static uint64_t      proxygen_vt_fatal_code(void *f) { return moq_proxygen_wt_managed_fatal_code((moq_proxygen_wt_managed_t *)f); }
static bool          proxygen_vt_clean_closed(void *f) { return moq_proxygen_wt_managed_is_closed((moq_proxygen_wt_managed_t *)f); } /* WT clean close */
/* proxygen managed exposes no public outbound-flush probe yet -> unsupported. */
static int           proxygen_vt_drain_state(void *f) { (void)f; return EP_DRAIN_UNSUPPORTED; }
static moq_result_t  proxygen_vt_stop(void *f) { return moq_proxygen_wt_managed_stop((moq_proxygen_wt_managed_t *)f); }
static void          proxygen_vt_destroy(void *f) { moq_proxygen_wt_managed_destroy((moq_proxygen_wt_managed_t *)f); }
static const ep_backend_vtable_t PROXYGEN_VT = {
    proxygen_vt_wait, proxygen_vt_wake, proxygen_vt_negver, proxygen_vt_is_fatal,
    proxygen_vt_fatal_code, proxygen_vt_clean_closed, proxygen_vt_drain_state,
    proxygen_vt_stop, proxygen_vt_destroy,
};
#endif

/* -- Backend dispatch (over ep->vt / ep->facade) -------------------- */

static moq_result_t ep_facade_wait(moq_endpoint_t *ep, uint64_t timeout_us)
{
    if (ep->vt && ep->facade) return ep->vt->wait(ep->facade, timeout_us);
    return MOQ_ERR_CLOSED;
}

static void ep_facade_wake(moq_endpoint_t *ep)
{
    if (ep->vt && ep->facade) ep->vt->wake(ep->facade);
}

/* Public (<moq/endpoint.h>): best-effort, non-allocating pump nudge. A bare
 * facade wake runs another pump cycle -- and thus every attached hook --
 * without queuing a task; blocked waiters return because the cycle marks
 * (coalesced, level-retained) activity, never by direct notification. Also
 * used internally by attached services to schedule reconciliation of
 * already-recorded state with no fallible allocation. */
void moq_endpoint_wake(moq_endpoint_t *ep)
{
    if (!ep) return;
    ep_facade_wake(ep);
}

#if defined(MOQ_SERVICE_HAVE_PQ_THREADED) || \
    defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_MVFST_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED)
static moq_version_t ep_facade_negotiated_version(const moq_endpoint_t *ep)
{
    if (ep->vt && ep->facade) return ep->vt->negotiated_version(ep->facade);
    return (moq_version_t)0;
}
#endif /* any facade */

static bool ep_facade_is_fatal(const moq_endpoint_t *ep)
{
    return (ep->vt && ep->facade) ? ep->vt->is_fatal(ep->facade) : false;
}

static uint64_t ep_facade_fatal_code(const moq_endpoint_t *ep)
{
    return (ep->vt && ep->facade) ? ep->vt->fatal_code(ep->facade) : 0;
}

/* A clean facade-level close (WT CLOSE capsule / GOAWAY drain); facades with no
 * clean-close concept (raw QUIC) supply a shim that returns false. */
static bool ep_facade_clean_closed(const moq_endpoint_t *ep)
{
    return (ep->vt && ep->facade) ? ep->vt->clean_closed(ep->facade) : false;
}

static int ep_facade_drain_state(const moq_endpoint_t *ep)
{
    return (ep->vt && ep->facade) ? ep->vt->drain_state(ep->facade)
                                  : EP_DRAIN_UNSUPPORTED;
}

static moq_result_t ep_facade_stop(moq_endpoint_t *ep)
{
    return (ep->vt && ep->facade) ? ep->vt->stop(ep->facade) : MOQ_OK;
}

static void ep_facade_destroy(moq_endpoint_t *ep)
{
    if (ep->vt && ep->facade) {
        ep->vt->destroy(ep->facade);
        ep->facade = NULL;
    }
}

/* True once the facade is terminal (no further pump cycle will run); the
 * caller pairs this with its own under-mutex read of ep->stopped. */
static bool ep_terminal(const moq_endpoint_t *ep)
{
    return ep_facade_is_fatal(ep) || ep_facade_clean_closed(ep);
}

/* -- Construction ------------------------------------------------------ */

#if defined(MOQ_SERVICE_HAVE_PQ_THREADED) || \
    defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_MVFST_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED)
static char *ep_strdup_bytes(const moq_alloc_t *a, moq_bytes_t b, size_t *len)
{
    char *p = (char *)a->alloc(b.len + 1, a->ctx);
    if (!p) return NULL;
    if (b.len > 0) memcpy(p, b.data, b.len);
    p[b.len] = '\0';
    *len = b.len + 1;
    return p;
}

#endif /* any facade */

static void ep_free_strings(moq_endpoint_t *ep)
{
    if (ep->host)    ep->alloc.free(ep->host, ep->host_len, ep->alloc.ctx);
    if (ep->sni)     ep->alloc.free(ep->sni, ep->sni_len, ep->alloc.ctx);
    if (ep->path)    ep->alloc.free(ep->path, ep->path_len, ep->alloc.ctx);
    if (ep->ca_file) ep->alloc.free(ep->ca_file, ep->ca_file_len, ep->alloc.ctx);
    if (ep->wt_offer) ep->alloc.free(ep->wt_offer, ep->wt_offer_len, ep->alloc.ctx);
    ep->host = ep->sni = ep->path = ep->ca_file = NULL;
    ep->wt_offer = NULL;
}

/* -- Backend registry ----------------------------------------------- *
 * Each create-fn builds a managed-facade cfg from the already-populated ep
 * (host/sni/path/ALPN offer + the backend's pump hook), creates the facade,
 * and on success stores it as ep->facade + ep->vt. The registry is a static
 * table of {protocol, backend, create-fn} rows in preference order; connect()
 * selects the row matching the resolved (protocol, backend). Adding a backend
 * = one create-fn + one row (+ its build define) -- no new dispatch sites. */
#if defined(MOQ_SERVICE_HAVE_PQ_THREADED) || \
    defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_MVFST_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED)

typedef moq_result_t (*ep_backend_create_fn)(moq_endpoint_t *ep,
                                             const moq_endpoint_resolved_t *r,
                                             const moq_endpoint_cfg_t *cfg);

#ifdef MOQ_SERVICE_HAVE_PQ_THREADED
static moq_result_t ep_create_pq(moq_endpoint_t *ep,
                                 const moq_endpoint_resolved_t *r,
                                 const moq_endpoint_cfg_t *cfg)
{
    moq_pq_threaded_cfg_t fc;
    moq_pq_threaded_cfg_init_sized(&fc, sizeof(fc));
    fc.alloc = &ep->alloc;
    fc.perspective = MOQ_PERSPECTIVE_CLIENT;
    /* Grant the peer request capacity so a relay/peer can SUBSCRIBE to
     * tracks this endpoint publishes (a media sender is a client publisher;
     * without this its tracks are unreachable). Inert for a pure subscriber
     * peer that issues no requests. */
    fc.send_request_capacity = true;
    fc.initial_request_capacity = 64;
    fc.host = ep->host;
    fc.sni = ep->sni;                   /* may differ from host; the verifier
                                           checks this name */
    fc.alpn_list = ep->alpn_offer;
    fc.alpn_count = ep->alpn_offer_count;
    fc.port = (int)r->url.port;
    fc.insecure_skip_verify = cfg->insecure_skip_verify;
    fc.configure_quic = ep_configure_quic;
    fc.configure_quic_ctx = ep;
    fc.on_pump = ep_pq_pump;
    fc.on_pump_ctx = ep;
    moq_pq_threaded_t *fac = NULL;
    moq_result_t crc = moq_pq_threaded_create(&fc, &fac);
    if (crc < 0) return crc;
    ep->facade = fac;
    ep->vt = &PQ_VT;
    return MOQ_OK;
}
#endif

#ifdef MOQ_SERVICE_HAVE_PICO_WT_MANAGED
static moq_result_t ep_create_wt(moq_endpoint_t *ep,
                                 const moq_endpoint_resolved_t *r,
                                 const moq_endpoint_cfg_t *cfg)
{
    moq_pico_wt_managed_cfg_t fc;
    moq_pico_wt_managed_cfg_init(&fc);
    fc.alloc = &ep->alloc;
    fc.perspective = MOQ_PERSPECTIVE_CLIENT;
    /* See the RAW_QUIC branch: grant the peer request capacity so it can
     * subscribe to published tracks. */
    fc.send_request_capacity = true;
    fc.initial_request_capacity = 64;
    fc.host = ep->host;
    fc.sni = ep->sni;
    fc.path = ep->path;
    fc.port = (int)r->url.port;
    fc.insecure_skip_verify = cfg->insecure_skip_verify;
    fc.wt_protocols = ep->wt_offer;
    fc.configure_quic = ep_configure_quic;
    fc.configure_quic_ctx = ep;
    fc.on_pump = ep_wt_pump;
    fc.on_pump_ctx = ep;
    moq_pico_wt_managed_t *fac = NULL;
    moq_result_t crc = moq_pico_wt_managed_create(&fc, &fac);
    if (crc < 0) return crc;
    ep->facade = fac;
    ep->vt = &WT_VT;
    return MOQ_OK;
}
#endif

#ifdef MOQ_SERVICE_HAVE_MVFST_MANAGED
static moq_result_t ep_create_mvfst(moq_endpoint_t *ep,
                                    const moq_endpoint_resolved_t *r,
                                    const moq_endpoint_cfg_t *cfg)
{
    /* Exact-version only: the managed mvfst adapter creates the MoQ session
     * before the ALPN handshake, so it cannot negotiate a multi-version offer
     * (see moq/mvfst.h). The endpoint offers in preference order; require a
     * single offered version and use it as the one ALPN. AUTO (the default
     * newest-first offer) therefore needs an EXACT version policy for mvfst. */
    if (ep->alpn_offer_count != 1)
        return MOQ_ERR_UNSUPPORTED;

    moq_mvfst_managed_cfg_t fc;
    /* Full current struct: this endpoint sets appended fields (sni/alpn_*), so
     * it must use the sized initializer -- the pointer-only init stamps only the
     * frozen prefix and would leave SNI/ALPN disabled. */
    moq_mvfst_managed_cfg_init_sized(&fc, sizeof(fc));
    fc.alloc = &ep->alloc;
    fc.perspective = MOQ_PERSPECTIVE_CLIENT;
    /* See the picoquic branch: grant the peer request capacity so it can
     * subscribe to tracks this endpoint publishes. */
    fc.send_request_capacity = true;
    fc.initial_request_capacity = 64;
    fc.host = ep->host;
    fc.port = (int)r->url.port;
    fc.sni = ep->sni;                   /* explicit SNI or the URL host */
    fc.alpn_list = ep->alpn_offer;      /* exactly one entry */
    fc.alpn_count = ep->alpn_offer_count;
    fc.insecure_skip_verify = cfg->insecure_skip_verify;
    /* mvfst has no configure_quic hook; it verifies internally. Mirror the
     * picoquic verifier policy: skip when insecure, else trust ep->ca_file
     * (NULL = system roots), with the SNI as the checked identity. */
    if (!cfg->insecure_skip_verify && ep->ca_file)
        fc.cert_path = ep->ca_file;
    fc.on_pump = ep_mvfst_pump;
    fc.user_ctx = ep;
    moq_mvfst_managed_t *fac = NULL;
    moq_result_t crc = moq_mvfst_managed_create(&fc, &fac);
    if (crc < 0) return crc;
    ep->facade = fac;
    ep->vt = &MVFST_VT;
    return MOQ_OK;
}
#endif

#ifdef MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED
static moq_result_t ep_create_proxygen(moq_endpoint_t *ep,
                                       const moq_endpoint_resolved_t *r,
                                       const moq_endpoint_cfg_t *cfg)
{
    moq_proxygen_wt_managed_cfg_t fc;
    moq_proxygen_wt_managed_cfg_init(&fc);
    fc.alloc = &ep->alloc;
    fc.perspective = MOQ_PERSPECTIVE_CLIENT;
    /* See the picoquic branch: grant the peer request capacity so it can
     * subscribe to tracks this endpoint publishes. */
    fc.send_request_capacity = true;
    fc.initial_request_capacity = 64;
    fc.host = ep->host;
    fc.port = (int)r->url.port;
    fc.sni = ep->sni;
    fc.path = ep->path;
    fc.insecure_skip_verify = cfg->insecure_skip_verify;
    /* proxygen verifies internally; mirror the verifier policy: skip when
     * insecure, else trust ep->ca_file (NULL = system roots). */
    if (!cfg->insecure_skip_verify && ep->ca_file)
        fc.ca_file = ep->ca_file;
    /* proxygen takes the WT-protocol token vector directly (it builds the
     * WT-Available-Protocols header itself): pass the parsed offer array,
     * NOT the pico_wt serialized wt_offer string. The session is created
     * after the CONNECT response, so the full multi-version offer (AUTO) is
     * negotiated. */
    fc.alpn_list = ep->alpn_offer;
    fc.alpn_count = ep->alpn_offer_count;
    fc.on_pump = ep_proxygen_pump;
    fc.user_ctx = ep;
    moq_proxygen_wt_managed_t *fac = NULL;
    moq_result_t crc = moq_proxygen_wt_managed_create(&fc, &fac);
    if (crc < 0) return crc;
    ep->facade = fac;
    ep->vt = &PROXYGEN_VT;
    return MOQ_OK;
}
#endif

typedef struct ep_backend_row {
    moq_transport_protocol_t protocol;
    moq_transport_backend_t  backend;
    ep_backend_create_fn     create;
} ep_backend_row_t;

/* Preference order within a protocol (first available wins for AUTO). */
static const ep_backend_row_t EP_BACKENDS[] = {
#ifdef MOQ_SERVICE_HAVE_PQ_THREADED
    { MOQ_TRANSPORT_PROTOCOL_RAW_QUIC,     MOQ_TRANSPORT_BACKEND_PICOQUIC, ep_create_pq },
#endif
#ifdef MOQ_SERVICE_HAVE_PICO_WT_MANAGED
    { MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT, MOQ_TRANSPORT_BACKEND_PICOQUIC, ep_create_wt },
#endif
#ifdef MOQ_SERVICE_HAVE_MVFST_MANAGED
    /* After picoquic: AUTO RAW_QUIC still picks picoquic; mvfst is reached
     * only by an explicit backend = MVFST. */
    { MOQ_TRANSPORT_PROTOCOL_RAW_QUIC,     MOQ_TRANSPORT_BACKEND_MVFST,    ep_create_mvfst },
#endif
#ifdef MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED
    /* After pico_wt: AUTO WEBTRANSPORT still picks pico_wt; proxygen is
     * reached only by an explicit backend = PROXYGEN. */
    { MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT, MOQ_TRANSPORT_BACKEND_PROXYGEN, ep_create_proxygen },
#endif
};

#endif /* any facade */

void moq_endpoint_cfg_init(moq_endpoint_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_endpoint_cfg_t);
    cfg->perspective = MOQ_PERSPECTIVE_CLIENT;
}

moq_result_t moq_endpoint_connect(const moq_endpoint_cfg_t *cfg,
                                  moq_endpoint_t **out)
{
    if (!cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;

    moq_endpoint_resolved_t r;
    moq_result_t rc = moq_endpoint_resolve_cfg(cfg, &r);
    if (rc < 0) return rc;


#if !defined(MOQ_SERVICE_HAVE_PQ_THREADED) && \
    !defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED) && \
    !defined(MOQ_SERVICE_HAVE_MVFST_MANAGED) && \
    !defined(MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED)
    return MOQ_ERR_UNSUPPORTED;   /* no managed facade in this build */
#else
    if (r.protocol == MOQ_TRANSPORT_PROTOCOL_RAW_QUIC) {
#if !defined(MOQ_SERVICE_HAVE_PQ_THREADED) && \
    !defined(MOQ_SERVICE_HAVE_MVFST_MANAGED)
        return MOQ_ERR_UNSUPPORTED;   /* no raw-QUIC facade built */
#endif
    } else {
#if !defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED) && \
    !defined(MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED)
        return MOQ_ERR_UNSUPPORTED;   /* no WebTransport facade built */
#endif
    }

    const moq_alloc_t *alloc = cfg->alloc ? cfg->alloc : moq_alloc_default();
    if (!alloc->alloc || !alloc->free || !alloc->realloc) return MOQ_ERR_INVAL;

    moq_endpoint_t *ep = (moq_endpoint_t *)alloc->alloc(sizeof(*ep), alloc->ctx);
    if (!ep) return MOQ_ERR_NOMEM;
    memset(ep, 0, sizeof(*ep));
    ep->alloc = *alloc;
    ep->protocol = r.protocol;
    ep->insecure = cfg->insecure_skip_verify;
    atomic_init(&ep->interrupted, false);
    atomic_init(&ep->closed, false);
    pthread_mutex_init(&ep->mu, NULL);
    ep->pump_state = MOQ_ENDPOINT_CONNECTING;

    ep->host = ep_strdup_bytes(alloc, r.url.host, &ep->host_len);
    ep->sni  = ep_strdup_bytes(alloc, r.sni, &ep->sni_len);
    bool oom = !ep->host || !ep->sni;
    if (!oom && r.protocol == MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT) {
        ep->path = ep_strdup_bytes(alloc, r.wt_path, &ep->path_len);
        oom = oom || !ep->path;
    }
    if (!oom && cfg->ca_file.len > 0) {
        ep->ca_file = ep_strdup_bytes(alloc, cfg->ca_file, &ep->ca_file_len);
        oom = oom || !ep->ca_file;
    }
    /* Convert the resolved version offer into transport tokens, preference
     * order preserved. Every resolved version is build-supported, so every
     * token maps. */
    if (!oom) {
        for (size_t i = 0; i < r.version_count; i++) {
            const char *tok = moq_alpn_for_version(r.versions[i]);
            if (!tok) { oom = true; break; }   /* unreachable: validated */
            ep->alpn_offer[i] = tok;
        }
        ep->alpn_offer_count = r.version_count;
    }
    if (!oom && r.protocol == MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT) {
        /* WT-Available-Protocols string. The WebTransport-over-HTTP/3
         * subprotocol header is an RFC 8941 Structured Fields list whose
         * members are quoted strings, so each token is double-quoted:
         * ("moqt-18", "moqt-16"). picoquic's own receiver strips the quotes
         * (picowt_select_wt_protocol), but a strict structured-fields parser
         * (e.g. moxygen/proxygen relays) rejects bare tokens outright. Each
         * token needs 4 extra bytes: two quotes plus the ", " separator. */
        size_t need = 1;
        for (size_t i = 0; i < ep->alpn_offer_count; i++)
            need += strlen(ep->alpn_offer[i]) + 4;
        ep->wt_offer = (char *)alloc->alloc(need, alloc->ctx);
        if (!ep->wt_offer) {
            oom = true;
        } else {
            ep->wt_offer_len = need;
            size_t off = 0;
            for (size_t i = 0; i < ep->alpn_offer_count; i++) {
                if (i > 0) { ep->wt_offer[off++] = ','; ep->wt_offer[off++] = ' '; }
                size_t n = strlen(ep->alpn_offer[i]);
                ep->wt_offer[off++] = '"';
                memcpy(ep->wt_offer + off, ep->alpn_offer[i], n);
                off += n;
                ep->wt_offer[off++] = '"';
            }
            ep->wt_offer[off] = '\0';
        }
    }
    if (oom) {
        ep_free_strings(ep);
        pthread_mutex_destroy(&ep->mu);
        alloc->free(ep, sizeof(*ep), alloc->ctx);
        return MOQ_ERR_NOMEM;
    }

    /* Select the backend for the resolved (protocol, backend) and let its
     * create-fn build the facade. The resolver already rejected any backend
     * with no compiled-in row, so a miss here is unreachable. */
    moq_result_t crc = MOQ_ERR_UNSUPPORTED;
    for (size_t i = 0; i < sizeof(EP_BACKENDS) / sizeof(EP_BACKENDS[0]); i++) {
        if (EP_BACKENDS[i].protocol == r.protocol &&
            EP_BACKENDS[i].backend  == r.backend) {
            crc = EP_BACKENDS[i].create(ep, &r, cfg);
            break;
        }
    }
    if (crc < 0) {
        ep_free_strings(ep);
        pthread_mutex_destroy(&ep->mu);
        alloc->free(ep, sizeof(*ep), alloc->ctx);
        return crc;
    }

    *out = ep;
    return MOQ_OK;
#endif /* any facade available */
}

/* -- Lifecycle ---------------------------------------------------------- */

moq_result_t moq_endpoint_attach_hook(moq_endpoint_t *ep,
                                      moq_endpoint_hook_kind_t kind,
                                      moq_endpoint_hook_fn fn, void *ctx)
{
    if (!ep || !fn || kind < 0 || kind >= MOQ_ENDPOINT_HOOK_KIND_COUNT)
        return MOQ_ERR_INVAL;
    pthread_mutex_lock(&ep->mu);
    if (ep->stopped || ep->hook_fn[kind]) {
        pthread_mutex_unlock(&ep->mu);
        return MOQ_ERR_WRONG_STATE;
    }
    /* A terminal endpoint will never pump again: attaching would only pin
     * the attachment count (blocking stop) on a service that can never
     * run. Same gate as post(). */
    if (ep_terminal(ep)) {
        pthread_mutex_unlock(&ep->mu);
        return MOQ_ERR_CLOSED;
    }
    ep->hook_fn[kind] = fn;
    ep->hook_ctx[kind] = ctx;
    ep->attachments++;
    pthread_mutex_unlock(&ep->mu);
    return MOQ_OK;
}

bool moq_endpoint_interrupted_internal(const moq_endpoint_t *ep)
{
    return ep && atomic_load(&((moq_endpoint_t *)(uintptr_t)ep)->interrupted);
}

/* Closed observation that does NOT take ep->mu, for a thread holding a service
 * mutex (the hook lock order is ep->mu -> s->mu, so an s->mu holder reachable
 * from a hook MUST NOT take ep->mu). Mirrors moq_endpoint_is_closed()'s
 * determination without ep->mu: the `closed` atomic captures the ep->mu-guarded
 * part (app stop() / a CLOSED session pump state), and ep_terminal() (facade
 * fatal / clean-close) is evaluated outside ep->mu (as moq_endpoint_state() does
 * too). NOTE: ep_terminal() may take a facade-internal lock, but never ep->mu,
 * so it cannot recreate the endpoint-mutex inversion this guards against. */
bool moq_endpoint_is_closed_internal(const moq_endpoint_t *ep)
{
    if (!ep) return true;
    if (atomic_load(&((moq_endpoint_t *)(uintptr_t)ep)->closed)) return true;
    return ep_terminal(ep);
}

bool moq_endpoint_is_fatal_internal(const moq_endpoint_t *ep)
{
    if (!ep) return false;
    return ep_facade_is_fatal(ep);
}

uint64_t moq_endpoint_fatal_code_internal(const moq_endpoint_t *ep)
{
    if (!ep) return 0;
    return ep_facade_fatal_code(ep);
}

void moq_endpoint_detach_hook(moq_endpoint_t *ep,
                              moq_endpoint_hook_kind_t kind, void *ctx)
{
    if (!ep || kind < 0 || kind >= MOQ_ENDPOINT_HOOK_KIND_COUNT) return;
    pthread_mutex_lock(&ep->mu);
    if (ep->hook_fn[kind] && ep->hook_ctx[kind] == ctx) {
        ep->hook_fn[kind] = NULL;
        ep->hook_ctx[kind] = NULL;
        ep->attachments--;
    }
    pthread_mutex_unlock(&ep->mu);
}

moq_result_t moq_endpoint_stop(moq_endpoint_t *ep)
{
    if (!ep) return MOQ_ERR_INVAL;

    pthread_mutex_lock(&ep->mu);
    int attachments = ep->attachments;
    bool already = ep->stopped;
    pthread_mutex_unlock(&ep->mu);
    if (attachments > 0) return MOQ_ERR_WRONG_STATE;
    if (already) return MOQ_OK;                       /* idempotent */

    moq_result_t rc = ep_facade_stop(ep);
    if (rc == MOQ_ERR_WRONG_STATE) return rc;         /* called from on_pump */

    pthread_mutex_lock(&ep->mu);
    ep->stopped = true;
    atomic_store(&ep->closed, true);   /* mirror for ep->mu-free readers */
    pthread_mutex_unlock(&ep->mu);

    /* The network thread is joined: drain accepted tasks with the
     * NULL-session closed marker (exactly-once; ctx cleanup deterministic). */
    ep_drain_terminal(ep);
    return MOQ_OK;
}

void moq_endpoint_destroy(moq_endpoint_t *ep)
{
    if (!ep) return;
    pthread_mutex_lock(&ep->mu);
    bool stopped = ep->stopped;
    pthread_mutex_unlock(&ep->mu);
    if (!stopped) {
        /* Programming error (§5.3): debug asserts; release force-stops
         * (joins the thread) so no live thread is ever aimed at freed
         * memory, then proceeds with the destroy. */
        assert(!"moq_endpoint_destroy called before moq_endpoint_stop");
        (void)ep_facade_stop(ep);
        pthread_mutex_lock(&ep->mu);
        ep->stopped = true;
        atomic_store(&ep->closed, true);   /* mirror for ep->mu-free readers */
        pthread_mutex_unlock(&ep->mu);
        ep_drain_terminal(ep);
    }
    ep_facade_destroy(ep);
    ep_free_strings(ep);
    pthread_mutex_destroy(&ep->mu);
    moq_alloc_t alloc = ep->alloc;
    alloc.free(ep, sizeof(*ep), alloc.ctx);
}

moq_result_t moq_endpoint_wait(moq_endpoint_t *ep, uint64_t timeout_us)
{
    if (!ep) return MOQ_ERR_INVAL;
    /* The latch wins over everything, including terminal states: while set,
     * every blocking call returns immediately (§5.3), with no re-block race
     * (set_interrupted wakes the facade, so a concurrent waiter re-checks). */
    if (atomic_load(&ep->interrupted)) return MOQ_ERR_INTERRUPTED;
    moq_result_t rc = ep_facade_wait(ep, timeout_us);
    if (atomic_load(&ep->interrupted)) return MOQ_ERR_INTERRUPTED;
    return rc;   /* MOQ_OK (wake) / MOQ_DONE (timeout) / MOQ_ERR_CLOSED */
}

void moq_endpoint_set_interrupted(moq_endpoint_t *ep, bool interrupted)
{
    if (!ep) return;
    atomic_store(&ep->interrupted, interrupted);
    if (interrupted)
        ep_facade_wake(ep);   /* break a blocked wait; rc irrelevant */
}

#define EP_DRAIN_POLL_SLICE_US 10000u   /* 10 ms re-check cadence */

static uint64_t ep_mono_now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

moq_result_t moq_endpoint_drain(moq_endpoint_t *ep, uint64_t timeout_us)
{
    if (!ep) return MOQ_ERR_INVAL;

    /* The sticky interrupt latch wins over everything (§5.3): a blocking
     * endpoint call returns immediately while it is set, ahead of any backend
     * probe. */
    if (atomic_load(&ep->interrupted)) return MOQ_ERR_INTERRUPTED;

    /* App-thread only, and only backends that can actually prove drain. The
     * network thread drives the flush, so calling this from a hook would
     * deadlock the very progress it waits on. */
    int ds = ep_facade_drain_state(ep);
    if (ds == EP_DRAIN_UNSUPPORTED)  return MOQ_ERR_UNSUPPORTED;
    if (ds == EP_DRAIN_WRONG_THREAD) return MOQ_ERR_WRONG_STATE;

    uint64_t start = ep_mono_now_us();
    for (;;) {
        if (atomic_load(&ep->interrupted)) return MOQ_ERR_INTERRUPTED;

        /* Terminal close during the drain is observable state, not success: the
         * caller asked to flush BEFORE stopping, so a close mid-drain means the
         * tail may have been truncated -- report CLOSED even if a (possibly
         * stale) backlog snapshot now reads empty. */
        if (moq_endpoint_is_closed(ep)) return MOQ_ERR_CLOSED;

        ds = ep_facade_drain_state(ep);
        if (ds == EP_DRAIN_WRONG_THREAD) return MOQ_ERR_WRONG_STATE;
        if (ds == EP_DRAIN_DONE) return MOQ_OK;

        uint64_t elapsed = ep_mono_now_us() - start;
        if (elapsed >= timeout_us) return MOQ_DONE;
        uint64_t remaining = timeout_us - elapsed;
        uint64_t slice = remaining < EP_DRAIN_POLL_SLICE_US
                       ? remaining : EP_DRAIN_POLL_SLICE_US;
        /* Actively kick the network thread so it makes send progress and
         * re-evaluates the drain state -- do not rely on autonomous packet-loop
         * wakeups alone (an idle loop with a pending FIN would never re-probe).
         * Then wait a bounded slice and re-check at the top. */
        ep_facade_wake(ep);
        moq_result_t wr = moq_endpoint_wait(ep, slice);
        if (wr == MOQ_ERR_INTERRUPTED) return MOQ_ERR_INTERRUPTED;
        /* MOQ_OK/MOQ_DONE/MOQ_ERR_CLOSED: loop; closed is handled at the top. */
    }
}

/* -- State -------------------------------------------------------------- */

moq_endpoint_state_t moq_endpoint_state(const moq_endpoint_t *ep)
{
    if (!ep) return MOQ_ENDPOINT_CLOSED;
    moq_endpoint_t *mep = (moq_endpoint_t *)ep;
    pthread_mutex_lock(&mep->mu);
    moq_endpoint_state_t st = ep->stopped ? MOQ_ENDPOINT_CLOSED
                                          : ep->pump_state;
    pthread_mutex_unlock(&mep->mu);
    if (st != MOQ_ENDPOINT_CLOSED &&
        (ep_facade_is_fatal(ep) || ep_facade_clean_closed(ep)))
        st = MOQ_ENDPOINT_CLOSED;
    return st;
}

bool moq_endpoint_is_closed(const moq_endpoint_t *ep)
{
    if (!ep) return true;
    return moq_endpoint_state(ep) == MOQ_ENDPOINT_CLOSED;
}

bool moq_endpoint_is_fatal(const moq_endpoint_t *ep)
{
    if (!ep) return false;
    return ep_facade_is_fatal(ep);
}

uint64_t moq_endpoint_fatal_code(const moq_endpoint_t *ep)
{
    if (!ep) return 0;
    return ep_facade_fatal_code(ep);
}

moq_version_t moq_endpoint_negotiated_version(const moq_endpoint_t *ep)
{
    if (!ep) return (moq_version_t)0;
    moq_endpoint_t *mep = (moq_endpoint_t *)ep;
    pthread_mutex_lock(&mep->mu);
    moq_version_t v = ep->negotiated;
    pthread_mutex_unlock(&mep->mu);
    return v;
}

/* -- post() executor ------------------------------------------------------ */

static moq_result_t ep_post_impl(moq_endpoint_t *ep, moq_endpoint_task_fn fn,
                                 void *ctx, bool retryable)
{
    if (!ep || !fn) return MOQ_ERR_INVAL;

    pthread_mutex_lock(&ep->mu);
    bool stopped = ep->stopped;
    pthread_mutex_unlock(&ep->mu);
    if (stopped || ep_terminal(ep))
        return MOQ_ERR_CLOSED;        /* never runs fn; caller keeps ctx */

    ep_task_t *t = (ep_task_t *)ep->alloc.alloc(sizeof(*t), ep->alloc.ctx);
    if (!t) return MOQ_ERR_NOMEM;
    t->next = NULL;
    t->fn = fn;
    t->ctx = ctx;
    t->retryable = retryable;

    pthread_mutex_lock(&ep->mu);
    if (ep->stopped) {
        /* stop() finished its terminal drain while we allocated. The task
         * was never accepted into the queue, and an accepted task only ever
         * runs on the network thread (or its post-join drain) -- never
         * inline on the caller. So this is the post-after-terminal case:
         * free the node, never run fn; the caller still owns ctx. */
        pthread_mutex_unlock(&ep->mu);
        ep->alloc.free(t, sizeof(*t), ep->alloc.ctx);
        return MOQ_ERR_CLOSED;
    }
    if (ep->q_tail) ep->q_tail->next = t;
    else            ep->q_head = t;
    ep->q_tail = t;
    pthread_mutex_unlock(&ep->mu);

    /* Wake the network thread so the task runs promptly. A terminal wake is
     * harmless: stop()'s drain owns any task the pump no longer reaches. */
    ep_facade_wake(ep);
    return MOQ_OK;
}

moq_result_t moq_endpoint_post(moq_endpoint_t *ep,
                               moq_endpoint_task_fn fn, void *ctx)
{
    /* Public path: exactly-once. The task's return value is ignored and the
     * node is freed after the single run (see the contract in endpoint.h). */
    return ep_post_impl(ep, fn, ctx, /*retryable=*/false);
}

moq_result_t moq_endpoint_post_retryable(moq_endpoint_t *ep,
                                         moq_endpoint_task_fn fn, void *ctx)
{
    /* Internal path: a task returning MOQ_ERR_WOULD_BLOCK is requeued (same
     * node) to retry on a later cycle; any other return frees the node. The
     * terminal drain still runs it once with session == NULL (no requeue), so
     * the task must complete + free ctx on the closed marker. Used by media
     * receiver teardown for allocation-free UNSUBSCRIBE retry under transient
     * session-queue backpressure -- NOT part of the public post() contract. */
    return ep_post_impl(ep, fn, ctx, /*retryable=*/true);
}

#if defined(MOQ_MEDIA_SENDER_TESTING) || defined(MOQ_MEDIA_RECEIVER_TESTING)
/* -- Bare-endpoint test seam (not built into the shipping library) ------ *
 * A "bare" endpoint carries only the mutex + atomics + post() queue, with NULL
 * vt/facade (ep_terminal() returns false). Two uses:
 *  - the media-sender lock-inversion regression holds ep->mu and exercises the
 *    sender's BLOCK_TIMEOUT closed-check without a real transport/network thread;
 *  - the media-receiver teardown-retry regression posts the real teardown task
 *    and drives ep_drain_posted() across cycles to exercise requeue-on-
 *    WOULD_BLOCK. ep_drain_posted touches only the queue + the task fn (never
 *    vt/facade), so it is safe on a bare endpoint even with a live session. */

moq_endpoint_t *moq_endpoint_test_make_bare(void)
{
    const moq_alloc_t *alloc = moq_alloc_default();
    moq_endpoint_t *ep = (moq_endpoint_t *)alloc->alloc(sizeof(*ep), alloc->ctx);
    if (!ep) return NULL;
    memset(ep, 0, sizeof(*ep));
    ep->alloc = *alloc;
    pthread_mutex_init(&ep->mu, NULL);
    atomic_init(&ep->interrupted, false);
    atomic_init(&ep->closed, false);
    ep->pump_state = MOQ_ENDPOINT_CONNECTING;
    return ep;
}

void moq_endpoint_test_free_bare(moq_endpoint_t *ep)
{
    if (!ep) return;
    pthread_mutex_destroy(&ep->mu);
    moq_alloc_t alloc = ep->alloc;
    alloc.free(ep, sizeof(*ep), alloc.ctx);
}

void moq_endpoint_test_lock_mu(moq_endpoint_t *ep)   { pthread_mutex_lock(&ep->mu); }
void moq_endpoint_test_unlock_mu(moq_endpoint_t *ep) { pthread_mutex_unlock(&ep->mu); }

/* Run one cycle's task drain (the real ep_pump_cycle drain step, including
 * requeue-on-WOULD_BLOCK) against a caller-supplied session. */
void moq_endpoint_test_drain_posted(moq_endpoint_t *ep, moq_session_t *session,
                                    uint64_t now_us)
{
    ep_drain_posted(ep, session, now_us);
}

/* Number of tasks currently queued (e.g. to assert a WOULD_BLOCK task was
 * requeued rather than dropped). */
size_t moq_endpoint_test_task_count(moq_endpoint_t *ep)
{
    size_t n = 0;
    pthread_mutex_lock(&ep->mu);
    for (ep_task_t *t = ep->q_head; t; t = t->next) n++;
    pthread_mutex_unlock(&ep->mu);
    return n;
}
#endif /* MOQ_MEDIA_SENDER_TESTING || MOQ_MEDIA_RECEIVER_TESTING */
