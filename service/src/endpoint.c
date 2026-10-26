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
 * moq_mvfst_managed_t (MOQ_SERVICE_HAVE_MVFST_MANAGED) or
 * moq_msquic_managed_t (MOQ_SERVICE_HAVE_MSQUIC_MANAGED, lane pump API) and
 * WEBTRANSPORT ->
 * moq_proxygen_wt_managed_t (MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED). Each
 * facade owns the managed execution context (a network thread; the
 * wtquic-Network backend instead binds its serial queue), the QUIC context,
 * and the MoQ session. The
 * endpoint adds
 * the service-tier contracts on top: the
 * sticky interrupt latch, the post() executor (exactly-once, FIFO,
 * NULL-session terminal drain), terminal-state classification, and the
 * attachment count the media services use.
 *
 * Threading: the facade's managed execution context runs ep_pump_cycle()
 * (drains post()
 * tasks, refreshes the cached endpoint state); the app thread calls the
 * public surface. Shared state lives under ep->mu; the interrupt latch is a
 * lone atomic so set_interrupted() never takes a lock the managed context
 * holds.
 *
 * Version negotiation is live for the picoquic facades: the resolved offer
 * becomes the RAW_QUIC ALPN offer list / the WebTransport
 * WT-Available-Protocols token list (both in PREFERENCE ORDER — the client's
 * order decides), the facade reads the negotiated ALPN / selected WT-Protocol
 * back and creates the MoQ session with the matching version. The proxygen
 * WebTransport facade is likewise live (it creates the session after the WT
 * CONNECT response). The mvfst and MsQuic facades are exact-version-first:
 * they create the session before the ALPN handshake, so they accept only a
 * single offered version (a multi-version / AUTO offer is MOQ_ERR_UNSUPPORTED
 * at create).
 * Either way moq_endpoint_negotiated_version() surfaces the version once
 * established.
 */

#include "endpoint_internal.h"
#include "endpoint_trust.h"

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
#ifdef MOQ_SERVICE_HAVE_MSQUIC_MANAGED
#include <moq/msquic_managed.h>
#endif
#ifdef MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED
#include <moq/proxygen_wt_managed.h>
#endif
#ifdef MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED
#include <moq/wtquic_network_managed.h>
#endif
#ifdef MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED
#include <moq/wtquic_msquic_managed.h>
/* The service profile enum is mapped EXPLICITLY onto the adapter enum in
 * ep_create_wtquic_msquic (they intentionally do not share values -- the service
 * enum has a BACKEND_DEFAULT sentinel). No equal-value assert: the mapping is
 * the switch there, and the adapter<->wtquic pin lives in the adapter. */
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
    /* Forward-compatible floor: accept any caller at or above the frozen v0
     * layout. Appended tail fields (wt_profile) are read below only when
     * struct_size covers them, so a v0-sized caller is served, not rejected. */
    if (cfg->struct_size < MOQ_ENDPOINT_CFG_V0_SIZE) return MOQ_ERR_INVAL;
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
     * pico_wt); every other backend is explicit opt-in. AUTO resolves
     * DIRECTLY and STABLY to picoquic -- if the required picoquic facade
     * is not compiled in, connect() reports MOQ_ERR_UNSUPPORTED rather
     * than falling through to mvfst/MsQuic/proxygen/wtquic-Network. An
     * explicit backend that is not compiled in (or not valid for the
     * resolved protocol) is MOQ_ERR_UNSUPPORTED. mvfst and MsQuic are
     * RAW_QUIC-only; proxygen and wtquic-Network are WEBTRANSPORT-only
     * (each rejects the other protocol). */
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
    case MOQ_TRANSPORT_BACKEND_MSQUIC:
#ifdef MOQ_SERVICE_HAVE_MSQUIC_MANAGED
        /* MsQuic managed is native QUIC only -- no WebTransport facade. */
        if (out->protocol != MOQ_TRANSPORT_PROTOCOL_RAW_QUIC)
            return MOQ_ERR_UNSUPPORTED;
        out->backend = MOQ_TRANSPORT_BACKEND_MSQUIC;
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
    case MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK:
#ifdef MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED
        /* The wtquic Network.framework client is WebTransport only (and
         * Apple-only -- off Apple the facade is never compiled in). */
        if (out->protocol != MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT)
            return MOQ_ERR_UNSUPPORTED;
        out->backend = MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK;
        break;
#else
        return MOQ_ERR_UNSUPPORTED;
#endif
    case MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC:
#ifdef MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED
        /* The wtquic-over-MsQuic client is WebTransport only (it has no raw-QUIC
         * facade) and cross-platform wherever wtquic's MsQuic component builds. */
        if (out->protocol != MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT)
            return MOQ_ERR_UNSUPPORTED;
        out->backend = MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC;
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

    /* WebTransport wire profile: an appended tail field, so read it ONLY when
     * struct_size fully covers it (complete-presence gate); a v0-sized caller
     * gets MOQ_WT_PROFILE_BACKEND_DEFAULT (0, already memset). */
    out->wt_profile = (uint32_t)MOQ_WT_PROFILE_BACKEND_DEFAULT;
    if ((size_t)cfg->struct_size >=
        offsetof(moq_endpoint_cfg_t, wt_profile) + sizeof(cfg->wt_profile)) {
        /* Range first: a bad value is a clean MOQ_ERR_INVAL, not a late reject. */
        if (cfg->wt_profile > (uint32_t)MOQ_WT_PROFILE_D13_14_COMPAT)
            return MOQ_ERR_INVAL;
        /* An EXPLICIT selection is honored ONLY by a backend that can pick a
         * dialect (wtquic-msquic). Every other backend has a fixed dialect or no
         * WebTransport, so reject rather than silently ignore -- the public
         * contract must not accept a selection it will not apply. */
        if (cfg->wt_profile != (uint32_t)MOQ_WT_PROFILE_BACKEND_DEFAULT &&
            out->backend != MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC)
            return MOQ_ERR_UNSUPPORTED;
        out->wt_profile = cfg->wt_profile;
    }

    /* Handshake bound: the same complete-presence gate; a v0-sized caller gets
     * 0 (already memset) = the backend's own default. Only picoquic can apply
     * it (the connect path's configure_quic hook); every other backend would
     * silently ignore it, so a non-zero value there is an UNSUPPORTED reject,
     * as with wt_profile. */
    out->handshake_timeout_us = 0;
    if ((size_t)cfg->struct_size >=
        offsetof(moq_endpoint_cfg_t, handshake_timeout_us) +
            sizeof(cfg->handshake_timeout_us)) {
        /* Range first, as with wt_profile: a value the backend would turn into
         * an immediate timeout by wrapping its absolute deadline is a clean
         * MOQ_ERR_INVAL on EVERY backend, not a capability answer. */
        if (cfg->handshake_timeout_us > MOQ_ENDPOINT_HANDSHAKE_TIMEOUT_MAX_US)
            return MOQ_ERR_INVAL;
        if (cfg->handshake_timeout_us != 0 &&
            out->backend != MOQ_TRANSPORT_BACKEND_PICOQUIC)
            return MOQ_ERR_UNSUPPORTED;
        out->handshake_timeout_us = cfg->handshake_timeout_us;
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
     * to MOQ_ERR_UNSUPPORTED); -2 = called on the managed context (MOQ_ERR_WRONG
     * _STATE). Evaluated only by the facade's own managed-context view, exposed
     * thread-safely. */
    int           (*drain_state)(void *facade);
    moq_result_t  (*stop)(void *facade);
    void          (*destroy)(void *facade);
    /* Classify the facade's terminal reason + native detail code. NULL is
     * treated as "generic" (NONE/PROTOCOL/TRANSPORT/CLEAN only).
     *
     * CONTRACT: this is OBSERVATION-ONLY and SAFE TO REPEAT. The central
     * classifier calls it several times in a row -- it composes separately
     * locked facade accessors, so one reading can straddle a backend's latch --
     * and requires the answer to be EVENTUALLY STABLE: once the facade's
     * terminal transition has settled, every later call must return the same
     * (reason, detail). An implementation must therefore not mutate facade
     * state, must not consume anything, and must not depend on being called
     * exactly once. */
    void          (*terminal)(void *facade,
                              moq_endpoint_terminal_reason_t *reason,
                              uint64_t *detail);
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
    char *ca_file; size_t ca_file_len; /* NULL = backend default roots if available */
    bool  insecure;
    uint64_t handshake_timeout_us;     /* 0 = backend default; picoquic only */
    /* The library-owned trust delegate, valid for the endpoint's lifetime.
     * NULL selects the ordinary path-verifier route. Not reachable from any
     * installed header: an application cannot supply one. */
    moq_endpoint_trust_fn trust_fn;
    void                 *trust_ctx;

    /* The version offer as transport tokens, preference order. The ALPN
     * entries point at the static strings in moq_alpn.h (no ownership);
     * the WT offer is one owned comma-joined string. */
    const char *alpn_offer[MOQ_ENDPOINT_MAX_VERSIONS];
    size_t      alpn_offer_count;
    char       *wt_offer; size_t wt_offer_len;

    /* Selected backend: an opaque managed-facade handle + its vtable. Set by
     * the backend create-fn in connect(); NULL until then / after destroy. */
    void                      *facade;
    /* Advances under ep->mu whenever the OBSERVABLE tuple
     * (stopped, pump_state, negotiated) changes value. Private: it is never
     * exposed, never part of the ABI, and never exported. */
    uint64_t observe_gen;
#if defined(MOQ_MEDIA_SENDER_TESTING) || defined(MOQ_MEDIA_RECEIVER_TESTING)
    void (*test_interpose)(void *);
    void  *test_interpose_ctx;
    int    test_interpose_at;
    unsigned test_interpose_fires;
    /* Connect-time observers, taken from the one-shot arming slot when this
     * endpoint is allocated (moq_endpoint_test_arm_next_connect). */
    void (*test_after_facade_create)(void *);
    void (*test_first_established)(void *, moq_endpoint_state_t,
                                   moq_version_t, moq_version_t, bool);
    void  *test_connect_ctx;
    bool   test_established_seen;
#endif
    const ep_backend_vtable_t *vt;

    /* Internal pump hooks (one slot per kind; see endpoint_internal.h).
     * Mutated under mu; invoked on the managed execution context under mu.
     * hook_ops is copied by value at attach; hook_ops[k].pump == NULL means the
     * slot is free. */
    moq_endpoint_hook_ops_t hook_ops[MOQ_ENDPOINT_HOOK_KIND_COUNT];
    void                   *hook_ctx[MOQ_ENDPOINT_HOOK_KIND_COUNT];

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
    defined(MOQ_SERVICE_HAVE_MSQUIC_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED) || \
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
 * Calling moq_session_* from a task is legal here (the managed context).
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
    defined(MOQ_SERVICE_HAVE_MSQUIC_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED) || \
    defined(MOQ_MEDIA_SENDER_TESTING) || defined(MOQ_MEDIA_RECEIVER_TESTING)
/*
 * One pump cycle, on the facade's managed execution context: drain accepted post()
 * tasks FIFO (calling moq_session_* is legal here), then refresh the cached
 * endpoint state from the session.
 */
static void ep_pump_cycle(moq_endpoint_t *ep, moq_session_t *session,
                          uint64_t now_us)
{
    /* Header §5.4 (normative, exactly-once): an accepted post() task runs with
     * session == NULL ONLY during stop()/terminal drain (ep_drain_terminal).
     * A managed facade can pump this cycle BEFORE its session is published --
     * a WebTransport CONNECT still handshaking -- and draining here would hand
     * fn a NULL session outside the terminal window, breaking that contract
     * (and, for a client that reads NULL as "gone", tearing down a live,
     * still-connecting session). So drain accepted tasks only once a live
     * session exists; a pre-establishment (NULL) cycle still refreshes the
     * cached endpoint state and runs attached hooks below, leaving the queue
     * intact for a later live cycle or the terminal drain. */
    if (session)
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
    /* A same-value refresh is not an observable change, so it must not advance
     * the observation generation -- otherwise an ordinary idle pump would make
     * every concurrent snapshot retry forever. */
    if (ep->pump_state != st)
        ep->observe_gen++;
    ep->pump_state = st;
    if (st == MOQ_ENDPOINT_CLOSED)
        atomic_store(&ep->closed, true);   /* mirror for ep->mu-free readers */
    if (st == MOQ_ENDPOINT_ESTABLISHED && ep->negotiated == 0) {
        /* The version comes from the ESTABLISHED session itself: the wire
         * profile it was created with, fixed for its lifetime. The facade
         * handle is not consulted -- a managed facade can pump its first
         * ESTABLISHED cycle before connect() has published it, and a version
         * read through it there would be 0 beside an ESTABLISHED state.
         *
         * This advance is DEFENSIVE and redundant: the version latches in the
         * same critical section as the ESTABLISHED transition, whose own
         * advance already covers it. It is kept because the generation's rule
         * is "advance on any observable tuple change", and a rule with an
         * exception is a rule waiting to be got wrong. */
        moq_version_t nv = moq_session_version(session);
        if (nv != ep->negotiated)
            ep->observe_gen++;
        ep->negotiated = nv;
    }
#if defined(MOQ_MEDIA_SENDER_TESTING) || defined(MOQ_MEDIA_RECEIVER_TESTING)
    /* The first settled ESTABLISHED cycle, reported as copied values while
     * ep->mu is held: the observer must not call back into the endpoint. */
    if (st == MOQ_ENDPOINT_ESTABLISHED && !ep->test_established_seen) {
        ep->test_established_seen = true;
        if (ep->test_first_established)
            ep->test_first_established(ep->test_connect_ctx, st, ep->negotiated,
                                       moq_session_version(session),
                                       ep->facade != NULL);
    }
#endif
    /* Attached-service hooks run under mu (detach is race-free); the hook
     * contract in endpoint_internal.h forbids re-entering public endpoint
     * APIs from inside. */
    for (int k = 0; k < MOQ_ENDPOINT_HOOK_KIND_COUNT; k++) {
        if (ep->hook_ops[k].pump)
            ep->hook_ops[k].pump(ep, session, now_us, ep->hook_ctx[k]);
    }
    pthread_mutex_unlock(&ep->mu);
}

#endif /* any facade */

/* Drain every queued task with the NULL-session closed marker (§5.4: each
 * accepted task runs exactly once; ctx cleanup is deterministic). Called
 * after the managed context is quiesced, so nothing races the queue. */
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
static int ep_pq_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                      uint64_t now_us, void *ctx)
{
    (void)lane;
    moq_endpoint_t *ep = (moq_endpoint_t *)ctx;
    /* The endpoint is a single-lane client; moq_pq_threaded_session() is the
     * documented client-session accessor, valid in the pump window. */
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
static int ep_mvfst_pump(moq_mvfst_managed_t *m, moq_mvfst_managed_lane_t *lane,
                         uint64_t now_us, void *ctx)
{
    (void)lane;
    moq_endpoint_t *ep = (moq_endpoint_t *)ctx;
    /* The endpoint is a single-lane client; moq_mvfst_managed_session() is the
     * documented client-session accessor, valid in the pump window. */
    ep_pump_cycle(ep, moq_mvfst_managed_session(m), now_us);
    return 0;
}
#endif

#ifdef MOQ_SERVICE_HAVE_MSQUIC_MANAGED
/*
 * MsQuic managed pump. The facade drives one lane per doorbell; this endpoint
 * is a single-lane client, so the lane carries exactly one connection. Drive
 * it through the lane's connection iterator (never the client-only
 * moq_msquic_managed_session accessor) so the same shim is correct if a future
 * sharded relay row runs the lane with multiple connections.
 */
static int ep_msquic_pump(moq_msquic_managed_t *m,
                          moq_msquic_managed_lane_t *lane,
                          uint64_t now_us, void *user)
{
    (void)m;
    moq_endpoint_t *ep = (moq_endpoint_t *)user;
    moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
    /* No connection yet (pre-CONNECTED): the session is NULL. ep_pump_cycle
     * refreshes cached state (the app thread observes CONNECTING) but DEFERS
     * accepted post() tasks per the §5.4 contract -- they run once a live
     * session exists or in the terminal drain, never with a NULL session
     * mid-connect. */
    ep_pump_cycle(ep, c ? moq_msquic_managed_conn_session(c) : NULL, now_us);
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

#ifdef MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED
/*
 * wtquic Network.framework managed pump. Drive the one client connection
 * through the lane iterator (the same shim shape as MsQuic). NOTE this
 * facade's pump begins only at establishment (the lane window never opens
 * pre-establishment), so pump cycles -- posted-task drains and hook runs --
 * start once the WT session is up; before that the endpoint stays
 * CONNECTING and accepted tasks defer (they still run exactly once: at the
 * first cycles, or in the terminal drain).
 */
static int ep_wtquic_network_pump(moq_wtquic_network_managed_t *m,
                         moq_wtquic_network_managed_lane_t *lane,
                         uint64_t now_us, void *user)
{
    (void)m;
    moq_endpoint_t *ep = (moq_endpoint_t *)user;
    moq_wtquic_network_managed_conn_t *c = moq_wtquic_network_lane_next_conn(lane, NULL);
    ep_pump_cycle(ep, c ? moq_wtquic_network_managed_conn_session(c) : NULL,
                  now_us);
    return 0;
}
#endif

#ifdef MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED
/*
 * wtquic-over-MsQuic managed pump. Same single-lane client shim as MsQuic and
 * wtquic-Network: drive the one connection through the lane iterator (never the
 * client-only session accessor) so the shim stays correct if a future sharded
 * relay row runs the lane with multiple connections. Before establishment the
 * lane still pumps to refresh state (the endpoint stays CONNECTING), but the
 * session is NULL, so accepted post() tasks DEFER rather than drain -- they run
 * on the first live cycle or in the terminal drain (see ep_pump_cycle).
 */
static int ep_wtquic_msquic_pump(moq_wtquic_msquic_managed_t *m,
                        moq_wtquic_msquic_managed_lane_t *lane,
                        uint64_t now_us, void *user)
{
    (void)m;
    moq_endpoint_t *ep = (moq_endpoint_t *)user;
    moq_wtquic_msquic_managed_conn_t *c = moq_wtquic_msquic_lane_next_conn(lane, NULL);
    ep_pump_cycle(ep, c ? moq_wtquic_msquic_managed_conn_session(c) : NULL,
                  now_us);
    return 0;
}
#endif

#if defined(MOQ_SERVICE_HAVE_PQ_THREADED) || defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED)
/* Apply the caller's handshake bound, then install real certificate
 * verification unless explicitly skipped: chain + server-name validation
 * against ep->ca_file, or the backend default roots when available. The
 * facades' built-in default accepts the peer cert, which is NOT
 * production-safe -- making the safe path the default is this tier's job. */
/* Translate the service verdict at the ONE boundary where the tiers meet.
 * Only ACCEPT maps to TRUST; REJECT and any unrecognised value fail closed. */
static moq_picoquic_cert_verdict_t ep_trust_bridge(
    void *ctx, moq_bytes_t server_name,
    const moq_bytes_t *certificates, size_t certificate_count)
{
    moq_endpoint_t *ep = (moq_endpoint_t *)ctx;
    if (ep == NULL || ep->trust_fn == NULL) return MOQ_PICOQUIC_CERT_REJECT;
    moq_endpoint_trust_verdict_t v =
        ep->trust_fn(ep->trust_ctx, server_name, certificates,
                     certificate_count);
    return (v == MOQ_ENDPOINT_TRUST_ACCEPT) ? MOQ_PICOQUIC_CERT_TRUST
                                            : MOQ_PICOQUIC_CERT_REJECT;
}

static int ep_configure_quic(picoquic_quic_t *quic, void *ctx)
{
    moq_endpoint_t *ep = (moq_endpoint_t *)ctx;
    if (ep->handshake_timeout_us > 0)
        picoquic_set_default_handshake_timeout(quic, ep->handshake_timeout_us);
    if (ep->insecure) return 0;
    /* A delegate selects the callback setter and NEVER falls back to the path
     * setter: falling back would silently verify against a policy the caller
     * did not ask for. Its incompatibility with ca_file/insecure was already
     * rejected before this endpoint existed. */
    if (ep->trust_fn != NULL)
        return moq_picoquic_set_cert_verify_callback(quic, ep_trust_bridge, ep);
    return moq_picoquic_set_cert_verifier(quic, ep->ca_file);
}
#endif

/* -- Terminal-reason classification --------------------------------- *
 * Turn a facade's raw terminal signals into a transport-agnostic reason. The
 * generic form (mvfst/proxygen) knows only PROTOCOL vs generic transport; the
 * picoquic form additionally reads picoquic's captured local error to tell a
 * TLS/certificate failure apart from a plain transport failure. */
/* The generic classification the backend vtable documents for a NULL `terminal`
 * slot. Compiled unconditionally: it is the fallback arm of the single-source
 * classifier below, which every build has. */
static void ep_classify_generic(bool is_fatal, uint64_t fatal_code,
                                bool clean_closed,
                                moq_endpoint_terminal_reason_t *reason,
                                uint64_t *detail)
{
    if (is_fatal && fatal_code != 0) {
        *reason = MOQ_ENDPOINT_TERMINAL_PROTOCOL; *detail = fatal_code;
    } else if (is_fatal) {
        *reason = MOQ_ENDPOINT_TERMINAL_TRANSPORT; *detail = 0;
    } else if (clean_closed) {
        *reason = MOQ_ENDPOINT_TERMINAL_CLEAN; *detail = 0;
    } else {
        *reason = MOQ_ENDPOINT_TERMINAL_NONE; *detail = 0;
    }
}

#if defined(MOQ_SERVICE_HAVE_PQ_THREADED) || \
    defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED)
#include <picoquic.h>   /* picoquic_is_handshake_error */
static void ep_classify_pico(bool is_fatal, uint64_t fatal_code,
                             bool clean_closed, uint64_t terminal_error,
                             moq_endpoint_terminal_reason_t *reason,
                             uint64_t *detail)
{
    if (is_fatal && fatal_code != 0) {
        /* A MoQ/bridge protocol fatal took precedence over any transport code. */
        *reason = MOQ_ENDPOINT_TERMINAL_PROTOCOL; *detail = fatal_code;
        return;
    }
    if (is_fatal) {
        if (picoquic_is_handshake_error(terminal_error)) {
            /* QUIC CRYPTO_ERROR: low byte is the TLS alert. Certificate-related
             * alerts (picotls PTLS_ALERT_*): 42 bad_certificate,
             * 44 certificate_revoked, 45 certificate_expired,
             * 46 certificate_unknown, 48 unknown_ca. */
            uint8_t alert = (uint8_t)(terminal_error & 0xFFu);
            *reason = (alert == 42 || alert == 44 || alert == 45 ||
                       alert == 46 || alert == 48)
                ? MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE
                : MOQ_ENDPOINT_TERMINAL_TLS;
        } else {
            *reason = MOQ_ENDPOINT_TERMINAL_TRANSPORT;
        }
        *detail = terminal_error;
        return;
    }
    if (clean_closed) {
        *reason = MOQ_ENDPOINT_TERMINAL_CLEAN; *detail = 0;
        return;
    }
    *reason = MOQ_ENDPOINT_TERMINAL_NONE; *detail = 0;
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
static void          pq_vt_terminal(void *f, moq_endpoint_terminal_reason_t *r, uint64_t *d) {
    moq_pq_threaded_t *t = (moq_pq_threaded_t *)f;
    ep_classify_pico(moq_pq_threaded_is_fatal(t), moq_pq_threaded_fatal_code(t),
        false /* raw QUIC: no clean-close concept */,
        moq_pq_threaded_terminal_error(t), r, d);
}
static const ep_backend_vtable_t PQ_VT = {
    pq_vt_wait, pq_vt_wake, pq_vt_negver, pq_vt_is_fatal,
    pq_vt_fatal_code, pq_vt_clean_closed, pq_vt_drain_state, pq_vt_stop,
    pq_vt_destroy, pq_vt_terminal,
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
static void          wt_vt_terminal(void *f, moq_endpoint_terminal_reason_t *r, uint64_t *d) {
    moq_pico_wt_managed_t *m = (moq_pico_wt_managed_t *)f;
    ep_classify_pico(moq_pico_wt_managed_is_fatal(m), moq_pico_wt_managed_fatal_code(m),
        moq_pico_wt_managed_is_closed(m), moq_pico_wt_managed_terminal_error(m), r, d);
}
static const ep_backend_vtable_t WT_VT = {
    wt_vt_wait, wt_vt_wake, wt_vt_negver, wt_vt_is_fatal,
    wt_vt_fatal_code, wt_vt_clean_closed, wt_vt_drain_state, wt_vt_stop,
    wt_vt_destroy, wt_vt_terminal,
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
static void          mvfst_vt_terminal(void *f, moq_endpoint_terminal_reason_t *r, uint64_t *d) {
    /* mvfst verifies internally and exposes no per-cnx transport error yet:
     * PROTOCOL vs generic transport only. */
    ep_classify_generic(moq_mvfst_managed_is_fatal((moq_mvfst_managed_t *)f),
        moq_mvfst_managed_fatal_code((moq_mvfst_managed_t *)f), false, r, d);
}
static const ep_backend_vtable_t MVFST_VT = {
    mvfst_vt_wait, mvfst_vt_wake, mvfst_vt_negver, mvfst_vt_is_fatal,
    mvfst_vt_fatal_code, mvfst_vt_clean_closed, mvfst_vt_drain_state,
    mvfst_vt_stop, mvfst_vt_destroy, mvfst_vt_terminal,
};
#endif

#ifdef MOQ_SERVICE_HAVE_MSQUIC_MANAGED
static moq_result_t  msquic_vt_wait(void *f, uint64_t t) { return moq_msquic_managed_wait((moq_msquic_managed_t *)f, t); }
static void          msquic_vt_wake(void *f) { (void)moq_msquic_managed_wake((moq_msquic_managed_t *)f); }
static moq_version_t msquic_vt_negver(void *f) { return moq_msquic_managed_negotiated_version((moq_msquic_managed_t *)f); }
static bool          msquic_vt_is_fatal(void *f) { return moq_msquic_managed_is_fatal((moq_msquic_managed_t *)f); }
static uint64_t      msquic_vt_fatal_code(void *f) { return moq_msquic_managed_fatal_code((moq_msquic_managed_t *)f); }
/* MsQuic exposes a terminal-close accessor: a graceful close latches is_closed
 * with is_fatal clear (fatal wins over closed when both latch, and
 * ep_classify_generic checks is_fatal first), so this maps to a CLEAN terminal. */
static bool          msquic_vt_clean_closed(void *f) { return moq_msquic_managed_is_closed((moq_msquic_managed_t *)f); }
/* Unlike mvfst, MsQuic exposes public outbound-flush probes: the drain is idle
 * once no connection is live and no stream send / datagram is still in flight.
 * All three probes are thread-safe, so no wrong-thread case arises. */
static int           msquic_vt_drain_state(void *f) {
    moq_msquic_managed_t *m = (moq_msquic_managed_t *)f;
    bool idle = moq_msquic_managed_conn_count(m) == 0 &&
                moq_msquic_managed_pending_sends(m) <= 0 &&
                moq_msquic_managed_pending_datagrams(m) <= 0;
    return idle ? EP_DRAIN_DONE : EP_DRAIN_PENDING;
}
static moq_result_t  msquic_vt_stop(void *f) { return moq_msquic_managed_stop((moq_msquic_managed_t *)f); }
static void          msquic_vt_destroy(void *f) { moq_msquic_managed_destroy((moq_msquic_managed_t *)f); }
static void          msquic_vt_terminal(void *f, moq_endpoint_terminal_reason_t *r, uint64_t *d) {
    /* MsQuic verifies internally and exposes no per-cnx transport error
     * (PROTOCOL vs generic transport only), but it DOES expose a clean-close
     * accessor: a graceful close maps to CLEAN, not NONE. */
    ep_classify_generic(moq_msquic_managed_is_fatal((moq_msquic_managed_t *)f),
        moq_msquic_managed_fatal_code((moq_msquic_managed_t *)f),
        moq_msquic_managed_is_closed((moq_msquic_managed_t *)f), r, d);
}
static const ep_backend_vtable_t MSQUIC_VT = {
    msquic_vt_wait, msquic_vt_wake, msquic_vt_negver, msquic_vt_is_fatal,
    msquic_vt_fatal_code, msquic_vt_clean_closed, msquic_vt_drain_state,
    msquic_vt_stop, msquic_vt_destroy, msquic_vt_terminal,
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
static void          proxygen_vt_terminal(void *f, moq_endpoint_terminal_reason_t *r, uint64_t *d) {
    ep_classify_generic(moq_proxygen_wt_managed_is_fatal((moq_proxygen_wt_managed_t *)f),
        moq_proxygen_wt_managed_fatal_code((moq_proxygen_wt_managed_t *)f),
        moq_proxygen_wt_managed_is_closed((moq_proxygen_wt_managed_t *)f), r, d);
}
static const ep_backend_vtable_t PROXYGEN_VT = {
    proxygen_vt_wait, proxygen_vt_wake, proxygen_vt_negver, proxygen_vt_is_fatal,
    proxygen_vt_fatal_code, proxygen_vt_clean_closed, proxygen_vt_drain_state,
    proxygen_vt_stop, proxygen_vt_destroy, proxygen_vt_terminal,
};
#endif

#ifdef MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED
/* The endpoint's public calls run on the app thread, which is off the
 * facade's serial-queue domain, so the blocking wait/stop compositions are
 * legal here by construction (on-domain they would be refused). */
static moq_result_t  wtquic_network_vt_wait(void *f, uint64_t t) { return moq_wtquic_network_managed_wait((moq_wtquic_network_managed_t *)f, t); }
/* best-effort per the endpoint wake contract: non-allocating and
 * infallible to call; a wake racing the facade's stop may be absorbed
 * by teardown, which the contract's "harmless during/after terminal
 * teardown" already covers */
static void          wtquic_network_vt_wake(void *f) { (void)moq_wtquic_network_managed_wake((moq_wtquic_network_managed_t *)f); }
static moq_version_t wtquic_network_vt_negver(void *f) { return moq_wtquic_network_managed_negotiated_version((moq_wtquic_network_managed_t *)f); }
static bool          wtquic_network_vt_is_fatal(void *f) { return moq_wtquic_network_managed_is_fatal((moq_wtquic_network_managed_t *)f); }
static uint64_t      wtquic_network_vt_fatal_code(void *f) { return moq_wtquic_network_managed_fatal_code((moq_wtquic_network_managed_t *)f); }
static bool          wtquic_network_vt_clean_closed(void *f) { return moq_wtquic_network_managed_is_closed((moq_wtquic_network_managed_t *)f); }
/* No public outbound-flush probe -> drain unsupported (never claim a drain
 * the backend cannot verify; same stance as mvfst/proxygen). */
static int           wtquic_network_vt_drain_state(void *f) { (void)f; return EP_DRAIN_UNSUPPORTED; }
static moq_result_t  wtquic_network_vt_stop(void *f) { return moq_wtquic_network_managed_stop((moq_wtquic_network_managed_t *)f); }
static void          wtquic_network_vt_destroy(void *f) { moq_wtquic_network_managed_destroy((moq_wtquic_network_managed_t *)f); }
/*
 * Pure classification (declared in endpoint_internal.h for white-box
 * tests). By PROVENANCE, not an OSStatus allowlist: NW_TRUST is sealed
 * only by wtquic's explicit trust evaluator (the peer's chain failed
 * SecTrustEvaluateWithError) -> TLS_CERTIFICATE; NW_TLS is any other
 * TLS-domain error NW itself delivered -> TLS; every other captured
 * record (NW_POSIX, NW_DNS, BACKEND) -> TRANSPORT. Each preserves the
 * RAW native_code bits as the detail (Security OSStatus values are
 * negative: errSecNotTrusted -67843 for a self-signed peer) -- the
 * terminal contract promises the backend's native detail, never a
 * codeless collapse. A MoQ/bridge protocol fatal (nonzero code) takes
 * precedence, as everywhere.
 */
void moq_endpoint_classify_wtquic_network(bool fatal, uint64_t fatal_code,
                                 bool clean_closed, bool have_record,
                                 const wtq_transport_error_t *rec,
                                 moq_endpoint_terminal_reason_t *reason,
                                 uint64_t *detail)
{
    if (fatal && fatal_code == 0 && have_record && rec != NULL &&
        rec->native_domain != (uint32_t)WTQ_ERRDOM_NONE) {
        if (rec->native_domain == (uint32_t)WTQ_ERRDOM_NW_TRUST)
            *reason = MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE;
        else if (rec->native_domain == (uint32_t)WTQ_ERRDOM_NW_TLS)
            *reason = MOQ_ENDPOINT_TERMINAL_TLS;
        else
            *reason = MOQ_ENDPOINT_TERMINAL_TRANSPORT;
        *detail = (uint64_t)rec->native_code;
        return;
    }
    ep_classify_generic(fatal, fatal_code, clean_closed, reason, detail);
}

static void          wtquic_network_vt_terminal(void *f, moq_endpoint_terminal_reason_t *r, uint64_t *d) {
    moq_wtquic_network_managed_t *m = (moq_wtquic_network_managed_t *)f;
    bool fatal = moq_wtquic_network_managed_is_fatal(m);
    uint64_t code = moq_wtquic_network_managed_fatal_code(m);
    wtq_transport_error_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.struct_size = (uint32_t)sizeof(rec);
    bool have = fatal && code == 0 &&
                moq_wtquic_network_managed_transport_error(m, &rec);
    moq_endpoint_classify_wtquic_network(fatal, code,
                                moq_wtquic_network_managed_is_closed(m),
                                have, &rec, r, d);
}
static const ep_backend_vtable_t WTQUIC_NETWORK_VT = {
    wtquic_network_vt_wait, wtquic_network_vt_wake, wtquic_network_vt_negver, wtquic_network_vt_is_fatal,
    wtquic_network_vt_fatal_code, wtquic_network_vt_clean_closed, wtquic_network_vt_drain_state,
    wtquic_network_vt_stop, wtquic_network_vt_destroy, wtquic_network_vt_terminal,
};
#endif

#ifdef MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED
/* The endpoint's public calls run on the app thread, off the facade's
 * coordinator/doorbell domain, so the blocking wait/stop compositions are legal
 * here by construction (inside a callback they would be refused). */
static moq_result_t  wtquic_msquic_vt_wait(void *f, uint64_t t) { return moq_wtquic_msquic_managed_wait((moq_wtquic_msquic_managed_t *)f, t); }
static void          wtquic_msquic_vt_wake(void *f) { (void)moq_wtquic_msquic_managed_wake((moq_wtquic_msquic_managed_t *)f); }
static moq_version_t wtquic_msquic_vt_negver(void *f) { return moq_wtquic_msquic_managed_negotiated_version((moq_wtquic_msquic_managed_t *)f); }
static bool          wtquic_msquic_vt_is_fatal(void *f) { return moq_wtquic_msquic_managed_is_fatal((moq_wtquic_msquic_managed_t *)f); }
static uint64_t      wtquic_msquic_vt_fatal_code(void *f) { return moq_wtquic_msquic_managed_fatal_code((moq_wtquic_msquic_managed_t *)f); }
static bool          wtquic_msquic_vt_clean_closed(void *f) { return moq_wtquic_msquic_managed_is_closed((moq_wtquic_msquic_managed_t *)f); }
/* No truthful outbound-flush probe: this facade exposes no per-connection
 * pending send/datagram count, and conn_count is not maintained for the single
 * client connection -- so any "idle" answer would be a guess that reports DONE
 * for a live client and truncates the drain. Report UNSUPPORTED (the caller maps
 * it to MOQ_ERR_UNSUPPORTED) rather than claim a drain the backend cannot prove;
 * same stance as mvfst/proxygen/wtquic-Network. A truthful probe would require an
 * attach-level pending-bytes API this facade does not expose. */
static int           wtquic_msquic_vt_drain_state(void *f) { (void)f; return EP_DRAIN_UNSUPPORTED; }
static moq_result_t  wtquic_msquic_vt_stop(void *f) { return moq_wtquic_msquic_managed_stop((moq_wtquic_msquic_managed_t *)f); }
static void          wtquic_msquic_vt_destroy(void *f) { moq_wtquic_msquic_managed_destroy((moq_wtquic_msquic_managed_t *)f); }
static void          wtquic_msquic_vt_terminal(void *f, moq_endpoint_terminal_reason_t *r, uint64_t *d) {
    /* Generic classification only: the facade exposes no per-connection
     * transport-error detail, so PROTOCOL vs generic transport (with a clean
     * close mapping to CLEAN) is the whole story -- do not invent TLS detail. */
    moq_wtquic_msquic_managed_t *m = (moq_wtquic_msquic_managed_t *)f;
    ep_classify_generic(moq_wtquic_msquic_managed_is_fatal(m),
        moq_wtquic_msquic_managed_fatal_code(m),
        moq_wtquic_msquic_managed_is_closed(m), r, d);
}
static const ep_backend_vtable_t WTQUIC_MSQUIC_VT = {
    wtquic_msquic_vt_wait, wtquic_msquic_vt_wake, wtquic_msquic_vt_negver, wtquic_msquic_vt_is_fatal,
    wtquic_msquic_vt_fatal_code, wtquic_msquic_vt_clean_closed, wtquic_msquic_vt_drain_state,
    wtquic_msquic_vt_stop, wtquic_msquic_vt_destroy, wtquic_msquic_vt_terminal,
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
    defined(MOQ_SERVICE_HAVE_MSQUIC_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED)
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
    defined(MOQ_SERVICE_HAVE_MSQUIC_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED)

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
    fc.ca_file = ep->ca_file;
    fc.alpn_list = ep->alpn_offer;
    fc.alpn_count = ep->alpn_offer_count;
    fc.port = (int)r->url.port;
    fc.insecure_skip_verify = cfg->insecure_skip_verify;
    fc.configure_quic = ep_configure_quic;
    fc.configure_quic_ctx = ep;
    fc.on_lane_pump = ep_pq_pump;
    fc.on_lane_pump_ctx = ep;
    fc.app_deadline_us = moq_endpoint_app_deadline_us;
    fc.app_deadline_ctx = ep;
    /* Send QUIC keepalive on the client connection so an idle publisher (one
     * with no subscriber yet) is not dropped by a relay's transport idle
     * timeout (moqx default ~30s). 15s stays comfortably under a 30s idle
     * bound. Distinct from a MoQ/session deadline; see picoquic_threaded.h. */
    fc.keep_alive_interval_ms = 15000;
    moq_pq_threaded_t *fac = NULL;
    moq_result_t crc = moq_pq_threaded_create(&fc, &fac);
    if (crc < 0) return crc;
#if defined(MOQ_MEDIA_SENDER_TESTING) || defined(MOQ_MEDIA_RECEIVER_TESTING)
    /* The facade's network thread is already running; ep->facade and ep->vt
     * are not yet published. */
    if (ep->test_after_facade_create)
        ep->test_after_facade_create(ep->test_connect_ctx);
#endif
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
    moq_pico_wt_managed_cfg_init_sized(&fc, sizeof(fc));  /* enables app_deadline */
    fc.alloc = &ep->alloc;
    fc.perspective = MOQ_PERSPECTIVE_CLIENT;
    fc.app_deadline_us = moq_endpoint_app_deadline_us;
    fc.app_deadline_ctx = ep;
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
#if defined(MOQ_MEDIA_SENDER_TESTING) || defined(MOQ_MEDIA_RECEIVER_TESTING)
    /* As for the raw-QUIC facade: its network thread is running and the
     * facade is not yet published. */
    if (ep->test_after_facade_create)
        ep->test_after_facade_create(ep->test_connect_ctx);
#endif
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
     * verifier policy: skip when insecure, else trust ep->ca_file when set,
     * otherwise mvfst's default roots, with the SNI as the checked identity. */
    if (!cfg->insecure_skip_verify && ep->ca_file)
        fc.cert_path = ep->ca_file;
    fc.on_lane_pump = ep_mvfst_pump;
    fc.user_ctx = ep;
    /* app service-deadline (media_sender catalog refresh, etc.): folded into the
     * EventBase pump's earliest deadline. Sized init covered the block. */
    fc.app_deadline_us = moq_endpoint_app_deadline_us;
    fc.app_deadline_ctx = ep;
    moq_mvfst_managed_t *fac = NULL;
    moq_result_t crc = moq_mvfst_managed_create(&fc, &fac);
    if (crc < 0) return crc;
    ep->facade = fac;
    ep->vt = &MVFST_VT;
    return MOQ_OK;
}
#endif

#ifdef MOQ_SERVICE_HAVE_MSQUIC_MANAGED
static moq_result_t ep_create_msquic(moq_endpoint_t *ep,
                                     const moq_endpoint_resolved_t *r,
                                     const moq_endpoint_cfg_t *cfg)
{
    /* Exact-version only: the managed MsQuic adapter creates the MoQ session
     * before the ALPN handshake (single offered ALPN), so it cannot negotiate
     * a multi-version offer. Require exactly one offered version and pass it as
     * the facade's single version. AUTO (newest-first) therefore needs an
     * EXACT version policy for MsQuic, like mvfst. */
    if (ep->alpn_offer_count != 1)
        return MOQ_ERR_UNSUPPORTED;
    /* The one offered ALPN token round-trips to its version (the offer was
     * built from moq_alpn_for_version). */
    moq_version_t version = 0;
    if (!moq_alpn_to_version(ep->alpn_offer[0], strlen(ep->alpn_offer[0]),
                             &version))
        return MOQ_ERR_UNSUPPORTED;

    /* TLS knobs the managed MsQuic facade cannot honor yet are REJECTED, not
     * silently ignored (the endpoint contract honors cfg.sni and cfg.ca_file).
     * moq_msquic_managed exposes only host (used as both remote and TLS server
     * name) + insecure_skip_verify -- no separate SNI, no CA-file field. So:
     *   - custom CA roots (ca_file set with verification on): unsupported.
     *   - explicit SNI override (resolved sni != url host): unsupported.
     * insecure_skip_verify (self-signed loopback) and default verification
     * against the platform/system roots stay supported. */
    if (!cfg->insecure_skip_verify && ep->ca_file != NULL)
        return MOQ_ERR_UNSUPPORTED;
    if (ep->sni != NULL && ep->host != NULL &&
        strcmp(ep->sni, ep->host) != 0)
        return MOQ_ERR_UNSUPPORTED;

    moq_msquic_managed_cfg_t fc;
    /* Sized init: this endpoint sets the appended `version` field, so the
     * pointer-only init (frozen prefix) would leave it at the default. */
    moq_msquic_managed_cfg_init_sized(&fc, sizeof(fc));
    fc.alloc = &ep->alloc;
    fc.perspective = MOQ_PERSPECTIVE_CLIENT;
    fc.host = ep->host;                 /* remote + TLS server name (sni==host,
                                           enforced above) */
    fc.port = (uint16_t)r->url.port;
    fc.insecure_skip_verify = cfg->insecure_skip_verify;
    /* See the picoquic branch: grant the peer request capacity so it can
     * subscribe to tracks this endpoint publishes. */
    fc.send_request_capacity = true;
    fc.initial_request_capacity = 64;
    fc.version = version;
    fc.on_lane_pump = ep_msquic_pump;
    fc.on_lane_pump_user = ep;
    /* app service-deadline (media_sender catalog refresh, etc.): folded into
     * each lane doorbell's next-wait computation. Sized init covered the block. */
    fc.app_deadline_us = moq_endpoint_app_deadline_us;
    fc.app_deadline_ctx = ep;
    moq_msquic_managed_t *fac = NULL;
    moq_result_t crc = moq_msquic_managed_create(&fc, &fac);
    if (crc < 0) return crc;
    ep->facade = fac;
    ep->vt = &MSQUIC_VT;
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
     * insecure, else trust ep->ca_file when set, otherwise proxygen's default
     * roots. */
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

#ifdef MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED
static moq_result_t ep_create_wtquic_network(moq_endpoint_t *ep,
                                             const moq_endpoint_resolved_t *r,
                                             const moq_endpoint_cfg_t *cfg)
{
    /* TLS knobs the facade cannot honor are REJECTED, not silently ignored
     * (the endpoint contract honors cfg.sni and cfg.ca_file). The facade
     * exposes only host (remote + TLS server name, via Network.framework's
     * system trust) + insecure_skip_verify -- no separate SNI, no CA-file
     * field. Same policy as the MsQuic backend. */
    if (!cfg->insecure_skip_verify && ep->ca_file != NULL)
        return MOQ_ERR_UNSUPPORTED;
    if (ep->sni != NULL && ep->host != NULL &&
        strcmp(ep->sni, ep->host) != 0)
        return MOQ_ERR_UNSUPPORTED;

    /* The facade's wt_protocols is a comma-separated UNQUOTED token list in
     * preference order (it builds the header itself) -- NOT the pico_wt
     * quoted wt_offer string. Negotiation is real, so the full multi-version
     * offer (AUTO/LIST) passes through. The facade copies the string. */
    char offer[MOQ_ENDPOINT_MAX_VERSIONS * 16];
    size_t off = 0;
    for (size_t i = 0; i < ep->alpn_offer_count; i++) {
        size_t n = strlen(ep->alpn_offer[i]);
        if (off + n + 3 > sizeof(offer))
            return MOQ_ERR_INTERNAL;   /* unreachable: 8 short tokens */
        if (i > 0) { offer[off++] = ','; offer[off++] = ' '; }
        memcpy(offer + off, ep->alpn_offer[i], n);
        off += n;
    }
    offer[off] = '\0';

    moq_wtquic_network_managed_cfg_t fc;
    moq_wtquic_network_managed_cfg_init(&fc);
    fc.alloc = &ep->alloc;
    fc.host = ep->host;                 /* remote + TLS server name (sni==host,
                                           enforced above) */
    fc.port = (int)r->url.port;
    fc.path = ep->path;
    fc.wt_protocols = ep->alpn_offer_count > 0 ? offer : NULL;
    fc.insecure_skip_verify = cfg->insecure_skip_verify;
    /* See the picoquic branch: grant the peer request capacity so it can
     * subscribe to tracks this endpoint publishes. */
    fc.send_request_capacity = true;
    fc.initial_request_capacity = 64;
    fc.on_lane_pump = ep_wtquic_network_pump;
    fc.on_lane_pump_user = ep;
    fc.perspective = MOQ_PERSPECTIVE_CLIENT;   /* explicit: client-only */
    fc.lane_count = 1;                         /* explicit: single lane */
    /* the app service-deadline (media_sender catalog refresh, etc.): the
     * pointer init stamped the full struct, so the block is honored and
     * the facade folds it into the native delayed doorbell. */
    fc.app_deadline_us = moq_endpoint_app_deadline_us;
    fc.app_deadline_ctx = ep;
    moq_wtquic_network_managed_t *fac = NULL;
    moq_result_t crc = moq_wtquic_network_managed_create(&fc, &fac);
    if (crc < 0) return crc;
    ep->facade = fac;
    ep->vt = &WTQUIC_NETWORK_VT;
    return MOQ_OK;
}
#endif

#ifdef MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED
static moq_result_t ep_create_wtquic_msquic(moq_endpoint_t *ep,
                                            const moq_endpoint_resolved_t *r,
                                            const moq_endpoint_cfg_t *cfg)
{
    /* TLS knobs the facade cannot honor are REJECTED, not silently ignored (the
     * endpoint contract honors cfg.sni and cfg.ca_file). The facade exposes only
     * host (remote + TLS server name) + insecure_skip_verify -- no separate SNI,
     * no CA-file field. Same policy as the MsQuic and wtquic-Network backends. */
    if (!cfg->insecure_skip_verify && ep->ca_file != NULL)
        return MOQ_ERR_UNSUPPORTED;
    if (ep->sni != NULL && ep->host != NULL &&
        strcmp(ep->sni, ep->host) != 0)
        return MOQ_ERR_UNSUPPORTED;

    moq_wtquic_msquic_managed_cfg_t fc;
    /* Sized init: this endpoint sets appended fields (send_request_capacity,
     * lane_count), so the pointer-only init (frozen prefix) would leave them at
     * their defaults. */
    moq_wtquic_msquic_managed_cfg_init_sized(&fc, sizeof(fc));
    fc.alloc = &ep->alloc;
    fc.perspective = MOQ_PERSPECTIVE_CLIENT;   /* explicit: client-only */
    fc.host = ep->host;                 /* remote + TLS server name (sni==host,
                                           enforced above) */
    fc.port = (uint16_t)r->url.port;
    fc.wt_path = ep->path;
    fc.insecure_skip_verify = cfg->insecure_skip_verify;
    /* The facade takes the WT-protocol token vector directly (it builds the
     * WT-Available-Protocols header itself, negotiation is real): pass the
     * parsed offer array and count, so the full multi-version offer (AUTO/LIST)
     * passes through. NULL/0 would select the facade default. */
    fc.wt_protocols = ep->alpn_offer_count > 0 ? ep->alpn_offer : NULL;
    fc.wt_protocol_count = ep->alpn_offer_count;
    /* See the picoquic branch: grant the peer request capacity so it can
     * subscribe to tracks this endpoint publishes. */
    fc.send_request_capacity = true;
    fc.initial_request_capacity = 64;
    fc.lane_count = 1;                         /* explicit: single lane */
    fc.on_lane_pump = ep_wtquic_msquic_pump;
    fc.on_lane_pump_user = ep;
    /* app service-deadline (media_sender catalog refresh, etc.): folded into
     * each lane pump's recomputed deadline. Sized init covered the block. */
    fc.app_deadline_us = moq_endpoint_app_deadline_us;
    fc.app_deadline_ctx = ep;
    /* WebTransport wire profile: map the service enum EXPLICITLY onto the
     * adapter enum. The two do NOT share numeric values -- the service enum
     * carries a BACKEND_DEFAULT sentinel the adapter has no notion of. The
     * resolver already gated/validated the value and guaranteed this backend is
     * the only one that accepts an explicit selection; BACKEND_DEFAULT resolves
     * to the backend's default (CURRENT). */
    switch (r->wt_profile) {
    case MOQ_WT_PROFILE_D13_14_COMPAT:
        fc.webtransport_profile =
            (uint32_t)MOQ_WTQUIC_MSQUIC_WT_PROFILE_D13_14_COMPAT;
        break;
    case MOQ_WT_PROFILE_CURRENT:
    case MOQ_WT_PROFILE_BACKEND_DEFAULT:
    default:
        fc.webtransport_profile =
            (uint32_t)MOQ_WTQUIC_MSQUIC_WT_PROFILE_CURRENT;
        break;
    }
    moq_wtquic_msquic_managed_t *fac = NULL;
    moq_result_t crc = moq_wtquic_msquic_managed_create(&fc, &fac);
    if (crc < 0) return crc;
    ep->facade = fac;
    ep->vt = &WTQUIC_MSQUIC_VT;
    return MOQ_OK;
}
#endif

typedef struct ep_backend_row {
    moq_transport_protocol_t protocol;
    moq_transport_backend_t  backend;
    ep_backend_create_fn     create;
} ep_backend_row_t;

/* One row per (protocol, backend); AUTO already resolved to PICOQUIC at
 * cfg resolution, so lookup here is exact -- never first-available. */
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
#ifdef MOQ_SERVICE_HAVE_MSQUIC_MANAGED
    /* After picoquic: AUTO RAW_QUIC still picks picoquic; MsQuic is reached
     * only by an explicit backend = MSQUIC. Uses the lane pump API and is the
     * intended path for future sharded relay work. */
    { MOQ_TRANSPORT_PROTOCOL_RAW_QUIC,     MOQ_TRANSPORT_BACKEND_MSQUIC,   ep_create_msquic },
#endif
#ifdef MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED
    /* After pico_wt: AUTO WEBTRANSPORT still picks pico_wt; proxygen is
     * reached only by an explicit backend = PROXYGEN. */
    { MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT, MOQ_TRANSPORT_BACKEND_PROXYGEN, ep_create_proxygen },
#endif
#ifdef MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED
    /* After pico_wt/proxygen: AUTO WEBTRANSPORT still picks pico_wt when
     * built; the Network.framework client is reached only by an explicit
     * backend = WTQUIC_NETWORK. Lane pump API; Apple/client-only. */
    { MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT, MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK, ep_create_wtquic_network },
#endif
#ifdef MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED
    /* After pico_wt/proxygen/wtquic-Network: AUTO WEBTRANSPORT still picks
     * pico_wt when built; the wtquic-over-MsQuic client is reached only by an
     * explicit backend = WTQUIC_MSQUIC. Lane pump API; cross-platform,
     * client-only. */
    { MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT, MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC, ep_create_wtquic_msquic },
#endif
};

#endif /* any facade */

/* The frozen v0 floor must anchor exactly at the first appended field
 * (wt_profile): everything through `alloc` is v0, wt_profile is the first tail
 * field. If a field is inserted before wt_profile this assert fires -- move it
 * past the floor instead. */
_Static_assert(MOQ_ENDPOINT_CFG_V0_SIZE ==
                   offsetof(moq_endpoint_cfg_t, wt_profile),
               "moq_endpoint_cfg_t v0 floor must end exactly at wt_profile");

/* Each further tail field is APPENDED after the previous tail, never inserted
 * into it: a caller compiled against the older layout keeps the same offsets
 * for every field it knows, and its buffer ends before the new one. If
 * handshake_timeout_us is ever moved ahead of wt_profile's end this fires. */
_Static_assert(offsetof(moq_endpoint_cfg_t, handshake_timeout_us) >=
                   offsetof(moq_endpoint_cfg_t, wt_profile) +
                       sizeof(((moq_endpoint_cfg_t *)0)->wt_profile),
               "handshake_timeout_us must be appended after wt_profile");

void moq_endpoint_cfg_init_sized(moq_endpoint_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg) return;
    if (cfg_size > sizeof(*cfg)) cfg_size = sizeof(*cfg);
    memset(cfg, 0, cfg_size);
    if (cfg_size >= sizeof(uint32_t))
        cfg->struct_size = (uint32_t)cfg_size;
    /* perspective (v0, CLIENT is the v0 default) -- set only when covered so we
     * never write past cfg_size for an undersized caller. */
    if (cfg_size >= offsetof(moq_endpoint_cfg_t, perspective) +
                        sizeof(cfg->perspective))
        cfg->perspective = MOQ_PERSPECTIVE_CLIENT;
}

void moq_endpoint_cfg_init(moq_endpoint_cfg_t *cfg)
{
    /* Pointer-only: clear and stamp ONLY the frozen v0 prefix, so this can
     * never overflow the buffer of an old caller that predates a tail field.
     * Tail fields (wt_profile) require moq_endpoint_cfg_init_sized(). */
    moq_endpoint_cfg_init_sized(cfg, MOQ_ENDPOINT_CFG_V0_SIZE);
}

/*
 * The ONE connect implementation. `moq_endpoint_connect()` is this with
 * `fn == NULL`, so there is no second lifecycle to drift and the public path
 * stays observably identical at every accepted `struct_size`.
 */
#if defined(MOQ_MEDIA_SENDER_TESTING) || defined(MOQ_MEDIA_RECEIVER_TESTING)
/* The one-shot connect arming slot, test builds only. An endpoint does not
 * exist until connect() allocates it, so observers for its create window are
 * armed here and moved onto the next endpoint allocated; the slot is empty
 * again as soon as that happens. */
static pthread_mutex_t ep_test_slot_mu = PTHREAD_MUTEX_INITIALIZER;
static bool   ep_test_slot_armed;
static void (*ep_test_slot_after_create)(void *);
static void (*ep_test_slot_first_established)(void *, moq_endpoint_state_t,
                                              moq_version_t, moq_version_t, bool);
static void  *ep_test_slot_ctx;

#if defined(MOQ_SERVICE_HAVE_PQ_THREADED) || \
    defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_MVFST_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_MSQUIC_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED)
/* Moves an armed slot onto a freshly allocated endpoint. Only a build with a
 * managed facade allocates one: without a facade, connect() returns
 * MOQ_ERR_UNSUPPORTED first and leaves any arming in the slot. */
static void ep_test_take_connect_hooks(moq_endpoint_t *ep)
{
    pthread_mutex_lock(&ep_test_slot_mu);
    if (ep_test_slot_armed) {
        ep->test_after_facade_create = ep_test_slot_after_create;
        ep->test_first_established = ep_test_slot_first_established;
        ep->test_connect_ctx = ep_test_slot_ctx;
        ep_test_slot_armed = false;
        ep_test_slot_after_create = NULL;
        ep_test_slot_first_established = NULL;
        ep_test_slot_ctx = NULL;
    }
    pthread_mutex_unlock(&ep_test_slot_mu);
}
#endif /* any managed facade */

/* Arm observers for the NEXT endpoint connect() allocates. after_create runs on
 * the connecting thread once a picoquic-threaded or pico_wt managed facade
 * exists and before the endpoint publishes it; first_established runs on the network thread, under
 * ep->mu, at the first pump cycle that settles ESTABLISHED. Refused while a
 * previous arming is still unconsumed. */
moq_result_t moq_endpoint_test_arm_next_connect(
    void (*after_create)(void *),
    void (*first_established)(void *, moq_endpoint_state_t, moq_version_t,
                              moq_version_t, bool),
    void *ctx)
{
    pthread_mutex_lock(&ep_test_slot_mu);
    if (ep_test_slot_armed) {
        pthread_mutex_unlock(&ep_test_slot_mu);
        return MOQ_ERR_WRONG_STATE;
    }
    ep_test_slot_armed = true;
    ep_test_slot_after_create = after_create;
    ep_test_slot_first_established = first_established;
    ep_test_slot_ctx = ctx;
    pthread_mutex_unlock(&ep_test_slot_mu);
    return MOQ_OK;
}

/* Whether an arming is still waiting for an endpoint. */
bool moq_endpoint_test_connect_armed(void)
{
    pthread_mutex_lock(&ep_test_slot_mu);
    bool armed = ep_test_slot_armed;
    pthread_mutex_unlock(&ep_test_slot_mu);
    return armed;
}

/* Clear an unconsumed arming, for a fixture's failure path. */
void moq_endpoint_test_disarm_next_connect(void)
{
    pthread_mutex_lock(&ep_test_slot_mu);
    ep_test_slot_armed = false;
    ep_test_slot_after_create = NULL;
    ep_test_slot_first_established = NULL;
    ep_test_slot_ctx = NULL;
    pthread_mutex_unlock(&ep_test_slot_mu);
}
#endif

moq_result_t moq_endpoint_connect_with_trust(const moq_endpoint_cfg_t *cfg,
                                             moq_endpoint_trust_fn fn,
                                             void *ctx,
                                             moq_endpoint_t **out)
{
    if (!cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;

    /* The frozen v0 floor is validated BEFORE any config field other than
     * struct_size is read. A caller that supplies a shorter object must be
     * refused, not read out of bounds: the trust fields below live inside the
     * v0 prefix, so a short object does not contain them. */
    if (cfg->struct_size < MOQ_ENDPOINT_CFG_V0_SIZE) return MOQ_ERR_INVAL;

    /* Conflict checks come first, before allocation and any TLS activity: a
     * delegate together with insecure_skip_verify or an explicit CA file is two
     * trust sources for one connection, and neither silently wins. */
    if (fn != NULL) {
        if (cfg->insecure_skip_verify) return MOQ_ERR_INVAL;
        if (cfg->ca_file.len > 0)      return MOQ_ERR_INVAL;
    }

    moq_endpoint_resolved_t r;
    moq_result_t rc = moq_endpoint_resolve_cfg(cfg, &r);
    if (rc < 0) return rc;

    /* Resolution is a prerequisite to the delegate decision, because only the
     * SELECTED backend can honour one. Every non-picoquic create path ignores
     * the delegate entirely, so accepting it there would silently verify with
     * that backend's own TLS policy -- the delegate would be requested and not
     * applied. That is refused here, before allocation, facade creation and
     * either verifier setter. */
    if (fn != NULL) {
        if (r.backend != MOQ_TRANSPORT_BACKEND_PICOQUIC)
            return MOQ_ERR_UNSUPPORTED;
#if defined(MOQ_SERVICE_HAVE_PQ_THREADED) || defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED)
        /* ... and the picoquic provider must actually be able to apply it. */
        if (!moq_picoquic_cert_verify_callback_supported())
            return MOQ_ERR_UNSUPPORTED;
#else
        return MOQ_ERR_UNSUPPORTED;   /* no picoquic facade in this build */
#endif
    }


#if !defined(MOQ_SERVICE_HAVE_PQ_THREADED) && \
    !defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED) && \
    !defined(MOQ_SERVICE_HAVE_MVFST_MANAGED) && \
    !defined(MOQ_SERVICE_HAVE_MSQUIC_MANAGED) && \
    !defined(MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED) && \
    !defined(MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED) && \
    !defined(MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED)
    /* No facade exists to carry a delegate, so `ctx` is never stored in this
     * configuration; the delegate itself was already refused above. */
    (void)ctx;
    return MOQ_ERR_UNSUPPORTED;   /* no managed facade in this build */
#else
    if (r.protocol == MOQ_TRANSPORT_PROTOCOL_RAW_QUIC) {
#if !defined(MOQ_SERVICE_HAVE_PQ_THREADED) && \
    !defined(MOQ_SERVICE_HAVE_MVFST_MANAGED) && \
    !defined(MOQ_SERVICE_HAVE_MSQUIC_MANAGED)
        return MOQ_ERR_UNSUPPORTED;   /* no raw-QUIC facade built */
#endif
    } else {
#if !defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED) && \
    !defined(MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED) && \
    !defined(MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED) && \
    !defined(MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED)
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
    ep->handshake_timeout_us = r.handshake_timeout_us;
    atomic_init(&ep->interrupted, false);
    atomic_init(&ep->closed, false);
    pthread_mutex_init(&ep->mu, NULL);
    ep->pump_state = MOQ_ENDPOINT_CONNECTING;
#if defined(MOQ_MEDIA_SENDER_TESTING) || defined(MOQ_MEDIA_RECEIVER_TESTING)
    ep_test_take_connect_hooks(ep);
#endif

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

#if defined(MOQ_SERVICE_HAVE_PQ_THREADED) || \
    defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED)
    /* Preflight the CA bundle: a file that cannot be loaded is a configuration
     * error, reported here as MOQ_ERR_INVAL rather than collapsing into a
     * generic transport/handshake failure inside the verifier install. */
    if (r.backend == MOQ_TRANSPORT_BACKEND_PICOQUIC &&
        !cfg->insecure_skip_verify &&
        !moq_picoquic_ca_file_loadable(ep->ca_file)) {
        ep_free_strings(ep);
        pthread_mutex_destroy(&ep->mu);
        alloc->free(ep, sizeof(*ep), alloc->ctx);
        return MOQ_ERR_INVAL;
    }
#endif

    /* The delegate is stored before any create runs, so the configure hook the
     * facade invokes already sees it. It stays valid for the endpoint's
     * lifetime; `ctx` is caller-owned and is not copied. */
    ep->trust_fn = fn;
    ep->trust_ctx = ctx;

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

/* The public entry: exactly the implementation above with no delegate, so its
 * observable behaviour is unchanged for every accepted `struct_size`. */
moq_result_t moq_endpoint_connect(const moq_endpoint_cfg_t *cfg,
                                  moq_endpoint_t **out)
{
    return moq_endpoint_connect_with_trust(cfg, NULL, NULL, out);
}

/* -- Lifecycle ---------------------------------------------------------- */

moq_result_t moq_endpoint_attach_hook(moq_endpoint_t *ep,
                                      moq_endpoint_hook_kind_t kind,
                                      const moq_endpoint_hook_ops_t *ops,
                                      void *ctx)
{
    if (!ep || !ops || !ops->pump ||
        kind < 0 || kind >= MOQ_ENDPOINT_HOOK_KIND_COUNT)
        return MOQ_ERR_INVAL;
    pthread_mutex_lock(&ep->mu);
    if (ep->stopped || ep->hook_ops[kind].pump) {
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
    ep->hook_ops[kind] = *ops;          /* copied by value */
    ep->hook_ctx[kind] = ctx;
    ep->attachments++;
    pthread_mutex_unlock(&ep->mu);
    return MOQ_OK;
}

/* Aggregate app-service deadline across attached hooks (ctx = the endpoint).
 * Runs on the managed context after a pump, OUTSIDE the pump's exclusive window;
 * takes ep->mu then each hook's own lock (ep->mu -> s->mu) and calls only the
 * hooks' pure cached next_deadline_us reads. */
uint64_t moq_endpoint_app_deadline_us(void *ctx)
{
    moq_endpoint_t *ep = (moq_endpoint_t *)ctx;
    if (!ep) return UINT64_MAX;
    uint64_t d = UINT64_MAX;
    pthread_mutex_lock(&ep->mu);
    for (int k = 0; k < MOQ_ENDPOINT_HOOK_KIND_COUNT; k++) {
        if (ep->hook_ops[k].pump && ep->hook_ops[k].next_deadline_us) {
            uint64_t hd = ep->hook_ops[k].next_deadline_us(ep->hook_ctx[k]);
            if (hd < d) d = hd;
        }
    }
    pthread_mutex_unlock(&ep->mu);
    return d;
}

/* Aggregate attached-service drain status. Same lock discipline as
 * moq_endpoint_app_deadline_us: ep->mu, then each hook's own lock inside its
 * pure cached read, and ep->mu is never held across a backend/facade call or a
 * wait. FAILED wins over PENDING, which wins over IDLE. */
moq_endpoint_send_drain_status_t
moq_endpoint_hooks_send_drain_status(moq_endpoint_t *ep)
{
    if (!ep) return MOQ_EP_SEND_DRAIN_IDLE;
    moq_endpoint_send_drain_status_t agg = MOQ_EP_SEND_DRAIN_IDLE;
    pthread_mutex_lock(&ep->mu);
    for (int k = 0; k < MOQ_ENDPOINT_HOOK_KIND_COUNT; k++) {
        if (!ep->hook_ops[k].pump || !ep->hook_ops[k].send_drain_status)
            continue;
        moq_endpoint_send_drain_status_t st =
            ep->hook_ops[k].send_drain_status(ep->hook_ctx[k]);
        if (st > agg) agg = st;          /* FAILED > PENDING > IDLE */
    }
    pthread_mutex_unlock(&ep->mu);
    return agg;
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
    if (ep->hook_ops[kind].pump && ep->hook_ctx[kind] == ctx) {
        ep->hook_ops[kind] = (moq_endpoint_hook_ops_t){0};
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
    if (!ep->stopped)
        ep->observe_gen++;
    ep->stopped = true;
    atomic_store(&ep->closed, true);   /* mirror for ep->mu-free readers */
    pthread_mutex_unlock(&ep->mu);

    /* The managed context is quiesced: drain accepted tasks with the
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
        if (!ep->stopped)
            ep->observe_gen++;
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

    /* PREFLIGHT, once, before the loop: one backend drain_state call purely to
     * classify capability and thread. App-thread only, and only backends that
     * can actually prove drain -- the managed context drives the flush, so
     * calling this from a hook would deadlock the very progress it waits on.
     * The value is not used as a flush answer here; the loop below re-asks. */
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

        /* (1) Attached-service output first WITHIN THIS ITERATION: while any
         * hook still owes work, this iteration does not go on to take a
         * transport progress snapshot. (The one preflight call above already
         * happened, for capability/thread only -- so it is not true that the
         * transport is never consulted before the hooks, and the hook query is
         * not literally the first backend call.) An attached hook can hold
         * work its application already handed it that has not reached the core
         * yet; a transport snapshot taken then describes an idle wire that
         * says nothing about the flush the caller asked for. The backend's own
         * cycle runs the hook and then services the session, so once no hook
         * reports pending, the work has entered the core and the transport
         * probe becomes the meaningful question. This query alone never proves
         * transport handoff -- that remains the backend's answer.
         *
         * Order per progress iteration: attachment status, then the transport
         * progress snapshot, then the final attachment recheck below. */
        moq_endpoint_send_drain_status_t hst =
            moq_endpoint_hooks_send_drain_status(ep);
        /* An attachment that terminalized holding accepted output can never
         * hand it over, and its endpoint may still be established and
         * transport-idle. Reporting success there would promise a flush that
         * never happened. */
        if (hst == MOQ_EP_SEND_DRAIN_FAILED) return MOQ_ERR_CLOSED;
        if (hst == MOQ_EP_SEND_DRAIN_PENDING) {
            uint64_t hel = ep_mono_now_us() - start;
            if (hel >= timeout_us) return MOQ_DONE;
            uint64_t hrem = timeout_us - hel;
            uint64_t hslice = hrem < EP_DRAIN_POLL_SLICE_US
                            ? hrem : EP_DRAIN_POLL_SLICE_US;
            ep_facade_wake(ep);
            moq_result_t hwr = moq_endpoint_wait(ep, hslice);
            if (hwr == MOQ_ERR_INTERRUPTED) return MOQ_ERR_INTERRUPTED;
            continue;
        }

        /* (2) the backend's own local-transport answer. */
        ds = ep_facade_drain_state(ep);
        if (ds == EP_DRAIN_WRONG_THREAD) return MOQ_ERR_WRONG_STATE;
        if (ds == EP_DRAIN_DONE) {
            /* (3) Final recheck. Steps (1) and (2) read two separate
             * snapshots, and the managed context runs between them: it can
             * accept new hook work, or terminalize while holding work, after
             * step (1) answered IDLE. Re-reading the aggregate here re-tests
             * the hook answer against the same instant the transport was
             * observed idle, so success is not returned on a stale snapshot.
             * This is a defensive recheck of THAT window only -- it is not a
             * synchronization guarantee about an application still writing
             * concurrently, which the caller is required to have quiesced. */
            moq_endpoint_send_drain_status_t fst =
                moq_endpoint_hooks_send_drain_status(ep);
            if (fst == MOQ_EP_SEND_DRAIN_FAILED) return MOQ_ERR_CLOSED;
            if (fst == MOQ_EP_SEND_DRAIN_IDLE) return MOQ_OK;
        }

        uint64_t elapsed = ep_mono_now_us() - start;
        if (elapsed >= timeout_us) return MOQ_DONE;
        uint64_t remaining = timeout_us - elapsed;
        uint64_t slice = remaining < EP_DRAIN_POLL_SLICE_US
                       ? remaining : EP_DRAIN_POLL_SLICE_US;
        /* Actively kick the managed context so it makes send progress and
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

/* A per-endpoint, one-shot interposition point, test builds only.
 *
 * It lets a test drive a real transition at an exact position INSIDE an
 * observation accessor, which is what makes a composed implementation tear
 * behaviourally rather than being judged by its shape. Caller-owned function
 * pointer, stored on the endpoint (never a global), fired at most once, and
 * absent from every shipped build. */
#if defined(MOQ_MEDIA_SENDER_TESTING) || defined(MOQ_MEDIA_RECEIVER_TESTING)
static void ep_test_fire_interpose(const moq_endpoint_t *ep, int at)
{
    moq_endpoint_t *mep = (moq_endpoint_t *)ep;
    /* Take and clear the one-shot UNDER ep->mu, so arming and firing cannot
     * race. The callback is then invoked with the mutex RELEASED: it drives
     * real endpoint work and may take ep->mu itself, and caller code must
     * never run under this lock. */
    pthread_mutex_lock(&mep->mu);
    void (*fn)(void *) = NULL;
    void *ctx = NULL;
    if (mep->test_interpose && mep->test_interpose_at == at) {
        fn = mep->test_interpose;
        ctx = mep->test_interpose_ctx;
        mep->test_interpose = NULL;
        mep->test_interpose_ctx = NULL;
        mep->test_interpose_fires++;
    }
    pthread_mutex_unlock(&mep->mu);
    if (fn) fn(ctx);
}
#define EP_TEST_INTERPOSE(ep, at) ep_test_fire_interpose((ep), (at))
#else
#define EP_TEST_INTERPOSE(ep, at) ((void)0)
#endif

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

/* The SINGLE source of terminal classification.
 *
 * Both moq_endpoint_get_terminal() and moq_endpoint_get_snapshot() answer from
 * here, so the legacy accessor and the snapshot cannot disagree. It honours the
 * vtable contract that a NULL `terminal` slot receives GENERIC classification
 * from the facade's fatal/code/clean inputs -- previously the slot was simply
 * skipped and every such backend reported NONE forever.
 *
 * Never called with ep->mu held: a facade terminal function may take a
 * facade-internal lock, and the endpoint must not impose a lock order on it. */
static void ep_terminal_sample(const moq_endpoint_t *ep,
                               moq_endpoint_terminal_reason_t *reason,
                               uint64_t *detail)
{
    *reason = MOQ_ENDPOINT_TERMINAL_NONE;
    *detail = 0;
    if (!ep->vt || !ep->facade)
        return;                     /* no facade: nothing to classify */
    if (ep->vt->terminal) {
        ep->vt->terminal(ep->facade, reason, detail);
        return;
    }
    ep_classify_generic(ep_facade_is_fatal(ep), ep_facade_fatal_code(ep),
                        ep_facade_clean_closed(ep), reason, detail);
}

/* A STABLE terminal classification.
 *
 * One sample is not an observation. Every shipped vt->terminal composes
 * separately-locked facade accessors -- msquic_managed.c latches term_fatal,
 * term_closed and both codes together under m->mu (:438-444) while each public
 * accessor takes that mutex on its own (:2874+), and pico_wt is the same shape
 * -- so a backend may latch a whole new terminal BETWEEN the reads of one
 * sample. A sample that straddles the latch can report a combination the
 * facade never held: fatal read before, clean read after, classified CLEAN.
 *
 * The endpoint's own observation generation cannot see that: nothing
 * endpoint-local moved.
 *
 * So samples are repeated until two CONSECUTIVE complete samples agree on both
 * reason and detail. That terminates because facade terminal state is
 * finite and latching -- it is a one-way transition per backend -- so a sample
 * that straddles it differs from the next settled sample, and once settled
 * every sample agrees. A transition after the final sample merely makes the
 * answer stale at the next instant, which the contract allows.
 *
 * Never called with ep->mu held: a facade terminal function may take a
 * facade-internal lock, and the endpoint must not impose a lock order on it. */
static void ep_terminal_classify(const moq_endpoint_t *ep,
                                 moq_endpoint_terminal_reason_t *reason,
                                 uint64_t *detail)
{
    ep_terminal_sample(ep, reason, detail);
    for (;;) {
        moq_endpoint_terminal_reason_t r2;
        uint64_t d2;
        ep_terminal_sample(ep, &r2, &d2);
        if (r2 == *reason && d2 == *detail)
            return;                 /* two consecutive samples agree */
        *reason = r2;
        *detail = d2;
    }
}

moq_result_t moq_endpoint_get_terminal(const moq_endpoint_t *ep,
                                       moq_endpoint_terminal_t *out,
                                       size_t out_size)
{
    if (!ep || !out || out_size < MOQ_ENDPOINT_TERMINAL_V0_SIZE)
        return MOQ_ERR_INVAL;
    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);

    moq_endpoint_terminal_t t;
    memset(&t, 0, sizeof(t));
    ep_terminal_classify(ep, &t.reason, &t.detail_code);

    t.struct_size = (uint32_t)n;
    memcpy(out, &t, n);
    return MOQ_OK;
}

/* One coherent observation. See the contract in endpoint.h.
 *
 * The mechanism is a private OBSERVATION GENERATION, and it exists because
 * monotonicity is not enough. Sampling the terminal first and then reading the
 * local pair admits this ordering:
 *
 *   1. the terminal classifies as NONE while the endpoint is CONNECTING/0;
 *   2. the facade goes terminal;
 *   3. first establishment commits ESTABLISHED/version under ep->mu;
 *   4. the reader pairs that new local tuple with the stale NONE.
 *
 * The endpoint was already terminal when it established, so no observer could
 * have seen ESTABLISHED/version/NONE. A NONE sampled early HIDES a terminal
 * that happened afterwards, which is exactly the case monotonicity cannot
 * cover.
 *
 * So the read is optimistic and validated:
 *
 *   capture (generation, stopped, pump_state, negotiated) under ep->mu;
 *   classify the terminal OUTSIDE ep->mu -- a facade terminal function may take
 *     a facade-internal lock, and the endpoint must not impose a lock order;
 *   re-read the generation under ep->mu;
 *   retry if it moved.
 *
 * A generation that did not move means no observable tuple mutation crossed
 * the terminal sample, so the pair and the reason describe the SAME instant.
 *
 * Retry terminates because the generation advances only on a real change of
 * the observable tuple, and those changes are finite: `negotiated` latches
 * once, `stopped` latches once, and `pump_state` follows a converging
 * lifecycle. A same-value pump refresh deliberately does not advance it, so an
 * idle endpoint being pumped cannot starve a reader. (The one non-monotone
 * write -- a lane whose reaped child makes the facade report a NULL session --
 * is a single transition per reap, not a cycle.)
 *
 * A classified terminal still forces CLOSED: the facade is authoritative about
 * being terminal, and the cached pump_state may not have caught up. */
moq_result_t moq_endpoint_get_snapshot(const moq_endpoint_t *ep,
                                       moq_endpoint_snapshot_t *out,
                                       size_t out_size)
{
    if (!ep || !out || out_size < MOQ_ENDPOINT_SNAPSHOT_V0_SIZE)
        return MOQ_ERR_INVAL;      /* no caller byte is written */
    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);

    moq_endpoint_snapshot_t s;
    memset(&s, 0, sizeof(s));      /* every written byte is deterministic */

    moq_endpoint_t *mep = (moq_endpoint_t *)ep;
    for (;;) {
        uint64_t gen;
        pthread_mutex_lock(&mep->mu);
        gen = ep->observe_gen;
        s.state = ep->stopped ? MOQ_ENDPOINT_CLOSED : ep->pump_state;
        s.negotiated_version = ep->negotiated;
        pthread_mutex_unlock(&mep->mu);

        ep_terminal_classify(ep, &s.terminal_reason, &s.terminal_detail_code);

        pthread_mutex_lock(&mep->mu);
        uint64_t gen2 = ep->observe_gen;
        pthread_mutex_unlock(&mep->mu);
        if (gen == gen2)
            break;                 /* nothing crossed the terminal sample */
    }

    if (s.terminal_reason != MOQ_ENDPOINT_TERMINAL_NONE)
        s.state = MOQ_ENDPOINT_CLOSED;

    s.struct_size = (uint32_t)n;
    memcpy(out, &s, n);            /* an oversized caller tail is untouched */
    return MOQ_OK;
}

moq_version_t moq_endpoint_negotiated_version(const moq_endpoint_t *ep)
{
    if (!ep) return (moq_version_t)0;
    EP_TEST_INTERPOSE(ep, 1);      /* before the version is read */
    moq_endpoint_t *mep = (moq_endpoint_t *)ep;
    pthread_mutex_lock(&mep->mu);
    moq_version_t v = ep->negotiated;
    pthread_mutex_unlock(&mep->mu);
    EP_TEST_INTERPOSE(ep, 2);      /* after the version is read */
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
         * runs on the managed context (or its post-quiesce drain) -- never
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

    /* Wake the managed context so the task runs promptly. A terminal wake is
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

moq_endpoint_t *moq_endpoint_test_make_bare_alloc(const moq_alloc_t *alloc)
{
    if (!alloc) alloc = moq_alloc_default();
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

moq_endpoint_t *moq_endpoint_test_make_bare(void)
{
    return moq_endpoint_test_make_bare_alloc(moq_alloc_default());
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

/* Run one full pump cycle (the real ep_pump_cycle: the session-gated task drain
 * + state refresh + hooks). Drives the NULL-vs-live post() contract directly. */
void moq_endpoint_test_pump_cycle(moq_endpoint_t *ep, moq_session_t *session,
                                  uint64_t now_us)
{
    ep_pump_cycle(ep, session, now_us);
}

/* Run the terminal drain (the sole NULL-session task drain, §5.4): every queued
 * task runs exactly once with the closed marker. */
void moq_endpoint_test_drain_terminal(moq_endpoint_t *ep)
{
    ep_drain_terminal(ep);
}

/* Current media-service attachment count (the value stop() gates on). */
int moq_endpoint_test_attachments(moq_endpoint_t *ep);
int moq_endpoint_test_attachments(moq_endpoint_t *ep)
{
    pthread_mutex_lock(&ep->mu);
    int n = ep->attachments;
    pthread_mutex_unlock(&ep->mu);
    return n;
}

/* -- Scripted facade probe (attached-service drain tests) --------------- *
 * A facade answers moq_endpoint_drain()'s transport question via
 * vt->drain_state. Every in-tree drain test reaches that view through a real
 * socket, which cannot express "the transport is idle while an attached
 * service still owes work" deterministically. This seam installs a facade that
 * REPORTS a caller-chosen drain state and COUNTS the drain-state / wake / wait
 * calls it receives.
 *
 * It exposes state; it does not decide outcomes. It performs no drain, queues
 * nothing, and does not alter moq_endpoint_drain()'s logic -- swapping the
 * reported value cannot make a pending service obligation complete. It does
 * NOT service wakes spontaneously: wake() only records. A test may install an
 * optional managed-context stand-in, and then EXACTLY ONE recorded wake is
 * serviced per wait(), from the wait path only -- i.e. never before the
 * product has asked the facade for progress. Its storage is CALLER-OWNED and
 * it allocates nothing, so an endpoint carrying it has no hidden allocation of
 * its own. It refuses to install over an existing facade rather than
 * overwriting (and leaking) one. Compiled only under the service testing gate;
 * never part of a shipped endpoint. */
struct ep_test_probe {
    int      report;
    unsigned drain_calls;
    unsigned wakes;
    unsigned waits;
    /* Optional managed-context stand-in. A real backend answers a wake by
     * running its next cycle; this probe answers one RECORDED wake by calling
     * the caller-supplied service function ONCE, from the wait path only --
     * i.e. only after the product has asked the facade for progress. It never
     * pre-pumps (wake() itself only records), never services more than one
     * recorded wake per wait, and cannot influence what a hook reports. */
    void   (*service)(void *ctx);
    void    *service_ctx;
    /* Optional one-shot fired from the drain_state probe: models the managed
     * context accepting new send work while the transport snapshot is being
     * taken. Used only to exercise the final recheck. */
    void   (*on_probe_once)(void *ctx);
    void    *on_probe_ctx;
    unsigned on_probe_at;   /* fire when drain_calls reaches this ordinal */
    /* One-shot fired from INSIDE the facade's terminal classification: it
     * models a real transition landing partway through an observation. */
    /* ONE one-shot slot with a position selector, rather than a slot per
     * position: only one is ever armed at a time, and the probe's storage is
     * caller-owned and size-checked by every consumer. */
    void   (*on_terminal_once)(void *ctx);
    void    *on_terminal_ctx;
    unsigned char terminal_hook_at;   /* EP_PROBE_HOOK_* */
    unsigned char hook_fires;
    unsigned char samples;
    unsigned serviced;      /* wakes answered so far */
    unsigned service_calls; /* service invocations (== serviced) */
    /* Scripted observation state. Zero-initialised by install, so a probe that
     * never sets these behaves exactly as before: version 0, not fatal, not
     * cleanly closed. It lets a test drive a REAL first-establishment or
     * terminal transition through ep_pump_cycle() without a socket -- the
     * endpoint's own commit path still decides what is latched. */
    moq_version_t negotiated;
    bool          fatal;
    uint64_t      fatal_code_v;
    bool          clean;
};

static moq_result_t ep_probe_wait(void *f, uint64_t t)
{
    (void)t;
    struct ep_test_probe *p = (struct ep_test_probe *)f;
    p->waits++;
    /* Answer at most ONE recorded-but-unserviced wake per wait. With no
     * service function installed this is the original behaviour: a timeout
     * during which nothing is serviced. */
    if (p->service && p->serviced < p->wakes) {
        p->serviced++;
        p->service_calls++;
        p->service(p->service_ctx);
        return MOQ_OK;                     /* woken: the caller re-checks */
    }
    return MOQ_DONE;                       /* timeout: nothing is serviced */
}
static void ep_probe_wake(void *f)
{
    ((struct ep_test_probe *)f)->wakes++;   /* recorded, deliberately unserviced */
}
/* Where a probe one-shot fires within one terminal classification sample. */
#define EP_PROBE_HOOK_BEFORE 0   /* before the sample's reads */
#define EP_PROBE_HOOK_MID    1   /* between the composed reads */
#define EP_PROBE_HOOK_AFTER  2   /* after the result is computed */

static void ep_probe_fire_hook(struct ep_test_probe *p, unsigned char at);

static bool ep_probe_is_fatal(void *f)
{
    struct ep_test_probe *p = (struct ep_test_probe *)f;
    ep_probe_fire_hook(p, EP_PROBE_HOOK_BEFORE);
    return p->fatal;
}
static uint64_t ep_probe_fatal_code(void *f)
{
    return ((struct ep_test_probe *)f)->fatal_code_v;
}
static bool ep_probe_clean_closed(void *f)
{
    return ((struct ep_test_probe *)f)->clean;
}
static moq_version_t ep_probe_ver(void *f)
{
    return ((struct ep_test_probe *)f)->negotiated;
}
/* The probe scripts the facade's INPUTS -- fatal, its code, cleanly-closed --
 * and the product's own classifier turns them into a reason. It does not choose
 * the reason: a test cannot ask for an output the classifier would not produce
 * from those inputs. */
static void ep_probe_fire_hook(struct ep_test_probe *p, unsigned char at)
{
    if (!p->on_terminal_once || p->terminal_hook_at != at) return;
    void (*fn)(void *) = p->on_terminal_once;
    void *c = p->on_terminal_ctx;
    p->on_terminal_once = NULL;          /* one-shot */
    p->on_terminal_ctx = NULL;
    if (p->hook_fires < 255) p->hook_fires++;
    fn(c);
}

static void ep_probe_terminal(void *f, moq_endpoint_terminal_reason_t *r,
                              uint64_t *d)
{
    struct ep_test_probe *p = (struct ep_test_probe *)f;
    ep_probe_fire_hook(p, EP_PROBE_HOOK_BEFORE);
    if (p->samples < 255) p->samples++;
    /* Composed reads with EXPLICIT ordering, modelling the real shims: every
     * shipped vt->terminal reads fatal, its code and clean-closed through
     * separately-locked facade accessors, so the backend may latch a new
     * terminal between them. (Argument evaluation order is unspecified in C,
     * so the reads are sequenced here deliberately.) */
    bool     fatal = p->fatal;
    uint64_t code  = p->fatal_code_v;
    ep_probe_fire_hook(p, EP_PROBE_HOOK_MID);
    bool clean = p->clean;
    ep_classify_generic(fatal, code, clean, r, d);
    /* Fired AFTER this classification is computed and BEFORE it is returned.
     * That is the only point at which a test can make the world move on while
     * an already-sampled terminal answer is still in flight -- the boundary a
     * terminal-first getter has to survive. The hook cannot choose the answer:
     * `*r`/`*d` are already set and are not touched again. */
    ep_probe_fire_hook(p, EP_PROBE_HOOK_AFTER);
}
static moq_result_t  ep_probe_stop(void *f) { (void)f; return MOQ_OK; }
static void     ep_probe_destroy(void *f) { (void)f; }   /* caller-owned */
static int      ep_probe_drain_state(void *f)
{
    struct ep_test_probe *p = (struct ep_test_probe *)f;
    p->drain_calls++;
    if (p->on_probe_once && p->drain_calls >= p->on_probe_at) {
        void (*fn)(void *) = p->on_probe_once;
        void *c = p->on_probe_ctx;
        p->on_probe_once = NULL;            /* one-shot */
        p->on_probe_ctx = NULL;
        fn(c);
    }
    return p->report;
}

/* Designated: a future vtable reorder cannot silently rebind these. */
static const ep_backend_vtable_t EP_TEST_PROBE_VT = {
    .wait = ep_probe_wait,
    .wake = ep_probe_wake,
    .negotiated_version = ep_probe_ver,
    .is_fatal = ep_probe_is_fatal,
    .fatal_code = ep_probe_fatal_code,
    .clean_closed = ep_probe_clean_closed,
    .drain_state = ep_probe_drain_state,
    .stop = ep_probe_stop,
    .destroy = ep_probe_destroy,
    .terminal = ep_probe_terminal,
};

/* A SECOND designated vtable, identical to the probe's except that its
 * `terminal` slot is NULL. The vtable field documents NULL as "treated as
 * generic (NONE/PROTOCOL/TRANSPORT/CLEAN only)", so this is the shape that
 * tests whether moq_endpoint_get_terminal() honours that. Same caller-owned
 * storage; it allocates nothing. */
static const ep_backend_vtable_t EP_TEST_PROBE_NULL_TERMINAL_VT = {
    .wait = ep_probe_wait,
    .wake = ep_probe_wake,
    .negotiated_version = ep_probe_ver,
    .is_fatal = ep_probe_is_fatal,
    .fatal_code = ep_probe_fatal_code,
    .clean_closed = ep_probe_clean_closed,
    .drain_state = ep_probe_drain_state,
    .stop = ep_probe_stop,
    .destroy = ep_probe_destroy,
    .terminal = NULL,
};

size_t moq_endpoint_test_probe_size(void) { return sizeof(struct ep_test_probe); }

moq_result_t moq_endpoint_test_install_probe(moq_endpoint_t *ep, void *storage,
                                             size_t size, int report)
{
    if (!ep || !storage || size < sizeof(struct ep_test_probe))
        return MOQ_ERR_INVAL;
    if (ep->vt || ep->facade) return MOQ_ERR_WRONG_STATE;   /* never overwrite */
    struct ep_test_probe *p = (struct ep_test_probe *)storage;
    memset(p, 0, sizeof(*p));
    p->report = report;
    ep->vt = &EP_TEST_PROBE_VT;
    ep->facade = p;
    return MOQ_OK;
}

/* Install the same probe behind the NULL-terminal vtable. */
moq_result_t moq_endpoint_test_install_probe_null_terminal(moq_endpoint_t *ep,
                                                           void *storage,
                                                           size_t size)
{
    if (!ep || !storage || size < sizeof(struct ep_test_probe))
        return MOQ_ERR_INVAL;
    if (ep->vt || ep->facade) return MOQ_ERR_WRONG_STATE;   /* never overwrite */
    struct ep_test_probe *p = (struct ep_test_probe *)storage;
    memset(p, 0, sizeof(*p));
    ep->vt = &EP_TEST_PROBE_NULL_TERMINAL_VT;
    ep->facade = p;
    return MOQ_OK;
}

unsigned moq_endpoint_test_probe_drain_calls(const void *probe)
{
    return probe ? ((const struct ep_test_probe *)probe)->drain_calls : 0u;
}

unsigned moq_endpoint_test_probe_wakes(const void *probe)
{
    return probe ? ((const struct ep_test_probe *)probe)->wakes : 0u;
}

unsigned moq_endpoint_test_probe_waits(const void *probe)
{
    return probe ? ((const struct ep_test_probe *)probe)->waits : 0u;
}

unsigned moq_endpoint_test_probe_services(const void *probe)
{
    return probe ? ((const struct ep_test_probe *)probe)->service_calls : 0u;
}

/* The version the facade reports at first establishment. The endpoint latches
 * it itself, under its own mutex, exactly as a real backend's would be. */
/* Arm the single one-shot at position `at` (0 before the sample, 1 between its
 * composed reads, 2 after its result is computed). */
void moq_endpoint_test_probe_arm_hook(void *probe, int at, void (*fn)(void *),
                                      void *ctx)
{
    if (!probe) return;
    struct ep_test_probe *p = (struct ep_test_probe *)probe;
    p->on_terminal_once = fn;
    p->on_terminal_ctx = ctx;
    p->terminal_hook_at = (unsigned char)at;
}

/* READ-ONLY inventory of the probe's terminal inputs. It adds no storage: the
 * fields already exist. A test uses it to observe what the facade holds after
 * an interposed transition, instead of setting the state again -- which would
 * observe nothing. */
void moq_endpoint_test_probe_get_terminal(const void *probe, bool *fatal,
                                          uint64_t *fatal_code, bool *clean)
{
    const struct ep_test_probe *p = (const struct ep_test_probe *)probe;
    if (!p) return;
    if (fatal)      *fatal = p->fatal;
    if (fatal_code) *fatal_code = p->fatal_code_v;
    if (clean)      *clean = p->clean;
}

unsigned moq_endpoint_test_probe_hook_fires(const void *probe)
{
    return probe ? ((const struct ep_test_probe *)probe)->hook_fires : 0u;
}

unsigned moq_endpoint_test_probe_terminal_samples(const void *probe)
{
    return probe ? ((const struct ep_test_probe *)probe)->samples : 0u;
}

/* Arm the per-endpoint one-shot interposition at point `at`: 1 = before the
 * negotiated version is read, 2 = after it. Taken and cleared under ep->mu;
 * the callback runs with the mutex RELEASED (see ep_test_fire_interpose). */
void moq_endpoint_test_arm_interpose(moq_endpoint_t *ep, int at,
                                     void (*fn)(void *), void *ctx)
{
    if (!ep) return;
    pthread_mutex_lock(&ep->mu);
    ep->test_interpose = fn;
    ep->test_interpose_ctx = ctx;
    ep->test_interpose_at = at;
    pthread_mutex_unlock(&ep->mu);
}

unsigned moq_endpoint_test_interpose_fires(const moq_endpoint_t *ep)
{
    if (!ep) return 0u;
    moq_endpoint_t *mep = (moq_endpoint_t *)ep;
    pthread_mutex_lock(&mep->mu);
    unsigned n = ep->test_interpose_fires;
    pthread_mutex_unlock(&mep->mu);
    return n;
}

void moq_endpoint_test_probe_set_negotiated(void *probe, moq_version_t v)
{
    if (!probe) return;
    ((struct ep_test_probe *)probe)->negotiated = v;
}

/* The facade's terminal INPUTS. The endpoint's own classifier decides the
 * reported reason from them; this seam cannot select an output. */
void moq_endpoint_test_probe_set_terminal(void *probe, bool fatal,
                                          uint64_t fatal_code, bool clean)
{
    if (!probe) return;
    struct ep_test_probe *p = (struct ep_test_probe *)probe;
    p->fatal = fatal;
    p->fatal_code_v = fatal_code;
    p->clean = clean;
}

void moq_endpoint_test_probe_set_service(void *probe, void (*fn)(void *),
                                         void *ctx)
{
    if (!probe) return;
    struct ep_test_probe *p = (struct ep_test_probe *)probe;
    p->service = fn;
    p->service_ctx = ctx;
}

/* Fire `fn` ONCE, from inside the drain_state probe whose ordinal (counting
 * this probe's lifetime drain_state calls) is `at_call`. moq_endpoint_drain()
 * takes one entry probe before its loop, so `at_call` selects which probe the
 * injection lands in. */
void moq_endpoint_test_probe_arm_on_probe_once(void *probe, void (*fn)(void *),
                                               void *ctx, unsigned at_call)
{
    if (!probe) return;
    struct ep_test_probe *p = (struct ep_test_probe *)probe;
    p->on_probe_once = fn;
    p->on_probe_ctx = ctx;
    p->on_probe_at = at_call;
}

void moq_endpoint_test_remove_probe(moq_endpoint_t *ep)
{
    if (!ep) return;
    ep->vt = NULL;
    ep->facade = NULL;   /* storage is the caller's; nothing to free */
}
#endif /* MOQ_MEDIA_SENDER_TESTING || MOQ_MEDIA_RECEIVER_TESTING */
