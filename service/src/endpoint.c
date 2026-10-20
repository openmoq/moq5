#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

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

#include "../../adapters/common/moq_alpn.h"
#include "../../adapters/common/moq_setup_tokens.h"
#include <moq/wire.h>
#include "../../core/src/base/uri_internal.h"

#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>   /* clock_gettime: drain deadline (service tier may use the wall clock) */
#include <string.h>

/* Draft-22 requires exact STOP and ACK-through-FIN stream outcomes from the
 * transport. picoquic provides them from stream-terminal contract version 1;
 * the picoquic and pico_wt adapters refuse to attach a draft-22 session on a
 * picoquic without it, so the endpoint never offers draft-22 on that backend. */
#if defined(MOQ_SERVICE_HAVE_PQ_THREADED) || \
    defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED)
#include <picoquic.h>
#endif
#if defined(PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION) && \
    PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION >= 1
#define EP_PICOQUIC_HOSTS_DRAFT_22 true
#else
#define EP_PICOQUIC_HOSTS_DRAFT_22 false
#endif

#if defined(MOQ_MEDIA_SENDER_TESTING) || defined(MOQ_MEDIA_RECEIVER_TESTING)
/* Test seam: -1 keeps the build's own picoquic capability; 0 or 1 replaces it
 * so offer filtering is testable against a picoquic without the contract. */
static int ep_test_picoquic_hosts_draft_22 = -1;
void moq_endpoint_test_set_picoquic_draft_22(int hosts);
void moq_endpoint_test_set_picoquic_draft_22(int hosts)
{
    ep_test_picoquic_hosts_draft_22 = hosts;
}
#endif

/* Whether the selected backend can carry a session of version `v`. */
static bool ep_backend_hosts_version(moq_transport_backend_t backend,
                                     moq_version_t v)
{
    if (v != MOQ_VERSION_DRAFT_22 || backend != MOQ_TRANSPORT_BACKEND_PICOQUIC)
        return true;
#if defined(MOQ_MEDIA_SENDER_TESTING) || defined(MOQ_MEDIA_RECEIVER_TESTING)
    if (ep_test_picoquic_hosts_draft_22 >= 0)
        return ep_test_picoquic_hosts_draft_22 != 0;
#endif
    return EP_PICOQUIC_HOSTS_DRAFT_22;
}

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
    /* The core's registry is the single point of truth: a version is
     * supported when its wire profile is complete in this build. The offer
     * rules (§5.2) depend on this predicate being exact. */
    return moq_version_supported(v);
}

/* AUTO offers every draft this build supports, newest first. */
#define EP_DEFAULT_PREFERRED_VERSION MOQ_VERSION_DRAFT_22

static size_t default_versions(moq_version_t preferred, moq_version_t *out)
{
    const moq_version_t candidates[] = {
        preferred, MOQ_VERSION_DRAFT_22, MOQ_VERSION_DRAFT_18,
        MOQ_VERSION_DRAFT_16
    };
    _Static_assert(sizeof(candidates) / sizeof(candidates[0]) <=
                   MOQ_ENDPOINT_MAX_VERSIONS, "default offer fits");
    size_t count = 0;
    for (size_t i = 0; i < sizeof(candidates) / sizeof(candidates[0]); i++) {
        if (!moq_endpoint_version_supported(candidates[i])) continue;
        bool present = false;
        for (size_t j = 0; j < count; j++) {
            if (out[j] == candidates[i]) present = true;
        }
        if (!present) out[count++] = candidates[i];
    }
    return count;
}

/* -- Resolution ------------------------------------------------------ */

static bool bytes_empty(moq_bytes_t b) { return b.len == 0; }

/* AUTO offers the preferred version first (the newest supported draft when
 * none is named), then every other supported draft, newest first. */
static moq_result_t resolve_auto(moq_version_t preferred,
                                 moq_endpoint_resolved_t *out)
{
    out->policy = MOQ_VERSION_POLICY_AUTO;
    if (preferred != 0 && !moq_endpoint_version_supported(preferred))
        return MOQ_ERR_UNSUPPORTED;
    out->preferred_version = preferred ? preferred : EP_DEFAULT_PREFERRED_VERSION;
    out->version_count = default_versions(out->preferred_version, out->versions);
    return MOQ_OK;
}

static moq_result_t resolve_versions(const moq_version_offer_t *offer,
                                     moq_version_t preferred,
                                     moq_endpoint_resolved_t *out)
{
    /* Zero-init (struct_size 0) means AUTO per the cfg comment; a non-zero
     * but undersized struct is a malformed offer. */
    if (offer->struct_size == 0)
        return resolve_auto(preferred, out);
    if (offer->struct_size < sizeof(moq_version_offer_t)) return MOQ_ERR_INVAL;

    switch (offer->policy) {
    case MOQ_VERSION_POLICY_AUTO:
        return resolve_auto(preferred, out);
    case MOQ_VERSION_POLICY_LIST:
    case MOQ_VERSION_POLICY_EXACT:
        break;
    default:
        return MOQ_ERR_INVAL;
    }

    /* LIST and EXACT state their own order; a preference beside them is a
     * contradiction, not a hint. */
    if (preferred != 0) return MOQ_ERR_INVAL;
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

    /* URI mode: an appended tail field, read only when struct_size covers it
     * (LEGACY, the zero value, otherwise); an unknown mode is a clean
     * MOQ_ERR_INVAL. */
    if ((size_t)cfg->struct_size >=
        offsetof(moq_endpoint_cfg_t, uri_mode) + sizeof(cfg->uri_mode)) {
        if (cfg->uri_mode > MOQ_ENDPOINT_URI_STANDARD) return MOQ_ERR_INVAL;
        out->uri_mode = cfg->uri_mode;
    }

    /* URL: required. LEGACY keeps the original parser and its defaults
     * (moqt/moq/https only, so an unknown scheme is MOQ_ERR_INVAL whether or
     * not the protocol field would have overridden it). STANDARD validates
     * the encoded MOQT URI syntax first and then applies its managed-client
     * subset. */
    if (bytes_empty(cfg->url) || !cfg->url.data) return MOQ_ERR_INVAL;
    moq_result_t urc;
    if (out->uri_mode == MOQ_ENDPOINT_URI_STANDARD) {
        if (cfg->wt_path.len ||
            (cfg->sni.len && memchr(cfg->sni.data, 0, cfg->sni.len)))
            return MOQ_ERR_INVAL;
        urc = moq_endpoint_uri_parse(cfg->url, &out->uri);
        if (urc == MOQ_OK) out->url = out->uri.url;
    } else {
        /* URI fragments are local to the caller, never a connection target
         * (transport-18 section 3.1.2). Keep the core application parser's
         * contract unchanged; resolve only the server-visible prefix here. */
        moq_bytes_t connection_url = cfg->url;
        const uint8_t *fragment =
            memchr(connection_url.data, '#', connection_url.len);
        if (fragment)
            connection_url.len = (size_t)(fragment - connection_url.data);
        urc = moq_url_parse(connection_url, &out->url);
    }
    if (urc < 0) return urc;

    /* The connection target (§6.2.2): the authority as spelled and the
     * path-abempty with "?" query. STANDARD has validated both; LEGACY takes
     * them from the URL bytes and keeps them only when they follow RFC 3986
     * with a nonempty host, so a legacy URL the grammar does not admit yields
     * no target rather than a target a peer would refuse as malformed. */
    out->target.struct_size = (uint32_t)sizeof(out->target);
    if (out->uri_mode == MOQ_ENDPOINT_URI_STANDARD) {
        out->target.target_present = MOQ_BINDING_TARGET_COMPLETE;
        out->target.authority = out->uri.authority;
        out->target.path_query = out->uri.path_query;
    } else {
        size_t start = out->url.scheme.len + 3;   /* past "://" */
        size_t end = start;
        while (end < cfg->url.len && cfg->url.data[end] != '#') end++;
        size_t path = start;
        while (path < end && cfg->url.data[path] != '/' &&
               cfg->url.data[path] != '?') path++;
        moq_bytes_t authority = { cfg->url.data + start, path - start };
        moq_bytes_t path_query = { cfg->url.data + path, end - path };
        if (start <= cfg->url.len && moq_uri_authority_valid(authority) &&
            moq_uri_path_query_valid(path_query) &&
            path_query.len <= UINT16_MAX - authority.len) {
            out->target.target_present = MOQ_BINDING_TARGET_COMPLETE;
            out->target.authority = authority;
            out->target.path_query = path_query;
        }
    }

    /* Protocol: an explicit non-AUTO field WINS over the URL scheme (§5.1 --
     * the field is the override knob, a scheme/field conflict is not an
     * error). AUTO derives from the scheme. */
    switch (cfg->protocol) {
    case MOQ_TRANSPORT_PROTOCOL_AUTO: {
        /* LEGACY admits exactly moqt/moq/https spelled in lowercase;
         * STANDARD has already matched the scheme case-insensitively. */
        bool https = out->uri_mode == MOQ_ENDPOINT_URI_STANDARD
            ? out->uri.https
            : (out->url.scheme.len == 5 &&
               memcmp(out->url.scheme.data, "https", 5) == 0);
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

    /* URI policy: a server's field, carried in the connection target's shape.
     * A client endpoint names a target and never a policy; the tail is read
     * only when struct_size covers both fields. */
    if ((size_t)cfg->struct_size >=
        offsetof(moq_endpoint_cfg_t, accept_uri_ctx) + sizeof(cfg->accept_uri_ctx)) {
        if (cfg->accept_uri || cfg->accept_uri_ctx) return MOQ_ERR_INVAL;
        out->target.accept_uri = cfg->accept_uri;
        out->target.accept_uri_ctx = cfg->accept_uri_ctx;
    }

    /* SNI: explicit value wins; the mandatory default is the URL host
     * (an SNI mismatch already cost a debug round -- §5.1). */
    out->sni = bytes_empty(cfg->sni) ? out->url.host : cfg->sni;

    /* WT target. STANDARD: the raw path and query. LEGACY: an explicit value
     * wins. Otherwise preserve the URL query, including a present but empty
     * "?", after its path (default "/moq").
     * Separate borrowed spans keep resolution allocation-free even when
     * the default path comes from static storage. RAW_QUIC leaves both
     * empty (the URL path is carried in the
     * PATH setup option there, which the endpoint does not emit yet). */
    if (out->protocol == MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT) {
        if (out->uri_mode == MOQ_ENDPOINT_URI_STANDARD) {
            /* The raw path/query; connect supplies the "/" an empty path
             * projects to, leaving the URI identity as written. */
            out->wt_path = out->uri.path_query;
        } else if (!bytes_empty(cfg->wt_path)) {
            out->wt_path = cfg->wt_path;
        } else if (out->url.path.len > 0) {
            out->wt_path = out->url.path;
        } else {
            static const uint8_t k_default_path[] = "/moq";
            out->wt_path.data = k_default_path;
            out->wt_path.len = sizeof(k_default_path) - 1;
        }
        if (out->uri_mode != MOQ_ENDPOINT_URI_STANDARD &&
            bytes_empty(cfg->wt_path) && out->url.query.data) {
            out->wt_query.data = out->url.query.data - 1; /* include "?" */
            out->wt_query.len = out->url.query.len + 1;
        }
        if (out->wt_path.len == SIZE_MAX ||
            out->wt_query.len > SIZE_MAX - out->wt_path.len - 1)
            return MOQ_ERR_INVAL;
        /* LEGACY WebTransport dereferences wt_path + wt_query (an explicit
         * wt_path, or the "/moq" default), which need not be the URL's own
         * path/query. The session binds that resource, so it is the target
         * path: connect stores it, and a resource the grammar does not admit
         * yields no target, as a legacy URL does. */
        if (out->uri_mode != MOQ_ENDPOINT_URI_STANDARD &&
            out->target.target_present &&
            !(moq_uri_path_query_valid(out->wt_path) &&
              moq_uri_path_query_valid(out->wt_query) &&
              out->wt_query.len <= UINT16_MAX - out->target.authority.len &&
              out->wt_path.len <=
                  UINT16_MAX - out->target.authority.len - out->wt_query.len)) {
            out->target.target_present = 0;
            out->target.authority = (moq_bytes_t){ NULL, 0 };
            out->target.path_query = (moq_bytes_t){ NULL, 0 };
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

    /* SETUP authorization tokens: the session's own limits, and that the
     * SETUP carrying them fits the default session send arena the backends
     * use (and so a default peer's receive buffer), counting the URL as the
     * connection target; checked here so create fails only on allocation.
     * Only the picoquic facades forward them, so another backend refuses a
     * non-empty list. */
    if ((size_t)cfg->struct_size >=
            offsetof(moq_endpoint_cfg_t, setup_auth_token_count) +
            sizeof(cfg->setup_auth_token_count) &&
        cfg->setup_auth_token_count > 0) {
        if (!cfg->setup_auth_tokens || cfg->setup_auth_token_count > 16)
            return MOQ_ERR_INVAL;
        for (size_t i = 0; i < cfg->setup_auth_token_count; i++) {
            const moq_auth_token_t *t = &cfg->setup_auth_tokens[i];
            if (t->token_type > MOQ_QUIC_VARINT_MAX ||
                (t->token_value.len && !t->token_value.data) ||
                t->token_value.len > UINT16_MAX)
                return MOQ_ERR_INVAL;
        }
        if (!moq_setup_tokens_fit(cfg->setup_auth_tokens,
                                  cfg->setup_auth_token_count, cfg->url.len,
                                  MOQ_SETUP_TOKENS_DEFAULT_ARENA))
            return MOQ_ERR_INVAL;
        if (out->backend != MOQ_TRANSPORT_BACKEND_PICOQUIC)
            return MOQ_ERR_UNSUPPORTED;
        out->setup_auth_tokens = cfg->setup_auth_tokens;
        out->setup_auth_token_count = cfg->setup_auth_token_count;
    }

    /* Application certificate trust: the pair is read field by field, each
     * only when struct_size covers it completely. A caller below the end of
     * verify_certificate has the older layout. A caller that covers the
     * callback but not its context, and asked for a callback, is refused:
     * requested trust policy is never silently replaced by default trust, and
     * the context is never read past the advertised size. Conflicts are
     * malformed configuration (INVAL), judged before this field's own
     * capability answer (UNSUPPORTED for a built backend that cannot honour
     * the callback, or a provider without it); an explicitly selected backend
     * that is not compiled in was already refused by backend selection above,
     * before any appended field is read. */
    out->verify_certificate = NULL;
    out->verify_certificate_ctx = NULL;
    if ((size_t)cfg->struct_size >=
        offsetof(moq_endpoint_cfg_t, verify_certificate) +
            sizeof(cfg->verify_certificate)) {
        bool ctx_covered = (size_t)cfg->struct_size >=
            offsetof(moq_endpoint_cfg_t, verify_certificate_ctx) +
                sizeof(cfg->verify_certificate_ctx);
        if (cfg->verify_certificate == NULL) {
            if (ctx_covered && cfg->verify_certificate_ctx != NULL)
                return MOQ_ERR_INVAL;
        } else {
            if (!ctx_covered) return MOQ_ERR_INVAL;
            if (cfg->ca_file.len != 0 || cfg->insecure_skip_verify)
                return MOQ_ERR_INVAL;
            if (out->backend != MOQ_TRANSPORT_BACKEND_PICOQUIC)
                return MOQ_ERR_UNSUPPORTED;
#ifndef MOQ_SERVICE_HAVE_PICOQUIC_CERT_CALLBACK
            return MOQ_ERR_UNSUPPORTED;
#else
            out->verify_certificate = cfg->verify_certificate;
            out->verify_certificate_ctx = cfg->verify_certificate_ctx;
#endif
        }
    }

    /* Preferred version: an appended tail field; 0 (absent or unset) is the
     * library default. */
    moq_version_t preferred = 0;
    if ((size_t)cfg->struct_size >=
        offsetof(moq_endpoint_cfg_t, preferred_version) +
            sizeof(cfg->preferred_version))
        preferred = cfg->preferred_version;
    moq_result_t vrc = resolve_versions(&cfg->versions, preferred, out);
    if (vrc < 0) return vrc;

    /* A profile that must be told its connection target cannot be offered
     * without one: AUTO drops it, an explicit offer is refused. */
    if (!out->target.target_present) {
        size_t kept = 0;
        for (size_t i = 0; i < out->version_count; i++) {
            if (moq_version_binding_requirements(out->versions[i]) != 0) {
                if (out->policy != MOQ_VERSION_POLICY_AUTO) return MOQ_ERR_UNSUPPORTED;
                continue;
            }
            out->versions[kept++] = out->versions[i];
        }
        out->version_count = kept;
        if (kept == 0) return MOQ_ERR_UNSUPPORTED;
    }

    /* A version the selected backend cannot carry is filtered exactly like a
     * version the URI cannot carry: an AUTO offer drops it -- a
     * preferred_version included, since a preference orders the eligible
     * versions and never makes one required -- while a LIST or EXACT offer
     * naming it is refused, never silently downgraded. */
    {
        size_t kept = 0;
        for (size_t i = 0; i < out->version_count; i++) {
            moq_version_t v = out->versions[i];
            if (!ep_backend_hosts_version(out->backend, v)) {
                if (out->policy != MOQ_VERSION_POLICY_AUTO)
                    return MOQ_ERR_UNSUPPORTED;
                continue;
            }
            out->versions[kept++] = v;
        }
        out->version_count = kept;
        if (kept == 0) return MOQ_ERR_UNSUPPORTED;
        if (out->policy == MOQ_VERSION_POLICY_AUTO)
            out->preferred_version = out->versions[0];
    }
    return MOQ_OK;
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
     * _STATE); -3 = stopped/terminal (MOQ_ERR_CLOSED); -4 = unsuccessful flush
     * (MOQ_ERR_INTERNAL), which does not itself make the endpoint terminal.
     * Evaluated only by the facade's own managed-context view, exposed
     * thread-safely. */
    int           (*drain_state)(void *facade);
    moq_result_t  (*stop)(void *facade);
    void          (*destroy)(void *facade);
    /* Classify the facade's terminal reason + native detail code. NULL is
     * treated as "generic" (NONE/PROTOCOL/TRANSPORT/CLEAN only). */
    void          (*terminal)(void *facade,
                              moq_endpoint_terminal_reason_t *reason,
                              uint64_t *detail);
} ep_backend_vtable_t;

/* drain_state return codes. */
#define EP_DRAIN_UNSUPPORTED  (-1)
#define EP_DRAIN_WRONG_THREAD (-2)
#define EP_DRAIN_CLOSED       (-3)
#define EP_DRAIN_FAILED       (-4)
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
    /* The connection target, owned: the URI identity a managed facade binds
     * the session with (uri holds the bytes; target points into them). */
    moq_endpoint_uri_owned_t uri;
    moq_connection_target_t  target;
    bool  insecure;
    uint64_t handshake_timeout_us;     /* 0 = backend default; picoquic only */
    moq_setup_tokens_t setup_tokens;   /* presented in SETUP; picoquic only */
    /* The application certificate callback pair, copied from the resolved
     * cfg; NULL callback = the ca_file / default-roots verifier. Valid for
     * the endpoint's lifetime; the context is caller-owned. */
    moq_tls_certificate_verify_fn verify_certificate;
    void                         *verify_certificate_ctx;

    /* The version offer as transport tokens, preference order. The ALPN
     * entries point at the static strings in moq_alpn.h (no ownership);
     * the WT offer is one owned comma-joined string. */
    const char *alpn_offer[MOQ_ENDPOINT_MAX_VERSIONS];
    size_t      alpn_offer_count;
    moq_version_t offer_versions[MOQ_ENDPOINT_MAX_VERSIONS];   /* the same offer */
    char       *wt_offer; size_t wt_offer_len;

    /* Selected backend: an opaque managed-facade handle + its vtable. Set by
     * the backend create-fn in connect(); NULL until then / after destroy. */
    void                      *facade;
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
    /* The session-wide GOAWAY's New Session URI, as a facade reported it
     * (under mu). */
    bool       goaway_received;
    bool       goaway_uri_lost;        /* the copy could not be allocated */
    uint8_t   *goaway_uri;
    size_t     goaway_uri_len;
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
static moq_version_t ep_facade_negotiated_version(const moq_endpoint_t *ep);
/*
 * One pump cycle, on the facade's managed execution context: cache the
 * negotiated version once the session is established, drain accepted post()
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
    /* The negotiated version is cached before any task of this cycle runs, so
     * a task that observes the established session -- and any thread it
     * signals -- reads it from moq_endpoint_negotiated_version(). */
    if (session && moq_session_state(session) == MOQ_SESS_ESTABLISHED) {
        pthread_mutex_lock(&ep->mu);
        if (ep->negotiated == 0)
            ep->negotiated = ep_facade_negotiated_version(ep);
        pthread_mutex_unlock(&ep->mu);
    }
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
    ep->pump_state = st;
    if (st == MOQ_ENDPOINT_CLOSED)
        atomic_store(&ep->closed, true);   /* mirror for ep->mu-free readers */
    if (st == MOQ_ENDPOINT_ESTABLISHED && ep->negotiated == 0)
        ep->negotiated = ep_facade_negotiated_version(ep);
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
/* The service verdict onto the adapter's, at the one site where the tiers
 * meet: only TRUST maps to TRUST; REJECT and any unrecognised value fail
 * closed. */
static moq_picoquic_cert_verdict_t ep_verify_certificate(
    void *ctx, moq_bytes_t server_name,
    const moq_bytes_t *certificates, size_t certificate_count)
{
    moq_endpoint_t *ep = (moq_endpoint_t *)ctx;
    if (ep == NULL || ep->verify_certificate == NULL)
        return MOQ_PICOQUIC_CERT_REJECT;
    moq_tls_certificate_verdict_t v = ep->verify_certificate(
        ep->verify_certificate_ctx, server_name, certificates, certificate_count);
    return v == MOQ_TLS_CERTIFICATE_TRUST ? MOQ_PICOQUIC_CERT_TRUST
                                          : MOQ_PICOQUIC_CERT_REJECT;
}

static int ep_configure_quic(picoquic_quic_t *quic, void *ctx)
{
    moq_endpoint_t *ep = (moq_endpoint_t *)ctx;
    if (ep->handshake_timeout_us > 0)
        picoquic_set_default_handshake_timeout(quic, ep->handshake_timeout_us);
    if (ep->insecure) return 0;
    /* A delegate selects the callback setter and never falls back to the
     * path verifier: falling back would silently verify against a policy the
     * caller did not ask for. Its conflicts with ca_file/insecure were
     * refused at resolve, before this endpoint existed. */
    if (ep->verify_certificate != NULL)
        return moq_picoquic_set_cert_verify_callback(quic, ep_verify_certificate, ep)
                   == MOQ_OK ? 0 : -1;
    return moq_picoquic_set_cert_verifier(quic, ep->ca_file);
}
#endif

/* -- Terminal-reason classification --------------------------------- *
 * Turn a facade's raw terminal signals into a transport-agnostic reason. The
 * generic form (mvfst/proxygen) knows only PROTOCOL vs generic transport; the
 * picoquic form additionally reads picoquic's captured local error to tell a
 * TLS/certificate failure apart from a plain transport failure. */
#if defined(MOQ_SERVICE_HAVE_MVFST_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_MSQUIC_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED)
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
#endif

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
/* Observe all local output under one client lane lock. A healthy live
 * connection can flush; pending-count mirrors cannot establish that fact. */
static int           msquic_vt_drain_state(void *f) {
    switch (moq_msquic_managed_poll_flush((moq_msquic_managed_t *)f)) {
    case MOQ_OK: return EP_DRAIN_DONE;
    case MOQ_ERR_WRONG_STATE: return EP_DRAIN_WRONG_THREAD;
    case MOQ_ERR_UNSUPPORTED: return EP_DRAIN_UNSUPPORTED;
    case MOQ_ERR_CLOSED: return EP_DRAIN_CLOSED;
    case MOQ_ERR_INTERNAL: return EP_DRAIN_FAILED;
    default: return EP_DRAIN_PENDING;
    }
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
static int wtquic_msquic_vt_drain_state(void *f)
{
    switch (moq_wtquic_msquic_managed_poll_flush(
                (const moq_wtquic_msquic_managed_t *)f)) {
    case MOQ_OK:              return EP_DRAIN_DONE;
    case MOQ_DONE:            return EP_DRAIN_PENDING;
    case MOQ_ERR_UNSUPPORTED: return EP_DRAIN_UNSUPPORTED;
    case MOQ_ERR_WRONG_STATE: return EP_DRAIN_WRONG_THREAD;
    case MOQ_ERR_CLOSED:      return EP_DRAIN_CLOSED;
    case MOQ_ERR_INTERNAL:    return EP_DRAIN_FAILED;
    default:                 return EP_DRAIN_FAILED;
    }
}
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

#if defined(MOQ_SERVICE_HAVE_PQ_THREADED) || \
    defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_MVFST_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_MSQUIC_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED) || \
    defined(MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED) || \
    defined(MOQ_MEDIA_SENDER_TESTING) || defined(MOQ_MEDIA_RECEIVER_TESTING)
static moq_version_t ep_facade_negotiated_version(const moq_endpoint_t *ep)
{
    if (ep->vt && ep->facade) return ep->vt->negotiated_version(ep->facade);
    return (moq_version_t)0;
}
#endif /* any facade (or the bare-endpoint test seam) */

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

static char *ep_strdup_wt_target(const moq_alloc_t *a,
                                 const moq_endpoint_resolved_t *r, size_t *len)
{
    /* The resolver has checked the sum, including the terminating NUL. */
    size_t n = r->wt_path.len + r->wt_query.len;
    char *p = (char *)a->alloc(n + 1, a->ctx);
    if (!p) return NULL;
    memcpy(p, r->wt_path.data, r->wt_path.len);
    if (r->wt_query.len)
        memcpy(p + r->wt_path.len, r->wt_query.data, r->wt_query.len);
    p[n] = '\0';
    *len = n + 1;
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
    moq_endpoint_uri_cleanup(&ep->alloc, &ep->uri);
    memset(&ep->target, 0, sizeof(ep->target));
    moq_setup_tokens_free(&ep->alloc, &ep->setup_tokens);
    if (ep->goaway_uri) ep->alloc.free(ep->goaway_uri, ep->goaway_uri_len, ep->alloc.ctx);
    ep->goaway_uri = NULL;
    ep->goaway_uri_len = 0;
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
    /* The URI identity the session is bound with when its profile needs a
     * transport binding; the facade knows the transport facts. */
    fc.target = ep->target.target_present ? &ep->target : NULL;
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
    fc.setup_auth_tokens = ep->setup_tokens.tokens;
    fc.setup_auth_token_count = ep->setup_tokens.count;
    if (cfg->struct_size >= offsetof(moq_endpoint_cfg_t, send_queue_cap_bytes) +
                            sizeof(cfg->send_queue_cap_bytes))
        fc.send_queue_cap_bytes = cfg->send_queue_cap_bytes;
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
    fc.target = ep->target.target_present ? &ep->target : NULL;
    fc.configure_quic = ep_configure_quic;
    fc.configure_quic_ctx = ep;
    fc.on_pump = ep_wt_pump;
    fc.on_pump_ctx = ep;
    /* Same idle-publisher policy as raw QUIC: emit QUIC keepalive before
     * common relay idle timers can reap a quiet WebTransport publisher. */
    fc.keep_alive_interval_ms = 15000;
    fc.setup_auth_tokens = ep->setup_tokens.tokens;
    fc.setup_auth_token_count = ep->setup_tokens.count;
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
    fc.target = ep->target.target_present ? &ep->target : NULL;
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
    /* The URI identity the session is bound with when its profile needs a
     * transport binding; the facade knows the transport facts. */
    fc.target = ep->target.target_present ? &ep->target : NULL;
    fc.on_lane_pump = ep_msquic_pump;
    fc.on_lane_pump_user = ep;
    /* Same idle-publisher policy as the picoquic raw-QUIC backend: emit QUIC
     * keepalive before common relay idle timers can reap a quiet publisher. */
    fc.keep_alive_interval_ms = 15000;
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
    fc.target = ep->target.target_present ? &ep->target : NULL;
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
    /* The URI identity the session is bound with when its profile needs a
     * transport binding; the facade knows the transport facts. */
    fc.target = ep->target.target_present ? &ep->target : NULL;
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
    /* The URI identity the session is bound with when its profile needs a
     * transport binding; the facade knows the transport facts. */
    fc.target = ep->target.target_present ? &ep->target : NULL;
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

/* The setup-token pair is appended after handshake_timeout_us, in this order:
 * an older caller's buffer ends before it, so the size gate reads none. */
_Static_assert(offsetof(moq_endpoint_cfg_t, setup_auth_tokens) >=
                   offsetof(moq_endpoint_cfg_t, handshake_timeout_us) +
                       sizeof(((moq_endpoint_cfg_t *)0)->handshake_timeout_us),
               "setup_auth_tokens must be appended after handshake_timeout_us");
_Static_assert(offsetof(moq_endpoint_cfg_t, setup_auth_token_count) >
                   offsetof(moq_endpoint_cfg_t, setup_auth_tokens),
               "setup_auth_token_count must follow setup_auth_tokens");
_Static_assert(offsetof(moq_endpoint_cfg_t, verify_certificate) >=
                   offsetof(moq_endpoint_cfg_t, setup_auth_token_count) +
                       sizeof(((moq_endpoint_cfg_t *)0)->setup_auth_token_count),
               "verify_certificate must be appended after setup_auth_token_count");
_Static_assert(offsetof(moq_endpoint_cfg_t, verify_certificate_ctx) >=
                   offsetof(moq_endpoint_cfg_t, verify_certificate) +
                       sizeof(((moq_endpoint_cfg_t *)0)->verify_certificate),
               "verify_certificate_ctx must follow verify_certificate");

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
     * Tail fields require moq_endpoint_cfg_init_sized(). */
    moq_endpoint_cfg_init_sized(cfg, MOQ_ENDPOINT_CFG_V0_SIZE);
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
    !defined(MOQ_SERVICE_HAVE_MSQUIC_MANAGED) && \
    !defined(MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED) && \
    !defined(MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED) && \
    !defined(MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED)
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
    ep->verify_certificate = r.verify_certificate;
    ep->verify_certificate_ctx = r.verify_certificate_ctx;
    atomic_init(&ep->interrupted, false);
    atomic_init(&ep->closed, false);
    pthread_mutex_init(&ep->mu, NULL);
    ep->pump_state = MOQ_ENDPOINT_CONNECTING;

    /* The connection target outlives the caller's cfg: copy its bytes once
     * and point the target at the copy. A STANDARD WebTransport path is the
     * HTTP projection of the raw target ("/" for an empty path). */
    bool oom = false;
    bool legacy_wt = r.protocol == MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT &&
                     r.uri_mode != MOQ_ENDPOINT_URI_STANDARD;
    if (legacy_wt) {
        ep->path = ep_strdup_wt_target(alloc, &r, &ep->path_len);
        if (!ep->path) {
            pthread_mutex_destroy(&ep->mu);
            alloc->free(ep, sizeof(*ep), alloc->ctx);
            return MOQ_ERR_NOMEM;
        }
    }
    if (r.target.target_present) {
        moq_endpoint_uri_t identity;
        memset(&identity, 0, sizeof(identity));
        identity.authority = r.target.authority;
        /* LEGACY WebTransport binds the resource its CONNECT dereferences
         * (see the resolver). */
        identity.path_query = legacy_wt
            ? (moq_bytes_t){ (const uint8_t *)ep->path, ep->path_len - 1 }
            : r.target.path_query;
        bool wt = r.protocol == MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT &&
                  r.uri_mode == MOQ_ENDPOINT_URI_STANDARD;
        moq_result_t trc = moq_endpoint_uri_clone(alloc, &identity, wt, &ep->uri);
        if (trc < 0) {
            if (ep->path) alloc->free(ep->path, ep->path_len, alloc->ctx);
            pthread_mutex_destroy(&ep->mu);
            alloc->free(ep, sizeof(*ep), alloc->ctx);
            return trc;
        }
        ep->target = r.target;
        ep->target.authority = ep->uri.authority;
        ep->target.path_query = ep->uri.path_query;
    }

    ep->host = ep_strdup_bytes(alloc, r.url.host, &ep->host_len);
    ep->sni  = ep_strdup_bytes(alloc, r.sni, &ep->sni_len);
    oom = oom || !ep->host || !ep->sni;
    if (!oom && r.protocol == MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT) {
        if (r.uri_mode == MOQ_ENDPOINT_URI_STANDARD && ep->uri.wt_path) {
            ep->path = ep_strdup_bytes(alloc,
                (moq_bytes_t){ (const uint8_t *)ep->uri.wt_path,
                               strlen(ep->uri.wt_path) },
                &ep->path_len);
        }
        oom = oom || !ep->path;
    }
    if (!oom && cfg->ca_file.len > 0) {
        ep->ca_file = ep_strdup_bytes(alloc, cfg->ca_file, &ep->ca_file_len);
        oom = oom || !ep->ca_file;
    }
    if (!oom && r.setup_auth_token_count > 0) {
        moq_result_t src = moq_setup_tokens_copy(alloc, r.setup_auth_tokens,
            r.setup_auth_token_count, &ep->setup_tokens);
        oom = src != MOQ_OK;
    }
    /* Convert the resolved version offer into transport tokens, preference
     * order preserved. Every resolved version is build-supported, so every
     * token maps. */
    if (!oom) {
        for (size_t i = 0; i < r.version_count; i++) {
            const char *tok = moq_alpn_for_version(r.versions[i]);
            if (!tok) { oom = true; break; }   /* unreachable: validated */
            ep->alpn_offer[i] = tok;
            ep->offer_versions[i] = r.versions[i];
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
     * managed context drives the flush, so calling this from a hook would
     * deadlock the very progress it waits on. */
    int ds = ep_facade_drain_state(ep);
    if (ds == EP_DRAIN_UNSUPPORTED)  return MOQ_ERR_UNSUPPORTED;
    if (ds == EP_DRAIN_WRONG_THREAD) return MOQ_ERR_WRONG_STATE;
    if (ds == EP_DRAIN_CLOSED) return MOQ_ERR_CLOSED;
    if (ds == EP_DRAIN_FAILED) return MOQ_ERR_INTERNAL;

    uint64_t start = ep_mono_now_us();
    for (;;) {
        if (atomic_load(&ep->interrupted)) return MOQ_ERR_INTERRUPTED;

        /* Terminal close during the drain is observable state, not success: the
         * caller asked to flush BEFORE stopping, so a close mid-drain means the
         * tail may have been truncated -- report CLOSED even if a (possibly
         * stale) backlog snapshot now reads empty. */
        if (moq_endpoint_is_closed(ep)) return MOQ_ERR_CLOSED;

        ds = ep_facade_drain_state(ep);
        if (ds == EP_DRAIN_UNSUPPORTED) return MOQ_ERR_UNSUPPORTED;
        if (ds == EP_DRAIN_WRONG_THREAD) return MOQ_ERR_WRONG_STATE;
        if (ds == EP_DRAIN_CLOSED) return MOQ_ERR_CLOSED;
        if (ds == EP_DRAIN_FAILED) return MOQ_ERR_INTERNAL;
        if (ds == EP_DRAIN_DONE) return MOQ_OK;

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

size_t moq_endpoint_offered_versions_internal(const moq_endpoint_t *ep,
                                              moq_version_t *out, size_t cap)
{
    if (!ep) return 0;
    for (size_t i = 0; i < ep->alpn_offer_count && i < cap; i++)
        out[i] = ep->offer_versions[i];
    return ep->alpn_offer_count;
}

void moq_endpoint_note_goaway_locked(moq_endpoint_t *ep, moq_bytes_t new_session_uri)
{
    if (!ep || ep->goaway_received) return;   /* one session-wide GOAWAY */
    ep->goaway_received = true;
    if (new_session_uri.len == 0) return;
    uint8_t *copy = (uint8_t *)ep->alloc.alloc(new_session_uri.len, ep->alloc.ctx);
    if (!copy) {
        ep->goaway_uri_lost = true;
        return;
    }
    memcpy(copy, new_session_uri.data, new_session_uri.len);
    ep->goaway_uri = copy;
    ep->goaway_uri_len = new_session_uri.len;
}

moq_result_t moq_endpoint_get_goaway_uri(moq_endpoint_t *ep, uint8_t *buf,
                                         size_t cap, size_t *uri_len)
{
    if (!ep || !uri_len || (!buf && cap > 0)) return MOQ_ERR_INVAL;
    moq_result_t rc = MOQ_OK;
    pthread_mutex_lock(&ep->mu);
    if (!ep->goaway_received) {
        rc = MOQ_DONE;
    } else if (ep->goaway_uri_lost) {
        rc = MOQ_ERR_NOMEM;
    } else {
        *uri_len = ep->goaway_uri_len;
        if (cap < ep->goaway_uri_len) rc = MOQ_ERR_BUFFER;
        else if (ep->goaway_uri_len) memcpy(buf, ep->goaway_uri, ep->goaway_uri_len);
    }
    pthread_mutex_unlock(&ep->mu);
    return rc;
}

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

moq_result_t moq_endpoint_get_terminal(const moq_endpoint_t *ep,
                                       moq_endpoint_terminal_t *out,
                                       size_t out_size)
{
    if (!ep || !out || out_size < MOQ_ENDPOINT_TERMINAL_V0_SIZE)
        return MOQ_ERR_INVAL;
    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);
    memset(out, 0, n);

    moq_endpoint_terminal_t t;
    memset(&t, 0, sizeof(t));
    t.reason = MOQ_ENDPOINT_TERMINAL_NONE;
    t.detail_code = 0;
    if (ep->vt && ep->facade && ep->vt->terminal)
        ep->vt->terminal(ep->facade, &t.reason, &t.detail_code);

    t.struct_size = (uint32_t)n;
    memcpy(out, &t, n);
    return MOQ_OK;
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
/* A facade stand-in that reports only a negotiated version: never terminal,
 * nothing to wake. Lets a bare endpoint's pump cycle cache a version from a
 * facade the way a managed transport does. */
static moq_version_t test_facade_version;
static moq_version_t test_facade_negotiated(void *facade) { (void)facade; return test_facade_version; }
static void test_facade_wake(void *facade) { (void)facade; }
static bool test_facade_false(void *facade) { (void)facade; return false; }
static const ep_backend_vtable_t test_facade_vt = {
    .wake = test_facade_wake,
    .negotiated_version = test_facade_negotiated,
    .is_fatal = test_facade_false,
    .clean_closed = test_facade_false,
};
void moq_endpoint_test_set_facade_version(moq_endpoint_t *ep, moq_version_t v);
void moq_endpoint_test_set_facade_version(moq_endpoint_t *ep, moq_version_t v)
{
    test_facade_version = v;
    ep->vt = &test_facade_vt;
    ep->facade = ep;
}

/* The version offer a bare endpoint reports (no ALPN tokens). */
void moq_endpoint_test_set_offer(moq_endpoint_t *ep, const moq_version_t *v, size_t n)
{
    if (n > MOQ_ENDPOINT_MAX_VERSIONS) n = MOQ_ENDPOINT_MAX_VERSIONS;
    for (size_t i = 0; i < n; i++) ep->offer_versions[i] = v[i];
    ep->alpn_offer_count = n;
}

#endif /* MOQ_MEDIA_SENDER_TESTING || MOQ_MEDIA_RECEIVER_TESTING */
