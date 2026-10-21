#ifndef MOQ_ENDPOINT_H
#define MOQ_ENDPOINT_H

/*
 * moq_endpoint_t — the service-tier connection object: URL → managed
 * connection + session. Owns threading, version negotiation (ALPN / WT
 * protocol tokens), TLS/SNI defaults, terminal-state classification, the
 * sticky interrupt latch, pump multiplexing for attached media services, and
 * the post() executor that makes the full moq_session_* surface reachable
 * without exposing the pump.
 *
 * Design: MEDIA_SERVICE_DESIGN.md §5. This tier is severable: lower tiers
 * build and work without it (MOQ_BUILD_SERVICE), the core stays sans-I/O,
 * and all threading lives here.
 *
 * Current status: lifecycle and version negotiation are live over the
 * managed picoquic facades (RAW_QUIC via moq_pq_threaded with ALPN
 * offer-list + readback, WEBTRANSPORT via moq_pico_wt_managed with WT
 * protocol tokens). picoquic is the DEFAULT RAW_QUIC backend. When compiled
 * in and selected explicitly via cfg.backend, RAW_QUIC is ALSO served by
 * mvfst (moq_mvfst_managed, exact-version) and by MsQuic (moq_msquic_managed,
 * exact-version, the shared lane pump API — the path intended for future
 * sharded relay work), and WEBTRANSPORT by proxygen (moq_proxygen_wt_managed,
 * full negotiation); AUTO keeps choosing picoquic/pico_wt (msquic/mvfst are
 * opt-in only, never selected ahead of picoquic). Offers go out in PREFERENCE ORDER (AUTO =
 * every draft this build supports, newest first: draft-22, draft-18, then
 * draft-16, with a named preferred version moved to the front); the negotiated version
 * configures the session before any SETUP byte is parsed, and an un-negotiable
 * connection fails terminally rather than silently downgrading.
 *
 * Media receiver/sender attach APIs are live (see moq/media_receiver.h and
 * moq/media_sender.h). v0 supports at most one receiver attachment and one
 * sender attachment per endpoint; while attached, moq_endpoint_stop() is gated
 * by the endpoint attachment count.
 */

#include <moq/types.h>
#include <moq/session.h>
#include <moq/connection_target.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moq_endpoint moq_endpoint_t;

/* -- Transport configuration (§5.1) --------------------------------- *
 * Protocol and backend are orthogonal axes. An explicit non-AUTO protocol
 * WINS over the URL scheme (the field is the override knob, never an error);
 * AUTO derives from the scheme: moqt:// or moq:// => RAW_QUIC, https:// =>
 * WEBTRANSPORT.
 *
 * Error precedence at connect()/resolve, the same in both URI modes (see
 * uri_mode below): the URL is read first. Malformed syntax, or a scheme the
 * endpoint reads in NO mode (anything but moqt, moq and https), is
 * MOQ_ERR_INVAL -- even when an explicit protocol would have overridden the
 * scheme; the override knob re-interprets a known URL, it does not admit
 * arbitrary ones (host/port/path defaults all derive from the parse). A
 * well-formed URL the selected mode does not support is MOQ_ERR_UNSUPPORTED.
 * Only then are protocol and backend resolved: a backend that cannot speak
 * the resolved transport, or is not compiled in, is MOQ_ERR_UNSUPPORTED,
 * while an out-of-range field value is MOQ_ERR_INVAL before any backend
 * capability check. */

typedef enum moq_transport_protocol {
    MOQ_TRANSPORT_PROTOCOL_AUTO = 0,    /* derive from the URL scheme */
    MOQ_TRANSPORT_PROTOCOL_RAW_QUIC,    /* native MoQ-over-QUIC (moqt-NN ALPN) */
    MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT /* H3 WebTransport */
} moq_transport_protocol_t;

typedef enum moq_transport_backend {
    MOQ_TRANSPORT_BACKEND_AUTO = 0,     /* the STABLE picoquic default: always
                                           resolves to the picoquic family
                                           (picoquic for RAW_QUIC, pico_wt for
                                           WEBTRANSPORT); when that facade is
                                           not compiled in, connect fails with
                                           MOQ_ERR_UNSUPPORTED -- AUTO never
                                           falls through to an opt-in backend */
    MOQ_TRANSPORT_BACKEND_PICOQUIC,     /* RAW_QUIC + WEBTRANSPORT */
    MOQ_TRANSPORT_BACKEND_MVFST,        /* RAW_QUIC only, when compiled in;
                                           else MOQ_ERR_UNSUPPORTED */
    MOQ_TRANSPORT_BACKEND_PROXYGEN,     /* WEBTRANSPORT only, when compiled in;
                                           else MOQ_ERR_UNSUPPORTED */
    MOQ_TRANSPORT_BACKEND_MSQUIC,      /* RAW_QUIC only, when compiled in
                                           (moq_msquic_managed, lane pump API);
                                           else MOQ_ERR_UNSUPPORTED. TLS caveat:
                                           requires sni == URL host and no
                                           custom ca_file (system roots or
                                           insecure_skip_verify only) -- an
                                           unsupported TLS config is rejected
                                           with MOQ_ERR_UNSUPPORTED at connect */
    MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK, /* WEBTRANSPORT only, when compiled in
                                           Current receive-admission policy:
                                           connect returns MOQ_ERR_UNSUPPORTED
                                           synchronously, even when compiled;
                                           no fallback or Network startup.
                                           Remaining notes describe retained
                                           backend mechanisms, not support.
                                           (moq_wtquic_network_managed, lane pump
                                           API; Apple-only, client-only; no
                                           network thread -- wtquic's backend
                                           serial queue is the endpoint's
                                           execution context). Real
                                           multi-version negotiation, so
                                           AUTO/LIST offers work. Same TLS
                                           caveat as MSQUIC: requires sni ==
                                           URL host and no custom ca_file
                                           (system trust or
                                           insecure_skip_verify only) --
                                           rejected with MOQ_ERR_UNSUPPORTED
                                           at connect. Else
                                           MOQ_ERR_UNSUPPORTED. */
    MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC /* WEBTRANSPORT only, when compiled in
                                           (moq_wtquic_msquic_managed, lane pump
                                           API; cross-platform wherever wtquic's
                                           MsQuic component builds; client-only,
                                           owns its coordinator + doorbell
                                           threads). Full multi-version
                                           WT-Protocol negotiation, so AUTO/LIST
                                           offers work. Explicit opt-in only:
                                           AUTO never resolves here. RAW_QUIC is
                                           rejected with MOQ_ERR_UNSUPPORTED.
                                           Same TLS caveat as MSQUIC/
                                           WTQUIC_NETWORK: requires sni == URL
                                           host and no custom ca_file (system
                                           trust or insecure_skip_verify only)
                                           -- rejected with MOQ_ERR_UNSUPPORTED
                                           at connect. Distinct from
                                           BACKEND_MSQUIC (direct raw QUIC over
                                           MsQuic) and BACKEND_WTQUIC_NETWORK
                                           (WebTransport over Network.framework).
                                           The reserved CLI spelling is
                                           "wtquic-msquic" (never bare
                                           "wtquic"). Else MOQ_ERR_UNSUPPORTED. */
} moq_transport_backend_t;

/* -- Version negotiation (§5.2) -------------------------------------- *
 * The endpoint owns version offer and readback for both protocols; apps
 * never touch ALPN or WT protocol tokens. AUTO offers every supported
 * version, newest first. Offering a version whose profile is not compiled in
 * fails with MOQ_ERR_UNSUPPORTED at create -- never silently downgraded. The
 * offer also follows what the selected backend can carry: a picoquic built
 * without its stream-terminal contract cannot host draft-22, so on that
 * backend AUTO leaves draft-22 out (a preferred_version of draft-22 only
 * orders the versions offered, as when the URI cannot carry it), and a LIST
 * or EXACT offer naming it makes moq_endpoint_connect fail with
 * MOQ_ERR_UNSUPPORTED before any connection is started. */

typedef enum moq_version_policy {
    MOQ_VERSION_POLICY_AUTO = 0,   /* offer the legacy defaults; see preferred_version */
    MOQ_VERSION_POLICY_LIST,       /* offer exactly versions[], peer/TLS picks */
    MOQ_VERSION_POLICY_EXACT       /* offer exactly one; mismatch = fatal connect */
} moq_version_policy_t;

typedef struct moq_version_offer {
    uint32_t struct_size;           /* 0 (zero-init) == AUTO */
    moq_version_policy_t policy;
    const moq_version_t *versions;  /* used by LIST/EXACT */
    size_t version_count;
} moq_version_offer_t;

/* -- Endpoint configuration (§5.1) ----------------------------------- */

/* WebTransport-over-HTTP/3 wire profile. Only a backend that can actually select
 * a dialect (wtquic-msquic today) accepts an EXPLICIT profile; a fixed-dialect
 * WebTransport backend (picoquic) or a raw-QUIC backend (msquic/mvfst) rejects
 * an explicit selection with MOQ_ERR_UNSUPPORTED rather than silently ignoring
 * it. The two real profiles are mutually exclusive and never auto-negotiated.
 *
 * BACKEND_DEFAULT is the zero value, so a zero-initialized cfg selects nothing
 * and works with every backend; the selectable backend maps it to its own
 * default (CURRENT). */
typedef enum moq_wt_profile {
    /* No explicit selection: use the backend's own default dialect. The only
     * value accepted by backends that do not expose a dialect choice. */
    MOQ_WT_PROFILE_BACKEND_DEFAULT = 0,
    /* The current WebTransport-H3 draft (":protocol = webtransport-h3"). */
    MOQ_WT_PROFILE_CURRENT = 1,
    /* Drafts-13/14 compatibility (":protocol = webtransport" + the
     * drafts-13/14 max-sessions signal), what proxygen/moxygen/moqx and the
     * picoquic h3zero family speak. */
    MOQ_WT_PROFILE_D13_14_COMPAT = 2
} moq_wt_profile_t;

/* Application-owned certificate trust decision. REJECT is zero so a zero,
 * default or unrecognised result fails closed. */
typedef enum moq_tls_certificate_verdict {
    MOQ_TLS_CERTIFICATE_REJECT = 0,
    MOQ_TLS_CERTIFICATE_TRUST  = 1
} moq_tls_certificate_verdict_t;

/* Synchronous client certificate trust callback.
 *
 * WHAT THE LIBRARY HAS ALREADY DONE when this runs: the peer's leaf has been
 * matched against `server_name` under the transport's own reference-identity
 * rules (a DNS name against the leaf's dNSName SANs, or its subject CN when it
 * has none, with no partial-label wildcards; an IP literal against iPAddress
 * SANs), and a chain whose leaf does not name `server_name` is refused before
 * the application is consulted. Neither an absent nor an empty identity is
 * ever presented. After a TRUST verdict the TLS stack still extracts the leaf
 * public key and verifies the TLS CertificateVerify signature, so a verdict
 * cannot bypass the peer's proof of key possession.
 *
 * WHAT THE CALLBACK OWNS: the whole chain policy. In delegated mode the
 * library verifies against an EMPTY local trust store and treats its own
 * chain-policy failures as the delegate's to answer, so nothing about issuer
 * trust, validity period, purpose, name/path constraints, revocation or
 * application policy (pinning, per-domain rules) is decided here. Return
 * TRUST only after the application's own evaluation of `certificates` for
 * `server_name`; any other value is REJECT. Stapled OCSP responses, SCTs and
 * CT inputs are not carried; a policy that needs them must reject.
 *
 * `server_name` is the exact name being authenticated (the resolved sni: an
 * explicit cfg sni, else the URL host). `certificates` is the leaf-first DER
 * chain exactly as the wire carried it, at most 16 entries. Both are BORROWED
 * for this invocation only.
 *
 * It runs synchronously on the transport's network thread while the handshake
 * is in progress, must not call any moq_endpoint_*, transport-adapter or
 * picoquic API, and must not retain a per-call JNIEnv or local references (a
 * JNI bridge keeps a global reference and attaches the thread itself). It
 * should return promptly; the library cannot bound arbitrary application code,
 * and a callback that has not returned can delay a later stop() join until it
 * does. */
typedef moq_tls_certificate_verdict_t (*moq_tls_certificate_verify_fn)(
    void *ctx,
    moq_bytes_t server_name,
    const moq_bytes_t *certificates,
    size_t certificate_count);

typedef struct moq_endpoint_cfg {
    uint32_t struct_size;             /* CFG_HAS pattern, as everywhere */
    moq_bytes_t url;                  /* moqt://host:port[/path] or https://... */
    moq_transport_protocol_t protocol;
    moq_transport_backend_t  backend;
    moq_version_offer_t      versions;  /* zero-init = AUTO */
    /* TLS */
    moq_bytes_t sni;                  /* empty = URL host (mandatory default);
                                         honored on the picoquic/proxygen
                                         transports -- the TLS server name and
                                         the name the cert verifier checks,
                                         carried separately from the connection
                                         host. The MsQuic and wtquic-Network
                                         backends have no separate SNI (host
                                         is both); an override != URL host is
                                         rejected with MOQ_ERR_UNSUPPORTED */
    moq_bytes_t ca_file;              /* empty = backend default roots when
                                         available. picoquic mbedTLS-only
                                         builds have no implicit store here and
                                         require a PEM ca_file. The MsQuic and
                                         wtquic-Network backends have no CA-file
                                         field: a custom ca_file with
                                         verification on is rejected with
                                         MOQ_ERR_UNSUPPORTED (use system trust
                                         or insecure_skip_verify) */
    bool        insecure_skip_verify; /* explicit, default false */
    /* WebTransport */
    /* Complete WebTransport CONNECT target override. Empty: the URL path
     * (or "/moq" if absent) followed by the URL query, including "?" when
     * present. URL fragments are never part of the connection target. */
    moq_bytes_t wt_path;
    moq_perspective_t perspective;    /* client-only in v0; server reserved:
                                         MOQ_ERR_UNSUPPORTED */
    /* The allocator MUST be thread-safe (NULL = default libc allocator,
     * which is). The service tier allocates on one thread and frees on
     * another by design -- a stated departure from the sans-I/O core's
     * single-thread-confined allocator convention. A non-thread-safe
     * arena/pool allocator is unsupported at this tier. */
    const moq_alloc_t *alloc;
    /* --- v0 layout floor is HERE (through alloc). Everything below is an
     *     appended, struct_size-gated tail field; the floor never moves. --- */
    /* appended (enabled by struct_size): the WebTransport wire profile
     * (moq_wt_profile_t). MOQ_WT_PROFILE_BACKEND_DEFAULT (0) selects nothing and
     * is accepted by every backend. An EXPLICIT profile (CURRENT / D13_14_COMPAT)
     * is honored only by the wtquic-msquic backend; on any other backend it is a
     * connect-time MOQ_ERR_UNSUPPORTED, not a silent no-op. Because it is past
     * the v0 floor, connect() reads it ONLY when struct_size covers it fully --
     * set it via moq_endpoint_cfg_init_sized(&cfg, sizeof cfg), not the
     * pointer-only moq_endpoint_cfg_init(). */
    uint32_t wt_profile;
    /* appended (enabled by struct_size): upper bound in microseconds on the
     * TRANSPORT handshake -- how long a peer that never completes the QUIC/TLS
     * handshake holds the endpoint in CONNECTING before it goes terminal. It
     * does NOT bound a peer that completes the handshake and then stalls the
     * WebTransport CONNECT or the MoQ SETUP. 0 leaves the backend's own value
     * (30 s on picoquic). Honored by the picoquic backend on both protocols. No
     * other service backend can apply THIS setting -- a transport-handshake-only
     * bound -- so a NON-ZERO value there is a connect-time MOQ_ERR_UNSUPPORTED,
     * not a silent no-op. That is a statement about this knob, not about
     * timeouts in general: other backends have their own timeout mechanisms
     * with different scope (the MsQuic managed facade, for one, carries a
     * combined idle/handshake setting), and this field does not drive them.
     *
     * ACCEPTED RANGE. The value is a real duration, not a switch: the backend
     * applies it as an absolute deadline (connection start + this bound), so an
     * unbounded value would wrap that sum and produce the OPPOSITE of a long
     * wait. The accepted values are therefore exactly:
     *   0                                           leave the backend default
     *   1 .. MOQ_ENDPOINT_HANDSHAKE_TIMEOUT_MAX_US  a microsecond duration
     *   anything larger                             MOQ_ERR_INVAL
     * The MOQ_ERR_INVAL is raised during resolution, BEFORE the backend
     * capability check, so an out-of-range value reads as INVAL on every
     * backend rather than UNSUPPORTED on some. There is NO "infinite" sentinel
     * and nothing is silently clamped -- use 0 to leave the backend's own
     * value. Because it is past
     * the v0 floor, connect() reads it ONLY when struct_size covers it fully --
     * set it via moq_endpoint_cfg_init_sized(&cfg, sizeof cfg). */
    uint64_t handshake_timeout_us;
    /* appended (enabled by struct_size): authorization tokens the session
     * presents in its SETUP (moq_session_cfg_t.setup_auth_tokens: sent by
     * value, at most 16, each value at most 65535 bytes), copied at connect.
     * The SETUP carrying them must fit the session's default 4096-byte send
     * arena (the send-arena rule on moq_session_cfg_t.setup_auth_tokens:
     * each token takes its value plus up to 32 bytes, beside the URL and the
     * other options), else MOQ_ERR_INVAL at connect, rather than a connection
     * that fails when it starts. Only the picoquic backend (raw QUIC and
     * WebTransport) carries them; any other backend refuses a non-empty list
     * with MOQ_ERR_UNSUPPORTED. Because it is past the v0 floor, connect()
     * reads it ONLY when struct_size covers setup_auth_token_count -- set it
     * via moq_endpoint_cfg_init_sized(&cfg, sizeof cfg). */
    const moq_auth_token_t *setup_auth_tokens;
    size_t setup_auth_token_count;
    /* appended (enabled by struct_size): application-owned certificate trust
     * for picoquic clients on the OpenSSL picotls provider, on both the raw
     * QUIC (moq_pq_threaded) and WebTransport (pico WT managed) routes.
     *
     * NULL = today's verification (chain + server name against ca_file or the
     * backend default roots). Non-NULL delegates the chain-policy decision to
     * the application as documented on moq_tls_certificate_verify_fn, while
     * the library keeps the reference-identity check and TLS CertificateVerify.
     *
     * READING RULE: a caller whose struct_size does not reach the END of
     * verify_certificate keeps the older behaviour, both fields untouched. Once
     * verify_certificate is fully covered, a non-NULL callback whose paired
     * verify_certificate_ctx is not also fully covered is MOQ_ERR_INVAL --
     * requested trust policy is never silently replaced by default trust, and
     * the context is never read past the advertised size. A fully covered pair
     * with a NULL ctx is valid (a stateless callback needs none); a non-NULL
     * ctx with a NULL callback is MOQ_ERR_INVAL.
     *
     * PRECEDENCE: a callback together with a non-empty ca_file or with
     * insecure_skip_verify is MOQ_ERR_INVAL (two trust authorities), judged
     * before this field's own capability answer; then a built backend other
     * than picoquic, or a picoquic build whose TLS provider cannot honour the
     * callback (mbedTLS), is MOQ_ERR_UNSUPPORTED. An explicitly selected
     * backend that is not compiled in is refused by backend selection before
     * any appended field is read, as for every appended field. All of these
     * are decided before the endpoint exists, so a refused configuration has
     * no effect and never installs default trust in place of the requested
     * policy.
     *
     * LIFETIME: the endpoint copies only the two pointers. ctx stays
     * caller-owned, must be FULLY INITIALISED BEFORE connect() is called, and
     * must remain valid through moq_endpoint_destroy(). The managed transport
     * starts its network thread inside connect(), before the endpoint handle
     * is published, so the callback may run concurrently with, and before the
     * return of, connect() itself; nothing sequences it after the caller.
     *
     * What IS sequenced: a configuration or setup refusal returns an error
     * from connect() with no endpoint and nothing retained (resolve refuses
     * before any allocation; a facade or verifier-install failure releases
     * the partially built endpoint before returning), whereas a REJECT
     * verdict is an asynchronous certificate failure, observed as a terminal
     * endpoint with reason MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE. Other
     * handshake failures retain the endpoint's TLS or transport classification.
     * Here asynchronous means the failure is not reported through connect()'s
     * return value, not that it happens after connect() returns.
     *
     * The canonical retirement order is: destroy every attached media
     * service, then moq_endpoint_stop() == MOQ_OK, then
     * moq_endpoint_destroy(). A SUCCESSFUL stop joins the network thread, so
     * a callback in flight has returned when stop returns -- and a callback
     * that does not return delays that join until it does. A stop refused
     * with MOQ_ERR_WRONG_STATE (attachments still present, or called from
     * the managed context) is NOT a retirement receipt: the callback may
     * still run and ctx must stay valid. destroy() requires a stopped
     * endpoint and is not an implicit stop. */
    moq_tls_certificate_verify_fn verify_certificate;
    void                         *verify_certificate_ctx;
    /* appended (enabled by struct_size): how the URL is read.
     * MOQ_ENDPOINT_URI_LEGACY (0, the default, also when the field is absent)
     * selects the original parser: exact lowercase "moqt" / "moq" / "https"
     * schemes, port 4433 for moqt and moq, the wt_path override and the
     * "/moq" WebTransport default.
     * MOQ_ENDPOINT_URI_STANDARD (1) reads the URL as the MOQT URI scheme
     * defines it: "moqt" and
     * "https" case-insensitively, port 443 when omitted, the encoded
     * authority and path/query preserved exactly, a WebTransport path of
     * "/" for an empty path (query retained), and no wt_path override (a
     * non-empty wt_path is MOQ_ERR_INVAL).
     * STANDARD's managed-client
     * subset is ASCII unencoded host names, IPv4 and bracketed IPv6, and
     * explicit ports 1..65535; well-formed input the subset does not admit
     * (the legacy "moq" scheme, userinfo, IPvFuture, encoded hosts, empty or
     * out-of-range ports, typed fragments) is MOQ_ERR_UNSUPPORTED and
     * malformed syntax is MOQ_ERR_INVAL; a scheme other than moqt, moq and
     * https is MOQ_ERR_INVAL in both modes (the error precedence above). No
     * fragment type is implemented and fragments are never transmitted. An
     * unknown mode is MOQ_ERR_INVAL.
     *
     * In both modes the endpoint derives the connection target -- the URL's
     * authority as spelled and its path-abempty with "?" query -- that a
     * wire profile carried natively over QUIC sends in its SETUP. A target
     * exists whenever those two components follow RFC 3986 syntax with a
     * nonempty host; when they do not, a version whose profile requires a
     * transport binding is dropped from an AUTO offer and refused with
     * MOQ_ERR_UNSUPPORTED in a LIST or EXACT offer. Authority plus path/query
     * is limited to 65535 bytes. */
    uint32_t uri_mode;
    /* appended (enabled by struct_size): the version placed FIRST in an AUTO
     * offer, 0 for the library default. Applies to AUTO only -- a non-zero
     * value with a LIST or EXACT policy, whose order is already explicit, is
     * MOQ_ERR_INVAL -- and must be a version this build offers, else
     * MOQ_ERR_UNSUPPORTED. The rest of the offer follows it newest first
     * (draft-22, draft-18, draft-16, each only when supported), without
     * duplicates. */
    moq_version_t preferred_version;
    /* appended (enabled by struct_size): a server endpoint's URI policy, in
     * the shape moq_connection_target_t carries it (see
     * <moq/connection_target.h>). A client endpoint supplies neither; a
     * client cfg carrying either is MOQ_ERR_INVAL. The server perspective is
     * reserved in v0, so today these fields can only be left zero. */
    moq_uri_accept_fn accept_uri;
    void *accept_uri_ctx;
} moq_endpoint_cfg_t;

#define MOQ_ENDPOINT_URI_LEGACY   UINT32_C(0)
#define MOQ_ENDPOINT_URI_STANDARD UINT32_C(1)

/* Largest accepted moq_endpoint_cfg_t.handshake_timeout_us (see its doc above).
 * A practical no-limit ceiling -- roughly 292,000 years -- chosen so a backend
 * can add the duration to its absolute microsecond clock without the unsigned
 * sum wrapping. A larger value is MOQ_ERR_INVAL, never a clamp. */
#define MOQ_ENDPOINT_HANDSHAKE_TIMEOUT_MAX_US ((uint64_t)INT64_MAX)

/* Frozen v0 config floor: the smallest cfg struct_size connect()/resolve accept.
 * The required prefix runs through `alloc` (url, protocol, backend, versions,
 * TLS, wt_path, perspective are all v0); every field past it is optional and
 * gated field-by-field by struct_size. This floor never moves, so an old caller
 * passing the v0 size keeps working against a newer library. Single source of
 * truth -- resolve and the ABI tests both use it. */
#define MOQ_ENDPOINT_CFG_V0_SIZE \
    (offsetof(moq_endpoint_cfg_t, alloc) + \
     sizeof(((moq_endpoint_cfg_t *)0)->alloc))

/* Pointer-only initializer: it cannot know the caller's struct size, so it
 * clears and stamps ONLY the frozen v0 prefix (MOQ_ENDPOINT_CFG_V0_SIZE) and
 * leaves every appended tail field untouched. A config produced this way runs
 * with the tail ABSENT (struct_size stops at the v0 floor), which resolves to
 * MOQ_WT_PROFILE_BACKEND_DEFAULT for wt_profile (no explicit selection), to 0
 * (the backend default) for handshake_timeout_us, to no setup tokens, to no
 * application certificate callback, LEGACY for uri_mode, the library default
 * for preferred_version and no URI policy. Because it never writes past the v0
 * floor, it can NEVER overflow the buffer of an old caller that predates a
 * later tail field. To SET any tail field, use moq_endpoint_cfg_init_sized(). */
MOQ_API void moq_endpoint_cfg_init(moq_endpoint_cfg_t *cfg);

/* Sized initializer: clears and stamps min(cfg_size, current sizeof) -- the
 * initializer to use whenever you set anything beyond the v0 floor (wt_profile,
 * handshake_timeout_us, setup_auth_tokens + setup_auth_token_count,
 * verify_certificate + verify_certificate_ctx, uri_mode, preferred_version,
 * accept_uri). Pass sizeof(moq_endpoint_cfg_t).
 * Never writes past cfg_size, so it is safe for a caller compiled against an older, smaller
 * header too. */
MOQ_API void moq_endpoint_cfg_init_sized(moq_endpoint_cfg_t *cfg,
                                         size_t cfg_size);

/* -- Lifecycle (§5.3) ------------------------------------------------ */

/* Resolve + validate the configuration, then create the managed transport
 * (its managed execution context -- a network thread, or wtquic-Network's
 * serial queue -- plus the QUIC context and MoQ session) and return the
 * endpoint. The
 * connection and handshake complete asynchronously; observe progress via
 * wait()/state(). Unless insecure_skip_verify is set, real certificate
 * verification (chain + server name, against ca_file or backend default roots
 * when available) is installed -- the safe path is the default at this tier. */
MOQ_API moq_result_t moq_endpoint_connect(const moq_endpoint_cfg_t *cfg,
                                          moq_endpoint_t **out);

/* Waits for the managed teardown to complete (joins the network thread,
 * or the wtquic-Network queue drain). Idempotent; callable while waiters are blocked
 * (implies a permanent interrupt). MOQ_ERR_WRONG_STATE with live
 * attachments -- destroy the attached receiver/sender first. */
MOQ_API moq_result_t moq_endpoint_stop(moq_endpoint_t *ep);

/* void, like every libmoq destroy. Precondition: no live attachments and
 * stopped. Violation is a programming error: debug builds assert; release
 * builds force-stop first (join the thread, drain the task queue) and
 * then destroy normally -- a live thread aimed at freed children would be
 * UAF, never the release behavior. Canonical teardown: child _destroy ->
 * optional moq_endpoint_drain (for graceful send-side flush) ->
 * moq_endpoint_stop -> moq_endpoint_destroy. */
MOQ_API void moq_endpoint_destroy(moq_endpoint_t *ep);

/* Block until the endpoint changes state / wakes, or timeout_us elapses.
 * Returns MOQ_OK on wake, MOQ_DONE on timeout, MOQ_ERR_INTERRUPTED while the
 * latch is set, MOQ_ERR_CLOSED once terminal. */
MOQ_API moq_result_t moq_endpoint_wait(moq_endpoint_t *ep, uint64_t timeout_us);

/* Nudge the endpoint to run a pump cycle soon: schedules endpoint service work
 * (every attached hook runs), it does NOT notify waiters directly -- a blocked
 * moq_endpoint_wait()/attached-service wait returns because the nudged pump
 * cycle marks activity, and that activity flag is coalesced (multiple nudges
 * before the cycle collapse into one) and LEVEL-retained (a wake that lands
 * before the next wait is not lost; the wait returns immediately).
 *
 * Callable from any thread. Non-allocating and infallible by contract:
 * unlike moq_endpoint_post() it queues no task, so it cannot fail -- use it
 * to schedule reconciliation of state the caller has already recorded
 * (record state, then wake), keeping the caller's own command atomic with no
 * fallible allocation. Best-effort and idempotent; harmless (a no-op) after
 * stop or terminal. It does not interact with the interrupt latch: waking a
 * latched endpoint still yields MOQ_ERR_INTERRUPTED from blocking calls. */
MOQ_API void moq_endpoint_wake(moq_endpoint_t *ep);

/* Block (app thread only) until the output this endpoint's attached services
 * owe has been handed to the session and the transport has flushed it. Call
 * this after the last writes (and end_track for a finite publication) while
 * the sender is still attached -- destroying it first discards what it still
 * has queued -- and before moq_endpoint_stop(), so the abrupt stop does not
 * truncate it.
 *
 * Scope of the guarantee, in layers:
 *   - Services: every attached service has handed over the output it owes --
 *     for a sender its queued END_OF_TRACK markers, objects of tracks with
 *     demand, catalog generations, resets and other lifecycle work. NOT
 *     owed: media retained for a track nobody has subscribed to and that
 *     has no established publication, together with an END_OF_TRACK queued
 *     behind that media -- both stay owned by the sender, are neither
 *     delivered nor discarded by this, and go out (media, then END) if a
 *     subscriber arrives. A graceful completion (moq_media_sender_complete)
 *     waiting on such media, or on a generated timeline's records kept for
 *     a timeline nobody has subscribed to, is held with them the same way:
 *     its terminal catalog is owed once they have gone out. A failed
 *     completion owes no further end and no terminal catalog; output it had
 *     already accepted and that is still sendable stays owed. A drain
 *     success is never proof that a broadcast completed. An accepted
 *     publication paused at Forward 0
 *     still owes its terminal: a drain with it held times out (MOQ_DONE)
 *     rather than reporting success.
 *   - Local flush: each still-required reliable stream byte and FIN has been
 *     handed to the transport for sending (local stream backlog empty).
 *   - Transport acknowledgement, where the backend observes it: on the
 *     picoquic backends (raw QUIC and WebTransport) the peer has also
 *     acknowledged every stream's sent bytes, its FIN once sent, and a
 *     RESET with its reliable prefix -- never a FIN the peer has yet to
 *     send, and not the packet backlog, which ACK-only packets keep non-empty
 *     on an idle connection.
 *   - It does NOT mean the peer application has consumed the media.
 * A success is always observed after the services' output was handed over:
 * never a transport state that predates it.
 *
 * Direct MsQuic and WTQuic-MsQuic may conservatively wait for acknowledgment
 * of non-superseded output. Accepted send cancellation or lawful peer STOP
 * of ordinary stream output can withdraw that stream's remaining byte/FIN
 * intent, but success must wait for retained ownership to settle. This does not
 * prove delivery of canceled bytes, FIN or RESET. Unexpected or critical-stream
 * output loss remains an unsuccessful flush. Datagrams are outside the
 * reliable-stream guarantee.
 *
 * Returns:
 *   MOQ_OK              the services' owed output was handed over and flushed
 *                       (and acknowledged, where observed): stopping will not
 *                       truncate it.
 *   MOQ_DONE            timeout_us elapsed before the flush completed.
 *   MOQ_ERR_INTERRUPTED the interrupt latch is set.
 *   MOQ_ERR_CLOSED      the endpoint went terminal/fatal during the drain.
 *   MOQ_ERR_WRONG_STATE called from the endpoint's managed execution
 *                       context (a hook).
 *   MOQ_ERR_UNSUPPORTED the active backend cannot prove a local stream flush (it
 *                       never claims a drain it cannot verify; mvfst/proxygen).
 *   MOQ_ERR_INTERNAL    unsuccessful flush, including accepted-output loss;
 *                       this result does not itself make the endpoint terminal.
 *
 * moq_endpoint_stop() is unchanged and remains an abrupt stop when graceful
 * drain is not wanted. */
MOQ_API moq_result_t moq_endpoint_drain(moq_endpoint_t *ep, uint64_t timeout_us);

typedef enum moq_endpoint_state {
    MOQ_ENDPOINT_CONNECTING = 0,
    MOQ_ENDPOINT_ESTABLISHED,
    MOQ_ENDPOINT_RECONNECTING,    /* reserved now so reconnect lands without an
                                     ABI break; v0 never enters this state */
    MOQ_ENDPOINT_DRAINING,        /* GOAWAY received */
    MOQ_ENDPOINT_CLOSED
} moq_endpoint_state_t;

MOQ_API moq_endpoint_state_t moq_endpoint_state(const moq_endpoint_t *ep);

/* The New Session URI of the session-wide GOAWAY the peer sent (the endpoint
 * is then DRAINING): where to reconnect, empty meaning the current URI. The
 * attached media sender and receiver record it as their facades observe the
 * GOAWAY. MOQ_OK copies the bytes (not NUL-terminated) into buf and sets
 * *uri_len; MOQ_ERR_BUFFER when cap is short (*uri_len is the length
 * needed, nothing copied); MOQ_DONE when no GOAWAY was recorded;
 * MOQ_ERR_NOMEM when one was received but its URI could not be kept;
 * MOQ_ERR_INVAL for a NULL ep or uri_len, or a NULL buf with cap > 0.
 * Callable from any thread. */
MOQ_API moq_result_t moq_endpoint_get_goaway_uri(moq_endpoint_t *ep,
                                                 uint8_t *buf, size_t cap,
                                                 size_t *uri_len);
MOQ_API bool     moq_endpoint_is_closed(const moq_endpoint_t *ep);
MOQ_API bool     moq_endpoint_is_fatal(const moq_endpoint_t *ep);
/* The MoQ/bridge fatal code (nonzero => a protocol-level fatal). It is 0 for a
 * transport/handshake failure -- do NOT treat fatal_code == 0 as "no error";
 * use moq_endpoint_get_terminal() below to classify why an endpoint went
 * terminal. Kept for protocol-fatal callers and backward compatibility. */
MOQ_API uint64_t moq_endpoint_fatal_code(const moq_endpoint_t *ep);

/* Why an endpoint reached a terminal state, classified transport-agnostically.
 * TLS_CERTIFICATE / TLS / TRANSPORT let a consumer tell a certificate rejection
 * apart from a timeout or an ALPN mismatch, which moq_endpoint_fatal_code()
 * cannot (they all report 0). */
typedef enum moq_endpoint_terminal_reason {
    MOQ_ENDPOINT_TERMINAL_NONE = 0,        /* not terminal, or reason unknown */
    MOQ_ENDPOINT_TERMINAL_CLEAN,           /* clean close */
    MOQ_ENDPOINT_TERMINAL_PROTOCOL,        /* MoQ/bridge fatal (nonzero code) */
    MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE, /* peer certificate verification failed */
    MOQ_ENDPOINT_TERMINAL_TLS,             /* other TLS/crypto handshake failure */
    MOQ_ENDPOINT_TERMINAL_TRANSPORT        /* generic transport/handshake failure */
} moq_endpoint_terminal_reason_t;

/* ABI-forward (struct_size prefix; sized-init like the media stats structs). */
typedef struct moq_endpoint_terminal {
    uint32_t struct_size;
    moq_endpoint_terminal_reason_t reason;
    /* Transport-native detail, carried as RAW BITS: the MoQ/bridge fatal
     * code for PROTOCOL; for TLS/TLS_CERTIFICATE/TRANSPORT the backend's
     * native error -- picoquic: a QUIC CRYPTO_ERROR whose low byte is the
     * TLS alert; wtquic-Network: a Security OSStatus, a NEGATIVE value
     * stored two's-complement (reinterpret as int64_t to read it; e.g.
     * errSecNotTrusted -67843 for a rejected self-signed peer); 0 for
     * CLEAN/NONE. */
    uint64_t detail_code;
} moq_endpoint_terminal_t;

/* v0 layout size: the whole struct as first published (all fields are v0). */
#define MOQ_ENDPOINT_TERMINAL_V0_SIZE \
    (offsetof(moq_endpoint_terminal_t, detail_code) + sizeof(uint64_t))

/* Classify the endpoint's terminal reason. Fills `*out` (sized-copy by
 * `out_size`, stamping out->struct_size with bytes written). Returns MOQ_OK, or
 * MOQ_ERR_INVAL for NULL args or an out_size below MOQ_ENDPOINT_TERMINAL_V0_SIZE.
 * On a non-terminal endpoint the reason is MOQ_ENDPOINT_TERMINAL_NONE. */
MOQ_API moq_result_t moq_endpoint_get_terminal(const moq_endpoint_t *ep,
                                               moq_endpoint_terminal_t *out,
                                               size_t out_size);

/* Returns 0 (no moq_version_t value) until the endpoint reaches ESTABLISHED;
 * the negotiated version thereafter. The media tier reads it at
 * CATALOG_READY to configure parsers/encoders. */
MOQ_API moq_version_t moq_endpoint_negotiated_version(const moq_endpoint_t *ep);

/* -- Interrupt latch (§5.3) ------------------------------------------ *
 * Sticky, not a one-shot wake: while latched, every blocking call on the
 * endpoint AND on attached media objects returns MOQ_ERR_INTERRUPTED
 * immediately (including calls made after the latch was set -- no re-block
 * race). Non-terminal: clearing the latch restores normal blocking.
 * Idempotent, callable from any thread. Maps 1:1 onto GStreamer
 * unlock()/unlock_stop(), the VLC flush/abort window, and FFmpeg's
 * interrupt-callback-returns-1-until-cleared. */
MOQ_API void moq_endpoint_set_interrupted(moq_endpoint_t *ep, bool interrupted);

/* -- post() executor (§5.4) ------------------------------------------ *
 * Run fn on the endpoint's managed execution context (its network
 * thread, or wtquic-Network's serial queue), where calling moq_session_* is
 * legal. Asynchronous; completion observable via wait()/events or app state
 * set inside fn.
 *
 * Lifetime contract (normative, exactly-once):
 *  - post() after the endpoint is terminal returns MOQ_ERR_CLOSED and never
 *    runs fn -- the caller still owns ctx.
 *  - If post() returns MOQ_OK, fn is invoked EXACTLY ONCE, on the managed
 *    execution context: either normally (live session) or during stop()/terminal drain
 *    with session == NULL as the closed marker; fn cleans up ctx only in
 *    that case. No accepted task is silently dropped.
 *  - Accepted tasks run in submission order (FIFO).
 *  - fn MUST NOT block: it runs on the managed execution context; a
 *    blocking fn stalls every attachment on this endpoint (and, on
 *    wtquic-Network, the transport's own serial domain).
 *  - Memory visibility: everything fn writes before returning is visible to
 *    an app thread once that thread's next blocking call on this endpoint or
 *    any attachment returns, and once any poll_* on an attachment returns an
 *    item. */
typedef moq_result_t (*moq_endpoint_task_fn)(moq_endpoint_t *ep,
                                             moq_session_t *session,
                                             uint64_t now_us, void *ctx);

MOQ_API moq_result_t moq_endpoint_post(moq_endpoint_t *ep,
                                       moq_endpoint_task_fn fn, void *ctx);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_ENDPOINT_H */
