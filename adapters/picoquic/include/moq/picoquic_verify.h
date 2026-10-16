#ifndef MOQ_PICOQUIC_VERIFY_H
#define MOQ_PICOQUIC_VERIFY_H

/*
 * Stability: pre-1.0, transport-specific adapter API. Mature enough for
 * integration pilots, but may change before 1.0.
 *
 * Production certificate verification for picoquic clients.
 *
 * picoquic's built-in default has no CA store and ACCEPTS the peer
 * certificate, so a client context with no verifier installed is NOT
 * production-safe — a real verifier must be installed. The picotls/picoquic
 * verifier types needed to do this are not part of the LibMoQ public
 * surface, so this small helper wraps the configured picoquic TLS
 * backend verifier and is the supported way for a cold consumer to
 * enable verification.
 *
 * The moq_pico_wt_managed facade and the raw moq_pq_threaded client both
 * call this themselves to fail closed by default (insecure_skip_verify=false),
 * so a DEFAULT client is verified with NO configure_quic hook and no extra
 * steps whenever the configured backend can create its default verifier.
 * Call this helper directly ONLY to customize that default — e.g. to pin a
 * private CA — from the configure_quic hook, which runs after the automatic
 * default verifier and transactionally replaces it. In OpenSSL builds,
 * re-installing system trust with a NULL ca_file from the hook is redundant
 * with the automatic default. In raw mbedTLS-only embedded builds, prefer the
 * moq_pq_threaded_cfg_t.ca_file field (or the service endpoint ca_file) so the
 * automatic verifier can be installed before any hook runs.
 *
 *   // Private-CA customization (the only reason to add the hook):
 *   static int pin_private_ca(picoquic_quic_t *quic, void *ctx) {
 *       (void)ctx;
 *       return moq_picoquic_set_cert_verifier(quic, "/etc/moq/ca.pem"); // 0=OK
 *   }
 *   cfg.configure_quic = pin_private_ca;  // moq_pq_threaded OR pico WT managed
 *
 * Both moq_pq_threaded_cfg_t.configure_quic and
 * moq_pico_wt_managed_cfg_t.configure_quic take a picoquic_quic_t*, so
 * the same call works for raw picoquic and pico WT managed clients.
 *
 * Packaging: this helper lives in the base picoquic adapter (CMake
 * component adapter-picoquic; folded into the libmoq.pc package when
 * libmoq is built with the picoquic adapter — no standalone .pc).
 *   - Raw moq_pq_threaded consumers get it transitively: the
 *     adapter-picoquic-threaded component links adapter-picoquic, and the
 *     libmoq.pc package already carries it.
 *   - pico WT managed consumers get it transitively: the
 *     adapter-pico-wt-managed component now depends on adapter-picoquic
 *     (the managed client installs the default verifier), so
 *     find_package(libmoq COMPONENTS adapter-pico-wt-managed) /
 *     `pkg-config --libs libmoq-pico-wt-managed` already carry it — no
 *     separate request is needed to call this helper from configure_quic.
 * See the transport integration guide §5. Picoquic-specific TLS policy,
 * not a generic libmoq abstraction.
 */

#include <moq/export.h>
#include <moq/types.h>   /* moq_bytes_t, for the delegated-trust callback */

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque picoquic QUIC context (matches picoquic's own forward decl). */
typedef struct st_picoquic_quic_t picoquic_quic_t;

/*
 * Install a picoquic TLS-backend certificate verifier on `quic` (a client
 * context). The verifier validates the peer's certificate chain and the
 * server name (SNI/hostname), and rejects a chain that does not validate
 * — so the handshake fails closed instead of silently accepting.
 *
 * ca_file:
 *   NULL  -> use the backend default trust store when the configured backend
 *            has one (OpenSSL builds do; mbedTLS-only embedded builds do not).
 *   path  -> use the PEM bundle at `path` as the trust anchors.
 *
 * In an mbedTLS-only picoquic build, this helper requires a non-empty PEM
 * `ca_file` and returns -1 without installing a verifier when no explicit CA
 * file is supplied. That preserves the fail-closed default on targets without
 * a system trust store; applications should pass their platform credential
 * route through the raw threaded ca_file field or the service ca_file field
 * when using the raw picoquic backend.
 *
 * The verifier's lifetime is owned by `quic`: picoquic frees it when the
 * QUIC context is destroyed. Do not call before picoquic_create.
 *
 * Returns 0 on success, -1 on error (bad args, CA file load failure,
 * allocation failure, or a setter rejection on picoquic revisions that can
 * report one). Transactional: the helper builds the trust store and verifier
 * first and calls the picoquic setter only after both succeed. On revisions
 * with the error-reporting setter, a rejected replacement is disposed by this
 * helper and any previously installed verifier remains unchanged. Older
 * picoquic revisions expose a void setter that unconditionally takes ownership.
 */
MOQ_API int moq_picoquic_set_cert_verifier(picoquic_quic_t *quic,
                                           const char *ca_file);

/* -- Delegated certificate trust ----------------------------------------- *
 *
 * The provider-stable shape of application-owned trust for picoquic CLIENTS.
 * REJECT is zero so a default-initialised or unrecognised verdict fails
 * closed. This is the adapter's one spelling of delegated trust; the service
 * tier translates its own verdict onto it at exactly one site. */
typedef enum moq_picoquic_cert_verdict {
    MOQ_PICOQUIC_CERT_REJECT = 0,
    MOQ_PICOQUIC_CERT_TRUST  = 1
} moq_picoquic_cert_verdict_t;

/* The trust decision over BORROWED spans valid for the call only:
 * `server_name` is the exact name the handshake authenticates, `certificates`
 * the leaf-first DER chain as received (at most 16 entries).
 *
 * Before the callback runs the adapter has matched the leaf against
 * `server_name` under the same reference-identity rules as the provider's
 * own verifier (DNS name against dNSName SANs or the subject CN, no
 * partial-label wildcards; IP literal against iPAddress SANs) and refused a
 * chain that does not name it, and it refuses an absent or empty name; the
 * callback is never consulted for those. After a TRUST verdict the provider
 * still extracts the leaf key and enforces TLS CertificateVerify. Everything
 * else about the chain -- issuer trust, validity, purpose, constraints,
 * revocation, application policy -- is the callback's to decide: the adapter
 * verifies against an empty store and hands its chain-policy outcome to the
 * delegate. Operational failures (allocation, library) are never converted
 * into acceptance.
 *
 * Runs synchronously on picoquic's network thread; must not re-enter picoquic
 * or LibMoQ. */
typedef moq_picoquic_cert_verdict_t (*moq_picoquic_cert_verify_fn)(
    void *ctx,
    moq_bytes_t server_name,
    const moq_bytes_t *certificates,
    size_t certificate_count);

/* True when this adapter build can install a delegated trust callback.
 * Declared and DEFINED in every provider build so a consumer asks at runtime:
 * the OpenSSL provider reports support; mbedTLS reports false rather than
 * accepting a callback it would ignore. */
MOQ_API bool moq_picoquic_supports_cert_verify_callback(void);

/* Install a delegated trust callback on a client context. `ctx` is opaque,
 * MAY be NULL, and stays caller-owned until the QUIC context is destroyed.
 * Returns MOQ_OK on success; MOQ_ERR_INVAL for NULL quic or verify (checked
 * before provider support); MOQ_ERR_UNSUPPORTED for a provider without this
 * capability; MOQ_ERR_NOMEM for allocation failure; MOQ_ERR_WRONG_STATE when
 * picoquic reports frozen TLS configuration; MOQ_ERR_INTERNAL for other
 * provider initialization or installer failures. Transactional under
 * the same ownership rules as moq_picoquic_set_cert_verifier(): on failure
 * nothing is installed and any previously installed verifier is unchanged. On
 * picoquic revisions with only the historical void setter, a successful return
 * means the setter took ownership; that API has no failure outcome. */
MOQ_API moq_result_t moq_picoquic_set_cert_verify_callback(
    picoquic_quic_t *quic,
    moq_picoquic_cert_verify_fn verify,
    void *ctx);

/* Preflight a CA bundle path without a QUIC context: returns 1 if `ca_file`
 * loads as a trust store (or, for backends with an implicit system store, is
 * NULL/empty), 0 if it cannot be loaded. Lets the service endpoint report a
 * bad CA file as a configuration error (MOQ_ERR_INVAL) up front, rather than
 * as a generic connect failure. */
MOQ_API int moq_picoquic_ca_file_loadable(const char *ca_file);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_PICOQUIC_VERIFY_H */
