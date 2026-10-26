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
MOQ_PQ_API int moq_picoquic_set_cert_verifier(picoquic_quic_t *quic,
                                              const char *ca_file);

/* -- delegated certificate trust ------------------------------------------- *
 *
 * The provider-stable shape. OpenSSL builds implement it; mbedTLS builds
 * report no support and refuse to install, so a consumer asks at runtime
 * rather than compiling provider-conditionally.
 *
 * This is the adapter's one spelling of delegated trust: any other consumer
 * of the same idea uses these names rather than a parallel vocabulary.
 *
 * REJECT is zero so a default-initialised or unrecognised verdict fails
 * closed. */
typedef enum moq_picoquic_cert_verdict {
    MOQ_PICOQUIC_CERT_REJECT = 0,
    MOQ_PICOQUIC_CERT_TRUST  = 1
} moq_picoquic_cert_verdict_t;

/* The trust decision, over BORROWED spans valid for the call only.
 * `certificates` is leaf-first DER.
 *
 * The callback is responsible for the trust policy. Whoever implements it is
 * also responsible for the reference-identity check: this adapter does not
 * perform it on the callback's behalf, so an implementation MUST perform it
 * itself before consulting any platform trust bridge. Nothing here names a
 * key type or key exchange; a platform that needs one derives it from the
 * protocol (TLS 1.3 for QUIC), not from the chain. */
typedef moq_picoquic_cert_verdict_t (*moq_picoquic_cert_verify_fn)(
    void *ctx,
    moq_bytes_t server_name,
    const moq_bytes_t *certificates,
    size_t certificate_count);

/* 1 when this adapter build can install a delegated trust callback, else 0.
 *
 * Declared and DEFINED in every provider build: a consumer must be able to ask
 * without provider-conditional compilation of its own. The OpenSSL build
 * reports support; the mbedTLS build reports 0 rather than accepting a
 * callback it would ignore. */
MOQ_PQ_API int moq_picoquic_cert_verify_callback_supported(void);

/* Install a delegated trust callback on a client context.
 *
 * `ctx` is opaque to this adapter and MAY be NULL: a stateless callback needs
 * no context, and one is never required. When non-NULL it stays caller-owned
 * until the QUIC context is destroyed. The callback
 * runs synchronously on picoquic's network thread and must not re-enter
 * picoquic or LibMoQ. Returns 0 on success and -1 for invalid arguments,
 * allocation or setup failure, a setter rejection, or an unsupported provider.
 * Transactional under the same ownership rules as
 * moq_picoquic_set_cert_verifier(): on failure nothing is installed and any
 * previously installed verifier is unchanged. */
MOQ_PQ_API int moq_picoquic_set_cert_verify_callback(
    picoquic_quic_t *quic,
    moq_picoquic_cert_verify_fn verify,
    void *ctx);

/* Preflight a CA bundle path without a QUIC context: returns 1 if `ca_file`
 * loads as a trust store (or, for backends with an implicit system store, is
 * NULL/empty), 0 if it cannot be loaded. Lets the service endpoint report a
 * bad CA file as a configuration error (MOQ_ERR_INVAL) up front, rather than
 * as a generic connect failure. */
MOQ_PQ_API int moq_picoquic_ca_file_loadable(const char *ca_file);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_PICOQUIC_VERIFY_H */
