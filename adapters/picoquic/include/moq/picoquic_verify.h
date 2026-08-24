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
 * verifier types needed to do this are not reachable from installed
 * headers (picotls headers are not installed; ptls_verify_certificate_t
 * is opaque), so this small helper wraps the picotls-OpenSSL verifier
 * and is the supported way for a cold consumer to enable verification.
 *
 * The moq_pico_wt_managed facade and the raw moq_pq_threaded client both
 * call this themselves to fail closed by default (insecure_skip_verify=false),
 * so a DEFAULT client is verified against the system trust store with NO
 * configure_quic hook and no extra steps. Call this helper directly ONLY to
 * customize that default — e.g. to pin a private CA — from the configure_quic
 * hook, which runs after the automatic default verifier and transactionally
 * replaces it. Passing a non-NULL PEM bundle path is what makes the hook do
 * something the default does not; re-installing system trust with a NULL
 * ca_file from the hook is redundant with the automatic default.
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

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque picoquic QUIC context (matches picoquic's own forward decl). */
typedef struct st_picoquic_quic_t picoquic_quic_t;

/*
 * Install an OpenSSL-backed certificate verifier on `quic` (a client
 * context). The verifier validates the peer's certificate chain and the
 * server name (SNI/hostname), and rejects a chain that does not validate
 * — so the handshake fails closed instead of silently accepting.
 *
 * ca_file:
 *   NULL  -> use the OpenSSL default trust store (system CAs).
 *   path  -> use the PEM bundle at `path` as the trust anchors.
 *
 * The verifier's lifetime is owned by `quic`: picoquic frees it when the
 * QUIC context is destroyed. Do not call before picoquic_create.
 *
 * Returns 0 on success, -1 on error (bad args, CA file load failure, or
 * allocation failure). Transactional: the helper builds the trust store and
 * verifier first and calls the picoquic setter only after both succeed, so on
 * error the replacement verifier is NOT installed and any previously installed
 * verifier remains unchanged.
 */
MOQ_API int moq_picoquic_set_cert_verifier(picoquic_quic_t *quic,
                                           const char *ca_file);

/* Preflight a CA bundle path without a QUIC context: returns 1 if `ca_file`
 * loads as a trust store (or is NULL/empty = system roots), 0 if it cannot be
 * loaded. Lets the service endpoint report a bad CA file as a configuration
 * error (MOQ_ERR_INVAL) up front, rather than as a generic connect failure. */
MOQ_API int moq_picoquic_ca_file_loadable(const char *ca_file);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_PICOQUIC_VERIFY_H */
