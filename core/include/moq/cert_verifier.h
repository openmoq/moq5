#ifndef MOQ_CERT_VERIFIER_H
#define MOQ_CERT_VERIFIER_H

/*
 * moq_cert_verifier_t: the certificate chain decision, delegated to the
 * application.
 *
 * The native verifier validates the chain against a CA file or the system
 * trust store. Some platforms cannot express their trust rules that way: a CA
 * set is only part of the answer, and supplying one bypasses everything else
 * the platform would apply -- per-app pinning, administrator/enterprise CA
 * policy, revocation lists, and per-domain overrides. A certificate the
 * platform would reject is then accepted.
 *
 * Delegating the decision keeps that logic where it lives. Only the trust
 * decision moves: the TLS stack still verifies CertificateVerify with the
 * leaf's public key, so nothing about the handshake proof is delegated.
 */

#include <moq/types.h>

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Outcome of a delegated chain check. The values are a stable numeric
 * contract: a platform verifier's own status can be mapped onto them once and
 * passed through. Each maps to a distinct TLS alert, so a peer learns why the
 * chain was refused rather than getting a generic failure.
 */
typedef enum moq_cert_status {
    MOQ_CERT_OK                = 0, /* trusted */
    MOQ_CERT_UNAVAILABLE       = 1, /* verifier could not run at all; NOT a
                                       statement about the chain */
    MOQ_CERT_EXPIRED           = 2, /* outside its validity period */
    MOQ_CERT_UNKNOWN_ISSUER    = 3, /* does not chain to a trusted root */
    MOQ_CERT_REVOKED           = 4,
    MOQ_CERT_BAD_ENCODING      = 5, /* not parsable as a certificate */
    MOQ_CERT_INVALID_EXTENSION = 6  /* parsable, but an extension forbids this
                                       use (e.g. EKU without server auth) */
} moq_cert_status_t;

typedef struct moq_cert_verifier {
    /*
     * Is `chain` trusted for `server_name`? DER-encoded, leaf first,
     * `num_certs` entries, valid only for the duration of the call.
     * `server_name` is NUL-terminated, or NULL if the peer offered none.
     *
     * Called on the transport thread during the handshake, so it must not
     * block on anything that could wait on that thread.
     */
    moq_cert_status_t (*verify_chain)(void *user_data, const char *server_name,
                                      const moq_bytes_t *chain,
                                      size_t num_certs);
    /* Opaque, passed back to verify_chain. */
    void *user_data;
} moq_cert_verifier_t;

#ifdef __cplusplus
}
#endif

#endif /* MOQ_CERT_VERIFIER_H */
