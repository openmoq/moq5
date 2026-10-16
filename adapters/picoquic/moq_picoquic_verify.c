/*
 * moq_picoquic_set_cert_verifier — production TLS verification for
 * picoquic clients. See <moq/picoquic_verify.h>.
 *
 * Wraps the configured picotls verifier (OpenSSL or mbedTLS, with
 * hostname/SNI checking) and installs it via picoquic. picoquic owns
 * the verifier lifetime: the dispose fn is invoked when the QUIC
 * context is freed.
 */

#include <moq/picoquic_verify.h>

#include <picoquic.h>

#include <stddef.h>
#include <stdint.h>

/* picoquic 33afc776 introduced the error-reporting setter together with the
 * TLS-config-frozen error. Older supported revisions expose only the historical
 * void setter, which cannot fail and takes ownership before returning. */
static int moq_picoquic_install_verifier(
    picoquic_quic_t *quic,
    ptls_verify_certificate_t *verifier,
    picoquic_free_verify_certificate_ctx dispose)
{
#if defined(PICOQUIC_ERROR_TLS_CONFIG_FROZEN)
    return picoquic_set_verify_certificate_callback_ex(quic, verifier, dispose);
#else
    picoquic_set_verify_certificate_callback(quic, verifier, dispose);
    return 0;
#endif
}

#if defined(MOQ_PICOQUIC_VERIFY_OPENSSL)
#include <picotls/openssl.h>
#include <openssl/x509.h>

#include <stdlib.h>

/* Free fn handed to picoquic. super is the first member of the OpenSSL
 * verifier, so the ptls_verify_certificate_t* aliases the wrapper. */
static void moq_picoquic_dispose_verifier(ptls_verify_certificate_t *verifier)
{
    if (!verifier) return;
    ptls_openssl_dispose_verify_certificate(
        (ptls_openssl_verify_certificate_t *)verifier);
    free(verifier);
}

int moq_picoquic_ca_file_loadable(const char *ca_file)
{
    /* Empty/NULL means "system roots" -- always acceptable. */
    if (ca_file == NULL || ca_file[0] == '\0') return 1;
    X509_STORE *store = X509_STORE_new();
    /* OOM is not a CA-file problem; do not report the file as unloadable. */
    if (store == NULL) return 1;
    int ok = (X509_STORE_load_locations(store, ca_file, NULL) == 1);
    X509_STORE_free(store);
    return ok;
}

int moq_picoquic_set_cert_verifier(picoquic_quic_t *quic, const char *ca_file)
{
    if (!quic) return -1;

    X509_STORE *store = NULL;
    if (ca_file != NULL && ca_file[0] != '\0') {
        store = X509_STORE_new();
        if (store == NULL) return -1;
        if (X509_STORE_load_locations(store, ca_file, NULL) != 1) {
            X509_STORE_free(store);
            return -1;
        }
    }

    ptls_openssl_verify_certificate_t *verifier =
        (ptls_openssl_verify_certificate_t *)calloc(1, sizeof(*verifier));
    if (verifier == NULL) {
        if (store != NULL) X509_STORE_free(store);
        return -1;
    }

    /* NULL store -> picotls uses the OpenSSL default (system) trust store.
     * A non-NULL store is up-ref'd by init, so we drop our reference. */
    if (ptls_openssl_init_verify_certificate(verifier, store) != 0) {
        free(verifier);
        if (store != NULL) X509_STORE_free(store);
        return -1;
    }
    if (store != NULL) X509_STORE_free(store);

    if (moq_picoquic_install_verifier(quic, &verifier->super,
                                      moq_picoquic_dispose_verifier) != 0) {
        moq_picoquic_dispose_verifier(&verifier->super);
        return -1;
    }
    return 0;
}

/* -- Delegated certificate trust (OpenSSL provider) --------------------- *
 *
 * picotls' OpenSSL verifier already owns what a delegated verifier must not
 * reimplement: the chain-verification operation and the verify_sign /
 * verify_data machinery that enforces TLS CertificateVerify and releases the
 * leaf key exactly once. It also exposes the seam this needs, the
 * ptls_openssl_verify_certificate_t override_callback, which picotls calls
 * with its chain result AFTER verification and BEFORE it extracts the public
 * key (lib/openssl.c, verify_cert); returning 0 there makes picotls continue
 * into its own verify_sign path.
 *
 * The delegate replaces chain POLICY only. Two things stay with the library
 * and run before the delegate is consulted: the reference-identity check
 * below, and the structural refusals (no chain, too many certificates, a leaf
 * that does not decode). After a TRUST verdict the inner verifier still parses
 * every DER certificate, extracts the leaf key and installs verify_sign.
 *
 * The installed verifier holds only immutable state: the delegate, its
 * context and the owned base OpenSSL verifier over a PRIVATE EMPTY store. Each
 * invocation shallow-copies that base verifier onto its own stack frame and
 * points the copy at a stack-local override, so no per-call routing lives in
 * the shared object. */

#include <picotls/openssl.h>
#include <openssl/x509v3.h>
#include <limits.h>
#include <string.h>

/* The chain-cardinality bound. Enforced before any stack-backed conversion, so
 * a peer-supplied count can never size or overrun a stack object. */
#define MOQ_PICOQUIC_CERT_CHAIN_MAX 16u

typedef struct moq_pq_callback_verifier {
    ptls_verify_certificate_t         super;    /* what picoquic installs */
    ptls_openssl_verify_certificate_t openssl;  /* owned base; immutable here */
    moq_picoquic_cert_verify_fn       verify;   /* immutable */
    void                             *ctx;      /* immutable; may be NULL */
} moq_pq_callback_verifier_t;

/* Reference identity, under the same rules as picotls' own verifier
 * (lib/openssl.c verify_cert_chain): an IP literal against iPAddress SANs, a
 * DNS name against dNSName SANs or, for a leaf without them, the subject CN,
 * with X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS so only a full-label wildcard
 * matches. A missing or empty name is never accepted for a client. 1 when the
 * leaf names `server_name`, else 0; a leaf that does not decode, or whose
 * length the decoder cannot take, is 0. */
static int moq_pq_reference_identity(const char *server_name,
                                     const uint8_t *leaf_der, size_t leaf_len)
{
    if (server_name == NULL || server_name[0] == '\0') return 0;
    if (leaf_der == NULL || leaf_len == 0 || leaf_len > (size_t)LONG_MAX) return 0;
    const unsigned char *p = leaf_der;
    X509 *cert = d2i_X509(NULL, &p, (long)leaf_len);
    if (cert == NULL) return 0;
    int ok;
    if (ptls_server_name_is_ipaddr(server_name)) {
        ok = X509_check_ip_asc(cert, server_name, 0) == 1;
    } else {
        ok = X509_check_host(cert, server_name, strlen(server_name),
                             X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS, NULL) == 1;
    }
    X509_free(cert);
    return ok;
}

/* The chain-verification results a certificate POLICY may legitimately
 * answer, enumerated from the pinned picotls source: every value
 * verify_cert_chain()'s switch produces plus the two verify_cert() produces
 * itself for an unusable or absent chain. Operational failures
 * (PTLS_ERROR_NO_MEMORY, PTLS_ERROR_LIBRARY) and every unlisted value are
 * not something a policy answered and are preserved. */
static int moq_pq_result_is_policy(int ret)
{
    switch (ret) {
    case PTLS_ALERT_BAD_CERTIFICATE:
    case PTLS_ALERT_CERTIFICATE_REVOKED:
    case PTLS_ALERT_CERTIFICATE_EXPIRED:
    case PTLS_ALERT_CERTIFICATE_UNKNOWN:
    case PTLS_ALERT_UNKNOWN_CA:
    case PTLS_ALERT_CERTIFICATE_REQUIRED:
        return 1;
    default:
        return 0;
    }
}

/* Applies an already-taken TRUST decision to picotls' chain result: a policy
 * refusal becomes success; an operational failure or an unrecognised result
 * is preserved exactly. Reached only after the delegate answered TRUST. */
static int moq_pq_override_trust(
    ptls_openssl_override_verify_certificate_t *self,
    ptls_t *tls, int ret, int ossl_ret, X509 *cert, STACK_OF(X509) * chain)
{
    (void)self; (void)tls; (void)ossl_ret; (void)cert; (void)chain;
    return moq_pq_result_is_policy(ret) ? 0 : ret;
}

static int moq_pq_verify_with_callback(
    ptls_verify_certificate_t *super, ptls_t *tls, const char *server_name,
    int (**verify_sign)(void *, uint16_t, ptls_iovec_t, ptls_iovec_t),
    void **verify_data, ptls_iovec_t *certs, size_t num_certs)
{
    moq_pq_callback_verifier_t *self = (moq_pq_callback_verifier_t *)super;

    /* Cleared before any work, so every refusing path leaves them clear. */
    if (verify_sign != NULL) *verify_sign = NULL;
    if (verify_data != NULL) *verify_data = NULL;

    /* Structural refusals, none of which consult the delegate: no chain, an
     * incoherent (NULL, n > 0) pair, and a count past the fixed bound. */
    if (certs == NULL || num_certs == 0 ||
        num_certs > MOQ_PICOQUIC_CERT_CHAIN_MAX)
        return PTLS_ALERT_BAD_CERTIFICATE;

    /* Identity, before the delegate: a leaf that does not name the server,
     * or no name to check against, is refused here. */
    if (!moq_pq_reference_identity(server_name, certs[0].base, certs[0].len))
        return PTLS_ALERT_BAD_CERTIFICATE;

    /* Borrowed for this call only, leaf-first, in picotls' own order. */
    moq_bytes_t chain[MOQ_PICOQUIC_CERT_CHAIN_MAX];
    for (size_t i = 0; i < num_certs; i++) {
        chain[i].data = certs[i].base;
        chain[i].len  = certs[i].len;
    }
    moq_bytes_t name = { (const uint8_t *)server_name, strlen(server_name) };

    /* Consulted HERE, exactly once, before picotls runs, so the delegate sees
     * every chain the identity check admitted -- including one picotls would
     * refuse as unusable, which the delegate cannot make usable. Only the
     * exact TRUST value proceeds. */
    if (self->verify(self->ctx, name, chain, num_certs) !=
        MOQ_PICOQUIC_CERT_TRUST)
        return PTLS_ALERT_BAD_CERTIFICATE;

    /* Per-invocation copies: the shared verifier is read-only and this
     * invocation's override routing lives on this stack frame. The inner
     * verifier still parses every certificate, extracts the leaf key and
     * installs verify_sign, so CertificateVerify is still enforced. */
    ptls_openssl_verify_certificate_t openssl = self->openssl;
    ptls_openssl_override_verify_certificate_t override;
    override.cb = moq_pq_override_trust;
    openssl.override_callback = &override;

    int ret = openssl.super.cb(&openssl.super, tls, server_name,
                               verify_sign, verify_data, certs, num_certs);
    if (ret != 0) {
        if (verify_sign != NULL) *verify_sign = NULL;
        if (verify_data != NULL) *verify_data = NULL;
    }
    return ret;
}

static void moq_pq_dispose_callback_verifier(ptls_verify_certificate_t *super)
{
    if (super == NULL) return;
    moq_pq_callback_verifier_t *self = (moq_pq_callback_verifier_t *)super;
    ptls_openssl_dispose_verify_certificate(&self->openssl);
    free(self);
}

bool moq_picoquic_supports_cert_verify_callback(void)
{
    return true;
}

moq_result_t moq_picoquic_set_cert_verify_callback(picoquic_quic_t *quic,
                                          moq_picoquic_cert_verify_fn verify,
                                          void *ctx)
{
    /* Refused before any allocation or setter call, so a refused install
     * cannot disturb an already-installed verifier. `ctx` may be NULL. */
    if (quic == NULL || verify == NULL) return MOQ_ERR_INVAL;

    moq_pq_callback_verifier_t *self =
        (moq_pq_callback_verifier_t *)calloc(1, sizeof(*self));
    if (self == NULL) return MOQ_ERR_NOMEM;

    /* A PRIVATE EMPTY store, never OpenSSL's default: in delegated mode the
     * callback is the sole chain-policy authority, and host roots would give
     * it a second, environment-dependent input. The empty store still lets
     * picotls parse the DER, run the verification operation, extract the
     * leaf key and enforce CertificateVerify; its policy alert is the
     * delegate's to answer. picotls takes its own reference. */
    X509_STORE *store = X509_STORE_new();
    if (store == NULL) {
        free(self);
        return MOQ_ERR_NOMEM;
    }
    int ret = ptls_openssl_init_verify_certificate(&self->openssl, store);
    if (ret != 0) {
        X509_STORE_free(store);
        free(self);
        return ret == PTLS_ERROR_NO_MEMORY ? MOQ_ERR_NOMEM : MOQ_ERR_INTERNAL;
    }
    X509_STORE_free(store);

    self->super.cb = moq_pq_verify_with_callback;
    self->super.algos = self->openssl.super.algos;
    self->verify = verify;
    self->ctx = ctx;

    ret = moq_picoquic_install_verifier(quic, &self->super,
                                       moq_pq_dispose_callback_verifier);
    if (ret != 0) {
        /* Rejected: this helper disposes the candidate it built; any
         * previously installed verifier is untouched. */
        moq_pq_dispose_callback_verifier(&self->super);
        /* Only the named provider error establishes a state refusal. */
#if defined(PICOQUIC_ERROR_TLS_CONFIG_FROZEN)
        if (ret == PICOQUIC_ERROR_TLS_CONFIG_FROZEN) return MOQ_ERR_WRONG_STATE;
#endif
#if defined(PICOQUIC_ERROR_MEMORY)
        if (ret == PICOQUIC_ERROR_MEMORY) return MOQ_ERR_NOMEM;
#endif
        return MOQ_ERR_INTERNAL;
    }
    return MOQ_OK;
}

#if defined(MOQ_PICOQUIC_VERIFY_TESTING)
/* White-box seams (test builds only): the override's result once the delegate
 * has answered TRUST, and the reference-identity check itself. */
int moq_picoquic_test_delegated_trust_result(int ret)
{
    return moq_pq_override_trust(NULL, NULL, ret, 0, NULL, NULL);
}
int moq_picoquic_test_reference_identity(const char *server_name,
                                         const uint8_t *leaf_der,
                                         size_t leaf_len)
{
    return moq_pq_reference_identity(server_name, leaf_der, leaf_len);
}
#endif

#elif defined(MOQ_PICOQUIC_VERIFY_MBEDTLS)
#include <ptls_mbedtls.h>

int moq_picoquic_ca_file_loadable(const char *ca_file)
{
    if (ca_file == NULL || ca_file[0] == '\0')
        return 0;   /* mbedTLS provider has no implicit system store here. */

    unsigned int store_loaded = 0;
    ptls_verify_certificate_t *verifier =
        ptls_mbedtls_get_certificate_verifier(ca_file, &store_loaded);
    if (verifier != NULL)
        ptls_mbedtls_dispose_verify_certificate(verifier);
    return verifier != NULL && store_loaded != 0;
}

int moq_picoquic_set_cert_verifier(picoquic_quic_t *quic, const char *ca_file)
{
    if (!quic || ca_file == NULL || ca_file[0] == '\0')
        return -1;

    unsigned int store_loaded = 0;
    ptls_verify_certificate_t *verifier =
        ptls_mbedtls_get_certificate_verifier(ca_file, &store_loaded);
    if (verifier == NULL || store_loaded == 0) {
        if (verifier != NULL)
            ptls_mbedtls_dispose_verify_certificate(verifier);
        return -1;
    }

    if (moq_picoquic_install_verifier(
            quic, verifier, ptls_mbedtls_dispose_verify_certificate) != 0) {
        ptls_mbedtls_dispose_verify_certificate(verifier);
        return -1;
    }
    return 0;
}
/* The mbedTLS provider has no delegated-trust route. Both symbols are DEFINED
 * so a consumer asks at runtime instead of compiling provider-conditionally,
 * and the setter fails deterministically rather than accepting a callback it
 * would ignore. Explicit-ca_file verification above is unchanged. */
bool moq_picoquic_supports_cert_verify_callback(void)
{
    return false;
}

moq_result_t moq_picoquic_set_cert_verify_callback(picoquic_quic_t *quic,
                                          moq_picoquic_cert_verify_fn verify,
                                          void *ctx)
{
    (void)ctx;
    if (quic == NULL || verify == NULL) return MOQ_ERR_INVAL;
    return MOQ_ERR_UNSUPPORTED;
}

#if defined(MOQ_PICOQUIC_VERIFY_TESTING)
int moq_picoquic_test_delegated_trust_result(int ret)
{
    return ret;
}
int moq_picoquic_test_reference_identity(const char *server_name,
                                         const uint8_t *leaf_der,
                                         size_t leaf_len)
{
    (void)server_name; (void)leaf_der; (void)leaf_len;
    return -1;
}
#endif

#else
#error "Define MOQ_PICOQUIC_VERIFY_OPENSSL or MOQ_PICOQUIC_VERIFY_MBEDTLS"
#endif
