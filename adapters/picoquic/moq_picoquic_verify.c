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
#include <string.h>

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
/* -- delegated certificate trust (OpenSSL) --------------------------------- *
 *
 * picotls' OpenSSL verifier already owns the two things a delegated verifier
 * must not reimplement: the chain-verification operation, and the
 * `verify_sign`/`verify_data` machinery that checks CertificateVerify and
 * releases the leaf key exactly once. It also exposes the exact seam this
 * needs -- `ptls_openssl_verify_certificate_t::override_callback`, which
 * picotls invokes with the chain result AFTER verification and BEFORE it
 * extracts the public key (picotls lib/openssl.c, verify_cert). Returning 0
 * there makes picotls continue into its own verify_sign path.
 *
 * The installed verifier holds ONLY immutable state: the caller's callback and
 * context, and the owned base OpenSSL verifier. Each invocation shallow-copies
 * that base verifier onto its own stack frame and points the copy at a
 * stack-local override, so no per-call pointer, verdict, or routing field ever
 * lives in the shared object. */

#include <picotls/openssl.h>

/* The chain-cardinality bound. Enforced before any stack-backed conversion, so
 * a peer-supplied count can never size or overrun a stack object. Private to
 * the implementation: a safety limit, not part of the API contract. */
#define MOQ_PICOQUIC_CERT_CHAIN_MAX 16u

typedef struct moq_pq_callback_verifier {
    ptls_verify_certificate_t         super;    /* what picoquic installs */
    ptls_openssl_verify_certificate_t openssl;  /* owned base; immutable here */
    moq_picoquic_cert_verify_fn       verify;   /* immutable */
    void                             *ctx;      /* immutable; may be NULL */
} moq_pq_callback_verifier_t;

/* The chain-verification results a certificate POLICY may legitimately
 * override, enumerated from the pinned picotls source rather than inferred
 * from sign or numeric range: every value `verify_cert_chain()`'s switch can
 * produce, plus the two `verify_cert()` produces itself for an unusable or
 * absent chain (lib/openssl.c).
 *
 * PTLS_ERROR_NO_MEMORY and PTLS_ERROR_LIBRARY are deliberately absent, and so
 * is every value not listed: an application replacing POLICY has not replaced
 * picotls' ability to perform the verification, and an unrecognised result is
 * not something a policy answered. Both fail closed. */
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

/* Applies an already-taken TRUST decision to picotls' chain result. Reached
 * only after the delegate answered TRUST, so it carries no per-call state and
 * needs none: a policy refusal becomes success, an operational failure and any
 * unrecognised result are preserved exactly. */
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

    /* Caller outputs are cleared before any work, so every refusing path below
     * leaves them clear without having to remember to. */
    if (verify_sign != NULL) *verify_sign = NULL;
    if (verify_data != NULL) *verify_data = NULL;

    /* A policy cannot bless an absent chain, an incoherent (NULL, n>0) pair is
     * refused rather than dereferenced, and the bound is enforced BEFORE the
     * fixed stack array is populated -- its size never derives from an
     * untrusted count. None of these consult the application. */
    if (certs == NULL || num_certs == 0 ||
        num_certs > MOQ_PICOQUIC_CERT_CHAIN_MAX)
        return PTLS_ALERT_BAD_CERTIFICATE;

    /* Borrowed for this call only, leaf-first, in picotls' own order. */
    moq_bytes_t chain[MOQ_PICOQUIC_CERT_CHAIN_MAX];
    for (size_t i = 0; i < num_certs; i++) {
        chain[i].data = certs[i].base;
        chain[i].len  = certs[i].len;
    }
    moq_bytes_t name = {
        (const uint8_t *)(server_name != NULL ? server_name : ""),
        (server_name != NULL) ? strlen(server_name) : 0
    };

    /* Consulted HERE, exactly once, before picotls runs. Consulting from the
     * override instead would skip the paths picotls exits early -- an unusable
     * chain never reaches the override -- and the application would silently
     * not see those certificates at all.
     *
     * ONLY the exact TRUST value proceeds: zero and every unknown enum value
     * refuse, and a delegate that refuses a chain picotls would have accepted
     * refuses it too, because the delegate IS the policy. */
    if (self->verify(self->ctx, name, chain, num_certs) !=
        MOQ_PICOQUIC_CERT_TRUST)
        return PTLS_ALERT_BAD_CERTIFICATE;

    /* Per-invocation copies: the shared verifier is read-only, and this
     * invocation's override routing lives on this stack frame. The inner
     * verifier still parses every DER certificate, extracts the leaf key and
     * installs verify_sign, so CertificateVerify is still enforced and the key
     * is still released by picotls' own machinery. */
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

int moq_picoquic_cert_verify_callback_supported(void)
{
    return 1;
}

int moq_picoquic_set_cert_verify_callback(picoquic_quic_t *quic,
                                          moq_picoquic_cert_verify_fn verify,
                                          void *ctx)
{
    /* `ctx` is opaque and MAY be NULL: a stateless callback needs no context,
     * and the installed header never required one. NULL `quic` or `verify` is
     * refused before any allocation or setter call, so a refused install
     * cannot disturb an already-installed verifier. */
    if (quic == NULL || verify == NULL) return -1;

    moq_pq_callback_verifier_t *self =
        (moq_pq_callback_verifier_t *)calloc(1, sizeof(*self));
    if (self == NULL) return -1;

    /* A PRIVATE EMPTY store, never OpenSSL's default. In delegated mode the
     * callback is the sole certificate-policy authority, and consulting host
     * roots would give it a second, environment-dependent input it never asked
     * for. The empty store still lets picotls parse the DER, perform the
     * verification operation, extract the leaf key and enforce
     * CertificateVerify; the resulting policy alert is the delegate's to
     * answer. picotls takes its own reference, so the local one is dropped. */
    X509_STORE *store = X509_STORE_new();
    if (store == NULL ||
        ptls_openssl_init_verify_certificate(&self->openssl, store) != 0) {
        if (store != NULL) X509_STORE_free(store);
        free(self);
        return -1;
    }
    X509_STORE_free(store);

    self->super.cb = moq_pq_verify_with_callback;
    self->super.algos = self->openssl.super.algos;
    self->verify = verify;
    self->ctx = ctx;

    if (moq_picoquic_install_verifier(quic, &self->super,
                                      moq_pq_dispose_callback_verifier) != 0) {
        /* Rejected: this helper disposes the candidate it built, and any
         * previously installed verifier is untouched. */
        moq_pq_dispose_callback_verifier(&self->super);
        return -1;
    }
    return 0;
}

#if defined(MOQ_PICOQUIC_VERIFY_TESTING)
/* The result the override produces once the delegate has answered TRUST. */
int moq_picoquic_test_delegated_trust_result(int ret)
{
    return moq_pq_override_trust(NULL, NULL, ret, 0, NULL, NULL);
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
/* The mbedTLS provider has no delegated-trust route. Both symbols are still
 * DEFINED here, so a consumer asks at runtime instead of compiling
 * provider-conditionally, and the setter fails deterministically rather than
 * accepting a callback it would ignore. Explicit-ca_file verification above is
 * unchanged. */
int moq_picoquic_cert_verify_callback_supported(void)
{
    return 0;
}

int moq_picoquic_set_cert_verify_callback(picoquic_quic_t *quic,
                                          moq_picoquic_cert_verify_fn verify,
                                          void *ctx)
{
    (void)quic; (void)verify; (void)ctx;
    return -1;
}

#if defined(MOQ_PICOQUIC_VERIFY_TESTING)
/* The classifier's operational-failure rule is provider-independent: an
 * allocation or library failure is never converted into an accepted
 * certificate. The current body preserves every result. */
int moq_picoquic_test_delegated_trust_result(int ret)
{
    return ret;
}
#endif

#else
#error "Define MOQ_PICOQUIC_VERIFY_OPENSSL or MOQ_PICOQUIC_VERIFY_MBEDTLS"
#endif
