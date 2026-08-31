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
#else
#error "Define MOQ_PICOQUIC_VERIFY_OPENSSL or MOQ_PICOQUIC_VERIFY_MBEDTLS"
#endif
