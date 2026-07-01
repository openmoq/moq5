/*
 * moq_picoquic_set_cert_verifier — production TLS verification for
 * picoquic clients. See <moq/picoquic_verify.h>.
 *
 * Wraps picotls' OpenSSL verifier (system or PEM trust store, with
 * hostname/SNI checking) and installs it via
 * picoquic_set_verify_certificate_callback. picoquic owns the verifier
 * lifetime: the dispose fn is invoked when the QUIC context is freed.
 */

#include <moq/picoquic_verify.h>

#include <picoquic.h>
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

    picoquic_set_verify_certificate_callback(quic, &verifier->super,
                                             moq_picoquic_dispose_verifier);
    return 0;
}
