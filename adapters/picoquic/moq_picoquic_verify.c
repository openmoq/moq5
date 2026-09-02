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

    picoquic_set_verify_certificate_callback(quic, &verifier->super,
                                             moq_picoquic_dispose_verifier);
    return 0;
}

/* -- Platform-delegated chain decision ------------------------------- *
 * The application rules on the chain; picotls still verifies
 * CertificateVerify with the leaf's public key, which is why this needs the
 * OpenSSL backend. picotls computes its own verdict first and override_callback
 * replaces it. */

typedef struct {
    ptls_openssl_override_verify_certificate_t super; /* first: aliased below */
    const moq_cert_verifier_t *app;
} moq_override_t;

typedef struct {
    ptls_openssl_verify_certificate_t super;          /* first: aliased by dispose */
    moq_override_t override;
} moq_platform_verifier_t;

static int moq_status_to_alert(moq_cert_status_t st)
{
    switch (st) {
    case MOQ_CERT_OK:                return 0;
    case MOQ_CERT_EXPIRED:           return PTLS_ALERT_CERTIFICATE_EXPIRED;
    case MOQ_CERT_UNKNOWN_ISSUER:    return PTLS_ALERT_UNKNOWN_CA;
    case MOQ_CERT_REVOKED:           return PTLS_ALERT_CERTIFICATE_REVOKED;
    case MOQ_CERT_BAD_ENCODING:      return PTLS_ALERT_BAD_CERTIFICATE;
    case MOQ_CERT_INVALID_EXTENSION: return PTLS_ALERT_UNSUPPORTED_CERTIFICATE;
    default:                         return PTLS_ALERT_CERTIFICATE_UNKNOWN;
    }
}

static int moq_override_cb(ptls_openssl_override_verify_certificate_t *self,
                           ptls_t *tls, int ret, int ossl_ret, X509 *cert,
                           STACK_OF(X509) * chain)
{
    moq_override_t *o = (moq_override_t *)self;
    (void)ret; (void)ossl_ret;   /* picotls' verdict is replaced, not consulted */

    if (cert == NULL) return PTLS_ALERT_CERTIFICATE_REQUIRED;

    int n_interm = chain != NULL ? sk_X509_num(chain) : 0;
    if (n_interm < 0) n_interm = 0;
    size_t n = (size_t)n_interm + 1;

    /* Back to DER: the hook exposes only the parsed form. */
    moq_bytes_t *der = calloc(n, sizeof(*der));
    unsigned char **buf = calloc(n, sizeof(*buf));
    if (der == NULL || buf == NULL) { free(der); free(buf); return PTLS_ERROR_NO_MEMORY; }

    int rc = 0;
    for (size_t i = 0; i < n; ++i) {
        X509 *x = (i == 0) ? cert : sk_X509_value(chain, (int)(i - 1));
        int len = i2d_X509(x, &buf[i]);
        if (len <= 0) { rc = PTLS_ALERT_BAD_CERTIFICATE; break; }
        der[i].data = buf[i];
        der[i].len  = (size_t)len;
    }
    if (rc == 0)
        rc = moq_status_to_alert(o->app->verify_chain(o->app->user_data,
                                                      ptls_get_server_name(tls),
                                                      der, n));

    for (size_t i = 0; i < n; ++i) OPENSSL_free(buf[i]);
    free(buf); free(der);
    return rc;
}

static void moq_platform_dispose(ptls_verify_certificate_t *verifier)
{
    if (!verifier) return;
    ptls_openssl_dispose_verify_certificate(
        (ptls_openssl_verify_certificate_t *)verifier);
    free(verifier);
}

int moq_picoquic_set_platform_cert_verifier(picoquic_quic_t *quic,
                                            const moq_cert_verifier_t *verifier)
{
    if (quic == NULL || verifier == NULL || verifier->verify_chain == NULL)
        return -1;

    moq_platform_verifier_t *v = calloc(1, sizeof(*v));
    if (v == NULL) return -1;
    /* NULL store: the verdict is overridden anyway, but init needs a context. */
    if (ptls_openssl_init_verify_certificate(&v->super, NULL) != 0) {
        free(v);
        return -1;
    }
    v->override.super.cb = moq_override_cb;
    v->override.app      = verifier;
    v->super.override_callback = &v->override.super;

    picoquic_set_verify_certificate_callback(quic, &v->super.super,
                                             moq_platform_dispose);
    return 0;
}
