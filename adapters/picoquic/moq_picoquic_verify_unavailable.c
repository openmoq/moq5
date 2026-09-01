/*
 * The MOQ_WITH_OPENSSL=OFF build of moq_picoquic_verify.c.
 *
 * The OpenSSL-backed verifier wraps picotls' OpenSSL X509 code. A build
 * without OpenSSL -- picotls' minicrypto backend, for instance -- has no X509
 * code at all, so there is nothing to verify a chain against. Both entry points
 * report that rather than silently accepting one, so a caller that asked for a
 * CA bundle fails instead of getting no verification.
 */

#include <moq/picoquic_verify.h>

int moq_picoquic_ca_file_loadable(const char *ca_file)
{
    (void)ca_file;
    return 0;
}

int moq_picoquic_set_cert_verifier(picoquic_quic_t *quic, const char *ca_file)
{
    (void)quic;
    (void)ca_file;
    return -1;
}
