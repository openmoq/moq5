#ifndef MOQ_TEST_PTLS_MBEDTLS_H
#define MOQ_TEST_PTLS_MBEDTLS_H

typedef struct st_ptls_verify_certificate_t {
    unsigned int marker;
} ptls_verify_certificate_t;

ptls_verify_certificate_t *ptls_mbedtls_get_certificate_verifier(
    const char *ca_file, unsigned int *store_loaded);
void ptls_mbedtls_dispose_verify_certificate(
    ptls_verify_certificate_t *verifier);

#endif
