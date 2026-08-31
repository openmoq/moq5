#ifndef MOQ_TEST_PICOQUIC_H
#define MOQ_TEST_PICOQUIC_H

#include <ptls_mbedtls.h>

typedef struct st_picoquic_quic_t picoquic_quic_t;
typedef void (*picoquic_free_verify_certificate_ctx)(
    ptls_verify_certificate_t *verifier);

#if defined(MOQ_TEST_PICOQUIC_MODERN_API)
#define PICOQUIC_ERROR_TLS_CONFIG_FROZEN (-123)
int picoquic_set_verify_certificate_callback_ex(
    picoquic_quic_t *quic,
    ptls_verify_certificate_t *verifier,
    picoquic_free_verify_certificate_ctx dispose);
#endif

void picoquic_set_verify_certificate_callback(
    picoquic_quic_t *quic,
    ptls_verify_certificate_t *verifier,
    picoquic_free_verify_certificate_ctx dispose);

#endif
