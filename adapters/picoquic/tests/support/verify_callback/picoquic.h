/*
 * A minimal stand-in for <picoquic.h>, included BEFORE the real one so the
 * verifier source under test is compiled against a concrete QUIC context whose
 * installed verifier and disposer are directly observable. picotls and the
 * selected crypto provider stay real; only picoquic's setter is recorded.
 */
#ifndef MOQ_TEST_VERIFY_CALLBACK_PICOQUIC_H
#define MOQ_TEST_VERIFY_CALLBACK_PICOQUIC_H

#include <picotls.h>

typedef struct st_picoquic_quic_t picoquic_quic_t;
typedef void (*picoquic_free_verify_certificate_ctx)(
    ptls_verify_certificate_t *verifier);

struct st_picoquic_quic_t {
    ptls_verify_certificate_t *verifier;
    picoquic_free_verify_certificate_ctx dispose;
};

#if defined(MOQ_TEST_PICOQUIC_MODERN_API)
#define PICOQUIC_ERROR_TLS_CONFIG_FROZEN (-205)

int picoquic_set_verify_certificate_callback_ex(
    picoquic_quic_t *quic,
    ptls_verify_certificate_t *verifier,
    picoquic_free_verify_certificate_ctx dispose);
#else
void picoquic_set_verify_certificate_callback(
    picoquic_quic_t *quic,
    ptls_verify_certificate_t *verifier,
    picoquic_free_verify_certificate_ctx dispose);
#endif

#endif
