#include <moq/picoquic_verify.h>

#include <picoquic.h>
#include <ptls_mbedtls.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct st_picoquic_quic_t {
    ptls_verify_certificate_t *verifier;
    picoquic_free_verify_certificate_ctx dispose;
};

static unsigned int allocated_count;
static unsigned int disposed_count;
static unsigned int legacy_setter_count;
static unsigned int extended_setter_count;
#if defined(MOQ_TEST_PICOQUIC_MODERN_API)
static int extended_result;
#endif
static int failures;

#define CHECK(cond, name)                                                     \
    do {                                                                      \
        if (!(cond)) {                                                        \
            fprintf(stderr, "FAIL[%s]: %s\n", (name), #cond);                \
            failures++;                                                       \
        }                                                                     \
    } while (0)

ptls_verify_certificate_t *ptls_mbedtls_get_certificate_verifier(
    const char *ca_file, unsigned int *store_loaded)
{
    if (strcmp(ca_file, "missing.pem") == 0) {
        *store_loaded = 0;
        return NULL;
    }

    ptls_verify_certificate_t *verifier = malloc(sizeof(*verifier));
    if (verifier == NULL) return NULL;
    verifier->marker = ++allocated_count;
    *store_loaded = strcmp(ca_file, "empty.pem") != 0;
    return verifier;
}

void ptls_mbedtls_dispose_verify_certificate(
    ptls_verify_certificate_t *verifier)
{
    if (verifier == NULL) return;
    disposed_count++;
    free(verifier);
}

static void replace_verifier(picoquic_quic_t *quic,
                             ptls_verify_certificate_t *verifier,
                             picoquic_free_verify_certificate_ctx dispose)
{
    if (quic->verifier != NULL && quic->dispose != NULL)
        quic->dispose(quic->verifier);
    quic->verifier = verifier;
    quic->dispose = dispose;
}

void picoquic_set_verify_certificate_callback(
    picoquic_quic_t *quic,
    ptls_verify_certificate_t *verifier,
    picoquic_free_verify_certificate_ctx dispose)
{
    legacy_setter_count++;
    replace_verifier(quic, verifier, dispose);
}

#if defined(MOQ_TEST_PICOQUIC_MODERN_API)
int picoquic_set_verify_certificate_callback_ex(
    picoquic_quic_t *quic,
    ptls_verify_certificate_t *verifier,
    picoquic_free_verify_certificate_ctx dispose)
{
    extended_setter_count++;
    if (extended_result != 0) return extended_result;
    replace_verifier(quic, verifier, dispose);
    return 0;
}
#endif

static void destroy_quic(picoquic_quic_t *quic)
{
    if (quic->verifier != NULL && quic->dispose != NULL)
        quic->dispose(quic->verifier);
    quic->verifier = NULL;
    quic->dispose = NULL;
}

static void test_ca_preflight(void)
{
    CHECK(moq_picoquic_ca_file_loadable(NULL) == 0, "ca.null");
    CHECK(moq_picoquic_ca_file_loadable("") == 0, "ca.empty");
    CHECK(moq_picoquic_ca_file_loadable("missing.pem") == 0, "ca.missing");

    unsigned int disposed0 = disposed_count;
    CHECK(moq_picoquic_ca_file_loadable("empty.pem") == 0,
          "ca.empty_store");
    CHECK(disposed_count == disposed0 + 1, "ca.empty_store_disposed");

    disposed0 = disposed_count;
    CHECK(moq_picoquic_ca_file_loadable("ca.pem") == 1, "ca.loaded");
    CHECK(disposed_count == disposed0 + 1, "ca.loaded_disposed");
}

static void test_install(void)
{
    picoquic_quic_t quic = {0};
    unsigned int allocated0 = allocated_count;
    unsigned int disposed0 = disposed_count;

    CHECK(moq_picoquic_set_cert_verifier(NULL, "ca.pem") == -1,
          "install.null_quic");
    CHECK(moq_picoquic_set_cert_verifier(&quic, NULL) == -1,
          "install.null_ca");
    CHECK(moq_picoquic_set_cert_verifier(&quic, "missing.pem") == -1,
          "install.missing_ca");
    CHECK(quic.verifier == NULL, "install.failure_unchanged");

    CHECK(moq_picoquic_set_cert_verifier(&quic, "ca.pem") == 0,
          "install.success");
    CHECK(allocated_count == allocated0 + 1, "install.allocated_once");
    CHECK(disposed_count == disposed0, "install.ownership_transferred");
    CHECK(quic.verifier != NULL, "install.active");

#if defined(MOQ_TEST_PICOQUIC_MODERN_API)
    CHECK(extended_setter_count == 1, "install.modern_setter");
    CHECK(legacy_setter_count == 0, "install.no_legacy_setter");

    ptls_verify_certificate_t *previous = quic.verifier;
    extended_result = PICOQUIC_ERROR_TLS_CONFIG_FROZEN;
    CHECK(moq_picoquic_set_cert_verifier(&quic, "ca.pem") == -1,
          "install.modern_rejection");
    CHECK(quic.verifier == previous, "install.modern_previous_preserved");
    CHECK(allocated_count == allocated0 + 2,
          "install.modern_replacement_built");
    CHECK(disposed_count == disposed0 + 1,
          "install.modern_rejected_disposed");
    CHECK(extended_setter_count == 2, "install.modern_rejection_observed");
#else
    CHECK(legacy_setter_count == 1, "install.legacy_setter");
    CHECK(extended_setter_count == 0, "install.no_extended_setter");
#endif

    destroy_quic(&quic);
#if defined(MOQ_TEST_PICOQUIC_MODERN_API)
    CHECK(disposed_count == disposed0 + 2, "install.modern_disposed_once");
#else
    CHECK(disposed_count == disposed0 + 1, "install.legacy_disposed_once");
#endif
}

int main(void)
{
    test_ca_preflight();
    test_install();
    if (failures != 0) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
#if defined(MOQ_TEST_PICOQUIC_MODERN_API)
    puts("picoquic verifier API modern: ok");
#else
    puts("picoquic verifier API legacy: ok");
#endif
    return 0;
}
