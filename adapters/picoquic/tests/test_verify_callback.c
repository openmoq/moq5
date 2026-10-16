/* Application certificate trust callback contract for the OpenSSL provider.
 * The product verifier source is compiled directly against the tiny picoquic
 * setter recorder in support/verify_callback; picotls/OpenSSL remain real.
 *
 * Three groups of rows:
 *   identity.*   the library-owned reference-identity check, driven through
 *                its white-box seam with certificates minted here: DNS exact
 *                match and mismatch, a full-label wildcard, a partial-label
 *                wildcard (refused, as the provider's own verifier refuses
 *                it), iPAddress SANs against IP literals, the subject-CN
 *                fallback of a SAN-less leaf, absent/empty names, and the
 *                leaf length/decoding boundaries;
 *   override.*   the enumerated chain-policy override: only chain-policy
 *                alerts become the delegate's answer, operational failures
 *                and anything unlisted are preserved;
 *   the installed verifier, end to end: TRUST keeps the CertificateVerify
 *                function and leaf key and a bad signature is still refused;
 *                REJECT and unknown verdicts refuse; an identity mismatch and
 *                an absent name refuse WITHOUT consulting the application;
 *                empty, oversized and malformed chains refuse. */
#include <moq/picoquic_verify.h>

#include <picoquic.h>
#include <picotls.h>
#include <picotls/openssl.h>

/* Only the test target renames disposal. Call the real provider below. */
#undef ptls_openssl_dispose_verify_certificate
void ptls_openssl_dispose_verify_certificate(ptls_openssl_verify_certificate_t *self);

#include <openssl/evp.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures;

/* Target-private renames affect the product translation unit only in this
 * executable. These wrappers still delegate successful operations to the
 * real allocator/provider; no test hooks enter the shipped library. */
#undef calloc
#undef free
#undef X509_STORE_new
#undef X509_STORE_free
#undef ptls_openssl_init_verify_certificate
void *calloc(size_t count, size_t size);
void free(void *ptr);
X509_STORE *X509_STORE_new(void);
void X509_STORE_free(X509_STORE *store);
int ptls_openssl_init_verify_certificate(ptls_openssl_verify_certificate_t *self,
                                         X509_STORE *store);

static bool fail_alloc;
static bool fail_store;
static int init_result;
static unsigned alloc_calls;
static unsigned live_allocations;
static unsigned store_calls;
static unsigned store_releases;

void *moq_test_verify_calloc(size_t count, size_t size)
{
    alloc_calls++;
    if (fail_alloc) return NULL;
    void *ptr = calloc(count, size);
    if (ptr != NULL) live_allocations++;
    return ptr;
}

void moq_test_verify_free(void *ptr)
{
    if (ptr != NULL) live_allocations--;
    free(ptr);
}

X509_STORE *moq_test_verify_store_new(void)
{
    store_calls++;
    return fail_store ? NULL : X509_STORE_new();
}

void moq_test_verify_store_free(X509_STORE *store)
{
    if (store != NULL) store_releases++;
    X509_STORE_free(store);
}

int moq_test_openssl_init_verify_certificate(ptls_openssl_verify_certificate_t *self,
                                             X509_STORE *store)
{
    if (init_result != 0) return init_result;
    return ptls_openssl_init_verify_certificate(self, store);
}

_Static_assert(_Generic(&moq_picoquic_supports_cert_verify_callback,
                        bool (*)(void): 1, default: 0), "capability returns bool");
_Static_assert(_Generic(&moq_picoquic_set_cert_verify_callback,
                        moq_result_t (*)(picoquic_quic_t *,
                                        moq_picoquic_cert_verify_fn, void *): 1,
                        default: 0), "setter returns moq_result_t");

int moq_picoquic_test_delegated_trust_result(int ret);
int moq_picoquic_test_reference_identity(const char *server_name,
                                         const uint8_t *leaf_der,
                                         size_t leaf_len);

#define CHECK(cond, name)                                                      \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "FAIL[%s]\n", (name));                           \
            failures++;                                                       \
        }                                                                      \
    } while (0)

static unsigned setter_calls;
static unsigned dispose_calls;
static uintptr_t disposed_verifiers[4];

void moq_test_openssl_dispose_verify_certificate(ptls_openssl_verify_certificate_t *self)
{
    CHECK(dispose_calls < sizeof(disposed_verifiers) / sizeof(disposed_verifiers[0]),
          "dispose.recorder_capacity");
    if (dispose_calls < sizeof(disposed_verifiers) / sizeof(disposed_verifiers[0]))
        disposed_verifiers[dispose_calls] = (uintptr_t)self;
    dispose_calls++;
    ptls_openssl_dispose_verify_certificate(self);
}

#if defined(MOQ_TEST_PICOQUIC_MODERN_API)
static int reject_install;
static uintptr_t rejected_candidate;

int moq_test_set_verify_certificate_callback_ex(
    picoquic_quic_t *quic,
    ptls_verify_certificate_t *verifier,
    picoquic_free_verify_certificate_ctx dispose)
{
    if (quic == NULL || verifier == NULL) return -1;
    setter_calls++;
    if (reject_install) {
        rejected_candidate = (uintptr_t)verifier;
        return reject_install;
    }
    quic->verifier = verifier;
    quic->dispose = dispose;
    return 0;
}
#else
void moq_test_set_verify_certificate_callback(
    picoquic_quic_t *quic,
    ptls_verify_certificate_t *verifier,
    picoquic_free_verify_certificate_ctx dispose)
{
    setter_calls++;
    quic->verifier = verifier;
    quic->dispose = dispose;
}
#endif

static void test_random_bytes(void *buf, size_t len)
{
    memset(buf, 0xa5, len);
}

/* One self-signed leaf: subject CN `cn`, and the subjectAltName extension
 * `san` when non-NULL (OpenSSL's "DNS:..., IP:..." spelling). */
static EVP_PKEY *shared_key;

static int make_leaf_der(const char *cn, const char *san,
                         uint8_t **out, size_t *out_len)
{
    int rc = -1;
    X509 *cert = NULL;
    unsigned char *der = NULL;

    *out = NULL;
    *out_len = 0;
    if (shared_key == NULL) {
        EVP_PKEY_CTX *key_ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
        if (key_ctx == NULL || EVP_PKEY_keygen_init(key_ctx) <= 0 ||
            EVP_PKEY_CTX_set_rsa_keygen_bits(key_ctx, 2048) <= 0 ||
            EVP_PKEY_keygen(key_ctx, &shared_key) <= 0) {
            EVP_PKEY_CTX_free(key_ctx);
            return -1;
        }
        EVP_PKEY_CTX_free(key_ctx);
    }

    cert = X509_new();
    if (cert == NULL || X509_set_version(cert, 2) != 1 ||
        ASN1_INTEGER_set(X509_get_serialNumber(cert), 1) != 1 ||
        X509_gmtime_adj(X509_get_notBefore(cert), 0) == NULL ||
        X509_gmtime_adj(X509_get_notAfter(cert), 3600) == NULL ||
        X509_set_pubkey(cert, shared_key) != 1)
        goto done;

    X509_NAME *name = X509_get_subject_name(cert);
    if (name == NULL ||
        X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                                   (const unsigned char *)cn, -1, -1, 0) != 1 ||
        X509_set_issuer_name(cert, name) != 1)
        goto done;
    if (san != NULL) {
        X509_EXTENSION *ext =
            X509V3_EXT_conf_nid(NULL, NULL, NID_subject_alt_name, san);
        if (ext == NULL || X509_add_ext(cert, ext, -1) != 1) {
            X509_EXTENSION_free(ext);
            goto done;
        }
        X509_EXTENSION_free(ext);
    }
    if (X509_sign(cert, shared_key, EVP_sha256()) <= 0) goto done;

    int der_len = i2d_X509(cert, &der);
    if (der_len <= 0) goto done;
    *out = (uint8_t *)der;
    *out_len = (size_t)der_len;
    der = NULL;
    rc = 0;

done:
    OPENSSL_free(der);
    X509_free(cert);
    return rc;
}

typedef struct leaf {
    const char *label;
    uint8_t    *der;
    size_t      len;
} leaf_t;

static int mint(leaf_t *l, const char *label, const char *cn, const char *san)
{
    l->label = label;
    return make_leaf_der(cn, san, &l->der, &l->len);
}

static void identity_rows(const leaf_t *dns, const leaf_t *wild,
                          const leaf_t *partial, const leaf_t *ip,
                          const leaf_t *cn_only)
{
    /* DNS: exact, case-insensitive, and a mismatch. */
    CHECK(moq_picoquic_test_reference_identity("relay.example", dns->der, dns->len) == 1,
          "identity.dns.exact");
    CHECK(moq_picoquic_test_reference_identity("RELAY.EXAMPLE", dns->der, dns->len) == 1,
          "identity.dns.case_insensitive");
    CHECK(moq_picoquic_test_reference_identity("other.example", dns->der, dns->len) == 0,
          "identity.dns.mismatch");
    CHECK(moq_picoquic_test_reference_identity("relay.example.evil", dns->der, dns->len) == 0,
          "identity.dns.suffix_mismatch");

    /* A full-label wildcard matches exactly one label; a partial-label
     * wildcard is refused, as the provider's own verifier refuses it. */
    CHECK(moq_picoquic_test_reference_identity("host.wild.example", wild->der, wild->len) == 1,
          "identity.wildcard.full_label");
    CHECK(moq_picoquic_test_reference_identity("a.b.wild.example", wild->der, wild->len) == 0,
          "identity.wildcard.one_label_only");
    CHECK(moq_picoquic_test_reference_identity("wild.example", wild->der, wild->len) == 0,
          "identity.wildcard.needs_a_label");
    CHECK(moq_picoquic_test_reference_identity("relay.example", partial->der, partial->len) == 0,
          "identity.wildcard.partial_label_refused");

    /* IP literals against iPAddress SANs, never against DNS names, and a DNS
     * name never against an IP-only leaf. */
    CHECK(moq_picoquic_test_reference_identity("127.0.0.1", ip->der, ip->len) == 1,
          "identity.ip.exact");
    CHECK(moq_picoquic_test_reference_identity("127.0.0.2", ip->der, ip->len) == 0,
          "identity.ip.mismatch");
    CHECK(moq_picoquic_test_reference_identity("127.0.0.1", dns->der, dns->len) == 0,
          "identity.ip.against_dns_leaf");
    CHECK(moq_picoquic_test_reference_identity("relay.example", ip->der, ip->len) == 0,
          "identity.dns.against_ip_leaf");

    /* A SAN-less leaf falls back to its subject CN, as the provider does. */
    CHECK(moq_picoquic_test_reference_identity("relay.example", cn_only->der, cn_only->len) == 1,
          "identity.cn.fallback");
    CHECK(moq_picoquic_test_reference_identity("other.example", cn_only->der, cn_only->len) == 0,
          "identity.cn.mismatch");

    /* No identity is ever accepted for a client. */
    CHECK(moq_picoquic_test_reference_identity(NULL, dns->der, dns->len) == 0,
          "identity.name.absent");
    CHECK(moq_picoquic_test_reference_identity("", dns->der, dns->len) == 0,
          "identity.name.empty");

    /* Leaf boundaries: nothing, a truncated encoding, and a length the DER
     * decoder cannot take are all refusals, never reads. */
    CHECK(moq_picoquic_test_reference_identity("relay.example", NULL, 0) == 0,
          "identity.leaf.absent");
    CHECK(moq_picoquic_test_reference_identity("relay.example", dns->der, 0) == 0,
          "identity.leaf.zero_length");
    CHECK(moq_picoquic_test_reference_identity("relay.example", dns->der, dns->len / 2) == 0,
          "identity.leaf.truncated");
    CHECK(moq_picoquic_test_reference_identity("relay.example", dns->der,
                                               (size_t)LONG_MAX + 1u) == 0,
          "identity.leaf.length_over_long_max");
}

static void override_rows(void)
{
    CHECK(moq_picoquic_test_delegated_trust_result(0) == 0,
          "override.existing_success");
    CHECK(moq_picoquic_test_delegated_trust_result(PTLS_ALERT_UNKNOWN_CA) == 0,
          "override.unknown_ca_delegated");
    CHECK(moq_picoquic_test_delegated_trust_result(PTLS_ALERT_BAD_CERTIFICATE) == 0,
          "override.bad_certificate_delegated");
    CHECK(moq_picoquic_test_delegated_trust_result(PTLS_ALERT_CERTIFICATE_EXPIRED) == 0,
          "override.expired_delegated");
    CHECK(moq_picoquic_test_delegated_trust_result(PTLS_ALERT_CERTIFICATE_REVOKED) == 0,
          "override.revoked_delegated");
    CHECK(moq_picoquic_test_delegated_trust_result(PTLS_ALERT_CERTIFICATE_UNKNOWN) == 0,
          "override.certificate_unknown_delegated");
    CHECK(moq_picoquic_test_delegated_trust_result(PTLS_ALERT_CERTIFICATE_REQUIRED) == 0,
          "override.certificate_required_delegated");
    CHECK(moq_picoquic_test_delegated_trust_result(PTLS_ERROR_NO_MEMORY) == PTLS_ERROR_NO_MEMORY,
          "override.no_memory_preserved");
    CHECK(moq_picoquic_test_delegated_trust_result(PTLS_ERROR_LIBRARY) == PTLS_ERROR_LIBRARY,
          "override.library_failure_preserved");
    CHECK(moq_picoquic_test_delegated_trust_result(PTLS_ALERT_DECODE_ERROR) == PTLS_ALERT_DECODE_ERROR,
          "override.decode_error_preserved");
    CHECK(moq_picoquic_test_delegated_trust_result(12345) == 12345,
          "override.unlisted_preserved");
}

typedef struct callback_probe {
    unsigned calls;
    moq_picoquic_cert_verdict_t verdict;
    const char *want_name;
    const uint8_t *want_der;
    size_t want_der_len;
} callback_probe_t;

static moq_picoquic_cert_verdict_t verify_chain(
    void *ctx, moq_bytes_t server_name,
    const moq_bytes_t *certificates, size_t certificate_count)
{
    callback_probe_t *p = (callback_probe_t *)ctx;
    p->calls++;
    CHECK(server_name.len == strlen(p->want_name), "callback.server_name_len");
    CHECK(server_name.data != NULL &&
              memcmp(server_name.data, p->want_name, strlen(p->want_name)) == 0,
          "callback.server_name_bytes");
    CHECK(certificate_count == 1, "callback.chain_count");
    CHECK(certificates != NULL, "callback.chain_nonnull");
    if (certificates != NULL && certificate_count == 1) {
        CHECK(certificates[0].len == p->want_der_len, "callback.leaf_len");
        CHECK(certificates[0].data != NULL &&
                  memcmp(certificates[0].data, p->want_der, p->want_der_len) == 0,
              "callback.leaf_bytes");
    }
    return p->verdict;
}

int main(void)
{
    leaf_t dns = {0}, wild = {0}, partial = {0}, ip = {0}, cn_only = {0};
    CHECK(mint(&dns, "dns", "relay.example", "DNS:relay.example") == 0, "fixture.dns");
    CHECK(mint(&wild, "wild", "wild.example", "DNS:*.wild.example") == 0, "fixture.wildcard");
    CHECK(mint(&partial, "partial", "relay.example", "DNS:re*.example") == 0, "fixture.partial_wildcard");
    CHECK(mint(&ip, "ip", "ip.example", "IP:127.0.0.1") == 0, "fixture.ip");
    CHECK(mint(&cn_only, "cn", "relay.example", NULL) == 0, "fixture.cn_only");
    if (dns.der == NULL || wild.der == NULL || partial.der == NULL ||
        ip.der == NULL || cn_only.der == NULL)
        return 1;

    identity_rows(&dns, &wild, &partial, &ip, &cn_only);
    override_rows();

    CHECK(moq_picoquic_supports_cert_verify_callback(), "supported.openssl");
    CHECK(moq_picoquic_set_cert_verify_callback(NULL, verify_chain, NULL) == MOQ_ERR_INVAL,
          "install.null_quic");
    picoquic_quic_t quic = {0};
    CHECK(moq_picoquic_set_cert_verify_callback(&quic, NULL, NULL) == MOQ_ERR_INVAL,
          "install.null_callback");
    CHECK(alloc_calls == 0 && setter_calls == 0, "install.invalid_no_side_effects");

    callback_probe_t probe = {0};
    probe.verdict = MOQ_PICOQUIC_CERT_TRUST;
    probe.want_name = "relay.example";
    probe.want_der = dns.der;
    probe.want_der_len = dns.len;
    CHECK(moq_picoquic_set_cert_verify_callback(&quic, verify_chain, &probe) == MOQ_OK,
          "install.trust_callback");
    CHECK(setter_calls == 1, "install.setter_once");
    CHECK(quic.verifier != NULL, "install.verifier_present");
    CHECK(quic.dispose != NULL, "install.disposer_present");
    CHECK(dispose_calls == 0, "install.live_verifier_not_disposed");

    ptls_verify_certificate_t *old_verifier = quic.verifier;
    picoquic_free_verify_certificate_ctx old_dispose = quic.dispose;
    unsigned stores_before = store_calls;
    unsigned releases_before = store_releases;
    fail_alloc = true;
    CHECK(moq_picoquic_set_cert_verify_callback(&quic, verify_chain, NULL) == MOQ_ERR_NOMEM,
          "install.allocation_failure");
    fail_alloc = false;
    CHECK(store_calls == stores_before, "install.allocation_no_store");
    CHECK(live_allocations == 1, "install.allocation_keeps_old_owner");
    fail_store = true;
    CHECK(moq_picoquic_set_cert_verify_callback(&quic, verify_chain, NULL) == MOQ_ERR_NOMEM,
          "install.store_allocation_failure");
    fail_store = false;
    CHECK(live_allocations == 1, "install.store_failure_frees_candidate");
    CHECK(store_releases == releases_before, "install.no_absent_store_release");
    const int init_errors[] = {PTLS_ERROR_NO_MEMORY, PTLS_ERROR_LIBRARY, -1, 12345};
    for (size_t i = 0; i < sizeof(init_errors) / sizeof(init_errors[0]); i++) {
        init_result = init_errors[i];
        CHECK(moq_picoquic_set_cert_verify_callback(&quic, verify_chain, NULL) ==
                  (init_result == PTLS_ERROR_NO_MEMORY ? MOQ_ERR_NOMEM : MOQ_ERR_INTERNAL),
              "install.init_exact_error");
        CHECK(live_allocations == 1, "install.init_failure_frees_candidate");
        CHECK(store_releases == releases_before + i + 1, "install.init_releases_store");
        CHECK(quic.verifier == old_verifier && quic.dispose == old_dispose,
              "install.init_preserves_old_owner");
    }
    init_result = 0;
    CHECK(setter_calls == 1 && dispose_calls == 0, "install.setup_failures_no_transfer");
    CHECK(probe.calls == 0, "install.setup_never_calls_delegate");

#if defined(MOQ_TEST_PICOQUIC_MODERN_API)
    callback_probe_t refused_probe = probe;
    refused_probe.verdict = MOQ_PICOQUIC_CERT_REJECT;
    reject_install = PICOQUIC_ERROR_TLS_CONFIG_FROZEN;
    CHECK(moq_picoquic_set_cert_verify_callback(&quic, verify_chain, &refused_probe) == MOQ_ERR_WRONG_STATE,
          "install.rejection_reported");
    reject_install = 0;
    CHECK(setter_calls == 2, "install.rejected_setter_once");
    CHECK(rejected_candidate != 0 && rejected_candidate != (uintptr_t)old_verifier,
          "install.distinct_candidate");
    CHECK(quic.verifier == old_verifier, "install.rejection_preserves_verifier");
    CHECK(quic.dispose == old_dispose, "install.rejection_preserves_disposer");
    CHECK(dispose_calls == 1, "install.rejected_candidate_disposed_once");
    CHECK(refused_probe.calls == 0, "install.rejected_callback_not_called");
    reject_install = PICOQUIC_ERROR_MEMORY;
    CHECK(moq_picoquic_set_cert_verify_callback(&quic, verify_chain, &refused_probe) == MOQ_ERR_NOMEM,
          "install.provider_allocation_failure");
    CHECK(setter_calls == 3 && dispose_calls == 2, "install.provider_oom_candidate_disposed_once");
    CHECK(live_allocations == 1 && quic.verifier == old_verifier && quic.dispose == old_dispose,
          "install.provider_oom_preserves_owner");
    reject_install = 12345;
    CHECK(moq_picoquic_set_cert_verify_callback(&quic, verify_chain, &refused_probe) == MOQ_ERR_INTERNAL,
          "install.unknown_rejection_is_internal");
    reject_install = 0;
    CHECK(setter_calls == 4 && dispose_calls == 3, "install.unknown_candidate_disposed_once");
    CHECK(live_allocations == 1, "install.refusals_keep_only_old_allocation");
    CHECK(quic.verifier == old_verifier && quic.dispose == old_dispose,
          "install.unknown_rejection_preserves_owner");
    /* All following handshake/signature rows use the still-installed OLD
     * verifier and its original probe, not the refused replacement. */
#endif

    ptls_context_t tls_ctx = {0};
    tls_ctx.random_bytes = test_random_bytes;
    tls_ctx.get_time = &ptls_get_time;
    ptls_t *tls = ptls_new(&tls_ctx, 0);
    CHECK(tls != NULL, "fixture.client_tls");
    if (tls != NULL && quic.verifier != NULL) {
        ptls_iovec_t cert = ptls_iovec_init(dns.der, dns.len);
        int (*verify_sign)(void *, uint16_t, ptls_iovec_t, ptls_iovec_t) = NULL;
        void *verify_data = NULL;

        /* TRUST: the delegate answered, the provider still handed back the
         * CertificateVerify machinery and the leaf key, and a bad signature is
         * still refused by it. */
        int rc = quic.verifier->cb(quic.verifier, tls, "relay.example",
                                   &verify_sign, &verify_data, &cert, 1);
        CHECK(rc == 0, "trust.overrides_untrusted_chain");
        CHECK(probe.calls == 1, "trust.callback_once");
        CHECK(verify_sign != NULL, "trust.certificate_verify_function");
        CHECK(verify_data != NULL, "trust.leaf_public_key");
        if (verify_sign != NULL && verify_data != NULL) {
            static const uint8_t transcript[] = {0x01};
            static const uint8_t bad_signature[] = {0x00};
            int verify_rc = verify_sign(
                verify_data, PTLS_SIGNATURE_RSA_PSS_RSAE_SHA256,
                ptls_iovec_init(transcript, sizeof(transcript)),
                ptls_iovec_init(bad_signature, sizeof(bad_signature)));
            CHECK(verify_rc == PTLS_ALERT_DECRYPT_ERROR,
                  "trust.invalid_signature_rejected");
            /* That call consumed the retained context: picotls' verify_sign
             * releases the leaf key on every invocation, so nothing is left
             * to free here. */
        }

        /* REJECT and an unknown verdict refuse, and hand nothing back. */
        probe.verdict = MOQ_PICOQUIC_CERT_REJECT;
        verify_sign = NULL;
        verify_data = NULL;
        rc = quic.verifier->cb(quic.verifier, tls, "relay.example",
                               &verify_sign, &verify_data, &cert, 1);
        CHECK(rc != 0, "reject.fails_handshake");
        CHECK(probe.calls == 2, "reject.callback_once");
        CHECK(verify_sign == NULL, "reject.no_verify_function");
        CHECK(verify_data == NULL, "reject.no_verify_data");

        probe.verdict = (moq_picoquic_cert_verdict_t)99;
        rc = quic.verifier->cb(quic.verifier, tls, "relay.example",
                               &verify_sign, &verify_data, &cert, 1);
        CHECK(rc != 0, "invalid_verdict.rejects");
        CHECK(probe.calls == 3, "invalid_verdict.callback_once");

        /* The library-owned identity check runs BEFORE the application: a
         * leaf that does not name the server, or no name at all, is refused
         * without a callback. */
        probe.verdict = MOQ_PICOQUIC_CERT_TRUST;
        rc = quic.verifier->cb(quic.verifier, tls, "other.example",
                               &verify_sign, &verify_data, &cert, 1);
        CHECK(rc == PTLS_ALERT_BAD_CERTIFICATE, "identity.mismatch.rejects");
        CHECK(probe.calls == 3, "identity.mismatch.callback_not_called");
        CHECK(verify_sign == NULL && verify_data == NULL,
              "identity.mismatch.nothing_handed_back");
        rc = quic.verifier->cb(quic.verifier, tls, NULL,
                               &verify_sign, &verify_data, &cert, 1);
        CHECK(rc == PTLS_ALERT_BAD_CERTIFICATE, "identity.absent_name.rejects");
        CHECK(probe.calls == 3, "identity.absent_name.callback_not_called");
        rc = quic.verifier->cb(quic.verifier, tls, "",
                               &verify_sign, &verify_data, &cert, 1);
        CHECK(rc == PTLS_ALERT_BAD_CERTIFICATE, "identity.empty_name.rejects");
        CHECK(probe.calls == 3, "identity.empty_name.callback_not_called");

        /* An IP literal is a name too: the iPAddress leaf reaches the
         * application with exactly that name. */
        ptls_iovec_t ip_cert = ptls_iovec_init(ip.der, ip.len);
        probe.want_name = "127.0.0.1";
        probe.want_der = ip.der;
        probe.want_der_len = ip.len;
        rc = quic.verifier->cb(quic.verifier, tls, "127.0.0.1",
                               &verify_sign, &verify_data, &ip_cert, 1);
        CHECK(rc == 0, "identity.ip_literal.trusts");
        CHECK(probe.calls == 4, "identity.ip_literal.callback_once");
        CHECK(verify_sign != NULL && verify_data != NULL,
              "identity.ip_literal.certificate_verify_retained");
        /* This case never invokes CertificateVerify, so release the retained
         * context the way picotls does when a handshake ends before it: empty
         * data and signature free the leaf key. */
        if (verify_sign != NULL && verify_data != NULL)
            (void)verify_sign(verify_data, 0, ptls_iovec_init(NULL, 0),
                              ptls_iovec_init(NULL, 0));
        probe.want_name = "relay.example";
        probe.want_der = dns.der;
        probe.want_der_len = dns.len;

        /* Chains the application may never bless: none, too many, malformed. */
        verify_sign = NULL;
        verify_data = NULL;
        rc = quic.verifier->cb(quic.verifier, tls, "relay.example",
                               &verify_sign, &verify_data, NULL, 0);
        CHECK(rc != 0, "empty_chain.rejects");
        CHECK(probe.calls == 4, "empty_chain.callback_not_called");

        uint8_t malformed[] = {0x30, 0x03, 0x01};
        ptls_iovec_t malformed_cert = ptls_iovec_init(malformed, sizeof(malformed));
        rc = quic.verifier->cb(quic.verifier, tls, "relay.example",
                               &verify_sign, &verify_data, &malformed_cert, 1);
        CHECK(rc != 0, "malformed_der.rejects");
        CHECK(probe.calls == 4, "malformed_der.callback_not_called");
        CHECK(verify_sign == NULL, "malformed_der.no_verify_function");
        CHECK(verify_data == NULL, "malformed_der.no_verify_data");

        ptls_iovec_t oversized[17];
        for (size_t i = 0; i < sizeof(oversized) / sizeof(oversized[0]); i++)
            oversized[i] = cert;
        rc = quic.verifier->cb(quic.verifier, tls, "relay.example",
                               &verify_sign, &verify_data,
                               oversized, sizeof(oversized) / sizeof(oversized[0]));
        CHECK(rc != 0, "oversized_chain.rejects");
        CHECK(probe.calls == 4, "oversized_chain.callback_not_called");
        CHECK(verify_sign == NULL, "oversized_chain.no_verify_function");
        CHECK(verify_data == NULL, "oversized_chain.no_verify_data");
    }

    ptls_free(tls);
    if (quic.dispose != NULL) quic.dispose(quic.verifier);
#if defined(MOQ_TEST_PICOQUIC_MODERN_API)
    CHECK(dispose_calls == 4, "dispose.candidates_and_old_exactly_once");
    CHECK(disposed_verifiers[0] != disposed_verifiers[3] &&
          disposed_verifiers[1] != disposed_verifiers[3] &&
          disposed_verifiers[2] != disposed_verifiers[3],
          "dispose.candidates_and_old_are_distinct");
    CHECK(refused_probe.calls == 0, "dispose.rejected_callback_never_used");
#else
    CHECK(dispose_calls == 1, "dispose.legacy_old_exactly_once");
#endif
    CHECK(live_allocations == 0, "dispose.no_owned_allocations_remain");
    OPENSSL_free(dns.der);
    OPENSSL_free(wild.der);
    OPENSSL_free(partial.der);
    OPENSSL_free(ip.der);
    OPENSSL_free(cn_only.der);
    EVP_PKEY_free(shared_key);
    if (failures == 0) printf("picoquic_verify_callback: ok\n");
    else fprintf(stderr, "picoquic_verify_callback: %d failure(s)\n", failures);
    return failures != 0;
}
