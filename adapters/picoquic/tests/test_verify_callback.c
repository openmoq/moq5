/*
 * The delegated certificate-trust wrapper's own contract.
 *
 * The product verifier source is compiled directly against the tiny picoquic
 * setter/context recorder in tests/support/verify_callback; picotls and the
 * selected crypto provider stay REAL. That makes the installed verifier and
 * disposer directly observable, so preservation is proven by identity rather
 * than inferred from a later install succeeding.
 *
 * Each row asserts on its own named oracle, so a regression is attributed to
 * the contract it broke rather than to the suite as a whole.
 */
#include <moq/picoquic_verify.h>

#include <picoquic.h>
#include <picotls.h>

#if defined(MOQ_PICOQUIC_VERIFY_OPENSSL)
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/x509.h>
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* The target renames the verifier source's two allocator calls so the
 * candidate's lifetime can be counted. This file implements the recorders, so
 * it must see the real functions. */
#ifdef calloc
#undef calloc
#endif
#ifdef free
#undef free
#endif
/* <stdlib.h> was preprocessed with those renames in force, so its own
 * declarations were renamed too. Re-declare the real ones. */
extern void *calloc(size_t nmemb, size_t size);
extern void free(void *ptr);

static int failures;

#define CHECK(cond, name)                                                     \
    do {                                                                      \
        if (!(cond)) {                                                        \
            fprintf(stderr, "FAIL[%s]\n", (name));                            \
            failures++;                                                       \
        }                                                                     \
    } while (0)

/* Rows that cannot execute because an earlier obligation was not met still
 * report, by name, so a wrapper that regresses produces named failures rather
 * than a smaller, quieter run. */
#define ROW_ABSENT(name) CHECK(0, name " [not reached: no verifier installed]")

int moq_picoquic_test_delegated_trust_result(int ret);

/* -- the recorded picoquic setter ---------------------------------------- */

static unsigned setter_calls;

/* Every candidate the helper hands to the setter -- INCLUDING one the modern
 * setter rejects -- is recorded with the disposer offered alongside it. That
 * disposer is the product's own release path
 * (ptls_openssl_dispose_verify_certificate via moq_picoquic_verify.c), so
 * test-owned cleanup after a failed row can invoke the EXACT disposer instead
 * of the renamed raw free. A raw free would balance the allocation count while
 * leaving provider-owned state inside the candidate un-released. */
typedef struct {
    void *verifier;
    picoquic_free_verify_certificate_ctx dispose;
} cand_attempt_t;
static cand_attempt_t cand_attempts[8];
static unsigned cand_attempt_n;
/* Cleanup performed BY THE TEST after a failed row, counted separately from
 * anything the product did, so test-owned cleanup can never stand in for a
 * missing product cleanup. */
static unsigned test_dispose_calls;
static unsigned cleanup_no_disposer;

static void cand_attempt_record(void *verifier,
                                picoquic_free_verify_certificate_ctx dispose)
{
    if (cand_attempt_n < sizeof(cand_attempts) / sizeof(cand_attempts[0])) {
        cand_attempts[cand_attempt_n].verifier = verifier;
        cand_attempts[cand_attempt_n].dispose = dispose;
        cand_attempt_n++;
    }
}

static picoquic_free_verify_certificate_ctx cand_attempt_disposer(void *v)
{
    for (unsigned i = 0; i < cand_attempt_n; i++)
        if (cand_attempts[i].verifier == v) return cand_attempts[i].dispose;
    return NULL;
}
/* True only while the recorded picoquic setter is executing, so a disposal can
 * be attributed to the SETTER releasing the previous owner rather than to the
 * helper disposing something directly. */
static bool in_setter;

#if defined(MOQ_TEST_PICOQUIC_MODERN_API)
static int setter_result;   /* the modern setter may REJECT the replacement */

/* Models picoquic's documented contract (picoquic.h:1012-1026,
 * tls_api.c:1702-1716): ownership of the incoming verifier transfers ONLY on
 * success, and a successful replacement releases the previous ownership
 * record. A rejected replacement leaves the previous verifier installed and
 * undisposed, and the caller keeps ownership of what it passed in. */
int moq_test_set_verify_certificate_callback_ex(
    picoquic_quic_t *quic,
    ptls_verify_certificate_t *verifier,
    picoquic_free_verify_certificate_ctx dispose)
{
    if (quic == NULL || verifier == NULL) return -1;
    setter_calls++;
    /* Recorded BEFORE the rejection test: a rejected candidate is exactly the
     * one whose ownership stays with the caller, so its disposer is the one
     * test-owned cleanup will need. */
    cand_attempt_record(verifier, dispose);
    if (setter_result != 0) return setter_result;   /* installs nothing */
    in_setter = true;
    if (quic->verifier != NULL && quic->verifier != verifier &&
        quic->dispose != NULL)
        quic->dispose(quic->verifier);              /* release the previous */
    in_setter = false;
    quic->verifier = verifier;
    quic->dispose = dispose;
    return 0;
}
#else
/* The historical void setter: the same replacement release, with no way to
 * report a rejection. */
void moq_test_set_verify_certificate_callback(
    picoquic_quic_t *quic,
    ptls_verify_certificate_t *verifier,
    picoquic_free_verify_certificate_ctx dispose)
{
    setter_calls++;
    cand_attempt_record(verifier, dispose);
    in_setter = true;
    if (quic->verifier != NULL && quic->verifier != verifier &&
        quic->dispose != NULL)
        quic->dispose(quic->verifier);
    in_setter = false;
    quic->verifier = verifier;
    quic->dispose = dispose;
}
#endif

/* -- candidate lifetime recorder ----------------------------------------- *
 * The verifier source is compiled in this binary with `calloc`/`free` renamed
 * to these, so the CANDIDATE verifier's construction and disposal are counted
 * directly. Nothing is inferred from the setter's return value or from which
 * verifier is installed afterwards. Absent from every shipped object: the
 * renames exist only on these test targets. */
static unsigned cand_allocs;
static unsigned cand_frees;
static unsigned cand_unknown_frees;
static unsigned cand_overflows;
static void *cand_live[8];
static unsigned cand_live_n;

void *moq_test_verify_calloc(size_t n, size_t sz)
{
    void *p = calloc(n, sz);
    if (p != NULL) {
        cand_allocs++;
        if (cand_live_n < sizeof(cand_live) / sizeof(cand_live[0]))
            cand_live[cand_live_n++] = p;
        else
            cand_overflows++;   /* capacity exceeded: evidence would be lost */
    }
    return p;
}

void moq_test_verify_free(void *p)
{
    if (p != NULL) {
        int found = 0;
        cand_frees++;
        for (unsigned i = 0; i < cand_live_n; i++) {
            if (cand_live[i] == p) {
                cand_live[i] = cand_live[--cand_live_n];
                found = 1;
                break;
            }
        }
        /* a free the recorder never saw allocated is lost lifetime evidence */
        if (!found) cand_unknown_frees++;
    }
    free(p);
}

/* Both providers compile the verifier source with the renamed allocator, so a
 * "no helper-owned allocation" claim is OBSERVED rather than inferred. The
 * mbedTLS PATH verifier itself comes from picotls, so only the DELEGATED
 * helper's own allocations are claimed on that provider.
 *
 * Resetting while a candidate is still live would erase the very obligation
 * the next row is about to judge, so it is itself a named failure. */
static bool cand_reset(const char *where)
{
    if (cand_live_n != 0) {
        /* Never clear a NON-EMPTY live set: those pointers are the only handle
         * on the leak, and zeroing them destroys the ability to release what
         * was recorded. The scenario stops here instead. */
        fprintf(stderr, "FAIL[candidate.live_at_reset:%s] %u still live\n",
                where, cand_live_n);
        CHECK(cand_live_n == 0, "candidate.live_at_reset");
        return false;
    }
    CHECK(cand_unknown_frees == 0, "candidate.unknown_free");
    CHECK(cand_overflows == 0, "candidate.recorder_overflow");
    CHECK(cleanup_no_disposer == 0, "candidate.cleanup_no_disposer");
    cand_allocs = 0;
    cand_frees = 0;
    cand_unknown_frees = 0;
    cand_overflows = 0;
    cand_attempt_n = 0;
    test_dispose_calls = 0;
    cleanup_no_disposer = 0;
    return true;
}

static unsigned cand_live_count(void) { return cand_live_n; }

/* Release every candidate the helper left live, through the EXACT disposer
 * recorded for it when the helper offered it to the setter. Calling the
 * renamed raw free here would balance the allocation count while leaving the
 * candidate's provider-owned state un-released, so the disposer identity --
 * not the byte count -- is what this cleanup asserts.
 *
 * Test-owned cleanup, never a reset: the recorder observes the disposer's own
 * free like any other, and `test_dispose_calls` keeps it attributable so it
 * can never be mistaken for product cleanup. */
static void test_release_live_candidates(void)
{
    unsigned guard = 0;
    while (cand_live_n > 0 && guard++ < sizeof(cand_live) / sizeof(cand_live[0])) {
        void *p = cand_live[cand_live_n - 1];
        picoquic_free_verify_certificate_ctx d = cand_attempt_disposer(p);
        if (d == NULL) {
            /* A live candidate the helper never offered to the setter cannot
             * be released correctly; record it rather than raw-freeing it. */
            cleanup_no_disposer++;
            CHECK(d != NULL, "candidate.cleanup_no_disposer");
            return;
        }
        unsigned before = cand_frees;
        test_dispose_calls++;
        d((ptls_verify_certificate_t *)p);
        /* The disposer must reach the candidate's own free exactly once. */
        CHECK(cand_frees == before + 1, "candidate.cleanup_disposer_freed_once");
        if (cand_frees == before) return;   /* no progress: stop, keep evidence */
    }
}

/* -- an identifiable pre-installed verifier ------------------------------ *
 * Distinct addresses, so "unchanged" is an identity statement rather than a
 * NULL check. */

static ptls_verify_certificate_t g_old_verifier;
static unsigned g_old_dispose_calls;        /* total */
static unsigned g_old_dispose_by_setter;    /* attributed to the setter */
static void old_dispose(ptls_verify_certificate_t *v)
{
    (void)v;
    g_old_dispose_calls++;
    if (in_setter) g_old_dispose_by_setter++;
}

static void seed_installed(picoquic_quic_t *quic)
{
    memset(quic, 0, sizeof(*quic));
    quic->verifier = &g_old_verifier;
    quic->dispose = old_dispose;
    g_old_dispose_calls = 0;
    g_old_dispose_by_setter = 0;
}

static void check_old_preserved(picoquic_quic_t *quic, const char *tag)
{
    char buf[128];
    snprintf(buf, sizeof(buf), "%s.old_verifier_identity", tag);
    CHECK(quic->verifier == &g_old_verifier, buf);
    snprintf(buf, sizeof(buf), "%s.old_disposer_identity", tag);
    CHECK(quic->dispose == old_dispose, buf);
    snprintf(buf, sizeof(buf), "%s.old_not_disposed", tag);
    CHECK(g_old_dispose_calls == 0, buf);
    snprintf(buf, sizeof(buf), "%s.old_not_disposed_by_setter", tag);
    CHECK(g_old_dispose_by_setter == 0, buf);
}

#if defined(MOQ_PICOQUIC_VERIFY_OPENSSL)

static void test_random_bytes(void *buf, size_t len) { memset(buf, 0xa5, len); }

/* `serial` makes each certificate byte-distinct, so a chain built from them
 * can prove ORDER: identical DER in every slot cannot. */
static int make_self_signed_der_serial(uint8_t **out, size_t *out_len,
                                       long serial)
{
    int rc = -1;
    EVP_PKEY_CTX *key_ctx = NULL;
    EVP_PKEY *key = NULL;
    X509 *cert = NULL;
    unsigned char *der = NULL;

    *out = NULL;
    *out_len = 0;
    key_ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
    if (key_ctx == NULL || EVP_PKEY_keygen_init(key_ctx) <= 0 ||
        EVP_PKEY_CTX_set_rsa_keygen_bits(key_ctx, 2048) <= 0 ||
        EVP_PKEY_keygen(key_ctx, &key) <= 0)
        goto done;

    cert = X509_new();
    if (cert == NULL || X509_set_version(cert, 2) != 1 ||
        ASN1_INTEGER_set(X509_get_serialNumber(cert), serial) != 1 ||
        X509_gmtime_adj(X509_get_notBefore(cert), 0) == NULL ||
        X509_gmtime_adj(X509_get_notAfter(cert), 3600) == NULL ||
        X509_set_pubkey(cert, key) != 1)
        goto done;

    X509_NAME *name = X509_get_subject_name(cert);
    if (name == NULL ||
        X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                                   (const unsigned char *)"relay.example",
                                   -1, -1, 0) != 1 ||
        X509_set_issuer_name(cert, name) != 1 ||
        X509_sign(cert, key, EVP_sha256()) <= 0)
        goto done;

    int der_len = i2d_X509(cert, &der);
    if (der_len <= 0) goto done;
    *out = (uint8_t *)der;
    *out_len = (size_t)der_len;
    der = NULL;
    rc = 0;

done:
    OPENSSL_free(der);
    X509_free(cert);
    EVP_PKEY_free(key);
    EVP_PKEY_CTX_free(key_ctx);
    return rc;
}

static int make_self_signed_der(uint8_t **out, size_t *out_len)
{
    return make_self_signed_der_serial(out, out_len, 1);
}

#define PROBE_MAX_CHAIN 16u

typedef struct callback_probe {
    unsigned calls;
    moq_picoquic_cert_verdict_t verdict;
    /* the COMPLETE expected input, element by element, leaf first */
    size_t want_count;
    const uint8_t *want_der[PROBE_MAX_CHAIN];
    size_t want_der_len[PROBE_MAX_CHAIN];
    void *identity;
} callback_probe_t;

/* Declare the exact chain the next call must deliver. */
static void probe_expect_uniform(callback_probe_t *p, const uint8_t *der,
                                 size_t len, size_t count)
{
    p->want_count = count;
    for (size_t i = 0; i < count && i < PROBE_MAX_CHAIN; i++) {
        p->want_der[i] = der;
        p->want_der_len[i] = len;
    }
}

/* A STATELESS delegate: it keeps no context at all, which is exactly what the
 * NULL-`ctx` contract permits. File-scope counters stand in for the state a
 * context would otherwise carry. */
static unsigned g_stateless_calls;
/* Counted per invocation, not latched: a sticky "some call saw NULL" boolean
 * cannot fail when a LATER call is handed a context. */
static unsigned g_stateless_null_ctx;
static unsigned g_stateless_nonnull_ctx;
static moq_picoquic_cert_verdict_t g_stateless_verdict;
static moq_picoquic_cert_verdict_t stateless_verify(
    void *ctx, moq_bytes_t server_name,
    const moq_bytes_t *certificates, size_t certificate_count)
{
    (void)server_name; (void)certificates; (void)certificate_count;
    g_stateless_calls++;
    if (ctx == NULL) g_stateless_null_ctx++; else g_stateless_nonnull_ctx++;
    return g_stateless_verdict;
}

/* The element comparison as a PURE predicate: it reports, it does not assert.
 * The order self-check below needs to observe a MISMATCH, and a green binary
 * must print nothing that looks like a failure -- so the observation and the
 * assertion are separated here rather than by suppressing output. */
static void chain_matches(const callback_probe_t *p,
                          const moq_bytes_t *certificates,
                          size_t certificate_count,
                          int *out_len_ok, int *out_bytes_ok)
{
    int len_ok = 1, bytes_ok = 1;
    for (size_t i = 0; i < certificate_count && i < PROBE_MAX_CHAIN; i++) {
        if (certificates[i].len != p->want_der_len[i]) len_ok = 0;
        else if (certificates[i].data == NULL ||
                 memcmp(certificates[i].data, p->want_der[i],
                        p->want_der_len[i]) != 0)
            bytes_ok = 0;
    }
    *out_len_ok = len_ok;
    *out_bytes_ok = bytes_ok;
}

static moq_picoquic_cert_verdict_t verify_chain(
    void *ctx, moq_bytes_t server_name,
    const moq_bytes_t *certificates, size_t certificate_count)
{
    callback_probe_t *p = (callback_probe_t *)ctx;
    static const char expected_name[] = "relay.example";
    if (p == NULL) return MOQ_PICOQUIC_CERT_REJECT;
    p->calls++;
    CHECK(p->identity == p, "callback.context_identity");
    CHECK(server_name.len == sizeof(expected_name) - 1,
          "callback.server_name_len");
    CHECK(server_name.data != NULL &&
              memcmp(server_name.data, expected_name,
                     sizeof(expected_name) - 1) == 0,
          "callback.server_name_bytes");
    /* the expected count is declared per call, not hard-coded to one */
    CHECK(certificate_count == p->want_count, "callback.chain_count");
    CHECK(certificates != NULL, "callback.chain_nonnull");
    if (certificates != NULL && certificate_count == p->want_count) {
        int len_ok = 1, bytes_ok = 1;
        chain_matches(p, certificates, certificate_count, &len_ok, &bytes_ok);
        /* every element compared, leaf first */
        CHECK(len_ok, "callback.chain_element_lens");
        CHECK(bytes_ok, "callback.chain_element_bytes");
    }
    return p->verdict;
}

/* Safe, distinct non-NULL output sentinels. Seeding the outputs before every
 * call that must clear them is what makes those rows able to detect an
 * implementation that simply never writes them: with NULL seeds they would
 * pass against a wrapper that never touches the outputs at all. */
static const uint8_t k_sentinel_data = 0xA5;
static int sentinel_sign(void *c, uint16_t a, ptls_iovec_t d, ptls_iovec_t g)
{
    (void)c; (void)a; (void)d; (void)g;
    CHECK(0, "sentinel.sign_invoked");
    return PTLS_ALERT_DECRYPT_ERROR;
}

/* -- OpenSSL key lifetime oracle ------------------------------------------ *
 * picotls' OpenSSL verifier hands back an EVP_PKEY in `verify_data`, and its
 * `verify_sign` frees that key -- including on the documented empty-data
 * cleanup path (picotls.h:795-799, picotls/lib/openssl.c:1730-1806). An
 * ex-data free callback on the key is invoked exactly when the key is freed,
 * so "the output was consumed" becomes an observation rather than a claim. */
static int g_pkey_ex_idx = -1;
static unsigned g_pkey_freed;

static void pkey_ex_free(void *parent, void *ptr, CRYPTO_EX_DATA *ad, int idx,
                         long argl, void *argp)
{
    (void)parent; (void)ptr; (void)ad; (void)idx; (void)argl; (void)argp;
    g_pkey_freed++;
}

/* Mark the key so its release is counted. Returns false if the key cannot be
 * marked, which is reported by the caller rather than silently skipped. */
static bool mark_pkey(void *verify_data)
{
    if (verify_data == NULL) return false;
    if (g_pkey_ex_idx < 0) {
        g_pkey_ex_idx = CRYPTO_get_ex_new_index(CRYPTO_EX_INDEX_EVP_PKEY, 0,
                                                NULL, NULL, NULL, pkey_ex_free);
        if (g_pkey_ex_idx < 0) return false;
    }
    return EVP_PKEY_set_ex_data((EVP_PKEY *)verify_data, g_pkey_ex_idx,
                                (void *)1) == 1;
}

/* THE output-cleared predicate. Pure: it reports, it does not assert, so the
 * permanent self-check can feed a non-clearing verifier through the SAME logic
 * without leaving diagnostics behind. Every refusing row uses it. */
static bool outputs_cleared(
    int (*verify_sign)(void *, uint16_t, ptls_iovec_t, ptls_iovec_t),
    void *verify_data)
{
    return verify_sign == NULL && verify_data == NULL;
}

/* Every refusing call goes through ONE helper that seeds the sentinels, makes
 * the call, applies the shared output-cleared check, and records the row by
 * name. Omitting the check at a call site is therefore not possible: the check
 * lives in the helper, and a call site that bypasses the helper is missing
 * from the ledger below. */
#define REFUSE_ROWS_MAX 8
static const char *g_refuse_rows[REFUSE_ROWS_MAX];
static unsigned g_refuse_n;
static unsigned g_refuse_overflow;

/* Exact BLOCK ledger. The two context/coherence blocks below run on their own
 * QUIC context rather than through the refusing-row helper, so they register
 * here by name. Deleting either block -- function body and all -- leaves an
 * empty ledger slot that is named at exit, so totality does not depend on an
 * unused-symbol warning. */
#define BLOCK_ROWS_MAX 4
static const char *g_block_rows[BLOCK_ROWS_MAX];
static unsigned g_block_n;
static unsigned g_block_overflow;

static void block_note(const char *name)
{
    if (g_block_n < BLOCK_ROWS_MAX) g_block_rows[g_block_n++] = name;
    else g_block_overflow++;
}

static void refuse_row_note(const char *name)
{
    if (g_refuse_n < REFUSE_ROWS_MAX) g_refuse_rows[g_refuse_n++] = name;
    else g_refuse_overflow++;
}

/* Seed both outputs with distinct non-NULL sentinels. */
static void seed_outputs(
    int (**verify_sign)(void *, uint16_t, ptls_iovec_t, ptls_iovec_t),
    void **verify_data)
{
    *verify_sign = sentinel_sign;
    *verify_data = (void *)&k_sentinel_data;
}

/* Make one refusing call: seed, invoke, require refusal, apply THE shared
 * output-cleared check, and record the row. Returns the callback delta. */
static unsigned refuse_call(ptls_verify_certificate_t *v, ptls_t *tls,
                            ptls_iovec_t *certs, size_t n,
                            callback_probe_t *probe, const char *row)
{
    int (*vs)(void *, uint16_t, ptls_iovec_t, ptls_iovec_t);
    void *vd;
    char buf[128];
    unsigned before = probe->calls;

    seed_outputs(&vs, &vd);
    int rc = v->cb(v, tls, "relay.example", &vs, &vd, certs, n);

    snprintf(buf, sizeof(buf), "%s.rejects", row);
    CHECK(rc != 0, buf);
    snprintf(buf, sizeof(buf), "%s.outputs_cleared", row);
    CHECK(outputs_cleared(vs, vd), buf);
    refuse_row_note(row);
    return probe->calls - before;
}

/* Permanent mechanism check for chain ORDER. It drives the element-byte oracle
 * directly with two non-leaf elements swapped while the count and every
 * element's validity stay unchanged, and requires the oracle to notice -- so
 * the ordering claim rests on a demonstrated discriminator rather than on a
 * chain that happens to arrive in order. Quiet on a passing run. */
static void selftest_chain_order(void)
{
    uint8_t *der[4] = {0};
    size_t len[4] = {0};
    int ok = 1;
    for (int i = 0; i < 4; i++)
        if (make_self_signed_der_serial(&der[i], &len[i], 900 + i) != 0)
            ok = 0;
    CHECK(ok, "selftest.chain_order_fixture");
    if (!ok) { for (int i = 0; i < 4; i++) OPENSSL_free(der[i]); return; }

    callback_probe_t p;
    memset(&p, 0, sizeof(p));
    p.identity = &p;
    p.verdict = MOQ_PICOQUIC_CERT_TRUST;
    p.want_count = 4;
    for (int i = 0; i < 4; i++) { p.want_der[i] = der[i]; p.want_der_len[i] = len[i]; }

    /* declared order, but elements 1 and 2 swapped in the DELIVERED chain */
    moq_bytes_t chain[4];
    const int order[4] = {0, 2, 1, 3};
    for (int i = 0; i < 4; i++) {
        chain[i].data = der[order[i]];
        chain[i].len = len[order[i]];
    }

    /* Exercised through the SAME predicate the live rows assert on, so this
     * still proves the element oracle would catch a reordered chain -- without
     * a green run emitting a failure-looking diagnostic. */
    int len_ok = 1, bytes_ok = 1;
    chain_matches(&p, chain, 4, &len_ok, &bytes_ok);
    CHECK(!bytes_ok, "selftest.chain_order_swap_detected");
    CHECK(len_ok, "selftest.chain_order_lens_unchanged");

    /* and the same predicate accepts the DECLARED order, so the mismatch above
     * is attributable to the swap and not to a predicate that never matches */
    moq_bytes_t ordered[4];
    for (int i = 0; i < 4; i++) { ordered[i].data = der[i]; ordered[i].len = len[i]; }
    chain_matches(&p, ordered, 4, &len_ok, &bytes_ok);
    CHECK(bytes_ok && len_ok, "selftest.chain_order_declared_matches");

    for (int i = 0; i < 4; i++) OPENSSL_free(der[i]);
}

/* -- negative self-test for the output-clearing rows ---------------------- *
 * A permanent non-vacuity check. It deliberately CONSTRUCTS a verifier that
 * refuses without touching the caller's outputs, then runs it through the same
 * predicate the real rows use: each must detect it. If the predicate passes
 * against a non-clearing verifier, every row built on it is vacuous and this
 * self-test says so by name. Constructing the negative here is what makes the
 * rows' positive answers mean something. */
static int nonclearing_cb(ptls_verify_certificate_t *super, ptls_t *tls,
                          const char *server_name,
                          int (**verify_sign)(void *, uint16_t, ptls_iovec_t,
                                              ptls_iovec_t),
                          void **verify_data, ptls_iovec_t *certs,
                          size_t num_certs)
{
    (void)super; (void)tls; (void)server_name;
    (void)verify_sign; (void)verify_data; (void)certs; (void)num_certs;
    return PTLS_ALERT_BAD_CERTIFICATE;    /* refuses, clears nothing */
}

/* Mechanism check for test-owned cleanup. It constructs one live candidate
 * deterministically -- through the same renamed allocator and the same attempt
 * record the fake setter writes -- so the disposer-identity oracle is exercised
 * on a known input rather than only on the paths the real rows happen to take.
 *
 * It is NOT a substitute for the real rows: it proves the mechanism can tell
 * an exact-disposer release from a raw free, which is precisely what an
 * allocation-balance assertion cannot do. */
static unsigned selftest_disposer_calls;
static void selftest_disposer(ptls_verify_certificate_t *v)
{
    selftest_disposer_calls++;
    moq_test_verify_free(v);
}

static void selftest_cleanup_disposer(void)
{
    if (!cand_reset(__func__)) return;
    selftest_disposer_calls = 0;

    void *p = moq_test_verify_calloc(1, 64);
    CHECK(p != NULL, "selftest.cleanup_alloc");
    if (p == NULL) return;
    cand_attempt_record(p, selftest_disposer);
    CHECK(cand_live_count() == 1, "selftest.cleanup_one_live");

    unsigned before_frees = cand_frees;
    test_release_live_candidates();

    /* The exact recorded disposer ran once ... */
    CHECK(selftest_disposer_calls == 1, "selftest.cleanup_exact_disposer_once");
    CHECK(test_dispose_calls == 1, "selftest.cleanup_disposer_call_counted");
    CHECK(cleanup_no_disposer == 0, "selftest.cleanup_disposer_known");
    /* ... and its free is still the allocation-balance observation. A raw free
     * would satisfy these two while failing the three above. */
    CHECK(cand_frees == before_frees + 1, "selftest.cleanup_balanced");
    CHECK(cand_live_count() == 0, "selftest.cleanup_complete");
}

static void selftest_output_clearing(void)
{
    ptls_verify_certificate_t v;
    memset(&v, 0, sizeof(v));
    v.cb = nonclearing_cb;

    int (*vs)(void *, uint16_t, ptls_iovec_t, ptls_iovec_t);
    void *vd;
    seed_outputs(&vs, &vd);
    int rc = v.cb(&v, NULL, "relay.example", &vs, &vd, NULL, 0);

    /* The refusal alone must not satisfy the rows. This runs the stand-in
     * through THE SAME predicate the real rows use, so removing that predicate
     * from a row is not something this self-check can bless. */
    CHECK(rc != 0, "selftest.nonclearing_refuses");
    CHECK(!outputs_cleared(vs, vd), "selftest.predicate_rejects_nonclearing");
}

/* -- candidate lifetime, on the PATH install helper ----------------------- *
 * `moq_picoquic_set_cert_verifier()` builds a candidate, installs it through
 * the generation-aware setter, and cleans up on a rejected install. These rows
 * pin its lifetime; the delegated setter's own lifetime is pinned separately by
 * the rows that follow, against the same four obligations. */
static void candidate_lifetime(void)
{
    picoquic_quic_t quic;

    /* 1. pre-setter failure (unloadable CA): no candidate, no setter. */
    seed_installed(&quic);
    if (!cand_reset(__func__)) return;
    setter_calls = 0;
    CHECK(moq_picoquic_set_cert_verifier(&quic, "/nonexistent/ca.pem") != 0,
          "candidate.pre_setter_failure_reported");
    CHECK(cand_allocs == 0, "candidate.pre_setter_no_candidate");
    CHECK(setter_calls == 0, "candidate.pre_setter_no_setter");
    CHECK(cand_live_count() == 0, "candidate.pre_setter_nothing_live");
    check_old_preserved(&quic, "candidate.pre_setter");

#if defined(MOQ_TEST_PICOQUIC_MODERN_API)
    /* 2. modern setter rejection: exactly one candidate built, setter called
     *    once, candidate disposed exactly once, nothing left live, and both
     *    old identities untouched. */
    seed_installed(&quic);
    if (!cand_reset(__func__)) return;
    setter_calls = 0;
    setter_result = PICOQUIC_ERROR_TLS_CONFIG_FROZEN;
    CHECK(moq_picoquic_set_cert_verifier(&quic, MOQ_TEST_CA_PEM) != 0,
          "candidate.rejection_reported");
    setter_result = 0;
    CHECK(cand_allocs == 1, "candidate.rejection_one_candidate");
    CHECK(setter_calls == 1, "candidate.rejection_setter_once");
    CHECK(cand_frees == 1, "candidate.rejection_disposed_once");
    CHECK(cand_live_count() == 0, "candidate.rejection_none_live");
    check_old_preserved(&quic, "candidate.rejection");
#endif

    /* 3. successful install: ownership TRANSFERS -- the helper must not
     *    dispose it -- and the simulated QUIC teardown disposes it once. */
    seed_installed(&quic);
    if (!cand_reset(__func__)) return;
    setter_calls = 0;
    CHECK(moq_picoquic_set_cert_verifier(&quic, MOQ_TEST_CA_PEM) == 0,
          "candidate.install_succeeds");
    CHECK(cand_allocs == 1, "candidate.install_one_candidate");
    CHECK(setter_calls == 1, "candidate.install_setter_once");
    CHECK(cand_frees == 0, "candidate.install_not_disposed_by_helper");
    CHECK(cand_live_count() == 1, "candidate.install_ownership_transferred");
    CHECK(quic.verifier != NULL && quic.verifier != &g_old_verifier,
          "candidate.install_replaced_old");
    CHECK(quic.dispose != NULL && quic.dispose != old_dispose,
          "candidate.install_owns_disposer");
    /* picoquic's contract: a successful replacement releases the previous
     * owner, and the SETTER is who does it -- a direct helper disposal would
     * be a different (wrong) behaviour with the same total. */
    CHECK(g_old_dispose_calls == 1, "candidate.install_old_released_once");
    CHECK(g_old_dispose_by_setter == 1, "candidate.install_old_released_by_setter");
    if (quic.dispose != NULL && quic.dispose != old_dispose)
        quic.dispose(quic.verifier);          /* simulated QUIC teardown */
    CHECK(cand_frees == 1, "candidate.teardown_disposes_once");
    CHECK(cand_live_count() == 0, "candidate.teardown_none_live");
}

/* -- candidate lifetime of the DELEGATED helper itself -------------------- *
 * The rows above pin the path helper's own lifetime. These put
 * the SAME recorder around `moq_picoquic_set_cert_verify_callback()` -- the
 * helper actually under test -- so its ownership is observed rather than
 * inferred from "the old disposer was not called". */
static void delegated_candidate_lifetime(callback_probe_t *probe)
{
    picoquic_quic_t quic;

    /* 1. pre-setter invalid arguments: no candidate, no setter, nothing live */
    seed_installed(&quic);
    if (!cand_reset(__func__)) return;
    setter_calls = 0;
    CHECK(moq_picoquic_set_cert_verify_callback(&quic, NULL, probe) == -1,
          "delegated.null_callback_refused");
    CHECK(cand_allocs == 0, "delegated.null_callback_no_candidate");
    CHECK(setter_calls == 0, "delegated.null_callback_no_setter");
    CHECK(cand_live_count() == 0, "delegated.null_callback_nothing_live");
    check_old_preserved(&quic, "delegated.null_callback");

#if defined(MOQ_TEST_PICOQUIC_MODERN_API)
    /* 2. modern setter rejection: the DELEGATED candidate is built, the setter
     *    is called once, the candidate is disposed exactly once, nothing is
     *    left live, and both old identities survive un-disposed. */
    seed_installed(&quic);
    if (!cand_reset(__func__)) return;
    setter_calls = 0;
    setter_result = PICOQUIC_ERROR_TLS_CONFIG_FROZEN;
    CHECK(moq_picoquic_set_cert_verify_callback(&quic, verify_chain, probe) == -1,
          "delegated.rejection_reported");
    setter_result = 0;
    CHECK(cand_allocs == 1, "delegated.rejection_one_candidate");
    CHECK(setter_calls == 1, "delegated.rejection_setter_once");
    CHECK(cand_frees == 1, "delegated.rejection_disposed_once");
    CHECK(cand_live_count() == 0, "delegated.rejection_none_live");
    check_old_preserved(&quic, "delegated.rejection");
#endif

    /* 3. successful install: ownership TRANSFERS to the QUIC context -- the
     *    helper must not dispose it -- and simulated teardown disposes it
     *    exactly once. Asserted on whichever API generation this build has. */
    seed_installed(&quic);
    if (!cand_reset(__func__)) return;
    setter_calls = 0;
    CHECK(moq_picoquic_set_cert_verify_callback(&quic, verify_chain, probe) == 0,
          "delegated.install_succeeds");
    CHECK(cand_allocs == 1, "delegated.install_one_candidate");
    CHECK(setter_calls == 1, "delegated.install_setter_once");
    CHECK(cand_frees == 0, "delegated.install_not_disposed_by_helper");
    CHECK(cand_live_count() == 1, "delegated.install_ownership_transferred");
    CHECK(quic.verifier != NULL && quic.verifier != &g_old_verifier,
          "delegated.install_replaced_old");
    CHECK(quic.dispose != NULL && quic.dispose != old_dispose,
          "delegated.install_owns_disposer");
    CHECK(g_old_dispose_calls == 1, "delegated.install_old_released_once");
    CHECK(g_old_dispose_by_setter == 1, "delegated.install_old_released_by_setter");
    if (quic.dispose != NULL && quic.dispose != old_dispose) {
        quic.dispose(quic.verifier);          /* simulated QUIC teardown */
        CHECK(cand_frees == 1, "delegated.teardown_disposes_once");
        CHECK(cand_live_count() == 0, "delegated.teardown_none_live");
    } else {
        ROW_ABSENT("delegated.teardown_disposes_once");
        ROW_ABSENT("delegated.teardown_none_live");
        /* The expected install did not happen. Recorder state is NEVER reset
         * to make the next row start clean -- that would erase the very leak
         * it exists to detect. Anything the helper did allocate is released
         * here through the real disposer, and THAT cleanup is itself asserted
         * balanced before the next reset. */
        unsigned before_frees = cand_frees;
        unsigned live_before = cand_live_count();
        test_release_live_candidates();
        /* Balance is the allocation observation; the disposer-call count is a
         * SEPARATE oracle, so replacing the exact disposer with a raw free
         * stays balanced and still fails here. */
        CHECK(test_dispose_calls == live_before,
              "delegated.absent_install_cleanup_via_disposer");
        CHECK(cleanup_no_disposer == 0,
              "delegated.absent_install_cleanup_disposer_known");
        CHECK(cand_frees == before_frees + live_before,
              "delegated.absent_install_cleanup_balanced");
        CHECK(cand_live_count() == 0,
              "delegated.absent_install_cleanup_complete");
    }
}

int main(void)
{
    selftest_cleanup_disposer();
    selftest_output_clearing();
    selftest_chain_order();
    candidate_lifetime();

    /* -- the delegated-trust result classifier ---------------------------- */
    CHECK(moq_picoquic_test_delegated_trust_result(0) == 0,
          "override.existing_success");
    /* the one thing delegation must change: a chain-POLICY alert */
    CHECK(moq_picoquic_test_delegated_trust_result(PTLS_ALERT_UNKNOWN_CA) == 0,
          "override.certificate_policy_delegated");
    /* the two it must never change: operational failures */
    CHECK(moq_picoquic_test_delegated_trust_result(PTLS_ERROR_NO_MEMORY) ==
              PTLS_ERROR_NO_MEMORY,
          "override.no_memory_preserved");
    CHECK(moq_picoquic_test_delegated_trust_result(PTLS_ERROR_LIBRARY) ==
              PTLS_ERROR_LIBRARY,
          "override.library_failure_preserved");

    /* -- capability and argument validation ------------------------------- */
    CHECK(moq_picoquic_cert_verify_callback_supported() == 1,
          "supported.openssl");

    callback_probe_t probe;
    memset(&probe, 0, sizeof(probe));
    probe.identity = &probe;
    probe.verdict = MOQ_PICOQUIC_CERT_TRUST;

    picoquic_quic_t quic;

    /* A NULL QUIC context is refused and nothing is recorded. `ctx` is a
     * different argument entirely and MAY be NULL -- see the stateless rows
     * below. */
    setter_calls = 0;
    CHECK(moq_picoquic_set_cert_verify_callback(NULL, verify_chain, &probe) == -1,
          "install.null_quic");
    CHECK(setter_calls == 0, "install.null_quic_no_setter");

    /* A pre-setter failure (NULL callback) never calls the setter and leaves
     * BOTH old identities in place. */
    seed_installed(&quic);
    setter_calls = 0;
    CHECK(moq_picoquic_set_cert_verify_callback(&quic, NULL, &probe) == -1,
          "install.null_callback");
    CHECK(setter_calls == 0, "install.null_callback_no_setter");
    check_old_preserved(&quic, "install.null_callback");


    uint8_t *der = NULL;
    size_t der_len = 0;
    CHECK(make_self_signed_der(&der, &der_len) == 0, "fixture.self_signed_der");
    if (der == NULL) return 1;
    probe_expect_uniform(&probe, der, der_len, 1);

    /* The delegated helper's OWN candidate ownership, directly observed. */
    delegated_candidate_lifetime(&probe);

    /* The modern-rejection ownership rows live in
     * delegated_candidate_lifetime(): "old disposer not called" plus a
     * preserved identity cannot prove the rejected candidate was released,
     * so that inference is deliberately not made here. */

    /* -- successful install ----------------------------------------------- */
    seed_installed(&quic);
    setter_calls = 0;
    CHECK(moq_picoquic_set_cert_verify_callback(&quic, verify_chain, &probe) == 0,
          "install.trust_callback");
    CHECK(setter_calls == 1, "install.setter_once");
    CHECK(quic.verifier != NULL && quic.verifier != &g_old_verifier,
          "install.verifier_replaced");
    CHECK(quic.dispose != NULL && quic.dispose != old_dispose,
          "install.disposer_owned");
    CHECK(g_old_dispose_calls == 1, "install.old_released_once");
    CHECK(g_old_dispose_by_setter == 1, "install.old_released_by_setter");

    ptls_context_t tls_ctx;
    memset(&tls_ctx, 0, sizeof(tls_ctx));
    tls_ctx.random_bytes = test_random_bytes;
    tls_ctx.get_time = &ptls_get_time;
    ptls_t *tls = ptls_new(&tls_ctx, 0);
    CHECK(tls != NULL, "fixture.client_tls");


    if (tls != NULL && quic.verifier != NULL &&
        quic.verifier != &g_old_verifier) {
        ptls_iovec_t cert = ptls_iovec_init(der, der_len);
        int (*verify_sign)(void *, uint16_t, ptls_iovec_t, ptls_iovec_t) = NULL;
        void *verify_data = NULL;

        /* TRUST delegates certificate policy ... */
        int rc = quic.verifier->cb(quic.verifier, tls, "relay.example",
                                   &verify_sign, &verify_data, &cert, 1);
        CHECK(rc == 0, "trust.overrides_untrusted_chain");
        CHECK(probe.calls == 1, "trust.callback_once");
        /* ... but cannot bypass TLS CertificateVerify: a usable leaf key must
         * still supply picotls' verification closure. */
        CHECK(verify_sign != NULL, "trust.certificate_verify_function");
        CHECK(verify_data != NULL, "trust.leaf_public_key");
        if (verify_sign != NULL && verify_data != NULL) {
            static const uint8_t transcript[] = {0x01};
            static const uint8_t bad_signature[] = {0x00};
            CHECK(mark_pkey(verify_data), "trust.key_markable");
            g_pkey_freed = 0;
            int vrc = verify_sign(
                verify_data, PTLS_SIGNATURE_RSA_PSS_RSAE_SHA256,
                ptls_iovec_init(transcript, sizeof(transcript)),
                ptls_iovec_init(bad_signature, sizeof(bad_signature)));
            CHECK(vrc == PTLS_ALERT_DECRYPT_ERROR,
                  "trust.invalid_signature_rejected");
            /* the ordinary rejection path consumes the key too */
            CHECK(g_pkey_freed == 1, "trust.key_released_once");
        } else {
            ROW_ABSENT("trust.key_markable");
            ROW_ABSENT("trust.invalid_signature_rejected");
            ROW_ABSENT("trust.key_released_once");
        }

        /* REJECT fails the handshake and clears the outputs. */
        probe.verdict = MOQ_PICOQUIC_CERT_REJECT;
        CHECK(refuse_call(quic.verifier, tls, &cert, 1, &probe, "reject") == 1,
              "reject.callback_delta_one");

        /* An unrecognised verdict is not "not REJECT". */
        probe.verdict = (moq_picoquic_cert_verdict_t)99;
        CHECK(refuse_call(quic.verifier, tls, &cert, 1, &probe,
                          "invalid_verdict") == 1,
              "invalid_verdict.callback_delta_one");

        /* An absent chain is refused WITHOUT consulting the application. */
        probe.verdict = MOQ_PICOQUIC_CERT_TRUST;
        unsigned before;
        CHECK(refuse_call(quic.verifier, tls, NULL, 0, &probe,
                          "empty_chain") == 0,
              "empty_chain.callback_not_called");

        /* TRUST cannot make malformed DER usable by the TLS stack. */
        uint8_t malformed[] = {0x30, 0x03, 0x01};
        ptls_iovec_t malformed_cert =
            ptls_iovec_init(malformed, sizeof(malformed));
        probe_expect_uniform(&probe, malformed, sizeof(malformed), 1);
        CHECK(refuse_call(quic.verifier, tls, &malformed_cert, 1, &probe,
                          "malformed_der") == 1,
              "malformed_der.callback_once");
        probe_expect_uniform(&probe, der, der_len, 1);

        /* The chain-cardinality bound, at its ACCEPTED EDGE (16) and one
         * beyond it (17). The over-limit input must never reach the
         * application. */
        /* SIXTEEN independently valid, BYTE-DISTINCT certificates, so the
         * element-by-element comparison actually proves leaf-first ORDER.
         * Repeating one DER in every slot could not. */
        uint8_t *chain_der[16] = {0};
        size_t chain_der_len[16] = {0};
        ptls_iovec_t chain16[16];
        int chain_ok = 1;
        for (size_t i = 0; i < 16; i++) {
            if (make_self_signed_der_serial(&chain_der[i], &chain_der_len[i],
                                            (long)(100 + i)) != 0) {
                chain_ok = 0;
                break;
            }
            chain16[i] = ptls_iovec_init(chain_der[i], chain_der_len[i]);
        }
        CHECK(chain_ok, "fixture.sixteen_distinct_certs");
        /* EVERY PAIR must differ, not merely each neighbour: adjacent-only
         * comparison cannot rule out a repeated element further apart, and a
         * repeat would make an order swap undetectable. */
        int all_distinct = chain_ok;
        for (size_t i = 0; i < 16 && all_distinct; i++)
            for (size_t j = i + 1; j < 16 && all_distinct; j++)
                if (chain_der_len[i] == chain_der_len[j] &&
                    memcmp(chain_der[i], chain_der[j], chain_der_len[i]) == 0)
                    all_distinct = 0;
        CHECK(all_distinct, "fixture.chain_elements_pairwise_distinct");

        if (chain_ok) {
            probe.want_count = 16;
            for (size_t i = 0; i < 16; i++) {
                probe.want_der[i] = chain_der[i];
                probe.want_der_len[i] = chain_der_len[i];
            }
            before = probe.calls;
            seed_outputs(&verify_sign, &verify_data);
            rc = quic.verifier->cb(quic.verifier, tls, "relay.example",
                                   &verify_sign, &verify_data, chain16, 16);
            CHECK(probe.calls == before + 1, "chain_edge_16.callback_once");
            CHECK(rc == 0, "chain_edge_16.accepted");
            CHECK(verify_sign != NULL && verify_sign != sentinel_sign,
                  "chain_edge_16.verify_function_replaced");
            CHECK(verify_data != NULL &&
                      verify_data != (void *)&k_sentinel_data,
                  "chain_edge_16.verify_data_replaced");

            /* picotls requires the verify callback to be invoked with EMPTY
             * data/sign when an error occurs after successful certificate
             * verification; OpenSSL's verify_sign frees the leaf key on that
             * path (picotls.h:795-799, picotls/lib/openssl.c:1730-1806).
             * Exercise it before the outputs are overwritten, so the accepted
             * row does not silently drop the key it obtained. */
            if (verify_sign != NULL && verify_data != NULL &&
                verify_sign != sentinel_sign) {
                /* Mark the returned key, then take the empty-data cleanup
                 * path and require the key to have been RELEASED exactly
                 * once: the balance is observed, not asserted. */
                CHECK(mark_pkey(verify_data), "chain_edge_16.key_markable");
                g_pkey_freed = 0;
                int crc = verify_sign(verify_data, 0,
                                      ptls_iovec_init(NULL, 0),
                                      ptls_iovec_init(NULL, 0));
                CHECK(crc == 0, "chain_edge_16.empty_data_cleanup");
                CHECK(g_pkey_freed == 1, "chain_edge_16.key_released_once");
            } else {
                ROW_ABSENT("chain_edge_16.key_markable");
                ROW_ABSENT("chain_edge_16.empty_data_cleanup");
                ROW_ABSENT("chain_edge_16.key_released_once");
            }
            verify_sign = NULL;
            verify_data = NULL;

            /* One beyond the bound: refused WITHOUT reaching the application,
             * and with the outputs cleared. */
            ptls_iovec_t chain17[17];
            for (size_t i = 0; i < 16; i++) chain17[i] = chain16[i];
            chain17[16] = chain16[0];
            CHECK(refuse_call(quic.verifier, tls, chain17, 17, &probe,
                              "chain_over_17") == 0,
                  "chain_over_17.callback_not_called");
        } else {
            ROW_ABSENT("chain_edge_16.accepted");
            ROW_ABSENT("chain_edge_16.empty_data_cleanup");
            ROW_ABSENT("chain_over_17.rejects");
        }
        for (size_t i = 0; i < 16; i++) OPENSSL_free(chain_der[i]);
        probe_expect_uniform(&probe, der, der_len, 1);
    } else {
        ROW_ABSENT("trust.overrides_untrusted_chain");
        ROW_ABSENT("trust.certificate_verify_function");
        ROW_ABSENT("trust.leaf_public_key");
        ROW_ABSENT("trust.invalid_signature_rejected");
        ROW_ABSENT("reject.fails_handshake");
        ROW_ABSENT("invalid_verdict.rejects");
        ROW_ABSENT("empty_chain.rejects");
        ROW_ABSENT("malformed_der.rejects_after_trust");
        ROW_ABSENT("chain_edge_16.accepted");
        ROW_ABSENT("chain_edge_16.empty_data_cleanup");
        ROW_ABSENT("chain_over_17.rejects");
        /* The refusing rows cannot execute without an installed verifier, but
         * they must still be ACCOUNTED FOR: each registers in the same ledger
         * so a deleted row is a missing ledger slot on whichever path runs. */
        ROW_ABSENT("reject.outputs_cleared");
        refuse_row_note("reject");
        ROW_ABSENT("invalid_verdict.outputs_cleared");
        refuse_row_note("invalid_verdict");
        ROW_ABSENT("empty_chain.outputs_cleared");
        refuse_row_note("empty_chain");
        ROW_ABSENT("malformed_der.outputs_cleared");
        refuse_row_note("malformed_der");
        ROW_ABSENT("chain_over_17.outputs_cleared");
        refuse_row_note("chain_over_17");
    }

    /* Exact refusing-row ledger: the five refusing rows must each have been
     * made through the one helper, exactly once, in order. Removing a real
     * row's use of the helper fails this ledger by name even on the branch
     * where no verifier could be installed and the rows cannot run. */
    {
        static const char *const want[] = {
            "reject", "invalid_verdict", "empty_chain",
            "malformed_der", "chain_over_17"
        };
        const unsigned want_n = (unsigned)(sizeof(want) / sizeof(want[0]));
        CHECK(g_refuse_overflow == 0, "refuse_ledger.no_overflow");
        if (g_refuse_n != want_n)
            fprintf(stderr, "REFUSE LEDGER: %u rows, expected %u\n",
                    g_refuse_n, want_n);
        CHECK(g_refuse_n == want_n, "refuse_ledger.exact_count");
        for (unsigned i = 0; i < want_n; i++) {
            int ok = i < g_refuse_n && g_refuse_rows[i] != NULL &&
                     strcmp(g_refuse_rows[i], want[i]) == 0;
            if (!ok)
                fprintf(stderr, "REFUSE LEDGER: slot %u is '%s', expected '%s'\n",
                        i, (i < g_refuse_n && g_refuse_rows[i]) ?
                               g_refuse_rows[i] : "<none>", want[i]);
            CHECK(ok, "refuse_ledger.row_present");
        }
    }

    /* -- a STATELESS callback: `ctx` is opaque and may be NULL ------------- *
     * The installed header never required a context, and a callback that keeps
     * no state legitimately has none. It must install, be consulted exactly
     * once per invocation with a NULL context, and translate accept/reject as
     * any other. Ownership is carried through the same candidate recorder the
     * rows above use, so the install and the teardown are observed rather than
     * assumed. */
    {
        /* Its OWN QUIC context: this block installs and disposes a verifier,
         * and must not disturb the one the rows above and below share. */
        picoquic_quic_t sq;
        block_note("stateless_ctx");
        /* Ownership is measured as a DELTA against the recorder's state on
         * entry, not against zero: the shared `quic` context above legitimately
         * still owns its installed candidate, which main disposes at the end.
         * The delta is what attributes exactly one candidate to THIS block. */
        unsigned live_before = cand_live_count();
        seed_installed(&sq);
        setter_calls = 0;
        g_stateless_calls = 0;
        g_stateless_null_ctx = 0;
        g_stateless_nonnull_ctx = 0;
        g_stateless_verdict = MOQ_PICOQUIC_CERT_TRUST;
        unsigned frees_before = cand_frees;

        /* Installation is a CONTROL-FLOW BOUNDARY, not just an assertion: on a
         * rejection `sq` still holds the seeded old verifier, so calling
         * through it would exercise the wrong object and dispose something this
         * block never installed. */
        int irc = moq_picoquic_set_cert_verify_callback(&sq, stateless_verify,
                                                        NULL);
        CHECK(irc == 0, "stateless.installed");
        CHECK(setter_calls == 1, "stateless.setter_once");
        bool installed = (irc == 0);

        if (installed) {
            /* the helper handed its candidate to the setter and disposed none */
            CHECK(cand_frees == frees_before,
                  "stateless.install_not_disposed_by_helper");
            CHECK(cand_live_count() == live_before + 1,
                  "stateless.install_ownership_transferred");
        } else {
            ROW_ABSENT("stateless.install_not_disposed_by_helper");
            ROW_ABSENT("stateless.install_ownership_transferred");
        }

        uint8_t *sder = NULL; size_t sder_len = 0;
        if (installed && make_self_signed_der(&sder, &sder_len) == 0) {
            ptls_iovec_t scert = ptls_iovec_init(sder, sder_len);
            int (*vs)(void *, uint16_t, ptls_iovec_t, ptls_iovec_t) = NULL;
            void *vd = NULL;
            seed_outputs(&vs, &vd);
            int src = sq.verifier->cb(sq.verifier, tls, "relay.example",
                                        &vs, &vd, &scert, 1);
            CHECK(g_stateless_calls == 1, "stateless.callback_once");
            CHECK(g_stateless_null_ctx == 1, "stateless.null_ctx_delivered");
            CHECK(g_stateless_nonnull_ctx == 0, "stateless.no_ctx_substituted");
            CHECK(src == 0, "stateless.accept_translated");

            /* TRUST hands back a real OWNED leaf key and its cleanup operation.
             * Overwriting the outputs without running that cleanup would leak
             * the key, so it is released here -- and the release is COUNTED,
             * which is also what proves the accept path produced a real key. */
            CHECK(vs != NULL && vd != NULL, "stateless.trust_outputs_present");
            if (vs != NULL && vd != NULL) {
                CHECK(mark_pkey(vd), "stateless.key_markable");
                g_pkey_freed = 0;
                int crc = vs(vd, 0, ptls_iovec_init(NULL, 0),
                             ptls_iovec_init(NULL, 0));
                CHECK(crc == 0, "stateless.empty_data_cleanup");
                CHECK(g_pkey_freed == 1, "stateless.key_released_once");
            } else {
                ROW_ABSENT("stateless.key_markable");
                ROW_ABSENT("stateless.empty_data_cleanup");
                ROW_ABSENT("stateless.key_released_once");
            }
            vs = NULL; vd = NULL;

            /* ... and the same stateless callback REFUSING rejects. */
            g_stateless_calls = 0;
            g_stateless_verdict = MOQ_PICOQUIC_CERT_REJECT;
            seed_outputs(&vs, &vd);
            src = sq.verifier->cb(sq.verifier, tls, "relay.example",
                                    &vs, &vd, &scert, 1);
            CHECK(g_stateless_calls == 1, "stateless.reject_callback_once");
            CHECK(g_stateless_null_ctx == 2, "stateless.reject_null_ctx");
            CHECK(g_stateless_nonnull_ctx == 0,
                  "stateless.reject_no_ctx_substituted");
            CHECK(src != 0, "stateless.reject_translated");
            CHECK(outputs_cleared(vs, vd), "stateless.reject_outputs_cleared");
            OPENSSL_free(sder);
        } else {
            ROW_ABSENT("stateless.callback_once");
            ROW_ABSENT("stateless.null_ctx_delivered");
            ROW_ABSENT("stateless.no_ctx_substituted");
            ROW_ABSENT("stateless.accept_translated");
            ROW_ABSENT("stateless.trust_outputs_present");
            ROW_ABSENT("stateless.key_markable");
            ROW_ABSENT("stateless.empty_data_cleanup");
            ROW_ABSENT("stateless.key_released_once");
            ROW_ABSENT("stateless.reject_callback_once");
            ROW_ABSENT("stateless.reject_null_ctx");
            ROW_ABSENT("stateless.reject_no_ctx_substituted");
            ROW_ABSENT("stateless.reject_translated");
            ROW_ABSENT("stateless.reject_outputs_cleared");
        }

        /* -- an INCOHERENT chain pair is refused, not dereferenced --------- *
         * (NULL, n>0) cannot be converted; the wrapper must refuse before any
         * conversion and without consulting the application. Its ledger slot is
         * registered either way, so a deleted block is still named at exit. */
        block_note("null_chain");
        g_stateless_calls = 0;
        if (installed) {
            int (*vs)(void *, uint16_t, ptls_iovec_t, ptls_iovec_t) = NULL;
            void *vd = NULL;
            seed_outputs(&vs, &vd);
            int nrc = sq.verifier->cb(sq.verifier, tls, "relay.example",
                                        &vs, &vd, NULL, 1);
            CHECK(nrc != 0, "null_chain.refused");
            CHECK(g_stateless_calls == 0, "null_chain.callback_not_called");
            CHECK(outputs_cleared(vs, vd), "null_chain.outputs_cleared");
        } else {
            ROW_ABSENT("null_chain.refused");
            ROW_ABSENT("null_chain.callback_not_called");
            ROW_ABSENT("null_chain.outputs_cleared");
        }

        /* Teardown disposes exactly the candidate this block installed, once,
         * and leaves the recorder empty. macOS ASan ships no LeakSanitizer, so
         * process exit proves nothing here -- these counters do. */
        if (installed) {
            unsigned frees_pre_dispose = cand_frees;
            if (sq.dispose != NULL && sq.dispose != old_dispose)
                sq.dispose(sq.verifier);
            CHECK(cand_frees == frees_pre_dispose + 1,
                  "stateless.teardown_disposes_once");
            CHECK(cand_live_count() == live_before,
                  "stateless.teardown_none_live");
        } else {
            /* nothing of ours is installed: disposing here would call a
             * disposer for an object this block does not own */
            ROW_ABSENT("stateless.teardown_disposes_once");
            ROW_ABSENT("stateless.teardown_none_live");
        }
    }

    /* Exact block ledger: both blocks run unconditionally on this provider,
     * once each, in source order. Derived from the block set itself, not from
     * the loop that produces it. */
    {
        static const char *const want_blk[] = { "stateless_ctx", "null_chain" };
        const unsigned want_blk_n =
            (unsigned)(sizeof(want_blk) / sizeof(want_blk[0]));
        CHECK(g_block_overflow == 0, "block_ledger.no_overflow");
        if (g_block_n != want_blk_n)
            fprintf(stderr, "BLOCK LEDGER: %u blocks, expected %u\n",
                    g_block_n, want_blk_n);
        CHECK(g_block_n == want_blk_n, "block_ledger.exact_count");
        for (unsigned i = 0; i < want_blk_n; i++) {
            int ok = i < g_block_n && g_block_rows[i] != NULL &&
                     strcmp(g_block_rows[i], want_blk[i]) == 0;
            if (!ok)
                fprintf(stderr,
                        "BLOCK LEDGER: slot %u is '%s', expected '%s'\n", i,
                        (i < g_block_n && g_block_rows[i]) ? g_block_rows[i]
                                                           : "<none>",
                        want_blk[i]);
            CHECK(ok, "block_ledger.row_present");
        }
    }

    ptls_free(tls);
    if (quic.dispose != NULL && quic.dispose != old_dispose)
        quic.dispose(quic.verifier);
    OPENSSL_free(der);
    if (failures == 0) printf("picoquic_verify_callback: ok\n");
    return failures != 0;
}

#else   /* mbedTLS provider */

static moq_picoquic_cert_verdict_t unused_verify(
    void *ctx, moq_bytes_t name, const moq_bytes_t *certs, size_t n)
{
    (void)ctx; (void)name; (void)certs; (void)n;
    CHECK(0, "mbedtls.callback_must_never_run");
    return MOQ_PICOQUIC_CERT_REJECT;
}

int main(void)
{
    /* The classifier's operational-failure preservation is provider-independent. */
    CHECK(moq_picoquic_test_delegated_trust_result(PTLS_ERROR_NO_MEMORY) ==
              PTLS_ERROR_NO_MEMORY,
          "override.no_memory_preserved");
    CHECK(moq_picoquic_test_delegated_trust_result(PTLS_ERROR_LIBRARY) ==
              PTLS_ERROR_LIBRARY,
          "override.library_failure_preserved");

    CHECK(moq_picoquic_cert_verify_callback_supported() == 0,
          "supported.mbedtls_zero");

    /* Called with a VALID non-NULL callback and non-NULL context, so an
     * invalid-argument path cannot explain the refusal. */
    int ctx_marker = 0;
    picoquic_quic_t quic;
    seed_installed(&quic);
    if (!cand_reset(__func__)) return failures != 0;
    setter_calls = 0;
    CHECK(moq_picoquic_set_cert_verify_callback(&quic, unused_verify,
                                                &ctx_marker) == -1,
          "install.unsupported_refused");
    CHECK(setter_calls == 0, "install.unsupported_no_provider_setter");
    /* observed, not inferred: the helper allocated nothing and left nothing */
    CHECK(cand_allocs == 0, "install.unsupported_no_candidate");
    CHECK(cand_live_count() == 0, "install.unsupported_nothing_live");
    check_old_preserved(&quic, "install.unsupported");

    /* Anything the helper did leave live is released through the real
     * disposer and that cleanup is asserted balanced -- never reset away. */
    {
        unsigned before_frees = cand_frees;
        unsigned live_before = cand_live_count();
        test_release_live_candidates();
        /* Allocation balance alone cannot tell an exact-disposer release from a
         * raw free, so the disposer-call count is asserted independently. */
        CHECK(test_dispose_calls == live_before,
              "install.unsupported_cleanup_via_disposer");
        CHECK(cleanup_no_disposer == 0,
              "install.unsupported_cleanup_disposer_known");
        CHECK(cand_frees == before_frees + live_before,
              "install.unsupported_cleanup_balanced");
        CHECK(cand_live_count() == 0, "install.unsupported_cleanup_complete");
    }

    if (failures == 0) printf("picoquic_verify_callback: ok\n");
    return failures != 0;
}

#endif
