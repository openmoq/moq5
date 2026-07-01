/*
 * Managed mvfst server lifecycle test.
 *
 * Verifies that a managed server can create/start/stop/destroy,
 * that on_pump runs, and that local_port is set after bind.
 *
 * No client connections here — just listener lifecycle.
 *
 * Separate binary: one QuicServer per process.
 */

#include <moq/mvfst.h>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <folly/ssl/OpenSSLPtrTypes.h>

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>

static int failures = 0;

#define MVFST_CHECK(expr) do { \
    if (!(expr)) { \
        std::fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- Cert/key generation --------------------------------------------- */

struct test_cert { std::string cert_pem, key_pem; bool ok = false; };

static test_cert gen_server_cert()
{
    test_cert r;
    auto pkey = folly::ssl::EvpPkeyUniquePtr(EVP_PKEY_new());
    if (!pkey) return r;
    auto *ec = EC_KEY_new_by_curve_name(NID_X9_62_prime256v1);
    if (!ec) return r;
    if (!EC_KEY_generate_key(ec)) { EC_KEY_free(ec); return r; }
    if (!EVP_PKEY_assign_EC_KEY(pkey.get(), ec)) { EC_KEY_free(ec); return r; }
    auto x509 = folly::ssl::X509UniquePtr(X509_new());
    if (!x509) return r;
    if (!ASN1_INTEGER_set(X509_get_serialNumber(x509.get()), 1)) return r;
    if (!X509_gmtime_adj(X509_get_notBefore(x509.get()), 0)) return r;
    if (!X509_gmtime_adj(X509_get_notAfter(x509.get()), 365*24*3600)) return r;
    if (!X509_set_pubkey(x509.get(), pkey.get())) return r;
    auto *name = X509_get_subject_name(x509.get());
    if (!X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
            reinterpret_cast<const unsigned char*>("localhost"),
            -1, -1, 0)) return r;
    if (!X509_set_issuer_name(x509.get(), name)) return r;
    X509V3_CTX v3ctx;
    X509V3_set_ctx_nodb(&v3ctx);
    X509V3_set_ctx(&v3ctx, x509.get(), x509.get(), nullptr, nullptr, 0);
    auto *bc = X509V3_EXT_conf_nid(nullptr, &v3ctx,
                                    NID_basic_constraints, "CA:TRUE");
    if (!bc) return r;
    if (!X509_add_ext(x509.get(), bc, -1)) { X509_EXTENSION_free(bc); return r; }
    X509_EXTENSION_free(bc);
    auto *san = X509V3_EXT_conf_nid(nullptr, &v3ctx,
                                     NID_subject_alt_name,
                                     "DNS:localhost,IP:127.0.0.1");
    if (!san) return r;
    if (!X509_add_ext(x509.get(), san, -1)) { X509_EXTENSION_free(san); return r; }
    X509_EXTENSION_free(san);
    if (!X509_sign(x509.get(), pkey.get(), EVP_sha256())) return r;
    auto cb = folly::ssl::BioUniquePtr(BIO_new(BIO_s_mem()));
    if (!cb || !PEM_write_bio_X509(cb.get(), x509.get())) return r;
    char *cd = nullptr; auto cl = BIO_get_mem_data(cb.get(), &cd);
    if (cl <= 0 || !cd) return r;
    r.cert_pem.assign(cd, cl);
    auto kb = folly::ssl::BioUniquePtr(BIO_new(BIO_s_mem()));
    if (!kb || !PEM_write_bio_PrivateKey(kb.get(), pkey.get(),
            nullptr, nullptr, 0, nullptr, nullptr)) return r;
    char *kd = nullptr; auto kl = BIO_get_mem_data(kb.get(), &kd);
    if (kl <= 0 || !kd) return r;
    r.key_pem.assign(kd, kl);
    r.ok = true;
    return r;
}

struct tmp_files {
    char cert_path[64] = "/tmp/moq_srv_cert_XXXXXX";
    char key_path[64]  = "/tmp/moq_srv_key_XXXXXX";
    bool ok = false;

    tmp_files(const test_cert &cm) {
        int cfd = mkstemp(cert_path);
        if (cfd < 0) return;
        ssize_t cw = write(cfd, cm.cert_pem.data(), cm.cert_pem.size());
        close(cfd);
        if (cw != static_cast<ssize_t>(cm.cert_pem.size())) return;

        int kfd = mkstemp(key_path);
        if (kfd < 0) { unlink(cert_path); return; }
        ssize_t kw = write(kfd, cm.key_pem.data(), cm.key_pem.size());
        close(kfd);
        if (kw != static_cast<ssize_t>(cm.key_pem.size())) {
            unlink(cert_path); return;
        }
        ok = true;
    }

    ~tmp_files() {
        if (ok) { unlink(cert_path); unlink(key_path); }
    }
};

/* -- Pump callbacks -------------------------------------------------- */

static std::atomic<int> pump_count{0};

static int pump_counting(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)m; (void)now; (void)ctx;
    pump_count.fetch_add(1);
    return (pump_count.load() >= 3) ? 1 : 0;
}

/* ================================================================== */
/* Test: server create/stop/destroy with ephemeral port               */
/* ================================================================== */

static void test_server_lifecycle()
{
    auto cm = gen_server_cert();
    MVFST_CHECK(cm.ok);
    if (!cm.ok) return;
    tmp_files tf(cm);
    MVFST_CHECK(tf.ok);
    if (!tf.ok) return;

    pump_count.store(0);

    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init(&cfg);
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.port = 0;
    cfg.cert_path = tf.cert_path;
    cfg.key_path = tf.key_path;
    cfg.on_pump = pump_counting;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;

    moq_mvfst_managed_t *m = nullptr;
    moq_result_t rc = moq_mvfst_managed_create(&cfg, &m);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return;

    MVFST_CHECK(moq_mvfst_managed_local_port(m) != 0);
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(m));
    MVFST_CHECK(moq_mvfst_managed_session(m) == nullptr);
    MVFST_CHECK(moq_mvfst_managed_conn_count(m) == 0);
    MVFST_CHECK(moq_mvfst_managed_next_conn(m, nullptr) == nullptr);

    auto dl = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < dl) {
        moq_mvfst_managed_wake(m);
        moq_result_t wr = moq_mvfst_managed_wait(m, 100000);
        if (wr == MOQ_ERR_CLOSED) break;
        if (pump_count.load() >= 3) break;
    }

    MVFST_CHECK(pump_count.load() >= 3);
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(m));

    moq_mvfst_managed_destroy(m);

    /* Server stop idempotency: stop, stop, destroy. */
    pump_count.store(0);
    cfg.on_pump = pump_counting;
    m = nullptr;
    rc = moq_mvfst_managed_create(&cfg, &m);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc == MOQ_OK) {
        MVFST_CHECK(moq_mvfst_managed_stop(m) == MOQ_OK);
        MVFST_CHECK(moq_mvfst_managed_stop(m) == MOQ_OK);
        moq_mvfst_managed_destroy(m);
    }
}

/* ================================================================== */
/* Test: server with bad cert/key content                             */
/* ================================================================== */

static int pump_noop(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)m; (void)now; (void)ctx;
    return 0;
}

static void test_server_bad_cert_content()
{
    char cert_path[] = "/tmp/moq_bad_cert_XXXXXX";
    int cfd = mkstemp(cert_path);
    MVFST_CHECK(cfd >= 0);
    if (cfd < 0) return;
    const char *bad = "not a PEM cert\n";
    write(cfd, bad, strlen(bad));
    close(cfd);

    char key_path[] = "/tmp/moq_bad_key_XXXXXX";
    int kfd = mkstemp(key_path);
    MVFST_CHECK(kfd >= 0);
    if (kfd < 0) { unlink(cert_path); return; }
    write(kfd, bad, strlen(bad));
    close(kfd);

    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init(&cfg);
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.port = 0;
    cfg.cert_path = cert_path;
    cfg.key_path = key_path;
    cfg.on_pump = pump_noop;

    moq_mvfst_managed_t *m = nullptr;
    moq_result_t rc = moq_mvfst_managed_create(&cfg, &m);
    MVFST_CHECK(rc == MOQ_ERR_INTERNAL);
    MVFST_CHECK(m == nullptr);

    unlink(cert_path);
    unlink(key_path);
}

int main()
{
    test_server_lifecycle();
    test_server_bad_cert_content();
    std::printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
