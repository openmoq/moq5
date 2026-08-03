/*
 * Real-transport idle app-deadline integration for the managed mvfst facade:
 * that each distinct EventBase one-shot timer actually FIRES from the folded
 * application deadline and reschedules — the client deadline_to and the server
 * server_deadline_to — not merely that compute_earliest_deadline() returns the
 * folded value (which test_managed_app_deadline.cpp proves deterministically).
 *
 * A cached, self-advancing deadline drives the pump: the app callback returns a
 * cached absolute target; on_lane_pump counts ONLY pumps where now >= target
 * (i.e. deadline-driven, not raw transport-driven), and advances the target one
 * interval out. After the facade is up, NO manual wakes are issued — so on an
 * idle facade whose session has no deadline, the app deadline is the sole
 * possible wake source. Each generation therefore requires the facade's
 * AsyncTimeout to have armed from the folded deadline and fired; requiring
 * several generations in a generous window confirms the timer both fires and
 * reschedules itself.
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
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>

static int g_fail = 0;
#define MVFST_CHECK(e) do { \
    if (!(e)) { std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #e); \
                g_fail++; } } while (0)

/* --- self-advancing app deadline + deadline-only pump counter ------------- */

static constexpr uint64_t INTERVAL_US = 40000;   /* 40 ms per generation */
static std::atomic<uint64_t> g_target{UINT64_MAX};   /* UINT64_MAX = disarmed */
static std::atomic<int>      g_generations{0};
static std::atomic<bool>     g_armed{false};

static void arm_reset()
{
    g_target.store(UINT64_MAX);
    g_generations.store(0);
    g_armed.store(true);
}

/* Pure cached read of the current absolute deadline (the app_deadline contract). */
static uint64_t app_cb(void *) { return g_target.load(std::memory_order_relaxed); }

/* Observed facade's pump: seed the cached deadline on the first pass, then count
 * ONLY the pumps that fired at/after it (a deadline generation) and advance it. */
static int deadline_pump(moq_mvfst_managed_t *, moq_mvfst_managed_lane_t *,
                         uint64_t now, void *)
{
    if (!g_armed.load(std::memory_order_relaxed)) return 0;
    uint64_t t = g_target.load(std::memory_order_relaxed);
    if (t == UINT64_MAX) {
        g_target.store(now + INTERVAL_US, std::memory_order_relaxed); /* seed; not counted */
    } else if (now >= t) {
        g_generations.fetch_add(1, std::memory_order_relaxed);
        g_target.store(now + INTERVAL_US, std::memory_order_relaxed);
    }
    return 0;
}

static int plain_pump(moq_mvfst_managed_t *, moq_mvfst_managed_lane_t *,
                      uint64_t, void *) { return 0; }

/* --- self-signed cert + temp files ---------------------------------------- */

struct test_cert { std::string cert_pem, key_pem; bool ok = false; };

static test_cert gen_cert()
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
            reinterpret_cast<const unsigned char*>("localhost"), -1, -1, 0)) return r;
    if (!X509_set_issuer_name(x509.get(), name)) return r;
    X509V3_CTX v3ctx;
    X509V3_set_ctx_nodb(&v3ctx);
    X509V3_set_ctx(&v3ctx, x509.get(), x509.get(), nullptr, nullptr, 0);
    auto *bc = X509V3_EXT_conf_nid(nullptr, &v3ctx, NID_basic_constraints, "CA:TRUE");
    if (!bc) return r;
    if (!X509_add_ext(x509.get(), bc, -1)) { X509_EXTENSION_free(bc); return r; }
    X509_EXTENSION_free(bc);
    auto *san = X509V3_EXT_conf_nid(nullptr, &v3ctx, NID_subject_alt_name,
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
    char cert_path[64] = "/tmp/moq_adl_cert_XXXXXX";
    char key_path[64]  = "/tmp/moq_adl_key_XXXXXX";
    bool cert_created = false, key_created = false, ok = false;
    tmp_files(const test_cert &cm) {
        int cfd = mkstemp(cert_path);
        if (cfd < 0) return;
        cert_created = true;
        ssize_t cw = write(cfd, cm.cert_pem.data(), cm.cert_pem.size());
        close(cfd);
        if (cw != static_cast<ssize_t>(cm.cert_pem.size())) return;
        int kfd = mkstemp(key_path);
        if (kfd < 0) return;
        key_created = true;
        ssize_t kw = write(kfd, cm.key_pem.data(), cm.key_pem.size());
        close(kfd);
        if (kw != static_cast<ssize_t>(cm.key_pem.size())) return;
        ok = true;
    }
    ~tmp_files() {
        if (cert_created) unlink(cert_path);
        if (key_created) unlink(key_path);
    }
};

static void server_cfg(moq_mvfst_managed_cfg_t *cfg, const tmp_files &tf,
                       moq_mvfst_lane_pump_fn pump, bool with_deadline)
{
    moq_mvfst_managed_cfg_init_sized(cfg, sizeof(*cfg));
    cfg->alloc = moq_alloc_default();
    cfg->perspective = MOQ_PERSPECTIVE_SERVER;
    cfg->port = 0;
    cfg->cert_path = tf.cert_path;
    cfg->key_path = tf.key_path;
    cfg->on_lane_pump = pump;
    cfg->send_request_capacity = true;
    cfg->initial_request_capacity = 16;
    if (with_deadline) { cfg->app_deadline_us = app_cb; cfg->app_deadline_ctx = nullptr; }
}

/* Generations must accrue over a generous window with NO manual wakes. */
static bool await_generations(int need, int window_ms)
{
    for (int i = 0; i < window_ms; i += 25) {
        if (g_generations.load() >= need) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return g_generations.load() >= need;
}

/* Client EventBase (deadline_to): an idle connected client with no session
 * deadline wakes solely from its app deadline. */
static void test_client_timer()
{
    int before = g_fail;
    test_cert cm = gen_cert();
    MVFST_CHECK(cm.ok);
    if (!cm.ok) return;
    tmp_files tf(cm);
    MVFST_CHECK(tf.ok);
    if (!tf.ok) return;

    /* Plain server (no app deadline) for the client to connect to. */
    moq_mvfst_managed_cfg_t scfg;
    server_cfg(&scfg, tf, plain_pump, /*with_deadline=*/false);
    moq_mvfst_managed_t *srv = nullptr;
    MVFST_CHECK(moq_mvfst_managed_create(&scfg, &srv) == MOQ_OK);
    if (!srv) return;
    uint16_t port = moq_mvfst_managed_local_port(srv);
    MVFST_CHECK(port != 0);

    arm_reset();   /* arm the app deadline BEFORE the client's first pump */

    moq_mvfst_managed_cfg_t ccfg;
    moq_mvfst_managed_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = moq_alloc_default();
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.host = "127.0.0.1";
    ccfg.port = port;
    ccfg.cert_path = tf.cert_path;
    ccfg.on_lane_pump = deadline_pump;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;
    ccfg.app_deadline_us = app_cb;
    ccfg.app_deadline_ctx = nullptr;
    moq_mvfst_managed_t *cli = nullptr;
    MVFST_CHECK(moq_mvfst_managed_create(&ccfg, &cli) == MOQ_OK);

    if (cli) {
        /* No manual wakes: the client's own EventBase connects, and the initial
         * pump arms deadline_to from the app deadline, which self-sustains. */
        MVFST_CHECK(await_generations(6, 3000));   /* several generations */
        MVFST_CHECK(!moq_mvfst_managed_is_fatal(cli));
        g_armed.store(false);
        moq_mvfst_managed_stop(cli);
        moq_mvfst_managed_destroy(cli);
    }
    g_armed.store(false);
    moq_mvfst_managed_stop(srv);
    moq_mvfst_managed_destroy(srv);
    if (g_fail == before) std::printf("PASS: app_deadline_client_timer\n");
}

/* Server EventBase (server_deadline_to): an idle server facade wakes solely from
 * its app deadline on the QuicServer worker EventBase. */
static void test_server_timer()
{
    int before = g_fail;
    test_cert cm = gen_cert();
    MVFST_CHECK(cm.ok);
    if (!cm.ok) return;
    tmp_files tf(cm);
    MVFST_CHECK(tf.ok);
    if (!tf.ok) return;

    arm_reset();
    moq_mvfst_managed_cfg_t scfg;
    server_cfg(&scfg, tf, deadline_pump, /*with_deadline=*/true);
    moq_mvfst_managed_t *srv = nullptr;
    MVFST_CHECK(moq_mvfst_managed_create(&scfg, &srv) == MOQ_OK);
    if (srv) {
        MVFST_CHECK(await_generations(6, 3000));
        MVFST_CHECK(!moq_mvfst_managed_is_fatal(srv));
        g_armed.store(false);
        moq_mvfst_managed_stop(srv);
        moq_mvfst_managed_destroy(srv);
    }
    g_armed.store(false);
    if (g_fail == before) std::printf("PASS: app_deadline_server_timer\n");
}

int main()
{
    test_client_timer();
    test_server_timer();

    if (g_fail == 0) { std::printf("PASS: mvfst_managed_app_deadline_timer\n"); return 0; }
    std::fprintf(stderr, "%d failure(s)\n", g_fail);
    return 1;
}
