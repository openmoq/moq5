/*
 * Managed mvfst server negative/failure tests.
 *
 * Tests cert/key mismatch, stop with active client, and client
 * disconnect cleanup. Separate binary: one QuicServer per process.
 */

#include <moq/mvfst.h>
#include <moq/session.h>

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
    char cert_path[64] = "/tmp/moq_sneg_cert_XXXXXX";
    char key_path[64]  = "/tmp/moq_sneg_key_XXXXXX";
    bool ok = false;

    tmp_files(const std::string &cert_pem, const std::string &key_pem) {
        int cfd = mkstemp(cert_path);
        if (cfd < 0) return;
        ssize_t cw = write(cfd, cert_pem.data(), cert_pem.size());
        close(cfd);
        if (cw != static_cast<ssize_t>(cert_pem.size())) return;

        int kfd = mkstemp(key_path);
        if (kfd < 0) { unlink(cert_path); return; }
        ssize_t kw = write(kfd, key_pem.data(), key_pem.size());
        close(kfd);
        if (kw != static_cast<ssize_t>(key_pem.size())) {
            unlink(cert_path); return;
        }
        ok = true;
    }

    ~tmp_files() {
        if (ok) { unlink(cert_path); unlink(key_path); }
    }
};

/* -- Pump callbacks -------------------------------------------------- */

static int pump_noop(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)m; (void)now; (void)ctx;
    return 0;
}

struct setup_state {
    std::atomic<bool> setup_complete{false};
};

static int pump_setup_watch(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)now;
    auto *ss = static_cast<setup_state *>(ctx);

    /* Server: iterate connections. */
    moq_mvfst_conn_t *conn = nullptr;
    while ((conn = moq_mvfst_managed_next_conn(m, conn)) != nullptr) {
        moq_session_t *s = moq_mvfst_conn_session(conn);
        if (!s) continue;
        moq_event_t ev[16]; size_t ne;
        moq_session_poll_events_ex(s, ev, 16, sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_SETUP_COMPLETE)
                ss->setup_complete.store(true);
            moq_event_cleanup(&ev[i]);
        }
    }

    /* Client: single session. */
    moq_session_t *s = moq_mvfst_managed_session(m);
    if (s) {
        moq_event_t ev[16]; size_t ne;
        moq_session_poll_events_ex(s, ev, 16, sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_SETUP_COMPLETE)
                ss->setup_complete.store(true);
            moq_event_cleanup(&ev[i]);
        }
    }

    return 0;
}

/* ================================================================== */
/* Test 1: bad cert/key pairing                                       */
/* ================================================================== */

static void test_bad_cert_key_pairing()
{
    /* Generate two different certs with different keys. */
    auto cert1 = gen_server_cert();
    MVFST_CHECK(cert1.ok);
    if (!cert1.ok) return;

    auto cert2 = gen_server_cert();
    MVFST_CHECK(cert2.ok);
    if (!cert2.ok) return;

    /* Use cert from first, key from second. */
    tmp_files tf(cert1.cert_pem, cert2.key_pem);
    MVFST_CHECK(tf.ok);
    if (!tf.ok) return;

    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.port = 0;
    cfg.cert_path = tf.cert_path;
    cfg.key_path = tf.key_path;
    cfg.on_pump = pump_noop;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;

    moq_mvfst_managed_t *m = nullptr;
    moq_result_t rc = moq_mvfst_managed_create(&cfg, &m);
    MVFST_CHECK(rc == MOQ_ERR_INTERNAL);
    MVFST_CHECK(m == nullptr);
}

/* ================================================================== */
/* Test 2: stop with active client                                    */
/* ================================================================== */

static void test_stop_with_active_client()
{
    auto cm = gen_server_cert();
    MVFST_CHECK(cm.ok);
    if (!cm.ok) return;
    tmp_files tf(cm.cert_pem, cm.key_pem);
    MVFST_CHECK(tf.ok);
    if (!tf.ok) return;

    /* Start server. */
    setup_state srv_ss;
    moq_mvfst_managed_cfg_t scfg;
    moq_mvfst_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.port = 0;
    scfg.cert_path = tf.cert_path;
    scfg.key_path = tf.key_path;
    scfg.on_pump = pump_setup_watch;
    scfg.user_ctx = &srv_ss;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;

    moq_mvfst_managed_t *srv = nullptr;
    moq_result_t rc = moq_mvfst_managed_create(&scfg, &srv);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return;

    uint16_t port = moq_mvfst_managed_local_port(srv);
    MVFST_CHECK(port != 0);
    if (port == 0) { moq_mvfst_managed_destroy(srv); return; }

    /* Connect managed client (insecure). */
    setup_state cli_ss;
    moq_mvfst_managed_cfg_t ccfg;
    moq_mvfst_managed_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.host = "127.0.0.1";
    ccfg.port = port;
    ccfg.insecure_skip_verify = true;
    ccfg.on_pump = pump_setup_watch;
    ccfg.user_ctx = &cli_ss;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;
    ccfg.max_num_ptos = 2;
    ccfg.initial_rtt_us = 10000;

    moq_mvfst_managed_t *cli = nullptr;
    rc = moq_mvfst_managed_create(&ccfg, &cli);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) { moq_mvfst_managed_destroy(srv); return; }

    /* Wait for setup on both sides. */
    auto dl = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < dl) {
        moq_mvfst_managed_wake(srv);
        moq_mvfst_managed_wake(cli);
        moq_mvfst_managed_wait(cli, 50000);
        moq_mvfst_managed_wait(srv, 50000);
        if (cli_ss.setup_complete.load() && srv_ss.setup_complete.load())
            break;
    }
    MVFST_CHECK(cli_ss.setup_complete.load());
    MVFST_CHECK(srv_ss.setup_complete.load());

    /* Destroy server while client is connected. */
    moq_mvfst_managed_destroy(srv);

    /* Client should observe MOQ_ERR_CLOSED from wait() eventually. */
    dl = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    bool got_closed = false;
    while (std::chrono::steady_clock::now() < dl) {
        moq_mvfst_managed_wake(cli);
        moq_result_t wr = moq_mvfst_managed_wait(cli, 200000);
        if (wr == MOQ_ERR_CLOSED) { got_closed = true; break; }
        if (moq_mvfst_managed_is_fatal(cli)) { got_closed = true; break; }
    }
    MVFST_CHECK(got_closed);

    /* Clean client destroy — no crash. */
    moq_mvfst_managed_destroy(cli);
}

/* ================================================================== */
/* Test 3: client disconnect cleanup                                  */
/* ================================================================== */

static void test_client_disconnect_cleanup()
{
    auto cm = gen_server_cert();
    MVFST_CHECK(cm.ok);
    if (!cm.ok) return;
    tmp_files tf(cm.cert_pem, cm.key_pem);
    MVFST_CHECK(tf.ok);
    if (!tf.ok) return;

    /* Start server. */
    setup_state srv_ss;
    moq_mvfst_managed_cfg_t scfg;
    moq_mvfst_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.port = 0;
    scfg.cert_path = tf.cert_path;
    scfg.key_path = tf.key_path;
    scfg.on_pump = pump_setup_watch;
    scfg.user_ctx = &srv_ss;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;

    moq_mvfst_managed_t *srv = nullptr;
    moq_result_t rc = moq_mvfst_managed_create(&scfg, &srv);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return;

    uint16_t port = moq_mvfst_managed_local_port(srv);
    MVFST_CHECK(port != 0);
    if (port == 0) { moq_mvfst_managed_destroy(srv); return; }

    /* Connect managed client (insecure). */
    setup_state cli_ss;
    moq_mvfst_managed_cfg_t ccfg;
    moq_mvfst_managed_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.host = "127.0.0.1";
    ccfg.port = port;
    ccfg.insecure_skip_verify = true;
    ccfg.on_pump = pump_setup_watch;
    ccfg.user_ctx = &cli_ss;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;
    ccfg.max_num_ptos = 2;
    ccfg.initial_rtt_us = 10000;

    moq_mvfst_managed_t *cli = nullptr;
    rc = moq_mvfst_managed_create(&ccfg, &cli);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) { moq_mvfst_managed_destroy(srv); return; }

    /* Wait for setup and conn_count == 1. */
    auto dl = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < dl) {
        moq_mvfst_managed_wake(srv);
        moq_mvfst_managed_wake(cli);
        moq_mvfst_managed_wait(cli, 50000);
        moq_mvfst_managed_wait(srv, 50000);
        if (cli_ss.setup_complete.load() && srv_ss.setup_complete.load() &&
            moq_mvfst_managed_conn_count(srv) == 1)
            break;
    }
    MVFST_CHECK(cli_ss.setup_complete.load());
    MVFST_CHECK(srv_ss.setup_complete.load());
    MVFST_CHECK(moq_mvfst_managed_conn_count(srv) == 1);

    /* Destroy client. */
    moq_mvfst_managed_destroy(cli);

    /* Server conn_count should drop to 0 eventually. */
    dl = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < dl) {
        moq_mvfst_managed_wake(srv);
        moq_mvfst_managed_wait(srv, 200000);
        if (moq_mvfst_managed_conn_count(srv) == 0)
            break;
    }
    MVFST_CHECK(moq_mvfst_managed_conn_count(srv) == 0);

    /* Server remains non-fatal. */
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(srv));

    moq_mvfst_managed_destroy(srv);
}

/* ================================================================== */
/* Test 4: stop/destroy from server on_activity rejected              */
/* ================================================================== */

struct activity_ctx {
    std::atomic<moq_result_t> stop_result{MOQ_OK};
    std::atomic<bool> callback_ran{false};
};

static void activity_try_stop_server(moq_mvfst_managed_t *m, void *ctx)
{
    auto *ac = static_cast<activity_ctx *>(ctx);
    ac->stop_result.store(moq_mvfst_managed_stop(m));
    moq_mvfst_managed_destroy(m);
    ac->callback_ran.store(true);
}

static void test_stop_from_server_activity_rejected()
{
    auto cm = gen_server_cert();
    MVFST_CHECK(cm.ok);
    if (!cm.ok) return;
    tmp_files tf(cm.cert_pem, cm.key_pem);
    MVFST_CHECK(tf.ok);
    if (!tf.ok) return;

    activity_ctx ac;
    moq_mvfst_managed_cfg_t scfg;
    moq_mvfst_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.port = 0;
    scfg.cert_path = tf.cert_path;
    scfg.key_path = tf.key_path;
    scfg.on_pump = pump_noop;
    scfg.on_activity = activity_try_stop_server;
    scfg.user_ctx = &ac;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;

    moq_mvfst_managed_t *srv = nullptr;
    MVFST_CHECK(moq_mvfst_managed_create(&scfg, &srv) == MOQ_OK);
    if (!srv) return;

    for (int i = 0; i < 20; i++) {
        moq_mvfst_managed_wake(srv);
        moq_mvfst_managed_wait(srv, 50000);
        if (ac.callback_ran.load()) break;
    }
    MVFST_CHECK(ac.callback_ran.load());
    MVFST_CHECK(ac.stop_result.load() == MOQ_ERR_INVAL);
    moq_mvfst_managed_destroy(srv);
}

/* ================================================================== */
/* Main                                                               */
/* ================================================================== */

/*
 * Bind failure test is intentionally not included. It would require
 * two QuicServers in the same process (start A, read its port, try
 * to bind B to the same port). QuicServer shutdown is not fully
 * synchronous, so only one QuicServer can exist per process. A
 * cross-process bind conflict test would be non-deterministic and
 * racy on CI. The underlying bind failure path (socket EADDRINUSE)
 * is exercised by mvfst's own test suite.
 */

int main()
{
    test_bad_cert_key_pairing();
    test_stop_with_active_client();
    test_client_disconnect_cleanup();
    test_stop_from_server_activity_rejected();

    std::printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
