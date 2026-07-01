/*
 * Managed server GOAWAY drain timeout test.
 *
 * Proves adapter::impl::service_all() drives session timers:
 * server sends GOAWAY with a short drain timeout, the adapter
 * ticks the session when the deadline passes, and CLOSE_SESSION
 * fires without any manual moq_session_tick() call.
 *
 * Separate binary: one QuicServer per process.
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

/* -- Cert ------------------------------------------------------------ */

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
    char cert_path[64] = "/tmp/moq_ga_cert_XXXXXX";
    char key_path[64]  = "/tmp/moq_ga_key_XXXXXX";
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
    ~tmp_files() { if (ok) { unlink(cert_path); unlink(key_path); } }
};

/* -- Pumps ----------------------------------------------------------- */

struct server_state {
    std::atomic<bool> setup_complete{false};
    std::atomic<bool> goaway_sent{false};
    std::atomic<bool> may_goaway{false};
};

static int server_pump(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    auto *ss = static_cast<server_state *>(ctx);
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
        if (ss->may_goaway.load() && !ss->goaway_sent.load()) {
            moq_session_goaway(s, nullptr, 0, now);
            ss->goaway_sent.store(true);
        }
    }
    return 0;
}

struct client_state {
    std::atomic<bool> setup_complete{false};
};

static int client_pump(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)now;
    auto *cs = static_cast<client_state *>(ctx);
    moq_session_t *s = moq_mvfst_managed_session(m);
    if (!s) return 0;
    moq_event_t ev[16]; size_t ne;
    moq_session_poll_events_ex(s, ev, 16, sizeof(moq_event_t), &ne);
    for (size_t i = 0; i < ne; i++) {
        if (ev[i].kind == MOQ_EVENT_SETUP_COMPLETE)
            cs->setup_complete.store(true);
        moq_event_cleanup(&ev[i]);
    }
    return 0;
}

/* ================================================================== */

static void test_goaway_drain_timeout()
{
    auto cm = gen_cert();
    MVFST_CHECK(cm.ok);
    if (!cm.ok) return;
    tmp_files tf(cm);
    MVFST_CHECK(tf.ok);
    if (!tf.ok) return;

    /* Server with 50ms GOAWAY drain timeout. */
    server_state ss;
    moq_mvfst_managed_cfg_t scfg;
    moq_mvfst_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.port = 0;
    scfg.cert_path = tf.cert_path;
    scfg.key_path = tf.key_path;
    scfg.on_pump = server_pump;
    scfg.user_ctx = &ss;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    scfg.goaway_timeout_us = 50000;

    moq_mvfst_managed_t *srv = nullptr;
    MVFST_CHECK(moq_mvfst_managed_create(&scfg, &srv) == MOQ_OK);
    if (!srv) return;
    uint16_t port = moq_mvfst_managed_local_port(srv);
    MVFST_CHECK(port != 0);

    /* Client. */
    client_state cs;
    moq_mvfst_managed_cfg_t ccfg;
    moq_mvfst_managed_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.host = "127.0.0.1";
    ccfg.port = port;
    ccfg.insecure_skip_verify = true;
    ccfg.on_pump = client_pump;
    ccfg.user_ctx = &cs;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;

    moq_mvfst_managed_t *cli = nullptr;
    MVFST_CHECK(moq_mvfst_managed_create(&ccfg, &cli) == MOQ_OK);
    if (!cli) { moq_mvfst_managed_destroy(srv); return; }

    /* Wait for setup. */
    auto dl = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < dl) {
        moq_mvfst_managed_wake(srv);
        moq_mvfst_managed_wake(cli);
        moq_mvfst_managed_wait(cli, 50000);
        moq_mvfst_managed_wait(srv, 50000);
        if (cs.setup_complete.load() && ss.setup_complete.load()) break;
    }
    MVFST_CHECK(cs.setup_complete.load());
    MVFST_CHECK(ss.setup_complete.load());

    /* Signal server to send GOAWAY. */
    ss.may_goaway.store(true);
    moq_mvfst_managed_wake(srv);

    /* Wait for the drain timeout to fire. The adapter's service_all
     * calls moq_session_tick when next_deadline_us <= now. After the
     * 50ms drain timeout, the session emits CLOSE_SESSION. The server
     * conn should be removed. No manual moq_session_tick call. */
    dl = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < dl) {
        moq_mvfst_managed_wake(srv);
        moq_mvfst_managed_wait(srv, 50000);
        if (moq_mvfst_managed_conn_count(srv) == 0) break;
    }
    MVFST_CHECK(ss.goaway_sent.load());
    MVFST_CHECK(moq_mvfst_managed_conn_count(srv) == 0);
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(srv));

    moq_mvfst_managed_destroy(cli);
    moq_mvfst_managed_destroy(srv);
}

int main()
{
    test_goaway_drain_timeout();
    std::printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
