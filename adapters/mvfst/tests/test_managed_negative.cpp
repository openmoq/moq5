/*
 * Managed mvfst client negative/failure tests.
 *
 * Tests connection failure, TLS verification failure, and
 * stop-during-connect behavior. Uses short PTO timeouts to
 * avoid long waits on unreachable endpoints.
 */

#include <moq/mvfst.h>
#include <moq/mvfst.hpp>
#include <moq/session.h>

#include <quic/QuicConstants.h>
#include <quic/server/QuicServer.h>
#include <quic/server/QuicServerTransport.h>

#include <fizz/backend/openssl/certificate/CertUtils.h>
#include <fizz/protocol/CertificateVerifier.h>
#include <fizz/server/DefaultCertManager.h>
#include <fizz/server/FizzServerContext.h>

#include <folly/io/async/EventBase.h>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>

static int failures = 0;

#define MVFST_CHECK(expr) do { \
    if (!(expr)) { \
        std::fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- Cert generation ------------------------------------------------- */

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

/* -- Minimal server for TLS test ------------------------------------- */

class noop_cb
    : public quic::QuicSocket::ConnectionSetupCallback
    , public quic::QuicSocket::ConnectionCallback
{
public:
    void onTransportReady() noexcept override {}
    void onConnectionSetupError(quic::QuicError) noexcept override {}
    void onReplaySafe() noexcept override {}
    void onFullHandshakeDone() noexcept override {}
    void onNewBidirectionalStream(quic::StreamId) noexcept override {}
    void onNewUnidirectionalStream(quic::StreamId) noexcept override {}
    void onStopSending(quic::StreamId, quic::ApplicationErrorCode) noexcept override {}
    void onConnectionEnd() noexcept override {}
    void onConnectionError(quic::QuicError) noexcept override {}
};

class noop_factory : public quic::QuicServerTransportFactory {
    noop_cb cb_;
public:
    quic::QuicServerTransport::Ptr make(
        folly::EventBase *evb,
        std::unique_ptr<quic::FollyAsyncUDPSocketAlias> sock,
        const folly::SocketAddress &, quic::QuicVersion,
        std::shared_ptr<const fizz::server::FizzServerContext> ctx) noexcept override
    {
        return quic::QuicServerTransport::make(
            evb, std::move(sock), &cb_, &cb_, std::move(ctx));
    }
};

/* -- Pump that does nothing ------------------------------------------ */

static int pump_noop(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)m; (void)now; (void)ctx;
    return 0;
}

/* -- Helpers --------------------------------------------------------- */

static bool get_closed_local_udp_port(int *out_port)
{
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) return false;
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(sock); return false;
    }
    socklen_t alen = sizeof(addr);
    if (getsockname(sock, (struct sockaddr *)&addr, &alen) != 0) {
        close(sock); return false;
    }
    *out_port = ntohs(addr.sin_port);
    close(sock);
    return true;
}

static moq_mvfst_managed_cfg_t make_fast_cfg(const char *host, int port)
{
    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = host;
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.on_pump = pump_noop;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_num_ptos = 2;
    cfg.initial_rtt_us = 10000; /* 10ms */
    return cfg;
}

/* ================================================================== */
/* Test 1: unreachable local port                                     */
/* ================================================================== */

static void test_unreachable_port()
{
    int port = 0;
    MVFST_CHECK(get_closed_local_udp_port(&port));
    if (port == 0) return;

    auto cfg = make_fast_cfg("127.0.0.1", port);
    moq_mvfst_managed_t *m = nullptr;
    moq_result_t rc = moq_mvfst_managed_create(&cfg, &m);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return;

    /* Wait for fatal or closed — bounded by PTO timeout. */
    auto dl = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < dl) {
        moq_result_t wr = moq_mvfst_managed_wait(m, 200000);
        if (wr == MOQ_ERR_CLOSED) break;
        if (moq_mvfst_managed_is_fatal(m)) break;
    }

    MVFST_CHECK(moq_mvfst_managed_is_fatal(m));
    MVFST_CHECK(moq_mvfst_managed_fatal_code(m) != 0);
    MVFST_CHECK(moq_mvfst_managed_wait(m, 0) == MOQ_ERR_CLOSED);

    moq_mvfst_managed_destroy(m);
}

/* ================================================================== */
/* Test 2: TLS verification failure                                   */
/* ================================================================== */

static void test_tls_verify_failure()
{
    auto cm = gen_cert();
    MVFST_CHECK(cm.ok);
    if (!cm.ok) return;

    auto sctx = std::make_shared<fizz::server::FizzServerContext>();
    sctx->setSupportedAlpns({"moqt-16"});
    auto mgr = std::make_shared<fizz::server::DefaultCertManager>();
    std::unique_ptr<fizz::SelfCert> sc;
    fizz::Error err;
    auto st = fizz::openssl::CertUtils::makeSelfCert(
        sc, err, cm.cert_pem, cm.key_pem);
    MVFST_CHECK(st == fizz::Status::Success && sc);
    if (st != fizz::Status::Success || !sc) return;
    mgr->addCertAndSetDefault(
        std::shared_ptr<fizz::SelfCert>(std::move(sc)));
    sctx->setCertManager(mgr);

    auto server = quic::QuicServer::createQuicServer();
    server->setFizzContext(sctx);
    server->setQuicServerTransportFactory(
        std::make_unique<noop_factory>());
    folly::SocketAddress addr("127.0.0.1", 0);
    server->start(addr, 1);
    server->waitUntilInitialized();
    int port = server->getAddress().getPort();

    /* Client with insecure_skip_verify=false → TLS should fail. */
    auto cfg = make_fast_cfg("127.0.0.1", port);
    cfg.insecure_skip_verify = false;
    moq_mvfst_managed_t *m = nullptr;
    moq_result_t rc = moq_mvfst_managed_create(&cfg, &m);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) {
        server->shutdown();
        return;
    }

    auto dl = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < dl) {
        moq_result_t wr = moq_mvfst_managed_wait(m, 200000);
        if (wr == MOQ_ERR_CLOSED) break;
        if (moq_mvfst_managed_is_fatal(m)) break;
    }

    MVFST_CHECK(moq_mvfst_managed_is_fatal(m));
    MVFST_CHECK(moq_mvfst_managed_fatal_code(m) != 0);
    MVFST_CHECK(moq_mvfst_managed_wait(m, 0) == MOQ_ERR_CLOSED);

    moq_mvfst_managed_destroy(m);
    server->shutdown();
}

/* ================================================================== */
/* Test 3: invalid cert_path                                          */
/* ================================================================== */

static void test_invalid_cert_path()
{
    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = 4433;
    cfg.insecure_skip_verify = false;
    cfg.cert_path = "/nonexistent/path/moq_test_cert.pem";
    cfg.on_pump = pump_noop;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;

    moq_mvfst_managed_t *m = nullptr;
    moq_result_t rc = moq_mvfst_managed_create(&cfg, &m);
    MVFST_CHECK(rc == MOQ_ERR_INVAL);
    MVFST_CHECK(m == nullptr);
}

/* ================================================================== */
/* Test 4: insecure + cert_path mutually exclusive                    */
/* ================================================================== */

static void test_insecure_plus_cert_path()
{
    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = 4433;
    cfg.insecure_skip_verify = true;
    cfg.cert_path = "/etc/ssl/cert.pem";
    cfg.on_pump = pump_noop;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;

    moq_mvfst_managed_t *m = nullptr;
    moq_result_t rc = moq_mvfst_managed_create(&cfg, &m);
    MVFST_CHECK(rc == MOQ_ERR_INVAL);
    MVFST_CHECK(m == nullptr);
}

/* ================================================================== */
/* Test 5: DNS resolution failure                                     */
/* ================================================================== */

static void test_dns_resolution_failure()
{
    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "this-host-does-not-exist.invalid";
    cfg.port = 4433;
    cfg.insecure_skip_verify = true;
    cfg.on_pump = pump_noop;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;

    moq_mvfst_managed_t *m = nullptr;
    moq_result_t rc = moq_mvfst_managed_create(&cfg, &m);
    MVFST_CHECK(rc == MOQ_ERR_INTERNAL);
    MVFST_CHECK(m == nullptr);
}

/* ================================================================== */
/* Test 6: stop during connection attempt                             */
/* ================================================================== */

static void test_stop_during_connect()
{
    int port = 0;
    MVFST_CHECK(get_closed_local_udp_port(&port));
    if (port == 0) return;

    auto cfg = make_fast_cfg("127.0.0.1", port);
    moq_mvfst_managed_t *m = nullptr;
    moq_result_t rc = moq_mvfst_managed_create(&cfg, &m);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return;

    /* Immediately stop — must return promptly, not hang. */
    auto t0 = std::chrono::steady_clock::now();
    moq_mvfst_managed_stop(m);
    auto elapsed = std::chrono::steady_clock::now() - t0;
    MVFST_CHECK(elapsed < std::chrono::seconds(3));

    MVFST_CHECK(moq_mvfst_managed_wait(m, 0) == MOQ_ERR_CLOSED);

    moq_mvfst_managed_destroy(m);
}

/* ================================================================== */
/* Main                                                               */
/* ================================================================== */

int main()
{
    test_unreachable_port();
    test_tls_verify_failure();
    test_invalid_cert_path();
    test_insecure_plus_cert_path();
    test_dns_resolution_failure();
    test_stop_during_connect();

    std::printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
