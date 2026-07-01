/*
 * Managed client DNS hostname identity mismatch test.
 *
 * Verifies that a trusted cert with wrong DNS SAN causes the
 * managed client to go fatal when connecting via "localhost".
 *
 * Separate binary: one QuicServer per process.
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
#include <openssl/x509v3.h>

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

/* -- Cert with wrong DNS SAN ----------------------------------------- */

struct test_cert { std::string cert_pem, key_pem; bool ok = false; };

static test_cert gen_cert_wrong_dns()
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
            reinterpret_cast<const unsigned char*>("wrong.example.com"),
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
                                     "DNS:wrong.example.com,IP:127.0.0.1");
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

/* -- Noop pump ------------------------------------------------------- */

static int pump_noop(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)m; (void)now; (void)ctx;
    return 0;
}

/* -- Noop server ----------------------------------------------------- */

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

/* ================================================================== */
/* Test: DNS identity mismatch — trusted cert, wrong DNS SAN          */
/* ================================================================== */

static void test_dns_identity_mismatch()
{
    auto cm = gen_cert_wrong_dns();
    MVFST_CHECK(cm.ok);
    if (!cm.ok) return;

    char tmppath[] = "/tmp/moq_test_dns_mm_XXXXXX";
    int tmpfd = mkstemp(tmppath);
    MVFST_CHECK(tmpfd >= 0);
    if (tmpfd < 0) return;
    ssize_t wr = write(tmpfd, cm.cert_pem.data(), cm.cert_pem.size());
    close(tmpfd);
    MVFST_CHECK(wr == static_cast<ssize_t>(cm.cert_pem.size()));
    if (wr != static_cast<ssize_t>(cm.cert_pem.size())) {
        unlink(tmppath); return;
    }

    auto sctx = std::make_shared<fizz::server::FizzServerContext>();
    sctx->setSupportedAlpns({"moqt-16"});
    auto mgr = std::make_shared<fizz::server::DefaultCertManager>();
    std::unique_ptr<fizz::SelfCert> sc;
    fizz::Error err;
    auto st = fizz::openssl::CertUtils::makeSelfCert(
        sc, err, cm.cert_pem, cm.key_pem);
    MVFST_CHECK(st == fizz::Status::Success && sc);
    if (st != fizz::Status::Success || !sc) { unlink(tmppath); return; }
    mgr->addCertAndSetDefault(
        std::shared_ptr<fizz::SelfCert>(std::move(sc)));
    sctx->setCertManager(mgr);

    auto server = quic::QuicServer::createQuicServer();
    server->setFizzContext(sctx);
    server->setQuicServerTransportFactory(
        std::make_unique<noop_factory>());
    folly::SocketAddress addr("localhost", 0, true);
    server->start(addr, 1);
    server->waitUntilInitialized();
    int port = server->getAddress().getPort();

    /*
     * Connect via "localhost" — cert has DNS:wrong.example.com
     * (not DNS:localhost), so identity check must reject.
     * Cert does have IP:127.0.0.1 but we're connecting by name,
     * so the identity verifier checks against "localhost" not
     * the resolved IP.
     */
    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "localhost";
    cfg.port = port;
    cfg.insecure_skip_verify = false;
    cfg.cert_path = tmppath;
    cfg.on_pump = pump_noop;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_num_ptos = 2;
    cfg.initial_rtt_us = 10000;

    moq_mvfst_managed_t *m = nullptr;
    moq_result_t rc = moq_mvfst_managed_create(&cfg, &m);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) {
        server->shutdown(); unlink(tmppath); return;
    }

    auto dl = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < dl) {
        moq_result_t wr2 = moq_mvfst_managed_wait(m, 200000);
        if (wr2 == MOQ_ERR_CLOSED) break;
        if (moq_mvfst_managed_is_fatal(m)) break;
    }

    MVFST_CHECK(moq_mvfst_managed_is_fatal(m));
    MVFST_CHECK(moq_mvfst_managed_wait(m, 0) == MOQ_ERR_CLOSED);

    moq_mvfst_managed_destroy(m);
    server->shutdown();
    unlink(tmppath);
}

int main()
{
    test_dns_identity_mismatch();
    std::printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
