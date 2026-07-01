#ifndef MOQ_MVFST_LOOPBACK_PAIR_HPP
#define MOQ_MVFST_LOOPBACK_PAIR_HPP

/*
 * MvfstPair: real mvfst client/server loopback harness.
 *
 * Starts a QuicServer on 127.0.0.1:0 with a runtime self-signed
 * cert, connects a QuicClientTransport, and attaches MOQ adapters
 * on both sides.
 *
 * Thread confinement:
 *   Server session/adapter run on the QuicServer worker EventBase.
 *   Client session/adapter run on the test thread's EventBase.
 *   run_on_server() uses runInEventBaseThreadAndWait.
 *
 * Single env per process: QuicServer shutdown is not fully
 * synchronous, so only one loopback_pair per process.
 */

#include <moq/mvfst.hpp>
#include <moq/session.h>
#include <moq/rcbuf.h>

#include <quic/QuicConstants.h>
#include <quic/client/QuicClientTransport.h>
#include <quic/common/events/FollyQuicEventBase.h>
#include <quic/common/udpsocket/FollyQuicAsyncUDPSocket.h>
#include <quic/fizz/client/handshake/FizzClientQuicHandshakeContext.h>
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

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace moq::mvfst::test {

/* -- Cert generation ------------------------------------------------- */

struct test_cert { std::string cert_pem, key_pem; bool ok = false; };

inline test_cert generate_self_signed_cert()
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

/* -- Server state ---------------------------------------------------- */

struct server_state {
    std::shared_ptr<quic::QuicServerTransport> transport;
    moq_session_t *session = nullptr;
    std::unique_ptr<moq::mvfst::adapter> adp;
    std::atomic<folly::EventBase *> evb{nullptr};

    std::atomic<bool> setup_complete{false};
    std::atomic<bool> sub_accepted{false};
    std::atomic<bool> ns_accepted{false};
    std::atomic<bool> ns_done{false};
    std::atomic<bool> error{false};
    std::atomic<bool> adapter_fatal{false};
    moq_subscription_t sub_handle = {};
    moq_announcement_t ns_handle = {};

    std::string expect_sub_ns1;
    std::string expect_sub_ns2;
    std::string expect_sub_track;

    /* MoQ wire version for the server session (0 = draft-16 default).
     * Draft-18 is a symmetric handshake: the server starts too. */
    moq_version_t version = (moq_version_t)0;
};

/* -- Server callbacks ------------------------------------------------ */

class server_callback
    : public quic::QuicSocket::ConnectionSetupCallback
    , public quic::QuicSocket::ConnectionCallback
{
public:
    explicit server_callback(server_state &ss) : ss_(ss) {}

    void onTransportReady() noexcept override {
        try {
            moq_session_cfg_t cfg;
            moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
            cfg.send_request_capacity = true;
            cfg.initial_request_capacity = 16;
            cfg.version = ss_.version;
            if (moq_session_create(&cfg, 0, &ss_.session) < 0) {
                ss_.error.store(true); return;
            }
            ss_.adp = std::make_unique<moq::mvfst::adapter>(
                moq::mvfst::adapter::config::server(),
                ss_.session, ss_.transport);
            /* Draft-18: symmetric handshake -- the server opens its own
             * unidirectional control channel and sends SETUP too. The
             * pending actions drain on the next pump_server service. */
            if (ss_.version == MOQ_VERSION_DRAFT_18 &&
                moq_session_start(ss_.session, 0) < 0) {
                ss_.error.store(true); return;
            }
        } catch (...) {
            if (ss_.session) {
                moq_session_destroy(ss_.session);
                ss_.session = nullptr;
            }
            if (ss_.transport) {
                ss_.transport->close(
                    quic::QuicError(quic::ApplicationErrorCode(0)));
            }
            ss_.error.store(true);
        }
    }
    void onConnectionSetupError(quic::QuicError) noexcept override {}
    void onReplaySafe() noexcept override {}
    void onFullHandshakeDone() noexcept override {}
    void onNewBidirectionalStream(quic::StreamId) noexcept override {}
    void onNewUnidirectionalStream(quic::StreamId) noexcept override {}
    void onStopSending(quic::StreamId, quic::ApplicationErrorCode) noexcept override {}
    void onConnectionEnd() noexcept override {}
    void onConnectionError(quic::QuicError) noexcept override {}
private:
    server_state &ss_;
};

class server_transport_factory : public quic::QuicServerTransportFactory {
public:
    server_transport_factory(server_state &ss, server_callback &cb)
        : ss_(ss), cb_(cb) {}
    quic::QuicServerTransport::Ptr make(
        folly::EventBase *evb,
        std::unique_ptr<quic::FollyAsyncUDPSocketAlias> sock,
        const folly::SocketAddress &, quic::QuicVersion,
        std::shared_ptr<const fizz::server::FizzServerContext> ctx) noexcept override
    {
        ss_.evb.store(evb);
        auto t = quic::QuicServerTransport::make(
            evb, std::move(sock), &cb_, &cb_, std::move(ctx));
        auto ts = t->getTransportSettings();
        ts.advertisedInitialMaxStreamsBidi = 100;
        ts.advertisedInitialMaxStreamsUni = 100;
        ts.datagramConfig.enabled = true;
        ts.datagramConfig.sendDropOldDataFirst = true;
        t->setTransportSettings(ts);
        ss_.transport = t;
        return t;
    }
private:
    server_state &ss_;
    server_callback &cb_;
};

/* -- Helpers --------------------------------------------------------- */

inline bool ns_matches(const moq_namespace_t &ns,
                        const char *a, const char *b) {
    if (ns.count != 2) return false;
    return ns.parts[0].len == std::strlen(a) &&
           std::memcmp(ns.parts[0].data, a, ns.parts[0].len) == 0 &&
           ns.parts[1].len == std::strlen(b) &&
           std::memcmp(ns.parts[1].data, b, ns.parts[1].len) == 0;
}

inline bool bytes_eq(moq_bytes_t b, const char *s) {
    return b.len == std::strlen(s) &&
           std::memcmp(b.data, s, b.len) == 0;
}

/* -- Loopback pair --------------------------------------------------- */

class loopback_pair {
public:
    server_state ss;
    bool init_ok = false;

    moq_session_t *client_session = nullptr;
    std::unique_ptr<moq::mvfst::adapter> client_adapter;
    std::atomic<bool> client_setup_complete{false};

    explicit loopback_pair(moq_version_t version = (moq_version_t)0) {
        ss.version = version;
        client_cfg_.version = version;
        const std::string alpn =
            (version == MOQ_VERSION_DRAFT_18) ? "moqt-18" : "moqt-16";
        auto cm = generate_self_signed_cert();
        if (!cm.ok) return;
        auto sctx = std::make_shared<fizz::server::FizzServerContext>();
        sctx->setSupportedAlpns({alpn});
        auto mgr = std::make_shared<fizz::server::DefaultCertManager>();
        std::unique_ptr<fizz::SelfCert> sc;
        fizz::Error err;
        auto st = fizz::openssl::CertUtils::makeSelfCert(
            sc, err, cm.cert_pem, cm.key_pem);
        if (st != fizz::Status::Success || !sc) return;
        mgr->addCertAndSetDefault(
            std::shared_ptr<fizz::SelfCert>(std::move(sc)));
        sctx->setCertManager(mgr);

        scb_ = std::make_unique<server_callback>(ss);
        server_ = quic::QuicServer::createQuicServer();
        server_->setFizzContext(sctx);
        server_->setQuicServerTransportFactory(
            std::make_unique<server_transport_factory>(ss, *scb_));
        folly::SocketAddress addr("127.0.0.1", 0);
        server_->start(addr, 1);
        server_->waitUntilInitialized();

        auto cctx = std::make_shared<fizz::client::FizzClientContext>();
        cctx->setSupportedAlpns({alpn});
        auto vf = std::make_shared<fizz::InsecureCertificateVerifier>(
            fizz::VerificationContext::Client);
        auto hsk = quic::FizzClientQuicHandshakeContext::Builder()
            .setFizzClientContext(cctx).setCertificateVerifier(vf).build();

        client_evb_ = std::make_unique<folly::EventBase>();
        auto qevb = std::make_shared<quic::FollyQuicEventBase>(client_evb_.get());
        auto sock = std::make_unique<quic::FollyQuicAsyncUDPSocket>(qevb);
        client_transport_ = quic::QuicClientTransport::newClient(
            qevb, std::move(sock), std::move(hsk));
        client_transport_->addNewPeerAddress(server_->getAddress());
        auto ts = client_transport_->getTransportSettings();
        ts.advertisedInitialMaxStreamsBidi = 100;
        ts.advertisedInitialMaxStreamsUni = 100;
        ts.datagramConfig.enabled = true;
        client_transport_->setTransportSettings(ts);

        if (moq_session_create(&client_cfg_, 0, &client_session) < 0) return;
        if (moq_session_start(client_session, 0) < 0) return;

        client_transport_->start(&start_cb_, &start_cb_);

        client_adapter = std::make_unique<moq::mvfst::adapter>(
            moq::mvfst::adapter::config::client(),
            client_session, client_transport_);
        init_ok = true;
    }

    ~loopback_pair() {
        auto *evb = ss.evb.load();
        if (evb) {
            evb->runInEventBaseThreadAndWait([this]() {
                ss.adp.reset();
                if (ss.transport) {
                    ss.transport->close(
                        quic::QuicError(quic::ApplicationErrorCode(0)));
                    ss.transport.reset();
                }
                if (ss.session) {
                    moq_session_destroy(ss.session);
                    ss.session = nullptr;
                }
            });
        }
        client_adapter.reset();
        if (client_transport_) {
            client_transport_->close(
                quic::QuicError(quic::ApplicationErrorCode(0)));
            client_transport_.reset();
        }
        if (client_session) moq_session_destroy(client_session);
        if (client_evb_) {
            client_evb_->loop();
            client_evb_.reset();
        }
        if (server_) server_->shutdown();
    }

    void service_server() {
        if (!ss.adp) return;
        auto rc = ss.adp->service(0);
        if (rc < 0 || ss.adp->is_fatal())
            ss.adapter_fatal.store(true);
    }

    void pump_server() {
        auto *evb = ss.evb.load();
        if (!evb) return;
        evb->runInEventBaseThreadAndWait([this]() {
            service_server();
            if (!ss.session) return;
            moq_event_t ev[16]; size_t ne;
            moq_session_poll_events_ex(ss.session, ev, 16,
                                        sizeof(moq_event_t), &ne);
            for (size_t i = 0; i < ne; i++) {
                if (ev[i].kind == MOQ_EVENT_SETUP_COMPLETE)
                    ss.setup_complete.store(true);
                if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_REQUEST &&
                    !ss.sub_accepted.load()) {
                    auto &sr = ev[i].u.subscribe_request;
                    if (!ss.expect_sub_ns1.empty() &&
                        (!ns_matches(sr.track_namespace,
                                     ss.expect_sub_ns1.c_str(),
                                     ss.expect_sub_ns2.c_str()) ||
                         !bytes_eq(sr.track_name,
                                   ss.expect_sub_track.c_str()))) {
                        ss.error.store(true);
                    } else {
                        ss.sub_handle = sr.sub;
                        moq_accept_subscribe_cfg_t acfg;
                        moq_accept_subscribe_cfg_init(&acfg);
                        moq_result_t rc = moq_session_accept_subscribe(
                            ss.session, ss.sub_handle, &acfg, 0);
                        if (rc < 0) ss.error.store(true);
                        else { service_server(); ss.sub_accepted.store(true); }
                    }
                }
                if (ev[i].kind == MOQ_EVENT_NAMESPACE_DONE) {
                    if (moq_announcement_eq(ev[i].u.namespace_done.ann,
                                             ss.ns_handle))
                        ss.ns_done.store(true);
                    else ss.error.store(true);
                }
                if (ev[i].kind == MOQ_EVENT_NAMESPACE_PUBLISHED &&
                    !ss.ns_accepted.load()) {
                    auto &np = ev[i].u.namespace_published;
                    if (!ns_matches(np.track_namespace, "mvfst", "pub")) {
                        ss.error.store(true);
                    } else {
                        ss.ns_handle = np.ann;
                        moq_accept_namespace_cfg_t ncfg;
                        moq_accept_namespace_cfg_init(&ncfg);
                        moq_result_t rc = moq_session_accept_namespace(
                            ss.session, ss.ns_handle, &ncfg, 0);
                        if (rc < 0) ss.error.store(true);
                        else { service_server(); ss.ns_accepted.store(true); }
                    }
                }
                moq_event_cleanup(&ev[i]);
            }
        });
    }

    void pump_client() {
        if (client_evb_) client_evb_->loopOnce(EVLOOP_NONBLOCK);
        if (client_adapter) {
            auto rc = client_adapter->service(0);
            if (rc < 0 || client_adapter->is_fatal())
                client_fatal_.store(true);
        }
    }

    void run_on_server(std::function<void()> fn) {
        auto *evb = ss.evb.load();
        if (!evb) return;
        evb->runInEventBaseThreadAndWait(std::move(fn));
    }

    bool wait_for(std::function<bool()> pred, int ms = 5000) {
        auto dl = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(ms);
        while (std::chrono::steady_clock::now() < dl) {
            pump_client();
            pump_server();
            if (has_error()) return false;
            if (pred()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        if (has_error()) return false;
        return pred();
    }

    bool wait_setup() {
        if (!init_ok) return false;
        return wait_for([this]() {
            moq_event_t ev[16]; size_t ne;
            moq_session_poll_events_ex(client_session, ev, 16,
                                        sizeof(moq_event_t), &ne);
            for (size_t i = 0; i < ne; i++) {
                if (ev[i].kind == MOQ_EVENT_SETUP_COMPLETE)
                    client_setup_complete.store(true);
                moq_event_cleanup(&ev[i]);
            }
            return client_setup_complete.load() && ss.setup_complete.load();
        });
    }

    bool has_error() const {
        return ss.error.load() || ss.adapter_fatal.load() ||
               client_fatal_.load();
    }

private:
    struct noop_cb : quic::QuicSocket::ConnectionSetupCallback,
                     quic::QuicSocket::ConnectionCallback {
        void onTransportReady() noexcept override {}
        void onConnectionSetupError(quic::QuicError) noexcept override {}
        void onReplaySafe() noexcept override {}
        void onFullHandshakeDone() noexcept override {}
        void onNewBidirectionalStream(quic::StreamId) noexcept override {}
        void onNewUnidirectionalStream(quic::StreamId) noexcept override {}
        void onStopSending(quic::StreamId, quic::ApplicationErrorCode) noexcept override {}
        void onConnectionEnd() noexcept override {}
        void onConnectionError(quic::QuicError) noexcept override {}
    } start_cb_;

    std::atomic<bool> client_fatal_{false};
    std::unique_ptr<server_callback> scb_;
    std::shared_ptr<quic::QuicServer> server_;
    std::unique_ptr<folly::EventBase> client_evb_;
    std::shared_ptr<quic::QuicClientTransport> client_transport_;
    moq_session_cfg_t client_cfg_ = []{
        moq_session_cfg_t c;
        moq_session_cfg_init_sized(&c, sizeof(c), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
        c.send_request_capacity = true;
        c.initial_request_capacity = 16;
        return c;
    }();
};

} // namespace moq::mvfst::test

#endif // MOQ_MVFST_LOOPBACK_PAIR_HPP
