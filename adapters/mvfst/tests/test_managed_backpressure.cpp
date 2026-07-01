/*
 * Managed mvfst inbound backpressure regression test.
 *
 * Exercises the real mvfst pauseRead/resumeRead path:
 * 1. Configure client session with max_events=2 (tiny event queue).
 * 2. Server sends a burst of objects without waiting for client drain.
 * 3. Client pump deliberately delays event draining, forcing
 *    event_queue_full → MOQ_ERR_WOULD_BLOCK → adapter pauses read.
 * 4. Client later drains events → adapter resumes read → remaining
 *    objects arrive.
 * 5. Assert: adapter never went fatal, all objects received.
 *
 * Prior to the pauseRead/resumeRead fix, this scenario would call
 * setReadCallback(id, nullptr) to "pause" and then fail to re-install
 * the callback, making the adapter fatal on resume.
 *
 * Separate binary: one QuicServer per process.
 */

#include <moq/mvfst.h>
#include <moq/mvfst.hpp>
#include <moq/session.h>
#include <moq/rcbuf.h>

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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

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

/* -- Server ---------------------------------------------------------- */

struct server_env {
    std::shared_ptr<quic::QuicServerTransport> transport;
    moq_session_t *session = nullptr;
    std::unique_ptr<moq::mvfst::adapter> adp;
    std::atomic<folly::EventBase *> evb{nullptr};
    std::atomic<bool> setup_complete{false};
    std::atomic<bool> sub_accepted{false};
    std::atomic<bool> error{false};
    std::atomic<bool> adapter_fatal{false};
    moq_subscription_t sub_handle = {};
};

class srv_cb
    : public quic::QuicSocket::ConnectionSetupCallback
    , public quic::QuicSocket::ConnectionCallback
{
public:
    explicit srv_cb(server_env &s) : s_(s) {}
    void onTransportReady() noexcept override {
        try {
            moq_session_cfg_t cfg;
            moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
            cfg.send_request_capacity = true;
            cfg.initial_request_capacity = 16;
            if (moq_session_create(&cfg, 0, &s_.session) < 0) {
                s_.error.store(true); return;
            }
            s_.adp = std::make_unique<moq::mvfst::adapter>(
                moq::mvfst::adapter::config::server(),
                s_.session, s_.transport);
        } catch (...) {
            if (s_.session) { moq_session_destroy(s_.session); s_.session = nullptr; }
            if (s_.transport) s_.transport->close(quic::QuicError(quic::ApplicationErrorCode(0)));
            s_.error.store(true);
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
    server_env &s_;
};

class srv_factory : public quic::QuicServerTransportFactory {
public:
    srv_factory(server_env &s, srv_cb &cb) : s_(s), cb_(cb) {}
    quic::QuicServerTransport::Ptr make(
        folly::EventBase *evb,
        std::unique_ptr<quic::FollyAsyncUDPSocketAlias> sock,
        const folly::SocketAddress &, quic::QuicVersion,
        std::shared_ptr<const fizz::server::FizzServerContext> ctx) noexcept override
    {
        s_.evb.store(evb);
        auto t = quic::QuicServerTransport::make(evb, std::move(sock), &cb_, &cb_, std::move(ctx));
        auto ts = t->getTransportSettings();
        ts.advertisedInitialMaxStreamsBidi = 100;
        ts.advertisedInitialMaxStreamsUni = 100;
        ts.datagramConfig.enabled = true;
        t->setTransportSettings(ts);
        s_.transport = t;
        return t;
    }
private:
    server_env &s_;
    srv_cb &cb_;
};

static void service_server(server_env &sv) {
    if (!sv.adp) return;
    auto rc = sv.adp->service(0);
    if (rc < 0 || sv.adp->is_fatal())
        sv.adapter_fatal.store(true);
}

static void pump_server(server_env &sv) {
    auto *evb = sv.evb.load();
    if (!evb) return;
    evb->runInEventBaseThreadAndWait([&sv]() {
        service_server(sv);
        if (!sv.session) return;
        moq_event_t ev[16]; size_t ne;
        moq_session_poll_events_ex(sv.session, ev, 16,
                                    sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_SETUP_COMPLETE)
                sv.setup_complete.store(true);
            if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_REQUEST &&
                !sv.sub_accepted.load()) {
                auto &req = ev[i].u.subscribe_request;
                sv.sub_handle = req.sub;
                moq_accept_subscribe_cfg_t acfg;
                moq_accept_subscribe_cfg_init(&acfg);
                moq_result_t rc = moq_session_accept_subscribe(
                    sv.session, sv.sub_handle, &acfg, 0);
                if (rc < 0) sv.error.store(true);
                else {
                    service_server(sv);
                    sv.sub_accepted.store(true);
                }
            }
            moq_event_cleanup(&ev[i]);
        }
    });
}

/* -- Client state ---------------------------------------------------- */

static constexpr int BURST_COUNT = 10;

struct client_state {
    std::atomic<bool> setup_complete{false};
    std::atomic<bool> subscribed{false};
    std::atomic<bool> sub_ok{false};
    std::atomic<bool> client_error{false};
    std::atomic<int> pump_count{0};
    std::atomic<bool> drain_enabled{false};

    std::mutex mu;
    std::vector<uint64_t> received_objects;
};

static int client_pump(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)now;
    auto *cs = static_cast<client_state *>(ctx);
    moq_session_t *s = moq_mvfst_managed_session(m);
    if (!s) return 0;

    cs->pump_count.fetch_add(1);

    if (cs->setup_complete.load() && !cs->subscribed.load()) {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        moq_bytes_t ns_parts[] = {
            {(const uint8_t *)"mvfst", 5},
            {(const uint8_t *)"bp", 2}
        };
        sc.track_namespace.parts = ns_parts;
        sc.track_namespace.count = 2;
        sc.track_name = {(const uint8_t *)"video", 5};
        sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        moq_result_t rc = moq_session_subscribe(s, &sc, 0, &sub);
        if (rc >= 0) cs->subscribed.store(true);
        else cs->client_error.store(true);
    }

    if (!cs->drain_enabled.load() && cs->sub_ok.load()) {
        /* Deliberately do NOT drain events. This lets the event queue
         * fill up, causing on_data_bytes to return WOULD_BLOCK, which
         * forces the adapter to pause reads. */
        return 0;
    }

    moq_event_t ev[16]; size_t ne;
    moq_session_poll_events_ex(s, ev, 16, sizeof(moq_event_t), &ne);
    for (size_t i = 0; i < ne; i++) {
        if (ev[i].kind == MOQ_EVENT_SETUP_COMPLETE)
            cs->setup_complete.store(true);
        if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_OK)
            cs->sub_ok.store(true);
        if (ev[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
            std::lock_guard<std::mutex> lk(cs->mu);
            cs->received_objects.push_back(
                ev[i].u.object_received.object_id);
        }
        moq_event_cleanup(&ev[i]);
    }
    return 0;
}

/* -- Wait helper ----------------------------------------------------- */

static bool wait_for(moq_mvfst_managed_t *m, server_env &sv,
                      client_state &cs, std::function<bool()> pred,
                      int ms = 5000)
{
    auto dl = std::chrono::steady_clock::now() +
              std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < dl) {
        if (cs.client_error.load() || sv.error.load()) return false;
        moq_mvfst_managed_wake(m);
        moq_result_t wr = moq_mvfst_managed_wait(m, 100000);
        if (wr == MOQ_ERR_CLOSED) break;
        pump_server(sv);
        if (pred()) return true;
    }
    return pred();
}

/* -- Test ------------------------------------------------------------ */

static void test_inbound_backpressure()
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
    mgr->addCertAndSetDefault(std::shared_ptr<fizz::SelfCert>(std::move(sc)));
    sctx->setCertManager(mgr);

    server_env sv;
    srv_cb scb(sv);
    auto server = quic::QuicServer::createQuicServer();
    server->setFizzContext(sctx);
    server->setQuicServerTransportFactory(
        std::make_unique<srv_factory>(sv, scb));
    folly::SocketAddress addr("127.0.0.1", 0);
    server->start(addr, 1);
    server->waitUntilInitialized();
    auto bound = server->getAddress();

    client_state cs;

    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init(&cfg);
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = bound.getPort();
    cfg.insecure_skip_verify = true;
    cfg.on_pump = client_pump;
    cfg.user_ctx = &cs;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 2;

    moq_mvfst_managed_t *m = nullptr;
    moq_result_t rc = moq_mvfst_managed_create(&cfg, &m);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) { server->shutdown(); return; }

    /* Phase 1: setup + subscribe. */
    MVFST_CHECK(wait_for(m, sv, cs, [&]() {
        return cs.setup_complete.load() && sv.setup_complete.load();
    }));
    MVFST_CHECK(wait_for(m, sv, cs, [&]() {
        return cs.sub_ok.load();
    }));
    MVFST_CHECK(sv.sub_accepted.load());
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(m));

    /* Phase 2: server sends a burst of objects in one subgroup on a
     * single uni stream. Client pump does NOT drain object events yet
     * (drain_enabled = false), so the event queue (max_events=2) fills
     * up. The adapter must pause reads via pauseRead, not
     * setReadCallback(nullptr). */
    std::atomic<bool> burst_ok{false};
    sv.evb.load()->runInEventBaseThreadAndWait([&]() {
        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = 0;
        sgcfg.publisher_priority = 200;
        moq_subgroup_handle_t sg;
        if (moq_session_open_subgroup(sv.session, sv.sub_handle,
                                       &sgcfg, 0, &sg) < 0) {
            sv.error.store(true); return;
        }
        for (int i = 0; i < BURST_COUNT; i++) {
            char payload[16];
            int plen = std::snprintf(payload, sizeof(payload), "obj-%d", i);
            moq_rcbuf_t *buf = nullptr;
            if (moq_rcbuf_create(moq_alloc_default(),
                    reinterpret_cast<const uint8_t *>(payload),
                    static_cast<size_t>(plen), &buf) < 0) {
                sv.error.store(true); return;
            }
            if (moq_session_write_object(sv.session, sg,
                    static_cast<uint64_t>(i), buf, 0) < 0) {
                moq_rcbuf_decref(buf);
                sv.error.store(true); return;
            }
            moq_rcbuf_decref(buf);
        }
        if (moq_session_close_subgroup(sv.session, sg, 0) < 0) {
            sv.error.store(true); return;
        }
        service_server(sv);
        burst_ok.store(true);
    });
    MVFST_CHECK(burst_ok.load());
    MVFST_CHECK(!sv.error.load());

    /* Give the adapter a few pump cycles to receive data and hit
     * backpressure. Events are not drained during this time. */
    for (int i = 0; i < 20; i++) {
        moq_mvfst_managed_wake(m);
        moq_mvfst_managed_wait(m, 50000);
        pump_server(sv);
        if (moq_mvfst_managed_is_fatal(m)) break;
    }
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(m));

    /* Phase 3: enable event draining and pump until all objects arrive. */
    cs.drain_enabled.store(true);
    MVFST_CHECK(wait_for(m, sv, cs, [&]() {
        std::lock_guard<std::mutex> lk(cs.mu);
        return static_cast<int>(cs.received_objects.size()) >= BURST_COUNT;
    }, 10000));

    {
        std::lock_guard<std::mutex> lk(cs.mu);
        MVFST_CHECK(static_cast<int>(cs.received_objects.size()) == BURST_COUNT);
        std::vector<uint64_t> expected(BURST_COUNT);
        for (int i = 0; i < BURST_COUNT; i++)
            expected[i] = static_cast<uint64_t>(i);
        auto sorted = cs.received_objects;
        std::sort(sorted.begin(), sorted.end());
        MVFST_CHECK(sorted == expected);
    }

    MVFST_CHECK(!moq_mvfst_managed_is_fatal(m));
    MVFST_CHECK(!sv.error.load());
    MVFST_CHECK(!sv.adapter_fatal.load());
    MVFST_CHECK(!cs.client_error.load());

    moq_mvfst_managed_destroy(m);

    if (sv.evb.load()) {
        sv.evb.load()->runInEventBaseThreadAndWait([&sv]() {
            sv.adp.reset();
            if (sv.transport) {
                sv.transport->close(
                    quic::QuicError(quic::ApplicationErrorCode(0)));
                sv.transport.reset();
            }
            if (sv.session) {
                moq_session_destroy(sv.session);
                sv.session = nullptr;
            }
        });
    }
    server->shutdown();
}

int main()
{
    test_inbound_backpressure();
    std::printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
