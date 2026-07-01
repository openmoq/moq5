/*
 * Managed-mode outbound stream-credit gating.
 *
 * A managed mvfst CLIENT publishes more unidirectional (subgroup) streams
 * than the peer's initial credit allows. The raw test SERVER advertises a
 * low initial max-uni-streams, so the client's createUnidirectionalStream()
 * hits STREAM_LIMIT_EXCEEDED. As the server reads and retires finished
 * streams it grants MAX_STREAMS, and the client must retry the blocked opens.
 *
 * Asserts (deterministic, counter/latch based -- no sleeps, no log scraping):
 *   - credit was actually blocked            (block_count > 0)
 *   - the credit-availability callback ran   (grant_count > 0)  <- the fix
 *   - every published object is delivered     (server receives all)
 *   - busy-retry is bounded, not per-tick spam (block_count <= bound)
 *
 * Without the fix, the credit-availability callback is not handled
 * (grant_count == 0) and the open is retried on every pump tick
 * (block_count grows without bound) -- both fail this test.
 *
 * Managed client + test-controlled raw server (the test_managed_loopback
 * pattern, reversed: here the client is the publisher).
 */

#include <moq/mvfst.h>
#include <moq/mvfst.hpp>
#include <moq/session.h>
#include <moq/rcbuf.h>

#include "../src/mvfst_managed_testing.h"

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

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>

static int failures = 0;

#define MVFST_CHECK(expr) do { \
    if (!(expr)) { \
        std::fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* Publish this many subgroups (one uni stream each). */
static constexpr int K_SUBGROUPS = 8;
/* Peer initial uni-stream credit -- low enough to force the block. */
static constexpr uint32_t SERVER_MAX_UNI = 3;

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

/* -- Server (subscriber) harness ------------------------------------- */

static bool ns_eq(const moq_namespace_t &ns, const char *a, const char *b) {
    if (ns.count != 2) return false;
    if (ns.parts[0].len != std::strlen(a) ||
        std::memcmp(ns.parts[0].data, a, ns.parts[0].len) != 0) return false;
    if (ns.parts[1].len != std::strlen(b) ||
        std::memcmp(ns.parts[1].data, b, ns.parts[1].len) != 0) return false;
    return true;
}

struct server_env {
    std::shared_ptr<quic::QuicServerTransport> transport;
    moq_session_t *session = nullptr;
    std::unique_ptr<moq::mvfst::adapter> adp;
    std::atomic<folly::EventBase *> evb{nullptr};
    std::atomic<bool> setup_complete{false};
    std::atomic<bool> subscribed{false};
    std::atomic<bool> error{false};
    std::atomic<bool> adapter_fatal{false};
    moq_subscription_t sub = {};

    std::mutex groups_mu;
    std::set<uint64_t> groups_received;   /* distinct group_ids delivered */
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
        /* The knob under test: starve the client's outbound uni-stream
         * credit so its subgroup opens hit STREAM_LIMIT_EXCEEDED. */
        ts.advertisedInitialMaxStreamsUni = SERVER_MAX_UNI;
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
        /* Subscribe to the client's track once setup completes. */
        if (sv.setup_complete.load() && !sv.subscribed.load()) {
            moq_subscribe_cfg_t sc;
            moq_subscribe_cfg_init(&sc);
            moq_bytes_t ns_parts[] = {
                {(const uint8_t *)"mvfst", 5},
                {(const uint8_t *)"credit", 6}
            };
            sc.track_namespace.parts = ns_parts;
            sc.track_namespace.count = 2;
            sc.track_name = {(const uint8_t *)"video", 5};
            sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
            if (moq_session_subscribe(sv.session, &sc, 0, &sv.sub) >= 0) {
                sv.subscribed.store(true);
                service_server(sv);
            } else {
                sv.error.store(true);
            }
        }
        moq_event_t ev[32]; size_t ne;
        moq_session_poll_events_ex(sv.session, ev, 32,
                                    sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_SETUP_COMPLETE)
                sv.setup_complete.store(true);
            if (ev[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                std::lock_guard<std::mutex> lk(sv.groups_mu);
                sv.groups_received.insert(ev[i].u.object_received.group_id);
            }
            moq_event_cleanup(&ev[i]);
        }
    });
}

/* -- Client (publisher) pump ----------------------------------------- */

struct client_state {
    std::atomic<bool> setup_complete{false};
    std::atomic<bool> sub_accepted{false};
    std::atomic<bool> published{false};
    std::atomic<bool> client_error{false};
    moq_subscription_t sub_handle = {};
};

static int client_pump(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)now;
    auto *cs = static_cast<client_state *>(ctx);
    moq_session_t *s = moq_mvfst_managed_session(m);
    if (!s) return 0;

    moq_event_t ev[32]; size_t ne;
    moq_session_poll_events_ex(s, ev, 32, sizeof(moq_event_t), &ne);
    for (size_t i = 0; i < ne; i++) {
        if (ev[i].kind == MOQ_EVENT_SETUP_COMPLETE)
            cs->setup_complete.store(true);
        if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_REQUEST &&
            !cs->sub_accepted.load()) {
            auto &req = ev[i].u.subscribe_request;
            if (!ns_eq(req.track_namespace, "mvfst", "credit") ||
                req.track_name.len != 5 ||
                std::memcmp(req.track_name.data, "video", 5) != 0) {
                cs->client_error.store(true);
            } else {
                cs->sub_handle = req.sub;
                moq_accept_subscribe_cfg_t acfg;
                moq_accept_subscribe_cfg_init(&acfg);
                if (moq_session_accept_subscribe(s, cs->sub_handle, &acfg, 0) < 0)
                    cs->client_error.store(true);
                else
                    cs->sub_accepted.store(true);
            }
        }
        moq_event_cleanup(&ev[i]);
    }

    /* Once subscribed, publish K subgroups (one uni stream each) in a single
     * burst. The over-credit opens block; the bridge retains them and retries
     * as the peer grants more stream credit. */
    if (cs->sub_accepted.load() && !cs->published.load()) {
        cs->published.store(true);
        for (int g = 0; g < K_SUBGROUPS; g++) {
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            sgcfg.group_id = (uint64_t)g;
            sgcfg.publisher_priority = 128;
            moq_subgroup_handle_t sg;
            if (moq_session_open_subgroup(s, cs->sub_handle, &sgcfg, 0, &sg) < 0) {
                cs->client_error.store(true); break;
            }
            char body[16];
            int n = std::snprintf(body, sizeof(body), "obj-%d", g);
            moq_rcbuf_t *buf = nullptr;
            if (moq_rcbuf_create(moq_alloc_default(),
                    reinterpret_cast<const uint8_t *>(body),
                    (size_t)n, &buf) < 0) {
                cs->client_error.store(true); break;
            }
            if (moq_session_write_object(s, sg, 0, buf, 0) < 0) {
                moq_rcbuf_decref(buf);
                cs->client_error.store(true); break;
            }
            moq_rcbuf_decref(buf);
            if (moq_session_close_subgroup(s, sg, 0) < 0) {
                cs->client_error.store(true); break;
            }
        }
    }

    return 0;
}

/* -- Wait helper ----------------------------------------------------- */

static bool wait_for(moq_mvfst_managed_t *m, server_env &sv,
                      client_state &cs, std::function<bool()> pred,
                      int ms = 8000)
{
    auto dl = std::chrono::steady_clock::now() +
              std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < dl) {
        if (cs.client_error.load() || sv.error.load() ||
            sv.adapter_fatal.load())
            return false;
        moq_mvfst_managed_wake(m);
        moq_result_t wr = moq_mvfst_managed_wait(m, 50000);
        if (wr == MOQ_ERR_CLOSED) break;
        pump_server(sv);
        if (pred()) return true;
    }
    return pred();
}

static size_t groups_count(server_env &sv) {
    std::lock_guard<std::mutex> lk(sv.groups_mu);
    return sv.groups_received.size();
}

/* -- Test ------------------------------------------------------------ */

static void test_managed_stream_credit()
{
    auto cm = gen_cert();
    MVFST_CHECK(cm.ok);
    if (!cm.ok) return;

    auto sctx = std::make_shared<fizz::server::FizzServerContext>();
    sctx->setSupportedAlpns({"moqt-16"});
    auto mgr = std::make_shared<fizz::server::DefaultCertManager>();
    std::unique_ptr<fizz::SelfCert> sc;
    fizz::Error err;
    auto st = fizz::openssl::CertUtils::makeSelfCert(sc, err, cm.cert_pem, cm.key_pem);
    MVFST_CHECK(st == fizz::Status::Success && sc);
    if (st != fizz::Status::Success || !sc) return;
    mgr->addCertAndSetDefault(std::shared_ptr<fizz::SelfCert>(std::move(sc)));
    sctx->setCertManager(mgr);

    server_env sv;
    srv_cb scb(sv);
    auto server = quic::QuicServer::createQuicServer();
    server->setFizzContext(sctx);
    server->setQuicServerTransportFactory(std::make_unique<srv_factory>(sv, scb));
    server->start(folly::SocketAddress("127.0.0.1", 0), 1);
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

    moq_mvfst_managed_t *m = nullptr;
    moq_result_t rc = moq_mvfst_managed_create(&cfg, &m);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) { server->shutdown(); return; }

    /* Setup + subscribe handshake. */
    MVFST_CHECK(wait_for(m, sv, cs, [&]() {
        return cs.setup_complete.load() && sv.setup_complete.load() &&
               sv.subscribed.load();
    }));

    /* All K objects delivered despite the starved initial uni credit. */
    bool delivered = wait_for(m, sv, cs, [&]() {
        return groups_count(sv) >= (size_t)K_SUBGROUPS;
    });
    MVFST_CHECK(delivered);
    MVFST_CHECK(!cs.client_error.load());
    MVFST_CHECK(!sv.error.load());
    MVFST_CHECK(!sv.adapter_fatal.load());
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(m));

    uint64_t blocks = moq_mvfst_managed_credit_block_count(m);
    uint64_t grants = moq_mvfst_managed_credit_grant_count(m);
    std::fprintf(stderr, "stream-credit: delivered=%zu/%d blocks=%llu grants=%llu\n",
                 groups_count(sv), K_SUBGROUPS,
                 (unsigned long long)blocks, (unsigned long long)grants);

    /* Credit backpressure actually occurred (K > initial credit). */
    MVFST_CHECK(blocks > 0);
    /* The retry was driven by the mvfst credit-availability callback -- the
     * fix. Without it this is 0 and the open would only retry via the poll. */
    MVFST_CHECK(grants > 0);
    /* Busy-retry is bounded: with gating, ~one create attempt per grant, not
     * one per pump tick. A generous ceiling that poll-spam would blow past. */
    MVFST_CHECK(blocks <= (uint64_t)(4 * K_SUBGROUPS));

    MVFST_CHECK(moq_mvfst_managed_stop(m) == MOQ_OK);
    moq_mvfst_managed_destroy(m);

    auto *evb = sv.evb.load();
    if (evb) {
        evb->runInEventBaseThreadAndWait([&]() {
            sv.adp.reset();
            if (sv.transport) {
                sv.transport->close(quic::QuicError(quic::ApplicationErrorCode(0)));
                sv.transport.reset();
            }
            if (sv.session) { moq_session_destroy(sv.session); sv.session = nullptr; }
        });
    }
    server->shutdown();
}

int main()
{
    test_managed_stream_credit();
    std::printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
