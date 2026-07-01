/*
 * mvfst adapter: wires quic::QuicSocket callbacks to the shared
 * transport bridge (moq_transport_bridge_t).
 *
 * adapter::impl inherits mvfst callback interfaces and routes
 * inbound events through bridge inbound handlers. Outbound actions
 * flow through bridge_service() → endpoint ops → QuicSocket.
 *
 * The bridge owns all pending state (outbound + inbound), stream
 * mapping, tombstones, and service ordering. The adapter only
 * manages mvfst-specific concerns: read callbacks, pauseRead/
 * resumeRead for transport-level backpressure, IOBuf→rcbuf
 * conversion, and the managed client/server lifecycle.
 */

#include <moq/mvfst.h>
#include <moq/mvfst.hpp>
#include <moq/transport_bridge.h>
#include <moq/rcbuf.h>
#include "mvfst_endpoint_ops.h"
#include "../../common/moq_alpn.h"  /* moq_alpn_to_version / _for_version */

#include <quic/api/QuicSocket.h>
#include <quic/QuicConstants.h>
#include <quic/state/QuicStreamUtilities.h>

#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <new>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

/* -- Managed lifecycle C API ----------------------------------------- */

#include <quic/client/QuicClientTransport.h>
#include <quic/common/events/FollyQuicEventBase.h>
#include <quic/common/udpsocket/FollyQuicAsyncUDPSocket.h>
#include <quic/fizz/client/handshake/FizzClientQuicHandshakeContext.h>
#include <quic/server/QuicServer.h>
#include <quic/server/QuicServerTransport.h>
#include <fizz/backend/openssl/certificate/CertUtils.h>
#include <fizz/backend/openssl/certificate/OpenSSLCertificateVerifier.h>
#include <fizz/protocol/CertificateVerifier.h>
#include <fizz/server/DefaultCertManager.h>
#include <fizz/server/FizzServerContext.h>
#include <folly/io/async/ssl/OpenSSLTransportCertificate.h>

#include <folly/io/async/EventBase.h>

#include <openssl/x509v3.h>

#include <arpa/inet.h>

#include <atomic>
#include <condition_variable>
#include <fstream>
#include <stdexcept>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

/*
 * Certificate verifier: chain validation + host/IP identity.
 *
 * IP literals (detected via inet_pton) match only iPAddress SANs
 * via X509_check_ip_asc. DNS names match only dNSName SANs via
 * X509_check_host. This prevents a cert with DNS:127.0.0.1 from
 * satisfying an IP-literal connection to 127.0.0.1.
 */
class identity_verifier final : public fizz::CertificateVerifier {
public:
    identity_verifier(
        std::shared_ptr<fizz::openssl::OpenSSLCertificateVerifier> chain,
        std::string expected_host)
        : chain_(std::move(chain))
        , expected_host_(std::move(expected_host))
        , is_ip_(detect_ip(expected_host_)) {}

    fizz::Status verify(
        std::shared_ptr<const fizz::Cert> &ret,
        fizz::Error &err,
        const std::vector<std::shared_ptr<const fizz::PeerCert>> &certs)
        const override
    {
        auto st = chain_->verify(ret, err, certs);
        if (st != fizz::Status::Success)
            return st;

        if (certs.empty())
            return err.error("no peer certificate");

        auto x509 = folly::OpenSSLTransportCertificate
            ::tryExtractX509(certs.front().get());
        if (!x509)
            return err.error("cannot extract X509 from peer cert");

        if (is_ip_) {
            if (X509_check_ip_asc(x509.get(),
                    expected_host_.c_str(), 0) == 1)
                return fizz::Status::Success;
        } else {
            if (X509_check_host(x509.get(),
                    expected_host_.c_str(),
                    expected_host_.size(), 0, nullptr) == 1)
                return fizz::Status::Success;
        }

        return err.error("peer certificate identity mismatch");
    }

    fizz::Status getCertificateRequestExtensions(
        std::vector<fizz::Extension> &ret,
        fizz::Error &err) const override
    {
        return chain_->getCertificateRequestExtensions(ret, err);
    }

private:
    static bool detect_ip(const std::string &host) {
        unsigned char buf[sizeof(struct in6_addr)];
        if (inet_pton(AF_INET, host.c_str(), buf) == 1) return true;
        if (inet_pton(AF_INET6, host.c_str(), buf) == 1) return true;
        return false;
    }

    std::shared_ptr<fizz::openssl::OpenSSLCertificateVerifier> chain_;
    std::string expected_host_;
    bool is_ip_;
};

/* Forward declare so conn_server_cb can reference it. */
struct moq_mvfst_managed;

/*
 * Per-connection state for managed server mode.
 * Owns session, adapter, transport, and callbacks for one
 * accepted QUIC connection. Created by the factory on the
 * managed EventBase thread.
 */
struct moq_mvfst_conn
    : public quic::QuicSocket::ConnectionSetupCallback
    , public quic::QuicSocket::ConnectionCallback
{
    moq_mvfst_managed *parent = nullptr;
    std::shared_ptr<quic::QuicServerTransport> transport;
    moq_session_t *session = nullptr;
    std::unique_ptr<moq::mvfst::adapter> adp;
    bool close_requested = false;
    bool fatal = false;

    void onTransportReady() noexcept override {}
    void onConnectionSetupError(quic::QuicError) noexcept override {
        fatal = true;
    }
    void onReplaySafe() noexcept override {}
    void onFullHandshakeDone() noexcept override {}
    void onNewBidirectionalStream(quic::StreamId) noexcept override {}
    void onNewUnidirectionalStream(quic::StreamId) noexcept override {}
    void onStopSending(quic::StreamId,
                        quic::ApplicationErrorCode) noexcept override {}
    void onConnectionEnd() noexcept override {}
    void onConnectionError(quic::QuicError) noexcept override {
        fatal = true;
    }

    bool should_remove() const {
        return fatal || close_requested ||
               (adp && (adp->is_fatal() || adp->is_closed()));
    }

    void destroy() {
        adp.reset();
        if (transport) {
            transport->close(quic::QuicError(
                quic::ApplicationErrorCode(0)));
            transport.reset();
        }
        if (session) {
            moq_session_destroy(session);
            session = nullptr;
        }
    }
};

class managed_server_factory : public quic::QuicServerTransportFactory {
public:
    explicit managed_server_factory(moq_mvfst_managed *owner)
        : owner_(owner) {}

    quic::QuicServerTransport::Ptr make(
        folly::EventBase *evb,
        std::unique_ptr<quic::FollyAsyncUDPSocketAlias> sock,
        const folly::SocketAddress &, quic::QuicVersion,
        std::shared_ptr<const fizz::server::FizzServerContext> ctx)
        noexcept override;

private:
    moq_mvfst_managed *owner_;
};

/*
 * Managed lifecycle state machine:
 *
 *   running:  true while the pump loop is active. Set by the thread
 *             on successful init, cleared by stop() request or
 *             natural pump exit (on_pump returns nonzero).
 *
 *   thread_exited: true once the network thread has finished
 *             (session destroyed, thread about to return). Set by
 *             the thread itself. After this, wake() and wait()
 *             return MOQ_ERR_CLOSED.
 *
 *   joined:   true once the app thread has joined the network
 *             thread via stop() or destroy(). Prevents double-join.
 */
/* Default cap on retained accepted connections in managed server mode; bounds
 * memory and per-tick CPU against idle peers. Overridable via cfg.max_connections. */
static constexpr size_t MOQ_MVFST_DEFAULT_MAX_CONNECTIONS = 1024;

struct moq_mvfst_managed {
    moq_alloc_t alloc;
    moq_session_cfg_t session_cfg;
    std::string host;
    int port = 0;
    bool insecure_skip_verify = false;
    std::string cert_path;
    uint16_t max_num_ptos = 0;
    uint32_t initial_rtt_us = 0;

    /* TLS server name (SNI + cert-verifier identity); empty = use `host`. */
    std::string sni;
    /* EXACT-VERSION: the single offered ALPN + its fixed session version.
     * negotiated latches cfg_version once the session reaches ESTABLISHED
     * (0 before); int holds the moq_version_t for lock-free reads. */
    std::string alpn = "moqt-16";
    moq_version_t cfg_version = MOQ_VERSION_DRAFT_16;
    std::atomic<int> negotiated{0};

    std::atomic<moq_session_t *> session{nullptr};
    moq_perspective_t perspective = MOQ_PERSPECTIVE_CLIENT;

    struct start_cb
        : quic::QuicSocket::ConnectionSetupCallback
        , quic::QuicSocket::ConnectionCallback {
        void onTransportReady() noexcept override {}
        void onConnectionSetupError(quic::QuicError) noexcept override {}
        void onReplaySafe() noexcept override {}
        void onFullHandshakeDone() noexcept override {}
        void onNewBidirectionalStream(quic::StreamId) noexcept override {}
        void onNewUnidirectionalStream(quic::StreamId) noexcept override {}
        void onStopSending(quic::StreamId,
            quic::ApplicationErrorCode) noexcept override {}
        void onConnectionEnd() noexcept override {}
        void onConnectionError(quic::QuicError) noexcept override {}
    } transport_start_cb;

    /* Client: transport/adapter owned by the network thread. */
    std::shared_ptr<quic::QuicClientTransport> transport;
    std::unique_ptr<moq::mvfst::adapter> adapter_ptr;

    /* Server: QuicServer + per-connection state. */
    std::string key_path;
    std::shared_ptr<quic::QuicServer> server;
    folly::EventBase *server_evb = nullptr;
    std::atomic<uint16_t> local_port{0};
    std::vector<std::unique_ptr<moq_mvfst_conn>> conns;
    size_t max_connections = MOQ_MVFST_DEFAULT_MAX_CONNECTIONS;
    std::atomic<size_t> conn_count{0};

    moq_mvfst_pump_fn on_pump = nullptr;
    moq_mvfst_activity_fn on_activity = nullptr;
    void *user_ctx = nullptr;

    std::unique_ptr<std::thread> thread;
    std::mutex tid_mu;
    std::thread::id network_tid;
    bool network_tid_set = false;

    std::atomic<bool> running{false};
    std::atomic<bool> thread_exited{false};
    std::atomic<bool> joined{false};
    std::atomic<bool> fatal{false};
    std::atomic<uint64_t> fatal_code_val{0};

    std::mutex init_mu;
    std::condition_variable init_cv;
    std::atomic<bool> init_done{false};
    std::atomic<moq_result_t> init_result{MOQ_ERR_INTERNAL};

    std::mutex wake_mu;
    std::condition_variable wake_cv;
    std::atomic<bool> wake_flag{false};

    std::mutex wait_mu;
    std::condition_variable wait_cv;

    std::atomic<bool> activity_flag{false};

    void set_network_tid() {
        std::lock_guard<std::mutex> lk(tid_mu);
        network_tid = std::this_thread::get_id();
        network_tid_set = true;
    }

    bool is_managed_thread() {
        std::lock_guard<std::mutex> lk(tid_mu);
        return network_tid_set &&
               network_tid == std::this_thread::get_id();
    }

    bool is_network_thread() {
        if (server_evb)
            return server_evb->isInEventBaseThread();
        return is_managed_thread();
    }

    bool is_closed() const {
        return fatal.load() || thread_exited.load() || !running.load();
    }

    void set_fatal(uint64_t code) {
        fatal.store(true);
        fatal_code_val.store(code);
    }

    void signal_activity() {
        activity_flag.store(true);
        wait_cv.notify_all();
    }

    void notify_shutdown() {
        wake_cv.notify_all();
        wait_cv.notify_all();
    }

    /* Turn on managed-only outbound stream-credit gating for the client
     * adapter and wire the credit-wake to this pump. Defined out-of-line
     * below, after adapter::impl is complete. */
    void enable_credit_gating();

#ifdef MOQ_MVFST_TESTING
    /* Test-only telemetry (compiled into the test-internals object lib, not the
     * shipped adapter): outbound stream-credit block events and peer credit
     * grants on the client adapter -- deterministic backpressure evidence. */
    uint64_t credit_block_count() const;
    uint64_t credit_grant_count() const;
#endif
};

quic::QuicServerTransport::Ptr managed_server_factory::make(
    folly::EventBase * /*evb*/,
    std::unique_ptr<quic::FollyAsyncUDPSocketAlias> sock,
    const folly::SocketAddress &, quic::QuicVersion,
    std::shared_ptr<const fizz::server::FizzServerContext> ctx)
    noexcept
{
    try {
        /* Connection cap: reject before allocating any per-connection state so
         * idle peers cannot grow memory/CPU without bound. Returning nullptr is
         * the factory's existing reject path (see the catch below) -- mvfst drops
         * the connection; nothing is appended to conns or counted. Read on the
         * server EventBase thread, the same thread that mutates conns. */
        if (owner_->conns.size() >= owner_->max_connections)
            return nullptr;

        auto conn = std::make_unique<moq_mvfst_conn>();
        conn->parent = owner_;

        auto t = quic::QuicServerTransport::make(
            sock->getEventBase(), std::move(sock),
            conn.get(), conn.get(), std::move(ctx));
        auto ts = t->getTransportSettings();
        ts.advertisedInitialMaxStreamsBidi = 100;
        ts.advertisedInitialMaxStreamsUni = 100;
        ts.datagramConfig.enabled = true;
        ts.datagramConfig.sendDropOldDataFirst = true;
        if (owner_->max_num_ptos)
            ts.maxNumPTOs = owner_->max_num_ptos;
        if (owner_->initial_rtt_us)
            ts.initialRtt =
                std::chrono::microseconds(owner_->initial_rtt_us);
        t->setTransportSettings(ts);
        conn->transport = t;

        owner_->conns.push_back(std::move(conn));
        auto *c = owner_->conns.back().get();
        owner_->conn_count.store(owner_->conns.size());

        moq_result_t rc = moq_session_create(
            &owner_->session_cfg, 0, &c->session);
        if (rc < 0) {
            c->fatal = true;
            t->close(quic::QuicError(quic::ApplicationErrorCode(0)));
            return t;
        }

        auto acfg = moq::mvfst::adapter::config::server();
        try {
            c->adp = std::make_unique<moq::mvfst::adapter>(
                acfg, c->session, c->transport);
        } catch (...) {
            c->fatal = true;
            t->close(quic::QuicError(quic::ApplicationErrorCode(0)));
        }

        return t;
    } catch (...) {
        return nullptr;
    }
}

static uint64_t managed_now_us()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
}

extern "C" {

static bool cfg_has_field(const moq_mvfst_managed_cfg_t *cfg,
                           size_t offset, size_t size)
{
    return cfg->struct_size >= offset && size <= cfg->struct_size - offset;
}

#define CFG_HAS(cfg, field) \
    cfg_has_field(cfg, offsetof(moq_mvfst_managed_cfg_t, field), \
                  sizeof((cfg)->field))

/* Frozen v0 prefix size: the layout up to and including the callback pointers,
 * ending just before the first appended field (goaway_timeout_us). The
 * pointer-only initializer cannot know the caller's storage size, so it touches
 * only this prefix -- safe for a caller whose moq_mvfst_managed_cfg_t ended
 * here (the original layout, before any field was appended). */
#define MOQ_MVFST_MANAGED_CFG_V0_SIZE \
    (offsetof(moq_mvfst_managed_cfg_t, goaway_timeout_us))

void moq_mvfst_managed_cfg_init(moq_mvfst_managed_cfg_t *cfg)
{
    if (!cfg) return;
    /* Clear and stamp ONLY the frozen v0 prefix: writing sizeof(*cfg) here
     * would overflow a caller compiled against the original (smaller) struct.
     * Appended fields stay disabled (struct_size == prefix); callers that want
     * them use moq_mvfst_managed_cfg_init_sized(). */
    std::memset(cfg, 0, MOQ_MVFST_MANAGED_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_MVFST_MANAGED_CFG_V0_SIZE;
}

void moq_mvfst_managed_cfg_init_sized(moq_mvfst_managed_cfg_t *cfg,
                                      size_t cfg_size)
{
    if (!cfg) return;
    /* Clear exactly what the caller allocated, never more than this library's
     * struct knows about. A caller older than the library passes a smaller
     * cfg_size (we clear/stamp that prefix); a caller newer than the library
     * passes a larger one (we clamp to our sizeof and leave its extra fields to
     * its own initializer). */
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;  /* too small to even stamp */
    std::memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

moq_result_t moq_mvfst_managed_create(
    const moq_mvfst_managed_cfg_t *cfg,
    moq_mvfst_managed_t **out)
{
    if (!cfg || !out) return MOQ_ERR_INVAL;
    *out = nullptr;

    /* Minimum struct_size must include at least through on_pump. */
    if (cfg->struct_size < offsetof(moq_mvfst_managed_cfg_t, on_pump) +
        sizeof(cfg->on_pump))
        return MOQ_ERR_INVAL;

    /* Validate perspective. */
    moq_perspective_t persp = CFG_HAS(cfg, perspective)
        ? cfg->perspective : static_cast<moq_perspective_t>(0);
    if (persp != MOQ_PERSPECTIVE_CLIENT && persp != MOQ_PERSPECTIVE_SERVER)
        return MOQ_ERR_INVAL;

    /* on_pump is required. */
    if (!CFG_HAS(cfg, on_pump) || !cfg->on_pump) return MOQ_ERR_INVAL;

    /* Read all fields via CFG_HAS. */
    const char *host = CFG_HAS(cfg, host) ? cfg->host : NULL;
    int port = CFG_HAS(cfg, port) ? cfg->port : 0;
    const char *cert = CFG_HAS(cfg, cert_path) ? cfg->cert_path : NULL;
    const char *key = CFG_HAS(cfg, key_path) ? cfg->key_path : NULL;
    bool insecure = CFG_HAS(cfg, insecure_skip_verify) ? cfg->insecure_skip_verify : false;
    const char *sni = CFG_HAS(cfg, sni) ? cfg->sni : NULL;

    /* MoQ-over-QUIC ALPN offer → exact session version. EXACT-VERSION only:
     * the session is created eagerly (before the TLS/ALPN handshake), so a
     * real multi-version offer / AUTO cannot be negotiated here yet. */
    moq_version_t version = MOQ_VERSION_DRAFT_16;
    const char *alpn_tok = "moqt-16";
    if (CFG_HAS(cfg, alpn_count) && cfg->alpn_count > 0) {
        if (cfg->alpn_count > 1) return MOQ_ERR_UNSUPPORTED;
        if (!CFG_HAS(cfg, alpn_list) || !cfg->alpn_list || !cfg->alpn_list[0])
            return MOQ_ERR_INVAL;
        alpn_tok = cfg->alpn_list[0];
        moq_version_t v;
        if (!moq_alpn_to_version(alpn_tok, std::strlen(alpn_tok), &v))
            return MOQ_ERR_UNSUPPORTED;   /* unknown / non-MoQ ALPN */
        version = v;
    }

    /* Allocator: optional (NULL = default), validate if provided. */
    const moq_alloc_t *alloc_ptr = CFG_HAS(cfg, alloc) ? cfg->alloc : NULL;
    if (alloc_ptr && (!alloc_ptr->alloc || !alloc_ptr->free || !alloc_ptr->realloc))
        return MOQ_ERR_INVAL;
    const moq_alloc_t *alloc = alloc_ptr ? alloc_ptr : moq_alloc_default();

    bool has_cert = cert && cert[0] != '\0';
    bool has_key = key && key[0] != '\0';

    if (persp == MOQ_PERSPECTIVE_SERVER) {
        if (!has_cert || !has_key) return MOQ_ERR_INVAL;
        if (insecure) return MOQ_ERR_INVAL;
        if (port < 0 || port > 65535) return MOQ_ERR_INVAL;
    } else {
        if (host && host[0] != '\0') {
            if (port < 1 || port > 65535) return MOQ_ERR_INVAL;
        }
        if (insecure && has_cert) return MOQ_ERR_INVAL;
    }
    if (has_cert) { auto *f = std::fopen(cert, "r"); if (!f) return MOQ_ERR_INVAL; std::fclose(f); }
    if (has_key) { auto *f = std::fopen(key, "r"); if (!f) return MOQ_ERR_INVAL; std::fclose(f); }

    auto *m = static_cast<moq_mvfst_managed_t *>(
        alloc->alloc(sizeof(moq_mvfst_managed_t), alloc->ctx));
    if (!m) return MOQ_ERR_NOMEM;
    bool constructed = false;
    try {
        new (m) moq_mvfst_managed_t();
        constructed = true;
        m->alloc = *alloc;
        m->perspective = persp;
        if (host) m->host = host;
        m->port = port;
        m->insecure_skip_verify = insecure;
        if (cert) m->cert_path = cert;
        if (key) m->key_path = key;
        if (sni && sni[0]) m->sni = sni;
        m->cfg_version = version;
        m->alpn = alpn_tok;
        if (CFG_HAS(cfg, max_num_ptos)) m->max_num_ptos = cfg->max_num_ptos;
        if (CFG_HAS(cfg, initial_rtt_us)) m->initial_rtt_us = cfg->initial_rtt_us;
        if (CFG_HAS(cfg, max_connections) && cfg->max_connections)
            m->max_connections = cfg->max_connections;
        m->on_pump = cfg->on_pump;
        m->on_activity = CFG_HAS(cfg, on_activity) ? cfg->on_activity : nullptr;
        m->user_ctx = CFG_HAS(cfg, user_ctx) ? cfg->user_ctx : nullptr;

        moq_session_cfg_init_sized(&m->session_cfg, sizeof(m->session_cfg), &m->alloc, persp);
        m->session_cfg.version = version;   /* exact version (16 == default) */
        if (CFG_HAS(cfg, send_request_capacity)) m->session_cfg.send_request_capacity = cfg->send_request_capacity;
        if (CFG_HAS(cfg, initial_request_capacity)) m->session_cfg.initial_request_capacity = cfg->initial_request_capacity;
        if (CFG_HAS(cfg, max_actions) && cfg->max_actions) m->session_cfg.max_actions = cfg->max_actions;
        if (CFG_HAS(cfg, max_events) && cfg->max_events) m->session_cfg.max_events = cfg->max_events;
        if (CFG_HAS(cfg, max_subscriptions) && cfg->max_subscriptions) m->session_cfg.max_subscriptions = cfg->max_subscriptions;
        if (CFG_HAS(cfg, max_data_streams) && cfg->max_data_streams) m->session_cfg.max_data_streams = cfg->max_data_streams;
        if (CFG_HAS(cfg, send_buffer_size) && cfg->send_buffer_size) m->session_cfg.send_buffer_size = cfg->send_buffer_size;
        if (CFG_HAS(cfg, recv_buffer_size) && cfg->recv_buffer_size) m->session_cfg.recv_buffer_size = cfg->recv_buffer_size;
        if (CFG_HAS(cfg, goaway_timeout_us) && cfg->goaway_timeout_us) m->session_cfg.goaway_timeout_us = cfg->goaway_timeout_us;
    } catch (...) {
        if (constructed) m->~moq_mvfst_managed_t();
        alloc->free(m, sizeof(*m), alloc->ctx);
        return MOQ_ERR_INTERNAL;
    }

    /*
     * signal_init: notify create() of success or failure.
     * Does NOT set thread_exited — only teardown does that.
     */
    auto signal_init = [](moq_mvfst_managed_t *h, moq_result_t rc) {
        h->init_result.store(rc);
        h->init_done.store(true);
        h->init_cv.notify_one();
    };

    /*
     * teardown: sole owner of thread_exited and shutdown notification.
     * Destroys adapter, transport, session, server. Safe with nulls.
     */
    auto teardown = [](moq_mvfst_managed_t *h) {
        h->adapter_ptr.reset();
        if (h->transport) {
            h->transport->close(
                quic::QuicError(quic::ApplicationErrorCode(0)));
            h->transport.reset();
        }
        moq_session_t *s = h->session.exchange(nullptr);
        if (s) moq_session_destroy(s);
        if (h->server_evb) {
            h->server_evb->runInEventBaseThreadAndWait([h]() {
                for (auto &c : h->conns) c->destroy();
                h->conns.clear();
                h->conn_count.store(0);
            });
        } else {
            for (auto &c : h->conns) c->destroy();
            h->conns.clear();
            h->conn_count.store(0);
        }
        if (h->server) {
            h->server->shutdown();
            h->server.reset();
            h->server_evb = nullptr;
        }
        h->thread_exited.store(true);
        h->notify_shutdown();
    };

    try {
    m->thread = std::make_unique<std::thread>(
        [m, signal_init, teardown]()
    {
        m->set_network_tid();

        std::unique_ptr<folly::EventBase> evb_ptr;

        if (m->perspective == MOQ_PERSPECTIVE_SERVER) {
            /* --- Server init --- */
            try {
                std::ifstream cf(m->cert_path);
                std::ifstream kf(m->key_path);
                if (!cf.good() || !kf.good())
                    throw std::runtime_error("open cert/key");
                std::ostringstream cs, ks;
                cs << cf.rdbuf(); ks << kf.rdbuf();

                auto sctx = std::make_shared<
                    fizz::server::FizzServerContext>();
                sctx->setSupportedAlpns({m->alpn});  /* exact version */
                auto mgr = std::make_shared<
                    fizz::server::DefaultCertManager>();
                std::unique_ptr<fizz::SelfCert> sc;
                fizz::Error ferr;
                auto fst = fizz::openssl::CertUtils::makeSelfCert(
                    sc, ferr, cs.str(), ks.str());
                if (fst != fizz::Status::Success || !sc)
                    throw std::runtime_error("load cert/key PEM");
                mgr->addCertAndSetDefault(
                    std::shared_ptr<fizz::SelfCert>(std::move(sc)));
                sctx->setCertManager(mgr);

                m->server = quic::QuicServer::createQuicServer();
                m->server->setFizzContext(sctx);
                m->server->setQuicServerTransportFactory(
                    std::make_unique<managed_server_factory>(m));

                folly::SocketAddress bind_addr;
                if (m->host.empty())
                    bind_addr = folly::SocketAddress("0.0.0.0", m->port);
                else
                    bind_addr = folly::SocketAddress(
                        m->host, static_cast<uint16_t>(m->port), true);

                m->server->start(bind_addr, 1);
                m->server->waitUntilInitialized();

                m->local_port.store(
                    m->server->getAddress().getPort());

                auto evbs = m->server->getWorkerEvbs();
                if (evbs.empty())
                    throw std::runtime_error("no worker evb");
                m->server_evb = evbs[0];

            } catch (...) {
                signal_init(m, MOQ_ERR_INTERNAL);
                teardown(m);
                return;
            }
        } else {
            /* --- Client init --- */
            moq_session_t *sess = nullptr;
            moq_result_t rc = moq_session_create(
                &m->session_cfg, 0, &sess);
            if (rc < 0) {
                signal_init(m, rc);
                teardown(m);
                return;
            }
            m->session.store(sess);

            rc = moq_session_start(sess, managed_now_us());
            if (rc < 0) {
                signal_init(m, rc);
                teardown(m);
                return;
            }

            try {
                if (!m->host.empty()) {
                    /* TLS server name: explicit SNI if set, else the host.
                     * Drives both the SNI sent and the verifier identity. */
                    const std::string &server_name =
                        m->sni.empty() ? m->host : m->sni;

                    evb_ptr = std::make_unique<folly::EventBase>();
                    auto qevb = std::make_shared<
                        quic::FollyQuicEventBase>(evb_ptr.get());
                    auto sock = std::make_unique<
                        quic::FollyQuicAsyncUDPSocket>(qevb);

                    auto fizz_ctx = std::make_shared<
                        fizz::client::FizzClientContext>();
                    fizz_ctx->setSupportedAlpns({m->alpn});

                    std::shared_ptr<const fizz::CertificateVerifier>
                        verifier;
                    if (m->insecure_skip_verify) {
                        verifier = std::make_shared<
                            fizz::InsecureCertificateVerifier>(
                                fizz::VerificationContext::Client);
                    } else {
                        std::shared_ptr<fizz::openssl::
                            OpenSSLCertificateVerifier> chain;
                        if (!m->cert_path.empty()) {
                            auto store =
                                folly::ssl::X509StoreUniquePtr(
                                    X509_STORE_new());
                            if (!store)
                                throw std::runtime_error(
                                    "X509_STORE_new");
                            if (X509_STORE_load_locations(store.get(),
                                    m->cert_path.c_str(), nullptr) != 1)
                                throw std::runtime_error(
                                    "load CA file");
                            X509_STORE_set_flags(store.get(),
                                X509_V_FLAG_PARTIAL_CHAIN);
                            chain = std::make_shared<fizz::openssl::
                                OpenSSLCertificateVerifier>(
                                    fizz::VerificationContext::Client,
                                    std::move(store));
                        } else {
                            chain = std::make_shared<fizz::openssl::
                                OpenSSLCertificateVerifier>(
                                    fizz::VerificationContext::Client);
                        }
                        verifier = std::make_shared<identity_verifier>(
                            std::move(chain), server_name);
                    }

                    auto hsk_builder =
                        quic::FizzClientQuicHandshakeContext::Builder()
                            .setFizzClientContext(fizz_ctx)
                            .setCertificateVerifier(verifier);
                    auto hsk = std::move(hsk_builder).build();

                    m->transport =
                        quic::QuicClientTransport::newClient(
                            qevb, std::move(sock), std::move(hsk));

                    folly::SocketAddress peer(
                        m->host,
                        static_cast<uint16_t>(m->port), true);
                    m->transport->addNewPeerAddress(peer);
                    m->transport->setHostname(server_name);  /* SNI */

                    auto ts =
                        m->transport->getTransportSettings();
                    ts.advertisedInitialMaxStreamsBidi = 100;
                    ts.advertisedInitialMaxStreamsUni = 100;
                    ts.datagramConfig.enabled = true;
                    ts.datagramConfig.sendDropOldDataFirst = true;
                    if (m->max_num_ptos)
                        ts.maxNumPTOs = m->max_num_ptos;
                    if (m->initial_rtt_us)
                        ts.initialRtt =
                            std::chrono::microseconds(
                                m->initial_rtt_us);
                    m->transport->setTransportSettings(ts);

                    m->transport->start(&m->transport_start_cb,
                                         &m->transport_start_cb);

                    auto acfg = moq::mvfst::adapter::config::client();
                    m->adapter_ptr =
                        std::make_unique<moq::mvfst::adapter>(
                            acfg, sess, m->transport);
                    /* Managed mode: gate outbound stream-credit backpressure
                     * (no createStream spam; retry on the MAX_STREAMS
                     * callback). Attach mode never calls this. */
                    m->enable_credit_gating();
                }
            } catch (...) {
                signal_init(m, MOQ_ERR_INTERNAL);
                teardown(m);
                if (evb_ptr) { evb_ptr->loop(); evb_ptr.reset(); }
                return;
            }
        }

        m->running.store(true);
        signal_init(m, MOQ_OK);

        /*
         * pump_tick: service adapters, call on_pump, remove dead
         * conns. For client mode, called directly on the managed
         * thread. For server mode, called on the worker EventBase
         * via runInEventBaseThreadAndWait.
         */
        struct pump_result { bool ok; uint64_t earliest_deadline; };

        auto pump_tick = [](moq_mvfst_managed_t *h) -> pump_result {
            auto now = managed_now_us();

            if (h->adapter_ptr) {
                moq_result_t src = h->adapter_ptr->service(now);
                if (src < 0 || h->adapter_ptr->is_fatal()) {
                    h->set_fatal(h->adapter_ptr->is_fatal()
                        ? h->adapter_ptr->fatal_code() : 0x1);
                    return {false, UINT64_MAX};
                }
            }

            for (auto &c : h->conns) {
                if (c->should_remove() || !c->adp) continue;
                moq_result_t crc = c->adp->service(now);
                if (crc < 0 || c->adp->is_fatal()) c->fatal = true;
                else if (c->adp->is_closed()) c->close_requested = true;
            }

            int pump_rc = h->on_pump(h, now, h->user_ctx);
            if (pump_rc != 0)
                return {false, UINT64_MAX};

            if (h->adapter_ptr) {
                moq_result_t src = h->adapter_ptr->service(now);
                if (src < 0 || h->adapter_ptr->is_fatal()) {
                    h->set_fatal(h->adapter_ptr->is_fatal()
                        ? h->adapter_ptr->fatal_code() : 0x1);
                    return {false, UINT64_MAX};
                }
                if (h->adapter_ptr->is_closed())
                    return {false, UINT64_MAX};
            }
            for (auto &c : h->conns) {
                if (c->should_remove() || !c->adp) continue;
                moq_result_t crc = c->adp->service(now);
                if (crc < 0 || c->adp->is_fatal()) c->fatal = true;
                else if (c->adp->is_closed()) c->close_requested = true;
            }

            auto it = h->conns.begin();
            while (it != h->conns.end()) {
                if ((*it)->should_remove()) {
                    (*it)->destroy();
                    it = h->conns.erase(it);
                } else {
                    ++it;
                }
            }
            h->conn_count.store(h->conns.size());

            /* Latch the negotiated version once a session is ESTABLISHED.
             * EXACT-VERSION adapter: the version is fixed at create, so this
             * just flips negotiated_version() from 0 to cfg_version at the
             * handshake/setup boundary (parity with the picoquic helper).
             * Runs on the network thread, where session access is valid. */
            if (h->negotiated.load(std::memory_order_relaxed) == 0) {
                moq_session_t *cs = h->session.load();
                bool est = cs && moq_session_state(cs) == MOQ_SESS_ESTABLISHED;
                for (auto &c : h->conns) {
                    if (est) break;
                    if (c->session &&
                        moq_session_state(c->session) == MOQ_SESS_ESTABLISHED)
                        est = true;
                }
                if (est)
                    h->negotiated.store((int)h->cfg_version);
            }

            /* Compute earliest session deadline on the correct
             * thread (server conns live on the worker EventBase). */
            uint64_t dl = UINT64_MAX;
            if (h->session.load()) {
                uint64_t d = moq_session_next_deadline_us(
                    h->session.load());
                if (d < dl) dl = d;
            }
            for (auto &c : h->conns) {
                if (!c->session) continue;
                uint64_t d = moq_session_next_deadline_us(c->session);
                if (d < dl) dl = d;
            }
            return {true, dl};
        };

        /* --- Pump loop --- */
        try {
            while (m->running.load()) {
                if (evb_ptr)
                    evb_ptr->loopOnce(EVLOOP_NONBLOCK);
                if (!m->running.load())
                    break;

                pump_result pr;
                if (m->server_evb) {
                    m->server_evb->runInEventBaseThreadAndWait(
                        [m, &pr, &pump_tick]() {
                            pr = pump_tick(m);
                        });
                } else {
                    pr = pump_tick(m);
                }
                if (!pr.ok) {
                    m->running.store(false);
                    break;
                }

                if (m->on_activity)
                    m->on_activity(m, m->user_ctx);

                m->signal_activity();

                /* Sleep no longer than needed to fire the next
                 * session deadline, capped at 5ms for general
                 * responsiveness. Deadline was computed on the
                 * correct thread inside pump_tick. */
                auto wait_us = std::chrono::microseconds(5000);
                if (pr.earliest_deadline != UINT64_MAX) {
                    auto cur = managed_now_us();
                    if (pr.earliest_deadline <= cur) {
                        wait_us = std::chrono::microseconds(0);
                    } else {
                        auto delta = pr.earliest_deadline - cur;
                        if (delta < 5000) wait_us =
                            std::chrono::microseconds(delta);
                    }
                }

                if (wait_us.count() > 0) {
                    std::unique_lock<std::mutex> lk(m->wake_mu);
                    m->wake_cv.wait_for(lk, wait_us, [m]() {
                        return m->wake_flag.load() ||
                               !m->running.load();
                    });
                    m->wake_flag.store(false);
                }
            }
        } catch (...) {
            m->set_fatal(0x1);
            m->running.store(false);
        }

        teardown(m);
        if (evb_ptr) {
            evb_ptr->loop();
            evb_ptr.reset();
        }
    });
    } catch (...) {
        m->~moq_mvfst_managed_t();
        alloc->free(m, sizeof(*m), alloc->ctx);
        return MOQ_ERR_INTERNAL;
    }

    /* Wait for thread to finish init. */
    {
        std::unique_lock<std::mutex> lk(m->init_mu);
        m->init_cv.wait(lk, [m]() { return m->init_done.load(); });
    }

    moq_result_t rc = m->init_result.load();
    if (rc < 0) {
        if (m->thread && m->thread->joinable())
            m->thread->join();
        m->~moq_mvfst_managed_t();
        alloc->free(m, sizeof(*m), alloc->ctx);
        return rc;
    }

    *out = m;
    return MOQ_OK;
}

moq_result_t moq_mvfst_managed_stop(moq_mvfst_managed_t *m)
{
    if (!m) return MOQ_ERR_INVAL;
    if (m->joined.load()) return MOQ_OK;
    if (m->is_managed_thread()) return MOQ_ERR_INVAL;
    if (m->is_network_thread()) return MOQ_ERR_INVAL;

    m->running.store(false);
    m->notify_shutdown();

    if (m->thread && m->thread->joinable())
        m->thread->join();

    m->joined.store(true);
    return MOQ_OK;
}

void moq_mvfst_managed_destroy(moq_mvfst_managed_t *m)
{
    if (!m) return;
    moq_result_t rc = moq_mvfst_managed_stop(m);
    if (rc == MOQ_ERR_INVAL) return;
    moq_alloc_t alloc = m->alloc;
    m->~moq_mvfst_managed_t();
    alloc.free(m, sizeof(*m), alloc.ctx);
}

moq_session_t *moq_mvfst_managed_session(moq_mvfst_managed_t *m)
{
    if (!m) return nullptr;
    if (m->perspective == MOQ_PERSPECTIVE_SERVER) return nullptr;
    if (!m->is_network_thread()) return nullptr;
    return m->session.load();
}

moq_mvfst_conn_t *moq_mvfst_managed_next_conn(
    moq_mvfst_managed_t *m, moq_mvfst_conn_t *prev)
{
    if (!m) return nullptr;
    if (!m->is_network_thread()) return nullptr;
    if (m->conns.empty()) return nullptr;

    if (!prev) return m->conns.front().get();

    for (size_t i = 0; i < m->conns.size(); i++) {
        if (m->conns[i].get() == prev) {
            if (i + 1 < m->conns.size())
                return m->conns[i + 1].get();
            return nullptr;
        }
    }
    return nullptr;
}

moq_session_t *moq_mvfst_conn_session(moq_mvfst_conn_t *conn)
{
    if (!conn) return nullptr;
    if (!conn->parent || !conn->parent->is_network_thread())
        return nullptr;
    return conn->session;
}

moq_result_t moq_mvfst_conn_close(moq_mvfst_conn_t *conn,
                                    uint64_t error_code)
{
    if (!conn) return MOQ_ERR_INVAL;
    if (!conn->parent || !conn->parent->is_network_thread())
        return MOQ_ERR_INVAL;
    conn->close_requested = true;
    if (conn->transport)
        conn->transport->close(quic::QuicError(
            quic::ApplicationErrorCode(error_code)));
    return MOQ_OK;
}

size_t moq_mvfst_managed_conn_count(const moq_mvfst_managed_t *m)
{
    if (!m) return 0;
    return m->conn_count.load();
}

uint16_t moq_mvfst_managed_local_port(const moq_mvfst_managed_t *m)
{
    if (!m) return 0;
    return m->local_port.load();
}

moq_result_t moq_mvfst_managed_wake(moq_mvfst_managed_t *m)
{
    if (!m) return MOQ_ERR_INVAL;
    if (m->is_closed()) return MOQ_ERR_CLOSED;
    m->wake_flag.store(true);
    m->wake_cv.notify_one();
    return MOQ_OK;
}

moq_result_t moq_mvfst_managed_wait(moq_mvfst_managed_t *m,
                                      uint64_t timeout_us)
{
    if (!m) return MOQ_ERR_INVAL;
    if (m->is_closed()) return MOQ_ERR_CLOSED;

    if (m->activity_flag.exchange(false)) {
        if (m->is_closed()) return MOQ_ERR_CLOSED;
        return MOQ_OK;
    }

    if (timeout_us == 0) return MOQ_DONE;

    std::unique_lock<std::mutex> lk(m->wait_mu);
    if (timeout_us == UINT64_MAX) {
        m->wait_cv.wait(lk, [m]() {
            return m->activity_flag.load() || m->is_closed();
        });
    } else {
        m->wait_cv.wait_for(lk, std::chrono::microseconds(timeout_us),
            [m]() {
                return m->activity_flag.load() || m->is_closed();
            });
    }

    if (m->is_closed()) return MOQ_ERR_CLOSED;
    if (m->activity_flag.exchange(false)) return MOQ_OK;
    return MOQ_DONE;
}

bool moq_mvfst_managed_is_fatal(const moq_mvfst_managed_t *m)
{
    if (!m) return false;
    return m->fatal.load();
}

uint64_t moq_mvfst_managed_fatal_code(const moq_mvfst_managed_t *m)
{
    if (!m) return 0;
    return m->fatal_code_val.load();
}

moq_version_t moq_mvfst_managed_negotiated_version(
    const moq_mvfst_managed_t *m)
{
    if (!m) return (moq_version_t)0;
    return (moq_version_t)m->negotiated.load();
}

} /* extern "C" */

/* -- C++ attach adapter ---------------------------------------------- */

namespace moq::mvfst {

static uint64_t now_us_from_clock()
{
    auto now = std::chrono::steady_clock::now();
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()).count());
}

static void iobuf_rcbuf_release(void *ctx,
                                const uint8_t * /*data*/,
                                size_t /*len*/)
{
    delete static_cast<folly::IOBuf *>(ctx);
}

struct __attribute__((visibility("hidden"))) adapter::impl
    : public quic::QuicSocket::ConnectionSetupCallback
    , public quic::QuicSocket::ConnectionCallback
    , public quic::QuicSocket::ReadCallback
    , public quic::QuicSocket::DatagramCallback
{
    moq_session_t *session;
    std::shared_ptr<quic::QuicSocket> socket;

    /* Shared transport bridge — all routing goes through this. */
    moq_transport_bridge_t *shared_bridge = nullptr;
    moq_transport_endpoint_ops_t shared_ops = {};
    mvfst_endpoint_ctx_t shared_ep_ctx = {};

    bool local_is_client = true;

    /* Control stream identification (bidi-control profiles only).
     * Uni-control-pair profiles (draft-18) carry control on unidirectional
     * streams the bridge classifies itself; no bidi is ever control and
     * these stay unset. */
    bool uni_control_mode = false;   /* set at bridge creation */
    bool peer_control_seen = false;
    uint64_t peer_control_stream_id = UINT64_MAX;
    uint64_t local_control_stream_id = UINT64_MAX;

    /* mvfst-specific: track which streams have read callbacks. */
    std::unordered_set<quic::StreamId> read_cb_streams;
    std::unordered_set<quic::StreamId> paused_read_streams;
    std::unordered_set<quic::StreamId> local_bidi_ids;

    /* Called from ep_open_bidi (inside bridge_service). Cannot call
     * bridge functions here (reentrancy). Just register the read
     * callback and track the control stream ID. Failures are handled
     * by register_read_cb setting a deferred error flag. */
    bool deferred_bidi_error = false;

    /* Set by the managed facade: wakes the managed pump when the peer grants
     * outbound stream credit. Unset (null) in attach mode. */
    std::function<void()> credit_wake;

    static void on_local_bidi_opened(void *ctx, uint64_t stream_id) {
        auto *self = static_cast<impl *>(ctx);
        try {
            self->local_bidi_ids.insert(stream_id);

            auto r = self->socket->setReadCallback(stream_id, self);
            if (r.hasError()) {
                self->deferred_bidi_error = true;
                return;
            }
            self->read_cb_streams.insert(stream_id);

            /* Bidi-control profiles: the client's first locally opened
             * bidi is the MoQ control stream. Uni-control-pair profiles
             * open a bidi per request; none is control. */
            if (self->local_is_client && !self->uni_control_mode &&
                self->local_control_stream_id == UINT64_MAX) {
                self->local_control_stream_id = stream_id;
                auto sr = self->socket->setControlStream(stream_id);
                if (sr.has_value())
                    self->deferred_bidi_error = true;
            }
        } catch (...) {
            self->deferred_bidi_error = true;
        }
    }

    impl(moq_session_t *sess, std::shared_ptr<quic::QuicSocket> sock,
         bool is_client)
        : session(sess)
        , socket(sock)
        , local_is_client(is_client)
    {
        mvfst_endpoint_ops_init(&shared_ops, &shared_ep_ctx, sock,
                                 on_local_bidi_opened, this);
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
        if (moq_transport_bridge_create(&bcfg, sess, &shared_ops,
                                         &shared_ep_ctx, &shared_bridge) < 0)
            throw std::bad_alloc();
        uni_control_mode =
            moq_transport_bridge_uses_uni_control(shared_bridge);
    }

    ~impl() {
        teardown_callbacks();
        moq_transport_bridge_destroy(shared_bridge);
    }

    moq_perspective_t local_perspective() const {
        return local_is_client ? MOQ_PERSPECTIVE_CLIENT
                               : MOQ_PERSPECTIVE_SERVER;
    }

    void install_callbacks() {
        socket->setConnectionSetupCallback(this);
        socket->setConnectionCallback(this);
        auto r = socket->setDatagramCallback(this);
        if (r.hasError()) {
            moq_transport_bridge_on_transport_error(
                shared_bridge, 0x1, now_us_from_clock());
        }
    }

    void teardown_callbacks() {
        for (auto id : read_cb_streams)
            socket->setReadCallback(id, nullptr, std::nullopt);
        read_cb_streams.clear();
        paused_read_streams.clear();
        socket->setConnectionSetupCallback(nullptr);
        socket->setConnectionCallback(nullptr);
        socket->setDatagramCallback(nullptr);
    }

    bool is_fatal() const {
        return moq_transport_bridge_is_fatal(shared_bridge);
    }

    bool is_closed() const {
        return moq_transport_bridge_is_closed(shared_bridge);
    }

    bool is_terminal() const {
        return moq_transport_bridge_is_terminal(shared_bridge);
    }

    uint64_t fatal_code() const {
        return moq_transport_bridge_fatal_code(shared_bridge);
    }

    void register_read_cb(quic::StreamId id) {
        auto r = socket->setReadCallback(id, this);
        if (r.hasError()) {
            moq_transport_bridge_on_transport_error(
                shared_bridge, 0x1, now_us_from_clock());
            return;
        }
        read_cb_streams.insert(id);
        paused_read_streams.erase(id);
    }

    void unregister_read_cb(quic::StreamId id) {
        socket->setReadCallback(id, nullptr, std::nullopt);
        read_cb_streams.erase(id);
        paused_read_streams.erase(id);
        local_bidi_ids.erase(id);
    }

    void pause_read_cb(quic::StreamId id) {
        if (!read_cb_streams.count(id)) return;
        if (paused_read_streams.count(id)) return;
        auto r = socket->pauseRead(id);
        if (!r.hasError()) paused_read_streams.insert(id);
    }

    void resume_paused_reads() {
        auto copy = paused_read_streams;
        for (auto id : copy) {
            if (!moq_transport_bridge_stream_has_pending(shared_bridge, id)) {
                auto r = socket->resumeRead(id);
                if (!r.hasError()) paused_read_streams.erase(id);
            }
        }
    }

    /*
     * Central service: delegates to shared bridge, then resumes reads.
     */
    moq_result_t service_all(uint64_t now) {
        if (is_terminal()) {
            return is_fatal() ? MOQ_ERR_INTERNAL : MOQ_OK;
        }

        moq_result_t rc = moq_transport_bridge_service(shared_bridge, now);

        if (deferred_bidi_error) {
            deferred_bidi_error = false;
            moq_transport_bridge_on_transport_error(
                shared_bridge, 0x1, now);
            return MOQ_ERR_INTERNAL;
        }

        resume_paused_reads();
        return rc;
    }

    /* -- ConnectionSetupCallback --------------------------------------- */

    void onTransportReady() noexcept override {
        try { service_all(now_us_from_clock()); } catch (...) {
            moq_transport_bridge_on_transport_error(
                shared_bridge, 0x1, now_us_from_clock());
        }
    }
    void onReplaySafe() noexcept override {
        try { service_all(now_us_from_clock()); } catch (...) {
            moq_transport_bridge_on_transport_error(
                shared_bridge, 0x1, now_us_from_clock());
        }
    }
    void onFullHandshakeDone() noexcept override {}

    void onConnectionSetupError(quic::QuicError error) noexcept override {
        (void)error;
        moq_transport_bridge_on_transport_error(
            shared_bridge, 0x1, now_us_from_clock());
    }

    /* -- ConnectionCallback -------------------------------------------- */

    void onNewBidirectionalStream(quic::StreamId id) noexcept override {
        try {
            register_read_cb(id);
        } catch (...) {
            moq_transport_bridge_on_transport_error(
                shared_bridge, 0x1, now_us_from_clock());
        }
    }

    void onNewUnidirectionalStream(quic::StreamId id) noexcept override {
        try {
            register_read_cb(id);
        } catch (...) {
            moq_transport_bridge_on_transport_error(
                shared_bridge, 0x1, now_us_from_clock());
        }
    }

    /* Peer granted more outbound stream credit (MAX_STREAMS). Clear the
     * matching credit-block so the next service() retries the pending open,
     * and wake the managed pump. No-op for attach mode (credit_wake unset;
     * the blocked flag is ignored when gating is off). */
    void onUnidirectionalStreamsAvailable(uint64_t) noexcept override {
        if (!shared_ep_ctx.credit_gating) return;  /* attach mode: inert */
        shared_ep_ctx.uni_blocked = false;
        shared_ep_ctx.uni_credit_grants.fetch_add(1, std::memory_order_relaxed);
        if (credit_wake) credit_wake();
    }

    void onBidirectionalStreamsAvailable(uint64_t) noexcept override {
        if (!shared_ep_ctx.credit_gating) return;  /* attach mode: inert */
        shared_ep_ctx.bidi_blocked = false;
        shared_ep_ctx.bidi_credit_grants.fetch_add(1, std::memory_order_relaxed);
        if (credit_wake) credit_wake();
    }

    void onStopSending(quic::StreamId id,
                        quic::ApplicationErrorCode error) noexcept override {
        try {
            uint64_t now = now_us_from_clock();
            moq_result_t rc = moq_transport_bridge_on_peer_stop_sending(
                shared_bridge, id, error, now);
            if (rc == MOQ_ERR_WOULD_BLOCK)
                pause_read_cb(id);
            service_all(now);
        } catch (...) {
            moq_transport_bridge_on_transport_error(
                shared_bridge, 0x1, now_us_from_clock());
        }
    }

    void onConnectionEnd() noexcept override {
        moq_transport_bridge_on_transport_close(
            shared_bridge, 0, now_us_from_clock());
    }

    void onConnectionError(quic::QuicError error) noexcept override {
        uint64_t code = 0x1;
        if (auto *tc = error.code.asTransportErrorCode())
            code = static_cast<uint64_t>(*tc);
        else if (auto *app = error.code.asApplicationErrorCode())
            code = *app;
        moq_transport_bridge_on_transport_error(
            shared_bridge, code, now_us_from_clock());
    }

    /* -- ReadCallback -------------------------------------------------- */

    void readAvailable(quic::StreamId id) noexcept override {
        try {
            if (is_terminal()) return;
            if (moq_transport_bridge_stream_has_pending(shared_bridge, id))
                return;

            auto result = socket->read(id, 0);
            if (result.hasError()) return;

            auto &[buf, eof] = *result;
            if (!buf && !eof) return;

            const uint8_t *data = nullptr;
            size_t len = 0;
            if (buf) {
                buf->coalesce();
                data = buf->data();
                len = buf->length();
            }

            uint64_t now = now_us_from_clock();
            moq_result_t rc = MOQ_OK;
            bool is_bidi = quic::isBidirectionalStream(id);
            bool is_local = local_bidi_ids.count(id) > 0;

            if (is_bidi) {
                /* Bidi-control profiles: MoQ control is client-initiated
                 * bidi stream 0 (marked for mvfst priority on first
                 * encounter). Uni-control-pair profiles route every bidi
                 * as a request stream -- including stream 0. */
                bool is_control = (id == 0) && !uni_control_mode;
                if (is_control && !peer_control_seen && !is_local) {
                    peer_control_seen = true;
                    peer_control_stream_id = id;
                    auto sr = socket->setControlStream(id);
                    if (sr.has_value()) {
                        moq_transport_bridge_on_transport_error(
                            shared_bridge, 0x1, now);
                        service_all(now);
                        return;
                    }
                }
                if (!is_control)
                    is_control = (id == local_control_stream_id);

                if (is_control) {
                    rc = moq_transport_bridge_on_peer_control_bytes(
                        shared_bridge, id, data, len, eof, now);
                } else {
                    rc = moq_transport_bridge_on_peer_bidi_bytes(
                        shared_bridge, id, data, len, eof, now);
                }
            } else if (buf && len > 0) {
                auto *raw = buf.release();
                moq_rcbuf_t *input = nullptr;
                moq_result_t wrc = moq_rcbuf_wrap(
                    moq_alloc_default(),
                    raw->data(), raw->length(),
                    iobuf_rcbuf_release, raw,
                    &input);
                if (wrc < 0) {
                    delete raw;
                    moq_transport_bridge_on_transport_error(
                        shared_bridge, 0x1, now);
                    service_all(now);
                    return;
                }
                rc = moq_transport_bridge_on_peer_uni_rcbuf(
                    shared_bridge, id, input, eof, now);
                moq_rcbuf_decref(input);
            } else {
                rc = moq_transport_bridge_on_peer_uni_bytes(
                    shared_bridge, id, data, len, eof, now);
            }

            if (rc == MOQ_ERR_WOULD_BLOCK) {
                pause_read_cb(id);
            } else if (rc < 0 && rc != MOQ_ERR_CLOSED) {
                unregister_read_cb(id);
            } else if (eof && rc >= 0) {
                unregister_read_cb(id);
            }

            service_all(now);
        } catch (...) {
            moq_transport_bridge_on_transport_error(
                shared_bridge, 0x1, now_us_from_clock());
        }
    }

    void readError(quic::StreamId id,
                    quic::QuicError error) noexcept override {
        try {
            uint64_t code = 0;
            if (auto *app = error.code.asApplicationErrorCode())
                code = *app;

            uint64_t now = now_us_from_clock();

            /* Bidi-control profiles: a control stream reset is a protocol
             * violation; route through on_transport_error because the
             * bridge does not special-case the BIDI control stream. (In
             * uni-control-pair mode these ids are never set; a peer reset
             * of its uni control channel falls through to
             * on_peer_stream_reset, where the bridge terminates the
             * session itself.) */
            if (id == local_control_stream_id ||
                id == peer_control_stream_id) {
                moq_transport_bridge_on_transport_error(
                    shared_bridge, code ? code : 0x1, now);
                unregister_read_cb(id);
                service_all(now);
                return;
            }

            moq_result_t rc = moq_transport_bridge_on_peer_stream_reset(
                shared_bridge, id, code, now);
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                pause_read_cb(id);
            } else {
                unregister_read_cb(id);
            }
            service_all(now);
        } catch (...) {
            moq_transport_bridge_on_transport_error(
                shared_bridge, 0x1, now_us_from_clock());
        }
    }

    /* -- DatagramCallback ---------------------------------------------- */

    void onDatagramsAvailable() noexcept override {
        try {
            auto result = socket->readDatagramBufs();
            if (result.hasError()) return;

            uint64_t now = now_us_from_clock();
            for (auto &buf : *result) {
                if (!buf) continue;
                buf->coalesce();
                moq_transport_bridge_on_peer_datagram(
                    shared_bridge, buf->data(), buf->length(), now);
            }
            service_all(now);
        } catch (...) {
            moq_transport_bridge_on_transport_error(
                shared_bridge, 0x1, now_us_from_clock());
        }
    }

};

adapter::adapter(config cfg,
                 moq_session_t *session,
                 std::shared_ptr<quic::QuicSocket> socket)
{
    if (cfg.local_perspective != MOQ_PERSPECTIVE_CLIENT &&
        cfg.local_perspective != MOQ_PERSPECTIVE_SERVER)
        throw std::invalid_argument(
            "adapter::config::local_perspective must be CLIENT or SERVER");
    if (!session)
        throw std::invalid_argument("adapter: session must not be null");
    if (moq_session_perspective(session) != cfg.local_perspective)
        throw std::invalid_argument(
            "adapter: config perspective must match session perspective");
    if (!socket)
        throw std::invalid_argument("adapter: socket must not be null");
    impl_ = std::make_unique<struct impl>(
        session, std::move(socket),
        cfg.local_perspective == MOQ_PERSPECTIVE_CLIENT);
    impl_->install_callbacks();
}

adapter::~adapter() {
}

moq_session_t *adapter::raw_session() const noexcept {
    return impl_->session;
}

// The socket() invariant below relies on two facts: (1) the constructor
// validates the socket at construction time (throws on null, so a live
// adapter never has a null socket), and (2) the adapter is non-movable -
// the user-declared destructor suppresses the implicit move constructor,
// so impl_ can never be moved-from to null. Guard (2) at compile time so
// a future move constructor cannot silently break the invariant.
static_assert(!std::is_move_constructible_v<adapter>,
              "adapter must stay non-movable: socket() relies on impl_ "
              "never being moved-from (see construction invariant)");

quic::QuicSocket &adapter::socket() const noexcept {
    // Invariant: a successfully-constructed adapter always holds a
    // non-null socket. The constructor rejects a null socket (throws
    // std::invalid_argument), and impl_->socket (a shared_ptr) is set
    // once at construction and never reassigned. This branch is therefore
    // unreachable; a debug-only assert documents/guards the invariant
    // instead of a production process abort.
    assert(impl_->socket &&
           "adapter::socket: socket is null (construction invariant violated)");
    return *impl_->socket;
}

moq_result_t adapter::service(uint64_t now_us) {
    try {
        return impl_->service_all(now_us);
    } catch (const std::bad_alloc &) {
        moq_transport_bridge_on_transport_error(
            impl_->shared_bridge, 0x1, now_us);
        return MOQ_ERR_NOMEM;
    } catch (...) {
        moq_transport_bridge_on_transport_error(
            impl_->shared_bridge, 0x1, now_us);
        return MOQ_ERR_INTERNAL;
    }
}

bool adapter::is_fatal() const noexcept {
    return impl_->is_fatal();
}

bool adapter::is_closed() const noexcept {
    return impl_->is_closed();
}

uint64_t adapter::fatal_code() const noexcept {
    return impl_->fatal_code();
}

bool adapter::has_pending() const noexcept {
    return moq_transport_bridge_has_pending(impl_->shared_bridge);
}

} // namespace moq::mvfst

/* -- Managed credit-gating wiring -------------------------------------- *
 * Defined at global scope (moq_mvfst_managed is a global C struct) and after
 * moq::mvfst::adapter::impl is complete. The friend declaration on adapter
 * grants access to its private impl_. */

void moq_mvfst_managed::enable_credit_gating()
{
    if (!adapter_ptr) return;
    auto *im = adapter_ptr->impl_.get();
    im->shared_ep_ctx.credit_gating = true;
    moq_mvfst_managed *self = this;
    im->credit_wake = [self]() {
        self->wake_flag.store(true);
        self->wake_cv.notify_all();
    };
}

#ifdef MOQ_MVFST_TESTING
/* Test-only telemetry: compiled only into the test-internals object library
 * (CMake target moq-adapter-mvfst-test-internals, which sets MOQ_MVFST_TESTING
 * and default visibility). The shipped moq-adapter-mvfst defines neither these
 * member functions nor the C accessors below, so no test seam reaches the
 * installed ABI and shared-library symbol hygiene is unaffected. */
uint64_t moq_mvfst_managed::credit_block_count() const
{
    if (!adapter_ptr) return 0;
    const auto *im = adapter_ptr->impl_.get();
    return im->shared_ep_ctx.uni_block_count.load(std::memory_order_relaxed) +
           im->shared_ep_ctx.bidi_block_count.load(std::memory_order_relaxed);
}

uint64_t moq_mvfst_managed::credit_grant_count() const
{
    if (!adapter_ptr) return 0;
    const auto *im = adapter_ptr->impl_.get();
    return im->shared_ep_ctx.uni_credit_grants.load(std::memory_order_relaxed) +
           im->shared_ep_ctx.bidi_credit_grants.load(std::memory_order_relaxed);
}

/* Declared in mvfst_managed_testing.h (not in <moq/mvfst.h>). */
extern "C" uint64_t moq_mvfst_managed_credit_block_count(
    const moq_mvfst_managed_t *m)
{
    return m ? m->credit_block_count() : 0;
}

extern "C" uint64_t moq_mvfst_managed_credit_grant_count(
    const moq_mvfst_managed_t *m)
{
    return m ? m->credit_grant_count() : 0;
}
#endif /* MOQ_MVFST_TESTING */
