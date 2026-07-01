/*
 * Managed proxygen WebTransport client facade.
 *
 * Owns a network thread + folly EventBase. On that EventBase it performs the
 * HTTP/3 WebTransport CONNECT by hand -- a manually built mvfst QUIC client
 * transport (quic::QuicClientTransport) driven by a proxygen HQUpstreamSession
 * + HTTPSessionBase::sendWebTransportRequest -- then negotiates the MoQ version
 * from the selected WT-Protocol, creates the
 * moq_session_t, and wraps the existing attach adapter (moq::wt::Adapter) to
 * drive the libmoq transport bridge. The libmoq pump runs as EventBase-owned
 * work (no manual loop, no blockingWait): the EventBase owns all progress.
 *
 * Threading: the session, the attach Adapter, the WebTransport, and the
 * forwarding handler are created and touched only on the EventBase thread.
 * create/stop/destroy run on the app thread; wake()/wait()/is_fatal()/... are
 * cross-thread safe.
 */

#include <moq/proxygen_wt_managed.h>
#include <moq/proxygen_wt.hpp>            /* attach moq::wt::Adapter */
#include <moq/session.h>

#include "../../common/moq_alpn.h"        /* moq_alpn_to_version / _for_version */

#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/http/HTTPMethod.h>
#include <proxygen/lib/http/coro/client/HTTPCoroConnector.h>  /* TLS helpers only */
#include <proxygen/lib/http/session/HQUpstreamSession.h>
#include <proxygen/lib/http/webtransport/HTTPWebTransport.h>
#include <proxygen/lib/http/webtransport/QuicWtSession.h>    /* H3WtSession */
#include <proxygen/lib/http/webtransport/WebTransport.h>
#include <proxygen/lib/http/webtransport/WtStreamManager.h>  /* MaxStreams* */

#include <fizz/client/FizzClientContext.h>
#include <fizz/protocol/CertificateVerifier.h>   /* fizz::InsecureCertificateVerifier */

#include <quic/client/QuicClientTransport.h>
#include <quic/common/events/FollyQuicEventBase.h>
#include <quic/common/udpsocket/FollyQuicAsyncUDPSocket.h>
#include <quic/fizz/client/handshake/FizzClientQuicHandshakeContext.h>

#include <wangle/acceptor/TransportInfo.h>

#include <folly/io/async/ssl/OpenSSLTransportCertificate.h>
#include <folly/ScopeGuard.h>
#include <folly/Try.h>
#include <folly/coro/Baton.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Task.h>
#include <folly/io/async/EventBase.h>

#include <openssl/x509v3.h>

#include <arpa/inet.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace proxygen;

/* WebTransport stream-credit compatibility shim (see connect_coro): peers like
 * moqx map WT streams onto QUIC streams and do not drive proxygen's newer
 * capsule-based WT stream flow control. We advertise this many WT streams in
 * BOTH directions so proxygen does not throttle the session below the real
 * QUIC stream limits -- inbound via our egress SETTINGS (lets the peer open
 * peer->us streams = RECEIVE) and outbound via H3WtSession::onMaxStreams (lets
 * us open us->peer streams = PUBLISH). The QUIC stream credit remains the true
 * lower bound. */
constexpr uint64_t kWtStreamCredit = 1u << 24;

/* WebTransport DATA flow-control credit (bytes) -- the byte-count analog of the
 * stream-count shim above. proxygen also caps INBOUND WT data
 * per stream and per connection from our advertised WT_INITIAL_MAX_STREAM_DATA_*
 * / WT_INITIAL_MAX_DATA, which default low; a peer that maps WT onto QUIC and
 * does not drive WT data-credit capsules (moqx) then loses bytes of any object
 * larger than the initial window (the observed <=4KB-clean / 16KB-lost
 * threshold). Advertise generous initial windows: per-stream covers a large GOP
 * subgroup, connection gives headroom for concurrent streams. proxygen
 * replenishes as the app reads; these WT windows are compatibility ceilings,
 * not a replacement for QUIC flow control. */
constexpr uint64_t kWtStreamDataCredit = 1u << 24;   /* 16 MiB per stream */
constexpr uint64_t kWtConnDataCredit   = 1u << 26;   /* 64 MiB per connection */

/*
 * Certificate verifier: chain validation + host/IP identity.
 *
 * Delegates chain/CA validation to proxygen's configured Fizz verifier, then
 * binds the accepted leaf certificate to the expected DNS name or IP literal.
 * In Fizz-style verification identity matching is part of the verifier policy:
 * transport->setHostname() below supplies SNI to the handshake, but does NOT by
 * itself make the verifier check the peer certificate's identity. Without this
 * wrapper a certificate chaining to a trusted CA for a *different* name would be
 * accepted (p1-high__01). Mirrors the mvfst adapter's identity_verifier.
 *
 * IP literals (detected via inet_pton) match only iPAddress SANs via
 * X509_check_ip_asc. DNS names match only dNSName SANs via X509_check_host, so a
 * cert with DNS:127.0.0.1 cannot satisfy an IP-literal connection to 127.0.0.1.
 */
class identity_verifier final : public fizz::CertificateVerifier {
public:
    identity_verifier(
        std::shared_ptr<const fizz::CertificateVerifier> chain,
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

    std::shared_ptr<const fizz::CertificateVerifier> chain_;
    std::string expected_host_;
    bool is_ip_;
};

uint64_t now_us_steady()
{
    return (uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

/* Forwarding WebTransportHandler: handed to sendWtReq before the attach
 * Adapter exists, then pointed at it (synchronously, on the EventBase thread)
 * once CONNECT returns the WebTransport. No callbacks can interleave in that
 * synchronous window because everything runs on the one EventBase thread. */
struct fwd_handler : public WebTransportHandler {
    moq::wt::Adapter *target = nullptr;
    /* The WT session is delivered here (on the EventBase thread) once the
     * extended CONNECT succeeds; connect_coro reads it after the response
     * future resolves. */
    std::shared_ptr<WebTransport> wt_captured;

    void onWebTransportSession(std::shared_ptr<WebTransport> w) noexcept override {
        wt_captured = std::move(w);
    }
    void onNewUniStream(WebTransport::StreamReadHandle *rh) noexcept override {
        if (target) target->onNewUniStream(rh);
    }
    void onNewBidiStream(WebTransport::BidiStreamHandle bh) noexcept override {
        if (target) target->onNewBidiStream(bh);
    }
    void onDatagram(std::unique_ptr<folly::IOBuf> d) noexcept override {
        if (target) target->onDatagram(std::move(d));
    }
    void onSessionEnd(folly::Optional<uint32_t> e) noexcept override {
        if (target) target->onSessionEnd(e);
    }
    void onSessionDrain() noexcept override {
        if (target) target->onSessionDrain();
    }
};

} // namespace

struct moq_proxygen_wt_managed {
    moq_alloc_t alloc{};

    /* Copied config (network-thread reads only after init). */
    std::string host;
    std::string sni;
    std::string path;
    std::string ca_file;
    int         port = 0;
    bool        insecure = false;
    bool        send_request_capacity = false;
    uint64_t    initial_request_capacity = 0;
    uint64_t    goaway_timeout_us = 0;
    std::vector<std::string> alpn;          /* WT-Available-Protocols offer
                                             * (never empty after create) */

    moq_proxygen_wt_pump_fn     on_pump = nullptr;
    moq_proxygen_wt_activity_fn on_activity = nullptr;
    void                       *user_ctx = nullptr;

    /* Cross-thread state. */
    std::atomic<int>      negotiated{0};
    std::atomic<bool>     fatal{false};
    std::atomic<uint64_t> fatal_code_val{0};
    std::atomic<bool>     closed{false};
    std::atomic<bool>     running{false};
    std::atomic<bool>     thread_exited{false};
    std::atomic<bool>     joined{false};

    /* Network thread + its EventBase (owns all progress). */
    std::unique_ptr<std::thread> thread;
    folly::EventBase     evb;
    std::thread::id      net_tid;

    /* EventBase-thread-only objects. */
    std::atomic<moq_session_t *> session{nullptr};
    std::shared_ptr<quic::QuicClientTransport> transport;
    std::shared_ptr<WebTransport> wt;
    std::unique_ptr<moq::wt::Adapter> adapter;
    proxygen::HQUpstreamSession *http_session = nullptr;  /* self-managed */
    fwd_handler         *fwd = nullptr;      /* owned by the live WT session */
    bool                 pump_scheduled = false;

    /* init handshake (app thread blocks in create() until set). */
    std::mutex              init_mu;
    std::condition_variable init_cv;
    std::atomic<bool>       init_done{false};
    std::atomic<moq_result_t> init_result{MOQ_ERR_INTERNAL};

    /* wait()/activity signalling. */
    std::mutex              wait_mu;
    std::condition_variable wait_cv;
    std::atomic<bool>       activity{false};

    bool on_net_thread() const { return std::this_thread::get_id() == net_tid; }

    void signal_init(moq_result_t r) {
        init_result.store(r);
        { std::lock_guard<std::mutex> lk(init_mu); init_done.store(true); }
        init_cv.notify_all();
    }
    void mark_activity() {
        activity.store(true);
        wait_cv.notify_all();
    }
    void set_fatal(uint64_t code) {
        fatal_code_val.store(code);
        fatal.store(true);
        mark_activity();
    }

    /* One pump cycle on the EventBase thread: service the adapter, run the
     * app pump, service again (flush queued actions), then reschedule. */
    void pump_once();
    void schedule_pump_soon();   /* (re)arm the pump on the EventBase */

    folly::coro::Task<void> connect_coro();   /* CONNECT + session + adapter */
};

void moq_proxygen_wt_managed::schedule_pump_soon()
{
    /* Re-arm the pump on the EventBase at a fixed 5ms responsiveness cap,
     * coalesced via pump_scheduled. (Deadline-aware scheduling -- shortening
     * this when a session deadline is nearer -- is a future refinement.) */
    if (pump_scheduled || !running.load()) return;
    pump_scheduled = true;
    evb.runAfterDelay([this]() {
        pump_scheduled = false;
        pump_once();
    }, 5);
}

void moq_proxygen_wt_managed::pump_once()
{
    if (!running.load()) return;
    auto *a = adapter.get();
    if (!a) { schedule_pump_soon(); return; }

    auto run_service = [&]() -> bool {
        moq_result_t rc = a->service();
        if (rc < 0 || a->is_fatal()) {
            set_fatal(a->is_fatal() ? a->fatal_code() : 0x1);
            return false;
        }
        if (a->is_closed()) { closed.store(true); mark_activity(); return false; }
        return true;
    };

    if (!run_service()) return;

    moq_session_t *s = session.load();
    if (on_pump && s) {
        int prc = on_pump(this, now_us_steady(), user_ctx);
        if (prc != 0) { running.store(false); evb.terminateLoopSoon(); return; }
    }
    if (!run_service()) return;

    if (on_activity) on_activity(this, user_ctx);
    mark_activity();
    schedule_pump_soon();
}

folly::coro::Task<void> moq_proxygen_wt_managed::connect_coro()
{
    try {
        const std::string &server_name = sni.empty() ? host : sni;

        /* TLS: reuse proxygen's verifier factory for chain/CA trust (tp.caPaths
         * supplies the trust roots when ca_file is set), then wrap it so server
         * identity (DNS/IP) is enforced against server_name -- proxygen's factory
         * verifier does NOT bind the expected host on its own. setHostname()
         * below is SNI only; identity enforcement lives in identity_verifier
         * (p1-high__01). Build our own client context pinned to h3. */
        coro::HTTPCoroConnector::TLSParams tp({"h3"});
        if (!ca_file.empty()) tp.caPaths = {ca_file};
        std::shared_ptr<const fizz::CertificateVerifier> verifier =
            std::make_shared<identity_verifier>(
                coro::HTTPCoroConnector::makeFizzCertVerifier(tp),
                server_name);
        if (insecure) {
            /* Accept any server cert, no identity check (testing only). */
            verifier = std::make_shared<fizz::InsecureCertificateVerifier>(
                fizz::VerificationContext::Client);
        }
        auto fizz_ctx = std::make_shared<fizz::client::FizzClientContext>();
        fizz_ctx->setSupportedAlpns({"h3"});

        /* Build the QUIC client transport by hand (mvfst-style). This is the
         * path that registers its UDP socket fd on the owning EventBase
         * correctly on macOS; HTTPCoroConnector's internal QUIC setup did not
         * (observed as "failed to register event handler for fd N"). */
        auto qevb = std::make_shared<quic::FollyQuicEventBase>(&evb);
        auto qsock = std::make_unique<quic::FollyQuicAsyncUDPSocket>(qevb);
        auto hsk = quic::FizzClientQuicHandshakeContext::Builder()
                       .setFizzClientContext(fizz_ctx)
                       .setCertificateVerifier(verifier)
                       .build();
        transport = quic::QuicClientTransport::newClient(
            qevb, std::move(qsock), std::move(hsk));
        transport->addNewPeerAddress(
            folly::SocketAddress(host, (uint16_t)port, /*allowNameLookup*/true));
        /* SNI only -- server identity is verified by identity_verifier above,
         * not by this call. */
        transport->setHostname(server_name);
        {
            auto ts = transport->getTransportSettings();
            ts.advertisedInitialMaxStreamsBidi = 100;
            ts.advertisedInitialMaxStreamsUni = 100;
            ts.datagramConfig.enabled = true;
            ts.datagramConfig.sendDropOldDataFirst = true;
            transport->setTransportSettings(ts);
        }

        /* H3 upstream session over the transport. startNow() prepares the
         * session machinery; transport->start(session, session) initiates the
         * QUIC handshake with the session as both connection callbacks (the
         * canonical proxygen HQConnector sequence). A coro Baton bridges the
         * ConnectCallback completion into this coroutine. */
        struct connect_cb : public proxygen::HQSession::ConnectCallback {
            folly::coro::Baton baton;
            bool ok = false;
            void onReplaySafe() noexcept override { ok = true; baton.post(); }
            void connectError(quic::QuicError) noexcept override {
                ok = false; baton.post();
            }
        } ccb;

        wangle::TransportInfo tinfo;
        auto *sess = new proxygen::HQUpstreamSession(
            std::chrono::milliseconds(60000),   /* transactions timeout */
            std::chrono::milliseconds(15000),   /* connect timeout */
            nullptr,                            /* controller */
            tinfo,
            nullptr);                           /* info callback */
        http_session = sess;
        sess->setSocket(transport);
        sess->setConnectCallback(&ccb);
        /* The session can outlive this coroutine, so it must never retain a
         * pointer to the stack ConnectCallback past any return -- clear it on
         * every exit path (success, early-return, exception). */
        auto cb_guard = folly::makeGuard([sess]() {
            sess->setConnectCallback(nullptr);
        });
        /* Advertise WebTransport over H3 in our SETTINGS: extended CONNECT,
         * WebTransport, and H3 datagrams. Without these supportsWebTransport()
         * stays false and sendWebTransportRequest() throws "WebTransport not
         * supported". Must precede startNow(), which emits the SETTINGS frame.
         *
         * WT_INITIAL_MAX_STREAMS_{UNI,BIDI} set our INBOUND WT stream credit:
         * proxygen derives the WtStreamManager selfMaxStreams* (how many WT
         * streams the peer may open peer->us) from these advertised settings and
         * defaults them to 1, so without seeding them a peer that maps WT streams
         * onto QUIC streams (moqx) can open only one inbound stream and the
         * receiver gets no media. This is the inbound mirror of the outbound
         * onMaxStreams shim below; the QUIC stream limit stays the real bound.
         *
         * WT_INITIAL_MAX_STREAM_DATA_{UNI,BIDI} + WT_INITIAL_MAX_DATA set our
         * inbound WT DATA credit per stream / per connection:
         * the defaults are low, so a peer that does not drive WT data-credit
         * capsules loses bytes of any object larger than the initial window.
         * Seed generous windows so large objects (GOP subgroups) arrive intact;
         * the connection window is raised consistently so it is not the next cap. */
        sess->setEgressSettings({
            {proxygen::SettingsId::_HQ_DATAGRAM, 1},
            {proxygen::SettingsId::ENABLE_CONNECT_PROTOCOL, 1},
            {proxygen::SettingsId::ENABLE_WEBTRANSPORT, 1},
            {proxygen::SettingsId::WT_INITIAL_MAX_DATA, kWtConnDataCredit},
            {proxygen::SettingsId::WT_INITIAL_MAX_STREAMS_UNI, kWtStreamCredit},
            {proxygen::SettingsId::WT_INITIAL_MAX_STREAMS_BIDI, kWtStreamCredit},
            {proxygen::SettingsId::WT_INITIAL_MAX_STREAM_DATA_UNI,
             kWtStreamDataCredit},
            {proxygen::SettingsId::WT_INITIAL_MAX_STREAM_DATA_BIDI,
             kWtStreamDataCredit},
        });
        sess->startNow();
        transport->start(sess, sess);

        co_await ccb.baton;
        if (!ccb.ok) { set_fatal(0x1); signal_init(MOQ_ERR_INTERNAL); co_return; }
        /* WebTransport needs the peer's SETTINGS (its WT support) before the
         * extended CONNECT; they can arrive after the handshake is replay-safe.
         * Poll the EventBase with a bounded budget so a silent or erroring peer
         * cannot hang the blocking create() path forever. */
        for (int i = 0; i < 300 /* 300 * 50ms = 15s */ &&
                        !sess->supportsWebTransport(); i++) {
            co_await folly::coro::sleep(std::chrono::milliseconds(50));
        }
        if (!sess->supportsWebTransport()) {
            set_fatal(0x1); signal_init(MOQ_ERR_INTERNAL); co_return;
        }

        HTTPMessage req;
        req.setMethod(HTTPMethod::CONNECT);
        req.setURL(path.empty() ? std::string("/moq") : path);
        req.getHeaders().set(proxygen::HTTP_HEADER_HOST, server_name);
        req.setUpgradeProtocol("webtransport");
        HTTPWebTransport::setWTAvailableProtocols(req, alpn);  /* never empty */

        auto handler = std::make_unique<fwd_handler>();
        fwd_handler *fwd_raw = handler.get();   /* owned by the session once sent */
        std::unique_ptr<HTTPMessage> resp = co_await
            sess->sendWebTransportRequest(req, std::move(handler));
        if (!resp) { set_fatal(0x1); signal_init(MOQ_ERR_INTERNAL); co_return; }
        /* The WT session arrived via fwd_handler::onWebTransportSession during
         * the request (single EventBase thread); read it now. */
        if (!fwd_raw->wt_captured) {
            set_fatal(0x1); signal_init(MOQ_ERR_INTERNAL); co_return;
        }
        wt = fwd_raw->wt_captured;
        fwd = fwd_raw;

        /* Interop: proxygen's WebTransport-over-H3 gates OUTGOING WT streams on
         * the peer's WT_INITIAL_MAX_STREAMS_{BIDI,UNI} SETTINGS (a newer WT
         * flow-control draft). moqx (like the quiche/picoquic WT stacks it
         * already interops with) maps WT streams onto QUIC stream limits and
         * never advertises those settings, so proxygen computes an outgoing
         * limit of 0 and refuses every stream. Seed the credit locally -- the
         * exact effect of a peer WT_MAX_STREAMS -- so streams can open; the
         * underlying QUIC stream credit (which moqx DOES grant) still bounds us. */
        auto *h3 = dynamic_cast<H3WtSession *>(wt.get());
        if (!h3) {   /* media depends on the shim -- fail closed, never half-up */
            set_fatal(0x1); signal_init(MOQ_ERR_INTERNAL); co_return;
        }
        detail::WtStreamManager::MaxStreamsBidi mb; mb.maxStreams = kWtStreamCredit;
        detail::WtStreamManager::MaxStreamsUni  mu; mu.maxStreams = kWtStreamCredit;
        if (h3->onMaxStreams(mb) != detail::WtStreamManager::Ok ||
            h3->onMaxStreams(mu) != detail::WtStreamManager::Ok) {
            set_fatal(0x1); signal_init(MOQ_ERR_INTERNAL); co_return;
        }

        /* Negotiated version from the selected WT-Protocol. The selection must
         * be present and must be one we offered; a missing/unknown/un-offered
         * selection is fatal -- never a silent downgrade. */
        auto sel = HTTPWebTransport::getWTProtocol(*resp);
        if (sel.hasError()) { set_fatal(0x1); signal_init(MOQ_ERR_UNSUPPORTED); co_return; }
        bool offered = false;
        for (const auto &t : alpn) { if (t == *sel) { offered = true; break; } }
        if (!offered) { set_fatal(0x1); signal_init(MOQ_ERR_UNSUPPORTED); co_return; }
        moq_version_t version;
        if (!moq_alpn_to_version(sel->c_str(), sel->size(), &version)) {
            set_fatal(0x1); signal_init(MOQ_ERR_UNSUPPORTED); co_return;
        }

        moq_session_cfg_t scfg;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), &alloc, MOQ_PERSPECTIVE_CLIENT);
        scfg.version = version;
        if (send_request_capacity) {
            scfg.send_request_capacity = true;
            scfg.initial_request_capacity =
                initial_request_capacity ? initial_request_capacity : 64;
        }
        if (goaway_timeout_us) scfg.goaway_timeout_us = goaway_timeout_us;
        moq_session_t *s = nullptr;
        if (moq_session_create(&scfg, now_us_steady(), &s) < 0) {
            set_fatal(0x1); signal_init(MOQ_ERR_INTERNAL); co_return;
        }

        moq::wt::Adapter::Config acfg{};
        acfg.session = s;
        acfg.alloc = &alloc;
        acfg.executor = &evb;
        acfg.now_us = now_us_steady;
        auto a = moq::wt::Adapter::create(acfg, wt.get());
        if (!a) {
            moq_session_destroy(s);
            set_fatal(0x1); signal_init(MOQ_ERR_INTERNAL); co_return;
        }
        fwd->target = a.get();      /* route inbound callbacks now */
        adapter = std::move(a);
        session.store(s);
        negotiated.store((int)version);

        if (moq_session_start(s, now_us_steady()) < 0) {
            set_fatal(0x1); signal_init(MOQ_ERR_INTERNAL); co_return;
        }

        signal_init(MOQ_OK);
        schedule_pump_soon();
    } catch (...) {
        set_fatal(0x1);
        signal_init(MOQ_ERR_INTERNAL);
    }
    co_return;
}

/* -- C API ----------------------------------------------------------- */

extern "C" {

void moq_proxygen_wt_managed_cfg_init(moq_proxygen_wt_managed_cfg_t *cfg)
{
    if (!cfg) return;
    std::memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
    cfg->perspective = MOQ_PERSPECTIVE_CLIENT;
}

#define CFG_HAS(cfg, field) \
    ((cfg)->struct_size >= offsetof(moq_proxygen_wt_managed_cfg_t, field) + \
        sizeof((cfg)->field))

moq_result_t moq_proxygen_wt_managed_create(
    const moq_proxygen_wt_managed_cfg_t *cfg,
    moq_proxygen_wt_managed_t **out)
{
    if (!cfg || !out) return MOQ_ERR_INVAL;
    *out = nullptr;
    if (cfg->struct_size < offsetof(moq_proxygen_wt_managed_cfg_t, on_pump) +
        sizeof(cfg->on_pump))
        return MOQ_ERR_INVAL;

    moq_perspective_t persp = CFG_HAS(cfg, perspective)
        ? cfg->perspective : MOQ_PERSPECTIVE_CLIENT;
    if (persp != MOQ_PERSPECTIVE_CLIENT) return MOQ_ERR_INVAL;  /* client-only */
    if (!CFG_HAS(cfg, on_pump) || !cfg->on_pump) return MOQ_ERR_INVAL;

    const char *host = CFG_HAS(cfg, host) ? cfg->host : nullptr;
    int port = CFG_HAS(cfg, port) ? cfg->port : 0;
    if (!host || !host[0] || port < 1 || port > 65535) return MOQ_ERR_INVAL;
    bool insecure = CFG_HAS(cfg, insecure_skip_verify)
        ? cfg->insecure_skip_verify : false;
    const char *ca = CFG_HAS(cfg, ca_file) ? cfg->ca_file : nullptr;
    if (insecure && ca && ca[0]) return MOQ_ERR_INVAL;

    /* WT-Available-Protocols offer → version vector (AUTO/multi supported).
     * An empty offer normalizes to the legacy single "moqt-16"; the offer is
     * always sent and the selected WT-Protocol must be one we offered (the
     * facade never silently downgrades -- see connect_coro). */
    std::vector<std::string> offer;
    if (CFG_HAS(cfg, alpn_count) && cfg->alpn_count > 0) {
        if (!CFG_HAS(cfg, alpn_list) || !cfg->alpn_list) return MOQ_ERR_INVAL;
        for (size_t i = 0; i < cfg->alpn_count; i++) {
            const char *t = cfg->alpn_list[i];
            if (!t || !t[0]) return MOQ_ERR_INVAL;
            moq_version_t v;
            if (!moq_alpn_to_version(t, std::strlen(t), &v))
                return MOQ_ERR_UNSUPPORTED;   /* unknown / non-MoQ ALPN */
            offer.emplace_back(t);
        }
    } else {
        offer.emplace_back("moqt-16");
    }

    const moq_alloc_t *alloc_ptr = CFG_HAS(cfg, alloc) ? cfg->alloc : nullptr;
    if (alloc_ptr && (!alloc_ptr->alloc || !alloc_ptr->free || !alloc_ptr->realloc))
        return MOQ_ERR_INVAL;
    const moq_alloc_t *alloc = alloc_ptr ? alloc_ptr : moq_alloc_default();

    auto *m = static_cast<moq_proxygen_wt_managed_t *>(
        alloc->alloc(sizeof(moq_proxygen_wt_managed_t), alloc->ctx));
    if (!m) return MOQ_ERR_NOMEM;
    bool constructed = false;
    try {
        new (m) moq_proxygen_wt_managed_t();
        constructed = true;
        m->alloc = *alloc;
        m->host = host;
        m->port = port;
        if (CFG_HAS(cfg, sni) && cfg->sni && cfg->sni[0]) m->sni = cfg->sni;
        if (CFG_HAS(cfg, path) && cfg->path && cfg->path[0]) m->path = cfg->path;
        else m->path = "/moq";
        if (ca && ca[0]) m->ca_file = ca;
        m->insecure = insecure;
        if (CFG_HAS(cfg, send_request_capacity))
            m->send_request_capacity = cfg->send_request_capacity;
        if (CFG_HAS(cfg, initial_request_capacity))
            m->initial_request_capacity = cfg->initial_request_capacity;
        if (CFG_HAS(cfg, goaway_timeout_us))
            m->goaway_timeout_us = cfg->goaway_timeout_us;
        m->alpn = std::move(offer);
        m->on_pump = cfg->on_pump;
        m->on_activity = CFG_HAS(cfg, on_activity) ? cfg->on_activity : nullptr;
        m->user_ctx = CFG_HAS(cfg, user_ctx) ? cfg->user_ctx : nullptr;
    } catch (...) {
        if (constructed) m->~moq_proxygen_wt_managed_t();
        alloc->free(m, sizeof(*m), alloc->ctx);
        return MOQ_ERR_INTERNAL;
    }

    m->running.store(true);
    try {
        m->thread = std::make_unique<std::thread>([m]() {
            m->net_tid = std::this_thread::get_id();
            /* Launch the CONNECT coroutine on the EventBase, then run it. */
            m->connect_coro().scheduleOn(&m->evb).start();
            m->evb.loopForever();
            /* Drain on this thread. Stop forwarding inbound callbacks BEFORE
             * destroying the adapter: proxygen shutdown (wt.reset / session
             * teardown) can still emit onSessionEnd/drain/stream callbacks to
             * the forwarding handler, which must not touch a freed adapter. */
            if (m->fwd) m->fwd->target = nullptr;
            m->adapter.reset();
            if (auto *s = m->session.exchange(nullptr)) moq_session_destroy(s);
            m->wt.reset();
            /* Close the QUIC transport on its own EventBase thread; the
             * HQUpstreamSession self-destructs on connection end. Drain any
             * deferred teardown callbacks (the EventBase can loop again after
             * terminateLoopSoon) before the thread exits. */
            m->http_session = nullptr;
            if (m->transport) {
                m->transport->close(quic::QuicError(
                    quic::ApplicationErrorCode(0)));
                m->transport.reset();
            }
            m->evb.loop();
            m->thread_exited.store(true);
            m->mark_activity();
        });
    } catch (...) {
        m->running.store(false);
        m->~moq_proxygen_wt_managed_t();
        alloc->free(m, sizeof(*m), alloc->ctx);
        return MOQ_ERR_INTERNAL;
    }

    { std::unique_lock<std::mutex> lk(m->init_mu);
      m->init_cv.wait(lk, [m]() { return m->init_done.load(); }); }
    moq_result_t rc = m->init_result.load();
    if (rc < 0) {
        m->running.store(false);
        m->evb.terminateLoopSoon();
        if (m->thread && m->thread->joinable()) m->thread->join();
        m->~moq_proxygen_wt_managed_t();
        alloc->free(m, sizeof(*m), alloc->ctx);
        return rc;
    }
    *out = m;
    return MOQ_OK;
}

moq_result_t moq_proxygen_wt_managed_stop(moq_proxygen_wt_managed_t *m)
{
    if (!m) return MOQ_ERR_INVAL;
    if (m->joined.load()) return MOQ_OK;
    if (m->on_net_thread()) return MOQ_ERR_INVAL;
    m->running.store(false);
    m->evb.terminateLoopSoon();
    m->wait_cv.notify_all();
    if (m->thread && m->thread->joinable()) m->thread->join();
    m->joined.store(true);
    return MOQ_OK;
}

void moq_proxygen_wt_managed_destroy(moq_proxygen_wt_managed_t *m)
{
    if (!m) return;
    moq_alloc_t alloc = m->alloc;
    moq_proxygen_wt_managed_stop(m);
    m->~moq_proxygen_wt_managed_t();
    alloc.free(m, sizeof(*m), alloc.ctx);
}

moq_session_t *moq_proxygen_wt_managed_session(moq_proxygen_wt_managed_t *m)
{
    if (!m || !m->on_net_thread()) return nullptr;
    return m->session.load();
}

moq_result_t moq_proxygen_wt_managed_wake(moq_proxygen_wt_managed_t *m)
{
    if (!m) return MOQ_ERR_INVAL;
    if (!m->running.load() || m->thread_exited.load()) return MOQ_ERR_CLOSED;
    m->evb.runInEventBaseThread([m]() { m->pump_once(); });
    return MOQ_OK;
}

moq_result_t moq_proxygen_wt_managed_wait(moq_proxygen_wt_managed_t *m,
                                          uint64_t timeout_us)
{
    if (!m) return MOQ_ERR_INVAL;
    if (m->thread_exited.load()) return MOQ_ERR_CLOSED;
    std::unique_lock<std::mutex> lk(m->wait_mu);
    if (m->activity.exchange(false)) return MOQ_OK;
    if (timeout_us == 0) return MOQ_DONE;
    auto pred = [m]() { return m->activity.load() || m->thread_exited.load(); };
    bool got = (timeout_us == UINT64_MAX)
        ? (m->wait_cv.wait(lk, pred), true)
        : m->wait_cv.wait_for(lk, std::chrono::microseconds(timeout_us), pred);
    if (m->thread_exited.load()) return MOQ_ERR_CLOSED;
    if (got && m->activity.exchange(false)) return MOQ_OK;
    return MOQ_DONE;
}

bool moq_proxygen_wt_managed_is_fatal(const moq_proxygen_wt_managed_t *m)
{
    return m ? m->fatal.load() : false;
}
uint64_t moq_proxygen_wt_managed_fatal_code(const moq_proxygen_wt_managed_t *m)
{
    return m ? m->fatal_code_val.load() : 0;
}
bool moq_proxygen_wt_managed_is_closed(const moq_proxygen_wt_managed_t *m)
{
    return m ? m->closed.load() : false;
}
moq_version_t moq_proxygen_wt_managed_negotiated_version(
    const moq_proxygen_wt_managed_t *m)
{
    return m ? (moq_version_t)m->negotiated.load() : (moq_version_t)0;
}

} /* extern "C" */
