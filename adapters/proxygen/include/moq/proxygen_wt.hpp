#ifndef MOQ_PROXYGEN_WT_HPP
#define MOQ_PROXYGEN_WT_HPP

/*
 * Proxygen WebTransport adapter for libmoq (C++ attach mode).
 *
 * Adapts an existing proxygen::WebTransport connection into libmoq's MoQ
 * session engine:
 *
 *     proxygen::WebTransport* + moq_session_t*  ->  moq::wt::Adapter
 *
 * The caller owns the WebTransport* and the moq_session_t*. Usage:
 *   1. auto a = Adapter::create(cfg, wt);
 *   2. install it as the connection's handler via the appropriate
 *      proxygen setHandler(a.get()) on the WebTransport/session — the
 *      adapter does NOT install itself;
 *   3. call a->service() to pump the bridge (and after session APIs).
 * The adapter drives the libmoq transport bridge underneath. It does NOT
 * expose raw WebTransport operations — it is a MoQ-session adapter, not a
 * transport library.
 *
 * Packaging: installed as CMake component adapter-proxygen-wt
 *   (find_package(libmoq COMPONENTS adapter-proxygen-wt); link
 *   moq::adapter-proxygen-wt). CMake-only — no pkg-config (proxygen/folly
 *   are CMake-native; same posture as the mvfst adapter). Consuming this
 *   header pulls in proxygen + folly headers, which is expected for the
 *   proxygen adapter component; those dependencies never enter libmoq core
 *   or any non-proxygen installed header.
 *
 * Threading: the adapter, its callbacks, and destruction must all run on
 * the same executor (typically the proxygen EventBase thread).
 *
 * Experimental: C++ only; no managed client/server. API may evolve.
 */

#include <moq/session.h>   /* moq_session_t, moq_alloc_t, moq_result_t, MOQ_API */

#include <proxygen/lib/http/webtransport/WebTransport.h>
#include <folly/Executor.h>

#include <cstddef>
#include <cstdint>
#include <memory>

namespace moq::wt {

/*
 * Attach adapter. Construct with create(); install as the WebTransport's
 * handler; call service() to pump the bridge. No libmoq-internal types
 * (transport bridge, endpoint ops) appear in this public surface — they
 * live behind an opaque private implementation.
 */
class MOQ_API Adapter : public proxygen::WebTransportHandler {
 public:
    struct Config {
        moq_session_t *session;      // NOT owned, must outlive adapter
        const moq_alloc_t *alloc;    // for bridge allocation
        folly::Executor *executor;   // for async read-loop callbacks
        uint64_t (*now_us)();        // time source; NULL = steady_clock. SHOULD NOT throw.
        uint32_t max_streams;        // 0 = bridge default
        uint32_t max_pending;        // 0 = bridge default
    };

    // Returns nullptr on invalid config or bridge-creation failure.
    static std::unique_ptr<Adapter> create(
        const Config &cfg, proxygen::WebTransport *wt);

    ~Adapter() noexcept override;

    Adapter(const Adapter &) = delete;
    Adapter &operator=(const Adapter &) = delete;

    // Pump the bridge: send queued actions, resume reads. Call after any
    // moq_session_t API that may queue work, and from the read loop.
    moq_result_t service();

    // The session this adapter drives (not owned).
    moq_session_t *session() const noexcept;

    // -- State observability ------------------------------------------
    // True once the adapter is closed or fatal (no further work runs).
    bool is_terminal() const;
    // True after a peer-initiated or local clean close (e.g. session
    // end / GOAWAY drain). Distinct from fatal.
    bool is_closed() const noexcept;
    // True after an unrecoverable transport/protocol error.
    bool is_fatal() const noexcept;
    // Error code latched on fatal (0 if not fatal).
    uint64_t fatal_code() const noexcept;
    // Application close code latched on clean close (0 if not closed).
    uint64_t close_code() const noexcept;
    // Experimental diagnostics (debugging/observability only - NOT a
    // stable application contract; may change or be removed). These
    // surface internal bridge counters. stream_count(): active MoQ
    // streams the adapter tracks. tombstone_count(): locally-closed
    // bidi streams awaiting the peer's FIN (half-close bookkeeping).
    size_t stream_count() const noexcept;
    size_t tombstone_count() const noexcept;

    // -- proxygen::WebTransportHandler --------------------------------
    void onNewUniStream(
        proxygen::WebTransport::StreamReadHandle *rh) noexcept override;
    void onNewBidiStream(
        proxygen::WebTransport::BidiStreamHandle bh) noexcept override;
    void onDatagram(
        std::unique_ptr<folly::IOBuf> datagram) noexcept override;
    void onSessionEnd(
        folly::Optional<uint32_t> error) noexcept override;
    void onSessionDrain() noexcept override;

 private:
    struct Impl;                       // defined in wt_adapter.cpp
    explicit Adapter(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

} // namespace moq::wt

#endif // MOQ_PROXYGEN_WT_HPP
