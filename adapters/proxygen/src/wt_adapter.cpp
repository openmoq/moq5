/*
 * wt_adapter.cpp — Proxygen WebTransport adapter for libmoq.
 *
 * Implements proxygen::WebTransportHandler and bridges incoming events
 * to moq_transport_bridge_t inbound handlers. All implementation state
 * lives in Adapter::Impl (PIMPL): the public header exposes no bridge,
 * endpoint-ops, or stream-tracking types.
 *
 * Read loops use the stream-ID API: wt_->readStreamData(id).
 * Callbacks are scheduled via the executor passed in Config.
 *
 * Threading: adapter, callbacks, and destruction must all run on the
 * same executor. The atomic alive_ flag is a safety net for executor
 * shutdown races, not a substitute for single-thread confinement.
 */

#include <moq/proxygen_wt.hpp>

#include "wt_endpoint_ops.h"
#include <moq/transport_bridge.h>

#include <folly/futures/Future.h>

#include <atomic>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace moq::wt {

static uint64_t default_now_us()
{
    auto now = std::chrono::steady_clock::now();
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()).count());
}

/* -- Private implementation ------------------------------------------ */

struct Adapter::Impl {
    enum class StreamKind { CONTROL, UNI, BIDI };

    Impl(moq_session_t *session,
         proxygen::WebTransport *wt,
         folly::Executor *executor,
         uint64_t (*now_us)())
        : session_(session)
        , wt_(wt)
        , executor_(executor)
        , now_us_(now_us ? now_us : default_now_us)
        , alive_(std::make_shared<std::atomic<bool>>(true))
        , perspective_(moq_session_perspective(session))
    {
    }

    ~Impl()
    {
        // Stop in-flight read callbacks from touching us, then tear down
        // the bridge. Order matters: alive_ first.
        alive_->store(false, std::memory_order_release);
        moq_transport_bridge_destroy(bridge_);
    }

    uint64_t now() const { return now_us_(); }
    uint64_t safe_now() const noexcept
    {
        try { return now_us_(); } catch (...) { return 0; }
    }

    void markFatal() noexcept
    {
        if (!moq_transport_bridge_is_terminal(bridge_))
            moq_transport_bridge_on_transport_error(bridge_, 0x1, safe_now());
    }

    moq_result_t terminalResult() const
    {
        if (moq_transport_bridge_is_fatal(bridge_))
            return MOQ_ERR_INTERNAL;
        return MOQ_OK;
    }

    static void onLocalBidiOpened(void *ctx, uint64_t stream_id)
    {
        auto *self = static_cast<Impl *>(ctx);
        self->local_bidi_pending_.push_back(stream_id);
    }

    bool is_terminal() const
    {
        return moq_transport_bridge_is_terminal(bridge_);
    }

    moq_result_t service()
    {
        if (moq_transport_bridge_is_terminal(bridge_))
            return terminalResult();

        try {
            moq_transport_bridge_service(bridge_, now());

            if (moq_transport_bridge_is_terminal(bridge_))
                return terminalResult();

            // Process locally-opened bidi streams queued during service().
            if (!local_bidi_pending_.empty()) {
                auto pending = std::move(local_bidi_pending_);
                local_bidi_pending_.clear();
                for (auto id : pending) {
                    if (moq_transport_bridge_is_terminal(bridge_))
                        break;
                    if (!local_control_opened_) {
                        local_control_opened_ = true;
                        local_control_stream_id_ = id;
                        stream_kinds_[id] = StreamKind::CONTROL;
                    } else {
                        stream_kinds_[id] = StreamKind::BIDI;
                    }
                    startRead(id);
                }
            }

            // Resume waiting reads (streams that returned empty data).
            if (!waiting_reads_.empty() &&
                !moq_transport_bridge_is_terminal(bridge_)) {
                auto waiting = std::move(waiting_reads_);
                waiting_reads_.clear();
                for (auto id : waiting) {
                    if (moq_transport_bridge_is_terminal(bridge_))
                        break;
                    if (stream_kinds_.count(id) && !active_reads_.count(id))
                        startRead(id);
                }
            }

            // Resume paused reads whose bridge pending cleared.
            // Collect into a vector first — startRead may synchronously
            // re-enter onReadData which could modify paused_reads_.
            if (!paused_reads_.empty() &&
                !moq_transport_bridge_is_terminal(bridge_)) {
                std::vector<uint64_t> resumable;
                for (auto id : paused_reads_) {
                    if (!moq_transport_bridge_stream_has_pending(bridge_, id))
                        resumable.push_back(id);
                }
                for (auto id : resumable)
                    paused_reads_.erase(id);
                for (auto id : resumable) {
                    if (moq_transport_bridge_is_terminal(bridge_))
                        break;
                    startRead(id);
                }
            }
        } catch (...) {
            markFatal();
        }

        return moq_transport_bridge_is_terminal(bridge_)
            ? terminalResult() : MOQ_OK;
    }

    void startRead(uint64_t id)
    {
        if (moq_transport_bridge_is_terminal(bridge_))
            return;
        if (active_reads_.count(id))
            return;
        if (!stream_kinds_.count(id))
            return;

        folly::Expected<folly::SemiFuture<proxygen::WebTransport::StreamData>,
                        proxygen::WebTransport::ErrorCode> result;
        try {
            result = wt_->readStreamData(id);
        } catch (...) {
            markFatal();
            return;
        }

        if (result.hasError()) {
            markFatal();
            return;
        }

        active_reads_.insert(id);
        auto alive = alive_;
        // alive_ == true is the sole guarantor that `this`/`bridge_` are
        // still valid inside these continuations: ~Impl stores alive_ =
        // false BEFORE moq_transport_bridge_destroy, so a true read here
        // (on the confining executor) means the bridge has not been torn
        // down. Always check alive_ before touching this/bridge_.
        std::move(result.value())
            .via(executor_)
            .thenValue([this, alive, id](
                    proxygen::WebTransport::StreamData sd) {
                if (!alive->load(std::memory_order_acquire) ||
                    moq_transport_bridge_is_terminal(bridge_))
                    return;
                active_reads_.erase(id);
                onReadData(id, std::move(sd));
            })
            .thenError([this, alive, id](folly::exception_wrapper ew) {
                if (!alive->load(std::memory_order_acquire)) return;
                active_reads_.erase(id);
                if (moq_transport_bridge_is_terminal(bridge_))
                    return;
                if (ew.is_compatible_with<folly::OperationCancelled>())
                    return;
                // Any read failure is a stream-level error. Extract the
                // error code if available; fall back to 0x1. Avoids RTTI
                // match failures across shared libraries.
                uint32_t code = 0x1;
                auto *wt_ex =
                    ew.get_exception<proxygen::WebTransport::Exception>();
                if (wt_ex)
                    code = wt_ex->error;
                onReadError(id, code);
            });
    }

    void onReadData(uint64_t id, proxygen::WebTransport::StreamData sd)
    {
        try {
            const uint8_t *data = nullptr;
            size_t len = 0;
            if (sd.data) {
                if (sd.data->isChained())
                    sd.data->coalesce();
                data = sd.data->data();
                len = sd.data->length();
            }

            if (len == 0 && !sd.fin) {
                waiting_reads_.insert(id);
                return;
            }

            auto kit = stream_kinds_.find(id);
            if (kit == stream_kinds_.end())
                return;

            moq_result_t rc;
            switch (kit->second) {
            case StreamKind::CONTROL:
                rc = moq_transport_bridge_on_peer_control_bytes(
                    bridge_, id, data, len, sd.fin, now());
                break;
            case StreamKind::UNI:
                rc = moq_transport_bridge_on_peer_uni_bytes(
                    bridge_, id, data, len, sd.fin, now());
                break;
            case StreamKind::BIDI:
                rc = moq_transport_bridge_on_peer_bidi_bytes(
                    bridge_, id, data, len, sd.fin, now());
                break;
            }

            if (sd.fin) {
                stream_kinds_.erase(id);
                paused_reads_.erase(id);
                waiting_reads_.erase(id);
                if (!moq_transport_bridge_is_terminal(bridge_))
                    moq_transport_bridge_service(bridge_, now());
                return;
            }

            if (!moq_transport_bridge_is_terminal(bridge_))
                moq_transport_bridge_service(bridge_, now());

            if (moq_transport_bridge_is_terminal(bridge_))
                return;

            // Pause only if the bridge STILL has pending inbound work for
            // this stream after service(). The synchronous service() above
            // may have already drained the WOULD_BLOCK that `rc` reported,
            // in which case pausing would stall the stream until an
            // external service() call. Re-check pending, not the stale rc.
            if (rc == MOQ_ERR_WOULD_BLOCK &&
                moq_transport_bridge_stream_has_pending(bridge_, id)) {
                paused_reads_.insert(id);
                return;
            }

            startRead(id);
        } catch (...) {
            markFatal();
        }
    }

    void onReadError(uint64_t id, uint32_t error_code)
    {
        try {
            bool is_control = (id == peer_control_stream_id_ ||
                               id == local_control_stream_id_);

            stream_kinds_.erase(id);
            paused_reads_.erase(id);
            waiting_reads_.erase(id);

            if (is_control) {
                moq_transport_bridge_on_transport_error(
                    bridge_, error_code, now());
            } else {
                moq_transport_bridge_on_peer_stream_reset(
                    bridge_, id, error_code, now());
                if (!moq_transport_bridge_is_terminal(bridge_))
                    moq_transport_bridge_service(bridge_, now());
            }
        } catch (...) {
            markFatal();
        }
    }

    void onNewUniStream(proxygen::WebTransport::StreamReadHandle *rh) noexcept
    {
        try {
            if (moq_transport_bridge_is_terminal(bridge_))
                return;
            if (!rh) {
                markFatal();
                return;
            }
            auto id = rh->getID();
            stream_kinds_[id] = StreamKind::UNI;
            startRead(id);
        } catch (...) {
            markFatal();
        }
    }

    void onNewBidiStream(proxygen::WebTransport::BidiStreamHandle bh) noexcept
    {
        try {
            if (moq_transport_bridge_is_terminal(bridge_))
                return;
            if (!bh.readHandle) {
                markFatal();
                return;
            }
            auto id = bh.readHandle->getID();
            if (!first_peer_bidi_seen_ &&
                perspective_ == MOQ_PERSPECTIVE_SERVER) {
                first_peer_bidi_seen_ = true;
                peer_control_stream_id_ = id;
                stream_kinds_[id] = StreamKind::CONTROL;
            } else {
                first_peer_bidi_seen_ = true;
                stream_kinds_[id] = StreamKind::BIDI;
            }
            startRead(id);
        } catch (...) {
            markFatal();
        }
    }

    void onDatagram(std::unique_ptr<folly::IOBuf> datagram) noexcept
    {
        try {
            if (moq_transport_bridge_is_terminal(bridge_))
                return;
            const uint8_t *data = nullptr;
            size_t len = 0;
            if (datagram) {
                if (datagram->isChained())
                    datagram->coalesce();
                data = datagram->data();
                len = datagram->length();
            }
            moq_transport_bridge_on_peer_datagram(bridge_, data, len, now());
            if (!moq_transport_bridge_is_terminal(bridge_))
                moq_transport_bridge_service(bridge_, now());
        } catch (...) {
            markFatal();
        }
    }

    // onSessionEnd is the "session is over" callback from proxygen. Both
    // error and no-error cases are peer-initiated closes, not transport
    // failures. Moxygen treats them identically (close with NO_ERROR).
    // on_transport_error is reserved for adapter-internal failures.
    void onSessionEnd(folly::Optional<uint32_t> error) noexcept
    {
        try {
            if (moq_transport_bridge_is_terminal(bridge_))
                return;
            uint64_t code = error ? static_cast<uint64_t>(*error) : 0;
            moq_transport_bridge_on_transport_close(bridge_, code, now());
        } catch (...) {
            markFatal();
        }
    }

    // State (moved out of the public class layout).
    moq_session_t *session_;
    moq_transport_bridge_t *bridge_ = nullptr;
    proxygen::WebTransport *wt_;
    folly::Executor *executor_;
    uint64_t (*now_us_)();

    moq_transport_endpoint_ops_t ops_{};
    std::unique_ptr<wt_endpoint_ctx_t> ep_ctx_;

    std::shared_ptr<std::atomic<bool>> alive_;
    moq_perspective_t perspective_;

    bool first_peer_bidi_seen_ = false;
    uint64_t peer_control_stream_id_ = UINT64_MAX;

    bool local_control_opened_ = false;
    uint64_t local_control_stream_id_ = UINT64_MAX;
    std::vector<uint64_t> local_bidi_pending_;

    std::unordered_map<uint64_t, StreamKind> stream_kinds_;
    std::unordered_set<uint64_t> active_reads_;
    std::unordered_set<uint64_t> paused_reads_;
    std::unordered_set<uint64_t> waiting_reads_;
};

/* -- Adapter: thin forwarding shell ---------------------------------- */

Adapter::Adapter(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl))
{
}

Adapter::~Adapter() noexcept = default;

std::unique_ptr<Adapter> Adapter::create(
    const Config &cfg, proxygen::WebTransport *wt)
{
    if (!cfg.session || !wt || !cfg.alloc || !cfg.executor)
        return nullptr;

    auto now_fn = cfg.now_us ? cfg.now_us : default_now_us;

    auto impl = std::make_unique<Impl>(cfg.session, wt, cfg.executor, now_fn);

    impl->ep_ctx_ = std::make_unique<wt_endpoint_ctx_t>();
    wt_endpoint_ops_init(&impl->ops_, impl->ep_ctx_.get(), wt);
    impl->ep_ctx_->on_bidi_opened = Impl::onLocalBidiOpened;
    impl->ep_ctx_->cb_ctx = impl.get();

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, cfg.alloc);
    if (cfg.max_streams) bcfg.max_streams = cfg.max_streams;
    if (cfg.max_pending) bcfg.max_pending = cfg.max_pending;

    moq_transport_bridge_t *bridge = nullptr;
    moq_result_t rc = moq_transport_bridge_create(
        &bcfg, cfg.session, &impl->ops_, impl->ep_ctx_.get(), &bridge);
    if (rc < 0)
        return nullptr;

    impl->bridge_ = bridge;
    return std::unique_ptr<Adapter>(new Adapter(std::move(impl)));
}

moq_result_t Adapter::service() { return impl_->service(); }
moq_session_t *Adapter::session() const noexcept { return impl_->session_; }
bool Adapter::is_terminal() const { return impl_->is_terminal(); }

bool Adapter::is_closed() const noexcept
{
    return moq_transport_bridge_is_closed(impl_->bridge_);
}
bool Adapter::is_fatal() const noexcept
{
    return moq_transport_bridge_is_fatal(impl_->bridge_);
}
uint64_t Adapter::fatal_code() const noexcept
{
    return moq_transport_bridge_fatal_code(impl_->bridge_);
}
uint64_t Adapter::close_code() const noexcept
{
    return moq_transport_bridge_close_code(impl_->bridge_);
}
size_t Adapter::stream_count() const noexcept
{
    return moq_transport_bridge_stream_count(impl_->bridge_);
}
size_t Adapter::tombstone_count() const noexcept
{
    return moq_transport_bridge_tombstone_count(impl_->bridge_);
}

void Adapter::onNewUniStream(
    proxygen::WebTransport::StreamReadHandle *rh) noexcept
{
    impl_->onNewUniStream(rh);
}

void Adapter::onNewBidiStream(
    proxygen::WebTransport::BidiStreamHandle bh) noexcept
{
    impl_->onNewBidiStream(std::move(bh));
}

void Adapter::onDatagram(std::unique_ptr<folly::IOBuf> datagram) noexcept
{
    impl_->onDatagram(std::move(datagram));
}

void Adapter::onSessionEnd(folly::Optional<uint32_t> error) noexcept
{
    impl_->onSessionEnd(error);
}

void Adapter::onSessionDrain() noexcept
{
    // Informational. No bridge action required.
}

} // namespace moq::wt
