/*
 * wt_endpoint_ops.cpp — Implements moq_transport_endpoint_ops_t
 * by forwarding to proxygen::WebTransport stream-ID API.
 *
 * All operations use the stream-ID-based WebTransport methods
 * (writeStreamData(id, ...), resetStream(id, ...), etc.) rather
 * than handle-based methods. No handle map needed.
 *
 * Exception guard on every op: C++ exceptions must not cross
 * the C transport bridge boundary.
 */

#include "wt_endpoint_ops.h"

#include <moq/rcbuf.h>
#include <folly/ScopeGuard.h>
#include <folly/io/IOBuf.h>

namespace moq::wt {

template <class Fn>
static moq_transport_result_t guard(Fn &&fn) noexcept {
    try {
        return fn();
    } catch (...) {
        return MOQ_TRANSPORT_ERROR;
    }
}

static moq_transport_result_t map_error(proxygen::WebTransport::ErrorCode ec)
{
    using EC = proxygen::WebTransport::ErrorCode;
    switch (ec) {
    case EC::STREAM_CREATION_ERROR:
    case EC::BLOCKED:
        return MOQ_TRANSPORT_WOULD_BLOCK;
    default:
        return MOQ_TRANSPORT_ERROR;
    }
}

static moq_transport_result_t map_fc_state(proxygen::WebTransport::FCState fc)
{
    using FC = proxygen::WebTransport::FCState;
    switch (fc) {
    case FC::UNBLOCKED:
    case FC::BLOCKED:
        // FCState is the *success* return of writeStreamData. Both
        // UNBLOCKED and BLOCKED mean the data was accepted. BLOCKED
        // only signals that future writes may hit flow control.
        return MOQ_TRANSPORT_OK;
    case FC::SESSION_CLOSED:
        return MOQ_TRANSPORT_ERROR;
    }
    return MOQ_TRANSPORT_ERROR;
}

/* -- Endpoint ops --------------------------------------------------- */

static moq_transport_result_t ep_open_uni(void *ctx, uint64_t *out_id)
{
    return guard([&]() {
        auto *ep = static_cast<wt_endpoint_ctx_t *>(ctx);
        auto result = ep->wt->createUniStream();
        if (result.hasError())
            return map_error(result.error());
        *out_id = result.value()->getID();
        return MOQ_TRANSPORT_OK;
    });
}

static moq_transport_result_t ep_open_bidi(void *ctx, uint64_t *out_id)
{
    return guard([&]() {
        auto *ep = static_cast<wt_endpoint_ctx_t *>(ctx);
        auto result = ep->wt->createBidiStream();
        if (result.hasError())
            return map_error(result.error());
        auto bh = result.value();
        if (!bh.writeHandle || !bh.readHandle)
            return MOQ_TRANSPORT_ERROR;
        *out_id = bh.writeHandle->getID();
        if (ep->on_bidi_opened)
            ep->on_bidi_opened(ep->cb_ctx, *out_id);
        return MOQ_TRANSPORT_OK;
    });
}

static moq_transport_result_t ep_write(void *ctx, uint64_t stream_id,
                                        const uint8_t *data, size_t len,
                                        bool fin)
{
    return guard([&]() {
        auto *ep = static_cast<wt_endpoint_ctx_t *>(ctx);
        std::unique_ptr<folly::IOBuf> buf;
        if (data && len > 0)
            buf = folly::IOBuf::copyBuffer(data, len);
        auto result = ep->wt->writeStreamData(
            stream_id, std::move(buf), fin, nullptr);
        if (result.hasError())
            return map_error(result.error());
        return map_fc_state(result.value());
    });
}

static void rcbuf_iobuf_free(void * /*buf*/, void *ud)
{
    moq_rcbuf_decref(static_cast<moq_rcbuf_t *>(ud));
}

static moq_transport_result_t ep_write_payload(void *ctx,
                                                uint64_t stream_id,
                                                moq_rcbuf_t *buf, bool fin)
{
    return guard([&]() {
        auto *ep = static_cast<wt_endpoint_ctx_t *>(ctx);
        const auto *data = moq_rcbuf_data(buf);
        auto len = moq_rcbuf_len(buf);

        moq_rcbuf_incref(buf);
        bool ref_owned_by_iobuf = false;
        auto ref_guard = folly::makeGuard([&]() {
            if (!ref_owned_by_iobuf) moq_rcbuf_decref(buf);
        });

        auto iobuf = folly::IOBuf::takeOwnership(
            const_cast<void *>(static_cast<const void *>(data)),
            len, len,
            rcbuf_iobuf_free,
            buf,
            false);
        ref_owned_by_iobuf = true;
        ref_guard.dismiss();

        auto result = ep->wt->writeStreamData(
            stream_id, std::move(iobuf), fin, nullptr);
        if (result.hasError())
            return map_error(result.error());
        return map_fc_state(result.value());
    });
}

static moq_transport_result_t ep_reset(void *ctx, uint64_t stream_id,
                                        uint64_t error_code)
{
    return guard([&]() {
        auto *ep = static_cast<wt_endpoint_ctx_t *>(ctx);
        auto result = ep->wt->resetStream(stream_id,
                                           static_cast<uint32_t>(error_code));
        if (result.hasError())
            return map_error(result.error());
        return MOQ_TRANSPORT_OK;
    });
}

static moq_transport_result_t ep_stop_sending(void *ctx, uint64_t stream_id,
                                               uint64_t error_code)
{
    return guard([&]() {
        auto *ep = static_cast<wt_endpoint_ctx_t *>(ctx);
        auto result = ep->wt->stopSending(stream_id,
                                           static_cast<uint32_t>(error_code));
        if (result.hasError())
            return map_error(result.error());
        return MOQ_TRANSPORT_OK;
    });
}

static moq_transport_result_t ep_send_datagram(void *ctx,
                                                const uint8_t *data,
                                                size_t len)
{
    return guard([&]() {
        auto *ep = static_cast<wt_endpoint_ctx_t *>(ctx);
        std::unique_ptr<folly::IOBuf> buf;
        if (data && len > 0)
            buf = folly::IOBuf::copyBuffer(data, len);
        auto result = ep->wt->sendDatagram(std::move(buf));
        if (result.hasError()) {
            auto ec = result.error();
            if (ec == proxygen::WebTransport::ErrorCode::BLOCKED)
                return MOQ_TRANSPORT_DROPPED;
            return MOQ_TRANSPORT_ERROR;
        }
        return MOQ_TRANSPORT_OK;
    });
}

static moq_transport_result_t ep_close(void *ctx, uint64_t code,
                                        const uint8_t * /*reason*/,
                                        size_t /*reason_len*/)
{
    return guard([&]() {
        auto *ep = static_cast<wt_endpoint_ctx_t *>(ctx);
        folly::Optional<uint32_t> error;
        if (code != 0)
            error = static_cast<uint32_t>(code);
        auto result = ep->wt->closeSession(error);
        if (result.hasError())
            return map_error(result.error());
        return MOQ_TRANSPORT_OK;
    });
}

/* -- Init ----------------------------------------------------------- */

void wt_endpoint_ops_init(moq_transport_endpoint_ops_t *ops,
                           wt_endpoint_ctx_t *ctx,
                           proxygen::WebTransport *wt)
{
    *ops = MOQ_TRANSPORT_ENDPOINT_OPS_INIT;
    ops->capabilities = MOQ_TRANSPORT_CAP_DATAGRAM |
                         MOQ_TRANSPORT_CAP_WRITE_PAYLOAD;
    ops->open_uni        = ep_open_uni;
    ops->open_bidi       = ep_open_bidi;
    ops->write           = ep_write;
    ops->write_payload   = ep_write_payload;
    ops->reset_stream    = ep_reset;
    ops->stop_sending    = ep_stop_sending;
    ops->send_datagram   = ep_send_datagram;
    ops->close_transport = ep_close;

    ctx->wt = wt;
    ctx->on_bidi_opened = nullptr;
    ctx->cb_ctx = nullptr;
}

} // namespace moq::wt
