/*
 * mvfst_endpoint_ops.cpp — Implements moq_transport_endpoint_ops_t
 * by forwarding to quic::QuicSocket methods.
 *
 * This is the C-compatible endpoint binding for the shared transport
 * bridge. It replaces the C++ quic_endpoint abstract class.
 */

#include "mvfst_endpoint_ops.h"

#include <moq/rcbuf.h>
#include <folly/ScopeGuard.h>
#include <folly/io/IOBuf.h>
#include <quic/QuicConstants.h>

namespace moq::mvfst {

/*
 * Exception guard: every endpoint op must be noexcept from the bridge's
 * perspective. C++ exceptions crossing the C bridge are undefined behavior.
 */
template <class Fn>
static moq_transport_result_t guard(Fn &&fn) noexcept {
    try {
        return fn();
    } catch (...) {
        return MOQ_TRANSPORT_ERROR;
    }
}

static moq_transport_result_t map_local_error(quic::LocalErrorCode ec)
{
    if (ec == quic::LocalErrorCode::STREAM_LIMIT_EXCEEDED)
        return MOQ_TRANSPORT_WOULD_BLOCK;
    return MOQ_TRANSPORT_ERROR;
}

static moq_transport_result_t map_datagram_error(quic::LocalErrorCode ec)
{
    if (ec == quic::LocalErrorCode::STREAM_LIMIT_EXCEEDED)
        return MOQ_TRANSPORT_WOULD_BLOCK;
    if (ec == quic::LocalErrorCode::INVALID_WRITE_DATA)
        return MOQ_TRANSPORT_DROPPED;
    return MOQ_TRANSPORT_ERROR;
}

static moq_transport_result_t ep_open_uni(void *ctx, uint64_t *out_id)
{
    return guard([&]() {
        auto *ep = static_cast<mvfst_endpoint_ctx_t *>(ctx);
        /* Stream-credit backpressure (managed mode): once blocked, do not
         * re-call createUnidirectionalStream() until the peer grants more
         * credit -- it would only fail with STREAM_LIMIT_EXCEEDED again and
         * log a warning on every pump tick. The impl's
         * onUnidirectionalStreamsAvailable callback clears uni_blocked and
         * wakes the pump when credit opens. */
        if (ep->credit_gating && ep->uni_blocked)
            return MOQ_TRANSPORT_WOULD_BLOCK;
        /* Pre-check available credit so the FIRST open never calls
         * createUnidirectionalStream() with zero credit -- that call logs a
         * STREAM_LIMIT_EXCEEDED warning before the error path below can set
         * uni_blocked. Treat zero openable streams as the blocked state (same
         * bookkeeping as a STREAM_LIMIT_EXCEEDED result); the
         * onUnidirectionalStreamsAvailable callback clears it on a grant. */
        if (ep->credit_gating &&
            ep->socket->getNumOpenableUnidirectionalStreams() == 0) {
            ep->uni_blocked = true;
            ep->uni_block_count.fetch_add(1, std::memory_order_relaxed);
            return MOQ_TRANSPORT_WOULD_BLOCK;
        }
        auto result = ep->socket->createUnidirectionalStream();
        if (result.hasError()) {
            if (ep->credit_gating &&
                result.error() == quic::LocalErrorCode::STREAM_LIMIT_EXCEEDED) {
                ep->uni_blocked = true;
                ep->uni_block_count.fetch_add(1, std::memory_order_relaxed);
            }
            return map_local_error(result.error());
        }
        if (ep->credit_gating)
            ep->uni_blocked = false;
        *out_id = *result;
        return MOQ_TRANSPORT_OK;
    });
}

static moq_transport_result_t ep_open_bidi(void *ctx, uint64_t *out_id)
{
    return guard([&]() {
        auto *ep = static_cast<mvfst_endpoint_ctx_t *>(ctx);
        if (ep->credit_gating && ep->bidi_blocked)
            return MOQ_TRANSPORT_WOULD_BLOCK;
        /* Pre-check credit so the first open never calls createBidirectional-
         * Stream() with zero credit (avoids a STREAM_LIMIT_EXCEEDED warning);
         * onBidirectionalStreamsAvailable clears the block on a grant. */
        if (ep->credit_gating &&
            ep->socket->getNumOpenableBidirectionalStreams() == 0) {
            ep->bidi_blocked = true;
            ep->bidi_block_count.fetch_add(1, std::memory_order_relaxed);
            return MOQ_TRANSPORT_WOULD_BLOCK;
        }
        auto result = ep->socket->createBidirectionalStream();
        if (result.hasError()) {
            if (ep->credit_gating &&
                result.error() == quic::LocalErrorCode::STREAM_LIMIT_EXCEEDED) {
                ep->bidi_blocked = true;
                ep->bidi_block_count.fetch_add(1, std::memory_order_relaxed);
            }
            return map_local_error(result.error());
        }
        if (ep->credit_gating)
            ep->bidi_blocked = false;
        *out_id = *result;
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
        auto *ep = static_cast<mvfst_endpoint_ctx_t *>(ctx);
        std::unique_ptr<folly::IOBuf> buf;
        if (data && len > 0)
            buf = folly::IOBuf::copyBuffer(data, len);
        else
            buf = folly::IOBuf::create(0);
        auto result = ep->socket->writeChain(stream_id, std::move(buf), fin);
        if (result.hasError())
            return map_local_error(result.error());
        return MOQ_TRANSPORT_OK;
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
        auto *ep = static_cast<mvfst_endpoint_ctx_t *>(ctx);
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

        auto result = ep->socket->writeChain(stream_id, std::move(iobuf), fin);
        if (result.hasError())
            return map_local_error(result.error());
        return MOQ_TRANSPORT_OK;
    });
}

static moq_transport_result_t ep_reset(void *ctx, uint64_t stream_id,
                                        uint64_t error_code)
{
    return guard([&]() {
        auto *ep = static_cast<mvfst_endpoint_ctx_t *>(ctx);
        auto result = ep->socket->resetStream(stream_id, error_code);
        if (result.hasError())
            return MOQ_TRANSPORT_ERROR;
        return MOQ_TRANSPORT_OK;
    });
}

static moq_transport_result_t ep_stop_sending(void *ctx, uint64_t stream_id,
                                               uint64_t error_code)
{
    return guard([&]() {
        auto *ep = static_cast<mvfst_endpoint_ctx_t *>(ctx);
        auto result = ep->socket->stopSending(stream_id, error_code);
        if (result.hasError())
            return MOQ_TRANSPORT_ERROR;
        return MOQ_TRANSPORT_OK;
    });
}

static moq_transport_result_t ep_send_datagram(void *ctx,
                                                const uint8_t *data,
                                                size_t len)
{
    return guard([&]() {
        auto *ep = static_cast<mvfst_endpoint_ctx_t *>(ctx);
        std::unique_ptr<folly::IOBuf> buf;
        if (data && len > 0)
            buf = folly::IOBuf::copyBuffer(data, len);
        else
            buf = folly::IOBuf::create(0);
        auto result = ep->socket->writeDatagram(std::move(buf));
        if (result.hasError())
            return map_datagram_error(result.error());
        return MOQ_TRANSPORT_OK;
    });
}

static moq_transport_result_t ep_close(void *ctx, uint64_t code,
                                        const uint8_t * /*reason*/,
                                        size_t /*reason_len*/)
{
    return guard([&]() {
        auto *ep = static_cast<mvfst_endpoint_ctx_t *>(ctx);
        ep->socket->close(quic::QuicError(
            quic::ApplicationErrorCode(code)));
        return MOQ_TRANSPORT_OK;
    });
}

void mvfst_endpoint_ops_init(moq_transport_endpoint_ops_t *ops,
                              mvfst_endpoint_ctx_t *ctx,
                              std::shared_ptr<quic::QuicSocket> socket,
                              void (*on_bidi_opened)(void *, uint64_t),
                              void *cb_ctx)
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

    ctx->socket = std::move(socket);
    ctx->on_bidi_opened = on_bidi_opened;
    ctx->cb_ctx = cb_ctx;
}

} // namespace moq::mvfst
