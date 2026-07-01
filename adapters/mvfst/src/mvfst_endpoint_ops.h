#ifndef MOQ_MVFST_ENDPOINT_OPS_H
#define MOQ_MVFST_ENDPOINT_OPS_H

/*
 * Private header for the mvfst endpoint ops binding.
 * Implements moq_transport_endpoint_ops_t for QuicSocket.
 * Not installed.
 */

#include <moq/transport_bridge.h>

#include <quic/api/QuicSocket.h>
#include <atomic>
#include <cstdint>
#include <memory>

namespace moq::mvfst {

struct mvfst_endpoint_ctx_t {
    std::shared_ptr<quic::QuicSocket> socket;
    /* Called after open_bidi succeeds so the adapter can register
     * a read callback on the newly opened stream. */
    void (*on_bidi_opened)(void *cb_ctx, uint64_t stream_id);
    void *cb_ctx;

    /* Outbound stream-credit backpressure. The gating/blocked flags are
     * network-thread confined (bridge service + mvfst callbacks), so they
     * stay plain; the telemetry counters are atomic because tests read them
     * from the application thread (see below).
     *
     * credit_gating is OFF by default -- attach-mode behavior is unchanged.
     * The managed C facade turns it on. When gating is on and a directional
     * blocked flag is set, ep_open_uni/ep_open_bidi short-circuit to
     * WOULD_BLOCK instead of re-calling createStream every pump tick (which
     * logs a warning each time it fails with STREAM_LIMIT_EXCEEDED). The
     * peer's MAX_STREAMS grant clears the flag via the impl's
     * onUni/BidirectionalStreamsAvailable callback, which also wakes the pump.
     *
     * credit_gating and the blocked flags are network-thread confined (only
     * touched by the bridge service / mvfst callbacks), so they stay plain.
     *
     * The counters below are read by tests from the application thread while
     * the network thread is running, so they are atomic (relaxed): block
     * counters record create attempts that hit the stream limit (only while
     * gating is enabled); grant counters record peer credit grants seen via
     * onUni/BidirectionalStreamsAvailable -- evidence that retry is driven by
     * the credit callback, not the poll. */
    bool     credit_gating    = false;
    bool     uni_blocked      = false;
    bool     bidi_blocked     = false;
    std::atomic<uint64_t> uni_block_count{0};
    std::atomic<uint64_t> bidi_block_count{0};
    std::atomic<uint64_t> uni_credit_grants{0};
    std::atomic<uint64_t> bidi_credit_grants{0};
};

void mvfst_endpoint_ops_init(moq_transport_endpoint_ops_t *ops,
                              mvfst_endpoint_ctx_t *ctx,
                              std::shared_ptr<quic::QuicSocket> socket,
                              void (*on_bidi_opened)(void *, uint64_t) = nullptr,
                              void *cb_ctx = nullptr);

} // namespace moq::mvfst

#endif // MOQ_MVFST_ENDPOINT_OPS_H
