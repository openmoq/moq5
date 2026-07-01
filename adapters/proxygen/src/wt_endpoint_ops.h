#ifndef MOQ_WT_ENDPOINT_OPS_H
#define MOQ_WT_ENDPOINT_OPS_H

/*
 * Private header for the proxygen WebTransport endpoint ops.
 * Implements moq_transport_endpoint_ops_t for proxygen::WebTransport.
 * Not installed.
 */

#include <moq/transport_bridge.h>

#include <proxygen/lib/http/webtransport/WebTransport.h>
#include <memory>

namespace moq::wt {

struct wt_endpoint_ctx_t {
    proxygen::WebTransport *wt;
    void (*on_bidi_opened)(void *ctx, uint64_t stream_id);
    void *cb_ctx;
};

void wt_endpoint_ops_init(moq_transport_endpoint_ops_t *ops,
                           wt_endpoint_ctx_t *ctx,
                           proxygen::WebTransport *wt);

} // namespace moq::wt

#endif // MOQ_WT_ENDPOINT_OPS_H
