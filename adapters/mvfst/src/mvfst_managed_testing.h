#ifndef MOQ_MVFST_MANAGED_TESTING_H
#define MOQ_MVFST_MANAGED_TESTING_H

/*
 * Internal, NON-PUBLIC telemetry for managed-mode regression tests.
 *
 * Not installed and not part of <moq/mvfst.h>: these symbols carry no
 * MOQ_API and are not shipped adapter API. They exist so deterministic
 * tests can observe outbound stream-credit backpressure without scraping
 * logs or relying on timing.
 */

#include <moq/mvfst.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Total outbound stream-credit block events (uni + bidi) on the managed
 * client adapter: each is one createStream() attempt that hit
 * STREAM_LIMIT_EXCEEDED. With credit gating, this is ~one per peer
 * MAX_STREAMS grant; without gating it grows once per pump tick while
 * blocked (the busy-retry spam). Returns 0 if m is NULL or has no adapter.
 */
uint64_t moq_mvfst_managed_credit_block_count(const moq_mvfst_managed_t *m);

/*
 * Total peer stream-credit grants observed via mvfst's
 * onUni/BidirectionalStreamsAvailable callbacks. > 0 proves the retry was
 * driven by the credit callback path (the fix), not only the poll loop.
 */
uint64_t moq_mvfst_managed_credit_grant_count(const moq_mvfst_managed_t *m);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_MVFST_MANAGED_TESTING_H */
