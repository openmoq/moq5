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

/*
 * The earliest deadline the pump would hand its one-shot AsyncTimeout — the
 * exact fold (min of session deadlines and the live application deadline) that
 * drives both the client and server timer reschedules. Lets a test assert the
 * app-deadline fold deterministically, without waiting on timer delivery. Use on
 * an idle facade with no sessions so the read is off-thread safe. UINT64_MAX for
 * a NULL handle or when nothing is pending.
 */
uint64_t moq_mvfst_managed_test_earliest_deadline(moq_mvfst_managed_t *m);

/*
 * Set the live/stopping latch (running) the fold guards on, so a test can prove
 * a stopping pump does not consult the application callback. NULL is a no-op.
 */
void moq_mvfst_managed_test_set_running(moq_mvfst_managed_t *m, bool running);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_MVFST_MANAGED_TESTING_H */
