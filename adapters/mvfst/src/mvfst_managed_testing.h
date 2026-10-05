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
 * True while the managed client facade still owns its session. Used to pin the
 * stop-vs-destroy ownership boundary: stop() joins the network thread but must
 * not free the client session a service-tier attachment may still reference.
 */
bool moq_mvfst_managed_test_has_client_session(const moq_mvfst_managed_t *m);

/*
 * Copy the mvfst client's configured QUIC version offer as raw uint32_t values.
 * Returns the total number configured; copies min(total, cap) values into out.
 * A NULL facade returns 0.
 */
size_t moq_mvfst_managed_test_client_quic_versions(
    const moq_mvfst_managed_t *m, uint32_t *out, size_t cap);

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

/*
 * Receive admission (held input) observation on the attach adapter.
 * moq_mvfst_test_set_hold_input: -1 = the adapter's own capability setting,
 * 0 = never advertise MOQ_TRANSPORT_CAP_HOLD_INPUT, 1 = always advertise it
 * (applies to adapters created afterwards). The observer is invoked on the
 * adapter's thread at each held-chunk transition with the chunk's byte
 * length and FIN flag.
 */
enum {
    MOQ_MVFST_TEST_HOLD_HELD = 1,          /* refused chunk retained */
    MOQ_MVFST_TEST_HOLD_REFUSED_AGAIN = 2, /* redelivery refused; chunk kept */
    MOQ_MVFST_TEST_HOLD_ACCEPTED = 3,      /* redelivery taken by the bridge */
    MOQ_MVFST_TEST_HOLD_DROPPED_RESET = 4, /* peer RESET while held */
    MOQ_MVFST_TEST_HOLD_DROPPED_TEARDOWN = 5, /* adapter terminal/destroyed */
    MOQ_MVFST_TEST_HOLD_REPLAY = 6,        /* actual bridge result + pending */
    MOQ_MVFST_TEST_HOLD_INPUT = 7          /* on_read entry, including injection */
};
typedef void (*moq_mvfst_test_hold_cb)(void *ctx, uint64_t stream_id,
                                        int phase, size_t len, bool fin,
                                        moq_result_t result, bool pending);
void moq_mvfst_test_set_hold_input(int mode);
void moq_mvfst_test_set_hold_observer(moq_mvfst_test_hold_cb cb, void *ctx);

/*
 * Allocation-failure injection: the next allocation at the named point fails
 * once (recording a refused chunk, re-homing it into an exact-size copy, or
 * the redelivery snapshot). Exercises the adapter's own failure paths.
 */
enum {
    MOQ_MVFST_TEST_FAIL_HOLD_RECORD = 1,
    MOQ_MVFST_TEST_FAIL_HOLD_COPY = 2,
    MOQ_MVFST_TEST_FAIL_REPLAY = 3
};
void moq_mvfst_test_fail_next(int which);

#ifdef __cplusplus
}

#include <memory>
namespace folly { class IOBuf; }
namespace moq::mvfst { class adapter; }

/*
 * Deliver one read result (what mvfst's read(id, 0) would have returned) to
 * an attach adapter for stream `stream_id`, through the same path as a real
 * readAvailable. Lets a test choose the chunk, its FIN and its buffer shape
 * (chain, oversized backing) deterministically over a live connection.
 */
void moq_mvfst_test_inject_uni_read(moq::mvfst::adapter *a, uint64_t stream_id,
                                    std::unique_ptr<folly::IOBuf> buf, bool eof);
#endif

#endif /* MOQ_MVFST_MANAGED_TESTING_H */
