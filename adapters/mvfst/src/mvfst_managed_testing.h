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
 * driven by the credit callback path, not only the poll loop.
 */
uint64_t moq_mvfst_managed_credit_grant_count(const moq_mvfst_managed_t *m);

/*
 * True while the managed client facade still owns its session. Used to pin the
 * stop-vs-destroy ownership boundary: stop() joins the network thread but must
 * not free the client session a service-tier attachment may still reference.
 */
bool moq_mvfst_managed_test_has_client_session(const moq_mvfst_managed_t *m);

/*
 * The client session the facade retains after stop(), for reading its queued
 * events once the network thread has been joined. NULL if m is NULL or holds
 * none. Read only after stop() returns.
 */
moq_session_t *moq_mvfst_managed_test_client_session(moq_mvfst_managed_t *m);

/*
 * Copy the mvfst client's configured QUIC version offer as raw uint32_t values.
 * Returns the total number configured; copies min(total, cap) values into out.
 * A NULL facade returns 0.
 */
size_t moq_mvfst_managed_test_client_quic_versions(
    const moq_mvfst_managed_t *m, uint32_t *out, size_t cap);

/* Serial test-only probes. Set before create; read after stop joins. The
 * client probe halts at actual transport installation before start(). The
 * server probe marks the real registration block and stops startup before
 * listen/bind; check server_registered BEFORE driving the separate handoff. */
typedef struct moq_mvfst_keepalive_probe {
    bool observed;
    bool server_registered;
    bool enabled;
    uint64_t interval_ms;
    uint64_t bidi, uni;
    bool datagrams, drop_old;
    uint64_t ptos, rtt_us;
} moq_mvfst_keepalive_probe;
void moq_mvfst_managed_test_set_keepalive_probe(moq_mvfst_keepalive_probe *probe);
bool moq_mvfst_managed_test_server_keepalive(moq_mvfst_managed_t *m,
    moq_mvfst_keepalive_probe *factory_out, moq_mvfst_keepalive_probe *worker_out);

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
 * EventBase lease observation. A managed facade publishes its client EventBase
 * (client mode) or the QuicServer worker EventBase (server mode) to other
 * threads only through a lease: a borrow keeps the EventBase alive for one call
 * into it, and the owner retires the lease before the EventBase is destroyed.
 *
 * One process-wide probe; set it before creating a facade and clear it (NULL)
 * after destroying it. Callbacks run on the borrowing or retiring thread with
 * no lease lock held:
 *  - borrowed: a borrow was granted, before the borrower uses the EventBase.
 *    Return false to make that borrower skip its use (the borrow still ends).
 *  - borrow_end: that borrow is over, after its use or its skip, and before
 *    the lease stops counting it.
 *  - retire_begin: the owner has unpublished the EventBase (no new borrow can
 *    be granted) and is about to wait for the `granted` borrows the lease
 *    still counts.
 *  - retired: the owner's retirement returned; from here on the owner may
 *    destroy the EventBase.
 *  - drain_begin: client mode, on the owner thread: teardown has closed the
 *    transport and the final drain of the client EventBase (`eventbase`, a
 *    folly::EventBase *) is about to run; work queued on it now belongs to
 *    that drain.
 *  - drain_fails: client mode, on the owner thread, after the non-blocking
 *    restart: return true to make the final drain fail as an event-loop error
 *    would, without running it (pending work stays pending).
 *  - destroy_begin: client mode, on the owner thread: the final drain has
 *    returned (after a failure, the transport was closed at once), the
 *    facade's transport reference is released, and the client EventBase
 *    (`eventbase`) is about to be destroyed; it and its backend are still
 *    alive. `transport_released` says whether the transport object is gone.
 * Any callback may be NULL. `evb` names the lease, `site` the borrow.
 */
enum {
    MOQ_MVFST_TEST_EVB_CLIENT = 1,
    MOQ_MVFST_TEST_EVB_SERVER = 2
};
enum {
    MOQ_MVFST_TEST_EVB_ENQUEUE = 1,      /* queue one pump callback */
    MOQ_MVFST_TEST_EVB_NOTIFY = 2,       /* ask the loop to terminate */
    MOQ_MVFST_TEST_EVB_THREAD_CHECK = 3  /* is the caller the loop thread? */
};
typedef struct moq_mvfst_test_evb_probe {
    void *ctx;
    bool (*borrowed)(void *ctx, int evb, int site);
    void (*borrow_end)(void *ctx, int evb, int site);
    void (*retire_begin)(void *ctx, int evb, unsigned granted);
    void (*retired)(void *ctx, int evb);
    void (*drain_begin)(void *ctx, void *eventbase);
    bool (*drain_fails)(void *ctx);
    void (*destroy_begin)(void *ctx, void *eventbase, bool transport_released);
} moq_mvfst_test_evb_probe;
void moq_mvfst_test_set_evb_probe(const moq_mvfst_test_evb_probe *probe);

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
