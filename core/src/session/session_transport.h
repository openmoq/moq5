#ifndef MOQ_SESSION_TRANSPORT_H
#define MOQ_SESSION_TRANSPORT_H

/*
 * Session <-> transport-bridge internal contract.
 *
 * Declarations the transport bridge needs from the session core that are not
 * part of the public application API (moq/session.h). Kept out of
 * session_internal.h so the bridge does not pull in the full session layout.
 */

#include "moq/session.h"
#include "profile.h"   /* moq_uni_class_t */

/*
 * True if the bound profile carries control on a local/peer pair of
 * unidirectional control channels (so the bridge runs in uni-control-pair
 * mode). False for profiles that use a single bidirectional control channel
 * (draft-16), which keep the bridge in its default mode.
 */
bool moq_session_uses_uni_control(const moq_session_t *s);

/*
 * True when the session still needs the PEER's terminal (FIN/RESET) on this
 * bidi stream after our own send half has closed -- i.e. a peer-origin
 * receive path, a locally-terminated request bidi in the drain ring, or a
 * no-slot admission carrier awaiting its terminal. This is deliberately
 * NARROWER than moq_session_has_transport_stream(), which additionally
 * recognises a live request-registry owner for retainability (#245a): a
 * local-origin request bidi we ourselves FIN'd must still retire even though
 * its owner lingers awaiting a response. The bridge uses this to decide
 * whether to keep a locally-closed mapping alive or tombstone it.
 */
bool moq_session_stream_awaits_peer_terminal(const moq_session_t *s,
                                             moq_stream_ref_t ref);

/*
 * Classify an inbound peer unidirectional stream by its leading bytes. If the
 * profile has no unidirectional control channel (e.g. draft-16), returns
 * MOQ_UNI_CLASS_DATA so every peer unidirectional stream is treated as data.
 */
moq_uni_class_t moq_session_classify_peer_uni(const moq_session_t *s,
                                              const uint8_t *data, size_t len);

/*
 * Opaque monotonic event-progress token. Advances (by an unspecified amount)
 * on every event ENQUEUE and every event DEQUEUE; stable across calls that move
 * no event. A coalesced-doorbell adapter snapshots it BEFORE the app pump and
 * compares for EQUALITY after the post-pump service pass: inequality means the
 * whole cycle moved events — the bounded pump dequeued some (draining a backlog)
 * and/or service enqueued some (refilling the queue). Paired with has_events it
 * drives a bounded re-pump. Never reset; unsigned wrap is acceptable
 * (equality-only). Not part of the public API.
 */
uint64_t moq_session_event_progress_token(const moq_session_t *s);

/*
 * True while the session's event queue holds at least one undelivered event.
 * The bridge pairs this with the progress token: a re-pump is owed only when
 * the cycle made progress AND events still remain to drain. Not public API.
 */
bool moq_session_has_events(const moq_session_t *s);

/*
 * Independent monotonic terminal facts. Recorded separately -- never derived
 * from each other or from a bridge latch -- because their order is not fixed:
 * a local close enqueues MOQ_EVENT_SESSION_CLOSED before any transport
 * shutdown, while a peer close can complete natively first.
 *
 *   enqueued  the terminal event was actually placed in the event queue.
 *   observed  poll_events_ex TRANSFERRED that event to a caller. Availability
 *             is not observation: a queued-but-unpolled terminal reads false.
 *
 * observed implies enqueued. Both are monotonic and idempotent. A managed
 * adapter reads them to gate reclamation on real application observation
 * instead of pump timing. Not public API.
 */
bool moq_session_terminal_enqueued(const moq_session_t *s);
bool moq_session_terminal_observed(const moq_session_t *s);


/*
 * Private suspension sentinel.
 *
 * Returned INTERNALLY when a budgeted advance could not complete its deferred
 * completion sweep within the caller's remaining work budget. moq_result_t is
 * an int whose negative values are errors by convention, so this is not outside
 * that space; it is a value distinct from every currently assigned public
 * result code, chosen far from them so that a boundary which forgets to handle
 * it fails loudly rather than aliasing a real error. Declared only here, in a
 * non-installed header: no application observes it, and unlimited-mode callers
 * complete the sweep before returning.
 *
 * It is NOT MOQ_ERR_WOULD_BLOCK: bridge inbound handlers read that value as
 * retained inbound backpressure and set pending flags, which would manufacture
 * a pending owner for a stream that never blocked. It is NOT an ordinary
 * negative either: every other negative at the bridge/session boundary reaches
 * bridge_set_fatal(). Boundaries must test for it BEFORE both branches, and
 * must leave retryable bridge state intact when they see it.
 */
#define MOQ_SESSION_SUSPENDED (-1000)

/*
 * Budgeted-advance context.
 *
 * Bridge service brackets its pass with these so a deferred-completion sweep
 * can suspend inside it. They MUST be structurally paired: every exit from the
 * bracketed region leaves the context, or a later ordinary session call would
 * inherit the budget and could observe MOQ_SESSION_SUSPENDED, which no
 * application-facing caller may ever see.
 *
 * Ordinary session APIs run with no context: they complete any active sweep at
 * its own epoch, then run one fresh sweep of their own at their now_us -- equal
 * timestamps included -- then their own operation.
 */
/*
 * Budgeted advancing-call preamble. Requires an active budget context and
 * spends it directly. Returns MOQ_SESSION_SUSPENDED only when runnable work
 * could not be afforded; the cursor then holds the resume point.
 */
moq_result_t session_begin_advance_budgeted(moq_session_t *s, uint64_t now_us);

void session_budget_enter(moq_session_t *s, uint32_t budget);
void session_budget_leave(moq_session_t *s);

/* Budget left in the active context. Read before session_budget_leave(), which
 * zeroes it, to report what a budgeted pass actually spent. */
uint32_t session_budget_remaining(const moq_session_t *s);

#if defined(MOQ_SESSION_SWEEP_TESTING)
/* Gated counters, compiled only into moq-core-test-internals. Tests assert
 * DELTAS across a call: several tests in one binary accumulate into them. */
extern uint64_t session_budget_enter_count;    /* budget contexts entered */
extern uint64_t session_budget_suspend_count;  /* budgeted advances suspended */
/* Namespace-suffix membership-work probe: bumped once per ns_suffix_key_cmp()
 * call, which is shared by the inbound (receive-budget-counted) and outbound
 * (publisher) suffix sets. The counter is NOT inbound-only; tests measure its
 * DELTA around a controlled inbound workload. It observes membership work and
 * does not change it. Representation-neutral: a future ordered-tree membership
 * would probe far fewer keys per insert. */
extern uint64_t session_ns_suffix_probe_count;
#endif

#endif /* MOQ_SESSION_TRANSPORT_H */
