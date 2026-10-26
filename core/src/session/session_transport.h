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

/* Observing: the bound profile's stream error for cancellation at a local
 * resource limit. Requires a fully initialized session; changes no state. */
uint64_t moq_session_resource_limit_stream_error(const moq_session_t *s);

/*
 * True when the session still needs the PEER's terminal (FIN/RESET) on this
 * bidi stream after our own send half has closed -- i.e. a peer-origin
 * receive path, a locally-terminated request bidi in the drain ring, or a
 * no-slot admission carrier awaiting its terminal. This is deliberately
 * NARROWER than moq_session_has_transport_stream(), which additionally
 * recognises a live request-registry owner for retainability: a
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
 * Exclusive transport ownership. A session is driven by at most one transport
 * owner (a transport bridge) at a time, from the owner's creation until its
 * destruction; the session must outlive its owner. Register fails with
 * MOQ_ERR_WRONG_STATE, changing nothing, while a different owner is
 * registered -- a fatal or closed bridge still holds the registration until it
 * is destroyed. Unregister clears the slot only when `owner` holds it. The
 * owner is an opaque identity: the session never dereferences it. Not public
 * API.
 */
moq_result_t moq_session_register_transport_owner(moq_session_t *s,
                                                  const void *owner);
void moq_session_unregister_transport_owner(moq_session_t *s,
                                            const void *owner);

/* Receive-entry pressure: private bridge policy, never application API.
 * All three operations are serialized with the session. They do not advance
 * time, run a sweep, invalidate borrows, or invoke the transport. The bridge
 * must settle prior inbound obligations before selecting/cancelling, and
 * retain its victim ref until rx_entry_active becomes false. A suspended
 * sweep makes selection/cancellation WOULD_BLOCK; resume it normally first.
 *
 * A candidate is an admitted SUBGROUP with exactly one live subscriber-role
 * subscription/publication binding. FETCH, fill, shared aliases, unbound or
 * ended bindings and already-STOPPED streams are excluded. Pending delivery,
 * peer terminal, local teardown or malformed-track work must drain first.
 * Both priorities are the owner's/current stream's effective values, not
 * pending update values. Larger numbers are worse; ties choose the lowest
 * full ref (not admission age). Capacity is not part of the ranking. */
typedef struct moq_admitted_data_candidate {
    moq_stream_ref_t stream_ref;
    uint8_t subscriber_priority;
    uint8_t publisher_priority;
} moq_admitted_data_candidate_t;

/* Observing. out is required and zeroed on every failure/no-candidate result.
 * INVAL: NULL session/out/owner or owner not registered on this session.
 * CLOSED: session inactive. OK: one eligible candidate in out.
 * WOULD_BLOCK: sweep suspended, or candidates exist but all owe prior work.
 * DONE: no candidate exists. None of these outcomes mutates session state. */
moq_result_t moq_session_worst_admitted_data(moq_session_t *s,
    const void *owner, moq_admitted_data_candidate_t *out);

/* Non-advancing transaction. INVAL: invalid/unregistered owner (checked first),
 * absent/ineligible ref, or code other than this profile's pressure code:
 * D16 CANCELLED (0x1), D18/D22 EXCESSIVE_LOAD (0x9). CLOSED: inactive session.
 * WOULD_BLOCK: suspended sweep, prior work, or insufficient action/event slots.
 * Every non-OK result leaves the session unchanged. OK queues exactly one
 * STOP_DATA with code; a surfaced unfinished object also gets one empty
 * OBJECT_CHUNK end with terminal STOP and error_code zero. The entry releases
 * parser resources but retains its binding in STOPPED. Genuine terminal or
 * positive local transport retirement counts/frees it through existing paths;
 * owner teardown may instead release the slot into a numeric stopped record.
 * OK does not claim recovered RX capacity or peer reset.
 * The bridge must preserve the selected victim on ring pressure and must not
 * invoke this operation past a retained inbound obligation for that ref. */
moq_result_t moq_session_cancel_admitted_data(moq_session_t *s,
    const void *owner, moq_stream_ref_t ref, uint64_t code);

/* Observing exact-ref RX occupancy, including STOPPED entries. False for NULL
 * session, invalid/unregistered owner, absent ref, or a numeric stopped record.
 * Independent of binding/session liveness: an occupied entry is still pressure.
 * This deliberately differs from moq_session_has_transport_stream(). */
bool moq_session_rx_entry_active(const moq_session_t *s, const void *owner,
                                 moq_stream_ref_t ref);

/*
 * The registered owner publishes its own next deadline (has == false clears
 * it). The session reports it through moq_session_next_deadline_us so an
 * idle adapter wakes for it, but never acts on it: moq_session_tick leaves it
 * in place, and only the owner's service pass rearms it. Ignored unless
 * `owner` is the registered owner; unregistering clears it. Not public API.
 */
void moq_session_set_owner_deadline(moq_session_t *s, const void *owner,
                                    bool has, uint64_t deadline_us);


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
