#ifndef MOQ_WTQUIC_ADAPTER_INTERNAL_H
#define MOQ_WTQUIC_ADAPTER_INTERNAL_H

/*
 * Private, non-installed lockstep SPI between the wtquic attach adapter and the
 * managed wtquic facades (which are SEPARATE DSOs). NOT part of the installed
 * public API (moq/wtquic.h) and NOT an application ABI.
 */

#include <moq/wtquic.h>

#include <stdbool.h>
#include <stdint.h>

/*
 * Read the attached session's coalesced-doorbell re-drive inputs in ONE call
 * across the DSO boundary: returns the opaque monotonic event-progress token
 * (see moq_transport_bridge_event_progress_token) and writes, through
 * out_has_events, whether the session still holds an undelivered event. The
 * managed facade snapshots the token before its app pump and, after the
 * post-pump service pass, re-arms a pump iff the token moved AND events remain.
 * This avoids exposing the bridge pointer or any application-facing session API.
 * Caller-serialized; token is equality-only (it wraps).
 */
MOQ_API uint64_t moq_wtquic_conn_event_progress(const moq_wtquic_conn_t *conn,
                                                bool *out_has_events);

/*
 * Read the attached session's independent monotonic TERMINAL facts in ONE call
 * across the DSO boundary (see moq_transport_bridge_terminal_facts):
 *
 *   returns        ENQUEUED -- MOQ_EVENT_SESSION_CLOSED was placed in the
 *                  session's event queue.
 *   *out_observed  OBSERVED -- a poll actually TRANSFERRED that event to the
 *                  application. Queued-but-unpolled reads false.
 *
 * A managed facade gates reclamation on OBSERVED (plus its own transport facts
 * and the application's acknowledgment) instead of on pump timing, which only
 * ever proved that a pump ran. Their order relative to transport terminal is
 * not fixed, so the facade must record its transport facts separately.
 * out_observed may be NULL. Caller-serialized. Keeps the bridge pointer out of
 * the facade exactly as the progress query above does.
 */
MOQ_API bool moq_wtquic_conn_terminal_facts(const moq_wtquic_conn_t *conn,
                                            bool *out_observed);

#endif /* MOQ_WTQUIC_ADAPTER_INTERNAL_H */
