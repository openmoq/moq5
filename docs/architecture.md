# Architecture

LibMoQ is a C11 sans-I/O library for Media over QUIC Transport. The session
is a deterministic state machine: same inputs plus same time produce identical
outputs, every time.

## Sans-I/O

The library performs no I/O. It does not open sockets, spawn threads, read
the system clock, use hidden allocators, or touch global state. All
allocation goes through the caller-provided `moq_alloc_t`.

Instead, the caller drives the session:

1. Feed incoming control channel bytes via `moq_session_on_control_bytes`.
2. Feed incoming data stream bytes via `moq_session_on_data_bytes`.
3. Advance virtual time via `moq_session_tick`.
4. Pull outbound transport instructions via `moq_session_poll_actions`.
5. Pull application notifications via `moq_session_poll_events`.

This separation means the same library code runs in production (with a real
QUIC stack) and in simulation (with SimPair wiring two sessions back-to-back,
no network involved).

## Advancing vs Observing Calls

Every public function on a session is either **advancing** or **observing**.

**Advancing calls** change the session's internal state. They:

- Take a `uint64_t now_us` parameter (virtual time in microseconds).
- Increment the borrow epoch, invalidating previously polled data pointers.
- May produce new actions and events.

Examples: `start`, `on_control_bytes`, `on_data_bytes`, `on_data_reset`,
`on_data_stop`, `process_pending`, `tick`, `subscribe`, `accept_subscribe`,
`reject_subscribe`, `open_subgroup`, `write_object`, `begin_object`,
`write_object_data`, `end_object`, `close_subgroup`,
`reset_subgroup`, `publish_namespace`, `publish_namespace_done`,
`accept_namespace`, `cancel_namespace`.

**Observing calls** do not take `now_us` and do not invalidate borrows.
Some observing calls, such as `poll_actions` and `poll_events`, drain
output queues. Polled data remains valid until the next advancing call.

Examples: `moq_session_state`, `moq_session_borrow_valid`,
`moq_session_poll_actions`, `moq_session_poll_events`,
`moq_session_peer_request_capacity`.

## Actions and Events

The session produces two kinds of output:

**Actions** are instructions for the transport adapter:

- `SEND_CONTROL` — write these bytes to the control channel.
- `CLOSE_SESSION` — close the transport with this error code.
- `SEND_DATA` — write header bytes and/or payload to a data stream.
  Carries an owned rcbuf payload that the adapter must release via
  `moq_action_cleanup`.
- `RESET_DATA` — reset a data stream with an error code.
- `STOP_DATA` — stop receiving a data stream (send QUIC STOP_SENDING).

**Events** are notifications for the application:

- `SETUP_COMPLETE` — handshake finished. May carry resolved authorization
  tokens (borrowed from output scratch).
- `SESSION_CLOSED` — session terminated with code and reason.
- `SUBSCRIBE_REQUEST` — peer wants to subscribe to a track. May carry
  resolved authorization tokens (borrowed from output scratch).
- `SUBSCRIBE_OK` — subscription accepted by the publisher.
- `SUBSCRIBE_ERROR` — subscription rejected.
- `REQUEST_READY` — previously blocked request can now be retried.
- `OBJECT_RECEIVED` — an object arrived on a data stream. Carries an
  owned rcbuf payload and optional properties rcbuf that the caller
  must release via `moq_event_cleanup`.
- `NAMESPACE_PUBLISHED` — peer advertised a namespace. May carry
  resolved authorization tokens (borrowed from output scratch).
- Namespace subscription ended — peer closed the namespace stream.
- `NAMESPACE_REJECTED` — peer rejected our namespace advertisement.
- `NAMESPACE_ACCEPTED` — peer accepted our namespace advertisement.
- `NAMESPACE_CANCELLED` — peer revoked prior acceptance of our namespace.

Both actions and events use a tagged-union record with a named `.u` member,
a `kind` discriminator, and reserved inline capacity for future variants.

### Owned Resources

Some actions and events carry owned `moq_rcbuf_t` references:

- `SEND_DATA` actions own a payload ref. The adapter must call
  `moq_action_cleanup` after processing or dropping every polled action.
- `OBJECT_RECEIVED` events own payload and properties refs. The caller
  must call `moq_event_cleanup` after processing or dropping every
  polled event.

Failure to clean up polled records leaks reference counts. The session
decrefs all queued owned resources on `close_with_error` and `destroy`.

## Feature Matrix

| Area | Implemented | Deferred |
|------|-------------|----------|
| **Session core** | Setup + request credit negotiation, subscribe/accept/reject/unsubscribe, subscription update (priority/forward/timeout), done-subscribe, publish (publisher-initiated), fetch (standalone + joining), track status, namespace advertisement, namespace subscription with prefix overlap + suffix ordering, authorization tokens (inbound + outbound), request updates (subscription full, others NOT_SUPPORTED), object delivery (subgroups + datagrams), GOAWAY drain, idle timeout, forward enforcement, bidi-stream dispatch | d18 wire profile, full publish-initiated update |
| **Publisher facade** | Track lifecycle, accept-all/reject-all/callback/defer authorization, deferred resolve with generation-safe handles, subscription update callback, object writes (complete + streaming + datagram), namespace advertisement with refcounting | — (multi-session fan-out is a host loop pattern, not a facade feature — see memory.md) |
| **Subscriber facade** | Subscribe/unsubscribe, subscription update, whole objects, streaming chunks, track status, fetch (standalone + joining), auth tokens, done-subscribe handling, GOAWAY URI callback | — |
| **Picoquic adapter** | Transport bridge for picoquic QUIC stack ([adapter guide](adapters.md)) | — |
| **Examples** | Facade-first publisher + subscriber over picoquic, priority update demo | — |

### Request-Stream Dispatch

Inbound bidi streams are classified by the wire profile before
session core routes them. In D16, only namespace subscription is a
valid bidi request stream; unknown stream types close the session.
The dispatch scaffold (`classify_bidi_stream` profile op) exists so
future profiles (d18+) can route additional request types without
changing session core.

### Version / Profile Selection

Each session binds to exactly one wire profile, selected at create time
by `moq_session_cfg_t.version` and resolved through `moq_profile_lookup`
to a `moq_profile_ops_t` vtable. `0` defaults to draft-16; an unsupported
value fails `moq_session_create` with `MOQ_ERR_INVAL`. The profile is
immutable for the session lifetime (profile state is co-allocated in the
session's contiguous block) — there is no runtime profile switch and no
"try each decoder until one parses." A future draft is added as a new
`profile_*.c` + a `moq_version_t` value + a `moq_profile_lookup` case,
with no change to session core.

The version is **chosen by transport negotiation, not by sniffing wire
bytes**. For native MoQ-over-QUIC and WebTransport-over-QUIC the QUIC ALPN
carries it (`moqt-16` ⇒ draft-16); for H3 WebTransport the ALPN is `h3`
and the MoQ version comes from the WebTransport protocol negotiation. The
sans-I/O core never sees the ALPN: the host/adapter maps the negotiated
transport version to `cfg.version` before creating the session. Because
the draft-16 setup handshake carries no version list, a peer that used a
legacy ALPN (e.g. `moq-00`, which prepends a version array) is rejected
by the strict setup parser, which emits an advisory diagnostic pointing
at the likely ALPN mismatch.

### Request Credit

The public API uses draft-neutral terms. D16 compatibility aliases
(containing wire-specific naming) remain for existing callers but
delegate to the semantic functions internally:

- `peer_request_capacity` (semantic) / `peer_max_request_id` (D16) <!-- api-boundary-exempt -->
- `local_request_capacity` (semantic) / `local_max_request_id` (D16) <!-- api-boundary-exempt -->
- `grant_request_capacity` (semantic) / `update_max_request_id` (D16) <!-- api-boundary-exempt -->

In draft-16, request capacity is realized as an exclusive request-ID
ceiling (even IDs for client requests, odd for server). The semantic API
abstracts this as a monotonically increasing credit value. The D16
compatibility aliases delegate to the semantic functions and remain for
existing callers; new code should use the semantic names.

Config fields `send_request_capacity` and `initial_request_capacity`
on `moq_session_cfg_t` are already draft-neutral.

## Header Tiers

LibMoQ exposes three public header levels plus the facade headers,
enforced by a CI boundary check:

| Header | Purpose | Includes |
|--------|---------|----------|
| `<moq/moq.h>` | Stable application API | Sessions, handles, actions, events, result codes, allocator, byte spans, rcbuf |
| `<moq/publisher.h>` | Publisher facade | Track management, accept/reject/defer authorization, object writing (include directly) |
| `<moq/subscriber.h>` | Subscriber facade | Subscribe, unsubscribe, subscription update, objects, chunks, fetch, track status (include directly) |
| `<moq/codec.h>` | Wire-profile internals | Integer encodings, parameter types, control message codecs (draft-16 wire) |
| `<moq/sim.h>` | Deterministic simulation | SimPair, trace callbacks, virtual time helpers |

Application code includes `<moq/moq.h>` and optionally the facade
headers. `<moq/publisher.h>` and `<moq/subscriber.h>` are not included
by the umbrella — include them directly. Adapters, fuzzers, test
harnesses, and relay internals include `<moq/codec.h>`. Simulation
tests include `<moq/sim.h>`.

Stable headers use semantic names (subscriptions, tracks, groups) rather
than wire encodings (integer encodings, parameter types, message codes). The CI
boundary guard fails the build if wire-internal terms appear in stable
headers or application examples.

## Module Layout

The session implementation is split by domain:

| File | Responsibility |
|------|---------------|
| `session.c` | Core lifecycle, dispatcher, poll, infrastructure |
| `session_setup.c` | Setup handshake |
| `session_subscribe.c` | Subscribe protocol and subscription pool |
| `session_subgroup.c` | Outgoing subgroup data path |
| `session_receive.c` | Incoming data stream parser |
| `session_namespace.c` | Namespace advertisement lifecycle |
| `session_auth.c` | Token cache and shared AUTH_TOKEN processing |
| `session_internal.h` | Private types, struct, shared declarations |

Each domain file has its own test file (`test_session_*.c`).

## Facades

The `moq_publisher_t` and `moq_subscriber_t` facades wrap session
event plumbing into ergonomic track-oriented APIs.

### Publisher scope

A `moq_publisher_t` owns exactly one `moq_session_t`. Session core
enforces that each track can have at most one publisher-role
subscription per session (duplicate same-track subscribes are
rejected before the publisher facade sees them). Internally the
publisher maintains a per-track subscriber slot array for structural
readiness, but same-session same-track fan-out is not possible at
this layer.

Multiple subscribers to the same content are handled by multiple
sessions — one per subscribing peer — each with its own publisher
facade instance. A future multi-session publisher service or relay
layer would coordinate across sessions.

### Deferred subscribe authorization

In `MOQ_PUB_CALLBACK` mode, the `on_subscribe` callback may return
`MOQ_PUB_DECISION_DEFER` to delay the accept/reject decision. The
facade stores the pending subscription and the caller resolves it
later via `moq_pub_resolve_deferred()`.

Constraints:
- One deferred request per publisher at a time. When the slot is
  occupied, `info->deferred` is NULL and the callback must return
  ACCEPT or REJECT synchronously.
- `deferred_id` from `info->deferred_id` must be passed back to
  `resolve_deferred`; mismatched IDs return `MOQ_ERR_STALE_HANDLE`.
- Deferred handles become stale after: resolve, peer unsubscribe,
  session close, or track removal.
- If resolve returns `MOQ_ERR_WOULD_BLOCK`, `moq_pub_flush` or
  `moq_pub_tick` retries the accept/reject.

### Subscription updates

When a subscriber changes priority, forward, or delivery timeout,
the session emits `MOQ_EVENT_SUBSCRIBE_UPDATED`. The publisher
facade dispatches this to `on_subscriber_updated` in
`moq_pub_callbacks_t` with a `moq_pub_subscribe_update_info_t`
containing the changed fields. The callback is appended and
`struct_size`-gated, so old callers that predate the field are
unaffected.
