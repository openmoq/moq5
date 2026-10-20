# Architecture

LibMoQ is a C11 sans-I/O library for Media over QUIC Transport. The session
is a deterministic state machine: same inputs plus same time produce identical
outputs, every time. It speaks draft-16, draft-18 and draft-22 of
draft-ietf-moq-transport, one per session; [conformance.md](conformance.md)
has the per-draft status.

## Sans-I/O

The library performs no I/O. It does not open sockets, spawn threads, read
the system clock, use hidden allocators, or touch global state. All
allocation goes through the caller-provided `moq_alloc_t`.

Instead, the caller drives the session:

1. Feed incoming control bytes (`moq_session_on_control_bytes`), data stream
   bytes (`moq_session_on_data_bytes` / `moq_session_on_data_rcbuf`),
   request-stream bytes (`moq_session_on_bidi_stream_bytes`) and datagrams
   (`moq_session_on_datagram`).
2. Advance virtual time via `moq_session_tick`.
3. Pull outbound transport instructions via `moq_session_poll_actions`.
4. Pull application notifications via `moq_session_poll_events`.

This separation means the same library code runs in production (with a real
QUIC stack) and in simulation (with SimPair wiring two sessions back-to-back,
no network involved). Adapters do not usually drive the session by hand: the
shared transport bridge (`moq_transport_bridge_t`) does it for them
([adapters.md](adapters.md)).

## Advancing vs Observing Calls

Every public function on a session is either **advancing** or **observing**.

**Advancing calls** change the session's internal state. They:

- Take a `uint64_t now_us` parameter (virtual time in microseconds).
- Increment the borrow epoch, invalidating previously polled data pointers.
- May produce new actions and events.

Examples: `start`, `on_control_bytes`, `on_data_bytes`, `tick`, `subscribe`,
`accept_subscribe`, `open_subgroup`, `write_object`, `reset_subgroup`,
`accept_subscription_update`, `open_fill`.

**Observing calls** do not take `now_us` and do not invalidate borrows.
Some observing calls, such as `poll_actions` and `poll_events`, drain
output queues. Polled data remains valid until the next advancing call.

Examples: `moq_session_state`, `moq_session_borrow_valid`,
`moq_session_poll_actions`, `moq_session_poll_events`,
`moq_session_version`, `moq_session_supports_fill`,
`moq_session_get_publish_initial_parameters`.

Advancing calls that can fail for want of queue space reserve every action
and event they need before changing anything, and return
`MOQ_ERR_WOULD_BLOCK` with nothing applied otherwise; the caller drains and
retries.

## Actions and Events

**Actions** are instructions for the transport (`MOQ_ACTION_*`):

- `SEND_CONTROL`, `OPEN_UNI_CONTROL`, `SEND_UNI_CONTROL` — control channel
  bytes (draft-16 uses a bidirectional control channel, draft-18 and draft-22
  one unidirectional stream per side).
- `OPEN_BIDI_STREAM`, `SEND_BIDI_STREAM`, `CLOSE_BIDI_STREAM`,
  `RESET_BIDI_STREAM`, `STOP_BIDI_STREAM`, `ABORT_BIDI_STREAM` — request
  streams.
- `SEND_DATA` — header bytes and/or an owned rcbuf payload on a data stream.
- `RESET_DATA` — reset a data stream; carries `reliable_size`, the length of
  the stream's own header (see "Reliable size" below).
- `STOP_DATA` — STOP_SENDING on a received data stream.
- `SEND_DATAGRAM` — one object datagram.
- `CLOSE_SESSION` — close the transport with this error code.

**Events** are notifications for the application (`MOQ_EVENT_*`): setup and
closure, the request families (subscribe, publish, fetch, track status,
namespaces, SUBSCRIBE_TRACKS) with their answers and terminals, objects and
object chunks, subgroup FIN and reset, request redirects and per-request
GOAWAY, update answers (`SUBSCRIPTION_UPDATE_OK`, `PUBLICATION_UPDATE_OK`),
publisher state notifications (`PUBLISH_STATE`), request terminals
(`REQUEST_TERMINATED`), and the fill family (`FILL_CREATED`,
`FILL_REQUEST`, `FILL_OBJECT`, `FILL_GAP`, `FILL_TERMINAL`). The full list is
in `<moq/session.h>`.

Both actions and events use a tagged-union record with a named `.u` member,
a `kind` discriminator, and reserved inline capacity for future variants. A
caller built against an older header polls with `moq_session_poll_*_ex` and
its own record size; fields appended to an event are read only when
`detail_size` covers them.

### Owned Resources

Some actions and events carry owned `moq_rcbuf_t` references: `SEND_DATA`
payloads, received object payloads and properties, fill objects. The adapter
calls `moq_action_cleanup` and the application `moq_event_cleanup` after
processing or dropping every polled record; the session decrefs all queued
owned resources on `close_with_error` and `destroy`. See
[memory.md](memory.md).

## Wire profiles and capabilities

Each session binds to exactly one wire profile, selected at create time by
`moq_session_cfg_t.version` (0 means draft-16) and resolved through
`moq_profile_lookup` to a constant `moq_profile_ops_t` table:
`profile_d16.c`, `profile_d18.c`, `profile_d22.c`. An unsupported value fails
`moq_session_create` with `MOQ_ERR_INVAL`. The profile is fixed for the
session's life; there is no runtime switch and no "try each decoder until one
parses".

The version is chosen by transport negotiation, not by sniffing bytes: the
ALPN (`moqt-16`, `moqt-18`, `moqt-22`) over raw QUIC, the WebTransport
protocol token over WebTransport. The sans-I/O core never sees either; the
adapter or host maps the negotiated version to `cfg.version` before creating
the session. `moq_versions_supported()` lists the wire-ready versions newest
first. The service endpoint's AUTO offer and the relay CLI's default offer
prefer the newest supported draft: `[22, 18, 16]`. Endpoint AUTO also filters
the offer to drafts its selected backend can carry
([conformance.md](conformance.md#versions-and-the-default-offer)).

The session core never tests a draft number. Wherever drafts differ in
behaviour, the profile declares a capability and the core asks it;
`scripts/check_profile_boundary.sh` rejects draft-named vocabulary in the
core and writable profile tables. The capabilities, with each profile's
value:

| Capability | Meaning | 16 | 18 | 22 |
| --- | --- | --- | --- | --- |
| `wire_ready` | The profile is complete enough to offer to peers | yes | yes | yes |
| `uses_request_streams` | One bidirectional stream per request, correlated by the stream | no | yes | yes |
| `uses_uni_control_channel` | One unidirectional control channel per side | no | yes | yes |
| `object_payload_len_max` | Largest encodable Object Payload Length | 2^62-1 | 2^64-1 | 2^64-1 |
| `location_varint_max` | Largest encodable Group / Object ID | 2^62-1 | 2^64-1 | 2^64-1 | <!-- api-boundary-exempt -->
| `request_error_wire_max` | Largest encodable REQUEST_ERROR code | 2^62-1 | 2^64-1 | 2^64-1 | <!-- api-boundary-exempt -->
| `min_track_namespace_fields` | Fewest fields a Track Namespace may have | 1 | 0 | 0 |
| `fetch_datagram_supported` | The fetch object header carries the datagram bit | no | yes | yes |
| `fetch_descending_supported` | A Descending FETCH response can be reconstructed | yes | no | no |
| `request_fin_is_not_cancel` | A requester FIN only closes its direction | no | no | yes |
| `premature_fin_fails_request` | A responder FIN before its required messages fails the request, not the session | no | no | yes |
| `request_drain_requires_receive_terminal` | A cancelled request stream is released only by the peer's FIN or RESET | no | no | yes |
| `allows_concurrent_subscriptions` | Several live subscriptions to one track | no | no | yes |
| `duplicate_subscription_error` | Code refusing a second subscription to a track | DUPLICATE_SUBSCRIPTION (0x19) | DUPLICATE_SUBSCRIPTION (0x19) | NOT_SUPPORTED (never sent: concurrent subscriptions are allowed) |
| `publish_ok_carries_params` | The PUBLISH response carries the subscriber's choices | yes | yes | no (they follow as a REQUEST_UPDATE) | <!-- api-boundary-exempt -->
| `supports_joining_fetch` | FETCH has joining forms | yes | yes | no |
| `supports_fill` | Subscriptions may request fills | no | no | yes |
| `fetch_request_options` | FETCH carries a Location Filter range and FILL_TIMEOUT | no | no | yes |
| `fetch_range_timed_out` | A fetch stream can mark a range Timed-Out | no | no | yes |
| `stream_count_unknown` | PUBLISH_DONE Stream Count meaning "not counted" | 2^62-1 | 2^62-1 | 2^64-1 | <!-- api-boundary-exempt -->
| `subgroup_timer_at_fin` | SUBGROUP_DELIVERY_TIMEOUT starts at the subgroup's FIN | no | no | yes |
| `object_timer_at_header` | OBJECT_DELIVERY_TIMEOUT starts at the object's header | yes | no (first payload byte) | yes |
| `first_object_timeout_override` | The first object's timeout property overrides the track's | no | no | yes |
| `publish_done_subscription_ended` | PUBLISH_DONE has SUBSCRIPTION_ENDED for an exhausted filter | yes | yes | no | <!-- api-boundary-exempt -->
| `redirect_on_any_request` | REDIRECT may answer every request family | no | no | yes |
| `binding_requirements` | Transport facts required before start | none | none | all (`MOQ_BINDING_FACTS_ALL`) |
| `request_update_errors_coalesce` | One REQUEST_ERROR may answer a coalesced run of failed updates | no | yes | yes | <!-- api-boundary-exempt -->
| `request_updates_max` | MAX_REQUEST_UPDATES this side advertises | none | none | 1 | <!-- api-boundary-exempt -->
| `request_updates_exceeded_error` | Session error for a peer that exceeds it | — | — | TOO_MANY_REQUEST_UPDATES (0x1B) | <!-- api-boundary-exempt -->
| `going_away_request_error` | REQUEST_ERROR code for a request after our GOAWAY | INTERNAL_ERROR | GOING_AWAY | GOING_AWAY | <!-- api-boundary-exempt -->
| `reserved_session_namespaces` | "." and ".session" namespaces are refused DOES_NOT_EXIST | no | yes | yes |

The table also carries the codec entry points: request and response decoding
for the shared request-stream engine (`request_codec`), data-plane encoders
and decoders, stream classifiers and the semantic mapping of received
request error codes. The draft-22 table reuses draft-18's data-plane
operations and adds only its End of Timed-Out Range handling. Public queries
expose the capabilities an application needs without naming a draft:
`moq_session_supports_fill`, `moq_session_supports_joining_fetch`,
`moq_session_supports_concurrent_subscriptions`,
`moq_session_supports_fetch_datagram`, `moq_session_binding_requirements`.

## Request streams

Draft-18 and draft-22 carry every request on its own bidirectional stream.
One engine serves both (`core/src/session/request_stream.c`): it allocates
parity Request IDs (client even, server odd, never recycled, peer IDs that
arrive out of order tracked as merged ranges), dispatches buffered request
and response messages to the core handlers through the profile's codec table,
applies the first-message rules in both directions, resolves authorization
tokens only after a request is admitted, and migrates a request on a
request-stream GOAWAY. The profile supplies only bytes: each decoder turns a
complete message body into the draft-neutral record the core handlers
consume.

Each request also has a lifetime value (`request_lifetime.c`), a pure state
machine over the two directions of its stream: FIN, RESET and STOP_SENDING
observed in each direction, the initial response, and owed update answers.
On draft-22 the directions close independently: a requester FIN is not a
cancellation, a responder FIN before its required messages fails only that
request (`MOQ_REQUEST_TERMINAL_MISSING_RESPONSE` or
`MOQ_REQUEST_TERMINAL_TRUNCATED_RESPONSE`), and a cancelled request's stream
is kept as a drain reference until the peer's own direction ends. Draft-18
keeps its older readings through the same capabilities.

### Owed stream actions

A teardown (unsubscribe, a peer reset or STOP_SENDING of a request stream,
a reject, a session-originated update refusal) reserves only its mandatory
actions: the response or terminal message it must send. Each subgroup RESET
the publisher owes and each STOP_SENDING the subscriber owes goes out if the
action queue has room; one that does not fit is recorded on its stream
entry. After every poll, `session_retry_owed_stream_actions()` sends owed
STOPs, then owed RESETs, then any terminal held behind them. A PUBLISH_DONE <!-- api-boundary-exempt -->
that must follow those resets (the publisher "MUST NOT send" it before the
streams are closed) is held until the last owed reset is queued. Nothing is
dropped and no teardown waits on queue space it cannot get.

### Reliable size

Every data-stream reset the session originates carries `reliable_size`: the
encoded length of that stream's own header (SUBGROUP_HEADER or
FETCH_HEADER), so a transport that can reset reliably keeps the header and
the receiver can tell which subscription, fetch or fill the reset ended. The
size never exceeds what the session queued on the stream. What each adapter
does with it is in [adapters.md](adapters.md#reliable-resets).

## Fills

A fill (draft-22 §3.4) is a fetch response a publisher owes a subscription
because the SUBSCRIBE or a subscription update asked for past objects. It is a
child of the subscription, never an independent request:

- `fill_owner.c` holds the ownership table for every fill a subscription
  owns on either side, with no allocation and no I/O.
- `fill_input.c` validates a requested fill; a value FILL_PARAMETERS omits
  takes the subscription's (§9.20.15).
- `fill_range.c` resolves the fill's Location Filter against one Largest
  Object snapshot; an empty range, or one starting past Largest, opens no
  stream.
- `session_fill.c` binds the table to the session: the pool
  (`moq_session_cfg_t.max_fills`), public handles, events, and the
  publisher's write path (`moq_session_open_fill`, `write_fill_object`,
  `write_fill_range`, `end_fill`, `abort_fill`).

Admission reserves the pool slot and the event the admission itself surfaces.
Every later effect (a RESET or STOP owed on the stream, the FILL_REQUEST, the
FILL_TERMINAL) is an obligation the table keeps until there is capacity, so
teardown is never dropped. A peer's fill the pool cannot admit is still
answered: its fetch stream is opened and reset right after the FETCH_HEADER,
so the subscriber learns that fill failed. Cancelling the subscription resets
every open fill; a fill ending never ends its subscription. Every fill ends
with exactly one FILL_TERMINAL on each side.

## Scheduling

The publisher's outbound data actions are ordered by draft-22 §5.1.2:
subscriber priority, then publisher priority, then the subscription's group
order between groups (a fill whose group order differs from its
subscription's yields to the subscription-delivered object), then within a
group the fill-delivered object first, a datagram before a subgroup object,
then the lowest Subgroup ID or Object ID. Actions on one stream keep their
order; control and other non-data actions keep their place in the queue and
data queued after one does not overtake it. A subscription whose GROUP_ORDER
was omitted uses the track's DEFAULT_PUBLISHER_GROUP_ORDER, Ascending when
that is absent too. An object's delivery timeout is checked when the object
is handed to the transport, so an expired object never reaches it. Tests: `scheduling_order`,
`delivery_timers`, `group_order_default`.

## Transport binding

A draft-22 session must be told how it is carried before it starts:
`moq_session_bind_transport()` with a `moq_transport_binding_t`
(`<moq/transport_binding.h>`). The binding states:

- the kind: raw QUIC, or WebTransport over HTTP/3 (WebTransport over HTTP/2
  is refused);
- facts, each either known true, known false or unknown: handshake complete,
  local and peer QUIC DATAGRAM support, datagram send and receive;
- the connection target: a raw-QUIC client's authority and path/query, which
  its SETUP sends as AUTHORITY and PATH, or the authority and path a
  WebTransport CONNECT named; a raw-QUIC server instead supplies an
  `accept_uri` policy consulted on each client SETUP.

A required fact that is not known yet is `MOQ_ERR_WOULD_BLOCK`, one known
false is `MOQ_ERR_UNSUPPORTED`; every refusal leaves the session unchanged. A
session whose profile requires a binding refuses to start, and refuses
input, until it has one. Profiles without requirements (16, 18) accept the
same binding optionally, with no facts needed: bound, a raw-QUIC client's
setup names its target and a server judges the target it is sent (refused
over WebTransport, or by its policy); never bound, the session starts and
emits its setup as it always has, naming no target. Bound or not, a server
refuses a malformed target value (MALFORMED_PATH / MALFORMED_AUTHORITY). Managed facades take a `moq_connection_target_t`
(`<moq/connection_target.h>`) in their config and bind for you; the service
endpoint passes the target it derives from its URL. Per-adapter details are
in [adapters.md](adapters.md#transport-binding).

## Concurrent subscriptions

On draft-22 several live subscriptions to one track are legal. On the
publisher side each gets its own alias by default; an explicit shared alias
for the same track is allowed, and an object matching several subscriptions
is sent once per matching subscription. On the subscriber side, the
subscriptions that share an alias form one set for attribution, events,
closures, Stream Count, expiry and datagrams:

- a subgroup's data stream on a shared alias is carried in shared mode: each object
  is matched at admission against every sharing subscription's Forward state
  and Location window, and the result is frozen for that object;
- an object is delivered as one event per matching subscription, each naming
  its subscription and owning its own reference to the same payload; an
  object no subscription accepts is discarded;
- Stream Count counts a shared stream only for a subscription it alone
  reached; an ambiguous count completes on the terminal timer;
- while a subscription to the track awaits its SUBSCRIBE_OK, objects on an
  alias held by an established subscription to that track that the pending
  filter would accept are kept for it in the bounded early-data buffer, and
  delivered if the SUBSCRIBE_OK names the same alias (§3.1.3). Objects
  delivered as chunks are not kept.

Range Filters are not part of attribution: the session accepts none.

## Request credit

The public API uses draft-neutral terms. D16 compatibility aliases
(containing wire-specific naming) remain for existing callers but
delegate to the semantic functions internally:

- `peer_request_capacity` (semantic) / `peer_max_request_id` (D16) <!-- api-boundary-exempt -->
- `local_request_capacity` (semantic) / `local_max_request_id` (D16) <!-- api-boundary-exempt -->
- `grant_request_capacity` (semantic) / `update_max_request_id` (D16) <!-- api-boundary-exempt -->

In draft-16, request capacity is realized as an exclusive request-ID
ceiling (even IDs for client requests, odd for server). On draft-18 and
draft-22 there is no request-ID ceiling; QUIC stream limits bound the number
of request streams.

## Header Tiers

LibMoQ exposes these public header levels, enforced by a CI boundary check
(`scripts/check_api_boundary.sh`):

| Header | Purpose |
|--------|---------|
| `<moq/moq.h>` | Stable application API umbrella: sessions, handles, actions, events, result codes, allocator, byte spans, rcbuf, plus `location.h`, `fill.h`, `transport_binding.h`, `connection_target.h` |
| `<moq/publisher.h>` | Publisher facade (include directly) |
| `<moq/subscriber.h>` | Subscriber facade (include directly) |
| `<moq/codec.h>` | Wire-profile internals: integer encodings, parameter types, control message codecs |
| `<moq/sim.h>` | Deterministic simulation |

Application code includes `<moq/moq.h>` and optionally the facade headers.
Adapters, fuzzers, test harnesses, and relay internals include
`<moq/codec.h>`. Stable headers use semantic names (subscriptions, tracks,
groups, filters, fills) rather than wire encodings; the boundary guard fails
the build if wire-internal terms appear in stable headers or application
examples. See [api-boundaries.md](api-boundaries.md).

Configuration structs carry `struct_size`; a field appended to one is read
only when `struct_size` covers it. Where a plain `*_init` function can only
stamp an older layout, a `*_init_sized(cfg, sizeof *cfg)` form does the full
initialization, and a same-name macro routes C and C++ callers to it.
Bindings that call the exported symbols directly (Swift, an FFI) must call the
sized form, or the fields they set past the old layout are ignored.

## Module Layout

`core/src/session/`:

| File | Responsibility |
|------|---------------|
| `session.c` | Lifecycle, dispatcher, poll, owed stream actions, infrastructure |
| `session_setup.c` | Session start and the setup completion shared by the stream-correlated profiles |
| `session_subscribe.c` | Subscribe protocol, subscription pool, update answers |
| `session_publish.c` | PUBLISH-initiated subscriptions |
| `session_fetch.c` | FETCH, both roles |
| `session_fill.c`, `fill_owner.c`, `fill_input.c`, `fill_range.c` | Fills |
| `session_subgroup.c` | Outgoing subgroup data path |
| `session_receive.c` | Incoming data stream parser, attribution |
| `session_track_status.c` | TRACK_STATUS |
| `session_namespace.c`, `session_namespace_sub.c` | Namespace advertisement and namespace subscriptions |
| `session_subscribe_tracks.c` | SUBSCRIBE_TRACKS |
| `session_track_hist.c` | Per-track Largest Object history |
| `session_auth.c` | Token cache and shared AUTH_TOKEN processing |
| `request_stream.c`, `request_lifetime.c` | Shared request-stream engine and per-request lifetime |
| `transport_binding.c` | Transport binding validation and storage |
| `location_d22.c` | Semantic Location Filter to draft-22 wire form |
| `profile_d16.c`, `profile_d18.c`, `profile_d22.c`, `profile_registry.c` | Wire profiles and the version registry |
| `session_internal.h` | Private types, struct, shared declarations |

`core/src/wire/` holds the codecs (`control_d16.c`, `control_d18.c`,
`control_d22.c`, `messages_d22.c`, `params_d22.c`, integer encodings);
`core/src/bridge/` the transport bridge; `core/src/facade/` the publisher
and subscriber facades.

## Facades

The `moq_publisher_t` and `moq_subscriber_t` facades wrap session
event plumbing into track-oriented APIs.

### Publisher scope

A `moq_publisher_t` owns exactly one `moq_session_t`. On drafts 16 and 18
the session allows one subscription per track and role, so a second
same-track subscribe is rejected before the facade sees it. On draft-22 the
facade accepts further subscriptions to a track it serves
(`moq_session_supports_concurrent_subscriptions()`), each with its own alias.
Multiple subscribers to the same content across connections are handled by
one session and one facade per peer; the relay libraries coordinate across
sessions ([relay/README.md](../relay/README.md)).

The publisher facade serves a fill from the track's retained group
([publisher-retained-groups.md](publisher-retained-groups.md)) or fails it
on the wire; it never leaves a fill unanswered.

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
unaffected. A session created with `defer_subscription_updates` leaves the
answer to the application (`moq_session_accept_subscription_update`,
`moq_session_reject_subscription_update`); see
[conformance.md](conformance.md#request_update).

### Subscriber fills

The subscriber facade can attach a fill to a track subscription or an
update, polls fill content with `moq_sub_poll_fill` (a terminal item carries
the reason), and cancels one fill without touching the subscription. On a
session without fills the request is refused before anything is sent.
