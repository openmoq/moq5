# API Boundaries

LibMoQ exposes three header tiers. Each tier defines what a consumer can
depend on and what they should not see.

## Application API — `<moq/moq.h>`

The stable umbrella for session-level embedders: media servers, video
rooms, CLI tools, downstream integrations, language bindings.

Includes: `export.h`, `version.h`, `types.h`, `rcbuf.h`, `session.h`.

### What belongs here

Semantic MoQ concepts:

- Sessions, perspectives, configuration
- Subscriptions, announcements, subgroups (handles)
- Actions (SEND_CONTROL, SEND_DATA, RESET_DATA, STOP_DATA, CLOSE_SESSION)
- Events (SETUP_COMPLETE, SESSION_CLOSED, SUBSCRIBE_REQUEST/OK/ERROR,
  REQUEST_READY, OBJECT_RECEIVED, NAMESPACE_PUBLISHED/DONE/
  REJECTED/ACCEPTED/CANCELLED)
- Owned resource cleanup (moq_action_cleanup, moq_event_cleanup)
- Backpressure signals, retry conventions, ABI mismatch errors
- Result codes, byte spans, allocator vtable
- Refcounted buffers (moq_rcbuf_t)
- Object status (NORMAL, END_OF_GROUP, END_OF_TRACK)
- Virtual time (`now_us` on every advancing call)

### What does not belong here

Wire encodings, protocol-version-specific names, codec internals:

- Wire integer encodings, buffer cursors, key-value-pair structures
- Message type codepoints (0x03, 0x04, etc.)
- Setup and message parameter type codes
- Draft-specific struct names (`moq_d16_*`)

These terms are enforced by `scripts/check_api_boundary.sh`, which runs
as a CTest.

## Codec/Tooling API — `<moq/codec.h>`

For adapters, fuzzers, test harnesses, relay internals, protocol
analyzers, and interop tools that need to construct or inspect raw wire
messages.

Includes `moq.h` plus: `wire.h`, `buf.h`, `kvp.h`, `control.h`.

## Simulation API — `<moq/sim.h>`

For deterministic testing, conformance harnesses, and seed-replayable
traces.

Includes `moq.h` (not `codec.h`).

## Publisher Facade — `<moq/publisher.h>`

A thin session-attached coordinator for publisher-side track, subscription,
and subgroup bookkeeping. Not included by `<moq/moq.h>` — include directly.

Includes `<moq/session.h>`. Lives in `moq-core` (not a separate library).

The facade never polls the session. The caller remains the single consumer
of `poll_events` and `poll_actions`, forwarding relevant events to the
facade via `moq_pub_handle_event`.

Subscription accept policies: `REJECT_ALL`, `ACCEPT_ALL`, or `CALLBACK`.
In CALLBACK mode, the callback receives borrowed subscribe request data
(namespace, name, tokens, filter) valid for the callback duration only.
The callback must not call advancing session or facade APIs.

## Examples Policy

| Example type | Allowed includes |
|---|---|
| Application examples | `<moq/moq.h>` and/or `<moq/publisher.h>` |
| Simulation examples | `<moq/moq.h>` + `<moq/sim.h>` |
| Codec/adapter/tools | `<moq/codec.h>` (includes everything) |

Application examples must not import codec helpers. If an example needs
raw bytes for error injection, use a fixed byte literal with a comment.

## Naming Policy

Stable application APIs use semantic MoQ concepts, not wire encodings:

| Allowed (app API) | Not allowed (wire internal) |
|---|---|
| `moq_session_subscribe` | `moq_d16_subscribe_encode` |
| `moq_subscription_t` | wire parameter types |
| `MOQ_EVENT_SUBSCRIBE_OK` | `MOQ_D16_SUBSCRIBE_OK` |
| `moq_announcement_t` | `moq_d16_publish_namespace_t` |
| `moq_bytes_t` | `moq_reader_t` |

If a name references wire encoding, a message codepoint, or a codec
data structure, it belongs behind `<moq/codec.h>`.

## Profile Boundary — Internal

Generic session core files (`session.c`, `session_subscribe.c`,
`session_fetch.c`, `session_publish.c`, `session_track_status.c`,
`session_namespace.c`, `session_namespace_sub.c`, `session_receive.c`,
`session_subgroup.c`, `session_setup.c`, `session_auth.c`) and internal
helpers used by session core (`validate.h`) must not reference any
draft-specific wire vocabulary or include `moq/control.h`. All
D16-specific logic lives in `profile_d16.c`.

The profile boundary covers:

- Wire codec functions and constants (D16-prefixed)  <!-- api-boundary-exempt -->
- Wire types: KVP entries, control envelopes  <!-- api-boundary-exempt -->
- Wire parameter and auth token constants  <!-- api-boundary-exempt -->
- Control framing decode and includes  <!-- api-boundary-exempt -->
- Request admission fields (request ID sequence, blocked state)

Session core communicates with the profile through `moq_profile_ops_t`:

- Request admission: `prepare_request` / `commit_request` / `abort_request`
- Inbound validation: `validate_inbound_request` / `commit_inbound_request`
- Capacity: `grant_capacity` / `peer_request_capacity` / `local_request_capacity`
- Track alias: `next_track_alias` / `advance_track_alias`
- Control framing: `process_control_data`
- Message decode: `decode_subgroup_header`, `decode_object_header`,
  `decode_ns_sub_request`, `decode_ns_sub_response`
- Message encode: `encode_subscribe`, `encode_subscribe_ok`,
  `encode_request_ok`, `encode_request_error`, `encode_unsubscribe`,
  `encode_goaway`, `encode_publish_namespace`, `encode_namespace_msg`,
  `encode_subgroup_header`, `encode_object_header`

`session_internal.h` does not include `moq/control.h` — session core
files cannot access D16 wire types at compile time.

Enforced by `scripts/check_profile_boundary.sh`, which runs as a CTest.
