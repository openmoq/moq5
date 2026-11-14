# Header preambles

The leading comment of each header this crate binds, verbatim: what a header states for all of its declarations at once rather than for one of them (rcbuf's shard and overflow contract among them), which clang attaches to no single item.

## moq/types.h

(no leading comment)

## moq/rcbuf.h

```text
Refcounted byte buffer for zero-copy relay forwarding.

Shard / threading contract: refcount operations (incref/decref) are
NOT atomic. All references to a given rcbuf must stay within one
shard — a single executor-affinity domain (a thread, executor, or run
loop). A shard is not pinned to one thread: it may move between threads
provided exactly one thread acts for it at a time, the handoff carries a
happens-before edge, and every rcbuf it holds moves with it (a session's
buffers move with the session). Sharing rcbuf ownership across shards via
incref/decref is NOT supported by libmoq's engine model. To hand bytes to
another shard,
make an independent copy with moq_rcbuf_clone() using an allocator
valid for the destination shard. (Legacy/manual external
synchronization may protect raw calls in single-buffer use, but it is
not the shard model and must not be used by engine/adapter/fan-out
code to share ownership across shards.)

Overflow protection: incref aborts if the refcount reaches
UINT32_MAX. This is a hard safety trap, not a recoverable error.

The allocator vtable is copied into the rcbuf at creation so the
last decref frees with the correct allocator regardless of which
session holds the final reference.
```

## moq/version.h

(no leading comment)

## moq/location.h

(no leading comment)

## moq/fill.h

(no leading comment)

## moq/cluster.h

(no leading comment)

## moq/session.h

```text
Sans-I/O session API.

The session is a deterministic state machine: same inputs + same time
produce identical outputs. No sockets, no threads, no wall clock
reads, no global mutable state.

Public input functions normalize into internal tagged records.
Outputs are pulled via poll_actions (adapter I/O) and poll_events
(application notifications).

This header is included by moq/moq.h (the stable umbrella).

-- ABI policy --

Output records (moq_action_t, moq_event_t):
  - Append-only: new fields added to the end of per-kind structs.
  - Kind enum values are never renumbered or removed.
  - Unknown kinds must be ignored/logged by the caller.
  - Reserved inline capacity absorbs new variants without changing
    sizeof. If a future variant exceeds the reserve, a SONAME bump
    is required.
  - The size-aware _ex poll functions are the ABI safety boundary.
    The library writes min(library_sizeof, caller_element_size) per
    element; callers compiled against older headers are safe.

Borrowed pointers:
  - Pointers in polled records are valid until the next advancing
    call on the same session. Advancing calls are: start,
    on_control_bytes, process_pending, tick, on_data_bytes,
    on_data_reset, on_data_stop, subscribe, accept_subscribe,
    reject_subscribe, unsubscribe, open_subgroup, write_object,
    begin_object, write_object_data, end_object,
    close_subgroup, reset_subgroup, goaway, close.
    publish_namespace, publish_namespace_done, accept_namespace,
    reject_namespace, cancel_namespace, grant_request_capacity,
    subscribe_namespace, on_bidi_stream_bytes, accept_ns_sub,
    reject_ns_sub, send_namespace, send_namespace_done,
    cancel_namespace_sub, on_bidi_stream_reset,
    fetch, accept_fetch, reject_fetch, fetch_cancel,
    write_fetch_object, write_fetch_range, end_fetch.
    Resolved tokens (moq_resolved_token_t) in SETUP_COMPLETE,
    SUBSCRIBE_REQUEST, and NAMESPACE_PUBLISHED are BORROWED from
    output scratch and follow the same borrow epoch.
    The borrow_epoch field, stamped at poll time, can be checked
    with moq_session_borrow_valid().

Terminal close:
  - A fatal close supersedes all previously queued but undrained
    actions and events. After close, the queues contain only the
    CLOSE_SESSION action and SESSION_CLOSED event. This is by
    design: stale outputs from a dead session are not meaningful.
```

## moq/transport_binding.h

```text
Transport binding: the facts about the underlying connection that a wire
profile may need before its first message, told to the session once.

The session core is sans-I/O and learns nothing about its transport on
its own. Every profile's setup depends on how the session is carried --
natively over QUIC or inside a WebTransport session: a raw-QUIC client
names its connection target (the URI authority, and the path with any
query), nothing else does, and a server refuses a target named over
WebTransport or one its URI policy does not serve. A profile that cannot
start without these facts declares them as required
(moq_session_binding_requirements(), nonzero for draft-22); the transport
host supplies them through moq_session_bind_transport() before the session
starts. A profile without requirements (draft-16, draft-18) accepts the
same binding optionally: bound, it behaves as above; never bound, it starts
and emits its setup as before, naming no target. Bound or not, a target
value outside URI syntax that a server receives closes the session.
```

## moq/connection_target.h

```text
Connection target: the immutable URI identity a managed transport host
binds a session with. It carries no transport evidence -- a host that
owns the connection supplies the handshake and datagram facts itself and
copies these fields into the moq_transport_binding_t it binds.
```

## moq/publisher.h

(no leading comment)

## moq/subscriber.h

```text
Subscriber facade.

Manages track subscriptions and object delivery for a subscriber.
Hides session event plumbing; the caller uses subscribe + tick + poll.

Event ownership: the facade owns session events. The caller must
NOT also poll session events. Call moq_sub_tick() to process events
and moq_sub_poll_object() to drain received objects.

Supports whole-object delivery, streaming chunks, track status
queries, fetch, and fills (draft-22 section 3.4: past objects a
subscription asks for on SUBSCRIBE or on a later update, delivered on
fill fetch streams and polled with moq_sub_poll_fill).

Thread safety: none. All calls serialized on the session's thread.
```
