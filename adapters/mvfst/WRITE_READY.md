# Write-Ready Integration (Deferred)

## Current State

Bridge pending writes are retried when `adapter::service()` is
called. Managed mode polls every 5ms via `loopOnce(EVLOOP_NONBLOCK)`.
Attach-mode callers must call `service()` themselves, typically on
each EventBase loop iteration.

`bridge::has_pending_output()` reports whether the bridge FIFO has
pending items. Callers can use this to decide whether to schedule
a retry.

## Why Deferred

mvfst provides connection-level write readiness via
`notifyPendingWriteOnConnection(ConnectionWriteCallback*)`, but:

- The callback is a raw pointer stored on the transport.
- It is cleared only when `onConnectionWriteReady` or
  `onConnectionWriteError` fires.
- There is no unregister API. `notifyPendingWriteOnConnection`
  rejects a second call while a callback is already registered
  (returns `INVALID_WRITE_CALLBACK`). Passing `nullptr` does not
  clear an existing registration.

This means the adapter cannot safely register for write readiness
in attach mode, where the adapter borrows the socket and must
allow destruction while the socket continues to live. If the
adapter is destroyed while mvfst holds a raw pointer to it, any
subsequent write-ready notification is use-after-free.

The adapter's attach-mode contract guarantees that destruction
clears all callbacks it can clear (connection setup, connection,
read, datagram). Write-ready cannot be included in this guarantee
because mvfst does not expose an unregister path.

## Attach-Mode Caller Guidance

Attach-mode callers should call `adapter::service()`:
- After each EventBase loop iteration
- Or on a timer if not driving the EventBase directly
- Check `is_fatal()` after each service call

This is correct and complete. Polling is less responsive than
callback-driven retry, but it is safe under the borrowed-socket
lifetime model.

## Managed Mode

Managed mode polls every 5ms and owns both the transport and the
adapter, so the lifetime concern does not apply. A future managed-
only optimization could register for write-ready since the managed
thread controls both lifetimes.

## Post-v1 Gaps

### Bidi STOP_SENDING (ignored)

Uni-stream STOP_SENDING is handled via `moq_session_on_data_stop`.

Bidi STOP_SENDING (both local-originated and peer-originated) is
silently ignored. The core session has no `moq_session_on_bidi_stop`
API. The expected behavior would be to signal the session to stop
sending on that bidi stream's send side.

This affects namespace subscription bidi streams where the peer
may send STOP_SENDING to cancel a response in flight. Revisit when
adding relay interop with moqx/reference implementations.

The shared transport bridge (`moq_transport_bridge_t`) routes bidi
STOP_SENDING through `on_peer_stop_sending()` for local-origin uni
streams only. Bidi STOP_SENDING is a v1 limitation.

## Future Options

1. **Managed-only write-ready:** Register in managed mode where
   lifetime is controlled. Attach mode continues to poll.
2. **Per-stream write callbacks:** `notifyPendingWriteOnStream` +
   `unregisterStreamWriteCallback` exists in mvfst and has an
   unregister path. Could be used instead of connection-level,
   but requires per-stream tracking in the bridge.
3. **Upstream mvfst change:** Add an unregister API for
   connection-level write callbacks.
