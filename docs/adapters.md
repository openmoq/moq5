# Adapter Guide

A MoQ adapter bridges a `moq_session_t` to a real QUIC transport.
The session is a sans-I/O state machine — it never reads or writes
the network directly. The adapter translates between session actions
and transport operations.

## Adapter Contract

### Session Actions → Transport Operations

After each advancing session call, the adapter polls actions and
performs the corresponding transport writes:

| Action | Transport Operation |
|--------|-------------------|
| `SEND_CONTROL` | Write bytes to the control channel | <!-- api-boundary-exempt -->
| `SEND_DATA` | Open a unidirectional stream (if needed) and write bytes |
| `RESET_DATA` | Reset the unidirectional stream |
| `STOP_DATA` | Send a stop-sending signal for a data stream |
| `SEND_DATAGRAM` | Send a QUIC datagram |
| `CLOSE_SESSION` | Close the QUIC connection with the given error code |
| `OPEN_BIDI_STREAM` | Open a bidirectional stream and write initial bytes |
| `SEND_BIDI_STREAM` | Write bytes to an existing bidirectional stream |
| `CLOSE_BIDI_STREAM` | Send FIN on a bidirectional stream |

Each action carries a `stream_ref` that the adapter maps to a
transport-level stream ID. The mapping is adapter-owned; the session
treats stream refs as opaque values.

### Transport Callbacks → Session Input

When the transport receives data, the adapter calls the corresponding
session input function:

| Transport Event | Session Input |
|-----------------|--------------|
| Control channel bytes | `moq_session_on_control_bytes` | <!-- api-boundary-exempt -->
| Unidirectional stream bytes | `moq_session_on_data_bytes` or `moq_session_on_data_rcbuf` |
| Unidirectional stream reset | `moq_session_on_data_reset` |
| Unidirectional stream stop | `moq_session_on_data_stop` |
| Datagram received | `moq_session_on_datagram` |
| Bidirectional stream bytes | `moq_session_on_bidi_stream_bytes` |
| Bidirectional stream reset | `moq_session_on_bidi_stream_reset` |
| Connection closed | `moq_session_on_transport_close` |

### Ownership and Lifetime

- **Action data is borrowed.** Action byte pointers (`send_control.data`,
  `send_bidi_stream.data`, etc.) borrow from the session's internal
  send buffer. The adapter must consume (copy or write) the data before
  calling `moq_action_cleanup` or the next `moq_session_poll_actions`.

- **SEND_DATA has header + payload.** The `send_data` action carries a
  short inline `header` (borrowed) and an `moq_rcbuf_t *payload` (owned
  reference). The adapter writes the header bytes first, then the
  payload data. `moq_action_cleanup` releases the payload ref.

- **rcbuf payloads on receive.** `moq_session_on_data_rcbuf` borrows
  the caller's rcbuf reference — the caller keeps its ref and may
  decref after the call. The session increfs internally if it needs to
  retain the data. `moq_session_on_data_bytes` copies bytes internally.

- **Action cleanup is mandatory.** After processing each action, call
  `moq_action_cleanup`. This releases owned references; do not retain
  borrowed action pointers after cleanup or a subsequent poll/advance.

### Batching Pattern

The typical adapter service loop:

```
moq_action_t acts[16];
size_t n;
while ((n = moq_session_poll_actions(session, acts, 16)) > 0) {
    for (size_t i = 0; i < n; i++) {
        switch (acts[i].kind) {
        case MOQ_ACTION_SEND_CONTROL:
            transport_write_control(acts[i].u.send_control.data,
                                    acts[i].u.send_control.len);
            break;
        case MOQ_ACTION_SEND_DATA:
            transport_write_stream(acts[i].u.send_data.stream_ref,
                                   acts[i].u.send_data.header,
                                   acts[i].u.send_data.header_len);
            transport_write_stream(acts[i].u.send_data.stream_ref,
                                   moq_rcbuf_data(acts[i].u.send_data.payload),
                                   moq_rcbuf_len(acts[i].u.send_data.payload));
            break;
        /* ... other action kinds ... */
        }
        moq_action_cleanup(&acts[i]);
    }
}
```

### WOULD_BLOCK Policy

Session APIs return `MOQ_ERR_WOULD_BLOCK` when internal queues are
full. The adapter should drain actions (poll + write + cleanup), then
retry the blocked call. The session never blocks on transport writes
— that is the adapter's responsibility.

The shared transport bridge (`moq_transport_bridge_t`) owns this
contract so adapters do not each reimplement it: it retains blocked
outbound actions in a FIFO pending queue and retries them on the next
`service()`, and it tracks per-stream inbound retry state. For inbound
data, the bridge's preferred path is that the adapter **pauses reads on
a stream that still has pending inbound work** (check
`moq_transport_bridge_stream_has_pending`) and resumes once pending
clears. Pausing keeps the retry cheap: the session replays its own
retained input via an empty-data retry rather than re-buffering.

Pausing is a recommendation, not a hard invariant. If an adapter cannot
pause and delivers more bytes while a stream is still pending, the bridge
handles it without tearing the connection down: it appends the bytes to
the session's retained input when the session still owns the stream, or
discards the remainder (until FIN) if the session has dropped or refused
the stream (e.g. its subscription went away). Inbound backpressure is
never, by itself, fatal — only genuinely malformed stream bytes are.

How an adapter satisfies the recommendation depends on its transport:

- **mvfst** soft-pauses the affected stream (`pauseRead`/`resumeRead`)
  and resumes once the bridge clears pending. (`setReadCallback(nullptr)`
  is *not* used for temporary pause — it is terminal in mvfst.)
- **proxygen WebTransport** pauses by not issuing the next read on its
  async read loop and re-issues it once pending clears; it re-checks
  pending *after* servicing, so a synchronously-cleared block does not
  strand the stream.
- **picoquic / pico_wt (h3zero)** cannot pause reads, so it simply keeps
  delivering inbound bytes; the bridge absorbs them per the rule above
  (append while owned, discard once dropped/refused) without faulting the
  connection on backpressure alone.

### Fatal Transport Errors

On unrecoverable transport failure, call
`moq_session_on_transport_close(s, error_code, now_us)`. This
transitions the session to CLOSED and emits a SESSION_CLOSED event.
Do not call other session input functions after transport close.

### Timer Integration

After each advancing call, check `moq_session_next_deadline_us`.
If it returns a finite value, arm a timer. When the timer fires,
call `moq_session_tick(s, now_us)` to process internal deadlines
(delivery timeouts, GOAWAY drain timeouts).

## Picoquic Adapter

The `moq-adapter-picoquic` library (`adapters/picoquic/`) implements
this contract for the picoquic QUIC stack:

- `moq_pq_conn_create` binds a session to a picoquic connection
- `moq_pq_service` implements the batching loop
- `moq_pq_callback` routes picoquic stream/datagram events to
  session input functions
- Stream ref → stream ID mapping is maintained internally

See `examples/picoquic/` for working publisher and subscriber
applications using the adapter.

## Porting Checklist

The recommended path for a new adapter is to implement the shared
transport bridge's endpoint-ops vtable rather than re-deriving the
contract by hand — all in-tree adapters (picoquic, mvfst, pico_wt,
proxygen WT) do this, so the stream mapping, backpressure retry, and
FIN/reset/tombstone lifecycle come for free. The steps below describe
the underlying contract the bridge implements (useful whether you use
the bridge or wire the session directly).

To build an adapter for another QUIC stack (quiche, msquic, etc.):

1. **Map stream refs to stream IDs.** Maintain a bidirectional
   mapping between `moq_stream_ref_t` values and transport-level
   stream identifiers.

2. **Implement the service loop.** Poll session actions and translate
   each to the transport's write/reset/stop/close API.

3. **Route inbound events.** From the transport's callback or event
   loop, call the appropriate `moq_session_on_*` function.

4. **Handle connection lifecycle.** For clients, call
   `moq_session_start` to initiate MoQ setup once the QUIC
   connection and adapter are ready. Servers wait for the peer's
   setup message (delivered via `on_control_bytes`). Call
   `moq_session_on_transport_close` on connection loss.

5. **Integrate timers.** Check `moq_session_next_deadline_us` and
   arm the transport's timer facility.

6. **Respect borrow lifetimes.** Consume action data before cleanup.
   Do not retain pointers past `moq_action_cleanup`.

7. **Thread safety.** The session is not thread-safe. All session
   and adapter calls must be serialized.
