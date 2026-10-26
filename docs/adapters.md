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
| `OPEN_UNI_CONTROL` / `SEND_UNI_CONTROL` | Open this side's unidirectional control channel and write to it (draft-18, draft-22) |
| `SEND_DATA` | Open a unidirectional stream (if needed) and write bytes |
| `RESET_DATA` | Reset the unidirectional stream, keeping its first `reliable_size` bytes where the transport can ([Reliable resets](#reliable-resets)) |
| `STOP_DATA` | Send a stop-sending signal for a data stream |
| `SEND_DATAGRAM` | Send a QUIC datagram |
| `CLOSE_SESSION` | Close the QUIC connection with the given error code |
| `OPEN_BIDI_STREAM` | Open a bidirectional stream and write initial bytes |
| `SEND_BIDI_STREAM` | Write bytes to an existing bidirectional stream |
| `CLOSE_BIDI_STREAM` | Send FIN on a bidirectional stream |
| `RESET_BIDI_STREAM` / `STOP_BIDI_STREAM` / `ABORT_BIDI_STREAM` | Reset our half, stop the peer's half, or both, of a request stream |
| `SET_DATA_PRIORITY` | Only after `moq_session_set_data_priority_updates(s, true)`: set a data stream's priority before its first write; `stream_ref` 0 sets the subscriber priority of every stream of `priority.owner` |

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
- **picoquic / pico_wt (h3zero)** cannot pause reads, so they keep the
  bytes themselves: a refused chunk is held at the front of the stream's
  buffer with later bytes queued behind it, and redelivered once the session
  can admit it.

Every bridge also requires `MOQ_TRANSPORT_CAP_HOLD_INPUT`: when the session
cannot admit a new data stream yet, the bridge refuses the chunk with
`MOQ_ERR_INPUT_NOT_CONSUMED` and the adapter owns and redelivers exactly that
chunk. The full contract is in [receive-admission.md](receive-admission.md).

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

## In-tree adapters

Every in-tree adapter implements the bridge's endpoint vtable. They differ
in transport, integration mode and how the MoQ version is chosen.

| Adapter | Transport | Attach mode | Managed mode | Version selection | Binds a draft-22 session | Reliable reset |
| --- | --- | --- | --- | --- | --- | --- |
| picoquic | raw QUIC | `moq_pq_conn_create` (`<moq/picoquic.h>`) | `moq_pq_threaded_t` (`<moq/picoquic_threaded.h>`), client and server | ALPN: with `alpn_list` a client creates the session once the handshake picked a version and a server selects per connection; without it, the single `moqt-16` | managed: yes, from `cfg.target`; attach: the caller | RESET_STREAM_AT when both sides advertised it |
| pico_wt | WebTransport over HTTP/3 (picoquic h3zero) | `moq_pico_wt_conn_create` (`<moq/pico_wt.h>`) | `moq_pico_wt_managed_t` (`<moq/pico_wt_managed.h>`), client and server | WebTransport protocol token from `wt_protocols`, in preference order | managed: yes (a server binds with the CONNECT's authority and path); attach: the caller | RESET_STREAM_AT when genuinely negotiated |
| mvfst | raw QUIC | `moq::mvfst::adapter` (`<moq/mvfst.hpp>`) | `moq_mvfst_managed_t` (`<moq/mvfst.h>`), client and server | Exact version: one ALPN; a multi-entry offer is `MOQ_ERR_UNSUPPORTED` | managed: yes, once the transport knows the peer's parameters; attach: the caller (`on_transport_ready`) | plain resets |
| MsQuic | raw QUIC | `moq_msquic_conn_*` (`<moq/msquic.h>`) | `moq_msquic_managed_t` (`<moq/msquic_managed.h>`), client and multi-connection server with lanes | Client exact-version; a server offers one ALPN or an ordered `versions` list and selects per connection | yes, in both modes, at CONNECTED from `cfg.target` | plain reset after the header is acknowledged |
| wtquic | WebTransport over HTTP/3 (MsQuic; Network.framework currently refused) | `moq_wtquic_conn_*` (`<moq/wtquic.h>`), requires a qualified receive contract | `moq_wtquic_msquic_managed_t`, client and server with lanes; `moq_wtquic_network_managed_t`, refuses at create | WebTransport protocol token, in preference order | managed MsQuic: yes (a server uses the accepted CONNECT target); Network: unavailable; attach: the caller | plain resets |
| proxygen | WebTransport | `moq::wt::Adapter` (`<moq/proxygen_wt.hpp>`) | `moq_proxygen_wt_managed_t` (`<moq/proxygen_wt_managed.h>`), client only | WebTransport protocol token | managed: yes; attach: the caller | plain resets |

The wtquic MsQuic server reads the accepted CONNECT's authority and path with
`wtq_session_connect_request_metadata()`, which the linked wtquic build must
provide. These are counted, connection-owned spans, not necessarily
NUL-terminated; they remain valid until the WT session is destroyed, including
on a retained terminal session. `moq_session_bind_transport()` copies the
target before the MoQ session starts. The Network.framework facade returns
`MOQ_ERR_UNSUPPORTED` from `moq_wtquic_network_managed_create()` for every
valid configuration, on every draft, before allocation or network startup
(its delivery-only receive policy is not admitted), so its binding path does
not run. The proxygen adapter's draft-22 binding
code has been syntax-checked but not built or run. Adapter-specific guides: [picoquic](../adapters/picoquic/README.md),
[MsQuic](../adapters/msquic/README.md), [wtquic](../adapters/wtquic/README.md),
[mvfst managed client](mvfst-managed-client.md),
[mvfst managed server](mvfst-managed-server.md), and the
[transport integration guide](transport-integration-guide.md) for choosing
one.

## Transport binding

A session whose profile requires a binding (draft-22:
`moq_session_binding_requirements()` returns `MOQ_BINDING_FACTS_ALL`) must
receive one through `moq_session_bind_transport()` before
`moq_session_start()` and before any byte reaches it. The binding
(`<moq/transport_binding.h>`) carries:

| Field | Content |
| --- | --- |
| `kind` | `MOQ_BINDING_RAW_QUIC` or `MOQ_BINDING_WEBTRANSPORT_H3` (`MOQ_BINDING_WEBTRANSPORT_H2` is refused) |
| `facts_known`, `facts_supported` | Handshake complete, local and peer QUIC DATAGRAM, datagram send and receive. A fact the stack cannot report is left unknown, never assumed. |
| `authority`, `path_query`, `target_present` | Raw-QUIC client: the URI's authority and path-abempty with query, sent as the SETUP's AUTHORITY and PATH. WebTransport, both sides: the authority and path the CONNECT named. Copied by the call. |
| `accept_uri`, `accept_uri_ctx` | Raw-QUIC server only: the policy asked about each client's AUTHORITY and PATH; it returns `MOQ_URI_ACCEPT`, `MOQ_URI_INVALID_AUTHORITY` or `MOQ_URI_INVALID_PATH`. |

A required fact not yet known is `MOQ_ERR_WOULD_BLOCK` (bind again later),
one known false is `MOQ_ERR_UNSUPPORTED`; every refusal leaves the session
unchanged. A managed facade refusing a binding fails the connection rather
than run an unbound session. Draft-16 and draft-18 require no facts and
accept the same binding optionally: the managed facades bind them whenever
they hold a target (a client's, or a WebTransport CONNECT's) or a raw-QUIC
server policy, so a raw-QUIC client's setup carries AUTHORITY and PATH on
every draft. An attach-mode host connecting to a moqt URI binds its client
session the same way. Facades that admit the configuration take the
target as a `moq_connection_target_t` config field (`target`); a raw-QUIC server
configured without `accept_uri` serves every authority and path. The service
endpoint derives the target from its URL and passes it to the facade it
creates.

### Deadline hooks for hosts that share a lane

The managed facades wake their network thread for the session's own
deadlines. A host with deadlines of its own (the relay's fill waits, the
service tier's catalog refresh) supplies them through `app_deadline_us` on
each facade's config. The MsQuic and wtquic-MsQuic facades run several lanes
and also take `app_lane_deadline_us(ctx, lane_index)`, so each lane wakes
only for its own deadlines. A lane pumps once for a given application
deadline instant: when a pump of that lane has run at or after the instant,
the same instant no longer wakes it, so a query that keeps returning an
instant already served leaves the lane waiting instead of spinning.

## Stream commitment

`MOQ_TRANSPORT_CAP_COMMIT_REPORT` declares that the adapter reports when every
byte through a local data stream's FIN has been acknowledged. The bridge retains
the stream mapping after an accepted FIN so a subgroup timeout can still reset
it until commitment or reset. Raw MsQuic reports this from native graceful send
shutdown, not buffer release or generic stream closure. WT/MsQuic enables the
capability only when
`wtq_session_send_commit_mode()` returns
`WTQ_SEND_COMMIT_LOCAL_UNI_ACK_FIN_RESETTABLE`; it opts each local UNI stream
in with `wtq_stream_enable_send_commit()` before its first send.

Reliable-prefix reset and datagram ops are separate capabilities. PicoQUIC,
PicoWT, mvfst and Proxygen do not yet report commitment, even though their
current selectors accept draft-22. That remains a qualification gap, not an
unsupported-version policy. See
[conformance status](conformance.md#delivery-timeouts-and-scheduling) for the
distinction between core/fake-adapter tests and native loopback tests.

## Reliable resets

Every data-stream reset the session originates carries `reliable_size`, the
length of the stream's own header, so the receiver can tell which
subscription, fetch or fill the reset ended (draft-22 §11.3.2; draft-16 and
draft-18 carry the same sentence). An adapter that can keep that prefix
declares `MOQ_TRANSPORT_CAP_RELIABLE_RESET` and implements the optional
`reset_stream_at` endpoint op. The bridge calls it only with
0 < `reliable_size` <= the bytes it handed the stream, after all of them were
accepted, so a header still retained under WOULD_BLOCK goes out first. The
op may answer `MOQ_TRANSPORT_DROPPED` when this connection cannot reset
reliably (reliable reset was not negotiated, or the stack cannot express the
size); the bridge then sends a plain RESET_STREAM, the fallback the protocol
allows (§1.4). Without the capability every reset is plain.

| Adapter | Declares the capability | Behaviour | Tests |
| --- | --- | --- | --- |
| picoquic | yes | RESET_STREAM_AT when both sides advertised the `reset_stream_at` transport parameter (the threaded facade advertises it); otherwise DROPPED and a plain reset. A prefix still in the adapter's send queue is kept and the frame issued once picoquic has taken it. | `pq_sim_reliable_reset` |
| pico_wt | yes | RESET_STREAM_AT when reliable reset was genuinely negotiated. When the managed client accepts a peer that omits the parameter, the reset is plain. | `pico_wt_reliable_reset`, `pico_wt_managed_no_reset_stream_at` |
| MsQuic | yes | Offers `ReliableResetEnabled`, but never sends RESET_STREAM_AT: it waits until the peer has acknowledged the reliable prefix, then resets plainly. A cancelled send, a closing connection, or nothing left in flight resets plainly at once. | `msquic_reliable_reset`, `msquic_stress` |
| mvfst | yes | Does not advertise reliable stream reset, so every reset is plain: the mvfst revision the adapter builds against closes the connection with PROTOCOL_VIOLATION when it receives RESET_STREAM_AT. A caller that enables `advertisedReliableResetStreamSupport` on an mvfst that accepts the frame gets reliable resets through the same op. | `mvfst_reset_header_d16`, `mvfst_reset_header_d22` |
| wtquic | no | Plain resets; wtquic has no reliable-size reset. | |
| proxygen | no | Plain resets; proxygen's API has no reliable-size reset. | |

Why MsQuic waits instead of sending RESET_STREAM_AT: with MsQuic 2.5.9 a
receiver that processes RESET_STREAM_AT before its application has taken
the reliable bytes delivers them with a clean FIN and drops the reset, and a
sender whose RESET_STREAM_AT is still unacknowledged when the connection
ends never completes the stream, so `MsQuicRegistrationClose` waits forever.
The acknowledged-then-plain approach keeps the header only for a receiver
that delivers data to its application before acknowledging it, as MsQuic
does; it is not a protocol guarantee for every peer. Over mvfst, wtquic and
proxygen a reset can discard a header the peer has not read yet.

A receiver learns nothing about reliable sizes from any in-tree stack, so a
received data stream that the session refused for want of a receive slot
and that is then reset cannot be attributed to its subscription.

Session and bridge behaviour are pinned by `reset_reliable_size` and
`bridge_reliable_reset` on drafts 16, 18 and 22.

## Shared send admission

An adapter whose send queue has one byte budget for the whole connection,
and whose `write`/`write_payload` refuse only when that budget is spent,
declares `MOQ_TRANSPORT_CAP_SHARED_ADMISSION`. Such a refusal is not about
its stream: no write adding bytes to any stream can be accepted until the
transport takes queued bytes, or until a reset drops some. With the
capability, the bridge stops offering writes that add bytes for the rest of
that hand-off of queued data; FIN-only writes, which the adapter may still
mark on bytes it holds, and resets still go, and a reset it accepts there
reopens the hand-off. Without it, a refused write holds back only its own
stream and the bridge tries the next ready stream, as an adapter with a
budget per stream needs. Open-stream credit, resets, control writes and
`close_transport` keep their own results either way.

| Adapter | Declares the capability | Tests |
| --- | --- | --- |
| picoquic | yes: one queue cap per connection (`send_queue_cap_bytes`) | `pq_priority_admission` (the queue's refusals), `pq_sim_shared_admission` (advertised and withdrawn over the simulator) |
| pico_wt | yes: the same queue | `pq_priority_admission`, `pico_wt_shared_admission` |
| MsQuic, mvfst, wtquic, proxygen | no | |

The bridge's side is pinned by `bridge_shared_admission` (the model queue
with and without the capability, FIN-only writes, resets and expiry while
the budget is spent) and `bridge_shared_admission_suite` (the scheduling
suite with the capability advertised, leaving out the rows that refuse per
stream).

## Porting Checklist

The recommended path for a new adapter is to implement the shared
transport bridge's endpoint-ops vtable rather than re-deriving the
contract by hand — all in-tree adapters (picoquic, pico_wt, mvfst, MsQuic,
wtquic, proxygen WT) do this, so the stream mapping, backpressure retry, and
FIN/reset/tombstone lifecycle come for free. The steps below describe
the underlying contract the bridge implements (useful whether you use
the bridge or wire the session directly).

To build an adapter for another QUIC stack:

1. **Map stream refs to stream IDs.** Maintain a bidirectional
   mapping between `moq_stream_ref_t` values and transport-level
   stream identifiers.

2. **Implement the service loop.** Poll session actions and translate
   each to the transport's write/reset/stop/close API.

3. **Route inbound events.** From the transport's callback or event
   loop, call the appropriate `moq_session_on_*` function.

4. **Bind, then start.** When `moq_session_binding_requirements()` is
   non-zero (draft-22), bind the session with the connection's facts and
   target before starting it ([Transport binding](#transport-binding)); on
   draft-16 and draft-18 a client connecting to a moqt URI binds its target
   the same way, facts optional.
   On draft-16 a client calls `moq_session_start` once the QUIC connection
   and adapter are ready and a server waits for the peer's setup message;
   on draft-18 and draft-22 both sides call `moq_session_start`, since each
   sends its SETUP without waiting. Call `moq_session_on_transport_close`
   on connection loss.

5. **Integrate timers.** Check `moq_session_next_deadline_us` and
   arm the transport's timer facility.

6. **Respect borrow lifetimes.** Consume action data before cleanup.
   Do not retain pointers past `moq_action_cleanup`.

7. **Thread safety.** The session is not thread-safe. All session
   and adapter calls must be serialized.
