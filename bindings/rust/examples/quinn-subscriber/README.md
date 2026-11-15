# moq5-quinn-subscriber

A bare MoQ subscriber over [quinn](https://github.com/quinn-rs/quinn).

`moq5-core` gives you the MoQ session with no I/O in it: bytes and time go
in, actions and events come out. Something has to own the transport and
drive that session. This example is the smallest such host we could write
over a real QUIC stack without cutting a corner of the host contract.

It connects over raw QUIC, completes the MoQ setup, subscribes to one track
and prints each object it receives.

```
moq5-quinn-subscriber --url moqt://HOST:PORT[/PATH] --ca ROOTS.pem \
    --namespace A/B --track NAME [--draft 18|22] [--objects N] [--tight]
```

```
connected 127.0.0.1:4443 moqt-22
setup complete
subscribed alias 1
object 0 0 32 bytes
object 0 1 32 bytes
done status 6 streams 1
closed 0
held kept 0 refused 0 owed 0
```

- `--ca` names the trust roots, a PEM file. The server certificate is
  verified the ordinary way, so it needs a subject alternative name for the
  host in the URL.
- `--draft` is 22 by default. The ALPN is `moqt-22` or `moqt-18`.
- Without `--objects`, it reads until the publisher completes the
  subscription (`done`). With `--objects N`, it unsubscribes after the Nth
  object; if the publisher had already completed the subscription, the
  session refuses the unsubscribe and the completion is printed instead.
- After an unsubscribe the session stops the streams of the subscription
  that are still open. Each is printed as `stopping STREAM`, and then as
  `retired STREAM` once the session has been told that nothing more of it
  will come, or as `ended STREAM` if its own FIN or reset had been read
  first.
- A subgroup whose stream the peer reset is printed as
  `subgroup GROUP SUBGROUP reset CODE`. The objects it had not carried yet
  are lost; the subscription goes on.
- `--tight` gives the session an event queue of two, so that it has to
  keep input it cannot finish at once. The last line counts how often the
  host had to hold something.
- The line before the last says how the session ended: `closed CODE` when
  this side closed it, `peer closed CODE` when the peer's application did,
  and `connection lost: ...` when the connection ended any other way (a
  timeout, a reset, a transport error). A lost connection carries no
  session code, and none is made up for it.
- It exits 0 when the publisher completed the subscription or this side
  unsubscribed, and the session was then closed with code 0. It exits 1
  for everything else, a lost connection included, whatever had arrived.

## Building

It is a workspace of its own, so its dependencies never reach the binding
crates' lock file. It needs what `moq5-core` needs: an installed libmoq SDK
with `libmoq-core.pc` (`MOQ_PREFIX=<prefix>`) and libclang for bindgen. See
[`moq5-sys`](../../moq5-sys).

```
MOQ_PREFIX=<prefix> cargo run --manifest-path bindings/rust/examples/quinn-subscriber/Cargo.toml -- \
    --url moqt://relay.example:4443 --ca roots.pem --namespace live/demo --track video
```

## How it is put together

Three files, with the tests of the session task in a fourth,
`src/tests.rs`.

- `src/main.rs`: the command line, the TLS and QUIC configuration, the
  transport binding the session is told about, and the tasks' start and
  end.
- `src/host.rs`: the one task that owns the `Session`. It feeds it, polls
  its actions and events, ticks it, and settles who holds every read.
- `src/transport.rs`: the quinn tasks. One accepts the peer's streams, one
  reads each stream, one writes and closes. None of them calls the
  session.

What the host does for the session, and where:

| The contract | Here |
|---|---|
| Route every peer unidirectional stream by its leading bytes; never parse a stream type | `Host::read`, with `Session::route_peer_uni` |
| A read the session kept is not delivered again: resume it with the empty call | `Reading::kept`, `Host::retry` |
| A read the session did not admit stays with the host, whole, and is offered again | `Reading::refused`, `Host::retry` |
| A peer signal the session still owes is delivered again | `Host::owed`, `Host::retry` |
| Bound what is held | one read per stream (the reading task waits), a stream limit on the connection, a bounded queue to the writer |
| Output reaches the transport in poll order | one writer task, one queue |
| The session's close reaches the transport whatever the transport is making wait | the close has a path of its own to the writer, and ends an open or a write that waits for the peer's credit: `Host::write_out`, `write_output` in `transport.rs` |
| Stop a stream when the session says so, then report that nothing more will come; an end of the stream that was read first is fed instead | `Host::stop_reading`, `Host::retire`, `Host::ended` |
| A peer's reset and a peer's stop are different inputs, each reported when it comes | `read_stream` and `watch_stop` in `transport.rs`; `Host::take` |
| The peer ending a control stream ends the session | `Session::on_control_stream_ended` |
| Time comes from the host | `Host::tick`, `Session::next_deadline` |
| The session is told how the transport ended | `Ended` in `transport.rs`, `Input::Closed` in `Host::take` |

## What it leaves out

- Publishing. It sends no objects and no datagrams, and the writer refuses
  those actions.
- Requests from the peer. The connection allows the peer no bidirectional
  streams.
- Draft 16, whose control stream is bidirectional.
- WebTransport.
- A drain before the close. The session's close closes the connection at
  once, and QUIC does not promise delivery of what was written just before.
- The tagged output mode, in which the host reports stream opens and
  accepted FINs and resets become a transaction. This host polls untagged
  and writes in poll order.

[`moq5-core/examples/transport_host`](../../moq5-core/examples/transport_host)
is the complete reference host: both roles, every draft, the tagged mode,
over an in-memory transport.

## Tests

`cargo test` runs the tests that need no relay:

- `src/tests.rs`: how the session task ends, with the test as its
  transport and a second session as the publisher. The close while the
  writer's queue is full; an unsubscribe settled to the session's own
  close; and each way a connection can end while that is still being
  settled.
- `tests/endings.rs`: the subscriber against a QUIC server that speaks no
  MoQ. The server gives no credit for a write, or for an open, and then
  sends an unknown stream type: the close must still reach it. Or it
  closes the connection, or goes silent. These need `openssl` on the path.
- `tests/relay.rs`, its last part: a stage whose relay or publisher fails
  to start leaves no process behind.

`tests/relay.rs` also runs the subscriber against a `moq5-relay` on
loopback, over real QUIC, with a native publisher behind the relay. Those
rows are ignored by default; `MOQ_RUST_QUINN_EXAMPLE=ON` registers them,
with the tests above, as the `rust_quinn_example` ctest row (see
[the bindings README](../../README.md)).
