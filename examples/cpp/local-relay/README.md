# local-relay

A tiny MoQ relay built on libmoq's **low-level `moq::session` event/action
API**, the same layer a moxygen-derived relay drives. It exists to show the
exact way a relay host should use libmoq, with **no third-party transport
stack** so the ownership pattern is the star rather than socket/TLS noise.

## The ownership boundary (the part to copy)

| The host (this example) owns | libmoq owns |
|---|---|
| the set of sessions (one per connection) | `moq::session` protocol state |
| the `track → subscribers` routing map | actions / events |
| the fan-out loop | `rcbuf` (`moq::buffer`) lifetime |
| per-recipient backpressure (retry on `would_block`) | the transport adapter |

libmoq provides **no** relay framework: no fan-out API, no recipient group,
no cache / namespace tree / subscription registry / scheduler. All of that is
plain host code here.

## What it does

Single thread, one namespace/track, one group:

1. Three subscribers subscribe to the relay; the relay accepts each and
   records it in a host-owned `track → subscribers` map.
2. On first demand, the relay subscribes **upstream** to the publisher
   (relays subscribe on demand).
3. The publisher writes one object; the relay receives it upstream and runs
   the **host fan-out loop**, writing the one shared payload to each
   subscriber via the ordinary per-session publish calls.
4. One subscriber is modeled as backed-up: its write returns
   `errc::would_block`. The relay stashes the object in **its own** pending
   state (a retained ref to the upstream payload; a real relay never has
   the publisher's original buffer) and **keeps going**. The block never
   aborts the fan-out or drops the other subscribers. Once that subscriber's
   queue drains, the relay retries from its pending state and clears it only
   after the write succeeds.

## Transport seam

The only transport stand-in in this example is `pump()`, which moves a
session's queued actions to its peer in-process. It is deliberately the *one*
thing a real host replaces; everything above it (the relay core) carries
over unchanged.

`pump()` is intentionally minimal. It is suitable for this one-object teaching
example, but it is **not** a deep/backpressure-safe transport harness. In
particular, `moq_session_on_data_bytes()` / `moq::session::on_data_bytes()`
can return `WOULD_BLOCK` when the receiver's event/action queue is full. A
real transport or stress harness must stop feeding that stream/link, drain the
peer, then retry with an empty data call before delivering more bytes:

```cpp
auto r = to.on_data_bytes(ref, bytes, fin, now);
if (!r.ok() && r.error().code() == moq::errc::would_block) {
    // Keep the blocked bytes/stream in transport-owned pending state.
    // Drain the receiver's events/actions, then retry the retained emission.
    to.on_data_bytes(ref, {}, false, now);
}
```

Ignoring that result can look like libmoq silently capped delivery at an event
queue depth. The bytes were actually dropped by the transport stand-in.

A real host owns accept, sockets, and threading, and creates **one
`moq::session` per connection**. The relay core stays host-owned regardless
of transport: the session collection, the `track → subscribers` map, upstream
demand, the fan-out loop, and the per-recipient pending retry. Only the action
drain (`pump()` here) is swapped for a real transport binding.

**pico WT (attach mode).** The host runs its own picoquic packet loop +
h3zero, and for each accepted WebTransport connection attaches the connection
to a session:

```cpp
moq_pico_wt_conn_cfg_t cfg;
moq_pico_wt_conn_cfg_init(&cfg);          // ABI-safe: sets struct_size
cfg.session  = downstream.relay.raw();    // moq::session -> moq_session_t*
cfg.cnx      = cnx;
cfg.h3_ctx   = h3_ctx;
cfg.ctrl_ctx = ctrl_ctx;

moq_pico_wt_conn_t *conn = nullptr;
moq_pico_wt_conn_create(&cfg, &conn);     // error handling omitted
```

then drives `moq_pico_wt_service(conn, now)` per connection in the loop (it
drains that session's actions to the transport, so a network host does *not*
also poll actions itself). `examples/pico_wt/server.c` is the
transport-lifecycle reference (it does this for one connection; a relay
generalizes it to a `cnx → connection` map).

**proxygen / moqx.** The analogous seam is the promoted proxygen WebTransport
adapter (`moq::wt::Adapter`): the host keeps its own proxygen accept loop and
hands each WebTransport connection to libmoq, while retaining all
routing/fan-out/cache policy itself. The transport API differs from pico WT,
but the pattern (attach a session per connection, drive it from your loop,
poll events, fan out) is identical.

No libmoq core or API changes are needed to bind either transport;
`moq::session` plus `session.raw()` for C attach adapters is sufficient. (A
production relay also generalizes the single-track upstream and single-group
shortcuts this example takes.)

## Why no `--net` mode here yet

This example stays in-process on purpose. A pico WT `--net` mode would be
mostly picoquic/h3zero multi-connection accept boilerplate (a `cnx →
connection` map, per-CONNECT session create + attach, iterating
`moq_pico_wt_service` over all connections), and it would exercise the pico
WT C attach path, **not** the proxygen transport moqx actually uses. That
boilerplate would bury the ownership pattern this example exists to show. If a
live local network testing tool is wanted later, it should be a separate
scoped slice/tool; `local-relay` stays focused on the pure host-owned
ownership pattern.

Same-shard note: every session here lives on one thread, so the upstream
object's payload buffer is reused across subscribers as-is. Handing a buffer
to another shard/executor would instead require an explicit copy via
`moq::buffer::clone_for_shard()`.
