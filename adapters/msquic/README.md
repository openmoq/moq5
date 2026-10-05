# adapters/msquic — raw MoQ over QUIC on MsQuic

Direct MoQ-over-QUIC transport adapter on
[MsQuic](https://github.com/microsoft/msquic): the MoQ wire protocol
straight over raw QUIC streams (ALPN `moqt-16`) — no HTTP/3, no
WebTransport, no wtquic dependency. The raw-QUIC counterpart to the
picoquic and mvfst adapters, built to exploit MsQuic's architecture:
direct push sends with zero-copy `moq_rcbuf_t` payloads borrowed until
SEND_COMPLETE (which, with send buffering disabled, means full
acknowledgment), native receive pause/resume for inbound backpressure,
and no adapter network thread — MsQuic's serialized per-connection
workers drive everything.

**Status: experimental, opt-in, default-OFF.** The picoquic adapters
remain the default transports.

Two layers, mirroring the picoquic adapter's shape:

- **Attach** (`<moq/msquic.h>`, `moq_msquic_conn_*`): the caller owns
  the MsQuic API table, registration, configuration and connection;
  the adapter is the connection's callback handler and bridges MsQuic
  events to the MoQ session. Wiring, required transport settings
  (`moq_msquic_settings_init()`; `SendBufferingEnabled=FALSE` is
  mandatory), threading and lifetime rules are documented in the
  header.
- **Managed** (`<moq/msquic_managed.h>`, `moq_msquic_managed_*`): owns
  the whole MsQuic lifecycle behind a small facade with the same
  app-visible contract as `moq_pq_threaded` / `moq_mvfst_managed`.

## Building

MsQuic must be discoverable — either an installed msquic CMake package
(e.g. `brew install libmsquic`) or a checkout via
`-DMOQ_MSQUIC_ROOT=<path>` (header at `src/inc/msquic.h`, library
under `build/bin/`):

```sh
cmake -S . -B build-msquic \
  -DMOQ_BUILD_ADAPTER_MSQUIC=ON \
  -DMOQ_BUILD_MSQUIC_MANAGED=ON
cmake --build build-msquic
```

With both options OFF (the default) nothing discovers or links MsQuic.
`MOQ_BUILD_MSQUIC_MANAGED` requires `MOQ_BUILD_ADAPTER_MSQUIC`.

## Consuming

Build tree: link `moq::adapter-msquic` / `moq::adapter-msquic-managed`
directly, or point `libmoq_DIR` at the build dir. Installed:

```cmake
find_package(libmoq REQUIRED COMPONENTS adapter-msquic-managed)
target_link_libraries(app PRIVATE moq::adapter-msquic-managed)
```

The installed components re-find MsQuic through the shipped
`FindMsQuic.cmake` (installed msquic package first, `MOQ_MSQUIC_ROOT`
fallback). No pkg-config file is installed: MsQuic ships no `.pc` to
express the dependency against, so CMake packages are the supported
consumer path.

The CMake target is the contract, not a library filename. When the
managed facade is enabled its implementation ships **inside** the attach
library (`moq-adapter-msquic`): the facade calls an adapter-internal
helper that must resolve in the same shared object, so a `BUILD_SHARED_LIBS=ON`
split into two dylibs cannot link. `moq::adapter-msquic-managed` is
therefore an INTERFACE target over the attach library (plus `Threads`) —
`find_package(... COMPONENTS adapter-msquic-managed)` and the target name
are unchanged, but there is no separate managed archive/dylib to link by
path. Requesting only the managed component loads the attach component
first. `MOQ_BUILD_MSQUIC_MANAGED=OFF` keeps the attach library lean and
thread-free.

## Managed facade threading contract

MsQuic owns its worker threads; there is no facade network thread.
Connections are partitioned across `cfg.lane_count` **lanes**, each an
independent lock domain — its own mutex and doorbell — owning a set of
connections. Transport events run on MsQuic's serialized per-connection
worker, and the facade brackets every one of those callbacks with the
connection's **lane** mutex; that lane's doorbell takes the same mutex
only for `moq_msquic_lane_wake()`/`moq_msquic_managed_wake()` delivery
and MoQ session deadlines. `on_lane_pump(m, lane, now, user)` — invoked
under the owning lane's mutex from either context — is the ONLY place
application code may call `moq_session_*`, and it services **all** of
that lane's connections in one pass (iterate them with
`moq_msquic_lane_next_conn()`). Distinct lanes run concurrently and
never take each other's mutexes; lock order is a lane mutex before the
facade mutex, never the reverse. App threads communicate through their
own queues plus `wake()`/`wait()`. From inside `on_lane_pump`/
`on_activity`, `stop()` and `wait()` are refused with
`MOQ_ERR_WRONG_STATE`; the terminal accessors and the wake calls detect
the callback context and are safe there. `stop()` quiesces every lane's
transport before any blocking MsQuic close runs, and never holds a lane
mutex across them.

`moq_msquic_managed_session()` is a CLIENT-only convenience (NULL on a
server); server code reaches its sessions through lane iteration.

## Current scope

- Server accepts up to `cfg.max_connections` concurrent connections
  (default 1); connects past the cap — or after
  `moq_msquic_managed_drain()` — are refused at the listener. Accepted
  connections are placed on a lane by `cfg.choose_lane` (or round-robin
  when it is NULL); an out-of-range choice refuses the connection
  rather than clamping. Each accepted connection carries its own MoQ
  session, iterated inside its lane's `on_lane_pump` via
  `moq_msquic_lane_next_conn()`; a closed connection's slot is
  reclaimed and can be reused. `stop()` shuts all live connections
  down, drains every pending send/datagram, and closes the listener.
- Exact-version negotiation: each endpoint offers exactly one MoQ ALPN
  chosen by `cfg.version` (default draft-16); a client/server mismatch
  fails the handshake with a bounded fatal terminal and no MoQ setup.
  Deferred multi-version offers (a real ALPN list) are a planned
  addition.
- No idle deadline tick in attach mode (the managed facade's doorbell
  services session deadlines).
- Service-tier (moq-service endpoint) wiring is deferred.

## Receive admission (held input)

The attach adapter declares `MOQ_TRANSPORT_CAP_HOLD_INPUT`. When the
session has no free receive entry for a NEW peer data stream, the bridge
refuses that stream's chunk with `MOQ_ERR_INPUT_NOT_CONSUMED` instead of
letting the session STOP it unparsed or discarding it. The adapter honours
the refusal with MsQuic's own partial-acceptance mechanism, in the default
(non-multi) receive mode `moq_msquic_settings_init()` mandates:

- In the RECEIVE callback the refused `QUIC_BUFFER` contributes nothing to
  the accepted `TotalBufferLength`; buffers fed before it count exactly
  their lengths. MsQuic keeps the refused buffer and everything after it
  (and the event's FIN), stops indicating RECEIVE on that stream, and
  re-indicates the held suffix once after `StreamReceiveSetEnabled(TRUE)`.
  Nothing is copied by the adapter and nothing MsQuic already accepted is
  ever re-fed. Retained-input backpressure (the bridge's `WOULD_BLOCK`) keeps
  its prior accounting: the buffer that caused it counts as accepted.
- Only the affected stream is paused; the resume happens on the service
  pass that sees the bridge's pending state clear (the session can admit a
  stream again). A redelivery can be refused again if another stream took
  the entry first; it is then held again, losslessly.
- MsQuic cannot hold a bare FIN. A zero-byte FIN receive or a
  `PEER_SEND_SHUTDOWN` that arrives while bytes are held is refused by the
  bridge and stays owned by the adapter (`fin_held`); it is fed, in order,
  after every byte of the stream: right after a re-indication the bridge
  accepts in full, or -- when the accepted redelivery lands in the
  session's own retained backpressure -- on the service pass that sees the
  bridge's pending input for that stream drain (no native callback ever
  comes for it). Exactly once; a stream reset or teardown before then
  releases it unfed. The shutdown does not clear the pause or the disabled
  delivery while bytes are held.
- `PEER_SEND_ABORTED` releases the hold (MsQuic drops its held bytes with
  the abort) and the bridge retires the stream; `SHUTDOWN_COMPLETE` frees the
  bookkeeping; a connection teardown frees every stream. No resume is ever
  issued on a retired handle.

Bounds: per stream, the bytes MsQuic holds are capped by the stream receive
window; across the connection by the connection flow-control window; the
number of such streams by the peer unidirectional stream count. The
figures `moq_msquic_settings_init()` stamps are `StreamRecvWindowDefault`
16 MiB, `ConnFlowControlWindow` 32 MiB and `PeerUnidiStreamCount` 131;
they are that helper's defaults, not a guarantee of the attach API -- a
caller that loads its own `QUIC_SETTINGS` into the native Configuration
owns whatever windows it configured, and the adapter cannot read them
back. Precondition: the default single-outstanding-RECEIVE mode
(`StreamMultiReceiveEnabled = FALSE`, mandatory per `moq/msquic.h`); the
partial-accept arrest relies on it, and in that mode the accepted-prefix
decision is made synchronously per event on MsQuic's serialized
per-connection worker (the adapter's callbacks run inline there, serialized
by the attach guard). The adapter does not verify the setting at attach
(MsQuic does not expose it on a connected handle); it is a documented
caller obligation, stamped by the settings helper the managed facade uses.

Proof: `msquic_hold_input` (fake API table, raw server relay, draft-16 and
draft-18 setup and request routing; accounting, multi-buffer events, FIN
with payload, bare FIN and shutdown while held, repeated refusal, abort
while held, teardown, retained-input accounting with an exact held suffix,
a FIN owed behind retained input, exact object inventories, legacy control).

## Managed server MTU policy (process-global)

A managed **server** conservatively caps its initial path MTU to a
1280-byte IP path MTU (a 1252-byte IPv4 UDP payload) so it interoperates
with peers that discard larger early datagrams. This happens at two
layers, deliberately:

- The listener **configuration** pins `MinimumMtu = MaximumMtu = 1280`,
  which caps every **connection-scoped** datagram once ALPN/SNI has
  selected the configuration.
- The configuration does **not** govern the server's very first,
  pre-configuration reply to a bare Initial. That reply is sized on the
  pre-configuration **connection** path: the new connection copies
  MsQuic's library-global settings and seeds its path MTU before any
  configuration is attached, so it is sized from the **process-global**
  default `MinimumMtu` (1288 in this MsQuic revision) and stayed 1260
  bytes with the configuration alone in place. So a managed server also
  installs a **process-global** floor once at create time — a single
  `SetParam(NULL, QUIC_PARAM_GLOBAL_SETTINGS, …)` declaring only
  `MinimumMtu = 1280`, issued after `MsQuicOpen2` and before
  `RegistrationOpen`.

Consequences to be honest about:

- The global floor is **process-wide** — MsQuic exposes no
  registration-scoped settings parameter. A managed **client** created in
  the same process **after** a managed server therefore inherits the
  conservative 1280 initial floor. The client's own configuration still
  declares no MTU override, and no process-global **maximum** is set, so
  DPLPMTUD can still grow either endpoint's path.
- It is **not restored** on destroy. MsQuic itself owns global-settings
  lifetime: it loads them at the first `MsQuicOpen2` reference and may
  reset them on a later `MsQuicOpen2` after the last `MsQuicClose`. So the
  floor remains in force while MsQuic stays initialized and another open
  API table or registration may depend on it; if MsQuic later resets its
  globals, the next managed-server create simply reapplies the floor.
  LibMoQ adds **no** refcount and **no** destroy-time restore. Only the
  conservative minimum moves; the maximum is never pinned globally.
- A managed **client** create does **not** issue the global write. If the
  global write is rejected on a server create, the create **fails closed**
  (`MOQ_ERR_INTERNAL`, `*out` left NULL) rather than starting a listener
  that reproduces the oversized reply.

## Tests

### Which command to run

A CTest label is inclusive metadata — it selects, it never excludes — so
the fast gate has to say what it is leaving out:

```sh
# fast correctness gate (this is the one that needs the exclusions)
ctest --test-dir <tree> -R '^msquic_' -LE 'qualification|soak' \
      --output-on-failure

# the qualification lane, required but run on its own
ctest --test-dir <tree> -L qualification --output-on-failure

# the soak lane
ctest --test-dir <tree> -L soak --output-on-failure
```

`-LE` takes a regex, so the two exclusions are alternated in one quoted
argument; quote it, or the shell will treat `|` as a pipe.

Plain `ctest` and `ctest -L msquic` deliberately run **everything** —
adding a `qualification` or `soak` label to a test does not remove it from
either. Anything that defines a fast gate must therefore spell the
exclusions out; nothing else makes the distinction real.

### What runs where

Most of the managed facade is proven **deterministically**, over a fake
`QUIC_API_TABLE` with no listener, socket, credential or certificate
(labelled `msquic;sansio`, and each well under a second):

- `msquic_unit` — the adapter white-box: stream-id computation, send
  budget, rcbuf ownership, credit gating, receive arrest/hold/resume, and
  capacity invariance across receive-queue ceilings;
- `msquic_cfg_prefix` — the size-qualified config-prefix derivation at
  three `struct_size` boundaries;
- `msquic_facade_control` — drain, stop idempotence, the facade pending
  aggregates as a true cross-lane sum, and `wait()` reporting `MOQ_DONE`
  rather than a terminal on a live server;
- `msquic_confinement` — the CLIENT-only session accessor's pump window,
  and `stop()` refused from inside a managed callback;
- `msquic_no_spin` — one wake owes exactly one pump generation, counted
  rather than timed;
- `msquic_child_forwarding` — `cfg.streaming_objects` reaching the child
  session through the production child path;
- `msquic_lanes`, `msquic_lane_stats`, `msquic_close_feed`,
  `msquic_terminal_ack`, `msquic_reap_fairness`, `msquic_wake`,
  `msquic_settings` — lane placement, statistics, close feeding, terminal
  acknowledgement and reap fairness.

The `network`-labelled tests are the residue that needs a real transport,
and they share one committed test-only loopback identity (see
`tests/README_test_certs.md` — there is no certificate-generation fixture):

- `msquic_conformance` — the production contract over real MsQuic:
  setup, subscribe/object, reset, datagrams, send pressure, and the
  clean/fatal version outcomes;
- `msquic_recv_loopback` — application-event hold/release: a receiver that
  stops polling object events survives the transfer without loss or a
  fatal, delivers nothing to its application while held, keeps pumping on
  real transport activity with no application wake, and on release takes
  the whole workload byte-exact, in order and exactly once. It claims no
  sender backpressure and no adapter arrest;
- `msquic_loopback` — streaming-object rendering in both dispositions, a
  real second client refused at the admission cap, and a connect that can
  never complete;
- `msquic_reap` — the reap/pump ordering pin;
- `msquic_over_window_credit` — **qualification**: 48 MiB across one
  subgroup, crossing the 16 MiB stream and 32 MiB connection receive
  windows, which is the one fact no fake rail can reproduce. It is *not*
  a pause/resume proof and does not own queue-capacity policy;
- `msquic_stress` — **soak**: repeated full-lifecycle churn, excluded from
  the fast gate and run through `ctest -L soak`.

`msquic_public_compile` / `_cxx` pin the standalone headers;
`msquic_adapter_consumer` / `msquic_install_consumer` build the
`find_package(libmoq COMPONENTS adapter-msquic-managed)` consumer against
the build tree and a scratch install prefix; `msquic_managed_shared_link`
configures a fresh `BUILD_SHARED_LIBS=ON` tree and builds + runs the
managed component through the shared base dylib (a static build cannot
catch the cross-dylib link).
