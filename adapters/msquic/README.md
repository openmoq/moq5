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

## Tests

```sh
ctest --test-dir build-msquic -R '^msquic_'
```

`msquic_unit` drives the adapter white-box over a fake QUIC_API_TABLE
(stream-id computation, send budget, rcbuf ownership, credit gating —
no real transport); `msquic_loopback` runs the managed client + server
over real MsQuic on localhost (self-signed certs generated into the
build dir), including an object above the 64 KiB send-budget floor and
a clean close; `msquic_public_compile` / `_cxx` pin the standalone
headers; `msquic_adapter_consumer` / `msquic_install_consumer` build
the `find_package(libmoq COMPONENTS adapter-msquic-managed)` consumer
against the build tree and a scratch install prefix;
`msquic_managed_shared_link` configures a fresh `BUILD_SHARED_LIBS=ON`
tree and builds + runs the managed component through the shared base
dylib (a static build cannot catch the cross-dylib link). The localhost
tests are labelled `network`.
