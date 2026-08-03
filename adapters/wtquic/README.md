# adapters/wtquic — MoQ over wtquic WebTransport

MoQ transport adapter over [wtquic](https://github.com/rwl4/wtquic), a standalone
WebTransport-over-HTTP/3 library. Two layers ship here:

- **Attach (`<moq/wtquic.h>`), backend-neutral** — the caller owns both the
  `moq_session_t` and the wtquic session; the adapter implements the
  transport-bridge endpoint ops on wtquic's public API and feeds wtquic's
  session events into the bridge. It names only core wtquic types
  (`wtq_session_t`, `wtq_session_events_t`), never a transport backend — its
  production dependency is the wtquic core. Which backend a session runs on
  (MsQuic, Network.framework, …) is the caller's choice; the real-transport
  tests below drive MsQuic.
- **Managed Network.framework client (`<moq/wtquic_network_managed.h>`),
  Apple-only** — `moq_wtquic_network_managed` owns the whole vertical (MoQ session →
  bridge → wtquic attach → wtquic's Network.framework backend), so an app
  connects, pumps, and tears down through one facade (see below).

Object payloads cross both libraries zero-copy (an `moq_rcbuf_t`
reference is held from the bridge's `write_payload` until wtquic
reports the send complete).

**Status: experimental, opt-in, default-OFF.** The picoquic adapters
remain the default transports.

## Naming glossary

These names look alike; they identify different things. Keep them distinct:

| Name | What it is |
|---|---|
| **msquic** | Direct MoQ over **raw QUIC** using Microsoft's MsQuic. A concrete transport backend, selected via the service backend enum `MOQ_TRANSPORT_BACKEND_MSQUIC`. |
| **wtquic** | The **backend-neutral** attach bridge (`<moq/wtquic.h>`, `moq_wtquic_conn_*`) over the wtquic core. Not itself a transport backend — a session's actual backend is chosen underneath it — so it is deliberately **not** a backend enum value: bare `wtquic` names no upstream transport. |
| **wtquic-network** | WebTransport over Apple's **Network.framework** (wtquic's `network` component). The managed facade is `<moq/wtquic_network_managed.h>` / `moq_wtquic_network_managed_*`; selected via `MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK` (Swift `.wtquicNetwork`). |
| **wtquic-msquic** | WebTransport over **MsQuic** (wtquic's `msquic` component). The managed facade is `<moq/wtquic_msquic_managed.h>` / `moq_wtquic_msquic_managed_*` (target `moq::adapter-wtquic-msquic-managed`, package `libmoq-wtquic-msquic-managed`, build option `MOQ_BUILD_WTQUIC_MSQUIC_MANAGED`); selected via the service backend enum `MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC`. Cross-platform wherever wtquic's MsQuic component builds; client-only; full multi-version WT-Protocol negotiation. Distinct from **msquic** (direct raw QUIC, no WebTransport) and **wtquic-network** (WebTransport over Network.framework). |

The selectors above are the committed C backend enum
(`MOQ_TRANSPORT_BACKEND_*` in `<moq/endpoint.h>`); there is no committed CLI
backend token yet. A future CLI would spell them in lowercase (e.g. `msquic`,
`wtquic-network`, `wtquic-msquic`) and must never accept bare `wtquic`, which
identifies no upstream transport.

`wtq_nw_*` and `wtq::network` are **upstream wtquic** names (Network.framework
backend); libmoq does not rename them.

## Building

wtquic must be installed and discoverable as a CMake package. The
standard path is the pinned setup script, which clones, builds and
installs wtquic (at a known-good commit) into `.deps/wtquic-ci/` and
prints the configure inputs:

```sh
eval "$(scripts/setup_wtquic_deps.sh)"
cmake -S . -B build-wtquic \
  -DMOQ_BUILD_ADAPTER_WTQUIC=ON \
  -DCMAKE_PREFIX_PATH="$CMAKE_PREFIX_PATH"
cmake --build build-wtquic
```

MsQuic must be discoverable for wtquic's backend (an installed msquic
CMake package such as Homebrew's `libmsquic`, or `WTQ_MSQUIC_ROOT` /
`msquic_DIR`, which the script forwards). `WTQUIC_REF`, `WTQUIC_REPO`,
`WTQUIC_DEPS_DIR` and `CMAKE_BUILD_TYPE` override the pin, remote,
location and build type; rerunning reuses the checkout and build.

An existing wtquic install works the same way without the script:

```sh
cmake --install <wtquic-build-dir> --prefix /path/to/wtquic/install

cmake -S . -B build-wtquic \
  -DMOQ_BUILD_ADAPTER_WTQUIC=ON \
  -DCMAKE_PREFIX_PATH=/path/to/wtquic/install
cmake --build build-wtquic
```

With `MOQ_BUILD_ADAPTER_WTQUIC=OFF` (the default) nothing requires
wtquic. With it ON and wtquic missing, configure fails with an
actionable message.

## Consuming

Build tree: link `moq::adapter-wtquic`. Installed:

```cmake
find_package(libmoq REQUIRED COMPONENTS adapter-wtquic)
target_link_libraries(app PRIVATE moq::adapter-wtquic)
```

The installed component re-finds the wtquic package (the backend-neutral
core, `find_dependency(wtquic CONFIG)` — no backend component forced)
through normal CMake package search, so wtquic's install prefix must be on
`CMAKE_PREFIX_PATH` for consumers too. A `libmoq-wtquic.pc` is installed for
pkg-config consumers; it expresses the transport dependency as
`Requires: wtquic` (the core) and therefore needs wtquic's `pkgconfig`
directory on `PKG_CONFIG_PATH`; a consumer that drives a real transport also
links that backend's own `.pc` (e.g. `wtquic-msquic`). CMake is the fully
supported consumer path.

The public header is `<moq/wtquic.h>`; wiring, threading and session
ownership are documented there. In short: create the MoQ session and
the adapter conn, hand `moq_wtquic_conn_events()` (with the conn as
user context) to `wtq_msquic_client_connect` or
`wtq_msquic_listener_start`, and drive the session from the adapter
hook, which runs on wtquic's transport worker. That worker is the only
thread that may make `moq_session_*` calls; the application thread
communicates through its own synchronization and reads results the
hook recorded. The MoQ session must outlive the conn, and the conn is
destroyed only after the wtquic side is fully torn down
(`wtq_msquic_env_close` is the quiescence barrier).

## Managed Network.framework client (Apple)

`moq_wtquic_network_managed` (`<moq/wtquic_network_managed.h>`, macOS 13 / iOS 16+)
is a managed MoQ **client** over wtquic's Network.framework backend —
WebTransport only (not raw QUIC), one client connection, and NOT a
listener/server facade (the separate MsQuic-backed managed facade below owns
the server story). There is NO worker thread: wtquic's backend-owned serial
queue is the connection's serialization domain, and every MoQ/bridge
operation — `on_lane_pump`/`on_activity`, teardown — runs on it, so
`moq_session_t` confinement holds by construction; cross-thread work maps
onto that domain via `moq_wtquic_network_managed_wake()`.

It speaks the same managed **lane** interface as the picoquic-threaded,
mvfst, and MsQuic-managed adapters, degenerated to exactly one lane owning
exactly one connection: `on_lane_pump(m, lane, now_us, user)` is the
exclusive access window; inside it `moq_wtquic_network_lane_next_conn()` yields
the one connection view and the view accessors (`_conn_session`,
`_conn_adapter`, `_conn_lane`) resolve; outside it — including
`on_activity` (notification-only) and posted closures — they return NULL.
`lane_count` is always 1; a config asking for more lanes or a server
perspective is refused (`MOQ_ERR_UNSUPPORTED`). Distinct from the strict
window, `moq_wtquic_network_managed_session()`/`_adapter()` are the ACTOR-DOMAIN
escape hatch: legal anywhere on the serial domain, including
`moq_wtquic_network_managed_post()` closures — "on the domain" is deliberately
broader than "inside the lane pump".

It needs wtquic's **`network`** component in addition to `msquic`. The setup
script builds it by default on Apple (`WTQ_BUILD_NETWORK`, Apple-only). Configure
libmoq with:

```sh
eval "$(scripts/setup_wtquic_deps.sh)"          # builds the network component on macOS
cmake -S . -B build-wtquic \
  -DMOQ_BUILD_ADAPTER_WTQUIC=ON \
  -DMOQ_BUILD_WTQUIC_NETWORK_MANAGED=ON \
  -DCMAKE_PREFIX_PATH="$CMAKE_PREFIX_PATH"
cmake --build build-wtquic
```

`MOQ_BUILD_WTQUIC_NETWORK_MANAGED` requires `MOQ_BUILD_ADAPTER_WTQUIC`. Consume
`moq::adapter-wtquic-network-managed` (build tree) or
`find_package(libmoq COMPONENTS adapter-wtquic-network-managed)`; a
`libmoq-wtquic-network-managed.pc` is installed for pkg-config. `test_wtquic_network_managed`
(labelled `network`) is the loopback + lifecycle suite; `consumer/main_network.c[pp]` are the
packaging smokes. The create-time earliest-callback window is covered by composed proof,
not a single test here — wtquic's `t_earliest_callback_publication` proves the conn slot is
published before create returns, and this facade passes that slot as `&m->conn` under its
mutex (the deterministic seam lives in wtquic's never-installed `network-testing`
component, so a libmoq test cannot reach it without distorting the package boundary).

## Managed MsQuic facade (cross-platform)

`moq_wtquic_msquic_managed` (`<moq/wtquic_msquic_managed.h>`) is a managed MoQ
facade over wtquic's **MsQuic** WebTransport backend — WebTransport only (not
raw QUIC), cross-platform wherever wtquic's `msquic` component builds. It is
**client** for the service tier (one lane, one connection) but also supports a
multi-connection **server** (used by its own loopback test). Unlike the
Network.framework client it owns its execution context: a coordinator thread
plus per-lane doorbell worker threads behind a per-lane lock domain, with
coalesced wake delivery, live reap of terminal connections, session-deadline
scheduling, an optional `on_activity` signal, and a bounded, ABI-frozen per-lane
statistics surface (`moq_wtquic_msquic_lane_get_stats`).

It speaks the same managed **lane** interface as the picoquic-threaded, mvfst,
MsQuic-managed, and Network.framework adapters: `on_lane_pump(m, lane, now_us,
user)` is the exclusive access window; inside it `moq_wtquic_msquic_lane_next_conn()`
yields each connection view and the view accessors resolve; outside it they
return NULL. WT-Protocol negotiation is **real and per-connection** (it offers
the full version list and the peer selects), so it is the multi-version
WebTransport path, distinct from the exact-version direct-MsQuic backend.

Build with `MOQ_BUILD_WTQUIC_MSQUIC_MANAGED` (requires `MOQ_BUILD_ADAPTER_WTQUIC`;
the same MsQuic discovery as the attach adapter above):

```sh
eval "$(scripts/setup_wtquic_deps.sh)"
cmake -S . -B build-wtquic \
  -DMOQ_BUILD_ADAPTER_WTQUIC=ON \
  -DMOQ_BUILD_WTQUIC_MSQUIC_MANAGED=ON \
  -DCMAKE_PREFIX_PATH="$CMAKE_PREFIX_PATH"
cmake --build build-wtquic
```

Consume `moq::adapter-wtquic-msquic-managed` (build tree) or
`find_package(libmoq COMPONENTS adapter-wtquic-msquic-managed)`; a
`libmoq-wtquic-msquic-managed.pc` is installed for pkg-config (its wtquic-msquic
dependency carries the MsQuic search path). `test_wtquic_msquic_managed` /
`_internal` are the black-box + white-box suites, `_loopback` is the real-MsQuic
loopback, and `consumer/main_msquic.c[pp]` are the packaging smokes. Selecting
it from the **service tier** is `MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC`
(`<moq/endpoint.h>`) — see `../../docs/transport-integration-guide.md`; that
backend's real loopback (both drafts, byte-exact delivery, TLS/SNI rejection) is
`endpoint_wtquic_msquic_smoke`.

## Tests

```sh
ctest --test-dir build-wtquic -R '^wtquic_'
```

`wtquic_loopback` runs the MoQ publish/subscribe smoke over real MsQuic
on localhost (self-signed certs generated into the build dir);
`wtquic_conformance` is the production-contract conformance rail
(scripted scenarios where every MoQ session call runs inside the
adapter hook on the transport worker — see
`tests/wtquic_contract_pair.h`); `wtquic_stress` churns repeated full
connection lifecycles with rotating object sizes, a close racing
outbound drain, a refusal, and a reset-recover cycle
(`MOQ_WTQUIC_STRESS_LOOPS=<n>` extends it for manual soaks);
`wtquic_public_compile` / `_cxx` pin the standalone header;
`wtquic_install_consumer` and `wtquic_adapter_consumer` build the
`find_package(libmoq COMPONENTS adapter-wtquic)` consumer against a
scratch install prefix and the build tree respectively. The localhost
tests are labelled `network`.

## Transport comparison benchmark

```sh
cmake -S . -B build-wtbench \
  -DMOQ_BUILD_ADAPTER_WTQUIC=ON \
  -DMOQ_BUILD_ADAPTER_PICOQUIC=ON \
  -DMOQ_BUILD_ADAPTER_PICO_WT=ON \
  -DMOQ_BUILD_PICO_WT_MANAGED=ON \
  -DMOQ_BUILD_BENCHMARKS=ON \
  -DMOQ_PICOQUIC_SOURCE_DIR=/path/to/picoquic \
  -DMOQ_PICOTLS_PREFIX=/path/to/picotls/build \
  -DCMAKE_PREFIX_PATH=/path/to/wtquic/install
cmake --build build-wtbench
ctest --test-dir build-wtbench -R '^bench_wt_compare_smoke$'
```

`moq_bench_wt_compare` runs the identical MoQ workload over this
adapter (real MsQuic loopback) and over the managed picoquic
WebTransport loopback; see its `--help`. Current numbers show parity
on the representative single-stream workload (ratios within a few
percent across runs at 1000 x 1200 B).

Performance note on object size: wtquic's per-stream send budget
throttles queued depth (1 MiB floor, growing with the transport's
buffering advice), an idle stream admits one legal send of any size,
and a send the budget refuses is retried from wtquic's stream-writable
edge — so media-sized objects pipeline instead of serializing on
acknowledgments. Current loopback numbers lead the managed picoquic
WebTransport path across 1200 B, 8 KiB and 64 KiB object sizes.

## Known limitations

- One WebTransport session per adapter conn (bind a fresh conn per
  accepted session).
- No outgoing MoQ datagram publishing capability yet (inbound
  datagrams are fed through).
- No idle/deadline tick integration: timer-driven session closes
  (GOAWAY drain) need external service calls.
- Held-buffer receive remains deferred in wtquic; inbound backpressure
  uses wtquic's receive pause/resume.
- The wtquic dependency must be installed and discoverable — it is
  never vendored or built by this repository.
