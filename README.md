# MOQ5

C library for [Media over QUIC Transport](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/):
a sans-I/O protocol core plus transport adapters, with a high-level **media
service tier** for applications. Supports **draft-16 and draft-18**, selected by
transport version negotiation.

**Status:** the draft-16 and draft-18 protocol surfaces are substantially
complete and exercised by deterministic scenario tests: subscribe, fetch
(standalone and joining), publish, track status, object datagrams, namespace
subscriptions, goaway, request updates, and auth tokens. On top of the core, a
**service tier** (`moq_endpoint_t`, `moq_media_receiver_t`, `moq_media_sender_t`)
is the recommended surface for media applications (FFmpeg / VLC / GStreamer /
OBS-style consumers) — it owns the connection, version negotiation, and the
catalog/media flow. Multiple QUIC/WebTransport adapters are implemented (see
below). This is pre-1.0: the core is stable and well-tested; the WebTransport
adapters are marked experimental. Not "production complete"; verify against your
own endpoint before depending on it.

**Media consumers start here:** [`examples/service/README.md`](examples/service/README.md).

## Design

- **Sans-I/O core:** no sockets, no threads, no wall clock, no global
  state. The caller drives all I/O; the library is a deterministic state
  machine. QUIC is plugged in through an adapter layer.
- **Explicit allocator:** every allocation goes through a caller-provided
  `moq_alloc_t` vtable. No hidden `malloc`.
- **Deterministic simulation:** built for FoundationDB-style
  seed-replayable testing with fault injection, OOM loops, and
  bounded-output invariants (`moq-sim` / SimPair).
- **Versioned profiles, negotiated not guessed:** draft-16 and draft-18 are
  both implemented as profiles. The session's wire version is fixed at creation
  and immutable; there is **no** version auto-detection. The version comes from
  the negotiated transport context: raw QUIC selects a MoQ ALPN (`moqt-16` /
  `moqt-18`); H3 WebTransport uses the `h3` ALPN plus WebTransport-protocol
  selection. The service tier and managed adapters drive this negotiation for
  you (offer the set, create the session for the agreed version). An
  unknown/unoffered version fails rather than guessing.
- **Shared transport bridge:** adapters do not each reimplement the
  session↔transport contract. `moq_transport_bridge_t` (in `moq-core`)
  owns stream mapping, outbound/inbound backpressure retry,
  FIN/reset/stop lifecycle, tombstoning, and close/drain semantics. An
  adapter implements a thin endpoint-ops vtable; the bridge does the
  rest. (Private adapter SPI, versioned lockstep with `moq-core`; not a
  stable third-party/application ABI — see the API stability table.)

## Media service tier

For media applications, the service tier is the recommended API. It links only
`moq::service`, uses only public service headers, and hides the session,
publisher/subscriber facades, and transport details. The application thread
calls a small poll/wait/write surface; the network thread and protocol live
below it.

- **`moq_endpoint_t`** — a URL becomes a managed connection + MoQ session:
  ALPN/WebTransport version negotiation, TLS/cert verification, connect / wait /
  interrupt / drain / `post()`, and the catalog subscription/publication. Your
  code never touches `moq_session_t`.
- **`moq_media_receiver_t`** — catalog-driven receive. Surfaces track discovery
  (`TRACK_ADDED` / `CATALOG_READY`) with stable handles; `auto_subscribe` for a
  simple player, or manual `subscribe_track` / `unsubscribe_track` for
  integrators that build streams/pads first; `track_state` for per-track status;
  `poll_object` delivers media with ownership transfer; bounded queues with an
  explicit overflow policy (FLOW_CONTROL pause, counted drops — never silent).
- **`moq_media_sender_t`** — advertise a namespace, publish a catalog, and send
  media: `add_track` (with codec + `init_data`), `write` through a sync-anchored
  queue with honest backpressure, and `end_track` for a clean, reliable
  per-track terminal that surfaces on receivers as `MOQ_MEDIA_TRACK_ENDED`.
  Ending one track leaves others and the session alive. For finite publishers,
  call `moq_endpoint_drain()` after the sender is destroyed and before
  `moq_endpoint_stop()` so reliable stream bytes already queued in the transport
  are flushed before the endpoint's abrupt stop.
- **Ownership:** `poll_object` MOQ_OK transfers the object's buffers to you
  (release with `moq_media_object_cleanup` on the polling thread); `write` MOQ_OK
  transfers your payload to the service (on any non-OK you still own it).
- **Decoder config & CMAF bytes (footguns):** codec init config travels in the
  catalog, not in objects — `desc.init.codec_config` is the decoder extradata
  (SPS/PPS/VPS, AAC ASC), `desc.init_data` is the full container/init segment.
  For CMAF objects the media bytes are the mdat slice of `obj.fragment`
  (`obj.payload` is empty); RAW/LOC objects use `obj.payload`.

See [`examples/service/README.md`](examples/service/README.md) for the full
contract, examples (`media_receive.c` / `media_send.c`), and build/consume
instructions.

## Transport adapters

All adapters share the transport bridge and a common conformance suite
(`tests/conformance/`); a backend declares which capabilities it
supports and the shared scenarios run against it.

| Adapter | Transport | Stack | Modes | Status |
|---|---|---|---|---|
| **picoquic** | raw QUIC (`moqt-16`/`moqt-18`) | picoquic (C) | attach + managed (`moq_pq_threaded_t`) | stable; lightest footprint |
| **mvfst** | raw QUIC (`moqt-16`/`moqt-18`) | Meta mvfst (C++/folly) | attach + managed (`moq_mvfst_managed_t`) | stable; multi-connection server |
| **picoquic WebTransport** | WebTransport / HTTP-3 (`h3`) | picoquic h3zero/picowt (C) | attach + managed (`moq_pico_wt_managed_t`) | **experimental** |
| **proxygen WebTransport** | WebTransport | proxygen (C++) | attach + managed (`moq_proxygen_wt_managed_t`) | **experimental** |

WebTransport is supported through the picoquic WT and proxygen WT
adapters; libmoq does **not** depend on moxygen. WebTransport support is
experimental relative to the raw-QUIC adapters. A *managed facade* owns
the QUIC context, a network thread, and the connection lifecycle and
hands you a running session to drive: picoquic raw, picoquic WT, mvfst
raw, and proxygen WT all have one (proxygen's also drives the WT CONNECT
itself). proxygen WT additionally offers a C++ attach mode where you own
the `proxygen::WebTransport*` and drive `service()` yourself.

**Backpressure:** the shared bridge owns the core WOULD_BLOCK contract
(retain and retry). Inbound backpressure depends on what the transport
can do: mvfst and proxygen WT can pause/resume reads; picoquic/h3zero
(pico_wt) cannot pause reads, so unresolvable inbound backpressure is a
hard failure that tears the connection down rather than silently
dropping data.

See [docs/transport-integration-guide.md](docs/transport-integration-guide.md)
for choosing and wiring a backend, and
[docs/adapters.md](docs/adapters.md) for the adapter contract and porting
guide.

## Build

Quickstart (core + sim + tests):

```sh
cmake -B build -S .
cmake --build build
ctest --test-dir build
```

Or use the canonical presets, which write to `build/<preset>`:

```sh
cmake --preset rc-static          # static core + sim + tests
cmake --build build/rc-static
ctest --test-dir build/rc-static
```

(Other presets: `rc-shared`, `cppbind`, `install`; see `CMakePresets.json`.)

Requires CMake ≥ 3.20 and a C11 compiler. No external dependencies for
`moq-core`. The C++ binding requires C++20 (`-DMOQ_BUILD_CPP_BINDING=ON`).
Adapters are opt-in CMake options (`MOQ_BUILD_ADAPTER_PICOQUIC`,
`MOQ_BUILD_ADAPTER_MVFST`, `MOQ_BUILD_ADAPTER_PICO_WT`,
`MOQ_BUILD_ADAPTER_PROXYGEN`, …) and pull in their respective QUIC stacks.

The media service tier is `MOQ_BUILD_SERVICE=ON` (it requires `MOQ_BUILD_MSF`
and `MOQ_BUILD_MEDIA_OBJECT`). To make `moq_endpoint_connect` usable for a
transport, also enable a managed facade: `MOQ_BUILD_ADAPTER_PICOQUIC=ON` +
`MOQ_BUILD_PQ_THREADED=ON` for raw QUIC, and/or `MOQ_BUILD_ADAPTER_PICO_WT=ON` +
`MOQ_BUILD_PICO_WT_MANAGED=ON` for WebTransport. Without a compiled managed
facade the service still builds, but that transport returns
`MOQ_ERR_UNSUPPORTED`. The consumer-facing examples build under
`examples/service/`.

> **Source-tree picoquic required for the managed facades.** Both
> `MOQ_BUILD_PQ_THREADED` and `MOQ_BUILD_PICO_WT_MANAGED` implement
> `moq_endpoint_drain`'s graceful flush using picoquic's private stream state,
> so they must be configured against a picoquic **source tree** —
> `-DMOQ_PICOQUIC_SOURCE_DIR=/path/to/picoquic`. The installed picoquic CONFIG
> package does not ship the private header and will fail configuration with a
> clear message.

## Packaging

Install with `cmake --install`, then consume via
`find_package(libmoq COMPONENTS ...)`:

| Component | CMake | pkg-config |
|---|---|---|
| core (`moq::core`) | always | `libmoq` |
| **media service tier (`moq::service`)** | `find_package(libmoq COMPONENTS service)` | `libmoq-service` |
| picoquic raw (`adapter-picoquic`, `adapter-picoquic-threaded`) | yes | folded into `libmoq` (no standalone `.pc`) |
| picoquic WebTransport (`adapter-pico-wt`) | yes | `libmoq-pico-wt` |
| picoquic WT managed (`adapter-pico-wt-managed`) | yes (experimental) | `libmoq-pico-wt-managed` |
| mvfst (`adapter-mvfst`) | yes | **CMake-only** |
| proxygen WebTransport (`adapter-proxygen-wt`) | yes (experimental) | **CMake-only** |

`libmoq-service` / `moq::service` is the high-level component for media
applications; it pulls the transport chain it was built with (the picoquic raw
+ threaded libs, and `libmoq-pico-wt-managed` when the managed WebTransport
facade was compiled in), so a consumer links it and gets a working endpoint.

mvfst and proxygen WT are CMake-only by design: their dependency stacks
(folly/fizz/proxygen) ship CMake config packages, not `.pc` files, and
their public headers require a C++ toolchain. See the integration guide
for the full consumer matrix.

## API stability

Everything is **pre-1.0**: no API/ABI is frozen yet. Within that, the
surfaces differ in maturity and intended audience:

| Surface | Headers | Status |
|---|---|---|
| **Media service API** (primary app API for media) | `<moq/endpoint.h>`, `<moq/media_receiver.h>`, `<moq/media_sender.h>` | the recommended surface for media applications; pre-1.0 |
| **Core session API** (lower-level protocol API) | `<moq/moq.h>`, `<moq/session.h>`, `types.h`, `rcbuf.h`, `version.h` | the protocol engine; for relays, conformance, and app specialists who need direct control; pre-1.0 |
| **Core facade API** (lower-level conveniences, used under the service tier) | `publisher.h`, `subscriber.h`, `url.h` | publish/subscribe conveniences over the session; pre-1.0 |
| **Media helpers** | `loc.h`, `cmaf.h`, `msf.h`, `media_object.h`, `playback.h` | pre-1.0 media parse/pipeline helpers |
| **Simulation harness** (`moq::sim`) | `sim.h` | deterministic SimPair testing helper; installed only when built with `MOQ_BUILD_SIM=ON`; pre-1.0 |
| **Wire codec / tooling** | `codec.h`, `control.h`, `wire.h`, `buf.h`, `kvp.h` | draft-specific tooling for adapters/tests/tools, **not** the application API; details change across drafts |
| **picoquic adapter** (raw + threaded) | `picoquic.h`, `picoquic_threaded.h`, `picoquic_verify.h` | transport-specific adapter API; mature but pre-1.0 |
| **mvfst adapter** | `mvfst.h`, `mvfst.hpp` | transport-specific adapter API; mature but pre-1.0 |
| **WebTransport adapters** | `pico_wt.h`, `pico_wt_managed.h`, `proxygen_wt.hpp` | **experimental**, expect API churn |
| **Transport bridge** | `transport_bridge.h` | **private adapter SPI**, not a stable third-party/application ABI; symbols are exported from shared `moq-core` only so separately-built adapter DSOs can link them, versioned lockstep with the core and may change before 1.0/between minor releases; header excluded from install; exported set pinned by the `bridge_symbol_policy` test |

Rule of thumb: build media applications on the **media service API**; drop to
the **core session API** only when you need direct protocol control (relays,
conformance, specialized apps); treat the WebTransport adapters as experimental;
never include the wire-codec or transport-bridge headers as application API.
Adapter APIs are mature enough for integration pilots but may still change
before 1.0.

## Project structure

```
core/           moq-core: sans-I/O engine + shared transport bridge
  include/moq/  public C headers
  src/          implementation (session, codec, wire, bridge)
service/        moq-service: high-level media tier (endpoint, media
                receiver/sender) over the managed adapters
sim/            moq-sim: SimPair deterministic simulation + fault injection
adapters/       picoquic, mvfst, pico_wt, proxygen (+ shared conformance)
bindings/cpp/   moq-cpp: header-only C++20 binding
media/          loc, cmaf, msf catalog, media object, playback (parse helpers)
tests/          unit, scenario, simulation, and adapter conformance tests
fuzz/           fuzz targets and corpus
examples/       service/ (start here for media apps), plus core, picoquic,
                mvfst, pico_wt, and C++ binding examples
docs/           integration guide, adapter contract, conformance status
scripts/        boundary checks, coverage, seed sweeps
```

## Author

Raymond Lucke and the [Red5](https://www.red5.net/) Team

## License

Apache-2.0. See [LICENSE.txt](LICENSE.txt).
