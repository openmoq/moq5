# MOQ5 Relay

A deterministic Media over QUIC relay. The protocol core is sans-I/O and the
relay serves draft-16 and draft-18 from one listener, translating terminal
codes by meaning rather than forwarding raw numbers between registries.

The installed command is `moq5-relay`.

Forwarding code lives in the top-level `relay/` component. The command under
`tools/moq5-relay/` composes those transport-independent libraries with
configuration, transports, process lifecycle, and the admin endpoint. Relay
libraries can also be installed without the command or any transport adapter.
See [the embedding guide](../../relay/README.md) for the public components.

## Build

The relay is off by default. Enable it, and the managed MsQuic transport it
runs on, at configure time:

```sh
cmake -B build -DMOQ_BUILD_RELAY=ON \
               -DMOQ_BUILD_ADAPTER_MSQUIC=ON \
               -DMOQ_BUILD_MSQUIC_MANAGED=ON
cmake --build build
cmake --install build --prefix /usr/local
```

## Run

```sh
moq5-relay serve --config relay.json
```

`capacity` resolves the same configuration and prints the ceiling the process
would run under, without starting a listener:

```sh
moq5-relay capacity --config relay.json
```

`moq5-relay --help`, `moq5-relay help serve` and `moq5-relay help capacity`
describe the command line; `moq5-relay --version` prints the version.

## Two listeners, one relay

A `webtransport` object adds a second listener to the same relay process. Both
listeners feed one shard runtime, so a publisher arriving over raw MoQ can serve
a subscriber arriving from a browser over WebTransport, and the reverse. Each
listener owns a disjoint range of shards; a configuration whose combined lane
count exceeds the runtime cap is refused before either listener is created, and
if the second listener cannot start, the first is torn down and the relay
reports no readiness.

The WebTransport profile is a transport dialect, not a MoQ version: the MoQ
draft is still chosen by the ordered subprotocol list.

Builds without wtquic keep working unchanged and reject a `webtransport`
config with a clear message.

## Configuration

`examples/relay.json` is a complete, inert example — it names no host-specific
path and contains no credential. `listener.cert` and `listener.key` are
required by `serve` and are read relative to the working directory unless given
as absolute paths.

| key | meaning |
|---|---|
| `listener.host`, `listener.port` | where the relay listens |
| `listener.versions` | ordered draft set; default `[18]`, use `[18, 16]` to serve both drafts from one listener |
| `listener.lanes` | independent shard lanes; more than one enables the cross-shard demand plane |
| `listener.cert`, `listener.key` | server credential, PEM |
| `webtransport` | optional second listener carrying MoQ over WebTransport, for browsers; absent means raw-only, exactly as before |
| `webtransport.host`, `.port`, `.cert`, `.key` | its own address and credential, separate from the raw listener |
| `webtransport.lanes` | its own shard lanes; `listener.lanes + webtransport.lanes` must not exceed the runtime cap |
| `webtransport.path` | request path browsers connect to, default `/moq` |
| `webtransport.versions` | ordered drafts offered as WebTransport subprotocols, default `[18, 16]` |
| `webtransport.profile` | WebTransport-over-HTTP/3 dialect: `current` (default), `d13_14_compat`, or opt-in `d02_rfc9297_compat`; a selection of what the listener emits, not a claim about any peer |
| `webtransport.origin_policy` | how a client's serialized Origin is authorized: `unset` (default, no Origin rule), `allow_any_non_opaque`, `allowlist`, or `allow_any_including_null`; required when `profile` is `d02_rfc9297_compat` |
| `webtransport.allowed_origins` | 1 to 8 exact Origin strings, at most 320 bytes each excluding its terminator and 512 bytes in total **including** every terminator; accepted only with `origin_policy: "allowlist"`, and required by it. Compared byte for byte, never normalized |
| `admin` | optional loopback HTTP endpoint serving `GET /metrics`, `GET /api/v1/info` (the frozen configuration document) and `GET /api/v1/shards` (one generation's per-shard counters as JSON); absent means no endpoint, no socket and no thread. The endpoint is part of relay health: if its owner stops for any reason but shutdown, the relay stops rather than advertise a dead endpoint |
| `admin.tcp.host`, `admin.tcp.port` | where it listens; the host must be a loopback literal (default `127.0.0.1`) and the port is required |
| `budgets.*` | pool ceilings resolved up front, so an oversized configuration is refused before the listener starts |
| `telemetry.trace_ring_records` | bounded flight-recorder depth |
| `logging.format` | `text` (default: the serve's stdout lines and stderr diagnostics exactly as today) or `json`: the serve's stdout carries one JSON event per line (`RELAY_READY_V1`, `RELAY_RUN_CONFIG_V1`, `RELAY_LANE_STATS_V3`, `RELAY_PAIR_STATS_V1`, `RELAY_STOP_V1`, each with a `schema` and an `elapsed_us` member) and every human message, the capacity ceiling included, moves to stderr; on an output failure no later event is written and the last line may be partial; other commands stay text; refused by the verify and measure builds |
| `auth.mode` | `allow_all` or the bundled deterministic `toy` policy |

Machine-readable rows (`RELAY_RUN_CONFIG_V1`, `RELAY_PAIR_STATS_V1`) are the
stable contract for tooling. Human diagnostics are prefixed `MOQ5 Relay:` and
are not a parsing surface.

See `moq5-relay(1)` for the command reference,
`moq5-relay.json(5)` for the configuration format, and
`../../relay/docs/architecture.md` for the internal relay design.
