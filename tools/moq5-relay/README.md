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

The relay command needs the raw managed MsQuic adapter even when a WebTransport
listener is configured. `MOQ_BUILD_RELAY=ON` alone builds the transport-independent
relay libraries and tests, not the command. Configure reports when the command
is disabled.

An installed MsQuic CMake package can be selected with `msquic_DIR`. For a
private source build, run this from the repository root (requires Git, CMake,
a C/C++ toolchain, Perl and make; Linux also needs libnuma development headers):

```sh
bash scripts/setup_msquic_deps.sh
. .deps/msquic-ci/msquic_deps.env
```

The script pins MsQuic 2.5.9 and applies the checked-in strict certificate-return
and C11-header corrections. It fetches only the required QuicTLS and clog
submodules, uses `QUIC_TLS_LIB=quictls`, and installs privately without sudo.
Build sources and logs are retained under `.deps/msquic-ci/build.*`.

Build a raw-QUIC relay:

```sh
cmake -S . -B build-relay -Dmsquic_DIR="$msquic_DIR" \
               -DMOQ_BUILD_RELAY=ON \
               -DMOQ_BUILD_ADAPTER_MSQUIC=ON \
               -DMOQ_BUILD_MSQUIC_MANAGED=ON
cmake --build build-relay --target moq5-relay
```

For both raw and WebTransport listeners, build WTQuic against the same provider,
then configure a separate tree:

```sh
msquic_DIR="$msquic_DIR" bash scripts/setup_wtquic_deps.sh
. .deps/wtquic-ci/wtquic_deps.env
cmake -S . -B build-relay-dual \
    -Dmsquic_DIR="$msquic_DIR" -Dwtquic_DIR="$wtquic_DIR" \
    -DMOQ_BUILD_RELAY=ON -DMOQ_BUILD_ADAPTER_MSQUIC=ON \
    -DMOQ_BUILD_MSQUIC_MANAGED=ON -DMOQ_BUILD_ADAPTER_WTQUIC=ON \
    -DMOQ_BUILD_WTQUIC_MSQUIC_MANAGED=ON
cmake --build build-relay-dual --target moq5-relay
```

The WTQuic recipe pins `da239546198d99080c120f32448cf3d8b8fc008e`, which includes
the upstream typed-callback portability fix. It applies no WTQuic patches and
keeps pedantic warnings enabled. Its MsQuic backend requires at least 2.5.9. `wtquic_DIR` is discovered
from the actual installation, including `lib64` layouts. `WTQ_MSQUIC_ROOT` is
an alternative for a source/build tree; it is not required with an installed
`msquic_DIR`. Both scripts accept individual extra CMake configure arguments.

Use `cmake --install <build-dir> --prefix <destination>` to install. The private
provider is not copied into the relay installation: arrange for the platform
loader to find it as well as any shared WTQuic libraries. Do not point a server
at a different provider merely to resolve a missing-library error.

Successful compilation is not browser compatibility evidence. Match the peer's
WebTransport profile explicitly, supply a certificate trusted by the client
with a SAN matching its DNS name or IP address, and keep certificate and Origin
validation enabled. A failure before MoQ readiness can originate in TLS,
HTTP/3, WebTransport or MoQ; a zero MoQ-session count does not locate it.

For local testing, match the destination address family to the listener:
`localhost` may resolve to `::1` first while a listener bound to `127.0.0.1`
accepts IPv4 only. Use `https://127.0.0.1:<port>/moq` with an IP SAN, or bind
the listener to `::1` and use `https://localhost:<port>/moq` with a localhost
DNS SAN. Neither disabling verification nor widening Origin policy fixes a
connection sent to an address on which the relay is not listening. The service
API's `moq_endpoint_get_terminal` distinguishes transport/TLS failures from a
MoQ protocol fatal; `moq_endpoint_fatal_code == 0` alone does not mean success
or prove that the relay refused a session.

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

An explicit Origin policy requires an `Origin` header.
`allow_any_including_null` permits the serialized value `null`; it does not
permit the header to be absent. A native publisher that sends no Origin will
therefore receive HTTP 403 even under that policy. The current PicoQUIC service
client does not expose an Origin input. For an intentionally unrestricted native
listener using `d13_14_compat`, `unset` makes no Origin requirement; this is an
operator policy choice, not a repair to certificate verification or a substitute
for authentication. Keep a required Origin policy intact and use a client that
can supply it when that policy is part of the deployment's security boundary.
Draft-02 compatibility still requires Origin and its draft-specific request
marker; it is not a fallback for a draft-13/14 native client.

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
