# LibMoQ Benchmarks

Three sans-I/O benchmarks measuring different layers of the MoQ
session stack. No sockets, no picoquic, no threads — pure
encode/pump/decode performance.

For current measured numbers and how to read them, see
[`RESULTS_BASELINE.md`](RESULTS_BASELINE.md).

## moq_bench_core

Single-subscriber throughput. One SimPair, one track, N objects.
Baseline for session-layer object publish/receive cost.

```
moq_bench_core [options]
  --objects N            total objects to publish (default 10000)
  --object-size N        payload bytes per object (default 1200)
  --objects-per-group N  objects per group (default 30)
  --warmup N             warmup objects, excluded from timing (default 100)
  --json                 output results as JSON
```

## moq_bench_session_scale

Per-subscriber session scaling. Creates N independent
publisher/subscriber session pairs via SimPair and writes the same
payload to each. Measures how per-session encode/pump/decode cost
scales with subscriber count.

Each subscriber has its own independent server session — this does
NOT measure shared-publisher fanout or relay forwarding cost.

```
moq_bench_session_scale [options]
  --subscribers N        number of session pairs (default 4)
  --objects N            objects per session (default 1000)
  --object-size N        payload bytes per object (default 1200)
  --objects-per-group N  objects per group (default 30)
  --warmup N             warmup objects (default 50)
  --json                 output results as JSON
```

## moq_bench_process_fanout

Process-level fanout. One input object enters the process and is
forwarded to N downstream server sessions (one per client), each
via its own SimPair. The payload rcbuf is shared across all N
downstream writes — zero per-subscriber copy.

This models the relay/process forwarding pattern with today's
session APIs. A future `libmoq-relay` will eventually provide a
dedicated relay API and its own benchmark; this manual forwarding
harness measures the session-layer floor.

```
moq_bench_process_fanout [options]
  --subscribers N        downstream clients (default 4)
  --objects N            input objects (default 1000)
  --object-size N        payload bytes per object (default 1200)
  --objects-per-group N  objects per group (default 30)
  --warmup N             warmup objects (default 50)
  --json                 output results as JSON
```

## moq_bench_session_lookup

Single-session subscription-**alias lookup** scaling. Establishes M
subscriptions (M distinct aliases) on one client/server SimPair, then
delivers objects as datagrams round-robin across those aliases. Each
received datagram runs the receiver's alias lookup exactly once —
`sub_find_by_alias_subscriber`, a linear scan of `s->subs` — so per-object
cost scales with the subscription count. This is the O(n) receive path a
Phase 3 alias index would replace; vary `--subscriptions` to get the curve
(`run_benchmarks.sh` sweeps `MOQ_BENCH_LOOKUP_COUNTS`, default `64 256 1024`).

The other benches scale the number of independent sessions (one subscription
each); this is the only one that scales subscriptions on a single session,
which is why it needs SimPair's `client_max_subscriptions` to exceed the
default pool of 64.

```
moq_bench_session_lookup [options]
  --mode alias           lookup path to stress (only: alias)
  --subscriptions N      subscriptions on one session (default 64)
  --objects N            objects delivered (default 1000)
  --object-size N        payload bytes per object (default 1200)
  --objects-per-group N  objects per group (default 30)
  --warmup N             warmup objects (default 50)
  --json                 output results as JSON
```

**Not covered — subgroup stream-ref lookup.** `sg_find_by_stream_ref` is a
scan, but its only caller is `moq_session_on_data_stop` (the peer STOP/reset
handler) — it is **not** a steady-state per-object path. The object write path
resolves subgroups by O(1) packed handle, and the per-chunk receive path uses
an O(1) stream-ref index (`rx_find_by_ref` → `moq_index_find`). There is no
per-object stream-ref scan to benchmark, so this bench does not attempt one.

## moq_bench_session_subgroup

Steady-state **subgroup receive** allocation profile. One SimPair, one
subscription, one subgroup opened **once**; then N objects are written into
that single subgroup and it is closed **once** at the end. Unlike
`moq_bench_session_scale` — which opens and closes a fresh subgroup stream per
object — this isolates the per-object receive cost with stream setup/teardown
amortized to zero, so the reported `allocs/object` is the genuine steady-state
figure.

Measured steady-state cost is **1 alloc/object** (balanced frees, so no
leak/growth), independent of payload size — a single allocation for the
delivered payload rcbuf itself. Everything around it is zero-alloc on the
**receive** side (the send path is zero-copy — inline header, payload incref;
SimPair delivery in the no-fault path is zero-alloc borrowed `on_data_bytes`):

- The object payload is assembled **directly into** the delivered rcbuf's inline
  storage (allocated up front via the internal `moq_rcbuf_alloc_uninit`), so
  there is no separate assembly buffer and no copy into the event's rcbuf at
  emit.
- Input staging (`rx->input_buf`) is retained and reused across drains, so it is
  zero-alloc in steady state regardless of how a transport chunks its reads
  (SimPair delivers header and payload as separate `on_data_bytes` calls). The
  retained capacity stays charged to the receive budget until rx stream teardown
  / session destroy.

The one remaining allocation is the delivered payload rcbuf, which is handed to
the app and freed on `moq_event_cleanup`. Reaching zero would require pooling
that app-visible rcbuf, which is deferred pending an explicit cross-thread
ownership model (the service tier cleans up payloads on a different thread).

```
moq_bench_session_subgroup [options]
  --objects N       objects into one subgroup (default 5000)
  --object-size N   payload bytes per object (default 1200)
  --warmup N        warmup objects, excluded from timing (default 100)
  --json            output results as JSON
```

## Benchmark layers

## moq_bench_picoquic_loopback

Transport-level loopback benchmark. Single process with one picoquic
context acting as both server (publisher) and client (subscriber)
on localhost. Measures the full transport path: QUIC framing + TLS
encryption + adapter + session encode/decode over kernel loopback.

Requires TLS cert/key files. Build only when
`MOQ_BUILD_ADAPTER_PICOQUIC=ON`.

This measures **local loopback transport overhead**, not internet
latency or congestion behavior.

```
moq_bench_picoquic_loopback --cert <cert.pem> --key <key.pem> [options]
  --objects N           objects to publish (default 1000)
  --object-size N       payload bytes per object (default 1200)
  --warmup N            warmup objects (default 50)
  --port N              loopback port (default 14443)
  --json                output results as JSON
```

## Benchmark layers

| Benchmark | What it measures | Transport |
|-----------|------------------|-----------|
| `bench_core` | 1:1 session throughput | SimPair (sans-I/O) |
| `bench_session_scale` | Per-session overhead × N | SimPair (sans-I/O) |
| `bench_process_fanout` | Process-level fanout cost | SimPair (sans-I/O) |
| `bench_picoquic_loopback` | Transport path overhead | picoquic/UDP loopback |

The sans-I/O benchmarks measure session-layer encode/pump/decode
cost without transport. The picoquic benchmark adds QUIC framing,
TLS, and UDP socket I/O on the loopback interface.

## What these benchmarks do not measure

- Internet latency, jitter, or packet loss.
- Congestion control behavior under load.
- Properties or extension header encoding cost.
- Fetch or datagram paths.
- Relay library forwarding (future `libmoq-relay`).
- Multi-subscriber transport fanout (picoquic benchmark is 1:1).

## Cycle latency caveat

Cycle latency in `bench_session_scale` and `bench_process_fanout`
measures CPU processing time only. Useful for regression detection,
but does not predict real-world end-to-end latency.

## Refactor equivalence guard

These benchmarks measure *speed*; they do not prove a hot-path refactor
preserved behavior. For that, run `scripts/run_seed_sweeps.sh` before and
after the change — it is the deterministic equivalence guard (run-twice
trace-hash determinism plus zero allocator imbalance across the scenario
seed corpus). See `docs/simulation.md` ("Refactor Equivalence Guard").
The intended pairing for an O(n)->O(1) swap is: equivalence guard proves
no behavior change, benchmark curve proves the speedup.

## Build

```
# Sans-I/O benchmarks only
cmake -B build -DMOQ_BUILD_BENCHMARKS=ON
cmake --build build

# With transport benchmark (requires picoquic)
cmake -B build -DMOQ_BUILD_BENCHMARKS=ON -DMOQ_BUILD_ADAPTER_PICOQUIC=ON
cmake --build build
```

## Runner script

`scripts/run_benchmarks.sh` runs all three benchmarks with
configurable parameters. Assumes binaries are already built.

```
scripts/run_benchmarks.sh [build-dir]
```

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MOQ_BENCH_OBJECTS` | 10000 | Objects per benchmark |
| `MOQ_BENCH_OBJECT_SIZE` | 1200 | Payload bytes per object |
| `MOQ_BENCH_WARMUP` | 100 | Warmup objects |
| `MOQ_BENCH_JSON_DIR` | (none) | Write JSON results to this directory |

Session scaling and process fanout run at 1, 4, and 16 subscribers.
Exits nonzero if any benchmark fails.

```
# Plain text output
scripts/run_benchmarks.sh build

# JSON results to a directory
MOQ_BENCH_JSON_DIR=results scripts/run_benchmarks.sh build
```

When `MOQ_BENCH_JSON_DIR` is set, the runner also writes a
`metadata.json` with the git commit, parameters, and subscriber
counts alongside the per-run JSON files.

## How to report results

1. Copy `benchmarks/RESULTS_TEMPLATE.md` and fill in the tables
   from a benchmark run.
2. Record the git commit, build type, compiler, and CPU.
3. Include caveats: these are sans-I/O numbers (no transport, no
   encryption). They measure session-layer cost only.

For JSON-based comparison, run with `MOQ_BENCH_JSON_DIR` and
archive the output directory. The `metadata.json` file records the
commit and parameters for reproducibility.

## Smoke tests

When `MOQ_BUILD_TESTS` is also enabled, CTest smoke tests run each
benchmark in a tiny deterministic mode. They do not assert
throughput thresholds.
