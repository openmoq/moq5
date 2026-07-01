# LibMoQ performance baseline

Durable baseline from fresh local runs of the sans-I/O benchmarks, backing the
performance claims from the reviewer comparison. These numbers are
**indicative** — one machine, single-threaded deterministic simulation, no real
transport. Treat the **allocation counts** and the **average** per-object
latency as the durable signals; wall-clock throughput and **max** latency are
scheduler-noisy and vary run to run.

## Environment

| Field | Value |
|-------|-------|
| Date | 2026-06-30 |
| Machine | Apple M1 Max, macOS (Darwin 24.6.0), arm64 |
| Build mode | Release (`-DCMAKE_BUILD_TYPE=Release`) |
| Transport | none — deterministic in-process SimPair (sans-I/O core) |

## Build

```sh
cmake -B build/bench-release \
  -DCMAKE_BUILD_TYPE=Release -DMOQ_BUILD_BENCHMARKS=ON -DMOQ_BUILD_SIM=ON
cmake --build build/bench-release -j
```

## 1. Single-session alias-lookup scaling (`bench_session_lookup`)

Stresses the receiver-side track-alias lookup with many subscriptions on one
session — the O(n) scan replaced by an O(1) open-addressing index
(`idx_sub_by_alias`).

```sh
build/bench-release/benchmarks/moq_bench_session_lookup \
  --mode alias --subscriptions <N> --objects 10000 --object-size 1200 \
  --warmup 100 --json
```

| subscriptions | objects/sec | cycle latency avg (µs) | cycle latency max (µs) | allocs | peak Δ (B) |
|---------------|-------------|------------------------|------------------------|--------|-----------|
| 64   | 2,789,400 | 0.3 | 19 | 10000 | 1280 |
| 256  | 2,836,879 | 0.3 | 1  | 10000 | 1280 |
| 1024 | 2,926,544 | 0.3 | 3  | 10000 | 1280 |
| 4096 | 2,748,008 | 0.3 | 33 | 10000 | 1280 |

**Read:** *average* latency is **flat (0.3 µs) from 64 to 4096 subscriptions** —
the alias lookup no longer scales with subscription count (O(1) index). The
*max* column and throughput wobble are scheduler noise; the flat **avg** is the
scaling signal. `allocs == objects` (1/object) is the datagram receive payload
copy from the borrowed `on_datagram` buffer, which is required.

## 2. Steady-state subgroup receive (`bench_session_subgroup`)

One subscription, one subgroup opened once, N objects — isolates per-object
receive cost with stream setup/teardown amortized out.

```sh
build/bench-release/benchmarks/moq_bench_session_subgroup \
  --objects 10000 --object-size 1200 --warmup 100 --json
```

| objects | received | lost | objects/sec | latency avg (µs) | allocs | allocs/object | peak Δ (B) |
|---------|----------|------|-------------|------------------|--------|---------------|-----------|
| 10000 | 10000 | 0 | 2,458,210 | 0.4 | 10000 | **1.00** | 1280 |

**Read:** **1 alloc/object** — the object is assembled directly into the
delivered rcbuf's inline storage (no separate staging buffer, no copy at emit),
and input staging is reused across drains. The one remaining allocation is the
**owned, app-visible payload rcbuf** the receiver frees normally. This is
copy-free owned delivery, **not** zero-by-default.

## 3. Zero-allocation relay fan-out (`bench_relay_fanout`)

One upstream publisher → relay ingest → M downstream subscribers, all on one
simulated shard sharing an opt-in per-shard recycling allocator. The relay
republishes the ingested rcbuf by reference (incref, no clone).

```sh
build/bench-release/benchmarks/moq_bench_relay_fanout \
  --fanout <F> --objects 5000 --object-size <S> --warmup 200 --json
```

| fanout | object size | downstream received | lost | objects/sec | allocs after warmup | frees after warmup | peak Δ (B) |
|--------|-------------|---------------------|------|-------------|---------------------|--------------------|-----------|
| 1  | 64   | 5000   | 0 | 1,506,024 | **0** | **0** | 288  |
| 1  | 1200 | 5000   | 0 | 681,013   | **0** | **0** | 2560 |
| 8  | 64   | 40000  | 0 | 1,692,047 | **0** | **0** | 288  |
| 8  | 1200 | 40000  | 0 | 1,008,115 | **0** | **0** | 2560 |
| 64 | 64   | 320000 | 0 | 2,007,415 | **0** | **0** | 288  |
| 64 | 1200 | 320000 | 0 | 878,797   | **0** | **0** | 2560 |

**Read:** after warm-up the **libmoq allocator does 0 malloc / 0 free on the
relay hot path**, across fan-out 1→64 and both object sizes, with every object
delivered. This is via an **opt-in per-shard recycling allocator supplied
through the existing `moq_alloc_t` seam** — it is **not** default process-wide
zero-malloc, and it counts only allocations flowing through libmoq's allocator
(a real transport's own allocator is separate). `peak Δ` tracks object size (the
recycled working set), not fan-out.

## 4. mvfst real-transport delivery latency (gauntlet `real_transport_wall`)

**Different metric class — do NOT compare to the sans-I/O numbers above.** This
is end-to-end wall time over a real localhost direct-QUIC (moqt-16) exchange
through the mvfst adapter, measured by the shared `moq-gauntlet` harness, not the
in-process benchmark suite. It is recorded here to validate the **managed mvfst
client-pump fix** (commit `2eb27bc`, which replaced the client pump's fixed 5 ms
polling cadence with an EventBase-driven pump) — it is **not** a moxygen
head-to-head or a general throughput claim.

Localhost single-object / tiny-fanout QUIC is dominated by connection setup and
scheduler noise, so read the **median**, and only compare rows measured in the
same run on the same machine.

```sh
# moq-gauntlet real-transport lane (libmoq mvfst driver), 10 reps/scenario:
scripts/run_transport_perf.sh build-transport-rel 10
```

| scenario | objects | 5 ms poll (before) | 200 µs diagnostic | EventBase pump (`2eb27bc`) |
|----------|---------|--------------------|-------------------|----------------------------|
| `cache_hit_single`       | 1   | 6,350,000 ns | 268,000 ns | **110,500 ns** |
| `transport_fanout_smoke` | 6   | 1,045,000 ns | 43,000 ns  | **47,500 ns**  |
| `transport_warm_fanout`  | 192 | 132,000 ns/obj | 7,950 ns/obj | **6,180 ns/obj** |

**Read:** the fixed 5 ms client-pump cadence added a per-exchange latency floor
that dominated small transfers (a 1-object cache hit paid ~6.35 ms of pure poll
latency). The EventBase-driven pump removes that floor entirely — ~47–56× faster
across the three scenarios, and slightly **better** than the temporary 200 µs
diagnostic poll (which only proved the cause) because the event-driven path has
no fixed interval at all. Warm fan-out settles at ~6.2 µs/object, at/under the
diagnostic's ~8 µs/object. The `200 µs diagnostic` column is the throwaway probe
used to localize the bottleneck; it never shipped. These are single-machine,
indicative wall figures — re-run the lane on the target platform for real
numbers.

## Other performance notes

- **Staged-datagram replay** (`staged_replay`, session.c): a selection-sort-shaped
  min-search, but bounded by a hard `MOQ_STAGED_DG_SLOTS = 16` cap with
  drop-oldest eviction, so worst case is ~16×16 = 256 comparisons — a fixed
  constant, **audited out** as not a scaling concern and not peer-scalable. It
  applies only to datagrams staged before a track alias binds in
  relay/forwarding scenarios; normal known-alias datagrams bypass it.
- **No head-to-head real-QUIC benchmark** against moxygen (or any other
  implementation) exists yet. Sections 1–3 are sans-I/O simulation numbers;
  section 4 is a real-transport *self*-comparison (before/after the pump fix),
  not a cross-implementation result. A fair over-the-wire moxygen comparison is
  separate, future work.

## Caveats

- Single machine, single-threaded deterministic simulation; absolute throughput
  is indicative only. Re-run on the target platform for real figures.
- Latency is in-process CPU time per input object, not end-to-end network
  latency.
- Numbers correspond to the tree at the time of writing; re-generate after
  material receive/allocation changes.
