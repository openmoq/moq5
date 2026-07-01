# Benchmark Results — [date]

## Environment

| Field | Value |
|-------|-------|
| Commit | `abcdef0` |
| Build | Release / `-O2` |
| Compiler | Apple clang 16.0 |
| CPU | Apple M2 |
| OS | macOS 15.x arm64 |
| Command | `scripts/run_benchmarks.sh build` |
| Objects | 10000 |
| Object size | 1200 B |
| Warmup | 100 |

## moq_bench_core (1:1 throughput)

| Metric | Value |
|--------|-------|
| objects/sec | |
| Mbps | |
| elapsed ms | |
| allocs | |
| peak delta B | |

## moq_bench_session_scale (per-session scaling)

| Subscribers | obj/s | Mbps | elapsed ms | cycle lat avg us | allocs | peak delta B |
|-------------|-------|------|------------|------------------|--------|--------------|
| 1 | | | | | | |
| 4 | | | | | | |
| 16 | | | | | | |

## moq_bench_process_fanout (process-level fanout)

| Subscribers | obj/s | Mbps | elapsed ms | cycle lat avg us | allocs | peak delta B |
|-------------|-------|------|------------|------------------|--------|--------------|
| 1 | | | | | | |
| 4 | | | | | | |
| 16 | | | | | | |

## moq_bench_picoquic_loopback (transport overhead)

| Metric | Value |
|--------|-------|
| objects/sec | |
| Mbps | |
| elapsed ms | |
| port | |

## moq_bench_session_lookup (single-session alias-lookup scaling)

Per-object cost vs. subscription count on one session (O(n) receiver
alias scan). One row per `MOQ_BENCH_LOOKUP_COUNTS` value.

| subscriptions | objects/sec | cycle latency us (avg) | allocs | peak_bytes_delta |
|---------------|-------------|------------------------|--------|------------------|
| 64 | | | | |
| 256 | | | | |
| 1024 | | | | |

## moq_bench_session_subgroup (steady-state subgroup receive)

One subgroup opened once, N objects written into it, closed once. Isolates
the per-object receive allocation profile with stream setup/teardown amortized
out. Steady-state `allocs / objects` is the headline (currently 1/object).

| object size | objects/sec | cycle latency us (avg) | allocs/object | peak_bytes_delta |
|-------------|-------------|------------------------|---------------|------------------|
| 64 | | | | |
| 1200 | | | | |

## Notes

- Sans-I/O benchmarks (core, session_scale, process_fanout,
  session_lookup, session_subgroup) measure session-layer
  encode/pump/decode cost only — no QUIC transport, no encryption, no
  network latency.
- session_subgroup isolates steady-state subgroup *receive* cost (one
  subgroup, N objects). Its 1 alloc/object is the delivered payload rcbuf
  itself: the object is assembled directly into that rcbuf's inline storage
  (no separate buffer, no copy), and input staging (`rx->input_buf`) is
  retained and reused across drains. Reaching zero would require pooling the
  app-visible rcbuf (deferred; cross-thread ownership).
- session_lookup is the only bench that scales subscriptions on a single
  session; the alias-lookup latency should rise with subscription count
  (the O(n) scan). An alias index would flatten that curve.
- The picoquic loopback benchmark adds QUIC framing, TLS encryption,
  and UDP socket I/O on the kernel loopback interface. It does NOT
  measure internet latency or congestion behavior.
- Cycle latency measures CPU processing time per input object across
  all subscribers. Not predictive of real-world end-to-end latency.
- session_scale and process_fanout have the same SimPair topology
  (N independent pairs). The distinction is semantic: session_scale
  models N independent publishers; process_fanout models one input
  forwarded to N downstream sessions with a shared payload rcbuf.
- Once libmoq-relay exists, process_fanout should be supplemented
  with a relay-API benchmark.
