# Benchmark Results — Baseline (2026-05-20)

## Environment

| Field | Value |
|-------|-------|
| Commit | `c0169ec` |
| Build | Default (no explicit `-O2`; CMake default) |
| Compiler | Apple clang 17.0.0 (clang-1700.6.4.2) |
| CPU | Apple M1 Max |
| OS | macOS 15.7.3 arm64 |
| Objects | 10000 (core) / 1000 (scaling/fanout/loopback) |
| Object size | 1200 B |
| Warmup | 100 (core) / 50 (others) |

## moq_bench_core (1:1 throughput)

| Metric | Value |
|--------|-------|
| objects/sec | 415,472 |
| Mbps | 3,804 |
| elapsed ms | 24.07 |
| allocs | 50,000 (5 per object) |
| peak delta B | 3,680 |

## moq_bench_session_scale (per-session scaling)

| Subscribers | obj/s | Mbps | elapsed ms | cycle lat avg us | allocs | peak delta B |
|-------------|-------|------|------------|------------------|--------|--------------|
| 1 | 444,642 | 4,071 | 2.25 | 2.2 | 5,000 | 3,680 |
| 4 | 440,626 | 4,034 | 9.08 | 9.0 | 20,000 | 3,680 |
| 16 | 433,158 | 3,966 | 36.94 | 36.9 | 80,000 | 3,680 |

## moq_bench_process_fanout (process-level fanout)

| Subscribers | obj/s | Mbps | elapsed ms | cycle lat avg us | allocs | peak delta B |
|-------------|-------|------|------------|------------------|--------|--------------|
| 1 | 455,996 | 4,175 | 2.19 | 2.2 | 5,000 | 3,680 |
| 4 | 449,489 | 4,115 | 8.90 | 8.9 | 20,000 | 3,680 |
| 16 | 432,281 | 3,958 | 37.01 | 37.0 | 80,000 | 3,680 |

## moq_bench_picoquic_loopback (transport overhead)

| Metric | Value |
|--------|-------|
| objects/sec | 50,472 |
| Mbps | 462 |
| elapsed ms | 19.81 |
| port | 14980 |

## Notes

- All sans-I/O benchmarks measure session-layer encode/pump/decode
  cost only — no QUIC transport, no encryption, no network latency.
- The picoquic loopback benchmark adds QUIC framing, TLS encryption,
  and UDP socket I/O on the kernel loopback interface.
- **Sans-I/O → transport overhead ratio: ~8.2x** (415K → 50K obj/s).
  This is the QUIC/TLS/socket cost on loopback.
- Throughput scales linearly with subscriber count (session_scale
  and process_fanout are within 3% of each other — expected since
  both use N independent SimPairs).
- Cycle latency scales linearly at ~2.3 us/subscriber.
- Peak memory delta is constant at 3,680 B regardless of subscriber
  count — steady-state per-object allocation is fully reclaimed.
- 5 allocations per object per subscriber (open_subgroup, write_object
  header+payload, close_subgroup, receive buffer, event).
