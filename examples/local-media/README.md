# Local Media Demo

Publisher → relay → client over real QUIC using picoquic and LOC-01
object properties. The flagship runnable demo for libmoq.

## Quick start

```sh
cmake -B build/pq \
  -DMOQ_BUILD_ADAPTER_PICOQUIC=ON \
  -DMOQ_BUILD_LOC=ON \
  -DMOQ_PICOQUIC_SOURCE_DIR=/path/to/picoquic
cmake --build build/pq
scripts/run_local_media_demo.sh build/pq
```

The script generates ephemeral TLS certificates, auto-picks ports,
runs 3 consecutive passes, and exits 0 only if all pass.

## Expected output

```
=== Local Media Demo (3 passes, 60 frames/pass) ===

--- Pass 1 (pub=12502 relay=12503) ---
PASS: pass 1 (relay_fwd=60, client_rx=22)
--- Pass 2 (pub=12504 relay=12505) ---
PASS: pass 2 (relay_fwd=60, client_rx=25)
--- Pass 3 (pub=12506 relay=12507) ---
PASS: pass 3 (relay_fwd=60, client_rx=23)

PASS: local_media_demo (3/3 passes)
```

## Architecture

```
  ┌────────────┐         ┌───────────┐         ┌────────────┐
  │  publisher │◀──QUIC──│   relay   │◀──QUIC──│   client   │
  │  (server)  │         │ (client + │         │  (client)  │
  └────────────┘         │  server)  │         └────────────┘
   emits frames          └───────────┘          subscribes to
   with LOC props         subscribes             relay, parses
   (timestamp +           upstream as            LOC props,
    frame marking)        client, serves         validates seq
                          downstream
```

## What each process proves

- **Publisher**: LOC-01 encode (timestamp + video frame marking),
  `moq_publisher_t` facade with ACCEPT_ALL, 30fps paced emission,
  graceful drain after final frame.
- **Relay**: raw session API for cross-connection forwarding,
  deferred upstream subscribe (waits for downstream client),
  properties preserved through `write_object_ex`.
- **Client**: `moq_subscriber_t` facade, LOC-01 parse, monotonic
  sequence validation, keyframe cadence check (every 10th frame),
  deterministic PASS/FAIL exit code.

## Zero-allocation relay hot path (opt-in)

The relay can run its libmoq allocations through an **optional single-shard
recycling pool** to demonstrate the zero-allocation relay pattern. It is
**opt-in** — pass `--pool` on the relay command line, or set
`MOQ_DEMO_RELAY_POOL=1`; the default remains the global allocator, so normal
demo behavior is unchanged.

```sh
MOQ_DEMO_RELAY_POOL=1 scripts/run_local_media_demo.sh build/pq
```

Each pass then prints the relay's hot-path report, e.g.:

```
  relay: pool ENABLED. libmoq allocator hot path after 34 warmup objects:
         real mallocs=0  real frees=0  peak delta=800 B  live at shutdown=... B
         (counts libmoq's moq_alloc_t only -- picoquic's own allocator and
          process-wide malloc are NOT included)
```

Warm-up is **adaptive**: the counters reset only once the pool has stopped
growing (no new real malloc for a stretch of forwarded objects), so the count
of excluded warm-up objects varies run to run while the measured steady-state
stays a deterministic zero. Media object sizes and the per-group subgroup state
first-touch their size classes at times that would race a fixed window; waiting
for growth to cease removes that intermittency.

**What this shows:** after warm-up, libmoq performs **zero malloc/free on the
relay hot path** for a single-shard relay. The key ideas:

- The inbound object's payload arrives as one libmoq rcbuf slab; the pool
  **recycles that slab** (freed back to a size-class free-list, popped for the
  next object), so no heap allocation recurs per object.
- **Fan-out is an rcbuf incref**, not a payload clone — `forward_one()`
  republishes the exact upstream `payload`/`properties` rcbufs downstream via
  `write_object_ex`/`write_object`. No per-subscriber copy.
- The relay is **single-shard** (one picoquic packet-loop thread), so the
  non-atomic rcbuf refcounts and the non-atomic pool free-list are safe with no
  locks or atomics.

**Scope of the claim — read carefully.** The counter measures allocations that
flow through libmoq's `moq_alloc_t` only (the sessions and the picoquic adapter
wrappers). **picoquic's own internal allocator and any other process-wide
`malloc` are not counted.** This demonstrates *libmoq's allocator hot path* is
zero-allocation, not that the whole process is. The pool here is an
**example-local** allocator for illustration, not a libmoq API.

## Inspecting logs on failure

On any failure the script dumps all three process logs to stderr
before exiting. To preserve logs for manual inspection:

```sh
scripts/run_local_media_demo.sh build/pq 2>demo.log
# On failure, demo.log contains client/publisher/relay stderr
```

## CTest integration

The demo is registered as CTest `local_media_demo` in picoquic+LOC
builds. It runs a single pass (not 3) under CTest to keep CI fast:

```sh
ctest --test-dir build/pq -R local_media_demo --output-on-failure
```
