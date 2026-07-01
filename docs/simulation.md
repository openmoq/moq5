# Deterministic Simulation

LibMoQ includes a separate `moq-sim` library for FoundationDB-style
deterministic testing. Production embedders link only `moq-core`;
tests and conformance harnesses link `moq-sim` as well.

## SimPair

`moq_simpair_t` wires two sessions back-to-back with no network. One
session's outbound actions become the other's inbound inputs, pumped
deterministically by the test.

```c
#include <moq/sim.h>

moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
cfg.alloc = &alloc;
cfg.seed = 0xCAFE;
cfg.initial_now_us = 1000;
cfg.client_send_request_capacity = true;   
cfg.client_initial_request_capacity = 10;  
cfg.server_send_request_capacity = true;   
cfg.server_initial_request_capacity = 10;  

moq_simpair_t *sp = NULL;
moq_simpair_create(&cfg, &sp);
moq_simpair_start(sp);           /* initiates setup handshake */
moq_simpair_run_until_quiescent(sp, 8, NULL);
/* Both sessions are now ESTABLISHED. */
```

### What SimPair routes

| Action kind | Routed to | Input function |
|------------|-----------|----------------|
| SEND_CONTROL | Peer session | `on_control_bytes` |
| SEND_DATA | Peer session | `on_data_bytes` (header then payload separately) |
| RESET_DATA | Peer session | `on_data_reset` |
| STOP_DATA | Sender session | `on_data_stop` (plus removes stream mapping) |
| SEND_DATAGRAM | Peer session | `on_datagram` |
| CLOSE_SESSION | — | Traced only; no peer-close input API exists |

SEND_DATA is fed as separate header and payload chunks (no fixed-size
combined buffer) to exercise the incremental parser.

SimPair maintains a 16-entry stream_ref mapping table to translate sender
stream_refs to receiver stream_refs. Mappings are created on first
SEND_DATA and removed on FIN or reset.

### Seeds

Every SimPair takes a `seed`. The seed is carried in trace records for
identification and drives the deterministic scenario runners. The first
BUGGIFY-style runner (`scenario_faults`) also derives allocator fault
decisions from the seed.

The same seed with the same inputs always produces the same outputs.
This is the core determinism guarantee.

### Virtual Time

SimPair tracks virtual time via `moq_simpair_now_us`. Time only
advances when the test calls `moq_simpair_advance_to`. No system
clock is ever read.

### Stepping

- `moq_simpair_step` pumps one round: client actions to server, then
  server actions to client. Returns the number of actions delivered.
- `moq_simpair_run_until_quiescent` pumps steps until no actions are
  delivered in a round, or a step limit is reached.

## Trace Callbacks

SimPair emits structured trace records through an optional callback:

```c
cfg.trace_fn = my_trace_callback;
cfg.trace_ctx = &my_state;
```

Each record contains:

| Field | Description |
|-------|-------------|
| `seed` | SimPair seed |
| `step` | Monotonic step counter |
| `now_us` | Current virtual time |
| `kind` | INPUT, ACTION, or QUIESCENT |
| `from` / `to` | CLIENT or SERVER perspective |
| `action_kind` | SEND_CONTROL, SEND_DATA, RESET_DATA, STOP_DATA, CLOSE_SESSION |
| `input_kind` | START, CONTROL_BYTES, TICK, DATA_BYTES, DATA_RESET, DATA_STOP |
| `bytes` | Actual wire bytes (borrowed until callback returns) |
| `code` | Close/reset/stop error code |
| `count` | Payload size for SEND_DATA actions |
| `result` | Return code of the operation |

The callback must not reenter SimPair. The `bytes` pointer is valid
only for the duration of the callback.

## Trace Determinism

The test suite verifies determinism by running the same scenario twice
with the same seed and comparing a semantic hash of all trace records.

The hash (FNV-1a) covers scheduling fields, record types, result codes,
and the actual byte content of wire messages. It does not include raw
pointer values or handle bits.

This catches protocol divergence that preserves message shapes but
changes request IDs, track aliases, error codes, or namespace content.
Two scenarios with identical message counts but different wire bytes
produce different hashes.

## OOM Sweep

The test suite includes an OOM sweep harness (`test_oom.c`) that runs
each scenario with a fail-at-N allocator. For every scenario:

1. Run once with no failures to learn the total allocation count.
2. For N = 1..count, fail exactly the Nth allocation.
3. Verify that every failure path returns an error or closes
   deterministically, and destroy frees all memory (balance == 0).

Current OOM scenarios: session create/destroy, setup handshake,
subscribe happy path, subscribe reject, object receive, and
publish namespace.

## Refactor Equivalence Guard

`scripts/run_seed_sweeps.sh` is the deterministic equivalence guard for
hot-path refactors — the required before/after check when swapping a
data structure without changing behavior (e.g. an O(n) alias scan or
subgroup stream-ref scan for an O(1) index, or adding a freelist/pool).

It drives the seeded scenario runners across a seed corpus, and each
runner already self-verifies the two properties a behavior-preserving
refactor must hold:

- **Trace-hash determinism.** Every seed runs twice and the FNV-1a
  semantic trace hash (see "Trace Determinism") must match, so any
  change in scheduling, message shapes, wire bytes, request IDs, track
  aliases, or error codes is caught.
- **Allocator balance.** The runners use a counting allocator and fail
  on a non-zero balance at teardown; the OOM sweep additionally proves
  balance == 0 on every fail-at-N path.

Run it before and after the change; the result must be identical (same
seeds pass, no trace-hash divergence, zero allocator imbalance):

```
# Point at the build root that contains the scenario binaries
# (tests/test_scenario_*). Default is `build`; pass --build-dir otherwise.
scripts/run_seed_sweeps.sh --profile quick   --build-dir build
scripts/run_seed_sweeps.sh --profile standard --build-dir build   # + ASan subset
```

Profiles: `quick` (~5s, 1k seeds), `standard` (~30s, 10k seeds + ASan
1k subset), `nightly` (~5min, 100k seeds + ASan 10k subset). Use
`--dry-run` to print the commands, and `--runner NAME --seeds N` to
sweep a single runner. When a seed diverges, replay it in isolation
with `scripts/replay_scenario.sh` (see "Replay Tool").

A refactor that changes a trace hash or leaves a non-zero allocator
balance has changed behavior and must be reworked or justified — the
guard does not measure performance, only equivalence. Pair it with the
benchmarks (see `benchmarks/README.md`) to confirm the intended speedup.

## Deterministic Scenario Runners

The test suite includes twenty seeded PRNG-driven scenario runners
that generate randomized operation sequences instead of replaying
hand-written cases. Each seed produces a deterministic sequence
from a splitmix64 PRNG, with invariants checked after every step.

### scenario_auth

`tests/scenario/test_scenario_auth.c` generates adversarial
AUTH_TOKEN operation sequences: REGISTER, USE_ALIAS, DELETE,
USE_VALUE token actions across SUBSCRIBE messages fed as raw
encoded control bytes. A shadow alias model predicts whether each
message should succeed, produce a request-level reject, or cause
a session close.

Multi-token adversarial orderings are biased toward dangerous
sequences: REGISTER+DELETE+REGISTER same alias, USE_ALIAS for
unknown alias, duplicate resolved tokens, DELETE after prior reject.

Uses raw message injection (not SimPair) for precise wire control.
No trace hash determinism verification.

### scenario_lifecycle

`tests/scenario/test_scenario_lifecycle.c` generates subscribe and
namespace lifecycle operations through SimPair: subscribe, accept,
reject, publish namespace, accept/reject/cancel/done namespace.

SimPair-backed with trace hash determinism: each seed runs twice
and the FNV-1a hash of all trace records must match.

### scenario_object

`tests/scenario/test_scenario_object.c` exercises object delivery
through SimPair with two oracle tiers:

**Deterministic prelude** (exact oracle): subscribes, accepts,
opens one subgroup, writes three known objects (non-empty payload,
zero-length payload, object ID gap), feeds through randomized
1-8 byte chunking, and asserts `expected == received` exactly
using `hash(group_id, object_id, payload_len, payload_bytes)`.
Catches parser corruption, payload loss, or duplicate delivery.

**Random phase**: randomized subscribe/accept/open/write/close/
reset sequences. SEND_DATA actions are fed to the client in
randomized 1-8 byte chunks with all `on_data_bytes` return codes
checked. Oracle tracks `fed` (payload-bearing SEND_DATA fully
delivered without error) and `received` (OBJECT_RECEIVED events).
Asserts `received <= fed` (no phantom objects) and deterministic
counts/hashes across two runs. Does not assert `fed == received`
because subgroup reset and timing can legitimately drop objects.

### scenario_streaming_object

`tests/scenario/test_scenario_streaming_object.c` exercises streaming
object delivery with `streaming_objects=true` (OBJECT_CHUNK events).

**Deterministic prelude** (exact oracle, manual sessions):
1. Small single-chunk object (5 bytes, begin+end)
2. Zero-length object (begin+end, chunk=NULL)
3. Object fed byte-by-byte (30 bytes → multiple chunks)
4. Large object > MOQ_STREAM_CHUNK_MAX (20000 bytes → multi-chunk)
5. Streaming send API: begin_object + two write_object_data chunks +
   end_object (12 bytes, exact payload reconstruction)
6. Zero-copy rcbuf continuation: header via on_data_bytes, payload
   via on_data_rcbuf with wrapped buffer, release callback verified
7. Full object in one rcbuf: begin chunk zero-copy emission with
   header+payload in a single on_data_rcbuf call, release verified
8. WOULD_BLOCK mid-object retry (max_events=1)
9. Zero-copy WOULD_BLOCK: rcbuf continuation with full event queue,
   pending slice retains backing buffer until retry + cleanup

Verifies: begin/end pairing, payload reconstruction, no structural
violations, correct retry behaviour, zero-copy release callback
timing. All 9 prelude objects must deliver with exact payload match.

**Random phase** (SimPair with `client_streaming_objects=true`):
randomized subscribe/accept/open/write/close/reset with data fed in
randomized 1-8 byte chunks. Object writes use a 50/50 coin flip
between `write_object` (whole-object) and the streaming send API
(`begin_object` + `write_object_data` + `end_object`). WOULD_BLOCK
during chunk delivery is retried inline. Chunk oracle validates
begin/end pairing, counts complete objects, and hashes all received
chunk data for determinism. Asserts `complete <= sent` (no phantom
objects).

Counters: `prelude=N chunks=N complete=N wb=N random=sent/complete
zc_in=N zc_chunks=N zc_rel=N`.

### scenario_streaming_faults

`tests/scenario/test_scenario_streaming_faults.c` exercises streaming
object delivery under transport-level data faults.

SimPair with `client_streaming_objects=true` and data-focused fault
flags: SPLIT_DATA, DROP_DATA, MUTATE_DATA, REORDER_ACTION,
INJECT_RESET, INJECT_STOP, INJECT_CLOSE. No control-stream
drop/mutation (this runner targets data-path resilience specifically).

**Prelude**: fault-free subscribe → accept → open subgroup → write
100-byte object → pump → reconstruct payload from OBJECT_CHUNK events
and verify exact byte match. Exactly one prelude object per seed.

**Random phase**: faults enabled. Persistent chunk oracle validates
begin/end pairing across all drain paths. Session close from injected
faults clears `in_object` state (dangling in-object at clean shutdown
is a violation). All operation return codes checked against expected
set. Counters: chunks, complete, closed, violations, per-fault-type
counts.

**Terminal coverage**: `handle_data_reset` emits
`OBJECT_CHUNK(end=true, terminal=RESET)` when a stream is reset
mid-object with `streaming_objects=true`. The INJECT_STOP path
validates via the STOP→RESET chain. The `rst_term` counter tracks
terminal RESET events observed by the oracle. Because SimPair's
inject phase fires between pump rounds (after all split chunks of
an action are delivered), small objects typically complete before
inject lands, so `rst_term` may be zero for a given sweep. The
terminal path is verified by dedicated unit tests; the scenario
validates no oracle corruption either way.

### scenario_publisher

`tests/scenario/test_scenario_publisher.c` exercises the publisher
facade (`moq_publisher_t`) through SimPair. Server side uses the
facade to manage tracks and write objects; client subscribes via
the session API and receives objects.

Operations: add/remove track, subscribe, write object, end group,
drain client/server events (with namespace accept/reject), pump,
advance time.

Delivery oracle: expected objects (from successful `write_object`)
and received objects (from `OBJECT_RECEIVED`) are hashed with the
same framing (group_id, object_id, payload_len, payload_bytes) and
compared within each run, catching silent drops or corruption.

Namespace lifecycle: client randomly accepts or rejects
`NAMESPACE_PUBLISHED`; shadow model tracks PENDING/ACCEPTED/TERMINAL
state; `remove_track` exercises both ACCEPTED (sends DONE) and
TERMINAL (skips DONE) paths.

### scenario_faults

`tests/scenario/test_scenario_faults.c` is the first BUGGIFY-style
runner. It uses SimPair plus the publisher facade, then enables a
seeded deterministic allocator that fails alloc/realloc calls during
runtime operations.

Operations: add/remove track, subscribe, write object, end group,
drain events, flush pending facade work, pump, advance time.

Every seed runs twice. The trace hash, allocation-call count, and
injected-failure count must match across runs, and allocator balance
must return to zero. This catches nondeterministic cleanup behavior,
NOMEM path leaks, and retry/cleanup mistakes in realistic interleavings.

### scenario_transport_faults

`tests/scenario/test_scenario_transport_faults.c` uses SimPair's
transport-level fault injection. Configured via `fault_per_mille`
and `fault_flags` on `moq_simpair_cfg_t`, faults are enabled after
setup via `moq_simpair_enable_faults`.

Fault types:
- `DROP_CONTROL`: skip `on_control_bytes` delivery
- `DROP_DATA`: skip `on_data_bytes` delivery (header + payload)
- `DROP_RESET`: skip `on_data_reset` delivery
- `DROP_STOP`: skip `on_data_stop` delivery
- `SPLIT_DATA`: deliver `SEND_DATA` in randomized 1-8 byte chunks
  instead of the normal header-then-payload two-call pattern,
  stressing the receive parser's incremental buffering
- `MUTATE_CONTROL`: flip one deterministic bit in a copy of
  `SEND_CONTROL` bytes before delivery, stressing the control
  message parser's error handling
- `MUTATE_DATA`: flip one deterministic bit in a copy of
  `SEND_DATA` header+payload before delivery, stressing the
  data parser's resilience to corrupted framing and payloads
- `SPLIT_CONTROL`: deliver `SEND_CONTROL` in randomized 1-8 byte
  chunks instead of a single call, stressing the control framing
  loop's incremental varint/envelope parser
- `SPLIT_BIDI`: deliver `OPEN_BIDI_STREAM` and `SEND_BIDI_STREAM`
  in randomized 1-8 byte chunks, stressing the bidi stream parser's
  incremental buffering for namespace subscription messages
- `DELAY`: defer delivery of control, data, and bidi stream
  inputs to a deterministic future virtual time. Per-chunk delay
  decisions use independent RNG seeds. Per-stream FIFO is
  preserved: if an earlier chunk on the same stream is delayed,
  later chunks are forced into the delay queue even if their own
  delay decision misses. Stream/bidi mappings are resolved before
  enqueue; terminal events (FIN, reset, stop) bump map generation
  to invalidate later queued bytes. Not in `MOQ_SIM_FAULT_ALL`
  because delay is time-domain: scenarios must advance virtual
  time past deadlines via `moq_simpair_advance_to` and
  `moq_simpair_next_deadline_us` to drain delayed inputs.
  Enable explicitly with `MOQ_SIM_FAULT_DELAY` or
  `MOQ_SIM_FAULT_ALL_DELAY`.
- `REORDER_ACTION`: adjacent swap of eligible actions
  (SEND_CONTROL/SEND_DATA only) within a poll batch, testing
  session state machine ordering assumptions
- `INJECT_RESET`: inject a synthetic `on_data_reset` to the
  receiver for an active mapped data stream, exercising
  unsolicited stream teardown. Stream map entry removed
  immediately.
- `INJECT_STOP`: inject a synthetic `on_data_stop` to the
  sender for an active mapped data stream. The sender
  transitions the subgroup to RESETTING and queues a follow-up
  RESET_DATA action, which routes to the receiver on the next
  pump and removes the stream map entry. If `on_data_stop`
  returns non-OK, the map entry is removed immediately to
  prevent slot leaks.
- `INJECT_CLOSE`: inject a synthetic protocol-close by feeding
  a malformed control envelope (unknown message type 0xFF) to
  the target session via `on_control_bytes`. The session closes
  with code 0x3 ("unknown control message type") through the
  real close path, emitting CLOSE_SESSION action and
  SESSION_CLOSED event. At most one injection per direction
  per step. Skipped if the target session is already closed.

`fault_per_mille` controls the probability for all enabled fault
types. Composition order: reorder first (adjacent swap within
batch), then per-action: drop (dropped actions never enter the
delay queue), truncate/mutate (content faults), split into
chunks, then each chunk is independently delayed or delivered
immediately. Delay preserves per-stream FIFO. After all real
actions: reset/stop injection scans active stream map entries;
close injection may feed malformed control bytes once per
direction if the target session is open.

`MOQ_SIM_FAULT_ALL` includes drop, split (data/control/bidi),
mutate, truncate, and reorder faults. `MOQ_SIM_FAULT_DELAY` is
not in `MOQ_SIM_FAULT_ALL` because it requires time-aware test
patterns — scenarios must call `moq_simpair_advance_to` past
delayed deadlines to drain inputs. `MOQ_SIM_FAULT_ALL_INJECT`
includes `INJECT_RESET`, `INJECT_STOP`, and `INJECT_CLOSE`.
Injection faults and delay are not in `MOQ_SIM_FAULT_ALL`
because they are more aggressive or require different test
scaffolding. Enable explicitly:

```c
cfg.fault_flags = MOQ_SIM_FAULT_ALL | MOQ_SIM_FAULT_ALL_INJECT;
```

Trace records:
- `FAULT_REORDER`: displaced action kind, original indices
  (`code` = i, `count` = i+1)
- `FAULT_DROP`: dropped action bytes and kind
- `FAULT_MUTATE`: mutated bytes, byte index (`code`), bit index
  (`count`). Action trace retains original bytes.
- `FAULT_INJECT`: synthetic reset, stop, or close.
  `action_kind` distinguishes RESET_DATA, STOP_DATA, or
  CLOSE_SESSION. For reset/stop: `code` = 0x100, `count` =
  stream map slot index. For close: `code` = 0x3 (protocol
  violation), `bytes` = injected malformed envelope. `result`
  = return code from the session input call.

Stream map entries are cleaned up on dropped FIN/RESET/STOP to
prevent slot leaks.

### scenario_combined_faults

`tests/scenario/test_scenario_combined_faults.c` exercises both
allocation failures and transport drops in the same run. This
catches interactions the single-fault runners won't see: OOM while
recovering from a dropped control message, cleanup paths after
partially completed faulted operations, and determinism of two
independent fault streams.

Separate env var controls for each fault rate:
`MOQ_SCENARIO_ALLOC_FAULT_PERMILLE` (default 25) and
`MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE` (default 40).

### scenario_goaway

`tests/scenario/test_scenario_goaway.c` exercises the GOAWAY
lifecycle and drain timeout through SimPair.

**Deterministic prelude**: subscribe+accept before GOAWAY,
server sends GOAWAY, subscribe blocked after GOAWAY, object
delivery after GOAWAY.

**Deterministic timeout prelude** (separate SimPair with
`server_goaway_timeout_us=5000`): subscription accepted,
subgroup opened, first object pumped to client, GOAWAY sent
(deadline arms), second object written without pumping
(SEND_DATA queued as active data), advance before deadline
does not close, advance at deadline closes with
`MOQ_CLOSE_GOAWAY_TIMEOUT`, cleanup of queued data is
leak-free.

**Random phase**: pump, write object, drain client/server,
advance time (1-2000us delta via `moq_simpair_advance_to`),
client GOAWAY, subscribe-blocked, duplicate GOAWAY. The
advance-time op can cross GOAWAY deadlines and trigger
timeout closes during the random phase.

Counters: goaway events, subscribe-blocked, objects after
GOAWAY, duplicate GOAWAY closes, timeout closes,
ticks-before-deadline. All verified deterministic across two
runs per seed.

### scenario_backpressure

`tests/scenario/test_scenario_backpressure.c` exercises queue
backpressure retry paths using tiny session capacities
(`max_actions=2`, `max_events=3`) via raw `establish_pair`
(not SimPair).

**Deterministic prelude** forces:

- `accept_subscribe` → WOULD_BLOCK → drain → retry OK
- `reject_subscribe` → WOULD_BLOCK → drain → retry OK
- `accept_namespace` → WOULD_BLOCK → drain → retry OK
- control input (SUBSCRIBE) event queue full → drain →
  `process_pending` → event emitted
- data input (OBJECT_RECEIVED) event queue full → drain →
  zero-byte `on_data_bytes` retry → object emitted with
  correct payload

**Random phase** (seeded, fresh session pair with same tiny
capacities): interleaves subscribe, accept/reject, namespace
publish/accept/done/cancel, open/close/reset subgroup, write
object, pump, drain, and `process_pending` operations. Each
operation pumps its output inline and handles data-input
WOULD_BLOCK via drain + retry. Shadow model tracks pending/
accepted subscriptions, namespaces, and open subgroups to
ensure only legal operations are attempted.

Counters: prelude — accept_wb, reject_wb, namespace_wb,
control_event_wb, object_event_wb, retry_success. Random —
rnd_ops, rnd_wb_act (action queue WB), rnd_wb_ctrl (control
input WB), rnd_wb_data (data input WB), rnd_retry
(successful WB recoveries), rnd_obj (sent/received with
payload hash oracle), rnd_ns (namespace ops completed).

Prelude retry oracle: `retry == accept + reject + namespace +
control + object`. All counters verified deterministic across
two runs per seed. Core-only (no SimPair dependency); runs in
no-sim builds.

Output scratch exhaustion and pool/capacity exhaustion are
covered by dedicated unit tests (not in this scenario runner).

### scenario_request_credit

`tests/scenario/test_scenario_request_credit.c` proves that
subscribe and publish_namespace share request capacity
correctly under random ordering with tiny initial capacity.

Uses raw `establish_pair` with initial request capacity of 2
(one request before blocking). Both subscribe and
publish_namespace are called on the same session (client)
so they share the local request counter. The peer grants
additional request capacity to unblock.

**Deterministic prelude**: subscribe OK, second subscribe
blocked, publish_namespace also blocked (shared counter),
grant credit, subscribe retries OK, grant more, namespace
retries OK, accept both families to prove cross-family
registry dispatch.

**Random phase** (seeded, fresh pair with same tiny credit):
interleaves subscribe, publish_namespace, accept/reject
subscription, accept/done namespace, credit grants, pump,
drain. Credit grants use `moq_session_grant_request_capacity`
on the server and pump the resulting control action.

Counters: credit_blocked_sub, credit_blocked_ns,
credit_grants, retry_after_credit, sub_ok, ns_ok,
request_errors. All verified deterministic across two runs.
Core-only; runs in no-sim builds.

### scenario_fetch

`tests/scenario/test_scenario_fetch.c` exercises FETCH end-to-end
through SimPair: client fetch, server accept/reject/cancel, object
writes, gap markers, end_fetch, and all client-side FETCH events.

**Deterministic prelude** (separate SimPair, no faults):
Standalone fetch: subscribe → accept (non-empty, end_group=5) →
write two objects ("obj0" at g=0, "obj1" at g=1) → write gap
(NON_EXISTENT at g=2) → end_fetch → pump → client verifies fetch
accepted + 2 fetch objects + 1 fetch gap + fetch complete.
Joining fetch: subscribe with LARGEST_OBJECT → accept with
largest=(5,2) → relative joining fetch 2 groups back → verify
calculated range start=3 end=(5,3) → accept → write object "join"
→ verify OK + object + complete.
Payload content hashed for exact oracle match.

**Random phase** (SimPair with trace hash): 11 operations —
fetch, accept, reject, write_object, write_range, end, cancel,
drain_client, drain_server, pump, advance_time. SEND_DATA actions
fed to client in randomized 1-8 byte chunks (pump_with_chunking).
Shadow model tracks 4 fetch slots with NONE/PENDING/ACCEPTED/DONE
states. Payload-bearing fed objects and received FETCH_OBJECT
events counted for no-phantom invariant.

Counters: prelude=N (exact objects), random=fed/received
(no-phantom: received <= fed).

### scenario_publish

`tests/scenario/test_scenario_publish.c` exercises publisher-initiated
subscription (PUBLISH message) lifecycle through SimPair.

**Deterministic prelude**: client publishes → server accepts → client
opens subgroup → writes object "prelude" → closes subgroup → server
receives exact payload → client finishes publish → server receives
PUBLISH_FINISHED.

**Random phase**: publish, accept, reject, open_subgroup, write_object,
close_subgroup, finish, pump, drain, advance time. Shadow model tracks
4 publication slots with NONE/PENDING/ACCEPTED/DONE states.

Counters: prelude (exact delivery), random fed/received (no phantoms).

### scenario_namespace_sub

`tests/scenario/test_scenario_namespace_sub.c` exercises
namespace subscription bidi-stream lifecycle through SimPair.

SimPair routes bidi stream actions: `OPEN_BIDI_STREAM` delivers
request bytes to the publisher via `on_bidi_stream_bytes`,
`SEND_BIDI_STREAM` delivers response bytes to the subscriber,
`CLOSE_BIDI_STREAM` delivers reset to the publisher.

**Deterministic prelude**: subscribe_namespace → ns_sub_request
on publisher → accept → ns_sub_ok on subscriber →
send_namespace → namespace_found event →
send_namespace_done → namespace_gone event → cancel →
close bidi stream → publisher reset.

**Random phase**: subscribe, accept/reject, cancel,
send_namespace/send_namespace_done on established streams,
pump, drain.

Counters: ns_sub_sent, ns_sub_request, ns_sub_ok, ns_sub_error,
ns_found, ns_gone, cancel_sent. Phantom assertion:
ok + error <= sent.

### scenario_track_status

`tests/scenario/test_scenario_track_status.c` exercises
track status request/response lifecycle through SimPair.

**Deterministic prelude**: track_status → server receives request →
accept → client receives OK → second track_status → server rejects
→ client receives ERROR with correct error code. Handles verified
stale after terminal responses.

**Random phase**: request, accept, reject, pump, drain, advance
time. Shadow model tracks 4 track-status slots with NONE/PENDING
states; slots recycle to NONE after terminal events. Small request
capacity (8) produces occasional request-blocked pressure.

Counters: sent, ok, error. Phantom assertion: ok + error <= sent.

### scenario_object_datagram

`tests/scenario/test_scenario_object_datagram.c` exercises
object datagram send and receive through SimPair.

**Deterministic prelude**: subscribe -> accept -> server sends payload
datagram -> client receives OBJECT_RECEIVED with datagram=true and
correct payload -> server sends status datagram -> client receives
status event with END_OF_GROUP.

**Random phase**: send_datagram (with ~25% properties), send_status,
pump, drain, advance time. Random payload sizes (1-63 bytes),
group/object IDs, end-of-group flag. Separate payload and properties
hash oracles: received <= fed for both. Beyond-prelude assertions
verify fed > num_seeds and properties exercised.

### scenario_delay

`tests/scenario/test_scenario_delay.c` exercises deterministic
delay fault injection through SimPair. Unlike other fault runners,
it opts into `MOQ_SIM_FAULT_DELAY` explicitly (not via `FAULT_ALL`)
because delay is time-domain: the runner must advance virtual time
past delayed-input deadlines to drain queued inputs.

Configured with `DELAY | SPLIT_CONTROL | SPLIT_DATA | SPLIT_BIDI`
at 50% fault rate. Uses a deadline-aware drain loop:
`run_until_quiescent` → `next_deadline_us` → `advance_to` → repeat
until no future deadlines remain.

**Deterministic prelude** (exact oracle): subscribe through delayed
control delivery → accept → write 16-byte object through delayed +
split data → close subgroup → verify exact payload match after
deadline advancement → namespace-sub roundtrip through delayed +
split bidi delivery → verify NS_SUB_OK on client.

**Random phase**: write objects, drain client/server events, advance
time (1-500us), deadline-aware pump, plain pump. Sessions must
remain coherent throughout.

Counters: prelude object (exact 16-byte match), delay enqueue
(must be > 0), trace hash determinism across two runs.

### scenario_crossed

`tests/scenario/test_scenario_crossed.c` exercises protocol races
where terminal and update messages cross in flight under delay.
Uses `DELAY | SPLIT_CONTROL | SPLIT_DATA | SPLIT_BIDI` at 50%.

Four deterministic crossing scenarios per seed:

1. **Subscription update × unsubscribe**: REQUEST_UPDATE sent, then
   UNSUBSCRIBE before the response drains. Late REQUEST_OK/ERROR
   must be absorbed by tombstones.

2. **PUBLISH update × PUBLISH_DONE**: Subscriber sends publication
   update, publisher sends PUBLISH_DONE before response delivery.
   Per D16 Section 9.11, a late REQUEST_UPDATE arriving at the
   publisher after state deletion causes PROTOCOL_VIOLATION (0x3).
   The scenario verifies the close code is 0x3 when this race
   occurs and accepts it as spec-compliant behavior.

3. **Namespace-sub terminal crossing**: NAMESPACE + NAMESPACE_DONE
   sent with delay/split on the bidi stream. Exercises delayed
   terminal delivery without false done-before-namespace violations.

4. **GOAWAY under delayed traffic**: Delayed control and bidi inputs
   queued, then GOAWAY sent. Verifies no crash/leak during drain.

Determinism: trace hash match across two runs per seed. Allocation
balance clean. Session close in the pub_update phase is accepted
as a valid protocol race.

### scenario_delay_backpressure

`tests/scenario/test_scenario_delay_backpressure.c` verifies
stability under delayed delivery with constrained queue capacities
(max_actions=2, max_events=2). Uses SimPair with `max_actions`/
`max_events` config fields. A local drain helper catches propagated
WOULD_BLOCK, drains queues, calls `process_pending`, and retries.
This runner proves no crash, leak, or unexpected close under queue
pressure, not explicit WOULD_BLOCK assertion (see
`scenario_backpressure` for raw-session WOULD_BLOCK proof).

Four independent sub-scenarios per seed:

1. **Control delivery**: Subscribe with max_actions=2 under delay.
2. **Object delivery**: Write + receive with max_events=2.
3. **Data terminal**: Write + close subgroup under dual pressure.
4. **Namespace-sub bidi**: Delayed bidi under small queues.

### Replaying delay scenario failures

Delay tests must drain by deadline, not just call
`run_until_quiescent`. On failure, the runner prints the seed,
sim state, and replay command:

```bash
scripts/replay_scenario.sh --runner scenario_delay --seed 0x42
```

Or directly:

```bash
MOQ_SCENARIO_SEED_START=0x42 MOQ_SCENARIO_SEEDS=1 \
  MOQ_SCENARIO_STEPS=50 ./build/tests/test_scenario_delay
```

If the failure involves delayed inputs that never matured, check
whether `moq_simpair_next_deadline_us` returned a future time
that the scenario's `drain_deadlines` loop did not reach.

### Coverage

| Runner | CTest (100) | Extended | ASan+UBSan |
|--------|-------------|----------|------------|
| scenario_auth | Pass | 100k | 1k clean |
| scenario_lifecycle | Pass | 10k | 1k clean |
| scenario_object | Pass | 10k | 1k clean |
| scenario_streaming_object | Pass | 10k | 1k clean |
| scenario_streaming_faults | Pass | 10k | 1k clean |
| scenario_publisher | Pass | 10k | 1k clean |
| scenario_goaway | Pass | 10k | 1k clean |
| scenario_backpressure | Pass | 10k | 1k clean |
| scenario_request_credit | Pass | 10k | 1k clean |
| scenario_namespace_sub | Pass | 10k | 1k clean |
| scenario_fetch | Pass | 10k | 1k clean |
| scenario_publish | Pass | 1k | --- |
| scenario_track_status | Pass | 1k | --- |
| scenario_object_datagram | Pass | 1k | --- |
| scenario_faults | Pass | 10k | 1k clean |
| scenario_transport_faults | Pass | 10k | 1k clean |
| scenario_combined_faults | Pass | 10k | 1k clean |
| scenario_delay | Pass | 10k | 10k clean |
| scenario_crossed | Pass | 10k | 10k clean |
| scenario_delay_backpressure | Pass | 10k | 10k clean |

### Environment variables

All runners support the same environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MOQ_SCENARIO_SEEDS` | 100 | Number of seeds to run |
| `MOQ_SCENARIO_SEED_START` | 0 | First seed value |
| `MOQ_SCENARIO_STEPS` | 50 | Steps per seed |
| `MOQ_SCENARIO_FAULT_PERMILLE` | 30/50 | Fault rate per thousand (`scenario_faults` / `scenario_transport_faults`) |
| `MOQ_SCENARIO_ALLOC_FAULT_PERMILLE` | 25 | Allocation fault rate (`scenario_combined_faults`) |
| `MOQ_SCENARIO_TRANSPORT_FAULT_PERMILLE` | 40 | Transport fault rate (`scenario_combined_faults`) |
| `MOQ_SCENARIO_MUTATE_PERMILLE` | 0 | Per-mille chance each case also runs one structured byte mutation (`scenario_media_*`); 0 = generate-only |

Accepts hex values (e.g., `MOQ_SCENARIO_SEED_START=0x36`).

On failure, each runner prints a replay command:

```
Replay: MOQ_SCENARIO_SEED_START=0x36 MOQ_SCENARIO_SEEDS=1 \
  MOQ_SCENARIO_STEPS=50 ./build/tests/test_scenario_auth
```

### Replay Tool

`scripts/replay_scenario.sh` replays failures from the printed
output. Paste the env-var tokens directly:

```bash
scripts/replay_scenario.sh MOQ_SCENARIO_SEED_START=0x36 \
  MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=50 \
  ./build/tests/test_scenario_auth
```

Or use explicit arguments:

```bash
scripts/replay_scenario.sh --runner scenario_auth --seed 0x36
```

With fault rates:

```bash
scripts/replay_scenario.sh --runner scenario_combined_faults \
  --seed 0x36 --alloc-fault-permille 25 \
  --transport-fault-permille 40
```

Run under a debugger:

```bash
scripts/replay_scenario.sh --runner scenario_auth --seed 0x36 \
  --debugger lldb
```

Use `--dry-run` to print the command without executing, `--verbose`
for parsed settings, and `--build-dir DIR` to select a specific
CMake build root.

### Failure Reducer

`scripts/reduce_scenario.sh` minimizes a failing scenario's step
count via binary search:

```bash
scripts/reduce_scenario.sh --runner scenario_combined_faults \
  --seed 0x36 --steps 50 --alloc-fault-permille 25 \
  --transport-fault-permille 40
```

Or paste the failing replay line directly. Output:

```
Reduced: steps 50 → 7 (6 iterations)
Minimized command:
  MOQ_SCENARIO_SEED_START=0x36 MOQ_SCENARIO_SEEDS=1 ...
```

Validate with `scripts/reduce_scenario.sh --self-test`.

The step count (from `--steps` or a pasted `MOQ_SCENARIO_STEPS=` token)
is validated as a plain non-negative decimal integer in the range
`0..1000000000` before it is used in the binary-search arithmetic: a
non-numeric value is rejected rather than evaluated by the shell, and a
value above the bound is rejected rather than wrapping the signed 64-bit
`(( ))` comparison.

Assumes prefix-monotonic failures: if step N fails, step N+K
also fails. This holds for all scenario runner invariants
(allocator balance, trace determinism, unexpected close).

### Current limitations

- **Reordering scope**: action reordering is limited to adjacent
  swaps of SEND_CONTROL/SEND_DATA within a single poll batch.
- **scenario_auth bypass**: Raw injection means no SimPair trace
  hash for this runner.

## Fuzz Targets

Five fuzz targets exercise moq-core with arbitrary inputs under
ASan and UBSan:

| Target | Scope |
|--------|-------|
| `fuzz_quic_varint` | QUIC varint roundtrip |
| `fuzz_kvp_decode` | KVP decoder |
| `fuzz_control_d16` | D16 control message codec + subgroup header |
| `fuzz_session_control` | Full session state machine via `on_control_bytes` |
| `fuzz_session_data` | Data parser via `on_data_bytes` with split modes |

Fuzz targets build only when `MOQ_BUILD_FUZZ=ON` and the compiler
provides libFuzzer. Normal and nosim CTest counts are unaffected.

### Apple Clang limitation

Apple Clang does not ship the libFuzzer runtime
(`libclang_rt.fuzzer_osx.a`). When configuring with Apple Clang,
fuzz targets are skipped with a configure message:

```
-- libFuzzer runtime is not linkable with this compiler;
   skipping fuzz targets. Set MOQ_REQUIRE_FUZZ=ON to make this fatal.
```

To build fuzz targets on macOS, install upstream LLVM Clang:

```bash
brew install llvm
```

Then configure with the upstream compiler:

```bash
CC=/opt/homebrew/opt/llvm/bin/clang cmake --preset fuzz
cmake --build build/fuzz-check
ctest --test-dir build/fuzz-check --output-on-failure
```

Or without presets:

```bash
CC=/opt/homebrew/opt/llvm/bin/clang \
  cmake -S . -B build/fuzz-check \
  -DMOQ_BUILD_FUZZ=ON -DMOQ_BUILD_SIM=ON
```

### MOQ_REQUIRE_FUZZ

Set `MOQ_REQUIRE_FUZZ=ON` to turn missing Clang/libFuzzer into a
fatal configure error. Useful for CI where fuzz validation must
not silently degrade:

```bash
CC=/opt/homebrew/opt/llvm/bin/clang \
  cmake --preset fuzz -DMOQ_REQUIRE_FUZZ=ON
```

### CTest smoke tests

When fuzz targets build successfully, 7 CTest tests are registered:
5 empty-input startup smoke tests and 2 corpus smoke tests that
feed valid protocol messages to prove the session targets reach
the parser.

### Running fuzz sessions

```bash
# Corpus smoke (no mutations, just replay corpus files)
build/fuzz-check/fuzz/fuzz_session_control \
  fuzz/corpus/session_control -runs=0
build/fuzz-check/fuzz/fuzz_session_data \
  fuzz/corpus/session_data -runs=0

# Short fuzz session (10k iterations)
build/fuzz-check/fuzz/fuzz_session_control -max_len=4096 -runs=10000
build/fuzz-check/fuzz/fuzz_session_data -max_len=4096 -runs=10000
```

## Build Presets

`CMakePresets.json` defines configure presets for the standard
build configurations:

| Preset | Build dir | Description |
|--------|-----------|-------------|
| `default` | `build` | Normal build with sim + tests |
| `nosim` | `build/nosim` | Tests without sim library |
| `asan` | `build/asan` | ASan + UBSan |
| `fuzz` | `build/fuzz-check` | Fuzz targets + sim + tests |

```bash
cmake --list-presets        # show available presets
cmake --preset default      # configure normal build
cmake --preset fuzz         # configure fuzz build
cmake --build build         # build
ctest --test-dir build      # test
```

Presets require CMake 3.20+. For local compiler overrides, create
a `CMakeUserPresets.json` (gitignored) or pass `CC=` on the
command line.

## Seed Sweep

`scripts/run_seed_sweeps.sh` runs extended seeded sweeps across all
scenario runners with named profiles. It is not a CTest target;
invoke it explicitly.

### Profiles

| Profile | Normal seeds | ASan seeds | ~Time |
|---------|-------------|-----------|-------|
| quick | 1k per runner | — | ~5s |
| standard | 10k per runner | 1k subset | ~30s |
| nightly | 100k per runner | 10k all | ~5min |

### Seeded media conformance runners

`scenario_media_loc`, `scenario_media_msf`, and `scenario_media_cmaf` are
seeded media-format property runners (see `tests/support/media_fuzz.h`). They
reuse the same `MOQ_SCENARIO_*` env, splitmix64 PRNG, and replay tooling, but
exercise media formats (LOC / MSF-CMSF catalog / CMAF object) rather than
sessions. The profiles run them generate-only in `quick` and add structured
byte mutation (`MOQ_SCENARIO_MUTATE_PERMILLE`) in `standard` and `nightly`,
including under ASan where mutation catches over-reads, not just rejects. They
depend on the optional media libraries (`MOQ_BUILD_LOC` / `MOQ_BUILD_MSF` /
`MOQ_BUILD_CMAF`) and are skipped when a build omits them.

### Usage

```bash
scripts/run_seed_sweeps.sh --profile quick
scripts/run_seed_sweeps.sh --profile standard --fail-fast
scripts/run_seed_sweeps.sh --profile nightly
scripts/run_seed_sweeps.sh --runner scenario_auth --seeds 50000
scripts/run_seed_sweeps.sh --runner scenario_combined_faults --seeds 100 \
  --alloc-fault-permille 25 --transport-fault-permille 40
scripts/run_seed_sweeps.sh --profile quick --dry-run
```

### Flags

| Flag | Description |
|------|-------------|
| `--profile NAME` | quick, standard, or nightly |
| `--runner NAME` | Filter to one runner |
| `--seeds N` | Override seed count |
| `--steps N` | Override step count (default: 50) |
| `--build-dir DIR` | Normal build root (default: build) |
| `--asan-dir DIR` | ASan build root (default: build/asan) |
| `--fault-permille N` | Fault rate for scenario_faults / scenario_transport_faults |
| `--alloc-fault-permille N` | Allocation fault rate for scenario_combined_faults |
| `--transport-fault-permille N` | Transport fault rate for scenario_combined_faults |
| `--fail-fast` | Stop on first failure |
| `--dry-run` | Print commands without executing; no log directory created |
| `--verbose` | Print each invocation |

Profiles use hardcoded fault rates; the fault-rate flags apply to
`--runner`/`--seeds` custom mode only.

On failure, the script prints replay and reduce commands for the
failing seed with all relevant flags (build dir, steps, fault rates).
If the ASan build directory does not exist, ASan entries are skipped
with a warning (and, like missing optional media runners, counted as
*skipped* — never as passed). The per-profile summary reports
`X passed, Y failed, Z skipped (of N selected)`; a run with skips but no
failures still succeeds.

`--runner` in profile mode is validated against the known runner list: an
unknown runner is a hard error (so a typo can never silently select zero
entries and report success), and a valid runner that is not part of the
chosen profile is likewise rejected rather than reported as 0-of-0.

All runs (pass and fail) preserve full output under
`/tmp/seed_sweep_XXXXXX/`, one log file per runner/label combination.

Default CTest (31 targets, ~0.2s) is unchanged.

## Line Coverage

`scripts/run_coverage.sh` builds with coverage instrumentation, runs
all tests, and generates a line-coverage report for core/src/session/.

```bash
scripts/run_coverage.sh                  # full build + test + report
scripts/run_coverage.sh --report         # regenerate from existing profiles
scripts/run_coverage.sh --toolchain llvm # force Clang/LLVM path
scripts/run_coverage.sh --toolchain gcov # force GCC/gcov path
```

Toolchain detection:
- **Clang/LLVM**: requires `clang`, `llvm-profdata`, `llvm-cov`.
  On macOS these are found via `xcrun`; on Linux/FreeBSD via `PATH`.
  The script forces `clang`/`clang++` as the CMake compiler when
  using LLVM coverage flags.
- **GCC/gcov**: requires `gcc`, `gcov`. Optionally `lcov` and
  `genhtml` for HTML reports.

Output in `build/coverage/coverage-report/`:

| File | Contents |
|------|----------|
| `session-summary.txt` | Session core line/function/branch coverage |
| `core-summary.txt` | Full core/ coverage including headers, wire, types |
| `html/index.html` | Annotated source HTML (LLVM) or genhtml (GCC) |

No hard coverage threshold is set. The report is a local development
tool for identifying uncovered paths, not a CI gate.

## What Is Not Yet Active

- **Output scratch exhaustion**: WOULD_BLOCK from scratch capacity.
