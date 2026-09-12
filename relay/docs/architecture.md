# moq5-relay — Architecture

The deterministic MoQ relay: a sans-I/O relay core over libmoq's public
session API, built to the same standard as the session core it consumes.
This file is the reviewed in-tree architecture reference.

## The one-sentence architecture

A relay is a set of single-writer **shards**; each shard owns sessions and
**tracks**; each track is a **bounded append-and-evict log** of refcounted
object records; every consumer of any kind — live subscriber, fetch, cache
retention, cross-shard pump, future cluster leg — is a **cursor** over that
log; and the whole relay is a deterministic state machine whose only inputs
are session events, delivery outcomes, control operations, and time.

Consequences, each load-bearing:

- **The cache is not a component.** It is the log's retained region behind
  the head; retention config *is* cache policy; a warm track serves rejoining
  subscribers with zero copies and no writeback machinery.
- **Backpressure is not a queue.** A slow subscriber is a cursor that stops
  advancing over shared records. Retained memory is bounded by log budgets
  and independent of subscriber count. Lag policy acts at group boundaries
  (skip or retire), always as explicit trace events.
- **Fan-out is reference manipulation.** One retained object record; N
  cursors; one `rcbuf` incref per delivery. Zero payload copies within a
  shard (proven at the session layer by `benchmarks/bench_relay_fanout.c`:
  0 alloc / 0 free after warmup at fanout 64).
- **Multi-core is clones-at-boundaries, not shared payloads.** A track is
  owned by one shard (pluggable pure placement over epoch-stamped snapshots).
  Control, remote-demand lifecycle, AND object data cross shards through
  bounded channels: the owner runs one pump cursor per requesting shard,
  payload and properties are cloned exactly once at the boundary into the
  requester's replica log, and all requester-side fan-out is local rcbuf
  references. No locks, atomics, or RCU appear in the core; the channels'
  leaf mutexes are the only cross-thread synchronization.
- **Determinism is the product.** All nondeterminism enters as inputs
  (events, delivery outcomes, capacity edges, control ops incl. membership
  epochs, time, shard schedule). In simulation every input derives from a
  seed; a bounded scalar-only trace ring hashes the run; identical seed ⇒
  identical hash. Multi-shard interleaving goes through a scheduler seam so
  cluster races are enumerable schedules, not heisenbugs.
- **Intents fan out within a bounded ring.** The core's intent ring is
  clamped at create to `max(2, max_subs, max_ns_subs, max_ns_nodes)` — the
  largest single *atomic* reservation (a track's upstream resolution
  ≤ max_subs; one namespace's withdrawal ≤ max_ns_subs; a namespace
  subscription's matches ≤ max_ns_nodes). A binding close, whose total
  fan-out could be multiplicative (announced namespaces × watchers), is
  **resumable**: it delivers as much as fits, returns WOULD_BLOCK, and the
  binding drains and retries until done — so a close never strands and a
  withdrawal is never suppressed. The binding drains and retries on any
  transient WOULD_BLOCK, so a control event is never dropped and a peer
  never waits forever for a response.
- **Memory bounds are explicit and closed-form, process-wide.** Every
  structure is bounded by config, and the capacity model computes the whole
  relay-state allocation-request ceiling before anything runs — core pools +
  per-track log budgets, bind tables and rcbuf headers, and (at multiple
  lanes) the cross-shard runtime: channels with their byte gauge and clone
  headers, canonical-key pools, allocate-before-push staging, the admission
  progress tables, and the CLI's own snapshot rows. `moq5-relay capacity`
  prints it at every lane count; overflow refuses `INVAL` rather than
  under-reporting. Admission control refuses past capacity instead of
  degrading.

## Hard rules (CI-enforced where possible)

1. `relay/src/core/` is sans-I/O: no sockets, threads, clocks, globals. Time is an
   input. Same discipline as libmoq `core/src/session/`.
2. Public libmoq API only. Never `transport_bridge.h` / `wire.h` / `codec.h`
   / `control*.h` / `session_internal.h`, and not the publisher/subscriber
   facades (they own session event polling; the relay needs raw events).
   Enforced by `scripts/check_relay_boundary.sh` (`relay_boundary` ctest).
3. Single-writer per shard. Cross-shard = bounded mailboxes + explicit
   `moq_rcbuf_clone` only.
4. Everything bounded; every bound is a capacity-model term.
5. Payload opacity: object payloads are never parsed, split, combined, or
   modified (spec MUST — citation pinned in log.h).
6. Borrow discipline: everything non-owned in a polled `moq_event_t` is
   borrowed until the next advancing call — `apply()` copies what it retains
   (interned names, parked auth-token bytes) during the call; trace records
   hold scalars only.
7. Determinism is tested (run-twice hash equality), not aspirational.
8. No protocol claims from memory — every wire behavior cites the local MoQ
   specs checkout.

## Layout

```
relay/
  include/moq/relay/    installed public headers; fixed-layout and ownership
                        contracts documented in docs/api.md
  src/core/  sans-I/O relay core (moq::relay-core; links moq::core only)
  src/bind/  production session binding (moq::relay; transport-
             agnostic — proven over SimPair by the parity suite, driven by
             the executable over the MsQuic managed lane)
  src/obs/   observability serializers (moq::relay; links the binding):
             Prometheus 0.0.4 and OpenMetrics 1.0 expositions over core +
             binding stat snapshots, selected by an explicit render mode.
             Pure — no I/O, no allocation. Trace JSONL and route dumps live
             with the core state they render.
  src/shard/ bounded multi-shard runtime (moq::relay): placement,
             announce replication, winner enforcement, directed control and
             demand channels, and deterministic plus concurrent step seams
  tests/     deterministic engine, binding, shard, observation, scheduler,
             and source-contract tests plus their owned fixtures
  bench/     relay benchmark binaries; orchestration and result analysis stay
             with the command-line tool

tools/moq5-relay/
  admin/     the bounded HTTP surface (moqr-admin; links moq::relay):
             a sans-I/O request parser with fixed request/header bounds and
             a deterministic multi-client state machine over preallocated
             body banks. No socket, no thread, no per-request allocation
  cli/       the moq5-relay executable: JSON config, capacity printout,
             `serve` over one or more MsQuic managed lanes (one shard/lane),
             the snapshot broker, the loopback admin listener and its two
             API documents, and the serve log (text or JSON events)
  tests/     CLI/admin/config/lifecycle tests and managed-transport integration
  scripts/   benchmark orchestration and result analysis
```
There is no standalone relay `sim/` directory. Deterministic coverage lives in
the shard scheduler, scenario, and SimPair-backed binding tests.

Build: `-DMOQ_BUILD_RELAY=ON` builds the transport-independent relay libraries,
benchmarks, and tests whose dependencies are available. The production
`moq5-relay` executable and its raw-QUIC loopback smoke are gated on
`moq::adapter-msquic-managed`
(`-DMOQ_BUILD_ADAPTER_MSQUIC=ON -DMOQ_BUILD_MSQUIC_MANAGED=ON`). Tests use the
committed test-only certificate and key; they do not mint credentials from the
wall clock. When `moq::adapter-wtquic-msquic-managed` is also available, the
same executable gains the separately gated WebTransport facade. Without the
raw MsQuic managed adapter the executable is not built. Tests carry the
`relay` ctest label.

## The production runtime (MsQuic managed lanes)

`moq5-relay serve` runs over the **MsQuic managed** server, gated on
`moq::adapter-msquic-managed` — there is no picoquic fallback for the
production executable. `listener.lanes` defaults to 1 and is bounded at 64.
Lane count 1 keeps the direct single-core path; lane count greater than 1
instantiates `moqr_shards_t` with exactly one shard per lane:

```
MsQuic managed lane i  ->  shard i { one moqr_bind_t, one moqr_core_t, one trace }  ->  sessions
```

- **`on_lane_pump` is the only session-access window.** Every `moq_session_*`
  and `moqr_bind_*` call happens inside it, under that lane's lock domain.
  Nothing touches a session outside the pump; `stop`/
  `wait`/`destroy` run only from the main thread, never in a callback.
- **Each lane iterates only its own connections.** It walks
  `moq_msquic_lane_next_conn(lane, …)` and gets each session via
  `moq_msquic_managed_conn_session(conn)`. It must **not** call
  `moq_msquic_managed_session()` — that is a client-only convenience and is
  NULL on a server (and outside the pump).
- **The admin endpoint adds one CLI-owned thread.** With an `admin`
  section configured, one further thread the CLI owns runs the bounded client
  state machine on a loopback socket, alongside whatever threads the
  transport keeps for its lanes. It never touches a session, core, binding,
  journal or trace: it reads only the rows a lane copied into the snapshot,
  and renders them into preallocated per-generation storage. Bind and thread
  start are all-or-nothing before the readiness records, and it is cancelled
  and joined before any facade stop, facade destroy or snapshot destroy.
- **One broker serial, separate demand.** Publication identity and output
  demand are distinct: the broker assigns one snapshot serial that every
  producer joins, while demand bits say who receives that generation. A
  scrape therefore never triggers a signal dump, and SIGUSR1 still emits
  metrics and routes. Two body banks are pinned for the duration of a
  client's write; a third generation while both are pinned is answered
  `503`, never truncated. Lanes gather stats only when the serial advances.
- **Per-connection state lives in `conn_user`** — a small tag (unattached →
  attach to the binding on first sight; attached; dead once the binding
  observes `SESSION_CLOSED` or the connection is refused). Never a map keyed
  by conn/session pointer: those values can be reused by a successor
  connection.
- **A lane is a shard's single writer.** Lane `i` attaches its sessions to
  shard `i`'s binding, runs `moqr_shards_step_shard`, then wakes exactly the
  lanes named by the returned wake mask: destinations that accepted a push
  PLUS producers whose channel toward this shard regained capacity through a
  durable pop (the producer-credit wake — a lane held on channel-full never
  waits for an idle sweep; redundant wakes are possible, lost credits are
  not). The shard core and binding never cross lane ownership.
- **Cross-shard control is live.** Namespace announcements replicate through
  directed coalescing mailboxes; deterministic rendezvous selects the winner;
  losing local publishers are force-withdrawn; and remote subscribe demand
  round-trips over separate ordered demand channels. Those channels carry
  owned copies and have leaf mutexes; they never introduce a lock into a
  shard core.
- **Exact namespaces have one active source per shard.** When another
  connection publishes an exact namespace already owned locally, the binding
  uses the newer PUBLISH_NAMESPACE request as the deterministic winner. It
  force-withdraws the incumbent generation, terminates tracks sourced by that
  generation, cancels the old request, then admits the replacement. This is
  the relay's bounded single-source policy under the multiple-publisher rules
  in draft-16 Section 8.3 and draft-18 Section 9.3; a duplicate on the same
  session remains invalid. Cross-shard rendezvous applies the corresponding
  deterministic winner policy between shards.
- **A shard step is six phases, in order:** (1) inbound drain — control
  mailboxes plus the demand channels, applying every message class above;
  (2) the bind pump; (3) dirty journal reconcile (mirrors, winners,
  re-targets); (4) outbound control; (5) the demand phase — liveness probes,
  DEMAND/UNDEMAND forwarding, ACK/DONE replies, and the staged retryable
  teardowns/terminals; (6) the data pump — pull admitted deliveries from the
  per-origin pseudo-bindings and cross them (whole records as one message,
  chunked/live-edge streams via the progress cursor, abandons as resets,
  eviction watermarks and seals as acknowledged notices). At K=1 the manager
  phases are structurally inert (no manager exists) and the step collapses to
  the bind pump.
- **Cross-shard object data is production behaviour, automatically on for
  multiple lanes.** The shared CLI builder sets `admit_remote_demand =
  (lanes > 1)` — the single place that rule lives, consumed identically by
  `capacity` and `serve`; a single-lane relay is the direct M1 path with no
  cross-shard plane, and there is **no user-facing admission key or toggle**.
  When admission is on, the demand channel is one ordered FIFO per directed
  shard pair carrying the full vocabulary in arrival order:
  DEMAND/UNDEMAND/ACK/DONE control, whole-object records (OBJ), live-edge
  chunk streams (OBJ_OPEN / OBJ_CHUNK… / OBJ_END with a per-demand
  subgroup-progress slot as the resume cursor), and terminals (OBJ_RESET and
  GRP_RESET with bit-exact 62-bit codes, GRP_EVICT carrying the eviction
  watermark, SG_SEAL as the durable subgroup FIN). Ordering by construction:
  the ACK precedes any data, data precedes its reset/seal/DONE. Every
  crossing payload is an independently owned clone — never a shared rcbuf,
  never a borrowed pointer across the boundary. Bounds are explicit:
  per-channel entry counts plus a logical-byte gauge (validated ≥ one
  resolved log record so a legal record is never unsendable), bounded
  per-demand progress tables on both sides, and per-turn message/byte
  budgets (soft-first, no debt; zero-byte terminals still spend a message).
  A sticky per-channel control/data arbiter keeps either class from starving
  the other; every notice or terminal is acknowledged to the core only after
  its channel message is durable; and any identity mismatch, lost
  observation, or unknown kind is a fail-stop, never a guess. When a lane
  holds on a full channel, a durable pop credits the producer through the
  step's returned wake mask, so a stalled lane never waits for an idle
  sweep. Every path is covered by the deterministic, threaded, and
  real-MsQuic suites, including a serve-shaped regression that drives the
  full lifecycle through the actual CLI composition on both drafts.
- **`streaming_objects = true` is the production receive mode**: the relay
  ingests `OBJECT_CHUNK` slices and forwards chunk-by-chunk with live-edge
  delivery within a shard, rather than surfacing whole-object events.
- **Server lifetime follows the facade, not any one connection.** The serve
  loop runs until signal-driven stop; it breaks only when `wait()` returns
  `MOQ_ERR_CLOSED` on a true facade terminal (stop / lane-pump exit). A
  per-connection terminal never ends the listener, so one bad client cannot
  take the relay down.
- **The transport admission cap uses the strict per-lane clamp.** Because
  each lane's manager consumes `lanes` core binding slots at K>1,
  `max_connections` is `lanes × usable_bindings_per_shard` (the external
  remainder per lane), not the old coarse `lanes × max_bindings`; each
  binding still enforces its own table, so uneven lane occupancy fails
  closed at the shard boundary. The CLI serve composition
  (`moqr_cli_serve_compose`) produces this cap and the shard config from the
  one builder, so `capacity` and `serve` never disagree.
- **Capacity output never overclaims, at any lane count.** `moq5-relay
  capacity` and serve startup print the complete process ceiling — including
  the cross-shard runtime and the CLI snapshot rows — for one lane or many;
  a lane-1 relay's ceiling is byte-identical whether admission is nominally
  on or off (K=1 builds no manager, so admission storage is zero).
- **Multi-version listener: one endpoint can serve several drafts.**
  `listener.versions` is an ordered, non-empty set of known drafts, most
  preferred first. The default is `[18]`; `[18, 16]` offers `moqt-18` and
  `moqt-16` from one listener, and ALPN selects each connection's draft.
  A single entry keeps the exact-version representation for byte-stable
  single-draft output.
- **Signal dumps: lanes own their traversal; who owns the aggregate depends
  on the admin endpoint.** Multi-lane SIGUSR epochs are latched by lock-free
  atomics. Each lane renders its own shard's route and journal dumps
  (SIGUSR1) and trace (SIGUSR2) inside its pump window and PUBLISHES its
  metrics row — that half is lane-owned in every composition. Exactly ONE
  multi-shard Prometheus document per epoch is then rendered from the
  published rows, and never by traversing a live lane's core, bind, journal,
  route or trace. With an admin endpoint configured, the admin thread is the
  broker's sole owner — including the transaction the latched signal demand
  opens — and emits that document through the signal sink from the same
  frozen rows the endpoint served; the main-thread coordinator deliberately
  does none of it, because two owners on one broker is the defect being
  avoided. Without an admin endpoint, the main-thread coordinator owns that
  render itself. Rendering waits until every row carries the newest requested
  epoch (newest-epoch-only coalescing);
  per-shard series keep their names, process aggregates use separate
  `moqrelay_process_*` names, and a lane's own metrics/stats are read
  through the stats seam (`moqr_shards_get_stats`), with route/journal epochs
  never aggregated.

## Public API fact table

The libmoq surface the relay relies on, with ownership semantics and
citations. Implementers extend this table rather than inferring — if a
needed fact isn't here, read the header, verify, and add the row. Citations
are source anchors in this checkout; when a cited header moves, update the
contract row, not just the path text.

| API | Contract that matters to the relay | Citation |
|---|---|---|
| `moq_session_on_data_rcbuf` | zero-copy ingest; session increfs/slices as needed; caller may decref on return | `core/include/moq/session.h:316` |
| `OBJECT_RECEIVED.payload/.properties` | **owned** refs transfer on poll; `moq_event_cleanup` mandatory | `core/include/moq/session.h:764-765`, `:1268` |
| `OBJECT_CHUNK` | owned chunk refs; zero-copy slices of transport buffers when eligible | `core/include/moq/session.h:903-920` |
| Borrowed event fields (names, namespaces, reasons) | valid only until the next advancing call (borrow epoch) | `core/include/moq/session.h:30-49` |
| `moq_resolved_token_t.token_value` | **BORROWED from output scratch**; deep-copy before any retention (DEFER, caches) | `core/include/moq/session.h:633-638` |
| `moq_session_write_object` / `_write_object_ex` | payload/properties increfed by the session before queueing; caller keeps its ref → N-way fan-out = N increfs of one rcbuf | `core/include/moq/session.h:2246`, `:2280`; incref at `core/src/session/session_subgroup.c:489` |
| `moq_session_open_subgroup` / `close` / `reset` | outgoing subgroup lifecycle per (group, subgroup); pooled, bounded | `core/include/moq/session.h:2239`, `:2286-2295` |
| Streaming send (`begin_object_ex` / `write_object_data` / `end_object`) | chunk-through egress; declared `payload_length` known up front | `core/include/moq/session.h:2414`, `:2431`, `:2447` |
| Fetch serving (`write_fetch_object` / `write_fetch_range` / `end_fetch`) | fetch data-stream egress incl. gap markers | `core/include/moq/session.h:1623`, `:1633`, `:1645` |
| Datagram send (`send_object_datagram`, pub variants) | lossy by contract; datagram-preference objects; status-only objects use `send_status_datagram` (`:1799`); the `properties` bytes are validated as a wire-form KVP block by the encoder (garbage ⇒ `MOQ_ERR_PROTO`) — relayed properties are wire-valid by origin, but synthetic ones must be encoded | `core/include/moq/session.h:1784`, `:1799`, `:1811` |
| `moq_session_request_goaway_*` | d18 per-request migration; keeps session + data streams alive | `core/include/moq/session.h:1658-1695` |
| `moq_redirect_target_t` on reject cfgs | REDIRECT (§10.6) targets for load shedding | `core/include/moq/session.h:1371-1391` |
| `MOQ_ERR_WOULD_BLOCK` on advancing calls | reserve-before-mutate: nothing half-applied; retry after draining actions | `core/include/moq/session.h:285-289` and per-call docs |
| `moq_session_has_transport_stream` | post- vs pre-retention distinction after WOULD_BLOCK (pre-retention ⇒ adapter goes fatal) | `core/include/moq/session.h:2464-2472` |
| `moq_action_cleanup` / poll `_ex` ABI | owned SEND_DATA refs transfer to poller; size-negotiated drains | `core/include/moq/session.h:539`, `:559` |
| `moq_rcbuf_*` shard contract | refcounts non-atomic; single affinity domain; **cross-shard transfer = `moq_rcbuf_clone` with destination-valid allocator, only** | `core/include/moq/rcbuf.h:7-16`, clone `:124` |
| `moq_rcbuf_wrap` / `slice` | zero-copy external wrap + zero-copy subranges (slices flattened to root) | `core/include/moq/rcbuf.h:76`, `:98` |
| `moq_alloc_t` seam | per-shard recycling pool plugs in here; rcbuf copies the vtable so last decref frees correctly | `core/include/moq/types.h:106-118`, `rcbuf.h:20-24` |
| `moq_handle_pack` family | public packed-handle layout the relay's handle universe reuses | `core/include/moq/types.h:201-226` |
| Per-request `*_cfg_init` (subscribe/publish/track-status/subscribe-namespace) | these DO fully `memset(sizeof(*cfg))`, zeroing appended fields — a plain init is safe (verified against the checkout) | `session_subscribe.c:2064`, `session_publish.c:591`, `session_track_status.c:386`, `session_namespace_sub.c:488` |
| `SUBSCRIBE_NAMESPACE` interest | draft-18 carries no interest field on the wire; the profile requires exactly `MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE` (draft-16 accepts 0/1/2) — use NAMESPACE_STATE for portable code | `core/src/session/profile_d18.c:696`; d16 validator `session_namespace_sub.c:539` |
| MsQuic managed (`moq_msquic_managed_t`) — the production relay transport | create/stop/destroy + `on_lane_pump` lane-thread confinement; multi-connection server; a client is exact-version, while a server offers either that one ALPN or an ordered `versions` list and selects per connection (the facade-level negotiated version is then 0; ask the connection inside the pump); `stop`/`wait` refused inside a callback | `adapters/msquic/include/moq/msquic_managed.h` |
| `moq_msquic_lane_next_conn` / `moq_msquic_managed_conn_session` | lane-local connection iteration + per-conn session, valid only inside `on_lane_pump`; a terminal conn stays visible for the batch delivering `SESSION_CLOSED`, then is reaped; `_conn_close(conn, code)` defers past the pump | `adapters/msquic/include/moq/msquic_managed.h` |
| `moq_msquic_managed_conn_user` / `_set_user` | per-connection state slot — the relay keys its per-conn binding tag here (never a pointer-keyed map: conn/session pointers can be reused by a successor connection) | `adapters/msquic/include/moq/msquic_managed.h` |
| `moq_msquic_managed_session` | CLIENT-only convenience — NULL on a SERVER and outside the pump; the relay server must NOT use it, it iterates lane conns instead | `adapters/msquic/include/moq/msquic_managed.h` |
| Other managed adapters (`moq_pq_threaded_t`, `moq_mvfst_managed_t`) — NOT the production relay transport | historical / non-production: same create/stop/destroy + `on_pump` confinement shape; the executable no longer links picoquic-threaded | `adapters/picoquic/include/moq/picoquic_threaded.h`, `adapters/mvfst/include/moq/mvfst.h` |

## Status

Implemented: the bounded log/cursor primitive with capacity model and trace
ring; the control plane (namespace trie, track table with coalescing,
pull-model subscribe delivery); retained standalone FETCH through the core
cursor and production binding, including FETCH over chunk-retained COMPLETE
records; and chunk-through ingest/delivery with live-edge stalls, downstream
reset propagation, and retained replay. The production session binding is
proven against real sessions over SimPair on both drafts.

The multi-shard runtime owns one core, binding, and trace per shard. Its
deterministic runner preserves reproducible schedule oracles; its concurrent
per-shard seam uses bounded directed control mailboxes and demand channels,
with sanitizer coverage for their cross-thread ownership. Announcements
replicate, rendezvous winners converge, losing publishers are force-withdrawn,
and remote subscribe demand round-trips to the owner with full ACK/DONE/cancel
lifecycle. The `moq5-relay` executable maps those shards onto one or more
MsQuic managed lanes. Same-shard and cross-shard object delivery are both
active; the shared CLI builder turns remote-demand admission on for every
multi-lane serve (`admit_remote_demand = (lanes > 1)`), with no operator
toggle, so the capacity command describes admission-on storage the moment
production would allocate it. A single lane keeps admission structurally
inert (K=1 builds no manager).

The observability surface includes bounded-cardinality counters (object flow,
refusals by resource, live entities by state, intent high-water), Prometheus
0.0.4 and OpenMetrics 1.0 expositions over core + binding snapshots, JSONL
trace, and entity-detailed route dumps (epoch triple, announces, namespace
watchers, tracks, log watermarks, and cursor state). SIGUSR1 emits metrics
plus routes and SIGUSR2 emits trace JSONL. Each lane renders only its own
shard's routes, journal and trace inside `on_lane_pump`; the one aggregate
metrics document is rendered from the published rows by whichever owner the
composition gives the broker — the admin thread when an endpoint is
configured, the main-thread coordinator otherwise. A
fixed-bucket forward-latency histogram observes successful downstream writes
from retained `arrival_us` to delivery time; blocked and failed writes are
excluded, and a regressed clock saturates to zero.

Authorization is a synchronous control-plane seam: the binding checks every
setup, announce, subscribe, namespace-subscribe, track-status, and publish
against a pluggable hook before the core acts. The default is allow-all (a
NULL hook, zero overhead); a bundled static-toy verifier (deterministic
action × namespace-prefix rules, no crypto) is the other in-tree option, and
the seam is shaped for a real CAT-4-MoQ verifier to drop in later. A setup
denial hard-closes the connection with the session UNAUTHORIZED code; a
request denial rejects with the request-level UNAUTHORIZED. A request-path
DEFER parks the request in bounded core storage (deep-copied action/namespace/
name/token bytes, capped by `max_parked` slots and `parked_bytes`), keyed by
the verifier's ticket; the verifier later calls `moqr_bind_auth_resolve`, and
the binding resumes the original op (ALLOW) or rejects it (DENY) — a core-
stores / binding-executes split, with binding close retiring the connection's
parked requests. A setup DEFER is not parked (it hard-closes, like a setup
denial). Decisions and denials fan out as bounded `moqrelay_auth_*` counters
(action × verdict, and denials by reason) — never any token bytes or peer
identity.

An ALLOW for a subscribe or an announce may carry a CAT `moqt-reval` lease
(`revalidate_after_us`): the binding then reserves a bounded core grant before
the op mutates routing state (deep-copied material, capped by `max_grants`
slots and `grant_bytes`) and commits it once the op succeeds — a reserve
failure fails the request closed with no wire accept. This holds for a
synchronous ALLOW and equally for an async ALLOW that resolves a parked DEFER,
so an async-approved subscription is revalidatable, not permanently authorized.
On each `tick`, a due grant re-invokes the hook off the control-plane clock
(never on the object path): a renewed ALLOW reschedules, an ALLOW without a
lease clears the grant, and a DENY/DEFER revokes — a subscribe by a wire
SUBSCRIBE_DONE, an announce by withdrawing the namespace and cancelling it
toward the publisher. The announce cancel is backpressure-safe: the revoked
grant is the durable record (peeked, not drained) and the cancel is retried on
the next pump until it lands, so a session-queue WOULD_BLOCK never drops it. The
denial code is preserved (the verifier's custom code, else UNAUTHORIZED), except
on draft-18 announce cancellation, which the wire expresses as a request-stream
reset carrying only the fixed §3.3.3 CANCELLED code, not a request-error code.
Binding close retires the connection's grants, so a later tick never touches a
torn-down session.

Not yet implemented: cross-shard standalone FETCH (fetch serves only
shard-local retained logs — the cross-shard message set carries object and
demand kinds, no fetch kind); external selector-based auth revocation
(`AUTH_REVOKE` — timer-driven lease revalidation is wired, immediate selector
revoke is not); qlog, OpenTelemetry export, tenancy or authorization scopes,
and paged route/session views. A second WebTransport listener exists as
conditional code, compiled only where the wtquic managed adapter target is
present and absent from the raw-only build. Nothing here claims browser
compatibility or a particular negotiated profile for it. Its Origin
authorization policy and allowlist are resolved from configuration and handed
to the transport. The explicit native boundary fixture has exercised all ten
Origin-policy rows over loopback on the supported macOS MsQuic lane, with the
client verifying a private-CA server certificate and its DNS identity before
the Origin decision. That proves the configured authorization boundary; it is
not evidence of browser compatibility or of a browser selecting any particular
WebTransport profile.
Cross-shard object admission and the process-wide multi-lane capacity
calculation are both complete: admission is on for every multi-lane serve
and the capacity command prints the closed-form process ceiling at any lane
count.

An optional loopback HTTP endpoint serves three read-only resources from the
same generations: `GET /metrics` (content-negotiated between the two
expositions; a missing or `*/*` Accept selects OpenMetrics, an unsatisfiable
one is `406`), `GET /api/v1/info` (an immutable document of the configuration
the process is running, from an allowlist that admits no credential path or
authorization rule), and `GET /api/v1/shards` (one JSON row per shard from the
frozen snapshot). Any other target is `404`. It is TCP loopback only — a
non-loopback host is refused at parse time — has no authentication, and is off
unless configured; the endpoint is part of the relay's health, so an owner
that stops for any reason but shutdown stops the relay rather than leaving a
dead surface advertised. A shard row states its capability rather than
inventing counters: a single-lane serve composes no shard runtime and reports
`absent` with the core and binding families only, while a refused row poisons
the generation into a `500`. Storage for every bank, row and document is
included in the capacity ceiling; the admin thread stack is reported beside it
as a reservation.

Serve output is text by default, byte for byte as before. `logging.format =
json` instead makes the serve's standard output one JSON event per line — the
same versioned records under a `schema` member, with a nullable `elapsed_us` —
and moves every human message, the capacity ceiling included, to standard
error. Each event is serialized completely before any of it is emitted, and
the sink stops for the rest of the run after one diagnostic on the first
write or flush failure, appending no later event. That is the whole
guarantee: it is not atomic delivery to stdio or to a reader, and a failed
write or flush can leave an incomplete final line, so a consumer must treat an
unterminated last line as absent. A successful flush hands bytes to the
operating system and is not durable delivery. Ordinary signal output remains
synchronous and can block on stderr.

Metric naming is deliberately clean-room: a distinct `moqrelay_*` prefix,
concept-aligned with common relay metrics but not copied from any schema
(no Prometheus schema exists in the workspace to copy — the comparison
stack uses fb303/ServiceData callbacks, not exposition text). Labels are
bounded by construction — shard, transport, draft version, and per-series
enum reason/state — never namespace, track, session, or cursor identity;
per-entity detail lives in the route dump and the trace ring.
