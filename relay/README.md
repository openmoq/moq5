# MOQ5 Relay Libraries

The forwarding implementation used by `tools/moq5-relay`, available to other
C and C++ applications without adopting the command's process model.

| Component | Target | Contents |
| --- | --- | --- |
| `relay-core` | `moq::relay-core` | Routing, retention, capacity, deterministic traces |
| `relay` | `moq::relay` | Session binding, shard coordination, pure metrics rendering; includes relay-core |

Neither library opens sockets, starts threads, installs signal handlers, or
owns stdout. The engine depends only on `moq::core`. The runtime also uses
platform thread synchronization for its bounded cross-shard queues; the host
owns the workers and schedules every step. Transport adapters remain optional
application dependencies.

## Cluster Routing

The reusable libraries support the
[MoQ Cluster draft-01 extension](../docs/draft-lcurley-moq-cluster-01.txt)
on draft-22 sessions. Enable it explicitly when constructing the relay core:

```c
moqr_core_relay_cfg_t core_cfg;
moqr_core_relay_cfg_init_sized(&core_cfg, sizeof(core_cfg), moq_alloc_default());
moqr_cluster_cfg_t cluster;
moqr_cluster_cfg_init(&cluster);
cluster.enabled = true;
cluster.local_hop_id = 1234; /* one identity across this relay's sessions */
moqr_core_t *core = NULL;
moq_result_t result = moqr_core_create_cluster(&core_cfg, &cluster, &core);
```

For the shard runtime, set `cluster_enabled`, `cluster_hop_id`, and optional
`max_cluster_routes`/`max_cluster_hops` on `moqr_shards_cfg_t`. Set
`cluster_enabled` and the same `cluster_hop_id` on each peer's
`moq_session_cfg_t` before creating its session. The peer must also offer the
extension. Zero is a valid withheld identity; an explicitly configured relay
cost of zero differs from an omitted cost, which defaults to one.

After attachment, the binding discovers namespaces on negotiated peer sessions,
retains alternative paths, and applies the same peer exclusions to namespace
advertisements and content requests. The host supplies connections and drives
the transport and binding pumps. `moqr_bind_conn_open_cluster()` can override
a peer's asserted relay cost and correlate connections known to belong to one
endpoint. These identities guide routing; the existing authorization hook
continues to decide access.

For capacity planning before creating the core, resolve the cluster settings
with `moqr_cluster_cfg_resolve()` and pass them to
`moqr_bind_cluster_capacity_describe()`. `moqr_shards_capacity_describe()`
includes cluster pools, metadata queues and binding storage.

See the [cluster API contract](docs/api.md#cluster-configuration-and-metadata)
for metadata ownership and [routing architecture](docs/architecture.md#cluster-routing)
for path selection and failover. The
[conformance page](../docs/conformance.md#moq-cluster-draft-01) records test
coverage and remaining validation limits. Peer dialing and reconnect policy
belong to the host; the relay command does not configure cluster peers.

## Session Binding

The binding serves draft-16, draft-18 and draft-22 sessions and forwards
between them. On draft-22 it answers subscriber updates itself (applying a
replacement Location Filter before acknowledging it), serves fills from the
retained log with Timed-Out markers at the fill's deadline, retrieves a
fill's missing range from the owning shard, resolves FETCH ranges against
its own Largest Object, and retrieves upstream the part of a standalone
FETCH it does not hold, in either group order, with the FETCH routed to the
shard that owns the track. It turns on update deferral and deferred
FETCH ranges on every session it attaches, so a host must not rely on those
sessions answering on their own. A host that pumps on its own schedule wakes
for `moqr_bind_next_deadline_us()`. Details and limits:
[docs/architecture.md](docs/architecture.md#draft-22-behaviour).

## Build And Install

From the project root, for the engine and runtime without the command:

```sh
cmake -S . -B build -DMOQ_BUILD_RELAY_CORE=ON \
    -DMOQ_BUILD_RELAY_RUNTIME=ON -DMOQ_BUILD_RELAY_TOOL=OFF \
    -DBUILD_SHARED_LIBS=ON
cmake --build build
cmake --install build --prefix /your/prefix
```

For just the thread-free engine, omit `MOQ_BUILD_RELAY_RUNTIME`. Set
`BUILD_SHARED_LIBS=OFF` for static libraries. As with other project components,
unrelated default examples/tests/media components can be disabled separately.

`MOQ_BUILD_RELAY` remains the compatibility switch: on a fresh configuration
it defaults all three new options to ON. Explicit options take precedence.
Runtime without core, or tool without runtime, is a configuration error.
None of these switches enables a transport backend.

Public headers install under `include/moq/relay/`, libraries under the selected
GNUInstallDirs library directory. The command remains `bin/moq5-relay`.
Internal inspection hooks and the toy authorization policy are not installed.

## Consume

```cmake
find_package(libmoq CONFIG REQUIRED COMPONENTS relay)
target_link_libraries(my_relay PRIVATE moq::relay)
```

An engine-only consumer requests `relay-core` and links `moq::relay-core`.
Base `find_package(libmoq)` does not import either relay target. Asking for a
component not installed fails configuration. Headers have C++ linkage guards;
there is no separate C++ relay API.

The matching pkg-config modules are `libmoq-relay-core` and `libmoq-relay`.
They use an additive minimal `libmoq-core.pc`, installed with the relay SDK,
instead of the existing enabled-component aggregate `libmoq.pc`. Use
`pkg-config --static` for the complete static link closure. Shared-library
loader search policy remains the consuming application's responsibility.

## Embed

Start with [examples/in_memory.c](examples/in_memory.c), a complete deterministic
publish/subscribe/delivery cycle with an explicit backpressure acknowledgement.
It uses only installed engine headers and runs without networking or a clock.

For a complete network host, see
[`examples/relay/simple-relay`](../examples/relay/simple-relay/README.md).
It uses the installed runtime and managed MsQuic APIs, with no CLI-private
headers or forwarding code. The source also installs under
`relay/examples/simple-relay/` alongside this document.

For real sessions, use `<moq/relay/moqr_bind.h>` and attach caller-owned
`moq_session_t` objects on their owning lane. For multiple lanes, construct a
`moqr_shards_t` and attach to its borrowed per-shard binding. Consume the full
wake mask returned by `moqr_shards_step_shard`; do not substitute a polling
timer. The command's adapter composition is in `tools/moq5-relay/cli/main.c`.
A standalone managed-server example and real-transport acceptance are a
separate integration step, not implied by installing these libraries.

Read [the API contract](docs/api.md) before retaining views, sharing an allocator,
reading stats, or shutting down. [architecture.md](docs/architecture.md) describes
the implementation and the CLI boundary. Tests and model fixtures live under
`tests/`; the exact exported function lists are in `abi/`.

This SDK does not yet provide a managed relay service, JNI/Kotlin bindings,
scopes, qlog, or a paginated live routing API.
