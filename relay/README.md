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
