# Core engine

| Responsibility | File |
| --- | --- |
| Configuration, lifecycle, epochs and tick | `relay.c` |
| Track registry, keys and namespace trie | `tracks.c` |
| Namespace discovery, announcements and cluster routes | `namespace.c` |
| Subscriptions, filters and updates | `subscribe.c` |
| Upstream resolution | `upstream.c` |
| Publication, ingest and capacity status | `publish.c` |
| Binding lifecycle and retained delivery pins | `bindings.c` |
| Ready set, group progress and object delivery | `delivery.c` |
| Retained FETCH cursors | `fetch.c` |
| Ordered output intents | `intents.c` |
| Authorization, deferred requests and grants | `auth.c` |
| Statistics and diagnostic names | `stats.c` |
| Read-only route dumps | `route_dump.c` |
| Bounded object log and retained storage | `log.c` |
| Structured diagnostic traces | `trace.c` |
| Public value initialization and error names | `types.c` |
| Per-draft protocol error and status codes | `wire_codes.c` |
| Property encoding and decoding | `props_codec.c` |
| Capacity policies and resource accounting | `capacity.c` |

`relay_internal.h` owns shared private state. Owner-specific internal headers
declare helpers used across these files. Public headers remain under `include/`.
