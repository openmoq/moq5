# Session binding

| Responsibility | File |
| --- | --- |
| Configuration, lifecycle, connections and pump | `bind.c` |
| Session events | `events.c` |
| Authorization and deferred admission | `authorization.c` |
| Namespace and track discovery, cluster announcements | `discovery.c` |
| Subscription-update resolution | `subscription_update.c` |
| Ordered output intents and their retained data | `intents.c` |
| Held accept, refuse and terminal answers | `request_answers.c` |
| Upstream ingest, cancellation and malformed-track cleanup | `upstream.c` |
| Track and object property handling | `metadata.c` |
| Retained retrieval and standalone FETCH | `fetch.c` |
| Forwarded Joining FETCH | `fetch_joining.c` |
| Subscription-owned fill retrieval | `fetch_fill.c` |
| Downstream object delivery and scheduling | `delivery.c` |
| Statistics and latency histograms | `stats.c` |
| Private fault-injection controls | `testing.c` |

`bind_internal.h` owns the shared private layout. Other internal headers declare
only helpers needed across implementation files. The fault-injection source is
compiled solely into the private test-internals target.
