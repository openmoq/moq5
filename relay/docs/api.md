# Relay Embedding Contract

## Interface And Layout

The installed headers and the function lists in `relay/abi/` define the public
interface. The two libraries have independent export/import macros and follow
the project's `BUILD_SHARED_LIBS` and release policy. No independent ABI version
or distro release is assigned by this extraction.

Existing public value types are **fixed v1 layouts**, including stats, handles,
intents, record/delivery views, limits, capacity descriptions, callback tables,
and nested configuration templates. Their size, member offsets, array stride,
and numeric constants cannot be changed compatibly. A future incompatible
layout requires a new type and entry point, not an append to an unsized record.
This also applies to records that happen to have an internal `struct_size` but
use a fixed-size initializer. A size field alone is not an extension contract.

Configuration entry points ending in `_init_sized` accept caller byte sizes.
They clear and initialize only the known, fully present fields. Pass the actual
allocation size, keep the resulting `struct_size`, and follow each resolver's
minimum-prefix rules. Do not claim a larger allocation or use packed structs.
Nested core/binding templates in the shard config must be initialized in full
with the shipped initializer; they are not independently extensible tails.
Existing enum and capacity validation continues to apply before construction.

The four `_get_stats_sized` functions require space for the complete v1 record.
An undersized buffer or NULL object/output returns `MOQR_ERR_INVAL` without
modifying output. A valid larger allocation receives exactly the v1 record;
its tail is untouched. A shard lookup failure likewise leaves output unchanged.
The older unsized getters remain available for a full v1 object, not a shorter
prefix. No getter makes a mutable owner safe to read from another thread.

Public declarations use only C types and opaque library objects. Test counters,
seal/inspection hooks, binding authorization glue, and the CLI toy policy are
private; defining a testing macro does not expose them from installed headers.

## Ownership

| Resource | Contract |
| --- | --- |
| Engine/log/trace | Created and destroyed through their respective API; no implicit process lifetime |
| Standalone binding | Owns its tables, borrows its engine and attached sessions |
| Shard runtime | Owns its engines, bindings, traces and bounded channels |
| `moqr_shards_core/bind/trace` return values | Borrowed children; never destroy independently or use after parent destruction |
| Allocator/context | Keep both alive through destruction and final retained-buffer release |
| Policy/router callbacks and contexts | Application-owned; valid until all invoking lanes are quiescent |
| Namespace/name input spans | Borrowed for the call unless that declaration explicitly specifies otherwise |
| Ingest payload/properties | Transfer on `MOQR_OK`; remain caller-owned on refusal |
| Delivery/fetch/intent views | Borrowed under the specific declaration's advancing-call and acknowledgement rules |

Do not retain a namespace pointer from an intent across an advancing core call.
Consume borrowed router intents in-call; only the documented scalar-safe kinds
can be deferred. Borrowed delivery buffers must not outlive their delivery
acknowledgement. Use `moq_rcbuf_incref` when the buffer contract permits retention
beyond a call, and match it with a decref; a copied pointer is not a retained ref.
Destroying a relay abandons retained relay work and releases its references; it
does not close transports or prove application delivery succeeded.

## Scheduling

An engine, binding, attached session and its stats have one owner. Invoke them
serially on that owner. Callbacks must not reenter advancing relay operations;
queue work back to the owner instead. Use each session's actual negotiated MoQ
version; neither an offered ALPN list nor a facade-wide default is sufficient.
The binding must be the only consumer of that session's events; do not attach
a publisher/subscriber facade that consumes the same queue.

The shard runtime has two mutually exclusive modes per instance:

- `moqr_shards_step`: a single caller drives deterministic rounds with explicit
  monotonic microsecond time. No wall-clock calls or background worker exists.
- `moqr_shards_step_shard`: one owner per shard; obey its complete wake-mask and
  continuation contract, including self-wakes and producer-credit notifications.

Cross-shard payloads can be released on another lane. Supply an allocator whose
allocation/release operations meet the runtime's thread-safety requirements.
Do not infer cross-thread snapshot safety from locks protecting the channels.
Collect owner-local snapshots first, then render immutable copies elsewhere.
The CLI's broker is not an installed synchronization API.

`MOQR_ERR_WOULD_BLOCK` requires progress and retry, not disposal or success.
Follow the individual operation: ordinary admissions reserve before mutation,
but binding close and forced namespace withdrawal explicitly permit resumable
partial progress. Drain intents and retry them until complete. Delivery
acknowledgements distinguish write backpressure, pre-begin refusal, live-edge
stall and terminal error; those outcomes are not interchangeable.

## Shutdown And Capacity

The embedding host supplies the lifecycle: construct, attach sessions, pump,
stop admitting work, detach/drain, quiesce, then destroy. Stop transport callbacks
and retire every in-flight lane callback before freeing their relay context.
While sessions remain alive, finish the binding's connection-close processing
on their owner and continue bounded progress when requested. Only after no
callback, caller, session event consumer or retained borrowed view can touch a
child may the parent runtime be destroyed. The runtime never joins the host's
threads or calls `exit` to enforce this rule.

Capacity descriptions bound relay allocation requests according to their
documented eager/lazy terms. They do not include allocator overhead, application
payloads outside those terms, MoQ sessions, adapters, QUIC/TLS, kernel buffers,
or host thread stacks. The pure metrics renderers consume supplied snapshots
only; callers must not turn an unavailable/incomplete snapshot into zeros.

Two independent engines and runtimes can coexist. There is no process-global
relay owner, allocator or policy context. Isolate their callbacks and schedule
them independently; closing one must not invalidate the other's objects.
