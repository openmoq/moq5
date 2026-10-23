# MoQ Cluster draft-01 support: design specification

- **Date:** 2026-10-22
- **Branch:** `feature/moq-cluster-01`
- **Baseline:** `2a305fc7158c47501391d6f59774b157e215df13`
- **Status:** approved and implemented; verification results are recorded in
  [moq-cluster-01-plan.txt](moq-cluster-01-plan.txt)

## Purpose and scope

Implement draft-lcurley-moq-cluster-01 across moq5's draft-22 session engine
and reusable relay libraries. Applications must be able to negotiate cluster
metadata, originate and update advertisements, and route subscriptions through
a mesh without advertisement or subscription cycles. A codec-only change does
not satisfy this scope. Use [draft-lcurley-moq-cluster-01.txt](draft-lcurley-moq-cluster-01.txt) as the pinned
extension reference and [draft22-wire-reference.md](draft22-wire-reference.md) for the current base
protocol mapping. The extension references transport draft-21; retain its
extension semantics on draft-22's existing request streams and acknowledgments.
Do not claim transport draft-21 wire support.

Preserve existing draft-16, draft-18, and unnegotiated draft-22 behavior. Cluster
configuration is opt-in and is refused on unsupported profiles. Do not enable
the extension on legacy wire profiles by implication. Preserve authorization,
retained media, request identities, and existing backpressure contracts.

The reusable core and relay runtime remain sans-I/O. Socket creation, peer
dialing, worker scheduling, credentials, and reconnect policy belong to the
host. This change does not introduce automatic peer discovery or a managed
cluster service. A host can attach established sessions as cluster peers.
Separate pattern-extension features are outside scope; describe their absence
explicitly rather than inventing cluster-specific `NO_CAPACITY` semantics.

## Session configuration and public API

Add size-aware, append-only cluster configuration and metadata using the
existing C ABI conventions. An explicit enabled flag distinguishes disabled
from enabled with Hop ID zero. Expose the negotiated state, peer Hop ID,
and effective peer relay cost to hosts and relay bindings. Both endpoints must
offer `HOP_ID` for a session to use the extension. Negotiation and peer metadata
are stored separately for every session.

Applications can supply a configured `uint64_t` Hop ID; zero is valid and
withholds identity. Do not generate a new identity for each advertisement.
Expose sender relay cost with a presence flag: absent is 1 and explicit zero
is free. A relay uses one configured local identity across its peer sessions.
Provide an explicit relay policy override for an untrusted peer's link cost;
Hop IDs never authorize requests or bypass the existing authorization hook.

Namespace publication and update configuration accept a borrowed ordered
array of Hop IDs and an independently present `uint64_t` route cost. Internally
retain owned copies for the advertisement lifetime. Namespace discovery events
expose metadata with the session's documented borrowed-output lifetime. Add a
size-aware send function for `NAMESPACE` metadata while retaining the existing
function for callers that do not configure clustering. Old configuration
prefixes remain valid and default to extension disabled.

Preserve frozen pointer-only initializer writes: initialize appended config
fields only through existing sized initializers or a new explicit sized API.
Do not enlarge writes to callers compiled against earlier headers. Fixed-size
event reserves and structs embedded in other public structs are ABI limits,
not permission to grow their layout; use reserved capacity or a separate
size-aware record/query when an extension does not fit.

## Wire representation and validation (draft sections 3, 5, 6.3, 10)

Register `HOP_ID` setup option `0x40B54` and `RELAY_COST` option `0x40B56` as even
integer KVPs. Register `HOP_PATH` parameter `0x40B57` as an odd byte-string KVP
whose value is a nonempty sequence of complete `vi64` integers. `ROUTE_COST`
parameter `0x40B58` is an even integer KVP. Use the full `uint64_t` `vi64` range,
including nonminimal encodings accepted by the existing decoder.

Cluster message parameters are legal only in `PUBLISH_NAMESPACE`, its
`REQUEST_UPDATE`, and negotiated `NAMESPACE`. Do not represent high extension
codes in the existing low-parameter 64-bit presence mask or shift by them.
Track extension presence and duplicate detection independently. Reject
duplicate singleton extension values and preserve existing delta encoding,
scope checks, and structural error classification for the rest of the KVPs.

Every negotiated `PUBLISH_NAMESPACE` and `NAMESPACE` carries `HOP_PATH`. Extend
`NAMESPACE` by exactly one parameter block following its namespace suffix;
`NAMESPACE_DONE` has no extension block. Unnegotiated send paths must not emit
cluster parameters or this block. Missing `HOP_PATH`, an empty path, an
incomplete `vi64` inside the bounded path value, or repeated nonzero IDs close
the session with `PROTOCOL_VIOLATION`. Repeated zeros are valid. Fragmented
outer messages wait for more bytes without exposing partial metadata.

`PUBLISH_NAMESPACE` updates use `REQUEST_UPDATE` on the original request stream,
and follow its request-ID, `REQUEST_OK`/`REQUEST_ERROR`, and update-limit rules.
Omitted metadata retains its prior value; explicit route cost zero replaces
the old cost. An error response closes the stream and withdraws the route.
`NAMESPACE` updates repeat `NAMESPACE` on the same namespace response stream and
must be delivered as updates, not rejected as duplicates.

Apply an update atomically after validation and capacity reservation. Preserve
subscriptions, retained state, and the advertisement handle on a same-origin
update, including a first path entry of zero. A first-entry change requires
withdrawal and a new advertisement; do not silently rewrite the publisher.
A session maintains at most one advertisement for one full namespace;
duplicate detection must span its namespace advertisement mechanisms.

## Relay route storage and selection (draft sections 4, 6, 7, 8)

Replace the namespace trie's single advertisement slot with bounded route
records keyed by namespace, source binding/generation, and advertisement
lifetime. Keep namespace indexing separate from route ownership. Retain
alternative routes without overwriting the live source of a track. Include
route/path storage in configuration resolution, memory ceilings, capacity
statistics, teardown, and diagnostic inspection.

A route records the received path, accumulated cost, arrival sequence,
source session identity, and original publisher identity. A route from a
non-cluster peer is bridged with path [0]; append the forwarding relay's
configured ID when sending downstream. Received zeros remain unchanged.
Locally assigned correlation identities, if supplied by a host, remain local
selection state and must never replace zero in transmitted `HOP_PATH`.

Drop routes containing the relay's own nonzero ID. A local ID of zero never
causes rejection solely because a path contains zero. Add the sender's
effective relay cost before selecting or forwarding a route, saturating at
`UINT64_MAX`. Retain the accumulated value separately from any advertised warm
discount. Configurable capacity limits never silently truncate a hop path.

For a requested namespace, identify its most specific advertised prefix.
Within that tier, remove routes containing the requester's declared nonzero
Hop ID, and routes returning to its source session or correlated peer.
Use this same eligibility rule for advertisement, `SUBSCRIBE`, and upstream
`FETCH` dispatch. An exhausted tier is unroutable. Among remaining routes,
prefer paths without zero, then lower accumulated cost, then fewer hops,
then the most recently received. Maintain deterministic arrival ordering.

Advertise at most one selected route for each namespace on each session.
Selection is per recipient: a peer excluded from the active path can receive
the best eligible standby. Withdraw the old advertisement when no eligible
path remains. Changes to selected metadata use an atomic update on the
existing advertisement; changes to selected publisher withdraw before
re-advertising, keeping advertised publisher and served publisher consistent.

Unknown origins are distinct between advertisements even if both begin with
zero. Only a shared nonzero first ID establishes interchangeable publishers.
Do not merge their group sequences, retained objects, or joining-`FETCH` state.
Pin each live track ingest and associated `FETCH` to its selected source and
generation so delayed teardown cannot redirect an old request to a new
publisher. Preserve one active source per subscription.

Same-origin failover may resume at a group boundary; it must not splice two
sources into one group. A different or unknown origin is a discontinuity:
terminate or re-establish affected requests explicitly and prevent old cache
entries from satisfying requests against the replacement publisher. Keep
the relay's existing retained-log accounting and cancellation behavior.

## Warm-route cost behavior (draft section 6.2)

When a namespace's selected source has a live track subscription, advertise
cost zero for that source. Keep standby routes at their accumulated costs.
Restore the accumulated cost when the last relevant live subscription ends.
Send explicit zero when entering the warm state, since omission preserves
the prior value. Re-evaluate recipients on cost and membership changes.

Before moving an active ingest to another warm relay (received cost zero,
at least two path entries), use one deterministic ordering of namespace
bytes and the two relay IDs so both sides cannot switch to each other.
Equal IDs, including equal zero IDs, do not re-parent. This selection policy
must be pure and reproducible in the relay's deterministic simulator.

No special standby seed guarantee is claimed by default. If deployment
policy enables that guarantee, expose and enforce admitted hop and charged
link-cost bounds, including the receiving link, and exclude unknown,
out-of-budget, and saturated paths from ranking above guaranteed standby.

## Ownership, backpressure, and integration

Use existing allocators, bounded pools, generations, and commit-last patterns.
A refused allocation, full event queue, or full action queue leaves the prior
advertisement and subscriptions intact. Retry must not duplicate metadata
updates, withdrawals, costs, or hop appends. Copy borrowed namespace/path
metadata before the next advancing session call. Free routes on stream end,
withdrawal, binding removal, and session close.

Replicate route metadata and invalidations through the existing shard runtime
with bounded messages and the full wake mask. All shards must apply the same
route and source identity; preserve authorized request material across the
queue. Extend memory bounds and trace data together with the data structures.
Adapters forward session bytes and events without becoming route selectors.

## Expected implementation locations

- `core/include/moq/session.h`: size-aware opt-in config, metadata and events.
- `core/src/wire/control_d22* and params_d22*`: setup and high-code KVP support.
- `core/src/wire/messages_d22*`: negotiated `NAMESPACE` parameter-block codec.
- `core/src/session/profile* and session_internal.h`: profile capability and state.
- `core/src/session/session_setup.c`: transactional peer option commit.
- `core/src/session/session_namespace*.c`: publication, discovery and updates.
- `relay/include/moq/relay/`: bounded cluster configuration and route metadata.
- `relay/src/core/`: route storage, pure selection, trace and memory accounting.
- `relay/src/bind/`: session integration, updates and source-pinned forwarding.
- `relay/src/`: shard propagation where the existing runtime owns it.
- `tests/, relay/tests/, ABI/export manifests and CMake registration`: validation.

Split new helpers by responsibility; avoid unrelated refactoring.

## Validation and acceptance

Use failing focused tests before implementation, then run the matching tests
after each layer. Verify setup negotiation independently on both endpoint
perspectives, explicit Hop ID zero, absent versus zero relay cost, high KVP
codes, full-width integers, path validation, fragmented input, parameter scope,
and unchanged unnegotiated `NAMESPACE` bytes.

Exercise same-stream partial updates, repeated `NAMESPACE`, duplicate streams,
origin changes, request-error withdrawal, and resource exhaustion with queued
events/actions. Check rollback and allocator cleanup under failure injection.

Relay fixtures cover a looped mesh, redundant sessions to one relay, anonymous
origins, asymmetric link costs, saturating addition, longest-prefix choice,
per-peer standby selection, equal costs and arrival ordering, warm transitions,
deterministic re-parenting, source loss, joining `FETCH`, standalone `FETCH`,
multiple publishers, stale generations, and cross-shard metadata propagation.

Require catalog or initialization and progressing media objects through at
least two cluster relay hops. Test native QUIC and WebTransport independently
when their adapters and fixtures are available; distinguish sans-I/O delivery
from live transport evidence. Locate the user's moq-pub reference app before
using it; the named sibling was absent at design time, so do not report it
as tested. Use deterministic session publishers for the library acceptance.

Run the existing core, relay and enabled service suites, an ASan/UBSan build,
ABI/export and installed-consumer checks, and git diff --check. Record exact
commands, counts, failures, skips and unavailable transport coverage. A setup
handshake or passing codec test alone is not cluster delivery acceptance.
