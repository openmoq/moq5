# D16 Conformance Status

Factual status of draft-ietf-moq-transport-16 support in libmoq.

## Control Messages

| Message | Codec | Session API | Scenario | Notes |
|---------|-------|-------------|----------|-------|
| CLIENT_SETUP / SERVER_SETUP | Full | Full | lifecycle, auth | Request capacity, auth token cache negotiation |
| SUBSCRIBE | Full | Full | lifecycle, object, backpressure | All filter types; params validated per scope |
| SUBSCRIBE_OK | Full | Full | lifecycle | LARGEST_OBJECT, EXPIRES, track properties |
| REQUEST_ERROR | Full | Full | Multiple | Shared across all request families |
| REQUEST_OK | Full | Full | track_status, fetch | Per-target param validation |
| UNSUBSCRIBE | Full | Full | lifecycle | Publisher resets open subgroups |
| REQUEST_UPDATE | Full | Full | subscribe, fetch, publish | Subscription + PUBLISH: auto-accept priority/forward/timeout with outbound API + tombstones. Other targets: REQUEST_ERROR(NOT_SUPPORTED). |
| PUBLISH | Full | Full | publish | FORWARD enforcement, alias collision prevention |
| PUBLISH_OK | Full | Full | publish | Subscriber priority, group order, delivery timeout, expires |
| PUBLISH_DONE | Full | Full | publish | Status codes including UPDATE_FAILED |
| FETCH | Full | Full | fetch | Standalone + relative/absolute joining |
| FETCH_OK | Full | Full | fetch | End location validation, track properties |
| FETCH_CANCEL | Full | Full | fetch | Tombstone for late data race |
| TRACK_STATUS | Full | Full | track_status | Stateless; LARGEST_OBJECT/EXPIRES on OK |
| GOAWAY | Full | Full | goaway | Drain timeout, new-session URI |
| MAX_REQUEST_ID | Full | Full | request_credit | Via grant_request_capacity API |
| REQUESTS_BLOCKED | Full | Full | request_credit | Event: REQUEST_READY |
| PUBLISH_NAMESPACE | Full | Full | namespace_sub | Auth token staging |
| PUBLISH_NAMESPACE_DONE | Full | Full | namespace_sub | — |
| PUBLISH_NAMESPACE_CANCEL | Full | Full | namespace_sub | — |
| SUBSCRIBE_NAMESPACE | Full | Full | namespace_sub | Bidi stream, auth, forward |
| NAMESPACE | Full | Full | namespace_sub | Via send_namespace API |
| NAMESPACE_DONE | Full | Full | namespace_sub | Via send_namespace_done API |

## Data Plane

| Feature | Codec | Session API | Scenario | Notes |
|---------|-------|-------------|----------|-------|
| Subgroup streams (ZERO/FIRST_OBJ/PRESENT) | Full | Full | object, streaming_object | Incremental parser with chunked delivery |
| Object properties/extensions | Full | Full | object, streaming_object | Owned rcbuf on events |
| Object status (NORMAL/END_OF_GROUP/END_OF_TRACK) | Full | Full | object | — |
| Streaming objects (OBJECT_CHUNK) | Full | Full | streaming_object | begin/end/write_data API |
| Fetch data stream (FETCH_HEADER + objects) | Full | Full | fetch | Objects, gaps, FIN completion |
| Object datagrams | Full | Full | object_datagram | Payload, properties, status; budget-accounted |
| Delivery timeout (subgroup reset) | Full | Full | streaming_object | Per-subscription and per-update timeout |

## Auth / Security

| Feature | Status | Notes |
|---------|--------|-------|
| AUTH_TOKEN inbound processing | Full | Cache with register/delete/use, staged transactions |
| AUTH_TOKEN outbound sending | Full | All request types: subscribe, fetch, publish, publish_namespace, track_status |
| Setup auth token | Full | Processed and cached on both sides |

## Known Limitations

- **REQUEST_UPDATE**: Auto-accepts priority, forward, and delivery timeout for subscriptions and PUBLISH-created subscriptions. Outbound update API for both subscription and publication sides. Pending update tombstones across PUBLISH_DONE and UNSUBSCRIBE. Filter, auth token, new group request, and subscription filter params are rejected as unsupported. Other targets (fetch, namespace, track-status) return REQUEST_ERROR(NOT_SUPPORTED).
- **d18 profile**: Implemented in the tree (see the draft 21 section below for the newest profile and the interop runner results).
- **QUIC transport**: Sans-I/O core. In-tree adapters: picoquic (raw QUIC), mvfst (raw QUIC), picoquic WebTransport, and proxygen WebTransport. The WebTransport adapters are experimental.
- **Facades**: Publisher and subscriber facades exist; relay facade not started.
- **Joining fetch filter**: Only LARGEST_OBJECT filter is valid per spec; client and server both enforce this.

## Test Infrastructure

| Layer | Count | Coverage |
|-------|-------|----------|
| Unit tests (CTest) | 52 sim + 31 nosim | Codec, session, vectors |
| Interop vectors | 33 binary fixtures | Decode + byte-identical re-encode |
| Seeded scenarios | 20 runners x 1000 seeds | Deterministic trace hash verification |
| Boundary checks | 2 scripts | API vocabulary + profile vocabulary |
| OOM sweep | 14 scenarios | Fail-at-N allocator exhaustive |


## Draft 21 (draft-ietf-moq-transport-21)

Status of the draft-21 profile (`core/src/session/profile_d21.c`, codec in
`core/src/wire/control_d21.c`), offered first by the endpoint ({21, 18, 16}). The wire
facts and the ambiguities are in `docs/draft21-wire-reference.md`; the build plan and
per-task results are in `docs/draft21-implementation-plan.md`.

| Area | Status |
|------|--------|
| Control codec, setup options, request layer | Full (draft 21 message set, REQUEST_OK for PUBLISH_OK, PUBLISH_SKIPPED, STATE_NOTIFY, GOAWAY without Request ID) |
| SETUP AUTHORITY / PATH | Sent by a native-QUIC client on the picoquic, msquic and mvfst endpoint paths (also under draft 18); not by the WebTransport paths, which must not |
| Subscriptions | Concurrent subscriptions per Track, exact Location Filter (relative start, end Object), Range Filters declined with INVALID_FILTER |
| PUBLISH | Accept sends non-default choices as a follow-up REQUEST_UPDATE; the publisher's initial parameters are readable (`moq_session_publish_initial_params`) |
| Fill streams (replace Joining FETCH) | Both sides: request a fill on SUBSCRIBE / REQUEST_UPDATE, receive it as fetch events; publisher opens, serves (facade: from the retained group), resets on cancel; STOP_SENDING cancels only the fill |
| Request streams | A FIN is not a cancellation; responder FIN without PUBLISH_DONE fails the request |
| PUBLISH_STATE_NOTIFY | Received and validated; sendable on an established subscription (`moq_session_notify_subscription_state`) |
| MAX_REQUEST_UPDATES | Advertised when configured (`moq_session_cfg_t::max_request_updates`); one update is processed and answered at a time, so no inbound counter enforces it and TOO_MANY_REQUEST_UPDATES is never sent |
| Data plane | Shared with draft 18 plus FIRST_OBJECT (set by the original publisher), End of Timed-Out Range |
| Delivery timeouts | The SUBGROUP timer starts at the subgroup's FIN, uses the first object's property override, and resets the stream on expiry (closed subgroups are kept until it fires). The per-object clock is not implemented: the session hands each object to the transport as it is written, so only an application that holds an object back (WOULD_BLOCK) could age one |
| FILL_TIMEOUT | Carried and surfaced; not acted on. It bounds how long a relay waits for upstream sources (9.20.6), which an origin publisher never does |
| LOC properties | LOC-04 ids (Timestamp 0x10, Frame Marking 0x09 as bytes, ...) selected by `moq_loc_profile_for_transport` |

Swift: `.draft21` added to `MoQTransportVersion` and its C mappings (plan 8.4).

Not implemented: the per-object delivery clock, per-adapter
loopback tests for 21 beyond picoquic (the other adapters take their version list from the
shared endpoint list).

### Interop runner results (moq-contribution-interop-runner, native QUIC, driven by `tools/interop-adapter`)

216 driven scenarios were run, each connected over ALPN `moqt-21`; requirements sharing scenarios
were run together. Of the 638 catalog rows (270 testable): **57 PASS, 1 FAIL**, 212 not run, 103 not
testable, 265 not applicable. `moq-interop-audit --draft 21` reports 173/173 required executable
coverage. WebTransport was not exercised (the picoquic WebTransport backend speaks the legacy
dialect the runner rejects).

The one FAIL, `D21-5-2-MUST-130` (`d21-subgroup-completion-withheld-acknowledgments`): the
runner asks for a 200 ms SUBGROUP_DELIVERY_TIMEOUT, holds each data stream at 64 bytes of flow
control and expects a reset once the publisher has completed the subgroup. The placeholder
`media_send` stalls under that flow control before it can finish a subgroup, so the spec's
timer (which starts at the FIN, 5.2) never starts. The row passed in an earlier run only because
the timer was then armed when the stream opened, which resets live subgroups early and is not
what the draft says; that behaviour was removed. Closing the gap needs the placeholder publisher
(or an adapter mode) to complete a subgroup while the receiver withholds credit.

Rows fixed on this branch because the first full run failed them: FIRST_OBJECT on a new subgroup
(D21-2-2-MUST-020), SETUP AUTHORITY / PATH including an empty path and a present empty query
(D21-6-3-2-MUST-150, D21-9-1-1-MUST-296, D21-9-1-2-MUST-303/304), a single response to
SUBSCRIBE_NAMESPACE (D21-4-1-MUST-082), the 8,192-byte GOAWAY URI, and process exit status when
the library closed a session over a peer's protocol violation (`media_send --peer-close-ok`).

Triage of the 212 rows not run (no row is reported as passed that was not):

| Bucket | Rows | Meaning |
|--------|------|---------|
| NOT_RUN: scenario not executable | about 109 | The runner has no executable profile for the scenario (100 scenario ids) or needs a different run configuration (six namespace-period probes are refused as an invalid run config) |
| NOT_RUN: needs a publisher mode | about 103 | The scenario ran but its evaluator needs the publisher to originate something the placeholder `media_send` does not (property repeats, gap properties, padding streams, fetch data, a premature reset). The adapter has no per-scenario modes |
| FIX | 1 | D21-5-2-MUST-130 above |
| DISPUTE | 0 | None raised |

Draft 18 under the same adapter: 69 PASS and 2 FAIL (a standalone FETCH for a track with no
objects, D18-10-12-3-MUST-004, and the SUBSCRIBE_TRACKS single response, D18-6-1-MUST-003). The
earlier 63 PASS / 7 FAIL became 69 / 2 because SETUP AUTHORITY / PATH and FIRST_OBJECT were also
added to the draft-18 profile. A full Task 0 baseline was not captured, so "not worse" rests on
that comparison.

Also green: default tree 131/131, ASan/UBSan tree 131/131, the simulator OOM test under drafts 18
and 21, all six fuzz targets clean for 30 s each, `scripts/check_profile_boundary.sh`; the `dev`
tree's failing set equals `main`'s. The seeded sweeps under draft 21 fail the same runners
they fail under draft 18 (they assume the draft-16 start order).
