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
| SETUP AUTHORITY / PATH | Sent by a native-QUIC client (picoquic endpoint path); not yet by the other raw-QUIC adapters |
| Subscriptions | Concurrent subscriptions per Track, exact Location Filter (relative start, end Object), Range Filters declined with INVALID_FILTER |
| PUBLISH accept | Priority / forward / filter / new-group request sent as a follow-up REQUEST_UPDATE |
| Fill streams (replace Joining FETCH) | Publisher side: opened on SUBSCRIBE / REQUEST_UPDATE with Forward 1, reset on cancel, served from the facade's retained group |
| Request streams | A FIN is not a cancellation; responder FIN without PUBLISH_DONE fails the request |
| Data plane | Shared with draft 18 plus FIRST_OBJECT (set by the original publisher), End of Timed-Out Range |
| LOC properties | LOC-04 ids (Timestamp 0x10, Frame Marking 0x09 as bytes, ...) selected by `moq_loc_profile_for_transport` |

Not implemented: delivery-timeout timer semantics (subgroup timer at FIN, per-object clock,
first-object property override), FILL_TIMEOUT expiry, the receiving side of a fill stream,
PUBLISH_STATE_NOTIFY sending, Swift enums, per-adapter loopback tests for 21 beyond picoquic.

### Interop runner results (moq-contribution-interop-runner, native QUIC, driven by `tools/interop-adapter`)

216 driven scenarios were run, each connected over ALPN `moqt-21`; requirements sharing scenarios
were run together. Of the 638 catalog rows (270 testable): **58 PASS (52 MUST, 5 MUST NOT, 1
SHOULD), 0 FAIL**, 212 not run, 103 not testable, 265 not applicable. WebTransport was not
exercised (the picoquic WebTransport backend speaks the legacy dialect the runner rejects).

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
| FIX | 0 | No remaining publisher-applicable testable FAIL |
| DISPUTE | 0 | None raised |

Draft 18 under the same adapter still has 7 FAIL rows, all existing gaps in the draft-18 profile
that this work did not touch: SETUP AUTHORITY / PATH and FIRST_OBJECT are not sent under draft 18
(D18-2-2-MUST-001, D18-3-2-MUST-001, D18-10-3-1-1-MUST-004, D18-10-3-1-2-MUST-004/005), a
standalone FETCH for a track with no objects (D18-10-12-3-MUST-004) and SUBSCRIBE_TRACKS
single response (D18-6-1-MUST-003). A full draft-18 baseline was not captured before this work,
so "not worse" is argued from the code (the d18 profile is unchanged) rather than measured.

Also green: default tree 131/131, ASan/UBSan tree 131/131, all six fuzz targets clean for 30 s
each, `scripts/check_profile_boundary.sh`; the `dev` tree's failing set equals `main`'s.
