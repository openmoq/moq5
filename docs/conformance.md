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
- **d18 profile**: Not implemented. Only draft-16 wire format is supported.
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
