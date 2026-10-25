# Conformance status

Factual status of libmoq's support for draft-ietf-moq-transport-16, -18 and
-22. Each session speaks exactly one of them, chosen when it is created
(`moq_session_cfg_t.version`, 0 meaning draft-16) from what the transport
negotiated: the ALPN (`moqt-16`, `moqt-18`, `moqt-22`) over raw QUIC, the
WebTransport protocol token over WebTransport. The wire formats live behind
one profile per draft (`core/src/session/profile_d16.c`, `profile_d18.c`,
`profile_d22.c`); the session core asks profile capabilities, never a draft
number ([architecture.md](architecture.md#wire-profiles-and-capabilities)).

**Draft-22 is an integration candidate, not a qualified release.** Its
API reconciliation, local-capacity behavior, media/service integration and
adapter/platform qualification remain incomplete. The named tests below are
an implementation and test inventory, not proof that every path or platform
has passed on the current candidate. Acceptance requires an actual build and
test log for the source being reviewed; older snapshot results do not carry
forward automatically.

How to read the tables: **Codec** is the wire encoder and decoder,
**Session API** the public calls and events in `<moq/session.h>`, **Tests**
the CTest names that exercise it. Scenario runners also register `_d18` and
`_d22` variants whose runners read `MOQ_SIM_VERSION` and explicitly set the
simulator configuration; SimPair itself never reads that version override. The plain
name runs the simulator's default, draft-16, unless the runner covers every
draft itself (`scenario_overlap_delivery`, `scenario_input_backpressure`) or
pins one (`scenario_update_answer` runs draft-22, with `_d16` and `_d18`
variants). Adapter, relay and service tests are registered only in trees
that build those components (see "Test trees" at the end).
The fixed D16 backpressure fixture and fixed D16/D18 relay-bootstrap fixture
run once under their ordinary names; environment-only aliases are not evidence
of another protocol version.

The draft-22 wire format is in [draft22-wire-reference.md](draft22-wire-reference.md),
draft-18's integer encoding in [draft18-wire-reference.md](draft18-wire-reference.md).

## Versions and the default offer

| | Draft-16 | Draft-18 | Draft-22 |
| --- | --- | --- | --- |
| Profile registered (`moq_version_supported`) | yes | yes | yes, candidate |
| ALPN / WebTransport token | `moqt-16` | `moqt-18` | `moqt-22` |
| Control channel | one bidirectional stream, requests correlated by Request ID | one unidirectional stream per side, one bidirectional stream per request | as draft-18 |
| Integer encoding | QUIC varint (2^62-1) | vi64 (2^64-1) | vi64 |
| Transport binding required before start | no | no | yes (connection facts and, over raw QUIC, the connection target) |

`moq_versions_supported()` lists registered versions newest first
({22, 18, 16}). Default offers list every supported version, newest first:

- the service endpoint's AUTO offer is {22, 18, 16};
  `moq_endpoint_cfg_t.preferred_version` (set after
  `moq_endpoint_cfg_init_sized()`) moves one version to the front, and a LIST
  or EXACT version policy names the order outright;
- the `moq5-relay` native and WebTransport listeners offer `[22, 18, 16]`
  unless `listener.versions` / `webtransport.versions` say otherwise
  ([moq5-relay README](../tools/moq5-relay/README.md));
- the simple-relay example offers {22, 18, 16};
- an attach-mode host chooses the version itself and creates the session for
  the ALPN or WebTransport token it negotiated.

## Draft-16

### Control messages

| Message | Codec | Session API | Tests | Notes |
|---------|-------|-------------|-------|-------|
| CLIENT_SETUP / SERVER_SETUP | Full | Full | `scenario_lifecycle`, `scenario_auth`, `control_d16`, `vectors_d16` | Request capacity, auth token cache negotiation |
| SUBSCRIBE | Full | Full | `session_subscribe`, `scenario_lifecycle`, `scenario_object`, `scenario_backpressure` | All filter types; parameters validated per scope |
| SUBSCRIBE_OK | Full | Full | `session_subscribe`, `scenario_lifecycle` | LARGEST_OBJECT, EXPIRES, track properties |
| REQUEST_ERROR | Full | Full | `request_refusal`, `retry_interval` | Shared across request families; Retry Interval carried as time + 1 |
| REQUEST_OK | Full | Full | `scenario_track_status`, `scenario_fetch` | Per-target parameter validation |
| UNSUBSCRIBE | Full | Full | `scenario_lifecycle`, `subscriber_cancel_streams` | Publisher resets open subgroups; subscriber stops its inbound streams |
| REQUEST_UPDATE | Full | Full | `scenario_update_answer_d16`, `subscription_update_answer`, `publication_update_answer` | Subscriptions and PUBLISH-initiated subscriptions: priority, forward, delivery timeout, filter, new group request, auth tokens, answered automatically or by the application. Other targets: REQUEST_ERROR NOT_SUPPORTED. |
| PUBLISH | Full | Full | `session_publish`, `scenario_publish_initiated` | FORWARD enforcement, alias collision prevention |
| PUBLISH_OK | Full | Full | `session_publish` | Subscriber priority, group order, delivery timeout, expires |
| PUBLISH_DONE | Full | Full | `session_stream_count`, `scenario_publish_initiated` | Status codes including UPDATE_FAILED; Stream Count accounting |
| FETCH | Full | Full | `session_fetch`, `scenario_fetch`, `publish_joining_fetch` | Standalone and relative/absolute joining; Ascending and Descending |
| FETCH_OK | Full | Full | `session_fetch` | End location validation, track properties |
| FETCH_CANCEL | Full | Full | `session_fetch_cancel_late_bytes` | Tombstone for late data |
| TRACK_STATUS | Full | Full | `session_track_status`, `scenario_track_status`, `d22_track_status_properties` | LARGEST_OBJECT / EXPIRES on OK |
| GOAWAY | Full | Full | `scenario_goaway`, `request_refusal` | Drain timeout, new-session URI; requests after our GOAWAY refused with INTERNAL_ERROR (draft-16 registers no GOING_AWAY request code) |
| MAX_REQUEST_ID / REQUESTS_BLOCKED | Full | Full | `scenario_request_credit` | `grant_request_capacity`; event REQUEST_READY |
| PUBLISH_NAMESPACE / _DONE / _CANCEL | Full | Full | `session_namespace`, `scenario_namespace_sub` | Auth token staging |
| SUBSCRIBE_NAMESPACE / NAMESPACE / NAMESPACE_DONE | Full | Full | `session_namespace_sub`, `scenario_namespace_sub` | Bidirectional stream, auth, forward |

### Data plane

| Feature | Codec | Session API | Tests | Notes |
|---------|-------|-------------|-------|-------|
| Subgroup streams | Full | Full | `session_subgroup`, `scenario_object`, `scenario_streaming_object` | Incremental parser with chunked delivery |
| Object properties | Full | Full | `scenario_object` | Owned rcbuf on events |
| Object status (Normal / End of Group / End of Track) | Full | Full | `scenario_object` | |
| Streaming objects (OBJECT_CHUNK) | Full | Full | `scenario_streaming_object`, `scenario_streaming_faults` | begin / write_data / end API |
| Fetch data stream | Full | Full | `scenario_fetch` | Objects, gaps, FIN completion |
| Object datagrams | Full | Full | `session_datagram`, `scenario_object_datagram` | Budget-accounted |
| Delivery timeout | Full | Full | `scenario_streaming_object` | Per subscription and per update |
| Group order | Full | Full | `group_order`, `group_order_default` | The subscriber's GROUP_ORDER, else the track's DEFAULT_PUBLISHER_GROUP_ORDER, else Ascending. The publisher facade and media sender advertise a track's `group_order` |
| Data scheduling at the transport | n/a | Full | `scheduling`, `pq_sim_scheduling`, `group_order_e2e` | Each data stream's key reaches the adapter in MOQT 7.2 order: subscriber priority, publisher priority, group in the request's group order, lowest subgroup; equal priorities across requests take turns. Applied by adapters that implement `set_stream_priority` (picoquic, PicoWT) |
| Reliable size on resets | n/a | Full | `reset_reliable_size` | Draft-16's text asks for it as draft-22's does; see [adapters.md](adapters.md#reliable-resets) |

### Auth

| Feature | Status | Tests | Notes |
|---------|--------|-------|-------|
| AUTH_TOKEN inbound | Full | `auth_cache`, `session_auth`, `scenario_auth` | Cache with register / delete / use, staged transactions |
| AUTH_TOKEN outbound | Full | `session_auth` | Subscribe, fetch, publish, publish_namespace, track_status |
| Setup auth token | Full | `scenario_auth` | Processed and cached on both sides |

## Draft-18

Draft-18 moves every request onto its own bidirectional stream with a
unidirectional control stream per side, and replaces the QUIC varint with vi64.

| Area | Codec | Session API | Tests | Notes |
|---------|-------|-------------|-------|-------|
| SETUP and Setup Options | Full | Full | `d18_setup`, `d18_setup_options`, `d18_unsupported`, `d18_unsupported_ext`, `control_d18` | One SETUP type for both directions |
| Request streams | Full | Full | `bridge_uni_control`, `d18_fin_handoff`, `d18_ns_response_fin`, `d18_teardown`, `request_terminal` | A requester FIN before the response is a cancel; a responder FIN before it is a protocol violation |
| SUBSCRIBE / SUBSCRIBE_OK | Full | Full | `d18_subscribe`, `d18_inbound_subscribe`, `d18_subscribe_response` | One subscription per track |
| PUBLISH / PUBLISH_OK / PUBLISH_DONE | Full | Full | `d18_publish`, `d18_publish_done` | PUBLISH_OK carries the subscriber's choices |
| REQUEST_UPDATE | Full | Full | `d18_update`, `scenario_update_answer_d18`, `subscription_update_answer`, `publication_update_answer`, `fetch_request_update` | Coalesced while an answer is owed; one REQUEST_ERROR answers a failed run |
| FETCH / FETCH_OK | Full | Full | `d18_fetch`, `d18_joining`, `scenario_fetch_d18`, `descending_fetch_fill` | Standalone and joining; local requests remain Ascending-only by profile policy. The shared codec can reconstruct and write Descending for an admitted peer request. |
| TRACK_STATUS | Full | Full | `d18_track_status`, `scenario_track_status_d18` | |
| PUBLISH_NAMESPACE, SUBSCRIBE_NAMESPACE, NAMESPACE, NAMESPACE_DONE | Full | Full | `d18_publish_namespace`, `d18_subscribe_namespace`, `scenario_namespace_sub_d18` | |
| SUBSCRIBE_TRACKS / PUBLISH_BLOCKED | Full | Full | `d18_subscribe_tracks` | |
| GOAWAY, per-request GOAWAY | Full | Full | `d18_goaway`, `d18_request_goaway`, `d18_request_goaway_send`, `request_goaway_late`, `request_refusal` | Requests after our GOAWAY refused with GOING_AWAY |
| REDIRECT | Full | Full | `d18_redirect` | On SUBSCRIBE, FETCH, TRACK_STATUS, PUBLISH_NAMESPACE and SUBSCRIBE_NAMESPACE only |
| Auth tokens | Full | Full | `d18_auth`, `scenario_auth_d18` | |
| Parameters | Full | Full | `d18_params` | |
| Objects, datagrams | Full | Full | `d18_object`, `d18_datagram`, `scenario_object_d18`, `scenario_object_datagram_d18` | |
| Reserved namespaces | n/a | Full | `request_refusal` | Requests under "." or ".session" refused DOES_NOT_EXIST |

## Draft-22

### Messages

These rows identify implemented paths and relevant tests. Typed terminal
events and lossless request observations have public records and focused
deterministic fixtures. Facade/service projection is implemented;
request-local capacity and whole-profile qualification remain open. See
"Known limitations".

| Message | Codec paths | Session API qualification | Named tests | Notes |
| --- | --- | --- | --- | --- |
| SETUP | Present | In progress | `d22_profile`, `transport_binding`, `vectors_d22` | Options in the wire reference; both sides send SETUP without waiting |
| GOAWAY (control and request stream) | Present | In progress | `d22_profile`, `scenario_goaway_d22`, `request_goaway_late`, `request_refusal` | See "GOAWAY" below |
| SUBSCRIBE / SUBSCRIBE_OK | Present | In progress | `d22_filters`, `d22_concurrent_subs`, `scenario_lifecycle_d22`, `scenario_object_d22` | Location Filters; concurrent subscriptions |
| PUBLISH / PUBLISH_DONE | Present | In progress | `d22_publish_response`, `scenario_publish_initiated_d22`, `session_stream_count` | The subscriber's accept is a REQUEST_OK; its choices follow as a REQUEST_UPDATE |
| PUBLISH_STATE_NOTIFY | Present | In progress | `d22_state_notify`, `publish_state_snapshot`, `publish_state_permission` | `moq_session_notify_subscription_state()`, `moq_session_notify_publication_state()`; `MOQ_EVENT_PUBLISH_STATE` carries an immutable event-epoch `location` borrow, including explicit None. Notification does not authorize changes to subscriber-controlled Forward or Location Filter. |
| REQUEST_UPDATE / REQUEST_OK / REQUEST_ERROR | Present | In progress | see "REQUEST_UPDATE" below | |
| FETCH / FETCH_OK | Present | In progress | `d22_fetch_filter`, `d22_fetch_ok`, `fetch_request_options`, `descending_fetch_fill`, `fetch_request_update`, `scenario_fetch_d22` | Standalone only (draft-22 has no joining FETCH); range as a Location Filter; Ascending and Descending |
| TRACK_STATUS | Present | In progress | `scenario_track_status_d22`, `d22_track_status_terminal` | Abnormal outstanding-request outcome: `MOQ_EVENT_TRACK_STATUS_TERMINATED` |
| PUBLISH_NAMESPACE, SUBSCRIBE_NAMESPACE, NAMESPACE, NAMESPACE_DONE | Present | In progress | `scenario_namespace_sub_d22`, `d22_namespace_terminal`, `d22_ns_subscription_terminal` | Distinct announcement and namespace-subscription terminals, including lawful responder-FIN completion; focused coverage is not whole-family qualification |
| SUBSCRIBE_TRACKS / PUBLISH_SKIPPED | Present | In progress | `d22_tracks_ok_properties`, `d22_redirect`, `d22_namespace_contract`, `relay_namespace_d22` | PUBLISH_SKIPPED surfaces as `MOQ_EVENT_PUBLISH_BLOCKED`; GROUP_ORDER and INCLUDE_PROPERTIES in the request options. The relay binding admits it and offers matching tracks as PUBLISHes |
| Objects, datagrams, fetch streams | Present | In progress | `vectors_d22_data`, `descending_fetch_fill`, `scenario_object_d22`, `scenario_object_datagram_d22`, `scenario_streaming_object_d22` | Shared draft-18 framing with retained FETCH/fill order; End of Timed-Out Range added |

### Request streams

Each request travels on its own bidirectional stream. Under §6.4.2.2 each
direction closes independently: FIN is not cancellation, and a sender must
first send the messages its role owes (including PUBLISH_DONE for an
established subscription's publisher). The native observations are
request-specific, not a generic public request-terminal event:

- TRACK_STATUS uses `MOQ_EVENT_TRACK_STATUS_TERMINATED` (51) for RESET/STOP
  or missing/truncated required response before completion. A lawful requester
  FIN permits a delayed answer; a committed response or migration does not
  gain an extra terminal from a later stream signal.
- PUBLISH_NAMESPACE uses `MOQ_EVENT_NAMESPACE_TERMINATED` (57) with its own
  `MOQ_NAMESPACE_TERMINAL_*` cause domain, including lawful responder FIN
  completion and local observation-capacity failure. These are not aliases
  for namespace-subscription causes or legacy DONE/CANCELLED events.
- SUBSCRIBE_NAMESPACE uses `MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED`
  (59) with `MOQ_REQUEST_TERMINAL_*` causes. RESET/STOP preserve the first
  cause and exact peer code, including zero; known suffixes produce
  `MOQ_EVENT_NAMESPACE_GONE` before the requester-side terminal. Missing or
  truncated initial response uses causes 3/4. Lawful responder FIN after OK
  and all required messages emits `MOQ_REQUEST_TERMINAL_PEER_FIN_COMPLETE`
  (5), with local role REQUESTER and code zero, after all remaining GONE
  observations. Initial OK alone and requester FIN are not completion.
  Announcement causes remain a separate domain.
- SUBSCRIBE, PUBLISH, FETCH and SUBSCRIBE_TRACKS retain their family-specific
  events and appended stream-cause fields in
  [`session.h`](../core/include/moq/session.h). A terminal public handle can
  be stale while private directional cleanup remains; STOP_SENDING alone is
  not evidence that the peer's send direction has ended.

These distinctions do not promise request-local handling of every malformed
message or capacity failure. The subscriber's sized status-outcome poll and
the publisher's namespace-terminal callback preserve typed outcomes; namespace
termination gives the media sender its own fatal outcome, not a fabricated
peer REQUEST_ERROR or an endpoint close. The facade projections, their additive
C++/Swift views, and the sender-local behavior are implemented and covered by
focused tests. Whole-profile qualification is separate from those results.
A publisher whose subscription is cancelled resets its open subgroups;
the subscriber stops its open inbound streams with CANCELLED. Mandatory
teardown actions can remain owed under queue pressure, and PUBLISH_DONE is
held until the owed resets are queued.
Tests: `request_fin`, `request_terminal`, `request_lifetime`,
`request_drain_stop`, `teardown_capacity`, `subscriber_stop_codes`,
`subscriber_cancel_streams`, `d22_track_status_terminal`,
`d22_namespace_terminal`, `d22_ns_subscription_terminal` (including lawful FIN),
`subscriber_status_outcome`, `publisher_namespace_terminated`, and
`media_sender_namespace_terminated`.

### REQUEST_UPDATE

| Update of | Handling | Tests |
| --- | --- | --- |
| A subscription this side publishes (from its subscriber) | Applied and answered with REQUEST_OK automatically, or, with `moq_session_cfg_t.defer_subscription_updates` (or `moq_session_set_defer_subscription_updates()`), surfaced as `MOQ_EVENT_SUBSCRIBE_UPDATED` with `answer_owed` and nothing applied until `moq_session_accept_subscription_update()` (applies, then REQUEST_OK) or `moq_session_reject_subscription_update()` (REQUEST_ERROR with the application's code, then PUBLISH_DONE UPDATE_FAILED). Both answers are atomic under WOULD_BLOCK. A replacement Location Filter and a fill may ride the update. | `subscription_update_answer`, `scenario_update_answer`, `fill_session` |
| A PUBLISH-initiated subscription this side publishes | The same, through `MOQ_EVENT_PUBLISH_UPDATED` and `moq_session_accept_publication_update()` / `moq_session_reject_publication_update()`. | `publication_update_answer`, `fill_inheritance` |
| A PUBLISH-initiated subscription, from its publisher | The subscriber answers REQUEST_OK and surfaces the change. | `d22_update_peers` |
| A FETCH this side serves | Priority applied; answered after FETCH_OK, in arrival order. A refused update ends the fetch: REQUEST_ERROR, data stream reset, `MOQ_EVENT_FETCH_CANCELLED`. | `fetch_request_update`, `d22_update_peers` |
| PUBLISH_NAMESPACE, SUBSCRIBE_NAMESPACE, SUBSCRIBE_TRACKS (from their requester) | With `moq_session_set_defer_namespace_updates()`, surfaced as `MOQ_EVENT_NAMESPACE_UPDATE_REQUEST` / `_NS_SUB_` / `_SUBSCRIBE_TRACKS_` naming the update's own Request ID, queried with the `*_owed_update()` calls, and answered once with `moq_session_accept_*_update()` (REQUEST_OK; a new prefix takes effect for later notifications) or `moq_session_reject_*_update()` (REQUEST_ERROR plus FIN; the request ends). Token failures, PREFIX_OVERLAP and an unservable Range Filter are refused by the session itself (`*_UPDATE_RESULT` with outcome REFUSED). Without the opt-in every update is refused with REQUEST_ERROR NOT_SUPPORTED plus FIN after the initial response, consumes its Request ID and ends that request. Both answers are atomic under WOULD_BLOCK. The relay binding opts in and authorizes each update as REQUEST_UPDATE plus the original namespace action. | `d22_namespace_update`, `d22_namespace_updates`, `relay_namespace_d22` |
| The same three, toward their responder | `moq_session_update_namespace()` / `_ns_sub()` / `_subscribe_tracks()` send one update at a time (a second while one is unanswered is REQUEST_BLOCKED); `*_UPDATE_RESULT` reports the answer, and for a namespace subscription it is the barrier at which a new prefix applies to the active set. | `d22_namespace_updates`, `cpp_namespace_updates` |

This side advertises MAX_REQUEST_UPDATES = 1: a second update arriving while
one answer is owed on the same request stream closes the session with
TOO_MANY_REQUEST_UPDATES (0x1B, §9.1.7). Every update consumes a Request ID
(`d22_request_ids`). Updates the session refuses on its own (an unsupported
parameter, a rejected authorization token) end the subscription with
UPDATE_FAILED, with no event naming the refused update; the handle is then
stale.

### Fills

A subscription may ask for past objects as a *fill* (§3.4): FILL_PARAMETERS
on SUBSCRIBE or on a subscription's REQUEST_UPDATE, including the update a
PUBLISH's subscriber sends. The publisher answers on a fetch stream whose
header carries the requesting message's Request ID.

| Side | API | Notes |
| --- | --- | --- |
| Subscriber | `moq_subscribe_cfg_t` / `moq_subscription_update_cfg_t` `fill` (`<moq/fill.h>`), `moq_session_cancel_fill()`; events `MOQ_EVENT_FILL_CREATED`, `MOQ_EVENT_FILL_OBJECT`, `MOQ_EVENT_FILL_GAP`, `MOQ_EVENT_FILL_TERMINAL` | A fill parameter omitted from FILL_PARAMETERS takes the subscription's value (§9.20.15). Several fills may run on one subscription; cancelling the subscription resets every open fill; a fill ending never ends the subscription. |
| Publisher | `MOQ_EVENT_FILL_REQUEST`; `moq_session_open_fill()`, `moq_session_write_fill_object()`, `moq_session_write_fill_range()`, `moq_session_end_fill()`, `moq_session_abort_fill()` | An empty or paused fill opens no stream. A fill that cannot be served is answered on the wire, its header then a reset, never left unanswered. `moq_session_cfg_t.max_fills` sizes the pool (default 16). |
| Facades | `moq_sub_*fill*` (value-type `moq_sub_fill_t`), `moq_sub_poll_fill`; the publisher facade serves a fill from the track's retained group or fails it on the wire | `moq_session_supports_fill()` gates it; on 16 and 18 a fill is refused before anything is sent. |
| Service | Only the D22 media companion (explicit, or AUTO resolved on a draft-22 session) joins a catalog with a Next Object subscribe plus an initial relative-1 fill of the current group (§3.5). Transport capability alone does not opt in. | `media_late_joiner_fill`, `media_receiver_companion`, `media_catalog_bootstrap` |

Tests: `fill_owner`, `fill_range`, `fill_input`, `fill_session`,
`fill_inheritance`, `facade_fill`, `scenario_fill_d22`,
`scenario_overlap_delivery_d22`, and `scenario_fill` / `scenario_fill_d18`
for the refusal on drafts without fills.

### Media companion

Sender and receiver configs default to `MOQ_MEDIA_COMPANION_AUTO`: a missing
or partial companion tail, and every sized initializer, select it. AUTO
resolves once, when the facade first observes a draft-16/18/22 session and
before anything is published or subscribed, to the companion that matches the
negotiated draft: legacy MSF-01/LOC-01 (Joining FETCH bootstrap) on drafts
16/18, `MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC` on draft 22. An explicit
`MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01` or
`MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC` pins the companion; a known
companion/version mismatch is then refused (sender `MOQ_ERR_UNSUPPORTED`,
receiver-local `FATAL_SETUP_FAILED`), never upgraded. Before resolution an
AUTO sender numbers Group IDs from the draft-22 wallclock base (renumbered
from 0 on a legacy resolution), accepts a CMAF track without init data
(reported `MOQ_ERR_UNSUPPORTED` if draft 22 is then negotiated while it is
registered), and queues RAW objects with or without a metadata tail. On draft
22 an AUTO sender generates LOC-04 metadata from the typed timing fields for a
RAW object without that tail: a capture time as an unscaled Timestamp (Unix
microseconds, LOC-04 §2.3.1.1), otherwise the presentation time as media time
with Timescale 1000000 (§2.3.1.2). Because a Track Properties Timescale
applies to every object without its own, a capture time without a metadata
tail on such a track (or one whose properties do not parse as LOC-04
metadata) is `MOQ_ERR_INVAL` whenever the object may go out as draft 22,
before anything is queued or evicted. The object parser takes only a resolved
companion. Unknown values are `MOQ_ERR_INVAL`.

The D22 companion combines MSF-01 catalog semantics with transport-22
fills and LOC-04 metadata. Initial catalog discovery stages the independent
base and selected deltas until the matching fill completes through the ACK's
Largest Object; only then is CATALOG_READY published. An empty catalog with
no ACK Largest and NO_STREAM can await a later live base. A gap, reset or
unusable selected history latches receiver-local CATALOG_UNUSABLE: a later
live generation cannot rescue that bootstrap, and the shared endpoint is not
made fatal. Live non-Normal status markers are not catalog JSON.

`moq_loc_parse_metadata()` / `moq_loc_encode_metadata()` use the separate
sized `moq_loc_metadata_t`; the legacy LOC record stays frozen. Raw ticks,
timescale, marking bytes (1..4), audio level and config presence are retained.
Object-present timescale/video/audio config overrides track metadata, including
present-empty config; raw Track and Object Properties stay separate. Duplicate
known fields in one block are `MOQ_ERR_UNSUPPORTED`, a companion policy, not
a transport requirement. With no effective timescale the timestamp is Unix
microseconds; any present timescale is application media time, even 1000000.
Zero-scale/overflow metadata remains observable without a usable converted
time. Four-byte marking remains opaque, with independence unknown. Explicit
D22 RAW sending requires raw metadata, not a capture/PTS reinterpretation;
AUTO generates it from those fields only when the tail is absent. The D22
receiver requires RAW time mode; SHARED_EPOCH remains unsupported, and an
AUTO receiver in that mode has no draft-22 match.

The sized service object owns its property backing through cleanup, including
an independent track-property copy, and distinguishes LIVE from FILL plus
the initiating request ID (zero is valid). See
[`media_receiver.h`](../service/include/moq/media_receiver.h) and
[`media_sender.h`](../service/include/moq/media_sender.h). Focused tests:
`loc_metadata`, `media_metadata`, `sender_companion`, `media_late_joiner_fill`,
`media_receiver_companion`, `media_receiver_metadata_copy`,
`media_receiver_origin`, `media_catalog_bootstrap`. These native contracts do
not by themselves qualify every language binding, provider or platform.

### Location and Range Filters

All six Location Filter forms (§9.20.9) are sent and received on SUBSCRIBE,
PUBLISH, FETCH, REQUEST_UPDATE and PUBLISH_STATE_NOTIFY, through the semantic
`moq_location_filter_cfg_t` in `<moq/location.h>`. The filter in effect on a
subscription or publication is read back with
`moq_session_subscription_filter()` / `moq_session_publication_filter()`.
A FETCH range is a Location Filter: by default the session resolves a
relative start, a Next Object start or an open end against what it published
on the track, and refuses with INVALID_RANGE when the start lies past it;
with `defer_fetch_ranges` the request is surfaced unresolved
(`moq_fetch_request_options_t`, with a by-value location and sized
`range_unresolved` observation) and the application answers it with
`moq_session_accept_fetch()`. Relative starts clamp to the full uint64 group
range; relative 0 at Largest {MAX, MAX} (MAX = UINT64_MAX) starts at {MAX, 0},
unlike the empty Next Object window beyond that Largest. Current filter queries
retain semantic intent; an UPDATE's resolved processing snapshot is separate.
An explicit range is the application's to answer; the publisher facade refuses
a FETCH for an unknown track with DOES_NOT_EXIST, and one for a track with
nothing published, or starting past its Largest Object, with INVALID_RANGE.
Draft-18 §10.12.3 and draft-22 §3.2 require both. Draft-16 §9.16.3 requires
only the start-past-Largest case (§9.16.2 covers a no-object Joining Fetch);
the no-object standalone answer there is library policy (test `publisher`,
rows `fetch_range_published_d16/d18/d22`).
Tests: `location_cfg`, `d22_filters`, `d22_fetch_filter`,
`fetch_request_options`, `local_relative_filter`, `update_resolution`,
`update_resolution_contract`, `fill_range`.

**Range Filters are not supported.** libmoq advertises MAX_FILTER_RANGES = 0
in its SETUP, refuses any request carrying a SUBGROUP, OBJECTID, PRIORITY,
OBJECT_PROPERTY or TRACK_PROPERTY filter with REQUEST_ERROR INVALID_FILTER
(0x36), and never sends one (`d22_filters`). Range Filters therefore play no
part in concurrent-subscription attribution either.

### Concurrent subscriptions

Draft-22 allows several live subscriptions to one track (§3.1). On 22 only
(`moq_session_supports_concurrent_subscriptions()`):

- a publisher gives each subscription its own alias by default; an explicit
  shared alias for the same track is allowed, and a PUBLISH-initiated and a
  SUBSCRIBE-initiated subscription to the same track may share one;
- an object matching several subscriptions is sent once per matching
  subscription;
- a subscriber attributes an object on a shared alias by re-applying each
  subscription's Forward state and Location window, emitting one event per
  matching subscription, each owning its own reference to the payload;
- Stream Count counts a shared stream only for a subscription it alone
  reached; an ambiguous count completes on the terminal timer (§9.9);
- while a subscription to the track awaits its answer, objects on a held
  alias that its filter would accept are kept for it in the session's bounded
  early-data buffer and delivered after its SUBSCRIBE_OK if that names the
  same alias (§3.1.3).

Drafts 16 and 18 keep one subscription per track and role. Tests:
`d22_concurrent_subs`, `scenario_concurrent_subs_d22`,
`track_alias_direction`.

### Delivery timeouts and scheduling

OBJECT_DELIVERY_TIMEOUT starts at the object's header and
SUBGROUP_DELIVERY_TIMEOUT at the subgroup's FIN, and the first object's
property overrides the track's value for its subgroup (§5.2, §10.1, §10.2).
The publisher's queue follows §5.1.2 (subscriber priority, publisher
priority, group order, fill-delivered first within a group, datagram before
subgroup, lowest Subgroup or Object ID). An omitted GROUP_ORDER on a
subscription resolves through the track's DEFAULT_PUBLISHER_GROUP_ORDER on
every draft. Details and readings in the
[wire reference](draft22-wire-reference.md#delivery-timeouts-and-scheduling).
Tests: `delivery_timers`, `scheduling_order`, `group_order_default`.

Initial SUBSCRIBE and subscription UPDATE expose independent, presence-selected
`object_delivery_timeout_ms` and `subgroup_delivery_timeout_ms`, including
exact 0 and UINT64_MAX. Incoming typed options preserve both. Selecting either
new initial timeout is refused on drafts 16/18; their legacy API is unchanged.
Draft-22 subscription UPDATE refuses the old generic `delivery_timeout_us`
selection. Tests: `subscription_timeout_input`,
`subscription_admission_options`, `subscription_update_options`.
The FIN-started timer/commit-report path has focused core/bridge coverage in
`subgroup_commit_timer`. `msquic_commit` and `wtquic_commit` exercise adapter
contracts with fake transport inputs; they do not establish native ACK behavior.
Raw MsQuic and contract-qualified WT/MsQuic report peer ACK through FIN and
retain reset reachability while FIN is pending. Native `msquic_commit_loopback`
and `wtquic_commit_loopback` pass on the selected Darwin ARM64 MsQuic 2.5.9 and
matched WT inputs. The latter has focused static/shared LibMoQ coverage, both
using the same shared WT provider. These results do not qualify every backend,
platform, provider build or combined current product. See
[stream commitment](adapters.md#stream-commitment); reliable-prefix reset is a
separate capability.

### Transport binding and SETUP options

A draft-22 session must be told how it is carried before it starts
(`moq_session_bind_transport()`, `<moq/transport_binding.h>`): raw QUIC or
WebTransport over HTTP/3, whether the handshake is complete, and the QUIC
DATAGRAM facts. A raw-QUIC client also supplies its connection target, which
its SETUP sends as AUTHORITY and PATH; a raw-QUIC server supplies a URI
policy (`accept_uri`) that may refuse with INVALID_AUTHORITY or INVALID_PATH;
over WebTransport the target is the CONNECT's and is never put in SETUP.
WebTransport over HTTP/2 is refused. Managed facades bind for configurations
they admit; Network.framework creation is refused on every draft before
allocation or binding. Attach-mode ownership is adapter-specific: most hosts
bind themselves, while direct MsQuic binds at CONNECTED (see
[adapters.md](adapters.md#transport-binding)).
Draft-16 and draft-18 accept the same binding optionally (no facts are
required): bound natively over QUIC, a client's SETUP carries PATH and
AUTHORITY (draft-16 §9.3.1.1-2, draft-18 §10.3.1.1-2), and a server closes
INVALID_* for either over WebTransport or by its policy; on every draft a
server closes MALFORMED_PATH / MALFORMED_AUTHORITY for a value outside RFC
3986 syntax. An unbound draft-16 or draft-18 session starts and emits its
setup as before (no target); a malformed target value it receives is now
refused, as on every draft. Managed facades and the service endpoint bind on every draft.
Tests: `transport_binding`, `d22_profile`, `d18_setup_options`,
`endpoint_target_setup`, `msquic_loopback`.

| Option | Behaviour |
| --- | --- |
| MOQT_IMPLEMENTATION | Omitted by the local privacy policy; optional peer bytes are retained privately (`d22_profile`). No public identity query or adapter-runtime qualification is implied. |
| MAX_FILTER_RANGES | Sent as 0 (no Range Filters). |
| MAX_REQUEST_UPDATES | Sent as 1. |
| MAX_AUTH_TOKEN_CACHE_SIZE, AUTHORIZATION_TOKEN | As on draft-18; this side sends no token in SETUP. |

### Reliable resets

Every data-stream reset the session originates (application reset, answer to
STOP_SENDING, teardown, delivery timeouts, fetch and fill aborts) carries the
encoded length of the stream's header as `reliable_size`, so a transport that
can use RESET_STREAM_AT keeps the header and the receiver can tell which
subscription, fetch or fill ended (§11.3.2). This is done on all three
drafts. Whether the header survives depends on the adapter:

| Adapter | What is sent | Tests |
| --- | --- | --- |
| picoquic (attach, threaded) | RESET_STREAM_AT when both sides advertised reset_stream_at, else a plain reset. | `pq_sim_reliable_reset` |
| pico_wt | RESET_STREAM_AT when genuinely negotiated, else a plain reset. | `pico_wt_reliable_reset`, `pico_wt_managed_no_reset_stream_at` |
| MsQuic (attach, managed) | A plain reset sent once the header bytes are acknowledged, never RESET_STREAM_AT; a plain reset at once when the send was cancelled, the connection is closing or nothing is in flight. | `msquic_reliable_reset` |
| mvfst | Plain resets: the adapter does not advertise reliable stream reset. | `mvfst_reset_header_d16`, `mvfst_reset_header_d22` |
| wtquic, proxygen | Plain resets: neither stack exposes a reliable-size reset to the adapter. | |

Over MsQuic the wait for the acknowledgment keeps the header only for a
receiver that delivers data to its application before acknowledging it, as
MsQuic does; it is not a protocol guarantee for every peer. Over mvfst,
wtquic and proxygen a reset can discard a header the peer had not yet read,
which §11.3.2 and §1.4 allow. Bridge-level behaviour (capability, DROPPED
fallback, a header still queued under WOULD_BLOCK going out first) is pinned
by `bridge_reliable_reset` and `reset_reliable_size`.

### Redirects

On draft-22 a REQUEST_ERROR with code REDIRECT may answer every request
family, PUBLISH and SUBSCRIBE_TRACKS included; the requester surfaces
`MOQ_EVENT_REQUEST_REDIRECT` with the family's handle and the request ends.
Draft-18 limits REDIRECT to five families and refuses the code elsewhere
before sending (`d22_redirect`, `d18_redirect`).

### GOAWAY

A GOAWAY on the control stream starts the session's drain; from a client it
must carry an empty New Session URI. A GOAWAY on a request stream migrates
that request: whatever else arrives on the stream is framed and discarded,
and only a second GOAWAY on the same stream closes the session. A request
that arrives after this side's GOAWAY is refused with REQUEST_ERROR
GOING_AWAY and the session stays up. Tests: `request_goaway_late`,
`request_refusal`, `scenario_goaway_d22`, `scenario_crossed_d22`.

### Reserved namespaces

A request naming a Track Namespace whose first field is "." or ".session"
is refused with DOES_NOT_EXIST before the application sees it, and the
application may not publish under one (§2.4.3, §6.5; `request_refusal`).

## Known limitations

Each of these is a stated limit or open decision a user can run into.

**Integration boundaries**

- **Subscriber byte admission:** The subscriber facade counts retained ACK
  metadata against its byte limit together with queued and pending objects,
  chunks, FETCH and fill storage, and charges each ACK copy when it is
  admitted. A delivery is admitted against that limit before it is dequeued;
  one that can never fit ends as a bounded local failure, reported through
  `moq_sub_poll_local_failure`. These are covered by unit tests; whole-profile
  and transport runtime qualification under sustained pressure is not done.
- **Transport commitment:** Raw MsQuic and contract-qualified WT/MsQuic
  declare `MOQ_TRANSPORT_CAP_COMMIT_REPORT`, with the native loopback coverage
  above. PicoQUIC, PicoWT, mvfst and Proxygen do not report commitment; their
  advertised draft-22 paths are not classified as unsupported. Whole-backend
  and whole-profile qualification are not complete.
- **Request-local projection:** Subscriber status outcomes, publisher
  namespace terminals and sender-local fatal projection are implemented,
  including additive C++/Swift views. Whole-profile qualification is not
  complete; this is not whole-media-service acceptance.
- **Publisher progress:** Every draft uses bounded FIFO event pumping with
  fixed per-parent admission owners and one immutable retained-FETCH snapshot
  per session FETCH slot. Each snapshot has an object-count and track byte
  limit; its references survive cache replacement and are released on
  completion, cancellation or local teardown. Generic refusal/status answers
  use fixed storage bounded by the session's responder pools and retain FIFO
  wire order. Manual callers continue dispatching after
  `WOULD_BLOCK + CONSUMED`; flush never polls and must not be spun to OK before
  another event is handled. Focused fixtures include
  `publisher_retained_fetch_event_credit`, `publisher_subgroup_terminal` and
  `publisher_response_ownership`. Whole-profile and transport qualification
  remain incomplete; bounded storage is not an unconditional progress promise.
- **Namespace completion:** A lawful responder FIN on an established
  namespace subscription reports its own completion cause after the remaining
  GONE observations, natively and in the C++/Swift views. Whole-profile
  qualification is not complete.
- **Pico stream outcomes (N28):** Public STOP-code/ACK observation and post-FIN
  reset lifetime remain unresolved in the selected Pico provider. The reviewed
  reliable-prefix reset corrections do not close these terminal/commit gaps.

**Protocol surface**

- **Range Filters are not supported.** MAX_FILTER_RANGES is advertised as 0;
  a request carrying one is refused with INVALID_FILTER; none is ever sent.
- **Draft-22 is a candidate.** It leads the endpoint AUTO, relay and simple-relay
  default offers. See "Versions and the default offer". No release-readiness claim is implied by implemented
  draft-22 paths.
- **Chunk-streamed objects are not held for a pending subscription.** While a
  subscription awaits its answer, whole objects on a shared alias are held
  for it; objects delivered as chunks (`streaming_objects`) are not.
- **Duplicates are the application's to remove.** Concurrent subscriptions
  with overlapping filters on a shared alias each receive every copy the
  publisher sent (§3.1).
- **Datagram fan-out is all or nothing.** A datagram on a shared alias needs
  free event slots for every matching subscription at once; otherwise it is
  dropped like any datagram that finds the event queue full.
- **Local Descending FETCH remains refused on draft-18.** Its profile returns
  `MOQ_ERR_INVAL` for that local selection; this is not a shared-codec limit.
  Draft-22 FETCH/fills and draft-16 FETCH support Descending.
- **RENDEZVOUS_TIMEOUT is surfaced, not acted on, by the session.** It is
  sent when a SUBSCRIBE selects it and reported exactly in the admission
  options on receipt; waiting for a publisher is the relay's policy, which
  holds the SUBSCRIBE until one deadline from first receipt, clamped to a
  private 10,000 ms ceiling.
- **Track discovery does not cross relay shards.** A SUBSCRIBE_TRACKS is
  offered the tracks its own shard knows; shards mirror announcements only.
- **A PUBLISH accept with a non-default group order is refused on 22**,
  because GROUP_ORDER cannot travel on the REQUEST_UPDATE that carries the
  subscriber's choices.
- **Transport scheduling is per stream.** Datagrams are not scheduled by
  priority at the transport. With the stream-drained capability, a FETCH
  retains its held objects' publisher priority until those bytes drain;
  a different priority waits in the bridge. Draft-22 fills also retain their
  current group and parent identity. Within a group, fills precede subgroup
  data; between groups the parent's order applies, except that an
  opposite-order fill yields to subscription data. Cyclic fill preferences
  share a rank. No peer acknowledgment is required. Without the drained
  capability, per-write reprioritization remains best effort. With
  `max_actions` below 4 the bridge keys no stream: the group order is not
  applied at the transport. An adapter that sets priorities without the
  stream-drained notice loses the order whenever its transport still holds a
  stream's data past that stream's FIN. Adapters without
  `set_stream_priority` (msquic, mvfst, proxygen, wtquic) keep their own
  stream scheduling.
- **A received data stream reset before the session admitted it loses its
  header.** No in-tree stack reports the reliable size of a received reset,
  so a stream refused for want of a receive slot and then reset cannot be
  attributed.

**Transports**

- **MsQuic sends plain resets.** See "Reliable resets": the header is kept
  only for receivers that deliver before acknowledging.
- **MsQuic 2.5.9 defects.** Its RESET_STREAM_AT is not usable on either
  side: a receiver that processes the frame before its application took the
  reliable bytes reports a clean FIN, and a sender whose RESET_STREAM_AT is
  unacknowledged at connection end never completes the stream, so
  `MsQuicRegistrationClose` waits forever. Separately,
  `MsQuicRegistrationClose` can hang in `QuicWorkerUninitialize` on a lost
  worker wake inside libmsquic, seen about once in 600 runs of the
  `wtquic_stress` harness. A rare client close with QUIC error 1 before
  SETUP under 8-way parallel stress is also open, its MsQuic status not
  captured.
- **mvfst sends plain resets.** The mvfst revision the adapter builds against
  closes the connection when it receives RESET_STREAM_AT, so the adapter does
  not advertise reliable resets.
- **wtquic over Network.framework refuses at create.**
  `moq_wtquic_network_managed_create()` returns `MOQ_ERR_UNSUPPORTED` for every
  valid configuration, so its draft-22 binding path cannot run.
- **wtquic sends no datagrams**; inbound datagrams are delivered.
- **The proxygen adapter's draft-22 binding has not been built or run**, only
  syntax-checked. Its managed facade is client-only, and it has no
  reliable-size reset.

**Relay**

The relay core and binding carry all six semantic Location Filter forms,
including object-granular inclusive ends (Absolute Range, type 0x04) on
SUBSCRIBE and replacement UPDATE. The bounded range stays established when
exhausted on draft-22. Source: `relay/src/core/relay.c` and
`relay/src/bind/moqr_bind.c`; `relay_control` includes object-end and full-width
location controls. This does not establish current-provider relay acceptance.

- Cross-shard retrieval serves draft-22 fills only; a standalone FETCH
  serves the shard's own retained log.
- A draft-22 FETCH without FILL_TIMEOUT waits at most the relay's default
  fill wait (1 s) for an Object still arriving or a hole, then marks what is
  missing Timed-Out.

## Interoperability evidence

What has been exercised, and against what. Nothing here is a claim about an
implementation that was not run.

**Draft-22 and compatibility test inventory.** These fixtures use libmoq against libmoq.
Their presence, registration or historical execution does not establish
current-candidate adapter/relay acceptance:

- In process: explicitly version-aware scenario runners under `_d22`, and `bridge_conformance_d22`
  (the scenarios through the real transport bridge over paired fake
  endpoints).
- Relay tests without a real provider: `relay_mixed_draft` uses fake MsQuic
  API/managed inputs; `relay_dual_listener` checks configuration and listener
  planning. Neither is evidence of native relay transport traffic.
- Real Pico provider code in a deterministic packet simulator, not socket
  loopback: `pq_sim_reliable_reset` and `pico_wt_reliable_reset`.
- Over real transports on loopback, libmoq on both ends: mvfst
  (`mvfst_loopback_d22`, `mvfst_managed_server_loopback_d22`,
  `mvfst_reset_header_d22`); picoquic WebTransport
  (`pico_wt_managed_loopback_22`, `pico_wt_managed_nego_22_16`,
  `pico_wt_managed_nego_22_18`); MsQuic
  (`msquic_loopback`, `msquic_conformance`, `msquic_multi_alpn`,
  `msquic_reliable_reset`); WebTransport over MsQuic
  (`wtquic_msquic_managed_loopback`, `endpoint_wtquic_msquic_smoke`).
- Through the relay over real MsQuic: one listener serving 16, 18 and 22
  (`relay_multiversion`), fills ending at their deadline on one lane,
  several lanes and the dual raw/WebTransport listener
  (`relay_fill_deadline_single`, `relay_fill_deadline_lanes`,
  `relay_fill_deadline_lanes_fill_first`, `relay_fill_deadline_dual_wt`,
  `relay_fill_deadline_dual_split`). The separate
  `relay_dual_loopback_raw_to_wt` and `relay_dual_loopback_wt_to_raw` fixtures
  fix draft-18. The example's `simple_relay_loopback` driver runs the four
  publisher/subscriber pairings of drafts 16 and 18, a 22/22 pairing, then
  repeated 18/18 connections, each with exact binary payloads through the
  separately launched relay process. Source: `examples/relay/simple-relay/tests/loopback.py`.
- Codec cross-check: the draft-22 vectors carry specification-based byte
  derivations, and codec/session fixtures replay them (`vectors_d22`,
  `vectors_d22_data`). Review their wire expectations independently of the
  implementation; an encoder/decoder round trip alone is insufficient.
- During development a draft-22 lift of an independent draft-21
  implementation (openmoq/moq5 PR #35) was prepared as a second peer; no run
  against it is recorded in this repository, so no result is claimed from it.

No cross-implementation draft-22 result is claimed for this candidate.
The interop client (`tools/moq-interop-client`) accepts `--draft 22`; it has
not been run against a draft-22 peer.

**Drafts 16 and 18.** The same interop-runner checkout registers libmoq as
`moq5` with drafts 16 and 18. Its stored results contain no completed moq5
run: the three recorded run sets of the moq5 client (30 client-relay cells,
all on 2026-08-18, targeting draft-18) failed while Docker was setting up the
containers, before any MoQ exchange. The [transport integration guide](transport-integration-guide.md#10-proxygen-webtransport-honest-current-state)
records publish and receive through moqx over the proxygen WebTransport
adapter in an end-to-end matrix kept outside this repository; this document
has not re-run it. In-repo evidence for 16 and 18 is libmoq against libmoq,
over the same transports and relay paths as above.

## Test trees

The core, simulator and media tests build with the `rc-static` preset (and
`asan`, `rc-shared`, `fuzz`). Adapter tests need their transport stacks
(`MOQ_BUILD_ADAPTER_*`); the MsQuic, wtquic and relay-runtime tests need the
relay runtime and MsQuic facades; the service tests need
`MOQ_BUILD_SERVICE`. Test classes:

| Class | Where | What it proves |
| --- | --- | --- |
| Unit | `tests/unit/` | One codec or one session message family, white-box |
| Vectors | `tests/vectors/d16`, `tests/vectors/d22` | Decode and byte-identical re-encode of independent fixtures |
| Scenario | `tests/scenario/` | Full flows over the simulator, seeded faults, trace-hash replay, per draft |
| Conformance | `bridge_conformance`, `_d18`, `_d22` | Transport bridge over paired fake endpoints, not a real provider |
| Adapter | `adapters/*/tests` | Fake-provider tests, deterministic provider packet simulation and real socket loopback, distinguished per fixture |
| Relay | `relay/tests`, `tools/moq5-relay/tests` | Shard-level and end-to-end relay behaviour |
| Fuzz | `fuzz/` | Varint, KVP, draft-16 and draft-22 control, session control and data |
| Boundary | `api_boundary`, `profile_boundary` | Public-header vocabulary; no draft tests in the session core |
