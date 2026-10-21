# Draft-22 wire reference

In-repo navigation aid for the draft-22 wire format. The transport specification,
not this implementation summary, is authoritative. Draft-22 is an
integration candidate, not a qualified release; this document is not a
conformance claim. The service endpoint AUTO offer is {22, 18, 16}; the media
companion defaults to AUTO, which follows the negotiated draft (see
[conformance.md](conformance.md#media-companion)).
Every row names the
section of the local specification (`draft-ietf-moq-transport-22`, dated
1 October 2026) it follows and, where the text leaves room, the reading the
code takes. Section numbers are that draft's.

Where the code lives:

| Part | File |
| --- | --- |
| Envelope, SETUP, Setup Options, GOAWAY, Location Filter value | `core/src/wire/control_d22.c` |
| Message bodies (every request-stream message) | `core/src/wire/messages_d22.c` |
| Message Parameters: encodings, scopes, Range Filters, FILL_PARAMETERS | `core/src/wire/params_d22.c` |
| Profile: SETUP options sent, setup validation, parameter projection, fetch End of Range | `core/src/session/profile_d22.c` |
| Semantic Location Filter to wire form | `core/src/session/location_d22.c` |
| Data plane (subgroup, datagram, fetch framing) | shared operations in `core/src/session/profile_d18.c`; FETCH uses retained request order, with draft-22 range-marker handling in `profile_d22.c` |

The independent vectors under [`tests/vectors/d22/`](../tests/vectors/d22/README.md)
were written from the specification alone and carry a byte-by-byte derivation
for each message; `vectors_d22` (control plane) and `vectors_d22_data` (data
plane) replay them against the codec. `control_d22`, `d22_params` and
`d22_messages` are the codec's own unit tests.

## Integers and common structures

| Item | Section | Fact |
| --- | --- | --- |
| Variable-length integer | §8.1 | vi64, the same encoding as draft-18 ([draft-18 reference](draft18-wire-reference.md)): 1 to 9 bytes, length from the leading 1-bits, full 64-bit range, non-minimal encodings accepted on receipt, minimal encodings sent. |
| Control message envelope | §9 | Message Type (vi64), Message Length (16 bits, network byte order), body. A length that does not match the body is PROTOCOL_VIOLATION. |
| Location | §8.2 | Group (vi64), Object (vi64). |
| Key-Value-Pair | §8.3 | Delta Type, then a Length for odd types; even types carry one vi64. Used for Setup Options and Properties, not for Message Parameters. |
| Reason Phrase | §8.5 | Length then bytes, at most 1024 bytes; a longer one is refused. |
| Track Namespace | §8.7 | Field count then length-prefixed fields: 0 to 32 fields, each non-empty, at most 4,096 bytes of field values. |
| Authorization token | §8.9, §9.20.2 | Alias Type DELETE 0x0, REGISTER 0x1, USE_ALIAS 0x2, USE_VALUE 0x3 (§16.5). |

## Streams

| Stream | Section | Fact |
| --- | --- | --- |
| Control | §6.3, §6.4.1 | One unidirectional control stream per side, opened with the SETUP type 0x2F00. Only SETUP and GOAWAY travel on it; any other message there closes the session with PROTOCOL_VIOLATION. |
| Request streams | §6.4.2 | One bidirectional stream per request, opened by the request message. Responses carry no Request ID; the stream correlates them. Each direction closes independently (see [architecture.md](architecture.md#request-streams)). |
| Request IDs | §6.4.2.1 | Client IDs even from 0, server IDs odd from 1. SUBSCRIBE, PUBLISH, FETCH, SUBSCRIBE_NAMESPACE, SUBSCRIBE_TRACKS, PUBLISH_NAMESPACE, REQUEST_UPDATE and TRACK_STATUS each consume one. Wrong parity closes with INVALID_REQUEST_ID (0x4); pinned by `d22_request_ids`. |
| Unidirectional stream types | §6.4.1 | FETCH_HEADER 0x05, SUBGROUP_HEADER `0b0XX1XXXX`, SETUP 0x2F00, PADDING 0x132B3E28. An unknown type closes the session. Padding streams are discarded. |
| Datagram types | §11.2.1, §11.5.2 | OBJECT_DATAGRAM Type Flags, or PADDING 0x132B3E29 (discarded). |

## SETUP and Setup Options

SETUP (0x2F00, §9.1) is one message type for both directions. It carries no
version list (negotiation is ALPN or the WebTransport protocol field, §6.2)
and no option count: the options are §8.3 Key-Value-Pairs spanning the body.
Both sides send SETUP as soon as they can, without waiting for the peer's
(§6.3). Unknown options are ignored.

| Option | Type | Section | Sent by libmoq | On receipt |
| --- | --- | --- | --- | --- |
| PATH | 0x01 | §9.1.2 | By a client bound to raw QUIC: the URI's path-abempty, then "?" and the query when present, from the connection target. Never by a server or over WebTransport. | From a server, or over WebTransport: INVALID_PATH (0x8). Outside RFC 3986 syntax: MALFORMED_PATH (0x9). A raw-QUIC server's URI policy may refuse it with INVALID_PATH. |
| AUTHORIZATION_TOKEN | 0x03 | §9.1.4 | No. | REGISTER and USE_VALUE are processed; DELETE or USE_ALIAS in SETUP closes with PROTOCOL_VIOLATION (nothing can be registered yet); a malformed token closes with KEY_VALUE_FORMATTING_ERROR (0x6). |
| MAX_AUTH_TOKEN_CACHE_SIZE | 0x04 | §9.1.3 | When the session is configured with a cache size. | Bounds what this side may register with the peer. |
| AUTHORITY | 0x05 | §9.1.1 | With PATH, by a raw-QUIC client: the URI's authority as spelled. | From a server, or over WebTransport: INVALID_AUTHORITY (0x19). Outside RFC 3986 syntax: MALFORMED_AUTHORITY (0x1A). A raw-QUIC server's URI policy may refuse it with INVALID_AUTHORITY. |
| MAX_FILTER_RANGES | 0x06 | §9.1.6 | Always, value 0: this side accepts no Range Filters. | Recorded. libmoq sends no Range Filters, so the peer's value never limits it. |
| MOQT_IMPLEMENTATION | 0x07 | §9.1.5 | Omitted by the local privacy policy. | Optional peer bytes are retained privately; no public identity query. |
| MAX_REQUEST_UPDATES | 0x08 | §9.1.7 | Always, value 1. | Recorded. libmoq never has more than one REQUEST_UPDATE outstanding on a request stream (a second update call is refused with `MOQ_ERR_WRONG_STATE` until the first is answered), so any non-zero peer limit is respected. |

A second SETUP closes with PROTOCOL_VIOLATION. The PATH and AUTHORITY a
raw-QUIC client sends come from the transport binding
(`moq_session_bind_transport()`,
[`transport_binding.h`](../core/include/moq/transport_binding.h)); a session
whose profile requires a binding refuses to start without one. Pinned by
`transport_binding`, `d22_profile` and the `setup_*` vectors.

## Control and request-stream messages

All types from §9 Table 5. "Params" lists the Message Parameters the codec
accepts in that message (the scope table in `params_d22.c`); any other
parameter, or a repeat of one that may not repeat, closes the session with
PROTOCOL_VIOLATION (§9.20).

| Message | Type | Section | Stream | Body | Params |
| --- | --- | --- | --- | --- | --- |
| SETUP | 0x2F00 | §9.1 | Control | Setup Options | n/a |
| GOAWAY | 0x10 | §9.2 | Control, Request | New Session URI (length, bytes, at most 8,192), Timeout. No Request ID. | n/a |
| SUBSCRIBE | 0x3 | §9.6 | Request, first | Request ID, Track Namespace, Track Name, parameters | 0x02, 0x03, 0x04, 0x06, 0x10, 0x20, 0x21, 0x22, 0x23, 0x25–0x28, 0x32, 0x35 |
| SUBSCRIBE_OK | 0x4 | §9.7 | Request | Track Alias, parameters, Track Properties | 0x08, 0x09 |
| REQUEST_ERROR | 0x5 | §9.4 | Request | Error Code, Retry Interval, Reason Phrase, and a Redirect when the code is REDIRECT (0x34) | n/a |
| PUBLISH_NAMESPACE | 0x6 | §9.14 | Request, first | Request ID, Track Namespace Prefix, parameters | 0x03 |
| REQUEST_OK | 0x7 | §9.3 | Request | parameters, Track Properties | by the request answered: PUBLISH 0x08; REQUEST_UPDATE 0x08, 0x09; TRACK_STATUS 0x09; PUBLISH_NAMESPACE, SUBSCRIBE_NAMESPACE and SUBSCRIBE_TRACKS 0x08 |
| NAMESPACE | 0x8 | §9.16 | Request | Track Namespace Suffix | n/a |
| PUBLISH_DONE | 0xB | §9.9 | Request | Status Code, Stream Count, Reason Phrase | n/a |
| TRACK_STATUS | 0xD | §9.13 | Request, first | Request ID, Track Namespace, Track Name, parameters | 0x03, 0x35 |
| NAMESPACE_DONE | 0xE | §9.17 | Request | Track Namespace Suffix | n/a |
| PUBLISH_SKIPPED | 0xF | §9.19 | Request | Track Namespace Suffix, Track Name | n/a |
| FETCH | 0x16 | §9.11 | Request, first | Request ID, Track Namespace, Track Name, parameters | 0x03, 0x0A, 0x20, 0x21, 0x22, 0x25–0x28, 0x35 |
| FETCH_OK | 0x18 | §9.12 | Request | End Of Track (8), End Location, parameters, Track Properties | none |
| PUBLISH | 0x1D | §9.8 | Request, first | Request ID, Track Namespace, Track Name, Track Alias, parameters, Track Properties | 0x02, 0x03, 0x06, 0x08, 0x09, 0x10, 0x20, 0x21, 0x22 |
| PUBLISH_STATE_NOTIFY | 0x22 | §9.10 | Request | parameters | 0x09, 0x10, 0x21 |
| SUBSCRIBE_NAMESPACE | 0x50 | §9.15 | Request, first | Request ID, Track Namespace Prefix, parameters | 0x03 |
| SUBSCRIBE_TRACKS | 0x51 | §9.18 | Request, first | Request ID, Track Namespace Prefix, parameters | 0x03, 0x10, 0x22, 0x25–0x29, 0x35 |
| REQUEST_UPDATE | 0x2 | §9.5 | Request | Request ID, parameters | by the request it updates and the sender's role, below |

REQUEST_UPDATE parameter scopes (§9.5 and each parameter's section):

| Update of | Params |
| --- | --- |
| a subscription, from its subscriber | 0x02, 0x03, 0x06, 0x10, 0x20, 0x21, 0x23, 0x25–0x28, 0x32 |
| a PUBLISH-initiated subscription, from its publisher | 0x02, 0x03, 0x06, 0x10, 0x20, 0x21, 0x32 |
| a FETCH | 0x03, 0x20 |
| PUBLISH_NAMESPACE | 0x03 |
| SUBSCRIBE_NAMESPACE | 0x03, 0x34 |
| SUBSCRIBE_TRACKS | 0x03, 0x10, 0x29, 0x34 |

The library's handling of each kind of update is in
[conformance.md](conformance.md#draft-22). Message-level vectors:
`publish_*`, `publish_state_notify_*`, `request_error_*`, `request_ok_*`,
`request_update_forward_zero`, `fetch_ok_*`, `goaway_*`.

### Message-level facts

| Item | Section | Fact |
| --- | --- | --- |
| GOAWAY on the control stream | §9.2 | A server receiving a GOAWAY with a non-empty New Session URI closes with PROTOCOL_VIOLATION. A GOAWAY before the session is established, or a second one, closes with PROTOCOL_VIOLATION. |
| GOAWAY on a request stream | §9.2 | Migrates that request; bytes that follow on the stream are framed and discarded; a second GOAWAY received on the same stream closes with PROTOCOL_VIOLATION. Pinned by `request_goaway_late`. |
| Retry Interval | §9.4.2 | The minimum retry time plus one; 0 means do not retry. The API carries the time itself (`retry_after_ms`): a retryable refusal is sent as time + 1 and a received non-zero value surfaces as value - 1. Pinned by `retry_interval`. |
| Redirect | §9.4.1 | Present only when the Error Code is REDIRECT (0x34): Connect URI, Track Namespace, Track Name. On draft-22 a REDIRECT may answer every request family; pinned by `d22_redirect`. |
| Track Properties on REQUEST_OK | §9.3 | Properties on a REQUEST_OK answering PUBLISH, REQUEST_UPDATE, SUBSCRIBE_NAMESPACE or PUBLISH_NAMESPACE close with PROTOCOL_VIOLATION. SUBSCRIBE_TRACKS_OK is in neither of §9.3's lists, so its properties are read past. Pinned by `d22_tracks_ok_properties`. |
| FETCH_OK End Location | §9.12 | Inclusive on the wire. The API keeps one form across drafts (an exclusive end object, 0 meaning the whole end group): end (G, O) is sent as {G, O-1}, a whole end group as {G, 2^64-1}; a received {G, O} surfaces as (G, O+1) and {G, 2^64-1} as (G, 0). An End Location below the FETCH's start closes with PROTOCOL_VIOLATION. Pinned by `d22_fetch_ok`. |
| PUBLISH_DONE Stream Count | §9.9 | 2^64-1 means the publisher could not count its streams: the subscriber completes the terminal at its wait deadline rather than by counting. Vector `publish_done_stream_count_max`. |
| PUBLISH response | §9.8 | A subscriber accepts a PUBLISH with a REQUEST_OK; any non-default choice (priority, forward, filter, delivery timeouts) follows as a REQUEST_UPDATE on the same stream. GROUP_ORDER has no place on an update, so an accept that asks for a non-default group order is refused before anything is sent. Pinned by `d22_publish_response`. |
| PUBLISH_STATE_NOTIFY | §9.10, §9.20.9 | `MOQ_EVENT_PUBLISH_STATE` carries this notification's borrowed, immutable Location Filter snapshot, including explicit None, rather than a later effective-filter query. Notification does not authorize a publisher to change subscriber-controlled Forward or Location Filter. Pinned by `publish_state_snapshot` and `publish_state_permission`. |
| INCLUDE_PROPERTIES | §9.20.21 | Honoured for SUBSCRIBE_OK, FETCH_OK and TRACK_STATUS_OK: a request that asked for 0 gets an empty Track Properties block. SUBSCRIBE, FETCH and TRACK_STATUS send it when the caller selects it (`has_include_properties`); drafts 16 and 18 refuse that with `MOQ_ERR_UNSUPPORTED`. Pinned by `d22_include_properties` and `d22_track_status_properties`. |

## Message Parameters

A parameter is Type Delta then Value (§9.20). Types ascend; a zero delta
repeats the previous type and is legal only for AUTHORIZATION_TOKEN and the
Range Filters. Message Parameters are not §8.3 Key-Value-Pairs: each value
uses the encoding its parameter names (uint8, varint, Location,
length-prefixed, Track Namespace), not the odd/even rule. Because unknown
parameters cannot be skipped, an unknown or out-of-scope parameter closes the
session with PROTOCOL_VIOLATION.

| Type | Name | Section | Value | libmoq |
| --- | --- | --- | --- | --- |
| 0x02 | OBJECT_DELIVERY_TIMEOUT | §9.20.4 | varint (ms) | Sent and applied independently of 0x06. Initial SUBSCRIBE and subscription UPDATE have presence-selected `object_delivery_timeout_ms`, preserving 0 and 2^64-1 exactly; incoming typed options preserve the same presence/value. |
| 0x03 | AUTHORIZATION_TOKEN | §9.20.2 | length-prefixed token | Sent and processed; may repeat. |
| 0x04 | RENDEZVOUS_TIMEOUT | §9.20.6 | varint (ms) | SUBSCRIBE only. Sent when selected (`moq_subscribe_cfg_t.has_rendezvous_timeout`); reported exactly on receipt in the admission options. |
| 0x06 | SUBGROUP_DELIVERY_TIMEOUT | §9.20.3 | varint (ms) | Independently selected `subgroup_delivery_timeout_ms`, with the same exact range as 0x02. An unselected initial parameter is omitted; an unselected UPDATE counterpart remains unchanged. |
| 0x08 | EXPIRES | §9.20.16 | varint (ms) | Sent and surfaced. |
| 0x09 | LARGEST_OBJECT | §9.20.17 | Location | Sent and surfaced. |
| 0x0A | FILL_TIMEOUT | §9.20.5 | varint (ms) | On FETCH and inside FILL_PARAMETERS; surfaced to the serving application. |
| 0x10 | FORWARD | §9.20.18 | uint8, 0 or 1 | Sent and applied; another value closes with PROTOCOL_VIOLATION. |
| 0x20 | SUBSCRIBER_PRIORITY | §9.20.7 | uint8 | Sent and applied; 128 when omitted from SUBSCRIBE or FETCH. |
| 0x21 | LOCATION_FILTER | §9.20.9 | Location Filter, below | Sent and applied on SUBSCRIBE, PUBLISH, FETCH, REQUEST_UPDATE and PUBLISH_STATE_NOTIFY. |
| 0x22 | GROUP_ORDER | §9.20.8 | uint8, 1 or 2 | Sent and applied; another value closes with PROTOCOL_VIOLATION. Omitted on a subscription: the track's DEFAULT_PUBLISHER_GROUP_ORDER, else Ascending. Omitted on a FETCH: Ascending. |
| 0x23 | FILL_PARAMETERS | §9.20.15 | length-prefixed; the value is Number of Parameters (vi64) then that many parameters in their own scope, delta chain restarting at 0; no nested FILL_PARAMETERS | Sent and served (fills). Allowed inside: 0x0A, 0x20, 0x21, 0x22, 0x25–0x28. Not retained as subscription state. |
| 0x25–0x28 | SUBGROUP / OBJECTID / PRIORITY / OBJECT_PROPERTY_FILTER | §9.20.10–§9.20.13 | length-prefixed SetID and delta-coded ranges | Parsed in full; a Range Filter on a request this side receives is refused with REQUEST_ERROR INVALID_FILTER (0x36), because the advertised MAX_FILTER_RANGES is 0. Never sent. |
| 0x29 | TRACK_PROPERTY_FILTER | §9.20.14 | as above | Only on SUBSCRIBE_TRACKS and its update; refused like the other Range Filters. |
| 0x32 | NEW_GROUP_REQUEST | §9.20.19 | varint | Sent when the track declared DYNAMIC_GROUPS, refused locally otherwise; received on a subscription without DYNAMIC_GROUPS it closes with PROTOCOL_VIOLATION. |
| 0x34 | TRACK_NAMESPACE_PREFIX | §9.20.20 | Track Namespace | Validated on namespace-request updates (handling in [conformance.md](conformance.md#draft-22)). |
| 0x35 | INCLUDE_PROPERTIES | §9.20.21 | uint8, 0 or 1 | Sent and honoured as above. |

The complete timeout blocks and their presence flags are declared in
[`session.h`](../core/include/moq/session.h); tests are
`subscription_timeout_input`, `subscription_admission_options` and
`subscription_update_options`. The legacy generic `delivery_timeout_us`
selection is not a substitute on draft-22 subscription UPDATE: it is refused
with `MOQ_ERR_UNSUPPORTED`. Exact parameter transport does not qualify an
adapter's delivery/ACK behavior.

### Location Filter

LOCATION_FILTER's value is a Location Filter Type (vi64) followed by the
fields that type selects (§9.20.9, Table 6). Any other type is a
PROTOCOL_VIOLATION.

| Type | Form | Fields | Public kind (`<moq/location.h>`) |
| --- | --- | --- | --- |
| 0x00 | None | none | `MOQ_LOCATION_FILTER_NONE` |
| 0x01 | Relative Start | StartGroup | `MOQ_LOCATION_FILTER_RELATIVE_GROUP` |
| 0x02 | Absolute Start | StartGroup, StartObject | `MOQ_LOCATION_FILTER_ABSOLUTE_START` |
| 0x03 | Absolute Start, Group End | StartGroup, StartObject, EndGroupDelta | `MOQ_LOCATION_FILTER_GROUP_END` |
| 0x04 | Absolute Range | StartGroup, StartObject, EndGroupDelta, EndObject (inclusive) | `MOQ_LOCATION_FILTER_OBJECT_END` |
| 0x05 | Next Object | none | `MOQ_LOCATION_FILTER_NEXT_OBJECT` |

A relative StartGroup counts back from the next group: the start is
{Largest Object.Group + 1 - StartGroup, 0}, clamped to [0, 2^64-1], so 0 is
the next group, 1 the current group and 2 the group before it. At Group
2^64-1, relative 0 clamps to {2^64-1, 0}; it is not the empty Next Object
window beyond {2^64-1, 2^64-1}.
StartGroup + EndGroupDelta past 2^64-1 closes with PROTOCOL_VIOLATION. On a
subscription an omitted end is open; on a FETCH it is the Largest Object
(§9.20.9). The public configuration carries absolute end groups, never
deltas; `location_d22.c` converts. Pinned by `location_cfg`, `d22_filters`,
`d22_fetch_filter` and the `fetch_filter_type_04` vector.

## Data plane

The data plane shares draft-18's framing operations. FETCH encoding and
decoding use the retained effective order of the request or fill, not a
later parent-filter query. The draft-22 profile also admits the Timed-Out
End of Range below.

| Item | Section | Fact |
| --- | --- | --- |
| SUBGROUP_HEADER Type Flags | §11.3.1 | PROPERTIES 0x01, SUBGROUP_ID_MODE mask 0x06, END_OF_GROUP 0x08, bit 4 always set, DEFAULT_PRIORITY 0x20, FIRST_OBJECT 0x40. SUBGROUP_ID_MODE 0b11, bit 4 clear, or a value of 128 or more closes with PROTOCOL_VIOLATION. |
| Subgroup object fields | §11.3.1 | Object ID Delta, [Properties], Object Payload Length, [Object Status when the length is 0], payload. |
| OBJECT_DATAGRAM Type Flags | §11.2.1 | PROPERTIES 0x01, END_OF_GROUP 0x02, ZERO_OBJECT_ID 0x04, DEFAULT_PRIORITY 0x08, STATUS 0x20. Bit 4, any bit with no defined meaning, STATUS with END_OF_GROUP, PROPERTIES with a zero Properties Length, or STATUS with PROPERTIES on a non-Normal status closes with PROTOCOL_VIOLATION. |
| FETCH_HEADER | §11.4.1 | Type 0x05, Request ID. A fill's fetch stream carries the Request ID of the SUBSCRIBE, PUBLISH or REQUEST_UPDATE that asked for it. |
| Fetch object Serialization Flags | §11.4.1.1 | Subgroup ID mode bits 0–1; Object ID Delta 0x04, Group ID Delta 0x08, Priority 0x10, Properties 0x20, Datagram 0x40. The first Group ID is absolute; a later Group ID Delta reconstructs prior group + delta + 1 for Ascending or prior group - delta - 1 for Descending, rejecting overflow/underflow. Both orders are implemented for draft-22 FETCH and fills; `descending_fetch_fill` covers these paths; draft-18 shares the delta rule and requests descending FETCH too (`d18_setup_options`, `relay_standalone_fetch_d18_descending_wire`). |
| End of Range | §11.4.1.2 | Serialization Flags 0x8C (Non-Existent), 0x10C (Unknown), 0x20C (Timed-Out), followed by Group ID and Object ID only. Any other value of 128 or more closes with PROTOCOL_VIOLATION. 0x20C is new in draft-22; the fetch range writers accept a Timed-Out marker only on a draft-22 session. |
| Object Status | §11.1.1 | Normal 0x0, End of Group 0x3, End of Track 0x4; only Normal may carry a payload or properties. |

### Delivery timeouts and scheduling

| Item | Section | Fact |
| --- | --- | --- |
| OBJECT_DELIVERY_TIMEOUT | §5.2, §10.2 | The clock of an object starts when its header has been provided; the object is checked when it is handed to the transport. An expired subgroup object resets its stream with DELIVERY_TIMEOUT; an expired datagram is dropped. The first object's property overrides the Track Property for its subgroup. |
| SUBGROUP_DELIVERY_TIMEOUT | §5.2, §10.1 | The timer starts once every object of the subgroup has been published (the FIN) and resets the stream if the transport has not reported "all data committed" when it expires. The first object's property overrides the Track Property for its subgroup. For datagrams it acts as OBJECT_DELIVERY_TIMEOUT; the smaller non-zero of the two applies. |
| Scheduling | §5.1.2 | Subscriber priority, then publisher priority, then the subscription's group order between groups (a fill whose order differs from the subscription's yields to the subscription-delivered object), then within a group the fill-delivered object first, a datagram before a subgroup, then the lowest Subgroup ID or Object ID. |

Focused session fixtures: `delivery_timers`, `subgroup_commit_timer` and
`scheduling_order`. These are not ACK-through-FIN adapter qualification.

## Error-code registries

The draft's registries (§16.11). The `MOQ_REQUEST_ERROR_*` and
`MOQ_STREAM_ERROR_*` constants in `session.h` name the request and stream
codes.

| Registry | Section | Codes |
| --- | --- | --- |
| Session termination | §12.2, §16.11.1 | NO_ERROR 0x0, INTERNAL_ERROR 0x1, UNAUTHORIZED 0x2, PROTOCOL_VIOLATION 0x3, INVALID_REQUEST_ID 0x4, DUPLICATE_TRACK_ALIAS 0x5, KEY_VALUE_FORMATTING_ERROR 0x6, INVALID_PATH 0x8, MALFORMED_PATH 0x9, GOAWAY_TIMEOUT 0x10, CONTROL_MESSAGE_TIMEOUT 0x11, DATA_STREAM_TIMEOUT 0x12, AUTH_TOKEN_CACHE_OVERFLOW 0x13, DUPLICATE_AUTH_TOKEN_ALIAS 0x14, MALFORMED_AUTH_TOKEN 0x16, UNKNOWN_AUTH_TOKEN_ALIAS 0x17, EXPIRED_AUTH_TOKEN 0x18, INVALID_AUTHORITY 0x19, MALFORMED_AUTHORITY 0x1A, TOO_MANY_REQUEST_UPDATES 0x1B. |
| REQUEST_ERROR | §12.3, §16.11.2 | INTERNAL_ERROR 0x0, UNAUTHORIZED 0x1, TIMEOUT 0x2, NOT_SUPPORTED 0x3, MALFORMED_AUTH_TOKEN 0x4, EXPIRED_AUTH_TOKEN 0x5, GOING_AWAY 0x6, EXCESSIVE_LOAD 0x9, DOES_NOT_EXIST 0x10, INVALID_RANGE 0x11, MALFORMED_TRACK 0x12, UNINTERESTED 0x20, PREFIX_OVERLAP 0x30, NAMESPACE_TOO_LARGE 0x31, UNSUPPORTED_EXTENSION 0x33, REDIRECT 0x34, CONFLICTING_FILTERS 0x35, INVALID_FILTER 0x36. A received code outside this registry surfaces as INTERNAL_ERROR. Draft-18's DUPLICATE_SUBSCRIPTION (0x19) and INVALID_JOINING_REQUEST_ID (0x32) are not in this registry and are never sent on draft-22. |
| PUBLISH_DONE status | §12.4, §16.11.3 | INTERNAL_ERROR 0x0, UNAUTHORIZED 0x1, TRACK_ENDED 0x2, GOING_AWAY 0x4, TOO_FAR_BEHIND 0x5, EXPIRED 0x6, UPDATE_FAILED 0x8, EXCESSIVE_LOAD 0x9, MALFORMED_TRACK 0x12. There is no SUBSCRIPTION_ENDED status, so on draft-22 passing the end of a filter does not end a subscription. |
| Stream reset | §12.5, §16.11.4 | INTERNAL_ERROR 0x0, CANCELLED 0x1, DELIVERY_TIMEOUT 0x2, SESSION_CLOSED 0x3, GOING_AWAY 0x4, TOO_FAR_BEHIND 0x5, UNKNOWN_OBJECT_STATUS 0x6, EXPIRED_AUTH_TOKEN 0x7, EXCESSIVE_LOAD 0x9, MALFORMED_TRACK 0x12. |

## Readings taken

Where the text leaves room, the code takes these readings.

- **FILL_TIMEOUT and RENDEZVOUS_TIMEOUT encoding.** §9.20.5 and §9.20.6 give
  durations in milliseconds without naming an encoding; both are read as
  varint like the other millisecond parameters.
- **FILL_PARAMETERS carries a parameter count.** §9.20.15 makes the value
  length-prefixed and holds Parameters "encoded as if they were Parameters for
  a separate message"; a message's Parameters are `Number of Parameters
  (vi64)` then the Parameters (§9.20: "the block is bounded by a parameter
  count rather than a length"). The value is therefore `Number of Parameters,
  Parameters...` inside the draft-22 Length, with the Type Delta chain
  restarting at 0. The count's parameters must end exactly at the Length;
  anything else, including an empty value, is PROTOCOL_VIOLATION, and a fill
  with no settings of its own is encoded `23 01 00`. The text alone also
  admits a reading without the count; the counted reading is the library's
  selected draft-22 interpretation, supported by the clarification on
  moq-wg/moq-transport#1991 (https://github.com/moq-wg/moq-transport/issues/1991#issuecomment-6199709619, 2026-10-15) and by #1868, which made the
  value Parameters rather than Key-Value-Pairs. It is not published
  replacement text. The same clarification proposes dropping the outer Length
  in a later draft; draft-22 keeps it, and so does this profile
  (vectors `subscribe_fill_parameters`, `subscribe_fill_parameters_empty` and
  `subscribe_fill_parameters_alt_uncounted`).
- **Zero-length LOCATION_FILTER.** §3.4 still speaks of a LOCATION_FILTER
  inside FILL_PARAMETERS being "zero-length". §9.20.9 defines the value with a
  mandatory Location Filter Type, so a zero-length value cannot occur; "no
  filter" is type 0x00, and a fill without a filter covers the track up to
  the Largest Object.
- **End of Range has no payload length.** §11.4.1.2 lists Group ID and Object
  ID as the fields of an End of Range indicator. The alternate reading with a
  trailing Object Payload Length of 0 is refused: the indicator ends after the
  Object ID and an extra byte is read as the start of the next element
  (vectors `fetch_object_eor_timed_out` and
  `fetch_object_eor_timed_out_alt_payload_length`).
- **SUBSCRIBE_TRACKS parameters.** §3.6.2 says any parameter valid on a
  subscription is valid on SUBSCRIBE_TRACKS and mentions a Location Filter
  and FILL_PARAMETERS there; §9.18 lists the parameters explicitly and leaves
  both out. The codec follows §9.18's list.
- **Track Properties on SUBSCRIBE_TRACKS_OK.** §9.3 requires empty
  properties on the REQUEST_OK of PUBLISH, REQUEST_UPDATE, SUBSCRIBE_NAMESPACE
  and PUBLISH_NAMESPACE and names no rule for SUBSCRIBE_TRACKS_OK, so
  properties there are read and ignored.
- **Omitted GROUP_ORDER on a subscription.** The publisher's preference from
  the track applies: its DEFAULT_PUBLISHER_GROUP_ORDER Track Property (§10.5),
  Ascending when that is absent. A fill whose FILL_PARAMETERS omit the order
  takes the subscription's (§9.20.15). Pinned by `group_order_default`.
- **Equal priorities across requests.** §5.1.2 leaves the order of two
  objects of different subscriptions or FETCH responses at equal priorities to
  the implementation: they keep the order they were queued in. Control and
  other non-data actions keep their place in the queue; data queued after one
  does not overtake it.
- **"All data committed".** This means peer acknowledgment of every byte
  through FIN, not send acceptance, buffer release or FIN submission. An
  adapter reports it
  through `moq_transport_bridge_on_local_stream_committed()` when it declares
  `MOQ_TRANSPORT_CAP_COMMIT_REPORT`, and the bridge then keeps a FIN'd data
  stream mapped until that report or a RESET, so the subgroup timer's reset
  can still reach the transport. No in-tree adapter declares the capability;
  without it the bridge retires the mapping at FIN handoff, so a later
  session timeout cannot establish that the transport performed the reset.
  The adapter boundary remains open, alongside the request-specific terminal,
  capacity and publisher-admission holds in
  [conformance.md](conformance.md#known-limitations).
- **Reliable size on resets.** §11.3.2 says RESET_STREAM_AT's reliable_size
  SHOULD include the stream header. Every data-stream reset the session
  originates carries the encoded length of that stream's header as
  `reliable_size`; whether the transport keeps the header is up to the
  adapter (see [adapters.md](adapters.md#reliable-resets)). Draft-16 resets
  carry it too, since draft-16's text has the same sentence.
