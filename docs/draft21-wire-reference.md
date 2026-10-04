# Draft-21 wire reference

In-repo reference for the draft-21 wire facts libmoq relies on, so the
implementation does not depend on scratch notes. Every layout, code and rule
here was read from the checked-in text of `draft-ietf-moq-transport-21`
(`../moq-contribution-interop-runner/docs/draft-ietf-moq-transport-21.txt`),
not copied from the draft-18 code. Section numbers are draft-21 sections.
Where the draft is silent or ambiguous this file says so (see
[Open ambiguities](#open-ambiguities)) and the draft wins over any
implementation, including the interop runner and moqxr.

Companion: [draft18-wire-reference.md](draft18-wire-reference.md) (vi64 coding
is unchanged and is not repeated here).

## What did not change from draft 18

- vi64 integer coding, the 16-bit control message `Length`, a pair of
  unidirectional control streams opened with SETUP (type `0x2F00`), one
  bidirectional stream per request, Request ID parity (client even, server odd,
  +2 per request), `PADDING` stream `0x132B3E28` and datagram `0x132B3E29`,
  `FETCH_HEADER` stream type `0x05`.
- The data-plane encodings are bit-for-bit the draft-18 ones. The draft now
  calls the first field of `SUBGROUP_HEADER` and `OBJECT_DATAGRAM` "Type
  Flags", but the bit meanings are identical (checked against draft 18
  section 11): the invalid values are the same set, restated as "a set bit
  with no specified meaning is a PROTOCOL_VIOLATION" (11.2.1, 11.3.1).
  The only data-plane addition is the `0x20C` end-of-range marker in FETCH
  objects (11.4.1).
- Session termination codes, stream reset codes (16.11.4) are unchanged except
  as listed under [Error codes](#error-codes).

## Control messages (section 9)

Every message is `Type (vi64)`, `Length (16)`, body. Control-stream messages are
SETUP and GOAWAY; all others travel on request streams. Request-stream first
messages are the seven in 6.3: `TRACK_STATUS`, `SUBSCRIBE`, `PUBLISH`, `FETCH`,
`PUBLISH_NAMESPACE`, `SUBSCRIBE_NAMESPACE`, `SUBSCRIBE_TRACKS`.

| Type | Message | Body (after Length) | Section | Change from d18 |
|---|---|---|---|---|
| 0x2F00 | SETUP | Setup Options (KVP, fills the Length) | 9.1 | options 0x06, 0x08 added |
| 0x10 | GOAWAY | New Session URI Length, URI, Timeout | 9.2 | Request ID removed |
| 0x07 | REQUEST_OK | Number of Parameters, Parameters, Track Properties | 9.3 | `PUBLISH_OK` is now a REQUEST_OK |
| 0x05 | REQUEST_ERROR | Error Code, Retry Interval, Reason Phrase, [Redirect] | 9.4 | codes changed |
| 0x02 | REQUEST_UPDATE | Request ID, Number of Parameters, Parameters | 9.5 | carries subscription params |
| 0x03 | SUBSCRIBE | Request ID, Track Namespace, Track Name Length, Track Name, Number of Parameters, Parameters | 9.6 | |
| 0x04 | SUBSCRIBE_OK | Track Alias, Number of Parameters, Parameters, Track Properties | 9.7 | |
| 0x1D | PUBLISH | Request ID, Track Namespace, Track Name, Track Alias, Number of Parameters, Parameters, Track Properties | 9.8 | carries subscription params |
| 0x1E | (reserved) | was PUBLISH_OK in <= 17 | 9.3 | message type reserved; d18 used it |
| 0x0B | PUBLISH_DONE | Status Code, Stream Count, Reason Phrase | 9.9 | counts fill streams too |
| 0x22 | PUBLISH_STATE_NOTIFY | Number of Parameters, Parameters | 9.10 | new |
| 0x16 | FETCH | Request ID, Track Namespace, Track Name Length, Track Name, Number of Parameters, Parameters | 9.11 | no fetch type, no joining fields |
| 0x18 | FETCH_OK | End Of Track (8), End Location (Location), Number of Parameters, Parameters, Track Properties | 9.12 | |
| 0x0D | TRACK_STATUS | identical to SUBSCRIBE | 9.13 | |
| 0x06 | PUBLISH_NAMESPACE | Request ID, Track Namespace, Number of Parameters, Parameters | 9.14 | |
| 0x50 | SUBSCRIBE_NAMESPACE | Request ID, Track Namespace Prefix, Number of Parameters, Parameters | 9.15 | |
| 0x08 | NAMESPACE | Track Namespace Suffix | 9.16 | |
| 0x0E | NAMESPACE_DONE | Track Namespace Suffix | 9.17 | |
| 0x51 | SUBSCRIBE_TRACKS | Request ID, Track Namespace Prefix, Number of Parameters, Parameters | 9.18 | GROUP_ORDER lives here now |
| 0x0F | PUBLISH_SKIPPED | Track Namespace Suffix, Track Name Length, Track Name | 9.19 | renamed from PUBLISH_BLOCKED, layout unchanged |

Shorthands used by the draft for REQUEST_OK: `PUBLISH_OK`, `REQUEST_UPDATE_OK`,
`TRACK_STATUS_OK`, `SUBSCRIBE_NAMESPACE_OK`, `SUBSCRIBE_TRACKS_OK`,
`PUBLISH_NAMESPACE_OK`. Track Properties in REQUEST_OK are populated only for
`TRACK_STATUS_OK`; they MUST be empty in the others, and a receiver that gets
them closes the session with PROTOCOL_VIOLATION (9.3).

Messages with no Request ID (responses, NAMESPACE, NAMESPACE_DONE,
PUBLISH_SKIPPED, PUBLISH_DONE, PUBLISH_STATE_NOTIFY, GOAWAY) rely on the request
stream for correlation. `REQUEST_ERROR.Redirect` is present only when the Error
Code is REDIRECT; its Track Name MUST be empty for namespace-scoped requests
(9.4.1).

### Behaviors worth testing (publisher role)

- GOAWAY (9.2): more than one on the control stream or on one request stream is
  PROTOCOL_VIOLATION; a client sends a zero-length URI; a server that receives a
  non-zero URI closes with PROTOCOL_VIOLATION; maximum URI length 8,192 bytes;
  a per-request GOAWAY goes on the request stream.
- REQUEST_UPDATE (9.5): exactly one REQUEST_OK or REQUEST_ERROR per update,
  unless failed updates are coalesced into one REQUEST_ERROR. A REQUEST_UPDATE
  that is not from the request's sender, or not on a request type that allows
  it, is PROTOCOL_VIOLATION. A subscriber may also update a subscription that
  was established by PUBLISH. A failed update on a subscription means the
  publisher MUST also send PUBLISH_DONE with UPDATE_FAILED; on a FETCH it resets
  the data stream; on SUBSCRIBE_NAMESPACE, SUBSCRIBE_TRACKS and
  PUBLISH_NAMESPACE it closes the bidi stream.
- MAX_REQUEST_UPDATES (9.1.7): outstanding updates per request stream are
  limited; exceeding the limit closes the session with
  TOO_MANY_REQUEST_UPDATES. REQUEST_OK or REQUEST_ERROR restores one credit.
- PUBLISH_DONE (9.9): not sent until every stream the publisher will open is
  closed; Stream Count counts fill fetch streams; if the exact count is unknown
  it is 2^64-1.
- FIN (6.4.2.2): a FIN is not a cancellation; a responder MUST send its response
  (and PUBLISH_DONE for an established subscription) before FIN; a FIN that
  arrives before all required messages is a failed request. Cancellation is
  RESET_STREAM and STOP_SENDING (6.4.2.3).
- FETCH (9.11): no objects, or Start greater than Largest Object, is
  REQUEST_ERROR INVALID_RANGE; FETCH_OK End Location smaller than the request's
  Start is PROTOCOL_VIOLATION on receipt.
- Publisher receives PUBLISH_STATE_NOTIFY (from a subscriber, or on a request
  that is not a subscription): PROTOCOL_VIOLATION (9.10).

## Setup options (9.1)

| Type | Name | Notes |
|---|---|---|
| 0x01 | PATH | client only, native QUIC only; INVALID_PATH / MALFORMED_PATH |
| 0x03 | AUTHORIZATION_TOKEN | may repeat; server receiving DELETE/USE_ALIAS in SETUP closes with PROTOCOL_VIOLATION |
| 0x04 | MAX_AUTH_TOKEN_CACHE_SIZE | default 0 (no aliases) |
| 0x05 | AUTHORITY | client only, native QUIC only; INVALID_AUTHORITY / MALFORMED_AUTHORITY |
| 0x06 | MAX_FILTER_RANGES | new. Limits the peer's total Ranges across Range Filters per subscription or fetch. Default 0: the peer MUST NOT send Range Filters. Exceeding it: REQUEST_ERROR INVALID_FILTER (9.1.6) |
| 0x07 | MOQT_IMPLEMENTATION | SHOULD be sent; UTF-8 name and version |
| 0x08 | MAX_REQUEST_UPDATES | new. Default 0 = unlimited. Exceeding it: session error TOO_MANY_REQUEST_UPDATES (9.1.7) |

Unknown Setup Options are ignored and may be duplicated; a sender MUST NOT
repeat a known option unless allowed (9.1). Setup Options are KVPs spanning the
whole message body.

## Message parameters (9.20)

Parameter block: `Number of Parameters (vi64)` then, per parameter,
`Type Delta (vi64)` (delta from the previous type, ascending order required) and
a value whose encoding the parameter defines: `uint8` is one raw byte, `varint`
a vi64, `Location` two consecutive vi64 (Group, Object), `length-prefixed` a vi64
length plus bytes. This differs from KVP: there is no even/odd rule. An unknown
parameter is a PROTOCOL_VIOLATION; a known parameter in a message that does not
allow it is a PROTOCOL_VIOLATION; repeating a type that is not allowed to repeat
is a PROTOCOL_VIOLATION (SHOULD).

Section 9.20.2 ("Allowed Parameters By Control Message") is an empty table in the
checked-in text. The matrix below is therefore built from each parameter's own
"MAY appear in" sentence (9.20.3 to 9.20.22); a codec test must pin it.

| Type | Name | Encoding | Allowed in |
|---|---|---|---|
| 0x02 | OBJECT_DELIVERY_TIMEOUT | varint | SUBSCRIBE, PUBLISH, REQUEST_UPDATE |
| 0x03 | AUTHORIZATION_TOKEN | length-prefixed | PUBLISH, SUBSCRIBE, REQUEST_UPDATE, SUBSCRIBE_NAMESPACE, SUBSCRIBE_TRACKS, PUBLISH_NAMESPACE, TRACK_STATUS, FETCH (never copied from SUBSCRIBE_TRACKS into PUBLISH) |
| 0x04 | RENDEZVOUS_TIMEOUT | varint | SUBSCRIBE |
| 0x06 | SUBGROUP_DELIVERY_TIMEOUT | varint | SUBSCRIBE, PUBLISH, REQUEST_UPDATE |
| 0x08 | EXPIRES | varint | SUBSCRIBE_OK, PUBLISH, REQUEST_OK (all shorthands) |
| 0x09 | LARGEST_OBJECT | Location | SUBSCRIBE_OK, PUBLISH, REQUEST_UPDATE_OK, TRACK_STATUS_OK, PUBLISH_STATE_NOTIFY |
| 0x0A | FILL_TIMEOUT | varint | FETCH; inside FILL_PARAMETERS |
| 0x10 | FORWARD | uint8 (0 or 1) | SUBSCRIBE, REQUEST_UPDATE, PUBLISH, SUBSCRIBE_TRACKS, PUBLISH_STATE_NOTIFY |
| 0x20 | SUBSCRIBER_PRIORITY | uint8 | SUBSCRIBE, PUBLISH, FETCH, REQUEST_UPDATE; inside FILL_PARAMETERS |
| 0x21 | LOCATION_FILTER | length-prefixed (see below) | FETCH, SUBSCRIBE, PUBLISH, REQUEST_UPDATE, PUBLISH_STATE_NOTIFY; inside FILL_PARAMETERS |
| 0x22 | GROUP_ORDER | uint8 (1 or 2) | SUBSCRIBE, PUBLISH, SUBSCRIBE_TRACKS, FETCH; inside FILL_PARAMETERS |
| 0x23 | FILL_PARAMETERS | length-prefixed (nested parameters) | SUBSCRIBE, REQUEST_UPDATE (for a subscription) |
| 0x25 | SUBGROUP_FILTER | Range Filter | FETCH, SUBSCRIBE, SUBSCRIBE_TRACKS, REQUEST_UPDATE; inside FILL_PARAMETERS |
| 0x26 | OBJECTID_FILTER | Range Filter | same as 0x25 |
| 0x27 | PRIORITY_FILTER | Range Filter | same as 0x25; values over 255 are INVALID_FILTER |
| 0x28 | OBJECT_PROPERTY_FILTER | Range Filter + Property Type | same as 0x25; Property Type MUST be even |
| 0x29 | TRACK_PROPERTY_FILTER | Range Filter + Property Type | SUBSCRIBE_TRACKS and its REQUEST_UPDATE; not inside FILL_PARAMETERS |
| 0x32 | NEW_GROUP_REQUEST | varint | SUBSCRIBE, REQUEST_UPDATE (REQUEST_UPDATE only if DYNAMIC_GROUPS=1) |
| 0x34 | TRACK_NAMESPACE_PREFIX | Track Namespace | REQUEST_UPDATE for SUBSCRIBE_NAMESPACE / SUBSCRIBE_TRACKS |
| 0x35 | INCLUDE_PROPERTIES | uint8 (0 or 1, default 1) | SUBSCRIBE, TRACK_STATUS, FETCH, SUBSCRIBE_TRACKS |

Two parameters that were message fields or other parameters in draft 18:

- `LOCATION_FILTER` (0x21) replaces d18's `SUBSCRIPTION_FILTER` and now also
  carries a FETCH's range. Value is `Length (vi64)` then optional vi64 fields in
  order `StartGroup, StartObject, EndGroupDelta, EndObject`; the byte length says
  how many are present. Length 0 means no filter (in REQUEST_UPDATE it removes
  the filter). Only StartGroup present: a relative start, `{Largest.Group + 1 -
  StartGroup, 0}` (0 = Next Group, 1 = current group). StartGroup and
  StartObject both 0: Next Object. Otherwise absolute; `EndGroupDelta` is delta
  from StartGroup; omitting the end makes a subscription open-ended and a FETCH
  end at Largest Object; omitting EndObject includes the whole End Group.
  `StartGroup + EndGroupDelta > 2^64-1` is PROTOCOL_VIOLATION (9.20.10).
- `GROUP_ORDER` (0x22) moved from PUBLISH_OK to SUBSCRIBE_TRACKS (and stays on
  SUBSCRIBE, PUBLISH, FETCH).

Range Filters (3.3.2, 8.6, 9.20.11 to 9.20.15) share
`Type, Length (vi64), [SetID (8)], [Property Type (vi64)], [Range ...]` where the
Property Type appears only in 0x28 and 0x29, each Range is `Start (vi64)` plus an
optional `End (vi64)` on the last Range only, Start is delta-coded from the
previous End (0 for the first), End is delta-coded from its own Start, and
Length 0 means no filter. Same SetID is AND, different SetIDs are OR; ranges
inside one parameter are OR. A repeat of the same Parameter Type + SetID +
Property Type is REQUEST_ERROR INVALID_FILTER. Overflow past 2^64-1 is
INVALID_FILTER.

`FILL_PARAMETERS` (0x23, 9.20.16): value is a sequence of parameters limited to
FILL_TIMEOUT 0x0A, SUBSCRIBER_PRIORITY 0x20, LOCATION_FILTER 0x21, GROUP_ORDER
0x22, and the Range Filters 0x25 to 0x28. Anything else inside it is
PROTOCOL_VIOLATION. It is its own parameter scope (a type may appear both in the
message and inside it), is not retained as subscription state, and its presence
is what requests a fill. See [Open ambiguities](#open-ambiguities) for its
internal framing.

## Fill fetch streams (3.4, 3.5, 9.5.1)

A SUBSCRIBE or REQUEST_UPDATE carrying FILL_PARAMETERS, processed while Forward
State is 1, makes the publisher open a unidirectional stream that starts with
`FETCH_HEADER` carrying the Request ID of that SUBSCRIBE or REQUEST_UPDATE.

- Fill range = the LOCATION_FILTER inside FILL_PARAMETERS, else the
  subscription's Location filter; evaluated by Fetch rules, so it never goes
  past Largest Object. Empty filter means the whole track up to Largest Object.
  An empty range, or one that starts after Largest Object, opens no stream.
- Forward State 0 opens no fill, and moving to Forward State 1 later without
  resending FILL_PARAMETERS does not either. A REQUEST_UPDATE without
  FILL_PARAMETERS opens none.
- The stream inherits the subscription's parameters; parameters inside
  FILL_PARAMETERS override them. Several fill streams can be open at once, one
  per Request ID; a new one does not cancel an older one.
- Complete: FIN after all objects in the range. Failure: reset the stream (open
  one and reset it right after the header if needed); there is no REQUEST_ERROR
  for a fill. A subscriber may STOP_SENDING it independently. Neither affects
  the subscription.
- Cancelling the subscription: the publisher MUST reset every open fill stream.
- FILL_TIMEOUT 0 means only immediately available objects; unavailable ones are
  reported as End of Timed-Out Range.

## Properties (section 10)

| Type | Name | Scope | Section |
|---|---|---|---|
| 0x02 | OBJECT_DELIVERY_TIMEOUT | Track and Object | 10.2 |
| 0x04 | MAX_CACHE_DURATION | Track | 10.3 |
| 0x06 | SUBGROUP_DELIVERY_TIMEOUT | Track and Object | 10.1 |
| 0x0B | IMMUTABLE_PROPERTIES | Track or Object | 10.7 |
| 0x0E | DEFAULT_PUBLISHER_PRIORITY | Track | 10.4 |
| 0x22 | DEFAULT_PUBLISHER_GROUP_ORDER | Track | 10.5 |
| 0x30 | DYNAMIC_GROUPS | Track | 10.6 |
| 0x3C | PRIOR_GROUP_ID_GAP | Object | 10.8 |
| 0x3E | PRIOR_OBJECT_ID_GAP | Object | 10.9 |

As an Object Property on the first object of a subgroup, a delivery timeout
overrides the Track value for that subgroup and is ignored elsewhere. Mandatory
Track Properties are 0x4000 to 0x7FFF (Track scope only; one arriving as an
Object Property makes the track malformed, 3.6). LOC properties (provisional
registry, 16.8) differ from d18: TIMESTAMP 0x06 becomes 0x10, VIDEO_FRAME_MARKING
0x0A becomes 0x09, VIDEO_CONFIG is Track scope at 0x0D, AUDIO_CONFIG 0x0F is new,
TIMESCALE 0x08 (Track), AUDIO_LEVEL 0x0C.

## Data plane (section 11)

Unchanged encodings (see the top of this file). Datagram Type Flags: PROPERTIES
0x01, END_OF_GROUP 0x02, ZERO_OBJECT_ID 0x04, DEFAULT_PRIORITY 0x08, bit 4 (0x10)
reserved zero, STATUS 0x20; STATUS with END_OF_GROUP is invalid; unspecified
bits are PROTOCOL_VIOLATION. Subgroup header Type Flags: PROPERTIES 0x01,
SUBGROUP_ID_MODE 0x06 (0b11 reserved), END_OF_GROUP 0x08, bit 4 (0x10) always
set, DEFAULT_PRIORITY 0x20, FIRST_OBJECT 0x40; values of 128 or more are
invalid. Fetch Serialization Flags: low two bits subgroup mode, 0x04 object-id
delta present, 0x08 group-id delta present, 0x10 priority present, 0x20
properties present, 0x40 datagram. End-of-range markers: `0x8C` non-existent,
`0x10C` unknown, new `0x20C` timed-out; each is followed by Group ID and Object
ID (11.4.1.2), and any other flag value of 128 or more is PROTOCOL_VIOLATION.

Behavioral changes in this section: OBJECT_DELIVERY_TIMEOUT starts counting at
the last header byte, not the first payload byte (A.2 #1844); fill-delivered and
subscription-delivered objects have defined scheduling (3.4, 5.1); an Object
Status payload rule is extensible through an IANA registry, and only Normal
(0x0) carries a payload today (11.1.2); datagrams win cross-forwarding-preference
scheduling ties (A.3 #1780).

## Subscriptions (section 3.1)

An endpoint MAY hold several concurrent subscriptions to one Track, each with its
own Request ID. A publisher may give them the same or different Track Aliases.
An Object that matches several subscriptions' filters MUST be sent once per
matching subscription, even when they share an alias. This replaces d18's
`DUPLICATE_SUBSCRIPTION` rejection. A publisher does not end a subscription
just because Largest Object passed the filter's end (3.3.1; the d18
`SUBSCRIPTION_ENDED` publish-done status no longer exists). A Track Alias must
not name two different tracks at once (DUPLICATE_TRACK_ALIAS).

## Error codes

Session termination (12.2): same set as draft 18 except `VERSION_NEGOTIATION_FAILED`
(0x15) is removed and `TOO_MANY_REQUEST_UPDATES` (0x1B) is added.

REQUEST_ERROR (16.11.2): INTERNAL_ERROR 0x0, UNAUTHORIZED 0x1, TIMEOUT 0x2,
NOT_SUPPORTED 0x3, MALFORMED_AUTH_TOKEN 0x4, EXPIRED_AUTH_TOKEN 0x5, GOING_AWAY
0x6, EXCESSIVE_LOAD 0x9, DOES_NOT_EXIST 0x10, INVALID_RANGE 0x11, MALFORMED_TRACK
0x12, UNINTERESTED 0x20, PREFIX_OVERLAP 0x30, NAMESPACE_TOO_LARGE 0x31,
UNSUPPORTED_EXTENSION 0x33, REDIRECT 0x34, CONFLICTING_FILTERS 0x35 (new),
INVALID_FILTER 0x36 (new). Removed since d18: DUPLICATE_SUBSCRIPTION 0x19,
INVALID_JOINING_REQUEST_ID 0x32. Unknown codes, GREASE included, are treated as
INTERNAL_ERROR and never close the session (13).

PUBLISH_DONE (16.11.3): INTERNAL_ERROR 0x0, UNAUTHORIZED 0x1, TRACK_ENDED 0x2,
GOING_AWAY 0x4, TOO_FAR_BEHIND 0x5, EXPIRED 0x6, UPDATE_FAILED 0x8,
EXCESSIVE_LOAD 0x9, MALFORMED_TRACK 0x12. Removed since d18: SUBSCRIPTION_ENDED 0x3.

Stream reset codes (16.11.4): unchanged.

## Resolved design questions (plan task 1.3)

| Question | Answer from the draft |
|---|---|
| a) A publisher that does not implement fill | The draft gives no opt-out: a FILL_PARAMETERS subscription "causes the publisher to open a fill fetch stream" (3.4). The only available refusal is REQUEST_ERROR NOT_SUPPORTED (12.3), which is a poor fit for a mandatory feature. Plan: implement fill (Task 7 is required, not optional). |
| b) MAX_FILTER_RANGES | Omit the option (default 0): the peer MUST NOT send Range Filters, and any that arrive exceed the limit, so reply REQUEST_ERROR INVALID_FILTER (9.1.6, 3.3.2). Declining Range Filters is fully conformant; implementing them is not required for the publisher role. |
| c) PUBLISH_STATE_NOTIFY | Optional for a publisher; unilateral, publisher to subscriber only, on a subscription stream, never answered, not counted against MAX_REQUEST_UPDATES. When sent it MUST include LARGEST_OBJECT if known, and MUST NOT change a subscriber-controlled parameter the subscriber did not request. Allowed parameters: LOCATION_FILTER, FORWARD, LARGEST_OBJECT. Receiving one from a subscriber or on a non-subscription is PROTOCOL_VIOLATION (9.10). |
| d) Known Track Properties | The publisher emits properties from the table above; relay processing rules (8.4, 10.x) are not exercised in the publisher role. |

## Open ambiguities

- **A1: framing inside FILL_PARAMETERS.** 9.20.16 says the value is "a sequence
  of Parameters ... encoded as if they were Parameters for a separate message
  (see Section 16.7)". Section 16.7 is only the IANA table of parameter types;
  neither section says whether a `Number of Parameters` count precedes the
  nested sequence. The literal reading of "length-prefixed" (9.20) is that the
  outer length alone bounds the block, so there is no count; the interop runner
  and moqxr both read it that way. Treat that as the working reading, pin it in
  one test, and raise it with the draft authors. Revisit if the draft changes.
- **A2: End-of-range field encoding.** 11.4.1.2 says Group ID and Object ID
  "are present" after an end-of-range marker; it does not say whether they are
  absolute or the deltas used for ordinary fetch objects. Draft 18 has the same
  sentence. Follow whatever the existing d18 codec does only after confirming
  it against the draft's delta rules in 11.4.1.1; settle by a test.
- **A3: empty 9.20.2 table.** The per-message parameter table is empty in the
  checked-in draft text; see the matrix above.
- **A4: EXPIRES in TRACK_STATUS_OK.** 9.13 says a TRACK_STATUS_OK carries "the same
  parameters and Track Properties it would have set in a SUBSCRIBE_OK", and a
  SUBSCRIBE_OK may carry EXPIRES and LARGEST_OBJECT; 9.20.17 lists the messages
  EXPIRES may appear in and TRACK_STATUS_OK is not among them (9.20.18 does list
  it for LARGEST_OBJECT). The codec accepts both parameters on TRACK_STATUS_OK,
  because rejecting one closes the session on a peer that followed 9.13.
- **A5: parameters on TRACK_STATUS.** 9.13 says TRACK_STATUS is "identical to
  SUBSCRIBE" without subscriber delivery parameters, but names no complete list.
  The codec allows exactly the parameters whose own text names TRACK_STATUS:
  AUTHORIZATION_TOKEN (9.20.3) and INCLUDE_PROPERTIES (9.20.22).
- **A6: duplicate known Setup Options.** 9.1 forbids a sender from repeating an
  option type and requires receivers to allow duplicates of unknown options; it
  does not say what a receiver does with a duplicate known option. The codec
  closes with PROTOCOL_VIOLATION, as it does for message parameters (9.20).

## Worked byte examples

Hand-derived from the draft text. They become vectors in
`tests/vectors/d21/`; Task 4 writes each as a failing decode and re-encode test
before implementing.

| Message | Bytes (hex) | Derivation |
|---|---|---|
| GOAWAY, empty URI, Timeout 1000 | `10 00 03 00 83 E8` | type 0x10; body = URI length 0 (`00`), Timeout 1000 as a two-byte vi64 (`83 E8`); Length 3 |
| PUBLISH_STATE_NOTIFY, FORWARD = 0 | `22 00 03 01 10 00` | type 0x22; body = count 1, type delta 0x10, uint8 value 0; Length 3 |
| REQUEST_UPDATE, request 4, Next Object filter | `02 00 06 04 01 21 02 00 00` | body = Request ID 4, count 1, LOCATION_FILTER 0x21, Length 2, StartGroup 0, StartObject 0 |
| LOCATION_FILTER, current group only | `21 01 01` | as first parameter; Length 1, StartGroup 1 |
| LOCATION_FILTER, remove | `21 00` | Length 0 |
| OBJECTID_FILTER, SetID 0, ranges 3-5 and 10-15 | `26 05 00 03 02 05 05` | Length 5 = SetID + Start 3, End delta 2, Start delta 5, End delta 5 (8.6) |
| PUBLISH_SKIPPED, suffix `a`, track `t` | `0F 00 05 01 01 61 01 74` | body = namespace (1 field, length 1, `a`), name length 1, `t` |
| REQUEST_OK (PUBLISH_OK), no parameters | `07 00 01 00` | body = count 0; no Track Properties |
| End of Timed-Out Range flag value | `82 0C` | 0x20C = 524 as a two-byte vi64 |

## Impact on existing libmoq code (found while reading)

- `core/src/facade/publisher.c` sends PUBLISH_DONE `SUBSCRIPTION_ENDED` (0x3)
  and rejects with `DUPLICATE_SUBSCRIPTION`; both are draft-18 concepts that the
  d21 profile must not emit (see Subscriptions, above).
- The joining-FETCH state machine (`PENDING_JOIN`, `INVALID_JOINING_REQUEST_ID`)
  lives in `core/src/session/session_fetch.c`, `session_subscribe.c` and
  `session_publish.c`, not only in the d18 profile. Draft 21 has no joining
  FETCH; fill streams replace it, so this needs a profile capability rather than
  a version test.
- `moq_publish_ok_encode_args` carries subscription parameters, which draft 21
  moves to REQUEST_UPDATE.
