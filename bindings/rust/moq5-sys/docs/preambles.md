# Header preambles

The leading comment of each header this crate binds, verbatim: what a header states for all of its declarations at once rather than for one of them (rcbuf's shard and overflow contract among them), which clang attaches to no single item.

## moq/types.h

(no leading comment)

## moq/rcbuf.h

```text
Refcounted byte buffer for zero-copy relay forwarding.

Shard / threading contract: refcount operations (incref/decref) are
NOT atomic. All references to a given rcbuf must stay within one
shard — a single executor-affinity domain (a thread, executor, or run
loop). Sharing rcbuf ownership across shards via incref/decref is NOT
supported by libmoq's engine model. To hand bytes to another shard,
make an independent copy with moq_rcbuf_clone() using an allocator
valid for the destination shard. (Legacy/manual external
synchronization may protect raw calls in single-buffer use, but it is
not the shard model and must not be used by engine/adapter/fan-out
code to share ownership across shards.)

Overflow protection: incref aborts if the refcount reaches
UINT32_MAX. This is a hard safety trap, not a recoverable error.

The allocator vtable is copied into the rcbuf at creation so the
last decref frees with the correct allocator regardless of which
session holds the final reference.
```

## moq/version.h

(no leading comment)

## moq/endpoint.h

```text
moq_endpoint_t — the service-tier connection object: URL → managed
connection + session. Owns threading, version negotiation (ALPN / WT
protocol tokens), TLS/SNI defaults, terminal-state classification, the
sticky interrupt latch, pump multiplexing for attached media services, and
the post() executor that makes the full moq_session_* surface reachable
without exposing the pump.

Design: MEDIA_SERVICE_DESIGN.md §5. This tier is severable: lower tiers
build and work without it (MOQ_BUILD_SERVICE), the core stays sans-I/O,
and all threading lives here.

Current status: lifecycle and version negotiation are live over the
managed picoquic facades (RAW_QUIC via moq_pq_threaded with ALPN
offer-list + readback, WEBTRANSPORT via moq_pico_wt_managed with WT
protocol tokens). picoquic is the DEFAULT RAW_QUIC backend. When compiled
in and selected explicitly via cfg.backend, RAW_QUIC is ALSO served by
mvfst (moq_mvfst_managed, exact-version) and by MsQuic (moq_msquic_managed,
exact-version, the shared lane pump API — the path intended for future
sharded relay work), and WEBTRANSPORT by proxygen (moq_proxygen_wt_managed,
full negotiation); AUTO keeps choosing picoquic/pico_wt (msquic/mvfst are
opt-in only, never selected ahead of picoquic). Offers go out in PREFERENCE ORDER (AUTO =
every draft this build supports, newest first: draft-22, draft-18, then
draft-16, with a named preferred version moved to the front); the negotiated version
configures the session before any SETUP byte is parsed, and an un-negotiable
connection fails terminally rather than silently downgrading.

Media receiver/sender attach APIs are live (see moq/media_receiver.h and
moq/media_sender.h). v0 supports at most one receiver attachment and one
sender attachment per endpoint; while attached, moq_endpoint_stop() is gated
by the endpoint attachment count.
```

## moq/cmaf.h

```text
CMAF (Common Media Application Format) segment parser.

Parse-only: extracts codec configuration from init segments
(ftyp+moov) and sample tables from media fragments (moof+mdat).
No muxer, no decoder, no session integration.

All string/byte fields in parsed output borrow from the input
buffer. The input must remain valid while the output is in use.
Parsing never allocates.

Link against moq::cmaf (depends only on moq::core).
```

## moq/msf.h

```text
MSF (MOQT Streaming Format) catalog parser and encoder.

Converts between MSF catalog JSON documents and typed C structs.
No network, no sessions.

Link against moq::msf (depends on moq::core; when built with
moq::media-object and moq::cmaf, also provides the
moq_msf_track_to_media_info_sized() helper via <moq/msf_media.h>).

Lifetime: string fields in the parsed catalog borrow from the
internal JSON DOM, which itself is allocated via the caller's
moq_alloc_t. All borrows are valid until moq_msf_catalog_cleanup()
is called. The original JSON input need not be kept alive after
moq_msf_catalog_parse() returns.

@see draft-ietf-moq-msf-01
```

## moq/media_object.h

```text
Stateless MoQ media object normalizer.

Parses one MoQ media object into timing, keyframe, and payload
metadata. Handles RAW (LOC properties), CMAF (fragment parsing) and, in a
build with moq::mpeg2ts, MPEG-2 TS packaging. No state, no heap allocation, no retained
refs — output spans borrow from input rcbufs.

Usable standalone by downstream integrations without requiring
moq_playback_t. Link against moq::media-object (depends on
moq::core, moq::loc, moq::cmaf).
```

## moq/media_sender.h

```text
moq_media_sender_t — the send-side media facade over an endpoint
(MEDIA_SERVICE_DESIGN.md §7). Owns a publisher on the endpoint's
network thread: it advertises a namespace, derives and publishes a
retained MSF catalog from the configured tracks, and reports readiness
once the namespace is accepted and the catalog is published. The
application adds tracks and submits media objects from its own thread.

Current status: tracks, catalog, readiness, and the media write path are
live -- objects written from the app thread are enqueued in decode order
into a bounded, per-track sync-anchored queue and drained to the session
on the network thread, generating the standard LOC properties from the
typed fields and abandoning open subgroups wire-correctly under
backpressure. The media companion defaults to AUTO, which follows the
negotiated draft (legacy LOC-01 on drafts 16/18, LOC-04 on draft 22); an
explicit companion pins one and refuses the other drafts.
```

## moq/media_receiver.h

```text
moq_media_receiver_t — the receive-side media facade over an endpoint
(MEDIA_SERVICE_DESIGN.md §6). Owns the catalog track subscription on the
endpoint's network thread, parses the MSF catalog, and surfaces track
discovery as a bounded event queue the application drains from its own
thread.

Current status: discovery, the object data path, and per-track
subscription control are live. Discovery: catalog subscribe + parse,
TRACK_ADDED per catalog track with structured codec information,
CATALOG_READY exactly once. Objects are parsed, queued, and handed to the
application with exclusive ownership via moq_media_receiver_poll_object().
With the D22 companion, initial discovery waits for the retained
catalog fill to complete: its independent base and all deltas through the
initial ACK's Largest must be present. Live catalog updates are staged in
the existing bounded object ring, not exposed as media or used to rescue
failed history. A failed/incomplete fill or staging-capacity refusal latches
CATALOG_UNUSABLE on this receiver, not its shared endpoint. An ACK without
Largest followed by NO_STREAM instead waits for the first live independent
catalog. Draft-16/18 retain the Joining FETCH bootstrap.

Subscription modes. One rule decides what can be subscribed: the receiver
has a DELIVERY PATH for a track when it is a media track whose structured
info derived from the catalog and whose packaging this build parses
(poll_object), or a CMSF SAP timeline (poll_sap) or MSF §7 media timeline
(poll_media_timeline). subscribe_track refuses any other track with
MOQ_ERR_UNSUPPORTED: a track whose info did not derive (e.g. an unknown
packaging), an MPEG2TS track in a build without moq::mpeg2ts, an
eventtimeline of another kind, and an MSF §5.2.2 namespace-override track
(the MSF namespace has no defined MoQT mapping).
 - auto_subscribe = true: every media track with a delivery path is
   subscribed after discovery (simple-player default); timeline tracks are
   not -- subscribe them explicitly.
 - auto_subscribe = false: discovery-only until the application selects
   tracks with moq_media_receiver_subscribe_track() and later
   moq_media_receiver_unsubscribe_track() -- the model FFmpeg/VLC/GStreamer
   integrations use to discover tracks, build streams/pads, then subscribe
   only the selected ones.
```
