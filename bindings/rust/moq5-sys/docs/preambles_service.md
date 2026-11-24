## moq/endpoint.h

```text
moq_endpoint_t — the service-tier connection object: URL → managed
connection + session. Owns threading, version negotiation (ALPN / WT
protocol tokens), TLS/SNI defaults, terminal-state classification, the
sticky interrupt latch, pump multiplexing for attached media services, and
the post() executor that makes the full moq_session_* surface reachable
without exposing the pump.

This tier is severable: lower tiers build and work without it
(MOQ_BUILD_SERVICE), and the core stays sans-I/O. The endpoint
orchestrates the consumer side on the managed execution context its
transport facade owns (a network thread, or a serial queue).

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

## moq/media_auth.h

(no leading comment)

## moq/media_sender.h

```text
moq_media_sender_t — the send-side media facade over an endpoint.
Owns a publisher on the endpoint's network thread: it advertises a
namespace, derives and publishes a retained MSF catalog from the
configured tracks, and reports readiness once the namespace is accepted
and the catalog is published. The
application adds tracks and submits media objects from its own thread.

Current status: tracks, catalog, readiness, and the media write path are
live -- objects written from the app thread are enqueued in decode order
into a bounded, per-track sync-anchored queue and drained to the session
on the network thread, generating the standard LOC properties from the
typed fields and abandoning open subgroups wire-correctly under
backpressure. The media companion defaults to AUTO, which follows the
negotiated draft (legacy LOC-01 on drafts 16/18, LOC-05 on draft 22); an
explicit companion pins one and refuses the other drafts.
```

## moq/media_receiver.h

```text
moq_media_receiver_t — the receive-side media facade over an endpoint.
Owns the catalog track subscription on the endpoint's network thread,
parses the MSF catalog, and surfaces track discovery as a bounded event
queue the application drains from its own thread.

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

Catalog revisions. Both MSF-01 and MSF-02 (draft-ietf-moq-msf-02)
catalogs are read; the revision is the independent catalog's "version",
and a delta follows the catalog it applies to (stats.catalog_revision).
Under MSF-01 a declared track's attributes are immutable except the
live-to-VOD conversion. Under MSF-02 (5.1.6, 5.3) a catalog may change a
declared track in place: an advisory change (bitrate, framerate,
dimensions, label, lang, targetLatency, renderGroup, altGroup, depends,
the SAP maxima, keyframeSpacing, accessibleWindowTime, mimeType,
cipherSuite, keyId, trackBaseKey, packagingMetadata keys other than
streamMapping, a template added) keeps the handle and publishes a new
description generation (MOQ_MEDIA_TRACK_UPDATED); a change that decides
how Objects are parsed or delivered (packaging, role, codec, timescale,
samplerate, channelConfig, eventType, the resolved init, the content
protection, the MPEG-2 TS record, the stream mapping, encryptionScheme,
or anything that changes the receiver's delivery decision) REPLACES the
handle: TRACK_REMOVED names the successor (replaced_by), and the
successor's TRACK_ADDED, immediately after it, names its predecessor
(replaces). The predecessor's selection moves to the successor whole, and
a command later issued on the predecessor acts on the successor. A change
MSF-02 forbids (isLive false to true, a present template changing) drops
the catalog (stats.catalog_drops). Every catalog is adopted whole or not
at all: one that cannot be (an allocation failure, a retained-state cap,
too little room in the track-event queue for its events once a catalog
is ready) changes nothing and is dropped.

What a replacement guarantees: a new handle and decoder lifecycle, and
that no Object the predecessor's subscription received is delivered on
the successor (its queue is purged, its late Objects are dropped, and the
successor subscribes only once the predecessor's subscription is
released). It does not guarantee that the successor's Objects use the new
configuration: MSF gives a catalog change no synchronization point with
the media, and a relay may serve the new subscription a Group produced
before the change. Where the packaging carries the configuration it is
checked: a LOC-05 Codec String or in-band VIDEO/AUDIO_CONFIG on an Object
governs that Object, and an MSF-02 CMAF fragment whose tfhd track_ID
differs from the init's is dropped (MOQ_MEDIA_PARSE_DROP_MEDIA). A LOC
Object without such properties, or a CMAF fragment with the init's
track_ID, reaches the new decoder unflagged.

Subscription modes. One rule decides what can be subscribed: the receiver
has a DELIVERY PATH for a track when it is a media track whose structured
info derived from the catalog and whose packaging this build parses
(poll_object), or a CMSF SAP timeline (poll_sap) or MSF §7 media timeline
(poll_media_timeline). subscribe_track refuses any other track with
MOQ_ERR_UNSUPPORTED: a track whose info did not derive (e.g. an unknown
packaging), an MPEG2TS track in a build without moq::mpeg2ts, an
eventtimeline of another kind, an MSF-01 namespace-override track (the
MSF-01 namespace has no defined MoQT mapping; an MSF-02 track's
"namespace" names its Track Namespace fields, MSF-02 5.2.2, and is
subscribed under them), and these MSF policies:
 - an encrypted track (encryptionScheme, MSF 4.4.1) of any packaging but
   "loc": the receiver interprets CMAF, MPEG-2 TS and timeline payloads,
   which it cannot decrypt. An encrypted "loc" track IS delivered, with
   its payload as received (ciphertext) and its LOC properties, which stay
   in plaintext (MSF-02 4.4.4); the key fields are on the description's
   supplement record (moq_msf_ext_supplement(desc->extensions, ...)), and
   the application decrypts;
 - an MSF-02 track whose decoder configuration the catalog places in band
   (an initDataList "track-property" or "object-property" entry) unless it
   is a "loc" track with VIDEO_CONFIG (0x0D) on role "video" or
   AUDIO_CONFIG (0x0F) on role "audio" on a draft-22 companion: the legacy
   companion carries no per-Object configuration, so a decoder would get
   media without it (desc.init_source says where it comes from);
 - an MSF-02 "loc" audio or video track without a codec.

Compression (MSF-02 12.1). A catalog or timeline Object whose origin's
Track Properties or own Object Properties carry MSF_COMPRESSION (0x78)
with a value other than 0 (uncompressed) is never processed as if it were
uncompressed. This build does not decompress yet, GZIP (1) included, so
such an Object is refused as an algorithm it does not support: a catalog
Object fails as a malformed one does (the first catalog's failure is
CATALOG_UNUSABLE), a timeline whose Track Properties say so fails locally
(MOQ_MEDIA_TRACK_LOCAL_FAILURE, MOQ_ERR_UNSUPPORTED) and one Object that
says so is a parse drop. An Object carrying the Object property where the
Track property is set is dropped (a publisher may not use both). Under an
MSF-01 catalog the property is not interpreted on timelines. See the
stats for the counters.
 - auto_subscribe = true: every media track with a delivery path is
   subscribed after discovery (simple-player default); timeline tracks are
   not -- subscribe them explicitly.
 - auto_subscribe = false: discovery-only until the application selects
   tracks with moq_media_receiver_subscribe_track() and later
   moq_media_receiver_unsubscribe_track() -- the model FFmpeg/VLC/GStreamer
   integrations use to discover tracks, build streams/pads, then subscribe
   only the selected ones.
```
