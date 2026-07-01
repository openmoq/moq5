# Service-tier examples — start here

These are the consumer-facing examples: the high-level libmoq surface a media
application (FFmpeg, VLC, OBS, a GStreamer plugin, your own player/encoder)
should use. They use **only public headers** and link **`moq::service`** — no
`moq_session_t`, no subscriber/publisher facade, no adapter or transport types.

- `media_receive.c` — connect an endpoint, attach a receiver, observe catalog
  discovery (`TRACK_ADDED` / `CATALOG_READY`), and poll media objects. Uses
  `auto_subscribe = true` (see *Subscription modes* below).
- `media_send.c` — connect an endpoint, attach a sender, add a track, and write
  media objects (placeholder payloads; see the note below).

## The model

```
moq_endpoint_t            URL -> managed connection + MoQ session.
  ├─ moq_media_receiver_t  catalog-driven receive: track discovery + objects.
  └─ moq_media_sender_t    advertise a namespace, publish a catalog, send media.
```

You give the endpoint a URL and the receiver/sender a namespace. The endpoint
owns everything below the API: the network thread, ALPN/WT-protocol version
negotiation, TLS and certificate verification, and the catalog
subscription/publication. Your code never touches the session.

### Subscription modes

The receiver has two ways to decide which catalog tracks to receive:

- **`auto_subscribe = true`** — every catalog track is subscribed automatically
  after discovery. This is the simple-player default and what `media_receive.c`
  uses.
- **`auto_subscribe = false`** — discovery only. The receiver surfaces
  `TRACK_ADDED` / `CATALOG_READY` but subscribes nothing until you call
  `moq_media_receiver_subscribe_track(rx, track, cfg)`. This is the integration
  model for **FFmpeg / VLC / GStreamer**: poll `TRACK_ADDED`, build the streams
  or pads, then subscribe only the tracks the application actually needs.

Both `subscribe_track` and `unsubscribe_track` are asynchronous: `MOQ_OK` means
the desired state was recorded, not that the peer accepted (acceptance shows up
as objects via `poll_object`; a peer rejection surfaces `MOQ_MEDIA_TRACK_ENDED`
for that track without failing the receiver). `unsubscribe_track` pauses delivery
and purges that track's already-queued objects, so enabling/disabling a track is
a cheap toggle you can flip as often as the UI demands. The `cfg` chooses the
start point — `MOQ_MEDIA_START_CURRENT` (live edge, the default) or
`MOQ_MEDIA_START_NEXT_GROUP` (a clean group boundary) — and an optional
subscriber priority; pass `NULL` for the defaults.

Note that enable/disable is a delivery **pause/resume** on the existing
subscription, not a seek or reconfiguration: the start mode and priority apply
only when the subscription is first issued. Re-enabling a track resumes it where
it is; it does not reapply `cfg` or rejoin at a new requested point. Changing the
start point or priority would require the subscription to end first.

### Lifecycle

1. `moq_endpoint_cfg_init` + `moq_endpoint_connect` — returns immediately;
   the connection and handshake complete asynchronously.
2. `moq_media_receiver_attach` / `moq_media_sender_attach` with
   `cfg->endpoint = NULL` (attach mode: you own the endpoint). The sugar
   `*_create` path instead builds and owns a private endpoint from
   `cfg->endpoint`.
3. Drive from your own thread: receiver `poll_track` / `poll_object` / `wait`;
   sender `add_track` (before readiness) then `write`.
4. Teardown, in order: destroy the child (`*_destroy`), then call
   `moq_endpoint_drain(ep, timeout_us)` if you have reliable stream bytes that
   should be flushed before shutdown, then `moq_endpoint_stop`, then
   `moq_endpoint_destroy`. (`stop` refuses with `MOQ_ERR_WRONG_STATE` while a
   child is still attached and remains an abrupt stop when you skip the drain.)
   `moq_endpoint_drain` is a **local stream flush**: it waits until the transport
   has no queued/ready reliable stream bytes or unsent FIN, not until packets
   are ACKed or the peer application has consumed the media.

For WebTransport interop with deployed relays that negotiate WT but omit the
`reset_stream_at` transport parameter required by draft-ietf-webtrans-http3, the
pico-WT managed client now has a self-gated compatibility path. There is no
example flag or endpoint-cfg knob to set; compliant peers are unaffected, and
the tolerance applies only on the WT client path.

### Threading

The endpoint runs one network thread; all session work happens there. Your
application thread only calls the public poll/wait/write surface, which is
internally synchronized. `moq_endpoint_set_interrupted(ep, true)` is a sticky
latch: every blocking call on the endpoint and its attachments returns
`MOQ_ERR_INTERRUPTED` immediately — this is the natural hook for GStreamer
`unlock()`, a VLC abort, or FFmpeg's interrupt callback.

### Ownership

- Receiver: `poll_object` MOQ_OK **transfers** the object's buffers to you;
  release them with `moq_media_object_cleanup(&obj)` on the polling thread.
- Sender: `write` MOQ_OK **transfers** your payload `moq_rcbuf_t` to the
  service. On any non-OK return you still own it and must `moq_rcbuf_decref`.

### Ending a track (EOS)

For a finite stream (a file, a clip, a stopped capture) call
`moq_media_sender_end_track(s, track)` to signal a clean end-of-stream. After the
track's already-queued objects drain, the service emits a reliable
`END_OF_TRACK` over a subgroup **stream** (not a QUIC datagram, so it does not
depend on datagram negotiation). Receivers surface `MOQ_MEDIA_TRACK_ENDED` and
`moq_media_receiver_track_state` returns `ENDED` — the EOS signal a
GStreamer/FFmpeg/VLC/OBS integration forwards downstream, rather than treating
connection teardown as the end. Ending one track does **not** end other tracks or
close the session, so a multi-track sender can end video while audio continues.
After `end_track`, `write` for that track returns `MOQ_ERR_WRONG_STATE`; a repeat
`end_track` is an idempotent `MOQ_OK`.

### Decoder configuration / init_data

Codec init/decoder config travels in the **catalog**, not in the media objects:

- The sender sets `moq_media_track_cfg_t.init_data` to the encoder's extradata —
  H.264/HEVC SPS/PPS/VPS, an AAC `AudioSpecificConfig`, or a CMAF init segment.
  This applies to **any** packaging (RAW/LOC or CMAF) when the codec needs it.
  The service copies it during `add_track` (your buffer need not outlive the
  call). How it is published depends on packaging:
  - **RAW/LOC** — base64'd **inline** on the track's catalog entry.
  - **CMAF** — published in the catalog's MSF-01 **`initDataList`** and
    referenced from the track via **`initRef`**. The emitter deduplicates:
    tracks whose CMAF init segment bytes are identical share a single
    `initDataList` entry, and the entry `id` is the name of the first track
    that introduced that init segment. The receiver follows the `initRef`
    back into `initDataList` and resolves it transparently (see below).

  Leave it empty when the codec carries its parameter sets in-band.
- The receiver gets two forms at `TRACK_ADDED` (both **borrowed** for the event
  — copy if you retain them):
  - `desc.init.codec_config` — the **decoder extradata** (SPS/PPS/VPS, AAC ASC).
    This is what you feed a decoder.
  - `desc.init_data` — the **full container/init segment** (for CMAF the
    `ftyp`+`moov`). Muxers/recorders that reproduce the container want this;
    decoders usually do **not**. This is the **resolved** segment whether the
    catalog carried it inline (RAW/LOC) or via `initRef`→`initDataList` (CMAF);
    the application sees the bytes either way and never resolves the reference
    itself.
- `init_data` is **not** app-written per-object metadata. Per-object LOC headers
  are generated by the service from the typed fields on each `write`.
- In v0 a mid-stream codec/config change is modeled as a **new track / config
  generation** (a new catalog entry), not by mutating a live track's
  `init_data`.

### CMAF bytes and decoder config

Where a polled object's media bytes live depends on `obj.packaging`:

- **CMAF** keeps the container context: the full fragment is `obj.fragment`. The
  decoder/muxer sample bytes are the mdat slice — `obj.fragment.data +
  obj.mdat_offset` for `obj.mdat_len` bytes. `obj.payload` is **empty** for
  CMAF; reading it (and seeing 0 bytes) is the common footgun. Decode the mdat
  slice; keep the whole `fragment` if you need the framing.
- **RAW / LOC / simple** objects put the media in `obj.payload` (`obj.fragment`
  is empty).
- For decoder setup, prefer `desc.init.codec_config` (extradata). Reach for
  `desc.init_data` only when you need the full init segment (e.g. a muxer or
  recorder reproducing `ftyp`+`moov`).

`media_receive.c` branches on `obj.packaging` and prints the right lengths for
each so the distinction is visible end to end.

### CMSF catalog metadata (SAP, content protection)

A CMSF (CMAF-constrained MSF) catalog carries optional per-track metadata that
`media_receive.c` prints at `TRACK_ADDED`. It is absent on a plain MSF catalog;
guard on the `has_*`/count fields before reading.

- **SAP starting types** (CMSF §3.5.2) — `desc.has_max_grp_sap` /
  `desc.max_grp_sap` and `desc.has_max_obj_sap` / `desc.max_obj_sap` describe the
  random-access structure (the strongest SAP type that begins a group / an
  object). libmoq surfaces them as advisory hints; it does not enforce them.
- **Content protection** (CMSF §4) — `desc.content_protection_ref_ids[]` lists
  the `contentProtectionRefIDs` for an encrypted track. Resolve each id to its
  root DRM entry (scheme `cenc`/`cbcs`, `defaultKID`s, `drmSystem`) with
  `moq_media_receiver_find_content_protection(rx, ref_id)`. **libmoq carries
  this DRM metadata only — decryption is the application/CDM's job.** All spans
  borrow from the receiver-retained catalog and stay valid for the receiver's
  lifetime; copy them if you outlive it.

### Validating CMAF output (sender)

A CMAF **sender** can opt into strict output checking with
A CMAF **sender** gets strict output checking by default:
`moq_media_sender_cfg_t.validate_cmaf = true` after the cfg initializers. The
service validates each written CMAF object against CMSF §3.3: it matches the
object's `track_ID` to the track's CMAF init segment and rejects a group-start
object whose first sample is known **not** to be a SAP (an `UNKNOWN` SAP type is
allowed through). It applies only to CMAF tracks; RAW/LOC writes are unaffected.
Set `validate_cmaf = false` after init only for deliberate passthrough in hot
paths where you trust the muxer.

### Catalog retrieval (SUBSCRIBE + Joining FETCH)

Per MSF-01 §5 the receiver obtains the catalog by **subscribing** to the catalog
track (so it sees later catalog updates) **and** issuing a **Joining
FETCH(offset = 0)** against that subscription — which pulls the latest complete
catalog immediately, even when the application joins long after the catalog was
first published. This happens inside the receiver; the application just observes
`TRACK_ADDED` / `CATALOG_READY`. A relay/origin that does not serve the FETCH
(it errors or ignores it) is non-fatal: the catalog SUBSCRIBE still delivers
subsequent live catalog generations. (A plain SUBSCRIBE does not replay the
retained base generation — that is obtained via the Joining FETCH.)

## Building

### Compile only (no transport)

The examples link only `moq::service`. `MOQ_BUILD_SERVICE` requires
`MOQ_BUILD_MSF` (the media-object normalizer is on by default), so:

```
cmake --preset rc-static -DMOQ_BUILD_SERVICE=ON -DMOQ_BUILD_MSF=ON
cmake --build build/rc-static --target moq_example_media_receive moq_example_media_send
```

This builds the binaries, but with no transport facade compiled in `connect()`
returns `MOQ_ERR_UNSUPPORTED` at runtime.

### Build to actually connect (raw QUIC via picoquic)

Add the picoquic adapter + threaded facade. The picoquic adapter needs the
picoquic source tree and picotls headers (no `.pc` is shipped), so pass their
locations:

```
cmake --preset rc-static \
    -DMOQ_BUILD_SERVICE=ON -DMOQ_BUILD_MSF=ON \
    -DMOQ_BUILD_ADAPTER_PICOQUIC=ON -DMOQ_BUILD_PQ_THREADED=ON \
    -DMOQ_PICOQUIC_SOURCE_DIR=/path/to/picoquic \
    -DMOQ_PICOTLS_PREFIX=/path/to/picotls/build
cmake --build build/rc-static --target moq_example_media_receive moq_example_media_send
```

(For WebTransport instead, use `-DMOQ_BUILD_ADAPTER_PICO_WT=ON
-DMOQ_BUILD_PICO_WT_MANAGED=ON` and an `https://host:port/path` URL.)

### Run (against a test relay)

TLS certificate verification is on by default — point these at a relay with a
trusted certificate. For a **local self-signed** test relay, append
`--insecure-skip-verify` (testing only):

```
./build/rc-static/examples/service/moq_example_media_send    moqt://localhost:4433 example video --insecure-skip-verify
./build/rc-static/examples/service/moq_example_media_receive moqt://localhost:4433 example       --insecure-skip-verify
```

### Consuming an installed libmoq (Meson / pkg-config)

After `cmake --install`, the service tier ships its own pkg-config file, so a
non-CMake build consumes it without `find_package` COMPONENTS:

```
pkg-config --cflags --libs libmoq-service
```

In Meson:

```meson
moq_service = dependency('libmoq-service')
executable('player', 'player.c', dependencies: moq_service)
```

`libmoq-service.pc` `Requires: libmoq` (and `libmoq-pico-wt-managed` when the
WebTransport facade was built), so the core/codec and transport link chain comes
through in one `pkg-config` call. It is a separate package from `libmoq.pc` —
the service tier is optional and not folded into the core `.pc`.

## Note on `media_send.c`

Its payloads are placeholder bytes, not an encoded bitstream — the example
shows the send *lifecycle*, not media encoding. A real sender passes encoded
access units (e.g. H.264 byte-stream AUs) as the payload and sets `is_sync`
on random-access points; the service generates the LOC-01 timing/keyframe
metadata from the typed fields. For a worked codec integration over a real
relay, see the GStreamer `moqsrc`/`moqsink` elements.
