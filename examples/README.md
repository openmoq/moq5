# libmoq examples

These examples are grouped by audience. If you are integrating libmoq into a
media application (FFmpeg, VLC, OBS, a GStreamer plugin, your own player or
encoder), **start with `service/`** — that is the high-level library surface.
The other groups are for libmoq contributors, transport-adapter authors, and
historical reference.

## Taxonomy

### `service/` — start here (consumer-facing)
The high-level API: `moq_endpoint_t` plus `moq_media_receiver_t` /
`moq_media_sender_t` (the `moq::service` component). You give it a URL and a
namespace; it owns the network thread, version negotiation, TLS, the MSF
catalog, and object packaging/parsing. Application code only touches public
headers (`moq/endpoint.h`, `moq/media_receiver.h`, `moq/media_sender.h`) and
links `moq::service` — no session plumbing, no adapter types.

- `service/media_receive` — connect an endpoint, attach a receiver, observe
  `TRACK_ADDED` / `CATALOG_READY`, and poll media objects.
- `service/media_send` — connect an endpoint, attach a sender, add a track,
  and write media objects.

See `service/README.md` for lifecycle, ownership, threading, and CMake usage.

### `session/` — sans-I/O protocol (contributors)
Examples that drive `moq_session_t` directly via the action/event model, with
no transport. These show how the protocol core works and are aimed at people
hacking on libmoq itself, not at application integrators.

- `session/setup_handshake`, `session/sans_io_demo`

### transport adapters (`picoquic/`, `pico_wt/`, `mvfst/`) — adapter authors
How to bind the core to a specific transport backend (raw QUIC via picoquic,
WebTransport via pico-wt, mvfst). One level below `service/`: useful if you
are writing or debugging an adapter, or need a backend the service tier does
not yet expose. (These live in per-adapter directories today; a future
consolidation under `adapters/` is possible.)

### `_archive/` — reference only, not a starting point
Older or narrowly-scoped examples kept for reference (`_archive/core`,
`_archive/relay`, `_archive/simulation`). Do not begin here.

### lower-level facade demos (`local-media/`, `publisher/`, `cpp/`)
Demos built on the publisher/subscriber facade (`moq_publisher_t` /
`moq_subscriber_t`) over an adapter, predating the service tier. They remain
useful as references, but for new consumer integrations the service tier
supersedes them — prefer `service/`.

## Building

The service examples build when the service tier is enabled. `MOQ_BUILD_SERVICE`
requires `MOQ_BUILD_MSF` (the media-object normalizer is on by default):

```
cmake --preset rc-static -DMOQ_BUILD_SERVICE=ON -DMOQ_BUILD_MSF=ON
cmake --build build/rc-static --target moq_example_media_receive moq_example_media_send
```

That compiles the examples; to actually connect them you also need a transport
facade built in — see `service/README.md`. (The canonical configure presets
place build trees under `build/<preset>`; see the top-level
`CMakePresets.json`.)
