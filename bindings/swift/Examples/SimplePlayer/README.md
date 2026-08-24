# SimplePlayer

The official MoQService example app: a universal SwiftUI player (iPhone /
iPad / macOS) that shows the complete receive path on one screen —

```
configure -> connect -> established -> MediaReceiver.attach(.live)
  -> catalog -> select video (+ audio) tracks -> makeFormatDescription
  -> for-await objects, routed by role -> makeSampleBuffer -> RenderPipeline
```

Audio and video share one `AVSampleBufferRenderSynchronizer`: when the stream
carries audio, the synchronizer slaves its clock to the audio renderer (video
is timed to audio); with no audio, video runs on its own timeline.

Reading order: `PlayerModel.swift` (state machine + strictly-serial session
lifecycle + object loop), `RenderPipeline.swift` (the A/V sync + render layer),
`ContentView.swift` (the cinematic-dark UI), `VideoSurface.swift` (the only
platform-conditional file: `UIViewRepresentable` on iOS, `NSViewRepresentable`
on macOS, same layer either way).

## Build

macOS (pkg-config lane; needs a CMake-installed service prefix). The default
backend is the picoquic family, so build the service with
`MOQ_BUILD_PICO_WT_MANAGED=ON` for WebTransport `https://` relays and
`MOQ_BUILD_PQ_THREADED=ON` for raw-QUIC `moqt://` relays; add
`MOQ_BUILD_WTQUIC_NETWORK_MANAGED=ON` only to experiment with the (unreliable)
Network.framework backend:

```bash
MOQ_SERVICE=1 PKG_CONFIG_PATH=<prefix>/lib/pkgconfig swift build
```

iOS / Xcode (env-free binary xcframework lane). This `SimplePlayer` package
depends on the root `MOQ5` service product, whose iOS lane is env-gated — env
Xcode never sets during manifest evaluation. So the iOS/Xcode build lives in the
sibling **`../SimplePlayerXcode`** package, which reuses these same sources but
depends on the `MoQServiceApple` wrapper (vends `MoQService` with no
`MOQ_SERVICE*` env). Build the `LibMoQ.xcframework` / `OpenSSL.xcframework`
first (see `docs/ios-packaging.md`), then, with **no** env:

```bash
cd ../SimplePlayerXcode
swift build \
  --triple arm64-apple-ios16.0-simulator \
  --sdk "$(xcrun --sdk iphonesimulator --show-sdk-path)"
# or bundle a .app:  ../SimplePlayer/scripts/make-app.sh ios-sim
```

CI builds both lanes but never runs the app (no network in CI); the
connection happens only behind the Connect button.

## Using it

Enter a relay URL (`https://…` for WebTransport, `moqt://…` for raw QUIC) and
a namespace, then Connect. The Advanced disclosure maps 1:1 to
`MoQEndpoint.Configuration`: transport override, **backend** (default
`.picoquic` — the reliable family for both WebTransport `https://` and raw-QUIC
`moqt://`; `Auto` also resolves to picoquic; MsQuic WebTransport for a
cross-platform WT stack; and the experimental Network.framework backend),
version pin, CA bundle path, and skip-verification toggle. The controls are
disabled while a connection is live (configuration is fixed for the life of a
connection).

While playing, the controls recede to leave just the video; tap it to bring
them back. The status line (`Playing — N samples · track · draft`) lives in
that control card, never over the video. The layout adapts to rotation — a 4:3
movie fills far more of the frame in landscape. `Info.plist` carries the
orientation config for when the executable is bundled into a `.app`.

Notes:

- TLS verification is ON by default. The `.wtquicNetwork` and `.wtquicMsquic`
  WebTransport backends verify against SYSTEM trust, so no CA file is used (the
  CA control is hidden for both). The picoquic family (the default) uses
  OpenSSL, and on iOS there is no readable system PEM bundle — so a
  picoquic-family connection needs an explicit CA file (Advanced → CA bundle
  path); the app bundles `cacert.pem` and uses it by default for that family.
- The **Network.framework** backend (`.wtquicNetwork`) is **experimental** and
  no longer the default. On the tested OS/SDK (macOS 15.7.3 / iOS 26.2),
  Network.framework's public QUIC-multiplex client drops a conformant HTTP/3
  server's eager control stream during the connection's ready transition, so
  establishment against third-party relays is unreliable. It stays selectable
  in Advanced for experimentation; prefer the default picoquic family.
- The **MsQuic WebTransport** backend (`.wtquicMsquic`) is cross-platform, but
  the iOS `LibMoQ.xcframework` does **not** build MsQuic — selecting it there
  compiles fine (the enum is always present) but fails at connect with
  `unsupported`. It is meant for the macOS build.
- Playback renders RAW/LOC tracks (one access unit per object): H.264/HEVC
  video and AAC audio. CMAF streams are refused honestly (`unsupported`)
  rather than mis-rendered.
- Sample buffers are enqueued on iOS 17 / macOS 14+ (the `sampleBufferRenderer`
  API); older systems count samples without display.
