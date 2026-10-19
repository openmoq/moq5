# SimplePlayer

The official MoQService example app: a universal SwiftUI player (iPhone /
iPad / macOS) that shows the complete receive path on one screen —

```
configure -> connect -> established -> MediaPlayer.attach (automatic selection)
  -> for-await outputs: CONFIGURE -> makeFormatDescription
                        TIMELINE  -> the synchronizer's clock
                        UNIT      -> makeSampleBuffer -> RenderPipeline
                        DISCONTINUITY -> flush
```

The media player does the work between the network and the renderers:
track selection, decode order, gap recovery, and the presentation timeline.
Audio and video share one `AVSampleBufferRenderSynchronizer`, started on the
player's `TIMELINE` mapping (media time onto the host clock), so each sample
presents at its own presentation time; when the stream carries audio the
synchronizer follows the audio device's clock. Each unit is enqueued only
when its renderer has room, and no further output is pulled meanwhile: the
player, and behind it the receiver, back up instead of the app buffering.

H.264/HEVC units that arrive as Annex-B under an avcC/hvcC configuration
are converted to length prefixes (`makeSampleBuffer(formatDescription:framing:
.annexB)`); CoreMedia decodes nothing otherwise.

Reading order: `PlayerModel.swift` (state machine + strictly-serial watch
lifecycle + output loop), `RenderPipeline.swift` (the A/V sync + render layer),
`ContentView.swift` (the cinematic-dark UI), `VideoSurface.swift` (the only
platform-conditional file: `UIViewRepresentable` on iOS, `NSViewRepresentable`
on macOS, same layer either way).

## Build

macOS (pkg-config lane; needs a CMake-installed service prefix built with
the playback engine, `MOQ_BUILD_PLAYBACK=ON`, which the media player needs).
The default backend is the picoquic family, so build the service with
`MOQ_BUILD_PICO_WT_MANAGED=ON` for WebTransport `https://` relays and
`MOQ_BUILD_PQ_THREADED=ON` for raw-QUIC `moqt://` relays; add
`MOQ_BUILD_WTQUIC_NETWORK_MANAGED=ON` only for Network packaging/policy checks.
The current admission policy rejects `.wtquicNetwork` synchronously with
`.unsupported`; enabling the build option does not enable playback:

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

Choosing **Draft 22** explicitly selects both its wire version and its
MSF-01/LOC-04 media companion profile. Automatic version selection keeps the
legacy profile and default draft-18/16 offer. This opt-in does not bypass the
backend or playback limitations below.

While playing, the controls recede to leave just the video; tap it to bring
them back. The status line (`Playing — N samples · track · draft`) lives in
that control card, never over the video. The layout adapts to rotation — a 4:3
movie fills far more of the frame in landscape. `Info.plist` carries the
orientation config for when the executable is bundled into a `.app`.

Notes:

- TLS verification is ON by default. The `.wtquicNetwork` and `.wtquicMsquic`
  selections retain their system-trust configuration; Network currently rejects
  before trust evaluation under the admission policy described below. These
  WebTransport backends verify against SYSTEM trust, so no CA file is used (the
  CA control is hidden for both). The picoquic family (the default) uses
  OpenSSL, and on iOS there is no readable system PEM bundle — so a
  picoquic-family connection needs an explicit CA file (Advanced → CA bundle
  path); the app bundles `cacert.pem` and uses it by default for that family.
- The **Network.framework** selection (`.wtquicNetwork`) currently returns
  `.unsupported` before network startup because LibMoQ requires qualified
  flow-controlled admission. This is not a claim that native WT Network is
  broken. The enum/UI selection is retained; use the default picoquic family
  for playback. Old successful Network runtime receipts are historical only.
- The **MsQuic WebTransport** backend (`.wtquicMsquic`) is cross-platform, but
  the iOS `LibMoQ.xcframework` does **not** build MsQuic — selecting it there
  compiles fine (the enum is always present) but fails at connect with
  `unsupported`. It is meant for the macOS build.
- Playback renders RAW/LOC tracks (one access unit per object): H.264/HEVC
  video and AAC audio. CMAF streams are refused honestly (`unsupported`)
  rather than mis-rendered.
- Sample buffers are enqueued on iOS 17 / macOS 14+ (the `sampleBufferRenderer`
  API); older systems count samples without display.
