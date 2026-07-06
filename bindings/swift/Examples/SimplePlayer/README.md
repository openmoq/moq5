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

macOS (pkg-config lane; needs a CMake-installed service prefix built with
`MOQ_BUILD_PICO_WT_MANAGED=ON` for WebTransport):

```bash
MOQ_SERVICE=1 PKG_CONFIG_PATH=<prefix>/lib/pkgconfig swift build
```

iOS simulator (binary xcframework lane; see `docs/ios-packaging.md` for
building `LibMoQ.xcframework` / `OpenSSL.xcframework` first):

```bash
MOQ_SERVICE=1 MOQ_SERVICE_IOS=1 swift build \
  --triple arm64-apple-ios16.0-simulator \
  --sdk "$(xcrun --sdk iphonesimulator --show-sdk-path)"
```

CI builds both lanes but never runs the app (no network in CI); the
connection happens only behind the Connect button.

## Using it

Enter a relay URL (`https://…` connects over WebTransport, `moqt://…` over
raw QUIC) and a namespace, then Connect. The Advanced disclosure maps 1:1 to
`MoQEndpoint.Configuration`: transport override, version pin, CA bundle
path, and skip-verification toggle.

While playing, the controls recede to leave just the video; tap it to bring
them back. The status line (`Playing — N samples · track · draft`) lives in
that control card, never over the video. The layout adapts to rotation — a 4:3
movie fills far more of the frame in landscape. `Info.plist` carries the
orientation config for when the executable is bundled into a `.app`.

Notes:

- TLS verification is ON by default. On iOS there is no readable system PEM
  bundle, so OpenSSL verification needs an explicit CA file (Advanced → CA
  bundle path). The app bundles `cacert.pem` and uses it by default.
- Playback renders RAW/LOC tracks (one access unit per object): H.264/HEVC
  video and AAC audio. CMAF streams are refused honestly (`unsupported`)
  rather than mis-rendered.
- Sample buffers are enqueued on iOS 17 / macOS 14+ (the `sampleBufferRenderer`
  API); older systems count samples without display.
