# iOS packaging for MoQService

The Swift `MoQService` product is consumed two ways, chosen at manifest-eval time
by the environment:

| Platform | Lane | How `import CMoQService` resolves | Env |
|---|---|---|---|
| macOS | pkg-config | `.systemLibrary(pkgConfig: "libmoq-service")` over a CMake-installed prefix | `MOQ_SERVICE=1` |
| iOS | binary xcframework | `.binaryTarget` on `LibMoQ.xcframework` whose module map vends `CMoQService` | `MOQ_SERVICE=1 MOQ_SERVICE_IOS=1` |

`MOQ_SERVICE_IOS=1` is an explicit opt-in for an iOS build. A macOS `MOQ_SERVICE=1`
build always takes the pkg-config path even when the iOS xcframeworks happen to be
built locally, so the default developer lane is unaffected.

## Building the iOS artifacts

Two scripts produce the xcframeworks (macOS host, no network for the libmoq step):

```bash
# 0. Pinned wtquic checkout (fetch-only: git only, no configure/MsQuic) --
#    the LibMoQ step cross-compiles wtquic core/network from it per slice
WTQ_FETCH_ONLY=1 scripts/setup_wtquic_deps.sh

# 1. iOS OpenSSL (pinned openssl-3.5.7) -> OpenSSL.xcframework + per-slice prefixes
scripts/build_ios_openssl.sh
#    prints: MOQ_IOS_OPENSSL_ROOT=<build-ios-openssl>/prefix-ios-device

# 2. LibMoQ (libmoq + picoquic + picotls + wtquic core/network merged)
#    -> LibMoQ.xcframework
MOQ_IOS_OPENSSL_ROOT=<build-ios-openssl>/prefix-ios-device \
  scripts/build_ios_xcframework.sh
```

Defaults land at `build-ios-openssl/dist/OpenSSL.xcframework` and
`build-ios/dist/LibMoQ.xcframework` — the relative paths the manifest expects
(a `.binaryTarget` path must be relative to the package root). Point elsewhere,
still relative to the root, with `MOQ_SERVICE_IOS_XCFRAMEWORK_DIR`.

Both scripts honor `MOQ_IOS_SLICES` (default `device simulator`). CI sets
`MOQ_IOS_SLICES=simulator` to build only the simulator slice it links against;
the local full build produces both device and simulator slices for a shippable
xcframework.

## Consuming from an app

```bash
MOQ_SERVICE=1 MOQ_SERVICE_IOS=1 swift build \
  --triple arm64-apple-ios16.0-simulator \
  --sdk "$(xcrun --sdk iphonesimulator --show-sdk-path)" \
  --target MoQService
```

`LibMoQ.xcframework` holds the libmoq/picoquic/picotls objects plus wtquic's
core and Network.framework backend (so `.wtquicNetwork` is selectable on iOS).
It does **not** build MsQuic, so the two MsQuic-backed backends — `.msquic`
(direct raw QUIC) and `.wtquicMsquic` (WebTransport over MsQuic) — are absent:
each compiles as API (the `MoQEndpoint.TransportBackend` enum always carries the
case) but fails at connect with `.unsupported` on this artifact. It also does
**not** hold OpenSSL;
the app links OpenSSL separately from `OpenSSL.xcframework`. The manifest models
this as two binary targets (`LibMoQBinary` + `OpenSSLBinary`), both dependencies of
`MoQService`. `MoQService` itself links the Network and Security system
frameworks automatically (Apple-conditioned linker settings in the manifest);
no other system frameworks are required.

## Trust roots per backend on iOS

The two backend families verify server certificates differently:

- **`.picoquic` family (raw QUIC / pico_wt WebTransport)** — verification runs
  through OpenSSL, and **iOS ships no readable system PEM bundle**: the app
  must supply its own trust roots via `MoQEndpoint.Configuration.caFileURL`
  (e.g. a bundled `cacert.pem`); there is no OS default to fall back on. This
  differs from macOS, where a system OpenSSL trust store is typically
  reachable.
- **`.wtquicNetwork` (WebTransport over Network.framework)** — verification is
  the platform's own trust evaluation (`SecTrustEvaluateWithError` against the
  system store); **no CA file is used, and `caFileURL` is rejected as
  unsupported** with verification on. Use system trust, or
  `insecureSkipVerify` for local self-signed testing only.
- **`.wtquicMsquic` (WebTransport over MsQuic)** — not built into the iOS
  xcframework (see above), so it never reaches a trust evaluation here: it
  fails at connect with `.unsupported`. On macOS, where it is built, it uses
  system trust with no CA file, like `.wtquicNetwork`.

## Consumer link checks

`scripts/check_ios_consumers.sh` builds SimplePlayer against the local
xcframeworks for the simulator and device triples (link-only; no boot). Run it
after regenerating the artifacts -- it fails on a stale `LibMoQ.xcframework`
whose headers predate a selectable backend.

## Binary exclusivity still applies

One app binary links **either** `MoQService` (the installed/binary libmoq archives)
**or** the source-built `MoQ`/`MoQMedia` stack — never both. Linking both yields two
copies of the core symbols; the `moq_swift_stack_guard` duplicate-symbol canary
(`scripts/check_swift_stack_guard.sh`) turns that into a hard link error rather than
a silent first-archive-wins binding. The rule is identical on iOS: an iOS app links
the `MoQService` xcframework lane, not the from-source targets.
