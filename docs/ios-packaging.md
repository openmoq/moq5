# iOS packaging for MoQService

The Swift `MoQService` product reaches an app two ways, by **package**, not by
environment:

| Platform | Package | Lane | How `import CMoQService` resolves | Env |
|---|---|---|---|---|
| iOS / Xcode | `MoQServiceApple` (`bindings/swift/Packages/MoQServiceApple`) | binary xcframework | `.binaryTarget` on `LibMoQ.xcframework` whose module map vends `CMoQService` | **none** |
| macOS | root `MOQ5` | pkg-config | `.systemLibrary(pkgConfig: "libmoq-service")` over a CMake-installed prefix | `MOQ_SERVICE=1` |

The iOS/Xcode lane is a **first-party wrapper package**, `MoQServiceApple`, that
vends `MoQService` **unconditionally** from the prebuilt xcframeworks. Xcode sets
no environment during manifest evaluation, so an env-gated product is invisible
to it; the wrapper needs no `MOQ_SERVICE*` env at all. It is **iOS-only** (iOS 16
floor, no macOS support — the xcframeworks carry iOS device + simulator slices
only) and does not depend on `MOQ5`. Its `MoQServiceCore` /
`MoQService` targets are the same source files the root package builds, symlinked
into the wrapper's own `Sources/` (SwiftPM rejects target paths escaping the
package root but accepts symlinked source directories inside it).

The macOS lane keeps `MOQ_SERVICE=1`. That is a **manifest product-visibility
gate** on the root `MOQ5` package — it selects the pkg-config service lane for a
source build — not something an iOS/Xcode consumer needs. A macOS `MOQ_SERVICE=1`
build always takes the pkg-config path even when the iOS xcframeworks happen to
be built locally, so the default developer lane is unaffected.

(The root `MOQ5` manifest still carries an internal env-gated iOS binary lane
behind `MOQ_SERVICE=1 MOQ_SERVICE_IOS=1` for a CLI `swift build`. It is superseded
by `MoQServiceApple` for consumers and is not the recommended path — Xcode cannot
set that env — so it is left in place but not taught here.)

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
`build-ios/dist/LibMoQ.xcframework` — the repo-root paths the `MoQServiceApple`
wrapper's `.binaryTarget`s point at (relative to the wrapper root as
`../../../../build-ios{,-openssl}/dist/…`). The `MoQServiceApple` lane expects
these default locations. (The root `MOQ5` CLI lane additionally honors
`MOQ_SERVICE_IOS_XCFRAMEWORK_DIR` for an alternate location; the wrapper does
not.)

Both scripts honor `MOQ_IOS_SLICES` (default `device simulator`). CI sets
`MOQ_IOS_SLICES=simulator` to build only the simulator slice it links against;
the local full build produces both device and simulator slices for a shippable
xcframework.

## Consuming from an app

Add the `MoQServiceApple` wrapper package and depend on its `MoQService`
product. In Xcode: File → Add Package Dependencies → Add Local →
`bindings/swift/Packages/MoQServiceApple`. No scheme environment is involved.

From the command line, build with **no** `MOQ_SERVICE*` env — the first-party
`SimplePlayerXcode` example is exactly this consumer:

```bash
cd bindings/swift/Examples/SimplePlayerXcode
swift build \
  --triple arm64-apple-ios16.0-simulator \
  --sdk "$(xcrun --sdk iphonesimulator --show-sdk-path)"
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

`scripts/check_ios_consumers.sh` builds the env-free `SimplePlayerXcode`
consumer (through the `MoQServiceApple` wrapper, no `MOQ_SERVICE*` env) against
the local xcframeworks for the simulator and device triples (link-only; no
boot). Run it after regenerating the artifacts -- it fails on a stale
`LibMoQ.xcframework` whose headers predate a selectable backend.

## Binary exclusivity still applies

One app binary links **either** `MoQService` (the installed/binary libmoq archives)
**or** the source-built `MoQ`/`MoQMedia` stack — never both. Linking both yields two
copies of the core symbols; the `moq_swift_stack_guard` duplicate-symbol canary
(`scripts/check_swift_stack_guard.sh`) turns that into a hard link error rather than
a silent first-archive-wins binding. The rule is identical on iOS: an iOS app links
the `MoQService` xcframework lane, not the from-source targets.
