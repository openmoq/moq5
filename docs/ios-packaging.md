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
# 1. iOS OpenSSL (pinned openssl-3.5.7) -> OpenSSL.xcframework + per-slice prefixes
scripts/build_ios_openssl.sh
#    prints: MOQ_IOS_OPENSSL_ROOT=<build-ios-openssl>/prefix-ios-device

# 2. LibMoQ (libmoq + picoquic + picotls merged) -> LibMoQ.xcframework
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

`LibMoQ.xcframework` holds the libmoq/picoquic/picotls objects but **not** OpenSSL;
the app links OpenSSL separately from `OpenSSL.xcframework`. The manifest models
this as two binary targets (`LibMoQBinary` + `OpenSSLBinary`), both dependencies of
`MoQService`. No extra system frameworks are required to link.

## OpenSSL and the CA bundle on iOS

The iOS demo/transport path is built against OpenSSL, and **iOS ships no readable
system PEM bundle**. An app relying on OpenSSL certificate verification must supply
its own trust roots via `MoQEndpoint.Configuration.caFileURL` (e.g. a bundled
`cacert.pem`); there is no OS default to fall back on. This differs from macOS,
where a system OpenSSL trust store is typically reachable.

## Binary exclusivity still applies

One app binary links **either** `MoQService` (the installed/binary libmoq archives)
**or** the source-built `MoQ`/`MoQMedia` stack — never both. Linking both yields two
copies of the core symbols; the `moq_swift_stack_guard` duplicate-symbol canary
(`scripts/check_swift_stack_guard.sh`) turns that into a hard link error rather than
a silent first-archive-wins binding. The rule is identical on iOS: an iOS app links
the `MoQService` xcframework lane, not the from-source targets.
