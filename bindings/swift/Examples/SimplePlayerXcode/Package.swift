// swift-tools-version: 6.0

// SimplePlayerXcode — the env-free iOS/Xcode consumer of MoQService.
//
// This is the iOS half of the SimplePlayer split. The universal example lives
// in ../SimplePlayer and depends on the root MOQ5 package: on macOS that
// resolves MoQService via the pkg-config service lane (MOQ_SERVICE=1), and its
// iOS lane needed MOQ_SERVICE=1 MOQ_SERVICE_IOS=1 — env Xcode never sets during
// manifest evaluation, so an Xcode iOS consumer could not see the product.
//
// This package instead depends on the first-party MoQServiceApple wrapper,
// which vends MoQService UNCONDITIONALLY from the prebuilt xcframeworks. No
// MOQ_SERVICE* environment is required, so Xcode (and env-free `swift build`)
// resolve it directly. The one-time xcframework build is still a prerequisite
// (scripts/build_ios_openssl.sh + scripts/build_ios_xcframework.sh); this
// removes the env requirement, not the artifacts.
//
// The SimplePlayer app sources are the SAME files ../SimplePlayer builds; they
// are symlinked under this package's own Sources/ (SwiftPM rejects target paths
// that escape the package root but accepts symlinked source directories inside
// it):
//   Sources/SimplePlayer -> ../../SimplePlayer/Sources/SimplePlayer
//   Info.plist           -> ../SimplePlayer/Info.plist
//
// No test target here: the model tests run in the ../SimplePlayer package.
//
// iOS ONLY. MoQServiceApple's xcframeworks are iOS device + simulator slices
// only, so this package declares an iOS 16 floor and no macOS support. The
// macOS build of the same sources is the ../SimplePlayer package (root MOQ5
// pkg-config lane).
import PackageDescription

let package = Package(
    name: "SimplePlayerXcode",
    platforms: [.iOS(.v16)],
    dependencies: [
        .package(name: "MoQServiceApple", path: "../../Packages/MoQServiceApple"),
    ],
    targets: [
        .executableTarget(
            name: "SimplePlayer",
            dependencies: [
                .product(name: "MoQService", package: "MoQServiceApple"),
            ],
            path: "Sources/SimplePlayer"
            // CA trust roots are NOT vendored here. scripts/make-app.sh fetches
            // the Mozilla CA bundle into the built .app at bundle time; the app
            // reads it from its own bundle (see PlayerModel).
        ),
    ]
)
