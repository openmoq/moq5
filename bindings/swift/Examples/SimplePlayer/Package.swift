// swift-tools-version: 6.0

// SimplePlayer — the official MoQService example app. A universal SwiftUI
// player (iPhone / iPad / macOS) that shows the complete receive path in one
// screen: configure -> connect -> established -> receiver attach -> first
// video track -> format description -> sample-buffer loop -> display layer.
//
// It is its OWN package (not a target of the root MOQ5 package) because the
// root deliberately carries no `platforms:` floor, and an @main SwiftUI App
// needs iOS 16 / macOS 13 deployment floors to satisfy the MoQService
// availability without per-declaration @available on @main.
//
// Build (macOS, pkg-config lane):
//   MOQ_SERVICE=1 PKG_CONFIG_PATH=<prefix>/lib/pkgconfig swift build
//
// Build (iOS simulator, binary xcframework lane — see docs/ios-packaging.md):
//   MOQ_SERVICE=1 MOQ_SERVICE_IOS=1 swift build \
//     --triple arm64-apple-ios16.0-simulator \
//     --sdk "$(xcrun --sdk iphonesimulator --show-sdk-path)"
//
// CI builds and links this app for both platforms but never runs it: the
// connection happens behind the Connect button, never automatically.
import PackageDescription

let package = Package(
    name: "SimplePlayer",
    platforms: [.iOS(.v16), .macOS(.v13)],
    dependencies: [
        .package(name: "MOQ5", path: "../../../.."),
    ],
    targets: [
        .executableTarget(
            name: "SimplePlayer",
            dependencies: [
                .product(name: "MoQService", package: "MOQ5"),
            ]
            // CA trust roots are NOT vendored here. `scripts/make-app.sh`
            // fetches the Mozilla CA bundle into the built .app at bundle time;
            // the app reads it from its own bundle (see PlayerModel).
        ),
    ]
)
