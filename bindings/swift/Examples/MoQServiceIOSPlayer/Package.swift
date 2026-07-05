// swift-tools-version: 6.0

// Standalone iOS example: a minimal SwiftUI player skeleton over the
// MoQService binary lane. It is its OWN package (not a target of the root
// MOQ5 package) because the root deliberately carries no `platforms:` floor,
// and an @main SwiftUI App needs an iOS-16 deployment floor to satisfy the
// MoQService availability without per-declaration @available on @main.
//
// Build (simulator, build/link-only — never run in CI, no network):
//   MOQ_SERVICE=1 MOQ_SERVICE_IOS=1 swift build \
//     --triple arm64-apple-ios16.0-simulator \
//     --sdk "$(xcrun --sdk iphonesimulator --show-sdk-path)"
//
// Resolving the MOQ5 dependency in binary mode requires the xcframeworks to
// exist (scripts/build_ios_*.sh); see docs/ios-packaging.md.
import PackageDescription

let package = Package(
    name: "MoQServiceIOSPlayer",
    platforms: [.iOS(.v16)],
    dependencies: [
        .package(name: "MOQ5", path: "../../../.."),
    ],
    targets: [
        .executableTarget(
            name: "MoQServiceIOSPlayer",
            dependencies: [
                .product(name: "MoQService", package: "MOQ5"),
            ]
        ),
    ]
)
