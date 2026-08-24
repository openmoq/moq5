// swift-tools-version: 6.0

import PackageDescription

// First-party Xcode/iOS wrapper for the MoQService product.
//
// The root MOQ5 package only vends MoQService under MOQ_SERVICE=1 (macOS
// pkg-config) or an additional MOQ_SERVICE_IOS=1 (iOS xcframeworks). Xcode sets
// no environment during manifest evaluation, so an iOS consumer adding the root
// package cannot see the product. This package vends MoQService unconditionally
// from the prebuilt xcframeworks and does not depend on MOQ5 (which would
// reintroduce the env gate).
//
// iOS ONLY. The xcframeworks carry iOS device + iOS simulator slices only
// (scripts/build_ios_xcframework.sh / build_ios_openssl.sh), so this package
// declares an iOS 16 floor and no macOS support — a macOS product shape here
// would be a claim the binary targets cannot satisfy. The macOS lane is the
// root MOQ5 pkg-config path (used by the ../../Examples/SimplePlayer package).
//
// The Swift sources are the same files the root package builds; they are
// symlinked under this package's own Sources/ because SwiftPM rejects target
// paths that escape the package root but accepts symlinked source directories
// inside it:
//   Sources/MoQServiceCore -> ../../../Sources/MoQServiceCore
//   Sources/MoQService     -> ../../../Sources/MoQService
//
// The xcframeworks are BUILT artifacts (scripts/build_ios_openssl.sh +
// scripts/build_ios_xcframework.sh), not committed. A clean checkout resolves
// this manifest only after they are built; that removes the env requirement,
// not the one-time build. No .xcframework is vendored or committed here.

// iOS framework linkage, matching the root service lane. wtquic declares
// Network, Security, and Foundation as public framework dependencies of
// wtq::network; link all three explicitly.
let moqServiceLinkerSettings: [LinkerSetting] = [
    .linkedFramework("Network", .when(platforms: [.iOS])),
    .linkedFramework("Security", .when(platforms: [.iOS])),
    .linkedFramework("Foundation", .when(platforms: [.iOS])),
]

let package = Package(
    name: "MoQServiceApple",
    platforms: [.iOS(.v16)],
    products: [
        .library(name: "MoQService", targets: ["MoQService"]),
    ],
    targets: [
        // Paths point up into the repo root's build output dirs. binaryTarget
        // paths are relative to this package's root.
        .binaryTarget(
            name: "LibMoQBinary",
            path: "../../../../build-ios/dist/LibMoQ.xcframework"
        ),
        .binaryTarget(
            name: "OpenSSLBinary",
            path: "../../../../build-ios-openssl/dist/OpenSSL.xcframework"
        ),
        .target(
            name: "MoQServiceCore",
            path: "Sources/MoQServiceCore"
        ),
        .target(
            name: "MoQService",
            // LibMoQBinary vends module CMoQService; OpenSSLBinary is the
            // separate libssl/libcrypto link.
            dependencies: ["MoQServiceCore", "LibMoQBinary", "OpenSSLBinary"],
            path: "Sources/MoQService",
            linkerSettings: moqServiceLinkerSettings
        ),
    ]
)
