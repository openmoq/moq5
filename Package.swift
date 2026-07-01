// swift-tools-version: 6.0

import PackageDescription
import Foundation

let enableTransport = ProcessInfo.processInfo.environment["MOQ_TRANSPORT"] == "1"

let package = Package(
    name: "MOQ5",
    products: [
        .library(name: "MoQ", targets: ["MoQ"]),
        .library(name: "MoQMedia", targets: ["MoQMedia"]),
    ],
    targets: [
        // C core: sans-I/O MoQ session engine.
        .target(
            name: "CMoQCore",
            path: "core",
            sources: ["src"],
            publicHeadersPath: "include",
            cSettings: [
                .headerSearchPath("src"),
                .headerSearchPath("src/session"),
                .headerSearchPath("src/internal"),
            ]
        ),

        // C media: LOC properties (draft-ietf-moq-loc-01).
        .target(
            name: "CMoQLOC",
            dependencies: ["CMoQCore"],
            path: "media/loc",
            sources: ["src"],
            publicHeadersPath: "include"
        ),

        // C media: CMAF fragment parser.
        .target(
            name: "CMoQCMAF",
            dependencies: ["CMoQCore"],
            path: "media/cmaf",
            sources: ["src"],
            publicHeadersPath: "include"
        ),

        // C media: packaging-independent object parser.
        .target(
            name: "CMoQMediaObject",
            dependencies: ["CMoQCore", "CMoQLOC", "CMoQCMAF"],
            path: "media/object",
            sources: ["src"],
            publicHeadersPath: "include"
        ),

        // Swift bindings.
        .target(
            name: "MoQ",
            dependencies: ["CMoQCore"],
            path: "bindings/swift/Sources/MoQ"
        ),

        // Swift media layer.
        .target(
            name: "MoQMedia",
            dependencies: ["MoQ", "CMoQMediaObject", "CMoQLOC", "CMoQCMAF", "CMoQPlayback"],
            path: "bindings/swift/Sources/MoQMedia"
        ),

        // C media: playback pipeline.
        .target(
            name: "CMoQPlayback",
            dependencies: ["CMoQCore", "CMoQLOC", "CMoQCMAF", "CMoQMediaObject"],
            path: "media/playback",
            sources: ["src"],
            publicHeadersPath: "include"
        ),

        // Example: in-process media publish/subscribe demo.
        .executableTarget(
            name: "swift-media-demo",
            dependencies: ["MoQ", "MoQMedia"],
            path: "bindings/swift/Examples/SwiftMediaDemo"
        ),

        // CLI argument parser (testable library).
        .target(
            name: "MoQRecvArgs",
            path: "bindings/swift/Sources/MoQRecvArgs"
        ),

        // CLI: receiver demo (no transport dependency).
        .executableTarget(
            name: "moq-swift-recv",
            dependencies: ["MoQ", "MoQMedia", "MoQRecvArgs"],
            path: "bindings/swift/Examples/MoQSwiftRecv"
        ),




        // Simulation (test-only).
        .target(
            name: "CMoQSim",
            dependencies: ["CMoQCore"],
            path: "sim",
            sources: ["src"],
            publicHeadersPath: "include",
            cSettings: [
                .headerSearchPath("src"),
            ]
        ),

        // Tests.
        .testTarget(
            name: "MoQTests",
            dependencies: ["MoQ", "CMoQSim"],
            path: "bindings/swift/Tests/MoQTests"
        ),
        .testTarget(
            name: "MoQMediaTests",
            dependencies: ["MoQMedia", "CMoQSim"],
            path: "bindings/swift/Tests/MoQMediaTests"
        ),
        .testTarget(
            name: "MoQRecvArgsTests",
            dependencies: ["MoQRecvArgs"],
            path: "bindings/swift/Tests/MoQRecvArgsTests"
        ),
    ]
)

// Live transport targets require CMake-installed adapter.
// Build with: MOQ_TRANSPORT=1 PKG_CONFIG_PATH=<prefix>/lib/pkgconfig swift build
if enableTransport {
    package.targets += [
        .systemLibrary(
            name: "CMoQTransport",
            path: "bindings/swift/SystemModules/CMoQTransport",
            pkgConfig: "libmoq",
            providers: [.brew(["libmoq"])]
        ),
        .target(
            name: "MoQTransport",
            dependencies: ["CMoQTransport", "MoQ"],
            path: "bindings/swift/Sources/MoQTransport"
        ),
        .executableTarget(
            name: "moq-swift-recv-live",
            dependencies: ["MoQ", "MoQMedia", "MoQRecvArgs", "MoQTransport"],
            path: "bindings/swift/Examples/MoQSwiftRecvLive"
        ),
        .executableTarget(
            name: "moq-swift-player",
            dependencies: ["MoQ", "MoQMedia", "MoQRecvArgs", "MoQTransport"],
            path: "bindings/swift/Examples/MoQSwiftPlayer",
            linkerSettings: [
                .linkedFramework("AppKit"),
                .linkedFramework("AVFoundation"),
                .linkedFramework("CoreMedia"),
                .linkedFramework("CoreVideo"),
            ]
        ),
    ]
}
