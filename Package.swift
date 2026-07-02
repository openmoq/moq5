// swift-tools-version: 6.0

import PackageDescription
import Foundation

let enableTransport = ProcessInfo.processInfo.environment["MOQ_TRANSPORT"] == "1"

// NOTE: no package-wide `platforms:` floor. The MoQService model layer uses
// Swift Duration (macOS 13 / iOS 16), but that requirement is contained to
// the MoQServiceCore target via per-declaration @available — raising the
// floor for the shipping MoQ/MoQMedia products is a product decision deferred
// to the MoQService product slice.
let package = Package(
    name: "MOQ5",
    products: [
        .library(name: "MoQ", targets: ["MoQ"]),
        .library(name: "MoQMedia", targets: ["MoQMedia"]),
        // C-free MSF catalog types: importable beside the (future, installed-
        // mode) MoQService product without pulling the from-source C stack in.
        .library(name: "MoQCatalog", targets: ["MoQCatalog"]),
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

        // Pure-Swift MSF catalog (no C dependencies). Split out of MoQMedia so
        // catalog authoring/parsing stays available to apps that must NOT link
        // the from-source C targets (see the MoQService plan: the installed-
        // mode service product is binary-exclusive with the source-built
        // stack). MoQMedia re-exports it for source compatibility.
        .target(
            name: "MoQCatalog",
            path: "bindings/swift/Sources/MoQCatalog"
        ),

        // Swift media layer.
        .target(
            name: "MoQMedia",
            dependencies: ["MoQ", "MoQCatalog", "CMoQMediaObject", "CMoQLOC", "CMoQCMAF", "CMoQPlayback"],
            path: "bindings/swift/Sources/MoQMedia"
        ),

        // Pure-Swift model layer for the (future) MoQService product: public
        // endpoint/receiver/sender/track/object types, configurations, errors,
        // and the package-internal backend seams. Always built; the C bridge
        // (CMoQService system module + real backends) lands in a later slice
        // behind an env gate, alongside the moq_swift_stack_guard collision
        // canary + negative link test (TODO: guard belongs to that slice --
        // there is no MoQService product to collide with yet).
        .target(
            name: "MoQServiceCore",
            path: "bindings/swift/Sources/MoQServiceCore"
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
        .testTarget(
            name: "MoQCatalogTests",
            dependencies: ["MoQCatalog"],
            path: "bindings/swift/Tests/MoQCatalogTests"
        ),
        .testTarget(
            name: "MoQServiceCoreTests",
            dependencies: ["MoQServiceCore"],
            path: "bindings/swift/Tests/MoQServiceCoreTests"
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
