// swift-tools-version: 6.0

import PackageDescription
import Foundation

let enableTransport = ProcessInfo.processInfo.environment["MOQ_TRANSPORT"] == "1"
let enableService = ProcessInfo.processInfo.environment["MOQ_SERVICE"] == "1"

// NOTE: no package-wide `platforms:` floor. The MoQService model layer uses
// Swift Duration (macOS 13 / iOS 16), but that requirement is contained to
// the MoQServiceCore target via per-declaration @available — raising the
// floor for the shipping MoQ/MoQMedia products is a product decision this
// package does not take.
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
            dependencies: ["CMoQCore", "MoQTransportModel"],
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
            dependencies: ["MoQ", "MoQCatalog", "MoQTransportModel", "CMoQMediaObject", "CMoQLOC", "CMoQCMAF", "CMoQPlayback"],
            path: "bindings/swift/Sources/MoQMedia"
        ),

        // The single transport-version nominal type, shared by the service
        // and media layers through their own aliases. Deliberately
        // dependency-free: it is a protocol model, not a binding, so neither
        // layer has to depend on the other to name a negotiated draft.
        //
        // Target only, NOT a package product: it exists so the two layers can
        // share one type, and an application has no reason to depend on a
        // module containing only that. The public spellings reach consumers
        // through MoQServiceCore's `MoQVersion` and MoQMedia's
        // `MediaTransportVersion` aliases.
        .target(
            name: "MoQTransportModel",
            path: "bindings/swift/Sources/MoQTransportModel"
        ),

        // Pure-Swift model layer for the MoQService product: public
        // endpoint/receiver/sender/track/object types, configurations, errors,
        // and the package-internal backend seams. Always built; the C bridge
        // (CMoQService system module + real backends) is built only behind the
        // MOQ_SERVICE env gate, where the moq_swift_stack_guard collision
        // canary applies (see below).
        .target(
            name: "MoQServiceCore",
            dependencies: ["MoQTransportModel"],
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
            name: "MoQRecvArgsTests",
            dependencies: ["MoQRecvArgs"],
            path: "bindings/swift/Tests/MoQRecvArgsTests"
        ),
        // Negotiated-profile corpus: an INDEPENDENT Swift reader of the same
        // vectors file the C and C++ readers read. Test wiring only -- no
        // product module, dependency or platform change.
        //
        // It resolves tests/vectors/negotiated_profile.vectors from #filePath
        // rather than from a bundle resource, because a bundle resource cannot
        // reach it: SwiftPM refuses a resource path outside the target
        // directory, and a symlink inside the target is copied VERBATIM into
        // the bundle, where its relative target no longer resolves (verified:
        // the bundle contained a dangling link). The alternatives were a
        // checked-in copy of the corpus (forbidden) or a build plugin (a
        // package change beyond test wiring).
        .testTarget(
            name: "NegotiatedProfileTests",
            path: "tests/swift/NegotiatedProfileTests"
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

// SwiftPM links every test target into ONE unified test bundle, and the
// moq_swift_stack_guard canary (correctly) refuses a binary containing both
// the source-built MoQ stack and MoQService. The MoQ-linking test targets
// therefore exist only in ungated builds; the MOQ_SERVICE=1 lane runs the
// service + C-free suites, complementing the ungated lane that runs these.
if !enableService {
    package.targets += [
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
        // Proves the service and media spellings of the negotiated
        // transport version denote ONE nominal type. It is the only test
        // target that imports both layers, and it exists for that reason.
        .testTarget(
            name: "TransportVersionAliasTests",
            dependencies: ["MoQMedia", "MoQServiceCore", "MoQTransportModel"],
            path: "bindings/swift/Tests/TransportVersionAliasTests"
        ),
    ]
}

// The installed-mode MoQService product requires a CMake-installed service
// tier with a pkg-config file: the picoquic-family backends and the
// wtquic-Network backend qualify (its chain is libmoq-service ->
// libmoq-wtquic-network-managed -> wtquic-network); an mvfst/proxygen/
// MsQuic-enabled service install has no pkg-config file by design.
// Build with: MOQ_SERVICE=1 PKG_CONFIG_PATH=<prefix>/lib/pkgconfig swift build
//
// BINARY EXCLUSIVITY: one app binary links EITHER MoQService (installed
// libmoq archives) OR the source-built MoQ/MoQMedia stack, never both --
// enforced by the moq_swift_stack_guard duplicate-symbol canary
// (scripts/check_swift_stack_guard.sh).
// iOS consumes MoQService from prebuilt xcframeworks (scripts/build_ios_*.sh)
// instead of pkg-config: `import CMoQService` resolves from the LibMoQ.xcframework
// module map (module name is independent of the SwiftPM binary-target name), and
// OpenSSL.xcframework supplies the separately-linked libssl/libcrypto. This lane
// is opt-in via MOQ_SERVICE_IOS=1 (an iOS build) so a macOS `MOQ_SERVICE=1` build
// keeps the pkg-config path even if the xcframeworks happen to be built locally.
// See docs/ios-packaging.md.
let iosBinaryService = ProcessInfo.processInfo.environment["MOQ_SERVICE_IOS"] == "1"
// Apple-only framework linkage shared by BOTH MoQService definitions:
// SwiftPM resolves pkg-config without --static, so an NF-enabled service
// install's Libs.private frameworks (wtquic-network: Network/Security)
// never reach the link, and the iOS xcframework will carry the backend
// too. Conditioned on Apple platforms -- a Linux MOQ_SERVICE=1 build must
// not attempt -framework linkage (the Network backend cannot exist there).
let moqServiceLinkerSettings: [LinkerSetting] = [
    .linkedFramework("Network",
                     .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
    .linkedFramework("Security",
                     .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
    // wtquic declares Network, Security, AND Foundation as public
    // framework dependencies of wtq::network; link all three explicitly
    // rather than riding whatever Foundation linkage Swift happens to
    // add for its own runtime.
    .linkedFramework("Foundation",
                     .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
]
if enableService {
    package.products += [
        .library(name: "MoQService", targets: ["MoQService"]),
    ]
    if iosBinaryService {
        // .binaryTarget paths MUST be relative to the package root. Default to
        // the build scripts' output dirs; an alternate location (still relative
        // to the root) may be given via MOQ_SERVICE_IOS_XCFRAMEWORK_DIR.
        let xcDir = ProcessInfo.processInfo.environment["MOQ_SERVICE_IOS_XCFRAMEWORK_DIR"]
        let libmoqXC = xcDir.map { "\($0)/LibMoQ.xcframework" } ?? "build-ios/dist/LibMoQ.xcframework"
        let opensslXC = xcDir.map { "\($0)/OpenSSL.xcframework" } ?? "build-ios-openssl/dist/OpenSSL.xcframework"
        package.targets += [
            .binaryTarget(name: "LibMoQBinary", path: libmoqXC),
            .binaryTarget(name: "OpenSSLBinary", path: opensslXC),
            .target(
                name: "MoQService",
                // LibMoQBinary vends module CMoQService; OpenSSLBinary is the
                // separate libssl/libcrypto link (LibMoQ.xcframework holds
                // libmoq/picoquic/picotls objects but not OpenSSL).
                dependencies: ["MoQServiceCore", "LibMoQBinary", "OpenSSLBinary"],
                path: "bindings/swift/Sources/MoQService",
                linkerSettings: moqServiceLinkerSettings
            ),
        ]
    } else {
        package.targets += [
            .systemLibrary(
                name: "CMoQService",
                path: "bindings/swift/SystemModules/CMoQService",
                pkgConfig: "libmoq-service",
                providers: [.brew(["libmoq"])]
            ),
            .target(
                name: "MoQService",
                dependencies: ["MoQServiceCore", "CMoQService"],
                path: "bindings/swift/Sources/MoQService",
                linkerSettings: moqServiceLinkerSettings
            ),
            .testTarget(
                name: "MoQServiceTests",
                dependencies: ["MoQService", "CMoQService"],
                path: "bindings/swift/Tests/MoQServiceTests"
            ),
            // The product-surface proof: builds in the gated lane; running it
            // needs a real relay (never default CI).
            .executableTarget(
                name: "moq-service-player",
                dependencies: ["MoQService"],
                path: "bindings/swift/Examples/MoQServicePlayer"
            ),
            // Standalone .wtquicNetwork runtime proof (a real process --
            // Network.framework does not establish inside an xctest host).
            // Run via scripts/check_wtquic_network_runtime.sh.
            .executableTarget(
                name: "moq-wtquic-network-runtime-proof",
                dependencies: ["MoQService"],
                path: "bindings/swift/Examples/WtquicNetworkRuntimeProof"
            ),
        ]
        // MOQ-SWIFT-VIDEO-DIMENSION-TRAP security regression: a
        // bounded subprocess that drives the REAL service CoreMedia dimension
        // conversion, so an out-of-range width/height is proven to be
        // range-rejected rather than trapping the parent test process. These import CoreMedia, so they are Apple-host
        // only (`#if os(macOS)`) -- the MoQService product itself stays
        // available on non-Apple service builds; only these test artifacts are
        // gated. Service stack only; never enrolled outside MOQ_SERVICE=1.
        #if os(macOS)
        package.targets += [
            .executableTarget(
                name: "MoQServiceTrapProbe",
                dependencies: ["MoQService", "MoQServiceCore"],
                path: "bindings/swift/Examples/MoQServiceTrapProbe"
            ),
            .testTarget(
                name: "MoQServiceDimensionTests",
                dependencies: ["MoQService", "MoQServiceCore", "MoQServiceTrapProbe"],
                path: "bindings/swift/Tests/MoQServiceDimensionTests"
            ),
        ]
        #endif
    }
}

// Package-internal decoder helpers extracted from the (transport-gated)
// MoQSwiftPlayer example. They import only the source-built MoQ/MoQMedia stack +
// Apple media frameworks. NOT a public product -- no library() entry; `package`
// visibility only. Apple-host-only (`#if os(macOS)`) so the package's default
// non-Apple buildability is preserved.
//
// Stack separation (see the moq_swift_stack_guard note above): the source-stack
// decoder TEST target must never share SwiftPM's unified test bundle with
// MoQServiceTests, or the two stacks collide on `moq_swift_stack_guard`. So:
//   * the library + child probe exist whenever they are needed -- the ordinary
//     (non-service) lane runs the decoder trap probes, and the transport player links the
//     library -- i.e. `!enableService || enableTransport`;
//   * the decoder test target exists ONLY in the non-service lane
//     (`!enableService`), exactly like MoQTests/MoQMediaTests above;
//   * the pure `MOQ_SERVICE=1` lane (service, no transport) enrolls NONE of
//     them, and `MOQ_TRANSPORT=1 MOQ_SERVICE=1` keeps the library so the player
//     never dangles -- while still contributing no source-stack test target.
#if os(macOS)
if !enableService || enableTransport {
    package.targets += [
        .target(
            name: "MoQSwiftPlayerDecoders",
            dependencies: ["MoQ", "MoQMedia"],
            path: "bindings/swift/Examples/MoQSwiftPlayerDecoders",
            linkerSettings: [
                .linkedFramework("AVFoundation"),
                .linkedFramework("CoreMedia"),
                .linkedFramework("CoreVideo"),
                .linkedFramework("AudioToolbox"),
            ]
        ),
        // Bounded subprocess trap probe: runs one decoder case per process so
        // the parent tests classify an abort (where a runtime still traps)
        // without taking the whole `swift test` process down. A standalone
        // source-stack executable -- it never links MoQService.
        .executableTarget(
            name: "MoQPlayerTrapProbe",
            dependencies: ["MoQSwiftPlayerDecoders"],
            path: "bindings/swift/Examples/MoQPlayerTrapProbe"
        ),
    ]
}
if !enableService {
    // Player decoder security regression (MOQ-SWIFTPLAYER-EMPTY-DECODER-POINTER,
    // MOQ-SWIFTPLAYER-AAC-LENGTH-TRAP): valid-vector behavior-neutrality +
    // malformed-input rejection oracles + child-process attribution. Source-
    // stack test, so it is excluded from the MOQ_SERVICE=1 unified test bundle.
    package.targets += [
        .testTarget(
            name: "MoQSwiftPlayerDecoderTests",
            dependencies: ["MoQSwiftPlayerDecoders", "MoQPlayerTrapProbe"],
            path: "bindings/swift/Tests/MoQSwiftPlayerDecoderTests"
        ),
    ]
}
#endif

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
            dependencies: ["MoQ", "MoQMedia", "MoQRecvArgs", "MoQTransport",
                           "MoQSwiftPlayerDecoders"],
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
