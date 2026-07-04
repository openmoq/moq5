// swift-tools-version: 6.0

// POSITIVE fixture: a cold consumer importing ONLY MoQService. This package
// must BUILD (the packaging smoke); it is never run. Driven by the gated CI
// lane with MOQ_SERVICE=1 and an installed libmoq-service prefix.
import PackageDescription

let package = Package(
    name: "ServiceConsumer",
    // Match the MoQService availability floor (Swift Duration: macOS 13 / iOS 16).
    // The iOS floor also aligns the link deployment target with the xcframework
    // objects (built at iOS 16), clearing the "built for newer version" ld noise.
    platforms: [.macOS(.v13), .iOS(.v16)],
    dependencies: [
        .package(name: "MOQ5", path: "../../../../.."),
    ],
    targets: [
        .executableTarget(
            name: "service-consumer",
            dependencies: [
                .product(name: "MoQService", package: "MOQ5"),
            ]
        ),
    ]
)
