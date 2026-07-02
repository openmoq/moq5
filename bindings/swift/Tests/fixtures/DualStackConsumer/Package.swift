// swift-tools-version: 6.0

// NEGATIVE fixture: one binary importing BOTH the source-built MoQ stack
// and the installed-mode MoQService. This package must FAIL to link with
// `duplicate symbol 'moq_swift_stack_guard'` -- that failure is the
// binary-exclusivity canary working. Driven by
// scripts/check_swift_stack_guard.sh; never built by the normal lanes.
import PackageDescription

let package = Package(
    name: "DualStackConsumer",
    dependencies: [
        .package(name: "MOQ5", path: "../../../../.."),
    ],
    targets: [
        .executableTarget(
            name: "dual-stack-consumer",
            dependencies: [
                .product(name: "MoQ", package: "MOQ5"),
                .product(name: "MoQService", package: "MOQ5"),
            ]
        ),
    ]
)
