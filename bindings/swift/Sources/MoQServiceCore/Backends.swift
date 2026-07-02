/// Package-internal backend seams.
///
/// The public model types are written against these protocols so the whole
/// async surface is unit-testable with scripted implementations — no relay,
/// no network, no wall-clock. The real implementations (over the C service
/// tier via the CMoQService system module) land with the bridge slice; the
/// scripted ones live in MoQServiceCoreTests.
///
/// These are implementation seams, not public API: everything here is
/// `package`-scoped on purpose.

/// The endpoint's confinement/backing: connection state, the interrupt
/// latch, and the pump nudge. (S2 grows the job queue + wait loop.)
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package protocol EndpointBackend: AnyObject, Sendable {
    var state: MoQEndpoint.State { get }
    var negotiatedVersion: MoQVersion? { get }
    func setInterrupted(_ interrupted: Bool)
    func wake()
}

/// Receiver-side backing: track discovery, object polling, subscription
/// control. Filled in by the engine slice (S2) — declared now so the model
/// layer's shape is fixed.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package protocol ReceiverBackend: AnyObject, Sendable {}

/// Sender-side backing: readiness, write, track lifecycle. Filled in by the
/// engine slice (S2) — declared now so the model layer's shape is fixed.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package protocol SenderBackend: AnyObject, Sendable {}

/// Exclusive ownership of one received object's backing storage (the C
/// tier's transferred rcbufs, or a test mock). ``MediaObject`` releases it
/// exactly once from `deinit`.
///
/// Sendable is part of the contract because ``MediaObject`` is Sendable and
/// stores this: every implementation must state its own concurrency story.
/// The C storage will be `@unchecked Sendable` on the exclusive-ownership +
/// thread-safe-allocator argument (bytes immutable, the only refcount
/// mutation is the exactly-once cleanup); mocks must lock their counters.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package protocol OwnedObjectStorage: AnyObject, Sendable {
    var mediaByteCount: Int { get }
    var fragmentByteCount: Int { get }
    func withMediaBytes<R>(_ body: (UnsafeRawBufferPointer) throws -> R) rethrows -> R
    func withFragmentBytes<R>(_ body: (UnsafeRawBufferPointer) throws -> R) rethrows -> R
    /// Release the backing storage. Called exactly once, from any single
    /// thread (exclusive ownership + thread-safe allocator make this legal).
    func cleanup()
}
