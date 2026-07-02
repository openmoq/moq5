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

/// What one blocking activity wait observed (the C `moq_endpoint_wait` /
/// receiver-wait / sender-wait return vocabulary: MOQ_OK / MOQ_DONE /
/// MOQ_ERR_INTERRUPTED / MOQ_ERR_CLOSED).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package enum EndpointWaitResult: Sendable, Hashable {
    case activity, timeout, interrupted, closed
}

/// One drain call's outcome (the C `moq_endpoint_drain` vocabulary; the
/// wrong-thread state cannot occur -- the engine never calls from the C
/// network thread).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package enum EndpointDrainResult: Sendable, Hashable {
    case complete, timeout, interrupted, closed, unsupported
}

/// The endpoint's transport backing. Two call classes with different rules:
///
/// Any-thread (C-snapshot semantics): `state`, `negotiatedVersion`,
/// `isFatal`, `fatalCode`, `setInterrupted`, `wake`. `wake` is the C pump
/// nudge: coalesced, LEVEL-retained (a wake landing before the next wait is
/// not lost), non-allocating, harmless after stop.
///
/// Service-thread only, never concurrent (the engine is the sole caller):
/// `waitForActivity` (the ONE blocking C wait; which C wait it maps to --
/// endpoint, receiver, or sender -- is this backend's choice), `drain`,
/// `stop` (joins the C network thread), `destroy`.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package protocol EndpointBackend: AnyObject, Sendable {
    var state: MoQEndpoint.State { get }
    var negotiatedVersion: MoQVersion? { get }
    var isFatal: Bool { get }
    var fatalCode: UInt64 { get }
    func setInterrupted(_ interrupted: Bool)
    func wake()
    func waitForActivity(timeoutMicroseconds: UInt64) -> EndpointWaitResult
    func drain(timeoutMicroseconds: UInt64) -> EndpointDrainResult
    func stop()
    func destroy()
}

/// One track event polled from the receiver backend (the C
/// `moq_media_track_event_t`, eagerly copied). `handleID` is the stable
/// track identity: the same C `moq_media_track_t*` (never recycled for the
/// receiver's life) always maps to the same ID, so the Swift side can keep
/// one `MediaTrack` instance per handle. `trackDescription` is a deep copy
/// taken AT poll time on the service thread — no C borrow crosses this seam.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package struct ReceiverPolledEvent: Sendable {
    package enum Kind: Sendable {
        case added, updated, removed, ended, catalogReady
    }
    package var kind: Kind
    package var handleID: UInt64
    package var trackDescription: TrackDescription?

    package init(kind: Kind, handleID: UInt64,
                 trackDescription: TrackDescription?) {
        self.kind = kind
        self.handleID = handleID
        self.trackDescription = trackDescription
    }
}

/// One `pollTrackEvent` outcome (the C `moq_media_receiver_poll_track`
/// vocabulary: MOQ_OK / MOQ_DONE / MOQ_ERR_CLOSED-when-empty-and-terminal).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package enum ReceiverPollResult: Sendable {
    case event(ReceiverPolledEvent)
    case none
    case closed
}

/// Receiver-side backing: track-event polling and detach. Object/SAP/
/// timeline polling and subscription control land with the objects slice.
///
/// `isFatal`/`fatalCode` are any-thread C snapshots (composing the
/// receiver's own fatal codes with the endpoint's, like
/// `moq_media_receiver_fatal_code`). `pollTrackEvent` and `detach` are
/// service-thread only; `detach` (the C receiver destroy) is called exactly
/// once, after which NO method may be called.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package protocol ReceiverBackend: AnyObject, Sendable {
    var isFatal: Bool { get }
    var fatalCode: UInt64 { get }
    func pollTrackEvent() -> ReceiverPollResult
    func detach()
}

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
