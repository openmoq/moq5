import Foundation

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

/// One media object polled from the receiver backend with EXCLUSIVE
/// ownership (the C `moq_media_object_t` on MOQ_OK): `storage` owns the
/// transferred buffers and is released exactly once by ``MediaObject``'s
/// deinit. Metadata is copied at poll time; `handleID` resolves to the S3
/// `MediaTrack`.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package struct ReceiverPolledObject: Sendable {
    package var handleID: UInt64
    package var storage: any OwnedObjectStorage
    package var isKeyframe: Bool
    package var endsGroup: Bool
    package var isDatagram: Bool
    package var presentationTime: Duration
    package var decodeTime: Duration
    package var compositionOffset: Duration
    package var captureTime: Date?

    package init(handleID: UInt64, storage: any OwnedObjectStorage,
                 isKeyframe: Bool, endsGroup: Bool, isDatagram: Bool,
                 presentationTime: Duration, decodeTime: Duration,
                 compositionOffset: Duration, captureTime: Date?) {
        self.handleID = handleID
        self.storage = storage
        self.isKeyframe = isKeyframe
        self.endsGroup = endsGroup
        self.isDatagram = isDatagram
        self.presentationTime = presentationTime
        self.decodeTime = decodeTime
        self.compositionOffset = compositionOffset
        self.captureTime = captureTime
    }
}

/// One `pollObject` outcome (the C `moq_media_receiver_poll_object`
/// vocabulary). Unlike the track-event poll, this IS latch-gated: the C
/// side checks the endpoint latch before inspecting the queue, so
/// `.interrupted` wins even over queued objects.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package enum ReceiverObjectPollResult: Sendable {
    case object(ReceiverPolledObject)   /* MOQ_OK: ownership transferred */
    case none                           /* MOQ_DONE */
    case interrupted                    /* MOQ_ERR_INTERRUPTED (latch) */
    case closed                         /* MOQ_ERR_CLOSED (empty AND terminal) */
}

/// One subscribe/unsubscribe command outcome (asynchronous commands in C:
/// ok means validated-and-recorded, not peer-accepted).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package enum ReceiverCommandResult: Sendable {
    case ok
    case invalidArgument
    case wrongState                     /* track already ended */
    case closed
    case unsupported                    /* MSF namespace-override tracks */
}

/// Receiver-side backing. SAP/timeline record polling lands later.
///
/// Any-thread C snapshots: `isFatal`/`fatalCode` (composing the receiver's
/// own fatal codes with the endpoint's), `isTerminal` (closed OR fatal --
/// the object waiter's level term), `hasQueuedObjects` (stats under the C
/// receiver lock). Service-thread only: `pollTrackEvent`, `pollObject`,
/// `subscribe`, `unsubscribe`, and `detach` (the C receiver destroy, called
/// exactly once, after which NO method may be called; queued undelivered
/// objects die with it, like the C destroy).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package protocol ReceiverBackend: AnyObject, Sendable {
    var isFatal: Bool { get }
    var fatalCode: UInt64 { get }
    var isTerminal: Bool { get }
    var hasQueuedObjects: Bool { get }
    func pollTrackEvent() -> ReceiverPollResult
    func pollObject() -> ReceiverObjectPollResult
    func subscribe(handleID: UInt64, start: StartMode,
                   priority: UInt8?) -> ReceiverCommandResult
    func unsubscribe(handleID: UInt64) -> ReceiverCommandResult
    func detach()
}

/// One write/endTrack outcome (the C media_sender vocabulary). For write,
/// `.ok` means ownership transferred; every other case transfers nothing.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package enum SenderOperationResult: Sendable {
    case ok
    case wouldBlock        /* bounded queue full / no anchor; retryable */
    case invalidArgument
    case wrongState        /* ended/removed/VOD track, or after complete */
    case interrupted       /* the endpoint latch (C checks it in write) */
    case closed            /* terminal; discriminate via isFatal/fatalCode */
}

/// One addTrack outcome.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package enum SenderAddTrackResult: Sendable {
    case track(handleID: UInt64)
    case invalidArgument   /* duplicate name, malformed cfg, bad CP refs */
    case wouldBlock        /* post-ready add vs a name still tearing down */
    case wrongState        /* after complete() */
    case closed
    case outOfMemory
}

/// One bounded sender wait's outcome (the S0 `moq_media_sender_wait`
/// vocabulary; its level is ready AND active-queue headroom, and the latch,
/// then terminal state, WIN over the level).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package enum SenderWaitOutcome: Sendable {
    case activity          /* MOQ_OK: level holds OR the endpoint
                              woke -- attempt (another) write now; the
                              level is NOT guaranteed */
    case timeout           /* MOQ_DONE */
    case interrupted
    case closed
}

/// Sender-side backing. Subscriber/demand events, remove/convert/complete,
/// and stats land in a later slice.
///
/// Any-thread C snapshots: `isReady` (announced + catalog published, NOT
/// demand), `isTerminal` (closed OR fatal), `isFatal`/`fatalCode` (the
/// terminal discriminator -- C collapses fatal and clean terminal into
/// MOQ_ERR_CLOSED). Service-thread only: `addTrack`, `write`, `endTrack`,
/// `wait` (BLOCKING for at most the given slice -- legal on the service
/// thread for the same reason the endpoint drain is: while a job runs, the
/// loop is not in the endpoint wait, so there is exactly one C waiter),
/// and `detach` (the C sender destroy; called exactly once, nothing after).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package protocol SenderBackend: AnyObject, Sendable {
    var isReady: Bool { get }
    var isTerminal: Bool { get }
    var isFatal: Bool { get }
    var fatalCode: UInt64 { get }
    func addTrack(_ configuration: TrackConfiguration) -> SenderAddTrackResult
    func write(handleID: UInt64, object: OutgoingObject) -> SenderOperationResult
    func endTrack(handleID: UInt64) -> SenderOperationResult
    func wait(timeoutMicroseconds: UInt64) -> SenderWaitOutcome
    func detach()
}

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
