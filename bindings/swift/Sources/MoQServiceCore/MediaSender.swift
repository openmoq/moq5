import Foundation

/// Publishes a namespace's catalog and media tracks over an endpoint.
///
/// The write path runs on the endpoint's service thread: drop policies map
/// to the C eviction, `.failFast` surfaces `.wouldBlock` immediately, and
/// `.lossless` suspends in bounded sender-wait slices (Task-cancellable,
/// never C BLOCK_TIMEOUT). ``endTrack(_:)`` returns once the reliable
/// END_OF_TRACK marker is queued. Subscriber/demand events, removeTrack,
/// convertToVOD, complete, and stats land in a later slice.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public final class MediaSender: @unchecked Sendable {

    // MARK: Configuration

    public struct Configuration: Sendable {
        /// What `write` does when the send queue is full. Drop policies map
        /// to the matching C eviction; `lossless`/`failFast` run over the C
        /// fail-fast mode with the retry loop in Swift (Task-cancellable).
        /// No default — the choice is forced, exactly like the C
        /// configuration.
        public enum SendPolicy: Sendable {
            /// Live: evict whole groups, always keep the newest keyframe
            /// suffix (C DROP_TO_KEYFRAME).
            case dropToKeyframe
            /// Evict whole groups from the oldest end (C DROP_GROUP).
            case dropGroup
            /// Never drop: `write` suspends until space or the deadline,
            /// then throws ``MoQServiceError/wouldBlock``.
            case lossless(timeout: Duration = .seconds(1))
            /// Throw ``MoQServiceError/wouldBlock`` immediately when full.
            case failFast
        }

        public struct QueueLimits: Sendable, Hashable {
            public var maxObjects: Int?
            public var maxBytes: Int?
            public init(maxObjects: Int? = nil, maxBytes: Int? = nil) {
                self.maxObjects = maxObjects
                self.maxBytes = maxBytes
            }
        }

        public var namespace: MediaNamespace
        public var sendPolicy: SendPolicy
        public var queueLimits: QueueLimits?
        /// Strict CMAF validation of written fragments (default true, like
        /// the C tier); disable only for known-good passthrough.
        public var validateCMAF: Bool
        /// Catalog track name; `nil` uses the MSF default ("catalog").
        public var catalogTrack: String?

        public init(namespace: MediaNamespace, sendPolicy: SendPolicy) {
            self.namespace = namespace
            self.sendPolicy = sendPolicy
            self.queueLimits = nil
            self.validateCMAF = true
            self.catalogTrack = nil
        }

        /// The live preset (mirrors the C `cfg_init_live`): drop-to-keyframe.
        public static func live(namespace: MediaNamespace) -> Configuration {
            Configuration(namespace: namespace, sendPolicy: .dropToKeyframe)
        }

        /// The lossless preset (mirrors the C `cfg_init_lossless`): suspend
        /// on backpressure with a bounded deadline.
        public static func lossless(namespace: MediaNamespace) -> Configuration {
            Configuration(namespace: namespace, sendPolicy: .lossless())
        }

        package func validate() throws {
            try namespace.validate()
            if case .lossless(let timeout) = sendPolicy, timeout <= .zero {
                throw MoQServiceError.invalidArgument(
                    "lossless timeout must be positive")
            }
            if let limits = queueLimits {
                if let o = limits.maxObjects, o <= 0 {
                    throw MoQServiceError.invalidArgument(
                        "queue maxObjects must be positive (nil = library default)")
                }
                if let b = limits.maxBytes, b <= 0 {
                    throw MoQServiceError.invalidArgument(
                        "queue maxBytes must be positive (nil = library default)")
                }
            }
        }
    }

    public let endpoint: MoQEndpoint
    public let configuration: Configuration
    /// The engine's hook closures capture THIS object -- never the sender
    /// or the endpoint -- so no retain cycle runs engine -> sender ->
    /// endpoint -> engine, and both deinit backstops stay reachable.
    package let attachment: SenderAttachment
    package var backend: any SenderBackend { attachment.backend }

    package init(endpoint: MoQEndpoint,
                 configuration: Configuration,
                 backend: any SenderBackend) {
        self.endpoint = endpoint
        self.configuration = configuration
        self.attachment = SenderAttachment(backend: backend)
    }

    deinit {
        /* Best-effort backstop: the app dropped the sender without
         * close(). Remove the hook ONLY when the detach job was accepted;
         * once engine shutdown has begun the still-installed hook makes
         * the shutdown teardown own the detach. */
        guard !attachment.state.isDetached else { return }
        let attachment = self.attachment
        let engine = endpoint.engine
        if engine.post({ attachment.finalizeDetach() }) {
            engine.removePumpHook(kind: .sender)
        }
    }

    // MARK: Lifecycle

    /// Attach to an existing endpoint (one sender per endpoint; may share
    /// the endpoint with one ``MediaReceiver``).
    public static func attach(to endpoint: MoQEndpoint,
                              configuration: Configuration) throws -> MediaSender {
        try configuration.validate()
        throw MoQServiceError.unsupported   // TODO(S6): real backend factory
    }

    /// The wiring the public attach uses once a backend exists (S6); tests
    /// drive it with scripted backends. Throws `.wrongState` when the
    /// endpoint already has a sender, `.closed` once it is shutting down.
    package static func attach(to endpoint: MoQEndpoint,
                               configuration: Configuration,
                               backend: any SenderBackend) throws -> MediaSender {
        try configuration.validate()
        let sender = MediaSender(
            endpoint: endpoint, configuration: configuration, backend: backend)
        let attachment = sender.attachment      /* NOT the sender: no cycle */
        try endpoint.engine.installPumpHook(
            kind: .sender,
            run: { /* S5b: drain subscriber/demand events here */ },
            teardown: { attachment.finalizeDetach() })
        return sender
    }

    /// Detach from the endpoint and release the attachment slot.
    /// Idempotent; safe concurrently with `endpoint.close()`;
    /// cancellation-immune (a cancelled caller still detaches).
    public func close() async {
        let attachment = self.attachment
        let engine = endpoint.engine
        await engine.postAndWait {
            attachment.finalizeDetach()
            engine.removePumpHook(kind: .sender)
        }
    }

    // MARK: Readiness

    /// True once the namespace is accepted AND the catalog is published --
    /// the publish path accepts media. NOT demand: a ready sender may have
    /// zero subscribers. False after close.
    public var isReady: Bool { attachment.isReadySnapshot }

    /// Suspends until ready (see ``isReady``). Terminal state beats the
    /// level: a failed sender throws ``MoQServiceError/fatal(code:)`` /
    /// ``MoQServiceError/closed`` even if the ready flag still reads true;
    /// the endpoint latch throws ``MoQServiceError/interrupted``.
    public func ready() async throws {
        let attachment = self.attachment
        do {
            try await endpoint.engine.waitUntil {
                if attachment.state.isDetached {
                    return .failure(attachment.recordedTerminalError())
                }
                /* Terminal beats level (the S0 sender-wait priority). */
                if attachment.backend.isTerminal {
                    return .failure(attachment.terminalError())
                }
                return attachment.backend.isReady ? .satisfied : .pending
            }
        } catch let error as MoQServiceError where error == .closed {
            throw attachment.recordedTerminalError()
        }
    }

    // MARK: Tracks

    /// Add a track (legal before and after readiness; a post-ready add
    /// republishes the catalog). Throws `.wouldBlock` for a post-ready add
    /// whose name is still tearing down (retry later).
    public func addTrack(_ trackConfiguration: TrackConfiguration) async throws -> MediaTrack {
        try trackConfiguration.validate()
        let attachment = self.attachment
        return try await performGuarded { [attachment] in
            switch attachment.backend.addTrack(trackConfiguration) {
            case .track(let handleID):
                return attachment.registerTrack(handleID: handleID,
                                                configuration: trackConfiguration)
            case .invalidArgument:
                throw MoQServiceError.invalidArgument(
                    "addTrack: the configuration was refused (duplicate name?)")
            case .wouldBlock:
                throw MoQServiceError.wouldBlock
            case .wrongState:
                throw MoQServiceError.wrongState
            case .closed:
                throw attachment.terminalError()
            case .outOfMemory:
                throw MoQServiceError.outOfMemory
            }
        }
    }

    /// Submit one object in decode order. Transfer-on-success: on return
    /// the payload has been handed to the service; on ANY thrown error
    /// nothing was consumed. Pre-ready writes queue into the bounded
    /// pre-ready buffer. Under `.lossless`, a full queue suspends and
    /// retries on readiness/queue activity -- pre-ready backpressure waits
    /// on READINESS arriving, not merely on space -- and throws
    /// ``MoQServiceError/wouldBlock`` when the timeout budget elapses
    /// first. `.failFast` (and the drop policies' no-anchor edge cases)
    /// throw `.wouldBlock` immediately.
    public func write(_ object: OutgoingObject, to track: MediaTrack) async throws {
        guard let handleID = attachment.handleID(for: track) else {
            throw MoQServiceError.invalidArgument(
                "write: the track does not belong to this sender")
        }
        if case .lossless(let timeout) = configuration.sendPolicy {
            try await losslessWrite(object, handleID: handleID,
                                    budget: timeout.wholeMicroseconds)
            return
        }
        let attachment = self.attachment
        try await performGuarded { [attachment] in
            try attachment.mapOperation(
                attachment.backend.write(handleID: handleID, object: object))
        }
    }

    /// End a finite track: returns once the reliable END_OF_TRACK marker is
    /// QUEUED (a momentarily full queue is retried internally, independent
    /// of the send policy); the service then drains it after the track's
    /// queued objects. Idempotent; does NOT end other tracks or the
    /// endpoint. Cancellation between retries leaves the marker unqueued --
    /// call again. Writes after end throw ``MoQServiceError/wrongState``.
    public func endTrack(_ track: MediaTrack) async throws {
        guard let handleID = attachment.handleID(for: track) else {
            throw MoQServiceError.invalidArgument(
                "endTrack: the track does not belong to this sender")
        }
        let attachment = self.attachment
        while true {
            let queued = try await performGuarded { [attachment] () -> Bool in
                switch attachment.backend.endTrack(handleID: handleID) {
                case .ok:
                    return true
                case .wouldBlock:
                    /* One bounded slice inside this job, then re-post so
                     * co-attached work interleaves between retries. */
                    switch attachment.backend.wait(
                        timeoutMicroseconds: EndpointEngine.drainSliceMicroseconds) {
                    case .activity, .timeout:
                        return false
                    case .interrupted:
                        throw MoQServiceError.interrupted
                    case .closed:
                        throw attachment.terminalError()
                    }
                case .invalidArgument:
                    throw MoQServiceError.invalidArgument("endTrack: invalid track")
                case .wrongState:
                    throw MoQServiceError.wrongState
                case .interrupted:
                    throw MoQServiceError.interrupted
                case .closed:
                    throw attachment.terminalError()
                }
            }
            if queued { return }
        }
    }

    // MARK: Internals

    /// The `.lossless` suspend-retry loop: each service-thread round is one
    /// write attempt plus, on wouldBlock, ONE bounded sender-wait slice
    /// (the drain precedent -- while the job runs, the loop is not in the
    /// endpoint wait, so the single-C-waiter rule holds). Budget accounting
    /// is in REQUESTED slices, deterministically; after the last slice one
    /// final attempt runs before `.wouldBlock` surfaces.
    private func losslessWrite(_ object: OutgoingObject, handleID: UInt64,
                               budget: UInt64) async throws {
        let attachment = self.attachment
        var remaining = budget
        while true {
            let finalAttempt = remaining == 0
            let slice = min(remaining, EndpointEngine.drainSliceMicroseconds)
            let delivered = try await performGuarded { [attachment] () -> Bool in
                switch attachment.backend.write(handleID: handleID, object: object) {
                case .ok:
                    return true
                case .wouldBlock:
                    if finalAttempt { throw MoQServiceError.wouldBlock }
                    switch attachment.backend.wait(timeoutMicroseconds: slice) {
                    case .activity, .timeout:
                        return false            /* retry next round */
                    case .interrupted:
                        throw MoQServiceError.interrupted
                    case .closed:
                        throw attachment.terminalError()
                    }
                case .invalidArgument:
                    throw MoQServiceError.invalidArgument("write: invalid object")
                case .wrongState:
                    throw MoQServiceError.wrongState
                case .interrupted:
                    throw MoQServiceError.interrupted
                case .closed:
                    throw attachment.terminalError()
                }
            }
            if delivered { return }
            remaining -= min(remaining, EndpointEngine.drainSliceMicroseconds)
        }
    }

    /// A service-thread hop with the shared guards: detached senders throw
    /// their recorded terminal error BEFORE any backend call; an engine
    /// that is already gone maps the same way.
    private func performGuarded<T: Sendable>(
        _ body: @escaping @Sendable () throws -> T
    ) async throws -> T {
        let attachment = self.attachment
        do {
            return try await endpoint.engine.perform {
                guard !attachment.state.isDetached else {
                    throw attachment.recordedTerminalError()
                }
                return try body()
            }
        } catch let error as MoQServiceError where error == .closed {
            throw attachment.recordedTerminalError()
        }
    }
}

// MARK: - The engine-facing attachment

/// The sender's engine-facing half: the backend plus all state the engine
/// hook and jobs touch. References NEITHER the sender NOR the endpoint
/// (no retain cycle through the hook slot).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package final class SenderAttachment: @unchecked Sendable {

    package let backend: any SenderBackend
    package let state = SenderState()

    init(backend: any SenderBackend) {
        self.backend = backend
    }

    /// Terminal discrimination (the ONLY path from a `.closed`-class C
    /// result to a thrown error): C collapses fatal and clean terminal
    /// into MOQ_ERR_CLOSED; isFatal/fatalCode discriminates.
    package func terminalError() -> MoQServiceError {
        backend.isFatal ? .fatal(code: backend.fatalCode) : .closed
    }

    /// Like ``terminalError()`` but safe AFTER detach: answers from the
    /// snapshot recorded at detach time (the backend is gone).
    package func recordedTerminalError() -> MoQServiceError {
        if let fatal = state.recordedFatal {
            return .fatal(code: fatal)
        }
        return .closed
    }

    package var isReadySnapshot: Bool {
        /* Under the state lock: detach flips the flag under the same lock
         * BEFORE destroying the backend, so this read can never race a
         * teardown into a freed backend. */
        state.ifLive { backend.isReady } ?? false
    }

    /// Exactly-once detach (service thread): first caller wins. The fatal
    /// snapshot is taken INSIDE the winner's locked region -- a losing
    /// caller (double close, teardown after close) returns before touching
    /// the backend at all.
    package func finalizeDetach() {
        let won = state.beginDetach { [backend] in
            backend.isFatal ? backend.fatalCode : nil
        }
        guard won else { return }
        backend.detach()
    }

    package func registerTrack(handleID: UInt64,
                               configuration: TrackConfiguration) -> MediaTrack {
        var description = TrackDescription(
            name: configuration.name, mediaType: configuration.mediaType,
            packaging: configuration.packaging)
        description.codec = configuration.codec
        description.bitrate = configuration.bitrate
        description.initData = configuration.initData
        description.timescale = configuration.timescale
        description.isLive = configuration.isLive
        description.width = configuration.width
        description.height = configuration.height
        description.framerateMillis = configuration.framerateMillis
        description.samplerate = configuration.samplerate
        description.channelConfig = configuration.channelConfig
        description.role = configuration.role
        description.language = configuration.language
        description.label = configuration.label
        let track = MediaTrack(description: description)
        state.register(track: track, handleID: handleID)
        return track
    }

    package func handleID(for track: MediaTrack) -> UInt64? {
        state.handleID(for: track)
    }

    /// Map a plain (non-retrying) operation result.
    package func mapOperation(_ result: SenderOperationResult) throws {
        switch result {
        case .ok:
            return
        case .wouldBlock:
            throw MoQServiceError.wouldBlock
        case .invalidArgument:
            throw MoQServiceError.invalidArgument("the object was refused")
        case .wrongState:
            throw MoQServiceError.wrongState
        case .interrupted:
            throw MoQServiceError.interrupted
        case .closed:
            throw terminalError()
        }
    }
}

/// Lock-guarded sender state: track identity maps and teardown flags.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package final class SenderState: @unchecked Sendable {

    private let lock = NSLock()
    private var tracksByID: [UInt64: MediaTrack] = [:]
    private var idsByTrack: [ObjectIdentifier: UInt64] = [:]
    private var detached = false
    private var detachFatalCode: UInt64?

    package var isDetached: Bool {
        lock.lock(); defer { lock.unlock() }
        return detached
    }

    /// The fatal code snapshotted at detach, if the sender was fatal then.
    package var recordedFatal: UInt64? {
        lock.lock(); defer { lock.unlock() }
        return detachFatalCode
    }

    /// True exactly once. The winner's `snapshot` closure runs UNDER the
    /// lock, before `detached` becomes observable -- so it may read the
    /// still-live backend, and no other locked path can interleave a read
    /// after the detach begins. Losers never run it.
    package func beginDetach(snapshot: () -> UInt64?) -> Bool {
        lock.lock(); defer { lock.unlock() }
        if detached { return false }
        detachFatalCode = snapshot()
        detached = true
        return true
    }

    /// Run `body` under the lock only while not detached (nil afterward) --
    /// the race-free way to read backend snapshots from any thread.
    package func ifLive<T>(_ body: () -> T) -> T? {
        lock.lock(); defer { lock.unlock() }
        return detached ? nil : body()
    }

    package func register(track: MediaTrack, handleID: UInt64) {
        lock.lock()
        tracksByID[handleID] = track
        idsByTrack[ObjectIdentifier(track)] = handleID
        lock.unlock()
    }

    package func handleID(for track: MediaTrack) -> UInt64? {
        lock.lock(); defer { lock.unlock() }
        return idsByTrack[ObjectIdentifier(track)]
    }
}
