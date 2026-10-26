import Foundation

/// Decode-order media for the application's own decoders: the C
/// `moq_media_player` over a media receiver it owns, attached to an
/// endpoint. Selection, decode order, gaps, timelines and track ends are the
/// player's; decoders, renderers and the presentation clock stay with the
/// application, which reports back through the `report*` calls.
///
/// The player occupies the endpoint's receiver slot (one per endpoint, like
/// ``MediaReceiver``). Every backend call runs on the endpoint's service
/// thread: each engine pass ticks the player and copies what it produced
/// into bounded staging, polling the backend only while that staging has
/// room, so a slow consumer backs the player up and, behind it, the
/// receiver.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public final class MediaPlayer: @unchecked Sendable {

    // MARK: Configuration

    public struct Configuration: Sendable {
        public enum Selection: Sendable, Hashable {
            /// Once the catalog is ready, the lowest-numbered render group's
            /// tracks (one per alternate group); every other track is
            /// announced with ``PlayerEvent/trackAvailable(_:)``.
            case automatic
            /// Nothing is selected until ``MediaPlayer/select(_:start:independentTiming:sharedEpoch:)``.
            case manual
        }

        public enum TimingPolicy: Sendable, Hashable {
            /// Automatic selection leaves out a render group whose tracks
            /// cannot share one timeline (``PlayerEvent/syncUnavailable(renderGroup:reason:)``).
            case requireSync
            /// Automatic selection plays such a group on a timeline per
            /// track (``PlayerEvent/timingIndependent(renderGroup:)``).
            case allowIndependent
        }

        public var namespace: MediaNamespace
        /// The receiver's overflow policy; the player takes objects as fast
        /// as its engine has room, so this governs what a slow decoder
        /// costs. Defaults to the live preset's drop-to-keyframe.
        public var overflowPolicy: MediaReceiver.Configuration.OverflowPolicy
        public var companionProfile: MediaCompanionProfile
        /// Catalog track name; `nil` uses the MSF default ("catalog").
        public var catalogTrack: String?
        public var selection: Selection
        /// Automatic selection: within an alternate group, prefer this
        /// catalog language.
        public var preferredLanguage: String?
        public var timingPolicy: TimingPolicy
        /// Automatic selection: treat a render group whose tracks are all in
        /// application media time as one timeline.
        public var assumeSharedEpoch: Bool
        /// A timeline's first unit presents this long after the pass that
        /// released it; `nil` selects the library default (100 ms).
        public var cushion: Duration?
        /// How long a track whose input ended unproven keeps accepting late
        /// input; `nil` selects the library default (2 s).
        public var endTailWait: Duration?

        public init(namespace: MediaNamespace) {
            self.namespace = namespace
            self.overflowPolicy = .dropToKeyframe()
            self.companionProfile = .automatic
            self.catalogTrack = nil
            self.selection = .automatic
            self.preferredLanguage = nil
            self.timingPolicy = .requireSync
            self.assumeSharedEpoch = false
            self.cushion = nil
            self.endTailWait = nil
        }

        package func validate() throws {
            try receiverConfiguration.validate()
            if let cushion, cushion <= .zero {
                throw MoQServiceError.invalidArgument(
                    "cushion must be positive (nil = library default)")
            }
            if let endTailWait, endTailWait <= .zero {
                throw MoQServiceError.invalidArgument(
                    "endTailWait must be positive (nil = library default)")
            }
        }

        /// The receiver the player owns: RAW time, no auto-subscribe (the
        /// player subscribes what it selects).
        package var receiverConfiguration: MediaReceiver.Configuration {
            var receiver = MediaReceiver.Configuration(
                namespace: namespace, overflowPolicy: overflowPolicy,
                autoSubscribe: false)
            receiver.companionProfile = companionProfile
            receiver.catalogTrack = catalogTrack
            return receiver
        }
    }

    /// Player counters, a snapshot.
    public struct Statistics: Sendable {
        public let tracksSelected: Int
        public let objectsTaken: UInt64
        public let objectsPushed: UInt64
        public let objectsDropped: UInt64
        public let objectsDiscarded: UInt64
        public let unitsOutput: UInt64
        public let configuresOutput: UInt64
        public let discontinuitiesOutput: UInt64
        public let endsOutput: UInt64
        public let eventsDropped: UInt64
        public let feedbackStale: UInt64
        /// Bytes the player's queued outputs keep charged to its engine.
        public let outputsBytes: UInt64
        /// Tracks automatic selection chose but could not select.
        public let selectionsFailed: UInt64

        package init(tracksSelected: Int, objectsTaken: UInt64, objectsPushed: UInt64,
                     objectsDropped: UInt64, objectsDiscarded: UInt64, unitsOutput: UInt64,
                     configuresOutput: UInt64, discontinuitiesOutput: UInt64,
                     endsOutput: UInt64, eventsDropped: UInt64, feedbackStale: UInt64,
                     outputsBytes: UInt64, selectionsFailed: UInt64) {
            self.tracksSelected = tracksSelected
            self.objectsTaken = objectsTaken
            self.objectsPushed = objectsPushed
            self.objectsDropped = objectsDropped
            self.objectsDiscarded = objectsDiscarded
            self.unitsOutput = unitsOutput
            self.configuresOutput = configuresOutput
            self.discontinuitiesOutput = discontinuitiesOutput
            self.endsOutput = endsOutput
            self.eventsDropped = eventsDropped
            self.feedbackStale = feedbackStale
            self.outputsBytes = outputsBytes
            self.selectionsFailed = selectionsFailed
        }
    }

    public let endpoint: MoQEndpoint
    public let configuration: Configuration
    /// The engine's hook closures capture THIS object -- never the player
    /// or the endpoint -- so the player's deinit backstop stays reachable.
    package let attachment: PlayerAttachment

    private init(endpoint: MoQEndpoint, configuration: Configuration,
                 attachment: PlayerAttachment) {
        self.endpoint = endpoint
        self.configuration = configuration
        self.attachment = attachment
    }

    deinit {
        /* Backstop for a player dropped without close(): detach on the
         * service thread, and release the slot only while it is still this
         * player's. Once shutdown has begun, its teardown pass owns it. */
        guard !attachment.isDetached else { return }
        let attachment = self.attachment
        let engine = endpoint.engine
        engine.post {
            attachment.finalizeDetach()
            attachment.hook.remove(from: engine, kind: .receiver)
        }
    }

    // MARK: Lifecycle

    /// Attach a player to an established or establishing endpoint. Throws
    /// ``MoQServiceError/wrongState`` when the endpoint already has a
    /// receiver or player, ``MoQServiceError/unsupported`` for an endpoint
    /// without an installed-stack backend, ``MoQServiceError/closed`` once
    /// it is shutting down, and the backend's attach refusals.
    public static func attach(to endpoint: MoQEndpoint,
                              configuration: Configuration) async throws -> MediaPlayer {
        try configuration.validate()
        guard let factory = endpoint.backend as? any PlayerBackendFactory else {
            throw MoQServiceError.unsupported
        }
        return try await attach(to: endpoint, configuration: configuration) {
            try factory.makePlayerBackend(configuration: configuration)
        }
    }

    /// The wiring behind the public attach, parameterized by the backend's
    /// construction; tests drive it with scripted backends. Construction,
    /// the slot claim and, when the claim fails, the backend's destruction
    /// are one service-thread job: no shutdown or other attachment can come
    /// between them, so the backend is torn down exactly once, before the
    /// endpoint's own.
    package static func attach(
        to endpoint: MoQEndpoint, configuration: Configuration,
        make: @escaping @Sendable () throws -> any PlayerBackend
    ) async throws -> MediaPlayer {
        try configuration.validate()
        let engine = endpoint.engine
        let attachment = try await engine.perform { () throws -> PlayerAttachment in
            let attachment = PlayerAttachment(backend: try make())
            do {
                let token = try engine.installPumpHook(
                    kind: .receiver,
                    run: { attachment.pump() },
                    teardown: { attachment.finalizeDetach() },
                    deadline: { attachment.nextDeadline() })
                attachment.hook.set(token)
            } catch {
                attachment.finalizeDetach()
                throw error
            }
            return attachment
        }
        return MediaPlayer(endpoint: endpoint, configuration: configuration,
                           attachment: attachment)
    }

    /// Detach: destroy the player and its receiver. Outputs and events
    /// already staged still deliver; then both sequences end. Idempotent
    /// and cancellation-immune.
    public func close() async {
        let attachment = self.attachment
        let engine = endpoint.engine
        await engine.postAndWait {
            attachment.finalizeDetach()
            attachment.hook.remove(from: engine, kind: .receiver)
        }
    }

    // MARK: Outputs and events

    /// Decode-order outputs; ends (`nil`) once the player is closed and
    /// everything it produced was delivered; throws
    /// ``MoQServiceError/fatal(code:)`` after a fatal receiver failure.
    /// One consumer at a time (a second concurrent iteration throws
    /// ``MoQServiceError/wrongState``). A cancelled consumer takes nothing:
    /// what is staged stays for the next.
    public var outputs: PlayerOutputs { PlayerOutputs(player: self) }

    /// Diagnostics and lifecycle events, ending after the player closes.
    /// One consumer at a time.
    public var events: PlayerEvents { PlayerEvents(player: self) }

    package func nextOutput() async throws -> PlayerOutput? {
        try await Self.next(of: attachment.outputQueue, engine: endpoint.engine)
    }

    package func nextEvent() async throws -> PlayerEvent? {
        try await Self.next(of: attachment.eventQueue, engine: endpoint.engine)
    }

    package static func next<T: Sendable>(of queue: StagedQueue<T>,
                                          engine: EndpointEngine) async throws -> T? {
        guard queue.claimConsumer() else { throw MoQServiceError.wrongState }
        defer { queue.releaseConsumer() }
        while true {
            try Task.checkCancellation()
            switch queue.pop() {
            case .item(let item, let freedCapacity):
                /* A full staging queue stops the pump polling the backend;
                 * the slot just freed is what lets it continue. */
                if freedCapacity { engine.nudgeWaiters() }
                return item
            case .finished:
                return nil
            case .failed(let code):
                throw MoQServiceError.fatal(code: code)
            case .empty:
                do {
                    try await engine.waitUntil {
                        queue.hasItemOrEnd ? .satisfied : .pending
                    }
                } catch let error as MoQServiceError where error == .closed {
                    /* Engine shutdown: its teardown pass finished the queue
                     * after a final pump; deliver what remains. */
                    if !queue.hasItemOrEnd { return nil }
                }
            }
        }
    }

    // MARK: Selection

    /// Select an announced track (manual selection, or one automatic
    /// selection left out). Throws ``MoQServiceError/unsupported`` for a
    /// packaging, codec or timing this release does not play,
    /// ``MoQServiceError/wrongState`` for a track already selected or ended.
    public func select(_ track: PlayerTrack, start: StartMode = .current,
                       independentTiming: Bool = false,
                       sharedEpoch: Bool = false) async throws {
        let attachment = self.attachment
        try await endpoint.engine.perform {
            try attachment.live().select(trackID: track.id, start: start,
                                         independentTiming: independentTiming,
                                         sharedEpoch: sharedEpoch)
        }
    }

    /// Deselect: unsubscribe and drop what the player holds for the track.
    /// Outputs already produced stay; no end follows.
    public func deselect(_ track: PlayerTrack) async throws {
        let attachment = self.attachment
        try await endpoint.engine.perform {
            try attachment.live().deselect(trackID: track.id)
        }
    }

    // MARK: Decoder feedback

    /// The track's decoder queue: release pauses while `depth` exceeds
    /// `maxRecommended`, and resumes below it. `generation` is the
    /// `configurationGeneration` of the outputs the decoder holds; feedback
    /// for an older one is ignored.
    public func reportDecoderQueue(_ track: PlayerTrack, generation: UInt64,
                                   depth: Int, maxRecommended: Int) async throws {
        try await feedback(track, generation: generation,
                           kind: .decoderQueue(depth: depth, maxRecommended: maxRecommended))
    }

    /// The decoder failed: the track resumes at its next independent unit,
    /// behind a ``PlayerDiscontinuityReason/decodeError`` discontinuity.
    public func reportDecodeError(_ track: PlayerTrack, generation: UInt64,
                                  message: String = "") async throws {
        try await feedback(track, generation: generation, kind: .decodeError(message))
    }

    /// The decoder flushed after the track's end.
    public func reportFlushComplete(_ track: PlayerTrack, generation: UInt64) async throws {
        try await feedback(track, generation: generation, kind: .flushComplete)
    }

    private func feedback(_ track: PlayerTrack, generation: UInt64,
                          kind: PlayerFeedback) async throws {
        let attachment = self.attachment
        try await endpoint.engine.perform {
            try attachment.live().feedback(trackID: track.id, generation: generation,
                                           kind: kind)
        }
    }

    // MARK: Observing

    public func statistics() async throws -> Statistics {
        let attachment = self.attachment
        return try await endpoint.engine.perform { try attachment.live().statistics() }
    }
}

/// The ``MediaPlayer/outputs`` async sequence.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct PlayerOutputs: AsyncSequence, Sendable {
    public typealias Element = PlayerOutput
    let player: MediaPlayer
    public func makeAsyncIterator() -> Iterator { Iterator(player: player) }
    public struct Iterator: AsyncIteratorProtocol, Sendable {
        let player: MediaPlayer
        public mutating func next() async throws -> PlayerOutput? {
            try await player.nextOutput()
        }
    }
}

/// The ``MediaPlayer/events`` async sequence.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct PlayerEvents: AsyncSequence, Sendable {
    public typealias Element = PlayerEvent
    let player: MediaPlayer
    public func makeAsyncIterator() -> Iterator { Iterator(player: player) }
    public struct Iterator: AsyncIteratorProtocol, Sendable {
        let player: MediaPlayer
        public mutating func next() async throws -> PlayerEvent? {
            try await player.nextEvent()
        }
    }
}

// MARK: - Backend

/// Decoder feedback, as the backend applies it.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package enum PlayerFeedback: Sendable {
    case decoderQueue(depth: Int, maxRecommended: Int)
    case decodeError(String)
    case flushComplete
}

/// One tick's outcome.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package enum PlayerTickResult: Sendable, Hashable {
    case ok
    /// The output queue is full and the player holds more.
    case wouldBlock
    case closed
}

/// A player the attachment drives: the C `moq_media_player` in the
/// MoQService product, scripted players in tests. Every call is made on the
/// endpoint's service thread, never after `destroy`.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package protocol PlayerBackend: AnyObject, Sendable {
    func tick(nowMicroseconds: UInt64) -> PlayerTickResult
    /// The next output, copied; nil when none is queued.
    func pollOutput() -> PlayerOutput?
    func pollEvent() -> PlayerEvent?
    /// The earliest clock value at which tick has time-driven work.
    var nextDeadlineMicroseconds: UInt64? { get }
    /// Closed: nothing more will be produced, and everything was polled.
    var isClosed: Bool { get }
    var fatalCode: UInt64? { get }
    func select(trackID: UInt64, start: StartMode, independentTiming: Bool,
                sharedEpoch: Bool) throws
    func deselect(trackID: UInt64) throws
    func feedback(trackID: UInt64, generation: UInt64, kind: PlayerFeedback) throws
    func statistics() throws -> MediaPlayer.Statistics
    func destroy()
}

/// An endpoint backend that can attach a player (the MoQService product's).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package protocol PlayerBackendFactory: Sendable {
    /// Called on the endpoint's service thread.
    func makePlayerBackend(configuration: MediaPlayer.Configuration) throws -> any PlayerBackend
}

// MARK: - Staging

/// A bounded, lock-guarded FIFO between the service thread (push, finish)
/// and one consumer task (pop).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package final class StagedQueue<T: Sendable>: @unchecked Sendable {
    package enum Popped {
        case item(T, freedCapacity: Bool)
        case empty
        case finished
        case failed(code: UInt64)
    }

    package let capacity: Int
    private let lock = NSLock()
    private var items: [T] = []
    private var head = 0
    private var finished = false
    private var fatalCode: UInt64?
    private var consumerBusy = false

    package init(capacity: Int) { self.capacity = capacity }

    package var hasRoom: Bool {
        lock.lock(); defer { lock.unlock() }
        return items.count - head < capacity
    }

    package var hasItemOrEnd: Bool {
        lock.lock(); defer { lock.unlock() }
        return items.count > head || finished
    }

    package func push(_ item: T) {
        lock.lock(); defer { lock.unlock() }
        items.append(item)
    }

    /// No more items will come; `fatalCode` makes the end a failure once
    /// the staged items are delivered. The first end stands.
    package func finish(fatalCode code: UInt64?) {
        lock.lock(); defer { lock.unlock() }
        guard !finished else { return }
        finished = true
        fatalCode = code
    }

    package func pop() -> Popped {
        lock.lock(); defer { lock.unlock() }
        if head < items.count {
            let wasFull = items.count - head >= capacity
            let item = items[head]
            head += 1
            if head == items.count {
                items.removeAll(keepingCapacity: true)
                head = 0
            } else if head >= 64 {
                items.removeFirst(head)
                head = 0
            }
            return .item(item, freedCapacity: wasFull)
        }
        guard finished else { return .empty }
        if let fatalCode { return .failed(code: fatalCode) }
        return .finished
    }

    package func claimConsumer() -> Bool {
        lock.lock(); defer { lock.unlock() }
        guard !consumerBusy else { return false }
        consumerBusy = true
        return true
    }

    package func releaseConsumer() {
        lock.lock(); defer { lock.unlock() }
        consumerBusy = false
    }
}

// MARK: - The engine-facing attachment

/// The backend, the staging queues and the service-thread work on them.
/// Deliberately references neither the player nor the endpoint, so the
/// engine's hook slot never keeps them alive.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package final class PlayerAttachment: @unchecked Sendable {

    /// Staged records; the backend's own queues sit behind these.
    package static let outputStaging = 32
    package static let eventStaging = 64

    package let outputQueue = StagedQueue<PlayerOutput>(capacity: PlayerAttachment.outputStaging)
    package let eventQueue = StagedQueue<PlayerEvent>(capacity: PlayerAttachment.eventStaging)
    package let hook = PumpHookOwnership()

    /* Service-thread state. */
    private var backend: (any PlayerBackend)?
    /// The last pass ended with the player's output queue full and no
    /// staging room to take from it: its deadline work cannot run until a
    /// consumer frees room, which wakes the engine.
    private var blockedOnStaging = false
    private let detachLock = NSLock()
    private var detached = false

    package init(backend: any PlayerBackend) {
        self.backend = backend
    }

    package var isDetached: Bool {
        detachLock.lock(); defer { detachLock.unlock() }
        return detached
    }

    /// The live backend, or `.closed` once detached. Service thread only.
    package func live() throws -> any PlayerBackend {
        guard let backend else { throw MoQServiceError.closed }
        return backend
    }

    /// Destroy the backend and end both sequences after what is staged.
    /// Idempotent; returns whether this call detached.
    @discardableResult
    package func finalizeDetach() -> Bool {
        detachLock.lock()
        let first = !detached
        detached = true
        detachLock.unlock()
        guard first else { return false }
        backend?.destroy()
        backend = nil
        outputQueue.finish(fatalCode: nil)
        eventQueue.finish(fatalCode: nil)
        return true
    }

    /// One engine pass: tick at the current time and stage what the player
    /// produced, ticking again while its output queue was full and the
    /// staging took from it.
    package func pump() {
        guard let backend else { return }
        let now = EndpointEngine.monotonicMicroseconds()
        var result = PlayerTickResult.ok
        for _ in 0..<8 {
            result = backend.tick(nowMicroseconds: now)
            let staged = stageOutputs(backend)
            stageEvents(backend)
            guard result == .wouldBlock, staged else { break }
        }
        blockedOnStaging = result == .wouldBlock && !outputQueue.hasRoom
        /* Closed means everything the player produced is staged. */
        if backend.isClosed {
            outputQueue.finish(fatalCode: backend.fatalCode)
            eventQueue.finish(fatalCode: nil)
        }
    }

    /// The player's next time-driven work for the engine's wait. While the
    /// last pass was blocked on full staging, the deadline is not work: a
    /// tick cannot hand anything over until a consumer frees room, and that
    /// pop wakes the engine itself. Otherwise a due deadline is work at once,
    /// whatever its value: the C player reports a deadline only for work its
    /// tick can do unless its output queue is full.
    package func nextDeadline() -> UInt64? {
        guard let backend, let due = backend.nextDeadlineMicroseconds else { return nil }
        return blockedOnStaging ? nil : due
    }

    /// Poll outputs while staging has room; true when any was staged.
    private func stageOutputs(_ backend: any PlayerBackend) -> Bool {
        var staged = false
        while outputQueue.hasRoom, let output = backend.pollOutput() {
            outputQueue.push(output)
            staged = true
        }
        return staged
    }

    private func stageEvents(_ backend: any PlayerBackend) {
        while eventQueue.hasRoom, let event = backend.pollEvent() {
            eventQueue.push(event)
        }
    }
}
