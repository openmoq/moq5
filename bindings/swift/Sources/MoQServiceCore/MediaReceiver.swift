import Foundation

/// Subscribes to a namespace's catalog and media tracks over an endpoint.
///
/// Track discovery is eager: the engine drains the receiver's track-event
/// queue every pass (the C queue's overflow is TERMINAL — the SDK's job is
/// to make that unreachable) into a Swift FIFO that ``trackEvents``
/// consumes. Object delivery is the opposite, strictly PULL-based: the C
/// object queue is the only buffer, and ``objects`` polls one object per
/// demand so the configured overflow policy keeps acting on the real queue
/// depth. ``subscribe(_:start:priority:)``/``unsubscribe(_:)`` provide
/// manual track selection.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public final class MediaReceiver: @unchecked Sendable {

    // MARK: Configuration

    public struct Configuration: Sendable {
        /// What happens when the app polls slower than media arrives. The C
        /// tier owns the queue and the eviction; there is no default — the
        /// choice is forced, exactly like the C configuration.
        public enum OverflowPolicy: Sendable {
            /// Live playback: evict whole groups, always keep the newest
            /// keyframe suffix.
            case dropToKeyframe(maxObjects: Int? = nil, maxBytes: Int? = nil)
            /// Evict whole groups from the oldest end.
            case dropGroup(maxObjects: Int? = nil, maxBytes: Int? = nil)
            /// Never drop: pause the subscription upstream instead
            /// (auto-resumes as the app catches up).
            case flowControl(maxObjects: Int? = nil, maxBytes: Int? = nil)
        }

        public var namespace: MediaNamespace
        public var overflowPolicy: OverflowPolicy
        public var autoSubscribe: Bool
        public var timeMode: TimeMode
        /// Catalog track name; `nil` uses the MSF default ("catalog").
        public var catalogTrack: String?

        public init(namespace: MediaNamespace,
                    overflowPolicy: OverflowPolicy,
                    autoSubscribe: Bool = true) {
            self.namespace = namespace
            self.overflowPolicy = overflowPolicy
            self.autoSubscribe = autoSubscribe
            self.timeMode = .raw
            self.catalogTrack = nil
        }

        /// The live-player preset (mirrors the C `cfg_init_live`):
        /// drop-to-keyframe overflow, auto-subscribe.
        public static func live(namespace: MediaNamespace) -> Configuration {
            Configuration(namespace: namespace, overflowPolicy: .dropToKeyframe())
        }

        /// The lossless/VOD preset (mirrors the C `cfg_init_flow_control`):
        /// upstream pause instead of drops.
        public static func flowControl(namespace: MediaNamespace) -> Configuration {
            Configuration(namespace: namespace, overflowPolicy: .flowControl())
        }

        package func validate() throws {
            try namespace.validate()
            let (maxObjects, maxBytes): (Int?, Int?)
            switch overflowPolicy {
            case .dropToKeyframe(let o, let b),
                 .dropGroup(let o, let b),
                 .flowControl(let o, let b):
                (maxObjects, maxBytes) = (o, b)
            }
            if let o = maxObjects, o <= 0 {
                throw MoQServiceError.invalidArgument(
                    "overflow maxObjects must be positive (nil = library default)")
            }
            if let b = maxBytes, b <= 0 {
                throw MoQServiceError.invalidArgument(
                    "overflow maxBytes must be positive (nil = library default)")
            }
        }
    }

    public let endpoint: MoQEndpoint
    public let configuration: Configuration
    /// The engine's hook closures capture THIS object -- never the receiver
    /// or the endpoint -- so no retain cycle runs engine -> receiver ->
    /// endpoint -> engine, and both deinit backstops stay reachable.
    package let attachment: ReceiverAttachment
    package var backend: any ReceiverBackend { attachment.backend }
    private var state: ReceiverState { attachment.state }

    package init(endpoint: MoQEndpoint,
                 configuration: Configuration,
                 backend: any ReceiverBackend) {
        self.endpoint = endpoint
        self.configuration = configuration
        self.attachment = ReceiverAttachment(backend: backend)
    }

    deinit {
        /* Best-effort backstop: the app dropped the receiver without
         * close(). Skip if already detached -- the slot may belong to a
         * NEWER receiver by then. Remove the hook ONLY when the detach job
         * was accepted: once engine shutdown has begun, submissions are
         * rejected, and the still-installed hook is what makes the shutdown
         * teardown own the detach instead. */
        guard !attachment.state.isDetached else { return }
        let attachment = self.attachment
        let engine = endpoint.engine
        if engine.post({ attachment.finalizeDetach() }) {
            engine.removePumpHook(kind: .receiver)
        }
    }

    // MARK: Lifecycle

    /// Attach to an existing endpoint (one receiver per endpoint; may share
    /// the endpoint with one ``MediaSender``). Synchronous: validation and
    /// the engine slot claim happen here; discovery proceeds on the
    /// endpoint's service thread.
    public static func attach(to endpoint: MoQEndpoint,
                              configuration: Configuration) throws -> MediaReceiver {
        try configuration.validate()
        throw MoQServiceError.unsupported   // TODO(S6): real backend factory
    }

    /// The wiring the public attach uses once a backend exists (S6); tests
    /// drive it with scripted backends. Throws `.wrongState` when the
    /// endpoint already has a receiver, `.closed` once it is shutting down.
    package static func attach(to endpoint: MoQEndpoint,
                               configuration: Configuration,
                               backend: any ReceiverBackend) throws -> MediaReceiver {
        try configuration.validate()
        let receiver = MediaReceiver(
            endpoint: endpoint, configuration: configuration, backend: backend)
        let attachment = receiver.attachment    /* NOT the receiver: no cycle */
        try endpoint.engine.installPumpHook(
            kind: .receiver,
            run: { attachment.drainTrackEvents() },
            teardown: { attachment.finalizeDetach() })
        return receiver
    }

    /// Detach from the endpoint: stops discovery, releases the attachment
    /// slot, and ends ``trackEvents`` cleanly (already-drained events still
    /// deliver first). Idempotent; safe concurrently with `endpoint.close()`
    /// (whichever runs first performs the one detach). Cancellation-immune:
    /// a cancelled caller still detaches.
    public func close() async {
        let attachment = self.attachment
        let engine = endpoint.engine
        await engine.postAndWait {
            attachment.finalizeDetach()
            engine.removePumpHook(kind: .receiver)
        }
        /* A false return means the engine is shutting down (or gone): its
         * teardown pass performs the same detach. */
    }

    // MARK: Track discovery

    /// The discovery event stream. Single active consumer (a second
    /// concurrent `next()` throws ``MoQServiceError/wrongState``); a new
    /// iteration after one ends picks up from the current queue. Ends `nil`
    /// on clean close/detach after all queued events drain; throws
    /// ``MoQServiceError/fatal(code:)`` once drained when the receiver
    /// failed; throws ``MoQServiceError/interrupted`` only when it would
    /// have to WAIT while the endpoint latch is set (queued events deliver
    /// through the latch, like the C track-event poll).
    public var trackEvents: TrackEvents { TrackEvents(receiver: self) }

    /// Snapshot of every discovered track, in discovery order. Handles stay
    /// valid (and listed) after removal/end, mirroring the C handle
    /// lifetime.
    public var tracks: [MediaTrack] { state.orderedTracks() }

    // MARK: Objects

    /// The media object stream, strictly PULL-based: each element is one
    /// demand-driven poll on the service thread, and the C object queue is
    /// the ONLY buffer -- the configured overflow policy keeps acting on
    /// the real queue depth. Single active consumer (a second concurrent
    /// `next()` throws ``MoQServiceError/wrongState``); may be consumed
    /// concurrently with ``trackEvents`` by a different task. Objects drain
    /// after terminal, then the stream ends `nil` (clean) or throws
    /// ``MoQServiceError/fatal(code:)``. Throws
    /// ``MoQServiceError/interrupted`` while the endpoint latch is set,
    /// EVEN with objects queued -- the C object poll is latch-gated, unlike
    /// the track-event poll. Cancellation: a poll that already started
    /// delivers its object (exactly-once cleanup either way); a cancel
    /// before it starts leaves the queue untouched.
    public var objects: MediaObjects { MediaObjects(receiver: self) }

    /// Subscribe a discovered track (manual selection; pause/resume model).
    /// An ASYNCHRONOUS command, like C: success means validated and
    /// recorded -- peer acceptance surfaces as delivered objects, rejection
    /// as a `.ended` track event. Start mode and priority apply only when
    /// the underlying subscription is first issued; later calls are
    /// idempotent resume commands.
    public func subscribe(_ track: MediaTrack,
                          start: StartMode = .current,
                          priority: UInt8? = nil) async throws {
        try await command(for: track, name: "subscribe") { backend, id in
            backend.subscribe(handleID: id, start: start, priority: priority)
        }
    }

    /// Pause delivery and purge this track's queued objects; a later
    /// ``subscribe(_:start:priority:)`` resumes cheaply.
    public func unsubscribe(_ track: MediaTrack) async throws {
        try await command(for: track, name: "unsubscribe") { backend, id in
            backend.unsubscribe(handleID: id)
        }
    }

    private func command(
        for track: MediaTrack, name: String,
        _ body: @escaping @Sendable (any ReceiverBackend, UInt64) -> ReceiverCommandResult
    ) async throws {
        guard let handleID = attachment.handleID(for: track) else {
            throw MoQServiceError.invalidArgument(
                "\(name): the track does not belong to this receiver")
        }
        let attachment = self.attachment
        try await endpoint.engine.perform {
            guard !attachment.state.isDetached else {
                throw MoQServiceError.closed
            }
            switch body(attachment.backend, handleID) {
            case .ok:
                return
            case .invalidArgument:
                throw MoQServiceError.invalidArgument("\(name): invalid track")
            case .wrongState:
                throw MoQServiceError.wrongState
            case .closed:
                throw MoQServiceError.closed
            case .unsupported:
                throw MoQServiceError.unsupported
            }
        }
    }

    // MARK: Stream plumbing

    /// One stream element; see ``trackEvents`` for the contract.
    package func nextTrackEvent() async throws -> TrackEvent? {
        guard state.claimConsumer() else {
            throw MoQServiceError.wrongState
        }
        defer { state.releaseConsumer() }
        while true {
            switch state.takeNext() {
            case .item(let event):
                return event
            case .terminalClean:
                return nil
            case .terminalFatal(let code):
                throw MoQServiceError.fatal(code: code)
            case .empty:
                break
            }
            do {
                try await endpoint.engine.waitUntil { [state] in
                    state.hasItemOrTerminal ? .satisfied : .pending
                }
            } catch let error as MoQServiceError where error == .closed {
                /* Engine shutdown: the teardown pass marked us terminal
                 * (after a final drain) -- loop to deliver what remains.
                 * Defensive nil if the state somehow never terminalized. */
                if !state.hasItemOrTerminal { return nil }
            }
        }
    }

    /// One object-stream element; see ``objects`` for the contract.
    package func nextObject() async throws -> MediaObject? {
        guard state.claimObjectsConsumer() else {
            throw MoQServiceError.wrongState
        }
        defer { state.releaseObjectsConsumer() }
        let attachment = self.attachment
        while true {
            let demand: ReceiverAttachment.ObjectDemand
            do {
                /* One demand = one service-thread poll. perform's started-
                 * job rule IS the cancellation guarantee: a poll that
                 * already began always delivers its result, so a
                 * transferred object reaches the (even cancelled) caller
                 * and ARC performs the one cleanup. */
                demand = try await endpoint.engine.perform {
                    attachment.demandObject()
                }
            } catch let error as MoQServiceError where error == .closed {
                /* Engine gone: teardown detached us. Undelivered C-queued
                 * objects died with the C receiver (destroy semantics). */
                return try state.finalObjectDisposition()
            }
            switch demand {
            case .object(let object):
                return object
            case .interrupted:
                throw MoQServiceError.interrupted
            case .terminalClean:
                return nil
            case .terminalFatal(let code):
                throw MoQServiceError.fatal(code: code)
            case .empty:
                do {
                    try await endpoint.engine.waitUntil {
                        attachment.objectWaiterReady ? .satisfied : .pending
                    }
                } catch let error as MoQServiceError where error == .closed {
                    return try state.finalObjectDisposition()
                }
            }
        }
    }
}

// MARK: - The engine-facing attachment

/// The receiver's engine-facing half: the backend plus all state the pump
/// hook touches. Deliberately references NEITHER the receiver NOR the
/// endpoint, so the engine's hook slot never keeps them alive (their deinit
/// backstops must stay reachable when an app forgets close()).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package final class ReceiverAttachment: @unchecked Sendable {

    package let backend: any ReceiverBackend
    fileprivate let state = ReceiverState()

    init(backend: any ReceiverBackend) {
        self.backend = backend
    }

    /// The eager drain: empty the backend's event queue into the Swift FIFO.
    /// Runs on the service thread every engine pass.
    package func drainTrackEvents() {
        while true {
            guard !state.isDetached else { return }
            switch backend.pollTrackEvent() {
            case .none:
                return
            case .closed:
                state.markTerminal(fatal: backend.isFatal,
                                   code: backend.fatalCode)
                return
            case .event(let polled):
                state.integrate(polled)
            }
        }
    }

    /// Exactly-once detach (service thread): the first caller — receiver
    /// close, the receiver deinit backstop, or the engine's shutdown
    /// teardown — wins.
    package func finalizeDetach() {
        guard state.beginDetach() else { return }
        backend.detach()
    }

    // MARK: Objects (demand-driven; the C queue is the only buffer)

    package enum ObjectDemand {
        case object(MediaObject)
        case empty
        case interrupted
        case terminalClean
        case terminalFatal(code: UInt64)
    }

    /// One demand: drain pending track events FIRST (the C discipline —
    /// every handle is known before its first object), then poll ONE
    /// object. Service thread only.
    package func demandObject() -> ObjectDemand {
        while true {
            guard !state.isDetached else { return state.objectTerminalDemand() }
            drainTrackEvents()
            switch backend.pollObject() {
            case .none:
                return .empty
            case .interrupted:
                return .interrupted
            case .closed:
                state.markTerminal(fatal: backend.isFatal,
                                   code: backend.fatalCode)
                return state.objectTerminalDemand()
            case .object(let polled):
                guard let track = state.track(for: polled.handleID) else {
                    /* Impossible per the C contract (TRACK_ADDED precedes
                     * every object); never leak the transferred buffers. */
                    polled.storage.cleanup()
                    continue
                }
                return .object(MediaObject(
                    storage: polled.storage,
                    track: track,
                    isKeyframe: polled.isKeyframe,
                    endsGroup: polled.endsGroup,
                    isDatagram: polled.isDatagram,
                    presentationTime: polled.presentationTime,
                    decodeTime: polled.decodeTime,
                    compositionOffset: polled.compositionOffset,
                    captureTime: polled.captureTime))
            }
        }
    }

    /// The parked object consumer's level term: something to poll, or a
    /// terminal state to report. Evaluated on the service thread each pass.
    package var objectWaiterReady: Bool {
        state.isDetached || backend.hasQueuedObjects || backend.isTerminal
    }

    /// The stable handle for a track discovered by THIS receiver.
    package func handleID(for track: MediaTrack) -> UInt64? {
        state.handleID(for: track)
    }
}

// MARK: - Track event stream

/// The ``MediaReceiver/trackEvents`` async sequence.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct TrackEvents: AsyncSequence, Sendable {
    public typealias Element = TrackEvent

    let receiver: MediaReceiver

    public func makeAsyncIterator() -> Iterator {
        Iterator(receiver: receiver)
    }

    public struct Iterator: AsyncIteratorProtocol {
        let receiver: MediaReceiver
        public mutating func next() async throws -> TrackEvent? {
            try await receiver.nextTrackEvent()
        }
    }
}

/// The ``MediaReceiver/objects`` async sequence.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct MediaObjects: AsyncSequence, Sendable {
    public typealias Element = MediaObject

    let receiver: MediaReceiver

    public func makeAsyncIterator() -> Iterator {
        Iterator(receiver: receiver)
    }

    public struct Iterator: AsyncIteratorProtocol {
        let receiver: MediaReceiver
        public mutating func next() async throws -> MediaObject? {
            try await receiver.nextObject()
        }
    }
}

// MARK: - Lock-guarded receiver state

/// FIFO + track map + terminal/consumer flags. Mutated by the service
/// thread (drain/teardown) and read/popped by consumer tasks; one lock
/// guards it all. Lock order: this lock may take a MediaTrack's inner lock
/// (updateDescription); nothing takes them in reverse.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private final class ReceiverState: @unchecked Sendable {

    enum Taken {
        case item(TrackEvent)
        case empty
        case terminalClean
        case terminalFatal(code: UInt64)
    }

    private let lock = NSLock()
    private var fifo: [TrackEvent] = []
    private var tracksByID: [UInt64: MediaTrack] = [:]
    private var idsByTrack: [ObjectIdentifier: UInt64] = [:]
    private var ordered: [MediaTrack] = []
    private var terminal = false
    private var fatal = false
    private var fatalCode: UInt64 = 0
    private var detached = false
    private var consumerBusy = false
    private var objectsConsumerBusy = false

    /// Map one polled backend event into the model and queue it. Unknown
    /// handles (an update/remove for a track never added) are dropped
    /// defensively — the C receiver never emits that ordering.
    func integrate(_ polled: ReceiverPolledEvent) {
        lock.lock()
        defer { lock.unlock() }
        switch polled.kind {
        case .added:
            guard let description = polled.trackDescription else { return }
            let track = MediaTrack(description: description)
            tracksByID[polled.handleID] = track
            idsByTrack[ObjectIdentifier(track)] = polled.handleID
            ordered.append(track)
            fifo.append(.added(track))
        case .updated:
            guard let track = tracksByID[polled.handleID] else { return }
            if let description = polled.trackDescription {
                track.updateDescription(description)
            }
            fifo.append(.updated(track))
        case .removed:
            guard let track = tracksByID[polled.handleID] else { return }
            fifo.append(.removed(track))
        case .ended:
            guard let track = tracksByID[polled.handleID] else { return }
            fifo.append(.ended(track))
        case .catalogReady:
            fifo.append(.catalogReady)
        }
    }

    func markTerminal(fatal isFatal: Bool, code: UInt64) {
        lock.lock()
        defer { lock.unlock() }
        terminal = true
        if isFatal && !fatal {          /* first fatal classification wins */
            fatal = true
            fatalCode = code
        }
    }

    /// True exactly once: the winner performs the backend detach.
    func beginDetach() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        if detached { return false }
        detached = true
        terminal = true                 /* detach is a clean terminal unless
                                         * a fatal was already recorded */
        return true
    }

    var isDetached: Bool {
        lock.lock(); defer { lock.unlock() }
        return detached
    }

    var hasItemOrTerminal: Bool {
        lock.lock(); defer { lock.unlock() }
        return !fifo.isEmpty || terminal
    }

    func takeNext() -> Taken {
        lock.lock()
        defer { lock.unlock() }
        if !fifo.isEmpty { return .item(fifo.removeFirst()) }
        if terminal {
            return fatal ? .terminalFatal(code: fatalCode) : .terminalClean
        }
        return .empty
    }

    func claimConsumer() -> Bool {
        lock.lock(); defer { lock.unlock() }
        if consumerBusy { return false }
        consumerBusy = true
        return true
    }

    func releaseConsumer() {
        lock.lock()
        consumerBusy = false
        lock.unlock()
    }

    func claimObjectsConsumer() -> Bool {
        lock.lock(); defer { lock.unlock() }
        if objectsConsumerBusy { return false }
        objectsConsumerBusy = true
        return true
    }

    func releaseObjectsConsumer() {
        lock.lock()
        objectsConsumerBusy = false
        lock.unlock()
    }

    func orderedTracks() -> [MediaTrack] {
        lock.lock(); defer { lock.unlock() }
        return ordered
    }

    func track(for handleID: UInt64) -> MediaTrack? {
        lock.lock(); defer { lock.unlock() }
        return tracksByID[handleID]
    }

    func handleID(for track: MediaTrack) -> UInt64? {
        lock.lock(); defer { lock.unlock() }
        return idsByTrack[ObjectIdentifier(track)]
    }

    /// The object stream's answer once the receiver is terminal/detached.
    func objectTerminalDemand() -> ReceiverAttachment.ObjectDemand {
        lock.lock(); defer { lock.unlock() }
        return fatal ? .terminalFatal(code: fatalCode) : .terminalClean
    }

    /// The object stream's answer once the ENGINE is gone: nil for a clean
    /// end, or the recorded fatal.
    func finalObjectDisposition() throws -> MediaObject? {
        lock.lock(); defer { lock.unlock() }
        if fatal { throw MoQServiceError.fatal(code: fatalCode) }
        return nil
    }
}
