import Foundation

/// A reusable, race-safe handle for watching one live namespace: it owns the
/// whole `connect → established → attach(.live) → discover → select → receive`
/// lifecycle plus the serialize-previous reconnect dance that every player
/// otherwise hand-rolls. One session owns at most ONE active watch;
/// ``start(_:)`` supersedes the previous watch, tearing it down completely
/// before the new one touches the network — so a single `LiveMediaSession`
/// can never run two live endpoints at once.
///
/// Module ownership: this type lives in `MoQServiceCore` (Foundation only) and
/// never references the gated `MoQEndpoint.connect` factory. It is constructed
/// with an injected endpoint factory; the `MoQService` product adds a
/// convenience initializer that supplies `MoQEndpoint.connect`.
///
/// Two surfaces to observe a watch:
/// - ``stateUpdates()`` — a fresh multicast observer that replays the latest
///   ``WatchState`` then future transitions.
/// - ``objects`` — a strictly pull-based, single-consumer sequence of the
///   selected tracks' ``MediaObject``s, pulled straight from the receiver's
///   own object queue (no second buffer).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public final class LiveMediaSession: @unchecked Sendable {

    // MARK: Configuration

    public struct Configuration: Sendable {
        /// How to reach the relay (URL, transport, versions, TLS).
        public var endpoint: MoQEndpoint.Configuration
        /// The namespace to watch.
        public var namespace: MediaNamespace
        /// Which tracks to receive, evaluated over the whole current snapshot.
        public var selector: TrackSelector
        /// The receiver's queue-overflow policy (the C queue is the buffer).
        public var overflowPolicy: MediaReceiver.Configuration.OverflowPolicy

        public init(endpoint: MoQEndpoint.Configuration,
                    namespace: MediaNamespace,
                    selector: TrackSelector = .firstVideo,
                    overflowPolicy: MediaReceiver.Configuration.OverflowPolicy
                        = .dropToKeyframe()) {
            self.endpoint = endpoint
            self.namespace = namespace
            self.selector = selector
            self.overflowPolicy = overflowPolicy
        }
    }

    // MARK: Stored state (one lock guards it all)

    private let endpointFactory: @Sendable (MoQEndpoint.Configuration) throws
        -> MoQEndpoint

    private let lock = NSLock()

    /// Bumped by every ``start(_:)``/``stop()``. Every state emission,
    /// selected-track update, and object-iterator transition is guarded by the
    /// generation captured when its watch began, so a superseded watch can
    /// never write into a newer watch's surface.
    private var generation = 0
    private var currentTask: Task<Void, Never>?
    private var currentEndpoint: MoQEndpoint?
    private var currentReceiver: MediaReceiver?

    /// The last emitted state (`nil` before the first watch and after
    /// ``stop()``); replayed to each new ``stateUpdates()`` observer.
    private var currentState: WatchState?
    /// Once a terminal state (`ended`/`failed`) is emitted for the current
    /// generation, later same-generation emissions are dropped.
    private var terminalReached = false
    /// The current watch's selected tracks (the last selector result). Kept
    /// current from BOTH the discovery loop AND the object iterator's own
    /// re-selection (see ``selectionIncludes(_:from:gen:)``), so a track the
    /// object poll integrated ahead of the discovery loop is still honored.
    private var selected: [MediaTrack] = []
    /// The current watch's selector (used by the object iterator to re-derive
    /// selection from the receiver's freshest track set at filter time).
    private var currentSelector: TrackSelector?
    /// Whether `.receiving` has already been emitted for the current watch.
    private var receivingEmitted = false

    private var observers: [Int: AsyncStream<WatchState>.Continuation] = [:]
    private var nextObserverID = 0

    /// Single-consumer guard for ``objects``.
    private var objectsConsumerActive = false

    // MARK: Init

    /// Core initializer: inject how an endpoint is created. Keeps this type
    /// free of the gated `MoQEndpoint.connect` factory; tests inject a scripted
    /// factory. See the `MoQService` convenience initializer for apps.
    public init(endpointFactory: @escaping @Sendable
                (MoQEndpoint.Configuration) throws -> MoQEndpoint) {
        self.endpointFactory = endpointFactory
    }

    deinit {
        /* Best-effort backstop for a session dropped without stop(): cancel
         * the driver (its teardown owns receiver.close()/endpoint.close()).
         * A live watch keeps this object alive via the running task, so this
         * only fires once no watch is active — the app must stop() (or let the
         * watch end) to release a live watch. */
        lock.lock()
        let task = currentTask
        currentTask = nil
        generation += 1
        lock.unlock()
        task?.cancel()
    }

    // MARK: State surface

    /// The current state, or `nil` before the first ``start(_:)`` and after
    /// ``stop()`` (no active watch).
    public var state: WatchState? {
        lock.lock(); defer { lock.unlock() }
        return currentState
    }

    /// A fresh observer stream. It immediately replays the current state (if
    /// any), then delivers every future transition. Multiple observers are
    /// supported; each is independent and coalesces to the newest state it has
    /// not yet consumed (`bufferingNewest(1)`).
    public func stateUpdates() -> AsyncStream<WatchState> {
        AsyncStream(bufferingPolicy: .bufferingNewest(1)) { continuation in
            lock.lock()
            let id = nextObserverID
            nextObserverID += 1
            observers[id] = continuation
            /* Replay under the lock so a concurrent emit() cannot interleave a
             * newer state ahead of this replay. */
            if let replay = currentState { continuation.yield(replay) }
            lock.unlock()
            continuation.onTermination = { [weak self] _ in
                self?.removeObserver(id)
            }
        }
    }

    /// The pull-based media stream over the selected tracks; see ``Objects``.
    public var objects: Objects { Objects(session: self) }

    // MARK: Escape hatches (do not hide the lower layer)

    public var endpoint: MoQEndpoint? {
        lock.lock(); defer { lock.unlock() }
        return currentEndpoint
    }
    public var receiver: MediaReceiver? {
        lock.lock(); defer { lock.unlock() }
        return currentReceiver
    }
    public var selectedTracks: [MediaTrack] {
        lock.lock(); defer { lock.unlock() }
        return selected
    }

    // MARK: Lifecycle

    /// Begin (or restart) watching. Supersedes any active watch: the previous
    /// watch is cancelled and torn down completely — receiver first, then
    /// endpoint — before this watch emits `.connecting` or calls the endpoint
    /// factory. Non-blocking; observe progress via ``stateUpdates()``.
    public func start(_ configuration: Configuration) {
        lock.lock()
        generation += 1
        let gen = generation
        let previous = currentTask
        /* Reset per-watch surface up front so a stale read between now and the
         * new watch's first emission sees "no state", not the old watch's. */
        currentEndpoint = nil
        currentReceiver = nil
        currentState = nil
        selected = []
        currentSelector = nil
        receivingEmitted = false
        terminalReached = false
        /* Create and record the driver in the SAME locked region that bumped
         * the generation, so a concurrent start() always chains onto THIS task
         * (never a stale currentTask). */
        let task = Task { [weak self] in
            await previous?.value        /* serialize behind full prior teardown */
            guard let self else { return }
            await self.driveWatch(configuration, gen: gen)
        }
        currentTask = task
        lock.unlock()
        previous?.cancel()
    }

    /// Stop the active watch and tear it down (receiver then endpoint),
    /// awaiting teardown completion. Suppresses any further state emission from
    /// the superseded watch; idempotent. Exactly ``beginStop()`` followed by
    /// awaiting its handle.
    public func stop() async {
        await beginStop().value
    }

    /// Supersede the active watch and begin its teardown, applied
    /// **synchronously**: the supersede is in effect before this returns, so a
    /// ``start(_:)`` called immediately afterward serializes behind the
    /// teardown — a disconnect→reconnect never runs two live endpoints at once,
    /// even when the caller cannot `await`.
    ///
    /// This is the race-free primitive for a synchronous context (a UI
    /// disconnect handler): calling `stop()` from a detached `Task` instead
    /// defers this supersede until that task is scheduled, and a `start()` in
    /// between would then be cancelled by the late-running stop. Prefer
    /// `beginStop()` there. Returns the teardown as a handle: ignore it for
    /// fire-and-forget, or `await …​.value` to wait (which is what `stop()`
    /// does). Idempotent.
    @discardableResult
    public func beginStop() -> Task<Void, Never> {
        lock.lock()
        defer { lock.unlock() }
        generation += 1
        let previous = currentTask
        currentEndpoint = nil
        currentReceiver = nil
        currentState = nil
        selected = []
        currentSelector = nil
        receivingEmitted = false
        terminalReached = false
        let teardown = Task { previous?.cancel(); await previous?.value }
        currentTask = teardown
        return teardown
    }

    // MARK: Driver (one watch)

    private func driveWatch(_ config: Configuration, gen: Int) async {
        /* We may have been superseded while awaiting the previous teardown. */
        guard isCurrent(gen) else { return }
        emit(.connecting, gen: gen)

        let endpoint: MoQEndpoint
        do {
            endpoint = try endpointFactory(config.endpoint)
        } catch {
            emit(.failed(Self.mapError(error)), gen: gen, terminal: true)
            return
        }
        /* Publish the endpoint only if still current; otherwise a start()
         * raced in during the (synchronous) factory call — tear it down. */
        guard setCurrentEndpoint(endpoint, gen: gen) else {
            await endpoint.close()
            return
        }

        var receiver: MediaReceiver?
        do {
            try await endpoint.established()
            emit(.established(version: endpoint.negotiatedVersion), gen: gen)
            emit(.awaitingCatalog, gen: gen)

            let attached = try MediaReceiver.attach(
                to: endpoint,
                configuration: MediaReceiver.Configuration(
                    namespace: config.namespace,
                    overflowPolicy: config.overflowPolicy,
                    autoSubscribe: true))
            receiver = attached
            _ = setCurrentReceiver(attached, selector: config.selector, gen: gen)

            /* Returns normally only when discovery ends cleanly (the track-
             * event stream reached its clean terminal). */
            try await discover(receiver: attached,
                               selector: config.selector, gen: gen)
            emit(.ended, gen: gen, terminal: true)
        } catch is CancellationError {
            /* Superseded or stopped: a newer generation (or stop()) owns the
             * state surface now — emit nothing. */
        } catch let error as MoQServiceError {
            emit(.failed(error), gen: gen, terminal: true)
        } catch {
            emit(.failed(Self.mapError(error)), gen: gen, terminal: true)
        }

        /* Teardown is cancellation-immune (receiver.close()/endpoint.close()
         * are), and strictly receiver-before-endpoint. */
        if let receiver { await receiver.close() }
        await endpoint.close()
        clearHandles(gen: gen)
    }

    private enum DiscoveryPhase { case awaitingCatalog, noMatch, selected }

    /// Drain track discovery. Selection is (re)run over the full snapshot on
    /// every event so `selectedTracks` stays current, but the watch stays in
    /// `awaitingCatalog` until `catalogReady` — only then does it classify into
    /// `awaitingFirstObject` (match) or `noMatchingTracks` (no match), and a
    /// late track can still lift `noMatchingTracks` into `awaitingFirstObject`.
    /// Returns when the event stream ends cleanly; throws
    /// `.fatal`/`CancellationError` otherwise.
    private func discover(receiver: MediaReceiver,
                          selector: TrackSelector, gen: Int) async throws {
        var catalogReady = false
        var phase = DiscoveryPhase.awaitingCatalog
        var announcedSelection: [MediaTrack] = []
        for try await event in receiver.trackEvents {
            guard isCurrent(gen) else { return }
            if case .catalogReady = event { catalogReady = true }
            let snapshot = receiver.tracks
            let selection = selector(snapshot)
            setSelected(selection, gen: gen)
            /* Classify only once the catalog is complete: before catalogReady
             * we are still enumerating and stay in `awaitingCatalog`. */
            guard catalogReady else { continue }
            if !selection.isEmpty {
                /* Emit on the first match AND whenever the selected set changes
                 * before the first object, so the `awaitingFirstObject` payload
                 * never goes stale (e.g. audio arriving after video under
                 * `firstVideoAndAudio`). `emitAwaitingFirstObject` suppresses
                 * the re-emit once `.receiving` has fired, so this never walks
                 * the state backwards. */
                if phase != .selected || selection != announcedSelection {
                    emitAwaitingFirstObject(selection, gen: gen)
                    phase = .selected
                    announcedSelection = selection
                }
            } else if phase == .awaitingCatalog {
                emit(.noMatchingTracks(catalog: snapshot.map(\.description)),
                     gen: gen)
                phase = .noMatch
            }
        }
    }

    // MARK: State plumbing (lock-guarded)

    private func emit(_ newState: WatchState, gen: Int, terminal: Bool = false) {
        lock.lock()
        guard gen == generation, !terminalReached else { lock.unlock(); return }
        currentState = newState
        if terminal { terminalReached = true }
        let conts = Array(observers.values)
        /* yield under the lock: cheap and non-blocking (AsyncStream just
         * updates its buffer), and it keeps emissions totally ordered against
         * a concurrent stateUpdates() replay. */
        for c in conts { c.yield(newState) }
        lock.unlock()
    }

    /// Emit `.awaitingFirstObject`, but never after the watch has already
    /// begun `.receiving` (an object can be pulled before discovery finishes
    /// classifying — media flowing wins over "waiting for the first object").
    private func emitAwaitingFirstObject(_ tracks: [MediaTrack], gen: Int) {
        lock.lock()
        guard gen == generation, !terminalReached, !receivingEmitted else {
            lock.unlock(); return
        }
        currentState = .awaitingFirstObject(tracks: tracks)
        let conts = Array(observers.values)
        for c in conts { c.yield(.awaitingFirstObject(tracks: tracks)) }
        lock.unlock()
    }

    /// Emit `.receiving` exactly once per watch, the moment the first selected
    /// object is actually pulled. Called from the object iterator.
    fileprivate func markReceiving(gen: Int) {
        lock.lock()
        guard gen == generation, !terminalReached, !receivingEmitted else {
            lock.unlock(); return
        }
        receivingEmitted = true
        let tracks = selected
        currentState = .receiving(tracks: tracks)
        let conts = Array(observers.values)
        for c in conts { c.yield(.receiving(tracks: tracks)) }
        lock.unlock()
    }

    private func removeObserver(_ id: Int) {
        lock.lock(); observers[id] = nil; lock.unlock()
    }

    // MARK: Generation-guarded helpers

    fileprivate func isCurrent(_ gen: Int) -> Bool {
        lock.lock(); defer { lock.unlock() }
        return gen == generation
    }

    private func setCurrentEndpoint(_ ep: MoQEndpoint, gen: Int) -> Bool {
        lock.lock(); defer { lock.unlock() }
        guard gen == generation else { return false }
        currentEndpoint = ep
        return true
    }

    private func setCurrentReceiver(_ r: MediaReceiver, selector: TrackSelector,
                                    gen: Int) -> Bool {
        lock.lock(); defer { lock.unlock() }
        guard gen == generation else { return false }
        currentReceiver = r
        currentSelector = selector
        return true
    }

    private func clearHandles(gen: Int) {
        lock.lock(); defer { lock.unlock() }
        guard gen == generation else { return }
        currentEndpoint = nil
        currentReceiver = nil
    }

    private func setSelected(_ tracks: [MediaTrack], gen: Int) {
        lock.lock(); defer { lock.unlock() }
        guard gen == generation else { return }
        selected = tracks
    }

    /// Whether `track` is selected, re-deriving the selection from the
    /// receiver's FRESHEST track snapshot (`tracks`) rather than trusting the
    /// discovery loop's last update — the object poll integrates a new track
    /// on the service thread before returning its first object, so that track
    /// can be present here before the (separate) discovery consumer observes
    /// its `.added` event. The freshly computed selection is stored so
    /// `selectedTracks` and the `.receiving` payload stay consistent.
    fileprivate func selectionIncludes(_ track: MediaTrack,
                                       from tracks: [MediaTrack], gen: Int) -> Bool {
        lock.lock(); defer { lock.unlock() }
        guard gen == generation, let selector = currentSelector else { return false }
        let selection = selector(tracks)
        selected = selection
        return selection.contains(track)
    }

    /// The current watch's receiver + its generation, for the object iterator
    /// to bind against.
    fileprivate func currentReceiverAndGeneration() -> (MediaReceiver, Int)? {
        lock.lock(); defer { lock.unlock() }
        guard let r = currentReceiver else { return nil }
        return (r, generation)
    }

    // MARK: Objects single-consumer claim

    fileprivate func claimObjectsConsumer() -> ObjectsConsumerToken {
        lock.lock(); defer { lock.unlock() }
        if objectsConsumerActive {
            return ObjectsConsumerToken(session: self, valid: false)
        }
        objectsConsumerActive = true
        return ObjectsConsumerToken(session: self, valid: true)
    }

    fileprivate func releaseObjectsConsumer() {
        lock.lock(); objectsConsumerActive = false; lock.unlock()
    }

    private static func mapError(_ error: Error) -> MoQServiceError {
        (error as? MoQServiceError) ?? .internalError(-1)
    }
}

// MARK: - Watch state

/// The lifecycle of one watch. SDK-generic (`receiving`, not `playing`);
/// rendering is app-level. Payloads carry the decisions an app needs.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum WatchState: Sendable, Equatable {
    /// Dialing; transport setup not yet complete.
    case connecting
    /// Transport established; the negotiated version is known.
    case established(version: MoQVersion?)
    /// Receiver attached; draining track discovery.
    case awaitingCatalog
    /// The catalog is complete but the selector matched nothing (soft — a
    /// later track can still lift this into `awaitingFirstObject`).
    case noMatchingTracks(catalog: [TrackDescription])
    /// Tracks selected; no object pulled yet.
    case awaitingFirstObject(tracks: [MediaTrack])
    /// The first selected object was pulled; media is flowing.
    case receiving(tracks: [MediaTrack])
    /// The streams ended cleanly after establishing.
    case ended
    /// Terminal failure, carried verbatim.
    case failed(MoQServiceError)
}

// MARK: - Track selection

/// Chooses which tracks to receive from the whole current snapshot, so
/// cross-track (`firstVideoAndAudio`) and future ABR/rendition logic express
/// directly.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct TrackSelector: Sendable {
    private let select: @Sendable ([MediaTrack]) -> [MediaTrack]

    public init(_ select: @escaping @Sendable ([MediaTrack]) -> [MediaTrack]) {
        self.select = select
    }

    public func callAsFunction(_ tracks: [MediaTrack]) -> [MediaTrack] {
        select(tracks)
    }

    /// The first video track (empty if none).
    public static let firstVideo = TrackSelector { tracks in
        tracks.first { $0.description.mediaType == .video }.map { [$0] } ?? []
    }

    /// The first audio track (empty if none).
    public static let firstAudio = TrackSelector { tracks in
        tracks.first { $0.description.mediaType == .audio }.map { [$0] } ?? []
    }

    /// The first video and the first audio track (either may be absent).
    public static let firstVideoAndAudio = TrackSelector { tracks in
        var out: [MediaTrack] = []
        if let v = tracks.first(where: { $0.description.mediaType == .video }) {
            out.append(v)
        }
        if let a = tracks.first(where: { $0.description.mediaType == .audio }) {
            out.append(a)
        }
        return out
    }

    /// Every track.
    public static let all = TrackSelector { $0 }
}

// MARK: - Object stream

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension LiveMediaSession {

    /// The selected tracks' media objects for the CURRENT watch: strictly
    /// pull-based (each `next()` pulls straight from the receiver's own object
    /// queue — no second buffer), filtered to the selected tracks. The first
    /// selected object pulled flips the watch into `.receiving`.
    ///
    /// Single active consumer: a second concurrent iterator throws
    /// ``MoQServiceError/wrongState``. Scoped to one watch — it returns `nil`
    /// once the watch ends or is superseded; re-iterate ``objects`` for the
    /// next watch (typically from the `awaitingFirstObject`/`receiving` state).
    /// Begin iterating once the watch has a receiver; an iterator created with
    /// no active watch ends immediately.
    public struct Objects: AsyncSequence, Sendable {
        public typealias Element = MediaObject
        let session: LiveMediaSession

        public func makeAsyncIterator() -> AsyncIterator {
            AsyncIterator(session: session,
                          token: session.claimObjectsConsumer())
        }

        public struct AsyncIterator: AsyncIteratorProtocol {
            let session: LiveMediaSession
            let token: ObjectsConsumerToken
            private var boundGen: Int?
            private var boundReceiver: MediaReceiver?
            private var underlying: MediaObjects.Iterator?

            init(session: LiveMediaSession, token: ObjectsConsumerToken) {
                self.session = session
                self.token = token
            }

            public mutating func next() async throws -> MediaObject? {
                try token.validate()
                if boundGen == nil {
                    guard let (receiver, gen) =
                        session.currentReceiverAndGeneration() else {
                        return nil       /* no active watch */
                    }
                    boundGen = gen
                    boundReceiver = receiver
                    underlying = receiver.objects.makeAsyncIterator()
                }
                let gen = boundGen!
                while true {
                    guard session.isCurrent(gen) else { return nil }
                    guard let object = try await underlying!.next() else {
                        return nil        /* this watch's objects ended */
                    }
                    /* Filter against the receiver's freshest track set: the
                     * object poll integrates a just-added track before it
                     * returns that track's first object, so re-deriving the
                     * selection here honors a track the discovery consumer has
                     * not observed yet. */
                    if session.selectionIncludes(
                        object.track, from: boundReceiver!.tracks, gen: gen) {
                        session.markReceiving(gen: gen)
                        return object
                    }
                    /* Not selected: drop. ARC releases the object's storage
                     * exactly once here (MediaObject.deinit). */
                }
            }
        }
    }
}

/// Enforces ``LiveMediaSession/objects``'s single-consumer contract. The
/// first iterator claims the slot; a second concurrent one is `invalid` and
/// throws on use. Released when the owning iterator is dropped.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class ObjectsConsumerToken: @unchecked Sendable {
    private let session: LiveMediaSession
    private let valid: Bool

    init(session: LiveMediaSession, valid: Bool) {
        self.session = session
        self.valid = valid
    }

    func validate() throws {
        guard valid else { throw MoQServiceError.wrongState }
    }

    deinit {
        if valid { session.releaseObjectsConsumer() }
    }
}
