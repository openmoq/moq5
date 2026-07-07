import Testing
import Foundation
@testable import MoQServiceCore

/* Deterministic LiveMediaSession tests: an injected scripted endpoint factory,
 * handshake waits (off-pool in async contexts), no sleeps, no network. Each
 * "watch" pairs a ScriptedEndpointBackend (drives connect/established/terminal)
 * with a ScriptedReceiverBackend (drives discovery/objects); the endpoint
 * backend is wrapped so it also vends the receiver backend as a
 * ReceiverBackendFactory -- exactly the seam the public MediaReceiver.attach
 * uses in production. */

// MARK: - Factory-endpoint backend (endpoint + receiver-backend factory)

/// Wraps a ``ScriptedEndpointBackend`` and additionally vends a preset
/// ``ScriptedReceiverBackend`` through ``ReceiverBackendFactory`` -- so the
/// public `MediaReceiver.attach(to:configuration:)` resolves a backend against
/// a scripted endpoint. (The bare ScriptedEndpointBackend deliberately does
/// NOT conform, so the "public attach throws .unsupported" test stays valid.)
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class FactoryEndpointBackend: EndpointBackend, ReceiverBackendFactory,
                                    @unchecked Sendable {
    let inner: ScriptedEndpointBackend
    private let receiverBackend: ScriptedReceiverBackend

    init(inner: ScriptedEndpointBackend, receiverBackend: ScriptedReceiverBackend) {
        self.inner = inner
        self.receiverBackend = receiverBackend
    }

    var state: MoQEndpoint.State { inner.state }
    var negotiatedVersion: MoQVersion? { inner.negotiatedVersion }
    var isFatal: Bool { inner.isFatal }
    var fatalCode: UInt64 { inner.fatalCode }
    var terminalFailure: TerminalFailure { inner.terminalFailure }
    func setInterrupted(_ value: Bool) { inner.setInterrupted(value) }
    func wake() { inner.wake() }
    func waitForActivity(timeoutMicroseconds t: UInt64) -> EndpointWaitResult {
        inner.waitForActivity(timeoutMicroseconds: t)
    }
    func drain(timeoutMicroseconds t: UInt64) -> EndpointDrainResult {
        inner.drain(timeoutMicroseconds: t)
    }
    func stop() { inner.stop() }
    func destroy() { inner.destroy() }

    func makeReceiverBackend(
        configuration: MediaReceiver.Configuration) throws -> any ReceiverBackend {
        receiverBackend
    }
}

// MARK: - One scripted watch + a factory that produces them

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class ScriptedWatch: @unchecked Sendable {
    let endpointBackend = ScriptedEndpointBackend()
    let receiverBackend: ScriptedReceiverBackend
    let backend: FactoryEndpointBackend
    let endpoint: MoQEndpoint

    init() {
        let eb = endpointBackend
        receiverBackend = ScriptedReceiverBackend(latch: { eb.isLatched })
        backend = FactoryEndpointBackend(inner: eb, receiverBackend: receiverBackend)
        endpoint = MoQEndpoint(
            configuration: .init(url: URL(string: "moqt://relay.test:4443")!),
            engine: EndpointEngine(backend: backend))
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class WatchFactory: @unchecked Sendable {
    private let cond = NSCondition()
    private var _watches: [ScriptedWatch] = []
    private let onCreate: (@Sendable (Int, ScriptedWatch) -> Void)?

    init(onCreate: (@Sendable (Int, ScriptedWatch) -> Void)? = nil) {
        self.onCreate = onCreate
    }

    var watches: [ScriptedWatch] {
        cond.lock(); defer { cond.unlock() }
        return _watches
    }

    /// The injected endpoint factory.
    var make: @Sendable (MoQEndpoint.Configuration) throws -> MoQEndpoint {
        { [self] _ in
            let watch = ScriptedWatch()
            cond.lock()
            let index = _watches.count
            _watches.append(watch)
            cond.broadcast()
            cond.unlock()
            onCreate?(index, watch)
            return watch.endpoint
        }
    }

    @discardableResult
    func awaitWatchCount(_ n: Int, ceiling: TimeInterval = 10) -> Bool {
        cond.lock(); defer { cond.unlock() }
        let deadline = Date().addingTimeInterval(ceiling)
        while _watches.count < n {
            if !cond.wait(until: deadline) { return false }
        }
        return true
    }

    @discardableResult
    func awaitWatchCount(_ n: Int, ceiling: TimeInterval = 10) async -> Bool {
        await offPool { self.awaitWatchCount(n, ceiling: ceiling) }
    }
}

// MARK: - State collector (a stateUpdates observer with handshake waits)

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class StateCollector: @unchecked Sendable {
    private let cond = NSCondition()
    private var _states: [WatchState] = []
    private let onEach: (@Sendable (WatchState) -> Void)?
    private var task: Task<Void, Never>!

    init(_ session: LiveMediaSession,
         onEach: (@Sendable (WatchState) -> Void)? = nil) {
        self.onEach = onEach
        self.task = Task { [weak self] in
            for await state in session.stateUpdates() {
                self?.append(state)
            }
        }
    }

    private func append(_ state: WatchState) {
        cond.lock()
        _states.append(state)
        cond.broadcast()
        cond.unlock()
        onEach?(state)
    }

    var all: [WatchState] {
        cond.lock(); defer { cond.unlock() }
        return _states
    }

    func cancel() { task.cancel() }

    @discardableResult
    func awaitStates(ceiling: TimeInterval = 10,
                     _ predicate: @escaping @Sendable ([WatchState]) -> Bool) -> Bool {
        cond.lock(); defer { cond.unlock() }
        let deadline = Date().addingTimeInterval(ceiling)
        while !predicate(_states) {
            if !cond.wait(until: deadline) { return false }
        }
        return true
    }

    @discardableResult
    func awaitStates(ceiling: TimeInterval = 10,
                     _ predicate: @escaping @Sendable ([WatchState]) -> Bool) async -> Bool {
        await offPool { self.awaitStates(ceiling: ceiling, predicate) }
    }
}

// MARK: - Ordered event log (teardown / connect ordering)

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class OrderedLog: @unchecked Sendable {
    private let lock = NSLock()
    private var entries: [String] = []

    func append(_ entry: String) {
        lock.lock(); entries.append(entry); lock.unlock()
    }
    var all: [String] {
        lock.lock(); defer { lock.unlock() }
        return entries
    }
    func firstIndex(_ entry: String) -> Int? {
        lock.lock(); defer { lock.unlock() }
        return entries.firstIndex(of: entry)
    }
    func lastIndex(_ entry: String) -> Int? {
        lock.lock(); defer { lock.unlock() }
        return entries.lastIndex(of: entry)
    }
}

// MARK: - Async gate + object recorder (for the two-pull race test)

/// A one-shot gate a driving Task can `await` on and the test opens. Blocks off
/// the cooperative pool so it never starves the executor.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class AsyncGate: @unchecked Sendable {
    private let cond = NSCondition()
    private var open = false

    func openGate() {
        cond.lock(); open = true; cond.broadcast(); cond.unlock()
    }
    func wait() async {
        await offPool {
            self.cond.lock()
            while !self.open { self.cond.wait() }
            self.cond.unlock()
        }
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class ObjectBox: @unchecked Sendable {
    private let cond = NSCondition()
    private var items: [MediaObject] = []

    func put(_ object: MediaObject?) {
        cond.lock(); if let object { items.append(object) }; cond.broadcast(); cond.unlock()
    }
    func item(_ index: Int) -> MediaObject? {
        cond.lock(); defer { cond.unlock() }
        return index < items.count ? items[index] : nil
    }
    @discardableResult
    func awaitCount(_ n: Int, ceiling: TimeInterval = 10) async -> Bool {
        await offPool {
            self.cond.lock(); defer { self.cond.unlock() }
            let deadline = Date().addingTimeInterval(ceiling)
            while self.items.count < n {
                if !self.cond.wait(until: deadline) { return false }
            }
            return true
        }
    }
}

// MARK: - Small matchers / factories

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func hasCase(_ states: [WatchState],
                     _ match: (WatchState) -> Bool) -> Bool {
    states.contains(where: match)
}

/// The tracks payload of an `.awaitingFirstObject`, else nil.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func awaitingTracks(_ state: WatchState) -> [MediaTrack]? {
    if case .awaitingFirstObject(let tracks) = state { return tracks }
    return nil
}

/// Cooperatively spin (never blocking a pool thread) until the session's
/// selected-track count reaches `n`. The count flips deterministically a few
/// scheduler turns after a wake integrates the track.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func awaitSelectedCount(_ session: LiveMediaSession, _ n: Int) async -> Bool {
    for _ in 0..<1_000_000 {
        if session.selectedTracks.count >= n { return true }
        await Task.yield()
    }
    return false
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func liveObject(_ handleID: UInt64, media: [UInt8], keyframe: Bool = false)
    -> (ReceiverPolledObject, MockObjectStorage) {
    let storage = MockObjectStorage(media: media)
    let polled = ReceiverPolledObject(
        handleID: handleID, storage: storage, isKeyframe: keyframe,
        endsGroup: false, isDatagram: false,
        presentationTime: .milliseconds(40), decodeTime: .milliseconds(40),
        compositionOffset: .zero, captureTime: nil)
    return (polled, storage)
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func liveConfig(namespace: MediaNamespace = "live/cam1",
                        selector: TrackSelector = .firstVideo)
    -> LiveMediaSession.Configuration {
    LiveMediaSession.Configuration(
        endpoint: .init(url: URL(string: "moqt://relay.test:4443")!),
        namespace: namespace, selector: selector)
}

// MARK: - Tests

@Suite("LiveMediaSession")
struct LiveMediaSessionTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Happy path: connecting -> established -> awaitingCatalog -> awaitingFirstObject -> receiving")
    func happyPath() async throws {
        let factory = WatchFactory()
        let session = LiveMediaSession(endpointFactory: factory.make)
        let collector = StateCollector(session)
        defer { collector.cancel() }

        session.start(liveConfig())
        #expect(await factory.awaitWatchCount(1))
        let watch = factory.watches[0]

        watch.endpointBackend.script(state: .established, version: .draft18)
        watch.receiverBackend.scriptEvents([.added(1, name: "video"), .catalogReady])
        watch.endpointBackend.wake()

        #expect(await collector.awaitStates {
            hasCase($0) { if case .awaitingFirstObject = $0 { true } else { false } }
        })
        /* Ordered prefix through discovery. */
        let prefix = collector.all
        #expect(prefix[0] == .connecting)
        #expect(prefix[1] == .established(version: .draft18))
        #expect(prefix[2] == .awaitingCatalog)
        #expect({ if case .awaitingFirstObject = prefix[3] { true } else { false } }())

        /* First selected object flips the watch into .receiving and yields. */
        let (polled, _) = liveObject(1, media: [1, 2, 3], keyframe: true)
        watch.receiverBackend.scriptObjects([polled])
        watch.endpointBackend.wake()

        var iterator = session.objects.makeAsyncIterator()
        let object = try await iterator.next()
        #expect(object?.mediaData == Data([1, 2, 3]))
        #expect(await collector.awaitStates {
            hasCase($0) { if case .receiving = $0 { true } else { false } }
        })

        await session.stop()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Reconnect serialization: A tears down (receiver then endpoint) before B factory + .connecting")
    func reconnectSerialization() async throws {
        let log = OrderedLog()
        let factory = WatchFactory(onCreate: { index, watch in
            log.append("create#\(index)")
            watch.receiverBackend.onDetach = { log.append("detach#\(index)") }
            watch.endpointBackend.onStop = { log.append("stop#\(index)") }
        })
        let session = LiveMediaSession(endpointFactory: factory.make)
        let collector = StateCollector(session, onEach: { state in
            if case .connecting = state { log.append("connecting") }
        })
        defer { collector.cancel() }

        // Watch A up to steady state.
        session.start(liveConfig())
        #expect(await factory.awaitWatchCount(1))
        let watchA = factory.watches[0]
        watchA.endpointBackend.script(state: .established, version: .draft18)
        watchA.receiverBackend.scriptEvents([.added(1, name: "video"), .catalogReady])
        watchA.endpointBackend.wake()
        #expect(await collector.awaitStates {
            hasCase($0) { if case .awaitingFirstObject = $0 { true } else { false } }
        })
        // A is parked in discovery; nothing torn down yet.
        #expect(watchA.receiverBackend.snapshot().detachCalls == 0)
        #expect(watchA.endpointBackend.snapshot().stopCount == 0)

        // Reconnect. B's factory runs only after A is fully torn down.
        session.start(liveConfig())
        #expect(await factory.awaitWatchCount(2))

        // At the moment B's endpoint was created, A must be torn down.
        let rA = watchA.receiverBackend.snapshot()
        let eA = watchA.endpointBackend.snapshot()
        #expect(rA.detachCalls >= 1)
        #expect(eA.stopCount >= 1)
        #expect(eA.destroyCount >= 1)
        #expect(rA.violations.isEmpty)
        #expect(eA.violations.isEmpty)

        // Teardown order: receiver detach BEFORE endpoint stop.
        let detachA = try #require(log.firstIndex("detach#0"))
        let stopA = try #require(log.firstIndex("stop#0"))
        #expect(detachA < stopA)

        // A teardown precedes B's factory call...
        let createB = try #require(log.firstIndex("create#1"))
        #expect(detachA < createB)
        #expect(stopA < createB)

        // ...and precedes B's .connecting emission (the last connecting seen).
        #expect(await collector.awaitStates {
            $0.filter { if case .connecting = $0 { true } else { false } }.count >= 2
        })
        let connectingB = try #require(log.lastIndex("connecting"))
        #expect(detachA < connectingB)
        #expect(stopA < connectingB)

        await session.stop()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("stop() suppresses post-stop state emissions and tears down")
    func stopSuppressesEmissions() async throws {
        let factory = WatchFactory()
        let session = LiveMediaSession(endpointFactory: factory.make)
        let collector = StateCollector(session)
        defer { collector.cancel() }

        session.start(liveConfig())
        #expect(await factory.awaitWatchCount(1))
        let watch = factory.watches[0]
        watch.endpointBackend.script(state: .established, version: .draft18)
        watch.receiverBackend.scriptEvents([.added(1, name: "video"), .catalogReady])
        watch.endpointBackend.wake()
        #expect(await collector.awaitStates {
            hasCase($0) { if case .awaitingFirstObject = $0 { true } else { false } }
        })

        await session.stop()
        let afterStop = collector.all

        // The old watch is gone: torn down, no current state.
        #expect(session.state == nil)
        #expect(watch.receiverBackend.snapshot().detachCalls >= 1)
        #expect(watch.endpointBackend.snapshot().stopCount >= 1)

        // Provoke the (dead) backend: it must not surface any new state.
        watch.receiverBackend.scriptEvents([.added(2, name: "audio")])
        watch.receiverBackend.scriptTerminal()
        watch.endpointBackend.wake()

        // No .ended / .failed / new state slipped through after stop.
        #expect(!hasCase(afterStop) { if case .ended = $0 { true } else { false } })
        #expect(!hasCase(afterStop) { if case .failed = $0 { true } else { false } })
        #expect(collector.all == afterStop)
        #expect(watch.receiverBackend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Terminal failure preserves connectionFailed(.certificateUnverified)")
    func terminalCertificateFailure() async throws {
        try await assertTerminal(
            .tlsCertificate(code: 0x2A),
            expected: .failed(.connectionFailed(
                ConnectionFailure(kind: .certificateUnverified, code: 0x2A))))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Terminal failure preserves connectionFailed(.transport)")
    func terminalTransportFailure() async throws {
        try await assertTerminal(
            .transport(code: 7),
            expected: .failed(.connectionFailed(
                ConnectionFailure(kind: .transport, code: 7))))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Clean close before setup maps to failed(.closed)")
    func cleanBeforeSetup() async throws {
        try await assertTerminal(.clean, expected: .failed(.closed))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    private func assertTerminal(_ reason: TerminalFailure,
                                expected: WatchState) async throws {
        let factory = WatchFactory()
        let session = LiveMediaSession(endpointFactory: factory.make)
        let collector = StateCollector(session)
        defer { collector.cancel() }

        session.start(liveConfig())
        #expect(await factory.awaitWatchCount(1))
        let watch = factory.watches[0]

        watch.endpointBackend.script(state: .closed, terminal: reason)

        #expect(await collector.awaitStates { $0.contains(expected) })
        await session.stop()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("noMatchingTracks on an audio-only catalog, then a late video -> awaitingFirstObject")
    func noMatchThenLateVideo() async throws {
        let factory = WatchFactory()
        let session = LiveMediaSession(endpointFactory: factory.make)
        let collector = StateCollector(session)
        defer { collector.cancel() }

        session.start(liveConfig(selector: .firstVideo))
        #expect(await factory.awaitWatchCount(1))
        let watch = factory.watches[0]

        watch.endpointBackend.script(state: .established, version: .draft18)
        watch.receiverBackend.scriptEvents([
            .added(1, name: "audio", mediaType: .audio), .catalogReady])
        watch.endpointBackend.wake()

        #expect(await collector.awaitStates {
            hasCase($0) { if case .noMatchingTracks = $0 { true } else { false } }
        })

        // A late video lifts noMatchingTracks into awaitingFirstObject.
        watch.receiverBackend.scriptEvents([.added(2, name: "video")])
        watch.endpointBackend.wake()
        #expect(await collector.awaitStates {
            hasCase($0) { if case .awaitingFirstObject = $0 { true } else { false } }
        })

        await session.stop()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Clean end after establishing -> ended")
    func cleanEnd() async throws {
        let factory = WatchFactory()
        let session = LiveMediaSession(endpointFactory: factory.make)
        let collector = StateCollector(session)
        defer { collector.cancel() }

        session.start(liveConfig())
        #expect(await factory.awaitWatchCount(1))
        let watch = factory.watches[0]

        watch.endpointBackend.script(state: .established, version: .draft18)
        watch.receiverBackend.scriptTerminal()      // clean terminal, no events
        watch.endpointBackend.wake()

        #expect(await collector.awaitStates {
            hasCase($0) { if case .ended = $0 { true } else { false } }
        })
        #expect(!hasCase(collector.all) { if case .failed = $0 { true } else { false } })

        await session.stop()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Objects stay selected-track filtered and pull-based")
    func objectsAreFilteredAndPull() async throws {
        let factory = WatchFactory()
        let session = LiveMediaSession(endpointFactory: factory.make)
        let collector = StateCollector(session)
        defer { collector.cancel() }

        session.start(liveConfig(selector: .firstVideo))
        #expect(await factory.awaitWatchCount(1))
        let watch = factory.watches[0]

        watch.endpointBackend.script(state: .established, version: .draft18)
        watch.receiverBackend.scriptEvents([
            .added(1, name: "video"),
            .added(2, name: "audio", mediaType: .audio),
            .catalogReady])
        watch.endpointBackend.wake()
        #expect(await collector.awaitStates {
            hasCase($0) { if case .awaitingFirstObject = $0 { true } else { false } }
        })

        // An unselected (audio) object, then a selected (video) object.
        let (audio, audioStore) = liveObject(2, media: [0xAA])
        let (video, _) = liveObject(1, media: [1, 2, 3])
        watch.receiverBackend.scriptObjects([audio, video])
        watch.endpointBackend.wake()

        var iterator = session.objects.makeAsyncIterator()
        let first = try await iterator.next()
        #expect(first?.mediaData == Data([1, 2, 3]))     // video, not audio
        #expect(first?.track === session.selectedTracks.first)
        #expect(audioStore.cleanupCount == 1)            // dropped audio freed

        await session.stop()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("firstVideoAndAudio selects exactly one video and one audio from the snapshot")
    func firstVideoAndAudioSelection() async throws {
        let factory = WatchFactory()
        let session = LiveMediaSession(endpointFactory: factory.make)
        let collector = StateCollector(session)
        defer { collector.cancel() }

        session.start(liveConfig(selector: .firstVideoAndAudio))
        #expect(await factory.awaitWatchCount(1))
        let watch = factory.watches[0]

        watch.endpointBackend.script(state: .established, version: .draft18)
        watch.receiverBackend.scriptEvents([
            .added(1, name: "video1"),
            .added(2, name: "audio1", mediaType: .audio),
            .added(3, name: "video2"),
            .catalogReady])
        watch.endpointBackend.wake()
        #expect(await collector.awaitStates {
            hasCase($0) { if case .awaitingFirstObject = $0 { true } else { false } }
        })

        #expect(session.receiver?.tracks.count == 3)
        let selected = session.selectedTracks
        #expect(selected.count == 2)
        #expect(selected.first?.description.mediaType == .video)
        #expect(selected.last?.description.mediaType == .audio)
        #expect(selected.first?.description.name == "video1")   // FIRST video
        #expect(selected.last?.description.name == "audio1")

        await session.stop()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("stateUpdates() replays the latest state to a fresh observer; multiple observers supported")
    func stateUpdatesReplayAndMulticast() async throws {
        let factory = WatchFactory()
        let session = LiveMediaSession(endpointFactory: factory.make)
        let first = StateCollector(session)
        defer { first.cancel() }

        session.start(liveConfig())
        #expect(await factory.awaitWatchCount(1))
        let watch = factory.watches[0]
        watch.endpointBackend.script(state: .established, version: .draft18)
        watch.receiverBackend.scriptEvents([.added(1, name: "video"), .catalogReady])
        watch.endpointBackend.wake()
        #expect(await first.awaitStates {
            hasCase($0) { if case .awaitingFirstObject = $0 { true } else { false } }
        })

        // A NEW observer must immediately replay the latest state.
        let second = StateCollector(session)
        defer { second.cancel() }
        #expect(await second.awaitStates { !$0.isEmpty })
        #expect({ if case .awaitingFirstObject = second.all.first { true } else { false } }())

        // Both observers see the next transition (.receiving) -> multicast.
        let (polled, _) = liveObject(1, media: [9])
        watch.receiverBackend.scriptObjects([polled])
        watch.endpointBackend.wake()
        var iterator = session.objects.makeAsyncIterator()
        _ = try await iterator.next()

        let isReceiving: @Sendable ([WatchState]) -> Bool = {
            hasCase($0) { if case .receiving = $0 { true } else { false } }
        }
        #expect(await first.awaitStates(isReceiving))
        #expect(await second.awaitStates(isReceiving))

        await session.stop()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("awaitingFirstObject is withheld until catalogReady, even with a matching track already added")
    func awaitingFirstObjectWaitsForCatalog() async throws {
        let factory = WatchFactory()
        let session = LiveMediaSession(endpointFactory: factory.make)
        let collector = StateCollector(session)
        defer { collector.cancel() }

        session.start(liveConfig())
        #expect(await factory.awaitWatchCount(1))
        let watch = factory.watches[0]

        // A matching track is added, but the catalog is NOT yet complete.
        watch.endpointBackend.script(state: .established, version: .draft18)
        watch.receiverBackend.scriptEvents([.added(1, name: "video")])
        watch.endpointBackend.wake()

        // Discovery integrates the track (selection updates) but the state
        // stays awaitingCatalog -- classification waits for catalogReady.
        #expect(await awaitSelectedCount(session, 1))
        #expect(session.state == .awaitingCatalog)
        #expect(!hasCase(collector.all) {
            if case .awaitingFirstObject = $0 { true } else { false }
        })

        // catalogReady lifts it into awaitingFirstObject.
        watch.receiverBackend.scriptEvents([.catalogReady])
        watch.endpointBackend.wake()
        #expect(await collector.awaitStates {
            hasCase($0) { if case .awaitingFirstObject = $0 { true } else { false } }
        })

        await session.stop()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("awaitingFirstObject refreshes when the selection grows before the first object")
    func awaitingFirstObjectRefreshesOnSelectionChange() async throws {
        let factory = WatchFactory()
        let session = LiveMediaSession(endpointFactory: factory.make)
        let collector = StateCollector(session)
        defer { collector.cancel() }

        session.start(liveConfig(selector: .firstVideoAndAudio))
        #expect(await factory.awaitWatchCount(1))
        let watch = factory.watches[0]

        // Catalog ready with video only -> awaitingFirstObject([video]).
        watch.endpointBackend.script(state: .established, version: .draft18)
        watch.receiverBackend.scriptEvents([.added(1, name: "video"), .catalogReady])
        watch.endpointBackend.wake()
        #expect(await collector.awaitStates {
            $0.contains { awaitingTracks($0)?.count == 1 }
        })
        let firstAwaiting = collector.all.compactMap(awaitingTracks).first
        #expect(firstAwaiting?.count == 1)
        #expect(firstAwaiting?.first?.description.mediaType == .video)

        // Late audio BEFORE any object -> awaitingFirstObject refreshes to
        // [video, audio].
        watch.receiverBackend.scriptEvents([.added(2, name: "audio", mediaType: .audio)])
        watch.endpointBackend.wake()
        #expect(await collector.awaitStates {
            $0.contains { tracks in
                guard let t = awaitingTracks(tracks), t.count == 2 else { return false }
                return t.contains { $0.description.mediaType == .video }
                    && t.contains { $0.description.mediaType == .audio }
            }
        })

        // A fresh observer replays the LATEST awaitingFirstObject payload.
        let fresh = StateCollector(session)
        defer { fresh.cancel() }
        #expect(await fresh.awaitStates { !$0.isEmpty })
        #expect(awaitingTracks(fresh.all.first ?? .connecting)?.count == 2)

        await session.stop()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A late .added(audio) + its first audio object is delivered under firstVideoAndAudio, even before discovery observes the track")
    func lateTrackObjectHonored() async throws {
        let factory = WatchFactory()
        let session = LiveMediaSession(endpointFactory: factory.make)
        let collector = StateCollector(session)
        defer { collector.cancel() }

        session.start(liveConfig(selector: .firstVideoAndAudio))
        #expect(await factory.awaitWatchCount(1))
        let watch = factory.watches[0]
        watch.endpointBackend.script(state: .established, version: .draft18)
        watch.receiverBackend.scriptEvents([.added(1, name: "video"), .catalogReady])
        let (videoObject, _) = liveObject(1, media: [1, 1, 1], keyframe: true)
        watch.receiverBackend.scriptObjects([videoObject])
        watch.endpointBackend.wake()
        #expect(await collector.awaitStates {
            hasCase($0) { if case .awaitingFirstObject = $0 { true } else { false } }
        })

        /* One driving Task, one iterator: pull the video, wait for the test to
         * stage the late audio, then pull the audio. */
        let gate = AsyncGate()
        let box = ObjectBox()
        let pull = Task {
            var iterator = session.objects.makeAsyncIterator()
            box.put(try? await iterator.next())      // video -> .receiving
            await gate.wait()
            box.put(try? await iterator.next())      // audio (the finding)
        }

        #expect(await box.awaitCount(1))             // video delivered
        #expect(await collector.awaitStates {
            hasCase($0) { if case .receiving = $0 { true } else { false } }
        })

        /* Stage the late audio track AND its first object, then freeze the
         * object poll mid-call. The demand job drains the track event (so
         * receiver.tracks gains audio) but blocks in pollObject before the
         * pass completes -- so the discovery loop cannot pop the .added(audio)
         * event, and the session's last-known selection stays stale. */
        watch.receiverBackend.setObjectGate(true)
        watch.receiverBackend.scriptEvents([.added(2, name: "audio", mediaType: .audio)])
        let (audioObject, _) = liveObject(2, media: [2, 2, 2])
        watch.receiverBackend.scriptObjects([audioObject])
        gate.openGate()

        #expect(await watch.receiverBackend.awaitObjectPollBlocked())
        /* Proof of the race the fix must survive: discovery has NOT yet added
         * the audio track to the selection. */
        #expect(session.selectedTracks.count == 1)
        #expect(session.selectedTracks.first?.description.mediaType == .video)

        watch.receiverBackend.setObjectGate(false)

        /* The fix re-derives selection from the receiver's freshest tracks, so
         * the audio object is delivered rather than dropped. */
        #expect(await box.awaitCount(2))
        #expect(box.item(1)?.mediaData == Data([2, 2, 2]))
        #expect(box.item(1)?.track.description.mediaType == .audio)

        pull.cancel()
        await session.stop()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("objects has a single consumer: a second concurrent iterator throws .wrongState")
    func objectsSingleConsumer() async throws {
        let factory = WatchFactory()
        let session = LiveMediaSession(endpointFactory: factory.make)
        let collector = StateCollector(session)
        defer { collector.cancel() }

        session.start(liveConfig())
        #expect(await factory.awaitWatchCount(1))
        let watch = factory.watches[0]
        watch.endpointBackend.script(state: .established, version: .draft18)
        watch.receiverBackend.scriptEvents([.added(1, name: "video"), .catalogReady])
        watch.endpointBackend.wake()
        #expect(await collector.awaitStates {
            hasCase($0) { if case .awaitingFirstObject = $0 { true } else { false } }
        })

        let first = session.objects.makeAsyncIterator()   // claims the slot
        var second = session.objects.makeAsyncIterator()  // invalid while first lives
        await #expect(throws: MoQServiceError.wrongState) {
            _ = try await second.next()
        }
        _ = first   // keep the valid iterator (and its claim) alive

        await session.stop()
    }
}
