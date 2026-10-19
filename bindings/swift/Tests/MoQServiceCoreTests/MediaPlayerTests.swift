import Testing
import Foundation
@testable import MoQServiceCore

/* MediaPlayer over a scripted player backend and the scripted endpoint:
 * the attach transaction and hook ownership, staging, cancellation, and how
 * the player's deadlines drive the engine. No C, no network. */

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class ScriptedPlayerBackend: PlayerBackend, @unchecked Sendable {
    private let lock = NSLock()
    private var outputs: [PlayerOutput] = []
    private var closed = false
    private var deadline: UInt64?
    /// Overdue tasks: each tick retires the earliest due one.
    private var dueTasks: [UInt64] = []
    private var retired = 0
    private var ticks = 0
    private var destroys = 0
    var onDestroy: (@Sendable () -> Void)?

    func queue(_ items: [PlayerOutput]) {
        lock.lock(); defer { lock.unlock() }
        outputs.append(contentsOf: items)
    }
    func setDeadline(_ d: UInt64?) {
        lock.lock(); defer { lock.unlock() }
        deadline = d
    }
    func setDueTasks(_ tasks: [UInt64]) {
        lock.lock(); defer { lock.unlock() }
        dueTasks = tasks.sorted()
    }
    var retiredCount: Int { lock.lock(); defer { lock.unlock() }; return retired }
    func setClosed() {
        lock.lock(); defer { lock.unlock() }
        closed = true
    }
    var tickCount: Int { lock.lock(); defer { lock.unlock() }; return ticks }
    var destroyCount: Int { lock.lock(); defer { lock.unlock() }; return destroys }
    var queued: Int { lock.lock(); defer { lock.unlock() }; return outputs.count }

    func tick(nowMicroseconds: UInt64) -> PlayerTickResult {
        lock.lock(); defer { lock.unlock() }
        ticks += 1
        if let first = dueTasks.first, first <= nowMicroseconds {
            dueTasks.removeFirst()
            retired += 1
        }
        /* As the C player: outputs it still holds mean its queue is full. */
        return outputs.isEmpty ? .ok : .wouldBlock
    }
    func pollOutput() -> PlayerOutput? {
        lock.lock(); defer { lock.unlock() }
        return outputs.isEmpty ? nil : outputs.removeFirst()
    }
    func pollEvent() -> PlayerEvent? { nil }
    var nextDeadlineMicroseconds: UInt64? {
        lock.lock(); defer { lock.unlock() }
        return dueTasks.first ?? deadline
    }
    var isClosed: Bool {
        lock.lock(); defer { lock.unlock() }
        return closed && outputs.isEmpty
    }
    var fatalCode: UInt64? { nil }
    func select(trackID: UInt64, start: StartMode, independentTiming: Bool,
                sharedEpoch: Bool) throws {}
    func deselect(trackID: UInt64) throws {}
    func feedback(trackID: UInt64, generation: UInt64, kind: PlayerFeedback) throws {}
    func statistics() throws -> MediaPlayer.Statistics {
        MediaPlayer.Statistics(tracksSelected: 0, objectsTaken: 0, objectsPushed: 0,
                               objectsDropped: 0, objectsDiscarded: 0, unitsOutput: 0,
                               configuresOutput: 0, discontinuitiesOutput: 0, endsOutput: 0,
                               eventsDropped: 0, feedbackStale: 0, outputsBytes: 0,
                               selectionsFailed: 0)
    }
    func destroy() {
        lock.lock()
        destroys += 1
        let hook = onDestroy
        lock.unlock()
        hook?()
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func makeEndpoint(_ backend: ScriptedEndpointBackend) -> MoQEndpoint {
    MoQEndpoint(configuration: .init(url: URL(string: "moqt://relay.test:4443")!),
                engine: EndpointEngine(backend: backend))
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private let playerConfiguration = MediaPlayer.Configuration(namespace: "live/cam1")

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func unit(_ object: UInt64) -> PlayerOutput {
    .unit(PlayerUnit(track: PlayerTrack(id: 1, description: nil), configurationGeneration: 1,
                     timelineID: 1, groupID: 0, objectID: object, decodeTime: .zero,
                     presentationTime: .zero, isIndependent: true, sampleDuration: nil,
                     captureTime: nil, data: Data()))
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func objectID(_ output: PlayerOutput?) -> UInt64? {
    if case .unit(let u)? = output { return u.objectID }
    return nil
}

/// Poll `condition` from a pool thread, up to `ceiling` seconds.
private func eventually(_ ceiling: TimeInterval = 5,
                        _ condition: @escaping @Sendable () -> Bool) async -> Bool {
    let end = Date().addingTimeInterval(ceiling)
    while Date() < end {
        if condition() { return true }
        try? await Task.sleep(for: .milliseconds(5))
    }
    return condition()
}

@Suite("Media player attachment")
struct MediaPlayerAttachmentTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Shutdown between construction and the slot claim destroys the backend once, before the endpoint's")
    func shutdownAtTheClaim() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        let player = ScriptedPlayerBackend()
        let order = EventRecorder()
        player.onDestroy = { order.record("player destroyed") }
        backend.onStop = { order.record("endpoint stopped") }
        let engine = endpoint.engine
        await #expect(throws: MoQServiceError.closed) {
            _ = try await MediaPlayer.attach(to: endpoint, configuration: playerConfiguration) {
                engine.beginShutdown()          /* shutdown begins mid-transaction */
                return player
            }
        }
        await endpoint.close()
        #expect(player.destroyCount == 1)
        #expect(order.events == ["player destroyed", "endpoint stopped"])
        #expect(backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Closing a player again never removes a later player's hook")
    func closeKeepsTheLaterHook() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        let first = ScriptedPlayerBackend(), second = ScriptedPlayerBackend()
        let a = try await MediaPlayer.attach(to: endpoint, configuration: playerConfiguration) { first }
        await a.close()
        #expect(first.destroyCount == 1)
        let b = try await MediaPlayer.attach(to: endpoint, configuration: playerConfiguration) { second }
        await a.close()                                   /* again: a no-op */
        /* B's hook still runs: a nudge makes another pass that ticks it. */
        let before = second.tickCount
        endpoint.engine.nudgeWaiters()
        #expect(await eventually { second.tickCount > before })
        /* And its teardown still runs at shutdown, exactly once. */
        await endpoint.close()
        #expect(second.destroyCount == 1)
        _ = b
        #expect(backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A second attach while one player holds the slot is refused, and its backend destroyed")
    func secondAttachRefused() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        let first = ScriptedPlayerBackend(), second = ScriptedPlayerBackend()
        let a = try await MediaPlayer.attach(to: endpoint, configuration: playerConfiguration) { first }
        await #expect(throws: MoQServiceError.wrongState) {
            _ = try await MediaPlayer.attach(to: endpoint, configuration: playerConfiguration) { second }
        }
        #expect(second.destroyCount == 1)
        #expect(first.destroyCount == 0)
        await a.close()
        await endpoint.close()
    }
}

@Suite("Media player staging and cancellation")
struct MediaPlayerStagingTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A cancelled consumer takes nothing: the staged record stays for the next")
    func cancelledConsumerTakesNothing() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        let fake = ScriptedPlayerBackend()
        fake.queue([unit(7)])
        let player = try await MediaPlayer.attach(to: endpoint, configuration: playerConfiguration) { fake }
        #expect(await eventually { player.attachment.outputQueue.hasItemOrEnd })
        let cancelled = Task { () async throws -> PlayerOutput? in
            while !Task.isCancelled { await Task.yield() }
            return try await player.nextOutput()
        }
        cancelled.cancel()
        await #expect(throws: CancellationError.self) { _ = try await cancelled.value }
        let staged = player.attachment.outputQueue.hasItemOrEnd
        #expect(staged)                                          /* still staged */
        if staged { #expect(objectID(try await player.nextOutput()) == 7) }
        await player.close()
        await endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A parked consumer is cancelled")
    func parkedConsumerCancelled() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        let fake = ScriptedPlayerBackend()
        let player = try await MediaPlayer.attach(to: endpoint, configuration: playerConfiguration) { fake }
        let parked = Task { try await player.nextOutput() }
        try await Task.sleep(for: .milliseconds(50))
        parked.cancel()
        await #expect(throws: CancellationError.self) { _ = try await parked.value }
        fake.queue([unit(3)])
        endpoint.engine.nudgeWaiters()
        #expect(objectID(try await player.nextOutput()) == 3)
        await player.close()
        await endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Outputs end after what is staged once the backend closes")
    func endsAfterStaged() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        let fake = ScriptedPlayerBackend()
        fake.queue([unit(0), unit(1)])
        fake.setClosed()
        let player = try await MediaPlayer.attach(to: endpoint, configuration: playerConfiguration) { fake }
        #expect(objectID(try await player.nextOutput()) == 0)
        #expect(objectID(try await player.nextOutput()) == 1)
        #expect(try await player.nextOutput() == nil)
        await player.close()
        await endpoint.close()
    }
}

@Suite("Media player deadlines")
struct MediaPlayerDeadlineTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A closed endpoint's park still wakes for the player's deadline")
    func closedParkHonorsDeadline() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        let fake = ScriptedPlayerBackend()
        let player = try await MediaPlayer.attach(to: endpoint, configuration: playerConfiguration) { fake }
        backend.script(state: .closed)
        endpoint.engine.nudgeWaiters()
        #expect(await eventually { fake.tickCount > 0 })
        try await Task.sleep(for: .milliseconds(50))   /* parked on the closed backend */
        let before = fake.tickCount
        fake.setDeadline(EndpointEngine.monotonicMicroseconds() + 80_000)
        endpoint.engine.nudgeWaiters()                 /* one pass reads the deadline */
        #expect(await eventually { fake.tickCount > before })
        let afterRead = fake.tickCount
        /* No further wake: the deadline alone must bring the next pass. */
        #expect(await eventually(2) { fake.tickCount > afterRead })
        await player.close()
        await endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Two overdue tasks retired one per tick both run, with staging below capacity and no wake",
          arguments: [[UInt64(1), 2], [1, 1]])
    func overdueTasksEachRun(_ due: [UInt64]) async throws {
        let backend = ScriptedEndpointBackend()
        backend.honorsTimeouts = true
        let endpoint = makeEndpoint(backend)
        let fake = ScriptedPlayerBackend()
        let player = try await MediaPlayer.attach(to: endpoint, configuration: playerConfiguration) { fake }
        #expect(await eventually { fake.tickCount > 0 })
        try await Task.sleep(for: .milliseconds(50))
        fake.setDueTasks(due)
        endpoint.engine.nudgeWaiters()                 /* one wake; the rest is the deadline's */
        #expect(await eventually(2) { fake.retiredCount == 2 })
        /* Then quiet: no deadline, so no spin. */
        let entries = backend.snapshot().waitEntries
        try await Task.sleep(for: .milliseconds(200))
        #expect(backend.snapshot().waitEntries - entries <= 2)
        await player.close()
        await endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A deadline the tick cannot retire under full staging parks, and a consumer's pop resumes")
    func unretiredDeadlineParks() async throws {
        let backend = ScriptedEndpointBackend()
        backend.honorsTimeouts = true
        let endpoint = makeEndpoint(backend)
        let fake = ScriptedPlayerBackend()
        fake.queue((0..<UInt64(PlayerAttachment.outputStaging + 8)).map(unit))
        fake.setDeadline(1)                            /* long due, never retired */
        let player = try await MediaPlayer.attach(to: endpoint, configuration: playerConfiguration) { fake }
        #expect(await eventually { fake.queued == 8 })  /* staging full */
        try await Task.sleep(for: .milliseconds(50))
        let entries = backend.snapshot().waitEntries
        try await Task.sleep(for: .milliseconds(200))
        #expect(backend.snapshot().waitEntries - entries <= 2)
        let ticks = fake.tickCount
        #expect(objectID(try await player.nextOutput()) == 0)
        #expect(await eventually { fake.tickCount > ticks && fake.queued == 7 })
        await player.close()
        await endpoint.close()
    }
}
