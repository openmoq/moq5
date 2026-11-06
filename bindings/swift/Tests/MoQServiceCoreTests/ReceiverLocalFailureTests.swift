import Testing
import Foundation
@testable import MoQServiceCore

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private struct LocalFailureRig {
    let endpointBackend: ScriptedEndpointBackend
    let backend: ScriptedReceiverBackend
    let endpoint: MoQEndpoint
    let receiver: MediaReceiver

    init(stateGate: TestGate? = nil) throws {
        let endpointBackend = ScriptedEndpointBackend()
        self.endpointBackend = endpointBackend
        backend = ScriptedReceiverBackend(trackStateGate: stateGate, latch: { endpointBackend.isLatched })
        endpoint = MoQEndpoint(configuration: .init(url: URL(string: "moqt://relay.test:4443")!),
                               engine: EndpointEngine(backend: endpointBackend))
        receiver = try MediaReceiver.attach(to: endpoint, configuration: .live(namespace: "live/video"), backend: backend)
    }

    func stage(_ events: [ReceiverPolledEvent]) async throws {
        backend.scriptEvents(events)
        let attachment = receiver.attachment
        _ = try await endpoint.engine.perform { attachment.drainTrackEvents() }
    }

    func discover() async throws -> MediaTrack {
        try await stage([.added(7, name: "video")])
        var iterator = receiver.trackEvents.makeAsyncIterator()
        guard case .added(let track)? = try await iterator.next() else { throw MoQServiceError.wrongState }
        return track
    }
}

@Suite("Receiver local failure model")
struct ReceiverLocalFailureTests {
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Mixed legacy/fill/failure FIFO keeps stable identity without global terminal or rearm")
    func mixedEventsAndHealthyTrack() async throws {
        let rig = try LocalFailureRig()
        let failed = try await rig.discover()
        try await rig.stage([.added(8, name: "healthy", mediaType: .audio)])
        var legacy = rig.receiver.trackEvents.makeAsyncIterator()
        guard case .added(let healthy)? = try await legacy.next() else { Issue.record("missing healthy track"); return }
        let description = TrackDescription(name: "updated", mediaType: .video, packaging: .raw)
        try await rig.stage([
            .localFailure(999, result: -5), // no invented identity
            .localFailure(7, result: -5), .fill(7, requestID: 0),
            .updated(7, description), .localFailure(7, result: nil),
            .localFailure(7, result: 0), .catalogReady,
        ])
        #expect(rig.receiver.attachment.stagedTrackEventCount == 6)
        #expect(!rig.backend.isFatal && !rig.backend.isTerminal)
        #expect(rig.backend.snapshot().subscribeCalls.isEmpty && rig.backend.snapshot().unsubscribeCalls.isEmpty)
        let storage = MockObjectStorage(media: [1, 0, 255])
        rig.backend.scriptObjects([ReceiverPolledObject(
            handleID: 8, storage: storage, isKeyframe: true, endsGroup: false, isDatagram: false,
            presentationTime: .zero, decodeTime: .zero, compositionOffset: .zero, captureTime: nil)])
        var objects = rig.receiver.objects.makeAsyncIterator()
        var owned = try await objects.next()
        #expect(owned?.track === healthy && owned?.mediaData == Data([1, 0, 255]))
        var extended = rig.receiver.trackEventsExtended.makeAsyncIterator()
        guard case .localFailure(let firstTrack, let first)? = try await extended.next(),
              case .fillTerminal(let fillTrack, let fill)? = try await extended.next(),
              case .updated(let updated)? = try await legacy.next(),
              case .localFailure(let absentTrack, let absent)? = try await extended.next(),
              case .localFailure(let zeroTrack, let zero)? = try await extended.next(),
              case .catalogReady? = try await legacy.next() else {
            Issue.record("mixed queue order changed"); await rig.endpoint.close(); return
        }
        #expect(firstTrack === failed && first == -5)
        #expect(fillTrack === failed && fill.initiatingRequestID == 0)
        #expect(updated === failed && absentTrack === failed && absent == nil)
        #expect(zeroTrack === failed && zero == 0)
        #expect(failed.description.name == "updated" && rig.receiver.tracks == [failed, healthy])
        #expect(!rig.backend.isTerminal && !rig.backend.isFatal)
        await rig.receiver.close()
        await rig.endpoint.close()
        #expect(try await extended.next() == nil && rig.backend.snapshot().detachCalls == 1)
        #expect(storage.cleanupCount == 0)
        owned = nil
        #expect(storage.cleanupCount == 1)
        #expect(rig.backend.snapshot().subscribeCalls.isEmpty && rig.backend.snapshot().cancelFillCalls.isEmpty)
        #expect(rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Copied failures survive clean/fatal detach exactly once before terminal disposition")
    func detachedEvents() async throws {
        for fatal in [false, true] {
            let rig = try LocalFailureRig()
            let track = try await rig.discover()
            try await rig.stage([.localFailure(7, result: .min), .fill(7, requestID: .max), .ended(7)])
            rig.backend.scriptTerminal(fatal: fatal, code: 42)
            try await rig.stage([])
            await rig.receiver.close()
            await rig.endpoint.close()
            let polls = rig.backend.snapshot().pollCalls
            var extended = rig.receiver.trackEventsExtended.makeAsyncIterator()
            guard case .localFailure(let failed, let code)? = try await extended.next(),
                  case .fillTerminal(let parent, _)? = try await extended.next(),
                  case .legacy(.ended(let ended))? = try await extended.next() else {
                Issue.record("staged tail lost"); continue
            }
            #expect(failed === track && parent === track && ended === track && code == .min)
            if fatal {
                await #expect(throws: MoQServiceError.fatal(code: 42)) { _ = try await extended.next() }
            } else {
                #expect(try await extended.next() == nil)
                #expect(try await extended.next() == nil)
            }
            #expect(rig.backend.snapshot().pollCalls == polls)
            #expect(rig.backend.snapshot().detachCalls == 1 && rig.backend.snapshot().violations.isEmpty)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Failure records share the 64-entry cap and legacy filtering alone wakes object demand")
    func pressureAndCapacityWake() async throws {
        let rig = try LocalFailureRig()
        let track = try await rig.discover()
        // Synthetic distinct records stress FIFO capacity, not native failure cardinality.
        try await rig.stage((0..<64).map { .localFailure(7, result: Int32($0)) } + [.catalogReady])
        #expect(rig.receiver.attachment.stagedTrackEventCount == 64)
        #expect(rig.backend.snapshot().queuedEventsRemaining == 1)
        let storage = MockObjectStorage(media: [0x42])
        rig.backend.scriptObjects([ReceiverPolledObject(
            handleID: 7, storage: storage, isKeyframe: false, endsGroup: false, isDatagram: false,
            presentationTime: .zero, decodeTime: .zero, compositionOffset: .zero, captureTime: nil)])
        let polls = rig.backend.snapshot().pollCalls
        let attachment = rig.receiver.attachment
        for _ in 0..<3 {
            _ = try await rig.endpoint.engine.perform { attachment.drainTrackEvents() }
            _ = try await rig.endpoint.engine.perform { attachment.demandObject() }
        }
        #expect(rig.backend.snapshot().pollCalls == polls && rig.backend.snapshot().objectPollCalls == 0)
        let wakes = rig.endpointBackend.snapshot().wakeCount
        let waiter = Task {
            var iterator = rig.receiver.objects.makeAsyncIterator()
            let object = try await iterator.next()
            return (object?.track, object?.mediaData)
        }
        #expect(await rig.endpointBackend.awaitCondition { $0.wakeCount > wakes })
        try await rig.endpoint.engine.perform { }
        #expect(rig.backend.snapshot().objectPollCalls == 0)
        var legacy = rig.receiver.trackEvents.makeAsyncIterator()
        if case .catalogReady? = try await legacy.next() {} else { Issue.record("legacy tail missing") }
        let object = try await waiter.value
        #expect(object.0 === track && object.1 == Data([0x42]))
        #expect(rig.backend.snapshot().objectPollCalls == 1 && rig.backend.snapshot().queuedEventsRemaining == 0)
        #expect(attachment.stagedTrackEventCount == 0)
        await rig.endpoint.close()
        #expect(storage.cleanupCount == 1)
        var extended = rig.receiver.trackEventsExtended.makeAsyncIterator()
        #expect(try await extended.next() == nil) // filtered records are not replayed
        #expect(rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Both consumer collision orders retain one claim across local-failure skip and wait")
    func mixedConsumerClaims() async throws {
        for legacyFirst in [true, false] {
            let rig = try LocalFailureRig()
            _ = try await rig.discover()
            if legacyFirst { try await rig.stage([.localFailure(7, result: -9)]) }
            let wakes = rig.endpointBackend.snapshot().wakeCount
            let first = Task {
                if legacyFirst {
                    var iterator = rig.receiver.trackEvents.makeAsyncIterator()
                    _ = try await iterator.next()
                } else {
                    var iterator = rig.receiver.trackEventsExtended.makeAsyncIterator()
                    _ = try await iterator.next()
                }
            }
            #expect(await rig.endpointBackend.awaitCondition { $0.wakeCount > wakes })
            await #expect(throws: MoQServiceError.wrongState) {
                if legacyFirst {
                    var iterator = rig.receiver.trackEventsExtended.makeAsyncIterator()
                    _ = try await iterator.next()
                } else {
                    var iterator = rig.receiver.trackEvents.makeAsyncIterator()
                    _ = try await iterator.next()
                }
            }
            first.cancel()
            await #expect(throws: CancellationError.self) { try await first.value }
            try await rig.stage([.localFailure(7, result: -5), .catalogReady])
            var extended = rig.receiver.trackEventsExtended.makeAsyncIterator()
            if case .localFailure(_, let result)? = try await extended.next() {
                #expect(result == -5)
            } else { Issue.record("claim or queue position lost") }
            var legacy = rig.receiver.trackEvents.makeAsyncIterator()
            if case .catalogReady? = try await legacy.next() {} else { Issue.record("legacy tail lost") }
            await rig.endpoint.close()
            #expect(try await extended.next() == nil && rig.backend.snapshot().violations.isEmpty)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A cancelled legacy call consumes no queued failure and releases the shared claim")
    func cancellationBetweenSkips() async throws {
        let rig = try LocalFailureRig()
        _ = try await rig.discover()
        try await rig.stage((0..<32).map { .localFailure(7, result: Int32($0)) } + [.catalogReady])
        await rig.receiver.close()
        let consumer = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            var iterator = rig.receiver.trackEvents.makeAsyncIterator()
            _ = try await iterator.next()
        }
        await #expect(throws: CancellationError.self) { try await consumer.value }
        #expect(rig.receiver.attachment.stagedTrackEventCount == 33)
        var extended = rig.receiver.trackEventsExtended.makeAsyncIterator()
        if case .localFailure(_, let result)? = try await extended.next() {
            #expect(result == 0)
        } else { Issue.record("claim remained busy or the cancelled call consumed a record") }
        var legacy = rig.receiver.trackEvents.makeAsyncIterator()
        if case .catalogReady? = try await legacy.next() {} else { Issue.record("missing tail") }
        #expect(try await extended.next() == nil)
        await rig.endpoint.close()
        #expect(rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Shutdown does not promise an unpolled 65th local failure")
    func boundedShutdown() async throws {
        let rig = try LocalFailureRig()
        let track = try await rig.discover()
        try await rig.stage((0..<65).map { .localFailure(7, result: Int32($0)) })
        #expect(rig.receiver.attachment.stagedTrackEventCount == 64)
        await rig.endpoint.close()
        let polls = rig.backend.snapshot().pollCalls
        var iterator = rig.receiver.trackEventsExtended.makeAsyncIterator()
        for expected in 0..<64 {
            if case .localFailure(let observed, let result)? = try await iterator.next() {
                #expect(observed === track && result == Int32(expected))
            } else { Issue.record("staged failure lost") }
        }
        #expect(try await iterator.next() == nil)
        #expect(rig.backend.snapshot().queuedEventsRemaining == 1 && rig.backend.snapshot().pollCalls == polls)
        #expect(rig.backend.snapshot().detachCalls == 1 && rig.backend.snapshot().violations.isEmpty)
    }
}

@Suite("Receiver track state query")
struct ReceiverTrackStateTests {
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Every call queries current backend state on the executor, independently of staged events")
    func currentStateNotEventCache() async throws {
        let rig = try LocalFailureRig()
        let track = try await rig.discover()
        let states: [ExtendedTrackSubscriptionState] = [
            .legacy(.discovered), .legacy(.pending), .legacy(.active), .legacy(.pausedByApp),
            .legacy(.pausedByFlowControl), .legacy(.ended), .failed, .unknown(.max),
        ]
        for state in states {
            rig.backend.scriptTrackState(7, .success(state))
            #expect(try await rig.receiver.trackStateExtended(track) == state)
        }
        try await rig.stage([.localFailure(7, result: -9)])
        rig.backend.scriptTrackState(7, .success(.failed))
        rig.endpoint.interrupt() // snapshot queries are not wait-latch gated
        #expect(try await rig.receiver.trackStateExtended(track) == .failed)
        rig.endpoint.resume()
        #expect(rig.receiver.attachment.stagedTrackEventCount == 1)
        #expect(rig.backend.snapshot().trackStateCalls == Array(repeating: 7, count: states.count + 1))
        #expect(rig.backend.snapshot().subscribeCalls.isEmpty)
        await rig.endpoint.close()
        #expect(rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Query guards foreign tracks and detached receivers; backend refusals remain exact")
    func queryGuardsAndRefusals() async throws {
        let rig = try LocalFailureRig()
        let track = try await rig.discover()
        let foreign = MediaTrack(description: track.description)
        await #expect(throws: MoQServiceError.self) { _ = try await rig.receiver.trackStateExtended(foreign) }
        #expect(rig.backend.snapshot().trackStateCalls.isEmpty)
        let refusals: [MoQServiceError] = [.invalidArgument("native invalid"), .wrongState, .closed,
                                         .unsupported, .internalError(-9), .internalError(.min)]
        for refusal in refusals {
            rig.backend.scriptTrackState(7, .failure(refusal))
            await #expect(throws: refusal) { _ = try await rig.receiver.trackStateExtended(track) }
        }
        await rig.receiver.close()
        await #expect(throws: MoQServiceError.closed) { _ = try await rig.receiver.trackStateExtended(track) }
        await rig.endpoint.close()
        await #expect(throws: MoQServiceError.closed) { _ = try await rig.receiver.trackStateExtended(track) }
        #expect(rig.backend.snapshot().trackStateCalls.count == refusals.count)
        #expect(rig.backend.snapshot().detachCalls == 1 && rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Prestart query cancellation leaves the getter uncalled")
    func queryCancelledBeforeStart() async throws {
        let rig = try LocalFailureRig()
        let track = try await rig.discover()
        let gate = TestGate()
        #expect(rig.endpoint.engine.post { gate.pass() })
        #expect(await gate.awaitBlocked(count: 1))
        let wakes = rig.endpointBackend.snapshot().wakeCount
        let query = Task { try await rig.receiver.trackStateExtended(track) }
        #expect(await rig.endpointBackend.awaitCondition { $0.wakeCount > wakes })
        query.cancel()
        gate.openGate()
        await #expect(throws: CancellationError.self) { _ = try await query.value }
        await rig.endpoint.close()
        #expect(rig.backend.snapshot().trackStateCalls.isEmpty && rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A queued close preceding query is rechecked on the service thread")
    func closeBeforeQueryStart() async throws {
        let rig = try LocalFailureRig()
        let track = try await rig.discover()
        let gate = TestGate()
        #expect(rig.endpoint.engine.post { gate.pass() })
        #expect(await gate.awaitBlocked(count: 1))
        let wakes = rig.endpointBackend.snapshot().wakeCount
        let closing = Task { await rig.receiver.close() }
        #expect(await rig.endpointBackend.awaitCondition { $0.wakeCount > wakes })
        let queuedWakes = rig.endpointBackend.snapshot().wakeCount
        let query = Task { try await rig.receiver.trackStateExtended(track) }
        #expect(await rig.endpointBackend.awaitCondition { $0.wakeCount > queuedWakes })
        gate.openGate()
        await closing.value
        await #expect(throws: MoQServiceError.closed) { _ = try await query.value }
        await rig.endpoint.close()
        #expect(rig.backend.snapshot().trackStateCalls.isEmpty)
        #expect(rig.backend.snapshot().detachCalls == 1 && rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Started getter returns its actual value/refusal despite cancellation and queued close")
    func startedQueryOutcome() async throws {
        let outcomes: [Result<ExtendedTrackSubscriptionState, MoQServiceError>] = [
            .success(.failed), .success(.unknown(.max)), .failure(.internalError(-12345)),
        ]
        for outcome in outcomes {
            let gate = TestGate()
            let rig = try LocalFailureRig(stateGate: gate)
            let track = try await rig.discover()
            rig.backend.scriptTrackState(7, outcome)
            let query = Task { try await rig.receiver.trackStateExtended(track) }
            #expect(await gate.awaitBlocked(count: 1))
            query.cancel()
            let wakes = rig.endpointBackend.snapshot().wakeCount
            let closing = Task { await rig.receiver.close() }
            closing.cancel()
            #expect(await rig.endpointBackend.awaitCondition { $0.wakeCount > wakes })
            gate.openGate()
            switch outcome {
            case .success(let state): #expect(try await query.value == state)
            case .failure(let error):
                await #expect(throws: error) { _ = try await query.value }
            }
            await closing.value
            await rig.endpoint.close()
            #expect(rig.backend.snapshot().trackStateCalls == [7])
            #expect(rig.backend.snapshot().detachCalls == 1 && rig.backend.snapshot().violations.isEmpty)
        }
    }
}
