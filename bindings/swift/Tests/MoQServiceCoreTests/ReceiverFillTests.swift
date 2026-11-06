import Testing
import Foundation
@testable import MoQServiceCore

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private struct FillRig {
    let endpointBackend: ScriptedEndpointBackend
    let backend: ScriptedReceiverBackend
    let endpoint: MoQEndpoint
    let receiver: MediaReceiver

    init(cancelGate: TestGate? = nil) throws {
        let endpointBackend = ScriptedEndpointBackend()
        self.endpointBackend = endpointBackend
        backend = ScriptedReceiverBackend(cancelFillGate: cancelGate, latch: { endpointBackend.isLatched })
        endpoint = MoQEndpoint(configuration: .init(url: URL(string: "moqt://relay.test:4443")!),
                               engine: EndpointEngine(backend: endpointBackend))
        receiver = try MediaReceiver.attach(to: endpoint, configuration: .live(namespace: "live/video"),
                                             backend: backend)
    }

    func stage(_ events: [ReceiverPolledEvent]) async throws {
        backend.scriptEvents(events)
        let attachment = receiver.attachment
        _ = try await endpoint.engine.perform { attachment.drainTrackEvents() }
    }

    func discover() async throws -> MediaTrack {
        try await stage([.added(7, name: "video")])
        var iterator = receiver.trackEvents.makeAsyncIterator()
        guard case .added(let track)? = try await iterator.next() else {
            throw MoQServiceError.wrongState
        }
        return track
    }
}

@Suite("Receiver fill model")
struct ReceiverFillTests {
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Fill terminals consume the same bounded staging slots as legacy events")
    func stagesFill() {
        let backend = ScriptedReceiverBackend()
        let attachment = ReceiverAttachment(backend: backend)
        backend.scriptEvents([
            .added(7, name: "video"),
            ReceiverPolledEvent(kind: .fillTerminal(.init(
                initiatingRequestID: 0, reason: .complete,
                errorDomain: .some(.none), errorCode: 0)),
                handleID: 7, trackDescription: nil),
            .catalogReady,
        ])
        #expect(!attachment.drainTrackEvents())
        #expect(attachment.stagedTrackEventCount == 3)
        #expect(backend.snapshot().pollCalls == 4)
        attachment.finalizeDetach()
        #expect(backend.snapshot().detachCalls == 1)
        #expect(backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Old subscription calls keep defaults; explicit options reach the service thread unchanged")
    func subscriptionCommands() async throws {
        let rig = try FillRig()
        let track = try await rig.discover()
        try await rig.receiver.subscribe(track)
        let options = MediaTrackSubscriptionOptions(
            location: .some(.none), fill: .init(subscriberPriority: 0, groupOrder: .descending,
                                               timeoutMilliseconds: .max, location: .relativeGroup(offset: .max)))
        try await rig.receiver.subscribe(track, start: .nextGroup, priority: .max, options: options)
        #expect(rig.backend.snapshot().subscribeCalls == [
            .init(handleID: 7, start: .current, priority: nil),
            .init(handleID: 7, start: .nextGroup, priority: .max, options: options),
        ])
        rig.backend.scriptSubscribeResults([.nativeFailure(-11)])
        await #expect(throws: MoQServiceError.internalError(-11)) {
            try await rig.receiver.subscribe(track, options: options)
        }
        await rig.endpoint.close()
        #expect(rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Switching event views consumes one ordered queue with stable track identity after detach")
    func identityAndSequentialViews() async throws {
        let rig = try FillRig()
        let description = TrackDescription(name: "updated", mediaType: .video, packaging: .raw)
        try await rig.stage([
            .fill(999, requestID: 9), // unknown handle: no invented parent
            .added(7, name: "video"),
            .fill(7, requestID: 0, reason: .peerCancelledOrReset, domain: .stream),
            .fill(7, requestID: .max, reason: .unknown(.max), domain: .unknown(.max), code: .max),
            .updated(7, description), .catalogReady,
        ])
        #expect(rig.receiver.attachment.stagedTrackEventCount == 5)
        #expect(!rig.backend.isFatal && !rig.backend.isTerminal)
        await rig.receiver.close()
        await rig.endpoint.close()
        let pollsAtDetach = rig.backend.snapshot().pollCalls
        var legacy = rig.receiver.trackEvents.makeAsyncIterator()
        var extended = rig.receiver.trackEventsExtended.makeAsyncIterator()
        guard case .added(let track)? = try await legacy.next() else {
            Issue.record("missing add"); return
        }
        guard case .fillTerminal(let firstTrack, let first)? = try await extended.next(),
              case .fillTerminal(let secondTrack, let second)? = try await extended.next(),
              case .legacy(.updated(let updatedTrack))? = try await extended.next() else {
            Issue.record("mixed FIFO order changed"); return
        }
        #expect(track === firstTrack && firstTrack === secondTrack && secondTrack === updatedTrack)
        #expect(first == MediaFillTerminal(initiatingRequestID: 0, reason: .peerCancelledOrReset,
                                          errorDomain: .stream, errorCode: 0))
        #expect(second.initiatingRequestID == .max && second.reason == .unknown(.max))
        #expect(second.errorDomain == .unknown(.max) && second.errorCode == .max)
        #expect(track.description.name == "updated" && rig.receiver.tracks == [track])
        if case .catalogReady? = try await legacy.next() {} else { Issue.record("legacy tail lost") }
        #expect(try await extended.next() == nil)
        #expect(try await legacy.next() == nil)
        #expect(rig.backend.snapshot().pollCalls == pollsAtDetach)
        #expect(rig.backend.snapshot().detachCalls == 1 && rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Accepted cancel intent through the latch does not synthesize a terminal or invalidate an object")
    func cancelLatchAndPeerRace() async throws {
        let rig = try FillRig()
        let track = try await rig.discover()
        let storage = MockObjectStorage(media: [1, 2, 3])
        rig.backend.scriptObjects([ReceiverPolledObject(
            handleID: 7, storage: storage, isKeyframe: true, endsGroup: false, isDatagram: false,
            presentationTime: .zero, decodeTime: .zero, compositionOffset: .zero, captureTime: nil,
            delivery: .fill(initiatingRequestID: 0))])
        var objects = rig.receiver.objects.makeAsyncIterator()
        var owned = try await objects.next()
        #expect(owned != nil)
        rig.endpoint.interrupt()
        try await rig.receiver.cancelFill(track, initiatingRequestID: 0)
        try await rig.receiver.cancelFill(track, initiatingRequestID: 0)
        try await rig.receiver.cancelFill(track, initiatingRequestID: .max)
        #expect(rig.receiver.attachment.stagedTrackEventCount == 0)
        #expect(rig.backend.snapshot().cancelFillCalls == [
            .init(handleID: 7, initiatingRequestID: 0), .init(handleID: 7, initiatingRequestID: 0),
            .init(handleID: 7, initiatingRequestID: .max),
        ])
        try await rig.stage([.fill(7, requestID: 0, reason: .peerCancelledOrReset, domain: .stream),
                             .fill(7, requestID: .max, reason: .localCancelled)])
        var events = rig.receiver.trackEventsExtended.makeAsyncIterator()
        guard case .fillTerminal(let firstTrack, let terminal)? = try await events.next(),
              case .fillTerminal(let secondTrack, _)? = try await events.next() else {
            Issue.record("missing scripted winning terminals"); await rig.endpoint.close(); return
        }
        #expect(firstTrack === track && secondTrack === track)
        #expect(terminal.reason == .peerCancelledOrReset && terminal.errorCode == 0)
        await #expect(throws: MoQServiceError.interrupted) { _ = try await events.next() }
        try await rig.receiver.subscribe(track) // parent still usable, even latched
        #expect(!rig.backend.isFatal && !rig.backend.isTerminal)
        await rig.receiver.close()
        await rig.endpoint.close()
        #expect(owned?.mediaData == Data([1, 2, 3]))
        #expect(owned?.delivery == .fill(initiatingRequestID: 0))
        #expect(storage.cleanupCount == 0)
        owned = nil
        #expect(storage.cleanupCount == 1)
        #expect(rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Foreign handles never call the backend; cancel refusals preserve raw codes")
    func cancelRefusals() async throws {
        let rig = try FillRig()
        let track = try await rig.discover()
        let foreign = MediaTrack(description: track.description)
        await #expect(throws: MoQServiceError.self) {
            try await rig.receiver.cancelFill(foreign, initiatingRequestID: 0)
        }
        #expect(rig.backend.snapshot().cancelFillCalls.isEmpty)
        let refusals: [(ReceiverCommandResult, MoQServiceError)] = [
            (.nativeFailure(-6), .internalError(-6)), (.nativeFailure(-11), .internalError(-11)),
            (.nativeFailure(.min), .internalError(.min)), (.wrongState, .wrongState),
            (.closed, .closed), (.unsupported, .unsupported),
        ]
        for (result, error) in refusals {
            rig.backend.scriptCancelFillResults([result])
            await #expect(throws: error) { try await rig.receiver.cancelFill(track, initiatingRequestID: .max) }
        }
        await rig.receiver.close()
        let before = rig.backend.snapshot().cancelFillCalls.count
        await #expect(throws: MoQServiceError.closed) {
            try await rig.receiver.cancelFill(track, initiatingRequestID: 0)
        }
        #expect(rig.backend.snapshot().cancelFillCalls.count == before)
        await rig.endpoint.close()
        #expect(rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Cancellation before command start leaves the native intent uncalled")
    func cancelledBeforeStart() async throws {
        let rig = try FillRig()
        let track = try await rig.discover()
        let gate = TestGate()
        #expect(rig.endpoint.engine.post { gate.pass() })
        #expect(await gate.awaitBlocked(count: 1))
        let wakes = rig.endpointBackend.snapshot().wakeCount
        let command = Task { try await rig.receiver.cancelFill(track, initiatingRequestID: .max) }
        #expect(await rig.endpointBackend.awaitCondition { $0.wakeCount > wakes })
        command.cancel()
        await #expect(throws: CancellationError.self) { try await command.value }
        gate.openGate()
        try await rig.endpoint.engine.perform { }
        #expect(rig.backend.snapshot().cancelFillCalls.isEmpty)
        await rig.endpoint.close()
        #expect(rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A started cancel returns its actual result before one cancellation-immune detach")
    func cancelledAfterStartAndClose() async throws {
        for stale in [false, true] {
            let gate = TestGate()
            let rig = try FillRig(cancelGate: gate)
            let track = try await rig.discover()
            rig.backend.scriptCancelFillResults([stale ? .nativeFailure(-6) : .ok])
            let command = Task { try await rig.receiver.cancelFill(track, initiatingRequestID: 0) }
            #expect(await gate.awaitBlocked(count: 1))
            command.cancel()
            let wakes = rig.endpointBackend.snapshot().wakeCount
            let close = Task { await rig.receiver.close() }
            close.cancel()
            #expect(await rig.endpointBackend.awaitCondition { $0.wakeCount > wakes })
            gate.openGate()
            if stale {
                await #expect(throws: MoQServiceError.internalError(-6)) { try await command.value }
            } else {
                try await command.value
            }
            await close.value
            await rig.endpoint.close()
            #expect(rig.backend.snapshot().cancelFillCalls == [.init(handleID: 7, initiatingRequestID: 0)])
            #expect(rig.backend.snapshot().detachCalls == 1)
            #expect(rig.backend.snapshot().violations.isEmpty)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Mixed next calls exclude each other in both orders and cancellation releases the shared claim")
    func mixedConsumers() async throws {
        for legacyFirst in [true, false] {
            let rig = try FillRig()
            _ = try await rig.discover()
            if legacyFirst { try await rig.stage([.fill(7, requestID: 0)]) }
            let wakes = rig.endpointBackend.snapshot().wakeCount
            let first = Task {
                if legacyFirst {
                    var iterator = rig.receiver.trackEvents.makeAsyncIterator()
                    _ = try await iterator.next() // skips fill, still owns the claim while waiting
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
            try await rig.stage([.fill(7, requestID: 1), .catalogReady])
            var extended = rig.receiver.trackEventsExtended.makeAsyncIterator()
            if case .fillTerminal(_, let terminal)? = try await extended.next() {
                #expect(terminal.initiatingRequestID == 1) // no replay of the filtered ID0
            } else { Issue.record("extended view lost its queue position") }
            var legacy = rig.receiver.trackEvents.makeAsyncIterator()
            if case .catalogReady? = try await legacy.next() {} else { Issue.record("legacy queue position lost") }
            await rig.endpoint.close()
            #expect(try await extended.next() == nil)
            #expect(rig.backend.snapshot().violations.isEmpty)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A cancelled legacy call consumes no queued record")
    func cancellationBetweenFilteredRecords() async throws {
        let rig = try FillRig()
        _ = try await rig.discover()
        try await rig.stage((0..<32).map { .fill(7, requestID: UInt64($0)) } + [.catalogReady])
        await rig.receiver.close()
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            var iterator = rig.receiver.trackEvents.makeAsyncIterator()
            _ = try await iterator.next()
        }
        await #expect(throws: CancellationError.self) { try await task.value }
        #expect(rig.receiver.attachment.stagedTrackEventCount == 33)
        var extended = rig.receiver.trackEventsExtended.makeAsyncIterator()
        if case .fillTerminal(_, let terminal)? = try await extended.next() {
            #expect(terminal.initiatingRequestID == 0)
        } else { Issue.record("filtered cancellation failed to release the claim") }
        var legacy = rig.receiver.trackEvents.makeAsyncIterator()
        if case .catalogReady? = try await legacy.next() {} else { Issue.record("legacy tail lost") }
        #expect(try await extended.next() == nil)
        await rig.endpoint.close()
        #expect(rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A running fill reaches the service unchanged; refusals map without staging anything")
    func runningFillCommand() async throws {
        let rig = try FillRig()
        let track = try await rig.discover()
        let options = MediaFillOptions(subscriberPriority: 77, groupOrder: .descending,
                                       timeoutMilliseconds: 1234, location: .relativeGroup(offset: 2))
        try await rig.receiver.fill(track, options: options)
        #expect(rig.backend.snapshot().fillTrackCalls == [.init(handleID: 7, options: options)])
        #expect(rig.receiver.attachment.stagedTrackEventCount == 0)
        let foreign = MediaTrack(description: track.description)
        await #expect(throws: MoQServiceError.self) { try await rig.receiver.fill(foreign, options: options) }
        let refusals: [(ReceiverCommandResult, MoQServiceError)] = [
            (.wouldBlock, .wouldBlock), (.wrongState, .wrongState), (.closed, .closed),
            (.unsupported, .unsupported), (.nativeFailure(-14), .internalError(-14)),
        ]
        for (result, error) in refusals {
            rig.backend.scriptFillTrackResults([result])
            await #expect(throws: error) { try await rig.receiver.fill(track, options: .init()) }
        }
        #expect(rig.backend.snapshot().fillTrackCalls.count == 1 + refusals.count)
        await rig.receiver.close()
        await #expect(throws: MoQServiceError.closed) { try await rig.receiver.fill(track, options: options) }
        #expect(rig.backend.snapshot().fillTrackCalls.count == 1 + refusals.count)
        await rig.endpoint.close()
        #expect(rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A staged receipt holds the command slot until the event consumer dequeues it")
    func receiptHoldsSlotUntilDequeued() async throws {
        let rig = try FillRig()
        let track = try await rig.discover()
        try await rig.receiver.fill(track, options: .init())
        /* A wait cancelled before the receipt arrives consumes nothing. */
        let wakes = rig.endpointBackend.snapshot().wakeCount
        let waiter = Task {
            var iterator = rig.receiver.trackEventsExtended.makeAsyncIterator()
            _ = try await iterator.next()
        }
        #expect(await rig.endpointBackend.awaitCondition { $0.wakeCount > wakes })
        waiter.cancel()
        await #expect(throws: CancellationError.self) { try await waiter.value }
        /* The eager drain frees the native slot; the binding still holds it. */
        try await rig.stage([.created(7, requestID: 12), .catalogReady])
        await #expect(throws: MoQServiceError.wouldBlock) { try await rig.receiver.fill(track, options: .init()) }
        #expect(rig.backend.snapshot().fillTrackCalls.count == 1)
        var events = rig.receiver.trackEventsExtended.makeAsyncIterator()
        guard case .fillCreated(let created, let id)? = try await events.next() else {
            Issue.record("missing receipt"); await rig.endpoint.close(); return
        }
        #expect(created === track && id == 12)
        try await rig.receiver.fill(track, options: .init())
        #expect(rig.backend.snapshot().fillTrackCalls.count == 2)
        if case .legacy(.catalogReady)? = try await events.next() {} else { Issue.record("tail lost") }
        await rig.endpoint.close()
        #expect(rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A cancelled call on either view leaves a staged receipt and its command slot")
    func cancelledCallsKeepReceipt() async throws {
        for extendedView in [false, true] {
            let rig = try FillRig()
            let track = try await rig.discover()
            try await rig.receiver.fill(track, options: .init())
            try await rig.stage([.created(7, requestID: 21)])
            let task = Task {
                withUnsafeCurrentTask { $0?.cancel() }
                if extendedView {
                    var iterator = rig.receiver.trackEventsExtended.makeAsyncIterator()
                    _ = try await iterator.next()
                } else {
                    var iterator = rig.receiver.trackEvents.makeAsyncIterator()
                    _ = try await iterator.next()
                }
            }
            await #expect(throws: CancellationError.self) { try await task.value }
            /* A lost receipt would leave the reads below waiting forever. */
            guard rig.receiver.attachment.stagedTrackEventCount == 1 else {
                Issue.record("a cancelled call consumed the receipt")
                await rig.endpoint.close(); return
            }
            await #expect(throws: MoQServiceError.wouldBlock) {
                try await rig.receiver.fill(track, options: .init())
            }
            #expect(rig.backend.snapshot().fillTrackCalls.count == 1)
            var events = rig.receiver.trackEventsExtended.makeAsyncIterator()
            guard case .fillCreated(let created, let id)? = try await events.next() else {
                Issue.record("receipt lost to a cancelled call"); await rig.endpoint.close(); return
            }
            #expect(created === track && id == 21)
            try await rig.receiver.fill(track, options: .init())
            #expect(rig.backend.snapshot().fillTrackCalls.count == 2)
            await rig.endpoint.close()
            #expect(rig.backend.snapshot().violations.isEmpty)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Dequeuing a receipt frees only its own command and its own track")
    func receiptFreesOnlyItsCommand() async throws {
        let rig = try FillRig()
        let video = try await rig.discover()
        try await rig.stage([.added(8, name: "audio", mediaType: .audio)])
        var legacy = rig.receiver.trackEvents.makeAsyncIterator()
        guard case .added(let audio)? = try await legacy.next() else {
            Issue.record("missing audio"); await rig.endpoint.close(); return
        }
        try await rig.receiver.fill(video, options: .init())
        try await rig.receiver.fill(audio, options: .init())
        /* A receipt the binding did not issue (no command) stages without a slot. */
        try await rig.stage([.created(7, requestID: 1), .created(8, requestID: 2), .created(7, requestID: nil)])
        await #expect(throws: MoQServiceError.wouldBlock) { try await rig.receiver.fill(video, options: .init()) }
        await #expect(throws: MoQServiceError.wouldBlock) { try await rig.receiver.fill(audio, options: .init()) }
        var events = rig.receiver.trackEventsExtended.makeAsyncIterator()
        guard case .fillCreated(_, let first)? = try await events.next() else {
            Issue.record("missing first receipt"); await rig.endpoint.close(); return
        }
        #expect(first == 1)
        try await rig.receiver.fill(video, options: .init())
        await #expect(throws: MoQServiceError.wouldBlock) { try await rig.receiver.fill(audio, options: .init()) }
        /* The audio receipt frees audio. The stray video receipt was staged
         * while the slot was held, so it answers no command: dequeuing it
         * leaves the video command issued since then in place, and that
         * command's own receipt below holds the slot again. */
        guard case .fillCreated(_, let second)? = try await events.next(),
              case .fillCreated(_, let absent)? = try await events.next() else {
            Issue.record("missing receipts"); await rig.endpoint.close(); return
        }
        #expect(second == 2 && absent == nil)
        try await rig.receiver.fill(audio, options: .init())
        try await rig.stage([.created(7, requestID: 3)])
        await #expect(throws: MoQServiceError.wouldBlock) { try await rig.receiver.fill(video, options: .init()) }
        #expect(rig.backend.snapshot().fillTrackCalls.map(\.handleID) == [7, 8, 7, 8])
        await rig.endpoint.close()
        #expect(rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("The legacy view consumes a receipt, forfeiting its ID and freeing the slot")
    func legacyViewConsumesReceipt() async throws {
        let rig = try FillRig()
        let track = try await rig.discover()
        try await rig.receiver.fill(track, options: .init())
        try await rig.stage([.created(7, requestID: 4), .catalogReady])
        var legacy = rig.receiver.trackEvents.makeAsyncIterator()
        if case .catalogReady? = try await legacy.next() {} else { Issue.record("legacy tail lost") }
        try await rig.receiver.fill(track, options: .init())
        #expect(rig.backend.snapshot().fillTrackCalls.count == 2)
        #expect(rig.receiver.attachment.stagedTrackEventCount == 0)
        await rig.endpoint.close()
        #expect(rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Receipts take bounded staging slots and the retained surplus keeps its order")
    func receiptsUnderStagingPressure() async throws {
        let rig = try FillRig()
        let track = try await rig.discover()
        try await rig.receiver.fill(track, options: .init())
        try await rig.stage((0..<64).map { .fill(7, requestID: UInt64($0)) } + [.created(7, requestID: 99)])
        #expect(rig.receiver.attachment.stagedTrackEventCount == 64)
        #expect(rig.backend.snapshot().queuedEventsRemaining == 1)
        /* Still only issued: the binding defers to the native slot. */
        rig.backend.scriptFillTrackResults([.wouldBlock])
        await #expect(throws: MoQServiceError.wouldBlock) { try await rig.receiver.fill(track, options: .init()) }
        var events = rig.receiver.trackEventsExtended.makeAsyncIterator()
        for _ in 0..<64 { _ = try await events.next() }
        guard case .fillCreated(_, let id)? = try await events.next() else {
            Issue.record("receipt lost under pressure"); await rig.endpoint.close(); return
        }
        #expect(id == 99)
        await rig.endpoint.close()
        #expect(rig.backend.snapshot().fillTrackCalls.count == 2)
        #expect(rig.backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Endpoint shutdown keeps the final hook bounded; only Swift-staged values survive")
    func boundedShutdown() async throws {
        let rig = try FillRig()
        try await rig.stage([.added(7, name: "video")] + (0..<64).map { .fill(7, requestID: UInt64($0)) })
        #expect(rig.receiver.attachment.stagedTrackEventCount == 64)
        #expect(rig.backend.snapshot().queuedEventsRemaining == 1)
        await rig.endpoint.close()
        let polls = rig.backend.snapshot().pollCalls
        var iterator = rig.receiver.trackEventsExtended.makeAsyncIterator()
        if case .legacy(.added)? = try await iterator.next() {} else { Issue.record("missing add") }
        for id in 0..<63 {
            if case .fillTerminal(_, let terminal)? = try await iterator.next() {
                #expect(terminal.initiatingRequestID == UInt64(id))
            } else { Issue.record("missing staged fill \(id)") }
        }
        #expect(try await iterator.next() == nil)
        #expect(rig.backend.snapshot().pollCalls == polls)
        #expect(rig.backend.snapshot().queuedEventsRemaining == 1)
        #expect(rig.backend.snapshot().detachCalls == 1 && rig.backend.snapshot().violations.isEmpty)
    }
}
