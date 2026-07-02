import Testing
import Foundation
@testable import MoQServiceCore

/* Deterministic object-delivery tests: scripted backends, handshake waits
 * (off-pool in async contexts), no sleeps. The receiver backend tripwires on
 * polls off the service thread, any call after detach, and leaks of
 * undelivered object storage. */

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private struct ObjectRig {
    let endpointBackend: ScriptedEndpointBackend
    let receiverBackend: ScriptedReceiverBackend
    let endpoint: MoQEndpoint
    let receiver: MediaReceiver
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func makeObjectRig() throws -> ObjectRig {
    let endpointBackend = ScriptedEndpointBackend()
    let endpoint = MoQEndpoint(
        configuration: .init(url: URL(string: "moqt://relay.test:4443")!),
        engine: EndpointEngine(backend: endpointBackend))
    /* The real C poll_object checks the endpoint latch internally; the
     * scripted backend models it through this probe. */
    let receiverBackend = ScriptedReceiverBackend(
        latch: { endpointBackend.isLatched })
    let receiver = try MediaReceiver.attach(
        to: endpoint, configuration: .live(namespace: "live/cam1"),
        backend: receiverBackend)
    return ObjectRig(endpointBackend: endpointBackend,
                     receiverBackend: receiverBackend,
                     endpoint: endpoint, receiver: receiver)
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func makeObject(_ handleID: UInt64, media: [UInt8],
                        keyframe: Bool = false)
    -> (ReceiverPolledObject, MockObjectStorage) {
    let storage = MockObjectStorage(media: media)
    let polled = ReceiverPolledObject(
        handleID: handleID, storage: storage, isKeyframe: keyframe,
        endsGroup: false, isDatagram: false,
        presentationTime: .milliseconds(40), decodeTime: .milliseconds(40),
        compositionOffset: .zero, captureTime: nil)
    return (polled, storage)
}

@Suite("Object stream")
struct ObjectStreamTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Objects are pulled on demand only: no consumer, no poll")
    func pullNotDrain() async throws {
        let rig = try makeObjectRig()
        rig.receiverBackend.scriptEvents([.added(1, name: "video")])
        let (polled, _) = makeObject(1, media: [1, 2, 3])
        rig.receiverBackend.scriptObjects([polled])
        rig.endpointBackend.wake()

        /* A full pass happened (the event drained), yet the object queue --
         * the C tier's buffer -- was never touched. */
        _ = try await rig.endpoint.engine.perform { }
        #expect(await rig.receiverBackend.awaitCondition { $0.pollCalls >= 2 })
        #expect(rig.receiverBackend.snapshot().objectPollCalls == 0)

        /* One demand, one poll, one object. */
        var iterator = rig.receiver.objects.makeAsyncIterator()
        let object = try await iterator.next()
        #expect(object?.mediaData == Data([1, 2, 3]))
        #expect(rig.receiverBackend.snapshot().objectPollCalls == 1)
        await rig.endpoint.close()
        #expect(rig.receiverBackend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Delivered objects reference the S3 MediaTrack instance and carry metadata")
    func deliveryAndIdentity() async throws {
        let rig = try makeObjectRig()
        rig.receiverBackend.scriptEvents([.added(7, name: "video")])
        let (polled, storage) = makeObject(7, media: [9, 9], keyframe: true)
        rig.receiverBackend.scriptObjects([polled])
        rig.endpointBackend.wake()

        var eventIterator = rig.receiver.trackEvents.makeAsyncIterator()
        guard case .added(let track)? = try await eventIterator.next() else {
            Issue.record("expected .added")
            return
        }
        var iterator = rig.receiver.objects.makeAsyncIterator()
        let object = try await iterator.next()
        #expect(object?.track === track)            /* stable identity */
        #expect(object?.isKeyframe == true)
        #expect(object?.presentationTime == .milliseconds(40))
        #expect(storage.cleanupCount == 0)          /* alive while held */
        await rig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Events integrate before objects, even when both arrive together")
    func eventsBeforeObjects() async throws {
        let rig = try makeObjectRig()
        /* Hold the engine INSIDE the jobs step, script the ADDED event and
         * the object while it is held, and queue the demand behind the
         * gate. The jobs step runs to exhaustion before the pump hook, so
         * the demand provably executes BEFORE any hook drain -- the demand
         * body itself must drain events first, or the object's handle
         * cannot resolve. (Without the gate this ordering would be
         * scheduling-dependent: a residual attach-time pass could drain
         * the events first.) */
        let gate = TestGate()
        rig.endpoint.engine.post { gate.pass() }
        #expect(await gate.awaitBlocked(count: 1))
        rig.receiverBackend.scriptEvents([.added(3, name: "video")])
        let (polled, _) = makeObject(3, media: [5])
        rig.receiverBackend.scriptObjects([polled])

        let wakesBefore = rig.endpointBackend.snapshot().wakeCount
        let consumer = Task { () -> MediaObject? in
            var iterator = rig.receiver.objects.makeAsyncIterator()
            return try await iterator.next()
        }
        #expect(await rig.endpointBackend.awaitCondition { $0.wakeCount > wakesBefore })
        gate.openGate()

        let object = try await consumer.value
        #expect(object != nil)
        #expect(object?.track.description.name == "video")
        await rig.endpoint.close()
        #expect(rig.receiverBackend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A parked consumer resumes when an object arrives")
    func parkedConsumerResumes() async throws {
        let rig = try makeObjectRig()
        rig.receiverBackend.scriptEvents([.added(1, name: "v")])
        rig.endpointBackend.wake()

        let task = Task { () -> MediaObject? in
            var iterator = rig.receiver.objects.makeAsyncIterator()
            return try await iterator.next()
        }
        /* The consumer's park registration wakes the backend. */
        #expect(await rig.receiverBackend.awaitCondition { $0.objectPollCalls >= 1 })
        let (polled, _) = makeObject(1, media: [1])
        rig.receiverBackend.scriptObjects([polled])
        rig.endpointBackend.wake()
        let object = try await task.value
        #expect(object != nil)
        await rig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Single object consumer; events and objects consumed concurrently")
    func consumers() async throws {
        let rig = try makeObjectRig()
        let wakesBefore = rig.endpointBackend.snapshot().wakeCount
        let objectConsumer = Task { () -> MediaObject? in
            var iterator = rig.receiver.objects.makeAsyncIterator()
            return try await iterator.next()
        }
        #expect(await rig.endpointBackend.awaitCondition { $0.wakeCount > wakesBefore })
        await #expect(throws: MoQServiceError.wrongState) {
            var second = rig.receiver.objects.makeAsyncIterator()
            _ = try await second.next()
        }
        /* The trackEvents stream is independent: it may run concurrently. */
        let eventConsumer = Task { () -> TrackEvent? in
            var iterator = rig.receiver.trackEvents.makeAsyncIterator()
            return try await iterator.next()
        }
        rig.receiverBackend.scriptEvents([.added(1, name: "v")])
        let (polled, _) = makeObject(1, media: [1])
        rig.receiverBackend.scriptObjects([polled])
        rig.endpointBackend.wake()
        let event = try await eventConsumer.value
        let object = try await objectConsumer.value
        #expect(event != nil && object != nil)
        await rig.endpoint.close()
    }
}

@Suite("Object ownership")
struct ObjectOwnershipStreamTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A delivered object cleans up exactly once when dropped")
    func exactlyOnceCleanup() async throws {
        let rig = try makeObjectRig()
        rig.receiverBackend.scriptEvents([.added(1, name: "v")])
        let (polled, storage) = makeObject(1, media: [1, 2])
        rig.receiverBackend.scriptObjects([polled])
        rig.endpointBackend.wake()

        var iterator = rig.receiver.objects.makeAsyncIterator()
        do {
            let object = try await iterator.next()
            #expect(object != nil)
            #expect(storage.cleanupCount == 0)
        }
        #expect(storage.cleanupCount == 1)          /* dropped: exactly once */
        await rig.endpoint.close()
        #expect(storage.cleanupCount == 1)          /* close adds nothing */
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Undelivered objects are cleaned by detach (the C destroy semantics)")
    func undeliveredCleanedAtClose() async throws {
        let rig = try makeObjectRig()
        rig.receiverBackend.scriptEvents([.added(1, name: "v")])
        let (polled, storage) = makeObject(1, media: [1])
        rig.receiverBackend.scriptObjects([polled])
        rig.endpointBackend.wake()

        await rig.receiver.close()
        #expect(storage.cleanupCount == 1)
        await rig.endpoint.close()
        #expect(storage.cleanupCount == 1)
        #expect(rig.receiverBackend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("An unknown-handle object is cleaned exactly once and skipped")
    func unknownHandleCleanedAndSkipped() async throws {
        let rig = try makeObjectRig()
        rig.receiverBackend.scriptEvents([.added(1, name: "video")])
        /* Handle 99 was never ADDED (impossible per the C contract, but the
         * transferred buffers must never leak if it happens). The valid
         * object behind it must still deliver. */
        let (unknown, unknownStorage) = makeObject(99, media: [0xEE])
        let (valid, validStorage) = makeObject(1, media: [4, 2])
        rig.receiverBackend.scriptObjects([unknown, valid])
        rig.endpointBackend.wake()

        var iterator = rig.receiver.objects.makeAsyncIterator()
        let object = try await iterator.next()
        #expect(object?.mediaData == Data([4, 2]))
        #expect(unknownStorage.cleanupCount == 1)   /* exactly once, skipped */
        #expect(validStorage.cleanupCount == 0)     /* delivered, still owned */
        await rig.endpoint.close()
        #expect(unknownStorage.cleanupCount == 1)   /* close adds nothing */
        #expect(rig.receiverBackend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Cancel before the poll job starts: no poll, the object is preserved")
    func cancelBeforePollStarts() async throws {
        let rig = try makeObjectRig()
        rig.receiverBackend.scriptEvents([.added(1, name: "v")])
        let (polled, storage) = makeObject(1, media: [8])
        rig.receiverBackend.scriptObjects([polled])
        rig.endpointBackend.wake()
        #expect(await rig.receiverBackend.awaitCondition { $0.pollCalls >= 1 })

        /* Hold the loop so the demand job cannot start, then cancel. */
        let gate = TestGate()
        rig.endpoint.engine.post { gate.pass() }
        #expect(await gate.awaitBlocked(count: 1))
        let wakesBefore = rig.endpointBackend.snapshot().wakeCount
        let consumer = Task { () -> MediaObject? in
            var iterator = rig.receiver.objects.makeAsyncIterator()
            return try await iterator.next()
        }
        #expect(await rig.endpointBackend.awaitCondition { $0.wakeCount > wakesBefore })
        consumer.cancel()
        await #expect(throws: CancellationError.self) { _ = try await consumer.value }
        gate.openGate()

        /* No poll happened; the object is intact for the next consumer. */
        #expect(rig.receiverBackend.snapshot().objectPollCalls == 0)
        var iterator = rig.receiver.objects.makeAsyncIterator()
        let object = try await iterator.next()
        #expect(object?.mediaData == Data([8]))
        #expect(storage.cleanupCount == 0)
        await rig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Cancel racing a started poll: the object is delivered, cleaned exactly once")
    func cancelDuringPollDelivers() async throws {
        let rig = try makeObjectRig()
        rig.receiverBackend.scriptEvents([.added(1, name: "v")])
        let (polled, storage) = makeObject(1, media: [3, 3])
        rig.receiverBackend.scriptObjects([polled])
        rig.endpointBackend.wake()
        #expect(await rig.receiverBackend.awaitCondition { $0.pollCalls >= 1 })

        /* Block INSIDE the object poll: the job has started and ownership
         * is transferring when the cancel lands. The plan rule: a started
         * poll always delivers -- never orphan a transferred buffer. The
         * task consumes the object and returns only its bytes, so the
         * MediaObject provably deinits inside the (cancelled) task body. */
        rig.receiverBackend.setObjectGate(true)
        let consumer = Task { () -> Data? in
            var iterator = rig.receiver.objects.makeAsyncIterator()
            let object = try await iterator.next()
            return object?.mediaData
        }
        #expect(await rig.receiverBackend.awaitObjectPollBlocked())
        consumer.cancel()
        rig.receiverBackend.setObjectGate(false)

        let data = try await consumer.value         /* delivered, not thrown */
        #expect(data == Data([3, 3]))
        #expect(storage.cleanupCount == 1)          /* exactly once, in-task */
        await rig.endpoint.close()
        #expect(storage.cleanupCount == 1)
    }
}

@Suite("Object latch and terminal")
struct ObjectLatchTerminalTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("The latch gates object delivery even with objects queued (C asymmetry)")
    func latchGatesQueuedObjects() async throws {
        let rig = try makeObjectRig()
        rig.receiverBackend.scriptEvents([.added(1, name: "v"), .catalogReady])
        let (polled, _) = makeObject(1, media: [1])
        rig.receiverBackend.scriptObjects([polled])
        rig.endpointBackend.wake()
        #expect(await rig.receiverBackend.awaitCondition { $0.pollCalls >= 3 })

        rig.endpoint.interrupt()
        /* trackEvents still delivers through the latch (S3, poll_track is
         * not latch-gated)... */
        var eventIterator = rig.receiver.trackEvents.makeAsyncIterator()
        #expect(try await eventIterator.next() != nil)
        /* ...but objects are refused (poll_object IS latch-gated). */
        await #expect(throws: MoQServiceError.interrupted) {
            var iterator = rig.receiver.objects.makeAsyncIterator()
            _ = try await iterator.next()
        }
        rig.endpoint.resume()
        var iterator = rig.receiver.objects.makeAsyncIterator()
        #expect(try await iterator.next() != nil)
        await rig.endpoint.close()
        #expect(rig.receiverBackend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Objects drain, then the stream terminates clean or fatal")
    func terminalDrains() async throws {
        let rig = try makeObjectRig()
        rig.receiverBackend.scriptEvents([.added(1, name: "v")])
        let (polled, _) = makeObject(1, media: [1])
        rig.receiverBackend.scriptObjects([polled])
        rig.receiverBackend.scriptTerminal()
        rig.endpointBackend.wake()

        var iterator = rig.receiver.objects.makeAsyncIterator()
        #expect(try await iterator.next() != nil)   /* queued item drains */
        #expect(try await iterator.next() == nil)   /* then clean end */

        let fatalRig = try makeObjectRig()
        fatalRig.receiverBackend.scriptEvents([.added(1, name: "v")])
        fatalRig.receiverBackend.scriptTerminal(fatal: true, code: 0x1)
        fatalRig.endpointBackend.wake()
        await #expect(throws: MoQServiceError.fatal(code: 0x1)) {
            var it = fatalRig.receiver.objects.makeAsyncIterator()
            _ = try await it.next()
        }
        await rig.endpoint.close()
        await fatalRig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Receiver-fatal with NO queued object wakes a parked consumer into fatal")
    func fatalWakesParkedConsumer() async throws {
        let rig = try makeObjectRig()
        let consumer = Task { () -> MediaObject? in
            var iterator = rig.receiver.objects.makeAsyncIterator()
            return try await iterator.next()
        }
        /* Parked: the empty poll happened, nothing queued. */
        #expect(await rig.receiverBackend.awaitCondition { $0.objectPollCalls >= 1 })

        /* The receiver turns fatal with an EMPTY object queue (e.g. event
         * overflow). The C transition marks endpoint activity; the parked
         * consumer must wake into .fatal -- not strand until unrelated
         * activity. */
        rig.receiverBackend.scriptTerminal(fatal: true, code: 0x2)
        rig.endpointBackend.wake()
        await #expect(throws: MoQServiceError.fatal(code: 0x2)) {
            _ = try await consumer.value
        }
        await rig.endpoint.close()
    }
}

@Suite("Subscription control")
struct SubscriptionControlTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Subscribe and unsubscribe run on the service thread with the track's handle")
    func subscribeRecordsCommand() async throws {
        let rig = try makeObjectRig()
        rig.receiverBackend.scriptEvents([.added(11, name: "video")])
        rig.endpointBackend.wake()
        var eventIterator = rig.receiver.trackEvents.makeAsyncIterator()
        guard case .added(let track)? = try await eventIterator.next() else {
            Issue.record("expected .added")
            return
        }

        try await rig.receiver.subscribe(track, start: .nextGroup, priority: 5)
        try await rig.receiver.unsubscribe(track)
        let snap = rig.receiverBackend.snapshot()
        #expect(snap.subscribeCalls ==
                [ScriptedReceiverBackend.SubscribeCall(
                    handleID: 11, start: .nextGroup, priority: 5)])
        #expect(snap.unsubscribeCalls == [11])
        #expect(snap.violations.isEmpty)            /* service thread only */
        await rig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("C command results map to the service errors")
    func subscribeErrorMapping() async throws {
        let rig = try makeObjectRig()
        rig.receiverBackend.scriptEvents([.added(1, name: "v")])
        rig.endpointBackend.wake()
        var eventIterator = rig.receiver.trackEvents.makeAsyncIterator()
        guard case .added(let track)? = try await eventIterator.next() else {
            Issue.record("expected .added")
            return
        }

        rig.receiverBackend.scriptSubscribeResults([.wrongState])
        await #expect(throws: MoQServiceError.wrongState) {
            try await rig.receiver.subscribe(track)
        }
        rig.receiverBackend.scriptSubscribeResults([.unsupported])
        await #expect(throws: MoQServiceError.unsupported) {
            try await rig.receiver.subscribe(track)
        }
        rig.receiverBackend.scriptSubscribeResults([.closed])
        await #expect(throws: MoQServiceError.closed) {
            try await rig.receiver.subscribe(track)
        }
        await rig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Foreign tracks and closed receivers are refused before any backend call")
    func subscribeGuards() async throws {
        let rig = try makeObjectRig()
        rig.receiverBackend.scriptEvents([.added(1, name: "v")])
        rig.endpointBackend.wake()
        var eventIterator = rig.receiver.trackEvents.makeAsyncIterator()
        guard case .added(let track)? = try await eventIterator.next() else {
            Issue.record("expected .added")
            return
        }

        /* A track this receiver never discovered. */
        let foreign = MediaTrack(description: TrackDescription(
            name: "other", mediaType: .video, packaging: .raw))
        await #expect(throws: MoQServiceError.self) {
            try await rig.receiver.subscribe(foreign)
        }

        await rig.receiver.close()
        await #expect(throws: MoQServiceError.closed) {
            try await rig.receiver.subscribe(track)
        }
        let snap = rig.receiverBackend.snapshot()
        #expect(snap.subscribeCalls.isEmpty)        /* guards fired first */
        #expect(snap.violations.isEmpty)            /* nothing after detach */
        await rig.endpoint.close()
    }
}
