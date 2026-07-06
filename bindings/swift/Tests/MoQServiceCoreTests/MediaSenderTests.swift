import Testing
import Foundation
@testable import MoQServiceCore

/* Deterministic sender tests: scripted backends, handshake waits, no
 * sleeps. Budget accounting is in requested slices (the drain precedent),
 * so timeout paths need no wall clock. */

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private struct SenderRig {
    let endpointBackend: ScriptedEndpointBackend
    let senderBackend: ScriptedSenderBackend
    let endpoint: MoQEndpoint
    let sender: MediaSender
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func makeSenderRig(
    configuration: MediaSender.Configuration = .live(namespace: "live/cam1")
) throws -> SenderRig {
    let endpointBackend = ScriptedEndpointBackend()
    let endpoint = MoQEndpoint(
        configuration: .init(url: URL(string: "moqt://relay.test:4443")!),
        engine: EndpointEngine(backend: endpointBackend))
    let senderBackend = ScriptedSenderBackend(
        latch: { endpointBackend.isLatched })
    let sender = try MediaSender.attach(
        to: endpoint, configuration: configuration, backend: senderBackend)
    return SenderRig(endpointBackend: endpointBackend,
                     senderBackend: senderBackend,
                     endpoint: endpoint, sender: sender)
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func addVideoTrack(_ rig: SenderRig) async throws -> MediaTrack {
    try await rig.sender.addTrack(.video(
        name: "video", codec: "avc1.64001f", bitrate: 3_000_000,
        width: 1280, height: 720))
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func frame(_ bytes: [UInt8], sync: Bool = false) -> OutgoingObject {
    OutgoingObject(payload: Data(bytes), isSync: sync, startsGroup: sync,
                   presentationTime: .milliseconds(40))
}

@Suite("Sender attach")
struct SenderAttachTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("One sender per endpoint; close frees the slot; public attach is TODO")
    func attachSlot() async throws {
        let rig = try makeSenderRig()
        #expect(throws: MoQServiceError.wrongState) {
            _ = try MediaSender.attach(
                to: rig.endpoint, configuration: .live(namespace: "live/x"),
                backend: ScriptedSenderBackend())
        }
        await rig.sender.close()
        let second = try MediaSender.attach(
            to: rig.endpoint, configuration: .live(namespace: "live/x"),
            backend: ScriptedSenderBackend())
        await second.close()

        #expect(throws: MoQServiceError.unsupported) {
            _ = try MediaSender.attach(
                to: rig.endpoint, configuration: .live(namespace: "live/x"))
        }
        await rig.endpoint.close()
        #expect(rig.senderBackend.snapshot().detachCalls == 1)
        #expect(rig.senderBackend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A receiver and a sender share one endpoint; teardown is ordered")
    func sharesEndpointWithReceiver() async throws {
        let endpointBackend = ScriptedEndpointBackend()
        let endpoint = MoQEndpoint(
            configuration: .init(url: URL(string: "moqt://relay.test:4443")!),
            engine: EndpointEngine(backend: endpointBackend))
        let receiverBackend = ScriptedReceiverBackend()
        let senderBackend = ScriptedSenderBackend()
        let order = EventRecorder()
        receiverBackend.onDetach = { order.record("receiver-detach") }
        senderBackend.onDetach = { order.record("sender-detach") }
        endpointBackend.onStop = { order.record("endpoint-stop") }

        let receiver = try MediaReceiver.attach(
            to: endpoint, configuration: .live(namespace: "live/cam1"),
            backend: receiverBackend)
        let sender = try MediaSender.attach(
            to: endpoint, configuration: .live(namespace: "live/cam1"),
            backend: senderBackend)

        /* Both attachments work on the shared endpoint. */
        receiverBackend.scriptEvents([.added(1, name: "v")])
        endpointBackend.wake()
        var events = receiver.trackEvents.makeAsyncIterator()
        #expect(try await events.next() != nil)
        let track = try await sender.addTrack(.video(
            name: "out", codec: "av01", bitrate: 1_000, width: 16, height: 16))
        try await sender.write(frame([1], sync: true), to: track)

        /* Endpoint close tears children down first, receiver before
         * sender (the fixed hook order), then stops. */
        await endpoint.close()
        #expect(order.events == ["receiver-detach", "sender-detach",
                                 "endpoint-stop"])
        #expect(receiverBackend.snapshot().violations.isEmpty)
        #expect(senderBackend.snapshot().violations.isEmpty)
    }
}

@Suite("Add track")
struct SenderAddTrackTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Validation rejects before the backend; success builds a stable track")
    func addTrackValidationAndIdentity() async throws {
        let rig = try makeSenderRig()
        /* Invalid config (zero width) never reaches the seam. */
        await #expect(throws: MoQServiceError.self) {
            _ = try await rig.sender.addTrack(.video(
                name: "v", codec: "av01", bitrate: 1, width: 0, height: 1))
        }
        #expect(rig.senderBackend.snapshot().addTrackNames.isEmpty)

        let track = try await addVideoTrack(rig)
        #expect(track.description.name == "video")
        #expect(track.description.codec == "avc1.64001f")
        #expect(track.description.width == 1280)
        #expect(rig.senderBackend.snapshot().addTrackNames == ["video"])

        /* Writing to it round-trips the same handle. */
        try await rig.sender.write(frame([1], sync: true), to: track)
        await rig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Backend add-track failures map, with the fatal discriminator")
    func addTrackErrorMapping() async throws {
        let rig = try makeSenderRig()
        rig.senderBackend.scriptAddTracks([.invalidArgument])
        await #expect(throws: MoQServiceError.self) {
            _ = try await addVideoTrack(rig)
        }
        rig.senderBackend.scriptAddTracks([.wouldBlock])
        await #expect(throws: MoQServiceError.wouldBlock) {
            _ = try await addVideoTrack(rig)
        }
        /* .closed while fatal must discriminate. */
        rig.senderBackend.scriptAddTracks([.closed])
        rig.senderBackend.scriptTerminal(fatal: true, code: 0x2)
        await #expect(throws: MoQServiceError.fatal(code: 0x2)) {
            _ = try await addVideoTrack(rig)
        }
        await rig.endpoint.close()
    }
}

@Suite("Write policies")
struct SenderWriteTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Transfer on ok only; failFast surfaces wouldBlock with no wait")
    func writeTransferAndFailFast() async throws {
        var config = MediaSender.Configuration(
            namespace: "live/cam1", sendPolicy: .failFast)
        config.validateCMAF = false
        let rig = try makeSenderRig(configuration: config)
        let track = try await addVideoTrack(rig)

        try await rig.sender.write(frame([7, 7], sync: true), to: track)
        #expect(rig.senderBackend.snapshot().acceptedPayloads == [Data([7, 7])])

        rig.senderBackend.scriptWrites([.wouldBlock])
        await #expect(throws: MoQServiceError.wouldBlock) {
            try await rig.sender.write(frame([8]), to: track)
        }
        let snap = rig.senderBackend.snapshot()
        #expect(snap.acceptedPayloads == [Data([7, 7])])  /* not transferred */
        #expect(snap.waitCalls.isEmpty)                   /* failFast never waits */
        await rig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Foreign track, ended track, latch, and terminal all map correctly")
    func writeErrorMapping() async throws {
        let rig = try makeSenderRig()
        let track = try await addVideoTrack(rig)

        let foreign = MediaTrack(description: TrackDescription(
            name: "x", mediaType: .video, packaging: .raw))
        await #expect(throws: MoQServiceError.self) {
            try await rig.sender.write(frame([1]), to: foreign)
        }
        #expect(rig.senderBackend.snapshot().writeCalls.isEmpty)

        rig.senderBackend.scriptWrites([.wrongState])   /* ended track */
        await #expect(throws: MoQServiceError.wrongState) {
            try await rig.sender.write(frame([1]), to: track)
        }

        rig.endpoint.interrupt()
        await #expect(throws: MoQServiceError.interrupted) {
            try await rig.sender.write(frame([1]), to: track)
        }
        rig.endpoint.resume()

        /* Clean terminal vs fatal terminal discriminate. */
        rig.senderBackend.scriptWrites([.closed])
        await #expect(throws: MoQServiceError.closed) {
            try await rig.sender.write(frame([1]), to: track)
        }
        rig.senderBackend.scriptWrites([.closed])
        rig.senderBackend.scriptTerminal(fatal: true, code: 0x3)
        await #expect(throws: MoQServiceError.fatal(code: 0x3)) {
            try await rig.sender.write(frame([1]), to: track)
        }
        await rig.endpoint.close()
    }
}

@Suite("Lossless retry")
struct SenderLosslessTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("wouldBlock retries through sliced sender waits until delivered")
    func losslessRetriesThenDelivers() async throws {
        let rig = try makeSenderRig(configuration: .lossless(namespace: "live/cam1"))
        let track = try await addVideoTrack(rig)

        rig.senderBackend.scriptWrites([.wouldBlock, .wouldBlock, .ok])
        rig.senderBackend.scriptWaits([.activity, .activity])
        try await rig.sender.write(frame([5], sync: true), to: track)
        let snap = rig.senderBackend.snapshot()
        #expect(snap.writeCalls.count == 3)
        #expect(snap.waitCalls == [50_000, 50_000])       /* one slice per retry */
        #expect(snap.acceptedPayloads == [Data([5])])
        await rig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("The budget is exact: slices consume it, then wouldBlock surfaces")
    func losslessBudgetExhaustion() async throws {
        let config = MediaSender.Configuration(
            namespace: "live/cam1",
            sendPolicy: .lossless(timeout: .milliseconds(100)))
        let rig = try makeSenderRig(configuration: config)
        let track = try await addVideoTrack(rig)

        /* Every attempt blocks; every wait times out. 100ms budget = two
         * 50ms slices, then one final attempt, then the honest error. */
        rig.senderBackend.scriptWrites([.wouldBlock, .wouldBlock, .wouldBlock])
        rig.senderBackend.scriptWaits([.timeout, .timeout])
        await #expect(throws: MoQServiceError.wouldBlock) {
            try await rig.sender.write(frame([9]), to: track)
        }
        let snap = rig.senderBackend.snapshot()
        #expect(snap.writeCalls.count == 3)
        #expect(snap.waitCalls == [50_000, 50_000])
        #expect(snap.acceptedPayloads.isEmpty)            /* nothing transferred */
        await rig.endpoint.close()
        _ = config
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Pre-ready lossless writes wait on readiness/activity, then deliver")
    func losslessPreReadyUnblocks() async throws {
        let rig = try makeSenderRig(configuration: .lossless(namespace: "live/cam1"))
        let track = try await addVideoTrack(rig)
        rig.senderBackend.setReady(false)   /* unscripted writes now block */
        rig.senderBackend.scriptWaits([.timeout, .activity])

        let writer = Task {
            try await rig.sender.write(frame([6], sync: true), to: track)
        }
        /* After the first blocked attempt+slice, readiness arrives (the C
         * pump publishes the catalog, marking activity). The loop's next
         * attempts succeed within budget. */
        #expect(await rig.senderBackend.awaitCondition { $0.waitCalls.count >= 1 })
        rig.senderBackend.setReady(true)
        try await writer.value
        #expect(rig.senderBackend.snapshot().acceptedPayloads == [Data([6])])
        await rig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Never-ready lossless writes exhaust the budget into wouldBlock")
    func losslessNeverReadyTimesOut() async throws {
        let config = MediaSender.Configuration(
            namespace: "live/cam1",
            sendPolicy: .lossless(timeout: .milliseconds(100)))
        let rig = try makeSenderRig(configuration: config)
        let track = try await addVideoTrack(rig)
        rig.senderBackend.setReady(false)
        rig.senderBackend.scriptWaits([.timeout, .timeout])

        await #expect(throws: MoQServiceError.wouldBlock) {
            try await rig.sender.write(frame([1]), to: track)
        }
        #expect(rig.senderBackend.snapshot().acceptedPayloads.isEmpty)
        await rig.endpoint.close()
        _ = config
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Cancellation between slices abandons the retry; nothing transfers")
    func losslessCancellation() async throws {
        let rig = try makeSenderRig(configuration: .lossless(namespace: "live/cam1"))
        let track = try await addVideoTrack(rig)
        rig.senderBackend.scriptWrites([.wouldBlock, .wouldBlock, .wouldBlock])
        rig.senderBackend.setWaitGate(true)

        let writer = Task {
            try await rig.sender.write(frame([2]), to: track)
        }
        #expect(await rig.senderBackend.awaitWaitBlocked())
        writer.cancel()
        rig.senderBackend.setWaitGate(false)
        await #expect(throws: CancellationError.self) { try await writer.value }
        #expect(rig.senderBackend.snapshot().acceptedPayloads.isEmpty)

        /* The sender stays usable. */
        rig.senderBackend.scriptWrites([.ok])
        try await rig.sender.write(frame([3], sync: true), to: track)
        await rig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Latch and fatal terminal cut the retry loop with the right errors")
    func losslessPriority() async throws {
        let rig = try makeSenderRig(configuration: .lossless(namespace: "live/cam1"))
        let track = try await addVideoTrack(rig)

        /* Latch mid-retry: the sliced wait returns interrupted. */
        rig.senderBackend.scriptWrites([.wouldBlock])
        rig.senderBackend.setWaitGate(true)
        let writer = Task { try await rig.sender.write(frame([1]), to: track) }
        #expect(await rig.senderBackend.awaitWaitBlocked())
        rig.endpoint.interrupt()
        rig.senderBackend.setWaitGate(false)
        await #expect(throws: MoQServiceError.interrupted) { try await writer.value }
        rig.endpoint.resume()

        /* Fatal mid-retry: terminal beats level, discriminated. */
        rig.senderBackend.scriptWrites([.wouldBlock])
        rig.senderBackend.setWaitGate(true)
        let second = Task { try await rig.sender.write(frame([1]), to: track) }
        #expect(await rig.senderBackend.awaitWaitBlocked())
        rig.senderBackend.scriptTerminal(fatal: true, code: 0x1)
        rig.senderBackend.setWaitGate(false)
        await #expect(throws: MoQServiceError.fatal(code: 0x1)) {
            try await second.value
        }
        await rig.endpoint.close()
    }
}

@Suite("End track")
struct SenderEndTrackTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Success means the END marker is queued; wouldBlock retries internally")
    func endTrackRetriesWouldBlock() async throws {
        /* A DROP policy sender: the endTrack retry must be independent of
         * the send policy. */
        let rig = try makeSenderRig()
        let track = try await addVideoTrack(rig)

        rig.senderBackend.scriptEndTracks([.wouldBlock, .wouldBlock, .ok])
        rig.senderBackend.scriptWaits([.activity, .activity])
        try await rig.sender.endTrack(track)
        let snap = rig.senderBackend.snapshot()
        #expect(snap.endTrackCalls.count == 3)            /* retried to queued */
        #expect(snap.waitCalls == [50_000, 50_000])

        /* Idempotent: a second end succeeds (C returns MOQ_OK). */
        try await rig.sender.endTrack(track)

        /* Writes after end surface the C wrong-state. */
        rig.senderBackend.scriptWrites([.wrongState])
        await #expect(throws: MoQServiceError.wrongState) {
            try await rig.sender.write(frame([1]), to: track)
        }
        await rig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Cancellation, latch, and fatal cut the endTrack retry correctly")
    func endTrackPriority() async throws {
        let rig = try makeSenderRig()
        let track = try await addVideoTrack(rig)

        /* Cancellation between retries: the marker is NOT queued; the call
         * is safely re-invokable. */
        rig.senderBackend.scriptEndTracks([.wouldBlock, .wouldBlock, .ok])
        rig.senderBackend.setWaitGate(true)
        let ender = Task { try await rig.sender.endTrack(track) }
        #expect(await rig.senderBackend.awaitWaitBlocked())
        ender.cancel()
        rig.senderBackend.setWaitGate(false)
        await #expect(throws: CancellationError.self) { try await ender.value }

        /* Retry again: now it completes. */
        try await rig.sender.endTrack(track)

        /* Latch mid-retry. */
        rig.senderBackend.scriptEndTracks([.wouldBlock])
        rig.senderBackend.setWaitGate(true)
        let latched = Task { try await rig.sender.endTrack(track) }
        #expect(await rig.senderBackend.awaitWaitBlocked())
        rig.endpoint.interrupt()
        rig.senderBackend.setWaitGate(false)
        await #expect(throws: MoQServiceError.interrupted) { try await latched.value }
        rig.endpoint.resume()

        /* Fatal mid-retry, discriminated. */
        rig.senderBackend.scriptEndTracks([.wouldBlock])
        rig.senderBackend.setWaitGate(true)
        let fatal = Task { try await rig.sender.endTrack(track) }
        #expect(await rig.senderBackend.awaitWaitBlocked())
        rig.senderBackend.scriptTerminal(fatal: true, code: 0x3)
        rig.senderBackend.setWaitGate(false)
        await #expect(throws: MoQServiceError.fatal(code: 0x3)) {
            try await fatal.value
        }
        await rig.endpoint.close()
    }
}

@Suite("Sender readiness")
struct SenderReadinessTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("ready() resolves immediately when ready, parks otherwise")
    func readyParksAndResumes() async throws {
        let rig = try makeSenderRig()
        #expect(rig.sender.isReady)
        try await rig.sender.ready()                     /* fast path */

        rig.senderBackend.setReady(false)
        #expect(!rig.sender.isReady)
        let waiter = Task { try await rig.sender.ready() }
        #expect(await rig.endpointBackend.awaitParked(entries: 1))
        rig.senderBackend.setReady(true)
        rig.endpointBackend.wake()                       /* readiness = activity */
        try await waiter.value
        await rig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Terminal beats level: a fatal sender fails ready() even while 'ready'")
    func terminalBeatsLevel() async throws {
        let rig = try makeSenderRig()
        /* ready stays true AND the sender is fatal: the terminal state must
         * win (a satisfied ready() would invite writes that C refuses). */
        rig.senderBackend.scriptTerminal(fatal: true, code: 0x2)
        rig.endpointBackend.wake()
        await #expect(throws: MoQServiceError.fatal(code: 0x2)) {
            try await rig.sender.ready()
        }
        await rig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A parked ready() wakes into fatal; the latch interrupts it")
    func readyPriority() async throws {
        let rig = try makeSenderRig()
        rig.senderBackend.setReady(false)

        let latchWaiter = Task { try await rig.sender.ready() }
        #expect(await rig.endpointBackend.awaitParked(entries: 1))
        rig.endpoint.interrupt()
        await #expect(throws: MoQServiceError.interrupted) {
            try await latchWaiter.value
        }
        rig.endpoint.resume()

        let fatalWaiter = Task { try await rig.sender.ready() }
        let wakesBefore = rig.endpointBackend.snapshot().wakeCount
        #expect(await rig.endpointBackend.awaitCondition { $0.wakeCount > wakesBefore })
        rig.senderBackend.scriptTerminal(fatal: true, code: 0x1)
        rig.endpointBackend.wake()
        await #expect(throws: MoQServiceError.fatal(code: 0x1)) {
            try await fatalWaiter.value
        }
        await rig.endpoint.close()
    }
}

@Suite("Sender teardown")
struct SenderTeardownTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A cancelled close() still detaches and frees the slot")
    func cancelledCloseStillDetaches() async throws {
        let rig = try makeSenderRig()
        let gate = TestGate()
        rig.endpoint.engine.post { gate.pass() }
        #expect(await gate.awaitBlocked(count: 1))
        let wakesBefore = rig.endpointBackend.snapshot().wakeCount
        let closer = Task { await rig.sender.close() }
        #expect(await rig.endpointBackend.awaitCondition { $0.wakeCount > wakesBefore })
        closer.cancel()
        gate.openGate()
        await closer.value
        #expect(rig.senderBackend.snapshot().detachCalls == 1)
        let second = try MediaSender.attach(
            to: rig.endpoint, configuration: .live(namespace: "live/x"),
            backend: ScriptedSenderBackend())
        await second.close()
        await rig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Forgotten close(): dropping sender AND endpoint still tears down")
    func forgottenCloseBackstop() throws {
        let endpointBackend = ScriptedEndpointBackend()
        let senderBackend = ScriptedSenderBackend()
        var endpoint: MoQEndpoint? = MoQEndpoint(
            configuration: .init(url: URL(string: "moqt://relay.test:4443")!),
            engine: EndpointEngine(backend: endpointBackend))
        var sender: MediaSender? = try MediaSender.attach(
            to: endpoint!, configuration: .live(namespace: "live/cam1"),
            backend: senderBackend)
        _ = sender
        sender = nil
        endpoint = nil
        #expect(senderBackend.awaitCondition { $0.detachCalls == 1 })
        #expect(endpointBackend.awaitCondition { $0.destroyCount == 1 })
        #expect(senderBackend.snapshot().violations.isEmpty)
        #expect(endpointBackend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Dropping a sender while endpoint.close() is in progress still detaches")
    func deinitDuringEndpointClose() async throws {
        let endpointBackend = ScriptedEndpointBackend()
        let senderBackend = ScriptedSenderBackend()
        let endpoint = MoQEndpoint(
            configuration: .init(url: URL(string: "moqt://relay.test:4443")!),
            engine: EndpointEngine(backend: endpointBackend))
        var sender: MediaSender? = try MediaSender.attach(
            to: endpoint, configuration: .live(namespace: "live/cam1"),
            backend: senderBackend)
        _ = sender

        let gate = TestGate()
        endpoint.engine.post { gate.pass() }
        #expect(await gate.awaitBlocked(count: 1))
        let wakesBefore = endpointBackend.snapshot().wakeCount
        let closer = Task { await endpoint.close() }
        #expect(await endpointBackend.awaitCondition { $0.wakeCount > wakesBefore })

        sender = nil            /* deinit in the stopping window */
        gate.openGate()
        await closer.value
        #expect(senderBackend.snapshot().detachCalls == 1)
        #expect(senderBackend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Double close and post-close snapshots never touch the backend")
    func doubleCloseBackendIsolation() async throws {
        let rig = try makeSenderRig()
        await rig.sender.close()
        /* The second close's detach must short-circuit BEFORE evaluating
         * any backend snapshot (the backend was destroyed by the first). */
        await rig.sender.close()
        /* Any-thread snapshots after close answer engine-side. */
        #expect(!rig.sender.isReady)
        let snap = rig.senderBackend.snapshot()
        #expect(snap.detachCalls == 1)
        #expect(snap.violations.isEmpty)
        await rig.endpoint.close()
        #expect(rig.senderBackend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Operations after close fail closed with no backend calls")
    func operationsAfterClose() async throws {
        let rig = try makeSenderRig()
        let track = try await addVideoTrack(rig)
        await rig.sender.close()

        await #expect(throws: MoQServiceError.closed) {
            try await rig.sender.write(frame([1]), to: track)
        }
        await #expect(throws: MoQServiceError.closed) {
            _ = try await addVideoTrack(rig)
        }
        await #expect(throws: MoQServiceError.closed) {
            try await rig.sender.endTrack(track)
        }
        await #expect(throws: MoQServiceError.closed) {
            try await rig.sender.ready()
        }
        #expect(!rig.sender.isReady)
        let snap = rig.senderBackend.snapshot()
        #expect(snap.writeCalls.isEmpty)            /* guards fired first */
        #expect(snap.endTrackCalls.isEmpty)
        #expect(snap.addTrackNames == ["video"])    /* only the pre-close add */
        #expect(snap.violations.isEmpty)            /* nothing after detach */
        await rig.endpoint.close()
    }
}
