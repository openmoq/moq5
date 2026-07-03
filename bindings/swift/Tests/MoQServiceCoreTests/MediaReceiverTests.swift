import Testing
import Foundation
@testable import MoQServiceCore

/* Deterministic receiver-pipeline tests over the S2 engine: scripted
 * backends, condition-variable handshakes, no sleeps or wall-clock
 * dependence. The receiver backend's violations recorder trips on polls off
 * the service thread and on any call after detach. */

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private struct Rig {
    let endpointBackend: ScriptedEndpointBackend
    let receiverBackend: ScriptedReceiverBackend
    let endpoint: MoQEndpoint
    let receiver: MediaReceiver
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func makeRig(
    configuration: MediaReceiver.Configuration = .live(namespace: "live/cam1")
) throws -> Rig {
    let endpointBackend = ScriptedEndpointBackend()
    let endpoint = MoQEndpoint(
        configuration: .init(url: URL(string: "moqt://relay.test:4443")!),
        engine: EndpointEngine(backend: endpointBackend))
    let receiverBackend = ScriptedReceiverBackend()
    let receiver = try MediaReceiver.attach(
        to: endpoint, configuration: configuration, backend: receiverBackend)
    return Rig(endpointBackend: endpointBackend,
               receiverBackend: receiverBackend,
               endpoint: endpoint, receiver: receiver)
}

@Suite("Receiver attach")
struct ReceiverAttachTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Attach wires the pipeline; a second attach is refused; close frees the slot")
    func attachSlot() async throws {
        let rig = try makeRig()
        /* One receiver per endpoint (C: MOQ_ERR_WRONG_STATE). */
        #expect(throws: MoQServiceError.wrongState) {
            _ = try MediaReceiver.attach(
                to: rig.endpoint, configuration: .live(namespace: "live/x"),
                backend: ScriptedReceiverBackend())
        }
        /* Close detaches and frees the slot for a fresh attach. */
        await rig.receiver.close()
        let second = try MediaReceiver.attach(
            to: rig.endpoint, configuration: .live(namespace: "live/x"),
            backend: ScriptedReceiverBackend())
        await second.close()
        await rig.endpoint.close()
        #expect(rig.receiverBackend.snapshot().detachCalls == 1)
        #expect(rig.receiverBackend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Attach validates the configuration and rejects a closed endpoint")
    func attachValidation() async throws {
        let endpointBackend = ScriptedEndpointBackend()
        let endpoint = MoQEndpoint(
            configuration: .init(url: URL(string: "moqt://h:1")!),
            engine: EndpointEngine(backend: endpointBackend))

        #expect(throws: MoQServiceError.self) {
            _ = try MediaReceiver.attach(
                to: endpoint,
                configuration: .live(namespace: MediaNamespace(parts: [])),
                backend: ScriptedReceiverBackend())
        }
        await endpoint.close()
        #expect(throws: MoQServiceError.closed) {
            _ = try MediaReceiver.attach(
                to: endpoint, configuration: .live(namespace: "live/cam1"),
                backend: ScriptedReceiverBackend())
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("The public attach stays a validated TODO until the C bridge")
    func publicAttachUnsupported() async throws {
        let rig = try makeRig()
        await rig.receiver.close()
        await #expect(throws: MoQServiceError.unsupported) {
            _ = try MediaReceiver.attach(
                to: rig.endpoint, configuration: .live(namespace: "live/cam1"))
        }
        await rig.endpoint.close()
    }
}

@Suite("Eager event draining")
struct ReceiverEagerDrainTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Events drain to exhaustion with NO consumer attached")
    func drainsWithoutConsumer() async throws {
        let rig = try makeRig()
        rig.receiverBackend.scriptEvents([
            .added(1, name: "video"),
            .added(2, name: "audio", mediaType: .audio),
            .catalogReady,
        ])
        rig.endpointBackend.wake()
        /* The C queue must be emptied (3 events + the trailing .none). */
        #expect(await rig.receiverBackend.awaitCondition { $0.pollCalls >= 4 })
        #expect(rig.receiver.tracks.map(\.description.name) == ["video", "audio"])
        await rig.endpoint.close()
        #expect(rig.receiverBackend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Same handle ID yields the same MediaTrack; updates land in place")
    func trackIdentity() async throws {
        let rig = try makeRig()
        var vodDescription = TrackDescription(
            name: "video", mediaType: .video, packaging: .cmaf)
        vodDescription.isLive = false
        vodDescription.trackDuration = .seconds(90)
        rig.receiverBackend.scriptEvents([
            .added(7, name: "video"),
            .updated(7, vodDescription),
            .removed(7),
        ])
        rig.endpointBackend.wake()

        var events: [TrackEvent] = []
        for try await event in rig.receiver.trackEvents {
            events.append(event)
            if events.count == 3 { break }
        }
        guard case .added(let a) = events[0],
              case .updated(let u) = events[1],
              case .removed(let d) = events[2] else {
            Issue.record("unexpected event shapes: \(events)")
            return
        }
        #expect(a === u && u === d)                 /* stable identity */
        #expect(a.description.isLive == false)      /* updated in place */
        #expect(a.description.trackDuration == .seconds(90))
        await rig.endpoint.close()
    }
}

@Suite("Track event stream")
struct TrackEventStreamTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A parked consumer resumes when events arrive")
    func parkedConsumerResumes() async throws {
        let rig = try makeRig()
        let task = Task { () -> TrackEvent? in
            var iterator = rig.receiver.trackEvents.makeAsyncIterator()
            return try await iterator.next()
        }
        #expect(await rig.endpointBackend.awaitParked(entries: 1))
        rig.receiverBackend.scriptEvents([.added(1, name: "video")])
        rig.endpointBackend.wake()
        let event = try await task.value
        guard case .added(let track)? = event else {
            Issue.record("expected .added, got \(String(describing: event))")
            return
        }
        #expect(track.description.name == "video")
        await rig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Concurrent second consumer throws wrongState; sequential re-iteration works")
    func singleConsumer() async throws {
        let rig = try makeRig()
        let wakesBefore = rig.endpointBackend.snapshot().wakeCount
        let first = Task { () -> TrackEvent? in
            var iterator = rig.receiver.trackEvents.makeAsyncIterator()
            return try await iterator.next()
        }
        /* The first consumer's waiter registration wakes the backend: once
         * the count grows, the consumer slot is provably claimed (the claim
         * happens before the registration), so the second consumer below
         * cannot win the slot instead and park forever. */
        #expect(await rig.endpointBackend.awaitCondition { $0.wakeCount > wakesBefore })
        await #expect(throws: MoQServiceError.wrongState) {
            var second = rig.receiver.trackEvents.makeAsyncIterator()
            _ = try await second.next()
        }
        rig.receiverBackend.scriptEvents([.added(1, name: "v"), .catalogReady])
        rig.endpointBackend.wake()
        _ = try await first.value

        /* Sequential re-iteration picks up remaining events. */
        var iterator = rig.receiver.trackEvents.makeAsyncIterator()
        let next = try await iterator.next()
        guard case .catalogReady? = next else {
            Issue.record("expected catalogReady, got \(String(describing: next))")
            return
        }
        await rig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("firstVideoTrack skips non-video events and returns the video track")
    func firstVideoTrack() async throws {
        let rig = try makeRig()
        rig.receiverBackend.scriptEvents([
            .added(1, name: "audio", mediaType: .audio),
            .catalogReady,
            .added(2, name: "video"),
        ])
        rig.endpointBackend.wake()
        let video = try await rig.receiver.trackEvents.firstVideoTrack()
        #expect(video?.description.name == "video")
        await rig.endpoint.close()

        /* A stream that ends with no video yields nil, not an error. */
        let empty = try makeRig()
        empty.receiverBackend.scriptEvents([
            .added(1, name: "audio", mediaType: .audio),
        ])
        empty.receiverBackend.scriptTerminal()
        empty.endpointBackend.wake()
        let none = try await empty.receiver.trackEvents.firstVideoTrack()
        #expect(none == nil)
        await empty.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Cancelling a parked consumer throws CancellationError; stream stays usable")
    func cancellation() async throws {
        let rig = try makeRig()
        let task = Task { () -> TrackEvent? in
            var iterator = rig.receiver.trackEvents.makeAsyncIterator()
            return try await iterator.next()
        }
        #expect(await rig.endpointBackend.awaitParked(entries: 1))
        task.cancel()
        await #expect(throws: CancellationError.self) { _ = try await task.value }

        rig.receiverBackend.scriptEvents([.added(1, name: "v")])
        rig.endpointBackend.wake()
        var iterator = rig.receiver.trackEvents.makeAsyncIterator()
        let event = try await iterator.next()
        #expect(event != nil)
        await rig.endpoint.close()
    }
}

@Suite("Stream latch semantics")
struct TrackEventLatchTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Queued events deliver THROUGH the latch; an empty stream throws interrupted")
    func latchSemantics() async throws {
        let rig = try makeRig()
        /* Fill the Swift FIFO first, then latch. */
        rig.receiverBackend.scriptEvents([.added(1, name: "v"), .catalogReady])
        rig.endpointBackend.wake()
        #expect(await rig.receiverBackend.awaitCondition { $0.pollCalls >= 3 })

        rig.endpoint.interrupt()
        var iterator = rig.receiver.trackEvents.makeAsyncIterator()
        /* Non-empty FIFO: delivery is latch-immune (C poll_track parity). */
        let first = try await iterator.next()
        let second = try await iterator.next()
        #expect(first != nil && second != nil)
        /* Empty FIFO while latched: the WAIT is refused. */
        await #expect(throws: MoQServiceError.interrupted) {
            var again = rig.receiver.trackEvents.makeAsyncIterator()
            _ = try await again.next()
        }

        /* Draining paused while latched-idle resumes on resume(). */
        rig.receiverBackend.scriptEvents([.added(2, name: "a", mediaType: .audio)])
        rig.endpoint.resume()
        var again = rig.receiver.trackEvents.makeAsyncIterator()
        let event = try await again.next()
        guard case .added(let track)? = event else {
            Issue.record("expected .added, got \(String(describing: event))")
            return
        }
        #expect(track.description.name == "a")
        await rig.endpoint.close()
        #expect(rig.receiverBackend.snapshot().violations.isEmpty)
    }
}

@Suite("Stream terminal semantics")
struct TrackEventTerminalTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Clean terminal: queued events drain, then the stream ends nil")
    func cleanTerminalDrains() async throws {
        let rig = try makeRig()
        rig.receiverBackend.scriptEvents([.added(1, name: "v"), .catalogReady])
        rig.receiverBackend.scriptTerminal()
        rig.endpointBackend.wake()

        var events: [TrackEvent] = []
        for try await event in rig.receiver.trackEvents { events.append(event) }
        #expect(events.count == 2)                  /* both pre-terminal events */
        /* A fresh iteration after terminal ends immediately. */
        var iterator = rig.receiver.trackEvents.makeAsyncIterator()
        let more = try await iterator.next()
        #expect(more == nil)
        await rig.endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Fatal terminal: queued events drain, then the stream throws the code")
    func fatalTerminalDrains() async throws {
        let rig = try makeRig()
        rig.receiverBackend.scriptEvents([.added(1, name: "v")])
        rig.receiverBackend.scriptTerminal(fatal: true, code: 0x2)  /* EVENT_OVERFLOW */
        rig.endpointBackend.wake()

        var iterator = rig.receiver.trackEvents.makeAsyncIterator()
        let event = try await iterator.next()
        #expect(event != nil)                       /* pre-terminal event drains */
        await #expect(throws: MoQServiceError.fatal(code: 0x2)) {
            var again = rig.receiver.trackEvents.makeAsyncIterator()
            _ = try await again.next()
        }
        await rig.endpoint.close()
    }
}

@Suite("Receiver teardown")
struct ReceiverTeardownTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("receiver.close() detaches once on the service thread and ends the stream")
    func receiverClose() async throws {
        let rig = try makeRig()
        let consumer = Task { () -> TrackEvent? in
            var iterator = rig.receiver.trackEvents.makeAsyncIterator()
            return try await iterator.next()
        }
        #expect(await rig.endpointBackend.awaitParked(entries: 1))
        await rig.receiver.close()
        let ended = try await consumer.value
        #expect(ended == nil)                       /* clean end, not an error */
        await rig.receiver.close()                  /* idempotent */
        await rig.endpoint.close()
        let snap = rig.receiverBackend.snapshot()
        #expect(snap.detachCalls == 1)
        #expect(snap.violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Forgotten close(): dropping receiver AND endpoint still tears everything down")
    func forgottenCloseBackstop() throws {
        let endpointBackend = ScriptedEndpointBackend()
        let receiverBackend = ScriptedReceiverBackend()
        var endpoint: MoQEndpoint? = MoQEndpoint(
            configuration: .init(url: URL(string: "moqt://relay.test:4443")!),
            engine: EndpointEngine(backend: endpointBackend))
        var receiver: MediaReceiver? = try MediaReceiver.attach(
            to: endpoint!, configuration: .live(namespace: "live/cam1"),
            backend: receiverBackend)
        _ = receiver

        /* The app forgets close() on both objects. The receiver's deinit
         * backstop must detach; the endpoint's deinit backstop must shut the
         * engine down -- which requires that NO retain cycle (engine hook ->
         * receiver -> endpoint -> engine) keeps them alive. */
        receiver = nil
        endpoint = nil
        #expect(receiverBackend.awaitCondition { $0.detachCalls == 1 })
        #expect(endpointBackend.awaitCondition { $0.destroyCount == 1 })
        #expect(receiverBackend.snapshot().violations.isEmpty)
        #expect(endpointBackend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Dropping a receiver while endpoint.close() is in progress still detaches")
    func deinitDuringEndpointClose() async throws {
        let endpointBackend = ScriptedEndpointBackend()
        let receiverBackend = ScriptedReceiverBackend()
        let endpoint = MoQEndpoint(
            configuration: .init(url: URL(string: "moqt://relay.test:4443")!),
            engine: EndpointEngine(backend: endpointBackend))
        var receiver: MediaReceiver? = try MediaReceiver.attach(
            to: endpoint, configuration: .live(namespace: "live/cam1"),
            backend: receiverBackend)
        _ = receiver

        /* Hold the loop, then begin close: the phase flips to stopping
         * BEFORE the teardown pass can run. */
        let gate = TestGate()
        endpoint.engine.post { gate.pass() }
        #expect(await gate.awaitBlocked(count: 1))
        let wakesBefore = endpointBackend.snapshot().wakeCount
        let closer = Task { await endpoint.close() }
        #expect(await endpointBackend.awaitCondition { $0.wakeCount > wakesBefore })

        /* Drop the receiver in exactly that window: its deinit backstop
         * cannot queue a detach job anymore (submissions are rejected), so
         * it must LEAVE the hook installed for the shutdown teardown to
         * own the detach -- removing it here would skip detach entirely. */
        receiver = nil
        gate.openGate()
        await closer.value

        #expect(receiverBackend.snapshot().detachCalls == 1)
        #expect(receiverBackend.snapshot().violations.isEmpty)
        #expect(endpointBackend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A cancelled close() still detaches and frees the slot")
    func cancelledCloseStillDetaches() async throws {
        let rig = try makeRig()
        let gate = TestGate()
        rig.endpoint.engine.post { gate.pass() }
        #expect(await gate.awaitBlocked(count: 1))

        /* Queue the close job behind the held loop, then cancel the calling
         * task BEFORE the job can start. Cleanup must be immune: the
         * receiver still detaches and the slot frees. */
        let wakesBefore = rig.endpointBackend.snapshot().wakeCount
        let closer = Task { await rig.receiver.close() }
        #expect(await rig.endpointBackend.awaitCondition { $0.wakeCount > wakesBefore })
        closer.cancel()
        gate.openGate()
        await closer.value

        #expect(rig.receiverBackend.snapshot().detachCalls == 1)
        let second = try MediaReceiver.attach(
            to: rig.endpoint, configuration: .live(namespace: "live/x"),
            backend: ScriptedReceiverBackend())
        await second.close()
        await rig.endpoint.close()
        #expect(rig.receiverBackend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("endpoint.close() tears the receiver down first: final drain, detach, then stop")
    func endpointCloseTearsDownReceiverFirst() async throws {
        let rig = try makeRig()
        let order = EventRecorder()
        rig.receiverBackend.onDetach = { order.record("receiver-detach") }
        rig.endpointBackend.onStop = { order.record("endpoint-stop") }

        /* Events still queued in the C receiver at close time must survive
         * into the Swift FIFO (final drain before detach). */
        rig.receiverBackend.scriptEvents([.added(1, name: "v")])
        await rig.endpoint.close()

        #expect(order.events == ["receiver-detach", "endpoint-stop"])
        var iterator = rig.receiver.trackEvents.makeAsyncIterator()
        let survived = try await iterator.next()
        #expect(survived != nil)                    /* final drain preserved it */
        let ended = try await iterator.next()
        #expect(ended == nil)
        let snap = rig.receiverBackend.snapshot()
        #expect(snap.detachCalls == 1)
        #expect(snap.violations.isEmpty)
        #expect(rig.endpointBackend.snapshot().violations.isEmpty)
    }
}
