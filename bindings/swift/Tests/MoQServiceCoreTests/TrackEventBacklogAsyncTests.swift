import Testing
import Foundation
@testable import MoQServiceCore

/*
 * Finding 1 of the 0276 review (0277): the F4 bound fixed the synchronous
 * demand gate, but the ASYNC object waiter's level term
 * (`ReceiverAttachment.objectWaiterReady`) still reported a queued object as
 * ready while the Swift event FIFO was full. The async `objects` stream would
 * then wake, run a service-thread demand, hit the same full-FIFO gate, return
 * `.empty`, and re-park -- a hot retry -- and, worse, once the readiness term
 * required a free slot, a parked object waiter had no wake when the track-event
 * consumer freed one, so it could sit until an unrelated engine wake.
 *
 * The fix makes queued objects ready only when the FIFO has room, and has
 * `nextTrackEvent()` nudge the engine when consuming an event drops the FIFO
 * out of its full state. This test drives the REAL public surfaces -- the
 * `objects` and `trackEvents` async streams over a live engine + service
 * thread -- with deterministic handshakes (poll/wake counters), no sleeps.
 */

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private struct AsyncBacklogRig {
    let endpointBackend: ScriptedEndpointBackend
    let receiverBackend: ScriptedReceiverBackend
    let endpoint: MoQEndpoint
    let receiver: MediaReceiver
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func makeAsyncBacklogRig() throws -> AsyncBacklogRig {
    let endpointBackend = ScriptedEndpointBackend()
    let endpoint = MoQEndpoint(
        configuration: .init(url: URL(string: "moqt://relay.test:4443")!),
        engine: EndpointEngine(backend: endpointBackend))
    let receiverBackend = ScriptedReceiverBackend(
        latch: { endpointBackend.isLatched })
    let receiver = try MediaReceiver.attach(
        to: endpoint, configuration: .live(namespace: "live/cam1"),
        backend: receiverBackend)
    return AsyncBacklogRig(endpointBackend: endpointBackend,
                           receiverBackend: receiverBackend,
                           endpoint: endpoint, receiver: receiver)
}

private let kStagingCap = 64

@Suite("Track-event backlog async waiter (0277 finding 1)")
struct TrackEventBacklogAsyncTests {

    /// The public-surface liveness regression. With the FIFO full (64 staged)
    /// and handle 65's add retained behind it, an `objects` iterator must NOT
    /// poll while full; consuming events on the real `trackEvents` stream must
    /// drive it to completion using only the capacity-free wake -- no external
    /// backend wake after the FIFO is filled.
    ///
    /// Ledger asserted: the retained add is staged (backend surplus reaches 0)
    /// before the object is polled, the object is polled exactly once, the
    /// delivered object is not discarded, and the Swift FIFO never exceeds 64.
    ///
    /// Note on the consume count: the retained add is backend-owned behind the
    /// full ring, so the FIRST freed slot is spent staging it (refilling the
    /// ring to 64), and the object only becomes pollable once the backend
    /// surplus is exhausted -- two consumes for a surplus of one. Each consume's
    /// nudge is load-bearing: without the capacity-free wake the iterator never
    /// completes (see the report).
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Objects stream completes via the capacity-free wake, never polling while full")
    func objectStreamProgressesOnFreedCapacity() async throws {
        let rig = try makeAsyncBacklogRig()
        // Baseline: the pump may have already made attach-time `.none` polls, so
        // handshakes are on live poll DELTAS (`queuedEventsRemaining` is a
        // derived snapshot field, not maintained live, so it cannot be awaited).
        let pollsBefore = rig.receiverBackend.snapshot().pollCalls

        // 64 adds (t1..t64) + handle 65's add retained; its object queued.
        var events: [ReceiverPolledEvent] =
            (1...kStagingCap).map { .added(UInt64($0), name: "t\($0)") }
        events.append(.added(UInt64(kStagingCap + 1), name: "t\(kStagingCap + 1)"))
        rig.receiverBackend.scriptEvents(events)
        let storage = MockObjectStorage(media: [0xAB, 0xCD])
        rig.receiverBackend.scriptObjects([ReceiverPolledObject(
            handleID: UInt64(kStagingCap + 1), storage: storage,
            isKeyframe: true, endsGroup: false, isDatagram: false,
            presentationTime: .milliseconds(40), decodeTime: .milliseconds(40),
            compositionOffset: .zero, captureTime: nil)])

        // Fill the ring: the pump polls exactly 64 real events (t1..t64) and
        // stops before polling the 65th, leaving handle 65's add backend-owned.
        rig.endpointBackend.wake()
        #expect(await rig.receiverBackend.awaitCondition {
            $0.pollCalls >= pollsBefore + kStagingCap })
        var snap = rig.receiverBackend.snapshot()
        #expect(snap.queuedEventsRemaining == 1)        // handle 65's add retained
        #expect(snap.objectPollCalls == 0)

        // Start the object consumer; it demands, hits the full ring, returns
        // .empty, and parks. Prove it actually reached the parked full-FIFO
        // state: the engine re-enters `waitForActivity` (a NEW wait entry) only
        // after the demand ran and the object waiter was evaluated not-ready.
        let waitBeforePark = rig.endpointBackend.snapshot().waitEntries
        let objectTask = Task { () -> MediaObject? in
            var it = rig.receiver.objects.makeAsyncIterator()
            return try await it.next()
        }
        #expect(await rig.endpointBackend.awaitParked(entries: waitBeforePark + 1))
        // The demand ran and parked, and NO object was polled while full.
        #expect(rig.receiverBackend.snapshot().objectPollCalls == 0)

        var eventIterator = rig.receiver.trackEvents.makeAsyncIterator()

        // Consume #1: frees a slot -> nudge -> the waiter re-evaluates and the
        // demand stages the retained add (surplus -> 0), refilling to 64, then
        // re-parks. The object is still NOT polled: its add is staged first.
        let first = try await eventIterator.next()
        #expect(first != nil)
        // The demand's drain polls handle 65's retained add (one more real
        // poll) and stops full again -- so the surplus is now staged.
        #expect(await rig.receiverBackend.awaitCondition {
            $0.pollCalls >= pollsBefore + kStagingCap + 1 })
        snap = rig.receiverBackend.snapshot()
        #expect(snap.queuedEventsRemaining == 0)        // add staged, backend empty
        #expect(snap.objectPollCalls == 0)              // add before object

        // Consume #2: frees a slot -> nudge -> the demand now drains empty and
        // polls the object. No external backend wake was issued after the fill;
        // only the capacity-free nudges from these consumes drive it.
        let second = try await eventIterator.next()
        #expect(second != nil)

        let object = try await objectTask.value
        #expect(object != nil)
        #expect(object?.mediaData == Data([0xAB, 0xCD]))
        #expect(storage.cleanupCount == 0)              // delivered, still owned

        snap = rig.receiverBackend.snapshot()
        #expect(snap.objectPollCalls == 1)              // polled exactly once
        #expect(snap.queuedObjectsRemaining == 0)       // not discarded, delivered

        await rig.endpoint.close()
        #expect(rig.receiverBackend.snapshot().violations.isEmpty)
    }

    /// Terminal liveness (0279): a terminal behind a full event FIFO must not
    /// complete or hot-poll the `objects` stream while full; it completes
    /// (clean `nil`) only after the real `trackEvents` consumer frees enough
    /// capacity for `drainTrackEvents()` to reach and poll the terminal.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Objects stream surfaces a terminal only after capacity frees, never hot-polling while full")
    func terminalSurfacesOnlyAfterFreedCapacity() async throws {
        let rig = try makeAsyncBacklogRig()
        let pollsBefore = rig.receiverBackend.snapshot().pollCalls

        // Exactly 64 events, then a clean terminal behind the full ring.
        rig.receiverBackend.scriptEvents(
            (1...kStagingCap).map { .added(UInt64($0), name: "t\($0)") })
        rig.receiverBackend.scriptTerminal()

        // Fill: the pump stages 64 and stops full without polling the terminal.
        rig.endpointBackend.wake()
        #expect(await rig.receiverBackend.awaitCondition {
            $0.pollCalls >= pollsBefore + kStagingCap })
        var snap = rig.receiverBackend.snapshot()
        #expect(snap.terminalPollsReturned == 0)        // terminal not observed
        #expect(snap.objectPollCalls == 0)

        // Start the object consumer; with the ring full and a terminal pending,
        // it must park (not complete) and poll nothing.
        let waitBeforePark = rig.endpointBackend.snapshot().waitEntries
        let objectTask = Task { () -> MediaObject? in
            var it = rig.receiver.objects.makeAsyncIterator()
            return try await it.next()
        }
        #expect(await rig.endpointBackend.awaitParked(entries: waitBeforePark + 1))
        snap = rig.receiverBackend.snapshot()
        #expect(snap.objectPollCalls == 0)              // no hot-poll while full
        #expect(snap.terminalPollsReturned == 0)        // terminal still not observed

        // Consume one event: frees a slot -> nudge -> the demand's drain now
        // reaches the terminal, and the object stream ends cleanly (nil).
        var eventIterator = rig.receiver.trackEvents.makeAsyncIterator()
        let first = try await eventIterator.next()
        #expect(first != nil)

        let terminal = try await objectTask.value
        #expect(terminal == nil)                        // clean terminal disposition

        snap = rig.receiverBackend.snapshot()
        #expect(snap.terminalPollsReturned >= 1)        // terminal reached, once freed
        await rig.endpoint.close()
        #expect(rig.receiverBackend.snapshot().violations.isEmpty)
    }
}
