import Testing
import Foundation
@testable import MoQServiceCore

/*
 * Finding 4 (MOQ-SWIFT-TRACK-EVENT-BACKLOG) security regression.
 *
 * The vulnerability (pre-fix): the Swift service layer eagerly drained the
 * bounded C track-event queue into an UNBOUNDED Swift FIFO --
 * `ReceiverAttachment.drainTrackEvents()` looped `backend.pollTrackEvent()`
 * with no capacity check and `ReceiverState.integrate` appended every event to
 * `fifo` -- moving the backlog from the bounded C layer into an unbounded Swift
 * one and defeating the C queue's capacity control.
 *
 * The Swift backend never sets `moq_media_receiver_cfg_t.max_track_events`, so
 * the C queue depth is its default of 64; the fix bounds the Swift staging at
 * the same 64, stops polling before the 65th event, and leaves the surplus in
 * the backend/C queue (end-to-end backpressure) using O(1) FIFO ops.
 *
 * These tests encode that bounded contract and now pass against the fix (they
 * failed RED against the pre-fix unbounded staging). They drive the eager drain
 * SYNCHRONOUSLY via `ReceiverAttachment.drainTrackEvents()` -- no service
 * thread, no sleeps,
 * no wall-clock, no large allocations. `pollCalls` (backend polls) and
 * `queuedEventsRemaining` (surplus left in the backend) come from the scripted
 * backend snapshot; `stagedTrackEventCount` is the read-only Swift FIFO depth.
 */

/// The Swift staging bound for v1: matches the C queue default (64) the Swift
/// backend always uses. Package-internal, no new public configuration field.
private let kSwiftStagingCap = 64

@Suite("Track-event backlog bound (finding 4)")
struct TrackEventBacklogTests {

    /// Build `total` track events: one `.added` (so the FIFO holds a real
    /// lifecycle event first) followed by mutable `.catalogReady` events -- a
    /// peer does not need distinct tracks to grow the FIFO.
    private func makeEvents(total: Int) -> [ReceiverPolledEvent] {
        var events: [ReceiverPolledEvent] = [.added(1, name: "video")]
        for _ in 1..<total { events.append(.catalogReady) }
        return events
    }

    /// Obligations 1 & 5: with more than 64 events and no consumer, the eager
    /// drain must stop at EXACTLY 64 staged, must NOT poll the 65th event, and
    /// must leave the surplus in the backend. `pollCalls == 64` (stopped before
    /// the next backend poll) together with `queuedEventsRemaining == 1`
    /// distinguishes "stopped before polling" from "polled then dropped/refused"
    /// -- a poll-then-drop fix would show `pollCalls == 65`, `remaining == 0`.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Eager drain stops at 64 staged and leaves the 65th unpolled")
    func stopsAtCapacity() {
        let backend = ScriptedReceiverBackend()
        let attachment = ReceiverAttachment(backend: backend)

        let events = makeEvents(total: kSwiftStagingCap + 1)   // 65
        backend.scriptEvents(events)

        attachment.drainTrackEvents()

        let snap = backend.snapshot()
        #expect(attachment.stagedTrackEventCount == kSwiftStagingCap)
        #expect(snap.pollCalls == kSwiftStagingCap)             // did not poll #65
        #expect(snap.queuedEventsRemaining == events.count - kSwiftStagingCap)
    }

    /// Obligation 2: repeated drains with no consumer never exceed 64 staged;
    /// the backend keeps the whole surplus; no extra backend polls occur once
    /// the ring is full.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Repeated drains without a consumer never exceed 64 staged")
    func repeatedDrainsStayBounded() {
        let backend = ScriptedReceiverBackend()
        let attachment = ReceiverAttachment(backend: backend)

        let total = 2 * kSwiftStagingCap + 2                    // 130
        backend.scriptEvents(makeEvents(total: total))

        attachment.drainTrackEvents()
        attachment.drainTrackEvents()
        attachment.drainTrackEvents()

        let snap = backend.snapshot()
        #expect(attachment.stagedTrackEventCount == kSwiftStagingCap)
        #expect(snap.pollCalls == kSwiftStagingCap)             // no re-poll when full
        #expect(snap.queuedEventsRemaining == total - kSwiftStagingCap)
    }

    /// Obligation 4: a terminal scripted behind more than 64 events must not be
    /// observed early -- the bounded drain stops at 64 with the terminal still
    /// queued in the backend, so already-staged events are not lost and the
    /// terminal only surfaces once space is consumed.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A terminal behind a full ring stays queued until space is consumed")
    func terminalBehindFullRingStaysQueued() {
        let backend = ScriptedReceiverBackend()
        let attachment = ReceiverAttachment(backend: backend)

        backend.scriptEvents(makeEvents(total: kSwiftStagingCap + 5))  // 69
        backend.scriptTerminal()                                       // terminal after

        attachment.drainTrackEvents()

        let snap = backend.snapshot()
        // Stopped at capacity; the terminal (and surplus) remain unpolled.
        #expect(attachment.stagedTrackEventCount == kSwiftStagingCap)
        #expect(snap.pollCalls == kSwiftStagingCap)
        #expect(snap.queuedEventsRemaining >= 5)
    }

    // MARK: - Backlog obligations (0259 Finding 4, remaining coverage)

    /// `count` distinct added tracks: handle `i` is named `"t\(i)"`, so both
    /// event ORDER and stable track IDENTITY are observable across a
    /// drain/consume/re-drain cycle.
    private func makeDistinctAdds(_ count: Int) -> [ReceiverPolledEvent] {
        (1...count).map { .added(UInt64($0), name: "t\($0)") }
    }

    /// The track name of a consumed `.added` event, or nil for any other
    /// disposition -- lets `#expect` assert order/identity in one comparison.
    private func addedName(
        _ taken: ReceiverAttachment.TakenTrackEvent) -> String? {
        if case .item(.added(let track)) = taken { return track.description.name }
        return nil
    }

    private func isEmptyTaken(
        _ taken: ReceiverAttachment.TakenTrackEvent) -> Bool {
        if case .empty = taken { return true }
        return false
    }

    private func isEmptyDemand(_ demand: ReceiverAttachment.ObjectDemand) -> Bool {
        if case .empty = demand { return true }
        return false
    }

    /// Obligation 1: after consuming staged events, another pump refills ONLY
    /// the freed slots, preserves exact order and identity, leaves the backend
    /// suffix unpolled, and follows an exact poll ledger -- including declaring
    /// when a terminal `.none` probe is and is NOT owed. With more backend
    /// surplus (16) than the slots freed by a first consume (10), the refill
    /// must top the ring back to exactly 64 and stop, never draining the whole
    /// backend.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A later pump refills only the freed slots, in order, with an exact poll ledger")
    func refillsOnlyFreedSlotsPreservingOrder() {
        let backend = ScriptedReceiverBackend()
        let attachment = ReceiverAttachment(backend: backend)
        let total = 80
        backend.scriptEvents(makeDistinctAdds(total))          // t1..t80

        // Drain #1: bounded staging stops at 64 without polling #65. The ring
        // fills before the backend empties, so NO terminal .none probe is owed.
        attachment.drainTrackEvents()
        var snap = backend.snapshot()
        #expect(attachment.stagedTrackEventCount == kSwiftStagingCap)
        #expect(snap.pollCalls == kSwiftStagingCap)            // #65 never polled
        #expect(snap.queuedEventsRemaining == total - kSwiftStagingCap)  // t65..t80
        #expect(snap.terminalPollsReturned == 0)

        // Consume 10 from the front: t1..t10 IN ORDER.
        for i in 1...10 {
            #expect(addedName(attachment.takeStagedTrackEvent()) == "t\(i)")
        }
        #expect(attachment.stagedTrackEventCount == kSwiftStagingCap - 10)

        // Drain #2: refills EXACTLY the 10 freed slots (t65..t74) and stops;
        // the ring fills again before the backend empties, so still no probe.
        attachment.drainTrackEvents()
        snap = backend.snapshot()
        #expect(attachment.stagedTrackEventCount == kSwiftStagingCap)
        #expect(snap.pollCalls == kSwiftStagingCap + 10)       // 64 + 10, not the tail
        #expect(snap.queuedEventsRemaining == 6)               // t75..t80 retained
        #expect(snap.terminalPollsReturned == 0)

        // Consume all 64: t11..t74 IN ORDER (order preserved across the refill).
        for i in 11...74 {
            #expect(addedName(attachment.takeStagedTrackEvent()) == "t\(i)")
        }
        #expect(attachment.stagedTrackEventCount == 0)

        // Drain #3: only 6 remain and the ring has room, so the bounded drain
        // polls the 6 (t75..t80) AND one terminal .none probe to discover
        // emptiness -- a .none poll IS owed here, unlike drains #1/#2.
        attachment.drainTrackEvents()
        snap = backend.snapshot()
        #expect(attachment.stagedTrackEventCount == 6)
        #expect(snap.pollCalls == kSwiftStagingCap + 10 + 6 + 1)  // + tail + .none
        #expect(snap.queuedEventsRemaining == 0)
        #expect(snap.terminalPollsReturned == 0)               // .none, not .closed

        // Consume the final 6: t75..t80 IN ORDER, then the FIFO is empty and,
        // with no terminal scripted, reports .empty (not a terminal).
        for i in 75...80 {
            #expect(addedName(attachment.takeStagedTrackEvent()) == "t\(i)")
        }
        #expect(isEmptyTaken(attachment.takeStagedTrackEvent()))
    }

    /// Obligation 4 (with 5): while the ring is full and nothing is consumed,
    /// repeated pumps must perform ZERO additional backend polls -- proving the
    /// bound stops BEFORE the next poll rather than polling then dropping. The
    /// 65th event is never polled: `pollCalls == 64`, `remaining == total-64`.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Repeated pumps while the ring is full perform zero additional backend polls")
    func repeatedDrainsWhileFullPerformZeroPolls() {
        let backend = ScriptedReceiverBackend()
        let attachment = ReceiverAttachment(backend: backend)
        let total = 200
        backend.scriptEvents(makeDistinctAdds(total))

        attachment.drainTrackEvents()
        let afterFirst = backend.snapshot()
        #expect(attachment.stagedTrackEventCount == kSwiftStagingCap)
        #expect(afterFirst.pollCalls == kSwiftStagingCap)      // stopped before #65
        #expect(afterFirst.queuedEventsRemaining == total - kSwiftStagingCap)

        // The ring is full and nothing was consumed: these pumps are no-ops.
        attachment.drainTrackEvents()
        attachment.drainTrackEvents()
        let afterThree = backend.snapshot()
        #expect(afterThree.pollCalls == afterFirst.pollCalls)  // ZERO extra polls
        #expect(afterThree.pollCalls == kSwiftStagingCap)
        #expect(attachment.stagedTrackEventCount == kSwiftStagingCap)
        #expect(afterThree.queuedEventsRemaining == total - kSwiftStagingCap)
        #expect(afterThree.terminalPollsReturned == 0)
    }

    /// Obligation 2: with the event ring full and one track's TRACK_ADDED still
    /// retained in the backend, the real object-demand path must NOT adopt,
    /// discard, or surface an object referencing that track. The add stays in
    /// the backend, the object stays queued, and the demand reports `.empty`.
    /// Driven through `demandObject()` (drain-then-poll) and the scripted
    /// object queue -- runtime behavior, not source-text.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A full event ring never surfaces or discards an object whose add is still retained")
    func trackAddedPrecedesObjectAtCapacity() {
        let backend = ScriptedReceiverBackend()
        let attachment = ReceiverAttachment(backend: backend)

        // 65 adds: the bound stages t1..t64 and RETAINS handle 65's add in the
        // backend. The object below references exactly that retained handle.
        backend.scriptEvents(makeDistinctAdds(kSwiftStagingCap + 1))
        let storage = MockObjectStorage(media: [0xAB])
        let object = ReceiverPolledObject(
            handleID: UInt64(kSwiftStagingCap + 1), storage: storage,
            isKeyframe: true, endsGroup: false, isDatagram: false,
            presentationTime: .zero, decodeTime: .zero,
            compositionOffset: .zero, captureTime: nil)
        backend.scriptObjects([object])

        // demandObject drains track events first, then polls one object. With
        // the ring full and handle 65's add retained, the object must be left
        // in the backend -- neither surfaced nor discarded.
        let demand = attachment.demandObject()

        let snap = backend.snapshot()
        #expect(isEmptyDemand(demand))                         // not .object/terminal
        #expect(snap.objectPollCalls == 0)                     // object never polled
        #expect(snap.queuedObjectsRemaining == 1)              // still backend-owned
        #expect(storage.cleanupCount == 0)                     // not discarded
        #expect(attachment.stagedTrackEventCount == kSwiftStagingCap)
        #expect(snap.queuedEventsRemaining == 1)               // handle 65's add retained
    }

    /// Async-waiter liveness (0277 finding 1, synchronous half): a queued
    /// object whose delivery is blocked by a full event FIFO must NOT report
    /// its parked object waiter as ready -- `demandObject()` would only wake it
    /// into an immediate `.empty` and re-park (a hot retry). With 64 staged,
    /// handle 65's `TRACK_ADDED` retained, and an object queued,
    /// `objectWaiterReady` is false; consuming ONE staged event frees a slot,
    /// so a demand can make progress and it becomes true. This pins the exact
    /// readiness predicate: weakening it back to `hasQueuedObjects` (ignoring
    /// staging pressure) fails the first assertion.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A queued object is not waiter-ready while the event FIFO is full")
    func objectWaiterNotReadyWhileFull() {
        let backend = ScriptedReceiverBackend()
        let attachment = ReceiverAttachment(backend: backend)

        // 65 adds: stage t1..t64, retain handle 65's add; queue its object.
        backend.scriptEvents(makeDistinctAdds(kSwiftStagingCap + 1))
        let storage = MockObjectStorage(media: [0x42])
        backend.scriptObjects([ReceiverPolledObject(
            handleID: UInt64(kSwiftStagingCap + 1), storage: storage,
            isKeyframe: false, endsGroup: false, isDatagram: false,
            presentationTime: .zero, decodeTime: .zero,
            compositionOffset: .zero, captureTime: nil)])

        attachment.drainTrackEvents()
        #expect(attachment.stagedTrackEventCount == kSwiftStagingCap)
        #expect(backend.hasQueuedObjects)                 // an object IS queued
        // ...yet the waiter is NOT ready: the FIFO is full, so a demand would
        // return .empty and re-park -- exactly the edge that must not spin.
        #expect(attachment.objectWaiterReady == false)

        // Consume one staged event: the FIFO drops below the cap, so a demand
        // can now stage the retained add and make progress -> ready.
        _ = attachment.takeStagedTrackEvent()
        #expect(attachment.stagedTrackEventCount == kSwiftStagingCap - 1)
        #expect(attachment.objectWaiterReady == true)
    }

    /// Async-waiter liveness (0279, terminal half): a backend terminal is
    /// observed only through `pollObject()`, which `demandObject()` reaches
    /// solely after `drainTrackEvents()` returns not-full -- so a terminal
    /// behind a full FIFO must NOT report the waiter ready either, or the object
    /// stream hot-retries `.empty` until capacity frees. With 64 staged and the
    /// backend terminal, `objectWaiterReady` is false despite `isTerminal`;
    /// consuming ONE staged event makes it ready. (`isDetached` is the one
    /// exception -- `demandObject()` reports the Swift-side terminal without
    /// polling -- and is covered by the detach/close tests, not gated here.)
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A backend terminal is not waiter-ready while the event FIFO is full")
    func terminalWaiterNotReadyWhileFull() {
        let backend = ScriptedReceiverBackend()
        let attachment = ReceiverAttachment(backend: backend)

        // Exactly 64 events, then a terminal behind the full ring.
        backend.scriptEvents(makeDistinctAdds(kSwiftStagingCap))
        backend.scriptTerminal()

        attachment.drainTrackEvents()                 // stages 64, terminal unpolled
        #expect(attachment.stagedTrackEventCount == kSwiftStagingCap)
        #expect(backend.isTerminal)                   // terminal IS pending
        #expect(backend.snapshot().terminalPollsReturned == 0)
        // ...yet the waiter is NOT ready: while full, demandObject would return
        // .empty before it could ever poll (and observe) the terminal.
        #expect(attachment.objectWaiterReady == false)

        // Consume one staged event: room for the drain to reach and poll the
        // terminal on the next demand -> ready.
        _ = attachment.takeStagedTrackEvent()
        #expect(attachment.objectWaiterReady == true)
    }

    /// Obligation 3 (clean): a clean terminal scripted behind a >64-event
    /// suffix must not surface early. Staged events drain in exact order, the
    /// backend suffix is retained then resumed without loss, staging never
    /// exceeds 64 at any observation point, and only afterward does the clean
    /// `nil` (`.terminalClean`) disposition surface -- exactly once.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A clean terminal behind a full ring surfaces only after the ordered suffix drains, exactly once")
    func cleanTerminalSurfacesOnlyAfterOrderedDrain() {
        let outcome = drainToTerminal(total: kSwiftStagingCap + 5, fatal: false)

        #expect(outcome.delivered == (1...(kSwiftStagingCap + 5)).map { "t\($0)" })
        #expect(outcome.maxStaged <= kSwiftStagingCap)         // never over-staged
        #expect(outcome.terminalCleanSeen == 1)                // exactly once
        #expect(outcome.terminalFatalSeen == 0)
        #expect(outcome.terminalPollsReturned == 1)            // one .closed at the backend
        #expect(outcome.firstDrainStaged == kSwiftStagingCap)  // terminal not observed early
        #expect(outcome.firstDrainTerminalPolls == 0)
        #expect(outcome.firstDrainRemaining == 5)              // suffix retained behind the terminal
    }

    /// Obligation 3 (fatal): identical ordering/no-loss guarantees, and the
    /// exact `MoQServiceError.fatal(code:)` disposition surfaces once, only
    /// after the whole ordered suffix has drained.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A fatal terminal behind a full ring surfaces only after the ordered suffix drains, exactly once")
    func fatalTerminalSurfacesOnlyAfterOrderedDrain() {
        let code: UInt64 = 0xC0DE
        let outcome = drainToTerminal(total: kSwiftStagingCap + 5,
                                      fatal: true, code: code)

        #expect(outcome.delivered == (1...(kSwiftStagingCap + 5)).map { "t\($0)" })
        #expect(outcome.maxStaged <= kSwiftStagingCap)
        #expect(outcome.terminalCleanSeen == 0)
        #expect(outcome.terminalFatalSeen == 1)                // exactly once
        #expect(outcome.fatalCode == code)
        #expect(outcome.terminalPollsReturned == 1)
        #expect(outcome.firstDrainStaged == kSwiftStagingCap)
        #expect(outcome.firstDrainTerminalPolls == 0)
        #expect(outcome.firstDrainRemaining == 5)
    }

    private struct TerminalDrainOutcome {
        var delivered: [String] = []
        var maxStaged = 0
        var terminalCleanSeen = 0
        var terminalFatalSeen = 0
        var fatalCode: UInt64 = 0
        var terminalPollsReturned = 0
        var firstDrainStaged = 0
        var firstDrainTerminalPolls = 0
        var firstDrainRemaining = 0
    }

    /// Drive the ordered consume/re-drain cycle to the terminal, recording the
    /// exact terminal oracle. `total` distinct adds precede a clean or fatal
    /// terminal; the drain must stage at most 64 at any point and surface the
    /// terminal only once, after all `total` events in order.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    private func drainToTerminal(total: Int, fatal: Bool,
                                 code: UInt64 = 0) -> TerminalDrainOutcome {
        let backend = ScriptedReceiverBackend()
        let attachment = ReceiverAttachment(backend: backend)
        backend.scriptEvents(makeDistinctAdds(total))
        backend.scriptTerminal(fatal: fatal, code: code)

        var outcome = TerminalDrainOutcome()

        // Drain #1: bounded staging stops at 64; the terminal is behind the
        // (total-64)-event suffix and must NOT be observed yet.
        attachment.drainTrackEvents()
        let first = backend.snapshot()
        outcome.firstDrainStaged = attachment.stagedTrackEventCount
        outcome.firstDrainTerminalPolls = first.terminalPollsReturned
        outcome.firstDrainRemaining = first.queuedEventsRemaining
        outcome.maxStaged = max(outcome.maxStaged, attachment.stagedTrackEventCount)

        loop: while true {
            switch attachment.takeStagedTrackEvent() {
            case .item(.added(let track)):
                outcome.delivered.append(track.description.name)
            case .item:
                Issue.record("unexpected non-added staged event")
            case .empty:
                // Ring drained but the backend may still hold events: pump and
                // require forward progress, else stop (no terminal reachable).
                let before = attachment.stagedTrackEventCount
                attachment.drainTrackEvents()
                if attachment.stagedTrackEventCount == before { break loop }
            case .terminalClean:
                outcome.terminalCleanSeen += 1
                break loop
            case .terminalFatal(let observedCode):
                outcome.terminalFatalSeen += 1
                outcome.fatalCode = observedCode
                break loop
            }
            outcome.maxStaged = max(outcome.maxStaged,
                                    attachment.stagedTrackEventCount)
        }

        outcome.terminalPollsReturned = backend.snapshot().terminalPollsReturned
        return outcome
    }
}
