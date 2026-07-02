import Foundation
@testable import MoQServiceCore

/// Scripted sender backend mirroring the C media_sender contract: write/
/// endTrack results, bounded sender waits (the S0 `moq_media_sender_wait`
/// shape: latch beats terminal beats level), transfer-on-ok recording, and
/// the standard tripwires (service thread only; nothing after detach).
///
/// Defaults model the healthy path: `ready` starts true; an empty write
/// script answers `.ok` when ready and `.wouldBlock` when not (the C
/// pre-ready queue is bounded); an empty wait script answers `.activity`.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class ScriptedSenderBackend: SenderBackend, @unchecked Sendable {

    /// @unchecked: an immutable value copy; the Thread is identity-only.
    struct Snapshot: @unchecked Sendable {
        var addTrackNames: [String] = []
        var writeCalls: [UInt64] = []           /* handleID per attempt */
        var acceptedPayloads: [Data] = []       /* transferred on .ok only */
        var endTrackCalls: [UInt64] = []
        var waitCalls: [UInt64] = []            /* requested slice args */
        var detachCalls = 0
        var violations: [String] = []
        var serviceThread: Thread?
        var waitBlocked = false
    }

    private let cond = NSCondition()
    private var readyFlag = true
    private var terminal = false
    private var scriptedFatal = false
    private var scriptedFatalCode: UInt64 = 0
    private var detached = false
    private var writeScript: [SenderOperationResult] = []
    private var endTrackScript: [SenderOperationResult] = []
    private var addTrackScript: [SenderAddTrackResult] = []
    private var waitScript: [SenderWaitOutcome] = []
    private var nextHandleID: UInt64 = 100
    private var waitGate = false
    private var r = Snapshot()
    private let latch: @Sendable () -> Bool

    init(latch: @escaping @Sendable () -> Bool = { false }) {
        self.latch = latch
    }

    // MARK: SenderBackend -- any-thread snapshots

    var isReady: Bool {
        cond.lock(); defer { cond.unlock() }
        tripwireLocked("isReady")
        return readyFlag
    }
    var isTerminal: Bool {
        cond.lock(); defer { cond.unlock() }
        tripwireLocked("isTerminal")
        return terminal
    }
    var isFatal: Bool {
        cond.lock(); defer { cond.unlock() }
        tripwireLocked("isFatal")
        return scriptedFatal
    }
    var fatalCode: UInt64 {
        cond.lock(); defer { cond.unlock() }
        tripwireLocked("fatalCode")
        return scriptedFatalCode
    }

    // MARK: SenderBackend -- service-thread surface

    func addTrack(_ configuration: TrackConfiguration) -> SenderAddTrackResult {
        cond.lock()
        defer { cond.unlock() }
        tripwireLocked("addTrack")
        noteServiceThreadLocked("addTrack")
        r.addTrackNames.append(configuration.name)
        cond.broadcast()
        if !addTrackScript.isEmpty { return addTrackScript.removeFirst() }
        nextHandleID += 1
        return .track(handleID: nextHandleID)
    }

    func write(handleID: UInt64, object: OutgoingObject) -> SenderOperationResult {
        cond.lock()
        defer { cond.unlock() }
        tripwireLocked("write")
        noteServiceThreadLocked("write")
        r.writeCalls.append(handleID)
        cond.broadcast()
        /* C order: the latch refuses the write before anything else. */
        if latch() { return .interrupted }
        let result: SenderOperationResult
        if !writeScript.isEmpty {
            result = writeScript.removeFirst()
        } else if terminal {
            result = .closed
        } else {
            result = readyFlag ? .ok : .wouldBlock  /* bounded pre-ready queue */
        }
        if case .ok = result {
            r.acceptedPayloads.append(object.payload)  /* transfer on OK only */
        }
        return result
    }

    func endTrack(handleID: UInt64) -> SenderOperationResult {
        cond.lock()
        defer { cond.unlock() }
        tripwireLocked("endTrack")
        noteServiceThreadLocked("endTrack")
        r.endTrackCalls.append(handleID)
        cond.broadcast()
        if latch() { return .interrupted }
        if !endTrackScript.isEmpty { return endTrackScript.removeFirst() }
        return terminal ? .closed : .ok
    }

    func wait(timeoutMicroseconds: UInt64) -> SenderWaitOutcome {
        cond.lock()
        tripwireLocked("wait")
        noteServiceThreadLocked("wait")
        r.waitCalls.append(timeoutMicroseconds)
        r.waitBlocked = true
        cond.broadcast()
        while waitGate { cond.wait() }
        r.waitBlocked = false
        defer {
            cond.broadcast()
            cond.unlock()
        }
        /* S0 priority: latch, then terminal, WIN over the level. */
        if latch() { return .interrupted }
        if terminal { return .closed }
        if !waitScript.isEmpty { return waitScript.removeFirst() }
        return .activity
    }

    func detach() {
        cond.lock()
        tripwireLocked("detach")
        noteServiceThreadLocked("detach")
        detached = true
        r.detachCalls += 1
        let hook = onDetach
        cond.broadcast()
        cond.unlock()
        hook?()
    }

    /// Test hook: called when detach() runs, for cross-backend
    /// teardown-ordering assertions.
    var onDetach: (@Sendable () -> Void)?

    // MARK: Test control

    func setReady(_ ready: Bool) {
        cond.lock()
        readyFlag = ready
        cond.broadcast()
        cond.unlock()
    }

    func scriptTerminal(fatal: Bool = false, code: UInt64 = 0) {
        cond.lock()
        terminal = true
        scriptedFatal = fatal
        scriptedFatalCode = code
        cond.broadcast()
        cond.unlock()
    }

    func scriptWrites(_ results: [SenderOperationResult]) {
        cond.lock()
        writeScript = results
        cond.unlock()
    }

    func scriptEndTracks(_ results: [SenderOperationResult]) {
        cond.lock()
        endTrackScript = results
        cond.unlock()
    }

    func scriptAddTracks(_ results: [SenderAddTrackResult]) {
        cond.lock()
        addTrackScript = results
        cond.unlock()
    }

    func scriptWaits(_ outcomes: [SenderWaitOutcome]) {
        cond.lock()
        waitScript = outcomes
        cond.unlock()
    }

    /// When gated, wait() blocks mid-slice until released -- the
    /// cancellation-between-slices window.
    func setWaitGate(_ gated: Bool) {
        cond.lock()
        waitGate = gated
        cond.broadcast()
        cond.unlock()
    }

    func snapshot() -> Snapshot {
        cond.lock(); defer { cond.unlock() }
        return r
    }

    @discardableResult
    func awaitCondition(ceiling: TimeInterval = 10,
                        _ predicate: (Snapshot) -> Bool) -> Bool {
        cond.lock()
        defer { cond.unlock() }
        let deadline = Date().addingTimeInterval(ceiling)
        while !predicate(r) {
            if !cond.wait(until: deadline) { return false }
        }
        return true
    }

    @discardableResult
    func awaitCondition(ceiling: TimeInterval = 10,
                        _ predicate: @escaping @Sendable (Snapshot) -> Bool
    ) async -> Bool {
        await offPool { self.awaitCondition(ceiling: ceiling, predicate) }
    }

    @discardableResult
    func awaitWaitBlocked(ceiling: TimeInterval = 10) async -> Bool {
        await awaitCondition(ceiling: ceiling) { $0.waitBlocked }
    }

    // MARK: Internals (cond held)

    private func tripwireLocked(_ what: String) {
        if detached { r.violations.append("\(what) after detach") }
    }

    private func noteServiceThreadLocked(_ what: String) {
        if let thread = r.serviceThread {
            if thread !== Thread.current {
                r.violations.append("\(what) off the service thread")
            }
        } else {
            r.serviceThread = Thread.current
        }
    }
}
