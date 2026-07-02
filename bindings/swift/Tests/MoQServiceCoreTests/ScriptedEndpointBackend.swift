import Foundation
@testable import MoQServiceCore

/// Scripted endpoint backend: implements the REAL wake/wait contract
/// (coalesced, level-retained activity flag; latch and terminal state win
/// over activity) so the engine tests exercise the C endpoint semantics, not
/// an idealized stand-in. Every blocking entry point records its calling
/// thread and rejects re-entrancy, deterministically proving the
/// single-service-thread confinement invariant. Every entry point after
/// `destroy()` records a violation -- a real C backend frees the endpoint
/// there, so ANY post-destroy call is a use-after-free in production.
///
/// Observation is race-free by construction: tests read recorders only via
/// `snapshot()` (an atomic, lock-guarded copy) or `awaitCondition`, whose
/// predicate receives the snapshot while the lock is held -- never through
/// the mutating storage directly.
///
/// All test-side control is condition-variable handshakes -- no sleeps, no
/// wall-clock dependence. `awaitCondition` ceilings exist only so a broken
/// engine fails instead of hanging CI.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class ScriptedEndpointBackend: EndpointBackend, @unchecked Sendable {

    /// An atomic copy of every recorder, taken under the backend lock.
    struct Snapshot {
        var waitEntries = 0
        var wakeCount = 0
        var drainCalls: [UInt64] = []
        var stopCount = 0
        var destroyCount = 0
        var violations: [String] = []
        var waitActive = false
        var drainBlocked = false
        var serviceThread: Thread?
    }

    private let cond = NSCondition()

    // Scripted endpoint state (cond-guarded).
    private var scriptedState: MoQEndpoint.State = .connecting
    private var scriptedFatal = false
    private var scriptedFatalCode: UInt64 = 0
    private var scriptedVersion: MoQVersion?
    private var interrupted = false
    private var wakeFlag = false                    /* level-retained activity */
    private var drainScript: [EndpointDrainResult] = []
    private var drainGate = false                   /* true: drain blocks */
    private var destroyed = false

    // Recorders (cond-guarded; exposed only via snapshot()).
    private var r = Snapshot()

    // MARK: EndpointBackend -- any-thread surface

    var state: MoQEndpoint.State {
        cond.lock()
        defer { cond.unlock() }
        tripwireLocked("state")
        return scriptedState
    }
    var negotiatedVersion: MoQVersion? {
        cond.lock()
        defer { cond.unlock() }
        tripwireLocked("negotiatedVersion")
        return scriptedVersion
    }
    var isFatal: Bool {
        cond.lock()
        defer { cond.unlock() }
        tripwireLocked("isFatal")
        return scriptedFatal
    }
    var fatalCode: UInt64 {
        cond.lock()
        defer { cond.unlock() }
        tripwireLocked("fatalCode")
        return scriptedFatalCode
    }

    func setInterrupted(_ value: Bool) {
        cond.lock()
        tripwireLocked("setInterrupted")
        interrupted = value
        cond.broadcast()
        cond.unlock()
    }

    func wake() {
        cond.lock()
        tripwireLocked("wake")
        wakeFlag = true
        r.wakeCount += 1
        cond.broadcast()
        cond.unlock()
    }

    // MARK: EndpointBackend -- service-thread surface

    func waitForActivity(timeoutMicroseconds: UInt64) -> EndpointWaitResult {
        cond.lock()
        tripwireLocked("waitForActivity")
        noteServiceThreadLocked("waitForActivity")
        if r.waitActive { r.violations.append("re-entrant waitForActivity") }
        r.waitActive = true
        r.waitEntries += 1
        cond.broadcast()                    /* unblock awaitCondition observers */
        while !wakeFlag && !interrupted && scriptedState != .closed {
            cond.wait()
        }
        let result: EndpointWaitResult
        if interrupted {
            result = .interrupted           /* latch wins; activity stays retained */
        } else if scriptedState == .closed {
            result = .closed
        } else {
            wakeFlag = false                /* single consumer takes the level */
            result = .activity
        }
        r.waitActive = false
        cond.broadcast()
        cond.unlock()
        return result
    }

    func drain(timeoutMicroseconds: UInt64) -> EndpointDrainResult {
        cond.lock()
        tripwireLocked("drain")
        noteServiceThreadLocked("drain")
        r.drainCalls.append(timeoutMicroseconds)
        r.drainBlocked = true
        cond.broadcast()
        while drainGate { cond.wait() }
        r.drainBlocked = false
        let result = drainScript.isEmpty
            ? EndpointDrainResult.complete
            : drainScript.removeFirst()
        cond.broadcast()
        cond.unlock()
        return result
    }

    func stop() {
        cond.lock()
        tripwireLocked("stop")
        noteServiceThreadLocked("stop")
        r.stopCount += 1
        cond.broadcast()
        cond.unlock()
    }

    func destroy() {
        cond.lock()
        tripwireLocked("destroy")            /* double destroy is a violation */
        noteServiceThreadLocked("destroy")
        destroyed = true
        r.destroyCount += 1
        cond.broadcast()
        cond.unlock()
    }

    // MARK: Test control

    /// Scripted state transition. Marks activity by default, like the real
    /// endpoint (state changes happen on pump cycles, which mark activity).
    func script(state: MoQEndpoint.State, fatal: Bool = false, code: UInt64 = 0,
                version: MoQVersion? = nil, markActivity: Bool = true) {
        cond.lock()
        scriptedState = state
        scriptedFatal = fatal
        scriptedFatalCode = code
        if let version { scriptedVersion = version }
        if markActivity { wakeFlag = true }
        cond.broadcast()
        cond.unlock()
    }

    func scriptDrain(_ results: [EndpointDrainResult]) {
        cond.lock()
        drainScript = results
        cond.unlock()
    }

    /// When gated, drain() blocks (recording drainBlocked) until released --
    /// lets a test act deterministically "while a drain slice is in flight".
    func setDrainGate(_ gated: Bool) {
        cond.lock()
        drainGate = gated
        cond.broadcast()
        cond.unlock()
    }

    /// An atomic copy of every recorder.
    func snapshot() -> Snapshot {
        cond.lock()
        defer { cond.unlock() }
        return r
    }

    /// Block the TEST thread until `predicate(snapshot)` holds (evaluated
    /// under the backend lock, re-checked on every state change). Pure
    /// handshake; the ceiling exists only so a broken engine fails instead
    /// of hanging. The predicate must read ONLY its snapshot argument --
    /// calling back into the backend would self-deadlock.
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

    /// Await the engine parked inside waitForActivity with >= `entries` total
    /// entries observed.
    @discardableResult
    func awaitParked(entries: Int, ceiling: TimeInterval = 10) -> Bool {
        awaitCondition(ceiling: ceiling) {
            $0.waitActive && $0.waitEntries >= entries
        }
    }

    /// Await a drain slice blocked on the gate.
    @discardableResult
    func awaitDrainBlocked(ceiling: TimeInterval = 10) -> Bool {
        awaitCondition(ceiling: ceiling) { $0.drainBlocked }
    }

    // MARK: Internals (cond held)

    private func tripwireLocked(_ what: String) {
        if destroyed { r.violations.append("\(what) after destroy") }
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

/// Thread-safe counter for predicate/callback evaluation counts.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class TestCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var n = 0
    /// Increment and return the new value.
    func next() -> Int {
        lock.lock(); defer { lock.unlock() }
        n += 1
        return n
    }
    var count: Int {
        lock.lock(); defer { lock.unlock() }
        return n
    }
}

/// Thread-safe recorder for job-side observations.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class EventRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var _events: [String] = []
    private var _threads: [Thread] = []

    func record(_ event: String) {
        lock.lock()
        _events.append(event)
        _threads.append(Thread.current)
        lock.unlock()
    }
    var events: [String] {
        lock.lock(); defer { lock.unlock() }
        return _events
    }
    var threads: [Thread] {
        lock.lock(); defer { lock.unlock() }
        return _threads
    }
}

/// A reusable test gate: blocks a service-thread job until released from the
/// test thread, with a handshake to observe "the job is now blocked".
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class TestGate: @unchecked Sendable {
    private let cond = NSCondition()
    private var open = false
    private var blockedCount = 0

    /// Called from the job: blocks until `openGate()`.
    func pass() {
        cond.lock()
        blockedCount += 1
        cond.broadcast()
        while !open { cond.wait() }
        cond.unlock()
    }

    /// Called from the test: release all current and future passers.
    func openGate() {
        cond.lock()
        open = true
        cond.broadcast()
        cond.unlock()
    }

    /// Await >= n jobs blocked at the gate.
    @discardableResult
    func awaitBlocked(count: Int, ceiling: TimeInterval = 10) -> Bool {
        cond.lock()
        defer { cond.unlock() }
        let deadline = Date().addingTimeInterval(ceiling)
        while blockedCount < count {
            if !cond.wait(until: deadline) { return false }
        }
        return true
    }
}
