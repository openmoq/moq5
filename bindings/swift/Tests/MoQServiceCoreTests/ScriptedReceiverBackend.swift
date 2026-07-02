import Foundation
@testable import MoQServiceCore

/// Scripted receiver backend: a queue of polled events plus a terminal flag,
/// mirroring the C `moq_media_receiver_poll_track` contract (drain to empty
/// -> DONE; empty AND terminal -> CLOSED; events queued before the terminal
/// transition still drain; NOT latch-gated). Records calling threads and
/// trips on any call after detach() -- a real C backend frees the receiver
/// there.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class ScriptedReceiverBackend: ReceiverBackend, @unchecked Sendable {

    /// @unchecked: an immutable value copy; the Thread is identity-only.
    struct SubscribeCall: Equatable, Sendable {
        var handleID: UInt64
        var start: StartMode
        var priority: UInt8?
    }

    /// @unchecked: an immutable value copy; the Thread is identity-only.
    struct Snapshot: @unchecked Sendable {
        var pollCalls = 0
        var objectPollCalls = 0
        var detachCalls = 0
        var subscribeCalls: [SubscribeCall] = []
        var unsubscribeCalls: [UInt64] = []
        var violations: [String] = []
        var serviceThread: Thread?
        var objectPollBlocked = false
    }

    private let cond = NSCondition()
    private var queue: [ReceiverPolledEvent] = []
    private var objectQueue: [ReceiverPolledObject] = []
    private var subscribeResults: [ReceiverCommandResult] = []
    private var terminal = false
    private var scriptedFatal = false
    private var scriptedFatalCode: UInt64 = 0
    private var detached = false
    private var objectGate = false      /* true: pollObject blocks mid-poll */
    private var r = Snapshot()
    /// Models the C fact that poll_object checks the ENDPOINT latch (poll
    /// of track events does not). Tests wire `{ endpointBackend.isLatched }`.
    private let latch: @Sendable () -> Bool
    /// Test hook: called (under the lock) when detach() runs, for
    /// cross-backend teardown-ordering assertions.
    var onDetach: (@Sendable () -> Void)?

    init(latch: @escaping @Sendable () -> Bool = { false }) {
        self.latch = latch
    }

    // MARK: ReceiverBackend

    var isFatal: Bool {
        cond.lock(); defer { cond.unlock() }
        return scriptedFatal
    }
    var fatalCode: UInt64 {
        cond.lock(); defer { cond.unlock() }
        return scriptedFatalCode
    }

    func pollTrackEvent() -> ReceiverPollResult {
        cond.lock()
        defer { cond.unlock() }
        tripwireLocked("pollTrackEvent")
        noteServiceThreadLocked("pollTrackEvent")
        r.pollCalls += 1
        cond.broadcast()
        if !queue.isEmpty { return .event(queue.removeFirst()) }
        return terminal ? .closed : .none
    }

    var isTerminal: Bool {
        cond.lock(); defer { cond.unlock() }
        return terminal
    }

    var hasQueuedObjects: Bool {
        cond.lock(); defer { cond.unlock() }
        return !objectQueue.isEmpty
    }

    func pollObject() -> ReceiverObjectPollResult {
        cond.lock()
        tripwireLocked("pollObject")
        noteServiceThreadLocked("pollObject")
        r.objectPollCalls += 1
        r.objectPollBlocked = true
        cond.broadcast()
        while objectGate { cond.wait() }
        r.objectPollBlocked = false
        defer {
            cond.broadcast()
            cond.unlock()
        }
        /* C order (media_receiver.c poll_object): the LATCH gates first,
         * before any queue inspection. */
        if latch() { return .interrupted }
        if !objectQueue.isEmpty { return .object(objectQueue.removeFirst()) }
        return terminal ? .closed : .none
    }

    func subscribe(handleID: UInt64, start: StartMode,
                   priority: UInt8?) -> ReceiverCommandResult {
        cond.lock()
        defer { cond.unlock() }
        tripwireLocked("subscribe")
        noteServiceThreadLocked("subscribe")
        r.subscribeCalls.append(SubscribeCall(
            handleID: handleID, start: start, priority: priority))
        cond.broadcast()
        return subscribeResults.isEmpty ? .ok : subscribeResults.removeFirst()
    }

    func unsubscribe(handleID: UInt64) -> ReceiverCommandResult {
        cond.lock()
        defer { cond.unlock() }
        tripwireLocked("unsubscribe")
        noteServiceThreadLocked("unsubscribe")
        r.unsubscribeCalls.append(handleID)
        cond.broadcast()
        return subscribeResults.isEmpty ? .ok : subscribeResults.removeFirst()
    }

    func detach() {
        cond.lock()
        tripwireLocked("detach")
        noteServiceThreadLocked("detach")
        detached = true
        r.detachCalls += 1
        /* The C destroy frees queued undelivered objects; model it so leak
         * assertions have teeth. */
        let undelivered = objectQueue
        objectQueue.removeAll()
        let hook = onDetach
        cond.broadcast()
        cond.unlock()
        for polled in undelivered { polled.storage.cleanup() }
        hook?()
    }

    // MARK: Test control

    func scriptEvents(_ events: [ReceiverPolledEvent]) {
        cond.lock()
        queue.append(contentsOf: events)
        cond.broadcast()
        cond.unlock()
    }

    func scriptObjects(_ objects: [ReceiverPolledObject]) {
        cond.lock()
        objectQueue.append(contentsOf: objects)
        cond.broadcast()
        cond.unlock()
    }

    func scriptSubscribeResults(_ results: [ReceiverCommandResult]) {
        cond.lock()
        subscribeResults = results
        cond.unlock()
    }

    /// When gated, pollObject blocks mid-poll (after the job started,
    /// before the transfer completes) until released -- the cancel-race
    /// window.
    func setObjectGate(_ gated: Bool) {
        cond.lock()
        objectGate = gated
        cond.broadcast()
        cond.unlock()
    }

    @discardableResult
    func awaitObjectPollBlocked(ceiling: TimeInterval = 10) async -> Bool {
        await awaitCondition(ceiling: ceiling) { $0.objectPollBlocked }
    }

    /// After the queued events drain, polls return .closed.
    func scriptTerminal(fatal: Bool = false, code: UInt64 = 0) {
        cond.lock()
        terminal = true
        scriptedFatal = fatal
        scriptedFatalCode = code
        cond.broadcast()
        cond.unlock()
    }

    func snapshot() -> Snapshot {
        cond.lock(); defer { cond.unlock() }
        return r
    }

    /// Handshake await on the recorders; predicate reads ONLY its argument.
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

    /// Async contexts get the off-pool variant automatically (overload by
    /// context): the blocking wait runs on GCD, never a cooperative thread.
    @discardableResult
    func awaitCondition(ceiling: TimeInterval = 10,
                        _ predicate: @escaping @Sendable (Snapshot) -> Bool
    ) async -> Bool {
        await offPool { self.awaitCondition(ceiling: ceiling, predicate) }
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

// MARK: Event factories

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension ReceiverPolledEvent {
    static func added(_ id: UInt64, name: String,
                      mediaType: MediaType = .video) -> ReceiverPolledEvent {
        ReceiverPolledEvent(
            kind: .added, handleID: id,
            trackDescription: TrackDescription(
                name: name, mediaType: mediaType, packaging: .cmaf))
    }
    static func updated(_ id: UInt64,
                        _ description: TrackDescription) -> ReceiverPolledEvent {
        ReceiverPolledEvent(kind: .updated, handleID: id,
                            trackDescription: description)
    }
    static func removed(_ id: UInt64) -> ReceiverPolledEvent {
        ReceiverPolledEvent(kind: .removed, handleID: id, trackDescription: nil)
    }
    static func ended(_ id: UInt64) -> ReceiverPolledEvent {
        ReceiverPolledEvent(kind: .ended, handleID: id, trackDescription: nil)
    }
    static var catalogReady: ReceiverPolledEvent {
        ReceiverPolledEvent(kind: .catalogReady, handleID: 0,
                            trackDescription: nil)
    }
}
