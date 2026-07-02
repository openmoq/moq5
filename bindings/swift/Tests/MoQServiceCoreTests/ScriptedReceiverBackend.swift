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
    struct Snapshot: @unchecked Sendable {
        var pollCalls = 0
        var detachCalls = 0
        var violations: [String] = []
        var serviceThread: Thread?
    }

    private let cond = NSCondition()
    private var queue: [ReceiverPolledEvent] = []
    private var terminal = false
    private var scriptedFatal = false
    private var scriptedFatalCode: UInt64 = 0
    private var detached = false
    private var r = Snapshot()
    /// Test hook: called (under the lock) when detach() runs, for
    /// cross-backend teardown-ordering assertions.
    var onDetach: (@Sendable () -> Void)?

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

    // MARK: Test control

    func scriptEvents(_ events: [ReceiverPolledEvent]) {
        cond.lock()
        queue.append(contentsOf: events)
        cond.broadcast()
        cond.unlock()
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
