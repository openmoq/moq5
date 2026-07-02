import Foundation

/// The per-endpoint confinement engine: one dedicated service thread that is
/// "the app thread" from the C service contract's perspective. Every backend
/// call that blocks or mutates (`waitForActivity`/`drain`/`stop`/`destroy`,
/// and all future receiver/sender calls) happens on this thread; async
/// surfaces enqueue work here and suspend on continuations.
///
/// The load-bearing C fact the loop is built around: the endpoint wake is
/// coalesced, LEVEL-retained, and single-consumer -- so exactly one blocking
/// C wait may exist, and a wake that lands before the next wait is never
/// lost. The loop therefore blocks in at most one `waitForActivity` call;
/// every other park is a Swift-side condition variable (latched, terminal,
/// or idle-after-close), which is invisible to the C tier.
///
/// Loop pass order: run queued jobs (FIFO, exactly once) -> handle shutdown
/// -> handle the latch (fail every parked waiter with `.interrupted`; latch
/// beats level, C §5.3) -> re-evaluate condition waiters -> block. Later
/// slices insert the track-event drain and parked object consumers between
/// the job and waiter steps.
///
/// Submission contract: once shutdown begins (`close()` or the deinit
/// backstop), NEW submissions fail deterministically with
/// ``MoQServiceError/closed`` and never run; jobs accepted before shutdown
/// still run before teardown.
///
/// Package callers must never await engine operations from inside a job --
/// the job would suspend the only thread that can complete it.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
package final class EndpointEngine: @unchecked Sendable {

    /// A condition waiter's answer, re-evaluated on the service thread each
    /// loop pass. Predicates read backend snapshots and must not block.
    package enum WaiterVerdict: Sendable {
        case pending
        case satisfied
        case failure(MoQServiceError)
    }

    private enum Phase { case running, stopping, stopped }

    /// One queued unit of service-thread work. All vars are engine-lock
    /// guarded; `body` runs on the service thread OUTSIDE the lock.
    private final class Job: @unchecked Sendable {
        var started = false
        var cancelled = false
        var body: (() -> Void)?
        /// Resumes the owner throwing CancellationError; taken exactly once.
        var cancelAction: (() -> Void)?
    }

    private final class Waiter: @unchecked Sendable {
        let predicate: @Sendable () -> WaiterVerdict
        /// Engine-lock guarded; nil once resumed (resolution or cancel).
        var continuation: CheckedContinuation<Void, any Error>?
        var cancelled = false
        init(_ predicate: @escaping @Sendable () -> WaiterVerdict) {
            self.predicate = predicate
        }
    }

    package let backend: any EndpointBackend

    private let cond = NSCondition()
    /* cond-guarded state */
    private var jobs: [Job] = []
    private var waiters: [Waiter] = []
    private var latched = false
    private var phase: Phase = .running
    private var dirty = false
    private var closeContinuations: [CheckedContinuation<Void, Never>] = []
    /// Once true, NO backend method may be called anymore: the teardown pass
    /// sets it (under the lock) immediately before `stop()`/`destroy()`, and
    /// a real C backend frees the endpoint in `destroy()`. Every any-thread
    /// path checks it under the lock; the accessors below synthesize their
    /// post-retirement answers engine-side.
    private var backendRetired = false
    /// The negotiated version snapshotted at retirement, so the accessor
    /// keeps answering after the backend is gone.
    private var retiredNegotiatedVersion: MoQVersion?

    /// Defensive ceiling on one C wait; wakes make it irrelevant in practice
    /// and a timeout just loops another pass.
    private static let waitSliceMicroseconds: UInt64 = 60_000_000
    /// One C drain slice: bounds how long the job queue is blocked and how
    /// stale a cancellation/shutdown check can get (the C sender slices its
    /// BLOCK_TIMEOUT waits at the same 50 ms granularity).
    package static let drainSliceMicroseconds: UInt64 = 50_000

    package init(backend: any EndpointBackend) {
        self.backend = backend
        let thread = Thread { [self] in run() }
        thread.name = "moq.endpoint.service"
        thread.start()
    }

    // MARK: State (safe before and after teardown)

    /// The endpoint state; `.closed` once the backend has been destroyed.
    package var state: MoQEndpoint.State {
        cond.lock()
        defer { cond.unlock() }
        return backendRetired ? .closed : backend.state
    }

    /// The negotiated version; the retirement snapshot once the backend has
    /// been destroyed.
    package var negotiatedVersion: MoQVersion? {
        cond.lock()
        defer { cond.unlock() }
        return backendRetired ? retiredNegotiatedVersion
                              : backend.negotiatedVersion
    }

    // MARK: Submission surface

    /// Fire-and-forget service-thread work (the C `post()` analog). Returns
    /// false -- and never runs `body` -- once shutdown has begun.
    @discardableResult
    package func post(_ body: @escaping @Sendable () -> Void) -> Bool {
        let job = Job()
        job.body = body
        cond.lock()
        guard phase == .running else {
            cond.unlock()
            return false
        }
        jobs.append(job)
        nudgeLocked()
        /* Under the lock: phase == .running implies the backend is alive
         * (retirement happens under this lock, in the stopping phase). The
         * engine -> backend lock order is one-way, so nesting is safe. */
        backend.wake()
        cond.unlock()
        return true
    }

    /// Run `body` on the service thread and return its result. Throws
    /// ``MoQServiceError/closed`` if shutdown has begun, `CancellationError`
    /// if the task is cancelled before the job starts (a started job always
    /// delivers its result).
    package func perform<T: Sendable>(
        _ body: @escaping @Sendable () throws -> T
    ) async throws -> T {
        let job = Job()
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation {
                (cont: CheckedContinuation<T, any Error>) in
                cond.lock()
                if job.cancelled {
                    cond.unlock()
                    cont.resume(throwing: CancellationError())
                    return
                }
                guard phase == .running else {
                    cond.unlock()
                    cont.resume(throwing: MoQServiceError.closed)
                    return
                }
                job.body = { cont.resume(with: Result { try body() }) }
                job.cancelAction = { cont.resume(throwing: CancellationError()) }
                jobs.append(job)
                nudgeLocked()
                backend.wake()          /* under the lock; see post() */
                cond.unlock()
            }
        } onCancel: {
            cancelJob(job)
        }
    }

    /// Park until `predicate` (evaluated on the service thread each pass)
    /// reports satisfied or a failure. The interrupt latch fails every
    /// parked -- and newly registered -- waiter with
    /// ``MoQServiceError/interrupted`` (latch beats level); shutdown fails
    /// them with ``MoQServiceError/closed``. Cancellation resumes from the
    /// cancelling thread with no backend calls.
    package func waitUntil(
        _ predicate: @escaping @Sendable () -> WaiterVerdict
    ) async throws {
        let waiter = Waiter(predicate)
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation {
                (cont: CheckedContinuation<Void, any Error>) in
                cond.lock()
                if waiter.cancelled {
                    cond.unlock()
                    cont.resume(throwing: CancellationError())
                    return
                }
                guard phase == .running else {
                    cond.unlock()
                    cont.resume(throwing: MoQServiceError.closed)
                    return
                }
                waiter.continuation = cont
                waiters.append(waiter)
                nudgeLocked()
                backend.wake()          /* under the lock; see post() */
                cond.unlock()
            }
        } onCancel: {
            cond.lock()
            waiter.cancelled = true
            let cont = waiter.continuation
            waiter.continuation = nil
            cond.unlock()
            cont?.resume(throwing: CancellationError())
        }
    }

    // MARK: Latch

    /// Set/clear the sticky interrupt latch. The engine's mirror is the
    /// Swift-side source of truth for the loop; the backend is kept in
    /// lockstep so C blocking calls agree. A no-op toward the backend once
    /// it has been destroyed.
    package func setInterrupted(_ value: Bool) {
        cond.lock()
        latched = value
        nudgeLocked()
        if !backendRetired {
            backend.setInterrupted(value)
            backend.wake()
        }
        cond.unlock()
    }

    // MARK: Drain

    /// Flush locally queued reliable stream data (the C drain), sliced so
    /// cancellation and shutdown are honored between slices. The budget is
    /// accounted in requested slices, deterministically. Throws:
    /// `.wouldBlock` when the budget elapses before the flush completes,
    /// `.interrupted` / `.closed` / `.fatal` / `.unsupported` mapping the C
    /// returns, `CancellationError` between slices (the in-flight C slice
    /// completes first on the service thread).
    package func drain(timeoutMicroseconds: UInt64) async throws {
        let job = Job()
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation {
                (cont: CheckedContinuation<Void, any Error>) in
                cond.lock()
                if job.cancelled {
                    cond.unlock()
                    cont.resume(throwing: CancellationError())
                    return
                }
                guard phase == .running else {
                    cond.unlock()
                    cont.resume(throwing: MoQServiceError.closed)
                    return
                }
                job.body = { [self] in
                    cont.resume(with: Result {
                        try runDrainSlices(budget: timeoutMicroseconds, job: job)
                    })
                }
                job.cancelAction = { cont.resume(throwing: CancellationError()) }
                jobs.append(job)
                nudgeLocked()
                backend.wake()          /* under the lock; see post() */
                cond.unlock()
            }
        } onCancel: {
            cancelJob(job)
        }
    }

    /// Service thread only.
    private func runDrainSlices(budget: UInt64, job: Job) throws {
        var remaining = budget
        while true {
            cond.lock()
            let abandoned = job.cancelled
            let stopping = phase != .running
            cond.unlock()
            if abandoned { throw CancellationError() }
            if stopping { throw MoQServiceError.closed }

            let slice = min(remaining, Self.drainSliceMicroseconds)
            switch backend.drain(timeoutMicroseconds: slice) {
            case .complete:
                return
            case .interrupted:
                throw MoQServiceError.interrupted
            case .closed:
                throw backend.isFatal
                    ? MoQServiceError.fatal(code: backend.fatalCode)
                    : MoQServiceError.closed
            case .unsupported:
                throw MoQServiceError.unsupported
            case .timeout:
                remaining -= slice
                if remaining == 0 { throw MoQServiceError.wouldBlock }
            }
        }
    }

    // MARK: Shutdown

    /// Tear down: run accepted jobs, fail parked waiters with `.closed`,
    /// stop and destroy the backend on the service thread, then return.
    /// Idempotent; concurrent calls all complete after the one teardown.
    /// Must not be called from a job (it would suspend the service thread).
    package func close() async {
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            cond.lock()
            switch phase {
            case .stopped:
                cond.unlock()
                cont.resume()
            case .running, .stopping:
                if phase == .running { phase = .stopping }
                closeContinuations.append(cont)
                nudgeLocked()
                if !backendRetired { backend.wake() }
                cond.unlock()
            }
        }
    }

    /// Non-blocking, best-effort shutdown trigger (the `deinit` backstop).
    /// Idempotent; touches the backend only when it actually starts the
    /// shutdown (so it is a pure no-op after teardown).
    package func beginShutdown() {
        cond.lock()
        if phase == .running {
            phase = .stopping
            nudgeLocked()
            backend.wake()              /* .running implies not retired */
        }
        cond.unlock()
    }

    // MARK: Internals

    /// Callers hold the lock.
    private func nudgeLocked() {
        dirty = true
        cond.broadcast()
    }

    private func cancelJob(_ job: Job) {
        cond.lock()
        job.cancelled = true
        let cancel = job.started ? nil : job.cancelAction
        job.cancelAction = nil
        cond.unlock()
        cancel?()
    }

    /// Callers hold the lock. Empties the waiter list and returns the
    /// continuations still parked (skipping cancelled ones).
    private func takeWaitersLocked() -> [CheckedContinuation<Void, any Error>] {
        var conts: [CheckedContinuation<Void, any Error>] = []
        for waiter in waiters {
            if let cont = waiter.continuation {
                waiter.continuation = nil
                conts.append(cont)
            }
        }
        waiters.removeAll()
        return conts
    }

    /// The service-thread loop.
    private func run() {
        while true {
            cond.lock()
            dirty = false

            /* 1. Jobs: FIFO, exactly once; accepted-before-stop jobs run. */
            while !jobs.isEmpty {
                let job = jobs.removeFirst()
                if job.cancelled { continue }
                job.started = true
                job.cancelAction = nil
                let body = job.body
                cond.unlock()
                body?()
                cond.lock()
            }

            /* 2. Shutdown: fail parked waiters, tear down, resume closers.
             * The backend is RETIRED under the lock before stop()/destroy():
             * from here on no other thread makes backend calls, and the
             * accessors answer from the snapshot. */
            if phase == .stopping {
                let failed = takeWaitersLocked()
                retiredNegotiatedVersion = backend.negotiatedVersion
                backendRetired = true
                cond.unlock()
                for cont in failed {
                    cont.resume(throwing: MoQServiceError.closed)
                }
                backend.stop()
                backend.destroy()
                cond.lock()
                phase = .stopped
                let closers = closeContinuations
                closeContinuations = []
                cond.unlock()
                for cont in closers { cont.resume() }
                return
            }

            /* 3. Latch: every parked waiter fails NOW (latch beats level,
             * C §5.3 -- including waiters registered after the latch was
             * set), then park internally; never hot-loop the C wait. */
            if latched {
                let failed = takeWaitersLocked()
                cond.unlock()
                for cont in failed {
                    cont.resume(throwing: MoQServiceError.interrupted)
                }
                cond.lock()
                while latched && !dirty && phase == .running { cond.wait() }
                cond.unlock()
                continue
            }

            /* 4. Waiters: sweep cancelled, evaluate the rest. Record
             * whether the backend was ALREADY terminal when this sweep
             * began: the block step uses it to detect a close that raced
             * the sweep (closed is a terminal state -- it never reverts --
             * so a true observation here means every predicate below saw
             * the terminal state too). */
            waiters.removeAll { $0.continuation == nil }
            let snapshot = waiters
            cond.unlock()
            let terminalAtSweep = backend.state == .closed
            for waiter in snapshot {
                let verdict = waiter.predicate()
                if case .pending = verdict { continue }
                cond.lock()
                let cont = waiter.continuation
                waiter.continuation = nil
                cond.unlock()
                guard let cont else { continue }   /* cancelled meanwhile */
                switch verdict {
                case .satisfied: cont.resume()
                case .failure(let error): cont.resume(throwing: error)
                case .pending: break
                }
            }

            /* 5. Block: in the single C wait normally; on the internal
             * condvar once the backend is terminal (the C wait would return
             * CLOSED immediately -- a hot spin). */
            cond.lock()
            guard jobs.isEmpty && !dirty && phase == .running && !latched else {
                cond.unlock()
                continue
            }
            if backend.state == .closed {
                /* The close may have landed AFTER this pass's waiter sweep
                 * began -- those waiters have not observed the terminal
                 * state yet and would strand in the park below (nothing
                 * external need ever dirty the engine again). Run one more
                 * pass first; its sweep starts with terminal already
                 * observed, so this cannot loop. */
                if !terminalAtSweep && !waiters.isEmpty {
                    cond.unlock()
                    continue
                }
                while jobs.isEmpty && !dirty && phase == .running && !latched {
                    cond.wait()
                }
                cond.unlock()
            } else {
                cond.unlock()
                _ = backend.waitForActivity(
                    timeoutMicroseconds: Self.waitSliceMicroseconds)
                /* The result is not consumed here: the next pass reads the
                 * latch mirror and backend state, which the wait return
                 * reflects (interrupted -> step 3, closed -> step 5). */
            }
        }
    }
}
