import Testing
import Foundation
@testable import MoQServiceCore

/* Deterministic engine tests: every ordering is enforced with condition-
 * variable handshakes on the ScriptedEndpointBackend / TestGate -- no sleeps,
 * no wall-clock dependence. Recorders are read only through the backend's
 * atomic snapshot() / awaitCondition(_:) so observation is race-free.
 * Ceilings on awaits exist only so a broken engine fails instead of hanging.
 *
 * `snapshot().violations` is a standing tripwire: the backend records any
 * re-entrant wait, any off-service-thread blocking call, and ANY call after
 * destroy() (a use-after-free against a real C backend). */

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func makeEndpoint(
    _ backend: ScriptedEndpointBackend
) -> MoQEndpoint {
    MoQEndpoint(
        configuration: .init(url: URL(string: "moqt://relay.test:4443")!),
        engine: EndpointEngine(backend: backend))
}

@Suite("Engine confinement")
struct EngineConfinementTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Jobs run FIFO, exactly once, on one service thread")
    func jobsRunFIFO() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        let recorder = EventRecorder()

        /* post() submission order from one thread IS execution order. */
        endpoint.engine.post { recorder.record("a") }
        endpoint.engine.post { recorder.record("b") }
        endpoint.engine.post { recorder.record("c") }
        /* perform() round-trips through the queue, so awaiting it proves the
         * earlier posts ran. */
        let value = try await endpoint.engine.perform { () -> Int in
            recorder.record("d")
            return 7
        }
        #expect(value == 7)
        #expect(recorder.events == ["a", "b", "c", "d"])

        /* All on ONE thread, and never the test thread. */
        let threads = recorder.threads
        #expect(Set(threads.map(ObjectIdentifier.init)).count == 1)
        #expect(threads.allSatisfy { $0 !== Thread.current })
        await endpoint.close()
        #expect(backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A submission while parked in the C wait nudges it awake")
    func nudgeUnblocksWait() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)

        #expect(await backend.awaitParked(entries: 1))
        let ran = try await endpoint.engine.perform { true }
        #expect(ran)
        /* The engine re-parks after the job: the wait was genuinely exited
         * and re-entered, not bypassed. */
        #expect(await backend.awaitParked(entries: 2))
        await endpoint.close()
        #expect(backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A wake landing while the engine is busy is not lost (level-retained)")
    func levelRetainedWake() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        let gate = TestGate()
        let recorder = EventRecorder()

        /* Block the loop inside job A, then submit B: B's wake fires while
         * the engine is NOT in the C wait. If the level were not retained,
         * B would strand until some unrelated activity. */
        endpoint.engine.post { gate.pass(); recorder.record("a") }
        #expect(await gate.awaitBlocked(count: 1))
        endpoint.engine.post { recorder.record("b") }
        gate.openGate()

        _ = try await endpoint.engine.perform { }   /* queue barrier */
        #expect(recorder.events == ["a", "b"])
        await endpoint.close()
        #expect(backend.snapshot().violations.isEmpty)
    }
}

@Suite("Endpoint lifecycle over the engine")
struct EndpointLifecycleEngineTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("established() resolves when already established")
    func establishedFastPath() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        backend.script(state: .established)
        try await endpoint.established()
        #expect(endpoint.state == .established)
        await endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("established() parks, then resumes on the scripted transition")
    func establishedParksAndResumes() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)

        let task = Task { try await endpoint.established() }
        /* The waiter registration nudges a pass; the engine then re-parks
         * with the waiter pending. */
        #expect(await backend.awaitParked(entries: 1))
        backend.script(state: .established)
        try await task.value
        await endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Terminal classification: fatal code vs clean close")
    func terminalClassification() async throws {
        let fatalBackend = ScriptedEndpointBackend()
        let fatalEndpoint = makeEndpoint(fatalBackend)
        fatalBackend.script(state: .closed, fatal: true, code: 7)
        await #expect(throws: MoQServiceError.fatal(code: 7)) {
            try await fatalEndpoint.established()
        }
        await fatalEndpoint.close()

        let cleanBackend = ScriptedEndpointBackend()
        let cleanEndpoint = makeEndpoint(cleanBackend)
        cleanBackend.script(state: .closed)
        await #expect(throws: MoQServiceError.closed) {
            try await cleanEndpoint.established()
        }
        await cleanEndpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A close racing the waiter sweep still resolves parked waiters")
    func terminalRaceFlushesWaiters() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        let evaluations = TestCounter()

        /* The predicate ITSELF closes the backend mid-sweep and stays
         * pending: the terminal transition lands after this pass's sweep
         * began, the exact window where a real backend can close from the
         * network thread. The engine must re-run the sweep once before the
         * internal terminal park -- with no external nudge, this waiter
         * resolves on the second evaluation or strands forever. */
        let task = Task {
            try await endpoint.engine.waitUntil {
                if evaluations.next() == 1 {
                    backend.script(state: .closed, markActivity: false)
                    return .pending
                }
                return .failure(.closed)
            }
        }
        await #expect(throws: MoQServiceError.closed) { try await task.value }
        #expect(evaluations.count >= 2)
        await endpoint.close()
        #expect(backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Once terminal, the engine parks instead of spinning the C wait")
    func terminalParksWithoutSpinning() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        #expect(await backend.awaitParked(entries: 1))

        backend.script(state: .closed)
        /* The pass after the .closed wait return evaluates waiters and then
         * must park internally. A probe job proves a full pass happened
         * while terminal -- and the wait-entry count must not have grown. */
        let recorder = EventRecorder()
        _ = try await endpoint.engine.perform { recorder.record("probe") }
        let entriesAfterProbe = backend.snapshot().waitEntries
        _ = try await endpoint.engine.perform { recorder.record("probe2") }
        #expect(recorder.events == ["probe", "probe2"])
        #expect(backend.snapshot().waitEntries == entriesAfterProbe)
        await endpoint.close()
        #expect(backend.snapshot().violations.isEmpty)
    }
}

@Suite("Interrupt latch")
struct EngineLatchTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("interrupt() fails parked waiters; resume() restores blocking")
    func latchFailsParkedWaiters() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)

        let task = Task { try await endpoint.established() }
        #expect(await backend.awaitParked(entries: 1))
        endpoint.interrupt()
        await #expect(throws: MoQServiceError.interrupted) {
            try await task.value
        }

        /* While latched the engine parks internally: jobs still run, but
         * the C wait is never re-entered (no hot loop). */
        let frozenEntries = backend.snapshot().waitEntries
        let recorder = EventRecorder()
        _ = try await endpoint.engine.perform { recorder.record("latched-job") }
        #expect(recorder.events == ["latched-job"])
        #expect(backend.snapshot().waitEntries == frozenEntries)

        /* A waiter registered while latched fails immediately too (the C
         * "no re-block race" rule). */
        await #expect(throws: MoQServiceError.interrupted) {
            try await endpoint.established()
        }

        /* resume() re-enters the C wait and blocking works again. */
        endpoint.resume()
        #expect(await backend.awaitParked(entries: frozenEntries + 1))
        backend.script(state: .established)
        try await endpoint.established()
        await endpoint.close()
        #expect(backend.snapshot().violations.isEmpty)
    }
}

@Suite("Cancellation")
struct EngineCancellationTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Cancelling a parked waiter throws CancellationError, touches no backend")
    func cancelParkedWaiter() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)

        let task = Task { try await endpoint.established() }
        #expect(await backend.awaitParked(entries: 1))
        let wakesBefore = backend.snapshot().wakeCount
        task.cancel()
        await #expect(throws: CancellationError.self) { try await task.value }
        /* The cancel path made no backend calls (no wake, no wait). */
        #expect(backend.snapshot().wakeCount == wakesBefore)

        /* Engine unaffected: still serving. */
        let ok = try await endpoint.engine.perform { true }
        #expect(ok)
        await endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Cancelling a queued job before it starts: it never runs")
    func cancelQueuedJob() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        let gate = TestGate()
        let recorder = EventRecorder()

        endpoint.engine.post { gate.pass() }
        #expect(await gate.awaitBlocked(count: 1))

        /* Queue B behind the blocked loop; its submission wake tells us the
         * continuation body ran (B is committed to the queue). */
        let wakesBefore = backend.snapshot().wakeCount
        let task = Task {
            try await endpoint.engine.perform { recorder.record("b") }
        }
        #expect(await backend.awaitCondition { $0.wakeCount > wakesBefore })
        task.cancel()
        await #expect(throws: CancellationError.self) { try await task.value }

        gate.openGate()
        _ = try await endpoint.engine.perform { }   /* queue barrier */
        #expect(recorder.events.isEmpty)            /* b never ran */
        await endpoint.close()
    }
}

@Suite("Shutdown")
struct EngineShutdownTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("close() fails parked waiters, stops and destroys exactly once, idempotent")
    func closeTeardown() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)

        let task = Task { try await endpoint.established() }
        #expect(await backend.awaitParked(entries: 1))
        await endpoint.close()
        await #expect(throws: MoQServiceError.closed) { try await task.value }
        var snap = backend.snapshot()
        #expect(snap.stopCount == 1)
        #expect(snap.destroyCount == 1)
        #expect(snap.violations.isEmpty)      /* stop/destroy on service thread */

        await endpoint.close()                /* idempotent */
        snap = backend.snapshot()
        #expect(snap.stopCount == 1)
        #expect(snap.destroyCount == 1)
        #expect(snap.violations.isEmpty)      /* second close: no backend calls */
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("After close, the endpoint never touches the backend")
    func postCloseBackendIsolation() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        backend.script(state: .established, version: .draft18)
        try await endpoint.established()
        #expect(endpoint.negotiatedVersion == .draft18)
        await endpoint.close()

        /* Engine-side synthesis: the scripted state is still .established,
         * so a .closed answer proves the backend was NOT consulted. */
        #expect(endpoint.state == .closed)
        /* The negotiated version survives as the retirement snapshot. */
        #expect(endpoint.negotiatedVersion == .draft18)
        /* The latch surface is a no-op toward the destroyed backend. */
        endpoint.interrupt()
        endpoint.resume()
        await endpoint.close()

        let snap = backend.snapshot()
        #expect(snap.destroyCount == 1)
        #expect(snap.violations.isEmpty)      /* the destroy tripwire is live */
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Concurrent close() calls both complete, one teardown")
    func concurrentClose() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        async let first: Void = endpoint.close()
        async let second: Void = endpoint.close()
        _ = await (first, second)
        let snap = backend.snapshot()
        #expect(snap.stopCount == 1)
        #expect(snap.destroyCount == 1)
        #expect(snap.violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Jobs accepted before stop still run; submissions after stopping fail closed")
    func postStopSubmissionContract() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        let gate = TestGate()
        let recorder = EventRecorder()

        /* Hold the loop in job A; queue B (accepted while running). */
        endpoint.engine.post { gate.pass(); recorder.record("a") }
        #expect(await gate.awaitBlocked(count: 1))
        endpoint.engine.post { recorder.record("b") }

        /* Begin close: the phase flips to stopping before close()'s wake. */
        let wakesBefore = backend.snapshot().wakeCount
        let closer = Task { await endpoint.close() }
        #expect(await backend.awaitCondition { $0.wakeCount > wakesBefore })

        /* Submissions DURING stopping are rejected deterministically. */
        #expect(endpoint.engine.post { recorder.record("never") } == false)
        await #expect(throws: MoQServiceError.closed) {
            try await endpoint.engine.perform { recorder.record("never2") }
        }
        await #expect(throws: MoQServiceError.closed) {
            try await endpoint.engine.waitUntil { .satisfied }
        }
        await #expect(throws: MoQServiceError.closed) {
            try await endpoint.engine.drain(timeoutMicroseconds: 1_000)
        }

        gate.openGate()
        await closer.value
        /* Accepted-before-stop jobs ran, in order; rejected ones never did. */
        #expect(recorder.events == ["a", "b"])
        #expect(backend.snapshot().destroyCount == 1)

        /* And after full stop, the same rejections hold -- with no backend
         * calls (the destroy tripwire stays quiet). */
        #expect(endpoint.engine.post { } == false)
        await #expect(throws: MoQServiceError.closed) {
            try await endpoint.engine.perform { }
        }
        #expect(backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Dropping an un-closed endpoint triggers the shutdown backstop")
    func deinitBackstop() {
        let backend = ScriptedEndpointBackend()
        var endpoint: MoQEndpoint? = makeEndpoint(backend)
        _ = endpoint
        endpoint = nil
        #expect(backend.awaitCondition { $0.destroyCount == 1 })
        let snap = backend.snapshot()
        #expect(snap.stopCount == 1)
        #expect(snap.violations.isEmpty)
    }
}

@Suite("Drain")
struct EngineDrainTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Completed drain returns; runs on the service thread in slices")
    func drainComplete() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        backend.scriptDrain([.complete])
        try await endpoint.drain(timeout: .milliseconds(100))
        #expect(backend.snapshot().drainCalls == [50_000])
        await endpoint.close()
        #expect(backend.snapshot().violations.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Timeout is sliced exactly; expiry throws wouldBlock")
    func drainSlicedTimeout() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        backend.scriptDrain([.timeout, .timeout])
        await #expect(throws: MoQServiceError.wouldBlock) {
            try await endpoint.drain(timeout: .milliseconds(100))
        }
        #expect(backend.snapshot().drainCalls == [50_000, 50_000])

        /* A non-multiple budget: the last slice is the remainder. */
        backend.scriptDrain([.timeout, .timeout])
        await #expect(throws: MoQServiceError.wouldBlock) {
            try await endpoint.drain(timeout: .milliseconds(70))
        }
        #expect(Array(backend.snapshot().drainCalls.suffix(2)) == [50_000, 20_000])
        await endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("C failure states map to the matching errors")
    func drainErrorMapping() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)

        backend.scriptDrain([.unsupported])
        await #expect(throws: MoQServiceError.unsupported) {
            try await endpoint.drain(timeout: .milliseconds(100))
        }
        backend.scriptDrain([.interrupted])
        await #expect(throws: MoQServiceError.interrupted) {
            try await endpoint.drain(timeout: .milliseconds(100))
        }
        backend.scriptDrain([.closed])
        backend.script(state: .closed, fatal: true, code: 9, markActivity: false)
        await #expect(throws: MoQServiceError.fatal(code: 9)) {
            try await endpoint.drain(timeout: .milliseconds(100))
        }
        await endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Zero/negative timeout is rejected before any C call")
    func drainRejectsNonPositiveTimeout() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        await #expect(throws: MoQServiceError.self) {
            try await endpoint.drain(timeout: .zero)
        }
        await #expect(throws: MoQServiceError.self) {
            try await endpoint.drain(timeout: .milliseconds(-5))
        }
        #expect(backend.snapshot().drainCalls.isEmpty)
        await endpoint.close()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Cancellation is honored between slices; the in-flight slice finishes")
    func drainCancellationBetweenSlices() async throws {
        let backend = ScriptedEndpointBackend()
        let endpoint = makeEndpoint(backend)
        backend.scriptDrain([.timeout, .timeout, .timeout])
        backend.setDrainGate(true)

        let task = Task { try await endpoint.drain(timeout: .seconds(10)) }
        #expect(await backend.awaitDrainBlocked())
        task.cancel()
        backend.setDrainGate(false)   /* slice 1 returns .timeout */
        await #expect(throws: CancellationError.self) { try await task.value }
        /* Exactly one slice ran: the between-slice check saw the cancel. */
        #expect(backend.snapshot().drainCalls.count == 1)
        await endpoint.close()
        #expect(backend.snapshot().violations.isEmpty)
    }
}
