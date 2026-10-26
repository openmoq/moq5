package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * The orderings the sequential rows cannot reach, forced by latches at the
 * two boundaries the product declares -- before admission and before commit --
 * both of which are OUTSIDE the monitor. Close never waits for a callback and a
 * callback never waits for close, so neither legal ordering can deadlock:
 *
 *  * paused BEFORE ADMISSION, close wins: the callback is ignored, nothing is
 *    fetched, nothing is released;
 *  * paused BEFORE COMMIT, close wins: the transfer was already protected work,
 *    so it completes -- copied, released once -- and the output is DISPOSED
 *    and witnessed, never made visible.
 *
 * Every row runs twice: against the Phase A declaration (RED at its named
 * `TODO`) and against the conforming host fixture, which must PASS -- that is
 * what shows the corrected schedules are satisfiable. Joins carry a fail-only
 * watchdog: expiry fails the row and is never ordering evidence.
 */
class CaptureConcurrencyRedTest {

    private class Rig(val kind: MediaKind = MediaKind.Video) {
        val buffers = HostBuffers()
        val releases = HostReleases()
        val faults = HostFaults()
        val ledger = HostLedger(kind, GENERATION, capacity = 64)
    }

    private fun real(rig: Rig, schedule: HandoffSchedule): CaptureHandoff =
        BoundedCaptureHandoff(rig.kind, GENERATION, rig.buffers, rig.releases, rig.ledger, rig.faults, CAPACITY, HOST_MAX_OUTPUT_BYTES, schedule = schedule)

    private fun conforming(rig: Rig, schedule: HandoffSchedule): CaptureHandoff =
        ConformingHandoff(rig.kind, GENERATION, rig.buffers, rig.releases, rig.ledger, rig.faults, CAPACITY, schedule = schedule)

    private fun output(rig: Rig, index: Int, pts: Long, fill: Byte = 1): CodecOutput {
        rig.buffers.put(index, ByteBuffer.wrap(ByteArray(8) { fill }).asReadOnlyBuffer())
        rig.ledger.observe(GENERATION, rig.kind, CaptureObservation(pts, 1_000L + pts))
        return CodecOutput(index, 0, 8, pts, CaptureContract.FLAG_KEY_FRAME)
    }

    /** Holds a callback at ONE named boundary until [release] is counted down; signals [reached]. */
    private class HoldAt(private val which: String) : HandoffSchedule {
        val reached = HostGate("the callback reached the $which boundary")
        val release = HostGate("the row released the $which boundary")

        override fun beforeAdmission() = hold("admission")

        override fun beforeRelease() = hold("release")

        override fun beforeCommit() = hold("commit")

        override fun beforeTerminal() = hold("terminal")

        private fun hold(at: String) {
            if (at != which) return
            reached.open()
            release.await()
        }
    }

    // ---- x1a: paused BEFORE ADMISSION, close wins -----------------------------

    private fun x1a(make: (Rig, HandoffSchedule) -> CaptureHandoff) {
        val rig = Rig()
        val hold = HoldAt("admission")
        val handoff = make(rig, hold)
        val out = output(rig, index = 0, pts = 0L)
        val outcome = AtomicReference<HandoffOutcome?>(null)
        val producer =
            HostWorker("codec-callback") {
                outcome.set(handoff.onOutput(GENERATION, out))
            }.start()
        val closer =
            HostWorker("stop") {
                hold.reached.await()
                try {
                    handoff.close(CloseMode.Failed)
                } finally {
                    hold.release.open()
                }
            }.start()
        settleAll(listOf(hold.reached, hold.release), listOf(producer, closer))
        assertIs<HandoffOutcome.Ignored>(outcome.get(), "close won before admission, so the callback is ignored")
        assertEquals(emptyList(), rig.buffers.fetched(), "nothing was fetched from a codec this session gave up")
        assertEquals(emptyList(), rig.releases.indices(), "and nothing was released")
        assertEquals(emptyList(), rig.faults.all(), "nothing was admitted, so there is nothing to witness")
        assertNull(handoff.next())
    }

    // ---- x1b: paused BEFORE COMMIT, close wins --------------------------------

    private fun x1b(make: (Rig, HandoffSchedule) -> CaptureHandoff) {
        val rig = Rig()
        val hold = HoldAt("commit")
        val handoff = make(rig, hold)
        val out = output(rig, index = 3, pts = 0L)
        val outcome = AtomicReference<HandoffOutcome?>(null)
        val producer =
            HostWorker("codec-callback") {
                outcome.set(handoff.onOutput(GENERATION, out))
            }.start()
        val closer =
            HostWorker("stop") {
                hold.reached.await()
                try {
                    handoff.close(CloseMode.Failed)
                } finally {
                    hold.release.open()
                }
            }.start()
        settleAll(listOf(hold.reached, hold.release), listOf(producer, closer))
        assertEquals(HandoffOutcome.Queued, outcome.get(), "the output was admitted; the callback's answer does not change")
        assertEquals(listOf(3), rig.releases.indices(), "the index went back exactly once -- the transfer was protected work")
        assertNull(handoff.next(), "but close won before commit, so it never became visible")
        assertEquals(
            listOf(HostFaults.Discarded(MediaKind.Video, 1)),
            rig.faults.all(),
            "and its loss is WITNESSED exactly once, after the in-flight transfer settled",
        )
    }

    // ---- x2a: adoption paused BEFORE SETTLEMENT, close wins -------------------

    private class HoldSettle : AdoptionSchedule {
        val reached = HostGate("the adoption reached its settlement boundary")
        val release = HostGate("the row released the settlement boundary")

        override fun beforeSettle() {
            reached.open()
            release.await()
        }
    }

    private fun x2a(make: (CallbackDispatcher, LateLog, AdoptionSchedule) -> CaptureOwnership) {
        val closes = mutableListOf<Any>()
        val late = LateLog()
        val hold = HoldSettle()
        val ownership = make(HostDispatcher(), late, hold)
        val worker = HostResource("worker")
        val camera = HostResource("camera")
        ownership.acquire(CaptureResource.Worker) { HostHandle(worker, closes) }
        val ticket = ownership.request(CaptureResource.Camera)
        val outcome = AtomicReference<AdoptOutcome?>(null)
        val adopter =
            HostWorker("camera-onOpened") {
                outcome.set(ownership.adopt(ticket, HostHandle(camera, closes)))
            }.start()
        val closer =
            HostWorker("stop") {
                hold.reached.await()
                try {
                    ownership.close(null)
                } finally {
                    hold.release.open()
                }
            }.start()
        settleAll(listOf(hold.reached, hold.release), listOf(adopter, closer))
        assertIs<AdoptOutcome.Retired>(outcome.get(), "close won before settlement, so the device is never installed")
        assertEquals(
            listOf<Any>(camera, worker),
            synchronized(closes) { closes.toList() },
            "the late device is closed once, and the worker -- retained for it -- closes after it",
        )
        assertEquals(0, ownership.pending)
        assertEquals(listOf("late-close:Camera", "late-close:Worker"), late.all(), "both are observable after close returned")
    }

    // ---- x2b: adoption already settled, then close ----------------------------

    private fun x2b(make: (CallbackDispatcher, LateLog, AdoptionSchedule) -> CaptureOwnership) {
        val closes = mutableListOf<Any>()
        val late = LateLog()
        val ownership = make(HostDispatcher(), late, AdoptionSchedule.NONE)
        val worker = HostResource("worker")
        val camera = HostResource("camera")
        ownership.acquire(CaptureResource.Worker) { HostHandle(worker, closes) }
        val ticket = ownership.request(CaptureResource.Camera)
        assertEquals(AdoptOutcome.Adopted, ownership.adopt(ticket, HostHandle(camera, closes)))
        ownership.close(null)
        assertEquals(listOf<Any>(camera, worker), closes, "an installed device closes in order, once, with the rest")
        assertEquals(emptyList(), late.all(), "and nothing is late")
    }

    // ---- x3: controlled inventories against close -----------------------------

    private fun x3(make: (Rig, HandoffSchedule) -> CaptureHandoff) {
        for (drainFirst in listOf(0, 2)) {
            val rig = Rig()
            val handoff = make(rig, HandoffSchedule.NONE)
            val fills = listOf<Byte>(11, 22, 33, 44)
            fills.forEachIndexed { i, fill ->
                assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, output(rig, i, i * 33_333L, fill)))
            }
            val drained = (0 until drainFirst).map { assertNotNull(handoff.next()) }
            handoff.close(CloseMode.Failed)
            drained.forEachIndexed { i, got ->
                assertContentEquals(ByteArray(8) { fills[i] }, got.output.buffer, "drained output $i is exactly output $i")
                assertEquals(i * 33_333L, got.output.presentationTimeUs)
                assertEquals(1_000L + i * 33_333L, got.captureElapsedNanos)
            }
            assertNull(handoff.next(), "a failed close leaves nothing drainable")
            assertEquals(
                listOf(HostFaults.Discarded(MediaKind.Video, fills.size - drainFirst)),
                rig.faults.all(),
                "after draining $drainFirst, exactly the remainder is witnessed, once",
            )
        }
    }

    // ---- x4: two terminal callbacks forced into the same window ---------------

    private fun x4(make: (Rig, HandoffSchedule) -> CaptureHandoff) {
        val rig = Rig()
        val arrived = HostGate("both terminal callbacks reached the boundary", count = 2)
        val go = HostGate("the row released both terminal callbacks")
        val schedule =
            object : HandoffSchedule {
                override fun beforeTerminal() {
                    arrived.open()
                    go.await()
                }
            }
        val handoff = make(rig, schedule)
        val callbacks =
            (0 until 2).map { n ->
                HostWorker("codec-onError-$n") { handoff.onCodecError(GENERATION, "CodecException $n") }.start()
            }
        withSettlement(listOf(arrived, go), callbacks) {
            arrived.await(*callbacks.toTypedArray())
            go.openFully()
            callbacks.forEach { it.join() }
            assertEquals(1, rig.faults.faults().size, "both arrived before either decided; the owner is told once")
            assertIs<CaptureFault.Codec>(rig.faults.faults().single())
            assertTrue(handoff.ended)
        }
    }

    // ---- x5: two tracks faulting into one session at once ---------------------

    private class NoAcquire : CaptureAcquirer {
        override val order: List<CaptureResource> = emptyList()

        override fun begin(ownership: CaptureOwnership, steps: AcquisitionSteps): AcquireOutcome.Failed? = null
    }

    private class Granted(override val generation: Long) : PermissionFlow {
        override fun stateOf(permission: CapturePermission) = PermissionState.Granted

        override fun request(permissions: Set<CapturePermission>) = emptySet<CapturePermission>()

        override fun onResult(generation: Long, permission: CapturePermission, facts: PermissionFacts) =
            PermissionOutcome.Ignored("granted")

        override fun retire(reason: String) = Unit

        override fun readiness(): CaptureReadiness = CaptureReadiness.Ready
    }

    /** A sink that forwards into whichever session is wired later. */
    private class Forward : CaptureFaultSink {
        @Volatile
        var to: CaptureFaultSink? = null

        override fun onFault(generation: Long, fault: CaptureFault) {
            to?.onFault(generation, fault)
        }

        override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) {
            to?.onDiscarded(generation, kind, outputs)
        }
    }

    private fun x5(
        makeHandoff: (MediaKind, CaptureFaultSink, HandoffSchedule) -> CaptureHandoff,
        makeSession: (CaptureOwnership, CaptureHandoff, CaptureHandoff, CaptureSessionListener) -> CaptureSession,
    ) {
        val arrived = HostGate("both tracks' terminal callbacks reached the boundary", count = 2)
        val go = HostGate("the row released both terminal callbacks")
        val both =
            object : HandoffSchedule {
                override fun beforeTerminal() {
                    arrived.open()
                    go.await()
                }
            }
        val forward = Forward()
        val video = makeHandoff(MediaKind.Video, forward, both)
        val audio = makeHandoff(MediaKind.Audio, forward, both)
        val listener = HostSessionListener()
        val ownership = ConformingOwnership(GENERATION, HostDispatcher(), LateLog())
        val session = makeSession(ownership, video, audio, listener)
        forward.to = session
        assertEquals(CaptureStart.Started, session.start(Granted(GENERATION), CaptureCapability.Available, CaptureCapability.Available))
        val faults =
            listOf(video to "video codec died", audio to "audio codec died").map { (track, why) ->
                HostWorker("onError-${track.kind}") { track.onCodecError(GENERATION, why) }.start()
            }
        withSettlement(listOf(arrived, go), faults) {
            arrived.await(*faults.toTypedArray())
            go.openFully()
            faults.forEach { it.join() }
            val termination = assertIs<CaptureTermination.Failed>(session.termination, "one of the two faults ended the session")
            assertIs<CaptureFault.Codec>(termination.fault)
            assertEquals(
                1,
                listener.all().count { it is CaptureTermination },
                "the session terminated ONCE, closing both tracks, with no lock cycle between them",
            )
            assertTrue(video.ended && audio.ended)
            assertTrue(ownership.isClosed)
        }
    }

    // ---- RED: the declarations this sample will ship --------------------------

    @Test
    fun x1a_closeBeforeAdmission_real() = x1a(::real)

    @Test
    fun x1b_closeBeforeCommit_real() = x1b(::real)

    @Test
    fun x2a_closeBeforeSettlement_real() = x2a { d, l, s -> OrderedCaptureOwnership(GENERATION, d, l, s) }

    @Test
    fun x2b_closeAfterSettlement_real() = x2b { d, l, s -> OrderedCaptureOwnership(GENERATION, d, l, s) }

    @Test
    fun x3_controlledInventories_real() = x3(::real)

    @Test
    fun x4_concurrentTerminals_real() = x4(::real)

    @Test
    fun x5_twoTracksFaultingIntoOneSession_real() =
        x5(
            { kind, sink, schedule ->
                val rig = Rig(kind)
                BoundedCaptureHandoff(kind, GENERATION, rig.buffers, rig.releases, rig.ledger, sink, CAPACITY, HOST_MAX_OUTPUT_BYTES, schedule = schedule)
            },
            { ownership, v, a, listener -> PlatformCaptureSession(GENERATION, ownership, NoAcquire(), v, a, listener) },
        )

    // ---- satisfiability: the same schedules against the conforming fixtures ---

    @Test
    fun s1a_closeBeforeAdmission_isSatisfiable() = x1a(::conforming)

    @Test
    fun s1b_closeBeforeCommit_isSatisfiable() = x1b(::conforming)

    @Test
    fun s2a_closeBeforeSettlement_isSatisfiable() = x2a { d, l, s -> ConformingOwnership(GENERATION, d, l, s) }

    @Test
    fun s2b_closeAfterSettlement_isSatisfiable() = x2b { d, l, s -> ConformingOwnership(GENERATION, d, l, s) }

    @Test
    fun s3_controlledInventories_areSatisfiable() = x3(::conforming)

    @Test
    fun s4_concurrentTerminals_areSatisfiable() = x4(::conforming)

    @Test
    fun s5_twoTracksFaultingIntoOneSession_isSatisfiable() =
        x5(
            { kind, sink, schedule ->
                val rig = Rig(kind)
                ConformingHandoff(kind, GENERATION, rig.buffers, rig.releases, rig.ledger, sink, CAPACITY, schedule = schedule)
            },
            { ownership, v, a, listener -> ConformingSession(GENERATION, ownership, NoAcquire(), v, a, listener) },
        )

    // ---- discrimination: a handoff that decides its terminal outside exclusion --

    /**
     * Conforming in every other respect, but its terminal is a check-then-set
     * with no exclusion, and the schedule point sits between the check and the
     * set. Both callbacks in x4 therefore read "not yet" and both report.
     */
    private class UnguardedTerminalHandoff(
        private val inner: ConformingHandoff,
        private val faults: CaptureFaultSink,
        private val schedule: HandoffSchedule,
        override val kind: MediaKind,
        override val generation: Long,
    ) : CaptureHandoff by inner {
        @Volatile
        private var reported = false

        override fun onCodecError(generation: Long, reason: String) {
            if (reported) return
            schedule.beforeTerminal()
            reported = true
            faults.onFault(generation, CaptureFault.Codec(kind, reason))
        }
    }

    @Test
    fun m1_anUnguardedTerminalIsCaughtByX4() {
        val thrown =
            try {
                x4 { rig, schedule ->
                    UnguardedTerminalHandoff(
                        ConformingHandoff(rig.kind, GENERATION, rig.buffers, rig.releases, rig.ledger, rig.faults, CAPACITY),
                        rig.faults,
                        schedule,
                        rig.kind,
                        GENERATION,
                    )
                }
                null
            } catch (assertion: AssertionError) {
                assertion
            }
        assertTrue(thrown != null, "x4 must reject a terminal decided outside exclusion")
    }

    // ---- how these rows settle, when they go wrong -------------------------

    /**
     * A controlling wait ends on the CAUSE, not on the deadline: a worker that
     * died before reaching the boundary is named, and it is still settled
     * before the failure leaves the row.
     */
    @Test
    fun w1_aWorkerThatFailsBeforeArrivingEndsTheControllingWaitWithItsCause() {
        val arrived = HostGate("the worker reached the boundary")
        val doomed = HostWorker("doomed") { throw IllegalStateException("it died on the way") }.start()
        val thrown =
            assertFailsWith<AssertionError> {
                withSettlement(listOf(arrived), listOf(doomed)) { arrived.await(doomed, millis = 2_000L) }
            }
        assertTrue(thrown.message.orEmpty().contains("failed first"), "named by its cause: ${thrown.message}")
        assertTrue(doomed.isSettled, "and it was settled before the failure propagated")
    }

    /** One partner failing does not leave the other parked: every worker is settled. */
    @Test
    fun w2_oneFailingPartnerStillSettlesTheOther() {
        val go = HostGate("the row released the survivor")
        val doomed = HostWorker("doomed") { throw IllegalStateException("partner failed") }.start()
        val survivor = HostWorker("survivor") { go.await() }.start()
        assertFailsWith<AssertionError> {
            withSettlement(listOf(go), listOf(doomed, survivor)) {
                HostGate("something that never happens").await(doomed, millis = 2_000L)
            }
        }
        assertTrue(doomed.isSettled, "the failing worker settled")
        assertTrue(survivor.isSettled, "and so did the one parked on a barrier the row owns")
    }

    /** An interrupted controlling wait is a failure, and still settles everything. */
    @Test
    fun w3_anInterruptedControllingWaitFailsAndStillSettlesEveryWorker() {
        val go = HostGate("the row released the worker")
        val parked = HostWorker("parked") { go.await() }.start()
        assertFailsWith<InterruptedException> {
            withSettlement(listOf(go), listOf(parked)) {
                Thread.currentThread().interrupt()
                go.await(parked, millis = 2_000L)
            }
        }
        assertTrue(parked.isSettled, "the parked worker was released and settled anyway")
        assertTrue(!Thread.currentThread().isInterrupted, "and the flag was cleared so the settlement could run")
    }

    /** An interrupt is a request. A thread that ignores it is reported as running. */
    @Test
    fun w4_aWorkerThatWillNotTerminateIsReportedAsStillRunningNotAsSettled() {
        val stubborn =
            HostWorker("stubborn", watchdogMs = 200L) {
                val until = System.nanoTime() + 3_000_000_000L
                while (System.nanoTime() < until) Thread.onSpinWait()
            }.start()
        val thrown = assertFailsWith<AssertionError> { stubborn.join() }
        assertTrue(thrown.message.orEmpty().contains("STILL RUNNING"), "an interrupt request is not proof of termination: ${thrown.message}")
    }

    /**
     * Settling a worker is CLEANUP, not evidence. A row whose worker had to be
     * released by the cleanup did not meet its own schedule, and a retry that
     * then succeeds cannot turn that into a pass.
     */
    @Test
    fun w5_aWorkerTheCleanupHadToReleaseIsStillAFailure() {
        val gate = HostGate("the row released the worker")
        val stubborn =
            HostWorker("stubborn", watchdogMs = 200L) {
                // Ignores the watchdog's interrupt entirely; only the gate ends it.
                while (!gate.isOpen) {
                    try {
                        gate.await(millis = 50L)
                    } catch (expiry: AssertionError) {
                        Unit
                    } catch (interrupted: InterruptedException) {
                        Unit
                    }
                }
            }.start()
        val thrown = assertFailsWith<AssertionError> { withSettlement(listOf(gate), listOf(stubborn)) { } }
        assertTrue(thrown.message.orEmpty().contains("watchdog"), "the original timeout is what propagates: ${thrown.message}")
        assertTrue(stubborn.isSettled, "and the worker was still settled, by opening the barrier the row owns")
    }

    /** A failing body keeps its cause; every worker is still attempted, and their failures kept. */
    @Test
    fun w6_aFailingBodyKeepsItsCauseAndStillSettlesEveryWorker() {
        val gate = HostGate("the row released the parked worker")
        val doomed = HostWorker("doomed") { throw IllegalStateException("cleanup A") }.start()
        val parked = HostWorker("parked") { gate.await() }.start()
        val stubborn =
            HostWorker("stubborn", watchdogMs = 200L) {
                while (!gate.isOpen) {
                    try {
                        gate.await(millis = 50L)
                    } catch (expiry: AssertionError) {
                        Unit
                    } catch (interrupted: InterruptedException) {
                        Unit
                    }
                }
            }.start()
        val thrown =
            assertFailsWith<IllegalStateException> {
                withSettlement(listOf(gate), listOf(doomed, parked, stubborn)) { throw IllegalStateException("the body failed") }
            }
        assertEquals("the body failed", thrown.message, "the body's failure is the primary one")
        val suppressed = thrown.suppressed.map { it.message.orEmpty() }
        assertTrue(suppressed.any { it.contains("cleanup A") }, "a worker failure is preserved: $suppressed")
        assertTrue(doomed.isSettled && parked.isSettled && stubborn.isSettled, "every worker was attempted and settled")
        assertTrue(thrown.suppressed.none { it === thrown }, "and nothing suppressed itself")
    }

    /** A sentinel that is an Error and is deliberately never caught as a failure. */
    private class SentinelError : Error("a sentinel Error the settlement must not convert")

    /**
     * An Error is not converted into an ordinary failure -- and that is not a
     * reason to abandon what the row owns. The exact Error escapes, and every
     * owned worker is still settled.
     */
    @Test
    fun w7_anUncaughtErrorStillSettlesEveryOwnedWorker() {
        val gate = HostGate("the row released the parked worker")
        val parked = HostWorker("parked") { gate.await() }.start()
        val sentinel = SentinelError()
        try {
            val thrown = assertFailsWith<SentinelError> { withSettlement(listOf(gate), listOf(parked)) { throw sentinel } }
            assertSame(sentinel, thrown, "the exact Error escapes, unconverted")
            assertTrue(parked.isSettled, "and the worker parked on the row's own barrier was settled anyway")
        } finally {
            // If the candidate did NOT settle it, this harness still must.
            gate.openFully()
            if (!parked.isSettled) parked.join()
        }
    }

    /** An Error from an early join does not skip the workers behind it. */
    @Test
    fun w8_anErrorFromAnEarlyJoinStillSettlesTheRest() {
        val gate = HostGate("the row released the parked worker")
        val sentinel = SentinelError()
        val erroring = HostWorker("erroring") { throw sentinel }.start()
        val parked = HostWorker("parked") { gate.await() }.start()
        try {
            val thrown = assertFailsWith<SentinelError> { withSettlement(listOf(gate), listOf(erroring, parked)) { } }
            assertSame(sentinel, thrown, "the same instance propagates: nothing converted it")
            assertTrue(erroring.isSettled && parked.isSettled, "the worker behind the failing one was settled too")
        } finally {
            gate.openFully()
            if (!parked.isSettled) parked.join()
        }
    }

    /**
     * A body Error AND a cleanup that fails. The Error is what propagates, by
     * identity; what the cleanup found travels with it rather than vanishing.
     */
    @Test
    fun w9_aBodyErrorCarriesWhatTheCleanupFound() {
        val gate = HostGate("the row released the parked worker")
        val sentinel = SentinelError()
        val workerFailure = IllegalStateException("the cleanup worker failed")
        val failing = HostWorker("failing") { throw workerFailure }.start()
        val parked = HostWorker("parked") { gate.await() }.start()
        // Parked on something this row does NOT hand to the helper, so opening
        // the owned barrier cannot settle it and its watchdog really fires.
        val unowned = HostGate("the row released the worker it did not hand over")
        val stubborn =
            HostWorker("stubborn", watchdogMs = 200L) {
                while (!unowned.isOpen) {
                    try {
                        unowned.await(millis = 50L)
                    } catch (expiry: AssertionError) {
                        Unit
                    } catch (interrupted: InterruptedException) {
                        Unit
                    }
                }
            }.start()
        try {
            val thrown =
                assertFailsWith<SentinelError> {
                    withSettlement(listOf(gate), listOf(failing, parked, stubborn)) { throw sentinel }
                }
            assertSame(sentinel, thrown, "the exact Error propagates, unconverted")
            val suppressed = thrown.suppressed.toList()
            assertTrue(suppressed.any { it === workerFailure }, "the worker's own failure travels with it, by identity: $suppressed")
            assertTrue(
                suppressed.any { it is AssertionError && it.message.orEmpty().contains("watchdog") },
                "and so does the watchdog failure a successful cleanup retry must not erase: $suppressed",
            )
            assertTrue(suppressed.none { it === thrown }, "nothing suppressed itself")
            assertTrue(failing.isSettled && parked.isSettled, "every worker the helper could settle was settled")
        } finally {
            gate.openFully()
            unowned.openFully()
            if (!parked.isSettled) parked.join()
            if (!stubborn.isSettled) stubborn.join()
        }
    }

    private companion object {
        const val GENERATION = 1L
        const val CAPACITY = 8
    }
}
