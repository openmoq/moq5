package org.openmoq.moq5.samples.publisher

import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Starting a capture session, against the platform's own causality.
 *
 * `CameraDevice.StateCallback.onOpened` is where the camera "is ready to use,
 * and CameraDevice#createCaptureSession can be called to set up the first
 * capture session" -- so the capture session is SUBMITTED from inside the
 * camera's own callback, not alongside it. A session that counts its
 * outstanding steps after the driver returns is counting a number that every
 * delivered callback has already raced, and the dependent step it has not seen
 * yet is not in it at all.
 *
 * Each window below is FORCED with fail-only bounded gates -- a settlement
 * inside `begin`, one in the window after `begin` returns and before the
 * session publishes its state, and one concurrent with `stop` -- and each row
 * asserts the exact ordered listener inventory, not a count and not a union of
 * outcomes that would accept either causality.
 *
 * No camera is opened here, and nothing below says a real `onOpened` arrives.
 */
class CaptureStartupRedTest {

    private class Devices {
        val closes = mutableListOf<Any>()
        private val identities = CaptureResource.entries.associateWith { HostResource(it.name.lowercase()) }

        fun identity(resource: CaptureResource): HostResource = identities.getValue(resource)

        fun handle(resource: CaptureResource) = HostHandle(identity(resource), closes)

        fun closed(): List<Any> = synchronized(closes) { closes.toList() }
    }

    private enum class Mode {
        /** Everything settles before `begin` returns, camera first, then what it enables. */
        InsideBegin,

        /** The camera settles in the window after `begin` returns, before state is published. */
        InTheReturnWindow,

        /** Nothing settles until the row says so. */
        ByHand,

        /** `createCaptureSession` throws where it stands, inside the camera's callback. */
        DependentSubmissionThrows,

        /** The driver reports a settlement for a step it never submitted. */
        UnsubmittedSettlement,

        /** The driver submits one step and reports the settlement of a DIFFERENT one. */
        WrongResourceSettlement,

        /** The camera's own callback fails, inside begin, before it returns. */
        FailedSettlementInsideBegin,
    }

    private class StartupAcquirer(
        private val devices: Devices,
        private val mode: Mode,
        private val failAt: CaptureResource? = null,
        private val window: HostGate? = null,
        private val windowDone: HostGate? = null,
    ) : CaptureAcquirer {
        override val order =
            listOf(CaptureResource.Worker, CaptureResource.VideoEncoder, CaptureResource.Camera, CaptureResource.Microphone)

        val tickets = mutableMapOf<CaptureResource, AcquisitionTicket>()
        private var steps: AcquisitionSteps? = null
        private var owner: CaptureOwnership? = null
        var beginCalls = 0
            private set
        var racer: HostWorker? = null
            private set

        override fun begin(ownership: CaptureOwnership, steps: AcquisitionSteps): AcquireOutcome.Failed? {
            beginCalls++
            this.steps = steps
            this.owner = ownership
            for (resource in order) {
                if (resource == failAt) return AcquireOutcome.Failed(resource, "$resource refused")
                if (resource == CaptureResource.Camera) {
                    if (mode == Mode.UnsubmittedSettlement) {
                        // Never submitted, and reported anyway.
                        steps.settled(CaptureResource.Camera, AdoptOutcome.Adopted)
                        continue
                    }
                    if (mode == Mode.WrongResourceSettlement) {
                        // The camera IS submitted and is still opening; what is
                        // reported settled is a different step entirely. The
                        // arithmetic is identical to a correct settlement.
                        submit(ownership, resource)
                        steps.settled(CaptureResource.CameraStream, AdoptOutcome.Adopted)
                        continue
                    }
                    if (mode == Mode.FailedSettlementInsideBegin) {
                        // onError instead of onOpened, before begin returns.
                        val ticket = submit(ownership, resource)
                        val failure = ownership.fail(ticket, "the camera reported onError", null)
                        steps.settled(resource, failure)
                        continue
                    }
                    submit(ownership, resource)
                    when (mode) {
                        Mode.InsideBegin -> settle(CaptureResource.Camera)
                        Mode.InTheReturnWindow ->
                            racer =
                                HostWorker("camera-callback") {
                                    // Runs only once the session is provably
                                    // inside the post-begin window.
                                    window?.await()
                                    settle(CaptureResource.Camera)
                                    windowDone?.open()
                                }.start()
                        else -> Unit
                    }
                } else {
                    val outcome = ownership.acquire(resource) { devices.handle(resource) }
                    if (outcome is AcquireOutcome.Failed) return outcome
                }
            }
            return null
        }

        /** onError instead of onOpened: the submitted step settles as a failure. */
        fun failed(resource: CaptureResource): AdoptOutcome {
            val ownership = owner ?: return AdoptOutcome.Unknown("begin has not run")
            val ticket = tickets[resource] ?: return AdoptOutcome.Unknown("nothing was requested for $resource")
            val outcome = ownership.fail(ticket, "$resource refused", null)
            steps?.settled(resource, outcome)
            return outcome
        }

        /** onOpened / onConfigured: the dependent step is submitted BEFORE this one is reported. */
        fun settle(resource: CaptureResource): AdoptOutcome {
            val ownership = owner ?: return AdoptOutcome.Unknown("begin has not run")
            val ticket = tickets[resource] ?: return AdoptOutcome.Unknown("nothing was requested for $resource")
            val outcome = ownership.adopt(ticket, devices.handle(resource))
            var failedSubmission: AdoptOutcome? = null
            if (resource == CaptureResource.Camera && outcome == AdoptOutcome.Adopted) {
                val next = submit(ownership, CaptureResource.CameraStream)
                if (mode == Mode.DependentSubmissionThrows) {
                    failedSubmission = ownership.fail(next, "createCaptureSession threw: the device is closed", null)
                }
            }
            steps?.settled(resource, outcome)
            failedSubmission?.let { steps?.settled(CaptureResource.CameraStream, it) }
            if (mode == Mode.InsideBegin && resource == CaptureResource.Camera && outcome == AdoptOutcome.Adopted) {
                settle(CaptureResource.CameraStream)
            }
            return outcome
        }

        private fun submit(ownership: CaptureOwnership, resource: CaptureResource): AcquisitionTicket =
            ownership.request(resource).also {
                tickets[resource] = it
                steps?.submitted(resource)
            }
    }

    /**
     * Records what the session says, in order, and -- from ANOTHER thread, with
     * a fail-only deadline -- reads the session back while it is saying it. A
     * session holding its own lock across the callback cannot answer that read.
     * [parkAt] holds the callback so a racing decision can be observed.
     */
    private class ProbingListener(
        private val parkAt: HostGate? = null,
        private val insideReady: ((CaptureSession) -> Unit)? = null,
        private val raiseOnReady: (() -> Throwable)? = null,
    ) : CaptureSessionListener {
        var session: CaptureSession? = null
        private val events = mutableListOf<Any>()
        private val probes = mutableListOf<String>()
        private val nested = mutableListOf<String>()
        private var readyDepth = 0
        val inReady = HostGate("the listener was inside onReady")

        override fun onReady(generation: Long, ready: CaptureReady) {
            synchronized(events) { events.add(ready) }
            synchronized(this) { readyDepth++ }
            try {
                probe("ready")
                if (ready == CaptureReady.Ready && parkAt != null) {
                    inReady.open()
                    parkAt.await()
                }
                // A listener is application code: it may call back in, and it
                // may fail.
                if (ready == CaptureReady.Ready) session?.let { current -> insideReady?.invoke(current) }
                if (ready == CaptureReady.Ready) raiseOnReady?.let { throw it() }
            } finally {
                synchronized(this) { readyDepth-- }
            }
        }

        override fun onTerminated(generation: Long, termination: CaptureTermination) {
            synchronized(events) { events.add(termination) }
            if (synchronized(this) { readyDepth } > 0) synchronized(nested) { nested.add("terminated-inside-ready") }
            probe("terminated")
        }

        fun nestedDeliveries(): List<String> = synchronized(nested) { nested.toList() }

        private fun probe(at: String) {
            val current = session ?: return
            val seen = AtomicReference("")
            val reader = HostWorker("probe-$at") { seen.set(if (current.video != null) "tracks" else "none") }.start()
            reader.join()
            synchronized(probes) { probes.add("$at:${seen.get()}") }
        }

        fun all(): List<Any> = synchronized(events) { events.toList() }

        fun probes(): List<String> = synchronized(probes) { probes.toList() }
    }

    private class Rig(
        mode: Mode,
        failAt: CaptureResource? = null,
        window: HostGate? = null,
        windowDone: HostGate? = null,
        adoption: AdoptionSchedule = AdoptionSchedule.NONE,
        parkInReady: HostGate? = null,
        insideReady: ((CaptureSession) -> Unit)? = null,
        raiseOnReady: (() -> Throwable)? = null,
    ) {
        val devices = Devices()
        val late = LateLog()
        val ownership = ConformingOwnership(GENERATION, HostDispatcher(), late, adoption)
        val acquirer = StartupAcquirer(devices, mode, failAt, window, windowDone)
        val faults = HostFaults()
        val video = ConformingHandoff(MediaKind.Video, GENERATION, HostBuffers(), HostReleases(), HostLedger(MediaKind.Video, GENERATION), faults, 4)
        val audio = ConformingHandoff(MediaKind.Audio, GENERATION, HostBuffers(), HostReleases(), HostLedger(MediaKind.Audio, GENERATION), faults, 4)
        val listener = ProbingListener(parkInReady, insideReady, raiseOnReady)

        fun wire(session: CaptureSession): CaptureSession = session.also { listener.session = it }

        fun start(session: CaptureSession): CaptureStart =
            session.start(GrantedStartup(GENERATION), CaptureCapability.Available, CaptureCapability.Available)
    }

    private class GrantedStartup(override val generation: Long) : PermissionFlow {
        override fun stateOf(permission: CapturePermission) = PermissionState.Granted

        override fun request(permissions: Set<CapturePermission>) = emptySet<CapturePermission>()

        override fun onResult(generation: Long, permission: CapturePermission, facts: PermissionFacts) = PermissionOutcome.Ignored("granted")

        override fun retire(reason: String) = Unit

        override fun readiness(): CaptureReadiness = CaptureReadiness.Ready
    }

    private fun interface Make {
        fun make(rig: Rig, schedule: StartupSchedule): CaptureSession
    }

    // ---- the rows --------------------------------------------------------------

    /** Everything settles inside begin: readiness still follows the state, not the count. */
    private fun u1_stepsThatSettleInsideBeginDoNotReportReadinessEarly(make: Make) {
        val rig = Rig(Mode.InsideBegin)
        val session = rig.wire(make.make(rig, StartupSchedule.NONE))
        assertEquals(CaptureStart.Started, rig.start(session), "every step settled before begin returned")
        assertEquals(listOf<Any>(CaptureReady.Ready), rig.all(), "ready exactly once, and nothing else")
        assertEquals(listOf("ready:tracks"), rig.listener.probes(), "and only once the session could answer for its tracks, from another thread")
        assertEquals(0, rig.ownership.pending, "nothing is left outstanding")
        assertNull(session.termination)
    }

    /**
     * The window the guard exists for: begin has returned, the session has NOT
     * published its state, and the camera's callback lands exactly there.
     */
    private fun u2_aSettlementInTheReturnWindowCannotReportReadiness(make: Make) {
        val window = HostGate("the session reached the post-begin window")
        val windowDone = HostGate("the camera callback finished inside that window")
        val rig = Rig(Mode.InTheReturnWindow, window = window, windowDone = windowDone)
        val schedule =
            object : StartupSchedule {
                override fun afterBegin() {
                    window.open()
                    windowDone.await()
                }
            }
        val session = rig.wire(make.make(rig, schedule))
        val start = rig.start(session)
        val racer = rig.acquirer.racer
        assertTrue(racer != null, "the driver started the callback it was scripted to deliver in that window")
        withSettlement(listOf(window, windowDone), listOf(racer)) {
            racer.join()
            assertEquals(CaptureStart.Pending, start, "the camera settled in the window, and the step it enabled did not")
            assertEquals(emptyList(), rig.all(), "no readiness was reported from inside that window")
            assertEquals(1, rig.ownership.pending, "exactly one step is outstanding: the one the opened camera enabled")
            rig.ownership.dispatcher.deliver { rig.acquirer.settle(CaptureResource.CameraStream) }
            assertEquals(listOf<Any>(CaptureReady.Ready), rig.all(), "ready once, after the last real prerequisite")
            assertEquals(listOf("ready:tracks"), rig.listener.probes())
        }
    }

    /**
     * A settlement concurrent with stop: the callback is parked INSIDE the
     * ownership's settlement while the row stops the session.
     */
    private fun u3_aSettlementConcurrentWithStopIsNeverReadiness(make: Make) {
        val settling = HostGate("the camera callback reached its settlement")
        val go = HostGate("the row released the settlement")
        val adoption =
            AdoptionSchedule {
                settling.open()
                go.await()
            }
        val rig = Rig(Mode.ByHand, adoption = adoption)
        val session = rig.wire(make.make(rig, StartupSchedule.NONE))
        assertEquals(CaptureStart.Pending, rig.start(session))
        assertEquals(1, rig.ownership.pending, "the camera is still opening")
        val outcome = AtomicReference<AdoptOutcome?>(null)
        val callback = HostWorker("camera-onOpened") { outcome.set(rig.acquirer.settle(CaptureResource.Camera)) }.start()
        withSettlement(listOf(settling, go), listOf(callback)) {
            settling.await(callback)
            // The platform is mid-settlement; the user leaves.
            session.stop(IllegalStateException("the user left"))
            go.openFully()
            callback.join()
            assertIs<AdoptOutcome.Retired>(outcome.get(), "the generation ended while the camera was opening")
            assertNull(rig.acquirer.tickets[CaptureResource.CameraStream], "a retired camera enables nothing")
            assertEquals(1, rig.all().size, "exactly one thing was said")
            assertIs<CaptureTermination.Cancelled>(rig.all().single(), "and it was the ending, not a readiness")
            assertTrue(
                rig.devices.closed().containsAll(listOf(rig.devices.identity(CaptureResource.Camera), rig.devices.identity(CaptureResource.Worker))),
                "the late camera and the worker that carried it are both closed",
            )
            assertEquals(0, rig.ownership.pending)
        }
    }

    /** The dependent submission throws where it stands: refused, unwound, said once, in order. */
    private fun u4_aSubmissionThatThrowsInsideTheCallbackRefusesStartupOnce(make: Make) {
        val rig = Rig(Mode.DependentSubmissionThrows)
        val session = rig.wire(make.make(rig, StartupSchedule.NONE))
        assertEquals(CaptureStart.Pending, rig.start(session))
        rig.ownership.dispatcher.deliver { rig.acquirer.settle(CaptureResource.Camera) }
        val said = rig.all()
        assertEquals(2, said.size, "exactly two things were said: $said")
        val refusal = assertIs<CaptureReady.Refused>(said[0], "the refusal comes first, after the unwind")
        assertTrue(refusal.reason.contains("createCaptureSession threw"), "by the platform's own reason: ${refusal.reason}")
        assertIs<CaptureTermination>(said[1], "then the ending, once")
        assertTrue(rig.ownership.isClosed, "what was acquired has unwound")
        assertEquals(0, rig.ownership.pending)
        assertEquals(listOf("ready:tracks", "terminated:tracks"), rig.listener.probes(), "neither callback was made under the session's lock")
    }

    /** A synchronous failure after a step is already in flight unwinds exactly the prefix. */
    private fun u5_aSynchronousFailureAfterASubmissionUnwindsAndNeverGoesNegative(make: Make) {
        val rig = Rig(Mode.ByHand, failAt = CaptureResource.Microphone)
        val session = rig.wire(make.make(rig, StartupSchedule.NONE))
        val start = assertIs<CaptureStart.Refused>(rig.start(session), "the microphone step failed where it stands")
        assertTrue(start.reason.contains("Microphone"), "named: ${start.reason}")
        assertEquals(1, rig.all().count { it is CaptureTermination }, "the ending, once")
        assertEquals(0, rig.all().count { it is CaptureReady }, "a refused start is never ready")
        assertEquals(1, rig.ownership.pending, "the camera was submitted before it and is still opening")
        assertEquals(
            listOf<Any>(rig.devices.identity(CaptureResource.VideoEncoder)),
            rig.devices.closed(),
            "the prefix unwinds in reverse; the worker waits for the callback still in flight",
        )
        rig.ownership.dispatcher.deliver { assertIs<AdoptOutcome.Retired>(rig.acquirer.settle(CaptureResource.Camera)) }
        assertEquals(
            listOf<Any>(
                rig.devices.identity(CaptureResource.VideoEncoder),
                rig.devices.identity(CaptureResource.Camera),
                rig.devices.identity(CaptureResource.Worker),
            ),
            rig.devices.closed(),
            "the late camera closes, and the worker that carried it goes last",
        )
        assertEquals(0, rig.ownership.pending)
        assertNull(session.video, "a refused start publishes no tracks")
    }

    /** The guard is not a counter entry: an unsubmitted settlement cannot spend it. */
    private fun u6_anUnsubmittedSettlementCannotConsumeTheStartupGuard(make: Make) {
        val rig = Rig(Mode.UnsubmittedSettlement)
        val session = rig.wire(make.make(rig, StartupSchedule.NONE))
        val thrown = assertFailsWith<IllegalStateException> { rig.start(session) }
        assertTrue(thrown.message.orEmpty().contains("never submitted"), "named: ${thrown.message}")
        assertEquals(0, rig.all().count { it is CaptureReady }, "and no readiness was manufactured out of the guard")
        assertEquals(1, rig.all().count { it is CaptureTermination }, "the start ended, once")
        assertTrue(rig.ownership.isClosed)
    }

    /** A Ready decided first is DELIVERED first, and the ending does not overtake it. */
    private fun u7_aReadyDecidedBeforeATerminationIsDeliveredBeforeIt(make: Make) {
        val held = HostGate("the row released the listener from onReady")
        val rig = Rig(Mode.InsideBegin, parkInReady = held)
        val session = rig.wire(make.make(rig, StartupSchedule.NONE))
        val starting = HostWorker("start") { rig.start(session) }.start()
        withSettlement(listOf(held, rig.listener.inReady), listOf(starting)) {
            rig.listener.inReady.await(starting)
            // The listener is inside onReady. The session is not holding its
            // lock across it, so the ending can be DECIDED right now...
            val stopper = HostWorker("stop") { session.stop(IllegalStateException("the user left")) }.start()
            val deadline = System.nanoTime() + 5_000_000_000L
            while (session.termination == null && System.nanoTime() < deadline) Thread.onSpinWait()
            assertTrue(session.termination != null, "the terminal decision was made while the readiness callback was still running")
            held.openFully()
            starting.join()
            stopper.join()
            // ...but its DELIVERY waits, so nobody sees the ending before the
            // readiness that was decided first.
            assertEquals(2, rig.all().size)
            assertEquals(CaptureReady.Ready, rig.all()[0], "the readiness decided first is delivered first")
            assertIs<CaptureTermination>(rig.all()[1], "and the ending it raced arrives behind it")
        }
    }

    /** A listener that stops the session from inside its own callback. */
    private fun u8_aListenerThatStopsFromInsideItsCallbackIsNotReordered(make: Make) {
        val stopped = AtomicReference<CaptureSession?>(null)
        val rig = Rig(Mode.InsideBegin, insideReady = { session -> stopped.set(session); session.stop(null) })
        val session = rig.wire(make.make(rig, StartupSchedule.NONE))
        assertEquals(CaptureStart.Started, rig.start(session))
        assertEquals(2, rig.all().size, "both things were said: ${rig.all()}")
        assertEquals(CaptureReady.Ready, rig.all()[0], "the readiness the listener was called for")
        assertIs<CaptureTermination>(rig.all()[1], "then the ending it asked for, after its own callback returned")
        assertEquals(
            emptyList(),
            rig.listener.nestedDeliveries(),
            "the ending was not delivered recursively from inside the readiness callback",
        )
        assertTrue(stopped.get() === session)
    }

    /** Same count, wrong step: the camera is still opening and nothing is ready. */
    private fun u9_aSettlementOfADifferentStepIsNotTheSubmittedOne(make: Make) {
        val rig = Rig(Mode.WrongResourceSettlement)
        val session = rig.wire(make.make(rig, StartupSchedule.NONE))
        val thrown = assertFailsWith<IllegalStateException> { rig.start(session) }
        assertTrue(thrown.message.orEmpty().contains("never submitted"), "named by the step reported: ${thrown.message}")
        assertEquals(0, rig.all().count { it is CaptureReady }, "the real camera never opened: nothing is ready")
        assertEquals(1, rig.all().count { it is CaptureTermination }, "the start ended, once")
    }

    /** One step settled twice while another is still pending. */
    private fun u10_aDuplicateSettlementWhileAnotherStepIsPendingIsRefused(make: Make) {
        val rig = Rig(Mode.ByHand)
        val session = rig.wire(make.make(rig, StartupSchedule.NONE))
        assertEquals(CaptureStart.Pending, rig.start(session))
        rig.ownership.dispatcher.deliver { rig.acquirer.settle(CaptureResource.Camera) }
        assertEquals(1, rig.ownership.pending, "the step the camera enabled is now in flight")
        val thrown = assertFailsWith<IllegalStateException> { rig.acquirer.settle(CaptureResource.Camera) }
        assertTrue(thrown.message.orEmpty().contains("never submitted"), "named: ${thrown.message}")
        assertEquals(0, rig.all().count { it is CaptureReady }, "and the pending step was not counted away by it")
        assertEquals(1, rig.ownership.pending)
    }

    /** A notification that throws: attempted once, and nothing after it is stranded. */
    private fun u11_aListenerFailureStrandsNeitherTheDrainNorTheCleanup(make: Make) {
        val raised = IllegalStateException("the listener threw")
        val rig = Rig(Mode.InsideBegin, raiseOnReady = { raised })
        val session = rig.wire(make.make(rig, StartupSchedule.NONE))
        val thrown = assertFailsWith<IllegalStateException> { rig.start(session) }
        assertTrue(thrown === raised, "the notification's own failure leaves, by identity")
        assertEquals(listOf<Any>(CaptureReady.Ready), rig.all(), "attempted once, and never delivered again")
        // The failure did not keep the delivery, so the ending still gets out,
        // and the mandatory cleanup still happens.
        session.stop(null)
        assertEquals(2, rig.all().size, "the ending was delivered behind it: ${rig.all()}")
        assertIs<CaptureTermination>(rig.all()[1])
        assertTrue(rig.ownership.isClosed)
        assertEquals(0, rig.ownership.pending)
    }

    /** A second start while the first is still inside the driver does not enter it. */
    private fun u12_aConcurrentStartIsRefusedWithoutReEnteringTheDriver(make: Make) {
        val window = HostGate("the first start reached the post-begin window")
        val released = HostGate("the row released the post-begin window")
        val rig = Rig(Mode.ByHand)
        val schedule =
            object : StartupSchedule {
                override fun afterBegin() {
                    window.open()
                    released.await()
                }
            }
        val session = rig.wire(make.make(rig, schedule))
        val first = AtomicReference<CaptureStart?>(null)
        val starter = HostWorker("start") { first.set(rig.start(session)) }.start()
        withSettlement(listOf(window, released), listOf(starter)) {
            window.await(starter)
            val second = assertIs<CaptureStart.Refused>(rig.start(session), "the concurrent start is refused")
            assertTrue(second.reason.contains("already been started"), "by name: ${second.reason}")
            assertEquals(1, rig.acquirer.beginCalls, "the driver was entered exactly once")
            released.openFully()
            starter.join()
            assertEquals(CaptureStart.Pending, first.get(), "and the start that claimed it is the one that ran")
            assertEquals(1, rig.ownership.pending, "exactly one camera was submitted")
            assertEquals(0, rig.all().count { it is CaptureReady }, "the refusal reported no readiness")
        }
    }

    /**
     * The last prerequisite has settled and the readiness is not decided yet.
     * The user leaves in exactly that window: the ending is the only thing
     * said, and no readiness follows a terminal decision.
     */
    private fun u13_anEndingDecidedInTheReadinessWindowLeavesNoReadinessBehindIt(make: Make) {
        val window = HostGate("the session reached its readiness decision")
        val released = HostGate("the row released the readiness window")
        val rig = Rig(Mode.ByHand)
        val schedule =
            object : StartupSchedule {
                override fun beforeReadiness() {
                    window.open()
                    released.await()
                }
            }
        val session = rig.wire(make.make(rig, schedule))
        assertEquals(CaptureStart.Pending, rig.start(session))
        rig.ownership.dispatcher.deliver { rig.acquirer.settle(CaptureResource.Camera) }
        val settling = HostWorker("camera-stream-callback") { rig.acquirer.settle(CaptureResource.CameraStream) }.start()
        withSettlement(listOf(window, released), listOf(settling)) {
            window.await(settling)
            // The prerequisites are all in; the readiness has not been decided.
            assertEquals(emptyList(), rig.all(), "nothing has been said yet")
            session.stop(IllegalStateException("the user left"))
            released.openFully()
            settling.join()
            assertEquals(1, rig.all().size, "exactly one thing was said: ${rig.all()}")
            assertIs<CaptureTermination.Cancelled>(rig.all().single(), "and it was the ending; a terminal decision is never followed by a readiness")
            assertTrue(rig.ownership.isClosed)
        }
    }

    /**
     * The user leaves in the readiness window of a start that would otherwise
     * have returned Started. Only the ending is said, and the start says what
     * actually happened rather than claiming a startup the terminal ended.
     */
    private fun u14_aTerminalInTheReadinessWindowRefusesTheStartItEnded(make: Make) {
        val stopped = AtomicReference<CaptureSession?>(null)
        val once = java.util.concurrent.atomic.AtomicBoolean(true)
        val schedule =
            object : StartupSchedule {
                override fun beforeReadiness() {
                    if (once.compareAndSet(true, false)) stopped.get()?.stop(null)
                }
            }
        val rig = Rig(Mode.InsideBegin)
        val session = rig.wire(make.make(rig, schedule))
        stopped.set(session)
        val start = assertIs<CaptureStart.Refused>(rig.start(session), "the startup did not survive to a readiness")
        assertTrue(start.reason.contains("ended"), "by name: ${start.reason}")
        assertEquals(1, rig.all().size, "exactly one thing was said: ${rig.all()}")
        assertIs<CaptureTermination>(rig.all().single(), "and it was the ending, never a readiness")
        assertNull(session.video, "a startup that ended before it was ready publishes no tracks")
        assertTrue(rig.ownership.isClosed)
    }

    /** A settlement that FAILED inside begin is not undone by begin then returning null. */
    private fun u15_aFailedSettlementInsideBeginRefusesTheStart(make: Make) {
        val rig = Rig(Mode.FailedSettlementInsideBegin)
        val session = rig.wire(make.make(rig, StartupSchedule.NONE))
        val start = assertIs<CaptureStart.Refused>(rig.start(session), "the camera failed before begin returned")
        assertTrue(start.reason.isNotEmpty(), "named: ${start.reason}")
        assertNull(session.video, "and no tracks are published by a startup that failed")
        assertEquals(1, rig.all().count { it is CaptureTermination }, "the ending, once")
        assertEquals(0, rig.all().count { it is CaptureReady && it !is CaptureReady.Refused }, "and never a successful readiness")
        assertTrue(rig.ownership.isClosed)
        assertEquals(0, rig.ownership.pending)
    }

    /**
     * The last prerequisite settles through its own callback in the window
     * after the startup's disposition is taken. That readiness is real: the
     * session is live and ready, and the start that was still in flight must
     * not answer with a refusal.
     */
    private fun u16_aCallbackThatWinsTheReadinessDoesNotRefuseTheStart(make: Make) {
        val rig = Rig(Mode.ByHand)
        val once = java.util.concurrent.atomic.AtomicBoolean(true)
        val schedule =
            object : StartupSchedule {
                override fun afterStartupDecision() {
                    if (!once.compareAndSet(true, false)) return
                    // onOpened, then onConfigured: the platform finishes the
                    // acquisition while the start is still on its way back.
                    rig.acquirer.settle(CaptureResource.Camera)
                    rig.acquirer.settle(CaptureResource.CameraStream)
                }
            }
        val session = rig.wire(make.make(rig, schedule))
        val start = rig.start(session)
        assertTrue(start !is CaptureStart.Refused, "a readiness another caller decided is not a refusal: $start")
        assertEquals(CaptureStart.Pending, start, "the disposition taken was a live pending startup")
        assertEquals(listOf<Any>(CaptureReady.Ready), rig.all(), "and the readiness was said, once")
        assertNull(session.termination, "nothing ended")
        assertTrue(session.video != null && session.audio != null, "a live session publishes its tracks")
        assertEquals(0, rig.ownership.pending)
    }

    /**
     * The same window, with the listener stopping from inside the readiness it
     * was called for. The successful readiness stands: it is not retroactively
     * turned into a refusal by the ending that followed it.
     */
    private fun u17_aReadinessFollowedByItsListenersStopIsStillSuccessful(make: Make) {
        val stopped = AtomicReference<CaptureSession?>(null)
        val rig = Rig(Mode.ByHand, insideReady = { session -> stopped.set(session); session.stop(null) })
        val once = java.util.concurrent.atomic.AtomicBoolean(true)
        val schedule =
            object : StartupSchedule {
                override fun afterStartupDecision() {
                    if (!once.compareAndSet(true, false)) return
                    rig.acquirer.settle(CaptureResource.Camera)
                    rig.acquirer.settle(CaptureResource.CameraStream)
                }
            }
        val session = rig.wire(make.make(rig, schedule))
        val start = rig.start(session)
        assertTrue(start !is CaptureStart.Refused, "the readiness succeeded before the ending: $start")
        assertEquals(2, rig.all().size, "both were said: ${rig.all()}")
        assertEquals(CaptureReady.Ready, rig.all()[0], "the readiness first")
        assertIs<CaptureTermination>(rig.all()[1], "then the ending its listener asked for")
        assertTrue(stopped.get() === session)
        assertTrue(rig.ownership.isClosed)
    }

    /**
     * The LAST prerequisite fails. Its failure is recorded where the
     * prerequisite is consumed, so a start reaching its disposition in the
     * window before the cleanup runs finds an ended startup -- never a zero
     * that says the failed step succeeded.
     */
    private fun u18_aFailedLastPrerequisiteCannotOpenAReadinessWindow(make: Make) {
        val parked = HostGate("the start parked after the driver submitted the camera")
        val released = HostGate("the failure was recorded; the start may reach its disposition")
        val answered = HostGate("the start produced its result")
        val started = AtomicReference<CaptureStart?>(null)
        val rig = Rig(Mode.ByHand)
        val schedule =
            object : StartupSchedule {
                override fun afterBegin() {
                    parked.open()
                    released.await()
                }

                override fun afterFailureRecorded() {
                    // The failed prerequisite has been consumed and its
                    // failure recorded; the cleanup has not run.
                    released.openFully()
                    answered.await()
                }
            }
        val session = rig.wire(make.make(rig, schedule))
        val starting = HostWorker("start") { started.set(rig.start(session)) }.start()
        val failing = HostWorker("camera-onError") { rig.acquirer.failed(CaptureResource.Camera) }
        withSettlement(listOf(parked, released, answered), listOf(starting)) {
            // The camera is submitted and the start is parked in the window.
            parked.await(starting)
            failing.start()
            withSettlement(emptyList(), listOf(failing)) {
                starting.join()
                answered.openFully()
                failing.join()
            }
            assertEquals(
                0,
                rig.all().count { it is CaptureReady && it !is CaptureReady.Refused },
                "a failed prerequisite never announces Ready: ${rig.all()}",
            )
            val start = assertIs<CaptureStart.Refused>(started.get(), "the start answers the failure that won its disposition")
            assertTrue(start.reason.contains("ended"), "by name: ${start.reason}")
            assertNull(session.video, "and it publishes no tracks")
            assertEquals(1, rig.all().count { it is CaptureTermination }, "exactly one terminal: ${rig.all()}")
            assertTrue(rig.ownership.isClosed)
            assertEquals(0, rig.ownership.pending, "nothing is left outstanding")
        }
    }

    /**
     * The synchronous half of the same invariant: an acquisition failure has
     * been accepted for unwind, and the last successful callback settles
     * before the cleanup runs. It cannot announce a readiness either.
     */
    private fun u19_aRecordedStartupFailureClosesTheReadinessWindow(make: Make) {
        val rig = Rig(Mode.ByHand, failAt = CaptureResource.Microphone)
        val once = java.util.concurrent.atomic.AtomicBoolean(true)
        val schedule =
            object : StartupSchedule {
                override fun afterFailureRecorded() {
                    // onOpened, then onConfigured, for the steps submitted
                    // before the failing one: the last prerequisite settles
                    // here, so this is the window a readiness would open in.
                    if (!once.compareAndSet(true, false)) return
                    rig.acquirer.settle(CaptureResource.Camera)
                    rig.acquirer.settle(CaptureResource.CameraStream)
                }
            }
        val session = rig.wire(make.make(rig, schedule))
        val start = assertIs<CaptureStart.Refused>(rig.start(session), "the microphone failed where it stood")
        assertTrue(start.reason.contains("Microphone"), "named: ${start.reason}")
        assertEquals(0, rig.all().count { it is CaptureReady }, "the camera that opened afterwards is not a readiness: ${rig.all()}")
        assertEquals(1, rig.all().count { it is CaptureTermination }, "one terminal")
        assertNull(session.video, "and no tracks")
        assertTrue(rig.ownership.isClosed)
        assertEquals(0, rig.ownership.pending, "every late step settled; nothing is left outstanding")
        assertTrue(
            rig.devices.closed().containsAll(listOf(rig.devices.identity(CaptureResource.Camera), rig.devices.identity(CaptureResource.Worker))),
            "the late camera and the worker that carried it are both closed: ${rig.devices.closed()}",
        )
    }

    /** The converse: a disposition already taken as Pending stays valid when a later callback fails. */
    private fun u20_aLivePendingDispositionSurvivesALaterFailure(make: Make) {
        val rig = Rig(Mode.ByHand)
        val session = rig.wire(make.make(rig, StartupSchedule.NONE))
        assertEquals(CaptureStart.Pending, rig.start(session), "a live startup with a prerequisite still in flight")
        rig.ownership.dispatcher.deliver { rig.acquirer.failed(CaptureResource.Camera) }
        assertEquals(0, rig.all().count { it is CaptureReady && it !is CaptureReady.Refused }, "the failure announced no readiness")
        assertEquals(1, rig.all().count { it is CaptureTermination }, "one terminal")
        assertTrue(rig.ownership.isClosed)
        assertEquals(0, rig.ownership.pending)
    }

    private fun runStartup(make: Make) {
        u1_stepsThatSettleInsideBeginDoNotReportReadinessEarly(make)
        u2_aSettlementInTheReturnWindowCannotReportReadiness(make)
        u3_aSettlementConcurrentWithStopIsNeverReadiness(make)
        u4_aSubmissionThatThrowsInsideTheCallbackRefusesStartupOnce(make)
        u5_aSynchronousFailureAfterASubmissionUnwindsAndNeverGoesNegative(make)
        u6_anUnsubmittedSettlementCannotConsumeTheStartupGuard(make)
        u7_aReadyDecidedBeforeATerminationIsDeliveredBeforeIt(make)
        u8_aListenerThatStopsFromInsideItsCallbackIsNotReordered(make)
        u9_aSettlementOfADifferentStepIsNotTheSubmittedOne(make)
        u10_aDuplicateSettlementWhileAnotherStepIsPendingIsRefused(make)
        u11_aListenerFailureStrandsNeitherTheDrainNorTheCleanup(make)
        u12_aConcurrentStartIsRefusedWithoutReEnteringTheDriver(make)
        u13_anEndingDecidedInTheReadinessWindowLeavesNoReadinessBehindIt(make)
        u14_aTerminalInTheReadinessWindowRefusesTheStartItEnded(make)
        u15_aFailedSettlementInsideBeginRefusesTheStart(make)
        u16_aCallbackThatWinsTheReadinessDoesNotRefuseTheStart(make)
        u17_aReadinessFollowedByItsListenersStopIsStillSuccessful(make)
        u18_aFailedLastPrerequisiteCannotOpenAReadinessWindow(make)
        u19_aRecordedStartupFailureClosesTheReadinessWindow(make)
        u20_aLivePendingDispositionSurvivesALaterFailure(make)
    }

    private val real = Make { rig, schedule -> PlatformCaptureSession(GENERATION, rig.ownership, rig.acquirer, rig.video, rig.audio, rig.listener, schedule) }
    private val conforming = Make { rig, schedule -> ConformingSession(GENERATION, rig.ownership, rig.acquirer, rig.video, rig.audio, rig.listener, schedule) }

    @Test
    fun u_theCaptureSessionStartsOnItsRealPrerequisites() = runStartup(real)

    @Test
    fun s_theStartupRulesAreSatisfiableByAConformingFixture() = runStartup(conforming)

    @Test
    fun m1_reportingMoreSettlementsThanSubmissionsIsCaught() {
        val rig = Rig(Mode.ByHand)
        val session = rig.wire(conforming.make(rig, StartupSchedule.NONE))
        assertEquals(CaptureStart.Pending, rig.start(session))
        rig.ownership.dispatcher.deliver { rig.acquirer.settle(CaptureResource.Camera) }
        rig.ownership.dispatcher.deliver { rig.acquirer.settle(CaptureResource.CameraStream) }
        assertEquals(listOf<Any>(CaptureReady.Ready), rig.all())
        val thrown = assertFailsWith<IllegalStateException> { rig.acquirer.settle(CaptureResource.CameraStream) }
        assertTrue(thrown.message.orEmpty().contains("never submitted"), "named: ${thrown.message}")
    }

    /**
     * A session that MARKS its readiness decision, releases the lock, and only
     * then queues the notification. The gap between the two is forced open, and
     * a termination decides, queues and delivers inside it.
     */
    private class SplitAnnouncingSession(
        override val generation: Long,
        private val listener: CaptureSessionListener,
        private val window: HostGate,
        private val resume: HostGate,
    ) : CaptureSession {
        private val lock = Any()
        private val outbox = ArrayDeque<() -> Unit>()
        private var readyReported = false
        private var decided: CaptureTermination? = null
        private var delivering = false

        override val video: CaptureHandoff? get() = null

        override val audio: CaptureHandoff? get() = null

        override val termination: CaptureTermination? get() = synchronized(lock) { decided }

        /** This generation, and no ending decided, under this session's lock. */
        override fun admitPublication(generation: Long): Boolean = synchronized(lock) { generation == this.generation && decided == null }

        /** This stand-in owns no children to close and admits no format callbacks, so a decided ending leaves no causes or cleanup behind; with none decided there is nothing to receive. */
        override suspend fun retirement(): RetirementReceipt = RetirementReceipt(checkNotNull(termination) { "no ending was claimed" }, causes = emptyList(), cleanup = emptyList())

        override fun start(permissions: PermissionFlow, cameraFacts: CaptureCapability, audioFacts: CaptureCapability): CaptureStart =
            CaptureStart.Pending

        /** The split shape: mark under the lock, publish afterwards. */
        fun announceReady() {
            val first = synchronized(lock) { if (readyReported || decided != null) false else { readyReported = true; true } }
            if (!first) return
            window.open()
            resume.await()
            synchronized(lock) { outbox.addLast { listener.onReady(generation, CaptureReady.Ready) } }
            deliver()
        }

        override fun onTrackEnded(generation: Long, kind: MediaKind) = Unit

        override fun onFault(generation: Long, fault: CaptureFault) = Unit

        override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) = Unit

        override fun stop(primary: Exception?): Exception? {
            val termination = CaptureTermination.Completed
            val first = synchronized(lock) { if (decided == null) { decided = termination; true } else false }
            if (!first) return primary
            synchronized(lock) { outbox.addLast { listener.onTerminated(generation, termination) } }
            deliver()
            return primary
        }

        private fun deliver() {
            while (true) {
                val mine = synchronized(lock) { if (delivering) false else { delivering = true; true } }
                if (!mine) return
                try {
                    while (true) {
                        val next = synchronized(lock) { outbox.removeFirstOrNull() } ?: break
                        next()
                    }
                } finally {
                    synchronized(lock) { delivering = false }
                }
                if (synchronized(lock) { outbox.isEmpty() }) return
            }
        }
    }

    @Test
    fun m2_markingReadinessBeforePublishingItLetsTheEndingOvertakeIt() {
        val window = HostGate("the split session reached its publication gap")
        val resume = HostGate("the row released the publication gap")
        val listener = HostSessionListener()
        val split = SplitAnnouncingSession(GENERATION, listener, window, resume)
        val announcing = HostWorker("ready") { split.announceReady() }.start()
        withSettlement(listOf(window, resume), listOf(announcing)) {
            window.await(announcing)
            // The readiness has been DECIDED and not yet published. The ending
            // decides, publishes and delivers in that gap.
            split.stop(null)
            resume.openFully()
            announcing.join()
            assertEquals(2, listener.all().size)
            assertIs<CaptureTermination>(
                listener.all()[0],
                "this is the defect: the ending overtook a readiness that was decided before it",
            )
            assertEquals(CaptureReady.Ready, listener.all()[1])
        }
    }

    private companion object {
        const val GENERATION = 1L
    }

    private fun Rig.all(): List<Any> = listener.all()
}
