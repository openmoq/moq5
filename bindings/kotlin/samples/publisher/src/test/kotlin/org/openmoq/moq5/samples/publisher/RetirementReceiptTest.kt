package org.openmoq.moq5.samples.publisher

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * The owner's result is settled only once the ending it did not claim itself
 * has finished its cleanup, and whatever that cleanup or the ending's
 * notification delivery raised is carried by the result -- whichever thread
 * claimed the ending, and whichever thread delivered its notifications.
 *
 * Two real geometries, both through the real session and coordinator, and
 * their combination (r10):
 * - a fault claimant on an owned worker holds its first close (r1-r4, r8);
 * - a readiness delivered by the camera's own thread keeps the notification
 *   drain while the coordinator claims Completed itself, so the terminal is
 *   delivered by that other drain owner (r5, r6). The coordinator's Completed
 *   claim is always its own, made inline from its last accepted `endTrack`,
 *   so a held claimant close on the successful path would hold the owner
 *   itself; the held delivery is the reachable successful-path geometry.
 *
 * Every worker and gate is owned by its row and released through handles the
 * row keeps itself, on every path, with bounded joins.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class RetirementReceiptTest {

    /**
     * A fault claimant on an owned worker, held inside its first close. When
     * released, each failing close first retires the real handoff, then
     * raises its declared failure at the close boundary.
     */
    private class HeldClaim(private val rig: Rig, val videoFailure: Throwable? = null, val audioFailure: Throwable? = null) {
        private val entered = CountDownLatch(1)
        private val release = CountDownLatch(1)
        private val raised = AtomicReference<Throwable?>()
        val fault = CaptureFault.Codec(MediaKind.Video, "a fault from outside the handoff")
        private val worker = Thread({ runCatching { rig.forward.onFault(GENERATION, fault) }.exceptionOrNull()?.let { raised.set(it) } }, "held-claim-worker")

        init {
            rig.videoEntry.beforeClose = {
                entered.countDown()
                check(release.await(BOUND_SECONDS, TimeUnit.SECONDS)) { "the held close was never released" }
                videoFailure?.let {
                    rig.video.handoff.close(CloseMode.Failed)
                    throw it
                }
            }
            audioFailure?.let { failure ->
                rig.audioEntry.beforeClose = {
                    rig.audio.handoff.close(CloseMode.Failed)
                    throw failure
                }
            }
        }

        val joined: Boolean get() = worker.state != Thread.State.NEW && !worker.isAlive

        /** What the claim's own call raised to its worker. */
        val claimantSaw: Throwable? get() = raised.get()

        fun open() {
            worker.start()
            assertTrue(entered.await(BOUND_SECONDS, TimeUnit.SECONDS), "the first close was reached")
            assertEquals(CaptureTermination.Failed(fault), rig.session.termination, "the ending is claimed")
            assertFalse(rig.video.handoff.ended, "the first close has not happened")
            assertFalse(rig.events.terminal.isCompleted, "the terminal is not yet delivered")
        }

        /** Releases the close and joins the worker, bounded. Asserts nothing; safe to repeat. */
        fun rescue() {
            release.countDown()
            if (worker.state != Thread.State.NEW) worker.join(BOUND_SECONDS * 1_000L)
        }
    }

    /** The camera step, submitted by `begin` and settled later, from the camera's own thread. */
    private class CameraLater : CaptureAcquirer {
        override val order = listOf(CaptureResource.Camera)
        val closes = mutableListOf<Any>()
        private var ownership: CaptureOwnership? = null
        private var steps: AcquisitionSteps? = null
        private var ticket: AcquisitionTicket? = null

        override fun begin(ownership: CaptureOwnership, steps: AcquisitionSteps): AcquireOutcome.Failed? {
            this.ownership = ownership
            this.steps = steps
            ticket = ownership.request(CaptureResource.Camera).also { steps.submitted(CaptureResource.Camera) }
            return null
        }

        /** onOpened: adopted, then reported, on the calling thread. */
        fun opened() {
            val outcome = checkNotNull(ownership).adopt(checkNotNull(ticket), HostHandle(HostResource("camera"), closes))
            assertEquals(AdoptOutcome.Adopted, outcome)
            checkNotNull(steps).settled(CaptureResource.Camera, outcome)
        }
    }

    /**
     * The coordinator's events, with readiness HELD after it is recorded --
     * so the thread delivering it keeps the session's notification drain --
     * and a terminal listener that may fail after the terminal is recorded.
     */
    private class HeldDeliveries(private val events: BroadcastEvents, private val terminalFailure: Throwable?, private val holdReadiness: Boolean = true) : CaptureSessionListener {
        val readyEntered = CountDownLatch(1)
        val readyRelease = CountDownLatch(1)
        val discards = AtomicInteger()
        val readyThread = AtomicReference<Thread?>()
        val terminalThread = AtomicReference<Thread?>()

        override fun onReady(generation: Long, ready: CaptureReady) {
            events.onReady(generation, ready)
            readyThread.set(Thread.currentThread())
            readyEntered.countDown()
            if (holdReadiness) check(readyRelease.await(BOUND_SECONDS, TimeUnit.SECONDS)) { "the held readiness was never released" }
        }

        override fun onTerminated(generation: Long, termination: CaptureTermination) {
            events.onTerminated(generation, termination)
            terminalThread.set(Thread.currentThread())
            terminalFailure?.let { throw it }
        }

        override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) {
            discards.incrementAndGet()
        }
    }

    /** The camera's own thread: settles the step, and so delivers the readiness it decides. */
    private class CameraThread(private val camera: CameraLater) {
        private val raised = AtomicReference<Throwable?>()
        private val worker = Thread({ runCatching { camera.opened() }.exceptionOrNull()?.let { raised.set(it) } }, "camera-callback")

        val saw: Throwable? get() = raised.get()

        val joined: Boolean get() = worker.state != Thread.State.NEW && !worker.isAlive

        /** The camera's thread itself, so a held boundary can recognise it. */
        val thread: Thread get() = worker

        fun start() = worker.start()

        fun join() {
            if (worker.state != Thread.State.NEW) worker.join(BOUND_SECONDS * 1_000L)
        }
    }

    /** Runs [body] on its own thread, keeping what it raised; joined with a bound, never waited on otherwise. */
    private class OwnedWorker(name: String, body: () -> Unit) {
        private val raised = AtomicReference<Throwable?>()
        private val worker = Thread({ runCatching(body).exceptionOrNull()?.let { raised.set(it) } }, name)

        val saw: Throwable? get() = raised.get()

        val joined: Boolean get() = worker.state != Thread.State.NEW && !worker.isAlive

        fun start() = worker.start()

        fun join() {
            if (worker.state != Thread.State.NEW) worker.join(BOUND_SECONDS * 1_000L)
        }
    }

    /** A driver whose submission throws where it stands, after [first] has run inside `begin`. */
    private class ThrowingBegin(private val first: () -> Unit, private val raise: Throwable) : CaptureAcquirer {
        override val order = emptyList<CaptureResource>()

        override fun begin(ownership: CaptureOwnership, steps: AcquisitionSteps): AcquireOutcome.Failed? {
            first()
            throw raise
        }
    }

    /**
     * The coordinator's events, recorded first, with any delivery optionally
     * HELD after it is recorded -- the delivering thread keeps the session's
     * drain meanwhile -- and optionally FAILING once released.
     */
    private class ScriptedDeliveries(private val events: BroadcastEvents) : CaptureSessionListener {
        @Volatile var holdReady = false
        @Volatile var readyFailure: Throwable? = null
        @Volatile var holdDiscard = false
        @Volatile var discardFailure: Throwable? = null

        /** When set, [discardFailure] is raised only for this kind. */
        @Volatile var discardFailureKind: MediaKind? = null
        private val discardedKinds = mutableListOf<MediaKind>()
        @Volatile var terminalFailure: Throwable? = null
        val readyEntered = CountDownLatch(1)
        val discardEntered = CountDownLatch(1)
        private val readyRelease = CountDownLatch(1)
        private val discardRelease = CountDownLatch(1)

        fun releaseReady() = readyRelease.countDown()

        fun releaseDiscard() = discardRelease.countDown()

        /** Every discard delivered to this listener, by kind, in order. */
        fun discarded(): List<MediaKind> = synchronized(discardedKinds) { discardedKinds.toList() }

        /** Every hold released; asserts nothing; safe to repeat. */
        fun rescue() {
            readyRelease.countDown()
            discardRelease.countDown()
        }

        override fun onReady(generation: Long, ready: CaptureReady) {
            events.onReady(generation, ready)
            readyEntered.countDown()
            if (holdReady) check(readyRelease.await(BOUND_SECONDS, TimeUnit.SECONDS)) { "the held readiness was never released" }
            readyFailure?.let { throw it }
        }

        override fun onTerminated(generation: Long, termination: CaptureTermination) {
            events.onTerminated(generation, termination)
            terminalFailure?.let { throw it }
        }

        override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) {
            synchronized(discardedKinds) { discardedKinds.add(kind) }
            discardEntered.countDown()
            if (holdDiscard) check(discardRelease.await(BOUND_SECONDS, TimeUnit.SECONDS)) { "the held discard was never released" }
            discardFailure?.takeIf { discardFailureKind.let { only -> only == null || only == kind } }?.let { throw it }
        }
    }

    private fun causeChain(raised: Throwable?, expected: Throwable) = generateSequence(raised) { it.cause }.any { it === expected }

    private fun Throwable.suppressedCount(of: Throwable) = suppressed.count { it === of }

    // -- a claim held by another thread ------------------------------------------------------------

    /** The shared held-claim drive: a queued video frame, the claim held, then audio resolves and the owner runs into it. */
    private suspend fun kotlinx.coroutines.test.TestScope.heldClaimRun(rig: Rig, claim: HeldClaim): Triple<Boolean, Result<BroadcastOutcome>?, Result<Unit>> {
        val video = CompletableDeferred(validatedVideo())
        val audio = CompletableDeferred<CompleteTrackFormat>()
        var heldOwnerDone = true
        var outcome: Result<BroadcastOutcome>? = null
        val wrapper =
            runCatching {
                withRun({ rig.run(rig.formatPublication(FormatSources(video, audio), BUDGET)) }) { owner ->
                    try {
                        runCurrent()
                        stillLive(owner)
                        deliver(rig.video, 0, VIDEO_ANNEXB.copyOf(), 33_333L)
                        runCurrent()
                        claim.open()
                        audio.complete(validatedAudio())
                        runCurrent()
                        heldOwnerDone = owner.job.isCompleted
                    } finally {
                        claim.rescue()
                    }
                    runCurrent()
                    outcome = settled(owner)
                }
            }
        return Triple(heldOwnerDone, outcome, wrapper)
    }

    @Test
    fun `r1 the owner does not settle while another thread's claim holds its close, and carries the failure that close reports`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val failure = IllegalStateException("the claimant's close failed after the real handoff closed")
        val claim = HeldClaim(rig, videoFailure = failure)
        val (heldOwnerDone, outcome, _) = heldClaimRun(rig, claim)
        assertFalse(heldOwnerDone, "the owner had not settled while the claimant still held its close")
        assertTrue(claim.joined, "the claimant was joined")
        assertSame(failure, claim.claimantSaw, "the claimant's own call still reports its cleanup failure")
        val raised = checkNotNull(outcome).exceptionOrNull()
        assertTrue(causeChain(raised, failure), "the owner's result carries the claimant's close failure: $raised")
        assertTrue(rig.video.handoff.ended && rig.audio.handoff.ended, "both real handoffs retired")
        assertEquals(listOf(Discard(GENERATION, MediaKind.Video, 1)), rig.forward.discards(), "the queued frame was discarded")
        assertEquals(emptyList(), rig.tracks.labels(), "no sender effect: nothing was admitted after the claim")
        assertTrue(rig.completion.calls.isEmpty())
        assertTrue(rig.owner.isDecided)
    }

    @Test
    fun `r2 control - the same held claim with a clean close settles Unfinished, only after the close is released`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val claim = HeldClaim(rig)
        val (heldOwnerDone, outcome, wrapper) = heldClaimRun(rig, claim)
        assertFalse(heldOwnerDone, "the owner had not settled while the claimant still held its close")
        assertTrue(claim.joined)
        assertNull(claim.claimantSaw, "a clean close reports nothing")
        wrapper.getOrThrow()
        assertEquals(BroadcastOutcome.Unfinished("the capture session ended before the video track was admitted", null, null), checkNotNull(outcome).getOrThrow())
        assertEquals(listOf(Discard(GENERATION, MediaKind.Video, 1)), rig.forward.discards())
        assertEquals(emptyList(), rig.tracks.labels())
        assertTrue(rig.completion.calls.isEmpty())
        assertTrue(rig.owner.isDecided)
    }

    @Test
    fun `r3 every close is attempted and every close failure is carried by the owner exactly once`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val videoFailure = IllegalStateException("the video close failed")
        val audioFailure = IllegalStateException("the audio close failed")
        val claim = HeldClaim(rig, videoFailure, audioFailure)
        val (heldOwnerDone, outcome, _) = heldClaimRun(rig, claim)
        assertFalse(heldOwnerDone)
        assertTrue(claim.joined)
        assertTrue(rig.video.handoff.ended && rig.audio.handoff.ended, "the audio close was attempted after the video close failed")
        val raised = checkNotNull(outcome).exceptionOrNull()
        assertTrue(causeChain(raised, videoFailure), "the first close failure is the owner's failure: $raised")
        assertEquals(1, videoFailure.suppressedCount(audioFailure), "the second close failure is carried, once")
        assertEquals(0, audioFailure.suppressedCount(videoFailure), "and the graph is not inverted")
        assertEquals(listOf<Throwable>(videoFailure, audioFailure), rig.session.retirement().cleanup, "the receipt itself holds both, in the order found -- not only the claimant's graph")
        assertEquals(emptyList(), rig.tracks.labels())
        assertTrue(rig.completion.calls.isEmpty())
    }

    @Test
    fun `r4 the owner's own cancellation stays primary and carries the held claim's close failure once`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val failure = IllegalStateException("the claimant's close failed after the real handoff closed")
        val claim = HeldClaim(rig, videoFailure = failure)
        val video = CompletableDeferred(validatedVideo())
        val audio = CompletableDeferred<CompleteTrackFormat>()
        var heldOwnerDone = true
        var why: Throwable? = null
        var run: RunOwner? = null
        var outcome: Result<BroadcastOutcome>? = null
        val wrapper =
            runCatching {
                withRun({ rig.run(rig.formatPublication(FormatSources(video, audio), BUDGET)) }) { owner ->
                    try {
                        runCurrent()
                        stillLive(owner)
                        claim.open()
                        run = owner
                        why = owner.cancel("the row stops the broadcast")
                        runCurrent()
                        heldOwnerDone = owner.job.isCompleted
                    } finally {
                        claim.rescue()
                    }
                    runCurrent()
                    outcome = settled(owner)
                }
            }
        assertFalse(heldOwnerDone, "the cancelled owner had not settled while the claimant still held its close")
        assertTrue(claim.joined)
        val raised = checkNotNull(outcome).exceptionOrNull()
        assertTrue(checkNotNull(run).issued(checkNotNull(raised)) && causeChain(raised, checkNotNull(why)), "the owner's own cancellation, or its reconstruction, is what it left with: $raised")
        assertEquals(1, generateSequence(raised) { it.cause }.sumOf { it.suppressedCount(failure) }, "carrying the claimant's close failure, once, along that chain")
        assertTrue(causeChain(wrapper.exceptionOrNull(), failure), "and the row sees that failure as a real one: ${wrapper.exceptionOrNull()}")
        assertEquals(emptyList(), rig.tracks.labels())
        assertTrue(rig.completion.calls.isEmpty())
    }

    // -- a terminal delivered by a different drain owner -------------------------------------------

    /** The camera's thread holds the drain in readiness while the coordinator publishes and claims Completed inline. */
    private suspend fun kotlinx.coroutines.test.TestScope.foreignDrainRun(terminalFailure: Throwable?): ForeignDrain {
        val camera = CameraLater()
        var listener: HeldDeliveries? = null
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")), acquirer = camera, listen = { events -> HeldDeliveries(events, terminalFailure).also { listener = it } })
        val held = checkNotNull(listener)
        val cameraThread = CameraThread(camera)
        val seen = ForeignDrain(rig, held, cameraThread)
        seen.wrapper =
            runCatching {
                withRun({ rig.run() }) { owner ->
                    try {
                        runCurrent()
                        stillLive(owner)
                        cameraThread.start()
                        assertTrue(held.readyEntered.await(BOUND_SECONDS, TimeUnit.SECONDS), "the camera's thread is delivering the readiness")
                        runCurrent()
                        deliver(rig.video, 0, VIDEO_ANNEXB.copyOf(), 33_333L)
                        runCurrent()
                        deliverEnd(rig.video, 1)
                        deliverEnd(rig.audio, 0)
                        runCurrent()
                        seen.claimedWhileHeld = rig.session.termination
                        seen.terminalWhileHeld = rig.events.terminal.isCompleted
                        seen.ownerDoneWhileHeld = owner.job.isCompleted
                        seen.completionWhileHeld = rig.completion.calls.toList()
                    } finally {
                        held.readyRelease.countDown()
                        cameraThread.join()
                    }
                    runCurrent()
                    seen.outcome = settled(owner)
                }
            }
        return seen
    }

    private class ForeignDrain(val rig: Rig, val held: HeldDeliveries, val camera: CameraThread) {
        var claimedWhileHeld: CaptureTermination? = null
        var terminalWhileHeld = true
        var ownerDoneWhileHeld = true
        var completionWhileHeld: List<String> = emptyList()
        var outcome: Result<BroadcastOutcome>? = null
        var wrapper: Result<Unit> = Result.success(Unit)

        /** The drain was owned by the camera's thread, and the coordinator's own claim had to leave the terminal to it. */
        fun assertForeignDrain() {
            assertEquals(CaptureTermination.Completed, claimedWhileHeld, "the coordinator claimed Completed while the drain was held")
            assertFalse(terminalWhileHeld, "the terminal was queued behind the held readiness, undelivered")
            assertFalse(ownerDoneWhileHeld, "the owner had not settled while the terminal was undelivered")
            assertEquals(emptyList(), completionWhileHeld, "no sender completion while the terminal was undelivered")
            assertTrue(camera.joined, "the camera's thread was joined")
            val readyOn = checkNotNull(held.readyThread.get())
            assertSame(readyOn, held.terminalThread.get(), "the terminal was delivered by the camera's thread, the drain owner")
        }
    }

    @Test
    fun `r5 a terminal delivered by another drain owner cannot let Published or sender completion conceal its listener failure`() = runTest {
        val failure = IllegalStateException("the terminal listener failed")
        val seen = foreignDrainRun(failure)
        seen.assertForeignDrain()
        assertTrue(causeChain(seen.camera.saw, failure), "the drain owner's own call still reports the listener failure")
        val raised = checkNotNull(seen.outcome).exceptionOrNull()
        assertTrue(causeChain(raised, failure), "the owner's result carries the terminal listener failure: ${checkNotNull(seen.outcome)}")
        assertEquals(emptyList(), seen.rig.completion.calls, "the sender was never told the broadcast completed")
        assertEquals(listOf("add:video", "add:audio", "write:v[11b@MediaMicros(33333)]", "end:v", "end:a", "remove:v", "remove:a"), seen.rig.tracks.labels(), "a publication that is not accepted retires both handles")
        assertTrue(seen.rig.owner.isDecided)
    }

    @Test
    fun `r6 control - the same foreign drain with a clean listener publishes, after the terminal is delivered`() = runTest {
        val seen = foreignDrainRun(null)
        seen.assertForeignDrain()
        assertNull(seen.camera.saw)
        seen.wrapper.getOrThrow()
        assertIs<BroadcastOutcome.Published>(checkNotNull(seen.outcome).getOrThrow())
        assertEquals(listOf("drain", "complete", "drain"), seen.rig.completion.calls)
        assertEquals(listOf("add:video", "add:audio", "write:v[11b@MediaMicros(33333)]", "end:v", "end:a"), seen.rig.tracks.labels())
        assertTrue(seen.rig.owner.isDecided)
    }

    /**
     * A drain owner that finishes AFTER the claim but BEFORE the claimant's
     * closes must not retire the ending: the claimant's facts are not in yet.
     */
    @Test
    fun `r10 a drain owner finishing while the claimant still closes does not retire the ending`() = runTest {
        val camera = CameraLater()
        var listener: HeldDeliveries? = null
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")), acquirer = camera, listen = { events -> HeldDeliveries(events, null).also { listener = it } })
        val held = checkNotNull(listener)
        val cameraThread = CameraThread(camera)
        val failure = IllegalStateException("the claimant's close failed after the real handoff closed")
        val claim = HeldClaim(rig, videoFailure = failure)
        var drainDoneFirst = false
        var heldOwnerDone = true
        var why: Throwable? = null
        var run: RunOwner? = null
        var outcome: Result<BroadcastOutcome>? = null
        val wrapper =
            runCatching {
                withRun({ rig.run() }) { owner ->
                    try {
                        runCurrent()
                        stillLive(owner)
                        cameraThread.start()
                        assertTrue(held.readyEntered.await(BOUND_SECONDS, TimeUnit.SECONDS), "the camera's thread is delivering the readiness")
                        runCurrent()
                        claim.open()
                        held.readyRelease.countDown()
                        cameraThread.join()
                        drainDoneFirst = cameraThread.joined && !claim.joined
                        run = owner
                        why = owner.cancel("the row stops the broadcast")
                        runCurrent()
                        heldOwnerDone = owner.job.isCompleted
                    } finally {
                        held.readyRelease.countDown()
                        claim.rescue()
                        cameraThread.join()
                    }
                    runCurrent()
                    outcome = settled(owner)
                }
            }
        assertTrue(drainDoneFirst, "the drain owner finished while the claimant was still held in its close")
        assertNull(cameraThread.saw)
        assertFalse(heldOwnerDone, "the owner had not settled while the claimant still held its close")
        assertTrue(claim.joined)
        val raised = checkNotNull(outcome).exceptionOrNull()
        assertTrue(checkNotNull(run).issued(checkNotNull(raised)) && causeChain(raised, checkNotNull(why)), "the owner's own cancellation, or its reconstruction: $raised")
        assertEquals(1, generateSequence(raised) { it.cause }.sumOf { it.suppressedCount(failure) }, "carrying the claimant's close failure, once")
        assertTrue(causeChain(wrapper.exceptionOrNull(), failure), "the row sees that failure as a real one")
        assertEquals(listOf("add:video", "add:audio", "remove:v", "remove:a"), rig.tracks.labels(), "both committed handles retired, nothing written")
        assertTrue(rig.completion.calls.isEmpty())
    }

    // -- a start that throws -----------------------------------------------------------------------

    /** The owner's own result for one start that throws, sampled BEFORE any rescue, then settled. */
    private class Started(val premature: Boolean, val result: Result<BroadcastOutcome>?)

    private suspend fun kotlinx.coroutines.test.TestScope.throwingStart(rig: Rig, rescue: () -> Unit, sample: () -> Unit = {}): Started {
        var result: Result<BroadcastOutcome>? = null
        val owner = launch { result = runCatching { rig.run() } }
        var premature = true
        try {
            runCurrent()
            premature = owner.isCompleted
            sample()
        } finally {
            rescue()
        }
        runCurrent()
        withTimeoutOrNull(BOUNDED) { owner.join() } ?: throw AssertionError("the owner did not settle even after the rescue")
        return Started(premature, result)
    }

    @Test
    fun `s1 a start that throws after another thread claimed waits for that claimant and carries its close failure`() = runTest {
        val startup = IllegalStateException("the camera submission threw where it stood")
        val failure = IllegalStateException("the claimant's close failed after the real handoff closed")
        lateinit var claim: HeldClaim
        val rig = Rig(acquirer = ThrowingBegin({ claim.open() }, startup))
        claim = HeldClaim(rig, videoFailure = failure)
        val started = throwingStart(rig, rescue = { claim.rescue() })
        assertFalse(started.premature, "the owner had not returned while the foreign claimant still held its close")
        assertTrue(claim.joined)
        assertSame(failure, claim.claimantSaw, "the claimant's own call still reports its cleanup failure")
        val raised = checkNotNull(started.result).exceptionOrNull()
        assertSame(startup, raised, "the start's own failure is what leaves")
        assertEquals(1, startup.suppressedCount(failure), "carrying the claimant's close failure, once")
        assertEquals(listOf<Throwable>(failure), rig.session.retirement().cleanup)
        assertTrue(rig.video.handoff.ended && rig.audio.handoff.ended)
        assertEquals(0, rig.session.stops, "a start that threw is not stopped by this coordinator")
        assertEquals(emptyList(), rig.tracks.labels())
    }

    @Test
    fun `s2 a start that claims inline while another thread owns the delivery waits for that delivery and carries its failure`() = runTest {
        val startup = IllegalStateException("the camera submission threw where it stood")
        val terminalFailure = IllegalStateException("the terminal listener failed")
        lateinit var listener: ScriptedDeliveries
        lateinit var rig: Rig
        val discarder = OwnedWorker("late-discard") { rig.forward.onDiscarded(GENERATION, MediaKind.Audio, 1) }
        rig =
            Rig(
                acquirer = ThrowingBegin({
                    discarder.start()
                    check(listener.discardEntered.await(BOUND_SECONDS, TimeUnit.SECONDS)) { "the other thread never took the delivery" }
                }, startup),
                listen = { events -> ScriptedDeliveries(events).also { it.holdDiscard = true; it.terminalFailure = terminalFailure; listener = it } },
            )
        var claimedWhileHeld: CaptureTermination? = null
        var terminalWhileHeld = true
        val started =
            throwingStart(rig, rescue = { listener.rescue(); discarder.join() }) {
                claimedWhileHeld = rig.session.termination
                terminalWhileHeld = rig.events.terminal.isCompleted
            }
        assertIs<CaptureTermination.Cancelled>(claimedWhileHeld, "the startup claimed its own ending inline")
        assertFalse(terminalWhileHeld, "its terminal was queued behind the other thread's held delivery")
        assertFalse(started.premature, "the owner had not returned while the other thread still owed the terminal")
        assertTrue(discarder.joined)
        assertSame(terminalFailure, discarder.saw, "the delivering thread's own call still reports the listener failure")
        val raised = checkNotNull(started.result).exceptionOrNull()
        assertSame(startup, raised)
        assertEquals(1, startup.suppressedCount(terminalFailure), "carrying the terminal listener failure, once")
        assertEquals(listOf<Throwable>(terminalFailure), rig.session.retirement().cleanup)
        assertEquals(0, rig.session.stops)
    }

    @Test
    fun `s3 control - a start that throws before claiming anything waits for nothing`() = runTest {
        val raise = IllegalStateException("the permission service failed")
        val rig = Rig()
        val flow = object : PermissionFlow by Granted(GENERATION) {
            override fun readiness(): CaptureReadiness = throw raise
        }
        val raised = runCatching { rig.broadcast().run(flow, CaptureCapability.Available, CaptureCapability.Available) }.exceptionOrNull()
        assertSame(raise, raised)
        assertEquals(emptyList(), raise.suppressed.toList())
        assertNull(rig.session.termination, "nothing was claimed")
        assertEquals(0, rig.session.stops)
    }

    // -- the delivering thread's caller-facing failure ---------------------------------------------

    /**
     * The camera's thread owns the delivery and attempts two failing
     * notifications; the claimant then retires the ending in the window after
     * that thread gives up the drain and before it lets its own failure
     * escape. What that thread throws must already be final by then.
     */
    @Test
    fun `d1 a delivering thread's failure graph is final before anyone can retire the ending`() = runTest {
        val readyFailure = IllegalStateException("the readiness listener failed")
        val discardFailure = IllegalStateException("the discard listener failed")
        val camera = CameraLater()
        val cameraThread = CameraThread(camera)
        val atBoundary = CountDownLatch(1)
        val boundaryRelease = CountDownLatch(1)
        val boundary =
            object : StartupSchedule {
                private var held = false

                override fun afterDeliveryReleased() {
                    if (Thread.currentThread() !== cameraThread.thread || held) return
                    held = true
                    atBoundary.countDown()
                    check(boundaryRelease.await(BOUND_SECONDS, TimeUnit.SECONDS)) { "the delivery boundary was never released" }
                }
            }
        lateinit var listener: ScriptedDeliveries
        val rig =
            Rig(
                ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")),
                acquirer = camera,
                listen = { events -> ScriptedDeliveries(events).also { it.holdReady = true; it.readyFailure = readyFailure; it.discardFailure = discardFailure; listener = it } },
                schedule = boundary,
            )
        val audioEntered = CountDownLatch(1)
        val audioRelease = CountDownLatch(1)
        rig.audioEntry.beforeClose = {
            audioEntered.countDown()
            check(audioRelease.await(BOUND_SECONDS, TimeUnit.SECONDS)) { "the held audio close was never released" }
        }
        val fault = CaptureFault.Codec(MediaKind.Video, "a fault from outside the handoff")
        val claimant = OwnedWorker("held-claim-worker") { rig.forward.onFault(GENERATION, fault) }
        val video = CompletableDeferred(validatedVideo())
        val audio = CompletableDeferred<CompleteTrackFormat>()
        val scope = this
        var frozenInWindow = false
        var ownerDoneInWindow = false
        var countInWindow = -1
        var outcome: Result<BroadcastOutcome>? = null
        val wrapper =
            runCatching {
                withRun({ rig.run(rig.formatPublication(FormatSources(video, audio), BUDGET)) }) { owner ->
                    try {
                        runCurrent()
                        stillLive(owner)
                        cameraThread.start()
                        assertTrue(listener.readyEntered.await(BOUND_SECONDS, TimeUnit.SECONDS), "the camera's thread is delivering the readiness")
                        runCurrent()
                        deliver(rig.video, 0, VIDEO_ANNEXB.copyOf(), 33_333L)
                        claimant.start()
                        assertTrue(audioEntered.await(BOUND_SECONDS, TimeUnit.SECONDS), "the claimant closed video and is held in its audio close")
                        listener.releaseReady()
                        assertTrue(atBoundary.await(BOUND_SECONDS, TimeUnit.SECONDS), "the camera's thread attempted both deliveries and gave up the drain")
                        audioRelease.countDown()
                        claimant.join()
                        val frozen = scope.async { rig.session.retirement() }
                        runCurrent()
                        frozenInWindow = frozen.isCompleted
                        ownerDoneInWindow = owner.job.isCompleted
                        countInWindow = readyFailure.suppressedCount(discardFailure)
                    } finally {
                        listener.rescue()
                        audioRelease.countDown()
                        boundaryRelease.countDown()
                        claimant.join()
                        cameraThread.join()
                    }
                    runCurrent()
                    outcome = settled(owner)
                }
            }
        assertTrue(claimant.joined && cameraThread.joined)
        assertNull(claimant.saw, "the claimant's own closes were clean")
        assertTrue(frozenInWindow, "the claimant retired the ending while the camera's thread was held after giving up the drain")
        assertTrue(ownerDoneInWindow, "and the owner consumed that receipt in the same window")
        assertEquals(1, countInWindow, "the owner's result carried the second failure once")
        assertSame(readyFailure, cameraThread.saw, "the camera's thread lets its first delivery failure escape")
        assertEquals(1, readyFailure.suppressedCount(discardFailure), "and nothing was appended to that graph after the ending retired")
        val receipt = rig.session.retirement()
        assertEquals(listOf<Throwable>(readyFailure, discardFailure), receipt.cleanup)
        assertSame(receipt, rig.session.retirement(), "a repeated await observes the same frozen receipt")
        val raised = checkNotNull(outcome).exceptionOrNull()
        assertTrue(causeChain(raised, readyFailure), "the owner's result is the first delivery failure: $raised")
        assertTrue(causeChain(wrapper.exceptionOrNull(), readyFailure))
        assertEquals(listOf(Discard(GENERATION, MediaKind.Video, 1)), rig.forward.discards())
        assertEquals(emptyList(), rig.tracks.labels())
    }

    /**
     * A delivery owner that has given up the drain after the ending's last
     * notification must not take it back for a notification queued after the
     * ending retired: its failure graph is already the receipt's. The late
     * notification is delivered, once, by the caller that queued it.
     */
    @Test
    fun `e1 a delivery owner never takes the drain back after releasing it, so a late failure cannot reach the retired graph`() = runTest {
        val readyFailure = IllegalStateException("the readiness listener failed")
        val lateFailure = IllegalStateException("the late audio discard listener failed")
        val camera = CameraLater()
        val cameraThread = CameraThread(camera)
        val lateThread = AtomicReference<Thread?>()
        val atRelease = CountDownLatch(1)
        val releaseHeld = CountDownLatch(1)
        val atQueued = CountDownLatch(1)
        val queuedHeld = CountDownLatch(1)
        val boundary =
            object : StartupSchedule {
                @Volatile private var releaseTaken = false

                @Volatile private var queuedTaken = false

                override fun afterDrainReleased() {
                    if (Thread.currentThread() !== cameraThread.thread || releaseTaken) return
                    releaseTaken = true
                    atRelease.countDown()
                    check(releaseHeld.await(BOUND_SECONDS, TimeUnit.SECONDS)) { "the drain release boundary was never released" }
                }

                override fun afterDiscardQueued() {
                    if (Thread.currentThread() !== lateThread.get() || queuedTaken) return
                    queuedTaken = true
                    atQueued.countDown()
                    check(queuedHeld.await(BOUND_SECONDS, TimeUnit.SECONDS)) { "the queued discard boundary was never released" }
                }
            }
        lateinit var listener: ScriptedDeliveries
        val rig =
            Rig(
                ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")),
                acquirer = camera,
                listen = { events ->
                    ScriptedDeliveries(events).also {
                        it.holdReady = true
                        it.readyFailure = readyFailure
                        it.discardFailure = lateFailure
                        it.discardFailureKind = MediaKind.Audio
                        listener = it
                    }
                },
                schedule = boundary,
            )
        val audioEntered = CountDownLatch(1)
        val audioRelease = CountDownLatch(1)
        rig.audioEntry.beforeClose = {
            audioEntered.countDown()
            check(audioRelease.await(BOUND_SECONDS, TimeUnit.SECONDS)) { "the held audio close was never released" }
        }
        val claimant = OwnedWorker("held-claim-worker") { rig.forward.onFault(GENERATION, CaptureFault.Codec(MediaKind.Video, "a fault from outside the handoff")) }
        val late = OwnedWorker("late-audio-discard") {
            lateThread.set(Thread.currentThread())
            rig.forward.onDiscarded(GENERATION, MediaKind.Audio, 3)
        }
        val video = CompletableDeferred(validatedVideo())
        val audio = CompletableDeferred<CompleteTrackFormat>()
        val scope = this
        var frozenBeforeLate: RetirementReceipt? = null
        var ownerDoneBeforeLate = false
        var outcome: Result<BroadcastOutcome>? = null
        val wrapper =
            runCatching {
                withRun({ rig.run(rig.formatPublication(FormatSources(video, audio), BUDGET)) }) { owner ->
                    try {
                        runCurrent()
                        stillLive(owner)
                        cameraThread.start()
                        assertTrue(listener.readyEntered.await(BOUND_SECONDS, TimeUnit.SECONDS), "the camera's thread is delivering the readiness")
                        runCurrent()
                        deliver(rig.video, 0, VIDEO_ANNEXB.copyOf(), 33_333L)
                        claimant.start()
                        assertTrue(audioEntered.await(BOUND_SECONDS, TimeUnit.SECONDS), "the claimant closed video and is held in its audio close")
                        listener.releaseReady()
                        assertTrue(atRelease.await(BOUND_SECONDS, TimeUnit.SECONDS), "the camera's thread delivered readiness and the video discard, and gave up the drain")
                        audioRelease.countDown()
                        claimant.join()
                        val frozen = scope.async { rig.session.retirement() }
                        runCurrent()
                        frozenBeforeLate = if (frozen.isCompleted) frozen.getCompleted() else null
                        ownerDoneBeforeLate = owner.job.isCompleted
                        late.start()
                        assertTrue(atQueued.await(BOUND_SECONDS, TimeUnit.SECONDS), "the late audio discard was admitted and queued, its caller not yet delivering")
                        releaseHeld.countDown()
                        cameraThread.join()
                        queuedHeld.countDown()
                        late.join()
                    } finally {
                        listener.rescue()
                        audioRelease.countDown()
                        releaseHeld.countDown()
                        queuedHeld.countDown()
                        claimant.join()
                        cameraThread.join()
                        late.join()
                    }
                    runCurrent()
                    outcome = settled(owner)
                }
            }
        assertTrue(claimant.joined && cameraThread.joined && late.joined)
        val receipt = checkNotNull(frozenBeforeLate) { "the receipt was frozen before the late witness" }
        assertTrue(ownerDoneBeforeLate, "the owner had consumed it before the late witness")
        assertEquals(listOf<Throwable>(readyFailure), receipt.cleanup)
        assertSame(readyFailure, cameraThread.saw, "the camera's thread lets its own delivery failure escape")
        assertEquals(emptyList(), readyFailure.suppressed.toList(), "the retired graph is unchanged after every worker joined")
        assertSame(lateFailure, late.saw, "the late failure is its own delivering caller's, under the late-delivery contract")
        assertEquals(listOf(MediaKind.Video, MediaKind.Audio), listener.discarded(), "the late discard was delivered exactly once, nothing stranded")
        rig.forward.onDiscarded(GENERATION, MediaKind.Audio, 3)
        assertEquals(listOf(MediaKind.Video, MediaKind.Audio), listener.discarded(), "a duplicate witness for that kind is refused")
        assertSame(receipt, rig.session.retirement())
        assertEquals(listOf<Throwable>(readyFailure), receipt.cleanup)
        assertTrue(causeChain(checkNotNull(outcome).exceptionOrNull(), readyFailure))
        assertTrue(causeChain(wrapper.exceptionOrNull(), readyFailure))
        assertEquals(emptyList(), rig.tracks.labels())
    }

    /**
     * An admitted callback's cause is recorded -- and claims the ending --
     * while another thread holds the notification drain: the session's
     * authority has it at once, and the receipt and the owner still wait for
     * the held delivery. Nothing depends on which notification a select sees.
     */
    @Test
    fun `k7 a cause recorded behind a held notification drain is authoritative at once, and retirement still waits for the delivery`() = runTest {
        val claiming = IllegalStateException("the video callback raised")
        val camera = CameraLater()
        val cameraThread = CameraThread(camera)
        lateinit var listener: ScriptedDeliveries
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")), acquirer = camera, listen = { events -> ScriptedDeliveries(events).also { it.holdReady = true; listener = it } })
        val video = rig.gate.bindFormatHolder(MediaKind.Video)
        val reservations = RowReservations(rig.gate)
        val scope = this
        var authorityWhileHeld: FormatInventory? = null
        var retiredWhileHeld = true
        var ownerDoneWhileHeld = true
        var terminalWhileHeld = true
        var outcome: Result<BroadcastOutcome>? = null
        runCatching {
            withRun({ rig.run(rig.formatPublication(FormatSources(CompletableDeferred(validatedVideo()), CompletableDeferred()), BUDGET)) }) { owner ->
                try {
                    reservations.owning {
                    runCurrent()
                    stillLive(owner)
                    cameraThread.start()
                    assertTrue(listener.readyEntered.await(BOUND_SECONDS, TimeUnit.SECONDS), "the camera's thread holds the delivery")
                    runCurrent()
                    val reservation = reservations.admit(video)
                    assertIs<FormatRelease.Released>(reservations.release(reservation, claiming))
                    val receipt = scope.async { rig.session.retirement() }
                    runCurrent()
                    authorityWhileHeld = rig.gate.formatInventory()
                    retiredWhileHeld = receipt.isCompleted
                    ownerDoneWhileHeld = owner.job.isCompleted
                    terminalWhileHeld = rig.events.terminal.isCompleted
                    }
                } finally {
                    listener.rescue()
                    cameraThread.join()
                }
                runCurrent()
                outcome = settled(owner)
            }
        }
        assertEquals(FormatInventory(emptySet(), listOf(claiming), CaptureTermination.Failed(CaptureFault.Unexpected(MediaKind.Video, claiming))), authorityWhileHeld, "recorded and claimed at once")
        assertFalse(terminalWhileHeld, "the terminal was still queued behind the held delivery")
        assertFalse(retiredWhileHeld, "so the ending had not retired")
        assertFalse(ownerDoneWhileHeld, "and the owner had not settled")
        assertTrue(cameraThread.joined)
        assertTrue(causeChain(checkNotNull(outcome).exceptionOrNull(), claiming), "the cause is the owner's failure: $outcome")
        assertEquals(listOf<Throwable>(claiming), rig.session.retirement().causes)
        assertEquals(emptyList(), rig.tracks.labels())
    }

    // -- controls ----------------------------------------------------------------------------------

    @Test
    fun `r7 a start refused before anything is acquired claims nothing and waits for nothing`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val outcome = rig.broadcast().run(Granted(GENERATION), CaptureCapability.Unavailable("no camera"), CaptureCapability.Available)
        assertEquals(BroadcastOutcome.Unfinished("the capture session refused to start: no camera", null, null), outcome)
        assertNull(rig.session.termination, "nothing was claimed")
        assertEquals(0, rig.session.stops, "a refused start is not stopped")
        assertEquals(emptyList(), rig.tracks.labels())
    }

    @Test
    fun `r8 a discard witnessed after the owner settled is still admitted and delivered once, and changes nothing the owner returned`() = runTest {
        var listener: HeldDeliveries? = null
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")), listen = { events -> HeldDeliveries(events, null, holdReadiness = false).also { listener = it } })
        val counted = checkNotNull(listener)
        val claim = HeldClaim(rig)
        val video = CompletableDeferred(validatedVideo())
        val audio = CompletableDeferred<CompleteTrackFormat>()
        var outcome: Result<BroadcastOutcome>? = null
        withRun({ rig.run(rig.formatPublication(FormatSources(video, audio), BUDGET)) }) { owner ->
            try {
                runCurrent()
                claim.open()
            } finally {
                claim.rescue()
            }
            runCurrent()
            outcome = settled(owner)
        }
        val settledWith = checkNotNull(outcome).getOrThrow()
        assertIs<BroadcastOutcome.Unfinished>(settledWith)
        assertEquals(0, counted.discards.get(), "nothing was queued when the claim closed its handoffs")
        val frozen = rig.session.retirement()
        assertEquals(emptyList(), frozen.cleanup)
        // A genuine late witness, through the same route the handoffs report on.
        rig.forward.onDiscarded(GENERATION, MediaKind.Audio, 2)
        assertEquals(1, counted.discards.get(), "the late witness is admitted and delivered once")
        rig.forward.onDiscarded(GENERATION, MediaKind.Audio, 2)
        assertEquals(1, counted.discards.get(), "a duplicate for that kind is not")
        assertEquals(settledWith, checkNotNull(outcome).getOrThrow(), "the settled result is unchanged")
        assertSame(frozen, rig.session.retirement(), "the receipt was frozen before the late witness")
        assertEquals(emptyList(), frozen.cleanup, "and the late witness did not reach it")
    }

    @Test
    fun `r9 a waiter resumed inline by the claimant finds the receipt final and the session lock free`() = runTest {
        val rig = Rig()
        val failure = IllegalStateException("the claimant's close failed after the real handoff closed")
        rig.videoEntry.beforeClose = {
            rig.video.handoff.close(CloseMode.Failed)
            throw failure
        }
        val fault = CaptureFault.Codec(MediaKind.Video, "a fault on the claiming thread")
        var insideClaim = false
        var resumedInside: Boolean? = null
        var seenInline: RetirementReceipt? = null
        var cleanupInline: List<Throwable>? = null
        var closedInline = false
        var deliveredInline = false
        var lockFreeInline = false
        val waiter =
            launch(Dispatchers.Unconfined) {
                val receipt = rig.session.retirement()
                resumedInside = insideClaim
                seenInline = receipt
                cleanupInline = receipt.cleanup.toList()
                closedInline = rig.video.handoff.ended && rig.audio.handoff.ended
                deliveredInline = rig.events.terminal.isCompleted
                // Another thread must be able to take the session's lock while this waiter runs.
                val reader = AtomicReference<CaptureTermination?>()
                val probe = Thread({ reader.set(rig.session.termination) }, "lock-probe").apply { start() }
                probe.join(BOUND_SECONDS * 1_000L)
                lockFreeInline = !probe.isAlive && reader.get() == CaptureTermination.Failed(fault)
            }
        assertFalse(waiter.isCompleted, "the waiter is suspended before the claim")
        insideClaim = true
        val claimantSaw = runCatching { rig.forward.onFault(GENERATION, fault) }.exceptionOrNull()
        insideClaim = false
        assertEquals(true, resumedInside, "the waiter resumed inline, inside the claimant's own call")
        assertTrue(waiter.isCompleted)
        assertSame(failure, claimantSaw, "the claimant still raises its cleanup failure after signalling")
        val receipt = checkNotNull(seenInline)
        assertEquals(CaptureTermination.Failed(fault), receipt.termination)
        assertEquals(listOf<Throwable>(failure), cleanupInline, "every fact was in place when the waiter ran")
        assertTrue(closedInline, "both closes had finished")
        assertTrue(deliveredInline, "the terminal had been delivered")
        assertTrue(lockFreeInline, "the session lock was not held across the signal")
        assertSame(receipt, rig.session.retirement(), "a later await observes the same receipt")
        assertEquals(listOf<Throwable>(failure), receipt.cleanup, "unchanged after the claimant returned")
    }

    private companion object {
        val BUDGET = 5.seconds
        const val BOUND_SECONDS = 10L
    }
}
