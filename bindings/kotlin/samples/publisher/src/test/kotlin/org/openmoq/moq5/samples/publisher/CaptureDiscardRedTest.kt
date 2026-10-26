package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * The late discard witness, from the real handoff to a session-owned record.
 *
 * The `r*` rows drive the REAL [PlatformCaptureSession] over REAL
 * [BoundedCaptureHandoff] tracks, with only the platform scripted: codec
 * buffers, acquisition and the listener that records what the session says.
 * The recording listener is an ORACLE -- nothing in those rows calls it
 * directly, because the session's own route is what they measure.
 *
 * `k1` and `r9` are the exceptions by design: they exercise the recipient and
 * the recording edge on their own, where no session is involved.
 */
class CaptureDiscardRedTest {

    /** Everything the session said, in the order it said it. */
    private sealed interface Said {
        data class Ready(val generation: Long, val ready: CaptureReady) : Said

        data class Terminated(val generation: Long, val termination: CaptureTermination) : Said

        data class Discarded(val generation: Long, val kind: MediaKind, val outputs: Int) : Said
    }

    /**
     * Records what the session says. [insideDiscard] lets a row be reentrant
     * or fail from inside the callback, the way application code can.
     */
    private class Recording(
        private val insideDiscard: ((Said.Discarded) -> Unit)? = null,
    ) : CaptureSessionListener {
        private val said = mutableListOf<Said>()

        /** Entry and exit of every callback, in order, so nesting is visible. */
        private val frames = mutableListOf<String>()
        private var depth = 0
        private var deepest = 0

        @Synchronized
        private fun add(one: Said) {
            said.add(one)
        }

        @Synchronized
        private fun enter(what: String) {
            depth++
            if (depth > deepest) deepest = depth
            frames.add("enter:$what")
        }

        @Synchronized
        private fun leave(what: String) {
            depth--
            frames.add("leave:$what")
        }

        override fun onReady(generation: Long, ready: CaptureReady) {
            enter("ready")
            try {
                add(Said.Ready(generation, ready))
            } finally {
                leave("ready")
            }
        }

        override fun onTerminated(generation: Long, termination: CaptureTermination) {
            enter("terminated")
            try {
                add(Said.Terminated(generation, termination))
            } finally {
                leave("terminated")
            }
        }

        override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) {
            val one = Said.Discarded(generation, kind, outputs)
            enter("discarded:$kind")
            try {
                add(one)
                insideDiscard?.invoke(one)
            } finally {
                leave("discarded:$kind")
            }
        }

        @Synchronized
        fun all(): List<Said> = said.toList()

        @Synchronized
        fun frames(): List<String> = frames.toList()

        @Synchronized
        fun deepest(): Int = deepest

        fun discards(): List<Said.Discarded> = all().filterIsInstance<Said.Discarded>()
    }

    private class NoAcquire : CaptureAcquirer {
        override val order: List<CaptureResource> = emptyList()

        override fun begin(ownership: CaptureOwnership, steps: AcquisitionSteps): AcquireOutcome.Failed? = null
    }

    private class Granted(override val generation: Long) : PermissionFlow {
        override fun stateOf(permission: CapturePermission) = PermissionState.Granted

        override fun request(permissions: Set<CapturePermission>) = emptySet<CapturePermission>()

        override fun onResult(generation: Long, permission: CapturePermission, facts: PermissionFacts) = PermissionOutcome.Ignored("granted")

        override fun retire(reason: String) = Unit

        override fun readiness(): CaptureReadiness = CaptureReadiness.Ready
    }

    /** Forwards each track's facts into the session it belongs to. */
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

    private class Track(val kind: MediaKind, sink: CaptureFaultSink, schedule: HandoffSchedule = HandoffSchedule.NONE) {
        val buffers = HostBuffers()
        val releases = HostReleases()
        val ledger = BoundedCaptureLedger(kind, GENERATION, capacity = 16)
        val handoff = BoundedCaptureHandoff(kind, GENERATION, buffers, releases, ledger, sink, CAPACITY, HOST_MAX_OUTPUT_BYTES, schedule = schedule)

        fun queue(index: Int, bytes: ByteArray, pts: Long): HandoffOutcome {
            buffers.put(index, ByteBuffer.wrap(bytes))
            assertEquals(ObserveOutcome.Recorded, ledger.observe(GENERATION, kind, CaptureObservation(pts, BASE_NANOS + pts * 1_000L)))
            return handoff.onOutput(GENERATION, CodecOutput(index, 0, bytes.size, pts, CaptureContract.FLAG_KEY_FRAME))
        }
    }

    private class Rig(
        val listener: Recording = Recording(),
        videoSchedule: HandoffSchedule = HandoffSchedule.NONE,
        /** When set, the session is built with the REAL recording edge over this record. */
        val record: CaptureDiscardRecord? = null,
    ) {
        private val forward = Forward()
        val video = Track(MediaKind.Video, forward, videoSchedule)
        val audio = Track(MediaKind.Audio, forward)
        val ownership = OrderedCaptureOwnership(GENERATION, HostDispatcher(), LateLog())

        /** The production edge when a record is owned; the oracle alone otherwise. */
        val edge: RecordingCaptureListener? = record?.let { RecordingCaptureListener(it, listener) }
        val session = PlatformCaptureSession(GENERATION, ownership, NoAcquire(), video.handoff, audio.handoff, edge ?: listener)

        init {
            forward.to = session
            check(session.start(Granted(GENERATION), CaptureCapability.Available, CaptureCapability.Available) == CaptureStart.Started)
        }
    }

    private fun bytes(tag: Int, size: Int) = ByteArray(size) { ((tag shl 4) + it).toByte() }

    // ---- the route ------------------------------------------------------------

    /**
     * A failed close throws away what each track had queued. The counts DIFFER
     * per track, so a swapped kind or a copied count cannot pass.
     */
    @Test
    fun r1_aFailedCloseWitnessesEachTracksOwnCount() {
        val rig = Rig()
        // Three video outputs and two audio ones, all admitted.
        repeat(3) { at -> assertEquals(HandoffOutcome.Queued, rig.video.queue(at, bytes(1, 6), 33_333L * (at + 1))) }
        repeat(2) { at -> assertEquals(HandoffOutcome.Queued, rig.audio.queue(at, bytes(2, 3), 21_333L * (at + 1))) }
        val primary = IllegalStateException("the user left")
        rig.session.stop(primary)
        assertEquals(
            listOf<Said>(
                Said.Ready(GENERATION, CaptureReady.Ready),
                Said.Discarded(GENERATION, MediaKind.Video, 3),
                Said.Discarded(GENERATION, MediaKind.Audio, 2),
                Said.Terminated(GENERATION, CaptureTermination.Cancelled("the user left")),
            ),
            rig.listener.all(),
            "the WHOLE inventory: each track's own count once, for this generation, then the ending",
        )
        assertEquals(1, rig.listener.deepest(), "and every callback was delivered on its own, never nested")
    }

    /**
     * An admitted callback is still inside the codec when the session ends.
     * The terminal arrives FIRST, and the real witness arrives behind it --
     * with no second terminal.
     */
    @Test
    fun r2_aWitnessThatSettlesAfterTheTerminalStillArrives() {
        val reached = HostGate("the transfer reached its commit")
        val release = HostGate("the row released the held transfer")
        val schedule =
            object : HandoffSchedule {
                override fun beforeCommit() {
                    reached.open()
                    release.await()
                }
            }
        val rig = Rig(videoSchedule = schedule)
        val worker = HostWorker("transfer") { rig.video.queue(0, bytes(1, 6), 33_333L) }
        withSettlement(listOf(reached, release), listOf(worker)) {
            worker.start()
            reached.await(worker)
            // The session ends while that output is still in flight.
            rig.session.stop(IllegalStateException("the user left"))
            val beforeWitness = rig.listener.all()
            assertTrue(beforeWitness.any { it is Said.Terminated }, "the terminal arrived first")
            assertEquals(emptyList(), beforeWitness.filterIsInstance<Said.Discarded>(), "and no witness yet")
            release.openFully()
            worker.join()
            assertEquals(
                listOf(Said.Discarded(GENERATION, MediaKind.Video, 1)),
                rig.listener.discards(),
                "the late witness arrives behind the terminal, exactly once",
            )
            assertEquals(1, rig.listener.all().count { it is Said.Terminated }, "and no second terminal was said")
        }
    }

    /** Controls: nothing discarded, and a completed close, are UNOBSERVED -- never zero. */
    @Test
    fun r3_nothingDiscardedIsUnobservedNotZero() {
        val empty = Rig()
        empty.session.stop(IllegalStateException("the user left"))
        assertEquals(emptyList(), empty.listener.discards(), "a failed close with nothing queued witnesses nothing")

        val completed = Rig()
        assertEquals(HandoffOutcome.Queued, completed.video.queue(0, bytes(1, 6), 33_333L))
        completed.session.stop(null)
        assertEquals(emptyList(), completed.listener.discards(), "and a completed close keeps its output instead of discarding it")
        // The absence above says UNOBSERVED. It is not a zero receipt and it
        // says nothing about whether any resource was physically disposed.
        val record = CaptureDiscardRecord(GENERATION)
        assertEquals(emptyMap(), record.snapshot(), "an unobserved kind is absent from the record")
    }

    /** Holds the session for callbacks that run before the row's own local is in scope. */
    private class Held {
        lateinit var session: CaptureSession
        lateinit var audio: CaptureHandoff
    }

    /**
     * A listener that really RE-ENTERS the session from inside a callback, and
     * a worker that reads the session WHILE that callback is running.
     *
     * The reentry is a second discard fact delivered from inside the first
     * one's callback: it must be serialized behind it, never nested. The
     * worker proves the callback is not holding the session's lock -- same
     * thread cannot, because JVM monitors are reentrant. It is preconstructed
     * and owned by `withSettlement`, so a worker that fails or never settles
     * fails this row rather than disappearing into an ignored join.
     */
    @Test
    fun r4_aReentrantListenerIsSerializedAndHoldsNoLock() {
        val readerDone = HostGate("a reader reached the session during the discard callback")
        val held = Held()
        val reentered = AtomicBoolean(false)
        // Preconstructed: owned before it is ever started.
        val reader =
            HostWorker("reader") {
                // A real read of a real session: both take its lock.
                held.session.video
                held.session.termination
                readerDone.open()
            }
        val rig =
            Rig(
                listener =
                    Recording(
                        insideDiscard = { one ->
                            if (one.kind == MediaKind.Video && reentered.compareAndSet(false, true)) {
                                reader.start()
                                readerDone.await(reader)
                                // ...and a SECOND fact arrives reentrantly.
                                held.session.onDiscarded(GENERATION, MediaKind.Audio, 2)
                            }
                        },
                    ),
            )
        held.session = rig.session
        repeat(3) { at -> assertEquals(HandoffOutcome.Queued, rig.video.queue(at, bytes(1, 6), 33_333L * (at + 1))) }
        withSettlement(listOf(readerDone), listOf(reader)) {
            rig.session.stop(IllegalStateException("the user left"))
            assertTrue(reentered.get(), "precondition -- the listener really re-entered the session")
            assertEquals(
                listOf(
                    Said.Discarded(GENERATION, MediaKind.Video, 3),
                    Said.Discarded(GENERATION, MediaKind.Audio, 2),
                ),
                rig.listener.discards(),
                "both facts were delivered, the reentrant one behind the first",
            )
            assertEquals(1, rig.listener.deepest(), "and NEVER nested: no callback ran inside another")
            val frames = rig.listener.frames()
            val firstExit = frames.indexOf("leave:discarded:Video")
            val secondEntry = frames.indexOf("enter:discarded:Audio")
            assertTrue(
                firstExit in 0 until secondEntry,
                "the first callback returned before the second began: $frames",
            )
        }
    }

    /**
     * A listener that throws from its discard callback: its own failure is
     * retained under the session's existing rules, the remaining affected
     * track is still told, the ending is still delivered, and nothing is
     * retried.
     */
    @Test
    fun r6_aThrowingDiscardListenerKeepsItsFailureAndTheRest() {
        for (raised in listOf<Throwable>(
            IllegalStateException("the listener threw"),
            AssertionError("the listener threw an Error"),
            kotlinx.coroutines.CancellationException("the listener cancelled itself"),
        )) {
            val attempts = java.util.concurrent.atomic.AtomicInteger(0)
            val rig =
                Rig(
                    listener =
                        Recording(
                            insideDiscard = { one ->
                                attempts.incrementAndGet()
                                if (one.kind == MediaKind.Video) throw raised
                            },
                        ),
                )
            repeat(3) { at -> assertEquals(HandoffOutcome.Queued, rig.video.queue(at, bytes(1, 6), 33_333L * (at + 1))) }
            repeat(2) { at -> assertEquals(HandoffOutcome.Queued, rig.audio.queue(at, bytes(2, 3), 21_333L * (at + 1))) }
            val primary = IllegalStateException("the user left")
            val returned = runCatching { rig.session.stop(primary) }
            assertTrue(returned.isSuccess, "$raised: the stop still returned: ${returned.exceptionOrNull()}")
            assertSame(primary, returned.getOrNull(), "$raised: with the caller's own primary")
            assertTrue(
                primary.suppressed.any { it === raised },
                "$raised: carrying the listener's actual failure: ${primary.suppressed.toList()}",
            )
            assertEquals(
                listOf(
                    Said.Discarded(GENERATION, MediaKind.Video, 3),
                    Said.Discarded(GENERATION, MediaKind.Audio, 2),
                ),
                rig.listener.discards(),
                "$raised: the other affected track was still told",
            )
            assertEquals(2, attempts.get(), "$raised: each attempted exactly once, never retried")
            assertEquals(
                listOf<Said>(Said.Terminated(GENERATION, CaptureTermination.Cancelled("the user left"))),
                rig.listener.all().filterIsInstance<Said.Terminated>(),
                "$raised: and the ending was still delivered, once",
            )
        }
    }

    /** A foreign generation's witness changes neither the route nor the record. */
    @Test
    fun r5_aForeignGenerationIsNotThisSessionsInventory() {
        val record = CaptureDiscardRecord(GENERATION)
        val rig = Rig(record = record)
        repeat(2) { at -> assertEquals(HandoffOutcome.Queued, rig.video.queue(at, bytes(1, 6), 33_333L * (at + 1))) }
        rig.session.stop(IllegalStateException("the user left"))
        val mine = rig.listener.discards()
        val recorded = record.snapshot()
        // A fact stamped for another generation, arriving on this session's sink.
        rig.session.onDiscarded(GENERATION + 1, MediaKind.Audio, 7)
        assertEquals(mine, rig.listener.discards(), "a foreign generation adds nothing to this session's inventory")
        assertEquals(recorded, record.snapshot(), "and nothing to the record either")
    }

    /**
     * The route into the SESSION-OWNED record, across the observation boundary.
     *
     * The session is built with the real recording edge; the oracle listener is
     * the observer behind it. One track discards at once, so the record is
     * PARTIAL while an admitted output is still in flight. The observer is then
     * retired -- as a UI or coordinator going away would retire it -- and the
     * held output settles: the late witness must still reach the record.
     */
    @Test
    fun r7_theLateWitnessReachesTheRecordAfterTheObserverRetires() {
        val reached = HostGate("the transfer reached its commit")
        val release = HostGate("the row released the held transfer")
        val schedule =
            object : HandoffSchedule {
                override fun beforeCommit() {
                    reached.open()
                    release.await()
                }
            }
        val record = CaptureDiscardRecord(GENERATION)
        val rig = Rig(videoSchedule = schedule, record = record)
        val edge = assertNotNull(rig.edge, "this row is built on the real recording edge")
        // Audio discards immediately; video's output is still in flight.
        repeat(2) { at -> assertEquals(HandoffOutcome.Queued, rig.audio.queue(at, bytes(2, 3), 21_333L * (at + 1))) }
        val worker = HostWorker("transfer") { rig.video.queue(0, bytes(1, 6), 33_333L) }
        withSettlement(listOf(reached, release), listOf(worker)) {
            worker.start()
            reached.await(worker)
            rig.session.stop(IllegalStateException("the user left"))
            assertEquals(
                mapOf(MediaKind.Audio to 2),
                record.snapshot(),
                "a PARTIAL record: audio observed, video still unobserved -- not zero",
            )
            // The observer goes away. The record does not.
            edge.retire()
            release.openFully()
            worker.join()
            assertEquals(
                mapOf(MediaKind.Audio to 2, MediaKind.Video to 1),
                record.snapshot(),
                "the late witness still reached the session-owned record after the observer retired",
            )
            assertEquals(
                listOf(Said.Discarded(GENERATION, MediaKind.Audio, 2)),
                rig.listener.discards(),
                "and the retired observer was told nothing further",
            )
        }
    }

    /**
     * Admission must be bounded where the queue is, not after it.
     *
     * While a real first witness is being delivered, an adversarial burst of
     * duplicates arrives at the SESSION's sink. A route that enqueues first and
     * filters at the listener would grow its notification queue by the whole
     * burst; a route that admits under its own lock queues none of them. The
     * queue is read directly, because a filtered callback count cannot tell
     * those two apart.
     */
    @Test
    fun r8_duplicatesDuringDeliveryNeverEnterTheNotificationQueue() {
        val record = CaptureDiscardRecord(GENERATION)
        val held = Held()
        val queuedDuringCallback = java.util.concurrent.atomic.AtomicInteger(-1)
        val burst = AtomicBoolean(false)
        val rig =
            Rig(
                listener =
                    Recording(
                        insideDiscard = { one ->
                            if (one.kind == MediaKind.Video && burst.compareAndSet(false, true)) {
                                // Adversarial: the same fact, again and again,
                                // while this very callback is still running.
                                repeat(64) { held.session.onDiscarded(GENERATION, MediaKind.Video, 3) }
                                // ...and facts that are invalid for other
                                // reasons, which must not take a later valid
                                // first fact's place either.
                                held.session.onDiscarded(GENERATION + 1, MediaKind.Audio, 4)
                                held.session.onDiscarded(GENERATION, MediaKind.Audio, 0)
                                held.session.onDiscarded(GENERATION, MediaKind.Audio, -1)
                                queuedDuringCallback.set(pendingNotifications(held.session))
                            }
                        },
                    ),
                record = record,
            )
        held.session = rig.session
        repeat(3) { at -> assertEquals(HandoffOutcome.Queued, rig.video.queue(at, bytes(1, 6), 33_333L * (at + 1))) }
        repeat(2) { at -> assertEquals(HandoffOutcome.Queued, rig.audio.queue(at, bytes(2, 3), 21_333L * (at + 1))) }
        rig.session.stop(IllegalStateException("the user left"))
        assertTrue(burst.get(), "precondition -- the burst really ran inside a live discard callback")
        assertEquals(
            0,
            queuedDuringCallback.get(),
            "not one duplicate or invalid fact was queued while the first was being delivered",
        )
        assertEquals(
            mapOf(MediaKind.Video to 3, MediaKind.Audio to 2),
            record.snapshot(),
            "and the later valid first fact for the other kind was still admitted and observed, " +
                "so neither the zero nor the negative reserved its slot",
        )
        assertEquals(
            listOf(
                Said.Discarded(GENERATION, MediaKind.Video, 3),
                Said.Discarded(GENERATION, MediaKind.Audio, 2),
            ),
            rig.listener.discards(),
            "each kind delivered exactly once",
        )
    }

    /**
     * A genuine FIRST witness for the other track, created reentrantly while
     * the first callback is still running: it is ADMITTED at once -- so a
     * duplicate of it queues nothing -- yet it waits in the existing queue and
     * is not OBSERVED until it is actually delivered.
     */
    @Test
    fun r10_anAdmittedWitnessWaitsInTheQueueAndIsNotYetObserved() {
        val record = CaptureDiscardRecord(GENERATION)
        val held = Held()
        val pendingDuringCallback = java.util.concurrent.atomic.AtomicInteger(-1)
        val audioSeenDuringCallback = java.util.concurrent.atomic.AtomicReference<Map<MediaKind, Int>>(emptyMap())
        val armed = AtomicBoolean(false)
        val rig =
            Rig(
                listener =
                    Recording(
                        insideDiscard = { one ->
                            if (one.kind == MediaKind.Video && armed.compareAndSet(false, true)) {
                                // A REAL first audio witness, produced from
                                // inside this callback by closing that track.
                                held.audio.close(CloseMode.Failed)
                                // ...and a duplicate of the very fact now
                                // waiting to be delivered.
                                held.session.onDiscarded(GENERATION, MediaKind.Audio, 2)
                                pendingDuringCallback.set(pendingNotifications(held.session))
                                audioSeenDuringCallback.set(record.snapshot())
                            }
                        },
                    ),
                record = record,
            )
        held.session = rig.session
        held.audio = rig.audio.handoff
        repeat(3) { at -> assertEquals(HandoffOutcome.Queued, rig.video.queue(at, bytes(1, 6), 33_333L * (at + 1))) }
        repeat(2) { at -> assertEquals(HandoffOutcome.Queued, rig.audio.queue(at, bytes(2, 3), 21_333L * (at + 1))) }
        rig.session.stop(IllegalStateException("the user left"))
        assertTrue(armed.get(), "precondition -- the audio track really was closed from inside the video callback")
        assertEquals(
            1,
            pendingDuringCallback.get(),
            "exactly ONE audio notification was waiting: admitted at enqueue, and its duplicate queued nothing",
        )
        assertEquals(
            mapOf(MediaKind.Video to 3),
            audioSeenDuringCallback.get(),
            "and it was not OBSERVED yet, because it had not been delivered",
        )
        assertEquals(
            mapOf(MediaKind.Video to 3, MediaKind.Audio to 2),
            record.snapshot(),
            "once delivered, exactly its declared count, once",
        )
        assertEquals(
            listOf(
                Said.Discarded(GENERATION, MediaKind.Video, 3),
                Said.Discarded(GENERATION, MediaKind.Audio, 2),
            ),
            rig.listener.discards(),
            "each kind delivered exactly once",
        )
    }

    /** Reads the session's own pending-notification count, for the queue-bound oracle only. */
    private fun pendingNotifications(session: CaptureSession): Int {
        val field = session.javaClass.getDeclaredField("outbox")
        field.isAccessible = true
        val outbox = field.get(session)
        val size = outbox.javaClass.methods.first { it.name == "size" && it.parameterCount == 0 }
        size.isAccessible = true
        return size.invoke(outbox) as Int
    }

    /**
     * CONTROL for what retirement actually is: a detachment, not a join.
     *
     * A callback already selected keeps running across `retire` and may finish
     * afterwards -- that is allowed and is measured here. What retirement
     * guarantees is only that NO NEW observer selection happens after it, while
     * the record keeps recording.
     */
    @Test
    fun r9_retirementDetachesWithoutJoiningAnInFlightCallback() {
        val inCallback = HostGate("the observer callback is running")
        val release = HostGate("the row released the in-flight callback")
        val finished = AtomicBoolean(false)
        val observer =
            Recording(
                insideDiscard = {
                    inCallback.open()
                    release.await()
                    finished.set(true)
                },
            )
        val record = CaptureDiscardRecord(GENERATION)
        val edge = RecordingCaptureListener(record, observer)
        val worker = HostWorker("delivery") { edge.onDiscarded(GENERATION, MediaKind.Video, 3) }
        withSettlement(listOf(inCallback, release), listOf(worker)) {
            worker.start()
            inCallback.await(worker)
            // The observer has ALREADY been selected and is running.
            edge.retire()
            edge.retire()
            assertTrue(!finished.get(), "the in-flight callback is still running across retire")
            release.openFully()
            worker.join()
            assertTrue(finished.get(), "and it was allowed to finish, because retire is not a join")
            assertEquals(mapOf(MediaKind.Video to 3), record.snapshot(), "its fact was recorded")
            // A later fact still reaches the record, and nobody is called.
            edge.onDiscarded(GENERATION, MediaKind.Audio, 2)
            assertEquals(
                mapOf(MediaKind.Video to 3, MediaKind.Audio to 2),
                record.snapshot(),
                "recording is independent of the observer",
            )
            assertEquals(
                listOf(Said.Discarded(GENERATION, MediaKind.Video, 3)),
                observer.discards(),
                "and no NEW observer selection happened after retirement",
            )
        }
    }

    // ---- the recipient, on its own ---------------------------------------------

    /**
     * The session-owned record itself: bounded, presence separate from value,
     * one count per kind, foreign generations and impossible counts refused.
     * This is the RECIPIENT, not the route.
     */
    @Test
    fun k1_theRecordIsBoundedAndPresenceIsNotValue() {
        val record = CaptureDiscardRecord(GENERATION)
        val empty = record.snapshot()
        assertTrue(record.record(GENERATION, MediaKind.Video, 3), "the first witness for a kind is recorded")
        val afterVideo = record.snapshot()
        assertTrue(!record.record(GENERATION, MediaKind.Video, 9), "a duplicate for that kind is refused, not accumulated")
        assertTrue(!record.record(GENERATION + 1, MediaKind.Audio, 4), "a foreign generation is refused")
        // A handoff only witnesses when it really threw output away, so these
        // are not facts it can produce -- and refusing them changes nothing.
        assertTrue(!record.record(GENERATION, MediaKind.Audio, 0), "a zero count is refused")
        assertTrue(!record.record(GENERATION, MediaKind.Audio, -1), "and a negative one is refused")
        assertEquals(mapOf(MediaKind.Video to 3), record.snapshot(), "audio is still UNOBSERVED after all of that")
        assertTrue(record.record(GENERATION, MediaKind.Audio, 2), "and a later legitimate first witness is still accepted")
        val afterAudio = record.snapshot()

        // Snapshots taken BEFORE genuine additions stay as they were.
        assertEquals(emptyMap(), empty, "the first snapshot was empty and stayed empty")
        assertEquals(mapOf(MediaKind.Video to 3), afterVideo, "the second saw only video and stayed that way")
        assertEquals(mapOf(MediaKind.Video to 3, MediaKind.Audio to 2), afterAudio)
    }

    private companion object {
        const val GENERATION = 1L
        const val CAPACITY = 8
        const val BASE_NANOS = 1_000_000_000L
    }
}
