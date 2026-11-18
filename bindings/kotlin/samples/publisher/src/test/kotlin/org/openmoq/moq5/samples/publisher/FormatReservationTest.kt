package org.openmoq.moq5.samples.publisher

import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Admitted format callbacks and the ending's causes, decided by the real
 * session under its own lock and consumed by the real coordinator.
 *
 * The holders that will call this boundary are not built yet: each row plays
 * a holder by calling the session's gate directly, on the row's own thread,
 * so what is pinned is the session and the coordinator, never a model of
 * them. These rows say nothing about the future holder's own validation or
 * slot delivery.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class FormatReservationTest {

    private fun causeChain(raised: Throwable?, expected: Throwable) = generateSequence(raised) { it.cause }.any { it === expected }

    private fun Throwable.suppressedCount(of: Throwable) = suppressed.count { it === of }

    // -- the boundary itself ---------------------------------------------------------------------

    @Test
    fun `k1 only the key a session minted reserves there, a second bind fails, and refusals change nothing`() = runTest {
        val rig = Rig()
        val other = Rig()
        val reservations = RowReservations(rig.gate)
        val theirs = RowReservations(other.gate)
        reservations.owning { theirs.owning {
        val video = rig.gate.bindFormatHolder(MediaKind.Video)
        rig.gate.bindFormatHolder(MediaKind.Audio)
        val first = reservations.admit(video)
        val before = rig.gate.formatInventory()
        assertEquals(FormatInventory(setOf(MediaKind.Video), emptyList(), null), before)

        assertFailsWith<IllegalStateException>("a second bind for a kind") { rig.gate.bindFormatHolder(MediaKind.Video) }
        assertSame(FormatAdmission.NotBound, rig.gate.reserveFormat(FormatHolderKey(MediaKind.Video)), "a key this session never minted")
        val foreign = other.gate.bindFormatHolder(MediaKind.Video)
        assertSame(FormatAdmission.NotBound, rig.gate.reserveFormat(foreign), "another session's key")
        val foreignReservation = theirs.admit(foreign)
        val raised = IllegalStateException("reported to the wrong session")
        assertEquals(FormatRelease.Refused(raised), rig.gate.releaseFormat(foreignReservation, raised), "another session's reservation")
        assertEquals(before, rig.gate.formatInventory(), "every refusal left the inventory exactly as it was")

        // The original key is still this session's after the refused rebind.
        assertEquals(FormatRelease.Released(null), reservations.release(first, null))
        reservations.admit(video)
        assertEquals(FormatInventory(setOf(MediaKind.Video), emptyList(), null), rig.gate.formatInventory())
        assertEquals(FormatInventory(setOf(MediaKind.Video), emptyList(), null), other.gate.formatInventory(), "and the other session's own reservation is untouched")
        } }
    }

    @Test
    fun `k2 a same-kind overlap counts nothing, the other kind is independent, and a stale release cannot touch a newer reservation`() = runTest {
        val rig = Rig()
        val reservations = RowReservations(rig.gate)
        reservations.owning {
        val video = rig.gate.bindFormatHolder(MediaKind.Video)
        val audio = rig.gate.bindFormatHolder(MediaKind.Audio)
        val first = reservations.admit(video)
        assertSame(FormatAdmission.Overlapping, rig.gate.reserveFormat(video))
        assertEquals(setOf(MediaKind.Video), rig.gate.formatInventory().active, "the overlap added nothing")
        val audioReservation = reservations.admit(audio)
        assertEquals(setOf(MediaKind.Video, MediaKind.Audio), rig.gate.formatInventory().active, "at most one per kind, two in all")

        assertEquals(FormatRelease.Released(null), reservations.release(first, null))
        val second = reservations.admit(video)
        val stale = IllegalStateException("reported by a callback that already released")
        val refused = assertIs<FormatRelease.Refused>(rig.gate.releaseFormat(first, stale), "a duplicate release of the old reservation")
        assertSame(stale, refused.cause, "its cause is handed back, not consumed")
        assertEquals(FormatInventory(setOf(MediaKind.Video, MediaKind.Audio), emptyList(), null), rig.gate.formatInventory(), "the newer reservation stands and nothing was recorded")

        assertEquals(FormatRelease.Released(null), reservations.release(second, null))
        assertEquals(FormatRelease.Released(null), reservations.release(audioReservation, null))
        assertEquals(FormatRelease.Refused(null), rig.gate.releaseFormat(second, null), "released once, never twice")
        assertEquals(FormatInventory(emptySet(), emptyList(), null), rig.gate.formatInventory())
        }
    }

    // -- the causes, through the real coordinator ------------------------------------------------

    /**
     * The owner waiting for its audio format; the row claims and releases
     * around it. Every driving step runs inside [reservations]' ownership, so
     * a callback the row admitted is released before [withRun] joins an owner
     * that is waiting for it.
     */
    private suspend fun TestScope.waitingOwner(rig: Rig, reservations: RowReservations = RowReservations(rig.gate), body: suspend (RunOwner) -> Unit): Result<BroadcastOutcome>? {
        var outcome: Result<BroadcastOutcome>? = null
        runCatching {
            withRun({ rig.run(rig.formatPublication(FormatSources(CompletableDeferred(validatedVideo()), CompletableDeferred()), BUDGET)) }) { owner ->
                reservations.owning {
                    runCurrent()
                    stillLive(owner)
                    body(owner)
                }
                runCurrent()
                outcome = settled(owner)
            }
        }
        return outcome
    }

    @Test
    fun `k3 with two callbacks admitted, the one that claims cannot settle the owner before the other reports, and a late Error is kept once`() = runTest {
        for (lateError in listOf<Throwable?>(AssertionError("the audio callback raised an Error"), null)) {
            val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
            val video = rig.gate.bindFormatHolder(MediaKind.Video)
            val audio = rig.gate.bindFormatHolder(MediaKind.Audio)
            val claiming = IllegalStateException("the video callback raised")
            var ownerDoneWhileHeld = true
            var retiredWhileHeld = true
            var inventoryWhileHeld: FormatInventory? = null
            val scope = this
            val reservations = RowReservations(rig.gate)
            val outcome =
                waitingOwner(rig, reservations) { owner ->
                    val videoReservation = reservations.admit(video)
                    val audioReservation = reservations.admit(audio)
                    val claimed = CaptureTermination.Failed(CaptureFault.Unexpected(MediaKind.Video, claiming))
                    assertEquals(FormatRelease.Released(claimed), reservations.release(videoReservation, claiming), "the first cause claims the ending")
                    val receipt = scope.async { rig.session.retirement() }
                    runCurrent()
                    ownerDoneWhileHeld = owner.job.isCompleted
                    retiredWhileHeld = receipt.isCompleted
                    inventoryWhileHeld = rig.gate.formatInventory()
                    assertSame(FormatAdmission.Ended, rig.gate.reserveFormat(video), "nothing new is admitted after the claim")
                    assertEquals(FormatRelease.Released(null), reservations.release(audioReservation, lateError))
                }
            val label = if (lateError == null) "clean" else "late Error"
            assertFalse(ownerDoneWhileHeld, "$label: the owner had not settled while the audio callback was still admitted")
            assertFalse(retiredWhileHeld, "$label: nor had the ending retired")
            assertEquals(FormatInventory(setOf(MediaKind.Audio), listOf(claiming), CaptureTermination.Failed(CaptureFault.Unexpected(MediaKind.Video, claiming))), inventoryWhileHeld)
            val raised = checkNotNull(outcome).exceptionOrNull()
            assertTrue(causeChain(raised, claiming), "$label: the claiming cause is the owner's failure, never an ordinary Unfinished: $outcome")
            val receipt = rig.session.retirement()
            assertSame(receipt, rig.session.retirement(), "$label: one frozen receipt")
            if (lateError == null) {
                assertEquals(listOf<Throwable>(claiming), receipt.causes)
                assertEquals(emptyList(), claiming.suppressed.toList())
            } else {
                assertEquals(listOf(claiming, lateError), receipt.causes, "the late cause is recorded after the one that claimed")
                assertEquals(1, generateSequence(raised) { it.cause }.sumOf { it.suppressedCount(lateError) }, "and carried once")
            }
            assertEquals(emptyList(), rig.tracks.labels(), "$label: no sender effect")
            assertTrue(rig.completion.calls.isEmpty())
            assertTrue(rig.video.handoff.ended && rig.audio.handoff.ended, "$label: both handoffs closed")
        }
    }

    @Test
    fun `k4 a callback still admitted when Completed wins holds back publication, and its late failure is the broadcast's`() = runTest {
        for (late in listOf<Throwable?>(IllegalStateException("the video callback raised after completion"), null)) {
            val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
            val video = rig.gate.bindFormatHolder(MediaKind.Video)
            var publishedWhileHeld = true
            var completionWhileHeld: List<String> = listOf("?")
            var terminationWhileHeld: CaptureTermination? = null
            var outcome: Result<BroadcastOutcome>? = null
            val reservations = RowReservations(rig.gate)
            runCatching {
                withRun({ rig.run(rig.formatPublication(FormatSources(CompletableDeferred(validatedVideo()), CompletableDeferred(validatedAudio())), BUDGET)) }) { owner ->
                    reservations.owning {
                        runCurrent()
                        val reservation = reservations.admit(video)
                        deliver(rig.video, 0, VIDEO_ANNEXB.copyOf(), 33_333L)
                        runCurrent()
                        deliverEnd(rig.video, 1)
                        deliverEnd(rig.audio, 0)
                        runCurrent()
                        terminationWhileHeld = rig.session.termination
                        publishedWhileHeld = owner.job.isCompleted
                        completionWhileHeld = rig.completion.calls.toList()
                        assertEquals(FormatRelease.Released(null), reservations.release(reservation, late))
                    }
                    runCurrent()
                    outcome = settled(owner)
                }
            }
            val label = if (late == null) "clean" else "late failure"
            assertEquals(CaptureTermination.Completed, terminationWhileHeld, "$label: Completed won while the callback was admitted")
            assertFalse(publishedWhileHeld, "$label: nothing was published while it was admitted")
            assertEquals(emptyList(), completionWhileHeld, "$label: and the sender was told nothing")
            if (late == null) {
                assertIs<BroadcastOutcome.Published>(checkNotNull(outcome).getOrThrow())
                assertEquals(listOf("drain", "complete", "drain"), rig.completion.calls)
                assertEquals(listOf("add:video", "add:audio", "write:v[11b@MediaMicros(33333)]", "end:v", "end:a"), rig.tracks.labels())
            } else {
                assertTrue(causeChain(checkNotNull(outcome).exceptionOrNull(), late), "the late cause is the broadcast's failure: $outcome")
                assertEquals(emptyList(), rig.completion.calls, "never completed")
                assertEquals(listOf("add:video", "add:audio", "write:v[11b@MediaMicros(33333)]", "end:v", "end:a", "remove:v", "remove:a"), rig.tracks.labels(), "both handles retired")
                assertEquals(listOf<Throwable>(late), rig.session.retirement().causes)
            }
            assertSame(rig.session.retirement(), rig.session.retirement())
        }
    }

    @Test
    fun `k5 the owner's own cancellation stays primary while a callback is admitted, and carries its late Error once`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val video = rig.gate.bindFormatHolder(MediaKind.Video)
        val late = AssertionError("the video callback raised an Error after cancellation")
        var ownerDoneWhileHeld = true
        var why: Throwable? = null
        var run: RunOwner? = null
        val reservations = RowReservations(rig.gate)
        val outcome =
            waitingOwner(rig, reservations) { owner ->
                val reservation = reservations.admit(video)
                run = owner
                why = owner.cancel("the row stops the broadcast")
                runCurrent()
                ownerDoneWhileHeld = owner.job.isCompleted
                assertIs<CaptureTermination.Cancelled>(rig.session.termination, "the owner's stop claimed the ending")
                assertEquals(FormatRelease.Released(null), reservations.release(reservation, late), "a late cause claims nothing")
            }
        assertFalse(ownerDoneWhileHeld, "the cancelled owner had not settled while the callback was admitted")
        val raised = checkNotNull(outcome).exceptionOrNull()
        assertTrue(checkNotNull(run).issued(checkNotNull(raised)) && causeChain(raised, checkNotNull(why)), "the owner's own cancellation leaves: $raised")
        assertEquals(1, generateSequence(raised) { it.cause }.sumOf { it.suppressedCount(late) }, "carrying the late Error once")
        assertEquals(listOf<Throwable>(late), rig.session.retirement().causes)
        assertEquals(emptyList(), rig.tracks.labels())
        assertTrue(rig.completion.calls.isEmpty())
    }

    @Test
    fun `k6 a waiter resumed inline by the last release sees the settled causes, and may re-enter the gate outside the monitor`() = runTest {
        val rig = Rig()
        val reservations = RowReservations(rig.gate)
        reservations.owning {
        val video = rig.gate.bindFormatHolder(MediaKind.Video)
        val reservation = reservations.admit(video)
        val fault = CaptureFault.Codec(MediaKind.Video, "a fault on the claiming thread")
        rig.forward.onFault(GENERATION, fault)
        assertEquals(CaptureTermination.Failed(fault), rig.session.termination)
        val late = IllegalStateException("the video callback raised after the fault")
        var releasing = false
        var resumedInside: Boolean? = null
        var seen: RetirementReceipt? = null
        var reentered: FormatAdmission? = null
        var lockFree = false
        val waiter =
            launch(Dispatchers.Unconfined) {
                val receipt = rig.session.retirement()
                resumedInside = releasing
                seen = receipt
                reentered = rig.gate.reserveFormat(video)
                val read = AtomicReference<FormatInventory?>()
                val probe = Thread({ read.set(rig.gate.formatInventory()) }, "lock-probe").apply { start() }
                probe.join(10_000L)
                lockFree = !probe.isAlive && read.get()?.causes == listOf<Throwable>(late)
            }
        assertFalse(waiter.isCompleted, "the admitted callback holds the receipt back")
        releasing = true
        assertEquals(FormatRelease.Released(null), reservations.release(reservation, late))
        releasing = false
        assertEquals(true, resumedInside, "the waiter resumed inline, inside the release")
        assertTrue(waiter.isCompleted)
        val receipt = checkNotNull(seen)
        assertEquals(listOf<Throwable>(late), receipt.causes, "the cause was recorded before the release could signal")
        assertEquals(CaptureTermination.Failed(fault), receipt.termination)
        assertSame(FormatAdmission.Ended, reentered, "re-entry from the waiter ran and was refused, without deadlock")
        assertTrue(lockFree, "another thread took the session lock while the waiter ran")
        assertSame(receipt, rig.session.retirement())
        }
    }

    /**
     * A row that fails while it holds an admitted callback must still leave
     * by itself: its own ownership releases the callback before the run
     * wrapper joins an owner that is waiting for it. The verdict is sampled
     * once the scheduler runs out of work, BEFORE any rescue; the rescue then
     * releases the observer's own retained reservations through the session
     * directly -- never through [RowReservations], which is what is under
     * test -- and never through the runTest timeout.
     */
    private suspend fun TestScope.injectedFailureLeaves(afterClaim: Boolean) {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val video = rig.gate.bindFormatHolder(MediaKind.Video)
        val audio = rig.gate.bindFormatHolder(MediaKind.Audio)
        val reservations = RowReservations(rig.gate)
        val retained = mutableListOf<FormatReservation>()
        val claiming = IllegalStateException("the video callback raised")
        val sentinel = AssertionError("injected while a callback is admitted (afterClaim=$afterClaim)")
        var seen: Throwable? = null
        var run: RunOwner? = null
        val scope = this
        val attempt =
            launch {
                seen =
                    runCatching {
                        withRun({ rig.run(rig.formatPublication(FormatSources(CompletableDeferred(validatedVideo()), CompletableDeferred()), BUDGET)) }) { owner ->
                            run = owner
                            reservations.owning {
                                scope.runCurrent()
                                val videoReservation = reservations.admit(video).also { retained.add(it) }
                                reservations.admit(audio).also { retained.add(it) }
                                if (afterClaim) reservations.release(videoReservation, claiming)
                                throw sentinel
                            }
                        }
                    }.exceptionOrNull()
            }
        var left = false
        var activeBeforeRescue: Set<MediaKind>? = null
        try {
            runCurrent()
            left = attempt.isCompleted
            activeBeforeRescue = rig.gate.formatInventory().active
        } finally {
            for (reservation in retained) rig.gate.releaseFormat(reservation, null)
        }
        runCurrent()
        kotlinx.coroutines.withTimeoutOrNull(30.seconds) { attempt.join() } ?: throw AssertionError("the row did not settle even after an explicit rescue")
        assertTrue(left, "afterClaim=$afterClaim: the failure left without an outside rescue")
        assertEquals(emptySet(), activeBeforeRescue, "afterClaim=$afterClaim: the row's own ownership had released every admitted callback")
        assertTrue(causeChain(seen, sentinel), "afterClaim=$afterClaim: the injected failure, or its reconstruction, escaped: $seen")
        val owner = checkNotNull(run)
        assertTrue(owner.job.isCompleted, "the owner settled")
        assertEquals(emptySet(), rig.gate.formatInventory().active)
        val receipt = rig.session.retirement()
        if (afterClaim) {
            assertEquals(CaptureTermination.Failed(CaptureFault.Unexpected(MediaKind.Video, claiming)), receipt.termination)
            assertEquals(listOf<Throwable>(claiming), receipt.causes, "the emergency release reported no cause of its own")
        } else {
            assertIs<CaptureTermination.Cancelled>(receipt.termination, "the wrapper's cancellation stopped the capture")
            assertEquals(emptyList(), receipt.causes, "and the emergency releases claimed nothing")
        }
        assertEquals(emptyList(), rig.tracks.labels())
        assertTrue(rig.completion.calls.isEmpty())
    }

    @Test
    fun `k8 a row that fails while holding an admitted callback leaves by itself, before or after a claim`() = runTest {
        injectedFailureLeaves(afterClaim = false)
        injectedFailureLeaves(afterClaim = true)
    }

    // -- the unexpected cause has ONE entry ------------------------------------------------------

    /**
     * `Unexpected` is what an accepted reservation release claims with; it is
     * not a fault a track submits. Through the general fault route it is
     * refused before anything changes -- before and after a claim, and for any
     * generation -- and its Throwable stays the caller's.
     */
    @Test
    fun `k9 the general fault route refuses an Unexpected fault, keeps its Throwable with the caller, and changes nothing`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val sentinel = AssertionError("an unexpected cause submitted as an ordinary fault")
        var beforeClaim: Throwable? = null
        var stale: Throwable? = null
        var afterClaim: Throwable? = null
        var inventoryBefore: FormatInventory? = null
        var inventoryAfterRefusal: FormatInventory? = null
        var terminationAfterRefusal: CaptureTermination? = null
        val real = CaptureFault.Codec(MediaKind.Video, "a real codec fault")
        val outcome =
            waitingOwner(rig) { owner ->
                inventoryBefore = rig.gate.formatInventory()
                beforeClaim = runCatching { rig.forward.onFault(GENERATION, CaptureFault.Unexpected(MediaKind.Video, sentinel)) }.exceptionOrNull()
                stale = runCatching { rig.forward.onFault(GENERATION + 1, CaptureFault.Unexpected(MediaKind.Audio, sentinel)) }.exceptionOrNull()
                inventoryAfterRefusal = rig.gate.formatInventory()
                terminationAfterRefusal = rig.session.termination
                runCurrent()
                stillLive(owner)
                rig.forward.onFault(GENERATION, real)
                afterClaim = runCatching { rig.forward.onFault(GENERATION, CaptureFault.Unexpected(MediaKind.Video, sentinel)) }.exceptionOrNull()
            }
        for ((label, raised) in listOf("before a claim" to beforeClaim, "for another generation" to stale, "after a claim" to afterClaim)) {
            val refused = assertIs<IllegalArgumentException>(raised, "$label: refused, not accepted as an ordinary fault")
            assertSame(sentinel, refused.cause, "$label: the Throwable is handed back to the caller")
        }
        assertEquals(inventoryBefore, inventoryAfterRefusal, "no cause was recorded")
        assertNull(terminationAfterRefusal, "and nothing was claimed")
        assertEquals(emptyList(), sentinel.suppressed.toList(), "nothing was attached to it")
        assertEquals(
            BroadcastOutcome.Unfinished("the capture session ended before the tracks were prepared: ${CaptureTermination.Failed(real)}", null, null),
            checkNotNull(outcome).getOrThrow(),
            "the real fault ended the broadcast as itself",
        )
        assertEquals(CaptureTermination.Failed(real), rig.session.termination, "the refusal after the claim changed nothing")
        assertEquals(emptyList(), rig.session.retirement().causes, "and no cause reached the receipt")
    }

    /** A claiming Error, through the one valid route, is the owner's failure as itself. */
    @Test
    fun `k10 an Error reported by an admitted callback claims the ending and is the owner's failure`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val video = rig.gate.bindFormatHolder(MediaKind.Video)
        val raised = AssertionError("the video callback raised an Error")
        val reservations = RowReservations(rig.gate)
        val outcome = waitingOwner(rig, reservations) { assertIs<FormatRelease.Released>(reservations.release(reservations.admit(video), raised)) }
        assertEquals(CaptureTermination.Failed(CaptureFault.Unexpected(MediaKind.Video, raised)), rig.session.termination)
        assertTrue(causeChain(checkNotNull(outcome).exceptionOrNull(), raised), "the Error leaves as the owner's failure: $outcome")
        assertEquals(listOf<Throwable>(raised), rig.session.retirement().causes)
        assertEquals(emptyList(), rig.tracks.labels())
    }

    /** A FOREIGN cancellation reported by a callback is a real cause, not the owner's own cancellation. */
    @Test
    fun `k11 a foreign cancellation reported by an admitted callback is a cause, never the owner's cancellation`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val video = rig.gate.bindFormatHolder(MediaKind.Video)
        val foreign = kotlinx.coroutines.CancellationException("cancelled inside the video callback's own work")
        var run: RunOwner? = null
        val reservations = RowReservations(rig.gate)
        val outcome =
            waitingOwner(rig, reservations) { owner ->
                run = owner
                assertIs<FormatRelease.Released>(reservations.release(reservations.admit(video), foreign))
            }
        val raised = checkNotNull(outcome).exceptionOrNull()
        assertTrue(causeChain(raised, foreign), "the foreign cancellation leaves as itself: $raised")
        assertFalse(checkNotNull(run).issued(checkNotNull(raised)), "and is not mistaken for a cancellation the row issued")
        assertEquals(listOf<Throwable>(foreign), rig.session.retirement().causes)
    }

    /** One Throwable reported by BOTH admitted kinds is one cause. */
    @Test
    fun `k12 the same Throwable reported by both admitted kinds is recorded and carried once`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val video = rig.gate.bindFormatHolder(MediaKind.Video)
        val audio = rig.gate.bindFormatHolder(MediaKind.Audio)
        val shared = IllegalStateException("one failure seen by both callbacks")
        val reservations = RowReservations(rig.gate)
        val outcome =
            waitingOwner(rig, reservations) {
                val videoReservation = reservations.admit(video)
                val audioReservation = reservations.admit(audio)
                assertIs<FormatRelease.Released>(reservations.release(videoReservation, shared))
                assertEquals(FormatRelease.Released(null), reservations.release(audioReservation, shared), "the second report of the same cause claims nothing")
            }
        assertEquals(listOf<Throwable>(shared), rig.session.retirement().causes, "one cause, not two")
        val raised = checkNotNull(outcome).exceptionOrNull()
        assertTrue(causeChain(raised, shared))
        assertEquals(emptyList(), shared.suppressed.toList(), "nothing attached to it twice, or at all")
    }

    private companion object {
        val BUDGET = 5.seconds
    }
}
