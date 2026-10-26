package org.openmoq.moq5.samples.publisher

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
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
 * New publication is admitted only while the session has not claimed its
 * ending -- decided atomically with that claim, not read from the later
 * terminal delivery.
 *
 * The window schedules (a1, a2, a3) hold the session between its claim and
 * its first handoff close (terminal undelivered, both handoffs open) and
 * each isolates ONE admission point: the first add, the second add, or the
 * publish entry that starts the drains. a0 is the positive control on the
 * same publication route; a4 and a5 exercise the real decision directly and
 * hold no window.
 *
 * The fault arrives on an owned worker through the session's own fault
 * route; the first close is gated by the fixture's forwarding handoff. A
 * held add waits under `NonCancellable`, and [withRun] joins its child under
 * `NonCancellable`, so every gate a row holds -- the held add and the close
 * latch -- is released by [Gates.owning] INSIDE the run's body, around
 * every driving step, before the wrapper joins. The c rows fail a held add
 * on purpose and require that failure to leave on its own; every window row
 * also joins its worker through [ClaimWindow.rescue], a path that does not
 * go through [Gates], so no row can leave a worker behind.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class PublicationAdmissionTest {

    /**
     * Every gate one row holds, released on every exit from the row's body.
     *
     * Release attempts every gate even when an earlier one fails; what the
     * release found is attached to the row's own failure, never put in its
     * place, and leaves by itself only when the row had none.
     */
    private class Gates {
        private val holds = mutableListOf<CompletableDeferred<Unit>>()
        private val closers = mutableListOf<() -> Unit>()
        var child: RunOwner? = null

        /** A commit-grade add gate this row owns. */
        fun hold(): CompletableDeferred<Unit> = CompletableDeferred<Unit>().also { holds.add(it) }

        fun onRelease(close: () -> Unit) {
            closers.add(close)
        }

        /** Completes every add gate, then runs every closer; returns what the closers raised. Safe to repeat. */
        fun release(): List<Throwable> {
            holds.forEach { it.complete(Unit) }
            return closers.mapNotNull { close -> runCatching { close() }.exceptionOrNull() }
        }

        suspend fun <T> owning(block: suspend () -> T): T {
            var primary: Throwable? = null
            try {
                return block()
            } catch (cancelled: CancellationException) {
                primary = cancelled
                throw cancelled
            } catch (failure: Exception) {
                primary = failure
                throw failure
            } catch (error: Error) {
                primary = error
                throw error
            } finally {
                val found = release()
                val leaving = primary
                if (found.isNotEmpty()) {
                    if (leaving == null) {
                        val first = found.first()
                        found.drop(1).forEach { if (it !== first) first.addSuppressed(it) }
                        throw first
                    }
                    found.forEach { if (it !== leaving) leaving.addSuppressed(it) }
                }
            }
        }
    }

    /** The session held between its claim and its first close, on an owned worker released through [gates]. */
    private class ClaimWindow(private val rig: Rig, gates: Gates) {
        private val entered = CountDownLatch(1)
        private val release = CountDownLatch(1)
        private val failure = AtomicReference<Throwable?>()
        val fault = CaptureFault.Codec(MediaKind.Video, "a fault from outside the handoff")
        private val worker = Thread({ runCatching { rig.forward.onFault(GENERATION, fault) }.exceptionOrNull()?.let { failure.set(it) } }, "claim-window-worker")

        init {
            rig.videoEntry.beforeClose = {
                entered.countDown()
                check(release.await(BOUND_SECONDS, TimeUnit.SECONDS)) { "the gated close was never released" }
            }
            gates.onRelease(::close)
        }

        val opened: Boolean get() = worker.state != Thread.State.NEW
        val joined: Boolean get() = !worker.isAlive && failure.get() == null

        /** Claims the ending and holds it before the first close; asserts the window's state. */
        fun open() {
            worker.start()
            assertTrue(entered.await(BOUND_SECONDS, TimeUnit.SECONDS), "the first close was reached")
            assertEquals(CaptureTermination.Failed(fault), rig.session.termination, "the ending is claimed")
            assertFalse(rig.video.handoff.ended || rig.audio.handoff.ended, "both handoffs are still open")
            assertFalse(rig.events.terminal.isCompleted, "the terminal is not yet delivered")
        }

        /**
         * The observer's own way out, independent of [Gates]: releases the
         * latch and joins the worker, bounded, asserting nothing.
         */
        fun rescue() {
            release.countDown()
            if (opened) worker.join(BOUND_SECONDS * 1_000L)
        }

        /** Releases the close and joins the worker, bounded; safe whether or not [open] ran, and to repeat. */
        private fun close() {
            release.countDown()
            if (opened) worker.join(BOUND_SECONDS * 1_000L)
            assertFalse(worker.isAlive, "the worker was joined")
            assertNull(failure.get(), "the close raised nothing")
        }
    }

    /** Where a c row fails its held add. */
    private enum class Inject { BeforeOpen, InWindow }

    /**
     * One commit-grade add held across the claim: the first add (video), or
     * the second (audio) with one video frame queued behind it.
     */
    private class HeldAdd(val second: Boolean) {
        val gates = Gates()
        val hold = gates.hold()
        val tracks = if (second) ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a"), hold) else ScriptedTracks().answers(HostTrack("v"), hold).answers(HostTrack("a"))
        val entered = tracks.entering()
        val rig = Rig(tracks)
        val window = ClaimWindow(rig, gates)
        val name = if (second) "the held audio add" else "the held video add"
        val committed = if (second) listOf("add:video", "add:audio") else listOf("add:video")
        val retired = if (second) listOf("add:video", "add:audio", "remove:v", "remove:a") else listOf("add:video", "remove:v")
    }

    private fun validated(): Pair<CompletableDeferred<CompleteTrackFormat>, CompletableDeferred<CompleteTrackFormat>> =
        CompletableDeferred(validatedVideo()) to CompletableDeferred(validatedAudio())

    private suspend fun TestScope.inWindow(gates: Gates, window: ClaimWindow, run: suspend () -> BroadcastOutcome, before: suspend () -> Unit, inside: suspend () -> Unit): BroadcastOutcome {
        val result =
            withRun(run) { owner ->
                gates.child = owner
                gates.owning {
                    runCurrent()
                    stillLive(owner)
                    before()
                    runCurrent()
                    window.open()
                    inside()
                }
                runCurrent()
                settled(owner)
            }
        return result.getOrThrow()
    }

    /** Drives [row] through its claim window; [inject] fails it at that point instead of releasing the add. */
    private suspend fun TestScope.drive(row: HeldAdd, inject: Inject? = null, sentinel: Throwable? = null): BroadcastOutcome {
        val (video, audio) = validated()
        return inWindow(
            row.gates,
            row.window,
            { row.rig.run(row.rig.formatPublication(FormatSources(video, audio), BUDGET)) },
            before = {
                if (row.second) deliver(row.rig.video, 0, VIDEO_ANNEXB.copyOf(), 33_333L)
                withTimeoutOrNull(BOUNDED) { row.entered.await() } ?: throw AssertionError("${row.name} was never entered")
                if (inject == Inject.BeforeOpen) throw checkNotNull(sentinel)
            },
            inside = {
                if (inject == Inject.InWindow) throw checkNotNull(sentinel)
                row.hold.complete(Unit)
                runCurrent()
                // The owner may already be retiring the committed handles; what may never appear is a further add, a write or an end.
                assertEquals(row.committed, row.rig.tracks.labels().filterNot { it.startsWith("remove:") }, "the admitted adds committed; nothing further was admitted")
            },
        )
    }

    /**
     * A failure while an add is held must leave by itself. The verdict is
     * sampled once the scheduler runs out of work, BEFORE any rescue; the
     * rescue then runs on every path through handles this observer retains
     * itself -- the add gate and the worker -- never through [Gates], which is
     * what is under test, and never through the runTest timeout.
     */
    private suspend fun TestScope.failureLeaves(second: Boolean, inject: Inject) {
        val row = HeldAdd(second)
        val sentinel = AssertionError("injected while ${row.name} is held: $inject")
        var seen: Throwable? = null
        var left = false
        var workerJoined = false
        var childSettled = false
        val scope = this
        val attempt = launch { seen = runCatching { scope.drive(row, inject, sentinel) }.exceptionOrNull() }
        try {
            runCurrent()
            left = attempt.isCompleted
            workerJoined = row.window.joined
            childSettled = row.gates.child?.job?.isCompleted == true
        } finally {
            row.hold.complete(Unit)
            row.window.rescue()
        }
        runCurrent()
        withTimeoutOrNull(BOUNDED) { attempt.join() } ?: throw AssertionError("${row.name}: the row did not settle even after an explicit rescue")
        assertTrue(left, "${row.name}, $inject: the failure left without an outside rescue")
        assertTrue(generateSequence(seen) { it.cause }.any { it === sentinel }, "the injected failure, or its coroutine reconstruction, escaped: $seen")
        assertEquals(emptyList(), sentinel.suppressed.toList(), "the cleanup attached nothing to the row's failure")
        assertEquals(inject == Inject.InWindow, row.window.opened, "the window opened exactly when the row reached it")
        assertTrue(workerJoined, "the worker was joined before the failure left")
        assertTrue(childSettled, "the child settled before the failure left")
        assertEquals(row.retired, row.rig.tracks.labels(), "every committed handle was retired exactly once, and nothing further was added")
        assertTrue(row.rig.completion.calls.isEmpty())
        assertTrue(row.rig.owner.isDecided)
    }

    @Test
    fun `a0 control - the same publication route publishes when nothing is claimed`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val (video, audio) = validated()
        val result =
            withRun({ rig.run(rig.formatPublication(FormatSources(video, audio), BUDGET)) }) { owner ->
                runCurrent()
                deliver(rig.video, 0, VIDEO_ANNEXB.copyOf(), 33_333L)
                runCurrent()
                deliverEnd(rig.video, 1)
                deliverEnd(rig.audio, 0)
                runCurrent()
                settled(owner)
            }
        assertIs<BroadcastOutcome.Published>(result.getOrThrow())
        assertEquals(listOf("add:video", "add:audio", "write:v[11b@MediaMicros(33333)]", "end:v", "end:a"), rig.tracks.labels())
    }

    /** The first add: both formats resolve only after the claim, with media already queued. */
    @Test
    fun `a1 a late resolution after the claim admits no add and no write`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val video = CompletableDeferred(validatedVideo())
        val audio = CompletableDeferred<CompleteTrackFormat>()
        val gates = Gates()
        val window = ClaimWindow(rig, gates)
        val outcome =
            try {
                inWindow(
                    gates,
                    window,
                    { rig.run(rig.formatPublication(FormatSources(video, audio), BUDGET)) },
                    before = { deliver(rig.video, 0, VIDEO_ANNEXB.copyOf(), 33_333L) },
                    inside = {
                        audio.complete(validatedAudio())
                        runCurrent()
                        assertEquals(emptyList(), rig.tracks.labels(), "no add and no write inside the claimed window")
                    },
                )
            } finally {
                window.rescue()
            }
        assertIs<BroadcastOutcome.Unfinished>(outcome)
        assertEquals(emptyList(), rig.tracks.labels(), "no handle was invented or removed")
        assertEquals(listOf(Discard(GENERATION, MediaKind.Video, 1)), rig.forward.discards(), "the queued frame was discarded")
        assertTrue(rig.completion.calls.isEmpty())
        assertTrue(rig.owner.isDecided)
    }

    /** The second add: the first is admitted and held across the claim; it commits, the second is never admitted. */
    @Test
    fun `a2 an add admitted before the claim commits and is retired once, and the next add is not admitted`() = runTest {
        val row = HeldAdd(second = false)
        val outcome =
            try {
                drive(row)
            } finally {
                row.window.rescue()
            }
        assertIs<BroadcastOutcome.Unfinished>(outcome)
        assertEquals(listOf("add:video", "remove:v"), row.rig.tracks.labels(), "the committed handle is retired once")
        assertTrue(row.rig.completion.calls.isEmpty())
        assertTrue(row.rig.owner.isDecided)
    }

    /** The publish entry: both adds admitted before the claim (the second held across it); no drain starts. */
    @Test
    fun `a3 publication fully prepared before the claim starts no drain after it`() = runTest {
        val row = HeldAdd(second = true)
        val outcome =
            try {
                drive(row)
            } finally {
                row.window.rescue()
            }
        assertIs<BroadcastOutcome.Unfinished>(outcome)
        assertEquals(listOf("add:video", "add:audio", "remove:v", "remove:a"), row.rig.tracks.labels(), "no write, no end, both handles retired once")
        assertEquals(listOf(Discard(GENERATION, MediaKind.Video, 1)), row.rig.forward.discards(), "the queued frame was discarded, not written")
        assertTrue(row.rig.completion.calls.isEmpty())
        assertTrue(row.rig.owner.isDecided)
    }

    /** The real decision: this generation while running; never another generation; never after a claimed ending. */
    @Test
    fun `a4 the real session admits its own generation until it claims an ending, and never a stale one`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        withRun({ rig.run(rig.formatPublication(FormatSources(CompletableDeferred(), CompletableDeferred()), BUDGET)) }) { owner ->
            runCurrent()
            stillLive(owner)
            assertTrue(rig.session.admitPublication(GENERATION), "its own generation, running")
            assertFalse(rig.session.admitPublication(GENERATION + 1), "a later generation")
            assertFalse(rig.session.admitPublication(GENERATION - 1), "an earlier generation")
            rig.forward.onFault(GENERATION, CaptureFault.Codec(MediaKind.Video, "claimed"))
            assertFalse(rig.session.admitPublication(GENERATION), "its own generation, after the claimed ending")
            runCurrent()
            settled(owner)
        }
        assertEquals(emptyList(), rig.tracks.labels())
    }

    /** An admission authority for another generation admits nothing: exactly zero sender effects, with both formats known. */
    @Test
    fun `a5 a stale-generation admission starts no add and no drain`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val (video, audio) = validated()
        val stale = livePublication(rig.owner, rig.tracks, rig.events, PublicationAdmission { rig.session.admitPublication(GENERATION + 1) }, FormatSources(video, audio), BUDGET, rig.limits, rig.clock, ORIGIN, keying = null)
        val result =
            withRun({ rig.run(stale) }) { owner ->
                runCurrent()
                settled(owner)
            }
        assertEquals(BroadcastOutcome.Unfinished("the capture session ended before the video track was admitted", null, null), result.getOrThrow())
        assertEquals(emptyList(), rig.tracks.labels(), "exactly zero sender effects")
        assertTrue(rig.completion.calls.isEmpty())
    }

    @Test
    fun `c1 a failure before the window opens, with the video add held, leaves by itself`() = runTest { failureLeaves(second = false, Inject.BeforeOpen) }

    @Test
    fun `c2 a failure inside the window, before the held video add is released, leaves by itself`() = runTest { failureLeaves(second = false, Inject.InWindow) }

    @Test
    fun `c3 a failure before the window opens, with the audio add held, leaves by itself`() = runTest { failureLeaves(second = true, Inject.BeforeOpen) }

    @Test
    fun `c4 a failure inside the window, before the held audio add is released, leaves by itself`() = runTest { failureLeaves(second = true, Inject.InWindow) }

    /** Release attempts every gate past a failing one, keeps the row's failure, and raises its own only when the row had none. */
    @Test
    fun `c5 release keeps the row's failure and attaches what it found, never replacing it`() = runTest {
        val found = AssertionError("a gate failed to close")
        val gates = Gates()
        val hold = gates.hold()
        var later = false
        gates.onRelease { throw found }
        gates.onRelease { later = true }
        val sentinel = AssertionError("the row's own failure")
        val raised = runCatching { gates.owning<Unit> { throw sentinel } }.exceptionOrNull()
        assertSame(sentinel, raised, "the row's failure leaves as itself")
        assertEquals(listOf<Throwable>(found), sentinel.suppressed.toList(), "the release failure is attached, once")
        assertTrue(hold.isCompleted, "the add gate was released")
        assertTrue(later, "the closer after the failing one still ran")

        val alone = AssertionError("the only failure")
        val quiet = Gates().apply { onRelease { throw alone } }
        assertSame(alone, runCatching { quiet.owning { 1 } }.exceptionOrNull(), "with no row failure, the release failure leaves by itself")
        assertEquals(7, Gates().owning { 7 }, "a clean release returns the row's value")
    }

    private companion object {
        val BUDGET = 5.seconds
        const val BOUND_SECONDS = 10L
    }
}
