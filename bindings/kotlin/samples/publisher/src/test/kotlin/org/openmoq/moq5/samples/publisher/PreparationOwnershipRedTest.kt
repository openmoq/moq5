package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.coroutines.coroutineContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNotSame
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.UnixMicros

/**
 * Ownership of a live publication's two commit-grade handles, from the return
 * of each add through one settlement decision.
 *
 * `w` rows measure [TrackOwner] at its own boundary through a scripted
 * [TrackOperations] -- the external effect boundary and nothing else. `f`
 * rows measure this file's own fixtures: that an expected failure is
 * interpreted on the run's frame and accounted at the boundary, that a held
 * child is always released, that the effect inventory rejects a wrong handle
 * or payload. `o` rows measure the coordinator's live preparation through
 * the SAME owner and the production [livePublication]; they fail at the
 * coordinator's live boundary until it is implemented, and their assertions
 * are the declared oracles.
 *
 * Coordinator inventories are compared as structured effects -- handle
 * IDENTITY, the whole SendObject, the declared timeout -- and every recorded
 * primary's roots exactly, by identity; labels are diagnostics only.
 *
 * Handles are opaque host values. Removal failures are host exceptions, not
 * library `SenderClosed`/`SendTrackUnavailable` instances: the public
 * forwarding adapter is compiled and source-reviewed, not integration-proven
 * here. Configurations are DECLARED fixtures that certify nothing about an
 * encoder.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class PreparationOwnershipRedTest {

    // ---- w: the owner at its own boundary -----------------------------------------

    @Test
    fun w1_aDuplicateKindIsRejectedBeforeAnySenderEffect() = runTest {
        val tracks = ScriptedTracks().answers(HostTrack("v"))
        val owner = TrackOwner(tracks)
        val first = assertIs<AddOutcome.Added<HostTrack>>(owner.add(MediaKind.Video, VIDEO_CONFIG))
        assertEquals("v", first.handle.name)
        assertEquals(listOf("add:video"), tracks.labels())
        assertSame(VIDEO_CONFIG, (tracks.all()[0] as Effect.Add).config)
        val again = assertIs<AddOutcome.Rejected>(owner.add(MediaKind.Video, VIDEO_CONFIG))
        assertEquals("a video track was already requested", again.reason)
        assertEquals(listOf("add:video"), tracks.labels(), "no second sender call")
        assertSame(first.handle, owner.handle(MediaKind.Video))
    }

    @Test
    fun w2_anAddAfterTheDecisionIsRejectedWithoutASenderCall() = runTest {
        val tracks = ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a"))
        val owner = TrackOwner(tracks)
        assertIs<AddOutcome.Added<HostTrack>>(owner.add(MediaKind.Video, VIDEO_CONFIG))
        assertEquals(Retired.Removed, owner.retire().video)
        assertTrue(owner.isDecided)
        val late = assertIs<AddOutcome.Rejected>(owner.add(MediaKind.Audio, AUDIO_CONFIG))
        assertEquals("the track ownership is already decided", late.reason)
        assertEquals(listOf("add:video", "remove:v"), tracks.labels())
        assertNull(owner.handle(MediaKind.Audio))
    }

    /** A commit-grade add completing during cancellation still commits to its slot; every hold is opened on every exit. */
    @Test
    fun w3_anAddAcceptedDuringCancellationStillCommitsToItsSlot() = runTest {
        val hold = CompletableDeferred<Unit>()
        val tracks = ScriptedTracks()
        val entered = tracks.entering()
        tracks.answers(HostTrack("v"), hold)
        val owner = TrackOwner(tracks)
        val settlement =
            supervisorScope {
                val adding = async { owner.add(MediaKind.Video, VIDEO_CONFIG) }
                withHolds(listOf(hold)) {
                    runCurrent()
                    withTimeoutOrNull(BOUNDED) { entered.await() } ?: throw AssertionError("the add was never entered")
                    adding.cancel(CancellationException("the row cancelled the caller"))
                    runCurrent()
                    assertNull(owner.handle(MediaKind.Video), "not yet: the sender has not answered")
                    hold.complete(Unit)
                    runCurrent()
                }
                withContext(NonCancellable) { adding.join() }
                assertTrue(adding.isCancelled)
                assertNotNull(owner.handle(MediaKind.Video), "the accepted handle is in its slot regardless")
                owner.retire()
            }
        assertEquals(Retired.Removed, settlement.video)
        assertEquals(listOf("add:video", "remove:v"), tracks.labels())
    }

    @Test
    fun w4_bothRemovalsAreAttemptedAndTypedFailuresKeepIdentity() = runTest {
        val v = HostTrack("v")
        val a = HostTrack("a")
        val refused = HostRemovalRefused("the sender is closed")
        val tracks = ScriptedTracks().answers(v).answers(a)
        tracks.removeRaises[v] = refused
        val owner = TrackOwner(tracks)
        owner.add(MediaKind.Video, VIDEO_CONFIG)
        owner.add(MediaKind.Audio, AUDIO_CONFIG)
        val s = owner.retire()
        assertEquals(listOf("add:video", "add:audio", "remove:v", "remove:a"), tracks.labels())
        assertEquals(Retired.Failed, s.video)
        assertSame(refused, s.videoRoot)
        assertEquals(Retired.Removed, s.audio)
        assertNull(s.audioRoot)
        assertSame(v, owner.handle(MediaKind.Video), "a failed removal keeps its handle referenced")
    }

    @Test
    fun w5_anErrorInTheFirstAttemptDoesNotSkipTheSecond() = runTest {
        val v = HostTrack("v")
        val a = HostTrack("a")
        val error = HostRemovalError("the first removal raised an Error")
        val tracks = ScriptedTracks().answers(v).answers(a)
        tracks.removeRaises[v] = error
        val owner = TrackOwner(tracks)
        owner.add(MediaKind.Video, VIDEO_CONFIG)
        owner.add(MediaKind.Audio, AUDIO_CONFIG)
        val s = owner.retire()
        assertEquals(listOf("add:video", "add:audio", "remove:v", "remove:a"), tracks.labels())
        assertSame(error, s.videoRoot)
        assertEquals(Retired.Removed, s.audio)
    }

    @Test
    fun w6_alreadyRemovedIsSkippedButAnAttemptedRefusalIsAFailure() = runTest {
        val v = HostTrack("v")
        val a = HostTrack("a")
        val unavailable = HostRemovalRefused("the track accepts no further content")
        val tracks = ScriptedTracks().answers(v).answers(a)
        tracks.removedState.add(v)
        tracks.removeRaises[a] = unavailable
        val owner = TrackOwner(tracks)
        owner.add(MediaKind.Video, VIDEO_CONFIG)
        owner.add(MediaKind.Audio, AUDIO_CONFIG)
        val s = owner.retire()
        assertEquals(listOf("add:video", "add:audio", "remove:a"), tracks.labels())
        assertEquals(Retired.Skipped, s.video)
        assertNull(s.videoRoot)
        assertEquals(Retired.Failed, s.audio)
        assertSame(unavailable, s.audioRoot)
    }

    @Test
    fun w7_releaseRemovesNothingAndTheDecisionIsMadeOnce() = runTest {
        val tracks = ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a"))
        val owner = TrackOwner(tracks)
        owner.add(MediaKind.Video, VIDEO_CONFIG)
        owner.add(MediaKind.Audio, AUDIO_CONFIG)
        val released = owner.release()
        assertEquals(Retired.Left, released.video)
        assertEquals(Retired.Left, released.audio)
        assertEquals(listOf("add:video", "add:audio"), tracks.labels())
        assertSame(released, owner.retire())
        assertEquals(listOf("add:video", "add:audio"), tracks.labels())
        assertIs<AddOutcome.Rejected>(owner.add(MediaKind.Video, VIDEO_CONFIG))
    }

    /** A declared refusal on the sender's failure path becomes the owner's Refused; the reservation stands. */
    @Test
    fun w8_aDeclaredRefusalIsAnOutcomeAndKeepsTheReservation() = runTest {
        val tracks = ScriptedTracks().refuses("the track name is busy: video")
        val owner = TrackOwner(tracks)
        val refused = assertIs<AddOutcome.Refused>(owner.add(MediaKind.Video, VIDEO_CONFIG))
        assertEquals("the track name is busy: video", refused.reason)
        assertNull(owner.handle(MediaKind.Video))
        assertIs<AddOutcome.Rejected>(owner.add(MediaKind.Video, VIDEO_CONFIG), "one attempt per name")
        assertEquals(listOf("add:video"), tracks.labels())
    }

    // ---- f: the fixtures themselves ------------------------------------------------

    /** An expected failure is interpreted on the run's frame by identity, and what escapes the helper is that instance or its recovered copy. */
    @Test
    fun f1_anExpectedFailureIsInterpretedOnTheRunFrameAndAccountedAtTheBoundary() = runTest {
        val failure = HostWriteFailed("the declared failing run")
        var interpreted = false
        val escaped =
            escaping {
                withRun({ throw failure }) { owner ->
                    runCurrent()
                    val recorded = assertNotNull(settled(owner).exceptionOrNull())
                    assertSame(failure, recorded, "the run's OWN record is the instance")
                    interpreted = true
                }
            }
        assertTrue(interpreted, "the row's assertions demonstrably executed")
        expectLeft(escaped, failure)   /* the helper let the unclaimed failure out, as that instance or its recovered copy */
    }

    /** A wrong wrapper -- another class with the original as cause -- fails the primary oracle. */
    @Test
    fun f2_aWrongWrapperIsNotTheSamePrimary() {
        val original = HostWriteFailed("original")
        val wrapper = IllegalArgumentException("wrapped", original)
        assertFalse(isSameOrRecoveredCopyOf(wrapper, original))
        assertTrue(isSameOrRecoveredCopyOf(original, original))
        val copy = HostWriteFailed("original").also { it.initCause(original) }
        assertTrue(isSameOrRecoveredCopyOf(copy, original), "the constrained recovered shape: same class, original as direct cause")
    }

    /**
     * A held child is never stranded: an assertion injected after the held
     * add signals entry escapes as itself, the hold is opened before the
     * helper joins, and the held child is observed to complete.
     */
    @Test
    fun f3_anAssertionWithAHeldChildEscapesAndReleasesTheHold() = runTest {
        val hold = CompletableDeferred<Unit>()
        val tracks = ScriptedTracks()
        val entered = tracks.entering()
        tracks.answers(HostTrack("v"), hold)
        val owner = TrackOwner(tracks)
        val mine = AssertionError("injected after the held add was entered")
        var childDone = false
        val escaped =
            escaping {
                withRun({ owner.add(MediaKind.Video, VIDEO_CONFIG); childDone = true; BroadcastOutcome.Unfinished("declared", null, null) }) {
                    withHolds(listOf(hold)) {
                        runCurrent()
                        withTimeoutOrNull(BOUNDED) { entered.await() } ?: throw AssertionError("never entered")
                        throw mine
                    }
                }
            }
        assertTrue(hold.isCompleted, "the hold was opened on the failure path")
        assertTrue(childDone, "and the held child completed rather than being stranded")
        assertNotNull(owner.handle(MediaKind.Video), "the accepted handle reached its owner")
        expectLeft(escaped, mine)   /* the injected assertion is what escaped */
    }

    /** The effect comparator refuses an equal-name wrong handle (write AND remove), a wrong payload, and same payload/time with wrong captureTime or grouping. */
    @Test
    fun f4_theInventoryRejectsWrongHandlesPayloadsAndMetadata() {
        val v = HostTrack("v")
        val impostor = HostTrack("v")
        val obj = sendObject(byteArrayOf(1, 2, 3), 10L, startsGroup = true)
        val write = Effect.Write(v, obj, WRITE_TIMEOUT)
        assertTrue(write.matches(Effect.Write(v, sendObject(byteArrayOf(1, 2, 3), 10L, startsGroup = true), WRITE_TIMEOUT)))
        assertFalse(write.matches(Effect.Write(impostor, obj, WRITE_TIMEOUT)), "same name, different handle")
        assertFalse(write.matches(Effect.Write(v, sendObject(byteArrayOf(1, 2, 4), 10L, startsGroup = true), WRITE_TIMEOUT)), "wrong payload")
        assertFalse(write.matches(Effect.Write(v, obj.copy(captureTime = UnixMicros(EPOCH + 11uL)), WRITE_TIMEOUT)), "wrong captureTime")
        assertFalse(write.matches(Effect.Write(v, obj.copy(startsGroup = false), WRITE_TIMEOUT)), "wrong grouping flag")
        assertFalse(write.matches(Effect.Write(v, obj, DRAIN_TIMEOUT)), "wrong timeout")
        assertFalse(Effect.Remove(v).matches(Effect.Remove(impostor)), "equal-name wrong REMOVE handle")
        assertFalse(Effect.End(v, WRITE_TIMEOUT).matches(Effect.End(v, DRAIN_TIMEOUT)), "end with the wrong timeout")
    }

    /**
     * The scheduler ordering o6 depends on, isolated: a handler registered
     * AFTER the scheduler was driven sees the child's result already
     * consumed; one registered from INSIDE the child before it completes
     * fires before any consumer resumes.
     */
    @Test
    fun f5_aHandlerRegisteredInsideTheChildFiresBeforeConsumption() = runTest {
        // Late registration: consumption has already happened.
        var consumedLate = false
        var lateHandlerSawConsumed: Boolean? = null
        supervisorScope {
            val child = async { 42 }
            val consumer = launch { child.await(); consumedLate = true }
            runCurrent()
            child.invokeOnCompletion { lateHandlerSawConsumed = consumedLate }
            consumer.join()
        }
        assertEquals(true, lateHandlerSawConsumed, "registered after the drive, the handler ran with the result already consumed")

        // Registration from inside the child, before it completes.
        var consumedInside = false
        var insideHandlerSawConsumed: Boolean? = null
        supervisorScope {
            val child =
                async {
                    currentCoroutineContext()[Job]!!.invokeOnCompletion { insideHandlerSawConsumed = consumedInside }
                    43
                }
            val consumer = launch { child.await(); consumedInside = true }
            runCurrent()
            consumer.join()
        }
        assertEquals(false, insideHandlerSawConsumed, "registered inside the child, the handler fired before the consumer resumed")
        assertTrue(consumedInside)
    }

    /**
     * The cancellation fixture reaches its assertions: a declared run that
     * parks until cancelled is cancelled through the owner, the boundary is
     * interpreted WITHOUT a liveness premise, nothing unexpected is swallowed
     * and nothing escapes. The identity shape the pinned runtime recorded --
     * the instance or its reconstruction -- is observed, not assumed.
     */
    @Test
    fun f7_theCancellationFixtureReachesItsAssertionsWithoutSwallowing() = runTest {
        var interpreted = false
        var shape = ""
        val escaped =
            escaping {
                withRun({ kotlinx.coroutines.awaitCancellation() }) { owner ->
                    runCurrent()
                    val issued = owner.cancel("the row cancelled the parked run")
                    runCurrent()
                    val recorded = expectRecordedCancellation(settled(owner), issued)
                    shape = if (recorded === issued) "instance" else "reconstruction"
                    expectRoots(recorded, emptyList())
                    interpreted = true
                }
            }
        expectNothingLeft(escaped)
        assertTrue(interpreted, "the boundary assertions executed")
        assertTrue(shape == "instance" || shape == "reconstruction", "observed shape: $shape")
        System.err.println("f7 recorded cancellation shape: $shape")
    }

    /** The cancellation identity oracle: the instance and its constrained reconstruction pass; nothing else does. */
    @Test
    fun f8_theCancellationIdentityOracleRejectsLookalikes() {
        val issued = CancellationException("the user left")
        assertTrue(isIssuedCancellation(issued, issued))
        val reconstruction = CancellationException("the user left").also { it.initCause(issued) }
        assertTrue(isIssuedCancellation(reconstruction, issued), "same class, issued as the direct cause")
        assertFalse(isIssuedCancellation(CancellationException("the user left"), issued), "an unrelated cancellation with the same message")
        assertFalse(isIssuedCancellation(IllegalStateException("wrapped", issued), issued), "another Throwable type wrapping the issued cause")
        val deeper = CancellationException("deeper").also { it.initCause(reconstruction) }
        assertFalse(isIssuedCancellation(deeper, issued), "an unrelated cause chain")
        assertFalse(isIssuedCancellation(HostWriteFailed("not a cancellation"), issued))
    }

    /**
     * Inside a lambda with a scope receiver, the bare `coroutineContext` is
     * the SCOPE's context, and a handler registered on its job fires at the
     * scope's completion, not the current coroutine's. The current
     * coroutine's own job is `currentCoroutineContext()[Job]`.
     */
    @Test
    fun f9_theCurrentJobIsNotTheScopeReceiversJob() = runTest {
        var scopeJob: Job? = null
        var ownJob: Job? = null
        var firedBeforeConsumption: Boolean? = null
        var consumed = false
        val prepare: suspend () -> Int = {
            scopeJob = coroutineContext[Job]
            ownJob = currentCoroutineContext()[Job]
            ownJob!!.invokeOnCompletion { firedBeforeConsumption = !consumed }
            7
        }
        supervisorScope {
            val child = async { prepare() }
            val consumer = launch { child.await(); consumed = true }
            runCurrent()
            consumer.join()
        }
        assertSame<Job?>(this.coroutineContext[Job], scopeJob, "the bare name resolved to the test scope's job")
        assertNotSame(scopeJob, ownJob, "the current coroutine's job is a different job")
        assertEquals(true, firedBeforeConsumption, "and a handler on the current job fired before the result was consumed")
    }

    /** The exact root comparator refuses an extra root and a duplicate root, and demands zero where none is owed. */
    @Test
    fun f6_theRootComparatorRejectsExtrasAndDuplicates() {
        val primary = HostWriteFailed("primary")
        val a = HostRemovalRefused("a")
        val b = HostRemovalRefused("b")
        primary.addSuppressed(a)
        expectRoots(primary, listOf(a))
        assertFalse(runCatching { expectRoots(primary, listOf(a, b)) }.isSuccess, "a missing root fails")
        assertFalse(runCatching { expectRoots(primary, emptyList()) }.isSuccess, "an extra root fails")
        primary.addSuppressed(b)
        assertFalse(runCatching { expectRoots(primary, listOf(a)) }.isSuccess, "an extra root fails")
        val twice = HostWriteFailed("twice")
        twice.addSuppressed(a)
        twice.addSuppressed(a)
        assertFalse(runCatching { expectRoots(twice, listOf(a)) }.isSuccess, "a duplicate root fails")
        expectRoots(HostWriteFailed("clean"), emptyList())
    }

    // ---- o: the coordinator's live preparation, through the same owner ------------

    /** Both adds precede every write; each lane writes its declared media through its OWN handle; ends, completion, drain, no removal. */
    @Test
    fun o2_bothAddsPrecedeWritesAndEachLaneWritesThroughItsOwnHandle() = runTest {
        val v = HostTrack("v")
        val a = HostTrack("a")
        val rig = Rig(ScriptedTracks().answers(v).answers(a))
        val videoBytes = VIDEO_ANNEXB.copyOf()
        val audioBytes = byteArrayOf(0x55, 0x66)
        val outcome =
            withRun(rig::run) { owner ->
                runCurrent()
                stillLive(owner)
                assertEquals(listOf("add:video", "add:audio"), rig.tracks.labels(), "both admitted before any write")
                assertSame(v, rig.owner.handle(MediaKind.Video))
                assertSame(a, rig.owner.handle(MediaKind.Audio))
                deliver(rig.video, 0, videoBytes, 33_333L)
                runCurrent()
                deliver(rig.audio, 0, audioBytes, 21_333L)
                runCurrent()
                deliverEnd(rig.video, 1)
                runCurrent()
                deliverEnd(rig.audio, 1)
                runCurrent()
                settled(owner)
            }
        assertIs<BroadcastOutcome.Published>(outcome.getOrThrow())
        expectEffects(
            listOf(
                Effect.Add(VIDEO_FORMAT_CONFIG),
                Effect.Add(AUDIO_FORMAT_CONFIG),
                Effect.Write(v, sendObject(VIDEO_AVC1, 33_333L, startsGroup = true), WRITE_TIMEOUT),
                Effect.Write(a, sendObject(audioBytes, 21_333L, startsGroup = false), WRITE_TIMEOUT),
                Effect.End(v, WRITE_TIMEOUT),
                Effect.End(a, WRITE_TIMEOUT),
            ),
            rig.tracks.all(),
        )
        assertTrue(rig.videoEntry.entered && rig.audioEntry.entered, "both lanes were entered")
        assertEquals(listOf("drain", "complete", "drain"), rig.completion.calls)
        assertTrue(rig.owner.isDecided, "released once")
    }

    @Test
    fun o3_aTerminalBeforePreparationAdmitsNothing() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        rig.events.terminal.complete(CaptureTermination.Cancelled("ended before preparation"))
        assertIs<BroadcastOutcome.Unfinished>(rig.run())
        expectEffects(emptyList(), rig.tracks.all())
        assertFalse(rig.videoEntry.entered || rig.audioEntry.entered, "no lane was entered")
    }

    @Test
    fun o4_aTerminalBetweenTheAddsRetiresTheFirstAndEntersNoSecond() = runTest {
        val hold = CompletableDeferred<Unit>()
        val v = HostTrack("v")
        val tracks = ScriptedTracks()
        val entered = tracks.entering()
        tracks.answers(v, hold).answers(HostTrack("a"))
        val rig = Rig(tracks)
        val outcome =
            withRun(rig::run) { owner ->
                withHolds(listOf(hold)) {
                    runCurrent()
                    stillLive(owner)
                    withTimeoutOrNull(BOUNDED) { entered.await() } ?: throw AssertionError("the first add was never entered")
                    rig.terminate()
                    runCurrent()
                    hold.complete(Unit)
                    runCurrent()
                    settled(owner)
                }
            }
        assertIs<BroadcastOutcome.Unfinished>(outcome.getOrThrow())
        expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Remove(v)), tracks.all())   /* the second add never entered; the first retired once */
        assertFalse(rig.videoEntry.entered || rig.audioEntry.entered)
    }

    /** Cancellation with an accepted add in flight: the coordinator cannot decide before the child settles; the handle is owned and retired once. */
    @Test
    fun o5_cancellationWithAnAcceptedAddInFlightStillOwnsAndRetiresTheHandle() = runTest {
        val hold = CompletableDeferred<Unit>()
        val v = HostTrack("v")
        val tracks = ScriptedTracks()
        val entered = tracks.entering()
        tracks.answers(v, hold).answers(HostTrack("a"))
        val rig = Rig(tracks)
        var cause: CancellationException? = null
        var recorded: Throwable? = null
        val escaped =
            escaping {
                withRun(rig::run) { owner ->
                    withHolds(listOf(hold)) {
                        runCurrent()
                        stillLive(owner)
                        withTimeoutOrNull(BOUNDED) { entered.await() } ?: throw AssertionError("the add was never entered")
                        cause = owner.cancel("the user left")
                        runCurrent()
                        assertFalse(owner.job.isCompleted, "no decision or return while the accepted add is in flight")
                        assertFalse(rig.owner.isDecided)
                        hold.complete(Unit)
                        runCurrent()
                        recorded = expectRecordedCancellation(settled(owner), assertNotNull(cause))   /* the issued cancellation, or its constrained reconstruction */
                        expectRoots(assertNotNull(recorded), emptyList())
                    }
                }
            }
        expectNothingLeft(escaped)   /* a clean own cancellation does not escape the helper */
        expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Remove(v)), tracks.all())   /* owned and retired once; audio never entered */
        assertFalse(rig.videoEntry.entered || rig.audioEntry.entered)
    }

    /** A RUNNING child cancelled after Ready is built (held before returning): both retired once, no lane. Not the completed-child case. */
    @Test
    fun o6a_aRunningChildCancelledAfterReadyIsBuiltIsRetiredOnce() = runTest {
        val v = HostTrack("v")
        val a = HostTrack("a")
        val tracks = ScriptedTracks().answers(v).answers(a)
        val rig = Rig(tracks)
        val built = CompletableDeferred<Unit>()
        val hold = CompletableDeferred<Unit>()
        val held = Publication.Live(rig.owner) {
            val prepared = rig.publication.prepare()
            check(prepared is Preparation.Ready)
            built.complete(Unit)
            withContext(NonCancellable) { hold.await() }
            prepared
        }
        val escaped =
            escaping {
                withRun({ rig.run(held) }) { owner ->
                    withHolds(listOf(hold)) {
                        runCurrent()
                        stillLive(owner)
                        withTimeoutOrNull(BOUNDED) { built.await() } ?: throw AssertionError("Ready was never built")
                        val cause = owner.cancel("cancelled while the child still runs")
                        runCurrent()
                        hold.complete(Unit)
                        runCurrent()
                        expectRoots(expectRecordedCancellation(settled(owner), cause), emptyList())
                    }
                }
            }
        expectNothingLeft(escaped)
        expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Add(AUDIO_FORMAT_CONFIG), Effect.Remove(v), Effect.Remove(a)), tracks.all())
        assertFalse(rig.videoEntry.entered || rig.audioEntry.entered, "no lane entered")
    }

    /**
     * The COMPLETED child whose delivery is lost. The completion handler is
     * registered FROM INSIDE the prepare closure, before Ready is built and
     * before the child can complete, with the run owner installed by the row
     * before its first scheduler drive. At the child's own completion the
     * handler records the boundary -- Ready was built, the child completed
     * cleanly, the owner is NOT decided, both handles are owned -- and
     * cancels the caller there, before any resumption the completion enqueues
     * can consume the result. No hold before return, no sleep. Consumption
     * not having happened is read from the lane-entry action itself.
     */
    @Test
    fun o6_aCompletedChildWhoseDeliveryIsLostIsRetiredOnce() = runTest {
        val v = HostTrack("v")
        val a = HostTrack("a")
        val tracks = ScriptedTracks().answers(v).answers(a)
        val rig = Rig(tracks)
        class Boundary {
            @Volatile var owner: RunOwner? = null
            @Volatile var readyBuilt = false
            @Volatile var childCause: Throwable? = null
            @Volatile var decidedAtCompletion: Boolean? = null
            @Volatile var handlesAtCompletion: Boolean? = null
            @Volatile var issued: CancellationException? = null
        }
        val boundary = Boundary()
        val observing = Publication.Live(rig.owner) {
            // The CURRENT coroutine's job -- inside a scope-receiver lambda the
            // bare `coroutineContext` names the enclosing scope's, not this one's.
            val job = assertNotNull(currentCoroutineContext()[Job], "the preparation runs in its own job")
            assertNotSame<Job?>(job, this@runTest.coroutineContext[Job], "and that job is not the test scope's")
            job.invokeOnCompletion { cause ->
                boundary.childCause = cause
                boundary.decidedAtCompletion = rig.owner.isDecided
                boundary.handlesAtCompletion = rig.owner.handle(MediaKind.Video) === v && rig.owner.handle(MediaKind.Audio) === a
                boundary.issued = assertNotNull(boundary.owner, "the run owner was installed before the first drive").cancel("cancelled at the child's completion, before delivery")
            }
            val prepared = rig.publication.prepare()
            boundary.readyBuilt = prepared is Preparation.Ready
            prepared
        }
        val escaped =
            escaping {
                withRun({ rig.run(observing) }) { owner ->
                    boundary.owner = owner            // installed BEFORE the first scheduler drive
                    runCurrent()
                    // No liveness premise here: this drive is EXPECTED to complete
                    // the cancellation. The recorded result is inspected at the
                    // boundary instead, and anything unexpected still propagates.
                    onlyIssuedSettlement(owner)
                    assertTrue(boundary.readyBuilt, "Ready was built")
                    assertNull(boundary.childCause, "and the child completed cleanly")
                    assertEquals(false, boundary.decidedAtCompletion, "at that boundary the owner was not decided")
                    assertEquals(true, boundary.handlesAtCompletion, "and both handles were owned")
                    val issued = assertNotNull(boundary.issued, "the handler issued the cancellation")
                    expectRoots(expectRecordedCancellation(settled(owner), issued), emptyList())   /* the recorded primary is that cancellation */
                }
            }
        expectNothingLeft(escaped)
        assertFalse(rig.videoEntry.entered || rig.audioEntry.entered, "the result was never consumed: no lane registered its wake")
        expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Add(AUDIO_FORMAT_CONFIG), Effect.Remove(v), Effect.Remove(a)), tracks.all())
    }

    /**
     * The session ends AT the child's completion boundary, after Ready is
     * built and before the coordinator consumes it: the Ready is owned and
     * retired, no lane is entered, and the outcome is the unfinished reason.
     */
    @Test
    fun o15_aTerminalAtTheCompletedChildsBoundaryRetiresTheReadyAndEntersNoLane() = runTest {
        val v = HostTrack("v")
        val a = HostTrack("a")
        val tracks = ScriptedTracks().answers(v).answers(a)
        val rig = Rig(tracks)
        var readyBuilt = false
        var terminatedAtCompletion = false
        val observing = Publication.Live(rig.owner) {
            val job = assertNotNull(currentCoroutineContext()[Job], "the preparation runs in its own job")
            job.invokeOnCompletion { cause ->
                if (cause == null) {
                    rig.terminate()
                    terminatedAtCompletion = true
                }
            }
            val prepared = rig.publication.prepare()
            readyBuilt = prepared is Preparation.Ready
            prepared
        }
        val escaped =
            escaping {
                withRun({ rig.run(observing) }) { owner ->
                    runCurrent()
                    assertTrue(readyBuilt, "Ready was built")
                    assertTrue(terminatedAtCompletion, "and the session ended at the child's completion")
                    assertIs<BroadcastOutcome.Unfinished>(settled(owner).getOrThrow())
                }
            }
        expectNothingLeft(escaped)
        assertFalse(rig.videoEntry.entered || rig.audioEntry.entered, "no lane registered its wake")
        expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Add(AUDIO_FORMAT_CONFIG), Effect.Remove(v), Effect.Remove(a)), tracks.all())
    }

    /**
     * Caller cancellation while the preparation is suspended in an ORDINARY
     * cancellable wait (the shape a format wait has), with no track added:
     * the only failure is that cancellation -- the child's recovered copy of
     * it is the same stopping, not a root -- and the owner is settled.
     */
    @Test
    fun o16_callerCancellationWhilePreparationAwaitsFormatIsTheOnlyFailure() = runTest {
        val tracks = ScriptedTracks()
        val rig = Rig(tracks)
        val entered = CompletableDeferred<Unit>()
        val format = CompletableDeferred<Preparation>()
        val waiting = Publication.Live(rig.owner) {
            entered.complete(Unit)
            format.await()
        }
        val escaped =
            escaping {
                withRun({ rig.run(waiting) }) { owner ->
                    runCurrent()
                    stillLive(owner)
                    withTimeoutOrNull(BOUNDED) { entered.await() } ?: throw AssertionError("the preparation never entered its wait")
                    val cause = owner.cancel("caller left while waiting for format")
                    runCurrent()
                    expectRoots(expectRecordedCancellation(settled(owner), cause), emptyList())
                    assertTrue(rig.owner.isDecided, "the owner was settled")
                }
            }
        expectNothingLeft(escaped)
        expectEffects(emptyList(), tracks.all())
        assertFalse(rig.videoEntry.entered || rig.audioEntry.entered, "no lane entered")
    }

    /** The same ordinary wait with ONE track already adopted: that track is retired exactly once and no second failure is invented. */
    @Test
    fun o17_callerCancellationWhileAwaitingFormatRetiresTheHeldTrackOnce() = runTest {
        val v = HostTrack("v")
        val tracks = ScriptedTracks().answers(v)
        val rig = Rig(tracks)
        val entered = CompletableDeferred<Unit>()
        val format = CompletableDeferred<Preparation>()
        val waiting = Publication.Live(rig.owner) {
            assertIs<AddOutcome.Added<HostTrack>>(rig.owner.add(MediaKind.Video, VIDEO_CONFIG))
            entered.complete(Unit)
            format.await()
        }
        val escaped =
            escaping {
                withRun({ rig.run(waiting) }) { owner ->
                    runCurrent()
                    stillLive(owner)
                    withTimeoutOrNull(BOUNDED) { entered.await() } ?: throw AssertionError("the preparation never entered its wait")
                    val cause = owner.cancel("caller left while waiting for format")
                    runCurrent()
                    expectRoots(expectRecordedCancellation(settled(owner), cause), emptyList())
                }
            }
        expectNothingLeft(escaped)
        expectEffects(listOf(Effect.Add(VIDEO_CONFIG), Effect.Remove(v)), tracks.all())
    }

    /**
     * Negative control: a child that raises its OWN cancellation -- a distinct
     * instance with no causal link to the caller's -- while the caller's
     * cancellation is settling stays a distinct retained root by identity.
     */
    @Test
    fun o18_aChildsOwnCancellationDuringCallerCancellationStaysADistinctRoot() = runTest {
        val hold = CompletableDeferred<Unit>()
        val own = CancellationException("the child stopped itself")
        val v = HostTrack("v")
        val tracks = ScriptedTracks().answers(v)
        val entered = tracks.entering()
        tracks.raises(own, hold)
        val rig = Rig(tracks)
        val escaped =
            escaping {
                withRun(rig::run) { owner ->
                    withHolds(listOf(hold)) {
                        runCurrent()
                        stillLive(owner)
                        withTimeoutOrNull(BOUNDED) { entered.await() } ?: throw AssertionError("the second add was never entered")
                        val cause = owner.cancel("the user left")
                        runCurrent()
                        hold.complete(Unit)
                        runCurrent()
                        val recorded = expectRecordedCancellation(settled(owner), cause)
                        expectRoots(recorded, listOf(own))   /* the child's own cancellation is its ONLY retained root, by identity */
                    }
                }
            }
        expectLeft(escaped, own)
        expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Add(AUDIO_FORMAT_CONFIG), Effect.Remove(v)), tracks.all())
    }

    /**
     * A child's OWN typed cancellation whose cause is the caller's
     * cancellation is a child cancelled BY what is leaving: the same stopping,
     * not a root -- and the same in both assertion modes, whether the frame
     * that is leaving holds the issued instance or its reconstruction.
     * (A child cancellation with NO causal link stays a root: o18.)
     */
    @Test
    fun o19_aChildsTypedCancellationWrappingTheCallersIsTheSameStoppingInBothModes() = runTest {
        val hold = CompletableDeferred<Unit>()
        val v = HostTrack("v")
        val tracks = ScriptedTracks().answers(v)
        val rig = Rig(tracks)
        val entered = CompletableDeferred<Unit>()
        var issued: CancellationException? = null
        var ownStop: HostChildStop? = null
        val waiting = Publication.Live(rig.owner) {
            assertIs<AddOutcome.Added<HostTrack>>(rig.owner.add(MediaKind.Video, VIDEO_CONFIG))
            entered.complete(Unit)
            withContext(NonCancellable) { hold.await() }
            throw HostChildStop("the child stopped, wrapping the caller's cancellation", assertNotNull(issued, "the caller had cancelled")).also { ownStop = it }
        }
        val escaped =
            escaping {
                withRun({ rig.run(waiting) }) { owner ->
                    withHolds(listOf(hold)) {
                        runCurrent()
                        stillLive(owner)
                        withTimeoutOrNull(BOUNDED) { entered.await() } ?: throw AssertionError("the preparation never entered its wait")
                        issued = owner.cancel("the user left")
                        runCurrent()
                        hold.complete(Unit)
                        runCurrent()
                        val recorded = expectRecordedCancellation(settled(owner), assertNotNull(issued))
                        assertNotNull(ownStop, "the child raised its wrapper")
                        expectRoots(recorded, emptyList())   /* the wrapper is the same stopping: zero roots, in -ea and -da alike */
                    }
                }
            }
        expectNothingLeft(escaped)
        expectEffects(listOf(Effect.Add(VIDEO_CONFIG), Effect.Remove(v)), tracks.all())
    }

    /** The terminal wins the race and the held add then raises an Error: the Error is the primary by identity. */
    @Test
    fun o7_aLateChildErrorIsThePrimaryEvenWhenTheTerminalWon() = runTest {
        val hold = CompletableDeferred<Unit>()
        val error = HostRemovalError("the second add raised an Error")
        val v = HostTrack("v")
        val tracks = ScriptedTracks().answers(v)
        val entered = tracks.entering()
        tracks.raises(error, hold)
        val rig = Rig(tracks)
        val escaped =
            escaping {
                withRun(rig::run) { owner ->
                    withHolds(listOf(hold)) {
                        runCurrent()
                        stillLive(owner)
                        withTimeoutOrNull(BOUNDED) { entered.await() } ?: throw AssertionError("the second add was never entered")
                        rig.terminate()
                        runCurrent()
                        hold.complete(Unit)
                        runCurrent()
                        expectRoots(expectRecorded(settled(owner), error), emptyList())   /* the child's Error is the recorded primary; the one removal succeeded */
                    }
                }
            }
        expectLeft(escaped, error)   /* an Error is never an outcome: it leaves the helper */
        expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Add(AUDIO_FORMAT_CONFIG), Effect.Remove(v)), tracks.all())
    }

    /** Caller cancellation plus a late child Error: the cancellation stays the primary INSTANCE; the Error is a retained root by identity. */
    @Test
    fun o7b_callerCancellationStaysPrimaryAndTheLateErrorIsARoot() = runTest {
        val hold = CompletableDeferred<Unit>()
        val error = HostRemovalError("the second add raised an Error")
        val v = HostTrack("v")
        val tracks = ScriptedTracks().answers(v)
        val entered = tracks.entering()
        tracks.raises(error, hold)
        val rig = Rig(tracks)
        val escaped =
            escaping {
                withRun(rig::run) { owner ->
                    withHolds(listOf(hold)) {
                        runCurrent()
                        stillLive(owner)
                        withTimeoutOrNull(BOUNDED) { entered.await() } ?: throw AssertionError("the second add was never entered")
                        val cause = owner.cancel("the user left")
                        runCurrent()
                        hold.complete(Unit)
                        runCurrent()
                        val recorded = expectRecordedCancellation(settled(owner), cause)   /* the caller's cancellation is the primary */
                        expectRoots(recorded, listOf(error))                  /* the Error is its ONLY retained root */
                    }
                }
            }
        expectLeft(escaped, error)   /* carried on the row's own cancellation, the Error is an unclaimed cleanup fact and leaves the helper */
        expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Add(AUDIO_FORMAT_CONFIG), Effect.Remove(v)), tracks.all())
    }

    @Test
    fun o8_aBusinessRefusalOnTheSecondAddRetiresTheFirstAndEntersNoLane() = runTest {
        val v = HostTrack("v")
        val tracks = ScriptedTracks().answers(v).refuses("the track name is busy: audio")
        val rig = Rig(tracks)
        val unfinished = assertIs<BroadcastOutcome.Unfinished>(rig.run())
        assertEquals("the track name is busy: audio", unfinished.reason)
        expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Add(AUDIO_FORMAT_CONFIG), Effect.Remove(v)), tracks.all())
        assertFalse(rig.videoEntry.entered || rig.audioEntry.entered, "no lane entered")
    }

    /** A failed terminal with NO Throwable primary: the typed removal failure escapes as itself; both attempted. */
    @Test
    fun o9_aRemovalFailureWithNoPrimaryEscapesAsItself() = runTest {
        val v = HostTrack("v")
        val a = HostTrack("a")
        val refused = HostRemovalRefused("the sender is closed")
        val tracks = ScriptedTracks().answers(v).answers(a)
        tracks.removeRaises[v] = refused
        val rig = Rig(tracks)
        val escaped =
            escaping {
                withRun(rig::run) { owner ->
                    runCurrent()
                    stillLive(owner)
                    rig.terminate()
                    runCurrent()
                    expectRoots(expectRecorded(settled(owner), refused), emptyList())   /* no primary: the first removal root is what the run recorded, alone */
                }
            }
        expectLeft(escaped, refused)
        expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Add(AUDIO_FORMAT_CONFIG), Effect.Remove(v), Effect.Remove(a)), tracks.all())   /* both attempted */
    }

    /** A drain failure is the Throwable primary; the removal failure is suppressed on it by identity; both attempted. */
    @Test
    fun o9b_aRemovalFailureIsSuppressedOnADrainFailurePrimary() = runTest {
        val v = HostTrack("v")
        val a = HostTrack("a")
        val writeFailed = HostWriteFailed("the video write failed")
        val refused = HostRemovalRefused("the sender is closed")
        val tracks = ScriptedTracks().answers(v).answers(a)
        tracks.writeRaises[v] = writeFailed
        tracks.removeRaises[v] = refused
        val rig = Rig(tracks)
        val escaped =
            escaping {
                withRun(rig::run) { owner ->
                    runCurrent()
                    stillLive(owner)
                    deliver(rig.video, 0, VIDEO_ANNEXB.copyOf(), 33_333L)
                    runCurrent()
                    val recorded = expectRecorded(settled(owner), writeFailed)   /* the drain failure is the primary instance */
                    expectRoots(recorded, listOf(refused))                        /* the removal failure is its ONLY root, by identity */
                }
            }
        expectLeft(escaped, writeFailed)
        expectEffects(
            listOf(
                Effect.Add(VIDEO_FORMAT_CONFIG),
                Effect.Add(AUDIO_FORMAT_CONFIG),
                Effect.Write(v, sendObject(VIDEO_AVC1, 33_333L, startsGroup = true), WRITE_TIMEOUT),
                Effect.Remove(v),
                Effect.Remove(a),
            ),
            tracks.all(),
        )
    }

    /** Refused after BOTH adds succeeded (a scripted preparer result, not codec validation); the first removal raises; both attempted. */
    @Test
    fun o10_aRefusalAfterBothAddsStillAttemptsBothRemovalsAndKeepsTheFailure() = runTest {
        val v = HostTrack("v")
        val a = HostTrack("a")
        val refused = HostRemovalRefused("the sender is closed")
        val tracks = ScriptedTracks().answers(v).answers(a)
        tracks.removeRaises[v] = refused
        val rig = Rig(tracks)
        val declined = Publication.Live(rig.owner) {
            check(rig.publication.prepare() is Preparation.Ready)
            Preparation.Refused("declined after both tracks were admitted")
        }
        val escaped =
            escaping {
                withRun({ rig.run(declined) }) { owner ->
                    runCurrent()
                    expectRoots(expectRecorded(settled(owner), refused), emptyList())
                }
            }
        expectLeft(escaped, refused)
        expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Add(AUDIO_FORMAT_CONFIG), Effect.Remove(v), Effect.Remove(a)), tracks.all())
        assertFalse(rig.videoEntry.entered || rig.audioEntry.entered, "no lane entered")
    }

    @Test
    fun o11_anAlreadyRemovedHandleIsSkippedByTheStateQuery() = runTest {
        val v = HostTrack("v")
        val a = HostTrack("a")
        val tracks = ScriptedTracks().answers(v).answers(a)
        val rig = Rig(tracks)
        val outcome =
            withRun(rig::run) { owner ->
                runCurrent()
                stillLive(owner)
                tracks.removedState.add(a)
                rig.terminate()
                runCurrent()
                settled(owner)
            }
        assertIs<BroadcastOutcome.Unfinished>(outcome.getOrThrow())
        expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Add(AUDIO_FORMAT_CONFIG), Effect.Remove(v)), tracks.all())   /* a skipped on the query, v attempted */
    }

    /** Capture cleanup raises an Error with both handles acquired: both removals are still attempted; every root leaves by identity. */
    @Test
    fun o12_aCaptureCleanupErrorDoesNotSkipEitherRemoval() = runTest {
        val v = HostTrack("v")
        val a = HostTrack("a")
        val stopError = HostStopError("the capture stop raised an Error")
        val tracks = ScriptedTracks().answers(v).answers(a)
        val rig = Rig(tracks, stopRaises = stopError)
        val escaped =
            escaping {
                withRun(rig::run) { owner ->
                    runCurrent()
                    stillLive(owner)
                    rig.terminate()
                    runCurrent()
                    expectRoots(expectRecorded(settled(owner), stopError), emptyList())   /* the capture-cleanup Error is the escaping root; both removals succeeded */
                }
            }
        expectLeft(escaped, stopError)
        expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Add(AUDIO_FORMAT_CONFIG), Effect.Remove(v), Effect.Remove(a)), tracks.all())   /* both attempted after the cleanup Error */
    }

    /** The first removal raises an Error at the coordinator level: the second is still attempted; the Error leaves by identity. */
    @Test
    fun o13_aFirstRemovalErrorAtTheCoordinatorStillAttemptsTheSecond() = runTest {
        val v = HostTrack("v")
        val a = HostTrack("a")
        val error = HostRemovalError("the first removal raised an Error")
        val tracks = ScriptedTracks().answers(v).answers(a)
        tracks.removeRaises[v] = error
        val rig = Rig(tracks)
        val escaped =
            escaping {
                withRun(rig::run) { owner ->
                    runCurrent()
                    stillLive(owner)
                    rig.terminate()
                    runCurrent()
                    expectRoots(expectRecorded(settled(owner), error), emptyList())
                }
            }
        expectLeft(escaped, error)
        expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Add(AUDIO_FORMAT_CONFIG), Effect.Remove(v), Effect.Remove(a)), tracks.all())
    }

    /** Caller cancellation during publish carrying a removal failure: the cancellation stays the primary instance; the failure is a root by identity. */
    @Test
    fun o14_aCancellationCarriesARemovalFailureAsARoot() = runTest {
        val v = HostTrack("v")
        val a = HostTrack("a")
        val refused = HostRemovalRefused("the sender is closed")
        val tracks = ScriptedTracks().answers(v).answers(a)
        tracks.removeRaises[v] = refused
        val rig = Rig(tracks)
        val escaped =
            escaping {
                withRun(rig::run) { owner ->
                    runCurrent()
                    stillLive(owner)
                    val cause = owner.cancel("the user left")
                    runCurrent()
                    val recorded = expectRecordedCancellation(settled(owner), cause)
                    expectRoots(recorded, listOf(refused))   /* the removal failure is the cancellation's ONLY root, by identity */
                }
            }
        expectLeft(escaped, refused)   /* the carried removal failure is the unclaimed fact that leaves the helper */
        expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Add(AUDIO_FORMAT_CONFIG), Effect.Remove(v), Effect.Remove(a)), tracks.all())
    }

}