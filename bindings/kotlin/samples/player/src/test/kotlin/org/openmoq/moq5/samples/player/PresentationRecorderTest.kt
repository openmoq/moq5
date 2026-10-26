package org.openmoq.moq5.samples.player

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * The acceptance observer: the per-Activity first-lease guard, the UI to
 * renderer handoff, the marker policy, the settlement clock and the sticky
 * freeze. Time is the test scheduler's virtual time, offset so the monotonic
 * domain is far from the media-relative texture timestamps.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class PresentationRecorderTest {
    private companion object {
        const val MONOTONIC_BASE_NS = 9_000_000_000_000L
        const val MS = 1_000_000L
        const val FINAL = 59
        const val RELEASED = 60
    }

    private fun ok(i: Int) = PlaybackMarkerReading(PlaybackMarkerOutcome.Ok, i)
    private fun of(o: PlaybackMarkerOutcome) = PlaybackMarkerReading(o, null)

    /** One Activity: its registry, its guard, one texture and a recorder on the virtual clock. */
    private class Rig(scope: TestScope?, capacity: Int = 512) {
        var manualNs = MONOTONIC_BASE_NS
        val nowNs: () -> Long = if (scope == null) ({ manualNs }) else ({ MONOTONIC_BASE_NS + scope.testScheduler.currentTime * MS })
        val registry = PlaybackDisplayRegistry<Any, Any>({ Any() }, {}, {})
        val gate = PresentationObserverGate()
        val texture = Any()
        init { registry.available(texture) }
        val lease: PlaybackDisplayLease<Any, Any> = registry.acquire()
        val recorder = PresentationRecorder(nowNs, FINAL, capacity).also { it.open(lease, gate.admit(lease)) }
        var ts = 2_400_000_000L                      // media-relative, as SurfaceTexture reports it
        fun offer(r: PlaybackMarkerReading, texture: Any = this.texture, id: Long? = registry.leaseIdFor(texture)): Boolean {
            ts += 41_666_000
            return recorder.offer(texture, id, PresentationFact(r, ts, nowNs()))
        }
    }

    private fun Rig.passing() { offer(ok(0)); offer(ok(30)); offer(ok(FINAL)) }

    // ---------------------------------------------------------------- 2. one-shot first-lease guard

    @Test fun firstLeaseOfAFreshActivityArmsAndCanPass() {
        val r = Rig(null)
        r.passing()
        val s = r.recorder.freeze("settled", r.nowNs(), RELEASED)
        assertTrue(s.pass, "refusal ${s.refusal}")
        assertEquals(listOf(0, 30, FINAL), s.indices)
        assertEquals(1, s.ordinal)
        assertEquals(r.lease.leaseId, s.leaseId)
    }

    @Test fun reannouncedSameTextureRefusesTheNewObserverAndTheStaleCallback() {
        val r = Rig(null)
        val staleId = r.registry.leaseIdFor(r.texture)
        val stale = PresentationFact(ok(FINAL), 2_458_000_000L, r.nowNs())   // captured under lease 1, not yet delivered
        r.recorder.retire()
        r.lease.close()
        r.recorder.close()
        assertTrue(r.registry.destroyed(r.texture))
        r.registry.available(r.texture)                                     // the SAME identity re-announced
        val again = r.registry.acquire()
        assertEquals(1, again.ordinal, "the entry ordinal reset: it alone cannot refuse this lease")
        val next = PresentationRecorder(r.nowNs, FINAL)
        assertFalse(r.gate.admit(again), "the Activity already observed its first lease")
        next.open(again, admitted = false)
        assertFalse(next.offer(r.texture, staleId, stale), "a lease-1 callback never counts under lease 2")
        assertFalse(next.offer(r.texture, again.leaseId, PresentationFact(ok(FINAL), 2_500_000_000L, r.nowNs())))
        val s = next.freeze("settled", r.nowNs(), RELEASED)
        assertFalse(s.pass)
        assertEquals("observer-refused:reused-surface", s.refusal)
        assertEquals(emptyList(), s.indices)
        assertTrue(next.wake.isCompleted, "a refused observer wakes the renderer rather than waiting out the window")
    }

    @Test fun secondLeaseOfTheSameEntryIsRefused() {
        val r = Rig(null)
        r.lease.close()
        val second = r.registry.acquire()
        assertEquals(2, second.ordinal)
        assertFalse(r.gate.admit(second))
    }

    @Test fun aNewTextureLaterInTheSameActivityIsAlsoRefused() {
        val r = Rig(null)
        r.lease.close()
        assertTrue(r.registry.destroyed(r.texture))
        val t2 = Any()
        r.registry.available(t2)
        val fresh = r.registry.acquire()
        assertEquals(1, fresh.ordinal)
        assertFalse(r.gate.admit(fresh), "the guard is per Activity and irreversible, not per texture")
    }

    @Test fun aFreshFirstSurfaceInAFreshActivityArms() {
        val registry = PlaybackDisplayRegistry<Any, Any>({ Any() }, {}, {})
        val t = Any()
        registry.available(t)
        assertTrue(PresentationObserverGate().admit(registry.acquire()))
    }

    // ---------------------------------------------------------------- 4. handoff identity, bounds, never waiting

    @Test fun onlyTheBoundTextureAndLeaseIdAreAccepted() {
        val r = Rig(null)
        val id = r.lease.leaseId
        assertFalse(r.offer(ok(0), texture = Any(), id = id), "wrong texture, current id")
        assertFalse(r.offer(ok(0), id = id + 1), "right texture, other id")
        assertFalse(r.offer(ok(0), id = null), "right texture, no lease")
        val unopened = PresentationRecorder(r.nowNs, FINAL)
        assertFalse(unopened.offer(r.texture, id, PresentationFact(ok(0), 1, r.nowNs())), "before open")
        assertTrue(r.offer(ok(0)))
        val s = r.recorder.freeze("settled", r.nowNs(), RELEASED)
        assertEquals(listOf(0), s.indices)
        assertEquals(1, s.latched)
        assertEquals(3, r.recorder.close().ignoredForeign)
    }

    @Test fun offerIsBoundedAndOverflowIsANamedStickyRefusal() {
        val r = Rig(null, capacity = 4)
        repeat(4) { assertTrue(r.offer(of(PlaybackMarkerOutcome.Blank))) }
        assertFalse(r.offer(ok(0)), "the fifth fact exceeds the bound")
        assertTrue(r.recorder.wake.isCompleted)
        assertFalse(r.offer(ok(FINAL)))
        val s = r.recorder.freeze("settled", r.nowNs(), RELEASED)
        assertEquals("overflowed", s.refusal)
        assertEquals(emptyList(), s.indices)
    }

    @Test fun offerNeverWaitsForTheRendererAndTheFinalFrameWakesIt() = runTest {
        val r = Rig(this)
        val t0 = r.nowNs()
        val settled = async { r.recorder.settle(t0, RELEASED) }
        runCurrent()
        assertFalse(settled.isCompleted)
        // Called from the test body (not a coroutine step): each returns while the renderer is suspended.
        assertTrue(r.offer(ok(0)))
        assertTrue(r.offer(ok(FINAL)))
        runCurrent()
        assertTrue(settled.isCompleted, "the final frame wakes the renderer at once")
        assertEquals(0L, testScheduler.currentTime)
        assertTrue(settled.await().pass)
    }

    @Test fun retireDuringTheAwaitWakesItBeforeTheWindow() = runTest {
        val r = Rig(this)
        r.offer(ok(0))
        val settled = async { r.recorder.settle(r.nowNs(), RELEASED) }
        advanceTimeBy(500); runCurrent()
        assertFalse(settled.isCompleted)
        r.recorder.retire()
        runCurrent()
        assertTrue(settled.isCompleted)
        assertEquals(500L, testScheduler.currentTime)
        val s = settled.await()
        assertEquals("display-retired", s.refusal)
        assertSame(s, r.recorder.freeze("again", r.nowNs(), RELEASED), "freeze happens once")
    }

    @Test fun cancellationFreezesOnceWithReasonCancelledBeforeTheRendererCloses() = runTest {
        val c = Rig(this)
        c.offer(ok(0))
        var atRendererClose: PresentationSnapshot? = null
        val pending = launch {
            // The caller's finally stands for the renderer close that follows settlement.
            try { c.recorder.settle(c.nowNs(), RELEASED) } finally { atRendererClose = c.recorder.freeze("probe", c.nowNs(), RELEASED) }
        }
        runCurrent()
        assertFalse(pending.isCompleted)
        pending.cancel(CancellationException("caller stop")); pending.join()
        val s = checkNotNull(atRendererClose)
        assertEquals("cancelled", s.freezeReason, "settlement froze before the renderer close ran")
        assertEquals("cancelled", s.refusal)
        assertSame(s, c.recorder.freeze("again", c.nowNs(), RELEASED))
    }

    @Test fun lateCallbacksAfterFreezeChangeNeitherSnapshotNorLine() {
        val r = Rig(null)
        r.passing()
        val s = r.recorder.freeze("settled", r.nowNs(), RELEASED)
        val copy = s.copy()
        val line = s.line()
        repeat(4) { assertFalse(r.offer(ok(it + 1))) }
        assertFalse(r.offer(ok(FINAL)))
        assertEquals(copy, s)
        assertEquals(line, s.line())
        assertSame(s, r.recorder.freeze("again", r.nowNs(), RELEASED))
        assertFalse("lateAfterFreeze" in line, "post-freeze diagnostics are never part of acceptance")
        val closed = r.recorder.close()
        assertEquals(5, closed.lateAfterFreeze)
        assertEquals("observer-closed: lateAfterFreeze=5 ignoredForeign=0 retiredAfterFreeze=0", closed.line())
        assertEquals(line, s.line())
    }

    // ---------------------------------------------------------------- 1. the two receipts' exact grammar

    private val acceptanceKeys = listOf("pass", "released", "latched", "distinctTs", "indices", "preVideo", "unreadable",
        "syncInvalid", "complementInvalid", "captureRefusals", "ordered", "settled", "ordinal", "leaseId", "refusal", "freezeReason")

    /** Parses `<prefix> k=v k=v …` strictly: exact prefix, single spaces, no empty or repeated key, no '=' in a value. */
    private fun fields(line: String, prefix: String): LinkedHashMap<String, String> {
        assertTrue(line.startsWith("$prefix "), "prefix: $line")
        val out = LinkedHashMap<String, String>()
        for (pair in line.removePrefix("$prefix ").split(' ')) {
            val parts = pair.split('=')
            assertEquals(2, parts.size, "malformed pair '$pair' in $line")
            assertTrue(parts[0].isNotEmpty() && parts[1].isNotEmpty(), "empty key or value in '$pair'")
            assertTrue(out.put(parts[0], parts[1]) == null, "repeated key ${parts[0]}")
        }
        return out
    }

    private fun assertReceipt(expected: List<Pair<String, String>>, line: String) {
        val got = fields(line, "presented:")
        assertEquals(acceptanceKeys, got.keys.toList(), "acceptance key inventory and order")
        for ((key, value) in expected) assertEquals(value, got[key], "field $key")
        assertEquals(expected.map { it.first }, acceptanceKeys, "the test declares every field")
    }

    @Test fun passingReceiptHasExactlyTheDeclaredFields() {
        val r = Rig(null)
        r.offer(of(PlaybackMarkerOutcome.Blank)); r.offer(of(PlaybackMarkerOutcome.Unreadable))
        r.offer(ok(0)); r.offer(ok(30)); r.offer(ok(FINAL))
        val s = r.recorder.freeze("settled", r.nowNs(), RELEASED)
        assertReceipt(listOf("pass" to "true", "released" to "60", "latched" to "5", "distinctTs" to "3", "indices" to "[0,30,59]",
            "preVideo" to "2", "unreadable" to "1", "syncInvalid" to "0", "complementInvalid" to "0", "captureRefusals" to "0",
            "ordered" to "true", "settled" to "true", "ordinal" to "1", "leaseId" to "1", "refusal" to "none", "freezeReason" to "settled"), s.line())
        assertEquals("presented: pass=true released=60 latched=5 distinctTs=3 indices=[0,30,59] preVideo=2 unreadable=1 syncInvalid=0 " +
            "complementInvalid=0 captureRefusals=0 ordered=true settled=true ordinal=1 leaseId=1 refusal=none freezeReason=settled", s.line())
    }

    @Test fun refusedReceiptsNameTheirCodeAndEmptyIndices() {
        val g = Rig(null)
        g.offer(of(PlaybackMarkerOutcome.SyncInvalid))
        assertEquals("presented: pass=false released=60 latched=1 distinctTs=0 indices=[] preVideo=0 unreadable=0 syncInvalid=1 " +
            "complementInvalid=0 captureRefusals=0 ordered=true settled=false ordinal=1 leaseId=1 refusal=geometry-refused freezeReason=settled",
            g.recorder.freeze("settled", g.nowNs(), RELEASED).line())
        val c = Rig(null)
        c.offer(ok(0)); c.offer(of(PlaybackMarkerOutcome.CaptureNull))
        assertReceipt(listOf("pass" to "false", "released" to "59", "latched" to "2", "distinctTs" to "1", "indices" to "[0]",
            "preVideo" to "0", "unreadable" to "0", "syncInvalid" to "0", "complementInvalid" to "0", "captureRefusals" to "1",
            "ordered" to "true", "settled" to "false", "ordinal" to "1", "leaseId" to "1", "refusal" to "capture-refused", "freezeReason" to "settled"),
            c.recorder.freeze("settled", c.nowNs(), 59).line())
        val k = Rig(null)
        k.offer(of(PlaybackMarkerOutcome.ComplementInvalid))
        val refused = k.recorder.freeze("cancelled", k.nowNs(), 0)
        assertEquals("1", fields(refused.line(), "presented:")["complementInvalid"])
        assertEquals("geometry-refused", fields(refused.line(), "presented:")["refusal"])
        assertEquals("cancelled", fields(refused.line(), "presented:")["freezeReason"])
        val o = Rig(null)
        o.recorder.retire()
        assertEquals("display-retired", fields(o.recorder.freeze("settled", o.nowNs(), RELEASED).line(), "presented:")["refusal"])
    }

    @Test fun observerRefusalCodeHasNoSpace() {
        val r = Rig(null)
        r.lease.close()
        val again = r.registry.acquire()
        val next = PresentationRecorder(r.nowNs, FINAL)
        next.open(again, r.gate.admit(again))
        val line = next.freeze("settled", r.nowNs(), RELEASED).line()
        assertEquals("observer-refused:reused-surface", fields(line, "presented:")["refusal"])
        assertEquals("2", fields(line, "presented:")["ordinal"])
    }

    @Test fun closedReceiptHasExactlyItsThreeFieldsAndNeverAcceptanceOnes() {
        val r = Rig(null)
        r.passing()
        r.recorder.freeze("settled", r.nowNs(), RELEASED)
        r.recorder.retire()
        r.offer(ok(1))
        r.offer(ok(2), texture = Any())
        val line = r.recorder.close().line()
        assertEquals(listOf("lateAfterFreeze", "ignoredForeign", "retiredAfterFreeze"), fields(line, "observer-closed:").keys.toList())
        assertEquals("observer-closed: lateAfterFreeze=1 ignoredForeign=1 retiredAfterFreeze=1", line)
    }

    // ---------------------------------------------------------------- 5. marker policy

    private fun verdict(vararg readings: PlaybackMarkerReading, released: Int = RELEASED): PresentationSnapshot {
        val r = Rig(null)
        readings.forEach { r.offer(it) }
        return r.recorder.freeze("settled", r.nowNs(), released)
    }

    @Test fun blankAndUnreadableBeforeTheFirstValidIndexAreTolerated() {
        val s = verdict(of(PlaybackMarkerOutcome.Blank), of(PlaybackMarkerOutcome.Unreadable), ok(0), ok(FINAL))
        assertTrue(s.pass, "refusal ${s.refusal}")
        assertEquals(2, s.preVideo)
        assertEquals(listOf(0, FINAL), s.indices)
        assertEquals(2, s.distinctTs)
        assertTrue(s.ordered && s.settled)
    }

    @Test fun blankPlusFinalFrameAloneCannotSettleAcceptance() {
        val s = verdict(of(PlaybackMarkerOutcome.Blank), ok(FINAL))
        assertFalse(s.pass)
        assertEquals("too-few-indices", s.refusal)
    }

    @Test fun missingFinalFrameCannotSettle() {
        assertEquals("final-frame-missing", verdict(ok(0), ok(58)).refusal)
    }

    @Test fun noValidMarkerAtAllIsAFailClosedRefusal() {
        assertEquals("no-valid-marker", verdict(of(PlaybackMarkerOutcome.Blank)).refusal)
        assertEquals("no-valid-marker", verdict(*Array(5) { of(PlaybackMarkerOutcome.Unreadable) }).refusal, "y-shifted marker")
    }

    @Test fun geometryRefusalIsImmediateAndDiscardsIndices() {
        for (bad in listOf(PlaybackMarkerOutcome.SyncInvalid, PlaybackMarkerOutcome.ComplementInvalid)) {
            val r = Rig(null)
            r.offer(of(bad))
            assertTrue(r.recorder.wake.isCompleted, "$bad wakes at once")
            r.offer(ok(0)); r.offer(ok(FINAL))
            val s = r.recorder.freeze("settled", r.nowNs(), RELEASED)
            assertEquals("geometry-refused", s.refusal, "$bad")
            assertEquals(emptyList(), s.indices, "a shifted-only run never becomes an accepted partial run")
        }
    }

    @Test fun captureRefusalIsImmediate() {
        for (bad in listOf(PlaybackMarkerOutcome.CaptureNull, PlaybackMarkerOutcome.CaptureSize, PlaybackMarkerOutcome.CaptureEmpty)) {
            val r = Rig(null)
            r.offer(ok(0))
            r.offer(of(bad))
            assertTrue(r.recorder.wake.isCompleted)
            assertEquals("capture-refused", r.recorder.freeze("settled", r.nowNs(), RELEASED).refusal, "$bad")
        }
    }

    @Test fun invalidAfterTheFirstValidIndexIsRefused() {
        assertEquals("post-valid-invalid", verdict(ok(3), of(PlaybackMarkerOutcome.Blank), ok(FINAL)).refusal)
        assertEquals("post-valid-invalid", verdict(ok(3), of(PlaybackMarkerOutcome.Unreadable), ok(FINAL)).refusal)
    }

    @Test fun orderingDistinctnessAndReleaseCountAreEachAcceptanceMisses() {
        assertEquals("unordered", verdict(ok(3), ok(2), ok(FINAL)).refusal)
        val r = Rig(null)
        r.offer(ok(0))
        r.recorder.offer(r.texture, r.lease.leaseId, PresentationFact(ok(FINAL), r.ts - 1, r.nowNs()))
        assertEquals("unordered", r.recorder.freeze("settled", r.nowNs(), RELEASED).refusal, "decreasing texture timestamp")
        val d = Rig(null)
        d.recorder.offer(d.texture, d.lease.leaseId, PresentationFact(ok(0), 5, d.nowNs()))
        d.recorder.offer(d.texture, d.lease.leaseId, PresentationFact(ok(FINAL), 5, d.nowNs()))
        val dup = d.recorder.freeze("settled", d.nowNs(), RELEASED)
        assertEquals(1, dup.distinctTs)
        assertEquals("too-few-timestamps", dup.refusal)
        assertEquals("released-mismatch", verdict(ok(0), ok(FINAL), released = 59).refusal)
    }

    // ---------------------------------------------------------------- 6. settlement clock and sticky freeze

    @Test fun settlementIsMonotonicFromDecoderEosNeverMediaTime() {
        // Media-relative release/texture time about 2.5 s; monotonic t0 at 9e12 ns.
        val r = Rig(null)
        r.offer(ok(0))
        val t0 = r.nowNs()
        r.manualNs = t0 + 1_500 * MS
        r.offer(ok(FINAL))
        r.manualNs = t0 + 1_600 * MS
        val s = r.recorder.freeze("settled", t0, RELEASED)
        assertTrue(s.settled && s.pass, "refusal ${s.refusal}")
        val late = Rig(null)
        late.offer(ok(0))
        val t1 = late.nowNs()
        late.manualNs = t1 + 3_200 * MS
        late.offer(ok(FINAL))
        val l = late.recorder.freeze("settled", t1, RELEASED)
        assertFalse(l.settled)
        assertEquals("final-frame-late", l.refusal)
    }

    @Test fun aFinalFrameLatchedBeforeDecoderEosCounts() {
        val r = Rig(null)
        r.offer(ok(0)); r.offer(ok(FINAL))
        r.manualNs += 700 * MS
        assertTrue(r.recorder.freeze("settled", r.nowNs(), RELEASED).pass)
    }

    @Test fun settlementWaitsExactlyTheWindowAfterDecoderEos() = runTest {
        val r = Rig(this)
        r.offer(ok(0))
        val settled = async { r.recorder.settle(r.nowNs(), RELEASED) }
        advanceTimeBy(2_999); runCurrent()
        assertFalse(settled.isCompleted)
        advanceTimeBy(2); runCurrent()
        assertTrue(settled.isCompleted)
        assertEquals("final-frame-missing", settled.await().refusal)
    }

    @Test fun finalThenRetireThenFreezeFails() {
        val r = Rig(null)
        r.passing()
        r.recorder.retire()
        val s = r.recorder.freeze("settled", r.nowNs(), RELEASED)
        assertEquals("display-retired", s.refusal)
        assertSame(s, r.recorder.freeze("again", r.nowNs(), RELEASED))
    }

    @Test fun retireThenFinalThenFreezeFails() {
        val r = Rig(null)
        r.offer(ok(0))
        r.recorder.retire()
        r.offer(ok(FINAL))
        assertEquals("display-retired", r.recorder.freeze("settled", r.nowNs(), RELEASED).refusal)
    }

    @Test fun finalThenInvalidThenFreezeFails() {
        val r = Rig(null)
        r.passing()
        r.offer(of(PlaybackMarkerOutcome.SyncInvalid))
        assertEquals("geometry-refused", r.recorder.freeze("settled", r.nowNs(), RELEASED).refusal)
        val b = Rig(null)
        b.passing()
        b.offer(of(PlaybackMarkerOutcome.Blank))
        assertEquals("post-valid-invalid", b.recorder.freeze("settled", b.nowNs(), RELEASED).refusal)
    }

    @Test fun finalThenOverflowThenFreezeFails() {
        val r = Rig(null, capacity = 3)
        r.passing()
        assertFalse(r.offer(ok(FINAL)))
        assertEquals("overflowed", r.recorder.freeze("settled", r.nowNs(), RELEASED).refusal)
    }

    @Test fun finalThenFreezeThenRetireStaysPassAndCountsOnlyInTheClosedReceipt() {
        val r = Rig(null)
        r.passing()
        val s = r.recorder.freeze("settled", r.nowNs(), RELEASED)
        assertTrue(s.pass)
        r.recorder.retire()
        assertSame(s, r.recorder.freeze("again", r.nowNs(), RELEASED))
        assertTrue(s.pass)
        assertEquals(1, r.recorder.close().retiredAfterFreeze)
    }

    @Test fun theFirstFailureWinsOverEveryLaterOne() = runTest {
        val g = Rig(this)
        g.offer(ok(0))
        g.offer(of(PlaybackMarkerOutcome.SyncInvalid))
        g.recorder.retire()
        assertEquals("geometry-refused", g.recorder.freeze("settled", g.nowNs(), RELEASED).refusal)
        val c = Rig(this)
        c.offer(ok(0))
        c.recorder.retire()
        var frozen: PresentationSnapshot? = null
        val job = launch { try { c.recorder.settle(c.nowNs() + 60_000 * MS, RELEASED) } finally { frozen = c.recorder.freeze("probe", c.nowNs(), RELEASED) } }
        runCurrent()
        job.cancel(); job.join()
        val s = checkNotNull(frozen)
        assertEquals("display-retired", s.refusal, "a later cancellation does not replace the first failure")
    }

    @Test fun theWakeKeepsItsFirstReason() {
        val r = Rig(null)
        r.passing()
        r.recorder.retire()
        assertEquals("final", r.recorder.wake.getCompleted())
        val q = Rig(null)
        q.recorder.retire()
        q.offer(ok(FINAL))
        assertEquals("display-retired", q.recorder.wake.getCompleted())
        assertNotEquals(r.recorder.wake.getCompleted(), q.recorder.wake.getCompleted())
    }
}
