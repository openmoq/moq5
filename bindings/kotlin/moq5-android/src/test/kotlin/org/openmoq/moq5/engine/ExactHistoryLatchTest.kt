package org.openmoq.moq5.engine

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaTrack
import org.openmoq.moq5.ReceiverTerminal
import org.openmoq.moq5.TrackDescription
import org.openmoq.moq5.TrackEvent
import org.openmoq.moq5.TrackEventHistoryOverflowException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * [ExactHistory]: the failure after the first gap is LATCHED, and one active
 * `next()` owns the consumer slot for its whole duration. RED-first.
 *
 * Source-derived, two defects:
 *  * `offer` keeps an event whenever a slot is free and only counts it
 *    otherwise (`:37`), so after the first gap a freed slot admits later
 *    events, and `takeLocked` delivers them before -- and, after the
 *    exception, after -- the overflow failure. The ruled contract: the
 *    pre-gap prefix is exact and complete; the first gap fails this stream
 *    permanently, independent of occupancy; the first rejected event and
 *    every later arrival are counted, never delivered, the witness
 *    saturating at `ULong.MAX_VALUE`; `next()` and a fresh `asFlow()`
 *    collection keep failing.
 *  * `offer` clears `waiter` when it dispatches a resumption (`:38`) and
 *    `next()` takes a queued value before it checks the waiter (`:55-56`),
 *    so a `next()` whose resumption is dispatched but not yet run no longer
 *    owns the slot: a second consumer is admitted and takes the event the
 *    first was resumed for. That violates the public single-consumer rule
 *    (`Receiver.kt`, `ExactStream`). The same shape exists between the two
 *    lock regions of `next()` (`:54-64`): after `takeLocked` finds nothing
 *    and before the continuation is registered, `waiter` is null, so a
 *    second `next()` passes the check too and the later registration
 *    overwrites the earlier continuation, which then never resumes. That
 *    window has no observable seam and is derived from source only; the
 *    dispatch window is demonstrated with the controlled dispatcher.
 */
internal class ExactHistoryLatchTest : EngineFixture() {
    private companion object {
        const val BOUND = 5_000L
        fun track(i: Int) = MediaTrack(i.toLong(), TrackDescription(name = MediaBytes("t$i".toByteArray())))
        fun added(i: Int) = TrackEvent.Added(track(i))
    }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    // -- the latch ----------------------------------------------------------------

    @Test
    fun `capacity two -- A B C offered, A consumed, D offered, B consumed - the next result is the overflow, never D`() =
        runTest {
            val h = ExactHistory(2)
            val a = added(1)
            val b = added(2)
            h.offer(a)
            h.offer(b)
            h.offer(added(3)) // the gap: C is counted, not kept
            assertEquals(a, withTimeout(BOUND) { h.next() }, "R7: the pre-gap prefix, A")
            h.offer(added(4)) // D arrives after the gap, while a slot is free
            assertEquals(b, withTimeout(BOUND) { h.next() }, "R7: the pre-gap prefix, B")
            val overflow = assertFailsWith<TrackEventHistoryOverflowException>("R7: after the pre-gap prefix the stream fails; a post-gap event must never be delivered") { withTimeout(BOUND) { h.next() } }
            assertEquals(2, overflow.capacity)
            assertEquals(2uL, overflow.dropped, "R7: C and D were both lost to the gap")
        }

    @Test
    fun `after the failure was observed, a later arrival is refused by next()`() =
        runTest {
            val h = ExactHistory(2)
            h.offer(added(1))
            h.offer(added(2))
            h.offer(added(3))
            withTimeout(BOUND) { h.next() }
            withTimeout(BOUND) { h.next() }
            assertFailsWith<TrackEventHistoryOverflowException>("R7: setup: the first failure") { withTimeout(BOUND) { h.next() } }
            h.offer(added(5)) // E, after the failure was observed
            val again = assertFailsWith<TrackEventHistoryOverflowException>("R7: the failure is latched; E must not be delivered by next()") { withTimeout(BOUND) { h.next() } }
            assertEquals(2uL, again.dropped, "R7: C and E are the events the stream did not deliver")
        }

    @Test
    fun `after the failure was observed, a fresh flow collection fails again without emitting the later arrival`() =
        runTest {
            val h = ExactHistory(2)
            h.offer(added(1))
            h.offer(added(2))
            h.offer(added(3))
            withTimeout(BOUND) { h.next() }
            withTimeout(BOUND) { h.next() }
            assertFailsWith<TrackEventHistoryOverflowException>("R7 recollection: setup: the first failure") { withTimeout(BOUND) { h.next() } }
            val e = added(5)
            h.offer(e)
            val emitted = mutableListOf<TrackEvent>()
            assertFailsWith<TrackEventHistoryOverflowException>("R7 recollection: the fresh collection fails") {
                withTimeout(BOUND) { h.asFlow().collect { emitted += it } }
            }
            assertEquals(emptyList(), emitted, "R7 recollection: the collection emitted a post-gap event before failing; the failure is latched and E is never delivered")
        }

    @Test
    fun `arrivals before the first exception is observed are all counted as dropped`() =
        runTest {
            val h = ExactHistory(2)
            h.offer(added(1))
            h.offer(added(2))
            h.offer(added(3)) // the gap
            assertTrue(withTimeout(BOUND) { h.next() } is TrackEvent.Added, "R7: A")
            h.offer(added(4))
            h.offer(added(5))
            assertTrue(withTimeout(BOUND) { h.next() } is TrackEvent.Added, "R7: B")
            val overflow = assertFailsWith<TrackEventHistoryOverflowException>("R7: the failure after the prefix") { withTimeout(BOUND) { h.next() } }
            assertEquals(3uL, overflow.dropped, "R7: C, D and E were all lost to the gap; the count is the exact witness of what the stream did not deliver")
        }

    @Test
    fun `the dropped count saturates at its boundary instead of wrapping`() {
        // The arithmetic the stream applies to every rejected arrival, at
        // the boundary a real stream cannot reach: no wrap to zero, so no
        // count can ever read as if nothing were lost.
        assertEquals(1uL, ExactHistory.saturatingIncrement(0uL))
        assertEquals(ULong.MAX_VALUE, ExactHistory.saturatingIncrement(ULong.MAX_VALUE - 1uL))
        assertEquals(ULong.MAX_VALUE, ExactHistory.saturatingIncrement(ULong.MAX_VALUE))
    }

    @Test
    fun `overflow outranks a later terminal`() =
        runTest {
            val h = ExactHistory(2)
            h.offer(added(1))
            h.offer(added(2))
            h.offer(added(3))
            h.close(ReceiverTerminal.Closed)
            assertTrue(withTimeout(BOUND) { h.next() } is TrackEvent.Added)
            assertTrue(withTimeout(BOUND) { h.next() } is TrackEvent.Added)
            assertFailsWith<TrackEventHistoryOverflowException>("R7 control: the gap is reported before the terminal") { withTimeout(BOUND) { h.next() } }
            assertFailsWith<TrackEventHistoryOverflowException>("R7 control: and stays reported; the clean terminal never replaces it") { withTimeout(BOUND) { h.next() } }
        }

    // -- the consumer reservation -------------------------------------------------

    @Test
    fun `a next whose resumption is dispatched but not yet run still owns the consumer slot`() =
        runTest {
            val h = ExactHistory(2)
            val dispatcher = ManualDispatcher()
            val scope = manualScope(dispatcher)
            run {
                val first = scope.async { h.next() }
                assertTrue(dispatcher.runOne(), "setup: the first caller starts")
                assertTrue(first.isActive, "setup: the first caller is suspended in next() on the empty history")
                assertEquals(0, dispatcher.queued(), "setup: no resumption yet")
                val a = added(41)
                h.offer(a)
                assertEquals(1, dispatcher.queued(), "setup: the first caller's resumption is dispatched and held")
                assertTrue(!first.isCompleted, "setup: the first next() has not returned")
                // A second consumer while the first next() is still active.
                val second = runCatching { withTimeout(BOUND) { h.next() } }
                assertTrue(
                    second.exceptionOrNull() is IllegalStateException,
                    "R7 reservation: a second consumer must be refused while the first next() is still active (the single-consumer rule); got $second -- the second consumer took the event the first was resumed for",
                )
                dispatcher.runAll()
                assertEquals(a, withTimeout(BOUND) { first.await() }, "R7 reservation: the first caller receives exactly the event it was resumed for")
                // The slot is free again once the first next() returned.
                h.offer(added(42))
                val next = withTimeout(BOUND) { h.next() }
                assertTrue(next is TrackEvent.Added && next.track.token == 42L, "R7 reservation: a consumer after the first returned is admitted; got $next")
            }
            // Retirement (cancel, service, completion and an empty queue) is the fixture's.
        }

    @Test
    fun `a consumer cancelled while parked frees the slot, and a replacement on the still-empty history receives the next event`() =
        runTest {
            val h = ExactHistory(2)
            val dispatcher = ManualDispatcher()
            val scope = manualScope(dispatcher)
            run {
                val first = scope.async { h.next() }
                assertTrue(dispatcher.runOne(), "setup: the first caller starts")
                assertTrue(first.isActive && dispatcher.queued() == 0, "setup: the first caller is parked in next() on the empty history")
                first.cancel()
                dispatcher.runAll()
                assertTrue(first.isCancelled && first.isCompleted, "setup: the first caller resolved as cancellation")
                // The replacement starts on the STILL EMPTY history: an omitted
                // waiter-cancellation cleanup refuses it by name here.
                val second = scope.async { h.next() }
                assertTrue(dispatcher.runOne(), "setup: the replacement starts")
                if (!second.isActive) {
                    fail("R7 cancellation: the replacement must be admitted and park on the empty history; got ${runCatching { second.await() }}")
                }
                val a = added(7)
                h.offer(a)
                assertEquals(1, dispatcher.queued(), "R7 cancellation: exactly the replacement's resumption is dispatched")
                dispatcher.runAll()
                assertEquals(a, withTimeout(BOUND) { second.await() }, "R7 cancellation: the replacement receives exactly A")
            }
            // Retirement (cancel, service, completion and an empty queue) is the fixture's.
        }
}
