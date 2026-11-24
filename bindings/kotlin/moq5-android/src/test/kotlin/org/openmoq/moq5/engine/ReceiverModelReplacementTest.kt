package org.openmoq.moq5.engine

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.EventHistoryPolicy
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.ReceiverTerminal
import org.openmoq.moq5.TrackDescription
import org.openmoq.moq5.TrackEvent
import org.openmoq.moq5.TrackPresence
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * How the receiver model folds an MSF-02 replacement in
 * (`media_receiver.h`: TRACK_REMOVED with `replaced_by`, then the
 * successor's TRACK_ADDED with `replaces`).
 *
 * Derived contract:
 *  * The REMOVED alone retires the old track (`Removed`, published as
 *    `Removed(replaced = true)`), moves its desired state to the successor's
 *    token -- as the C moves the selection -- and leaves the successor
 *    pending; nothing waits for the ADDED.
 *  * The successor's ADDED creates the new track and links both ways
 *    (`replaces` / `replacedBy`), in the same drain or a later one, and is
 *    published as `Added(successor, replaces = old)`.
 *  * A chain A -> B -> C links each step, and a command on any member reaches
 *    the newest ([ReceiverModel.chainHead]).
 *  * Retirement drops a pending successor: the old track keeps a null
 *    `replacedBy`.
 *  * An ADDED naming a predecessor that does not match a pending replacement
 *    is an anomaly; the new track is still integrated.
 *  * An advisory UPDATED carries its generation into `Updated`.
 */
internal class ReceiverModelReplacementTest {
    private companion object {
        const val A = -0x7fff_ffff_ffff_fefdL
        const val B = -0x7fff_ffff_ffff_fdfdL
        const val C = -0x7fff_ffff_ffff_fcfdL
        val BOUND = 5.seconds
        fun desc(name: String, codec: String, generation: UInt = 0u) =
            TrackDescription(name = MediaBytes(name.toByteArray()), codec = MediaBytes(codec.toByteArray()), configGeneration = generation)
        fun added(t: Long, d: TrackDescription, replaces: Long? = null) = BackendTrackEvent(BackendTrackEventKind.Added, t, d, replaces = replaces)
        fun removed(t: Long, replacedBy: Long? = null) = BackendTrackEvent(BackendTrackEventKind.Removed, t, null, replacedBy = replacedBy)
        fun updated(t: Long, d: TrackDescription, generation: UInt) = BackendTrackEvent(BackendTrackEventKind.Updated, t, d, configGeneration = generation)
    }

    private fun drain(model: ReceiverModel, count: Int): List<TrackEvent> =
        runBlocking { withTimeout(BOUND) { List(count) { checkNotNull(model.history).next()!! } } }

    @Test
    fun `a replacement links both tracks and moves the desired state with the REMOVED`() {
        val m = ReceiverModel(EventHistoryPolicy.Exact(16))
        m.integrate(added(A, desc("video", "av01")))
        m.setDesired(A, true)
        m.integrate(removed(A, replacedBy = B))
        val a = m.resolve(A)!!
        assertEquals(TrackPresence.Removed, a.presence, "the REMOVED alone retires the old handle")
        assertTrue(!m.isDesired(A) && m.isDesired(B), "the desire moved to the successor's token at the REMOVED")
        assertTrue(m.isReplaced(A) && !m.isReplaced(B))
        assertEquals(B, m.chainHead(A))
        m.integrate(added(B, desc("video", "avc1.64001f"), replaces = A))
        val b = m.resolve(B)!!
        assertTrue(a.replacedBy === b && b.replaces === a, "linked both ways")
        assertEquals(null, a.replaces)
        assertEquals(null, b.replacedBy)
        assertEquals(TrackPresence.Present, b.presence)
        assertEquals(listOf(a, b), m.inventory.value.tracks, "the old track is retained, the new one appended")
        assertEquals(0, m.inventory.value.anomalies)
        assertEquals(listOf(TrackEvent.Added(a), TrackEvent.Removed(a, replaced = true), TrackEvent.Added(b, replaces = a)), drain(m, 3))
    }

    @Test
    fun `a replacement split across two drains -- the REMOVED is published alone, and the ADDED completes the link later`() {
        val m = ReceiverModel(EventHistoryPolicy.Exact(16))
        m.integrate(added(A, desc("video", "av01")))
        m.integrate(removed(A, replacedBy = B))
        val a = m.resolve(A)!!
        val between = m.inventory.value
        assertEquals(listOf(a), between.tracks, "no successor track until its ADDED")
        assertEquals(null, a.replacedBy, "not linked yet")
        assertEquals(TrackPresence.Removed, a.presence)
        assertEquals(listOf(TrackEvent.Added(a), TrackEvent.Removed(a, replaced = true)), drain(m, 2), "the REMOVED is delivered without waiting for the ADDED")
        m.integrate(added(B, desc("video", "avc1.64001f"), replaces = A))
        val b = m.resolve(B)!!
        assertTrue(a.replacedBy === b && b.replaces === a)
        assertEquals(between.revision + 1uL, m.inventory.value.revision, "one revision for the ADDED")
        assertEquals(listOf(TrackEvent.Added(b, replaces = a)), drain(m, 1))
        assertEquals(0, m.inventory.value.anomalies)
    }

    @Test
    fun `a replacement chain links each step, carries the desire to the newest track, and commands reach the head`() {
        val m = ReceiverModel(EventHistoryPolicy.Exact(16))
        m.integrate(added(A, desc("video", "av01")))
        m.setDesired(A, true)
        m.integrate(removed(A, replacedBy = B))
        m.integrate(added(B, desc("video", "avc1.64001f"), replaces = A))
        m.integrate(removed(B, replacedBy = C))
        m.integrate(added(C, desc("video", "hvc1.1.6.L93.B0"), replaces = B))
        val (a, b, c) = listOf(A, B, C).map { m.resolve(it)!! }
        assertTrue(a.replacedBy === b && b.replaces === a && b.replacedBy === c && c.replaces === b)
        assertEquals(listOf(TrackPresence.Removed, TrackPresence.Removed, TrackPresence.Present), listOf(a, b, c).map { it.presence })
        assertEquals(listOf(C, C, C), listOf(A, B, C).map { m.chainHead(it) })
        assertTrue(!m.isDesired(A) && !m.isDesired(B) && m.isDesired(C), "the desire followed the chain")
        assertEquals(0, m.inventory.value.anomalies)
    }

    @Test
    fun `a track that was not wanted moves no desire, and a plain REMOVED is not a replacement`() {
        val m = ReceiverModel(EventHistoryPolicy.Exact(16))
        m.integrate(added(A, desc("video", "av01")))
        m.integrate(added(C, desc("audio", "opus")))
        m.integrate(removed(A, replacedBy = B))
        assertTrue(!m.isDesired(A) && !m.isDesired(B), "nothing was wanted, so nothing moved")
        m.integrate(removed(C))
        assertTrue(!m.isReplaced(C) && m.chainHead(C) == C, "a catalog removal names no successor")
        assertEquals(listOf(TrackEvent.Added(m.resolve(A)!!), TrackEvent.Added(m.resolve(C)!!), TrackEvent.Removed(m.resolve(A)!!, replaced = true), TrackEvent.Removed(m.resolve(C)!!)), drain(m, 4))
    }

    @Test
    fun `retirement with a pending replacement drops it -- the old track keeps a null replacedBy`() {
        val m = ReceiverModel(EventHistoryPolicy.Exact(16))
        m.integrate(added(A, desc("video", "av01")))
        m.integrate(removed(A, replacedBy = B))
        m.retire()
        val a = m.resolve(A)!!
        assertEquals(null, a.replacedBy)
        assertTrue(a.isRetired)
        assertEquals(ReceiverTerminal.Closed, m.inventory.value.terminal)
        m.integrate(added(B, desc("video", "avc1.64001f"), replaces = A))
        assertEquals(null, m.resolve(B), "nothing integrates after retirement")
        assertEquals(null, a.replacedBy)
    }

    @Test
    fun `an ADDED naming a predecessor without a matching replacement is an anomaly, and the new track still integrates`() {
        val m = ReceiverModel(EventHistoryPolicy.Disabled)
        m.integrate(added(A, desc("video", "av01")))
        m.integrate(added(B, desc("video", "avc1.64001f"), replaces = A))
        val a = m.resolve(A)!!
        val b = m.resolve(B)!!
        assertEquals(1, m.inventory.value.anomalies, "A was never removed as replaced by B")
        assertTrue(b.replaces === a && a.replacedBy === b, "the event's own link is kept")
        m.integrate(removed(B, replacedBy = C))
        m.integrate(added(C, desc("video", "av01")))
        assertEquals(2, m.inventory.value.anomalies, "a pending successor announced without its predecessor")
        assertEquals(null, m.resolve(C)!!.replaces)
    }

    @Test
    fun `an advisory UPDATED carries its generation, and the snapshot carries its own`() {
        val m = ReceiverModel(EventHistoryPolicy.Exact(16))
        m.integrate(added(A, desc("video", "av01")))
        m.integrate(updated(A, desc("video", "av01", generation = 2u), generation = 1u))
        val a = m.resolve(A)!!
        assertEquals(2u, a.description.configGeneration, "the snapshot taken at the drain may already be a later generation")
        assertEquals(listOf(TrackEvent.Added(a), TrackEvent.Updated(a, configGeneration = 1u)), drain(m, 2))
    }
}
