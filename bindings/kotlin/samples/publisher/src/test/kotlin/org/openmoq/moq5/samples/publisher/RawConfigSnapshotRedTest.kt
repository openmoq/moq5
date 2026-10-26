package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException

/**
 * A raw configuration capture: borrowed, opaque units copied into one owned,
 * immutable snapshot under explicit count, unit and aggregate bounds, with
 * every length and the aggregate checked before any unit is read.
 *
 * `g` rows measure this file's own scripted sources. `s` rows measure the
 * real capture -- through [ConfigByteSource] for the ordering and failure
 * rows, and through real heap, direct and read-only [ByteBuffer]s for the
 * adapter rows. Synthetic lengths are ARITHMETIC tests: their sources hold
 * no payload and refuse to be read, so an implementation that reads or
 * allocates before refusing fails by name before any allocation.
 */
class RawConfigSnapshotRedTest {

    /** A scripted unit: every length query and read is logged in order; a synthetic one refuses to be read. */
    private class Scripted(
        private val name: String,
        private val declared: Int,
        private val bytes: ByteArray?,
        private val log: MutableList<String>,
        private val raise: Throwable? = null,
    ) : ConfigByteSource {
        override val length: Int
            get() {
                log.add("len:$name")
                return declared
            }

        override fun read(length: Int): ByteArray {
            log.add("read:$name")
            raise?.let { throw it }
            val payload = bytes ?: throw AssertionError("the synthetic unit $name was read: a capture must refuse before reading or allocating")
            assertEquals(declared, length, "read with the validated length")
            return payload.copyOf()
        }
    }

    private fun units(log: MutableList<String>, vararg payloads: ByteArray): List<ConfigByteSource> =
        payloads.mapIndexed { i, p -> Scripted("u$i", p.size, p, log) }

    private fun bytes(tag: Int, size: Int) = ByteArray(size) { ((tag shl 4) + it).toByte() }

    private fun taken(result: RawSnapshotResult): RawConfigSnapshot = assertIs<RawSnapshotResult.Taken>(result, "taken").snapshot

    private fun refused(result: RawSnapshotResult, reason: String) = assertEquals(reason, assertIs<RawSnapshotResult.Refused>(result, "refused").reason)

    private fun expectUnits(expected: List<ByteArray>, snapshot: RawConfigSnapshot) {
        assertEquals(expected.size, snapshot.unitCount, "unit count")
        expected.forEachIndexed { i, e -> assertContentEquals(e, snapshot.unit(i), "unit $i, exact bytes and boundary") }
        assertEquals(expected.sumOf { it.size }, snapshot.totalBytes)
    }

    // ---- g: this file's own scripted sources ---------------------------------------

    @Test
    fun g1_theScriptedSourceLogsEveryQueryAndTheSyntheticOneRefusesToBeRead() {
        val log = mutableListOf<String>()
        val real = Scripted("a", 2, byteArrayOf(1, 2), log)
        assertEquals(2, real.length)
        assertContentEquals(byteArrayOf(1, 2), real.read(2))
        val synthetic = Scripted("huge", Int.MAX_VALUE, null, log)
        assertEquals(Int.MAX_VALUE, synthetic.length)
        assertFailsWith<AssertionError> { synthetic.read(Int.MAX_VALUE) }
        assertEquals(listOf("len:a", "read:a", "len:huge", "read:huge"), log)
    }

    @Test
    fun g2_theRaisingSourceRaisesItsDeclaredInstance() {
        val log = mutableListOf<String>()
        val failure = IllegalStateException("declared")
        val raising = Scripted("r", 1, byteArrayOf(1), log, raise = failure)
        assertTrue(runCatching { raising.read(1) }.exceptionOrNull() === failure)
        assertEquals(listOf("read:r"), log)
    }

    // ---- s: the capture ------------------------------------------------------------

    @Test
    fun s0_everyNonPositiveLimitIsAnArgumentError() {
        for ((units, unit, total) in listOf(Triple(0, 1, 1), Triple(1, 0, 1), Triple(1, 1, 0), Triple(-1, 1, 1), Triple(1, -1, 1), Triple(1, 1, -1))) {
            assertFailsWith<IllegalArgumentException>("bounds $units/$unit/$total") { ConfigBounds(units, unit, total) }
        }
    }

    @Test
    fun s1_zeroOrTooManyUnitsAreRefusedBeforeAnyLengthIsRead() {
        val log = mutableListOf<String>()
        refused(RawConfigSnapshots.captureFrom(emptyList(), ConfigBounds(2, 8, 16)), "no configuration units")
        refused(RawConfigSnapshots.captureFrom(units(log, bytes(1, 1), bytes(2, 1), bytes(3, 1)), ConfigBounds(2, 8, 16)), "3 configuration units exceed the limit of 2")
        assertEquals(emptyList(), log, "nothing was queried")
    }

    @Test
    fun s2_anEmptyOrOversizedLaterUnitIsRefusedBeforeAnyUnitIsRead() {
        val log = mutableListOf<String>()
        val oversized = RawConfigSnapshots.captureFrom(units(log, bytes(1, 4), bytes(2, 4), bytes(3, 9)), ConfigBounds(4, 8, 64))
        refused(oversized, "configuration unit 2 of 9 bytes exceeds the limit of 8")
        assertEquals(listOf("len:u0", "len:u1", "len:u2"), log, "every length, then nothing read: the valid earlier units were not copied")
        log.clear()
        val empty = RawConfigSnapshots.captureFrom(units(log, bytes(1, 4), ByteArray(0)), ConfigBounds(4, 8, 64))
        refused(empty, "configuration unit 1 is empty")
        assertEquals(listOf("len:u0", "len:u1"), log)
    }

    @Test
    fun s3_anAggregateOverTheLimitIsRefusedBeforeAnyUnitIsRead() {
        val log = mutableListOf<String>()
        refused(RawConfigSnapshots.captureFrom(units(log, bytes(1, 6), bytes(2, 6), bytes(3, 6)), ConfigBounds(4, 8, 17)), "18 configuration bytes exceed the limit of 17")
        assertEquals(listOf("len:u0", "len:u1", "len:u2"), log)
    }

    @Test
    fun s4_exactLimitsAreTakenAndOneAboveEachIsRefused() {
        val log = mutableListOf<String>()
        val atLimits = listOf(bytes(1, 8), bytes(2, 8), bytes(3, 4))
        expectUnits(atLimits, taken(RawConfigSnapshots.captureFrom(units(log, *atLimits.toTypedArray()), ConfigBounds(3, 8, 20))))
        assertEquals(listOf("len:u0", "len:u1", "len:u2", "read:u0", "read:u1", "read:u2"), log, "all lengths first, then one read each, in order")
        refused(RawConfigSnapshots.captureFrom(units(mutableListOf(), *atLimits.toTypedArray()), ConfigBounds(2, 8, 20)), "3 configuration units exceed the limit of 2")
        refused(RawConfigSnapshots.captureFrom(units(mutableListOf(), *atLimits.toTypedArray()), ConfigBounds(3, 7, 20)), "configuration unit 0 of 8 bytes exceeds the limit of 7")
        refused(RawConfigSnapshots.captureFrom(units(mutableListOf(), *atLimits.toTypedArray()), ConfigBounds(3, 8, 19)), "20 configuration bytes exceed the limit of 19")
    }

    /** Explicitly SYNTHETIC lengths: an Int sum would wrap negative and pass; the checked aggregate refuses, and nothing is read or allocated. */
    @Test
    fun s5_anAggregateThatWouldOverflowAnIntIsRefusedWithoutReading() {
        val log = mutableListOf<String>()
        val huge = listOf(Scripted("h0", Int.MAX_VALUE, null, log), Scripted("h1", Int.MAX_VALUE, null, log))
        refused(RawConfigSnapshots.captureFrom(huge, ConfigBounds(2, Int.MAX_VALUE, Int.MAX_VALUE)), "4294967294 configuration bytes exceed the limit of ${Int.MAX_VALUE}")
        assertEquals(listOf("len:h0", "len:h1"), log)
    }

    /** Real heap slice, direct and read-only buffers with nonzero position, a limit below capacity and a mark: exactly the remaining bytes, and the caller's view untouched. */
    @Test
    fun s6_realBuffersAreCopiedExactlyAndTheirPositionLimitAndMarkAreUntouched() {
        val backing = ByteArray(32) { (0x40 + it).toByte() }
        val heapSlice = ByteBuffer.wrap(backing, 4, 20).slice().also { it.position(2); it.mark(); it.position(3); it.limit(9) }
        val direct = ByteBuffer.allocateDirect(16).also { d -> repeat(16) { d.put((0x60 + it).toByte()) }; d.position(1); d.mark(); d.position(5); d.limit(11) }
        val readOnly = ByteBuffer.wrap(ByteArray(12) { (0x70 + it).toByte() }).also { it.position(2); it.mark(); it.position(4); it.limit(10) }.asReadOnlyBuffer()
        readOnly.position(4)
        readOnly.limit(10)
        val buffers = listOf(heapSlice, direct, readOnly)
        val before = buffers.map { Triple(it.position(), it.limit(), it.capacity()) }
        // DECLARED from the fixture geometry, not read back through the buffers.
        val expected =
            listOf(
                byteArrayOf(0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c),
                byteArrayOf(0x65, 0x66, 0x67, 0x68, 0x69, 0x6a),
                byteArrayOf(0x74, 0x75, 0x76, 0x77, 0x78, 0x79),
            )
        expectUnits(expected, taken(RawConfigSnapshots.capture(buffers, ConfigBounds(3, 8, 32))))
        assertEquals(before, buffers.map { Triple(it.position(), it.limit(), it.capacity()) }, "position, limit and capacity untouched")
        heapSlice.reset()
        assertEquals(2, heapSlice.position(), "the heap slice's mark survived")
        direct.reset()
        assertEquals(1, direct.position(), "the direct buffer's mark survived")
        readOnly.reset()
        assertEquals(2, readOnly.position(), "the read-only buffer's mark survived")
    }

    @Test
    fun s7_theSnapshotIsIsolatedFromTheSourceAndFromItsOwnAccessorCopies() {
        val source = byteArrayOf(1, 2, 3, 4)
        val snapshot = taken(RawConfigSnapshots.capture(listOf(ByteBuffer.wrap(source)), ConfigBounds(1, 8, 8)))
        source[0] = 99
        assertContentEquals(byteArrayOf(1, 2, 3, 4), snapshot.unit(0), "a mutation of the source after capture changes nothing")
        snapshot.unit(0)[1] = 99
        assertContentEquals(byteArrayOf(1, 2, 3, 4), snapshot.unit(0), "a mutation of an accessor copy changes nothing")
    }

    /** Raw equality is count, order, boundaries and bytes: the same concatenated bytes with different boundaries are DIFFERENT snapshots. */
    @Test
    fun s8_rawEqualityIncludesBoundariesOrderAndCount() {
        val bounds = ConfigBounds(4, 8, 32)
        fun of(vararg u: ByteArray) = taken(RawConfigSnapshots.capture(u.map { ByteBuffer.wrap(it) }, bounds))
        val a = of(byteArrayOf(1, 2), byteArrayOf(3))
        assertEquals(a, of(byteArrayOf(1, 2), byteArrayOf(3)), "same units")
        assertEquals(a.hashCode(), of(byteArrayOf(1, 2), byteArrayOf(3)).hashCode())
        assertNotEquals(a, of(byteArrayOf(1), byteArrayOf(2, 3)), "same bytes, different boundaries")
        assertNotEquals(a, of(byteArrayOf(3), byteArrayOf(1, 2)), "different order")
        assertNotEquals(a, of(byteArrayOf(1, 2, 3)), "different count")
        assertNotEquals(a, of(byteArrayOf(1, 2), byteArrayOf(4)), "same lengths, one different byte")
    }

    /** A NEGATIVE declared length -- an internal-source contract violation, not platform buffer behaviour -- is refused by name, distinct from empty, before any read. */
    @Test
    fun s10_aNegativeLaterLengthIsRefusedByNameBeforeAnyRead() {
        val log = mutableListOf<String>()
        val sources = listOf(Scripted("a", 2, byteArrayOf(1, 2), log), Scripted("b", -3, null, log), Scripted("c", 2, byteArrayOf(3, 4), log))
        refused(RawConfigSnapshots.captureFrom(sources, ConfigBounds(3, 8, 16)), "configuration unit 1 declares a negative length of -3")
        assertEquals(listOf("len:a", "len:b"), log, "stopped at the negative length; nothing read")
    }

    private fun real(vararg sizes: Int): List<ByteBuffer> = sizes.mapIndexed { i, n -> ByteBuffer.wrap(bytes(i + 1, n + 2)).also { it.position(1); it.limit(1 + n) } }

    private fun states(buffers: List<ByteBuffer>) = buffers.map { Triple(it.position(), it.limit(), it.capacity()) }

    /** The REAL ByteBuffer entry point applies the caller's bounds itself, and leaves every buffer's state untouched whether it refuses or takes. */
    @Test
    fun s11_theByteBufferEntryPointAppliesTheCallersBounds() {
        fun check(buffers: List<ByteBuffer>, bounds: ConfigBounds, reason: String?) {
            val before = states(buffers)
            val result = RawConfigSnapshots.capture(buffers, bounds)
            if (reason == null) {
                expectUnits(buffers.mapIndexed { i, b -> bytes(i + 1, b.limit() + 1).copyOfRange(1, b.limit()) }, taken(result))
            } else {
                refused(result, reason)
            }
            assertEquals(before, states(buffers), "buffer state untouched")
        }
        check(emptyList(), ConfigBounds(2, 8, 16), "no configuration units")
        check(real(1, 1, 1), ConfigBounds(2, 8, 16), "3 configuration units exceed the limit of 2")
        check(real(4, 0), ConfigBounds(4, 8, 64), "configuration unit 1 is empty")
        check(real(4, 4, 9), ConfigBounds(4, 8, 64), "configuration unit 2 of 9 bytes exceeds the limit of 8")
        check(real(6, 6, 6), ConfigBounds(4, 8, 17), "18 configuration bytes exceed the limit of 17")
        check(real(8, 8, 4), ConfigBounds(3, 8, 20), null)
        check(real(8, 8, 4), ConfigBounds(2, 8, 20), "3 configuration units exceed the limit of 2")
        check(real(8, 8, 4), ConfigBounds(3, 7, 20), "configuration unit 0 of 8 bytes exceeds the limit of 7")
        check(real(8, 8, 4), ConfigBounds(3, 8, 19), "20 configuration bytes exceed the limit of 19")
    }

    /** An over-count list is refused before ANY element is accessed: the count guard precedes adaptation. A list-access discriminator only. */
    @Test
    fun s12_anOverCountBufferListIsRefusedWithoutTouchingAnElement() {
        val accessed = mutableListOf<Int>()
        val poisoned =
            object : AbstractList<ByteBuffer>() {
                override val size: Int get() = 5

                override fun get(index: Int): ByteBuffer {
                    accessed.add(index)
                    throw AssertionError("element $index was accessed before the count was refused")
                }
            }
        refused(RawConfigSnapshots.capture(poisoned, ConfigBounds(2, 8, 16)), "5 configuration units exceed the limit of 2")
        assertEquals(emptyList(), accessed)
    }

    /** A read that raises at a LATER unit leaves as itself -- Exception, cancellation and Error alike -- after exactly the earlier reads, and no later unit is read. */
    @Test
    fun s9_aFailingReadLeavesAsItselfAndNoLaterUnitIsRead() {
        for (failure in listOf<Throwable>(IllegalStateException("the copy failed"), CancellationException("the copy was cancelled"), AssertionError("the copy asserted"))) {
            val log = mutableListOf<String>()
            val sources = listOf(Scripted("a", 2, byteArrayOf(1, 2), log), Scripted("b", 2, byteArrayOf(3, 4), log, raise = failure), Scripted("c", 2, byteArrayOf(5, 6), log))
            val result = runCatching { RawConfigSnapshots.captureFrom(sources, ConfigBounds(3, 8, 16)) }
            val thrown = result.exceptionOrNull() ?: throw AssertionError("expected $failure to leave, but the capture returned ${result.getOrNull()}")
            if (thrown !== failure) throw AssertionError("expected $failure to leave by identity, but $thrown left", thrown)
            assertEquals(listOf("len:a", "len:b", "len:c", "read:a", "read:b"), log, "every length, then the reads up to the failing one; c never read")
            assertFalse(thrown.suppressed.isNotEmpty(), "nothing was attached to it")
        }
    }
}
