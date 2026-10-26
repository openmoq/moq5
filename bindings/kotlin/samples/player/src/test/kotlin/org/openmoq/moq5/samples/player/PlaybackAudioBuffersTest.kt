package org.openmoq.moq5.samples.player

import java.nio.ByteBuffer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNull

class PlaybackAudioBuffersTest {
    @Test
    fun consumptionReturnsTheCodecIndexNotTheCallbackToken() {
        val buffers = PlaybackAudioBuffers(2)
        val first = ByteBuffer.wrap(byteArrayOf(1, 2))
        val second = ByteBuffer.wrap(byteArrayOf(3, 4))
        val a = buffers.retain(17, first)
        val b = buffers.retain(3, second)
        assertNotEquals(a, b)
        assertEquals(3, buffers.consumed(b, second))
        assertEquals(17, buffers.consumed(a, first))
        assertEquals(emptyList(), buffers.afterSyncStopped())
    }

    @Test
    fun contentEqualityIsNotBorrowIdentity() {
        val buffers = PlaybackAudioBuffers(1)
        val buffer = ByteBuffer.wrap(byteArrayOf(7, 8))
        val token = buffers.retain(4, buffer)
        val equalCopy = ByteBuffer.wrap(byteArrayOf(7, 8))
        assertEquals(buffer, equalCopy, "ByteBuffer equality is content-based")
        assertFailsWith<IllegalStateException> { buffers.consumed(token, equalCopy) }
        assertEquals(4, buffers.consumed(token, buffer), "the refused callback consumed nothing")
    }

    @Test
    fun theBorrowerMayAdvancePositionWithoutChangingIdentity() {
        val buffers = PlaybackAudioBuffers(1)
        val buffer = ByteBuffer.wrap(byteArrayOf(7, 8))
        val token = buffers.retain(11, buffer)
        buffer.position(buffer.limit())
        assertEquals(11, buffers.consumed(token, buffer))
    }

    @Test
    fun duplicateAndStaleCallbacksCannotConsumeANewerUseOfTheSameBuffer() {
        val buffers = PlaybackAudioBuffers(1)
        val buffer = ByteBuffer.allocate(8)
        val old = buffers.retain(5, buffer)
        assertEquals(5, buffers.consumed(old, buffer))
        assertFailsWith<IllegalStateException> { buffers.consumed(old, buffer) }
        val next = buffers.retain(5, buffer)
        assertNotEquals(old, next)
        assertFailsWith<IllegalStateException> { buffers.consumed(old, buffer) }
        assertEquals(5, buffers.consumed(next, buffer))
    }

    @Test
    fun capacityRefusalPreservesEveryOutstandingBorrow() {
        val buffers = PlaybackAudioBuffers(2)
        val first = ByteBuffer.allocate(8)
        val second = ByteBuffer.allocate(8)
        val third = ByteBuffer.allocate(8)
        val a = buffers.retain(8, first)
        buffers.retain(2, second)
        assertFailsWith<IllegalStateException> { buffers.retain(9, third) }
        assertEquals(8, buffers.consumed(a, first))
        buffers.retain(9, third)
        assertEquals(listOf(2, 9), buffers.afterSyncStopped())
    }

    @Test
    fun codecIndexCannotBeBorrowedTwiceAtOnce() {
        val buffers = PlaybackAudioBuffers(3)
        val first = ByteBuffer.allocate(8)
        buffers.retain(4, first)
        assertFailsWith<IllegalStateException> { buffers.retain(4, ByteBuffer.allocate(8)) }
        assertEquals(listOf(4), buffers.afterSyncStopped())
    }

    @Test
    fun terminalStopReclaimsOnlyOutstandingIndicesAndNeverAgain() {
        val buffers = PlaybackAudioBuffers(3)
        val first = ByteBuffer.allocate(8)
        val second = ByteBuffer.allocate(8)
        val a = buffers.retain(4, first)
        val b = buffers.retain(7, second)
        assertEquals(4, buffers.consumed(a, first))
        assertEquals(listOf(7), buffers.afterSyncStopped())
        assertNull(buffers.consumed(b, second), "a queued late callback must not release a stopped codec")
        assertEquals(emptyList(), buffers.afterSyncStopped())
        assertFailsWith<IllegalStateException> { buffers.retain(4, first) }
    }

    @Test
    fun invalidArgumentsDoNotAdmitAnything() {
        assertFailsWith<IllegalArgumentException> { PlaybackAudioBuffers(0) }
        assertFailsWith<IllegalArgumentException> { PlaybackAudioBuffers(-1) }
        val buffers = PlaybackAudioBuffers(1)
        assertFailsWith<IllegalArgumentException> { buffers.retain(-1, ByteBuffer.allocate(8)) }
        assertEquals(emptyList(), buffers.afterSyncStopped())
    }

    @Test
    fun callbackTokensRefuseExhaustionRatherThanWrapOrReuse() {
        val buffers = PlaybackAudioBuffers(3)
        // White-box numeric-boundary setup, not a claim that the fixture
        // submitted two billion buffers or exercised a real MediaSync.
        val counter = PlaybackAudioBuffers::class.java.getDeclaredField("nextId")
        counter.isAccessible = true
        counter.setLong(buffers, Int.MAX_VALUE.toLong() - 1)
        val first = ByteBuffer.allocate(8)
        val second = ByteBuffer.allocate(8)
        assertEquals(Int.MAX_VALUE - 1, buffers.retain(2, first))
        assertEquals(Int.MAX_VALUE, buffers.retain(3, second))
        assertEquals(2, buffers.consumed(Int.MAX_VALUE - 1, first))
        assertFailsWith<IllegalStateException> { buffers.retain(4, ByteBuffer.allocate(8)) }
        assertEquals(listOf(3), buffers.afterSyncStopped())
    }
}
