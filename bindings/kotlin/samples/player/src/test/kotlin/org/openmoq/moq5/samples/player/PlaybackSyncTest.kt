package org.openmoq.moq5.samples.player

import java.nio.ByteBuffer
import kotlinx.coroutines.CancellationException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

class PlaybackSyncTest {
    private class Backend : PlaybackSyncBackend {
        val calls = mutableListOf<String>()
        val queued = mutableListOf<Pair<Int, ByteBuffer>>()
        var queueFailure: Throwable? = null
        var flushFailure: Throwable? = null
        var releaseFailure: Throwable? = null
        override fun queueAudio(bytes: ByteBuffer, token: Int, presentationUs: Long) {
            calls.add("queue:$presentationUs")
            queued.add(token to bytes)
            queueFailure?.let { throw it }
        }
        override fun flush() { calls.add("flush"); flushFailure?.let { throw it } }
        override fun release() { calls.add("release"); releaseFailure?.let { throw it } }
    }

    private class Rig(count: Int = 2, bytes: Int = 16) {
        val backend = Backend()
        val returned = mutableListOf<PlaybackDecodedOutput>()
        val failed = mutableListOf<Throwable>()
        var returnFailure: Throwable? = null
        val sync = PlaybackSync(backend, count, bytes, { output ->
            returned.add(output)
            returnFailure?.let { throw it }
        }, { failed.add(it) })
        fun consume(at: Int) {
            val (token, buffer) = backend.queued[at]
            sync.onAudioConsumed(token, buffer)
        }
    }

    private fun output(index: Int = 0, size: Int = 4, end: Boolean = false, config: Boolean = false) =
        PlaybackDecodedOutput(index, ByteBuffer.allocate(size), size, 100, end, config)

    @Test fun callbackReturnsTheExactLeaseAndRepeatedIndexGetsANewToken() {
        val rig = Rig()
        val first = output()
        assertTrue(rig.sync.queueAudio(first, 42))
        assertSame(first.bytes, rig.backend.queued.single().second)
        first.bytes!!.position(4) // Android is permitted to consume bytes.
        rig.consume(0)
        assertSame(first, rig.returned.single())
        val next = output()
        assertTrue(rig.sync.queueAudio(next, 45))
        assertTrue(rig.backend.queued[1].first != rig.backend.queued[0].first)
        rig.consume(1)
        assertEquals(listOf(first, next), rig.returned)
        assertTrue(rig.sync.close().quiesced)
    }

    @Test fun countAndByteRefusalsOwnNothingAndCanRetryAfterConsumption() {
        for (rig in listOf(Rig(count = 1), Rig(bytes = 6))) {
            val first = output(0)
            val second = output(1)
            assertTrue(rig.sync.queueAudio(first, 0))
            assertFalse(rig.sync.queueAudio(second, 1))
            assertEquals(listOf("queue:0"), rig.backend.calls)
            rig.consume(0)
            assertTrue(rig.sync.queueAudio(second, 1))
            rig.sync.close()
            assertEquals(listOf(first, second), rig.returned)
        }
    }

    @Test fun invalidMediaOrTimestampIsRefusedBeforeAdoption() {
        val rig = Rig(bytes = 8)
        for (bad in listOf(output(size = 0), output(size = 9), output(config = true))) {
            assertFailsWith<IllegalArgumentException> { rig.sync.queueAudio(bad, 0) }
        }
        assertFailsWith<IllegalArgumentException> { rig.sync.queueAudio(output(), -1) }
        val changedRegion = output().apply { bytes!!.position(1) }
        assertFailsWith<IllegalArgumentException> { rig.sync.queueAudio(changedRegion, 0) }
        assertEquals(emptyList(), rig.backend.calls)
        rig.sync.close()
        assertEquals(emptyList(), rig.returned)
    }

    @Test fun nonemptyEosIsQueuedAndReturnedLikeOtherPcm() {
        val rig = Rig()
        val last = output(end = true)
        assertTrue(rig.sync.queueAudio(last, 22))
        rig.consume(0)
        assertSame(last, rig.returned.single())
        rig.sync.close()
    }

    @Test fun duplicateBorrowDoesNotReachPlatformOrReleaseTheFirst() {
        val rig = Rig()
        val first = output()
        rig.sync.queueAudio(first, 0)
        assertFailsWith<IllegalArgumentException> { rig.sync.queueAudio(output(), 1) }
        assertEquals(listOf("queue:0"), rig.backend.calls)
        assertEquals(emptyList(), rig.returned)
        rig.sync.close()
        assertSame(first, rig.returned.single())
    }

    @Test fun wrongBufferCallbackPoisonsAndDoesNotReturnALease() {
        val rig = Rig()
        val first = output()
        rig.sync.queueAudio(first, 0)
        rig.sync.onAudioConsumed(rig.backend.queued.single().first, ByteBuffer.allocate(4))
        assertEquals(1, rig.failed.size)
        assertEquals(emptyList(), rig.returned)
        assertFailsWith<IllegalStateException> { rig.sync.queueAudio(output(1), 1) }
        rig.sync.close()
        assertSame(first, rig.returned.single())
    }

    @Test fun oldTokenCannotReleaseAReusedIndexEvenWithTheSameBuffer() {
        val rig = Rig()
        val first = output()
        rig.sync.queueAudio(first, 0)
        rig.consume(0)
        val next = PlaybackDecodedOutput(0, first.bytes, 4, 1, false, false)
        rig.sync.queueAudio(next, 1)
        rig.consume(0)
        assertEquals(listOf(first), rig.returned)
        assertEquals(1, rig.failed.size)
        rig.sync.close()
        assertEquals(listOf(first, next), rig.returned)
    }

    @Test fun possibleQueueAdoptionIsKeptUntilSyncQuiescence() {
        val rig = Rig()
        val original = CancellationException("queue")
        rig.backend.queueFailure = original
        val lease = output()
        assertSame(original, assertFailsWith<CancellationException> { rig.sync.queueAudio(lease, 0) })
        assertEquals(emptyList(), rig.returned)
        assertEquals(emptyList(), rig.failed, "cancellation is propagated, not reported as a sync failure")
        assertEquals(null, rig.sync.failure)
        rig.sync.close()
        assertEquals(listOf("queue:0", "flush", "release"), rig.backend.calls)
        assertSame(lease, rig.returned.single())
    }

    @Test fun bothCloseOperationsAreAttemptedAndEitherSuccessProvesBorrowReturn() {
        for (flushFails in listOf(true, false)) {
            val rig = Rig()
            val failure = AssertionError("one close failed")
            if (flushFails) rig.backend.flushFailure = failure else rig.backend.releaseFailure = failure
            val lease = output()
            rig.sync.queueAudio(lease, 0)
            val closed = rig.sync.close()
            assertTrue(closed.quiesced)
            assertEquals(flushFails, closed.released, "flush returns PCM borrows but cannot stand in for sync release")
            assertSame(failure, closed.failure)
            assertEquals(listOf("queue:0", "flush", "release"), rig.backend.calls)
            assertSame(lease, rig.returned.single())
            assertSame(closed, rig.sync.close())
            assertEquals(3, rig.backend.calls.size)
        }
    }

    @Test fun twoFailedClosesDoNotAuthorizeCodecDisposalOrInventConsumption() {
        val rig = Rig()
        val first = CancellationException("flush")
        val second = AssertionError("release")
        rig.backend.flushFailure = first
        rig.backend.releaseFailure = second
        rig.sync.queueAudio(output(), 0)
        val closed = rig.sync.close()
        assertFalse(closed.quiesced)
        assertFalse(closed.released)
        assertSame(first, closed.failure)
        assertEquals(listOf(second), first.suppressed.toList())
        assertEquals(emptyList(), rig.returned)
        rig.consume(0) // Late callbacks do not change the frozen close disposition.
        assertEquals(emptyList(), rig.returned)
        assertSame(closed, rig.sync.close())
        assertEquals(listOf("queue:0", "flush", "release"), rig.backend.calls)
    }

    @Test fun flushReclaimsAllLeasesEvenIfOneDecoderReleaseFails() {
        val rig = Rig()
        val first = output(0)
        val second = output(1)
        rig.sync.queueAudio(first, 0)
        rig.sync.queueAudio(second, 1)
        val failure = AssertionError("decoder release")
        rig.returnFailure = failure
        val closed = rig.sync.close()
        assertTrue(closed.quiesced)
        assertSame(failure, closed.failure)
        assertEquals(listOf(first, second), rig.returned)
        assertEquals(emptyList(), failure.suppressed.toList())
    }

    @Test fun callbackDecoderReleaseFailureWakesOwnerOnceAndRemainsPrimary() {
        val rig = Rig()
        val failure = AssertionError("output release")
        rig.returnFailure = failure
        rig.sync.queueAudio(output(), 0)
        rig.consume(0)
        assertEquals(listOf<Throwable>(failure), rig.failed)
        rig.sync.onFailure(IllegalStateException("later sync error"))
        assertEquals(listOf<Throwable>(failure), rig.failed)
        assertSame(failure, rig.sync.failure)
        rig.sync.close()
        assertEquals(1, rig.returned.size)
    }

    @Test fun closedSyncIgnoresLateCallbacksAndRejectsFurtherQueues() {
        val rig = Rig()
        rig.sync.queueAudio(output(), 0)
        assertTrue(rig.sync.close().quiesced)
        rig.consume(0)
        rig.sync.onFailure(AssertionError("late"))
        assertEquals(1, rig.returned.size)
        assertEquals(emptyList(), rig.failed)
        assertFailsWith<IllegalStateException> { rig.sync.queueAudio(output(1), 1) }
    }

    private fun traced(block: () -> Unit): List<String> {
        val seen = mutableListOf<String>()
        val sink = PlaybackTrace.sink
        PlaybackTrace.resetForTest()
        PlaybackTrace.enabled = true
        PlaybackTrace.sink = { seen.add(it.substringAfter(' ')) }
        try {
            block()
        } finally {
            PlaybackTrace.enabled = false
            PlaybackTrace.sink = sink
            PlaybackTrace.resetForTest()
        }
        return seen
    }

    @Test fun countsFollowActualSubmissionsAndReturnsByIdentityNotAnExpectedTotal() {
        val rig = Rig(count = 3, bytes = 64)
        val a = output(0, size = 4)
        val b = output(1, size = 6)
        val c = output(2, size = 2)
        val lines = traced {
            assertTrue(rig.sync.queueAudio(a, 0))
            assertTrue(rig.sync.queueAudio(b, 21_333))
            assertTrue(rig.sync.queueAudio(c, 42_666))
            assertFalse(rig.sync.queueAudio(output(3, size = 4), 64_000))          // capacity refusal owns nothing
            rig.consume(1)
            rig.consume(0)
            assertTrue(rig.sync.close().quiesced)
        }
        val tokens = rig.backend.queued.map { it.first }
        assertEquals(3, rig.sync.submitted)
        assertEquals(12L, rig.sync.submittedBytes)
        assertEquals(2, rig.sync.returned)
        assertEquals(10L, rig.sync.returnedBytes)
        assertEquals(1, rig.sync.flushedAtClose)
        assertEquals(listOf(
            "pcm-submit token=${tokens[0]} index=0 bytes=4 pts=0",
            "pcm-submit token=${tokens[1]} index=1 bytes=6 pts=21333",
            "pcm-submit token=${tokens[2]} index=2 bytes=2 pts=42666",
            "pcm-returned token=${tokens[1]} index=1 bytes=6",
            "pcm-returned token=${tokens[0]} index=0 bytes=4",
            "pcm-flushed-at-close index=2 bytes=2",
        ), lines)
    }

    @Test fun aQueueThatThrowsIsNotCountedAsASubmission() {
        val rig = Rig()
        rig.backend.queueFailure = IllegalStateException("platform refused")
        assertFailsWith<IllegalStateException> { rig.sync.queueAudio(output(), 0) }
        assertEquals(0, rig.sync.submitted)
        assertEquals(0L, rig.sync.submittedBytes)
    }

    @Test fun aForeignOrRepeatedReturnIsNeverCounted() {
        val rig = Rig()
        assertTrue(rig.sync.queueAudio(output(0), 0))
        rig.sync.onAudioConsumed(rig.backend.queued[0].first + 1000, ByteBuffer.allocate(4))
        assertEquals(0, rig.sync.returned)
        assertEquals(1, rig.failed.size)
        val repeat = Rig()
        assertTrue(repeat.sync.queueAudio(output(0), 0))
        repeat.consume(0)
        repeat.consume(0)
        assertEquals(1, repeat.sync.returned)
    }
}
