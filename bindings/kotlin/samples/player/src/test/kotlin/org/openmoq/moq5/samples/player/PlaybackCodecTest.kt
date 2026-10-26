package org.openmoq.moq5.samples.player

import java.nio.ByteBuffer
import java.util.ArrayDeque
import kotlinx.coroutines.CancellationException
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

class PlaybackCodecTest {
    private class Backend : PlaybackCodecBackend {
        val calls = mutableListOf<String>()
        var inputIndex = 0
        var input = ByteBuffer.allocate(16)
        val outputs = ArrayDeque<PlaybackCodecEvent>()
        var failure: Pair<String, Throwable>? = null
        var stopFailure: Throwable? = null
        var releaseFailure: Throwable? = null
        var queued = byteArrayOf()
        private fun call(name: String) {
            calls.add(name)
            failure?.takeIf { it.first == name }?.let { throw it.second }
        }
        override fun inputIndex(): Int { call("input"); return inputIndex }
        override fun inputBuffer(index: Int): ByteBuffer { call("buffer:$index"); return input }
        override fun queueInput(index: Int, size: Int, presentationUs: Long, end: Boolean) {
            call("queue:$index:$size:$presentationUs:$end")
            queued = ByteArray(size) { input.get(it) }
        }
        override fun output(): PlaybackCodecEvent {
            call("output")
            return outputs.pollFirst() ?: PlaybackCodecEvent.Idle
        }
        override fun releaseOutput(index: Int, renderTimeNs: Long?) { call("release:$index:$renderTimeNs") }
        override fun stop() { calls.add("stop"); stopFailure?.let { throw it } }
        override fun release() { calls.add("close"); releaseFailure?.let { throw it } }
    }

    private fun Backend.emit(index: Int = 2, bytes: ByteBuffer? = ByteBuffer.allocate(8), offset: Int = 1, size: Int = 3, end: Boolean = false) {
        outputs.add(PlaybackCodecEvent.Buffer(index, bytes, offset, size, 123L, end, false))
    }

    @Test fun inputRefusalDoesNotFetchOrQueueAndCanRetry() {
        val backend = Backend().apply { inputIndex = -1 }
        val driver = PlaybackCodec(backend, audio = true, maxUnitBytes = 8)
        assertFalse(driver.offer(byteArrayOf(4, 5), 99))
        assertEquals(listOf("input"), backend.calls)
        backend.inputIndex = 3
        assertTrue(driver.offer(byteArrayOf(4, 5), 99))
        assertContentEquals(byteArrayOf(4, 5), backend.queued)
        assertEquals(listOf("input", "input", "buffer:3", "queue:3:2:99:false"), backend.calls)
        driver.close()
    }

    @Test fun inputBoundsPrecedeAnyBackendCall() {
        val backend = Backend()
        val driver = PlaybackCodec(backend, audio = true, maxUnitBytes = 2)
        assertFailsWith<IllegalArgumentException> { driver.offer(byteArrayOf(), 0) }
        assertFailsWith<IllegalArgumentException> { driver.offer(byteArrayOf(1, 2, 3), 0) }
        assertEquals(emptyList(), backend.calls)
        driver.close()
    }

    @Test fun platformBufferTooSmallPoisonsUntilClose() {
        val backend = Backend().apply { input = ByteBuffer.allocate(1) }
        val driver = PlaybackCodec(backend, audio = true)
        assertFailsWith<IllegalStateException> { driver.offer(byteArrayOf(1, 2), 0) }
        val before = backend.calls.toList()
        assertFailsWith<IllegalStateException> { driver.offer(byteArrayOf(1), 0) }
        assertFailsWith<IllegalStateException> { driver.poll() }
        assertEquals(before, backend.calls)
        driver.close()
        assertEquals(listOf("stop", "close"), backend.calls.takeLast(2))
    }

    @Test fun queueFailureLeavesAsItselfAndIsNeverRetried() {
        val raised = AssertionError("queue")
        val backend = Backend().apply { failure = "queue:0:1:7:false" to raised }
        val driver = PlaybackCodec(backend, audio = true)
        assertSame(raised, assertFailsWith<AssertionError> { driver.offer(byteArrayOf(1), 7) })
        val before = backend.calls.toList()
        assertFailsWith<IllegalStateException> { driver.endInput(7) }
        assertEquals(before, backend.calls)
        driver.close()
    }

    @Test fun inputEosHasItsOwnRetryAndThenForbidsNewMedia() {
        val backend = Backend().apply { inputIndex = -1 }
        val driver = PlaybackCodec(backend, audio = true)
        assertFalse(driver.endInput(456))
        backend.inputIndex = 4
        assertTrue(driver.endInput(456))
        val before = backend.calls.toList()
        assertTrue(driver.endInput(456))
        assertFailsWith<IllegalStateException> { driver.offer(byteArrayOf(1), 456) }
        assertEquals(before, backend.calls)
        assertEquals("queue:4:0:456:true", backend.calls.last())
        driver.close()
    }

    @Test fun outputKeepsTheActualBufferAndDeclaredRegionUntilReleased() {
        val bytes = ByteBuffer.allocateDirect(8)
        val backend = Backend().apply { emit(bytes = bytes) }
        val driver = PlaybackCodec(backend, audio = true)
        val output = (driver.poll() as PlaybackCodec.Output.Buffer).lease
        assertSame(bytes, output.bytes)
        assertEquals(1, bytes.position())
        assertEquals(4, bytes.limit())
        assertEquals(123L, output.presentationUs)
        assertFalse(output.end)
        assertFalse(backend.calls.any { it.startsWith("release:") })
        driver.release(output)
        assertEquals("release:2:null", backend.calls.last())
        driver.close()
    }

    @Test fun staleAndForeignLeasesCannotReleaseReusedIndices() {
        val firstBackend = Backend().apply { emit() }
        val secondBackend = Backend().apply { emit() }
        val first = PlaybackCodec(firstBackend, audio = true)
        val second = PlaybackCodec(secondBackend, audio = true)
        val old = (first.poll() as PlaybackCodec.Output.Buffer).lease
        val foreign = (second.poll() as PlaybackCodec.Output.Buffer).lease
        first.release(old)
        firstBackend.emit()
        val current = (first.poll() as PlaybackCodec.Output.Buffer).lease
        val before = firstBackend.calls.toList()
        assertFailsWith<IllegalArgumentException> { first.release(old) }
        assertFailsWith<IllegalArgumentException> { first.release(foreign) }
        assertEquals(before, firstBackend.calls)
        first.release(current)
        second.release(foreign)
        first.close()
        second.close()
    }

    @Test fun outputCountBoundPreventsAnotherPlatformDequeue() {
        val backend = Backend().apply { emit(2); emit(3) }
        val driver = PlaybackCodec(backend, audio = true, maxOutputBorrows = 1)
        val first = (driver.poll() as PlaybackCodec.Output.Buffer).lease
        val before = backend.calls.toList()
        assertSame(PlaybackCodec.Output.Idle, driver.poll())
        assertEquals(before, backend.calls)
        driver.release(first)
        val second = (driver.poll() as PlaybackCodec.Output.Buffer).lease
        driver.release(second)
        assertEquals("release:3:null", backend.calls.last())
        driver.close()
    }

    @Test fun duplicatePlatformIndexIsFatalRatherThanReplacingABorrow() {
        val backend = Backend().apply { emit(); emit() }
        val driver = PlaybackCodec(backend, audio = true)
        driver.poll()
        assertFailsWith<IllegalStateException> { driver.poll() }
        val before = backend.calls.toList()
        assertFailsWith<IllegalStateException> { driver.poll() }
        assertEquals(before, backend.calls)
        driver.close()
    }

    @Test fun badOutputRegionCannotEscapeAsABuffer() {
        for ((offset, size) in listOf(-1 to 1, 0 to -1, 7 to 2, Int.MAX_VALUE to 2)) {
            val backend = Backend().apply { emit(offset = offset, size = size) }
            val driver = PlaybackCodec(backend, audio = true)
            assertFailsWith<IllegalStateException> { driver.poll() }
            assertFailsWith<IllegalStateException> { driver.poll() }
            driver.close()
        }
    }

    @Test fun audioNeedsBufferButSurfaceVideoDoesNot() {
        val audioBackend = Backend().apply { emit(bytes = null) }
        val audio = PlaybackCodec(audioBackend, audio = true)
        assertFailsWith<IllegalStateException> { audio.poll() }
        audio.close()
        val videoBackend = Backend().apply { emit(bytes = null) }
        val video = PlaybackCodec(videoBackend, audio = false)
        val output = (video.poll() as PlaybackCodec.Output.Buffer).lease
        video.release(output, 123_000L)
        assertEquals("release:2:123000", videoBackend.calls.last())
        video.close()
    }

    @Test fun outputFormatAndEmptyEndAreDistinctAndEndStillOwesRelease() {
        val format = PlaybackOutputFormat("audio/raw", sampleRate = 48_000, channels = 2)
        val backend = Backend().apply {
            outputs.add(PlaybackCodecEvent.Format(format))
            emit(bytes = null, offset = 0, size = 0, end = true)
        }
        val driver = PlaybackCodec(backend, audio = true)
        assertSame(format, (driver.poll() as PlaybackCodec.Output.Format).format)
        val output = (driver.poll() as PlaybackCodec.Output.Buffer).lease
        assertTrue(output.end)
        val before = backend.calls.toList()
        assertSame(PlaybackCodec.Output.Idle, driver.poll())
        assertEquals(before, backend.calls)
        driver.release(output)
        assertEquals("release:2:null", backend.calls.last())
        driver.close()
    }

    @Test fun failedOutputReleasePoisonsDriverWithoutAnAutomaticDoubleRelease() {
        val failure = IllegalStateException("release")
        val backend = Backend().apply { emit(); this.failure = "release:2:null" to failure }
        val driver = PlaybackCodec(backend, audio = true)
        val output = (driver.poll() as PlaybackCodec.Output.Buffer).lease
        assertSame(failure, assertFailsWith<IllegalStateException> { driver.release(output) })
        val before = backend.calls.toList()
        assertFailsWith<IllegalStateException> { driver.release(output) }
        assertEquals(before, backend.calls)
        driver.close()
    }

    @Test fun stopFailureDoesNotPreventReleaseAndRepeatedCloseDoesNotPretendSuccess() {
        val first = AssertionError("stop")
        val second = IllegalStateException("close")
        val backend = Backend().apply { stopFailure = first; releaseFailure = second }
        val driver = PlaybackCodec(backend, audio = true)
        assertSame(first, assertFailsWith<AssertionError> { driver.close() })
        assertEquals(listOf("stop", "close"), backend.calls)
        assertEquals(listOf(second), first.suppressed.toList())
        assertSame(first, assertFailsWith<AssertionError> { driver.close() })
        assertEquals(listOf("stop", "close"), backend.calls)
    }

    @Test fun cleanCloseIsIdempotentAndInvalidatesBorrowedLeases() {
        val backend = Backend().apply { emit() }
        val driver = PlaybackCodec(backend, audio = true)
        val output = (driver.poll() as PlaybackCodec.Output.Buffer).lease
        driver.close()
        val before = backend.calls.toList()
        driver.close()
        assertFailsWith<IllegalStateException> { driver.release(output) }
        assertFailsWith<IllegalStateException> { driver.offer(byteArrayOf(1), 0) }
        assertEquals(before, backend.calls)
    }

    @Test fun cleanupCancellationIsNotSwallowedAndSameFailureIsNotSuppressedOntoItself() {
        val same = CancellationException("platform cleanup")
        val backend = Backend().apply { stopFailure = same; releaseFailure = same }
        val driver = PlaybackCodec(backend, audio = true)
        assertSame(same, assertFailsWith<CancellationException> { driver.close() })
        assertEquals(listOf("stop", "close"), backend.calls)
        assertEquals(emptyList(), same.suppressed.toList())
    }

    @Test fun audioRenderMisuseDoesNotPoisonOrConsumeTheLease() {
        val backend = Backend().apply { emit() }
        val driver = PlaybackCodec(backend, audio = true)
        val output = (driver.poll() as PlaybackCodec.Output.Buffer).lease
        val before = backend.calls.toList()
        assertFailsWith<IllegalArgumentException> { driver.release(output, 0) }
        assertEquals(before, backend.calls)
        driver.release(output)
        assertEquals("release:2:null", backend.calls.last())
        driver.close()
    }

    @Test fun nonFrameConfigurationBufferIsReportedAsSuch() {
        val backend = Backend().apply {
            outputs.add(PlaybackCodecEvent.Buffer(0, ByteBuffer.allocate(2), 0, 2, 0L, false, true))
        }
        val driver = PlaybackCodec(backend, audio = true)
        val output = (driver.poll() as PlaybackCodec.Output.Buffer).lease
        assertTrue(output.config)
        driver.release(output)
        driver.close()
    }
}
