package org.openmoq.moq5.samples.player

import kotlinx.coroutines.CancellationException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame

class PlaybackPcmTest {
    private fun <T : Any> createPlaybackAudio(format: PlaybackOutputFormat, api: Int, factory: PlaybackAudioFactory<T>, maxBufferBytes: Int = 4 * 1024 * 1024): T =
        org.openmoq.moq5.samples.player.createPlaybackAudio(format, api, factory, PlaybackResourceOwner(factory::release), maxBufferBytes)

    private val stereo = PlaybackOutputFormat("audio/raw", sampleRate = 48000, channels = 2)

    private class Factory : PlaybackAudioFactory<Any> {
        val calls = mutableListOf<String>()
        val track = Any()
        var minimum = 1025
        var ready = true
        var failAt: String? = null
        var failure: Throwable = IllegalStateException("factory failed")
        var releaseFailure: Throwable? = null
        var format: PlaybackPcm? = null
        var bytes = 0
        private fun step(name: String) {
            calls.add(name)
            if (name == failAt) throw failure
        }
        override fun minimumBytes(pcm: PlaybackPcm): Int {
            step("minimum")
            format = pcm
            return minimum
        }
        override fun create(pcm: PlaybackPcm, bufferBytes: Int): Any {
            step("create")
            assertSame(format, pcm)
            bytes = bufferBytes
            return track
        }
        override fun initialized(track: Any): Boolean {
            assertSame(this.track, track)
            step("initialized")
            return ready
        }
        override fun release(track: Any) {
            assertSame(this.track, track)
            step("release")
            releaseFailure?.let { throw it }
        }
    }

    @Test fun absentEncodingAndMonoStereoMasksHaveExplicitDefaults() {
        val mono = PlaybackPcm.from(stereo.copy(channels = 1), 23)
        val two = PlaybackPcm.from(stereo, 23)
        assertEquals(48000, two.sampleRate)
        assertEquals(4, mono.channelMask)
        assertEquals(12, two.channelMask)
        assertEquals(2, two.encoding)
        assertEquals(2, mono.frameBytes)
        assertEquals(4, two.frameBytes)
    }

    @Test fun explicitLayoutIsPreservedRatherThanDerivedFromCount() {
        // Six channels with side speakers, not the back-speaker 0xfc mask.
        val pcm = PlaybackPcm.from(stereo.copy(channels = 6, channelMask = 0x183c, pcmEncoding = 4), 23)
        assertEquals(0x183c, pcm.channelMask)
        assertEquals(4, pcm.encoding)
        assertEquals(24, pcm.frameBytes)
    }

    @Test fun missingMultichannelLayoutAndInconsistentMasksAreRefused() {
        for (facts in listOf(
            stereo.copy(channels = 6), stereo.copy(channelMask = 4),
            stereo.copy(channelMask = 0), stereo.copy(channelMask = 1),
            stereo.copy(channelMask = 3), stereo.copy(channelMask = Int.MIN_VALUE),
            stereo.copy(channelMask = 0x30000000),
        )) {
            val factory = Factory()
            assertFailsWith<IllegalArgumentException> { createPlaybackAudio(facts, 36, factory) }
            assertEquals(emptyList(), factory.calls)
        }
    }

    @Test fun malformedOrNonPcmFormatHasNoFactoryEffects() {
        for (facts in listOf(
            stereo.copy(mime = "audio/mp4a-latm"), stereo.copy(sampleRate = null),
            stereo.copy(sampleRate = 0), stereo.copy(sampleRate = -1),
            stereo.copy(channels = null), stereo.copy(channels = 0),
            stereo.copy(channels = Int.MAX_VALUE), stereo.copy(pcmEncoding = 1),
            stereo.copy(pcmEncoding = 0), stereo.copy(pcmEncoding = -1),
        )) {
            val factory = Factory()
            assertFailsWith<IllegalArgumentException> { createPlaybackAudio(facts, 36, factory) }
            assertEquals(emptyList(), factory.calls)
        }
    }

    @Test fun encodingWidthsAndApiFloorAreExact() {
        for ((encoding, sampleBytes) in listOf(2 to 2, 3 to 1, 4 to 4, 21 to 3, 22 to 4)) {
            val pcm = PlaybackPcm.from(stereo.copy(pcmEncoding = encoding), 31)
            assertEquals(sampleBytes * 2, pcm.frameBytes)
            assertEquals(encoding, pcm.encoding)
        }
        for (encoding in listOf(21, 22)) {
            val factory = Factory()
            assertFailsWith<IllegalArgumentException> { createPlaybackAudio(stereo.copy(pcmEncoding = encoding), 30, factory) }
            assertEquals(emptyList(), factory.calls)
        }
        assertFailsWith<IllegalArgumentException> { PlaybackPcm.from(stereo, 22) }
    }

    @Test fun minimumIsDoubledAndRoundedToAWholePcmFrame() {
        val factory = Factory()
        assertSame(factory.track, createPlaybackAudio(stereo, 23, factory))
        assertEquals(listOf("minimum", "create", "initialized"), factory.calls)
        assertEquals(2052, factory.bytes)
    }

    @Test fun badOrOverPolicyMinimumNeverAllocates() {
        for (minimum in listOf(0, -1, -2, Int.MAX_VALUE, 2049)) {
            val factory = Factory().apply { this.minimum = minimum }
            assertFailsWith<IllegalArgumentException> { createPlaybackAudio(stereo, 23, factory, maxBufferBytes = 4096) }
            assertEquals(listOf("minimum"), factory.calls)
        }
        val exact = Factory().apply { minimum = 2048 }
        createPlaybackAudio(stereo, 23, exact, maxBufferBytes = 4096)
        assertEquals(4096, exact.bytes)
    }

    @Test fun roundingCannotCrossTheDeclaredByteBound() {
        val factory = Factory().apply { minimum = 1025 }
        assertFailsWith<IllegalArgumentException> { createPlaybackAudio(stereo, 23, factory, maxBufferBytes = 2051) }
        assertEquals(listOf("minimum"), factory.calls)
        val invalid = Factory()
        assertFailsWith<IllegalArgumentException> { createPlaybackAudio(stereo, 23, invalid, maxBufferBytes = 0) }
        assertEquals(emptyList(), invalid.calls)
    }

    @Test fun preAllocationFailureDoesNotReleaseANonexistentTrack() {
        for (where in listOf("minimum", "create")) {
            val factory = Factory().apply { failAt = where }
            assertSame(factory.failure, assertFailsWith<IllegalStateException> { createPlaybackAudio(stereo, 23, factory) })
            assertEquals(if (where == "minimum") listOf("minimum") else listOf("minimum", "create"), factory.calls)
        }
    }

    @Test fun uninitializedTrackIsReleasedBeforeRefusalEscapes() {
        val factory = Factory().apply { ready = false }
        val failure = assertFailsWith<IllegalStateException> { createPlaybackAudio(stereo, 23, factory) }
        assertEquals("AudioTrack did not initialize", failure.message)
        assertEquals(listOf("minimum", "create", "initialized", "release"), factory.calls)
    }

    @Test fun inspectionFailureRemainsPrimaryThroughEveryCleanupFailureClass() {
        for (primary in listOf(IllegalStateException("inspect"), AssertionError("inspect"), CancellationException("inspect"))) {
            for (secondary in listOf(IllegalStateException("release"), AssertionError("release"), CancellationException("release"))) {
                val factory = Factory().apply { failAt = "initialized"; failure = primary; releaseFailure = secondary }
                val previous = primary.suppressed.toList()
                assertSame(primary, assertFailsWith<Throwable> { createPlaybackAudio(stereo, 23, factory) })
                assertEquals(previous + secondary, primary.suppressed.toList())
                assertEquals(listOf("minimum", "create", "initialized", "release"), factory.calls)
            }
        }
    }

    @Test fun sameCleanupFailureCannotSelfSuppressOrReplacePrimary() {
        val primary = CancellationException("same")
        val factory = Factory().apply { failAt = "initialized"; failure = primary; releaseFailure = primary }
        assertSame(primary, assertFailsWith<CancellationException> { createPlaybackAudio(stereo, 23, factory) })
        assertEquals(emptyList(), primary.suppressed.toList())
        assertEquals("release", factory.calls.last())
    }
}
