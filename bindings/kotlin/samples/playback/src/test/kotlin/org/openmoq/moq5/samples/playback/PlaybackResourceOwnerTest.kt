package org.openmoq.moq5.samples.playback

import kotlinx.coroutines.CancellationException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

class PlaybackResourceOwnerTest {
    @Test fun `ownership exists inside setup and successful close releases exactly once`() {
        val native = Any()
        var releases = 0
        val owner = PlaybackResourceOwner<Any> { assertSame(native, it); releases++ }
        assertSame(native, owner.prepare({ native }) { assertTrue(owner.acquired); assertTrue(owner.retained) })
        owner.close()
        owner.close()
        assertEquals(1, releases)
        assertFalse(owner.retained)
        assertFailsWith<IllegalStateException> { owner.prepare({ Any() }) {} }
    }

    @Test fun `failed start and failed release retain the codec without retrying native disposal`() {
        val native = Any()
        val start = AssertionError("start")
        val release = CancellationException("release")
        var releases = 0
        val owner = PlaybackResourceOwner<Any> { assertSame(native, it); releases++; throw release }
        assertSame(start, assertFailsWith<AssertionError> { owner.prepare({ native }) { throw start } })
        val result = disposePlaybackRender(if (owner.acquired) ({ owner.close() }) else null, null, null)
        assertEquals(setOf(PlaybackRetainedResource.VideoCodec), result.retained)
        assertTrue(owner.retained)
        assertEquals(1, releases)
        assertSame(release, result.failure)
        assertEquals(listOf<Throwable>(release), start.suppressed.toList())
    }

    @Test fun `actual audio factory inspection failure retains allocation when release raises`() {
        for (primary in listOf<Throwable>(IllegalStateException("inspect"), AssertionError("inspect"), CancellationException("inspect"))) {
            val native = Any()
            val cleanup = AssertionError("release")
            var releases = 0
            val factory = object : PlaybackAudioFactory<Any> {
                override fun minimumBytes(pcm: PlaybackPcm): Int = 16
                override fun create(pcm: PlaybackPcm, bufferBytes: Int): Any = native
                override fun initialized(track: Any): Boolean { assertSame(native, track); throw primary }
                override fun release(track: Any) { releases++; throw cleanup }
            }
            val owner = PlaybackResourceOwner(factory::release)
            assertSame(primary, assertFailsWith<Throwable> {
                createPlaybackAudio(PlaybackOutputFormat("audio/raw", sampleRate = 48_000, channels = 2), 23, factory, owner)
            })
            val closed = disposePlaybackRender(null, null, if (owner.acquired) ({ owner.close() }) else null)
            assertEquals(setOf(PlaybackRetainedResource.AudioTrack), closed.retained)
            assertTrue(owner.retained)
            assertEquals(1, releases)
            assertEquals(listOf<Throwable>(cleanup), primary.suppressed.toList())
        }
    }

    @Test fun `setup failure with successful release retains nothing and cannot self suppress`() {
        val primary = CancellationException("setup")
        val good = PlaybackResourceOwner<Any> {}
        assertSame(primary, assertFailsWith<CancellationException> { good.prepare({ Any() }) { throw primary } })
        assertTrue(good.acquired)
        assertFalse(good.retained)
        good.close()
        val same = PlaybackResourceOwner<Any> { throw primary }
        assertSame(primary, assertFailsWith<CancellationException> { same.prepare({ Any() }) { throw primary } })
        assertTrue(same.retained)
        assertTrue(primary.suppressed.isEmpty())
    }

    @Test fun `constructor failure owns nothing and does not run setup or disposal`() {
        val primary = IllegalStateException("allocation")
        val owner = PlaybackResourceOwner<Any> { error("nothing was allocated") }
        assertSame(primary, assertFailsWith<IllegalStateException> { owner.prepare({ throw primary }) { error("no setup") } })
        assertFalse(owner.acquired)
        assertFalse(owner.retained)
        owner.close()
    }
}
