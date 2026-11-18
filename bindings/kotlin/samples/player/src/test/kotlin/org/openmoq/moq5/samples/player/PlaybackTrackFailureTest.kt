package org.openmoq.moq5.samples.player

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * A selected track that the receiver failed locally ends playback with a
 * named error instead of waiting for objects that can no longer arrive; the
 * pending read is cancelled, and nothing is claimed about the peer.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class PlaybackTrackFailureTest {
    @Test fun `the failed lane is the video's first, then the audio's, and none while both stand`() {
        assertEquals(PlaybackLane.Video, failedSelection("v", "a") { true })
        assertEquals(PlaybackLane.Audio, failedSelection("v", "a") { it == "a" })
        assertNull(failedSelection("v", "a") { false })
        assertNull(failedSelection("v", null) { it == "a" }, "no audio selected: only the video can fail")
    }

    @Test fun `a selected track's failure ends a waiting read with its named error and cancels the read`() = runTest {
        val failure = CompletableDeferred<Throwable>()
        var readCancelled = false
        val read = async {
            runCatching {
                nextUnlessFailed({
                    try { CompletableDeferred<String>().await() } finally { readCancelled = true }
                }) { failure.await() }
            }
        }
        runCurrent()
        assertFalse(read.isCompleted, "nothing has arrived and nothing has failed")
        val named = PlaybackTrackFailedException(PlaybackLane.Audio)
        failure.complete(named)
        val outcome = read.await()
        assertSame(named, outcome.exceptionOrNull())
        assertEquals("the selected audio track failed locally in the receiver", named.message)
        assertTrue(readCancelled, "the pending read was cancelled, not left behind")
    }

    @Test fun `an object that arrives first is returned and the waiting watch is cancelled`() = runTest {
        var watchCancelled = false
        // The read yields once, so the watch is already waiting when the object arrives.
        val got = nextUnlessFailed({ yield(); "object" }) {
            try { CompletableDeferred<Throwable>().await() } finally { watchCancelled = true }
        }
        assertEquals("object", got)
        assertTrue(watchCancelled, "the waiting watch was cancelled, not left behind")
        assertNull(nextUnlessFailed({ null }) { CompletableDeferred<Throwable>().await() }, "the receiver's clean end passes through")
    }
}
