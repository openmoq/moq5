package org.openmoq.moq5.samples.player

import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** The breadcrumbs are diagnostic only: off by default, bounded, and they never alter the traced call. */
class PlaybackTraceTest {
    private val seen = mutableListOf<String>()
    private val defaultSink = PlaybackTrace.sink

    @BeforeTest
    fun capture() {
        PlaybackTrace.resetForTest()
        PlaybackTrace.sink = { seen.add(it) }
    }

    @AfterTest
    fun restore() {
        PlaybackTrace.enabled = false
        PlaybackTrace.sink = defaultSink
        PlaybackTrace.resetForTest()
    }

    @Test
    fun offItWritesNothingAndStillRunsTheCall() {
        var ran = 0
        assertEquals(7, PlaybackTrace.around("call") { ran++; 7 })
        PlaybackTrace.mark { error("an event is never built while tracing is off") }
        assertEquals(1, ran)
        assertEquals(emptyList(), seen)
    }

    @Test
    fun onItNumbersEachLineAndStopsAfterItsBound() {
        PlaybackTrace.enabled = true
        repeat(PlaybackTrace.LIMIT + 5) { i -> PlaybackTrace.mark { "event $i" } }
        assertEquals(PlaybackTrace.LIMIT + 1, seen.size)
        assertEquals("1 event 0", seen.first())
        assertEquals("${PlaybackTrace.LIMIT} event ${PlaybackTrace.LIMIT - 1}", seen[PlaybackTrace.LIMIT - 1])
        assertEquals("${PlaybackTrace.LIMIT + 1} breadcrumbs capped at ${PlaybackTrace.LIMIT}; later ones are dropped", seen.last())
    }

    @Test
    fun aCallThatThrowsLeavesOnlyItsEntry() {
        PlaybackTrace.enabled = true
        assertEquals(3, PlaybackTrace.around("queueInputBuffer") { 3 })
        assertFailsWith<IllegalStateException> { PlaybackTrace.around("releaseOutputBuffer") { error("codec") } }
        assertEquals(listOf("1 enter queueInputBuffer", "2 return queueInputBuffer", "3 enter releaseOutputBuffer"), seen)
    }
}
