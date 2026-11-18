package org.openmoq.moq5.samples.player

import android.util.Log
import java.util.concurrent.atomic.AtomicInteger

/**
 * Diagnostic-only progress breadcrumbs for the acceptance harness's unfiltered log stream, under
 * [TAG]. Off unless the diagnostic trace is switched on. At most [LIMIT] numbered lines per process,
 * then one line saying the rest are dropped. Nothing reads them back: they change no verdict,
 * scheduling, ownership or rendering, and a caller never waits on them.
 */
internal object PlaybackTrace {
    const val TAG = "MoqPlayerDiag"
    // A finite A/V diagnostic emits about seven lines per object (≈1250 for 60 video + 118 audio
    // objects) before its closing summary; the bound keeps that whole run with room to spare.
    const val LIMIT = 4096

    @Volatile
    var enabled: Boolean = false

    @Volatile
    var sink: (String) -> Unit = { Log.i(TAG, it) }

    private val lines = AtomicInteger()

    fun mark(event: () -> String) {
        if (!enabled) return
        val n = lines.incrementAndGet()
        if (n <= LIMIT) sink("$n ${event()}")
        else if (n == LIMIT + 1) sink("$n breadcrumbs capped at $LIMIT; later ones are dropped")
    }

    /** Entry and return around one synchronous platform call: a missing return names where it stopped. */
    inline fun <T> around(name: String, call: () -> T): T {
        mark { "enter $name" }
        val result = call()
        mark { "return $name" }
        return result
    }

    fun resetForTest() = lines.set(0)
}
