package org.openmoq.moq5.samples.player

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.runBlocking
import org.openmoq.moq5.ReceiverStats
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The diagnostic stats probe: armed only in a debuggable build with the trace switch on (API 33+),
 * delivered off the main thread, at most one admitted request per session, nothing reaching a retired
 * receiver, and a listener whose owned thread and registration are cleaned up on every failure.
 */
class PlaybackDiagnosticsTest {
    private val seen = mutableListOf<String>()
    private val defaultSink = PlaybackTrace.sink
    private val stats = ReceiverStats(60u, 57u, 30000u, 0u, 0u, 0u, 0u, 0u, 0u, false, 0u, true, 0u, 0u)

    @BeforeTest
    fun capture() {
        PlaybackTrace.resetForTest()
        PlaybackTrace.enabled = true
        PlaybackTrace.sink = { synchronized(seen) { seen.add(it) } }
    }

    @AfterTest
    fun restore() {
        PlaybackDiagnostics.resetForTest()
        PlaybackTrace.enabled = false
        PlaybackTrace.sink = defaultSink
        PlaybackTrace.resetForTest()
    }

    private fun lines() = synchronized(seen) { seen.map { it.substringAfter(' ') } }

    private fun requestAndJoin() = runBlocking {
        val job = Job()
        PlaybackDiagnostics.request(CoroutineScope(Dispatchers.Default + job))
        job.children.forEach { it.join() }
        job.complete()
        job.join()
    }

    @Test
    fun armedOnlyInADebuggableBuildWithTheTraceOnAndTheExportFlag() {
        assertTrue(PlaybackDiagnostics.armed(debuggable = true, trace = true, sdk = 33))
        assertFalse(PlaybackDiagnostics.armed(debuggable = false, trace = true, sdk = 36))       // release
        assertFalse(PlaybackDiagnostics.armed(debuggable = true, trace = false, sdk = 36))       // unarmed
        assertFalse(PlaybackDiagnostics.armed(debuggable = true, trace = true, sdk = 32))
    }

    @Test
    fun oneRequestPerSessionIsAdmittedAndItsAnswerIsThePublicStats() {
        val session = PlaybackDiagnostics.open { stats }
        requestAndJoin()
        requestAndJoin()                                               // a duplicate delivery
        PlaybackDiagnostics.close(session)
        assertEquals(listOf(
            "stats-request",
            "stats objectsReceived=60 objectsQueued=57 bytesQueued=30000 objectsDropped=0 groupsDropped=0 " +
                "keyframesDropped=0 parseDrops=0 overflowEvents=0 paused=false catalogComplete=true",
            "stats-request",
            "stats-request refused: one request per session already admitted",
        ), lines())
    }

    @Test
    fun aRequestAfterTheSessionRetiredReachesNoReceiver() {
        var reads = 0
        val session = PlaybackDiagnostics.open { reads++; stats }
        PlaybackDiagnostics.close(session)
        requestAndJoin()
        assertEquals(0, reads)
        assertEquals(listOf("stats-request", "stats-none: no live receiver"), lines())
    }

    @Test
    fun aStatsReadThatFailsIsRecordedNotThrown() {
        val session = PlaybackDiagnostics.open { error("receiver closed") }
        requestAndJoin()
        PlaybackDiagnostics.close(session)
        assertEquals(listOf("stats-request", "stats-error java.lang.IllegalStateException: receiver closed"), lines())
    }

    @Test
    fun retiringTheSessionCancelsItsAdmittedRead() {
        val entered = CountDownLatch(1)
        val session = PlaybackDiagnostics.open { entered.countDown(); kotlinx.coroutines.awaitCancellation() }
        val job = Job()
        PlaybackDiagnostics.request(CoroutineScope(Dispatchers.Default + job))
        assertTrue(entered.await(5, TimeUnit.SECONDS))
        PlaybackDiagnostics.close(session)
        runBlocking { job.children.forEach { it.join() } }
        assertEquals(listOf("stats-request", "stats-cancelled: session retired"), lines())
    }

    @Test
    fun aHeldMainThreadCannotPreventTheDiagnosticDispatch() {
        val main = Executors.newSingleThreadExecutor { Thread(it, "main") }
        val release = CountDownLatch(1)
        try {
            main.execute { release.await() }                           // the main thread, held
            PlaybackDiagnostics.open { stats }
            val listenerThread = Executors.newSingleThreadExecutor { Thread(it, "moq5-player-diagnostics") }
            val done = CountDownLatch(1)
            listenerThread.execute { requestAndJoin(); done.countDown() }
            assertTrue(done.await(5, TimeUnit.SECONDS), "the dispatch waited for the held main thread")
            listenerThread.shutdown()
            assertTrue(lines().any { it.startsWith("stats objectsReceived=60") })
        } finally {
            release.countDown()
            main.shutdown()
            main.awaitTermination(5, TimeUnit.SECONDS)
        }
    }

    private class Resources {
        val calls = mutableListOf<String>()
        var failStart = false
        var failRegister = false

        fun owner() = DiagnosticsListenerOwner(
            startThread = { calls.add("start"); if (failStart) error("no thread") },
            register = { calls.add("register"); if (failRegister) error("refused") },
            unregister = { calls.add("unregister") },
            quitThread = { calls.add("quit") },
        )
    }

    @Test
    fun aStartOrRegistrationRefusalLeavesNothingOwned() {
        val a = Resources().apply { failStart = true }
        assertFalse(a.owner().open())
        assertEquals(listOf("start"), a.calls)
        val b = Resources().apply { failRegister = true }
        assertFalse(b.owner().open())
        assertEquals(listOf("start", "register", "quit"), b.calls)
    }

    @Test
    fun onlyTheExactActionIsServedAndTheFirstRetiresTheListener() {
        val r = Resources()
        val owner = r.owner()
        assertTrue(owner.open())
        var requests = 0
        owner.delivered("some.other.ACTION") { requests++ }
        assertEquals(0, requests)
        owner.delivered(PlaybackDiagnostics.ACTION) { requests++ }
        owner.delivered(PlaybackDiagnostics.ACTION) { requests++ }       // raced in before the unregister
        owner.retire()                                                   // the Activity's teardown, afterwards
        assertEquals(1, requests)
        assertEquals(listOf("start", "register", "unregister", "quit"), r.calls)
    }
}
