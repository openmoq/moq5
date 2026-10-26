package org.openmoq.moq5.samples.player

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import org.openmoq.moq5.ReceiverStats
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * Diagnostic-only: in a debuggable build with the trace on, the live receiver's public stats can be
 * requested by the harness's [ACTION] broadcast. Each playback session admits at most one request; the
 * request and its one answer are recorded as breadcrumbs. The read is not passive: it runs on the
 * endpoint's service worker, which a request can therefore wake.
 */
internal object PlaybackDiagnostics {
    const val ACTION = "org.openmoq.moq5.samples.player.DIAGNOSTIC_STATS"
    const val TIMEOUT_MS = 5_000L

    /** One playback session's stats read, its one admission, and the work it admitted. */
    class Session internal constructor(internal val read: suspend () -> ReceiverStats) {
        internal val admitted = AtomicBoolean(false)
        internal val work = AtomicReference<Job?>(null)
    }

    private val current = AtomicReference<Session?>(null)

    /**
     * The listener is registered only in a debuggable build, only with the explicit trace switch, and
     * only where the platform takes the explicit export flag (API 33).
     */
    fun armed(debuggable: Boolean, trace: Boolean, sdk: Int): Boolean = debuggable && trace && sdk >= 33

    /** Makes [read] the live session's stats; the returned session must be [close]d. */
    fun open(read: suspend () -> ReceiverStats): Session = Session(read).also { current.set(it) }

    /** Retires [session]: a later request reaches no receiver, and its admitted work is cancelled. */
    fun close(session: Session) {
        current.compareAndSet(session, null)
        session.admitted.set(true)
        session.work.getAndSet(null)?.cancel()
    }

    fun request(scope: CoroutineScope) {
        PlaybackTrace.mark { "stats-request" }
        val session = current.get()
        if (session == null) {
            PlaybackTrace.mark { "stats-none: no live receiver" }
            return
        }
        if (!session.admitted.compareAndSet(false, true)) {
            PlaybackTrace.mark { "stats-request refused: one request per session already admitted" }
            return
        }
        val job = scope.launch(start = CoroutineStart.LAZY) {
            runCatching { withTimeoutOrNull(TIMEOUT_MS) { session.read() } }.fold(
                { answer -> PlaybackTrace.mark { if (answer == null) "stats-timeout ${TIMEOUT_MS}ms" else "stats " + describe(answer) } },
                { failure ->
                    if (failure is CancellationException) {
                        PlaybackTrace.mark { "stats-cancelled: session retired" }
                        throw failure
                    }
                    PlaybackTrace.mark { "stats-error $failure" }
                },
            )
        }
        session.work.set(job)
        // A session retired between admission and here has already cancelled what it could see.
        if (current.get() === session) job.start() else job.cancel()
    }

    internal fun resetForTest() {
        current.getAndSet(null)?.let(::close)
    }

    private fun describe(s: ReceiverStats): String =
        "objectsReceived=${s.objectsReceived} objectsQueued=${s.objectsQueued} bytesQueued=${s.bytesQueued} " +
            "objectsDropped=${s.objectsDropped} groupsDropped=${s.groupsDropped} keyframesDropped=${s.keyframesDropped} " +
            "parseDrops=${s.parseDrops} overflowEvents=${s.overflowEvents} paused=${s.paused} catalogComplete=${s.catalogComplete}"
}

/**
 * The one-shot lifetime of the diagnostic listener and the thread its deliveries run on. [open] starts
 * the thread and registers, owning nothing if either step is refused; the first delivery of the exact
 * action is served and then retires the listener; [retire] is idempotent and safe from any thread. The
 * thread is asked to quit, never joined, so retirement does not wait on it.
 */
internal class DiagnosticsListenerOwner(
    private val startThread: () -> Unit,
    private val register: () -> Unit,
    private val unregister: () -> Unit,
    private val quitThread: () -> Unit,
) {
    private val served = AtomicBoolean(false)
    private val retired = AtomicBoolean(true)

    fun open(): Boolean {
        runCatching(startThread).onFailure { failure ->
            PlaybackTrace.mark { "diagnostics-listener refused: thread $failure" }
            return false
        }
        runCatching(register).onFailure { failure ->
            PlaybackTrace.mark { "diagnostics-listener refused: register $failure" }
            quitThread()
            return false
        }
        retired.set(false)
        return true
    }

    fun delivered(action: String?, request: () -> Unit) {
        if (action != PlaybackDiagnostics.ACTION || !served.compareAndSet(false, true)) return
        try {
            request()
        } finally {
            retire()
        }
    }

    fun retire() {
        if (!retired.compareAndSet(false, true)) return
        try {
            unregister()
        } finally {
            quitThread()
            PlaybackTrace.mark { "diagnostics-listener retired: thread quit requested, not joined" }
        }
    }
}
