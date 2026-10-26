package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch

/**
 * Where a UI update is delivered from.
 *
 * The Activity implements it with `runOnUiThread`: produced on the session's
 * thread, delivered on the UI thread, with an unavoidable gap in between.
 */
internal fun interface UiDispatch {
    fun post(action: () -> Unit)
}

/** What the session tells the UI. */
internal sealed interface SessionUpdate {
    public data object Started : SessionUpdate

    public data class Reported(public val line: String) : SessionUpdate

    public data class Failed(public val failure: Exception) : SessionUpdate

    public data object Stopped : SessionUpdate
}

/**
 * How a running session speaks to the UI.
 *
 * Each one is bound to the session that was given it, so an old session that
 * still holds a reporter cannot have its lines mistaken for the current
 * session's: the identity travels WITH the reporter and is never read from
 * whatever happens to be running when it is called.
 */
internal fun interface SessionReporter {
    fun report(line: String)
}

/**
 * The Start/Stop lifecycle of the sample.
 *
 * THREAD CONFINEMENT, stated because it is the whole safety argument:
 * `toggle`, `destroy` and the render callback run on the UI thread -- the
 * Activity calls the first two from a click and from `onDestroy`, and the
 * third only ever runs inside a [UiDispatch] post. All mutable state here is
 * touched on that thread and nowhere else. A session's work and its reporter
 * run on `Dispatchers.Default`, and the ONLY thing they do to this object is
 * `post`, which hands the decision back to the UI thread. No state is shared
 * across threads, so no lock is needed and none is implied.
 *
 * Two rules follow from the defects this replaces:
 *  * RETIREMENT IS JOB COMPLETION. Not the body's `finally`: a session
 *    cancelled before its body ran never has one, and a body can return with a
 *    child of the same job still finishing. `Stopped` is rendered from the
 *    completion handler, once, after the whole job has settled -- which is
 *    the JOB completing, not the body's `finally` having run.
 *  * SERIALISATION -- one session at a time. While a session is stopping,
 *    Start is IGNORED rather than queued: a hidden queue of restarts is a
 *    worse surprise than a button that does nothing for a moment. A new Start
 *    is accepted once the previous session's JOB has completed -- not its
 *    body's `finally` having run, and not its job merely being inactive.
 *  * DELIVERY -- every update carries the generation that produced it, and the
 *    guard is read WHEN IT IS DELIVERED. Dropping early is fine as an
 *    optimisation but cannot replace the delivery check, because the gap
 *    between production and delivery is exactly where a session ends or an
 *    Activity is destroyed.
 */
internal class SessionController(
    private val scope: CoroutineScope,
    private val ui: UiDispatch,
    private val render: (SessionUpdate) -> Unit,
) {
    private class Running(val generation: Long) {
        var job: Job? = null
        var stopping: Boolean = false
        var retired: Boolean = false
    }

    private var generation = 0L
    private var running: Running? = null
    private var destroyed = false

    /** Stop the running session, or start a new one. UI thread only. */
    fun toggle(work: suspend (SessionReporter) -> Unit) {
        if (destroyed) return
        val current = running
        if (current != null) {
            // Already stopping: ignore, do not queue a restart.
            if (!current.stopping) {
                current.stopping = true
                current.job?.cancel()
            }
            return
        }
        start(work)
    }

    /**
     * Starts [work] only when nothing is running or stopping, and never stops
     * anything: the live launcher's entry, so a permission result can never
     * act as a Stop. False when it started nothing. UI thread only.
     */
    fun startIfIdle(work: suspend (SessionReporter) -> Unit): Boolean {
        if (!idle()) return false
        start(work)
        return true
    }

    /** Nothing is running or stopping, and the owner is alive. UI thread only. */
    fun idle(): Boolean = !destroyed && running == null

    /** One new session through the one lifecycle both entries share. */
    private fun start(work: suspend (SessionReporter) -> Unit) {
        val session = Running(++generation)
        // The Job is created LAZY and installed as this session's ownership
        // BEFORE anything is rendered, because a render runs live: a Stop or a
        // destroy issued from inside the Started render must find a real Job to
        // cancel. Nothing has been dispatched yet, so installing it first
        // cannot start work early.
        val job =
            scope.launch(start = CoroutineStart.LAZY) {
                try {
                    work(reporterFor(session))
                } catch (cancelled: CancellationException) {
                    // Stop and Activity teardown arrive as cancellation, and
                    // cancellation IS an Exception: the generic catch below
                    // would report an ordinary stop as a failure and swallow
                    // the signal structured concurrency runs on.
                    throw cancelled
                } catch (failure: Exception) {
                    deliver(session.generation, SessionUpdate.Failed(failure))
                }
            }
        session.job = job
        running = session

        deliver(session.generation, SessionUpdate.Started)

        // RETIREMENT IS A JOB FACT: not the body's `finally`, which a session
        // cancelled before its body was entered never runs, and which a body
        // can leave behind with a child of the same job still finishing.
        // `invokeOnCompletion` fires for every one of those exactly once --
        // including immediately, for a Job that is already complete -- and it
        // is registered AFTER Started so that immediate firing cannot invert
        // the rendered order.
        job.invokeOnCompletion {
            ui.post {
                if (!session.retired) {
                    session.retired = true
                    if (!destroyed && running?.generation == session.generation) {
                        render(SessionUpdate.Stopped)
                    }
                    if (running === session) running = null
                }
            }
        }
        // A Stop or destroy that arrived while the state above was being built
        // has already cancelled this Job, and `start()` on a cancelled Job runs
        // nothing.
        job.start()
    }

    /** Activity teardown: permanent and idempotent. */
    fun destroy() {
        destroyed = true
        running?.let {
            it.stopping = true
            it.job?.cancel()
        }
    }

    /** A reporter bound to ONE session; identity travels with it. */
    private fun reporterFor(session: Running): SessionReporter =
        SessionReporter { line -> deliver(session.generation, SessionUpdate.Reported(line)) }

    private fun deliver(generation: Long, update: SessionUpdate) {
        ui.post {
            // Read at DELIVERY: the session may have ended, or the owner may
            // have been destroyed, while this update sat in the gap.
            if (!destroyed && running?.generation == generation) {
                render(update)
            }
        }
    }
}
