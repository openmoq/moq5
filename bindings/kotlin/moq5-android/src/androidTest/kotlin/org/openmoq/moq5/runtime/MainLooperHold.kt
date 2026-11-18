package org.openmoq.moq5.runtime

import android.os.Handler
import android.os.Looper
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The main-thread-independence oracle.
 *
 * "Does not block the main thread" is only evidence when the operation is
 * shown to COMPLETE while the main looper is provably unavailable. Watching a
 * looper stay lively passes for anything that finishes before the first tick;
 * holding the looper and then ignoring whether the hold expired passes for
 * anything that finishes once the hold gives up. This helper closes both
 * gaps by ordering four facts and refusing to conflate them:
 *
 *   1. the holder has ENTERED the main looper before the work starts;
 *   2. the work has its own deadline, strictly shorter than the holder's
 *      emergency timeout, so "still held" is not a wall-clock coincidence;
 *   3. success requires the work to have completed AND the holder to still
 *      be blocked AND the holder never to have expired;
 *   4. the hold is always released afterwards, and the looper must service a
 *      later task, so a passing or failing run leaves the process usable.
 *
 * Every wait here is bounded. The verdict is a closed value, so a test asserts
 * the classification it expects rather than reading a boolean that a timeout
 * could have produced by accident.
 */
internal object MainLooperHold {

    sealed interface Verdict<out T> {
        /** The work finished while the main looper was still held. */
        data class Independent<T>(val value: T) : Verdict<T>

        /** The work did not finish before its deadline while the looper was held. */
        data class MainThreadDependent(val workDeadlineMillis: Long) : Verdict<Nothing>

        /** The work threw. Its own failure is retained, never re-labelled. */
        data class Failed(val cause: Throwable) : Verdict<Nothing>

        /** The harness itself could not establish or release the hold. */
        data class HarnessFailure(val reason: String) : Verdict<Nothing>
    }

    /**
     * Hold the main looper, run [work] on a worker, and classify.
     *
     * [workDeadlineMillis] bounds the work; [holdTimeoutMillis] is the holder's
     * emergency release and MUST be longer, so a work deadline can never be
     * answered by the hold quietly ending first. [recoveryMillis] bounds the
     * proof that the looper services a task again after release.
     */
    fun <T> run(
        workDeadlineMillis: Long,
        holdTimeoutMillis: Long,
        recoveryMillis: Long,
        work: () -> T,
    ): Verdict<T> {
        require(holdTimeoutMillis > workDeadlineMillis) {
            "the hold ($holdTimeoutMillis ms) must outlast the work deadline ($workDeadlineMillis ms)"
        }
        val mainLooper = Looper.getMainLooper()
        if (Looper.myLooper() === mainLooper) {
            return Verdict.HarnessFailure("the oracle itself is on the main thread and cannot hold it")
        }
        val handler = Handler(mainLooper)
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val exited = CountDownLatch(1)
        // Written by the holder, read by this thread BEFORE release to see
        // whether the emergency timeout fired.
        val expired = AtomicBoolean(false)

        handler.post {
            entered.countDown()
            try {
                expired.set(!release.await(holdTimeoutMillis, TimeUnit.MILLISECONDS))
            } catch (interrupted: InterruptedException) {
                Thread.currentThread().interrupt()
                expired.set(true)
            } finally {
                exited.countDown()
            }
        }

        val executor = Executors.newSingleThreadExecutor { r -> Thread(r, "moq5-main-looper-hold-work") }
        var future: Future<T>? = null
        try {
            if (!entered.await(holdTimeoutMillis, TimeUnit.MILLISECONDS)) {
                return Verdict.HarnessFailure("the main looper never entered the holding runnable")
            }

            val submitted = executor.submit<T> { work() }
            future = submitted
            val verdict: Verdict<T> =
                try {
                    val value = submitted.get(workDeadlineMillis, TimeUnit.MILLISECONDS)
                    // Completion alone is not the fact. The holder must still
                    // be inside its wait, and must not have expired: both are
                    // read here, before anything releases it.
                    if (expired.get() || exited.count == 0L) {
                        Verdict.HarnessFailure("the hold ended before the work completed")
                    } else {
                        Verdict.Independent(value)
                    }
                } catch (timeout: TimeoutException) {
                    submitted.cancel(true)
                    Verdict.MainThreadDependent(workDeadlineMillis)
                } catch (execution: ExecutionException) {
                    Verdict.Failed(execution.cause ?: execution)
                }

            // Release, then judge recovery. A recovery failure is computed, not
            // returned: once a worker has been submitted EVERY path leaves
            // through the one bounded reap below, so no branch can hand a
            // still-running worker to the next observation.
            release.countDown()
            var harness: String? = null
            if (!exited.await(recoveryMillis, TimeUnit.MILLISECONDS)) {
                harness = "the holder did not exit after release"
            } else {
                val recovered = CountDownLatch(1)
                handler.post { recovered.countDown() }
                if (!recovered.await(recoveryMillis, TimeUnit.MILLISECONDS)) {
                    harness = "the main looper did not service a task after release"
                }
            }

            // REAP the worker, bounded. A cancelled work function that ignores
            // its interrupt would otherwise outlive this call and contaminate
            // whatever runs next, so a worker still alive after the bound is a
            // named harness failure that overrides the expected dependent
            // verdict. A worker that did exit leaves the work's own outcome --
            // including its Failed cause -- exactly as judged above. When a
            // recovery failure and a surviving worker coincide, both are named.
            val reaped = reap(executor, recoveryMillis)
            return when {
                harness != null && !reaped -> Verdict.HarnessFailure("$harness, and $WORKER_SURVIVED")
                harness != null -> Verdict.HarnessFailure(harness)
                !reaped -> Verdict.HarnessFailure(WORKER_SURVIVED)
                else -> verdict
            }
        } catch (interrupted: InterruptedException) {
            // This thread was interrupted mid-oracle. The flag is restored for
            // the caller, and the hold and the worker are still cleaned up
            // within the same bound before the failure is reported.
            release.countDown()
            future?.cancel(true)
            val reaped = reap(executor, recoveryMillis)
            Thread.currentThread().interrupt()
            return Verdict.HarnessFailure(
                if (reaped) "the oracle was interrupted" else "the oracle was interrupted and $WORKER_SURVIVED",
            )
        } finally {
            release.countDown()
            executor.shutdownNow()
        }
    }

    /**
     * Shut the executor down, interrupting a still-running worker, and wait a
     * bounded time for it to terminate. True only when the worker is gone.
     */
    private fun reap(executor: ExecutorService, boundMillis: Long): Boolean {
        executor.shutdownNow()
        return try {
            executor.awaitTermination(boundMillis, TimeUnit.MILLISECONDS)
        } catch (interrupted: InterruptedException) {
            Thread.currentThread().interrupt()
            false
        }
    }

    /** The diagnostic a surviving worker produces; tests match it by name. */
    const val WORKER_SURVIVED: String = "the work thread survived cleanup and is still running"
}
