@file:OptIn(ExperimentalCoroutinesApi::class)

package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withContext

/*
 * The settlement substrate the broadcast rows share: a run owned on its own
 * frame, every row cancellation issued through one owner, and cleanup that
 * attaches what it finds to the failure that is leaving rather than replacing
 * it. Host fixtures, not product.
 */

/**
 * The suspend-capable form of `withSettlement`: every gate this row owns
 * is opened and every worker joined on EVERY path, and what the cleanup
 * finds is attached to the failure that is leaving.
 */
internal suspend fun <T> settling(gates: List<HostGate>, workers: List<HostWorker>, body: suspend () -> T): T {
    var primary: Throwable? = null
    try {
        return body()
    } catch (cancelled: CancellationException) {
        primary = cancelled
        throw cancelled
    } catch (failure: Exception) {
        primary = failure
        throw failure
    } catch (error: Error) {
        primary = error
        throw error
    } finally {
        for (gate in gates) gate.openFully()
        val found = mutableListOf<Throwable>()
        for (worker in workers) {
            try {
                worker.join()
            } catch (failure: RuntimeException) {
                found.add(failure)
            } catch (interrupted: InterruptedException) {
                found.add(interrupted)
            } catch (error: Error) {
                found.add(error)
            }
        }
        val leaving = primary
        if (leaving != null) {
            found.forEach { if (it !== leaving) leaving.addSuppressed(it) }
        } else {
            found.firstOrNull()?.let { first ->
                found.drop(1).forEach { if (it !== first) first.addSuppressed(it) }
                throw first
            }
        }
    }
}

/**
 * The running broadcast, with its outcome taken on the run's OWN frame.
 *
 * `await` hands back a stack-trace-recovered copy rather than the child's
 * Result, so what the rows compare is what [observed] recorded. Every
 * cancellation this row issues goes through [cancel], so the cleanup can
 * tell the row's own cancellation apart from a failure the broadcast
 * really died of -- which is the one thing `isCancelled` cannot say.
 */
internal class RunOwner(val job: Deferred<BroadcastOutcome>) {
    private val mine = mutableListOf<CancellationException>()
    private var seen: Result<BroadcastOutcome>? = null

    @Synchronized
    fun record(result: Result<BroadcastOutcome>) {
        if (seen == null) seen = result
    }

    @get:Synchronized
    val reached: Boolean get() = seen != null

    @Synchronized
    fun observed(): Result<BroadcastOutcome> =
        seen ?: throw AssertionError("the broadcast never reached its own boundary")

    fun cancel(why: String): CancellationException {
        val cause = CancellationException(why)
        synchronized(this) { mine.add(cause) }
        job.cancel(cause)
        return cause
    }

    /** Whether [raised] is one of the cancellations this row itself issued, or a copy of one. */
    @Synchronized
    fun issued(raised: Throwable): Boolean =
        raised is CancellationException && generateSequence<Throwable>(raised) { it.cause }.any { step -> mine.any { it === step } }

    /**
     * What the settlement left behind that this row did not ask for.
     *
     * A cancellation this row issued is CLEAN only when it carries
     * nothing. Accepted cleanup keeps the cancellation as the carrier and
     * attaches what it found to it, so the carrier is READ rather than
     * assumed clean -- neither [issued] nor `isCancelled` can say that on
     * its own. The caller-frame observation is read alongside the job's
     * own exception, because a stack-trace-recovered copy is not the
     * object the cleanup wrote into.
     */
    @Synchronized
    fun cleanupFacts(died: Throwable?): List<Throwable> {
        val facts = mutableListOf<Throwable>()
        fun keep(fact: Throwable) {
            if (issued(fact)) return
            if (facts.none { it === fact }) facts.add(fact)
        }
        for (carrier in listOfNotNull(died, seen?.exceptionOrNull())) {
            if (issued(carrier)) carrier.suppressed.forEach { keep(it) } else keep(carrier)
        }
        return facts
    }
}

/**
 * Runs one broadcast for the length of [body] and settles it on EVERY
 * path, under `NonCancellable` so the settlement finishes even when the
 * row is leaving under a cancellation of its own.
 *
 * What the job died of is never discarded: a failure that is not one of
 * this row's cancellations leaves with the body's failure, or by itself
 * when the body had none.
 */
internal suspend fun <T> withRun(run: suspend () -> BroadcastOutcome, body: suspend (RunOwner) -> T): T =
    supervisorScope {
        lateinit var owner: RunOwner
        val job =
            async {
                val result = runCatching { run() }
                owner.record(result)
                result.getOrThrow()
            }
        owner = RunOwner(job)
        var primary: Throwable? = null
        try {
            return@supervisorScope body(owner)
        } catch (cancelled: CancellationException) {
            primary = cancelled
            throw cancelled
        } catch (failure: Exception) {
            primary = failure
            throw failure
        } catch (error: Error) {
            primary = error
            throw error
        } finally {
            owner.cancel("the row is finished")
            withContext(NonCancellable) { job.join() }
            val facts = owner.cleanupFacts(job.getCompletionExceptionOrNull())
            val leaving = primary
            if (facts.isNotEmpty()) {
                if (leaving == null) {
                    val first = facts.first()
                    facts.drop(1).forEach { if (it !== first) first.addSuppressed(it) }
                    throw first
                }
                facts.forEach { fact ->
                    if (fact !== leaving && leaving.suppressed.none { it === fact }) leaving.addSuppressed(fact)
                }
            }
        }
    }

/**
 * Whether [raised] IS [original], or the stack-trace-recovered copy
 * kotlinx makes of it when it crosses a scope boundary. Measured: a
 * failure thrown inside `supervisorScope` reaches the caller as a copy
 * whose cause chain reaches the original, so identity is only available on
 * the throwing frame.
 */
internal fun recovers(raised: Throwable, original: Throwable): Boolean =
    generateSequence(raised) { it.cause }.any { it === original }


/**
 * The constrained shape of a failure that crossed a coroutine boundary:
 * either the SAME instance, or kotlinx's stack-trace-recovered copy of it --
 * the same class, with the original as its direct cause. Cause-chain
 * reachability alone is NOT identity: an unrelated wrapper around the
 * original is refused here.
 */
internal fun isSameOrRecoveredCopyOf(escaped: Throwable, recorded: Throwable): Boolean =
    escaped === recorded || (escaped::class == recorded::class && escaped.cause === recorded)

/** What an expected-failure boundary produced: the value, or the instance that left. */
internal class Escaped<T>(val value: T?, val failure: Throwable?)

/**
 * Accounts for a failure the run helper deliberately lets out at its
 * boundary, so a row can assert the run's OWN recorded outcome inside the
 * body and then compare what actually escaped against it -- without
 * weakening the helper. `runCatching` is a function, not a catch clause: it
 * takes what leaves and hands it back as data for comparison.
 */
internal suspend fun <T> escaping(block: suspend () -> T): Escaped<T> {
    val outcome = runCatching { block() }
    return Escaped(outcome.getOrNull(), outcome.exceptionOrNull())
}

/**
 * Every hold a row owns is opened on EVERY exit from [body] -- assertion,
 * timeout, failure, `Error` or cancellation -- and BEFORE whatever encloses
 * the body joins a child parked on one of them.
 */
internal suspend fun <T> withHolds(holds: List<CompletableDeferred<Unit>>, body: suspend () -> T): T =
    try {
        body()
    } finally {
        for (hold in holds) hold.complete(Unit)
    }

/**
 * The instance [expected] must be what left the boundary, itself or as its
 * constrained recovered copy. Anything ELSE that left is rethrown as itself,
 * so a row fails with the real cause rather than with a comparison; nothing
 * leaving at all is a named failure.
 */
internal fun expectLeft(escaped: Escaped<*>, expected: Throwable) {
    val out = escaped.failure ?: throw AssertionError("expected $expected to leave the boundary, but nothing did")
    if (!isSameOrRecoveredCopyOf(out, expected)) throw out
}

/** Nothing may leave the boundary; whatever did is rethrown as itself. */
internal fun expectNothingLeft(escaped: Escaped<*>) {
    escaped.failure?.let { throw it }
}

/**
 * The run's OWN recorded outcome must be [expected] by identity. A different
 * recorded failure is rethrown as itself, so the row fails with the real
 * cause; a recorded success is a named failure.
 */
internal fun expectRecorded(recorded: Result<*>, expected: Throwable): Throwable {
    val out = recorded.exceptionOrNull() ?: throw AssertionError("expected $expected to be recorded on the run's frame, but the run succeeded with ${recorded.getOrNull()}")
    if (out !== expected) throw out
    return out
}

/**
 * A recorded cancellation is the ISSUED one when it is that instance, or the
 * pinned runtime's constrained reconstruction of it -- the same runtime
 * class with the issued instance as its DIRECT cause. Measured: under `-ea`
 * the run's frame can record the reconstruction, under `-da` the instance.
 * An unrelated cancellation with the same message, another Throwable type
 * wrapping the issued cause, and a deeper cause chain are all refused.
 */
internal fun isIssuedCancellation(recorded: Throwable, issued: CancellationException): Boolean =
    recorded is CancellationException && (recorded === issued || (recorded::class == issued::class && recorded.cause === issued))

/**
 * The run's OWN recorded outcome must be the issued cancellation in that
 * constrained sense. Any OTHER recorded failure is rethrown as itself, so an
 * unexpected TODO, Error or refusal still fails the row with its real cause;
 * a recorded success is a named failure. Returns the recorded instance --
 * the primary on which exact root inventories are asserted.
 */
internal fun expectRecordedCancellation(recorded: Result<*>, issued: CancellationException): CancellationException {
    val out = recorded.exceptionOrNull() ?: throw AssertionError("expected the issued cancellation to be recorded on the run's frame, but the run succeeded with ${recorded.getOrNull()}")
    if (!isIssuedCancellation(out, issued)) throw out
    return out as CancellationException
}
