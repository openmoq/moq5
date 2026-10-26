package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext

/**
 * The sample's own resource lifetime.
 *
 * Four rules, each one a defect this replaces:
 *  * OWNERSHIP STARTS AT ACQUISITION. The endpoint is owned the moment
 *    `connect` returns, so a failure or cancellation between connect and
 *    attach still closes it.
 *  * CHILD BEFORE ENDPOINT, ONCE EACH, ALWAYS ATTEMPTED. Every successfully
 *    acquired resource is closed even when an earlier close FAILED: the
 *    outcome of the whole cleanup is established first, and only then is a
 *    failure propagated.
 *  * CLEANUP IS NOT CANCELLABLE. The closes run under [NonCancellable],
 *    because the library's own close goes through the engine's `perform`,
 *    which calls `ensureActive()`: submitted from an already-cancelled session
 *    it need not run at all. That stops INHERITED cancellation; it does not
 *    stop a close from raising its own, so a close's `CancellationException`
 *    is COLLECTED like any other close failure rather than abandoning the
 *    resources after it or replacing an already-established primary.
 *  * THE PRIMARY FAILURE IS THE ACTUAL ONE. Whatever ended the body --
 *    including the exact [CancellationException] instance, and an [Error] --
 *    is what propagates, with every cleanup failure attached to it in order.
 *    It is never inferred from Job state, never replaced by a cleanup failure,
 *    and never reduced to a UI message a delivery guard may discard.
 *
 * Every one of those rules holds across the SAME three domains on both sides.
 * A body that ends with an `Error` is a primary like any other, and a close
 * that raises one is a collected cleanup failure like any other: it may not
 * abandon the resources behind it, and it may not replace what is already
 * leaving. This makes no promise about recovering from an `OutOfMemoryError`;
 * it is about not losing the failure that happened.
 */
internal suspend fun <E : Any, C : Any> broadcastLifetime(
    connect: suspend () -> E,
    describe: suspend (E) -> Unit,
    attach: suspend (E) -> C,
    run: suspend (E, C) -> Unit,
    closeChild: suspend (C) -> Unit,
    closeEndpoint: suspend (E) -> Unit,
) {
    val endpoint = connect()
    var child: C? = null
    var primary: Throwable? = null
    try {
        describe(endpoint)
        child = attach(endpoint)
        run(endpoint, child)
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
        // Every acquired resource is attempted, in order, and the failures are
        // COLLECTED rather than thrown from inside the sequence: a throw there
        // would skip the closes that come after it, and throwing from inside
        // `withContext` would hand back a coroutine-recovered COPY, losing the
        // identity and the attached failures this contract is about.
        val failures = mutableListOf<Throwable>()
        withContext(NonCancellable) {
            val acquired = child
            if (acquired != null) {
                try {
                    closeChild(acquired)
                } catch (cancelled: CancellationException) {
                    failures.add(cancelled)
                } catch (closeFailure: Exception) {
                    failures.add(closeFailure)
                } catch (closeError: Error) {
                    failures.add(closeError)
                }
            }
            try {
                closeEndpoint(endpoint)
            } catch (cancelled: CancellationException) {
                failures.add(cancelled)
            } catch (closeFailure: Exception) {
                failures.add(closeFailure)
            } catch (closeError: Error) {
                failures.add(closeError)
            }
        }

        val ending = primary
        if (ending != null) {
            failures.forEach { if (it !== ending) ending.addSuppressed(it) }
        } else if (failures.isNotEmpty()) {
            // No primary: the FIRST cleanup failure is the outcome and any
            // later one is attached to it.
            val first = failures.first()
            failures.drop(1).forEach { if (it !== first) first.addSuppressed(it) }
            throw first
        }
    }
}
