package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The publisher's Start/Stop lifecycle, measured separately from the player's
 * because each sample is an independent consumer module with its own copy.
 *
 * Driven through the code its Activity actually runs -- [SessionController] and the [UiDispatch] boundary the
 * Activity implements with `runOnUiThread` -- rather than through a copy.
 *
 * OWNERSHIP. Every session is a child of the test's own scope, and every test
 * body runs inside [lifecycle], whose `finally` releases every barrier this
 * fixture opened, destroys the controller and drains the scheduler WHETHER OR
 * NOT an assertion failed. A failing R row therefore leaves nothing parked, and
 * cleanup is never a timeout oracle: the scheduler is virtual and every wait is
 * explicit.
 *
 * DELIVERY. [Fixture.ui] is the real boundary: `post` enqueues, and nothing
 * reaches the screen until the test drains it. That gap -- produced on the
 * session's thread, delivered on the UI thread -- is where a stale update
 * lives, and holding it open is what makes the R2/R3 rows measurements rather
 * than races.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class SessionLifecycleRedTest {

    /** The UI boundary, the screen it renders to, and the session under test. */
    private class Fixture(val scope: TestScope) {
        val queued = ArrayDeque<() -> Unit>()
        val rendered = mutableListOf<String>()
        val barriers = mutableListOf<CompletableDeferred<Unit>>()

        val jobs = mutableListOf<Job>()

        val controller =
            SessionController(scope, { action -> queued.addLast(action) }) { update ->
                rendered.add(
                    when (update) {
                        is SessionUpdate.Started -> "started"
                        is SessionUpdate.Reported -> "reported(" + update.line + ")"
                        is SessionUpdate.Failed -> "failed(" + update.failure::class.simpleName + ")"
                        is SessionUpdate.Stopped -> "stopped"
                    },
                )
            }

        fun barrier(): CompletableDeferred<Unit> = CompletableDeferred<Unit>().also { barriers.add(it) }

        /** Deliver everything queued so far, in order. */
        fun drain(): Int {
            var n = 0
            while (queued.isNotEmpty()) {
                queued.removeFirst().invoke()
                n++
            }
            return n
        }

        fun idle() {
            scope.testScheduler.advanceUntilIdle()
        }

        /** Every barrier released, the session destroyed, the scheduler drained. */
        fun settle() {
            barriers.forEach { it.complete(Unit) }
            controller.destroy()
            idle()
        }

        /** Owned children that have not COMPLETED -- a cancelling job is neither active nor retired. */
        fun unfinishedChildren(): Int = scope.coroutineContext[Job]!!.children.count { !it.isCompleted }

        fun activeChildren(): Int = scope.coroutineContext[Job]!!.children.count { it.isActive }
    }

    private fun lifecycle(body: (Fixture) -> Unit) =
        runTest {
            val fixture = Fixture(this)
            try {
                body(fixture)
            } finally {
                fixture.settle()
            }
        }

    // ---- P: positive controls, true of the sample today --------------------

    @Test
    fun p1_firstStartRunsTheWorkAndRendersTheExactOrderedSequence() = lifecycle { fx ->
        val trace = mutableListOf<String>()
        fx.controller.toggle { reporter ->
            trace.add("work:ran")
            reporter.report("hello")
        }
        fx.idle()
        fx.drain()
        assertEquals(listOf("work:ran"), trace)
        assertEquals(listOf("started", "reported(hello)", "stopped"), fx.rendered)
    }

    @Test
    fun p2_stopCancelsTheRunningSessionExactlyAndRunsNoReplacement() = lifecycle { fx ->
        val trace = mutableListOf<String>()
        val entered = CompletableDeferred<Unit>()
        fx.controller.toggle { _ ->
            try {
                entered.complete(Unit)
                fx.barrier().await()
            } catch (cancelled: CancellationException) {
                trace.add("work:cancelled")
                throw cancelled
            } finally {
                trace.add("work:teardown")
            }
        }
        fx.idle()
        assertTrue(entered.isCompleted, "the work must have started before it is stopped")

        fx.controller.toggle { _ -> trace.add("replacement:ran") } // Stop
        fx.idle()
        fx.drain()

        // Exact cancellation, exact cleanup, exact completion, and no
        // replacement work: a Stop that simply dropped the cancel would render
        // the same absence of `failed`, so absence alone is not the oracle.
        assertEquals(listOf("work:cancelled", "work:teardown"), trace, "rendered: " + fx.rendered)
        assertEquals(listOf("started", "stopped"), fx.rendered)
        assertEquals(0, fx.activeChildren(), "the cancelled session must be finished, not merely stopped")
    }

    /**
     * A NO-STOP control, and named as one: it withholds the Stop press rather
     * than mutating the product, so it shows what a still-running session looks
     * like. The executed product mutant -- the controller's own cancel removed
     * -- is run separately and is killed by `p2` and `r1`.
     */
    @Test
    fun p2b_aSessionThatWasNeverStoppedKeepsRunning() = lifecycle { fx ->
        val trace = mutableListOf<String>()
        val entered = CompletableDeferred<Unit>()
        fx.controller.toggle { _ ->
            try {
                entered.complete(Unit)
                fx.barrier().await()
            } catch (cancelled: CancellationException) {
                trace.add("work:cancelled")
                throw cancelled
            } finally {
                trace.add("work:teardown")
            }
        }
        fx.idle()
        assertTrue(entered.isCompleted)
        // The perturbation: no cancel is issued at all.
        fx.idle()
        assertEquals(
            emptyList(),
            trace,
            "with the cancel withheld the work neither cancels nor tears down; p2 asserts " +
                "exactly those two steps",
        )
        assertEquals(1, fx.activeChildren(), "the session is still running, which is the perturbation")
    }

    @Test
    fun p3_aTypedFailureIsRenderedOnceBeforeTeardown() = lifecycle { fx ->
        fx.controller.toggle { _ -> throw IllegalStateException("boom") }
        fx.idle()
        fx.drain()
        assertEquals(listOf("started", "failed(IllegalStateException)", "stopped"), fx.rendered)
    }

    @Test
    fun p4_aFailingAssertionCrossesTheRealFinallyAndRetiresEveryOwnedJob() {
        // The sentinel escapes the owned body, so it crosses `lifecycle`'s own
        // `finally` -- the boundary under test -- and is caught OUT HERE.
        // Neutering that finally makes this row fail by name instead of
        // silently passing on an explicit settle inside the body.
        val sentinel = "the sentinel assertion this row exists to carry"
        var observed: Fixture? = null
        val escaped =
            runCatching {
                lifecycle { fx ->
                    observed = fx
                    val hold = fx.barrier()
                    fx.controller.toggle { hold.await() }
                    fx.idle()
                    assertEquals(1, fx.activeChildren(), "the session is parked, the shape of a failing R row")
                    assertEquals(1, 2, sentinel)
                }
            }
        val thrown = escaped.exceptionOrNull()
        assertTrue(thrown is AssertionError, "the sentinel must escape as an AssertionError; got " + thrown)
        assertTrue(
            thrown!!.message!!.contains(sentinel),
            "and it must be the ORIGINAL assertion, not one raised by cleanup; got " + thrown.message,
        )

        val fx = observed!!
        assertEquals(
            0,
            fx.unfinishedChildren(),
            "after the real finally, every owned job must be COMPLETED -- a cancelling job is " +
                "neither active nor retired",
        )
        assertTrue(fx.barriers.all { it.isCompleted }, "every fixture barrier must have been settled")
    }

    // ---- R: the R16 defects ------------------------------------------------

    @Test
    fun r1_secondStartIsSerialisedBehindTheFirstSessionsTeardown() = lifecycle { fx ->
        val trace = mutableListOf<String>()
        val teardown = fx.barrier()
        val entered = CompletableDeferred<Unit>()
        fx.controller.toggle { _ ->
            try {
                entered.complete(Unit)
                fx.barrier().await()
            } finally {
                // A real session closes its receiver and then its endpoint
                // here; both suspend, and the C endpoint refuses to stop while
                // a receiver is attached, so this is where the time goes.
                withContext(NonCancellable) {
                    trace.add("first:teardown-begins")
                    teardown.await()
                    trace.add("first:teardown-ends")
                }
            }
        }
        fx.idle()
        assertTrue(entered.isCompleted, "the first session must be running")

        fx.controller.toggle { } // Stop
        fx.idle()
        assertEquals(listOf("first:teardown-begins"), trace, "the first session must be tearing down")

        // Start pressed while the first session is still closing its endpoint.
        fx.controller.toggle { _ -> trace.add("second:work-ran") }
        fx.idle()
        assertEquals(
            listOf("first:teardown-begins"),
            trace,
            "a second Start ran replacement work while the first session was still closing " +
                "its endpoint: the two sessions overlap",
        )

        // ... and once the first session has REALLY finished, Start works again.
        // The drain models the device: a real looper is always running, so the
        // session's own retirement post is processed as soon as it is made.
        teardown.complete(Unit)
        fx.idle()
        fx.drain()
        assertEquals(listOf("first:teardown-begins", "first:teardown-ends"), trace)
        fx.controller.toggle { _ -> trace.add("third:work-ran") }
        fx.idle()
        assertEquals(
            listOf("first:teardown-begins", "first:teardown-ends", "third:work-ran"),
            trace,
            "after the first session has completely finished, Start must run again",
        )
    }

    @Test
    fun r2_aRetiredSessionsReporterCannotRenderOverItsReplacement() = lifecycle { fx ->
        // The first session runs to completion on its own and captures its
        // reporter, exactly as `PlaybackSession` holds the one it was given.
        // No overlap defect is assumed: a serialisation-only correction reaches
        // this state unchanged.
        var oldReporter: SessionReporter? = null
        fx.controller.toggle { reporter ->
            oldReporter = reporter
            reporter.report("from the first session")
        }
        fx.idle()
        fx.drain()
        assertEquals(
            listOf("started", "reported(from the first session)", "stopped"),
            fx.rendered,
            "the first session's own sequence",
        )

        // The old session's reporter speaks once more, after its session has
        // retired. Rejecting it at PRODUCTION is legal, so no queue-size
        // premise is asserted here: what matters is that it never renders.
        // The enqueue-before-destroy / deliver-after case is r3b's job.
        oldReporter!!.report("late line from the first session")

        // A replacement starts legitimately and produces its own line.
        fx.controller.toggle { reporter -> reporter.report("from the replacement") }
        fx.idle()
        fx.drain()

        assertEquals(
            listOf(
                "started",
                "reported(from the first session)",
                "stopped",
                "started",
                "reported(from the replacement)",
                "stopped",
            ),
            fx.rendered,
            "the whole declared UI sequence: the OLD session's late line must never appear, " +
                "because the reporter that produced it belongs to a session that has ended",
        )
    }

    @Test
    fun r3_nothingRendersAfterDestroyEvenThoughTheSessionRanAndCleanedUp() = lifecycle { fx ->
        val teardown = fx.barrier()
        val ran = CompletableDeferred<Unit>()
        val toreDown = CompletableDeferred<Unit>()
        fx.controller.toggle {
            try {
                ran.complete(Unit)
                fx.barrier().await()
            } finally {
                withContext(NonCancellable) {
                    teardown.await()
                    toreDown.complete(Unit)
                }
            }
        }
        fx.idle()
        fx.drain()
        assertTrue(ran.isCompleted, "the session must actually have run")

        fx.controller.destroy() // the Activity is going away
        teardown.complete(Unit)
        fx.idle()
        assertTrue(toreDown.isCompleted, "and its cleanup must still have run to completion")

        // Rejecting the update early is allowed; rendering it is not.
        val before = fx.rendered.size
        fx.drain()
        assertEquals(
            emptyList(),
            fx.rendered.drop(before),
            "an update from a session whose owner was destroyed was rendered; on a device that " +
                "is a view mutation after onDestroy",
        )
    }

    @Test
    fun r3b_aLineQueuedWhileTheSessionWasAliveCannotRenderAfterDestroy() = lifecycle { fx ->
        val reported = CompletableDeferred<Unit>()
        val hold = fx.barrier()
        fx.controller.toggle { reporter ->
            reporter.report("queued while alive")
            reported.complete(Unit)
            hold.await()
        }
        fx.idle()
        assertTrue(reported.isCompleted, "the line was produced by the LIVE session")
        assertTrue(fx.queued.isNotEmpty(), "and it is queued, not yet delivered")

        val before = fx.rendered.size
        fx.controller.destroy()
        fx.idle()
        fx.drain() // delivery happens after destruction
        assertEquals(
            emptyList(),
            fx.rendered.drop(before),
            "a line enqueued before destruction was rendered after it; the guard must be read " +
                "at DELIVERY, not only when the update is produced",
        )
    }

    @Test
    fun r14_aCapturedOldReporterIsNotStampedWithTheCurrentGeneration() = lifecycle { fx ->
        var oldReporter: SessionReporter? = null
        fx.controller.toggle { reporter -> oldReporter = reporter }
        fx.idle()
        fx.drain()

        // A legitimate replacement is running when the OLD reporter speaks.
        val hold = fx.barrier()
        fx.controller.toggle { hold.await() }
        fx.idle()
        fx.drain()
        val before = fx.rendered.size

        oldReporter!!.report("old reporter speaking during the replacement")
        fx.idle()
        fx.drain()
        assertEquals(
            emptyList(),
            fx.rendered.drop(before),
            "a captured OLD reporter was rendered as though it belonged to the running session; " +
                "identity must travel with the reporter, not be read from whatever is current",
        )
    }

    // ---- promoted from 1748's private probes -------------------------------

    @Test
    fun r19_startWorksAgainAfterAStopThatLandedBeforeTheWorkerRan() = lifecycle { fx ->
        var ran = false
        fx.controller.toggle { ran = true }
        // No idle(): the launch has not been dispatched yet, so its body -- and
        // the `finally` the old design retired from -- never runs.
        fx.controller.toggle { }
        fx.idle()
        fx.drain()
        assertTrue(!ran, "the first session was stopped before it ever executed")

        var replacement = 0
        fx.controller.toggle { replacement++ }
        fx.idle()
        fx.drain()
        assertEquals(
            1,
            replacement,
            "a session cancelled before its body was entered never runs its finally, so " +
                "retirement keyed on that finally leaves the controller permanently stopping",
        )
    }

    @Test
    fun r20_retirementWaitsForTheWholeJobNotJustTheBody() = lifecycle { fx ->
        val childRunning = CompletableDeferred<Unit>()
        val releaseChild = fx.barrier()
        var firstChildCompleted = false
        fx.controller.toggle {
            // A session may leave work of its own still finishing when its body
            // returns. Retirement is a JOB fact, not a body fact.
            // The WHOLE context, not just the Job: building a scope from the
            // Job alone drops the TestDispatcher, and the child would then run
            // on real Default workers where `advanceUntilIdle` proves nothing
            // and the fixture's queue would be touched off-thread.
            val self = kotlin.coroutines.coroutineContext
            CoroutineScope(self).launch {
                childRunning.complete(Unit)
                releaseChild.await()
                firstChildCompleted = true
            }
        }
        fx.idle()
        fx.drain()
        assertTrue(childRunning.isCompleted, "the child work is running")

        var replacement = 0
        fx.controller.toggle { replacement++ }
        fx.idle()
        fx.drain()
        assertEquals(
            0,
            replacement,
            "a replacement started while the first session's own child was still unfinished; " +
                "first-child-completed=" + firstChildCompleted,
        )

        releaseChild.complete(Unit)
        fx.idle()
        fx.drain()
        fx.controller.toggle { replacement++ }
        fx.idle()
        fx.drain()
        assertEquals(1, replacement, "and once the whole job completed, Start works again")
    }

    @Test
    fun r21_destroyBeforeTheFirstExecutionIsPermanentAndIdempotent() = lifecycle { fx ->
        var ran = false
        fx.controller.toggle { ran = true }
        fx.controller.destroy()
        fx.controller.destroy()
        fx.idle()
        fx.drain()
        assertTrue(!ran, "the session never executed")

        fx.controller.toggle { ran = true }
        fx.idle()
        fx.drain()
        assertTrue(!ran, "and a destroyed controller starts nothing afterwards")
        assertEquals(emptyList(), fx.rendered, "nothing is rendered for a destroyed owner")
    }

    @Test
    fun r22_theSameSequenceIsRenderedWhenTheUiDispatchIsImmediate() = runTest {
        // The queued fixture is one dispatch shape; a device's looper can also
        // run a post inline. The declared sequence must not depend on which.
        val rendered = mutableListOf<String>()
        val controller =
            SessionController(this, { action -> action() }) { update ->
                rendered.add(
                    when (update) {
                        is SessionUpdate.Started -> "started"
                        is SessionUpdate.Reported -> "reported(" + update.line + ")"
                        is SessionUpdate.Failed -> "failed"
                        is SessionUpdate.Stopped -> "stopped"
                    },
                )
            }
        controller.toggle { reporter -> reporter.report("hello") }
        testScheduler.advanceUntilIdle()
        assertEquals(listOf("started", "reported(hello)", "stopped"), rendered)

        var second = 0
        controller.toggle { second++ }
        testScheduler.advanceUntilIdle()
        assertEquals(1, second, "and an immediate dispatch still permits a later Start")
    }

    // ---- promoted from 1750's probes: reentrant initialisation -------------

    @Test
    fun r27_aStopFromInsideTheStartedRenderCancelsTheSession() = runTest {
        var bodyRuns = 0
        var reentrant = true
        val rendered = mutableListOf<String>()
        lateinit var controller: SessionController
        controller =
            SessionController(this, { action -> action() }) { update ->
                rendered.add(
                    when (update) {
                        is SessionUpdate.Started -> "started"
                        is SessionUpdate.Reported -> "reported"
                        is SessionUpdate.Failed -> "failed"
                        is SessionUpdate.Stopped -> "stopped"
                    },
                )
                // A real render can press Stop: the UI is live while this
                // runs. Only the FIRST session is stopped this way -- pressing
                // Stop on every Started would also stop the restart below, and
                // that would be the fixture's doing, not the product's.
                if (update is SessionUpdate.Started && reentrant) {
                    reentrant = false
                    controller.toggle { }
                }
            }
        controller.toggle { bodyRuns++ }
        testScheduler.advanceUntilIdle()
        assertEquals(
            0,
            bodyRuns,
            "a Stop issued from inside the Started render found no Job to cancel, so the work " +
                "started anyway; ownership must be installed before Started is exposed",
        )
        assertEquals(listOf("started", "stopped"), rendered, "the rendered inventory")

        rendered.clear()
        var later = 0
        controller.toggle { later++ }
        testScheduler.advanceUntilIdle()
        assertEquals(1, later, "and an explicit Start afterwards still works")
        assertEquals(listOf("started", "stopped"), rendered, "the restart's own inventory")
    }

    @Test
    fun r28_aDestroyFromInsideTheStartedRenderStartsNothing() = runTest {
        var bodyRuns = 0
        val rendered = mutableListOf<String>()
        lateinit var controller: SessionController
        controller =
            SessionController(this, { action -> action() }) { update ->
                rendered.add(if (update is SessionUpdate.Started) "started" else "other")
                if (update is SessionUpdate.Started) controller.destroy()
            }
        controller.toggle { bodyRuns++ }
        testScheduler.advanceUntilIdle()
        assertEquals(
            0,
            bodyRuns,
            "the owner was destroyed from inside the Started render and the work ran anyway",
        )
        assertEquals(listOf("started"), rendered, "nothing renders after destruction")

        var later = 0
        controller.toggle { later++ }
        testScheduler.advanceUntilIdle()
        assertEquals(0, later, "and destruction is permanent")
    }

    @Test
    fun r29_anAlreadyCancelledOwnerScopeStartsNothingAndStaysConsistent() = runTest {
        val owner = CoroutineScope(coroutineContext + Job())
        owner.cancel()
        var bodyRuns = 0
        val rendered = mutableListOf<String>()
        val controller =
            SessionController(owner, { action -> action() }) { update ->
                // Exhaustive, so an unexpected Failed or Reported cannot be
                // counted as the Stopped this row's inventory asserts.
                rendered.add(
                    when (update) {
                        is SessionUpdate.Started -> "started"
                        is SessionUpdate.Reported -> "reported(" + update.line + ")"
                        is SessionUpdate.Failed -> "failed(" + update.failure::class.simpleName + ")"
                        is SessionUpdate.Stopped -> "stopped"
                    },
                )
            }
        controller.toggle { bodyRuns++ }
        testScheduler.advanceUntilIdle()
        assertEquals(0, bodyRuns, "an already-cancelled owner scope runs no work")
        assertEquals(
            listOf("started", "stopped"),
            rendered,
            "and the session still retires in order, so the controller is not left stopping",
        )
        var later = 0
        controller.toggle { later++ }
        testScheduler.advanceUntilIdle()
        assertEquals(0, later, "the owner scope is dead, so a later Start runs nothing either")
    }
}
