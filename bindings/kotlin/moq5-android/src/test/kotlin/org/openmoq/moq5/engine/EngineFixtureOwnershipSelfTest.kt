package org.openmoq.moq5.engine

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import org.openmoq.moq5.EndpointConfig
import org.openmoq.moq5.Namespace
import org.openmoq.moq5.NamespacePart
import org.openmoq.moq5.SendBackpressure
import org.openmoq.moq5.SenderConfig
import org.openmoq.moq5.MoqEndpoint
import org.openmoq.moq5.MoqResultCode
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The fixture's own caller-ownership bookkeeping (chats/libmoq/1248 F2).
 *
 * Two mechanisms are pinned here rather than assumed by the tests that use
 * them:
 *
 *  * a teardown-grade caller is classified STRUCTURALLY -- normal return,
 *    thrown value, or bounded non-return -- never by matching prose in a
 *    diagnostic;
 *  * a test may CONSUME its own caller's verdict so cleanup does not judge the
 *    same fact twice, and an engine may be marked QUARANTINED so cleanup stops
 *    demanding a later successful `close()`. Neither may erase a genuinely
 *    non-returning helper, and neither changes anything for an ordinary test.
 */
internal class EngineFixtureOwnershipSelfTest : EngineFixture() {

    @Test
    fun `a caller that returns normally classifies as NormalReturn and needs no cleanup`() =
        runTest {
            val caller = callBlocking("selftest-normal") { }
            val outcome = caller.consume()
            assertTrue(
                outcome is CallerOutcome.NormalReturn,
                "a returning caller must classify as NormalReturn, was $outcome",
            )
            assertNull(caller.cleanupProblem(), "a normal return needs no cleanup report")
            // The three-way classification is total because the wrapper catches
            // Throwable and sets `finished` right after the body returns. That
            // redundancy is checked, not assumed.
            assertTrue(
                caller.normalReturnAgreesWithFinished(),
                "a NormalReturn must coincide with the wrapper's own completion flag",
            )
        }

    /**
     * The accepted/suppressed set, stated exactly: consumption owns a
     * NormalReturn and a Threw, and owns nothing else. There is no fourth
     * outcome to test -- see [BlockingCaller.outcome] for why the wrapper
     * cannot produce one.
     */
    @Test
    fun `consumption suppresses exactly NormalReturn and Threw`() = runTest {
        val normal = callBlocking("selftest-set-normal") { }
        val threw = callBlocking("selftest-set-threw") { throw IllegalStateException("x") }
        val release = gate("selftest-set-blocked")
        val blocked = callBlocking("selftest-set-blocked-caller") { release.await() }

        assertTrue(normal.consume() is CallerOutcome.NormalReturn)
        assertTrue(threw.consume() is CallerOutcome.Threw)
        assertTrue(blocked.consume(ceilingMillis = 250) is CallerOutcome.NotReturned)

        assertNull(normal.cleanupProblem(), "a consumed NormalReturn is owned by the test")
        assertNull(threw.cleanupProblem(), "a consumed Threw is owned by the test")
        assertNotNull(
            blocked.cleanupProblem(ceilingMillis = 250),
            "a consumed NotReturned is NEVER owned by the test",
        )

        release.open()
        assertTrue(blocked.consume() is CallerOutcome.NormalReturn)
    }

    @Test
    fun `a caller that throws classifies as Threw and is not a cleanup failure once consumed`() =
        runTest {
            val boom = IllegalStateException("scripted")
            val caller = callBlocking("selftest-throw") { throw boom }
            val outcome = caller.consume()
            assertTrue(
                outcome is CallerOutcome.Threw,
                "a throwing caller must classify as Threw, was $outcome",
            )
            assertTrue(
                (outcome as CallerOutcome.Threw).error === boom,
                "the structured outcome must carry the exact thrown value",
            )
            // The point: a waiter that correctly reports failure by throwing
            // must not become an @AfterTest failure for the test that expected
            // it. Without consumption it still would.
            assertNull(
                caller.cleanupProblem(),
                "a consumed throwing caller must not be reported by cleanup",
            )
        }

    @Test
    fun `an unconsumed throwing caller is still a cleanup failure`() = runTest {
        val caller = callBlocking("selftest-throw-unconsumed") { throw IllegalStateException("x") }
        val outcome = caller.outcome()
        assertTrue(outcome is CallerOutcome.Threw, "expected Threw, was $outcome")
        assertNotNull(
            caller.cleanupProblem(),
            "default behaviour is unchanged: an unconsumed throw is still reported",
        )
        // Consume it so this self-test's own teardown stays clean.
        caller.consume()
    }

    @Test
    fun `consumption never erases a bounded non-return`() = runTest {
        val release = gate("selftest-release")
        val caller = callBlocking("selftest-blocked") { release.await() }

        // A short bound: this is a deadlock ceiling, not a timing oracle.
        val blocked = caller.consume(ceilingMillis = 250)
        assertTrue(
            blocked is CallerOutcome.NotReturned,
            "a still-running caller must classify as NotReturned, was $blocked",
        )
        assertNotNull(
            caller.cleanupProblem(ceilingMillis = 250),
            "consumption must NOT erase a non-returning helper from cleanup",
        )

        // Let it finish, so this self-test leaves nothing contained behind.
        release.open()
        assertTrue(
            caller.consume() is CallerOutcome.NormalReturn,
            "the released caller must return",
        )
        assertNull(caller.cleanupProblem(), "a released caller needs no cleanup report")
    }

    /**
     * Owned public endpoints. `newBackend()` never registered the engine that
     * `MoqEndpoint.open` later created, so a row that failed before its own
     * teardown left the endpoint to a daemon. `ownEndpoint` registers it the
     * moment it exists; `collectCleanupProblems()` is the same collection
     * `retireEngines()` consumes, called here directly so the self-test can
     * judge its report instead of failing in its own @AfterTest.
     */
    @Test
    fun `an owned public endpoint whose setup failed right after open is still closed, joined and audited`() = runBlocking {
        val backend = newBackend()
        val endpoint = ownEndpoint(MoqEndpoint.open(EndpointConfig("moqt://moq.example:4433/p")) { backend }, backend)
        // The row's setup fails immediately after open: no attach, no catalog, no explicit teardown.
        val setup = runCatching { error("setup failed after open") }
        assertTrue(setup.isFailure)
        assertTrue(!endpoint.snapshots.value.state.name.isEmpty(), "the endpoint exists")
        val problems = collectCleanupProblems()
        assertEquals(emptyList(), problems, "cleanup owns the endpoint the failed setup left behind")
        assertEquals(1, backend.observed().stopCount, "stopped exactly once by cleanup")
        assertEquals(1, backend.observed().destroyCount, "destroyed exactly once by cleanup")
        assertEquals(false, backend.observed().serviceThread?.isAlive, "the exact retained worker was joined")
        assertEquals(emptyList(), collectCleanupProblems(), "the registry is consumed: nothing is closed twice")
    }

    /**
     * Manual-dispatcher ownership. Cancelling a scope only schedules the
     * cancellation continuations on the manual dispatcher; nothing has run,
     * so `isCancelled` says nothing about completion. The collector must
     * cancel, service the dispatched continuations and verify that the root
     * job completed and the dispatcher queue is empty -- bounded, and for a
     * caller whose result is only dispatched once a held native job is
     * released as well. The measured false pass (an empty report with a
     * live child and a queued resumption) is pinned by the first row.
     */
    @Test
    fun `cleanup completes a manual caller parked in a cancellation and leaves the dispatcher empty`() {
        val dispatcher = ManualDispatcher()
        val scope = manualScope(dispatcher)
        val caller = scope.launch { awaitCancellation() }
        assertTrue(dispatcher.runOne(), "setup: the owned caller reaches its suspension")
        assertTrue(caller.isActive && !caller.isCompleted, "setup: the owned caller is pending")
        assertEquals(0, dispatcher.queued(), "setup: no resumption before cleanup")
        val problems = collectCleanupProblems()
        val scopeDone = scope.coroutineContext[Job]!!.isCompleted
        assertTrue(
            problems.isEmpty() && caller.isCompleted && scopeDone && dispatcher.queued() == 0,
            "fixture.manual_cleanup: reported $problems with callerCompleted=${caller.isCompleted}, scopeCompleted=$scopeDone, queued=${dispatcher.queued()}",
        )
    }

    @Test
    fun `cleanup completes a manual caller whose accepted native job only answers once its held predecessor is released`() = runBlocking {
        val inner = newBackend()
        val hooked = HookedBackend(inner)
        val endpoint = ownEndpoint(MoqEndpoint.open(EndpointConfig("moqt://moq.example:4433/p")) { hooked }, inner)
        val sender = endpoint.attachSender(SenderConfig(Namespace(listOf(NamespacePart("live".toByteArray()))), SendBackpressure.ReturnWouldBlock))
        val ledger = ledger()
        val hold = gate("selftest-native-hold")
        hooked.beforeSenderStats = { ledger.started("held"); hold.await() }
        val dispatcher = ManualDispatcher()
        val scope = manualScope(dispatcher)
        val caller = scope.async { sender.statsSnapshot() }
        assertTrue(dispatcher.runOne(), "setup: the owned caller submits its job")
        assertTrue(ledger.await("the native job is held") { it.started.contains("held") }, "setup: the job never reached its held native call")
        assertTrue(caller.isActive && dispatcher.queued() == 0, "setup: the caller is suspended on an accepted job with nothing dispatched")
        // Cleanup opens the gate, the worker answers, the result is dispatched
        // only then, and the collector must still service and verify it.
        val problems = collectCleanupProblems()
        val scopeDone = scope.coroutineContext[Job]!!.isCompleted
        assertTrue(
            problems.isEmpty() && caller.isCompleted && scopeDone && dispatcher.queued() == 0,
            "fixture.manual_cleanup(native): reported $problems with callerCompleted=${caller.isCompleted}, scopeCompleted=$scopeDone, queued=${dispatcher.queued()}",
        )
        assertEquals(1, inner.observed().destroyCount, "the endpoint was closed once")
    }

    @Test
    fun `omitting the dispatcher service cannot pass the manual-scope liveness check`() {
        val dispatcher = ManualDispatcher()
        val scope = manualScope(dispatcher)
        val caller = scope.launch { awaitCancellation() }
        assertTrue(dispatcher.runOne(), "setup: the owned caller reaches its suspension")
        // The omission: cancel only, service nothing.
        scope.cancel()
        assertTrue(caller.isCancelled && !caller.isCompleted && dispatcher.queued() == 1, "setup: cancelled, not completed, one resumption queued")
        val live = manualLivenessProblem("selftest-omitted", dispatcher, scope)
        assertNotNull(live, "a live child with a queued resumption must be reported, never a clean result")
        assertTrue("rootCompleted=false" in live && "queued=1" in live, "the report names the live root and the queued resumption: $live")
        // Serviced, it retires; the fixture's own cleanup then finds it clean.
        dispatcher.runAll()
        assertNull(manualLivenessProblem("selftest-serviced", dispatcher, scope), "after servicing, the scope is retired")
    }

    /**
     * The retirement bound must bound the work it surrounds. Two obstructions
     * a dispatched continuation can raise: it keeps redispatching itself (the
     * deadline must be checked between tasks), and it blocks the thread that
     * runs it (no check between tasks can preempt that, so the driver is an
     * owned bounded helper and a non-returning one is reported as CONTAINED,
     * never as joined or retired). Both go through the real collector on an
     * exact retained helper thread with a small explicit ceiling; the clocks
     * bound the fixture's own failure and nothing else. Every obstruction is
     * released, and every helper joined, in the row's finally.
     */
    private fun collectOnHelper(name: String, outcome: AtomicReference<Result<List<String>>?>, returned: CountDownLatch): Thread {
        val t = Thread({
            try {
                outcome.set(runCatching { collectCleanupProblems() })
            } finally {
                returned.countDown()
            }
        }, "moq.test.selftest.$name")
        t.isDaemon = true
        t.start()
        return t
    }

    @Test
    fun `the retirement bound holds against continuously redispatched cancellation work`() {
        cleanupCeilingMillis = 500
        val dispatcher = ManualDispatcher()
        val scope = manualScope(dispatcher)
        val keepRunning = AtomicBoolean(true)
        val entered = CountDownLatch(1)
        val iterations = AtomicInteger()
        val caller = scope.launch {
            try {
                awaitCancellation()
            } finally {
                withContext(NonCancellable) {
                    entered.countDown()
                    while (keepRunning.get()) {
                        iterations.incrementAndGet()
                        yield()
                    }
                }
            }
        }
        assertTrue(dispatcher.runOne(), "setup: the caller reached its suspension")
        val outcome = AtomicReference<Result<List<String>>?>(null)
        val returned = CountDownLatch(1)
        val helper = collectOnHelper("redispatch", outcome, returned)
        val returnedWithinBound: Boolean
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS), "setup: the cancellation cleanup was entered")
            returnedWithinBound = returned.await(500 + 1_000 + 2_000, TimeUnit.MILLISECONDS)
        } finally {
            keepRunning.set(false)
            helper.join(ScriptedEndpointBackend.CEILING_MILLIS)
            assertTrue(!helper.isAlive, "row cleanup: the exact helper returned after release")
            for (d in containedRetirementDrivers()) {
                d.join(ScriptedEndpointBackend.CEILING_MILLIS)
                assertTrue(!d.isAlive, "row cleanup: the contained driver '${d.name}' returned after release")
            }
            dispatcher.runAll()
            assertTrue(caller.isCompleted && scope.coroutineContext[Job]!!.isCompleted, "row cleanup: caller and root retired after release")
            assertEquals(0, dispatcher.queued(), "row cleanup: nothing left dispatched")
        }
        assertTrue(iterations.get() > 1, "setup: the cleanup really redispatched itself")
        assertTrue(returnedWithinBound, "fixture.manual_deadline: continuously redispatched work prevented the collector's deadline check; iterations=${iterations.get()}, eventualOutcome=${outcome.get()}")
        val problems = outcome.get()!!.getOrThrow()
        assertTrue(problems.any { "not retired" in it || "bound" in it }, "a non-terminating owned caller is a named problem, never clean: $problems")
    }

    @Test
    fun `a dispatched step that blocks the driver is reported as contained, not joined, within the bound`() {
        cleanupCeilingMillis = 500
        val dispatcher = ManualDispatcher()
        val scope = manualScope(dispatcher)
        val release = CountDownLatch(1)
        val entered = CountDownLatch(1)
        val caller = scope.launch {
            try {
                awaitCancellation()
            } finally {
                withContext(NonCancellable) {
                    entered.countDown()
                    release.await() // blocks whichever thread runs this continuation
                }
            }
        }
        assertTrue(dispatcher.runOne(), "setup: the caller reached its suspension")
        val outcome = AtomicReference<Result<List<String>>?>(null)
        val returned = CountDownLatch(1)
        val helper = collectOnHelper("blocked", outcome, returned)
        val returnedWithinBound: Boolean
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS), "setup: the blocking step was entered")
            returnedWithinBound = returned.await(500 + 1_000 + 2_000, TimeUnit.MILLISECONDS)
        } finally {
            release.countDown()
            helper.join(ScriptedEndpointBackend.CEILING_MILLIS)
            assertTrue(!helper.isAlive, "row cleanup: the exact helper returned")
            for (d in containedRetirementDrivers()) {
                d.join(ScriptedEndpointBackend.CEILING_MILLIS)
                assertTrue(!d.isAlive, "row cleanup: the contained driver '${d.name}' returned after release")
            }
            dispatcher.runAll()
            assertTrue(caller.isCompleted && scope.coroutineContext[Job]!!.isCompleted, "row cleanup: caller and root retired after release")
            assertEquals(0, dispatcher.queued(), "row cleanup: nothing left dispatched")
        }
        assertTrue(returnedWithinBound, "fixture.manual_blocked: a blocking dispatched step held the collector past its bound; eventualOutcome=${outcome.get()}")
        val problems = outcome.get()!!.getOrThrow()
        assertTrue(problems.any { "contained" in it && "NOT terminated" in it }, "a blocked driver is reported as contained, never as retired: $problems")
    }

    /**
     * Waiting for more work must never miss work that arrived before the
     * wait began: the sole completion continuation is dispatched at the
     * fixture's pre-wait synchronization point and must be consumed without
     * any second dispatch. The control lets the completion arrive while the
     * driver is genuinely parked on an empty queue.
     */
    @Test
    fun `a sole completion dispatched just before the retirement wait is consumed without a second dispatch`() {
        cleanupCeilingMillis = 500
        val dispatcher = ManualDispatcher()
        val scope = manualScope(dispatcher)
        val release = CompletableDeferred<Unit>()
        val caller = scope.launch { withContext(NonCancellable) { release.await() } }
        assertTrue(dispatcher.runOne(), "setup: the caller is suspended on the deferred, immune to cancellation")
        assertEquals(1, dispatcher.dispatchedCount(), "setup: only the start was dispatched")
        val completions = AtomicInteger()
        beforeRetirementWait = { if (completions.incrementAndGet() == 1) release.complete(Unit) }
        try {
            val problems = collectCleanupProblems()
            assertTrue(
                problems.isEmpty() && caller.isCompleted && scope.coroutineContext[Job]!!.isCompleted && dispatcher.queued() == 0,
                "fixture.manual_arrival: the completion dispatched before the wait was not consumed: $problems (callerCompleted=${caller.isCompleted}, queued=${dispatcher.queued()})",
            )
            assertEquals(2, dispatcher.dispatchedCount(), "exactly the start and the sole completion were ever dispatched: no second dispatch was needed")
        } finally {
            // Unconditional: whether or not the hook ran or the completion was
            // driven, the row releases its own obstruction, drives what that
            // leaves, and joins every contained driver before anyone else
            // touches this dispatcher. Contained-but-running stays a failure.
            release.complete(Unit)
            for (d in containedRetirementDrivers()) {
                d.join(ScriptedEndpointBackend.CEILING_MILLIS)
                assertTrue(!d.isAlive, "row cleanup: the contained driver '${d.name}' returned after release")
            }
            dispatcher.runAll()
            assertTrue(caller.isCompleted && scope.coroutineContext[Job]!!.isCompleted, "row cleanup: caller and root retired")
            assertEquals(0, dispatcher.queued(), "row cleanup: nothing left dispatched")
        }
    }

    @Test
    fun `a completion that arrives while the retirement wait is parked on an empty queue is consumed`() {
        cleanupCeilingMillis = 2_000
        val dispatcher = ManualDispatcher()
        val scope = manualScope(dispatcher)
        val release = CompletableDeferred<Unit>()
        val caller = scope.launch { withContext(NonCancellable) { release.await() } }
        assertTrue(dispatcher.runOne(), "setup: the caller is suspended on the deferred")
        val arrivals = mutableListOf<Thread>()
        dispatcher.onAwaitWorkParked = {
            // Under the dispatcher's lock: the arrival happens from another
            // thread, which dispatches once the waiter has released the lock.
            val t = Thread({ release.complete(Unit) }, "moq.test.selftest.arrival")
            arrivals += t
            t.start()
        }
        try {
            val problems = collectCleanupProblems()
            for (t in arrivals) t.join(ScriptedEndpointBackend.CEILING_MILLIS)
            assertTrue(
                problems.isEmpty() && caller.isCompleted && scope.coroutineContext[Job]!!.isCompleted && dispatcher.queued() == 0,
                "fixture.manual_parked_arrival: $problems (callerCompleted=${caller.isCompleted}, queued=${dispatcher.queued()})",
            )
            assertTrue(arrivals.isNotEmpty() && arrivals.none { it.isAlive }, "the arrival thread ran and was joined")
        } finally {
            release.complete(Unit)
            for (t in arrivals) {
                t.join(ScriptedEndpointBackend.CEILING_MILLIS)
                assertTrue(!t.isAlive, "row cleanup: the arrival thread '${t.name}' returned")
            }
            for (d in containedRetirementDrivers()) {
                d.join(ScriptedEndpointBackend.CEILING_MILLIS)
                assertTrue(!d.isAlive, "row cleanup: the contained driver '${d.name}' returned after release")
            }
            dispatcher.runAll()
            assertTrue(caller.isCompleted && scope.coroutineContext[Job]!!.isCompleted, "row cleanup: caller and root retired")
            assertEquals(0, dispatcher.queued(), "row cleanup: nothing left dispatched")
        }
    }

    /**
     * Retirement execution failures are owned outcomes. A joined driver and
     * an empty queue say nothing about whether execution SUCCEEDED: a
     * dispatched task can throw on the driver, and an owned coroutine's
     * cleanup can throw when cancellation reaches it. Both must appear in the
     * collector's report, by declared error and by scope/driver identity,
     * without short-circuiting the other audits. The observer installed on
     * the driver thread is per-thread and per-row, never process-global.
     */
    @Test
    fun `retirement cannot report clean after a dispatched task throws on its driver`() {
        val dispatcher = ManualDispatcher()
        val scope = manualScope(dispatcher)
        val marker = AssertionError("selftest-driver-marker")
        val seen = AtomicReference<Throwable?>(null)
        val thread = AtomicReference<Thread?>(null)
        dispatcher.dispatch(EmptyCoroutineContext, Runnable {
            thread.set(Thread.currentThread())
            Thread.currentThread().uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { _, failure -> seen.set(failure) }
            throw marker
        })
        val problems = collectCleanupProblems()
        val driver = thread.get()
        assertTrue(driver != null && !driver.isAlive, "the exact driver ran the task and is dead")
        assertTrue(scope.coroutineContext[Job]!!.isCompleted, "root completed")
        assertEquals(0, dispatcher.queued(), "queue empty")
        assertTrue(seen.get() === marker || problems.any { "selftest-driver-marker" in it }, "the exact deliberately thrown failure was observed")
        assertTrue(
            problems.any { "selftest-driver-marker" in it && "manual scope #0" in it && driver!!.name in it },
            "fixture.manual_driver_failure: collector reported $problems despite the exact task failure $marker on ${driver?.name}",
        )
    }

    @Test
    fun `retirement cannot report clean after an owned coroutine's cleanup throws`() {
        val dispatcher = ManualDispatcher()
        val scope = manualScope(dispatcher)
        val marker = AssertionError("selftest-coroutine-marker")
        val seen = AtomicReference<Throwable?>(null)
        val thread = AtomicReference<Thread?>(null)
        val caller = scope.launch {
            try {
                awaitCancellation()
            } finally {
                thread.set(Thread.currentThread())
                Thread.currentThread().uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { _, failure -> seen.set(failure) }
                throw marker
            }
        }
        assertTrue(dispatcher.runOne(), "setup: the owned caller reached its suspension")
        assertTrue(caller.isActive && dispatcher.queued() == 0)
        val problems = collectCleanupProblems()
        val driver = thread.get()
        assertTrue(driver != null && !driver.isAlive, "the exact driver ran the cleanup and is dead")
        assertTrue(caller.isCompleted && scope.coroutineContext[Job]!!.isCompleted, "caller and root completed")
        assertEquals(0, dispatcher.queued(), "queue empty")
        assertTrue(seen.get() === marker || problems.any { "selftest-coroutine-marker" in it }, "the exact deliberately thrown failure was observed")
        assertTrue(
            problems.any { "selftest-coroutine-marker" in it && "manual scope #0" in it },
            "fixture.manual_coroutine_failure: collector reported $problems despite the exact cleanup failure $marker",
        )
    }

    @Test
    fun `a reported execution failure in one scope does not stop the retirement of another scope or the endpoint audit`() = runBlocking {
        val backend = newBackend()
        ownEndpoint(MoqEndpoint.open(EndpointConfig("moqt://moq.example:4433/p")) { backend }, backend)
        val failing = ManualDispatcher()
        val failingScope = manualScope(failing)
        failing.dispatch(EmptyCoroutineContext, Runnable { throw AssertionError("selftest-independent-marker") })
        val healthy = ManualDispatcher()
        val healthyScope = manualScope(healthy)
        val parked = healthyScope.launch { awaitCancellation() }
        assertTrue(healthy.runOne(), "setup: the healthy caller reached its suspension")
        val problems = collectCleanupProblems()
        assertTrue(problems.any { "selftest-independent-marker" in it && "manual scope #0" in it }, "the failing scope is reported: $problems")
        assertTrue(problems.none { "manual scope #1" in it }, "the healthy scope retired cleanly: $problems")
        assertTrue(parked.isCompleted && healthyScope.coroutineContext[Job]!!.isCompleted && healthy.queued() == 0, "the healthy scope really retired")
        assertTrue(failingScope.coroutineContext[Job]!!.isCompleted && failing.queued() == 0, "the failing scope's root and queue are settled too")
        assertEquals(1, backend.observed().destroyCount, "the owned endpoint was still closed and audited")
        assertTrue(problems.none { "public endpoint" in it }, "no endpoint problem: $problems")
    }

    @Test
    fun `an ordinary requested cancellation of an owned coroutine is not an execution failure`() {
        val dispatcher = ManualDispatcher()
        val scope = manualScope(dispatcher)
        val caller = scope.launch { awaitCancellation() }
        assertTrue(dispatcher.runOne(), "setup: the owned caller reached its suspension")
        val problems = collectCleanupProblems()
        assertEquals(emptyList(), problems, "requested cancellation is the normal retirement, not a failure")
        assertTrue(caller.isCancelled && caller.isCompleted && dispatcher.queued() == 0)
    }

    @Test
    fun `a deliberate backend violation on an owned endpoint reaches the cleanup audit despite an earlier behavioral failure`() = runBlocking {
        val backend = newBackend()
        ownEndpoint(MoqEndpoint.open(EndpointConfig("moqt://moq.example:4433/p")) { backend }, backend)
        // The row's intended behavioral failure fires first ...
        val behavioral = runCatching { fail("the row's own RED assertion") }
        assertTrue(behavioral.isFailure)
        // ... and a confinement violation happens afterwards, where the row's
        // own audit would never have run: a stateful call from this thread.
        backend.snapshot()
        val problems = collectCleanupProblems()
        assertTrue(
            problems.any { "backend violations" in it && "snapshot ran on thread" in it },
            "the violation must be reported by cleanup independently of the earlier failure: $problems",
        )
        assertEquals(1, backend.observed().destroyCount, "the endpoint was still closed")
    }

    /**
     * Quarantine bookkeeping.
     *
     * This is a FIXTURE-MECHANISM self-test, not a product discriminator. An
     * end-to-end version was tried first and was NOT discriminating: today's
     * defective close returns normally from a late call, so skipping or not
     * skipping it is observationally identical. The discriminating oracle is
     * the shared [cleanupPlan], which `retireEngines()` itself consumes.
     *
     * The close that reaches the refused stop is driven through the structured
     * bounded caller and consumed, so this self-test survives the intended
     * GREEN: a corrected engine that THROWS to that waiter is accepted here
     * exactly as today's normal return is. Requiring a delivered failure is the
     * primary behavioural RED's job, not this one's.
     */
    @Test
    fun `a quarantined engine is exempted from cleanup close while others are not`() = runTest {
        val (a, ba) = newEngine()
        val (bEngine, _) = newEngine()

        ba.refuseStopWith(MoqResultCode(-5))
        val closer = callBlocking("selftest-quarantine-close") { a.close() }
        assertTrue(
            ba.awaitCondition("engine A attempts stop") { it.stopCount == 1 },
            "engine A never attempted stop",
        )
        // Marked only once the stop-refusal path has actually been reached.
        markQuarantined(a)

        val outcome = closer.consume()
        assertTrue(
            outcome is CallerOutcome.NormalReturn || outcome is CallerOutcome.Threw,
            "the quarantining close must terminate, by return or by failure, was $outcome",
        )

        // Engine B is left entirely to ordinary fixture cleanup.
        assertTrue(bEngine.post { } == SubmitOutcome.ACCEPTED, "engine B must still be running")

        // The plan asserted here is the one retireEngines() actually consumes,
        // not a restatement of the rule: A is audited without a second close,
        // B keeps ordinary close-and-audit.
        assertEquals(
            listOf(CleanupAction.AUDIT_ONLY, CleanupAction.CLOSE_AND_AUDIT),
            cleanupPlan(),
            "cleanup must exempt only the quarantined engine",
        )
    }
}
