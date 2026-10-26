package org.openmoq.moq5.engine

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.openmoq.moq5.MoqEndpoint
import kotlin.test.AfterTest
import kotlin.test.assertTrue

/**
 * Shared fixture for the engine contract suite.
 *
 * Cleanup is the point, and it must survive a DEFECTIVE engine. `close()` is
 * contractually cancellation-immune, so it cannot be bounded by wrapping it in
 * `withTimeoutOrNull` -- a correct implementation is entitled to ignore that
 * cancellation, and `runBlocking` would then sit suspended forever on the test
 * worker. The call is therefore made on a separate helper thread that the
 * fixture joins with a bound; the helper is a daemon purely so a wedged engine
 * cannot keep the JVM alive, never as the liveness oracle.
 *
 * Liveness is proven by joining the EXACT service `Thread` the backend retained
 * on its first blocking call, not by scanning or spinning over JVM thread ids.
 *
 * Every problem is collected into one diagnostic: a failed close, a live
 * worker, backend violations and sticky harness failures are independent facts,
 * and the first of them must not hide the rest.
 */
internal abstract class EngineFixture {

    protected val failures = TestFailures()

    private val engines = mutableListOf<Pair<EndpointEngine, ScriptedEndpointBackend>>()

    /**
     * Engines that have deliberately reached the quarantined stop-refusal
     * terminal path. Their native endpoint is intentionally NOT reclaimed, so
     * cleanup must not demand a later successful `close()` or retry native
     * teardown -- it still joins and audits the exact service thread and
     * backend. Empty for every other test, so no default behaviour changes.
     */
    private val quarantined = mutableSetOf<EndpointEngine>()

    /**
     * Mark [engine] as deliberately quarantined. Arm it only AFTER the
     * scenario is established, so an earlier failure still gets ordinary
     * cleanup.
     */
    protected fun markQuarantined(engine: EndpointEngine) {
        quarantined += engine
    }

    /** What cleanup will do to each engine. */
    protected enum class CleanupAction { CLOSE_AND_AUDIT, AUDIT_ONLY }

    /**
     * The cleanup decision, as ONE function.
     *
     * `retireEngines()` consumes exactly this, so a self-test that asserts the
     * plan is asserting the real behaviour rather than a second copy of the
     * rule that could drift from it.
     */
    protected fun cleanupPlan(): List<CleanupAction> =
        engines.map { (engine, _) ->
            if (engine in quarantined) CleanupAction.AUDIT_ONLY else CleanupAction.CLOSE_AND_AUDIT
        }

    /**
     * Every helper caller, whether or not the test body reached
     * `assertCompleted()`. Throughout an intentional RED an assertion fails
     * first, so a caller registered only by its own completion check would
     * never be joined or reported at all.
     */
    private val callers = mutableListOf<BlockingCaller>()

    /**
     * Public endpoints a row opened through `MoqEndpoint.open`, registered
     * the moment they exist and BEFORE any suspending setup, so an attach or
     * catalog step that fails still leaves an owned endpoint for cleanup to
     * close (off the worker, bounded), join by exact thread identity, and
     * audit. The audit runs whether or not the row's body failed.
     */
    private class OwnedEndpoint(val label: String, val endpoint: MoqEndpoint, val backend: ScriptedEndpointBackend)

    private val owned = mutableListOf<OwnedEndpoint>()

    /** Every gate a row created: cleanup opens them first, so a hook or caller parked by a failed row cannot wedge the closes. */
    private val gates = mutableListOf<Gate>()

    protected fun ledger() = EngineLedger()

    protected fun gate(name: String) = Gate(name, failures).also { gates += it }

    /** Register a public endpoint for cleanup. Call it on the `open` result itself, before any further suspending setup. */
    protected fun ownEndpoint(endpoint: MoqEndpoint, backend: ScriptedEndpointBackend): MoqEndpoint {
        owned += OwnedEndpoint("public endpoint #${owned.size}", endpoint, backend)
        return endpoint
    }

    /**
     * A scope over a [ManualDispatcher], retained WITH its dispatcher and root
     * job until cleanup has cancelled it, serviced the dispatched
     * continuations and verified completion, or reported a bounded failure.
     */
    private class ManualOwnership(val label: String, val dispatcher: ManualDispatcher, val scope: CoroutineScope) {
        /**
         * Failures of owned coroutines, per scope, never process-global:
         * what the scope-local [CoroutineExceptionHandler] received (a root
         * `Job()` does not handle a child's failure, so without the handler
         * it would escape to the global coroutine exception path), and the
         * root job's final cause when it is not a REQUESTED cancellation.
         * Recorded by identity, so one failure is reported once.
         */
        private val failures = mutableListOf<Throwable>()

        fun recordFailure(t: Throwable) = synchronized(failures) { if (failures.none { it === t }) failures += t }

        fun coroutineFailures(): List<Throwable> = synchronized(failures) { failures.toList() }
    }

    private val manual = mutableListOf<ManualOwnership>()

    protected fun manualScope(dispatcher: ManualDispatcher): CoroutineScope {
        val root = Job()
        lateinit var ownership: ManualOwnership
        val handler = CoroutineExceptionHandler { _, t -> ownership.recordFailure(t) }
        val scope = CoroutineScope(dispatcher + root + handler)
        ownership = ManualOwnership("manual scope #${manual.size}", dispatcher, scope)
        root.invokeOnCompletion { cause ->
            val failure = if (cause is CancellationException) cause.cause else cause
            if (failure != null && failure !is CancellationException) ownership.recordFailure(failure)
        }
        manual += ownership
        return scope
    }

    /** The retirement bound; a self-test may lower it to exercise the same retirement function quickly. */
    protected var cleanupCeilingMillis: Long = ScriptedEndpointBackend.CEILING_MILLIS

    /** Test-only synchronization point: invoked by the retirement driver right before it waits for more work. */
    protected var beforeRetirementWait: (() -> Unit)? = null

    /**
     * Retire a cancelled manual scope within the bound. An owned daemon
     * DRIVER thread services the dispatcher ONE task at a time, checking the
     * deadline between tasks -- work that keeps redispatching itself cannot
     * postpone the check -- and, on an empty queue, waits on the
     * dispatcher's queue predicate under its own lock, so a continuation
     * dispatched before the wait began is never missed. A task that blocks
     * cannot be preempted by any check, so the fixture joins the driver with
     * the ceiling plus a margin and reports a driver still alive as
     * CONTAINED (a daemon, never terminated, never "retired"). The verdict
     * is always a FRESH liveness reading, never a pre-wait snapshot; an
     * expired bound with live work is a named problem, never clean success.
     * Execution failures -- a Throwable escaping the driver, or an owned
     * coroutine that failed -- are reported independently of liveness: a
     * joined driver and an empty queue do not mean execution succeeded.
     */
    private fun retireManual(m: ManualOwnership, ceilingMillis: Long = cleanupCeilingMillis): List<String> {
        val deadline = System.nanoTime() + ceilingMillis * 1_000_000
        val expired = java.util.concurrent.atomic.AtomicBoolean(false)
        // The driver's EXECUTION outcome, classified structurally like a
        // BlockingCaller: returned, threw (an escaping Throwable is an
        // aborted execution, whatever the queue looks like afterwards), or
        // still running. Owned by this retirement, never inferred from
        // thread termination or stderr.
        val thrown = java.util.concurrent.atomic.AtomicReference<Throwable?>(null)
        val driver =
            Thread({
                try {
                    while (true) {
                        if (System.nanoTime() >= deadline) {
                            expired.set(true)
                            break
                        }
                        if (m.dispatcher.runOne()) continue // one task per deadline check
                        if (manualLivenessProblem(m.label, m.dispatcher, m.scope) == null) break
                        beforeRetirementWait?.invoke()
                        if (!m.dispatcher.awaitWork(deadline)) {
                            expired.set(true)
                            break
                        }
                    }
                } catch (t: Throwable) {
                    thrown.set(t)
                }
            }, "moq.test.retire.${m.label.replace(' ', '-')}")
        driver.isDaemon = true
        driver.start()
        driver.join(ceilingMillis + DRIVER_JOIN_MARGIN_MILLIS)
        val problems = mutableListOf<String>()
        if (driver.isAlive) {
            containedDrivers += driver
            problems += "${m.label}: the retirement driver '${driver.name}' did not return within ${ceilingMillis + DRIVER_JOIN_MARGIN_MILLIS} ms " +
                "-- a dispatched step is blocking it; contained as a daemon, NOT terminated, not retired " +
                "(${manualLivenessProblem(m.label, m.dispatcher, m.scope) ?: "queue empty, root completed"})"
        } else {
            thrown.get()?.let { problems += "${m.label}: the retirement driver '${driver.name}' threw ${it::class.simpleName}: ${it.message} -- execution aborted" }
            manualLivenessProblem(m.label, m.dispatcher, m.scope)?.let { fresh ->
                problems += if (expired.get()) "$fresh -- retirement stopped at its $ceilingMillis ms bound" else fresh
            }
        }
        // Independent of liveness: an owned coroutine that failed (its root's
        // final cause is not a requested cancellation) is a reported failure
        // even when everything completed and the queue is empty.
        for (t in m.coroutineFailures()) problems += "${m.label}: an owned coroutine failed with ${t::class.simpleName}: ${t.message}"
        return problems
    }

    /** Retirement drivers that never returned: contained as daemons, never terminated; a self-test joins them after releasing its obstruction. */
    private val containedDrivers = mutableListOf<Thread>()

    protected fun containedRetirementDrivers(): List<Thread> = containedDrivers.toList()

    private companion object {
        /** Grace for a driver whose last task is finishing when the bound expires; a driver alive past it is contained, not joined. */
        const val DRIVER_JOIN_MARGIN_MILLIS: Long = 1_000
    }

    /**
     * The liveness verdict cleanup relies on: null only when the scope's root
     * job has COMPLETED (not merely been cancelled) and nothing is left queued
     * on its dispatcher. `isCancelled` is not `isCompleted`: cancellation only
     * schedules continuations, and an unrun one is a live caller.
     */
    protected fun manualLivenessProblem(label: String, dispatcher: ManualDispatcher, scope: CoroutineScope): String? {
        val root = scope.coroutineContext[Job]!!
        val queued = dispatcher.queued()
        if (root.isCompleted && queued == 0) return null
        val live = root.children.count { it.isActive || !it.isCompleted }
        return "$label: not retired (rootCompleted=${root.isCompleted}, rootCancelled=${root.isCancelled}, liveChildren=$live, queued=$queued)"
    }

    protected val slice = ScriptedEndpointBackend.WAIT_SLICE_MICROS

    protected fun newEngine(): Pair<EndpointEngine, ScriptedEndpointBackend> {
        val b = ScriptedEndpointBackend(failures)
        return newEngine(b) { b }
    }

    /**
     * An engine whose root is produced by [factory], invoked on the worker.
     *
     * [backend] is the object cleanup audits. It is passed separately because a
     * factory may deliberately never run, and cleanup must still be able to
     * join the worker and read the backend's violations.
     */
    protected fun newEngine(
        backend: ScriptedEndpointBackend,
        factory: () -> EndpointBackend,
    ): Pair<EndpointEngine, ScriptedEndpointBackend> = newEngine(backend, publishSnapshots = false, factory = factory)

    /** An owned engine with snapshot publication chosen explicitly, for rows that judge the record stream's interaction with the worker. */
    protected fun newEngine(
        backend: ScriptedEndpointBackend,
        publishSnapshots: Boolean,
        factory: () -> EndpointBackend,
    ): Pair<EndpointEngine, ScriptedEndpointBackend> {
        val e = EndpointEngine(factory, publishSnapshots = publishSnapshots)
        val pair = e to backend
        engines += pair
        return pair
    }

    /** A scripted backend registered with this fixture's failure sink. */
    protected fun newBackend() = ScriptedEndpointBackend(failures)

    @AfterTest
    fun retireEngines() {
        val problems = collectCleanupProblems()
        assertTrue(problems.isEmpty(), "engine retirement: $problems")
    }

    /**
     * Every cleanup fact, collected independently and in one pass: held gates
     * opened, manual scopes cancelled, owned public endpoints closed off the
     * worker within a bound, exact workers joined, native teardown counted,
     * backend violations read, manual scopes serviced and verified complete
     * with empty dispatchers, then the engine-level audit, the callers and
     * the sticky registry. Nothing here stops at the first problem, and
     * nothing depends on the row's body having succeeded. The registries are
     * consumed, so a second call reports only what happened since.
     */
    protected fun collectCleanupProblems(): List<String> {
        val problems = mutableListOf<String>()

        for (g in gates) g.open()
        gates.clear()
        // Cancel first; service and verify only after the owned endpoints
        // have been closed, because an accepted commit-grade job's result
        // may be dispatched only by that native teardown.
        for (m in manual) m.scope.cancel()

        for (o in owned) {
            closeOffWorker(o.label) { o.endpoint.closeAndJoin() }?.let { problems += it }
            o.backend.observed().serviceThread?.let { worker ->
                worker.join(ScriptedEndpointBackend.CEILING_MILLIS)
                if (worker.isAlive) {
                    problems += "${o.label} service thread '${worker.name}' (id ${worker.threadId()}) is still alive after closeAndJoin()"
                }
            }
            val snap = o.backend.observed()
            if (snap.stopCount != 1 || snap.destroyCount != 1) {
                problems += "${o.label}: native stop ran ${snap.stopCount} time(s) and destroy ${snap.destroyCount} time(s), expected once each"
            }
            if (snap.violations.isNotEmpty()) problems += "${o.label} backend violations: ${snap.violations}"
        }
        owned.clear()

        for (m in manual) problems += retireManual(m)
        manual.clear()

        val plan = cleanupPlan()
        for ((index, entry) in engines.withIndex()) {
            val (engine, backend) = entry
            if (plan[index] == CleanupAction.CLOSE_AND_AUDIT) {
                val outcome = closeOffWorker("engine #$index") { engine.close() }
                if (outcome != null) problems += outcome
            }

            val worker = backend.observed().serviceThread
            if (worker != null) {
                worker.join(ScriptedEndpointBackend.CEILING_MILLIS)
                if (worker.isAlive) {
                    problems +=
                        "engine #$index service thread '${worker.name}' (id ${worker.threadId()}) " +
                            "is still alive after close()"
                }
            }

            val snap = backend.observed()
            if (snap.violations.isNotEmpty()) {
                problems += "engine #$index backend violations: ${snap.violations}"
            }
            // Deliberately NOT asserted here: an ignored wake is not
            // universally erroneous. The transport can terminalise the endpoint
            // asynchronously just before an otherwise valid accepted-submission
            // wake, and endpoint.h defines that wake as a harmless no-op -- so a
            // universal mixed-cause assertion would misclassify a legal race and
            // would block later terminal-state work. A wake after the ENGINE'S
            // OWN stop is asserted where it is controlled, in the close arm; a
            // wake after destroy is already a backend violation above.
        }
        engines.clear()

        for (caller in callers) caller.cleanupProblem()?.let { problems += it }
        callers.clear()
        quarantined.clear()

        val sticky = failures.drain()
        if (sticky.isNotEmpty()) problems += "sticky harness failures: $sticky"

        return problems
    }

    /**
     * Invoke a teardown-grade suspending call from a bounded helper thread.
     *
     * `postAndWait` and `close()` are cancellation-immune by contract, so a
     * coroutine parked in one cannot be cancelled -- launching them into
     * `backgroundScope` would wedge `runTest` for its whole timeout whenever
     * the engine fails to resume them, replacing a named diagnostic with an
     * anonymous stall. The helper is a daemon so a wedged engine cannot keep
     * the JVM alive; completion is still asserted explicitly.
     */
    protected fun callBlocking(name: String, block: suspend () -> Unit): BlockingCaller {
        val caller = BlockingCaller(name)
        callers += caller // fixture-owned from creation, not from assertCompleted()
        caller.start(block)
        return caller
    }

    /**
     * How a teardown-grade call ended, as a VALUE.
     *
     * Control flow is never inferred from diagnostic prose: "threw" and "exited
     * without completing" are different facts, and a substring match over a
     * human-readable message cannot tell them apart.
     */
    protected sealed interface CallerOutcome {
        /** Returned normally. */
        data object NormalReturn : CallerOutcome

        /** Completed by throwing. */
        data class Threw(val error: Throwable) : CallerOutcome

        /** Still running when the bound expired. */
        data object NotReturned : CallerOutcome
    }

    /** A teardown-grade call in flight on its own bounded, joinable thread. */
    protected class BlockingCaller(private val name: String) {
        @Volatile private var finished = false

        @Volatile private var thrown: Throwable? = null

        private lateinit var thread: Thread

        internal fun start(block: suspend () -> Unit) {
            thread =
                Thread({
                    try {
                        runBlocking { block() }
                        finished = true
                    } catch (t: Throwable) {
                        thrown = t
                    }
                }, "moq.test.caller.$name")
            thread.isDaemon = true
            thread.start()
        }

        @Volatile private var consumed = false

        /**
         * Bounded-join and classify STRUCTURALLY.
         *
         * The ceiling is a deadlock bound, never a timing oracle: no verdict
         * depends on how long anything took, only on a hung helper being
         * reported instead of hanging the suite.
         */
        fun outcome(ceilingMillis: Long = ScriptedEndpointBackend.CEILING_MILLIS): CallerOutcome {
            thread.join(ceilingMillis)
            val t = thrown
            return when {
                t != null -> CallerOutcome.Threw(t)
                thread.isAlive -> CallerOutcome.NotReturned
                else -> CallerOutcome.NormalReturn
            }
        }

        /**
         * This caller's OWN thread, for an ordering observation (is it queued
         * on a particular lock?). It is never a liveness oracle: the
         * structural classification above stays the only verdict on whether
         * the call finished.
         */
        fun ownThread(): Thread = thread

        /**
         * True once the body RETURNED normally -- a fact the caller itself
         * recorded, never a join and never a deadline. "Not completed" is not
         * evidence of anything by itself.
         */
        fun completedNormally(): Boolean = finished

        /**
         * The three-way classification above is TOTAL for every reachable exit.
         *
         * [start]'s body catches `Throwable` -- there is no narrower filter and
         * no rethrow -- and sets `finished` on the statement immediately after
         * the call returns. So a thread that is no longer alive either recorded
         * a throwable or completed; there is no fourth state to classify, which
         * is why no "exited incomplete" case exists. `finished` is therefore
         * REDUNDANT with "not alive and did not throw", and
         * [normalReturnAgreesWithFinished] exists so that redundancy is checked
         * rather than assumed.
         */
        fun normalReturnAgreesWithFinished(): Boolean = finished

        /**
         * Classify this caller and take ownership of the verdict.
         *
         * A test that has asserted its own expectation about a caller -- for
         * example that a quarantined close waiter must NOT report success --
         * consumes it, so fixture cleanup does not judge the same fact again by
         * its own default rule. A helper that never returned is NEVER erased by
         * consumption: that is a containment fact the fixture must still report.
         */
        fun consume(ceilingMillis: Long = ScriptedEndpointBackend.CEILING_MILLIS): CallerOutcome {
            val o = outcome(ceilingMillis)
            consumed = true
            return o
        }

        /**
         * What fixture cleanup should report for this caller, or null.
         *
         * The ceiling is a deadlock bound; cleanup uses the default, and a
         * self-test may pass a smaller one so a deliberately blocked helper is
         * classified while it is still blocked.
         */
        fun cleanupProblem(
            ceilingMillis: Long = ScriptedEndpointBackend.CEILING_MILLIS,
        ): String? {
            if (!consumed) return accountFor()
            // Only an exact NormalReturn or Threw is owned by the consuming
            // test. A bounded non-return is a containment fact that no amount
            // of consumption may erase.
            return when (outcome(ceilingMillis)) {
                is CallerOutcome.NormalReturn, is CallerOutcome.Threw -> null
                is CallerOutcome.NotReturned ->
                    "$name: the teardown-grade caller never returned; the helper thread is " +
                        "still alive (contained as a daemon until the test worker exits, " +
                        "NOT terminated)"
            }
        }

        /** Assert the call returned; a stranded continuation must not pass. */
        fun assertCompleted() {
            val problem = accountFor()
            assertTrue(problem == null, problem ?: "")
        }

        /**
         * Bounded-join and classify, for fixture cleanup.
         *
         * Returns null when the caller returned cleanly, otherwise a named
         * problem. A helper still alive at the end is reported as CONTAINED,
         * not terminated: it is a daemon, so it cannot hold the disposable
         * Gradle test worker open, but nothing here made it stop.
         */
        fun accountFor(): String? {
            thread.join(ScriptedEndpointBackend.CEILING_MILLIS)
            return when {
                thrown != null ->
                    "$name: the teardown-grade caller threw " +
                        "${thrown!!::class.simpleName}: ${thrown!!.message}"
                thread.isAlive ->
                    "$name: the teardown-grade caller never returned; the helper thread is " +
                        "still alive (contained as a daemon until the test worker exits, " +
                        "NOT terminated)"
                !finished -> "$name: the teardown-grade caller exited without completing"
                else -> null
            }
        }
    }

    /**
     * Run a cancellation-immune close (an engine's `close()`, a public
     * endpoint's `closeAndJoin()`) on a bounded helper thread.
     *
     * Returns null on success, or a named problem. A worker that never returns
     * is reported whether or not a service thread was ever observed -- "no
     * thread was created" and "a thread was created and wedged before its first
     * backend call" are different failures and neither may be skipped.
     */
    private fun closeOffWorker(what: String, close: suspend () -> Unit): String? {
        var thrown: Throwable? = null
        var finished = false
        val helper =
            Thread({
                try {
                    runBlocking { close() }
                    finished = true
                } catch (t: Throwable) {
                    thrown = t
                }
            }, "moq.test.close.$what")
        helper.isDaemon = true // process-survival only; termination is asserted below
        helper.start()
        helper.join(ScriptedEndpointBackend.CEILING_MILLIS)

        return when {
            thrown != null -> "$what: close() threw ${thrown!!::class.simpleName}: ${thrown!!.message}"
            helper.isAlive -> "$what: close() did not return within the ceiling"
            !finished -> "$what: close() helper exited without completing close()"
            else -> null
        }
    }
}
