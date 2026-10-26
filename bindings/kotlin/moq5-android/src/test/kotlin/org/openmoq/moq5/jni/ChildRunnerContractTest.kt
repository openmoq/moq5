package org.openmoq.moq5.jni

import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * Children that exist only to drive the shared runner's own contract.
 *
 * Nothing here loads the bridge or touches the registry: these arms are about
 * how a child JVM is waited for, judged and retired, so the subject must be the
 * runner and nothing else.
 */
internal object ChildRunnerProbeMain {
    /** The complete, ordered output of the child that then stops making progress. */
    val HANG_OUTPUT: List<String> = listOf("BEGIN hang", "MARKER started")

    /** The status a child uses to contradict its own printed result line. */
    const val NONZERO: Int = 3

    /**
     * The hanging child's own self-limit.
     *
     * It is an order of magnitude beyond any bound a row gives the runner, so
     * the child is unfinished for every purpose these rows measure. It is
     * SECONDARY containment only: every arm below terminates and reaps the exact
     * process it owns, and asserts that it did, rather than waiting this out.
     */
    const val SELF_LIMIT_MILLIS: Long = 60_000

    @JvmStatic
    fun main(args: Array<String>) {
        when (args.firstOrNull()) {
            "ok" -> println("RESULT ok")
            "okthennonzero" -> {
                println("RESULT ok")
                System.out.flush()
                exitProcess(NONZERO)
            }
            "hang" -> {
                HANG_OUTPUT.forEach { println(it) }
                System.out.flush()
                // No further output, no EOF and no exit: the runner's own bound
                // is what ends this, and the output above is then final.
                val until = System.nanoTime() + SELF_LIMIT_MILLIS * 1_000_000
                while (System.nanoTime() < until) {
                    try {
                        Thread.sleep(25)
                    } catch (_: InterruptedException) {
                        // Interruption is not this child's exit condition.
                    }
                }
            }
            else -> println("RESULT unknown-probe")
        }
    }
}

/**
 * A real child process whose wait can be interrupted, or whose termination can
 * be withheld, at an exactly chosen point.
 *
 * The process is real and so is its death; only the two calls the runner makes
 * to manage it are instrumented, so an arm can put an interruption inside the
 * retirement path -- or take retirement away entirely -- without guessing how
 * long a signal takes to arrive. Everything else delegates.
 */
private class InstrumentedProcess(
    private val real: Process,
    /** 1-based indexes of `waitFor(timeout)` calls that must raise an interruption instead of waiting. */
    private val interruptOnCalls: Set<Int> = emptySet(),
    /** When true, the runner's forced termination does nothing, as a runner that fails to terminate would. */
    private val withholdTermination: Boolean = false,
) : Process() {
    private var waits = 0

    /** How many bounded waits the runner performed, so an arm can state which one it instrumented. */
    var observedWaits: Int = 0
        private set

    override fun getOutputStream() = real.outputStream
    override fun getInputStream() = real.inputStream
    override fun getErrorStream() = real.errorStream
    override fun waitFor(): Int = real.waitFor()
    override fun exitValue(): Int = real.exitValue()
    override fun destroy() {
        if (!withholdTermination) real.destroy()
    }
    override fun destroyForcibly(): Process {
        if (!withholdTermination) real.destroyForcibly()
        return this
    }
    override fun isAlive(): Boolean = real.isAlive

    override fun waitFor(timeout: Long, unit: TimeUnit): Boolean {
        waits++
        observedWaits = waits
        if (waits in interruptOnCalls) throw InterruptedException("instrumented interruption on wait $waits")
        return real.waitFor(timeout, unit)
    }
}

/**
 * The shared child runner's contract.
 *
 * Every JNI row in this package reads its verdict out of a child process, so
 * the runner decides whether those verdicts mean anything. What is pinned here:
 * a child that finishes normally is reported verbatim; a child whose status
 * contradicts its own output cannot pass on that output; a child that never
 * finishes is bounded, terminated, reaped and named with its output intact; a
 * runner that fails to terminate its child says so rather than calling the
 * sampled output complete; and an interruption -- during the wait or inside the
 * retirement itself -- never leaves a child of ours running, and is handed back
 * to the caller only once retirement is over.
 *
 * Every arm below OWNS what it starts. The fixture holds the real Process and
 * the helper thread, and its `finally` terminates, reaps and joins both, then
 * asserts that it did. That holds even when the runner under test is the one at
 * fault, which is what lets these rows drive a runner that never returns.
 */
class ChildRunnerContractTest {

    private companion object {
        val PROBE: String = ChildRunnerProbeMain::class.java.name

        /** Comfortably longer than a probe child needs, far shorter than the production ceiling. */
        const val SHORT_CEILING: Long = 5_000

        /** The outer bound on the helper itself, so a runner that never returns fails a row rather than the suite. */
        const val HELPER_CEILING_MILLIS: Long = 25_000

        /** The bound the FIXTURE gives its own cleanup, independent of anything the runner does. */
        const val OWNED_CLEANUP_MILLIS: Long = 10_000

        /** The bound on the child reaching the state an arm needs before it starts measuring. */
        const val PRECONDITION_MILLIS: Long = 30_000
    }

    /**
     * Wait until the child has written its whole declared output.
     *
     * An arm that asserts the complete inventory, or that instruments the very
     * first wait, must not race the child's first two lines. This is a bounded
     * wait for an OBSERVED precondition with a named failure of its own; it is
     * not an oracle for anything the arm goes on to measure.
     */
    /** A bounded wait for an observed precondition, named on expiry. Never an oracle for what the arm measures. */
    private fun awaitCondition(what: String, predicate: () -> Boolean) {
        val deadline = System.nanoTime() + PRECONDITION_MILLIS * 1_000_000
        while (System.nanoTime() < deadline) {
            if (predicate()) return
            Thread.sleep(10)
        }
        assertTrue(false, "setup: $what did not hold within $PRECONDITION_MILLIS ms")
    }

    private fun awaitDeclaredOutput(owned: EndpointAbi.OwnedChild) {
        val deadline = System.nanoTime() + PRECONDITION_MILLIS * 1_000_000
        while (System.nanoTime() < deadline) {
            if (runCatching { owned.log.readLines() }.getOrDefault(emptyList()) == ChildRunnerProbeMain.HANG_OUTPUT) return
            Thread.sleep(10)
        }
        assertTrue(
            false,
            "setup: the child never wrote its declared output within $PRECONDITION_MILLIS ms; saw " +
                runCatching { owned.log.readLines() }.getOrDefault(emptyList()),
        )
    }

    /** Everything one arm owns: the exact child it started, and every helper it launched. */
    private class Owned(val child: EndpointAbi.OwnedChild) {
        val helpers = mutableListOf<Thread>()
    }

    /** What an arm's ownership boundary saw, so a row can assert on it instead of the arm merely surviving. */
    private class Ownership(
        val primary: Throwable?,
        val cleanupProblems: List<String>,
        /** Whether the child's output file is still on disk after the boundary finished. */
        val logRetained: Boolean,
        val childAlive: Boolean,
        val helpersAlive: Int,
        val elapsedMillis: Long,
    )

    /**
     * Own a child from the moment it exists.
     *
     * The process is created here and retired here, and everything in between
     * -- setup, helper startup, the runner call and every assertion -- happens
     * inside that boundary. So a setup that fails, or is interrupted, before a
     * helper is even started still ends with the exact child terminated and
     * reaped and every helper joined, rather than left to a self-limit.
     *
     * The primary failure is kept; cleanup runs regardless and its problems are
     * reported alongside it, never in place of it.
     */
    private fun own(name: String, scenario: String, body: (Owned) -> Unit): Ownership =
        own(name, EndpointAbi.startChild(scenario, null, mainClass = PROBE), body)

    private fun own(name: String, child: EndpointAbi.OwnedChild, body: (Owned) -> Unit): Ownership {
        val started = System.nanoTime()
        val owned = Owned(child)
        var primary: Throwable? = null
        try {
            body(owned)
        } catch (t: Throwable) {
            primary = t
        }
        val problems = mutableListOf<String>()
        val p = owned.child.process
        try {
            if (p.isAlive) {
                p.destroyForcibly()
                if (!p.waitFor(OWNED_CLEANUP_MILLIS, TimeUnit.MILLISECONDS)) {
                    problems += "the child did not die within $OWNED_CLEANUP_MILLIS ms"
                }
            }
        } catch (t: Throwable) {
            problems += "terminating the child failed: $t"
        }
        // Terminating the child is also what releases a helper still blocked
        // inside the runner, so the joins come after it.
        for (h in owned.helpers) {
            try {
                h.join(OWNED_CLEANUP_MILLIS)
            } catch (t: Throwable) {
                problems += "joining ${h.name} failed: $t"
            }
            if (h.isAlive) problems += "${h.name} did not finish"
        }
        val survived = p.isAlive
        if (survived) {
            // The only diagnostic a surviving child leaves is its output file,
            // and it may still be writing to it. Keep it.
            problems += "the exact child this row owned is still running; its output is left at ${owned.child.log.path}"
        } else if (owned.child.log.exists() && !owned.child.log.delete()) {
            problems += "the child's output file ${owned.child.log.path} could not be removed"
        }
        return Ownership(
            primary = primary,
            cleanupProblems = problems,
            logRetained = survived || owned.child.log.exists(),
            childAlive = survived,
            helpersAlive = owned.helpers.count { it.isAlive },
            elapsedMillis = (System.nanoTime() - started) / 1_000_000,
        )
    }

    /**
     * An arm that must succeed, and the boundary's reporting rule.
     *
     * The failure a row is ABOUT is the one it reports. A cleanup failure that
     * happens alongside it is attached as suppressed, never substituted for it;
     * a cleanup failure on its own is the row's failure; and neither means
     * success is reported as anything but success.
     */
    private fun ownPassing(name: String, scenario: String, body: (Owned) -> Unit) =
        reportOwnership(name, own(name, scenario, body))

    private fun reportOwnership(name: String, o: Ownership) {
        val cleanup =
            if (o.cleanupProblems.isEmpty()) null
            else AssertionError("$name: the arm did not retire what it owned: " + o.cleanupProblems.joinToString("; "))
        val primary = o.primary
        if (primary != null) {
            cleanup?.let { primary.addSuppressed(it) }
            throw primary
        }
        cleanup?.let { throw it }
    }

    /**
     * Own a REAL child around an arm that drives [own] with an instrumented
     * one, so a deliberately broken inner cleanup never leaves a process behind.
     *
     * The arm sees an instrumented process whose termination is withheld; this
     * fixture keeps the real handle and reaps it afterwards, so the leak is
     * simulated for the boundary under test and bounded for everything else.
     */
    private fun aroundRealChild(name: String, body: (EndpointAbi.OwnedChild) -> Unit) {
        val real = EndpointAbi.startChild("hang", null, mainClass = PROBE)
        try {
            body(real)
        } finally {
            if (real.process.isAlive) {
                real.process.destroyForcibly()
                real.process.waitFor(OWNED_CLEANUP_MILLIS, TimeUnit.MILLISECONDS)
            }
            real.log.delete()
            assertTrue(!real.process.isAlive, "$name: the real child was not reaped by its outer owner")
        }
    }

    /**
     * Start [body] on an owned helper thread and wait for it, bounded.
     *
     * The helper is registered with [owned] before it is started, so the
     * ownership boundary joins it even if this call throws.
     */
    private fun <T> onHelper(owned: Owned, name: String, body: () -> T): T {
        var produced: T? = null
        var thrown: Throwable? = null
        var restored = false
        val helper =
            Thread({
                try {
                    produced = body()
                } catch (t: Throwable) {
                    thrown = t
                } finally {
                    restored = Thread.currentThread().isInterrupted
                }
            }, "runner-contract-$name")
        helper.isDaemon = false
        owned.helpers += helper
        helper.start()
        helper.join(HELPER_CEILING_MILLIS)
        assertTrue(!helper.isAlive, "$name: the runner did not return within $HELPER_CEILING_MILLIS ms")
        helperInterruptRestored = restored
        thrown?.let { throw it }
        @Suppress("UNCHECKED_CAST")
        return produced as T
    }

    /** Whether the last helper still carried an interrupt when it left the runner. */
    private var helperInterruptRestored: Boolean = false

    // -- the ordinary path, through the whole runner -----------------------------

    @Test
    fun `a child that finishes normally is reported verbatim`() {
        val out = EndpointAbi.child("ok", null, mainClass = PROBE, ceilingMillis = SHORT_CEILING)
        assertEquals(listOf("RESULT ok"), out, "the runner must return exactly what the child wrote")
    }

    @Test
    fun `a child whose status contradicts its own result line cannot pass on that line`() {
        val e = assertFailsWith<AssertionError> {
            EndpointAbi.child("okthennonzero", null, mainClass = PROBE, ceilingMillis = SHORT_CEILING)
        }
        val m = e.message.orEmpty()
        assertTrue(
            m.contains("it exited ${ChildRunnerProbeMain.NONZERO}"),
            "the failure must name the status that discredits the output: $m",
        )
        assertTrue(
            m.contains("RESULT ok"),
            "the child's own output must still be reported, not discarded, when its status is rejected: $m",
        )
    }

    // -- the bound, and the retirement it triggers -------------------------------

    @Test
    fun `a child that never finishes is bounded, retired, and named with its whole output`() =
        ownPassing("hang", "hang") { owned ->
            awaitDeclaredOutput(owned.child)
            val started = System.nanoTime()
            val outcome = onHelper(owned, "hang") { EndpointAbi.awaitOwnedChild(owned.child, SHORT_CEILING) }
            val elapsedMillis = (System.nanoTime() - started) / 1_000_000

            assertTrue(!outcome.finishedWithinBound, "the child must not have finished within $SHORT_CEILING ms")
            assertTrue(outcome.reaped, "the runner must terminate and reap the child whose bound expired")
            assertTrue(!owned.child.process.isAlive, "the exact process the runner owned is still alive")
            assertTrue(outcome.outputComplete, "a reaped child's output is complete")
            assertEquals(
                ChildRunnerProbeMain.HANG_OUTPUT, outcome.output,
                "the whole ordered output written before the child stopped must survive: the wait cannot " +
                    "depend on the child closing its output",
            )
            assertEquals(emptyList(), outcome.cleanupProblems, "retiring the child reported a problem")
            assertTrue(!outcome.interrupted, "nothing interrupted this arm")
            assertTrue(elapsedMillis < HELPER_CEILING_MILLIS, "the runner waited $elapsedMillis ms for a bound of $SHORT_CEILING ms")

            val e = assertFailsWith<AssertionError> { EndpointAbi.requireOwnedChild(outcome, SHORT_CEILING) }
            assertTrue(
                e.message.orEmpty().contains("did not finish within $SHORT_CEILING ms"),
                "the failure must name the bound it exceeded: ${e.message}",
            )
        }

    // -- a runner that does not manage to terminate its child --------------------

    @Test
    fun `a child that survives termination is named, and its sampled output is not called complete`() =
        ownPassing("survivor", "hang") { owned ->
            awaitDeclaredOutput(owned.child)
            // Termination is withheld, exactly as a runner that failed to
            // terminate its child would leave it. The ARM still owns the real
            // process and retires it at its boundary.
            val instrumented = EndpointAbi.OwnedChild(owned.child.name, InstrumentedProcess(owned.child.process, withholdTermination = true), owned.child.log)
            val outcome = onHelper(owned, "survivor") { EndpointAbi.awaitOwnedChild(instrumented, SHORT_CEILING) }

            assertTrue(!outcome.reaped, "the child could not be retired, so the runner must say so")
            assertTrue(!outcome.outputComplete, "a live child's sampled output must never be called complete")
            assertEquals(null, outcome.exitStatus, "a child that was never reaped has no exit status")
            assertTrue(
                outcome.cleanupProblems.any { it.contains("not retired") },
                "the failure to retire the child must be reported, not swallowed: ${outcome.cleanupProblems}",
            )
            val e = assertFailsWith<AssertionError> { EndpointAbi.requireOwnedChild(outcome, SHORT_CEILING) }
            val m = e.message.orEmpty()
            assertTrue(m.contains("it survived termination"), "the failure must name the surviving child: $m")
            assertTrue(m.contains("may be incomplete"), "the reported output must be marked incomplete: $m")
        }

    // -- interruption, at each point the runner can meet it ----------------------

    @Test
    fun `an interruption of the wait still retires the child, and is handed back only afterwards`() =
        ownPassing("interrupt-wait", "hang") { owned ->
            awaitDeclaredOutput(owned.child)
            // Call 1 is the bounded wait for a normal finish.
            val instrumented = EndpointAbi.OwnedChild(owned.child.name, InstrumentedProcess(owned.child.process, interruptOnCalls = setOf(1)), owned.child.log)
            val outcome = onHelper(owned, "interrupt-wait") { EndpointAbi.awaitOwnedChild(instrumented, SHORT_CEILING) }

            assertTrue(outcome.interrupted, "the interruption must be reported")
            assertTrue(outcome.reaped, "an interrupted wait must still terminate and reap the child")
            assertTrue(!owned.child.process.isAlive, "the exact process the runner owned is still alive")
            assertTrue(outcome.outputComplete, "a reaped child's output is complete")
            assertEquals(ChildRunnerProbeMain.HANG_OUTPUT, outcome.output, "the child's whole output must still be read")
            assertEquals(emptyList(), outcome.cleanupProblems, "retiring the child reported a problem")
            assertTrue(helperInterruptRestored, "the interrupt must be restored to the caller, after cleanup")
            val e = assertFailsWith<AssertionError> { EndpointAbi.requireOwnedChild(outcome, SHORT_CEILING) }
            assertTrue(
                e.message.orEmpty().contains("waiting for it was interrupted"),
                "the interruption must be named: ${e.message}",
            )
        }

    @Test
    fun `an interruption inside the retirement is absorbed and the reap is completed`() =
        ownPassing("interrupt-reap", "hang") { owned ->
            awaitDeclaredOutput(owned.child)
            // Call 1 is the bounded wait; call 2 is the FIRST wait of the
            // retirement, after forced termination has already been requested.
            val process = InstrumentedProcess(owned.child.process, interruptOnCalls = setOf(2))
            val instrumented = EndpointAbi.OwnedChild(owned.child.name, process, owned.child.log)
            val outcome = onHelper(owned, "interrupt-reap") { EndpointAbi.awaitOwnedChild(instrumented, SHORT_CEILING) }

            assertTrue(process.observedWaits >= 3, "the runner must retry the reap after the interruption; waits=${process.observedWaits}")
            assertTrue(outcome.interrupted, "the interruption must be reported")
            assertTrue(outcome.reaped, "an interruption inside the retirement must not abandon the child")
            assertTrue(!owned.child.process.isAlive, "the exact process the runner owned is still alive")
            assertTrue(outcome.outputComplete, "a reaped child's output is complete")
            assertEquals(ChildRunnerProbeMain.HANG_OUTPUT, outcome.output, "the child's whole output must still be read")
            assertEquals(emptyList(), outcome.cleanupProblems, "retiring the child reported a problem")
            assertTrue(helperInterruptRestored, "the interrupt must be restored to the caller, after cleanup")
        }

    // -- how the boundary REPORTS: primary and cleanup, in all four combinations ---

    /** The exact cleanup diagnostic a withheld inner retirement must produce. */
    private fun assertNamesTheSurvivor(name: String, e: Throwable, child: EndpointAbi.OwnedChild) {
        val suppressed = e.suppressed.firstOrNull()
        val text = (suppressed ?: e).message.orEmpty()
        assertTrue(
            text.contains("the exact child this row owned is still running"),
            "$name: the cleanup failure must name the surviving child: $text",
        )
        assertTrue(
            text.contains(child.log.path),
            "$name: the cleanup failure must name the diagnostic it left behind: $text",
        )
    }

    @Test
    fun `an arm that succeeds and retires cleanly reports success and removes its diagnostic`() {
        var log: java.io.File? = null
        ownPassing("clean", "ok") { owned ->
            log = owned.child.log
            awaitCondition("the child finished") { !owned.child.process.isAlive }
        }
        assertTrue(log != null && !log!!.exists(), "a cleanly retired child's output file must be removed: ${log?.path}")
    }

    @Test
    fun `a failing arm that retires cleanly reports its own failure and nothing else`() {
        val deliberate = "the failure this row is about"
        val e = assertFailsWith<AssertionError> {
            ownPassing("primary-only", "hang") { owned ->
                awaitDeclaredOutput(owned.child)
                throw AssertionError(deliberate)
            }
        }
        assertEquals(deliberate, e.message, "the arm's own failure must be the one reported")
        assertEquals(0, e.suppressed.size, "clean cleanup must attach nothing")
    }

    @Test
    fun `an arm that succeeds but cannot retire what it owned fails on the cleanup alone`() =
        aroundRealChild("cleanup-only") { real ->
            awaitDeclaredOutput(real)
            val instrumented = EndpointAbi.OwnedChild(real.name, InstrumentedProcess(real.process, withholdTermination = true), real.log)
            val e = assertFailsWith<AssertionError> {
                reportOwnership("cleanup-only", own("cleanup-only", instrumented) { })
            }
            assertNamesTheSurvivor("cleanup-only", e, real)
            assertEquals(0, e.suppressed.size, "with no primary failure the cleanup failure IS the failure")
            assertTrue(real.log.exists(), "a surviving child's only diagnostic must be kept, not deleted")
            assertEquals(
                ChildRunnerProbeMain.HANG_OUTPUT, real.log.readLines(),
                "and it must still hold what the child wrote",
            )
        }

    @Test
    fun `an arm that fails AND cannot retire what it owned reports its own failure with the cleanup attached`() =
        aroundRealChild("mixed") { real ->
            awaitDeclaredOutput(real)
            val deliberate = AssertionError("the failure this row is about")
            val instrumented = EndpointAbi.OwnedChild(real.name, InstrumentedProcess(real.process, withholdTermination = true), real.log)
            val e = assertFailsWith<AssertionError> {
                reportOwnership("mixed", own("mixed", instrumented) { throw deliberate })
            }
            assertSame(deliberate, e, "the arm's own failure must be the thrown one, not the cleanup's")
            assertEquals(1, e.suppressed.size, "the cleanup failure must be attached, exactly once")
            assertNamesTheSurvivor("mixed", e, real)
            assertTrue(real.log.exists(), "a surviving child's only diagnostic must be kept, not deleted")
        }

    // -- the ownership boundary itself, before any helper exists -----------------    // -- the ownership boundary itself, before any helper exists -----------------

    @Test
    fun `a setup that fails after the child is running still retires it, without waiting out its self-limit`() {
        val deliberate = "deliberate setup failure after the child's declared output"
        val o = own("setup-failure", "hang") { owned ->
            awaitDeclaredOutput(owned.child)
            throw AssertionError(deliberate)
        }
        assertEquals(deliberate, o.primary?.message, "the primary setup failure must be the one reported")
        assertEquals(emptyList(), o.cleanupProblems, "the arm did not retire what it owned")
        assertTrue(!o.childAlive, "the exact child is still running after a failed setup")
        assertEquals(0, o.helpersAlive, "a helper survived a failed setup")
        assertTrue(
            o.elapsedMillis < ChildRunnerProbeMain.SELF_LIMIT_MILLIS,
            "the child was left to its ${ChildRunnerProbeMain.SELF_LIMIT_MILLIS} ms self-limit; the arm took ${o.elapsedMillis} ms",
        )
    }

    @Test
    fun `a setup interrupted after the child is running still retires it, and no helper survives`() {
        val o = own("setup-interrupted", "hang") { owned ->
            awaitDeclaredOutput(owned.child)
            // A real interruption of the setup itself, before any helper exists.
            Thread.currentThread().interrupt()
            Thread.sleep(1)
        }
        assertTrue(o.primary is InterruptedException, "the interrupted setup must report its own interruption; got ${o.primary}")
        assertEquals(emptyList(), o.cleanupProblems, "the arm did not retire what it owned")
        assertTrue(!o.childAlive, "the exact child is still running after an interrupted setup")
        assertEquals(0, o.helpersAlive, "a helper survived an interrupted setup")
        assertTrue(
            o.elapsedMillis < ChildRunnerProbeMain.SELF_LIMIT_MILLIS,
            "the child was left to its ${ChildRunnerProbeMain.SELF_LIMIT_MILLIS} ms self-limit; the arm took ${o.elapsedMillis} ms",
        )
        // The interrupt was consumed by the sleep; leave this thread clean.
        Thread.interrupted()
    }
}
