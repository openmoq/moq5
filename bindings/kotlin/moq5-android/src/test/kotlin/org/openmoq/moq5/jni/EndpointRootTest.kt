package org.openmoq.moq5.jni

import java.io.File
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * The endpoint-root constructor boundary.
 *
 * The selector is read once during `JNI_OnLoad`, before registration exposes any
 * native method, so which target a process uses is fixed before anything is
 * callable and cannot be varied inside one JVM. Every selector state therefore
 * runs in its OWN PROCESS: the real target is this JVM, with the variable
 * explicitly absent, and each other state is a child JVM this test starts with
 * exactly one environment difference.
 *
 * A child is used rather than three more Gradle tasks so that one task, one
 * `-Xcheck:jni` configuration and one diagnostics gate still judge every row.
 */
internal object EndpointAbi {
    /** Mirrors the C result codes this boundary returns verbatim. */
    const val OK: Int = 0
    const val ERR_NOMEM: Int = -1
    const val ERR_INVAL: Int = -2
    const val ERR_UNSUPPORTED: Int = -14
    const val INVAL: Int = -2
    const val WRONG_STATE: Int = -5
    const val WOULD_BLOCK: Int = -8
    const val DONE: Int = 1
    const val INTERRUPTED: Int = -13
    const val ERR_INTERRUPTED: Int = -13
    const val CLOSED: Int = -4

    /** Mirrors `MOQ5_ODD_WAIT_RESULT`: no other result uses it. */
    const val ODD: Int = -777

    /** Mirrors `MOQ5_FUTURE_VERSION`. */
    const val FUTURE_VERSION: Long = 0x7FFFFFFFL

    /** Mirrors `MOQ_ENDPOINT_CLOSED` and `MOQ_ENDPOINT_TERMINAL_PROTOCOL`. */
    const val STATE_CLOSED: Long = 4L
    const val TERMINAL_PROTOCOL: Long = 2L

    /** Mirrors `MOQ5_MODE_ENV` and its accepted values, host-only. */
    const val MODE: String = "MOQ5_JNI_HOST_TEST_MODE"
    const val MODE_STOP_REFUSE: String = "stop-refuse-v1"
    const val MODE_INTERRUPT: String = "wait-interrupt-v1"
    const val MODE_WAKE_HOLD: String = "wake-hold-v1"
    const val MODE_WAIT_CLOSED: String = "wait-closed-v1"
    const val MODE_WAIT_ODD: String = "wait-odd-v1"
    const val MODE_SNAP_FUTURE: String = "snapshot-future-v1"
    const val MODE_DRAIN_PENDING: String = "drain-pending-v1"
    const val MODE_DRAIN_DONE: String = "drain-done-v1"
    const val MODE_DRAIN_UNSUPPORTED: String = "drain-unsupported-v1"

    /** Inputs the scripted facade supplies; the assertions are about the JNI. */
    const val SCRIPTED_VERSION: Long = 0x7FFFFFFFL
    const val SCRIPTED_DETAIL: Long = -0x7FFFFFFFFFFFFFFFL  // 0x8000_0000_0000_0001

    /** `MOQ5_KIND_ROOT`, the low byte of every root token. */
    const val KIND_ROOT: Long = 1L

    /** The exact accepted selector value; mirrors `MOQ5_CTOR_TOKEN`. */
    const val SELECTOR: String = "MOQ5_JNI_HOST_TEST_CONSTRUCTOR"
    const val SCRIPTED_TOKEN: String = "scripted-endpoint-v1"

    /** Mirrors `MOQ5_FAILPUB_ENV` / `MOQ5_FAILPUB_TOKEN`, host-only. */
    const val FAILPUB: String = "MOQ5_JNI_HOST_TEST_FAIL_PUBLISH"
    const val FAILPUB_TOKEN: String = "fail-publish-v1"

    /**
     * A value no result or token ever takes, seeded into an output slot so
     * "untouched" is a positive observation and not a zero that a failed write
     * could also have left.
     */
    const val SENTINEL: Long = 0x5EED_0000_0000_5EEDL

    /* Host-only lifecycle verdicts, reported through the ordinary result
     * channel so a violation is named rather than inferred from a crash. */
    const val LC_BASE: Int = -2000
    const val LC_DESTROY_BEFORE_STOP: Int = LC_BASE - 1
    const val LC_DOUBLE_DESTROY: Int = LC_BASE - 2
    const val LC_LEAKED: Int = LC_BASE - 3
    const val LC_CLEANUP_RACED: Int = LC_BASE - 5
    const val LC_RETIRING_ACCEPTED: Int = LC_BASE - 6
    const val LC_MILESTONE_REPEATED: Int = LC_BASE - 7

    /* Registry categories as the bridge reports them: MOQ5_ERR_REGISTRY_BASE
     * plus the negative `moq5_reg_result_t`. Asserting the exact category is
     * what distinguishes "the registry refused for the right reason" from a
     * collapsed or early failure. */
    const val REG_BASE: Int = -1000
    const val REG_INVALID: Int = REG_BASE - 1
    const val REG_STALE: Int = REG_BASE - 2
    const val REG_KIND: Int = REG_BASE - 3
    const val REG_OWNER: Int = REG_BASE - 4
    const val REG_THREAD: Int = REG_BASE - 5
    const val REG_STATE: Int = REG_BASE - 6
    const val REG_CAPACITY: Int = REG_BASE - 7

    /** `MOQ5_REG_ERR_TYPE`: a live token of the right lifetime kind but the wrong CONCRETE type. */
    const val REG_TYPE: Int = REG_BASE - 10

    /** Token fields, mirroring `moq5_registry.h`: generation 63..32, slot 31..8, kind 7..0. */
    fun slotOf(token: Long): Long = (token ushr 8) and 0xFFFFFFL
    fun generationOf(token: Long): Long = (token ushr 32) and 0xFFFFFFFFL
    fun tokenOf(kind: Long, slot: Long, generation: Long): Long = (generation shl 32) or (slot shl 8) or kind

    /**
     * A well-formed request this build cannot serve.
     *
     * The host image links no managed backend on purpose, so the real
     * constructor reaches its "no managed facade in this build" refusal without
     * a socket, a DNS lookup or a peer.
     */
    const val UNSERVABLE: String = "moqt://example.invalid:4433/p"

    /** The same unservable host over WebTransport: an https URL the parser accepts and no facade can serve. */
    const val UNSERVABLE_WT: String = "https://example.invalid:4433/live"

    fun kindOf(token: Long): Long = token and 0xFFL

    fun load() = System.load(BridgeDsos.path("moq5_jni"))

    /** The outer bound on a child. It decides no protocol or lifecycle outcome: expiry is a named SETUP failure. */
    const val CHILD_CEILING_MILLIS: Long = 120_000

    /** The bound on retiring a child that had to be terminated. Containment only. */
    const val CHILD_REAP_MILLIS: Long = 10_000

    /**
     * A child process this JVM owns, together with the file its merged output
     * goes to.
     *
     * The output is redirected to a FILE rather than a pipe, so waiting for the
     * process never depends on the child closing its stream and there is no
     * reader thread to leak.
     */
    internal class OwnedChild(val name: String, val process: Process, val log: File)

    /**
     * What became of an owned child once this JVM was finished with it.
     *
     * [reaped] is the honest boundary: only a reaped child's [output] is
     * complete, and only a reaped child has an [exitStatus]. A child that
     * survived leaves whatever it had flushed so far, which is a diagnostic and
     * never a result.
     */
    internal class ChildOutcome(
        val name: String,
        val output: List<String>,
        val outputComplete: Boolean,
        val reaped: Boolean,
        val finishedWithinBound: Boolean,
        val exitStatus: Int?,
        val interrupted: Boolean,
        val cleanupProblems: List<String>,
    ) {
        /** The child's own words, for a failure that has to carry them. */
        val diagnostics: String
            get() {
                val body = if (output.isEmpty()) "(no output)" else output.joinToString("\n")
                return if (outputComplete) body else "$body\n(the child was not retired, so this output may be incomplete)"
            }
    }

    /** Start [scenario] in a child JVM whose only difference from this one is the selector variable. */
    internal fun startChild(
        scenario: String,
        selector: String?,
        extra: Map<String, String> = emptyMap(),
        classPath: String = System.getProperty("java.class.path").orEmpty(),
        jvmArgs: List<String> = emptyList(),
        mainClass: String = EndpointChildMain::class.java.name,
    ): OwnedChild {
        val java = File(File(System.getProperty("java.home"), "bin"), "java")
        assertTrue(java.canExecute(), "setup: no java at ${java.path}")
        val log = File.createTempFile("moq5-child-$scenario-", ".log")
        log.deleteOnExit()
        val cmd = listOf(
            java.path, "-Xcheck:jni",
            "-Djava.library.path=" + File("../.native/_bridge").absolutePath,
        ) + jvmArgs + listOf(
            "-cp", classPath,
            mainClass, scenario,
        )
        val pb = ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(log)
        if (selector == null) pb.environment().remove(SELECTOR)
        else pb.environment()[SELECTOR] = selector
        extra.forEach { (k, v) -> pb.environment()[k] = v }
        return OwnedChild(scenario, pb.start(), log)
    }

    /**
     * Wait for an owned child within [ceilingMillis], then retire it.
     *
     * The bound is an outer failure bound; it decides no protocol or lifecycle
     * outcome. Whatever ends the wait -- the child finishing, the bound
     * expiring, or an interruption -- the same retirement follows: the exact
     * process this JVM started is terminated and reaped under ONE monotonic
     * cleanup deadline, and further interruptions are absorbed and retried
     * inside that deadline rather than abandoning the child. The interrupt is
     * restored only once cleanup is over, so no caller sees it early and
     * cleanup is never cut short by it.
     *
     * The output file is sampled only after the retirement attempt. If the
     * child was reaped, the file is complete and is removed; if it survived,
     * the sample is still reported -- it is the only diagnostic there is -- but
     * is declared INCOMPLETE and the file is left where it is, because a live
     * child may still be writing to it. Failures to read or remove it are
     * reported, never swallowed.
     */
    internal fun awaitOwnedChild(owned: OwnedChild, ceilingMillis: Long = CHILD_CEILING_MILLIS): ChildOutcome {
        val p = owned.process
        val problems = mutableListOf<String>()
        var interrupted = false

        val finished =
            try {
                p.waitFor(ceilingMillis, TimeUnit.MILLISECONDS)
            } catch (_: InterruptedException) {
                interrupted = true
                false
            }

        var reaped = !p.isAlive
        if (!reaped) {
            p.destroyForcibly()
            val deadline = System.nanoTime() + CHILD_REAP_MILLIS * 1_000_000
            while (!reaped) {
                val leftMillis = (deadline - System.nanoTime()) / 1_000_000
                if (leftMillis <= 0) break
                reaped =
                    try {
                        p.waitFor(leftMillis, TimeUnit.MILLISECONDS)
                    } catch (_: InterruptedException) {
                        // Absorbed and retried WITHIN the same deadline: an
                        // interrupt must not leave a child of ours running.
                        interrupted = true
                        false
                    }
            }
            if (!reaped) reaped = !p.isAlive
        }

        // Sampled only now. A reaped child's file is COMPLETE and is reported
        // as such; a surviving child's is sampled too -- it is the only
        // diagnostic there is -- and carried as explicitly incomplete, because
        // it may still be being written.
        val output =
            try {
                owned.log.readLines()
            } catch (e: Exception) {
                problems += "the child's output file could not be read: $e"
                emptyList()
            }
        if (reaped) {
            // Removed only once nothing can write to it any more.
            if (owned.log.exists() && !owned.log.delete()) {
                problems += "the child's output file ${owned.log.path} could not be removed"
            }
        } else {
            problems += "the child was not retired; its output file ${owned.log.path} is left in place"
        }

        if (interrupted) Thread.currentThread().interrupt()

        return ChildOutcome(
            name = owned.name,
            output = output,
            outputComplete = reaped,
            reaped = reaped,
            finishedWithinBound = finished,
            exitStatus = if (reaped) p.exitValue() else null,
            interrupted = interrupted,
            cleanupProblems = problems,
        )
    }

    /**
     * The child's output, or a named SETUP failure.
     *
     * Every way a child can fail to stand behind its own output is named here,
     * in one place, and all of them are reported together: output that merely
     * happens to contain a result line never passes on its own, and a cleanup
     * problem never hides the failure that came first. Intentional
     * load-refusal children print their line and exit normally, so they are
     * unaffected.
     */
    internal fun requireOwnedChild(outcome: ChildOutcome, ceilingMillis: Long): List<String> {
        val problems = mutableListOf<String>()
        if (!outcome.reaped) problems += "it survived termination"
        if (!outcome.finishedWithinBound) {
            problems += "it did not finish within $ceilingMillis ms and was terminated"
        }
        if (outcome.interrupted) problems += "waiting for it was interrupted"
        outcome.exitStatus?.let { if (it != 0) problems += "it exited $it" }
        problems += outcome.cleanupProblems
        assertTrue(
            problems.isEmpty(),
            "setup: the child '${outcome.name}' cannot stand as a result -- " +
                problems.joinToString("; ") + "; output was:\n" + outcome.diagnostics,
        )
        return outcome.output
    }

    /**
     * Run [scenario] in a child JVM and return its complete output.
     *
     * Start, retire, judge: the three operations above, in that order. The
     * runner's own contract rows drive the SAME three, so nothing here is a
     * second implementation of them.
     */
    fun child(
        scenario: String,
        selector: String?,
        extra: Map<String, String> = emptyMap(),
        /** The child's class path; this JVM's own unless a row supplies a variant. */
        classPath: String = System.getProperty("java.class.path").orEmpty(),
        /** Extra JVM options, placed before the main class. */
        jvmArgs: List<String> = emptyList(),
        /** The child's main class; the scenario runner unless a row drives the runner's own contract. */
        mainClass: String = EndpointChildMain::class.java.name,
        /** The outer bound, lowered only by the runner's own rows. */
        ceilingMillis: Long = CHILD_CEILING_MILLIS,
    ): List<String> {
        val owned = startChild(scenario, selector, extra, classPath, jvmArgs, mainClass)
        return requireOwnedChild(awaitOwnedChild(owned, ceilingMillis), ceilingMillis)
    }
}

/**
 * THE REAL TARGET, with the selector absent from this very process.
 *
 * The discriminator is independently derived rather than chosen here: with no
 * managed backend linked, `moq_endpoint_connect()` refuses a well-formed
 * request with MOQ_ERR_UNSUPPORTED and a malformed one with MOQ_ERR_INVAL,
 * before any socket exists. The scripted target ignores its configuration
 * entirely and so cannot produce either value -- which is what makes these rows
 * evidence that the default target is the real function.
 */
class EndpointRootRealTargetTest {

    @Test
    fun `the selector is absent from this process`() {
        assertEquals(
            null,
            System.getenv(EndpointAbi.SELECTOR),
            "setup: the real row must run with the selector unset",
        )
    }

    @Test
    fun `an unservable but well-formed request refuses with UNSUPPORTED and publishes nothing`() {
        EndpointAbi.load()
        val out = longArrayOf(0L)
        assertEquals(
            EndpointAbi.ERR_UNSUPPORTED,
            NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, out),
            "the real constructor must refuse a request this build cannot serve",
        )
        assertEquals(0L, out[0], "a failed construction must publish no handle")
    }

    @Test
    fun `a malformed request refuses with INVAL and publishes nothing`() {
        EndpointAbi.load()
        val out = longArrayOf(0L)
        assertEquals(
            EndpointAbi.ERR_INVAL,
            NativeBridge.nativeEndpointCreate("", out),
            "an empty URL is refused by the real constructor, not by the bridge",
        )
        assertEquals(0L, out[0], "a failed construction must publish no handle")
    }

    @Test
    fun `an undersized output array is refused before anything is constructed`() {
        EndpointAbi.load()
        assertEquals(
            EndpointAbi.ERR_INVAL,
            NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, LongArray(0)),
        )
    }

    @Test
    fun `tokens that were never issued are refused and clean nothing`() {
        EndpointAbi.load()
        assertTrue(NativeBridge.nativeEndpointRelease(0L) < 0, "the invalid token is refused")
        assertTrue(
            NativeBridge.nativeEndpointRelease(0x7FFF_FFFF_FFFF_FF01L) < 0,
            "a forged token is refused, and is never dereferenced",
        )
    }
}

/**
 * THE SCRIPTED TARGET, in its own process.
 *
 * It constructs a real `moq_endpoint_t` through the service's own bare-endpoint
 * and probe seam -- absent from every shipped artifact -- so publication,
 * ownership and retirement run through the real registry and the real endpoint
 * lifecycle with no socket, thread or peer.
 */
class EndpointRootScriptedTargetTest {

    @Test
    fun `the scripted target constructs, publishes one root token, and retires exactly once`() {
        val out = EndpointAbi.child("scripted", EndpointAbi.SCRIPTED_TOKEN)
        assertTrue(
            out.contains("RESULT ok"),
            "the scripted child must complete its own oracles; output was:\n" + out.joinToString("\n"),
        )
    }

    @Test
    fun `the scripted target cannot synthesize the real discriminator`() {
        val out = EndpointAbi.child("discriminator", EndpointAbi.SCRIPTED_TOKEN)
        assertTrue(
            out.contains("RESULT ok"),
            "the scripted constructor ignores the configuration, so neither " +
                "INVAL nor UNSUPPORTED can come out of it; output was:\n" + out.joinToString("\n"),
        )
    }
}

/**
 * The selector's refusal, proven at LOAD time.
 *
 * An unrecognised value must refuse the whole load with a named diagnostic --
 * never bind a method that silently uses the wrong target. Absent means the
 * real function, which is the row above.
 */
/**
 * OWNERSHIP ON THE FAILING BRANCHES.
 *
 * Both rows assert the same rule from opposite ends: on every branch an
 * endpoint has exactly one durable owner, and a construction that does not
 * complete consumes nothing at all. A result code is not the oracle -- slot
 * reuse is, because a consumed reservation or an abandoned root would move the
 * next token.
 */
class EndpointRootOwnershipTest {

    @Test
    fun `a full registry refuses before constructing and consumes nothing`() {
        val out = EndpointAbi.child("capacity", EndpointAbi.SCRIPTED_TOKEN)
        assertTrue(
            out.contains("RESULT ok"),
            "the capacity refusal must consume no slot or generation; output was:\n" +
                out.joinToString("\n"),
        )
    }

    @Test
    fun `a failed token publication leaves no owner behind and no token with the caller`() {
        val out = EndpointAbi.child(
            "publishfail",
            EndpointAbi.SCRIPTED_TOKEN,
            mapOf(EndpointAbi.FAILPUB to EndpointAbi.FAILPUB_TOKEN),
        )
        assertTrue(
            out.contains("RESULT ok"),
            "a failed publication must stop the registry-owned endpoint and retire the " +
                "root, letting registry cleanup destroy it exactly once; output was:\n" +
                out.joinToString("\n"),
        )
    }

    @Test
    fun `an unrecognised injection value refuses the load like the selector does`() {
        val out = EndpointAbi.child(
            "load",
            EndpointAbi.SCRIPTED_TOKEN,
            mapOf(EndpointAbi.FAILPUB to "nonsense"),
        )
        val line = out.firstOrNull { it.startsWith("LOADFAIL ") }
        assertTrue(line != null, "the load must be refused; output was:\n" + out.joinToString("\n"))
        assertTrue(
            line!!.contains("publish-failure injection") && line.contains("nonsense"),
            "the refusal must name the injection and quote the value: $line",
        )
    }
}

class EndpointSelectorRejectTest {

    private fun refusalFor(value: String): String {
        val out = EndpointAbi.child("load", value)
        val line = out.firstOrNull { it.startsWith("LOADFAIL ") }
        assertTrue(
            line != null,
            "the load must be refused; output was:\n" + out.joinToString("\n"),
        )
        return line!!.removePrefix("LOADFAIL ")
    }

    @Test
    fun `an unrecognised selector value refuses the load by name`() {
        val m = refusalFor("not-a-token")
        assertTrue(m.contains("moq5-bridge"), "the refusal must be the bridge's own: $m")
        assertTrue(m.contains("constructor selector"), "the refusal must name the selector: $m")
        assertTrue(m.contains("not-a-token"), "the refusal must quote the offending value: $m")
    }

    @Test
    fun `an empty selector value is refused rather than treated as absent`() {
        val m = refusalFor("")
        assertTrue(m.contains("moq5-bridge"), "the refusal must be the bridge's own: $m")
        assertTrue(
            m.contains("present but empty"),
            "an empty value must be distinguished from an absent one: $m",
        )
    }
}

/**
 * THE TOKEN-BACKED STATEFUL BOUNDARY.
 *
 * Every row here operates on a real registry token: no native pointer is ever
 * stored or returned, and each stateful call re-acquires the root, enforces its
 * kind and authority thread, pins for the duration of the C call and releases
 * once. `wake` is the single any-thread exception and leases instead.
 */
class EndpointBackendTest {

    @Test
    fun `the stateful operations run on a real token`() {
        val out = EndpointAbi.child("backend", EndpointAbi.SCRIPTED_TOKEN)
        assertTrue(
            out.contains("RESULT ok"),
            "the backend rows must hold; output was:\n" + out.joinToString("\n"),
        )
    }

    @Test
    fun `a retired token reaches no endpoint through any stateful operation`() {
        val out = EndpointAbi.child("retired", EndpointAbi.SCRIPTED_TOKEN)
        assertTrue(
            out.contains("RESULT ok"),
            "every operation must refuse a retired token by registry category; output was:\n" +
                out.joinToString("\n"),
        )
    }

    @Test
    fun `a refused stop is carried exactly and nothing is retired`() {
        val out = EndpointAbi.child(
            "stoprefuse", EndpointAbi.SCRIPTED_TOKEN,
            mapOf(EndpointAbi.MODE to EndpointAbi.MODE_STOP_REFUSE),
        )
        assertTrue(
            out.contains("RESULT ok"),
            "a refused stop must leave the root live, unready and uncleaned; output was:\n" +
                out.joinToString("\n"),
        )
    }

    @Test
    fun `an interrupted wait crosses as MOQ_ERR_INTERRUPTED`() {
        val out = EndpointAbi.child(
            "interrupt", EndpointAbi.SCRIPTED_TOKEN,
            mapOf(EndpointAbi.MODE to EndpointAbi.MODE_INTERRUPT),
        )
        assertTrue(
            out.contains("RESULT ok"),
            "the interrupt result must cross exactly; output was:\n" + out.joinToString("\n"),
        )
    }

    @Test
    fun `an any-thread wake holds its lease across a concurrent retirement`() {
        val out = EndpointAbi.child(
            "wakerace", EndpointAbi.SCRIPTED_TOKEN,
            mapOf(EndpointAbi.MODE to EndpointAbi.MODE_WAKE_HOLD),
        )
        assertTrue(
            out.contains("RESULT ok"),
            "cleanup must not run while a lease is held; output was:\n" + out.joinToString("\n"),
        )
    }

    @Test
    fun `a closed wait result crosses exactly`() {
        val out = EndpointAbi.child(
            "waitclosed", EndpointAbi.SCRIPTED_TOKEN,
            mapOf(EndpointAbi.MODE to EndpointAbi.MODE_WAIT_CLOSED),
        )
        assertTrue(out.contains("RESULT ok"),
            "MOQ_ERR_CLOSED must not be folded; output was:\n" + out.joinToString("\n"))
    }

    @Test
    fun `an unexpected negative wait result crosses exactly`() {
        val out = EndpointAbi.child(
            "waitodd", EndpointAbi.SCRIPTED_TOKEN,
            mapOf(EndpointAbi.MODE to EndpointAbi.MODE_WAIT_ODD),
        )
        assertTrue(out.contains("RESULT ok"),
            "an unknown result must not be normalised; output was:\n" + out.joinToString("\n"))
    }

    @Test
    fun `a future service record crosses the snapshot boundary intact`() {
        val out = EndpointAbi.child(
            "snapfuture", EndpointAbi.SCRIPTED_TOKEN,
            mapOf(EndpointAbi.MODE to EndpointAbi.MODE_SNAP_FUTURE),
        )
        assertTrue(out.contains("RESULT ok"),
            "a future version and a full-width detail must survive; output was:\n" +
                out.joinToString("\n"))
    }

    @Test
    fun `the real backend maps every native answer into the right domain`() {
        val out = EndpointAbi.child("backendmap", EndpointAbi.SCRIPTED_TOKEN)
        assertTrue(
            out.contains("RESULT ok"),
            "the JNI backend must keep the registry and endpoint domains apart; output was:\n" +
                out.joinToString("\n"),
        )
    }

    @Test
    fun `the real backend maps one snapshot record without narrowing it`() {
        val out = EndpointAbi.child(
            "backendsnap", EndpointAbi.SCRIPTED_TOKEN,
            mapOf(EndpointAbi.MODE to EndpointAbi.MODE_SNAP_FUTURE),
        )
        assertTrue(
            out.contains("RESULT ok"),
            "a future version and a full-width detail must survive the mapping; output was:\n" +
                out.joinToString("\n"),
        )
    }

    @Test
    fun `a wait answer outside the contract becomes a bridge failure, not a state`() {
        val out = EndpointAbi.child(
            "backendwaitodd", EndpointAbi.SCRIPTED_TOKEN,
            mapOf(EndpointAbi.MODE to EndpointAbi.MODE_WAIT_ODD),
        )
        assertTrue(
            out.contains("RESULT ok"),
            "an out-of-contract wait result must not be normalised; output was:\n" +
                out.joinToString("\n"),
        )
    }

    @Test
    fun `an interrupted wait maps to INTERRUPTED and leaves the root live`() {
        val out = EndpointAbi.child(
            "backendinterrupt", EndpointAbi.SCRIPTED_TOKEN,
            mapOf(EndpointAbi.MODE to EndpointAbi.MODE_INTERRUPT),
        )
        assertTrue(
            out.contains("RESULT ok"),
            "MOQ_ERR_INTERRUPTED must map to INTERRUPTED, outrank a raised level, and leave " +
                "the root tearable down; output was:\n" + out.joinToString("\n"),
        )
    }

    @Test
    fun `a terminal wait answer maps to CLOSED`() {
        val out = EndpointAbi.child(
            "backendwaitclosed", EndpointAbi.SCRIPTED_TOKEN,
            mapOf(EndpointAbi.MODE to EndpointAbi.MODE_WAIT_CLOSED),
        )
        assertTrue(
            out.contains("RESULT ok"),
            "MOQ_ERR_CLOSED must map to CLOSED; output was:\n" + out.joinToString("\n"),
        )
    }

    @Test
    fun `a stateful operation off the authority thread is refused`() {
        val out = EndpointAbi.child("foreign", EndpointAbi.SCRIPTED_TOKEN)
        assertTrue(
            out.contains("RESULT ok"),
            "only wake may run off the authority thread; output was:\n" + out.joinToString("\n"),
        )
    }
}
