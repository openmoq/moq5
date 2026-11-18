package org.openmoq.moq5.engine

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Sticky test failures.
 *
 * A ceiling that expires is never "the wait finished". Every expiry is recorded
 * here as well as returned, and the fixture asserts this registry is empty in
 * cleanup, so an expiry whose return value a caller forgot to consume still
 * fails the test by name instead of silently releasing a blocked job and making
 * elapsed wall time part of the observed behavior.
 */
internal class TestFailures {
    private val lock = ReentrantLock()
    private val entries = mutableListOf<String>()

    fun record(message: String) = lock.withLock { entries += message }

    fun drain(): List<String> = lock.withLock { entries.toList() }

    fun assertNone(phase: String) {
        val e = drain()
        assertTrue(e.isEmpty(), "$phase: sticky test-harness failures: $e")
    }
}

/**
 * Four SEPARATE ledgers, deliberately not collapsed into one counter.
 *
 * Doing the work but losing its result is a real failure mode of a job queue,
 * and a single "it ran" tally cannot see it: a job whose body completed while
 * its caller was resolved as cancelled would look identical to a clean run.
 * Bodies, completions, cancellations and deliveries are therefore recorded
 * independently, in order.
 *
 * Thread identity is the ID. A name is a diagnostic: two live threads may share
 * one, so a same-name/different-ID pair must never satisfy a confinement claim.
 */
internal class EngineLedger {

    internal data class View(
        val started: List<String> = emptyList(),
        val completed: List<String> = emptyList(),
        val delivered: List<String> = emptyList(),
        val cancelled: List<String> = emptyList(),
        val threadIds: Map<String, Long> = emptyMap(),
        val threadNames: Map<String, String> = emptyMap(),
    )

    private val lock = ReentrantLock()
    private val changed = lock.newCondition()
    private var v = View()

    fun started(label: String) = record {
        val t = Thread.currentThread()
        v =
            v.copy(
                started = v.started + label,
                threadIds = v.threadIds + (label to t.threadId()),
                threadNames = v.threadNames + (label to t.name),
            )
    }

    fun completed(label: String) = record {
        val t = Thread.currentThread()
        v =
            v.copy(
                completed = v.completed + label,
                threadIds = v.threadIds + (label to t.threadId()),
                threadNames = v.threadNames + (label to t.name),
            )
    }

    fun delivered(label: String) = record { v = v.copy(delivered = v.delivered + label) }

    fun cancelled(label: String) = record { v = v.copy(cancelled = v.cancelled + label) }

    fun view(): View = lock.withLock { v }

    /**
     * Ceiling-bounded handshake.
     *
     * The result is ALWAYS consumed by a named assertion at the call site, so
     * an expiry here can never release anything silently and needs no sticky
     * record; [Gate] is the sticky one, because a job body genuinely ignores
     * its return.
     */
    fun await(
        what: String,
        ceilingMillis: Long = ScriptedEndpointBackend.CEILING_MILLIS,
        predicate: (View) -> Boolean,
    ): Boolean {
        val deadline = System.nanoTime() + ceilingMillis * 1_000_000
        lock.withLock {
            while (!predicate(v)) {
                val remaining = deadline - System.nanoTime()
                if (remaining <= 0) return false
                changed.awaitNanos(remaining)
            }
            return true
        }
    }

    private inline fun record(block: () -> Unit) =
        lock.withLock {
            block()
            changed.signalAll()
        }
}

/**
 * A latch the test opens explicitly.
 *
 * An expired ceiling does NOT release the waiter's caller as though the gate had
 * opened: it records a named sticky failure first. Without that, a held job
 * would simply resume after the ceiling and the suite would be measuring wall
 * time instead of asserting behavior.
 */
internal class Gate(private val name: String, private val failures: TestFailures) {
    private val lock = ReentrantLock()
    private val changed = lock.newCondition()
    private var open = false

    fun open() = lock.withLock {
        open = true
        changed.signalAll()
    }

    fun await(ceilingMillis: Long = ScriptedEndpointBackend.CEILING_MILLIS): Boolean {
        val deadline = System.nanoTime() + ceilingMillis * 1_000_000
        lock.withLock {
            while (!open) {
                val remaining = deadline - System.nanoTime()
                if (remaining <= 0) {
                    failures.record("gate '$name' ceiling expired without being opened")
                    return false
                }
                changed.awaitNanos(remaining)
            }
            return true
        }
    }
}

/**
 * Join a thread within a bound and fail by name if it is still alive. An
 * unbounded `join()` after a release attempt can hang the whole test process.
 */
internal fun joinBounded(
    t: Thread,
    what: String,
    ceilingMillis: Long = ScriptedEndpointBackend.CEILING_MILLIS,
) {
    t.join(ceilingMillis)
    assertEquals(false, t.isAlive, "$what: thread '${t.name}' (id ${t.threadId()}) is still alive")
}
