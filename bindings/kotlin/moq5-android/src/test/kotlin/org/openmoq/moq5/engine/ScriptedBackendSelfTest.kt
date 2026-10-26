package org.openmoq.moq5.engine

import kotlin.concurrent.thread
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * Non-vacuity for the oracle itself, established BEFORE the engine contract is
 * asserted. If the scripted backend's wake were not level-retained, if a zero
 * timeout parked, if a post-stop wake resurrected activity, or if the tripwires
 * and barriers did not fire, an engine test built on it could pass while
 * proving nothing.
 *
 * These are expected to PASS in this tranche: they test the double, not the
 * engine.
 */
class ScriptedBackendSelfTest {

    private val failures = TestFailures()

    private fun backend() = ScriptedEndpointBackend(failures)

    private val slice = ScriptedEndpointBackend.WAIT_SLICE_MICROS

    @AfterTest
    fun noStickyFailures() = failures.assertNone("backend self-test")

    // -- the wake level -------------------------------------------------------

    @Test
    fun `a wake landing before the wait is retained and consumed by it`() {
        val b = backend()
        b.wake() // lands while nobody is waiting
        assertTrue(b.observed().activityLevel, "the wake did not raise the activity level")

        assertEquals(
            BackendWaitResult.ACTIVITY,
            b.waitForActivity(slice),
            "a level-retained wake must make the next wait return immediately",
        )
        assertFalse(b.observed().activityLevel, "the single consumer must take the level")
    }

    @Test
    fun `repeated wakes coalesce into one level`() {
        val b = backend()
        repeat(3) { b.wake() }
        assertEquals(3, b.observed().wakeCount, "every wake must be recorded")

        assertEquals(BackendWaitResult.ACTIVITY, b.waitForActivity(slice))

        // Coalesced: three wakes yield ONE satisfied wait, not three.
        val second = thread(start = true) { b.waitForActivity(slice) }
        assertTrue(
            b.awaitCondition("the second wait parks") { it.waitParked },
            "the second wait must block: three coalesced wakes are one level",
        )
        b.expireWait()
        joinBounded(second, "coalescing control")
        assertEquals(
            listOf(BackendWaitResult.ACTIVITY, BackendWaitResult.TIMEOUT),
            b.observed().waitReturns,
        )
    }

    // -- the timeout contract -------------------------------------------------

    @Test
    fun `a zero timeout elapses immediately instead of parking`() {
        val b = backend()
        assertEquals(
            BackendWaitResult.TIMEOUT,
            b.waitForActivity(0),
            "endpoint.h: a zero timeout must elapse at once, not park",
        )
        assertFalse(b.observed().waitParked, "a zero-timeout wait must never park")
    }

    @Test
    fun `a zero timeout still consumes a retained level`() {
        val b = backend()
        b.wake()
        assertEquals(BackendWaitResult.ACTIVITY, b.waitForActivity(0))
        assertFalse(b.observed().activityLevel, "the level must be consumed")
    }

    @Test
    fun `a positive wait parks until an explicit scripted event releases it`() {
        val b = backend()
        val t = thread(start = true) { b.waitForActivity(slice) }

        assertTrue(b.awaitCondition("the wait parks") { it.waitParked }, "the wait did not park")
        assertEquals(
            emptyList(),
            b.observed().waitReturns,
            "the wait returned while nothing had released it",
        )

        b.markActivity()
        joinBounded(t, "parked-wait control")
        assertEquals(listOf(BackendWaitResult.ACTIVITY), b.observed().waitReturns)
    }

    @Test
    fun `a parked wait expires only on an explicit expiry event`() {
        val b = backend()
        val t = thread(start = true) { b.waitForActivity(slice) }
        assertTrue(b.awaitCondition("the wait parks") { it.waitParked })

        b.expireWait()
        joinBounded(t, "expiry control")
        assertEquals(listOf(BackendWaitResult.TIMEOUT), b.observed().waitReturns)
        assertEquals(1, b.observed().expiries, "the expiry must be scripted, never wall-clock")
    }

    // -- precedence -----------------------------------------------------------

    @Test
    fun `the latch beats a retained level and does not consume it`() {
        val b = backend()
        b.markActivity()
        b.armInterrupted(true)

        assertEquals(BackendWaitResult.INTERRUPTED, b.waitForActivity(slice))
        assertTrue(
            b.observed().activityLevel,
            "the latch must not consume the retained activity level",
        )
    }

    @Test
    fun `a terminal endpoint reports closed`() {
        val b = backend()
        b.setClosed(true)
        assertEquals(BackendWaitResult.CLOSED, b.waitForActivity(slice))
    }

    // -- wake after stop / terminal ------------------------------------------

    @Test
    fun `a wake after stop is a no-op and cannot resurrect activity`() {
        val b = backend()
        b.stop()
        b.wake()

        val s = b.observed()
        assertEquals(0, s.wakeCount, "endpoint.h: a wake after stop must be a no-op")
        assertEquals(1, s.postStopWakeCount, "the wake must be recorded as a POST-STOP wake")
        assertEquals(0, s.externalTerminalWakeCount, "it was not an external terminal")
        assertFalse(s.activityLevel, "a post-stop wake must not raise activity")
        assertEquals(
            BackendWaitResult.CLOSED,
            b.waitForActivity(0),
            "endpoint.c:1877-1880: stop sets the closed state, so a later wait reports " +
                "CLOSED -- and a post-stop wake must not satisfy it either",
        )
    }

    @Test
    fun `stop is terminal`() {
        val b = backend()
        b.stop()
        assertEquals(
            BackendWaitResult.CLOSED,
            b.waitForActivity(0),
            "moq_endpoint_stop() sets ep->closed, so the endpoint is terminal afterwards",
        )
    }

    @Test
    fun `a wake once the endpoint is externally terminal is a benign no-op`() {
        // The transport can terminalise the endpoint asynchronously, so this
        // wake races every legitimate submission and endpoint.h defines it as
        // harmless. It is recorded under its OWN cause and must never be
        // conflated with a wake published after the engine's own stop.
        val b = backend()
        b.setClosed(true)
        b.wake()

        val s = b.observed()
        assertEquals(0, s.wakeCount, "an externally terminal endpoint must ignore the wake")
        assertEquals(1, s.externalTerminalWakeCount, "recorded under the external-terminal cause")
        assertEquals(0, s.postStopWakeCount, "it must not count as a post-stop wake")
        assertEquals(emptyList(), s.violations, "an external terminal wake is not a violation")
        assertFalse(s.activityLevel)
    }

    // -- tripwires ------------------------------------------------------------

    @Test
    fun `a second thread on the blocking surface is a confinement violation`() {
        val b = backend()
        b.waitForActivity(0) // defines the service thread
        assertEquals(emptyList(), b.observed().violations)

        joinBounded(thread(start = true) { b.waitForActivity(0) }, "confinement control")

        val v = b.observed().violations
        assertEquals(1, v.size, "expected exactly one confinement violation, got $v")
        assertContains(v.single(), "but the service thread is id")
    }

    @Test
    fun `confinement is decided by thread ID, not by name`() {
        val b = backend()
        val name = "moq.endpoint.service"
        val first = Thread({ b.waitForActivity(0) }, name)
        first.start()
        joinBounded(first, "same-name control (first)")

        // A DIFFERENT thread wearing the SAME name must still be a violation.
        val second = Thread({ b.waitForActivity(0) }, name)
        second.start()
        joinBounded(second, "same-name control (second)")

        assertEquals(
            1,
            b.observed().violations.size,
            "a same-name/different-ID thread must not pass as the service thread",
        )
    }

    @Test
    fun `a reentrant backend call is recorded`() {
        val b = backend()
        b.onStop = { b.wake() } // calls back while stop() is still in progress
        b.stop()

        assertContains(
            b.observed().violations,
            "reentrant backend call: wake",
            "the reentrancy tripwire did not fire",
        )
    }

    @Test
    fun `any call after destroy is recorded as a use-after-free`() {
        val b = backend()
        b.destroy()
        b.wake()
        b.destroy()

        val v = b.observed().violations
        assertContains(v, "post-destroy call: wake")
        assertContains(v, "post-destroy call: destroy", "a second destroy must be flagged")
    }

    // -- the harness's own failure semantics ---------------------------------

    @Test
    fun `an expired gate is a sticky named failure and never releases the waiter`() {
        val local = TestFailures()
        val gate = Gate("probe", local)

        assertFalse(
            gate.await(ceilingMillis = 25),
            "an unopened gate must report expiry, never release as though opened",
        )
        assertEquals(
            listOf("gate 'probe' ceiling expired without being opened"),
            local.drain(),
            "the expiry must be recorded stickily so an unconsumed return still fails",
        )
    }

    @Test
    fun `the backend retains the exact service thread it observed`() {
        // The retirement oracle joins THIS object. It is not a JVM-wide scan,
        // not a spin, and not a daemon-status check -- so what has to hold is
        // that the backend hands back the very thread it saw, and that the
        // thread can be joined once it finishes.
        val b = backend()
        val release = java.util.concurrent.CountDownLatch(1)
        val worker =
            Thread({
                b.waitForActivity(0)
                release.await()
            }, "retained-probe")
        worker.start()

        assertTrue(
            b.awaitCondition("the backend observes a service thread") { it.serviceThread != null },
            "the backend never recorded a service thread",
        )
        val retained = b.observed().serviceThread
        assertSame(worker, retained, "the backend must retain the EXACT thread it observed")
        assertEquals(worker.threadId(), b.observed().serviceThreadId, "id and object must agree")

        release.countDown()
        joinBounded(worker, "retained-thread control")
        assertFalse(retained!!.isAlive, "the retained thread must be joinable to termination")
    }

    @Test
    fun `an expired backend condition reports false instead of passing it off as met`() {
        val local = TestFailures()
        val b = ScriptedEndpointBackend(local)
        assertFalse(
            b.awaitCondition("stop happens", ceilingMillis = 25) { it.stopCount > 0 },
            "an unmet condition must never be reported as met",
        )
    }
}
