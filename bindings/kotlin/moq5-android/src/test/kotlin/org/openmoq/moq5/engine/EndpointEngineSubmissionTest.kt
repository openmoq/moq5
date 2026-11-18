package org.openmoq.moq5.engine

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * Per-shape submission boundaries.
 *
 * `post`, `perform` and `postAndWait` are three independent implementation
 * paths, so acceptance, refusal, wake accounting and the shape of the refusal
 * are asserted for each rather than proven once through `post` and assumed for
 * the others.
 */
internal class EndpointEngineSubmissionTest : EngineFixture() {

    /** Retire the engine and wait for the backend to be destroyed. */
    private fun shutDown(e: EndpointEngine, b: ScriptedEndpointBackend, what: String) {
        val closer = callBlocking("$what-close") { e.close() }
        assertTrue(
            b.awaitCondition("$what: the backend retires") { it.destroyCount == 1 },
            "$what: shutdown never reached the backend",
        )
        closer.assertCompleted()
    }

    // -- accepted: exactly one wake, body once ------------------------------

    @Test
    fun `perform commits once and publishes exactly one wake`() = runTest {
        val (e, b) = newEngine()
        val ledger = ledger()

        val caller =
            backgroundScope.launch(Dispatchers.Default) {
                ledger.delivered(e.perform { ledger.completed("performed") ; "value" })
            }
        assertTrue(
            ledger.await("perform delivers") { it.delivered.isNotEmpty() },
            "perform: an accepted submission never ran or never delivered",
        )
        caller.join()

        assertEquals(listOf("performed"), ledger.view().completed, "perform: body exactly once")
        assertEquals(listOf("value"), ledger.view().delivered, "perform: one delivery")
        assertEquals(1, b.observed().wakeCount, "perform: exactly one wake per accepted submission")
    }

    @Test
    fun `postAndWait commits once and publishes exactly one wake`() = runTest {
        val (e, b) = newEngine()
        val ledger = ledger()

        val caller =
            callBlocking("postAndWait-accepted") {
                val outcome = e.postAndWait { ledger.completed("teardown") }
                assertEquals(SubmitOutcome.ACCEPTED, outcome)
                ledger.delivered("teardown")
            }
        assertTrue(
            ledger.await("postAndWait delivers") { it.delivered.isNotEmpty() },
            "postAndWait: an accepted submission never ran or never returned",
        )
        caller.assertCompleted()

        assertEquals(listOf("teardown"), ledger.view().completed, "postAndWait: body exactly once")
        assertEquals(
            1,
            b.observed().wakeCount,
            "postAndWait: exactly one wake per accepted submission",
        )
    }

    // -- refused after shutdown: no wake, no body, the right refusal --------

    @Test
    fun `perform refuses after shutdown with EngineClosedException and no wake`() = runTest {
        val (e, b) = newEngine()
        val ledger = ledger()
        shutDown(e, b, "perform-refusal")
        val wakesBefore = b.observed().wakeCount

        val caller =
            backgroundScope.launch(Dispatchers.Default) {
                try {
                    e.perform { ledger.completed("late") }
                    fail("perform after shutdown must not succeed")
                } catch (_: EngineClosedException) {
                    ledger.cancelled("refused")
                }
            }
        caller.join()

        assertEquals(
            listOf("refused"),
            ledger.view().cancelled,
            "perform: a submission after shutdown must be refused with EngineClosedException",
        )
        assertEquals(emptyList(), ledger.view().completed, "perform: a refused body must not run")
        assertEquals(wakesBefore, b.observed().wakeCount, "perform: a refusal must not wake")
    }

    @Test
    fun `postAndWait refuses after shutdown without running the body`() = runTest {
        val (e, b) = newEngine()
        val ledger = ledger()
        shutDown(e, b, "postAndWait-refusal")
        val wakesBefore = b.observed().wakeCount

        val caller =
            callBlocking("postAndWait-refused") {
                ledger.delivered(e.postAndWait { ledger.completed("late") }.name)
            }
        caller.assertCompleted()

        assertEquals(
            listOf(SubmitOutcome.REFUSED_SHUTDOWN.name),
            ledger.view().delivered,
            "postAndWait: a submission after shutdown must return REFUSED_SHUTDOWN",
        )
        assertEquals(emptyList(), ledger.view().completed, "postAndWait: refused body must not run")
        assertEquals(wakesBefore, b.observed().wakeCount, "postAndWait: a refusal must not wake")
    }

    @Test
    fun `a pre-cancelled postAndWait after shutdown is still refused, not accepted`() = runTest {
        val (e, b) = newEngine()
        val ledger = ledger()
        shutDown(e, b, "postAndWait-precancelled-refusal")
        val wakesBefore = b.observed().wakeCount

        val caller =
            callBlocking("postAndWait-precancelled-refused") {
                coroutineScope {
                    launch {
                        coroutineContext[Job]!!.cancel()
                        ledger.delivered(e.postAndWait { ledger.completed("late") }.name)
                    }
                }
            }
        caller.assertCompleted()

        // Teardown-grade immunity means a cancelled caller still gets its work
        // done -- it does NOT mean a closed engine starts accepting again.
        assertEquals(
            listOf(SubmitOutcome.REFUSED_SHUTDOWN.name),
            ledger.view().delivered,
            "postAndWait: cancellation immunity must not turn a refusal into acceptance",
        )
        assertEquals(emptyList(), ledger.view().completed, "postAndWait: refused body must not run")
        assertEquals(
            wakesBefore,
            b.observed().wakeCount,
            "postAndWait: a refused pre-cancelled submission must not wake either",
        )
    }

    // -- one dedicated worker PER endpoint -----------------------------------

    @Test
    fun `each endpoint gets its own dedicated worker thread`() = runTest {
        val (e1, b1) = newEngine()
        val (e2, b2) = newEngine()
        val ledger = ledger()

        assertEquals(SubmitOutcome.ACCEPTED, e1.post { ledger.completed("one") })
        assertEquals(SubmitOutcome.ACCEPTED, e2.post { ledger.completed("two") })
        assertTrue(
            ledger.await("both engines run their job") { it.completed.size == 2 },
            "per-endpoint worker: both engines must run their own job, saw ${ledger.view().completed}",
        )

        // The BODY ids are the immediate evidence. Reading the backend's
        // service-thread id at this instant would race a legal engine that runs
        // a queued body before making its first blocking backend call.
        val body1 = ledger.view().threadIds["one"]
        val body2 = ledger.view().threadIds["two"]
        assertTrue(body1 != null && body2 != null, "per-endpoint worker: a body id was not recorded")
        assertNotEquals(
            body1,
            body2,
            "per-endpoint worker: both endpoints ran their body on ONE thread id -- a shared " +
                "executor wearing the expected name is not a dedicated thread per endpoint",
        )

        // Only then, having awaited each backend's FIRST service-thread
        // observation, require each backend to converge on its own body's id.
        assertTrue(
            b1.awaitCondition("engine 1 makes a blocking call") { it.serviceThreadId != null } &&
                b2.awaitCondition("engine 2 makes a blocking call") { it.serviceThreadId != null },
            "per-endpoint worker: an engine never made a blocking backend call",
        )
        assertEquals(
            body1,
            b1.observed().serviceThreadId,
            "per-endpoint worker: engine 1's body and its blocking calls must share one thread",
        )
        assertEquals(
            body2,
            b2.observed().serviceThreadId,
            "per-endpoint worker: engine 2's body and its blocking calls must share one thread",
        )
    }

    // -- perform's exception contract ----------------------------------------

    @Test
    fun `a throwing body delivers its exception once and leaves the worker live`() = runTest {
        val (e, b) = newEngine()
        val ledger = ledger()
        val boom = IllegalStateException("boom")
        var caught: Throwable? = null

        val caller =
            backgroundScope.launch(Dispatchers.Default) {
                try {
                    e.perform<Unit> { ledger.completed("throwing") ; throw boom }
                    ledger.delivered("unreachable")
                } catch (t: Throwable) {
                    caught = t
                    ledger.cancelled("threw")
                }
            }
        // Await the RESOLUTION before joining: a `join()` placed ahead of any
        // bounded evidence would sit on the test thread until runTest's own
        // timeout and report an anonymous stall instead of this contract.
        assertTrue(
            ledger.await("the throwing body resolves") { it.cancelled.isNotEmpty() },
            "throwing perform: the caller never resolved -- the exception was never delivered",
        )
        caller.join()

        // kotlinx-coroutines performs stack-trace recovery across a suspension,
        // so what arrives may be a COPY carrying the original as its cause
        // rather than the same instance. Both forms are accepted; a different
        // type, a lost message or a lost cause chain is not.
        val delivered = caught
        assertTrue(
            delivered is IllegalStateException && delivered.message == "boom",
            "throwing perform: the caller received $delivered, not the body's IllegalStateException",
        )
        assertTrue(
            delivered === boom || delivered?.cause === boom,
            "throwing perform: the delivered exception is neither the body's own instance nor a " +
                "recovered copy carrying it as the cause (got $delivered, cause ${delivered?.cause})",
        )
        assertEquals(listOf("throwing"), ledger.view().completed, "throwing perform: body once")
        assertEquals(emptyList(), ledger.view().delivered, "throwing perform: nothing delivered")

        // The sole service thread must survive: one bad operation may not
        // strand every other waiter on this endpoint. The identity comes from
        // the THROWING BODY's own record -- reading the backend here would race
        // an engine that has not yet made its first blocking call.
        val workerId = ledger.view().threadIds["throwing"]
        assertTrue(workerId != null, "throwing perform: the body's thread id was not recorded")
        assertEquals(SubmitOutcome.ACCEPTED, e.post { ledger.completed("after") })
        assertTrue(
            ledger.await("a later job still runs") { it.completed.contains("after") },
            "throwing perform: a throwing body killed the service thread; the next job never ran",
        )
        assertEquals(
            workerId,
            ledger.view().threadIds["after"],
            "throwing perform: the following job must run on the SAME still-live worker",
        )
    }

    @Test
    fun `a body that throws after its caller was cancelled delivers nothing`() = runTest {
        val (e, _) = newEngine()
        val ledger = ledger()
        val hold = gate("throw-after-cancel")

        val caller =
            backgroundScope.launch(Dispatchers.Default) {
                try {
                    e.perform<Unit> {
                        ledger.started("late-throw")
                        hold.await()
                        ledger.completed("late-throw")
                        throw IllegalStateException("late")
                    }
                    ledger.delivered("late-throw")
                } catch (_: CancellationException) {
                    ledger.cancelled("late-throw")
                } catch (_: IllegalStateException) {
                    ledger.delivered("exception-after-cancellation")
                }
            }
        assertTrue(
            ledger.await("the body starts") { it.started.contains("late-throw") },
            "late-throw: the body never started, so the precedence is untested",
        )
        caller.cancel()
        hold.open()
        // A cancelled coroutine can finish BEFORE the already-claimed body
        // does, so joining the caller is not a body-completion barrier. Await
        // both facts explicitly.
        assertTrue(
            ledger.await("the body completes and the caller resolves") {
                it.completed.contains("late-throw") && it.cancelled.contains("late-throw")
            },
            "late-throw: the claimed body or the caller's resolution never arrived",
        )
        caller.join()

        val v = ledger.view()
        assertEquals(listOf("late-throw"), v.completed, "late-throw: the claimed body still finishes")
        assertEquals(
            listOf("late-throw"),
            v.cancelled,
            "late-throw: cancellation won, so the caller resolves as cancellation",
        )
        assertEquals(
            emptyList(),
            v.delivered,
            "late-throw: once cancellation wins, a later exception is not delivered either",
        )
    }

    // -- close publishes its wake while the backend is live ------------------

    @Test
    fun `close publishes its wake while the backend is still live`() = runTest {
        val (e, b) = newEngine()
        val ledger = ledger()
        val hold = gate("close-wake-hold")

        assertEquals(SubmitOutcome.ACCEPTED, e.post { ledger.started("held") ; hold.await() })
        assertTrue(
            ledger.await("the engine is busy") { it.started.contains("held") },
            "close-wake: the engine never claimed the holding job",
        )

        val closer = callBlocking("close-wake") { e.close() }
        hold.open()
        assertTrue(
            b.awaitCondition("the backend retires") { it.destroyCount == 1 },
            "close-wake: the backend was never retired",
        )
        closer.assertCompleted()

        // Controlled arm: nothing external terminalises this endpoint, so the
        // ONLY way a wake can be ignored here is that the engine published it
        // after its own stop() -- an engine lifecycle error, caught by cause.
        assertEquals(
            0,
            b.observed().postStopWakeCount,
            "close-wake: the engine published a wake after its own stop()",
        )
        assertEquals(
            emptyList(),
            b.observed().violations,
            "close-wake: the backend recorded a post-destroy call",
        )
    }
}

/**
 * The terminal-versus-accepted-command race.
 *
 * `moq_endpoint_wait()` can report CLOSED forever once the peer terminalises
 * the endpoint, and a backend wake in that state is a documented no-op. Neither
 * fact authorises losing an engine command that was already accepted.
 *
 * The row is deterministic without a production seam: the scripted backend can
 * hold a computed wait result inside the call, so the command is submitted
 * while the engine is provably between "the endpoint answered CLOSED" and "the
 * engine saw that answer".
 */
internal class EndpointEngineTerminalTest : EngineFixture() {

    @Test
    fun `a command accepted as the endpoint goes terminal still runs exactly once`() = runTest {
        val (e, b) = newEngine()
        val ledger = ledger()

        assertTrue(
            b.awaitCondition("the engine parks") { it.waitParked },
            "terminal race: the engine never parked in the backend wait",
        )

        b.holdNextWaitReturn()
        b.setClosed(true) // the peer terminalises the endpoint
        assertTrue(
            b.awaitCondition("the terminal result is held") { it.waitReturnHeld },
            "terminal race: the terminal wait result was never held",
        )

        assertEquals(
            SubmitOutcome.ACCEPTED,
            e.post { ledger.completed("command") },
            "terminal race: an engine command must still be accepted",
        )
        assertEquals(
            1,
            b.observed().externalTerminalWakeCount,
            "terminal race: the wake should be the documented terminal no-op",
        )

        b.releaseHeldWaitReturn()
        assertTrue(
            ledger.await("the accepted command runs") { it.completed.contains("command") },
            "terminal race: an accepted command was lost because the endpoint went terminal",
        )
        assertEquals(
            listOf("command"),
            ledger.view().completed,
            "terminal race: the command must run exactly once",
        )

        // And the engine must not hot-loop on a wait that returns CLOSED
        // immediately forever: it parks on its own condition instead.
        val waitsAfter = b.observed().waitEntries
        assertTrue(
            waitsAfter <= 8,
            "terminal race: the engine hot-looped the terminal wait ($waitsAfter entries)",
        )
    }
}

/**
 * Cancellation cause fidelity.
 *
 * Kotlin owns the resolution of a cancelled continuation. If the engine were to
 * attempt its own second resume with a freshly fabricated exception, the
 * caller's own cancellation cause would be at risk of being replaced -- so what
 * is pinned here is the identity the CALLER supplied, through the standard
 * stack-recovery cause chain.
 */
internal class EndpointEnginePerformCauseTest : EngineFixture() {

    @Test
    fun `cancelling a queued perform preserves the caller's cancellation cause`() = runTest {
        val (e, b) = newEngine()
        val ledger = ledger()
        val block = gate("cause-block")

        // Occupy the engine so the next job is provably still QUEUED.
        assertEquals(SubmitOutcome.ACCEPTED, e.post { ledger.started("block") ; block.await() })
        assertTrue(
            ledger.await("the engine is occupied") { it.started.contains("block") },
            "cancel cause: the engine never claimed the holding job",
        )

        var observed: Throwable? = null
        val marker = CancellationException("moq5-caller-supplied-cancellation")
        val queued =
            backgroundScope.launch(Dispatchers.Default) {
                try {
                    ledger.delivered(e.perform { ledger.completed("queued") ; "value" })
                } catch (t: CancellationException) {
                    observed = t
                    ledger.cancelled("queued")
                }
            }
        assertTrue(
            b.awaitCondition("the queued submission wakes") { it.wakeCount == 2 },
            "cancel cause: the queued submission was never accepted",
        )

        queued.cancel(marker)
        queued.join()
        block.open()

        val v = ledger.view()
        assertEquals(emptyList(), v.completed, "cancel cause: the body must never run")
        assertEquals(listOf("queued"), v.cancelled, "cancel cause: exactly one cancellation")
        assertEquals(emptyList(), v.delivered, "cancel cause: nothing may be delivered")

        val t = observed
        assertTrue(
            t === marker || t?.cause === marker,
            "cancel cause: the caller observed $t (cause ${t?.cause}), not its own " +
                "CancellationException '${marker.message}' nor a recovered copy of it",
        )
    }
}
