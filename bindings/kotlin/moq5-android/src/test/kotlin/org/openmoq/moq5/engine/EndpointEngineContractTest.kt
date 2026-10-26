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
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * The behavioral contract of the endpoint engine.
 *
 * These are the permanent behavioral pins for the engine: command authority,
 * FIFO exactly-once, wake accounting, the cancellation boundary and the
 * shutdown sequence. They were written RED-first against a skeleton that owned
 * no service thread, and every one of them failed then for absence of behavior
 * rather than a missing symbol or an unlucky timing window.
 *
 * Nothing here measures how long anything takes. Every cross-thread wait is a
 * condition handshake whose expiry is a named sticky failure, never a silent
 * release; every scheduling boundary is forced by an explicit handshake rather
 * than left to the scheduler.
 *
 * Suspending engine calls are launched on [Dispatchers.Default] via
 * `backgroundScope` and observed through ledgers; the test thread never awaits
 * a continuation that a missing service thread can never resume.
 */
internal class EndpointEngineContractTest : EngineFixture() {

    // -- 1. one command authority --------------------------------------------

    @Test
    fun `job bodies and blocking backend calls run on one engine-owned thread`() = runTest {
        val (e, b) = newEngine()
        val ledger = ledger()
        val callerId = Thread.currentThread().threadId()

        assertEquals(SubmitOutcome.ACCEPTED, e.post { ledger.completed("job") })

        assertTrue(
            ledger.await("the job body runs") { it.completed.isNotEmpty() },
            "command authority: the accepted job body never ran -- no engine-owned thread claimed it",
        )
        val v = ledger.view()
        assertEquals(listOf("job"), v.completed, "command authority: the body must run exactly once")
        assertNotEquals(
            callerId,
            v.threadIds["job"],
            "command authority: the body ran on the caller's own thread (id $callerId)",
        )
        assertTrue(
            b.awaitCondition("a blocking wait is made") { it.serviceThreadId != null },
            "command authority: no blocking backend wait was ever made from the engine thread",
        )
        assertEquals(
            v.threadIds["job"],
            b.observed().serviceThreadId,
            "command authority: job bodies and blocking backend calls must share ONE thread id",
        )
        assertEquals(
            emptyList(),
            b.observed().violations,
            "command authority: the backend recorded confinement or post-destroy violations",
        )
    }

    // -- 2. FIFO, exactly once ------------------------------------------------

    @Test
    fun `accepted jobs run in enqueue order exactly once`() = runTest {
        val (e, _) = newEngine()
        val ledger = ledger()
        val hold = gate("fifo-hold")

        // Occupy the engine FIRST. Acceptance means enqueued, so a/b/c are all
        // provably queued before any of them can be claimed -- otherwise a LIFO
        // claim survives simply by keeping up with the submitter.
        assertEquals(SubmitOutcome.ACCEPTED, e.post { ledger.started("hold") ; hold.await() })
        assertTrue(
            ledger.await("the engine is occupied") { it.started.contains("hold") },
            "FIFO: the engine never claimed the holding job, so ordering is untested",
        )

        listOf("a", "b", "c").forEach { label ->
            assertEquals(SubmitOutcome.ACCEPTED, e.post { ledger.completed(label) })
        }
        hold.open()

        assertTrue(
            ledger.await("three bodies run") { it.completed.size == 3 },
            "FIFO: expected 3 job bodies to run, saw ${ledger.view().completed}",
        )
        assertEquals(
            listOf("a", "b", "c"),
            ledger.view().completed,
            "FIFO: accepted jobs must run in enqueue order",
        )
    }

    // -- 3. wake on every accepted submission --------------------------------

    @Test
    fun `each accepted submission wakes the backend exactly once`() = runTest {
        val (e, b) = newEngine()
        val ledger = ledger()
        val hold = gate("wake-hold")

        assertEquals(0, b.observed().wakeCount, "wake: no wake may precede the first submission")
        assertEquals(SubmitOutcome.ACCEPTED, e.post { ledger.started("first") ; hold.await() })
        assertEquals(
            1,
            b.observed().wakeCount,
            "wake: an accepted submission must wake the backend exactly once",
        )
        assertTrue(
            ledger.await("the first job starts") { it.started.contains("first") },
            "wake: the first accepted job never started, so the engine never observed its wake",
        )

        // A wake landing while the engine is BUSY must stay level-retained.
        assertEquals(SubmitOutcome.ACCEPTED, e.post { ledger.completed("second") })
        assertEquals(
            2,
            b.observed().wakeCount,
            "wake: the second accepted submission must wake exactly once",
        )

        hold.open()
        assertTrue(
            ledger.await("the second job runs") { it.completed.contains("second") },
            "wake: the retained wake was lost -- the second job needed another external nudge",
        )
        assertEquals(
            2,
            b.observed().wakeCount,
            "wake: the second job must run on the RETAINED level, with no unaccounted extra wake",
        )
    }

    @Test
    fun `a refused submission does not wake the backend`() = runTest {
        val (e, b) = newEngine()
        val closer = callBlocking("wake-refusal-close") { e.close() }
        assertTrue(
            b.awaitCondition("the backend retires") { it.destroyCount == 1 },
            "wake: shutdown never reached the backend, so refusal cannot be observed",
        )
        closer.assertCompleted() // the close caller must return, not stay suspended

        val wakesBefore = b.observed().wakeCount
        assertEquals(
            SubmitOutcome.REFUSED_SHUTDOWN,
            e.post { /* must never run */ },
            "wake: a submission after shutdown must be refused",
        )
        assertEquals(
            wakesBefore,
            b.observed().wakeCount,
            "wake: a refused submission must not wake the backend",
        )
    }

    @Test
    fun `a job is enqueued before its wake is published`() = runTest {
        val (e, b) = newEngine()
        val ledger = ledger()

        // The barrier holds the submitter inside wake() until the single
        // consumer has taken the level. A wake published BEFORE its job was
        // enqueued is therefore consumed while the queue is still empty, and
        // the job is stranded with no further nudge to recover it.
        b.armWakeBarrier()
        backgroundScope.launch(Dispatchers.Default) { e.post { ledger.completed("ordered") } }

        assertTrue(
            b.awaitCondition("the wake is published") { it.wakeCount == 1 },
            "enqueue-before-wake: the submission never woke the backend",
        )
        assertTrue(
            ledger.await("the ordered job runs") { it.completed.contains("ordered") },
            "enqueue-before-wake: the wake was published before the job was enqueued, " +
                "so the consumer took the level with an empty queue and stranded the job",
        )
    }

    // -- 4. fire-and-forget ---------------------------------------------------

    @Test
    fun `fire and forget is accepted while running and refused once shutdown starts`() = runTest {
        val (e, b) = newEngine()
        val ledger = ledger()

        assertEquals(
            SubmitOutcome.ACCEPTED,
            e.post { ledger.completed("live") },
            "fire-and-forget: must be accepted while the engine is running",
        )
        assertTrue(
            ledger.await("the live job runs") { it.completed.contains("live") },
            "fire-and-forget: an accepted body never ran",
        )

        val closer = callBlocking("fire-and-forget-close") { e.close() }
        assertTrue(
            b.awaitCondition("the backend retires") { it.destroyCount == 1 },
            "fire-and-forget: shutdown never completed",
        )
        closer.assertCompleted()
        assertEquals(
            SubmitOutcome.REFUSED_SHUTDOWN,
            e.post { ledger.completed("late") },
            "fire-and-forget: submissions after shutdown must be refused deterministically",
        )
        assertEquals(
            listOf("live"),
            ledger.view().completed,
            "fire-and-forget: a refused body must never run",
        )
    }

    // -- 5. teardown-grade submission is cancellation-immune ------------------

    @Test
    fun `teardown grade work completes and delivers even if its waiter is cancelled`() = runTest {
        val (e, _) = newEngine()
        val ledger = ledger()
        val hold = gate("teardown-hold")

        lateinit var inner: Job
        val waiter =
            callBlocking("teardown-immune") {
                coroutineScope {
                    inner =
                        launch {
                            val outcome =
                                e.postAndWait {
                                    ledger.started("teardown")
                                    hold.await()
                                    ledger.completed("teardown")
                                }
                            assertEquals(SubmitOutcome.ACCEPTED, outcome)
                            ledger.delivered("teardown")
                        }
                }
            }

        assertTrue(
            ledger.await("the teardown job starts") { it.started.contains("teardown") },
            "cancellation-immune: the teardown job never started",
        )
        inner.cancel() // the caller goes away mid-flight
        hold.open()

        assertTrue(
            ledger.await("the teardown job completes and delivers") {
                it.completed.contains("teardown") && it.delivered.contains("teardown")
            },
            "cancellation-immune: cancelling the waiter abandoned teardown-grade work or its result",
        )
        waiter.assertCompleted() // the waiter must complete, not stay suspended
        val v = ledger.view()
        assertEquals(listOf("teardown"), v.completed, "cancellation-immune: body exactly once")
        assertEquals(listOf("teardown"), v.delivered, "cancellation-immune: delivery exactly once")
        assertEquals(emptyList(), v.cancelled, "cancellation-immune: must not resolve as cancelled")
    }

    @Test
    fun `teardown grade work runs for a caller that was cancelled before submitting`() = runTest {
        val (e, _) = newEngine()
        val ledger = ledger()

        val waiter =
            callBlocking("teardown-pre-cancelled") {
                coroutineScope {
                    launch {
                        coroutineContext[Job]!!.cancel() // cancelled BEFORE the call
                        val outcome = e.postAndWait { ledger.completed("pre-cancelled") }
                        assertEquals(SubmitOutcome.ACCEPTED, outcome)
                        ledger.delivered("pre-cancelled")
                    }
                }
            }

        assertTrue(
            ledger.await("the pre-cancelled teardown job runs and delivers") {
                it.completed.contains("pre-cancelled") && it.delivered.contains("pre-cancelled")
            },
            "cancellation-immune: a pre-cancelled caller must still run and receive teardown work",
        )
        waiter.assertCompleted()
    }

    // -- 6. perform: the cancellation boundary --------------------------------

    @Test
    fun `perform on an already cancelled coroutine enqueues nothing`() = runTest {
        val (e, b) = newEngine()
        val ledger = ledger()

        val waiter =
            backgroundScope.launch(Dispatchers.Default) {
                coroutineContext[Job]!!.cancel()
                try {
                    ledger.delivered(e.perform { ledger.completed("never") ; "value" })
                } catch (_: CancellationException) {
                    ledger.cancelled("never")
                }
            }
        waiter.join()

        val v = ledger.view()
        assertEquals(listOf("never"), v.cancelled, "pre-cancelled perform: exactly one cancellation")
        assertEquals(emptyList(), v.completed, "pre-cancelled perform: the body must not run")
        assertEquals(emptyList(), v.delivered, "pre-cancelled perform: nothing may be delivered")
        assertEquals(0, b.observed().wakeCount, "pre-cancelled perform: no enqueue, so no wake")
    }

    @Test
    fun `cancelling a queued job prevents the body and resolves once as cancellation`() = runTest {
        val (e, b) = newEngine()
        val ledger = ledger()
        val blockEngine = gate("queued-block")

        // Occupy the engine so the next job is provably still QUEUED.
        assertEquals(SubmitOutcome.ACCEPTED, e.post { ledger.started("block") ; blockEngine.await() })
        assertTrue(
            ledger.await("the engine is occupied") { it.started.contains("block") },
            "cancel-until-started: the engine never claimed the holding job",
        )

        val queued =
            backgroundScope.launch(Dispatchers.Default) {
                try {
                    ledger.delivered(e.perform { ledger.completed("queued") ; "value" })
                } catch (_: CancellationException) {
                    ledger.cancelled("queued")
                }
            }
        assertTrue(
            b.awaitCondition("the queued submission wakes") { it.wakeCount == 2 },
            "cancel-until-started: the queued submission must be accepted with exactly one " +
                "wake (saw ${b.observed().wakeCount} in total, expected 2)",
        )

        queued.cancel()
        queued.join()
        blockEngine.open()

        val v = ledger.view()
        assertEquals(
            emptyList(),
            v.completed,
            "cancel-until-started: the body of a cancelled queued job must never run",
        )
        assertEquals(listOf("queued"), v.cancelled, "cancel-until-started: resolve exactly once")
        assertEquals(emptyList(), v.delivered, "cancel-until-started: nothing may be delivered")
    }

    @Test
    fun `cancelling after the body starts completes it but resolves as cancellation`() = runTest {
        val (e, _) = newEngine()
        val ledger = ledger()
        val hold = gate("started-hold")

        val started =
            backgroundScope.launch(Dispatchers.Default) {
                try {
                    ledger.delivered(
                        e.perform {
                            ledger.started("started")
                            hold.await()
                            ledger.completed("started")
                            "value"
                        },
                    )
                } catch (_: CancellationException) {
                    ledger.cancelled("started")
                }
            }

        assertTrue(
            ledger.await("the job body starts") { it.started.contains("started") },
            "cancel-after-start: the job never started, so the boundary is untested",
        )
        started.cancel() // too late: the engine has claimed the job
        hold.open()

        assertTrue(
            ledger.await("the body completes and the caller resolves") {
                it.completed.contains("started") && (it.cancelled + it.delivered).isNotEmpty()
            },
            "cancel-after-start: a claimed job's body was abandoned by a late cancellation",
        )
        started.join()
        val v = ledger.view()
        assertEquals(listOf("started"), v.completed, "cancel-after-start: body exactly once")
        // The ruled outcome: Kotlin's prompt cancellation wins, so the caller
        // resolves as cancelled and NO value is delivered. The body is still
        // never abandoned. This is deliberately not an either/or assertion.
        assertEquals(
            listOf("started"),
            v.cancelled,
            "cancel-after-start: the caller must resolve exactly once as cancellation",
        )
        assertEquals(
            emptyList(),
            v.delivered,
            "cancel-after-start: a cancelled caller must not also receive a value",
        )
    }

    @Test
    fun `a job that completes before any cancellation delivers its value`() = runTest {
        val (e, _) = newEngine()
        val ledger = ledger()

        val done =
            backgroundScope.launch(Dispatchers.Default) {
                try {
                    ledger.delivered(e.perform { ledger.completed("won") ; "value" })
                } catch (_: CancellationException) {
                    ledger.cancelled("won")
                }
            }
        assertTrue(
            ledger.await("the value is delivered") { it.delivered.isNotEmpty() },
            "completion-wins: the result of an uncancelled perform was never delivered",
        )
        done.join()

        val v = ledger.view()
        assertEquals(listOf("won"), v.completed, "completion-wins: body exactly once")
        assertEquals(listOf("value"), v.delivered, "completion-wins: the VALUE must be delivered")
        assertEquals(emptyList(), v.cancelled, "completion-wins: no cancellation may be recorded")
    }

    // -- 7. shutdown boundary -------------------------------------------------

    @Test
    fun `shutdown drains accepted jobs in order then retires the backend exactly once`() = runTest {
        val (e, b) = newEngine()
        val ledger = ledger()
        val retiredAtStop = mutableListOf<Boolean>()
        b.onStop = { retiredAtStop += e.isBackendRetired }

        val hold = gate("shutdown-hold")
        assertEquals(SubmitOutcome.ACCEPTED, e.post { ledger.started("x") ; hold.await() ; ledger.completed("x") })
        assertTrue(
            ledger.await("the pre-shutdown job starts") { it.started.contains("x") },
            "shutdown: the pre-shutdown job never started",
        )
        assertEquals(SubmitOutcome.ACCEPTED, e.post { ledger.completed("y") })

        // The boundary is "close() has begun shutdown", not "the helper is about
        // to call close()". Opening a gate before the call leaves a preemption
        // window in which a correct engine would still accept the late post, so
        // the gate is opened only after an UNDISPATCHED launch of close() has
        // returned from its FIRST suspension -- by which point a correct engine
        // has already marked itself stopping. A mutant that suspends before
        // marking stays observable as an accepted late post.
        val entered = gate("close-entered")
        val closer =
            callBlocking("shutdown-close") {
                coroutineScope {
                    val child = launch(start = CoroutineStart.UNDISPATCHED) { e.close() }
                    entered.open()
                    child.join()
                }
            }
        assertTrue(entered.await(), "shutdown: close() never reached its first suspension")
        val secondClose = callBlocking("shutdown-close-2") { e.close() }

        assertEquals(
            SubmitOutcome.REFUSED_SHUTDOWN,
            e.post { ledger.completed("late") },
            "shutdown: new submissions must be refused once shutdown starts",
        )
        hold.open()

        assertTrue(
            b.awaitCondition("the backend retires") { it.destroyCount == 1 },
            "shutdown: the backend was never retired",
        )
        closer.assertCompleted()
        secondClose.assertCompleted()

        val snap = b.observed()
        assertEquals(
            listOf("x", "y"),
            ledger.view().completed,
            "shutdown: jobs accepted before shutdown must drain in FIFO order",
        )
        assertEquals(listOf("stop", "destroy"), snap.callOrder, "shutdown: stop must precede destroy")
        assertEquals(1, snap.stopCount, "shutdown: stop must happen exactly once")
        assertEquals(1, snap.destroyCount, "shutdown: destroy must happen exactly once")
        assertEquals(
            listOf(true),
            retiredAtStop,
            "shutdown: retirement must be latched BEFORE the first backend teardown call",
        )
        assertEquals(
            emptyList(),
            snap.violations,
            "shutdown: the backend recorded a post-destroy call or a confinement violation",
        )

        // A close arriving AFTER retirement is the deterministic idempotence
        // case: two concurrent closes can both land before teardown and prove
        // nothing. This one cannot touch the backend at all.
        e.close()
        val after = b.observed()
        assertEquals(1, after.stopCount, "shutdown: a late close must not stop the backend again")
        assertEquals(1, after.destroyCount, "shutdown: a late close must not destroy again")
        assertEquals(
            emptyList(),
            after.violations,
            "shutdown: a close after retirement must make no backend call at all",
        )
    }

    @Test
    fun `close wakes an engine parked in the backend wait`() = runTest {
        val (e, b) = newEngine()

        // Let the engine reach its idle park with nothing queued.
        assertTrue(
            b.awaitCondition("the engine parks") { it.waitParked },
            "close-while-parked: the engine never parked in the backend wait",
        )
        val expiriesBefore = b.observed().expiries

        val closer = callBlocking("parked-close") { e.close() }

        assertTrue(
            b.awaitCondition("the backend retires") { it.destroyCount == 1 },
            "close-while-parked: shutdown did not wake the parked engine",
        )
        closer.assertCompleted()
        assertEquals(
            expiriesBefore,
            b.observed().expiries,
            "close-while-parked: retirement must not depend on the defensive wait slice expiring",
        )
        assertTrue(
            b.observed().waitTimeouts.all { it > 0 },
            "close-while-parked: the engine must pass a POSITIVE defensive slice, not zero",
        )
    }

    @Test
    fun `close retires the backend for a caller cancelled before it is invoked`() = runTest {
        val (e, b) = newEngine()

        val closer =
            callBlocking("close-pre-cancelled") {
                coroutineScope {
                    launch {
                        coroutineContext[Job]!!.cancel()
                        e.close()
                    }
                }
            }
        assertTrue(
            b.awaitCondition("the backend retires") { it.destroyCount == 1 },
            "close: a pre-cancelled caller must still retire the endpoint",
        )
        closer.assertCompleted()
        val snap = b.observed()
        assertEquals(1, snap.stopCount, "close: stop exactly once")
        assertEquals(listOf("stop", "destroy"), snap.callOrder, "close: stop must precede destroy")
    }

    @Test
    fun `close retires the backend when its caller is cancelled mid flight`() = runTest {
        val (e, b) = newEngine()
        val ledger = ledger()
        val hold = gate("close-hold")

        assertEquals(SubmitOutcome.ACCEPTED, e.post { ledger.started("held") ; hold.await() })
        assertTrue(
            ledger.await("the held job starts") { it.started.contains("held") },
            "close: the engine never claimed the holding job",
        )

        lateinit var innerClose: Job
        val closer =
            callBlocking("close-mid-flight") {
                coroutineScope { innerClose = launch { e.close() } }
            }
        assertTrue(
            b.awaitCondition("close reaches the backend") { it.wakeCount >= 2 },
            "close: the close caller never reached the engine",
        )
        innerClose.cancel() // the caller goes away while the drain is in flight
        hold.open()

        assertTrue(
            b.awaitCondition("the backend retires") { it.destroyCount == 1 },
            "close: a cancelled caller must not leave the endpoint un-retired",
        )
        closer.assertCompleted() // a cancelled close caller must still COMPLETE
        val snap = b.observed()
        assertEquals(1, snap.stopCount, "close: stop exactly once")
        assertEquals(1, snap.destroyCount, "close: destroy exactly once")
        assertFalse(
            snap.violations.isNotEmpty(),
            "close: teardown after a cancelled caller recorded violations: ${snap.violations}",
        )
    }
}
