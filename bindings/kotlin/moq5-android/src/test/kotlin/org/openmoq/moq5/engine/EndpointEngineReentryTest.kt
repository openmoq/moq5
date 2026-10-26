package org.openmoq.moq5.engine

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.Continuation
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.startCoroutine
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Reentry from inside an engine body.
 *
 * The sole worker runs a body synchronously. A suspending engine call made from
 * that body waits for the very worker that is inside it, so it deadlocks
 * permanently. The public architecture keeps application callbacks off the
 * worker, but an internal implementation mistake is still possible and must
 * fail at the point of misuse rather than wedging the endpoint forever.
 *
 * `post` is deliberately NOT rejected: it is non-blocking FIFO tail work.
 */
internal class EndpointEngineReentryTest : EngineFixture() {

    private fun assertWorkerStillLive(
        e: EndpointEngine,
        ledger: EngineLedger,
        outerLabel: String,
        what: String,
    ) {
        assertEquals(
            SubmitOutcome.ACCEPTED,
            e.post { ledger.completed("after-$what") },
            "$what: the engine stopped accepting work",
        )
        assertTrue(
            ledger.await("a later command runs") { it.completed.contains("after-$what") },
            "$what: the worker did not survive the refused reentry",
        )
        assertEquals(
            ledger.view().threadIds[outerLabel],
            ledger.view().threadIds["after-$what"],
            "$what: the later command ran on a different thread -- the worker was replaced",
        )
    }

    @Test
    fun `a suspending perform from inside a body is refused, not deadlocked`() = runTest {
        val (e, _) = newEngine()
        val ledger = ledger()
        val observed = AtomicReference<String?>(null)
        val finished = gate("reentry-perform")

        assertEquals(
            SubmitOutcome.ACCEPTED,
            e.post {
                ledger.started("outer-perform")
                try {
                    runBlocking { e.perform { ledger.completed("nested-perform") } }
                } catch (t: Throwable) {
                    observed.set("${t::class.simpleName}: ${t.message}")
                }
                ledger.completed("outer-perform")
                finished.open()
            },
        )

        assertTrue(
            finished.await(),
            "reentry perform: the outer body never finished -- the nested call wedged the worker",
        )
        assertEquals(
            emptyList(),
            ledger.view().completed.filter { it == "nested-perform" },
            "reentry perform: the nested body must not run",
        )
        assertTrue(
            observed.get()?.startsWith("EngineReentryException") == true,
            "reentry perform: expected the engine's reentry refusal, saw ${observed.get()}",
        )
        assertWorkerStillLive(e, ledger, "outer-perform", "reentry perform")
    }

    @Test
    fun `a suspending postAndWait from inside a body is refused, not deadlocked`() = runTest {
        val (e, _) = newEngine()
        val ledger = ledger()
        val observed = AtomicReference<String?>(null)
        val finished = gate("reentry-postAndWait")

        assertEquals(
            SubmitOutcome.ACCEPTED,
            e.post {
                ledger.started("outer-paw")
                try {
                    runBlocking { e.postAndWait { ledger.completed("nested-paw") } }
                } catch (t: Throwable) {
                    observed.set("${t::class.simpleName}: ${t.message}")
                }
                ledger.completed("outer-paw")
                finished.open()
            },
        )

        assertTrue(
            finished.await(),
            "reentry postAndWait: the outer body never finished -- the nested call wedged the worker",
        )
        assertEquals(
            emptyList(),
            ledger.view().completed.filter { it == "nested-paw" },
            "reentry postAndWait: the nested body must not run",
        )
        assertTrue(
            observed.get()?.startsWith("EngineReentryException") == true,
            "reentry postAndWait: expected the engine's reentry refusal, saw ${observed.get()}",
        )
        assertWorkerStillLive(e, ledger, "outer-paw", "reentry postAndWait")
    }

    @Test
    fun `a suspending close from inside a body is refused, not deadlocked`() = runTest {
        val (e, _) = newEngine()
        val ledger = ledger()
        val observed = AtomicReference<String?>(null)
        val finished = gate("reentry-close")

        assertEquals(
            SubmitOutcome.ACCEPTED,
            e.post {
                ledger.started("outer-close")
                try {
                    runBlocking { e.close() }
                } catch (t: Throwable) {
                    observed.set("${t::class.simpleName}: ${t.message}")
                }
                ledger.completed("outer-close")
                finished.open()
            },
        )

        assertTrue(
            finished.await(),
            "reentry close: the outer body never finished -- the nested call wedged the worker",
        )
        assertTrue(
            observed.get()?.startsWith("EngineReentryException") == true,
            "reentry close: expected the engine's reentry refusal, saw ${observed.get()}",
        )
        assertWorkerStillLive(e, ledger, "outer-close", "reentry close")
    }

    @Test
    fun `post from inside a body is ordinary FIFO tail work and is not refused`() = runTest {
        val (e, _) = newEngine()
        val ledger = ledger()
        val outcome = AtomicReference<SubmitOutcome?>(null)

        assertEquals(
            SubmitOutcome.ACCEPTED,
            e.post {
                ledger.started("outer-post")
                outcome.set(e.post { ledger.completed("tail") })
                ledger.completed("outer-post")
            },
        )
        assertTrue(
            ledger.await("the tail job runs") { it.completed.contains("tail") },
            "reentry post: a post from a body must be accepted and run as FIFO tail work",
        )
        assertEquals(SubmitOutcome.ACCEPTED, outcome.get(), "reentry post: must not be refused")
        assertEquals(
            listOf("outer-post", "tail"),
            ledger.view().completed,
            "reentry post: the tail job must run AFTER the body that queued it",
        )
    }

    @Test
    fun `a continuation resumed inline on the worker may still call the engine`() {
        // The guard must bracket BODY EXECUTION, not the later result delivery.
        //
        // The caller is a bare `suspend () -> Unit` started with
        // `startCoroutine` against a completion `Continuation` whose context is
        // `EmptyCoroutineContext`: with no dispatcher in the context there is
        // nothing to dispatch through, so the resume performed by the worker
        // while delivering the first result continues the caller INLINE on that
        // worker. No dispatcher, no `runTest`, no delay, no sleep and no
        // thread-name inference is involved -- the row asserts the exact
        // retained worker identity instead.
        val (e, b) = newEngine()
        val ledger = ledger()
        val problem = AtomicReference<String?>(null)
        val completions = AtomicInteger(0)
        val done = gate("inline-delivery")

        assertTrue(
            b.awaitCondition("the worker is observed") { it.serviceThread != null },
            "inline delivery: the engine never made a blocking backend call",
        )
        val worker = b.observed().serviceThread
        val workerId = b.observed().serviceThreadId

        // Occupy the worker FIRST, so the caller's first `perform` is provably
        // queued and suspended before any body can run. Without that, the
        // worker can finish the job before `suspendCancellableCoroutine`'s
        // block returns, the continuation is resumed on the fast path, and the
        // caller continues on its OWN thread -- which proves nothing here.
        val hold = gate("inline-hold")
        assertEquals(
            SubmitOutcome.ACCEPTED,
            e.post { ledger.started("hold") ; hold.await() },
            "inline delivery: the holding job was refused",
        )
        assertTrue(
            ledger.await("the worker is occupied") { it.started.contains("hold") },
            "inline delivery: the worker never claimed the holding job",
        )

        val caller: suspend () -> Unit = {
            ledger.delivered(e.perform { ledger.completed("first") ; "one" })
            // Records the thread the continuation resumed on.
            ledger.started("continuation")
            ledger.delivered(e.perform { ledger.completed("second") ; "two" })
        }
        caller.startCoroutine(
            Continuation(EmptyCoroutineContext) { result ->
                completions.incrementAndGet()
                result.exceptionOrNull()?.let { problem.set("${it::class.simpleName}: ${it.message}") }
                done.open()
            },
        )

        hold.open() // now the worker runs the first body and delivers inline
        assertTrue(done.await(), "inline delivery: the caller never completed")
        val v = ledger.view()

        // Preconditions: the first body ran on the retained exact worker, and
        // the code immediately after the first `perform` resumed on it.
        assertEquals(
            workerId,
            v.threadIds["first"],
            "inline delivery: the first body did not run on the retained worker",
        )
        assertEquals(
            worker?.threadId(),
            v.threadIds["continuation"],
            "inline delivery: the continuation did not resume inline on the worker, so this row " +
                "does not exercise the body-execution bracket",
        )

        // The contract: delivery is not body execution, so the second call is
        // accepted and runs on the same worker.
        assertEquals(
            null,
            problem.get(),
            "inline delivery: a call made from a continuation resumed on the worker was refused " +
                "(${problem.get()}); the guard must bracket body execution only",
        )
        assertEquals(1, completions.get(), "inline delivery: the caller must complete exactly once")
        assertEquals(
            workerId,
            v.threadIds["second"],
            "inline delivery: the second body must run on the same worker",
        )
        assertEquals(
            listOf("first", "second"),
            v.completed,
            "inline delivery: each body runs exactly once, in order",
        )
        assertEquals(listOf("one", "two"), v.delivered, "inline delivery: exact delivery order")
    }
}
