package org.openmoq.moq5.engine

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * [EndpointEngine.performCommitted], the commit-grade submission for a body
 * whose native call decides ownership. The three cancellation windows, each
 * pinned directly:
 *  * cancelled BEFORE submission: nothing is enqueued, no wake is published,
 *    the body never runs, the caller resolves as cancellation;
 *  * cancelled while QUEUED after an accepted submission: the body still runs
 *    exactly once and its value is delivered to the caller;
 *  * cancelled while IN FLIGHT: the body completes and its value -- or its
 *    exception -- is delivered to the caller.
 * Contrast [EndpointEngine.perform], whose claimed job's result a cancelled
 * caller drops.
 */
internal class EndpointEnginePerformCommittedTest : EngineFixture() {
    private companion object {
        val BOUND = 5.seconds
    }

    @Test
    fun `cancelled before submission -- nothing enqueued, no wake, the body never runs`() =
        runBlocking {
            val (engine, backend) = newEngine()
            engine.awaitConstructed()
            val wakes = backend.observed().wakeCount
            val ran = AtomicInteger()
            val job = async(Dispatchers.Default) { engine.performCommitted { ran.incrementAndGet() } }
            // Cancel through a scope that is already cancelled at the call.
            val cancelled = async(Dispatchers.Default) {
                val self = coroutineContext[kotlinx.coroutines.Job]!!
                self.cancel()
                engine.performCommitted { ran.incrementAndGet() }
            }
            assertFailsWith<CancellationException> { withTimeout(BOUND) { cancelled.await() } }
            assertEquals(1, withTimeout(BOUND) { job.await() }, "the ordinary caller's body ran once")
            assertEquals(1, ran.get(), "the cancelled caller's body never ran")
            assertEquals(wakes + 1, backend.observed().wakeCount, "one wake for the one accepted submission; none for the refused one")
            engine.close()
        }

    @Test
    fun `cancelled while queued after an accepted submission -- the body still runs once and its value is delivered`() =
        runBlocking {
            val (engine, backend) = newEngine()
            engine.awaitConstructed()
            val gate = CountDownLatch(1)
            val entered = CountDownLatch(1)
            // Occupy the worker so the next submission stays QUEUED.
            val blocker = async(Dispatchers.Default) { engine.perform { entered.countDown(); gate.await(5, TimeUnit.SECONDS) } }
            assertTrue(entered.await(5, TimeUnit.SECONDS))
            val ran = AtomicInteger()
            // A cancelled coroutine's own Deferred can never report a value: the delivery is proven through a side channel the caller fills.
            val delivered = CompletableDeferred<Result<String>>()
            val queued = launch(Dispatchers.Default) { delivered.complete(runCatching { engine.performCommitted { ran.incrementAndGet(); "value" } }) }
            assertTrue(backend.awaitCondition("submitted: the wake is published under the lock") { it.wakeCount >= 2 })
            queued.cancel()
            gate.countDown()
            assertEquals("value", withTimeout(BOUND) { delivered.await() }.getOrThrow(), "delivered despite the caller's cancellation while queued")
            assertEquals(1, ran.get())
            withTimeout(BOUND) { queued.join() }
            withTimeout(BOUND) { blocker.await() }
            engine.close()
        }

    @Test
    fun `cancelled while in flight -- the body completes and its value or exception is delivered`() =
        runBlocking {
            val (engine, _) = newEngine()
            engine.awaitConstructed()
            val gate = CountDownLatch(1)
            val entered = CountDownLatch(1)
            val ran = AtomicInteger()
            val delivered = CompletableDeferred<Result<Int>>()
            val inFlight = launch(Dispatchers.Default) { delivered.complete(runCatching { engine.performCommitted { entered.countDown(); gate.await(5, TimeUnit.SECONDS); ran.incrementAndGet() } }) }
            assertTrue(entered.await(5, TimeUnit.SECONDS))
            inFlight.cancel()
            gate.countDown()
            assertEquals(1, withTimeout(BOUND) { delivered.await() }.getOrThrow(), "the in-flight body's value is delivered, not a cancellation")
            withTimeout(BOUND) { inFlight.join() }
            val gate2 = CountDownLatch(1)
            val entered2 = CountDownLatch(1)
            val failed = CompletableDeferred<Result<Int>>()
            val failing = launch(Dispatchers.Default) { failed.complete(runCatching { engine.performCommitted<Int> { entered2.countDown(); gate2.await(5, TimeUnit.SECONDS); throw IllegalStateException("the body's own failure") } }) }
            assertTrue(entered2.await(5, TimeUnit.SECONDS))
            failing.cancel()
            gate2.countDown()
            assertEquals("the body's own failure", assertFailsWith<IllegalStateException> { withTimeout(BOUND) { failed.await() }.getOrThrow() }.message)
            withTimeout(BOUND) { failing.join() }
            engine.close()
        }

    @Test
    fun `after close the submission is refused with the engine's closed failure, and the body never runs`() =
        runBlocking {
            val (engine, _) = newEngine()
            engine.awaitConstructed()
            engine.close()
            val ran = AtomicInteger()
            assertFailsWith<EngineClosedException> { engine.performCommitted { ran.incrementAndGet() } }
            assertEquals(0, ran.get())
        }
}
