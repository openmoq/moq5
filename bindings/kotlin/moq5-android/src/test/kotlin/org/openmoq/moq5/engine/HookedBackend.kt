package org.openmoq.moq5.engine

import kotlinx.coroutines.CoroutineDispatcher
import org.openmoq.moq5.ReceiverConfig
import org.openmoq.moq5.SendTrackConfig
import org.openmoq.moq5.SenderConfig
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.coroutines.CoroutineContext

/**
 * A test-side delegating backend: every call goes to the scripted backend it
 * wraps, and a row may install a hook that runs on the calling (service)
 * thread immediately BEFORE or AFTER the delegated call of one operation.
 *
 * The hooks run outside the scripted backend's own lock and outside its
 * reentrancy tripwire, so a hook may park on a [Gate]: that is how a row
 * holds the worker at "claimed, before the native mutation" or at "the
 * native call succeeded, before its result returns" without a production
 * seam. The wrapped scripted backend stays the object the fixture audits.
 */
internal class HookedBackend(private val inner: ScriptedEndpointBackend) : EndpointBackend by inner {
    var beforeAttachReceiver: (() -> Unit)? = null
    var afterAttachReceiver: (() -> Unit)? = null
    var beforeAttachSender: (() -> Unit)? = null
    var afterAttachSender: (() -> Unit)? = null
    var beforeSenderAddTrack: (() -> Unit)? = null
    var afterSenderAddTrack: (() -> Unit)? = null
    var beforeSenderConvertToVod: (() -> Unit)? = null
    var afterSenderConvertToVod: (() -> Unit)? = null
    var beforeReceiverStats: (() -> Unit)? = null
    var beforeSenderStats: (() -> Unit)? = null

    /** Runs with the ACTUAL answer of one object poll, after the scripted backend decided it and before it returns to the engine. */
    var afterPollObject: ((BackendObjectResult) -> Unit)? = null

    override fun pollObject(token: Long): BackendObjectResult {
        val r = inner.pollObject(token)
        afterPollObject?.invoke(r)
        return r
    }

    override fun attachReceiver(config: ReceiverConfig): BackendAttachResult {
        beforeAttachReceiver?.invoke()
        val r = inner.attachReceiver(config)
        afterAttachReceiver?.invoke()
        return r
    }

    override fun attachSender(config: SenderConfig): BackendAttachResult {
        beforeAttachSender?.invoke()
        val r = inner.attachSender(config)
        afterAttachSender?.invoke()
        return r
    }

    override fun senderAddTrack(token: Long, config: SendTrackConfig): BackendSenderTrackResult {
        beforeSenderAddTrack?.invoke()
        val r = inner.senderAddTrack(token, config)
        afterSenderAddTrack?.invoke()
        return r
    }

    override fun senderConvertToVod(token: Long, items: List<Pair<Long, ULong>>): BackendCommandResult {
        beforeSenderConvertToVod?.invoke()
        val r = inner.senderConvertToVod(token, items)
        afterSenderConvertToVod?.invoke()
        return r
    }

    override fun receiverStats(token: Long): BackendStatsResult {
        beforeReceiverStats?.invoke()
        return inner.receiverStats(token)
    }

    override fun senderStats(token: Long): BackendSenderStatsResult {
        beforeSenderStats?.invoke()
        return inner.senderStats(token)
    }
}

/**
 * A dispatcher the test drives by hand: every dispatched block is queued and
 * runs only when something calls [runOne] / [runAll] -- a row's own steps run
 * on the test thread; the fixture's retirement runs them on its owned,
 * bounded driver thread. It makes the window between "the worker delivered a
 * result to a suspended caller" and "the caller's coroutine actually resumed"
 * a controlled, observable state rather than a race, with no sleep and no
 * timing assumption.
 */
internal class ManualDispatcher : CoroutineDispatcher() {
    private val lock = ReentrantLock()
    private val changed = lock.newCondition()
    private val queue = ArrayDeque<Runnable>()
    private var dispatched = 0

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        lock.withLock {
            queue.addLast(block)
            dispatched++
            changed.signalAll()
        }
    }

    /** Block until at least [count] blocks have ever been dispatched here; false when the ceiling expires first. */
    fun awaitDispatched(count: Int, ceilingMillis: Long = ScriptedEndpointBackend.CEILING_MILLIS): Boolean {
        val deadline = System.nanoTime() + ceilingMillis * 1_000_000
        lock.withLock {
            while (dispatched < count) {
                val remaining = deadline - System.nanoTime()
                if (remaining <= 0) return false
                changed.awaitNanos(remaining)
            }
            return true
        }
    }

    /** Test-only synchronization point: invoked under the lock, once, immediately before [awaitWork] parks on an empty queue. */
    var onAwaitWorkParked: (() -> Unit)? = null

    /**
     * Block until work is QUEUED or [deadlineNanos] passes. The predicate is
     * the actual queue under the dispatcher's own lock -- the same lock
     * [dispatch] signals under -- so an item dispatched before this wait
     * began is seen immediately and can never be missed by a count taken
     * earlier. Returns false only when the deadline passed with an empty queue.
     */
    fun awaitWork(deadlineNanos: Long): Boolean {
        lock.withLock {
            var parked = false
            while (queue.isEmpty()) {
                val remaining = deadlineNanos - System.nanoTime()
                if (remaining <= 0) return false
                if (!parked) {
                    parked = true
                    onAwaitWorkParked?.invoke()
                }
                changed.awaitNanos(remaining)
            }
            return true
        }
    }

    /** Run the oldest queued block on the calling thread; false when nothing is queued. */
    fun runOne(): Boolean {
        val block = lock.withLock { queue.removeFirstOrNull() } ?: return false
        block.run()
        return true
    }

    fun runAll() {
        while (runOne()) {
            // drain
        }
    }

    fun queued(): Int = lock.withLock { queue.size }

    /** How many blocks were ever dispatched here. */
    fun dispatchedCount(): Int = lock.withLock { dispatched }
}
