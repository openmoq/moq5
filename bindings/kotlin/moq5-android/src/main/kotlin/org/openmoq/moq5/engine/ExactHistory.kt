package org.openmoq.moq5.engine

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.suspendCancellableCoroutine
import org.openmoq.moq5.ExactStream
import org.openmoq.moq5.ReceiverFatalException
import org.openmoq.moq5.ReceiverTerminal
import org.openmoq.moq5.TrackEvent
import org.openmoq.moq5.TrackEventHistoryOverflowException
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.coroutines.resume

/**
 * A bounded exact history with one consumer.
 *
 * The producer (the service worker) never blocks and never loses an event
 * silently. The stream is EXACT or it is FAILED: every event it keeps is
 * delivered in order, and the first event it cannot keep -- the first gap --
 * latches this stream's failure permanently. After the gap nothing is
 * admitted again, whatever space later frees: the pre-gap prefix is
 * delivered, then the stream fails with [TrackEventHistoryOverflowException]
 * carrying the count of events it did not deliver because of the gap (the
 * one that overflowed and every arrival after it), and every later `next()`
 * or fresh `asFlow()` collection fails the same way. The count saturates at
 * `ULong.MAX_VALUE`; it never wraps and never clears the latch. A terminal
 * waits its turn behind the kept prefix and never outranks the failure. The
 * failure is this stream's alone; the producer keeps integrating.
 *
 * One consumer at a time, for the WHOLE of an active `next()`: from entry
 * until it returns, fails or is cancelled -- including the interval in which
 * its resumption is dispatched but not yet run -- a second caller is refused,
 * even when a value is queued. The reservation is released on every exit
 * path; a cancellation callback releases only the continuation it belongs
 * to, never a newer caller's; and a cancelled caller never consumes a
 * buffered value, because values are taken only at the top of the loop.
 */
internal class ExactHistory(private val capacity: Int) : ExactStream<TrackEvent> {
    private val lock = ReentrantLock()

    /* lock-guarded */
    private val kept = ArrayDeque<TrackEvent>()

    /** Latched at the first gap; never clears. */
    private var failed = false

    /** Events not delivered because of the gap, saturating. */
    private var dropped = 0uL
    private var terminal: ReceiverTerminal? = null

    /** The one active `next()`, held for its whole duration. */
    private var consumer = false
    private var waiter: CancellableContinuation<Unit>? = null

    fun offer(event: TrackEvent) {
        val w =
            lock.withLock {
                if (failed || kept.size >= capacity) {
                    failed = true
                    dropped = saturatingIncrement(dropped)
                } else {
                    kept.addLast(event)
                }
                waiter.also { waiter = null }
            }
        w?.resume(Unit)
    }

    fun close(t: ReceiverTerminal) {
        val w =
            lock.withLock {
                if (terminal == null) terminal = t
                waiter.also { waiter = null }
            }
        w?.resume(Unit)
    }

    override suspend fun next(): TrackEvent? {
        lock.withLock {
            check(!consumer) { "an exact stream has one consumer at a time; a next() is already in progress" }
            consumer = true
        }
        try {
            while (true) {
                lock.withLock {
                    takeLocked()?.let { return it.value }
                }
                suspendCancellableCoroutine<Unit> { cont ->
                    val ready =
                        lock.withLock {
                            if (readyLocked()) {
                                true
                            } else {
                                waiter = cont
                                false
                            }
                        }
                    if (ready) {
                        cont.resume(Unit)
                    } else {
                        cont.invokeOnCancellation { lock.withLock { if (waiter === cont) waiter = null } }
                    }
                }
            }
        } finally {
            lock.withLock { consumer = false }
        }
    }

    override fun asFlow(): Flow<TrackEvent> =
        flow {
            while (true) emit(next() ?: break)
        }

    private class Taken(val value: TrackEvent?)

    private fun readyLocked(): Boolean = kept.isNotEmpty() || failed || terminal != null

    /** Caller holds the lock. The kept prefix first; then the latched failure, or the terminal; null when nothing is ready. */
    private fun takeLocked(): Taken? {
        if (kept.isNotEmpty()) return Taken(kept.removeFirst())
        if (failed) throw TrackEventHistoryOverflowException(capacity, dropped)
        return when (val t = terminal) {
            null -> null
            is ReceiverTerminal.Closed -> Taken(null)
            is ReceiverTerminal.Fatal -> throw ReceiverFatalException(t.code)
        }
    }

    internal companion object {
        /** The dropped-count arithmetic: saturates at `ULong.MAX_VALUE` instead of wrapping to zero. */
        fun saturatingIncrement(count: ULong): ULong = if (count == ULong.MAX_VALUE) count else count + 1uL
    }
}
