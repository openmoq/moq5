package org.openmoq.moq5.samples.publisher

import android.os.Handler
import android.os.HandlerThread

/**
 * The capture worker's [CallbackDispatcher]: `Handler.post` onto a looper this
 * object OWNS.
 *
 * It creates its own `HandlerThread` and takes no looper or handler from
 * anyone, so [quit] can only ever end that thread -- never the main looper or
 * one a caller still depends on. Constructing it starts nothing: the thread is
 * started by [startWorker], which is the Worker acquisition and returns the
 * handle whose close quits it. Before that, and once quit, [deliver] refuses
 * and delivers nothing.
 *
 * Wrapping a handler does not by itself make quitting safe: the ownership
 * decides WHEN the worker closes, retaining it while an acquisition is
 * outstanding or a transfer holds it.
 */
internal class HandlerDispatcher(name: String) : CallbackDispatcher, WorkerTimer {
    private val thread = HandlerThread(name)
    private val lock = Any()
    private var handler: Handler? = null
    private var quitting = false

    /** Starts the owned thread, once. Closing the returned handle is [quit]. */
    fun startWorker(): ResourceHandle {
        synchronized(lock) {
            check(!quitting) { "the ${thread.name} worker has already quit" }
            check(handler == null) { "the ${thread.name} worker has already started" }
            thread.start()
            handler = Handler(thread.looper)
        }
        return object : ResourceHandle {
            override val identity: Any = thread

            override fun close() = quit()
        }
    }

    /** The owned worker's handler, for `MediaCodec.setCallback`. Refused before start and once quit. */
    fun handler(): Handler =
        synchronized(lock) {
            check(!quitting) { "the ${thread.name} worker has quit" }
            checkNotNull(handler) { "the ${thread.name} worker has not started" }
        }

    override val isAlive: Boolean
        get() = synchronized(lock) { handler != null && !quitting } && thread.isAlive

    override fun deliver(callback: () -> Unit): Boolean {
        val target = synchronized(lock) { if (quitting) null else handler } ?: return false
        // `post` itself answers false once the looper is quitting.
        return target.post { callback() }
    }

    /** `postDelayed` onto the owned worker: a ticket that removes it, best effort; null once quit or when the post is refused. */
    override fun post(delayMillis: Long, action: () -> Unit): TimerTicket? {
        val target = synchronized(lock) { if (quitting) null else handler } ?: return null
        val runnable = Runnable { action() }
        if (!target.postDelayed(runnable, delayMillis)) return null
        return TimerTicket { target.removeCallbacks(runnable) }
    }

    /** `quitSafely()`: messages already due still run; later posts fail. Idempotent. */
    override fun quit() {
        val started =
            synchronized(lock) {
                if (quitting) return
                quitting = true
                handler != null
            }
        if (started) thread.quitSafely()
    }
}
