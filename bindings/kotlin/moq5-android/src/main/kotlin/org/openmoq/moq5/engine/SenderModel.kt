package org.openmoq.moq5.engine

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.openmoq.moq5.SendTrack
import org.openmoq.moq5.SenderStatus
import org.openmoq.moq5.SenderTerminal
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * The durable state behind one sender: the sender-owned track handles keyed
 * by their native token, the status the worker publishes from each pass's
 * probe, and the completion latch. Mutation runs on the endpoint's service
 * worker; readers take the lock.
 */
internal class SenderModel {
    private val lock = ReentrantLock()

    /* lock-guarded */
    private val byToken = HashMap<Long, SendTrack>()
    private var completed = false
    private var retired = false

    private val flow = MutableStateFlow(SenderStatus(ready = false, terminal = null, mediaDemand = false))

    val status: StateFlow<SenderStatus> = flow

    /** This sender's handle for a native token, or null: a foreign or unknown handle never reaches the bridge. */
    fun resolve(token: Long): SendTrack? = lock.withLock { byToken[token] }

    /** Worker only: the handle `add_track` returned. */
    fun add(track: SendTrack) = lock.withLock { byToken[track.token] = track }

    val isCompleted: Boolean
        get() = lock.withLock { completed }

    fun markCompleted() = lock.withLock { completed = true }

    /** Worker only: the status one probe answered. After a local close the terminal stays [SenderTerminal.Closed]. */
    fun publish(status: SenderStatus) =
        lock.withLock {
            if (!retired) flow.value = status
        }

    /** The sender was closed by the application or the endpoint's teardown: a terminal already reported stands, else Closed. */
    fun retire() =
        lock.withLock {
            retired = true
            val current = flow.value
            if (current.terminal == null) flow.value = SenderStatus(ready = false, terminal = SenderTerminal.Closed, mediaDemand = false)
        }
}
