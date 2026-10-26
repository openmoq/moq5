package org.openmoq.moq5.engine

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.openmoq.moq5.EventHistoryPolicy
import org.openmoq.moq5.MediaObject
import org.openmoq.moq5.MediaTrack
import org.openmoq.moq5.ReceiverInventory
import org.openmoq.moq5.ReceiverTerminal
import org.openmoq.moq5.TrackEvent
import org.openmoq.moq5.TrackPresence
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * The durable state behind one receiver: the track identities keyed by
 * their native handle token, the published inventory, and the optional
 * exact history.
 *
 * Integration runs on the endpoint's service worker, one event at a time,
 * and every integrated change publishes a fresh [ReceiverInventory] whose
 * revision advanced by one -- the `StateFlow` therefore emits even when the
 * track list's contents are unchanged (an in-place update, an end). Native
 * handles are never recycled, so a token seen twice as ADDED, or a change
 * for a token never announced, is a contract anomaly: it is counted and
 * dropped rather than folded into an invented or duplicated track.
 */
internal class ReceiverModel(policy: EventHistoryPolicy) {
    private val lock = ReentrantLock()

    /* lock-guarded */
    private val byToken = HashMap<Long, MediaTrack>()
    private val order = ArrayList<MediaTrack>()
    private var catalogReady = false
    private var terminal: ReceiverTerminal? = null
    private var anomalies = 0
    private var revision = 0uL

    /* The C's shape, kept apart: ONE terminal reason for the receiver, but
     * exhaustion per native queue -- each poll answers CLOSED only when its
     * own ring is empty and the receiver is terminal -- and, separately,
     * explicit retirement (the child destroyed), after which nothing native
     * is polled again. */
    private var retired = false
    private var trackExhausted = false
    private val exhausted = HashSet<NativeQueue>()

    private val flow = MutableStateFlow(ReceiverInventory(emptyList(), catalogReady = false, terminal = null, anomalies = 0, revision = 0uL))

    val inventory: StateFlow<ReceiverInventory> = flow

    /** The exact history, or null when the policy is disabled. */
    val history: ExactHistory? =
        when (policy) {
            is EventHistoryPolicy.Disabled -> null
            is EventHistoryPolicy.Exact -> ExactHistory(policy.capacity)
        }

    /* The object stream's state: one consumer, and the ONE-object handoff
     * slot that keeps a demand cancelled after its poll began from losing
     * the object the worker already dequeued. */
    private var objectConsumer = false
    private var handoff: MediaObject? = null

    /** The receiver's native data queues, each exhausted on its own. */
    enum class NativeQueue { Objects, Sap, Timeline }

    /** Whether the receiver still has a terminal to reach (false once one is recorded). */
    val isLive: Boolean
        get() = lock.withLock { terminal == null }

    /** True once the child was destroyed: no native queue is polled again. */
    val isRetired: Boolean
        get() = lock.withLock { retired }

    /** True once the track-event ring answered CLOSED: its final tail is integrated and the history is closed. */
    val isTrackExhausted: Boolean
        get() = lock.withLock { trackExhausted }

    fun isExhausted(queue: NativeQueue): Boolean = lock.withLock { retired || queue in exhausted }

    /** Worker only: [queue] answered CLOSED -- empty and terminal -- so it is never polled again. */
    fun markExhausted(queue: NativeQueue) = lock.withLock { exhausted += queue }

    /** The recorded terminal, if any. */
    val recordedTerminal: ReceiverTerminal?
        get() = lock.withLock { terminal }

    /** The inventory's track for a native token, or null while its ADDED has not been drained. */
    fun resolve(token: Long): MediaTrack? = lock.withLock { byToken[token] }

    /* The binding's own record of which tracks the application wants: one
     * backend command per transition, none for a repeat. */
    private val desired = HashSet<Long>()

    fun isDesired(token: Long): Boolean = lock.withLock { token in desired }

    fun setDesired(token: Long, wanted: Boolean) = lock.withLock { if (wanted) desired += token else desired -= token }

    fun claimObjectConsumer(): Boolean = lock.withLock { if (objectConsumer) false else { objectConsumer = true; true } }

    fun releaseObjectConsumer() = lock.withLock { objectConsumer = false }

    fun hasHandoff(): Boolean = lock.withLock { handoff != null }

    /** Worker only: park a dequeued object for the consumer. The slot holds one; a second stage is an engine defect. */
    fun stageHandoff(o: MediaObject) = lock.withLock {
        check(handoff == null) { "the object handoff slot already holds an object" }
        handoff = o
    }

    fun takeHandoff(): MediaObject? = lock.withLock { handoff.also { handoff = null } }

    /** Fold one native event in. Service worker only. A recorded terminal does not discard the track ring's remaining tail; only retirement ends integration. */
    fun integrate(event: BackendTrackEvent) {
        val public: TrackEvent? =
            lock.withLock {
                if (retired) return
                val known = byToken[event.track]
                val e: TrackEvent? =
                    when (event.kind) {
                        BackendTrackEventKind.Added -> {
                            val d = event.description
                            if (known != null || d == null) {
                                anomalies++
                                null
                            } else {
                                val t = MediaTrack(event.track, d)
                                byToken[event.track] = t
                                order += t
                                TrackEvent.Added(t)
                            }
                        }
                        BackendTrackEventKind.Updated -> {
                            val d = event.description
                            if (known == null || d == null) {
                                anomalies++
                                null
                            } else {
                                known.description = d
                                TrackEvent.Updated(known)
                            }
                        }
                        BackendTrackEventKind.Removed ->
                            if (known == null) {
                                anomalies++
                                null
                            } else {
                                known.presence = TrackPresence.Removed
                                TrackEvent.Removed(known)
                            }
                        BackendTrackEventKind.Ended ->
                            if (known == null) {
                                anomalies++
                                null
                            } else {
                                known.presence = TrackPresence.Ended
                                TrackEvent.Ended(known)
                            }
                        BackendTrackEventKind.CatalogReady -> {
                            catalogReady = true
                            TrackEvent.CatalogReady
                        }
                        // Folded into the track's diagnostics; never a public event.
                        BackendTrackEventKind.ParseDrop -> {
                            if (known == null) anomalies++ else known.parseDrops = event.parseDropsTotal
                            null
                        }
                        // The acknowledgement of an application-initiated update
                        // belongs to the subscription surface; drained, not staged.
                        BackendTrackEventKind.UpdateOk -> {
                            if (known == null) anomalies++
                            null
                        }
                    }
                publishLocked()
                e
            }
        if (public != null) history?.offer(public)
    }

    /**
     * The receiver's ONE terminal reason, recorded once from whichever poll
     * reported it first. Service worker only. It does not close the history:
     * the track ring may still hold its final tail, and the history closes
     * only when that ring is exhausted ([trackRingExhausted]) or the receiver
     * is retired.
     */
    fun terminal(t: ReceiverTerminal) {
        lock.withLock {
            if (terminal != null) return
            terminal = t
            publishLocked()
        }
    }

    /** Worker only: the track ring answered CLOSED. Its tail is integrated; the history ends with the recorded terminal. */
    fun trackRingExhausted() {
        val recorded =
            lock.withLock {
                if (trackExhausted) return
                trackExhausted = true
                terminal ?: return
            }
        history?.close(recorded)
    }

    /**
     * The receiver was destroyed: nothing native is polled again, the
     * inventory freezes with a clean terminal (a fatal one already recorded
     * stands), every track retires and the history ends.
     */
    fun retire() {
        val recorded =
            lock.withLock {
                if (retired) return
                retired = true
                trackExhausted = true
                exhausted += NativeQueue.entries
                for (t in order) t.isRetired = true
                if (terminal == null) {
                    terminal = ReceiverTerminal.Closed
                    publishLocked()
                }
                terminal!!
            }
        history?.close(recorded)
    }

    private fun publishLocked() {
        revision += 1uL
        flow.value = ReceiverInventory(order.toList(), catalogReady, terminal, anomalies, revision)
    }
}
