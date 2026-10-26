package org.openmoq.moq5.engine

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.openmoq.moq5.ExactStream
import org.openmoq.moq5.MediaTimelineRecord
import org.openmoq.moq5.MediaTrack
import org.openmoq.moq5.ReceiverFatalException
import org.openmoq.moq5.ReceiverRecordEvent
import org.openmoq.moq5.ReceiverTerminal
import org.openmoq.moq5.SapRecord
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * A pull-only record stream over one receiver, one per family, with the
 * same demand mechanics as [ObjectStream]: the handoff slot first, then ONE
 * demand on the worker (the track drain before its poll, and again after it
 * when the record's track is not yet in the inventory); an empty answer is
 * RETAINED by the worker, which re-polls on its own later passes and resumes
 * the consumer when something can be delivered, so waiting costs no wake.
 * The family decides which backend poll runs, which native queue it
 * exhausts, and how a backend record becomes the public value.
 *
 * The deferral witness: every poll answer carries the family's counter as
 * the bridge row read it after the poll. On an EMPTY answer whose counter
 * exceeds what this stream has witnessed, the demand yields ONE
 * [ReceiverRecordEvent.Deferral] and the stream records the counter; a
 * record answer never yields a witness (the admitted prefix is exact), and
 * an unchanged counter never repeats one.
 */
internal class RecordStream<R : Any, T : Any>(
    private val engine: EndpointEngine,
    private val token: Long,
    private val model: ReceiverModel,
    private val family: Family<R, T>,
) : ExactStream<ReceiverRecordEvent<T>> {
    internal class Family<R : Any, T : Any>(
        val name: String,
        val trackOp: String,
        val queue: ReceiverModel.NativeQueue,
        val poll: (EndpointBackend, Long) -> BackendRecordResult<R>,
        val trackOf: (R) -> Long,
        val toPublic: (R, MediaTrack) -> T,
    )

    private val lock = ReentrantLock()

    /* lock-guarded */
    private var consumer = false
    private var handoff: ReceiverRecordEvent<T>? = null
    private var witnessed = 0uL

    /** Worker only: what the demand produced, kept for the (possibly cancelled) consumer. */
    internal fun stage(e: ReceiverRecordEvent<T>) = lock.withLock {
        check(handoff == null) { "the ${family.name} handoff slot already holds an element" }
        handoff = e
    }

    internal fun hasHandoff(): Boolean = lock.withLock { handoff != null }

    private fun takeHandoff(): ReceiverRecordEvent<T>? = lock.withLock { handoff.also { handoff = null } }

    /** Worker only: decide the witness for an empty answer carrying [counter]. */
    internal fun witnessIfAdvanced(counter: ULong): ReceiverRecordEvent.Deferral? =
        lock.withLock {
            if (counter > witnessed) {
                val previous = witnessed
                witnessed = counter
                ReceiverRecordEvent.Deferral(counter, previous)
            } else {
                null
            }
        }

    override suspend fun next(): ReceiverRecordEvent<T>? {
        check(lock.withLock { if (consumer) false else { consumer = true; true } }) { "the ${family.name} stream has one consumer at a time; a next() is already in progress" }
        try {
            while (true) {
                takeHandoff()?.let { return it }
                // The terminal is this FAMILY's: the engine answers Closed only
                // once this queue has answered CLOSED (empty and terminal) or
                // the receiver was retired -- a terminal another queue reported
                // never ends this stream while its own ring still owes records.
                val demand =
                    try {
                        engine.demandRecord(token, model, this, family)
                    } catch (_: EngineClosedException) {
                        return afterEngineClosed()
                    }
                when (demand) {
                    is ObjectDemand.Delivered -> continue
                    is ObjectDemand.Closed ->
                        return when (val t = model.recordedTerminal) {
                            is ReceiverTerminal.Fatal -> throw ReceiverFatalException(t.code)
                            else -> null
                        }
                    is ObjectDemand.Interrupted -> error("record polls do not report the latch")
                    is ObjectDemand.Poll -> error("a poll marker never leaves the engine")
                }
            }
        } finally {
            lock.withLock { consumer = false }
        }
    }

    private fun afterEngineClosed(): ReceiverRecordEvent<T>? {
        takeHandoff()?.let { return it }
        return when (val t = model.recordedTerminal) {
            is ReceiverTerminal.Fatal -> throw ReceiverFatalException(t.code)
            else -> null
        }
    }

    override fun asFlow(): Flow<ReceiverRecordEvent<T>> =
        flow {
            while (true) emit(next() ?: break)
        }

    internal companion object {
        val SAP: Family<BackendSapRecord, SapRecord> =
            Family(
                name = "sap-record",
                trackOp = "receiver-sap-track",
                queue = ReceiverModel.NativeQueue.Sap,
                poll = { b, token -> b.pollSap(token) },
                trackOf = { it.track },
                toPublic = { r, track -> SapRecord(track, r.location, r.sapType, r.earliestPresentationTime) },
            )

        val TIMELINE: Family<BackendTimelineRecord, MediaTimelineRecord> =
            Family(
                name = "media-timeline",
                trackOp = "receiver-timeline-track",
                queue = ReceiverModel.NativeQueue.Timeline,
                poll = { b, token -> b.pollMediaTimeline(token) },
                trackOf = { it.track },
                toPublic = { r, track -> MediaTimelineRecord(track, r.mediaTime, r.location, r.wallclock) },
            )
    }
}
