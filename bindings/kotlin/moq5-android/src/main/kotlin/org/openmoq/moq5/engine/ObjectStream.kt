package org.openmoq.moq5.engine

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.openmoq.moq5.ExactStream
import org.openmoq.moq5.MediaObject
import org.openmoq.moq5.ReceiverFatalException
import org.openmoq.moq5.ReceiverInterruptedException
import org.openmoq.moq5.ReceiverTerminal

/**
 * The pull-only object stream over one receiver.
 *
 * Each `next()` is one demand: the handoff slot first, then ONE demand on
 * the service worker, which drains track events first and stages any object
 * it dequeues into the slot. An empty answer is RETAINED by the worker: it
 * re-polls on its own later passes (genuine activity, a drain's
 * observation) and resumes the consumer when an object, an interrupt or
 * this queue's own CLOSED can be delivered. The consumer never resubmits
 * while it waits, so waiting costs no wake. Nothing here runs on a pass by
 * itself.
 */
internal class ObjectStream(
    private val engine: EndpointEngine,
    private val token: Long,
    private val model: ReceiverModel,
) : ExactStream<MediaObject> {
    override suspend fun next(): MediaObject? {
        check(model.claimObjectConsumer()) { "the object stream has one consumer at a time; a next() is already in progress" }
        try {
            while (true) {
                model.takeHandoff()?.let { return it }
                // The terminal is the OBJECT queue's: the engine answers Closed
                // only once that ring answered CLOSED (empty and terminal) or
                // the receiver was retired. A terminal another queue reported
                // never ends this stream while objects are still owed.
                val demand =
                    try {
                        engine.demandObject(token, model)
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
                    is ObjectDemand.Interrupted -> throw ReceiverInterruptedException()
                    is ObjectDemand.Poll -> error("a poll marker never leaves the engine")
                }
            }
        } finally {
            model.releaseObjectConsumer()
        }
    }

    /** The engine is gone: the receiver was retired with it; deliver what is staged, then the recorded end. */
    private fun afterEngineClosed(): MediaObject? {
        model.takeHandoff()?.let { return it }
        return when (val t = model.recordedTerminal) {
            is ReceiverTerminal.Fatal -> throw ReceiverFatalException(t.code)
            else -> null
        }
    }

    override fun asFlow(): Flow<MediaObject> =
        flow {
            while (true) emit(next() ?: break)
        }
}

/** What one demand on the worker did. */
internal sealed interface ObjectDemand {
    /** An element is in the handoff slot (dequeued now, or left by a cancelled demand). */
    data object Delivered : ObjectDemand

    data object Interrupted : ObjectDemand

    /** THIS native queue answered CLOSED (empty and terminal), or the receiver was retired. */
    data object Closed : ObjectDemand

    /** The prelude found nothing to answer without a poll. Never leaves the engine. */
    data object Poll : ObjectDemand
}
