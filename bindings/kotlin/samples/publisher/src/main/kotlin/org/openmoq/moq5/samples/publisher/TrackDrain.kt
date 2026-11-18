package org.openmoq.moq5.samples.publisher

import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.ensureActive
import kotlin.time.Duration
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.WriteOutcome

/**
 * One live track's drain: from the capture handoff, through the accepted
 * publish pipeline, to the sender.
 *
 * It is the consumer a LIVE track needs and the finite synthetic demonstration
 * never did. `null` from a live source means "nothing right now", not "none
 * left": only the drained terminal ends a track. The wake is an
 * empty-to-non-empty EDGE, so the signal is installed BEFORE the queue is ever
 * looked at and the queue is drained to empty before suspending again; a wake
 * that arrives while draining is kept, and one wake may stand for any number of
 * outputs.
 *
 * What it does NOT do: it never sleeps, polls or paces; it owns no codec, no
 * ownership and no sender; and it invents no receipt. A write that ended in an
 * exception, an `Error` or cancellation settles this drain's own admission and
 * says nothing about what the transport did with an operation already claimed.
 *
 * Its progress on a silent terminal is not guaranteed by [OutputSignal] alone:
 * an observer that throws can keep the handoff from raising the wake, so the
 * coordinator that owns this drain still needs its own failure route.
 */
internal interface TrackSink {
    /** One `MediaSender.write` for this track. */
    suspend fun write(obj: SendObject, timeout: Duration): WriteOutcome

    /** One `MediaSender.endTrack` for this track. */
    suspend fun endTrack(timeout: Duration): WriteOutcome
}

/** How one track's drain finished. */
internal sealed interface TrackDrain {
    /** The source's own terminal was drained AND `endTrack` was accepted. */
    data class Ended(val written: Int, val refused: Int, val skipped: Int) : TrackDrain

    /**
     * Anything else that is still an outcome: a source that ended without a
     * terminal, or a terminal whose `endTrack` was refused. Cancellation is
     * NOT one of these -- it propagates as itself.
     */
    data class Unfinished(val reason: String, val written: Int, val refused: Int, val skipped: Int) : TrackDrain
}

/**
 * Drains [source] into [sink] until the track ends.
 *
 * [pipeline] is this track's own: one admitted write at a time, and every
 * payload, presentation time and capture instant comes from it, never from
 * when this loop happened to run.
 */
internal suspend fun drainTrack(
    kind: MediaKind,
    source: CaptureHandoff,
    pipeline: PublishPipeline,
    sink: TrackSink,
    writeTimeout: Duration,
    report: (String) -> Unit = {},
): TrackDrain {
    // ONE token, kept until it is used: a wake raised while this drain is
    // working is not lost, and several outputs behind one wake cost one token.
    val wakes = Channel<Unit>(Channel.CONFLATED)
    // REGISTERED BEFORE THE QUEUE IS EVER LOOKED AT. The handoff signals the
    // empty-to-non-empty edge and replays nothing, so a consumer that looks
    // first can suspend holding outputs it will never be woken for.
    source.signal { wakes.trySend(Unit) }
    var written = 0
    var refused = 0
    var skipped = 0
    try {
        while (true) {
            var terminal = false
            // Drain to EMPTY before suspending again.
            while (!terminal) {
                // Cooperative, and BEFORE any further work is taken on. A
                // suspend call need not actually suspend, so a drain whose
                // outputs are all available could otherwise run to the end of
                // a cancelled track. A write that already RESOLVED is settled
                // before this point; what stops here is the next one.
                coroutineContext.ensureActive()
                val item = source.next() ?: break
                when (val step = pipeline.step(item.output, item.captureElapsedNanos)) {
                    is PublishStep.Configure -> report("$kind codec configuration: ${step.bytes.size} byte(s)")
                    // Nothing was admitted for it; the track ends here, without
                    // a terminal, through the coordinator's own settlement.
                    is PublishStep.Refused -> return TrackDrain.Unfinished(step.reason, written, refused, skipped)
                    is PublishStep.Skipped -> {
                        skipped++
                        report("$kind skipped an output: ${step.reason}")
                    }
                    // The empty terminal.
                    PublishStep.EndTrack -> terminal = true
                    is PublishStep.Write -> {
                        when (writeOne(pipeline, sink, step.obj, writeTimeout)) {
                            WriteOutcome.Accepted -> written++
                            WriteOutcome.WouldBlock -> refused++
                        }
                        // A final payload is BOTH media and the terminal: it is
                        // written and settled first, and nothing follows it.
                        if (step.endsStream) terminal = true
                    }
                }
            }
            if (terminal) {
                // The terminal is a new operation on the sender: a cancelled
                // drain does not start one. This says nothing about an
                // operation the library may already have claimed.
                coroutineContext.ensureActive()
                check(pipeline.mayEndTrack()) { "$kind: a write is still admitted; ending the track would publish after it" }
                val end = sink.endTrack(writeTimeout)
                return if (end == WriteOutcome.Accepted) {
                    TrackDrain.Ended(written, refused, skipped)
                } else {
                    TrackDrain.Unfinished("$kind: endTrack was refused ($end)", written, refused, skipped)
                }
            }
            // The track is over and never produced one: a failed close or a
            // codec fault ended it. That is not a clean end of media, and this
            // drain does not invent an end-of-stream for it.
            if (source.ended) {
                return TrackDrain.Unfinished("the $kind track ended without a terminal", written, refused, skipped)
            }
            wakes.receive()
        }
    } finally {
        // Nothing is retained through the callback afterwards: a late signal
        // finds a closed channel, and `trySend` on it does nothing.
        wakes.close()
    }
}

/**
 * One write, settled EXACTLY once.
 *
 * A resolved `Accepted` settles the admission as published and a resolved
 * `WouldBlock` abandons it; both are answers the library gave. An exception,
 * an `Error` or a cancellation is not: the local admission is released so the
 * track is not left with a write in flight, and nothing here claims the
 * transport accepted or discarded an operation it may already have taken.
 * Shared by the live drain and the finite demonstration's loop.
 */
internal suspend fun writeOne(
    pipeline: PublishPipeline,
    sink: TrackSink,
    obj: SendObject,
    timeout: Duration,
): WriteOutcome {
    val pending = PendingWrite(obj)
    var resolved: WriteOutcome? = null
    try {
        val outcome = sink.write(pending.next(), timeout)
        resolved = outcome
        if (outcome == WriteOutcome.Accepted) {
            pending.onAccepted()
            pipeline.accepted(obj)
        } else {
            pipeline.abandoned(obj)
        }
        return outcome
    } finally {
        if (resolved == null) pipeline.abandoned(obj)
    }
}
