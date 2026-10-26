package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CancellationException

/**
 * The adapter from a real encoder's output callbacks to the already accepted
 * [EncodedOutputSource] seam.
 *
 * The platform encoder's asynchronous callback is
 * `onOutputBufferAvailable(codec, index, BufferInfo)`. What it carries is an
 * INDEX and the four `BufferInfo` facts -- `offset`, `size`,
 * `presentationTimeUs`, `flags`. The bytes are NOT among them: they are
 * fetched afterwards with `getOutputBuffer(index)`, which returns a
 * `ByteBuffer` whose position/limit bracket the valid data, may be direct or
 * read-only, returns `null` for an index the codec does not own, and throws
 * `IllegalStateException`/`CodecException`. That fetch therefore happens INSIDE
 * this boundary, after the generation check and before the release, so a
 * failing fetch still settles the index it was given.
 *
 * What the seam guarantees:
 *
 *  * exactly `[offset, offset + size)` of the fetched buffer is copied, through
 *    a duplicate view -- never `array()`, never by moving the borrowed view;
 *  * the index is released exactly once on every path on which it was fetched
 *    or admitted: a refusal, a malformed range, a full queue, a missing
 *    observation, a null or throwing fetch. A release that THREW is not
 *    retried -- it may have taken effect;
 *  * after close, and for any other generation, a callback is IGNORED: nothing
 *    is fetched from a codec this session no longer owns, nothing is released;
 *  * the queue is bounded; overflow is a named refusal AND a fault to the owner;
 *  * a codec-configuration buffer and an empty end-of-stream buffer claim no
 *    observation and carry `0L`, which the accepted `PublishPipeline.step`
 *    never reads for those two;
 *  * a media output whose presentation timestamp is not strictly greater than
 *    the last admitted one on this track is REORDERED: refused, released, and
 *    reported. This sample's encoder contract is no reordering, and a decode
 *    time it does not carry cannot be guessed.
 *
 * **Availability.** The frozen caller reads `next() == null` as "no outputs
 * left". A LIVE track is empty most of the time, so this seam separates the two
 * facts: `next()` returns `null` whenever the queue is empty, [ended] says
 * whether the track has delivered its terminal, and [OutputSignal] is raised --
 * outside the monitor -- whenever the queue becomes non-empty or the track ends.
 * Nothing polls, and one empty track never blocks the other.
 *
 * **Termination and witness.** At most ONE terminal fault reaches the owner
 * per handoff, through [CaptureFaultSink.onFault]; a later fault on a track
 * that has already failed is not delivered. The DISCARD WITNESS is a different
 * fact and travels on its own method, [CaptureFaultSink.onDiscarded], exactly
 * once, when a failed close has SETTLED -- every admitted output is either
 * visible, drained, or counted there.
 *
 * **The exclusion rule GREEN must implement.** One monitor per handoff. An
 * output is handled in three phases:
 *
 *  1. ADMISSION, under the monitor: generation and terminal state are checked,
 *     the timestamp order is checked, a queue slot is RESERVED. Reservation
 *     counts against capacity from here on.
 *  2. TRANSFER, outside the monitor: take the codec LEASE, fetch, validate,
 *     copy, claim the observation, release the index, release the lease.
 *     Platform calls never run under the monitor, and the codec cannot be
 *     disposed while a lease is held ([CodecLifetime]).
 *  3. COMMIT, under the monitor: the reservation becomes a visible output --
 *     unless close arrived in between, in which case the copied output is
 *     disposed and counted into the discard witness.
 *
 * Close takes the monitor, marks the handoff closed, and returns; it never waits
 * for a transfer in flight, so no close can block on a callback and no callback
 * can block on close. The discard witness is delivered when the last reservation
 * settles. Owner notification is always made OUTSIDE the monitor, so a fault
 * that makes the session close both tracks cannot form a lock cycle.
 *
 * **What close returning does NOT mean.** A returned close does not mean the
 * codec is idle: a transfer admitted before it may still be between
 * `getOutputBuffer` and `releaseOutputBuffer`. Disposing the codec there would
 * free memory a live transfer is reading and would leave an index that can no
 * longer be given back. Close is therefore separated from DISPOSAL: the owner
 * may close whenever it likes, but the codec and the dispatcher its callbacks
 * arrive on are disposed only after the last admitted transfer has released its
 * index -- see [CodecLifetime]. Disposal happens outside every lock, exactly
 * once, and every path out of a transfer (refusal, exception, cancellation)
 * releases its lease.
 *
 * [HandoffSchedule] names the three boundaries -- before admission, before the
 * index release, and before commit -- so every legal ordering against close,
 * including close arriving while the codec is in use, can be forced by a test.
 */
internal class CodecOutput(
    /** The codec-owned output index this callback named. */
    val index: Int,
    val offset: Int,
    val size: Int,
    val presentationTimeUs: Long,
    val flags: Int,
)

/**
 * Access to the codec-owned buffers, as `MediaCodec` exposes them. Called only
 * for an admitted output of the live generation, and at most once per index.
 */
internal fun interface OutputBufferAccess {
    /** `getOutputBuffer(index)`: `null` when not owned; may throw. */
    fun outputBuffer(index: Int): ByteBuffer?
}

/** Giving one output index back to the codec. Called at most once per index; may throw. */
internal fun interface CodecBufferOwner {
    fun release(index: Int)
}

/** Raised, outside the monitor, when a track's queue becomes non-empty or the track ends. */
internal fun interface OutputSignal {
    fun onOutputs(kind: MediaKind)
}

/**
 * The codec's lifetime, as a transfer sees it.
 *
 * A reserved queue slot says the handoff may keep an output; it says NOTHING
 * about the codec still existing. TRANSFER calls into codec-owned memory --
 * `getOutputBuffer`, the copy, `releaseOutputBuffer` -- so between admission
 * and the index release the codec itself must stay alive, and so must the
 * dispatcher its callbacks arrive on.
 *
 * Every admitted transfer therefore takes a LEASE before it touches a buffer
 * and releases it after the index is released, in a `finally`, on every path
 * including an exception and a cancellation. The owner's close is never
 * blocked by a lease: it marks its intent, closes everything a transfer does
 * not hold, and the LAST lease released performs the deferred disposal --
 * outside every lock, exactly once, reported through the same late-cleanup
 * seam a retained worker uses.
 *
 * [retainForTransfer] returns false when the codec is already gone; a handoff
 * that is refused a lease must not touch a buffer or release an index.
 */
internal interface CodecLifetime {
    /** Takes a transfer lease. False when the codec has already been disposed. */
    fun retainForTransfer(): Boolean

    /** Releases a lease taken by [retainForTransfer]; the last one disposes a deferred codec. */
    fun releaseAfterTransfer()

    companion object {
        /** A codec nothing can dispose: every lease is granted and releasing does nothing. */
        val RETAINED: CodecLifetime =
            object : CodecLifetime {
                override fun retainForTransfer(): Boolean = true

                override fun releaseAfterTransfer() = Unit
            }
    }
}

/**
 * The two boundaries a test may hold a callback at. Both are OUTSIDE the
 * monitor; the defaults do nothing.
 */
internal interface HandoffSchedule {
    /** Before admission: nothing has been checked, reserved, fetched or released. */
    fun beforeAdmission() {}

    /**
     * Before the index is released: the buffer has been fetched and copied, the
     * codec is still being used, and the lease is still held. This is the only
     * boundary that can hold a transfer INSIDE the codec's lifetime.
     */
    fun beforeRelease() {}

    /** Before commit: the output is copied and its index released; nothing is visible yet. */
    fun beforeCommit() {}

    /** Before a terminal decision: nothing has been decided or reported. */
    fun beforeTerminal() {}

    companion object {
        val NONE: HandoffSchedule = object : HandoffSchedule {}
    }
}

/** Why a track can no longer publish, reported to its owner at most once. */
internal sealed interface CaptureFault {
    data class Overflow(val kind: MediaKind, val reason: String) : CaptureFault

    data class Malformed(val kind: MediaKind, val reason: String) : CaptureFault

    data class Correlation(val kind: MediaKind, val reason: String) : CaptureFault

    /** Output presentation time did not advance; this sample's encoder contract is no reordering. */
    data class Reordered(val kind: MediaKind, val reason: String) : CaptureFault

    /** `getOutputBuffer` returned `null` or threw, or a release threw. */
    data class BufferAccess(val kind: MediaKind, val reason: String) : CaptureFault

    /** The platform encoder raised `MediaCodec.CodecException`. */
    data class Codec(val kind: MediaKind, val reason: String) : CaptureFault

    /**
     * The device a track draws from -- its camera or its microphone -- was
     * lost AFTER its acquisition settled: an installed camera's
     * `onDisconnected`/`onError`. Never a second acquisition result.
     */
    data class Source(val kind: MediaKind, val reason: String) : CaptureFault

    /**
     * A track's encoder output format stopped agreeing with the format its
     * track was validated with: a later format that differs, a later typed
     * refusal, or an observation that overlapped one still being handled.
     * [reason] names which. The held format stays; the track is never
     * restarted with the new one.
     */
    data class Format(val kind: MediaKind, val reason: String) : CaptureFault

    /**
     * An admitted callback -- a format observation or a codec callback --
     * raised something that is not a typed verdict. [cause] is that exact
     * Throwable, retained as itself; it is also one of the ending's causes in
     * [RetirementReceipt.causes]. Only an accepted callback release
     * ([FormatCallbackGate.releaseFormat], [CodecCallbackGate.releaseCodec])
     * produces this; the general fault route refuses it.
     */
    data class Unexpected(val kind: MediaKind, val cause: Throwable) : CaptureFault
}

/**
 * Where a track's faults and witnesses go. A returned [HandoffOutcome] reaches
 * the platform callback and nobody else; this is the callback-driven route into
 * the session that owns the track. Both methods are invoked outside the
 * handoff's monitor.
 */
internal interface CaptureFaultSink {
    /** The first terminal fault of this track; delivered at most once per handoff. */
    fun onFault(generation: Long, fault: CaptureFault)

    /**
     * A failed close has settled and threw away this many admitted outputs.
     * Delivered exactly once per handoff that discards anything, after the last
     * in-flight transfer has settled.
     */
    fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int)
}

/** What one encoder callback did. */
internal sealed interface HandoffOutcome {
    data object Queued : HandoffOutcome

    /** Understood and refused, by name. The index is still released. */
    data class Refused(val reason: String) : HandoffOutcome

    /**
     * The callback belongs to a retired generation, or arrived after close or
     * after a fault. Nothing is fetched, nothing is queued, NOTHING is released.
     */
    data class Ignored(val reason: String) : HandoffOutcome
}

/** How a track ends, which decides what happens to output already accepted. */
internal enum class CloseMode {
    /** End-of-stream reached. Output already copied stays drainable. */
    Completed,

    /** Failure or cancellation. Queued output is discarded and witnessed once. */
    Failed,
}

/**
 * One track's encoder handoff.
 *
 * Implementations are judged by which indices were fetched and released and in
 * what order, by the exact bytes copied, by which observation each output
 * claimed, and by what a late callback does -- never by how many callbacks were
 * seen.
 */
internal interface CaptureHandoff : EncodedOutputSource {
    val kind: MediaKind

    /** The generation this handoff speaks for; any other is ignored. */
    val generation: Long

    /** True once the terminal has been delivered through [next], or the handoff closed. */
    val ended: Boolean

    /** Registers the consumer's availability signal. One consumer per track. */
    fun signal(sink: OutputSignal)

    /** One encoder output callback. */
    fun onOutput(generation: Long, output: CodecOutput): HandoffOutcome

    /** `MediaCodec.Callback.onError`: terminal, reported once, releases nothing. */
    fun onCodecError(generation: Long, reason: String)

    /**
     * The codec or session is gone: later callbacks are ignored and release
     * nothing. Returns without waiting for a transfer in flight, and so does
     * NOT authorize disposing the codec -- see [CodecLifetime].
     */
    fun close(mode: CloseMode)
}

/**
 * The handoff this sample runs: a bounded queue in front of the accepted output
 * seam, correlating each output against its track's ledger.
 *
 * The three phases are the ones this file declares. ADMISSION decides under a
 * short monitor and reserves a slot -- capacity counts reservations, not just
 * queued outputs. TRANSFER runs outside the monitor under a codec LEASE:
 * fetch, validate, copy, claim, release the index. COMMIT takes the monitor
 * again and either publishes the copy or, if close arrived meanwhile, counts it
 * into the discard witness.
 *
 * The reservation of an admitted callback is settled EXACTLY ONCE, by whichever
 * of the refusal, the commit or the unwind gets there first, so a cleanup that
 * throws after the index has gone back cannot consume capacity for good. Every
 * granted lease is released in a `finally`, on every path. No platform call,
 * release, cleanup or owner notification happens under the monitor, and close
 * never waits for a callback in flight.
 */
internal class BoundedCaptureHandoff(
    override val kind: MediaKind,
    override val generation: Long,
    val buffers: OutputBufferAccess,
    val owner: CodecBufferOwner,
    val ledger: CaptureLedger,
    val faults: CaptureFaultSink,
    val capacity: Int,
    /** The largest output this handoff will copy, in bytes: the declared bound on one queued or reserved payload. */
    val maxOutputBytes: Int,
    val lifetime: CodecLifetime = CodecLifetime.RETAINED,
    val schedule: HandoffSchedule = HandoffSchedule.NONE,
) : CaptureHandoff {
    private val lock = Any()
    private val queue = ArrayDeque<CapturedOutput>()
    private var reserved = 0
    private var closed = false
    private var closeMode: CloseMode? = null
    private var terminal: CaptureFault? = null
    private var lastMediaPts: Long? = null
    private var discarded = 0
    private var witnessed = false
    private var sink: OutputSignal? = null
    private var endedFlag = false

    init {
        require(maxOutputBytes > 0) { "the output byte limit is positive, not $maxOutputBytes" }
    }

    override val ended: Boolean get() = synchronized(lock) { endedFlag }

    override fun signal(sink: OutputSignal) {
        synchronized(lock) { this.sink = sink }
    }

    override fun onOutput(generation: Long, output: CodecOutput): HandoffOutcome {
        schedule.beforeAdmission()
        val media = output.flags and CaptureContract.FLAG_CODEC_CONFIG == 0 && output.size > 0
        var refusal: CaptureFault? = null
        synchronized(lock) {
            // ADMISSION. Nothing here touches the codec or the owner.
            if (generation != this.generation) return HandoffOutcome.Ignored("not generation ${this.generation}")
            if (closed || terminal != null) return HandoffOutcome.Ignored("this track has ended")
            val last = lastMediaPts
            if (media && last != null && output.presentationTimeUs <= last) {
                refusal = CaptureFault.Reordered(kind, "pts ${output.presentationTimeUs} does not advance past $last")
            } else if (queue.size + reserved >= capacity) {
                refusal = CaptureFault.Overflow(kind, "the $kind queue holds $capacity outputs")
            } else if (output.size > maxOutputBytes) {
                // Before any fetch, copy, claim or reservation: an output this
                // large is never read, whatever it carries.
                refusal = CaptureFault.Malformed(kind, "the $kind output of ${output.size} bytes exceeds the limit of $maxOutputBytes bytes")
            } else {
                reserved++
                if (media) lastMediaPts = output.presentationTimeUs
            }
        }
        // A refusal decided at admission still hands an index back, and that IS
        // a codec access: it takes a lease like any other.
        refusal?.let { return refuseAtAdmission(output.index, it) }

        val slot = AtomicBoolean(false)
        var primary: Throwable? = null
        try {
            val copied =
                when (val result = transfer(output, media, slot)) {
                    is Transferred.Rejected -> return result.outcome
                    is Transferred.Copied -> result
                }
            schedule.beforeCommit()

            // COMMIT.
            val captured = CapturedOutput(EncodedOutput(copied.bytes, 0, output.size, output.presentationTimeUs, output.flags), copied.captureNanos)
            var witness = 0
            var signalNow = false
            synchronized(lock) {
                if (slot.compareAndSet(false, true)) {
                    reserved--
                    if (closed) {
                        discarded++
                        witness = witnessIfSettledLocked()
                    } else {
                        queue.addLast(captured)
                        signalNow = queue.size == 1
                    }
                }
            }
            if (witness > 0) faults.onDiscarded(generation, kind, witness)
            if (signalNow) currentSink()?.onOutputs(kind)
            return HandoffOutcome.Queued
        } catch (cancelled: CancellationException) {
            primary = cancelled
            throw cancelled
        } catch (failure: Exception) {
            primary = failure
            throw failure
        } catch (error: Error) {
            primary = error
            throw error
        } finally {
            // The slot settles even if the callback is unwinding, and the
            // witness it may deliver cannot replace what is already leaving.
            carry(primary, attempt { settleSlot(slot) })
        }
    }

    override fun onCodecError(generation: Long, reason: String) {
        schedule.beforeTerminal()
        val fault = CaptureFault.Codec(kind, reason)
        var report = false
        synchronized(lock) {
            if (generation == this.generation && terminal == null && !closed) {
                terminal = fault
                endedFlag = true
                report = true
            }
        }
        if (report) {
            faults.onFault(generation, fault)
            currentSink()?.onOutputs(kind)
        }
    }

    override fun next(): CapturedOutput? =
        synchronized(lock) {
            val out = queue.removeFirstOrNull()
            if (out != null && out.output.isEndOfStream) endedFlag = true
            out
        }

    override fun close(mode: CloseMode) {
        var witness = 0
        synchronized(lock) {
            // Close marks and returns. It never waits for a transfer in flight,
            // and it never disposes the codec -- that is the lifetime's.
            if (closed) return
            closed = true
            closeMode = mode
            endedFlag = true
            if (mode == CloseMode.Failed) {
                discarded += queue.size
                queue.clear()
            }
            witness = witnessIfSettledLocked()
        }
        if (witness > 0) faults.onDiscarded(generation, kind, witness)
        currentSink()?.onOutputs(kind)
    }

    /** What the transfer produced: the copy and its claimed instant, or the refusal that ended it. */
    private sealed interface Transferred {
        class Copied(val bytes: ByteArray, val captureNanos: Long) : Transferred

        class Rejected(val outcome: HandoffOutcome) : Transferred
    }

    /**
     * Everything that reaches codec-owned memory, between taking the lease and
     * giving it back. The index is attempted at most once; an attempt that
     * threw is never retried. On any throw the index is settled, the cause
     * keeps its identity and carries what the cleanup found, and the lease is
     * released last.
     */
    private fun transfer(output: CodecOutput, media: Boolean, slot: AtomicBoolean): Transferred {
        if (!lifetime.retainForTransfer()) {
            return Transferred.Rejected(refuse(null, CaptureFault.Codec(kind, "the codec was disposed before this transfer began"), slot))
        }
        var released = false
        var completed = false
        var raised: Throwable? = null

        fun reject(fault: CaptureFault): Transferred {
            released = true
            val outcome = refuse(output.index, fault, slot)
            completed = true
            return Transferred.Rejected(outcome)
        }
        try {
            val buffer =
                try {
                    buffers.outputBuffer(output.index)
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (failure: Exception) {
                    return reject(CaptureFault.BufferAccess(kind, "getOutputBuffer threw: ${failure.message}"))
                }
                    ?: return reject(CaptureFault.BufferAccess(kind, "getOutputBuffer returned null"))
            // Overflow-safe, and against the LIMIT the platform set.
            if (output.offset < 0 || output.size < 0 || output.offset.toLong() + output.size > buffer.limit()) {
                return reject(CaptureFault.Malformed(kind, "range past the buffer limit"))
            }
            val bytes = ByteArray(output.size)
            // A DUPLICATE, so the view the platform owns keeps its position and
            // limit, and a direct or read-only buffer is read the same way.
            val view = buffer.duplicate()
            view.position(output.offset)
            view.limit(output.offset + output.size)
            view.get(bytes)
            val captureNanos =
                if (!media) {
                    0L
                } else {
                    when (val claim = ledger.claim(generation, kind, output.presentationTimeUs)) {
                        is ClaimOutcome.Matched -> claim.captureElapsedNanos
                        is ClaimOutcome.Missing -> return reject(CaptureFault.Correlation(kind, claim.reason))
                        is ClaimOutcome.Ignored -> return reject(CaptureFault.Correlation(kind, claim.reason))
                    }
                }
            schedule.beforeRelease()
            released = true
            val releaseFailure = releaseOnce(output.index)
            if (releaseFailure != null) {
                val outcome = refuse(null, CaptureFault.BufferAccess(kind, "release threw: ${releaseFailure.message}"), slot)
                completed = true
                return Transferred.Rejected(outcome)
            }
            completed = true
            return Transferred.Copied(bytes, captureNanos)
        } catch (cancelled: CancellationException) {
            raised = cancelled
            throw cancelled
        } catch (failure: Exception) {
            raised = failure
            throw failure
        } catch (error: Error) {
            raised = error
            throw error
        } finally {
            // EVERY obligation is attempted, whatever an earlier one raised:
            // the index goes back once, and the lease always goes back.
            val cleanup = mutableListOf<Throwable>()
            if (!completed && !released) {
                released = true
                attempt { owner.release(output.index) }?.let { cleanup.add(it) }
            }
            attempt { lifetime.releaseAfterTransfer() }?.let { cleanup.add(it) }
            carry(raised, cleanup)
        }
    }

    /**
     * A refusal decided under the monitor, before any lease: the index still
     * goes back, so the codec is held for exactly that call. If the codec is
     * already gone its indices went with it, and nothing is released.
     */
    private fun refuseAtAdmission(index: Int, fault: CaptureFault): HandoffOutcome {
        if (!lifetime.retainForTransfer()) return refuse(null, fault, slot = null)
        var primary: Throwable? = null
        try {
            return refuse(index, fault, slot = null)
        } catch (cancelled: CancellationException) {
            primary = cancelled
            throw cancelled
        } catch (failure: Exception) {
            primary = failure
            throw failure
        } catch (error: Error) {
            primary = error
            throw error
        } finally {
            carry(primary, attempt { lifetime.releaseAfterTransfer() })
        }
    }

    /** A refusal: release once, fault once, settle the slot once, notify outside the monitor. */
    private fun refuse(indexToRelease: Int?, fault: CaptureFault, slot: AtomicBoolean?): HandoffOutcome {
        var report = false
        var witness = 0
        var endNow = false
        // Every obligation is attempted, whatever an earlier one raised: the
        // index goes back once, the refusal is recorded, the owner is told,
        // the witness and the wake are delivered. Then the FIRST failure
        // leaves by identity, carrying the later distinct ones in the order
        // they happened. The refusal was decided, so it stays terminal.
        val raised = mutableListOf<Throwable>()
        indexToRelease?.let { index -> attempt { owner.release(index) }?.let { raised.add(it) } }
        synchronized(lock) {
            if (slot != null && slot.compareAndSet(false, true)) reserved--
            if (terminal == null && !closed) {
                terminal = fault
                report = true
                endedFlag = true
                endNow = true
            }
            witness = witnessIfSettledLocked()
        }
        if (report) attempt { faults.onFault(generation, fault) }?.let { raised.add(it) }
        if (witness > 0) attempt { faults.onDiscarded(generation, kind, witness) }?.let { raised.add(it) }
        if (endNow) attempt { currentSink()?.onOutputs(kind) }?.let { raised.add(it) }
        carry(null, raised)
        return HandoffOutcome.Refused(fault.toString())
    }

    /** The admitted callback's reservation, settled exactly once, whoever gets here first. */
    private fun settleSlot(slot: AtomicBoolean) {
        if (!slot.compareAndSet(false, true)) return
        var witness = 0
        synchronized(lock) {
            reserved--
            if (closed) discarded++
            witness = witnessIfSettledLocked()
        }
        if (witness > 0) faults.onDiscarded(generation, kind, witness)
    }

    private fun releaseOnce(index: Int): Exception? =
        try {
            owner.release(index)
            null
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (failure: Exception) {
            failure
        }

    /**
     * One cleanup attempt whose failure is COLLECTED rather than allowed to end
     * the cleanup around it, or to replace what is already leaving.
     */
    private fun attempt(action: () -> Unit): Throwable? =
        try {
            try {
                action()
                null
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (failure: Exception) {
                failure
            } catch (error: Error) {
                error
            }
        } catch (cancelled: CancellationException) {
            cancelled
        }

    private fun carry(primary: Throwable?, raised: Throwable?) = carry(primary, listOfNotNull(raised))

    /**
     * The primary keeps its identity and carries what the cleanup found. With
     * no primary the first cleanup failure escapes rather than disappearing.
     */
    private fun carry(primary: Throwable?, cleanup: List<Throwable>) {
        val carrier = primary ?: cleanup.firstOrNull() ?: return
        // Distinct by IDENTITY, against what the carrier already holds too:
        // one instance raised twice is one root, and never a root of itself.
        val attached = carrier.suppressed.toMutableList()
        for (other in cleanup) {
            if (other === carrier || attached.any { it === other }) continue
            attached.add(other)
            carrier.addSuppressed(other)
        }
        if (primary == null) throw carrier
    }

    /** The discard witness: once, with the exact count, when every reservation has settled. */
    private fun witnessIfSettledLocked(): Int {
        if (closed && closeMode == CloseMode.Failed && reserved == 0 && discarded > 0 && !witnessed) {
            witnessed = true
            return discarded
        }
        return 0
    }

    private fun currentSink(): OutputSignal? = synchronized(lock) { sink }
}
