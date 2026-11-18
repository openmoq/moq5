package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException

/**
 * One part of the session's single acquisition: the camera's, or the
 * microphone's. [SequencedAcquirer] starts the owned worker once and then
 * runs each part.
 */
internal interface CapturePart : CaptureFaultRouting {
    val order: List<CaptureResource>

    /** A refusal decided before the worker starts or anything is opened; null to proceed. */
    fun check(): AcquireOutcome.Failed? = null

    fun begin(ownership: CaptureOwnership, steps: AcquisitionSteps): AcquireOutcome.Failed?
}

/** The audio track's own inputs, bound by assembly before anything starts: its ledger, its keying and its generation. */
internal interface CaptureInputRouting {
    fun bindAudioInputs(ledger: CaptureLedger, keying: AudioKeying, generation: Long)
}

/**
 * The session's one acquirer: the camera and audio parts' own checks, then
 * the owned worker, once, then the camera part, then the audio part. Each
 * stops the startup at its first failure; what was installed unwinds by the
 * ownership's rules.
 *
 * The parts do not serialize each other's callbacks: the camera and its stream
 * install whenever they settle, before, between or after the audio steps. Only
 * each part's own edges are ordered.
 */
internal class SequencedAcquirer(
    private val worker: WorkerStart,
    private val camera: CameraPart?,
    private val audio: AudioPart?,
) : CaptureAcquirer, CaptureFaultRouting, CaptureInputRouting {
    override val order: List<CaptureResource> =
        listOf(CaptureResource.Worker) + camera?.order.orEmpty() + audio?.order.orEmpty()

    override fun begin(ownership: CaptureOwnership, steps: AcquisitionSteps): AcquireOutcome.Failed? {
        camera?.check()?.let { return it }
        audio?.check()?.let { return it }
        when (val started = ownership.acquire(CaptureResource.Worker) { worker.startWorker() }) {
            is AcquireOutcome.Failed -> return started
            is AcquireOutcome.Retired -> return null
            AcquireOutcome.Acquired -> Unit
        }
        camera?.begin(ownership, steps)?.let { return it }
        return audio?.begin(ownership, steps)
    }

    override fun bindFaults(sink: CaptureFaultSink) {
        camera?.bindFaults(sink)
        audio?.bindFaults(sink)
    }

    /** The audio part's inputs; an acquisition without one has none to bind. */
    override fun bindAudioInputs(ledger: CaptureLedger, keying: AudioKeying, generation: Long) {
        audio?.bindAudioInputs(ledger, keying, generation)
    }
}

/** What the recorder reports it records. */
internal data class MicrophoneFormat(val sampleRate: Int, val channelCount: Int, val pcm16: Boolean)

/**
 * One built `AudioRecord`, as the audio part drives it. Every call may throw;
 * none is made under the part's lock.
 */
internal interface MicrophoneLink {
    /** The platform recorder itself. */
    val identity: Any

    /** `getState() == STATE_INITIALIZED`. */
    val initialized: Boolean

    /** The rate, channel count and encoding the recorder reports. */
    val format: MicrophoneFormat

    /** `getBufferSizeInFrames()`, read once at acquisition. */
    val bufferSizeInFrames: Int

    /**
     * `setRecordPositionUpdateListener(listener, workerHandler)`, then
     * `setPositionNotificationPeriod(periodFrames)`; the period call's result
     * code. A notification is a wake HINT: [wake] runs on the owned worker.
     */
    fun listen(periodFrames: Int, wake: () -> Unit): Int

    /** `startRecording()`. */
    fun start()

    /** `getRecordingState() == RECORDSTATE_RECORDING`. */
    val recording: Boolean

    /** `read(into, bytes, READ_NON_BLOCKING)`: bytes read from [into]'s start, 0, or a negative error code. */
    fun read(into: java.nio.ByteBuffer, bytes: Int): Int

    /** `getTimestamp(ts, TIMEBASE_BOOTTIME)`, or null when unavailable. */
    fun timestamp(): AudioTimestampFacts?

    /** `setRecordPositionUpdateListener(null)`: best effort; a queued callback may still arrive. */
    fun detach()

    fun stop()

    fun release()
}

/** Builds the recorder for one configuration. May throw; builds nothing then. */
internal fun interface MicrophonePort {
    fun build(configuration: AudioEncoderConfiguration): MicrophoneLink
}

/** A posted one-shot action, cancelled best effort. */
internal fun interface TimerTicket {
    fun cancel()
}

/** `Handler.postDelayed` on the owned worker: a ticket, or null when the post was refused. */
internal fun interface WorkerTimer {
    fun post(delayMillis: Long, action: () -> Unit): TimerTicket?
}

/**
 * The audio part: the AAC encoder, then the microphone, and the bounded pump
 * that moves recorded PCM into the encoder's input.
 *
 * **The microphone holds its codec.** Opening the microphone first takes the
 * audio encoder's own transfer lease -- the worker, then the codec, both or
 * neither -- BEFORE any recorder is built. That lease is given back only after
 * the recorder's final stop and release attempts: when a failed open has
 * cleaned up its local record, or when the installed microphone's disposal
 * has run. So however the ownership's closes interleave -- a stop while the
 * microphone is still opening, or while a read is in flight -- the codec is
 * never disposed while the recorder can still feed it, and the worker is
 * released last, inside the lease's return.
 *
 * **The microphone's own lifetime.** An installed microphone is disposed like
 * the codec is: each pump pass holds a READ lease around its reads and its
 * queue; closing the handle requests the disposal, and the last lease
 * returned runs it, once. The disposal marks the microphone dead, then
 * attempts detaching the listener, stopping, releasing, and finally returns
 * the codec lease -- every owed step, the first failure carrying the rest.
 *
 * **Admission.** Every pass -- a position notification, the codec offering an
 * input index, the bootstrap, the watchdog -- runs as one of the audio codec's
 * callbacks, admitted by the session's codec-role authority, AND only for the
 * one installed, live microphone it was issued for. A microphone is PENDING
 * until activated after its installation, and DEAD from the first of its
 * disposal, a failed open's cleanup or a failed activation; a pass for any
 * other does nothing at all -- no read, no timestamp, no index, no queue, no
 * timer. Detaching the listener is best effort: a queued callback may still
 * arrive, and is refused here.
 *
 * **Activation** happens only once the ownership has installed the
 * microphone: arm the watchdog, then post one bootstrap pass. Either refused
 * is a named startup failure, and the installed microphone closes through the
 * ownership.
 *
 * **One pass** reads without blocking into one owned direct staging unit of
 * exactly one access unit, never past it, and never discards a positive read.
 * A complete unit is keyed ([AudioKeying.onRead], [AudioKeying.keyFor]), its
 * first frame placed by [AudioFrameClock.elapsedNanosOf] from the recorder's
 * timestamp, OBSERVED on the audio ledger, and only then queued into an input
 * index the codec offered. Without a usable timestamp, or without an index,
 * the unit stays staged and reading stops: nothing is overwritten. A queue
 * that raises is never retried; the index is the codec's either way.
 *
 * **What this does not know.** The recorder's frame position and the frames
 * read are not a synchronized producer/consumer pair, and Java exposes no
 * reliable platform-loss signal. A backlog beyond the buffer size read at
 * acquisition is named -- "continuity cannot be established" -- but a backlog
 * within it proves nothing about loss. Platform-side loss the Java API does
 * not expose is a stated limitation.
 *
 * **The watchdog** is one-shot, armed at activation and re-armed only by a
 * pass that made progress; each arm has its own number, and a replaced or
 * late one does nothing. When it fires it runs one pass: progress re-arms it,
 * and none is a named fault. Its deadline is elapsed time used for that
 * failure only, never to place media.
 */
internal class AudioPart(
    private val encoder: TrackEncoder,
    private val microphone: MicrophonePort,
    private val timer: WorkerTimer,
    private val dispatcher: CallbackDispatcher,
    private val late: LateCleanupSink,
    private val progressBoundMillis: Long = PROGRESS_BOUND_MILLIS,
) : CapturePart, CaptureInputRouting {
    override val order: List<CaptureResource> = listOf(CaptureResource.AudioEncoder, CaptureResource.Microphone)

    private val configuration: AudioEncoderConfiguration =
        checkNotNull(encoder.configuration as? AudioEncoderConfiguration) { "the microphone feeds an audio encoder, not a ${encoder.kind} one" }

    private val lock = Any()
    private var faults: CaptureFaultSink? = null
    private var inputs: AudioInputs? = null
    private var begun = false

    /** The installed microphone, once activated. */
    private var live: Microphone? = null

    private class AudioInputs(val ledger: CaptureLedger, val keying: AudioKeying, val generation: Long)

    override fun bindFaults(sink: CaptureFaultSink) {
        synchronized(lock) {
            check(faults == null) { "this audio part's faults are already bound" }
            faults = sink
        }
    }

    override fun bindAudioInputs(ledger: CaptureLedger, keying: AudioKeying, generation: Long) {
        synchronized(lock) {
            check(inputs == null) { "this audio part's inputs are already bound" }
            inputs = AudioInputs(ledger, keying, generation)
        }
    }

    override fun begin(ownership: CaptureOwnership, steps: AcquisitionSteps): AcquireOutcome.Failed? {
        val bound =
            synchronized(lock) {
                check(!begun) { "this audio part has already begun" }
                begun = true
                checkNotNull(inputs) { "the audio part's inputs are not bound" }
            }
        // Already inside the codec callback that offered the index: admitted.
        encoder.onInputOffered { synchronized(lock) { live }?.passAdmitted(deadline = false) }
        when (val opened = ownership.acquire(CaptureResource.AudioEncoder) { encoder.open(ownership) }) {
            is AcquireOutcome.Failed -> return opened
            is AcquireOutcome.Retired -> return null
            AcquireOutcome.Acquired -> Unit
        }
        var built: Microphone? = null
        when (val installed = ownership.acquire(CaptureResource.Microphone) { openMicrophone(ownership, bound).also { built = it } }) {
            is AcquireOutcome.Failed -> return installed
            is AcquireOutcome.Retired -> return null
            AcquireOutcome.Acquired -> Unit
        }
        return checkNotNull(built).activate()
    }

    /**
     * The `open` the ownership runs for the microphone: the codec lease first,
     * then the recorder, built, checked, listened to and started. Any step
     * failing cleans up the LOCAL record and returns the lease; a cleanup that
     * failed is stated as [UnprovedDisposal].
     */
    private fun openMicrophone(ownership: CaptureOwnership, bound: AudioInputs): Microphone {
        if (!encoder.retainForTransfer()) throw IllegalStateException("the audio encoder is no longer available")
        val epoch = Epoch()
        var link: MicrophoneLink? = null
        var started = false
        var primary: Throwable? = null
        try {
            val record = microphone.build(configuration)
            link = record
            check(record.initialized) { "the microphone could not be initialized" }
            val format = record.format
            if (format.sampleRate != configuration.sampleRate || format.channelCount != configuration.channelCount || !format.pcm16) {
                throw IllegalStateException(
                    "the microphone records ${format.sampleRate} Hz, ${format.channelCount} channel(s), " +
                        (if (format.pcm16) "16-bit PCM" else "not 16-bit PCM") +
                        "; the encoder was configured for ${configuration.sampleRate} Hz, ${configuration.channelCount} channel(s)",
                )
            }
            val code = record.listen(AudioEncoderConfiguration.FRAMES_PER_UNIT) { wake(epoch) }
            if (code != 0) throw IllegalStateException("the microphone's position notification could not be set: $code")
            started = true
            record.start()
            check(record.recording) { "the microphone did not start recording" }
            return Microphone(ownership, bound, record, epoch)
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
            primary?.let { first ->
                epoch.kill()
                val failures = mutableListOf<Throwable>()
                link?.let { record ->
                    collected { record.detach() }?.let { failures.add(it) }
                    if (started) collected { record.stop() }?.let { failures.add(it) }
                    collected { record.release() }?.let { failures.add(it) }
                }
                collected { encoder.releaseAfterTransfer() }?.let { failures.add(it) }
                if (failures.isNotEmpty()) throw UnprovedDisposal(first, failures)
            }
        }
    }

    /** A position notification, for the microphone whose listener it was. */
    private fun wake(epoch: Epoch) {
        synchronized(lock) { live }?.takeIf { it.epoch === epoch }?.pass(deadline = false)
    }

    private fun report(generation: Long, fault: CaptureFault) {
        checkNotNull(synchronized(lock) { faults }) { "an audio fault arrived before the session's fault route was bound" }.onFault(generation, fault)
    }

    /** PENDING until activated, LIVE, then DEAD for good. */
    private class Epoch {
        @Volatile var state = PENDING

        val isLive: Boolean get() = state == LIVE

        fun kill() {
            state = DEAD
        }

        companion object {
            const val PENDING = 0
            const val LIVE = 1
            const val DEAD = 2
        }
    }

    /** One installed recorder: its lifetime, its staging unit, its watchdog. */
    private inner class Microphone(
        private val ownership: CaptureOwnership,
        private val bound: AudioInputs,
        private val link: MicrophoneLink,
        val epoch: Epoch,
    ) : ResourceHandle {
        override val identity: Any get() = link.identity

        private val unitBytes = configuration.maxInputBytes
        private val bytesPerFrame = configuration.channelCount * AudioEncoderConfiguration.BYTES_PER_SAMPLE
        private val staging: java.nio.ByteBuffer = java.nio.ByteBuffer.allocateDirect(unitBytes)

        /** Read once at acquisition; a heuristic bound, not refreshed on reroute. */
        private val backlogBound = link.bufferSizeInFrames.toLong()

        // Guarded by `lock` below.
        private val lock = Any()
        private var reads = 0
        private var requested = false
        private var committed = false
        private var arm = 0
        private var ticket: TimerTicket? = null
        private var failed = false

        // Touched only by one pass at a time: passes run on the one worker, under the codec-role reservation.
        private var fill = 0
        private var staged: Long? = null

        /** The staged unit already observed on the ledger: observed once, then only queued. */
        private var observed: Long? = null
        private var framesConsumed = 0L
        private var lastFramePosition: Long? = null
        private var progress = 0L

        /** Installed: LIVE, then the watchdog, then the bootstrap. */
        fun activate(): AcquireOutcome.Failed? {
            synchronized(this@AudioPart.lock) { live = this }
            synchronized(lock) { if (!requested) epoch.state = Epoch.LIVE }
            if (!epoch.isLive) return null
            if (!arm()) return AcquireOutcome.Failed(CaptureResource.Microphone, "the capture watchdog could not be scheduled")
            if (!dispatcher.deliver { pass(deadline = false) }) {
                return AcquireOutcome.Failed(CaptureResource.Microphone, "the capture bootstrap could not be posted")
            }
            return null
        }

        /** Arms the watchdog afresh; the previous arm is cancelled, best effort, and made stale. False when refused. */
        private fun arm(): Boolean {
            val number: Int
            val previous: TimerTicket?
            synchronized(lock) {
                if (requested || !epoch.isLive) return true
                arm++
                number = arm
                previous = ticket
                ticket = null
            }
            previous?.cancel()
            val posted = timer.post(progressBoundMillis) { deadline(number) } ?: return false
            synchronized(lock) { if (arm == number) ticket = posted }
            return true
        }

        private fun deadline(number: Int) {
            if (synchronized(lock) { arm != number }) return
            pass(deadline = true)
        }

        /** One bounded pass for this microphone, admitted as one of the audio codec's callbacks. */
        fun pass(deadline: Boolean) = encoder.driveInput { passAdmitted(deadline) }

        /** The pass itself, from inside an admitted codec callback. */
        fun passAdmitted(deadline: Boolean) {
            run {
                if (!epoch.isLive || synchronized(this@AudioPart.lock) { live } !== this) return
                if (!retainRead()) return
                var primary: Throwable? = null
                try {
                    pump(deadline)
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
                    val disposal = releaseRead()
                    if (disposal.isNotEmpty()) {
                        val leaving = primary
                        if (leaving != null) {
                            disposal.forEach { if (it !== leaving) leaving.addSuppressed(it) }
                        } else {
                            reportLate(disposal)
                        }
                    }
                }
            }
        }

        private fun pump(deadline: Boolean) {
            val before = progress
            while (fill < unitBytes && staged == null) {
                if (stopping()) return
                val into = staging.duplicate()
                into.position(fill)
                val n = link.read(into.slice(), unitBytes - fill)
                if (n < 0) return fail(CaptureFault.Source(MediaKind.Audio, "the microphone read failed: $n"))
                if (n == 0) break
                fill += n
                val frames = n / bytesPerFrame
                framesConsumed += frames
                progress++
                bound.keying.onRead(frames).firstOrNull()?.let { staged = it }
            }
            if (staged != null && !stopping()) queueStaged()
            if (failed || stopping()) return
            if (progress > before) {
                if (!arm()) fail(CaptureFault.Source(MediaKind.Audio, "the capture watchdog could not be scheduled"))
                return
            }
            if (deadline) {
                val waiting = staged != null && observed != staged && link.timestamp() == null
                val bound = progressBoundMillis * 1_000L
                fail(CaptureFault.Source(MediaKind.Audio, if (waiting) "no usable audio timestamp within $bound us" else "no audio capture progress within $bound us"))
            }
        }

        /**
         * A complete unit: keyed, placed and OBSERVED -- once -- and only then
         * given an input index and queued. Holds it when it cannot be yet.
         */
        private fun queueStaged() {
            val unit = checkNotNull(staged)
            val key =
                when (val keyed = bound.keying.keyFor(unit)) {
                    is AudioKeyOutcome.Key -> keyed.presentationTimeUs
                    is AudioKeyOutcome.Refused -> return fail(CaptureFault.Source(MediaKind.Audio, "unit $unit has no key: ${keyed.reason}"))
                }
            if (observed != unit && !observe(unit, key)) return
            val index = encoder.takeInput() ?: return
            val whole = staging.duplicate()
            whole.position(0)
            whole.limit(unitBytes)
            val refused =
                try {
                    encoder.queueInput(index, whole, key)
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (failure: Exception) {
                    return fail(CaptureFault.Codec(MediaKind.Audio, "the audio input could not be queued: ${failure.message}"))
                }
            if (refused != null) return fail(CaptureFault.Codec(MediaKind.Audio, "the audio input could not be queued: $refused"))
            fill = 0
            staged = null
            observed = null
            progress++
        }

        /** The unit's first frame placed and filed on the ledger. False when it cannot be yet, or failed. */
        private fun observe(unit: Long, key: Long): Boolean {
            val stamp = link.timestamp() ?: return false
            if (stamp.timebase == PlatformTimebase.ElapsedRealtime) {
                lastFramePosition?.let { previous ->
                    if (stamp.framePosition < previous) {
                        return failed(CaptureFault.Source(MediaKind.Audio, "the audio timestamp's frame position went backwards: $previous then ${stamp.framePosition}"))
                    }
                }
                lastFramePosition = stamp.framePosition
            }
            val firstFrame = unit * AudioEncoderConfiguration.FRAMES_PER_UNIT
            val elapsed =
                when (val placed = AudioFrameClock.elapsedNanosOf(stamp, firstFrame, configuration.sampleRate)) {
                    is FrameElapsed.At -> placed.nanos
                    is FrameElapsed.Unavailable -> return failed(CaptureFault.Source(MediaKind.Audio, "unit $unit has no capture instant: ${placed.reason}"))
                }
            if (stamp.framePosition - framesConsumed > backlogBound) {
                return failed(CaptureFault.Source(MediaKind.Audio, "capture backlog exceeded bound: continuity cannot be established"))
            }
            when (val filed = bound.ledger.observe(bound.generation, MediaKind.Audio, CaptureObservation(key, elapsed))) {
                ObserveOutcome.Recorded -> Unit
                is ObserveOutcome.Refused -> {
                    fail(CaptureFault.Correlation(MediaKind.Audio, "the audio ledger refused unit $unit: ${filed.reason}"))
                    return false
                }
                is ObserveOutcome.Ignored -> return false
            }
            observed = unit
            return true
        }

        private fun fail(fault: CaptureFault) {
            failed = true
            report(bound.generation, fault)
        }

        /** [fail], as the false a placing step answers with. */
        private fun failed(fault: CaptureFault): Boolean {
            fail(fault)
            return false
        }

        private fun stopping(): Boolean = synchronized(lock) { requested } || !epoch.isLive

        private fun retainRead(): Boolean =
            synchronized(lock) {
                if (committed || requested) {
                    false
                } else {
                    reads++
                    true
                }
            }

        /** Gives a read lease back; the last one runs a disposal that was requested meanwhile. */
        private fun releaseRead(): List<Throwable> {
            val commit = synchronized(lock) { reads--; commitLocked() }
            return if (commit) dispose() else emptyList()
        }

        override fun close() {
            val commit = synchronized(lock) { requested = true; commitLocked() }
            if (!commit) return
            val failures = dispose()
            val first = failures.firstOrNull() ?: return
            failures.drop(1).forEach { if (it !== first) first.addSuppressed(it) }
            throw first
        }

        private fun commitLocked(): Boolean {
            if (!requested || committed || reads > 0) return false
            committed = true
            return true
        }

        /** Dead first; then detach, stop, release; then the codec lease. Every step attempted. */
        private fun dispose(): List<Throwable> {
            epoch.kill()
            synchronized(this@AudioPart.lock) { if (live === this) live = null }
            val cancelling = synchronized(lock) { ticket.also { ticket = null; arm++ } }
            val failures = mutableListOf<Throwable>()
            cancelling?.let { t -> collected { t.cancel() }?.let { failures.add(it) } }
            collected { link.detach() }?.let { failures.add(it) }
            collected { link.stop() }?.let { failures.add(it) }
            collected { link.release() }?.let { failures.add(it) }
            collected { encoder.releaseAfterTransfer() }?.let { failures.add(it) }
            return failures
        }

        /** A disposal the last read lease ran, after the ownership's close returned: its failure is a late cleanup fact. */
        private fun reportLate(failures: List<Throwable>) {
            val first = failures.first()
            failures.drop(1).forEach { if (it !== first) first.addSuppressed(it) }
            if (first is CancellationException || first !is Exception) throw first
            val reporting = collected { late.onLateClose(ownership.generation, CaptureResource.Microphone, first) } ?: return
            if (reporting !== first) reporting.addSuppressed(first)
            throw reporting
        }
    }

    /**
     * One step whose failure is COLLECTED, whatever its class, and returned
     * for the caller to own. The inner rethrow is the cancellation rule for
     * ordinary bodies; this is a cleanup collector, so it is caught again here.
     */
    private fun collected(step: () -> Unit): Throwable? =
        try {
            try {
                step()
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

    companion object {
        /** The watchdog's bound on a capture with no progress: a sample policy, not a platform fact. */
        const val PROGRESS_BOUND_MILLIS = 1_000L
    }
}
