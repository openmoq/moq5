package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException

/**
 * The camera's part of the capture acquisition: the video encoder, the camera
 * and its capture stream, in that order, on the owned worker that
 * [SequencedAcquirer] has already started.
 *
 * **Why that order.** The ownership closes what it installed in REVERSE
 * installation order. The camera and its stream install from callbacks, so
 * this is a partial order, not a serialization: the encoder is installed
 * before the stream can be, and is held by the camera's surface lease until
 * the device's `onClosed`. The camera stream draws into the encoder's input surface,
 * and that surface exists only once the encoder is open, so the encoder is
 * acquired first. The input surface is owned by the encoder alone -- created
 * with it, released after it -- and never installed a second time as a camera
 * surface.
 *
 * **The surface outlives the camera that draws into it.** Closing the camera
 * is not proof that it has stopped touching the surface: `close()` stops
 * further calls and callbacks and discards remaining requests, but the device
 * is FULLY shut down only at its later `onClosed`
 * (`CameraDevice.java:1390-1405`, `:1621-1632`). So from the moment the surface
 * is handed to a camera, that camera holds a lease on the encoder's lifetime
 * -- the same lease a transfer takes, which also holds the worker `onClosed`
 * arrives on -- and gives it back only at that device's `onClosed`. The
 * encoder's disposal is deferred until then. A camera that never reports
 * `onClosed` keeps the encoder and its worker for the life of the process: the
 * ownership's stated no-deadline cost, not a timer.
 *
 * **Tickets.** The camera and the stream settle through platform callbacks,
 * so each is REQUESTED from the ownership and SUBMITTED to the session's
 * accounting before the platform call that can settle it, including one that
 * settles it inline. The stream depends on the opened camera
 * (`CameraDevice.StateCallback.onOpened`, `CameraDevice.java:1577`), so its
 * ticket is submitted inside `onOpened`, before the camera's settlement is
 * reported. Each ticket's FIRST settlement is the only one; a later callback
 * is handed to the ownership, which disposes of a foreign resource and leaves
 * a duplicate alone, and is never reported again.
 *
 * **A stream still configuring when its camera goes.** Once a camera is
 * closed no callback but `onClosed` arrives from it, so a stream it was still
 * configuring may never be settled by the platform. Closing the kept camera --
 * by a stop, a failure or a loss -- therefore settles that stream's ticket and
 * step itself, once, as failed; a configure callback arriving afterwards finds
 * the ticket settled and is disposed of as nobody's.
 *
 * **Device loss.** `onDisconnected`/`onError` on the kept camera is device
 * loss, not an acquisition result: the video track fails with
 * [CaptureFault.Source] first -- so the loss is the ending's cause -- and the
 * ownership closes the camera. A failed session "is considered to be closed"
 * (`CameraCaptureSession.java:1057-1064`), so it carries nothing to close.
 *
 * **A platform call that raises a cancellation or an Error** is never turned
 * into a named refusal, and leaves as itself. Every ticket it had submitted is
 * released, once; a step an inline callback already settled is not settled
 * again. The camera's open raises into the startup, which unwinds with it. A
 * stream submission or a repeating request raises inside a platform callback:
 * its stream is withdrawn -- the ticket released, the adopted stream closed --
 * the video track fails with [CaptureFault.Source] naming the raise, and the
 * raise is rethrown to the callback's caller. What the release or the report
 * raised travels with it.
 *
 * **The repeating request** starts only once the configured stream -- whose
 * target is the encoder's surface -- has been ADOPTED, which means this
 * generation is still live. If it is refused, the stream is closed and its
 * step fails; readiness is never reported for a stream that does not run.
 *
 * Nothing platform-specific happens here: the camera is reached through
 * [CameraPort], which `Camera2Capture.kt` implements over Camera2. The encoder's disposal,
 * when the camera's `onClosed` completes it, reports an ordinary failure to
 * [late]; a report that raises leaves as itself, whatever its class, carrying
 * that failure.
 */
internal class CameraPart(
    private val encoder: TrackEncoder,
    private val camera: CameraPort,
    private val late: LateCleanupSink,
) : CapturePart {
    override val order: List<CaptureResource> =
        listOf(CaptureResource.VideoEncoder, CaptureResource.Camera, CaptureResource.CameraStream)

    /** The frame-rate range the camera's own facts planned, before anything started. */
    private var planned: IntRange? = null

    private val lock = Any()
    private var faults: CaptureFaultSink? = null
    private var begun = false
    private var cameraSettled = false
    private var streamSettled = false

    /** The kept camera, by its platform identity. */
    private var kept: Any? = null

    /** The camera holding the encoder-lifetime lease for its surface, by identity; null once given back. */
    private var leased: Any? = null

    /** The stream ticket, once submitted, with what settles it. */
    private var stream: PendingStream? = null

    private class PendingStream(val ownership: CaptureOwnership, val steps: AcquisitionSteps, val ticket: AcquisitionTicket)

    override fun bindFaults(sink: CaptureFaultSink) {
        synchronized(lock) {
            check(faults == null) { "this camera part's faults are already bound" }
            faults = sink
        }
    }

    /**
     * Every refusal the camera's own facts can decide, decided before the
     * worker is started or anything is opened or submitted.
     */
    override fun check(): AcquireOutcome.Failed? {
        val configuration =
            checkNotNull(encoder.configuration as? VideoEncoderConfiguration) { "the camera feeds a video encoder, not a ${encoder.kind} one" }
        val plan =
            try {
                CameraCheck.plan(configuration, camera.facts())
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (failure: Exception) {
                return AcquireOutcome.Failed(CaptureResource.Camera, "the camera's characteristics could not be read: ${failure.message}")
            }
        return when (plan) {
            is CameraPlan.Refused -> AcquireOutcome.Failed(CaptureResource.Camera, plan.reason)
            is CameraPlan.Planned -> {
                synchronized(lock) { planned = plan.fps }
                null
            }
        }
    }

    override fun begin(ownership: CaptureOwnership, steps: AcquisitionSteps): AcquireOutcome.Failed? {
        val fps =
            synchronized(lock) {
                check(!begun) { "this camera part has already begun" }
                begun = true
                checkNotNull(planned) { "the camera part begins only after its check" }
            }
        when (val opened = ownership.acquire(CaptureResource.VideoEncoder) { encoder.open(ownership) }) {
            is AcquireOutcome.Failed -> return opened
            is AcquireOutcome.Retired -> return null
            AcquireOutcome.Acquired -> Unit
        }
        val surface = encoder.inputSurface ?: return AcquireOutcome.Failed(CaptureResource.VideoEncoder, "the video encoder offered no input surface")

        val ticket = ownership.request(CaptureResource.Camera)
        steps.submitted(CaptureResource.Camera)
        val failure = submitOpen(CameraSettlement(ownership, steps, ticket, surface, fps), ownership, ticket) ?: return null
        // `openCamera` failed where it stands: the ticket it was submitted
        // under settles here, once, carrying nothing.
        val reason = "the camera could not be opened: ${failure.message}"
        if (settleCamera()) steps.settled(CaptureResource.Camera, ownership.fail(ticket, reason, null))
        return AcquireOutcome.Failed(CaptureResource.Camera, reason)
    }

    /**
     * `openCamera`. An ordinary failure is returned for the named refusal. A
     * cancellation or an Error leaves as itself, into the startup's own unwind,
     * with the camera ticket released unless a callback already settled it.
     */
    private fun submitOpen(events: CameraSettlement, ownership: CaptureOwnership, ticket: AcquisitionTicket): Exception? {
        var raised: Throwable? = null
        try {
            camera.open(events)
            return null
        } catch (cancelled: CancellationException) {
            raised = cancelled
            throw cancelled
        } catch (failure: Exception) {
            return failure
        } catch (error: Error) {
            raised = error
            throw error
        } finally {
            raised?.let { leaving ->
                if (settleCamera()) attach(leaving, collected { ownership.fail(ticket, "the camera's opening raised ${named(leaving)}", null) })
            }
        }
    }

    /** True for the camera ticket's FIRST settlement, and only then. */
    private fun settleCamera(): Boolean = synchronized(lock) { if (cameraSettled) false else { cameraSettled = true; true } }

    private fun settleStream(): Boolean = synchronized(lock) { if (streamSettled) false else { streamSettled = true; true } }

    private fun report(ownership: CaptureOwnership, fault: CaptureFault) {
        checkNotNull(synchronized(lock) { faults }) { "a camera fault arrived before the session's fault route was bound" }.onFault(ownership.generation, fault)
    }

    /**
     * A stream still configuring when its camera closed: its ticket and its
     * step settle here, once, as failed. Outside the lock.
     */
    private fun withdrawStream(reason: String) {
        val pending = synchronized(lock) { if (streamSettled) null else stream?.also { streamSettled = true } } ?: return
        pending.steps.settled(CaptureResource.CameraStream, pending.ownership.fail(pending.ticket, reason, null))
    }

    /**
     * The kept camera as the ownership holds it: closing it closes the device
     * and then settles a stream the device can no longer configure.
     */
    private inner class KeptCamera(private val link: CameraLink) : ResourceHandle {
        override val identity: Any get() = link.identity

        override fun close() {
            val closing = collected { link.close() }
            val withdrawing = collected { withdrawStream("the camera closed before its stream was configured") }
            val first = closing ?: withdrawing ?: return
            attach(first, withdrawing)
            throw first
        }
    }

    /** The camera ticket's settlement, the stream it submits, and the device's later life. Platform callbacks arrive here on the owned worker. */
    private inner class CameraSettlement(
        private val ownership: CaptureOwnership,
        private val steps: AcquisitionSteps,
        private val ticket: AcquisitionTicket,
        private val surface: Any,
        private val fps: IntRange,
    ) : CameraEvents {
        override fun onOpened(camera: CameraLink) {
            if (!settleCamera()) {
                // Not this ticket's settlement: the ownership leaves a
                // duplicate of the kept camera alone and disposes of any
                // other, once.
                ownership.adopt(ticket, camera)
                return
            }
            // The dependent stream is submitted BEFORE the camera's settlement
            // is reported, and before anything can settle the stream itself.
            val streamTicket = ownership.request(CaptureResource.CameraStream)
            synchronized(lock) { stream = PendingStream(ownership, steps, streamTicket) }
            steps.submitted(CaptureResource.CameraStream)
            val outcome = ownership.adopt(ticket, KeptCamera(camera))
            if (outcome != AdoptOutcome.Adopted) {
                // A camera this generation did not keep gets no surface and no stream.
                val streamFailure = if (settleStream()) ownership.fail(streamTicket, "the camera was not kept: $outcome", null) else null
                steps.settled(CaptureResource.Camera, outcome)
                streamFailure?.let { steps.settled(CaptureResource.CameraStream, it) }
                return
            }
            synchronized(lock) { kept = camera.identity }
            // From here until this device's onClosed, the camera may touch the
            // encoder's surface: it holds the encoder, and its worker.
            if (!encoder.retainForTransfer()) {
                val streamFailure = if (settleStream()) ownership.fail(streamTicket, "the encoder's input surface is no longer available", null) else null
                steps.settled(CaptureResource.Camera, outcome)
                streamFailure?.let { steps.settled(CaptureResource.CameraStream, it) }
                return
            }
            synchronized(lock) { leased = camera.identity }
            var raised: Throwable? = null
            var streamFailure: AdoptOutcome? = null
            try {
                camera.createStream(surface, StreamSettlement(ownership, steps, streamTicket, surface, fps))
            } catch (cancelled: CancellationException) {
                raised = cancelled
                throw cancelled
            } catch (failure: Exception) {
                if (settleStream()) streamFailure = ownership.fail(streamTicket, "the capture stream could not be submitted: ${failure.message}", null)
            } catch (error: Error) {
                raised = error
                throw error
            } finally {
                raised?.let { leaving ->
                    // Withdrawn: the ticket is released without a step, the
                    // loss is the video track's, and the camera's own
                    // settlement is still the one it was.
                    if (settleStream()) attach(leaving, collected { ownership.fail(streamTicket, "the capture stream's submission raised ${named(leaving)}", null) })
                    attach(leaving, collected { report(ownership, CaptureFault.Source(MediaKind.Video, "the capture stream's submission raised ${named(leaving)}")) })
                    attach(leaving, collected { steps.settled(CaptureResource.Camera, outcome) })
                }
            }
            steps.settled(CaptureResource.Camera, outcome)
            streamFailure?.let { steps.settled(CaptureResource.CameraStream, it) }
        }

        override fun onDisconnected(camera: CameraLink) = failed(camera, "the camera was disconnected")

        override fun onError(camera: CameraLink, error: Int) = failed(camera, "the camera reported error $error")

        private fun failed(camera: CameraLink, reason: String) {
            if (settleCamera()) {
                // Instead of onOpened: the ticket fails, and the device the
                // platform hands over is closed by the ownership, once.
                steps.settled(CaptureResource.Camera, ownership.fail(ticket, reason, camera))
                return
            }
            // After the acquisition settled, only the KEPT camera can be
            // lost; any other was closed when it was refused.
            if (synchronized(lock) { kept !== camera.identity }) return
            report(ownership, CaptureFault.Source(MediaKind.Video, reason))
            ownership.lost(CaptureResource.Camera, reason)
        }

        override fun onClosed(camera: CameraLink) {
            val release = synchronized(lock) { if (leased != null && leased === camera.identity) { leased = null; true } else false }
            if (!release) return
            val failure = collected { encoder.releaseAfterTransfer() } ?: return
            if (failure is CancellationException || failure !is Exception) throw failure
            // As the ownership reports a late close: the sink is what carries
            // the failure, so a sink that raises leaves carrying it.
            val reporting = collected { late.onLateClose(ownership.generation, CaptureResource.VideoEncoder, failure) } ?: return
            attach(reporting, failure)
            throw reporting
        }
    }

    /** The stream ticket's settlement, and the repeating request once the stream is kept. */
    private inner class StreamSettlement(
        private val ownership: CaptureOwnership,
        private val steps: AcquisitionSteps,
        private val ticket: AcquisitionTicket,
        private val surface: Any,
        private val fps: IntRange,
    ) : StreamEvents {
        override fun onConfigured(stream: StreamLink) {
            if (!settleStream()) {
                // Settled already -- by an earlier callback, or withdrawn when
                // its camera closed: the ownership leaves a duplicate of the
                // kept stream alone and disposes of any other, once.
                ownership.adopt(ticket, stream)
                return
            }
            val outcome = ownership.adopt(ticket, stream)
            if (outcome != AdoptOutcome.Adopted) {
                steps.settled(CaptureResource.CameraStream, outcome)
                return
            }
            var raised: Throwable? = null
            val refused =
                try {
                    stream.startRepeating(surface, fps)
                    null
                } catch (cancelled: CancellationException) {
                    raised = cancelled
                    throw cancelled
                } catch (failure: Exception) {
                    failure
                } catch (error: Error) {
                    raised = error
                    throw error
                } finally {
                    raised?.let { leaving ->
                        attach(leaving, collected { ownership.lost(CaptureResource.CameraStream, "the repeating request raised ${named(leaving)}") })
                        attach(leaving, collected { report(ownership, CaptureFault.Source(MediaKind.Video, "the repeating request raised ${named(leaving)}")) })
                    }
                }
            if (refused != null) {
                val reason = "the repeating request could not start: ${refused.message}"
                ownership.lost(CaptureResource.CameraStream, reason)
                steps.settled(CaptureResource.CameraStream, AdoptOutcome.Failed(reason))
                return
            }
            steps.settled(CaptureResource.CameraStream, AdoptOutcome.Adopted)
        }

        override fun onConfigureFailed(stream: StreamLink) {
            // A failed session is already closed by the platform: nothing is carried.
            if (settleStream()) steps.settled(CaptureResource.CameraStream, ownership.fail(ticket, "the capture stream could not be configured", null))
        }
    }

    private fun named(raised: Throwable): String = "${raised::class.simpleName}: ${raised.message}"

    /** [extra], when present and distinct, travels with [carrier]. */
    private fun attach(carrier: Throwable, extra: Throwable?) {
        if (extra != null && extra !== carrier && carrier.suppressed.none { it === extra }) carrier.addSuppressed(extra)
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
}

/** Starts the owned callback worker: the Worker acquisition. [HandlerDispatcher.startWorker] on the platform. */
internal fun interface WorkerStart {
    fun startWorker(): ResourceHandle
}

/**
 * One camera, as the acquisition reaches it. Implemented over
 * `CameraManager` in `Camera2Capture.kt`.
 */
internal interface CameraPort {
    /** What the camera reports; may throw. Read before anything is opened. */
    fun facts(): CameraFacts

    /**
     * `CameraManager.openCamera`: may throw where it stands; otherwise
     * exactly one of [CameraEvents]'s callbacks settles it, possibly inline.
     */
    fun open(events: CameraEvents)
}

/** An opened camera device, closed once by its owner; `identity` is the platform device. */
internal interface CameraLink : ResourceHandle {
    /** `createCaptureSession` with [surface] as its one output; may throw where it stands. */
    fun createStream(surface: Any, events: StreamEvents)
}

/** A configured capture stream, closed once by its owner; `identity` is the platform session. */
internal interface StreamLink : ResourceHandle {
    /** A recording request targeting [surface] at [fps], repeated; may throw. */
    fun startRepeating(surface: Any, fps: IntRange)
}

/** `CameraDevice.StateCallback`, with the device wrapped as a [CameraLink]. */
internal interface CameraEvents {
    fun onOpened(camera: CameraLink)

    fun onDisconnected(camera: CameraLink)

    fun onError(camera: CameraLink, error: Int)

    /** `onClosed`: the device a `close()` was called on has fully shut down. */
    fun onClosed(camera: CameraLink)
}

/** `CameraCaptureSession.StateCallback`, with the session wrapped as a [StreamLink]. */
internal interface StreamEvents {
    fun onConfigured(stream: StreamLink)

    fun onConfigureFailed(stream: StreamLink)
}

/**
 * What the camera says it can stream into an encoder's input surface: the
 * output sizes for that surface class, and the auto-exposure target frame-rate
 * ranges.
 */
internal data class CameraFacts(val encoderOutputSizes: List<Pair<Int, Int>>, val targetFpsRanges: List<IntRange>)

/** The camera's side of a video configuration: the frame-rate range to request, or why it cannot be met. */
internal sealed interface CameraPlan {
    data class Planned(val fps: IntRange) : CameraPlan

    data class Refused(val reason: String) : CameraPlan
}

internal object CameraCheck {
    /**
     * The encoder's size must be one the camera streams into an encoder
     * surface, and some target range must END at the encoder's frame rate: a
     * fixed range of exactly that rate when there is one, otherwise the one
     * with the highest floor. Nothing is scaled or substituted.
     */
    fun plan(configuration: VideoEncoderConfiguration, facts: CameraFacts): CameraPlan {
        val size = configuration.width to configuration.height
        if (size !in facts.encoderOutputSizes) {
            return CameraPlan.Refused("the camera cannot stream ${configuration.width}x${configuration.height} into an encoder surface")
        }
        val ending = facts.targetFpsRanges.filter { it.last == configuration.frameRate }
        val chosen = ending.maxByOrNull { it.first }
            ?: return CameraPlan.Refused("the camera has no target frame-rate range ending at ${configuration.frameRate} fps")
        return CameraPlan.Planned(chosen)
    }
}
