package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Deferred
import org.openmoq.moq5.TrackName

/** What one output-format observation did. */
internal sealed interface FormatOutcome {
    /** The session has claimed its ending: nothing was read, validated or offered. */
    data object Ended : FormatOutcome

    /**
     * This track's previous observation was still being handled: nothing was
     * read or validated, no callback was admitted, and the overlap was
     * reported as the track's `CaptureFault.Format`.
     */
    data object Overlapping : FormatOutcome

    /** The first verdict was complete: the slot now holds [format]. */
    class Resolved(val format: CompleteTrackFormat) : FormatOutcome

    /** The first verdict was a refusal: the slot failed with [failure]. */
    class Refused(val failure: TrackFormatRefused) : FormatOutcome

    /** A later complete verdict equal to the held format: no effect. */
    data object Equal : FormatOutcome

    /** A later verdict that differs from [held] -- complete or refused -- reported as [fault]; [held] stays. */
    class Changed(val held: CompleteTrackFormat, val fault: CaptureFault.Format) : FormatOutcome

    /** The slot had already failed with a refusal; nothing repairs it. */
    data object AlreadyRefused : FormatOutcome

    /** The slot had already failed with an unexpected [cause]; nothing repairs it. */
    class AlreadyFailed(val cause: Throwable) : FormatOutcome
}

/**
 * The one shape both holders share: bound ONCE to one session's key for its
 * kind and to that kind's slot, and admitted per observation by the session
 * alone.
 *
 * **One observation.** The session admits it or not before anything is read:
 * an ended session, and an overlap with this track's observation still being
 * handled, read no snapshot, call no utility and touch no slot -- the overlap
 * is reported as the track's format fault, outside every lock. An admitted
 * observation's reservation is owned from the moment it is admitted and
 * released exactly once, whatever happens: validation, the slot's offer --
 * whose delivery may run handlers inline, even ones that observe again --
 * and a fault's delivery all run while it is held.
 *
 * **Verdicts.** The first complete verdict resolves the slot and an equal
 * repeat does nothing; a later verdict that differs, or a later refusal, is
 * reported as `CaptureFault.Format` and never replaces the held format. A
 * first refusal fails the slot, and so the format wait.
 *
 * **Unexpected failures.** Anything validation or the offer raises that is
 * not a typed verdict -- an Exception, an Error, a foreign cancellation --
 * fails the slot with that same cause if it has not settled, then is
 * reported by releasing the reservation, so the session owns it (claiming
 * the ending if none is claimed yet), and is then rethrown to the caller as
 * itself. What failing the slot raised is attached to the cause before the
 * release; a cleanup failure the release's claimant work raises -- of any
 * class, a cancellation included -- is the receipt's already and never
 * replaces the cause. Nothing here waits.
 */
private class FormatHolderCore(private val gate: FormatCallbackGate, private val slot: TrackFormatSlot, private val kind: MediaKind) {
    init {
        require(slot.kind == kind) { "a $kind holder bound to the ${slot.kind} slot" }
    }

    private val key: FormatHolderKey = gate.bindFormatHolder(kind)

    fun observe(validate: () -> TrackFormatVerdict): FormatOutcome {
        val reservation =
            when (val admission = gate.reserveFormat(key)) {
                is FormatAdmission.Admitted -> admission.reservation
                FormatAdmission.Ended -> return FormatOutcome.Ended
                FormatAdmission.Overlapping -> {
                    gate.reportFormat(key, OVERLAPPING)
                    return FormatOutcome.Overlapping
                }
                FormatAdmission.NotBound -> throw IllegalStateException("this $kind holder's key is not bound to its session")
            }
        var cause: Throwable? = null
        try {
            val offer =
                try {
                    slot.offer(validate())
                } catch (cancelled: CancellationException) {
                    cause = cancelled
                    throw cancelled
                } catch (failure: Exception) {
                    cause = failure
                    throw failure
                } catch (error: Error) {
                    cause = error
                    throw error
                }
            return when (offer) {
                is SlotOffer.Resolved -> FormatOutcome.Resolved(offer.format)
                is SlotOffer.Refused -> FormatOutcome.Refused(offer.failure)
                SlotOffer.Equal -> FormatOutcome.Equal
                SlotOffer.AlreadyRefused -> FormatOutcome.AlreadyRefused
                is SlotOffer.AlreadyFailed -> FormatOutcome.AlreadyFailed(offer.cause)
                is SlotOffer.Changed -> {
                    val fault = CaptureFault.Format(kind, reasonFor(offer))
                    gate.reportFormat(key, fault.reason)
                    FormatOutcome.Changed(offer.held, fault)
                }
            }
        } finally {
            settle(reservation, cause)
        }
    }

    /**
     * Fails the slot with [cause] if it has not settled, then releases
     * [reservation] exactly once. Neither step's failure -- of whatever class,
     * a cancellation included -- ends the other or replaces [cause]: what
     * failing the slot raised is attached to [cause] before the release, and
     * what the release raised is a cleanup failure of the claimant work it
     * did, already owned by the receipt, so it leaves by itself only when no
     * cause is leaving.
     */
    private fun settle(reservation: FormatReservation, cause: Throwable?) {
        if (cause != null) {
            // The slot's delivery wraps a handler's failure, so what reaches
            // here is that wrapper or the slot's own contract failure; it is
            // collected the same way whatever its class.
            collected { slot.fail(cause) }?.let { if (it !== cause) cause.addSuppressed(it) }
        }
        val raised = collected { gate.releaseFormat(reservation, cause) }
        if (raised != null && cause == null) throw raised
    }

    /**
     * One synchronous cleanup step whose failure is COLLECTED, whatever its
     * class, and returned for the caller to own -- attached, rethrown, or
     * already the receipt's -- rather than allowed to replace what is leaving.
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

    private fun reasonFor(changed: SlotOffer.Changed): String =
        when (val observed = changed.observed) {
            is TrackFormatVerdict.Complete -> "the ${kind.name.lowercase()} output format changed after it was validated"
            is TrackFormatVerdict.Refused -> observed.refusal.description
        }

    private companion object {
        const val OVERLAPPING = "overlapping output-format observations"
    }
}

/** The video track's format holder: observations in, the video slot's format out. See [FormatHolderCore]. */
internal class VideoFormatHolder(
    gate: FormatCallbackGate,
    private val slot: TrackFormatSlot,
    private val name: TrackName,
    private val bounds: ConfigBounds,
    private val utility: AvcRecordUtility = PublicCodecSignaling,
) {
    private val core = FormatHolderCore(gate, slot, MediaKind.Video)

    val format: Deferred<CompleteTrackFormat> get() = slot.format

    /** One encoder output-format observation; its codec-specific data is read only inside this call. */
    fun onOutputFormat(observed: VideoFormatObservation): FormatOutcome = core.observe { TrackFormats.video(name, observed, bounds, utility) }

    /**
     * One encoder output-format callback, read from [fields] only once this
     * observation is ADMITTED and while its reservation is held: an ended or
     * overlapping callback reads no field and no codec-specific data.
     * [maximumBitrate] is the track's declared maximum, the caller's input.
     */
    fun onOutputFormat(fields: FormatFields, maximumBitrate: ULong?): FormatOutcome =
        core.observe {
            when (val extracted = FormatExtraction.video(fields, maximumBitrate)) {
                is Extracted.Observed -> TrackFormats.video(name, extracted.observation, bounds, utility)
                is Extracted.Refused -> TrackFormatVerdict.Refused(MediaKind.Video, extracted.refusal)
            }
        }
}

/** The audio track's format holder: observations in, the audio slot's format out. See [FormatHolderCore]. */
internal class AudioFormatHolder(
    gate: FormatCallbackGate,
    private val slot: TrackFormatSlot,
    private val name: TrackName,
    private val bounds: ConfigBounds,
    private val utility: AacStringUtility = PublicAacSignaling,
) {
    private val core = FormatHolderCore(gate, slot, MediaKind.Audio)

    val format: Deferred<CompleteTrackFormat> get() = slot.format

    /** One encoder output-format observation; its codec-specific data is read only inside this call. */
    fun onOutputFormat(observed: AudioFormatObservation): FormatOutcome = core.observe { TrackFormats.audio(name, observed, bounds, utility) }

    /** The audio counterpart of [VideoFormatHolder.onOutputFormat] over [FormatFields]: read only once admitted. */
    fun onOutputFormat(fields: FormatFields, maximumBitrate: ULong?): FormatOutcome =
        core.observe {
            when (val extracted = FormatExtraction.audio(fields, maximumBitrate)) {
                is Extracted.Observed -> TrackFormats.audio(name, extracted.observation, bounds, utility)
                is Extracted.Refused -> TrackFormatVerdict.Refused(MediaKind.Audio, extracted.refusal)
            }
        }
}

/**
 * The two holders of one session, each over a fresh slot of its kind, and
 * the [sources] the live format-wait preparation awaits.
 */
internal class FormatHolders(
    gate: FormatCallbackGate,
    videoName: TrackName,
    audioName: TrackName,
    bounds: ConfigBounds,
    videoUtility: AvcRecordUtility = PublicCodecSignaling,
    audioUtility: AacStringUtility = PublicAacSignaling,
) {
    val video = VideoFormatHolder(gate, TrackFormatSlot(MediaKind.Video), videoName, bounds, videoUtility)
    val audio = AudioFormatHolder(gate, TrackFormatSlot(MediaKind.Audio), audioName, bounds, audioUtility)
    val sources: FormatSources = FormatSources(video.format, audio.format)
}
