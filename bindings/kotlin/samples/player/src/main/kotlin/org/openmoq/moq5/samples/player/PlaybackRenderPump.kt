package org.openmoq.moq5.samples.player

import kotlinx.coroutines.CancellationException

internal enum class PlaybackLane { Video, Audio }

internal sealed interface PlaybackOffer {
    data object Queued : PlaybackOffer
    data object Busy : PlaybackOffer
    data object DiscardedUntilSync : PlaybackOffer
}

internal class PlaybackRenderRefused(val reason: String) : IllegalStateException(reason)

/**
 * Serial, nonreentrant decoder/synchronizer pump. The owner services bounded
 * passes while active, interleaved with receiver input and MediaSync callbacks.
 * It supplies ONE logical time domain for both tracks, not arrival timestamps.
 * No thread, delay or transport polling lives here; the only clock read is
 * the injected display clock of video-only [PlaybackVideoPacing].
 *
 * Startup keeps a bounded prefix until every selected track has media, then
 * freezes its minimum timestamp as a common nonnegative platform origin.
 * Later timestamps preceding it fail explicitly. Decode order is unchanged;
 * a per-track clamp/rebase would corrupt A/V offsets. Queue pressure after
 * startup is retryable; before startup it fails instead of parking the sole
 * receiver behind one track while waiting for another. The enclosing receive
 * operation must also bound waiting for an absent selected track.
 *
 * Video reaches the display one of two ways. With a synchronizer, each
 * frame is released at once, stamped with its relative media time, into
 * MediaSync's input. Without one (video only, [pacing] set), the decoder
 * renders to the display Surface itself and at most one frame is held until
 * its display deadline: later outputs stay with the decoder, so order is kept,
 * and nothing is dropped, however late.
 *
 * Queued transfers ownership of the access-unit bytes to this pump. Busy
 * transfers nothing. Caller must not mutate an accepted byte array. Inputs
 * are bounded by count per track and bytes across tracks. Platform decoder
 * backing allocations and MediaSync PCM borrows have their separate bounds.
 */
internal class PlaybackRenderPump(
    private val video: PlaybackCodec,
    private val audio: PlaybackCodec?,
    private val sync: PlaybackSync?,
    private val configureAudio: (PlaybackOutputFormat) -> Unit,
    private val maxUnitsPerTrack: Int = 64,
    private val maxQueuedBytes: Int = 16 * 1024 * 1024,
    private val pacing: PlaybackVideoPacing? = null,
) {
    init {
        require(maxUnitsPerTrack > 0 && maxQueuedBytes > 0)
        require(audio == null || sync != null) { "audio is presented only through the synchronizer" }
        require(pacing == null || sync == null) { "paced direct video is the video-only path" }
        require(sync != null || pacing != null) { "video needs the synchronizer or display pacing" }
    }

    private class Input(val bytes: ByteArray, val logicalUs: Long)
    private class Lane(val codec: PlaybackCodec) {
        val input = ArrayDeque<Input>()
        var lastInputUs = 0L
        var endQueued = false
        var ended = false
    }

    private val v = Lane(video)
    private val a = audio?.let { Lane(it) }
    private var queuedBytes = 0L
    private var videoSync = false
    private var pcmFormat: PlaybackOutputFormat? = null
    private var heldAudio: PlaybackDecodedOutput? = null
    private var heldVideo: PlaybackDecodedOutput? = null
    private var heldVideoDeadlineNs = 0L
    private var exhausted = false
    private var stopped = false
    private var failure: Throwable? = null
    var originUs: Long? = null
        private set
    var discardedBeforeSync: Long = 0
        private set

    /**
     * Video outputs released to the display Surface: counted only after a
     * successful release of a nonempty, non-config buffer. Config and empty
     * EOS buffers are discarded, not rendered, and never count.
     */
    var videoReleased: Int = 0
        private set

    /**
     * Decoder output EOS only, and on the paced path only once a frame carried
     * by the EOS buffer has been released. Does NOT mean all media was presented.
     */
    val decoderOutputsEnded: Boolean get() = v.ended && (a == null || a.ended)

    fun offer(lane: PlaybackLane, unit: AccessUnitOutcome.Unit, logicalUs: Long): PlaybackOffer {
        usable()
        check(!exhausted) { "the renderer input is exhausted" }
        val target = when (lane) {
            PlaybackLane.Video -> v
            PlaybackLane.Audio -> requireNotNull(a) { "no audio track was selected" }
        }
        require(unit.bytes.isNotEmpty() && unit.bytes.size <= maxQueuedBytes) { "access unit exceeds the renderer byte policy" }
        if (lane == PlaybackLane.Video && !videoSync && !unit.sync) {
            check(discardedBeforeSync < Long.MAX_VALUE) { "video discard counter exhausted" }
            discardedBeforeSync++
            return PlaybackOffer.DiscardedUntilSync
        }
        return guarded {
            originUs?.let { platformTime(logicalUs, it) }
            if (target.input.size >= maxUnitsPerTrack || queuedBytes + unit.bytes.size > maxQueuedBytes) {
                if (originUs == null) throw PlaybackRenderRefused("startup media exceeded its bound before all selected tracks arrived")
                return@guarded PlaybackOffer.Busy
            }
            target.input.addLast(Input(unit.bytes, logicalUs))
            queuedBytes += unit.bytes.size
            if (lane == PlaybackLane.Video) videoSync = true
            if (originUs == null && v.input.isNotEmpty() && (a == null || a.input.isNotEmpty())) {
                val lower = sequenceOf(v, a).filterNotNull().flatMap { it.input.asSequence() }.minOf { it.logicalUs }
                // Validate every retained timestamp before publishing the origin
                // or submitting a prefix to either decoder.
                sequenceOf(v, a).filterNotNull().forEach { track -> track.input.forEach { platformTime(it.logicalUs, lower) } }
                originUs = lower
            }
            PlaybackOffer.Queued
        }
    }

    /** Only call after the supplied input itself is exhausted, never on track END/quiet. */
    fun endInputs() {
        usable()
        if (exhausted) return
        guarded {
            if (originUs == null) throw PlaybackRenderRefused("input ended before every selected track supplied startup media")
            exhausted = true
        }
    }

    /** At most one output and one input operation per track per round. */
    fun pump(rounds: Int = 8): Boolean {
        usable()
        require(rounds in 1..64) { "renderer pass must be bounded" }
        return guarded {
            sync?.failure?.let { throw it }
            val origin = originUs ?: return@guarded false
            var progress = false
            repeat(rounds) {
                // Drain before feeding so an input-full codec can relinquish
                // output capacity. No short-circuit may skip the other lane.
                if (drainVideo()) progress = true
                if (a != null && drainAudio(a)) progress = true
                if (feed(v, origin)) progress = true
                if (a != null && feed(a, origin)) progress = true
            }
            progress
        }
    }

    private fun feed(lane: Lane, origin: Long): Boolean {
        val next = lane.input.firstOrNull()
        if (next != null) {
            val time = platformTime(next.logicalUs, origin)
            if (!lane.codec.offer(next.bytes, time)) return false
            lane.input.removeFirst()
            queuedBytes -= next.bytes.size
            lane.lastInputUs = time
            return true
        }
        if (exhausted && !lane.endQueued && lane.codec.endInput(lane.lastInputUs)) {
            lane.endQueued = true
            return true
        }
        return false
    }

    private fun drainVideo(): Boolean {
        heldVideo?.let { return releaseWhenDue(it) }
        return when (val output = video.poll()) {
            PlaybackCodec.Output.Idle -> false
            is PlaybackCodec.Output.Format -> {
                if (!output.format.mime.startsWith("video/")) throw PlaybackRenderRefused("video decoder reported a non-video output format")
                true
            }
            is PlaybackCodec.Output.Buffer -> {
                val lease = output.lease
                val time = if (lease.config || lease.size == 0) null else checkedNanoseconds(lease.presentationUs)
                val paced = pacing
                if (paced != null && time != null) {
                    heldVideoDeadlineNs = paced.deadline(time)
                    heldVideo = lease
                    releaseWhenDue(lease)
                    return true
                }
                video.release(lease, time)
                if (time != null) videoReleased++
                if (lease.end) v.ended = true
                true
            }
        }
    }

    /** Release the held frame to the display, stamped with its deadline, once that is reached. */
    private fun releaseWhenDue(lease: PlaybackDecodedOutput): Boolean {
        if (!checkNotNull(pacing).due(heldVideoDeadlineNs)) return false
        video.release(lease, heldVideoDeadlineNs)
        heldVideo = null
        videoReleased++
        if (lease.end) v.ended = true
        return true
    }

    private fun drainAudio(lane: Lane): Boolean {
        val held = heldAudio
        if (held != null) return submitAudio(lane, held)
        return when (val output = lane.codec.poll()) {
            PlaybackCodec.Output.Idle -> false
            is PlaybackCodec.Output.Format -> {
                val prior = pcmFormat
                if (prior != null && prior != output.format) throw PlaybackRenderRefused("decoded PCM format changed during playback")
                if (prior == null) {
                    configureAudio(output.format)
                    pcmFormat = output.format
                }
                true
            }
            is PlaybackCodec.Output.Buffer -> {
                val lease = output.lease
                if (lease.config || lease.size == 0) {
                    lane.codec.release(lease)
                    if (lease.end) lane.ended = true
                    true
                } else {
                    if (pcmFormat == null) throw PlaybackRenderRefused("PCM arrived before the decoder output format")
                    heldAudio = lease
                    submitAudio(lane, lease)
                }
            }
        }
    }

    private fun submitAudio(lane: Lane, lease: PlaybackDecodedOutput): Boolean {
        checkedNanoseconds(lease.presentationUs)
        if (!checkNotNull(sync).queueAudio(lease, lease.presentationUs)) return false
        heldAudio = null
        if (lease.end) lane.ended = true
        return true
    }

    /** Stop admissions only. Enclosing owner closes sync before disposing the codecs. */
    fun stop() {
        if (stopped) return
        stopped = true
        v.input.clear()
        a?.input?.clear()
        queuedBytes = 0
        // heldAudio is still owned by its codec, which cannot close before
        // sync quiescence; stop is not an alternate lease-release path. A held
        // video frame likewise stays with its codec, whose close settles it.
        heldVideo = null
    }

    private fun usable() {
        check(!stopped) { "the render pump has stopped" }
        failure?.let { throw IllegalStateException("the render pump failed; only stop is legal", it) }
    }

    private inline fun <T> guarded(block: () -> T): T = try {
        block()
    } catch (cancelled: CancellationException) {
        failure = cancelled
        throw cancelled
    } catch (raised: Exception) {
        failure = raised
        throw raised
    } catch (raised: Error) {
        failure = raised
        throw raised
    }

    private fun platformTime(logical: Long, origin: Long): Long {
        if (logical < origin) throw PlaybackRenderRefused("media precedes the frozen platform origin")
        // Subtract without negating origin (which may be Long.MIN_VALUE).
        if (origin < 0 && logical > Long.MAX_VALUE + origin) throw PlaybackRenderRefused("platform timestamp difference overflows")
        val relative = logical - origin
        checkedNanoseconds(relative)
        return relative
    }

    private fun checkedNanoseconds(micros: Long): Long {
        if (micros < 0 || micros > Long.MAX_VALUE / 1000) throw PlaybackRenderRefused("media timestamp is outside the platform nanosecond range")
        return micros * 1000
    }
}
