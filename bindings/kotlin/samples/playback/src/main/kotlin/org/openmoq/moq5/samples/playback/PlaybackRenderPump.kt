package org.openmoq.moq5.samples.playback

import kotlinx.coroutines.CancellationException

internal enum class PlaybackLane { Video, Audio }

internal sealed interface PlaybackOffer {
    data object Queued : PlaybackOffer
    data object Busy : PlaybackOffer
    data object DiscardedUntilSync : PlaybackOffer
    /** A unit of a group this lane has already moved past: it arrived too late to be decoded in order. */
    data object DiscardedLateGroup : PlaybackOffer
}

internal class PlaybackRenderRefused(val reason: String) : IllegalStateException(reason)

/**
 * Serial, nonreentrant decoder/render pump. The owner services bounded passes
 * while active, interleaved with receiver input. It supplies ONE logical time
 * domain for both tracks, not arrival timestamps. No thread, delay or
 * transport polling lives here; the only clock reads are those of [pacing]
 * and the audio output.
 *
 * Startup keeps a bounded prefix until every selected track has media, then
 * freezes its minimum timestamp as a common nonnegative platform origin.
 * Later timestamps preceding it fail explicitly. Decode order is unchanged;
 * a per-track clamp/rebase would corrupt A/V offsets. A relay can deliver one
 * track's backlog of several groups before the other track's first object,
 * so the prefix has its own admission budget, [startupUnitsPerTrack] entries
 * per track within the shared [maxQueuedBytes], larger than the steady
 * [maxUnitsPerTrack]. Exceeding either before startup fails by name instead of
 * parking the sole receiver behind one track while waiting for another; the
 * enclosing receive operation also bounds the wait for an absent selected
 * track in time. Once the origin is frozen the steady bound applies: a prefix
 * larger than it drains through retryable Busy and group pressure, and
 * nothing of it is dropped.
 *
 * Groups arrive interleaved: each travels on its own QUIC stream, so a later
 * group's first units can overtake an earlier group's tail, and a relay may
 * serve a cached group alongside the live one. Each lane's input is kept in
 * group order (a group's own units keep their arrival order), so a late tail
 * is still decoded before the next group. The next group starts at once when
 * the input is exhausted, and otherwise when the playback clock is within
 * [GROUP_LEAD_NS] of it, when [GROUP_HOLD_US] of later media is queued behind
 * it, when the input is under pressure, or when nothing of the earlier group
 * has been fed for [GROUP_QUIET_NS]. Whatever of the earlier group has not
 * arrived by then is given up, and a unit of a group already passed is
 * [PlaybackOffer.DiscardedLateGroup]. When a later queued group is already
 * due, the earlier group's queued rest is skipped undecoded: it could only be
 * shown late. Units without a group keep their arrival order.
 *
 * Each PCM output is written to [audioOut] without blocking and returned to
 * its decoder once written; a full sink keeps it held, and later outputs stay
 * with the decoder.
 *
 * The decoder renders video to the display Surface itself. At most one frame
 * is held until [pacing] releases or drops it: later outputs stay with the
 * decoder, so order is kept. [pacing] follows the audio output while audio
 * leads, and its own display clock otherwise.
 *
 * Queued transfers ownership of the access-unit bytes to this pump. Busy
 * transfers nothing. Caller must not mutate an accepted byte array. Inputs
 * are bounded by count per track and bytes across tracks; platform decoder
 * backing allocations have their separate bounds.
 */
internal class PlaybackRenderPump(
    private val video: PlaybackCodec,
    private val audio: PlaybackCodec?,
    private val audioOut: PlaybackAudioOutput?,
    private val pacing: PlaybackVideoPacing,
    private val maxUnitsPerTrack: Int = 64,
    private val maxQueuedBytes: Int = 16 * 1024 * 1024,
    private val startupUnitsPerTrack: Int = STARTUP_UNITS_PER_TRACK,
) {
    init {
        require(maxUnitsPerTrack > 0 && maxQueuedBytes > 0)
        require(startupUnitsPerTrack >= maxUnitsPerTrack) { "the startup budget never admits less than the steady bound" }
        require((audio == null) == (audioOut == null)) { "audio is decoded exactly when it has an output" }
    }

    private class Input(val bytes: ByteArray, val logicalUs: Long, val group: ULong?)
    private class Lane(val codec: PlaybackCodec) {
        val input = ArrayDeque<Input>()
        var lastInputUs = 0L
        /** The group being decoded; null before the first grouped unit goes to the decoder. */
        var group: ULong? = null
        /** Since when a later group has waited on [group]'s tail with nothing fed; null while not waiting. */
        var waitingSinceNs: Long? = null
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
    private var heldVideoMediaNs = 0L
    private var exhausted = false
    private var stopped = false
    private var failure: Throwable? = null
    var originUs: Long? = null
        private set
    var discardedBeforeSync: Long = 0
        private set
    /** Units of a group their lane had already moved past. */
    var discardedLateGroup: Long = 0
        private set

    /** Times a lane started its next group before the earlier one was known complete. */
    var groupAdvances: Long = 0
        private set

    /** Queued units skipped undecoded: a later group was already due, so they could only be shown late. */
    var skippedLate: Long = 0
        private set

    /**
     * Video outputs released to the display Surface: counted only after a
     * successful release of a nonempty, non-config buffer. Config and empty
     * EOS buffers are discarded, not rendered, and never count; neither do
     * frames dropped as late.
     */
    var videoReleased: Int = 0
        private set

    /** Video frames released unrendered because they were too late for the audio. */
    val videoDroppedLate: Long get() = pacing.droppedLate

    /**
     * Decoder output EOS only, and only once a frame carried by the EOS buffer
     * has been released. Does NOT mean all media was presented.
     */
    val decoderOutputsEnded: Boolean get() = v.ended && (a == null || a.ended)

    fun offer(lane: PlaybackLane, unit: AccessUnitOutcome.Unit, logicalUs: Long, group: ULong? = null): PlaybackOffer {
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
        val decoding = target.group
        if (group != null && decoding != null && group < decoding) {
            check(discardedLateGroup < Long.MAX_VALUE) { "late-group discard counter exhausted" }
            discardedLateGroup++
            PlaybackTrace.mark { "late-group lane=$lane group=$group decoding=$decoding" }
            return PlaybackOffer.DiscardedLateGroup
        }
        return guarded {
            originUs?.let { platformTime(logicalUs, it) }
            val starting = originUs == null
            if (target.input.size >= (if (starting) startupUnitsPerTrack else maxUnitsPerTrack)) {
                if (starting) throw PlaybackRenderRefused("startup media exceeded its entry bound before all selected tracks arrived")
                return@guarded PlaybackOffer.Busy
            }
            if (queuedBytes + unit.bytes.size > maxQueuedBytes) {
                if (starting) throw PlaybackRenderRefused("startup media exceeded its payload bound before all selected tracks arrived")
                return@guarded PlaybackOffer.Busy
            }
            insertInGroupOrder(target.input, Input(unit.bytes, logicalUs, group))
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

    /** After every queued unit of the same or an earlier group, so a group's own units keep their order. */
    private fun insertInGroupOrder(queue: ArrayDeque<Input>, item: Input) {
        val g = item.group
        var at = queue.size
        if (g != null) {
            while (at > 0) {
                val before = queue[at - 1].group
                if (before == null || before <= g) break
                at--
            }
        }
        queue.add(at, item)
    }

    private fun feed(lane: Lane, origin: Long): Boolean {
        skipToDueGroup(lane, origin)
        val next = lane.input.firstOrNull()
        if (next != null) {
            val time = platformTime(next.logicalUs, origin)
            val start = start(lane, next, time) ?: return false
            if (!lane.codec.offer(next.bytes, time)) return false
            lane.input.removeFirst()
            queuedBytes -= next.bytes.size
            lane.lastInputUs = time
            lane.waitingSinceNs = null
            if (start != GroupStart.Within) {
                groupAdvances++
                PlaybackTrace.mark { "group-advance group=${next.group} from=${lane.group} reason=${start.name.lowercase()}" }
            }
            next.group?.let { g -> if (lane.group.let { it == null || g > it }) lane.group = g }
            return true
        }
        if (exhausted && !lane.endQueued && lane.codec.endInput(lane.lastInputUs)) {
            lane.endQueued = true
            return true
        }
        return false
    }

    /** Why a lane's next unit may go to the decoder. */
    private enum class GroupStart {
        /** No later group is involved: the unit has no group, is the lane's first, or belongs to the group being decoded. */
        Within,
        Exhausted,
        Clock,
        Hold,
        Pressure,
        Quiet,
    }

    /**
     * Whether [next] may go to the decoder, and why; null while it waits.
     * Within the group being decoded, always. A later group waits while the
     * earlier one's tail may still arrive, and starts as soon as the input is
     * exhausted (no tail can follow), the clock is within [GROUP_LEAD_NS] of
     * it, [GROUP_HOLD_US] of later media is queued behind it, the input is
     * under pressure, or nothing of the earlier group has been fed for
     * [GROUP_QUIET_NS] of monotonic time. The quiet bound is what carries a
     * lane across a timestamp gap: no presentation advances the media clock
     * there, so the clock alone would wait on the very unit it holds back.
     */
    private fun start(lane: Lane, next: Input, timeUs: Long): GroupStart? {
        val g = next.group ?: return GroupStart.Within
        val current = lane.group ?: return GroupStart.Within
        if (g <= current) return GroupStart.Within
        if (exhausted) return GroupStart.Exhausted
        val clock = clockNs()
        if (clock != null && clock >= timeUs * 1000 - GROUP_LEAD_NS) return GroupStart.Clock
        if (lane.input.last().logicalUs - next.logicalUs >= GROUP_HOLD_US) return GroupStart.Hold
        if (lane.input.size >= maxUnitsPerTrack || queuedBytes * 2 >= maxQueuedBytes) return GroupStart.Pressure
        val now = pacing.now()
        val since = lane.waitingSinceNs ?: now.also { lane.waitingSinceNs = it }
        return if (now - since >= GROUP_QUIET_NS) GroupStart.Quiet else null
    }

    /**
     * Once a later queued group's first unit is already due on the clock,
     * the rest of the group being decoded could only be shown late (a relay's
     * cached backlog, say): it is skipped undecoded and the lane moves on.
     * "Due" is the clock reaching that unit, not the decode lead, so the end
     * of a group that is still on time is never cut.
     */
    private fun skipToDueGroup(lane: Lane, origin: Long) {
        val current = lane.group ?: return
        val clock = clockNs() ?: return
        val later = lane.input.indexOfFirst { unit -> unit.group.let { it != null && it > current } }
        if (later <= 0) return
        val first = lane.input[later]
        if (clock < platformTime(first.logicalUs, origin) * 1000) return
        repeat(later) {
            val skipped = lane.input.removeFirst()
            queuedBytes -= skipped.bytes.size
            skippedLate++
        }
        lane.group = first.group
        lane.waitingSinceNs = null
        groupAdvances++
        PlaybackTrace.mark { "group-skip group=${first.group} from=$current skipped=$later" }
    }

    /** The latest relative media time known presented, video or audio; null before either. */
    private fun clockNs(): Long? {
        val shown = pacing.presentedNs
        val heard = audioOut?.presentedMediaNs
        return when {
            shown == null -> heard
            heard == null -> shown
            else -> maxOf(shown, heard)
        }
    }

    private fun drainVideo(): Boolean {
        heldVideo?.let { return place(it) }
        return when (val output = video.poll()) {
            PlaybackCodec.Output.Idle -> false
            is PlaybackCodec.Output.Format -> {
                if (!output.format.mime.startsWith("video/")) throw PlaybackRenderRefused("video decoder reported a non-video output format")
                true
            }
            is PlaybackCodec.Output.Buffer -> {
                val lease = output.lease
                if (lease.config || lease.size == 0) {
                    video.release(lease, null)
                    if (lease.end) v.ended = true
                    return true
                }
                heldVideoMediaNs = checkedNanoseconds(lease.presentationUs)
                heldVideo = lease
                place(lease)
                true
            }
        }
    }

    /** Release or drop the held frame once [pacing] decides it. */
    private fun place(lease: PlaybackDecodedOutput): Boolean {
        when (val step = pacing.step(heldVideoMediaNs)) {
            PlaybackVideoStep.Hold -> return false
            PlaybackVideoStep.Drop -> video.release(lease, null)
            is PlaybackVideoStep.Render -> {
                video.release(lease, step.atNs)
                videoReleased++
            }
        }
        heldVideo = null
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
                    checkNotNull(audioOut).configure(output.format)
                    pcmFormat = output.format
                }
                true
            }
            is PlaybackCodec.Output.Buffer -> {
                val lease = output.lease
                if (lease.config || lease.size == 0) {
                    lane.codec.release(lease)
                    if (lease.end) endAudio(lane)
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
        if (!checkNotNull(audioOut).write(lease)) return false
        heldAudio = null
        lane.codec.release(lease)
        if (lease.end) endAudio(lane)
        return true
    }

    private fun endAudio(lane: Lane) {
        lane.ended = true
        checkNotNull(audioOut).endInput()
    }

    /** Stop admissions only. The enclosing owner then closes the codecs and the track. */
    fun stop() {
        if (stopped) return
        stopped = true
        v.input.clear()
        a?.input?.clear()
        queuedBytes = 0
        // A held PCM output or video frame is still owned by its codec, whose
        // close settles it; stop is not an alternate lease-release path.
        heldAudio = null
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

    private companion object {
        /** How far ahead of the clock a later group starts: the decoder needs its input early. */
        const val GROUP_LEAD_NS = 500_000_000L

        /** Later media queued behind a waiting group that gives up the earlier group's tail. */
        const val GROUP_HOLD_US = 1_000_000L

        /** How long nothing of the earlier group may be fed before a waiting group gives up its tail. */
        const val GROUP_QUIET_NS = 500_000_000L

        /**
         * Units one track may hold before every selected track has media: an
         * admission budget, not a heap measurement. 512 covers about 10.9 s of
         * AAC (1,024-sample frames at 48 kHz), 10.2 s of 20 ms Opus, 8.5 s of
         * 60 fps and 21 s of 24 fps video: a relay's several-group backlog of
         * either track, and close to the receive loop's 10 s wait for an absent
         * track at real-time arrival. Each entry also costs its holder object,
         * a deque slot and possibly a boxed group, so the two tracks' 1,024
         * entries stay small beside the payload cap. Group-ordered insertion
         * shifts at most 511 entries and the origin scan reads at most 1,024
         * once, so no single offer can starve the cooperative deadline.
         */
        const val STARTUP_UNITS_PER_TRACK = 512
    }

    private fun checkedNanoseconds(micros: Long): Long {
        if (micros < 0 || micros > Long.MAX_VALUE / 1000) throw PlaybackRenderRefused("media timestamp is outside the platform nanosecond range")
        return micros * 1000
    }
}
