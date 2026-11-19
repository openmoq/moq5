package org.openmoq.moq5.samples.playback

import java.nio.ByteBuffer

/** One platform presentation reading: [frames] had reached the output at monotonic [nanoTime]. */
internal class PlaybackAudioTimestamp(val frames: Long, val nanoTime: Long)

/**
 * The streaming AudioTrack as the renderer drives it: on the owner thread
 * only, and no call blocks. Written PCM is copied, so nothing stays borrowed
 * from the decoder.
 */
internal interface PlaybackAudioSink {
    /**
     * Copies whole frames from [bytes]' position and advances it. Returns the
     * bytes taken, 0 when the track is full, or a negative platform error.
     */
    fun write(bytes: ByteBuffer): Int

    /** Frames the track has consumed: AudioTrack's playback head, an unsigned 32-bit counter. */
    fun head(): Int

    /** The platform's latest presentation reading, or null while it has none. */
    fun timestamp(): PlaybackAudioTimestamp?

    fun play()
}

/** A playing-ready sink and the decoded PCM layout it was opened for. */
internal class PlaybackAudioOpened(val sink: PlaybackAudioSink, val sampleRate: Int, val frameBytes: Int) {
    init { require(sampleRate > 0 && frameBytes > 0) }
}

/** What the clock that leads video says about the media timeline now. */
internal sealed interface PlaybackMasterReading {
    /** Nothing has been heard yet, so there is nothing to align video to. */
    data object Pending : PlaybackMasterReading

    /** The leader is presenting relative media time [mediaNs] now. */
    class At(val mediaNs: Long) : PlaybackMasterReading

    /** The leader does not lead (stalled, failed or finished): video keeps its own time. */
    data object Detached : PlaybackMasterReading
}

/** A media clock that video follows while it leads. */
internal interface PlaybackMasterClock {
    /**
     * One reading at monotonic [nowNs], taken only when video has a frame to
     * place. [presentedNs] is the relative media time of the last frame shown,
     * or null before the first. Successive [PlaybackMasterReading.At] values
     * never decrease.
     */
    fun reading(nowNs: Long, presentedNs: Long?): PlaybackMasterReading
}

/**
 * The audio half of A/V playback, and the clock video follows.
 *
 * PCM is written to the sink without blocking. Each decoder output is held
 * until all of it is written, so a full track is ordinary backpressure. The
 * clock is the audio the device is presenting: the platform timestamp,
 * extrapolated on the monotonic clock, or the playback head before a timestamp
 * exists. It never exceeds the frames written and never moves backwards. Each
 * output's first frame records its media time when it does not continue the
 * previous one, so a gap in the audio stays a gap on the clock.
 *
 * Audio leads only while it makes progress. When the playback head has not
 * moved for [stallNs] while video waited on it, or the sink reports an error,
 * audio is detached: video falls back to its own display clock and [report]
 * says so once. A detached sink is still written without blocking, and what
 * it refuses is dropped, so a stalled device never backs up the decoders. It
 * leads again once its head has advanced [resumeNs] worth of frames without a
 * pause longer than [resumeGapNs], and only when what it presents has reached
 * the last frame shown, so video never steps back. After input ends, audio
 * that has played out, or stops moving, simply stops leading.
 *
 * Owner thread only; no call blocks or calls back into the platform reentrantly.
 * [open] creates the sink when the decoder reports its PCM format. [report]
 * must not throw.
 */
internal class PlaybackAudioOutput(
    private val open: (PlaybackOutputFormat) -> PlaybackAudioOpened,
    private val nowNs: () -> Long,
    private val report: (String) -> Unit,
    private val stallNs: Long = 2_000_000_000,
    private val resumeNs: Long = 500_000_000,
    private val resumeGapNs: Long = 250_000_000,
    private val maxAnchors: Int = 64,
    private val timestampRefreshNs: Long = 50_000_000,
    private val timestampStaleNs: Long = 1_000_000_000,
) : PlaybackMasterClock {
    init {
        require(stallNs > 0 && resumeNs > 0 && resumeGapNs > 0 && maxAnchors > 1)
        require(timestampRefreshNs > 0 && timestampStaleNs > 0)
    }

    private enum class Mode { Leading, Detached, Ended }

    /** The media time of the frame written at [frame]; later frames continue it. */
    private class Anchor(val frame: Long, val mediaNs: Long)

    private var opened: PlaybackAudioOpened? = null
    private var mode = Mode.Leading
    private val anchors = ArrayDeque<Anchor>()
    private var current: PlaybackDecodedOutput? = null
    private var inputEnded = false

    private var headRaw = 0L
    private var headWraps = 0L
    private var progressHead = 0L
    private var idleSinceNs: Long? = null
    private var resumeFromHead: Long? = null
    private var lastProgressNs = 0L
    /** How long the head stood still before its latest move. */
    private var lastPauseNs = 0L

    private var stamp: PlaybackAudioTimestamp? = null
    private var stampReadNs: Long? = null
    private var presentedFrames = 0L
    private var positionNs: Long? = null

    /** Frames accepted by the sink. */
    var writtenFrames: Long = 0
        private set

    /** PCM bytes refused while detached and dropped instead of waiting. */
    var droppedBytes: Long = 0
        private set

    var detaches: Int = 0
        private set

    var resumes: Int = 0
        private set

    /** The relative media time last presented (never decreasing); null before any. */
    val presentedMediaNs: Long? get() = positionNs

    /** True while audio leads video. */
    val leading: Boolean get() = mode == Mode.Leading

    /** Once, from the decoder's first PCM format: opens the sink and starts it playing. */
    fun configure(format: PlaybackOutputFormat) {
        check(opened == null) { "the audio output is configured once" }
        val sink = open(format)
        opened = sink
        sink.sink.play()
    }

    /**
     * Writes as much of [lease] as the sink takes now. True once all of it is
     * written, or dropped while detached: the caller then returns it to its
     * decoder. False means the sink is full; offer the same lease again later.
     */
    fun write(lease: PlaybackDecodedOutput): Boolean {
        val out = checkNotNull(opened) { "PCM arrived before the audio output was configured" }
        check(!inputEnded) { "PCM arrived after the audio input ended" }
        val bytes = requireNotNull(lease.bytes) { "PCM output has no buffer" }
        if (current !== lease) {
            if (!begin(lease)) return refused(bytes)
            current = lease
        }
        if (!bytes.hasRemaining()) return finished()
        val taken = out.sink.write(bytes)
        if (taken < 0) {
            detach("audio output failed (AudioTrack write returned $taken)")
            return refused(bytes)
        }
        if (taken % out.frameBytes != 0) throw PlaybackRenderRefused("the audio sink accepted part of a PCM frame")
        writtenFrames += taken / out.frameBytes
        if (!bytes.hasRemaining()) return finished()
        return refused(bytes)
    }

    /** The audio decoder's output has ended: no more PCM follows. */
    fun endInput() {
        inputEnded = true
        current = null
    }

    override fun reading(nowNs: Long, presentedNs: Long?): PlaybackMasterReading {
        if (mode == Mode.Ended) return PlaybackMasterReading.Detached
        val progressed = observe(nowNs)
        if (inputEnded && (presentedFrames >= writtenFrames || stalled(nowNs))) {
            mode = Mode.Ended
            PlaybackTrace.mark { "audio-clock ended frames=$presentedFrames written=$writtenFrames" }
            return PlaybackMasterReading.Detached
        }
        when (mode) {
            Mode.Leading -> {
                if (stalled(nowNs)) {
                    detach(if (opened == null || progressHead == 0L) "audio output did not start" else "audio output stalled")
                    return PlaybackMasterReading.Detached
                }
                val position = position() ?: return PlaybackMasterReading.Pending
                return PlaybackMasterReading.At(position)
            }
            Mode.Detached -> {
                if (!recovered(nowNs, progressed)) return PlaybackMasterReading.Detached
                val position = position() ?: return PlaybackMasterReading.Detached
                if (presentedNs != null && position < presentedNs) return PlaybackMasterReading.Detached
                mode = Mode.Leading
                resumes++
                idleSinceNs = null
                PlaybackTrace.mark { "audio-clock resumed position=$position presented=$presentedNs" }
                report("audio output resumed")
                return PlaybackMasterReading.At(position)
            }
            Mode.Ended -> return PlaybackMasterReading.Detached
        }
    }

    /** Diagnostic line: never an acoustic claim. */
    fun summary(): String =
        "audio-clock mode=$mode written=$writtenFrames presented=$presentedFrames head=${extendedHead()} " +
            "dropped=$droppedBytes detaches=$detaches resumes=$resumes"

    private fun finished(): Boolean {
        current = null
        return true
    }

    /**
     * A full or unusable sink: wait while leading, drop while detached. A sink
     * that stays full without consuming is a stall even when no video waits,
     * so held PCM can never back the decoders up indefinitely.
     */
    private fun refused(bytes: ByteBuffer): Boolean {
        if (mode == Mode.Leading) {
            val now = nowNs()
            observe(now)
            if (!stalled(now)) return false
            detach(if (progressHead == 0L) "audio output did not start" else "audio output stalled")
        }
        droppedBytes += bytes.remaining()
        bytes.position(bytes.limit())
        return finished()
    }

    /** Records where [lease]'s media starts on the written frames. False when no anchor can be kept. */
    private fun begin(lease: PlaybackDecodedOutput): Boolean {
        val mediaNs = nanoseconds(lease.presentationUs)
        prune()
        val last = anchors.lastOrNull()
        when {
            last == null -> anchors.addLast(Anchor(writtenFrames, mediaNs))
            last.frame == writtenFrames -> {
                // Nothing of the previous output reached the sink; this one replaces its anchor.
                anchors.removeLast()
                anchors.addLast(Anchor(writtenFrames, mediaNs))
            }
            else -> {
                val expected = last.mediaNs + framesToNs(writtenFrames - last.frame)
                if (kotlin.math.abs(mediaNs - expected) > CONTINUITY_NS) {
                    if (anchors.size >= maxAnchors) return false
                    anchors.addLast(Anchor(writtenFrames, mediaNs))
                }
            }
        }
        return true
    }

    /** Drops anchors wholly behind what is presented; the one in effect stays. */
    private fun prune() {
        while (anchors.size > 1 && anchors[1].frame <= presentedFrames) anchors.removeFirst()
    }

    private fun detach(reason: String) {
        if (mode != Mode.Leading) return
        mode = Mode.Detached
        detaches++
        resumeFromHead = null
        PlaybackTrace.mark { "audio-clock detached reason=$reason ${summary()}" }
        report("$reason; playing video only")
    }

    /** Samples the head and timestamp. True when the head advanced since the previous sample. */
    private fun observe(now: Long): Boolean {
        val out = opened
        if (out == null) {
            if (idleSinceNs == null) idleSinceNs = now
            return false
        }
        val head = readHead(out.sink)
        val progressed = head > progressHead
        if (progressed) {
            lastPauseNs = now - lastProgressNs
            progressHead = head
            idleSinceNs = null
            lastProgressNs = now
        } else if (idleSinceNs == null) {
            idleSinceNs = now
        }
        var frames = head
        val fresh = stampReadNs?.let { now - it >= timestampRefreshNs } ?: true
        if (fresh) {
            stamp = out.sink.timestamp()
            stampReadNs = now
        }
        val s = stamp
        if (s != null && s.frames > 0 && s.nanoTime <= now && now - s.nanoTime <= timestampStaleNs) {
            frames = s.frames + nsToFrames(now - s.nanoTime, out.sampleRate)
        }
        frames = minOf(frames, writtenFrames)
        if (frames > presentedFrames) presentedFrames = frames
        return progressed
    }

    private fun stalled(now: Long): Boolean = idleSinceNs?.let { now - it >= stallNs } ?: false

    /**
     * Detached: has the head advanced [resumeNs] worth of frames without a
     * pause longer than [resumeGapNs]? A pause counts whether it is still
     * going on or has just ended with this move; the count then starts again,
     * from the move that ends it.
     */
    private fun recovered(now: Long, progressed: Boolean): Boolean {
        val out = opened ?: return false
        val pause = if (progressed) lastPauseNs else now - lastProgressNs
        if (pause > resumeGapNs) {
            resumeFromHead = if (progressed) progressHead else null
            return false
        }
        val from = resumeFromHead ?: progressHead.also { resumeFromHead = it }
        return progressHead - from >= nsToFrames(resumeNs, out.sampleRate)
    }

    /** The media time being presented, or null before any written frame is presented. */
    private fun position(): Long? {
        if (presentedFrames == 0L) return null
        prune()
        val anchor = anchors.firstOrNull() ?: return null
        if (anchor.frame > presentedFrames) return null
        val media = anchor.mediaNs + framesToNs(presentedFrames - anchor.frame)
        val previous = positionNs
        val next = if (previous != null && previous > media) previous else media
        positionNs = next
        return next
    }

    /** The head as a 64-bit count. A backward step that is not a 32-bit wrap is a failure. */
    private fun readHead(sink: PlaybackAudioSink): Long {
        val raw = sink.head().toLong() and 0xffff_ffffL
        if (raw < headRaw) {
            if (headRaw >= WRAP_HIGH && raw < WRAP_LOW) headWraps++
            else throw PlaybackRenderRefused("the audio playback head moved backwards")
        }
        headRaw = raw
        return (headWraps shl 32) + raw
    }

    private fun extendedHead(): Long = (headWraps shl 32) + headRaw

    private fun framesToNs(frames: Long): Long {
        val rate = checkNotNull(opened).sampleRate.toLong()
        return (frames / rate) * NS_PER_S + (frames % rate) * NS_PER_S / rate
    }

    private fun nsToFrames(ns: Long, rate: Int): Long = (ns / NS_PER_S) * rate + (ns % NS_PER_S) * rate / NS_PER_S

    private fun nanoseconds(micros: Long): Long {
        if (micros < 0 || micros > Long.MAX_VALUE / 1000) throw PlaybackRenderRefused("media timestamp is outside the platform nanosecond range")
        return micros * 1000
    }

    private companion object {
        const val NS_PER_S = 1_000_000_000L
        /** Decoder timestamps are microseconds; anything within a millisecond continues the stream. */
        const val CONTINUITY_NS = 1_000_000L
        const val WRAP_HIGH = 0xc000_0000L
        const val WRAP_LOW = 0x4000_0000L
    }
}
