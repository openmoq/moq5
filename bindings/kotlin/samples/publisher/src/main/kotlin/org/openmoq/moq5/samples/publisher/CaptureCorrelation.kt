package org.openmoq.moq5.samples.publisher

import java.math.BigInteger

/**
 * Where a capture instant comes from, given that the encoder does not supply
 * one.
 *
 * The encoder's asynchronous callback is
 * `onOutputBufferAvailable(codec, index, BufferInfo)`, and `BufferInfo` carries
 * only `offset`, `size`, `presentationTimeUs` and `flags`. There is no camera
 * or microphone instant in it. So the capture instant has to be OBSERVED on the
 * acquisition side and CLAIMED on the output side, and the only thing the two
 * sides share is the media timestamp.
 *
 * What the installed API (android-36.1 sources) actually promises:
 *
 *  * `MediaCodec.BufferInfo.presentationTimeUs` is "derived from the
 *    presentation timestamp passed in with the corresponding input buffer".
 *  * `CaptureResult.SENSOR_TIMESTAMP` is the exposure start of the first row,
 *    in nanoseconds, and "the timestamps are also included in all image buffers
 *    produced for the same capture, and will be identical on all the outputs".
 *  * With `SENSOR_INFO_TIMESTAMP_SOURCE == REALTIME` that value is on
 *    `SystemClock.elapsedRealtimeNanos`, which is the base the accepted
 *    `PlatformCaptureClock` is calibrated against.
 *
 * So the two tracks are NOT equally well founded, and this file says so rather
 * than flattening them:
 *
 *  * AUDIO has a key this sample OWNS -- the application chooses the input
 *    `presentationTimeUs` when it queues recorded frames -- but owning the
 *    input key is not the same as the output carrying it. `MediaCodec.java`
 *    141-156 says an encoded audio output buffer "is normally a single access
 *    unit" and "may contain multiple encoded access units", and the output
 *    timestamp is "derived from" the input's. Neither one-output-per-input nor
 *    output-pts-equals-input-pts is promised. [AudioKeying] below declares the
 *    framing this sample uses to make the map well defined, and what it
 *    refuses; the exact equality on a given encoder is device evidence.
 *  * VIDEO through a codec input surface is NOT documented to that standard.
 *    The camera stamps the buffer with `SENSOR_TIMESTAMP`; the encoder derives
 *    an output `presentationTimeUs` from the corresponding input. The installed
 *    API does not state that the derivation is the identity, nor whether the
 *    nanosecond-to-microsecond conversion truncates or rounds. That exact fact
 *    is DEVICE evidence and is listed as such.
 *
 * The ledger's "claiming a key discards strictly older observations" rule is
 * conditional on this sample's declared no-reordering encoder contract; it is
 * not a fact about encoders in general.
 *
 * The contract below is therefore key-agnostic and refuses rather than guesses:
 * an output whose key matches no observation is a NAMED refusal. No arrival
 * time, no output ordinal, and no nearest-neighbour search may stand in for the
 * missing fact.
 */
internal class CaptureObservation(
    /**
     * The media timestamp this observation is filed under, in microseconds on
     * the same timeline the encoder will report. Audio owns this value
     * directly; video derives it from the camera's own frame timestamp.
     */
    val key: Long,
    /**
     * The platform capture instant, on the elapsed-realtime base the accepted
     * `PlatformCaptureClock` is calibrated against.
     */
    val captureElapsedNanos: Long,
)

/** What recording one observation did. */
internal sealed interface ObserveOutcome {
    data object Recorded : ObserveOutcome

    /** A duplicate key, a full ledger, or an observation on the wrong track. */
    data class Refused(val reason: String) : ObserveOutcome

    /** The generation retired; nothing is recorded. */
    data class Ignored(val reason: String) : ObserveOutcome
}

/** What claiming an observation for one encoder output did. */
internal sealed interface ClaimOutcome {
    data class Matched(val captureElapsedNanos: Long) : ClaimOutcome

    /**
     * No observation is filed under this key: it was never observed, it was
     * already consumed, or it was discarded as older than one already claimed.
     * This is a refusal, never a fallback.
     */
    data class Missing(val reason: String) : ClaimOutcome

    data class Ignored(val reason: String) : ClaimOutcome
}

/**
 * One track's capture observations, consumed at most once each.
 *
 * The ledger is ORDERED by key and self-cleaning: claiming a key discards every
 * strictly older observation, because an encoder that has emitted a later frame
 * will never ask for an earlier one. Without that, one frame the encoder
 * dropped would hold a slot for the life of the broadcast.
 *
 * Implementations are judged by WHICH observation a claim returned and by what
 * remains afterwards -- never by how many were recorded.
 */
internal interface CaptureLedger {
    val kind: MediaKind

    val generation: Long

    /** The bound on outstanding observations. A full ledger refuses by name. */
    val capacity: Int

    /** Observations recorded and not yet consumed or discarded. */
    val outstanding: Int

    fun observe(generation: Long, kind: MediaKind, observation: CaptureObservation): ObserveOutcome

    fun claim(generation: Long, kind: MediaKind, key: Long): ClaimOutcome

    fun retire(reason: String)
}

/**
 * The ledger this sample runs.
 *
 * **Bounded state.** At most [capacity] observations, plus constant
 * bookkeeping: one SETTLED-KEY boundary. There is no set of consumed keys --
 * that would grow for the life of the broadcast. Under the declared
 * no-reordering contract the boundary is enough: a successful claim of `k`
 * means the encoder has emitted `k`, so no key at or below `k` can ever be
 * legitimately observed again, and every key at or below it is either consumed
 * or discarded as older. A key at or below the boundary is therefore refused as
 * STALE, whether it was claimed or discarded.
 *
 * A MISSING claim moves nothing: it is not a watermark, and the observations it
 * did not match stay exactly where they were.
 *
 * The map is ordered by KEY, never by the order callbacks arrive in: an
 * observation for a later key may be recorded before an earlier one, and both
 * remain independently claimable.
 *
 * Everything is guarded by one monitor, so acquisition callbacks and encoder
 * output callbacks may call it from different threads. It calls nothing back,
 * sleeps for nothing, and touches no platform.
 */
internal class BoundedCaptureLedger(
    override val kind: MediaKind,
    override val generation: Long,
    override val capacity: Int,
) : CaptureLedger {
    private val lock = Any()
    private val byKey = sortedMapOf<Long, Long>()

    /** The highest key a claim has consumed; nothing at or below it is observable. */
    private var settled: Long? = null
    private var retired = false

    override val outstanding: Int get() = synchronized(lock) { byKey.size }

    override fun observe(generation: Long, kind: MediaKind, observation: CaptureObservation): ObserveOutcome =
        synchronized(lock) {
            if (retired) return ObserveOutcome.Ignored("this generation has retired")
            if (generation != this.generation) return ObserveOutcome.Ignored("not generation ${this.generation}")
            if (kind != this.kind) return ObserveOutcome.Refused("this ledger is the $kind track's")
            settled?.let { boundary ->
                if (observation.key <= boundary) {
                    return ObserveOutcome.Refused(
                        "key ${observation.key} is at or below the settled key $boundary: that media has already been published or discarded",
                    )
                }
            }
            if (byKey.containsKey(observation.key)) return ObserveOutcome.Refused("key ${observation.key} is already observed")
            if (byKey.size >= capacity) return ObserveOutcome.Refused("this ledger holds $capacity observations")
            byKey[observation.key] = observation.captureElapsedNanos
            ObserveOutcome.Recorded
        }

    override fun claim(generation: Long, kind: MediaKind, key: Long): ClaimOutcome =
        synchronized(lock) {
            if (retired) return ClaimOutcome.Ignored("this generation has retired")
            if (generation != this.generation) return ClaimOutcome.Ignored("not generation ${this.generation}")
            if (kind != this.kind) return ClaimOutcome.Missing("this ledger is the $kind track's")
            // EXACT key only. A near miss is a different frame, and a miss
            // changes nothing at all -- not the map, not the boundary.
            val nanos = byKey.remove(key) ?: return ClaimOutcome.Missing("no observation for $key")
            // The encoder has emitted this key, so under the no-reordering
            // contract it will never ask for an earlier one.
            byKey.headMap(key).keys.toList().forEach { byKey.remove(it) }
            val boundary = settled
            if (boundary == null || key > boundary) settled = key
            ClaimOutcome.Matched(nanos)
        }

    override fun retire(reason: String) {
        synchronized(lock) {
            retired = true
            byKey.clear()
        }
    }
}

/**
 * The audio key authority: how a recorded PCM buffer becomes an observation
 * whose key the encoder's output can claim.
 *
 * Declared framing:
 *
 *  * the microphone is read in UNITS of exactly [framesPerUnit] PCM frames --
 *    one encoder access unit, whose length is supplied from the verified
 *    configuration (1024 is the only length this sample accepts; it is not
 *    a consequence of AAC-LC alone). A read that returns fewer
 *    frames is ACCUMULATED, not queued; a unit is queued only when complete;
 *  * unit `i` is queued with `presentationTimeUs = keyFor(i)`, where the key
 *    is `originUs + i * framesPerUnit * 1_000_000 / sampleRate`, computed in
 *    integer arithmetic with the remainder carried so the keys never drift;
 *  * the observation for unit `i` is its FIRST frame's capture instant, from
 *    the accepted `AudioFrameClock` over the `AudioTimestamp` the platform
 *    reported -- never a read-return time;
 *  * ONE ENCODED OUTPUT PER UNIT is a restriction this sample IMPOSES, not a
 *    fact it has. An output's key either matches an outstanding observation --
 *    which it then claims -- or it matches nothing, which is a Correlation
 *    refusal. Nothing is inferred from a near miss.
 *
 * **What a coalesced output would do, and why this sample will not pretend
 * otherwise.** `MediaCodec` says an encoded audio buffer "is normally a single
 * access unit... but this requirement is slightly relaxed in that a buffer may
 * contain multiple encoded access units of audio", and gives the buffer ONE
 * `presentationTimeUs` "derived from" its input. If such a buffer arrived here,
 * its key could match the FIRST unit in it, and the observations for the units
 * behind that key would be LATER than it, not older -- so the ledger's
 * strictly-older discard cannot remove them at that moment, and must not.
 *
 * **The ledger does not DETECT that, and this file does not pretend it does.**
 * Those leftovers do not accumulate to the bound and do not produce a refusal:
 * the NEXT output's claim carries a later key, and the strictly-older rule
 * discards them then. Aggregation would therefore be invisible here -- no
 * fault, no refusal, just units that never publish. Nothing is mis-stamped,
 * and nothing is reported either.
 *
 * This sample also cannot tell that case from a unit the encoder simply
 * dropped: distinguishing them means counting access units inside the encoded
 * buffer -- parsing ADTS/LATM framing -- which is deliberately outside this
 * sample's scope, and not every encoded stream carries either wrapper anyway.
 * One access unit per output therefore remains a CODEC AND CONFIGURATION
 * qualification condition, established on a device, not an invariant the ledger
 * can enforce. Unknown access-unit membership stays unknown: it is never
 * inferred from a timestamp, nor from how full the queue happens to be.
 *
 * A rate whose unit duration is not a whole number of microseconds is NOT a
 * reason to refuse: 44.1 kHz is the ordinary case, and the carried remainder is
 * exactly how it is supported. What IS refused, by name: a sample rate or unit
 * size that is not positive, a negative unit index, a units-per-access-unit
 * ratio other than one, a PCM encoding other than 16-bit, and a unit index
 * whose key is not representable in Long microseconds -- a wrapped key would
 * file an observation under a timestamp no output can ever carry.
 */
internal sealed interface AudioKeyOutcome {
    data class Key(val presentationTimeUs: Long) : AudioKeyOutcome

    data class Refused(val reason: String) : AudioKeyOutcome
}

internal interface AudioKeying {
    val sampleRate: Int

    val framesPerUnit: Int

    val originUs: Long

    /** The presentation timestamp unit [unitIndex] is queued with, and observed under. */
    fun keyFor(unitIndex: Long): AudioKeyOutcome

    /**
     * Feeds one platform read of [frames] PCM frames. Returns the indices of the
     * units that became COMPLETE with this read, in order; a partial unit is
     * retained until the next read completes it.
     *
     * This returns a list, so it has no outcome channel to refuse through. The
     * two inputs it cannot serve raise `IllegalArgumentException` instead, and
     * change NOTHING when they do: a negative frame count, and a read whose
     * completed units would advance the unit index past `Long.MAX_VALUE`.
     */
    fun onRead(frames: Int): List<Long>

    /** Frames retained and not yet forming a complete unit. */
    val retainedFrames: Int
}

/**
 * The keying this sample runs.
 *
 * **Exact integer arithmetic, no floating point.** A unit's key is
 * `originUs + unitIndex * framesPerUnit * 1_000_000 / sampleRate`, floored
 * ONCE over the whole expression -- never a per-unit duration rounded and then
 * multiplied, which drifts (at 44.1 kHz, 95 us per hundred units).
 *
 * Only the FINAL timestamp's representability decides. An intermediate value
 * overflowing a Long says nothing: a frame count past `Long.MAX_VALUE` can
 * still divide down to an ordinary key, and a duration past it can still land
 * in range once a negative origin is added. So the whole expression is
 * evaluated exactly, in arbitrary precision, and only the result is range
 * checked. `java.math.BigInteger` is the standard library and the operand
 * widths bound the expression to a handful of words; that bound is the whole
 * justification, and no claim is made about how often this is called.
 *
 * What this class does NOT establish: that a codec's output
 * `presentationTimeUs` equals the input it was queued with, or whether an
 * output carried more than one access unit. Both remain device and codec
 * qualification facts, and nothing here parses a bitstream or substitutes a
 * stamp for them.
 */
internal class UnitAudioKeying(
    override val sampleRate: Int,
    override val framesPerUnit: Int,
    override val originUs: Long,
) : AudioKeying {
    private val lock = Any()
    private var retained = 0
    private var nextUnit = 0L

    override val retainedFrames: Int get() = synchronized(lock) { retained }

    override fun keyFor(unitIndex: Long): AudioKeyOutcome {
        if (sampleRate <= 0) return AudioKeyOutcome.Refused("a sample rate of $sampleRate places no unit")
        if (framesPerUnit <= 0) return AudioKeyOutcome.Refused("a unit of $framesPerUnit frames places no unit")
        if (unitIndex < 0L) return AudioKeyOutcome.Refused("unit $unitIndex is before the origin")
        // Every operand here is non-negative, so the truncating divide IS the
        // floor, and it happens once, over the whole offset.
        val offset =
            BigInteger.valueOf(unitIndex)
                .multiply(BigInteger.valueOf(framesPerUnit.toLong()))
                .multiply(MICROS_PER_SECOND)
                .divide(BigInteger.valueOf(sampleRate.toLong()))
        val key = BigInteger.valueOf(originUs).add(offset)
        if (key < LONG_MIN || key > LONG_MAX) return unrepresentable(unitIndex)
        return AudioKeyOutcome.Key(key.toLong())
    }

    override fun onRead(frames: Int): List<Long> {
        require(frames >= 0) { "a read cannot return $frames frames" }
        return synchronized(lock) {
            if (framesPerUnit <= 0) return@synchronized emptyList()
            // Widened BEFORE anything is mutated: neither the accumulation nor
            // the index may advance on an input this cannot represent.
            val total = retained.toLong() + frames.toLong()
            val completed = total / framesPerUnit
            val last = sum(nextUnit, completed)
            require(last != null) { "unit index $nextUnit + $completed is not representable" }
            val indices = (0 until completed).map { nextUnit + it }
            nextUnit += completed
            retained = (total % framesPerUnit).toInt()
            indices
        }
    }

    private fun unrepresentable(unitIndex: Long): AudioKeyOutcome =
        AudioKeyOutcome.Refused("unit $unitIndex has no key in Long microseconds: the exact value would overflow")

    private fun sum(a: Long, b: Long): Long? =
        try {
            Math.addExact(a, b)
        } catch (raised: ArithmeticException) {
            null
        }

    private companion object {
        val MICROS_PER_SECOND: BigInteger = BigInteger.valueOf(1_000_000L)
        val LONG_MIN: BigInteger = BigInteger.valueOf(Long.MIN_VALUE)
        val LONG_MAX: BigInteger = BigInteger.valueOf(Long.MAX_VALUE)
    }
}
