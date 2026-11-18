package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer

/**
 * The fields of one encoder output format that a track's extraction reads:
 * exactly the `android.media.MediaFormat` getters it uses, all present since
 * API 16, below this sample's floor. Borrowed for one admitted callback only;
 * nothing read through it is retained.
 *
 * [getInteger] follows the platform's contract: it raises for a key that is
 * absent or not an integer, so an optional fact is read only after
 * [containsKey] says it is there.
 */
internal interface FormatFields {
    fun containsKey(key: String): Boolean

    fun getInteger(key: String): Int

    fun getString(key: String): String?

    fun getByteBuffer(key: String): ByteBuffer?
}

/** The `MediaFormat` keys and MIME types the extraction reads, as the platform declares them. */
internal object FormatKeys {
    const val MIME = "mime"
    const val WIDTH = "width"
    const val HEIGHT = "height"
    const val BIT_RATE = "bitrate"
    const val SAMPLE_RATE = "sample-rate"
    const val CHANNEL_COUNT = "channel-count"
    const val CSD_0 = "csd-0"
    const val CSD_1 = "csd-1"
    const val VIDEO_AVC = "video/avc"
    const val AUDIO_AAC = "audio/mp4a-latm"
}

/** What reading one output format produced: an observation for the validator, or a named refusal. */
internal sealed interface Extracted<out T> {
    class Observed<T>(val observation: T) : Extracted<T>

    class Refused(val refusal: TrackFormatRefusal) : Extracted<Nothing>
}

/**
 * One output format to the observation its validator takes, or a named
 * refusal -- no validation, no utility call, no copy. The MIME type is
 * checked FIRST against the track's configured codec, so a format for
 * another codec never reaches this one's validator.
 *
 * **Facts.** Width and height, or sample rate and channel count, and the
 * diagnostic target bitrate are read from the format when present and are
 * otherwise absent -- the validator names a missing required fact; nothing
 * is substituted from a request, a target or a catalog. The declared maximum
 * bitrate is the caller's explicit input, carried unchanged over its whole
 * unsigned range and never inferred from the target.
 *
 * **Codec-specific data.** AVC needs `csd-0` then `csd-1`; AAC needs
 * `csd-0`. An absent one is a named refusal; nothing is invented. The
 * buffers are borrowed as they are, in that order, for the synchronous
 * validation that follows: its bounded snapshot reads their remaining bytes
 * through a duplicate, so their position, limit and mark are untouched, and
 * nothing is copied before its bounds are checked.
 *
 * A getter that raises -- a key of the wrong type, say -- leaves as itself:
 * it is the holder's unexpected cause, not a refusal.
 */
internal object FormatExtraction {
    fun video(fields: FormatFields, maximumBitrate: ULong?): Extracted<VideoFormatObservation> {
        mimeRefusal(fields, FormatKeys.VIDEO_AVC)?.let { return Extracted.Refused(it) }
        val sps = fields.getByteBuffer(FormatKeys.CSD_0) ?: return Extracted.Refused(TrackFormatRefusal.MissingFact("codec-specific data ${FormatKeys.CSD_0}"))
        val pps = fields.getByteBuffer(FormatKeys.CSD_1) ?: return Extracted.Refused(TrackFormatRefusal.MissingFact("codec-specific data ${FormatKeys.CSD_1}"))
        return Extracted.Observed(
            VideoFormatObservation(
                width = integer(fields, FormatKeys.WIDTH),
                height = integer(fields, FormatKeys.HEIGHT),
                targetBitrate = integer(fields, FormatKeys.BIT_RATE),
                maximumBitrate = maximumBitrate,
                config = listOf(sps, pps),
            ),
        )
    }

    fun audio(fields: FormatFields, maximumBitrate: ULong?): Extracted<AudioFormatObservation> {
        mimeRefusal(fields, FormatKeys.AUDIO_AAC)?.let { return Extracted.Refused(it) }
        val asc = fields.getByteBuffer(FormatKeys.CSD_0) ?: return Extracted.Refused(TrackFormatRefusal.MissingFact("codec-specific data ${FormatKeys.CSD_0}"))
        return Extracted.Observed(
            AudioFormatObservation(
                sampleRate = integer(fields, FormatKeys.SAMPLE_RATE),
                channelCount = integer(fields, FormatKeys.CHANNEL_COUNT),
                targetBitrate = integer(fields, FormatKeys.BIT_RATE),
                maximumBitrate = maximumBitrate,
                config = listOf(asc),
            ),
        )
    }

    private fun mimeRefusal(fields: FormatFields, expected: String): TrackFormatRefusal.Mime? {
        val actual = if (fields.containsKey(FormatKeys.MIME)) fields.getString(FormatKeys.MIME) else null
        return if (actual == expected) null else TrackFormatRefusal.Mime(expected, actual)
    }

    private fun integer(fields: FormatFields, key: String): Int? = if (fields.containsKey(key)) fields.getInteger(key) else null
}
