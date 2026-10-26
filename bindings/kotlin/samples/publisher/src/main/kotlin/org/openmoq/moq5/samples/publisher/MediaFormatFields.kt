package org.openmoq.moq5.samples.publisher

import android.media.MediaFormat
import java.nio.ByteBuffer

/**
 * The platform's actual output format, read through the same [FormatFields]
 * view -- and so the same extraction and holder path -- the host rows
 * exercise. It is a borrowed view for one callback: nothing keeps it.
 */
internal class MediaFormatFields(private val format: MediaFormat) : FormatFields {
    override fun containsKey(key: String): Boolean = format.containsKey(key)

    override fun getInteger(key: String): Int = format.getInteger(key)

    override fun getString(key: String): String? = format.getString(key)

    override fun getByteBuffer(key: String): ByteBuffer? = format.getByteBuffer(key)
}

/** The video encoder's `onOutputFormatChanged`, through its holder; read only once admitted. */
internal fun VideoFormatHolder.onOutputFormat(format: MediaFormat, maximumBitrate: ULong?): FormatOutcome = onOutputFormat(MediaFormatFields(format), maximumBitrate)

/** The audio encoder's `onOutputFormatChanged`, through its holder; read only once admitted. */
internal fun AudioFormatHolder.onOutputFormat(format: MediaFormat, maximumBitrate: ULong?): FormatOutcome = onOutputFormat(MediaFormatFields(format), maximumBitrate)
