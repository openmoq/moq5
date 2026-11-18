package org.openmoq.moq5.samples.publisher.host

import java.nio.ByteBuffer
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.TrackName
import org.openmoq.moq5.samples.publisher.AudioFormatObservation
import org.openmoq.moq5.samples.publisher.ConfigBounds
import org.openmoq.moq5.samples.publisher.FormatAuthority
import org.openmoq.moq5.samples.publisher.PublicAacSignaling
import org.openmoq.moq5.samples.publisher.TrackFormatVerdict
import org.openmoq.moq5.samples.publisher.TrackFormats
import org.openmoq.moq5.samples.publisher.UtilityOutcome
import org.openmoq.moq5.samples.publisher.VideoFormatObservation

/**
 * The format validator through the REAL public binding, in a JVM where the
 * host image is present (the `testHostCodec` task). The independent native
 * controls these rows rely on are `TrackFormatHostControlTest`, in their own
 * JVM.
 *
 * The host image's codec recorder holds 16 native calls per JVM; this class
 * makes 9: t1 2 (size query and exact call), t2 1 (a refusal answers the
 * size query), t3 4 (build 2, format 2), t4 2.
 */
class TrackFormatHostIntegrationTest {

    private fun hex(s: String): ByteArray = ByteArray(s.length / 2) { s.substring(2 * it, 2 * it + 2).toInt(16).toByte() }

    private fun cat(vararg parts: ByteArray): ByteArray = parts.fold(ByteArray(0)) { a, b -> a + b }

    private val sc = hex("00000001")
    private val spsA = hex("6742001eabcd")
    private val ppsA = hex("68ce3c80")
    private val avcc = hex("0142001effe100066742001eabcd01000468ce3c80")
    private val asc441 = hex("1210")
    private val bounds = ConfigBounds(maxUnits = 4, maxUnitBytes = 4096, maxTotalBytes = 8192)

    @Test
    fun `t1 the production AAC adapter formats exactly that string`() {
        assertEquals("mp4a.40.2", assertIs<UtilityOutcome.Done<String>>(PublicAacSignaling.formatCodecString(MediaBytes(asc441))).value)
    }

    @Test
    fun `t2 the production AAC adapter turns a real native refusal into an outcome`() {
        // One byte ends inside the sampling frequency index.
        assertEquals(-3, assertIs<UtilityOutcome.Refused>(PublicAacSignaling.formatCodecString(MediaBytes(hex("12")))).code.value, "PROTO")
    }

    @Test
    fun `t3 a real video format carries the real record, string and retained sets`() {
        val observed = VideoFormatObservation(640, 480, 1_000_000, 1_500_000uL, listOf(ByteBuffer.wrap(cat(sc, spsA)), ByteBuffer.wrap(cat(sc, ppsA))))
        val format = assertIs<TrackFormatVerdict.Complete>(TrackFormats.video(TrackName("video".encodeToByteArray()), observed, bounds)).format
        assertEquals("avc1.42001e", format.config.codec.bytes().decodeToString())
        assertEquals(1_500_000uL, format.config.bitrate, "the declared maximum, not the target")
        assertContentEquals(avcc, assertIs<MediaBytes>(format.config.initData).bytes())
        val authority = assertIs<FormatAuthority.Avc>(format.authority).config
        assertContentEquals(avcc, authority.record.bytes())
        assertContentEquals(spsA, authority.parameterSets.sequenceParameterSet(0))
        assertContentEquals(ppsA, authority.parameterSets.pictureParameterSet(0))
    }

    @Test
    fun `t4 a real audio format carries the real string and the supported facts`() {
        val observed = AudioFormatObservation(44_100, 2, 96_000, 128_000uL, listOf(ByteBuffer.wrap(asc441)))
        val format = assertIs<TrackFormatVerdict.Complete>(TrackFormats.audio(TrackName("audio".encodeToByteArray()), observed, bounds)).format
        assertEquals("mp4a.40.2", format.config.codec.bytes().decodeToString())
        assertContentEquals(asc441, assertIs<MediaBytes>(format.config.initData).bytes())
        assertEquals(44_100, assertIs<FormatAuthority.Aac>(format.authority).facts.sampleRate)
    }
}
