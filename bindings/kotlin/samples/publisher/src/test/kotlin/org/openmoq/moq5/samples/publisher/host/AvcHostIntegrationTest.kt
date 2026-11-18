package org.openmoq.moq5.samples.publisher.host

import java.nio.ByteBuffer
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import org.openmoq.moq5.CodecConfigFormat
import org.openmoq.moq5.CodecConfigSource
import org.openmoq.moq5.CodecSignaling
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.SampleEntry
import org.openmoq.moq5.samples.publisher.AvcConfigVerdict
import org.openmoq.moq5.samples.publisher.AvcConfiguration
import org.openmoq.moq5.samples.publisher.AvcPayloadLimits
import org.openmoq.moq5.samples.publisher.AvcPayloadRefusal
import org.openmoq.moq5.samples.publisher.AvcPayloadVerdict
import org.openmoq.moq5.samples.publisher.AvcPayloads
import org.openmoq.moq5.samples.publisher.ConfigBounds
import org.openmoq.moq5.samples.publisher.PublicCodecSignaling
import org.openmoq.moq5.samples.publisher.RawConfigSnapshot
import org.openmoq.moq5.samples.publisher.RawConfigSnapshots
import org.openmoq.moq5.samples.publisher.RawSnapshotResult
import org.openmoq.moq5.samples.publisher.UtilityOutcome
import org.openmoq.moq5.samples.publisher.UtilityStage

/**
 * The AVC configuration step through the REAL public binding and its
 * process-wide native owner, in a JVM where the host image is present (the
 * `testHostCodec` task). Nothing is scripted: the record and string are what
 * the native utility returns. `h0` is the independent control that the image
 * and utility work at all; the other rows go through the sample's own paths.
 *
 * The host image's codec recorder holds 16 native calls per JVM, and this
 * class makes exactly 16 -- a successful build or format is a size query and
 * an exact call, a refusal answers the size query: h0 4, h1 4, h1b 4 (its
 * conversions make none), h2 1, h3 1, h4 2. Another native call here needs
 * its own class.
 */
class AvcHostIntegrationTest {

    private fun hex(s: String): ByteArray = ByteArray(s.length / 2) { s.substring(2 * it, 2 * it + 2).toInt(16).toByte() }

    private fun cat(vararg parts: ByteArray): ByteArray = parts.fold(ByteArray(0)) { a, b -> a + b }

    private val sc = hex("00000001")
    private val spsA = hex("6742001eabcd")
    private val ppsA = hex("68ce3c80")

    /** avcC by field: version 1, SPS bytes 1..3, 0xfc|(4-1), 0xe0|1, length 6, SPS, 1, length 4, PPS. */
    private val avcc = hex("0142001effe100066742001eabcd01000468ce3c80")

    private val bounds = ConfigBounds(maxUnits = 4, maxUnitBytes = 4096, maxTotalBytes = 8192)

    private fun snapshot(vararg units: ByteArray): RawConfigSnapshot =
        assertIs<RawSnapshotResult.Taken>(RawConfigSnapshots.capture(units.map { ByteBuffer.wrap(it) }, bounds), "setup: the snapshot is taken").snapshot

    @Test
    fun `h0 control - the public binding builds and formats the reviewed record in this JVM`() {
        val record = CodecSignaling.buildInitData(CodecConfigSource.AvcAnnexB, MediaBytes(cat(sc, spsA, sc, ppsA)), 4)
        assertContentEquals(avcc, record.bytes())
        assertEquals("avc1.42001e", CodecSignaling.formatCodecString(CodecConfigFormat.AvcRecord, SampleEntry("avc1"), record))
    }

    @Test
    fun `h1 the sample resolves csd-0 and csd-1 to the real record and its string`() {
        val c = assertIs<AvcConfigVerdict.Resolved>(AvcConfiguration.resolve(snapshot(cat(sc, spsA), cat(sc, ppsA)))).config
        assertContentEquals(avcc, c.record.bytes(), "the record the native utility built")
        assertEquals("avc1.42001e", c.codecString, "formatted from that record")
        assertEquals(1, c.sequenceParameterSets)
        assertEquals(1, c.pictureParameterSets)
    }

    @Test
    fun `h1b the real resolution's retained sets strip identical in-band sets from a frame`() {
        val c = assertIs<AvcConfigVerdict.Resolved>(AvcConfiguration.resolve(snapshot(cat(sc, spsA), cat(sc, ppsA)))).config
        assertContentEquals(avcc, c.record.bytes(), "the real record, as in h1")
        assertContentEquals(spsA, c.parameterSets.sequenceParameterSet(0))
        assertContentEquals(ppsA, c.parameterSets.pictureParameterSet(0))
        val idr = hex("65888421")
        val limits = AvcPayloadLimits(maxNalUnits = 16, maxOutputBytes = 4096)
        val v = assertIs<AvcPayloadVerdict.Converted>(AvcPayloads.convert(MediaBytes(cat(sc, spsA, sc, ppsA, sc, idr)), c.parameterSets, limits))
        assertContentEquals(hex("0000000465888421"), v.payload.bytes(), "only the slice, after its length")
        assertEquals(1, v.strippedSequenceParameterSets)
        assertEquals(1, v.strippedPictureParameterSets)
        assertEquals(10L, v.strippedParameterSetBytes, "6 + 4 NAL bytes, headers included, start codes excluded")
        val other = AvcPayloads.convert(MediaBytes(cat(sc, hex("6742001fabcd"), sc, ppsA, sc, idr)), c.parameterSets, limits)
        assertEquals(AvcPayloadRefusal.UnknownParameterSet(0, 7), assertIs<AvcPayloadVerdict.OutsideSubset>(other).refusal)
    }

    @Test
    fun `h2 a short SPS inside the sample subset is refused by the real builder, as a verdict`() {
        // 67 42 1e: nal_ref_idc 3, type 7, three bytes. The sample's checks
        // accept it; the builder needs profile, constraint and level bytes.
        val v = assertIs<AvcConfigVerdict.UtilityRefused>(AvcConfiguration.resolve(snapshot(cat(sc, hex("67421e")), cat(sc, ppsA))))
        assertEquals(UtilityStage.BUILD, v.stage)
        assertEquals(-3, v.code.value, "PROTO")
    }

    @Test
    fun `h3 the adapter turns a real native build refusal into an outcome`() {
        // An SPS with no PPS configures no out-of-band decoder.
        val o = PublicCodecSignaling.buildInitData(CodecConfigSource.AvcAnnexB, MediaBytes(cat(sc, spsA)), 4)
        assertEquals(-3, assertIs<UtilityOutcome.Refused>(o).code.value, "PROTO")
    }

    @Test
    fun `h4 the adapter turns real native format refusals into outcomes`() {
        val short = PublicCodecSignaling.formatCodecString(CodecConfigFormat.AvcRecord, SampleEntry("avc1"), MediaBytes(hex("01")))
        assertEquals(-3, assertIs<UtilityOutcome.Refused>(short).code.value, "a record shorter than four bytes is PROTO")
        val entry = PublicCodecSignaling.formatCodecString(CodecConfigFormat.AvcRecord, SampleEntry("mp4a"), MediaBytes(avcc))
        assertEquals(-2, assertIs<UtilityOutcome.Refused>(entry).code.value, "an entry that does not match the format is INVAL")
    }
}
