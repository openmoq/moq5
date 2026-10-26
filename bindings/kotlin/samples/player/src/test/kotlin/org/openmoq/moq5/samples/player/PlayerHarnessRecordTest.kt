package org.openmoq.moq5.samples.player

import org.openmoq.moq5.MediaType
import org.openmoq.moq5.PackagingKind
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs

/**
 * The catalog record the acceptance harness's publisher declares: the avcC the public codec-signaling
 * utility builds from the SPS/PPS of the generated input's first sync object
 * (bindings/python/tests/runtime/android_media/make_input.py, marker_recipe.json). The harness checks
 * that its publisher derives exactly [RECORD_HEX]; this pins that the player's format contract accepts
 * exactly that record for the declared track, and refuses the same track without it.
 */
class PlayerHarnessRecordTest {
    private fun hex(text: String) = text.chunked(2).map { it.toInt(16).toByte() }.toByteArray()

    private val record = hex(RECORD_HEX)
    private val sps = hex("6742c01ed90141fb011000000300100000030300f162e480")
    private val pps = hex("68cb83cb20")

    private fun facts(config: ByteArray?) =
        TrackFacts(MediaType.Video, PackagingKind.Raw, "avc1.42C01E", config, timescale = 90000u, width = 320u, height = 240u)

    @Test
    fun theHarnessRecordConfiguresTheDeclaredVideoDecoder() {
        val video = assertIs<FormatOutcome.Video>(DecoderFormats.of(facts(record)))
        assertEquals("video/avc", video.mime)
        assertEquals(320 to 240, video.width to video.height)
        assertEquals(PackagingKind.Raw, video.packaging)
        assertEquals(2, video.csd.size)
        assertContentEquals(MediaContract.START_CODE + sps, video.csd[0])
        assertContentEquals(MediaContract.START_CODE + pps, video.csd[1])
    }

    @Test
    fun theSameTrackWithoutTheRecordIsRefused() {
        assertEquals(
            FormatOutcome.Unsupported("the track carries no codec configuration for avc1.42C01E"),
            DecoderFormats.of(facts(null)),
        )
    }

    companion object {
        const val RECORD_HEX = "0142c01effe100186742c01ed90141fb011000000300100000030300f162e48001000568cb83cb20"
    }
}
