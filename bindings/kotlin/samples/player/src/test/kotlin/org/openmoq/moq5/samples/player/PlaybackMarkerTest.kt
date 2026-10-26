package org.openmoq.moq5.samples.player

import java.io.File
import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The Kotlin marker reader against the test-owned vectors that the Python
 * reader also consumes. Every expected outcome and index is DECLARED in
 * `marker_vectors.json` (written from the recipe geometry by
 * `build_vectors.py`), never computed by either reader.
 */
internal class MarkerVector(val name: String, val width: Int, val height: Int, val luma: ByteArray, val outcome: String, val index: Int?)

/** The accepted marker vectors, read in place from the test-owned Python tree. */
internal val markerVectors: List<MarkerVector> by lazy {
        // The path is the test task's declared input; see the player build script.
        val file = File(checkNotNull(System.getProperty("moq5.player.markerVectors")) { "moq5.player.markerVectors is not set" })
        check(file.isFile) { "marker vectors not found at ${file.path}" }
        @Suppress("UNCHECKED_CAST")
        val doc = MiniJson(file.readText()).value() as Map<String, Any?>
        check(doc["schema"] == "moq5-android-media-marker-vectors-v1") { "unexpected vector schema ${doc["schema"]}" }
        @Suppress("UNCHECKED_CAST")
        (doc["vectors"] as List<Map<String, Any?>>).map {
            @Suppress("UNCHECKED_CAST") val expected = it["expected"] as Map<String, Any?>
            MarkerVector(
                it["name"] as String, (it["width"] as Long).toInt(), (it["height"] as Long).toInt(),
                Base64.getDecoder().decode(it["luma_b64"] as String),
                expected["outcome"] as String, (expected["index"] as Long?)?.toInt(),
            )
        }
}

class PlaybackMarkerTest {
    private val vectors get() = markerVectors

    @Test fun vectorFileCoversEveryDeclaredClassItClaims() {
        // Non-vacuity: the declared set is the one the review required, so a
        // truncated or regenerated file cannot silently weaken this suite.
        val names = vectors.map { it.name }.toSet()
        for (required in listOf("index_1_one_bit", "sync_row_data_swapped", "sync_row_complement_swapped",
            "complement_bit2_equals_data", "blank_black", "blank_white", "shifted_one_cell_right",
            "shifted_down_off_rows", "margin_exact_high", "margin_short_high", "margin_exact_low", "margin_short_low",
            "index_59_final", "index_0", "index_63_all_bits", "grey_unreadable")) {
            assertTrue(required in names, "declared vector $required is missing")
        }
        assertEquals(20, vectors.size)
        assertEquals(setOf("ok", "blank", "unreadable", "sync-invalid", "complement-invalid"), vectors.map { it.outcome }.toSet())
        vectors.forEach { assertEquals(it.width * it.height, it.luma.size, it.name) }
    }

    @Test fun kotlinReaderMatchesEveryDeclaredVector() {
        val wrong = vectors.mapNotNull { v ->
            val got = PlaybackMarker.classify(v.luma, v.width, v.height)
            if (got.outcome.wire == v.outcome && got.index == v.index) null
            else "${v.name}: got ${got.outcome.wire}/${got.index}, declared ${v.outcome}/${v.index}"
        }
        assertEquals(emptyList(), wrong)
    }

    @Test fun captureRefusalsAreNamedBeforeAnyPixelIsRead() {
        assertEquals(PlaybackMarkerReading(PlaybackMarkerOutcome.CaptureNull, null), PlaybackMarker.classify(null, 64, 36))
        assertEquals(PlaybackMarkerReading(PlaybackMarkerOutcome.CaptureSize, null), PlaybackMarker.classify(ByteArray(64 * 36 - 1), 64, 36))
        assertEquals(PlaybackMarkerReading(PlaybackMarkerOutcome.CaptureEmpty, null), PlaybackMarker.classify(ByteArray(0), 0, 36))
        assertEquals(PlaybackMarkerReading(PlaybackMarkerOutcome.CaptureEmpty, null), PlaybackMarker.classify(null, 64, 0))
    }

    @Test fun lumaBytesAreUnsigned() {
        // 235 and 16 are the painted white and black; a signed read of 235 is negative.
        val white = vectors.single { it.name == "blank_white" }
        assertTrue(white.luma.all { (it.toInt() and 0xFF) == 235 })
        assertEquals(PlaybackMarkerOutcome.Blank, PlaybackMarker.classify(white.luma, white.width, white.height).outcome)
    }
}

/** A strict reader for the JSON this test consumes: objects, arrays, strings, integers, null, booleans. */
internal class MiniJson(private val text: String) {
    private var at = 0

    fun value(): Any? {
        val v = parse()
        space()
        check(at == text.length) { "trailing data at $at" }
        return v
    }

    private fun space() { while (at < text.length && text[at].isWhitespace()) at++ }

    private fun obj(): Map<String, Any?> {
        at++
        val out = LinkedHashMap<String, Any?>()
        space()
        if (text[at] == '}') { at++; return out }
        while (true) {
            space()
            val key = parse() as String
            space()
            check(text[at++] == ':') { "expected ':' at ${at - 1}" }
            check(key !in out) { "duplicate key $key" }
            out[key] = parse()
            space()
            when (text[at++]) { ',' -> continue; '}' -> return out; else -> error("bad object at ${at - 1}") }
        }
    }

    private fun arr(): List<Any?> {
        at++
        val out = ArrayList<Any?>()
        space()
        if (text[at] == ']') { at++; return out }
        while (true) {
            out += parse()
            space()
            when (text[at++]) { ',' -> continue; ']' -> return out; else -> error("bad array at ${at - 1}") }
        }
    }

    private fun parse(): Any? {
        space()
        check(at < text.length) { "unexpected end" }
        return when (val c = text[at]) {
            '{' -> obj()
            '[' -> arr()
            '"' -> {
                at++; val sb = StringBuilder()
                while (true) {
                    val ch = text[at++]
                    if (ch == '"') break
                    check(ch != '\\') { "escapes are not used by this file" }
                    sb.append(ch)
                }
                sb.toString()
            }
            'n' -> { check(text.startsWith("null", at)); at += 4; null }
            't' -> { check(text.startsWith("true", at)); at += 4; true }
            'f' -> { check(text.startsWith("false", at)); at += 5; false }
            else -> {
                check(c == '-' || c.isDigit()) { "unexpected '$c' at $at" }
                val start = at; at++
                while (at < text.length && text[at].isDigit()) at++
                text.substring(start, at).toLong()
            }
        }
    }
}
