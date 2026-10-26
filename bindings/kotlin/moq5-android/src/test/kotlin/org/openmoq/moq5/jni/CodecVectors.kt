package org.openmoq.moq5.jni

/**
 * The reviewed native codec vectors, carried forward VERBATIM as binding
 * expectations from `media/codec_signaling/tests/test_codec_signaling_spec.c`
 * (`k_asc_lc`, `k_avc_annexb`, `k_avcc`, and the `avc3.42001e` and `mp4a.40.2`
 * rows). They prove the Kotlin result equals the reviewed C contract; they are
 * not a renewed certification of the codec formats.
 */
internal object CodecVectors {
    fun bytes(vararg v: Int): ByteArray = ByteArray(v.size) { v[it].toByte() }

    /** AAC-LC AudioSpecificConfig. */
    val ASC_LC: ByteArray = bytes(0x12, 0x10)

    /** The same leading fields with frameLengthFlag set: the codec string cannot tell them apart. */
    val ASC_LC_960: ByteArray = bytes(0x12, 0x14)

    /** AVC Annex-B Baseline SPS + PPS. */
    val AVC_ANNEXB: ByteArray =
        bytes(
            0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1e, 0xab, 0xcd,
            0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x3c, 0x80,
        )

    /** The avcC they assemble into, with a four-byte NAL length. */
    val AVCC: ByteArray =
        bytes(
            0x01, 0x42, 0x00, 0x1e, 0xff, 0xe1, 0x00, 0x06,
            0x67, 0x42, 0x00, 0x1e, 0xab, 0xcd,
            0x01, 0x00, 0x04, 0x68, 0xce, 0x3c, 0x80,
        )

    /** An SPS with no PPS: structurally incomplete for an out-of-band record. */
    val AVC_SPS_ONLY: ByteArray = bytes(0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1e, 0xab, 0xcd)

    /** Seventeen SPS NALs and one PPS: more parameter sets than the builder handles. */
    val AVC_SEVENTEEN_SPS: ByteArray =
        (List(17) { bytes(0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1e) }.fold(ByteArray(0)) { a, b -> a + b }) +
            bytes(0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x3c, 0x80)

    const val MOQ_OK = 0
    const val MOQ_ERR_NOMEM = -1
    const val MOQ_ERR_INVAL = -2
    const val MOQ_ERR_PROTO = -3
    const val MOQ_ERR_BUFFER = -9
    const val MOQ_ERR_UNSUPPORTED = -14
    const val MOQ_ERR_INTERNAL = -99
}

/** One recorded codec call, as the host recorder logged it. */
internal data class CodecCall(
    val kind: Long,
    val format: Long,
    val lengthSize: Long,
    val hasOti: Long,
    val oti: Long,
    val cap: Long,
    val rc: Long,
    val outLen: Long,
    val inputLen: Long,
    val entryLen: Long,
    val injected: Long,
) {
    companion object {
        fun all(): List<CodecCall> {
            val raw = TrustRouteProbe.nativeCodecCalls()
            val f = TrustRouteProbe.CODEC_FIELDS
            return (0 until raw.size / f).map { i ->
                val r = raw.copyOfRange(i * f, i * f + f)
                CodecCall(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10])
            }
        }
    }
}

/** One declared codec call: every recorded field, plus the exact input bytes and (for a string call) sample-entry bytes. */
internal class ExpectedCall(val call: CodecCall, val input: ByteArray, val entry: ByteArray = ByteArray(0))

/**
 * The binding-fidelity oracle: EVERY recorded call equals its declaration in
 * every field, and its recorded input and entry bytes equal the declared ones,
 * in order, with nothing extra.
 */
internal fun expectCodecCalls(expected: List<ExpectedCall>, label: String) {
    val actual = CodecCall.all()
    kotlin.test.assertEquals(expected.map { it.call }, actual, "$label: every field of every call, in order")
    expected.forEachIndexed { i, e ->
        kotlin.test.assertContentEquals(e.input, TrustRouteProbe.nativeCodecBytes(i, false), "$label: call $i input bytes")
        kotlin.test.assertContentEquals(e.entry, TrustRouteProbe.nativeCodecBytes(i, true), "$label: call $i sample-entry bytes")
    }
}

/** A declared size query and build pair for one operation. */
internal fun queryAndBuild(
    kind: Long,
    format: Long,
    lengthSize: Long,
    outLen: Long,
    input: ByteArray,
    entry: ByteArray = ByteArray(0),
    hasOti: Long = 0,
    oti: Long = 0,
): List<ExpectedCall> =
    listOf(
        ExpectedCall(CodecCall(kind, format, lengthSize, hasOti, oti, 0, CodecVectors.MOQ_ERR_BUFFER.toLong(), outLen, input.size.toLong(), entry.size.toLong(), 0), input, entry),
        ExpectedCall(CodecCall(kind, format, lengthSize, hasOti, oti, outLen, CodecVectors.MOQ_OK.toLong(), outLen, input.size.toLong(), entry.size.toLong(), 0), input, entry),
    )

/**
 * The allocation ledger of one armed invocation: every attempt's (size, state),
 * 0 live, 1 retired, 2 failed, as they stood when it was disarmed, plus how many
 * frees of still-live blocks the ledger itself then executed ([reclaimed]).
 */
internal class AllocLedger(val attempts: Long, val doubleFrees: Long, val foreignFrees: Long, val reclaimed: Long, val entries: List<Pair<Long, Long>>) {
    companion object {
        fun of(v: LongArray): AllocLedger {
            val n = (v.size - 4) / 2
            return AllocLedger(v[0], v[1], v[2], v[3], (0 until n).map { v[4 + 2 * it] to v[5 + 2 * it] })
        }
    }

    override fun toString(): String = "attempts=$attempts doubleFrees=$doubleFrees foreignFrees=$foreignFrees reclaimed=$reclaimed entries=$entries"
}

internal const val ALLOC_LIVE = 0L
internal const val ALLOC_RETIRED = 1L
internal const val ALLOC_FAILED = 2L

/**
 * Every attempt exactly as declared; nothing freed twice or freed that this
 * invocation did not own; and one free executed by the ledger itself for every
 * block declared live.
 */
internal fun expectAllocations(expected: List<Pair<Long, Long>>, ledger: AllocLedger?, label: String) {
    kotlin.test.assertNotNull(ledger, "$label: the allocation ledger was not settled")
    kotlin.test.assertEquals(expected.size.toLong(), ledger.attempts, "$label: allocation attempts ($ledger)")
    kotlin.test.assertEquals(expected, ledger.entries, "$label: (size, state) per attempt; state 0 live, 1 retired, 2 failed")
    kotlin.test.assertEquals(0L, ledger.doubleFrees, "$label: no block retired twice")
    kotlin.test.assertEquals(0L, ledger.foreignFrees, "$label: no block freed that this invocation did not own")
    kotlin.test.assertEquals(expected.count { it.second == ALLOC_LIVE }.toLong(), ledger.reclaimed, "$label: every live block reclaimed by the ledger")
}

/** What one scoped invocation produced, and what its observers recorded once they were settled. */
internal class Observation<T>(val result: Result<T>, val env: IntArray?, val alloc: AllocLedger?) {
    /** The value, or the original Throwable rethrown as itself. */
    fun get(): T = result.getOrThrow()
}

/**
 * Runs [body] with the requested observers armed on this thread and settles
 * them UNCONDITIONALLY, env first (restoring the JNI table) and then the
 * allocator (reclaiming any block still live), whatever [body] threw -- an
 * exception, an Error or a failed assertion -- which is kept as itself in
 * [Observation.result]. Nothing is asserted here; a row asserts on the result
 * and the settled observations afterwards.
 */
internal fun <T> observed(
    allocFailAt: Int? = null,
    envSite: Int? = null,
    envSkip: Int = 0,
    envThrowable: Throwable? = null,
    body: () -> T,
): Observation<T> {
    var allocArmed = false
    var envArmed = false
    var env: IntArray? = null
    var alloc: AllocLedger? = null
    val result: Result<T>
    try {
        if (allocFailAt != null) {
            check(TrustRouteProbe.nativeCodecAllocArm(allocFailAt)) { "setup: an allocation ledger is already armed" }
            allocArmed = true
        }
        if (envSite != null) {
            TrustRouteProbe.nativeCodecEnvArm(envSite, envSkip, envThrowable)
            envArmed = true
        }
        result = runCatching(body)
    } finally {
        try {
            if (envArmed) env = TrustRouteProbe.nativeCodecEnvDisarm()
        } finally {
            if (allocArmed) alloc = TrustRouteProbe.nativeCodecAllocDisarm()?.let { AllocLedger.of(it) }
        }
    }
    return Observation(result, env, alloc)
}

/**
 * Runs every subcase, whatever an earlier one did, prints which executed and
 * which failed, and fails the row with all of them if any failed.
 */
internal fun subcases(row: String, cases: List<Pair<String, () -> Unit>>) {
    val results = cases.map { (name, body) -> name to runCatching(body).exceptionOrNull() }
    val failed = results.filter { it.second != null }
    println("SUBCASES $row executed=${results.size} failed=${failed.size}" + failed.joinToString("") { "\n  FAILED ${it.first}: ${it.second}" })
    if (failed.isNotEmpty()) {
        throw AssertionError(
            "$row: ${failed.size} of ${results.size} subcase(s) failed:\n" + failed.joinToString("\n") { "  ${it.first}: ${it.second}" },
            failed.first().second,
        )
    }
}
