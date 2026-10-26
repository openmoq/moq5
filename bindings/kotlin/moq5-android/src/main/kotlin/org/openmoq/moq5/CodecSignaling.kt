package org.openmoq.moq5

import org.openmoq.moq5.jni.BridgeInit
import org.openmoq.moq5.jni.NativeBridge
import org.openmoq.moq5.jni.NativeBridgeOwner

/**
 * The byte format of a source handed to [CodecSignaling.buildInitData]: the
 * `moq_codec_source_format_t` rows this binding exposes, each carrying its
 * native value explicitly -- never an ordinal.
 *
 * This first release exposes AVC and AAC only. The native utility supports
 * further families; they are not reachable through this enum, and nothing
 * here maps them onto an exposed value.
 */
public enum class CodecConfigSource(internal val native: Int) {
    /** AVC Annex-B SPS/PPS, assembled into an avcC record. */
    AvcAnnexB(1),

    /** An avcC record, validated structurally and copied. */
    AvcRecord(2),

    /** An AudioSpecificConfig, its leading fields validated and copied. */
    AacAudioSpecificConfig(3),
}

/**
 * The decoder configuration format handed to [CodecSignaling.formatCodecString]:
 * the `moq_codec_config_format_t` rows this binding exposes, with explicit
 * native values.
 */
public enum class CodecConfigFormat(internal val native: Int) {
    AvcRecord(1),
    AacAudioSpecificConfig(2),
}

/**
 * An exact four-character sample entry such as `avc1`, `avc3` or `mp4a`: four
 * printable ASCII characters, kept byte for byte. There is no case folding --
 * `Avc1` is a different entry.
 */
public data class SampleEntry(public val code: String) {
    init {
        require(code.length == 4 && code.all { it.code in 0x21..0x7E }) {
            "a sample entry is exactly four printable ASCII characters, not '$code'"
        }
    }
}

/**
 * The native codec utility refused the request, and [code] is its result,
 * carried exactly: `MOQ_ERR_INVAL` (-2), `MOQ_ERR_PROTO` (-3) or
 * `MOQ_ERR_UNSUPPORTED` (-14). An allocation failure, a cancellation or any
 * other [Throwable] is never one of these.
 */
public class CodecSignalingException internal constructor(
    public val code: MoqResultCode,
    message: String,
) : IllegalArgumentException(message)

/**
 * The native codec utility, as pure value operations. Each call passes the
 * process-wide native bridge decision first -- the same one every endpoint
 * uses, made once per process -- and needs no endpoint, token or worker.
 * Both results are STRUCTURAL: a successful build or string says the input
 * was self-consistent for its format, not that a decoder will accept it.
 */
public object CodecSignaling {
    private val calls = CodecSignalingCalls(NativeBridgeOwner::initialize, NativeBridgeCodec)

    /**
     * `moq_codec_init_data_build`: the decoder configuration record for
     * [bytes] in the [source] format. [nalLengthSize] must be `0..255`; the
     * native utility decides its meaning (1..4 for [CodecConfigSource.AvcAnnexB],
     * ignored for the others).
     */
    public fun buildInitData(source: CodecConfigSource, bytes: MediaBytes, nalLengthSize: Int = 4): MediaBytes =
        calls.buildInitData(source, bytes, nalLengthSize)

    /**
     * `moq_codec_string_format`: the ASCII codec string for [decoderConfig]
     * in [format] under the exact [sampleEntry]. [mp4ObjectTypeIndication],
     * when present, must be `0..255`; the native utility decides which formats
     * require or forbid it.
     */
    public fun formatCodecString(
        format: CodecConfigFormat,
        sampleEntry: SampleEntry,
        decoderConfig: MediaBytes,
        mp4ObjectTypeIndication: Int? = null,
    ): String = calls.formatCodecString(format, sampleEntry, decoderConfig, mp4ObjectTypeIndication)
}

/** The production port: the two registered bridge rows, forwarded as they are. */
internal object NativeBridgeCodec : CodecNative {
    override fun initDataBuild(sourceFormat: Int, source: ByteArray, nalLengthSize: Int, out: Array<ByteArray?>): Int =
        NativeBridge.nativeCodecInitDataBuild(sourceFormat, source, nalLengthSize, out)

    override fun stringFormat(configFormat: Int, sampleEntry: ByteArray, hasOti: Boolean, oti: Int, decoderConfig: ByteArray, out: Array<ByteArray?>): Int =
        NativeBridge.nativeCodecStringFormat(configFormat, sampleEntry, hasOti, oti, decoderConfig, out)
}

/**
 * The native call port the codec operations reach: one method per registered
 * bridge row, with the row's exact argument shape. Production forwards to the
 * bridge; host mapping tests script it.
 */
internal interface CodecNative {
    fun initDataBuild(sourceFormat: Int, source: ByteArray, nalLengthSize: Int, out: Array<ByteArray?>): Int

    fun stringFormat(configFormat: Int, sampleEntry: ByteArray, hasOti: Boolean, oti: Int, decoderConfig: ByteArray, out: Array<ByteArray?>): Int
}

/**
 * One configured route to the codec utility: the bridge decision it must pass
 * and the port it calls. The public [CodecSignaling] object uses the
 * process-wide owner and the bridge; a test builds its own instance.
 */
internal class CodecSignalingCalls(
    private val decide: () -> BridgeInit,
    private val native: CodecNative,
) {
    /**
     * The argument is checked for representability first -- the native field
     * is one unsigned byte -- so an unrepresentable value never reaches the
     * handshake or the native side. The MEANING of a representable value
     * (1..4 for the Annex-B builders; ignored for the other sources) is the
     * native utility's decision.
     */
    fun buildInitData(source: CodecConfigSource, bytes: MediaBytes, nalLengthSize: Int): MediaBytes {
        require(nalLengthSize in UNSIGNED_BYTE) { "nalLengthSize $nalLengthSize is not representable in the native unsigned byte" }
        requireBridge()
        val out = arrayOfNulls<ByteArray>(1)
        val status = native.initDataBuild(source.native, bytes.bytes(), nalLengthSize, out)
        return MediaBytes(result(status, out, "codec init data build"))
    }

    /**
     * An absent [mp4ObjectTypeIndication] crosses as `false`/`0`; a present one
     * must be representable in the native unsigned byte. Whether the format
     * requires or forbids it is the native utility's decision.
     */
    fun formatCodecString(format: CodecConfigFormat, sampleEntry: SampleEntry, decoderConfig: MediaBytes, mp4ObjectTypeIndication: Int?): String {
        if (mp4ObjectTypeIndication != null) {
            require(mp4ObjectTypeIndication in UNSIGNED_BYTE) {
                "the object type indication $mp4ObjectTypeIndication is not representable in the native unsigned byte"
            }
        }
        requireBridge()
        val out = arrayOfNulls<ByteArray>(1)
        val status =
            native.stringFormat(
                format.native,
                sampleEntry.code.toByteArray(Charsets.US_ASCII),
                mp4ObjectTypeIndication != null,
                mp4ObjectTypeIndication ?: 0,
                decoderConfig.bytes(),
                out,
            )
        val ascii = result(status, out, "codec string format")
        check(ascii.all { it in PRINTABLE_ASCII }) { "the codec bridge returned a non-ASCII codec string" }
        return String(ascii, Charsets.US_ASCII)
    }

    private fun requireBridge() {
        val decision = decide()
        if (decision !is BridgeInit.Ready) {
            throw NativeBridgeInitializationException(decision.toInitializationFailure())
        }
    }

    /** The one status translation for this operation. A Throwable from the port never reaches it. */
    private fun result(status: Int, out: Array<ByteArray?>, operation: String): ByteArray =
        when (status) {
            MOQ_OK -> {
                val bytes = out[0]
                check(bytes != null && bytes.isNotEmpty()) { "the codec bridge returned OK without a result for the $operation" }
                bytes
            }
            MOQ_ERR_INVAL, MOQ_ERR_PROTO, MOQ_ERR_UNSUPPORTED ->
                throw CodecSignalingException(MoqResultCode(status), "the native codec utility refused the $operation ($status)")
            MOQ_ERR_NOMEM -> throw OutOfMemoryError("a native allocation failed in the $operation")
            MOQ_ERR_INTERNAL -> throw IllegalStateException("codec bridge contract failure in the $operation")
            else -> throw IllegalStateException("the codec bridge returned unexpected status $status for the $operation")
        }

    private companion object {
        val UNSIGNED_BYTE = 0..255
        val PRINTABLE_ASCII = 0x21.toByte()..0x7E.toByte()
        const val MOQ_OK = 0
        const val MOQ_ERR_NOMEM = -1
        const val MOQ_ERR_INVAL = -2
        const val MOQ_ERR_PROTO = -3
        const val MOQ_ERR_UNSUPPORTED = -14
        const val MOQ_ERR_INTERNAL = -99
    }
}
