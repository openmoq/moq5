package org.openmoq.moq5

/**
 * A MoQ identity that is carried on the wire as raw bytes.
 *
 * `moq_bytes_t` is documented as "not required to be UTF-8 or NUL-terminated"
 * (`core/include/moq/types.h`), and namespace parts and track names are exactly
 * those bytes. So identity here is the byte string and nothing else: two values
 * are equal when their bytes are equal, a NUL is content rather than a
 * terminator, and no conversion through [String] is involved in deciding what a
 * value *is*.
 *
 * The stored array is never shared with a caller. It is copied on the way in and
 * on the way out, so neither the constructing caller nor a later reader can
 * mutate a value that other code already holds.
 *
 * This is not a `data class`: a generated `equals` would compare the [ByteArray]
 * by identity, so two values carrying the same bytes would differ.
 */
public sealed class ByteIdentity(bytes: ByteArray, label: String) {
    private val storage: ByteArray = bytes.copyOf()

    /**
     * The stable name this kind of identity uses in diagnostics, supplied by
     * the subtype and stored as private final state.
     *
     * It is a literal rather than `this::class.simpleName`: a runtime class
     * name is not part of the public contract, so obfuscation or a rename would
     * silently change diagnostic text that operators and tests read. It is
     * private, not a protected abstract property, because the value is fixed
     * per library-owned subtype and an overridable getter would be public ABI
     * for a constant.
     */
    private val diagnosticLabel: String = label

    /** The number of bytes in this identity. */
    public val size: Int get() = storage.size

    /** A copy of the bytes. Mutating the result cannot affect this value. */
    public fun bytes(): ByteArray = storage.copyOf()

    /**
     * The bytes decoded as UTF-8, or `null` when they are not valid UTF-8.
     *
     * This is the explicit human-text view. It is deliberately not the identity:
     * two distinct byte strings can both fail to decode, and a lossy conversion
     * would map them to the same replacement text and collapse values the
     * protocol keeps apart.
     */
    public fun utf8OrNull(): String? = decodeUtf8OrNull(storage)

    final override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false
        return storage.contentEquals((other as ByteIdentity).storage)
    }

    final override fun hashCode(): Int = storage.contentHashCode()

    /**
     * A deterministic, ASCII-safe, unambiguous rendering: every byte as two
     * lowercase hex digits.
     *
     * Diagnostics go to shared sinks such as logcat, and these bytes are
     * peer-influenced: a part containing a newline, ESC, NUL or a bidi control
     * could inject or visually rewrite surrounding log lines if it were emitted
     * literally. Hex is total, injective and locale-independent, so one
     * rendering serves every byte string, and leading zero bytes stay visible.
     * [utf8OrNull] remains the way to ask for text, explicitly.
     */
    final override fun toString(): String {
        val out = StringBuilder(storage.size * 2 + 24)
        out.append(diagnosticLabel).append("(hex:")
        for (b in storage) {
            val v = b.toInt() and 0xFF
            out.append(HEX[v ushr 4]).append(HEX[v and 0x0F])
        }
        return out.append(')').toString()
    }

    private companion object {
        // A fixed table rather than String.format: a default-locale conversion
        // in a byte loop is both slower and locale-dependent.
        private val HEX = "0123456789abcdef".toCharArray()
    }
}

/**
 * Wire limits, MIRRORING the C and spec contract.
 *
 * These are handwritten Kotlin literals, not values extracted from the C
 * headers: nothing here reads `MOQ_FULL_TRACK_NAME_MAX` at build time. What
 * keeps them honest is the independent boundary tests, which assert the exact
 * accept/reject edges rather than comparing one constant with itself.
 */
public object MoqLimits {
    /** `MOQ_FULL_TRACK_NAME_MAX` (`core/include/moq/types.h`). */
    public const val FULL_TRACK_NAME_MAX: Int = 4096

    /** The Track Namespace field maximum (`core/include/moq/buf.h`). */
    public const val NAMESPACE_MAX_PARTS: Int = 32
}

/**
 * One field of a Track Namespace.
 *
 * `core/include/moq/types.h` states that every present part must be non-empty,
 * so an empty part is rejected at construction rather than carried to the wire.
 */
public class NamespacePart(bytes: ByteArray) : ByteIdentity(bytes, "NamespacePart") {
    init {
        require(bytes.isNotEmpty()) {
            "a namespace part must be non-empty (core/include/moq/types.h); " +
                "an empty NAMESPACE is expressed as zero parts, not as one empty part"
        }
        require(bytes.size <= MoqLimits.FULL_TRACK_NAME_MAX) {
            "a namespace part of ${bytes.size} bytes exceeds the " +
                "${MoqLimits.FULL_TRACK_NAME_MAX}-byte Full Track Name bound"
        }
    }
}

/**
 * A Track Name.
 *
 * May be empty: the non-empty rule in `moq_namespace_t` is about namespace
 * fields, not track names (draft-16 §2.4.1, draft-18 §2.4.1). Its own bytes are
 * still bounded by the Full Track Name maximum; the COMBINED bound with a
 * namespace lives in [FullTrackName].
 */
public class TrackName(bytes: ByteArray) : ByteIdentity(bytes, "TrackName") {
    init {
        require(bytes.size <= MoqLimits.FULL_TRACK_NAME_MAX) {
            "a track name of ${bytes.size} bytes exceeds the " +
                "${MoqLimits.FULL_TRACK_NAME_MAX}-byte Full Track Name bound"
        }
    }
}

/**
 * A Track Namespace: an ordered sequence of 0..32 parts whose value bytes total
 * at most 4096.
 *
 * Both bounds are the wire's: `core/include/moq/buf.h` encodes and decodes a
 * Track Namespace with 0..32 fields and a total field-value size of at most
 * 4096 bytes, and `core/src/internal/validate.h` performs exactly that check.
 *
 * Zero parts is legal here. The draft-16 rule that a *general* namespace has at
 * least one field is a negotiated-profile concern — a Track Namespace Prefix
 * legitimately has zero fields, meaning "match all" — so it is deliberately not
 * baked into this profile-neutral value.
 */
public class Namespace(parts: List<NamespacePart>) {
    // Private owned storage. `List` is a read-only INTERFACE, not an immutable
    // runtime type: `toList()` of a multi-element list is commonly an
    // ArrayList, so handing it out would let a consumer cast it back to
    // MutableList and mutate a value that is already a key in a set or map.
    private val storage: List<NamespacePart> = parts.toList()

    /**
     * The parts, in wire order.
     *
     * Each call returns an independent snapshot, so mutating the result — even
     * by casting it to a mutable type — cannot alter this namespace, its
     * equality or its hash.
     */
    public val parts: List<NamespacePart> get() = storage.toList()

    /** The number of parts, without materialising a snapshot. */
    public val partCount: Int get() = storage.size

    /** The part at [index], in wire order. */
    public fun partAt(index: Int): NamespacePart = storage[index]

    /** Total namespace value bytes, as the wire counts them. */
    public val valueByteCount: Int

    init {
        require(storage.size <= MoqLimits.NAMESPACE_MAX_PARTS) {
            "a track namespace carries at most ${MoqLimits.NAMESPACE_MAX_PARTS} parts " +
                "(core/include/moq/buf.h); got ${storage.size}"
        }
        // Overflow-safe accumulation, in the same subtraction form
        // core/src/internal/validate.h uses: the sum is never formed.
        var total = 0
        for (p in storage) {
            require(p.size <= MoqLimits.FULL_TRACK_NAME_MAX - total) {
                "track namespace value bytes exceed " +
                    "${MoqLimits.FULL_TRACK_NAME_MAX} (core/include/moq/buf.h)"
            }
            total += p.size
        }
        valueByteCount = total
    }

    override fun equals(other: Any?): Boolean =
        this === other || (other is Namespace && storage == other.storage)

    override fun hashCode(): Int = storage.hashCode()

    override fun toString(): String = "Namespace(${storage.joinToString("/")})"
}

/**
 * A Full Track Name: a [Namespace] together with a [TrackName].
 *
 * The combined bound is the one the wire actually imposes — namespace value
 * bytes plus track-name bytes at most `MOQ_FULL_TRACK_NAME_MAX`
 * (`core/src/internal/validate.h`, draft-16 §2.4.1, draft-18 §2.4.1). Checking
 * the two components separately is not equivalent: two individually legal
 * components can exceed the bound together.
 *
 * The raw [TrackName] keeps its spec meaning and is not renamed.
 */
public class FullTrackName(
    public val namespace: Namespace,
    public val name: TrackName,
) {
    /** Namespace value bytes plus track-name bytes. */
    public val byteCount: Int

    init {
        // Subtraction, so the sum is never formed and cannot overflow.
        require(name.size <= MoqLimits.FULL_TRACK_NAME_MAX - namespace.valueByteCount) {
            "a full track name of ${namespace.valueByteCount} namespace byte(s) plus " +
                "${name.size} track-name byte(s) exceeds " +
                "${MoqLimits.FULL_TRACK_NAME_MAX} (core/src/internal/validate.h)"
        }
        byteCount = namespace.valueByteCount + name.size
    }

    override fun equals(other: Any?): Boolean =
        this === other ||
            (other is FullTrackName && namespace == other.namespace && name == other.name)

    override fun hashCode(): Int = 31 * namespace.hashCode() + name.hashCode()

    override fun toString(): String = "FullTrackName($namespace, $name)"
}

/**
 * Decodes UTF-8 strictly, returning `null` rather than substituting replacement
 * characters. Strictness is the point: a lossy decode is what would let two
 * distinct wire identities look alike.
 */
private fun decodeUtf8OrNull(bytes: ByteArray): String? =
    try {
        val decoder =
            Charsets.UTF_8.newDecoder()
                .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
        decoder.decode(java.nio.ByteBuffer.wrap(bytes)).toString()
    } catch (_: java.nio.charset.CharacterCodingException) {
        null
    }
