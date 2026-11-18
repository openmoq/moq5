package org.openmoq.moq5

/**
 * A LibMoQ API result.
 *
 * `moq_result_t` is `int` (`core/include/moq/types.h`), so this is signed and
 * 32-bit. It is a SEPARATE domain from [MoqErrorCode]: a result code describes
 * what a local call did, while an error code is a value carried on the wire.
 * Mixing them would let a local failure be reported to a peer, or a peer's code
 * be interpreted as a local status.
 */
@JvmInline
public value class MoqResultCode(public val value: Int) {
    override fun toString(): String = "MoqResultCode($value)"
}

/**
 * A MoQ protocol error code, as carried on the wire.
 *
 * The wire type is `uint64_t` (`core/include/moq/session.h`), so the full
 * unsigned 64-bit range is preserved. A JNI `jlong` / Kotlin `Long` carries all
 * 64 raw bits unchanged and round-trips through `toULong()`; the error is
 * INTERPRETING those bits as a signed numeric value, which reorders and
 * mis-compares codes at the top of the range. Narrowing to `Int` is what
 * actually loses bits.
 */
@JvmInline
public value class MoqErrorCode(public val value: ULong) {
    override fun toString(): String = "MoqErrorCode($value)"
}

/**
 * A media receiver's fatal code (`moq_media_receiver_fatal_code()`).
 *
 * This is the RECEIVER'S OWN domain, not a MoQ wire error code: when the
 * receiver itself failed it is one of the `MOQ_MEDIA_RECEIVER_FATAL_*`
 * constants below, and when the endpoint failed underneath it, it is the
 * endpoint's fatal code. The C type is `uint64_t`; the JNI `jlong` carries
 * all 64 bits and [fromNative] reinterprets them, never narrows or signs
 * them. There is deliberately no conversion to [MoqErrorCode]: the two
 * domains never meet.
 */
@JvmInline
public value class ReceiverFatalCode(public val value: ULong) {
    override fun toString(): String = "ReceiverFatalCode($value)"

    public companion object {
        /** The catalog could not be parsed or derived into tracks. */
        public val CATALOG_UNUSABLE: ReceiverFatalCode = ReceiverFatalCode(0x1uL)

        /** The track-event queue overflowed; a discovery event was lost. */
        public val EVENT_OVERFLOW: ReceiverFatalCode = ReceiverFatalCode(0x2uL)

        /** The catalog subscription could not be set up. */
        public val SETUP_FAILED: ReceiverFatalCode = ReceiverFatalCode(0x3uL)

        /** The publisher refused the catalog subscription. */
        public val CATALOG_REJECTED: ReceiverFatalCode = ReceiverFatalCode(0x4uL)

        /** All 64 raw bits of a native `uint64_t`, reinterpreted. */
        public fun fromNative(raw: Long): ReceiverFatalCode = ReceiverFatalCode(raw.toULong())
    }
}

/**
 * A MoQ Request ID, `uint64_t` on the wire
 * (`core/include/moq/session.h`). Full width, unsigned.
 */
@JvmInline
public value class RequestId(public val value: ULong) {
    override fun toString(): String = "RequestId($value)"
}

/**
 * Wall-clock microseconds since the Unix epoch.
 *
 * This is the domain of the LOC Capture Timestamp, `capture_time_us`
 * (`service/include/moq/media_sender.h`): an absolute instant, not a media
 * timeline position.
 */
@JvmInline
public value class UnixMicros(public val value: ULong) {
    override fun toString(): String = "UnixMicros($value)"
}

/**
 * A media-timeline position in microseconds, unsigned
 * (for example `decode_time_us`).
 */
@JvmInline
public value class MediaMicros(public val value: ULong) {
    override fun toString(): String = "MediaMicros($value)"
}

/**
 * A SIGNED media-timeline offset in microseconds.
 *
 * `composition_offset_us` is `int64_t` (`service/include/moq/media_receiver.h`):
 * a composition offset is legitimately negative, so this domain is signed while
 * [MediaMicros] is not.
 */
@JvmInline
public value class MediaOffsetMicros(public val value: Long) {
    override fun toString(): String = "MediaOffsetMicros($value)"
}

/**
 * A media-timeline time in MILLISECONDS.
 *
 * This is the domain of `ept_ms` (earliest presentation time) and
 * `media_time_ms` (presentation time) — positions on the media timeline
 * (`service/include/moq/media_receiver.h`). It is NOT wallclock: see
 * [UnixMillis].
 *
 * The domain stays in milliseconds rather than being normalised to
 * microseconds: multiplying a uint64 millisecond value by 1000 overflows across
 * most of its range, so a "convenience" conversion would silently corrupt large
 * values.
 */
@JvmInline
public value class MediaMillis(public val value: ULong) {
    override fun toString(): String = "MediaMillis($value)"
}

/**
 * Wallclock MILLISECONDS since the Unix epoch.
 *
 * This is `wallclock_ms` — encode wallclock, an absolute instant
 * (`service/include/moq/media_receiver.h`), and the MSF `startWallclock` /
 * `deltaWallclock` domain (`media/msf/include/moq/msf.h`). It is a different
 * domain from [MediaMillis]: one names a point on a media timeline, the other a
 * point in civil time, and confusing them silently misplaces media in time.
 *
 * Zero is the NATIVE ABSENT marker here, not the epoch instant: MSF records
 * `0 for VOD`, meaning "no live wallclock". Use [fromNativeOrNull] at the
 * boundary so an absent value becomes `null` rather than 1970-01-01. The
 * constructor itself does not reject zero — a caller that genuinely means the
 * epoch may say so — but nothing in this binding should invent one.
 */
@JvmInline
public value class UnixMillis(public val value: ULong) {
    override fun toString(): String = "UnixMillis($value)"

    public companion object {
        /**
         * Maps a native `wallclock_ms`-style field: zero means absent
         * (`media/msf/include/moq/msf.h`), anything else is that instant.
         */
        public fun fromNativeOrNull(value: ULong): UnixMillis? =
            if (value == 0uL) null else UnixMillis(value)
    }
}

/**
 * A negotiated MoQ protocol version.
 *
 * The C authority is `moq_version_t`, the enum surfaced by
 * `moq_session_version()` and `moq_endpoint_negotiated_version()`
 * (`core/include/moq/session.h`, `service/include/moq/endpoint.h`). It is a
 * profile selector, not a generic 64-bit wire field, so this carries a [UInt].
 *
 * The value space is open: new drafts appear, and a build may be asked to name
 * a version whose profile it does not implement. An unrecognised value is
 * therefore carried as ITSELF — there is no `Unknown` wrapper and no fallback,
 * so a future draft round-trips exactly and can never be quietly folded into a
 * version this build understands.
 *
 * **Zero is not a version.** `moq_session_version()` returns `(moq_version_t)0`
 * when there is no version to report, so zero means ABSENT. It is rejected at
 * construction, and [fromNativeOrNull] is the boundary helper that turns a
 * native zero into `null`. Treating zero as draft-16 would invent a negotiation
 * that never happened.
 *
 * Because this is a value class over a nonzero `UInt`, there is exactly one
 * representation of each version: `MoqVersion.Draft18` and
 * `MoqVersion(18u)` are the same value.
 */
@JvmInline
public value class MoqVersion(public val code: UInt) {
    init {
        // A value class init block runs on every construction, so there is no
        // second, unchecked way in: `MoqVersion(0u)` cannot exist.
        require(code != 0u) {
            "version code 0 means ABSENT (moq_session_version returns 0 when " +
                "there is no version); it is not draft-16 and not any other draft"
        }
    }

    override fun toString(): String =
        when (code) {
            16u -> "MoqVersion.Draft16"
            18u -> "MoqVersion.Draft18"
            else -> "MoqVersion($code)"
        }

    public companion object {
        /** Draft-16 (`MOQ_VERSION_DRAFT_16`). */
        public val Draft16: MoqVersion = MoqVersion(16u)

        /** Draft-18 (`MOQ_VERSION_DRAFT_18`). */
        public val Draft18: MoqVersion = MoqVersion(18u)

        /**
         * Maps a native `moq_version_t`: zero becomes `null` (absent), anything
         * else is that version, named or not.
         */
        public fun fromNativeOrNull(code: UInt): MoqVersion? =
            if (code == 0u) null else MoqVersion(code)
    }
}
