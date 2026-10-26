package org.openmoq.moq5

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

/**
 * The sender's public VALUES, pinned against `media_sender.h`.
 *
 *  * [SendObject] is an immutable outbound value: its bytes are copied on
 *    the way in ([MediaBytes]), so a caller mutating its own array after
 *    construction cannot change what is written. Timing fields keep their
 *    domains: `presentation_time_us` / `decode_time_us` are media-timeline
 *    microseconds ([MediaMicros]); `capture_time_us` is wall-clock
 *    microseconds ([UnixMicros]); `has_capture_time` is the nullability of
 *    [SendObject.captureTime], never a zero sentinel.
 *  * [SenderStats] carries every `moq_media_sender_stats_t` field in the
 *    struct's order, counters as full-width `ULong`; `last_error` is a
 *    `moq_result_t`, so it is a signed [MoqResultCode], not a wire error.
 *  * [SenderFatalCode] is the sender's own domain (`MOQ_MEDIA_SENDER_FATAL_*`,
 *    or the endpoint's code underneath), distinct from the receiver's and
 *    from [MoqErrorCode].
 *  * [SendBackpressure] spells exactly the policies the binding admits:
 *    the two drop policies and `RETURN_WOULD_BLOCK`. `BLOCK_TIMEOUT` has no
 *    spelling: it parks the calling thread inside C, and on Android every
 *    native call runs on the endpoint's service worker, so one parked write
 *    would stall every other operation on the endpoint and could not be
 *    cancelled. Its intent -- the app decides the drop -- is
 *    `MediaSender.write(timeout)` over `ReturnWouldBlock`.
 *  * Configuration defaults are the values the C resolves `0` to, stated as
 *    such, and the MSF-required fields are refused at construction.
 */
class SenderValueDomainTest {
    private companion object {
        val VIDEO =
            SendTrackConfig(
                name = TrackName("v".toByteArray()), mediaType = MediaType.Video, packaging = PackagingKind.Raw,
                codec = MediaBytes("avc1.64001f".toByteArray()), bitrate = 2_000_000uL,
            )
        val AUDIO =
            SendTrackConfig(
                name = TrackName("a".toByteArray()), mediaType = MediaType.Audio, packaging = PackagingKind.Raw,
                codec = MediaBytes("opus".toByteArray()), bitrate = 96_000uL, samplerate = 48_000u, channelConfig = MediaBytes("2".toByteArray()),
            )
        val NAMESPACE = Namespace(listOf(NamespacePart("live".toByteArray())))
        val widthProbes = listOf(1uL shl 32, (1uL shl 32) or 1uL, 0xDEADBEEF_00000001uL, ULong.MAX_VALUE - 1uL)
    }

    @Test
    fun `a SendObject is immutable -- the payload is copied on construction and on read`() {
        val source = byteArrayOf(1, 2, 3)
        val obj = SendObject(payload = MediaBytes(source), isSync = true, startsGroup = true, presentationTime = MediaMicros(10uL))
        source[0] = 9
        assertEquals(listOf<Byte>(1, 2, 3), obj.payload.bytes().toList(), "a caller's later mutation is not the object's bytes")
        val view = obj.payload.bytes()
        view[1] = 9
        assertEquals(listOf<Byte>(1, 2, 3), obj.payload.bytes().toList(), "a reader cannot mutate the value through the accessor")
    }

    @Test
    fun `a SendObject's optional fields default to absent, and its timing fields keep their domains`() {
        val obj = SendObject(payload = MediaBytes(byteArrayOf(1)), isSync = false, startsGroup = false, presentationTime = MediaMicros(5uL))
        assertEquals(false, obj.endsGroup)
        assertNull(obj.decodeTime, "decode_time_us is advisory; absent means the presentation time is sent")
        assertNull(obj.captureTime, "has_capture_time=false is null, never a zero instant")
        assertNull(obj.sapType, "has_sap_type=false is null")
        assertNull(obj.cmafProperties, "RAW/LOC objects carry no property block")
        val timed = obj.copy(decodeTime = MediaMicros(4uL), captureTime = UnixMicros(1_700_000_000_000_000uL), sapType = SapType.Type1)
        assertEquals(MediaMicros(4uL), timed.decodeTime)
        assertEquals(UnixMicros(1_700_000_000_000_000uL), timed.captureTime)
        assertNotEquals<Any>(MediaMicros(4uL), UnixMicros(4uL), "media time and wall-clock time are different types")
    }

    @Test
    fun `SenderStats names every C field in struct order, full width, with last_error a signed result code`() {
        for (v in widthProbes) {
            val s = SenderStats(v, v, v, v, v, v, v, v, v, MoqResultCode(-9), v)
            assertEquals(v, s.objectsWritten)
            assertEquals(v, s.objectsSent)
            assertEquals(v, s.objectsQueued)
            assertEquals(v, s.bytesQueued)
            assertEquals(v, s.objectsDropped)
            assertEquals(v, s.groupsDropped)
            assertEquals(v, s.keyframesDropped)
            assertEquals(v, s.groupsAbandoned)
            assertEquals(v, s.backpressureStalls)
            assertEquals(v, s.sapRecordsEvicted)
        }
        val s = SenderStats(0uL, 0uL, 0uL, 0uL, 0uL, 0uL, 0uL, 0uL, 0uL, MoqResultCode(Int.MIN_VALUE), 0uL)
        assertEquals(Int.MIN_VALUE, s.lastError.value, "moq_result_t is int: signed, 32-bit")
        assertNotEquals<Any>(s.lastError, MoqErrorCode(0uL), "a local result is never a wire error code")
    }

    @Test
    fun `SenderFatalCode is the sender's own domain -- the five named codes, full width, distinct from the receiver's`() {
        assertEquals(SenderFatalCode(0x1uL), SenderFatalCode.CATALOG_ENCODE)
        assertEquals(SenderFatalCode(0x2uL), SenderFatalCode.SETUP_FAILED)
        assertEquals(SenderFatalCode(0x3uL), SenderFatalCode.NAMESPACE_REJECTED)
        assertEquals(SenderFatalCode(0x4uL), SenderFatalCode.NAMESPACE_CANCELLED)
        assertEquals(SenderFatalCode(0x5uL), SenderFatalCode.PUBLISH_REJECTED)
        for (v in widthProbes) assertEquals(v, SenderFatalCode.fromNative(v.toLong()).value, "all 64 bits reinterpreted at $v")
        assertNotEquals<Any>(SenderFatalCode(0x3uL), ReceiverFatalCode(0x3uL), "the same number in two domains is two values")
        assertNotEquals<Any>(SenderFatalCode(0x3uL), MoqErrorCode(0x3uL))
    }

    @Test
    fun `a SendObject's decode time is the C decode_time_us -- absent sends the presentation time -- and RAW-versus-CMAF property rules stay the C's`() {
        val obj = SendObject(payload = MediaBytes(byteArrayOf(1)), isSync = true, startsGroup = true, presentationTime = MediaMicros(77uL))
        assertEquals(MediaMicros(77uL), obj.decodeTimeOrPresentation(), "decode_time_us carries the presentation time when the app declares none")
        assertEquals(MediaMicros(5uL), obj.copy(decodeTime = MediaMicros(5uL)).decodeTimeOrPresentation())
        val props = obj.copy(cmafProperties = MediaBytes(byteArrayOf(9)))
        assertEquals(listOf<Byte>(9), props.cmafProperties!!.bytes().toList(), "a property block is carried as given: whether it is legal for the track's packaging is the C's INVAL, not a binding rule")
    }

    @Test
    fun `SendBackpressure spells the two drop policies and ReturnWouldBlock, and nothing else`() {
        assertEquals(listOf("DropToKeyframe", "DropGroup", "ReturnWouldBlock"), SendBackpressure.entries.map { it.name })
    }

    @Test
    fun `SenderConfig states the C defaults as the values they resolve to, and refuses what the C refuses, before any native work`() {
        val cfg = SenderConfig(namespace = NAMESPACE, backpressure = SendBackpressure.DropToKeyframe)
        assertNull(cfg.catalogTrack, "null is the C default: the track named \"catalog\"")
        assertEquals(TrackName("cat".toByteArray()), cfg.copy(catalogTrack = TrackName("cat".toByteArray())).catalogTrack, "a TRACK NAME (catalog_track is moq_bytes_t), not a namespace part")
        assertFailsWith<IllegalArgumentException> { cfg.copy(catalogTrack = TrackName(ByteArray(0))) }
        assertEquals(256u, cfg.queueMaxObjects)
        assertEquals(32u * 1024u * 1024u, cfg.queueMaxBytes)
        assertEquals(256u, cfg.preReadyMaxObjects)
        assertEquals(32u * 1024u * 1024u, cfg.preReadyMaxBytes)
        assertEquals(true, cfg.validateCmaf, "strict by default, as every cfg_init sets it")
        assertEquals(false, cfg.publishTracks)
        assertEquals(false, cfg.dropWithoutDemand)
        assertEquals(CatalogRefresh.Default, cfg.catalogRefresh)
        assertFailsWith<IllegalArgumentException> { cfg.copy(namespace = Namespace(emptyList())) }
        // Every part non-empty (media_sender.c:3493-3498) is the NamespacePart type's own rule: an empty part cannot be constructed, so no namespace reaching the config can carry one.
        assertFailsWith<IllegalArgumentException> { Namespace(listOf(NamespacePart("live".toByteArray()), NamespacePart(ByteArray(0)))) }
        assertEquals(2, cfg.copy(namespace = Namespace(listOf(NamespacePart("a".toByteArray()), NamespacePart("b".toByteArray())))).namespace.partCount)
        assertFailsWith<IllegalArgumentException> { cfg.copy(queueMaxObjects = 0u) }
        assertFailsWith<IllegalArgumentException> { cfg.copy(queueMaxBytes = 0u) }
        assertFailsWith<IllegalArgumentException> { cfg.copy(preReadyMaxObjects = 0u) }
        assertFailsWith<IllegalArgumentException> { cfg.copy(preReadyMaxBytes = 0u) }
    }

    @Test
    fun `CatalogRefresh is the three C meanings -- the library default, a positive whole-microsecond interval, or disabled`() {
        assertEquals(1.seconds, CatalogRefresh.Every(1.seconds).interval)
        assertEquals(CatalogRefresh.Every(250.milliseconds), CatalogRefresh.Every(250.milliseconds))
        assertNotEquals<CatalogRefresh>(CatalogRefresh.Default, CatalogRefresh.Disabled)
        assertFailsWith<IllegalArgumentException> { CatalogRefresh.Every(Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { CatalogRefresh.Every((-1).seconds) }
        assertFailsWith<IllegalArgumentException> { CatalogRefresh.Every(Duration.INFINITE) }
        assertFailsWith<IllegalArgumentException> { CatalogRefresh.Every(500.nanoseconds) }
        assertFailsWith<IllegalArgumentException> { CatalogRefresh.Every(1_500.nanoseconds) }
        assertEquals(1.microseconds, CatalogRefresh.Every(1.microseconds).interval, "the smallest interval the C field can carry")
    }

    @Test
    fun `CatalogRefresh lowers to catalog_refresh_interval_us exactly -- Default 0, Disabled UINT64_MAX, Every its whole microseconds`() {
        assertEquals(0L, CatalogRefresh.Default.toNativeMicros())
        assertEquals(-1L, CatalogRefresh.Disabled.toNativeMicros(), "UINT64_MAX through the signed Long carrier")
        assertEquals(1_000_000L, CatalogRefresh.Every(1.seconds).toNativeMicros())
        assertEquals(250_000L, CatalogRefresh.Every(250.milliseconds).toNativeMicros())
        assertEquals(1L, CatalogRefresh.Every(1.microseconds).toNativeMicros())
        val long = 1_000_000.days
        assertEquals(long.inWholeMicroseconds, CatalogRefresh.Every(long).toNativeMicros())
        assertTrue(CatalogRefresh.Every(long).toNativeMicros() > 0L, "every finite interval is a positive Long: only infinity is unrepresentable, and it is refused at construction")
    }

    @Test
    fun `SendTrackConfig refuses the MSF-required omissions and the live-with-duration conflict at construction`() {
        assertEquals(1_000_000u, VIDEO.timescale, "0 on the wire means microseconds; the default is stated as that value")
        assertEquals(true, VIDEO.isLive)
        assertNull(VIDEO.trackDuration)
        assertEquals(false, VIDEO.emitSapTimeline)
        assertEquals(8u, VIDEO.sapTimelineHistoryGroups, "0 on the wire means 8")
        assertEquals(false, VIDEO.emitMediaTimeline)
        assertEquals(8u, VIDEO.mediaTimelineHistoryGroups)
        assertNull(VIDEO.altGroup)
        assertNull(VIDEO.maxGroupSap)
        assertNull(VIDEO.maxObjectSap)
        assertFailsWith<IllegalArgumentException> { VIDEO.copy(name = TrackName(ByteArray(0))) }
        assertFailsWith<IllegalArgumentException> { VIDEO.copy(codec = MediaBytes(ByteArray(0))) }
        assertFailsWith<IllegalArgumentException> { VIDEO.copy(bitrate = 0uL) }
        assertFailsWith<IllegalArgumentException> { AUDIO.copy(samplerate = 0u) }
        assertFailsWith<IllegalArgumentException> { AUDIO.copy(channelConfig = null) }
        assertFailsWith<IllegalArgumentException> { AUDIO.copy(channelConfig = MediaBytes(ByteArray(0))) }
        assertFailsWith<IllegalArgumentException> { VIDEO.copy(trackDuration = MediaMillis(10uL)) }
        val vod = VIDEO.copy(isLive = false, trackDuration = MediaMillis(90_000uL))
        assertEquals(MediaMillis(90_000uL), vod.trackDuration, "a VOD-from-start duration is media milliseconds")
        assertNull(VIDEO.copy(isLive = false).trackDuration, "the C forbids a duration WITH isLive; it does not require one without, and neither does this surface")
        assertFailsWith<IllegalArgumentException> { VIDEO.copy(timescale = 0u) }
        assertFailsWith<IllegalArgumentException> { VIDEO.copy(sapTimelineHistoryGroups = 0u) }
    }

    @Test
    fun `a VodConversion pairs a sender-owned track handle with a media-millisecond duration`() {
        val track = SendTrack(token = 7L, config = VIDEO)
        val item = VodConversion(track, MediaMillis(90_000uL))
        val duration: MediaMillis = item.duration
        assertEquals(MediaMillis(90_000uL), duration, "MSF §5.2.35 trackDuration is integer media milliseconds")
        assertTrue(item.track === track, "the item names the sender's own handle, never a track name")
        assertEquals(item, item.copy(), "a value: equal by content")
    }
}
